// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! HTTP-registry discovery backend.
//!
//! Listens on a TCP port and accepts `POST /register` requests from workers
//! that self-register at startup.  Each registration emits a
//! [`DiscoveryEvent::Added`]; the worker manager then introspects the
//! worker via `/server_info` (just like static-urls / k8s backends).
//!
//! This backend is useful for bare-metal deployments where Kubernetes
//! service-discovery is not available and the worker topology is dynamic.

use crate::config::HttpRegistryConfig;
use crate::discovery::{DiscoveryEvent, WorkerId, WorkerMode, WorkerSpec};
use anyhow::Result;
use axum::{extract::State, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Request body for `POST /register`.
#[derive(Debug, Deserialize, Serialize)]
pub struct RegisterRequest {
    /// Worker URL, e.g. `http://10.0.0.7:30100`.
    pub url: String,
    /// Worker role: `prefill`, `decode`, or `plain`.
    #[serde(default = "default_mode")]
    pub mode: String,
    /// Bootstrap port for prefill workers (RDMA KV transfer).
    pub bootstrap_port: Option<u16>,
}

fn default_mode() -> String {
    "plain".to_string()
}

impl RegisterRequest {
    fn into_spec(self) -> WorkerSpec {
        let mode = match self.mode.as_str() {
            "prefill" => WorkerMode::Prefill,
            "decode" => WorkerMode::Decode,
            _ => WorkerMode::Plain,
        };
        WorkerSpec {
            id: WorkerId(self.url.clone()),
            url: self.url,
            mode,
            model_ids: Vec::new(),
            bootstrap_port: self.bootstrap_port,
        }
    }
}

/// Shared state passed to the axum handlers.
#[derive(Clone)]
struct AppState {
    tx: mpsc::Sender<DiscoveryEvent>,
}

/// Spawn the HTTP-registry discovery backend.
///
/// Starts an axum HTTP server that listens for `POST /register` requests.
/// Each valid request emits a [`DiscoveryEvent::Added`] on the channel.
/// The task runs for the lifetime of the router (unlike static_urls which
/// exits after the initial fan-out).
pub async fn spawn(
    cfg: HttpRegistryConfig,
    tx: mpsc::Sender<DiscoveryEvent>,
) -> Result<tokio::task::JoinHandle<()>> {
    let state = Arc::new(AppState { tx });
    let app = Router::new()
        .route("/register", post(handle_register))
        .route("/health", axum::routing::get(|| async { "ok" }))
        .with_state(state);

    let addr = format!("{}:{}", cfg.host, cfg.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(addr = %addr, "HTTP registry discovery server listening");

    let handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "HTTP registry server error");
        }
    });

    Ok(handle)
}

async fn handle_register(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterRequest>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    tracing::info!(
        url = %req.url,
        mode = %req.mode,
        bootstrap_port = ?req.bootstrap_port,
        "HTTP registry: worker registered"
    );

    let spec = req.into_spec();
    let worker_url = spec.url.clone();
    let event = DiscoveryEvent::Added(spec);
    if state.tx.send(event).await.is_err() {
        return Err((
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "discovery channel closed".to_string(),
        ));
    }

    Ok(Json(serde_json::json!({
        "status": "registered",
        "worker_url": worker_url
    })))
}
