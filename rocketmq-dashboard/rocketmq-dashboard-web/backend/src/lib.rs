// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#![recursion_limit = "256"]

pub mod admin;
pub mod api;
pub mod config;
pub mod error;
pub mod middleware;
pub mod model;
pub mod service;
pub mod state;

use crate::api::build_router;
use crate::config::AppConfig;
use crate::state::AppState;
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::Instant;

const APPLICATION_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn run(config: AppConfig) -> anyhow::Result<()> {
    let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.port).parse()?;
    let state = AppState::try_new(config).await?;
    let admin_client = state.admin_client.clone();
    let app = build_router(state);
    let listener = TcpListener::bind(addr).await?;

    tracing::info!("RocketMQ Dashboard Web backend listening on http://{addr}");
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown_receiver.await;
        })
        .into_future();
    tokio::pin!(server);

    tokio::select! {
        server_result = server.as_mut() => {
            tokio::time::timeout(APPLICATION_SHUTDOWN_TIMEOUT, admin_client.shutdown())
                .await
                .map_err(|_| anyhow::anyhow!("timed out while shutting down the dashboard admin session"))?;
            server_result?;
        }
        () = shutdown_signal() => {
            let deadline = Instant::now() + APPLICATION_SHUTDOWN_TIMEOUT;
            let _ = shutdown_sender.send(());
            let shutdown_result = tokio::time::timeout_at(deadline, async {
                let server_result = server.as_mut().await;
                admin_client.shutdown().await;
                server_result
            })
            .await;
            match shutdown_result {
                Ok(server_result) => server_result?,
                Err(_) => {
                    return Err(anyhow::anyhow!(
                        "timed out while draining requests and shutting down the dashboard admin session"
                    ));
                }
            }
        }
    }

    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::SignalKind;
        use tokio::signal::unix::signal;

        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    () = wait_for_ctrl_c() => {},
                    _ = terminate.recv() => {},
                }
            }
            Err(error) => {
                tracing::warn!(%error, "failed to install SIGTERM shutdown signal");
                wait_for_ctrl_c().await;
            }
        }
    }

    #[cfg(not(unix))]
    wait_for_ctrl_c().await;
}

async fn wait_for_ctrl_c() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(%error, "failed to install Ctrl-C shutdown signal");
        std::future::pending::<()>().await;
    }
}
