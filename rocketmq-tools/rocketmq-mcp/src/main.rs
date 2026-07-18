#![recursion_limit = "256"]

// Copyright 2026 The RocketMQ Rust Authors
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

use clap::Parser;
use rocketmq_mcp::app::McpApp;
use rocketmq_mcp::config::Args;
use rocketmq_mcp::config::TransportKind;
use rocketmq_mcp::error::McpError;
use rocketmq_mcp::transport;
use rocketmq_runtime::ServiceLifecycle;
use rocketmq_runtime::ServiceLifecycleState;
use rocketmq_runtime::ShutdownReason;

#[tokio::main]
async fn main() -> Result<(), McpError> {
    let args = Args::parse();
    let lifecycle = ServiceLifecycle::from_env("rocketmq-mcp")
        .map_err(|error| McpError::InvalidConfig(format!("invalid MCP lifecycle configuration: {error}")))?;
    let app = McpApp::bootstrap_typed(args).await?;
    if let Err(error) = app.start_lifecycle(&lifecycle).await {
        lifecycle.mark_failed();
        let request = lifecycle.request_shutdown(ShutdownReason::Internal);
        app.shutdown_with_deadline(request.deadline).await.log_if_unhealthy();
        return Err(error);
    }

    tracing::info!(
        server = %app.config().server.name,
        transport = app.transport().as_str(),
        cluster_count = app.config().clusters.len(),
        "{} startup initialized",
        app.config().server.name,
    );

    let result = match app.transport() {
        TransportKind::Stdio => serve_stdio(app.clone(), lifecycle.clone()).await,
        TransportKind::StreamableHttp => serve_streamable_http(app.clone(), lifecycle.clone()).await,
    };
    if result.is_err() {
        lifecycle.mark_failed();
    }
    let lifecycle_failed = lifecycle.state() == ServiceLifecycleState::Failed;
    let shutdown_request = lifecycle
        .shutdown_request()
        .unwrap_or_else(|| lifecycle.request_shutdown(ShutdownReason::Internal));
    let shutdown_report = app.shutdown_with_deadline(shutdown_request.deadline).await;
    shutdown_report.log_if_unhealthy();
    if result.is_ok() && shutdown_report.is_healthy() && !lifecycle_failed {
        lifecycle.mark_stopped();
    } else {
        lifecycle.mark_failed();
    }
    result?;
    if lifecycle_failed {
        return Err(McpError::Infrastructure {
            operation: "complete MCP lifecycle shutdown",
            source: Box::new(std::io::Error::other(
                "MCP lifecycle failed while observing or completing shutdown",
            )),
        });
    }
    if !shutdown_report.is_healthy() {
        return Err(McpError::Infrastructure {
            operation: "shutdown MCP within the shared lifecycle deadline",
            source: Box::new(std::io::Error::other("MCP shutdown report is unhealthy")),
        });
    }

    Ok(())
}

async fn serve_stdio(app: McpApp, lifecycle: ServiceLifecycle) -> Result<(), McpError> {
    lifecycle
        .mark_ready()
        .map_err(|error| McpError::InvalidConfig(format!("failed to publish MCP readiness: {error}")))?;
    tokio::select! {
        result = transport::stdio::serve_typed(app) => result,
        result = lifecycle.wait_for_shutdown_signal() => result.map(|_| ()).map_err(|source| McpError::Infrastructure {
            operation: "wait for MCP lifecycle shutdown",
            source: Box::new(source),
        }),
    }
}

async fn serve_streamable_http(app: McpApp, lifecycle: ServiceLifecycle) -> Result<(), McpError> {
    #[cfg(feature = "streamable-http")]
    {
        transport::streamable_http::serve_typed_with_lifecycle(app, lifecycle).await
    }

    #[cfg(not(feature = "streamable-http"))]
    {
        let _ = (app, lifecycle);
        Err(McpError::FeatureDisabled {
            transport: "streamable-http",
            feature: "streamable-http",
        })
    }
}
