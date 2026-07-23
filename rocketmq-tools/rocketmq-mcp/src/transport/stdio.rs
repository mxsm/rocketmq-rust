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

use rmcp::ServiceExt;

use crate::app::McpApp;
use crate::error::McpError;
use crate::protocol::server::RocketmqMcpServer;

pub async fn serve_typed(app: McpApp) -> Result<(), McpError> {
    let server = RocketmqMcpServer::new(app);
    let service = server
        .serve(rmcp::transport::stdio())
        .await
        .map_err(|source| McpError::infrastructure("start MCP stdio service", source))?;
    service
        .waiting()
        .await
        .map_err(|source| McpError::infrastructure("wait for MCP stdio service", source))?;
    Ok(())
}

/// Serves stdio until the transport closes or the process lifecycle requests shutdown.
///
/// The RMCP service is explicitly cancelled and awaited before application-owned runtime
/// shutdown begins. Tokio's stdin reader itself may still have an uncancellable blocking read;
/// the binary's bounded top-level runtime teardown owns that final process-level boundary.
///
/// # Errors
///
/// Returns an infrastructure error when the stdio service cannot start, the lifecycle signal
/// cannot be observed, the service task fails, or transport cleanup exceeds the shared shutdown
/// deadline.
pub async fn serve_typed_with_lifecycle(
    app: McpApp,
    lifecycle: rocketmq_runtime::ServiceLifecycle,
) -> Result<(), McpError> {
    let server = RocketmqMcpServer::new(app);
    let service = server
        .serve(rmcp::transport::stdio())
        .await
        .map_err(|source| McpError::infrastructure("start MCP stdio service", source))?;
    let cancellation = service.cancellation_token();
    let waiting = service.waiting();
    tokio::pin!(waiting);

    let shutdown_request = tokio::select! {
        result = &mut waiting => {
            result.map_err(|source| McpError::infrastructure("wait for MCP stdio service", source))?;
            return Ok(());
        }
        result = lifecycle.wait_for_shutdown_signal() => {
            result.map_err(|source| McpError::infrastructure("wait for MCP lifecycle shutdown", source))?
        }
    };

    cancellation.cancel();
    match tokio::time::timeout(shutdown_request.deadline.remaining(), &mut waiting).await {
        Ok(result) => {
            result.map_err(|source| McpError::infrastructure("stop MCP stdio service", source))?;
            Ok(())
        }
        Err(source) => Err(McpError::infrastructure(
            "stop MCP stdio service before shutdown deadline",
            source,
        )),
    }
}

#[deprecated(since = "1.0.0", note = "use serve_typed")]
pub async fn serve(app: McpApp) -> anyhow::Result<()> {
    serve_typed(app).await.map_err(anyhow::Error::new)
}
