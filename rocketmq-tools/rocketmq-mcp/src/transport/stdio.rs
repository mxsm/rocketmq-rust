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

#[deprecated(since = "1.0.0", note = "use serve_typed")]
pub async fn serve(app: McpApp) -> anyhow::Result<()> {
    serve_typed(app).await.map_err(anyhow::Error::new)
}
