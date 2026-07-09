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
use rocketmq_mcp::transport;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let app = McpApp::bootstrap(args).await?;

    tracing::info!(
        server = %app.config().server.name,
        transport = app.transport().as_str(),
        cluster_count = app.config().clusters.len(),
        "{} startup initialized",
        app.config().server.name,
    );

    match app.transport() {
        TransportKind::Stdio => transport::stdio::serve(app).await?,
        TransportKind::StreamableHttp => anyhow::bail!("streamable-http transport is not implemented yet"),
    }

    Ok(())
}
