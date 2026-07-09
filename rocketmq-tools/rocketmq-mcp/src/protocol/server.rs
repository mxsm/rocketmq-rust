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

use rmcp::model::Implementation;
use rmcp::model::ServerCapabilities;
use rmcp::model::ServerInfo;
use rmcp::ServerHandler;

use crate::app::McpApp;

#[derive(Debug, Clone)]
pub struct RocketmqMcpServer {
    app: McpApp,
}

impl RocketmqMcpServer {
    pub fn new(app: McpApp) -> Self {
        Self { app }
    }

    pub fn app(&self) -> &McpApp {
        &self.app
    }
}

impl ServerHandler for RocketmqMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_prompts()
                .build(),
        )
        .with_server_info(Implementation::new(
            self.app.config().server.name.clone(),
            self.app.config().server.version.clone(),
        ))
        .with_instructions("RocketMQ-Rust MCP server for read-only context, diagnostics, and SRE runbooks.")
    }
}

#[cfg(test)]
mod tests {
    use rmcp::ServerHandler;

    use super::*;
    use crate::app::McpApp;
    use crate::config::McpConfig;

    #[test]
    fn server_info_declares_mvp_capabilities() {
        let app = McpApp::new(McpConfig::load(example_config_path()).unwrap());
        let server = RocketmqMcpServer::new(app);

        let info = server.get_info();

        assert_eq!(info.server_info.name, "rocketmq-mcp");
        assert_eq!(info.server_info.version, "1.0.0");
        assert!(info.capabilities.tools.is_some());
        assert!(info.capabilities.resources.is_some());
        assert!(info.capabilities.prompts.is_some());
    }

    fn example_config_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("mcp.example.toml")
    }
}
