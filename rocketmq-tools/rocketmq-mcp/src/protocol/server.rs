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

use rmcp::model::CallToolRequestParams;
use rmcp::model::CallToolResult;
use rmcp::model::GetPromptRequestParams;
use rmcp::model::GetPromptResult;
use rmcp::model::Implementation;
use rmcp::model::ListPromptsResult;
use rmcp::model::ListResourceTemplatesResult;
use rmcp::model::ListResourcesResult;
use rmcp::model::ListToolsResult;
use rmcp::model::PaginatedRequestParams;
use rmcp::model::ReadResourceRequestParams;
use rmcp::model::ReadResourceResult;
use rmcp::model::ServerCapabilities;
use rmcp::model::ServerInfo;
use rmcp::model::Tool;
use rmcp::service::RequestContext;
use rmcp::ErrorData;
use rmcp::RoleServer;
use rmcp::ServerHandler;

use crate::adapter::admin_core_adapter::AdminCoreAdapter;
use crate::app::McpApp;
use crate::prompts;
use crate::resources;
use crate::tools;
use crate::tools::executor::ToolExecutor;

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

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        Ok(resources::registry::list_resources())
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        Ok(resources::registry::list_resource_templates())
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, ErrorData> {
        resources::reader::read_resource(self.app.config(), &request.uri)
    }

    async fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, ErrorData> {
        prompts::registry::list_prompts().map_err(|error| ErrorData::internal_error(error.to_string(), None))
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, ErrorData> {
        prompts::renderer::get_prompt(request)
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(tools::registry::list_tools())
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        ToolExecutor::new(
            AdminCoreAdapter::new(self.app.config().clone()),
            self.app.guard().clone(),
        )
        .call(request)
        .await
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        tools::registry::get_tool(name)
    }
}

#[cfg(test)]
mod tests {
    use rmcp::ServerHandler;
    use serde_json::json;

    use super::*;
    use crate::app::McpApp;
    use crate::config::McpConfig;
    use crate::prompts;
    use crate::resources;
    use crate::tools;

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

    #[test]
    fn mcp_protocol_surface_snapshot() {
        let tools = tools::registry::list_tools()
            .tools
            .into_iter()
            .map(|tool| {
                let annotations = tool.annotations.expect("tool annotations");
                json!({
                    "name": tool.name.as_ref(),
                    "has_input_schema": tool.input_schema.get("type").is_some(),
                    "has_output_schema": tool.output_schema.is_some(),
                    "read_only": annotations.read_only_hint,
                    "destructive": annotations.destructive_hint,
                })
            })
            .collect::<Vec<_>>();
        let resources = resources::registry::list_resources()
            .resources
            .into_iter()
            .map(|resource| {
                json!({
                    "uri": resource.uri,
                    "name": resource.name,
                    "mime_type": resource.mime_type,
                })
            })
            .collect::<Vec<_>>();
        let resource_templates = resources::registry::list_resource_templates().resource_templates;
        let prompts = prompts::registry::list_prompts()
            .unwrap()
            .prompts
            .into_iter()
            .map(|prompt| {
                json!({
                    "name": prompt.name,
                    "argument_count": prompt.arguments.as_ref().map(Vec::len).unwrap_or_default(),
                })
            })
            .collect::<Vec<_>>();

        insta::assert_json_snapshot!(
            "mcp_protocol_surface",
            json!({
                "tools": tools,
                "resources": resources,
                "resource_templates": resource_templates,
                "prompts": prompts,
            })
        );
    }

    fn example_config_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("mcp.example.toml")
    }
}
