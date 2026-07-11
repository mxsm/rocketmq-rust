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
use rmcp::model::InitializeRequestParams;
use rmcp::model::InitializeResult;
use rmcp::model::ListPromptsResult;
use rmcp::model::ListResourceTemplatesResult;
use rmcp::model::ListResourcesResult;
use rmcp::model::ListToolsResult;
use rmcp::model::PaginatedRequestParams;
use rmcp::model::ProtocolVersion;
use rmcp::model::ReadResourceRequestParams;
use rmcp::model::ReadResourceResult;
use rmcp::model::ServerCapabilities;
use rmcp::model::ServerInfo;
use rmcp::model::Tool;
use rmcp::service::RequestContext;
use rmcp::ErrorData;
use rmcp::RoleServer;
use rmcp::ServerHandler;
use serde_json::json;

use crate::app::McpApp;
use crate::guard::context::RequestContext as AccessContext;
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
        .with_protocol_version(ProtocolVersion::V_2025_11_25)
        .with_instructions("RocketMQ-Rust MCP server for read-only context, diagnostics, and SRE runbooks.")
    }

    async fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, ErrorData> {
        if request.protocol_version != ProtocolVersion::V_2025_11_25 {
            return Err(ErrorData::invalid_params(
                format!(
                    "unsupported MCP protocol version {}; rocketmq-mcp requires 2025-11-25",
                    request.protocol_version
                ),
                Some(json!({
                    "requested": request.protocol_version,
                    "supported": ["2025-11-25"],
                })),
            ));
        }
        context.peer.set_peer_info(request);
        Ok(self.get_info())
    }

    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        let access = self.access_context(&context);
        resources::registry::list_resources_for(self.app.config(), request.as_ref(), |cluster| {
            self.app.guard().authorize_resource(&access, cluster).is_ok()
        })
    }

    async fn list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        if !self.app.guard().allows_resources(&self.access_context(&context)) {
            return Ok(ListResourceTemplatesResult::with_all_items(Vec::new()));
        }
        resources::registry::list_resource_templates(request.as_ref())
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, ErrorData> {
        let access = self.access_context(&context);
        let resource = crate::resources::uri::RocketmqResourceUri::parse(&request.uri)
            .ok_or_else(|| ErrorData::invalid_params("invalid RocketMQ resource URI", None))?;
        let _guarded_resource = self
            .app
            .guard()
            .begin_resource_read(&access, &resource.cluster)
            .map_err(|error| ErrorData::invalid_params(error.to_string(), None))?;
        let query = self.app.query().as_ref().clone().with_cancellation(context.ct);
        let result = resources::reader::read_resource(&query, &request.uri).await;
        self.app.trace_cache_metrics();
        result
    }

    async fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, ErrorData> {
        if !self.app.guard().allows_resources(&self.access_context(&context)) {
            return Ok(ListPromptsResult::with_all_items(Vec::new()));
        }
        prompts::registry::list_prompts().map_err(|error| ErrorData::internal_error(error.to_string(), None))
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, ErrorData> {
        if !self.app.guard().allows_resources(&self.access_context(&context)) {
            return Err(ErrorData::invalid_params("prompt access is denied", None));
        }
        prompts::renderer::get_prompt(request)
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let access = self.access_context(&context);
        Ok(tools::catalog::list_tools_for(|descriptor| {
            self.app
                .guard()
                .allows_tool(&access, descriptor.name, descriptor.risk_level)
        }))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let query = self.app.query().as_ref().clone().with_cancellation(context.ct.clone());
        let access = self.access_context(&context);
        let result = ToolExecutor::new(query, self.app.guard().clone())
            .with_request_context(access)
            .call_with_request_id(request, &request_id_string(&context.id))
            .await;
        self.app.trace_cache_metrics();
        result
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        tools::catalog::get_tool(name)
    }
}

impl RocketmqMcpServer {
    fn access_context(&self, context: &RequestContext<RoleServer>) -> AccessContext {
        #[cfg(feature = "streamable-http")]
        if let Some(access) = context
            .extensions
            .get::<axum::http::request::Parts>()
            .and_then(|parts| parts.extensions.get::<AccessContext>())
        {
            return access.clone();
        }
        #[cfg(not(feature = "streamable-http"))]
        let _ = context;
        self.app.guard().local_request_context()
    }
}

fn request_id_string(request_id: &rmcp::model::RequestId) -> String {
    match serde_json::to_value(request_id) {
        Ok(serde_json::Value::String(value)) => value,
        Ok(serde_json::Value::Number(value)) => value.to_string(),
        _ => "unknown-request".to_string(),
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
        let app = McpApp::new(McpConfig::load(example_config_path()).unwrap()).unwrap();
        let server = RocketmqMcpServer::new(app);

        let info = server.get_info();

        assert_eq!(info.server_info.name, "rocketmq-mcp");
        assert_eq!(info.server_info.version, "1.0.0");
        assert_eq!(info.protocol_version, ProtocolVersion::V_2025_11_25);
        assert!(info.capabilities.tools.is_some());
        assert!(info.capabilities.resources.is_some());
        assert!(info.capabilities.prompts.is_some());
    }

    #[test]
    fn mcp_protocol_surface_snapshot() {
        let tools = tools::catalog::list_tools()
            .tools
            .into_iter()
            .map(|tool| serde_json::to_value(tool).expect("tool descriptor serializes"))
            .collect::<Vec<_>>();
        let resources = resources::registry::list_resources(&McpConfig::load(example_config_path()).unwrap(), None)
            .unwrap()
            .resources
            .into_iter()
            .map(|resource| serde_json::to_value(resource).expect("resource descriptor serializes"))
            .collect::<Vec<_>>();
        let resource_templates = serde_json::to_value(
            resources::registry::list_resource_templates(None)
                .unwrap()
                .resource_templates,
        )
        .expect("resource templates serialize");
        let prompts = prompts::registry::list_prompts()
            .unwrap()
            .prompts
            .into_iter()
            .map(|prompt| serde_json::to_value(prompt).expect("prompt descriptor serializes"))
            .collect::<Vec<_>>();

        let surface = json!({
            "tools": tools,
            "resources": resources,
            "resource_templates": resource_templates,
            "prompts": prompts,
        });

        #[cfg(not(feature = "change-planning"))]
        insta::assert_json_snapshot!("mcp_protocol_surface", surface);

        #[cfg(feature = "change-planning")]
        insta::assert_json_snapshot!("mcp_protocol_surface_with_change_planning", surface);
    }

    fn example_config_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("mcp.example.toml")
    }
}
