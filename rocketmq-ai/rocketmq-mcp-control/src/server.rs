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
use rmcp::model::InitializeRequestParams;
use rmcp::model::InitializeResult;
use rmcp::model::ListResourcesResult;
use rmcp::model::ListToolsResult;
use rmcp::model::PaginatedRequestParams;
use rmcp::model::ProtocolVersion;
use rmcp::model::ReadResourceRequestParams;
use rmcp::model::ReadResourceResponse;
use rmcp::model::ReadResourceResult;
use rmcp::model::Resource;
use rmcp::model::ResourceContents;
use rmcp::model::ServerCapabilities;
use rmcp::model::ServerInfo;
use rmcp::service::RequestContext;
use rmcp::ErrorData;
use rmcp::RoleServer;
use rmcp::ServerHandler;

use crate::catalog::OperationCatalog;
use crate::model::ControlCapabilities;

pub const CAPABILITY_RESOURCE_URI: &str = "rocketmq-control://capabilities";

#[derive(Debug, Clone)]
pub struct ControlServer {
    catalog: OperationCatalog,
    capabilities: ControlCapabilities,
    #[cfg(test)]
    response_delay: Option<std::time::Duration>,
}

impl ControlServer {
    pub fn new(mutations_runtime_enabled: bool) -> Self {
        let catalog = OperationCatalog;
        let capabilities = ControlCapabilities::from_catalog(mutations_runtime_enabled, &catalog);
        Self {
            catalog,
            capabilities,
            #[cfg(test)]
            response_delay: None,
        }
    }

    pub const fn capabilities(&self) -> &ControlCapabilities {
        &self.capabilities
    }

    pub fn tools_list(&self) -> ListToolsResult {
        self.catalog.list_tools()
    }

    #[cfg(test)]
    pub(crate) fn with_response_delay(mut self, delay: std::time::Duration) -> Self {
        self.response_delay = Some(delay);
        self
    }
}

impl ServerHandler for ControlServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().enable_resources().build())
            .with_server_info(Implementation::new("rocketmq-mcp-control", env!("CARGO_PKG_VERSION")))
            .with_protocol_version(ProtocolVersion::V_2025_11_25)
            .with_instructions("Isolated RocketMQ control foundation. No production mutation tools are registered.")
    }

    async fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, ErrorData> {
        if request.protocol_version != ProtocolVersion::V_2025_11_25 {
            return Err(ErrorData::invalid_params("unsupported MCP protocol version", None));
        }
        context.peer.set_peer_info(request);
        Ok(self.get_info())
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        #[cfg(test)]
        if let Some(delay) = self.response_delay {
            tokio::time::sleep(delay).await;
        }
        Ok(self.tools_list())
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        Ok(ListResourcesResult::with_all_items(vec![Resource::new(
            CAPABILITY_RESOURCE_URI,
            "rocketmq_control_capabilities",
        )
        .with_title("RocketMQ control capabilities")
        .with_description("Deny-by-default compile, runtime, and registration state.")
        .with_mime_type("application/json")]))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        if request.uri != CAPABILITY_RESOURCE_URI {
            return Err(ErrorData::resource_not_found("resource is unavailable", None));
        }
        let text = serde_json::to_string_pretty(&self.capabilities)
            .map_err(|_| ErrorData::internal_error("failed to serialize capability resource", None))?;
        Ok(ReadResourceResult::new(vec![
            ResourceContents::text(text, CAPABILITY_RESOURCE_URI).with_mime_type("application/json")
        ])
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_foundation_modes_keep_tools_list_empty() {
        for runtime_enabled in [false, true] {
            let server = ControlServer::new(runtime_enabled);
            assert!(server.tools_list().tools.is_empty());
            assert_eq!(server.capabilities.registered_operations(), 0);
            assert!(!server.capabilities.mutation_supported());
        }
    }

    #[test]
    fn capability_contract_snapshot() {
        let server = ControlServer::new(true);
        #[cfg(not(feature = "write-tools"))]
        insta::assert_json_snapshot!("control_capabilities_default", server.capabilities());
        #[cfg(feature = "write-tools")]
        insta::assert_json_snapshot!("control_capabilities_write_tools", server.capabilities());
    }

    #[test]
    fn protocol_surface_snapshot_has_zero_tools() {
        let server = ControlServer::new(true);
        let surface = serde_json::json!({
            "protocol_version": "2025-11-25",
            "tools": server.tools_list().tools,
            "resources": [CAPABILITY_RESOURCE_URI],
            "capabilities": server.capabilities(),
        });
        #[cfg(not(feature = "write-tools"))]
        insta::assert_json_snapshot!("control_protocol_surface_default", surface);
        #[cfg(feature = "write-tools")]
        insta::assert_json_snapshot!("control_protocol_surface_write_tools", surface);
    }
}
