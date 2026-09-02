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

use std::collections::BTreeSet;
#[cfg(feature = "write-tools")]
use std::sync::Arc;

use rmcp::model::CallToolRequestParams;
use rmcp::model::CallToolResponse;
use rmcp::model::CallToolResult;
use rmcp::model::ContentBlock;
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
use crate::guard::MutationGuard;
use crate::model::ClusterName;
use crate::model::ControlCapabilities;
use crate::model::ControlOperation;
use crate::model::Principal;
use crate::tools::UPSERT_CONSUMER_GROUP_TOOL;
use crate::tools::UPSERT_TOPIC_TOOL;

pub const CAPABILITY_RESOURCE_URI: &str = "rocketmq-control://capabilities";

#[derive(Clone)]
pub struct ControlServer {
    catalog: OperationCatalog,
    capabilities: ControlCapabilities,
    guard: MutationGuard,
    configured_clusters: BTreeSet<ClusterName>,
    #[cfg(feature = "write-tools")]
    tool_runtime: Option<Arc<crate::tool_runtime::ToolRuntime>>,
    #[cfg(test)]
    response_delay: Option<std::time::Duration>,
}

impl ControlServer {
    pub fn new(mutations_runtime_enabled: bool) -> Self {
        let policy = crate::config::MutationPolicyConfig {
            mutations_enabled: mutations_runtime_enabled,
            ..Default::default()
        };
        let catalog = OperationCatalog::from_policy(&policy);
        let capabilities = ControlCapabilities::from_catalog(mutations_runtime_enabled, &catalog);
        Self {
            catalog,
            capabilities,
            guard: MutationGuard::new(&policy),
            configured_clusters: BTreeSet::new(),
            #[cfg(feature = "write-tools")]
            tool_runtime: None,
            #[cfg(test)]
            response_delay: None,
        }
    }

    #[cfg(feature = "write-tools")]
    pub(crate) fn from_config(
        config: &crate::config::ControlConfig,
        audit: crate::audit::AuditTrail,
        service_context: rocketmq_runtime::ChildServiceContext,
    ) -> Result<Self, crate::error::ControlError> {
        let catalog = OperationCatalog::from_policy(&config.mutations);
        let capabilities = ControlCapabilities::from_catalog(config.mutations.mutations_enabled, &catalog);
        let guard = MutationGuard::new(&config.mutations);
        let configured_clusters = config
            .mutation_clusters()
            .iter()
            .map(|cluster| cluster.name().clone())
            .collect();
        let tool_runtime = if catalog.registered_operations() == 0 {
            None
        } else {
            let factory = Arc::new(crate::tool_runtime::AdminUpsertFactory::new(
                service_context.component("admin-factory"),
                config.mutation_clusters(),
            )?);
            Some(Arc::new(crate::tool_runtime::ToolRuntime::new(
                audit,
                factory,
                std::time::Duration::from_secs(config.mutations.operation_timeout_seconds),
                service_context.task_group().clone(),
            )))
        };
        Ok(Self {
            catalog,
            capabilities,
            guard,
            configured_clusters,
            tool_runtime,
            #[cfg(test)]
            response_delay: None,
        })
    }

    #[cfg(all(test, feature = "write-tools"))]
    pub(crate) fn with_test_factory(
        policy: &crate::config::MutationPolicyConfig,
        configured_clusters: BTreeSet<ClusterName>,
        audit: crate::audit::AuditTrail,
        factory: Arc<dyn crate::tool_runtime::UpsertSessionFactory>,
        owner: rocketmq_runtime::TaskGroup,
    ) -> Self {
        let catalog = OperationCatalog::from_policy(policy);
        let capabilities = ControlCapabilities::from_catalog(policy.mutations_enabled, &catalog);
        let tool_runtime = crate::tool_runtime::ToolRuntime::new(
            audit,
            factory,
            std::time::Duration::from_secs(policy.operation_timeout_seconds),
            owner,
        );
        Self {
            catalog,
            capabilities,
            guard: MutationGuard::new(policy),
            configured_clusters,
            tool_runtime: Some(Arc::new(tool_runtime)),
            response_delay: None,
        }
    }

    pub const fn capabilities(&self) -> &ControlCapabilities {
        &self.capabilities
    }

    pub fn tools_list(&self) -> ListToolsResult {
        self.catalog.list_tools()
    }

    fn tools_list_for(&self, principal: &Principal) -> ListToolsResult {
        self.catalog.list_tools_for(|operation| {
            self.guard
                .allows_discovery(principal, operation, &self.configured_clusters)
        })
    }

    #[cfg(test)]
    pub(crate) fn with_response_delay(mut self, delay: std::time::Duration) -> Self {
        self.response_delay = Some(delay);
        self
    }
}

impl std::fmt::Debug for ControlServer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControlServer")
            .field("capabilities", &self.capabilities)
            .field("configured_cluster_count", &self.configured_clusters.len())
            .finish()
    }
}

impl ServerHandler for ControlServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().enable_resources().build())
            .with_server_info(Implementation::new("rocketmq-mcp-control", env!("CARGO_PKG_VERSION")))
            .with_protocol_version(ProtocolVersion::V_2025_11_25)
            .with_instructions(
                "Isolated RocketMQ control server. Reviewed Topic and Consumer Group upserts are available only when compile-time, runtime, server-policy, and principal authorization all intersect.",
            )
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
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        #[cfg(test)]
        if let Some(delay) = self.response_delay {
            tokio::time::sleep(delay).await;
        }
        let principal = principal_from_context(&context)?;
        Ok(self.tools_list_for(&principal))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let principal = principal_from_context(&context)?;
        let raw = request
            .arguments
            .clone()
            .map(serde_json::Value::Object)
            .unwrap_or(serde_json::Value::Null);
        let cluster = raw
            .as_object()
            .and_then(|object| object.get("cluster"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let (operation_name, operation) = match request.name.as_ref() {
            UPSERT_TOPIC_TOOL => (
                ControlOperation::TopicUpsert.as_str(),
                Some(ControlOperation::TopicUpsert),
            ),
            UPSERT_CONSUMER_GROUP_TOOL => (
                ControlOperation::ConsumerGroupUpsert.as_str(),
                Some(ControlOperation::ConsumerGroupUpsert),
            ),
            _ => ("unknown", None),
        };
        let authorized = match self
            .guard
            .authorize_raw(&principal, operation_name, cluster, &self.catalog)
        {
            Ok(authorized) => authorized,
            Err(error) => return Ok(tool_error(error).into()),
        };
        #[cfg(not(feature = "write-tools"))]
        {
            let _ = (authorized, operation, context);
            return Ok(tool_error(crate::error::ControlError::operation_unavailable()).into());
        }
        #[cfg(feature = "write-tools")]
        {
            let dry_run_omitted = raw.as_object().is_some_and(|object| !object.contains_key("dry_run"));
            let upsert = match operation {
                Some(ControlOperation::TopicUpsert) => {
                    let mut args: crate::tools::UpsertTopicArgs = match serde_json::from_value(raw) {
                        Ok(args) => args,
                        Err(_) => return Ok(tool_error(crate::error::ControlError::invalid_arguments()).into()),
                    };
                    if let Err(error) = args.validate(self.guard.default_dry_run(), dry_run_omitted) {
                        return Ok(tool_error(error).into());
                    }
                    args.dry_run = args.effective_dry_run(self.guard.default_dry_run(), dry_run_omitted);
                    crate::tool_runtime::UpsertRequest::Topic(args)
                }
                Some(ControlOperation::ConsumerGroupUpsert) => {
                    let mut args: crate::tools::UpsertConsumerGroupArgs = match serde_json::from_value(raw) {
                        Ok(args) => args,
                        Err(_) => return Ok(tool_error(crate::error::ControlError::invalid_arguments()).into()),
                    };
                    if let Err(error) = args.validate(self.guard.default_dry_run(), dry_run_omitted) {
                        return Ok(tool_error(error).into());
                    }
                    args.dry_run = args.effective_dry_run(self.guard.default_dry_run(), dry_run_omitted);
                    crate::tool_runtime::UpsertRequest::ConsumerGroup(args)
                }
                _ => return Ok(tool_error(crate::error::ControlError::permission_denied()).into()),
            };
            let Some(runtime) = &self.tool_runtime else {
                return Ok(tool_error(crate::error::ControlError::operation_unavailable()).into());
            };
            let result = runtime
                .execute(&principal, &authorized, upsert, context.ct.clone())
                .await;
            Ok(match result {
                Ok(response) => tool_response(response),
                Err(error) => tool_error(error),
            }
            .into())
        }
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

fn principal_from_context(context: &RequestContext<RoleServer>) -> Result<Principal, ErrorData> {
    let parts = context
        .extensions
        .get::<axum::http::request::Parts>()
        .ok_or_else(|| ErrorData::invalid_request("authenticated request context is unavailable", None))?;
    parts
        .extensions
        .get::<Principal>()
        .cloned()
        .ok_or_else(|| ErrorData::invalid_request("authenticated request context is unavailable", None))
}

#[cfg(feature = "write-tools")]
fn tool_response(response: crate::tool_runtime::UpsertResponse) -> CallToolResult {
    let structured = serde_json::to_value(&response).unwrap_or_else(|_| {
        serde_json::to_value(crate::error::ControlError::execution_failed().envelope()).unwrap_or_default()
    });
    let text = serde_json::to_string(&structured).unwrap_or_else(|_| "mutation response unavailable".to_owned());
    let mut result = if response.is_error() {
        CallToolResult::error(vec![ContentBlock::text(text)])
    } else {
        CallToolResult::success(vec![ContentBlock::text(text)])
    };
    result.structured_content = Some(structured);
    result
}

fn tool_error(error: crate::error::ControlError) -> CallToolResult {
    let structured = serde_json::to_value(error.envelope()).unwrap_or_default();
    let text = serde_json::to_string(&structured).unwrap_or_else(|_| "mutation failed".to_owned());
    let mut result = CallToolResult::error(vec![ContentBlock::text(text)]);
    result.structured_content = Some(structured);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_without_reviewed_policy_keep_tools_list_empty() {
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
