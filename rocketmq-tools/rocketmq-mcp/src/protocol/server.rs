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
use std::time::Instant;
use tracing::Instrument;

use crate::app::McpApp;
use crate::guard::context::RequestContext as AccessContext;
use crate::guard::GuardError;
use crate::prompts;
use crate::resources;
use crate::tools;
use crate::tools::executor::ToolExecutor;
use rocketmq_observability::metrics::mcp::McpErrorKind;
use rocketmq_observability::metrics::mcp::McpOperationKind;
use rocketmq_observability::metrics::mcp::McpOperationOutcome;

#[derive(Debug, Clone)]
pub struct RocketmqMcpServer {
    app: McpApp,
}

struct ResourceSpanRecorder {
    outcome: McpOperationOutcome,
    span: tracing::Span,
}

impl ResourceSpanRecorder {
    fn new(operation: &'static str) -> Self {
        Self {
            outcome: McpOperationOutcome::Failure,
            span: rocketmq_observability::trace::mcp::resource_span(operation),
        }
    }

    fn span(&self) -> tracing::Span {
        self.span.clone()
    }

    fn denied(&mut self) {
        self.outcome = McpOperationOutcome::Denied;
    }

    fn observe_call_result(&mut self, result: &Result<ReadResourceResult, ErrorData>) {
        if self.outcome != McpOperationOutcome::Denied {
            self.outcome = if result.is_ok() {
                McpOperationOutcome::Success
            } else {
                McpOperationOutcome::Failure
            };
        }
    }
}

impl Drop for ResourceSpanRecorder {
    fn drop(&mut self) {
        rocketmq_observability::trace::mcp::record_outcome(&self.span, self.outcome);
    }
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
        let access = self.access_context(&context)?;
        resources::registry::list_resources_for(
            self.app.config(),
            request.as_ref(),
            |cluster| self.app.guard().authorize_resource(&access, cluster).is_ok(),
            self.app.guard().allows_system_resources(&access),
        )
    }

    async fn list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        if !self.app.guard().allows_resources(&self.access_context(&context)?) {
            return Ok(ListResourceTemplatesResult::with_all_items(Vec::new()));
        }
        resources::registry::list_resource_templates(request.as_ref())
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, ErrorData> {
        let operation = crate::resources::uri::RocketmqResourceUri::parse(&request.uri)
            .map(|resource| resource.kind.metric_operation())
            .unwrap_or("invalid_resource_uri");
        let mut span_recorder = ResourceSpanRecorder::new(operation);
        let span = span_recorder.span();
        let result = async {
            let started_at = Instant::now();
            let access = match self.access_context(&context) {
                Ok(access) => access,
                Err(error) => {
                    span_recorder.denied();
                    record_resource_error("resource_access_context", McpErrorKind::PermissionDenied);
                    record_resource_operation("resource_access_context", McpOperationOutcome::Denied, started_at);
                    return Err(error);
                }
            };
            let resource = match crate::resources::uri::RocketmqResourceUri::parse(&request.uri) {
                Some(resource) => resource,
                None => {
                    record_resource_error("invalid_resource_uri", McpErrorKind::InvalidRequest);
                    record_resource_operation("invalid_resource_uri", McpOperationOutcome::Failure, started_at);
                    return Err(ErrorData::invalid_params("invalid RocketMQ resource URI", None));
                }
            };
            let operation = resource.kind.metric_operation();
            let guarded_resource = match resource.cluster() {
                Some(cluster) => self.app.guard().begin_resource_read(&access, cluster, &request.uri),
                None => self.app.guard().begin_system_resource_read(&access, &request.uri),
            };
            let guarded_resource = match guarded_resource {
                Ok(guarded_resource) => guarded_resource,
                Err(error) => {
                    span_recorder.denied();
                    record_resource_error(operation, guard_error_metric_kind(&error));
                    record_resource_operation(operation, McpOperationOutcome::Denied, started_at);
                    return Err(resource_guard_error(error, &request_id_string(&context.id)));
                }
            };
            let result = match &resource.kind {
                crate::resources::uri::ResourceKind::Capabilities => {
                    let cluster = resource
                        .cluster()
                        .ok_or_else(|| ErrorData::invalid_params("capability resource requires a cluster", None))?;
                    let descriptors = crate::tools::catalog::ToolId::ALL
                        .iter()
                        .map(|tool| tool.descriptor())
                        .filter(|descriptor| {
                            self.app
                                .guard()
                                .allows_tool(&access, descriptor.name, descriptor.risk_level)
                        });
                    resources::capability::read_result(
                        &request.uri,
                        resources::capability::manifest_for(
                            cluster,
                            descriptors,
                            self.app.guard().allows_system_resources(&access),
                        ),
                    )
                }
                crate::resources::uri::ResourceKind::SystemRuntimeV1 => {
                    resources::system::read_result(&request.uri, "runtime", self.app.runtime_diagnostics_view())
                }
                crate::resources::uri::ResourceKind::SystemObservabilityV1 => {
                    resources::system::read_result(&request.uri, "observability", self.app.observability_status_view())
                }
                _ => {
                    let query = self.app.query().as_ref().clone().with_cancellation(context.ct);
                    resources::reader::read_resource(&query, &request.uri).await
                }
            };
            self.app.trace_cache_metrics();
            let result = guarded_resource.finish_result(result);
            let outcome = if let Err(error) = &result {
                record_resource_error(operation, resource_error_metric_kind(error));
                McpOperationOutcome::Failure
            } else {
                McpOperationOutcome::Success
            };
            record_resource_operation(operation, outcome, started_at);
            result
        }
        .instrument(span)
        .await;
        span_recorder.observe_call_result(&result);
        result
    }

    async fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, ErrorData> {
        if !self.app.guard().allows_resources(&self.access_context(&context)?) {
            return Ok(ListPromptsResult::with_all_items(Vec::new()));
        }
        prompts::registry::list_prompts().map_err(|error| ErrorData::internal_error(error.to_string(), None))
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, ErrorData> {
        if !self.app.guard().allows_resources(&self.access_context(&context)?) {
            return Err(ErrorData::invalid_params("prompt access is denied", None));
        }
        prompts::renderer::get_prompt(request)
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let access = self.access_context(&context)?;
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
        let access = self.access_context(&context)?;
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
    fn access_context(&self, context: &RequestContext<RoleServer>) -> Result<AccessContext, ErrorData> {
        #[cfg(feature = "streamable-http")]
        if let Some(parts) = context.extensions.get::<axum::http::request::Parts>() {
            return parts
                .extensions
                .get::<AccessContext>()
                .cloned()
                .ok_or_else(|| ErrorData::invalid_request("authenticated HTTP context is unavailable", None));
        }
        #[cfg(not(feature = "streamable-http"))]
        let _ = context;
        Ok(self.app.guard().local_request_context())
    }
}

fn request_id_string(request_id: &rmcp::model::RequestId) -> String {
    match serde_json::to_value(request_id) {
        Ok(serde_json::Value::String(value)) => value,
        Ok(serde_json::Value::Number(value)) => value.to_string(),
        _ => "unknown-request".to_string(),
    }
}

fn resource_guard_error(error: GuardError, correlation_id: &str) -> ErrorData {
    let message = match &error {
        GuardError::InvalidArgument(_) => "invalid resource request",
        GuardError::RateLimited(_) => "resource access is rate limited",
        GuardError::ChangePlanningDisabled(_) => "resource capability is disabled",
        GuardError::PermissionDenied(_)
        | GuardError::UnauthorizedScope(_)
        | GuardError::TenantMismatch(_)
        | GuardError::ClusterNotAllowed(_) => "resource access denied",
    };
    ErrorData::invalid_params(
        message,
        Some(json!({
            "code": error.code(),
            "retryable": error.retryable(),
            "correlation_id": correlation_id,
        })),
    )
}

fn guard_error_metric_kind(error: &GuardError) -> McpErrorKind {
    match error {
        GuardError::InvalidArgument(_) => McpErrorKind::InvalidRequest,
        GuardError::RateLimited(_) => McpErrorKind::RateLimited,
        GuardError::PermissionDenied(_)
        | GuardError::UnauthorizedScope(_)
        | GuardError::TenantMismatch(_)
        | GuardError::ClusterNotAllowed(_)
        | GuardError::ChangePlanningDisabled(_) => McpErrorKind::PermissionDenied,
    }
}

fn resource_error_metric_kind(error: &ErrorData) -> McpErrorKind {
    let code = error
        .data
        .as_ref()
        .and_then(|data| data.get("code"))
        .and_then(serde_json::Value::as_str);
    match code {
        Some("permission_denied" | "unauthorized_scope" | "tenant_mismatch" | "cluster_not_allowed") => {
            McpErrorKind::PermissionDenied
        }
        Some("resource_rate_limited") => McpErrorKind::RateLimited,
        Some("source_unavailable" | "resource_query_timeout" | "resource_query_cancelled") => {
            McpErrorKind::SourceUnavailable
        }
        Some("output_too_large") => McpErrorKind::OutputTooLarge,
        Some("invalid_arguments" | "resource_not_found") => McpErrorKind::InvalidRequest,
        _ => McpErrorKind::Internal,
    }
}

fn record_resource_error(operation: &'static str, error: McpErrorKind) {
    rocketmq_observability::metrics::mcp::record_error(McpOperationKind::Resource, operation, error);
}

fn record_resource_operation(operation: &'static str, outcome: McpOperationOutcome, started_at: Instant) {
    rocketmq_observability::metrics::mcp::record_operation(
        McpOperationKind::Resource,
        operation,
        outcome,
        started_at.elapsed(),
    );
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
        let owner =
            rocketmq_runtime::RuntimeOwner::new(rocketmq_runtime::RuntimeConfig::server_default("mcp-protocol-test"))
                .unwrap();
        let app = McpApp::new(
            McpConfig::load(example_config_path()).unwrap(),
            owner.root_context().component("mcp-app"),
            rocketmq_observability::TelemetryHandle::noop(),
        )
        .unwrap();
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
    fn resource_guard_errors_are_stable_correlated_and_sanitized() {
        let error = resource_guard_error(
            GuardError::TenantMismatch("secret tenant details".to_string()),
            "request-7",
        );
        let data = error.data.as_ref().unwrap();

        assert_eq!(data["code"], "tenant_mismatch");
        assert_eq!(data["retryable"], false);
        assert_eq!(data["correlation_id"], "request-7");
        assert!(!error.message.contains("secret tenant details"));
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
