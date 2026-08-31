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
use rmcp::model::CallToolResponse;
use rmcp::model::GetPromptRequestParams;
use rmcp::model::GetPromptResponse;
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
use rmcp::model::ReadResourceResponse;
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

use crate::adapter::admin_session::AdminCoreSessionFactory;
use crate::adapter::query_facade::QueryFacade;
use crate::app::McpApp;
use crate::guard::context::RequestContext as AccessContext;
use crate::guard::GuardError;
use crate::prompts;
use crate::resources;
use crate::tools;
use crate::tools::executor::ToolExecutor;
use rocketmq_observability::metrics::mcp::McpErrorKind;
use rocketmq_observability::metrics::mcp::McpMetricsRecorder;
use rocketmq_observability::metrics::mcp::McpOperationKind;
use rocketmq_observability::metrics::mcp::McpOperationOutcome;

#[derive(Debug, Clone)]
pub struct RocketmqMcpServer {
    app: McpApp,
}

struct ResourceSpanRecorder {
    metrics: McpMetricsRecorder,
    operation: &'static str,
    started_at: Instant,
    outcome: McpOperationOutcome,
    span: tracing::Span,
}

impl ResourceSpanRecorder {
    fn new(metrics: McpMetricsRecorder, operation: &'static str) -> Self {
        Self {
            metrics,
            operation,
            started_at: Instant::now(),
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
        self.metrics.record_operation(
            McpOperationKind::Resource,
            self.operation,
            self.outcome,
            self.started_at.elapsed(),
        );
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
    ) -> Result<ReadResourceResponse, ErrorData> {
        let operation = crate::resources::uri::RocketmqResourceUri::parse(&request.uri)
            .map(|resource| resource.kind.metric_operation())
            .unwrap_or("invalid_resource_uri");
        let mut span_recorder = ResourceSpanRecorder::new(self.app.metrics().clone(), operation);
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
                    let query = self.request_query(&access, context.ct);
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
        result.map(Into::into)
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
    ) -> Result<GetPromptResponse, ErrorData> {
        if !self.app.guard().allows_resources(&self.access_context(&context)?) {
            return Err(ErrorData::invalid_params("prompt access is denied", None));
        }
        prompts::renderer::get_prompt(request).map(Into::into)
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
    ) -> Result<CallToolResponse, ErrorData> {
        let access = self.access_context(&context)?;
        let query = self.request_query(&access, context.ct.clone());
        let result = ToolExecutor::new(query, self.app.guard().clone())
            .with_metrics(self.app.metrics().clone())
            .with_request_context(access)
            .call_with_request_id(request, &request_id_string(&context.id))
            .await;
        self.app.trace_cache_metrics();
        result.map(Into::into)
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        tools::catalog::get_tool(name)
    }
}

impl RocketmqMcpServer {
    fn request_query(
        &self,
        access: &AccessContext,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> QueryFacade<AdminCoreSessionFactory> {
        self.app
            .query()
            .as_ref()
            .clone()
            .with_visibility_class(access.visibility_class())
            .with_cancellation(cancellation)
    }

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
    use std::collections::BTreeSet;
    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    use std::sync::atomic::Ordering;
    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    use std::sync::Arc;

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    use rmcp::model::NumberOrString;
    use rmcp::ServerHandler;
    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    use rmcp::ServiceExt;
    use serde_json::json;

    use super::*;
    use crate::app::McpApp;
    use crate::config::McpConfig;
    use crate::guard::context::Principal;
    use crate::guard::context::VisibilityClass;
    use crate::prompts;
    use crate::resources;
    use crate::tools;

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    use crate::adapter::admin_session::ProtocolTestCounters;
    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    use crate::adapter::admin_session::ProtocolTestGate;
    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    use crate::adapter::admin_session::ProtocolTestSessionFactory;

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
    fn request_query_unit_binds_closed_visibility_without_retaining_identity() {
        let owner =
            rocketmq_runtime::RuntimeOwner::new(rocketmq_runtime::RuntimeConfig::server_default("mcp-visibility-test"))
                .unwrap();
        let app = McpApp::new(
            McpConfig::load(example_config_path()).unwrap(),
            owner.root_context().component("mcp-app"),
            rocketmq_observability::TelemetryHandle::noop(),
        )
        .unwrap();
        let server = RocketmqMcpServer::new(app);
        let access = |scope: &str| AccessContext {
            principal: Principal {
                id: "private-principal-sentinel".to_string(),
                tenant: Some("private-tenant-sentinel".to_string()),
                roles: ["private-role-sentinel".to_string()].into_iter().collect(),
                scopes: [scope.to_string()].into_iter().collect(),
                allowed_clusters: Some(BTreeSet::from(["local-dev".to_string()])),
            },
            client: Some("private-client-sentinel".to_string()),
        };

        let standard = server.request_query(&access("rocketmq:read"), tokio_util::sync::CancellationToken::new());
        let sensitive = server.request_query(&access("rocketmq:diagnose"), tokio_util::sync::CancellationToken::new());

        assert_eq!(standard.visibility_class(), VisibilityClass::Standard);
        assert_eq!(sensitive.visibility_class(), VisibilityClass::Sensitive);
        for debug in [format!("{standard:?}"), format!("{sensitive:?}")] {
            assert!(!debug.contains("private-principal-sentinel"));
            assert!(!debug.contains("private-tenant-sentinel"));
            assert!(!debug.contains("private-role-sentinel"));
            assert!(!debug.contains("private-client-sentinel"));
        }
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn real_handlers_bind_http_and_stdio_visibility_and_isolate_query_state() {
        let (_owner, server, counters) = test_server("diagnose", None);
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();

        let standard = oauth_context(&peer, 1, "oauth-read", ["read_only"], ["rocketmq:read"]);
        let standard_tool = complete_tool_response(
            running
                .service()
                .call_tool(list_topics_request(Some(1), None), standard)
                .await
                .unwrap(),
        );
        assert_eq!(standard_tool.is_error, Some(false));
        let cursor = standard_tool.structured_content.as_ref().unwrap()["data"]["next_cursor"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 1);

        let standard_resource = oauth_context(&peer, 2, "other-oauth-read", ["read_only"], ["rocketmq:read"]);
        running
            .service()
            .read_resource(
                ReadResourceRequestParams::new("rocketmq://clusters/local-dev/topics?limit=1"),
                standard_resource,
            )
            .await
            .unwrap();
        assert_eq!(
            counters.topic_inventory_queries.load(Ordering::SeqCst),
            1,
            "same-class Tool and Resource must share the retained snapshot"
        );

        let sensitive = oauth_context(
            &peer,
            3,
            "oauth-diagnose",
            ["diagnose"],
            ["rocketmq:read", "rocketmq:diagnose"],
        );
        running
            .service()
            .read_resource(
                ReadResourceRequestParams::new("rocketmq://clusters/local-dev/topics?limit=1"),
                sensitive,
            )
            .await
            .unwrap();
        assert_eq!(
            counters.topic_inventory_queries.load(Ordering::SeqCst),
            2,
            "standard and sensitive snapshots must not share"
        );

        let before_cursor_replay = counters.starts.load(Ordering::SeqCst);
        let replay = complete_tool_response(
            running
                .service()
                .call_tool(
                    list_topics_request(Some(1), Some(cursor)),
                    oauth_context(
                        &peer,
                        4,
                        "oauth-diagnose",
                        ["diagnose"],
                        ["rocketmq:read", "rocketmq:diagnose"],
                    ),
                )
                .await
                .unwrap(),
        );
        assert_eq!(replay.is_error, Some(true));
        assert_eq!(
            counters.starts.load(Ordering::SeqCst),
            before_cursor_replay,
            "cross-class cursor rejection must not start an admin session"
        );

        let stdio_diagnose = local_context(&peer, 5);
        running
            .service()
            .read_resource(
                ReadResourceRequestParams::new("rocketmq://clusters/local-dev/topics?limit=1"),
                stdio_diagnose,
            )
            .await
            .unwrap();
        assert_eq!(
            counters.topic_inventory_queries.load(Ordering::SeqCst),
            2,
            "local diagnose must share the sensitive class"
        );

        let overview = || {
            CallToolRequestParams::new("rocketmq_get_cluster_overview")
                .with_arguments(json!({"cluster": "local-dev"}).as_object().unwrap().clone())
        };
        running
            .service()
            .call_tool(
                overview(),
                oauth_context(&peer, 6, "oauth-read", ["read_only"], ["rocketmq:read"]),
            )
            .await
            .unwrap();
        running
            .service()
            .call_tool(
                overview(),
                oauth_context(
                    &peer,
                    7,
                    "oauth-diagnose",
                    ["diagnose"],
                    ["rocketmq:read", "rocketmq:diagnose"],
                ),
            )
            .await
            .unwrap();
        running
            .service()
            .call_tool(
                overview(),
                oauth_context(&peer, 8, "another-read", ["read_only"], ["rocketmq:read"]),
            )
            .await
            .unwrap();
        assert_eq!(
            counters.broker_queries.load(Ordering::SeqCst),
            2,
            "ordinary cache entries must share within, but not across, visibility classes"
        );

        drop(running);
        drop(client);

        let (_owner, read_only_server, read_only_counters) = test_server("read_only", None);
        let (read_only_running, read_only_client) = connected_test_server(read_only_server).await;
        let read_only_peer = read_only_running.peer().clone();

        read_only_running
            .service()
            .call_tool(
                list_topics_request(None, None),
                oauth_context(&read_only_peer, 9, "oauth-read", ["read_only"], ["rocketmq:read"]),
            )
            .await
            .unwrap();
        read_only_running
            .service()
            .read_resource(
                ReadResourceRequestParams::new("rocketmq://clusters/local-dev/topics"),
                local_context(&read_only_peer, 10),
            )
            .await
            .unwrap();
        assert_eq!(
            read_only_counters.topic_inventory_queries.load(Ordering::SeqCst),
            1,
            "local read-only must use the standard HTTP read class"
        );

        drop(read_only_running);
        drop(read_only_client);
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn real_handlers_map_local_operator_and_reject_unauthorized_before_sessions() {
        let (_owner, server, counters) = test_server("operator", None);
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();

        running
            .service()
            .call_tool(
                list_topics_request(None, None),
                oauth_context(&peer, 1, "oauth-read", ["read_only"], ["rocketmq:read"]),
            )
            .await
            .unwrap();
        running
            .service()
            .call_tool(list_topics_request(None, None), local_context(&peer, 2))
            .await
            .unwrap();
        assert_eq!(
            counters.topic_inventory_queries.load(Ordering::SeqCst),
            2,
            "local operator must use sensitive rather than the HTTP read class"
        );

        drop(running);
        drop(client);

        let (_owner, denied_server, denied_counters) = test_server("read_only", None);
        let (denied_running, denied_client) = connected_test_server(denied_server).await;
        let denied_peer = denied_running.peer().clone();
        let diagnose = CallToolRequestParams::new("rocketmq_diagnose_consumer_lag").with_arguments(
            json!({"cluster": "local-dev", "topic": "orders", "consumer_group": "orders-service"})
                .as_object()
                .unwrap()
                .clone(),
        );
        let denied = complete_tool_response(
            denied_running
                .service()
                .call_tool(diagnose, local_context(&denied_peer, 3))
                .await
                .unwrap(),
        );
        assert_eq!(denied.is_error, Some(true));
        let unknown = denied_running
            .service()
            .call_tool(
                CallToolRequestParams::new("rocketmq_unknown_query"),
                local_context(&denied_peer, 4),
            )
            .await;
        assert!(unknown.is_err());
        let no_scope = oauth_context(&denied_peer, 5, "oauth-no-scope", ["diagnose"], []);
        let denied_tool = complete_tool_response(
            denied_running
                .service()
                .call_tool(list_topics_request(None, None), no_scope)
                .await
                .unwrap(),
        );
        assert_eq!(denied_tool.is_error, Some(true));
        let unknown_scope = complete_tool_response(
            denied_running
                .service()
                .call_tool(
                    list_topics_request(None, None),
                    oauth_context(
                        &denied_peer,
                        6,
                        "oauth-unknown-scope",
                        ["diagnose"],
                        ["rocketmq:unknown"],
                    ),
                )
                .await
                .unwrap(),
        );
        assert_eq!(unknown_scope.is_error, Some(true));
        let denied_resource = denied_running
            .service()
            .read_resource(
                ReadResourceRequestParams::new("rocketmq://clusters/local-dev/topics"),
                oauth_context(&denied_peer, 7, "oauth-no-scope", ["diagnose"], []),
            )
            .await;
        assert!(denied_resource.is_err());
        assert_eq!(
            denied_counters.starts.load(Ordering::SeqCst),
            0,
            "denied and unknown requests must not reach Admin/session work"
        );
        assert_eq!(denied_counters.broker_queries.load(Ordering::SeqCst), 0);
        assert_eq!(denied_counters.topic_inventory_queries.load(Ordering::SeqCst), 0);
        assert_eq!(denied_counters.shutdowns.load(Ordering::SeqCst), 0);

        drop(denied_running);
        drop(denied_client);
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn real_handler_singleflight_coalesces_within_but_not_across_classes() {
        let gate = Arc::new(ProtocolTestGate::new(2));
        let (_owner, server, counters) = test_server("diagnose", Some(gate.clone()));
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();
        let start = Arc::new(tokio::sync::Barrier::new(5));
        let mut tasks = tokio::task::JoinSet::new();

        for (id, principal, role, scopes) in [
            (1, "read-a", "read_only", vec!["rocketmq:read"]),
            (2, "read-b", "read_only", vec!["rocketmq:read"]),
            (3, "diagnose-a", "diagnose", vec!["rocketmq:read", "rocketmq:diagnose"]),
            (4, "diagnose-b", "diagnose", vec!["rocketmq:read", "rocketmq:diagnose"]),
        ] {
            let server = running.service().clone();
            let peer = peer.clone();
            let start = start.clone();
            tasks.spawn(async move {
                start.wait().await;
                server
                    .call_tool(
                        list_topics_request(None, None),
                        oauth_context(&peer, id, principal, [role], scopes),
                    )
                    .await
                    .unwrap()
            });
        }

        start.wait().await;
        gate.wait_until_entered(2).await;
        gate.release().await;
        while let Some(result) = tasks.join_next().await {
            assert_eq!(complete_tool_response(result.unwrap()).is_error, Some(false));
        }
        assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 2);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 2);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 2);

        drop(running);
        drop(client);
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    fn test_server(
        profile: &str,
        gate: Option<Arc<ProtocolTestGate>>,
    ) -> (
        rocketmq_runtime::RuntimeOwner,
        RocketmqMcpServer,
        Arc<ProtocolTestCounters>,
    ) {
        let owner = rocketmq_runtime::RuntimeOwner::new(rocketmq_runtime::RuntimeConfig::server_default(
            "mcp-real-handler-test",
        ))
        .unwrap();
        let mut config = McpConfig::load(example_config_path()).unwrap();
        config.security.profile = profile.to_string();
        let factory = ProtocolTestSessionFactory::new(gate);
        let counters = factory.counters.clone();
        let app = McpApp::new(
            config,
            owner.root_context().component("mcp-app"),
            rocketmq_observability::TelemetryHandle::noop(),
        )
        .unwrap()
        .with_test_session_factory(factory);
        (owner, RocketmqMcpServer::new(app), counters)
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    async fn connected_test_server(
        server: RocketmqMcpServer,
    ) -> (
        rmcp::service::RunningService<RoleServer, RocketmqMcpServer>,
        tokio::io::DuplexStream,
    ) {
        use tokio::io::AsyncWriteExt;

        const INITIALIZE: &[u8] = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-11-25\",\"capabilities\":{},\"clientInfo\":{\"name\":\"handler-test\",\"version\":\"1.0.0\"}}}\n";
        let (server_transport, mut client_transport) = tokio::io::duplex(64 * 1024);
        let server_task = tokio::spawn(async move { server.serve(server_transport).await });
        client_transport.write_all(INITIALIZE).await.unwrap();
        let server = server_task.await.unwrap().unwrap();
        (server, client_transport)
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    fn oauth_context(
        peer: &rmcp::service::Peer<RoleServer>,
        id: i64,
        principal: &str,
        roles: impl IntoIterator<Item = &'static str>,
        scopes: impl IntoIterator<Item = &'static str>,
    ) -> RequestContext<RoleServer> {
        let request = axum::http::Request::builder().uri("/mcp").body(()).unwrap();
        let (mut parts, _) = request.into_parts();
        parts.extensions.insert(AccessContext {
            principal: Principal {
                id: principal.to_string(),
                tenant: Some("test-tenant".to_string()),
                roles: roles.into_iter().map(str::to_string).collect(),
                scopes: scopes.into_iter().map(str::to_string).collect(),
                allowed_clusters: Some(BTreeSet::from(["local-dev".to_string()])),
            },
            client: Some("oauth-test".to_string()),
        });
        let mut context = RequestContext::new(NumberOrString::Number(id), peer.clone());
        context.extensions.insert(parts);
        context
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    fn local_context(peer: &rmcp::service::Peer<RoleServer>, id: i64) -> RequestContext<RoleServer> {
        RequestContext::new(NumberOrString::Number(id), peer.clone())
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    fn list_topics_request(limit: Option<u32>, cursor: Option<String>) -> CallToolRequestParams {
        let mut arguments = serde_json::Map::new();
        arguments.insert("cluster".to_string(), json!("local-dev"));
        if let Some(limit) = limit {
            arguments.insert("limit".to_string(), json!(limit));
        }
        if let Some(cursor) = cursor {
            arguments.insert("cursor".to_string(), json!(cursor));
        }
        CallToolRequestParams::new("rocketmq_list_topics").with_arguments(arguments)
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    fn complete_tool_response(response: CallToolResponse) -> rmcp::model::CallToolResult {
        match response {
            CallToolResponse::Complete(result) => result,
            response => panic!("expected a completed tool response, got {response:?}"),
        }
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
