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
        let auth_claims = access.canonical_auth_claims();
        self.app.resources().list_resources(
            self.app.config(),
            request.as_ref(),
            &auth_claims,
            |cluster, kind| self.app.guard().allows_resource(&access, cluster, kind),
            self.app.guard().allows_system_resources(&access),
        )
    }

    async fn list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        let access = self.access_context(&context)?;
        let auth_claims = access.canonical_auth_claims();
        self.app.resources().list_resource_templates(
            self.app.config(),
            request.as_ref(),
            &auth_claims,
            |cluster, kind| self.app.guard().allows_resource(&access, cluster, kind),
        )
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
                    self.app
                        .guard()
                        .record_resource_rejection(&access, "resource:unavailable", "invalid_resource_uri");
                    record_resource_error("invalid_resource_uri", McpErrorKind::InvalidRequest);
                    record_resource_operation("invalid_resource_uri", McpOperationOutcome::Failure, started_at);
                    return Err(resource_unavailable(&request_id_string(&context.id)));
                }
            };
            let operation = resource.kind.metric_operation();
            let canonical_uri = resource.as_string();
            let guarded_resource = match resource.cluster() {
                Some(cluster) => self.app.guard().begin_resource_read(&access, cluster, &resource.kind),
                None => self.app.guard().begin_system_resource_read(&access, &resource.kind),
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
                        .filter(|descriptor| self.app.guard().allows_tool_on_cluster(&access, descriptor.id, cluster));
                    resources::capability::read_result(
                        &canonical_uri,
                        resources::capability::manifest_for(
                            cluster,
                            descriptors,
                            self.app.guard().allows_system_resources(&access),
                        ),
                    )
                }
                crate::resources::uri::ResourceKind::SystemRuntimeV1 => {
                    resources::system::read_result(&canonical_uri, "runtime", self.app.runtime_diagnostics_view())
                }
                crate::resources::uri::ResourceKind::SystemObservabilityV1 => resources::system::read_result(
                    &canonical_uri,
                    "observability",
                    self.app.observability_status_view(),
                ),
                _ => {
                    let query = self.request_query(&access, context.ct);
                    resources::reader::read_resource(&query, &canonical_uri).await
                }
            };
            self.app.trace_cache_metrics();
            let result = guarded_resource.finish_result(result).map_err(|error| {
                if error.code == rmcp::model::ErrorCode::RESOURCE_NOT_FOUND {
                    resource_unavailable(&request_id_string(&context.id))
                } else {
                    error
                }
            });
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
        let access = self.access_context(&context)?;
        prompts::registry::list_prompts_for(self.app.config(), |tool, cluster| {
            self.app.guard().allows_tool_on_cluster(&access, tool, cluster)
        })
        .map_err(|error| ErrorData::internal_error(error.to_string(), None))
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResponse, ErrorData> {
        let access = self.access_context(&context)?;
        prompts::renderer::get_prompt_for(request, self.app.config(), |tool, cluster| {
            self.app.guard().allows_tool_on_cluster(&access, tool, cluster)
        })
        .map(Into::into)
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
    let _ = error;
    resource_unavailable(correlation_id)
}

fn resource_unavailable(correlation_id: &str) -> ErrorData {
    ErrorData::invalid_params(
        "resource is unavailable",
        Some(json!({
            "code": "resource_unavailable",
            "retryable": false,
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

        assert_eq!(data["code"], "resource_unavailable");
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
        assert!(standard_tool.structured_content.as_ref().unwrap()["data"]["next_cursor"].is_string());
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

        drop(running);
        drop(client);
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn real_handlers_isolate_topic_snapshots_and_share_sensitive_visibility() {
        let (_owner, server, counters) = test_server("diagnose", None);
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();

        running
            .service()
            .read_resource(
                ReadResourceRequestParams::new("rocketmq://clusters/local-dev/topics?limit=1"),
                oauth_context(&peer, 1, "oauth-read", ["read_only"], ["rocketmq:read"]),
            )
            .await
            .unwrap();

        let sensitive = oauth_context(
            &peer,
            2,
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

        let stdio_diagnose = local_context(&peer, 3);
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

        drop(running);
        drop(client);
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn real_handlers_reject_cross_visibility_cursor_without_session_work() {
        let (_owner, server, counters) = test_server("diagnose", None);
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();
        let standard_tool = complete_tool_response(
            running
                .service()
                .call_tool(
                    list_topics_request(Some(1), None),
                    oauth_context(&peer, 1, "oauth-read", ["read_only"], ["rocketmq:read"]),
                )
                .await
                .unwrap(),
        );
        let cursor = standard_tool.structured_content.as_ref().unwrap()["data"]["next_cursor"]
            .as_str()
            .unwrap()
            .to_string();
        let before_cursor_replay = counters.starts.load(Ordering::SeqCst);
        let replay = complete_tool_response(
            running
                .service()
                .call_tool(
                    list_topics_request(Some(1), Some(cursor)),
                    oauth_context(
                        &peer,
                        2,
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
        drop(running);
        drop(client);
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn real_handlers_isolate_ordinary_cache_by_visibility() {
        let (_owner, server, counters) = test_server("diagnose", None);
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();

        let overview = || {
            CallToolRequestParams::new("rocketmq_get_cluster_overview")
                .with_arguments(json!({"cluster": "local-dev"}).as_object().unwrap().clone())
        };
        running
            .service()
            .call_tool(
                overview(),
                oauth_context(&peer, 1, "oauth-read", ["read_only"], ["rocketmq:read"]),
            )
            .await
            .unwrap();
        running
            .service()
            .call_tool(
                overview(),
                oauth_context(
                    &peer,
                    2,
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
                oauth_context(&peer, 3, "another-read", ["read_only"], ["rocketmq:read"]),
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
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn real_handlers_map_local_read_only_to_standard_visibility() {
        let (_owner, server, counters) = test_server("read_only", None);
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
            .read_resource(
                ReadResourceRequestParams::new("rocketmq://clusters/local-dev/topics"),
                local_context(&peer, 2),
            )
            .await
            .unwrap();
        assert_eq!(
            counters.topic_inventory_queries.load(Ordering::SeqCst),
            1,
            "local read-only must use the standard HTTP read class"
        );

        drop(running);
        drop(client);
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
    async fn broker_resources_enforce_backing_risk_and_share_tool_cache() {
        let (_owner, denied_server, denied_counters) = test_server("read_only", None);
        let (denied_running, denied_client) = connected_test_server(denied_server).await;
        let denied_peer = denied_running.peer().clone();
        let templates = denied_running
            .service()
            .list_resource_templates(None, local_context(&denied_peer, 1))
            .await
            .unwrap()
            .resource_templates
            .into_iter()
            .map(|template| template.name.to_string())
            .collect::<BTreeSet<_>>();
        assert!(!templates.contains("rocketmq_broker_diagnostics"));
        assert!(templates.contains("rocketmq_broker_config_summary"));
        let denied = denied_running
            .service()
            .read_resource(
                ReadResourceRequestParams::new("rocketmq://clusters/local-dev/brokers/broker-a/diagnostics"),
                local_context(&denied_peer, 2),
            )
            .await;
        assert!(denied.is_err());
        assert_eq!(denied_counters.starts.load(Ordering::SeqCst), 0);
        drop(denied_running);
        drop(denied_client);

        let (_owner, server, counters) = test_server("diagnose", None);
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();
        let diagnostics_uri = "rocketmq://clusters/local-dev/brokers/broker-a/diagnostics";
        let config_uri = "rocketmq://clusters/local-dev/brokers/broker-a/config-summary";

        let diagnostics = complete_tool_response(
            running
                .service()
                .call_tool(
                    tool_request(
                        "rocketmq_get_broker_diagnostics",
                        json!({"cluster":"local-dev","broker_name":"broker-a"}),
                    ),
                    local_context(&peer, 3),
                )
                .await
                .unwrap(),
        );
        assert_eq!(diagnostics.is_error, Some(false));
        running
            .service()
            .read_resource(ReadResourceRequestParams::new(diagnostics_uri), local_context(&peer, 4))
            .await
            .unwrap();
        assert_eq!(counters.broker_diagnostics_queries.load(Ordering::SeqCst), 1);

        running
            .service()
            .read_resource(ReadResourceRequestParams::new(config_uri), local_context(&peer, 5))
            .await
            .unwrap();
        let config = complete_tool_response(
            running
                .service()
                .call_tool(
                    tool_request(
                        "rocketmq_get_broker_config_summary",
                        json!({"cluster":"local-dev","broker_name":"broker-a"}),
                    ),
                    local_context(&peer, 6),
                )
                .await
                .unwrap(),
        );
        assert_eq!(config.is_error, Some(false));
        assert_eq!(counters.broker_config_summary_queries.load(Ordering::SeqCst), 1);

        let records = running.service().app().guard().audit_log().records();
        assert_eq!(
            records
                .iter()
                .find(|record| record.tool == "resource:broker_diagnostics")
                .unwrap()
                .risk_level,
            crate::guard::RiskLevel::Diagnose
        );
        assert_eq!(
            records
                .iter()
                .find(|record| record.tool == "resource:broker_config_summary")
                .unwrap()
                .risk_level,
            crate::guard::RiskLevel::ReadOnly
        );
        drop(running);
        drop(client);
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn topic_resources_share_tool_cache_and_cross_surface_cursor() {
        let (_owner, server, counters) = test_server("read_only", None);
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();
        let stats = complete_tool_response(
            running
                .service()
                .call_tool(
                    tool_request(
                        "rocketmq_get_topic_stats",
                        json!({"cluster":"local-dev","topic":"orders","limit":1}),
                    ),
                    local_context(&peer, 1),
                )
                .await
                .unwrap(),
        );
        let cursor = stats.structured_content.as_ref().unwrap()["data"]["next_cursor"]
            .as_str()
            .unwrap()
            .to_string();
        let continuation_uri = format!("rocketmq://clusters/local-dev/topics/orders/stats?limit=1&cursor={cursor}");
        running
            .service()
            .read_resource(
                ReadResourceRequestParams::new(continuation_uri),
                local_context(&peer, 2),
            )
            .await
            .unwrap();
        assert_eq!(counters.topic_stats_queries.load(Ordering::SeqCst), 1);

        running
            .service()
            .read_resource(
                ReadResourceRequestParams::new("rocketmq://clusters/local-dev/topics/orders/config"),
                local_context(&peer, 3),
            )
            .await
            .unwrap();
        let config = complete_tool_response(
            running
                .service()
                .call_tool(
                    tool_request(
                        "rocketmq_get_topic_config",
                        json!({"cluster":"local-dev","topic":"orders"}),
                    ),
                    local_context(&peer, 4),
                )
                .await
                .unwrap(),
        );
        assert_eq!(config.is_error, Some(false));
        assert_eq!(counters.topic_config_queries.load(Ordering::SeqCst), 1);
        drop(running);
        drop(client);
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn consumer_progress_cursor_replays_from_resource_to_tool_without_rpc() {
        let (_owner, server, counters) = test_server("read_only", None);
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();
        let response = running
            .service()
            .read_resource(
                ReadResourceRequestParams::new(
                    "rocketmq://clusters/local-dev/consumer-groups/orders-service/progress?limit=1",
                ),
                local_context(&peer, 1),
            )
            .await
            .unwrap();
        let cursor = complete_resource_payload(response)["consumer_progress"]["next_cursor"]
            .as_str()
            .unwrap()
            .to_string();
        let continuation = complete_tool_response(
            running
                .service()
                .call_tool(
                    tool_request(
                        "rocketmq_get_consumer_progress",
                        json!({
                            "cluster":"local-dev",
                            "consumer_group":"orders-service",
                            "limit":1,
                            "cursor":cursor,
                        }),
                    ),
                    local_context(&peer, 2),
                )
                .await
                .unwrap(),
        );
        assert_eq!(continuation.is_error, Some(false));
        assert_eq!(counters.consumer_progress_queries.load(Ordering::SeqCst), 1);
        drop(running);
        drop(client);
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn disabled_cache_reloads_first_pages_but_keeps_cross_surface_cursors() {
        let (_owner, server, counters) = test_server_with_config("read_only", None, |config| {
            config.cache.enabled = false;
        });
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();
        let stats = complete_tool_response(
            running
                .service()
                .call_tool(
                    tool_request(
                        "rocketmq_get_topic_stats",
                        json!({"cluster":"local-dev","topic":"orders","limit":1}),
                    ),
                    local_context(&peer, 1),
                )
                .await
                .unwrap(),
        );
        let cursor = stats.structured_content.as_ref().unwrap()["data"]["next_cursor"]
            .as_str()
            .unwrap();
        running
            .service()
            .read_resource(
                ReadResourceRequestParams::new("rocketmq://clusters/local-dev/topics/orders/stats?limit=1"),
                local_context(&peer, 2),
            )
            .await
            .unwrap();
        running
            .service()
            .read_resource(
                ReadResourceRequestParams::new(format!(
                    "rocketmq://clusters/local-dev/topics/orders/stats?limit=1&cursor={cursor}"
                )),
                local_context(&peer, 3),
            )
            .await
            .unwrap();
        assert_eq!(counters.topic_stats_queries.load(Ordering::SeqCst), 2);

        let progress = complete_tool_response(
            running
                .service()
                .call_tool(
                    tool_request(
                        "rocketmq_get_consumer_progress",
                        json!({"cluster":"local-dev","consumer_group":"orders-service","limit":1}),
                    ),
                    local_context(&peer, 4),
                )
                .await
                .unwrap(),
        );
        let cursor = progress.structured_content.as_ref().unwrap()["data"]["next_cursor"]
            .as_str()
            .unwrap();
        running
            .service()
            .read_resource(
                ReadResourceRequestParams::new(
                    "rocketmq://clusters/local-dev/consumer-groups/orders-service/progress?limit=1",
                ),
                local_context(&peer, 5),
            )
            .await
            .unwrap();
        running
            .service()
            .read_resource(
                ReadResourceRequestParams::new(format!(
                    "rocketmq://clusters/local-dev/consumer-groups/orders-service/progress?limit=1&cursor={cursor}"
                )),
                local_context(&peer, 6),
            )
            .await
            .unwrap();
        assert_eq!(counters.consumer_progress_queries.load(Ordering::SeqCst), 2);
        drop(running);
        drop(client);
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn prompt_handlers_filter_and_reauthorize_without_starting_sessions() {
        let (_owner, server, counters) = test_server("read_only", None);
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();
        let names = running
            .service()
            .list_prompts(None, local_context(&peer, 1))
            .await
            .unwrap()
            .prompts
            .into_iter()
            .map(|prompt| prompt.name.to_string())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            names,
            BTreeSet::from([
                "broker_health_check".to_string(),
                "diagnose_message_delivery".to_string(),
                "analyze_consumer_connections".to_string(),
            ])
        );

        let allowed = running
            .service()
            .get_prompt(
                prompt_request(
                    "diagnose_message_delivery",
                    json!({"cluster":"local-dev","topic":"orders","consumer_group":"group-a"}),
                ),
                local_context(&peer, 2),
            )
            .await;
        assert!(matches!(allowed, Ok(GetPromptResponse::Complete(_))));
        let unauthorized = running
            .service()
            .get_prompt(
                prompt_request(
                    "diagnose_broker_health",
                    json!({"cluster":"local-dev","broker_name":"broker-a"}),
                ),
                local_context(&peer, 3),
            )
            .await
            .unwrap_err();
        let unknown = running
            .service()
            .get_prompt(prompt_request("private-prompt", json!({})), local_context(&peer, 4))
            .await
            .unwrap_err();
        assert_eq!(unauthorized.message, unknown.message);
        assert_eq!(unauthorized.data, unknown.data);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 0);
        drop(running);
        drop(client);

        let (_owner, server, counters) = test_server("diagnose", None);
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();
        assert_eq!(
            running
                .service()
                .list_prompts(None, local_context(&peer, 5))
                .await
                .unwrap()
                .prompts
                .len(),
            5
        );
        let broker = running
            .service()
            .get_prompt(
                prompt_request(
                    "diagnose_broker_health",
                    json!({"cluster":"local-dev","broker_name":"broker-a"}),
                ),
                local_context(&peer, 6),
            )
            .await;
        assert!(matches!(broker, Ok(GetPromptResponse::Complete(_))));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 0);
        drop(running);
        drop(client);
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn unauthorized_resource_variants_share_one_oracle_safe_envelope() {
        let (_owner, server, counters) = test_server_with_config("diagnose", None, |config| {
            config.clusters[0].tenant = Some("test-tenant".to_string());
        });
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();
        let uris = [
            "rocketmq://clusters/local-dev/topics/orders/config",
            "rocketmq://clusters/local-dev/topics/missing/config",
            "rocketmq://clusters/unconfigured/topics/orders/config",
            "rocketmq://clusters/local-dev/unknown",
            "rocketmq://clusters/local-dev/topics/token%3Dsecret/config",
            "rocketmq://clusters/127.0.0.1/topics/orders/config",
        ];
        let mut errors = Vec::new();
        for uri in uris {
            let error = running
                .service()
                .read_resource(
                    ReadResourceRequestParams::new(uri),
                    oauth_context(&peer, 70, "no-resource-scope", ["diagnose"], []),
                )
                .await
                .unwrap_err();
            errors.push(error);
        }
        let baseline = &errors[0];
        for error in &errors[1..] {
            assert_eq!(error.code, baseline.code);
            assert_eq!(error.message, baseline.message);
            assert_eq!(error.data, baseline.data);
        }

        let tenant_error = running
            .service()
            .read_resource(
                ReadResourceRequestParams::new("rocketmq://clusters/local-dev/topics/orders/config"),
                oauth_read_context(&peer, 70, "wrong-tenant", ["local-dev"]),
            )
            .await
            .unwrap_err();
        assert_eq!(tenant_error.code, baseline.code);
        assert_eq!(tenant_error.message, baseline.message);
        assert_eq!(tenant_error.data, baseline.data);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 0);
        assert_eq!(counters.topic_config_queries.load(Ordering::SeqCst), 0);

        let audit = running.service().app().guard().audit_log().records();
        let audit_text = format!("{audit:?}");
        for secret in ["rocketmq://", "token=secret", "token%3Dsecret", "127.0.0.1"] {
            assert!(!audit_text.contains(secret), "audit retained {secret}");
        }
        drop(running);
        drop(client);
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn unauthorized_prompt_argument_matrix_is_indistinguishable_and_session_free() {
        let (_owner, server, counters) = test_server("diagnose", None);
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();
        let invalid = [
            json!({}),
            json!({"cluster":"local-dev","topic":"orders","consumer_group":"group","unknown":"x"}),
            json!({"cluster":null,"topic":"orders","consumer_group":"group"}),
            json!({"cluster":7,"topic":"orders","consumer_group":"group"}),
            json!({"cluster":"local-dev","topic":"","consumer_group":"group"}),
            json!({"cluster":"local-dev","topic":"x".repeat(crate::model::identifier::TOPIC_MAX_BYTES + 1),"consumer_group":"group"}),
            json!({"cluster":"local-dev","topic":"orders\nreset","consumer_group":"group"}),
            json!({"cluster":"local-dev","topic":"orders%7B%7B","consumer_group":"group"}),
        ];
        let context = || oauth_context(&peer, 71, "no-prompt-scope", ["diagnose"], []);
        let baseline = running
            .service()
            .get_prompt(
                prompt_request(
                    "diagnose_message_delivery",
                    json!({"cluster":"local-dev","topic":"orders","consumer_group":"group"}),
                ),
                context(),
            )
            .await
            .unwrap_err();
        for arguments in invalid {
            for name in ["diagnose_message_delivery", "private-prompt"] {
                let error = running
                    .service()
                    .get_prompt(prompt_request(name, arguments.clone()), context())
                    .await
                    .unwrap_err();
                assert_eq!(error.code, baseline.code, "name={name}, arguments={arguments}");
                assert_eq!(error.message, baseline.message, "name={name}, arguments={arguments}");
                assert_eq!(error.data, baseline.data, "name={name}, arguments={arguments}");
            }
        }
        let check_level = running
            .service()
            .get_prompt(
                prompt_request(
                    "broker_health_check",
                    json!({"cluster":"local-dev","check_level":"unbounded"}),
                ),
                context(),
            )
            .await
            .unwrap_err();
        assert_eq!(check_level.code, baseline.code);
        assert_eq!(check_level.message, baseline.message);
        assert_eq!(check_level.data, baseline.data);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 0);
        drop(running);
        drop(client);
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn tenant_and_principal_cluster_limits_apply_to_discovery_read_and_prompt_get() {
        let (_owner, server, counters) = test_server_with_config("read_only", None, |config| {
            config.clusters[0].tenant = Some("tenant-a".to_string());
        });
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();
        let mismatch = oauth_read_context(&peer, 1, "tenant-b", ["local-dev"]);
        assert!(running
            .service()
            .list_resource_templates(None, mismatch)
            .await
            .unwrap()
            .resource_templates
            .is_empty());
        assert!(running
            .service()
            .read_resource(
                ReadResourceRequestParams::new("rocketmq://clusters/local-dev/topics/orders/config"),
                oauth_read_context(&peer, 2, "tenant-b", ["local-dev"]),
            )
            .await
            .is_err());
        let prompt = running
            .service()
            .get_prompt(
                prompt_request(
                    "diagnose_message_delivery",
                    json!({"cluster":"local-dev","topic":"orders","consumer_group":"group-a"}),
                ),
                oauth_read_context(&peer, 3, "tenant-b", ["local-dev"]),
            )
            .await
            .unwrap_err();
        assert_eq!(prompt.data.unwrap()["code"], "prompt_unavailable");

        let cluster_limited = running
            .service()
            .read_resource(
                ReadResourceRequestParams::new("rocketmq://clusters/local-dev/topics/orders/config"),
                oauth_read_context(&peer, 4, "tenant-a", ["other-cluster"]),
            )
            .await;
        assert!(cluster_limited.is_err());
        assert_eq!(counters.starts.load(Ordering::SeqCst), 0);

        let allowed = running
            .service()
            .get_prompt(
                prompt_request(
                    "diagnose_message_delivery",
                    json!({"cluster":"local-dev","topic":"orders","consumer_group":"group-a"}),
                ),
                oauth_read_context(&peer, 5, "tenant-a", ["local-dev"]),
            )
            .await;
        assert!(matches!(allowed, Ok(GetPromptResponse::Complete(_))));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 0);
        drop(running);
        drop(client);
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn unsafe_only_configured_cluster_publishes_no_cluster_discovery_surface() {
        let (_owner, server, counters) = test_server_with_config("diagnose", None, |config| {
            config.clusters[0].name = "token=secret".to_string();
        });
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();

        let resources = running
            .service()
            .list_resources(None, local_context(&peer, 1))
            .await
            .unwrap();
        assert_eq!(resources.resources.len(), 2);
        assert!(resources
            .resources
            .iter()
            .all(|resource| resource.uri.starts_with("rocketmq://system/")));
        assert!(running
            .service()
            .list_resource_templates(None, local_context(&peer, 2))
            .await
            .unwrap()
            .resource_templates
            .is_empty());
        assert!(running
            .service()
            .list_prompts(None, local_context(&peer, 3))
            .await
            .unwrap()
            .prompts
            .is_empty());
        let prompt = running
            .service()
            .get_prompt(
                prompt_request(
                    "diagnose_message_delivery",
                    json!({"cluster":"token=secret","topic":"orders","consumer_group":"group"}),
                ),
                local_context(&peer, 4),
            )
            .await
            .unwrap_err();
        assert_eq!(prompt.data.as_ref().unwrap()["code"], "prompt_unavailable");

        let encoded_sensitive = running
            .service()
            .read_resource(
                ReadResourceRequestParams::new("rocketmq://clusters/token%3Dsecret/topics"),
                local_context(&peer, 5),
            )
            .await
            .unwrap_err();
        let unconfigured = running
            .service()
            .read_resource(
                ReadResourceRequestParams::new("rocketmq://clusters/local-dev/topics"),
                local_context(&peer, 5),
            )
            .await
            .unwrap_err();
        assert_eq!(encoded_sensitive.code, unconfigured.code);
        assert_eq!(encoded_sensitive.message, unconfigured.message);
        assert_eq!(encoded_sensitive.data, unconfigured.data);
        let wire =
            serde_json::to_string(&(resources, prompt, running.service().app.guard().audit_log().records())).unwrap();
        assert!(!wire.contains("token=secret"));
        assert!(!wire.contains("token%3Dsecret"));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 0);
        drop(running);
        drop(client);
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn mixed_configured_clusters_publish_only_the_safe_cluster_surface() {
        let (_owner, server, counters) = test_server_with_config("diagnose", None, |config| {
            let mut unsafe_cluster = config.clusters[0].clone();
            unsafe_cluster.name = "%74oken%3Dsecret".to_string();
            config.clusters.push(unsafe_cluster);
        });
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();

        let resources = running
            .service()
            .list_resources(None, local_context(&peer, 1))
            .await
            .unwrap();
        let templates = running
            .service()
            .list_resource_templates(None, local_context(&peer, 2))
            .await
            .unwrap();
        let prompts = running
            .service()
            .list_prompts(None, local_context(&peer, 3))
            .await
            .unwrap();
        assert_eq!(resources.resources.len(), 7);
        assert_eq!(templates.resource_templates.len(), 15);
        assert_eq!(prompts.prompts.len(), 5);
        assert!(resources.resources.iter().all(|resource| {
            resource.uri.starts_with("rocketmq://clusters/local-dev/") || resource.uri.starts_with("rocketmq://system/")
        }));
        let wire = serde_json::to_string(&(resources, templates, prompts)).unwrap();
        assert!(!wire.contains("token=secret"));
        assert!(!wire.contains("%74oken%3Dsecret"));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 0);
        drop(running);
        drop(client);
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn diagnose_handler_discovers_all_infrastructure_tools() {
        let (_owner, server, _counters) = test_server("diagnose", None);
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();

        let names = running
            .service()
            .list_tools(None, local_context(&peer, 1))
            .await
            .unwrap()
            .tools
            .into_iter()
            .map(|tool| tool.name.to_string())
            .collect::<BTreeSet<_>>();

        assert!(names.contains("rocketmq_get_ha_status"));
        assert!(names.contains("rocketmq_get_controller_metadata"));
        assert!(names.contains("rocketmq_get_nameserver_config_summary"));
        drop(running);
        drop(client);
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn read_only_handler_discovers_only_read_infrastructure_tool() {
        let (_owner, server, _counters) = test_server("read_only", None);
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();

        let names = running
            .service()
            .list_tools(None, local_context(&peer, 1))
            .await
            .unwrap()
            .tools
            .into_iter()
            .map(|tool| tool.name.to_string())
            .collect::<BTreeSet<_>>();

        assert!(!names.contains("rocketmq_get_ha_status"));
        assert!(!names.contains("rocketmq_get_controller_metadata"));
        assert!(names.contains("rocketmq_get_nameserver_config_summary"));
        drop(running);
        drop(client);
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    #[tokio::test]
    async fn read_only_handler_denies_infrastructure_diagnosis_before_session_work() {
        let (_owner, server, counters) = test_server("read_only", None);
        let (running, client) = connected_test_server(server).await;
        let peer = running.peer().clone();
        let request = CallToolRequestParams::new("rocketmq_get_ha_status").with_arguments(
            json!({"cluster": "local-dev", "endpoint": "private-controller.internal:9878"})
                .as_object()
                .unwrap()
                .clone(),
        );

        let denied = complete_tool_response(
            running
                .service()
                .call_tool(request, local_context(&peer, 1))
                .await
                .unwrap(),
        );

        assert_eq!(denied.is_error, Some(true));
        assert_eq!(counters.starts.load(Ordering::SeqCst), 0);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 0);
        drop(running);
        drop(client);
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
        test_server_with_config(profile, gate, |_| {})
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    fn test_server_with_config(
        profile: &str,
        gate: Option<Arc<ProtocolTestGate>>,
        configure: impl FnOnce(&mut McpConfig),
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
        config.audit.sink = "memory".to_string();
        config.audit.path.clear();
        configure(&mut config);
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
    fn oauth_read_context(
        peer: &rmcp::service::Peer<RoleServer>,
        id: i64,
        tenant: &str,
        allowed_clusters: impl IntoIterator<Item = &'static str>,
    ) -> RequestContext<RoleServer> {
        let request = axum::http::Request::builder().uri("/mcp").body(()).unwrap();
        let (mut parts, _) = request.into_parts();
        parts.extensions.insert(AccessContext {
            principal: Principal {
                id: format!("oauth-{id}"),
                tenant: Some(tenant.to_string()),
                roles: BTreeSet::from(["read_only".to_string()]),
                scopes: BTreeSet::from(["rocketmq:read".to_string()]),
                allowed_clusters: Some(allowed_clusters.into_iter().map(str::to_string).collect()),
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
    fn tool_request(name: &'static str, arguments: serde_json::Value) -> CallToolRequestParams {
        CallToolRequestParams::new(name).with_arguments(arguments.as_object().unwrap().clone())
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    fn prompt_request(name: &'static str, arguments: serde_json::Value) -> GetPromptRequestParams {
        GetPromptRequestParams::new(name).with_arguments(arguments.as_object().unwrap().clone())
    }

    #[cfg(all(feature = "streamable-http", feature = "stdio"))]
    fn complete_resource_payload(response: ReadResourceResponse) -> serde_json::Value {
        let result = match response {
            ReadResourceResponse::Complete(result) => result,
            response => panic!("expected a completed resource response, got {response:?}"),
        };
        match &result.contents[0] {
            rmcp::model::ResourceContents::TextResourceContents { text, .. } => serde_json::from_str(text).unwrap(),
            contents => panic!("expected text resource contents, got {contents:?}"),
        }
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
        let config = McpConfig::load(example_config_path()).unwrap();
        let registry = resources::registry::ResourceRegistry::new().unwrap();
        let resources = registry
            .list_resources(&config, None, b"snapshot", |_, _| true, true)
            .unwrap()
            .resources
            .into_iter()
            .map(|resource| serde_json::to_value(resource).expect("resource descriptor serializes"))
            .collect::<Vec<_>>();
        let resource_templates = serde_json::to_value(
            registry
                .list_resource_templates(&config, None, b"snapshot", |_, _| true)
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

        #[cfg(not(feature = "change-planning"))]
        assert_eq!(tools.len(), 24);
        #[cfg(feature = "change-planning")]
        assert_eq!(tools.len(), 29);
        assert_eq!(resources.len(), 7);
        assert_eq!(resource_templates.as_array().unwrap().len(), 15);
        assert_eq!(prompts.len(), 5);

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
