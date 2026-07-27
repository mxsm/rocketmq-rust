// Copyright 2023 The RocketMQ Rust Authors
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

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::routing::get;
use axum::routing::post;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::wait_for_signal_result;
use rocketmq_sre_contracts::EvidenceSnapshot;
use serde::Serialize;

use crate::ConnectorCapabilitiesView;
use crate::ConnectorConfig;
use crate::ConnectorEngine;
use crate::ConnectorError;
use crate::EvidenceQueryRequest;
use crate::McpGateway;
use crate::RmcpGateway;
use crate::channel::ControlPlaneChannel;

const MAX_INTERNAL_REQUEST_BYTES: usize = 64 * 1024;

/// Builds the production connector API.
pub(crate) fn build_router(engine: Arc<ConnectorEngine<RmcpGateway>>) -> Router {
    router(engine)
}

fn router<G>(engine: Arc<ConnectorEngine<G>>) -> Router
where
    G: McpGateway,
{
    let router = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready::<G>))
        .layer(DefaultBodyLimit::max(MAX_INTERNAL_REQUEST_BYTES));
    if engine.reverse_channel_enabled() {
        // In normal Phase 01 operation the connector exposes no inbound
        // evidence or capability API. Commands arrive only on the active,
        // authenticated HTTP/2 reverse channel.
        router.with_state(engine)
    } else {
        router
            .route("/internal/v1/evidence/query", post(evidence_query::<G>))
            .route("/internal/v1/capabilities", get(capabilities::<G>))
            .with_state(engine)
    }
}

/// Runs the connector, including an owned capability reconciler and graceful
/// MCP session shutdown.
///
/// # Errors
///
/// Returns a bind, serving, runtime-ownership, or connector initialization
/// error.
pub async fn run(config: ConnectorConfig, service_context: ChildServiceContext) -> Result<(), ConnectorError> {
    let config = Arc::new(config);
    let gateway = Arc::new(RmcpGateway::new(config.clone())?);
    let engine = Arc::new(ConnectorEngine::new(config.clone(), gateway)?);
    engine
        .initialize_sources(service_context.child("evidence-sources"))
        .await;

    if let Err(error) = engine.reconcile().await {
        tracing::warn!(
            code = error.code.as_str(),
            retryable = error.retryable,
            "initial MCP compatibility handshake did not complete"
        );
    }

    if let Some(channel) = ControlPlaneChannel::new(engine.clone(), config.clone())? {
        let channel_context = service_context.child("control-plane-reverse-channel");
        let channel = Arc::new(channel);
        service_context
            .spawn_service("rocketmq-sre-connector.control-plane-channel", {
                let channel = channel.clone();
                async move {
                    channel.run(channel_context).await;
                }
            })
            .map_err(|error| {
                ConnectorError::source(format!(
                    "control-plane channel could not be owned by TaskGroup: {error}"
                ))
            })?;
    }

    let reconciler = engine.clone();
    let interval = config.handshake_interval;
    let mut schedule = ScheduledTaskConfig::fixed_delay("rocketmq-sre-connector.handshake-reconciler", interval);
    // The initial reconciliation was performed synchronously above.
    schedule.initial_delay = interval;
    service_context
        .scheduled_tasks("rocketmq-sre-connector.schedules")
        .schedule_fixed_delay(schedule, move || {
            let reconciler = reconciler.clone();
            async move {
                if let Err(error) = reconciler.reconcile().await {
                    tracing::warn!(
                        code = error.code.as_str(),
                        retryable = error.retryable,
                        "periodic MCP compatibility handshake failed"
                    );
                }
            }
        })
        .map_err(|error| {
            ConnectorError::source(format!("handshake reconciler could not be owned by TaskGroup: {error}"))
        })?;

    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .map_err(|error| ConnectorError::source(format!("connector HTTP listener cannot bind: {error}")))?;
    let local_addr = listener
        .local_addr()
        .map_err(|error| ConnectorError::source(format!("connector HTTP listener address is unavailable: {error}")))?;
    tracing::info!(
        bind_addr = %local_addr,
        scope = service_context.name(),
        effective_access = "read_only",
        "RocketMQ AI SRE connector is serving"
    );

    let server_result = axum::serve(listener, build_router(engine.clone()))
        .with_graceful_shutdown(async {
            if let Err(error) = wait_for_signal_result().await {
                tracing::warn!(
                    error = %error,
                    "connector shutdown signal watcher failed"
                );
            }
        })
        .await;
    service_context.task_group().cancel();
    engine.close().await;
    server_result.map_err(|error| ConnectorError::source(format!("connector HTTP server failed: {error}")))
}

#[derive(Serialize)]
struct ServiceStatus {
    status: &'static str,
}

async fn health() -> Json<ServiceStatus> {
    Json(ServiceStatus { status: "healthy" })
}

async fn ready<G>(State(engine): State<Arc<ConnectorEngine<G>>>) -> (StatusCode, Json<ServiceStatus>)
where
    G: McpGateway,
{
    if engine.is_ready().await {
        (StatusCode::OK, Json(ServiceStatus { status: "ready" }))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ServiceStatus { status: "not_ready" }),
        )
    }
}

async fn capabilities<G>(
    State(engine): State<Arc<ConnectorEngine<G>>>,
    headers: HeaderMap,
) -> Result<Json<ConnectorCapabilitiesView>, ConnectorError>
where
    G: McpGateway,
{
    engine.authorize(bearer_header(&headers))?;
    Ok(Json(engine.capabilities().await))
}

async fn evidence_query<G>(
    State(engine): State<Arc<ConnectorEngine<G>>>,
    headers: HeaderMap,
    request: Result<Json<EvidenceQueryRequest>, JsonRejection>,
) -> Result<Json<EvidenceSnapshot>, ConnectorError>
where
    G: McpGateway,
{
    engine.authorize(bearer_header(&headers))?;
    let Json(request) = request.map_err(|_| {
        ConnectorError::new(
            crate::ConnectorErrorCode::InvalidEvidenceQuery,
            false,
            "internal evidence query body is not valid JSON",
        )
    })?;
    engine.evidence(request).await.map(Json)
}

fn bearer_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::path::PathBuf;
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::Request;
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::TenantId;
    use tower::ServiceExt;
    use url::Url;

    use super::*;
    use crate::CapabilityManifest;
    use crate::EvidenceOperation;
    use crate::MCP_BUSINESS_SCHEMA;
    use crate::MCP_PROTOCOL_VERSION;
    use crate::VerifiedCapability;
    use crate::WireEvidenceEnvelope;
    use crate::config::ConnectorAuth;
    use crate::config::ControlPlaneConfig;
    use crate::config::SecretValue;

    struct FakeGateway;

    impl McpGateway for FakeGateway {
        async fn handshake(&self) -> Result<BTreeMap<String, VerifiedCapability>, ConnectorError> {
            Ok(BTreeMap::from([(
                "local".to_owned(),
                VerifiedCapability {
                    manifest: CapabilityManifest {
                        mcp_protocol_version: MCP_PROTOCOL_VERSION.to_owned(),
                        business_schema_version: MCP_BUSINESS_SCHEMA.to_owned(),
                        server_version: "test".to_owned(),
                        cluster: "local".to_owned(),
                        tools: Vec::new(),
                        resources: Vec::new(),
                        tool_surface_digest: format!("sha256:{}", "0".repeat(64)),
                        mutation_supported: false,
                    },
                    observed_at: chrono::Utc::now(),
                },
            )]))
        }

        async fn query(
            &self,
            _cluster: &str,
            _operation: &EvidenceOperation,
        ) -> Result<WireEvidenceEnvelope, ConnectorError> {
            Err(ConnectorError::source("unused test gateway"))
        }

        async fn close(&self) {}
    }

    fn test_config() -> ConnectorConfig {
        ConnectorConfig {
            bind_addr: "127.0.0.1:8091".parse().expect("socket"),
            mcp_url: Url::parse("http://127.0.0.1:8089/mcp").expect("URL"),
            mcp_ca_path: Some(PathBuf::from("ca.pem")),
            mcp_ca_pem: Vec::new(),
            auth: ConnectorAuth::DevelopmentToken {
                token_env: "TEST_MCP_TOKEN".to_owned(),
                token: SecretValue::new("mcp-token".to_owned()),
            },
            tenant_id: TenantId::new(),
            cluster_allowlist: BTreeSet::from(["local".to_owned()]),
            cluster_ids: BTreeMap::from([("local".to_owned(), ClusterId::new())]),
            request_timeout: Duration::from_secs(1),
            handshake_interval: Duration::from_secs(1),
            shutdown_timeout: Duration::from_secs(1),
            max_concurrency: 1,
            max_response_bytes: 4096,
            expected_tool_surface_digest: None,
            prometheus_url: None,
            loki_url: None,
            tempo_url: None,
            admin_source: None,
            kubernetes_source: None,
            source_limits: crate::config::test_source_limits(1, 4096),
            internal_token_env: "TEST_INTERNAL_TOKEN".to_owned(),
            internal_token: SecretValue::new("internal-token".to_owned()),
            control_plane: None,
        }
    }

    fn test_engine() -> Arc<ConnectorEngine<FakeGateway>> {
        Arc::new(ConnectorEngine::new(Arc::new(test_config()), Arc::new(FakeGateway)).expect("test engine"))
    }

    fn reverse_channel_engine() -> Arc<ConnectorEngine<FakeGateway>> {
        let mut config = test_config();
        config.control_plane = Some(ControlPlaneConfig {
            base_url: Url::parse("http://127.0.0.1:8090").expect("URL"),
            cluster_id: *config.cluster_ids.values().next().expect("cluster"),
            connector_subject: "connector".to_owned(),
            connector_issuer: "test".to_owned(),
            ca_pem: Vec::new(),
            client_identity_pem: Vec::new(),
            poll_wait: Duration::from_secs(1),
            heartbeat_interval: Duration::from_secs(1),
        });
        Arc::new(ConnectorEngine::new(Arc::new(config), Arc::new(FakeGateway)).expect("test engine"))
    }

    #[tokio::test]
    async fn health_is_public_but_internal_capabilities_require_bearer() {
        let engine = test_engine();
        let app = router(engine.clone());
        let health = app
            .clone()
            .oneshot(Request::builder().uri("/healthz").body(Body::empty()).expect("request"))
            .await
            .expect("health response");
        assert_eq!(health.status(), StatusCode::OK);

        let not_ready = app
            .clone()
            .oneshot(Request::builder().uri("/readyz").body(Body::empty()).expect("request"))
            .await
            .expect("readiness response");
        assert_eq!(not_ready.status(), StatusCode::SERVICE_UNAVAILABLE);
        engine.reconcile().await.expect("mock handshake");
        let ready = app
            .clone()
            .oneshot(Request::builder().uri("/readyz").body(Body::empty()).expect("request"))
            .await
            .expect("readiness response");
        assert_eq!(ready.status(), StatusCode::OK);

        let unauthorized = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/internal/v1/capabilities")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("unauthorized response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let authorized = app
            .oneshot(
                Request::builder()
                    .uri("/internal/v1/capabilities")
                    .header("authorization", "Bearer internal-token")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("authorized response");
        assert_eq!(authorized.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn reverse_channel_mode_has_no_inbound_evidence_api() {
        let app = router(reverse_channel_engine());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/v1/evidence/query")
                    .header("authorization", "Bearer internal-token")
                    .body(Body::from("{}"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
