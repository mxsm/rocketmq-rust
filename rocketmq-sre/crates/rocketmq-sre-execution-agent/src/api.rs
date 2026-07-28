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

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::routing::post;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::wait_for_signal_result;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::ExecutionAction;
use serde::Serialize;
use subtle::ConstantTimeEq;

use crate::AdvanceFenceRequest;
use crate::AdvanceFenceResponse;
use crate::AgentDispatchRequest;
use crate::AgentDispatchResponse;
use crate::AgentDriverRegistry;
use crate::AgentEffectStore;
use crate::AgentReadRequest;
use crate::AgentReadResult;
use crate::BrokerConfigPatchHandler;
use crate::DispatchBarrier;
use crate::ExecutionAgent;
use crate::ExecutionAgentCapabilities;
use crate::ExecutionAgentConfig;
use crate::ExecutionAgentError;
use crate::FenceAckSigner;
use crate::HttpLeaseAuthorityClient;
use crate::ReconcileEffectRequest;
use crate::ReconcileEffectResponse;
use crate::drivers::ProductionBrokerConfigPatchClient;

#[derive(Clone)]
struct AppState {
    agent: ExecutionAgent,
    executor_token: Arc<str>,
    require_mtls_identity: bool,
}

#[derive(Serialize)]
struct ServiceStatus {
    schema_version: &'static str,
    status: &'static str,
}

#[derive(Serialize)]
struct MetricsStatus {
    schema_version: &'static str,
    active_dispatches: usize,
    dispatch_total: u64,
    replay_total: u64,
    fence_rejections_total: u64,
    unknown_effects_total: u64,
}

#[derive(Serialize)]
struct ErrorEnvelope {
    schema_version: &'static str,
    code: &'static str,
    message: &'static str,
    retryable: bool,
    correlation_id: CorrelationId,
}

pub fn build_router(agent: ExecutionAgent, executor_token: impl Into<Arc<str>>, require_mtls_identity: bool) -> Router {
    let state = AppState {
        agent,
        executor_token: executor_token.into(),
        require_mtls_identity,
    };
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/internal/v1/execution-agent/capabilities", get(capabilities))
        .route("/internal/v1/execution-agent/status", get(status))
        .route("/internal/v1/execution-agent/precheck", post(precheck))
        .route("/internal/v1/execution-agent/dispatch", post(dispatch))
        .route("/internal/v1/execution-agent/reconcile", post(reconcile))
        .route("/internal/v1/execution-agent/advance-fence", post(advance_fence))
        .with_state(state)
}

/// Runs the production Agent with PostgreSQL fencing and explicitly enabled
/// typed handlers. Disabled actions fail closed as `action_not_registered`.
///
/// # Errors
///
/// Returns configuration, database, bind, or serving failures.
pub async fn run(
    config: ExecutionAgentConfig,
    service_context: ChildServiceContext,
) -> Result<(), ExecutionAgentError> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(16)
        .connect(&config.database_url)
        .await
        .map_err(crate::AgentStoreError::Database)?;
    let broker_driver = match &config.broker_admin {
        Some(driver_config) => Some(Arc::new(
            ProductionBrokerConfigPatchClient::start(
                driver_config,
                pool.clone(),
                service_context.child("broker-config-driver"),
            )
            .await?,
        )),
        None => None,
    };
    let mut registry = AgentDriverRegistry::empty();
    if let Some(driver) = &broker_driver {
        registry.register_admin(
            ExecutionAction::BrokerConfigPatchAllowlisted,
            BrokerConfigPatchHandler::new(Arc::clone(driver)),
        )?;
    }
    let authority = Arc::new(HttpLeaseAuthorityClient::new(
        config.authority_url.clone(),
        Arc::<str>::from(config.authority_token.as_str()),
        Arc::<str>::from(config.agent_subject.as_str()),
        config.request_timeout,
        config.dev_insecure_http,
    )?);
    let agent = ExecutionAgent::new(
        AgentEffectStore::new(pool.clone()),
        DispatchBarrier::new(pool),
        authority,
        registry,
        FenceAckSigner::new(config.ack_signing_key.as_bytes(), config.agent_subject.clone())?,
        config.driver_timeout,
    );
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    let local_addr = listener.local_addr()?;
    tracing::info!(
        bind_addr = %local_addr,
        scope = service_context.name(),
        mutation_boundary = "typed_fenced_agent",
        "RocketMQ SRE Execution Agent is serving"
    );
    let server_result = axum::serve(
        listener,
        build_router(
            agent,
            Arc::<str>::from(config.executor_token.as_str()),
            !config.dev_insecure_http,
        ),
    )
    .with_graceful_shutdown(async {
        if let Err(error) = wait_for_signal_result().await {
            tracing::warn!(error = %error, "Execution Agent shutdown signal watcher failed");
        }
    })
    .await;
    if let Some(driver) = broker_driver {
        driver.shutdown().await;
    }
    service_context.task_group().cancel();
    server_result.map_err(|_| ExecutionAgentError::Io(std::io::Error::other("Execution Agent server failed")))
}

async fn health() -> Json<ServiceStatus> {
    Json(ServiceStatus {
        schema_version: "rocketmq-sre.service-status.v1",
        status: "healthy",
    })
}

async fn ready(State(state): State<AppState>) -> (StatusCode, Json<ServiceStatus>) {
    if state.agent.ready().await {
        (
            StatusCode::OK,
            Json(ServiceStatus {
                schema_version: "rocketmq-sre.service-status.v1",
                status: "ready",
            }),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ServiceStatus {
                schema_version: "rocketmq-sre.service-status.v1",
                status: "not_ready",
            }),
        )
    }
}

async fn capabilities(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ExecutionAgentCapabilities>, ExecutionAgentError> {
    authorize(&state, &headers)?;
    Ok(Json(state.agent.capabilities()))
}

async fn status(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<MetricsStatus>, ExecutionAgentError> {
    authorize(&state, &headers)?;
    let metrics = state.agent.metrics();
    Ok(Json(MetricsStatus {
        schema_version: "rocketmq-sre.execution-agent-status.v1",
        active_dispatches: metrics.active_dispatches,
        dispatch_total: metrics.dispatch_total,
        replay_total: metrics.replay_total,
        fence_rejections_total: metrics.fence_rejections_total,
        unknown_effects_total: metrics.unknown_effects_total,
    }))
}

async fn precheck(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<AgentReadRequest>,
) -> Result<Json<AgentReadResult>, ExecutionAgentError> {
    authorize(&state, &headers)?;
    state.agent.read_state(&request).await.map(Json)
}

async fn dispatch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<AgentDispatchRequest>,
) -> Result<Json<AgentDispatchResponse>, ExecutionAgentError> {
    authorize(&state, &headers)?;
    state.agent.dispatch(&request).await.map(Json)
}

async fn reconcile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ReconcileEffectRequest>,
) -> Result<Json<ReconcileEffectResponse>, ExecutionAgentError> {
    authorize(&state, &headers)?;
    state.agent.reconcile_effect(&request).await.map(Json)
}

async fn advance_fence(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<AdvanceFenceRequest>,
) -> Result<Json<AdvanceFenceResponse>, ExecutionAgentError> {
    authorize(&state, &headers)?;
    state.agent.advance_fence(&request).await.map(|fence_ack| {
        Json(AdvanceFenceResponse {
            schema_version: crate::EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
            fence_ack,
        })
    })
}

fn authorize(state: &AppState, headers: &HeaderMap) -> Result<(), ExecutionAgentError> {
    let bearer = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or(ExecutionAgentError::Unauthorized)?;
    let token_matches = bearer.len() == state.executor_token.len()
        && bool::from(bearer.as_bytes().ct_eq(state.executor_token.as_bytes()));
    if !token_matches {
        return Err(ExecutionAgentError::Unauthorized);
    }
    if state.require_mtls_identity {
        let identity = headers
            .get("x-forwarded-client-cert")
            .and_then(|value| value.to_str().ok())
            .ok_or(ExecutionAgentError::Unauthorized)?;
        if !has_spiffe_identity(identity, "spiffe://rocketmq-sre/executor") {
            return Err(ExecutionAgentError::Unauthorized);
        }
    }
    Ok(())
}

fn has_spiffe_identity(header: &str, expected: &str) -> bool {
    header.split([',', ';']).any(|part| {
        let part = part.trim();
        part == expected
            || part
                .strip_prefix("URI=")
                .map(str::trim)
                .map(|value| value.trim_matches('"'))
                .is_some_and(|value| value == expected)
    })
}

impl IntoResponse for ExecutionAgentError {
    fn into_response(self) -> Response {
        let (status, code, retryable) = match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized_workload_identity", false),
            Self::InvalidRequest => (StatusCode::BAD_REQUEST, "invalid_agent_request", false),
            Self::ActionNotRegistered => (StatusCode::CONFLICT, "action_not_registered", false),
            Self::AuthorityRejected => (StatusCode::FORBIDDEN, "stale_lease_epoch", false),
            Self::UnresolvedEffect => (StatusCode::CONFLICT, "unresolved_old_effects", false),
            Self::DriverFailed => (StatusCode::CONFLICT, "driver_failed", false),
            Self::DriverUnknown => (StatusCode::CONFLICT, "effect_unknown", false),
            Self::Configuration => (StatusCode::INTERNAL_SERVER_ERROR, "source_unavailable", false),
            Self::AuthorityUnavailable
            | Self::DispatchBarrierUnavailable
            | Self::Store(_)
            | Self::Http(_)
            | Self::Io(_) => (StatusCode::SERVICE_UNAVAILABLE, "source_unavailable", true),
        };
        (
            status,
            Json(ErrorEnvelope {
                schema_version: "rocketmq-sre.error.v1",
                code,
                message: "Execution Agent rejected the request without exposing target details",
                retryable,
                correlation_id: CorrelationId::new(),
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::has_spiffe_identity;

    #[test]
    fn forwarded_identity_requires_an_exact_spiffe_uri() {
        assert!(has_spiffe_identity(
            "By=spiffe://mesh/gateway;URI=spiffe://rocketmq-sre/executor",
            "spiffe://rocketmq-sre/executor"
        ));
        assert!(!has_spiffe_identity(
            "URI=spiffe://rocketmq-sre/executor-evil",
            "spiffe://rocketmq-sre/executor"
        ));
        assert!(!has_spiffe_identity(
            "DNS=spiffe://rocketmq-sre/executor",
            "spiffe://rocketmq-sre/executor"
        ));
    }
}
