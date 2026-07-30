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
use axum::extract::Path;
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
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ExecutionRequest;
use serde::Serialize;
use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::ChangeExecutor;
use crate::ExecutionJournal;
use crate::ExecutionPrechecker;
use crate::ExecutionVerifier;
use crate::ExecutorActionRegistry;
use crate::ExecutorConfig;
use crate::ExecutorError;
use crate::HttpExecutionAgentClient;
use crate::HttpExecutionSliClient;
use crate::HttpExecutorAuthorityClient;
use crate::ProductionVerificationSource;
use crate::ResourceSafetyStore;

#[derive(Clone)]
struct AppState {
    executor: ChangeExecutor,
    control_plane_token: Arc<str>,
    require_mtls_identity: bool,
}

#[derive(Serialize)]
struct ServiceStatus {
    schema_version: &'static str,
    status: &'static str,
}

#[derive(Serialize)]
struct ExecutorStatus {
    schema_version: &'static str,
    active_executions: usize,
    execution_total: u64,
    replay_total: u64,
    precondition_rejections_total: u64,
    fence_rejections_total: u64,
    reconcile_blocks_total: u64,
}

#[derive(Serialize)]
struct ErrorEnvelope {
    schema_version: &'static str,
    code: &'static str,
    message: &'static str,
    retryable: bool,
    correlation_id: CorrelationId,
}

/// Builds the workload-only Executor HTTP surface.
pub fn build_router(
    executor: ChangeExecutor,
    control_plane_token: impl Into<Arc<str>>,
    require_mtls_identity: bool,
) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/internal/v1/executor/status", get(status))
        .route("/internal/v1/executor/executions", post(execute))
        .route("/internal/v1/executor/executions/{id}/recover", post(recover_execution))
        .with_state(AppState {
            executor,
            control_plane_token: control_plane_token.into(),
            require_mtls_identity,
        })
}

/// Runs the fenced Change Executor. It constructs only PostgreSQL and internal
/// HTTP clients; no RocketMQ Admin, Kubernetes, config writer, MCP, or model
/// client enters this process.
///
/// # Errors
///
/// Returns configuration, database, HTTP client, bind, or serving failures.
pub async fn run(config: ExecutorConfig, service_context: ChildServiceContext) -> Result<(), ExecutorError> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(16)
        .connect(&config.database_url)
        .await
        .map_err(crate::JournalError::Database)?;
    let authority = Arc::new(HttpExecutorAuthorityClient::new(
        config.authority_url.clone(),
        Arc::<str>::from(config.authority_token.as_str()),
        Arc::<str>::from(config.executor_subject.as_str()),
        config.request_timeout,
        config.dev_insecure_http,
    )?);
    let agent = Arc::new(HttpExecutionAgentClient::new(
        config.agent_url.clone(),
        Arc::<str>::from(config.agent_token.as_str()),
        config.request_timeout,
        config.dev_insecure_http,
    )?);
    let sli_client = Arc::new(HttpExecutionSliClient::new(
        config.authority_url.clone(),
        Arc::<str>::from(config.authority_token.as_str()),
        Arc::<str>::from(config.executor_subject.as_str()),
        config.request_timeout,
        config.dev_insecure_http,
    )?);
    let registry = Arc::new(ExecutorActionRegistry::embedded()?);
    let prechecker = ExecutionPrechecker::new(Arc::clone(&registry), agent.clone());
    let verification_source = Arc::new(ProductionVerificationSource::new(agent.clone(), sli_client));
    let verifier = ExecutionVerifier::new(verification_source, config.verification_poll_interval);
    let executor = ChangeExecutor::new(
        ExecutionJournal::new(pool.clone(), "rocketmq-sre-executor"),
        ResourceSafetyStore::new(pool),
        authority,
        agent,
        prechecker,
        Arc::<str>::from(config.executor_subject.as_str()),
        config.lease_ttl_seconds,
        config.resource_lock_ttl,
    )
    .with_verifier(verifier);
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    let local_addr = listener.local_addr()?;
    tracing::info!(
        bind_addr = %local_addr,
        scope = service_context.name(),
        target_credentials = false,
        "RocketMQ SRE Change Executor is serving"
    );
    let result = axum::serve(
        listener,
        build_router(
            executor,
            Arc::<str>::from(config.control_plane_token.as_str()),
            !config.dev_insecure_http,
        ),
    )
    .with_graceful_shutdown(async {
        if let Err(error) = wait_for_signal_result().await {
            tracing::warn!(error = %error, "Change Executor shutdown signal watcher failed");
        }
    })
    .await;
    service_context.task_group().cancel();
    result.map_err(|_| ExecutorError::Io(std::io::Error::other("Change Executor server failed")))
}

async fn health() -> Json<ServiceStatus> {
    Json(ServiceStatus {
        schema_version: "rocketmq-sre.service-status.v1",
        status: "healthy",
    })
}

async fn ready(State(state): State<AppState>) -> (StatusCode, Json<ServiceStatus>) {
    if state.executor.ready().await {
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

async fn status(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<ExecutorStatus>, ExecutorError> {
    authorize(&state, &headers)?;
    let metrics = state.executor.metrics();
    Ok(Json(ExecutorStatus {
        schema_version: "rocketmq-sre.executor-status.v1",
        active_executions: metrics.active_executions,
        execution_total: metrics.execution_total,
        replay_total: metrics.replay_total,
        precondition_rejections_total: metrics.precondition_rejections_total,
        fence_rejections_total: metrics.fence_rejections_total,
        reconcile_blocks_total: metrics.reconcile_blocks_total,
    }))
}

async fn execute(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ExecutionRequest>,
) -> Result<Json<crate::ExecuteOutcome>, ExecutorError> {
    authorize(&state, &headers)?;
    state.executor.execute(&request).await.map(Json)
}

async fn recover_execution(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<crate::ExecuteOutcome>, ExecutorError> {
    authorize(&state, &headers)?;
    state
        .executor
        .recover_execution(parse_execution_id(&id)?)
        .await
        .map(Json)
}

fn parse_execution_id(value: &str) -> Result<ExecutionId, ExecutorError> {
    Uuid::parse_str(value)
        .map(ExecutionId::from_uuid)
        .map_err(|_| ExecutorError::InvalidRequest)
}

fn authorize(state: &AppState, headers: &HeaderMap) -> Result<(), ExecutorError> {
    let bearer = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or(ExecutorError::Unauthorized)?;
    if bearer.len() != state.control_plane_token.len()
        || !bool::from(bearer.as_bytes().ct_eq(state.control_plane_token.as_bytes()))
    {
        return Err(ExecutorError::Unauthorized);
    }
    if state.require_mtls_identity {
        let identity = headers
            .get("x-forwarded-client-cert")
            .and_then(|value| value.to_str().ok())
            .ok_or(ExecutorError::Unauthorized)?;
        if !has_spiffe_identity(identity, "spiffe://rocketmq-sre/control-plane") {
            return Err(ExecutorError::Unauthorized);
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

impl IntoResponse for ExecutorError {
    fn into_response(self) -> Response {
        let (status, code, retryable) = match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized_workload_identity", false),
            Self::InvalidRequest | Self::Catalog(_) => (StatusCode::BAD_REQUEST, "invalid_execution_request", false),
            Self::AuthorityRejected => (StatusCode::FORBIDDEN, "execution_authority_rejected", false),
            Self::AgentRejected => (StatusCode::CONFLICT, "execution_agent_rejected", false),
            Self::VerificationRejected => (StatusCode::CONFLICT, "execution_verification_rejected", false),
            Self::PreconditionChanged => (StatusCode::CONFLICT, "precondition_changed", false),
            Self::ReconcileBlocked => (StatusCode::CONFLICT, "unresolved_old_effects", false),
            Self::Configuration => (StatusCode::INTERNAL_SERVER_ERROR, "source_unavailable", false),
            Self::AuthorityUnavailable
            | Self::AgentUnavailable
            | Self::VerificationUnavailable
            | Self::Journal(_)
            | Self::Http(_)
            | Self::Io(_) => (StatusCode::SERVICE_UNAVAILABLE, "source_unavailable", true),
        };
        (
            status,
            Json(ErrorEnvelope {
                schema_version: "rocketmq-sre.error.v1",
                code,
                message: "Change Executor rejected the request without exposing target details",
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
    use super::parse_execution_id;

    #[test]
    fn forwarded_identity_requires_an_exact_spiffe_uri() {
        assert!(has_spiffe_identity(
            "By=spiffe://mesh/gateway;URI=spiffe://rocketmq-sre/control-plane",
            "spiffe://rocketmq-sre/control-plane"
        ));
        assert!(!has_spiffe_identity(
            "URI=spiffe://rocketmq-sre/control-plane-evil",
            "spiffe://rocketmq-sre/control-plane"
        ));
    }

    #[test]
    fn recovery_path_rejects_invalid_execution_ids() {
        assert!(parse_execution_id("not-an-execution-id").is_err());
    }
}
