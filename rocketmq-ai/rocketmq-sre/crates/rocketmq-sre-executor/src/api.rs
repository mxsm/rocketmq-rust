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

use std::future::IntoFuture;
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
use rocketmq_runtime::OperationContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::TaskKind;
use rocketmq_runtime::wait_for_signal_result;
use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentReadResult;
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
use crate::ExecutorOperationOutcome;
use crate::ExecutorRejection;
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
    shutdown_storage_failures_total: u64,
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
        .route("/internal/v1/executor/preconditions", post(read_precondition))
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
    let signal_owner = service_context.task_group().clone();
    let grace = config.shutdown_timeout;
    service_context
        .task_group()
        .spawn_cancellable_service("executor-shutdown-signal", async move {
            if let Err(error) = wait_for_signal_result().await {
                tracing::warn!(error=%error,"Change Executor shutdown signal watcher failed");
            }
            // Publish the same boundary before interrupting any startup read.
            drop(signal_owner.shutdown_until(ShutdownDeadline::after(grace)));
            signal_owner.cancel();
        })
        .map_err(ExecutorError::configuration_source)?;
    let connect = sqlx::postgres::PgPoolOptions::new()
        .max_connections(16)
        .connect(&config.database_url);
    let owner = service_context.task_group().clone();
    let pool = tokio::select! { biased;
        ()=owner.cancellation_token().cancelled_owned()=>{
            let deadline=owner.shutdown_deadline().unwrap_or_else(||ShutdownDeadline::after(grace));
            let report=owner.shutdown_until(deadline).await;
            return if report.is_healthy() {Ok(())} else {Err(ExecutorError::Io(std::io::Error::other("startup connection drain did not complete healthily")))};
        },
        pool=connect=>pool.map_err(crate::error::JournalError::Database)?,
    };
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
    let verifier = ExecutionVerifier::new(verification_source, config.verification_poll_interval)
        .with_profiles(config.verification_profiles.clone())
        .with_owner(service_context.task_group().clone());
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
    .with_verifier(verifier)
    .with_lifecycle(
        service_context.task_group().clone(),
        config.shutdown_timeout,
        config.request_timeout,
    );
    let startup_owner = service_context.task_group().clone();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let recovery_executor = executor.clone();
    startup_owner
        .spawn_draining_operation(
            &OperationContext::without_deadline(TaskKind::Worker),
            "startup-reconciliation",
            async move {
                let _ = sender.send(recovery_executor.recover_interrupted_executions(100).await);
            },
        )
        .map_err(ExecutorError::configuration_source)?;
    let recovery = tokio::select! { biased;
        ()=startup_owner.cancellation_token().cancelled_owned()=>{
            let deadline=startup_owner.shutdown_deadline().unwrap_or_else(||ShutdownDeadline::after(grace));
            let report=startup_owner.shutdown_until(deadline).await;
            return if report.is_healthy() && executor.settlement_healthy() {Ok(())} else {Err(ExecutorError::Io(std::io::Error::other("startup reconciliation drain did not complete healthily")))};
        },
        result=receiver=>result.map_err(|_|ExecutorError::AuthorityUnavailable)?,
    };
    match recovery {
        Ok(summary) => tracing::info!(
            attempted = summary.attempted,
            recovered = summary.recovered,
            blocked = summary.blocked,
            "bounded interrupted-execution recovery sweep completed"
        ),
        Err(error) => tracing::warn!(
            error = %error,
            "interrupted-execution recovery sweep is deferred to the authenticated recovery endpoint"
        ),
    }
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    let local_addr = listener.local_addr()?;
    tracing::info!(
        bind_addr = %local_addr,
        scope = service_context.name(),
        target_credentials = false,
        "RocketMQ SRE Change Executor is serving"
    );
    let settlement = executor.clone();
    let router = build_router(
        executor,
        Arc::<str>::from(config.control_plane_token.as_str()),
        !config.dev_insecure_http,
    );
    let owner = service_context.task_group().clone();
    let signal_owner = owner.clone();
    serve_owned(
        listener,
        router,
        owner,
        settlement,
        config.shutdown_timeout,
        async move {
            signal_owner.cancellation_token().cancelled_owned().await;
        },
    )
    .await
    .map_err(ExecutorError::Io)
}

async fn serve_owned(
    listener: tokio::net::TcpListener,
    router: Router,
    owner: rocketmq_runtime::task_group::TaskGroup,
    settlement: ChangeExecutor,
    timeout: std::time::Duration,
    shutdown: impl std::future::Future<Output = ()>,
) -> std::io::Result<()> {
    let server = axum::serve(listener, router)
        .with_graceful_shutdown(owner.cancellation_token().cancelled_owned())
        .into_future();
    tokio::pin!(server);
    let early = tokio::select! {
        result=&mut server=>Some(result),
        ()=shutdown=>None,
    };
    let deadline = owner
        .shutdown_deadline()
        .unwrap_or_else(|| ShutdownDeadline::after(timeout));
    // Publish one absolute deadline before cancellation. Admitted operations
    // reserve settlement time against this same boundary, independently of a
    // lost HTTP response waiter. No journal append gets its own reset timeout.
    let drain = owner.shutdown_until(deadline);
    owner.cancel();
    let (served, report) = match early {
        Some(result) => (Ok(result), drain.await),
        None => tokio::join!(
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline.instant()), &mut server),
            drain
        ),
    };
    if !report.is_healthy() || !settlement.settlement_healthy() {
        return Err(std::io::Error::other(
            "Executor operation drain did not complete healthily",
        ));
    }
    served.map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "Executor HTTP drain exceeded the shared deadline",
        )
    })?
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

async fn status(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<ExecutorStatus>, ExecutorApiFailure> {
    authorize(&state, &headers)?;
    let metrics = state.executor.metrics();
    Ok(Json(ExecutorStatus {
        shutdown_storage_failures_total: metrics.shutdown_storage_failures_total,
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
) -> Result<Json<crate::ExecuteOutcome>, ExecutorApiFailure> {
    authorize(&state, &headers)?;
    let result = if let Some(owner) = state.executor.operation_owner().cloned() {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let executor = state.executor.clone();
        owner
            .spawn_draining_operation(
                &OperationContext::without_deadline(TaskKind::Worker),
                "accepted-execution",
                async move {
                    let _ = sender.send(executor.execute(&request).await);
                },
            )
            .map_err(|_| ExecutorRejection::ObservationUnknown)?;
        receiver.await.map_err(|_| ExecutorError::AuthorityUnavailable)?
    } else {
        state.executor.execute(&request).await
    };
    match result? {
        ExecutorOperationOutcome::Accepted(outcome) => Ok(Json(outcome)),
        ExecutorOperationOutcome::Rejected(rejection) => Err(rejection.into()),
    }
}

async fn read_precondition(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<AgentReadRequest>,
) -> Result<Json<AgentReadResult>, ExecutorApiFailure> {
    authorize(&state, &headers)?;
    match state.executor.read_precondition(&request).await? {
        ExecutorOperationOutcome::Accepted(result) => Ok(Json(result)),
        ExecutorOperationOutcome::Rejected(rejection) => Err(rejection.into()),
    }
}

async fn recover_execution(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<crate::ExecuteOutcome>, ExecutorApiFailure> {
    authorize(&state, &headers)?;
    let id = parse_execution_id(&id)?;
    let result = if let Some(owner) = state.executor.operation_owner().cloned() {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let executor = state.executor.clone();
        owner
            .spawn_draining_operation(
                &OperationContext::without_deadline(TaskKind::Worker),
                "accepted-reconciliation-read",
                async move {
                    let _ = sender.send(executor.recover_execution(id).await);
                },
            )
            .map_err(|_| ExecutorRejection::ObservationUnknown)?;
        receiver.await.map_err(|_| ExecutorError::AuthorityUnavailable)?
    } else {
        state.executor.recover_execution(id).await
    };
    match result? {
        ExecutorOperationOutcome::Accepted(outcome) => Ok(Json(outcome)),
        ExecutorOperationOutcome::Rejected(rejection) => Err(rejection.into()),
    }
}

fn parse_execution_id(value: &str) -> Result<ExecutionId, ExecutorRejection> {
    Uuid::parse_str(value)
        .map(ExecutionId::from_uuid)
        .map_err(|_| ExecutorRejection::InvalidRequest)
}

fn authorize(state: &AppState, headers: &HeaderMap) -> Result<(), ExecutorRejection> {
    let bearer = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or(ExecutorRejection::Unauthorized)?;
    if bearer.len() != state.control_plane_token.len()
        || !bool::from(bearer.as_bytes().ct_eq(state.control_plane_token.as_bytes()))
    {
        return Err(ExecutorRejection::Unauthorized);
    }
    if state.require_mtls_identity {
        let identity = headers
            .get("x-forwarded-client-cert")
            .and_then(|value| value.to_str().ok())
            .ok_or(ExecutorRejection::Unauthorized)?;
        if !has_spiffe_identity(identity, "spiffe://rocketmq-sre/control-plane") {
            return Err(ExecutorRejection::Unauthorized);
        }
    }
    Ok(())
}

enum ExecutorApiFailure {
    Rejected(ExecutorRejection),
    Operational(ExecutorError),
}

impl From<ExecutorRejection> for ExecutorApiFailure {
    fn from(rejection: ExecutorRejection) -> Self {
        Self::Rejected(rejection)
    }
}

impl From<ExecutorError> for ExecutorApiFailure {
    fn from(error: ExecutorError) -> Self {
        Self::Operational(error)
    }
}

impl IntoResponse for ExecutorApiFailure {
    fn into_response(self) -> Response {
        let (status, code, retryable) = match self {
            Self::Rejected(rejection) => rejection.http_classification(),
            Self::Operational(error) => error.http_classification(),
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
        let (status, code, retryable) = self.http_classification();
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

    #[tokio::test]
    async fn startup_owner_cancel_interrupts_unconnected_database_and_signal_watcher_without_os_signal() {
        use rocketmq_runtime::RuntimeContext;
        use std::{sync::Arc, time::Duration};
        let runtime = RuntimeContext::from_current("d20-startup-owner");
        let context = runtime.service_context("http");
        let owner = context.task_group().clone();
        let peer_owner = runtime.service_context("postgres-peer").task_group().clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let accepted = entered.clone();
        let token = peer_owner.cancellation_token();
        peer_owner
            .spawn_service("stalled-startup-handshake", async move {
                let (stream, _) = listener.accept().await.unwrap();
                accepted.notify_one();
                token.cancelled().await;
                drop(stream);
            })
            .unwrap();
        let config = crate::ExecutorConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            database_url: format!("postgres://synthetic:synthetic@{endpoint}/synthetic?sslmode=disable"),
            authority_url: "http://127.0.0.1:1".parse().unwrap(),
            authority_token: "synthetic".to_owned(),
            agent_url: "http://127.0.0.1:1".parse().unwrap(),
            agent_token: "synthetic".to_owned(),
            control_plane_token: "synthetic".to_owned(),
            executor_subject: "SyntheticFixture".to_owned(),
            request_timeout: Duration::from_secs(30),
            lease_ttl_seconds: 30,
            resource_lock_ttl: Duration::from_secs(30),
            verification_poll_interval: Duration::from_secs(1),
            verification_profiles: crate::VerificationProfiles::default(),
            shutdown_timeout: Duration::from_secs(1),
            dev_insecure_http: true,
        };
        let runner = runtime.service_context("process-root").task_group().clone();
        let (finished, done) = tokio::sync::oneshot::channel();
        runner
            .spawn_service("run-root", async move {
                finished.send(super::run(config, context).await.is_ok()).unwrap();
            })
            .unwrap();
        entered.notified().await;
        owner.cancel();
        assert!(
            tokio::time::timeout(Duration::from_secs(2), done)
                .await
                .unwrap()
                .unwrap()
        );
        assert!(owner.shutdown(Duration::from_secs(1)).await.is_healthy());
        assert!(runner.shutdown(Duration::from_secs(1)).await.is_healthy());
        assert!(peer_owner.shutdown(Duration::from_secs(1)).await.is_healthy());
    }
}
