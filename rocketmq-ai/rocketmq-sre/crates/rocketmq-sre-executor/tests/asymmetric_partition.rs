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

#[path = "postgres_recovery/support.rs"]
#[allow(
    dead_code,
    reason = "this focused integration target reuses only the PostgreSQL fixture helpers needed for fencing"
)]
mod support;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::routing::post;
use chrono::TimeDelta;
use chrono::Utc;
use rocketmq_sre_contracts::AdvanceFenceRequest;
use rocketmq_sre_contracts::AgentDispatchAuthorization;
use rocketmq_sre_contracts::AgentDispatchRequest;
use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::AgentStepRequest;
use rocketmq_sre_contracts::EffectState;
use rocketmq_sre_contracts::FenceAck;
use rocketmq_sre_contracts::GrantVerification;
use rocketmq_sre_contracts::IssueFenceGrantRequest;
use rocketmq_sre_contracts::LEASE_AUTHORITY_SCHEMA_VERSION;
use rocketmq_sre_contracts::LeaseFenceGrant;
use rocketmq_sre_contracts::ReconcileEffectResponse;
use rocketmq_sre_contracts::ReconcileEffectState;
use rocketmq_sre_contracts::ReconcileGrant;
use rocketmq_sre_contracts::VerifyFenceGrantRequest;
use rocketmq_sre_contracts::VerifyReconcileGrantRequest;
use rocketmq_sre_execution_agent::AgentActionHandler;
use rocketmq_sre_execution_agent::AgentDriverRegistry;
use rocketmq_sre_execution_agent::AgentEffectStore;
use rocketmq_sre_execution_agent::DispatchBarrier;
use rocketmq_sre_execution_agent::DriverDispatchOutcome;
use rocketmq_sre_execution_agent::DriverFuture;
use rocketmq_sre_execution_agent::ExecutionAgent;
use rocketmq_sre_execution_agent::FenceAckSigner;
use rocketmq_sre_execution_agent::HttpLeaseAuthorityClient;
use rocketmq_sre_executor::ExecutionAgentClient;
use rocketmq_sre_executor::ExecutionJournal;
use rocketmq_sre_executor::ExecutorAuthorityClient;
use rocketmq_sre_executor::ExecutorError;
use rocketmq_sre_executor::HttpExecutionAgentClient;
use rocketmq_sre_executor::HttpExecutorAuthorityClient;
use rocketmq_sre_executor::LeaseCoordinator;
use tokio::io::copy_bidirectional;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;
use url::Url;
use uuid::Uuid;

use support::cleanup_schema;
use support::isolated_pool;
use support::seed_fixture;
use support::step_intent;

const AUTHORITY_TOKEN: &str = "partition-authority-fixture-token";
const AGENT_TOKEN: &str = "partition-agent-fixture-token";
const OLD_EXECUTOR_SUBJECT: &str = "executor-before-partition";
const AGENT_SUBJECT: &str = "execution-agent-partition-fixture";

#[derive(Clone)]
struct AuthorityState {
    tenant_id: rocketmq_sre_contracts::TenantId,
    cluster_id: rocketmq_sre_contracts::ClusterId,
    active_epoch: Arc<AtomicU64>,
    pending_epoch: Arc<AtomicU64>,
    old_grant: Arc<LeaseFenceGrant>,
    agent_verifications: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct CountingDriver {
    writes: Arc<AtomicUsize>,
}

impl AgentActionHandler for CountingDriver {
    fn read_state<'a>(&'a self, request: &'a AgentReadRequest) -> DriverFuture<'a, AgentReadResult> {
        Box::pin(async move {
            Ok(AgentReadResult {
                schema_version: rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                action: request.action,
                target: request.target.clone(),
                precondition_hash: format!("sha256:{}", "a".repeat(64)),
                ready: true,
                reason_codes: Vec::new(),
                resource_conditions: Default::default(),
                observed_at: Utc::now(),
            })
        })
    }

    fn dispatch<'a>(
        &'a self,
        _request: &'a AgentStepRequest,
        operation_id: &'a str,
    ) -> DriverFuture<'a, DriverDispatchOutcome> {
        let writes = Arc::clone(&self.writes);
        let operation_id = operation_id.to_owned();
        Box::pin(async move {
            writes.fetch_add(1, Ordering::SeqCst);
            Ok(DriverDispatchOutcome {
                operation_id,
                outcome_code: "applied".to_owned(),
                sanitized_summary: "one bounded partition qualification effect was applied".to_owned(),
            })
        })
    }

    fn reconcile<'a>(
        &'a self,
        _request: &'a AgentReadRequest,
        _operation_id: Option<&str>,
    ) -> DriverFuture<'a, ReconcileEffectResponse> {
        Box::pin(async {
            Ok(ReconcileEffectResponse {
                schema_version: rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                state: ReconcileEffectState::Applied,
                outcome_code: "applied".to_owned(),
                sanitized_summary: "bounded target state confirms the effect".to_owned(),
                observed_at: Utc::now(),
            })
        })
    }

    fn compensate<'a>(
        &'a self,
        _request: &'a AgentStepRequest,
        operation_id: &'a str,
    ) -> DriverFuture<'a, DriverDispatchOutcome> {
        let operation_id = operation_id.to_owned();
        Box::pin(async move {
            Ok(DriverDispatchOutcome {
                operation_id,
                outcome_code: "compensated".to_owned(),
                sanitized_summary: "bounded partition qualification effect was compensated".to_owned(),
            })
        })
    }
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to disposable Docker PostgreSQL"]
async fn old_executor_cannot_write_after_asymmetric_authority_partition_and_epoch_takeover() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let schema = format!("asymmetric_fence_{}", Uuid::new_v4().simple());
    let pool = isolated_pool(&database_url, &schema).await;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("empty-schema migrations");
    let fixture = seed_fixture(&pool).await;
    seed_execution(&pool, &fixture).await;
    let leases = LeaseCoordinator::new(pool.clone());
    let store = AgentEffectStore::new(pool.clone());
    let now = Utc::now();
    let old_lease = activate_initial_lease(&leases, &store, &fixture, now).await;
    let old_intent = step_intent(
        &fixture,
        old_lease.id,
        old_lease.epoch,
        &old_lease.owner,
        old_lease.expires_at,
        "asymmetric-stale-dispatch",
        now + TimeDelta::seconds(1),
    );
    let old_grant_request = issue_request(&fixture, &old_intent);
    let authority_state = AuthorityState {
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        active_epoch: Arc::new(AtomicU64::new(old_lease.epoch.0)),
        pending_epoch: Arc::new(AtomicU64::new(0)),
        old_grant: Arc::new(old_intent.fence_grant.clone()),
        agent_verifications: Arc::new(AtomicUsize::new(0)),
    };
    let authority_router = Router::new()
        .route(
            "/internal/v1/execution-authority/leases/fence-grant",
            post(issue_old_grant),
        )
        .route(
            "/internal/v1/execution-authority/verify/fence-grant",
            post(verify_fence_grant),
        )
        .route(
            "/internal/v1/execution-authority/verify/reconcile-grant",
            post(verify_reconcile_grant),
        )
        .with_state(authority_state.clone());
    let authority = start_http_server(authority_router).await;
    let (old_authority_url, partition, partition_task) = start_partitionable_proxy(authority.address).await;
    let old_authority = HttpExecutorAuthorityClient::new(
        old_authority_url,
        AUTHORITY_TOKEN,
        OLD_EXECUTOR_SUBJECT,
        Duration::from_secs(2),
        true,
    )
    .expect("old Executor authority client");
    let old_grant = old_authority
        .issue_fence_grant(&old_grant_request)
        .await
        .expect("epoch-N grant before partition");
    assert_eq!(old_grant, old_intent.fence_grant);

    let writes = Arc::new(AtomicUsize::new(0));
    let mut registry = AgentDriverRegistry::empty();
    registry
        .register_kubernetes(
            fixture.plan.steps[0].action,
            CountingDriver {
                writes: Arc::clone(&writes),
            },
        )
        .expect("closed qualification driver registration");
    let agent_authority = Arc::new(
        HttpLeaseAuthorityClient::new(
            authority.url.clone(),
            AUTHORITY_TOKEN,
            AGENT_SUBJECT,
            Duration::from_secs(2),
            true,
        )
        .expect("Agent authority client"),
    );
    let agent = ExecutionAgent::new(
        store.clone(),
        DispatchBarrier::new(pool.clone()),
        agent_authority,
        registry,
        FenceAckSigner::new("partition-agent-signing-key-at-least-32-bytes", AGENT_SUBJECT).expect("Agent signer"),
        Duration::from_secs(5),
    );
    let agent_metrics = agent.clone();
    let agent_server = start_http_server(rocketmq_sre_execution_agent::build_router(agent, AGENT_TOKEN, false)).await;
    let old_agent = HttpExecutionAgentClient::new(agent_server.url.clone(), AGENT_TOKEN, Duration::from_secs(3), true)
        .expect("old Executor Agent client");
    old_agent
        .capabilities()
        .await
        .expect("Agent reachable before partition");

    partition.send(true).expect("partition control receiver");
    partition_task.await.expect("partition proxy task");
    assert!(matches!(
        old_authority.issue_fence_grant(&old_grant_request).await,
        Err(ExecutorError::Http(_) | ExecutorError::AuthorityUnavailable)
    ));
    old_agent
        .capabilities()
        .await
        .expect("old Executor retains Agent reachability after authority partition");

    let new_lease = leases
        .begin_takeover(
            fixture.tenant_id,
            fixture.cluster_id,
            "executor-after-partition",
            "pending-after-partition",
            now + TimeDelta::seconds(2),
            now + TimeDelta::minutes(10),
        )
        .await
        .expect("new pending epoch");
    assert!(new_lease.epoch > old_lease.epoch);
    authority_state.pending_epoch.store(new_lease.epoch.0, Ordering::SeqCst);
    let reconcile_grant = ReconcileGrant {
        lease_id: new_lease.id,
        owner: new_lease.owner.clone(),
        cluster_id: new_lease.cluster_id,
        pending_epoch: new_lease.epoch,
        audience: "rocketmq-sre-execution-agent-reconcile".to_owned(),
        issued_at: new_lease.acquired_at,
        expires_at: new_lease.expires_at,
        nonce: new_lease.pending_nonce.clone(),
        signature: "partition-reconcile-fixture-signature".to_owned(),
    };
    let fence_ack = old_agent
        .advance_fence(&AdvanceFenceRequest {
            schema_version: rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
            tenant_id: fixture.tenant_id,
            reconcile_grant,
        })
        .await
        .expect("Agent advances durable fence through its independent authority path")
        .fence_ack;
    leases
        .activate(&new_lease, &fence_ack)
        .await
        .expect("new epoch activation");
    authority_state.active_epoch.store(new_lease.epoch.0, Ordering::SeqCst);
    authority_state.pending_epoch.store(0, Ordering::SeqCst);

    let stale_request = dispatch_request(&fixture, old_intent);
    assert!(matches!(
        old_agent.dispatch(&stale_request).await,
        Err(ExecutorError::AgentRejected)
    ));
    let stale_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution_agent_effects WHERE idempotency_key = $1")
        .bind("asymmetric-stale-dispatch")
        .fetch_one(&pool)
        .await
        .expect("stale effect count");
    assert_eq!(stale_rows, 0);
    assert_eq!(writes.load(Ordering::SeqCst), 0);

    let fresh_intent = step_intent(
        &fixture,
        new_lease.id,
        new_lease.epoch,
        &new_lease.owner,
        new_lease.expires_at,
        "asymmetric-fresh-dispatch",
        now + TimeDelta::seconds(3),
    );
    let fresh = old_agent
        .dispatch(&dispatch_request(&fixture, fresh_intent))
        .await
        .expect("fresh epoch dispatch remains available");
    assert_eq!(fresh.result.state, EffectState::Confirmed);
    assert!(!fresh.replayed);
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    let metrics = agent_metrics.metrics();
    assert!(metrics.fence_rejections_total >= 1);
    assert!(authority_state.agent_verifications.load(Ordering::SeqCst) >= 3);

    println!(
        "ASYMMETRIC_EXECUTOR_PARTITION_OK old_authority_reachable=false \
         old_agent_reachable=true agent_authority_reachable=true old_epoch={} active_epoch={} \
         stale_dispatch_rejected=true stale_effect_rows={} stale_target_writes=0 \
         fresh_target_writes={} fence_rejections={}",
        old_lease.epoch.0,
        new_lease.epoch.0,
        stale_rows,
        writes.load(Ordering::SeqCst),
        metrics.fence_rejections_total,
    );

    agent_server.stop().await;
    authority.stop().await;
    cleanup_schema(&pool, &schema).await;
}

async fn issue_old_grant(
    State(state): State<AuthorityState>,
    headers: HeaderMap,
    Json(request): Json<IssueFenceGrantRequest>,
) -> Result<Json<LeaseFenceGrant>, StatusCode> {
    require_identity(&headers, OLD_EXECUTOR_SUBJECT)?;
    let grant = state.old_grant.as_ref();
    if request.tenant_id != state.tenant_id
        || request.cluster_id != state.cluster_id
        || request.lease_id != grant.lease_id
        || request.epoch.0 != state.active_epoch.load(Ordering::SeqCst)
        || request.execution_id != grant.execution_id
        || request.step_id != grant.step_id
        || request.plan_step_id != grant.plan_step_id
        || request.compensation != grant.compensation
    {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(Json(grant.clone()))
}

async fn verify_fence_grant(
    State(state): State<AuthorityState>,
    headers: HeaderMap,
    Json(request): Json<VerifyFenceGrantRequest>,
) -> Result<Json<GrantVerification>, StatusCode> {
    require_identity(&headers, AGENT_SUBJECT)?;
    if request.tenant_id != state.tenant_id
        || request.grant.cluster_id != state.cluster_id
        || request.grant.epoch.0 != state.active_epoch.load(Ordering::SeqCst)
        || request.grant.expires_at <= Utc::now()
    {
        return Err(StatusCode::FORBIDDEN);
    }
    state.agent_verifications.fetch_add(1, Ordering::SeqCst);
    Ok(Json(GrantVerification {
        schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
        valid: true,
        cluster_id: request.grant.cluster_id,
        epoch: request.grant.epoch,
        expires_at: request.grant.expires_at,
    }))
}

async fn verify_reconcile_grant(
    State(state): State<AuthorityState>,
    headers: HeaderMap,
    Json(request): Json<VerifyReconcileGrantRequest>,
) -> Result<Json<GrantVerification>, StatusCode> {
    require_identity(&headers, AGENT_SUBJECT)?;
    if request.tenant_id != state.tenant_id
        || request.grant.cluster_id != state.cluster_id
        || request.grant.pending_epoch.0 != state.pending_epoch.load(Ordering::SeqCst)
        || request.grant.expires_at <= Utc::now()
    {
        return Err(StatusCode::FORBIDDEN);
    }
    state.agent_verifications.fetch_add(1, Ordering::SeqCst);
    Ok(Json(GrantVerification {
        schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
        valid: true,
        cluster_id: request.grant.cluster_id,
        epoch: request.grant.pending_epoch,
        expires_at: request.grant.expires_at,
    }))
}

fn require_identity(headers: &HeaderMap, expected_subject: &str) -> Result<(), StatusCode> {
    let bearer = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    let subject = headers.get("x-rocketmq-subject").and_then(|value| value.to_str().ok());
    if bearer != Some(&format!("Bearer {AUTHORITY_TOKEN}")) || subject != Some(expected_subject) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(())
}

fn issue_request(fixture: &support::Fixture, intent: &rocketmq_sre_contracts::StepIntent) -> IssueFenceGrantRequest {
    IssueFenceGrantRequest {
        schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        lease_id: intent.fence_grant.lease_id,
        epoch: intent.fence_grant.epoch,
        execution_id: intent.execution_id,
        step_id: intent.step_id,
        plan_step_id: intent.step.id,
        compensation: false,
    }
}

fn dispatch_request(fixture: &support::Fixture, intent: rocketmq_sre_contracts::StepIntent) -> AgentDispatchRequest {
    AgentDispatchRequest {
        schema_version: rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
        tenant_id: fixture.tenant_id,
        plan_id: Some(fixture.plan.id),
        authorization: AgentDispatchAuthorization::HumanApproved,
        request: AgentStepRequest {
            action: intent.step.action,
            descriptor_version: intent.step.descriptor_version.clone(),
            target: intent.step.resource.clone(),
            parameters: intent.step.parameters.clone(),
            intent,
        },
    }
}

async fn activate_initial_lease(
    leases: &LeaseCoordinator,
    store: &AgentEffectStore,
    fixture: &support::Fixture,
    now: chrono::DateTime<Utc>,
) -> rocketmq_sre_executor::ExecutorLeaseRecord {
    let pending = leases
        .begin_takeover(
            fixture.tenant_id,
            fixture.cluster_id,
            OLD_EXECUTOR_SUBJECT,
            "pending-before-partition",
            now,
            now + TimeDelta::minutes(10),
        )
        .await
        .expect("initial pending lease");
    let ack = FenceAck {
        cluster_id: fixture.cluster_id,
        epoch: pending.epoch,
        pending_nonce: pending.pending_nonce.clone(),
        agent_subject: AGENT_SUBJECT.to_owned(),
        acknowledged_at: now + TimeDelta::milliseconds(1),
        signature: "initial-fence-fixture-signature".to_owned(),
    };
    store
        .accept_fence(fixture.tenant_id, pending.id, &ack)
        .await
        .expect("initial durable Agent fence");
    leases.activate(&pending, &ack).await.expect("initial active lease")
}

async fn seed_execution(pool: &sqlx::PgPool, fixture: &support::Fixture) {
    ExecutionJournal::new(pool.clone(), "rocketmq-sre-executor")
        .create_execution(
            &fixture.request,
            &fixture.plan.steps[0].resource,
            fixture.plan.steps[0].action,
            Utc::now(),
        )
        .await
        .expect("durable execution before Agent effect");
}

struct RunningServer {
    address: SocketAddr,
    url: Url,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl RunningServer {
    async fn stop(self) {
        let _ = self.shutdown.send(());
        self.task.await.expect("HTTP server task");
    }
}

async fn start_http_server(router: Router) -> RunningServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("ephemeral HTTP listener");
    let address = listener.local_addr().expect("HTTP listener address");
    let url = Url::parse(&format!("http://{address}/")).expect("loopback URL");
    let (shutdown, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .expect("qualification HTTP server");
    });
    RunningServer {
        address,
        url,
        shutdown,
        task,
    }
}

async fn start_partitionable_proxy(target: SocketAddr) -> (Url, watch::Sender<bool>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("partition proxy listener");
    let address = listener.local_addr().expect("partition proxy address");
    let url = Url::parse(&format!("http://{address}/")).expect("partition proxy URL");
    let (partition, mut partitioned) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accept = listener.accept() => {
                    let Ok((mut inbound, _)) = accept else {
                        break;
                    };
                    connections.spawn(async move {
                        if let Ok(mut outbound) = TcpStream::connect(target).await {
                            let _ = copy_bidirectional(&mut inbound, &mut outbound).await;
                        }
                    });
                }
                changed = partitioned.changed() => {
                    if changed.is_err() || *partitioned.borrow() {
                        break;
                    }
                }
            }
        }
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    });
    (url, partition, task)
}
