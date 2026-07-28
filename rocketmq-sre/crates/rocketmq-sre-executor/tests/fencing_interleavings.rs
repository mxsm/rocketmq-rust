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
mod support;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chrono::TimeDelta;
use chrono::Utc;
use rocketmq_sre_contracts::AdvanceFenceRequest;
use rocketmq_sre_contracts::AgentDispatchRequest;
use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::AgentStepRequest;
use rocketmq_sre_contracts::EffectState;
use rocketmq_sre_contracts::FenceAck;
use rocketmq_sre_contracts::GrantVerification;
use rocketmq_sre_contracts::LEASE_AUTHORITY_SCHEMA_VERSION;
use rocketmq_sre_contracts::LeaseFenceGrant;
use rocketmq_sre_contracts::ReconcileEffectResponse;
use rocketmq_sre_contracts::ReconcileEffectState;
use rocketmq_sre_contracts::ReconcileGrant;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_execution_agent::AgentActionHandler;
use rocketmq_sre_execution_agent::AgentDriverRegistry;
use rocketmq_sre_execution_agent::AgentEffectStore;
use rocketmq_sre_execution_agent::AuthorityFuture;
use rocketmq_sre_execution_agent::DispatchBarrier;
use rocketmq_sre_execution_agent::DriverDispatchOutcome;
use rocketmq_sre_execution_agent::DriverFuture;
use rocketmq_sre_execution_agent::ExecutionAgent;
use rocketmq_sre_execution_agent::ExecutionAgentError;
use rocketmq_sre_execution_agent::FenceAckSigner;
use rocketmq_sre_execution_agent::KubernetesDriver;
use rocketmq_sre_execution_agent::LeaseAuthorityClient;
use rocketmq_sre_executor::ExecutionJournal;
use rocketmq_sre_executor::LeaseCoordinator;
use tokio::sync::Notify;
use tokio::time::timeout;
use uuid::Uuid;

use support::assert_critic_review_is_immutable;
use support::assert_phase_three_tables;
use support::cleanup_schema;
use support::isolated_pool;
use support::seed_fixture;
use support::step_intent;

#[derive(Clone)]
struct AcceptingAuthority;

impl LeaseAuthorityClient for AcceptingAuthority {
    fn verify_fence_grant<'a>(&'a self, _tenant_id: TenantId, grant: &'a LeaseFenceGrant) -> AuthorityFuture<'a> {
        Box::pin(async move {
            Ok(GrantVerification {
                schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
                valid: true,
                cluster_id: grant.cluster_id,
                epoch: grant.epoch,
                expires_at: grant.expires_at,
            })
        })
    }

    fn verify_reconcile_grant<'a>(&'a self, _tenant_id: TenantId, grant: &'a ReconcileGrant) -> AuthorityFuture<'a> {
        Box::pin(async move {
            Ok(GrantVerification {
                schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
                valid: true,
                cluster_id: grant.cluster_id,
                epoch: grant.pending_epoch,
                expires_at: grant.expires_at,
            })
        })
    }
}

#[derive(Clone)]
struct BlockingKubernetesDriver {
    started: Arc<Notify>,
    release: Arc<Notify>,
    writes: Arc<AtomicUsize>,
}

impl AgentActionHandler for BlockingKubernetesDriver {
    fn read_state<'a>(&'a self, request: &'a AgentReadRequest) -> DriverFuture<'a, AgentReadResult> {
        Box::pin(async move {
            Ok(AgentReadResult {
                schema_version: rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                action: request.action,
                target: request.target.clone(),
                precondition_hash: format!("sha256:{}", "b".repeat(64)),
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
        let started = Arc::clone(&self.started);
        let release = Arc::clone(&self.release);
        let writes = Arc::clone(&self.writes);
        let operation_id = operation_id.to_owned();
        Box::pin(async move {
            started.notify_one();
            release.notified().await;
            writes.fetch_add(1, Ordering::SeqCst);
            Ok(DriverDispatchOutcome {
                operation_id,
                outcome_code: "applied".to_owned(),
                sanitized_summary: "one bounded proxy replica was added".to_owned(),
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
                sanitized_summary: "live state confirms the bounded change".to_owned(),
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
                sanitized_summary: "bounded proxy replica compensation completed".to_owned(),
            })
        })
    }
}

impl KubernetesDriver for BlockingKubernetesDriver {}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn fence_ack_waits_for_inflight_dispatch_and_old_epoch_cannot_write_after_ack() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let schema = format!("phase3_fence_{}", Uuid::new_v4().simple());
    let pool = isolated_pool(&database_url, &schema).await;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("empty-schema migrations");
    assert_phase_three_tables(&pool).await;
    let fixture = seed_fixture(&pool).await;
    assert_critic_review_is_immutable(&pool, &fixture).await;
    seed_execution(&pool, &fixture).await;
    let now = Utc::now();
    let leases = LeaseCoordinator::new(pool.clone());
    let store = AgentEffectStore::new(pool.clone());
    let active = activate_lease(&leases, &store, &fixture, "executor-n", now).await;
    let intent = step_intent(
        &fixture,
        active.id,
        active.epoch,
        &active.owner,
        active.expires_at,
        "interleave-dispatch",
        now + TimeDelta::seconds(1),
    );
    let request = AgentDispatchRequest {
        schema_version: rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
        tenant_id: fixture.tenant_id,
        request: AgentStepRequest {
            intent: intent.clone(),
            action: intent.step.action,
            descriptor_version: intent.step.descriptor_version.clone(),
            target: intent.step.resource.clone(),
            parameters: intent.step.parameters.clone(),
        },
    };
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let writes = Arc::new(AtomicUsize::new(0));
    let mut registry = AgentDriverRegistry::empty();
    registry
        .register_kubernetes(
            intent.step.action,
            BlockingKubernetesDriver {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
                writes: Arc::clone(&writes),
            },
        )
        .expect("closed Kubernetes action registration");
    let agent = ExecutionAgent::new(
        store.clone(),
        DispatchBarrier::new(pool.clone()),
        Arc::new(AcceptingAuthority),
        registry,
        FenceAckSigner::new("agent-interleave-signing-key-at-least-32-bytes", "execution-agent-a")
            .expect("test signer"),
        Duration::from_secs(10),
    );

    let dispatch_agent = agent.clone();
    let old_request = request.clone();
    let dispatch = tokio::spawn(async move { dispatch_agent.dispatch(&old_request).await });
    timeout(Duration::from_secs(5), started.notified())
        .await
        .expect("old epoch driver started");
    assert_eq!(writes.load(Ordering::SeqCst), 0);

    let pending = leases
        .begin_takeover(
            fixture.tenant_id,
            fixture.cluster_id,
            "executor-n-plus-one",
            "pending-n-plus-one",
            now + TimeDelta::seconds(2),
            now + TimeDelta::minutes(10),
        )
        .await
        .expect("new pending epoch");
    let reconcile_grant = reconcile_grant(&pending);
    let advance_agent = agent.clone();
    let advance_request = AdvanceFenceRequest {
        schema_version: rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
        tenant_id: fixture.tenant_id,
        reconcile_grant,
    };
    let advance = tokio::spawn(async move { advance_agent.advance_fence(&advance_request).await });
    wait_for_exclusive_barrier_waiter(&pool).await;
    assert!(
        !advance.is_finished(),
        "FenceAck must wait for the shared dispatch guard"
    );

    release.notify_one();
    let dispatch_response = timeout(Duration::from_secs(5), dispatch)
        .await
        .expect("old dispatch join timeout")
        .expect("old dispatch task")
        .expect("old dispatch result");
    assert!(!dispatch_response.replayed);
    assert_eq!(dispatch_response.result.state, EffectState::Confirmed);
    let ack = timeout(Duration::from_secs(5), advance)
        .await
        .expect("fence advance join timeout")
        .expect("fence advance task")
        .expect("fence advance result");
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    assert_eq!(ack.epoch, pending.epoch);
    leases
        .activate(&pending, &ack)
        .await
        .expect("new epoch activates only after durable Agent acknowledgement");

    assert!(matches!(
        agent.dispatch(&request).await,
        Err(ExecutionAgentError::AuthorityRejected)
    ));
    assert_eq!(writes.load(Ordering::SeqCst), 1);

    cleanup_schema(&pool, &schema).await;
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn dispatched_unknown_effect_blocks_takeover_until_read_only_reconciliation_confirms_terminal_state() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let schema = format!("phase3_crash_{}", Uuid::new_v4().simple());
    let pool = isolated_pool(&database_url, &schema).await;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("empty-schema migrations");
    assert_phase_three_tables(&pool).await;
    let fixture = seed_fixture(&pool).await;
    seed_execution(&pool, &fixture).await;
    let now = Utc::now();
    let leases = LeaseCoordinator::new(pool.clone());
    let store = AgentEffectStore::new(pool.clone());
    let active = activate_lease(&leases, &store, &fixture, "executor-before-crash", now).await;
    let intent = step_intent(
        &fixture,
        active.id,
        active.epoch,
        &active.owner,
        active.expires_at,
        "crash-after-dispatch",
        now + TimeDelta::seconds(1),
    );
    let agent_request = AgentStepRequest {
        intent: intent.clone(),
        action: intent.step.action,
        descriptor_version: intent.step.descriptor_version.clone(),
        target: intent.step.resource.clone(),
        parameters: intent.step.parameters.clone(),
    };
    store
        .prepare(fixture.tenant_id, &agent_request, now + TimeDelta::seconds(1))
        .await
        .expect("effect prepared before driver call");
    store
        .mark_dispatched(
            &intent.idempotency_key,
            "target-operation-delayed",
            now + TimeDelta::seconds(2),
        )
        .await
        .expect("dispatch marker durable before simulated crash");
    let pending = leases
        .begin_takeover(
            fixture.tenant_id,
            fixture.cluster_id,
            "executor-after-crash",
            "pending-after-crash",
            now + TimeDelta::seconds(3),
            now + TimeDelta::minutes(10),
        )
        .await
        .expect("pending takeover");
    let agent = ExecutionAgent::new(
        store.clone(),
        DispatchBarrier::new(pool.clone()),
        Arc::new(AcceptingAuthority),
        AgentDriverRegistry::empty(),
        FenceAckSigner::new("agent-crash-signing-key-at-least-32-bytes", "execution-agent-a").expect("test signer"),
        Duration::from_secs(5),
    );
    let advance_request = AdvanceFenceRequest {
        schema_version: rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
        tenant_id: fixture.tenant_id,
        reconcile_grant: reconcile_grant(&pending),
    };

    assert!(matches!(
        agent.advance_fence(&advance_request).await,
        Err(ExecutionAgentError::UnresolvedEffect)
    ));
    assert_eq!(
        store
            .highest_epoch(fixture.cluster_id)
            .await
            .expect("old durable epoch"),
        Some(active.epoch)
    );

    store
        .confirm(
            &intent.idempotency_key,
            "applied_after_crash",
            "read-only reconciliation confirmed delayed target effect",
            now + TimeDelta::seconds(4),
        )
        .await
        .expect("reconciliation records terminal result without redispatch");
    let ack = agent
        .advance_fence(&advance_request)
        .await
        .expect("takeover after terminal reconciliation");
    leases
        .activate(&pending, &ack)
        .await
        .expect("new epoch activation after reconciliation");
    assert_eq!(
        store
            .effect(&intent.idempotency_key)
            .await
            .expect("durable effect")
            .state,
        EffectState::Confirmed
    );

    cleanup_schema(&pool, &schema).await;
}

async fn activate_lease(
    leases: &LeaseCoordinator,
    store: &AgentEffectStore,
    fixture: &support::Fixture,
    owner: &str,
    now: chrono::DateTime<Utc>,
) -> rocketmq_sre_executor::ExecutorLeaseRecord {
    let pending = leases
        .begin_takeover(
            fixture.tenant_id,
            fixture.cluster_id,
            owner,
            &format!("pending-{owner}"),
            now,
            now + TimeDelta::minutes(10),
        )
        .await
        .expect("pending lease");
    let ack = FenceAck {
        cluster_id: fixture.cluster_id,
        epoch: pending.epoch,
        pending_nonce: pending.pending_nonce.clone(),
        agent_subject: "execution-agent-a".to_owned(),
        acknowledged_at: now + TimeDelta::milliseconds(1),
        signature: "fixture-fence-ack".to_owned(),
    };
    store
        .accept_fence(fixture.tenant_id, pending.id, &ack)
        .await
        .expect("persist initial Agent epoch");
    leases.activate(&pending, &ack).await.expect("activate initial epoch")
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

fn reconcile_grant(lease: &rocketmq_sre_executor::ExecutorLeaseRecord) -> ReconcileGrant {
    ReconcileGrant {
        lease_id: lease.id,
        owner: lease.owner.clone(),
        cluster_id: lease.cluster_id,
        pending_epoch: lease.epoch,
        audience: "rocketmq-sre-execution-agent-reconcile".to_owned(),
        issued_at: lease.acquired_at,
        expires_at: lease.expires_at,
        nonce: lease.pending_nonce.clone(),
        signature: "fixture-reconcile-grant".to_owned(),
    }
}

async fn wait_for_exclusive_barrier_waiter(pool: &sqlx::PgPool) {
    timeout(Duration::from_secs(5), async {
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)
                 FROM pg_locks
                 WHERE locktype = 'advisory' AND NOT granted",
            )
            .fetch_one(pool)
            .await
            .expect("advisory lock wait observation");
            if waiting > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("exclusive fence barrier should wait behind the old shared dispatch");
}
