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

#[allow(
    dead_code,
    reason = "the shared PostgreSQL fixture exposes recovery helpers not used by this focused flow test"
)]
#[path = "postgres_recovery/support.rs"]
mod support;

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chrono::TimeDelta;
use chrono::Utc;
use rocketmq_sre_contracts::ActivateLeaseRequest;
use rocketmq_sre_contracts::AdvanceFenceRequest;
use rocketmq_sre_contracts::AdvanceFenceResponse;
use rocketmq_sre_contracts::AgentDispatchRequest;
use rocketmq_sre_contracts::AgentDispatchResponse;
use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::AgentStepResult;
use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::AutonomyCohortId;
use rocketmq_sre_contracts::AutonomyGrant;
use rocketmq_sre_contracts::AutonomyPolicyId;
use rocketmq_sre_contracts::BeginLeaseTakeoverRequest;
use rocketmq_sre_contracts::BeginLeaseTakeoverResponse;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::CriticReviewId;
use rocketmq_sre_contracts::DynamicSafetyDecision;
use rocketmq_sre_contracts::DynamicSafetyDecisionId;
use rocketmq_sre_contracts::DynamicSafetyEvaluationRequest;
use rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION;
use rocketmq_sre_contracts::EffectState;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceExposure;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionAgentCapabilities;
use rocketmq_sre_contracts::ExecutionState;
use rocketmq_sre_contracts::ExecutionTransition;
use rocketmq_sre_contracts::ExecutorLease;
use rocketmq_sre_contracts::FenceAck;
use rocketmq_sre_contracts::GrantVerification;
use rocketmq_sre_contracts::IssueFenceGrantRequest;
use rocketmq_sre_contracts::LEASE_AUTHORITY_SCHEMA_VERSION;
use rocketmq_sre_contracts::LeaseFenceGrant;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::ReconcileEffectRequest;
use rocketmq_sre_contracts::ReconcileEffectResponse;
use rocketmq_sre_contracts::ReconcileEffectState;
use rocketmq_sre_contracts::ReconcileGrant;
use rocketmq_sre_contracts::ResourceLockId;
use rocketmq_sre_contracts::Sensitivity;
use rocketmq_sre_contracts::StepResult;
use rocketmq_sre_contracts::TimeRange;
use rocketmq_sre_contracts::VerifyExecutionRequest;
use rocketmq_sre_contracts::current_evidence_schema;
use rocketmq_sre_execution_agent::AgentEffectStore;
use rocketmq_sre_executor::ChangeExecutor;
use rocketmq_sre_executor::ExecutionAgentClient;
use rocketmq_sre_executor::ExecutionJournal;
use rocketmq_sre_executor::ExecutionPrechecker;
use rocketmq_sre_executor::ExecutionVerifier;
use rocketmq_sre_executor::ExecutorActionRegistry;
use rocketmq_sre_executor::ExecutorAuthorityClient;
use rocketmq_sre_executor::ExecutorError;
use rocketmq_sre_executor::LeaseCoordinator;
use rocketmq_sre_executor::ResourceLockRequest;
use rocketmq_sre_executor::ResourceSafetyStore;
use rocketmq_sre_executor::VerificationCaptureRequest;
use rocketmq_sre_executor::VerificationFuture;
use rocketmq_sre_executor::VerificationObservation;
use rocketmq_sre_executor::VerificationSource;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use support::audit;
use support::cleanup_schema;
use support::isolated_pool;
use support::seed_fixture;
use support::seed_logger_fixture;
use support::seed_proxy_restart_fixture;
use support::seed_telemetry_collector_restart_fixture;
use support::step_intent;

type TestFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ExecutorError>> + Send + 'a>>;

#[derive(Clone)]
struct TestAuthority {
    leases: LeaseCoordinator,
    owner: Arc<str>,
    action: ExecutionAction,
    resource: Arc<str>,
    dynamic_safety_calls: Arc<AtomicUsize>,
}

impl ExecutorAuthorityClient for TestAuthority {
    fn verify_execution<'a>(&'a self, request: &'a VerifyExecutionRequest) -> TestFuture<'a, GrantVerification> {
        Box::pin(async move {
            Ok(GrantVerification {
                schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
                valid: true,
                cluster_id: request.execution.cluster_id,
                epoch: rocketmq_sre_contracts::LeaseEpoch(0),
                expires_at: request.execution.expires_at,
            })
        })
    }

    fn begin_takeover<'a>(
        &'a self,
        request: &'a BeginLeaseTakeoverRequest,
    ) -> TestFuture<'a, BeginLeaseTakeoverResponse> {
        Box::pin(async move {
            let acquired_at = Utc::now();
            let nonce = format!("test-takeover-{}", Uuid::new_v4());
            let lease = self
                .leases
                .begin_takeover(
                    request.tenant_id,
                    request.cluster_id,
                    &self.owner,
                    &nonce,
                    acquired_at,
                    acquired_at + TimeDelta::seconds(i64::from(request.requested_ttl_seconds)),
                )
                .await?;
            Ok(BeginLeaseTakeoverResponse {
                schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
                lease: lease_contract(&lease),
                reconcile_grant: ReconcileGrant {
                    lease_id: lease.id,
                    owner: lease.owner,
                    cluster_id: lease.cluster_id,
                    pending_epoch: lease.epoch,
                    audience: "rocketmq-sre-execution-agent".to_owned(),
                    issued_at: acquired_at,
                    expires_at: lease.expires_at,
                    nonce,
                    signature: "test-reconcile-signature".to_owned(),
                },
            })
        })
    }

    fn activate<'a>(
        &'a self,
        _tenant_id: rocketmq_sre_contracts::TenantId,
        _cluster_id: rocketmq_sre_contracts::ClusterId,
        request: &'a ActivateLeaseRequest,
    ) -> TestFuture<'a, ExecutorLease> {
        Box::pin(async move {
            let pending = self.leases.lease(request.lease_id).await?;
            let active = self.leases.activate(&pending, &request.fence_ack).await?;
            Ok(lease_contract(&active))
        })
    }

    fn issue_fence_grant<'a>(&'a self, request: &'a IssueFenceGrantRequest) -> TestFuture<'a, LeaseFenceGrant> {
        Box::pin(async move {
            let issued_at = Utc::now();
            Ok(LeaseFenceGrant {
                lease_id: request.lease_id,
                owner: self.owner.to_string(),
                cluster_id: request.cluster_id,
                epoch: request.epoch,
                execution_id: request.execution_id,
                step_id: request.step_id,
                plan_step_id: request.plan_step_id,
                action: self.action,
                resource: self.resource.to_string(),
                compensation: request.compensation,
                audience: "rocketmq-sre-execution-agent".to_owned(),
                issued_at,
                expires_at: issued_at + TimeDelta::minutes(2),
                nonce: format!("test-grant-{}", request.step_id),
                signature: "test-fence-signature".to_owned(),
            })
        })
    }

    fn evaluate_dynamic_safety<'a>(
        &'a self,
        request: &'a DynamicSafetyEvaluationRequest,
    ) -> TestFuture<'a, DynamicSafetyDecision> {
        Box::pin(async move {
            self.dynamic_safety_calls.fetch_add(1, Ordering::Relaxed);
            let issued_at = Utc::now();
            Ok(DynamicSafetyDecision {
                id: DynamicSafetyDecisionId::new(),
                tenant_id: request.tenant_id,
                cluster_id: request.cluster_id,
                action: request.action,
                action_version: request.action_version.clone(),
                plan_id: request.plan_id,
                plan_hash: request.plan_hash.clone(),
                execution_id: request.execution_id,
                execution_step_id: request.execution_step_id,
                policy_definition_version: request.policy_definition_version,
                lifecycle_revision: request.lifecycle_revision,
                error_budget_available: true,
                freeze_revision: 0,
                kill_switch_revision: 0,
                evidence_fresh: true,
                allowed: true,
                reason_codes: Vec::new(),
                issued_at,
                expires_at: issued_at + TimeDelta::seconds(30),
                nonce: format!("dynamic-safety-{}", request.execution_step_id),
                signature: "test-dynamic-safety-signature".to_owned(),
            })
        })
    }
}

#[derive(Clone)]
struct TestAgent {
    effects: AgentEffectStore,
    action: ExecutionAction,
    precondition_hash: Arc<str>,
    dispatches: Arc<Mutex<Vec<bool>>>,
    reconcile_state: ReconcileEffectState,
}

impl ExecutionAgentClient for TestAgent {
    fn capabilities<'a>(&'a self) -> TestFuture<'a, ExecutionAgentCapabilities> {
        Box::pin(async {
            Ok(ExecutionAgentCapabilities {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                registered_actions: vec![self.action],
                raw_admin_request_supported: false,
                arbitrary_json_patch_supported: false,
                shell_supported: false,
                durable_fencing: true,
            })
        })
    }

    fn precheck<'a>(&'a self, request: &'a AgentReadRequest) -> TestFuture<'a, AgentReadResult> {
        Box::pin(async move {
            Ok(AgentReadResult {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                action: request.action,
                target: request.target.clone(),
                precondition_hash: self.precondition_hash.to_string(),
                ready: true,
                reason_codes: Vec::new(),
                resource_conditions: Default::default(),
                observed_at: Utc::now(),
            })
        })
    }

    fn dispatch<'a>(&'a self, request: &'a AgentDispatchRequest) -> TestFuture<'a, AgentDispatchResponse> {
        Box::pin(async move {
            let compensation = request.request.intent.compensation;
            self.dispatches.lock().expect("test dispatch lock").push(compensation);
            Ok(AgentDispatchResponse {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                result: AgentStepResult {
                    execution_id: request.request.intent.execution_id,
                    step_id: request.request.intent.step_id,
                    state: EffectState::Confirmed,
                    operation_id: format!("test-operation-{}", request.request.intent.step_id),
                    outcome_code: if compensation {
                        "proxy_replicas_restored"
                    } else {
                        "proxy_scaled_out_one"
                    }
                    .to_owned(),
                    sanitized_summary: "bounded test driver result".to_owned(),
                    completed_at: Utc::now(),
                },
                replayed: false,
            })
        })
    }

    fn reconcile<'a>(&'a self, _request: &'a ReconcileEffectRequest) -> TestFuture<'a, ReconcileEffectResponse> {
        Box::pin(async move {
            Ok(ReconcileEffectResponse {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                state: self.reconcile_state,
                outcome_code: "test_effect_applied".to_owned(),
                sanitized_summary: "bounded test reconciliation".to_owned(),
                observed_at: Utc::now(),
            })
        })
    }

    fn advance_fence<'a>(&'a self, request: &'a AdvanceFenceRequest) -> TestFuture<'a, AdvanceFenceResponse> {
        Box::pin(async move {
            let ack = FenceAck {
                cluster_id: request.reconcile_grant.cluster_id,
                epoch: request.reconcile_grant.pending_epoch,
                pending_nonce: request.reconcile_grant.nonce.clone(),
                agent_subject: "spiffe://rocketmq-sre/execution-agent".to_owned(),
                acknowledged_at: Utc::now(),
                signature: "test-fence-ack-signature".to_owned(),
            };
            self.effects
                .accept_fence(request.tenant_id, request.reconcile_grant.lease_id, &ack)
                .await
                .map_err(|_| ExecutorError::AgentRejected)?;
            Ok(AdvanceFenceResponse {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                fence_ack: ack,
            })
        })
    }
}

#[derive(Clone, Copy)]
struct Signal {
    offset_seconds: i64,
    resource_ok: bool,
    sli_ok: bool,
}

struct ScriptedVerification {
    base: chrono::DateTime<Utc>,
    signals: Mutex<VecDeque<Signal>>,
}

impl VerificationSource for ScriptedVerification {
    fn observe<'a>(&'a self, request: &'a VerificationCaptureRequest) -> VerificationFuture<'a> {
        Box::pin(async move {
            let signal = self
                .signals
                .lock()
                .expect("verification script lock")
                .pop_front()
                .ok_or(ExecutorError::AgentUnavailable)?;
            let observed_at = self.base + TimeDelta::seconds(signal.offset_seconds);
            let resource_conditions = request
                .resource_conditions
                .iter()
                .cloned()
                .map(|condition| (condition, signal.resource_ok))
                .collect();
            let technical_slis = request
                .technical_slis
                .iter()
                .cloned()
                .map(|condition| (condition, signal.sli_ok))
                .collect();
            let query = EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: request.correlation_id,
                tenant_id: request.tenant_id,
                cluster_id: request.cluster_id,
                source: "execution-flow-test".to_owned(),
                resource: request.target.clone(),
                time_range: TimeRange::new(observed_at, observed_at).map_err(|_| ExecutorError::InvalidRequest)?,
            };
            let mut evidence = rocketmq_sre_contracts::EvidenceSnapshot::capture(
                query,
                current_evidence_schema(),
                observed_at,
                EvidenceContent::Inline(json!({
                    "resource_ok": signal.resource_ok,
                    "sli_ok": signal.sli_ok,
                })),
            )
            .map_err(|_| ExecutorError::InvalidRequest)?;
            evidence.coverage = CoverageStatus::Available;
            evidence.exposure = EvidenceExposure::Synthetic;
            evidence.sensitivity = Sensitivity::Internal;
            Ok(VerificationObservation {
                evidence,
                resource_conditions,
                technical_slis,
            })
        })
    }
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn successful_verification_reaches_succeeded_and_releases_lock() {
    let signals = [signal(0, true), signal(1, true), signal(2, true), signal(122, true)];
    let run = run_execution(&signals).await;

    assert_eq!(run.state, ExecutionState::Succeeded);
    assert_eq!(run.dispatches, vec![false]);
    assert_eq!(run.active_locks, 0);
    assert_eq!(run.compensation_intents, 0);
    cleanup_schema(&run.pool, &run.schema).await;
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn interrupted_compensation_recovers_only_after_effect_is_proven_absent() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let schema = format!("phase3_recovery_{}", Uuid::new_v4().simple());
    let pool = isolated_pool(&database_url, &schema).await;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("empty-schema migrations");
    let fixture = seed_fixture(&pool).await;
    let journal = ExecutionJournal::new(pool.clone(), "rocketmq-sre-executor");
    let now = Utc::now();
    journal
        .create_execution(
            &fixture.request,
            &fixture.plan.steps[0].resource,
            fixture.plan.steps[0].action,
            now,
        )
        .await
        .expect("interrupted execution");
    transition_execution(
        &journal,
        &fixture,
        ExecutionState::Pending,
        ExecutionState::Prechecking,
        "recovery_fixture_prechecking",
        now,
    )
    .await;

    let dynamic_safety_calls = Arc::new(AtomicUsize::new(0));
    let authority = Arc::new(TestAuthority {
        leases: LeaseCoordinator::new(pool.clone()),
        owner: Arc::from("spiffe://rocketmq-sre/executor"),
        action: fixture.plan.steps[0].action,
        resource: Arc::from(fixture.plan.steps[0].resource.as_str()),
        dynamic_safety_calls,
    });
    let agent = Arc::new(TestAgent {
        effects: AgentEffectStore::new(pool.clone()),
        action: fixture.plan.steps[0].action,
        precondition_hash: Arc::from(fixture.plan.steps[0].precondition_hash.as_str()),
        dispatches: Arc::new(Mutex::new(Vec::new())),
        reconcile_state: ReconcileEffectState::NotApplied,
    });
    let takeover = authority
        .begin_takeover(&BeginLeaseTakeoverRequest {
            schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
            tenant_id: fixture.tenant_id,
            cluster_id: fixture.cluster_id,
            requested_ttl_seconds: 120,
        })
        .await
        .expect("fixture lease takeover");
    let fence = agent
        .advance_fence(&AdvanceFenceRequest {
            schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
            tenant_id: fixture.tenant_id,
            reconcile_grant: takeover.reconcile_grant,
        })
        .await
        .expect("fixture fence advance");
    let active = authority
        .activate(
            fixture.tenant_id,
            fixture.cluster_id,
            &ActivateLeaseRequest {
                schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
                tenant_id: fixture.tenant_id,
                lease_id: takeover.lease.id,
                fence_ack: fence.fence_ack,
            },
        )
        .await
        .expect("fixture lease activation");
    let intent = step_intent(
        &fixture,
        active.id,
        active.epoch,
        &active.owner,
        active.expires_at,
        "interrupted-forward-effect",
        now + TimeDelta::seconds(1),
    );
    journal
        .append_intent_with_audit(
            &intent,
            &audit(
                &fixture,
                AuditEventKind::StepIntentPersisted,
                "recovery_fixture_intent",
                now + TimeDelta::seconds(1),
            ),
        )
        .await
        .expect("forward intent");
    transition_execution(
        &journal,
        &fixture,
        ExecutionState::Prechecking,
        ExecutionState::IntentPersisted,
        "recovery_fixture_intent_persisted",
        now + TimeDelta::seconds(2),
    )
    .await;
    transition_execution(
        &journal,
        &fixture,
        ExecutionState::IntentPersisted,
        ExecutionState::Applying,
        "recovery_fixture_applying",
        now + TimeDelta::seconds(3),
    )
    .await;
    journal
        .append_result_with_audit(
            fixture.request.id,
            intent.attempt,
            &StepResult {
                step_id: intent.step_id,
                state: ExecutionState::Verifying,
                agent_result: None,
                verification: None,
                reason_code: "forward_effect_applied".to_owned(),
                completed_at: now + TimeDelta::seconds(4),
            },
            &audit(
                &fixture,
                AuditEventKind::StepResultPersisted,
                "recovery_fixture_result",
                now + TimeDelta::seconds(4),
            ),
        )
        .await
        .expect("forward result");
    transition_execution(
        &journal,
        &fixture,
        ExecutionState::Applying,
        ExecutionState::Verifying,
        "recovery_fixture_verifying",
        now + TimeDelta::seconds(5),
    )
    .await;
    transition_execution(
        &journal,
        &fixture,
        ExecutionState::Verifying,
        ExecutionState::Compensating,
        "recovery_fixture_interrupted",
        now + TimeDelta::seconds(6),
    )
    .await;
    let safety = ResourceSafetyStore::new(pool.clone());
    safety
        .acquire(&ResourceLockRequest {
            id: ResourceLockId::new(),
            tenant_id: fixture.tenant_id,
            cluster_id: fixture.cluster_id,
            resource_key: fixture.plan.steps[0].resource.clone(),
            action: fixture.plan.steps[0].action,
            holder_execution_id: fixture.request.id,
            acquired_at: now,
            expires_at: now + TimeDelta::minutes(5),
        })
        .await
        .expect("interrupted execution lock");

    let mut descriptor: rocketmq_sre_contracts::ActionDescriptor =
        serde_yaml::from_str(include_str!("../../../config/actions/proxy.scale_out_one.v1.yaml"))
            .expect("R1 action descriptor");
    descriptor.execution_supported = true;
    let registry = Arc::new(ExecutorActionRegistry::from_descriptors([descriptor]).expect("test registry"));
    let executor = ChangeExecutor::new(
        journal.clone(),
        safety.clone(),
        authority,
        agent.clone(),
        ExecutionPrechecker::new(registry, agent),
        "spiffe://rocketmq-sre/executor",
        120,
        Duration::from_secs(300),
    );
    let recovery = executor
        .recover_interrupted_executions(100)
        .await
        .expect("effect-absent recovery sweep");
    assert_eq!(recovery.attempted, 1);
    assert_eq!(recovery.recovered, 1);
    assert_eq!(recovery.blocked, 0);
    assert_eq!(
        journal
            .execution_state(fixture.request.id)
            .await
            .expect("recovered state"),
        ExecutionState::RolledBack
    );
    assert!(
        safety
            .unreleased_for_execution(fixture.request.id)
            .await
            .expect("recovered locks")
            .is_empty()
    );
    cleanup_schema(&pool, &schema).await;
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn failed_verification_runs_compensation_and_verifies_rollback() {
    let signals = [
        signal(0, true),
        signal(1, true),
        signal(2, false),
        signal(902, false),
        signal(3, true),
        signal(4, true),
        signal(124, true),
    ];
    let run = run_execution(&signals).await;

    assert_eq!(run.state, ExecutionState::RolledBack);
    assert_eq!(run.dispatches, vec![false, true]);
    assert_eq!(run.active_locks, 0);
    assert_eq!(run.compensation_intents, 1);
    let compensation_verifications: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_verifications
         WHERE execution_id = $1 AND compensation",
    )
    .bind(run.execution_id)
    .fetch_one(&run.pool)
    .await
    .expect("compensation verification count");
    assert_eq!(compensation_verifications, 1);
    cleanup_schema(&run.pool, &run.schema).await;
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn failed_rollback_escalates_quarantines_and_releases_temporary_lock() {
    let signals = [
        signal(0, true),
        signal(1, true),
        signal(2, false),
        signal(902, false),
        signal(3, true),
        signal(4, false),
        signal(904, false),
    ];
    let run = run_execution(&signals).await;

    assert_eq!(run.state, ExecutionState::Escalated);
    assert_eq!(run.dispatches, vec![false, true]);
    assert_eq!(run.active_locks, 0);
    assert_eq!(run.active_quarantines, 1);
    cleanup_schema(&run.pool, &run.schema).await;
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn autonomous_execution_persists_live_safety_before_forward_dispatch() {
    let signals = [signal(0, true), signal(1, true), signal(2, true), signal(122, true)];
    let run = run_execution_with_authorization(&signals, true).await;

    assert_eq!(run.state, ExecutionState::Succeeded);
    assert_eq!(run.dynamic_safety_calls, 1);
    assert_eq!(run.dispatches, vec![false]);
    let safety_intents: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_steps
         WHERE execution_id = $1
           AND record_kind = 'intent'
           AND NOT compensation
           AND intent_snapshot->'dynamic_safety' IS NOT NULL",
    )
    .bind(run.execution_id)
    .fetch_one(&run.pool)
    .await
    .expect("dynamic safety intent count");
    assert_eq!(safety_intents, 1);
    cleanup_schema(&run.pool, &run.schema).await;
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn autonomous_rollback_reuses_journal_without_a_new_safety_gate() {
    let signals = [
        signal(0, true),
        signal(1, true),
        signal(2, false),
        signal(902, false),
        signal(3, true),
        signal(4, true),
        signal(124, true),
    ];
    let run = run_execution_with_authorization(&signals, true).await;

    assert_eq!(run.state, ExecutionState::RolledBack);
    assert_eq!(run.dynamic_safety_calls, 1);
    assert_eq!(run.dispatches, vec![false, true]);
    assert_eq!(run.compensation_intents, 1);
    let compensation_verifications: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_verifications
         WHERE execution_id = $1 AND compensation",
    )
    .bind(run.execution_id)
    .fetch_one(&run.pool)
    .await
    .expect("autonomous compensation verification count");
    assert_eq!(compensation_verifications, 1);
    cleanup_schema(&run.pool, &run.schema).await;
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn logger_ttl_autonomous_execution_uses_dynamic_safety_and_succeeds() {
    let signals = [signal(0, true), signal(1, true), signal(2, true), signal(32, true)];
    let run = run_logger_execution_with_authorization(&signals).await;

    assert_eq!(run.state, ExecutionState::Succeeded);
    assert_eq!(run.dynamic_safety_calls, 1);
    assert_eq!(run.dispatches, vec![false]);
    assert_eq!(run.compensation_intents, 0);
    cleanup_schema(&run.pool, &run.schema).await;
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn logger_ttl_autonomous_failure_rolls_back_without_a_second_safety_gate() {
    let signals = [
        signal(0, true),
        signal(1, true),
        signal(2, false),
        signal(122, false),
        signal(3, true),
        signal(4, true),
        signal(34, true),
    ];
    let run = run_logger_execution_with_authorization(&signals).await;

    assert_eq!(run.state, ExecutionState::RolledBack);
    assert_eq!(run.dynamic_safety_calls, 1);
    assert_eq!(run.dispatches, vec![false, true]);
    assert_eq!(run.compensation_intents, 1);
    cleanup_schema(&run.pool, &run.schema).await;
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn proxy_restart_autonomous_execution_uses_dynamic_safety_and_succeeds() {
    let signals = [signal(0, true), signal(1, true), signal(2, true), signal(182, true)];
    let run = run_execution_for_action(&signals, true, ExecutionAction::ProxyRestartOne).await;

    assert_eq!(run.state, ExecutionState::Succeeded);
    assert_eq!(run.dynamic_safety_calls, 1);
    assert_eq!(run.dispatches, vec![false]);
    assert_eq!(run.compensation_intents, 0);
    cleanup_schema(&run.pool, &run.schema).await;
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn telemetry_collector_restart_autonomous_execution_uses_dynamic_safety_and_succeeds() {
    let signals = [signal(0, true), signal(1, true), signal(2, true), signal(182, true)];
    let run = run_execution_for_action(&signals, true, ExecutionAction::TelemetryCollectorRestartOne).await;

    assert_eq!(run.state, ExecutionState::Succeeded);
    assert_eq!(run.dynamic_safety_calls, 1);
    assert_eq!(run.dispatches, vec![false]);
    assert_eq!(run.compensation_intents, 0);
    cleanup_schema(&run.pool, &run.schema).await;
}

struct ExecutionRun {
    pool: PgPool,
    schema: String,
    execution_id: Uuid,
    state: ExecutionState,
    dispatches: Vec<bool>,
    active_locks: i64,
    active_quarantines: i64,
    compensation_intents: i64,
    dynamic_safety_calls: usize,
}

async fn run_execution(signals: &[Signal]) -> ExecutionRun {
    run_execution_with_authorization(signals, false).await
}

async fn run_execution_with_authorization(signals: &[Signal], autonomous: bool) -> ExecutionRun {
    run_execution_for_action(signals, autonomous, ExecutionAction::ProxyScaleOutOne).await
}

async fn run_logger_execution_with_authorization(signals: &[Signal]) -> ExecutionRun {
    run_execution_for_action(signals, true, ExecutionAction::ObservabilityLoggerLevelTtl).await
}

async fn run_execution_for_action(signals: &[Signal], autonomous: bool, action: ExecutionAction) -> ExecutionRun {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let schema = format!("phase4_flow_{}", Uuid::new_v4().simple());
    let pool = isolated_pool(&database_url, &schema).await;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("empty-schema migrations");
    let mut fixture = match action {
        ExecutionAction::ProxyScaleOutOne => seed_fixture(&pool).await,
        ExecutionAction::ObservabilityLoggerLevelTtl => seed_logger_fixture(&pool).await,
        ExecutionAction::ProxyRestartOne => seed_proxy_restart_fixture(&pool).await,
        ExecutionAction::TelemetryCollectorRestartOne => seed_telemetry_collector_restart_fixture(&pool).await,
        _ => panic!("the executor integration flow supports only the qualified R1 scenarios under test"),
    };
    if autonomous {
        fixture.request.approvals.clear();
        fixture.request.autonomy_grant = Some(autonomy_grant(&fixture));
        fixture.request.requested_by = "autonomy-orchestrator".to_owned();
    }
    let descriptor_yaml = match action {
        ExecutionAction::ProxyScaleOutOne => include_str!("../../../config/actions/proxy.scale_out_one.v1.yaml"),
        ExecutionAction::ObservabilityLoggerLevelTtl => {
            include_str!("../../../config/actions/observability.logger_level_ttl.v1.yaml")
        }
        ExecutionAction::ProxyRestartOne => include_str!("../../../config/actions/proxy.restart_one.v1.yaml"),
        ExecutionAction::TelemetryCollectorRestartOne => {
            include_str!("../../../config/actions/telemetry.collector.restart_one.v1.yaml")
        }
        _ => panic!("the executor integration flow supports only the qualified R1 descriptors under test"),
    };
    let mut descriptor: rocketmq_sre_contracts::ActionDescriptor =
        serde_yaml::from_str(descriptor_yaml).expect("R1 action descriptor");
    descriptor.execution_supported = true;
    let registry = Arc::new(ExecutorActionRegistry::from_descriptors([descriptor]).expect("test registry"));
    let dispatches = Arc::new(Mutex::new(Vec::new()));
    let agent = Arc::new(TestAgent {
        effects: AgentEffectStore::new(pool.clone()),
        action,
        precondition_hash: Arc::from(fixture.plan.steps[0].precondition_hash.as_str()),
        dispatches: Arc::clone(&dispatches),
        reconcile_state: ReconcileEffectState::Applied,
    });
    let dynamic_safety_calls = Arc::new(AtomicUsize::new(0));
    let authority = Arc::new(TestAuthority {
        leases: LeaseCoordinator::new(pool.clone()),
        owner: Arc::from("spiffe://rocketmq-sre/executor"),
        action: fixture.plan.steps[0].action,
        resource: Arc::from(fixture.plan.steps[0].resource.as_str()),
        dynamic_safety_calls: Arc::clone(&dynamic_safety_calls),
    });
    let verifier = ExecutionVerifier::new(
        Arc::new(ScriptedVerification {
            base: Utc::now(),
            signals: Mutex::new(signals.iter().copied().collect()),
        }),
        Duration::ZERO,
    );
    let executor = ChangeExecutor::new(
        ExecutionJournal::new(pool.clone(), "rocketmq-sre-executor"),
        ResourceSafetyStore::new(pool.clone()),
        authority,
        agent.clone(),
        ExecutionPrechecker::new(registry, agent),
        "spiffe://rocketmq-sre/executor",
        120,
        Duration::from_secs(300),
    )
    .with_verifier(verifier);
    let outcome = executor.execute(&fixture.request).await.expect("supervised execution");
    let active_locks: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM resource_locks
         WHERE holder_execution_id = $1 AND released_at IS NULL",
    )
    .bind(fixture.request.id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("active lock count");
    let compensation_intents: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_steps
         WHERE execution_id = $1 AND record_kind = 'intent' AND compensation",
    )
    .bind(fixture.request.id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("compensation intent count");
    let active_quarantines: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM resource_quarantines
         WHERE source_execution_id = $1 AND cleared_at IS NULL",
    )
    .bind(fixture.request.id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("active quarantine count");
    let dispatches = dispatches.lock().expect("test dispatch lock").clone();
    ExecutionRun {
        pool,
        schema,
        execution_id: fixture.request.id.as_uuid(),
        state: outcome.state,
        dispatches,
        active_locks,
        active_quarantines,
        compensation_intents,
        dynamic_safety_calls: dynamic_safety_calls.load(Ordering::Relaxed),
    }
}

async fn transition_execution(
    journal: &ExecutionJournal,
    fixture: &support::Fixture,
    from: ExecutionState,
    to: ExecutionState,
    reason: &str,
    occurred_at: chrono::DateTime<Utc>,
) {
    assert!(
        journal
            .transition_with_audit(
                fixture.request.id,
                &ExecutionTransition {
                    from,
                    to,
                    reason_code: reason.to_owned(),
                    occurred_at,
                },
                &audit(fixture, AuditEventKind::StateChanged, reason, occurred_at),
            )
            .await
            .expect("execution transition")
    );
}

const fn signal(offset_seconds: i64, healthy: bool) -> Signal {
    Signal {
        offset_seconds,
        resource_ok: healthy,
        sli_ok: healthy,
    }
}

fn lease_contract(record: &rocketmq_sre_executor::ExecutorLeaseRecord) -> ExecutorLease {
    ExecutorLease {
        id: record.id,
        tenant_id: record.tenant_id,
        cluster_id: record.cluster_id,
        epoch: record.epoch,
        owner: record.owner.clone(),
        state: record.state,
        pending_nonce: record.pending_nonce.clone(),
        acquired_at: record.acquired_at,
        activated_at: record.activated_at,
        expires_at: record.expires_at,
    }
}

fn autonomy_grant(fixture: &support::Fixture) -> AutonomyGrant {
    let now = Utc::now();
    AutonomyGrant {
        issuer: "control-plane".to_owned(),
        audience: "rocketmq-sre-executor".to_owned(),
        plan_id: fixture.plan.id,
        plan_hash: fixture.plan.plan_hash.clone(),
        diagnosis_revision_id: fixture.plan.diagnosis_revision,
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        action: fixture.plan.steps[0].action,
        action_version: fixture.plan.steps[0].descriptor_version.clone(),
        policy_id: AutonomyPolicyId::new(),
        policy_definition_version: 1,
        lifecycle_revision: 1,
        autonomous_cohort_id: AutonomyCohortId::new(),
        autonomous_cohort_hash: format!("sha256:{}", "c".repeat(64)),
        critic_review_id: CriticReviewId::new(),
        primary_model_invocation_id: fixture.primary_invocation_id,
        critic_model_invocation_id: fixture.critic_invocation_id,
        issued_at: now - TimeDelta::seconds(1),
        expires_at: now + TimeDelta::seconds(30),
        nonce: "autonomous-execution-test".to_owned(),
        signature: "fixture-signature".to_owned(),
    }
}
