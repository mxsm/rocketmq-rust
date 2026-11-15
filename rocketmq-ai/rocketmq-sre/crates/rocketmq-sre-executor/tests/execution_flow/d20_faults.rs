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

use super::*;
use rocketmq_runtime::{RuntimeContext, TaskGroup};
use serde_json::Value;
use tokio::sync::Notify;

#[derive(Default)]
struct Gate {
    entered: Notify,
    release: Notify,
}
struct LostResponseAgent {
    inner: Arc<TestAgent>,
    gate: Arc<Gate>,
}
impl ExecutionAgentClient for LostResponseAgent {
    fn capabilities<'a>(&'a self) -> TestFuture<'a, ExecutionAgentCapabilities> {
        self.inner.capabilities()
    }
    fn precheck<'a>(&'a self, request: &'a AgentReadRequest) -> TestFuture<'a, AgentReadResult> {
        self.inner.precheck(request)
    }
    fn dispatch<'a>(&'a self, request: &'a AgentDispatchRequest) -> TestFuture<'a, AgentDispatchResponse> {
        Box::pin(async move {
            let response = self.inner.dispatch(request).await?;
            self.gate.entered.notify_one();
            self.gate.release.notified().await;
            Ok(response)
        })
    }
    fn reconcile<'a>(&'a self, request: &'a ReconcileEffectRequest) -> TestFuture<'a, ReconcileEffectResponse> {
        self.inner.reconcile(request)
    }
    fn advance_fence<'a>(&'a self, request: &'a AdvanceFenceRequest) -> TestFuture<'a, AdvanceFenceResponse> {
        self.inner.advance_fence(request)
    }
}

async fn serve(harness: &ExecutionHarness, owner: &TaskGroup) -> (String, TaskGroup) {
    let context = RuntimeContext::from_current("d20-http-regression");
    let server_owner = context.service_context("http").task_group().clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let router = rocketmq_sre_executor::build_router(
        harness
            .executor
            .clone()
            .with_lifecycle(owner.clone(), Duration::from_secs(2), Duration::from_millis(100)),
        "test-cp-token",
        false,
    );
    let token = server_owner.cancellation_token();
    server_owner
        .spawn_service("loopback-http", async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(token.cancelled_owned())
                .await
                .unwrap();
        })
        .unwrap();
    (endpoint, server_owner)
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL; real loopback HTTP loss and Synthetic Agent effect store"]
async fn d20_lost_response_keeps_original_intent_identity_and_durable_unknown_without_another_write() {
    loss_case(false).await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL; forced typed journal IO failure after lost HTTP response"]
async fn d20_lost_waiter_does_not_hide_shutdown_journal_settlement_failure() {
    loss_case(true).await;
}

async fn loss_case(storage_failure: bool) {
    let gate = Arc::new(Gate::default());
    let wrapped_gate = gate.clone();
    let mut harness =
        build_execution_for_action(&[signal(0, true)], false, ExecutionAction::ProxyScaleOutOne, |inner| {
            Arc::new(LostResponseAgent {
                inner,
                gate: wrapped_gate,
            })
        })
        .await;
    let context = RuntimeContext::from_current("d20-owned-accepted");
    let owner = context.service_context("accepted").task_group().clone();
    harness.executor =
        harness
            .executor
            .with_lifecycle(owner.clone(), Duration::from_secs(2), Duration::from_millis(100));
    if storage_failure {
        sqlx::query("CREATE FUNCTION synthetic_settlement_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
            IF NEW.record_kind='result' THEN RAISE EXCEPTION 'synthetic journal unavailable' USING ERRCODE='58000'; END IF; RETURN NEW; END $$")
            .execute(&harness.pool).await.unwrap();
        sqlx::query("CREATE TRIGGER synthetic_settlement_failure BEFORE INSERT ON execution_steps FOR EACH ROW EXECUTE FUNCTION synthetic_settlement_failure()")
            .execute(&harness.pool).await.unwrap();
    }
    let (endpoint, server_owner) = serve(&harness, &owner).await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let mut request = Box::pin(
        client
            .post(format!("{endpoint}/internal/v1/executor/executions"))
            .bearer_auth("test-cp-token")
            .json(&harness.fixture.request)
            .send(),
    );
    tokio::select! {
        ()=gate.entered.notified()=>{},
        result=&mut request=>panic!("request unexpectedly returned before the deterministic response gate: {result:?}"),
    }
    drop(request); // An accepted tracked operation outlives this real response waiter.
    owner.cancel();
    if storage_failure {
        gate.release.notify_one();
    }
    let report = owner.shutdown(Duration::from_secs(2)).await;
    assert!(
        report.is_healthy(),
        "task destruction must be confirmed independently of business settlement"
    );
    assert_eq!(harness.executor.metrics().active_executions, 0);
    let intents: i64 =
        sqlx::query_scalar("SELECT count(*) FROM execution_steps WHERE execution_id=$1 AND record_kind='intent'")
            .bind(harness.fixture.request.id.as_uuid())
            .fetch_one(&harness.pool)
            .await
            .unwrap();
    let results: i64 =
        sqlx::query_scalar("SELECT count(*) FROM execution_steps WHERE execution_id=$1 AND record_kind='result'")
            .bind(harness.fixture.request.id.as_uuid())
            .fetch_one(&harness.pool)
            .await
            .unwrap();
    let state: String = sqlx::query_scalar("SELECT state FROM executions WHERE id=$1")
        .bind(harness.fixture.request.id.as_uuid())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
    assert_eq!(intents, 1);
    assert_eq!(harness.dispatches.lock().unwrap().as_slice(), [false]);
    assert!(
        !harness.executor.ready().await,
        "a canceled operation owner cannot accept new work"
    );
    if storage_failure {
        assert_eq!(results, 0);
        assert_eq!(state, "applying");
        assert_eq!(harness.executor.metrics().shutdown_storage_failures_total, 1);
    } else {
        // A missing response leaves the original intent pending. A fabricated
        // result would remove it from the existing read-only recovery query.
        assert_eq!(results, 0);
        assert_eq!(state, "unknown");
        let original: rocketmq_sre_contracts::ExecutionRequest = serde_json::from_value(
            sqlx::query_scalar("SELECT request_snapshot FROM executions WHERE id=$1")
                .bind(harness.fixture.request.id.as_uuid())
                .fetch_one(&harness.pool)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(original, harness.fixture.request);
        assert_eq!(
            harness.executor.metrics().shutdown_storage_failures_total,
            0,
            "ordinary Unknown is not a storage settlement error"
        );
    }
    let compensation: i64 =
        sqlx::query_scalar("SELECT count(*) FROM execution_steps WHERE execution_id=$1 AND compensation")
            .bind(harness.fixture.request.id.as_uuid())
            .fetch_one(&harness.pool)
            .await
            .unwrap();
    assert_eq!(compensation, 0);
    assert!(server_owner.shutdown(Duration::from_secs(2)).await.is_healthy());
    cleanup_schema(&harness.pool, &harness.schema).await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL; Synthetic 2-step Plan, no second effect or compensation"]
async fn d20_second_pre_unknown_after_confirmed_effect_records_manual_handoff_without_next_write() {
    let mut unavailable = signal(2, true);
    unavailable.source_complete = false;
    let mut harness = build_execution_for_action(
        &[signal(0, true), signal(1, true), unavailable],
        false,
        ExecutionAction::ProxyScaleOutOne,
        |agent| agent,
    )
    .await;
    let original_id = harness.fixture.plan.id;
    let mut plan = harness.fixture.plan.clone();
    plan.id = rocketmq_sre_contracts::ActionPlanId::new();
    plan.version += 1;
    let mut second = plan.steps[0].clone();
    second.id = rocketmq_sre_contracts::PlanStepId::new();
    second.sequence = 2;
    plan.steps.push(second);
    plan.plan_hash = plan.compute_plan_hash().unwrap();
    sqlx::query("INSERT INTO action_plans(id,tenant_id,cluster_id,incident_id,diagnosis_revision_id,primary_model_invocation_id,version,
        plan_hash,evidence_hash,risk,status,request_snapshot,created_by,created_at,expires_at,submitted_at)
        SELECT $1,tenant_id,cluster_id,incident_id,diagnosis_revision_id,primary_model_invocation_id,version+1,$2,evidence_hash,risk,
        status,$3,created_by,created_at,expires_at,submitted_at FROM action_plans WHERE id=$4")
        .bind(plan.id.as_uuid()).bind(&plan.plan_hash).bind(json!(plan)).bind(original_id.as_uuid()).execute(&harness.pool).await.unwrap();
    for approval in &mut harness.fixture.request.approvals {
        approval.plan_id = plan.id;
        approval.plan_hash = plan.plan_hash.clone();
        approval.precondition_hash = plan.compute_precondition_hash().unwrap();
    }
    harness.fixture.request.plan = plan;
    let outcome = harness.executor.execute(&harness.fixture.request).await.unwrap();
    let rocketmq_sre_executor::ExecutorOperationOutcome::Accepted(outcome) = outcome else {
        panic!("expected durable manual handoff")
    };
    assert_eq!(outcome.state, ExecutionState::Escalated);
    assert_eq!(harness.dispatches.lock().unwrap().as_slice(), [false]);
    let (intents, compensation, quarantine): (i64, i64, i64) = sqlx::query_as(
        "SELECT
        (SELECT count(*) FROM execution_steps WHERE execution_id=$1 AND record_kind='intent'),
        (SELECT count(*) FROM execution_steps WHERE execution_id=$1 AND compensation),
        (SELECT count(*) FROM resource_quarantines WHERE source_execution_id=$1 AND cleared_at IS NULL)",
    )
    .bind(harness.fixture.request.id.as_uuid())
    .fetch_one(&harness.pool)
    .await
    .unwrap();
    assert_eq!((intents, compensation, quarantine), (1, 0, 1));
    cleanup_schema(&harness.pool, &harness.schema).await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL; missing first source must not create a target intent"]
async fn d20_first_pre_unknown_stops_before_any_target_write_or_compensation() {
    let mut unavailable = signal(0, true);
    unavailable.source_complete = false;
    let harness =
        build_execution_for_action(&[unavailable], false, ExecutionAction::ProxyScaleOutOne, |agent| agent).await;
    let outcome = harness.executor.execute(&harness.fixture.request).await.unwrap();
    assert!(matches!(
        outcome,
        rocketmq_sre_executor::ExecutorOperationOutcome::Rejected(_)
    ));
    assert!(harness.dispatches.lock().unwrap().is_empty());
    let intents: i64 =
        sqlx::query_scalar("SELECT count(*) FROM execution_steps WHERE execution_id=$1 AND record_kind='intent'")
            .bind(harness.fixture.request.id.as_uuid())
            .fetch_one(&harness.pool)
            .await
            .unwrap();
    assert_eq!(intents, 0);
    cleanup_schema(&harness.pool, &harness.schema).await;
}

struct StartupReadAgent {
    inner: Arc<TestAgent>,
    entered: Arc<Notify>,
    fences: Arc<AtomicUsize>,
}
impl ExecutionAgentClient for StartupReadAgent {
    fn capabilities<'a>(&'a self) -> TestFuture<'a, ExecutionAgentCapabilities> {
        self.inner.capabilities()
    }
    fn precheck<'a>(&'a self, request: &'a AgentReadRequest) -> TestFuture<'a, AgentReadResult> {
        self.inner.precheck(request)
    }
    fn dispatch<'a>(&'a self, request: &'a AgentDispatchRequest) -> TestFuture<'a, AgentDispatchResponse> {
        self.inner.dispatch(request)
    }
    fn reconcile<'a>(&'a self, _request: &'a ReconcileEffectRequest) -> TestFuture<'a, ReconcileEffectResponse> {
        Box::pin(async move {
            self.entered.notify_one();
            std::future::pending().await
        })
    }
    fn advance_fence<'a>(&'a self, request: &'a AdvanceFenceRequest) -> TestFuture<'a, AdvanceFenceResponse> {
        self.fences.fetch_add(1, Ordering::SeqCst);
        self.inner.advance_fence(request)
    }
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL; isolated historical compensating intent, no live Executor startup"]
async fn d20_startup_recovery_cancel_keeps_original_history_and_stops_before_fresh_fence_or_write() {
    let entered = Arc::new(Notify::new());
    let fences = Arc::new(AtomicUsize::new(0));
    let notify = entered.clone();
    let fence_count = fences.clone();
    let mut harness = build_execution_for_action(&[], false, ExecutionAction::ProxyScaleOutOne, |inner| {
        Arc::new(StartupReadAgent {
            inner,
            entered: notify,
            fences: fence_count,
        })
    })
    .await;
    let journal = ExecutionJournal::new(harness.pool.clone(), "rocketmq-sre-executor");
    let now = Utc::now();
    journal
        .create_execution(
            &harness.fixture.request,
            &harness.fixture.plan.steps[0].resource,
            harness.fixture.plan.steps[0].action,
            now,
        )
        .await
        .unwrap();
    let lease = rocketmq_sre_contracts::LeaseId::new();
    sqlx::query("INSERT INTO executor_leases(id,tenant_id,cluster_id,epoch,owner,state,pending_nonce,acquired_at,activated_at,expires_at,updated_at,fence_ack_snapshot)
        VALUES($1,$2,$3,1,'SyntheticFixture','active','synthetic',$4,$4,$5,$4,'{}'::jsonb)")
        .bind(lease.as_uuid()).bind(harness.fixture.tenant_id.as_uuid()).bind(harness.fixture.cluster_id.as_uuid())
        .bind(now).bind(now+TimeDelta::minutes(2)).execute(&harness.pool).await.unwrap();
    let intent = step_intent(
        &harness.fixture,
        lease,
        rocketmq_sre_contracts::LeaseEpoch(1),
        "SyntheticFixture",
        now + TimeDelta::minutes(2),
        "startup-original",
        now,
    );
    journal
        .append_intent_with_audit(
            &intent,
            &audit(
                &harness.fixture,
                AuditEventKind::StepIntentPersisted,
                "SyntheticFixture",
                now,
            ),
        )
        .await
        .unwrap();
    journal
        .append_result_with_audit(
            harness.fixture.request.id,
            1,
            &StepResult {
                step_id: intent.step_id,
                state: ExecutionState::Compensating,
                agent_result: None,
                verification: None,
                reason_code: "synthetic_interrupted_history".to_owned(),
                completed_at: now,
            },
            &audit(
                &harness.fixture,
                AuditEventKind::StepResultPersisted,
                "SyntheticFixture",
                now,
            ),
        )
        .await
        .unwrap();
    for (from, to) in [
        (ExecutionState::Pending, ExecutionState::Prechecking),
        (ExecutionState::Prechecking, ExecutionState::IntentPersisted),
        (ExecutionState::IntentPersisted, ExecutionState::Applying),
        (ExecutionState::Applying, ExecutionState::Verifying),
        (ExecutionState::Verifying, ExecutionState::Compensating),
    ] {
        transition_execution(&journal, &harness.fixture, from, to, "SyntheticFixture", now).await;
    }
    sqlx::query("UPDATE executor_leases SET state='expired' WHERE id=$1")
        .bind(lease.as_uuid())
        .execute(&harness.pool)
        .await
        .unwrap();
    let before:Vec<Value>=sqlx::query_scalar("SELECT COALESCE(intent_snapshot,result_snapshot) FROM execution_steps WHERE execution_id=$1 ORDER BY sequence_id")
        .bind(harness.fixture.request.id.as_uuid()).fetch_all(&harness.pool).await.unwrap();
    let runtime = RuntimeContext::from_current("d20-startup-recovery");
    let owner = runtime.service_context("http").task_group().clone();
    harness.executor = harness
        .executor
        .with_lifecycle(owner.clone(), Duration::from_secs(2), Duration::from_secs(1));
    let executor = harness.executor.clone();
    let (finished, done) = tokio::sync::oneshot::channel();
    owner
        .spawn_draining_operation(
            &rocketmq_runtime::OperationContext::without_deadline(rocketmq_runtime::TaskKind::Worker),
            "startup-reconciliation",
            async move {
                let _ = finished.send(executor.recover_interrupted_executions(100).await);
            },
        )
        .unwrap();
    entered.notified().await;
    owner.cancel();
    assert!(owner.shutdown(Duration::from_secs(2)).await.is_healthy());
    let _ = done.await.unwrap();
    assert_eq!(fences.load(Ordering::SeqCst), 0);
    assert!(harness.dispatches.lock().unwrap().is_empty());
    let after:Vec<Value>=sqlx::query_scalar("SELECT COALESCE(intent_snapshot,result_snapshot) FROM execution_steps WHERE execution_id=$1 ORDER BY sequence_id")
        .bind(harness.fixture.request.id.as_uuid()).fetch_all(&harness.pool).await.unwrap();
    assert_eq!(before, after);
    assert_eq!(
        journal.execution_state(harness.fixture.request.id).await.unwrap(),
        ExecutionState::Compensating
    );
    assert_eq!(harness.executor.metrics().active_executions, 0);
    cleanup_schema(&harness.pool, &harness.schema).await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL; synchronized readonly startup SELECT lock"]
async fn d20_startup_scan_cancel_does_not_wait_for_a_locked_readonly_relation() {
    let mut harness = build_execution_for_action(&[], false, ExecutionAction::ProxyScaleOutOne, |agent| agent).await;
    let mut lock = harness.pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE executions IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let runtime = RuntimeContext::from_current("d20-startup-scan");
    let owner = runtime.service_context("http").task_group().clone();
    harness.executor = harness
        .executor
        .with_lifecycle(owner.clone(), Duration::from_secs(2), Duration::from_secs(1));
    let executor = harness.executor.clone();
    let (finished, done) = tokio::sync::oneshot::channel();
    owner
        .spawn_draining_operation(
            &rocketmq_runtime::OperationContext::without_deadline(rocketmq_runtime::TaskKind::Worker),
            "startup-scan",
            async move {
                let _ = finished.send(executor.recover_interrupted_executions(100).await);
            },
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='executions'::regclass AND NOT granted)",
            )
            .fetch_one(&harness.pool)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    owner.cancel();
    let report = tokio::time::timeout(Duration::from_secs(1), owner.shutdown(Duration::from_secs(2)))
        .await
        .unwrap();
    assert!(report.is_healthy());
    assert!(done.await.unwrap().is_err());
    // Unlock only after the owned operation has completed; canceling a local
    // readonly wait makes no claim about server-side statement cancellation.
    lock.rollback().await.unwrap();
    assert!(harness.dispatches.lock().unwrap().is_empty());
    cleanup_schema(&harness.pool, &harness.schema).await;
}
