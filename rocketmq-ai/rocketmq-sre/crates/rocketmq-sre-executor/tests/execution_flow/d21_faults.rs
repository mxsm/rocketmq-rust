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

//! Protocol controls use an injected test authority and Synthetic target effects.
//! They do not satisfy normal first-intent or physical target qualification.

use rocketmq_runtime::RuntimeContext;
use rocketmq_sre_execution_agent::AgentEffectStoreOperations;
use rocketmq_sre_executor::ExecutorOperationOutcome;
use tokio::sync::Notify;

use super::*;

struct HeldReply {
    inner: Arc<TestAgent>,
    applied: Arc<Notify>,
    release: Arc<Notify>,
}

impl ExecutionAgentClient for HeldReply {
    fn capabilities<'a>(&'a self) -> TestFuture<'a, ExecutionAgentCapabilities> {
        self.inner.capabilities()
    }

    fn precheck<'a>(&'a self, request: &'a AgentReadRequest) -> TestFuture<'a, AgentReadResult> {
        self.inner.precheck(request)
    }

    fn dispatch<'a>(&'a self, request: &'a AgentDispatchRequest) -> TestFuture<'a, AgentDispatchResponse> {
        Box::pin(async move {
            let creation = self
                .inner
                .effects
                .prepare(request.tenant_id, &request.request, Utc::now())
                .await
                .map_err(|_| ExecutorError::unavailable())?;
            assert!(
                creation.created,
                "the concurrent duplicate must not re-enter the Synthetic effect driver"
            );
            let operation_id = format!("test-operation-{}", request.request.intent.step_id);
            self.inner
                .effects
                .mark_dispatched(&request.request.intent.idempotency_key, &operation_id, Utc::now())
                .await
                .map_err(|_| ExecutorError::unavailable())?;
            let response = self.inner.dispatch(request).await?;
            self.inner
                .effects
                .confirm(
                    &request.request.intent.idempotency_key,
                    &response.result.outcome_code,
                    &response.result.sanitized_summary,
                    Utc::now(),
                )
                .await
                .map_err(|_| ExecutorError::unavailable())?;
            self.applied.notify_one();
            self.release.notified().await;
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

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; concurrent real HTTP, injected authority and Synthetic Agent effect"]
async fn d21_concurrent_original_http_requests_keep_one_accepted_identity_and_one_effect() {
    let started_at = Utc::now();
    let applied = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let harness = build_execution_for_action(
        &r1_success_signals(ExecutionAction::ProxyScaleOutOne),
        false,
        ExecutionAction::ProxyScaleOutOne,
        |inner| {
            Arc::new(HeldReply {
                inner,
                applied: applied.clone(),
                release: release.clone(),
            })
        },
    )
    .await;
    let runtime = RuntimeContext::from_current("d21-concurrent-original-intent");
    let owner = runtime.service_context("accepted").task_group().clone();
    let (endpoint, server_owner) = super::d20_faults::serve(&harness, &owner).await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let send = || {
        client
            .post(format!("{endpoint}/internal/v1/executor/executions"))
            .bearer_auth("test-cp-token")
            .json(&harness.fixture.request)
            .send()
    };
    let first = send();
    tokio::pin!(first);
    let entered = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            ()=applied.notified()=>true,
            _=&mut first=>false,
        }
    })
    .await;
    if !matches!(entered, Ok(true)) {
        release.notify_one();
        owner.cancel();
        let _ = server_owner.shutdown(Duration::from_secs(2)).await;
        let _ = owner.shutdown(Duration::from_secs(2)).await;
        panic!("first original request did not reach the bounded controlled effect gate");
    }
    let epoch_before = cluster_lease_and_fence(&harness).await;
    let mut changed = harness.fixture.request.clone();
    changed.correlation_id = rocketmq_sre_contracts::CorrelationId::new();
    let changed = client
        .post(format!("{endpoint}/internal/v1/executor/executions"))
        .bearer_auth("test-cp-token")
        .json(&changed)
        .send()
        .await
        .unwrap();
    assert_eq!(changed.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(
        changed.json::<serde_json::Value>().await.unwrap()["code"],
        "precondition_changed"
    );
    assert_eq!(cluster_lease_and_fence(&harness).await, epoch_before);
    let neighbor = seed_interrupted_neighbor(&harness).await;
    let recovery = harness.executor.recover_execution(neighbor).await.unwrap();
    assert_eq!(
        recovery,
        ExecutorOperationOutcome::rejected(rocketmq_sre_executor::ExecutorRejection::ReconcileBlocked)
    );
    assert_eq!(
        cluster_lease_and_fence(&harness).await,
        epoch_before,
        "recovery of another original identity must not steal a live local writer's epoch"
    );
    // The second original request is concurrent with an accepted first effect,
    // not a new key and not a shortcut around the normal CP qualification gate.
    let second = send().await.unwrap();
    let second_status = second.status();
    let second_body: serde_json::Value = second.json().await.unwrap();
    release.notify_one();
    let first = first.await.unwrap();
    let first_status = first.status();
    let first_body: serde_json::Value = first.json().await.unwrap();
    let epoch_after = cluster_lease_and_fence(&harness).await;
    let terminal_replay = send().await.unwrap();
    assert!(terminal_replay.status().is_success());
    let terminal_replay: serde_json::Value = terminal_replay.json().await.unwrap();
    assert_eq!(
        terminal_replay["execution_id"],
        serde_json::json!(harness.fixture.request.id)
    );
    assert_eq!(terminal_replay["replayed"], true);
    assert_eq!(
        cluster_lease_and_fence(&harness).await,
        epoch_after,
        "terminal receipt replay must leave lease and Agent fence rows unchanged"
    );
    let requests: i64 = sqlx::query_scalar("SELECT count(*) FROM executions WHERE idempotency_key=$1")
        .bind(&harness.fixture.request.idempotency_key)
        .fetch_one(&harness.pool)
        .await
        .unwrap();
    let original: rocketmq_sre_contracts::ExecutionRequest = serde_json::from_value(
        sqlx::query_scalar("SELECT request_snapshot FROM executions WHERE id=$1")
            .bind(harness.fixture.request.id.as_uuid())
            .fetch_one(&harness.pool)
            .await
            .unwrap(),
    )
    .unwrap();
    let effects: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_agent_effects WHERE execution_id=$1")
        .bind(harness.fixture.request.id.as_uuid())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
    let compensation: i64 =
        sqlx::query_scalar("SELECT count(*) FROM execution_steps WHERE execution_id=$1 AND compensation")
            .bind(harness.fixture.request.id.as_uuid())
            .fetch_one(&harness.pool)
            .await
            .unwrap();
    let server_report = server_owner.shutdown(Duration::from_secs(2)).await;
    let owner_report = owner.shutdown(Duration::from_secs(2)).await;
    eprintln!(
        "D21_DUPLICATE_HTTP first_status={first_status} first_code={} second_status={second_status} second_code={}",
        first_body["code"].as_str().unwrap_or("accepted"),
        second_body["code"].as_str().unwrap_or("accepted")
    );
    assert!(
        first_status.is_success(),
        "the accepted first request failed: {first_status}/{}",
        first_body["code"]
    );
    assert_eq!(first_body["state"], "succeeded");
    assert_eq!(
        first_body["execution_id"],
        serde_json::json!(harness.fixture.request.id)
    );
    if second_status.is_success() {
        assert_eq!(
            second_body["execution_id"],
            serde_json::json!(harness.fixture.request.id)
        );
    } else {
        assert_eq!(second_status, reqwest::StatusCode::CONFLICT);
        assert_eq!(second_body["code"], "unresolved_old_effects");
    }
    assert_eq!(requests, 1);
    assert_eq!(original, harness.fixture.request);
    assert_eq!(effects, 1);
    assert_eq!(harness.dispatches.lock().unwrap().as_slice(), [false]);
    assert_eq!(compensation, 0);
    assert!(server_report.is_healthy());
    assert!(owner_report.is_healthy());
    cleanup_schema(&harness.pool, &harness.schema).await;
    record_protocol_fragment(
        &harness.fixture.request,
        started_at,
        epoch_after.1["highest_epoch"].as_i64().unwrap(),
    );
}

fn record_protocol_fragment(
    request: &rocketmq_sre_contracts::ExecutionRequest,
    started_at: chrono::DateTime<Utc>,
    epoch: i64,
) {
    let Some(path) = std::env::var_os("ROCKETMQ_SRE_D21_PROTOCOL_REPORT") else {
        return;
    };
    let path = std::path::Path::new(&path);
    assert!(path.is_absolute(), "drill reports belong outside the repository");
    assert!(epoch > 0);
    let step = &request.plan.steps[0];
    let record = serde_json::json!({
    "schema_version":"rocketmq-sre.execution-drill-fragment.v1", "run_id":"d21-current-drills",
    "production_qualified":false, "runtime_admission_authority":false,
    "cases":[{
        "action":step.action,"descriptor_version":step.descriptor_version,
        "tenant_id":request.tenant_id,"cluster_id":request.cluster_id,
        "target_id":"synthetic-proxy-scale-target",
        "target_artifact_identity":{"kind":"deterministic_fixture","opaque_id":"synthetic-agent-pg-v1"},
        "observed_software_version":null,"evidence_class":"protocol_fault",
        "execution_path":"injected_executor_agent","scenario":"accepted_duplicate",
        "started_at":started_at,"finished_at":Utc::now(),"outcome":"passed","observed_state":"recorded_effect",
        "guard":null,"guard_reached":null,"original_identity_preserved":true,"cleanup_confirmed":true,
        "accepted_identity":{"execution_id":request.id,"correlation_id":request.correlation_id,
            "plan_id":request.plan.id,"plan_hash":request.plan.plan_hash,"plan_step_id":step.id,
            "original_selector_digest":rocketmq_sre_contracts::canonical_sha256(&serde_json::json!({
                "plan_id":request.plan.id,"plan_hash":request.plan.plan_hash,
                "precondition_hash":step.precondition_hash,"idempotency_key":request.idempotency_key})).unwrap(),
            "lease_epoch":epoch.to_string()},
        "counters":{"accepted_identities":1,"forward_effects":1,"compensation_effects":0,
            "stale_effects":0,"new_idempotency_keys":0,"quarantines":0},
        "artifact_references":["d21-concurrent-original-recorded.log"],
        "limitations":["software_version_unknown","physical_attestation_unavailable",
            "independent_business_observation_unavailable","exact_sli_unavailable","injected_authority"]
    }]});
    let encoded = serde_json::to_vec_pretty(&record).unwrap();
    assert!(encoded.len() <= 64 * 1024);
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    std::io::Write::write_all(&mut output, &encoded).unwrap();
}

async fn cluster_lease_and_fence(harness: &ExecutionHarness) -> (serde_json::Value, serde_json::Value) {
    let leases =
        sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(l) ORDER BY epoch) FROM executor_leases l WHERE cluster_id=$1")
            .bind(harness.fixture.cluster_id.as_uuid())
            .fetch_one(&harness.pool)
            .await
            .unwrap();
    let fence = sqlx::query_scalar("SELECT to_jsonb(f) FROM execution_agent_fences f WHERE cluster_id=$1")
        .bind(harness.fixture.cluster_id.as_uuid())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
    (leases, fence)
}

async fn seed_interrupted_neighbor(harness: &ExecutionHarness) -> rocketmq_sre_contracts::ExecutionId {
    // A distinct, previously interrupted intent in the same disposable cluster.
    // It has no target effect and no active local worker of its own.
    let mut request = harness.fixture.request.clone();
    request.id = rocketmq_sre_contracts::ExecutionId::new();
    request.correlation_id = rocketmq_sre_contracts::CorrelationId::new();
    request.idempotency_key = format!("d21-interrupted-neighbor-{}", request.id);
    let fixture = support::Fixture {
        tenant_id: harness.fixture.tenant_id,
        cluster_id: harness.fixture.cluster_id,
        primary_invocation_id: harness.fixture.primary_invocation_id,
        critic_invocation_id: harness.fixture.critic_invocation_id,
        plan: request.plan.clone(),
        request,
    };
    let journal = ExecutionJournal::new(harness.pool.clone(), "rocketmq-sre-executor");
    let now = Utc::now();
    journal
        .create_execution(
            &fixture.request,
            &fixture.plan.steps[0].resource,
            fixture.plan.steps[0].action,
            now,
        )
        .await
        .unwrap();
    transition_execution(
        &journal,
        &fixture,
        ExecutionState::Pending,
        ExecutionState::Prechecking,
        "d21_interrupted_neighbor",
        now,
    )
    .await;
    let snapshot: serde_json::Value = sqlx::query_scalar("SELECT intent_snapshot FROM execution_steps WHERE execution_id=$1 AND record_kind='intent' AND NOT compensation LIMIT 1")
        .bind(harness.fixture.request.id.as_uuid()).fetch_one(&harness.pool).await.unwrap();
    let mut intent: rocketmq_sre_contracts::StepIntent = serde_json::from_value(snapshot).unwrap();
    intent.execution_id = fixture.request.id;
    intent.step_id = rocketmq_sre_contracts::ExecutionStepId::new();
    intent.idempotency_key = format!("d21-neighbor-effect-{}", intent.step_id);
    intent.fence_grant.execution_id = intent.execution_id;
    intent.fence_grant.step_id = intent.step_id;
    journal
        .append_intent_with_audit(
            &intent,
            &audit(
                &fixture,
                AuditEventKind::StepIntentPersisted,
                "d21_interrupted_neighbor",
                now,
            ),
        )
        .await
        .unwrap();
    for (from, to) in [
        (ExecutionState::Prechecking, ExecutionState::IntentPersisted),
        (ExecutionState::IntentPersisted, ExecutionState::Applying),
        (ExecutionState::Applying, ExecutionState::Verifying),
        (ExecutionState::Verifying, ExecutionState::Compensating),
    ] {
        transition_execution(&journal, &fixture, from, to, "d21_interrupted_neighbor", now).await;
    }
    fixture.request.id
}
