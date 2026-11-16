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

pub(super) async fn terminal(fixture: &Fixture, plan: &ActionPlan, state: &str) -> ExecutionRequest {
    let now = *fixture.clock.lock().unwrap();
    let request = ExecutionRequest {
        schema_version: ExecutionRequest::SCHEMA_VERSION.to_owned(),
        id: ExecutionId::new(),
        tenant_id: plan.tenant_id,
        cluster_id: plan.cluster_id,
        correlation_id: CorrelationId::new(),
        plan: plan.clone(),
        approvals: Vec::new(),
        autonomy_grant: None,
        requested_by: fixture.auth.subject.clone(),
        idempotency_key: format!("d20-original-{}", uuid::Uuid::new_v4()),
        issuer: "SyntheticFixture".to_owned(),
        audience: "rocketmq-sre-executor".to_owned(),
        issued_at: now
            - Duration::seconds(i64::try_from(plan.steps[0].verification.stable_window_seconds).unwrap() + 2),
        expires_at: now + Duration::seconds(60),
        nonce: "private-nonce".to_owned(),
        signature: "private_signature_canary".to_owned(),
    };
    store_terminal(fixture, &request, state).await;
    request
}

pub(super) async fn store_terminal(fixture: &Fixture, request: &ExecutionRequest, state: &str) {
    store_terminal_with_key(fixture, request, state, &request.idempotency_key).await;
}

pub(super) async fn store_terminal_with_key(fixture: &Fixture, request: &ExecutionRequest, state: &str, key: &str) {
    let now = *fixture.clock.lock().unwrap();
    sqlx::query(
        "INSERT INTO executions(id,tenant_id,cluster_id,correlation_id,plan_id,plan_hash,resource_key,action_id,
        idempotency_key,state,request_snapshot,requested_by,started_at,updated_at,completed_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$14)",
    )
    .bind(request.id.as_uuid())
    .bind(request.tenant_id.as_uuid())
    .bind(request.cluster_id.as_uuid())
    .bind(request.correlation_id.as_uuid())
    .bind(request.plan.id.as_uuid())
    .bind(&request.plan.plan_hash)
    .bind(&request.plan.steps[0].resource)
    .bind(request.plan.steps[0].action.id())
    .bind(key)
    .bind(state)
    .bind(json!(request))
    .bind(&request.requested_by)
    .bind(request.issued_at)
    .bind(now)
    .execute(&fixture.repository.pool)
    .await
    .unwrap();
}

pub(super) async fn attempt(fixture: &Fixture, request: &ExecutionRequest, compensation: bool) -> StepIntent {
    attempt_inner(fixture, request, compensation, false).await
}

pub(super) async fn bad_fence_attempt(fixture: &Fixture, request: &ExecutionRequest) -> StepIntent {
    attempt_inner(fixture, request, false, true).await
}

async fn attempt_inner(fixture: &Fixture, request: &ExecutionRequest, compensation: bool, bad: bool) -> StepIntent {
    let now = *fixture.clock.lock().unwrap();
    let step = &request.plan.steps[0];
    let started = now - Duration::seconds(i64::try_from(step.verification.stable_window_seconds).unwrap() + 1);
    let lease = LeaseId::new();
    let epoch: i64 = sqlx::query_scalar("SELECT COALESCE(max(epoch),0)+1 FROM executor_leases WHERE cluster_id=$1")
        .bind(request.cluster_id.as_uuid())
        .fetch_one(&fixture.repository.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO executor_leases(id,tenant_id,cluster_id,epoch,owner,state,pending_nonce,acquired_at,expires_at,updated_at,activated_at,fence_ack_snapshot)
        VALUES($1,$2,$3,$6,'SyntheticFixture','active','private-nonce',$4,$5,$5,$4,'{}'::jsonb)")
        .bind(lease.as_uuid()).bind(request.tenant_id.as_uuid()).bind(request.cluster_id.as_uuid())
        .bind(started).bind(now).bind(epoch).execute(&fixture.repository.pool).await.unwrap();
    let step_id = ExecutionStepId::new();
    let intent = StepIntent {
        execution_id: request.id,
        step_id,
        plan_hash: request.plan.plan_hash.clone(),
        step: step.clone(),
        attempt: 1,
        idempotency_key: format!("{}:step", request.idempotency_key),
        dynamic_safety: None,
        intended_at: started,
        compensation,
        fence_grant: LeaseFenceGrant {
            lease_id: lease,
            owner: "SyntheticFixture".to_owned(),
            cluster_id: request.cluster_id,
            epoch: LeaseEpoch(u64::try_from(epoch).unwrap() + u64::from(bad)),
            execution_id: request.id,
            step_id,
            plan_step_id: step.id,
            action: step.action,
            resource: step.resource.clone(),
            compensation,
            audience: "rocketmq-sre-execution-agent".to_owned(),
            issued_at: started,
            expires_at: now,
            nonce: "private-nonce".to_owned(),
            signature: "private_signature_canary".to_owned(),
        },
    };
    sqlx::query("INSERT INTO execution_steps(execution_id,step_id,attempt,record_kind,lease_id,lease_epoch,compensation,intent_snapshot,reason_code,occurred_at)
        VALUES($1,$2,1,'intent',$3,$7,$4,$5,'fixture',$6)")
        .bind(request.id.as_uuid()).bind(step_id.as_uuid()).bind(lease.as_uuid()).bind(compensation)
        .bind(json!(intent)).bind(started).bind(epoch).execute(&fixture.repository.pool).await.unwrap();
    let result = StepResult {
        step_id,
        state: ExecutionState::Applying,
        agent_result: Some(AgentStepResult {
            execution_id: request.id,
            step_id,
            state: EffectState::Confirmed,
            operation_id: "private-operation".to_owned(),
            outcome_code: "applied".to_owned(),
            sanitized_summary: "private_agent_result".to_owned(),
            completed_at: now,
        }),
        verification: None,
        reason_code: "agent_effect_confirmed".to_owned(),
        completed_at: now,
    };
    // The actual shared append_result writer has always stored FALSE here,
    // including compensation. The immutable intent is the authoritative role.
    sqlx::query("INSERT INTO execution_steps(execution_id,step_id,attempt,record_kind,compensation,result_snapshot,reason_code,occurred_at)
        VALUES($1,$2,1,'result',FALSE,$3,'fixture',$4)").bind(request.id.as_uuid()).bind(step_id.as_uuid())
        .bind(json!(result)).bind(now).execute(&fixture.repository.pool).await.unwrap();
    sqlx::query("UPDATE executor_leases SET state='expired' WHERE id=$1")
        .bind(lease.as_uuid())
        .execute(&fixture.repository.pool)
        .await
        .unwrap();
    intent
}

pub(super) fn verification(
    fixture: &Fixture,
    request: &ExecutionRequest,
    intent: &StepIntent,
    kind: &str,
) -> (VerificationResult, Vec<EvidenceSnapshot>) {
    let step = &intent.step;
    let completed = *fixture.clock.lock().unwrap();
    let started = completed - Duration::seconds(i64::try_from(step.verification.stable_window_seconds).unwrap());
    let mut evidence = Vec::new();
    let mut samples = Vec::new();
    for point in [started, completed] {
        let resource: std::collections::BTreeMap<_, _> = step
            .verification
            .resource_conditions
            .iter()
            .map(|name| (name.clone(), true))
            .collect();
        let technical: std::collections::BTreeMap<_, _> = step
            .verification
            .technical_slis
            .iter()
            .map(|name| (name.clone(), true))
            .collect();
        let mut snapshot = EvidenceSnapshot::capture(
            EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: request.correlation_id,
                tenant_id: request.tenant_id,
                cluster_id: request.cluster_id,
                source: "d20-synthetic-window".to_owned(),
                resource: step.resource.clone(),
                time_range: TimeRange::new(point, point).unwrap(),
            },
            current_evidence_schema(),
            point,
            EvidenceContent::Inline(json!({
                "resource_conditions":resource,"technical_slis":technical,"source_facts":{
                    "provenance":"synthetic_fixture","resource_observed_at":point,"technical_sli_observed_at":point,
                    "technical_scope_bound":true,"target_version_observed":"synthetic-1"}})),
        )
        .unwrap();
        snapshot.exposure = EvidenceExposure::Synthetic;
        samples.push(VerificationWindowSample {
            evidence_id: snapshot.evidence_id,
            content_hash: snapshot.content_hash.clone(),
            resource_observed_at: point,
            technical_sli_observed_at: point,
            collected_at: point,
            source_age_seconds: 0,
            elapsed_nanos: u64::try_from((point - started).num_nanoseconds().unwrap()).unwrap(),
            complete: true,
            resource_conditions_satisfied: true,
            technical_slis_satisfied: true,
            target_version_matched: true,
        });
        evidence.push(snapshot);
    }
    let proof = VerificationWindowProof {
        schema_version: VERIFICATION_WINDOW_PROOF_SCHEMA_VERSION.to_owned(),
        tenant_id: request.tenant_id,
        cluster_id: request.cluster_id,
        execution_id: request.id,
        step_id: intent.step_id,
        plan_step_id: step.id,
        action: step.action,
        descriptor_version: step.descriptor_version.clone(),
        target: step.resource.clone(),
        parameters_digest: canonical_sha256(&step.parameters).unwrap(),
        policy: Some(VerificationWindowPolicy {
            policy_digest: format!("sha256:{}", "1".repeat(64)),
            policy_revision: "SyntheticFixture".to_owned(),
            declared_target_version: "synthetic-1".to_owned(),
            stable_window_seconds: step.verification.stable_window_seconds,
            max_wait_seconds: step.verification.max_wait_seconds,
            max_source_age_seconds: 10,
            max_sample_gap_seconds: 300,
        }),
        provenance: VerificationProofOrigin::SyntheticFixture,
        target_version_observed: Some("synthetic-1".to_owned()),
        started_at: started,
        completed_at: completed,
        observed_stable_seconds: step.verification.stable_window_seconds,
        samples,
        missing_conditions: Vec::new(),
        reason_codes: vec![ExecutionObservationReason::PolicyWindowSatisfied],
        proof_hash: String::new(),
    }
    .seal()
    .unwrap();
    let mut result = VerificationResult {
        step_id: intent.step_id,
        outcome: VerificationOutcome::Succeeded,
        started_at: started,
        completed_at: completed,
        pre_evidence_ids: Vec::new(),
        during_evidence_ids: Vec::new(),
        post_evidence_ids: evidence.iter().map(|item| item.evidence_id).collect(),
        satisfied_conditions: step
            .verification
            .resource_conditions
            .iter()
            .map(|name| format!("resource:{name}"))
            .chain(
                step.verification
                    .technical_slis
                    .iter()
                    .map(|name| format!("sli:{name}")),
            )
            .collect(),
        failed_conditions: Vec::new(),
        stable_window_seconds: step.verification.stable_window_seconds,
        window_proof: Some(proof),
    };
    match kind {
        "legacy" => result.window_proof = None,
        "duplicate_legacy" => {
            result.window_proof = None;
            result.satisfied_conditions = vec![result.satisfied_conditions[0].clone(); 1024];
        }
        "inconsistent_success" => {
            result.failed_conditions = vec![result.satisfied_conditions[0].clone()];
        }
        "invalid" => result.window_proof.as_mut().unwrap().proof_hash = "0".repeat(64),
        "unavailable" => evidence.clear(),
        _ => {}
    }
    (result, evidence)
}

pub(super) async fn store_verification(
    fixture: &Fixture,
    request: &ExecutionRequest,
    intent: &StepIntent,
    result: VerificationResult,
    evidence: Vec<EvidenceSnapshot>,
) {
    for snapshot in evidence {
        sqlx::query("INSERT INTO execution_verification_evidence(execution_id,step_id,attempt,phase,evidence_id,evidence_snapshot,observed_at)
            VALUES($1,$2,1,'post',$3,$4,$5)").bind(request.id.as_uuid()).bind(intent.step_id.as_uuid()).bind(snapshot.evidence_id.as_uuid())
            .bind(json!(snapshot)).bind(snapshot.observed_at).execute(&fixture.repository.pool).await.unwrap();
    }
    sqlx::query("INSERT INTO execution_verifications(execution_id,step_id,attempt,compensation,outcome,result_snapshot,started_at,completed_at)
        VALUES($1,$2,1,$3,'succeeded',$4,$5,$6)").bind(request.id.as_uuid()).bind(intent.step_id.as_uuid()).bind(intent.compensation)
        .bind(json!(result)).bind(result.started_at).bind(result.completed_at).execute(&fixture.repository.pool).await.unwrap();
}

pub(super) async fn journal_counts(fixture: &Fixture) -> (i64, i64, i64, i64, i64) {
    // Other fixtures share the database but own distinct tenant/cluster scopes.
    sqlx::query_as(
        "WITH owned_executions AS (SELECT id FROM executions WHERE tenant_id=$1 AND cluster_id=$2)
        SELECT (SELECT count(*) FROM owned_executions),
            (SELECT count(*) FROM execution_steps WHERE execution_id IN (SELECT id FROM owned_executions)),
            (SELECT count(*) FROM execution_verifications WHERE execution_id IN (SELECT id FROM owned_executions)),
            (SELECT count(*) FROM executor_leases WHERE tenant_id=$1 AND cluster_id=$2),
            (SELECT count(*) FROM audit_events WHERE tenant_id=$1 AND cluster_id=$2)",
    )
    .bind(fixture.auth.tenant_id.as_uuid())
    .bind(fixture.request.cluster_id.as_uuid())
    .fetch_one(&fixture.repository.pool)
    .await
    .unwrap()
}
