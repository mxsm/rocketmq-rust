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

use super::support::*;
use axum::http::StatusCode;
use chrono::Duration;
use rocketmq_sre_contracts::*;
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::atomic::Ordering};

mod seed;
use seed::*;

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL; terminal SyntheticFixture only"]
async fn postgres_observation_exact_readonly_lookup_and_receipts_do_not_dispatch_or_infer_results() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    fixture.prepare().await;
    let plan = fixture.draft().await;
    *fixture.clock.lock().unwrap() += Duration::hours(1);
    let request = terminal(&fixture, &plan, "escalated").await;
    let path = format!("/v1/executions/{}/observation", request.id);
    let selector = json!({"plan_id":plan.id,"plan_hash":plan.plan_hash,
        "precondition_hash":plan.compute_precondition_hash().unwrap(),"idempotency_key":request.idempotency_key});
    let counts = journal_counts(&fixture).await;
    for _ in 0..2 {
        let (status, body) = fixture.get(&path, &fixture.auth).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let view: ExecutionObservationView = serde_json::from_value(body.clone()).unwrap();
        assert_eq!(view.binding.execution_id, request.id);
        assert_eq!(view.history.state, ExecutionObservationState::NotObserved);
        assert_eq!(view.history.reason_codes, [ExecutionObservationReason::NoEffectResult]);
        assert_eq!(view.business_recovery.state, ExecutionObservationState::Unknown);
        assert!(view.steps[0].attempts.is_empty());
        assert!(view.timeline.is_empty());
        assert_safe(&body);
    }
    let (status, found) = fixture
        .call("/v1/executions/lookup", &fixture.auth, selector.clone())
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(found["status"], "accepted");
    assert_eq!(found["receipt"]["execution_id"], request.id.to_string());
    let mut other = fixture.auth.clone();
    other.subject = "different-current-reader".to_owned();
    other.roles = BTreeSet::from(["viewer".to_owned()]);
    let (status, other_found) = fixture.call("/v1/executions/lookup", &other, selector.clone()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(other_found["status"], "not_observed");
    assert!(other_found["receipt"].is_null());
    let (status, page) = fixture
        .get(
            &format!("/v1/plans/{}/executions?plan_hash={}", plan.id, plan.plan_hash),
            &other,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    let mut missing = selector.clone();
    missing["idempotency_key"] = json!("original-selector-not-observed");
    let (status, body) = fixture.call("/v1/executions/lookup", &fixture.auth, missing).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "not_observed");
    let mut mismatch = selector;
    mismatch["precondition_hash"] = json!(format!("sha256:{}", "0".repeat(64)));
    let (status, body) = fixture.call("/v1/executions/lookup", &fixture.auth, mismatch).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "execution_intent_mismatch");
    *fixture.clock.lock().unwrap() += Duration::days(2);
    assert_eq!(
        fixture.get(&path, &fixture.auth).await.0,
        StatusCode::OK,
        "history is not current admission"
    );
    assert_eq!(counts, journal_counts(&fixture).await);
    assert_eq!(fixture.observer.reads.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.observer.writes.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.observer.reconciliations.load(Ordering::SeqCst), 0);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL; terminal SyntheticFixture only"]
async fn postgres_observation_legacy_and_invalid_some_proof_do_not_become_business_recovery() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    fixture.prepare().await;
    let plan = fixture.draft().await;
    *fixture.clock.lock().unwrap() += Duration::hours(1);
    for kind in [
        "legacy",
        "valid",
        "invalid",
        "unavailable",
        "duplicate_legacy",
        "inconsistent_success",
    ] {
        let request = terminal(&fixture, &plan, "succeeded").await;
        let intent = attempt(&fixture, &request, false).await;
        let (result, evidence) = verification(&fixture, &request, &intent, kind);
        store_verification(&fixture, &request, &intent, result, evidence).await;
        let (status, body) = fixture
            .get(&format!("/v1/executions/{}/observation", request.id), &fixture.auth)
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_safe(&body);
        let view: ExecutionObservationView = serde_json::from_value(body).unwrap();
        assert_eq!(view.binding.state, ExecutionState::Succeeded);
        assert_eq!(view.business_recovery.state, ExecutionObservationState::Unknown);
        let saved = &view.steps[0].verifications[0];
        assert_eq!(saved.recorded_outcome, VerificationOutcome::Succeeded);
        assert_eq!(
            saved.proof_state,
            match kind {
                "legacy" | "duplicate_legacy" => ExecutionWindowProofState::LegacyMissing,
                "valid" => ExecutionWindowProofState::ValidHistorical,
                "unavailable" => ExecutionWindowProofState::Unavailable,
                _ => ExecutionWindowProofState::Invalid,
            }
        );
        assert!(saved.satisfied_conditions.len() <= 64 && saved.failed_conditions.len() <= 64);
        if kind == "valid" {
            assert_eq!(saved.provenance, VerificationProofOrigin::SyntheticFixture);
            assert_eq!(
                saved.observed_stable_seconds,
                Some(plan.steps[0].verification.stable_window_seconds)
            );
            assert!(
                !saved.target_version_verified,
                "Synthetic matching is not physical target attestation"
            );
            assert!(saved.sample_count.unwrap() >= 2);
        } else {
            assert!(!saved.target_version_verified);
        }
    }
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL; preserves actual legacy compensation result FALSE column"]
async fn postgres_observation_compensation_uses_original_intent_and_detects_orphan_or_bad_fence() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    fixture.prepare().await;
    let plan = fixture.draft().await;
    *fixture.clock.lock().unwrap() += Duration::hours(1);
    let request = terminal(&fixture, &plan, "escalated").await;
    let intent = attempt(&fixture, &request, true).await;
    let (status, body) = fixture
        .get(&format!("/v1/executions/{}/observation", request.id), &fixture.auth)
        .await;
    assert_eq!(status, StatusCode::OK);
    let view: ExecutionObservationView = serde_json::from_value(body).unwrap();
    assert!(!view.steps[0].partial);
    assert!(view.steps[0].attempts[0].compensation);
    assert_eq!(view.steps[0].compensation.state, ExecutionObservationState::Observed);
    assert_eq!(view.steps[0].effect.state, ExecutionObservationState::NotObserved);
    // A result without an intent cannot acquire a guessed Plan-step identity.
    let orphan = StepResult {
        step_id: ExecutionStepId::new(),
        state: ExecutionState::Escalated,
        agent_result: None,
        verification: None,
        reason_code: "private_orphan_text".to_owned(),
        completed_at: intent.intended_at,
    };
    sqlx::query("INSERT INTO execution_steps(execution_id,step_id,attempt,record_kind,result_snapshot,reason_code,occurred_at) VALUES($1,$2,1,'result',$3,'fixture',$4)")
        .bind(request.id.as_uuid()).bind(orphan.step_id.as_uuid()).bind(json!(orphan)).bind(orphan.completed_at).execute(&fixture.repository.pool).await.unwrap();
    let (_, body) = fixture
        .get(&format!("/v1/executions/{}/observation", request.id), &fixture.auth)
        .await;
    assert_eq!(body["history"]["state"], "unknown");
    assert_safe(&body);
    let bad = terminal(&fixture, &plan, "escalated").await;
    let _ = bad_fence_attempt(&fixture, &bad).await;
    let (_, body) = fixture
        .get(&format!("/v1/executions/{}/observation", bad.id), &fixture.auth)
        .await;
    assert_eq!(body["history"]["state"], "unknown");
    assert!(body["steps"][0]["attempts"].as_array().unwrap().is_empty());
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL; all failures precede any Executor or Agent request"]
async fn postgres_observation_scope_and_bounded_payload_errors_are_safe() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    fixture.prepare().await;
    let plan = fixture.draft().await;
    *fixture.clock.lock().unwrap() += Duration::hours(1);
    let request = terminal(&fixture, &plan, "escalated").await;
    for cross_tenant in [false, true] {
        let mut foreign = fixture.auth.clone();
        if cross_tenant {
            foreign.tenant_id = TenantId::new();
        } else {
            foreign.clusters.clear();
        }
        let (status, body) = fixture
            .get(&format!("/v1/executions/{}/observation", request.id), &foreign)
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_safe(&body);
    }
    let mut oversized = request.clone();
    oversized.id = ExecutionId::new();
    oversized.idempotency_key = format!("d20-oversized-{}", uuid::Uuid::new_v4());
    oversized.signature = "private_signature_canary".repeat(30_000);
    store_terminal(&fixture, &oversized, "escalated").await;
    for path in [
        format!("/v1/executions/{}/observation", oversized.id),
        format!("/v1/plans/{}/executions?plan_hash={}", plan.id, plan.plan_hash),
    ] {
        let (status, body) = fixture.get(&path, &fixture.auth).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "execution_history_unavailable");
        assert_safe(&body);
    }
    let (status, body) = fixture
        .call(
            "/v1/executions/lookup",
            &fixture.auth,
            json!({"plan_id":plan.id,"plan_hash":plan.plan_hash,
        "precondition_hash":plan.compute_precondition_hash().unwrap(),"idempotency_key":oversized.idempotency_key}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "execution_history_unavailable");
    assert_safe(&body);
    let mut corrupt = request.clone();
    corrupt.id = ExecutionId::new();
    corrupt.idempotency_key = "snapshot-key-B".to_owned();
    let outer_key = format!("outer-column-key-{}", corrupt.id);
    store_terminal_with_key(&fixture, &corrupt, "escalated", &outer_key).await;
    let (status, body) = fixture
        .call(
            "/v1/executions/lookup",
            &fixture.auth,
            json!({"plan_id":plan.id,"plan_hash":plan.plan_hash,
        "precondition_hash":plan.compute_precondition_hash().unwrap(),"idempotency_key":outer_key}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_execution_receipt");
    assert!(!body.to_string().contains("snapshot-key-B"));
    assert_eq!(fixture.observer.writes.load(Ordering::SeqCst), 0);
    fixture.close().await;
}

fn assert_safe(body: &Value) {
    let encoded = body.to_string();
    for private in [
        "private_signature_canary",
        "private_agent_result",
        "private_orphan_text",
        "fence_grant",
        "agent_result",
        "origin_signature",
        "postgresql://",
    ] {
        assert!(!encoded.contains(private), "unsafe projection contains {private}");
    }
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL; exact audit shapes, no target work"]
async fn postgres_observation_quarantine_timeline_uses_exact_source_execution_even_with_independent_clear_correlation()
{
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    fixture.prepare().await;
    let plan = fixture.draft().await;
    *fixture.clock.lock().unwrap() += Duration::hours(1);
    let request = terminal(&fixture, &plan, "escalated").await;
    let foreign = terminal(&fixture, &plan, "escalated").await;
    let now = *fixture.clock.lock().unwrap();
    for (execution, include) in [(&request, true), (&foreign, false)] {
        let id = ResourceQuarantineId::new();
        sqlx::query("INSERT INTO resource_quarantines(id,tenant_id,cluster_id,resource_key,reason_code,source_execution_id,created_by,created_at)
            VALUES($1,$2,$3,$4,'SyntheticFixture',$5,'SyntheticFixture',$6)")
            .bind(id.as_uuid()).bind(plan.tenant_id.as_uuid()).bind(plan.cluster_id.as_uuid()).bind(format!("{TARGET}:{id}"))
            .bind(execution.id.as_uuid()).bind(now).execute(&fixture.repository.pool).await.unwrap();
        for kind in [AuditEventKind::QuarantineCreated, AuditEventKind::QuarantineCleared] {
            let event = AuditEvent {
                id: AuditEventId::new(),
                tenant_id: plan.tenant_id,
                cluster_id: plan.cluster_id,
                correlation_id: if kind == AuditEventKind::QuarantineCleared {
                    CorrelationId::new()
                } else {
                    request.correlation_id
                },
                event_kind: kind,
                actor_subject: fixture.auth.subject.clone(),
                actor_role: "operator".to_owned(),
                resource_kind: "resource_quarantine".to_owned(),
                resource_id: id.to_string(),
                reason_code: "private_orphan_text".to_owned(),
                details: json!({"private_agent_result":"not_a_public_field"}),
                occurred_at: now,
            };
            sqlx::query("INSERT INTO audit_events(event_id,tenant_id,cluster_id,correlation_id,event_kind,actor_subject,actor_role,
                resource_kind,resource_id,reason_code,details,event_snapshot,occurred_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)")
                .bind(event.id.as_uuid()).bind(event.tenant_id.as_uuid()).bind(event.cluster_id.as_uuid()).bind(event.correlation_id.as_uuid())
                .bind(serde_json::to_value(kind).unwrap().as_str().unwrap()).bind(&event.actor_subject).bind(&event.actor_role)
                .bind(&event.resource_kind).bind(&event.resource_id).bind(&event.reason_code).bind(&event.details).bind(json!(event))
                .bind(now).execute(&fixture.repository.pool).await.unwrap();
        }
        if include {
            sqlx::query("UPDATE resource_quarantines SET cleared_at=$2,cleared_by='SyntheticFixture',clear_reason='fixture',clear_evidence_ids=$3 WHERE id=$1")
                .bind(id.as_uuid()).bind(now).bind(vec![fixture.diagnostic_id.as_uuid()]).execute(&fixture.repository.pool).await.unwrap();
        }
    }
    let (status, body) = fixture
        .get(&format!("/v1/executions/{}/observation", request.id), &fixture.auth)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_safe(&body);
    let view: ExecutionObservationView = serde_json::from_value(body).unwrap();
    assert_eq!(view.timeline.len(), 2);
    assert_eq!(view.timeline[0].kind, ExecutionObservationEventKind::QuarantineCreated);
    assert_eq!(view.timeline[1].kind, ExecutionObservationEventKind::QuarantineCleared);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL; wrong correlation is refused before SLO evaluation"]
async fn postgres_observation_internal_sli_binds_original_execution_correlation() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    fixture.prepare().await;
    let plan = fixture.draft().await;
    let request = terminal(&fixture, &plan, "escalated").await;
    let step = &plan.steps[0];
    let query = ExecutionSliQuery {
        schema_version: EXECUTION_VERIFICATION_SCHEMA_VERSION.to_owned(),
        tenant_id: plan.tenant_id,
        cluster_id: plan.cluster_id,
        correlation_id: CorrelationId::new(),
        conditions: step.verification.technical_slis.clone(),
        binding: Some(ExecutionSliBinding {
            execution_id: request.id,
            plan_step_id: step.id,
            action: step.action,
            descriptor_version: step.descriptor_version.clone(),
            target: step.resource.clone(),
            parameters_digest: canonical_sha256(&step.parameters).unwrap(),
        }),
    };
    let mut executor = fixture.auth.clone();
    executor.roles = BTreeSet::from(["executor_service".to_owned()]);
    let router = crate::execution_verification::routes().with_state(fixture.state.clone());
    let (status, body) = super::support::call(
        router,
        "/internal/v1/execution-verification/sli",
        &executor,
        json!(query),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["code"], "execution_verification_binding_invalid");
    assert_eq!(fixture.observer.reads.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.observer.writes.load(Ordering::SeqCst), 0);
    fixture.close().await;
}
