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

use std::sync::atomic::Ordering;

use axum::http::StatusCode;
use chrono::{Duration, Timelike};
use rocketmq_sre_contracts::*;
use serde_json::json;

use super::service::SupervisedExecutionService;
use crate::workflow::{WorkflowEventBus, WorkflowService};

mod critic;
mod d21_drills;
mod history;
mod observation;
mod protocol;
mod races;
mod selectors;
pub(super) mod support;
use support::*;

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL; uses real JWT/router, persisted proof and bounded readonly HTTP observer"]
async fn postgres_readiness_two_actor_preparation_draft_approval_and_first_dispatch() {
    let mut fixture = Fixture::new().await;
    let initial = fixture.readiness(&fixture.auth, &fixture.request).await;
    assert!(initial.operations.confirm_diagnosis.allowed);
    assert!(!initial.operations.prepare_precondition.allowed);
    assert_eq!(initial.stages.handler.state, ActionReadinessState::Unknown);
    assert_eq!(fixture.observer.reads.load(Ordering::SeqCst), 0);
    fixture.confirm().await;
    let confirmed = fixture.readiness(&fixture.auth, &fixture.request).await;
    assert!(confirmed.diagnosis.primary_model_lineage_valid);
    assert!(confirmed.operations.prepare_precondition.allowed);
    assert!(!confirmed.operations.confirm_diagnosis.allowed);
    assert_eq!(
        confirmed.operations.confirm_diagnosis.reason_codes,
        vec![ActionReadinessReason::DiagnosisAlreadyConfirmed]
    );
    let prepared = fixture.prepare().await;
    assert_eq!(fixture.observer.reads.load(Ordering::SeqCst), 1);
    let loaded = fixture
        .repository
        .evidence(&fixture.auth, prepared.evidence_id)
        .await
        .unwrap();
    loaded.verify_content_hash().unwrap();
    assert_eq!(loaded.observed_at.nanosecond(), 123_456_000);
    let EvidenceContent::Inline(content) = &loaded.content else {
        panic!("prepared proof is inline");
    };
    let proof: ActionPreparationEvidence = serde_json::from_value(content.clone()).unwrap();
    assert_eq!(proof.agent_result.observed_at.nanosecond(), 123_456_789);
    assert!(!proof.agent_result.resource_conditions["logger_level_applied"]);
    assert!(!proof.agent_result.resource_conditions["ttl_restore_scheduled"]);
    let readiness = fixture.readiness(&fixture.auth, &fixture.request).await;
    assert!(readiness.operations.create_plan.allowed, "{readiness:?}");
    assert_eq!(readiness.stages.handler.state, ActionReadinessState::Satisfied);
    assert_eq!(readiness.stages.enabled.state, ActionReadinessState::Satisfied);
    assert_eq!(readiness.stages.target.state, ActionReadinessState::Unknown);
    assert_eq!(readiness.stages.qualification.state, ActionReadinessState::Unknown);
    assert_eq!(readiness.stages.compensation.state, ActionReadinessState::Unknown);
    let plan = fixture.draft().await;
    assert_eq!(plan.status, PlanStatus::ReadyForApproval);
    let approval = json!({"plan_hash":plan.plan_hash,"precondition_hash":plan.compute_precondition_hash().unwrap(),
        "reason":"Independent action consent for this immutable candidate", "validity_seconds":30});
    let mut self_approver = fixture.auth.clone();
    self_approver.roles.insert("approver".to_owned());
    let (status, body) = fixture
        .call(
            &format!("/v1/plans/{}/approve", plan.id),
            &self_approver,
            approval.clone(),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "self_approval_forbidden");
    let (status, body) = fixture
        .call(&format!("/v1/plans/{}/approve", plan.id), &fixture.approver(), approval)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut plan_request = fixture.request.clone();
    plan_request.plan_id = Some(plan.id);
    plan_request.plan_step_id = Some(plan.steps[0].id);
    plan_request.precondition_evidence_id = None;
    let approval_read = fixture.readiness(&fixture.approver(), &plan_request).await;
    assert_eq!(
        approval_read.stages.evidence.state,
        ActionReadinessState::Satisfied,
        "{approval_read:?}"
    );
    assert_eq!(approval_read.stages.approval.state, ActionReadinessState::Satisfied);
    let (status, body) = fixture.call("/v1/executions", &fixture.auth, json!({"plan_id":plan.id,"plan_hash":plan.plan_hash,
        "precondition_hash":plan.compute_precondition_hash().unwrap(),"idempotency_key":"d18-first-submit-not-qualified"})).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "action_readiness_blocked");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM executions WHERE plan_id=$1")
        .bind(plan.id.as_uuid())
        .fetch_one(&fixture.repository.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(fixture.observer.reads.load(Ordering::SeqCst), 1);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL"]
async fn postgres_readiness_scope_roles_rule_rotation_and_candidate_binding_are_current() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    let mut viewer = fixture.auth.clone();
    viewer.roles.clear();
    let view = fixture.readiness(&viewer, &fixture.request).await;
    assert!(!view.operations.prepare_precondition.allowed);
    assert!(
        view.operations
            .prepare_precondition
            .reason_codes
            .contains(&ActionReadinessReason::OperatorRequired)
    );
    let path = format!("/v1/incidents/{}/execution-preconditions", fixture.incident_id);
    let (status, _) = fixture.call(&path, &viewer, fixture.prepare_body()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let mut foreign = fixture.prepare_body();
    foreign["resource"] = json!("broker/192.0.2.55:10911");
    let (status, body) = fixture.call(&path, &fixture.auth, foreign).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "target_scope_unverified");
    let mut wrong_claims = fixture.auth.clone();
    wrong_claims.clusters.clear();
    let (status, _) = fixture.call(&path, &wrong_claims, fixture.prepare_body()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(fixture.observer.reads.load(Ordering::SeqCst), 0);
    fixture.prepare().await;
    let mut changed = fixture.request.clone();
    changed.parameters["ttl_seconds"] = json!(121);
    let view = fixture.readiness(&fixture.auth, &changed).await;
    assert!(!view.operations.create_plan.allowed);
    assert!(
        view.stages
            .evidence
            .reason_codes
            .contains(&ActionReadinessReason::PreparationBindingMismatch)
    );
    let old_service = fixture.state.supervised_execution.clone();
    fixture.state.supervised_execution = old_service.clone().with_target_scope(scope(&fixture.auth, "2"));
    let view = fixture.readiness(&fixture.auth, &fixture.request).await;
    assert!(
        view.stages
            .evidence
            .reason_codes
            .contains(&ActionReadinessReason::TargetScopeChanged)
    );
    assert_eq!(
        fixture.call("/v1/plans", &fixture.auth, fixture.create_body()).await.0,
        StatusCode::CONFLICT
    );
    let clock = fixture.clock.clone();
    fixture.state.supervised_execution = SupervisedExecutionService::new_with_clock(
        fixture.repository.clone(),
        WorkflowService::new(fixture.repository.clone(), WorkflowEventBus::new(16)),
        KEY,
        std::sync::Arc::new(move || *clock.lock().unwrap()),
    )
    .unwrap()
    .with_target_scope(fixture.scope.clone());
    let disabled = fixture.readiness(&fixture.auth, &fixture.request).await;
    assert_eq!(disabled.stages.handler.state, ActionReadinessState::Satisfied);
    assert_eq!(disabled.stages.enabled.state, ActionReadinessState::Blocked);
    assert_eq!(
        disabled.stages.enabled.reason_codes,
        vec![ActionReadinessReason::ExecutorNotConfigured]
    );
    assert!(!disabled.operations.create_plan.allowed);
    fixture.state.supervised_execution = old_service;
    assert_eq!(fixture.observer.reads.load(Ordering::SeqCst), 1);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL"]
async fn postgres_readiness_origin_forgery_and_outer_time_renewal_fail_before_plan_admission() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    let prepared = fixture.prepare().await;
    let mut forged = prepared.clone();
    forged.evidence_id = EvidenceId::new();
    if let EvidenceContent::Inline(value) = &mut forged.content {
        value["evidence_id"] = json!(forged.evidence_id);
        value["origin_signature"] = json!("hmac-sha256:public-caller-forged-origin");
    }
    forged.content_hash = forged.compute_content_hash().unwrap();
    let (status, body) = fixture
        .call(
            "/internal/v1/evidence",
            &fixture.auth,
            json!({"incident_id":fixture.incident_id,"evidence":forged}),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "reserved_evidence_schema");
    // Defense in depth: an engineering fixture bypasses the generic ingress,
    // preserving valid content integrity but not a valid CP origin attestation.
    fixture
        .repository
        .persist_evidence(
            &fixture.auth,
            &forged,
            None,
            Some(fixture.incident_id),
            &forged.content_hash,
        )
        .await
        .unwrap();
    fixture.request.precondition_evidence_id = Some(forged.evidence_id);
    let view = fixture.readiness(&fixture.auth, &fixture.request).await;
    assert_eq!(view.stages.handler.state, ActionReadinessState::Unknown);
    assert!(
        view.stages
            .evidence
            .reason_codes
            .contains(&ActionReadinessReason::PreparationOriginInvalid)
    );
    let (status, body) = fixture.call("/v1/plans", &fixture.auth, fixture.create_body()).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "preparation_origin_invalid");
    // A copied valid seal with another EID also fails, even without editing content.
    let mut copied = prepared.clone();
    copied.evidence_id = EvidenceId::new();
    fixture
        .repository
        .persist_evidence(
            &fixture.auth,
            &copied,
            None,
            Some(fixture.incident_id),
            &copied.content_hash,
        )
        .await
        .unwrap();
    // persist_evidence may deduplicate identical immutable content to its original
    // record. Change only the outer point to a different microsecond in the same ms.
    let mut shifted = prepared.clone();
    shifted.evidence_id = EvidenceId::new();
    shifted.observed_at += Duration::microseconds(1);
    shifted.time_range = TimeRange::new(shifted.observed_at, shifted.observed_at).unwrap();
    shifted.content_hash = shifted.compute_content_hash().unwrap();
    let persisted = fixture
        .repository
        .persist_evidence(
            &fixture.auth,
            &shifted,
            None,
            Some(fixture.incident_id),
            &shifted.content_hash,
        )
        .await
        .unwrap();
    fixture.request.precondition_evidence_id = Some(persisted.evidence_id);
    let view = fixture.readiness(&fixture.auth, &fixture.request).await;
    assert!(!view.operations.create_plan.allowed);
    assert!(
        view.stages
            .evidence
            .reason_codes
            .contains(&ActionReadinessReason::PreparationBindingMismatch)
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM action_plans WHERE incident_id=$1")
        .bind(fixture.incident_id.as_uuid())
        .fetch_one(&fixture.repository.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(fixture.observer.reads.load(Ordering::SeqCst), 1);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL"]
async fn postgres_readiness_manual_runbook_remains_non_executable_without_prepared_proof() {
    let fixture = Fixture::new().await;
    let request = json!({"cluster_id":fixture.request.cluster_id,"incident_id":fixture.incident_id,
        "diagnosis_revision_id":fixture.source_revision,"steps":[{"action_id":"topic.delete","descriptor_version":"1.0.0",
        "resource":"topic/orders","parameters":{},"evidence_ids":[fixture.diagnostic_id]}]});
    let (status, body) = fixture.call("/v1/plans", &fixture.auth, request).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], "manual_runbook");
    assert_eq!(body["runbook"]["execution_supported"], false);
    assert!(body.get("plan").is_none());
    let unknown_plan = ActionPlanId::new();
    for path in [format!("/v1/plans/{unknown_plan}/approve"), "/v1/executions".to_owned()] {
        let request = if path == "/v1/executions" {
            json!({"plan_id":unknown_plan,"plan_hash":format!("sha256:{}","a".repeat(64)),
            "precondition_hash":format!("sha256:{}","b".repeat(64)),"idempotency_key":"d18-manual-cannot-dispatch"})
        } else {
            json!({"plan_hash":format!("sha256:{}","a".repeat(64)),"precondition_hash":format!("sha256:{}","b".repeat(64)),"reason":"manual output cannot be approved"})
        };
        let actor = if path == "/v1/executions" {
            fixture.auth.clone()
        } else {
            fixture.approver()
        };
        assert_eq!(fixture.call(&path, &actor, request).await.0, StatusCode::NOT_FOUND);
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM action_plans WHERE incident_id=$1")
        .bind(fixture.incident_id.as_uuid())
        .fetch_one(&fixture.repository.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(fixture.observer.reads.load(Ordering::SeqCst), 0);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL; metadata corruption is confined to the regression database"]
async fn postgres_readiness_distinguishes_missing_invalid_and_stale_diagnostic_evidence() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    let prepared = fixture.prepare().await;
    assert_eq!(fixture.request.precondition_evidence_id, Some(prepared.evidence_id));
    let baseline = fixture
        .repository
        .evidence(&fixture.auth, fixture.diagnostic_id)
        .await
        .unwrap();
    let now = *fixture.clock.lock().unwrap();
    let baseline_hash = baseline.content_hash.clone();
    let invalid_hash = format!("sha256:{}", "0".repeat(64));
    assert!(
        fixture
            .readiness(&fixture.auth, &fixture.request)
            .await
            .operations
            .create_plan
            .allowed
    );
    for (partial, coverage, hash, observed_at, source_age, reason, state) in [
        (
            true,
            "available",
            baseline_hash.as_str(),
            baseline.observed_at,
            5,
            ActionReadinessReason::EvidenceMissing,
            ActionReadinessState::Unknown,
        ),
        (
            false,
            "partial",
            baseline_hash.as_str(),
            baseline.observed_at,
            5,
            ActionReadinessReason::EvidenceMissing,
            ActionReadinessState::Unknown,
        ),
        (
            false,
            "available",
            invalid_hash.as_str(),
            baseline.observed_at,
            5,
            ActionReadinessReason::EvidenceInvalid,
            ActionReadinessState::Blocked,
        ),
        (
            false,
            "available",
            baseline_hash.as_str(),
            now + Duration::seconds(1),
            5,
            ActionReadinessReason::EvidenceInvalid,
            ActionReadinessState::Blocked,
        ),
        (
            false,
            "available",
            baseline_hash.as_str(),
            baseline.observed_at,
            901,
            ActionReadinessReason::EvidenceStale,
            ActionReadinessState::Blocked,
        ),
    ] {
        // A real sealed preparation is kept unchanged. Only this disposable
        // diagnostic record is varied, so its MAC cannot hide a later gap.
        sqlx::query("UPDATE evidence_snapshots SET partial=$2,coverage=$3,content_hash=$4,observed_at=$5,freshness_seconds=$6 WHERE id=$1")
            .bind(fixture.diagnostic_id.as_uuid())
            .bind(partial)
            .bind(coverage)
            .bind(hash)
            .bind(observed_at)
            .bind(i64::from(source_age))
            .execute(&fixture.repository.pool)
            .await
            .unwrap();
        for proof in [None, Some(prepared.evidence_id)] {
            let mut request = fixture.request.clone();
            request.precondition_evidence_id = proof;
            let view = fixture.readiness(&fixture.auth, &request).await;
            assert_eq!(
                view.binding
                    .precondition_evidence
                    .as_ref()
                    .map(|value| value.evidence_id),
                proof
            );
            assert_eq!(view.stages.evidence.state, state, "{reason:?}/{proof:?}: {view:?}");
            assert_eq!(view.stages.evidence.reason_codes, vec![reason]);
            assert!(!view.operations.prepare_precondition.allowed);
            assert!(!view.operations.create_plan.allowed);
            assert!(view.operations.prepare_precondition.reason_codes.contains(&reason));
            assert!(view.operations.create_plan.reason_codes.contains(&reason));
            if reason != ActionReadinessReason::EvidenceStale {
                assert!(
                    !view
                        .stages
                        .evidence
                        .reason_codes
                        .contains(&ActionReadinessReason::EvidenceStale)
                );
                assert!(
                    !view
                        .operations
                        .prepare_precondition
                        .reason_codes
                        .contains(&ActionReadinessReason::EvidenceStale)
                );
            }
        }
        let (status, result) = fixture
            .call(
                &format!("/v1/incidents/{}/execution-preconditions", fixture.incident_id),
                &fixture.auth,
                fixture.prepare_body(),
            )
            .await;
        if hash == invalid_hash.as_str() {
            // Repository integrity rejection precedes the age helper. Preserve
            // its existing safe HTTP code instead of bypassing that check.
            assert_eq!(status, StatusCode::BAD_REQUEST, "{result}");
            assert_eq!(result["code"], "source_unavailable");
        } else {
            assert_eq!(status, StatusCode::CONFLICT, "{reason:?}: {result}");
            assert_eq!(result["code"], "action_evidence_stale_or_invalid");
        }
        assert_eq!(fixture.observer.reads.load(Ordering::SeqCst), 1);
    }
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL; retention/link absence is confined to this fixture"]
async fn postgres_readiness_unavailable_diagnostic_records_remain_unknown_without_hiding_independent_binding_errors() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    fixture.prepare().await;
    let expires_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT expires_at FROM evidence_snapshots WHERE id=$1")
            .bind(fixture.diagnostic_id.as_uuid())
            .fetch_one(&fixture.repository.pool)
            .await
            .unwrap();
    for retained in [false, true] {
        if retained {
            sqlx::query("UPDATE evidence_snapshots SET expires_at=$2 WHERE id=$1")
                .bind(fixture.diagnostic_id.as_uuid())
                .bind(expires_at)
                .execute(&fixture.repository.pool)
                .await
                .unwrap();
            sqlx::query("DELETE FROM evidence_links WHERE evidence_id=$1 AND incident_id=$2")
                .bind(fixture.diagnostic_id.as_uuid())
                .bind(fixture.incident_id.as_uuid())
                .execute(&fixture.repository.pool)
                .await
                .unwrap();
        } else {
            sqlx::query("UPDATE evidence_snapshots SET expires_at=NOW()-INTERVAL '1 second' WHERE id=$1")
                .bind(fixture.diagnostic_id.as_uuid())
                .execute(&fixture.repository.pool)
                .await
                .unwrap();
            let error = fixture
                .repository
                .evidence(&fixture.auth, fixture.diagnostic_id)
                .await
                .unwrap_err();
            assert_eq!(error.failure(), crate::ControlPlaneFailure::NotFound);
        }
        assert!(
            !fixture
                .repository
                .evidence_linked_to_incident(&fixture.auth, fixture.incident_id, fixture.diagnostic_id)
                .await
                .unwrap()
        );
        for proof in [None, fixture.request.precondition_evidence_id] {
            let mut request = fixture.request.clone();
            request.precondition_evidence_id = proof;
            let view = fixture.readiness(&fixture.auth, &request).await;
            assert_eq!(view.stages.evidence.state, ActionReadinessState::Unknown, "{view:?}");
            assert_eq!(
                view.stages.evidence.reason_codes,
                vec![ActionReadinessReason::EvidenceMissing]
            );
            assert!(!view.operations.prepare_precondition.allowed);
            assert!(!view.operations.create_plan.allowed);
            assert!(
                view.operations
                    .prepare_precondition
                    .reason_codes
                    .contains(&ActionReadinessReason::EvidenceMissing)
            );
        }
        let mut wrong_parameters = fixture.request.clone();
        wrong_parameters.parameters["level"] = json!("INFO");
        let view = fixture.readiness(&fixture.auth, &wrong_parameters).await;
        assert_eq!(view.stages.evidence.state, ActionReadinessState::Blocked);
        assert!(
            view.stages
                .evidence
                .reason_codes
                .contains(&ActionReadinessReason::PreparationBindingMismatch)
        );
        let (status, _) = fixture
            .call(
                &format!("/v1/incidents/{}/execution-preconditions", fixture.incident_id),
                &fixture.auth,
                fixture.prepare_body(),
            )
            .await;
        assert!(status.is_client_error());
        assert_eq!(fixture.observer.reads.load(Ordering::SeqCst), 1);
    }
    fixture.close().await;
}
