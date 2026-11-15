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

use axum::http::StatusCode;
use chrono::Duration;
use rocketmq_sre_contracts::*;
use serde_json::json;
use std::sync::atomic::Ordering;

use super::super::signing::GrantSigner;
use super::support::*;

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL"]
async fn postgres_readiness_new_same_resource_proof_cannot_renew_original_plan_evidence() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    let original = fixture.prepare().await;
    let old_plan = fixture.draft().await;
    let old_request = fixture.request.clone();
    let old_body = fixture.create_body();
    *fixture.clock.lock().unwrap() += Duration::seconds(121);
    let now = *fixture.clock.lock().unwrap();
    *fixture.observer.sample_at.lock().unwrap() = now - Duration::milliseconds(50);
    let fresh = fixture.prepare().await;
    assert_ne!(original.evidence_id, fresh.evidence_id);
    let old = fixture.readiness(&fixture.auth, &old_request).await;
    let new = fixture.readiness(&fixture.auth, &fixture.request).await;
    assert!(!old.operations.create_plan.allowed);
    assert_eq!(
        old.stages.evidence.reason_codes,
        vec![ActionReadinessReason::EvidenceStale]
    );
    assert!(new.operations.create_plan.allowed, "{new:?}");
    let (status, result) = fixture.call("/v1/plans", &fixture.auth, old_body).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(result["code"], "action_evidence_stale_or_invalid");
    let (status, result) = fixture
        .call(
            &format!("/v1/plans/{}/approve", old_plan.id),
            &fixture.approver(),
            json!({"plan_hash":old_plan.plan_hash,"precondition_hash":old_plan.compute_precondition_hash().unwrap(),
            "reason":"new evidence must not renew the original immutable plan"}),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(result["code"], "action_evidence_stale_or_invalid");
    fixture.draft().await;
    assert_eq!(fixture.observer.reads.load(Ordering::SeqCst), 2);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL"]
async fn postgres_readiness_plain_legacy_proof_is_readable_but_cannot_create_new_action_plan() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    let prepared = fixture.prepare().await;
    let EvidenceContent::Inline(content) = &prepared.content else {
        panic!("inline proof");
    };
    let proof: ActionPreparationEvidence = serde_json::from_value(content.clone()).unwrap();
    let mut legacy = prepared;
    legacy.evidence_id = EvidenceId::new();
    legacy.content = EvidenceContent::Inline(serde_json::to_value(proof.agent_result).unwrap());
    legacy.content_hash = legacy.compute_content_hash().unwrap();
    let legacy = fixture
        .repository
        .persist_evidence(
            &fixture.auth,
            &legacy,
            None,
            Some(fixture.incident_id),
            &legacy.content_hash,
        )
        .await
        .unwrap();
    fixture
        .repository
        .evidence(&fixture.auth, legacy.evidence_id)
        .await
        .unwrap()
        .verify_content_hash()
        .unwrap();
    fixture.request.precondition_evidence_id = Some(legacy.evidence_id);
    let view = fixture.readiness(&fixture.auth, &fixture.request).await;
    assert_eq!(
        view.stages.evidence.reason_codes,
        vec![ActionReadinessReason::PreparationBindingMissing]
    );
    let (status, result) = fixture.call("/v1/plans", &fixture.auth, fixture.create_body()).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(result["code"], "preparation_binding_missing");
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL; CP-only accepted reconciliation uses an exact immutable HTTP receipt fixture"]
async fn postgres_readiness_accepted_and_finished_receipts_keep_original_linearization() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    fixture.prepare().await;
    let mut plan = fixture.draft().await;
    let approval = fixture.approve(&plan).await;
    plan.status = PlanStatus::Approved;
    let now = *fixture.clock.lock().unwrap();
    let mut accepted = ExecutionRequest {
        schema_version: ExecutionRequest::SCHEMA_VERSION.to_owned(),
        id: ExecutionId::new(),
        tenant_id: plan.tenant_id,
        cluster_id: plan.cluster_id,
        correlation_id: CorrelationId::new(),
        plan: plan.clone(),
        approvals: vec![approval.clone()],
        autonomy_grant: None,
        requested_by: fixture.auth.subject.clone(),
        idempotency_key: format!("d18-accepted-original-{}", uuid::Uuid::new_v4()),
        issuer: "rocketmq-sre-control-plane".to_owned(),
        audience: "rocketmq-sre-executor".to_owned(),
        issued_at: now,
        expires_at: approval.expires_at,
        nonce: uuid::Uuid::new_v4().to_string(),
        signature: String::new(),
    };
    GrantSigner::new(KEY).unwrap().sign_execution(&mut accepted).unwrap();
    // This deliberately models an intent accepted before D18. It never invokes
    // new dispatch admission and is confined to the regression database.
    sqlx::query(
        "INSERT INTO executions(id,tenant_id,cluster_id,correlation_id,plan_id,plan_hash,resource_key,action_id,
        idempotency_key,state,request_snapshot,requested_by,started_at,updated_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending',$10,$11,$12,$12)",
    )
    .bind(accepted.id.as_uuid())
    .bind(plan.tenant_id.as_uuid())
    .bind(plan.cluster_id.as_uuid())
    .bind(accepted.correlation_id.as_uuid())
    .bind(plan.id.as_uuid())
    .bind(&plan.plan_hash)
    .bind(TARGET)
    .bind(ACTION)
    .bind(&accepted.idempotency_key)
    .bind(json!(accepted))
    .bind(&fixture.auth.subject)
    .bind(now)
    .execute(&fixture.repository.pool)
    .await
    .unwrap();
    *fixture.observer.accepted.lock().unwrap() = Some(accepted.clone());
    *fixture.clock.lock().unwrap() += Duration::hours(2);
    fixture
        .repository
        .persist_diagnosis_revision(
            &fixture.auth,
            fixture.incident_id,
            IncidentStatus::Diagnosing,
            json!({"provenance":"SyntheticFixture"}),
            json!([]),
            vec![fixture.diagnostic_id],
            false,
            None,
            "rules_only",
            CorrelationId::new(),
        )
        .await
        .unwrap();
    let body = json!({"plan_id":plan.id,"plan_hash":plan.plan_hash,"precondition_hash":plan.compute_precondition_hash().unwrap(),
        "idempotency_key":accepted.idempotency_key});
    let (status, result) = fixture.call("/v1/executions", &fixture.auth, body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["execution"], json!(accepted));
    assert_eq!(fixture.observer.reconciliations.load(Ordering::SeqCst), 1);
    // Finish this engineering receipt through the permitted no-target-effect
    // escalation transition; do not invent a successful execution.
    sqlx::query("UPDATE executions SET state='escalated', completed_at=NOW() WHERE id=$1")
        .bind(accepted.id.as_uuid())
        .execute(&fixture.repository.pool)
        .await
        .unwrap();
    let (status, result) = fixture.call("/v1/executions", &fixture.auth, body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["execution"], json!(accepted));
    assert_eq!(result["state"], "escalated");
    assert_eq!(fixture.observer.reconciliations.load(Ordering::SeqCst), 1);
    let mut changed = body.clone();
    changed["precondition_hash"] = json!(format!("sha256:{}", "e".repeat(64)));
    let (status, result) = fixture.call("/v1/executions", &fixture.auth, changed).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(result["code"], "idempotency_conflict");
    let mut viewer = fixture.auth.clone();
    viewer.roles.clear();
    assert_eq!(
        fixture.call("/v1/executions", &viewer, body).await.0,
        StatusCode::FORBIDDEN
    );
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM executions WHERE plan_id=$1")
        .bind(plan.id.as_uuid())
        .fetch_one(&fixture.repository.pool)
        .await
        .unwrap();
    assert_eq!(rows, 1);
    assert_eq!(fixture.observer.reads.load(Ordering::SeqCst), 1);
    fixture.close().await;
}
