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

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;

use chrono::Duration;
use chrono::Timelike;
use chrono::Utc;
use rocketmq_sre_contracts::ActionRisk;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::DiagnosisRevisionId;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceExposure;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::PlanStatus;
use rocketmq_sre_contracts::PolicyEffect;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::ResourceQuarantineId;
use rocketmq_sre_contracts::SchemaVersion;
use rocketmq_sre_contracts::Sensitivity;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::TimeRange;
use serde_json::json;
use uuid::Uuid;

use super::model::ApprovalDecisionRequest;
use super::model::CandidatePlanStep;
use super::model::ClearQuarantineRequest;
use super::model::CreatePlanRequest;
use super::model::CreatePlanResponse;
use super::model::SubmitExecutionRequest;
use super::service::SupervisedExecutionService;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::workflow::WorkflowEventBus;
use crate::workflow::WorkflowService;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_plan_policy_approval_audit_and_quarantine_are_fail_closed() {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 5).await.expect("repository");
    let fixture = seed_fixture(&repository).await;
    let workflow = WorkflowService::new(repository.clone(), WorkflowEventBus::new(64));
    let clock_now = Arc::new(Mutex::new(Utc::now()));
    let service_clock = Arc::clone(&clock_now);
    let service = SupervisedExecutionService::new_with_clock(
        repository.clone(),
        workflow,
        "phase3-test-signing-key-not-exported",
        Arc::new(move || *service_clock.lock().expect("test clock")),
    )
    .expect("service");
    let operator = auth(fixture.tenant_id, fixture.cluster_id, "operator-a", &["operator"]);
    let approver = auth(fixture.tenant_id, fixture.cluster_id, "approver-b", &["approver"]);
    let create = CreatePlanRequest {
        cluster_id: fixture.cluster_id,
        incident_id: fixture.incident_id,
        diagnosis_revision_id: fixture.diagnosis_id,
        expires_at: Some(Utc::now() + Duration::hours(1)),
        steps: vec![CandidatePlanStep {
            action_id: "proxy.scale_out_one.v1".to_owned(),
            descriptor_version: "1.0.0".to_owned(),
            resource: "deployment/default/proxy".to_owned(),
            parameters: json!({
                "namespace": "default",
                "workload": "proxy",
                "expected_replicas": 2
            }),
            evidence_ids: vec![fixture.evidence_id],
        }],
    };
    let created = service
        .create_plan(&operator, &create, CorrelationId::new())
        .await
        .expect("plan");
    let (plan, risk, policy) = match created {
        CreatePlanResponse::ActionPlan {
            plan,
            risk,
            policy_decision,
        } => (plan, risk, policy_decision),
        CreatePlanResponse::ManualRunbook { .. } => panic!("model-backed diagnosis must produce a plan"),
    };
    assert_eq!(risk, ActionRisk::R1);
    assert_eq!(plan.status, PlanStatus::ReadyForApproval);
    assert_eq!(policy.effect, PolicyEffect::RequireApproval);
    let precondition_hash = plan.compute_precondition_hash().expect("precondition hash");
    let approval_request = ApprovalDecisionRequest {
        plan_hash: plan.plan_hash.clone(),
        precondition_hash: precondition_hash.clone(),
        reason: "Evidence and blast radius reviewed".to_owned(),
        validity_seconds: Some(30),
    };
    let reject_created = service
        .create_plan(&operator, &create, CorrelationId::new())
        .await
        .expect("plan for rejection");
    let CreatePlanResponse::ActionPlan { plan: reject_plan, .. } = reject_created else {
        panic!("model-backed diagnosis must produce a rejectable plan");
    };
    let reject_correlation = CorrelationId::new();
    let rejected = service
        .reject(
            &approver,
            reject_plan.id,
            &ApprovalDecisionRequest {
                plan_hash: reject_plan.plan_hash.clone(),
                precondition_hash: reject_plan.compute_precondition_hash().expect("precondition hash"),
                reason: "Blast radius is not acceptable".to_owned(),
                validity_seconds: None,
            },
            reject_correlation,
        )
        .await
        .expect("rejection");
    assert_eq!(rejected.plan.status, PlanStatus::Rejected);
    assert_eq!(
        rejected.approval.decision,
        rocketmq_sre_contracts::ApprovalDecision::Rejected
    );
    assert!(rejected.grant.is_none());
    assert!(
        service
            .audit(&approver, reject_correlation)
            .await
            .expect("rejection audit")
            .items
            .len()
            >= 2
    );

    let no_approval = service
        .submit_execution(
            &operator,
            &SubmitExecutionRequest {
                plan_id: plan.id,
                plan_hash: plan.plan_hash.clone(),
                precondition_hash: precondition_hash.clone(),
                idempotency_key: format!("phase3-no-approval-{}", Uuid::new_v4()),
            },
            CorrelationId::new(),
        )
        .await
        .expect_err("execution without approval must fail");
    assert_eq!(error_code(&no_approval), "approval_required");
    let self_approval = service
        .approve(
            &auth(
                fixture.tenant_id,
                fixture.cluster_id,
                "operator-a",
                &["operator", "approver"],
            ),
            plan.id,
            &approval_request,
            CorrelationId::new(),
        )
        .await
        .expect_err("self approval must fail");
    assert_eq!(error_code(&self_approval), "self_approval_forbidden");
    let no_approver = service
        .approve(
            &auth(fixture.tenant_id, fixture.cluster_id, "operator-c", &["operator"]),
            plan.id,
            &approval_request,
            CorrelationId::new(),
        )
        .await
        .expect_err("missing approver role must fail");
    assert_eq!(error_code(&no_approver), "approver_role_required");
    let mut wrong_hash = approval_request.clone();
    wrong_hash.plan_hash = format!("sha256:{}", "f".repeat(64));
    assert_eq!(
        error_code(
            &service
                .approve(&approver, plan.id, &wrong_hash, CorrelationId::new())
                .await
                .expect_err("wrong hash")
        ),
        "plan_hash_mismatch"
    );
    let wrong_approval_scope = auth(fixture.tenant_id, ClusterId::new(), "approver-e", &["approver"]);
    assert_eq!(
        error_code(
            &service
                .approve(&wrong_approval_scope, plan.id, &approval_request, CorrelationId::new(),)
                .await
                .expect_err("approval outside cluster scope must fail")
        ),
        "cluster_not_allowed"
    );
    let approved = service
        .approve(&approver, plan.id, &approval_request, CorrelationId::new())
        .await
        .expect("approval");
    assert_eq!(approved.plan.status, PlanStatus::Approved);
    assert!(approved.grant.is_some());

    let wrong_precondition = service
        .submit_execution(
            &operator,
            &SubmitExecutionRequest {
                plan_id: plan.id,
                plan_hash: plan.plan_hash.clone(),
                precondition_hash: format!("sha256:{}", "e".repeat(64)),
                idempotency_key: format!("phase3-wrong-precondition-{}", Uuid::new_v4()),
            },
            CorrelationId::new(),
        )
        .await
        .expect_err("changed precondition must fail");
    assert_eq!(error_code(&wrong_precondition), "precondition_changed");

    let quarantine_id = insert_quarantine(&repository, &fixture).await;
    let quarantined = service
        .submit_execution(
            &operator,
            &SubmitExecutionRequest {
                plan_id: plan.id,
                plan_hash: plan.plan_hash.clone(),
                precondition_hash: precondition_hash.clone(),
                idempotency_key: format!("phase3-quarantined-{}", Uuid::new_v4()),
            },
            CorrelationId::new(),
        )
        .await
        .expect_err("quarantine must fail closed");
    assert_eq!(error_code(&quarantined), "ResourceQuarantined");
    let no_evidence = service
        .clear_quarantine(
            &approver,
            quarantine_id,
            &ClearQuarantineRequest {
                reason: "verified".to_owned(),
                evidence_ids: Vec::new(),
            },
            CorrelationId::new(),
        )
        .await
        .expect_err("clear requires evidence");
    assert_eq!(error_code(&no_evidence), "verification_evidence_required");
    let wrong_scope = auth(fixture.tenant_id, ClusterId::new(), "approver-d", &["approver"]);
    assert!(
        service
            .clear_quarantine(
                &wrong_scope,
                quarantine_id,
                &ClearQuarantineRequest {
                    reason: "verified".to_owned(),
                    evidence_ids: vec![fixture.evidence_id],
                },
                CorrelationId::new(),
            )
            .await
            .is_err()
    );
    let clear_correlation = CorrelationId::new();
    let cleared = service
        .clear_quarantine(
            &approver,
            quarantine_id,
            &ClearQuarantineRequest {
                reason: "Synthetic path and replica readiness manually verified".to_owned(),
                evidence_ids: vec![fixture.evidence_id],
            },
            clear_correlation,
        )
        .await
        .expect("quarantine clear");
    assert!(!cleared.is_active());
    let audit = service.audit(&approver, clear_correlation).await.expect("audit");
    assert_eq!(audit.items.len(), 2);

    {
        let mut now = clock_now.lock().expect("test clock");
        *now += Duration::seconds(31);
    }
    let expired_approval = service
        .submit_execution(
            &operator,
            &SubmitExecutionRequest {
                plan_id: plan.id,
                plan_hash: plan.plan_hash.clone(),
                precondition_hash: precondition_hash.clone(),
                idempotency_key: format!("phase3-expired-approval-{}", Uuid::new_v4()),
            },
            CorrelationId::new(),
        )
        .await
        .expect_err("expired approval must fail");
    assert_eq!(error_code(&expired_approval), "approval_expired");

    persist_changed_evidence(&repository, &operator, &fixture).await;
    let changed_target = service
        .submit_execution(
            &operator,
            &SubmitExecutionRequest {
                plan_id: plan.id,
                plan_hash: plan.plan_hash.clone(),
                precondition_hash,
                idempotency_key: format!("phase3-changed-target-{}", Uuid::new_v4()),
            },
            CorrelationId::new(),
        )
        .await
        .expect_err("newer live Evidence must invalidate the approval");
    assert_eq!(error_code(&changed_target), "precondition_changed");

    let rules_only = CreatePlanRequest {
        diagnosis_revision_id: fixture.rules_only_diagnosis_id,
        ..create.clone()
    };
    assert!(matches!(
        service
            .create_plan(&operator, &rules_only, CorrelationId::new())
            .await
            .expect("manual runbook"),
        CreatePlanResponse::ManualRunbook { .. }
    ));
    let r3 = CreatePlanRequest {
        steps: vec![CandidatePlanStep {
            action_id: "broker.reset_master_flush_offset".to_owned(),
            descriptor_version: "1.0.0".to_owned(),
            resource: "broker/broker-a".to_owned(),
            parameters: json!({}),
            evidence_ids: vec![fixture.evidence_id],
        }],
        ..create
    };
    let CreatePlanResponse::ManualRunbook { runbook } = service
        .create_plan(&operator, &r3, CorrelationId::new())
        .await
        .expect("R3 runbook")
    else {
        panic!("R3 must never create an ActionPlan");
    };
    assert!(!runbook.execution_supported);
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_phase3_seed_is_idempotent_and_cryptographically_valid() {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 5).await.expect("repository");
    sqlx::raw_sql(include_str!("../../../../deploy/dev/postgres/phase3-seed.sql"))
        .execute(&repository.pool)
        .await
        .expect("phase3 seed");
    let tenant_id = "03000000-0000-4000-8000-000000000002"
        .parse::<TenantId>()
        .expect("tenant id");
    let cluster_id = "03000000-0000-4000-8000-000000000001"
        .parse::<ClusterId>()
        .expect("cluster id");
    let incident_id = "03000000-0000-4000-8000-000000000003"
        .parse::<IncidentId>()
        .expect("incident id");
    let diagnosis_id = "03000000-0000-4000-8000-000000000004"
        .parse::<DiagnosisRevisionId>()
        .expect("diagnosis id");
    let evidence_id = "03000000-0000-4000-8000-000000000008".parse().expect("evidence id");
    let auth = auth(tenant_id, cluster_id, "phase3-seed-check", &["operator"]);
    let evidence = repository.evidence(&auth, evidence_id).await.expect("seed evidence");
    evidence.verify_content_hash().expect("seed evidence hash");
    let diagnosis = repository
        .diagnosis_plan_context(&auth, cluster_id, incident_id, diagnosis_id)
        .await
        .expect("seed diagnosis");

    assert_eq!(diagnosis.status, "confirmed");
    assert!(diagnosis.execution_eligible);
    assert!(!diagnosis.partial);
    assert_eq!(diagnosis.evidence_ids, vec![evidence_id]);
    assert!(diagnosis.primary_model_invocation_id.is_some());
}

#[derive(Clone, Copy)]
struct Fixture {
    tenant_id: TenantId,
    cluster_id: ClusterId,
    incident_id: IncidentId,
    diagnosis_id: DiagnosisRevisionId,
    rules_only_diagnosis_id: DiagnosisRevisionId,
    evidence_id: rocketmq_sre_contracts::EvidenceId,
}

async fn seed_fixture(repository: &PostgresRepository) -> Fixture {
    let tenant_id = TenantId::new();
    let cluster_id = ClusterId::new();
    let incident_id = IncidentId::new();
    let diagnosis_id = DiagnosisRevisionId::new();
    let rules_only_diagnosis_id = DiagnosisRevisionId::new();
    let model_profile_id = Uuid::new_v4();
    let model_invocation_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO clusters (
            id, tenant_id, external_cluster_key, environment, region,
            rocketmq_version, deployment_mode, owner_name,
            requested_access_profile, effective_access_profile, onboarding_state
         ) VALUES ($1, $2, $3, 'test', 'local', 'test', 'docker',
                   'phase3-test', 'read_only', 'read_only', 'ready_read_only')",
    )
    .bind(cluster_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(format!("phase3-test-{cluster_id}"))
    .execute(&repository.pool)
    .await
    .expect("cluster");
    sqlx::query(
        "INSERT INTO sre_incidents (
            id, tenant_id, cluster_id, title, resource, symptom_family,
            fingerprint, status, workflow_checkpoint, created_by_subject,
            created_at, updated_at
         ) VALUES ($1, $2, $3, 'Phase 3 test', 'deployment/default/proxy',
                   'proxy_saturation', $4, 'diagnosing', '{}'::JSONB,
                   'phase3-test', NOW(), NOW())",
    )
    .bind(incident_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(format!("sha256:{}", "1".repeat(64)))
    .execute(&repository.pool)
    .await
    .expect("incident");
    for (id, revision) in [(diagnosis_id, 1_i32), (rules_only_diagnosis_id, 2_i32)] {
        sqlx::query(
            "INSERT INTO diagnosis_revisions (
                id, incident_id, revision, status, rule_result, hypotheses,
                evidence_ids, primary_model_invocation_id,
                execution_eligible, partial, created_at
             ) VALUES ($1, $2, $3, 'confirmed', '{}'::JSONB, '[]'::JSONB,
                       '{}', NULL, FALSE, FALSE, NOW())",
        )
        .bind(id.as_uuid())
        .bind(incident_id.as_uuid())
        .bind(revision)
        .execute(&repository.pool)
        .await
        .expect("diagnosis");
    }
    sqlx::query(
        "INSERT INTO model_profiles (
            id, tenant_id, profile_name, provider_family, protocol_family,
            model_family, model_name, model_revision, endpoint_instance,
            region, data_residency, data_classes, capabilities, priority,
            credential_ref, credential_owner, enabled, health, created_at, updated_at
         ) VALUES ($1, $2, $3, 'openai-compatible', 'openai-compatible',
                   'fixture-family', 'fixture-model', 'fixture-r1',
                   'fixture-endpoint', 'local', 'local', '[]'::JSONB,
                   '{\"structured_output\":true}'::JSONB, 100,
                   'test-reference', 'gateway', TRUE, 'healthy', NOW(), NOW())",
    )
    .bind(model_profile_id)
    .bind(tenant_id.as_uuid())
    .bind(format!("phase3-test-{model_profile_id}"))
    .execute(&repository.pool)
    .await
    .expect("model profile");
    sqlx::query(
        "INSERT INTO model_invocations (
            id, tenant_id, cluster_id, incident_id, diagnosis_revision_id,
            parent_invocation_id, purpose, requested_profile_id,
            actual_profile_id, provider_family, model_family, model_revision,
            endpoint_instance, fallback_chain, prompt_version, schema_version,
            rationale, started_at, completed_at
         ) VALUES ($1, $2, $3, $4, $5, NULL, 'primary_diagnosis', $6, $6,
                   'openai-compatible', 'fixture-family', 'fixture-r1',
                   'fixture-endpoint', '{}', 'phase3-test',
                   'rocketmq-sre.model.v1', 'test invocation', NOW(), NOW())",
    )
    .bind(model_invocation_id)
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(incident_id.as_uuid())
    .bind(diagnosis_id.as_uuid())
    .bind(model_profile_id)
    .execute(&repository.pool)
    .await
    .expect("model invocation");
    let auth = auth(tenant_id, cluster_id, "fixture", &["operator"]);
    let observed_at = Utc::now().with_nanosecond(0).expect("whole-second timestamp");
    let mut evidence = rocketmq_sre_contracts::EvidenceSnapshot::capture(
        EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id,
            cluster_id,
            source: "synthetic".to_owned(),
            resource: "deployment/default/proxy".to_owned(),
            time_range: TimeRange::new(observed_at - Duration::minutes(1), observed_at).expect("time range"),
        },
        SchemaVersion::new("rocketmq-sre.evidence", 1, 0),
        observed_at,
        EvidenceContent::Inline(json!({
            "desired_replicas": 2,
            "ready_replicas": 2,
            "proxy_error_ratio": 0.0
        })),
    )
    .expect("evidence");
    evidence.freshness_seconds = 600;
    evidence.coverage = CoverageStatus::Available;
    evidence.sensitivity = Sensitivity::Internal;
    evidence.exposure = EvidenceExposure::Synthetic;
    let evidence = repository
        .persist_evidence(&auth, &evidence, None, Some(incident_id), &evidence.content_hash)
        .await
        .expect("persist evidence");
    sqlx::query(
        "UPDATE diagnosis_revisions
         SET evidence_ids = ARRAY[$2]::UUID[],
             primary_model_invocation_id = $3,
             execution_eligible = TRUE
         WHERE id = $1",
    )
    .bind(diagnosis_id.as_uuid())
    .bind(evidence.evidence_id.as_uuid())
    .bind(model_invocation_id)
    .execute(&repository.pool)
    .await
    .expect("eligible diagnosis");
    sqlx::query(
        "UPDATE diagnosis_revisions
         SET evidence_ids = ARRAY[$2]::UUID[]
         WHERE id = $1",
    )
    .bind(rules_only_diagnosis_id.as_uuid())
    .bind(evidence.evidence_id.as_uuid())
    .execute(&repository.pool)
    .await
    .expect("rules-only diagnosis");
    Fixture {
        tenant_id,
        cluster_id,
        incident_id,
        diagnosis_id,
        rules_only_diagnosis_id,
        evidence_id: evidence.evidence_id,
    }
}

async fn insert_quarantine(repository: &PostgresRepository, fixture: &Fixture) -> ResourceQuarantineId {
    let id = ResourceQuarantineId::new();
    sqlx::query(
        "INSERT INTO resource_quarantines (
            id, tenant_id, cluster_id, resource_key, action_id,
            reason_code, evidence_ids, created_by, created_at
         ) VALUES ($1, $2, $3, 'deployment/default/proxy',
                   'proxy.scale_out_one.v1', 'ManualIsolation',
                   ARRAY[$4]::UUID[], 'phase3-test', NOW())",
    )
    .bind(id.as_uuid())
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .bind(fixture.evidence_id.as_uuid())
    .execute(&repository.pool)
    .await
    .expect("quarantine");
    id
}

async fn persist_changed_evidence(repository: &PostgresRepository, auth: &AuthContext, fixture: &Fixture) {
    let observed_at = repository
        .evidence(auth, fixture.evidence_id)
        .await
        .expect("bound evidence")
        .observed_at
        + Duration::seconds(1);
    let mut evidence = rocketmq_sre_contracts::EvidenceSnapshot::capture(
        EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: fixture.tenant_id,
            cluster_id: fixture.cluster_id,
            source: "synthetic".to_owned(),
            resource: "deployment/default/proxy".to_owned(),
            time_range: TimeRange::new(observed_at - Duration::minutes(1), observed_at).expect("time range"),
        },
        SchemaVersion::new("rocketmq-sre.evidence", 1, 0),
        observed_at,
        EvidenceContent::Inline(json!({
            "desired_replicas": 3,
            "ready_replicas": 2,
            "proxy_error_ratio": 0.01
        })),
    )
    .expect("changed evidence");
    evidence.freshness_seconds = 600;
    evidence.coverage = CoverageStatus::Available;
    evidence.sensitivity = Sensitivity::Internal;
    evidence.exposure = EvidenceExposure::Synthetic;
    let content_hash = evidence.content_hash.clone();
    let persisted = repository
        .persist_evidence(auth, &evidence, None, Some(fixture.incident_id), &content_hash)
        .await
        .expect("persist changed evidence");
    let latest = repository
        .latest_cluster_source_evidence(auth, fixture.cluster_id, "synthetic", "deployment/default/proxy")
        .await
        .expect("query latest changed evidence")
        .expect("latest changed evidence");
    assert_eq!(latest.evidence_id, persisted.evidence_id);
    latest.verify_content_hash().expect("latest changed evidence hash");
}

fn auth(tenant_id: TenantId, cluster_id: ClusterId, subject: &str, roles: &[&str]) -> AuthContext {
    AuthContext {
        tenant_id,
        subject: subject.to_owned(),
        clusters: BTreeSet::from([cluster_id]),
        roles: roles.iter().map(|role| (*role).to_owned()).collect(),
    }
}

fn error_code(error: &ControlPlaneError) -> &'static str {
    match error {
        ControlPlaneError::Validation { code, .. }
        | ControlPlaneError::Forbidden { code, .. }
        | ControlPlaneError::Conflict { code, .. } => code,
        _ => "unexpected",
    }
}
