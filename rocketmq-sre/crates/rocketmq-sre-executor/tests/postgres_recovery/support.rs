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

use std::str::FromStr;
use std::time::Duration;

use chrono::TimeDelta;
use chrono::Utc;
use rocketmq_sre_contracts::ActionPlan;
use rocketmq_sre_contracts::ActionPlanDraft;
use rocketmq_sre_contracts::ApprovalGrant;
use rocketmq_sre_contracts::ApprovalId;
use rocketmq_sre_contracts::AuditEvent;
use rocketmq_sre_contracts::AuditEventId;
use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CompensationMode;
use rocketmq_sre_contracts::CompensationSpec;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::DiagnosisRevisionId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ExecutionRequest;
use rocketmq_sre_contracts::ExecutionStepId;
use rocketmq_sre_contracts::ImpactScope;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::LeaseEpoch;
use rocketmq_sre_contracts::LeaseFenceGrant;
use rocketmq_sre_contracts::LeaseId;
use rocketmq_sre_contracts::ModelInvocationId;
use rocketmq_sre_contracts::PlanStatus;
use rocketmq_sre_contracts::PlanStep;
use rocketmq_sre_contracts::PlanStepId;
use rocketmq_sre_contracts::StepIntent;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::VerificationSpec;
use serde_json::json;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

pub(crate) struct Fixture {
    pub(crate) tenant_id: TenantId,
    pub(crate) cluster_id: ClusterId,
    pub(crate) primary_invocation_id: ModelInvocationId,
    pub(crate) critic_invocation_id: ModelInvocationId,
    pub(crate) plan: ActionPlan,
    pub(crate) request: ExecutionRequest,
}

pub(crate) async fn isolated_pool(database_url: &str, schema: &str) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect(database_url)
        .await
        .expect("Docker PostgreSQL");
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&pool)
        .await
        .expect("isolated schema");
    sqlx::query(&format!("SET search_path TO \"{schema}\""))
        .execute(&pool)
        .await
        .expect("isolated search path");
    pool
}

pub(crate) async fn cleanup_schema(pool: &PgPool, schema: &str) {
    sqlx::query("SET search_path TO public")
        .execute(pool)
        .await
        .expect("restore search path");
    sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(pool)
        .await
        .expect("drop isolated schema");
}

pub(crate) async fn assert_phase_three_tables(pool: &PgPool) {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM information_schema.tables
         WHERE table_schema = current_schema()
           AND table_name = ANY($1)",
    )
    .bind(vec![
        "action_plans",
        "policy_decisions",
        "approvals",
        "critic_reviews",
        "executions",
        "execution_steps",
        "audit_events",
        "resource_locks",
        "resource_quarantines",
        "executor_leases",
        "execution_agent_fences",
        "execution_agent_effects",
    ])
    .fetch_one(pool)
    .await
    .expect("phase three table count");
    assert_eq!(count, 12);
}

pub(crate) async fn seed_fixture(pool: &PgPool) -> Fixture {
    sqlx::raw_sql(include_str!("../../../../deploy/dev/postgres/phase3-seed.sql"))
        .execute(pool)
        .await
        .expect("development seed");
    let tenant_id = TenantId::from_uuid(fixture_uuid("03000000-0000-4000-8000-000000000002"));
    let cluster_id = ClusterId::from_uuid(fixture_uuid("03000000-0000-4000-8000-000000000001"));
    let incident_id = IncidentId::from_uuid(fixture_uuid("03000000-0000-4000-8000-000000000003"));
    let diagnosis_id = DiagnosisRevisionId::from_uuid(fixture_uuid("03000000-0000-4000-8000-000000000004"));
    let primary_invocation_id = ModelInvocationId::from_uuid(fixture_uuid("03000000-0000-4000-8000-000000000006"));
    let critic_invocation_id = ModelInvocationId::from_uuid(fixture_uuid("03000000-0000-4000-8000-000000000007"));
    let plan = action_plan(tenant_id, cluster_id, incident_id, diagnosis_id, primary_invocation_id);
    sqlx::query(
        "INSERT INTO action_plans (
            id, tenant_id, cluster_id, incident_id, diagnosis_revision_id,
            primary_model_invocation_id, version, plan_hash, evidence_hash,
            risk, status, request_snapshot, created_by, created_at,
            expires_at, submitted_at
         ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9,
            'r1', 'approved', $10, $11, $12,
            $13, $14
         )",
    )
    .bind(plan.id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(incident_id.as_uuid())
    .bind(diagnosis_id.as_uuid())
    .bind(primary_invocation_id.as_uuid())
    .bind(i32::try_from(plan.version).expect("plan version"))
    .bind(&plan.plan_hash)
    .bind(&plan.evidence_hash)
    .bind(serde_json::to_value(&plan).expect("plan snapshot"))
    .bind(&plan.created_by)
    .bind(plan.created_at)
    .bind(plan.expires_at)
    .bind(plan.submitted_at)
    .execute(pool)
    .await
    .expect("action plan");
    let request = execution_request(&plan, "execution-request-1");
    Fixture {
        tenant_id,
        cluster_id,
        primary_invocation_id,
        critic_invocation_id,
        plan,
        request,
    }
}

fn action_plan(
    tenant_id: TenantId,
    cluster_id: ClusterId,
    incident_id: IncidentId,
    diagnosis_id: DiagnosisRevisionId,
    primary_invocation_id: ModelInvocationId,
) -> ActionPlan {
    let now = Utc::now();
    let draft = ActionPlanDraft {
        id: rocketmq_sre_contracts::ActionPlanId::new(),
        tenant_id,
        cluster_id,
        incident_id,
        diagnosis_revision: diagnosis_id,
        primary_model_invocation_id: primary_invocation_id,
        diagnosis_execution_eligible: true,
        version: 1,
        created_by: "operator-a".to_owned(),
        created_at: now - TimeDelta::minutes(1),
        expires_at: now + TimeDelta::hours(1),
        evidence_hash: format!("sha256:{}", "a".repeat(64)),
        steps: vec![PlanStep {
            id: PlanStepId::new(),
            sequence: 1,
            action: ExecutionAction::ProxyScaleOutOne,
            descriptor_version: "1.0.0".to_owned(),
            resource: "deployment/default/proxy".to_owned(),
            parameters: json!({
                "namespace": "default",
                "workload": "proxy",
                "expected_replicas": 2
            }),
            evidence_ids: vec![EvidenceId::new()],
            precondition_hash: format!("sha256:{}", "b".repeat(64)),
            max_impact: ImpactScope::OneReplica,
            verification: VerificationSpec {
                resource_conditions: vec!["desired_replicas_plus_one".to_owned(), "new_replica_ready".to_owned()],
                technical_slis: vec!["proxy_error_ratio".to_owned(), "proxy_p99_latency".to_owned()],
                stable_window_seconds: 120,
                max_wait_seconds: 900,
            },
            compensation: CompensationSpec {
                mode: CompensationMode::Automatic,
                required_before_fields: vec!["expected_replicas".to_owned()],
                timeout_seconds: 600,
            },
        }],
    };
    let mut plan = ActionPlan::seal(draft).expect("valid plan");
    plan.status = PlanStatus::Approved;
    plan.submitted_at = Some(now - TimeDelta::seconds(30));
    plan
}

pub(crate) fn execution_request(plan: &ActionPlan, key: &str) -> ExecutionRequest {
    let now = Utc::now();
    let precondition_hash = plan.compute_precondition_hash().expect("precondition hash");
    ExecutionRequest {
        schema_version: ExecutionRequest::SCHEMA_VERSION.to_owned(),
        id: ExecutionId::new(),
        tenant_id: plan.tenant_id,
        cluster_id: plan.cluster_id,
        correlation_id: CorrelationId::new(),
        plan: plan.clone(),
        approvals: vec![ApprovalGrant {
            issuer: "control-plane".to_owned(),
            audience: "rocketmq-sre-executor".to_owned(),
            approval_id: ApprovalId::new(),
            plan_id: plan.id,
            plan_hash: plan.plan_hash.clone(),
            precondition_hash,
            tenant_id: plan.tenant_id,
            cluster_id: plan.cluster_id,
            approver_subject: "approver-a".to_owned(),
            issued_at: now - TimeDelta::seconds(10),
            expires_at: now + TimeDelta::minutes(30),
            nonce: format!("approval-{key}"),
            signature: "fixture-signature".to_owned(),
        }],
        requested_by: "operator-a".to_owned(),
        idempotency_key: key.to_owned(),
        issuer: "control-plane".to_owned(),
        audience: "rocketmq-sre-executor".to_owned(),
        issued_at: now - TimeDelta::seconds(5),
        expires_at: now + TimeDelta::minutes(10),
        nonce: format!("request-{key}"),
        signature: "fixture-signature".to_owned(),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the test helper mirrors the complete fenced intent identity"
)]
pub(crate) fn step_intent(
    fixture: &Fixture,
    lease_id: LeaseId,
    epoch: LeaseEpoch,
    owner: &str,
    expires_at: chrono::DateTime<Utc>,
    idempotency_key: &str,
    intended_at: chrono::DateTime<Utc>,
) -> StepIntent {
    StepIntent {
        execution_id: fixture.request.id,
        step_id: ExecutionStepId::new(),
        plan_hash: fixture.plan.plan_hash.clone(),
        step: fixture.plan.steps[0].clone(),
        attempt: 1,
        idempotency_key: idempotency_key.to_owned(),
        fence_grant: LeaseFenceGrant {
            lease_id,
            owner: owner.to_owned(),
            cluster_id: fixture.cluster_id,
            epoch,
            audience: "execution-agent".to_owned(),
            issued_at: intended_at - TimeDelta::seconds(1),
            expires_at,
            nonce: format!("grant-{idempotency_key}"),
            signature: "fixture-signature".to_owned(),
        },
        intended_at,
        compensation: false,
    }
}

pub(crate) fn audit(
    fixture: &Fixture,
    event_kind: AuditEventKind,
    reason_code: &str,
    occurred_at: chrono::DateTime<Utc>,
) -> AuditEvent {
    AuditEvent {
        id: AuditEventId::new(),
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        correlation_id: fixture.request.correlation_id,
        event_kind,
        actor_subject: "executor-service".to_owned(),
        actor_role: "executor_service".to_owned(),
        resource_kind: "execution".to_owned(),
        resource_id: fixture.request.id.to_string(),
        reason_code: reason_code.to_owned(),
        details: json!({"bounded": true}),
        occurred_at,
    }
}

pub(crate) async fn assert_critic_review_is_immutable(pool: &PgPool, fixture: &Fixture) {
    let review_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO critic_reviews (
            id, plan_id, plan_hash, primary_invocation_id,
            critic_invocation_id, primary_model_family, critic_model_family,
            critic_provider, critic_profile, critic_model_revision,
            endpoint_instance, conclusion, status, review_hash,
            review_snapshot, created_at
         ) VALUES (
            $1, $2, $3, $4,
            $5, 'fixture-primary', 'fixture-critic',
            'fixture-provider', 'fixture-profile', 'fixture-r1',
            'fixture-endpoint', 'accept', 'valid', $6,
            '{}'::JSONB, $7
         )",
    )
    .bind(review_id)
    .bind(fixture.plan.id.as_uuid())
    .bind(&fixture.plan.plan_hash)
    .bind(fixture.primary_invocation_id.as_uuid())
    .bind(fixture.critic_invocation_id.as_uuid())
    .bind(format!("sha256:{}", "c".repeat(64)))
    .bind(Utc::now())
    .execute(pool)
    .await
    .expect("critic review");
    assert!(
        sqlx::query("UPDATE critic_reviews SET conclusion = 'reject' WHERE id = $1")
            .bind(review_id)
            .execute(pool)
            .await
            .is_err()
    );
}

fn fixture_uuid(value: &str) -> Uuid {
    Uuid::from_str(value).expect("static fixture UUID")
}
