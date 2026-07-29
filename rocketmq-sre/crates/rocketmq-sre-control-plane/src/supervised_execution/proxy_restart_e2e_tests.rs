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
use std::time::Duration as StdDuration;

use chrono::Duration;
use chrono::Timelike;
use chrono::Utc;
use reqwest::Url;
use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::DiagnosisRevisionId;
use rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceExposure;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ExecutionState;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::PlanStepId;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::SchemaVersion;
use rocketmq_sre_contracts::Sensitivity;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::TimeRange;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use super::ExecutorSubmissionClient;
use super::model::ApprovalDecisionRequest;
use super::model::CandidatePlanStep;
use super::model::CreatePlanRequest;
use super::model::CreatePlanResponse;
use super::model::SubmitExecutionRequest;
use super::service::SupervisedExecutionService;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::models::ModelGatewayService;
use crate::repository::ClusterRepository;
use crate::workflow::WorkflowEventBus;
use crate::workflow::WorkflowService;

const DEFAULT_TENANT_ID: &str = "00000000-0000-4000-8000-000000000002";
const DEFAULT_CLUSTER_ID: &str = "00000000-0000-4000-8000-000000000001";

#[tokio::test]
#[ignore = "requires the live Kind PostgreSQL, Executor, Execution Agent, and dual-replica Proxy fixture"]
async fn real_kind_supervised_proxy_restart_reaches_verified_success() {
    let Some(database_url) = required_env("ROCKETMQ_SRE_PHASE3_DATABASE_URL") else {
        return;
    };
    let Some(executor_url) = required_env("ROCKETMQ_SRE_PHASE3_EXECUTOR_URL") else {
        return;
    };
    let Some(agent_url) = required_env("ROCKETMQ_SRE_PHASE3_AGENT_URL") else {
        return;
    };
    let Some(workload_token) = required_env("ROCKETMQ_SRE_PHASE3_WORKLOAD_TOKEN") else {
        return;
    };
    let Some(signing_key) = required_env("ROCKETMQ_SRE_PHASE3_SIGNING_KEY") else {
        return;
    };
    let Some(target_pod) = required_env("ROCKETMQ_SRE_PHASE3_PROXY_POD") else {
        return;
    };
    let Some(expected_uid) = required_env("ROCKETMQ_SRE_PHASE3_PROXY_UID") else {
        return;
    };
    let tenant_id = optional_id("ROCKETMQ_SRE_PHASE3_TENANT_ID", DEFAULT_TENANT_ID);
    let cluster_id = optional_id("ROCKETMQ_SRE_PHASE3_CLUSTER_ID", DEFAULT_CLUSTER_ID);
    let target = format!("pod/rocketmq-system/{target_pod}");
    let parameters = json!({
        "namespace": "rocketmq-system",
        "pod": target_pod,
        "expected_uid": expected_uid,
    });
    let agent_state = fetch_agent_state(
        &agent_url,
        &workload_token,
        tenant_id,
        cluster_id,
        &target,
        parameters.clone(),
    )
    .await;
    assert!(agent_state.ready, "live Proxy restart precheck must be ready");

    let repository = PostgresRepository::connect(&database_url, 5)
        .await
        .expect("Kind PostgreSQL repository");
    let mut fixture = seed_execution_fixture(&repository, tenant_id, cluster_id, &target, &agent_state).await;
    seed_complete_slo_evidence(&repository, &fixture).await;
    let agent_state = fetch_agent_state(
        &agent_url,
        &workload_token,
        tenant_id,
        cluster_id,
        &target,
        parameters.clone(),
    )
    .await;
    assert!(
        agent_state.ready,
        "live Proxy restart precheck must remain ready after SLI refresh"
    );
    fixture.agent_evidence_id = persist_agent_evidence(&repository, &fixture, &target, &agent_state).await;

    let workflow = WorkflowService::new(repository.clone(), WorkflowEventBus::new(64));
    let model_gateway = ModelGatewayService::disabled(repository.clone());
    let executor = ExecutorSubmissionClient::http(
        executor_url.parse::<Url>().expect("Executor URL"),
        workload_token,
        StdDuration::from_secs(360),
        true,
    )
    .expect("Executor client");
    let service =
        SupervisedExecutionService::new_with_executor(repository, workflow, signing_key, model_gateway, executor)
            .expect("supervised execution service");
    let operator = auth(tenant_id, cluster_id, "phase3-kind-operator", &["operator"]);
    let approver = auth(tenant_id, cluster_id, "phase3-kind-approver", &["approver"]);
    let created = service
        .create_plan(
            &operator,
            &CreatePlanRequest {
                cluster_id,
                incident_id: fixture.incident_id,
                diagnosis_revision_id: fixture.diagnosis_id,
                expires_at: Some(Utc::now() + Duration::minutes(25)),
                steps: vec![CandidatePlanStep {
                    action_id: ExecutionAction::ProxyRestartOne.id().to_owned(),
                    descriptor_version: "1.0.0".to_owned(),
                    resource: target,
                    parameters,
                    evidence_ids: vec![fixture.agent_evidence_id],
                }],
            },
            CorrelationId::new(),
        )
        .await
        .expect("create Proxy restart plan");
    let CreatePlanResponse::ActionPlan { plan, .. } = created else {
        panic!("an enabled R1 Proxy restart must create an ActionPlan");
    };
    assert_eq!(plan.steps[0].precondition_hash, agent_state.precondition_hash);
    let precondition_hash = plan.compute_precondition_hash().expect("plan precondition hash");
    service
        .approve(
            &approver,
            plan.id,
            &ApprovalDecisionRequest {
                plan_hash: plan.plan_hash.clone(),
                precondition_hash: precondition_hash.clone(),
                reason: "Kind dual-replica drain, UID, probe, and SLO evidence reviewed".to_owned(),
                validity_seconds: Some(1_500),
            },
            CorrelationId::new(),
        )
        .await
        .expect("independent human approval");
    let submitted = service
        .submit_execution(
            &operator,
            &SubmitExecutionRequest {
                plan_id: plan.id,
                plan_hash: plan.plan_hash,
                precondition_hash,
                idempotency_key: format!("phase3-proxy-restart-{}", Uuid::new_v4()),
            },
            CorrelationId::new(),
        )
        .await
        .expect("execute supervised Proxy restart");
    assert_eq!(submitted.state, ExecutionState::Succeeded);
}

async fn fetch_agent_state(
    base_url: &str,
    token: &str,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    target: &str,
    parameters: serde_json::Value,
) -> AgentReadResult {
    let url = base_url
        .parse::<Url>()
        .expect("Agent URL")
        .join("/internal/v1/execution-agent/precheck")
        .expect("Agent precheck URL");
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(StdDuration::from_secs(45))
        .build()
        .expect("Agent HTTP client")
        .post(url)
        .bearer_auth(token)
        .header("x-forwarded-client-cert", "URI=spiffe://rocketmq-sre/executor")
        .json(&AgentReadRequest {
            schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
            tenant_id,
            cluster_id,
            execution_id: ExecutionId::new(),
            plan_step_id: PlanStepId::new(),
            action: ExecutionAction::ProxyRestartOne,
            descriptor_version: "1.0.0".to_owned(),
            target: target.to_owned(),
            parameters,
        })
        .send()
        .await
        .expect("Agent precheck response")
        .error_for_status()
        .expect("successful Agent precheck")
        .json()
        .await
        .expect("typed Agent precheck")
}

#[derive(Clone, Copy)]
struct ExecutionFixture {
    tenant_id: TenantId,
    cluster_id: ClusterId,
    incident_id: IncidentId,
    diagnosis_id: DiagnosisRevisionId,
    model_invocation_id: Uuid,
    agent_evidence_id: EvidenceId,
}

async fn seed_execution_fixture(
    repository: &PostgresRepository,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    target: &str,
    agent_state: &AgentReadResult,
) -> ExecutionFixture {
    repository.get(cluster_id).await.expect("onboarded Kind cluster");
    let incident_id = IncidentId::new();
    let diagnosis_id = DiagnosisRevisionId::new();
    let model_profile_id = Uuid::new_v4();
    let model_invocation_id = Uuid::new_v4();
    insert_diagnosis_records(
        &repository.pool,
        tenant_id,
        cluster_id,
        incident_id,
        diagnosis_id,
        model_profile_id,
        model_invocation_id,
        target,
    )
    .await;
    let mut fixture = ExecutionFixture {
        tenant_id,
        cluster_id,
        incident_id,
        diagnosis_id,
        model_invocation_id,
        agent_evidence_id: EvidenceId::new(),
    };
    fixture.agent_evidence_id = persist_agent_evidence(repository, &fixture, target, agent_state).await;
    fixture
}

async fn persist_agent_evidence(
    repository: &PostgresRepository,
    fixture: &ExecutionFixture,
    target: &str,
    agent_state: &AgentReadResult,
) -> EvidenceId {
    let auth = auth(
        fixture.tenant_id,
        fixture.cluster_id,
        "phase3-kind-fixture",
        &["operator"],
    );
    let observed_at = agent_state
        .observed_at
        .with_nanosecond(0)
        .expect("whole-second Agent timestamp");
    let mut evidence = rocketmq_sre_contracts::EvidenceSnapshot::capture(
        EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: fixture.tenant_id,
            cluster_id: fixture.cluster_id,
            source: "execution-agent".to_owned(),
            resource: target.to_owned(),
            time_range: TimeRange::new(observed_at, observed_at).expect("Agent Evidence time range"),
        },
        SchemaVersion::new("rocketmq-sre.evidence", 1, 0),
        observed_at,
        EvidenceContent::Inline(serde_json::to_value(agent_state).expect("Agent Evidence content")),
    )
    .expect("Agent Evidence");
    evidence.freshness_seconds = 1_800;
    evidence.coverage = CoverageStatus::Available;
    evidence.sensitivity = Sensitivity::Internal;
    evidence.exposure = EvidenceExposure::Synthetic;
    let content_hash = evidence.content_hash.clone();
    let evidence = repository
        .persist_evidence(&auth, &evidence, None, Some(fixture.incident_id), &content_hash)
        .await
        .expect("persist Agent Evidence");
    sqlx::query(
        "UPDATE diagnosis_revisions
         SET evidence_ids = ARRAY[$2]::UUID[],
             primary_model_invocation_id = $3,
             execution_eligible = TRUE
         WHERE id = $1",
    )
    .bind(fixture.diagnosis_id.as_uuid())
    .bind(evidence.evidence_id.as_uuid())
    .bind(fixture.model_invocation_id)
    .execute(&repository.pool)
    .await
    .expect("mark diagnosis execution eligible");
    evidence.evidence_id
}

#[allow(
    clippy::too_many_arguments,
    reason = "the test fixture binds all persisted foreign keys explicitly"
)]
async fn insert_diagnosis_records(
    pool: &PgPool,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    incident_id: IncidentId,
    diagnosis_id: DiagnosisRevisionId,
    model_profile_id: Uuid,
    model_invocation_id: Uuid,
    target: &str,
) {
    let fingerprint = rocketmq_sre_contracts::canonical_sha256(&incident_id).expect("canonical incident fingerprint");
    sqlx::query(
        "INSERT INTO sre_incidents (
            id, tenant_id, cluster_id, title, resource, symptom_family,
            fingerprint, status, workflow_checkpoint, created_by_subject,
            created_at, updated_at
         ) VALUES ($1, $2, $3, 'Phase 3 Proxy restart E2E', $4,
                   'proxy_restart_validation', $5, 'diagnosing', '{}'::JSONB,
                   'phase3-kind-fixture', NOW(), NOW())",
    )
    .bind(incident_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(target)
    .bind(fingerprint)
    .execute(pool)
    .await
    .expect("incident fixture");
    sqlx::query(
        "INSERT INTO diagnosis_revisions (
            id, incident_id, revision, status, rule_result, hypotheses,
            evidence_ids, primary_model_invocation_id,
            execution_eligible, partial, created_at
         ) VALUES ($1, $2, 1, 'confirmed', '{}'::JSONB, '[]'::JSONB,
                   '{}', NULL, FALSE, FALSE, NOW())",
    )
    .bind(diagnosis_id.as_uuid())
    .bind(incident_id.as_uuid())
    .execute(pool)
    .await
    .expect("diagnosis fixture");
    sqlx::query(
        "INSERT INTO model_profiles (
            id, tenant_id, profile_name, provider_family, protocol_family,
            model_family, model_name, model_revision, endpoint_instance,
            region, data_residency, data_classes, capabilities, priority,
            credential_ref, credential_owner, enabled, health, created_at, updated_at
         ) VALUES ($1, $2, $3, 'openai-compatible', 'openai-compatible',
                   'phase3-kind-family', 'phase3-kind-model', 'phase3-kind-r1',
                   'phase3-kind-endpoint', 'kind', 'local', '[]'::JSONB,
                   '{\"structured_output\":true}'::JSONB, 100,
                   'test-reference', 'gateway', TRUE, 'healthy', NOW(), NOW())",
    )
    .bind(model_profile_id)
    .bind(tenant_id.as_uuid())
    .bind(format!("phase3-kind-{model_profile_id}"))
    .execute(pool)
    .await
    .expect("model profile fixture");
    sqlx::query(
        "INSERT INTO model_invocations (
            id, tenant_id, cluster_id, incident_id, diagnosis_revision_id,
            parent_invocation_id, purpose, requested_profile_id,
            actual_profile_id, provider_family, model_family, model_revision,
            endpoint_instance, fallback_chain, prompt_version, schema_version,
            rationale, started_at, completed_at
         ) VALUES ($1, $2, $3, $4, $5, NULL, 'primary_diagnosis', $6, $6,
                   'openai-compatible', 'phase3-kind-family', 'phase3-kind-r1',
                   'phase3-kind-endpoint', '{}', 'phase3-kind',
                   'rocketmq-sre.model.v1', 'Kind supervised restart fixture',
                   NOW(), NOW())",
    )
    .bind(model_invocation_id)
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(incident_id.as_uuid())
    .bind(diagnosis_id.as_uuid())
    .bind(model_profile_id)
    .execute(pool)
    .await
    .expect("model invocation fixture");
}

async fn seed_complete_slo_evidence(repository: &PostgresRepository, fixture: &ExecutionFixture) {
    let observed_at = Utc::now().with_nanosecond(0).expect("whole-second SLO timestamp");
    let sample_at = observed_at + Duration::seconds(55);
    let mut series = Vec::new();
    for (sli, dimension) in [("delivery_ratio", "traffic"), ("proxy_connection", "routing_proxy")] {
        for window in ["fast", "medium", "slow"] {
            for role in ["short", "long"] {
                series.push(json!({
                    "labels": {
                        "sli": sli,
                        "dimension": dimension,
                        "window_pair": window,
                        "window_role": role,
                    },
                    "samples": [{"observed_at": sample_at, "value": 0.0}],
                }));
            }
        }
    }
    let mut evidence = rocketmq_sre_contracts::EvidenceSnapshot::capture(
        EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: fixture.tenant_id,
            cluster_id: fixture.cluster_id,
            source: "prometheus".to_owned(),
            resource: "instant/rocketmq_sre_sli_burn_rate".to_owned(),
            time_range: TimeRange::new(observed_at, observed_at).expect("SLO Evidence time range"),
        },
        SchemaVersion::new("rocketmq-sre.evidence", 1, 0),
        observed_at,
        EvidenceContent::Inline(json!({
            "schema_version": "rocketmq.prometheus-evidence.v1",
            "query_kind": "instant",
            "metric": "rocketmq_sre_sli_burn_rate",
            "series": series,
        })),
    )
    .expect("SLO Evidence");
    evidence.freshness_seconds = 1_800;
    evidence.coverage = CoverageStatus::Available;
    evidence.sensitivity = Sensitivity::Internal;
    evidence.exposure = EvidenceExposure::Synthetic;
    let auth = auth(
        fixture.tenant_id,
        fixture.cluster_id,
        "phase3-kind-slo-fixture",
        &["diagnose"],
    );
    let content_hash = evidence.content_hash.clone();
    repository
        .persist_evidence(&auth, &evidence, None, Some(fixture.incident_id), &content_hash)
        .await
        .expect("persist complete SLO Evidence");
}

fn auth(tenant_id: TenantId, cluster_id: ClusterId, subject: &str, roles: &[&str]) -> AuthContext {
    AuthContext {
        tenant_id,
        subject: subject.to_owned(),
        clusters: BTreeSet::from([cluster_id]),
        roles: roles.iter().map(|role| (*role).to_owned()).collect(),
    }
}

fn required_env(name: &'static str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn optional_id<T>(name: &'static str, default: &'static str) -> T
where
    T: std::str::FromStr,
    T::Err: std::fmt::Debug,
{
    std::env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .parse()
        .expect("valid scoped identifier")
}
