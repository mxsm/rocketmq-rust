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

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration as StdDuration;

use chrono::Duration;
use chrono::Utc;
use reqwest::Url;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::CriticGateState;
use rocketmq_sre_contracts::CriticReviewStatus;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionState;
use rocketmq_sre_contracts::PlanStatus;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_model_gateway::AsyncModelTransport;
use rocketmq_sre_model_gateway::ProviderError;
use rocketmq_sre_model_gateway::TransportFuture;
use rocketmq_sre_model_gateway::TransportRequest;
use rocketmq_sre_model_gateway::TransportResponse;
use serde_json::json;
use uuid::Uuid;

use super::ExecutorSubmissionClient;
use super::model::ApprovalDecisionRequest;
use super::model::CandidatePlanStep;
use super::model::CreatePlanRequest;
use super::model::CreatePlanResponse;
use super::model::CriticReviewRequest;
use super::model::SubmitExecutionRequest;
use super::proxy_restart_e2e_tests::auth;
use super::proxy_restart_e2e_tests::fetch_agent_state;
use super::proxy_restart_e2e_tests::persist_agent_evidence;
use super::proxy_restart_e2e_tests::seed_complete_slo_evidence;
use super::proxy_restart_e2e_tests::seed_execution_fixture;
use super::service::SupervisedExecutionService;
use crate::PostgresRepository;
use crate::models::ModelGatewayService;
use crate::workflow::WorkflowEventBus;
use crate::workflow::WorkflowService;

const DEFAULT_TENANT_ID: &str = "00000000-0000-4000-8000-000000000002";
const DEFAULT_CLUSTER_ID: &str = "00000000-0000-4000-8000-000000000001";

#[tokio::test]
#[ignore = "requires live Kind PostgreSQL, Executor, Agent, credential fixtures, and authenticated Broker"]
async fn real_kind_supervised_credential_overlap_passes_critic_and_verification() {
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
    let tenant_id = optional_id("ROCKETMQ_SRE_PHASE3_TENANT_ID", DEFAULT_TENANT_ID);
    let cluster_id = optional_id("ROCKETMQ_SRE_PHASE3_CLUSTER_ID", DEFAULT_CLUSTER_ID);
    let target = "credential-set/broker-admin".to_owned();
    let parameters = json!({
        "credential_set": "broker-admin",
        "active_version": "v1",
        "candidate_version": "v2",
        "candidate_secret_ref": "kubernetes://rocketmq-sre/broker-admin-credential-v2",
        "overlap_seconds": 300,
        "validation_probe_topic": "SRE_PROBE_CREDENTIAL_ROTATION",
    });
    let baseline = fetch_agent_state(
        &agent_url,
        &workload_token,
        tenant_id,
        cluster_id,
        ExecutionAction::SecurityCredentialRotateOverlap,
        &target,
        parameters.clone(),
    )
    .await;
    assert!(baseline.ready, "live credential baseline must be ready");
    assert_eq!(baseline.resource_conditions.get("candidate_active"), Some(&false));

    let repository = PostgresRepository::connect(&database_url, 5)
        .await
        .expect("Kind PostgreSQL repository");
    let mut fixture = seed_execution_fixture(&repository, tenant_id, cluster_id, &target, &baseline).await;
    seed_complete_slo_evidence(&repository, &fixture).await;
    let refreshed = fetch_agent_state(
        &agent_url,
        &workload_token,
        tenant_id,
        cluster_id,
        ExecutionAction::SecurityCredentialRotateOverlap,
        &target,
        parameters.clone(),
    )
    .await;
    assert!(refreshed.ready, "credential precheck must remain ready");
    fixture.agent_evidence_id = persist_agent_evidence(&repository, &fixture, &target, &refreshed).await;

    let critic_profile = critic_profile();
    let critic_family = critic_profile.model_family.clone();
    let model_gateway = ModelGatewayService::for_tests(
        repository.clone(),
        vec![critic_profile],
        Arc::new(ScriptedTransport::new([Ok(valid_critic_response(
            fixture.agent_evidence_id,
        ))])),
    );
    let workflow = WorkflowService::new(repository.clone(), WorkflowEventBus::new(64));
    let executor = ExecutorSubmissionClient::http(
        executor_url.parse::<Url>().expect("Executor URL"),
        workload_token,
        StdDuration::from_secs(900),
        true,
    )
    .expect("Executor client");
    let service = SupervisedExecutionService::new_with_executor(
        repository.clone(),
        workflow,
        signing_key,
        model_gateway,
        executor,
    )
    .expect("supervised execution service");
    let operator = auth(tenant_id, cluster_id, "phase3-credential-operator", &["operator"]);
    let approver = auth(tenant_id, cluster_id, "phase3-credential-approver", &["approver"]);
    let created = service
        .create_plan(
            &operator,
            &CreatePlanRequest {
                cluster_id,
                incident_id: fixture.incident_id,
                diagnosis_revision_id: fixture.diagnosis_id,
                expires_at: Some(Utc::now() + Duration::minutes(30)),
                steps: vec![CandidatePlanStep {
                    action_id: ExecutionAction::SecurityCredentialRotateOverlap.id().to_owned(),
                    descriptor_version: "1.0.0".to_owned(),
                    resource: target.clone(),
                    parameters: parameters.clone(),
                    evidence_ids: vec![fixture.agent_evidence_id],
                }],
            },
            CorrelationId::new(),
        )
        .await
        .expect("create credential rotation plan");
    let CreatePlanResponse::ActionPlan { plan, .. } = created else {
        panic!("enabled credential rotation must create an ActionPlan");
    };
    assert_eq!(plan.status, PlanStatus::NeedsCritic);
    assert_eq!(plan.steps[0].precondition_hash, refreshed.precondition_hash);

    let reviewed = service
        .review_with_critic(
            &operator,
            plan.id,
            &CriticReviewRequest {
                plan_hash: plan.plan_hash.clone(),
            },
            CorrelationId::new(),
        )
        .await
        .expect("heterogeneous credential rotation Critic");
    assert_eq!(reviewed.plan.status, PlanStatus::ReadyForApproval);
    assert_eq!(reviewed.review.status, CriticReviewStatus::Valid);
    assert_eq!(reviewed.critic_state, CriticGateState::Accepted);
    assert_eq!(
        reviewed.review.critic_model_family.as_deref(),
        Some(critic_family.as_str())
    );
    let precondition_hash = reviewed
        .plan
        .compute_precondition_hash()
        .expect("reviewed plan precondition hash");
    service
        .approve(
            &approver,
            reviewed.plan.id,
            &ApprovalDecisionRequest {
                plan_hash: reviewed.plan.plan_hash.clone(),
                precondition_hash: precondition_hash.clone(),
                reason: "Independent review accepted the bounded credential overlap and rollback".to_owned(),
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
                plan_id: reviewed.plan.id,
                plan_hash: reviewed.plan.plan_hash,
                precondition_hash,
                idempotency_key: format!("phase3-credential-overlap-{}", Uuid::new_v4()),
            },
            CorrelationId::new(),
        )
        .await
        .expect("execute supervised credential overlap");
    assert_eq!(submitted.state, ExecutionState::Succeeded);

    let applied = fetch_agent_state(
        &agent_url,
        &workload_token,
        tenant_id,
        cluster_id,
        ExecutionAction::SecurityCredentialRotateOverlap,
        &target,
        parameters,
    )
    .await;
    assert!(applied.ready);
    assert_eq!(applied.resource_conditions.get("candidate_active"), Some(&true));
    assert_eq!(applied.resource_conditions.get("previous_retiring"), Some(&true));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM execution_agent_credential_rotation_before_states
             WHERE execution_id = $1",
        )
        .bind(submitted.execution.id.as_uuid())
        .fetch_one(&repository.pool)
        .await
        .expect("credential rotation before-state count"),
        1
    );
}

struct ScriptedTransport {
    responses: Mutex<VecDeque<Result<TransportResponse, ProviderError>>>,
}

impl ScriptedTransport {
    fn new(responses: impl IntoIterator<Item = Result<TransportResponse, ProviderError>>) -> Self {
        Self {
            responses: Mutex::new(responses.into_iter().collect()),
        }
    }
}

impl AsyncModelTransport for ScriptedTransport {
    fn invoke(&self, _request: TransportRequest) -> TransportFuture<'_> {
        let response = self
            .responses
            .lock()
            .expect("credential Critic transport lock")
            .pop_front()
            .expect("credential Critic response");
        Box::pin(async move { response })
    }
}

fn critic_profile() -> rocketmq_sre_model_gateway::ProviderProfile {
    let mut profile = rocketmq_sre_model_gateway::builtin_provider_profiles()
        .into_iter()
        .find(|profile| profile.id == "kimi-moonshot")
        .expect("Kimi Critic profile");
    profile.credential_ref = None;
    profile.model_revision = "phase3-credential-critic-r1".to_owned();
    profile
}

fn valid_critic_response(evidence_id: rocketmq_sre_contracts::EvidenceId) -> TransportResponse {
    TransportResponse {
        status: 200,
        body: json!({
            "id": "chatcmpl-phase3-credential-critic",
            "model": "phase3-credential-critic",
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": serde_json::to_string(&json!({
                        "conclusion": "accept",
                        "cited_evidence_ids": [evidence_id],
                        "counter_evidence_ids": [],
                        "parameter_ranges_valid": true,
                        "missing_preconditions": [],
                        "impact_scope_valid": true,
                        "rollback_available": true,
                        "findings": [],
                        "rationale": "The candidate probe, overlap bound, exact selector, and durable rollback are valid."
                    })).expect("credential Critic payload")
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 20,
                "completion_tokens": 10,
                "total_tokens": 30
            }
        }),
    }
}

fn required_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn optional_id<T>(name: &str, default: &str) -> T
where
    T: std::str::FromStr,
    T::Err: std::fmt::Debug,
{
    std::env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .parse()
        .unwrap_or_else(|error| panic!("{name} must contain a valid identifier: {error:?}"))
}
