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

use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::ActionPlan;
use rocketmq_sre_contracts::CriticGateState;
use rocketmq_sre_contracts::CriticReviewStatus;
use rocketmq_sre_contracts::PlanStatus;
use rocketmq_sre_model_gateway::AsyncModelTransport;
use rocketmq_sre_model_gateway::ProviderError;
use rocketmq_sre_model_gateway::TransportFuture;
use rocketmq_sre_model_gateway::TransportRequest;
use rocketmq_sre_model_gateway::TransportResponse;
use serde_json::json;

use super::model::ApprovalDecisionRequest;
use super::model::CandidatePlanStep;
use super::model::CreatePlanRequest;
use super::model::CreatePlanResponse;
use super::model::CriticReviewRequest;
use super::service::SupervisedExecutionService;
use super::service_tests::auth;
use super::service_tests::seed_fixture;
use crate::PostgresRepository;
use crate::models::ModelGatewayService;
use crate::workflow::WorkflowEventBus;
use crate::workflow::WorkflowService;

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
            .expect("scripted Critic transport lock")
            .pop_front()
            .expect("scripted Critic response");
        Box::pin(async move { response })
    }
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_r2_critic_is_heterogeneous_durable_and_required_for_approval() {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 5).await.expect("repository");
    let fixture = seed_fixture(&repository).await;
    let operator = auth(fixture.tenant_id, fixture.cluster_id, "operator-critic", &["operator"]);
    let approver = auth(fixture.tenant_id, fixture.cluster_id, "approver-critic", &["approver"]);

    let mut deepseek = profile("deepseek", 1);
    deepseek.model_revision = "deepseek-actual-r7".to_owned();
    let mut kimi = profile("kimi-moonshot", 2);
    kimi.model_revision = "kimi-actual-r9".to_owned();
    let response = valid_openai_critic_response(fixture.evidence_id, "moonshot-actual");
    let transport = Arc::new(ScriptedTransport::new([
        Ok(TransportResponse {
            status: 503,
            body: json!({"error": {"message": "temporarily unavailable"}}),
        }),
        Ok(response),
    ]));
    let model_gateway =
        ModelGatewayService::for_tests(repository.clone(), vec![deepseek.clone(), kimi.clone()], transport);
    let workflow = WorkflowService::new(repository.clone(), WorkflowEventBus::new(64));
    let service = SupervisedExecutionService::new_with_clock_and_model(
        repository.clone(),
        workflow,
        "phase3-critic-test-signing-key",
        model_gateway,
        Arc::new(Utc::now),
    )
    .expect("service");

    let topic_plan = create_r2_plan(
        &service,
        &operator,
        &fixture,
        "topic.config.patch_allowlisted.v1",
        "topic/orders",
        json!({
            "topic": "orders",
            "expected_version": 7,
            "patch": {"read_queue_nums": 16}
        }),
    )
    .await;
    let broker_plan = create_r2_plan(
        &service,
        &operator,
        &fixture,
        "broker.config.patch_allowlisted.v1",
        "broker/broker-a",
        json!({
            "broker": "broker-a",
            "expected_generation": 9,
            "patch": {"send_message_thread_pool_nums": 32}
        }),
    )
    .await;
    for plan in [&topic_plan, &broker_plan] {
        let error = service
            .approve(
                &approver,
                plan.id,
                &approval_request(plan),
                rocketmq_sre_contracts::CorrelationId::new(),
            )
            .await
            .expect_err("R2 approval must require Critic");
        assert_eq!(super::service_tests::error_code(&error), "critic_required");
    }

    let reviewed = service
        .review_with_critic(
            &operator,
            topic_plan.id,
            &CriticReviewRequest {
                plan_hash: topic_plan.plan_hash.clone(),
            },
            rocketmq_sre_contracts::CorrelationId::new(),
        )
        .await
        .expect("Critic review");
    assert_eq!(reviewed.plan.status, PlanStatus::ReadyForApproval);
    assert_eq!(reviewed.review.status, CriticReviewStatus::Valid);
    assert_eq!(reviewed.critic_state, CriticGateState::Accepted);
    assert_eq!(reviewed.review.primary_model_family, "fixture-family");
    assert_eq!(
        reviewed.review.critic_model_family.as_deref(),
        Some(kimi.model_family.as_str())
    );
    assert_eq!(
        reviewed.review.critic_model_revision.as_deref(),
        Some(kimi.model_revision.as_str())
    );
    assert!(
        reviewed
            .review
            .fallback_chain
            .iter()
            .any(|identity| identity.contains(&deepseek.model_revision))
    );
    assert_eq!(reviewed.review.diagnosis_revision_id, fixture.diagnosis_id);
    assert_eq!(
        reviewed
            .review
            .assessment
            .as_ref()
            .map(|assessment| assessment.rollback_available),
        Some(true)
    );
    assert!(reviewed.review_hash.starts_with("sha256:"));

    let approved = service
        .approve(
            &approver,
            reviewed.plan.id,
            &approval_request(&reviewed.plan),
            rocketmq_sre_contracts::CorrelationId::new(),
        )
        .await
        .expect("approved after Critic");
    assert_eq!(approved.plan.status, PlanStatus::Approved);

    let invocation = sqlx::query(
        "SELECT purpose, parent_invocation_id, model_family, model_revision,
                fallback_chain, diagnosis_revision_id
         FROM model_invocations
         WHERE id = $1",
    )
    .bind(
        reviewed
            .review
            .critic_invocation_id
            .expect("Critic invocation")
            .as_uuid(),
    )
    .fetch_one(&repository.pool)
    .await
    .expect("Critic invocation row");
    use sqlx::Row as _;
    assert_eq!(invocation.try_get::<String, _>("purpose").expect("purpose"), "critic");
    assert_eq!(
        invocation
            .try_get::<uuid::Uuid, _>("parent_invocation_id")
            .expect("parent"),
        topic_plan.primary_model_invocation_id.as_uuid()
    );
    assert_eq!(
        invocation
            .try_get::<uuid::Uuid, _>("diagnosis_revision_id")
            .expect("diagnosis"),
        fixture.diagnosis_id.as_uuid()
    );
    assert_eq!(
        invocation.try_get::<String, _>("model_revision").expect("revision"),
        kimi.model_revision
    );
    assert_eq!(
        invocation
            .try_get::<Vec<uuid::Uuid>, _>("fallback_chain")
            .expect("fallback chain")
            .len(),
        1
    );
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_same_family_alias_and_endpoint_degrade_without_unlocking_r2() {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 5).await.expect("repository");
    let fixture = seed_fixture(&repository).await;
    let operator = auth(
        fixture.tenant_id,
        fixture.cluster_id,
        "operator-same-family",
        &["operator"],
    );
    let approver = auth(
        fixture.tenant_id,
        fixture.cluster_id,
        "approver-same-family",
        &["approver"],
    );
    let mut alias = profile("deepseek", 1);
    alias.id = "different-profile-alias".to_owned();
    alias.model_family = " FIXTURE_FAMILY ".to_owned();
    alias.endpoint_instance = "different-region:different-endpoint".to_owned();
    let model_gateway =
        ModelGatewayService::for_tests(repository.clone(), vec![alias], Arc::new(ScriptedTransport::new([])));
    let workflow = WorkflowService::new(repository.clone(), WorkflowEventBus::new(64));
    let service = SupervisedExecutionService::new_with_clock_and_model(
        repository,
        workflow,
        "phase3-same-family-signing-key",
        model_gateway,
        Arc::new(Utc::now),
    )
    .expect("service");
    let plan = create_r2_plan(
        &service,
        &operator,
        &fixture,
        "topic.config.patch_allowlisted.v1",
        "topic/orders",
        json!({
            "topic": "orders",
            "expected_version": 8,
            "patch": {"write_queue_nums": 16}
        }),
    )
    .await;

    let reviewed = service
        .review_with_critic(
            &operator,
            plan.id,
            &CriticReviewRequest {
                plan_hash: plan.plan_hash.clone(),
            },
            rocketmq_sre_contracts::CorrelationId::new(),
        )
        .await
        .expect("degraded review record");
    assert_eq!(reviewed.review.status, CriticReviewStatus::Unavailable);
    assert_eq!(reviewed.critic_state, CriticGateState::Unavailable);
    assert_eq!(reviewed.plan.status, PlanStatus::NeedsCritic);
    assert!(reviewed.review.critic_invocation_id.is_none());
    assert!(reviewed.review.critic_model_family.is_none());

    let error = service
        .approve(
            &approver,
            plan.id,
            &approval_request(&plan),
            rocketmq_sre_contracts::CorrelationId::new(),
        )
        .await
        .expect_err("same-family alias must not unlock R2 approval");
    assert_eq!(super::service_tests::error_code(&error), "critic_required");
}

fn profile(id: &str, priority: u16) -> rocketmq_sre_model_gateway::ProviderProfile {
    let mut profile = rocketmq_sre_model_gateway::builtin_provider_profiles()
        .into_iter()
        .find(|profile| profile.id == id)
        .expect("built-in model profile");
    profile.credential_ref = None;
    profile.priority = priority;
    profile
}

fn valid_openai_critic_response(evidence_id: rocketmq_sre_contracts::EvidenceId, model: &str) -> TransportResponse {
    TransportResponse {
        status: 200,
        body: json!({
            "id": "chatcmpl-critic",
            "model": model,
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
                        "rationale": "Evidence, parameter bounds, impact, preconditions, and rollback passed."
                    })).expect("Critic payload")
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

async fn create_r2_plan(
    service: &SupervisedExecutionService,
    operator: &crate::auth::AuthContext,
    fixture: &super::service_tests::Fixture,
    action_id: &str,
    resource: &str,
    parameters: serde_json::Value,
) -> ActionPlan {
    let response = service
        .create_plan(
            operator,
            &CreatePlanRequest {
                cluster_id: fixture.cluster_id,
                incident_id: fixture.incident_id,
                diagnosis_revision_id: fixture.diagnosis_id,
                expires_at: Some(Utc::now() + Duration::hours(1)),
                steps: vec![CandidatePlanStep {
                    action_id: action_id.to_owned(),
                    descriptor_version: "1.0.0".to_owned(),
                    resource: resource.to_owned(),
                    parameters,
                    evidence_ids: vec![fixture.evidence_id],
                }],
            },
            rocketmq_sre_contracts::CorrelationId::new(),
        )
        .await
        .expect("R2 plan");
    let CreatePlanResponse::ActionPlan { plan, .. } = response else {
        panic!("R2 descriptor must create a plan");
    };
    assert_eq!(plan.status, PlanStatus::NeedsCritic);
    *plan
}

fn approval_request(plan: &ActionPlan) -> ApprovalDecisionRequest {
    ApprovalDecisionRequest {
        plan_hash: plan.plan_hash.clone(),
        precondition_hash: plan.compute_precondition_hash().expect("precondition hash"),
        reason: "Heterogeneous review and live preconditions verified".to_owned(),
        validity_seconds: Some(60),
    }
}
