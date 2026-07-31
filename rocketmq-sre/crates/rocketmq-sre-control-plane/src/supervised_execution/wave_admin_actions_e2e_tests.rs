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

use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::Duration;
use chrono::Utc;
use reqwest::Url;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::CriticGateState;
use rocketmq_sre_contracts::CriticReviewStatus;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionState;
use rocketmq_sre_contracts::PlanStatus;
use rocketmq_sre_contracts::TenantId;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;
use uuid::Uuid;

use super::ExecutorSubmissionClient;
use super::credential_rotation_e2e_tests::ScriptedTransport;
use super::credential_rotation_e2e_tests::critic_profile;
use super::credential_rotation_e2e_tests::valid_critic_response;
use super::model::ApprovalDecisionRequest;
use super::model::CandidatePlanStep;
use super::model::CreatePlanRequest;
use super::model::CreatePlanResponse;
use super::model::CriticReviewRequest;
use super::model::SubmitExecutionRequest;
use super::proxy_restart_e2e_tests::ExecutionFixture;
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
const BROKER_ADDRESS: &str = "rocketmq-broker.rocketmq-system.svc.cluster.local:10911";
const WAVE_TOPIC: &str = "SRE_PHASE03_WAVE_TOPIC";
const WAVE_GROUP: &str = "SRE_PHASE03_WAVE_GROUP";

struct DiscoveredActionCase {
    action: ExecutionAction,
    target: String,
    parameters: Value,
    baseline: AgentReadResult,
    required_conditions: &'static [&'static str],
}

#[tokio::test]
#[ignore = "requires live Kind PostgreSQL, Executor, Agent, authenticated Broker, and wave Admin fixtures"]
async fn real_kind_wave_admin_actions_share_r2_critic_approval_and_verification() {
    let Some(environment) = LiveEnvironment::from_process() else {
        return;
    };
    let cases = [
        discover_case(
            &environment,
            ExecutionAction::BrokerConfigPatchAllowlisted,
            format!("broker/{BROKER_ADDRESS}"),
            "broker",
            BROKER_ADDRESS,
            "expected_generation",
            1..=512,
            &[
                json!({"max_client_event_count": 9_999}),
                json!({"max_client_event_count": 9_998}),
            ],
            &["generation_incremented", "patch_visible"],
        )
        .await,
        discover_case(
            &environment,
            ExecutionAction::TopicConfigPatchAllowlisted,
            format!("topic/{WAVE_TOPIC}"),
            "topic",
            WAVE_TOPIC,
            "expected_version",
            0..=512,
            &[
                json!({"read_queue_nums": 5, "write_queue_nums": 5}),
                json!({"read_queue_nums": 6, "write_queue_nums": 6}),
            ],
            &["topic_version_incremented", "patch_visible"],
        )
        .await,
        discover_case(
            &environment,
            ExecutionAction::SubscriptionGroupPatchAllowlisted,
            format!("subscription-group/{WAVE_GROUP}"),
            "group",
            WAVE_GROUP,
            "expected_version",
            0..=512,
            &[json!({"retry_max_times": 15}), json!({"retry_max_times": 14})],
            &["subscription_group_version_incremented", "patch_visible"],
        )
        .await,
    ];

    for case in cases {
        execute_r2_case(&environment, case).await;
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the live discovery fixture binds a typed action schema and its bounded CAS search"
)]
async fn discover_case(
    environment: &LiveEnvironment,
    action: ExecutionAction,
    target: String,
    resource_field: &str,
    resource_value: &str,
    revision_field: &str,
    revisions: std::ops::RangeInclusive<u64>,
    patches: &[Value],
    required_conditions: &'static [&'static str],
) -> DiscoveredActionCase {
    for patch in patches {
        for revision in revisions.clone() {
            let mut parameters = Map::new();
            parameters.insert(resource_field.to_owned(), Value::String(resource_value.to_owned()));
            parameters.insert(revision_field.to_owned(), Value::from(revision));
            parameters.insert("patch".to_owned(), patch.clone());
            let parameters = Value::Object(parameters);
            let baseline = fetch_agent_state(
                &environment.agent_url,
                &environment.workload_token,
                environment.tenant_id,
                environment.cluster_id,
                action,
                &target,
                parameters.clone(),
            )
            .await;
            if baseline.ready {
                return DiscoveredActionCase {
                    action,
                    target,
                    parameters,
                    baseline,
                    required_conditions,
                };
            }
        }
    }
    panic!(
        "{} did not expose a ready CAS revision in the bounded live fixture",
        action.id()
    );
}

async fn execute_r2_case(environment: &LiveEnvironment, case: DiscoveredActionCase) {
    let repository = PostgresRepository::connect(&environment.database_url, 5)
        .await
        .expect("Kind PostgreSQL repository");
    let mut fixture = seed_execution_fixture(
        &repository,
        environment.tenant_id,
        environment.cluster_id,
        &case.target,
        &case.baseline,
    )
    .await;
    seed_complete_slo_evidence(&repository, &fixture).await;
    let refreshed = fetch_agent_state(
        &environment.agent_url,
        &environment.workload_token,
        environment.tenant_id,
        environment.cluster_id,
        case.action,
        &case.target,
        case.parameters.clone(),
    )
    .await;
    assert!(
        refreshed.ready,
        "{} CAS precondition changed before planning: {:?}",
        case.action.id(),
        refreshed.reason_codes
    );
    fixture.agent_evidence_id = persist_agent_evidence(&repository, &fixture, &case.target, &refreshed).await;

    let profile = critic_profile();
    let profile_id = profile.id.clone();
    let critic_family = profile.model_family.clone();
    let model_gateway = ModelGatewayService::for_tests(
        repository.clone(),
        vec![profile],
        Arc::new(ScriptedTransport::new([Ok(valid_critic_response(
            fixture.agent_evidence_id,
        ))])),
    );
    let governance = auth(
        environment.tenant_id,
        environment.cluster_id,
        "phase3-wave-admin-model-governance",
        &["model-governance"],
    );
    model_gateway
        .certify_profile_for_tests(&governance, &profile_id, CorrelationId::new())
        .await
        .unwrap_or_else(|error| panic!("certify {} Critic profile: {error}", case.action.id()));
    let workflow = WorkflowService::new(repository.clone(), WorkflowEventBus::new(64));
    let executor = ExecutorSubmissionClient::http(
        environment.executor_url.parse::<Url>().expect("Executor URL"),
        environment.workload_token.clone(),
        StdDuration::from_secs(900),
        true,
    )
    .expect("Executor client");
    let service = SupervisedExecutionService::new_with_executor(
        repository.clone(),
        workflow,
        environment.signing_key.clone(),
        model_gateway,
        executor,
    )
    .expect("supervised execution service");
    let operator = auth(
        environment.tenant_id,
        environment.cluster_id,
        "phase3-wave-admin-operator",
        &["operator"],
    );
    let approver = auth(
        environment.tenant_id,
        environment.cluster_id,
        "phase3-wave-admin-approver",
        &["approver"],
    );
    let correlation_id = CorrelationId::new();
    let created = service
        .create_plan(
            &operator,
            &CreatePlanRequest {
                cluster_id: environment.cluster_id,
                incident_id: fixture.incident_id,
                diagnosis_revision_id: fixture.diagnosis_id,
                expires_at: Some(Utc::now() + Duration::minutes(30)),
                steps: vec![CandidatePlanStep {
                    action_id: case.action.id().to_owned(),
                    descriptor_version: "1.0.0".to_owned(),
                    resource: case.target.clone(),
                    parameters: case.parameters.clone(),
                    evidence_ids: vec![fixture.agent_evidence_id],
                }],
            },
            correlation_id,
        )
        .await
        .unwrap_or_else(|error| panic!("create {} plan: {error}", case.action.id()));
    let CreatePlanResponse::ActionPlan { plan, .. } = created else {
        panic!("{} must create an executable ActionPlan", case.action.id());
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
            correlation_id,
        )
        .await
        .unwrap_or_else(|error| panic!("review {} with heterogeneous Critic: {error}", case.action.id()));
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
                reason: format!(
                    "Independent reviewer accepted the bounded {} CAS change and automatic inverse patch",
                    case.action.id()
                ),
                validity_seconds: Some(1_800),
            },
            correlation_id,
        )
        .await
        .unwrap_or_else(|error| panic!("approve {} plan: {error}", case.action.id()));
    let execution_request = SubmitExecutionRequest {
        plan_id: reviewed.plan.id,
        plan_hash: reviewed.plan.plan_hash,
        precondition_hash,
        idempotency_key: format!("phase3-wave-admin-{}-{}", case.action.id(), Uuid::new_v4()),
    };
    let submitted = submit_with_slo_refresh(
        &service,
        &operator,
        &execution_request,
        correlation_id,
        &repository,
        &fixture,
    )
    .await;
    assert_eq!(
        submitted.state,
        ExecutionState::Succeeded,
        "{} supervised execution did not succeed",
        case.action.id()
    );

    let applied = fetch_agent_state(
        &environment.agent_url,
        &environment.workload_token,
        environment.tenant_id,
        environment.cluster_id,
        case.action,
        &case.target,
        case.parameters,
    )
    .await;
    for condition in case.required_conditions {
        assert_eq!(
            applied.resource_conditions.get(*condition),
            Some(&true),
            "{} did not prove condition {condition}: {:?}",
            case.action.id(),
            applied
        );
    }
    let audit = service
        .audit(&operator, correlation_id)
        .await
        .expect("shared correlation audit");
    for event_kind in [
        AuditEventKind::PlanCreated,
        AuditEventKind::CriticReviewed,
        AuditEventKind::Approved,
        AuditEventKind::ExecutionSubmitted,
    ] {
        assert!(
            audit.items.iter().any(|event| event.event_kind == event_kind),
            "{} correlation timeline is missing {event_kind:?}",
            case.action.id()
        );
    }
}

async fn submit_with_slo_refresh(
    service: &SupervisedExecutionService,
    operator: &crate::auth::AuthContext,
    request: &SubmitExecutionRequest,
    correlation_id: CorrelationId,
    repository: &PostgresRepository,
    fixture: &ExecutionFixture,
) -> super::model::ExecutionSubmissionView {
    let submission = service.submit_execution(operator, request, correlation_id);
    tokio::pin!(submission);
    loop {
        tokio::select! {
            result = &mut submission => {
                return result.expect("execute supervised wave Admin action");
            }
            () = tokio::time::sleep(StdDuration::from_secs(60)) => {
                seed_complete_slo_evidence(repository, fixture).await;
            }
        }
    }
}

struct LiveEnvironment {
    database_url: String,
    executor_url: String,
    agent_url: String,
    workload_token: String,
    signing_key: String,
    tenant_id: TenantId,
    cluster_id: ClusterId,
}

impl LiveEnvironment {
    fn from_process() -> Option<Self> {
        Some(Self {
            database_url: required_env("ROCKETMQ_SRE_PHASE3_DATABASE_URL")?,
            executor_url: required_env("ROCKETMQ_SRE_PHASE3_EXECUTOR_URL")?,
            agent_url: required_env("ROCKETMQ_SRE_PHASE3_AGENT_URL")?,
            workload_token: required_env("ROCKETMQ_SRE_PHASE3_WORKLOAD_TOKEN")?,
            signing_key: required_env("ROCKETMQ_SRE_PHASE3_SIGNING_KEY")?,
            tenant_id: optional_id("ROCKETMQ_SRE_PHASE3_TENANT_ID", DEFAULT_TENANT_ID),
            cluster_id: optional_id("ROCKETMQ_SRE_PHASE3_CLUSTER_ID", DEFAULT_CLUSTER_ID),
        })
    }
}

fn required_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
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
