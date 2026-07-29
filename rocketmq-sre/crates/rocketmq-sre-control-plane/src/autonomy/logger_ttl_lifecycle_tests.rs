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

use super::AutonomyService;
use super::model::AutonomyScopeQuery;
use super::model::AutonomyTransitionRequest;
use super::model::CreateAutonomyPolicyRequest;
use super::model::CreateShadowCohortRequest;
use super::model::PrepareAutonomousCohortRequest;
use super::model::RecordQualificationSampleRequest;
use super::repository_tests::Fixture;
use super::repository_tests::seed_critic_review;
use super::repository_tests::seed_fixture;
use super::repository_tests::seed_successful_supervised_execution_for_action_at;
use super::repository_tests::unique_digest;
use crate::PostgresRepository;
use crate::SupervisedRepository;
use crate::alerting::AlertingService;
use crate::auth::AuthContext;
use crate::connector_channel::PostgresConnectorChannelService;
use crate::evidence::EvidenceBlobStore;
use crate::evidence::EvidenceService;
use crate::slo::SloService;
use crate::workflow::WorkflowEventBus;
use crate::workflow::WorkflowService;
use chrono::DateTime;
use chrono::Duration;
use chrono::Timelike;
use chrono::Utc;
use rocketmq_sre_contracts::ActionDescriptor;
use rocketmq_sre_contracts::ActionPlan;
use rocketmq_sre_contracts::ActionPlanDraft;
use rocketmq_sre_contracts::ActionRisk;
use rocketmq_sre_contracts::AutonomyMode;
use rocketmq_sre_contracts::AutonomySampleKind;
use rocketmq_sre_contracts::CompensationMode;
use rocketmq_sre_contracts::CompensationSpec;
use rocketmq_sre_contracts::CriticReviewId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ImpactScope;
use rocketmq_sre_contracts::PlanStep;
use rocketmq_sre_contracts::PlanStepId;
use rocketmq_sre_contracts::VerificationSpec;
use rocketmq_sre_contracts::canonical_sha256;
use rocketmq_sre_core::EMBEDDED_ACTION_DESCRIPTOR_YAMLS;
use uuid::Uuid;

const SHADOW_SAMPLES: usize = 20;
const SUPERVISED_SUCCESSES: usize = 5;
const OBSERVATION_DAYS: i64 = 7;

#[derive(Clone, Copy)]
enum R1ActionScenario {
    LoggerTtl,
    ProxyScaleOut,
    ProxyRestartOne,
    TelemetryCollectorRestartOne,
}

impl R1ActionScenario {
    const fn action(self) -> ExecutionAction {
        match self {
            Self::LoggerTtl => ExecutionAction::ObservabilityLoggerLevelTtl,
            Self::ProxyScaleOut => ExecutionAction::ProxyScaleOutOne,
            Self::ProxyRestartOne => ExecutionAction::ProxyRestartOne,
            Self::TelemetryCollectorRestartOne => ExecutionAction::TelemetryCollectorRestartOne,
        }
    }

    const fn diagnostic_pack_id(self) -> &'static str {
        match self {
            Self::LoggerTtl => "runtime-diagnostics",
            Self::ProxyScaleOut => "proxy-connectivity",
            Self::ProxyRestartOne => "proxy-drain-readiness",
            Self::TelemetryCollectorRestartOne => "telemetry-recovery",
        }
    }

    const fn stable_window_seconds(self) -> i64 {
        match self {
            Self::LoggerTtl => 30,
            Self::ProxyScaleOut => 120,
            Self::ProxyRestartOne => 180,
            Self::TelemetryCollectorRestartOne => 600,
        }
    }

    const fn symptom_family(self) -> &'static str {
        match self {
            Self::LoggerTtl => "logger_qualification",
            Self::ProxyScaleOut => "proxy_capacity_qualification",
            Self::ProxyRestartOne => "proxy_restart_qualification",
            Self::TelemetryCollectorRestartOne => "telemetry_recovery_qualification",
        }
    }

    const fn resource(self) -> &'static str {
        match self {
            Self::LoggerTtl => "broker/test",
            Self::ProxyScaleOut => "deployment/rocketmq-system/rocketmq-proxy",
            Self::ProxyRestartOne => "pod/rocketmq-system/rocketmq-proxy-qualification",
            Self::TelemetryCollectorRestartOne => "pod/observability/otel-collector-qualification",
        }
    }
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn logger_ttl_qualifies_through_shadow_supervised_and_autonomous() {
    qualify_r1_action_through_autonomy(R1ActionScenario::LoggerTtl).await;
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn proxy_scale_qualifies_through_shadow_supervised_and_autonomous() {
    qualify_r1_action_through_autonomy(R1ActionScenario::ProxyScaleOut).await;
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn proxy_restart_qualifies_through_shadow_supervised_and_autonomous() {
    qualify_r1_action_through_autonomy(R1ActionScenario::ProxyRestartOne).await;
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn telemetry_collector_restart_qualifies_through_shadow_supervised_and_autonomous() {
    qualify_r1_action_through_autonomy(R1ActionScenario::TelemetryCollectorRestartOne).await;
}

async fn qualify_r1_action_through_autonomy(scenario: R1ActionScenario) {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 8)
        .await
        .expect("repository with migrations");
    let fixture = seed_fixture(&repository).await;
    let started_at = Utc::now().with_nanosecond(0).expect("whole-second qualification clock");
    let auth = operator_auth(&fixture);
    let clock_value = Arc::new(Mutex::new(started_at));
    let service = autonomy_service(&repository, Arc::clone(&clock_value));
    let descriptor = action_descriptor(scenario.action());
    let descriptor_digest = canonical_sha256(&descriptor).expect("descriptor digest");

    let created = service
        .create_policy(
            &auth,
            &CreateAutonomyPolicyRequest {
                cluster_id: fixture.cluster_id,
                action: scenario.action(),
                action_version: descriptor.version.clone(),
                descriptor_digest,
                diagnostic_pack_id: scenario.diagnostic_pack_id().to_owned(),
                diagnostic_pack_version: "1.0.0".to_owned(),
                owner: descriptor.owner.clone(),
                minimum_evidence_freshness_seconds: 60,
                required_evidence_sources: vec!["prometheus".to_owned()],
                min_shadow_samples: SHADOW_SAMPLES as u32,
                min_supervised_successes: SUPERVISED_SUCCESSES as u32,
                observation_window_days: OBSERVATION_DAYS as u16,
                max_unresolved_unknown: 0,
                max_recent_rollbacks: 0,
                max_executions_per_hour: 2,
                cooldown_seconds: 900,
                max_concurrent_executions: 1,
                stable_window_seconds: scenario.stable_window_seconds() as u64,
            },
        )
        .await
        .expect("logger autonomy policy");
    assert_eq!(created.lifecycle.mode, AutonomyMode::Disabled);

    let query = scope_query(fixture.cluster_id, scenario.action());
    let shadow = service
        .transition(
            &auth,
            &query,
            &AutonomyTransitionRequest {
                target_mode: AutonomyMode::Shadow,
                reason: Some("start bounded logger qualification".to_owned()),
                owner_confirmed: false,
            },
        )
        .await
        .expect("Disabled to Shadow");
    assert_eq!(shadow.lifecycle.mode, AutonomyMode::Shadow);
    let shadow_cohort = service
        .create_shadow_cohort(
            &auth,
            &CreateShadowCohortRequest {
                cluster_id: fixture.cluster_id,
                action: scenario.action(),
                action_version: "1.0.0".to_owned(),
                primary_profile: fixture.primary_profile.clone(),
                primary_model_family: "deepseek".to_owned(),
                primary_model_revision: "v3.2".to_owned(),
            },
        )
        .await
        .expect("Shadow cohort");

    let mut plans = Vec::with_capacity(SHADOW_SAMPLES);
    for sequence in 0..SHADOW_SAMPLES {
        plans.push(seed_action_plan(&repository, &fixture, started_at, sequence, scenario).await);
    }
    let shadow_observed_at = shadow_cohort.created_at + Duration::seconds(30);
    for plan in &plans {
        let sample = service
            .record_qualification_sample(
                &auth,
                &qualification_request(
                    plan,
                    shadow_cohort.id,
                    AutonomySampleKind::ShadowOutcome,
                    None,
                    shadow_observed_at,
                    scenario.action(),
                ),
            )
            .await
            .expect("qualified Shadow sample");
        assert!(sample.qualified);
    }
    assert!(
        service
            .transition(
                &auth,
                &query,
                &AutonomyTransitionRequest {
                    target_mode: AutonomyMode::Supervised,
                    reason: Some("window must not be bypassed".to_owned()),
                    owner_confirmed: true,
                },
            )
            .await
            .is_err()
    );

    set_clock(
        &clock_value,
        shadow_cohort.created_at + Duration::days(OBSERVATION_DAYS) + Duration::seconds(30),
    );
    let supervised = service
        .transition(
            &auth,
            &query,
            &AutonomyTransitionRequest {
                target_mode: AutonomyMode::Supervised,
                reason: Some("Shadow sample and window targets met".to_owned()),
                owner_confirmed: true,
            },
        )
        .await
        .expect("Shadow to Supervised");
    assert_eq!(supervised.lifecycle.mode, AutonomyMode::Supervised);
    assert_eq!(supervised.qualification.qualified_shadow_samples, SHADOW_SAMPLES as u32);
    assert!(supervised.qualification.shadow_observation_window_met);

    let primary_plan = &plans[0];
    let (base_review_id, critic_invocation_id, critic_profile) = seed_critic_review(&repository, primary_plan).await;
    let mut critic_reviews = vec![base_review_id];
    for plan in plans.iter().take(SUPERVISED_SUCCESSES).skip(1) {
        critic_reviews.push(seed_shared_critic_review(&repository, plan, critic_invocation_id, &critic_profile).await);
    }
    let autonomous_cohort = service
        .prepare_autonomous_cohort(
            &auth,
            &PrepareAutonomousCohortRequest {
                cluster_id: fixture.cluster_id,
                action: scenario.action(),
                action_version: "1.0.0".to_owned(),
                diagnosis_revision_id: primary_plan.diagnosis_revision_id,
                plan_id: primary_plan.plan_id,
                plan_hash: primary_plan.plan_hash.clone(),
                critic_review_id: critic_reviews[0],
                primary_model_invocation_id: primary_plan.primary_invocation_id,
                critic_model_invocation_id: critic_invocation_id,
                primary_profile: primary_plan.primary_profile.clone(),
                primary_model_family: "deepseek".to_owned(),
                primary_model_revision: "v3.2".to_owned(),
                critic_profile,
                critic_model_family: "glm".to_owned(),
                critic_model_revision: "glm-5".to_owned(),
            },
        )
        .await
        .expect("heterogeneous Autonomous cohort");
    let supervised_observed_at = autonomous_cohort.created_at + Duration::seconds(30);
    set_clock(&clock_value, supervised_observed_at);
    for plan in plans.iter().take(SUPERVISED_SUCCESSES) {
        let execution_id = seed_successful_supervised_execution_for_action_at(
            &repository,
            plan,
            scenario.stable_window_seconds(),
            supervised_observed_at,
            scenario.action(),
            scenario.resource(),
        )
        .await;
        let sample = service
            .record_qualification_sample(
                &auth,
                &qualification_request(
                    plan,
                    autonomous_cohort.id,
                    AutonomySampleKind::SupervisedSuccess,
                    Some(execution_id),
                    supervised_observed_at,
                    scenario.action(),
                ),
            )
            .await
            .expect("qualified Supervised execution");
        assert!(sample.qualified);
    }
    assert!(
        service
            .transition(
                &auth,
                &query,
                &AutonomyTransitionRequest {
                    target_mode: AutonomyMode::Autonomous,
                    reason: Some("autonomous window must not be bypassed".to_owned()),
                    owner_confirmed: true,
                },
            )
            .await
            .is_err()
    );

    set_clock(
        &clock_value,
        autonomous_cohort.created_at + Duration::days(OBSERVATION_DAYS) + Duration::seconds(30),
    );
    let autonomous = service
        .transition(
            &auth,
            &query,
            &AutonomyTransitionRequest {
                target_mode: AutonomyMode::Autonomous,
                reason: Some("Supervised target and observation window met".to_owned()),
                owner_confirmed: true,
            },
        )
        .await
        .expect("Supervised to Autonomous");
    assert_eq!(autonomous.lifecycle.mode, AutonomyMode::Autonomous);
    assert_eq!(
        autonomous.qualification.qualified_supervised_successes,
        SUPERVISED_SUCCESSES as u32
    );
    assert!(autonomous.qualification.autonomous_observation_window_met);
    assert_eq!(autonomous.qualification.unresolved_unknown, 0);
    assert_eq!(autonomous.qualification.recent_rollbacks, 0);
}

fn autonomy_service(repository: &PostgresRepository, clock: Arc<Mutex<DateTime<Utc>>>) -> AutonomyService {
    let evidence = EvidenceService::new(repository.clone(), EvidenceBlobStore::in_memory(64 * 1024));
    let workflow = WorkflowService::new(repository.clone(), WorkflowEventBus::new(64));
    let alerting = AlertingService::new(repository.clone(), workflow).expect("alerting service");
    let connector = PostgresConnectorChannelService::postgres(repository.clone(), "logger-lifecycle-test-token")
        .expect("connector");
    let slo = SloService::new(repository.clone(), connector, evidence, alerting).expect("SLO service");
    let clock = Arc::new(move || *clock.lock().expect("qualification clock"));
    AutonomyService::new_with_clock(repository.clone(), slo, &[17_u8; 32], clock).expect("autonomy service")
}

fn operator_auth(fixture: &Fixture) -> AuthContext {
    AuthContext {
        tenant_id: fixture.tenant_id,
        subject: "r1-autonomy-owner".to_owned(),
        clusters: BTreeSet::from([fixture.cluster_id]),
        roles: BTreeSet::from(["operator".to_owned()]),
    }
}

fn action_descriptor(action: ExecutionAction) -> ActionDescriptor {
    EMBEDDED_ACTION_DESCRIPTOR_YAMLS
        .iter()
        .map(|yaml| serde_yaml::from_str::<ActionDescriptor>(yaml).expect("embedded action descriptor"))
        .find(|descriptor| descriptor.id == action.id())
        .expect("R1 action descriptor")
}

fn scope_query(cluster_id: rocketmq_sre_contracts::ClusterId, action: ExecutionAction) -> AutonomyScopeQuery {
    AutonomyScopeQuery {
        cluster_id,
        action,
        action_version: "1.0.0".to_owned(),
    }
}

fn set_clock(clock: &Mutex<DateTime<Utc>>, now: DateTime<Utc>) {
    *clock.lock().expect("qualification clock") = now;
}

fn qualification_request(
    fixture: &Fixture,
    cohort_id: rocketmq_sre_contracts::AutonomyCohortId,
    kind: AutonomySampleKind,
    execution_id: Option<rocketmq_sre_contracts::ExecutionId>,
    observed_at: DateTime<Utc>,
    action: ExecutionAction,
) -> RecordQualificationSampleRequest {
    RecordQualificationSampleRequest {
        cluster_id: fixture.cluster_id,
        action,
        action_version: "1.0.0".to_owned(),
        cohort_id,
        kind,
        incident_id: fixture.incident_id,
        plan_id: fixture.plan_id,
        plan_hash: fixture.plan_hash.clone(),
        execution_id,
        reason_codes: Vec::new(),
        human_outcome_linked: true,
        evidence_complete: true,
        stable_window_passed: true,
        offline_replay: false,
        debug_only: false,
        observed_at,
        reconciled_at: observed_at,
    }
}

async fn seed_action_plan(
    repository: &PostgresRepository,
    base: &Fixture,
    created_at: DateTime<Utc>,
    sequence: usize,
    scenario: R1ActionScenario,
) -> Fixture {
    let fixture = Fixture {
        tenant_id: base.tenant_id,
        cluster_id: base.cluster_id,
        incident_id: rocketmq_sre_contracts::IncidentId::new(),
        diagnosis_revision_id: rocketmq_sre_contracts::DiagnosisRevisionId::new(),
        primary_invocation_id: rocketmq_sre_contracts::ModelInvocationId::new(),
        primary_profile: base.primary_profile.clone(),
        plan_id: rocketmq_sre_contracts::ActionPlanId::new(),
        plan_hash: String::new(),
    };
    sqlx::query(
        "INSERT INTO sre_incidents (
            id, tenant_id, cluster_id, title, resource, symptom_family,
            fingerprint, status, workflow_checkpoint, created_by_subject,
            created_at, updated_at
         ) VALUES ($1, $2, $3, $4, $5, $6,
                   $7, 'diagnosing', '{}'::JSONB, 'r1-autonomy-test',
                   $8, $8)",
    )
    .bind(fixture.incident_id.as_uuid())
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .bind(format!("R1 qualification sample {sequence}"))
    .bind(scenario.resource())
    .bind(scenario.symptom_family())
    .bind(unique_digest())
    .bind(created_at)
    .execute(&repository.pool)
    .await
    .expect("qualification incident");
    sqlx::query(
        "INSERT INTO diagnosis_revisions (
            id, incident_id, revision, status, rule_result, hypotheses,
            evidence_ids, primary_model_invocation_id,
            execution_eligible, partial, created_at
         ) VALUES ($1, $2, 1, 'confirmed', '{}'::JSONB, '[]'::JSONB,
                   '{}', NULL, FALSE, FALSE, $3)",
    )
    .bind(fixture.diagnosis_revision_id.as_uuid())
    .bind(fixture.incident_id.as_uuid())
    .bind(created_at)
    .execute(&repository.pool)
    .await
    .expect("qualification diagnosis");
    let profile_id: Uuid = sqlx::query_scalar(
        "SELECT id
         FROM model_profiles
         WHERE tenant_id = $1 AND profile_name = $2",
    )
    .bind(fixture.tenant_id.as_uuid())
    .bind(&fixture.primary_profile)
    .fetch_one(&repository.pool)
    .await
    .expect("primary model profile");
    sqlx::query(
        "INSERT INTO model_invocations (
            id, tenant_id, cluster_id, incident_id, diagnosis_revision_id,
            parent_invocation_id, purpose, requested_profile_id,
            actual_profile_id, provider_family, model_family, model_revision,
            endpoint_instance, fallback_chain, prompt_version, schema_version,
            rationale, started_at, completed_at
         ) VALUES (
            $1, $2, $3, $4, $5,
            NULL, 'primary_diagnosis', $6,
            $6, 'openai-compatible', 'deepseek', 'v3.2',
            'local', '{}', 'r1-autonomy-test',
            'rocketmq-sre.model.v1', 'R1 qualification fixture', $7, $7
         )",
    )
    .bind(fixture.primary_invocation_id.as_uuid())
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .bind(fixture.incident_id.as_uuid())
    .bind(fixture.diagnosis_revision_id.as_uuid())
    .bind(profile_id)
    .bind(created_at)
    .execute(&repository.pool)
    .await
    .expect("primary model invocation");
    sqlx::query(
        "UPDATE diagnosis_revisions
         SET primary_model_invocation_id = $2, execution_eligible = TRUE
         WHERE id = $1",
    )
    .bind(fixture.diagnosis_revision_id.as_uuid())
    .bind(fixture.primary_invocation_id.as_uuid())
    .execute(&repository.pool)
    .await
    .expect("bind primary model invocation");
    let mut fixture = fixture;
    let plan = action_plan(&fixture, created_at + Duration::seconds(sequence as i64), scenario);
    fixture.plan_hash.clone_from(&plan.plan_hash);
    repository
        .store_action_plan(&plan, ActionRisk::R1)
        .await
        .expect("qualification action plan");
    fixture
}

fn action_plan(fixture: &Fixture, created_at: DateTime<Utc>, scenario: R1ActionScenario) -> ActionPlan {
    let (resource, parameters, max_impact, verification, compensation) = match scenario {
        R1ActionScenario::LoggerTtl => (
            "broker/127.0.0.1:10911",
            serde_json::json!({
                "component": "broker",
                "logger": "rocketmq_broker::processor",
                "level": "DEBUG",
                "ttl_seconds": 60
            }),
            ImpactScope::SingleResource,
            VerificationSpec {
                resource_conditions: vec!["logger_level_applied".to_owned(), "ttl_restore_scheduled".to_owned()],
                technical_slis: vec!["runtime_error_ratio".to_owned()],
                stable_window_seconds: scenario.stable_window_seconds() as u64,
                max_wait_seconds: 120,
            },
            CompensationSpec {
                mode: CompensationMode::Automatic,
                required_before_fields: vec!["previous_level".to_owned()],
                timeout_seconds: 60,
            },
        ),
        R1ActionScenario::ProxyScaleOut => (
            "deployment/rocketmq-system/rocketmq-proxy",
            serde_json::json!({
                "namespace": "rocketmq-system",
                "workload": "rocketmq-proxy",
                "expected_replicas": 1
            }),
            ImpactScope::OneReplica,
            VerificationSpec {
                resource_conditions: vec!["desired_replicas_plus_one".to_owned(), "new_replica_ready".to_owned()],
                technical_slis: vec!["proxy_error_ratio".to_owned(), "proxy_p99_latency".to_owned()],
                stable_window_seconds: scenario.stable_window_seconds() as u64,
                max_wait_seconds: 900,
            },
            CompensationSpec {
                mode: CompensationMode::Automatic,
                required_before_fields: vec!["expected_replicas".to_owned()],
                timeout_seconds: 600,
            },
        ),
        R1ActionScenario::ProxyRestartOne => (
            "pod/rocketmq-system/rocketmq-proxy-qualification",
            serde_json::json!({
                "namespace": "rocketmq-system",
                "pod": "rocketmq-proxy-qualification",
                "expected_uid": "00000000-0000-4000-8000-000000000001"
            }),
            ImpactScope::SingleInstance,
            VerificationSpec {
                resource_conditions: vec!["replacement_ready".to_owned(), "accepting_and_routed".to_owned()],
                technical_slis: vec!["proxy_error_ratio".to_owned(), "synthetic_message_path".to_owned()],
                stable_window_seconds: scenario.stable_window_seconds() as u64,
                max_wait_seconds: 1_200,
            },
            CompensationSpec {
                mode: CompensationMode::ManualTakeover,
                required_before_fields: vec![
                    "admission_state".to_owned(),
                    "readiness_state".to_owned(),
                    "routing_state".to_owned(),
                ],
                timeout_seconds: 300,
            },
        ),
        R1ActionScenario::TelemetryCollectorRestartOne => (
            "pod/observability/otel-collector-qualification",
            serde_json::json!({
                "namespace": "observability",
                "pod": "otel-collector-qualification",
                "expected_uid": "00000000-0000-4000-8000-000000000002",
                "pipeline": "combined"
            }),
            ImpactScope::SingleInstance,
            VerificationSpec {
                resource_conditions: vec![
                    "replacement_uid_observed".to_owned(),
                    "collector_ready".to_owned(),
                    "exporter_connected".to_owned(),
                ],
                technical_slis: vec![
                    "telemetry_export_success_ratio".to_owned(),
                    "telemetry_queue_utilization".to_owned(),
                ],
                stable_window_seconds: scenario.stable_window_seconds() as u64,
                max_wait_seconds: 900,
            },
            CompensationSpec {
                mode: CompensationMode::ManualTakeover,
                required_before_fields: vec!["expected_uid".to_owned(), "pipeline_health".to_owned()],
                timeout_seconds: 300,
            },
        ),
    };
    ActionPlan::seal(ActionPlanDraft {
        id: fixture.plan_id,
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        incident_id: fixture.incident_id,
        diagnosis_revision: fixture.diagnosis_revision_id,
        primary_model_invocation_id: fixture.primary_invocation_id,
        diagnosis_execution_eligible: true,
        version: 1,
        created_by: "r1-autonomy-owner".to_owned(),
        created_at,
        expires_at: created_at + Duration::days(30),
        evidence_hash: unique_digest(),
        steps: vec![PlanStep {
            id: PlanStepId::new(),
            sequence: 1,
            action: scenario.action(),
            descriptor_version: "1.0.0".to_owned(),
            resource: resource.to_owned(),
            parameters,
            evidence_ids: vec![EvidenceId::new()],
            precondition_hash: unique_digest(),
            max_impact,
            verification,
            compensation,
        }],
    })
    .expect("valid R1 plan")
    .submit_for_review(created_at + Duration::seconds(1), false)
    .expect("ready R1 plan")
}

async fn seed_shared_critic_review(
    repository: &PostgresRepository,
    fixture: &Fixture,
    critic_invocation_id: rocketmq_sre_contracts::ModelInvocationId,
    critic_profile: &str,
) -> CriticReviewId {
    let review_id = CriticReviewId::new();
    sqlx::query(
        "INSERT INTO critic_reviews (
            id, plan_id, plan_hash, diagnosis_revision_id,
            primary_invocation_id, critic_invocation_id,
            primary_model_family, critic_model_family,
            critic_provider, critic_profile, critic_model_revision,
            endpoint_instance, fallback_chain, prompt_version,
            schema_version, payload_hash, conclusion, status,
            review_hash, review_snapshot, created_at
         ) VALUES (
            $1, $2, $3, $4, $5, $6,
            'deepseek', 'glm', 'openai-compatible', $7, 'glm-5',
            'local', '{}', 'logger-autonomy-test',
            'rocketmq-sre.critic.v1', $8, 'accept', 'valid',
            $9, '{}'::JSONB, NOW()
         )",
    )
    .bind(review_id.as_uuid())
    .bind(fixture.plan_id.as_uuid())
    .bind(&fixture.plan_hash)
    .bind(fixture.diagnosis_revision_id.as_uuid())
    .bind(fixture.primary_invocation_id.as_uuid())
    .bind(critic_invocation_id.as_uuid())
    .bind(critic_profile)
    .bind(unique_digest())
    .bind(unique_digest())
    .execute(&repository.pool)
    .await
    .expect("shared heterogeneous critic review");
    review_id
}
