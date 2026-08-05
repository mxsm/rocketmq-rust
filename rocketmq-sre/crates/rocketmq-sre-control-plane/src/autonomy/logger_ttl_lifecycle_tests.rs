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
use std::fs;
use std::path::Path;
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
use rocketmq_sre_contracts::AutonomousExecutionFailure;
use rocketmq_sre_contracts::AutonomyMode;
use rocketmq_sre_contracts::AutonomyOutcome;
use rocketmq_sre_contracts::AutonomyOutcomeClass;
use rocketmq_sre_contracts::AutonomyOutcomeId;
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
use serde::Serialize;
use uuid::Uuid;

const SHADOW_SAMPLES: usize = 20;
const SUPERVISED_SUCCESSES: usize = 5;
const OBSERVATION_DAYS: i64 = 7;
const PRIMARY_MODEL_FAMILY: &str = "qualification-primary";
const PRIMARY_MODEL_REVISION: &str = "fixture-r1";
const CRITIC_MODEL_FAMILY: &str = "qualification-critic";
const CRITIC_MODEL_REVISION: &str = "fixture-r1";

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

    const fn qualification_stable_window_seconds(self) -> i64 {
        match self {
            Self::LoggerTtl => 300,
            Self::ProxyScaleOut => 600,
            Self::ProxyRestartOne => 900,
            Self::TelemetryCollectorRestartOne => 600,
        }
    }

    const fn execution_stable_window_seconds(self) -> i64 {
        match self {
            Self::LoggerTtl => 30,
            Self::ProxyScaleOut => 120,
            Self::ProxyRestartOne | Self::TelemetryCollectorRestartOne => 180,
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

#[derive(Serialize)]
struct AutonomyLifecycleOutcome {
    id: String,
    initial_mode: &'static str,
    final_mode: &'static str,
    shadow_samples: u32,
    supervised_successes: u32,
    observation_window_days: i64,
    shadow_cohorts: u8,
    supervised_cohorts: u8,
    same_family_critic_denied: bool,
    autonomous_transition_executed: bool,
    expected_deny_paused: bool,
    execution_failure_paused: bool,
    critic_transport: &'static str,
    primary_model_family: &'static str,
    critic_model_family: &'static str,
}

#[derive(Serialize)]
struct AutonomyLifecycleFragment {
    schema_version: &'static str,
    live_mode_ceiling: &'static str,
    unattended_autonomous_execution: bool,
    model_provider_network_calls: u8,
    actions: Vec<AutonomyLifecycleOutcome>,
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL and an explicit machine-local fragment path"]
async fn all_r1_actions_persist_shadow_and_supervised_qualification_matrix() {
    let Ok(fragment_path) = std::env::var("ROCKETMQ_SRE_AUTONOMY_QUALIFICATION_FRAGMENT") else {
        return;
    };
    let path = Path::new(&fragment_path);
    let rendered = path.to_string_lossy();
    assert!(
        path.is_absolute(),
        "autonomy qualification fragment path must be absolute"
    );
    assert!(
        rendered.starts_with("D:\\") || rendered.starts_with("F:\\"),
        "autonomy qualification fragment must use the D or F drive"
    );

    let scenarios = [
        R1ActionScenario::LoggerTtl,
        R1ActionScenario::ProxyScaleOut,
        R1ActionScenario::ProxyRestartOne,
        R1ActionScenario::TelemetryCollectorRestartOne,
    ];
    let mut outcomes = Vec::with_capacity(scenarios.len());
    for scenario in scenarios {
        outcomes.push(
            qualify_r1_action(scenario, false)
                .await
                .expect("qualification database must be configured"),
        );
    }
    let fragment = AutonomyLifecycleFragment {
        schema_version: "rocketmq-sre.autonomy-action-lifecycle-fragment.v1",
        live_mode_ceiling: "supervised",
        unattended_autonomous_execution: false,
        model_provider_network_calls: 0,
        actions: outcomes,
    };
    let mut bytes = serde_json::to_vec_pretty(&fragment).expect("serialize autonomy lifecycle fragment");
    bytes.push(b'\n');
    fs::write(path, bytes).expect("write autonomy lifecycle fragment");
}

async fn qualify_r1_action_through_autonomy(scenario: R1ActionScenario) {
    let _ = qualify_r1_action(scenario, true).await;
}

async fn qualify_r1_action(
    scenario: R1ActionScenario,
    promote_to_autonomous: bool,
) -> Option<AutonomyLifecycleOutcome> {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok()?;
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
                stable_window_seconds: scenario.qualification_stable_window_seconds() as u64,
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
                primary_model_family: PRIMARY_MODEL_FAMILY.to_owned(),
                primary_model_revision: PRIMARY_MODEL_REVISION.to_owned(),
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
    let qualified_shadow_samples = supervised.qualification.qualified_shadow_samples;

    let primary_plan = &plans[0];
    let (base_review_id, critic_invocation_id, critic_profile) =
        seed_qualification_critic_review(&repository, primary_plan).await;
    let mut critic_reviews = vec![base_review_id];
    for plan in plans.iter().take(SUPERVISED_SUCCESSES).skip(1) {
        critic_reviews.push(seed_shared_critic_review(&repository, plan, critic_invocation_id, &critic_profile).await);
    }
    assert!(
        service
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
                    primary_model_family: PRIMARY_MODEL_FAMILY.to_owned(),
                    primary_model_revision: PRIMARY_MODEL_REVISION.to_owned(),
                    critic_profile: critic_profile.clone(),
                    critic_model_family: PRIMARY_MODEL_FAMILY.to_owned(),
                    critic_model_revision: PRIMARY_MODEL_REVISION.to_owned(),
                },
            )
            .await
            .is_err(),
        "same-family Critic must fail closed"
    );
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
                primary_model_family: PRIMARY_MODEL_FAMILY.to_owned(),
                primary_model_revision: PRIMARY_MODEL_REVISION.to_owned(),
                critic_profile,
                critic_model_family: CRITIC_MODEL_FAMILY.to_owned(),
                critic_model_revision: CRITIC_MODEL_REVISION.to_owned(),
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
            scenario.qualification_stable_window_seconds(),
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
    if !promote_to_autonomous {
        let qualified = service.scope(&auth, &query).await.expect("qualified Supervised scope");
        assert_eq!(qualified.lifecycle.mode, AutonomyMode::Supervised);
        assert_eq!(
            qualified.qualification.qualified_supervised_successes,
            SUPERVISED_SUCCESSES as u32
        );
        assert!(qualified.qualification.autonomous_observation_window_met);

        let deny_plan = &plans[SUPERVISED_SUCCESSES];
        let denied_at = autonomous_cohort.created_at + Duration::days(OBSERVATION_DAYS) + Duration::seconds(31);
        repository
            .record_autonomy_outcome(
                &AutonomyOutcome {
                    id: AutonomyOutcomeId::new(),
                    tenant_id: fixture.tenant_id,
                    cluster_id: fixture.cluster_id,
                    action: scenario.action(),
                    action_version: "1.0.0".to_owned(),
                    incident_id: deny_plan.incident_id,
                    plan_id: deny_plan.plan_id,
                    plan_hash: deny_plan.plan_hash.clone(),
                    execution_id: None,
                    cohort_id: Some(autonomous_cohort.id),
                    class: AutonomyOutcomeClass::ExpectedDeny,
                    failure: None,
                    reason_codes: vec!["freeze_active".to_owned()],
                    first_positive_intent_persisted: false,
                    occurred_at: denied_at,
                    reconciled_at: denied_at,
                },
                "autonomy-qualification-reconciler",
            )
            .await
            .expect("persist ExpectedDeny outcome");
        let after_deny = service.scope(&auth, &query).await.expect("scope after ExpectedDeny");
        assert_eq!(after_deny.lifecycle.mode, AutonomyMode::Supervised);

        let failure_plan = &plans[SUPERVISED_SUCCESSES + 1];
        let failed_at = denied_at + Duration::seconds(1);
        repository
            .record_autonomy_outcome(
                &AutonomyOutcome {
                    id: AutonomyOutcomeId::new(),
                    tenant_id: fixture.tenant_id,
                    cluster_id: fixture.cluster_id,
                    action: scenario.action(),
                    action_version: "1.0.0".to_owned(),
                    incident_id: failure_plan.incident_id,
                    plan_id: failure_plan.plan_id,
                    plan_hash: failure_plan.plan_hash.clone(),
                    execution_id: None,
                    cohort_id: Some(autonomous_cohort.id),
                    class: AutonomyOutcomeClass::AutonomousExecutionFailure,
                    failure: Some(AutonomousExecutionFailure::VerificationFailed),
                    reason_codes: vec!["qualification_failure_fixture".to_owned()],
                    first_positive_intent_persisted: true,
                    occurred_at: failed_at,
                    reconciled_at: failed_at,
                },
                "autonomy-qualification-reconciler",
            )
            .await
            .expect("persist failure and pause lifecycle");
        let paused = service.scope(&auth, &query).await.expect("paused scope");
        assert_eq!(paused.lifecycle.mode, AutonomyMode::Paused);
        assert_eq!(paused.lifecycle.previous_mode, Some(AutonomyMode::Supervised));

        set_clock(&clock_value, failed_at + Duration::seconds(1));
        let resumed = service
            .transition(
                &auth,
                &query,
                &AutonomyTransitionRequest {
                    target_mode: AutonomyMode::Supervised,
                    reason: Some("owner reviewed bounded qualification failure".to_owned()),
                    owner_confirmed: true,
                },
            )
            .await
            .expect("human owner resumes Supervised mode");
        assert_eq!(resumed.lifecycle.mode, AutonomyMode::Supervised);

        return Some(AutonomyLifecycleOutcome {
            id: scenario.action().id().to_owned(),
            initial_mode: "disabled",
            final_mode: "supervised",
            shadow_samples: qualified_shadow_samples,
            supervised_successes: qualified.qualification.qualified_supervised_successes,
            observation_window_days: OBSERVATION_DAYS,
            shadow_cohorts: 1,
            supervised_cohorts: 1,
            same_family_critic_denied: true,
            autonomous_transition_executed: false,
            expected_deny_paused: false,
            execution_failure_paused: true,
            critic_transport: "offline_scripted",
            primary_model_family: PRIMARY_MODEL_FAMILY,
            critic_model_family: CRITIC_MODEL_FAMILY,
        });
    }
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
    Some(AutonomyLifecycleOutcome {
        id: scenario.action().id().to_owned(),
        initial_mode: "disabled",
        final_mode: "autonomous",
        shadow_samples: autonomous.qualification.qualified_shadow_samples,
        supervised_successes: autonomous.qualification.qualified_supervised_successes,
        observation_window_days: OBSERVATION_DAYS,
        shadow_cohorts: 1,
        supervised_cohorts: 1,
        same_family_critic_denied: true,
        autonomous_transition_executed: true,
        expected_deny_paused: false,
        execution_failure_paused: false,
        critic_transport: "offline_scripted",
        primary_model_family: PRIMARY_MODEL_FAMILY,
        critic_model_family: CRITIC_MODEL_FAMILY,
    })
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
            $6, 'offline-scripted', 'qualification-primary', 'fixture-r1',
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
                stable_window_seconds: scenario.execution_stable_window_seconds() as u64,
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
                stable_window_seconds: scenario.execution_stable_window_seconds() as u64,
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
                stable_window_seconds: scenario.execution_stable_window_seconds() as u64,
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
                stable_window_seconds: scenario.execution_stable_window_seconds() as u64,
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
            'qualification-primary', 'qualification-critic',
            'offline-scripted', $7, 'fixture-r1',
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

async fn seed_qualification_critic_review(
    repository: &PostgresRepository,
    fixture: &Fixture,
) -> (CriticReviewId, rocketmq_sre_contracts::ModelInvocationId, String) {
    let profile_id = Uuid::new_v4();
    let profile_name = format!("qualification-critic-{profile_id}");
    sqlx::query(
        "INSERT INTO model_profiles (
            id, tenant_id, profile_name, provider_family, protocol_family,
            model_family, model_name, model_revision, endpoint_instance,
            region, data_residency, data_classes, capabilities, priority,
            credential_ref, credential_owner, enabled, health, created_at, updated_at
         ) VALUES (
            $1, $2, $3, 'offline-scripted', 'fixture',
            'qualification-critic', 'qualification-critic', 'fixture-r1', 'local',
            'local', 'local', '[]'::JSONB, '{}'::JSONB, 90,
            'offline-fixture-reference', 'gateway', TRUE, 'healthy', NOW(), NOW()
         )",
    )
    .bind(profile_id)
    .bind(fixture.tenant_id.as_uuid())
    .bind(&profile_name)
    .execute(&repository.pool)
    .await
    .expect("offline Critic profile");

    let critic_invocation_id = rocketmq_sre_contracts::ModelInvocationId::new();
    sqlx::query(
        "INSERT INTO model_invocations (
            id, tenant_id, cluster_id, incident_id, diagnosis_revision_id,
            parent_invocation_id, purpose, requested_profile_id,
            actual_profile_id, provider_family, model_family, model_revision,
            endpoint_instance, fallback_chain, prompt_version, schema_version,
            rationale, started_at, completed_at
         ) VALUES (
            $1, $2, $3, $4, $5, $6, 'critic', $7,
            $7, 'offline-scripted', 'qualification-critic', 'fixture-r1',
            'local', '{}', 'autonomy-qualification-fixture',
            'rocketmq-sre.critic.v1', 'offline qualification fixture', NOW(), NOW()
         )",
    )
    .bind(critic_invocation_id.as_uuid())
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .bind(fixture.incident_id.as_uuid())
    .bind(fixture.diagnosis_revision_id.as_uuid())
    .bind(fixture.primary_invocation_id.as_uuid())
    .bind(profile_id)
    .execute(&repository.pool)
    .await
    .expect("offline Critic invocation");

    let critic_review_id = CriticReviewId::new();
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
            'qualification-primary', 'qualification-critic',
            'offline-scripted', $7, 'fixture-r1',
            'local', '{}', 'autonomy-qualification-fixture',
            'rocketmq-sre.critic.v1', $8, 'accept', 'valid',
            $9, '{}'::JSONB, NOW()
         )",
    )
    .bind(critic_review_id.as_uuid())
    .bind(fixture.plan_id.as_uuid())
    .bind(&fixture.plan_hash)
    .bind(fixture.diagnosis_revision_id.as_uuid())
    .bind(fixture.primary_invocation_id.as_uuid())
    .bind(critic_invocation_id.as_uuid())
    .bind(&profile_name)
    .bind(unique_digest())
    .bind(unique_digest())
    .execute(&repository.pool)
    .await
    .expect("offline heterogeneous Critic review");
    (critic_review_id, critic_invocation_id, profile_name)
}
