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

use chrono::Duration;
use chrono::Timelike;
use chrono::Utc;
use rocketmq_sre_contracts::ActionPlanId;
use rocketmq_sre_contracts::AutonomousExecutionFailure;
use rocketmq_sre_contracts::AutonomyGrant;
use rocketmq_sre_contracts::AutonomyMode;
use rocketmq_sre_contracts::AutonomyOutcome;
use rocketmq_sre_contracts::AutonomyOutcomeClass;
use rocketmq_sre_contracts::AutonomyOutcomeId;
use rocketmq_sre_contracts::AutonomyPolicyDefinition;
use rocketmq_sre_contracts::AutonomyPolicyId;
use rocketmq_sre_contracts::AutonomyQualificationSample;
use rocketmq_sre_contracts::AutonomySampleId;
use rocketmq_sre_contracts::AutonomySampleKind;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CriticReviewId;
use rocketmq_sre_contracts::DiagnosisRevisionId;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::ModelInvocationId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_core::ActualModelIdentity;
use rocketmq_sre_core::AutonomyActor;
use rocketmq_sre_core::AutonomyPolicy;
use rocketmq_sre_core::AutonomyStateMachine;
use rocketmq_sre_core::PromotionQualification;
use uuid::Uuid;

use super::AutonomyPauseReconciler;
use crate::PostgresRepository;
use crate::execution_authority::LeaseAuthorityRepository;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_autonomy_state_cohorts_and_controls_are_durable_and_idempotent() {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 5)
        .await
        .expect("repository with migrations");
    let fixture = seed_fixture(&repository).await;
    let now = (Utc::now() - Duration::seconds(30))
        .with_nanosecond(0)
        .expect("whole-second timestamp");
    let policy = policy(&fixture, now);

    let (stored, initial) = repository
        .store_autonomy_policy(policy, "autonomy-owner")
        .await
        .expect("policy");
    assert_eq!(stored.definition_version, 1);
    assert_eq!(initial.mode, AutonomyMode::Disabled);
    assert_eq!(initial.lifecycle_revision, 1);

    let shadow = AutonomyStateMachine::transition(
        &initial,
        AutonomyMode::Shadow,
        AutonomyActor::HumanOperator,
        "autonomy-owner",
        None,
        PromotionQualification::default(),
        now + Duration::seconds(1),
    )
    .expect("Shadow transition");
    repository
        .update_autonomy_lifecycle(
            &initial,
            &shadow,
            &stored.action_version,
            false,
            "repository_test_shadow",
        )
        .await
        .expect("persist Shadow");

    let candidate = AutonomyPolicy::shadow_cohort(
        &stored,
        &ActualModelIdentity {
            profile: fixture.primary_profile.clone(),
            model_family: "deepseek".to_owned(),
            model_revision: "v3.2".to_owned(),
        },
        now + Duration::seconds(2),
    )
    .expect("cohort");
    let cohort = repository
        .store_autonomy_cohort(stored.id, &candidate)
        .await
        .expect("store cohort");
    let duplicate_candidate = AutonomyPolicy::shadow_cohort(
        &stored,
        &ActualModelIdentity {
            profile: fixture.primary_profile.clone(),
            model_family: "deepseek".to_owned(),
            model_revision: "v3.2".to_owned(),
        },
        candidate.created_at,
    )
    .expect("duplicate cohort");
    let duplicate = repository
        .store_autonomy_cohort(stored.id, &duplicate_candidate)
        .await
        .expect("idempotent cohort");
    assert_eq!(duplicate.id, cohort.id);

    let sample = AutonomyQualificationSample {
        id: AutonomySampleId::new(),
        cohort_id: cohort.id,
        kind: AutonomySampleKind::ShadowOutcome,
        incident_id: fixture.incident_id,
        plan_id: fixture.plan_id,
        plan_hash: fixture.plan_hash.clone(),
        execution_id: None,
        qualified: true,
        reason_codes: Vec::new(),
        human_outcome_linked: true,
        evidence_complete: true,
        stable_window_passed: true,
        observed_at: now + Duration::seconds(3),
        reconciled_at: now + Duration::seconds(4),
    };
    let first = repository
        .store_qualification_sample(&sample)
        .await
        .expect("qualification sample");
    let mut retry = sample.clone();
    retry.id = AutonomySampleId::new();
    let retried = repository
        .store_qualification_sample(&retry)
        .await
        .expect("idempotent sample retry");
    assert_eq!(retried.id, first.id);
    retry.qualified = false;
    assert!(repository.store_qualification_sample(&retry).await.is_err());

    let shadow_scope = repository
        .autonomy_scope(
            fixture.tenant_id,
            fixture.cluster_id,
            ExecutionAction::ObservabilityLoggerLevelTtl,
            "1.0.0",
        )
        .await
        .expect("qualified Shadow scope");
    let supervised = AutonomyStateMachine::transition(
        &shadow_scope.lifecycle,
        AutonomyMode::Supervised,
        AutonomyActor::HumanOperator,
        "autonomy-owner",
        None,
        PromotionQualification {
            shadow_qualified: true,
            owner_confirmed: true,
            ..PromotionQualification::default()
        },
        now + Duration::seconds(5),
    )
    .expect("Supervised transition");
    repository
        .update_autonomy_lifecycle(
            &shadow_scope.lifecycle,
            &supervised,
            &stored.action_version,
            true,
            "repository_test_supervised",
        )
        .await
        .expect("persist Supervised");
    let (critic_review_id, critic_invocation_id, critic_profile) = seed_critic_review(&repository, &fixture).await;
    let autonomous_candidate = AutonomyPolicy::autonomous_cohort(
        &stored,
        &ActualModelIdentity {
            profile: fixture.primary_profile.clone(),
            model_family: "deepseek".to_owned(),
            model_revision: "v3.2".to_owned(),
        },
        &ActualModelIdentity {
            profile: critic_profile,
            model_family: "glm".to_owned(),
            model_revision: "glm-5".to_owned(),
        },
        now + Duration::seconds(6),
    )
    .expect("Autonomous cohort");
    let autonomous_cohort = repository
        .store_autonomy_cohort(stored.id, &autonomous_candidate)
        .await
        .expect("store Autonomous cohort");
    let execution_id = seed_successful_supervised_execution(&repository, &fixture, 300).await;
    let execution_facts = repository
        .supervised_execution_qualification(
            fixture.tenant_id,
            fixture.cluster_id,
            ExecutionAction::ObservabilityLoggerLevelTtl,
            fixture.incident_id,
            fixture.plan_id,
            &fixture.plan_hash,
            execution_id,
            300,
        )
        .await
        .expect("authoritative supervised facts");
    assert!(execution_facts.succeeded);
    assert!(execution_facts.human_approved);
    assert!(execution_facts.timeline_safe);
    assert!(execution_facts.evidence_complete);
    assert!(execution_facts.stable_window_passed);
    let supervised_sample = AutonomyQualificationSample {
        id: AutonomySampleId::new(),
        cohort_id: autonomous_cohort.id,
        kind: AutonomySampleKind::SupervisedSuccess,
        incident_id: fixture.incident_id,
        plan_id: fixture.plan_id,
        plan_hash: fixture.plan_hash.clone(),
        execution_id: Some(execution_id),
        qualified: true,
        reason_codes: Vec::new(),
        human_outcome_linked: true,
        evidence_complete: true,
        stable_window_passed: true,
        observed_at: execution_facts.observed_at,
        reconciled_at: execution_facts.observed_at + Duration::seconds(1),
    };
    let stored_supervised = repository
        .store_qualification_sample(&supervised_sample)
        .await
        .expect("Supervised sample");
    let mut supervised_retry = supervised_sample;
    supervised_retry.id = AutonomySampleId::new();
    supervised_retry.reconciled_at += Duration::seconds(1);
    let stored_retry = repository
        .store_qualification_sample(&supervised_retry)
        .await
        .expect("idempotent supervised execution retry");
    assert_eq!(stored_retry.id, stored_supervised.id);
    let supervised_scope = repository
        .autonomy_scope(
            fixture.tenant_id,
            fixture.cluster_id,
            ExecutionAction::ObservabilityLoggerLevelTtl,
            "1.0.0",
        )
        .await
        .expect("qualified Supervised scope");
    assert_eq!(supervised_scope.qualification.qualified_supervised_successes, 1);
    assert!(!supervised_scope.qualification.autonomous_observation_window_met);
    let autonomous = AutonomyStateMachine::transition(
        &supervised_scope.lifecycle,
        AutonomyMode::Autonomous,
        AutonomyActor::HumanOperator,
        "autonomy-owner",
        None,
        PromotionQualification {
            autonomous_qualified: true,
            critic_ready: true,
            owner_confirmed: true,
            ..PromotionQualification::default()
        },
        now + Duration::seconds(9),
    )
    .expect("Autonomous transition");
    repository
        .update_autonomy_lifecycle(
            &supervised_scope.lifecycle,
            &autonomous,
            &stored.action_version,
            true,
            "repository_test_autonomous",
        )
        .await
        .expect("persist Autonomous");
    let grant = AutonomyGrant {
        issuer: "rocketmq-sre-control-plane".to_owned(),
        audience: "rocketmq-sre-executor".to_owned(),
        plan_id: fixture.plan_id,
        plan_hash: fixture.plan_hash.clone(),
        diagnosis_revision_id: fixture.diagnosis_revision_id,
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        action: ExecutionAction::ObservabilityLoggerLevelTtl,
        action_version: stored.action_version.clone(),
        policy_id: stored.id,
        policy_definition_version: stored.definition_version,
        lifecycle_revision: autonomous.lifecycle_revision,
        autonomous_cohort_id: autonomous_cohort.id,
        autonomous_cohort_hash: autonomous_cohort.cohort_hash.clone(),
        critic_review_id,
        primary_model_invocation_id: fixture.primary_invocation_id,
        critic_model_invocation_id: critic_invocation_id,
        issued_at: now + Duration::seconds(9),
        expires_at: now + Duration::minutes(2),
        nonce: "authority-repository-grant".to_owned(),
        signature: "verified-separately".to_owned(),
    };
    let authority_repository = LeaseAuthorityRepository::new(repository.pool.clone());
    authority_repository
        .autonomy_grant_is_current(&grant)
        .await
        .expect("current R1 autonomy grant");
    let outcome = AutonomyOutcome {
        id: AutonomyOutcomeId::new(),
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        action: ExecutionAction::ObservabilityLoggerLevelTtl,
        action_version: "1.0.0".to_owned(),
        incident_id: fixture.incident_id,
        plan_id: fixture.plan_id,
        plan_hash: fixture.plan_hash.clone(),
        execution_id: None,
        cohort_id: Some(autonomous_cohort.id),
        class: AutonomyOutcomeClass::AutonomousExecutionFailure,
        failure: Some(AutonomousExecutionFailure::VerificationFailed),
        reason_codes: vec!["verification_failed".to_owned()],
        first_positive_intent_persisted: true,
        occurred_at: now + Duration::seconds(10),
        reconciled_at: now + Duration::seconds(11),
    };
    repository
        .record_autonomy_outcome(&outcome, "autonomy-pause-reconciler")
        .await
        .expect("failure outcome");
    let mut outcome_retry = outcome.clone();
    outcome_retry.id = AutonomyOutcomeId::new();
    repository
        .record_autonomy_outcome(&outcome_retry, "autonomy-pause-reconciler")
        .await
        .expect("idempotent failure outcome");
    assert!(authority_repository.autonomy_grant_is_current(&grant).await.is_err());

    let tenant_freeze = repository
        .set_autonomy_freeze(
            fixture.tenant_id,
            None,
            None,
            None,
            true,
            "organization maintenance",
            now,
            Some(now + Duration::hours(1)),
            "autonomy-owner",
        )
        .await
        .expect("tenant freeze");
    assert!(tenant_freeze.cluster_id.is_none());
    let action_freeze = repository
        .set_autonomy_freeze(
            fixture.tenant_id,
            Some(fixture.cluster_id),
            Some(ExecutionAction::ObservabilityLoggerLevelTtl),
            Some("1.0.0"),
            true,
            "action maintenance",
            now,
            Some(now + Duration::hours(1)),
            "autonomy-owner",
        )
        .await
        .expect("action freeze");
    assert_eq!(action_freeze.revision, 1);
    let action_freeze_updated = repository
        .set_autonomy_freeze(
            fixture.tenant_id,
            Some(fixture.cluster_id),
            Some(ExecutionAction::ObservabilityLoggerLevelTtl),
            Some("1.0.0"),
            false,
            "maintenance complete",
            now,
            None,
            "autonomy-owner",
        )
        .await
        .expect("freeze update");
    assert_eq!(action_freeze_updated.revision, 2);
    assert!(!action_freeze_updated.active);
    let kill_switch = repository
        .set_autonomy_kill_switch(
            fixture.tenant_id,
            fixture.cluster_id,
            ExecutionAction::ObservabilityLoggerLevelTtl,
            "1.0.0",
            true,
            "operator emergency stop",
            "autonomy-owner",
        )
        .await
        .expect("kill switch");
    assert!(kill_switch.active);

    let scope = repository
        .autonomy_scope(
            fixture.tenant_id,
            fixture.cluster_id,
            ExecutionAction::ObservabilityLoggerLevelTtl,
            "1.0.0",
        )
        .await
        .expect("scope");
    assert_eq!(scope.lifecycle.mode, AutonomyMode::Paused);
    assert_eq!(scope.lifecycle.previous_mode, Some(AutonomyMode::Autonomous));
    assert_eq!(scope.qualification.qualified_shadow_samples, 1);
    assert_eq!(scope.active_freezes.len(), 1);
    assert!(scope.kill_switch.is_some_and(|state| state.active));
    let pause_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM autonomy_outbox
         WHERE outcome_id = $1 AND event_kind = 'autonomy_paused'",
    )
    .bind(outcome.id.as_uuid())
    .fetch_one(&repository.pool)
    .await
    .expect("pause outbox count");
    assert_eq!(pause_events, 1);

    let mut revised_policy = stored;
    revised_policy.created_at = now + Duration::seconds(12);
    let (revised, revised_lifecycle) = repository
        .store_autonomy_policy(revised_policy, "autonomy-owner")
        .await
        .expect("policy revision");
    assert_eq!(revised.definition_version, 2);
    assert_eq!(revised_lifecycle.mode, AutonomyMode::Paused);
    assert_eq!(revised_lifecycle.lifecycle_revision, 6);
    let revised_scope = repository
        .autonomy_scope(
            fixture.tenant_id,
            fixture.cluster_id,
            ExecutionAction::ObservabilityLoggerLevelTtl,
            "1.0.0",
        )
        .await
        .expect("revised scope");
    assert_eq!(revised_scope.qualification.qualified_shadow_samples, 0);
    assert!(
        sqlx::query(
            "UPDATE autonomy_qualification_samples
             SET qualified = FALSE
             WHERE id = $1",
        )
        .bind(first.id.as_uuid())
        .execute(&repository.pool)
        .await
        .is_err()
    );
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_pause_reconciler_repairs_a_dropped_failure_event_once() {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 5)
        .await
        .expect("repository with migrations");
    let fixture = seed_fixture(&repository).await;
    let now = Utc::now().with_nanosecond(0).expect("whole-second timestamp");
    let (stored, lifecycle) = repository
        .store_autonomy_policy(policy(&fixture, now), "autonomy-owner")
        .await
        .expect("policy");
    assert_eq!(lifecycle.mode, AutonomyMode::Disabled);
    sqlx::query(
        "UPDATE autonomy_lifecycle_states
         SET mode = 'autonomous',
             previous_mode = NULL,
             pause_reason = NULL,
             lifecycle_revision = lifecycle_revision + 1,
             updated_by = 'repository-test-fixture',
             updated_at = $5
         WHERE tenant_id = $1 AND cluster_id = $2
           AND action_id = $3 AND action_version = $4",
    )
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .bind(ExecutionAction::ObservabilityLoggerLevelTtl.id())
    .bind(&stored.action_version)
    .bind(now + Duration::seconds(1))
    .execute(&repository.pool)
    .await
    .expect("simulate an autonomous lifecycle");

    let outcome = AutonomyOutcome {
        id: AutonomyOutcomeId::new(),
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        action: ExecutionAction::ObservabilityLoggerLevelTtl,
        action_version: stored.action_version,
        incident_id: fixture.incident_id,
        plan_id: fixture.plan_id,
        plan_hash: fixture.plan_hash.clone(),
        execution_id: None,
        cohort_id: None,
        class: AutonomyOutcomeClass::AutonomousExecutionFailure,
        failure: Some(AutonomousExecutionFailure::UnknownEffect),
        reason_codes: vec!["unknown_effect".to_owned()],
        first_positive_intent_persisted: true,
        occurred_at: now + Duration::seconds(2),
        reconciled_at: now + Duration::seconds(3),
    };
    sqlx::query(
        "INSERT INTO autonomy_outcomes (
            id, tenant_id, cluster_id, action_id, action_version,
            incident_id, plan_id, plan_hash, execution_id, cohort_id,
            outcome_class, failure_code, reason_codes,
            first_positive_intent_persisted, outcome_snapshot,
            occurred_at, reconciled_at
         ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, NULL, NULL,
            'autonomous_execution_failure', 'unknown_effect', $9,
            TRUE, $10, $11, $12
         )",
    )
    .bind(outcome.id.as_uuid())
    .bind(outcome.tenant_id.as_uuid())
    .bind(outcome.cluster_id.as_uuid())
    .bind(outcome.action.id())
    .bind(&outcome.action_version)
    .bind(outcome.incident_id.as_uuid())
    .bind(outcome.plan_id.as_uuid())
    .bind(&outcome.plan_hash)
    .bind(&outcome.reason_codes)
    .bind(serde_json::to_value(&outcome).expect("outcome snapshot"))
    .bind(outcome.occurred_at)
    .bind(outcome.reconciled_at)
    .execute(&repository.pool)
    .await
    .expect("simulate a committed outcome with a dropped pause event");

    let reconciler = AutonomyPauseReconciler::new(repository.clone());
    let first = reconciler.run_once().await.expect("first reconciliation");
    assert_eq!(first.candidates, 1);
    assert_eq!(first.repaired, 1);
    let scope = repository
        .autonomy_scope(
            fixture.tenant_id,
            fixture.cluster_id,
            ExecutionAction::ObservabilityLoggerLevelTtl,
            &outcome.action_version,
        )
        .await
        .expect("reconciled scope");
    assert_eq!(scope.lifecycle.mode, AutonomyMode::Paused);
    assert_eq!(scope.lifecycle.previous_mode, Some(AutonomyMode::Autonomous));
    let pause_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM autonomy_outbox
         WHERE outcome_id = $1 AND event_kind = 'autonomy_paused'",
    )
    .bind(outcome.id.as_uuid())
    .fetch_one(&repository.pool)
    .await
    .expect("pause outbox count");
    assert_eq!(pause_events, 1);

    let retry = reconciler.run_once().await.expect("idempotent reconciliation");
    assert_eq!(retry.candidates, 0);
    assert_eq!(retry.repaired, 0);
    let pause_events_after_retry: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM autonomy_outbox
         WHERE outcome_id = $1 AND event_kind = 'autonomy_paused'",
    )
    .bind(outcome.id.as_uuid())
    .fetch_one(&repository.pool)
    .await
    .expect("pause outbox count after retry");
    assert_eq!(pause_events_after_retry, 1);
}

#[derive(Clone)]
pub(super) struct Fixture {
    pub(super) tenant_id: TenantId,
    pub(super) cluster_id: ClusterId,
    pub(super) incident_id: IncidentId,
    pub(super) diagnosis_revision_id: DiagnosisRevisionId,
    pub(super) primary_invocation_id: ModelInvocationId,
    pub(super) primary_profile: String,
    pub(super) plan_id: ActionPlanId,
    pub(super) plan_hash: String,
}

pub(super) async fn seed_fixture(repository: &PostgresRepository) -> Fixture {
    let profile_id = Uuid::new_v4();
    let fixture = Fixture {
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        incident_id: IncidentId::new(),
        diagnosis_revision_id: DiagnosisRevisionId::new(),
        primary_invocation_id: ModelInvocationId::new(),
        primary_profile: format!("autonomy-repository-{profile_id}"),
        plan_id: ActionPlanId::new(),
        plan_hash: unique_digest(),
    };
    sqlx::query(
        "INSERT INTO clusters (
            id, tenant_id, external_cluster_key, environment, region,
            rocketmq_version, deployment_mode, owner_name,
            requested_access_profile, effective_access_profile, onboarding_state
         ) VALUES ($1, $2, $3, 'test', 'local', 'test', 'docker',
                   'autonomy-repository-test', 'read_only', 'read_only', 'ready_read_only')",
    )
    .bind(fixture.cluster_id.as_uuid())
    .bind(fixture.tenant_id.as_uuid())
    .bind(format!("autonomy-repository-{}", fixture.cluster_id))
    .execute(&repository.pool)
    .await
    .expect("cluster");
    sqlx::query(
        "INSERT INTO sre_incidents (
            id, tenant_id, cluster_id, title, resource, symptom_family,
            fingerprint, status, workflow_checkpoint, created_by_subject,
            created_at, updated_at
         ) VALUES ($1, $2, $3, 'Autonomy repository test', 'broker/test',
                   'autonomy', $4, 'diagnosing', '{}'::JSONB,
                   'autonomy-repository-test', NOW(), NOW())",
    )
    .bind(fixture.incident_id.as_uuid())
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .bind(unique_digest())
    .execute(&repository.pool)
    .await
    .expect("incident");
    sqlx::query(
        "INSERT INTO diagnosis_revisions (
            id, incident_id, revision, status, rule_result, hypotheses,
            evidence_ids, primary_model_invocation_id,
            execution_eligible, partial, created_at
         ) VALUES ($1, $2, 1, 'confirmed', '{}'::JSONB, '[]'::JSONB,
                   '{}', NULL, FALSE, FALSE, NOW())",
    )
    .bind(fixture.diagnosis_revision_id.as_uuid())
    .bind(fixture.incident_id.as_uuid())
    .execute(&repository.pool)
    .await
    .expect("diagnosis");
    sqlx::query(
        "INSERT INTO model_profiles (
            id, tenant_id, profile_name, provider_family, protocol_family,
            model_family, model_name, model_revision, endpoint_instance,
            region, data_residency, data_classes, capabilities, priority,
            credential_ref, credential_owner, enabled, health, created_at, updated_at
         ) VALUES ($1, $2, $3, 'openai-compatible', 'openai-compatible',
                   'deepseek', 'deepseek-v3', 'v3.2', 'local',
                   'local', 'local', '[]'::JSONB, '{}'::JSONB, 100,
                   'test-reference', 'gateway', TRUE, 'healthy', NOW(), NOW())",
    )
    .bind(profile_id)
    .bind(fixture.tenant_id.as_uuid())
    .bind(&fixture.primary_profile)
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
                   'openai-compatible', 'deepseek', 'v3.2', 'local',
                   '{}', 'autonomy-repository-test', 'rocketmq-sre.model.v1',
                   'autonomy repository fixture', NOW(), NOW())",
    )
    .bind(fixture.primary_invocation_id.as_uuid())
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .bind(fixture.incident_id.as_uuid())
    .bind(fixture.diagnosis_revision_id.as_uuid())
    .bind(profile_id)
    .execute(&repository.pool)
    .await
    .expect("model invocation");
    sqlx::query(
        "UPDATE diagnosis_revisions
         SET primary_model_invocation_id = $2, execution_eligible = TRUE
         WHERE id = $1",
    )
    .bind(fixture.diagnosis_revision_id.as_uuid())
    .bind(fixture.primary_invocation_id.as_uuid())
    .execute(&repository.pool)
    .await
    .expect("executable diagnosis");
    sqlx::query(
        "INSERT INTO action_plans (
            id, tenant_id, cluster_id, incident_id, diagnosis_revision_id,
            primary_model_invocation_id, version, plan_hash, evidence_hash,
            risk, status, request_snapshot, created_by, created_at, expires_at,
            submitted_at
         ) VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $8,
                   'r1', 'ready_for_approval', '{}'::JSONB, 'autonomy-operator',
                   NOW(), NOW() + INTERVAL '1 hour', NOW())",
    )
    .bind(fixture.plan_id.as_uuid())
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .bind(fixture.incident_id.as_uuid())
    .bind(fixture.diagnosis_revision_id.as_uuid())
    .bind(fixture.primary_invocation_id.as_uuid())
    .bind(&fixture.plan_hash)
    .bind(unique_digest())
    .execute(&repository.pool)
    .await
    .expect("action plan");
    fixture
}

pub(super) async fn seed_successful_supervised_execution(
    repository: &PostgresRepository,
    fixture: &Fixture,
    stable_window_seconds: i64,
) -> ExecutionId {
    seed_successful_supervised_execution_at(
        repository,
        fixture,
        stable_window_seconds,
        Utc::now() - Duration::seconds(2),
    )
    .await
}

pub(super) async fn seed_successful_supervised_execution_at(
    repository: &PostgresRepository,
    fixture: &Fixture,
    stable_window_seconds: i64,
    observed_at: chrono::DateTime<Utc>,
) -> ExecutionId {
    let execution_id = ExecutionId::new();
    let step_id = Uuid::new_v4();
    let existing_lease: Option<(Uuid, i64)> = sqlx::query_as(
        "SELECT id, epoch
         FROM executor_leases
         WHERE cluster_id = $1 AND state = 'active'",
    )
    .bind(fixture.cluster_id.as_uuid())
    .fetch_optional(&repository.pool)
    .await
    .expect("active qualification lease");
    let started_at = observed_at - Duration::seconds(stable_window_seconds + 1);
    let (lease_id, lease_epoch) = match existing_lease {
        Some(lease) => lease,
        None => {
            let lease_id = Uuid::new_v4();
            let lease_epoch: i64 = sqlx::query_scalar(
                "SELECT COALESCE(MAX(epoch), 0) + 1
                 FROM executor_leases
                 WHERE cluster_id = $1",
            )
            .bind(fixture.cluster_id.as_uuid())
            .fetch_one(&repository.pool)
            .await
            .expect("next qualification lease epoch");
            sqlx::query(
                "INSERT INTO executor_leases (
                    id, tenant_id, cluster_id, epoch, owner, state, pending_nonce,
                    fence_ack_snapshot, acquired_at, activated_at, expires_at, updated_at
                 ) VALUES (
                    $1, $2, $3, $4, 'supervised-qualification-test', 'active',
                    $5, '{}'::JSONB, $6, $6, $7, $6
                 )",
            )
            .bind(lease_id)
            .bind(fixture.tenant_id.as_uuid())
            .bind(fixture.cluster_id.as_uuid())
            .bind(lease_epoch)
            .bind(format!("qualification-{lease_id}"))
            .bind(started_at)
            .bind(observed_at + Duration::hours(1))
            .execute(&repository.pool)
            .await
            .expect("qualification lease");
            (lease_id, lease_epoch)
        }
    };
    sqlx::query(
        "INSERT INTO executions (
            id, tenant_id, cluster_id, correlation_id, plan_id, plan_hash,
            resource_key, action_id, idempotency_key, state,
            request_snapshot, requested_by, started_at, completed_at, updated_at
         ) VALUES (
            $1, $2, $3, $4, $5, $6,
            'broker/test', $7, $8, 'succeeded',
            $9, 'operator@example.com', $10, $11, $11
         )",
    )
    .bind(execution_id.as_uuid())
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .bind(Uuid::new_v4())
    .bind(fixture.plan_id.as_uuid())
    .bind(&fixture.plan_hash)
    .bind(ExecutionAction::ObservabilityLoggerLevelTtl.id())
    .bind(format!("supervised-qualification-{execution_id}"))
    .bind(serde_json::json!({
        "approvals": [{"approval_id": Uuid::new_v4()}],
        "autonomy_grant": null
    }))
    .bind(started_at)
    .bind(observed_at)
    .execute(&repository.pool)
    .await
    .expect("successful supervised execution");
    sqlx::query(
        "INSERT INTO execution_steps (
            execution_id, step_id, attempt, record_kind, lease_id,
            lease_epoch, compensation, intent_snapshot, result_snapshot,
            reason_code, occurred_at
         ) VALUES (
            $1, $2, 1, 'intent', $3,
            $4, FALSE, '{}'::JSONB, NULL,
            'step_intent_persisted', $5
         )",
    )
    .bind(execution_id.as_uuid())
    .bind(step_id)
    .bind(lease_id)
    .bind(lease_epoch)
    .bind(started_at)
    .execute(&repository.pool)
    .await
    .expect("supervised forward intent");
    for (phase, evidence_at) in [("pre", started_at), ("post", observed_at)] {
        sqlx::query(
            "INSERT INTO execution_verification_evidence (
                execution_id, step_id, attempt, phase, evidence_id,
                evidence_snapshot, observed_at
             ) VALUES ($1, $2, 1, $3, $4, '{}'::JSONB, $5)",
        )
        .bind(execution_id.as_uuid())
        .bind(step_id)
        .bind(phase)
        .bind(Uuid::new_v4())
        .bind(evidence_at)
        .execute(&repository.pool)
        .await
        .expect("supervised verification evidence");
    }
    sqlx::query(
        "INSERT INTO execution_verifications (
            execution_id, step_id, attempt, compensation, outcome,
            result_snapshot, started_at, completed_at
         ) VALUES (
            $1, $2, 1, FALSE, 'succeeded', $3, $4, $5
         )",
    )
    .bind(execution_id.as_uuid())
    .bind(step_id)
    .bind(serde_json::json!({
        "stable_window_seconds": stable_window_seconds,
    }))
    .bind(started_at)
    .bind(observed_at)
    .execute(&repository.pool)
    .await
    .expect("supervised stable verification");
    execution_id
}

pub(super) async fn seed_critic_review(
    repository: &PostgresRepository,
    fixture: &Fixture,
) -> (CriticReviewId, ModelInvocationId, String) {
    let profile_id = Uuid::new_v4();
    let profile_name = format!("autonomy-critic-{profile_id}");
    sqlx::query(
        "INSERT INTO model_profiles (
            id, tenant_id, profile_name, provider_family, protocol_family,
            model_family, model_name, model_revision, endpoint_instance,
            region, data_residency, data_classes, capabilities, priority,
            credential_ref, credential_owner, enabled, health, created_at, updated_at
         ) VALUES ($1, $2, $3, 'openai-compatible', 'openai-compatible',
                   'glm', 'glm-5', 'glm-5', 'local',
                   'local', 'local', '[]'::JSONB, '{}'::JSONB, 90,
                   'test-reference', 'gateway', TRUE, 'healthy', NOW(), NOW())",
    )
    .bind(profile_id)
    .bind(fixture.tenant_id.as_uuid())
    .bind(&profile_name)
    .execute(&repository.pool)
    .await
    .expect("critic model profile");
    let critic_invocation_id = ModelInvocationId::new();
    sqlx::query(
        "INSERT INTO model_invocations (
            id, tenant_id, cluster_id, incident_id, diagnosis_revision_id,
            parent_invocation_id, purpose, requested_profile_id,
            actual_profile_id, provider_family, model_family, model_revision,
            endpoint_instance, fallback_chain, prompt_version, schema_version,
            rationale, started_at, completed_at
         ) VALUES ($1, $2, $3, $4, $5, $6, 'critic', $7, $7,
                   'openai-compatible', 'glm', 'glm-5', 'local',
                   '{}', 'autonomy-critic-test', 'rocketmq-sre.critic.v1',
                   'autonomy authority fixture', NOW(), NOW())",
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
    .expect("critic model invocation");
    let critic_review_id = CriticReviewId::new();
    let review_hash = unique_digest();
    let payload_hash = unique_digest();
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
            $1, $2, $3, $4,
            $5, $6,
            'deepseek', 'glm',
            'openai-compatible', $7, 'glm-5',
            'local', '{}', 'autonomy-critic-test',
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
    .bind(payload_hash)
    .bind(review_hash)
    .execute(&repository.pool)
    .await
    .expect("critic review");
    (critic_review_id, critic_invocation_id, profile_name)
}

fn policy(fixture: &Fixture, created_at: chrono::DateTime<Utc>) -> AutonomyPolicyDefinition {
    AutonomyPolicyDefinition {
        id: AutonomyPolicyId::new(),
        definition_version: 1,
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        action: ExecutionAction::ObservabilityLoggerLevelTtl,
        action_version: "1.0.0".to_owned(),
        descriptor_digest: unique_digest(),
        diagnostic_pack_id: "runtime-diagnostics".to_owned(),
        diagnostic_pack_version: "1.0.0".to_owned(),
        owner: "messaging-observability".to_owned(),
        minimum_evidence_freshness_seconds: 60,
        required_evidence_sources: vec!["prometheus".to_owned()],
        min_shadow_samples: 1,
        min_supervised_successes: 1,
        observation_window_days: 7,
        max_unresolved_unknown: 0,
        max_recent_rollbacks: 0,
        max_executions_per_hour: 2,
        cooldown_seconds: 900,
        max_concurrent_executions: 1,
        stable_window_seconds: 300,
        created_at,
    }
}

pub(super) fn unique_digest() -> String {
    let value = Uuid::new_v4().simple().to_string();
    format!("sha256:{value}{value}")
}
