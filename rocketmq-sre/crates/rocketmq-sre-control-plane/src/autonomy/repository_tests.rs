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
use rocketmq_sre_contracts::AutonomyMode;
use rocketmq_sre_contracts::AutonomyPolicyDefinition;
use rocketmq_sre_contracts::AutonomyPolicyId;
use rocketmq_sre_contracts::AutonomyQualificationSample;
use rocketmq_sre_contracts::AutonomySampleId;
use rocketmq_sre_contracts::AutonomySampleKind;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_core::ActualModelIdentity;
use rocketmq_sre_core::AutonomyActor;
use rocketmq_sre_core::AutonomyPolicy;
use rocketmq_sre_core::AutonomyStateMachine;
use rocketmq_sre_core::PromotionQualification;
use uuid::Uuid;

use crate::PostgresRepository;

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
    let now = Utc::now().with_nanosecond(0).expect("whole-second timestamp");
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
            profile: "deepseek-primary".to_owned(),
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
            profile: "deepseek-primary".to_owned(),
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
    assert_eq!(scope.lifecycle.mode, AutonomyMode::Shadow);
    assert_eq!(scope.qualification.qualified_shadow_samples, 1);
    assert_eq!(scope.active_freezes.len(), 1);
    assert!(scope.kill_switch.is_some_and(|state| state.active));

    let mut revised_policy = stored;
    revised_policy.created_at = now + Duration::seconds(5);
    let (revised, revised_lifecycle) = repository
        .store_autonomy_policy(revised_policy, "autonomy-owner")
        .await
        .expect("policy revision");
    assert_eq!(revised.definition_version, 2);
    assert_eq!(revised_lifecycle.mode, AutonomyMode::Shadow);
    assert_eq!(revised_lifecycle.lifecycle_revision, 3);
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

struct Fixture {
    tenant_id: TenantId,
    cluster_id: ClusterId,
    incident_id: IncidentId,
    plan_id: ActionPlanId,
    plan_hash: String,
}

async fn seed_fixture(repository: &PostgresRepository) -> Fixture {
    let fixture = Fixture {
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        incident_id: IncidentId::new(),
        plan_id: ActionPlanId::new(),
        plan_hash: unique_digest(),
    };
    let diagnosis_id = Uuid::new_v4();
    let profile_id = Uuid::new_v4();
    let invocation_id = Uuid::new_v4();
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
    .bind(diagnosis_id)
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
    .bind(format!("autonomy-repository-{profile_id}"))
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
    .bind(invocation_id)
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .bind(fixture.incident_id.as_uuid())
    .bind(diagnosis_id)
    .bind(profile_id)
    .execute(&repository.pool)
    .await
    .expect("model invocation");
    sqlx::query(
        "UPDATE diagnosis_revisions
         SET primary_model_invocation_id = $2, execution_eligible = TRUE
         WHERE id = $1",
    )
    .bind(diagnosis_id)
    .bind(invocation_id)
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
    .bind(diagnosis_id)
    .bind(invocation_id)
    .bind(&fixture.plan_hash)
    .bind(unique_digest())
    .execute(&repository.pool)
    .await
    .expect("action plan");
    fixture
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

fn unique_digest() -> String {
    let value = Uuid::new_v4().simple().to_string();
    format!("sha256:{value}{value}")
}
