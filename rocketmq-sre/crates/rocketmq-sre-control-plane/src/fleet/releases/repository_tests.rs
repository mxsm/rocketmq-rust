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

use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::ActionPlanId;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::FleetId;
use rocketmq_sre_contracts::FleetReleaseStatus;
use rocketmq_sre_contracts::FleetReleaseTargetState;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::RegionId;
use rocketmq_sre_contracts::ReleaseId;
use rocketmq_sre_contracts::RunbookId;
use rocketmq_sre_contracts::TenantId;
use uuid::Uuid;

use super::model::CreateFleetReleaseRequest;
use super::model::FleetReleaseQuery;
use super::model::FleetReleaseTargetSpec;
use super::model::RecordFleetTargetOutcomeRequest;
use super::model::RecordFleetTargetReadinessRequest;
use super::model::StartFleetReleaseBatchRequest;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::fleet::FleetService;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_two_region_release_denies_unready_target_and_pauses_on_canary_regression() {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 5)
        .await
        .expect("repository with Fleet release migrations");
    let fixture = seed_fixture(&repository).await;
    let service = FleetService::new(repository.clone());
    let auth = operator_auth(
        fixture.tenant_id,
        [fixture.canary.cluster_id, fixture.second.cluster_id],
    );
    let request = CreateFleetReleaseRequest {
        fleet_id: fixture.fleet_id,
        release_ref: format!("fleet-release-{}", Uuid::new_v4()),
        artifact_digest: unique_digest(),
        target_version: "5.3.3".to_owned(),
        owner: "fleet-release-test".to_owned(),
        maintenance_window_start: Utc::now() - Duration::minutes(1),
        maintenance_window_end: Utc::now() + Duration::hours(1),
        rollback_artifact_digest: unique_digest(),
        slo_policy_id: "rocketmq-release-slo.v1".to_owned(),
        regional_max_concurrency: 1,
        targets: vec![
            FleetReleaseTargetSpec {
                cluster_id: fixture.canary.cluster_id,
                region_id: fixture.canary.region_id,
                canary: true,
            },
            FleetReleaseTargetSpec {
                cluster_id: fixture.second.cluster_id,
                region_id: fixture.second.region_id,
                canary: false,
            },
        ],
    };
    let created = service
        .create_fleet_release(&auth, &request)
        .await
        .expect("two-region Fleet release");
    assert_eq!(created.release.status, FleetReleaseStatus::Planned);
    assert_eq!(created.release.batches.len(), 2);
    assert_eq!(created.release.batches[0].cluster_ids, vec![fixture.canary.cluster_id]);
    assert_eq!(created.release.batches[1].cluster_ids, vec![fixture.second.cluster_id]);
    assert_ne!(
        created.release.batches[0].region_id,
        created.release.batches[1].region_id
    );

    let readiness = service
        .begin_fleet_release_readiness(&auth, created.release.id)
        .await
        .expect("Fleet release readiness");
    assert_eq!(readiness.release.status, FleetReleaseStatus::ReadinessChecking);
    service
        .record_fleet_target_readiness(
            &auth,
            created.release.id,
            fixture.canary.cluster_id,
            &RecordFleetTargetReadinessRequest {
                eligible: true,
                release_id: Some(fixture.canary.release_id),
                reason_codes: Vec::new(),
            },
        )
        .await
        .expect("eligible canary");
    let ready = service
        .record_fleet_target_readiness(
            &auth,
            created.release.id,
            fixture.second.cluster_id,
            &RecordFleetTargetReadinessRequest {
                eligible: false,
                release_id: None,
                reason_codes: vec!["pdb_not_ready".to_owned()],
            },
        )
        .await
        .expect("bounded readiness denial");
    assert_eq!(ready.release.status, FleetReleaseStatus::Ready);
    assert_eq!(
        ready
            .targets
            .iter()
            .find(|target| target.cluster_id == fixture.second.cluster_id)
            .map(|target| target.state),
        Some(FleetReleaseTargetState::Ineligible)
    );
    assert!(
        service
            .start_fleet_release_batch(
                &auth,
                created.release.id,
                &StartFleetReleaseBatchRequest { expected_sequence: 1 },
            )
            .await
            .is_err()
    );
    let running = service
        .start_fleet_release_batch(
            &auth,
            created.release.id,
            &StartFleetReleaseBatchRequest { expected_sequence: 0 },
        )
        .await
        .expect("canary batch");
    assert_eq!(running.release.status, FleetReleaseStatus::CanaryRunning);
    assert_eq!(running.release.active_batch, Some(0));

    transition_linked_release(&repository, fixture.canary.release_id, "canary_running", false, None).await;
    transition_linked_release(
        &repository,
        fixture.canary.release_id,
        "paused",
        true,
        Some("canary_regression"),
    )
    .await;
    let paused = service
        .record_fleet_target_outcome(
            &auth,
            created.release.id,
            fixture.canary.cluster_id,
            &RecordFleetTargetOutcomeRequest {
                state: FleetReleaseTargetState::Paused,
                regression_detected: true,
                sanitized_outcome: Some("Canary SLO regression detected by the bounded probe".to_owned()),
            },
        )
        .await
        .expect("canary regression pause");
    assert_eq!(paused.release.status, FleetReleaseStatus::Paused);
    assert!(
        service
            .resume_fleet_release(
                &auth,
                created.release.id,
                &super::model::FleetReleaseReasonRequest {
                    reason: "Regression remains open".to_owned(),
                },
            )
            .await
            .is_err()
    );

    transition_linked_release(
        &repository,
        fixture.canary.release_id,
        "rolling_back",
        true,
        Some("approved_rollback"),
    )
    .await;
    transition_linked_release(
        &repository,
        fixture.canary.release_id,
        "rolled_back",
        false,
        Some("rollback_verified"),
    )
    .await;
    let rolled_back = service
        .record_fleet_target_outcome(
            &auth,
            created.release.id,
            fixture.canary.cluster_id,
            &RecordFleetTargetOutcomeRequest {
                state: FleetReleaseTargetState::RolledBack,
                regression_detected: false,
                sanitized_outcome: Some("Approved canary rollback completed and was verified".to_owned()),
            },
        )
        .await
        .expect("Fleet rollback projection");
    assert_eq!(rolled_back.release.status, FleetReleaseStatus::RolledBack);

    let report = service
        .fleet_release_report(&auth, created.release.id)
        .await
        .expect("Fleet release report");
    assert_eq!(report.state_counts.get("rolled_back"), Some(&1));
    assert_eq!(report.state_counts.get("ineligible"), Some(&1));
    assert_eq!(report.skipped_clusters, vec![fixture.second.cluster_id]);
    let page = service
        .fleet_releases(
            &auth,
            &FleetReleaseQuery {
                status: Some(FleetReleaseStatus::RolledBack),
                limit: 10,
                offset: 0,
            },
        )
        .await
        .expect("Fleet release history");
    assert_eq!(page.total, 1);
    assert_eq!(page.items[0].id, created.release.id);

    let restricted = operator_auth(fixture.tenant_id, [fixture.canary.cluster_id]);
    assert!(service.fleet_release(&restricted, created.release.id).await.is_err());
    assert!(
        sqlx::query("UPDATE fleet_release_events SET reason_code = 'tampered' WHERE fleet_release_id = $1")
            .bind(created.release.id.as_uuid())
            .execute(&repository.pool)
            .await
            .is_err()
    );
}

#[derive(Clone, Copy)]
struct LinkedReleaseFixture {
    cluster_id: ClusterId,
    region_id: RegionId,
    release_id: ReleaseId,
}

struct FleetReleaseFixture {
    fleet_id: FleetId,
    tenant_id: TenantId,
    canary: LinkedReleaseFixture,
    second: LinkedReleaseFixture,
}

async fn seed_fixture(repository: &PostgresRepository) -> FleetReleaseFixture {
    let fleet_id = FleetId::new();
    let tenant_id = TenantId::new();
    let regions = [RegionId::new(), RegionId::new()];
    sqlx::query("INSERT INTO fleets (id, name, owner_name) VALUES ($1, $2, 'fleet-release-test')")
        .bind(fleet_id.as_uuid())
        .bind(format!("fleet-release-{fleet_id}"))
        .execute(&repository.pool)
        .await
        .expect("Fleet release fixture");
    sqlx::query(
        "INSERT INTO fleet_tenants (id, fleet_id, name, owner_name)
         VALUES ($1, $2, $3, 'fleet-release-test')",
    )
    .bind(tenant_id.as_uuid())
    .bind(fleet_id.as_uuid())
    .bind(format!("tenant-release-{tenant_id}"))
    .execute(&repository.pool)
    .await
    .expect("Fleet release tenant");
    for (index, region_id) in regions.iter().enumerate() {
        sqlx::query(
            "INSERT INTO fleet_regions (
                id, fleet_id, region_key, display_name, owner_name, residency_tags
             ) VALUES ($1, $2, $3, $3, 'fleet-release-test', $4)",
        )
        .bind(region_id.as_uuid())
        .bind(fleet_id.as_uuid())
        .bind(format!("release-region-{index}-{region_id}"))
        .bind(serde_json::json!([format!("release-region-{index}")]))
        .execute(&repository.pool)
        .await
        .expect("Fleet release region");
    }
    let canary = seed_linked_release(repository, fleet_id, tenant_id, regions[0], "canary").await;
    let second = seed_linked_release(repository, fleet_id, tenant_id, regions[1], "second").await;
    FleetReleaseFixture {
        fleet_id,
        tenant_id,
        canary,
        second,
    }
}

async fn seed_linked_release(
    repository: &PostgresRepository,
    fleet_id: FleetId,
    tenant_id: TenantId,
    region_id: RegionId,
    label: &str,
) -> LinkedReleaseFixture {
    let cluster_id = ClusterId::new();
    let incident_id = IncidentId::new();
    let plan_id = ActionPlanId::new();
    let runbook_id = RunbookId::new();
    let release_id = ReleaseId::new();
    let diagnosis_id = Uuid::new_v4();
    let profile_id = Uuid::new_v4();
    let invocation_id = Uuid::new_v4();
    let plan_hash = unique_digest();
    sqlx::query(
        "INSERT INTO clusters (
            id, tenant_id, external_cluster_key, environment, region,
            rocketmq_version, deployment_mode, owner_name,
            requested_access_profile, effective_access_profile, onboarding_state
         ) VALUES ($1, $2, $3, 'test', $4, '5.3.2', 'docker',
                   'fleet-release-test', 'supervised', 'read_only', 'ready_read_only')",
    )
    .bind(cluster_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(format!("fleet-release-{label}-{cluster_id}"))
    .bind(region_id.to_string())
    .execute(&repository.pool)
    .await
    .expect("Fleet release cluster");
    sqlx::query(
        "INSERT INTO fleet_cluster_registrations (
            cluster_id, fleet_id, tenant_id, region_id, environment,
            owner_name, lifecycle_state, residency_tags
         ) VALUES ($1, $2, $3, $4, 'test', 'fleet-release-test', 'active', $5)",
    )
    .bind(cluster_id.as_uuid())
    .bind(fleet_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(region_id.as_uuid())
    .bind(serde_json::json!([region_id.to_string()]))
    .execute(&repository.pool)
    .await
    .expect("Fleet release cluster registration");
    sqlx::query(
        "INSERT INTO sre_incidents (
            id, tenant_id, cluster_id, title, resource, symptom_family,
            fingerprint, status, workflow_checkpoint, created_by_subject,
            created_at, updated_at
         ) VALUES ($1, $2, $3, $4, 'deployment/default/proxy', 'release',
                   $5, 'diagnosing', '{}'::JSONB, 'fleet-release-test', NOW(), NOW())",
    )
    .bind(incident_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(format!("Fleet release {label}"))
    .bind(unique_digest())
    .execute(&repository.pool)
    .await
    .expect("Fleet release incident");
    sqlx::query(
        "INSERT INTO diagnosis_revisions (
            id, incident_id, revision, status, rule_result, hypotheses,
            evidence_ids, primary_model_invocation_id,
            execution_eligible, partial, created_at
         ) VALUES ($1, $2, 1, 'confirmed', '{}'::JSONB, '[]'::JSONB,
                   '{}', NULL, FALSE, FALSE, NOW())",
    )
    .bind(diagnosis_id)
    .bind(incident_id.as_uuid())
    .execute(&repository.pool)
    .await
    .expect("Fleet release diagnosis");
    sqlx::query(
        "INSERT INTO model_profiles (
            id, tenant_id, profile_name, provider_family, protocol_family,
            model_family, model_name, model_revision, endpoint_instance,
            region, data_residency, data_classes, capabilities, priority,
            credential_ref, credential_owner, enabled, health, created_at, updated_at
         ) VALUES ($1, $2, $3, 'openai-compatible', 'openai-compatible',
                   'fleet-release-test', 'fleet-release-test', 'r1', 'local',
                   'local', 'local', '[]'::JSONB, '{}'::JSONB, 100,
                   'test-reference', 'gateway', TRUE, 'healthy', NOW(), NOW())",
    )
    .bind(profile_id)
    .bind(tenant_id.as_uuid())
    .bind(format!("fleet-release-{profile_id}"))
    .execute(&repository.pool)
    .await
    .expect("Fleet release model profile");
    sqlx::query(
        "INSERT INTO model_invocations (
            id, tenant_id, cluster_id, incident_id, diagnosis_revision_id,
            parent_invocation_id, purpose, requested_profile_id,
            actual_profile_id, provider_family, model_family, model_revision,
            endpoint_instance, fallback_chain, prompt_version, schema_version,
            rationale, started_at, completed_at
         ) VALUES ($1, $2, $3, $4, $5, NULL, 'primary_diagnosis', $6, $6,
                   'openai-compatible', 'fleet-release-test', 'r1', 'local',
                   '{}', 'fleet-release-test', 'rocketmq-sre.model.v1',
                   'Fleet release test fixture', NOW(), NOW())",
    )
    .bind(invocation_id)
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(incident_id.as_uuid())
    .bind(diagnosis_id)
    .bind(profile_id)
    .execute(&repository.pool)
    .await
    .expect("Fleet release model invocation");
    sqlx::query(
        "UPDATE diagnosis_revisions
         SET primary_model_invocation_id = $2, execution_eligible = TRUE
         WHERE id = $1",
    )
    .bind(diagnosis_id)
    .bind(invocation_id)
    .execute(&repository.pool)
    .await
    .expect("Fleet release executable diagnosis");
    sqlx::query(
        "INSERT INTO action_plans (
            id, tenant_id, cluster_id, incident_id, diagnosis_revision_id,
            primary_model_invocation_id, version, plan_hash, evidence_hash,
            risk, status, request_snapshot, created_by, created_at, expires_at,
            submitted_at
         ) VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $8,
                   'r2', 'approved', '{}'::JSONB, 'fleet-release-test',
                   NOW(), NOW() + INTERVAL '1 hour', NOW())",
    )
    .bind(plan_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(incident_id.as_uuid())
    .bind(diagnosis_id)
    .bind(invocation_id)
    .bind(&plan_hash)
    .bind(unique_digest())
    .execute(&repository.pool)
    .await
    .expect("Fleet release action plan");
    sqlx::query(
        "INSERT INTO runbook_definitions (
            tenant_id, cluster_id, id, version, risk,
            definition_snapshot, created_by, created_at
         ) VALUES ($1, $2, $3, '1.0.0', 'r2', '{}'::JSONB,
                   'fleet-release-test', NOW())",
    )
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(runbook_id.as_uuid())
    .execute(&repository.pool)
    .await
    .expect("Fleet release runbook");
    sqlx::query(
        "INSERT INTO release_workflows (
            id, tenant_id, cluster_id, incident_id, correlation_id,
            change_id, release_ref, target_version, runbook_id, runbook_version,
            plan_id, plan_hash, rollback_plan_id, rollback_plan_hash,
            readiness_snapshot, status, active_execution_id,
            regression_detected, pause_reason, workflow_snapshot,
            created_by, created_at, updated_at
         ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, '5.3.3', $8, '1.0.0',
            $9, $10, NULL, NULL, NULL, 'ready', NULL,
            FALSE, NULL, '{}'::JSONB, 'fleet-release-test', NOW(), NOW()
         )",
    )
    .bind(release_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(incident_id.as_uuid())
    .bind(Uuid::new_v4())
    .bind(format!("CHG-{label}-{}", Uuid::new_v4()))
    .bind(format!("REL-{label}-{}", Uuid::new_v4()))
    .bind(runbook_id.as_uuid())
    .bind(plan_id.as_uuid())
    .bind(plan_hash)
    .execute(&repository.pool)
    .await
    .expect("independently ready cluster release");
    LinkedReleaseFixture {
        cluster_id,
        region_id,
        release_id,
    }
}

async fn transition_linked_release(
    repository: &PostgresRepository,
    release_id: ReleaseId,
    status: &str,
    regression_detected: bool,
    pause_reason: Option<&str>,
) {
    sqlx::query(
        "UPDATE release_workflows
         SET status = $2, regression_detected = $3, pause_reason = $4,
             updated_at = GREATEST(updated_at + INTERVAL '1 microsecond', NOW())
         WHERE id = $1",
    )
    .bind(release_id.as_uuid())
    .bind(status)
    .bind(regression_detected)
    .bind(pause_reason)
    .execute(&repository.pool)
    .await
    .expect("linked release transition");
}

fn operator_auth(tenant_id: TenantId, clusters: impl IntoIterator<Item = ClusterId>) -> AuthContext {
    AuthContext {
        tenant_id,
        subject: "fleet-release-operator".to_owned(),
        clusters: clusters.into_iter().collect::<BTreeSet<_>>(),
        roles: BTreeSet::from(["operator".to_owned()]),
    }
}

fn unique_digest() -> String {
    let value = Uuid::new_v4().simple().to_string();
    format!("sha256:{value}{value}")
}
