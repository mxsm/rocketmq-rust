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
use rocketmq_sre_contracts::ActionItemStatus;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::DrBackupAssetKind;
use rocketmq_sre_contracts::DrExerciseMode;
use rocketmq_sre_contracts::DrExerciseState;
use rocketmq_sre_contracts::DrFindingSeverity;
use rocketmq_sre_contracts::DrSubject;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::FleetId;
use rocketmq_sre_contracts::RecoveryCheckpointDefinition;
use rocketmq_sre_contracts::RecoveryCheckpointStatus;
use rocketmq_sre_contracts::RegionId;
use rocketmq_sre_contracts::RtoRpoTarget;
use rocketmq_sre_contracts::TenantId;

use super::model::CreateDrPlanRequest;
use super::model::DrActionItemQuery;
use super::model::RecordDrFindingRequest;
use super::model::RecordRecoveryCheckpointRequest;
use super::model::StartDrExerciseRequest;
use super::model::TransitionDrExerciseRequest;
use super::model::UpdateDrActionItemRequest;
use super::model::UpsertDrBackupAssetRequest;
use super::service::DrService;
use crate::PostgresRepository;
use crate::auth::AuthContext;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_dr_center_enforces_test_boundary_and_tracks_findings() {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 5)
        .await
        .expect("repository with DR migrations");
    let fixture = seed_fixture(&repository).await;
    let service = DrService::new(repository);
    let auth = operator_auth(fixture.tenant_id, [fixture.test_cluster, fixture.production_cluster]);
    let evidence_id = EvidenceId::new();

    let plan = service
        .create_plan(
            &auth,
            &plan_request(
                &fixture,
                fixture.test_cluster,
                "test-cluster-recovery",
                vec![DrExerciseMode::Readiness, DrExerciseMode::SupervisedTest],
            ),
        )
        .await
        .expect("test-cluster DR plan");
    let production_plan = service
        .create_plan(
            &auth,
            &plan_request(
                &fixture,
                fixture.production_cluster,
                "production-cluster-recovery",
                vec![DrExerciseMode::Readiness, DrExerciseMode::SupervisedTest],
            ),
        )
        .await
        .expect("production DR plan");
    let production_rejection = service
        .create_exercise(
            &auth,
            &StartDrExerciseRequest {
                plan_id: production_plan.id,
                mode: DrExerciseMode::SupervisedTest,
            },
        )
        .await;
    assert!(production_rejection.is_err());

    let unencrypted = service
        .upsert_backup_asset(
            &auth,
            plan.id,
            &UpsertDrBackupAssetRequest {
                kind: DrBackupAssetKind::PostgreSql,
                owner: "database-platform".to_owned(),
                access_owner: "security-platform".to_owned(),
                backup_locator_digest: digest('a'),
                encrypted: false,
                last_backup_at: Some(Utc::now()),
                restore_verified_at: None,
                evidence_ids: vec![evidence_id],
            },
        )
        .await;
    assert!(unencrypted.is_err());
    let backup = service
        .upsert_backup_asset(
            &auth,
            plan.id,
            &UpsertDrBackupAssetRequest {
                kind: DrBackupAssetKind::PostgreSql,
                owner: "database-platform".to_owned(),
                access_owner: "security-platform".to_owned(),
                backup_locator_digest: digest('b'),
                encrypted: true,
                last_backup_at: Some(Utc::now() - Duration::minutes(5)),
                restore_verified_at: Some(Utc::now()),
                evidence_ids: vec![evidence_id],
            },
        )
        .await
        .expect("encrypted backup inventory");
    assert_eq!(backup.owner, "database-platform");

    let exercise = service
        .create_exercise(
            &auth,
            &StartDrExerciseRequest {
                plan_id: plan.id,
                mode: DrExerciseMode::SupervisedTest,
            },
        )
        .await
        .expect("test-cluster supervised exercise");
    let running = service
        .transition_exercise(
            &auth,
            exercise.id,
            &TransitionDrExerciseRequest {
                state: DrExerciseState::Running,
                actual_rto_seconds: None,
                actual_rpo_seconds: None,
                evidence_ids: Vec::new(),
            },
        )
        .await
        .expect("running exercise");
    assert_eq!(running.state, DrExerciseState::Running);

    let checkpoint_started = Utc::now() - Duration::seconds(30);
    let first_checkpoint = service
        .record_checkpoint(
            &auth,
            exercise.id,
            &RecordRecoveryCheckpointRequest {
                sequence: 0,
                key: "restore-data".to_owned(),
                title: "Restore durable data".to_owned(),
                status: RecoveryCheckpointStatus::Passed,
                expected_duration_seconds: 300,
                actual_duration_seconds: Some(30),
                observed_rpo_seconds: Some(5),
                manual_confirmation_required: true,
                confirmed_by: Some("recovery-operator".to_owned()),
                cleanup_required: true,
                cleanup_complete: true,
                evidence_ids: vec![evidence_id],
                finding_codes: vec!["restore-documentation-gap".to_owned()],
                note: Some("Restore completed in the isolated test cluster".to_owned()),
                started_at: checkpoint_started,
                completed_at: Some(Utc::now()),
            },
        )
        .await
        .expect("restore checkpoint");
    assert!(first_checkpoint.cleanup_complete);
    service
        .record_checkpoint(
            &auth,
            exercise.id,
            &RecordRecoveryCheckpointRequest {
                sequence: 1,
                key: "reconcile-ledgers".to_owned(),
                title: "Reconcile recovery ledgers".to_owned(),
                status: RecoveryCheckpointStatus::Passed,
                expected_duration_seconds: 120,
                actual_duration_seconds: Some(20),
                observed_rpo_seconds: Some(0),
                manual_confirmation_required: false,
                confirmed_by: None,
                cleanup_required: false,
                cleanup_complete: false,
                evidence_ids: vec![evidence_id],
                finding_codes: Vec::new(),
                note: None,
                started_at: Utc::now() - Duration::seconds(20),
                completed_at: Some(Utc::now()),
            },
        )
        .await
        .expect("ledger checkpoint");

    let finding_request = RecordDrFindingRequest {
        code: "restore-documentation-gap".to_owned(),
        severity: DrFindingSeverity::Warning,
        summary: "Restore ownership handoff was not documented".to_owned(),
        remediation: "Publish and verify the recovery ownership matrix".to_owned(),
        evidence_ids: vec![evidence_id],
        owner: Some("sre-platform".to_owned()),
        due_at: Some(Utc::now() + Duration::days(7)),
    };
    let finding = service
        .record_finding(&auth, exercise.id, &finding_request)
        .await
        .expect("finding and action item");
    let duplicate = service
        .record_finding(&auth, exercise.id, &finding_request)
        .await
        .expect("idempotent finding retry");
    assert_eq!(duplicate.id, finding.id);
    assert_eq!(duplicate.action_item_id, finding.action_item_id);

    let completed = service
        .transition_exercise(
            &auth,
            exercise.id,
            &TransitionDrExerciseRequest {
                state: DrExerciseState::Completed,
                actual_rto_seconds: Some(50),
                actual_rpo_seconds: Some(5),
                evidence_ids: vec![evidence_id],
            },
        )
        .await
        .expect("completed recovery exercise");
    assert_eq!(completed.manual_checkpoint_count, 1);
    assert!(completed.cleanup_complete);
    assert_eq!(completed.actual_rto_seconds, Some(50));

    let open_actions = service
        .action_items(
            &auth,
            &DrActionItemQuery {
                cluster_id: Some(fixture.test_cluster),
                status: Some(ActionItemStatus::Open),
                limit: 20,
            },
        )
        .await
        .expect("open DR action items");
    assert_eq!(open_actions.items.len(), 1);
    assert_eq!(open_actions.items[0].id, finding.action_item_id);
    service
        .update_action_item(
            &auth,
            finding.action_item_id,
            &UpdateDrActionItemRequest {
                status: ActionItemStatus::InProgress,
                owner: Some("sre-platform".to_owned()),
                due_at: finding_request.due_at,
                verification: None,
                evidence_ids: Vec::new(),
            },
        )
        .await
        .expect("start DR action item");
    let closed_action = service
        .update_action_item(
            &auth,
            finding.action_item_id,
            &UpdateDrActionItemRequest {
                status: ActionItemStatus::Completed,
                owner: Some("sre-platform".to_owned()),
                due_at: finding_request.due_at,
                verification: Some("Ownership matrix reviewed during the next exercise".to_owned()),
                evidence_ids: vec![evidence_id],
            },
        )
        .await
        .expect("complete DR action item");
    assert_eq!(closed_action.status, ActionItemStatus::Completed);
    let findings = service
        .findings(&auth, exercise.id)
        .await
        .expect("resolved DR findings");
    assert_eq!(findings.items[0].status, rocketmq_sre_contracts::DrFindingStatus::Resolved);
}

fn plan_request(
    fixture: &DrFixture,
    cluster_id: ClusterId,
    name: &str,
    allowed_modes: Vec<DrExerciseMode>,
) -> CreateDrPlanRequest {
    CreateDrPlanRequest {
        fleet_id: fixture.fleet_id,
        region_id: fixture.region_id,
        cluster_id: Some(cluster_id),
        subject: DrSubject::RocketMqCluster,
        name: format!("{name}-{}", uuid::Uuid::new_v4()),
        version: 1,
        owner: "recovery-platform".to_owned(),
        target: RtoRpoTarget {
            rto_seconds: 600,
            rpo_seconds: 30,
        },
        allowed_modes,
        required_sources: vec![
            "nameserver-route".to_owned(),
            "controller-quorum".to_owned(),
            "broker-ha".to_owned(),
            "store-recovery-report".to_owned(),
            "rocksdb-checkpoint".to_owned(),
            "tiered-reconcile".to_owned(),
            "kubernetes-storage".to_owned(),
        ],
        checkpoints: vec![
            RecoveryCheckpointDefinition {
                key: "restore-data".to_owned(),
                title: "Restore durable data".to_owned(),
                expected_duration_seconds: 300,
                manual_confirmation_required: true,
                cleanup_required: true,
                required_evidence_kinds: vec!["recovery-report".to_owned()],
            },
            RecoveryCheckpointDefinition {
                key: "reconcile-ledgers".to_owned(),
                title: "Reconcile recovery ledgers".to_owned(),
                expected_duration_seconds: 120,
                manual_confirmation_required: false,
                cleanup_required: false,
                required_evidence_kinds: vec!["ledger-reconciliation".to_owned()],
            },
        ],
    }
}

#[derive(Clone, Copy)]
struct DrFixture {
    fleet_id: FleetId,
    tenant_id: TenantId,
    region_id: RegionId,
    test_cluster: ClusterId,
    production_cluster: ClusterId,
}

async fn seed_fixture(repository: &PostgresRepository) -> DrFixture {
    let fixture = DrFixture {
        fleet_id: FleetId::new(),
        tenant_id: TenantId::new(),
        region_id: RegionId::new(),
        test_cluster: ClusterId::new(),
        production_cluster: ClusterId::new(),
    };
    sqlx::query("INSERT INTO fleets (id, name, owner_name) VALUES ($1, $2, 'dr-test')")
        .bind(fixture.fleet_id.as_uuid())
        .bind(format!("dr-fleet-{}", fixture.fleet_id))
        .execute(&repository.pool)
        .await
        .expect("DR Fleet fixture");
    sqlx::query(
        "INSERT INTO fleet_tenants (id, fleet_id, name, owner_name)
         VALUES ($1, $2, $3, 'dr-test')",
    )
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.fleet_id.as_uuid())
    .bind(format!("dr-tenant-{}", fixture.tenant_id))
    .execute(&repository.pool)
    .await
    .expect("DR tenant fixture");
    sqlx::query(
        "INSERT INTO fleet_regions (
            id, fleet_id, region_key, display_name, owner_name, residency_tags
         ) VALUES ($1, $2, $3, 'DR test region', 'dr-test', $4)",
    )
    .bind(fixture.region_id.as_uuid())
    .bind(fixture.fleet_id.as_uuid())
    .bind(format!("dr-region-{}", fixture.region_id))
    .bind(serde_json::json!(["test-residency"]))
    .execute(&repository.pool)
    .await
    .expect("DR region fixture");
    for (cluster_id, environment) in [
        (fixture.test_cluster, "test"),
        (fixture.production_cluster, "production"),
    ] {
        sqlx::query(
            "INSERT INTO clusters (
                id, tenant_id, external_cluster_key, environment, region,
                rocketmq_version, deployment_mode, owner_name,
                requested_access_profile, effective_access_profile, onboarding_state
             ) VALUES (
                $1, $2, $3, $4, $5, '5.3.2', 'docker', 'dr-test',
                'read_only', 'read_only', 'ready_read_only'
             )",
        )
        .bind(cluster_id.as_uuid())
        .bind(fixture.tenant_id.as_uuid())
        .bind(format!("dr-{environment}-{cluster_id}"))
        .bind(environment)
        .bind(fixture.region_id.to_string())
        .execute(&repository.pool)
        .await
        .expect("DR cluster fixture");
        sqlx::query(
            "INSERT INTO fleet_cluster_registrations (
                cluster_id, fleet_id, tenant_id, region_id, environment,
                owner_name, lifecycle_state, residency_tags
             ) VALUES ($1, $2, $3, $4, $5, 'dr-test', 'active', $6)",
        )
        .bind(cluster_id.as_uuid())
        .bind(fixture.fleet_id.as_uuid())
        .bind(fixture.tenant_id.as_uuid())
        .bind(fixture.region_id.as_uuid())
        .bind(environment)
        .bind(serde_json::json!(["test-residency"]))
        .execute(&repository.pool)
        .await
        .expect("DR cluster registration fixture");
    }
    fixture
}

fn operator_auth(tenant_id: TenantId, clusters: impl IntoIterator<Item = ClusterId>) -> AuthContext {
    AuthContext {
        tenant_id,
        subject: "dr-operator".to_owned(),
        clusters: clusters.into_iter().collect::<BTreeSet<_>>(),
        roles: BTreeSet::from(["operator".to_owned()]),
    }
}

fn digest(fill: char) -> String {
    format!("sha256:{}", fill.to_string().repeat(64))
}
