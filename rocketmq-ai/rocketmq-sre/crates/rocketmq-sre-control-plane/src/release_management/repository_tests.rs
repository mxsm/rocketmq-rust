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
use chrono::Timelike;
use chrono::Utc;
use rocketmq_sre_contracts::ActionPlanId;
use rocketmq_sre_contracts::AuditEvent;
use rocketmq_sre_contracts::AuditEventId;
use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CmdbSnapshot;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::ENTERPRISE_INTEGRATION_EVENT_SCHEMA_VERSION;
use rocketmq_sre_contracts::EnterpriseIntegrationEvent;
use rocketmq_sre_contracts::EnterpriseIntegrationEventId;
use rocketmq_sre_contracts::EnterpriseIntegrationEventKind;
use rocketmq_sre_contracts::EnterpriseIntegrationPayload;
use rocketmq_sre_contracts::GitOpsSnapshot;
use rocketmq_sre_contracts::INTEGRATION_DELIVERY_SCHEMA_VERSION;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::IntegrationAdapterKind;
use rocketmq_sre_contracts::IntegrationDelivery;
use rocketmq_sre_contracts::IntegrationDeliveryId;
use rocketmq_sre_contracts::IntegrationDeliveryStatus;
use rocketmq_sre_contracts::IntegrationEventKind;
use rocketmq_sre_contracts::IntegrationHealth;
use rocketmq_sre_contracts::IntegrationHealthStatus;
use rocketmq_sre_contracts::IntegrationTarget;
use rocketmq_sre_contracts::IntegrationTargetId;
use rocketmq_sre_contracts::NotificationTargetId;
use rocketmq_sre_contracts::ReleaseId;
use rocketmq_sre_contracts::ReleaseObservation;
use rocketmq_sre_contracts::ReleaseObservationPhase;
use rocketmq_sre_contracts::ReleasePipelineEvent;
use rocketmq_sre_contracts::ReleaseReport;
use rocketmq_sre_contracts::ReleaseReportId;
use rocketmq_sre_contracts::ReleaseStatus;
use rocketmq_sre_contracts::ReleaseWorkflow;
use rocketmq_sre_contracts::RunbookId;
use rocketmq_sre_contracts::TenantId;
use serde_json::json;
use uuid::Uuid;

use super::model::AdapterDeliveryReceipt;
use super::model::IntegrationTargetView;
use super::model::QueuedIntegrationDelivery;
use super::model::ReleaseEventRecord;
use crate::PostgresRepository;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_release_and_integration_records_are_durable_idempotent_and_append_only() {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 5)
        .await
        .expect("repository with migrations");
    let fixture = seed_fixture(&repository).await;
    let now = Utc::now().with_nanosecond(0).expect("whole-second timestamp");
    let correlation_id = CorrelationId::new();
    let target = integration_target(&fixture, now);
    repository
        .insert_integration_target(
            &target,
            &audit(
                &fixture,
                correlation_id,
                AuditEventKind::IntegrationTargetRegistered,
                "integration_target",
                target.target.id.to_string(),
                "RepositoryTestIntegrationTargetRegistered",
                now,
            ),
        )
        .await
        .expect("integration target");
    let chatops_target = notification_integration_target(
        &repository,
        &fixture,
        now,
        IntegrationAdapterKind::ChatOpsWebhook,
        "signed_webhook",
    )
    .await;
    let pager_target =
        notification_integration_target(&repository, &fixture, now, IntegrationAdapterKind::Pager, "pager").await;
    for notification_target in [&chatops_target, &pager_target] {
        repository
            .insert_integration_target(
                notification_target,
                &audit(
                    &fixture,
                    correlation_id,
                    AuditEventKind::IntegrationTargetRegistered,
                    "integration_target",
                    notification_target.target.id.to_string(),
                    "RepositoryTestNotificationTargetRegistered",
                    now,
                ),
            )
            .await
            .expect("notification-backed integration target");
    }
    let workflow = release_workflow(&fixture, correlation_id, now);
    let queued = [&target, &chatops_target, &pager_target]
        .into_iter()
        .map(|delivery_target| {
            let delivery = integration_delivery(&fixture, delivery_target, &workflow, now);
            QueuedIntegrationDelivery {
                target: delivery_target.clone(),
                audit: audit(
                    &fixture,
                    correlation_id,
                    AuditEventKind::IntegrationDeliveryQueued,
                    "integration_delivery",
                    delivery.id.to_string(),
                    "RepositoryTestIntegrationDeliveryQueued",
                    now,
                ),
                delivery,
            }
        })
        .collect::<Vec<_>>();
    let delivery = queued[0].delivery.clone();
    repository
        .insert_release_workflow(
            &workflow,
            &release_event(&workflow, None, "RepositoryTestReleaseCreated"),
            &audit(
                &fixture,
                correlation_id,
                AuditEventKind::ReleaseCreated,
                "release",
                workflow.id.to_string(),
                "RepositoryTestReleaseCreated",
                now,
            ),
            &queued,
        )
        .await
        .expect("release workflow");

    assert_eq!(
        repository
            .release_workflow(fixture.tenant_id, workflow.id)
            .await
            .expect("reload release"),
        workflow
    );
    assert!(repository.release_workflow(TenantId::new(), workflow.id).await.is_err());
    let deliveries = repository
        .integration_deliveries(fixture.tenant_id, fixture.cluster_id, Some(target.target.id), 10)
        .await
        .expect("outbox");
    assert_eq!(deliveries.len(), 1);
    assert_delivery_projection(&deliveries[0], &delivery);
    assert!(
        deliveries[0]
            .next_attempt_at
            .is_some_and(|next_attempt| next_attempt >= delivery.created_at)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM notification_outbox
             WHERE tenant_id = $1 AND cluster_id = $2 AND incident_id = $3",
        )
        .bind(fixture.tenant_id.as_uuid())
        .bind(fixture.cluster_id.as_uuid())
        .bind(fixture.incident_id.as_uuid())
        .fetch_one(&repository.pool)
        .await
        .expect("notification outbox count"),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT target.channel
             FROM notification_outbox outbox
             JOIN notification_targets target ON target.id = outbox.target_id
             WHERE outbox.tenant_id = $1 AND outbox.cluster_id = $2
             ORDER BY target.channel",
        )
        .bind(fixture.tenant_id.as_uuid())
        .bind(fixture.cluster_id.as_uuid())
        .fetch_all(&repository.pool)
        .await
        .expect("notification outbox channels"),
        vec!["pager".to_owned(), "signed_webhook".to_owned()]
    );
    let claims = repository.claim_integration_deliveries(32).await.expect("claim outbox");
    let claim = claims
        .into_iter()
        .find(|claim| claim.delivery.id == delivery.id)
        .expect("release delivery claim");
    sqlx::query(
        "UPDATE integration_outbox
         SET claimed_at = NOW() - INTERVAL '3 minutes'
         WHERE id = $1 AND status = 'delivering'",
    )
    .bind(delivery.id.as_uuid())
    .execute(&repository.pool)
    .await
    .expect("stale worker claim fixture");
    let recovered_claim = repository
        .claim_integration_deliveries(32)
        .await
        .expect("recover stale outbox claim")
        .into_iter()
        .find(|recovered| recovered.delivery.id == delivery.id)
        .expect("recovered release delivery claim");
    assert_ne!(recovered_claim.claim_token, claim.claim_token);
    repository
        .finish_integration_delivery(
            &recovered_claim,
            Ok(AdapterDeliveryReceipt {
                external_ticket_key: Some(format!("CHG-{}", workflow.id)),
            }),
        )
        .await
        .expect("finish outbox");
    let delivered = repository
        .integration_deliveries(fixture.tenant_id, fixture.cluster_id, Some(target.target.id), 10)
        .await
        .expect("delivered outbox");
    assert_eq!(delivered[0].status, IntegrationDeliveryStatus::Delivered);
    assert_eq!(delivered[0].attempt_count, 1);
    sqlx::query(
        "UPDATE integration_outbox
         SET status = 'failed', last_error_code = 'repository_test_failure',
             delivered_at = NULL
         WHERE id = $1",
    )
    .bind(delivery.id.as_uuid())
    .execute(&repository.pool)
    .await
    .expect("failed delivery fixture");
    let failed = repository
        .integration_delivery(fixture.tenant_id, delivery.id)
        .await
        .expect("failed delivery");
    let replayed = repository
        .replay_integration_delivery(
            &failed,
            &audit(
                &fixture,
                correlation_id,
                AuditEventKind::IntegrationDeliveryQueued,
                "integration_delivery",
                delivery.id.to_string(),
                "RepositoryTestIntegrationDeliveryReplay",
                Utc::now(),
            ),
        )
        .await
        .expect("manual delivery replay");
    assert_eq!(replayed.status, IntegrationDeliveryStatus::Pending);
    assert_eq!(replayed.attempt_count, 0);

    let mut current = workflow;
    for next in [
        ReleaseStatus::ReadinessChecking,
        ReleaseStatus::Ready,
        ReleaseStatus::CanaryRunning,
        ReleaseStatus::Verifying,
        ReleaseStatus::Completed,
    ] {
        let previous = current.clone();
        current.status = next;
        current.updated_at += Duration::seconds(1);
        repository
            .update_release_workflow(
                &current,
                previous.status,
                previous.updated_at,
                &release_event(&current, Some(previous.status), "RepositoryTestReleaseAdvanced"),
                &audit(
                    &fixture,
                    correlation_id,
                    AuditEventKind::ReleaseStateChanged,
                    "release",
                    current.id.to_string(),
                    "RepositoryTestReleaseAdvanced",
                    current.updated_at,
                ),
                &[],
            )
            .await
            .expect("advance release");
    }
    let observation = ReleaseObservation {
        phase: ReleaseObservationPhase::After,
        slo_healthy: true,
        synthetic_probe_healthy: true,
        regression_detected: false,
        evidence_ids: Vec::new(),
        sanitized_summary: "Post-release SLO and synthetic probe are healthy".to_owned(),
        observed_at: current.updated_at,
    };
    repository
        .insert_release_observation(
            Uuid::new_v4(),
            &current,
            None,
            &observation,
            None,
            &[audit(
                &fixture,
                correlation_id,
                AuditEventKind::ReleaseObservationCaptured,
                "release",
                current.id.to_string(),
                "RepositoryTestObservationCaptured",
                current.updated_at,
            )],
            &[],
        )
        .await
        .expect("release observation");
    assert_eq!(
        repository
            .release_observations(fixture.tenant_id, current.id)
            .await
            .expect("release observations"),
        vec![observation.clone()]
    );

    let report = ReleaseReport {
        schema_version: "rocketmq-sre.release-report.v1".to_owned(),
        id: ReleaseReportId::new(),
        release_id: current.id,
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        incident_id: fixture.incident_id,
        change_id: current.change_id.clone(),
        release_ref: current.release_ref.clone(),
        final_status: ReleaseStatus::Completed,
        before: Vec::new(),
        during: Vec::new(),
        after: vec![observation],
        generated_at: current.updated_at,
    };
    let persisted = repository
        .insert_release_report(
            &report,
            &audit(
                &fixture,
                correlation_id,
                AuditEventKind::ReleaseReportGenerated,
                "release_report",
                report.id.to_string(),
                "RepositoryTestReportGenerated",
                report.generated_at,
            ),
        )
        .await
        .expect("release report");
    assert_eq!(persisted, report);
    assert_eq!(
        repository
            .insert_release_report(
                &report,
                &audit(
                    &fixture,
                    correlation_id,
                    AuditEventKind::ReleaseReportGenerated,
                    "release_report",
                    report.id.to_string(),
                    "RepositoryTestReportDuplicate",
                    report.generated_at,
                ),
            )
            .await
            .expect("idempotent release report"),
        report
    );
    assert!(
        sqlx::query("UPDATE release_reports SET final_status = 'failed' WHERE release_id = $1")
            .bind(current.id.as_uuid())
            .execute(&repository.pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE release_workflows SET plan_hash = $2 WHERE id = $1")
            .bind(current.id.as_uuid())
            .bind(digest('f'))
            .execute(&repository.pool)
            .await
            .is_err()
    );
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_enterprise_integration_events_are_signed_scoped_and_idempotent() {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 5)
        .await
        .expect("repository with enterprise integration migrations");
    let fixture = seed_fixture(&repository).await;
    let now = Utc::now().with_nanosecond(0).expect("whole-second timestamp");
    let target = IntegrationTargetView {
        target: IntegrationTarget {
            id: IntegrationTargetId::new(),
            tenant_id: fixture.tenant_id,
            cluster_id: Some(fixture.cluster_id),
            descriptor_id: "rocketmq-sre.integration.mock-cmdb.v1".to_owned(),
            descriptor_version: "1.0.0".to_owned(),
            name: format!("CMDB repository test {}", Uuid::new_v4()),
            adapter_kind: IntegrationAdapterKind::MockCmdb,
            endpoint: "https://cmdb.example.test/events".to_owned(),
            secret_reference: Some("env:ROCKETMQ_SRE_CMDB_TEST_SECRET".to_owned()),
            enabled: true,
            inbound_approval: false,
            outbound_events: BTreeSet::new(),
            created_at: now,
            updated_at: now,
        },
        notification_target_id: None,
    };
    repository
        .insert_integration_target(
            &target,
            &audit(
                &fixture,
                CorrelationId::new(),
                AuditEventKind::IntegrationTargetRegistered,
                "integration_target",
                target.target.id.to_string(),
                "EnterpriseRepositoryTargetRegistered",
                now,
            ),
        )
        .await
        .expect("CMDB target");
    let target = repository
        .rotate_integration_secret(
            &target,
            "env:ROCKETMQ_SRE_CMDB_ROTATED_SECRET",
            now + Duration::seconds(1),
            &audit(
                &fixture,
                CorrelationId::new(),
                AuditEventKind::StateChanged,
                "integration_target",
                target.target.id.to_string(),
                "EnterpriseRepositorySecretRotated",
                now + Duration::seconds(1),
            ),
        )
        .await
        .expect("secret reference rotation");
    assert_eq!(
        target.target.secret_reference.as_deref(),
        Some("env:ROCKETMQ_SRE_CMDB_ROTATED_SECRET")
    );
    let event = EnterpriseIntegrationEvent {
        schema_version: ENTERPRISE_INTEGRATION_EVENT_SCHEMA_VERSION.to_owned(),
        id: EnterpriseIntegrationEventId::new(),
        target_id: target.target.id,
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        event_kind: EnterpriseIntegrationEventKind::CmdbSnapshot,
        external_event_id: format!("cmdb-event-{}", Uuid::new_v4()),
        source_version: "1.0.0".to_owned(),
        payload_digest: unique_digest(),
        payload: EnterpriseIntegrationPayload::Cmdb(CmdbSnapshot {
            cluster_id: fixture.cluster_id,
            owner: "messaging-platform".to_owned(),
            environment: "test".to_owned(),
            service_dependencies: BTreeSet::from(["nameserver".to_owned()]),
            labels: std::collections::BTreeMap::from([("tier".to_owned(), "messaging".to_owned())]),
        }),
        signature_verified: true,
        occurred_at: now,
        received_at: now,
    };
    let nonce = format!("nonce-{}", Uuid::new_v4());
    let (stored, duplicate, followup) = repository
        .store_enterprise_integration_event(&event, &nonce)
        .await
        .expect("signed enterprise event");
    assert_eq!(stored, event);
    assert!(!duplicate);
    assert!(followup.is_none());
    let (reloaded, duplicate, _) = repository
        .store_enterprise_integration_event(&event, "different-valid-nonce")
        .await
        .expect("idempotent enterprise event");
    assert_eq!(reloaded, event);
    assert!(duplicate);
    let replay = EnterpriseIntegrationEvent {
        id: EnterpriseIntegrationEventId::new(),
        external_event_id: format!("cmdb-event-{}", Uuid::new_v4()),
        ..event.clone()
    };
    assert!(
        repository
            .store_enterprise_integration_event(&replay, &nonce)
            .await
            .is_err()
    );
    let events = repository
        .enterprise_events(
            fixture.tenant_id,
            target.target.id,
            Some(EnterpriseIntegrationEventKind::CmdbSnapshot),
            10,
        )
        .await
        .expect("enterprise event history");
    assert_eq!(events, vec![event]);

    let gitops_target = IntegrationTargetView {
        target: IntegrationTarget {
            id: IntegrationTargetId::new(),
            tenant_id: fixture.tenant_id,
            cluster_id: Some(fixture.cluster_id),
            descriptor_id: "rocketmq-sre.integration.mock-gitops.v1".to_owned(),
            descriptor_version: "1.0.0".to_owned(),
            name: format!("GitOps repository test {}", Uuid::new_v4()),
            adapter_kind: IntegrationAdapterKind::MockGitOps,
            endpoint: "https://gitops.example.test/events".to_owned(),
            secret_reference: Some("env:ROCKETMQ_SRE_GITOPS_TEST_SECRET".to_owned()),
            enabled: true,
            inbound_approval: false,
            outbound_events: BTreeSet::new(),
            created_at: now,
            updated_at: now,
        },
        notification_target_id: None,
    };
    repository
        .insert_integration_target(
            &gitops_target,
            &audit(
                &fixture,
                CorrelationId::new(),
                AuditEventKind::IntegrationTargetRegistered,
                "integration_target",
                gitops_target.target.id.to_string(),
                "EnterpriseRepositoryGitOpsTargetRegistered",
                now,
            ),
        )
        .await
        .expect("GitOps target");
    let gitops_event = EnterpriseIntegrationEvent {
        schema_version: ENTERPRISE_INTEGRATION_EVENT_SCHEMA_VERSION.to_owned(),
        id: EnterpriseIntegrationEventId::new(),
        target_id: gitops_target.target.id,
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        event_kind: EnterpriseIntegrationEventKind::GitOpsSnapshot,
        external_event_id: format!("gitops-event-{}", Uuid::new_v4()),
        source_version: "1.0.0".to_owned(),
        payload_digest: unique_digest(),
        payload: EnterpriseIntegrationPayload::GitOps(GitOpsSnapshot {
            cluster_id: fixture.cluster_id,
            repository_ref: "rocketmq/platform-config".to_owned(),
            commit_sha: "a".repeat(40),
            desired_image_digest: Some(digest('a')),
            configuration_digest: Some(digest('b')),
            feature_digest: Some(digest('c')),
            rollout_link: Some("https://gitops.example.test/rollouts/phase5".to_owned()),
        }),
        signature_verified: true,
        occurred_at: now,
        received_at: now,
    };
    let (_, duplicate, _) = repository
        .store_enterprise_integration_event(&gitops_event, &format!("gitops-nonce-{}", Uuid::new_v4()))
        .await
        .expect("GitOps event");
    assert!(!duplicate);
    let (persisted_gitops, duplicate, _) = repository
        .store_enterprise_integration_event(&gitops_event, &format!("gitops-retry-{}", Uuid::new_v4()))
        .await
        .expect("idempotent GitOps event");
    assert_eq!(persisted_gitops, gitops_event);
    assert!(duplicate);

    let health = IntegrationHealth {
        target_id: target.target.id,
        status: IntegrationHealthStatus::Healthy,
        config_valid: true,
        secret_available: true,
        endpoint_valid: true,
        last_delivery_at: None,
        last_error_code: None,
        observed_at: now,
    };
    repository
        .store_integration_health(&health)
        .await
        .expect("integration health");
    assert_eq!(
        repository
            .integration_health(fixture.tenant_id, target.target.id)
            .await
            .expect("latest integration health"),
        health
    );

    let release_target = IntegrationTargetView {
        target: IntegrationTarget {
            id: IntegrationTargetId::new(),
            tenant_id: fixture.tenant_id,
            cluster_id: Some(fixture.cluster_id),
            descriptor_id: "rocketmq-sre.integration.signed-release-webhook.v1".to_owned(),
            descriptor_version: "1.0.0".to_owned(),
            name: format!("CI repository test {}", Uuid::new_v4()),
            adapter_kind: IntegrationAdapterKind::SignedReleaseWebhook,
            endpoint: "https://ci.example.test/events".to_owned(),
            secret_reference: Some("env:ROCKETMQ_SRE_CI_TEST_SECRET".to_owned()),
            enabled: true,
            inbound_approval: false,
            outbound_events: BTreeSet::new(),
            created_at: now,
            updated_at: now,
        },
        notification_target_id: None,
    };
    repository
        .insert_integration_target(
            &release_target,
            &audit(
                &fixture,
                CorrelationId::new(),
                AuditEventKind::IntegrationTargetRegistered,
                "integration_target",
                release_target.target.id.to_string(),
                "EnterpriseRepositoryReleaseTargetRegistered",
                now,
            ),
        )
        .await
        .expect("CI/CD target");
    let release_event = EnterpriseIntegrationEvent {
        schema_version: ENTERPRISE_INTEGRATION_EVENT_SCHEMA_VERSION.to_owned(),
        id: EnterpriseIntegrationEventId::new(),
        target_id: release_target.target.id,
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        event_kind: EnterpriseIntegrationEventKind::ReleaseStarted,
        external_event_id: format!("release-event-{}", Uuid::new_v4()),
        source_version: "1.0.0".to_owned(),
        payload_digest: unique_digest(),
        payload: EnterpriseIntegrationPayload::Release(ReleasePipelineEvent {
            cluster_id: fixture.cluster_id,
            release_ref: format!("release-{}", Uuid::new_v4()),
            change_id: format!("change-{}", Uuid::new_v4()),
            artifact_digest: unique_digest(),
            target_version: "5.3.2".to_owned(),
        }),
        signature_verified: true,
        occurred_at: now,
        received_at: now,
    };
    repository
        .store_enterprise_integration_event(&release_event, &format!("release-nonce-{}", Uuid::new_v4()))
        .await
        .expect("CI/CD release event");
    let followup_id = Uuid::new_v4();
    repository
        .record_enterprise_followup(fixture.tenant_id, release_event.id, followup_id)
        .await
        .expect("read-only readiness follow-up");
    let (_, duplicate, persisted_followup) = repository
        .store_enterprise_integration_event(&release_event, &format!("release-retry-{}", Uuid::new_v4()))
        .await
        .expect("idempotent CI/CD release event");
    assert!(duplicate);
    assert_eq!(persisted_followup, Some(followup_id));
}

#[derive(Clone)]
struct Fixture {
    tenant_id: TenantId,
    cluster_id: ClusterId,
    incident_id: IncidentId,
    plan_id: ActionPlanId,
    plan_hash: String,
    runbook_id: RunbookId,
}

async fn seed_fixture(repository: &PostgresRepository) -> Fixture {
    let fixture = Fixture {
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        incident_id: IncidentId::new(),
        plan_id: ActionPlanId::new(),
        plan_hash: unique_digest(),
        runbook_id: RunbookId::new(),
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
                   'release-repository-test', 'read_only', 'read_only', 'ready_read_only')",
    )
    .bind(fixture.cluster_id.as_uuid())
    .bind(fixture.tenant_id.as_uuid())
    .bind(format!("release-repository-{}", fixture.cluster_id))
    .execute(&repository.pool)
    .await
    .expect("cluster");
    sqlx::query(
        "INSERT INTO sre_incidents (
            id, tenant_id, cluster_id, title, resource, symptom_family,
            fingerprint, status, workflow_checkpoint, created_by_subject,
            created_at, updated_at
         ) VALUES ($1, $2, $3, 'Release repository test', 'deployment/default/proxy',
                   'release', $4, 'diagnosing', '{}'::JSONB,
                   'release-repository-test', NOW(), NOW())",
    )
    .bind(fixture.incident_id.as_uuid())
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .bind(digest('1'))
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
                   'release-fixture', 'release-fixture', 'r1', 'local',
                   'local', 'local', '[]'::JSONB, '{}'::JSONB, 100,
                   'test-reference', 'gateway', TRUE, 'healthy', NOW(), NOW())",
    )
    .bind(profile_id)
    .bind(fixture.tenant_id.as_uuid())
    .bind(format!("release-repository-{profile_id}"))
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
                   'openai-compatible', 'release-fixture', 'r1', 'local',
                   '{}', 'release-repository-test', 'rocketmq-sre.model.v1',
                   'release repository fixture', NOW(), NOW())",
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
                   'r2', 'approved', '{}'::JSONB, 'release-operator',
                   NOW(), NOW() + INTERVAL '1 hour', NOW())",
    )
    .bind(fixture.plan_id.as_uuid())
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .bind(fixture.incident_id.as_uuid())
    .bind(diagnosis_id)
    .bind(invocation_id)
    .bind(&fixture.plan_hash)
    .bind(digest('e'))
    .execute(&repository.pool)
    .await
    .expect("action plan");
    sqlx::query(
        "INSERT INTO runbook_definitions (
            tenant_id, cluster_id, id, version, risk,
            definition_snapshot, created_by, created_at
         ) VALUES ($1, $2, $3, '1.0.0', 'r2', '{}'::JSONB,
                   'release-operator', NOW())",
    )
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .bind(fixture.runbook_id.as_uuid())
    .execute(&repository.pool)
    .await
    .expect("runbook");
    fixture
}

fn integration_target(fixture: &Fixture, now: chrono::DateTime<Utc>) -> IntegrationTargetView {
    IntegrationTargetView {
        target: IntegrationTarget {
            id: IntegrationTargetId::new(),
            tenant_id: fixture.tenant_id,
            cluster_id: Some(fixture.cluster_id),
            descriptor_id: "rocketmq-sre.integration.mock-itsm".to_owned(),
            descriptor_version: "1.0.0".to_owned(),
            name: format!("Release repository ITSM {}", Uuid::new_v4()),
            adapter_kind: IntegrationAdapterKind::MockItsm,
            endpoint: "http://mock-itsm.invalid/events".to_owned(),
            secret_reference: None,
            enabled: true,
            inbound_approval: true,
            outbound_events: BTreeSet::from([IntegrationEventKind::ReleaseStarted]),
            created_at: now,
            updated_at: now,
        },
        notification_target_id: None,
    }
}

async fn notification_integration_target(
    repository: &PostgresRepository,
    fixture: &Fixture,
    now: chrono::DateTime<Utc>,
    adapter_kind: IntegrationAdapterKind,
    channel: &str,
) -> IntegrationTargetView {
    let notification_target_id = NotificationTargetId::new();
    let (adapter_name, descriptor_id) = match adapter_kind {
        IntegrationAdapterKind::ChatOpsWebhook => ("chatops", "rocketmq-sre.integration.chatops-webhook.v1"),
        IntegrationAdapterKind::Pager => ("pager", "rocketmq-sre.integration.pager.v1"),
        _ => panic!("test helper only accepts notification-backed representative adapters"),
    };
    let endpoint = format!("https://{adapter_name}.example.test/events");
    let secret_reference = format!("env:ROCKETMQ_SRE_{}_TEST_SECRET", adapter_name.to_ascii_uppercase());
    sqlx::query(
        "INSERT INTO notification_targets (
            id, tenant_id, cluster_id, name, channel, endpoint,
            secret_reference, enabled, created_at, updated_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE, $8, $8)",
    )
    .bind(notification_target_id.as_uuid())
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .bind(format!("Phase 5 {adapter_name} {}", Uuid::new_v4()))
    .bind(channel)
    .bind(&endpoint)
    .bind(&secret_reference)
    .bind(now)
    .execute(&repository.pool)
    .await
    .expect("notification target fixture");

    IntegrationTargetView {
        target: IntegrationTarget {
            id: IntegrationTargetId::new(),
            tenant_id: fixture.tenant_id,
            cluster_id: Some(fixture.cluster_id),
            descriptor_id: descriptor_id.to_owned(),
            descriptor_version: "1.0.0".to_owned(),
            name: format!("Phase 5 {adapter_name} integration {}", Uuid::new_v4()),
            adapter_kind,
            endpoint,
            secret_reference: Some(secret_reference),
            enabled: true,
            inbound_approval: false,
            outbound_events: BTreeSet::from([IntegrationEventKind::ReleaseStarted]),
            created_at: now,
            updated_at: now,
        },
        notification_target_id: Some(notification_target_id),
    }
}

fn release_workflow(fixture: &Fixture, correlation_id: CorrelationId, now: chrono::DateTime<Utc>) -> ReleaseWorkflow {
    ReleaseWorkflow {
        schema_version: "rocketmq-sre.release-workflow.v1".to_owned(),
        id: ReleaseId::new(),
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        incident_id: fixture.incident_id,
        correlation_id,
        change_id: format!("CHG-{}", Uuid::new_v4()),
        release_ref: format!("REL-{}", Uuid::new_v4()),
        target_version: "5.3.0".to_owned(),
        runbook_id: fixture.runbook_id,
        runbook_version: "1.0.0".to_owned(),
        plan_id: fixture.plan_id,
        plan_hash: fixture.plan_hash.clone(),
        rollback_plan_id: None,
        rollback_plan_hash: None,
        readiness: None,
        status: ReleaseStatus::Planned,
        active_execution_id: None,
        regression_detected: false,
        pause_reason: None,
        created_by: "release-operator".to_owned(),
        created_at: now,
        updated_at: now,
    }
}

fn integration_delivery(
    fixture: &Fixture,
    target: &IntegrationTargetView,
    workflow: &ReleaseWorkflow,
    now: chrono::DateTime<Utc>,
) -> IntegrationDelivery {
    IntegrationDelivery {
        schema_version: INTEGRATION_DELIVERY_SCHEMA_VERSION.to_owned(),
        id: IntegrationDeliveryId::new(),
        target_id: target.target.id,
        descriptor_id: target.target.descriptor_id.clone(),
        descriptor_version: target.target.descriptor_version.clone(),
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        incident_id: fixture.incident_id,
        plan_id: Some(fixture.plan_id),
        release_id: Some(workflow.id),
        event_kind: IntegrationEventKind::ReleaseStarted,
        idempotency_key: format!("release:{}:started", workflow.id),
        sanitized_summary: "Release started under supervised execution".to_owned(),
        deep_link: format!("/changes/releases/{}", workflow.id),
        status: IntegrationDeliveryStatus::Pending,
        attempt_count: 0,
        next_attempt_at: Some(now),
        last_error_code: None,
        delivered_at: None,
        created_at: now,
    }
}

fn release_event(
    workflow: &ReleaseWorkflow,
    from_status: Option<ReleaseStatus>,
    reason_code: &str,
) -> ReleaseEventRecord {
    ReleaseEventRecord {
        id: Uuid::new_v4(),
        release_id: workflow.id,
        correlation_id: workflow.correlation_id,
        from_status,
        to_status: workflow.status,
        reason_code: reason_code.to_owned(),
        actor_subject: "release-repository-test".to_owned(),
        details: json!({}),
        occurred_at: workflow.updated_at,
    }
}

fn audit(
    fixture: &Fixture,
    correlation_id: CorrelationId,
    event_kind: AuditEventKind,
    resource_kind: &str,
    resource_id: String,
    reason_code: &str,
    occurred_at: chrono::DateTime<Utc>,
) -> AuditEvent {
    AuditEvent {
        id: AuditEventId::new(),
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        correlation_id,
        event_kind,
        actor_subject: "release-repository-test".to_owned(),
        actor_role: "operator".to_owned(),
        resource_kind: resource_kind.to_owned(),
        resource_id,
        reason_code: reason_code.to_owned(),
        details: json!({}),
        occurred_at,
    }
}

fn digest(value: char) -> String {
    format!("sha256:{}", value.to_string().repeat(64))
}

fn unique_digest() -> String {
    let value = Uuid::new_v4().simple().to_string();
    format!("sha256:{value}{value}")
}

fn assert_delivery_projection(actual: &IntegrationDelivery, expected: &IntegrationDelivery) {
    let mut actual = actual.clone();
    let mut expected = expected.clone();
    actual.next_attempt_at = None;
    expected.next_attempt_at = None;
    assert_eq!(actual, expected);
}
