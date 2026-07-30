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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use chrono::Utc;
use rocketmq_sre_contracts::AlertSeverity;
use rocketmq_sre_contracts::AlertSource;
use rocketmq_sre_contracts::AlertStatus;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::InspectionTemplate;
use rocketmq_sre_contracts::ResourceKind;
use rocketmq_sre_contracts::TenantId;
use uuid::Uuid;

use super::EventEntrySourceKind;
use super::EventEntryTargetKind;
use super::EventEntryWorkflowTarget;
use super::UnifiedEventEntryRequest;
use super::UnifiedEventEntryService;
use super::UnifiedEventPayload;
use super::WorkflowEventBus;
use super::WorkflowService;
use super::event_entry_model::ChangeEventKind;
use super::event_entry_model::EVENT_ENTRY_SCHEMA;
use super::event_entry_model::ExternalEventChannel;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::alerting::AlertingService;
use crate::auth::AuthContext;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn five_event_sources_create_replay_and_isolate_workflow_targets() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let repository = PostgresRepository::connect(&database_url, 8)
        .await
        .expect("database and migrations");
    let tenant_id = TenantId::new();
    let cluster_id = ClusterId::new();
    seed_cluster(&repository, tenant_id, cluster_id).await;
    let auth = AuthContext {
        tenant_id,
        subject: "unified-event-entry-test".to_owned(),
        clusters: BTreeSet::from([cluster_id]),
        roles: BTreeSet::from(["operator".to_owned()]),
    };
    let workflow = WorkflowService::new(repository.clone(), WorkflowEventBus::new(32));
    let alerting = AlertingService::new(repository.clone(), workflow.clone()).expect("alerting service");
    let service = UnifiedEventEntryService::new(repository.clone(), workflow, alerting);
    let suffix = Uuid::new_v4();
    let occurred_at = Utc::now();
    let requests = vec![
        UnifiedEventEntryRequest {
            schema_version: EVENT_ENTRY_SCHEMA.to_owned(),
            cluster_id,
            idempotency_key: format!("event-entry:alert:{suffix}"),
            occurred_at: Some(occurred_at),
            payload: UnifiedEventPayload::Alert {
                source: AlertSource::Alertmanager,
                source_event_id: format!("broker-down-{suffix}"),
                resource_kind: ResourceKind::Broker,
                resource_key: "broker-a".to_owned(),
                display_name: Some("Broker A".to_owned()),
                symptom_family: "broker_unavailable".to_owned(),
                severity: AlertSeverity::Critical,
                status: AlertStatus::Firing,
                summary: "Broker readiness failed".to_owned(),
                labels: BTreeMap::from([("owner".to_owned(), "messaging".to_owned())]),
                evidence_ids: Vec::new(),
                sequence: 1,
            },
        },
        UnifiedEventEntryRequest {
            schema_version: EVENT_ENTRY_SCHEMA.to_owned(),
            cluster_id,
            idempotency_key: format!("event-entry:manual:{suffix}"),
            occurred_at: Some(occurred_at),
            payload: UnifiedEventPayload::ManualIssue {
                title: "Investigate intermittent consumer lag".to_owned(),
                resource: Some("consumer-group:orders".to_owned()),
                symptom_family: "consumer_lag".to_owned(),
            },
        },
        UnifiedEventEntryRequest {
            schema_version: EVENT_ENTRY_SCHEMA.to_owned(),
            cluster_id,
            idempotency_key: format!("event-entry:inspection:{suffix}"),
            occurred_at: Some(occurred_at),
            payload: UnifiedEventPayload::ScheduledInspection {
                template: InspectionTemplate::FullCluster,
                schedule: Some("every 37m".to_owned()),
            },
        },
        UnifiedEventEntryRequest {
            schema_version: EVENT_ENTRY_SCHEMA.to_owned(),
            cluster_id,
            idempotency_key: format!("event-entry:change:{suffix}"),
            occurred_at: Some(occurred_at),
            payload: UnifiedEventPayload::ChangeEvent {
                change_kind: ChangeEventKind::Release,
                target: EventEntryWorkflowTarget::Investigation,
                title: "Release 2026.07 readiness observation".to_owned(),
                resource: Some(format!("release:{suffix}")),
                symptom_family: "release_change".to_owned(),
            },
        },
        UnifiedEventEntryRequest {
            schema_version: EVENT_ENTRY_SCHEMA.to_owned(),
            cluster_id,
            idempotency_key: format!("event-entry:external:{suffix}"),
            occurred_at: Some(occurred_at),
            payload: UnifiedEventPayload::ExternalIntegration {
                channel: ExternalEventChannel::ChatOps,
                target: EventEntryWorkflowTarget::Incident,
                title: "ChatOps escalation for broker availability".to_owned(),
                resource: Some("broker:broker-a".to_owned()),
                symptom_family: "broker_unavailable".to_owned(),
            },
        },
    ];
    let expected = [
        (EventEntrySourceKind::Alert, EventEntryTargetKind::Incident),
        (EventEntrySourceKind::ManualIssue, EventEntryTargetKind::Investigation),
        (
            EventEntrySourceKind::ScheduledInspection,
            EventEntryTargetKind::InspectionRun,
        ),
        (EventEntrySourceKind::ChangeEvent, EventEntryTargetKind::Investigation),
        (
            EventEntrySourceKind::ExternalIntegration,
            EventEntryTargetKind::Incident,
        ),
    ];

    for (request, (source_kind, target_kind)) in requests.iter().zip(expected) {
        let created = service
            .ingest(&auth, request, CorrelationId::new())
            .await
            .expect("first event entry");
        assert!(created.created);
        assert!(!created.replayed);
        assert_eq!(created.source_kind, source_kind);
        assert_eq!(created.target_kind, target_kind);
        assert_target_exists(&repository, target_kind, created.target_id).await;

        let replayed = service
            .ingest(&auth, request, CorrelationId::new())
            .await
            .expect("idempotent replay");
        assert!(!replayed.created);
        assert!(replayed.replayed);
        assert_eq!(replayed.entry_id, created.entry_id);
        assert_eq!(replayed.target_id, created.target_id);
        assert_eq!(replayed.correlation_id, created.correlation_id);
    }

    let entry_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM workflow_event_entries
         WHERE tenant_id = $1 AND cluster_id = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .fetch_one(&repository.pool)
    .await
    .expect("event entry count");
    assert_eq!(entry_count, 5);

    let before_conflict: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM investigations WHERE tenant_id = $1 AND cluster_id = $2")
            .bind(tenant_id.as_uuid())
            .bind(cluster_id.as_uuid())
            .fetch_one(&repository.pool)
            .await
            .expect("investigation count before conflict");
    let mut conflicting = requests[1].clone();
    if let UnifiedEventPayload::ManualIssue { title, .. } = &mut conflicting.payload {
        *title = "Different request content under the same idempotency key".to_owned();
    }
    let conflict = service
        .ingest(&auth, &conflicting, CorrelationId::new())
        .await
        .expect_err("same key with different content must fail");
    assert!(matches!(
        conflict,
        ControlPlaneError::Conflict {
            code: "event_entry_idempotency_conflict",
            ..
        }
    ));
    let after_conflict: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM investigations WHERE tenant_id = $1 AND cluster_id = $2")
            .bind(tenant_id.as_uuid())
            .bind(cluster_id.as_uuid())
            .fetch_one(&repository.pool)
            .await
            .expect("investigation count after conflict");
    assert_eq!(after_conflict, before_conflict);

    let cross_tenant_auth = AuthContext {
        tenant_id: TenantId::new(),
        subject: "cross-tenant-entry-test".to_owned(),
        clusters: BTreeSet::from([cluster_id]),
        roles: BTreeSet::new(),
    };
    let mut cross_tenant_request = requests[1].clone();
    cross_tenant_request.idempotency_key = format!("event-entry:cross-tenant:{suffix}");
    let cross_tenant_error = service
        .ingest(&cross_tenant_auth, &cross_tenant_request, CorrelationId::new())
        .await
        .expect_err("database tenant scope must fail closed");
    assert!(matches!(
        cross_tenant_error,
        ControlPlaneError::Forbidden {
            code: "cluster_not_allowed",
            ..
        }
    ));
}

async fn seed_cluster(repository: &PostgresRepository, tenant_id: TenantId, cluster_id: ClusterId) {
    sqlx::query(
        "INSERT INTO clusters (
            id, tenant_id, external_cluster_key, environment, region,
            rocketmq_version, deployment_mode, owner_name,
            requested_access_profile, effective_access_profile, onboarding_state
         ) VALUES (
            $1, $2, $3, 'test', 'local', 'test', 'test', 'event-entry-test',
            'read_only', 'read_only', 'ready_read_only'
         )",
    )
    .bind(cluster_id.as_uuid())
    .bind(tenant_id.to_string())
    .bind(format!("event-entry-{cluster_id}"))
    .execute(&repository.pool)
    .await
    .expect("test cluster");
}

async fn assert_target_exists(repository: &PostgresRepository, kind: EventEntryTargetKind, id: Uuid) {
    let query = match kind {
        EventEntryTargetKind::Investigation => "SELECT EXISTS (SELECT 1 FROM investigations WHERE id = $1)",
        EventEntryTargetKind::Incident => "SELECT EXISTS (SELECT 1 FROM sre_incidents WHERE id = $1)",
        EventEntryTargetKind::InspectionRun => "SELECT EXISTS (SELECT 1 FROM inspection_runs WHERE id = $1)",
    };
    let exists: bool = sqlx::query_scalar(query)
        .bind(id)
        .fetch_one(&repository.pool)
        .await
        .expect("target existence query");
    assert!(exists, "{kind:?} target {id} must exist");
}
