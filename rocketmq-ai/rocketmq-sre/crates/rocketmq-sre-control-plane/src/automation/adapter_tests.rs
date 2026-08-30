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
use std::time::Duration;

use chrono::Utc;
use rocketmq_sre_contracts::AUTOMATION_SCHEMA_VERSION;
use rocketmq_sre_contracts::AutomationBudget;
use rocketmq_sre_contracts::AutomationRunId;
use rocketmq_sre_contracts::AutomationRunStatus;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ConnectorCapabilityState;
use rocketmq_sre_contracts::ConnectorRegister;
use rocketmq_sre_contracts::ConnectorResponseEnvelope;
use rocketmq_sre_contracts::ConnectorSessionId;
use rocketmq_sre_contracts::ConnectorSourceCapability;
use rocketmq_sre_contracts::ConnectorSourceStatus;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::NoSideEffectAutomationKind;
use rocketmq_sre_contracts::NoSideEffectAutomationRequest;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::current_evidence_schema;
use serde_json::json;
use uuid::Uuid;

use super::AutomationService;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::connector_channel::ConnectorCommand;
use crate::connector_channel::ConnectorPrincipal;
use crate::connector_channel::PollRequest;
use crate::connector_channel::PostgresConnectorChannelService;
use crate::connector_channel::channel_schema;
use crate::evidence::EvidenceBlobStore;
use crate::evidence::EvidenceService;
use crate::models::ModelGatewayService;
use crate::postmortem::PostmortemService;
use crate::workflow::WorkflowEventBus;
use crate::workflow::WorkflowService;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_no_side_effect_adapters_complete_all_six_kinds_without_mutation_authority() {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 8)
        .await
        .expect("repository with migrations");
    let fixture = seed_fixture(&repository).await;
    let auth = AuthContext {
        tenant_id: fixture.tenant_id,
        subject: "automation-adapter-test".to_owned(),
        clusters: BTreeSet::from([fixture.cluster_id]),
        roles: BTreeSet::from(["operator".to_owned()]),
    };
    let connector =
        PostgresConnectorChannelService::postgres(repository.clone(), "automation-adapter-token").expect("connector");
    let evidence = EvidenceService::new(repository.clone(), EvidenceBlobStore::in_memory(64 * 1024));
    let workflow = WorkflowService::new(repository.clone(), WorkflowEventBus::new(32));
    let postmortems = PostmortemService::new(
        repository.clone(),
        evidence.clone(),
        ModelGatewayService::disabled(repository.clone()),
        workflow,
    );
    let service = AutomationService::new(repository.clone(), connector.clone(), evidence, postmortems)
        .expect("automation service");

    let mut runs = Vec::new();
    for kind in [
        NoSideEffectAutomationKind::AlertCorrelation,
        NoSideEffectAutomationKind::SeverityOwnerSuggestion,
        NoSideEffectAutomationKind::ShiftSummary,
        NoSideEffectAutomationKind::Notification,
        NoSideEffectAutomationKind::PostmortemDraft,
    ] {
        let request = automation_request(&fixture, &auth, kind);
        let run = service.submit(&auth, &request).await.expect("bounded automation");
        assert_eq!(run.status, AutomationRunStatus::Succeeded, "{kind:?}");
        runs.push(run);
    }

    let (principal, session_id) = register_connector(&connector, &fixture).await;
    let evidence_request = automation_request(&fixture, &auth, NoSideEffectAutomationKind::EvidenceCollection);
    let submit = service.submit(&auth, &evidence_request);
    let respond = respond_with_cluster_overview(&connector, &principal, session_id);
    let (evidence_run, ()) = tokio::time::timeout(Duration::from_secs(10), async { tokio::join!(submit, respond) })
        .await
        .expect("Evidence automation should finish before its deadline");
    let evidence_run = evidence_run.expect("Evidence automation");
    assert_eq!(evidence_run.status, AutomationRunStatus::Succeeded);
    runs.push(evidence_run);

    assert_eq!(runs.len(), 6);
    assert!(runs.iter().all(|run| run.status == AutomationRunStatus::Succeeded));
    assert!(runs.iter().all(|run| !run.artifacts.is_empty()));
    let status: String = sqlx::query_scalar("SELECT status FROM sre_incidents WHERE id = $1")
        .bind(fixture.incident_id.as_uuid())
        .fetch_one(&repository.pool)
        .await
        .expect("incident status");
    assert_eq!(status, "diagnosing");
    let notification_status: String = sqlx::query_scalar(
        "SELECT status
         FROM notification_outbox
         WHERE tenant_id = $1 AND incident_id = $2
         ORDER BY created_at DESC
         LIMIT 1",
    )
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.incident_id.as_uuid())
    .fetch_one(&repository.pool)
    .await
    .expect("notification outbox");
    assert_eq!(notification_status, "pending");
    let postmortem_status: String = sqlx::query_scalar(
        "SELECT status
         FROM postmortems
         WHERE tenant_id = $1 AND incident_id = $2",
    )
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.incident_id.as_uuid())
    .fetch_one(&repository.pool)
    .await
    .expect("postmortem draft");
    assert_eq!(postmortem_status, "draft");
    let evidence_links: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM evidence_links WHERE incident_id = $1")
        .bind(fixture.incident_id.as_uuid())
        .fetch_one(&repository.pool)
        .await
        .expect("Evidence links");
    assert!(evidence_links >= 1);
}

struct Fixture {
    tenant_id: TenantId,
    cluster_id: ClusterId,
    incident_id: IncidentId,
}

async fn seed_fixture(repository: &PostgresRepository) -> Fixture {
    let fixture = Fixture {
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        incident_id: IncidentId::new(),
    };
    sqlx::query(
        "INSERT INTO clusters (
            id, tenant_id, external_cluster_key, environment, region,
            rocketmq_version, deployment_mode, owner_name,
            requested_access_profile, effective_access_profile, onboarding_state
         ) VALUES (
            $1, $2, $3, 'test', 'local',
            'test', 'docker', 'automation-adapter-test',
            'read_only', 'read_only', 'ready_read_only'
         )",
    )
    .bind(fixture.cluster_id.as_uuid())
    .bind(fixture.tenant_id.as_uuid())
    .bind(format!("automation-adapter-{}", fixture.cluster_id))
    .execute(&repository.pool)
    .await
    .expect("cluster");
    sqlx::query(
        "INSERT INTO connector_identities (
            id, cluster_id, subject, issuer, created_at
         ) VALUES ($1, $2, $3, 'automation-adapter-test', NOW())",
    )
    .bind(Uuid::new_v4())
    .bind(fixture.cluster_id.as_uuid())
    .bind(format!("automation-connector-{}", fixture.cluster_id))
    .execute(&repository.pool)
    .await
    .expect("connector identity");
    sqlx::query(
        "INSERT INTO sre_incidents (
            id, tenant_id, cluster_id, title, resource, symptom_family,
            fingerprint, severity, owner_name, occurrence_count, last_alert_at,
            status, workflow_checkpoint, created_by_subject, created_at, updated_at
         ) VALUES (
            $1, $2, $3, 'Automation adapter incident', 'broker/test', 'broker_unavailable',
            $4, 'critical', 'platform-team', 3, NOW(),
            'diagnosing', '{}'::JSONB, 'automation-adapter-test', NOW(), NOW()
         )",
    )
    .bind(fixture.incident_id.as_uuid())
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .bind(format!("automation-adapter:{}", fixture.incident_id))
    .execute(&repository.pool)
    .await
    .expect("incident");
    let alert_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO alert_events (
            id, tenant_id, cluster_id, source, source_event_id, fingerprint,
            correlation_key, affected_resource, symptom_family, severity,
            status, summary, labels, evidence_ids, occurrence_count,
            last_sequence, first_occurred_at, last_occurred_at, received_at
         ) VALUES (
            $1, $2, $3, 'synthetic_probe', $4, $5,
            '{}'::JSONB, '{}'::JSONB, 'broker_unavailable', 'critical',
            'firing', 'bounded synthetic alert', '{}'::JSONB, '{}', 3,
            3, NOW(), NOW(), NOW()
         )",
    )
    .bind(alert_id)
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .bind(format!("automation-adapter-alert-{alert_id}"))
    .bind(format!("fingerprint:{alert_id}"))
    .execute(&repository.pool)
    .await
    .expect("alert");
    sqlx::query(
        "INSERT INTO incident_alerts (
            incident_id, alert_id, tenant_id, cluster_id, linked_at
         ) VALUES ($1, $2, $3, $4, NOW())",
    )
    .bind(fixture.incident_id.as_uuid())
    .bind(alert_id)
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .execute(&repository.pool)
    .await
    .expect("incident alert link");
    sqlx::query(
        "INSERT INTO notification_targets (
            id, tenant_id, cluster_id, name, channel, endpoint,
            secret_reference, enabled, created_at, updated_at
         ) VALUES (
            $1, $2, $3, 'automation-email', 'email', 'automation@example.test',
            NULL, TRUE, NOW(), NOW()
         )",
    )
    .bind(Uuid::new_v4())
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .execute(&repository.pool)
    .await
    .expect("notification target");
    fixture
}

fn automation_request(
    fixture: &Fixture,
    auth: &AuthContext,
    kind: NoSideEffectAutomationKind,
) -> NoSideEffectAutomationRequest {
    let name = match kind {
        NoSideEffectAutomationKind::AlertCorrelation => "alert-correlation",
        NoSideEffectAutomationKind::SeverityOwnerSuggestion => "severity-owner",
        NoSideEffectAutomationKind::EvidenceCollection => "evidence",
        NoSideEffectAutomationKind::ShiftSummary => "shift-summary",
        NoSideEffectAutomationKind::Notification => "notification",
        NoSideEffectAutomationKind::PostmortemDraft => "postmortem",
    };
    NoSideEffectAutomationRequest {
        schema_version: AUTOMATION_SCHEMA_VERSION.to_owned(),
        id: AutomationRunId::new(),
        tenant_id: fixture.tenant_id,
        cluster_id: (kind != NoSideEffectAutomationKind::ShiftSummary).then_some(fixture.cluster_id),
        incident_id: (kind != NoSideEffectAutomationKind::ShiftSummary).then_some(fixture.incident_id),
        correlation_id: CorrelationId::new(),
        kind,
        idempotency_key: format!("automation:{name}:{}", Uuid::new_v4()),
        budget: AutomationBudget {
            max_model_calls: u8::from(kind == NoSideEffectAutomationKind::PostmortemDraft),
            max_output_bytes: 16_384,
            timeout_seconds: 5,
        },
        evidence_ids: Vec::new(),
        requested_by: auth.subject.clone(),
        requested_at: Utc::now(),
    }
}

async fn register_connector(
    connector: &PostgresConnectorChannelService,
    fixture: &Fixture,
) -> (ConnectorPrincipal, ConnectorSessionId) {
    let principal = ConnectorPrincipal {
        subject: format!("automation-connector-{}", fixture.cluster_id),
        issuer: "automation-adapter-test".to_owned(),
    };
    let session_id = ConnectorSessionId::new();
    connector
        .register(
            &principal,
            &ConnectorRegister {
                schema: channel_schema(),
                session_id,
                tenant_id: fixture.tenant_id,
                cluster_id: fixture.cluster_id,
                subject: principal.subject.clone(),
                capability: ConnectorCapabilityState {
                    mutation_supported: false,
                    sources: vec![ConnectorSourceCapability {
                        source: "rocketmq-mcp".to_owned(),
                        schema_major: 1,
                        status: ConnectorSourceStatus::Queryable,
                        max_rows: 100,
                        max_bytes: 65_536,
                        max_time_range_seconds: 3_600,
                        last_success_at: None,
                        freshness_seconds: Some(15),
                    }],
                },
                observed_at: Utc::now(),
            },
        )
        .await
        .expect("register connector");
    (principal, session_id)
}

async fn respond_with_cluster_overview(
    connector: &PostgresConnectorChannelService,
    principal: &ConnectorPrincipal,
    session_id: ConnectorSessionId,
) {
    loop {
        let poll = connector
            .poll(
                principal,
                session_id,
                &PollRequest {
                    schema: channel_schema(),
                    session_id,
                    after_sequence: 0,
                    wait_millis: 100,
                    max_commands: 8,
                },
            )
            .await
            .expect("poll connector commands");
        let command = poll
            .commands
            .into_iter()
            .find(|command| matches!(command, ConnectorCommand::Query { .. }));
        if let Some(ConnectorCommand::Query { envelope }) = command {
            let evidence = EvidenceSnapshot::capture(
                envelope.query,
                current_evidence_schema(),
                Utc::now(),
                EvidenceContent::Inline(json!({
                    "brokers": 1,
                    "topics": 1,
                    "mutation_supported": false
                })),
            )
            .expect("canonical Evidence");
            connector
                .submit_response(
                    principal,
                    session_id,
                    &ConnectorResponseEnvelope {
                        schema: channel_schema(),
                        session_id,
                        correlation_id: envelope.correlation_id,
                        sequence: envelope.sequence,
                        evidence: Some(evidence),
                        error_code: None,
                        retryable: false,
                    },
                )
                .await
                .expect("submit connector response");
            return;
        }
        tokio::task::yield_now().await;
    }
}
