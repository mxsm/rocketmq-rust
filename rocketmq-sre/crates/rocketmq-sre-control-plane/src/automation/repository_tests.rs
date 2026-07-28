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
use rocketmq_sre_contracts::AUTOMATION_SCHEMA_VERSION;
use rocketmq_sre_contracts::AutomationArtifact;
use rocketmq_sre_contracts::AutomationBudget;
use rocketmq_sre_contracts::AutomationFeedbackSubject;
use rocketmq_sre_contracts::AutomationFeedbackVerdict;
use rocketmq_sre_contracts::AutomationRunId;
use rocketmq_sre_contracts::AutomationRunStatus;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::NoSideEffectAutomationKind;
use rocketmq_sre_contracts::NoSideEffectAutomationRequest;
use rocketmq_sre_contracts::TenantId;
use sqlx::Row;
use uuid::Uuid;

use super::AutomationService;
use super::model::CompleteAutomationRunRequest;
use super::model::RecordAutomationFeedbackRequest;
use crate::PostgresRepository;
use crate::auth::AuthContext;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_automation_runs_are_idempotent_terminal_and_auditable() {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 5)
        .await
        .expect("repository with migrations");
    let (tenant_id, cluster_id, incident_id) = seed_fixture(&repository).await;
    let auth = operator_auth(tenant_id, cluster_id);
    let service = AutomationService::new(repository.clone());
    let started_at = Utc::now();
    let request = NoSideEffectAutomationRequest {
        schema_version: AUTOMATION_SCHEMA_VERSION.to_owned(),
        id: AutomationRunId::new(),
        tenant_id,
        cluster_id: Some(cluster_id),
        incident_id: Some(incident_id),
        correlation_id: CorrelationId::new(),
        kind: NoSideEffectAutomationKind::EvidenceCollection,
        idempotency_key: format!("automation:evidence:{incident_id}"),
        budget: AutomationBudget {
            max_model_calls: 0,
            max_output_bytes: 16_384,
            timeout_seconds: 30,
        },
        evidence_ids: Vec::new(),
        requested_by: auth.subject.clone(),
        requested_at: started_at,
    };

    let pending = service.submit(&auth, &request).await.expect("pending run");
    assert_eq!(pending.status, AutomationRunStatus::Pending);
    let mut retry = request.clone();
    retry.id = AutomationRunId::new();
    retry.requested_at += Duration::seconds(1);
    let retried = service.submit(&auth, &retry).await.expect("idempotent retry");
    assert_eq!(retried.id, pending.id);

    let mut conflict = retry;
    conflict.evidence_ids.push(EvidenceId::new());
    assert!(service.submit(&auth, &conflict).await.is_err());

    let completion = CompleteAutomationRunRequest {
        status: AutomationRunStatus::Succeeded,
        result_code: "evidence_collection_completed".to_owned(),
        sanitized_summary: "Collected bounded evidence references for the incident".to_owned(),
        artifacts: vec![AutomationArtifact {
            kind: "evidence_snapshot".to_owned(),
            id: Uuid::new_v4(),
        }],
        model_invocation_id: None,
        completed_at: started_at + Duration::seconds(2),
    };
    let automation_auth = service_auth(tenant_id, cluster_id);
    let completed = service
        .complete(&automation_auth, pending.id, &completion)
        .await
        .expect("complete run");
    assert_eq!(completed.status, AutomationRunStatus::Succeeded);
    let replayed = service
        .complete(&automation_auth, pending.id, &completion)
        .await
        .expect("idempotent completion");
    assert_eq!(replayed, completed);
    let mut conflicting_completion = completion;
    conflicting_completion.sanitized_summary = "Different immutable result".to_owned();
    assert!(
        service
            .complete(&automation_auth, pending.id, &conflicting_completion)
            .await
            .is_err()
    );

    let statuses = sqlx::query(
        "SELECT from_status, to_status
         FROM automation_run_events
         WHERE run_id = $1
         ORDER BY sequence_id",
    )
    .bind(pending.id.as_uuid())
    .fetch_all(&repository.pool)
    .await
    .expect("run events")
    .into_iter()
    .map(|row| {
        (
            row.try_get::<Option<String>, _>("from_status").expect("from status"),
            row.try_get::<String, _>("to_status").expect("to status"),
        )
    })
    .collect::<Vec<_>>();
    assert_eq!(
        statuses,
        vec![
            (None, "pending".to_owned()),
            (Some("pending".to_owned()), "running".to_owned()),
            (Some("running".to_owned()), "succeeded".to_owned()),
        ]
    );
    assert!(
        sqlx::query("DELETE FROM no_side_effect_automation_runs WHERE id = $1")
            .bind(pending.id.as_uuid())
            .execute(&repository.pool)
            .await
            .is_err()
    );

    let feedback = service
        .record_feedback(
            &auth,
            &RecordAutomationFeedbackRequest {
                cluster_id: Some(cluster_id),
                incident_id: Some(incident_id),
                subject: AutomationFeedbackSubject::Summary,
                subject_id: Some(pending.id.as_uuid()),
                verdict: AutomationFeedbackVerdict::Useful,
                comment: Some("Useful bounded incident evidence summary".to_owned()),
            },
        )
        .await
        .expect("operator feedback");
    assert_eq!(feedback.actor_subject, auth.subject);
    let mut model_auth = auth;
    model_auth.roles.insert("model_service".to_owned());
    assert!(
        service
            .record_feedback(
                &model_auth,
                &RecordAutomationFeedbackRequest {
                    cluster_id: Some(cluster_id),
                    incident_id: Some(incident_id),
                    subject: AutomationFeedbackSubject::Summary,
                    subject_id: Some(pending.id.as_uuid()),
                    verdict: AutomationFeedbackVerdict::Correct,
                    comment: None,
                },
            )
            .await
            .is_err()
    );
}

async fn seed_fixture(repository: &PostgresRepository) -> (TenantId, ClusterId, IncidentId) {
    let tenant_id = TenantId::new();
    let cluster_id = ClusterId::new();
    let incident_id = IncidentId::new();
    sqlx::query(
        "INSERT INTO clusters (
            id, tenant_id, external_cluster_key, environment, region,
            rocketmq_version, deployment_mode, owner_name,
            requested_access_profile, effective_access_profile, onboarding_state
         ) VALUES (
            $1, $2, $3, 'test', 'local',
            'test', 'docker', 'automation-repository-test',
            'read_only', 'read_only', 'ready_read_only'
         )",
    )
    .bind(cluster_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(format!("automation-repository-{cluster_id}"))
    .execute(&repository.pool)
    .await
    .expect("cluster fixture");
    sqlx::query(
        "INSERT INTO sre_incidents (
            id, tenant_id, cluster_id, title, resource, symptom_family,
            fingerprint, severity, owner_name, status, workflow_checkpoint,
            created_by_subject, created_at, updated_at
         ) VALUES (
            $1, $2, $3, 'Automation repository test', 'broker/test', 'automation',
            $4, 'warning', 'automation-owner', 'diagnosing', '{}'::JSONB,
            'automation-repository-test', NOW(), NOW()
         )",
    )
    .bind(incident_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(format!("automation:{incident_id}"))
    .execute(&repository.pool)
    .await
    .expect("incident fixture");
    (tenant_id, cluster_id, incident_id)
}

fn operator_auth(tenant_id: TenantId, cluster_id: ClusterId) -> AuthContext {
    AuthContext {
        tenant_id,
        subject: "automation-operator".to_owned(),
        clusters: BTreeSet::from([cluster_id]),
        roles: BTreeSet::from(["operator".to_owned()]),
    }
}

fn service_auth(tenant_id: TenantId, cluster_id: ClusterId) -> AuthContext {
    AuthContext {
        tenant_id,
        subject: "automation-service".to_owned(),
        clusters: BTreeSet::from([cluster_id]),
        roles: BTreeSet::from(["automation_service".to_owned()]),
    }
}
