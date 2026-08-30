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
use rocketmq_sre_contracts::AlertSeverity;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::IncidentOperationRequest;
use rocketmq_sre_contracts::TenantId;

use super::repository::MAX_SUPPRESSION_DAYS;
use super::repository::OperatorWorkbenchRepository;
use super::repository::sla_deadlines;
use super::repository::validate_operation;
use crate::PostgresRepository;
use crate::auth::AuthContext;

#[test]
fn suppression_is_bounded() {
    let request = IncidentOperationRequest::Suppress {
        until: Utc::now() + Duration::days(MAX_SUPPRESSION_DAYS + 1),
        reason: "too long".to_owned(),
    };

    assert!(validate_operation(&request).is_err());
}

#[test]
fn severity_controls_sla_deadlines() {
    let now = Utc::now();
    let (ack_due, resolve_due) = sla_deadlines(Some(AlertSeverity::Critical), now);

    assert_eq!(ack_due - now, Duration::minutes(15));
    assert_eq!(resolve_due - now, Duration::hours(4));
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to an isolated PostgreSQL database"]
async fn postgres_reopen_preserves_terminal_incident_and_audits_metadata() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let postgres = PostgresRepository::connect(&database_url, 4)
        .await
        .expect("database and migrations");
    let tenant_id = TenantId::new();
    let cluster_id = ClusterId::new();
    sqlx::query(
        "INSERT INTO clusters (
            id, tenant_id, external_cluster_key, environment, region,
            rocketmq_version, deployment_mode, owner_name,
            requested_access_profile, effective_access_profile,
            onboarding_state
         ) VALUES (
            $1, $2, $3, 'test', 'local', 'test', 'test',
            'operator-workbench-test', 'read_only', 'read_only',
            'ready_read_only'
         )",
    )
    .bind(cluster_id.as_uuid())
    .bind(tenant_id.to_string())
    .bind(format!("operator-workbench-{cluster_id}"))
    .execute(&postgres.pool)
    .await
    .expect("test cluster");
    let incident_id = IncidentId::new();
    sqlx::query(
        "INSERT INTO sre_incidents (
            id, tenant_id, cluster_id, title, symptom_family, fingerprint,
            status, severity, owner_name, occurrence_count,
            created_by_subject, created_at, updated_at, sla_ack_due_at,
            sla_resolve_due_at
         ) VALUES (
            $1, $2, $3, 'terminal fixture', 'test', $4, 'resolved',
            'critical', 'test-owner', 1, 'test', NOW() - INTERVAL '1 hour',
            NOW(), NOW() - INTERVAL '45 minutes', NOW() + INTERVAL '3 hours'
         )",
    )
    .bind(incident_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(format!("operator-workbench-{incident_id}"))
    .execute(&postgres.pool)
    .await
    .expect("test incident");
    let auth = AuthContext {
        tenant_id,
        subject: "test-operator".to_owned(),
        clusters: BTreeSet::from([cluster_id]),
        roles: BTreeSet::from(["rocketmq:diagnose".to_owned()]),
    };
    let repository = OperatorWorkbenchRepository::new(postgres.pool.clone());
    let result = repository
        .apply(
            &auth,
            incident_id,
            &IncidentOperationRequest::Reopen {
                reason: "symptom returned".to_owned(),
            },
            CorrelationId::new(),
        )
        .await
        .expect("reopen metadata operation");
    let related_id = result.related_incident_id.expect("linked incident");
    let original_status: String = sqlx::query_scalar("SELECT status FROM sre_incidents WHERE id = $1")
        .bind(incident_id.as_uuid())
        .fetch_one(&postgres.pool)
        .await
        .expect("original status");
    let related_status: String = sqlx::query_scalar("SELECT status FROM sre_incidents WHERE id = $1")
        .bind(related_id.as_uuid())
        .fetch_one(&postgres.pool)
        .await
        .expect("related status");
    let audit_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM incident_operations
         WHERE incident_id = $1 AND operation_kind = 'reopen'",
    )
    .bind(incident_id.as_uuid())
    .fetch_one(&postgres.pool)
    .await
    .expect("operation audit");

    assert_eq!(original_status, "resolved");
    assert_eq!(related_status, "new");
    assert_eq!(audit_count, 1);
    assert!(!result.cluster_mutation_performed);
}
