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
use rocketmq_sre_contracts::AutomationBudget;
use rocketmq_sre_contracts::AutomationRunId;
use rocketmq_sre_contracts::AutomationRunStatus;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::PreventiveAutomationRequest;
use rocketmq_sre_contracts::PreventiveAutomationRun;
use rocketmq_sre_contracts::PreventiveRiskFamily;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::TimeRange;
use rocketmq_sre_contracts::current_evidence_schema;
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

use super::PreventiveAutomationService;
use super::model::PreventiveScheduleRequest;
use crate::PostgresRepository;
use crate::alerting::AlertingService;
use crate::auth::AuthContext;
use crate::autonomy::AutonomyService;
use crate::connector_channel::PostgresConnectorChannelService;
use crate::evidence::EvidenceBlobStore;
use crate::evidence::EvidenceService;
use crate::inspection::InspectionService;
use crate::slo::SloService;
use crate::workflow::WorkflowEventBus;
use crate::workflow::WorkflowService;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_preventive_automation_runs_all_risk_families_and_freezes_critical_capacity() {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 8)
        .await
        .expect("repository with migrations");
    let tenant_id = TenantId::new();
    let cluster_id = ClusterId::new();
    seed_cluster(&repository, tenant_id, cluster_id).await;
    let auth = AuthContext {
        tenant_id,
        subject: "preventive-automation-test".to_owned(),
        clusters: BTreeSet::from([cluster_id]),
        roles: BTreeSet::from(["operator".to_owned()]),
    };
    let evidence = EvidenceService::new(repository.clone(), EvidenceBlobStore::in_memory(64 * 1024));
    persist_capacity_fault(&evidence, &auth, cluster_id).await;
    let workflow = WorkflowService::new(repository.clone(), WorkflowEventBus::new(64));
    let alerting = AlertingService::new(repository.clone(), workflow.clone()).expect("alerting service");
    let connector =
        PostgresConnectorChannelService::postgres(repository.clone(), "preventive-test-token").expect("connector");
    let slo = SloService::new(repository.clone(), connector, evidence.clone(), alerting).expect("SLO service");
    let autonomy = AutonomyService::new(repository.clone(), slo, &[11_u8; 32]).expect("autonomy service");
    let inspections = InspectionService::new(repository.clone(), workflow, evidence).expect("inspection service");
    let service = PreventiveAutomationService::new(repository.clone(), inspections, autonomy);

    let mut runs = Vec::new();
    for risk_family in [
        PreventiveRiskFamily::Capacity,
        PreventiveRiskFamily::Certificate,
        PreventiveRiskFamily::Config,
        PreventiveRiskFamily::Route,
        PreventiveRiskFamily::Ha,
        PreventiveRiskFamily::Upgrade,
    ] {
        let request = preventive_request(&auth, cluster_id, risk_family);
        let run = service.submit(&auth, &request).await.expect("preventive automation");
        assert_eq!(run.status, AutomationRunStatus::Succeeded, "{risk_family:?}");
        assert!(run.inspection_run_id.is_some());
        assert_eq!(run.risk_family, risk_family);
        let retried = service.submit(&auth, &request).await.expect("idempotent retry");
        assert_eq!(retried, run);
        runs.push(run);
    }

    assert_eq!(runs.len(), 6);
    let capacity = runs
        .iter()
        .find(|run| run.risk_family == PreventiveRiskFamily::Capacity)
        .expect("capacity run");
    assert_eq!(capacity.result_code, "critical_risk_frozen");
    assert!(!capacity.recommendation_ids.is_empty());
    assert!(capacity.freeze_id.is_some());
    assert!(capacity.kill_switch_suggested);

    let freeze = sqlx::query(
        "SELECT active, action_id, expires_at
         FROM autonomy_freezes
         WHERE id = $1 AND tenant_id = $2 AND cluster_id = $3",
    )
    .bind(capacity.freeze_id.expect("capacity freeze"))
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .fetch_one(&repository.pool)
    .await
    .expect("persisted preventive freeze");
    assert!(freeze.try_get::<bool, _>("active").expect("active"));
    assert!(
        freeze
            .try_get::<Option<String>, _>("action_id")
            .expect("action id")
            .is_none()
    );
    assert!(
        freeze
            .try_get::<Option<chrono::DateTime<Utc>>, _>("expires_at")
            .expect("expiry")
            .is_none()
    );

    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM automation_run_events
         WHERE tenant_id = $1 AND run_family = 'preventive'",
    )
    .bind(tenant_id.as_uuid())
    .fetch_one(&repository.pool)
    .await
    .expect("preventive events");
    assert_eq!(event_count, 18);

    let schedule = service
        .schedule(
            &auth,
            &PreventiveScheduleRequest {
                cluster_id,
                risk_family: PreventiveRiskFamily::Route,
                schedule: "@hourly".to_owned(),
            },
        )
        .await
        .expect("preventive schedule");
    assert_eq!(schedule.risk_family, PreventiveRiskFamily::Route);
    let stored_schedule: (String, String) = sqlx::query_as(
        "SELECT status, schedule
         FROM inspection_runs
         WHERE id = $1 AND tenant_id = $2",
    )
    .bind(schedule.inspection_run_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .fetch_one(&repository.pool)
    .await
    .expect("scheduled inspection");
    assert_eq!(stored_schedule, ("scheduled".to_owned(), "@hourly".to_owned()));
    sqlx::query(
        "UPDATE inspection_runs
         SET next_run_at = TIMESTAMPTZ '2000-01-01 00:00:00+00'
         WHERE id = $1 AND tenant_id = $2",
    )
    .bind(schedule.inspection_run_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .execute(&repository.pool)
    .await
    .expect("make preventive schedule due");
    service.run_due().await;
    let scheduled_snapshot: serde_json::Value = sqlx::query_scalar(
        "SELECT result_snapshot
         FROM preventive_automation_runs
         WHERE tenant_id = $1 AND inspection_run_id = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(schedule.inspection_run_id.as_uuid())
    .fetch_one(&repository.pool)
    .await
    .expect("scheduled preventive run");
    let scheduled_run: PreventiveAutomationRun =
        serde_json::from_value(scheduled_snapshot).expect("scheduled preventive result");
    assert_eq!(scheduled_run.status, AutomationRunStatus::Succeeded);
    assert_eq!(scheduled_run.risk_family, PreventiveRiskFamily::Route);
    let successor_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM inspection_runs
         WHERE tenant_id = $1 AND cluster_id = $2
           AND template = 'routing_proxy' AND schedule = '@hourly'
           AND status = 'scheduled' AND id <> $3",
    )
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(schedule.inspection_run_id.as_uuid())
    .fetch_one(&repository.pool)
    .await
    .expect("recurring preventive successor");
    assert_eq!(successor_count, 1);

    let delete = sqlx::query("DELETE FROM preventive_automation_runs WHERE id = $1")
        .bind(capacity.id.as_uuid())
        .execute(&repository.pool)
        .await;
    assert!(delete.is_err(), "preventive audit runs must not be deletable");
}

fn preventive_request(
    auth: &AuthContext,
    cluster_id: ClusterId,
    risk_family: PreventiveRiskFamily,
) -> PreventiveAutomationRequest {
    PreventiveAutomationRequest {
        schema_version: AUTOMATION_SCHEMA_VERSION.to_owned(),
        id: AutomationRunId::new(),
        tenant_id: auth.tenant_id,
        cluster_id,
        correlation_id: CorrelationId::new(),
        risk_family,
        idempotency_key: format!("preventive:{}:{}", risk_name(risk_family), Uuid::new_v4()),
        budget: automation_budget(),
        requested_by: auth.subject.clone(),
        requested_at: Utc::now(),
    }
}

const fn automation_budget() -> AutomationBudget {
    AutomationBudget {
        max_model_calls: 0,
        max_output_bytes: 64 * 1_024,
        timeout_seconds: 60,
    }
}

async fn persist_capacity_fault(evidence: &EvidenceService, auth: &AuthContext, cluster_id: ClusterId) {
    let observed_at = Utc::now();
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id: CorrelationId::new(),
        tenant_id: auth.tenant_id,
        cluster_id,
        source: "prometheus".to_owned(),
        resource: "capacity-runway/preventive-test".to_owned(),
        time_range: TimeRange::new(observed_at - Duration::minutes(1), observed_at).expect("valid capacity time range"),
    };
    let snapshot = EvidenceSnapshot::capture(
        query,
        current_evidence_schema(),
        observed_at,
        EvidenceContent::Inline(json!({
            "disk_runway_days": 10,
            "backlog_runway_days": 14,
            "connection_headroom_ratio": 0.5
        })),
    )
    .expect("capacity Evidence");
    evidence
        .persist_cluster(auth, snapshot)
        .await
        .expect("persist capacity Evidence");
}

async fn seed_cluster(repository: &PostgresRepository, tenant_id: TenantId, cluster_id: ClusterId) {
    sqlx::query(
        "INSERT INTO clusters (
            id, tenant_id, external_cluster_key, environment, region,
            rocketmq_version, deployment_mode, owner_name,
            requested_access_profile, effective_access_profile, onboarding_state
         ) VALUES (
            $1, $2, $3, 'test', 'local',
            'test', 'docker', 'preventive-automation-test',
            'read_only', 'read_only', 'ready_read_only'
         )",
    )
    .bind(cluster_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(format!("preventive-cluster-{cluster_id}"))
    .execute(&repository.pool)
    .await
    .expect("seed preventive cluster");
}

const fn risk_name(risk: PreventiveRiskFamily) -> &'static str {
    match risk {
        PreventiveRiskFamily::Capacity => "capacity",
        PreventiveRiskFamily::Certificate => "certificate",
        PreventiveRiskFamily::Config => "config",
        PreventiveRiskFamily::Route => "route",
        PreventiveRiskFamily::Ha => "ha",
        PreventiveRiskFamily::Upgrade => "upgrade",
    }
}
