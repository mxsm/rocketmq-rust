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
use rocketmq_sre_contracts::AutonomyOutcome;
use rocketmq_sre_contracts::AutonomyOutcomeClass;
use rocketmq_sre_contracts::AutonomyOutcomeId;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::ModelInvocationId;
use rocketmq_sre_contracts::TenantId;
use uuid::Uuid;

use super::AutonomyOperationsService;
use super::operations::AutonomyOperationalReportQuery;
use super::operations::AutonomyOutcomeListQuery;
use super::operations::AutonomyReportPeriod;
use super::operations::OperationsAnalyticsQuery;
use crate::PostgresRepository;
use crate::auth::AuthContext;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_outcomes_cost_and_operational_report_are_scoped_and_idempotent() {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 5)
        .await
        .expect("repository with migrations");
    let fixture = seed_operations_fixture(&repository).await;
    let service = AutonomyOperationsService::new(repository.clone()).expect("operations service");
    let auth = AuthContext {
        tenant_id: fixture.tenant_id,
        subject: "report-reader@example.com".to_owned(),
        clusters: BTreeSet::from([fixture.cluster_id]),
        roles: BTreeSet::from(["rocketmq:diagnose".to_owned()]),
    };

    let page = service
        .outcomes(
            &auth,
            &AutonomyOutcomeListQuery {
                cluster_id: Some(fixture.cluster_id),
                action: Some(ExecutionAction::ObservabilityLoggerLevelTtl),
                class: Some(AutonomyOutcomeClass::Success),
                from: Some(fixture.now - Duration::hours(1)),
                until: Some(fixture.now + Duration::hours(1)),
                limit: 20,
            },
        )
        .await
        .expect("outcome page");
    assert_eq!(page.items.len(), 1);
    assert!(!page.truncated);
    assert_eq!(page.items[0].id, fixture.outcome_id);

    let report = service
        .report(
            &auth,
            &AutonomyOperationalReportQuery {
                period: AutonomyReportPeriod::Monthly,
                anchor: Some(fixture.now),
                cluster_id: Some(fixture.cluster_id),
            },
        )
        .await
        .expect("monthly operations report");
    assert_eq!(report.cluster_ids, vec![fixture.cluster_id]);
    assert_eq!(report.outcomes.candidates, 1);
    assert_eq!(report.outcomes.eligible, 1);
    assert_eq!(report.outcomes.successes, 1);
    assert_eq!(report.model_usage.calls, 1);
    assert_eq!(report.model_usage.input_tokens, 80);
    assert_eq!(report.model_usage.output_tokens, 20);
    assert_eq!(report.model_usage.cost_micros, 4_000);
    assert_eq!(report.model_usage.usage_coverage_basis_points, Some(10_000));
    assert_eq!(report.quality.raw_alert_occurrences, 2);
    assert_eq!(report.quality.correlated_alerts, 1);
    assert_eq!(report.quality.noise_reduction_basis_points, Some(5_000));
    assert_eq!(report.feedback.adopted, 1);
    assert_eq!(report.savings.estimated_minutes_saved, 10);
    assert_eq!(report.action_breakdown.len(), 1);
    assert_eq!(report.incident_costs.len(), 1);
    assert!(report.budget_alerts.is_empty());

    let analytics = service
        .analytics(
            &auth,
            &OperationsAnalyticsQuery {
                period: AutonomyReportPeriod::Monthly,
                anchor: Some(fixture.now),
                cluster_id: Some(fixture.cluster_id),
                scenario: Some("consumer_lag".to_owned()),
                provider_family: Some("deepseek".to_owned()),
                model_family: Some("deepseek".to_owned()),
                action_id: Some(ExecutionAction::ObservabilityLoggerLevelTtl.id().to_owned()),
            },
        )
        .await
        .expect("cross-dimensional operations analytics");
    assert_eq!(analytics.tenant_id, fixture.tenant_id);
    assert_eq!(analytics.filters.cluster_ids, vec![fixture.cluster_id]);
    assert_eq!(analytics.filters.scenario.as_deref(), Some("consumer_lag"));
    assert_eq!(analytics.incidents.total, 1);
    assert_eq!(analytics.incidents.diagnosed, 1);
    assert_eq!(analytics.incidents.terminal, 1);
    assert_eq!(analytics.incidents.mean_time_to_detect_seconds, Some(300.0));
    assert_eq!(analytics.incidents.mean_time_to_resolve_seconds, Some(1_080.0));
    assert_eq!(analytics.model_usage.calls, 1);
    assert_eq!(analytics.model_usage.input_tokens, 80);
    assert_eq!(analytics.model_usage.output_tokens, 20);
    assert_eq!(analytics.model_usage.cost_micros, 4_000);
    assert_eq!(analytics.recommendation_feedback.adopted, 1);
    assert_eq!(analytics.executions.total, 1);
    assert_eq!(analytics.executions.succeeded, 1);
    assert_eq!(analytics.executions.success_basis_points, Some(10_000));
    assert_eq!(analytics.savings.successful_autonomous_actions, 1);
    assert_eq!(analytics.savings.estimated_minutes_saved, 15);
    assert!(analytics.warnings.is_empty());

    let empty_scenario = service
        .analytics(
            &auth,
            &OperationsAnalyticsQuery {
                period: AutonomyReportPeriod::Monthly,
                anchor: Some(fixture.now),
                cluster_id: Some(fixture.cluster_id),
                scenario: Some("broker_unavailable".to_owned()),
                provider_family: Some("deepseek".to_owned()),
                model_family: Some("deepseek".to_owned()),
                action_id: Some(ExecutionAction::ObservabilityLoggerLevelTtl.id().to_owned()),
            },
        )
        .await
        .expect("empty dimensions are explicit");
    assert_eq!(empty_scenario.incidents.total, 0);
    assert_eq!(empty_scenario.model_usage.calls, 0);
    assert_eq!(empty_scenario.executions.total, 0);
    assert_eq!(empty_scenario.executions.success_basis_points, None);
    assert_eq!(empty_scenario.savings.estimated_minutes_saved, 0);
    assert_eq!(empty_scenario.warnings.len(), 4);

    let mut completed = report;
    completed.window.start = fixture.now - Duration::days(8);
    completed.window.end = fixture.now - Duration::days(1);
    completed.window.complete = true;
    assert!(
        repository
            .persist_autonomy_operational_report(&completed)
            .await
            .expect("first report persistence")
    );
    assert!(
        !repository
            .persist_autonomy_operational_report(&completed)
            .await
            .expect("idempotent report persistence")
    );
}

struct OperationsFixture {
    tenant_id: TenantId,
    cluster_id: ClusterId,
    outcome_id: AutonomyOutcomeId,
    now: chrono::DateTime<Utc>,
}

async fn seed_operations_fixture(repository: &PostgresRepository) -> OperationsFixture {
    let tenant_id = TenantId::new();
    let cluster_id = ClusterId::new();
    let incident_id = IncidentId::new();
    let invocation_id = ModelInvocationId::new();
    let profile_id = Uuid::new_v4();
    let diagnosis_id = Uuid::new_v4();
    let plan_id = ActionPlanId::new();
    let execution_id = ExecutionId::new();
    let outcome_id = AutonomyOutcomeId::new();
    let now = Utc::now().with_nanosecond(0).expect("whole-second timestamp");
    let plan_hash = digest();

    sqlx::query(
        "INSERT INTO clusters (
            id, tenant_id, external_cluster_key, environment, region,
            rocketmq_version, deployment_mode, owner_name, onboarding_state
         ) VALUES ($1, $2, $3, 'test', 'local', 'test', 'local', 'sre-test', 'ready_read_only')",
    )
    .bind(cluster_id.as_uuid())
    .bind(tenant_id.to_string())
    .bind(format!("operations-{cluster_id}"))
    .execute(&repository.pool)
    .await
    .expect("cluster fixture");

    sqlx::query(
        "INSERT INTO sre_incidents (
            id, tenant_id, cluster_id, title, symptom_family, fingerprint,
            status, workflow_checkpoint, created_by_subject,
            created_at, updated_at, acknowledged_at, owner_name, occurrence_count
         ) VALUES (
            $1, $2, $3, 'Operations report incident', 'consumer_lag', $4,
            'resolved', '{}'::JSONB, 'test',
            $5, $6, $7, 'messaging-oncall', 2
         )",
    )
    .bind(incident_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(format!("operations-{incident_id}"))
    .bind(now - Duration::minutes(20))
    .bind(now - Duration::minutes(2))
    .bind(now - Duration::minutes(18))
    .execute(&repository.pool)
    .await
    .expect("incident fixture");

    sqlx::query(
        "INSERT INTO model_profiles (
            id, tenant_id, profile_name, provider_family, protocol_family,
            model_family, model_name, model_revision, endpoint_instance,
            region, data_residency, data_classes, capabilities, priority,
            credential_ref, credential_owner, enabled, health, created_at, updated_at
         ) VALUES (
            $1, $2, $3, 'deepseek', 'openai-compatible',
            'deepseek', 'deepseek-chat', 'v3.2', 'test-endpoint',
            'local', 'local', '[]'::JSONB, '{}'::JSONB, 10,
            'test-reference', 'gateway', TRUE, 'healthy', $4, $4
         )",
    )
    .bind(profile_id)
    .bind(tenant_id.as_uuid())
    .bind(format!("operations-profile-{profile_id}"))
    .bind(now - Duration::hours(1))
    .execute(&repository.pool)
    .await
    .expect("model profile fixture");

    sqlx::query(
        "INSERT INTO model_invocations (
            id, tenant_id, cluster_id, incident_id, purpose,
            requested_profile_id, actual_profile_id, provider_family,
            model_family, model_revision, endpoint_instance, fallback_chain,
            prompt_version, schema_version, input_tokens, output_tokens,
            cost_micros, rationale, started_at, completed_at, actual_model
         ) VALUES (
            $1, $2, $3, $4, 'planner',
            $5, $5, 'deepseek',
            'deepseek', 'v3.2', 'test-endpoint', '{}',
            'rocketmq-sre.planner.v1', 'rocketmq-sre.plan.v1', 80, 20,
            4000, 'operations fixture', $6, $7, 'deepseek-chat'
         )",
    )
    .bind(invocation_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(incident_id.as_uuid())
    .bind(profile_id)
    .bind(now - Duration::minutes(17))
    .bind(now - Duration::minutes(16))
    .execute(&repository.pool)
    .await
    .expect("model invocation fixture");

    sqlx::query(
        "INSERT INTO diagnosis_revisions (
            id, incident_id, revision, status, rule_result, hypotheses,
            evidence_ids, primary_model_invocation_id, execution_eligible,
            partial, created_at
         ) VALUES (
            $1, $2, 1, 'completed', '{}'::JSONB, '[]'::JSONB,
            '{}', $3, TRUE, FALSE, $4
         )",
    )
    .bind(diagnosis_id)
    .bind(incident_id.as_uuid())
    .bind(invocation_id.as_uuid())
    .bind(now - Duration::minutes(15))
    .execute(&repository.pool)
    .await
    .expect("diagnosis fixture");

    sqlx::query(
        "INSERT INTO action_plans (
            id, tenant_id, cluster_id, incident_id, diagnosis_revision_id,
            primary_model_invocation_id, version, plan_hash, evidence_hash,
            risk, status, request_snapshot, created_by, created_at, expires_at
         ) VALUES (
            $1, $2, $3, $4, $5,
            $6, 1, $7, $8,
            'r1', 'approved', '{}'::JSONB, 'test', $9, $10
         )",
    )
    .bind(plan_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(incident_id.as_uuid())
    .bind(diagnosis_id)
    .bind(invocation_id.as_uuid())
    .bind(&plan_hash)
    .bind(digest())
    .bind(now - Duration::minutes(14))
    .bind(now + Duration::hours(1))
    .execute(&repository.pool)
    .await
    .expect("plan fixture");

    sqlx::query(
        "INSERT INTO executions (
            id, tenant_id, cluster_id, correlation_id, plan_id, plan_hash,
            resource_key, action_id, idempotency_key, state,
            request_snapshot, requested_by, started_at, completed_at, updated_at
         ) VALUES (
            $1, $2, $3, $4, $5, $6,
            'broker/test', $7, $8, 'succeeded',
            '{}'::JSONB, 'test', $9, $10, $10
         )",
    )
    .bind(execution_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(Uuid::new_v4())
    .bind(plan_id.as_uuid())
    .bind(&plan_hash)
    .bind(ExecutionAction::ObservabilityLoggerLevelTtl.id())
    .bind(format!("operations-execution-{execution_id}"))
    .bind(now - Duration::minutes(12))
    .bind(now - Duration::minutes(10))
    .execute(&repository.pool)
    .await
    .expect("execution fixture");

    let outcome = AutonomyOutcome {
        id: outcome_id,
        tenant_id,
        cluster_id,
        action: ExecutionAction::ObservabilityLoggerLevelTtl,
        action_version: "1.0.0".to_owned(),
        incident_id,
        plan_id,
        plan_hash: plan_hash.clone(),
        execution_id: Some(execution_id),
        cohort_id: None,
        class: AutonomyOutcomeClass::Success,
        failure: None,
        reason_codes: Vec::new(),
        first_positive_intent_persisted: true,
        occurred_at: now - Duration::minutes(9),
        reconciled_at: now - Duration::minutes(8),
    };
    sqlx::query(
        "INSERT INTO autonomy_outcomes (
            id, tenant_id, cluster_id, action_id, action_version,
            incident_id, plan_id, plan_hash, execution_id,
            outcome_class, reason_codes, first_positive_intent_persisted,
            outcome_snapshot, occurred_at, reconciled_at
         ) VALUES (
            $1, $2, $3, $4, '1.0.0',
            $5, $6, $7, $8,
            'success', '{}', TRUE,
            $9, $10, $11
         )",
    )
    .bind(outcome_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(ExecutionAction::ObservabilityLoggerLevelTtl.id())
    .bind(incident_id.as_uuid())
    .bind(plan_id.as_uuid())
    .bind(&plan_hash)
    .bind(execution_id.as_uuid())
    .bind(serde_json::to_value(outcome).expect("outcome JSON"))
    .bind(now - Duration::minutes(9))
    .bind(now - Duration::minutes(8))
    .execute(&repository.pool)
    .await
    .expect("outcome fixture");

    seed_alerts(repository, tenant_id, cluster_id, incident_id, now).await;
    seed_feedback_and_savings(repository, tenant_id, cluster_id, incident_id, now).await;
    OperationsFixture {
        tenant_id,
        cluster_id,
        outcome_id,
        now,
    }
}

async fn seed_alerts(
    repository: &PostgresRepository,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    incident_id: IncidentId,
    now: chrono::DateTime<Utc>,
) {
    let alert_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO alert_events (
            id, tenant_id, cluster_id, source, source_event_id, fingerprint,
            correlation_key, affected_resource, symptom_family, severity,
            status, summary, labels, evidence_ids, occurrence_count,
            last_sequence, first_occurred_at, last_occurred_at, received_at
         ) VALUES (
            $1, $2, $3, 'alertmanager', $4, $5,
            '{}'::JSONB, '{}'::JSONB, 'consumer_lag', 'warning',
            'firing', 'lag alert', '{}'::JSONB, '{}', 2,
            2, $6, $7, $7
         )",
    )
    .bind(alert_id)
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(format!("operations-alert-{alert_id}"))
    .bind(format!("operations-fingerprint-{alert_id}"))
    .bind(now - Duration::minutes(19))
    .bind(now - Duration::minutes(18))
    .execute(&repository.pool)
    .await
    .expect("alert fixture");
    for sequence in 1..=2 {
        sqlx::query(
            "INSERT INTO alert_occurrences (
                alert_id, source_occurrence_id, status, severity,
                evidence_ids, occurred_at, received_at
             ) VALUES ($1, $2, 'firing', 'warning', '{}', $3, $3)",
        )
        .bind(alert_id)
        .bind(format!("operations-occurrence-{alert_id}-{sequence}"))
        .bind(now - Duration::minutes(20 - sequence))
        .execute(&repository.pool)
        .await
        .expect("alert occurrence fixture");
    }
    sqlx::query(
        "INSERT INTO incident_alerts (
            incident_id, alert_id, tenant_id, cluster_id, linked_at
         ) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(incident_id.as_uuid())
    .bind(alert_id)
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(now - Duration::minutes(17))
    .execute(&repository.pool)
    .await
    .expect("incident alert fixture");
}

async fn seed_feedback_and_savings(
    repository: &PostgresRepository,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    incident_id: IncidentId,
    now: chrono::DateTime<Utc>,
) {
    sqlx::query(
        "INSERT INTO autonomy_operator_feedback (
            id, tenant_id, cluster_id, incident_id, subject_kind,
            subject_id, verdict, actor_subject, created_at
         ) VALUES ($1, $2, $3, $4, 'recommendation', $5, 'useful', 'operator@example.com', $6)",
    )
    .bind(Uuid::new_v4())
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(incident_id.as_uuid())
    .bind(Uuid::new_v4())
    .bind(now - Duration::minutes(7))
    .execute(&repository.pool)
    .await
    .expect("feedback fixture");

    sqlx::query(
        "INSERT INTO no_side_effect_automation_runs (
            id, tenant_id, cluster_id, incident_id, automation_kind,
            idempotency_key, status, result_snapshot, started_at, completed_at,
            correlation_id, budget_snapshot, request_snapshot, updated_at
         ) VALUES (
            $1, $2, $3, $4, 'evidence_collection',
            $5, 'succeeded', '{}'::JSONB, $6, $7,
            $8, '{}'::JSONB, '{}'::JSONB, $7
         )",
    )
    .bind(Uuid::new_v4())
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(incident_id.as_uuid())
    .bind(format!("operations:evidence:{}", Uuid::new_v4()))
    .bind(now - Duration::minutes(7))
    .bind(now - Duration::minutes(6))
    .bind(Uuid::new_v4())
    .execute(&repository.pool)
    .await
    .expect("automation fixture");
}

fn digest() -> String {
    let value = Uuid::new_v4().simple().to_string();
    format!("sha256:{value}{value}")
}
