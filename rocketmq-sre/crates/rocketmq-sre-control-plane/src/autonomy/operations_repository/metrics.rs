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

use rocketmq_sre_contracts::TenantId;
use sqlx::Row;
use uuid::Uuid;

use super::SAVINGS_METHOD;
use super::count;
use super::non_negative;
use super::ratio_basis_points;
use super::reduction_basis_points;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::autonomy::operations::AutomationSavingsMetrics;
use crate::autonomy::operations::AutonomyDurationMetrics;
use crate::autonomy::operations::AutonomyFeedbackMetrics;
use crate::autonomy::operations::AutonomyQualityMetrics;
use crate::autonomy::operations::AutonomyReportWindow;

impl PostgresRepository {
    pub(super) async fn duration_metrics(
        &self,
        tenant_id: TenantId,
        clusters: &[Uuid],
        window: &AutonomyReportWindow,
    ) -> Result<AutonomyDurationMetrics, ControlPlaneError> {
        let row = sqlx::query(
            "WITH incidents AS (
                SELECT id, created_at, updated_at, acknowledged_at, status
                FROM sre_incidents
                WHERE tenant_id = $1 AND cluster_id = ANY($2)
                  AND created_at < $4 AND updated_at >= $3
             ), diagnosis AS (
                SELECT incident_id, MIN(created_at) AS diagnosed_at
                FROM diagnosis_revisions
                WHERE incident_id IN (SELECT id FROM incidents)
                GROUP BY incident_id
             ), completed_execution AS (
                SELECT id, plan_id, started_at, completed_at
                FROM executions
                WHERE tenant_id = $1 AND cluster_id = ANY($2)
                  AND completed_at >= $3 AND completed_at < $4
             ), recovery AS (
                SELECT incident.id,
                       EXTRACT(EPOCH FROM (
                           incident.updated_at - MIN(execution.started_at)
                       ))::DOUBLE PRECISION AS seconds
                FROM incidents incident
                JOIN action_plans plan ON plan.incident_id = incident.id
                JOIN completed_execution execution ON execution.plan_id = plan.id
                WHERE incident.status IN ('resolved', 'escalated')
                GROUP BY incident.id, incident.updated_at
             )
             SELECT
                AVG(EXTRACT(EPOCH FROM (acknowledged_at - created_at)))
                    FILTER (WHERE acknowledged_at IS NOT NULL)::DOUBLE PRECISION AS mtta,
                AVG(EXTRACT(EPOCH FROM (updated_at - created_at)))
                    FILTER (WHERE status IN ('resolved', 'escalated'))::DOUBLE PRECISION AS mttr,
                AVG(EXTRACT(EPOCH FROM (diagnosis.diagnosed_at - incidents.created_at)))
                    FILTER (WHERE diagnosis.diagnosed_at IS NOT NULL)::DOUBLE PRECISION AS diagnosis_seconds,
                (SELECT AVG(EXTRACT(EPOCH FROM (completed_at - started_at)))::DOUBLE PRECISION
                 FROM completed_execution) AS execution_seconds,
                (SELECT AVG(seconds)::DOUBLE PRECISION FROM recovery) AS recovery_seconds,
                COUNT(*) FILTER (WHERE acknowledged_at IS NOT NULL) AS acknowledged_incidents,
                COUNT(*) FILTER (WHERE status IN ('resolved', 'escalated')) AS resolved_incidents,
                COUNT(*) FILTER (WHERE diagnosis.diagnosed_at IS NOT NULL) AS diagnosed_incidents,
                (SELECT COUNT(*) FROM completed_execution) AS completed_executions
             FROM incidents
             LEFT JOIN diagnosis ON diagnosis.incident_id = incidents.id",
        )
        .bind(tenant_id.as_uuid())
        .bind(clusters)
        .bind(window.start)
        .bind(window.end)
        .fetch_one(&self.pool)
        .await?;
        Ok(AutonomyDurationMetrics {
            mean_time_to_acknowledge_seconds: non_negative(row.try_get("mtta")?),
            mean_time_to_resolve_seconds: non_negative(row.try_get("mttr")?),
            average_diagnosis_seconds: non_negative(row.try_get("diagnosis_seconds")?),
            average_execution_seconds: non_negative(row.try_get("execution_seconds")?),
            average_recovery_seconds: non_negative(row.try_get("recovery_seconds")?),
            acknowledged_incidents: count(row.try_get("acknowledged_incidents")?)?,
            resolved_incidents: count(row.try_get("resolved_incidents")?)?,
            diagnosed_incidents: count(row.try_get("diagnosed_incidents")?)?,
            completed_executions: count(row.try_get("completed_executions")?)?,
        })
    }

    pub(super) async fn quality_metrics(
        &self,
        tenant_id: TenantId,
        clusters: &[Uuid],
        window: &AutonomyReportWindow,
    ) -> Result<AutonomyQualityMetrics, ControlPlaneError> {
        let row = sqlx::query(
            "WITH scoped_occurrences AS (
                SELECT occurrence.alert_id
                FROM alert_occurrences occurrence
                JOIN alert_events alert ON alert.id = occurrence.alert_id
                WHERE alert.tenant_id = $1 AND alert.cluster_id = ANY($2)
                  AND occurrence.occurred_at >= $3 AND occurrence.occurred_at < $4
             ), scoped_incidents AS (
                SELECT id, owner_name, status, reopened_from_incident_id
                FROM sre_incidents
                WHERE tenant_id = $1 AND cluster_id = ANY($2)
                  AND created_at >= $3 AND created_at < $4
             ), health_ranked AS (
                SELECT cluster_id, score, observed_at,
                       ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY observed_at, id) AS first_rank,
                       ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY observed_at DESC, id DESC) AS last_rank
                FROM cluster_health_snapshots
                WHERE tenant_id = $1 AND cluster_id = ANY($2)
                  AND observed_at >= $3 AND observed_at < $4
                  AND score IS NOT NULL
             ), health_delta AS (
                SELECT first.cluster_id, (last.score - first.score)::DOUBLE PRECISION AS delta
                FROM health_ranked first
                JOIN health_ranked last ON last.cluster_id = first.cluster_id
                WHERE first.first_rank = 1 AND last.last_rank = 1
             )
             SELECT
                (SELECT COUNT(*) FROM scoped_occurrences) AS raw_alert_occurrences,
                (SELECT COUNT(DISTINCT incident_alerts.incident_id)
                 FROM incident_alerts
                 JOIN scoped_occurrences ON scoped_occurrences.alert_id = incident_alerts.alert_id) AS \
             correlated_alerts,
                (SELECT COUNT(*) FILTER (WHERE owner_name <> 'unassigned') FROM scoped_incidents) AS routed_incidents,
                (SELECT COUNT(*) FROM scoped_incidents) AS incident_count,
                (SELECT COUNT(*) FILTER (WHERE status IN ('resolved', 'escalated')) FROM scoped_incidents)
                    AS terminal_incidents,
                (SELECT COUNT(*) FILTER (WHERE reopened_from_incident_id IS NOT NULL) FROM scoped_incidents)
                    AS recurrent_incidents,
                (SELECT COUNT(*) FROM action_items
                 WHERE tenant_id = $1 AND cluster_id = ANY($2)
                   AND due_at < $4
                   AND status NOT IN ('completed', 'cancelled')) AS overdue_action_items,
                (SELECT COUNT(*) FROM incident_recurrences recurrence
                 JOIN sre_incidents incident ON incident.id = recurrence.incident_id
                 WHERE incident.tenant_id = $1 AND incident.cluster_id = ANY($2)
                   AND recurrence.matched_at >= $3 AND recurrence.matched_at < $4) AS post_close_recurrences,
                (SELECT AVG(delta)::DOUBLE PRECISION FROM health_delta) AS health_score_delta",
        )
        .bind(tenant_id.as_uuid())
        .bind(clusters)
        .bind(window.start)
        .bind(window.end)
        .fetch_one(&self.pool)
        .await?;
        let raw_alert_occurrences = count(row.try_get("raw_alert_occurrences")?)?;
        let correlated_alerts = count(row.try_get("correlated_alerts")?)?;
        let incident_count = count(row.try_get("incident_count")?)?;
        let routed_incidents = count(row.try_get("routed_incidents")?)?;
        let terminal_incidents = count(row.try_get("terminal_incidents")?)?;
        let recurrent_incidents = count(row.try_get("recurrent_incidents")?)?;
        Ok(AutonomyQualityMetrics {
            raw_alert_occurrences,
            correlated_alerts,
            noise_reduction_basis_points: reduction_basis_points(raw_alert_occurrences, correlated_alerts),
            routed_incidents,
            owner_routing_hit_basis_points: ratio_basis_points(routed_incidents, incident_count),
            terminal_incidents,
            recurrent_incidents,
            recurrence_basis_points: ratio_basis_points(recurrent_incidents, terminal_incidents),
            overdue_action_items: count(row.try_get("overdue_action_items")?)?,
            post_close_recurrences: count(row.try_get("post_close_recurrences")?)?,
            health_score_delta: row.try_get("health_score_delta")?,
        })
    }

    pub(super) async fn feedback_metrics(
        &self,
        tenant_id: TenantId,
        clusters: &[Uuid],
        window: &AutonomyReportWindow,
    ) -> Result<AutonomyFeedbackMetrics, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE verdict IN ('correct', 'useful')) AS adopted,
                COUNT(*) FILTER (WHERE verdict = 'incorrect') AS modified,
                COUNT(*) FILTER (WHERE verdict = 'not_useful') AS rejected
             FROM autonomy_operator_feedback
             WHERE tenant_id = $1
               AND (cluster_id IS NULL OR cluster_id = ANY($2))
               AND subject_kind IN ('recommendation', 'plan')
               AND created_at >= $3 AND created_at < $4",
        )
        .bind(tenant_id.as_uuid())
        .bind(clusters)
        .bind(window.start)
        .bind(window.end)
        .fetch_one(&self.pool)
        .await?;
        let total = count(row.try_get("total")?)?;
        let adopted = count(row.try_get("adopted")?)?;
        let modified = count(row.try_get("modified")?)?;
        let rejected = count(row.try_get("rejected")?)?;
        Ok(AutonomyFeedbackMetrics {
            total,
            adopted,
            modified,
            rejected,
            adoption_basis_points: ratio_basis_points(adopted, total),
            modification_basis_points: ratio_basis_points(modified, total),
            rejection_basis_points: ratio_basis_points(rejected, total),
        })
    }

    pub(super) async fn savings_metrics(
        &self,
        tenant_id: TenantId,
        clusters: &[Uuid],
        window: &AutonomyReportWindow,
    ) -> Result<AutomationSavingsMetrics, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT
                (SELECT COUNT(*) FROM no_side_effect_automation_runs
                 WHERE tenant_id = $1
                   AND (cluster_id IS NULL OR cluster_id = ANY($2))
                   AND status = 'succeeded'
                   AND completed_at >= $3 AND completed_at < $4) AS no_side_effect_runs,
                (SELECT COUNT(*) FROM preventive_automation_runs
                 WHERE tenant_id = $1 AND cluster_id = ANY($2)
                   AND status = 'succeeded'
                   AND completed_at >= $3 AND completed_at < $4) AS preventive_runs,
                COALESCE((
                    SELECT SUM(CASE automation_kind
                        WHEN 'alert_correlation' THEN 3
                        WHEN 'severity_owner_suggestion' THEN 4
                        WHEN 'evidence_collection' THEN 10
                        WHEN 'shift_summary' THEN 15
                        WHEN 'notification' THEN 2
                        WHEN 'postmortem_draft' THEN 30
                        ELSE 0 END)
                    FROM no_side_effect_automation_runs
                    WHERE tenant_id = $1
                      AND (cluster_id IS NULL OR cluster_id = ANY($2))
                      AND status = 'succeeded'
                      AND completed_at >= $3 AND completed_at < $4
                ), 0)::BIGINT
                + COALESCE((
                    SELECT COUNT(*) * 20
                    FROM preventive_automation_runs
                    WHERE tenant_id = $1 AND cluster_id = ANY($2)
                      AND status = 'succeeded'
                      AND completed_at >= $3 AND completed_at < $4
                ), 0)::BIGINT AS estimated_minutes",
        )
        .bind(tenant_id.as_uuid())
        .bind(clusters)
        .bind(window.start)
        .bind(window.end)
        .fetch_one(&self.pool)
        .await?;
        Ok(AutomationSavingsMetrics {
            successful_no_side_effect_runs: count(row.try_get("no_side_effect_runs")?)?,
            successful_preventive_runs: count(row.try_get("preventive_runs")?)?,
            estimated_minutes_saved: count(row.try_get("estimated_minutes")?)?,
            estimate_method: SAVINGS_METHOD.to_owned(),
        })
    }
}
