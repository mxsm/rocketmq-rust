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

use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::TenantId;
use sqlx::Row;
use uuid::Uuid;

use super::operations::AttributedAutomationSavingsMetrics;
use super::operations::AutonomyFeedbackMetrics;
use super::operations::AutonomyReportWindow;
use super::operations::ExecutionOperationsMetrics;
use super::operations::IncidentOperationsMetrics;
use super::operations::ModelUsageMetrics;
use super::operations::OPERATIONS_ANALYTICS_SCHEMA_VERSION;
use super::operations::OperationsAnalyticsFilters;
use super::operations::OperationsAnalyticsQuery;
use super::operations::OperationsAnalyticsReport;
use crate::ControlPlaneError;
use crate::PostgresRepository;

const MTTD_DEFINITION: &str =
    "mean seconds from incident creation to the first persisted diagnosis revision in the selected scope";
const MTTR_DEFINITION: &str =
    "mean seconds from incident creation to terminal resolved or escalated update in the selected scope";
const SAVINGS_DEFINITION: &str = "fixed conservative estimate: alert correlation 3m, owner/severity 4m, evidence \
                                  10m, shift summary 15m, notification 2m, postmortem draft 30m, preventive \
                                  inspection 20m, successful autonomous action 15m";

impl PostgresRepository {
    pub(super) async fn operations_analytics(
        &self,
        tenant_id: TenantId,
        cluster_ids: &[ClusterId],
        query: &OperationsAnalyticsQuery,
        window: AutonomyReportWindow,
    ) -> Result<OperationsAnalyticsReport, ControlPlaneError> {
        let clusters = cluster_ids
            .iter()
            .copied()
            .map(ClusterId::as_uuid)
            .collect::<Vec<Uuid>>();
        let row = sqlx::query(
            "WITH scoped_incidents AS (
                SELECT incident.*
                FROM sre_incidents incident
                WHERE incident.tenant_id = $1
                  AND incident.cluster_id = ANY($2)
                  AND incident.created_at < $4
                  AND incident.updated_at >= $3
                  AND ($5::TEXT IS NULL OR incident.symptom_family = $5)
                  AND (
                    ($6::TEXT IS NULL AND $7::TEXT IS NULL)
                    OR EXISTS (
                        SELECT 1
                        FROM model_invocations invocation
                        WHERE invocation.incident_id = incident.id
                          AND invocation.started_at >= $3
                          AND invocation.started_at < $4
                          AND ($6::TEXT IS NULL OR invocation.provider_family = $6)
                          AND ($7::TEXT IS NULL OR invocation.model_family = $7)
                    )
                  )
                  AND (
                    $8::TEXT IS NULL
                    OR EXISTS (
                        SELECT 1
                        FROM action_plans plan
                        JOIN executions execution ON execution.plan_id = plan.id
                        WHERE plan.incident_id = incident.id
                          AND execution.action_id = $8
                          AND execution.started_at >= $3
                          AND execution.started_at < $4
                    )
                  )
             ), first_diagnosis AS (
                SELECT diagnosis.incident_id, MIN(diagnosis.created_at) AS diagnosed_at
                FROM diagnosis_revisions diagnosis
                WHERE diagnosis.incident_id IN (SELECT id FROM scoped_incidents)
                GROUP BY diagnosis.incident_id
             ), scoped_models AS (
                SELECT invocation.*
                FROM model_invocations invocation
                WHERE invocation.tenant_id = $1
                  AND invocation.cluster_id = ANY($2)
                  AND invocation.started_at >= $3
                  AND invocation.started_at < $4
                  AND ($6::TEXT IS NULL OR invocation.provider_family = $6)
                  AND ($7::TEXT IS NULL OR invocation.model_family = $7)
                  AND (
                    $5::TEXT IS NULL
                    OR invocation.incident_id IN (SELECT id FROM scoped_incidents)
                  )
                  AND (
                    $8::TEXT IS NULL
                    OR EXISTS (
                        SELECT 1
                        FROM action_plans plan
                        JOIN executions execution ON execution.plan_id = plan.id
                        WHERE plan.primary_model_invocation_id = invocation.id
                          AND execution.action_id = $8
                          AND execution.started_at >= $3
                          AND execution.started_at < $4
                    )
                  )
             ), scoped_executions AS (
                SELECT execution.*
                FROM executions execution
                JOIN action_plans plan ON plan.id = execution.plan_id
                JOIN scoped_incidents incident ON incident.id = plan.incident_id
                JOIN model_invocations invocation ON invocation.id = plan.primary_model_invocation_id
                WHERE execution.tenant_id = $1
                  AND execution.cluster_id = ANY($2)
                  AND execution.started_at >= $3
                  AND execution.started_at < $4
                  AND ($8::TEXT IS NULL OR execution.action_id = $8)
                  AND ($6::TEXT IS NULL OR invocation.provider_family = $6)
                  AND ($7::TEXT IS NULL OR invocation.model_family = $7)
             ), incident_metrics AS (
                SELECT
                    COUNT(*) AS incident_total,
                    COUNT(*) FILTER (WHERE diagnosis.diagnosed_at IS NOT NULL) AS incident_diagnosed,
                    COUNT(*) FILTER (WHERE incident.status IN ('resolved', 'escalated')) AS incident_terminal,
                    COUNT(*) FILTER (WHERE incident.reopened_from_incident_id IS NOT NULL) AS incident_recurrent,
                    AVG(EXTRACT(EPOCH FROM (diagnosis.diagnosed_at - incident.created_at)))
                        FILTER (WHERE diagnosis.diagnosed_at IS NOT NULL)::DOUBLE PRECISION AS mttd,
                    AVG(EXTRACT(EPOCH FROM (incident.updated_at - incident.created_at)))
                        FILTER (WHERE incident.status IN ('resolved', 'escalated'))::DOUBLE PRECISION AS mttr
                FROM scoped_incidents incident
                LEFT JOIN first_diagnosis diagnosis ON diagnosis.incident_id = incident.id
             ), model_metrics AS (
                SELECT
                    COUNT(*) AS model_calls,
                    COALESCE(SUM(input_tokens), 0)::BIGINT AS input_tokens,
                    COALESCE(SUM(output_tokens), 0)::BIGINT AS output_tokens,
                    COALESCE(SUM(cost_micros), 0)::BIGINT AS cost_micros,
                    COUNT(*) FILTER (WHERE input_tokens IS NULL OR output_tokens IS NULL) AS missing_tokens,
                    COUNT(*) FILTER (WHERE cost_micros IS NULL) AS missing_cost,
                    COUNT(*) FILTER (WHERE error_code IS NOT NULL) AS failed_calls,
                    COUNT(*) FILTER (WHERE CARDINALITY(fallback_chain) > 0) AS fallback_calls
                FROM scoped_models
             ), feedback_metrics AS (
                SELECT
                    COUNT(*) AS feedback_total,
                    COUNT(*) FILTER (WHERE feedback.verdict IN ('correct', 'useful')) AS feedback_adopted,
                    COUNT(*) FILTER (WHERE feedback.verdict = 'incorrect') AS feedback_modified,
                    COUNT(*) FILTER (WHERE feedback.verdict = 'not_useful') AS feedback_rejected
                FROM autonomy_operator_feedback feedback
                JOIN scoped_incidents incident ON incident.id = feedback.incident_id
                WHERE feedback.tenant_id = $1
                  AND feedback.subject_kind IN ('recommendation', 'plan')
                  AND feedback.created_at >= $3
                  AND feedback.created_at < $4
             ), execution_metrics AS (
                SELECT
                    COUNT(*) AS execution_total,
                    COUNT(*) FILTER (
                        WHERE state IN ('succeeded', 'rolled_back', 'escalated')
                    ) AS execution_terminal,
                    COUNT(*) FILTER (WHERE state = 'succeeded') AS execution_succeeded,
                    COUNT(*) FILTER (WHERE state = 'rolled_back') AS execution_rolled_back,
                    COUNT(*) FILTER (WHERE state = 'escalated') AS execution_escalated
                FROM scoped_executions
             ), savings_metrics AS (
                SELECT
                    (
                        SELECT COUNT(*)
                        FROM no_side_effect_automation_runs run
                        WHERE run.tenant_id = $1
                          AND (run.cluster_id IS NULL OR run.cluster_id = ANY($2))
                          AND run.status = 'succeeded'
                          AND run.completed_at >= $3
                          AND run.completed_at < $4
                          AND $8::TEXT IS NULL
                          AND (
                            $5::TEXT IS NULL
                            OR run.incident_id IN (SELECT id FROM scoped_incidents)
                          )
                          AND (
                            ($6::TEXT IS NULL AND $7::TEXT IS NULL)
                            OR run.model_invocation_id IN (SELECT id FROM scoped_models)
                          )
                    ) AS no_side_effect_runs,
                    (
                        SELECT COUNT(*)
                        FROM preventive_automation_runs run
                        WHERE run.tenant_id = $1
                          AND run.cluster_id = ANY($2)
                          AND run.status = 'succeeded'
                          AND run.completed_at >= $3
                          AND run.completed_at < $4
                          AND $6::TEXT IS NULL
                          AND $7::TEXT IS NULL
                          AND $8::TEXT IS NULL
                          AND ($5::TEXT IS NULL OR run.risk_family = $5)
                    ) AS preventive_runs,
                    (
                        SELECT COUNT(*)
                        FROM autonomy_outcomes outcome
                        JOIN scoped_executions execution ON execution.id = outcome.execution_id
                        WHERE outcome.tenant_id = $1
                          AND outcome.outcome_class = 'success'
                          AND outcome.occurred_at >= $3
                          AND outcome.occurred_at < $4
                    ) AS autonomous_actions,
                    COALESCE((
                        SELECT SUM(CASE run.automation_kind
                            WHEN 'alert_correlation' THEN 3
                            WHEN 'severity_owner_suggestion' THEN 4
                            WHEN 'evidence_collection' THEN 10
                            WHEN 'shift_summary' THEN 15
                            WHEN 'notification' THEN 2
                            WHEN 'postmortem_draft' THEN 30
                            ELSE 0 END)
                        FROM no_side_effect_automation_runs run
                        WHERE run.tenant_id = $1
                          AND (run.cluster_id IS NULL OR run.cluster_id = ANY($2))
                          AND run.status = 'succeeded'
                          AND run.completed_at >= $3
                          AND run.completed_at < $4
                          AND $8::TEXT IS NULL
                          AND (
                            $5::TEXT IS NULL
                            OR run.incident_id IN (SELECT id FROM scoped_incidents)
                          )
                          AND (
                            ($6::TEXT IS NULL AND $7::TEXT IS NULL)
                            OR run.model_invocation_id IN (SELECT id FROM scoped_models)
                          )
                    ), 0)::BIGINT
                    + COALESCE((
                        SELECT COUNT(*) * 20
                        FROM preventive_automation_runs run
                        WHERE run.tenant_id = $1
                          AND run.cluster_id = ANY($2)
                          AND run.status = 'succeeded'
                          AND run.completed_at >= $3
                          AND run.completed_at < $4
                          AND $6::TEXT IS NULL
                          AND $7::TEXT IS NULL
                          AND $8::TEXT IS NULL
                          AND ($5::TEXT IS NULL OR run.risk_family = $5)
                    ), 0)::BIGINT
                    + (
                        SELECT COUNT(*) * 15
                        FROM autonomy_outcomes outcome
                        JOIN scoped_executions execution ON execution.id = outcome.execution_id
                        WHERE outcome.tenant_id = $1
                          AND outcome.outcome_class = 'success'
                          AND outcome.occurred_at >= $3
                          AND outcome.occurred_at < $4
                    )::BIGINT AS estimated_minutes
             )
             SELECT *
             FROM incident_metrics
             CROSS JOIN model_metrics
             CROSS JOIN feedback_metrics
             CROSS JOIN execution_metrics
             CROSS JOIN savings_metrics",
        )
        .bind(tenant_id.as_uuid())
        .bind(&clusters)
        .bind(window.start)
        .bind(window.end)
        .bind(query.scenario.as_deref())
        .bind(query.provider_family.as_deref())
        .bind(query.model_family.as_deref())
        .bind(query.action_id.as_deref())
        .fetch_one(&self.pool)
        .await?;

        let model_calls = analytics_count(row.try_get("model_calls")?)?;
        let missing_tokens = analytics_count(row.try_get("missing_tokens")?)?;
        let missing_cost = analytics_count(row.try_get("missing_cost")?)?;
        let feedback_total = analytics_count(row.try_get("feedback_total")?)?;
        let feedback_adopted = analytics_count(row.try_get("feedback_adopted")?)?;
        let feedback_modified = analytics_count(row.try_get("feedback_modified")?)?;
        let feedback_rejected = analytics_count(row.try_get("feedback_rejected")?)?;
        let execution_terminal = analytics_count(row.try_get("execution_terminal")?)?;
        let execution_succeeded = analytics_count(row.try_get("execution_succeeded")?)?;
        let incidents = IncidentOperationsMetrics {
            total: analytics_count(row.try_get("incident_total")?)?,
            diagnosed: analytics_count(row.try_get("incident_diagnosed")?)?,
            terminal: analytics_count(row.try_get("incident_terminal")?)?,
            recurrent: analytics_count(row.try_get("incident_recurrent")?)?,
            mean_time_to_detect_seconds: non_negative(row.try_get("mttd")?),
            mean_time_to_resolve_seconds: non_negative(row.try_get("mttr")?),
        };
        let model_usage = ModelUsageMetrics {
            calls: model_calls,
            input_tokens: analytics_count(row.try_get("input_tokens")?)?,
            output_tokens: analytics_count(row.try_get("output_tokens")?)?,
            cost_micros: analytics_count(row.try_get("cost_micros")?)?,
            calls_missing_tokens: missing_tokens,
            calls_missing_cost: missing_cost,
            failed_calls: analytics_count(row.try_get("failed_calls")?)?,
            fallback_calls: analytics_count(row.try_get("fallback_calls")?)?,
            usage_coverage_basis_points: ratio_basis_points(model_calls.saturating_sub(missing_tokens), model_calls),
            cost_coverage_basis_points: ratio_basis_points(model_calls.saturating_sub(missing_cost), model_calls),
        };
        let recommendation_feedback = AutonomyFeedbackMetrics {
            total: feedback_total,
            adopted: feedback_adopted,
            modified: feedback_modified,
            rejected: feedback_rejected,
            adoption_basis_points: ratio_basis_points(feedback_adopted, feedback_total),
            modification_basis_points: ratio_basis_points(feedback_modified, feedback_total),
            rejection_basis_points: ratio_basis_points(feedback_rejected, feedback_total),
        };
        let executions = ExecutionOperationsMetrics {
            total: analytics_count(row.try_get("execution_total")?)?,
            terminal: execution_terminal,
            succeeded: execution_succeeded,
            rolled_back: analytics_count(row.try_get("execution_rolled_back")?)?,
            escalated: analytics_count(row.try_get("execution_escalated")?)?,
            success_basis_points: ratio_basis_points(execution_succeeded, execution_terminal),
        };
        let savings = AttributedAutomationSavingsMetrics {
            successful_no_side_effect_runs: analytics_count(row.try_get("no_side_effect_runs")?)?,
            successful_preventive_runs: analytics_count(row.try_get("preventive_runs")?)?,
            successful_autonomous_actions: analytics_count(row.try_get("autonomous_actions")?)?,
            estimated_minutes_saved: analytics_count(row.try_get("estimated_minutes")?)?,
            estimate_method: SAVINGS_DEFINITION.to_owned(),
        };
        let mut warnings = Vec::new();
        if incidents.total == 0 {
            warnings.push("incident_samples_missing:no incident matches the selected dimensions".to_owned());
        }
        if model_usage.calls == 0 {
            warnings.push("model_usage_missing:no model invocation matches the selected dimensions".to_owned());
        } else {
            if missing_tokens > 0 {
                warnings.push(format!(
                    "model_token_usage_partial:{missing_tokens} invocation(s) did not report token usage"
                ));
            }
            if missing_cost > 0 {
                warnings.push(format!(
                    "model_cost_partial:{missing_cost} invocation(s) did not expose cost"
                ));
            }
        }
        if executions.terminal == 0 {
            warnings.push("execution_samples_missing:no terminal execution matches the selected dimensions".to_owned());
        }
        if recommendation_feedback.total == 0 {
            warnings.push(
                "recommendation_feedback_missing:no recommendation or plan feedback matches the selected dimensions"
                    .to_owned(),
            );
        }

        Ok(OperationsAnalyticsReport {
            schema_version: OPERATIONS_ANALYTICS_SCHEMA_VERSION,
            tenant_id,
            filters: OperationsAnalyticsFilters {
                cluster_ids: cluster_ids.to_vec(),
                scenario: query.scenario.clone(),
                provider_family: query.provider_family.clone(),
                model_family: query.model_family.clone(),
                action_id: query.action_id.clone(),
            },
            window,
            incidents,
            model_usage,
            recommendation_feedback,
            executions,
            savings,
            mttd_definition: MTTD_DEFINITION,
            mttr_definition: MTTR_DEFINITION,
            savings_definition: SAVINGS_DEFINITION,
            warnings,
            observed_at: Utc::now(),
        })
    }
}

fn analytics_count(value: i64) -> Result<u64, ControlPlaneError> {
    u64::try_from(value).map_err(|_| {
        ControlPlaneError::configuration("database contains a negative or overflowing operations analytics counter")
    })
}

fn ratio_basis_points(numerator: u64, denominator: u64) -> Option<u32> {
    if denominator == 0 {
        return None;
    }
    let value = numerator.saturating_mul(10_000) / denominator;
    Some(u32::try_from(value.min(10_000)).unwrap_or(10_000))
}

fn non_negative(value: Option<f64>) -> Option<f64> {
    value.filter(|value| value.is_finite() && *value >= 0.0)
}

#[cfg(test)]
mod tests {
    use super::ratio_basis_points;

    #[test]
    fn execution_success_rate_is_bounded_and_sample_aware() {
        assert_eq!(ratio_basis_points(3, 4), Some(7_500));
        assert_eq!(ratio_basis_points(8, 4), Some(10_000));
        assert_eq!(ratio_basis_points(0, 0), None);
    }
}
