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
use rocketmq_sre_contracts::AutonomyOutcome;
use rocketmq_sre_contracts::AutonomyOutcomeClass;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::TenantId;
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

mod metrics;

use super::operations::ActionOutcomeBreakdown;
use super::operations::AutonomyOperationalReport;
use super::operations::AutonomyOutcomeListQuery;
use super::operations::AutonomyOutcomeMetrics;
use super::operations::AutonomyReportWindow;
use super::operations::IncidentModelCost;
use super::operations::ModelCostBreakdown;
use super::operations::ModelUsageMetrics;
use super::operations::OPERATIONS_SCHEMA_VERSION;
use super::operations::VersionEffectComparison;
use crate::ControlPlaneError;
use crate::PostgresRepository;

const MAX_BREAKDOWN_ROWS: i64 = 201;
const SAVINGS_METHOD: &str = "fixed conservative estimate: alert correlation 3m, owner/severity 4m, evidence 10m, \
                              shift summary 15m, notification 2m, postmortem draft 30m, preventive inspection 20m";

impl PostgresRepository {
    pub(super) async fn autonomy_outcomes(
        &self,
        tenant_id: TenantId,
        authorized_clusters: &[ClusterId],
        query: &AutonomyOutcomeListQuery,
        limit: i64,
    ) -> Result<Vec<AutonomyOutcome>, ControlPlaneError> {
        let cluster_ids = cluster_uuids(authorized_clusters);
        let action = query.action.map(|action| action.id());
        let class = query.class.map(outcome_class_name);
        let rows = sqlx::query(
            "SELECT outcome_snapshot
             FROM autonomy_outcomes
             WHERE tenant_id = $1
               AND cluster_id = ANY($2)
               AND ($3::UUID IS NULL OR cluster_id = $3)
               AND ($4::TEXT IS NULL OR action_id = $4)
               AND ($5::TEXT IS NULL OR outcome_class = $5)
               AND ($6::TIMESTAMPTZ IS NULL OR occurred_at >= $6)
               AND ($7::TIMESTAMPTZ IS NULL OR occurred_at < $7)
             ORDER BY occurred_at DESC, sequence_id DESC
             LIMIT $8",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_ids)
        .bind(query.cluster_id.map(ClusterId::as_uuid))
        .bind(action)
        .bind(class)
        .bind(query.from)
        .bind(query.until)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| parse_json(row.try_get("outcome_snapshot")?, "autonomy outcome"))
            .collect()
    }

    pub(super) async fn build_autonomy_operational_report(
        &self,
        tenant_id: TenantId,
        cluster_ids: &[ClusterId],
        window: AutonomyReportWindow,
    ) -> Result<AutonomyOperationalReport, ControlPlaneError> {
        let clusters = cluster_uuids(cluster_ids);
        let outcomes = self.outcome_metrics(tenant_id, &clusters, &window).await?;
        let durations = self.duration_metrics(tenant_id, &clusters, &window).await?;
        let quality = self.quality_metrics(tenant_id, &clusters, &window).await?;
        let feedback = self.feedback_metrics(tenant_id, &clusters, &window).await?;
        let savings = self.savings_metrics(tenant_id, &clusters, &window).await?;
        let model_usage = self.model_usage_metrics(tenant_id, &clusters, &window).await?;
        let (action_breakdown, action_truncated) = self.action_breakdown(tenant_id, &clusters, &window).await?;
        let (model_breakdown, model_truncated) = self.model_breakdown(tenant_id, &clusters, &window).await?;
        let (incident_costs, incident_truncated) = self.incident_costs(tenant_id, &clusters, &window).await?;
        let (version_effects, version_truncated) = self.version_effects(tenant_id, &clusters, &window).await?;
        let mut warnings = Vec::new();
        if model_usage.calls == 0 {
            warnings.push("model_usage_missing:no model invocation facts exist for this period".to_owned());
        } else {
            if model_usage.calls_missing_tokens > 0 {
                warnings.push(format!(
                    "model_token_usage_partial:{} invocation(s) did not report token usage",
                    model_usage.calls_missing_tokens
                ));
            }
            if model_usage.calls_missing_cost > 0 {
                warnings.push(format!(
                    "model_cost_partial:{} invocation(s) did not expose token-derived cost",
                    model_usage.calls_missing_cost
                ));
            }
        }
        if action_truncated || model_truncated || incident_truncated || version_truncated {
            warnings.push("breakdown_truncated:one or more report dimensions exceeded 200 rows".to_owned());
        }
        Ok(AutonomyOperationalReport {
            schema_version: OPERATIONS_SCHEMA_VERSION.to_owned(),
            tenant_id,
            cluster_ids: cluster_ids.to_vec(),
            window,
            outcomes,
            durations,
            quality,
            feedback,
            savings,
            model_usage,
            action_breakdown,
            model_breakdown,
            incident_costs,
            version_effects,
            budget_alerts: Vec::new(),
            optimization_candidates: Vec::new(),
            warnings,
            generated_at: Utc::now(),
        })
    }

    pub(super) async fn persist_autonomy_operational_report(
        &self,
        report: &AutonomyOperationalReport,
    ) -> Result<bool, ControlPlaneError> {
        if !report.window.complete {
            return Err(ControlPlaneError::validation(
                "open_report_period",
                "only completed UTC report periods can be persisted",
            ));
        }
        let snapshot = serde_json::to_value(report).map_err(|_| {
            ControlPlaneError::validation(
                "invalid_operational_report",
                "autonomy operational report cannot be represented as JSON",
            )
        })?;
        let inserted = sqlx::query(
            "INSERT INTO autonomy_operational_reports (
                id, tenant_id, period_kind, period_start, period_end,
                report_snapshot, generated_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (tenant_id, period_kind, period_start, period_end)
             DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(report.tenant_id.as_uuid())
        .bind(report.window.period.as_str())
        .bind(report.window.start)
        .bind(report.window.end)
        .bind(snapshot)
        .bind(report.generated_at)
        .execute(&self.pool)
        .await?;
        Ok(inserted.rows_affected() == 1)
    }

    pub(super) async fn report_tenant_scopes(&self) -> Result<Vec<(TenantId, Vec<ClusterId>)>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT tenant_id, ARRAY_AGG(id ORDER BY id) AS cluster_ids
             FROM clusters
             WHERE onboarding_state <> 'offboarded'
             GROUP BY tenant_id
             ORDER BY tenant_id
             LIMIT 1000",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                let tenant_id = row.try_get::<String, _>("tenant_id")?.parse().map_err(|_| {
                    ControlPlaneError::configuration("database contains an invalid report tenant identifier")
                })?;
                let cluster_ids = row
                    .try_get::<Vec<Uuid>, _>("cluster_ids")?
                    .into_iter()
                    .map(ClusterId::from_uuid)
                    .collect();
                Ok((tenant_id, cluster_ids))
            })
            .collect()
    }

    async fn outcome_metrics(
        &self,
        tenant_id: TenantId,
        clusters: &[Uuid],
        window: &AutonomyReportWindow,
    ) -> Result<AutonomyOutcomeMetrics, ControlPlaneError> {
        let row = sqlx::query(
            "WITH candidate_rows AS (
                SELECT plan_id, qualified AS eligible, NOT qualified AS denied
                FROM autonomy_shadow_outcomes
                WHERE tenant_id = $1 AND cluster_id = ANY($2)
                  AND observed_at >= $3 AND observed_at < $4
                UNION ALL
                SELECT plan_id,
                       outcome_class <> 'expected_deny' AS eligible,
                       outcome_class = 'expected_deny' AS denied
                FROM autonomy_outcomes
                WHERE tenant_id = $1 AND cluster_id = ANY($2)
                  AND occurred_at >= $3 AND occurred_at < $4
             ), candidates AS (
                SELECT plan_id, BOOL_OR(eligible) AS eligible, BOOL_OR(denied) AS denied
                FROM candidate_rows GROUP BY plan_id
             ), terminal AS (
                SELECT outcome_class, failure_code
                FROM autonomy_outcomes
                WHERE tenant_id = $1 AND cluster_id = ANY($2)
                  AND occurred_at >= $3 AND occurred_at < $4
             )
             SELECT
                (SELECT COUNT(*) FROM candidates) AS candidates,
                (SELECT COUNT(*) FILTER (WHERE eligible) FROM candidates) AS eligible,
                (SELECT COUNT(*) FILTER (WHERE denied AND NOT eligible) FROM candidates) AS denied,
                COUNT(*) FILTER (WHERE outcome_class = 'success') AS successes,
                COUNT(*) FILTER (WHERE outcome_class = 'autonomous_execution_failure') AS execution_failures,
                COUNT(*) FILTER (WHERE failure_code = 'rolled_back') AS rollbacks,
                COUNT(*) FILTER (WHERE failure_code = 'unknown_effect') AS unknown_effects,
                COUNT(*) FILTER (
                    WHERE failure_code IN (
                        'unknown_effect', 'escalated', 'operator_stopped',
                        'critic_unavailable', 'critic_invalid', 'critic_conflict',
                        'evidence_degraded'
                    )
                ) AS human_handoffs
             FROM terminal",
        )
        .bind(tenant_id.as_uuid())
        .bind(clusters)
        .bind(window.start)
        .bind(window.end)
        .fetch_one(&self.pool)
        .await?;
        Ok(AutonomyOutcomeMetrics {
            candidates: count(row.try_get("candidates")?)?,
            eligible: count(row.try_get("eligible")?)?,
            denied: count(row.try_get("denied")?)?,
            successes: count(row.try_get("successes")?)?,
            execution_failures: count(row.try_get("execution_failures")?)?,
            rollbacks: count(row.try_get("rollbacks")?)?,
            unknown_effects: count(row.try_get("unknown_effects")?)?,
            human_handoffs: count(row.try_get("human_handoffs")?)?,
        })
    }

    async fn model_usage_metrics(
        &self,
        tenant_id: TenantId,
        clusters: &[Uuid],
        window: &AutonomyReportWindow,
    ) -> Result<ModelUsageMetrics, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT
                COUNT(*) AS calls,
                COALESCE(SUM(input_tokens), 0)::BIGINT AS input_tokens,
                COALESCE(SUM(output_tokens), 0)::BIGINT AS output_tokens,
                COALESCE(SUM(cost_micros), 0)::BIGINT AS cost_micros,
                COUNT(*) FILTER (WHERE input_tokens IS NULL OR output_tokens IS NULL) AS missing_tokens,
                COUNT(*) FILTER (WHERE cost_micros IS NULL) AS missing_cost,
                COUNT(*) FILTER (WHERE error_code IS NOT NULL) AS failed_calls,
                COUNT(*) FILTER (WHERE CARDINALITY(fallback_chain) > 0) AS fallback_calls
             FROM model_invocations
             WHERE tenant_id = $1 AND cluster_id = ANY($2)
               AND started_at >= $3 AND started_at < $4",
        )
        .bind(tenant_id.as_uuid())
        .bind(clusters)
        .bind(window.start)
        .bind(window.end)
        .fetch_one(&self.pool)
        .await?;
        model_usage_from_row(&row)
    }

    async fn action_breakdown(
        &self,
        tenant_id: TenantId,
        clusters: &[Uuid],
        window: &AutonomyReportWindow,
    ) -> Result<(Vec<ActionOutcomeBreakdown>, bool), ControlPlaneError> {
        let rows = sqlx::query(
            "WITH candidate_rows AS (
                SELECT cluster_id, action_id, action_version, plan_id,
                       qualified AS eligible, NOT qualified AS denied
                FROM autonomy_shadow_outcomes
                WHERE tenant_id = $1 AND cluster_id = ANY($2)
                  AND observed_at >= $3 AND observed_at < $4
                UNION ALL
                SELECT cluster_id, action_id, action_version, plan_id,
                       outcome_class <> 'expected_deny',
                       outcome_class = 'expected_deny'
                FROM autonomy_outcomes
                WHERE tenant_id = $1 AND cluster_id = ANY($2)
                  AND occurred_at >= $3 AND occurred_at < $4
             ), candidate_rollup AS (
                SELECT cluster_id, action_id, action_version, plan_id,
                       BOOL_OR(eligible) AS eligible, BOOL_OR(denied) AS denied
                FROM candidate_rows
                GROUP BY cluster_id, action_id, action_version, plan_id
             ), terminal AS (
                SELECT outcome.cluster_id, outcome.action_id, outcome.action_version,
                       outcome.outcome_class, outcome.failure_code,
                       EXTRACT(EPOCH FROM (execution.completed_at - execution.started_at))
                           FILTER (WHERE execution.completed_at IS NOT NULL) AS execution_seconds
                FROM autonomy_outcomes outcome
                LEFT JOIN executions execution ON execution.id = outcome.execution_id
                WHERE outcome.tenant_id = $1 AND outcome.cluster_id = ANY($2)
                  AND outcome.occurred_at >= $3 AND outcome.occurred_at < $4
             ), candidate_counts AS (
                SELECT cluster_id, action_id, action_version,
                       COUNT(*) AS candidates,
                       COUNT(*) FILTER (WHERE eligible) AS eligible,
                       COUNT(*) FILTER (WHERE denied AND NOT eligible) AS denied
                FROM candidate_rollup
                GROUP BY cluster_id, action_id, action_version
             ), terminal_counts AS (
                SELECT cluster_id, action_id, action_version,
                       COUNT(*) FILTER (WHERE outcome_class = 'success') AS successes,
                       COUNT(*) FILTER (WHERE outcome_class = 'autonomous_execution_failure') AS execution_failures,
                       COUNT(*) FILTER (WHERE failure_code = 'rolled_back') AS rollbacks,
                       COUNT(*) FILTER (WHERE failure_code = 'unknown_effect') AS unknown_effects,
                       COUNT(*) FILTER (
                           WHERE failure_code IN (
                               'unknown_effect', 'escalated', 'operator_stopped',
                               'critic_unavailable', 'critic_invalid', 'critic_conflict',
                               'evidence_degraded'
                           )
                       ) AS human_handoffs,
                       AVG(execution_seconds)::DOUBLE PRECISION AS average_execution_seconds
                FROM terminal
                GROUP BY cluster_id, action_id, action_version
             )
             SELECT candidate.cluster_id, candidate.action_id, candidate.action_version,
                    candidate.candidates, candidate.eligible, candidate.denied,
                    COALESCE(terminal.successes, 0) AS successes,
                    COALESCE(terminal.execution_failures, 0) AS execution_failures,
                    COALESCE(terminal.rollbacks, 0) AS rollbacks,
                    COALESCE(terminal.unknown_effects, 0) AS unknown_effects,
                    COALESCE(terminal.human_handoffs, 0) AS human_handoffs,
                    terminal.average_execution_seconds
             FROM candidate_counts candidate
             LEFT JOIN terminal_counts terminal USING (cluster_id, action_id, action_version)
             ORDER BY candidate.cluster_id, candidate.action_id, candidate.action_version
             LIMIT $5",
        )
        .bind(tenant_id.as_uuid())
        .bind(clusters)
        .bind(window.start)
        .bind(window.end)
        .bind(MAX_BREAKDOWN_ROWS)
        .fetch_all(&self.pool)
        .await?;
        bounded_map(rows, |row| {
            Ok(ActionOutcomeBreakdown {
                cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
                action_id: row.try_get("action_id")?,
                action_version: row.try_get("action_version")?,
                outcomes: AutonomyOutcomeMetrics {
                    candidates: count(row.try_get("candidates")?)?,
                    eligible: count(row.try_get("eligible")?)?,
                    denied: count(row.try_get("denied")?)?,
                    successes: count(row.try_get("successes")?)?,
                    execution_failures: count(row.try_get("execution_failures")?)?,
                    rollbacks: count(row.try_get("rollbacks")?)?,
                    unknown_effects: count(row.try_get("unknown_effects")?)?,
                    human_handoffs: count(row.try_get("human_handoffs")?)?,
                },
                average_execution_seconds: non_negative(row.try_get("average_execution_seconds")?),
            })
        })
    }

    async fn model_breakdown(
        &self,
        tenant_id: TenantId,
        clusters: &[Uuid],
        window: &AutonomyReportWindow,
    ) -> Result<(Vec<ModelCostBreakdown>, bool), ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT provider_family, model_family, model_revision, actual_profile_id,
                    COUNT(*) AS calls,
                    COALESCE(SUM(input_tokens), 0)::BIGINT AS input_tokens,
                    COALESCE(SUM(output_tokens), 0)::BIGINT AS output_tokens,
                    COALESCE(SUM(cost_micros), 0)::BIGINT AS cost_micros,
                    COUNT(*) FILTER (WHERE input_tokens IS NULL OR output_tokens IS NULL) AS missing_tokens,
                    COUNT(*) FILTER (WHERE cost_micros IS NULL) AS missing_cost,
                    COUNT(*) FILTER (WHERE error_code IS NOT NULL) AS failed_calls,
                    COUNT(*) FILTER (WHERE CARDINALITY(fallback_chain) > 0) AS fallback_calls
             FROM model_invocations
             WHERE tenant_id = $1 AND cluster_id = ANY($2)
               AND started_at >= $3 AND started_at < $4
             GROUP BY provider_family, model_family, model_revision, actual_profile_id
             ORDER BY cost_micros DESC, calls DESC, provider_family, model_family
             LIMIT $5",
        )
        .bind(tenant_id.as_uuid())
        .bind(clusters)
        .bind(window.start)
        .bind(window.end)
        .bind(MAX_BREAKDOWN_ROWS)
        .fetch_all(&self.pool)
        .await?;
        bounded_map(rows, |row| {
            Ok(ModelCostBreakdown {
                provider_family: row.try_get("provider_family")?,
                model_family: row.try_get("model_family")?,
                model_revision: row.try_get("model_revision")?,
                actual_profile_id: row.try_get("actual_profile_id")?,
                usage: model_usage_from_row(row)?,
            })
        })
    }

    async fn incident_costs(
        &self,
        tenant_id: TenantId,
        clusters: &[Uuid],
        window: &AutonomyReportWindow,
    ) -> Result<(Vec<IncidentModelCost>, bool), ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT incident_id,
                    COUNT(*) AS calls,
                    COALESCE(SUM(input_tokens), 0)::BIGINT AS input_tokens,
                    COALESCE(SUM(output_tokens), 0)::BIGINT AS output_tokens,
                    COALESCE(SUM(cost_micros), 0)::BIGINT AS cost_micros,
                    COUNT(*) FILTER (WHERE input_tokens IS NULL OR output_tokens IS NULL) AS missing_tokens,
                    COUNT(*) FILTER (WHERE cost_micros IS NULL) AS missing_cost,
                    COUNT(*) FILTER (WHERE error_code IS NOT NULL) AS failed_calls,
                    COUNT(*) FILTER (WHERE CARDINALITY(fallback_chain) > 0) AS fallback_calls
             FROM model_invocations
             WHERE tenant_id = $1 AND cluster_id = ANY($2)
               AND incident_id IS NOT NULL
               AND started_at >= $3 AND started_at < $4
             GROUP BY incident_id
             ORDER BY cost_micros DESC, calls DESC, incident_id
             LIMIT $5",
        )
        .bind(tenant_id.as_uuid())
        .bind(clusters)
        .bind(window.start)
        .bind(window.end)
        .bind(MAX_BREAKDOWN_ROWS)
        .fetch_all(&self.pool)
        .await?;
        bounded_map(rows, |row| {
            Ok(IncidentModelCost {
                incident_id: rocketmq_sre_contracts::IncidentId::from_uuid(row.try_get("incident_id")?),
                usage: model_usage_from_row(row)?,
            })
        })
    }

    async fn version_effects(
        &self,
        tenant_id: TenantId,
        clusters: &[Uuid],
        window: &AutonomyReportWindow,
    ) -> Result<(Vec<VersionEffectComparison>, bool), ControlPlaneError> {
        let rows = sqlx::query(
            "WITH action_effects AS (
                SELECT 'action'::TEXT AS dimension,
                       outcome.action_id || '@' || outcome.action_version AS version,
                       COUNT(*) AS samples,
                       COUNT(*) FILTER (WHERE outcome.outcome_class = 'success') AS successes,
                       COALESCE(SUM(invocation.cost_micros), 0)::BIGINT AS cost_micros
                FROM autonomy_outcomes outcome
                LEFT JOIN action_plans plan ON plan.id = outcome.plan_id
                LEFT JOIN model_invocations invocation ON invocation.id = plan.primary_model_invocation_id
                WHERE outcome.tenant_id = $1 AND outcome.cluster_id = ANY($2)
                  AND outcome.occurred_at >= $3 AND outcome.occurred_at < $4
                GROUP BY outcome.action_id, outcome.action_version
             ), pack_effects AS (
                SELECT 'diagnostic_pack'::TEXT AS dimension,
                       cohort.diagnostic_pack_id || '@' || cohort.diagnostic_pack_version AS version,
                       COUNT(*) AS samples,
                       COUNT(*) FILTER (WHERE outcome.outcome_class = 'success') AS successes,
                       0::BIGINT AS cost_micros
                FROM autonomy_outcomes outcome
                JOIN autonomy_qualification_cohorts cohort ON cohort.id = outcome.cohort_id
                WHERE outcome.tenant_id = $1 AND outcome.cluster_id = ANY($2)
                  AND outcome.occurred_at >= $3 AND outcome.occurred_at < $4
                GROUP BY cohort.diagnostic_pack_id, cohort.diagnostic_pack_version
             ), policy_effects AS (
                SELECT 'policy'::TEXT AS dimension,
                       cohort.policy_id::TEXT || '@' || cohort.policy_definition_version::TEXT AS version,
                       COUNT(*) AS samples,
                       COUNT(*) FILTER (WHERE outcome.outcome_class = 'success') AS successes,
                       0::BIGINT AS cost_micros
                FROM autonomy_outcomes outcome
                JOIN autonomy_qualification_cohorts cohort ON cohort.id = outcome.cohort_id
                WHERE outcome.tenant_id = $1 AND outcome.cluster_id = ANY($2)
                  AND outcome.occurred_at >= $3 AND outcome.occurred_at < $4
                GROUP BY cohort.policy_id, cohort.policy_definition_version
             ), model_effects AS (
                SELECT 'model'::TEXT AS dimension,
                       provider_family || '/' || model_family || '@' || model_revision AS version,
                       COUNT(*) AS samples,
                       COUNT(*) FILTER (WHERE error_code IS NULL) AS successes,
                       COALESCE(SUM(cost_micros), 0)::BIGINT AS cost_micros
                FROM model_invocations
                WHERE tenant_id = $1 AND cluster_id = ANY($2)
                  AND started_at >= $3 AND started_at < $4
                GROUP BY provider_family, model_family, model_revision
             ), prompt_effects AS (
                SELECT 'prompt'::TEXT AS dimension, prompt_version AS version,
                       COUNT(*) AS samples,
                       COUNT(*) FILTER (WHERE error_code IS NULL) AS successes,
                       COALESCE(SUM(cost_micros), 0)::BIGINT AS cost_micros
                FROM model_invocations
                WHERE tenant_id = $1 AND cluster_id = ANY($2)
                  AND started_at >= $3 AND started_at < $4
                GROUP BY prompt_version
             )
             SELECT * FROM action_effects
             UNION ALL SELECT * FROM pack_effects
             UNION ALL SELECT * FROM policy_effects
             UNION ALL SELECT * FROM model_effects
             UNION ALL SELECT * FROM prompt_effects
             ORDER BY dimension, version
             LIMIT $5",
        )
        .bind(tenant_id.as_uuid())
        .bind(clusters)
        .bind(window.start)
        .bind(window.end)
        .bind(MAX_BREAKDOWN_ROWS)
        .fetch_all(&self.pool)
        .await?;
        bounded_map(rows, |row| {
            let samples = count(row.try_get("samples")?)?;
            let successes = count(row.try_get("successes")?)?;
            Ok(VersionEffectComparison {
                dimension: row.try_get("dimension")?,
                version: row.try_get("version")?,
                samples,
                successes,
                success_basis_points: ratio_basis_points(successes, samples),
                cost_micros: count(row.try_get("cost_micros")?)?,
            })
        })
    }
}

fn model_usage_from_row(row: &sqlx::postgres::PgRow) -> Result<ModelUsageMetrics, ControlPlaneError> {
    let calls = count(row.try_get("calls")?)?;
    let calls_missing_tokens = count(row.try_get("missing_tokens")?)?;
    let calls_missing_cost = count(row.try_get("missing_cost")?)?;
    Ok(ModelUsageMetrics {
        calls,
        input_tokens: count(row.try_get("input_tokens")?)?,
        output_tokens: count(row.try_get("output_tokens")?)?,
        cost_micros: count(row.try_get("cost_micros")?)?,
        calls_missing_tokens,
        calls_missing_cost,
        failed_calls: count(row.try_get("failed_calls")?)?,
        fallback_calls: count(row.try_get("fallback_calls")?)?,
        usage_coverage_basis_points: ratio_basis_points(calls.saturating_sub(calls_missing_tokens), calls),
        cost_coverage_basis_points: ratio_basis_points(calls.saturating_sub(calls_missing_cost), calls),
    })
}

fn bounded_map<T>(
    mut rows: Vec<sqlx::postgres::PgRow>,
    mapper: impl Fn(&sqlx::postgres::PgRow) -> Result<T, ControlPlaneError>,
) -> Result<(Vec<T>, bool), ControlPlaneError> {
    let truncated = rows.len() >= usize::try_from(MAX_BREAKDOWN_ROWS).unwrap_or(usize::MAX);
    if truncated {
        rows.pop();
    }
    Ok((rows.iter().map(mapper).collect::<Result<_, _>>()?, truncated))
}

fn cluster_uuids(cluster_ids: &[ClusterId]) -> Vec<Uuid> {
    cluster_ids.iter().copied().map(ClusterId::as_uuid).collect()
}

fn count(value: i64) -> Result<u64, ControlPlaneError> {
    u64::try_from(value).map_err(|_| {
        ControlPlaneError::configuration("database contains a negative or overflowing autonomy report counter")
    })
}

fn ratio_basis_points(numerator: u64, denominator: u64) -> Option<u32> {
    if denominator == 0 {
        return None;
    }
    let value = numerator.saturating_mul(10_000) / denominator;
    Some(u32::try_from(value.min(10_000)).unwrap_or(10_000))
}

fn reduction_basis_points(raw: u64, correlated: u64) -> Option<u32> {
    ratio_basis_points(raw.saturating_sub(correlated.min(raw)), raw)
}

fn non_negative(value: Option<f64>) -> Option<f64> {
    value.filter(|value| value.is_finite() && *value >= 0.0)
}

fn parse_json<T: serde::de::DeserializeOwned>(value: Value, name: &'static str) -> Result<T, ControlPlaneError> {
    serde_json::from_value(value)
        .map_err(|_| ControlPlaneError::configuration(format!("database contains an incompatible {name} snapshot")))
}

const fn outcome_class_name(class: AutonomyOutcomeClass) -> &'static str {
    match class {
        AutonomyOutcomeClass::ExpectedDeny => "expected_deny",
        AutonomyOutcomeClass::Success => "success",
        AutonomyOutcomeClass::AutonomousExecutionFailure => "autonomous_execution_failure",
    }
}

#[cfg(test)]
mod tests {
    use super::ratio_basis_points;
    use super::reduction_basis_points;

    #[test]
    fn rates_are_bounded_and_missing_when_denominator_is_zero() {
        assert_eq!(ratio_basis_points(1, 4), Some(2_500));
        assert_eq!(ratio_basis_points(8, 4), Some(10_000));
        assert_eq!(ratio_basis_points(1, 0), None);
        assert_eq!(reduction_basis_points(10, 3), Some(7_000));
    }
}
