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

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::FinOpsShowbackRow;
use rocketmq_sre_contracts::TenantId;
use sqlx::Row;

use super::FinOpsRepository;
use crate::ControlPlaneError;
use crate::finops::model::FinOpsReportQuery;
use crate::finops::model::bounded_report_limit;

pub(in crate::finops) struct FinOpsReportData {
    pub(in crate::finops) rows: Vec<FinOpsShowbackRow>,
    pub(in crate::finops) total_cost_micros: u64,
    pub(in crate::finops) entries: u64,
    pub(in crate::finops) entries_missing_cost: u64,
    pub(in crate::finops) successful_outcomes: u64,
    pub(in crate::finops) estimated_minutes_saved: u64,
    pub(in crate::finops) truncated: bool,
}

impl FinOpsRepository {
    pub(in crate::finops) async fn report_data(
        &self,
        tenant_id: TenantId,
        query: &FinOpsReportQuery,
    ) -> Result<FinOpsReportData, ControlPlaneError> {
        let limit = bounded_report_limit(query.limit);
        let rows = sqlx::query(
            "WITH entries AS (
                SELECT
                    source_kind,
                    workload_kind,
                    region_id::TEXT AS region_key,
                    cluster_id::TEXT AS cluster_key,
                    provider_profile,
                    model_family,
                    incident_id::TEXT AS incident_key,
                    pack_id,
                    workflow_id,
                    request_count,
                    input_tokens,
                    output_tokens,
                    latency_millis,
                    error_count,
                    cost_micros,
                    FALSE AS missing_cost
                FROM finops_cost_ledger
                WHERE tenant_id = $1
                  AND occurred_at >= $2 AND occurred_at < $3
                  AND ($4::UUID IS NULL OR cluster_id = $4)
                UNION ALL
                SELECT
                    'model_invocation'::TEXT AS source_kind,
                    CASE
                        WHEN invocation.incident_id IS NOT NULL THEN 'incident'
                        ELSE 'system'
                    END AS workload_kind,
                    registration.region_id::TEXT AS region_key,
                    invocation.cluster_id::TEXT AS cluster_key,
                    invocation.actual_profile_id::TEXT AS provider_profile,
                    invocation.model_family,
                    invocation.incident_id::TEXT AS incident_key,
                    NULL::TEXT AS pack_id,
                    NULL::TEXT AS workflow_id,
                    1::BIGINT AS request_count,
                    COALESCE(invocation.input_tokens, 0)::BIGINT AS input_tokens,
                    COALESCE(invocation.output_tokens, 0)::BIGINT AS output_tokens,
                    GREATEST(
                        EXTRACT(EPOCH FROM (invocation.completed_at - invocation.started_at)) * 1000,
                        0
                    )::BIGINT AS latency_millis,
                    CASE WHEN invocation.error_code IS NULL THEN 0 ELSE 1 END::BIGINT AS error_count,
                    COALESCE(invocation.cost_micros, 0)::BIGINT AS cost_micros,
                    invocation.cost_micros IS NULL AS missing_cost
                FROM model_invocations invocation
                JOIN fleet_cluster_registrations registration
                  ON registration.cluster_id = invocation.cluster_id
                 AND registration.tenant_id = invocation.tenant_id
                WHERE invocation.tenant_id = $1
                  AND invocation.started_at >= $2 AND invocation.started_at < $3
                  AND ($4::UUID IS NULL OR invocation.cluster_id = $4)
             )
             SELECT
                source_kind,
                workload_kind,
                region_key,
                cluster_key,
                provider_profile,
                model_family,
                incident_key,
                pack_id,
                workflow_id,
                COALESCE(SUM(request_count), 0)::BIGINT AS request_count,
                COALESCE(SUM(input_tokens), 0)::BIGINT AS input_tokens,
                COALESCE(SUM(output_tokens), 0)::BIGINT AS output_tokens,
                COALESCE(SUM(latency_millis), 0)::BIGINT AS latency_millis,
                COALESCE(SUM(error_count), 0)::BIGINT AS error_count,
                COALESCE(SUM(cost_micros), 0)::BIGINT AS cost_micros,
                COUNT(*)::BIGINT AS entries,
                COUNT(*) FILTER (WHERE missing_cost)::BIGINT AS entries_missing_cost
             FROM entries
             GROUP BY
                source_kind, workload_kind, region_key, cluster_key,
                provider_profile, model_family, incident_key, pack_id, workflow_id
             ORDER BY cost_micros DESC, request_count DESC, source_kind
             LIMIT $5",
        )
        .bind(tenant_id.as_uuid())
        .bind(query.from)
        .bind(query.to)
        .bind(query.cluster_id.map(ClusterId::as_uuid))
        .bind(limit + 1)
        .fetch_all(&self.pool)
        .await?;
        let truncated = i64::try_from(rows.len()).unwrap_or(i64::MAX) > limit;
        let mut report_rows = Vec::new();
        let mut total_cost_micros = 0_u64;
        let mut entries = 0_u64;
        let mut entries_missing_cost = 0_u64;
        for row in rows.into_iter().take(usize::try_from(limit).unwrap_or(500)) {
            let requests = unsigned(row.try_get("request_count")?, "report request count")?;
            let latency = unsigned(row.try_get("latency_millis")?, "report latency")?;
            let cost = unsigned(row.try_get("cost_micros")?, "report cost")?;
            let row_entries = unsigned(row.try_get("entries")?, "report entries")?;
            let missing = unsigned(row.try_get("entries_missing_cost")?, "report entries missing cost")?;
            total_cost_micros = total_cost_micros.saturating_add(cost);
            entries = entries.saturating_add(row_entries);
            entries_missing_cost = entries_missing_cost.saturating_add(missing);
            report_rows.push(FinOpsShowbackRow {
                dimensions: dimensions(&row)?,
                request_count: requests,
                input_tokens: unsigned(row.try_get("input_tokens")?, "report input tokens")?,
                output_tokens: unsigned(row.try_get("output_tokens")?, "report output tokens")?,
                error_count: unsigned(row.try_get("error_count")?, "report errors")?,
                average_latency_millis: (requests > 0).then(|| latency / requests),
                cost_micros: cost,
                successful_outcomes: 0,
                slo_compliant_outcomes: 0,
                estimated_minutes_saved: 0,
            });
        }
        let (successful_outcomes, estimated_minutes_saved) = self.outcomes_and_savings(tenant_id, query).await?;
        if let Some(tenant_row) = report_rows.first_mut() {
            tenant_row.successful_outcomes = successful_outcomes;
            tenant_row.estimated_minutes_saved = estimated_minutes_saved;
        }
        Ok(FinOpsReportData {
            rows: report_rows,
            total_cost_micros,
            entries,
            entries_missing_cost,
            successful_outcomes,
            estimated_minutes_saved,
            truncated,
        })
    }

    pub(in crate::finops) async fn window_cost(
        &self,
        tenant_id: TenantId,
        cluster_id: Option<ClusterId>,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<u64, ControlPlaneError> {
        let row = sqlx::query(
            "WITH costs AS (
                SELECT cost_micros
                FROM finops_cost_ledger
                WHERE tenant_id = $1 AND occurred_at >= $2 AND occurred_at < $3
                  AND ($4::UUID IS NULL OR cluster_id = $4)
                UNION ALL
                SELECT COALESCE(cost_micros, 0)::BIGINT
                FROM model_invocations
                WHERE tenant_id = $1 AND started_at >= $2 AND started_at < $3
                  AND ($4::UUID IS NULL OR cluster_id = $4)
             )
             SELECT COALESCE(SUM(cost_micros), 0)::BIGINT AS cost_micros FROM costs",
        )
        .bind(tenant_id.as_uuid())
        .bind(from)
        .bind(to)
        .bind(cluster_id.map(ClusterId::as_uuid))
        .fetch_one(&self.pool)
        .await?;
        unsigned(row.try_get("cost_micros")?, "window cost")
    }

    async fn outcomes_and_savings(
        &self,
        tenant_id: TenantId,
        query: &FinOpsReportQuery,
    ) -> Result<(u64, u64), ControlPlaneError> {
        let row = sqlx::query(
            "SELECT
                (
                    SELECT COUNT(*)
                    FROM autonomy_outcomes
                    WHERE tenant_id = $1
                      AND occurred_at >= $2 AND occurred_at < $3
                      AND outcome_class = 'success'
                      AND ($4::UUID IS NULL OR cluster_id = $4)
                ) AS successful_outcomes,
                (
                    SELECT COUNT(*) * 4
                    FROM no_side_effect_automation_runs
                    WHERE tenant_id = $1
                      AND completed_at >= $2 AND completed_at < $3
                      AND status = 'succeeded'
                      AND ($4::UUID IS NULL OR cluster_id = $4)
                ) + (
                    SELECT COUNT(*) * 12
                    FROM preventive_automation_runs
                    WHERE tenant_id = $1
                      AND completed_at >= $2 AND completed_at < $3
                      AND status = 'succeeded'
                      AND ($4::UUID IS NULL OR cluster_id = $4)
                ) AS estimated_minutes_saved",
        )
        .bind(tenant_id.as_uuid())
        .bind(query.from)
        .bind(query.to)
        .bind(query.cluster_id.map(ClusterId::as_uuid))
        .fetch_one(&self.pool)
        .await?;
        Ok((
            unsigned(row.try_get("successful_outcomes")?, "successful outcomes")?,
            unsigned(row.try_get("estimated_minutes_saved")?, "estimated minutes saved")?,
        ))
    }
}

fn dimensions(row: &sqlx::postgres::PgRow) -> Result<BTreeMap<String, String>, ControlPlaneError> {
    let mut dimensions = BTreeMap::from([
        ("source".to_owned(), row.try_get("source_kind")?),
        ("workload".to_owned(), row.try_get("workload_kind")?),
    ]);
    for (column, name) in [
        ("region_key", "region"),
        ("cluster_key", "cluster"),
        ("provider_profile", "provider_profile"),
        ("model_family", "model"),
        ("incident_key", "incident"),
        ("pack_id", "diagnostic_pack"),
        ("workflow_id", "workflow"),
    ] {
        if let Some(value) = row.try_get::<Option<String>, _>(column)? {
            dimensions.insert(name.to_owned(), value);
        }
    }
    Ok(dimensions)
}

fn unsigned(value: i64, field: &str) -> Result<u64, ControlPlaneError> {
    u64::try_from(value).map_err(|_| {
        ControlPlaneError::validation(
            "invalid_persisted_finops_state",
            format!("persisted FinOps {field} is invalid"),
        )
    })
}
