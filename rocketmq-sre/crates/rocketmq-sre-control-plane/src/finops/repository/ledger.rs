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

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::FinOpsCostEntry;
use rocketmq_sre_contracts::TenantId;
use sqlx::Row;

use super::FinOpsRepository;
use super::support::cost_entry_from_row;
use super::support::cost_source_name;
use super::support::workload_name;
use crate::ControlPlaneError;
use crate::finops::model::FinOpsLedgerQuery;
use crate::finops::model::bounded_limit;

impl FinOpsRepository {
    pub(in crate::finops) async fn scope_exists(&self, entry: &FinOpsCostEntry) -> Result<bool, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT EXISTS (
                SELECT 1
                FROM fleet_cluster_registrations registration
                WHERE registration.fleet_id = $1
                  AND registration.tenant_id = $2
                  AND registration.region_id = $3
                  AND ($4::UUID IS NULL OR registration.cluster_id = $4)
                  AND registration.lifecycle_state IN ('active', 'read_only_degraded')
             ) AS present",
        )
        .bind(entry.fleet_id.as_uuid())
        .bind(entry.tenant_id.as_uuid())
        .bind(entry.region_id.as_uuid())
        .bind(entry.cluster_id.map(ClusterId::as_uuid))
        .fetch_one(&self.pool)
        .await?;
        Ok(row.try_get("present")?)
    }

    pub(in crate::finops) async fn record_cost(
        &self,
        entry: &FinOpsCostEntry,
    ) -> Result<FinOpsCostEntry, ControlPlaneError> {
        let inserted = sqlx::query(
            "INSERT INTO finops_cost_ledger (
                id, idempotency_key, fleet_id, tenant_id, region_id, cluster_id,
                source_kind, workload_kind, provider_profile, model_family,
                incident_id, pack_id, workflow_id, request_count, input_tokens,
                output_tokens, latency_millis, error_count, quantity_millis,
                cost_micros, occurred_at, recorded_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19,
                $20, $21, $22
             )
             ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
             RETURNING *",
        )
        .bind(entry.id.as_uuid())
        .bind(&entry.idempotency_key)
        .bind(entry.fleet_id.as_uuid())
        .bind(entry.tenant_id.as_uuid())
        .bind(entry.region_id.as_uuid())
        .bind(entry.cluster_id.map(ClusterId::as_uuid))
        .bind(cost_source_name(entry.source))
        .bind(workload_name(entry.workload_kind))
        .bind(entry.provider_profile.as_deref())
        .bind(entry.model_family.as_deref())
        .bind(entry.incident_id.map(rocketmq_sre_contracts::IncidentId::as_uuid))
        .bind(entry.pack_id.as_deref())
        .bind(entry.workflow_id.as_deref())
        .bind(stored(entry.request_count, "request count")?)
        .bind(stored(entry.input_tokens, "input tokens")?)
        .bind(stored(entry.output_tokens, "output tokens")?)
        .bind(stored(entry.latency_millis, "latency")?)
        .bind(stored(entry.error_count, "error count")?)
        .bind(stored(entry.quantity_millis, "quantity")?)
        .bind(stored(entry.cost_micros, "cost")?)
        .bind(entry.occurred_at)
        .bind(entry.recorded_at)
        .fetch_optional(&self.pool)
        .await?;
        if let Some(row) = inserted {
            return cost_entry_from_row(&row);
        }
        let row = sqlx::query(
            "SELECT *
             FROM finops_cost_ledger
             WHERE tenant_id = $1 AND idempotency_key = $2",
        )
        .bind(entry.tenant_id.as_uuid())
        .bind(&entry.idempotency_key)
        .fetch_one(&self.pool)
        .await?;
        let existing = cost_entry_from_row(&row)?;
        if same_cost_identity(&existing, entry) {
            Ok(existing)
        } else {
            Err(ControlPlaneError::conflict_code(
                "finops_idempotency_conflict",
                "FinOps idempotency key is bound to different cost dimensions or values",
            ))
        }
    }

    pub(in crate::finops) async fn list_costs(
        &self,
        tenant_id: TenantId,
        query: &FinOpsLedgerQuery,
    ) -> Result<(Vec<FinOpsCostEntry>, bool), ControlPlaneError> {
        let limit = bounded_limit(query.limit);
        let source = query.source.map(cost_source_name);
        let rows = sqlx::query(
            "SELECT *
             FROM finops_cost_ledger
             WHERE tenant_id = $1
               AND ($2::UUID IS NULL OR cluster_id = $2)
               AND ($3::TEXT IS NULL OR source_kind = $3)
               AND ($4::TIMESTAMPTZ IS NULL OR occurred_at >= $4)
               AND ($5::TIMESTAMPTZ IS NULL OR occurred_at < $5)
             ORDER BY occurred_at DESC, id
             LIMIT $6",
        )
        .bind(tenant_id.as_uuid())
        .bind(query.cluster_id.map(ClusterId::as_uuid))
        .bind(source)
        .bind(query.from)
        .bind(query.to)
        .bind(limit + 1)
        .fetch_all(&self.pool)
        .await?;
        let truncated = i64::try_from(rows.len()).unwrap_or(i64::MAX) > limit;
        rows.into_iter()
            .take(usize::try_from(limit).unwrap_or(200))
            .map(|row| cost_entry_from_row(&row))
            .collect::<Result<Vec<_>, _>>()
            .map(|items| (items, truncated))
    }
}

fn same_cost_identity(left: &FinOpsCostEntry, right: &FinOpsCostEntry) -> bool {
    left.idempotency_key == right.idempotency_key
        && left.fleet_id == right.fleet_id
        && left.tenant_id == right.tenant_id
        && left.region_id == right.region_id
        && left.cluster_id == right.cluster_id
        && left.source == right.source
        && left.workload_kind == right.workload_kind
        && left.provider_profile == right.provider_profile
        && left.model_family == right.model_family
        && left.incident_id == right.incident_id
        && left.pack_id == right.pack_id
        && left.workflow_id == right.workflow_id
        && left.request_count == right.request_count
        && left.input_tokens == right.input_tokens
        && left.output_tokens == right.output_tokens
        && left.latency_millis == right.latency_millis
        && left.error_count == right.error_count
        && left.quantity_millis == right.quantity_millis
        && left.cost_micros == right.cost_micros
        && left.occurred_at == right.occurred_at
}

fn stored(value: u64, field: &str) -> Result<i64, ControlPlaneError> {
    i64::try_from(value).map_err(|_| {
        ControlPlaneError::validation(
            "invalid_finops_cost",
            format!("FinOps {field} exceeds the supported storage range"),
        )
    })
}
