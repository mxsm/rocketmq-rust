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

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ComplianceFinding;
use rocketmq_sre_contracts::ComplianceFindingId;
use rocketmq_sre_contracts::FleetAssetIndex;
use rocketmq_sre_contracts::FleetInspectionRun;
use rocketmq_sre_contracts::FleetInspectionRunId;
use rocketmq_sre_contracts::FleetInspectionState;
use rocketmq_sre_contracts::TenantId;
use sqlx::Row;
use uuid::Uuid;

use super::FleetRepository;
use super::support::asset_from_row;
use super::support::compliance_severity_name;
use super::support::environment_name;
use super::support::finding_from_row;
use super::support::finding_state_name;
use super::support::inspection_from_row;
use crate::ControlPlaneError;
use crate::fleet::model::ComplianceFindingQuery;
use crate::fleet::model::CreateFleetInspectionRequest;
use crate::fleet::model::EvaluateComplianceRequest;
use crate::fleet::model::FleetScopeQuery;
use crate::fleet::model::UpdateFleetInspectionRequest;
use crate::fleet::model::bounded_limit;

impl FleetRepository {
    pub(in crate::fleet) async fn upsert_asset(
        &self,
        asset: &FleetAssetIndex,
    ) -> Result<FleetAssetIndex, ControlPlaneError> {
        let attributes = serde_json::to_value(&asset.attributes)
            .map_err(|_| ControlPlaneError::validation("invalid_request", "Fleet asset attributes are invalid"))?;
        let row = sqlx::query(
            "INSERT INTO fleet_asset_index (
                cluster_id, fleet_id, tenant_id, region_id, environment,
                owner_name, component, component_version, image_digest,
                feature_digest, configuration_digest, health, attributes,
                observed_at
             )
             VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
             )
             ON CONFLICT (cluster_id, component) DO UPDATE SET
                fleet_id = EXCLUDED.fleet_id,
                tenant_id = EXCLUDED.tenant_id,
                region_id = EXCLUDED.region_id,
                environment = EXCLUDED.environment,
                owner_name = EXCLUDED.owner_name,
                component_version = EXCLUDED.component_version,
                image_digest = EXCLUDED.image_digest,
                feature_digest = EXCLUDED.feature_digest,
                configuration_digest = EXCLUDED.configuration_digest,
                health = EXCLUDED.health,
                attributes = EXCLUDED.attributes,
                observed_at = EXCLUDED.observed_at
             WHERE fleet_asset_index.tenant_id = EXCLUDED.tenant_id
               AND fleet_asset_index.region_id = EXCLUDED.region_id
               AND fleet_asset_index.observed_at <= EXCLUDED.observed_at
             RETURNING cluster_id, fleet_id, tenant_id, region_id, environment,
                       owner_name, component, component_version, image_digest,
                       feature_digest, configuration_digest, health, attributes,
                       observed_at",
        )
        .bind(asset.cluster_id.as_uuid())
        .bind(asset.fleet_id.as_uuid())
        .bind(asset.tenant_id.as_uuid())
        .bind(asset.region_id.as_uuid())
        .bind(environment_name(asset.environment))
        .bind(asset.owner.trim())
        .bind(asset.component.trim())
        .bind(asset.component_version.trim())
        .bind(asset.image_digest.as_deref())
        .bind(asset.feature_digest.as_deref())
        .bind(asset.configuration_digest.as_deref())
        .bind(asset.health.trim())
        .bind(attributes)
        .bind(asset.observed_at)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            ControlPlaneError::conflict_code(
                "fleet_asset_stale_or_scope_mismatch",
                "Fleet asset update is stale or outside the registered scope",
            )
        })?;
        asset_from_row(&row)
    }

    pub(in crate::fleet) async fn assets(
        &self,
        tenant_id: TenantId,
        allowed_clusters: &[ClusterId],
        query: &FleetScopeQuery,
    ) -> Result<(Vec<FleetAssetIndex>, u64, BTreeMap<String, u64>, Option<String>), ControlPlaneError> {
        let allowed = cluster_uuids(allowed_clusters);
        let environment = query.environment.map(environment_name);
        let limit = i64::from(bounded_limit(query.limit));
        let offset = i64::from(query.offset);
        let rows = sqlx::query(
            "SELECT cluster_id, fleet_id, tenant_id, region_id, environment,
                    owner_name, component, component_version, image_digest,
                    feature_digest, configuration_digest, health, attributes,
                    observed_at
             FROM fleet_asset_index
             WHERE tenant_id = $1
               AND cluster_id = ANY($2)
               AND ($3::UUID IS NULL OR region_id = $3)
               AND ($4::TEXT IS NULL OR environment = $4)
               AND ($5::TEXT IS NULL OR owner_name = $5)
               AND ($6::TEXT IS NULL OR component_version = $6)
               AND ($7::TEXT IS NULL OR health = $7)
             ORDER BY
                CASE health
                    WHEN 'critical' THEN 0
                    WHEN 'error' THEN 1
                    WHEN 'warning' THEN 2
                    WHEN 'degraded' THEN 3
                    WHEN 'healthy' THEN 4
                    ELSE 5
                END,
                region_id, cluster_id, component
             LIMIT $8 OFFSET $9",
        )
        .bind(tenant_id.as_uuid())
        .bind(&allowed)
        .bind(query.region_id.map(|value| value.as_uuid()))
        .bind(environment)
        .bind(query.owner.as_deref())
        .bind(query.component_version.as_deref())
        .bind(query.health.as_deref())
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        let items = rows.iter().map(asset_from_row).collect::<Result<Vec<_>, _>>()?;
        let total = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM fleet_asset_index
             WHERE tenant_id = $1
               AND cluster_id = ANY($2)
               AND ($3::UUID IS NULL OR region_id = $3)
               AND ($4::TEXT IS NULL OR environment = $4)
               AND ($5::TEXT IS NULL OR owner_name = $5)
               AND ($6::TEXT IS NULL OR component_version = $6)
               AND ($7::TEXT IS NULL OR health = $7)",
        )
        .bind(tenant_id.as_uuid())
        .bind(&allowed)
        .bind(query.region_id.map(|value| value.as_uuid()))
        .bind(environment)
        .bind(query.owner.as_deref())
        .bind(query.component_version.as_deref())
        .bind(query.health.as_deref())
        .fetch_one(&self.pool)
        .await?;
        let health_rows = sqlx::query(
            "SELECT health, COUNT(*)::BIGINT AS asset_count
             FROM fleet_asset_index
             WHERE tenant_id = $1
               AND cluster_id = ANY($2)
               AND ($3::UUID IS NULL OR region_id = $3)
               AND ($4::TEXT IS NULL OR environment = $4)
               AND ($5::TEXT IS NULL OR owner_name = $5)
               AND ($6::TEXT IS NULL OR component_version = $6)
               AND ($7::TEXT IS NULL OR health = $7)
             GROUP BY health
             ORDER BY
                CASE health
                    WHEN 'critical' THEN 0
                    WHEN 'error' THEN 1
                    WHEN 'warning' THEN 2
                    WHEN 'degraded' THEN 3
                    WHEN 'healthy' THEN 4
                    ELSE 5
                END,
                health",
        )
        .bind(tenant_id.as_uuid())
        .bind(&allowed)
        .bind(query.region_id.map(|value| value.as_uuid()))
        .bind(environment)
        .bind(query.owner.as_deref())
        .bind(query.component_version.as_deref())
        .bind(query.health.as_deref())
        .fetch_all(&self.pool)
        .await?;
        let mut health_distribution = BTreeMap::new();
        let mut worst_health = None;
        for row in health_rows {
            let health: String = row.try_get("health")?;
            let count: i64 = row.try_get("asset_count")?;
            let count =
                u64::try_from(count).map_err(|_| ControlPlaneError::configuration("Fleet asset count is invalid"))?;
            if worst_health.is_none() {
                worst_health = Some(health.clone());
            }
            health_distribution.insert(health, count);
        }
        Ok((
            items,
            u64::try_from(total).unwrap_or_default(),
            health_distribution,
            worst_health,
        ))
    }

    pub(in crate::fleet) async fn upsert_finding(
        &self,
        tenant_id: TenantId,
        request: &EvaluateComplianceRequest,
    ) -> Result<ComplianceFinding, ControlPlaneError> {
        let id = ComplianceFindingId::new();
        let evidence_ids = request
            .evidence_ids
            .iter()
            .copied()
            .map(|value| value.as_uuid())
            .collect::<Vec<_>>();
        let row = sqlx::query(
            "INSERT INTO fleet_compliance_findings (
                id, fleet_id, tenant_id, region_id, cluster_id, category,
                expected_digest, live_digest, evidence_ids, severity,
                owner_name, recommendation, finding_state, observed_at
             )
             VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'open', NOW()
             )
             ON CONFLICT (cluster_id, category, expected_digest, live_digest)
             DO UPDATE SET
                evidence_ids = EXCLUDED.evidence_ids,
                severity = EXCLUDED.severity,
                owner_name = EXCLUDED.owner_name,
                recommendation = EXCLUDED.recommendation,
                observed_at = EXCLUDED.observed_at,
                updated_at = NOW()
             WHERE fleet_compliance_findings.tenant_id = EXCLUDED.tenant_id
             RETURNING id, fleet_id, tenant_id, region_id, cluster_id,
                       category, expected_digest, live_digest, evidence_ids,
                       severity, owner_name, recommendation, finding_state,
                       observed_at",
        )
        .bind(id.as_uuid())
        .bind(request.fleet_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(request.region_id.as_uuid())
        .bind(request.cluster_id.as_uuid())
        .bind(request.category.trim())
        .bind(request.expected_digest.trim())
        .bind(request.live_digest.trim())
        .bind(evidence_ids)
        .bind(compliance_severity_name(request.severity))
        .bind(request.owner.trim())
        .bind(request.recommendation.trim())
        .fetch_one(&self.pool)
        .await?;
        finding_from_row(&row)
    }

    pub(in crate::fleet) async fn resolve_matching_findings(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        category: &str,
    ) -> Result<u64, ControlPlaneError> {
        let result = sqlx::query(
            "UPDATE fleet_compliance_findings
             SET finding_state = 'resolved', updated_at = NOW()
             WHERE tenant_id = $1 AND cluster_id = $2 AND category = $3
               AND finding_state IN ('open', 'acknowledged')",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(category)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub(in crate::fleet) async fn findings(
        &self,
        tenant_id: TenantId,
        allowed_clusters: &[ClusterId],
        query: &ComplianceFindingQuery,
    ) -> Result<(Vec<ComplianceFinding>, u64), ControlPlaneError> {
        let allowed = cluster_uuids(allowed_clusters);
        let limit = i64::from(bounded_limit(query.limit));
        let offset = i64::from(query.offset);
        let severity = query.severity.map(compliance_severity_name);
        let state = query.state.map(finding_state_name);
        let rows = sqlx::query(
            "SELECT id, fleet_id, tenant_id, region_id, cluster_id, category,
                    expected_digest, live_digest, evidence_ids, severity,
                    owner_name, recommendation, finding_state, observed_at
             FROM fleet_compliance_findings
             WHERE tenant_id = $1 AND cluster_id = ANY($2)
               AND ($3::UUID IS NULL OR region_id = $3)
               AND ($4::UUID IS NULL OR cluster_id = $4)
               AND ($5::TEXT IS NULL OR severity = $5)
               AND ($6::TEXT IS NULL OR finding_state = $6)
             ORDER BY
                CASE severity
                    WHEN 'critical' THEN 0
                    WHEN 'error' THEN 1
                    WHEN 'warning' THEN 2
                    ELSE 3
                END,
                observed_at DESC
             LIMIT $7 OFFSET $8",
        )
        .bind(tenant_id.as_uuid())
        .bind(&allowed)
        .bind(query.region_id.map(|value| value.as_uuid()))
        .bind(query.cluster_id.map(|value| value.as_uuid()))
        .bind(severity)
        .bind(state)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        let items = rows.iter().map(finding_from_row).collect::<Result<Vec<_>, _>>()?;
        let total = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM fleet_compliance_findings
             WHERE tenant_id = $1 AND cluster_id = ANY($2)
               AND ($3::UUID IS NULL OR region_id = $3)
               AND ($4::UUID IS NULL OR cluster_id = $4)
               AND ($5::TEXT IS NULL OR severity = $5)
               AND ($6::TEXT IS NULL OR finding_state = $6)",
        )
        .bind(tenant_id.as_uuid())
        .bind(&allowed)
        .bind(query.region_id.map(|value| value.as_uuid()))
        .bind(query.cluster_id.map(|value| value.as_uuid()))
        .bind(severity)
        .bind(state)
        .fetch_one(&self.pool)
        .await?;
        Ok((items, u64::try_from(total).unwrap_or_default()))
    }

    pub(in crate::fleet) async fn create_inspection(
        &self,
        tenant_id: TenantId,
        request: &CreateFleetInspectionRequest,
    ) -> Result<FleetInspectionRun, ControlPlaneError> {
        let id = FleetInspectionRunId::new();
        let region_ids = request
            .region_ids
            .iter()
            .copied()
            .map(|value| value.as_uuid())
            .collect::<Vec<_>>();
        let cluster_ids = cluster_uuids(&request.cluster_ids);
        let row = sqlx::query(
            "INSERT INTO fleet_inspection_runs (
                id, fleet_id, tenant_id, region_ids, cluster_ids, pack_ids,
                max_concurrency, timeout_seconds, model_token_budget,
                evidence_byte_budget, inspection_state
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'pending')
             RETURNING id, fleet_id, tenant_id, region_ids, cluster_ids,
                       pack_ids, max_concurrency, timeout_seconds,
                       model_token_budget, evidence_byte_budget,
                       inspection_state, completed_clusters, failed_clusters,
                       created_at, completed_at",
        )
        .bind(id.as_uuid())
        .bind(request.fleet_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(region_ids)
        .bind(cluster_ids)
        .bind(&request.pack_ids)
        .bind(i32::try_from(request.max_concurrency).unwrap_or(i32::MAX))
        .bind(i32::try_from(request.timeout_seconds).unwrap_or(i32::MAX))
        .bind(i64::try_from(request.model_token_budget).unwrap_or(i64::MAX))
        .bind(i64::try_from(request.evidence_byte_budget).unwrap_or(i64::MAX))
        .fetch_one(&self.pool)
        .await?;
        inspection_from_row(&row)
    }

    pub(in crate::fleet) async fn update_inspection(
        &self,
        tenant_id: TenantId,
        id: FleetInspectionRunId,
        request: &UpdateFleetInspectionRequest,
    ) -> Result<FleetInspectionRun, ControlPlaneError> {
        let next_state = if request.terminal {
            if request.failed_clusters == 0 {
                FleetInspectionState::Completed
            } else if request.completed_clusters == 0 {
                FleetInspectionState::Failed
            } else {
                FleetInspectionState::PartiallyCompleted
            }
        } else {
            FleetInspectionState::Running
        };
        let row = sqlx::query(
            "UPDATE fleet_inspection_runs
             SET completed_clusters = $3,
                 failed_clusters = $4,
                 inspection_state = $5,
                 completed_at = CASE WHEN $6 THEN NOW() ELSE NULL END
             WHERE id = $1 AND tenant_id = $2
               AND inspection_state IN ('pending', 'running')
             RETURNING id, fleet_id, tenant_id, region_ids, cluster_ids,
                       pack_ids, max_concurrency, timeout_seconds,
                       model_token_budget, evidence_byte_budget,
                       inspection_state, completed_clusters, failed_clusters,
                       created_at, completed_at",
        )
        .bind(id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(i32::try_from(request.completed_clusters).unwrap_or(i32::MAX))
        .bind(i32::try_from(request.failed_clusters).unwrap_or(i32::MAX))
        .bind(inspection_state_name(next_state))
        .bind(request.terminal)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| ControlPlaneError::conflict_code("inspection_terminal", "Fleet inspection cannot be updated"))?;
        inspection_from_row(&row)
    }

    pub(in crate::fleet) async fn inspections(
        &self,
        tenant_id: TenantId,
        limit: u16,
    ) -> Result<(Vec<FleetInspectionRun>, bool), ControlPlaneError> {
        let requested = i64::from(bounded_limit(limit));
        let rows = sqlx::query(
            "SELECT id, fleet_id, tenant_id, region_ids, cluster_ids,
                    pack_ids, max_concurrency, timeout_seconds,
                    model_token_budget, evidence_byte_budget, inspection_state,
                    completed_clusters, failed_clusters, created_at, completed_at
             FROM fleet_inspection_runs
             WHERE tenant_id = $1
             ORDER BY created_at DESC
             LIMIT $2",
        )
        .bind(tenant_id.as_uuid())
        .bind(requested + 1)
        .fetch_all(&self.pool)
        .await?;
        let truncated = i64::try_from(rows.len()).unwrap_or(i64::MAX) > requested;
        let items = rows
            .iter()
            .take(usize::try_from(requested).unwrap_or(usize::MAX))
            .map(inspection_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((items, truncated))
    }
}

fn inspection_state_name(state: FleetInspectionState) -> &'static str {
    match state {
        FleetInspectionState::Pending => "pending",
        FleetInspectionState::Running => "running",
        FleetInspectionState::Completed => "completed",
        FleetInspectionState::PartiallyCompleted => "partially_completed",
        FleetInspectionState::Failed => "failed",
        FleetInspectionState::Cancelled => "cancelled",
    }
}

fn cluster_uuids(cluster_ids: &[ClusterId]) -> Vec<Uuid> {
    cluster_ids.iter().copied().map(ClusterId::as_uuid).collect()
}
