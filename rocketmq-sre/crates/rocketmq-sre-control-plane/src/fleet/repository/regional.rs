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
use rocketmq_sre_contracts::RegionalEndpoint;
use rocketmq_sre_contracts::TenantId;
use uuid::Uuid;

use super::FleetRepository;
use super::support::endpoint_from_row;
use super::support::endpoint_health_name;
use super::support::endpoint_kind_name;
use crate::ControlPlaneError;
use crate::fleet::model::RegionalEndpointQuery;
use crate::fleet::model::bounded_limit;

impl FleetRepository {
    pub(in crate::fleet) async fn upsert_regional_endpoint(
        &self,
        endpoint: &RegionalEndpoint,
    ) -> Result<RegionalEndpoint, ControlPlaneError> {
        let capabilities = serde_json::to_value(&endpoint.capabilities).map_err(|_| {
            ControlPlaneError::validation("invalid_request", "regional endpoint capabilities are invalid")
        })?;
        let residency_tags = serde_json::to_value(&endpoint.residency_tags).map_err(|_| {
            ControlPlaneError::validation("invalid_request", "regional endpoint residency tags are invalid")
        })?;
        let row = sqlx::query(
            "INSERT INTO regional_endpoints (
                id, fleet_id, tenant_id, region_id, cluster_id, endpoint_kind,
                component_version, protocol_version, schema_digest,
                capabilities, residency_tags, capacity, health,
                last_heartbeat_at, updated_at
             )
             VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11, $12, $13, $14, NOW()
             )
             ON CONFLICT (id) DO UPDATE SET
                fleet_id = EXCLUDED.fleet_id,
                tenant_id = EXCLUDED.tenant_id,
                region_id = EXCLUDED.region_id,
                cluster_id = EXCLUDED.cluster_id,
                endpoint_kind = EXCLUDED.endpoint_kind,
                component_version = EXCLUDED.component_version,
                protocol_version = EXCLUDED.protocol_version,
                schema_digest = EXCLUDED.schema_digest,
                capabilities = EXCLUDED.capabilities,
                residency_tags = EXCLUDED.residency_tags,
                capacity = EXCLUDED.capacity,
                health = EXCLUDED.health,
                last_heartbeat_at = EXCLUDED.last_heartbeat_at,
                updated_at = NOW()
             WHERE regional_endpoints.tenant_id = EXCLUDED.tenant_id
               AND regional_endpoints.region_id = EXCLUDED.region_id
             RETURNING id, fleet_id, tenant_id, region_id, cluster_id,
                       endpoint_kind, component_version, protocol_version,
                       schema_digest, capabilities, residency_tags, capacity,
                       health, last_heartbeat_at",
        )
        .bind(endpoint.id.trim())
        .bind(endpoint.fleet_id.as_uuid())
        .bind(endpoint.tenant_id.as_uuid())
        .bind(endpoint.region_id.as_uuid())
        .bind(endpoint.cluster_id.map(|value| value.as_uuid()))
        .bind(endpoint_kind_name(endpoint.kind))
        .bind(endpoint.component_version.trim())
        .bind(endpoint.protocol_version.trim())
        .bind(endpoint.schema_digest.trim())
        .bind(capabilities)
        .bind(residency_tags)
        .bind(i32::try_from(endpoint.capacity).unwrap_or(i32::MAX))
        .bind(endpoint_health_name(endpoint.health))
        .bind(endpoint.last_heartbeat_at)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            ControlPlaneError::forbidden(
                "regional_endpoint_scope_mismatch",
                "regional endpoint identity cannot move across tenant or region",
            )
        })?;
        endpoint_from_row(&row)
    }

    pub(in crate::fleet) async fn regional_endpoints(
        &self,
        tenant_id: TenantId,
        allowed_clusters: &[ClusterId],
        query: &RegionalEndpointQuery,
    ) -> Result<(Vec<RegionalEndpoint>, bool), ControlPlaneError> {
        let allowed = cluster_uuids(allowed_clusters);
        let requested = i64::from(bounded_limit(query.limit));
        let rows = sqlx::query(
            "SELECT id, fleet_id, tenant_id, region_id, cluster_id,
                    endpoint_kind, component_version, protocol_version,
                    schema_digest, capabilities, residency_tags, capacity,
                    health, last_heartbeat_at
             FROM regional_endpoints
             WHERE tenant_id = $1
               AND ($2::UUID IS NULL OR region_id = $2)
               AND ($3::UUID IS NULL OR cluster_id = $3 OR cluster_id IS NULL)
               AND ($4::TEXT IS NULL OR endpoint_kind = $4)
               AND (cluster_id IS NULL OR cluster_id = ANY($5))
             ORDER BY region_id, endpoint_kind, health, last_heartbeat_at DESC
             LIMIT $6",
        )
        .bind(tenant_id.as_uuid())
        .bind(query.region_id.map(|value| value.as_uuid()))
        .bind(query.cluster_id.map(|value| value.as_uuid()))
        .bind(query.kind.map(endpoint_kind_name))
        .bind(&allowed)
        .bind(requested + 1)
        .fetch_all(&self.pool)
        .await?;
        let truncated = i64::try_from(rows.len()).unwrap_or(i64::MAX) > requested;
        let items = rows
            .iter()
            .take(usize::try_from(requested).unwrap_or(usize::MAX))
            .map(endpoint_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((items, truncated))
    }
}

fn cluster_uuids(cluster_ids: &[ClusterId]) -> Vec<Uuid> {
    cluster_ids.iter().copied().map(ClusterId::as_uuid).collect()
}
