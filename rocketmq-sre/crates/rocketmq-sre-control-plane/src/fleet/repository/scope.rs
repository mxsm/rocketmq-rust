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
use rocketmq_sre_contracts::ClusterRegistration;
use rocketmq_sre_contracts::Fleet;
use rocketmq_sre_contracts::FleetRegion;
use rocketmq_sre_contracts::FleetTenant;
use rocketmq_sre_contracts::QuotaPolicy;
use rocketmq_sre_contracts::QuotaPolicyId;
use rocketmq_sre_contracts::QuotaUsage;
use rocketmq_sre_contracts::TenantId;
use uuid::Uuid;

use super::FleetRepository;
use super::support::environment_name;
use super::support::fleet_from_row;
use super::support::quota_policy_from_row;
use super::support::quota_usage_from_row;
use super::support::region_from_row;
use super::support::registration_from_row;
use super::support::tenant_from_row;
use crate::ControlPlaneError;
use crate::fleet::model::CreateQuotaPolicyRequest;
use crate::fleet::model::FleetScopeQuery;
use crate::fleet::model::bounded_limit;

impl FleetRepository {
    pub(in crate::fleet) async fn cluster_registration(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
    ) -> Result<ClusterRegistration, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT registration.cluster_id, registration.fleet_id,
                    registration.tenant_id, registration.region_id,
                    cluster.external_cluster_key, registration.environment,
                    registration.owner_name, registration.lifecycle_state,
                    registration.residency_tags, registration.lifecycle_revision,
                    registration.created_at, registration.updated_at
             FROM fleet_cluster_registrations registration
             JOIN clusters cluster ON cluster.id = registration.cluster_id
             WHERE registration.tenant_id = $1
               AND registration.cluster_id = $2",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        registration_from_row(&row)
    }

    pub(in crate::fleet) async fn tenant_scope(
        &self,
        tenant_id: TenantId,
        allowed_clusters: &[ClusterId],
    ) -> Result<(Fleet, FleetTenant, Vec<FleetRegion>), ControlPlaneError> {
        let tenant_row = sqlx::query(
            "SELECT tenant.id, tenant.fleet_id, tenant.name, tenant.owner_name,
                    tenant.active, tenant.created_at, tenant.updated_at
             FROM fleet_tenants tenant
             WHERE tenant.id = $1 AND tenant.active = TRUE",
        )
        .bind(tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let tenant = tenant_from_row(&tenant_row)?;
        let fleet_row = sqlx::query(
            "SELECT id, name, owner_name, created_at, updated_at
             FROM fleets
             WHERE id = $1",
        )
        .bind(tenant.fleet_id.as_uuid())
        .fetch_one(&self.pool)
        .await?;
        let fleet = fleet_from_row(&fleet_row)?;
        let allowed = cluster_uuids(allowed_clusters);
        let region_rows = sqlx::query(
            "SELECT DISTINCT region.id, region.fleet_id, region.region_key,
                    region.display_name, region.owner_name, region.residency_tags,
                    region.active, region.created_at, region.updated_at
             FROM fleet_regions region
             JOIN fleet_cluster_registrations registration
               ON registration.region_id = region.id
             WHERE registration.tenant_id = $1
               AND registration.cluster_id = ANY($2)
               AND region.active = TRUE
             ORDER BY region.region_key",
        )
        .bind(tenant_id.as_uuid())
        .bind(&allowed)
        .fetch_all(&self.pool)
        .await?;
        let regions = region_rows.iter().map(region_from_row).collect::<Result<Vec<_>, _>>()?;
        Ok((fleet, tenant, regions))
    }

    pub(in crate::fleet) async fn cluster_registrations(
        &self,
        tenant_id: TenantId,
        allowed_clusters: &[ClusterId],
        query: &FleetScopeQuery,
    ) -> Result<(Vec<ClusterRegistration>, u64), ControlPlaneError> {
        let allowed = cluster_uuids(allowed_clusters);
        let environment = query.environment.map(environment_name);
        let limit = i64::from(bounded_limit(query.limit));
        let offset = i64::from(query.offset);
        let rows = sqlx::query(
            "SELECT registration.cluster_id, registration.fleet_id,
                    registration.tenant_id, registration.region_id,
                    cluster.external_cluster_key, registration.environment,
                    registration.owner_name, registration.lifecycle_state,
                    registration.residency_tags, registration.lifecycle_revision,
                    registration.created_at, registration.updated_at
             FROM fleet_cluster_registrations registration
             JOIN clusters cluster ON cluster.id = registration.cluster_id
             WHERE registration.tenant_id = $1
               AND registration.cluster_id = ANY($2)
               AND ($3::UUID IS NULL OR registration.region_id = $3)
               AND ($4::TEXT IS NULL OR registration.environment = $4)
               AND ($5::TEXT IS NULL OR registration.owner_name = $5)
             ORDER BY registration.region_id, cluster.external_cluster_key
             LIMIT $6 OFFSET $7",
        )
        .bind(tenant_id.as_uuid())
        .bind(&allowed)
        .bind(query.region_id.map(|value| value.as_uuid()))
        .bind(environment)
        .bind(query.owner.as_deref())
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        let items = rows.iter().map(registration_from_row).collect::<Result<Vec<_>, _>>()?;
        let total = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM fleet_cluster_registrations registration
             WHERE registration.tenant_id = $1
               AND registration.cluster_id = ANY($2)
               AND ($3::UUID IS NULL OR registration.region_id = $3)
               AND ($4::TEXT IS NULL OR registration.environment = $4)
               AND ($5::TEXT IS NULL OR registration.owner_name = $5)",
        )
        .bind(tenant_id.as_uuid())
        .bind(&allowed)
        .bind(query.region_id.map(|value| value.as_uuid()))
        .bind(environment)
        .bind(query.owner.as_deref())
        .fetch_one(&self.pool)
        .await?;
        Ok((items, u64::try_from(total).unwrap_or_default()))
    }

    pub(in crate::fleet) async fn create_quota_policy(
        &self,
        tenant_id: TenantId,
        request: &CreateQuotaPolicyRequest,
    ) -> Result<QuotaPolicy, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let version = sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(MAX(policy_version), 0) + 1
             FROM fleet_quota_policies
             WHERE fleet_id = $1 AND tenant_id = $2
               AND region_id IS NOT DISTINCT FROM $3
               AND cluster_id IS NOT DISTINCT FROM $4",
        )
        .bind(request.fleet_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(request.region_id.map(|value| value.as_uuid()))
        .bind(request.cluster_id.map(|value| value.as_uuid()))
        .fetch_one(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE fleet_quota_policies
             SET active = FALSE
             WHERE fleet_id = $1 AND tenant_id = $2
               AND region_id IS NOT DISTINCT FROM $3
               AND cluster_id IS NOT DISTINCT FROM $4
               AND active = TRUE",
        )
        .bind(request.fleet_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(request.region_id.map(|value| value.as_uuid()))
        .bind(request.cluster_id.map(|value| value.as_uuid()))
        .execute(&mut *transaction)
        .await?;
        let id = QuotaPolicyId::new();
        let row = sqlx::query(
            "INSERT INTO fleet_quota_policies (
                id, fleet_id, tenant_id, region_id, cluster_id, policy_version,
                queries_per_minute, model_tokens_per_hour, concurrent_workflows,
                concurrent_inspections, evidence_bytes_per_hour,
                notifications_per_hour, automatic_actions_per_hour,
                owner_name, active
             )
             VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11, $12, $13, $14, TRUE
             )
             RETURNING id, fleet_id, tenant_id, region_id, cluster_id,
                       policy_version, queries_per_minute, model_tokens_per_hour,
                       concurrent_workflows, concurrent_inspections,
                       evidence_bytes_per_hour, notifications_per_hour,
                       automatic_actions_per_hour, owner_name, active, created_at",
        )
        .bind(id.as_uuid())
        .bind(request.fleet_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(request.region_id.map(|value| value.as_uuid()))
        .bind(request.cluster_id.map(|value| value.as_uuid()))
        .bind(version)
        .bind(i32::try_from(request.limits.queries_per_minute).unwrap_or(i32::MAX))
        .bind(i64::try_from(request.limits.model_tokens_per_hour).unwrap_or(i64::MAX))
        .bind(i32::try_from(request.limits.concurrent_workflows).unwrap_or(i32::MAX))
        .bind(i32::try_from(request.limits.concurrent_inspections).unwrap_or(i32::MAX))
        .bind(i64::try_from(request.limits.evidence_bytes_per_hour).unwrap_or(i64::MAX))
        .bind(i32::try_from(request.limits.notifications_per_hour).unwrap_or(i32::MAX))
        .bind(i32::try_from(request.limits.automatic_actions_per_hour).unwrap_or(i32::MAX))
        .bind(request.owner.trim())
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        quota_policy_from_row(&row)
    }

    pub(in crate::fleet) async fn quota_policy(
        &self,
        tenant_id: TenantId,
        cluster_id: Option<ClusterId>,
    ) -> Result<(QuotaPolicy, QuotaUsage), ControlPlaneError> {
        let row = sqlx::query(
            "SELECT id, fleet_id, tenant_id, region_id, cluster_id,
                    policy_version, queries_per_minute, model_tokens_per_hour,
                    concurrent_workflows, concurrent_inspections,
                    evidence_bytes_per_hour, notifications_per_hour,
                    automatic_actions_per_hour, owner_name, active, created_at
             FROM fleet_quota_policies
             WHERE tenant_id = $1 AND active = TRUE
               AND (cluster_id = $2 OR cluster_id IS NULL)
             ORDER BY (cluster_id IS NOT NULL) DESC, (region_id IS NOT NULL) DESC,
                      policy_version DESC
             LIMIT 1",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.map(|value| value.as_uuid()))
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let policy = quota_policy_from_row(&row)?;
        let usage_row = sqlx::query(
            "SELECT
                COALESCE(SUM(amount) FILTER (
                    WHERE resource_kind = 'query'
                      AND occurred_at >= NOW() - INTERVAL '1 minute'
                ), 0)::BIGINT AS queries,
                COALESCE(SUM(amount) FILTER (
                    WHERE resource_kind = 'model_token'
                      AND occurred_at >= NOW() - INTERVAL '1 hour'
                ), 0)::BIGINT AS model_tokens,
                (
                    SELECT COUNT(*)::INTEGER FROM executions
                    WHERE tenant_id = $2
                      AND ($3::UUID IS NULL OR cluster_id = $3)
                      AND state NOT IN ('succeeded', 'rolled_back', 'escalated')
                ) AS active_workflows,
                (
                    SELECT COUNT(*)::INTEGER FROM fleet_inspection_runs
                    WHERE tenant_id = $2
                      AND inspection_state IN ('pending', 'running')
                ) AS active_inspections,
                COALESCE(SUM(amount) FILTER (
                    WHERE resource_kind = 'evidence_byte'
                      AND occurred_at >= NOW() - INTERVAL '1 hour'
                ), 0)::BIGINT AS evidence_bytes,
                COALESCE(SUM(amount) FILTER (
                    WHERE resource_kind = 'notification'
                      AND occurred_at >= NOW() - INTERVAL '1 hour'
                ), 0)::BIGINT AS notifications,
                COALESCE(SUM(amount) FILTER (
                    WHERE resource_kind = 'automatic_action'
                      AND occurred_at >= NOW() - INTERVAL '1 hour'
                ), 0)::BIGINT AS automatic_actions,
                NOW() AS observed_at
             FROM fleet_quota_usage_events
             WHERE quota_policy_id = $1",
        )
        .bind(policy.id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.map(|value| value.as_uuid()))
        .fetch_one(&self.pool)
        .await?;
        let usage = quota_usage_from_row(policy.id, &usage_row)?;
        Ok((policy, usage))
    }

    pub(in crate::fleet) async fn record_quota_usage(
        &self,
        policy: &QuotaPolicy,
        resource_kind: &str,
        amount: u64,
    ) -> Result<(), ControlPlaneError> {
        sqlx::query(
            "INSERT INTO fleet_quota_usage_events (
                event_id, quota_policy_id, tenant_id, region_id, cluster_id,
                resource_kind, amount, occurred_at
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(Uuid::new_v4())
        .bind(policy.id.as_uuid())
        .bind(policy.tenant_id.as_uuid())
        .bind(policy.region_id.map(|value| value.as_uuid()))
        .bind(policy.cluster_id.map(|value| value.as_uuid()))
        .bind(resource_kind)
        .bind(i64::try_from(amount).unwrap_or(i64::MAX))
        .bind(Utc::now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

fn cluster_uuids(cluster_ids: &[ClusterId]) -> Vec<Uuid> {
    cluster_ids.iter().copied().map(ClusterId::as_uuid).collect()
}
