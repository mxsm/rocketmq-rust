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
use rocketmq_sre_contracts::DrBackupAsset;
use rocketmq_sre_contracts::DrPlan;
use rocketmq_sre_contracts::DrPlanId;
use rocketmq_sre_contracts::TenantId;
use sqlx::Row;

use super::DrRepository;
use super::support::backup_asset_from_row;
use super::support::backup_kind_name;
use super::support::exercise_mode_name;
use super::support::plan_from_row;
use super::support::subject_name;
use crate::ControlPlaneError;
use crate::dr::model::DrPlanQuery;
use crate::dr::model::bounded_limit;

impl DrRepository {
    pub(in crate::dr) async fn scope_exists(
        &self,
        tenant_id: TenantId,
        fleet_id: rocketmq_sre_contracts::FleetId,
        region_id: rocketmq_sre_contracts::RegionId,
        cluster_id: Option<ClusterId>,
    ) -> Result<bool, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT EXISTS (
                SELECT 1
                FROM fleet_tenants tenant
                JOIN fleet_regions region
                  ON region.fleet_id = tenant.fleet_id
                WHERE tenant.id = $1
                  AND tenant.fleet_id = $2
                  AND tenant.active
                  AND region.id = $3
                  AND region.active
                  AND (
                      $4::UUID IS NULL
                      OR EXISTS (
                          SELECT 1
                          FROM fleet_cluster_registrations registration
                          WHERE registration.cluster_id = $4
                            AND registration.fleet_id = tenant.fleet_id
                            AND registration.tenant_id = tenant.id
                            AND registration.region_id = region.id
                            AND registration.lifecycle_state IN ('active', 'read_only_degraded')
                      )
                  )
             ) AS present",
        )
        .bind(tenant_id.as_uuid())
        .bind(fleet_id.as_uuid())
        .bind(region_id.as_uuid())
        .bind(cluster_id.map(ClusterId::as_uuid))
        .fetch_one(&self.pool)
        .await?;
        Ok(row.try_get("present")?)
    }

    pub(in crate::dr) async fn create_plan(&self, plan: &DrPlan) -> Result<DrPlan, ControlPlaneError> {
        let checkpoint_definitions = serde_json::to_value(&plan.checkpoints)
            .map_err(|_| ControlPlaneError::validation("invalid_dr_plan", "checkpoint definitions are invalid"))?;
        let allowed_modes = plan
            .allowed_modes
            .iter()
            .copied()
            .map(exercise_mode_name)
            .collect::<Vec<_>>();
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "UPDATE dr_plans
             SET active = FALSE, updated_at = $3
             WHERE tenant_id = $1 AND name = $2 AND active",
        )
        .bind(plan.tenant_id.as_uuid())
        .bind(&plan.name)
        .bind(plan.updated_at)
        .execute(&mut *transaction)
        .await?;
        let row = sqlx::query(
            "INSERT INTO dr_plans (
                id, fleet_id, tenant_id, region_id, cluster_id, subject, name,
                plan_version, owner_name, rto_seconds, rpo_seconds, allowed_modes,
                required_sources, checkpoint_definitions, active, created_at, updated_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                $13, $14, TRUE, $15, $15
             )
             RETURNING *",
        )
        .bind(plan.id.as_uuid())
        .bind(plan.fleet_id.as_uuid())
        .bind(plan.tenant_id.as_uuid())
        .bind(plan.region_id.as_uuid())
        .bind(plan.cluster_id.map(ClusterId::as_uuid))
        .bind(subject_name(plan.subject))
        .bind(&plan.name)
        .bind(i32::try_from(plan.version).map_err(|_| {
            ControlPlaneError::validation("invalid_dr_plan", "plan version exceeds the supported range")
        })?)
        .bind(&plan.owner)
        .bind(
            i64::try_from(plan.target.rto_seconds)
                .map_err(|_| ControlPlaneError::validation("invalid_dr_plan", "RTO exceeds the supported range"))?,
        )
        .bind(
            i64::try_from(plan.target.rpo_seconds)
                .map_err(|_| ControlPlaneError::validation("invalid_dr_plan", "RPO exceeds the supported range"))?,
        )
        .bind(allowed_modes)
        .bind(&plan.required_sources)
        .bind(checkpoint_definitions)
        .bind(plan.created_at)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        plan_from_row(&row)
    }

    pub(in crate::dr) async fn get_plan(&self, tenant_id: TenantId, id: DrPlanId) -> Result<DrPlan, ControlPlaneError> {
        let row = sqlx::query("SELECT * FROM dr_plans WHERE tenant_id = $1 AND id = $2")
            .bind(tenant_id.as_uuid())
            .bind(id.as_uuid())
            .fetch_optional(&self.pool)
            .await?
            .ok_or(ControlPlaneError::NotFound)?;
        plan_from_row(&row)
    }

    pub(in crate::dr) async fn list_plans(
        &self,
        tenant_id: TenantId,
        query: &DrPlanQuery,
    ) -> Result<(Vec<DrPlan>, bool), ControlPlaneError> {
        let subject = query.subject.map(subject_name);
        let limit = bounded_limit(query.limit);
        let rows = sqlx::query(
            "SELECT *
             FROM dr_plans
             WHERE tenant_id = $1
               AND ($2::UUID IS NULL OR cluster_id = $2)
               AND ($3::TEXT IS NULL OR subject = $3)
               AND ($4::BOOLEAN IS NULL OR active = $4)
             ORDER BY updated_at DESC, id
             LIMIT $5",
        )
        .bind(tenant_id.as_uuid())
        .bind(query.cluster_id.map(ClusterId::as_uuid))
        .bind(subject)
        .bind(query.active)
        .bind(limit + 1)
        .fetch_all(&self.pool)
        .await?;
        let truncated = i64::try_from(rows.len()).unwrap_or(i64::MAX) > limit;
        rows.into_iter()
            .take(usize::try_from(limit).unwrap_or(200))
            .map(|row| plan_from_row(&row))
            .collect::<Result<Vec<_>, _>>()
            .map(|items| (items, truncated))
    }

    pub(in crate::dr) async fn upsert_backup_asset(
        &self,
        tenant_id: TenantId,
        asset: &DrBackupAsset,
    ) -> Result<DrBackupAsset, ControlPlaneError> {
        let row = sqlx::query(
            "INSERT INTO dr_backup_assets (
                id, plan_id, asset_kind, owner_name, access_owner,
                backup_locator_digest, encrypted, last_backup_at,
                restore_verified_at, evidence_ids, updated_at
             )
             SELECT $1, plan.id, $3, $4, $5, $6, $7, $8, $9, $10, $11
             FROM dr_plans plan
             WHERE plan.id = $2 AND plan.tenant_id = $12
             ON CONFLICT (plan_id, asset_kind) DO UPDATE SET
                owner_name = EXCLUDED.owner_name,
                access_owner = EXCLUDED.access_owner,
                backup_locator_digest = EXCLUDED.backup_locator_digest,
                encrypted = EXCLUDED.encrypted,
                last_backup_at = EXCLUDED.last_backup_at,
                restore_verified_at = EXCLUDED.restore_verified_at,
                evidence_ids = EXCLUDED.evidence_ids,
                updated_at = EXCLUDED.updated_at
             RETURNING *",
        )
        .bind(asset.id.as_uuid())
        .bind(asset.plan_id.as_uuid())
        .bind(backup_kind_name(asset.kind))
        .bind(&asset.owner)
        .bind(&asset.access_owner)
        .bind(&asset.backup_locator_digest)
        .bind(asset.encrypted)
        .bind(asset.last_backup_at)
        .bind(asset.restore_verified_at)
        .bind(asset.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
        .bind(asset.updated_at)
        .bind(tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        backup_asset_from_row(&row)
    }

    pub(in crate::dr) async fn list_backup_assets(
        &self,
        tenant_id: TenantId,
        plan_id: DrPlanId,
    ) -> Result<Vec<DrBackupAsset>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT asset.*
             FROM dr_backup_assets asset
             JOIN dr_plans plan ON plan.id = asset.plan_id
             WHERE plan.tenant_id = $1 AND asset.plan_id = $2
             ORDER BY asset.asset_kind",
        )
        .bind(tenant_id.as_uuid())
        .bind(plan_id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(|row| backup_asset_from_row(&row)).collect()
    }

    pub(in crate::dr) async fn cluster_environment(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
    ) -> Result<String, ControlPlaneError> {
        sqlx::query(
            "SELECT environment
             FROM fleet_cluster_registrations
             WHERE tenant_id = $1 AND cluster_id = $2
               AND lifecycle_state IN ('active', 'read_only_degraded')",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .map(|row| row.try_get("environment"))
        .transpose()?
        .ok_or(ControlPlaneError::NotFound)
    }
}
