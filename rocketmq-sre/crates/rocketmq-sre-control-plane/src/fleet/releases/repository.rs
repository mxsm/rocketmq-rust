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

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::FleetId;
use rocketmq_sre_contracts::FleetRelease;
use rocketmq_sre_contracts::FleetReleaseBatch;
use rocketmq_sre_contracts::FleetReleaseId;
use rocketmq_sre_contracts::FleetReleaseStatus;
use rocketmq_sre_contracts::FleetReleaseTarget;
use rocketmq_sre_contracts::FleetReleaseTargetState;
use rocketmq_sre_contracts::RegionId;
use rocketmq_sre_contracts::ReleaseId;
use rocketmq_sre_contracts::ReleaseStatus;
use rocketmq_sre_contracts::TenantId;
use sqlx::PgPool;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use uuid::Uuid;

use super::model::FleetReleaseQuery;
use super::model::FleetReleaseTransition;
use super::model::FleetReleaseView;
use super::model::bounded_limit;
use crate::ControlPlaneError;
use crate::fleet::repository::FleetRepository;

impl FleetRepository {
    pub(in crate::fleet) async fn create_fleet_release(
        &self,
        release: &FleetRelease,
        targets: &[FleetReleaseTarget],
        actor_subject: &str,
    ) -> Result<(), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO fleet_releases (
                id, fleet_id, tenant_id, correlation_id, release_ref,
                artifact_digest, target_version, owner_name,
                maintenance_window_start, maintenance_window_end,
                rollback_artifact_digest, slo_policy_id, release_status,
                active_batch, created_at, updated_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14, $15, $16
             )",
        )
        .bind(release.id.as_uuid())
        .bind(release.fleet_id.as_uuid())
        .bind(release.tenant_id.as_uuid())
        .bind(release.correlation_id.as_uuid())
        .bind(&release.release_ref)
        .bind(&release.artifact_digest)
        .bind(&release.target_version)
        .bind(&release.owner)
        .bind(release.maintenance_window_start)
        .bind(release.maintenance_window_end)
        .bind(&release.rollback_artifact_digest)
        .bind(&release.slo_policy_id)
        .bind(release_status_name(release.status))
        .bind(release.active_batch.map(i64::from))
        .bind(release.created_at)
        .bind(release.updated_at)
        .execute(&mut *transaction)
        .await?;
        for batch in &release.batches {
            insert_batch(&mut transaction, release.id, batch).await?;
        }
        for target in targets {
            insert_target(&mut transaction, target).await?;
        }
        insert_event(
            &mut transaction,
            release.id,
            release.tenant_id,
            None,
            None,
            release.status,
            None,
            None,
            "fleet_release_created",
            actor_subject,
            serde_json::json!({
                "batch_count": release.batches.len(),
                "target_count": targets.len(),
            }),
            release.created_at,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(in crate::fleet) async fn fleet_release(
        &self,
        tenant_id: TenantId,
        id: FleetReleaseId,
        allowed_clusters: &[ClusterId],
    ) -> Result<FleetReleaseView, ControlPlaneError> {
        let allowed = cluster_uuids(allowed_clusters);
        let row = sqlx::query(
            "SELECT release.*
             FROM fleet_releases release
             WHERE release.tenant_id = $1
               AND release.id = $2
               AND NOT EXISTS (
                    SELECT 1
                    FROM fleet_release_targets target
                    WHERE target.fleet_release_id = release.id
                      AND NOT (target.cluster_id = ANY($3))
               )",
        )
        .bind(tenant_id.as_uuid())
        .bind(id.as_uuid())
        .bind(&allowed)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let release = release_from_row(&self.pool, &row).await?;
        let targets = self.fleet_release_targets(tenant_id, id).await?;
        Ok(FleetReleaseView {
            schema_version: super::model::FLEET_RELEASE_API_SCHEMA_VERSION,
            release,
            targets,
        })
    }

    pub(in crate::fleet) async fn fleet_releases(
        &self,
        tenant_id: TenantId,
        allowed_clusters: &[ClusterId],
        query: &FleetReleaseQuery,
    ) -> Result<(Vec<FleetRelease>, u64), ControlPlaneError> {
        let allowed = cluster_uuids(allowed_clusters);
        let limit = i64::from(bounded_limit(query.limit));
        let offset = i64::from(query.offset);
        let rows = sqlx::query(
            "SELECT release.*
             FROM fleet_releases release
             WHERE release.tenant_id = $1
               AND ($2::TEXT IS NULL OR release.release_status = $2)
               AND NOT EXISTS (
                    SELECT 1
                    FROM fleet_release_targets target
                    WHERE target.fleet_release_id = release.id
                      AND NOT (target.cluster_id = ANY($3))
               )
             ORDER BY release.updated_at DESC, release.id
             LIMIT $4 OFFSET $5",
        )
        .bind(tenant_id.as_uuid())
        .bind(query.status.map(release_status_name))
        .bind(&allowed)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in &rows {
            items.push(release_from_row(&self.pool, row).await?);
        }
        let total = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM fleet_releases release
             WHERE release.tenant_id = $1
               AND ($2::TEXT IS NULL OR release.release_status = $2)
               AND NOT EXISTS (
                    SELECT 1
                    FROM fleet_release_targets target
                    WHERE target.fleet_release_id = release.id
                      AND NOT (target.cluster_id = ANY($3))
               )",
        )
        .bind(tenant_id.as_uuid())
        .bind(query.status.map(release_status_name))
        .bind(&allowed)
        .fetch_one(&self.pool)
        .await?;
        let total =
            u64::try_from(total).map_err(|_| ControlPlaneError::configuration("Fleet release count is invalid"))?;
        Ok((items, total))
    }

    pub(in crate::fleet) async fn linked_release_status(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        release_id: ReleaseId,
    ) -> Result<ReleaseStatus, ControlPlaneError> {
        let status = sqlx::query_scalar::<_, String>(
            "SELECT status
             FROM release_workflows
             WHERE tenant_id = $1 AND cluster_id = $2 AND id = $3",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(release_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        parse_linked_release_status(&status)
    }

    pub(in crate::fleet) async fn apply_fleet_release_transition(
        &self,
        previous: &FleetReleaseView,
        transition: &FleetReleaseTransition,
    ) -> Result<(), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let updated = sqlx::query(
            "UPDATE fleet_releases
             SET release_status = $4, active_batch = $5, updated_at = $6
             WHERE id = $1 AND tenant_id = $2
               AND release_status = $3 AND updated_at = $7",
        )
        .bind(previous.release.id.as_uuid())
        .bind(previous.release.tenant_id.as_uuid())
        .bind(release_status_name(previous.release.status))
        .bind(release_status_name(transition.release.status))
        .bind(transition.release.active_batch.map(i64::from))
        .bind(transition.release.updated_at)
        .bind(previous.release.updated_at)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if updated != 1 {
            return Err(ControlPlaneError::conflict_code(
                "fleet_release_state_changed",
                "Fleet release was changed by another operator",
            ));
        }

        for next in &transition.targets {
            let current = previous
                .targets
                .iter()
                .find(|target| target.cluster_id == next.cluster_id)
                .ok_or_else(|| {
                    ControlPlaneError::configuration("Fleet release transition contains an unknown target")
                })?;
            if current == next {
                continue;
            }
            update_target(&mut transaction, current, next).await?;
            insert_event(
                &mut transaction,
                transition.release.id,
                transition.release.tenant_id,
                Some(next.cluster_id),
                Some(previous.release.status),
                transition.release.status,
                Some(current.state),
                Some(next.state),
                transition.reason_code,
                &transition.actor_subject,
                transition.details.clone(),
                transition.release.updated_at,
            )
            .await?;
        }
        if previous.release.status != transition.release.status
            || previous.release.active_batch != transition.release.active_batch
        {
            insert_event(
                &mut transaction,
                transition.release.id,
                transition.release.tenant_id,
                None,
                Some(previous.release.status),
                transition.release.status,
                None,
                None,
                transition.reason_code,
                &transition.actor_subject,
                transition.details.clone(),
                transition.release.updated_at,
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn fleet_release_targets(
        &self,
        tenant_id: TenantId,
        id: FleetReleaseId,
    ) -> Result<Vec<FleetReleaseTarget>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT *
             FROM fleet_release_targets
             WHERE tenant_id = $1 AND fleet_release_id = $2
             ORDER BY batch_sequence, canary DESC, cluster_id",
        )
        .bind(tenant_id.as_uuid())
        .bind(id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(target_from_row).collect()
    }
}

async fn insert_batch(
    transaction: &mut Transaction<'_, Postgres>,
    release_id: FleetReleaseId,
    batch: &FleetReleaseBatch,
) -> Result<(), ControlPlaneError> {
    let clusters = batch.cluster_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>();
    sqlx::query(
        "INSERT INTO fleet_release_batches (
            fleet_release_id, batch_sequence, region_id,
            cluster_ids, max_concurrency, canary
         ) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(release_id.as_uuid())
    .bind(i64::from(batch.sequence))
    .bind(batch.region_id.as_uuid())
    .bind(clusters)
    .bind(i64::from(batch.max_concurrency))
    .bind(batch.canary)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_target(
    transaction: &mut Transaction<'_, Postgres>,
    target: &FleetReleaseTarget,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO fleet_release_targets (
            fleet_release_id, tenant_id, cluster_id, region_id,
            batch_sequence, canary, target_state, release_id,
            readiness_reason_codes, regression_detected,
            sanitized_outcome, updated_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
    )
    .bind(target.fleet_release_id.as_uuid())
    .bind(target.tenant_id.as_uuid())
    .bind(target.cluster_id.as_uuid())
    .bind(target.region_id.as_uuid())
    .bind(i64::from(target.batch_sequence))
    .bind(target.canary)
    .bind(target_state_name(target.state))
    .bind(target.release_id.map(ReleaseId::as_uuid))
    .bind(&target.readiness_reason_codes)
    .bind(target.regression_detected)
    .bind(target.sanitized_outcome.as_deref())
    .bind(target.updated_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn update_target(
    transaction: &mut Transaction<'_, Postgres>,
    current: &FleetReleaseTarget,
    next: &FleetReleaseTarget,
) -> Result<(), ControlPlaneError> {
    let updated = sqlx::query(
        "UPDATE fleet_release_targets
         SET target_state = $5, release_id = $6,
             readiness_reason_codes = $7, regression_detected = $8,
             sanitized_outcome = $9, updated_at = $10
         WHERE fleet_release_id = $1 AND tenant_id = $2 AND cluster_id = $3
           AND target_state = $4 AND updated_at = $11",
    )
    .bind(current.fleet_release_id.as_uuid())
    .bind(current.tenant_id.as_uuid())
    .bind(current.cluster_id.as_uuid())
    .bind(target_state_name(current.state))
    .bind(target_state_name(next.state))
    .bind(next.release_id.map(ReleaseId::as_uuid))
    .bind(&next.readiness_reason_codes)
    .bind(next.regression_detected)
    .bind(next.sanitized_outcome.as_deref())
    .bind(next.updated_at)
    .bind(current.updated_at)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if updated != 1 {
        return Err(ControlPlaneError::conflict_code(
            "fleet_release_target_changed",
            "Fleet release target was changed by another operator",
        ));
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "append-only event fields mirror the durable audit record"
)]
async fn insert_event(
    transaction: &mut Transaction<'_, Postgres>,
    release_id: FleetReleaseId,
    tenant_id: TenantId,
    cluster_id: Option<ClusterId>,
    from_release_status: Option<FleetReleaseStatus>,
    to_release_status: FleetReleaseStatus,
    from_target_state: Option<FleetReleaseTargetState>,
    to_target_state: Option<FleetReleaseTargetState>,
    reason_code: &str,
    actor_subject: &str,
    details: serde_json::Value,
    occurred_at: DateTime<Utc>,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO fleet_release_events (
            id, fleet_release_id, tenant_id, cluster_id,
            from_release_status, to_release_status,
            from_target_state, to_target_state,
            reason_code, actor_subject, details, occurred_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
    )
    .bind(Uuid::new_v4())
    .bind(release_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.map(ClusterId::as_uuid))
    .bind(from_release_status.map(release_status_name))
    .bind(release_status_name(to_release_status))
    .bind(from_target_state.map(target_state_name))
    .bind(to_target_state.map(target_state_name))
    .bind(reason_code)
    .bind(actor_subject)
    .bind(details)
    .bind(occurred_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn release_from_row(pool: &PgPool, row: &sqlx::postgres::PgRow) -> Result<FleetRelease, ControlPlaneError> {
    let id = FleetReleaseId::from_uuid(row.try_get("id")?);
    Ok(FleetRelease {
        schema_version: super::model::FLEET_RELEASE_SCHEMA_VERSION.to_owned(),
        id,
        fleet_id: FleetId::from_uuid(row.try_get("fleet_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        correlation_id: CorrelationId::from_uuid(row.try_get("correlation_id")?),
        release_ref: row.try_get("release_ref")?,
        artifact_digest: row.try_get("artifact_digest")?,
        target_version: row.try_get("target_version")?,
        owner: row.try_get("owner_name")?,
        maintenance_window_start: row.try_get("maintenance_window_start")?,
        maintenance_window_end: row.try_get("maintenance_window_end")?,
        rollback_artifact_digest: row.try_get("rollback_artifact_digest")?,
        slo_policy_id: row.try_get("slo_policy_id")?,
        status: parse_release_status(row.try_get::<String, _>("release_status")?.as_str())?,
        active_batch: optional_u32(row.try_get("active_batch")?, "Fleet release active batch")?,
        batches: batches(pool, id).await?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

async fn batches(pool: &PgPool, id: FleetReleaseId) -> Result<Vec<FleetReleaseBatch>, ControlPlaneError> {
    let rows = sqlx::query(
        "SELECT batch_sequence, region_id, cluster_ids, max_concurrency, canary
         FROM fleet_release_batches
         WHERE fleet_release_id = $1
         ORDER BY batch_sequence",
    )
    .bind(id.as_uuid())
    .fetch_all(pool)
    .await?;
    rows.iter()
        .map(|row| {
            let clusters: Vec<Uuid> = row.try_get("cluster_ids")?;
            Ok(FleetReleaseBatch {
                sequence: required_u32(row.try_get("batch_sequence")?, "Fleet release batch sequence")?,
                region_id: RegionId::from_uuid(row.try_get("region_id")?),
                cluster_ids: clusters.into_iter().map(ClusterId::from_uuid).collect(),
                max_concurrency: required_u32(row.try_get("max_concurrency")?, "Fleet release batch concurrency")?,
                canary: row.try_get("canary")?,
            })
        })
        .collect()
}

fn target_from_row(row: &sqlx::postgres::PgRow) -> Result<FleetReleaseTarget, ControlPlaneError> {
    Ok(FleetReleaseTarget {
        fleet_release_id: FleetReleaseId::from_uuid(row.try_get("fleet_release_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        region_id: RegionId::from_uuid(row.try_get("region_id")?),
        batch_sequence: required_u32(row.try_get("batch_sequence")?, "Fleet release target batch")?,
        canary: row.try_get("canary")?,
        state: parse_target_state(row.try_get::<String, _>("target_state")?.as_str())?,
        release_id: row.try_get::<Option<Uuid>, _>("release_id")?.map(ReleaseId::from_uuid),
        readiness_reason_codes: row.try_get("readiness_reason_codes")?,
        regression_detected: row.try_get("regression_detected")?,
        sanitized_outcome: row.try_get("sanitized_outcome")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn cluster_uuids(values: &[ClusterId]) -> Vec<Uuid> {
    values.iter().map(|id| id.as_uuid()).collect()
}

fn required_u32(value: i32, field: &str) -> Result<u32, ControlPlaneError> {
    u32::try_from(value).map_err(|_| ControlPlaneError::configuration(format!("{field} is invalid")))
}

fn optional_u32(value: Option<i32>, field: &str) -> Result<Option<u32>, ControlPlaneError> {
    value.map(|value| required_u32(value, field)).transpose()
}

pub(super) const fn release_status_name(status: FleetReleaseStatus) -> &'static str {
    match status {
        FleetReleaseStatus::Planned => "planned",
        FleetReleaseStatus::ReadinessChecking => "readiness_checking",
        FleetReleaseStatus::Ready => "ready",
        FleetReleaseStatus::CanaryRunning => "canary_running",
        FleetReleaseStatus::BatchRunning => "batch_running",
        FleetReleaseStatus::Paused => "paused",
        FleetReleaseStatus::Verifying => "verifying",
        FleetReleaseStatus::RollingBack => "rolling_back",
        FleetReleaseStatus::RolledBack => "rolled_back",
        FleetReleaseStatus::Completed => "completed",
        FleetReleaseStatus::ManualTakeover => "manual_takeover",
        FleetReleaseStatus::Failed => "failed",
    }
}

fn parse_release_status(value: &str) -> Result<FleetReleaseStatus, ControlPlaneError> {
    match value {
        "planned" => Ok(FleetReleaseStatus::Planned),
        "readiness_checking" => Ok(FleetReleaseStatus::ReadinessChecking),
        "ready" => Ok(FleetReleaseStatus::Ready),
        "canary_running" => Ok(FleetReleaseStatus::CanaryRunning),
        "batch_running" => Ok(FleetReleaseStatus::BatchRunning),
        "paused" => Ok(FleetReleaseStatus::Paused),
        "verifying" => Ok(FleetReleaseStatus::Verifying),
        "rolling_back" => Ok(FleetReleaseStatus::RollingBack),
        "rolled_back" => Ok(FleetReleaseStatus::RolledBack),
        "completed" => Ok(FleetReleaseStatus::Completed),
        "manual_takeover" => Ok(FleetReleaseStatus::ManualTakeover),
        "failed" => Ok(FleetReleaseStatus::Failed),
        _ => Err(ControlPlaneError::configuration(
            "Fleet release contains an invalid persisted status",
        )),
    }
}

pub(super) const fn target_state_name(state: FleetReleaseTargetState) -> &'static str {
    match state {
        FleetReleaseTargetState::Pending => "pending",
        FleetReleaseTargetState::ReadinessChecking => "readiness_checking",
        FleetReleaseTargetState::Ready => "ready",
        FleetReleaseTargetState::Ineligible => "ineligible",
        FleetReleaseTargetState::CanaryRunning => "canary_running",
        FleetReleaseTargetState::BatchRunning => "batch_running",
        FleetReleaseTargetState::Paused => "paused",
        FleetReleaseTargetState::RollingBack => "rolling_back",
        FleetReleaseTargetState::RolledBack => "rolled_back",
        FleetReleaseTargetState::Completed => "completed",
        FleetReleaseTargetState::Skipped => "skipped",
        FleetReleaseTargetState::Failed => "failed",
    }
}

fn parse_target_state(value: &str) -> Result<FleetReleaseTargetState, ControlPlaneError> {
    match value {
        "pending" => Ok(FleetReleaseTargetState::Pending),
        "readiness_checking" => Ok(FleetReleaseTargetState::ReadinessChecking),
        "ready" => Ok(FleetReleaseTargetState::Ready),
        "ineligible" => Ok(FleetReleaseTargetState::Ineligible),
        "canary_running" => Ok(FleetReleaseTargetState::CanaryRunning),
        "batch_running" => Ok(FleetReleaseTargetState::BatchRunning),
        "paused" => Ok(FleetReleaseTargetState::Paused),
        "rolling_back" => Ok(FleetReleaseTargetState::RollingBack),
        "rolled_back" => Ok(FleetReleaseTargetState::RolledBack),
        "completed" => Ok(FleetReleaseTargetState::Completed),
        "skipped" => Ok(FleetReleaseTargetState::Skipped),
        "failed" => Ok(FleetReleaseTargetState::Failed),
        _ => Err(ControlPlaneError::configuration(
            "Fleet release target contains an invalid persisted state",
        )),
    }
}

fn parse_linked_release_status(value: &str) -> Result<ReleaseStatus, ControlPlaneError> {
    match value {
        "planned" => Ok(ReleaseStatus::Planned),
        "readiness_checking" => Ok(ReleaseStatus::ReadinessChecking),
        "ready" => Ok(ReleaseStatus::Ready),
        "canary_running" => Ok(ReleaseStatus::CanaryRunning),
        "paused" => Ok(ReleaseStatus::Paused),
        "verifying" => Ok(ReleaseStatus::Verifying),
        "rolling_back" => Ok(ReleaseStatus::RollingBack),
        "rolled_back" => Ok(ReleaseStatus::RolledBack),
        "completed" => Ok(ReleaseStatus::Completed),
        "manual_takeover" => Ok(ReleaseStatus::ManualTakeover),
        "failed" => Ok(ReleaseStatus::Failed),
        _ => Err(ControlPlaneError::configuration(
            "linked release workflow contains an invalid persisted status",
        )),
    }
}
