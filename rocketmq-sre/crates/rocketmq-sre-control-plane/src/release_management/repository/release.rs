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
use rocketmq_sre_contracts::AuditEvent;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ReleaseId;
use rocketmq_sre_contracts::ReleaseObservation;
use rocketmq_sre_contracts::ReleaseReport;
use rocketmq_sre_contracts::ReleaseStatus;
use rocketmq_sre_contracts::ReleaseWorkflow;
use rocketmq_sre_contracts::TenantId;
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use super::super::model::QueuedIntegrationDelivery;
use super::super::model::ReleaseEventRecord;
use super::integration::enqueue_delivery_in_transaction;
use super::support::from_json;
use super::support::insert_audit;
use super::support::insert_release_event;
use super::support::json_value;
use super::support::observation_phase_name;
use super::support::parse_release_status;
use super::support::release_status_name;
use crate::ControlPlaneError;
use crate::PostgresRepository;

impl PostgresRepository {
    pub(super) async fn insert_release_workflow(
        &self,
        workflow: &ReleaseWorkflow,
        event: &ReleaseEventRecord,
        audit: &AuditEvent,
        outbound: &[QueuedIntegrationDelivery],
    ) -> Result<(), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO release_workflows (
                id, tenant_id, cluster_id, incident_id, correlation_id,
                change_id, release_ref, target_version, runbook_id,
                runbook_version, plan_id, plan_hash, rollback_plan_id,
                rollback_plan_hash, readiness_snapshot, status,
                active_execution_id, regression_detected, pause_reason,
                workflow_snapshot, created_by, created_at, updated_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, $11, $12, $13,
                $14, $15, $16,
                $17, $18, $19,
                $20, $21, $22, $23
             )",
        )
        .bind(workflow.id.as_uuid())
        .bind(workflow.tenant_id.as_uuid())
        .bind(workflow.cluster_id.as_uuid())
        .bind(workflow.incident_id.as_uuid())
        .bind(workflow.correlation_id.as_uuid())
        .bind(&workflow.change_id)
        .bind(&workflow.release_ref)
        .bind(&workflow.target_version)
        .bind(workflow.runbook_id.as_uuid())
        .bind(&workflow.runbook_version)
        .bind(workflow.plan_id.as_uuid())
        .bind(&workflow.plan_hash)
        .bind(workflow.rollback_plan_id.map(|id| id.as_uuid()))
        .bind(&workflow.rollback_plan_hash)
        .bind(workflow.readiness.as_ref().map(json_value).transpose()?)
        .bind(release_status_name(workflow.status))
        .bind(workflow.active_execution_id.map(ExecutionId::as_uuid))
        .bind(workflow.regression_detected)
        .bind(&workflow.pause_reason)
        .bind(json_value(workflow)?)
        .bind(&workflow.created_by)
        .bind(workflow.created_at)
        .bind(workflow.updated_at)
        .execute(&mut *transaction)
        .await
        .map_err(map_release_insert_error)?;
        insert_release_event(&mut transaction, event).await?;
        insert_audit(&mut transaction, audit).await?;
        enqueue_outbound(&mut transaction, outbound).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn release_workflow(
        &self,
        tenant_id: TenantId,
        id: ReleaseId,
    ) -> Result<ReleaseWorkflow, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT *
             FROM release_workflows
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant_id.as_uuid())
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        release_workflow_from_row(&row)
    }

    pub(super) async fn release_workflows(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        status: Option<ReleaseStatus>,
        limit: i64,
    ) -> Result<Vec<ReleaseWorkflow>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT *
             FROM release_workflows
             WHERE tenant_id = $1 AND cluster_id = $2
               AND ($3::text IS NULL OR status = $3)
             ORDER BY updated_at DESC, id
             LIMIT $4",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(status.map(release_status_name))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(release_workflow_from_row).collect()
    }

    pub(super) async fn update_release_workflow(
        &self,
        workflow: &ReleaseWorkflow,
        expected_status: ReleaseStatus,
        expected_updated_at: DateTime<Utc>,
        event: &ReleaseEventRecord,
        audit: &AuditEvent,
        outbound: &[QueuedIntegrationDelivery],
    ) -> Result<(), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let updated = update_release_row(&mut transaction, workflow, expected_status, expected_updated_at).await?;
        if updated != 1 {
            return Err(ControlPlaneError::conflict_code(
                "release_state_changed",
                "release workflow was changed by another operator",
            ));
        }
        insert_release_event(&mut transaction, event).await?;
        insert_audit(&mut transaction, audit).await?;
        enqueue_outbound(&mut transaction, outbound).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn insert_release_observation(
        &self,
        observation_id: Uuid,
        current: &ReleaseWorkflow,
        updated: Option<&ReleaseWorkflow>,
        observation: &ReleaseObservation,
        event: Option<&ReleaseEventRecord>,
        audits: &[AuditEvent],
        outbound: &[QueuedIntegrationDelivery],
    ) -> Result<(), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO release_observations (
                observation_id, release_id, phase, observation_snapshot,
                observed_at
             ) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(observation_id)
        .bind(current.id.as_uuid())
        .bind(observation_phase_name(observation.phase))
        .bind(json_value(observation)?)
        .bind(observation.observed_at)
        .execute(&mut *transaction)
        .await?;
        if let Some(updated) = updated {
            let affected = update_release_row(&mut transaction, updated, current.status, current.updated_at).await?;
            if affected != 1 {
                return Err(ControlPlaneError::conflict_code(
                    "release_state_changed",
                    "release workflow changed while the observation was being recorded",
                ));
            }
        }
        if let Some(event) = event {
            insert_release_event(&mut transaction, event).await?;
        }
        for audit in audits {
            insert_audit(&mut transaction, audit).await?;
        }
        enqueue_outbound(&mut transaction, outbound).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn release_observations(
        &self,
        tenant_id: TenantId,
        release_id: ReleaseId,
    ) -> Result<Vec<ReleaseObservation>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT observation_snapshot
             FROM release_observations observation
             JOIN release_workflows release ON release.id = observation.release_id
             WHERE release.tenant_id = $1 AND observation.release_id = $2
             ORDER BY observation.sequence_id",
        )
        .bind(tenant_id.as_uuid())
        .bind(release_id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| from_json(row.try_get("observation_snapshot")?))
            .collect()
    }

    pub(super) async fn release_report(
        &self,
        tenant_id: TenantId,
        release_id: ReleaseId,
    ) -> Result<Option<ReleaseReport>, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT report.report_snapshot
             FROM release_reports report
             JOIN release_workflows release ON release.id = report.release_id
             WHERE release.tenant_id = $1 AND report.release_id = $2",
        )
        .bind(tenant_id.as_uuid())
        .bind(release_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| from_json(row.try_get("report_snapshot")?)).transpose()
    }

    pub(super) async fn insert_release_report(
        &self,
        report: &ReleaseReport,
        audit: &AuditEvent,
    ) -> Result<ReleaseReport, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT INTO release_reports (
                report_id, release_id, tenant_id, cluster_id, incident_id,
                change_id, release_ref, final_status, report_snapshot,
                generated_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10
             )
             ON CONFLICT (release_id) DO NOTHING",
        )
        .bind(report.id.as_uuid())
        .bind(report.release_id.as_uuid())
        .bind(report.tenant_id.as_uuid())
        .bind(report.cluster_id.as_uuid())
        .bind(report.incident_id.as_uuid())
        .bind(&report.change_id)
        .bind(&report.release_ref)
        .bind(release_status_name(report.final_status))
        .bind(json_value(report)?)
        .bind(report.generated_at)
        .execute(&mut *transaction)
        .await?
        .rows_affected()
            == 1;
        if inserted {
            insert_audit(&mut transaction, audit).await?;
        }
        transaction.commit().await?;
        if inserted {
            Ok(report.clone())
        } else {
            self.release_report(report.tenant_id, report.release_id)
                .await?
                .ok_or_else(|| {
                    ControlPlaneError::configuration("release report conflict did not resolve to a persisted report")
                })
        }
    }
}

async fn update_release_row(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow: &ReleaseWorkflow,
    expected_status: ReleaseStatus,
    expected_updated_at: DateTime<Utc>,
) -> Result<u64, ControlPlaneError> {
    let result = sqlx::query(
        "UPDATE release_workflows
         SET readiness_snapshot = $4,
             status = $5,
             active_execution_id = $6,
             regression_detected = $7,
             pause_reason = $8,
             workflow_snapshot = $9,
             updated_at = $10
         WHERE id = $1 AND tenant_id = $2
           AND status = $3 AND updated_at = $11",
    )
    .bind(workflow.id.as_uuid())
    .bind(workflow.tenant_id.as_uuid())
    .bind(release_status_name(expected_status))
    .bind(workflow.readiness.as_ref().map(json_value).transpose()?)
    .bind(release_status_name(workflow.status))
    .bind(workflow.active_execution_id.map(ExecutionId::as_uuid))
    .bind(workflow.regression_detected)
    .bind(&workflow.pause_reason)
    .bind(json_value(workflow)?)
    .bind(workflow.updated_at)
    .bind(expected_updated_at)
    .execute(&mut **transaction)
    .await?;
    Ok(result.rows_affected())
}

async fn enqueue_outbound(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    outbound: &[QueuedIntegrationDelivery],
) -> Result<(), ControlPlaneError> {
    for queued in outbound {
        enqueue_delivery_in_transaction(transaction, &queued.target, &queued.delivery, &queued.audit).await?;
    }
    Ok(())
}

fn release_workflow_from_row(row: &PgRow) -> Result<ReleaseWorkflow, ControlPlaneError> {
    let mut workflow: ReleaseWorkflow = from_json(row.try_get("workflow_snapshot")?)?;
    workflow.readiness = row
        .try_get::<Option<serde_json::Value>, _>("readiness_snapshot")?
        .map(from_json)
        .transpose()?;
    workflow.status = parse_release_status(row.try_get("status")?)?;
    workflow.active_execution_id = row
        .try_get::<Option<Uuid>, _>("active_execution_id")?
        .map(ExecutionId::from_uuid);
    workflow.regression_detected = row.try_get("regression_detected")?;
    workflow.pause_reason = row.try_get("pause_reason")?;
    workflow.updated_at = row.try_get("updated_at")?;
    Ok(workflow)
}

fn map_release_insert_error(error: sqlx::Error) -> ControlPlaneError {
    if let sqlx::Error::Database(database) = &error
        && database.is_unique_violation()
    {
        return ControlPlaneError::conflict_code(
            "release_exists",
            "release change identifier or release reference already exists in this cluster",
        );
    }
    if let sqlx::Error::Database(database) = &error
        && database.code().as_deref() == Some("P0001")
    {
        return ControlPlaneError::conflict_code(
            "release_scope_mismatch",
            "release plan, rollback, execution, or incident scope does not match",
        );
    }
    ControlPlaneError::Database(error)
}
