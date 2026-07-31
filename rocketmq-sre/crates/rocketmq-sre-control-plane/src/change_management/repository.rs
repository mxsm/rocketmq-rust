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
use rocketmq_sre_contracts::ChangeSchedule;
use rocketmq_sre_contracts::ChangeScheduleId;
use rocketmq_sre_contracts::ChangeScheduleStatus;
use rocketmq_sre_contracts::ChangeWindow;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::RunbookDefinition;
use rocketmq_sre_contracts::RunbookId;
use rocketmq_sre_contracts::RunbookStepId;
use rocketmq_sre_contracts::RunbookStepPlanBinding;
use rocketmq_sre_contracts::TenantId;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use uuid::Uuid;

use super::model::ManualGateDecisionRecord;
use super::model::ScheduleEvent;
use crate::ControlPlaneError;
use crate::PostgresRepository;

impl PostgresRepository {
    pub(super) async fn store_runbook_definition(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        actor: &str,
        definition: &RunbookDefinition,
        audit: &AuditEvent,
    ) -> Result<bool, ControlPlaneError> {
        let snapshot = json_value(definition)?;
        let mut transaction = self.pool.begin().await?;
        let result = sqlx::query(
            "INSERT INTO runbook_definitions (
                tenant_id, cluster_id, id, version, risk, definition_snapshot,
                created_by, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (tenant_id, cluster_id, id, version) DO NOTHING",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(definition.id.as_uuid())
        .bind(&definition.version)
        .bind(action_risk_name(definition.risk)?)
        .bind(snapshot.clone())
        .bind(actor)
        .bind(definition.created_at)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            let existing: Value = sqlx::query_scalar(
                "SELECT definition_snapshot
                 FROM runbook_definitions
                 WHERE tenant_id = $1 AND cluster_id = $2
                   AND id = $3 AND version = $4",
            )
            .bind(tenant_id.as_uuid())
            .bind(cluster_id.as_uuid())
            .bind(definition.id.as_uuid())
            .bind(&definition.version)
            .fetch_one(&mut *transaction)
            .await?;
            if existing != snapshot {
                return Err(ControlPlaneError::conflict_code(
                    "runbook_version_conflict",
                    "runbook identity and version already contain a different immutable definition",
                ));
            }
            transaction.commit().await?;
            return Ok(false);
        }
        insert_audit(&mut transaction, audit).await?;
        transaction.commit().await?;
        Ok(true)
    }

    pub(crate) async fn runbook_definition(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        id: RunbookId,
        version: &str,
    ) -> Result<RunbookDefinition, ControlPlaneError> {
        let snapshot: Value = sqlx::query_scalar(
            "SELECT definition_snapshot
             FROM runbook_definitions
             WHERE tenant_id = $1 AND cluster_id = $2
               AND id = $3 AND version = $4",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(id.as_uuid())
        .bind(version)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        from_json(snapshot)
    }

    pub(super) async fn runbook_definitions(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        limit: i64,
    ) -> Result<Vec<RunbookDefinition>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT definition_snapshot
             FROM runbook_definitions
             WHERE tenant_id = $1 AND cluster_id = $2
             ORDER BY created_at DESC, id, version DESC
             LIMIT $3",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| from_json(row.try_get("definition_snapshot")?))
            .collect()
    }

    pub(super) async fn store_change_window(
        &self,
        window: &ChangeWindow,
        audit: &AuditEvent,
    ) -> Result<bool, ControlPlaneError> {
        let snapshot = json_value(window)?;
        let mut transaction = self.pool.begin().await?;
        let result = sqlx::query(
            "INSERT INTO change_windows (
                id, tenant_id, cluster_id, kind, timezone, starts_at, ends_at,
                resource_keys, max_parallelism, window_snapshot, created_by,
                created_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9, $10, $11, $12
             )
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(window.id.as_uuid())
        .bind(window.tenant_id.as_uuid())
        .bind(window.cluster_id.as_uuid())
        .bind(change_window_kind_name(window.kind))
        .bind(&window.timezone)
        .bind(window.starts_at)
        .bind(window.ends_at)
        .bind(window.resource_keys.iter().cloned().collect::<Vec<_>>())
        .bind(i32::from(window.max_parallelism))
        .bind(snapshot.clone())
        .bind(&window.created_by)
        .bind(window.created_at)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            let existing: Value = sqlx::query_scalar("SELECT window_snapshot FROM change_windows WHERE id = $1")
                .bind(window.id.as_uuid())
                .fetch_one(&mut *transaction)
                .await?;
            if existing != snapshot {
                return Err(ControlPlaneError::conflict_code(
                    "change_window_conflict",
                    "change window identifier already contains a different immutable definition",
                ));
            }
            transaction.commit().await?;
            return Ok(false);
        }
        insert_audit(&mut transaction, audit).await?;
        transaction.commit().await?;
        Ok(true)
    }

    pub(super) async fn change_windows(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        limit: i64,
    ) -> Result<Vec<ChangeWindow>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT window_snapshot
             FROM change_windows
             WHERE tenant_id = $1 AND cluster_id = $2
               AND ends_at > $3 AND starts_at < $4
             ORDER BY starts_at, id
             LIMIT $5",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(from)
        .bind(to)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| from_json(row.try_get("window_snapshot")?))
            .collect()
    }

    pub(super) async fn store_change_schedule(
        &self,
        schedule: &ChangeSchedule,
        allowed_parallelism: u16,
        event: &ScheduleEvent,
        audit: &AuditEvent,
    ) -> Result<(), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let lock_key = format!("{}:{}", schedule.tenant_id, schedule.cluster_id);
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(lock_key)
            .execute(&mut *transaction)
            .await?;
        let resources = schedule.resource_keys.iter().cloned().collect::<Vec<_>>();
        let resource_overlap: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1
                FROM change_schedules
                WHERE tenant_id = $1 AND cluster_id = $2
                  AND status NOT IN ('completed', 'cancelled', 'rejected')
                  AND scheduled_start < $4 AND scheduled_end > $3
                  AND resource_keys && $5
             )",
        )
        .bind(schedule.tenant_id.as_uuid())
        .bind(schedule.cluster_id.as_uuid())
        .bind(schedule.scheduled_start)
        .bind(schedule.scheduled_end)
        .bind(&resources)
        .fetch_one(&mut *transaction)
        .await?;
        if resource_overlap {
            return Err(ControlPlaneError::conflict_code(
                "change_schedule_conflict",
                "another non-terminal schedule targets the same resource in the requested interval",
            ));
        }
        let overlapping: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)
             FROM change_schedules
             WHERE tenant_id = $1 AND cluster_id = $2
               AND status NOT IN ('completed', 'cancelled', 'rejected')
               AND scheduled_start < $4 AND scheduled_end > $3",
        )
        .bind(schedule.tenant_id.as_uuid())
        .bind(schedule.cluster_id.as_uuid())
        .bind(schedule.scheduled_start)
        .bind(schedule.scheduled_end)
        .fetch_one(&mut *transaction)
        .await?;
        if overlapping >= i64::from(allowed_parallelism) {
            return Err(ControlPlaneError::conflict_code(
                "change_parallelism_exceeded",
                "overlapping schedules meet or exceed the approved parallelism bound",
            ));
        }
        sqlx::query(
            "INSERT INTO change_schedules (
                id, tenant_id, cluster_id, correlation_id, runbook_id,
                runbook_version, plan_bindings, scheduled_start, scheduled_end,
                resource_keys, status, intent_persisted, next_step_sequence,
                active_execution_id, waiting_manual_gate, completed_step_ids,
                pause_requested_at, cancel_requested_at, created_by, created_at,
                updated_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, $11, $12, $13,
                $14, $15, $16,
                $17, $18, $19, $20,
                $21
             )",
        )
        .bind(schedule.id.as_uuid())
        .bind(schedule.tenant_id.as_uuid())
        .bind(schedule.cluster_id.as_uuid())
        .bind(schedule.correlation_id.as_uuid())
        .bind(schedule.runbook_id.as_uuid())
        .bind(&schedule.runbook_version)
        .bind(json_value(&schedule.plan_bindings)?)
        .bind(schedule.scheduled_start)
        .bind(schedule.scheduled_end)
        .bind(resources)
        .bind(change_schedule_status_name(schedule.status))
        .bind(schedule.intent_persisted)
        .bind(i32::from(schedule.next_step_sequence))
        .bind(schedule.active_execution_id.map(ExecutionId::as_uuid))
        .bind(schedule.waiting_manual_gate.map(RunbookStepId::as_uuid))
        .bind(
            schedule
                .completed_steps
                .iter()
                .map(|id| id.as_uuid())
                .collect::<Vec<_>>(),
        )
        .bind(schedule.pause_requested_at)
        .bind(schedule.cancel_requested_at)
        .bind(&schedule.created_by)
        .bind(schedule.created_at)
        .bind(schedule.updated_at)
        .execute(&mut *transaction)
        .await?;
        insert_schedule_event(&mut transaction, event).await?;
        insert_audit(&mut transaction, audit).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn change_schedule(
        &self,
        tenant_id: TenantId,
        id: ChangeScheduleId,
    ) -> Result<ChangeSchedule, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT *
             FROM change_schedules
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant_id.as_uuid())
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        schedule_from_row(&row)
    }

    pub(super) async fn change_schedules(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        status: Option<ChangeScheduleStatus>,
        limit: i64,
    ) -> Result<Vec<ChangeSchedule>, ControlPlaneError> {
        let status = status.map(change_schedule_status_name);
        let rows = sqlx::query(
            "SELECT *
             FROM change_schedules
             WHERE tenant_id = $1 AND cluster_id = $2
               AND ($3::text IS NULL OR status = $3)
             ORDER BY scheduled_start DESC, id
             LIMIT $4",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(status)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(schedule_from_row).collect()
    }

    pub(super) async fn conflicting_change_schedules(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        starts_at: DateTime<Utc>,
        ends_at: DateTime<Utc>,
    ) -> Result<Vec<ChangeSchedule>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT *
             FROM change_schedules
             WHERE tenant_id = $1 AND cluster_id = $2
               AND status NOT IN ('completed', 'cancelled', 'rejected')
               AND scheduled_start < $4 AND scheduled_end > $3
             ORDER BY scheduled_start, id
             LIMIT 257",
        )
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(starts_at)
        .bind(ends_at)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(schedule_from_row).collect()
    }

    pub(super) async fn due_change_schedules(
        &self,
        now: DateTime<Utc>,
        limit: i64,
    ) -> Result<Vec<ChangeSchedule>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT *
             FROM change_schedules
             WHERE status IN ('scheduled', 'running', 'safe_stopping', 'reconciling')
               AND scheduled_start <= $1
             ORDER BY scheduled_start, id
             LIMIT $2",
        )
        .bind(now)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(schedule_from_row).collect()
    }

    pub(super) async fn update_change_schedule(
        &self,
        schedule: &ChangeSchedule,
        expected_status: ChangeScheduleStatus,
        expected_updated_at: DateTime<Utc>,
        event: &ScheduleEvent,
        audit: &AuditEvent,
    ) -> Result<(), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let result = update_schedule_row(&mut transaction, schedule, expected_status, expected_updated_at).await?;
        if result == 0 {
            return Err(ControlPlaneError::conflict_code(
                "change_schedule_state_changed",
                "change schedule was updated by another operator or scheduler",
            ));
        }
        insert_schedule_event(&mut transaction, event).await?;
        insert_audit(&mut transaction, audit).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn record_manual_gate_decision(
        &self,
        schedule: &ChangeSchedule,
        expected_updated_at: DateTime<Utc>,
        decision: &ManualGateDecisionRecord,
        event: &ScheduleEvent,
        audit: &AuditEvent,
    ) -> Result<(), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO runbook_manual_gate_decisions (
                decision_id, schedule_id, step_id, decision, actor_subject,
                actor_role, reason, occurred_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(decision.id)
        .bind(decision.schedule_id.as_uuid())
        .bind(decision.step_id.as_uuid())
        .bind(decision.decision.as_str())
        .bind(&decision.actor_subject)
        .bind(&decision.actor_role)
        .bind(&decision.reason)
        .bind(decision.occurred_at)
        .execute(&mut *transaction)
        .await
        .map_err(map_manual_gate_error)?;
        let updated = update_schedule_row(
            &mut transaction,
            schedule,
            ChangeScheduleStatus::AwaitingManualGate,
            expected_updated_at,
        )
        .await?;
        if updated == 0 {
            return Err(ControlPlaneError::conflict_code(
                "change_schedule_state_changed",
                "manual gate no longer matches the active schedule projection",
            ));
        }
        insert_schedule_event(&mut transaction, event).await?;
        insert_audit(&mut transaction, audit).await?;
        transaction.commit().await?;
        Ok(())
    }
}

async fn update_schedule_row(
    transaction: &mut Transaction<'_, Postgres>,
    schedule: &ChangeSchedule,
    expected_status: ChangeScheduleStatus,
    expected_updated_at: DateTime<Utc>,
) -> Result<u64, ControlPlaneError> {
    let result = sqlx::query(
        "UPDATE change_schedules
         SET status = $4,
             intent_persisted = $5,
             next_step_sequence = $6,
             active_execution_id = $7,
             waiting_manual_gate = $8,
             completed_step_ids = $9,
             pause_requested_at = $10,
             cancel_requested_at = $11,
             updated_at = $12
         WHERE id = $1 AND tenant_id = $2
           AND status = $3 AND updated_at = $13",
    )
    .bind(schedule.id.as_uuid())
    .bind(schedule.tenant_id.as_uuid())
    .bind(change_schedule_status_name(expected_status))
    .bind(change_schedule_status_name(schedule.status))
    .bind(schedule.intent_persisted)
    .bind(i32::from(schedule.next_step_sequence))
    .bind(schedule.active_execution_id.map(ExecutionId::as_uuid))
    .bind(schedule.waiting_manual_gate.map(RunbookStepId::as_uuid))
    .bind(
        schedule
            .completed_steps
            .iter()
            .map(|id| id.as_uuid())
            .collect::<Vec<_>>(),
    )
    .bind(schedule.pause_requested_at)
    .bind(schedule.cancel_requested_at)
    .bind(schedule.updated_at)
    .bind(expected_updated_at)
    .execute(&mut **transaction)
    .await?;
    Ok(result.rows_affected())
}

async fn insert_schedule_event(
    transaction: &mut Transaction<'_, Postgres>,
    event: &ScheduleEvent,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO change_schedule_events (
            event_id, schedule_id, correlation_id, from_status, to_status,
            reason_code, actor_subject, details, occurred_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(event.id)
    .bind(event.schedule_id.as_uuid())
    .bind(event.correlation_id.as_uuid())
    .bind(event.from_status.map(change_schedule_status_name))
    .bind(change_schedule_status_name(event.to_status))
    .bind(&event.reason_code)
    .bind(&event.actor_subject)
    .bind(&event.details)
    .bind(event.occurred_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_audit(
    transaction: &mut Transaction<'_, Postgres>,
    event: &AuditEvent,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO audit_events (
            event_id, tenant_id, cluster_id, correlation_id, event_kind,
            actor_subject, actor_role, resource_kind, resource_id,
            reason_code, details, event_snapshot, occurred_at
         ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9,
            $10, $11, $12, $13
         )",
    )
    .bind(event.id.as_uuid())
    .bind(event.tenant_id.as_uuid())
    .bind(event.cluster_id.as_uuid())
    .bind(event.correlation_id.as_uuid())
    .bind(audit_event_kind_name(event.event_kind))
    .bind(&event.actor_subject)
    .bind(&event.actor_role)
    .bind(&event.resource_kind)
    .bind(&event.resource_id)
    .bind(&event.reason_code)
    .bind(&event.details)
    .bind(json_value(event)?)
    .bind(event.occurred_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn schedule_from_row(row: &sqlx::postgres::PgRow) -> Result<ChangeSchedule, ControlPlaneError> {
    let plan_bindings: Vec<RunbookStepPlanBinding> = from_json(row.try_get("plan_bindings")?)?;
    let completed_steps = row
        .try_get::<Vec<Uuid>, _>("completed_step_ids")?
        .into_iter()
        .map(RunbookStepId::from_uuid)
        .collect();
    Ok(ChangeSchedule {
        schema_version: ChangeSchedule::SCHEMA_VERSION.to_owned(),
        id: ChangeScheduleId::from_uuid(row.try_get("id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        correlation_id: rocketmq_sre_contracts::CorrelationId::from_uuid(row.try_get("correlation_id")?),
        runbook_id: RunbookId::from_uuid(row.try_get("runbook_id")?),
        runbook_version: row.try_get("runbook_version")?,
        plan_bindings,
        scheduled_start: row.try_get("scheduled_start")?,
        scheduled_end: row.try_get("scheduled_end")?,
        resource_keys: row.try_get::<Vec<String>, _>("resource_keys")?.into_iter().collect(),
        status: parse_change_schedule_status(row.try_get("status")?)?,
        intent_persisted: row.try_get("intent_persisted")?,
        next_step_sequence: u16::try_from(row.try_get::<i32, _>("next_step_sequence")?).map_err(|_| {
            ControlPlaneError::validation(
                "invalid_persisted_schedule",
                "persisted next step sequence exceeds the contract bound",
            )
        })?,
        active_execution_id: row
            .try_get::<Option<Uuid>, _>("active_execution_id")?
            .map(ExecutionId::from_uuid),
        waiting_manual_gate: row
            .try_get::<Option<Uuid>, _>("waiting_manual_gate")?
            .map(RunbookStepId::from_uuid),
        completed_steps,
        pause_requested_at: row.try_get("pause_requested_at")?,
        cancel_requested_at: row.try_get("cancel_requested_at")?,
        created_by: row.try_get("created_by")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn change_schedule_status_name(status: ChangeScheduleStatus) -> &'static str {
    match status {
        ChangeScheduleStatus::Scheduled => "scheduled",
        ChangeScheduleStatus::Running => "running",
        ChangeScheduleStatus::AwaitingManualGate => "awaiting_manual_gate",
        ChangeScheduleStatus::Paused => "paused",
        ChangeScheduleStatus::SafeStopping => "safe_stopping",
        ChangeScheduleStatus::Reconciling => "reconciling",
        ChangeScheduleStatus::Completed => "completed",
        ChangeScheduleStatus::Cancelled => "cancelled",
        ChangeScheduleStatus::Rejected => "rejected",
    }
}

fn parse_change_schedule_status(value: &str) -> Result<ChangeScheduleStatus, ControlPlaneError> {
    match value {
        "scheduled" => Ok(ChangeScheduleStatus::Scheduled),
        "running" => Ok(ChangeScheduleStatus::Running),
        "awaiting_manual_gate" => Ok(ChangeScheduleStatus::AwaitingManualGate),
        "paused" => Ok(ChangeScheduleStatus::Paused),
        "safe_stopping" => Ok(ChangeScheduleStatus::SafeStopping),
        "reconciling" => Ok(ChangeScheduleStatus::Reconciling),
        "completed" => Ok(ChangeScheduleStatus::Completed),
        "cancelled" => Ok(ChangeScheduleStatus::Cancelled),
        "rejected" => Ok(ChangeScheduleStatus::Rejected),
        _ => Err(ControlPlaneError::validation(
            "invalid_persisted_schedule",
            "persisted schedule status is unknown",
        )),
    }
}

fn change_window_kind_name(kind: rocketmq_sre_contracts::ChangeWindowKind) -> &'static str {
    match kind {
        rocketmq_sre_contracts::ChangeWindowKind::Maintenance => "maintenance",
        rocketmq_sre_contracts::ChangeWindowKind::Freeze => "freeze",
        rocketmq_sre_contracts::ChangeWindowKind::Blackout => "blackout",
    }
}

fn action_risk_name(risk: rocketmq_sre_contracts::ActionRisk) -> Result<&'static str, ControlPlaneError> {
    match risk {
        rocketmq_sre_contracts::ActionRisk::R1 => Ok("r1"),
        rocketmq_sre_contracts::ActionRisk::R2 => Ok("r2"),
        _ => Err(ControlPlaneError::validation(
            "invalid_runbook_risk",
            "executable runbook risk must be R1 or R2",
        )),
    }
}

fn audit_event_kind_name(kind: rocketmq_sre_contracts::AuditEventKind) -> &'static str {
    match kind {
        rocketmq_sre_contracts::AuditEventKind::PlanCreated => "plan_created",
        rocketmq_sre_contracts::AuditEventKind::PlanSubmitted => "plan_submitted",
        rocketmq_sre_contracts::AuditEventKind::PolicyEvaluated => "policy_evaluated",
        rocketmq_sre_contracts::AuditEventKind::CriticReviewed => "critic_reviewed",
        rocketmq_sre_contracts::AuditEventKind::Approved => "approved",
        rocketmq_sre_contracts::AuditEventKind::Rejected => "rejected",
        rocketmq_sre_contracts::AuditEventKind::ExecutionSubmitted => "execution_submitted",
        rocketmq_sre_contracts::AuditEventKind::StateChanged => "state_changed",
        rocketmq_sre_contracts::AuditEventKind::StepIntentPersisted => "step_intent_persisted",
        rocketmq_sre_contracts::AuditEventKind::StepResultPersisted => "step_result_persisted",
        rocketmq_sre_contracts::AuditEventKind::VerificationCaptured => "verification_captured",
        rocketmq_sre_contracts::AuditEventKind::VerificationCompleted => "verification_completed",
        rocketmq_sre_contracts::AuditEventKind::RollbackStarted => "rollback_started",
        rocketmq_sre_contracts::AuditEventKind::ManualTakeoverRequired => "manual_takeover_required",
        rocketmq_sre_contracts::AuditEventKind::QuarantineCreated => "quarantine_created",
        rocketmq_sre_contracts::AuditEventKind::QuarantineClearRequested => "quarantine_clear_requested",
        rocketmq_sre_contracts::AuditEventKind::QuarantineCleared => "quarantine_cleared",
        rocketmq_sre_contracts::AuditEventKind::Cancelled => "cancelled",
        rocketmq_sre_contracts::AuditEventKind::RunbookCreated => "runbook_created",
        rocketmq_sre_contracts::AuditEventKind::ChangeWindowCreated => "change_window_created",
        rocketmq_sre_contracts::AuditEventKind::ChangeScheduleCreated => "change_schedule_created",
        rocketmq_sre_contracts::AuditEventKind::ChangeScheduleStateChanged => "change_schedule_state_changed",
        rocketmq_sre_contracts::AuditEventKind::ManualGateDecided => "manual_gate_decided",
        rocketmq_sre_contracts::AuditEventKind::IntegrationTargetRegistered => "integration_target_registered",
        rocketmq_sre_contracts::AuditEventKind::IntegrationDeliveryQueued => "integration_delivery_queued",
        rocketmq_sre_contracts::AuditEventKind::IntegrationDeliveryCompleted => "integration_delivery_completed",
        rocketmq_sre_contracts::AuditEventKind::ExternalApprovalReceived => "external_approval_received",
        rocketmq_sre_contracts::AuditEventKind::ReleaseCreated => "release_created",
        rocketmq_sre_contracts::AuditEventKind::ReleaseReadinessEvaluated => "release_readiness_evaluated",
        rocketmq_sre_contracts::AuditEventKind::ReleaseStateChanged => "release_state_changed",
        rocketmq_sre_contracts::AuditEventKind::ReleaseObservationCaptured => "release_observation_captured",
        rocketmq_sre_contracts::AuditEventKind::ReleaseReportGenerated => "release_report_generated",
    }
}

fn json_value<T: Serialize>(value: &T) -> Result<Value, ControlPlaneError> {
    serde_json::to_value(value)
        .map_err(|_| ControlPlaneError::validation("invalid_request", "value cannot be represented as JSON"))
}

fn from_json<T: DeserializeOwned>(value: Value) -> Result<T, ControlPlaneError> {
    serde_json::from_value(value)
        .map_err(|_| ControlPlaneError::validation("invalid_persisted_state", "stored JSON is incompatible"))
}

fn map_manual_gate_error(error: sqlx::Error) -> ControlPlaneError {
    if let sqlx::Error::Database(database) = &error
        && database.is_unique_violation()
    {
        return ControlPlaneError::conflict_code(
            "manual_gate_already_decided",
            "manual gate already has an immutable decision",
        );
    }
    ControlPlaneError::Database(error)
}
