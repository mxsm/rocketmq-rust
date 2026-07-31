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
use rocketmq_sre_contracts::AUTOMATION_SCHEMA_VERSION;
use rocketmq_sre_contracts::AutomationRunId;
use rocketmq_sre_contracts::AutomationRunStatus;
use rocketmq_sre_contracts::InspectionRunId;
use rocketmq_sre_contracts::PreventiveAutomationRequest;
use rocketmq_sre_contracts::PreventiveAutomationRun;
use rocketmq_sre_contracts::PreventiveRiskFamily;
use rocketmq_sre_contracts::TenantId;
use serde_json::Value;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use uuid::Uuid;

use super::model::CompletePreventiveRunRequest;
use super::model::PreventiveRunListQuery;
use crate::ControlPlaneError;
use crate::PostgresRepository;

impl PostgresRepository {
    pub(super) async fn create_preventive_run(
        &self,
        request: &PreventiveAutomationRequest,
        inspection_run_id: Option<InspectionRunId>,
    ) -> Result<PreventiveAutomationRun, ControlPlaneError> {
        request
            .validate()
            .map_err(|error| ControlPlaneError::validation("invalid_preventive_request", error.to_string()))?;
        let run = PreventiveAutomationRun {
            schema_version: AUTOMATION_SCHEMA_VERSION.to_owned(),
            id: request.id,
            tenant_id: request.tenant_id,
            cluster_id: request.cluster_id,
            correlation_id: request.correlation_id,
            risk_family: request.risk_family,
            status: AutomationRunStatus::Pending,
            idempotency_key: request.idempotency_key.clone(),
            inspection_run_id,
            recommendation_ids: Vec::new(),
            freeze_id: None,
            kill_switch_suggested: false,
            result_code: "preventive_pending".to_owned(),
            sanitized_summary: "Bounded preventive inspection accepted for processing".to_owned(),
            started_at: request.requested_at,
            completed_at: None,
        };
        run.validate()
            .map_err(|error| ControlPlaneError::validation("invalid_preventive_run", error.to_string()))?;
        let mut transaction = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT INTO preventive_automation_runs (
                id, tenant_id, cluster_id, inspection_run_id, risk_family,
                status, recommendation_id, freeze_id, result_snapshot,
                started_at, completed_at, correlation_id, idempotency_key,
                budget_snapshot, request_snapshot, updated_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                'pending', NULL, NULL, $6,
                $7, NULL, $8, $9,
                $10, $11, $7
             )
             ON CONFLICT (tenant_id, risk_family, idempotency_key)
             DO NOTHING",
        )
        .bind(run.id.as_uuid())
        .bind(run.tenant_id.as_uuid())
        .bind(run.cluster_id.as_uuid())
        .bind(run.inspection_run_id.map(InspectionRunId::as_uuid))
        .bind(preventive_risk_name(run.risk_family))
        .bind(json_value(&run)?)
        .bind(run.started_at)
        .bind(run.correlation_id.as_uuid())
        .bind(&run.idempotency_key)
        .bind(json_value(&request.budget)?)
        .bind(json_value(request)?)
        .execute(&mut *transaction)
        .await?;
        if inserted.rows_affected() == 1 {
            insert_preventive_event(
                &mut transaction,
                &run,
                None,
                AutomationRunStatus::Pending,
                "preventive_accepted",
            )
            .await?;
            transaction.commit().await?;
            return Ok(run);
        }
        let row = sqlx::query(
            "SELECT request_snapshot, result_snapshot
             FROM preventive_automation_runs
             WHERE tenant_id = $1 AND risk_family = $2
               AND idempotency_key = $3",
        )
        .bind(request.tenant_id.as_uuid())
        .bind(preventive_risk_name(request.risk_family))
        .bind(&request.idempotency_key)
        .fetch_one(&mut *transaction)
        .await?;
        let stored_request: PreventiveAutomationRequest = from_json(row.try_get("request_snapshot")?)?;
        if !same_request(&stored_request, request) {
            return Err(ControlPlaneError::conflict_code(
                "preventive_idempotency_conflict",
                "preventive idempotency key already binds different request content",
            ));
        }
        let stored = from_json(row.try_get("result_snapshot")?)?;
        transaction.commit().await?;
        Ok(stored)
    }

    pub(super) async fn claim_preventive_run(
        &self,
        tenant_id: TenantId,
        run_id: AutomationRunId,
    ) -> Result<(PreventiveAutomationRun, bool), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT result_snapshot
             FROM preventive_automation_runs
             WHERE id = $1 AND tenant_id = $2
             FOR UPDATE",
        )
        .bind(run_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let current: PreventiveAutomationRun = from_json(row.try_get("result_snapshot")?)?;
        if current.status != AutomationRunStatus::Pending {
            transaction.commit().await?;
            return Ok((current, false));
        }
        let mut running = current.clone();
        running.status = AutomationRunStatus::Running;
        running.result_code = "preventive_running".to_owned();
        running.sanitized_summary = "Bounded preventive inspection is in progress".to_owned();
        update_preventive_run(&mut transaction, &current, &running).await?;
        insert_preventive_event(
            &mut transaction,
            &running,
            Some(AutomationRunStatus::Pending),
            AutomationRunStatus::Running,
            "preventive_started",
        )
        .await?;
        transaction.commit().await?;
        Ok((running, true))
    }

    pub(super) async fn complete_preventive_run(
        &self,
        tenant_id: TenantId,
        run_id: AutomationRunId,
        completion: &CompletePreventiveRunRequest,
    ) -> Result<PreventiveAutomationRun, ControlPlaneError> {
        if !completion.status.is_terminal() {
            return Err(ControlPlaneError::validation(
                "invalid_preventive_transition",
                "preventive completion must use a terminal status",
            ));
        }
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT result_snapshot
             FROM preventive_automation_runs
             WHERE id = $1 AND tenant_id = $2
             FOR UPDATE",
        )
        .bind(run_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let current: PreventiveAutomationRun = from_json(row.try_get("result_snapshot")?)?;
        if current.status.is_terminal() {
            let expected = completed_run(&current, completion);
            if current == expected {
                transaction.commit().await?;
                return Ok(current);
            }
            return Err(ControlPlaneError::conflict_code(
                "preventive_completion_conflict",
                "terminal preventive result is immutable",
            ));
        }
        let running = if current.status == AutomationRunStatus::Pending {
            let mut running = current.clone();
            running.status = AutomationRunStatus::Running;
            running.result_code = "preventive_running".to_owned();
            running.sanitized_summary = "Bounded preventive inspection is in progress".to_owned();
            update_preventive_run(&mut transaction, &current, &running).await?;
            insert_preventive_event(
                &mut transaction,
                &running,
                Some(AutomationRunStatus::Pending),
                AutomationRunStatus::Running,
                "preventive_started",
            )
            .await?;
            running
        } else {
            current
        };
        let completed = completed_run(&running, completion);
        completed
            .validate()
            .map_err(|error| ControlPlaneError::validation("invalid_preventive_result", error.to_string()))?;
        update_preventive_run(&mut transaction, &running, &completed).await?;
        insert_preventive_event(
            &mut transaction,
            &completed,
            Some(AutomationRunStatus::Running),
            completed.status,
            &completed.result_code,
        )
        .await?;
        transaction.commit().await?;
        Ok(completed)
    }

    pub(super) async fn preventive_runs(
        &self,
        tenant_id: TenantId,
        query: &PreventiveRunListQuery,
        limit: i64,
    ) -> Result<Vec<PreventiveAutomationRun>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT result_snapshot
             FROM preventive_automation_runs
             WHERE tenant_id = $1
               AND ($2::UUID IS NULL OR cluster_id = $2)
               AND ($3::TEXT IS NULL OR risk_family = $3)
               AND ($4::TEXT IS NULL OR status = $4)
             ORDER BY started_at DESC, id DESC
             LIMIT $5",
        )
        .bind(tenant_id.as_uuid())
        .bind(query.cluster_id.map(rocketmq_sre_contracts::ClusterId::as_uuid))
        .bind(query.risk_family.map(preventive_risk_name))
        .bind(query.status.map(automation_status_name))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| from_json(row.try_get("result_snapshot")?))
            .collect()
    }
}

fn completed_run(
    current: &PreventiveAutomationRun,
    completion: &CompletePreventiveRunRequest,
) -> PreventiveAutomationRun {
    let mut completed = current.clone();
    completed.status = completion.status;
    if completion.inspection_run_id.is_some() {
        completed.inspection_run_id = completion.inspection_run_id;
    }
    completed.recommendation_ids.clone_from(&completion.recommendation_ids);
    completed.freeze_id = completion.freeze_id;
    completed.kill_switch_suggested = completion.kill_switch_suggested;
    completed.result_code.clone_from(&completion.result_code);
    completed.sanitized_summary.clone_from(&completion.sanitized_summary);
    completed.completed_at = Some(completion.completed_at);
    completed
}

async fn update_preventive_run(
    transaction: &mut Transaction<'_, Postgres>,
    current: &PreventiveAutomationRun,
    next: &PreventiveAutomationRun,
) -> Result<(), ControlPlaneError> {
    let updated = sqlx::query(
        "UPDATE preventive_automation_runs
         SET status = $3,
             inspection_run_id = $4,
             recommendation_id = $5,
             freeze_id = $6,
             result_snapshot = $7,
             completed_at = $8,
             updated_at = $9
         WHERE id = $1 AND tenant_id = $2 AND status = $10",
    )
    .bind(next.id.as_uuid())
    .bind(next.tenant_id.as_uuid())
    .bind(automation_status_name(next.status))
    .bind(next.inspection_run_id.map(InspectionRunId::as_uuid))
    .bind(next.recommendation_ids.first().map(|id| id.as_uuid()))
    .bind(next.freeze_id)
    .bind(json_value(next)?)
    .bind(next.completed_at)
    .bind(next.completed_at.unwrap_or_else(Utc::now))
    .bind(automation_status_name(current.status))
    .execute(&mut **transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(ControlPlaneError::conflict_code(
            "preventive_transition_conflict",
            "preventive automation run changed concurrently",
        ));
    }
    Ok(())
}

async fn insert_preventive_event(
    transaction: &mut Transaction<'_, Postgres>,
    run: &PreventiveAutomationRun,
    from: Option<AutomationRunStatus>,
    to: AutomationRunStatus,
    reason_code: &str,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO automation_run_events (
            id, run_id, run_family, tenant_id, cluster_id, correlation_id,
            from_status, to_status, reason_code, event_snapshot, occurred_at
         ) VALUES (
            $1, $2, 'preventive', $3, $4, $5,
            $6, $7, $8, $9, $10
         )",
    )
    .bind(Uuid::new_v4())
    .bind(run.id.as_uuid())
    .bind(run.tenant_id.as_uuid())
    .bind(run.cluster_id.as_uuid())
    .bind(run.correlation_id.as_uuid())
    .bind(from.map(automation_status_name))
    .bind(automation_status_name(to))
    .bind(reason_code)
    .bind(serde_json::json!({
        "schema_version": AUTOMATION_SCHEMA_VERSION,
        "run_id": run.id,
        "risk_family": run.risk_family,
        "status": to,
        "result_code": run.result_code,
        "inspection_run_id": run.inspection_run_id,
    }))
    .bind(match to {
        AutomationRunStatus::Pending => run.started_at,
        AutomationRunStatus::Running => Utc::now(),
        AutomationRunStatus::Succeeded | AutomationRunStatus::Failed | AutomationRunStatus::Denied => {
            run.completed_at.unwrap_or_else(Utc::now)
        }
    })
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn same_request(left: &PreventiveAutomationRequest, right: &PreventiveAutomationRequest) -> bool {
    left.tenant_id == right.tenant_id
        && left.cluster_id == right.cluster_id
        && left.correlation_id == right.correlation_id
        && left.risk_family == right.risk_family
        && left.idempotency_key == right.idempotency_key
        && left.budget == right.budget
        && left.requested_by == right.requested_by
}

pub(super) const fn preventive_risk_name(risk: PreventiveRiskFamily) -> &'static str {
    match risk {
        PreventiveRiskFamily::Capacity => "capacity",
        PreventiveRiskFamily::Certificate => "certificate",
        PreventiveRiskFamily::Config => "config",
        PreventiveRiskFamily::Route => "route",
        PreventiveRiskFamily::Ha => "ha",
        PreventiveRiskFamily::Upgrade => "upgrade",
    }
}

const fn automation_status_name(status: AutomationRunStatus) -> &'static str {
    match status {
        AutomationRunStatus::Pending => "pending",
        AutomationRunStatus::Running => "running",
        AutomationRunStatus::Succeeded => "succeeded",
        AutomationRunStatus::Failed => "failed",
        AutomationRunStatus::Denied => "denied",
    }
}

fn json_value(value: &impl serde::Serialize) -> Result<Value, ControlPlaneError> {
    serde_json::to_value(value).map_err(|_| {
        ControlPlaneError::validation(
            "invalid_preventive_json",
            "preventive automation value is not valid JSON",
        )
    })
}

fn from_json<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, ControlPlaneError> {
    serde_json::from_value(value).map_err(|_| {
        ControlPlaneError::validation(
            "invalid_persisted_preventive_run",
            "persisted preventive automation data is incompatible",
        )
    })
}
