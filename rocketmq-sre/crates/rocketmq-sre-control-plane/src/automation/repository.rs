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
use rocketmq_sre_contracts::AutomationOperatorFeedback;
use rocketmq_sre_contracts::AutomationRunId;
use rocketmq_sre_contracts::AutomationRunStatus;
use rocketmq_sre_contracts::NoSideEffectAutomationKind;
use rocketmq_sre_contracts::NoSideEffectAutomationRequest;
use rocketmq_sre_contracts::NoSideEffectAutomationRun;
use rocketmq_sre_contracts::TenantId;
use serde_json::Value;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use uuid::Uuid;

use super::model::AutomationRunListQuery;
use super::model::CompleteAutomationRunRequest;
use crate::ControlPlaneError;
use crate::PostgresRepository;

impl PostgresRepository {
    pub(super) async fn create_no_side_effect_run(
        &self,
        request: &NoSideEffectAutomationRequest,
    ) -> Result<NoSideEffectAutomationRun, ControlPlaneError> {
        request
            .validate()
            .map_err(|error| ControlPlaneError::validation("invalid_automation_request", error.to_string()))?;
        let run = NoSideEffectAutomationRun {
            schema_version: AUTOMATION_SCHEMA_VERSION.to_owned(),
            id: request.id,
            tenant_id: request.tenant_id,
            cluster_id: request.cluster_id,
            incident_id: request.incident_id,
            correlation_id: request.correlation_id,
            kind: request.kind,
            status: AutomationRunStatus::Pending,
            idempotency_key: request.idempotency_key.clone(),
            result_code: "automation_pending".to_owned(),
            sanitized_summary: "Bounded automation request accepted for processing".to_owned(),
            artifacts: Vec::new(),
            model_invocation_id: None,
            started_at: request.requested_at,
            completed_at: None,
        };
        run.validate()
            .map_err(|error| ControlPlaneError::validation("invalid_automation_run", error.to_string()))?;
        let mut transaction = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT INTO no_side_effect_automation_runs (
                id, tenant_id, cluster_id, incident_id, automation_kind,
                idempotency_key, status, result_snapshot, model_invocation_id,
                started_at, completed_at, correlation_id, budget_snapshot,
                request_snapshot, updated_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, 'pending', $7, NULL,
                $8, NULL, $9, $10,
                $11, $8
             )
             ON CONFLICT (tenant_id, automation_kind, idempotency_key)
             DO NOTHING",
        )
        .bind(run.id.as_uuid())
        .bind(run.tenant_id.as_uuid())
        .bind(run.cluster_id.map(rocketmq_sre_contracts::ClusterId::as_uuid))
        .bind(run.incident_id.map(rocketmq_sre_contracts::IncidentId::as_uuid))
        .bind(automation_kind_name(run.kind))
        .bind(&run.idempotency_key)
        .bind(json_value(&run)?)
        .bind(run.started_at)
        .bind(run.correlation_id.as_uuid())
        .bind(json_value(&request.budget)?)
        .bind(json_value(request)?)
        .execute(&mut *transaction)
        .await?;
        if inserted.rows_affected() == 1 {
            insert_run_event(
                &mut transaction,
                &run,
                None,
                AutomationRunStatus::Pending,
                "automation_accepted",
            )
            .await?;
            transaction.commit().await?;
            return Ok(run);
        }
        let row = sqlx::query(
            "SELECT request_snapshot, result_snapshot
             FROM no_side_effect_automation_runs
             WHERE tenant_id = $1 AND automation_kind = $2
               AND idempotency_key = $3",
        )
        .bind(request.tenant_id.as_uuid())
        .bind(automation_kind_name(request.kind))
        .bind(&request.idempotency_key)
        .fetch_one(&mut *transaction)
        .await?;
        let stored_request: NoSideEffectAutomationRequest = from_json(row.try_get("request_snapshot")?)?;
        if !same_request(&stored_request, request) {
            return Err(ControlPlaneError::conflict_code(
                "automation_idempotency_conflict",
                "automation idempotency key already binds different request content",
            ));
        }
        let stored = from_json(row.try_get("result_snapshot")?)?;
        transaction.commit().await?;
        Ok(stored)
    }

    pub(super) async fn complete_no_side_effect_run(
        &self,
        tenant_id: TenantId,
        run_id: AutomationRunId,
        completion: &CompleteAutomationRunRequest,
    ) -> Result<NoSideEffectAutomationRun, ControlPlaneError> {
        if !completion.status.is_terminal() {
            return Err(ControlPlaneError::validation(
                "invalid_automation_transition",
                "automation completion must use a terminal status",
            ));
        }
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT result_snapshot
             FROM no_side_effect_automation_runs
             WHERE id = $1 AND tenant_id = $2
             FOR UPDATE",
        )
        .bind(run_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let current: NoSideEffectAutomationRun = from_json(row.try_get("result_snapshot")?)?;
        if current.status.is_terminal() {
            let expected = completed_run(&current, completion);
            if current == expected {
                transaction.commit().await?;
                return Ok(current);
            }
            return Err(ControlPlaneError::conflict_code(
                "automation_completion_conflict",
                "terminal automation result is immutable",
            ));
        }
        let running = if current.status == AutomationRunStatus::Pending {
            let mut running = current.clone();
            running.status = AutomationRunStatus::Running;
            running.result_code = "automation_running".to_owned();
            running.sanitized_summary = "Bounded automation run is in progress".to_owned();
            update_run(&mut transaction, &current, &running).await?;
            insert_run_event(
                &mut transaction,
                &running,
                Some(AutomationRunStatus::Pending),
                AutomationRunStatus::Running,
                "automation_started",
            )
            .await?;
            running
        } else {
            current
        };
        let completed = completed_run(&running, completion);
        completed
            .validate()
            .map_err(|error| ControlPlaneError::validation("invalid_automation_result", error.to_string()))?;
        update_run(&mut transaction, &running, &completed).await?;
        insert_run_event(
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

    pub(super) async fn claim_no_side_effect_run(
        &self,
        tenant_id: TenantId,
        run_id: AutomationRunId,
    ) -> Result<(NoSideEffectAutomationRun, bool), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT result_snapshot
             FROM no_side_effect_automation_runs
             WHERE id = $1 AND tenant_id = $2
             FOR UPDATE",
        )
        .bind(run_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let current: NoSideEffectAutomationRun = from_json(row.try_get("result_snapshot")?)?;
        if current.status != AutomationRunStatus::Pending {
            transaction.commit().await?;
            return Ok((current, false));
        }
        let mut running = current.clone();
        running.status = AutomationRunStatus::Running;
        running.result_code = "automation_running".to_owned();
        running.sanitized_summary = "Bounded automation run is in progress".to_owned();
        update_run(&mut transaction, &current, &running).await?;
        insert_run_event(
            &mut transaction,
            &running,
            Some(AutomationRunStatus::Pending),
            AutomationRunStatus::Running,
            "automation_started",
        )
        .await?;
        transaction.commit().await?;
        Ok((running, true))
    }

    pub(super) async fn no_side_effect_runs(
        &self,
        tenant_id: TenantId,
        query: &AutomationRunListQuery,
        limit: i64,
    ) -> Result<Vec<NoSideEffectAutomationRun>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT result_snapshot
             FROM no_side_effect_automation_runs
             WHERE tenant_id = $1
               AND ($2::UUID IS NULL OR cluster_id = $2)
               AND ($3::UUID IS NULL OR incident_id = $3)
               AND ($4::TEXT IS NULL OR automation_kind = $4)
               AND ($5::TEXT IS NULL OR status = $5)
             ORDER BY started_at DESC, id DESC
             LIMIT $6",
        )
        .bind(tenant_id.as_uuid())
        .bind(query.cluster_id.map(rocketmq_sre_contracts::ClusterId::as_uuid))
        .bind(query.incident_id.map(rocketmq_sre_contracts::IncidentId::as_uuid))
        .bind(query.kind.map(automation_kind_name))
        .bind(query.status.map(automation_status_name))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| from_json(row.try_get("result_snapshot")?))
            .collect()
    }

    pub(super) async fn store_automation_feedback(
        &self,
        feedback: &AutomationOperatorFeedback,
    ) -> Result<AutomationOperatorFeedback, ControlPlaneError> {
        feedback
            .validate()
            .map_err(|error| ControlPlaneError::validation("invalid_automation_feedback", error.to_string()))?;
        sqlx::query(
            "INSERT INTO autonomy_operator_feedback (
                id, tenant_id, cluster_id, incident_id, subject_kind,
                subject_id, verdict, comment, actor_subject, created_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10
             )
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(feedback.id.as_uuid())
        .bind(feedback.tenant_id.as_uuid())
        .bind(feedback.cluster_id.map(rocketmq_sre_contracts::ClusterId::as_uuid))
        .bind(feedback.incident_id.map(rocketmq_sre_contracts::IncidentId::as_uuid))
        .bind(feedback_subject_name(feedback.subject))
        .bind(feedback.subject_id)
        .bind(feedback_verdict_name(feedback.verdict))
        .bind(&feedback.comment)
        .bind(&feedback.actor_subject)
        .bind(feedback.created_at)
        .execute(&self.pool)
        .await?;
        Ok(feedback.clone())
    }
}

fn completed_run(
    current: &NoSideEffectAutomationRun,
    completion: &CompleteAutomationRunRequest,
) -> NoSideEffectAutomationRun {
    let mut completed = current.clone();
    completed.status = completion.status;
    completed.result_code.clone_from(&completion.result_code);
    completed.sanitized_summary.clone_from(&completion.sanitized_summary);
    completed.artifacts.clone_from(&completion.artifacts);
    completed.model_invocation_id = completion.model_invocation_id;
    completed.completed_at = Some(completion.completed_at);
    completed
}

async fn update_run(
    transaction: &mut Transaction<'_, Postgres>,
    current: &NoSideEffectAutomationRun,
    next: &NoSideEffectAutomationRun,
) -> Result<(), ControlPlaneError> {
    let updated = sqlx::query(
        "UPDATE no_side_effect_automation_runs
         SET status = $3,
             result_snapshot = $4,
             model_invocation_id = $5,
             completed_at = $6,
             updated_at = $7
         WHERE id = $1 AND tenant_id = $2 AND status = $8",
    )
    .bind(next.id.as_uuid())
    .bind(next.tenant_id.as_uuid())
    .bind(automation_status_name(next.status))
    .bind(json_value(next)?)
    .bind(
        next.model_invocation_id
            .map(rocketmq_sre_contracts::ModelInvocationId::as_uuid),
    )
    .bind(next.completed_at)
    .bind(next.completed_at.unwrap_or_else(Utc::now))
    .bind(automation_status_name(current.status))
    .execute(&mut **transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(ControlPlaneError::conflict_code(
            "automation_transition_conflict",
            "automation run changed concurrently",
        ));
    }
    Ok(())
}

async fn insert_run_event(
    transaction: &mut Transaction<'_, Postgres>,
    run: &NoSideEffectAutomationRun,
    from: Option<AutomationRunStatus>,
    to: AutomationRunStatus,
    reason_code: &str,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO automation_run_events (
            id, run_id, run_family, tenant_id, cluster_id, correlation_id,
            from_status, to_status, reason_code, event_snapshot, occurred_at
         ) VALUES (
            $1, $2, 'no_side_effect', $3, $4, $5,
            $6, $7, $8, $9, $10
         )",
    )
    .bind(Uuid::new_v4())
    .bind(run.id.as_uuid())
    .bind(run.tenant_id.as_uuid())
    .bind(run.cluster_id.map(rocketmq_sre_contracts::ClusterId::as_uuid))
    .bind(run.correlation_id.as_uuid())
    .bind(from.map(automation_status_name))
    .bind(automation_status_name(to))
    .bind(reason_code)
    .bind(serde_json::json!({
        "schema_version": AUTOMATION_SCHEMA_VERSION,
        "run_id": run.id,
        "kind": run.kind,
        "status": to,
        "result_code": run.result_code,
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

fn same_request(left: &NoSideEffectAutomationRequest, right: &NoSideEffectAutomationRequest) -> bool {
    left.tenant_id == right.tenant_id
        && left.cluster_id == right.cluster_id
        && left.incident_id == right.incident_id
        && left.correlation_id == right.correlation_id
        && left.kind == right.kind
        && left.idempotency_key == right.idempotency_key
        && left.budget == right.budget
        && left.evidence_ids == right.evidence_ids
        && left.requested_by == right.requested_by
}

const fn automation_kind_name(kind: NoSideEffectAutomationKind) -> &'static str {
    match kind {
        NoSideEffectAutomationKind::AlertCorrelation => "alert_correlation",
        NoSideEffectAutomationKind::SeverityOwnerSuggestion => "severity_owner_suggestion",
        NoSideEffectAutomationKind::EvidenceCollection => "evidence_collection",
        NoSideEffectAutomationKind::ShiftSummary => "shift_summary",
        NoSideEffectAutomationKind::Notification => "notification",
        NoSideEffectAutomationKind::PostmortemDraft => "postmortem_draft",
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

const fn feedback_subject_name(subject: rocketmq_sre_contracts::AutomationFeedbackSubject) -> &'static str {
    match subject {
        rocketmq_sre_contracts::AutomationFeedbackSubject::Severity => "severity",
        rocketmq_sre_contracts::AutomationFeedbackSubject::Owner => "owner",
        rocketmq_sre_contracts::AutomationFeedbackSubject::Summary => "summary",
        rocketmq_sre_contracts::AutomationFeedbackSubject::Recommendation => "recommendation",
        rocketmq_sre_contracts::AutomationFeedbackSubject::Plan => "plan",
    }
}

const fn feedback_verdict_name(verdict: rocketmq_sre_contracts::AutomationFeedbackVerdict) -> &'static str {
    match verdict {
        rocketmq_sre_contracts::AutomationFeedbackVerdict::Correct => "correct",
        rocketmq_sre_contracts::AutomationFeedbackVerdict::Incorrect => "incorrect",
        rocketmq_sre_contracts::AutomationFeedbackVerdict::Useful => "useful",
        rocketmq_sre_contracts::AutomationFeedbackVerdict::NotUseful => "not_useful",
    }
}

fn json_value(value: &impl serde::Serialize) -> Result<Value, ControlPlaneError> {
    serde_json::to_value(value)
        .map_err(|_| ControlPlaneError::validation("invalid_automation_json", "automation value is not valid JSON"))
}

fn from_json<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, ControlPlaneError> {
    serde_json::from_value(value).map_err(|_| {
        ControlPlaneError::validation(
            "invalid_persisted_automation",
            "persisted automation data is incompatible",
        )
    })
}
