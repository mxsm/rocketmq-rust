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
use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::ChangeSchedule;
use rocketmq_sre_contracts::ChangeScheduleStatus;
use rocketmq_sre_contracts::ExecutionState;
use rocketmq_sre_contracts::RunbookDefinition;
use rocketmq_sre_contracts::RunbookStepBody;
use serde_json::json;

use super::service::ChangeManagementService;
use super::service::audit_event;
use super::service::next_timestamp;
use super::service::schedule_event;
use super::service::scheduler_auth;
use crate::ControlPlaneError;
use crate::supervised_execution::SubmitExecutionRequest;

const SCHEDULER_BATCH_SIZE: i64 = 64;

impl ChangeManagementService {
    /// Advances due schedules without overlapping runs. Each action dispatch
    /// still enters the existing approved Plan -> Executor -> Agent chain.
    pub(crate) async fn run_due(&self) {
        let now = self.now();
        let schedules = match self.repository.due_change_schedules(now, SCHEDULER_BATCH_SIZE).await {
            Ok(schedules) => schedules,
            Err(error) => {
                tracing::warn!(error = %error, "change scheduler could not load due schedules");
                return;
            }
        };
        for schedule in schedules {
            let schedule_id = schedule.id;
            if let Err(error) = self.tick_schedule(schedule).await {
                tracing::warn!(
                    schedule_id = %schedule_id,
                    error = %error,
                    "change scheduler left the schedule for a later retry"
                );
            }
        }
    }

    async fn tick_schedule(&self, schedule: ChangeSchedule) -> Result<(), ControlPlaneError> {
        match schedule.status {
            ChangeScheduleStatus::Scheduled | ChangeScheduleStatus::Running => {
                if schedule.scheduled_end <= self.now() {
                    return self.reject_expired_schedule(schedule).await;
                }
                if schedule.active_execution_id.is_some() {
                    self.observe_active_execution(schedule).await
                } else {
                    self.dispatch_next_step(schedule).await
                }
            }
            ChangeScheduleStatus::SafeStopping => self.advance_safe_stopping(schedule).await,
            ChangeScheduleStatus::Reconciling
            | ChangeScheduleStatus::AwaitingManualGate
            | ChangeScheduleStatus::Paused
            | ChangeScheduleStatus::Completed
            | ChangeScheduleStatus::Cancelled
            | ChangeScheduleStatus::Rejected => Ok(()),
        }
    }

    async fn dispatch_next_step(&self, mut schedule: ChangeSchedule) -> Result<(), ControlPlaneError> {
        let definition = self.definition_for_schedule(&schedule).await?;
        let expected_status = schedule.status;
        let expected_updated_at = schedule.updated_at;
        let next = definition
            .steps
            .iter()
            .find(|step| step.sequence == schedule.next_step_sequence);
        let Some(step) = next else {
            schedule.status = ChangeScheduleStatus::Completed;
            schedule.updated_at = next_timestamp(schedule.updated_at, self.now());
            return self
                .persist_scheduler_update(
                    schedule,
                    expected_status,
                    expected_updated_at,
                    "RunbookCompleted",
                    json!({}),
                )
                .await;
        };
        match &step.body {
            RunbookStepBody::ManualGate { .. } => {
                schedule.status = ChangeScheduleStatus::AwaitingManualGate;
                schedule.waiting_manual_gate = Some(step.id);
                schedule.updated_at = next_timestamp(schedule.updated_at, self.now());
                self.persist_scheduler_update(
                    schedule,
                    expected_status,
                    expected_updated_at,
                    "ManualGateAwaitingDecision",
                    json!({"step_id": step.id, "sequence": step.sequence}),
                )
                .await
            }
            RunbookStepBody::Action { .. } => {
                let binding = schedule
                    .plan_bindings
                    .iter()
                    .find(|binding| binding.step_id == step.id)
                    .ok_or_else(|| {
                        ControlPlaneError::conflict_code(
                            "approved_plan_binding_required",
                            "scheduled action has no immutable approved plan binding",
                        )
                    })?;
                let auth = scheduler_auth(schedule.tenant_id, schedule.cluster_id);
                let submission = self
                    .supervised_execution
                    .submit_execution(
                        &auth,
                        &SubmitExecutionRequest {
                            plan_id: binding.plan_id,
                            plan_hash: binding.plan_hash.clone(),
                            precondition_hash: binding.precondition_hash.clone(),
                            idempotency_key: format!("schedule:{}:step:{}", schedule.id, step.id),
                        },
                        schedule.correlation_id,
                    )
                    .await?;
                schedule.intent_persisted = true;
                schedule.active_execution_id = Some(submission.execution.id);
                schedule.status = ChangeScheduleStatus::Running;
                schedule.updated_at = next_timestamp(schedule.updated_at, self.now());
                self.persist_scheduler_update(
                    schedule,
                    expected_status,
                    expected_updated_at,
                    "RunbookStepDispatched",
                    json!({
                        "step_id": step.id,
                        "sequence": step.sequence,
                        "execution_id": submission.execution.id,
                    }),
                )
                .await
            }
        }
    }

    async fn observe_active_execution(&self, mut schedule: ChangeSchedule) -> Result<(), ControlPlaneError> {
        let execution_id = schedule.active_execution_id.ok_or_else(|| {
            ControlPlaneError::conflict_code(
                "invalid_schedule_projection",
                "running schedule has no active execution",
            )
        })?;
        let auth = scheduler_auth(schedule.tenant_id, schedule.cluster_id);
        let execution = self.supervised_execution.execution(&auth, execution_id).await?;
        match execution.state {
            ExecutionState::Succeeded => {
                let expected_status = schedule.status;
                let expected_updated_at = schedule.updated_at;
                let definition = self.definition_for_schedule(&schedule).await?;
                let completed = definition
                    .steps
                    .iter()
                    .find(|step| step.sequence == schedule.next_step_sequence)
                    .ok_or_else(|| {
                        ControlPlaneError::conflict_code(
                            "invalid_schedule_projection",
                            "active execution does not match a runbook step",
                        )
                    })?;
                schedule.completed_steps.insert(completed.id);
                schedule.next_step_sequence = schedule.next_step_sequence.saturating_add(1);
                schedule.active_execution_id = None;
                schedule.status = if usize::from(schedule.next_step_sequence) > definition.steps.len() {
                    ChangeScheduleStatus::Completed
                } else {
                    ChangeScheduleStatus::Running
                };
                schedule.updated_at = next_timestamp(schedule.updated_at, self.now());
                self.persist_scheduler_update(
                    schedule,
                    expected_status,
                    expected_updated_at,
                    "RunbookStepSucceeded",
                    json!({"step_id": completed.id, "execution_id": execution_id}),
                )
                .await
            }
            ExecutionState::RolledBack | ExecutionState::Escalated => {
                let expected_status = schedule.status;
                let expected_updated_at = schedule.updated_at;
                schedule.active_execution_id = None;
                schedule.status = ChangeScheduleStatus::Reconciling;
                schedule.updated_at = next_timestamp(schedule.updated_at, self.now());
                self.persist_scheduler_update(
                    schedule,
                    expected_status,
                    expected_updated_at,
                    "RunbookExecutionRequiresReconcile",
                    json!({"execution_id": execution_id, "execution_state": execution.state}),
                )
                .await
            }
            ExecutionState::Pending
            | ExecutionState::Prechecking
            | ExecutionState::IntentPersisted
            | ExecutionState::Applying
            | ExecutionState::Unknown
            | ExecutionState::Reconciling
            | ExecutionState::Verifying
            | ExecutionState::Compensating => Ok(()),
        }
    }

    async fn advance_safe_stopping(&self, mut schedule: ChangeSchedule) -> Result<(), ControlPlaneError> {
        if let Some(execution_id) = schedule.active_execution_id {
            let auth = scheduler_auth(schedule.tenant_id, schedule.cluster_id);
            let execution = self.supervised_execution.execution(&auth, execution_id).await?;
            if !matches!(
                execution.state,
                ExecutionState::Succeeded | ExecutionState::RolledBack | ExecutionState::Escalated
            ) {
                return Ok(());
            }
        }
        let expected_status = schedule.status;
        let expected_updated_at = schedule.updated_at;
        schedule.active_execution_id = None;
        schedule.status = ChangeScheduleStatus::Reconciling;
        schedule.updated_at = next_timestamp(schedule.updated_at, self.now());
        self.persist_scheduler_update(
            schedule,
            expected_status,
            expected_updated_at,
            "SafeStopReachedReconcile",
            json!({}),
        )
        .await
    }

    async fn reject_expired_schedule(&self, mut schedule: ChangeSchedule) -> Result<(), ControlPlaneError> {
        let expected_status = schedule.status;
        let expected_updated_at = schedule.updated_at;
        schedule.status = ChangeScheduleStatus::Rejected;
        schedule.updated_at = next_timestamp(schedule.updated_at, self.now());
        let details = json!({"scheduled_end": schedule.scheduled_end});
        self.persist_scheduler_update(
            schedule,
            expected_status,
            expected_updated_at,
            "ScheduleWindowExpired",
            details,
        )
        .await
    }

    async fn definition_for_schedule(&self, schedule: &ChangeSchedule) -> Result<RunbookDefinition, ControlPlaneError> {
        self.repository
            .runbook_definition(
                schedule.tenant_id,
                schedule.cluster_id,
                schedule.runbook_id,
                &schedule.runbook_version,
            )
            .await
    }

    async fn persist_scheduler_update(
        &self,
        schedule: ChangeSchedule,
        expected_status: ChangeScheduleStatus,
        expected_updated_at: chrono::DateTime<Utc>,
        reason_code: &'static str,
        details: serde_json::Value,
    ) -> Result<(), ControlPlaneError> {
        let auth = scheduler_auth(schedule.tenant_id, schedule.cluster_id);
        let event = schedule_event(&schedule, Some(expected_status), reason_code, &auth.subject, details);
        let audit = audit_event(
            &auth,
            schedule.cluster_id,
            schedule.correlation_id,
            AuditEventKind::ChangeScheduleStateChanged,
            "scheduler",
            "change_schedule",
            schedule.id.to_string(),
            reason_code,
            event.details.clone(),
            schedule.updated_at,
        );
        self.repository
            .update_change_schedule(&schedule, expected_status, expected_updated_at, &event, &audit)
            .await
    }
}
