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

use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::ActionDescriptor;
use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::ChangeSchedule;
use rocketmq_sre_contracts::ChangeScheduleId;
use rocketmq_sre_contracts::ChangeScheduleStatus;
use rocketmq_sre_contracts::ChangeWindow;
use rocketmq_sre_contracts::ChangeWindowId;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::PlanStatus;
use rocketmq_sre_contracts::RunbookDefinition;
use rocketmq_sre_contracts::RunbookId;
use rocketmq_sre_contracts::RunbookStepBody;
use rocketmq_sre_contracts::RunbookStepId;
use rocketmq_sre_core::ActionCatalog;
use rocketmq_sre_core::ChangeCalendar;
use rocketmq_sre_core::EMBEDDED_ACTION_DESCRIPTOR_YAMLS;
use rocketmq_sre_core::RunbookValidator;
use serde_json::json;
use uuid::Uuid;

mod support;

pub(super) use support::audit_event;
use support::bounded_limit;
use support::effective_parallelism;
pub(super) use support::next_timestamp;
use support::require_cluster;
use support::require_role;
use support::runbook_resources;
pub(super) use support::schedule_event;
pub(super) use support::scheduler_auth;
use support::validate_reason;
use support::validate_schedule_window;
use support::validate_version;

use super::model::ChangeSchedulePage;
use super::model::ChangeSchedulePreview;
use super::model::ChangeWindowPage;
use super::model::CreateChangeScheduleRequest;
use super::model::CreateChangeWindowRequest;
use super::model::CreateRunbookRequest;
use super::model::ManualGateDecision;
use super::model::ManualGateDecisionRecord;
use super::model::RunbookPage;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::supervised_execution::SupervisedExecutionService;

#[derive(Clone)]
pub(crate) struct ChangeManagementService {
    pub(super) repository: PostgresRepository,
    pub(super) supervised_execution: SupervisedExecutionService,
    catalog: Arc<ActionCatalog>,
    clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
}

impl ChangeManagementService {
    pub(crate) fn new(
        repository: PostgresRepository,
        supervised_execution: SupervisedExecutionService,
    ) -> Result<Self, ControlPlaneError> {
        Self::new_with_clock(repository, supervised_execution, Arc::new(Utc::now))
    }

    fn new_with_clock(
        repository: PostgresRepository,
        supervised_execution: SupervisedExecutionService,
        clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
    ) -> Result<Self, ControlPlaneError> {
        let mut catalog = ActionCatalog::default();
        for yaml in EMBEDDED_ACTION_DESCRIPTOR_YAMLS {
            let descriptor: ActionDescriptor = serde_yaml::from_str(yaml).map_err(|_| {
                ControlPlaneError::configuration("embedded action descriptor cannot be parsed for runbook validation")
            })?;
            catalog.register(descriptor).map_err(|error| {
                ControlPlaneError::configuration(format!("embedded action descriptor is invalid: {error}"))
            })?;
        }
        Ok(Self {
            repository,
            supervised_execution,
            catalog: Arc::new(catalog),
            clock,
        })
    }

    pub(super) async fn create_runbook(
        &self,
        auth: &AuthContext,
        request: &CreateRunbookRequest,
        correlation_id: CorrelationId,
    ) -> Result<RunbookDefinition, ControlPlaneError> {
        require_role(auth, "operator")?;
        require_cluster(auth, request.cluster_id)?;
        let mut definition = request.definition.clone();
        definition.created_at = self.now();
        RunbookValidator::validate(&definition, &self.catalog)
            .map_err(|error| ControlPlaneError::validation("invalid_runbook", error.to_string()))?;
        let audit = audit_event(
            auth,
            request.cluster_id,
            correlation_id,
            AuditEventKind::RunbookCreated,
            "operator",
            "runbook",
            format!("{}@{}", definition.id, definition.version),
            "RunbookVersionCreated",
            json!({
                "runbook_id": definition.id,
                "version": definition.version,
                "risk": definition.risk,
                "step_count": definition.steps.len(),
            }),
            definition.created_at,
        );
        self.repository
            .store_runbook_definition(auth.tenant_id, request.cluster_id, &auth.subject, &definition, &audit)
            .await?;
        Ok(definition)
    }

    pub(super) async fn runbook(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        id: RunbookId,
        version: &str,
    ) -> Result<RunbookDefinition, ControlPlaneError> {
        require_cluster(auth, cluster_id)?;
        validate_version(version)?;
        self.repository
            .runbook_definition(auth.tenant_id, cluster_id, id, version)
            .await
    }

    pub(super) async fn runbooks(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        limit: Option<u32>,
    ) -> Result<RunbookPage, ControlPlaneError> {
        require_cluster(auth, cluster_id)?;
        let (query_limit, page_limit) = bounded_limit(limit)?;
        let mut items = self
            .repository
            .runbook_definitions(auth.tenant_id, cluster_id, query_limit)
            .await?;
        let partial = items.len() > page_limit;
        items.truncate(page_limit);
        Ok(RunbookPage {
            schema_version: "rocketmq-sre.runbook-page.v1",
            items,
            partial,
        })
    }

    pub(super) async fn create_window(
        &self,
        auth: &AuthContext,
        request: &CreateChangeWindowRequest,
        correlation_id: CorrelationId,
    ) -> Result<ChangeWindow, ControlPlaneError> {
        require_role(auth, "operator")?;
        require_cluster(auth, request.cluster_id)?;
        let now = self.now();
        request.timezone.parse::<chrono_tz::Tz>().map_err(|_| {
            ControlPlaneError::validation(
                "invalid_change_window",
                "timezone must be a valid IANA timezone identifier",
            )
        })?;
        let window = ChangeWindow {
            schema_version: ChangeWindow::SCHEMA_VERSION.to_owned(),
            id: ChangeWindowId::new(),
            tenant_id: auth.tenant_id,
            cluster_id: request.cluster_id,
            name: request.name.trim().to_owned(),
            kind: request.kind,
            timezone: request.timezone.trim().to_owned(),
            starts_at: request.starts_at,
            ends_at: request.ends_at,
            resource_keys: request.resource_keys.clone(),
            max_parallelism: request.max_parallelism,
            reason: request.reason.trim().to_owned(),
            created_by: auth.subject.clone(),
            created_at: now,
        };
        ChangeCalendar::validate_window(&window)
            .map_err(|error| ControlPlaneError::validation("invalid_change_window", error.to_string()))?;
        let audit = audit_event(
            auth,
            request.cluster_id,
            correlation_id,
            AuditEventKind::ChangeWindowCreated,
            "operator",
            "change_window",
            window.id.to_string(),
            "ChangeWindowCreated",
            json!({
                "window_id": window.id,
                "kind": window.kind,
                "starts_at": window.starts_at,
                "ends_at": window.ends_at,
                "resource_count": window.resource_keys.len(),
            }),
            now,
        );
        self.repository.store_change_window(&window, &audit).await?;
        Ok(window)
    }

    pub(super) async fn windows(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        limit: Option<u32>,
    ) -> Result<ChangeWindowPage, ControlPlaneError> {
        require_cluster(auth, cluster_id)?;
        if from >= to || to - from > Duration::days(366) {
            return Err(ControlPlaneError::validation(
                "invalid_time_range",
                "change window query must be a positive range no longer than 366 days",
            ));
        }
        let (query_limit, page_limit) = bounded_limit(limit)?;
        let mut items = self
            .repository
            .change_windows(auth.tenant_id, cluster_id, from, to, query_limit)
            .await?;
        let partial = items.len() > page_limit;
        items.truncate(page_limit);
        Ok(ChangeWindowPage {
            schema_version: "rocketmq-sre.change-window-page.v1",
            items,
            partial,
        })
    }

    pub(super) async fn preview_schedule(
        &self,
        auth: &AuthContext,
        request: &CreateChangeScheduleRequest,
        correlation_id: CorrelationId,
    ) -> Result<ChangeSchedulePreview, ControlPlaneError> {
        require_role(auth, "operator")?;
        require_cluster(auth, request.cluster_id)?;
        let definition = self
            .repository
            .runbook_definition(
                auth.tenant_id,
                request.cluster_id,
                request.runbook_id,
                &request.runbook_version,
            )
            .await?;
        RunbookValidator::validate(&definition, &self.catalog)
            .map_err(|error| ControlPlaneError::validation("invalid_runbook", error.to_string()))?;
        validate_schedule_window(self.now(), request.scheduled_start, request.scheduled_end)?;
        let resource_keys = runbook_resources(&definition);
        let now = self.now();
        let schedule = ChangeSchedule {
            schema_version: ChangeSchedule::SCHEMA_VERSION.to_owned(),
            id: ChangeScheduleId::new(),
            tenant_id: auth.tenant_id,
            cluster_id: request.cluster_id,
            correlation_id,
            runbook_id: request.runbook_id,
            runbook_version: request.runbook_version.clone(),
            plan_bindings: request.plan_bindings.clone(),
            scheduled_start: request.scheduled_start,
            scheduled_end: request.scheduled_end,
            resource_keys,
            status: ChangeScheduleStatus::Scheduled,
            intent_persisted: false,
            next_step_sequence: 1,
            active_execution_id: None,
            waiting_manual_gate: None,
            completed_steps: BTreeSet::new(),
            pause_requested_at: None,
            cancel_requested_at: None,
            created_by: auth.subject.clone(),
            created_at: now,
            updated_at: now,
        };
        ChangeCalendar::validate_schedule(&schedule)
            .map_err(|error| ControlPlaneError::validation("invalid_change_schedule", error.to_string()))?;
        RunbookValidator::validate_schedule_bindings(&definition, &schedule)
            .map_err(|error| ControlPlaneError::validation("invalid_plan_binding", error.to_string()))?;
        self.validate_bound_plans(auth, &definition, &schedule).await?;
        let windows = self
            .repository
            .change_windows(
                auth.tenant_id,
                request.cluster_id,
                request.scheduled_start,
                request.scheduled_end,
                257,
            )
            .await?;
        let existing = self
            .repository
            .conflicting_change_schedules(
                auth.tenant_id,
                request.cluster_id,
                request.scheduled_start,
                request.scheduled_end,
            )
            .await?;
        let conflicts = ChangeCalendar::conflicts(&schedule, definition.max_parallelism, &windows, &existing)
            .map_err(|error| ControlPlaneError::validation("invalid_change_schedule", error.to_string()))?;
        Ok(ChangeSchedulePreview {
            schema_version: "rocketmq-sre.change-schedule-preview.v1",
            schedulable: conflicts.is_empty(),
            schedule,
            conflicts,
        })
    }

    pub(super) async fn create_schedule(
        &self,
        auth: &AuthContext,
        request: &CreateChangeScheduleRequest,
        correlation_id: CorrelationId,
    ) -> Result<ChangeSchedule, ControlPlaneError> {
        let preview = self.preview_schedule(auth, request, correlation_id).await?;
        if !preview.conflicts.is_empty() {
            return Err(ControlPlaneError::conflict_code(
                "change_schedule_conflict",
                "change schedule has one or more blocking calendar conflicts",
            ));
        }
        let schedule = preview.schedule;
        let definition = self
            .repository
            .runbook_definition(
                auth.tenant_id,
                request.cluster_id,
                request.runbook_id,
                &request.runbook_version,
            )
            .await?;
        let windows = self
            .repository
            .change_windows(
                auth.tenant_id,
                request.cluster_id,
                request.scheduled_start,
                request.scheduled_end,
                257,
            )
            .await?;
        let allowed_parallelism = effective_parallelism(&definition, &schedule, &windows);
        let event = schedule_event(
            &schedule,
            None,
            "ScheduleCreated",
            &auth.subject,
            json!({"runbook_id": schedule.runbook_id, "runbook_version": schedule.runbook_version}),
        );
        let audit = audit_event(
            auth,
            schedule.cluster_id,
            schedule.correlation_id,
            AuditEventKind::ChangeScheduleCreated,
            "operator",
            "change_schedule",
            schedule.id.to_string(),
            "ScheduleCreated",
            json!({
                "schedule_id": schedule.id,
                "runbook_id": schedule.runbook_id,
                "starts_at": schedule.scheduled_start,
                "ends_at": schedule.scheduled_end,
            }),
            schedule.created_at,
        );
        self.repository
            .store_change_schedule(&schedule, allowed_parallelism, &event, &audit)
            .await?;
        Ok(schedule)
    }

    pub(super) async fn schedule(
        &self,
        auth: &AuthContext,
        id: ChangeScheduleId,
    ) -> Result<ChangeSchedule, ControlPlaneError> {
        let schedule = self.repository.change_schedule(auth.tenant_id, id).await?;
        require_cluster(auth, schedule.cluster_id)?;
        Ok(schedule)
    }

    pub(super) async fn schedules(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        status: Option<ChangeScheduleStatus>,
        limit: Option<u32>,
    ) -> Result<ChangeSchedulePage, ControlPlaneError> {
        require_cluster(auth, cluster_id)?;
        let (query_limit, page_limit) = bounded_limit(limit)?;
        let mut items = self
            .repository
            .change_schedules(auth.tenant_id, cluster_id, status, query_limit)
            .await?;
        let partial = items.len() > page_limit;
        items.truncate(page_limit);
        Ok(ChangeSchedulePage {
            schema_version: "rocketmq-sre.change-schedule-page.v1",
            items,
            partial,
        })
    }

    pub(super) async fn pause(
        &self,
        auth: &AuthContext,
        id: ChangeScheduleId,
        reason: &str,
    ) -> Result<ChangeSchedule, ControlPlaneError> {
        require_role(auth, "operator")?;
        self.transition(auth, id, reason, "SchedulePaused", |schedule, now| {
            ChangeCalendar::pause(schedule, now)
        })
        .await
    }

    pub(super) async fn resume(
        &self,
        auth: &AuthContext,
        id: ChangeScheduleId,
        reason: &str,
    ) -> Result<ChangeSchedule, ControlPlaneError> {
        require_role(auth, "operator")?;
        self.transition(auth, id, reason, "ScheduleResumed", |schedule, now| {
            ChangeCalendar::resume(schedule, now)
        })
        .await
    }

    pub(super) async fn cancel(
        &self,
        auth: &AuthContext,
        id: ChangeScheduleId,
        reason: &str,
    ) -> Result<ChangeSchedule, ControlPlaneError> {
        require_role(auth, "operator")?;
        self.transition(auth, id, reason, "ScheduleCancelledOrSafeStopping", |schedule, now| {
            ChangeCalendar::cancel(schedule, now)
        })
        .await
    }

    pub(super) async fn reconcile(
        &self,
        auth: &AuthContext,
        id: ChangeScheduleId,
        reason: &str,
    ) -> Result<ChangeSchedule, ControlPlaneError> {
        require_role(auth, "operator")?;
        self.transition(auth, id, reason, "ScheduleReconcileStarted", |schedule, now| {
            ChangeCalendar::begin_reconcile(schedule, now)
        })
        .await
    }

    pub(super) async fn decide_manual_gate(
        &self,
        auth: &AuthContext,
        schedule_id: ChangeScheduleId,
        step_id: RunbookStepId,
        decision: ManualGateDecision,
        reason: &str,
    ) -> Result<ChangeSchedule, ControlPlaneError> {
        require_role(auth, "approver")?;
        validate_reason(reason)?;
        let mut schedule = self.repository.change_schedule(auth.tenant_id, schedule_id).await?;
        require_cluster(auth, schedule.cluster_id)?;
        if schedule.created_by == auth.subject {
            return Err(ControlPlaneError::forbidden(
                "separation_of_duties_required",
                "schedule creator cannot decide its manual gate",
            ));
        }
        if schedule.status != ChangeScheduleStatus::AwaitingManualGate || schedule.waiting_manual_gate != Some(step_id)
        {
            return Err(ControlPlaneError::conflict_code(
                "manual_gate_not_active",
                "requested manual gate is not the active schedule gate",
            ));
        }
        let definition = self
            .repository
            .runbook_definition(
                auth.tenant_id,
                schedule.cluster_id,
                schedule.runbook_id,
                &schedule.runbook_version,
            )
            .await?;
        let required_role = definition
            .steps
            .iter()
            .find_map(|step| {
                if step.id == step_id
                    && let RunbookStepBody::ManualGate { gate } = &step.body
                {
                    Some(gate.required_role.as_str())
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                ControlPlaneError::conflict_code("manual_gate_not_active", "runbook manual gate no longer exists")
            })?;
        require_role(auth, required_role)?;
        let expected_updated_at = schedule.updated_at;
        let previous = schedule.status;
        let now = next_timestamp(schedule.updated_at, self.now());
        schedule.waiting_manual_gate = None;
        match decision {
            ManualGateDecision::Approved => {
                schedule.completed_steps.insert(step_id);
                schedule.next_step_sequence = schedule.next_step_sequence.saturating_add(1);
                schedule.status = if usize::from(schedule.next_step_sequence) > definition.steps.len() {
                    ChangeScheduleStatus::Completed
                } else {
                    ChangeScheduleStatus::Running
                };
            }
            ManualGateDecision::Rejected => {
                schedule.status = ChangeScheduleStatus::Rejected;
            }
        }
        schedule.updated_at = now;
        let decision_record = ManualGateDecisionRecord {
            id: Uuid::new_v4(),
            schedule_id,
            step_id,
            decision,
            actor_subject: auth.subject.clone(),
            actor_role: required_role.to_owned(),
            reason: reason.trim().to_owned(),
            occurred_at: now,
        };
        let event = schedule_event(
            &schedule,
            Some(previous),
            if decision == ManualGateDecision::Approved {
                "ManualGateApproved"
            } else {
                "ManualGateRejected"
            },
            &auth.subject,
            json!({"step_id": step_id, "decision": decision.as_str()}),
        );
        let audit = audit_event(
            auth,
            schedule.cluster_id,
            schedule.correlation_id,
            AuditEventKind::ManualGateDecided,
            required_role,
            "change_schedule",
            schedule.id.to_string(),
            event.reason_code.clone(),
            event.details.clone(),
            now,
        );
        self.repository
            .record_manual_gate_decision(&schedule, expected_updated_at, &decision_record, &event, &audit)
            .await?;
        Ok(schedule)
    }

    async fn transition<F>(
        &self,
        auth: &AuthContext,
        id: ChangeScheduleId,
        reason: &str,
        reason_code: &'static str,
        transition: F,
    ) -> Result<ChangeSchedule, ControlPlaneError>
    where
        F: FnOnce(&mut ChangeSchedule, DateTime<Utc>) -> Result<(), rocketmq_sre_core::ChangeCalendarError>,
    {
        validate_reason(reason)?;
        let mut schedule = self.repository.change_schedule(auth.tenant_id, id).await?;
        require_cluster(auth, schedule.cluster_id)?;
        let expected_status = schedule.status;
        let expected_updated_at = schedule.updated_at;
        let now = next_timestamp(schedule.updated_at, self.now());
        transition(&mut schedule, now)
            .map_err(|error| ControlPlaneError::conflict_code("invalid_schedule_transition", error.to_string()))?;
        let event = schedule_event(
            &schedule,
            Some(expected_status),
            reason_code,
            &auth.subject,
            json!({"reason": reason.trim()}),
        );
        let audit = audit_event(
            auth,
            schedule.cluster_id,
            schedule.correlation_id,
            AuditEventKind::ChangeScheduleStateChanged,
            "operator",
            "change_schedule",
            schedule.id.to_string(),
            reason_code,
            event.details.clone(),
            now,
        );
        self.repository
            .update_change_schedule(&schedule, expected_status, expected_updated_at, &event, &audit)
            .await?;
        Ok(schedule)
    }

    async fn validate_bound_plans(
        &self,
        auth: &AuthContext,
        definition: &RunbookDefinition,
        schedule: &ChangeSchedule,
    ) -> Result<(), ControlPlaneError> {
        for binding in &schedule.plan_bindings {
            let runbook_step = definition
                .steps
                .iter()
                .find(|step| step.id == binding.step_id)
                .ok_or_else(|| ControlPlaneError::validation("invalid_plan_binding", "runbook step does not exist"))?;
            let view = self.supervised_execution.plan(auth, binding.plan_id).await?;
            let plan = view.plan;
            if plan.status != PlanStatus::Approved
                || plan.tenant_id != auth.tenant_id
                || plan.cluster_id != schedule.cluster_id
                || plan.plan_hash != binding.plan_hash
                || plan.expires_at < schedule.scheduled_end
                || plan.steps.len() != 1
                || plan
                    .compute_precondition_hash()
                    .map_err(|error| ControlPlaneError::validation("invalid_plan_binding", error.to_string()))?
                    != binding.precondition_hash
            {
                return Err(ControlPlaneError::conflict_code(
                    "approved_plan_binding_required",
                    "each runbook action requires one current, approved, same-scope plan binding",
                ));
            }
            let plan_step = &plan.steps[0];
            let matches = match &runbook_step.body {
                RunbookStepBody::Action {
                    action,
                    descriptor_version,
                    resource,
                    parameters,
                } => {
                    plan_step.action == *action
                        && plan_step.descriptor_version == *descriptor_version
                        && plan_step.resource == *resource
                        && plan_step.parameters == *parameters
                }
                RunbookStepBody::ManualGate { .. } => false,
            };
            if !matches {
                return Err(ControlPlaneError::conflict_code(
                    "approved_plan_binding_required",
                    "approved plan content does not exactly match its runbook action step",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn now(&self) -> DateTime<Utc> {
        (self.clock)()
    }
}
