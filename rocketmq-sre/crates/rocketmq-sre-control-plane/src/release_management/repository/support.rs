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

use rocketmq_sre_contracts::AuditEvent;
use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::IntegrationAdapterKind;
use rocketmq_sre_contracts::IntegrationDeliveryStatus;
use rocketmq_sre_contracts::IntegrationEventKind;
use rocketmq_sre_contracts::ReleaseObservationPhase;
use rocketmq_sre_contracts::ReleaseStatus;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::Postgres;
use sqlx::Transaction;

use super::super::model::ReleaseEventRecord;
use crate::ControlPlaneError;

pub(super) const fn adapter_kind_name(kind: IntegrationAdapterKind) -> &'static str {
    match kind {
        IntegrationAdapterKind::MockItsm => "mock_itsm",
        IntegrationAdapterKind::SignedWebhookItsm => "signed_webhook_itsm",
        IntegrationAdapterKind::ChatOpsWebhook => "chatops_webhook",
        IntegrationAdapterKind::Pager => "pager",
        IntegrationAdapterKind::Email => "email",
    }
}

pub(super) fn parse_adapter_kind(value: &str) -> Result<IntegrationAdapterKind, ControlPlaneError> {
    match value {
        "mock_itsm" => Ok(IntegrationAdapterKind::MockItsm),
        "signed_webhook_itsm" => Ok(IntegrationAdapterKind::SignedWebhookItsm),
        "chatops_webhook" => Ok(IntegrationAdapterKind::ChatOpsWebhook),
        "pager" => Ok(IntegrationAdapterKind::Pager),
        "email" => Ok(IntegrationAdapterKind::Email),
        _ => Err(invalid_persisted("integration adapter kind")),
    }
}

pub(super) const fn integration_event_name(kind: IntegrationEventKind) -> &'static str {
    match kind {
        IntegrationEventKind::PlanSubmitted => "plan_submitted",
        IntegrationEventKind::ApprovalChanged => "approval_changed",
        IntegrationEventKind::ReleaseStarted => "release_started",
        IntegrationEventKind::ReleasePaused => "release_paused",
        IntegrationEventKind::ReleaseRollingBack => "release_rolling_back",
        IntegrationEventKind::ReleaseCompleted => "release_completed",
        IntegrationEventKind::ManualTakeoverRequired => "manual_takeover_required",
    }
}

pub(super) fn parse_integration_event(value: &str) -> Result<IntegrationEventKind, ControlPlaneError> {
    match value {
        "plan_submitted" => Ok(IntegrationEventKind::PlanSubmitted),
        "approval_changed" => Ok(IntegrationEventKind::ApprovalChanged),
        "release_started" => Ok(IntegrationEventKind::ReleaseStarted),
        "release_paused" => Ok(IntegrationEventKind::ReleasePaused),
        "release_rolling_back" => Ok(IntegrationEventKind::ReleaseRollingBack),
        "release_completed" => Ok(IntegrationEventKind::ReleaseCompleted),
        "manual_takeover_required" => Ok(IntegrationEventKind::ManualTakeoverRequired),
        _ => Err(invalid_persisted("integration event kind")),
    }
}

pub(super) fn parse_delivery_status(value: &str) -> Result<IntegrationDeliveryStatus, ControlPlaneError> {
    match value {
        "pending" => Ok(IntegrationDeliveryStatus::Pending),
        "delivering" => Ok(IntegrationDeliveryStatus::Delivering),
        "delivered" => Ok(IntegrationDeliveryStatus::Delivered),
        "retry_scheduled" => Ok(IntegrationDeliveryStatus::RetryScheduled),
        "failed" => Ok(IntegrationDeliveryStatus::Failed),
        _ => Err(invalid_persisted("integration delivery status")),
    }
}

pub(super) const fn release_status_name(status: ReleaseStatus) -> &'static str {
    match status {
        ReleaseStatus::Planned => "planned",
        ReleaseStatus::ReadinessChecking => "readiness_checking",
        ReleaseStatus::Ready => "ready",
        ReleaseStatus::CanaryRunning => "canary_running",
        ReleaseStatus::Paused => "paused",
        ReleaseStatus::Verifying => "verifying",
        ReleaseStatus::RollingBack => "rolling_back",
        ReleaseStatus::RolledBack => "rolled_back",
        ReleaseStatus::Completed => "completed",
        ReleaseStatus::ManualTakeover => "manual_takeover",
        ReleaseStatus::Failed => "failed",
    }
}

pub(super) fn parse_release_status(value: &str) -> Result<ReleaseStatus, ControlPlaneError> {
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
        _ => Err(invalid_persisted("release status")),
    }
}

pub(super) const fn observation_phase_name(phase: ReleaseObservationPhase) -> &'static str {
    match phase {
        ReleaseObservationPhase::Before => "before",
        ReleaseObservationPhase::During => "during",
        ReleaseObservationPhase::After => "after",
    }
}

pub(super) async fn insert_audit(
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

pub(super) async fn insert_release_event(
    transaction: &mut Transaction<'_, Postgres>,
    event: &ReleaseEventRecord,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO release_events (
            event_id, release_id, correlation_id, from_status, to_status,
            reason_code, actor_subject, details, occurred_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(event.id)
    .bind(event.release_id.as_uuid())
    .bind(event.correlation_id.as_uuid())
    .bind(event.from_status.map(release_status_name))
    .bind(release_status_name(event.to_status))
    .bind(&event.reason_code)
    .bind(&event.actor_subject)
    .bind(&event.details)
    .bind(event.occurred_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(super) fn json_value<T: Serialize>(value: &T) -> Result<Value, ControlPlaneError> {
    serde_json::to_value(value)
        .map_err(|_| ControlPlaneError::validation("invalid_request", "value cannot be represented as JSON"))
}

pub(super) fn from_json<T: DeserializeOwned>(value: Value) -> Result<T, ControlPlaneError> {
    serde_json::from_value(value).map_err(|_| invalid_persisted("JSON snapshot"))
}

fn audit_event_kind_name(kind: AuditEventKind) -> &'static str {
    match kind {
        AuditEventKind::PlanCreated => "plan_created",
        AuditEventKind::PlanSubmitted => "plan_submitted",
        AuditEventKind::PolicyEvaluated => "policy_evaluated",
        AuditEventKind::CriticReviewed => "critic_reviewed",
        AuditEventKind::Approved => "approved",
        AuditEventKind::Rejected => "rejected",
        AuditEventKind::ExecutionSubmitted => "execution_submitted",
        AuditEventKind::StateChanged => "state_changed",
        AuditEventKind::StepIntentPersisted => "step_intent_persisted",
        AuditEventKind::StepResultPersisted => "step_result_persisted",
        AuditEventKind::VerificationCaptured => "verification_captured",
        AuditEventKind::VerificationCompleted => "verification_completed",
        AuditEventKind::RollbackStarted => "rollback_started",
        AuditEventKind::ManualTakeoverRequired => "manual_takeover_required",
        AuditEventKind::QuarantineCreated => "quarantine_created",
        AuditEventKind::QuarantineClearRequested => "quarantine_clear_requested",
        AuditEventKind::QuarantineCleared => "quarantine_cleared",
        AuditEventKind::Cancelled => "cancelled",
        AuditEventKind::RunbookCreated => "runbook_created",
        AuditEventKind::ChangeWindowCreated => "change_window_created",
        AuditEventKind::ChangeScheduleCreated => "change_schedule_created",
        AuditEventKind::ChangeScheduleStateChanged => "change_schedule_state_changed",
        AuditEventKind::ManualGateDecided => "manual_gate_decided",
        AuditEventKind::IntegrationTargetRegistered => "integration_target_registered",
        AuditEventKind::IntegrationDeliveryQueued => "integration_delivery_queued",
        AuditEventKind::IntegrationDeliveryCompleted => "integration_delivery_completed",
        AuditEventKind::ExternalApprovalReceived => "external_approval_received",
        AuditEventKind::ReleaseCreated => "release_created",
        AuditEventKind::ReleaseReadinessEvaluated => "release_readiness_evaluated",
        AuditEventKind::ReleaseStateChanged => "release_state_changed",
        AuditEventKind::ReleaseObservationCaptured => "release_observation_captured",
        AuditEventKind::ReleaseReportGenerated => "release_report_generated",
    }
}

fn invalid_persisted(name: &str) -> ControlPlaneError {
    ControlPlaneError::validation(
        "invalid_persisted_state",
        format!("persisted {name} is incompatible with this service version"),
    )
}
