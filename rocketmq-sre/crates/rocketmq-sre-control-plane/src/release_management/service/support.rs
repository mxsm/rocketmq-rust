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
use rocketmq_sre_contracts::AuditEventId;
use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::INTEGRATION_DELIVERY_SCHEMA_VERSION;
use rocketmq_sre_contracts::IntegrationDelivery;
use rocketmq_sre_contracts::IntegrationDeliveryId;
use rocketmq_sre_contracts::IntegrationDeliveryStatus;
use rocketmq_sre_contracts::IntegrationEventKind;
use rocketmq_sre_contracts::ReleaseStatus;
use rocketmq_sre_contracts::ReleaseWorkflow;
use rocketmq_sre_core::IntegrationValidator;
use rocketmq_sre_core::ReleaseStateMachine;
use rocketmq_sre_core::ReleaseValidator;
use serde_json::Value;
use serde_json::json;
use uuid::Uuid;

use super::ReleaseManagementService;
use crate::ControlPlaneError;
use crate::auth::AuthContext;
use crate::release_management::descriptors::resolve_descriptor;
use crate::release_management::model::QueuedIntegrationDelivery;
use crate::release_management::model::ReleaseEventRecord;

const MAX_INTEGRATION_TARGETS: usize = 256;

pub(super) struct ReleaseTransition {
    pub(super) workflow: ReleaseWorkflow,
    pub(super) event: ReleaseEventRecord,
    pub(super) audit: AuditEvent,
}

impl ReleaseManagementService {
    pub(super) async fn outbound_deliveries(
        &self,
        workflow: &ReleaseWorkflow,
        event_kind: IntegrationEventKind,
        sanitized_summary: &str,
        actor: &AuthContext,
    ) -> Result<Vec<QueuedIntegrationDelivery>, ControlPlaneError> {
        validate_bounded_text("integration summary", sanitized_summary, 2_048)?;
        reject_sensitive(sanitized_summary)?;
        let targets = self
            .repository
            .integration_targets(
                workflow.tenant_id,
                workflow.cluster_id,
                None,
                Some(true),
                i64::try_from(MAX_INTEGRATION_TARGETS + 1).unwrap_or(i64::MAX),
            )
            .await?;
        if targets.len() > MAX_INTEGRATION_TARGETS {
            return Err(ControlPlaneError::conflict_code(
                "integration_target_limit_exceeded",
                "release has more than 256 enabled integration targets",
            ));
        }
        let mut queued = Vec::new();
        for target in targets {
            if !target.target.outbound_events.contains(&event_kind) {
                continue;
            }
            if target.target.cluster_id != Some(workflow.cluster_id) {
                return Err(ControlPlaneError::forbidden(
                    "integration_scope_mismatch",
                    "integration target does not match the release cluster",
                ));
            }
            let descriptor = resolve_descriptor(
                &target.target.descriptor_id,
                &target.target.descriptor_version,
                target.target.adapter_kind,
            )
            .ok_or_else(|| {
                ControlPlaneError::conflict_code(
                    "integration_descriptor_mismatch",
                    "integration target references an unknown descriptor version",
                )
            })?;
            IntegrationValidator::validate_target(&target.target, &descriptor)
                .map_err(|error| ControlPlaneError::validation("integration_target_invalid", error.to_string()))?;
            let id = IntegrationDeliveryId::new();
            let delivery = IntegrationDelivery {
                schema_version: INTEGRATION_DELIVERY_SCHEMA_VERSION.to_owned(),
                id,
                target_id: target.target.id,
                descriptor_id: descriptor.id.clone(),
                descriptor_version: descriptor.version.clone(),
                tenant_id: workflow.tenant_id,
                cluster_id: workflow.cluster_id,
                incident_id: workflow.incident_id,
                plan_id: Some(workflow.plan_id),
                release_id: Some(workflow.id),
                event_kind,
                idempotency_key: format!(
                    "release:{}:{}:{}",
                    workflow.id,
                    integration_event_name(event_kind),
                    workflow.updated_at.timestamp_micros()
                ),
                sanitized_summary: sanitized_summary.trim().to_owned(),
                deep_link: format!("/changes/releases/{}", workflow.id),
                status: IntegrationDeliveryStatus::Pending,
                attempt_count: 0,
                next_attempt_at: Some(workflow.updated_at),
                last_error_code: None,
                delivered_at: None,
                created_at: workflow.updated_at,
            };
            IntegrationValidator::validate_delivery(&delivery, &target.target, &descriptor)
                .map_err(|error| ControlPlaneError::validation("integration_delivery_invalid", error.to_string()))?;
            queued.push(QueuedIntegrationDelivery {
                target,
                audit: audit_event(
                    actor,
                    workflow.cluster_id,
                    workflow.correlation_id,
                    AuditEventKind::IntegrationDeliveryQueued,
                    "integration_delivery",
                    id.to_string(),
                    "IntegrationDeliveryQueued",
                    json!({
                        "release_id": workflow.id,
                        "event_kind": event_kind,
                    }),
                    workflow.updated_at,
                ),
                delivery,
            });
        }
        Ok(queued)
    }
}

pub(super) fn transition_release(
    current: &ReleaseWorkflow,
    to: ReleaseStatus,
    auth: &AuthContext,
    reason_code: &str,
    reason: &str,
    details: Value,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> Result<ReleaseTransition, ControlPlaneError> {
    require_cluster(auth, current.cluster_id)?;
    validate_reason(reason)?;
    ReleaseStateMachine::transition(current.status, to)
        .map_err(|error| ControlPlaneError::conflict_code("release_state_invalid", error.to_string()))?;
    let mut workflow = current.clone();
    workflow.status = to;
    workflow.updated_at = occurred_at;
    if matches!(
        to,
        ReleaseStatus::Paused | ReleaseStatus::ManualTakeover | ReleaseStatus::Failed
    ) {
        workflow.pause_reason = Some(reason.trim().to_owned());
    } else if current.status == ReleaseStatus::Paused && to == ReleaseStatus::CanaryRunning {
        workflow.pause_reason = None;
    }
    ReleaseValidator::validate_workflow(&workflow)
        .map_err(|error| ControlPlaneError::validation("release_invalid", error.to_string()))?;
    Ok(ReleaseTransition {
        event: ReleaseEventRecord {
            id: Uuid::new_v4(),
            release_id: current.id,
            correlation_id: current.correlation_id,
            from_status: Some(current.status),
            to_status: to,
            reason_code: reason_code.to_owned(),
            actor_subject: auth.subject.clone(),
            details: details.clone(),
            occurred_at,
        },
        audit: audit_event(
            auth,
            current.cluster_id,
            current.correlation_id,
            AuditEventKind::ReleaseStateChanged,
            "release",
            current.id.to_string(),
            reason_code,
            json!({
                "from": current.status,
                "to": to,
                "reason": reason.trim(),
                "details": details,
            }),
            occurred_at,
        ),
        workflow,
    })
}

pub(super) fn audit_event(
    auth: &AuthContext,
    cluster_id: ClusterId,
    correlation_id: rocketmq_sre_contracts::CorrelationId,
    event_kind: AuditEventKind,
    resource_kind: &str,
    resource_id: String,
    reason_code: &str,
    details: Value,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> AuditEvent {
    AuditEvent {
        id: AuditEventId::new(),
        tenant_id: auth.tenant_id,
        cluster_id,
        correlation_id,
        event_kind,
        actor_subject: auth.subject.clone(),
        actor_role: actor_role(auth).to_owned(),
        resource_kind: resource_kind.to_owned(),
        resource_id,
        reason_code: reason_code.to_owned(),
        details,
        occurred_at,
    }
}

pub(super) fn require_operator(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    require_role(auth, "operator")
}

pub(super) fn require_approver(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    require_role(auth, "approver")
}

pub(super) fn require_cluster(auth: &AuthContext, cluster_id: ClusterId) -> Result<(), ControlPlaneError> {
    if !auth.clusters.contains(&cluster_id) {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "release cluster is outside the authenticated scope",
        ));
    }
    Ok(())
}

pub(super) fn validate_reason(value: &str) -> Result<(), ControlPlaneError> {
    validate_bounded_text("reason", value, 2_048)?;
    reject_sensitive(value)
}

pub(super) fn validate_bounded_text(name: &str, value: &str, max_chars: usize) -> Result<(), ControlPlaneError> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > max_chars || value.chars().any(char::is_control) {
        return Err(ControlPlaneError::validation(
            "invalid_request",
            format!("{name} must be non-empty, bounded, and contain no control characters"),
        ));
    }
    Ok(())
}

pub(super) fn reject_sensitive(value: &str) -> Result<(), ControlPlaneError> {
    let normalized = value.to_ascii_lowercase();
    if [
        "token=",
        "secret=",
        "password=",
        "authorization:",
        "private key",
        "message body",
        "message_body",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
    {
        return Err(ControlPlaneError::validation(
            "sensitive_data_rejected",
            "release content contains prohibited sensitive material",
        ));
    }
    Ok(())
}

fn require_role(auth: &AuthContext, role: &'static str) -> Result<(), ControlPlaneError> {
    if !auth.roles.contains(role) {
        return Err(ControlPlaneError::forbidden(
            "role_required",
            format!("release operation requires the {role} role"),
        ));
    }
    Ok(())
}

fn actor_role(auth: &AuthContext) -> &'static str {
    if auth.roles.contains("approver") {
        "approver"
    } else if auth.roles.contains("operator") {
        "operator"
    } else {
        "authenticated"
    }
}

const fn integration_event_name(event: IntegrationEventKind) -> &'static str {
    match event {
        IntegrationEventKind::PlanSubmitted => "plan_submitted",
        IntegrationEventKind::ApprovalChanged => "approval_changed",
        IntegrationEventKind::ReleaseStarted => "release_started",
        IntegrationEventKind::ReleasePaused => "release_paused",
        IntegrationEventKind::ReleaseRollingBack => "release_rolling_back",
        IntegrationEventKind::ReleaseCompleted => "release_completed",
        IntegrationEventKind::ManualTakeoverRequired => "manual_takeover_required",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use chrono::Duration;
    use chrono::Utc;
    use rocketmq_sre_contracts::ActionPlanId;
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::CorrelationId;
    use rocketmq_sre_contracts::IncidentId;
    use rocketmq_sre_contracts::ReleaseId;
    use rocketmq_sre_contracts::RunbookId;
    use rocketmq_sre_contracts::TenantId;

    use super::*;

    fn auth(cluster_id: ClusterId) -> AuthContext {
        AuthContext {
            tenant_id: TenantId::new(),
            subject: "operator-a".to_owned(),
            clusters: BTreeSet::from([cluster_id]),
            roles: BTreeSet::from(["operator".to_owned()]),
        }
    }

    fn workflow(cluster_id: ClusterId) -> ReleaseWorkflow {
        let now = Utc::now();
        let auth = auth(cluster_id);
        ReleaseWorkflow {
            schema_version: "rocketmq-sre.release-workflow.v1".to_owned(),
            id: ReleaseId::new(),
            tenant_id: auth.tenant_id,
            cluster_id,
            incident_id: IncidentId::new(),
            correlation_id: CorrelationId::new(),
            change_id: "CHG-1001".to_owned(),
            release_ref: "release-2026.07.28".to_owned(),
            target_version: "5.3.0".to_owned(),
            runbook_id: RunbookId::new(),
            runbook_version: "1.0.0".to_owned(),
            plan_id: ActionPlanId::new(),
            plan_hash: format!("sha256:{}", "a".repeat(64)),
            rollback_plan_id: None,
            rollback_plan_hash: None,
            readiness: None,
            status: ReleaseStatus::CanaryRunning,
            active_execution_id: None,
            regression_detected: false,
            pause_reason: None,
            created_by: auth.subject,
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn release_transition_cannot_skip_or_exit_terminal_state() {
        let cluster_id = ClusterId::new();
        let auth = auth(cluster_id);
        let release = workflow(cluster_id);
        assert!(
            transition_release(
                &release,
                ReleaseStatus::Paused,
                &auth,
                "CanaryRegression",
                "SLO regression detected",
                json!({}),
                release.updated_at + Duration::seconds(1),
            )
            .is_ok()
        );
        assert!(
            transition_release(
                &release,
                ReleaseStatus::Completed,
                &auth,
                "Skipped",
                "skip verification",
                json!({}),
                release.updated_at + Duration::seconds(1),
            )
            .is_err()
        );
    }
}
