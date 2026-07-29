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

use rocketmq_sre_contracts::ActionItemStatus;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::DrSubject;
use rocketmq_sre_contracts::RecoveryCheckpointDefinition;
use rocketmq_sre_contracts::RecoveryCheckpointStatus;

use crate::ControlPlaneError;
use crate::auth::AuthContext;
use crate::dr::model::CreateDrPlanRequest;
use crate::dr::model::RecordRecoveryCheckpointRequest;

const MAX_PLAN_CHECKPOINTS: usize = 64;
const MAX_REQUIRED_SOURCES: usize = 64;
const MAX_EVIDENCE_IDS: usize = 256;

pub(super) fn validate_plan_request(request: &CreateDrPlanRequest) -> Result<(), ControlPlaneError> {
    validate_text("DR plan name", &request.name, 256)?;
    validate_text("DR plan owner", &request.owner, 256)?;
    if request.version == 0 || request.target.rto_seconds == 0 {
        return Err(ControlPlaneError::validation(
            "invalid_dr_plan",
            "plan version and RTO must be greater than zero",
        ));
    }
    if request.subject == DrSubject::RocketMqCluster && request.cluster_id.is_none() {
        return Err(ControlPlaneError::validation(
            "cluster_required",
            "RocketMQ DR plans require a cluster scope",
        ));
    }
    if request.allowed_modes.is_empty() || request.checkpoints.is_empty() {
        return Err(ControlPlaneError::validation(
            "invalid_dr_plan",
            "DR plans require at least one mode and checkpoint",
        ));
    }
    if request.allowed_modes.len() > 3
        || request.allowed_modes.iter().copied().collect::<BTreeSet<_>>().len() != request.allowed_modes.len()
    {
        return Err(ControlPlaneError::validation(
            "invalid_dr_plan",
            "DR exercise modes must be unique and bounded",
        ));
    }
    if request.checkpoints.len() > MAX_PLAN_CHECKPOINTS || request.required_sources.len() > MAX_REQUIRED_SOURCES {
        return Err(ControlPlaneError::validation(
            "invalid_dr_plan",
            "DR plan sources or checkpoints exceed the supported bound",
        ));
    }
    let mut keys = BTreeSet::new();
    for checkpoint in &request.checkpoints {
        validate_text("checkpoint key", &checkpoint.key, 128)?;
        validate_text("checkpoint title", &checkpoint.title, 256)?;
        if !keys.insert(checkpoint.key.as_str()) {
            return Err(ControlPlaneError::validation(
                "duplicate_recovery_checkpoint",
                "DR checkpoint keys must be unique",
            ));
        }
    }
    for source in &request.required_sources {
        validate_text("required DR source", source, 128)?;
    }
    Ok(())
}

pub(super) fn validate_checkpoint(
    request: &RecordRecoveryCheckpointRequest,
    definition: &RecoveryCheckpointDefinition,
) -> Result<(), ControlPlaneError> {
    if request.key.trim() != definition.key
        || request.title.trim() != definition.title
        || request.expected_duration_seconds != definition.expected_duration_seconds
        || request.manual_confirmation_required != definition.manual_confirmation_required
        || request.cleanup_required != definition.cleanup_required
    {
        return Err(ControlPlaneError::validation(
            "recovery_checkpoint_definition_mismatch",
            "checkpoint fields do not match the immutable DR plan",
        ));
    }
    bound_evidence(&request.evidence_ids)?;
    if request.note.as_deref().is_some_and(|note| note.len() > 2_048) {
        return Err(ControlPlaneError::validation(
            "invalid_recovery_checkpoint",
            "checkpoint note exceeds 2048 bytes",
        ));
    }
    if request.completed_at.is_some_and(|completed| completed < request.started_at) {
        return Err(ControlPlaneError::validation(
            "invalid_recovery_checkpoint",
            "checkpoint completion precedes its start",
        ));
    }
    if request.status.is_terminal() {
        if request.completed_at.is_none() || request.actual_duration_seconds.is_none() {
            return Err(ControlPlaneError::validation(
                "invalid_recovery_checkpoint",
                "terminal checkpoints require completion time and actual duration",
            ));
        }
        if request.status != RecoveryCheckpointStatus::Skipped && request.evidence_ids.is_empty() {
            return Err(ControlPlaneError::validation(
                "dr_evidence_required",
                "passed or failed checkpoints require Evidence",
            ));
        }
    }
    if request.status == RecoveryCheckpointStatus::Failed && request.finding_codes.is_empty() {
        return Err(ControlPlaneError::validation(
            "dr_finding_required",
            "failed checkpoints require at least one finding code",
        ));
    }
    if request.manual_confirmation_required
        && request.confirmed_by.as_deref().is_none_or(str::is_empty)
        && request.status != RecoveryCheckpointStatus::ManualConfirmationRequired
    {
        return Err(ControlPlaneError::validation(
            "manual_confirmation_required",
            "this checkpoint requires an explicit human confirmation",
        ));
    }
    if request.cleanup_complete && !request.cleanup_required {
        return Err(ControlPlaneError::validation(
            "invalid_recovery_checkpoint",
            "cleanup cannot be complete when the plan does not require cleanup",
        ));
    }
    Ok(())
}

pub(super) fn validate_checkpoint_transition(
    current: RecoveryCheckpointStatus,
    next: RecoveryCheckpointStatus,
) -> Result<(), ControlPlaneError> {
    let allowed = matches!(
        (current, next),
        (RecoveryCheckpointStatus::Pending, RecoveryCheckpointStatus::Running)
            | (
                RecoveryCheckpointStatus::Pending | RecoveryCheckpointStatus::Running,
                RecoveryCheckpointStatus::Passed
                    | RecoveryCheckpointStatus::Failed
                    | RecoveryCheckpointStatus::ManualConfirmationRequired
                    | RecoveryCheckpointStatus::Skipped
            )
            | (
                RecoveryCheckpointStatus::ManualConfirmationRequired,
                RecoveryCheckpointStatus::Passed | RecoveryCheckpointStatus::Failed
            )
    );
    if allowed {
        Ok(())
    } else {
        Err(ControlPlaneError::conflict_code(
            "invalid_recovery_checkpoint_transition",
            "the requested checkpoint transition is not allowed",
        ))
    }
}

pub(super) fn action_item_transition_allowed(current: ActionItemStatus, next: ActionItemStatus) -> bool {
    current == next
        || matches!(
            (current, next),
            (
                ActionItemStatus::Open,
                ActionItemStatus::Assigned
                    | ActionItemStatus::InProgress
                    | ActionItemStatus::Blocked
                    | ActionItemStatus::Cancelled
            )
                | (
                    ActionItemStatus::Assigned,
                    ActionItemStatus::InProgress | ActionItemStatus::Blocked | ActionItemStatus::Cancelled
                )
                | (
                    ActionItemStatus::InProgress,
                    ActionItemStatus::Blocked | ActionItemStatus::Completed | ActionItemStatus::Cancelled
                )
                | (
                    ActionItemStatus::Blocked,
                    ActionItemStatus::InProgress | ActionItemStatus::Cancelled
                )
                | (ActionItemStatus::Completed, ActionItemStatus::Reopened)
                | (
                    ActionItemStatus::Reopened,
                    ActionItemStatus::InProgress | ActionItemStatus::Completed | ActionItemStatus::Cancelled
                )
        )
}

pub(super) fn require_read(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if auth.roles.iter().any(|role| {
        matches!(
            role.as_str(),
            "diagnose" | "operator" | "approver" | "rocketmq:diagnose"
        )
    }) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "DR Center requires diagnose or operator access",
        ))
    }
}

pub(super) fn require_operator(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if auth.roles.contains("operator") {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "DR Center writes require the operator role",
        ))
    }
}

pub(super) fn require_cluster(
    auth: &AuthContext,
    cluster_id: Option<ClusterId>,
) -> Result<(), ControlPlaneError> {
    if cluster_id.is_none_or(|cluster_id| auth.clusters.contains(&cluster_id)) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "the authenticated identity cannot access this DR cluster",
        ))
    }
}

pub(super) fn validate_text(name: &str, value: &str, max: usize) -> Result<(), ControlPlaneError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.len() > max {
        return Err(ControlPlaneError::validation(
            "invalid_dr_request",
            format!("{name} must contain between 1 and {max} bytes"),
        ));
    }
    Ok(())
}

pub(super) fn bound_evidence(
    evidence_ids: &[rocketmq_sre_contracts::EvidenceId],
) -> Result<(), ControlPlaneError> {
    if evidence_ids.len() > MAX_EVIDENCE_IDS {
        return Err(ControlPlaneError::validation(
            "invalid_dr_request",
            "DR Evidence references exceed the supported bound",
        ));
    }
    Ok(())
}
