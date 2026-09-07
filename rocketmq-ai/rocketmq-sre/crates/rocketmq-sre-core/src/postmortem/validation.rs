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

use rocketmq_sre_contracts::SreContractError;
use std::collections::BTreeSet;

use rocketmq_sre_contracts::ActionItemStatus;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::PostmortemConclusion;

use super::PostmortemAssembly;

/// Validates bounded content and all material Evidence citations.
pub fn validate_revision(
    content: &PostmortemAssembly,
    allowed_evidence: &BTreeSet<EvidenceId>,
) -> Result<(), SreContractError> {
    for (_field, value, max) in [
        ("summary", content.summary.as_str(), 4_000),
        ("impact", content.impact.as_str(), 4_000),
        ("detection", content.detection.as_str(), 4_000),
        ("recovery", content.recovery.as_str(), 8_000),
    ] {
        if value.trim().is_empty() {
            return Err(rocketmq_sre_contracts::SreContractError::new(
                rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
            ));
        }
        if value.chars().count() > max {
            return Err(rocketmq_sre_contracts::SreContractError::new(
                rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
            ));
        }
    }
    if content.timeline.len() > 256 {
        return Err(rocketmq_sre_contracts::SreContractError::new(
            rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
        ));
    }
    for (_field, values, max) in [
        ("root_causes", content.root_causes.len(), 16),
        ("contributing_factors", content.contributing_factors.len(), 32),
        ("conclusions", content.conclusions.len(), 32),
        ("effective_actions", content.effective_actions.len(), 32),
        ("ineffective_actions", content.ineffective_actions.len(), 32),
        ("evidence_ids", content.evidence_ids.len(), 128),
    ] {
        if values > max {
            return Err(rocketmq_sre_contracts::SreContractError::new(
                rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
            ));
        }
    }
    for conclusion in content
        .root_causes
        .iter()
        .chain(&content.contributing_factors)
        .chain(&content.conclusions)
    {
        validate_conclusion(conclusion, allowed_evidence)?;
    }
    for evidence_id in &content.evidence_ids {
        if !allowed_evidence.contains(evidence_id) {
            return Err(rocketmq_sre_contracts::SreContractError::new(
                rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
            ));
        }
    }
    Ok(())
}

/// Enforces the human-owned action-item lifecycle.
pub fn validate_action_item_transition(
    current: ActionItemStatus,
    next: ActionItemStatus,
    owner: Option<&str>,
    verification: Option<&str>,
    evidence_ids: &[EvidenceId],
) -> Result<(), SreContractError> {
    let allowed = current == next
        || matches!(
            (current, next),
            (
                ActionItemStatus::Open | ActionItemStatus::Reopened,
                ActionItemStatus::Assigned
            ) | (
                ActionItemStatus::Open | ActionItemStatus::Assigned | ActionItemStatus::Reopened,
                ActionItemStatus::InProgress
            ) | (
                ActionItemStatus::Assigned | ActionItemStatus::InProgress,
                ActionItemStatus::Blocked
            ) | (
                ActionItemStatus::Assigned
                    | ActionItemStatus::InProgress
                    | ActionItemStatus::Blocked
                    | ActionItemStatus::Reopened,
                ActionItemStatus::Completed
            ) | (
                ActionItemStatus::Completed | ActionItemStatus::Blocked,
                ActionItemStatus::Reopened
            ) | (
                ActionItemStatus::Open
                    | ActionItemStatus::Assigned
                    | ActionItemStatus::Blocked
                    | ActionItemStatus::Reopened,
                ActionItemStatus::Cancelled
            )
        );
    if !allowed {
        return Err(rocketmq_sre_contracts::SreContractError::new(
            rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
        ));
    }
    if matches!(
        next,
        ActionItemStatus::Assigned | ActionItemStatus::InProgress | ActionItemStatus::Completed
    ) && owner.is_none_or(|owner| owner.trim().is_empty())
    {
        return Err(rocketmq_sre_contracts::SreContractError::new(
            rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
        ));
    }
    if next == ActionItemStatus::Completed
        && verification.is_none_or(|value| value.trim().is_empty())
        && evidence_ids.is_empty()
    {
        return Err(rocketmq_sre_contracts::SreContractError::new(
            rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
        ));
    }
    Ok(())
}

fn validate_conclusion(
    conclusion: &PostmortemConclusion,
    allowed_evidence: &BTreeSet<EvidenceId>,
) -> Result<(), SreContractError> {
    if conclusion.code.trim().is_empty() || conclusion.statement.trim().is_empty() {
        return Err(rocketmq_sre_contracts::SreContractError::new(
            rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
        ));
    }
    if conclusion.code.chars().count() > 128 || conclusion.statement.chars().count() > 2_000 {
        return Err(rocketmq_sre_contracts::SreContractError::new(
            rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
        ));
    }
    if conclusion.evidence_ids.is_empty() {
        return Err(rocketmq_sre_contracts::SreContractError::new(
            rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
        ));
    }
    for evidence_id in &conclusion.evidence_ids {
        if !allowed_evidence.contains(evidence_id) {
            return Err(rocketmq_sre_contracts::SreContractError::new(
                rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completing_an_action_requires_operator_verification() {
        assert_eq!(
            (validate_action_item_transition(
                ActionItemStatus::InProgress,
                ActionItemStatus::Completed,
                Some("owner"),
                None,
                &[],
            ))
            .unwrap_err()
            .code(),
            rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor
        );
        assert!(
            validate_action_item_transition(
                ActionItemStatus::InProgress,
                ActionItemStatus::Completed,
                Some("owner"),
                Some("verified in the next inspection"),
                &[],
            )
            .is_ok()
        );
    }

    #[test]
    fn completed_action_can_be_reopened_but_not_started_directly() {
        assert!(
            validate_action_item_transition(
                ActionItemStatus::Completed,
                ActionItemStatus::Reopened,
                Some("owner"),
                Some("verification"),
                &[],
            )
            .is_ok()
        );
        assert!(matches!(
            validate_action_item_transition(
                ActionItemStatus::Completed,
                ActionItemStatus::InProgress,
                Some("owner"),
                Some("verification"),
                &[],
            ),
            Err(error) if error.code() == rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor
        ));
    }
}
