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

//! Explicit business-result projections; authoritative reload failures remain separate facts.

use rocketmq_dashboard_common::{
    AuditFacts, AuditOutcome, ConsumerFailureCode, ConsumerFailureStage, TopicFailureCode, TopicFailureStage,
};

use super::{
    audit::{MutationAudit, rejected_conflict, target_outcome},
    brokers::BrokerConfigMutationResult,
    consumers::{ConsumerConfigMutationResult, ConsumerMutationResult},
    topics::{TopicMutationResult, TopicQueuePatchResult},
};

impl MutationAudit for TopicMutationResult {
    fn audit(&self) -> (AuditOutcome, AuditFacts) {
        let outcome = match self {
            Self::Rejected(outcome) | Self::Applied { outcome, .. } | Self::AppliedReloadFailed { outcome, .. } => {
                outcome
            }
        };
        let (mut result, facts) =
            target_outcome(outcome.applied_count(), outcome.failed_count(), outcome.reload_failed);
        let may_have_executed = outcome.targets.iter().any(|target| {
            !target.applied
                && target.stage == TopicFailureStage::Mutation
                && matches!(target.failure, Some(TopicFailureCode::Unavailable) | None)
        });
        if outcome.applied_count() == 0 && !may_have_executed {
            result = AuditOutcome::Rejected;
        }
        (result, facts)
    }
}

impl MutationAudit for ConsumerMutationResult {
    fn audit(&self) -> (AuditOutcome, AuditFacts) {
        let outcome = match self {
            Self::Rejected(outcome) | Self::Applied { outcome, .. } | Self::AppliedReloadFailed { outcome, .. } => {
                outcome
            }
        };
        let (mut result, facts) =
            target_outcome(outcome.applied_count(), outcome.failed_count(), outcome.reload_failed);
        let may_have_executed = outcome.targets.iter().any(|target| {
            !target.applied
                && matches!(
                    target.stage,
                    ConsumerFailureStage::Mutation | ConsumerFailureStage::Cleanup
                )
                && matches!(target.failure, Some(ConsumerFailureCode::Unavailable) | None)
        });
        if outcome.applied_count() == 0 && !may_have_executed {
            result = AuditOutcome::Rejected;
        }
        (result, facts)
    }
}

impl MutationAudit for TopicQueuePatchResult {
    fn audit(&self) -> (AuditOutcome, AuditFacts) {
        match self {
            Self::VersionConflict { .. } => rejected_conflict(),
            Self::Applied { .. } => target_outcome(1, 0, false),
            Self::AppliedReloadFailed { .. } => target_outcome(1, 0, true),
        }
    }
}

impl MutationAudit for ConsumerConfigMutationResult {
    fn audit(&self) -> (AuditOutcome, AuditFacts) {
        match self {
            Self::GenerationConflict { .. } => rejected_conflict(),
            Self::Applied { .. } => target_outcome(1, 0, false),
            Self::AppliedReloadFailed { .. } => target_outcome(1, 0, true),
        }
    }
}

impl MutationAudit for BrokerConfigMutationResult {
    fn audit(&self) -> (AuditOutcome, AuditFacts) {
        match self {
            Self::GenerationConflict { .. } => rejected_conflict(),
            Self::Applied { .. } => target_outcome(1, 0, false),
            Self::AppliedReloadFailed { .. } => target_outcome(1, 0, true),
        }
    }
}
