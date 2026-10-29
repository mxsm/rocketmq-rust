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
    AuditFacts, AuditFailureCode, AuditOutcome, ConsumerFailureCode, ConsumerFailureStage, TopicFailureCode,
    TopicFailureStage,
};

use super::{
    audit::{MutationAudit, rejected_conflict, target_outcome},
    brokers::BrokerConfigMutationResult,
    consumers::{ConsumerConfigMutationResult, ConsumerMutationResult},
    topics::{TopicFieldsPatchResult, TopicMutationResult},
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

impl MutationAudit for TopicFieldsPatchResult {
    fn audit(&self) -> (AuditOutcome, AuditFacts) {
        let outcome = &self.edit.outcome;
        let applied = outcome.applied_count();
        let unconfirmed = outcome
            .targets
            .iter()
            .filter(|target| {
                target.failure == Some(TopicFailureCode::Unconfirmed)
                    || (!target.applied && target.stage == TopicFailureStage::Mutation && target.failure.is_none())
            })
            .count();
        // A known persistence failure may retain applied in-memory effects; only the explicit
        // Unconfirmed result represents uncertainty about execution or durability.
        let known_failure = outcome.targets.iter().any(|target| {
            target.stage == TopicFailureStage::Mutation && target.failure == Some(TopicFailureCode::Unavailable)
        });
        let result = if applied > 0 {
            if outcome.failed_count() > 0 {
                AuditOutcome::Partial
            } else {
                AuditOutcome::Succeeded
            }
        } else if unconfirmed > 0 {
            AuditOutcome::Unknown
        } else if known_failure {
            AuditOutcome::Failed
        } else {
            AuditOutcome::Rejected
        };
        let (_, mut facts) = target_outcome(applied, unconfirmed, outcome.reload_failed);
        facts.failure_code = if unconfirmed > 0 {
            Some(AuditFailureCode::Unknown)
        } else if outcome
            .targets
            .iter()
            .any(|target| target.failure == Some(TopicFailureCode::Unavailable))
        {
            Some(AuditFailureCode::Connection)
        } else if outcome
            .targets
            .iter()
            .any(|target| target.failure == Some(TopicFailureCode::Conflict))
        {
            Some(AuditFailureCode::Conflict)
        } else if outcome.targets.iter().any(|target| {
            matches!(
                target.failure,
                Some(TopicFailureCode::InvalidData | TopicFailureCode::NotFound)
            )
        }) {
            Some(AuditFailureCode::Validation)
        } else {
            None
        };
        (result, facts)
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
