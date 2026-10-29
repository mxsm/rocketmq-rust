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
    AuditConsumerCleanupFacts, AuditFacts, AuditFailureCode, AuditOutcome, ConsumerFailureCode, TopicFailureCode,
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
        let counts = outcome.counts();
        let unconfirmed = counts.broker_targets.unconfirmed + counts.cleanup_steps.unconfirmed;
        let not_applied = counts.broker_targets.not_applied + counts.cleanup_steps.not_applied;
        let result = if outcome.has_applied_effects() {
            if unconfirmed + not_applied > 0 {
                AuditOutcome::Partial
            } else {
                AuditOutcome::Succeeded
            }
        } else if unconfirmed > 0 {
            AuditOutcome::Unknown
        } else {
            AuditOutcome::Rejected
        };
        let facts = AuditFacts {
            applied_targets: u32::try_from(counts.broker_targets.applied).unwrap_or(u32::MAX),
            unconfirmed_targets: u32::try_from(counts.broker_targets.unconfirmed).unwrap_or(u32::MAX),
            refresh_failed: outcome.reload_failed,
            consumer_cleanup: (counts.cleanup_steps.total() > 0).then(|| AuditConsumerCleanupFacts {
                applied_steps: u32::try_from(counts.cleanup_steps.applied).unwrap_or(u32::MAX),
                failed_steps: u32::try_from(counts.cleanup_steps.not_applied).unwrap_or(u32::MAX),
                unconfirmed_steps: u32::try_from(counts.cleanup_steps.unconfirmed).unwrap_or(u32::MAX),
            }),
            failure_code: if unconfirmed > 0 {
                Some(AuditFailureCode::Unknown)
            } else {
                outcome
                    .targets
                    .iter()
                    .filter(|target| !target.applied)
                    .find_map(|target| match target.failure {
                        Some(ConsumerFailureCode::NotFound | ConsumerFailureCode::InvalidData) => {
                            Some(AuditFailureCode::Validation)
                        }
                        Some(ConsumerFailureCode::Unsupported) => Some(AuditFailureCode::CapabilityUnavailable),
                        Some(ConsumerFailureCode::Conflict) => Some(AuditFailureCode::Conflict),
                        Some(ConsumerFailureCode::Authorization) => Some(AuditFailureCode::Authentication),
                        Some(ConsumerFailureCode::Unavailable) => Some(AuditFailureCode::Connection),
                        Some(ConsumerFailureCode::NotApplied) | None => None,
                    })
            },
            ..Default::default()
        };
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
            Self::PersistenceIncomplete {
                applied, persistence, ..
            } => {
                use rocketmq_dashboard_common::ConsumerConfigPersistence;
                let (outcome, failure_code, unconfirmed_targets) = match (applied, persistence) {
                    (true, ConsumerConfigPersistence::Failed) => {
                        (AuditOutcome::Partial, AuditFailureCode::Connection, 0)
                    }
                    (false, ConsumerConfigPersistence::Failed) => {
                        (AuditOutcome::Failed, AuditFailureCode::Connection, 0)
                    }
                    (true, ConsumerConfigPersistence::Unconfirmed) => {
                        (AuditOutcome::Partial, AuditFailureCode::Unknown, 1)
                    }
                    (false, ConsumerConfigPersistence::Unconfirmed) => {
                        (AuditOutcome::Unknown, AuditFailureCode::Unknown, 1)
                    }
                };
                (
                    outcome,
                    AuditFacts {
                        applied_targets: u32::from(*applied),
                        unconfirmed_targets,
                        failure_code: Some(failure_code),
                        ..Default::default()
                    },
                )
            }
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
