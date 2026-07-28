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

use std::error::Error;
use std::fmt;

use rocketmq_sre_contracts::AutonomyLifecycleState;
use rocketmq_sre_contracts::AutonomyMode;
use rocketmq_sre_contracts::SreTimestamp;

/// Authority class attempting an autonomy lifecycle transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AutonomyActor {
    HumanOperator,
    SafetyReconciler,
    Model,
}

/// Qualification facts required for an operator promotion.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PromotionQualification {
    pub shadow_qualified: bool,
    pub autonomous_qualified: bool,
    pub critic_ready: bool,
    pub owner_confirmed: bool,
}

/// Fail-closed lifecycle transition error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AutonomyTransitionError {
    ModelAuthorityForbidden,
    HumanAuthorityRequired,
    InvalidTransition,
    QualificationMissing,
    CriticNotReady,
    OwnerConfirmationRequired,
    PauseReasonRequired,
    RevisionExhausted,
}

impl fmt::Display for AutonomyTransitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ModelAuthorityForbidden => formatter.write_str("models cannot change autonomy lifecycle state"),
            Self::HumanAuthorityRequired => formatter.write_str("autonomy lifecycle transition requires a human operator"),
            Self::InvalidTransition => formatter.write_str("autonomy lifecycle transition is not allowed"),
            Self::QualificationMissing => formatter.write_str("autonomy promotion qualification is incomplete"),
            Self::CriticNotReady => formatter.write_str("autonomous promotion requires a valid heterogeneous critic"),
            Self::OwnerConfirmationRequired => {
                formatter.write_str("autonomous promotion requires action owner confirmation")
            }
            Self::PauseReasonRequired => formatter.write_str("paused autonomy requires a bounded reason"),
            Self::RevisionExhausted => formatter.write_str("autonomy lifecycle revision is exhausted"),
        }
    }
}

impl Error for AutonomyTransitionError {}

/// Deterministic operator-controlled autonomy lifecycle.
pub struct AutonomyStateMachine;

impl AutonomyStateMachine {
    /// Returns the next state without changing policy or cohort identity.
    ///
    /// # Errors
    ///
    /// Rejects model-initiated changes, skipped promotions, direct
    /// Paused-to-Autonomous recovery, missing qualification, and unbounded
    /// pause reasons.
    pub fn transition(
        current: &AutonomyLifecycleState,
        target: AutonomyMode,
        actor: AutonomyActor,
        actor_subject: &str,
        pause_reason: Option<&str>,
        qualification: PromotionQualification,
        updated_at: SreTimestamp,
    ) -> Result<AutonomyLifecycleState, AutonomyTransitionError> {
        if actor == AutonomyActor::Model {
            return Err(AutonomyTransitionError::ModelAuthorityForbidden);
        }
        if current.mode == target {
            return Err(AutonomyTransitionError::InvalidTransition);
        }
        let reason = pause_reason.map(str::trim).filter(|value| !value.is_empty());
        if reason.is_some_and(|value| value.chars().count() > 512) {
            return Err(AutonomyTransitionError::PauseReasonRequired);
        }
        let human = actor == AutonomyActor::HumanOperator;
        match (current.mode, target) {
            (_, AutonomyMode::Disabled) if human => {}
            (AutonomyMode::Disabled, AutonomyMode::Shadow) if human => {}
            (AutonomyMode::Shadow, AutonomyMode::Supervised) if human => {
                if !qualification.shadow_qualified {
                    return Err(AutonomyTransitionError::QualificationMissing);
                }
                if !qualification.owner_confirmed {
                    return Err(AutonomyTransitionError::OwnerConfirmationRequired);
                }
            }
            (AutonomyMode::Supervised, AutonomyMode::Autonomous) if human => {
                if !qualification.critic_ready {
                    return Err(AutonomyTransitionError::CriticNotReady);
                }
                if !qualification.autonomous_qualified {
                    return Err(AutonomyTransitionError::QualificationMissing);
                }
                if !qualification.owner_confirmed {
                    return Err(AutonomyTransitionError::OwnerConfirmationRequired);
                }
            }
            (
                AutonomyMode::Shadow | AutonomyMode::Supervised | AutonomyMode::Autonomous,
                AutonomyMode::Paused,
            ) if reason.is_some() => {}
            (AutonomyMode::Paused, AutonomyMode::Shadow | AutonomyMode::Supervised) if human => {}
            (_, AutonomyMode::Disabled | AutonomyMode::Shadow | AutonomyMode::Supervised | AutonomyMode::Autonomous)
                if !human =>
            {
                return Err(AutonomyTransitionError::HumanAuthorityRequired);
            }
            _ => return Err(AutonomyTransitionError::InvalidTransition),
        }

        let lifecycle_revision = current
            .lifecycle_revision
            .checked_add(1)
            .ok_or(AutonomyTransitionError::RevisionExhausted)?;
        let paused = target == AutonomyMode::Paused;
        Ok(AutonomyLifecycleState {
            tenant_id: current.tenant_id,
            cluster_id: current.cluster_id,
            action: current.action,
            mode: target,
            previous_mode: paused.then_some(current.mode),
            owner: current.owner.clone(),
            pause_reason: paused.then(|| reason.unwrap_or_default().to_owned()),
            lifecycle_revision,
            updated_by: actor_subject.to_owned(),
            updated_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::ExecutionAction;
    use rocketmq_sre_contracts::TenantId;

    use super::*;

    fn state(mode: AutonomyMode) -> AutonomyLifecycleState {
        AutonomyLifecycleState {
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            action: ExecutionAction::ObservabilityLoggerLevelTtl,
            mode,
            previous_mode: None,
            owner: "messaging-observability".to_owned(),
            pause_reason: None,
            lifecycle_revision: 1,
            updated_by: "bootstrap".to_owned(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn model_cannot_change_lifecycle() {
        assert_eq!(
            AutonomyStateMachine::transition(
                &state(AutonomyMode::Disabled),
                AutonomyMode::Shadow,
                AutonomyActor::Model,
                "model",
                None,
                PromotionQualification::default(),
                chrono::Utc::now(),
            ),
            Err(AutonomyTransitionError::ModelAuthorityForbidden)
        );
    }

    #[test]
    fn promotions_are_sequential_and_qualified() {
        let qualification = PromotionQualification {
            shadow_qualified: true,
            autonomous_qualified: true,
            critic_ready: true,
            owner_confirmed: true,
        };
        let shadow = AutonomyStateMachine::transition(
            &state(AutonomyMode::Disabled),
            AutonomyMode::Shadow,
            AutonomyActor::HumanOperator,
            "operator",
            None,
            qualification,
            chrono::Utc::now(),
        )
        .expect("shadow");
        let supervised = AutonomyStateMachine::transition(
            &shadow,
            AutonomyMode::Supervised,
            AutonomyActor::HumanOperator,
            "operator",
            None,
            qualification,
            chrono::Utc::now(),
        )
        .expect("supervised");
        assert_eq!(supervised.mode, AutonomyMode::Supervised);
        assert_eq!(supervised.lifecycle_revision, 3);
    }

    #[test]
    fn paused_state_recovers_only_to_safe_modes() {
        let paused = AutonomyStateMachine::transition(
            &state(AutonomyMode::Autonomous),
            AutonomyMode::Paused,
            AutonomyActor::SafetyReconciler,
            "pause-reconciler",
            Some("verification_failed"),
            PromotionQualification::default(),
            chrono::Utc::now(),
        )
        .expect("pause");
        assert_eq!(paused.previous_mode, Some(AutonomyMode::Autonomous));
        assert!(
            AutonomyStateMachine::transition(
                &paused,
                AutonomyMode::Autonomous,
                AutonomyActor::HumanOperator,
                "operator",
                None,
                PromotionQualification {
                    autonomous_qualified: true,
                    critic_ready: true,
                    owner_confirmed: true,
                    ..PromotionQualification::default()
                },
                chrono::Utc::now(),
            )
            .is_err()
        );
        assert!(
            AutonomyStateMachine::transition(
                &paused,
                AutonomyMode::Shadow,
                AutonomyActor::HumanOperator,
                "operator",
                None,
                PromotionQualification::default(),
                chrono::Utc::now(),
            )
            .is_ok()
        );
    }
}
