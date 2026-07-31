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

use rocketmq_sre_contracts::ContractError;
use rocketmq_sre_contracts::ExecutionState;
use rocketmq_sre_contracts::ExecutionTransition;

/// Deterministic in-memory projection of append-only execution transitions.
#[derive(Clone, Debug)]
pub struct ExecutionStateMachine {
    current: ExecutionState,
    transitions: Vec<ExecutionTransition>,
}

impl Default for ExecutionStateMachine {
    fn default() -> Self {
        Self {
            current: ExecutionState::Pending,
            transitions: Vec::new(),
        }
    }
}

impl ExecutionStateMachine {
    /// Applies one legal transition and records it.
    ///
    /// # Errors
    ///
    /// Rejects illegal state graph edges.
    pub fn apply(&mut self, transition: ExecutionTransition) -> Result<&ExecutionTransition, ContractError> {
        if transition.from != self.current {
            return Err(ContractError::InvalidStateTransition {
                from: format!("{:?}", self.current),
                to: format!("{:?}", transition.to),
            });
        }
        transition.validate()?;
        self.current = transition.to;
        let appended_index = self.transitions.len();
        self.transitions.push(transition);
        Ok(&self.transitions[appended_index])
    }

    /// Returns the current derived state.
    #[must_use]
    pub const fn current(&self) -> ExecutionState {
        self.current
    }

    /// Returns immutable transition history.
    #[must_use]
    pub fn transitions(&self) -> &[ExecutionTransition] {
        &self.transitions
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;

    fn transition(from: ExecutionState, to: ExecutionState) -> ExecutionTransition {
        ExecutionTransition {
            from,
            to,
            reason_code: "test".to_owned(),
            occurred_at: Utc::now(),
        }
    }

    #[test]
    fn happy_path_reaches_succeeded_without_skipping_intent() {
        let mut machine = ExecutionStateMachine::default();
        for (from, to) in [
            (ExecutionState::Pending, ExecutionState::Prechecking),
            (ExecutionState::Prechecking, ExecutionState::IntentPersisted),
            (ExecutionState::IntentPersisted, ExecutionState::Applying),
            (ExecutionState::Applying, ExecutionState::Verifying),
            (ExecutionState::Verifying, ExecutionState::Succeeded),
        ] {
            machine.apply(transition(from, to)).expect("legal transition");
        }

        assert_eq!(machine.current(), ExecutionState::Succeeded);
        assert_eq!(machine.transitions().len(), 5);
    }

    #[test]
    fn stale_from_state_is_rejected_with_typed_error() {
        let mut machine = ExecutionStateMachine::default();

        assert!(matches!(
            machine.apply(transition(ExecutionState::Prechecking, ExecutionState::IntentPersisted)),
            Err(ContractError::InvalidStateTransition { .. })
        ));
    }
}
