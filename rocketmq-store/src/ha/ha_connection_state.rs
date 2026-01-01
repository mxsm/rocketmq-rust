// Copyright 2023 The RocketMQ Rust Authors
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

/// Connection states for High Availability service
///
/// Represents the different states a connection can be in during the HA replication process.
use std::fmt;

use serde::Deserialize;
use serde::Serialize;

/// High Availability (HA) connection state enumeration
/// Represents the various states of a connection in RocketMQ's HA mechanism
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HAConnectionState {
    /// Ready to start connection
    Ready,
    /// CommitLog consistency checking
    Handshake,
    /// Synchronizing data
    Transfer,
    /// Temporarily stop transferring
    Suspend,
    /// Connection shutdown
    Shutdown,
}

impl HAConnectionState {
    /// Get all possible connection states
    pub fn all_states() -> Vec<HAConnectionState> {
        vec![
            HAConnectionState::Ready,
            HAConnectionState::Handshake,
            HAConnectionState::Transfer,
            HAConnectionState::Suspend,
            HAConnectionState::Shutdown,
        ]
    }

    /// Check if the connection state is active (can perform operations)
    pub fn is_active(&self) -> bool {
        matches!(self, HAConnectionState::Transfer)
    }

    /// Check if the connection state is inactive (cannot perform operations)
    pub fn is_inactive(&self) -> bool {
        matches!(self, HAConnectionState::Suspend | HAConnectionState::Shutdown)
    }

    /// Check if the connection is in a transitional state
    pub fn is_transitional(&self) -> bool {
        matches!(self, HAConnectionState::Ready | HAConnectionState::Handshake)
    }

    /// Check if the connection is terminated
    pub fn is_terminated(&self) -> bool {
        matches!(self, HAConnectionState::Shutdown)
    }

    /// Check if the connection can transition to the given state
    pub fn can_transition_to(&self, target: HAConnectionState) -> bool {
        use HAConnectionState::*;

        match (self, target) {
            // From Ready
            (Ready, Handshake) => true,
            (Ready, Shutdown) => true,

            // From Handshake
            (Handshake, Transfer) => true,
            (Handshake, Shutdown) => true,

            // From Transfer
            (Transfer, Suspend) => true,
            (Transfer, Shutdown) => true,

            // From Suspend
            (Suspend, Transfer) => true,
            (Suspend, Shutdown) => true,

            // From Shutdown (terminal state)
            (Shutdown, _) => false,

            // Same state
            (state, target) if state == &target => true,

            // Invalid transitions
            _ => false,
        }
    }

    /// Get the next logical state in the connection lifecycle
    pub fn next_state(&self) -> Option<HAConnectionState> {
        match self {
            HAConnectionState::Ready => Some(HAConnectionState::Handshake),
            HAConnectionState::Handshake => Some(HAConnectionState::Transfer),
            HAConnectionState::Transfer => None, // Can go to Suspend or Shutdown
            HAConnectionState::Suspend => Some(HAConnectionState::Transfer),
            HAConnectionState::Shutdown => None, // Terminal state
        }
    }

    /// Get a human-readable description of the state
    pub fn description(&self) -> &'static str {
        match self {
            HAConnectionState::Ready => "Ready to start connection",
            HAConnectionState::Handshake => "CommitLog consistency checking",
            HAConnectionState::Transfer => "Synchronizing data",
            HAConnectionState::Suspend => "Temporarily stop transferring",
            HAConnectionState::Shutdown => "Connection shutdown",
        }
    }

    /// Convert to string representation (lowercase)
    pub fn as_str(&self) -> &'static str {
        match self {
            HAConnectionState::Ready => "ready",
            HAConnectionState::Handshake => "handshake",
            HAConnectionState::Transfer => "transfer",
            HAConnectionState::Suspend => "suspend",
            HAConnectionState::Shutdown => "shutdown",
        }
    }

    /// Convert to string representation (uppercase)
    pub fn as_upper_str(&self) -> &'static str {
        match self {
            HAConnectionState::Ready => "READY",
            HAConnectionState::Handshake => "HANDSHAKE",
            HAConnectionState::Transfer => "TRANSFER",
            HAConnectionState::Suspend => "SUSPEND",
            HAConnectionState::Shutdown => "SHUTDOWN",
        }
    }
}

impl fmt::Display for HAConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_upper_str())
    }
}

impl From<&str> for HAConnectionState {
    fn from(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "READY" => HAConnectionState::Ready,
            "HANDSHAKE" => HAConnectionState::Handshake,
            "TRANSFER" => HAConnectionState::Transfer,
            "SUSPEND" => HAConnectionState::Suspend,
            "SHUTDOWN" => HAConnectionState::Shutdown,
            _ => HAConnectionState::Ready, // Default to Ready for unknown states
        }
    }
}

impl From<String> for HAConnectionState {
    fn from(s: String) -> Self {
        HAConnectionState::from(s.as_str())
    }
}

impl From<HAConnectionState> for String {
    fn from(state: HAConnectionState) -> Self {
        state.as_upper_str().to_string()
    }
}

impl From<HAConnectionState> for i32 {
    fn from(state: HAConnectionState) -> Self {
        match state {
            HAConnectionState::Ready => 0,
            HAConnectionState::Handshake => 1,
            HAConnectionState::Transfer => 2,
            HAConnectionState::Suspend => 3,
            HAConnectionState::Shutdown => 4,
        }
    }
}

impl TryFrom<i32> for HAConnectionState {
    type Error = HAConnectionStateError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(HAConnectionState::Ready),
            1 => Ok(HAConnectionState::Handshake),
            2 => Ok(HAConnectionState::Transfer),
            3 => Ok(HAConnectionState::Suspend),
            4 => Ok(HAConnectionState::Shutdown),
            _ => Err(HAConnectionStateError::InvalidValue(value)),
        }
    }
}

/// Error type for HAConnectionState operations
#[derive(Debug, thiserror::Error)]
pub enum HAConnectionStateError {
    #[error("Invalid state transition from {from:?} to {to:?}")]
    InvalidTransition {
        from: HAConnectionState,
        to: HAConnectionState,
    },

    #[error("Invalid state value: {0}")]
    InvalidValue(i32),

    #[error("Cannot perform operation in state {state:?}: {reason}")]
    OperationNotAllowed { state: HAConnectionState, reason: String },
}

/// State machine for managing HA connection state transitions
#[derive(Debug, Clone)]
pub struct HAConnectionStateMachine {
    current_state: HAConnectionState,
    previous_state: Option<HAConnectionState>,
    transition_count: u64,
}

impl HAConnectionStateMachine {
    /// Create a new state machine starting in Ready state
    pub fn new() -> Self {
        Self {
            current_state: HAConnectionState::Ready,
            previous_state: None,
            transition_count: 0,
        }
    }

    /// Create a state machine with a specific initial state
    pub fn with_initial_state(state: HAConnectionState) -> Self {
        Self {
            current_state: state,
            previous_state: None,
            transition_count: 0,
        }
    }

    /// Get the current state
    pub fn current_state(&self) -> HAConnectionState {
        self.current_state
    }

    /// Get the previous state
    pub fn previous_state(&self) -> Option<HAConnectionState> {
        self.previous_state
    }

    /// Get the number of transitions
    pub fn transition_count(&self) -> u64 {
        self.transition_count
    }

    /// Attempt to transition to a new state
    pub fn transition_to(&mut self, new_state: HAConnectionState) -> Result<(), HAConnectionStateError> {
        if !self.current_state.can_transition_to(new_state) {
            return Err(HAConnectionStateError::InvalidTransition {
                from: self.current_state,
                to: new_state,
            });
        }

        self.previous_state = Some(self.current_state);
        self.current_state = new_state;
        self.transition_count += 1;

        Ok(())
    }

    /// Force transition to a new state (bypasses validation)
    pub fn force_transition_to(&mut self, new_state: HAConnectionState) {
        self.previous_state = Some(self.current_state);
        self.current_state = new_state;
        self.transition_count += 1;
    }

    /// Check if the state machine can transition to the given state
    pub fn can_transition_to(&self, state: HAConnectionState) -> bool {
        self.current_state.can_transition_to(state)
    }

    /// Reset the state machine to Ready
    pub fn reset(&mut self) {
        self.previous_state = Some(self.current_state);
        self.current_state = HAConnectionState::Ready;
        self.transition_count += 1;
    }

    /// Check if the connection is in an active state
    pub fn is_active(&self) -> bool {
        self.current_state.is_active()
    }

    /// Check if the connection is terminated
    pub fn is_terminated(&self) -> bool {
        self.current_state.is_terminated()
    }
}

impl Default for HAConnectionStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

/// Utility functions for working with HA connection states
pub mod ha_connection_utils {
    use super::*;

    /// Get states that are considered "healthy" for monitoring
    pub fn healthy_states() -> Vec<HAConnectionState> {
        vec![
            HAConnectionState::Ready,
            HAConnectionState::Handshake,
            HAConnectionState::Transfer,
        ]
    }

    /// Get states that indicate problems
    pub fn problematic_states() -> Vec<HAConnectionState> {
        vec![HAConnectionState::Suspend, HAConnectionState::Shutdown]
    }

    /// Check if a state transition represents progress
    pub fn is_progress_transition(from: HAConnectionState, to: HAConnectionState) -> bool {
        use HAConnectionState::*;
        matches!(
            (from, to),
            (Ready, Handshake) | (Handshake, Transfer) | (Suspend, Transfer)
        )
    }

    /// Check if a state transition represents regression
    pub fn is_regression_transition(from: HAConnectionState, to: HAConnectionState) -> bool {
        use HAConnectionState::*;
        matches!((from, to), (Transfer, Suspend) | (Transfer, Shutdown) | (_, Shutdown))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_properties() {
        assert!(HAConnectionState::Transfer.is_active());
        assert!(!HAConnectionState::Ready.is_active());

        assert!(HAConnectionState::Suspend.is_inactive());
        assert!(HAConnectionState::Shutdown.is_inactive());
        assert!(!HAConnectionState::Transfer.is_inactive());

        assert!(HAConnectionState::Ready.is_transitional());
        assert!(HAConnectionState::Handshake.is_transitional());
        assert!(!HAConnectionState::Transfer.is_transitional());

        assert!(HAConnectionState::Shutdown.is_terminated());
        assert!(!HAConnectionState::Transfer.is_terminated());
    }

    #[test]
    fn test_valid_transitions() {
        assert!(HAConnectionState::Ready.can_transition_to(HAConnectionState::Handshake));
        assert!(HAConnectionState::Handshake.can_transition_to(HAConnectionState::Transfer));
        assert!(HAConnectionState::Transfer.can_transition_to(HAConnectionState::Suspend));
        assert!(HAConnectionState::Suspend.can_transition_to(HAConnectionState::Transfer));
        assert!(HAConnectionState::Transfer.can_transition_to(HAConnectionState::Shutdown));
    }

    #[test]
    fn test_invalid_transitions() {
        assert!(!HAConnectionState::Ready.can_transition_to(HAConnectionState::Transfer));
        assert!(!HAConnectionState::Shutdown.can_transition_to(HAConnectionState::Ready));
        assert!(!HAConnectionState::Handshake.can_transition_to(HAConnectionState::Suspend));
    }

    #[test]
    fn test_string_conversions() {
        let state = HAConnectionState::Transfer;
        assert_eq!(state.as_str(), "transfer");
        assert_eq!(state.as_upper_str(), "TRANSFER");
        assert_eq!(state.to_string(), "TRANSFER");

        assert_eq!(HAConnectionState::from("TRANSFER"), HAConnectionState::Transfer);
        assert_eq!(HAConnectionState::from("transfer"), HAConnectionState::Transfer);
        assert_eq!(HAConnectionState::from("invalid"), HAConnectionState::Ready);
    }

    #[test]
    fn test_numeric_conversions() {
        let state = HAConnectionState::Transfer;
        let numeric: i32 = state.into();
        assert_eq!(numeric, 2);

        let converted = HAConnectionState::try_from(2).unwrap();
        assert_eq!(converted, HAConnectionState::Transfer);

        assert!(HAConnectionState::try_from(99).is_err());
    }

    #[test]
    fn test_state_machine() {
        let mut sm = HAConnectionStateMachine::new();
        assert_eq!(sm.current_state(), HAConnectionState::Ready);
        assert_eq!(sm.transition_count(), 0);

        // Valid transition
        sm.transition_to(HAConnectionState::Handshake).unwrap();
        assert_eq!(sm.current_state(), HAConnectionState::Handshake);
        assert_eq!(sm.previous_state(), Some(HAConnectionState::Ready));
        assert_eq!(sm.transition_count(), 1);

        // Invalid transition
        let result = sm.transition_to(HAConnectionState::Suspend);
        assert!(result.is_err());
        assert_eq!(sm.current_state(), HAConnectionState::Handshake); // Should remain unchanged

        // Force transition
        sm.force_transition_to(HAConnectionState::Shutdown);
        assert_eq!(sm.current_state(), HAConnectionState::Shutdown);
        assert_eq!(sm.transition_count(), 2);
    }

    #[test]
    fn test_state_machine_reset() {
        let mut sm = HAConnectionStateMachine::with_initial_state(HAConnectionState::Transfer);
        assert_eq!(sm.current_state(), HAConnectionState::Transfer);

        sm.reset();
        assert_eq!(sm.current_state(), HAConnectionState::Ready);
        assert_eq!(sm.previous_state(), Some(HAConnectionState::Transfer));
    }

    #[test]
    fn test_utility_functions() {
        let healthy = ha_connection_utils::healthy_states();
        assert!(healthy.contains(&HAConnectionState::Transfer));
        assert!(!healthy.contains(&HAConnectionState::Shutdown));

        let problematic = ha_connection_utils::problematic_states();
        assert!(problematic.contains(&HAConnectionState::Shutdown));
        assert!(!problematic.contains(&HAConnectionState::Transfer));

        assert!(ha_connection_utils::is_progress_transition(
            HAConnectionState::Ready,
            HAConnectionState::Handshake
        ));

        assert!(ha_connection_utils::is_regression_transition(
            HAConnectionState::Transfer,
            HAConnectionState::Shutdown
        ));
    }

    #[test]
    fn test_descriptions() {
        assert_eq!(HAConnectionState::Transfer.description(), "Synchronizing data");
        assert_eq!(HAConnectionState::Shutdown.description(), "Connection shutdown");
    }

    #[test]
    fn test_serialization() {
        let state = HAConnectionState::Transfer;
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: HAConnectionState = serde_json::from_str(&json).unwrap();
        assert_eq!(state, deserialized);
    }
}
