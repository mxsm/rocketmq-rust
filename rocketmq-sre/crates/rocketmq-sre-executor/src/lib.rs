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

//! Disabled execution boundary.
//!
//! No target driver, credential type, approval path, or mutation dependency is
//! present in Phase 00.

use thiserror::Error;

/// Compile-time-visible execution availability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionAvailability {
    pub enabled: bool,
    pub reason: &'static str,
}

/// Error returned by every Phase 00 execution attempt.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("target execution is disabled in Phase 00")]
pub struct ExecutionDisabled;

/// Returns the immutable Phase 00 execution status.
#[must_use]
pub const fn availability() -> ExecutionAvailability {
    ExecutionAvailability {
        enabled: false,
        reason: "approval, policy, and mutation drivers are intentionally absent",
    }
}

/// Rejects execution without accepting a target, command, or credential.
///
/// # Errors
///
/// Always returns [`ExecutionDisabled`].
pub const fn reject_execution() -> Result<(), ExecutionDisabled> {
    Err(ExecutionDisabled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_is_unconditionally_disabled() {
        assert!(!availability().enabled);
        assert_eq!(reject_execution(), Err(ExecutionDisabled));
    }
}
