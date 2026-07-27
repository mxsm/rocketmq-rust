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

//! Durable but dispatch-disabled supervised execution boundary.
//!
//! Phase 3 persistence can journal immutable requests, intents, results,
//! resource locks, quarantines, and fenced executor leases. No target driver,
//! credential type, external mutation call, or execution binary is present
//! until the later handler milestones explicitly enable one.

mod error;
mod journal;
mod lease;
mod lock;
mod reconcile;

use thiserror::Error;

pub use error::JournalError;
pub use journal::ExecutionCreation;
pub use journal::ExecutionJournal;
pub use journal::PendingIntent;
pub use lease::ExecutorLeaseRecord;
pub use lease::LeaseCoordinator;
pub use lock::ResourceLock;
pub use lock::ResourceLockRequest;
pub use lock::ResourceSafetyStore;
pub use reconcile::LiveEffectState;
pub use reconcile::ReconcileDisposition;
pub use reconcile::ReconcilePlanner;

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
        reason: "durable fencing exists, but typed handlers and dispatch remain disabled",
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
