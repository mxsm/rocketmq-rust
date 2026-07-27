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

//! Single-active, fenced supervised change Executor.
//!
//! This crate deliberately has no target mutation dependency. It talks only to
//! the Control Plane Lease Authority and the typed Execution Agent wire API.

mod agent_client;
mod api;
mod authority_client;
mod config;
mod engine;
mod error;
mod journal;
mod lease;
mod lock;
mod precheck;
mod reconcile;
mod registry;

use thiserror::Error;

pub use agent_client::ExecutionAgentClient;
pub use agent_client::HttpExecutionAgentClient;
pub use api::build_router;
pub use api::run;
pub use authority_client::ExecutorAuthorityClient;
pub use authority_client::HttpExecutorAuthorityClient;
pub use config::DEFAULT_EXECUTOR_PORT;
pub use config::ExecutorConfig;
pub use engine::ChangeExecutor;
pub use engine::ExecuteOutcome;
pub use engine::ExecutorMetricsSnapshot;
pub use error::ExecutorError;
pub use error::JournalError;
pub use journal::ExecutionCreation;
pub use journal::ExecutionJournal;
pub use journal::PendingIntent;
pub use lease::ExecutorLeaseRecord;
pub use lease::LeaseCoordinator;
pub use lock::ResourceLock;
pub use lock::ResourceLockRequest;
pub use lock::ResourceSafetyStore;
pub use precheck::ExecutionPrechecker;
pub use reconcile::LiveEffectState;
pub use reconcile::ReconcileDisposition;
pub use reconcile::ReconcilePlanner;
pub use registry::ExecutorActionRegistry;

/// Compile-time-visible execution availability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionAvailability {
    pub enabled: bool,
    pub reason: &'static str,
}

/// Error returned when a caller attempts direct, untyped execution.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("direct target execution is forbidden; use a signed request and typed Agent")]
pub struct ExecutionDisabled;

/// Returns the immutable supervised execution status.
#[must_use]
pub const fn availability() -> ExecutionAvailability {
    ExecutionAvailability {
        enabled: true,
        reason: "fenced Executor is active; only descriptor-enabled typed Agent handlers can dispatch",
    }
}

/// Rejects attempts to bypass the signed request and typed Agent boundary.
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
    fn executor_boundary_is_enabled_but_direct_execution_is_rejected() {
        assert!(availability().enabled);
        assert_eq!(reject_execution(), Err(ExecutionDisabled));
    }
}
