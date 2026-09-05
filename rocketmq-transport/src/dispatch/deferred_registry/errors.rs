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

use std::convert::Infallible;

use super::DeferredParts;
use super::DeferredRegistration;
use super::DeferredRequest;
use crate::contract::TransportContractViolation;
use crate::error::TransportError;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::dispatch) enum RegistryFailure {
    DuplicateRequest,
    IdentityExhausted,
    ParentCancelled,
    SessionClosed,
    DeadlineExpired,
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "used by the test-only affine cleanup installer")
    )]
    CleanupInstallerRejected,
    RegistryInvariant,
}

/// Caller-owned state recovered from a deferred registration attempt.
#[must_use]
pub enum DeferredRegistryRecovery<R, F = Infallible> {
    /// The failure consumed no recoverable caller state.
    None,
    /// The complete direct-registration request is returned.
    Request(DeferredRequest<R>),
    /// The pre-builder response ownership is returned.
    Parts(DeferredParts),
    /// An uninvoked builder and its response ownership are returned together.
    Builder { builder: F, parts: DeferredParts },
}

/// Result carrier for one ownership-preserving deferred registration attempt.
///
/// Normal duplicate and lifecycle branches are source-free. Contract and
/// operational failure branches remain typed failures and carry the exact
/// caller-owned state that can be retried or released.
#[must_use]
pub enum DeferredRegistryOutcome<R, E = Infallible, F = Infallible> {
    /// The request was installed provisionally.
    Registered(DeferredRegistration),
    /// The request already has provisional or active ownership.
    DuplicateRequest(DeferredRegistryRecovery<R, F>),
    /// The bounded deferred identity namespace was exhausted.
    IdentityExhausted(DeferredRegistryRecovery<R, F>),
    /// The parent lifecycle ended and response ownership was terminalized.
    ParentCancelled,
    /// The session closed and response ownership was terminalized.
    SessionClosed,
    /// The immutable owner deadline elapsed and response ownership was terminalized.
    DeadlineExpired,
    /// The business-index builder rejected the request and returned its input ownership.
    BuilderRejected { error: E, parts: DeferredParts },
    /// A deterministic construction or accounting contract failed.
    ContractViolation {
        violation: TransportContractViolation,
        recovery: DeferredRegistryRecovery<R, F>,
    },
    /// An operational registry failure occurred.
    OperationalFailure {
        error: TransportError,
        recovery: DeferredRegistryRecovery<R, F>,
    },
}
