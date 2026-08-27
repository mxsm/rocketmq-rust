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
use std::error::Error;
use std::fmt;

use super::DeferredParts;
use super::DeferredRequest;
use super::RequestId;

/// Stable category for transactional registry failures.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DeferredRegistryErrorKind {
    /// Checked retained-size accounting overflowed.
    RetainedSizeOverflow,
    /// The supplied permit does not cover the registry's mandatory floor.
    RetainedSizeUnderreported,
    /// The exact request already has a provisional or active registration.
    DuplicateRequest,
    /// Process-local deferred identities are permanently exhausted.
    IdentityExhausted,
    /// The parent lifecycle owner was cancelled.
    ParentCancelled,
    /// The trusted session lifecycle was closed.
    SessionClosed,
    /// The immutable request deadline expired at a registration checkpoint.
    DeadlineExpired,
    /// The caller's business-index builder rejected the request.
    Builder,
    /// A sealed registry phase invariant was not satisfied.
    RegistryInvariant,
}

impl DeferredRegistryErrorKind {
    /// Returns a stable low-cardinality label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RetainedSizeOverflow => "retained_size_overflow",
            Self::RetainedSizeUnderreported => "retained_size_underreported",
            Self::DuplicateRequest => "duplicate_request",
            Self::IdentityExhausted => "identity_exhausted",
            Self::ParentCancelled => "parent_cancelled",
            Self::SessionClosed => "session_closed",
            Self::DeadlineExpired => "deadline_expired",
            Self::Builder => "builder",
            Self::RegistryInvariant => "registry_invariant",
        }
    }
}

pub(super) enum RegistryRecovery<R, E> {
    None,
    Request(Box<DeferredRequest<R>>),
    Parts(Box<DeferredParts>),
    Builder(Box<(E, DeferredParts)>),
}

/// Typed, redacted failure from a deferred registry transaction.
///
/// Preflight and index failures return the exact request or parts when the
/// lifecycle remains usable. Builder failures preserve their typed source and
/// return it together with the original parts. A lifecycle stop consumes all
/// affine ownership and deliberately exposes no source or recovery value.
pub struct DeferredRegistryError<R, E = Infallible> {
    kind: DeferredRegistryErrorKind,
    request_id: RequestId,
    recovery: RegistryRecovery<R, E>,
}

impl<R, E> DeferredRegistryError<R, E> {
    pub(super) const fn new(
        kind: DeferredRegistryErrorKind,
        request_id: RequestId,
        recovery: RegistryRecovery<R, E>,
    ) -> Self {
        Self {
            kind,
            request_id,
            recovery,
        }
    }

    /// Returns the stable failure category.
    #[must_use]
    pub const fn kind(&self) -> DeferredRegistryErrorKind {
        self.kind
    }

    /// Returns the exact request whose registration failed.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Recovers a complete request when registration received one directly.
    #[must_use]
    pub fn into_request(self) -> Option<DeferredRequest<R>> {
        match self.recovery {
            RegistryRecovery::Request(request) => Some(*request),
            RegistryRecovery::None | RegistryRecovery::Parts(_) | RegistryRecovery::Builder(_) => None,
        }
    }

    /// Recovers response ownership after a pre-builder failure.
    #[must_use]
    pub fn into_parts(self) -> Option<DeferredParts> {
        match self.recovery {
            RegistryRecovery::Parts(parts) => Some(*parts),
            RegistryRecovery::None | RegistryRecovery::Request(_) | RegistryRecovery::Builder(_) => None,
        }
    }

    /// Recovers a typed builder failure together with response ownership.
    #[must_use]
    pub fn into_builder_failure(self) -> Option<(E, DeferredParts)> {
        match self.recovery {
            RegistryRecovery::Builder(builder) => Some(*builder),
            RegistryRecovery::None | RegistryRecovery::Request(_) | RegistryRecovery::Parts(_) => None,
        }
    }
}

impl<R, E> fmt::Debug for DeferredRegistryError<R, E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeferredRegistryError")
            .field("kind", &self.kind.as_str())
            .field("request_id", &self.request_id)
            .finish_non_exhaustive()
    }
}

impl<R, E> fmt::Display for DeferredRegistryError<R, E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "deferred registry error: {}", self.kind.as_str())
    }
}

impl<R, E> Error for DeferredRegistryError<R, E>
where
    E: Error + Send + Sync + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.recovery {
            RegistryRecovery::Builder(builder) => Some(&builder.0),
            RegistryRecovery::None | RegistryRecovery::Request(_) | RegistryRecovery::Parts(_) => None,
        }
    }
}
