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

use std::fmt;

/// Protocol-independent condition assigned by a canonical error descriptor.
///
/// A condition classifies the semantic outcome of a failure without adopting
/// any boundary protocol's status-code vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CanonicalCondition {
    /// The caller supplied an invalid argument.
    InvalidArgument,
    /// The requested resource does not exist.
    NotFound,
    /// The requested resource already exists.
    AlreadyExists,
    /// The caller has not been authenticated.
    Unauthenticated,
    /// The authenticated caller lacks permission.
    PermissionDenied,
    /// A required resource has been exhausted.
    ResourceExhausted,
    /// The system is not in a state that permits the operation.
    FailedPrecondition,
    /// The operation was aborted.
    Aborted,
    /// The service is unavailable.
    Unavailable,
    /// The operation exceeded its deadline.
    DeadlineExceeded,
    /// The system detected unrecoverable data loss.
    DataLoss,
    /// The operation was cancelled.
    Cancelled,
    /// The operation is not implemented.
    Unimplemented,
    /// An internal failure occurred.
    Internal,
}

impl CanonicalCondition {
    /// Returns the stable condition name used by catalog projections.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid_argument",
            Self::NotFound => "not_found",
            Self::AlreadyExists => "already_exists",
            Self::Unauthenticated => "unauthenticated",
            Self::PermissionDenied => "permission_denied",
            Self::ResourceExhausted => "resource_exhausted",
            Self::FailedPrecondition => "failed_precondition",
            Self::Aborted => "aborted",
            Self::Unavailable => "unavailable",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::DataLoss => "data_loss",
            Self::Cancelled => "cancelled",
            Self::Unimplemented => "unimplemented",
            Self::Internal => "internal",
        }
    }
}

impl fmt::Display for CanonicalCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Catalog-owned recovery advice for a canonical error descriptor.
///
/// A recovery hint is not a retry decision and does not imply that an operation
/// is retryable. Callers combine it with operation idempotency, stage, and
/// remaining budget when deciding whether to retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RecoveryHint {
    /// Do not attempt recovery automatically.
    Never,
    /// Retry only after applying backoff.
    Backoff,
    /// Refresh route information before continuing.
    RefreshRoute,
    /// Refresh leader information before continuing.
    RefreshLeader,
    /// Select a different broker before continuing.
    SwitchBroker,
    /// Refresh credentials before continuing.
    RefreshCredentials,
    /// Require an operator to take corrective action.
    OperatorAction,
}

impl RecoveryHint {
    /// Returns the stable recovery-hint name used by catalog projections.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::Backoff => "backoff",
            Self::RefreshRoute => "refresh_route",
            Self::RefreshLeader => "refresh_leader",
            Self::SwitchBroker => "switch_broker",
            Self::RefreshCredentials => "refresh_credentials",
            Self::OperatorAction => "operator_action",
        }
    }
}

impl fmt::Display for RecoveryHint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
