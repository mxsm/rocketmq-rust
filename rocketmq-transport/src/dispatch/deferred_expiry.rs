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
use std::time::Duration;

use tokio::time::Instant;

use super::DeferredParts;
use super::RequestControlView;
use super::RequestId;

/// Explicit safety margins reserved before the canonical request-owner deadline.
///
/// Both margins must be non-zero. Validation is performed when the margins are
/// attached to affine deferred ownership so a failure can return that ownership
/// unchanged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeferredExpiryMargins {
    recovery: Duration,
    write: Duration,
}

impl DeferredExpiryMargins {
    /// Declares the time reserved for business recovery and canonical response writing.
    #[must_use]
    pub const fn new(recovery: Duration, write: Duration) -> Self {
        Self { recovery, write }
    }

    /// Returns the business recovery margin.
    #[must_use]
    pub const fn recovery(self) -> Duration {
        self.recovery
    }

    /// Returns the canonical response-write margin.
    #[must_use]
    pub const fn write(self) -> Duration {
        self.write
    }
}

/// The first action selected by a deferred expiry policy.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DeferredExpiryKind {
    /// Re-enter business handling through the registry's single-claim path.
    LongPollTimeout,
    /// Stop without re-entering business handling because owner budget won.
    OwnerDeadline,
}

impl DeferredExpiryKind {
    /// Returns a stable low-cardinality label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LongPollTimeout => "long_poll_timeout",
            Self::OwnerDeadline => "owner_deadline",
        }
    }
}

/// Opaque expiry policy frozen from protocol time and canonical request ownership.
///
/// The original protocol instant is retained even when the derived owner resume
/// cutoff is earlier. At an equal instant, request ownership wins and business
/// logic is not resumed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeferredExpiry {
    protocol_at: Instant,
    resume_cutoff: Option<Instant>,
    write_cutoff: Option<Instant>,
}

impl DeferredExpiry {
    pub(crate) fn try_from_control(
        control: &RequestControlView,
        protocol_at: Instant,
        margins: DeferredExpiryMargins,
    ) -> Result<Self, DeferredExpiryErrorKind> {
        if margins.recovery.is_zero() {
            return Err(DeferredExpiryErrorKind::ZeroRecoveryMargin);
        }
        if margins.write.is_zero() {
            return Err(DeferredExpiryErrorKind::ZeroWriteMargin);
        }

        let now = Instant::now();
        if protocol_at <= now {
            return Err(DeferredExpiryErrorKind::ProtocolAlreadyExpired);
        }

        let Some(owner_at) = control.deadline().map(|deadline| deadline.instant()) else {
            return Ok(Self {
                protocol_at,
                resume_cutoff: None,
                write_cutoff: None,
            });
        };
        if owner_at <= now {
            return Err(DeferredExpiryErrorKind::OwnerAlreadyExpired);
        }
        let write_cutoff = owner_at
            .checked_sub(margins.write)
            .ok_or(DeferredExpiryErrorKind::OwnerBudgetInsufficient)?;
        let resume_cutoff = write_cutoff
            .checked_sub(margins.recovery)
            .ok_or(DeferredExpiryErrorKind::OwnerBudgetInsufficient)?;
        if write_cutoff <= now || resume_cutoff <= now {
            return Err(DeferredExpiryErrorKind::OwnerBudgetInsufficient);
        }
        Ok(Self {
            protocol_at,
            resume_cutoff: Some(resume_cutoff),
            write_cutoff: Some(write_cutoff),
        })
    }

    /// Returns the original protocol expiry instant.
    #[must_use]
    pub const fn protocol_at(self) -> Instant {
        self.protocol_at
    }

    /// Returns the latest instant at which deferred business recovery may begin.
    #[must_use]
    pub const fn resume_cutoff(self) -> Option<Instant> {
        self.resume_cutoff
    }

    /// Returns the latest instant at which a deferred response write may begin.
    #[must_use]
    pub const fn write_cutoff(self) -> Option<Instant> {
        self.write_cutoff
    }

    /// Returns the first scheduled action. Owner expiry wins an equal boundary.
    #[must_use]
    pub fn kind(self) -> DeferredExpiryKind {
        match self.resume_cutoff {
            Some(owner_at) if owner_at <= self.protocol_at => DeferredExpiryKind::OwnerDeadline,
            Some(_) | None => DeferredExpiryKind::LongPollTimeout,
        }
    }

    /// Returns the instant of the first scheduled action.
    #[must_use]
    pub fn next_at(self) -> Instant {
        match self.resume_cutoff {
            Some(owner_at) if owner_at <= self.protocol_at => owner_at,
            Some(_) | None => self.protocol_at,
        }
    }
}

/// Stable category for a deferred expiry attachment failure.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DeferredExpiryErrorKind {
    /// Expiry ownership was already attached to these affine parts.
    AlreadyAttached,
    /// The business recovery margin was zero.
    ZeroRecoveryMargin,
    /// The response-write margin was zero.
    ZeroWriteMargin,
    /// Checked owner-cutoff arithmetic could not be represented.
    OwnerBudgetInsufficient,
    /// The protocol expiry was already reached when ownership was attached.
    ProtocolAlreadyExpired,
    /// The canonical owner deadline was already reached when ownership was attached.
    OwnerAlreadyExpired,
}

impl DeferredExpiryErrorKind {
    /// Returns a stable low-cardinality label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AlreadyAttached => "already_attached",
            Self::ZeroRecoveryMargin => "zero_recovery_margin",
            Self::ZeroWriteMargin => "zero_write_margin",
            Self::OwnerBudgetInsufficient => "owner_budget_insufficient",
            Self::ProtocolAlreadyExpired => "protocol_already_expired",
            Self::OwnerAlreadyExpired => "owner_already_expired",
        }
    }
}

/// Typed, redacted failure to attach protocol expiry to affine deferred parts.
pub struct DeferredExpiryError {
    kind: DeferredExpiryErrorKind,
    request_id: RequestId,
    parts: Box<DeferredParts>,
}

impl DeferredExpiryError {
    pub(crate) fn new(kind: DeferredExpiryErrorKind, request_id: RequestId, parts: DeferredParts) -> Self {
        Self {
            kind,
            request_id,
            parts: Box::new(parts),
        }
    }

    /// Returns the stable failure category.
    #[must_use]
    pub const fn kind(&self) -> DeferredExpiryErrorKind {
        self.kind
    }

    /// Returns the trusted request identity without exposing protocol timing.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Recovers the exact affine parts supplied to the failed attachment.
    pub fn into_parts(self) -> DeferredParts {
        *self.parts
    }
}

impl fmt::Debug for DeferredExpiryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeferredExpiryError")
            .field("kind", &self.kind.as_str())
            .field("request_id", &self.request_id)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for DeferredExpiryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "deferred expiry attachment failed: {}", self.kind.as_str())
    }
}

impl Error for DeferredExpiryError {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;

    use super::*;
    use crate::deadline::RequestDeadline;
    use crate::dispatch::RequestMeta;
    use crate::session_view::EmbeddedSessionRecord;

    fn control(deadline: Option<RequestDeadline>) -> (RuntimeOwner, RequestControlView) {
        let runtime =
            RuntimeOwner::new(RuntimeConfig::server_default("deferred-expiry-policy")).expect("expiry test runtime");
        let session = Arc::new(EmbeddedSessionRecord::new(9819));
        let parent = runtime
            .root_context()
            .component("deferred-expiry-policy")
            .task_group()
            .clone();
        let view = RequestControlView::from_meta(
            &RequestMeta::new(std::time::Instant::now(), deadline),
            session.view().state().clone(),
            &parent,
        );
        (runtime, view)
    }

    #[tokio::test(start_paused = true)]
    async fn owner_cutoffs_are_checked_and_protocol_is_capped() {
        let now = Instant::now();
        let (_runtime, control) = control(Some(RequestDeadline::after(Duration::from_secs(30))));
        let expiry = DeferredExpiry::try_from_control(
            &control,
            now + Duration::from_secs(29),
            DeferredExpiryMargins::new(Duration::from_secs(5), Duration::from_secs(3)),
        )
        .expect("valid owner margins");

        assert_eq!(expiry.write_cutoff(), Some(now + Duration::from_secs(27)));
        assert_eq!(expiry.resume_cutoff(), Some(now + Duration::from_secs(22)));
        assert_eq!(expiry.protocol_at(), now + Duration::from_secs(29));
        assert_eq!(expiry.next_at(), now + Duration::from_secs(22));
        assert_eq!(expiry.kind(), DeferredExpiryKind::OwnerDeadline);
    }

    #[tokio::test(start_paused = true)]
    async fn notification_protocol_deadline_owner_early_equal_late_matrix_is_owner_safe() {
        let now = Instant::now();
        let (_runtime, control) = control(Some(RequestDeadline::after(Duration::from_secs(10))));
        let margins = DeferredExpiryMargins::new(Duration::from_secs(3), Duration::from_secs(2));
        let cases = [
            (
                now + Duration::from_secs(4),
                DeferredExpiryKind::LongPollTimeout,
                now + Duration::from_secs(4),
            ),
            (
                now + Duration::from_secs(5),
                DeferredExpiryKind::OwnerDeadline,
                now + Duration::from_secs(5),
            ),
            (
                now + Duration::from_secs(6),
                DeferredExpiryKind::OwnerDeadline,
                now + Duration::from_secs(5),
            ),
        ];

        for (protocol_at, expected_kind, expected_next) in cases {
            let expiry = DeferredExpiry::try_from_control(&control, protocol_at, margins)
                .expect("Notification owner/protocol boundary remains live");
            assert_eq!(expiry.resume_cutoff(), Some(now + Duration::from_secs(5)));
            assert_eq!(expiry.kind(), expected_kind);
            assert_eq!(expiry.next_at(), expected_next);
        }

        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(Instant::now(), now + Duration::from_secs(5));
    }

    #[test]
    fn margins_are_non_zero_even_without_an_owner_deadline() {
        let (_runtime, control) = control(None);
        let protocol_at = Instant::now() + Duration::from_secs(1);
        assert_eq!(
            DeferredExpiry::try_from_control(
                &control,
                protocol_at,
                DeferredExpiryMargins::new(Duration::ZERO, Duration::from_millis(1)),
            ),
            Err(DeferredExpiryErrorKind::ZeroRecoveryMargin)
        );
        assert_eq!(
            DeferredExpiry::try_from_control(
                &control,
                protocol_at,
                DeferredExpiryMargins::new(Duration::from_millis(1), Duration::ZERO),
            ),
            Err(DeferredExpiryErrorKind::ZeroWriteMargin)
        );
    }
}

#[cfg(test)]
#[path = "deferred_expiry/acceptance_tests.rs"]
mod acceptance_tests;
