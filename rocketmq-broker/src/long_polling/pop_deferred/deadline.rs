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

const EARLY_WAKE_MILLIS: u64 = 50;

/// Broker-owned POP protocol deadline, kept distinct from transport ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LongPollingDeadline {
    protocol_millis: u64,
    protocol_at: tokio::time::Instant,
}

impl LongPollingDeadline {
    /// Converts the legacy millisecond predicate into one monotonic deadline.
    ///
    /// The old predicate was `now > (born + poll).saturating_sub(50)`. The
    /// additional millisecond preserves that strict comparison exactly at the
    /// integer-millisecond boundary.
    pub(crate) fn checked(
        born_time: u64,
        poll_time: u64,
        wall_now: u64,
        monotonic_now: tokio::time::Instant,
    ) -> Result<LongPollingDeadlineOutcome, LongPollingDeadlineError> {
        if poll_time == 0 {
            return Ok(LongPollingDeadlineOutcome::Immediate);
        }
        let expires = born_time
            .checked_add(poll_time)
            .ok_or_else(|| LongPollingDeadlineError::new(LongPollingDeadlineErrorKind::ProtocolOverflow))?;
        let legacy_threshold = expires.saturating_sub(EARLY_WAKE_MILLIS);
        if wall_now > legacy_threshold {
            return Ok(LongPollingDeadlineOutcome::Immediate);
        }
        let remaining_millis = legacy_threshold
            .checked_sub(wall_now)
            .and_then(|remaining| remaining.checked_add(1))
            .ok_or_else(|| LongPollingDeadlineError::new(LongPollingDeadlineErrorKind::ProtocolOverflow))?;
        let protocol_millis = legacy_threshold
            .checked_add(1)
            .ok_or_else(|| LongPollingDeadlineError::new(LongPollingDeadlineErrorKind::ProtocolOverflow))?;
        let protocol_at = monotonic_now
            .checked_add(Duration::from_millis(remaining_millis))
            .ok_or_else(|| LongPollingDeadlineError::new(LongPollingDeadlineErrorKind::MonotonicOverflow))?;
        Ok(LongPollingDeadlineOutcome::Pending(Self {
            protocol_millis,
            protocol_at,
        }))
    }

    #[must_use]
    pub(crate) const fn protocol_millis(self) -> u64 {
        self.protocol_millis
    }

    #[must_use]
    pub(crate) const fn protocol_at(self) -> tokio::time::Instant {
        self.protocol_at
    }
}

#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LongPollingDeadlineOutcome {
    Pending(LongPollingDeadline),
    Immediate,
}

/// Stable category for a checked POP protocol-deadline conversion failure.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum LongPollingDeadlineErrorKind {
    ProtocolOverflow,
    MonotonicOverflow,
}

impl LongPollingDeadlineErrorKind {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ProtocolOverflow => "protocol_overflow",
            Self::MonotonicOverflow => "monotonic_overflow",
        }
    }
}

/// Typed, redacted deadline conversion failure.
pub(crate) struct LongPollingDeadlineError {
    kind: LongPollingDeadlineErrorKind,
}

impl LongPollingDeadlineError {
    const fn new(kind: LongPollingDeadlineErrorKind) -> Self {
        Self { kind }
    }

    #[must_use]
    pub(crate) const fn kind(&self) -> LongPollingDeadlineErrorKind {
        self.kind
    }
}

impl fmt::Debug for LongPollingDeadlineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LongPollingDeadlineError")
            .field("kind", &self.kind.as_str())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for LongPollingDeadlineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "POP long-polling deadline failed: {}", self.kind.as_str())
    }
}

impl Error for LongPollingDeadlineError {}
