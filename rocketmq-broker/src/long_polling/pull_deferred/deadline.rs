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

/// Broker-owned Pull protocol deadline, distinct from the response owner deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PullWaitDeadline {
    protocol_end_millis: u64,
    protocol_at: tokio::time::Instant,
}

impl PullWaitDeadline {
    /// Preserves the legacy inclusive `now >= suspend_start + timeout` rule.
    pub(crate) fn checked(
        suspend_wall_millis: u64,
        suspend_monotonic: tokio::time::Instant,
        effective_timeout_millis: u64,
        wall_now_millis: u64,
        monotonic_now: tokio::time::Instant,
    ) -> Result<Self, PullWaitDeadlineError> {
        let protocol_end_millis = suspend_wall_millis
            .checked_add(effective_timeout_millis)
            .ok_or_else(|| PullWaitDeadlineError::new(PullWaitDeadlineErrorKind::ProtocolOverflow))?;
        let protocol_at = suspend_monotonic
            .checked_add(Duration::from_millis(effective_timeout_millis))
            .ok_or_else(|| PullWaitDeadlineError::new(PullWaitDeadlineErrorKind::MonotonicOverflow))?;
        if wall_now_millis >= protocol_end_millis || monotonic_now >= protocol_at {
            return Err(PullWaitDeadlineError::new(PullWaitDeadlineErrorKind::AlreadyExpired));
        }
        Ok(Self {
            protocol_end_millis,
            protocol_at,
        })
    }

    #[must_use]
    pub(crate) const fn protocol_end_millis(self) -> u64 {
        self.protocol_end_millis
    }

    #[must_use]
    pub(crate) const fn protocol_at(self) -> tokio::time::Instant {
        self.protocol_at
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PullWaitDeadlineErrorKind {
    ProtocolOverflow,
    AlreadyExpired,
    MonotonicOverflow,
}

impl PullWaitDeadlineErrorKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ProtocolOverflow => "protocol_overflow",
            Self::AlreadyExpired => "already_expired",
            Self::MonotonicOverflow => "monotonic_overflow",
        }
    }
}

pub(crate) struct PullWaitDeadlineError {
    kind: PullWaitDeadlineErrorKind,
}

impl PullWaitDeadlineError {
    const fn new(kind: PullWaitDeadlineErrorKind) -> Self {
        Self { kind }
    }

    #[must_use]
    pub(crate) const fn kind(&self) -> PullWaitDeadlineErrorKind {
        self.kind
    }
}

impl fmt::Debug for PullWaitDeadlineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PullWaitDeadlineError")
            .field("kind", &self.kind.as_str())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for PullWaitDeadlineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Pull wait deadline failed: {}", self.kind.as_str())
    }
}

impl Error for PullWaitDeadlineError {}
