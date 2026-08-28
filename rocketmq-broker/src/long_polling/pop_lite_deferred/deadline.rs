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
pub(crate) const DEFAULT_POP_LITE_MAX_AGE: Duration = Duration::from_secs(30);

/// Checked PopLite business deadline with the legacy strict 50 ms cutoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PopLiteWaitDeadline {
    effective_end_millis: u64,
    protocol_millis: u64,
    protocol_at: tokio::time::Instant,
}

impl PopLiteWaitDeadline {
    pub(crate) fn checked(
        born_time: i64,
        poll_time: i64,
        admission_wall_now: u64,
        monotonic_now: tokio::time::Instant,
        max_age: Duration,
    ) -> Result<Self, PopLiteWaitDeadlineError> {
        if born_time < 0 {
            return Err(PopLiteWaitDeadlineError::new(
                PopLiteWaitDeadlineErrorKind::NegativeBornTime,
            ));
        }
        if poll_time <= 0 {
            return Err(PopLiteWaitDeadlineError::new(
                PopLiteWaitDeadlineErrorKind::NonPositivePollTime,
            ));
        }
        let requested_end = born_time
            .checked_add(poll_time)
            .ok_or_else(|| PopLiteWaitDeadlineError::new(PopLiteWaitDeadlineErrorKind::RequestedEndOverflow))?;
        let requested_end = u64::try_from(requested_end)
            .map_err(|_| PopLiteWaitDeadlineError::new(PopLiteWaitDeadlineErrorKind::RequestedEndOverflow))?;
        let max_age_millis = u64::try_from(max_age.as_millis()).unwrap_or(u64::MAX);
        let cap_end = admission_wall_now.saturating_add(max_age_millis);
        let effective_end_millis = requested_end.min(cap_end);
        let cutoff = effective_end_millis.saturating_sub(EARLY_WAKE_MILLIS);
        if admission_wall_now > cutoff {
            return Err(PopLiteWaitDeadlineError::new(
                PopLiteWaitDeadlineErrorKind::AlreadyExpired,
            ));
        }
        let remaining_millis = cutoff
            .checked_sub(admission_wall_now)
            .and_then(|remaining| remaining.checked_add(1))
            .ok_or_else(|| PopLiteWaitDeadlineError::new(PopLiteWaitDeadlineErrorKind::ProtocolOverflow))?;
        let protocol_millis = cutoff
            .checked_add(1)
            .ok_or_else(|| PopLiteWaitDeadlineError::new(PopLiteWaitDeadlineErrorKind::ProtocolOverflow))?;
        let protocol_at = monotonic_now
            .checked_add(Duration::from_millis(remaining_millis))
            .ok_or_else(|| PopLiteWaitDeadlineError::new(PopLiteWaitDeadlineErrorKind::MonotonicOverflow))?;
        Ok(Self {
            effective_end_millis,
            protocol_millis,
            protocol_at,
        })
    }

    pub(crate) const fn effective_end_millis(self) -> u64 {
        self.effective_end_millis
    }

    pub(crate) const fn protocol_millis(self) -> u64 {
        self.protocol_millis
    }

    pub(crate) const fn protocol_at(self) -> tokio::time::Instant {
        self.protocol_at
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PopLiteWaitDeadlineErrorKind {
    NegativeBornTime,
    NonPositivePollTime,
    RequestedEndOverflow,
    AlreadyExpired,
    ProtocolOverflow,
    MonotonicOverflow,
}

impl PopLiteWaitDeadlineErrorKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NegativeBornTime => "negative_born_time",
            Self::NonPositivePollTime => "non_positive_poll_time",
            Self::RequestedEndOverflow => "requested_end_overflow",
            Self::AlreadyExpired => "already_expired",
            Self::ProtocolOverflow => "protocol_overflow",
            Self::MonotonicOverflow => "monotonic_overflow",
        }
    }
}

pub(crate) struct PopLiteWaitDeadlineError {
    kind: PopLiteWaitDeadlineErrorKind,
}

impl PopLiteWaitDeadlineError {
    const fn new(kind: PopLiteWaitDeadlineErrorKind) -> Self {
        Self { kind }
    }

    pub(crate) const fn kind(&self) -> PopLiteWaitDeadlineErrorKind {
        self.kind
    }
}

impl fmt::Debug for PopLiteWaitDeadlineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PopLiteWaitDeadlineError")
            .field("kind", &self.kind.as_str())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for PopLiteWaitDeadlineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "PopLite wait deadline failed: {}", self.kind.as_str())
    }
}

impl Error for PopLiteWaitDeadlineError {}
