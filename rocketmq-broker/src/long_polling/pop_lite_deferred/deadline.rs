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
    ) -> Result<PopLiteWaitDeadlineOutcome, PopLiteWaitDeadlineOperationalError> {
        if born_time < 0 {
            return Ok(PopLiteWaitDeadlineOutcome::Rejected(PopLiteWaitDeadlineRejection::new(
                PopLiteWaitDeadlineRejectionReason::NegativeBornTime,
            )));
        }
        if poll_time <= 0 {
            return Ok(PopLiteWaitDeadlineOutcome::Rejected(PopLiteWaitDeadlineRejection::new(
                PopLiteWaitDeadlineRejectionReason::NonPositivePollTime,
            )));
        }
        let requested_end = born_time
            .checked_add(poll_time)
            .ok_or(PopLiteWaitDeadlineOperationalError::RequestedEnd)?;
        let requested_end =
            u64::try_from(requested_end).map_err(|_| PopLiteWaitDeadlineOperationalError::RequestedEnd)?;
        let max_age_millis = u64::try_from(max_age.as_millis()).unwrap_or(u64::MAX);
        let cap_end = admission_wall_now.saturating_add(max_age_millis);
        let effective_end_millis = requested_end.min(cap_end);
        let cutoff = effective_end_millis.saturating_sub(EARLY_WAKE_MILLIS);
        if admission_wall_now > cutoff {
            return Ok(PopLiteWaitDeadlineOutcome::Rejected(PopLiteWaitDeadlineRejection::new(
                PopLiteWaitDeadlineRejectionReason::AlreadyExpired,
            )));
        }
        let remaining_millis = cutoff
            .checked_sub(admission_wall_now)
            .and_then(|remaining| remaining.checked_add(1))
            .ok_or(PopLiteWaitDeadlineOperationalError::Protocol)?;
        let protocol_millis = cutoff
            .checked_add(1)
            .ok_or(PopLiteWaitDeadlineOperationalError::Protocol)?;
        let protocol_at = monotonic_now
            .checked_add(Duration::from_millis(remaining_millis))
            .ok_or(PopLiteWaitDeadlineOperationalError::Monotonic)?;
        Ok(PopLiteWaitDeadlineOutcome::Pending(Self {
            effective_end_millis,
            protocol_millis,
            protocol_at,
        }))
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

#[derive(Debug)]
#[must_use]
pub(crate) enum PopLiteWaitDeadlineOutcome {
    Pending(PopLiteWaitDeadline),
    Rejected(PopLiteWaitDeadlineRejection),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PopLiteWaitDeadlineRejectionReason {
    NegativeBornTime,
    NonPositivePollTime,
    AlreadyExpired,
}

impl PopLiteWaitDeadlineRejectionReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NegativeBornTime => "negative_born_time",
            Self::NonPositivePollTime => "non_positive_poll_time",
            Self::AlreadyExpired => "already_expired",
        }
    }
}

pub(crate) struct PopLiteWaitDeadlineRejection {
    reason: PopLiteWaitDeadlineRejectionReason,
}

impl PopLiteWaitDeadlineRejection {
    const fn new(reason: PopLiteWaitDeadlineRejectionReason) -> Self {
        Self { reason }
    }

    pub(crate) const fn reason(&self) -> PopLiteWaitDeadlineRejectionReason {
        self.reason
    }
}

impl fmt::Debug for PopLiteWaitDeadlineRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PopLiteWaitDeadlineRejection")
            .field("reason", &self.reason.as_str())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for PopLiteWaitDeadlineRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "PopLite wait deadline rejected: {}", self.reason.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PopLiteWaitDeadlineOperationalError {
    RequestedEnd,
    Protocol,
    Monotonic,
}

impl fmt::Display for PopLiteWaitDeadlineOperationalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RequestedEnd => "PopLite requested deadline overflowed",
            Self::Protocol => "PopLite protocol deadline overflowed",
            Self::Monotonic => "PopLite monotonic deadline overflowed",
        })
    }
}

impl Error for PopLiteWaitDeadlineOperationalError {}
