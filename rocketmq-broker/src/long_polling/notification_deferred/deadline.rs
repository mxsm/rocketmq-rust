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

const EARLY_WAKE_MILLIS: i64 = 50;

/// Checked Notification protocol deadline, separate from transport ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NotificationWaitDeadline {
    protocol_millis: i64,
    protocol_at: tokio::time::Instant,
}

impl NotificationWaitDeadline {
    /// Freezes `now > (born + poll).saturating_sub(50)` as a monotonic instant.
    pub(crate) fn checked(
        born_time: i64,
        poll_time: i64,
        wall_now: i64,
        monotonic_now: tokio::time::Instant,
    ) -> Result<Self, NotificationWaitDeadlineError> {
        if born_time < 0 {
            return Err(NotificationWaitDeadlineError::new(
                NotificationWaitDeadlineErrorKind::NegativeBornTime,
            ));
        }
        if poll_time <= 0 {
            return Err(NotificationWaitDeadlineError::new(
                NotificationWaitDeadlineErrorKind::NonPositivePollTime,
            ));
        }
        if wall_now < 0 {
            return Err(NotificationWaitDeadlineError::new(
                NotificationWaitDeadlineErrorKind::NegativeWallTime,
            ));
        }
        let requested_end = born_time
            .checked_add(poll_time)
            .ok_or_else(|| NotificationWaitDeadlineError::new(NotificationWaitDeadlineErrorKind::ProtocolOverflow))?;
        let cutoff = requested_end.saturating_sub(EARLY_WAKE_MILLIS);
        if wall_now > cutoff {
            return Err(NotificationWaitDeadlineError::new(
                NotificationWaitDeadlineErrorKind::AlreadyExpired,
            ));
        }
        let protocol_millis = cutoff
            .checked_add(1)
            .ok_or_else(|| NotificationWaitDeadlineError::new(NotificationWaitDeadlineErrorKind::ProtocolOverflow))?;
        let remaining = protocol_millis
            .checked_sub(wall_now)
            .ok_or_else(|| NotificationWaitDeadlineError::new(NotificationWaitDeadlineErrorKind::ProtocolOverflow))?;
        let remaining = u64::try_from(remaining)
            .map_err(|_| NotificationWaitDeadlineError::new(NotificationWaitDeadlineErrorKind::ProtocolOverflow))?;
        let protocol_at = monotonic_now
            .checked_add(Duration::from_millis(remaining))
            .ok_or_else(|| NotificationWaitDeadlineError::new(NotificationWaitDeadlineErrorKind::MonotonicOverflow))?;
        Ok(Self {
            protocol_millis,
            protocol_at,
        })
    }

    #[must_use]
    pub(crate) const fn protocol_millis(self) -> i64 {
        self.protocol_millis
    }

    #[must_use]
    pub(crate) const fn protocol_at(self) -> tokio::time::Instant {
        self.protocol_at
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum NotificationWaitDeadlineErrorKind {
    NegativeBornTime,
    NonPositivePollTime,
    NegativeWallTime,
    ProtocolOverflow,
    AlreadyExpired,
    MonotonicOverflow,
}

pub(crate) struct NotificationWaitDeadlineError {
    kind: NotificationWaitDeadlineErrorKind,
}

impl NotificationWaitDeadlineError {
    const fn new(kind: NotificationWaitDeadlineErrorKind) -> Self {
        Self { kind }
    }

    #[must_use]
    pub(crate) const fn kind(&self) -> NotificationWaitDeadlineErrorKind {
        self.kind
    }
}

impl fmt::Debug for NotificationWaitDeadlineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NotificationWaitDeadlineError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for NotificationWaitDeadlineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Notification wait deadline failed: {:?}", self.kind)
    }
}

impl Error for NotificationWaitDeadlineError {}
