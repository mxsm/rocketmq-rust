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

use std::time::Duration;
use std::time::Instant;

/// Upper bound on how long an aborted task may take to confirm cancellation.
///
/// Every abort path reaches this window only after the graceful deadline has
/// elapsed, so the window is deliberately independent of
/// [`ShutdownDeadline::remaining`]. Deriving it from the expired deadline would
/// always yield zero. `TaskGroup::abort_task_and_wait` treats a zero timeout as an
/// unconfirmed cancellation, so a zero window would report a timed-out driver as
/// not aborted and would let a caller observe a task whose running future had
/// not been dropped yet.
pub(crate) const ABORT_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(1);

/// A single absolute shutdown deadline shared by nested lifecycle owners.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownDeadline {
    at: Instant,
}

impl ShutdownDeadline {
    /// Creates the after value.
    pub fn after(timeout: Duration) -> Self {
        Self {
            at: Instant::now() + timeout,
        }
    }

    /// Creates the at value.
    pub fn at(at: Instant) -> Self {
        Self { at }
    }

    /// Returns the instant.
    pub fn instant(self) -> Instant {
        self.at
    }

    /// Returns the remaining.
    pub fn remaining(self) -> Duration {
        self.at.saturating_duration_since(Instant::now())
    }

    /// Returns whether expired.
    pub fn is_expired(self) -> bool {
        self.remaining().is_zero()
    }

    pub(crate) fn earliest(self, other: Self) -> Self {
        if self.at <= other.at {
            self
        } else {
            other
        }
    }
}
