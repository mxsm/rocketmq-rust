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

//! Immutable end-to-end request deadline.

use std::future::Future;
use std::time::Duration;

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

/// One absolute deadline frozen at the public request boundary.
///
/// Copies preserve the same expiry instant and original budget. Downstream
/// stages can only observe the remaining budget; they cannot extend it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestDeadline {
    expires_at: tokio::time::Instant,
    budget: Duration,
}

impl RequestDeadline {
    /// Freezes a relative timeout into one absolute Tokio deadline.
    #[must_use]
    pub fn after(timeout: Duration) -> Self {
        let now = tokio::time::Instant::now();
        let expires_at = now.checked_add(timeout).unwrap_or(now);
        Self {
            expires_at,
            budget: timeout,
        }
    }

    /// Freezes a millisecond timeout into one absolute Tokio deadline.
    #[must_use]
    pub fn from_timeout_millis(timeout_millis: u64) -> Self {
        Self::after(Duration::from_millis(timeout_millis))
    }

    /// Returns the immutable absolute expiry instant.
    #[must_use]
    pub const fn instant(self) -> tokio::time::Instant {
        self.expires_at
    }

    /// Returns the original caller budget.
    #[must_use]
    pub const fn budget(self) -> Duration {
        self.budget
    }

    /// Returns the original caller budget, saturated to the public millisecond field.
    #[must_use]
    pub fn budget_millis(self) -> u64 {
        self.budget.as_millis().min(u128::from(u64::MAX)) as u64
    }

    /// Returns the budget still available at the current Tokio clock instant.
    #[must_use]
    pub fn remaining(self) -> Duration {
        self.expires_at.saturating_duration_since(tokio::time::Instant::now())
    }

    /// Returns whether the deadline has elapsed.
    #[must_use]
    pub fn is_expired(self) -> bool {
        tokio::time::Instant::now() >= self.expires_at
    }

    /// Rejects work that has not started its socket write before expiry.
    ///
    /// # Errors
    ///
    /// Returns a typed before-send error once the immutable deadline has elapsed.
    pub fn ensure_before_send(self, addr: impl Into<String>) -> RocketMQResult<()> {
        if self.is_expired() {
            Err(RocketMQError::network_deadline_exceeded_before_send(addr))
        } else {
            Ok(())
        }
    }

    /// Runs a future against this exact absolute deadline.
    pub async fn timeout<F>(self, future: F) -> Result<F::Output, tokio::time::error::Elapsed>
    where
        F: Future,
    {
        tokio::time::timeout_at(self.expires_at, future).await
    }
}
