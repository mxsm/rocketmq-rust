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

//! CommitLog micro-batch policy and resource-permit preserving batch container.

use std::time::Duration;

use rocketmq_runtime::resource_budget::BudgetedItem;

/// Limits applied while draining adjacent FIFO append requests.
///
/// `max_bytes` is an aggregation limit rather than an admission limit. A single request larger
/// than that value is still processed as a one-item batch when it fits the sequencer queue budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MicroBatchPolicy {
    max_items: usize,
    max_bytes: usize,
    max_wait: Duration,
}

impl MicroBatchPolicy {
    /// Creates an enabled micro-batch policy.
    ///
    /// # Errors
    ///
    /// Returns [`MicroBatchPolicyError`] when either hard limit is zero.
    pub const fn try_new(
        max_items: usize,
        max_bytes: usize,
        max_wait: Duration,
    ) -> Result<Self, MicroBatchPolicyError> {
        if max_items == 0 {
            return Err(MicroBatchPolicyError::ZeroMaxItems);
        }
        if max_bytes == 0 {
            return Err(MicroBatchPolicyError::ZeroMaxBytes);
        }
        Ok(Self {
            max_items,
            max_bytes,
            max_wait,
        })
    }

    /// Creates a policy that preserves sequencer ownership without request aggregation.
    ///
    /// # Errors
    ///
    /// Returns [`MicroBatchPolicyError::ZeroMaxBytes`] when the queue has no byte capacity.
    pub const fn disabled(max_payload_bytes: usize) -> Result<Self, MicroBatchPolicyError> {
        Self::try_new(1, max_payload_bytes, Duration::ZERO)
    }

    /// Returns the maximum number of requests in one drain.
    #[must_use]
    pub const fn max_items(self) -> usize {
        self.max_items
    }

    /// Returns the aggregate-byte target for one drain.
    #[must_use]
    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    /// Returns how long a non-full drain may wait for adjacent requests.
    #[must_use]
    pub const fn max_wait(self) -> Duration {
        self.max_wait
    }
}

/// Invalid micro-batch policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MicroBatchPolicyError {
    /// A batch cannot have a zero item limit.
    #[error("CommitLog micro-batch max-items must be greater than zero")]
    ZeroMaxItems,
    /// A batch cannot have a zero byte limit.
    #[error("CommitLog micro-batch max-bytes must be greater than zero")]
    ZeroMaxBytes,
}

/// One FIFO drain whose resource permits remain held until each item is consumed.
pub struct MicroBatch<T> {
    items: Vec<BudgetedItem<T>>,
    retained_bytes: usize,
}

impl<T> MicroBatch<T> {
    pub(crate) fn new(items: Vec<BudgetedItem<T>>, retained_bytes: usize) -> Self {
        Self { items, retained_bytes }
    }

    /// Returns the number of append requests in this drain.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns whether this drain is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns aggregate bytes retained by all queue permits.
    #[must_use]
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Consumes the batch while preserving each permit alongside its request.
    #[must_use]
    pub fn into_budgeted_items(self) -> Vec<BudgetedItem<T>> {
        self.items
    }
}
