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

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

use super::budget::BudgetAcquireError;
use super::budget::ResourceBudget;
use super::budget::ResourcePermit;
use super::limit::BudgetClass;
use super::limit::BudgetDimension;
use super::limit::FullPolicy;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Identifies the queue push outcome state.
pub enum QueuePushOutcome {
    /// Represents the enqueued case.
    Enqueued,
    /// Represents the coalesced case.
    Coalesced {
        /// The replaced value.
        replaced: usize,
    },
    /// Represents the dropped stale case.
    DroppedStale {
        /// The dropped value.
        dropped: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Identifies the queue push error kind state.
pub enum QueuePushErrorKind {
    /// Represents the budget exhausted case.
    BudgetExhausted(BudgetAcquireError),
    /// The absolute admission deadline elapsed before capacity became available.
    DeadlineExceeded,
    /// Represents the closed case.
    Closed,
    /// Represents the slow consumer closed case.
    SlowConsumerClosed,
}

/// Represents queue push error.
pub struct QueuePushError<T> {
    kind: QueuePushErrorKind,
    item: T,
}

impl<T> QueuePushError<T> {
    #[must_use]
    /// Returns the kind.
    pub fn kind(&self) -> &QueuePushErrorKind {
        &self.kind
    }

    #[must_use]
    /// Converts this value into item.
    pub fn into_item(self) -> T {
        self.item
    }
}

impl<T> fmt::Debug for QueuePushError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueuePushError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl<T> fmt::Display for QueuePushError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            QueuePushErrorKind::BudgetExhausted(error) => error.fmt(formatter),
            QueuePushErrorKind::DeadlineExceeded => {
                formatter.write_str("resource-budgeted queue admission deadline exceeded")
            }
            QueuePushErrorKind::Closed => formatter.write_str("resource-budgeted queue is closed"),
            QueuePushErrorKind::SlowConsumerClosed => {
                formatter.write_str("resource-budgeted queue closed its slow consumer")
            }
        }
    }
}

impl<T: fmt::Debug> std::error::Error for QueuePushError<T> {}

/// Represents budgeted item.
pub struct BudgetedItem<T> {
    item: T,
    enqueued_at: Duration,
    permit: ResourcePermit,
}

impl<T> BudgetedItem<T> {
    #[must_use]
    /// Converts this value into item.
    pub fn into_item(self) -> T {
        self.item
    }

    #[must_use]
    /// Converts this value into parts.
    pub fn into_parts(self) -> (T, ResourcePermit, Duration) {
        (self.item, self.permit, self.enqueued_at)
    }

    #[must_use]
    /// Returns the enqueued at.
    pub const fn enqueued_at(&self) -> Duration {
        self.enqueued_at
    }

    #[must_use]
    /// Returns the retained bytes.
    pub fn retained_bytes(&self) -> usize {
        self.permit.bytes()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Represents queue snapshot.
pub struct QueueSnapshot {
    /// The path value.
    pub path: Arc<str>,
    /// The depth value.
    pub depth: usize,
    /// The number of reserved entries.
    pub reserved_count: usize,
    /// The retained size in bytes.
    pub retained_bytes: usize,
    /// The oldest age value.
    pub oldest_age: Option<Duration>,
    /// The number of throttled entries.
    pub throttled_count: u64,
    /// The number of rejected entries.
    pub rejected_count: u64,
    /// The number of dropped entries.
    pub dropped_count: u64,
    /// The number of coalesced entries.
    pub coalesced_count: u64,
    /// The number of closed slow consumer entries.
    pub closed_slow_consumer_count: u64,
    /// The number of producers currently waiting for admission.
    pub waiters: usize,
    /// The cumulative number of producers that waited for admission.
    pub wait_count: u64,
    /// The number of items returned because their admission deadline elapsed.
    pub deadline_exceeded_count: u64,
    /// Whether closed.
    pub closed: bool,
}

/// Represents budgeted queue.
pub struct BudgetedQueue<T> {
    inner: Arc<QueueInner<T>>,
}

impl<T> Clone for BudgetedQueue<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> fmt::Debug for BudgetedQueue<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BudgetedQueue")
            .field("budget", &self.inner.budget)
            .finish_non_exhaustive()
    }
}

impl<T> BudgetedQueue<T> {
    #[must_use]
    /// Creates a new `BudgetedQueue`.
    pub fn new(budget: ResourceBudget) -> Self {
        Self {
            inner: Arc::new(QueueInner {
                budget,
                state: Mutex::new(QueueState {
                    items: VecDeque::new(),
                    closed: false,
                }),
                coalesce_push: Mutex::new(()),
                notify: Notify::new(),
                metrics: QueueMetrics::default(),
            }),
        }
    }

    /// Returns whether two handles refer to the same physical queue.
    #[must_use]
    pub fn is_same_queue(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// Attempts to push.
    pub fn try_push(
        &self,
        item: T,
        retained_bytes: usize,
        class: BudgetClass,
    ) -> Result<QueuePushOutcome, QueuePushError<T>> {
        let _coalesce_guard = if self.inner.budget.limit().full_policy == FullPolicy::CoalesceLatest {
            Some(
                self.inner
                    .coalesce_push
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            )
        } else {
            None
        };
        let dropped = self.apply_age_policy();
        match self.inner.budget.try_acquire(retained_bytes, class) {
            Ok(permit) => {
                self.enqueue(item, permit)?;
                if dropped == 0 {
                    Ok(QueuePushOutcome::Enqueued)
                } else {
                    Ok(QueuePushOutcome::DroppedStale { dropped })
                }
            }
            Err(error) => self.handle_full(item, retained_bytes, class, error),
        }
    }

    /// Attempts to push data.
    pub fn try_push_data(&self, item: T, retained_bytes: usize) -> Result<QueuePushOutcome, QueuePushError<T>> {
        self.try_push(item, retained_bytes, BudgetClass::Data)
    }

    /// Attempts to push control.
    pub fn try_push_control(&self, item: T, retained_bytes: usize) -> Result<QueuePushOutcome, QueuePushError<T>> {
        self.try_push(item, retained_bytes, BudgetClass::Control)
    }

    /// Pushes one item, waiting for count and byte capacity until an absolute
    /// Tokio deadline when this queue uses [`FullPolicy::WaitUntilDeadline`].
    ///
    /// Other full policies retain their existing immediate behavior. Rate
    /// exhaustion remains an immediate [`QueuePushErrorKind::BudgetExhausted`]
    /// result because capacity-release notifications do not represent token
    /// refill time.
    ///
    /// # Errors
    ///
    /// Returns the original item with [`QueuePushErrorKind::DeadlineExceeded`]
    /// when the deadline elapses, [`QueuePushErrorKind::Closed`] when the queue
    /// closes, or [`QueuePushErrorKind::BudgetExhausted`] when the item can
    /// never fit or rate capacity is unavailable.
    pub async fn push_until(
        &self,
        item: T,
        retained_bytes: usize,
        class: BudgetClass,
        deadline: Instant,
    ) -> Result<QueuePushOutcome, QueuePushError<T>> {
        if self.inner.budget.limit().full_policy != FullPolicy::WaitUntilDeadline {
            return self.try_push(item, retained_bytes, class);
        }
        if self.is_closed() {
            return Err(QueuePushError {
                kind: QueuePushErrorKind::Closed,
                item,
            });
        }
        if let Some(error) = self.inner.budget.permanent_acquire_error(retained_bytes, class) {
            return Err(QueuePushError {
                kind: QueuePushErrorKind::BudgetExhausted(error),
                item,
            });
        }

        let mut waiter = None;
        loop {
            let capacity_notified = self.inner.budget.capacity_notify().notified();
            tokio::pin!(capacity_notified);
            capacity_notified.as_mut().enable();
            let state_notified = self.inner.notify.notified();
            tokio::pin!(state_notified);
            state_notified.as_mut().enable();

            if self.is_closed() {
                return Err(QueuePushError {
                    kind: QueuePushErrorKind::Closed,
                    item,
                });
            }
            if Instant::now() >= deadline {
                self.inner.metrics.record_deadline_exceeded();
                return Err(QueuePushError {
                    kind: QueuePushErrorKind::DeadlineExceeded,
                    item,
                });
            }

            match self.inner.budget.try_acquire_waiting(retained_bytes, class) {
                Ok(permit) => {
                    self.enqueue(item, permit)?;
                    return Ok(QueuePushOutcome::Enqueued);
                }
                Err(error) if error.dimension() == BudgetDimension::Rate => {
                    self.inner.budget.record_acquire_error(&error);
                    return Err(QueuePushError {
                        kind: QueuePushErrorKind::BudgetExhausted(error),
                        item,
                    });
                }
                Err(_) => {}
            }

            if waiter.is_none() {
                waiter = Some(self.inner.metrics.begin_wait());
            }
            let deadline_sleep = tokio::time::sleep_until(deadline);
            tokio::pin!(deadline_sleep);
            tokio::select! {
                biased;
                () = &mut deadline_sleep => {
                    self.inner.metrics.record_deadline_exceeded();
                    return Err(QueuePushError {
                        kind: QueuePushErrorKind::DeadlineExceeded,
                        item,
                    });
                }
                () = &mut state_notified => {}
                () = &mut capacity_notified => {}
            }
        }
    }

    /// Attempts to pop.
    pub fn try_pop(&self) -> Option<T> {
        self.try_pop_budgeted().map(BudgetedItem::into_item)
    }

    /// Attempts to pop budgeted.
    pub fn try_pop_budgeted(&self) -> Option<BudgetedItem<T>> {
        self.apply_age_policy();
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.items.pop_front()
    }

    /// Retains queued items that satisfy `keep` and releases the resource
    /// permits owned by every removed item.
    ///
    /// The relative order of retained items is preserved.
    pub fn retain(&self, mut keep: impl FnMut(&T) -> bool) -> usize {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous_len = state.items.len();
        state.items.retain(|item| keep(&item.item));
        previous_len - state.items.len()
    }

    /// Returns the recv.
    pub async fn recv(&self) -> Option<T> {
        self.recv_budgeted().await.map(BudgetedItem::into_item)
    }

    /// Returns the recv budgeted.
    pub async fn recv_budgeted(&self) -> Option<BudgetedItem<T>> {
        loop {
            let notified = self.inner.notify.notified();
            if let Some(item) = self.try_pop_budgeted() {
                return Some(item);
            }
            if self.is_closed() {
                return None;
            }
            notified.await;
        }
    }

    /// Executes close.
    pub fn close(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
        drop(state);
        self.inner.notify.notify_waiters();
    }

    #[must_use]
    /// Returns whether closed.
    pub fn is_closed(&self) -> bool {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed
    }

    #[must_use]
    /// Returns the len.
    pub fn len(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .items
            .len()
    }

    #[must_use]
    /// Returns whether empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    /// Returns the snapshot.
    pub fn snapshot(&self) -> QueueSnapshot {
        let now = self.inner.budget.monotonic_now();
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let budget = self.inner.budget.snapshot();
        QueueSnapshot {
            path: budget.path,
            depth: state.items.len(),
            reserved_count: budget.current_count,
            retained_bytes: budget.current_bytes,
            oldest_age: state.items.front().map(|entry| now.saturating_sub(entry.enqueued_at)),
            throttled_count: budget.throttled_count,
            rejected_count: budget.rejected_count,
            dropped_count: budget.dropped_count,
            coalesced_count: budget.coalesced_count,
            closed_slow_consumer_count: budget.closed_slow_consumer_count,
            waiters: self.inner.metrics.waiters.load(Ordering::Acquire),
            wait_count: self.inner.metrics.wait_count.load(Ordering::Relaxed),
            deadline_exceeded_count: self.inner.metrics.deadline_exceeded_count.load(Ordering::Relaxed),
            closed: state.closed,
        }
    }

    fn enqueue(&self, item: T, permit: ResourcePermit) -> Result<(), QueuePushError<T>> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return Err(QueuePushError {
                kind: QueuePushErrorKind::Closed,
                item,
            });
        }
        state.items.push_back(BudgetedItem {
            item,
            enqueued_at: self.inner.budget.monotonic_now(),
            permit,
        });
        drop(state);
        self.inner.notify.notify_one();
        Ok(())
    }

    fn handle_full(
        &self,
        item: T,
        retained_bytes: usize,
        class: BudgetClass,
        error: BudgetAcquireError,
    ) -> Result<QueuePushOutcome, QueuePushError<T>> {
        match self.inner.budget.limit().full_policy {
            FullPolicy::Reject | FullPolicy::WaitUntilDeadline | FullPolicy::DropStale => Err(QueuePushError {
                kind: QueuePushErrorKind::BudgetExhausted(error),
                item,
            }),
            FullPolicy::CoalesceLatest => {
                if error.dimension() == BudgetDimension::Rate || error.exhausted_path() != self.inner.budget.path() {
                    return Err(QueuePushError {
                        kind: QueuePushErrorKind::BudgetExhausted(error),
                        item,
                    });
                }
                if !self.item_can_fit(retained_bytes, class) {
                    return Err(QueuePushError {
                        kind: QueuePushErrorKind::BudgetExhausted(error),
                        item,
                    });
                }
                let replaced = {
                    let mut state = self
                        .inner
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if state.closed {
                        return Err(QueuePushError {
                            kind: QueuePushErrorKind::Closed,
                            item,
                        });
                    }
                    let replaced = state.items.len();
                    state.items.clear();
                    replaced
                };
                self.inner.budget.record_coalesced(replaced);
                let permit = match self.inner.budget.try_acquire(retained_bytes, class) {
                    Ok(permit) => permit,
                    Err(retry_error) => {
                        return Err(QueuePushError {
                            kind: QueuePushErrorKind::BudgetExhausted(retry_error),
                            item,
                        });
                    }
                };
                self.enqueue(item, permit)?;
                Ok(QueuePushOutcome::Coalesced { replaced })
            }
            FullPolicy::CloseSlowConsumer => {
                let dropped = {
                    let mut state = self
                        .inner
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state.closed = true;
                    let dropped = state.items.len();
                    state.items.clear();
                    dropped
                };
                self.inner.budget.record_dropped(dropped);
                self.inner.budget.record_slow_consumer_closed();
                self.inner.notify.notify_waiters();
                Err(QueuePushError {
                    kind: QueuePushErrorKind::SlowConsumerClosed,
                    item,
                })
            }
        }
    }

    fn item_can_fit(&self, retained_bytes: usize, class: BudgetClass) -> bool {
        let limit = self.inner.budget.limit();
        let (count_capacity, byte_capacity) = match class {
            BudgetClass::Data => (
                limit.capacity.count.saturating_sub(limit.control_reserve.count),
                limit.capacity.bytes.saturating_sub(limit.control_reserve.bytes),
            ),
            BudgetClass::Control => (limit.capacity.count, limit.capacity.bytes),
        };
        count_capacity > 0 && retained_bytes <= byte_capacity
    }

    fn apply_age_policy(&self) -> usize {
        let Some(max_age) = self.inner.budget.limit().max_age else {
            return 0;
        };
        let policy = self.inner.budget.limit().full_policy;
        if !matches!(policy, FullPolicy::DropStale | FullPolicy::CloseSlowConsumer) {
            return 0;
        }
        let now = self.inner.budget.monotonic_now();
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if policy == FullPolicy::CloseSlowConsumer
            && state
                .items
                .front()
                .is_some_and(|entry| now.saturating_sub(entry.enqueued_at) >= max_age)
        {
            state.closed = true;
            let dropped = state.items.len();
            state.items.clear();
            drop(state);
            self.inner.budget.record_dropped(dropped);
            self.inner.budget.record_slow_consumer_closed();
            self.inner.notify.notify_waiters();
            return dropped;
        }
        let mut dropped = 0;
        while state
            .items
            .front()
            .is_some_and(|entry| now.saturating_sub(entry.enqueued_at) >= max_age)
        {
            state.items.pop_front();
            dropped += 1;
        }
        drop(state);
        self.inner.budget.record_dropped(dropped);
        dropped
    }
}

struct QueueInner<T> {
    budget: ResourceBudget,
    state: Mutex<QueueState<T>>,
    coalesce_push: Mutex<()>,
    notify: Notify,
    metrics: QueueMetrics,
}

struct QueueState<T> {
    items: VecDeque<BudgetedItem<T>>,
    closed: bool,
}

#[derive(Default)]
struct QueueMetrics {
    waiters: AtomicUsize,
    wait_count: AtomicU64,
    deadline_exceeded_count: AtomicU64,
}

impl QueueMetrics {
    fn begin_wait(&self) -> WaiterGuard<'_> {
        self.wait_count.fetch_add(1, Ordering::Relaxed);
        self.waiters.fetch_add(1, Ordering::AcqRel);
        WaiterGuard { metrics: self }
    }

    fn record_deadline_exceeded(&self) {
        self.deadline_exceeded_count.fetch_add(1, Ordering::Relaxed);
    }
}

struct WaiterGuard<'a> {
    metrics: &'a QueueMetrics,
}

impl Drop for WaiterGuard<'_> {
    fn drop(&mut self) {
        let previous = self.metrics.waiters.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
    }
}
