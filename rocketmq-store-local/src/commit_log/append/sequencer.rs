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

//! Bounded FIFO admission and micro-batch draining for CommitLog append requests.

use std::fmt;

use rocketmq_runtime::resource_budget::BudgetConfigError;
use rocketmq_runtime::resource_budget::BudgetLimit;
use rocketmq_runtime::resource_budget::BudgetedItem;
use rocketmq_runtime::resource_budget::BudgetedQueue;
use rocketmq_runtime::resource_budget::FullPolicy;
use rocketmq_runtime::resource_budget::QueuePushErrorKind;
use rocketmq_runtime::resource_budget::QueueSnapshot;
use rocketmq_runtime::resource_budget::ResourceBudgetTree;
use tokio_util::sync::CancellationToken;

use super::micro_batch::MicroBatch;
use super::micro_batch::MicroBatchPolicy;

/// Bounded queue capacities and drain policy for one CommitLog sequencer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendSequencerConfig {
    /// Maximum admitted requests whose processing permits have not yet been released.
    pub queue_capacity: usize,
    /// Maximum aggregate request bytes retained by queued and in-flight requests.
    pub queue_bytes: usize,
    /// Adjacent FIFO drain policy.
    pub micro_batch: MicroBatchPolicy,
}

/// Factory namespace for the single-consumer CommitLog append queue.
pub struct AppendSequencer;

impl AppendSequencer {
    /// Creates cloneable admission and exclusive drain halves.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetConfigError`] when configured queue capacities are invalid.
    pub fn bounded<T>(
        config: AppendSequencerConfig,
    ) -> Result<(AppendSequencerSender<T>, AppendSequencerReceiver<T>), BudgetConfigError> {
        let budget = ResourceBudgetTree::new(
            "commitlog-append",
            BudgetLimit::new(config.queue_capacity, config.queue_bytes, FullPolicy::Reject),
        )?;
        let queue = BudgetedQueue::new(budget.root());
        Ok((
            AppendSequencerSender { queue: queue.clone() },
            AppendSequencerReceiver {
                queue,
                policy: config.micro_batch,
                carry: None,
            },
        ))
    }
}

/// Cloneable bounded-admission port for append callers.
pub struct AppendSequencerSender<T> {
    queue: BudgetedQueue<T>,
}

impl<T> Clone for AppendSequencerSender<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
        }
    }
}

impl<T> fmt::Debug for AppendSequencerSender<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AppendSequencerSender")
            .field("queue", &self.queue.snapshot())
            .finish()
    }
}

impl<T> AppendSequencerSender<T> {
    /// Attempts to admit one request without waiting for capacity.
    ///
    /// # Errors
    ///
    /// Returns the original request when the byte/count budget is saturated or the sequencer is
    /// closed.
    pub fn try_submit(&self, request: T, retained_bytes: usize) -> Result<(), AppendAdmissionError<T>> {
        self.queue
            .try_push_data(request, retained_bytes)
            .map(|_| ())
            .map_err(|error| {
                let kind = match error.kind() {
                    QueuePushErrorKind::BudgetExhausted(_) | QueuePushErrorKind::DeadlineExceeded => {
                        AppendAdmissionErrorKind::Saturated
                    }
                    QueuePushErrorKind::Closed | QueuePushErrorKind::SlowConsumerClosed => {
                        AppendAdmissionErrorKind::Closed
                    }
                };
                AppendAdmissionError {
                    kind,
                    request: error.into_item(),
                }
            })
    }

    /// Stops new admission and wakes the drain owner. Already admitted requests remain queued.
    pub fn close(&self) {
        self.queue.close();
    }

    /// Closes admission and drops every request that has not reached the exclusive receiver.
    ///
    /// This is reserved for terminal consumer failure so producers are released instead of
    /// waiting forever on work that can no longer be processed.
    pub fn close_and_discard_pending(&self) -> usize {
        self.queue.close();
        let mut discarded = 0;
        while self.queue.try_pop_budgeted().is_some() {
            discarded += 1;
        }
        discarded
    }

    /// Returns the current bounded queue state.
    #[must_use]
    pub fn snapshot(&self) -> QueueSnapshot {
        self.queue.snapshot()
    }
}

/// Exclusive FIFO drain owner.
pub struct AppendSequencerReceiver<T> {
    queue: BudgetedQueue<T>,
    policy: MicroBatchPolicy,
    carry: Option<BudgetedItem<T>>,
}

impl<T> AppendSequencerReceiver<T> {
    /// Drains the next bounded FIFO micro-batch.
    ///
    /// Cancellation closes admission, then preserves and drains already admitted requests. The
    /// queue receive branch is biased ahead of cancellation and deadline branches so an item is
    /// never popped by a losing `select!` branch.
    pub async fn next_batch(&mut self, cancellation: &CancellationToken) -> Option<MicroBatch<T>> {
        let first = if let Some(item) = self.carry.take() {
            Some(item)
        } else {
            tokio::select! {
                biased;
                item = self.queue.recv_budgeted() => item,
                () = cancellation.cancelled() => {
                    self.queue.close();
                    self.queue.recv_budgeted().await
                }
            }
        }?;

        let mut retained_bytes = first.retained_bytes();
        let mut items = Vec::with_capacity(self.policy.max_items());
        items.push(first);
        if self.policy.max_items() == 1 {
            return Some(MicroBatch::new(items, retained_bytes));
        }

        let deadline = tokio::time::Instant::now() + self.policy.max_wait();
        loop {
            if items.len() >= self.policy.max_items() {
                break;
            }
            if let Some(next) = self.queue.try_pop_budgeted() {
                if !self.try_include(next, &mut items, &mut retained_bytes) {
                    break;
                }
                continue;
            }
            if self.queue.is_closed() || self.policy.max_wait().is_zero() {
                break;
            }

            enum WaitOutcome<T> {
                Item(Option<BudgetedItem<T>>),
                Cancelled,
                Deadline,
            }
            let outcome = tokio::select! {
                biased;
                item = tokio::time::timeout_at(deadline, self.queue.recv_budgeted()) => {
                    match item {
                        Ok(item) => WaitOutcome::Item(item),
                        Err(_) => WaitOutcome::Deadline,
                    }
                },
                () = cancellation.cancelled() => WaitOutcome::Cancelled,
            };
            match outcome {
                WaitOutcome::Item(Some(next)) => {
                    if !self.try_include(next, &mut items, &mut retained_bytes) {
                        break;
                    }
                }
                WaitOutcome::Item(None) | WaitOutcome::Deadline => break,
                WaitOutcome::Cancelled => {
                    self.queue.close();
                }
            }
        }

        Some(MicroBatch::new(items, retained_bytes))
    }

    fn try_include(
        &mut self,
        next: BudgetedItem<T>,
        items: &mut Vec<BudgetedItem<T>>,
        retained_bytes: &mut usize,
    ) -> bool {
        let next_bytes = next.retained_bytes();
        if retained_bytes
            .checked_add(next_bytes)
            .is_none_or(|total| total > self.policy.max_bytes())
        {
            self.carry = Some(next);
            return false;
        }
        *retained_bytes += next_bytes;
        items.push(next);
        true
    }
}

/// Admission rejection category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendAdmissionErrorKind {
    /// Count or byte capacity is exhausted.
    Saturated,
    /// The sequencer no longer accepts requests.
    Closed,
}

/// Rejected append request, retaining ownership for caller-side error projection.
pub struct AppendAdmissionError<T> {
    kind: AppendAdmissionErrorKind,
    request: T,
}

impl<T> AppendAdmissionError<T> {
    /// Returns the rejection category.
    #[must_use]
    pub const fn kind(&self) -> AppendAdmissionErrorKind {
        self.kind
    }

    /// Recovers the rejected request.
    #[must_use]
    pub fn into_request(self) -> T {
        self.request
    }
}

impl<T> fmt::Debug for AppendAdmissionError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AppendAdmissionError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl<T> fmt::Display for AppendAdmissionError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            AppendAdmissionErrorKind::Saturated => formatter.write_str("CommitLog append sequencer is saturated"),
            AppendAdmissionErrorKind::Closed => formatter.write_str("CommitLog append sequencer is closed"),
        }
    }
}

impl<T: fmt::Debug> std::error::Error for AppendAdmissionError<T> {}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::future::Future;
    use std::task::Poll;
    use std::time::Duration;

    use super::*;

    fn config(policy: MicroBatchPolicy) -> AppendSequencerConfig {
        AppendSequencerConfig {
            queue_capacity: 8,
            queue_bytes: 1024,
            micro_batch: policy,
        }
    }

    #[tokio::test]
    async fn drains_fifo_by_item_and_byte_limits() {
        let policy = MicroBatchPolicy::try_new(3, 5, Duration::ZERO).expect("policy");
        let (sender, mut receiver) = AppendSequencer::bounded(config(policy)).expect("sequencer");
        sender.try_submit(1, 2).expect("first");
        sender.try_submit(2, 2).expect("second");
        sender.try_submit(3, 2).expect("third");
        let cancellation = CancellationToken::new();

        let first = receiver.next_batch(&cancellation).await.expect("first batch");
        let first_items = first
            .into_budgeted_items()
            .into_iter()
            .map(BudgetedItem::into_item)
            .collect::<Vec<_>>();
        let second = receiver.next_batch(&cancellation).await.expect("second batch");
        let second_items = second
            .into_budgeted_items()
            .into_iter()
            .map(BudgetedItem::into_item)
            .collect::<Vec<_>>();

        assert_eq!(first_items, vec![1, 2]);
        assert_eq!(second_items, vec![3]);
    }

    #[tokio::test]
    async fn disabled_policy_preserves_single_request_batches() {
        let policy = MicroBatchPolicy::disabled(1024).expect("policy");
        let (sender, mut receiver) = AppendSequencer::bounded(config(policy)).expect("sequencer");
        sender.try_submit(1, 1).expect("first");
        sender.try_submit(2, 1).expect("second");
        let cancellation = CancellationToken::new();

        assert_eq!(receiver.next_batch(&cancellation).await.expect("first").len(), 1);
        assert_eq!(receiver.next_batch(&cancellation).await.expect("second").len(), 1);
    }

    #[tokio::test]
    async fn request_larger_than_batch_target_is_processed_as_a_singleton() {
        let policy = MicroBatchPolicy::try_new(4, 5, std::time::Duration::ZERO).expect("policy");
        let (sender, mut receiver) = AppendSequencer::bounded(config(policy)).expect("sequencer");
        sender.try_submit("oversized", 8).expect("oversized request fits queue");
        sender.try_submit("next", 1).expect("next");
        let cancellation = CancellationToken::new();

        let first = receiver.next_batch(&cancellation).await.expect("first");
        let second = receiver.next_batch(&cancellation).await.expect("second");

        assert_eq!(first.retained_bytes(), 8);
        assert_eq!(
            first.into_budgeted_items().pop().map(BudgetedItem::into_item),
            Some("oversized")
        );
        assert_eq!(
            second.into_budgeted_items().pop().map(BudgetedItem::into_item),
            Some("next")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn partial_batch_is_released_at_its_configured_deadline() {
        let policy = MicroBatchPolicy::try_new(4, 1024, Duration::from_millis(10)).expect("policy");
        let (sender, mut receiver) = AppendSequencer::bounded(config(policy)).expect("sequencer");
        sender.try_submit("first", 1).expect("first");
        let cancellation = CancellationToken::new();
        let mut next_batch = Box::pin(receiver.next_batch(&cancellation));

        let first_poll = poll_fn(|context| Poll::Ready(next_batch.as_mut().poll(context))).await;
        assert!(first_poll.is_pending());
        tokio::time::advance(Duration::from_millis(9)).await;
        let before_deadline = poll_fn(|context| Poll::Ready(next_batch.as_mut().poll(context))).await;
        assert!(before_deadline.is_pending());

        tokio::time::advance(Duration::from_millis(1)).await;
        let batch = next_batch.await.expect("deadline batch");

        assert_eq!(batch.len(), 1);
        assert_eq!(batch.retained_bytes(), 1);
    }

    #[tokio::test]
    async fn cancellation_closes_admission_but_drains_existing_fifo() {
        let policy = MicroBatchPolicy::try_new(8, 1024, Duration::from_millis(10)).expect("policy");
        let (sender, mut receiver) = AppendSequencer::bounded(config(policy)).expect("sequencer");
        sender.try_submit(1, 1).expect("first");
        sender.try_submit(2, 1).expect("second");
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let batch = receiver.next_batch(&cancellation).await.expect("drained batch");
        let items = batch
            .into_budgeted_items()
            .into_iter()
            .map(BudgetedItem::into_item)
            .collect::<Vec<_>>();

        assert_eq!(items, vec![1, 2]);
        assert_eq!(
            sender.try_submit(3, 1).expect_err("closed").kind(),
            AppendAdmissionErrorKind::Closed
        );
        assert!(receiver.next_batch(&cancellation).await.is_none());
    }

    #[test]
    fn rejects_count_and_byte_saturation_without_losing_requests() {
        let policy = MicroBatchPolicy::disabled(1024).expect("policy");
        let config = AppendSequencerConfig {
            queue_capacity: 1,
            queue_bytes: 4,
            micro_batch: policy,
        };
        let (sender, _receiver) = AppendSequencer::bounded(config).expect("sequencer");
        sender.try_submit("first", 4).expect("first");

        let error = sender.try_submit("second", 1).expect_err("count saturated");

        assert_eq!(error.kind(), AppendAdmissionErrorKind::Saturated);
        assert_eq!(error.into_request(), "second");
    }

    #[test]
    fn terminal_consumer_failure_releases_pending_queue_budget() {
        let policy = MicroBatchPolicy::disabled(1024).expect("policy");
        let (sender, _receiver) = AppendSequencer::bounded(config(policy)).expect("sequencer");
        sender.try_submit("first", 4).expect("first");
        sender.try_submit("second", 8).expect("second");

        assert_eq!(sender.close_and_discard_pending(), 2);

        let snapshot = sender.snapshot();
        assert!(snapshot.closed);
        assert_eq!(snapshot.depth, 0);
        assert_eq!(snapshot.reserved_count, 0);
        assert_eq!(snapshot.retained_bytes, 0);
    }
}
