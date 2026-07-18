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

//! Bounded admission contract for telemetry exporters during collector outages.
//!
//! The data plane uses [`TelemetryOutageQueue::try_enqueue`], which never waits for the exporter
//! lock. Export workers drain batches on their own owned lifecycle. Shutdown closes admission and
//! uses one caller-provided absolute deadline; reaching that deadline drops and reports the
//! remaining telemetry instead of extending the service shutdown budget.

use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use std::sync::TryLockError;
use std::time::Instant;

/// Default maximum number of telemetry records admitted during a collector outage.
pub const DEFAULT_MAX_QUEUE_ITEMS: usize = 2_048;

/// Default maximum estimated bytes admitted during a collector outage.
pub const DEFAULT_MAX_QUEUE_BYTES: usize = 8 * 1024 * 1024;

/// Default maximum size of one admitted telemetry record.
pub const DEFAULT_MAX_RECORD_BYTES: usize = 64 * 1024;

/// Default maximum number of records returned in one exporter batch.
pub const DEFAULT_MAX_EXPORT_BATCH_ITEMS: usize = 512;

/// Default interval before a partial exporter batch is flushed.
pub const DEFAULT_SCHEDULED_DELAY_MILLIS: u64 = 5_000;

/// Default maximum duration of one collector export attempt.
pub const DEFAULT_EXPORT_TIMEOUT_MILLIS: u64 = 3_000;

/// Default total deadline for telemetry provider shutdown.
pub const DEFAULT_SHUTDOWN_TIMEOUT_MILLIS: u64 = 5_000;

/// Count and byte limits for an exporter outage queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelemetryQueueLimits {
    /// Maximum number of queued records.
    max_items: usize,
    /// Maximum sum of estimated record bytes.
    max_bytes: usize,
    /// Maximum estimated size of one record.
    max_record_bytes: usize,
}

impl TelemetryQueueLimits {
    /// Creates validated non-zero queue limits.
    ///
    /// # Errors
    ///
    /// Returns [`TelemetryQueueConfigError`] when a limit is zero or one record could exceed the
    /// entire byte budget.
    pub fn new(max_items: usize, max_bytes: usize, max_record_bytes: usize) -> Result<Self, TelemetryQueueConfigError> {
        if max_items == 0 {
            return Err(TelemetryQueueConfigError::ZeroLimit("max_items"));
        }
        if max_bytes == 0 {
            return Err(TelemetryQueueConfigError::ZeroLimit("max_bytes"));
        }
        if max_record_bytes == 0 {
            return Err(TelemetryQueueConfigError::ZeroLimit("max_record_bytes"));
        }
        if max_record_bytes > max_bytes {
            return Err(TelemetryQueueConfigError::RecordExceedsQueue {
                max_record_bytes,
                max_queue_bytes: max_bytes,
            });
        }
        Ok(Self {
            max_items,
            max_bytes,
            max_record_bytes,
        })
    }

    /// Returns the maximum number of queued records.
    pub const fn max_items(self) -> usize {
        self.max_items
    }

    /// Returns the maximum sum of estimated queued bytes.
    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    /// Returns the maximum estimated size of one record.
    pub const fn max_record_bytes(self) -> usize {
        self.max_record_bytes
    }
}

impl Default for TelemetryQueueLimits {
    fn default() -> Self {
        Self {
            max_items: DEFAULT_MAX_QUEUE_ITEMS,
            max_bytes: DEFAULT_MAX_QUEUE_BYTES,
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
        }
    }
}

/// Invalid telemetry outage queue configuration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TelemetryQueueConfigError {
    /// A queue limit was configured as zero.
    #[error("telemetry queue limit {0} must be greater than zero")]
    ZeroLimit(&'static str),
    /// The maximum record size exceeds the entire byte budget.
    #[error("telemetry max_record_bytes {max_record_bytes} exceeds max_queue_bytes {max_queue_bytes}")]
    RecordExceedsQueue {
        /// Maximum configured record size.
        max_record_bytes: usize,
        /// Maximum configured queue bytes.
        max_queue_bytes: usize,
    },
}

/// Reason a telemetry record was dropped before export.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetryDropReason {
    /// The queue has stopped accepting records for shutdown.
    Closed,
    /// The queue item limit was reached.
    ItemLimit,
    /// The queue byte limit was reached.
    ByteLimit,
    /// The individual record exceeded its byte limit.
    RecordTooLarge,
    /// The exporter lock was contended or poisoned; the data plane did not wait.
    LockUnavailable,
}

/// Result of non-blocking telemetry admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetryEnqueueOutcome {
    /// The record was admitted.
    Accepted,
    /// The record was dropped for the supplied reason.
    Dropped(TelemetryDropReason),
}

/// Current queue and lifetime counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelemetryQueueSnapshot {
    /// Records currently waiting for export.
    pub queued_items: usize,
    /// Estimated bytes currently waiting for export.
    pub queued_bytes: usize,
    /// Records admitted over the queue lifetime.
    pub accepted_items: u64,
    /// Estimated bytes admitted over the queue lifetime.
    pub accepted_bytes: u64,
    /// Records removed by exporter workers.
    pub drained_items: u64,
    /// Estimated bytes removed by exporter workers.
    pub drained_bytes: u64,
    /// Records dropped before export or at the shutdown deadline.
    pub dropped_items: u64,
    /// Estimated bytes dropped before export or at the shutdown deadline.
    pub dropped_bytes: u64,
    /// Whether new admission is closed.
    pub closed: bool,
}

/// Final state produced when an outage queue drains or reaches its absolute deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelemetryOutageShutdownReport {
    /// Whether the absolute shutdown deadline expired with queued telemetry remaining.
    pub timed_out: bool,
    /// Records dropped specifically because the deadline expired.
    pub deadline_dropped_items: u64,
    /// Estimated bytes dropped specifically because the deadline expired.
    pub deadline_dropped_bytes: u64,
    /// Final queue and lifetime counters.
    pub snapshot: TelemetryQueueSnapshot,
}

#[derive(Debug)]
struct Queued<T> {
    item: T,
    estimated_bytes: usize,
}

#[derive(Debug)]
struct QueueState<T> {
    records: VecDeque<Queued<T>>,
    queued_bytes: usize,
    closed: bool,
    shutdown_deadline: Option<Instant>,
}

impl<T> Default for QueueState<T> {
    fn default() -> Self {
        Self {
            records: VecDeque::new(),
            queued_bytes: 0,
            closed: false,
            shutdown_deadline: None,
        }
    }
}

/// A count-and-byte bounded queue for exporter adapters.
///
/// This type does not create a task or thread. The exporter remains owned by the service's
/// existing lifecycle and calls [`Self::drain_batch`] from that owned worker.
#[derive(Debug)]
pub struct TelemetryOutageQueue<T> {
    limits: TelemetryQueueLimits,
    state: Mutex<QueueState<T>>,
    accepted_items: AtomicU64,
    accepted_bytes: AtomicU64,
    drained_items: AtomicU64,
    drained_bytes: AtomicU64,
    dropped_items: AtomicU64,
    dropped_bytes: AtomicU64,
}

impl<T> TelemetryOutageQueue<T> {
    /// Creates an empty queue with validated limits.
    pub fn new(limits: TelemetryQueueLimits) -> Self {
        Self {
            limits,
            state: Mutex::new(QueueState::default()),
            accepted_items: AtomicU64::new(0),
            accepted_bytes: AtomicU64::new(0),
            drained_items: AtomicU64::new(0),
            drained_bytes: AtomicU64::new(0),
            dropped_items: AtomicU64::new(0),
            dropped_bytes: AtomicU64::new(0),
        }
    }

    /// Attempts to admit a telemetry record without waiting for an exporter lock.
    ///
    /// `estimated_bytes` must include the envelope and attribute payload estimate used by the
    /// owning exporter. A zero estimate is accounted as one byte so the byte budget always moves.
    pub fn try_enqueue(&self, item: T, estimated_bytes: usize) -> TelemetryEnqueueOutcome {
        let estimated_bytes = estimated_bytes.max(1);
        if estimated_bytes > self.limits.max_record_bytes {
            self.record_drop(estimated_bytes);
            return TelemetryEnqueueOutcome::Dropped(TelemetryDropReason::RecordTooLarge);
        }

        let mut state = match self.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock | TryLockError::Poisoned(_)) => {
                self.record_drop(estimated_bytes);
                return TelemetryEnqueueOutcome::Dropped(TelemetryDropReason::LockUnavailable);
            }
        };
        if state.closed {
            self.record_drop(estimated_bytes);
            return TelemetryEnqueueOutcome::Dropped(TelemetryDropReason::Closed);
        }
        if state.records.len() >= self.limits.max_items {
            self.record_drop(estimated_bytes);
            return TelemetryEnqueueOutcome::Dropped(TelemetryDropReason::ItemLimit);
        }
        if state.queued_bytes.saturating_add(estimated_bytes) > self.limits.max_bytes {
            self.record_drop(estimated_bytes);
            return TelemetryEnqueueOutcome::Dropped(TelemetryDropReason::ByteLimit);
        }

        state.records.push_back(Queued { item, estimated_bytes });
        state.queued_bytes += estimated_bytes;
        self.accepted_items.fetch_add(1, Ordering::Relaxed);
        self.accepted_bytes.fetch_add(estimated_bytes as u64, Ordering::Relaxed);
        TelemetryEnqueueOutcome::Accepted
    }

    /// Removes a bounded FIFO batch for an exporter worker.
    ///
    /// `max_items` and `max_bytes` are clamped to at least one. One admitted record may exceed
    /// `max_bytes`; it is still returned alone so a worker can always make progress.
    pub fn drain_batch(&self, max_items: usize, max_bytes: usize) -> Vec<T> {
        let mut state = self.lock_state();
        let mut items = Vec::new();
        let mut drained_bytes = 0usize;
        let max_items = max_items.max(1);
        let max_bytes = max_bytes.max(1);
        while items.len() < max_items {
            let Some(front) = state.records.front() else {
                break;
            };
            if !items.is_empty() && drained_bytes.saturating_add(front.estimated_bytes) > max_bytes {
                break;
            }
            let Some(record) = state.records.pop_front() else {
                break;
            };
            state.queued_bytes -= record.estimated_bytes;
            drained_bytes += record.estimated_bytes;
            items.push(record.item);
        }
        self.drained_items.fetch_add(items.len() as u64, Ordering::Relaxed);
        self.drained_bytes.fetch_add(drained_bytes as u64, Ordering::Relaxed);
        items
    }

    /// Stops new admission and binds shutdown to one absolute deadline.
    pub fn begin_shutdown(&self, deadline: Instant) {
        let mut state = self.lock_state();
        state.closed = true;
        state.shutdown_deadline = Some(deadline);
    }

    /// Returns a final report once the queue drains or the absolute deadline expires.
    ///
    /// Before the deadline, `None` means the exporter worker should continue draining. At the
    /// deadline, all remaining records are dropped and included in the report.
    pub fn poll_shutdown(&self, now: Instant) -> Option<TelemetryOutageShutdownReport> {
        let mut state = self.lock_state();
        if !state.closed {
            return None;
        }
        if state.records.is_empty() {
            return Some(TelemetryOutageShutdownReport {
                timed_out: false,
                deadline_dropped_items: 0,
                deadline_dropped_bytes: 0,
                snapshot: self.snapshot_from_state(&state),
            });
        }
        let deadline = state.shutdown_deadline?;
        if now < deadline {
            return None;
        }

        let deadline_dropped_items = state.records.len() as u64;
        let deadline_dropped_bytes = state.queued_bytes as u64;
        state.records.clear();
        state.queued_bytes = 0;
        self.dropped_items.fetch_add(deadline_dropped_items, Ordering::Relaxed);
        self.dropped_bytes.fetch_add(deadline_dropped_bytes, Ordering::Relaxed);
        Some(TelemetryOutageShutdownReport {
            timed_out: true,
            deadline_dropped_items,
            deadline_dropped_bytes,
            snapshot: self.snapshot_from_state(&state),
        })
    }

    /// Returns current queue occupancy and lifetime counters.
    pub fn snapshot(&self) -> TelemetryQueueSnapshot {
        let state = self.lock_state();
        self.snapshot_from_state(&state)
    }

    fn record_drop(&self, estimated_bytes: usize) {
        self.dropped_items.fetch_add(1, Ordering::Relaxed);
        self.dropped_bytes.fetch_add(estimated_bytes as u64, Ordering::Relaxed);
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, QueueState<T>> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn snapshot_from_state(&self, state: &QueueState<T>) -> TelemetryQueueSnapshot {
        TelemetryQueueSnapshot {
            queued_items: state.records.len(),
            queued_bytes: state.queued_bytes,
            accepted_items: self.accepted_items.load(Ordering::Relaxed),
            accepted_bytes: self.accepted_bytes.load(Ordering::Relaxed),
            drained_items: self.drained_items.load(Ordering::Relaxed),
            drained_bytes: self.drained_bytes.load(Ordering::Relaxed),
            dropped_items: self.dropped_items.load(Ordering::Relaxed),
            dropped_bytes: self.dropped_bytes.load(Ordering::Relaxed),
            closed: state.closed,
        }
    }
}

impl<T> Default for TelemetryOutageQueue<T> {
    fn default() -> Self {
        Self::new(TelemetryQueueLimits::default())
    }
}

#[cfg(feature = "otlp-traces")]
pub(crate) fn trace_batch_config() -> opentelemetry_sdk::trace::BatchConfig {
    opentelemetry_sdk::trace::BatchConfigBuilder::default()
        .with_max_queue_size(DEFAULT_MAX_QUEUE_ITEMS)
        .with_max_export_batch_size(DEFAULT_MAX_EXPORT_BATCH_ITEMS)
        .with_scheduled_delay(std::time::Duration::from_millis(DEFAULT_SCHEDULED_DELAY_MILLIS))
        .build()
}

#[cfg(feature = "otlp-logs")]
pub(crate) fn log_batch_config() -> opentelemetry_sdk::logs::BatchConfig {
    opentelemetry_sdk::logs::BatchConfigBuilder::default()
        .with_max_queue_size(DEFAULT_MAX_QUEUE_ITEMS)
        .with_max_export_batch_size(DEFAULT_MAX_EXPORT_BATCH_ITEMS)
        .with_scheduled_delay(std::time::Duration::from_millis(DEFAULT_SCHEDULED_DELAY_MILLIS))
        .build()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn queue() -> TelemetryOutageQueue<&'static str> {
        TelemetryOutageQueue::new(TelemetryQueueLimits::new(2, 8, 6).expect("test limits should be valid"))
    }

    #[test]
    fn queue_is_count_and_byte_bounded_and_drops_are_measurable() {
        let queue = queue();

        assert_eq!(queue.try_enqueue("one", 3), TelemetryEnqueueOutcome::Accepted);
        assert_eq!(queue.try_enqueue("two", 5), TelemetryEnqueueOutcome::Accepted);
        assert_eq!(
            queue.try_enqueue("count-full", 1),
            TelemetryEnqueueOutcome::Dropped(TelemetryDropReason::ItemLimit)
        );

        let snapshot = queue.snapshot();
        assert_eq!(snapshot.queued_items, 2);
        assert_eq!(snapshot.queued_bytes, 8);
        assert_eq!(snapshot.accepted_items, 2);
        assert_eq!(snapshot.dropped_items, 1);
        assert_eq!(snapshot.dropped_bytes, 1);
    }

    #[test]
    fn record_and_byte_limits_fail_closed() {
        let queue = queue();

        assert_eq!(
            queue.try_enqueue("oversized", 7),
            TelemetryEnqueueOutcome::Dropped(TelemetryDropReason::RecordTooLarge)
        );
        assert_eq!(queue.try_enqueue("five", 5), TelemetryEnqueueOutcome::Accepted);
        assert_eq!(
            queue.try_enqueue("byte-full", 4),
            TelemetryEnqueueOutcome::Dropped(TelemetryDropReason::ByteLimit)
        );
        assert_eq!(queue.snapshot().queued_bytes, 5);
    }

    #[test]
    fn data_plane_drops_instead_of_waiting_for_exporter_lock() {
        let queue = queue();
        let _exporter_lock = queue.state.lock().expect("test should hold exporter lock");

        assert_eq!(
            queue.try_enqueue("contended", 2),
            TelemetryEnqueueOutcome::Dropped(TelemetryDropReason::LockUnavailable)
        );
        assert_eq!(queue.dropped_items.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn exporter_drains_fifo_batches_with_bounded_accounting() {
        let queue = queue();
        assert_eq!(queue.try_enqueue("one", 3), TelemetryEnqueueOutcome::Accepted);
        assert_eq!(queue.try_enqueue("two", 5), TelemetryEnqueueOutcome::Accepted);

        assert_eq!(queue.drain_batch(2, 4), vec!["one"]);
        assert_eq!(queue.drain_batch(2, 8), vec!["two"]);
        let snapshot = queue.snapshot();
        assert_eq!(snapshot.queued_items, 0);
        assert_eq!(snapshot.queued_bytes, 0);
        assert_eq!(snapshot.drained_items, 2);
        assert_eq!(snapshot.drained_bytes, 8);
    }

    #[test]
    fn absolute_shutdown_deadline_reports_collector_outage() {
        let queue = queue();
        assert_eq!(queue.try_enqueue("one", 3), TelemetryEnqueueOutcome::Accepted);
        assert_eq!(queue.try_enqueue("two", 5), TelemetryEnqueueOutcome::Accepted);
        let deadline = Instant::now() + Duration::from_millis(10);
        queue.begin_shutdown(deadline);

        assert_eq!(
            queue.try_enqueue("closed", 1),
            TelemetryEnqueueOutcome::Dropped(TelemetryDropReason::Closed)
        );
        assert!(queue.poll_shutdown(deadline - Duration::from_millis(1)).is_none());
        let report = queue
            .poll_shutdown(deadline)
            .expect("absolute deadline should finish shutdown");

        assert!(report.timed_out);
        assert_eq!(report.deadline_dropped_items, 2);
        assert_eq!(report.deadline_dropped_bytes, 8);
        assert_eq!(report.snapshot.queued_items, 0);
        assert_eq!(report.snapshot.dropped_items, 3);
        assert_eq!(report.snapshot.dropped_bytes, 9);
    }

    #[test]
    fn drained_queue_finishes_before_deadline_without_timeout() {
        let queue = queue();
        assert_eq!(queue.try_enqueue("one", 3), TelemetryEnqueueOutcome::Accepted);
        let deadline = Instant::now() + Duration::from_secs(1);
        queue.begin_shutdown(deadline);
        assert_eq!(queue.drain_batch(1, 8), vec!["one"]);

        let report = queue.poll_shutdown(Instant::now()).expect("empty queue should finish");
        assert!(!report.timed_out);
        assert_eq!(report.deadline_dropped_items, 0);
        assert_eq!(report.snapshot.drained_items, 1);
    }

    #[test]
    fn invalid_limits_are_rejected() {
        assert_eq!(
            TelemetryQueueLimits::new(0, 8, 4),
            Err(TelemetryQueueConfigError::ZeroLimit("max_items"))
        );
        assert_eq!(
            TelemetryQueueLimits::new(1, 4, 5),
            Err(TelemetryQueueConfigError::RecordExceedsQueue {
                max_record_bytes: 5,
                max_queue_bytes: 4,
            })
        );
    }
}
