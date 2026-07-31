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
//! The data plane uses [`TelemetryOutageQueue::try_enqueue`], which admits records synchronously
//! without awaiting capacity. Export workers drain batches on their own owned lifecycle. Shutdown
//! closes admission and uses one caller-provided absolute deadline; reaching that deadline drops
//! and reports the remaining telemetry instead of extending the service shutdown budget.

use std::fmt;
#[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
#[cfg(feature = "otlp-logs")]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
#[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::TryLockError;
use std::time::Instant;

use rocketmq_runtime::BudgetDimension;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::BudgetedItem;
use rocketmq_runtime::BudgetedQueue;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::QueuePushErrorKind;
use rocketmq_runtime::ResourceBudgetTree;

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
    /// A legacy exporter adapter could not acquire its internal lock.
    ///
    /// The resource-budget-backed queue does not emit this reason; the variant remains part of the
    /// public reporting contract for older adapters.
    LockUnavailable,
}

impl TelemetryDropReason {
    #[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::ItemLimit => "item_limit",
            Self::ByteLimit => "byte_limit",
            Self::RecordTooLarge => "record_too_large",
            Self::LockUnavailable => "lock_unavailable",
        }
    }
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

struct Queued<T> {
    item: T,
    estimated_bytes: usize,
}

/// A count-and-byte bounded queue for exporter adapters.
///
/// This type does not create a task or thread. The exporter remains owned by the service's
/// existing lifecycle and calls [`Self::drain_batch`] from that owned worker.
pub struct TelemetryOutageQueue<T> {
    limits: TelemetryQueueLimits,
    admission_gate: Mutex<()>,
    queue: BudgetedQueue<Queued<T>>,
    deferred: Mutex<Option<BudgetedItem<Queued<T>>>>,
    shutdown_deadline: Mutex<Option<Instant>>,
    accepted_items: AtomicU64,
    accepted_bytes: AtomicU64,
    drained_items: AtomicU64,
    drained_bytes: AtomicU64,
    dropped_items: AtomicU64,
    dropped_bytes: AtomicU64,
}

impl<T> fmt::Debug for TelemetryOutageQueue<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelemetryOutageQueue")
            .field("limits", &self.limits)
            .field("queue", &self.queue)
            .field("snapshot", &self.snapshot())
            .finish_non_exhaustive()
    }
}

impl<T> TelemetryOutageQueue<T> {
    /// Creates an empty queue with validated limits.
    pub fn new(limits: TelemetryQueueLimits) -> Self {
        // `TelemetryQueueLimits` has private fields and can only be constructed through its
        // validation boundary or `Default`, so the resource budget must accept these values.
        let budget = match ResourceBudgetTree::new(
            "telemetry-export",
            BudgetLimit::new(limits.max_items, limits.max_bytes, FullPolicy::Reject),
        ) {
            Ok(tree) => tree.root(),
            Err(error) => unreachable!("validated telemetry queue limits must form a resource budget: {error}"),
        };
        Self {
            limits,
            admission_gate: Mutex::new(()),
            queue: BudgetedQueue::new(budget),
            deferred: Mutex::new(None),
            shutdown_deadline: Mutex::new(None),
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
        self.try_enqueue_with(item, estimated_bytes, || {})
    }

    /// Admits a record and synchronously forwards it to the owning SDK queue under one ordering
    /// boundary.
    ///
    /// The SDK processor entrypoint used by this adapter is non-blocking. Keeping it inside the
    /// admission gate prevents concurrent callers from reserving resource permits in one order
    /// while reaching the SDK queue in another. Export completion may therefore release FIFO
    /// permits without freeing the byte budget of a different in-flight record.
    fn try_enqueue_with<F>(&self, item: T, estimated_bytes: usize, forward: F) -> TelemetryEnqueueOutcome
    where
        F: FnOnce(),
    {
        let estimated_bytes = estimated_bytes.max(1);
        if estimated_bytes > self.limits.max_record_bytes {
            self.record_drop(estimated_bytes);
            return TelemetryEnqueueOutcome::Dropped(TelemetryDropReason::RecordTooLarge);
        }

        let _admission = match self.admission_gate.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock | TryLockError::Poisoned(_)) => {
                self.record_drop(estimated_bytes);
                return TelemetryEnqueueOutcome::Dropped(TelemetryDropReason::LockUnavailable);
            }
        };
        // `begin_shutdown` closes the queue under this same gate, so a completed shutdown
        // transition must take precedence over resource-budget exhaustion.
        if self.queue.is_closed() {
            self.record_drop(estimated_bytes);
            return TelemetryEnqueueOutcome::Dropped(TelemetryDropReason::Closed);
        }
        let record = Queued { item, estimated_bytes };
        if let Err(error) = self.queue.try_push_data(record, estimated_bytes) {
            let reason = match error.kind() {
                QueuePushErrorKind::Closed | QueuePushErrorKind::SlowConsumerClosed => TelemetryDropReason::Closed,
                QueuePushErrorKind::DeadlineExceeded => TelemetryDropReason::ItemLimit,
                QueuePushErrorKind::BudgetExhausted(error) => match error.dimension() {
                    BudgetDimension::Bytes => TelemetryDropReason::ByteLimit,
                    // This queue has no rate budget. Keep the exhaustive fallback fail-closed if
                    // the shared budget implementation ever reports one.
                    BudgetDimension::Count | BudgetDimension::Rate => TelemetryDropReason::ItemLimit,
                },
            };
            self.record_drop(estimated_bytes);
            return TelemetryEnqueueOutcome::Dropped(reason);
        }

        self.accepted_items.fetch_add(1, Ordering::Relaxed);
        self.accepted_bytes.fetch_add(estimated_bytes as u64, Ordering::Relaxed);
        forward();
        TelemetryEnqueueOutcome::Accepted
    }

    /// Removes a bounded FIFO batch for an exporter worker.
    ///
    /// `max_items` and `max_bytes` are clamped to at least one. One admitted record may exceed
    /// `max_bytes`; it is still returned alone so a worker can always make progress.
    pub fn drain_batch(&self, max_items: usize, max_bytes: usize) -> Vec<T> {
        let _admission = self.lock_admission();
        let mut deferred = self.lock_deferred();
        let mut items = Vec::new();
        let mut drained_bytes = 0usize;
        let max_items = max_items.max(1);
        let max_bytes = max_bytes.max(1);
        while items.len() < max_items {
            let Some(record) = deferred.take().or_else(|| self.queue.try_pop_budgeted()) else {
                break;
            };
            let record_bytes = record.retained_bytes();
            if !items.is_empty() && drained_bytes.saturating_add(record_bytes) > max_bytes {
                *deferred = Some(record);
                break;
            }
            let (record, permit, _) = record.into_parts();
            debug_assert_eq!(record.estimated_bytes, record_bytes);
            drop(permit);
            drained_bytes += record_bytes;
            items.push(record.item);
        }
        self.drained_items.fetch_add(items.len() as u64, Ordering::Relaxed);
        self.drained_bytes.fetch_add(drained_bytes as u64, Ordering::Relaxed);
        items
    }

    /// Stops new admission and binds shutdown to one absolute deadline.
    pub fn begin_shutdown(&self, deadline: Instant) {
        let _admission = self.lock_admission();
        *self.lock_shutdown_deadline() = Some(deadline);
        self.queue.close();
    }

    /// Returns a final report once the queue drains or the absolute deadline expires.
    ///
    /// Before the deadline, `None` means the exporter worker should continue draining. At the
    /// deadline, all remaining records are dropped and included in the report.
    pub fn poll_shutdown(&self, now: Instant) -> Option<TelemetryOutageShutdownReport> {
        let _admission = self.lock_admission();
        let snapshot = self.snapshot_locked();
        if !snapshot.closed {
            return None;
        }
        if snapshot.queued_items == 0 {
            return Some(TelemetryOutageShutdownReport {
                timed_out: false,
                deadline_dropped_items: 0,
                deadline_dropped_bytes: 0,
                snapshot,
            });
        }
        let deadline = (*self.lock_shutdown_deadline())?;
        if now < deadline {
            return None;
        }

        let mut deadline_dropped_items = 0u64;
        let mut deadline_dropped_bytes = 0u64;
        if let Some(record) = self.lock_deferred().take() {
            deadline_dropped_items += 1;
            deadline_dropped_bytes += record.retained_bytes() as u64;
            drop(record);
        }
        while let Some(record) = self.queue.try_pop_budgeted() {
            deadline_dropped_items += 1;
            deadline_dropped_bytes += record.retained_bytes() as u64;
            drop(record);
        }
        self.dropped_items.fetch_add(deadline_dropped_items, Ordering::Relaxed);
        self.dropped_bytes.fetch_add(deadline_dropped_bytes, Ordering::Relaxed);
        Some(TelemetryOutageShutdownReport {
            timed_out: deadline_dropped_items > 0,
            deadline_dropped_items,
            deadline_dropped_bytes,
            snapshot: self.snapshot_locked(),
        })
    }

    /// Returns current queue occupancy and lifetime counters.
    pub fn snapshot(&self) -> TelemetryQueueSnapshot {
        let _admission = self.lock_admission();
        self.snapshot_locked()
    }

    fn snapshot_locked(&self) -> TelemetryQueueSnapshot {
        let queue = self.queue.snapshot();
        TelemetryQueueSnapshot {
            // `reserved_count` and `retained_bytes` include a look-ahead item temporarily removed
            // from the FIFO but still holding its resource permit.
            queued_items: queue.reserved_count,
            queued_bytes: queue.retained_bytes,
            accepted_items: self.accepted_items.load(Ordering::Relaxed),
            accepted_bytes: self.accepted_bytes.load(Ordering::Relaxed),
            drained_items: self.drained_items.load(Ordering::Relaxed),
            drained_bytes: self.drained_bytes.load(Ordering::Relaxed),
            dropped_items: self.dropped_items.load(Ordering::Relaxed),
            dropped_bytes: self.dropped_bytes.load(Ordering::Relaxed),
            closed: queue.closed,
        }
    }

    fn record_drop(&self, estimated_bytes: usize) {
        self.dropped_items.fetch_add(1, Ordering::Relaxed);
        self.dropped_bytes.fetch_add(estimated_bytes as u64, Ordering::Relaxed);
    }

    #[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
    fn drop_remaining(&self) {
        let _admission = self.lock_admission();
        let mut dropped_items = 0u64;
        let mut dropped_bytes = 0u64;
        if let Some(record) = self.lock_deferred().take() {
            dropped_items += 1;
            dropped_bytes += record.retained_bytes() as u64;
            drop(record);
        }
        while let Some(record) = self.queue.try_pop_budgeted() {
            dropped_items += 1;
            dropped_bytes += record.retained_bytes() as u64;
            drop(record);
        }
        self.dropped_items.fetch_add(dropped_items, Ordering::Relaxed);
        self.dropped_bytes.fetch_add(dropped_bytes, Ordering::Relaxed);
    }

    fn lock_deferred(&self) -> std::sync::MutexGuard<'_, Option<BudgetedItem<Queued<T>>>> {
        self.deferred.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_admission(&self) -> std::sync::MutexGuard<'_, ()> {
        self.admission_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_shutdown_deadline(&self) -> std::sync::MutexGuard<'_, Option<Instant>> {
        self.shutdown_deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl<T> Default for TelemetryOutageQueue<T> {
    fn default() -> Self {
        Self::new(TelemetryQueueLimits::default())
    }
}

#[cfg(feature = "otlp-traces")]
fn trace_batch_config(limits: TelemetryQueueLimits) -> opentelemetry_sdk::trace::BatchConfig {
    opentelemetry_sdk::trace::BatchConfigBuilder::default()
        // The resource-budget queue counts both channel-resident and in-flight records. Giving
        // the SDK channel the same count capacity means a successfully admitted record cannot be
        // rejected by the SDK merely because an export batch is currently in flight.
        .with_max_queue_size(limits.max_items())
        .with_max_export_batch_size(DEFAULT_MAX_EXPORT_BATCH_ITEMS.min(limits.max_items()))
        .with_scheduled_delay(std::time::Duration::from_millis(DEFAULT_SCHEDULED_DELAY_MILLIS))
        .build()
}

#[cfg(feature = "otlp-logs")]
fn log_batch_config(limits: TelemetryQueueLimits) -> opentelemetry_sdk::logs::BatchConfig {
    let max_export_batch_size = log_export_batch_size(limits);
    opentelemetry_sdk::logs::BatchConfigBuilder::default()
        .with_max_queue_size(limits.max_items())
        // A completed LogBatch still borrows the SDK worker's storage until the exporter method
        // returns. Keep one full-sized record of count-and-byte headroom so another batch can
        // begin and safely release the previous batch's permits without steady-state starvation.
        .with_max_export_batch_size(max_export_batch_size)
        .with_scheduled_delay(std::time::Duration::from_millis(DEFAULT_SCHEDULED_DELAY_MILLIS))
        .build()
}

#[cfg(feature = "otlp-logs")]
fn log_export_batch_size(limits: TelemetryQueueLimits) -> usize {
    assert!(
        limits.max_items() >= 2 && limits.max_bytes() >= limits.max_record_bytes().saturating_mul(2),
        "log outage admission requires count and byte headroom for one maximum-sized record"
    );
    let count_bound = limits.max_items() - 1;
    let byte_bound = (limits.max_bytes() - limits.max_record_bytes()) / limits.max_record_bytes();
    DEFAULT_MAX_EXPORT_BATCH_ITEMS.min(count_bound).min(byte_bound)
}

#[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
#[derive(Debug)]
struct AdmissionDropReporter {
    signal: &'static str,
    warning_emitted: AtomicBool,
}

#[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
impl AdmissionDropReporter {
    const fn new(signal: &'static str) -> Self {
        Self {
            signal,
            warning_emitted: AtomicBool::new(false),
        }
    }

    fn record(&self, reason: TelemetryDropReason, limits: TelemetryQueueLimits) {
        if !self.warning_emitted.swap(true, Ordering::Relaxed) {
            let _suppressed = opentelemetry::Context::enter_telemetry_suppressed_scope();
            tracing::warn!(
                target: "rocketmq_observability",
                signal = self.signal,
                reason = reason.as_str(),
                max_queue_items = limits.max_items(),
                max_queue_bytes = limits.max_bytes(),
                max_record_bytes = limits.max_record_bytes(),
                "telemetry was rejected by the non-blocking count-and-byte admission boundary; further drops are summarized during shutdown"
            );
        }
    }

    fn report_shutdown(&self, snapshot: TelemetryQueueSnapshot) {
        if snapshot.dropped_items == 0 {
            return;
        }
        let _suppressed = opentelemetry::Context::enter_telemetry_suppressed_scope();
        tracing::warn!(
            target: "rocketmq_observability",
            signal = self.signal,
            accepted_items = snapshot.accepted_items,
            accepted_bytes = snapshot.accepted_bytes,
            drained_items = snapshot.drained_items,
            drained_bytes = snapshot.drained_bytes,
            dropped_items = snapshot.dropped_items,
            dropped_bytes = snapshot.dropped_bytes,
            "telemetry outage admission and shutdown drop totals"
        );
    }
}

#[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
fn shutdown_deadline(timeout: std::time::Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(timeout).unwrap_or(now)
}

#[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
fn release_exported(queue: &TelemetryOutageQueue<()>, count: usize) {
    if count > 0 {
        let _ = queue.drain_batch(count, usize::MAX);
    }
}

#[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
struct ByteEstimator {
    total: usize,
    limit: usize,
}

#[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
impl ByteEstimator {
    fn new(limit: usize) -> Self {
        Self { total: 0, limit }
    }

    fn add(&mut self, bytes: usize) {
        if self.exceeded() {
            return;
        }
        self.total = self.total.saturating_add(bytes);
        if self.total > self.limit {
            self.total = self.limit.saturating_add(1);
        }
    }

    fn add_allocation_str(&mut self, value: &str) {
        // `Key` and `StringValue` hide whether their storage is static, boxed, or ref-counted.
        // Charging the payload plus an Arc header is conservative for every current SDK variant.
        self.add(value.len().saturating_add(2 * std::mem::size_of::<usize>()));
    }

    fn exceeded(&self) -> bool {
        self.total > self.limit
    }

    fn finish(self) -> usize {
        self.total.max(1)
    }
}

#[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
fn add_attribute(estimator: &mut ByteEstimator, attribute: &opentelemetry::KeyValue) {
    estimator.add_allocation_str(attribute.key.as_str());
    add_attribute_value(estimator, &attribute.value);
}

#[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
fn add_attribute_value(estimator: &mut ByteEstimator, value: &opentelemetry::Value) {
    match value {
        opentelemetry::Value::Bool(_) | opentelemetry::Value::I64(_) | opentelemetry::Value::F64(_) => {}
        opentelemetry::Value::String(value) => estimator.add_allocation_str(value.as_str()),
        opentelemetry::Value::Array(values) => match values {
            opentelemetry::Array::Bool(values) => {
                // Vec<bool>::capacity is in bits. Charging one byte per bit is deliberately
                // conservative and avoids depending on its internal word representation.
                estimator.add(values.capacity());
            }
            opentelemetry::Array::I64(values) => {
                estimator.add(values.capacity().saturating_mul(std::mem::size_of::<i64>()));
            }
            opentelemetry::Array::F64(values) => {
                estimator.add(values.capacity().saturating_mul(std::mem::size_of::<f64>()));
            }
            opentelemetry::Array::String(values) => {
                estimator.add(
                    values
                        .capacity()
                        .saturating_mul(std::mem::size_of::<opentelemetry::StringValue>()),
                );
                for value in values {
                    estimator.add_allocation_str(value.as_str());
                    if estimator.exceeded() {
                        break;
                    }
                }
            }
            _ => estimator.total = estimator.limit.saturating_add(1),
        },
        _ => estimator.total = estimator.limit.saturating_add(1),
    }
}

#[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
fn add_instrumentation_scope(estimator: &mut ByteEstimator, scope: &opentelemetry::InstrumentationScope) {
    // The SDK clones a scope into each queued record. Its private Vec clone is compact for the
    // current implementation; charging two slots per observed attribute remains conservative if
    // allocator growth rounds the capacity upward.
    estimator.add_allocation_str(scope.name());
    if let Some(version) = scope.version() {
        estimator.add_allocation_str(version);
    }
    if let Some(schema_url) = scope.schema_url() {
        estimator.add_allocation_str(schema_url);
    }
    let attribute_count = scope.attributes().count();
    estimator.add(
        attribute_count
            .saturating_mul(2)
            .saturating_mul(std::mem::size_of::<opentelemetry::KeyValue>()),
    );
    for attribute in scope.attributes() {
        add_attribute(estimator, attribute);
        if estimator.exceeded() {
            break;
        }
    }
}

#[cfg(feature = "otlp-traces")]
fn add_trace_state(estimator: &mut ByteEstimator, context: &opentelemetry::trace::SpanContext) {
    let entries = context.trace_state().into_iter();
    let entry_count = entries.len();
    let rounded_slots = if entry_count == 0 {
        0
    } else {
        entry_count
            .checked_next_power_of_two()
            .unwrap_or(usize::MAX)
            .saturating_mul(2)
    };
    estimator.add(rounded_slots.saturating_mul(std::mem::size_of::<(String, String)>()));
    for (key, value) in entries {
        estimator.add_allocation_str(key);
        estimator.add_allocation_str(value);
        if estimator.exceeded() {
            break;
        }
    }
}

#[cfg(feature = "otlp-traces")]
fn estimate_span_bytes(span: &opentelemetry_sdk::trace::SpanData, limit: usize) -> usize {
    let mut estimator = ByteEstimator::new(limit);
    estimator.add(std::mem::size_of::<opentelemetry_sdk::trace::SpanData>());
    if let std::borrow::Cow::Owned(name) = &span.name {
        estimator.add(name.capacity());
    }
    add_trace_state(&mut estimator, &span.span_context);

    estimator.add(
        span.attributes
            .capacity()
            .saturating_mul(std::mem::size_of::<opentelemetry::KeyValue>()),
    );
    for attribute in &span.attributes {
        add_attribute(&mut estimator, attribute);
        if estimator.exceeded() {
            return estimator.finish();
        }
    }

    estimator.add(
        span.events
            .events
            .capacity()
            .saturating_mul(std::mem::size_of::<opentelemetry::trace::Event>()),
    );
    for event in &span.events.events {
        if let std::borrow::Cow::Owned(name) = &event.name {
            estimator.add(name.capacity());
        }
        estimator.add(
            event
                .attributes
                .capacity()
                .saturating_mul(std::mem::size_of::<opentelemetry::KeyValue>()),
        );
        for attribute in &event.attributes {
            add_attribute(&mut estimator, attribute);
            if estimator.exceeded() {
                return estimator.finish();
            }
        }
    }

    estimator.add(
        span.links
            .links
            .capacity()
            .saturating_mul(std::mem::size_of::<opentelemetry::trace::Link>()),
    );
    for link in &span.links.links {
        add_trace_state(&mut estimator, &link.span_context);
        estimator.add(
            link.attributes
                .capacity()
                .saturating_mul(std::mem::size_of::<opentelemetry::KeyValue>()),
        );
        for attribute in &link.attributes {
            add_attribute(&mut estimator, attribute);
            if estimator.exceeded() {
                return estimator.finish();
            }
        }
    }

    if let opentelemetry::trace::Status::Error {
        description: std::borrow::Cow::Owned(description),
    } = &span.status
    {
        estimator.add(description.capacity());
    }
    add_instrumentation_scope(&mut estimator, &span.instrumentation_scope);
    estimator.finish()
}

#[cfg(feature = "otlp-logs")]
const LOG_INLINE_ATTRIBUTE_CAPACITY: usize = 5;

#[cfg(feature = "otlp-logs")]
const MAX_LOG_VALUE_DEPTH: usize = 64;

#[cfg(feature = "otlp-logs")]
fn estimate_log_bytes(
    record: &opentelemetry_sdk::logs::SdkLogRecord,
    instrumentation: &opentelemetry::InstrumentationScope,
    limit: usize,
) -> usize {
    let mut estimator = ByteEstimator::new(limit);
    estimator.add(std::mem::size_of::<(
        opentelemetry_sdk::logs::SdkLogRecord,
        opentelemetry::InstrumentationScope,
    )>());
    estimator.add(std::mem::size_of::<
        Box<(
            opentelemetry_sdk::logs::SdkLogRecord,
            opentelemetry::InstrumentationScope,
        )>,
    >());
    if let Some(target) = record.target() {
        // BatchLogProcessor clones the target. Charge its full visible payload even when the
        // source happens to borrow static storage.
        estimator.add_allocation_str(target.as_ref());
    }
    if let Some(body) = record.body() {
        add_log_value(&mut estimator, body, 0);
    }

    let attribute_count = record.attributes_iter().count();
    let overflow_items = attribute_count.saturating_sub(LOG_INLINE_ATTRIBUTE_CAPACITY);
    if overflow_items > 0 {
        let overflow_capacity = overflow_items
            .checked_next_power_of_two()
            .unwrap_or(usize::MAX)
            .max(LOG_INLINE_ATTRIBUTE_CAPACITY);
        estimator.add(overflow_capacity.saturating_mul(std::mem::size_of::<
            Option<(opentelemetry::Key, opentelemetry::logs::AnyValue)>,
        >()));
    }
    for (key, value) in record.attributes_iter() {
        estimator.add_allocation_str(key.as_str());
        add_log_value(&mut estimator, value, 0);
        if estimator.exceeded() {
            return estimator.finish();
        }
    }
    add_instrumentation_scope(&mut estimator, instrumentation);
    estimator.finish()
}

#[cfg(feature = "otlp-logs")]
fn add_log_value(estimator: &mut ByteEstimator, value: &opentelemetry::logs::AnyValue, depth: usize) {
    if depth >= MAX_LOG_VALUE_DEPTH {
        estimator.total = estimator.limit.saturating_add(1);
        return;
    }
    match value {
        opentelemetry::logs::AnyValue::Int(_)
        | opentelemetry::logs::AnyValue::Double(_)
        | opentelemetry::logs::AnyValue::Boolean(_) => {}
        opentelemetry::logs::AnyValue::String(value) => {
            estimator.add_allocation_str(value.as_str());
        }
        opentelemetry::logs::AnyValue::Bytes(values) => estimator.add(values.capacity()),
        opentelemetry::logs::AnyValue::ListAny(values) => {
            estimator.add(
                values
                    .capacity()
                    .saturating_mul(std::mem::size_of::<opentelemetry::logs::AnyValue>()),
            );
            for value in values.iter() {
                add_log_value(estimator, value, depth + 1);
                if estimator.exceeded() {
                    break;
                }
            }
        }
        opentelemetry::logs::AnyValue::Map(values) => {
            // HashMap::capacity is its element admission capacity, not its raw bucket count.
            // Doubling the visible capacity covers control bytes and the current 7/8 load factor.
            estimator.add(values.capacity().saturating_mul(2).saturating_mul(
                std::mem::size_of::<(opentelemetry::Key, opentelemetry::logs::AnyValue)>().saturating_add(1),
            ));
            for (key, value) in values.iter() {
                estimator.add_allocation_str(key.as_str());
                add_log_value(estimator, value, depth + 1);
                if estimator.exceeded() {
                    break;
                }
            }
        }
        _ => estimator.total = estimator.limit.saturating_add(1),
    }
}

#[cfg(feature = "otlp-traces")]
#[derive(Debug)]
struct QueueReleasingSpanExporter<E> {
    exporter: E,
    queue: Arc<TelemetryOutageQueue<()>>,
}

#[cfg(feature = "otlp-traces")]
impl<E> opentelemetry_sdk::trace::SpanExporter for QueueReleasingSpanExporter<E>
where
    E: opentelemetry_sdk::trace::SpanExporter,
{
    async fn export(&self, batch: Vec<opentelemetry_sdk::trace::SpanData>) -> opentelemetry_sdk::error::OTelSdkResult {
        let batch_items = batch.len();
        let result = self.exporter.export(batch).await;
        // The span batch is owned by the exporter future. Once that future completes, the batch
        // has been consumed and its resource permits can be released without an overlap window.
        release_exported(&self.queue, batch_items);
        result
    }

    fn shutdown_with_timeout(&self, timeout: std::time::Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        self.exporter.shutdown_with_timeout(timeout)
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.exporter.force_flush()
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.exporter.set_resource(resource);
    }
}

/// OTLP trace processor whose outage admission is bounded by both record count and estimated bytes.
///
/// The SDK remains the worker and lifecycle owner. This wrapper does not create a task or thread:
/// it reserves one [`TelemetryOutageQueue`] permit before handing a span to the SDK and the wrapped
/// exporter releases that permit only after the corresponding owned export batch has completed.
#[cfg(feature = "otlp-traces")]
#[derive(Debug)]
pub(crate) struct OutageBoundedBatchSpanProcessor {
    inner: opentelemetry_sdk::trace::BatchSpanProcessor,
    queue: Arc<TelemetryOutageQueue<()>>,
    limits: TelemetryQueueLimits,
    drop_reporter: AdmissionDropReporter,
}

#[cfg(feature = "otlp-traces")]
impl OutageBoundedBatchSpanProcessor {
    pub(crate) fn new<E>(exporter: E) -> Self
    where
        E: opentelemetry_sdk::trace::SpanExporter + Send + 'static,
    {
        Self::new_with_limits(exporter, TelemetryQueueLimits::default())
    }

    fn new_with_limits<E>(exporter: E, limits: TelemetryQueueLimits) -> Self
    where
        E: opentelemetry_sdk::trace::SpanExporter + Send + 'static,
    {
        let queue = Arc::new(TelemetryOutageQueue::new(limits));
        let exporter = QueueReleasingSpanExporter {
            exporter,
            queue: Arc::clone(&queue),
        };
        let inner = opentelemetry_sdk::trace::BatchSpanProcessor::builder(exporter)
            .with_batch_config(trace_batch_config(limits))
            .build();
        Self {
            inner,
            queue,
            limits,
            drop_reporter: AdmissionDropReporter::new("traces"),
        }
    }

    #[cfg(test)]
    fn admission_queue(&self) -> Arc<TelemetryOutageQueue<()>> {
        Arc::clone(&self.queue)
    }
}

#[cfg(feature = "otlp-traces")]
impl opentelemetry_sdk::trace::SpanProcessor for OutageBoundedBatchSpanProcessor {
    fn on_start(&self, span: &mut opentelemetry_sdk::trace::Span, context: &opentelemetry::Context) {
        self.inner.on_start(span, context);
    }

    fn on_end(&self, span: opentelemetry_sdk::trace::SpanData) {
        if !span.span_context.is_sampled() {
            return;
        }
        let estimated_bytes = estimate_span_bytes(&span, self.limits.max_record_bytes());
        match self
            .queue
            .try_enqueue_with((), estimated_bytes, || self.inner.on_end(span))
        {
            TelemetryEnqueueOutcome::Accepted => {}
            TelemetryEnqueueOutcome::Dropped(reason) => self.drop_reporter.record(reason, self.limits),
        }
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: std::time::Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        let deadline = shutdown_deadline(timeout);
        self.queue.begin_shutdown(deadline);
        let result = self
            .inner
            .shutdown_with_timeout(deadline.saturating_duration_since(Instant::now()));
        // A successful SDK shutdown may still discard records beyond its final batch. A timed-out
        // SDK worker can also retain records briefly, but admission is already closed. Releasing
        // the permits here cannot reopen the queue and keeps shutdown non-blocking.
        self.queue.drop_remaining();
        self.drop_reporter.report_shutdown(self.queue.snapshot());
        result
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.inner.set_resource(resource);
    }
}

#[cfg(feature = "otlp-logs")]
#[derive(Debug)]
struct QueueReleasingLogExporter<E> {
    exporter: E,
    queue: Arc<TelemetryOutageQueue<()>>,
    completed_batch_items: Arc<AtomicUsize>,
}

#[cfg(feature = "otlp-logs")]
impl<E> QueueReleasingLogExporter<E> {
    fn release_completed(&self) {
        release_exported(&self.queue, self.completed_batch_items.swap(0, Ordering::AcqRel));
    }
}

#[cfg(feature = "otlp-logs")]
impl<E> opentelemetry_sdk::logs::LogExporter for QueueReleasingLogExporter<E>
where
    E: opentelemetry_sdk::logs::LogExporter,
{
    async fn export(&self, batch: opentelemetry_sdk::logs::LogBatch<'_>) -> opentelemetry_sdk::error::OTelSdkResult {
        // A LogBatch borrows the SDK worker's storage. Permits from the previous export are
        // released now, after the SDK has cleared that storage, instead of immediately after its
        // exporter future resolves while the borrowed batch is still alive.
        self.release_completed();
        let batch_items = batch.iter().count();
        let result = self.exporter.export(batch).await;
        self.completed_batch_items.fetch_add(batch_items, Ordering::Release);
        result
    }

    fn shutdown_with_timeout(&self, timeout: std::time::Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        self.release_completed();
        self.exporter.shutdown_with_timeout(timeout)
    }

    fn event_enabled(&self, level: opentelemetry::logs::Severity, target: &str, name: Option<&str>) -> bool {
        self.exporter.event_enabled(level, target, name)
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.exporter.set_resource(resource);
    }
}

/// OTLP log processor whose queued and in-flight payload shares one count-and-byte budget.
#[cfg(feature = "otlp-logs")]
#[derive(Debug)]
pub(crate) struct OutageBoundedBatchLogProcessor {
    inner: opentelemetry_sdk::logs::BatchLogProcessor,
    queue: Arc<TelemetryOutageQueue<()>>,
    completed_batch_items: Arc<AtomicUsize>,
    limits: TelemetryQueueLimits,
    drop_reporter: AdmissionDropReporter,
}

#[cfg(feature = "otlp-logs")]
impl OutageBoundedBatchLogProcessor {
    pub(crate) fn new<E>(exporter: E) -> Self
    where
        E: opentelemetry_sdk::logs::LogExporter + Send + Sync + 'static,
    {
        Self::new_with_limits(exporter, TelemetryQueueLimits::default())
    }

    fn new_with_limits<E>(exporter: E, limits: TelemetryQueueLimits) -> Self
    where
        E: opentelemetry_sdk::logs::LogExporter + Send + Sync + 'static,
    {
        let queue = Arc::new(TelemetryOutageQueue::new(limits));
        let completed_batch_items = Arc::new(AtomicUsize::new(0));
        let exporter = QueueReleasingLogExporter {
            exporter,
            queue: Arc::clone(&queue),
            completed_batch_items: Arc::clone(&completed_batch_items),
        };
        let inner = opentelemetry_sdk::logs::BatchLogProcessor::builder(exporter)
            .with_batch_config(log_batch_config(limits))
            .build();
        Self {
            inner,
            queue,
            completed_batch_items,
            limits,
            drop_reporter: AdmissionDropReporter::new("logs"),
        }
    }

    fn release_completed(&self) {
        release_exported(&self.queue, self.completed_batch_items.swap(0, Ordering::AcqRel));
    }

    #[cfg(test)]
    fn admission_queue(&self) -> Arc<TelemetryOutageQueue<()>> {
        Arc::clone(&self.queue)
    }
}

#[cfg(feature = "otlp-logs")]
impl opentelemetry_sdk::logs::LogProcessor for OutageBoundedBatchLogProcessor {
    fn emit(
        &self,
        record: &mut opentelemetry_sdk::logs::SdkLogRecord,
        instrumentation: &opentelemetry::InstrumentationScope,
    ) {
        let estimated_bytes = estimate_log_bytes(record, instrumentation, self.limits.max_record_bytes());
        match self
            .queue
            .try_enqueue_with((), estimated_bytes, || self.inner.emit(record, instrumentation))
        {
            TelemetryEnqueueOutcome::Accepted => {}
            TelemetryEnqueueOutcome::Dropped(reason) => self.drop_reporter.record(reason, self.limits),
        }
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        let result = self.inner.force_flush();
        // Once force_flush returns, the SDK has cleared its borrowed batch storage.
        self.release_completed();
        result
    }

    fn shutdown_with_timeout(&self, timeout: std::time::Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        let deadline = shutdown_deadline(timeout);
        self.queue.begin_shutdown(deadline);
        let result = self
            .inner
            .shutdown_with_timeout(deadline.saturating_duration_since(Instant::now()));
        self.release_completed();
        self.queue.drop_remaining();
        self.drop_reporter.report_shutdown(self.queue.snapshot());
        result
    }

    fn event_enabled(&self, level: opentelemetry::logs::Severity, target: &str, name: Option<&str>) -> bool {
        self.inner.event_enabled(level, target, name)
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.inner.set_resource(resource);
    }
}

#[cfg(test)]
mod tests {
    #[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
    use std::sync::Arc;
    #[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
    use std::sync::Condvar;
    #[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;

    #[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
    #[derive(Debug, Default)]
    struct ExportGate {
        state: Mutex<ExportGateState>,
        changed: Condvar,
    }

    #[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
    #[derive(Debug, Default)]
    struct ExportGateState {
        started_count: usize,
        released: bool,
    }

    #[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
    impl ExportGate {
        fn block_export(&self) {
            let mut state = self.state.lock().expect("export gate should remain available");
            state.started_count += 1;
            self.changed.notify_all();
            while !state.released {
                state = self.changed.wait(state).expect("export gate should remain available");
            }
        }

        fn wait_until_started(&self, timeout: Duration) -> bool {
            self.wait_until_started_count(1, timeout)
        }

        fn wait_until_started_count(&self, expected: usize, timeout: Duration) -> bool {
            let state = self.state.lock().expect("export gate should remain available");
            if state.started_count >= expected {
                return true;
            }
            self.changed
                .wait_timeout_while(state, timeout, |state| state.started_count < expected)
                .expect("export gate should remain available")
                .0
                .started_count
                >= expected
        }

        fn release(&self) {
            let mut state = self.state.lock().expect("export gate should remain available");
            state.released = true;
            self.changed.notify_all();
        }
    }

    #[cfg(feature = "otlp-traces")]
    #[derive(Debug, Clone)]
    struct BlockingSpanExporter {
        gate: Arc<ExportGate>,
    }

    #[cfg(feature = "otlp-traces")]
    impl opentelemetry_sdk::trace::SpanExporter for BlockingSpanExporter {
        async fn export(
            &self,
            _batch: Vec<opentelemetry_sdk::trace::SpanData>,
        ) -> opentelemetry_sdk::error::OTelSdkResult {
            self.gate.block_export();
            Ok(())
        }
    }

    #[cfg(feature = "otlp-logs")]
    #[derive(Debug, Clone)]
    struct BlockingLogExporter {
        gate: Arc<ExportGate>,
    }

    #[cfg(feature = "otlp-logs")]
    impl opentelemetry_sdk::logs::LogExporter for BlockingLogExporter {
        async fn export(
            &self,
            _batch: opentelemetry_sdk::logs::LogBatch<'_>,
        ) -> opentelemetry_sdk::error::OTelSdkResult {
            self.gate.block_export();
            Ok(())
        }
    }

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
    fn data_plane_drops_instead_of_waiting_for_queue_state() {
        let queue = queue();
        let _state_operation = queue
            .admission_gate
            .lock()
            .expect("test should hold the queue admission gate");

        assert_eq!(
            queue.try_enqueue("contended", 2),
            TelemetryEnqueueOutcome::Dropped(TelemetryDropReason::LockUnavailable)
        );
        assert_eq!(queue.dropped_items.load(Ordering::Relaxed), 1);
        assert_eq!(queue.dropped_bytes.load(Ordering::Relaxed), 2);
    }

    #[cfg(any(feature = "otlp-traces", feature = "otlp-logs"))]
    #[test]
    fn sdk_forwarding_cannot_reorder_resource_permits() {
        let queue = Arc::new(queue());
        let (forward_entered_tx, forward_entered_rx) = std::sync::mpsc::channel();
        let (release_forward_tx, release_forward_rx) = std::sync::mpsc::channel();
        let first_queue = Arc::clone(&queue);
        let first = std::thread::spawn(move || {
            first_queue.try_enqueue_with("first", 6, || {
                forward_entered_tx
                    .send(())
                    .expect("test should observe the first SDK forwarding boundary");
                release_forward_rx
                    .recv()
                    .expect("test should release the first SDK forwarding boundary");
            })
        });
        forward_entered_rx
            .recv()
            .expect("first admission should reach its SDK forwarding boundary");

        let mut second_forwarded = false;
        assert_eq!(
            queue.try_enqueue_with("second", 1, || second_forwarded = true),
            TelemetryEnqueueOutcome::Dropped(TelemetryDropReason::LockUnavailable)
        );
        assert!(!second_forwarded);

        release_forward_tx
            .send(())
            .expect("test should release the first forwarding boundary");
        assert_eq!(
            first.join().expect("first admission thread should finish"),
            TelemetryEnqueueOutcome::Accepted
        );
        assert_eq!(queue.drain_batch(2, usize::MAX), vec!["first"]);
        assert_eq!(queue.snapshot().queued_bytes, 0);
    }

    #[test]
    fn deferred_lookahead_retains_its_shared_resource_budget() {
        let queue = queue();
        assert_eq!(queue.try_enqueue("one", 3), TelemetryEnqueueOutcome::Accepted);
        assert_eq!(queue.try_enqueue("two", 5), TelemetryEnqueueOutcome::Accepted);

        assert_eq!(queue.drain_batch(2, 4), vec!["one"]);
        let snapshot = queue.snapshot();
        assert_eq!(snapshot.queued_items, 1);
        assert_eq!(snapshot.queued_bytes, 5);
        assert_eq!(
            queue.try_enqueue("over-budget", 4),
            TelemetryEnqueueOutcome::Dropped(TelemetryDropReason::ByteLimit)
        );
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

    #[cfg(feature = "otlp-traces")]
    #[test]
    fn sdk_trace_batch_config_matches_resource_budget_count_capacity() {
        let limits = TelemetryQueueLimits::new(7, 8_192, 4_096).expect("test limits should be valid");
        let rendered = format!("{:?}", trace_batch_config(limits));

        assert!(rendered.contains("max_queue_size: 7"));
        assert!(rendered.contains("max_export_batch_size: 7"));
    }

    #[cfg(feature = "otlp-logs")]
    #[test]
    fn sdk_log_batch_config_matches_resource_budget_count_capacity() {
        let limits = TelemetryQueueLimits::new(7, 8_192, 1_024).expect("test limits should be valid");
        let rendered = format!("{:?}", log_batch_config(limits));

        assert!(rendered.contains("max_queue_size: 7"));
        assert!(rendered.contains("max_export_batch_size: 6"));
    }

    #[cfg(feature = "otlp-traces")]
    #[test]
    fn trace_budget_covers_sdk_queue_and_in_flight_export_batch() {
        use opentelemetry_sdk::trace::SpanProcessor as _;

        let gate = Arc::new(ExportGate::default());
        let limits = TelemetryQueueLimits::new(2, 8_192, 4_096).expect("test limits should be valid");
        let processor = OutageBoundedBatchSpanProcessor::new_with_limits(
            BlockingSpanExporter {
                gate: Arc::clone(&gate),
            },
            limits,
        );
        let queue = processor.admission_queue();

        processor.on_end(sampled_span("one"));
        processor.on_end(sampled_span("two"));
        let export_started = gate.wait_until_started(Duration::from_secs(2));
        let in_flight = queue.snapshot();
        processor.on_end(sampled_span("rejected"));
        let after_rejection = queue.snapshot();

        gate.release();
        let shutdown_result = processor.shutdown_with_timeout(Duration::from_secs(2));
        let final_snapshot = queue.snapshot();

        assert!(export_started, "the SDK should begin exporting the full batch");
        assert_eq!(in_flight.queued_items, 2);
        assert!(in_flight.queued_bytes > 0);
        assert!(in_flight.queued_bytes <= limits.max_bytes());
        assert_eq!(after_rejection.queued_items, 2);
        assert_eq!(after_rejection.dropped_items, 1);
        assert!(shutdown_result.is_ok());
        assert_eq!(final_snapshot.queued_items, 0);
        assert_eq!(final_snapshot.drained_items, 2);
    }

    #[cfg(feature = "otlp-traces")]
    #[test]
    fn oversized_span_is_rejected_before_the_sdk_queue() {
        use opentelemetry_sdk::trace::SpanProcessor as _;

        let gate = Arc::new(ExportGate::default());
        let limits = TelemetryQueueLimits::new(2, 1_024, 512).expect("test limits should be valid");
        let processor = OutageBoundedBatchSpanProcessor::new_with_limits(
            BlockingSpanExporter {
                gate: Arc::clone(&gate),
            },
            limits,
        );
        let queue = processor.admission_queue();

        processor.on_end(sampled_span(&"x".repeat(2_048)));
        let rejected = queue.snapshot();
        gate.release();
        let shutdown_result = processor.shutdown_with_timeout(Duration::from_secs(2));

        assert_eq!(rejected.accepted_items, 0);
        assert_eq!(rejected.dropped_items, 1);
        assert_eq!(rejected.queued_items, 0);
        assert!(shutdown_result.is_ok());
    }

    #[cfg(feature = "otlp-logs")]
    #[test]
    fn log_budget_retains_borrowed_batches_without_starving_follow_up_exports() {
        use opentelemetry::logs::LoggerProvider as _;

        let gate = Arc::new(ExportGate::default());
        let limits = TelemetryQueueLimits::new(2, 8_192, 4_096).expect("test limits should be valid");
        let processor = OutageBoundedBatchLogProcessor::new_with_limits(
            BlockingLogExporter {
                gate: Arc::clone(&gate),
            },
            limits,
        );
        let queue = processor.admission_queue();
        let provider = opentelemetry_sdk::logs::SdkLoggerProvider::builder()
            .with_log_processor(processor)
            .build();
        let logger = provider.logger("outage-test");

        emit_log(&logger, "one");
        emit_log(&logger, "two");
        let export_started = gate.wait_until_started(Duration::from_secs(2));
        let in_flight = queue.snapshot();
        emit_log(&logger, "rejected");
        let after_rejection = queue.snapshot();

        gate.release();
        let follow_up_export_started = gate.wait_until_started_count(2, Duration::from_secs(2));
        emit_log(&logger, "continued");
        let after_continuation = queue.snapshot();
        let shutdown_result = provider.shutdown_with_timeout(Duration::from_secs(2));
        let final_snapshot = queue.snapshot();

        assert!(export_started, "the SDK should begin exporting the full batch");
        assert_eq!(in_flight.queued_items, 2);
        assert!(in_flight.queued_bytes > 0);
        assert!(in_flight.queued_bytes <= limits.max_bytes());
        assert_eq!(after_rejection.queued_items, 2);
        assert_eq!(after_rejection.dropped_items, 1);
        assert!(
            follow_up_export_started,
            "reserved headroom should let a second batch start without an explicit flush"
        );
        assert_eq!(after_continuation.accepted_items, 3);
        assert_eq!(after_continuation.dropped_items, 1);
        assert!(after_continuation.queued_items <= limits.max_items());
        assert!(shutdown_result.is_ok());
        assert_eq!(final_snapshot.queued_items, 0);
        assert_eq!(final_snapshot.drained_items, 3);
    }

    #[cfg(feature = "otlp-traces")]
    fn sampled_span(name: &str) -> opentelemetry_sdk::trace::SpanData {
        opentelemetry_sdk::trace::SpanData {
            span_context: opentelemetry::trace::SpanContext::new(
                opentelemetry::TraceId::from(1),
                opentelemetry::SpanId::from(1),
                opentelemetry::TraceFlags::SAMPLED,
                false,
                opentelemetry::trace::TraceState::default(),
            ),
            parent_span_id: opentelemetry::SpanId::INVALID,
            parent_span_is_remote: false,
            span_kind: opentelemetry::trace::SpanKind::Internal,
            name: name.to_owned().into(),
            start_time: std::time::SystemTime::now(),
            end_time: std::time::SystemTime::now(),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            events: opentelemetry_sdk::trace::SpanEvents::default(),
            links: opentelemetry_sdk::trace::SpanLinks::default(),
            status: opentelemetry::trace::Status::Unset,
            instrumentation_scope: opentelemetry::InstrumentationScope::builder("outage-test").build(),
        }
    }

    #[cfg(feature = "otlp-logs")]
    fn emit_log<L>(logger: &L, body: &str)
    where
        L: opentelemetry::logs::Logger,
    {
        use opentelemetry::logs::LogRecord as _;

        let mut record = logger.create_log_record();
        record.set_body(body.to_owned().into());
        logger.emit(record);
    }
}
