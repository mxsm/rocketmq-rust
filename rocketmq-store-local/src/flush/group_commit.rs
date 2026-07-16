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

//! Runtime-neutral group-commit request and batch-completion ownership.

use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use parking_lot::Mutex;
use tokio::sync::oneshot;

/// Neutral completion state for a Local group-commit request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupCommitStatus {
    /// The durable watermark reached the requested offset.
    Flushed,
    /// The request expired before its offset became durable.
    TimedOut,
}

/// Result delivered to a group-commit waiter.
pub type GroupCommitResult<E> = Result<GroupCommitStatus, Arc<E>>;

/// Receiver returned to the Store compatibility adapter.
pub type GroupCommitReceiver<E> = oneshot::Receiver<GroupCommitResult<E>>;

/// Canonical Local group-commit request.
#[derive(Debug)]
pub struct GroupCommitRequest<E> {
    next_offset: i64,
    result_sender: Option<oneshot::Sender<GroupCommitResult<E>>>,
    deadline_nanos: u64,
    enqueue_time_millis: u64,
}

impl<E> GroupCommitRequest<E> {
    /// Creates a request and its single-consumer response channel.
    pub fn new(next_offset: i64, timeout_millis: u64) -> (Self, GroupCommitReceiver<E>) {
        let deadline_nanos = current_nanos().saturating_add(timeout_millis.saturating_mul(1_000_000));
        let (result_sender, result_receiver) = oneshot::channel();
        (
            Self {
                next_offset,
                result_sender: Some(result_sender),
                deadline_nanos,
                enqueue_time_millis: current_millis(),
            },
            result_receiver,
        )
    }

    /// Returns the exclusive offset that must be durable for success.
    pub const fn next_offset(&self) -> i64 {
        self.next_offset
    }

    /// Returns the wall-clock enqueue timestamp used by runtime metrics.
    pub const fn enqueue_time_millis(&self) -> u64 {
        self.enqueue_time_millis
    }

    /// Returns whether the configured deadline has elapsed.
    pub fn is_expired(&self) -> bool {
        current_nanos() >= self.deadline_nanos
    }

    /// Completes the request with a neutral flush status.
    pub fn complete(mut self, status: GroupCommitStatus) {
        if let Some(sender) = self.result_sender.take() {
            let _ = sender.send(Ok(status));
        }
    }

    /// Completes the request with the adapter's typed flush error.
    pub fn complete_error(mut self, error: Arc<E>) {
        if let Some(sender) = self.result_sender.take() {
            let _ = sender.send(Err(error));
        }
    }
}

/// Public snapshot of SyncFlush queue and wait-time counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SyncFlushRuntimeInfo {
    pub queue_depth: u64,
    pub enqueue_total: u64,
    pub completed_total: u64,
    pub timeout_total: u64,
    pub oldest_wait_millis: u64,
    pub max_wait_millis: u64,
    pub wait_total_millis: u64,
}

/// Canonical counter owner shared by the group-commit request and worker.
#[doc(hidden)]
#[derive(Clone, Default)]
pub struct SyncFlushStats {
    inner: Arc<SyncFlushStatsInner>,
}

#[derive(Default)]
struct SyncFlushStatsInner {
    queue_depth: AtomicU64,
    enqueue_total: AtomicU64,
    completed_total: AtomicU64,
    timeout_total: AtomicU64,
    max_wait_millis: AtomicU64,
    wait_total_millis: AtomicU64,
    pending_enqueue_times: Mutex<VecDeque<u64>>,
}

impl SyncFlushStats {
    /// Records a successfully enqueued request.
    #[doc(hidden)]
    pub fn record_enqueue(&self, enqueue_time_millis: u64) {
        self.inner.pending_enqueue_times.lock().push_back(enqueue_time_millis);
        self.inner.queue_depth.fetch_add(1, Ordering::Relaxed);
        self.inner.enqueue_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the current diagnostic snapshot.
    #[doc(hidden)]
    pub fn snapshot(&self) -> SyncFlushRuntimeInfo {
        let oldest_wait_millis = self
            .inner
            .pending_enqueue_times
            .lock()
            .front()
            .map(|enqueue_time| current_millis().saturating_sub(*enqueue_time))
            .unwrap_or_default();

        SyncFlushRuntimeInfo {
            queue_depth: self.inner.queue_depth.load(Ordering::Relaxed),
            enqueue_total: self.inner.enqueue_total.load(Ordering::Relaxed),
            completed_total: self.inner.completed_total.load(Ordering::Relaxed),
            timeout_total: self.inner.timeout_total.load(Ordering::Relaxed),
            oldest_wait_millis,
            max_wait_millis: self.inner.max_wait_millis.load(Ordering::Relaxed),
            wait_total_millis: self.inner.wait_total_millis.load(Ordering::Relaxed),
        }
    }

    fn record_completion<E>(&self, request: &GroupCommitRequest<E>, status: GroupCommitStatus) {
        let _ = self
            .inner
            .queue_depth
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |depth| {
                Some(depth.saturating_sub(1))
            });
        self.inner.pending_enqueue_times.lock().pop_front();

        let wait_millis = current_millis().saturating_sub(request.enqueue_time_millis());
        self.inner.completed_total.fetch_add(1, Ordering::Relaxed);
        self.inner.wait_total_millis.fetch_add(wait_millis, Ordering::Relaxed);
        if status == GroupCommitStatus::TimedOut {
            self.inner.timeout_total.fetch_add(1, Ordering::Relaxed);
        }
        update_atomic_max(&self.inner.max_wait_millis, wait_millis);
    }
}

/// Completes each request from the final durable watermark of one batch.
#[doc(hidden)]
pub fn complete_group_commit_batch<E>(
    requests: Vec<GroupCommitRequest<E>>,
    flushed_where: i64,
    sync_flush_stats: &SyncFlushStats,
) {
    for request in requests {
        let status = if flushed_where >= request.next_offset() {
            GroupCommitStatus::Flushed
        } else {
            GroupCommitStatus::TimedOut
        };
        sync_flush_stats.record_completion(&request, status);
        request.complete(status);
    }
}

/// Completes every waiter in one batch with the same typed I/O failure.
#[doc(hidden)]
pub fn complete_group_commit_batch_error<E>(
    requests: Vec<GroupCommitRequest<E>>,
    error: Arc<E>,
    sync_flush_stats: &SyncFlushStats,
) {
    for request in requests {
        sync_flush_stats.record_completion(&request, GroupCommitStatus::TimedOut);
        request.complete_error(error.clone());
    }
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn current_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn update_atomic_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}
