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

//! Runtime-neutral progress state for a Local mapped-file queue.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;

/// Canonical progress and commit-serialization state for a mapped-file queue.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct MappedFileQueueRuntimeState {
    flushed_where: Arc<AtomicU64>,
    committed_where: Arc<AtomicU64>,
    store_timestamp: Arc<AtomicU64>,
    closing: Arc<AtomicBool>,
    commit_lock: Arc<Mutex<()>>,
    /// Start offset plus one of the next segment whose background preallocation was requested;
    /// zero before the first request.
    preallocation_requested: Arc<AtomicU64>,
}

impl Default for MappedFileQueueRuntimeState {
    fn default() -> Self {
        Self {
            flushed_where: Arc::new(AtomicU64::new(0)),
            committed_where: Arc::new(AtomicU64::new(0)),
            store_timestamp: Arc::new(AtomicU64::new(0)),
            closing: Arc::new(AtomicBool::new(false)),
            commit_lock: Arc::new(Mutex::new(())),
            preallocation_requested: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl MappedFileQueueRuntimeState {
    /// Returns whether retirement has permanently closed this queue to new appends.
    #[doc(hidden)]
    pub fn is_closing(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }

    /// Permanently closes this queue to new appends.
    ///
    /// Callers must hold [`Self::commit_lock`] while publishing this transition so creation and
    /// retirement share one serialization boundary.
    #[doc(hidden)]
    pub fn begin_close(&self) -> bool {
        self.closing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Returns the last committed physical offset.
    #[doc(hidden)]
    pub fn committed_where(&self) -> i64 {
        self.committed_where.load(Ordering::Acquire) as i64
    }

    /// Replaces the last committed physical offset.
    #[doc(hidden)]
    pub fn set_committed_where(&self, committed_where: i64) {
        self.committed_where.store(committed_where as u64, Ordering::SeqCst);
    }

    /// Returns the last durable physical offset.
    #[doc(hidden)]
    pub fn flushed_where(&self) -> i64 {
        self.flushed_where.load(Ordering::Acquire) as i64
    }

    /// Replaces the last durable physical offset.
    #[doc(hidden)]
    pub fn set_flushed_where(&self, flushed_where: i64) {
        self.flushed_where.store(flushed_where as u64, Ordering::SeqCst);
    }

    /// Returns the timestamp associated with the last full flush.
    #[doc(hidden)]
    pub fn store_timestamp(&self) -> u64 {
        self.store_timestamp.load(Ordering::Acquire)
    }

    /// Replaces the timestamp associated with the last full flush.
    #[doc(hidden)]
    pub fn set_store_timestamp(&self, store_timestamp: u64) {
        self.store_timestamp.store(store_timestamp, Ordering::Release);
    }

    /// Returns the queue-wide commit serialization lock.
    #[doc(hidden)]
    pub fn commit_lock(&self) -> &Mutex<()> {
        self.commit_lock.as_ref()
    }

    /// Returns whether background preallocation of the segment starting at `next_offset` was
    /// already requested.
    ///
    /// Every append past the preallocation threshold asks for the next segment, so the request
    /// itself, which takes the commit lock and checks the free space of every root, is made once.
    #[doc(hidden)]
    pub fn preallocation_requested_for(&self, next_offset: u64) -> bool {
        self.preallocation_requested.load(Ordering::Acquire) == next_offset.wrapping_add(1)
    }

    /// Records that background preallocation of the segment starting at `next_offset` needs no
    /// further request.
    #[doc(hidden)]
    pub fn record_preallocation_requested(&self, next_offset: u64) {
        self.preallocation_requested
            .store(next_offset.wrapping_add(1), Ordering::Release);
    }
}
