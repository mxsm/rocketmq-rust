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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;

/// Canonical progress and commit-serialization state for a mapped-file queue.
#[doc(hidden)]
#[derive(Debug)]
pub struct MappedFileQueueRuntimeState {
    flushed_where: Arc<AtomicU64>,
    committed_where: Arc<AtomicU64>,
    store_timestamp: Arc<AtomicU64>,
    commit_lock: Arc<Mutex<()>>,
}

impl Default for MappedFileQueueRuntimeState {
    fn default() -> Self {
        Self {
            flushed_where: Arc::new(AtomicU64::new(0)),
            committed_where: Arc::new(AtomicU64::new(0)),
            store_timestamp: Arc::new(AtomicU64::new(0)),
            commit_lock: Arc::new(Mutex::new(())),
        }
    }
}

impl MappedFileQueueRuntimeState {
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
}
