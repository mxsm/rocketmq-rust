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

use std::sync::atomic::fence;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use parking_lot::Mutex;

#[inline(always)]
fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Atomic progress state for one local mapped-file segment.
#[repr(align(64))]
pub struct MappedFileProgress {
    wrote_position: AtomicI32,
    committed_position: AtomicI32,
    flushed_position: AtomicI32,
    file_size: u64,
    store_timestamp: AtomicU64,
    last_flush_time: AtomicU64,
    start_timestamp: AtomicI64,
    stop_timestamp: AtomicI64,
}

impl MappedFileProgress {
    #[inline]
    pub fn new(file_size: u64) -> Self {
        Self {
            wrote_position: AtomicI32::new(0),
            committed_position: AtomicI32::new(0),
            flushed_position: AtomicI32::new(0),
            file_size,
            store_timestamp: AtomicU64::new(0),
            last_flush_time: AtomicU64::new(0),
            start_timestamp: AtomicI64::new(-1),
            stop_timestamp: AtomicI64::new(-1),
        }
    }

    #[inline]
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.file_size == self.wrote_position() as u64
    }

    #[inline]
    pub fn wrote_position(&self) -> i32 {
        self.wrote_position.load(Ordering::Acquire)
    }

    #[inline]
    pub fn wrote_position_relaxed(&self) -> i32 {
        self.wrote_position.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn set_wrote_position(&self, position: i32) {
        self.wrote_position.store(position, Ordering::SeqCst);
    }

    #[inline]
    pub fn advance_wrote_position(&self, delta: i32) {
        self.wrote_position.fetch_add(delta, Ordering::AcqRel);
    }

    #[inline]
    pub fn committed_position(&self) -> i32 {
        self.committed_position.load(Ordering::Acquire)
    }

    #[inline]
    pub fn set_committed_position(&self, position: i32) {
        self.committed_position.store(position, Ordering::SeqCst);
    }

    #[inline]
    pub fn set_committed_position_release(&self, position: i32) {
        self.committed_position.store(position, Ordering::Release);
    }

    #[inline]
    pub fn commit_wrote_position(&self) {
        self.committed_position.store(self.wrote_position(), Ordering::Release);
    }

    #[inline]
    pub fn flushed_position(&self) -> i32 {
        self.flushed_position.load(Ordering::Acquire)
    }

    #[inline]
    pub fn set_flushed_position(&self, position: i32) {
        self.flushed_position.store(position, Ordering::SeqCst);
    }

    #[inline]
    pub fn record_flush_success(&self, position: i32) {
        self.flushed_position.store(position, Ordering::Release);
        self.record_flush_time();
    }

    #[inline]
    pub fn record_flush_time(&self) {
        self.last_flush_time.store(current_millis(), Ordering::Relaxed);
    }

    #[inline]
    pub fn store_timestamp(&self) -> u64 {
        self.store_timestamp.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn set_store_timestamp(&self, timestamp: u64) {
        self.store_timestamp.store(timestamp, Ordering::Release);
    }

    #[inline]
    pub fn record_append(&self, wrote_bytes: i32, store_timestamp: u64) {
        self.advance_wrote_position(wrote_bytes);
        self.set_store_timestamp(store_timestamp);
    }

    #[inline]
    pub fn last_flush_time(&self) -> u64 {
        self.last_flush_time.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn start_timestamp(&self) -> i64 {
        self.start_timestamp.load(Ordering::Acquire)
    }

    #[inline]
    pub fn set_start_timestamp(&self, timestamp: i64) {
        self.start_timestamp.store(timestamp, Ordering::Release);
    }

    #[inline]
    pub fn stop_timestamp(&self) -> i64 {
        self.stop_timestamp.load(Ordering::Acquire)
    }

    #[inline]
    pub fn set_stop_timestamp(&self, timestamp: i64) {
        self.stop_timestamp.store(timestamp, Ordering::Release);
    }

    #[inline]
    pub fn read_position(&self, transient_store: bool) -> i32 {
        if transient_store {
            self.committed_position()
        } else {
            self.wrote_position()
        }
    }
}

/// Shared state for the mapped-file reference lifecycle.
#[repr(align(64))]
pub struct ReferenceResourceBase {
    pub ref_count: AtomicI64,
    pub available: AtomicBool,
    pub cleanup_over: AtomicBool,
    pub first_shutdown_timestamp: AtomicU64,
    pub hold_lock: Mutex<()>,
    pub release_lock: Mutex<()>,
}

impl ReferenceResourceBase {
    #[inline]
    pub fn new() -> Self {
        Self {
            ref_count: AtomicI64::new(1),
            available: AtomicBool::new(true),
            cleanup_over: AtomicBool::new(false),
            first_shutdown_timestamp: AtomicU64::new(0),
            hold_lock: Mutex::new(()),
            release_lock: Mutex::new(()),
        }
    }
}

impl Default for ReferenceResourceBase {
    fn default() -> Self {
        Self::new()
    }
}

/// Lifecycle behavior shared by local mapped-file resources.
pub trait ReferenceResource: Send + Sync {
    fn base(&self) -> &ReferenceResourceBase;

    #[inline]
    fn hold(&self) -> bool {
        if !self.base().available.load(Ordering::Relaxed) {
            return false;
        }
        let _guard = self.base().hold_lock.lock();
        if self.base().available.load(Ordering::Acquire) {
            let previous = self.base().ref_count.fetch_add(1, Ordering::Relaxed);
            if previous > 0 {
                return true;
            }
            self.base().ref_count.fetch_sub(1, Ordering::Relaxed);
        }
        false
    }

    #[inline]
    fn is_available(&self) -> bool {
        self.base().available.load(Ordering::Relaxed)
    }

    fn shutdown(&self, interval_forcibly: u64) {
        if self.base().available.load(Ordering::Acquire) {
            self.base().available.store(false, Ordering::Release);
            self.base()
                .first_shutdown_timestamp
                .store(current_millis(), Ordering::Release);
            self.release();
        } else if self.get_ref_count() > 0 {
            let elapsed = current_millis().saturating_sub(self.base().first_shutdown_timestamp.load(Ordering::Acquire));
            if elapsed >= interval_forcibly {
                let current_count = self.get_ref_count();
                self.base().ref_count.store(-1000 - current_count, Ordering::Release);
                self.release();
            }
        }
    }

    #[inline]
    fn release(&self) {
        let value = self.base().ref_count.fetch_sub(1, Ordering::Release) - 1;
        if value > 0 {
            return;
        }
        fence(Ordering::Acquire);
        let _guard = self.base().release_lock.lock();
        if !self.base().cleanup_over.load(Ordering::Relaxed) {
            let cleanup_result = self.cleanup(value);
            self.base().cleanup_over.store(cleanup_result, Ordering::Release);
        }
    }

    #[inline]
    fn get_ref_count(&self) -> i64 {
        self.base().ref_count.load(Ordering::Relaxed)
    }

    fn cleanup(&self, current_ref: i64) -> bool;

    #[inline]
    fn is_cleanup_over(&self) -> bool {
        self.base().cleanup_over.load(Ordering::Relaxed) && self.base().ref_count.load(Ordering::Relaxed) <= 0
    }
}

/// Default lifecycle counter used by the Store compatibility adapter.
pub struct ReferenceResourceCounter {
    base: ReferenceResourceBase,
}

impl ReferenceResourceCounter {
    #[inline]
    pub fn new() -> Self {
        Self {
            base: ReferenceResourceBase::new(),
        }
    }

    #[inline]
    pub fn base(&self) -> &ReferenceResourceBase {
        &self.base
    }
}

impl Default for ReferenceResourceCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl ReferenceResource for ReferenceResourceCounter {
    fn base(&self) -> &ReferenceResourceBase {
        &self.base
    }

    fn cleanup(&self, _current_ref: i64) -> bool {
        true
    }
}
