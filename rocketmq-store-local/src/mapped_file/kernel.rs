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

/// Fixed legacy page unit used by mapped-file flush and commit thresholds.
///
/// This value intentionally remains 4 KiB on every platform. It is a compatibility unit rather
/// than a query of the host operating system's current page size.
pub const OS_PAGE_SIZE: u64 = 1024 * 4;

/// One ordered operation in the compatibility warmup schedule for a mapped file.
///
/// The schedule describes byte offsets and ranges only. Callers retain ownership of the mapped
/// memory, platform I/O, error handling, metrics, and lifecycle behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MappedFileWarmupOperation {
    /// Touch the page whose first scheduled byte is `offset`.
    Touch { offset: usize },
    /// Flush the byte range beginning at `offset`.
    Flush {
        offset: usize,
        len: usize,
        final_flush: bool,
    },
}

/// Visits the legacy mapped-file warmup schedule without allocating an operation list.
///
/// `page_size` and `flush_every_pages` are normalized to at least one. Synchronous schedules
/// preserve the historical periodic boundary of `offset + 1`; that boundary intentionally does
/// not expand to the end of the page. An empty file emits no operations.
pub fn visit_mapped_file_warmup_schedule<F>(
    file_size: usize,
    page_size: usize,
    flush_every_pages: usize,
    sync_flush: bool,
    mut visitor: F,
) where
    F: FnMut(MappedFileWarmupOperation),
{
    if file_size == 0 {
        return;
    }
    let page_size = page_size.max(1);
    let flush_every_pages = flush_every_pages.max(1);
    let mut touched_pages = 0usize;
    let mut last_flush_offset = 0usize;
    for offset in (0..file_size).step_by(page_size) {
        visitor(MappedFileWarmupOperation::Touch { offset });
        touched_pages += 1;
        if sync_flush && touched_pages.is_multiple_of(flush_every_pages) {
            let end = (offset + 1).min(file_size);
            visitor(MappedFileWarmupOperation::Flush {
                offset: last_flush_offset,
                len: end - last_flush_offset,
                final_flush: false,
            });
            last_flush_offset = end;
        }
    }
    if sync_flush && last_flush_offset < file_size {
        visitor(MappedFileWarmupOperation::Flush {
            offset: last_flush_offset,
            len: file_size - last_flush_offset,
            final_flush: true,
        });
    }
}

/// Platform-independent integer plan for one mapped-file cache-residency query.
///
/// The plan does not own or dereference mapped memory. Platform adapters retain responsibility
/// for validating the requested file range and invoking the operating-system residency probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MappedFileCacheResidencyPlan {
    /// Page-aligned integer address passed to the platform adapter.
    pub aligned_start: usize,
    /// Number of bytes checked from `aligned_start`, including the leading page offset.
    pub checked_len: usize,
    /// Number of page-residency result bytes required by the platform adapter.
    pub page_count: usize,
}

/// Plans the aligned address range and residency-vector size for a mapped-file cache query.
///
/// This preserves the legacy platform-sized position conversion and saturating address/length
/// arithmetic. `page_size` is normalized to at least one. Range validation intentionally remains
/// the caller's responsibility.
#[inline]
pub fn plan_mapped_file_cache_residency(
    base_addr: usize,
    position: i64,
    size: usize,
    page_size: usize,
) -> Option<MappedFileCacheResidencyPlan> {
    let position = position as usize;
    let page_size = page_size.max(1);
    let start_addr = base_addr.saturating_add(position);
    let aligned_start = start_addr / page_size * page_size;
    let page_offset = start_addr - aligned_start;
    let checked_len = page_offset.saturating_add(size);
    let page_count = checked_len.div_ceil(page_size);
    if page_count == 0 {
        return None;
    }

    Some(MappedFileCacheResidencyPlan {
        aligned_start,
        checked_len,
        page_count,
    })
}

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

    /// Clips a requested memory-lock range to this mapped-file segment.
    ///
    /// Returns the platform-sized byte offset and clipped non-zero length, or `None` when the
    /// request is empty, begins outside the segment, or its offset cannot be represented by
    /// `usize` on the current platform.
    #[inline]
    pub fn lock_region_range(&self, offset: u64, requested_len: usize) -> Option<(usize, usize)> {
        if requested_len == 0 || offset >= self.file_size() {
            return None;
        }

        let remaining = self.file_size().saturating_sub(offset);
        let len = requested_len.min(usize::try_from(remaining).unwrap_or(usize::MAX));
        if len == 0 {
            return None;
        }

        let offset = usize::try_from(offset).ok()?;
        Some((offset, len))
    }

    /// Returns whether a non-empty cache-residency query fits in this mapped-file segment.
    ///
    /// The signed position is rejected before preserving the legacy platform-sized conversion.
    /// The end boundary is checked without wrapping.
    #[inline]
    pub fn is_valid_cache_range(&self, position: i64, size: usize) -> bool {
        if position < 0 || size == 0 {
            return false;
        }

        let file_size = self.file_size() as usize;
        let position = position as usize;
        position < file_size && position.checked_add(size).is_some_and(|end| end <= file_size)
    }

    /// Returns whether the readable progress has reached the legacy flush threshold.
    ///
    /// `read_position` must be the committed position when a transient store pool is active and
    /// the wrote position otherwise. Positions are expected to satisfy the mapped-file progress
    /// invariants and remain within the configured segment size.
    #[inline]
    pub fn is_able_to_flush(&self, read_position: i32, flush_least_pages: i32) -> bool {
        if self.is_full() {
            return true;
        }
        let flush = self.flushed_position();
        if flush_least_pages > 0 {
            return (read_position - flush) / OS_PAGE_SIZE as i32 >= flush_least_pages;
        }
        read_position > flush
    }

    /// Returns whether wrote progress has reached the legacy commit threshold.
    ///
    /// Positions are expected to satisfy the mapped-file progress invariants and remain within
    /// the configured segment size.
    #[inline]
    pub fn is_able_to_commit(&self, commit_least_pages: i32) -> bool {
        if self.is_full() {
            return true;
        }
        let committed = self.committed_position();
        let write = self.wrote_position();
        if commit_least_pages > 0 {
            return (write - committed) / OS_PAGE_SIZE as i32 >= commit_least_pages;
        }
        write > committed
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
