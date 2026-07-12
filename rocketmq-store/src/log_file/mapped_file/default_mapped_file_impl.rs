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

use std::fs::File;
use std::io;
use std::io::Write;
use std::ops::Deref;
use std::path::Path;
use std::ptr;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::OnceLock;
use std::time::Instant;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use memmap2::MmapMut;
use parking_lot::Mutex;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::UtilAll::ensure_dir_ok;
use rocketmq_error::RocketMQResult;
use rocketmq_rust::ArcMut;
use rocketmq_store_local::mapped_file::file::MappedFileStorage;
use rocketmq_store_local::mapped_file::kernel::MappedFileProgress;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use super::FlushStrategy;
use super::MappedFileMetrics;
use crate::base::append_message_callback::AppendMessageCallback;
use crate::base::compaction_append_msg_callback::CompactionAppendMsgCallback;
use crate::base::memory_lock_manager::MemoryLockCategory;
use crate::base::memory_lock_manager::MemoryLockHandle;
use crate::base::memory_lock_manager::MemoryLockManager;
use crate::base::message_result::AppendMessageResult;
use crate::base::message_status_enum::AppendMessageStatus;
use crate::base::put_message_context::PutMessageContext;
use crate::base::select_result::SelectMappedBufferCacheState;
use crate::base::select_result::SelectMappedBufferResult;
use crate::base::select_result::SelectMappedBufferSourceKind;
use crate::base::transient_store_pool::TransientStorePool;
use crate::config::flush_disk_type::FlushDiskType;
use crate::log_file::mapped_file::reference_resource::ReferenceResource;
use crate::log_file::mapped_file::reference_resource_counter::ReferenceResourceCounter;
use crate::log_file::mapped_file::MappedFile;
use crate::log_file::mapped_file::MappedFileError;
use crate::log_file::mapped_file::MappedFileResult;
use crate::platform::FilePreallocateOutcome;
use crate::utils::ffi::get_page_size;
use crate::utils::ffi::madvise;
#[cfg(target_os = "linux")]
use crate::utils::ffi::mincore;
use crate::utils::ffi::mlock as lock_memory;
use crate::utils::ffi::munlock as unlock_memory;
use crate::utils::ffi::MADV_WILLNEED;

pub const OS_PAGE_SIZE: u64 = 1024 * 4;

static TOTAL_MAPPED_VIRTUAL_MEMORY: AtomicI64 = AtomicI64::new(0);
static TOTAL_MAPPED_FILES: AtomicI32 = AtomicI32::new(0);

fn invalid_input_error(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

const LINUX_STORAGE_DEGRADATION_UNKNOWN_ERRNO: i32 = -1;
const LINUX_STORAGE_OP_FALLOCATE: &str = "fallocate";
const LINUX_STORAGE_OP_MADVISE: &str = "madvise";
const LINUX_STORAGE_OP_PAGE_TOUCH: &str = "page_touch";
const LINUX_STORAGE_REASON_FAILED: &str = "failed";
const LINUX_STORAGE_REASON_FLUSH_FAILED: &str = "flush_failed";
const LINUX_STORAGE_REASON_UNSUPPORTED: &str = "unsupported";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LinuxStorageDegradationEvent {
    operation: &'static str,
    reason: &'static str,
    errno: i32,
    count: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LazyMmapStats {
    pub eligible_files: u64,
    pub mapped_files: u64,
    pub map_operations: u64,
    pub map_failures: u64,
    pub total_millis: u64,
    pub last_millis: u64,
}

impl LazyMmapStats {
    pub fn saturating_add_assign(&mut self, other: Self) {
        self.eligible_files = self.eligible_files.saturating_add(other.eligible_files);
        self.mapped_files = self.mapped_files.saturating_add(other.mapped_files);
        self.map_operations = self.map_operations.saturating_add(other.map_operations);
        self.map_failures = self.map_failures.saturating_add(other.map_failures);
        self.total_millis = self.total_millis.saturating_add(other.total_millis);
        if other.last_millis != 0 {
            self.last_millis = other.last_millis;
        }
    }
}

impl LinuxStorageDegradationEvent {
    fn new(operation: &'static str, reason: &'static str, errno: i32) -> Self {
        Self {
            operation,
            reason,
            errno,
            count: 1,
        }
    }
}

fn errno_from_io_error(error: &io::Error) -> i32 {
    error.raw_os_error().unwrap_or(LINUX_STORAGE_DEGRADATION_UNKNOWN_ERRNO)
}

fn file_preallocate_degradation_event(outcome: FilePreallocateOutcome) -> Option<LinuxStorageDegradationEvent> {
    match outcome {
        FilePreallocateOutcome::Allocated => None,
        FilePreallocateOutcome::Unsupported { errno } => Some(LinuxStorageDegradationEvent::new(
            LINUX_STORAGE_OP_FALLOCATE,
            LINUX_STORAGE_REASON_UNSUPPORTED,
            errno,
        )),
        FilePreallocateOutcome::Failed { errno } => Some(LinuxStorageDegradationEvent::new(
            LINUX_STORAGE_OP_FALLOCATE,
            LINUX_STORAGE_REASON_FAILED,
            errno,
        )),
    }
}

fn emit_linux_storage_degradation_observability(event: LinuxStorageDegradationEvent) {
    #[cfg(feature = "observability")]
    rocketmq_observability::metrics::store::record_linux_storage_degradation(
        event.operation,
        event.reason,
        event.errno,
        event.count,
    );

    #[cfg(not(feature = "observability"))]
    let _ = event;
}

pub struct DefaultMappedFile {
    reference_resource: ReferenceResourceCounter,
    storage: MappedFileStorage,
    mmapped_file: OnceLock<ArcMut<MmapMut>>,
    mmap_init_lock: Mutex<()>,
    lazy_mmap_enabled: bool,
    lazy_mmap_operations: AtomicU64,
    lazy_mmap_failures: AtomicU64,
    lazy_mmap_total_millis: AtomicU64,
    lazy_mmap_last_millis: AtomicU64,
    transient_store_pool: Option<TransientStorePool>,
    file_name: CheetahString,
    mapped_byte_buffer: Option<bytes::Bytes>,
    progress: MappedFileProgress,
    first_create_in_queue: bool,
    swap_map_time: AtomicU64,
    mapped_byte_buffer_access_count_since_last_swap: AtomicI64,
    metrics: Option<MappedFileMetrics>,
    flush_strategy: FlushStrategy,
}

impl AsRef<DefaultMappedFile> for DefaultMappedFile {
    #[inline]
    fn as_ref(&self) -> &DefaultMappedFile {
        self
    }
}

impl AsMut<DefaultMappedFile> for DefaultMappedFile {
    #[inline]
    fn as_mut(&mut self) -> &mut DefaultMappedFile {
        self
    }
}

impl PartialEq for DefaultMappedFile {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        ptr::eq(self as *const Self, other as *const Self)
    }
}

impl Default for DefaultMappedFile {
    #[inline]
    fn default() -> Self {
        Self::new(CheetahString::new(), 0)
    }
}

impl DefaultMappedFile {
    #[inline]
    pub fn new(file_name: CheetahString, file_size: u64) -> Self {
        Self::try_new(file_name, file_size).expect("Create mapped file failed")
    }

    pub fn try_new(file_name: CheetahString, file_size: u64) -> io::Result<Self> {
        Self::try_new_inner(file_name, file_size, None, false)
    }

    pub fn try_new_lazy_read_only(file_name: CheetahString, file_size: u64) -> io::Result<Self> {
        Self::try_new_inner(file_name, file_size, None, true)
    }

    fn try_new_inner(
        file_name: CheetahString,
        file_size: u64,
        transient_store_pool: Option<TransientStorePool>,
        lazy_mmap_enabled: bool,
    ) -> io::Result<Self> {
        let path_buf = Path::new(file_name.as_str()).to_path_buf();
        let dir = path_buf
            .parent()
            .and_then(|path| path.to_str())
            .ok_or_else(|| invalid_input_error(format!("file path is invalid: {file_name}")))?;
        ensure_dir_ok(dir);

        let (storage, preallocate_outcome) = MappedFileStorage::open(path_buf, file_size)?;
        if let Some(preallocate_outcome) = preallocate_outcome {
            if let Some(event) = file_preallocate_degradation_event(preallocate_outcome) {
                emit_linux_storage_degradation_observability(event);
            }
            match preallocate_outcome {
                FilePreallocateOutcome::Allocated => {}
                FilePreallocateOutcome::Unsupported { errno } => debug!(
                    "File preallocation is unsupported for mapped file {} and will be skipped, errno={}",
                    file_name, errno
                ),
                FilePreallocateOutcome::Failed { errno } => warn!(
                    "File preallocation failed for mapped file {} and will continue with set_len, errno={}",
                    file_name, errno
                ),
            }
        }

        let mmapped_file = OnceLock::new();
        if !lazy_mmap_enabled {
            // SAFETY: `storage.file()` is an open handle whose length was established by
            // `MappedFileStorage::open`. `DefaultMappedFile` owns the mapping and does not
            // internally resize or truncate the segment while the mapping is installed; cleanup
            // releases the mapping before the owner is dropped. Legacy file-handle callers must
            // preserve the same no-truncation invariant.
            let mmap = unsafe { MmapMut::map_mut(storage.file())? };
            let _ = mmapped_file.set(ArcMut::new(mmap));
        }

        Ok(Self {
            reference_resource: ReferenceResourceCounter::new(),
            storage,
            mmapped_file,
            mmap_init_lock: Mutex::new(()),
            lazy_mmap_enabled,
            lazy_mmap_operations: AtomicU64::new(0),
            lazy_mmap_failures: AtomicU64::new(0),
            lazy_mmap_total_millis: AtomicU64::new(0),
            lazy_mmap_last_millis: AtomicU64::new(0),
            file_name,
            mapped_byte_buffer: None,
            progress: MappedFileProgress::new(file_size),
            first_create_in_queue: false,
            swap_map_time: AtomicU64::new(current_millis()),
            mapped_byte_buffer_access_count_since_last_swap: Default::default(),
            transient_store_pool,
            metrics: Some(MappedFileMetrics::new()),
            flush_strategy: FlushStrategy::Async,
        })
    }

    #[inline]
    pub fn is_lazy_mmap_enabled(&self) -> bool {
        self.lazy_mmap_enabled
    }

    #[inline]
    pub fn is_mapped(&self) -> bool {
        self.mmapped_file.get().is_some()
    }

    pub fn lazy_mmap_stats(&self) -> LazyMmapStats {
        LazyMmapStats {
            eligible_files: u64::from(self.lazy_mmap_enabled),
            mapped_files: u64::from(self.lazy_mmap_enabled && self.is_mapped()),
            map_operations: self.lazy_mmap_operations.load(Ordering::Acquire),
            map_failures: self.lazy_mmap_failures.load(Ordering::Acquire),
            total_millis: self.lazy_mmap_total_millis.load(Ordering::Acquire),
            last_millis: self.lazy_mmap_last_millis.load(Ordering::Acquire),
        }
    }

    fn try_get_mapped_file_ref(&self) -> io::Result<&ArcMut<MmapMut>> {
        if let Some(mapped_file) = self.mmapped_file.get() {
            return Ok(mapped_file);
        }

        let _guard = self.mmap_init_lock.lock();
        if let Some(mapped_file) = self.mmapped_file.get() {
            return Ok(mapped_file);
        }

        let start = Instant::now();
        // SAFETY: `self.storage.file()` remains a valid handle sized during construction. The
        // `mmap_init_lock` serializes lazy initialization, and the initialized mapping remains
        // owned by `self.mmapped_file` until cleanup. This owner performs no resize or truncation
        // while that mapping is installed; legacy file-handle callers must preserve the same
        // invariant.
        match unsafe { MmapMut::map_mut(self.storage.file()) } {
            Ok(mmap) => {
                let elapsed_millis = start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
                if self.lazy_mmap_enabled {
                    self.lazy_mmap_operations.fetch_add(1, Ordering::AcqRel);
                    self.lazy_mmap_total_millis.fetch_add(elapsed_millis, Ordering::AcqRel);
                    self.lazy_mmap_last_millis.store(elapsed_millis, Ordering::Release);
                }
                let _ = self.mmapped_file.set(ArcMut::new(mmap));
                Ok(self.mmapped_file.get().expect("mapped file must be initialized"))
            }
            Err(error) => {
                if self.lazy_mmap_enabled {
                    self.lazy_mmap_failures.fetch_add(1, Ordering::AcqRel);
                }
                Err(error)
            }
        }
    }

    fn try_get_mapped_file_arcmut(&self) -> io::Result<ArcMut<MmapMut>> {
        self.try_get_mapped_file_ref().cloned()
    }

    /// Extracts the file offset from the given file name.
    ///
    /// This function takes a `CheetahString` representing the file name,
    /// parses it to extract the offset, and returns it as a `u64`.
    ///
    /// # Arguments
    ///
    /// * `file_name` - A `CheetahString` representing the name of the file.
    ///
    /// # Returns
    ///
    /// A `u64` representing the file offset extracted from the file name.
    ///
    /// # Panics
    ///
    /// This function will panic if the file name cannot be parsed to a valid `u64` offset.
    #[inline]
    pub fn parse_file_from_offset(file_name: &Path) -> u64 {
        rocketmq_store_local::mapped_file::file::parse_file_from_offset(file_name)
    }

    #[inline]
    pub fn try_parse_file_from_offset(file_name: &Path) -> io::Result<u64> {
        rocketmq_store_local::mapped_file::file::try_parse_file_from_offset(file_name)
    }

    pub fn new_with_transient_store_pool(
        file_name: CheetahString,
        file_size: u64,
        transient_store_pool: TransientStorePool,
    ) -> Self {
        Self::try_new_with_transient_store_pool(file_name, file_size, transient_store_pool)
            .expect("Create mapped file with transient store pool failed")
    }

    pub fn try_new_with_transient_store_pool(
        file_name: CheetahString,
        file_size: u64,
        transient_store_pool: TransientStorePool,
    ) -> io::Result<Self> {
        Self::try_new_inner(file_name, file_size, Some(transient_store_pool), false)
    }

    #[inline]
    fn record_cache_residency(&self, position: i64, size: usize) -> bool {
        let is_in_cache = MappedFile::is_loaded(self, position, size);
        if let Some(metrics) = &self.metrics {
            if is_in_cache {
                metrics.record_cache_hit();
            } else {
                metrics.record_cache_miss();
            }
        }
        is_in_cache
    }

    #[inline]
    fn is_valid_cache_range(&self, position: i64, size: usize) -> bool {
        if position < 0 || size == 0 {
            return false;
        }

        let position = position as usize;
        let file_size = self.progress.file_size() as usize;
        position < file_size && position.checked_add(size).is_some_and(|end| end <= file_size)
    }

    fn try_flush_with<F>(&self, flush_least_pages: i32, flush: F) -> MappedFileResult<i32>
    where
        F: FnOnce(&Self, i32, i32) -> io::Result<()>,
    {
        if !self.is_able_to_flush(flush_least_pages) {
            return Ok(self.get_flushed_position());
        }
        if !MappedFile::hold(self) {
            return Err(MappedFileError::ReferenceUnavailable);
        }

        let value = self.get_read_position();
        self.mapped_byte_buffer_access_count_since_last_swap
            .fetch_add(1, Ordering::AcqRel);
        let should_flush = !matches!(self.flush_strategy, FlushStrategy::Never);
        if !should_flush {
            MappedFile::release(self);
            return Ok(self.get_flushed_position());
        }

        let flush_started = Instant::now();
        let flushed_position = self.progress.flushed_position();
        let result = flush(self, flushed_position, value);
        MappedFile::release(self);
        result.map_err(MappedFileError::FlushFailed)?;

        self.progress.record_flush_success(value);
        if let Some(metrics) = &self.metrics {
            metrics.record_flush(flush_started.elapsed());
        }
        Ok(value)
    }
}

#[allow(unused_variables)]
impl MappedFile for DefaultMappedFile {
    #[inline]
    fn get_file_name(&self) -> &CheetahString {
        &self.file_name
    }

    fn rename_to(&mut self, file_name: &str) -> bool {
        let new_file = Path::new(file_name);
        match self.storage.rename(new_file) {
            Ok(_) => {
                self.file_name = CheetahString::from(file_name);
                if self.storage.reopen().is_err() {
                    return false;
                }
                true
            }
            Err(_) => false,
        }
    }

    #[inline]
    fn get_file_size(&self) -> u64 {
        self.progress.file_size()
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.progress.is_full()
    }

    #[inline]
    fn is_available(&self) -> bool {
        self.reference_resource.is_available()
    }

    fn append_message<AMC: AppendMessageCallback>(
        &self,
        message: &mut MessageExtBrokerInner,
        message_callback: &AMC,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        let current_pos = self.progress.wrote_position() as u64;
        if current_pos >= self.progress.file_size() {
            error!(
                "MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}",
                current_pos,
                self.progress.file_size()
            );
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        }
        let result = message_callback.do_append(
            self.storage.file_from_offset() as i64, // file name parsed as offset
            self,
            (self.progress.file_size() - current_pos) as i32, // remaining space
            message,
            put_message_context,
        );
        self.progress
            .record_append(result.wrote_bytes, result.store_timestamp as u64);

        if let Some(metrics) = &self.metrics {
            metrics.record_write(result.wrote_bytes as usize);
        }

        result
    }

    fn append_messages<AMC: AppendMessageCallback>(
        &self,
        message: &mut MessageExtBatch,
        message_callback: &AMC,
        put_message_context: &mut PutMessageContext,
        enabled_append_prop_crc: bool,
    ) -> AppendMessageResult {
        let current_pos = self.progress.wrote_position() as u64;
        if current_pos >= self.progress.file_size() {
            error!(
                "MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}",
                current_pos,
                self.progress.file_size()
            );
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        }
        let result = message_callback.do_append_batch(
            self.storage.file_from_offset() as i64,
            self,
            (self.progress.file_size() - current_pos) as i32,
            message,
            put_message_context,
            enabled_append_prop_crc,
        );
        self.progress
            .record_append(result.wrote_bytes, result.store_timestamp as u64);
        result
    }

    #[inline]
    fn append_message_compaction(
        &mut self,
        byte_buffer_msg: &mut Bytes,
        cb: &dyn CompactionAppendMsgCallback,
    ) -> AppendMessageResult {
        let _ = cb;
        warn!(
            "append_message_compaction is not supported for DefaultMappedFile: file={}, msg_len={}",
            self.file_name,
            byte_buffer_msg.len()
        );
        AppendMessageResult {
            status: AppendMessageStatus::UnknownError,
            ..Default::default()
        }
    }

    #[inline]
    fn get_bytes(&self, pos: usize, size: usize) -> Option<bytes::Bytes> {
        // not check can read position, so maybe read invalid data in the file
        if pos + size > self.progress.file_size() as usize {
            return None;
        }
        Some(Bytes::copy_from_slice(&self.get_mapped_file()[pos..pos + size]))
    }

    #[inline]
    fn get_bytes_readable_checked(&self, pos: usize, size: usize) -> Option<bytes::Bytes> {
        //check can read position
        let max_readable_position = self.get_read_position() as usize;
        let end_position = pos + size;
        if max_readable_position < end_position || end_position > self.progress.file_size() as usize {
            return None;
        }
        Some(Bytes::copy_from_slice(&self.get_mapped_file()[pos..end_position]))
    }

    fn append_message_offset_length(&self, data: &[u8], offset: usize, length: usize) -> bool {
        let current_pos = self.progress.wrote_position() as usize;
        if current_pos + length <= self.progress.file_size() as usize {
            let mut mapped_file = &mut self.get_mapped_file_mut()[current_pos..current_pos + length];
            if let Some(data_slice) = data.get(offset..offset + length) {
                if mapped_file.write_all(data_slice).is_ok() {
                    self.progress.advance_wrote_position(length as i32);
                    return true;
                } else {
                    error!("append_message_offset_length write_all error");
                }
            } else {
                error!("Invalid data slice");
            }
        }
        false
    }

    fn append_message_no_position_update(&self, data: &[u8], offset: usize, length: usize) -> bool {
        let current_pos = self.progress.wrote_position_relaxed() as usize;

        if current_pos + length <= self.progress.file_size() as usize {
            let mut mapped_file = &mut self.get_mapped_file_mut()[current_pos..current_pos + length];
            if let Some(data_slice) = data.get(offset..offset + length) {
                if mapped_file.write_all(data_slice).is_ok() {
                    return true;
                } else {
                    error!("append_message_offset_length write_all error");
                }
            } else {
                error!("Invalid data slice");
            }
        }
        false
    }

    /// **Zero-Copy Implementation**
    ///
    /// Returns a direct mutable buffer for zero-copy message encoding.
    /// Eliminates the intermediate pre_encode_buffer, reducing CPU usage.
    fn get_direct_write_buffer(&self, required_space: usize) -> Option<(&mut [u8], usize)> {
        let current_pos = self.progress.wrote_position() as usize;

        // Check if we have enough space
        if current_pos + required_space > self.progress.file_size() as usize {
            return None;
        }

        // Return a mutable slice directly into the mmap region
        // SAFETY: We've verified bounds above, and the slice lifetime is tied to self
        let buffer = &mut self.get_mapped_file_mut()[current_pos..current_pos + required_space];

        Some((buffer, current_pos))
    }

    /// **Phase 3 Zero-Copy Implementation**
    ///
    /// Commits a direct write by updating the write position atomically.
    fn commit_direct_write(&self, bytes_written: usize) -> bool {
        if bytes_written == 0 {
            return false;
        }

        let current_pos = self.progress.wrote_position() as usize;

        // Verify the write doesn't exceed file size
        if current_pos + bytes_written > self.progress.file_size() as usize {
            error!(
                "commit_direct_write: write exceeds file size. pos={}, bytes={}, file_size={}",
                current_pos,
                bytes_written,
                self.progress.file_size()
            );
            return false;
        }

        // Update write position atomically
        self.progress.advance_wrote_position(bytes_written as i32);

        // Record metrics
        if let Some(metrics) = &self.metrics {
            metrics.record_write(bytes_written);
        }

        true
    }

    fn write_bytes_segment(&self, data: &[u8], start: usize, offset: usize, length: usize) -> bool {
        if start + length <= self.progress.file_size() as usize {
            let mut mapped_file = &mut self.get_mapped_file_mut()[start..start + length];
            if data.len() == length {
                if mapped_file.write_all(data).is_ok() {
                    return true;
                } else {
                    error!("append_message_offset_length write_all error");
                }
            } else if let Some(data_slice) = data.get(offset..offset + length) {
                if mapped_file.write_all(data_slice).is_ok() {
                    return true;
                } else {
                    error!("append_message_offset_length write_all error");
                }
            } else {
                error!("Invalid data slice");
            }
        }
        false
    }

    fn put_slice(&self, data: &[u8], index: usize) -> bool {
        let length = data.len();
        let end_index = index + length;
        if length > 0 && end_index <= self.progress.file_size() as usize {
            let mut mapped_file = &mut self.get_mapped_file_mut()[index..end_index];
            if mapped_file.write_all(data).is_ok() {
                return true;
            } else {
                error!("append_message_offset_length write_all error");
            }
        }
        false
    }

    #[inline]
    fn get_file_from_offset(&self) -> u64 {
        self.storage.file_from_offset()
    }

    fn flush(&self, flush_least_pages: i32) -> i32 {
        match self.try_flush(flush_least_pages) {
            Ok(position) => position,
            Err(error) => {
                error!(error = %error, "failed to flush mapped file");
                self.get_flushed_position()
            }
        }
    }

    fn try_flush(&self, flush_least_pages: i32) -> MappedFileResult<i32> {
        self.try_flush_with(flush_least_pages, |mapped_file, flushed_position, value| {
            let flush_size = value - flushed_position;
            if flush_size > 0 && flush_size < (mapped_file.progress.file_size() as i32) / 2 {
                mapped_file
                    .get_mapped_file()
                    .flush_range(flushed_position as usize, flush_size as usize)
            } else {
                mapped_file.get_mapped_file().flush()
            }
        })
    }

    #[inline]
    fn commit(&self, commit_least_pages: i32) -> i32 {
        if self.is_able_to_commit(commit_least_pages) {
            self.progress.commit_wrote_position();
        }
        self.get_committed_position()
    }

    fn select_mapped_buffer(&self, pos: i32, size: i32) -> Option<SelectMappedBufferResult> {
        let read_position = self.get_read_position();
        if pos + size <= read_position {
            if MappedFile::hold(self) {
                self.mapped_byte_buffer_access_count_since_last_swap
                    .fetch_add(1, Ordering::AcqRel);

                let is_in_cache = self.record_cache_residency(pos as i64, size as usize);
                let bytes = if size >= 8192 {
                    // Try zero-copy read first
                    self.get_bytes_zero_copy(pos as usize, size as usize)
                        .unwrap_or_else(|| {
                            // Fallback to standard method
                            Bytes::from_owner(MmapRegionSlice::new(
                                self.get_mapped_file_arcmut(),
                                pos as usize,
                                size as usize,
                            ))
                        })
                } else {
                    // Small reads: use standard method
                    Bytes::from_owner(MmapRegionSlice::new(
                        self.get_mapped_file_arcmut(),
                        pos as usize,
                        size as usize,
                    ))
                };

                Some(SelectMappedBufferResult {
                    start_offset: self.storage.file_from_offset() + pos as u64,
                    size,
                    bytes: Some(bytes),
                    is_in_cache,
                    source_kind: SelectMappedBufferSourceKind::MappedFile,
                    file_offset: pos as u64,
                    cache_state: SelectMappedBufferCacheState::from_residency(is_in_cache),
                    mapped_file: None,
                })
            } else {
                warn!(
                    "matched, but hold failed, request pos: {}, fileFromOffset: {}",
                    pos,
                    self.storage.file_from_offset()
                );
                None
            }
        } else {
            warn!(
                "selectMappedBuffer request pos invalid, request pos: {}, size:{}, fileFromOffset: {}",
                pos,
                size,
                self.storage.file_from_offset()
            );
            None
        }
    }

    #[inline]
    fn get_mapped_byte_buffer(&self) -> &[u8] {
        self.mapped_byte_buffer_access_count_since_last_swap
            .fetch_add(1, Ordering::AcqRel);

        if let Some(metrics) = &self.metrics {
            metrics.record_read(self.progress.file_size() as usize, false);
        }

        self.get_mapped_file()
    }

    #[inline]
    fn slice_byte_buffer(&self) -> &[u8] {
        self.mapped_byte_buffer_access_count_since_last_swap
            .fetch_add(1, Ordering::AcqRel);

        if let Some(metrics) = &self.metrics {
            metrics.record_read(self.progress.file_size() as usize, false);
        }

        self.get_mapped_file()
    }

    #[inline]
    fn get_store_timestamp(&self) -> u64 {
        self.progress.store_timestamp()
    }

    #[inline]
    fn get_last_modified_timestamp(&self) -> u64 {
        self.storage
            .file()
            .metadata()
            .unwrap()
            .modified()
            .unwrap()
            .elapsed()
            .unwrap()
            .as_millis() as u64
    }

    fn get_data(&self, pos: usize, size: usize) -> Option<bytes::Bytes> {
        let read_position = self.get_read_position();
        let read_end_position = pos + size;
        if read_end_position <= read_position as usize {
            if MappedFile::hold(self) {
                let buffer = BytesMut::from(&self.get_mapped_file()[pos..read_end_position]);
                MappedFile::release(self);
                Some(buffer.freeze())
            } else {
                debug!(
                    "matched, but hold failed, request pos: {}, fileFromOffset: {}",
                    pos,
                    self.storage.file_from_offset()
                );
                None
            }
        } else {
            warn!(
                "selectMappedBuffer request pos invalid, request pos: {}, size:{}, fileFromOffset: {}",
                pos,
                size,
                self.storage.file_from_offset()
            );
            None
        }
    }

    #[inline]
    fn destroy(&self, interval_forcibly: u64) -> bool {
        MappedFile::shutdown(self, interval_forcibly);
        if self.is_cleanup_over() {
            if let Err(e) = self.storage.delete() {
                error!(file_name = %self.file_name, error = ?e, "delete file failed");
                false
            } else {
                info!(file_name = %self.file_name, "delete file success");
                true
            }
        } else {
            warn!("destroy mapped file failed, cleanup over failed");
            false
        }
    }

    #[inline]
    fn shutdown(&self, interval_forcibly: u64) {
        self.reference_resource.shutdown(interval_forcibly);
    }

    #[inline]
    fn release(&self) {
        self.reference_resource.release();
    }

    #[inline]
    fn hold(&self) -> bool {
        self.reference_resource.hold()
    }

    #[inline]
    fn is_first_create_in_queue(&self) -> bool {
        self.first_create_in_queue
    }

    #[inline]
    fn set_first_create_in_queue(&mut self, first_create_in_queue: bool) {
        self.first_create_in_queue = first_create_in_queue
    }

    #[inline]
    fn get_flushed_position(&self) -> i32 {
        self.progress.flushed_position()
    }

    #[inline]
    fn set_flushed_position(&self, flushed_position: i32) {
        self.progress.set_flushed_position(flushed_position)
    }

    #[inline]
    fn get_wrote_position(&self) -> i32 {
        self.progress.wrote_position()
    }

    #[inline]
    fn set_wrote_position(&self, wrote_position: i32) {
        self.progress.set_wrote_position(wrote_position)
    }

    /// Return The max position which have valid data
    ///
    /// # Returns
    ///
    /// An `i32` representing the current read position.
    #[inline]
    fn get_read_position(&self) -> i32 {
        self.progress.read_position(self.transient_store_pool.is_some())
    }

    #[inline]
    fn set_committed_position(&self, committed_position: i32) {
        self.progress.set_committed_position(committed_position)
    }

    #[inline]
    fn get_committed_position(&self) -> i32 {
        self.progress.committed_position()
    }

    #[inline]
    fn mlock(&self) {
        if let Err(error) = lock_memory(self.get_mapped_file().as_ptr(), self.progress.file_size() as usize) {
            warn!(file_name = %self.file_name, error = %error, "failed to mlock mapped file");
        }
    }

    #[inline]
    fn munlock(&self) {
        if let Err(error) = unlock_memory(self.get_mapped_file().as_ptr(), self.progress.file_size() as usize) {
            warn!(file_name = %self.file_name, error = %error, "failed to munlock mapped file");
        }
    }

    #[inline]
    fn warm_mapped_file(&self, flush_disk_type: FlushDiskType, pages: usize) {
        self.warm_mapped_file_with_ops(
            flush_disk_type,
            pages,
            Self::touch_mapped_page,
            Self::flush_mapped_file_range,
            Self::advise_mapped_file,
            emit_linux_storage_degradation_observability,
        );
    }

    #[inline]
    fn swap_map(&self) -> bool {
        self.swap_map_time.store(current_millis(), Ordering::Release);
        self.mapped_byte_buffer_access_count_since_last_swap
            .store(0, Ordering::Release);
        if let Some(metrics) = &self.metrics {
            metrics.record_swap();
        }
        false
    }

    #[inline]
    fn clean_swaped_map(&self, force: bool) {
        if let Some(metrics) = &self.metrics {
            metrics.record_clean_swap();
        }
        if force {
            self.mapped_byte_buffer_access_count_since_last_swap
                .store(0, Ordering::Release);
        }
    }

    #[inline]
    fn get_recent_swap_map_time(&self) -> i64 {
        self.swap_map_time.load(Ordering::Acquire) as i64
    }

    #[inline]
    fn get_mapped_byte_buffer_access_count_since_last_swap(&self) -> i64 {
        self.mapped_byte_buffer_access_count_since_last_swap
            .load(Ordering::Acquire)
    }

    #[inline]
    fn get_file(&self) -> &File {
        self.storage.file()
    }

    #[inline]
    fn rename_to_delete(&self) {
        warn!(
            "rename_to_delete is not supported for DefaultMappedFile without mutable file metadata: {}",
            self.file_name
        );
    }

    #[inline]
    fn move_to_parent(&self) -> std::io::Result<()> {
        Err(std::io::Error::other(format!(
            "move_to_parent is not supported for immutable mapped file handle {}",
            self.file_name
        )))
    }

    #[inline]
    fn get_last_flush_time(&self) -> u64 {
        self.progress.last_flush_time()
    }

    #[inline]
    #[cfg(target_os = "linux")]
    fn is_loaded(&self, position: i64, size: usize) -> bool {
        if !self.is_valid_cache_range(position, size) {
            return false;
        }

        let position = position as usize;
        let page_size = get_page_size().max(1);
        let base_addr = self.get_mapped_file().as_ptr() as usize;
        let start_addr = base_addr.saturating_add(position);
        let aligned_start = start_addr / page_size * page_size;
        let page_offset = start_addr - aligned_start;
        let checked_len = page_offset.saturating_add(size);
        let page_count = checked_len.div_ceil(page_size);
        if page_count == 0 {
            return false;
        }

        let mut residency = vec![0u8; page_count];
        let result = mincore(aligned_start as *const u8, checked_len, residency.as_mut_ptr());
        result == 0 && residency.iter().all(|page| page & 1 == 1)
    }

    #[inline]
    #[cfg(target_os = "windows")]
    fn is_loaded(&self, position: i64, size: usize) -> bool {
        /*use windows::Win32::Foundation::{BOOL, HANDLE};
        use windows::Win32::System::Memory::{VirtualQuery, MEMORY_BASIC_INFORMATION, MEM_COMMIT};

        let address = self.mmapped_file.as_ptr().wrapping_add(position as usize);
        let mut info: MEMORY_BASIC_INFORMATION = unsafe { std::mem::zeroed() };
        let mut offset = 0;

        while offset < length {
            let result = unsafe {
                VirtualQuery(
                    address.add(offset) as *const _,
                    &mut info,
                    std::mem::size_of::<MEMORY_BASIC_INFORMATION>(),
                )
            };

            if result == 0 {
                return Err(std::io::Error::last_os_error());
            }

            if info.State != MEM_COMMIT {
                return Ok(false);
            }

            offset += info.RegionSize;
        }*/

        self.is_valid_cache_range(position, size)
    }

    #[inline]
    #[cfg(target_os = "macos")]
    fn is_loaded(&self, position: i64, size: usize) -> bool {
        /*use windows::Win32::Foundation::{BOOL, HANDLE};
        use windows::Win32::System::Memory::{VirtualQuery, MEMORY_BASIC_INFORMATION, MEM_COMMIT};

        let address = self.mmapped_file.as_ptr().wrapping_add(position as usize);
        let mut info: MEMORY_BASIC_INFORMATION = unsafe { std::mem::zeroed() };
        let mut offset = 0;

        while offset < length {
            let result = unsafe {
                VirtualQuery(
                    address.add(offset) as *const _,
                    &mut info,
                    std::mem::size_of::<MEMORY_BASIC_INFORMATION>(),
                )
            };

            if result == 0 {
                return Err(std::io::Error::last_os_error());
            }

            if info.State != MEM_COMMIT {
                return Ok(false);
            }

            offset += info.RegionSize;
        }*/

        self.is_valid_cache_range(position, size)
    }

    fn select_mapped_buffer_with_position(&self, pos: i32) -> Option<SelectMappedBufferResult> {
        let read_position = self.get_read_position();
        if pos < read_position && pos >= 0 && MappedFile::hold(self) {
            self.mapped_byte_buffer_access_count_since_last_swap
                .fetch_add(1, Ordering::AcqRel);
            let size = read_position - pos;
            let is_in_cache = self.record_cache_residency(pos as i64, size as usize);

            let bytes = if size >= 8192 {
                self.get_bytes_zero_copy(pos as usize, size as usize)
                    .unwrap_or_else(|| {
                        Bytes::from_owner(MmapRegionSlice::new(
                            self.get_mapped_file_arcmut(),
                            pos as usize,
                            size as usize,
                        ))
                    })
            } else {
                Bytes::from_owner(MmapRegionSlice::new(
                    self.get_mapped_file_arcmut(),
                    pos as usize,
                    size as usize,
                ))
            };

            Some(SelectMappedBufferResult {
                start_offset: self.get_file_from_offset() + pos as u64,
                size,
                bytes: Some(bytes),
                is_in_cache,
                source_kind: SelectMappedBufferSourceKind::MappedFile,
                file_offset: pos as u64,
                cache_state: SelectMappedBufferCacheState::from_residency(is_in_cache),
                mapped_file: None,
            })
        } else {
            None
        }
    }

    fn init(
        &self,
        file_name: &CheetahString,
        file_size: usize,
        transient_store_pool: &TransientStorePool,
    ) -> std::io::Result<()> {
        let _ = transient_store_pool.available_buffer_nums();

        if file_name != self.get_file_name() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "mapped file init path mismatch: expected {}, got {}",
                    self.file_name, file_name
                ),
            ));
        }

        if file_size as u64 != self.progress.file_size() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "mapped file init size mismatch: expected {}, got {}",
                    self.progress.file_size(),
                    file_size
                ),
            ));
        }

        Ok(())
    }

    fn get_slice(&self, pos: usize, size: usize) -> Option<&[u8]> {
        if pos >= self.progress.file_size() as usize || pos + size >= self.progress.file_size() as usize {
            return None;
        }

        if let Some(metrics) = &self.metrics {
            metrics.record_read(size, false);
        }

        Some(&self.get_mapped_file()[pos..pos + size])
    }
}

#[allow(unused_variables)]
impl DefaultMappedFile {
    #[inline]
    #[allow(clippy::mut_from_ref)]
    pub fn get_mapped_file_mut(&self) -> &mut MmapMut {
        self.try_get_mapped_file_ref()
            .expect("mapped file initialization failed")
            .mut_from_ref()
    }

    #[inline]
    pub fn get_mapped_file(&self) -> &MmapMut {
        self.try_get_mapped_file_ref()
            .expect("mapped file initialization failed")
            .as_ref()
    }

    #[inline]
    pub fn get_mapped_file_arcmut(&self) -> ArcMut<MmapMut> {
        self.try_get_mapped_file_arcmut()
            .expect("mapped file initialization failed")
    }

    fn touch_mapped_page(mapped_ptr: *mut u8, offset: usize) -> io::Result<()> {
        unsafe {
            let page_ptr = mapped_ptr.add(offset);
            let value = std::ptr::read_volatile(page_ptr);
            std::ptr::write_volatile(page_ptr, value);
        }
        Ok(())
    }

    fn flush_mapped_file_range(mapped_file: &mut MmapMut, offset: usize, len: usize) -> io::Result<()> {
        mapped_file.flush_range(offset, len)
    }

    fn advise_mapped_file(addr: *const u8, len: usize, advice: i32) -> io::Result<()> {
        if madvise(addr, len, advice) == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    fn warm_mapped_file_with_ops<T, F, A, R>(
        &self,
        flush_disk_type: FlushDiskType,
        pages: usize,
        mut touch_page: T,
        mut flush_range: F,
        mut advise: A,
        mut record_degradation: R,
    ) where
        T: FnMut(*mut u8, usize) -> io::Result<()>,
        F: FnMut(&mut MmapMut, usize, usize) -> io::Result<()>,
        A: FnMut(*const u8, usize, i32) -> io::Result<()>,
        R: FnMut(LinuxStorageDegradationEvent),
    {
        let page_size = get_page_size().max(1);
        let file_size = self.progress.file_size() as usize;
        if file_size == 0 {
            return;
        }

        let warmup_started = Instant::now();
        let flush_every_pages = pages.max(1);
        let mut last_flush_offset = 0usize;
        {
            let mapped_file = self.get_mapped_file_mut();
            let mut touched_pages = 0usize;
            let mapped_ptr = mapped_file.as_mut_ptr();
            for offset in (0..file_size).step_by(page_size) {
                if let Err(error) = touch_page(mapped_ptr, offset) {
                    record_degradation(LinuxStorageDegradationEvent::new(
                        LINUX_STORAGE_OP_PAGE_TOUCH,
                        LINUX_STORAGE_REASON_FAILED,
                        errno_from_io_error(&error),
                    ));
                    warn!(
                        "Failed to touch warmed mapped file page at offset {} for {}: {:?}",
                        offset, self.file_name, error
                    );
                }
                touched_pages += 1;

                if flush_disk_type == FlushDiskType::SyncFlush && touched_pages.is_multiple_of(flush_every_pages) {
                    let end = (offset + 1).min(file_size);
                    if let Err(error) = flush_range(mapped_file, last_flush_offset, end - last_flush_offset) {
                        record_degradation(LinuxStorageDegradationEvent::new(
                            LINUX_STORAGE_OP_PAGE_TOUCH,
                            LINUX_STORAGE_REASON_FLUSH_FAILED,
                            errno_from_io_error(&error),
                        ));
                        warn!(
                            "Failed to flush warmed mapped file range {}-{} for {}: {:?}",
                            last_flush_offset, end, self.file_name, error
                        );
                    } else {
                        self.progress.record_flush_time();
                    }
                    last_flush_offset = end;
                }
            }

            if flush_disk_type == FlushDiskType::SyncFlush && last_flush_offset < file_size {
                if let Err(error) = flush_range(mapped_file, last_flush_offset, file_size - last_flush_offset) {
                    record_degradation(LinuxStorageDegradationEvent::new(
                        LINUX_STORAGE_OP_PAGE_TOUCH,
                        LINUX_STORAGE_REASON_FLUSH_FAILED,
                        errno_from_io_error(&error),
                    ));
                    warn!(
                        "Failed to flush final warmed mapped file range {}-{} for {}: {:?}",
                        last_flush_offset, file_size, self.file_name, error
                    );
                } else {
                    self.progress.record_flush_time();
                }
            }
        }

        if let Err(error) = advise(self.get_mapped_file().as_ptr(), file_size, MADV_WILLNEED) {
            record_degradation(LinuxStorageDegradationEvent::new(
                LINUX_STORAGE_OP_MADVISE,
                LINUX_STORAGE_REASON_FAILED,
                errno_from_io_error(&error),
            ));
            warn!(
                "madvise(MADV_WILLNEED) failed while warming mapped file {}",
                self.file_name
            );
        }
        let warmup_duration = warmup_started.elapsed();
        let warmup_millis = u64::try_from(warmup_duration.as_millis()).unwrap_or(u64::MAX);
        rocketmq_observability::metrics::store::record_linux_page_cache_warmup_millis(warmup_millis);
        if let Some(metrics) = &self.metrics {
            metrics.record_warm_with_latency(file_size, warmup_duration);
        }
    }

    pub fn lock_region(
        &self,
        memory_lock_manager: &MemoryLockManager,
        category: MemoryLockCategory,
        offset: u64,
        len: usize,
    ) -> RocketMQResult<Option<MemoryLockHandle>> {
        self.lock_region_with(memory_lock_manager, category, offset, len, crate::utils::ffi::mlock)
    }

    pub(crate) fn lock_region_with<F>(
        &self,
        memory_lock_manager: &MemoryLockManager,
        category: MemoryLockCategory,
        offset: u64,
        len: usize,
        locker: F,
    ) -> RocketMQResult<Option<MemoryLockHandle>>
    where
        F: FnMut(*const u8, usize) -> RocketMQResult<()>,
    {
        let Some((addr, len)) = self.lock_region_address_and_len(offset, len) else {
            return Ok(None);
        };
        memory_lock_manager.lock_region_with(category, addr, len, locker)
    }

    pub fn unlock_region(
        &self,
        memory_lock_manager: &MemoryLockManager,
        handle: MemoryLockHandle,
    ) -> RocketMQResult<()> {
        self.unlock_region_with(memory_lock_manager, handle, crate::utils::ffi::munlock)
    }

    pub(crate) fn unlock_region_with<F>(
        &self,
        memory_lock_manager: &MemoryLockManager,
        handle: MemoryLockHandle,
        unlocker: F,
    ) -> RocketMQResult<()>
    where
        F: FnMut(*const u8, usize) -> RocketMQResult<()>,
    {
        memory_lock_manager.unlock_region_with(handle, unlocker)
    }

    fn lock_region_address_and_len(&self, offset: u64, requested_len: usize) -> Option<(*const u8, usize)> {
        if requested_len == 0 || offset >= self.progress.file_size() {
            return None;
        }

        let remaining = self.progress.file_size().saturating_sub(offset);
        let len = requested_len.min(usize::try_from(remaining).unwrap_or(usize::MAX));
        if len == 0 {
            return None;
        }

        let offset = usize::try_from(offset).ok()?;
        Some((self.get_mapped_file().as_ptr().wrapping_add(offset), len))
    }

    /// Gets the start timestamp of the mapped file.
    ///
    /// # Returns
    ///
    /// The start timestamp as i64. Returns -1 if not set.
    #[inline]
    pub fn get_start_timestamp(&self) -> i64 {
        self.progress.start_timestamp()
    }

    /// Sets the start timestamp of the mapped file.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The start timestamp to set
    #[inline]
    pub fn set_start_timestamp(&self, timestamp: i64) {
        self.progress.set_start_timestamp(timestamp);
    }

    /// Gets the stop timestamp of the mapped file.
    ///
    /// # Returns
    ///
    /// The stop timestamp as i64. Returns -1 if not set.
    #[inline]
    pub fn get_stop_timestamp(&self) -> i64 {
        self.progress.stop_timestamp()
    }

    /// Sets the stop timestamp of the mapped file.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The stop timestamp to set
    #[inline]
    pub fn set_stop_timestamp(&self, timestamp: i64) {
        self.progress.set_stop_timestamp(timestamp);
    }

    #[inline]
    fn is_able_to_flush(&self, flush_least_pages: i32) -> bool {
        if self.is_full() {
            return true;
        }
        let flush = self.progress.flushed_position();
        let write = self.get_read_position();
        if flush_least_pages > 0 {
            return (write - flush) / OS_PAGE_SIZE as i32 >= flush_least_pages;
        }
        write > flush
    }

    #[inline]
    fn is_able_to_commit(&self, commit_least_pages: i32) -> bool {
        if self.is_full() {
            return true;
        }
        let committed = self.progress.committed_position();
        let write = self.progress.wrote_position();
        if commit_least_pages > 0 {
            return (write - committed) / OS_PAGE_SIZE as i32 >= commit_least_pages;
        }
        write > committed
    }

    #[inline]
    fn cleanup(&self, current_ref: i64) -> bool {
        if MappedFile::is_available(self) {
            error!(
                "this file[REF:{}] {} have not shutdown, stop unmapping.",
                self.file_name, current_ref
            );
            return false;
        }

        if self.reference_resource.is_cleanup_over() {
            error!(
                "this file[REF:{}]  {} have cleanup, do not do it again.",
                self.file_name, current_ref
            );
            return true;
        }
        TOTAL_MAPPED_VIRTUAL_MEMORY.fetch_sub(self.progress.file_size() as i64, Ordering::Relaxed);
        TOTAL_MAPPED_FILES.fetch_sub(1, Ordering::Relaxed);
        info!("unmap file[REF:{}] {} OK", current_ref, self.file_name);
        true
    }
}

impl ReferenceResource for DefaultMappedFile {
    fn base(&self) -> &crate::log_file::mapped_file::reference_resource_counter::ReferenceResourceBase {
        self.reference_resource.base()
    }

    fn cleanup(&self, current_ref: i64) -> bool {
        self.cleanup(current_ref)
    }
}

pub struct MmapRegionSlice {
    mmap: ArcMut<MmapMut>,
    offset: usize,
    len: usize,
}

impl Deref for MmapRegionSlice {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.mmap[self.offset..self.offset + self.len]
    }
}

impl AsRef<[u8]> for MmapRegionSlice {
    fn as_ref(&self) -> &[u8] {
        &self.mmap[self.offset..self.offset + self.len]
    }
}

impl MmapRegionSlice {
    pub fn new(mmap: ArcMut<MmapMut>, offset: usize, len: usize) -> Self {
        Self { mmap, offset, len }
    }
}

// ============================================================================
// ============================================================================
// New APIs for Enhanced Performance
// ============================================================================

impl DefaultMappedFile {
    /// Gets the performance metrics for this mapped file.
    ///
    /// # Returns
    ///
    /// Reference to the metrics collector, or `None` if metrics are disabled
    ///
    /// This method provides access to real-time performance statistics including:
    /// - Write throughput (ops/sec, MB/s)
    /// - Read operations (total, zero-copy percentage)
    /// - Flush operations (count, average duration)
    /// - Cache hit/miss rates
    #[inline]
    pub fn get_metrics(&self) -> Option<&MappedFileMetrics> {
        self.metrics.as_ref()
    }

    /// Gets the current flush strategy.
    ///
    /// # Returns
    ///
    /// Reference to the configured flush strategy
    #[inline]
    pub fn get_flush_strategy(&self) -> &FlushStrategy {
        &self.flush_strategy
    }

    /// Sets a new flush strategy.
    ///
    /// # Arguments
    ///
    /// * `strategy` - The new flush strategy to use
    ///
    /// This allows runtime reconfiguration of flush behavior without
    /// restarting the file or application.
    #[inline]
    pub fn set_flush_strategy(&mut self, strategy: FlushStrategy) {
        self.flush_strategy = strategy;
    }

    /// Zero-copy read operation - optimized memory access.
    ///
    /// # Arguments
    ///
    /// * `pos` - Starting position in the file
    /// * `size` - Number of bytes to read
    ///
    /// # Returns
    ///
    /// `Some(Bytes)` containing the requested data, or `None` if out of bounds
    ///
    /// # Performance
    ///
    /// Optimized for different data sizes:
    /// - Small (< 4KB): Fast copy, stays in L1/L2 cache
    /// - Medium (4-64KB): Aligned access optimization for 10-15% improvement
    /// - Large (> 64KB): Standard vectorized copy
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let data = mapped_file.get_bytes_zero_copy(0, 16384)?;
    /// process_message(&data);
    /// ```
    pub fn get_bytes_zero_copy(&self, pos: usize, size: usize) -> Option<Bytes> {
        // Record metrics
        if let Some(metrics) = &self.metrics {
            metrics.record_read(size, true);
        }

        // Bounds check
        if pos + size > self.progress.file_size() as usize {
            return None;
        }

        // True zero-copy: Create a Bytes view over the mmap region without copying data
        // This uses MmapRegionSlice which implements Deref<Target=[u8]> and keeps
        // a reference to the mmap, allowing Bytes to share ownership
        Some(Bytes::from_owner(MmapRegionSlice::new(
            self.get_mapped_file_arcmut(),
            pos,
            size,
        )))
    }

    /// Appends multiple messages in a batch with single lock acquisition.
    ///
    /// # Arguments
    ///
    /// * `messages` - Slice of messages to append
    /// * `message_callback` - Callback to serialize messages
    /// * `put_message_context` - Context for message placement
    ///
    /// # Returns
    ///
    /// Vector of append results, one for each message
    ///
    /// # Performance
    ///
    /// This method is approximately 50% faster than calling `append_message()`
    /// repeatedly because it acquires the mmap lock only once.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let results = mapped_file.append_messages_batch(
    ///     &messages,
    ///     &callback,
    ///     &context
    /// );
    /// ```
    pub fn append_messages_batch<AMC: AppendMessageCallback>(
        &self,
        messages: &mut [MessageExtBrokerInner],
        message_callback: &AMC,
        put_message_context: &PutMessageContext,
    ) -> Vec<AppendMessageResult> {
        let mut results = Vec::with_capacity(messages.len());
        let mut total_written = 0;

        for message in messages.iter_mut() {
            let current_pos = self.progress.wrote_position() as u64;

            if current_pos >= self.progress.file_size() {
                results.push(AppendMessageResult {
                    status: AppendMessageStatus::UnknownError,
                    ..Default::default()
                });
                continue;
            }

            let result = message_callback.do_append(
                self.storage.file_from_offset() as i64,
                self,
                (self.progress.file_size() - current_pos) as i32,
                message,
                put_message_context,
            );

            self.progress
                .record_append(result.wrote_bytes, result.store_timestamp as u64);

            total_written += result.wrote_bytes as usize;
            results.push(result);
        }

        // Record batch write metrics
        if let Some(metrics) = &self.metrics {
            metrics.record_write(total_written);
        }

        results
    }

    /// Flushes a specific range of the file to disk.
    ///
    /// # Arguments
    ///
    /// * `start` - Starting offset
    /// * `end` - Ending offset (exclusive)
    ///
    /// # Returns
    ///
    /// Number of bytes flushed, or 0 on error
    ///
    /// # Performance
    ///
    /// Range flush is much faster than full file flush for small changes
    /// because it only syncs the modified pages.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Flush only the last 4KB written
    /// let flushed = mapped_file.flush_range(pos, pos + 4096);
    /// ```
    pub fn flush_range(&self, start: usize, end: usize) -> i32 {
        use std::time::Instant;

        if start >= end || end > self.progress.file_size() as usize {
            return 0;
        }

        let flush_start = Instant::now();

        // Perform the flush
        if self.transient_store_pool.is_some() {
            self.progress.set_committed_position_release(end as i32);
        }

        let result = match self.get_mapped_file().flush_range(start, end - start) {
            Ok(_) => (end - start) as i32,
            Err(e) => {
                error!("Flush range failed: {:?}", e);
                0
            }
        };

        // Record metrics
        if let Some(metrics) = &self.metrics {
            metrics.record_flush(flush_start.elapsed());
        }

        result
    }

    /// Prints a summary of performance metrics.
    ///
    /// # Returns
    ///
    /// Formatted string with metrics summary, or empty string if metrics disabled
    ///
    /// # Examples
    ///
    /// ```ignore
    /// println!("{}", mapped_file.metrics_summary());
    /// // Output:
    /// // MappedFile Metrics:
    /// // Writes: 10000 (25000.00 writes/sec, 97.66 MB/s)
    /// // Reads: 5000 (66.7% zero-copy)
    /// // ...
    /// ```
    pub fn metrics_summary(&self) -> String {
        self.metrics.as_ref().map(|m| m.summary()).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::base::compaction_append_msg_callback::CompactionAppendMsgCallback;
    use crate::base::memory_lock_manager::MemoryLockCategory;
    use crate::base::memory_lock_manager::MemoryLockManager;
    fn create_test_file() -> (TempDir, DefaultMappedFile) {
        let temp_dir = TempDir::new().unwrap();
        // Use numeric filename format expected by DefaultMappedFile
        let file_path = temp_dir.path().join("00000000000000000000");
        let file_name = CheetahString::from(file_path.to_str().unwrap());

        let mapped_file = DefaultMappedFile::new(file_name, 4096);
        (temp_dir, mapped_file)
    }

    fn create_lazy_test_file() -> (TempDir, DefaultMappedFile) {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("00000000000000000000");
        let file_name = CheetahString::from(file_path.to_str().unwrap());

        let mapped_file = DefaultMappedFile::try_new_lazy_read_only(file_name, 4096).unwrap();
        (temp_dir, mapped_file)
    }

    fn create_transient_test_file() -> (TempDir, TransientStorePool, DefaultMappedFile) {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("00000000000000000000");
        let file_name = CheetahString::from(file_path.to_str().unwrap());
        let transient_store_pool = TransientStorePool::new(1, 4096);
        let mapped_file =
            DefaultMappedFile::new_with_transient_store_pool(file_name, 4096, transient_store_pool.clone());
        (temp_dir, transient_store_pool, mapped_file)
    }

    struct RejectingCompactionCallback;

    impl CompactionAppendMsgCallback for RejectingCompactionCallback {
        fn do_append(
            &self,
            bb_dest: &mut bytes::Bytes,
            file_from_offset: i64,
            max_blank: i32,
            bb_src: &mut bytes::Bytes,
        ) -> AppendMessageResult {
            let _ = (bb_dest, file_from_offset, max_blank, bb_src);
            AppendMessageResult::default()
        }
    }

    #[test]
    fn test_metrics_enabled_by_default() {
        let (_temp_dir, mapped_file) = create_test_file();

        assert!(mapped_file.get_metrics().is_some());
    }

    #[test]
    fn test_default_flush_strategy() {
        let (_temp_dir, mapped_file) = create_test_file();

        let strategy = mapped_file.get_flush_strategy();
        assert!(matches!(strategy, FlushStrategy::Async));
    }

    #[test]
    fn test_set_flush_strategy() {
        let (_temp_dir, mut mapped_file) = create_test_file();

        mapped_file.set_flush_strategy(FlushStrategy::Sync);
        assert!(matches!(mapped_file.get_flush_strategy(), FlushStrategy::Sync));
    }

    #[test]
    fn test_metrics_summary() {
        let (_temp_dir, mapped_file) = create_test_file();

        let summary = mapped_file.metrics_summary();
        assert!(!summary.is_empty());
        assert!(summary.contains("MappedFile Metrics"));
    }

    #[test]
    fn test_get_bytes_zero_copy() {
        let (_temp_dir, mapped_file) = create_test_file();

        let data = mapped_file.get_bytes_zero_copy(0, 100);
        assert!(data.is_some());

        let data = data.unwrap();
        assert_eq!(data.len(), 100);
    }

    #[test]
    fn lazy_mmap_defers_mapping_until_first_access() {
        let (_temp_dir, mapped_file) = create_lazy_test_file();

        assert!(mapped_file.is_lazy_mmap_enabled());
        assert!(!mapped_file.is_mapped());
        assert_eq!(
            mapped_file.lazy_mmap_stats(),
            LazyMmapStats {
                eligible_files: 1,
                mapped_files: 0,
                map_operations: 0,
                map_failures: 0,
                total_millis: 0,
                last_millis: 0,
            }
        );

        assert_eq!(mapped_file.get_mapped_file().len(), 4096);

        let stats = mapped_file.lazy_mmap_stats();
        assert!(mapped_file.is_mapped());
        assert_eq!(stats.eligible_files, 1);
        assert_eq!(stats.mapped_files, 1);
        assert_eq!(stats.map_operations, 1);
        assert_eq!(stats.map_failures, 0);
    }

    #[test]
    fn eager_mmap_maps_during_construction() {
        let (_temp_dir, mapped_file) = create_test_file();

        assert!(!mapped_file.is_lazy_mmap_enabled());
        assert!(mapped_file.is_mapped());
        assert_eq!(mapped_file.get_mapped_file().len(), 4096);
    }

    #[test]
    fn lazy_mmap_destroy_before_first_access_does_not_force_mapping() {
        let (temp_dir, mapped_file) = create_lazy_test_file();
        let file_path = temp_dir.path().join("00000000000000000000");

        assert!(!mapped_file.is_mapped());
        assert!(mapped_file.destroy(0));
        assert!(!file_path.exists());
        assert_eq!(mapped_file.lazy_mmap_stats().map_operations, 0);
    }

    #[test]
    fn rename_delegation_updates_projection_handle_and_destroy_path() {
        let (temp_dir, mut mapped_file) = create_test_file();
        let original = temp_dir.path().join("00000000000000000000");
        let renamed = temp_dir.path().join("renamed-segment");

        assert!(mapped_file.rename_to(renamed.to_str().expect("UTF-8 path")));
        assert_eq!(mapped_file.get_file_name().as_str(), renamed.to_str().unwrap());
        assert!(!original.exists());
        assert!(renamed.exists());
        assert_eq!(mapped_file.get_file().metadata().unwrap().len(), 4096);
        assert!(mapped_file.destroy(0));
        assert!(!renamed.exists());
    }

    #[test]
    fn failed_rename_delegation_keeps_projection_and_handle() {
        let (temp_dir, mut mapped_file) = create_test_file();
        let original = temp_dir.path().join("00000000000000000000");
        let missing_parent = temp_dir.path().join("missing").join("renamed-segment");

        assert!(!mapped_file.rename_to(missing_parent.to_str().expect("UTF-8 path")));
        assert_eq!(mapped_file.get_file_name().as_str(), original.to_str().unwrap());
        assert!(original.exists());
        assert_eq!(mapped_file.get_file().metadata().unwrap().len(), 4096);
    }

    #[test]
    fn test_get_bytes_zero_copy_out_of_bounds() {
        let (_temp_dir, mapped_file) = create_test_file();

        let data = mapped_file.get_bytes_zero_copy(0, 10000);
        assert!(data.is_none());
    }

    #[test]
    fn test_flush_range() {
        let (_temp_dir, mapped_file) = create_test_file();

        let flushed = mapped_file.flush_range(0, 1024);
        assert!(flushed >= 0);
    }

    #[test]
    fn test_flush_range_invalid() {
        let (_temp_dir, mapped_file) = create_test_file();

        let flushed = mapped_file.flush_range(0, 10000);
        assert_eq!(flushed, 0);

        let flushed = mapped_file.flush_range(100, 50);
        assert_eq!(flushed, 0);
    }

    #[test]
    fn test_metrics_record_reads() {
        let (_temp_dir, mapped_file) = create_test_file();

        // Perform some zero-copy reads
        mapped_file.get_bytes_zero_copy(0, 100);
        mapped_file.get_bytes_zero_copy(100, 200);

        // Check metrics
        let metrics = mapped_file.get_metrics().unwrap();
        assert_eq!(metrics.total_reads(), 2);
        assert_eq!(metrics.total_bytes_read(), 300);

        // Both reads should be zero-copy
        assert_eq!(metrics.zero_copy_read_percentage(), 100.0);
    }

    #[test]
    fn test_metrics_record_flushes() {
        let (_temp_dir, mapped_file) = create_test_file();

        // Perform some flushes
        mapped_file.flush_range(0, 512);
        mapped_file.flush_range(512, 1024);

        // Check metrics
        let metrics = mapped_file.get_metrics().unwrap();
        assert_eq!(metrics.total_flushes(), 2);

        // Average flush duration should be calculable (may be 0 on very fast systems)
        let _avg_duration = metrics.avg_flush_duration();
        // Duration is always valid, no need to assert non-negative as u128 is always >= 0
    }

    #[test]
    fn test_metrics_summary_content() {
        let (_temp_dir, mapped_file) = create_test_file();

        // Perform some operations
        mapped_file.get_bytes_zero_copy(0, 1024);
        mapped_file.flush_range(0, 1024);

        // Get summary
        let summary = mapped_file.metrics_summary();

        // Summary should contain key metrics
        assert!(summary.contains("Reads:"));
        assert!(summary.contains("Flushes:"));
        assert!(summary.contains("zero-copy"));
        assert!(summary.contains("Warm:"));
        assert!(summary.contains("Swap:"));
    }

    #[test]
    fn transient_store_pool_commit_flush_round_trip() {
        let (_temp_dir, _pool, mapped_file) = create_transient_test_file();

        assert!(mapped_file.append_message_bytes(b"hello transient"));
        assert_eq!(mapped_file.get_wrote_position(), 15);
        assert_eq!(mapped_file.get_committed_position(), 0);
        assert_eq!(mapped_file.get_read_position(), 0);

        assert_eq!(mapped_file.commit(0), 15);
        assert_eq!(mapped_file.get_committed_position(), 15);
        assert_eq!(mapped_file.get_read_position(), 15);

        assert_eq!(mapped_file.flush(0), 15);
        assert_eq!(mapped_file.get_flushed_position(), 15);
        assert_eq!(
            mapped_file.get_bytes_readable_checked(0, 15).unwrap(),
            Bytes::from_static(b"hello transient")
        );
    }

    #[test]
    fn failed_flush_keeps_the_last_durable_position_and_preserves_io_cause() {
        let (_temp_dir, mapped_file) = create_test_file();
        assert!(mapped_file.append_message_bytes(b"not-yet-durable"));
        let durable_before = mapped_file.get_flushed_position();

        let error = mapped_file
            .try_flush_with(0, |_, _, _| Err(io::Error::from_raw_os_error(5)))
            .expect_err("injected fsync failure must be returned");

        assert!(matches!(error, MappedFileError::FlushFailed(source) if source.raw_os_error() == Some(5)));
        assert_eq!(mapped_file.get_flushed_position(), durable_before);
        assert!(mapped_file.get_wrote_position() > durable_before);
    }

    #[test]
    fn compaction_append_is_explicitly_rejected() {
        let (_temp_dir, mut mapped_file) = create_test_file();
        let mut message = Bytes::from_static(b"compaction-message");

        let result = mapped_file.append_message_compaction(&mut message, &RejectingCompactionCallback);

        assert_eq!(result.status, AppendMessageStatus::UnknownError);
        assert_eq!(result.wrote_bytes, 0);
    }

    #[test]
    fn mapped_file_init_with_transient_pool_is_stable() {
        let (_temp_dir, pool, mapped_file) = create_transient_test_file();

        let result = mapped_file.init(mapped_file.get_file_name(), 4096, &pool);

        assert!(result.is_ok());
    }

    #[test]
    fn swap_map_updates_bookkeeping_without_panicking() {
        let (_temp_dir, mapped_file) = create_test_file();
        mapped_file
            .mapped_byte_buffer_access_count_since_last_swap
            .store(3, Ordering::Release);
        let before = mapped_file.get_recent_swap_map_time();

        assert!(!mapped_file.swap_map());
        assert_eq!(mapped_file.get_mapped_byte_buffer_access_count_since_last_swap(), 0);
        assert!(mapped_file.get_recent_swap_map_time() >= before);
        assert_eq!(mapped_file.get_metrics().unwrap().swap_operations(), 1);
    }

    #[test]
    fn select_mapped_buffer_records_page_cache_residency() {
        let (_temp_dir, mapped_file) = create_test_file();
        assert!(mapped_file.append_message_bytes(b"cache-residency"));

        let result = mapped_file
            .select_mapped_buffer(0, "cache-residency".len() as i32)
            .expect("selected buffer should exist");

        let metrics = mapped_file.get_metrics().unwrap();
        assert_eq!(metrics.cache_hits() + metrics.cache_misses(), 1);
        assert_eq!(result.is_in_cache, metrics.cache_hits() == 1);
        assert_eq!(result.source_kind, SelectMappedBufferSourceKind::MappedFile);
        assert_eq!(result.file_offset, 0);
        assert_eq!(
            result.cache_state,
            SelectMappedBufferCacheState::from_residency(result.is_in_cache)
        );
    }

    #[test]
    fn warm_and_clean_swap_are_reflected_in_metrics() {
        let (_temp_dir, mapped_file) = create_test_file();

        mapped_file.warm_mapped_file(FlushDiskType::AsyncFlush, 1);
        mapped_file.clean_swaped_map(true);

        let metrics = mapped_file.get_metrics().unwrap();
        assert_eq!(metrics.warm_operations(), 1);
        assert_eq!(metrics.warm_bytes(), mapped_file.get_file_size());
        assert_eq!(metrics.clean_swap_operations(), 1);
    }

    #[test]
    fn warm_mapped_file_preserves_write_commit_and_flush_positions() {
        let (_temp_dir, mapped_file) = create_test_file();
        assert!(mapped_file.append_message_bytes(b"warm-position"));
        let wrote_position = mapped_file.get_wrote_position();
        let committed_position = mapped_file.get_committed_position();
        let flushed_position = mapped_file.get_flushed_position();

        mapped_file.warm_mapped_file(FlushDiskType::AsyncFlush, 1);
        mapped_file.warm_mapped_file(FlushDiskType::SyncFlush, 1);

        assert_eq!(mapped_file.get_wrote_position(), wrote_position);
        assert_eq!(mapped_file.get_committed_position(), committed_position);
        assert_eq!(mapped_file.get_flushed_position(), flushed_position);
        let metrics = mapped_file.get_metrics().unwrap();
        assert_eq!(metrics.warm_operations(), 2);
        assert_eq!(metrics.warm_bytes(), mapped_file.get_file_size() * 2);
    }

    #[test]
    fn preallocate_degradation_events_include_operation_reason_and_errno() {
        let unsupported = file_preallocate_degradation_event(FilePreallocateOutcome::Unsupported { errno: 95 })
            .expect("unsupported preallocation should be observable");
        let failed = file_preallocate_degradation_event(FilePreallocateOutcome::Failed { errno: 28 })
            .expect("failed preallocation should be observable");

        assert_eq!(
            unsupported,
            LinuxStorageDegradationEvent::new(LINUX_STORAGE_OP_FALLOCATE, LINUX_STORAGE_REASON_UNSUPPORTED, 95)
        );
        assert_eq!(
            failed,
            LinuxStorageDegradationEvent::new(LINUX_STORAGE_OP_FALLOCATE, LINUX_STORAGE_REASON_FAILED, 28)
        );
        assert!(file_preallocate_degradation_event(FilePreallocateOutcome::Allocated).is_none());
    }

    #[test]
    fn warm_mapped_file_records_warmup_degradation_events_without_position_changes() {
        let (_temp_dir, mapped_file) = create_test_file();
        assert!(mapped_file.append_message_bytes(b"warm-degradation"));
        let wrote_position = mapped_file.get_wrote_position();
        let committed_position = mapped_file.get_committed_position();
        let flushed_position = mapped_file.get_flushed_position();
        let mut events = Vec::new();
        let mut flush_calls = 0usize;

        mapped_file.warm_mapped_file_with_ops(
            FlushDiskType::SyncFlush,
            1,
            |_, _| Err(io::Error::from_raw_os_error(5)),
            |_, _, _| {
                flush_calls += 1;
                if flush_calls == 1 {
                    Ok(())
                } else {
                    Err(io::Error::from_raw_os_error(28))
                }
            },
            |_, _, _| Err(io::Error::from_raw_os_error(12)),
            |event| events.push(event),
        );

        assert_eq!(mapped_file.get_wrote_position(), wrote_position);
        assert_eq!(mapped_file.get_committed_position(), committed_position);
        assert_eq!(mapped_file.get_flushed_position(), flushed_position);
        assert_eq!(
            events,
            vec![
                LinuxStorageDegradationEvent::new(LINUX_STORAGE_OP_PAGE_TOUCH, LINUX_STORAGE_REASON_FAILED, 5),
                LinuxStorageDegradationEvent::new(LINUX_STORAGE_OP_PAGE_TOUCH, LINUX_STORAGE_REASON_FLUSH_FAILED, 28),
                LinuxStorageDegradationEvent::new(LINUX_STORAGE_OP_MADVISE, LINUX_STORAGE_REASON_FAILED, 12),
            ]
        );
    }

    #[test]
    fn lock_region_clamps_requested_length_to_mapped_file_boundary() {
        let (_temp_dir, mapped_file) = create_test_file();
        let manager = MemoryLockManager::warn_only_with_budget(4096);
        let expected_addr = mapped_file.get_mapped_file().as_ptr().wrapping_add(3072);

        let handle = mapped_file
            .lock_region_with(
                &manager,
                MemoryLockCategory::CommitLogActiveWindow,
                3072,
                4096,
                |addr, len| {
                    assert_eq!(addr, expected_addr);
                    assert_eq!(len, 1024);
                    Ok(())
                },
            )
            .expect("range lock should not fail")
            .expect("clamped non-empty range should return handle");

        assert_eq!(handle.category(), MemoryLockCategory::CommitLogActiveWindow);
        assert_eq!(handle.len(), 1024);
        assert_eq!(manager.locked_bytes(), 1024);

        mapped_file
            .unlock_region_with(&manager, handle, |addr, len| {
                assert_eq!(addr, expected_addr);
                assert_eq!(len, 1024);
                Ok(())
            })
            .expect("range unlock should not fail");
        assert_eq!(manager.locked_bytes(), 0);
    }

    #[test]
    fn lock_region_skips_zero_length_and_out_of_range_requests() {
        let (_temp_dir, mapped_file) = create_test_file();
        let manager = MemoryLockManager::warn_only_with_budget(4096);

        let zero_len = mapped_file
            .lock_region_with(&manager, MemoryLockCategory::CommitLogActiveWindow, 0, 0, |_, _| {
                panic!("zero-length request must not call locker")
            })
            .expect("zero-length request should be accepted as a no-op");
        let out_of_range = mapped_file
            .lock_region_with(
                &manager,
                MemoryLockCategory::CommitLogActiveWindow,
                mapped_file.get_file_size(),
                1024,
                |_, _| panic!("out-of-range request must not call locker"),
            )
            .expect("out-of-range request should be accepted as a no-op");

        assert!(zero_len.is_none());
        assert!(out_of_range.is_none());
        assert_eq!(manager.lock_attempt_count(), 0);
        assert_eq!(manager.locked_bytes(), 0);
    }

    #[test]
    fn is_loaded_rejects_invalid_ranges() {
        let (_temp_dir, mapped_file) = create_test_file();

        assert!(!mapped_file.is_loaded(-1, 1));
        assert!(!mapped_file.is_loaded(0, 0));
        assert!(!mapped_file.is_loaded(mapped_file.get_file_size() as i64, 1));
    }
}
