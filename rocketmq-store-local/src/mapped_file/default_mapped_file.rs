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
use std::path::Path;
use std::ptr;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQResult;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use super::FlushStrategy;
use super::MappedFile;
use super::MappedFileError;
use super::MappedFileMetrics;
use super::MappedFileRawCore;
use super::MappedFileResult;
use super::MappedMemory;
use super::NativeMappedMemory;
use super::SelectMappedBufferCacheState;
use super::SelectMappedBufferResult;
use super::SelectMappedBufferSourceKind;
use crate::base::memory_lock_manager::MemoryLockCategory;
use crate::base::memory_lock_manager::MemoryLockHandle;
use crate::base::memory_lock_manager::MemoryLockManager;
use crate::base::transient_store_pool::TransientStorePool;
use crate::config::FlushDiskType;
use crate::mapped_file::file::FilePreallocateOutcome;
use crate::mapped_file::file::MappedFileStorage;
use crate::mapped_file::kernel::visit_mapped_file_warmup_schedule;
use crate::mapped_file::kernel::MappedFileWarmupOperation;
use crate::mapped_file::kernel::ReferenceResource;
use crate::mapped_file::kernel::ReferenceResourceBase;
use crate::mapped_file::kernel::ReferenceResourceCounter;
pub use crate::mapped_file::kernel::OS_PAGE_SIZE;
pub use crate::mapped_file::mapping::LazyMmapStats;
use crate::mapped_file::mapping::MappedFileMapping;
use crate::utils::ffi::get_page_size;
use crate::utils::ffi::madvise;
#[cfg(target_os = "linux")]
use crate::utils::ffi::mincore;
use crate::utils::ffi::MADV_WILLNEED;

static TOTAL_MAPPED_VIRTUAL_MEMORY: AtomicI64 = AtomicI64::new(0);
static TOTAL_MAPPED_FILES: AtomicI32 = AtomicI32::new(0);

fn invalid_input_error(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or_default()
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

pub struct DefaultMappedFile<M: MappedMemory = NativeMappedMemory> {
    reference_resource: ReferenceResourceCounter,
    storage: MappedFileStorage,
    mapping: MappedFileMapping<M>,
    transient_store_pool: Option<TransientStorePool>,
    file_name: CheetahString,
    raw_core: MappedFileRawCore,
    first_create_in_queue: bool,
    swap_map_time: AtomicU64,
    mapped_byte_buffer_access_count_since_last_swap: AtomicI64,
    metrics: Option<MappedFileMetrics>,
    flush_strategy: FlushStrategy,
}

impl<M: MappedMemory> AsRef<DefaultMappedFile<M>> for DefaultMappedFile<M> {
    #[inline]
    fn as_ref(&self) -> &DefaultMappedFile<M> {
        self
    }
}

impl<M: MappedMemory> AsMut<DefaultMappedFile<M>> for DefaultMappedFile<M> {
    #[inline]
    fn as_mut(&mut self) -> &mut DefaultMappedFile<M> {
        self
    }
}

impl<M: MappedMemory> PartialEq for DefaultMappedFile<M> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        ptr::eq(self as *const Self, other as *const Self)
    }
}

impl<M: MappedMemory> Default for DefaultMappedFile<M> {
    #[inline]
    fn default() -> Self {
        Self::new(CheetahString::new(), 0)
    }
}

impl<M: MappedMemory> DefaultMappedFile<M> {
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
            .ok_or_else(|| invalid_input_error(format!("file path is invalid: {file_name}")))?;
        std::fs::create_dir_all(dir)?;

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

        let mapping = if lazy_mmap_enabled {
            MappedFileMapping::new_lazy()
        } else {
            MappedFileMapping::new_eager(M::map_mut(storage.file())?)
        };

        Ok(Self {
            reference_resource: ReferenceResourceCounter::new(),
            storage,
            mapping,
            file_name,
            raw_core: MappedFileRawCore::new(file_size),
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
        self.mapping.is_lazy_enabled()
    }

    #[inline]
    pub fn is_mapped(&self) -> bool {
        self.mapping.is_mapped()
    }

    pub fn lazy_mmap_stats(&self) -> LazyMmapStats {
        self.mapping.stats()
    }

    fn try_get_mapped_file_ref(&self) -> io::Result<&M> {
        self.mapping.get_or_try_init(|| M::map_mut(self.storage.file()))
    }

    fn try_get_mapped_memory(&self) -> io::Result<M> {
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
        crate::mapped_file::file::parse_file_from_offset(file_name)
    }

    #[inline]
    pub fn try_parse_file_from_offset(file_name: &Path) -> io::Result<u64> {
        crate::mapped_file::file::try_parse_file_from_offset(file_name)
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
        self.raw_core.is_valid_cache_range(position, size)
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
        let flushed_position = self.raw_core.flushed_position();
        let result = flush(self, flushed_position, value);
        MappedFile::release(self);
        result.map_err(MappedFileError::FlushFailed)?;

        self.raw_core.record_flush_success(value);
        if let Some(metrics) = &self.metrics {
            metrics.record_flush(flush_started.elapsed());
        }
        Ok(value)
    }
}

#[allow(unused_variables)]
impl<M: MappedMemory> MappedFile for DefaultMappedFile<M> {
    type SelectResult = SelectMappedBufferResult<M>;

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
        self.raw_core.file_size()
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.raw_core.is_full()
    }

    #[inline]
    fn is_available(&self) -> bool {
        self.reference_resource.is_available()
    }

    #[inline]
    fn get_bytes(&self, pos: usize, size: usize) -> Option<bytes::Bytes> {
        self.raw_core
            .copied_read_slice(|| self.get_mapped_file(), pos, size)
            .map(Bytes::copy_from_slice)
    }

    #[inline]
    fn get_bytes_readable_checked(&self, pos: usize, size: usize) -> Option<bytes::Bytes> {
        self.raw_core
            .copied_readable_slice(|| self.get_mapped_file(), pos, size, self.get_read_position())
            .map(Bytes::copy_from_slice)
    }

    fn append_message_offset_length(&self, data: &[u8], offset: usize, length: usize) -> bool {
        self.raw_core.append_bytes_with_position_update(
            || {
                // SAFETY: mapped-file mutation is serialized by the owning Store operation.
                let (ptr, len) = unsafe { self.mapped_file_mut_parts() };
                // SAFETY: the live mapping remains exclusively owned for this append callback.
                unsafe { Self::mutable_slice_from_parts(ptr, len) }
            },
            data,
            offset,
            length,
        )
    }

    fn append_message_no_position_update(&self, data: &[u8], offset: usize, length: usize) -> bool {
        self.raw_core.append_bytes_without_position_update(
            || {
                // SAFETY: mapped-file mutation is serialized by the owning Store operation.
                let (ptr, len) = unsafe { self.mapped_file_mut_parts() };
                // SAFETY: the live mapping remains exclusively owned for this append callback.
                unsafe { Self::mutable_slice_from_parts(ptr, len) }
            },
            data,
            offset,
            length,
        )
    }

    /// **Zero-Copy Implementation**
    ///
    /// Returns a direct mutable buffer for zero-copy message encoding.
    /// Eliminates the intermediate pre_encode_buffer, reducing CPU usage.
    fn get_direct_write_buffer(&self, required_space: usize) -> Option<(&mut [u8], usize)> {
        self.raw_core.direct_write_range(
            || {
                // SAFETY: the caller owns the append operation until `commit_direct_write`.
                let (ptr, len) = unsafe { self.mapped_file_mut_parts() };
                // SAFETY: the direct-write contract keeps the selected range exclusive until it
                // is committed.
                unsafe { Self::mutable_slice_from_parts(ptr, len) }
            },
            required_space,
        )
    }

    /// **Phase 3 Zero-Copy Implementation**
    ///
    /// Commits a direct write by updating the write position atomically.
    fn commit_direct_write(&self, bytes_written: usize) -> bool {
        let committed = self.raw_core.commit_direct_write(bytes_written);
        if committed {
            if let Some(metrics) = &self.metrics {
                metrics.record_write(bytes_written);
            }
        }
        committed
    }

    fn write_bytes_segment(&self, data: &[u8], start: usize, offset: usize, length: usize) -> bool {
        self.raw_core.write_bytes_segment(
            || {
                // SAFETY: mapped-file mutation is serialized by the owning Store operation.
                let (ptr, len) = unsafe { self.mapped_file_mut_parts() };
                // SAFETY: the live mapping remains exclusively owned for this segment write.
                unsafe { Self::mutable_slice_from_parts(ptr, len) }
            },
            data,
            start,
            offset,
            length,
        )
    }

    fn put_slice(&self, data: &[u8], index: usize) -> bool {
        self.raw_core.put_slice(
            || {
                // SAFETY: mapped-file mutation is serialized by the owning Store operation.
                let (ptr, len) = unsafe { self.mapped_file_mut_parts() };
                // SAFETY: the live mapping remains exclusively owned for this indexed write.
                unsafe { Self::mutable_slice_from_parts(ptr, len) }
            },
            data,
            index,
        )
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
            if flush_size > 0 && flush_size < (mapped_file.raw_core.file_size() as i32) / 2 {
                mapped_file
                    .try_get_mapped_file_ref()?
                    .flush_range(flushed_position as usize, flush_size as usize)
            } else {
                mapped_file.try_get_mapped_file_ref()?.flush()
            }
        })
    }

    #[inline]
    fn commit(&self, commit_least_pages: i32) -> i32 {
        self.raw_core.commit(commit_least_pages)
    }

    fn select_mapped_buffer(&self, pos: i32, size: i32) -> Option<SelectMappedBufferResult<M>> {
        let read_position = self.get_read_position();
        if self.raw_core.is_readable_range(pos, size, read_position) {
            if MappedFile::hold(self) {
                self.mapped_byte_buffer_access_count_since_last_swap
                    .fetch_add(1, Ordering::AcqRel);

                let is_in_cache = self.record_cache_residency(pos as i64, size as usize);
                let bytes = if size >= 8192 {
                    // Try zero-copy read first
                    self.get_bytes_zero_copy(pos as usize, size as usize)
                        .unwrap_or_else(|| {
                            // Fallback to standard method
                            Bytes::from_owner(self.get_mapped_memory().region(pos as usize, size as usize))
                        })
                } else {
                    // Small reads: use standard method
                    Bytes::from_owner(self.get_mapped_memory().region(pos as usize, size as usize))
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
            metrics.record_read(self.raw_core.file_size() as usize, false);
        }

        self.get_mapped_file()
    }

    #[inline]
    fn slice_byte_buffer(&self) -> &[u8] {
        self.mapped_byte_buffer_access_count_since_last_swap
            .fetch_add(1, Ordering::AcqRel);

        if let Some(metrics) = &self.metrics {
            metrics.record_read(self.raw_core.file_size() as usize, false);
        }

        self.get_mapped_file()
    }

    #[inline]
    fn get_store_timestamp(&self) -> u64 {
        self.raw_core.store_timestamp()
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
        if self.raw_core.is_readable_byte_range(pos, size, read_position) {
            if MappedFile::hold(self) {
                let Some(slice) = self
                    .raw_core
                    .readable_slice(|| self.get_mapped_file(), pos, size, read_position)
                else {
                    MappedFile::release(self);
                    return None;
                };
                let buffer = BytesMut::from(slice);
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
        self.raw_core.flushed_position()
    }

    #[inline]
    fn set_flushed_position(&self, flushed_position: i32) {
        self.raw_core.set_flushed_position(flushed_position)
    }

    #[inline]
    fn get_wrote_position(&self) -> i32 {
        self.raw_core.wrote_position()
    }

    #[inline]
    fn set_wrote_position(&self, wrote_position: i32) {
        self.raw_core.set_wrote_position(wrote_position)
    }

    #[inline]
    fn record_append(&self, wrote_bytes: i32, store_timestamp: u64) {
        self.raw_core.record_append(wrote_bytes, store_timestamp);
        if let Some(metrics) = &self.metrics {
            metrics.record_write(wrote_bytes as usize);
        }
    }

    /// Return The max position which have valid data
    ///
    /// # Returns
    ///
    /// An `i32` representing the current read position.
    #[inline]
    fn get_read_position(&self) -> i32 {
        if self.transient_store_pool.is_some() {
            self.raw_core.transient_read_position()
        } else {
            self.raw_core.normal_read_position()
        }
    }

    #[inline]
    fn set_committed_position(&self, committed_position: i32) {
        self.raw_core.set_committed_position(committed_position)
    }

    #[inline]
    fn get_committed_position(&self) -> i32 {
        self.raw_core.committed_position()
    }

    #[inline]
    fn mlock(&self) {
        if let Err(error) =
            crate::utils::ffi::mlock(self.get_mapped_file().as_ptr(), self.raw_core.file_size() as usize)
        {
            warn!(file_name = %self.file_name, error = %error, "failed to mlock mapped file");
        }
    }

    #[inline]
    fn munlock(&self) {
        if let Err(error) =
            crate::utils::ffi::munlock(self.get_mapped_file().as_ptr(), self.raw_core.file_size() as usize)
        {
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
        self.raw_core.last_flush_time()
    }

    #[inline]
    #[cfg(target_os = "linux")]
    fn is_loaded(&self, position: i64, size: usize) -> bool {
        if !self.is_valid_cache_range(position, size) {
            return false;
        }

        let page_size = get_page_size();
        let base_addr = self.get_mapped_file().as_ptr() as usize;
        let Some(plan) =
            crate::mapped_file::kernel::plan_mapped_file_cache_residency(base_addr, position, size, page_size)
        else {
            return false;
        };

        let mut residency = vec![0u8; plan.page_count];
        let result = mincore(
            plan.aligned_start as *const u8,
            plan.checked_len,
            residency.as_mut_ptr(),
        );
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

    fn select_mapped_buffer_with_position(&self, pos: i32) -> Option<SelectMappedBufferResult<M>> {
        let read_position = self.get_read_position();
        let size = self.raw_core.readable_tail_size(pos, read_position)?;
        if MappedFile::hold(self) {
            self.mapped_byte_buffer_access_count_since_last_swap
                .fetch_add(1, Ordering::AcqRel);
            let is_in_cache = self.record_cache_residency(pos as i64, size as usize);

            let bytes = if size >= 8192 {
                self.get_bytes_zero_copy(pos as usize, size as usize)
                    .unwrap_or_else(|| Bytes::from_owner(self.get_mapped_memory().region(pos as usize, size as usize)))
            } else {
                Bytes::from_owner(self.get_mapped_memory().region(pos as usize, size as usize))
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

        if file_size as u64 != self.raw_core.file_size() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "mapped file init size mismatch: expected {}, got {}",
                    self.raw_core.file_size(),
                    file_size
                ),
            ));
        }

        Ok(())
    }

    fn get_slice(&self, pos: usize, size: usize) -> Option<&[u8]> {
        let slice = self.raw_core.raw_slice(|| self.get_mapped_file(), pos, size)?;
        if let Some(metrics) = &self.metrics {
            metrics.record_read(size, false);
        }
        Some(slice)
    }
}

#[allow(unused_variables)]
impl<M: MappedMemory> DefaultMappedFile<M> {
    #[inline]
    /// Returns the pointer and length of the writable mapped bytes.
    ///
    /// # Safety
    ///
    /// The caller must exclusively own mapped-file mutation for every mutable slice or reference
    /// derived from the returned parts. No read, write, flush, selection, or region operation may
    /// overlap that access.
    pub unsafe fn mapped_file_mut_parts(&self) -> (*mut u8, usize) {
        let mapped_memory = self
            .try_get_mapped_file_ref()
            .expect("mapped file initialization failed");
        (mapped_memory.as_mut_ptr(), mapped_memory.as_slice().len())
    }

    unsafe fn mutable_slice_from_parts<'a>(ptr: *mut u8, len: usize) -> &'a mut [u8] {
        // SAFETY: the caller guarantees that the parts describe a live mapping and that mutation
        // remains exclusive for the returned borrow.
        unsafe { std::slice::from_raw_parts_mut(ptr, len) }
    }

    #[inline]
    pub fn get_mapped_file(&self) -> &[u8] {
        self.try_get_mapped_file_ref()
            .expect("mapped file initialization failed")
            .as_slice()
    }

    /// Returns a cloned handle to the mapping backend for compatibility adapters.
    #[inline]
    pub fn get_mapped_memory(&self) -> M {
        self.try_get_mapped_memory().expect("mapped file initialization failed")
    }

    fn touch_mapped_page(mapped_ptr: *mut u8, offset: usize) -> io::Result<()> {
        // SAFETY: the scheduled offset is bounded by the mapped file size and `mapped_ptr` points
        // to the live writable mapping for this operation.
        unsafe {
            let page_ptr = mapped_ptr.add(offset);
            let value = std::ptr::read_volatile(page_ptr);
            std::ptr::write_volatile(page_ptr, value);
        }
        Ok(())
    }

    fn flush_mapped_file_range(mapped_file: &M, offset: usize, len: usize) -> io::Result<()> {
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
        F: FnMut(&M, usize, usize) -> io::Result<()>,
        A: FnMut(*const u8, usize, i32) -> io::Result<()>,
        R: FnMut(LinuxStorageDegradationEvent),
    {
        let file_size = self.raw_core.file_size() as usize;
        if file_size == 0 {
            return;
        }

        let warmup_started = Instant::now();
        {
            let mapped_file = self
                .try_get_mapped_file_ref()
                .expect("mapped file initialization failed");
            let mapped_ptr = mapped_file.as_mut_ptr();
            visit_mapped_file_warmup_schedule(
                file_size,
                get_page_size(),
                pages,
                flush_disk_type == FlushDiskType::SyncFlush,
                |operation| match operation {
                    MappedFileWarmupOperation::Touch { offset } => {
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
                    }
                    MappedFileWarmupOperation::Flush {
                        offset,
                        len,
                        final_flush,
                    } => {
                        let end = offset + len;
                        if let Err(error) = flush_range(mapped_file, offset, len) {
                            record_degradation(LinuxStorageDegradationEvent::new(
                                LINUX_STORAGE_OP_PAGE_TOUCH,
                                LINUX_STORAGE_REASON_FLUSH_FAILED,
                                errno_from_io_error(&error),
                            ));
                            if final_flush {
                                warn!(
                                    "Failed to flush final warmed mapped file range {}-{} for {}: {:?}",
                                    offset, end, self.file_name, error
                                );
                            } else {
                                warn!(
                                    "Failed to flush warmed mapped file range {}-{} for {}: {:?}",
                                    offset, end, self.file_name, error
                                );
                            }
                        } else {
                            self.raw_core.record_flush_time();
                        }
                    }
                },
            );
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
        #[cfg(feature = "observability")]
        rocketmq_observability::metrics::store::record_linux_page_cache_warmup_millis(warmup_millis);
        #[cfg(not(feature = "observability"))]
        let _ = warmup_millis;
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

    pub fn lock_region_with<F>(
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

    pub fn unlock_region_with<F>(
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
        let (offset, len) = self.raw_core.lock_region_range(offset, requested_len)?;
        Some((self.get_mapped_file().as_ptr().wrapping_add(offset), len))
    }

    /// Gets the start timestamp of the mapped file.
    ///
    /// # Returns
    ///
    /// The start timestamp as i64. Returns -1 if not set.
    #[inline]
    pub fn get_start_timestamp(&self) -> i64 {
        self.raw_core.start_timestamp()
    }

    /// Sets the start timestamp of the mapped file.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The start timestamp to set
    #[inline]
    pub fn set_start_timestamp(&self, timestamp: i64) {
        self.raw_core.set_start_timestamp(timestamp);
    }

    /// Gets the stop timestamp of the mapped file.
    ///
    /// # Returns
    ///
    /// The stop timestamp as i64. Returns -1 if not set.
    #[inline]
    pub fn get_stop_timestamp(&self) -> i64 {
        self.raw_core.stop_timestamp()
    }

    /// Sets the stop timestamp of the mapped file.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The stop timestamp to set
    #[inline]
    pub fn set_stop_timestamp(&self, timestamp: i64) {
        self.raw_core.set_stop_timestamp(timestamp);
    }

    #[inline]
    fn is_able_to_flush(&self, flush_least_pages: i32) -> bool {
        self.raw_core
            .is_able_to_flush(self.get_read_position(), flush_least_pages)
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
        TOTAL_MAPPED_VIRTUAL_MEMORY.fetch_sub(self.raw_core.file_size() as i64, Ordering::Relaxed);
        TOTAL_MAPPED_FILES.fetch_sub(1, Ordering::Relaxed);
        info!("unmap file[REF:{}] {} OK", current_ref, self.file_name);
        true
    }
}

impl<M: MappedMemory> ReferenceResource for DefaultMappedFile<M> {
    fn base(&self) -> &ReferenceResourceBase {
        self.reference_resource.base()
    }

    fn cleanup(&self, current_ref: i64) -> bool {
        self.cleanup(current_ref)
    }
}

// ============================================================================
// ============================================================================
// New APIs for Enhanced Performance
// ============================================================================

impl<M: MappedMemory> DefaultMappedFile<M> {
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
        if !self.raw_core.is_file_range(pos, size) {
            return None;
        }

        // True zero-copy: the backend region keeps the mapping alive while `Bytes` owns the view.
        Some(Bytes::from_owner(self.get_mapped_memory().region(pos, size)))
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

        let Some((start, len)) = self.raw_core.prepare_flush_range(start, end) else {
            return 0;
        };

        let flush_start = Instant::now();

        // Perform the flush
        if self.transient_store_pool.is_some() {
            self.raw_core.record_transient_flush_range(end);
        }

        let result = match self
            .try_get_mapped_file_ref()
            .and_then(|mapped_memory| mapped_memory.flush_range(start, len))
        {
            Ok(_) => len as i32,
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
    use std::fs::File;
    use std::ops::Deref;
    use std::sync::Arc;

    use memmap2::MmapMut;
    use tempfile::TempDir;

    use super::*;
    use crate::base::memory_lock_manager::MemoryLockCategory;
    use crate::base::memory_lock_manager::MemoryLockManager;

    #[derive(Clone)]
    struct TestMappedMemory {
        mmap: Arc<MmapMut>,
    }

    struct TestMappedRegion {
        mmap: Arc<MmapMut>,
        offset: usize,
        len: usize,
    }

    impl Deref for TestMappedRegion {
        type Target = [u8];

        fn deref(&self) -> &Self::Target {
            &self.mmap[self.offset..self.offset + self.len]
        }
    }

    impl AsRef<[u8]> for TestMappedRegion {
        fn as_ref(&self) -> &[u8] {
            self
        }
    }

    // SAFETY: the backend owns the mapping through an `Arc`, keeps regions alive, and tests
    // serialize mutable access through `DefaultMappedFile`.
    unsafe impl MappedMemory for TestMappedMemory {
        type Region = TestMappedRegion;

        fn map_mut(file: &File) -> io::Result<Self> {
            // SAFETY: `MappedFileStorage` sizes the file before invoking the backend and never
            // resizes it while this mapping is live.
            let mmap = unsafe { MmapMut::map_mut(file)? };
            Ok(Self { mmap: Arc::new(mmap) })
        }

        fn as_slice(&self) -> &[u8] {
            self.mmap.as_ref()
        }

        fn as_mut_ptr(&self) -> *mut u8 {
            self.mmap.as_ptr().cast_mut()
        }

        fn flush(&self) -> io::Result<()> {
            self.mmap.flush()
        }

        fn flush_range(&self, offset: usize, len: usize) -> io::Result<()> {
            self.mmap.flush_range(offset, len)
        }

        fn region(&self, offset: usize, len: usize) -> Self::Region {
            TestMappedRegion {
                mmap: self.mmap.clone(),
                offset,
                len,
            }
        }
    }

    type TestDefaultMappedFile = DefaultMappedFile<TestMappedMemory>;

    fn create_test_file() -> (TempDir, TestDefaultMappedFile) {
        let temp_dir = TempDir::new().unwrap();
        // Use numeric filename format expected by DefaultMappedFile
        let file_path = temp_dir.path().join("00000000000000000000");
        let file_name = CheetahString::from(file_path.to_str().unwrap());

        let mapped_file = TestDefaultMappedFile::new(file_name, 4096);
        (temp_dir, mapped_file)
    }

    fn create_lazy_test_file() -> (TempDir, TestDefaultMappedFile) {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("00000000000000000000");
        let file_name = CheetahString::from(file_path.to_str().unwrap());

        let mapped_file = TestDefaultMappedFile::try_new_lazy_read_only(file_name, 4096).unwrap();
        (temp_dir, mapped_file)
    }

    fn create_transient_test_file() -> (TempDir, TransientStorePool, TestDefaultMappedFile) {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("00000000000000000000");
        let file_name = CheetahString::from(file_path.to_str().unwrap());
        let transient_store_pool = TransientStorePool::new(1, 4096);
        let mapped_file =
            TestDefaultMappedFile::new_with_transient_store_pool(file_name, 4096, transient_store_pool.clone());
        (temp_dir, transient_store_pool, mapped_file)
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
    fn invalid_raw_byte_requests_do_not_materialize_lazy_mapping() {
        macro_rules! assert_stays_unmapped {
            ($mapped_file:ident, $request:block) => {{
                let (_temp_dir, $mapped_file) = create_lazy_test_file();
                $request
                assert!(!$mapped_file.is_mapped());
                assert_eq!($mapped_file.lazy_mmap_stats().map_operations, 0);
            }};
        }

        assert_stays_unmapped!(mapped_file, {
            assert!(MappedFile::get_bytes(&mapped_file, 4096, 1).is_none());
        });
        assert_stays_unmapped!(mapped_file, {
            assert!(MappedFile::get_bytes_readable_checked(&mapped_file, 0, 1).is_none());
        });
        assert_stays_unmapped!(mapped_file, {
            assert!(!MappedFile::append_message_offset_length(&mapped_file, b"x", 0, 4097,));
        });
        assert_stays_unmapped!(mapped_file, {
            assert!(!MappedFile::append_message_no_position_update(
                &mapped_file,
                b"x",
                0,
                4097,
            ));
        });
        assert_stays_unmapped!(mapped_file, {
            assert!(MappedFile::get_direct_write_buffer(&mapped_file, 4097).is_none());
        });
        assert_stays_unmapped!(mapped_file, {
            assert!(!MappedFile::write_bytes_segment(&mapped_file, b"x", 4096, 0, 1,));
        });
        assert_stays_unmapped!(mapped_file, {
            assert!(!MappedFile::put_slice(&mapped_file, b"x", 4096));
        });
        assert_stays_unmapped!(mapped_file, {
            assert!(MappedFile::get_slice(&mapped_file, 4096, 0).is_none());
        });
    }

    #[test]
    fn eager_mmap_maps_during_construction() {
        let (_temp_dir, mapped_file) = create_test_file();

        assert!(!mapped_file.is_lazy_mmap_enabled());
        assert!(mapped_file.is_mapped());
        assert_eq!(mapped_file.get_mapped_file().len(), 4096);
        assert_eq!(mapped_file.lazy_mmap_stats(), LazyMmapStats::default());
    }

    #[test]
    fn lazy_mmap_stats_legacy_path_has_local_type_identity() {
        fn round_trip(stats: LazyMmapStats) -> crate::mapped_file::mapping::LazyMmapStats {
            stats
        }

        assert_eq!(round_trip(LazyMmapStats::default()), LazyMmapStats::default());
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
        assert!(!mapped_file.is_valid_cache_range(i64::MAX, usize::MAX));
        assert!(mapped_file.is_valid_cache_range(0, mapped_file.get_file_size() as usize));
        assert!(mapped_file.is_valid_cache_range(mapped_file.get_file_size() as i64 - 1, 1));
        assert!(!mapped_file.is_valid_cache_range(mapped_file.get_file_size() as i64 - 1, 2));
    }
}
