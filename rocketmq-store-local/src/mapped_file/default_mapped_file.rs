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

use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::ptr;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use cheetah_string::CheetahString;
use parking_lot::Mutex;
use parking_lot::MutexGuard;
use rocketmq_error::RocketMQResult;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use super::FlushStrategy;
use super::MappedFile;
use super::MappedFileAdmissionState;
use super::MappedFileDestroyOutcome;
use super::MappedFileDetachOutcome;
use super::MappedFileError;
use super::MappedFileMetrics;
use super::MappedFileRawCore;
use super::MappedFileResult;
use super::MappedMemory;
use super::MappedWriteLease;
use super::NativeMappedMemory;
use super::SelectMappedBufferCacheState;
use super::SelectMappedBufferResult;
use crate::base::memory_lock_manager::MemoryLockCategory;
use crate::base::memory_lock_manager::MemoryLockHandle;
use crate::base::memory_lock_manager::MemoryLockManager;
use crate::base::memory_lock_manager::OwnedMemoryRegion;
use crate::base::transient_store_pool::PoolLease;
use crate::base::transient_store_pool::TransientStorePool;
use crate::config::FlushDiskType;
use crate::mapped_file::file::FileOwner;
use crate::mapped_file::file::FilePreallocateOutcome;
use crate::mapped_file::file::MappedFileStorage;
use crate::mapped_file::generation::GenerationRegion;
use crate::mapped_file::generation::MappedFileMapping;
use crate::mapped_file::generation::MappedReadLease;
use crate::mapped_file::generation::MappingPublicationError;
use crate::mapped_file::generation::ReadOnlyAccess;
use crate::mapped_file::generation::ReadOnlyMappingGeneration;
use crate::mapped_file::generation::WritableAccess;
use crate::mapped_file::generation::WritableMappingGeneration;
use crate::mapped_file::kernel::visit_mapped_file_warmup_schedule;
use crate::mapped_file::kernel::MappedFileWarmupOperation;
use crate::mapped_file::kernel::ReferenceResource;
use crate::mapped_file::kernel::ReferenceResourceBase;
use crate::mapped_file::kernel::ReferenceResourceCounter;
pub use crate::mapped_file::kernel::OS_PAGE_SIZE;
use crate::mapped_file::lifecycle::BorrowedMappedFileLease;
use crate::mapped_file::lifecycle::MappedFileLease;
use crate::mapped_file::lifecycle::MappedFileLeaseProof;
use crate::mapped_file::lifecycle::PhysicalDetachClaimResult;
use crate::mapped_file::lifecycle::PhysicalDetachHook;
pub use crate::mapped_file::mapping::LazyMmapStats;
use crate::mapped_file::memory::ReadOnlyMappedMemory;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::state::reconciliation::ReconciledSegmentFile;
use crate::mapped_file::MappedFileLifecycleSnapshot;
use crate::mapped_file::MappedFileOperation;
use crate::utils::ffi::advise_memory;
use crate::utils::ffi::get_page_size;
use crate::utils::ffi::lock_memory_region;
#[cfg(target_os = "linux")]
use crate::utils::ffi::memory_residency;
use crate::utils::ffi::unlock_memory_region;
use crate::utils::ffi::MemoryAdvice;

fn invalid_input_error(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn borrow_transient_buffer(
    transient_store_pool: Option<&TransientStorePool>,
    file_size: u64,
) -> io::Result<Option<PoolLease>> {
    let transient_buffer = transient_store_pool
        .map(|pool| {
            pool.borrow_lease().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "transient store pool has no available buffer",
                )
            })
        })
        .transpose()?;
    if transient_buffer
        .as_ref()
        .is_some_and(|buffer| buffer.len() != file_size as usize)
    {
        return Err(invalid_input_error(format!(
            "transient buffer size does not match mapped file: expected {file_size}, got {}",
            transient_buffer.as_ref().map_or(0, PoolLease::len)
        )));
    }
    Ok(transient_buffer)
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or_default()
}

const LINUX_STORAGE_DEGRADATION_UNKNOWN_ERRNO: i32 = -1;
#[cfg(test)]
const LINUX_STORAGE_OP_FALLOCATE: &str = "fallocate";
const LINUX_STORAGE_OP_MADVISE: &str = "madvise";
const LINUX_STORAGE_OP_PAGE_TOUCH: &str = "page_touch";
const LINUX_STORAGE_REASON_FAILED: &str = "failed";
const LINUX_STORAGE_REASON_FLUSH_FAILED: &str = "flush_failed";
#[cfg(test)]
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

#[cfg(test)]
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

pub struct DefaultMappedFile<M: MappedMemory = NativeMappedMemory> {
    reference_resource: ReferenceResourceCounter,
    physical_owners: Arc<MappedFilePhysicalOwners<M>>,
    transient_store_pool: Option<TransientStorePool>,
    file_name: CheetahString,
    raw_core: MappedFileRawCore,
    write_state: Mutex<MappedWriteState>,
    first_create_in_queue: bool,
    swap_state: Mutex<SwapLifecycleState<M::ReadOnly>>,
    mapped_byte_buffer_access_count_since_last_swap: AtomicI64,
    metrics: Option<Arc<MappedFileMetrics>>,
    seal_lock: Mutex<()>,
    flush_strategy: FlushStrategy,
    #[cfg(feature = "observability")]
    store_metrics: rocketmq_observability::metrics::store::StoreMetricsRecorder,
}

struct MappedFilePhysicalOwners<M: MappedMemory> {
    storage: Mutex<MappedFileStorage>,
    mapping: MappedFileMapping<M, M::ReadOnly>,
    metrics: Arc<MappedFileMetrics>,
    detach_report: Mutex<Option<(Option<u64>, bool)>>,
}

impl<M: MappedMemory> MappedFilePhysicalOwners<M> {
    fn detach_owner_slots(&self) -> (Option<u64>, bool) {
        if let Some(report) = *self.detach_report.lock() {
            return report;
        }

        let detached_generation = self.mapping.detach().into_generation();
        let generation_id = detached_generation.as_ref().map(|generation| generation.id().get());
        let detached_file_owner = self.storage.lock().take_owner();
        let had_file_owner = detached_file_owner.is_some();
        self.metrics.record_lifecycle_detach();

        // Drop the mapping before the canonical file owner. A generation itself retains one
        // FileOwner Arc, so this ordering guarantees unmap precedes last-close on Windows.
        drop(detached_generation);
        drop(detached_file_owner);

        let report = (generation_id, had_file_owner);
        *self.detach_report.lock() = Some(report);
        report
    }
}

impl<M: MappedMemory> PhysicalDetachHook for MappedFilePhysicalOwners<M> {
    fn detach_owner_slots(&self) {
        let _ = self.detach_owner_slots();
    }
}

enum AdmittedReadGeneration<M: MappedMemory> {
    Writable(Arc<WritableMappingGeneration<M>>),
    ReadOnly(Arc<ReadOnlyMappingGeneration<M::ReadOnly>>),
}

enum MappedMemoryLockRegion<M: MappedMemory> {
    Writable(GenerationRegion<M, WritableAccess>),
    ReadOnly(GenerationRegion<M::ReadOnly, ReadOnlyAccess>),
}

impl<M: MappedMemory> OwnedMemoryRegion for MappedMemoryLockRegion<M> {
    fn address(&self) -> *const u8 {
        match self {
            Self::Writable(region) => region.as_ptr(),
            Self::ReadOnly(region) => region.as_ref().as_ptr(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Writable(region) => region.len(),
            Self::ReadOnly(region) => region.len(),
        }
    }
}

impl<M: MappedMemory> AdmittedReadGeneration<M> {
    fn with_slice<T>(&self, operation: impl FnOnce(&[u8]) -> T) -> T {
        match self {
            Self::Writable(generation) => generation.with_mapping(|mapping| operation(mapping.as_slice())),
            Self::ReadOnly(generation) => generation.with_mapping(|mapping| operation(mapping.as_slice())),
        }
    }
}

#[derive(Default)]
struct MappedWriteState {
    staging: Vec<u8>,
    transient_buffer: Option<PoolLease>,
}

struct SwapLifecycleState<R: ReadOnlyMappedMemory> {
    time_ms: u64,
    generation: u64,
    retired: Vec<Weak<ReadOnlyMappingGeneration<R>>>,
}

#[derive(Clone, Copy)]
pub(crate) struct SwapGenerationSnapshot {
    time_ms: u64,
    generation: u64,
}

impl SwapGenerationSnapshot {
    #[inline]
    pub(crate) fn time_millis(self) -> i64 {
        i64::try_from(self.time_ms).unwrap_or(i64::MAX)
    }
}

/// Single-writer reservation backed by an owned staging buffer.
///
/// The lease holds the mapped file's writer sequencer for its complete lifetime. It never exposes
/// the mmap itself, and dropping it does not publish or copy staged bytes.
pub struct DefaultMappedWriteLease<'a, M: MappedMemory = NativeMappedMemory> {
    owner: &'a DefaultMappedFile<M>,
    state: MutexGuard<'a, MappedWriteState>,
    admission: BorrowedMappedFileLease<'a>,
    start_position: usize,
    capacity: usize,
}

impl<M: MappedMemory> MappedWriteLease for DefaultMappedWriteLease<'_, M> {
    #[inline]
    fn start_position(&self) -> usize {
        self.start_position
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    fn buffer_mut(&mut self) -> &mut [u8] {
        &mut self.state.staging[..self.capacity]
    }

    fn commit(mut self, actual_bytes: usize, store_timestamp: Option<u64>) -> MappedFileResult<usize> {
        if actual_bytes == 0 || actual_bytes > self.capacity {
            return Err(MappedFileError::InvalidWriteCommit {
                reserved: self.capacity,
                actual: actual_bytes,
            });
        }

        let end_position = self
            .start_position
            .checked_add(actual_bytes)
            .filter(|end| *end <= self.owner.raw_core.file_size() as usize)
            .ok_or_else(|| {
                MappedFileError::out_of_bounds(self.start_position, actual_bytes, self.owner.raw_core.file_size())
            })?;
        let end_position_i32 = i32::try_from(end_position)
            .map_err(|_| MappedFileError::WritePositionOverflow { position: end_position })?;
        let current_position = self.owner.raw_core.wrote_position();
        if usize::try_from(current_position).ok() != Some(self.start_position) {
            return Err(MappedFileError::InvalidWritePosition {
                position: current_position,
                capacity: self.owner.raw_core.file_size(),
            });
        }

        if self.owner.transient_store_pool.is_some() {
            let MappedWriteState {
                staging,
                transient_buffer,
            } = &mut *self.state;
            let transient_buffer = transient_buffer
                .as_mut()
                .ok_or(MappedFileError::TransientStoreExhausted)?;
            transient_buffer[self.start_position..end_position].copy_from_slice(&staging[..actual_bytes]);
        } else {
            self.owner.copy_to_mapping(
                &self.admission,
                self.start_position,
                &self.state.staging[..actual_bytes],
            )?;
        }
        if let Some(store_timestamp) = store_timestamp {
            self.owner.raw_core.set_store_timestamp(store_timestamp);
        }
        self.owner.raw_core.set_wrote_position(end_position_i32);
        if let Some(metrics) = &self.owner.metrics {
            metrics.record_write(actual_bytes);
        }
        Ok(end_position)
    }
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
    /// Returns one coherent snapshot of mapped-file admission and drain state.
    #[inline]
    pub fn lifecycle_snapshot(&self) -> MappedFileLifecycleSnapshot {
        self.reference_resource.lifecycle().snapshot()
    }

    /// Seals the segment against new writes while keeping reads and maintenance available.
    pub fn seal_readable(&self) -> bool {
        match self.try_seal_readable() {
            Ok(sealed) => sealed,
            Err(error) => {
                warn!(file_name = %self.file_name, error = %error, "failed to seal mapped file as read-only");
                false
            }
        }
    }

    /// Rejects writers, drains already-admitted writes, performs the final flush, and publishes a
    /// read-only mapping generation.
    pub fn try_seal_readable(&self) -> MappedFileResult<bool> {
        let _seal = self.seal_lock.lock();
        let lifecycle = self.reference_resource.lifecycle();
        let started = lifecycle
            .seal_readable_and_wait_for_writers()
            .map_err(|error| match error {
                crate::mapped_file::lifecycle::LifecycleAcquireError::Unavailable { state, operation } => {
                    MappedFileError::Unavailable { state, operation }
                }
                crate::mapped_file::lifecycle::LifecycleAcquireError::LeaseCountOverflow => {
                    MappedFileError::LeaseCountOverflow
                }
            })?;
        if self.physical_owners.mapping.load_read_only().is_some() {
            return Ok(false);
        }

        let admission = self.acquire_borrowed(MappedFileOperation::Maintenance)?;
        let mut writer = self.write_state.lock();
        self.commit_transient_buffer(&mut writer, &admission)?;
        let writable = self.try_get_writable_generation(&admission, MappedFileOperation::Maintenance)?;
        writable
            .with_mapping(MappedMemory::flush)
            .map_err(MappedFileError::FlushFailed)?;
        let file_owner = Arc::clone(writable.file_owner());
        let expected = writable.id();
        self.physical_owners
            .mapping
            .replace_with_read_only(
                expected,
                || {
                    // SAFETY: sealing has rejected and drained writers, `write_state` excludes
                    // mutation, and the candidate immediately enters a generation retaining this
                    // canonical FileOwner.
                    unsafe { file_owner.map_read_only::<M>() }
                },
                || {
                    self.validate_lease(&admission, MappedFileOperation::Maintenance)
                        .is_ok()
                },
                |publication| {
                    lifecycle
                        .try_publish_before_close(&admission, MappedFileOperation::Maintenance, || {
                            publication.publish()
                        })
                        .ok()
                },
            )
            .map_err(|error| self.map_publication_error(error, MappedFileOperation::Maintenance))?;
        Ok(started || self.physical_owners.mapping.load_read_only().is_some())
    }

    /// Waits for currently admitted operations to drain.
    #[inline]
    pub fn wait_for_lifecycle_drain(&self, timeout: std::time::Duration) -> bool {
        self.reference_resource.lifecycle().wait_for_drain(timeout)
    }

    /// Acquires an immutable owner-bound mapped range from a sealed generation.
    ///
    /// The returned lease keeps both the read-only mapping generation and one read admission alive.
    /// Clones and split ranges share that admission, and the final alias releases the generation
    /// before it releases lifecycle admission. Active writable segments return `Ok(None)` because
    /// safe cross-call byte slices are exposed only after [`Self::try_seal_readable`] publishes a
    /// read-only generation.
    ///
    /// # Errors
    ///
    /// Returns a lifecycle error after close, or [`MappedFileError::OutOfBounds`] when the range
    /// overflows or exceeds the configured segment mapping.
    pub fn try_mapped_read_lease(
        &self,
        offset: usize,
        len: usize,
    ) -> MappedFileResult<Option<MappedReadLease<M::ReadOnly>>> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| MappedFileError::out_of_bounds(offset, len, self.raw_core.file_size()))?;
        let readable_position =
            usize::try_from(self.get_read_position()).map_err(|_| MappedFileError::InvalidWritePosition {
                position: self.get_read_position(),
                capacity: self.raw_core.file_size(),
            })?;
        if end > readable_position {
            return Ok(None);
        }

        let admission = self.acquire_owned(MappedFileOperation::Read)?;
        let Some(generation) = self.physical_owners.mapping.load_read_only() else {
            return Ok(None);
        };
        MappedReadLease::try_new(generation, admission, offset, len)
            .map(Some)
            .map_err(|_| MappedFileError::out_of_bounds(offset, len, self.raw_core.file_size()))
    }

    #[inline]
    pub(crate) fn try_acquire_owned_lease(&self, operation: MappedFileOperation) -> MappedFileResult<MappedFileLease> {
        self.acquire_owned(operation)
    }

    #[inline]
    pub(crate) fn swap_generation_snapshot(&self) -> SwapGenerationSnapshot {
        let state = self.swap_state.lock();
        SwapGenerationSnapshot {
            time_ms: state.time_ms,
            generation: state.generation,
        }
    }

    #[inline]
    pub(crate) fn try_clean_swapped_generation(&self, expected: SwapGenerationSnapshot, force: bool) -> bool {
        let mut state = self.swap_state.lock();
        if state.generation != expected.generation || state.time_ms != expected.time_ms {
            return false;
        }
        let before = state.retired.len();
        state.retired.retain(|generation| generation.upgrade().is_some());
        if state.retired.len() == before {
            return false;
        }
        if force {
            self.mapped_byte_buffer_access_count_since_last_swap
                .store(0, Ordering::Release);
        }
        if let Some(metrics) = &self.metrics {
            metrics.record_clean_swap();
        }
        true
    }

    #[cfg(test)]
    fn retired_swap_observation_count(&self) -> usize {
        self.swap_state.lock().retired.len()
    }

    fn try_swap_read_only_generation(&self) -> MappedFileResult<bool> {
        if self.lifecycle_snapshot().state != MappedFileAdmissionState::SealedReadable {
            return Ok(false);
        }

        let admission = self.acquire_borrowed(MappedFileOperation::Maintenance)?;
        let _writer = self.write_state.lock();
        let mut swap_state = self.swap_state.lock();
        let Some(retired) = self.physical_owners.mapping.load_read_only() else {
            return Ok(false);
        };
        let expected = retired.id();
        let file_owner = Arc::clone(retired.file_owner());
        let lifecycle = self.reference_resource.lifecycle();
        let current = self
            .physical_owners
            .mapping
            .replace_with_read_only(
                expected,
                || {
                    // SAFETY: sealed-readable state rejects writers, `write_state` serializes
                    // maintenance, and the candidate retains this canonical FileOwner before it
                    // can be published.
                    unsafe { file_owner.map_read_only::<M>() }
                },
                || {
                    self.validate_lease(&admission, MappedFileOperation::Maintenance)
                        .is_ok()
                },
                |publication| {
                    lifecycle
                        .try_publish_before_close(&admission, MappedFileOperation::Maintenance, || {
                            publication.publish()
                        })
                        .ok()
                },
            )
            .map_err(|error| self.map_publication_error(error, MappedFileOperation::Maintenance))?;

        swap_state.time_ms = current_millis();
        swap_state.generation = current.id().get();
        swap_state.retired.push(Arc::downgrade(&retired));
        self.mapped_byte_buffer_access_count_since_last_swap
            .store(0, Ordering::Release);
        if let Some(metrics) = &self.metrics {
            metrics.record_swap();
        }
        Ok(true)
    }

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

    /// Builds an eager managed mapping from the exact handle retained during reconciliation.
    ///
    /// No namespace path is opened or modified by this constructor.
    pub(crate) fn try_new_reconciled(store_root: &Path, segment: ReconciledSegmentFile) -> io::Result<Self> {
        let (relative_path, binding, file) = segment.into_parts();
        let path = relative_path.join_under(store_root);
        let file_name = path
            .to_str()
            .ok_or_else(|| invalid_input_error(format!("mapped-file path is not UTF-8: {}", path.display())))?;
        let file_name = CheetahString::from_string(file_name.to_owned());
        let file_size = binding.expected_length();
        let metrics = Arc::new(MappedFileMetrics::new());
        let storage = MappedFileStorage::from_reconciled_file(
            path,
            binding.segment_offset(),
            file_size,
            binding.physical_key(),
            file,
            Arc::clone(&metrics),
        )?;
        Self::try_from_storage(file_name, file_size, None, None, false, storage, metrics)
    }

    /// Builds an eager managed mapping from a newly published, already verified file handle.
    ///
    /// The caller must have durably recorded `PublishIncarnation` before invoking this method.
    /// This constructor never opens, creates, resizes, or preallocates the namespace path.
    pub(crate) fn try_new_managed_created(
        path: PathBuf,
        segment_offset: u64,
        file_size: u64,
        physical_key: PhysicalFileKey,
        file: std::fs::File,
        transient_store_pool: Option<TransientStorePool>,
    ) -> io::Result<Self> {
        let file_name = path
            .to_str()
            .ok_or_else(|| invalid_input_error(format!("mapped-file path is not UTF-8: {}", path.display())))?;
        let file_name = CheetahString::from_string(file_name.to_owned());
        let transient_buffer = borrow_transient_buffer(transient_store_pool.as_ref(), file_size)?;
        let metrics = Arc::new(MappedFileMetrics::new());
        let storage = MappedFileStorage::from_reconciled_file(
            path,
            segment_offset,
            file_size,
            physical_key,
            file,
            Arc::clone(&metrics),
        )?;
        Self::try_from_storage(
            file_name,
            file_size,
            transient_store_pool,
            transient_buffer,
            false,
            storage,
            metrics,
        )
    }

    /// Returns the identifier of the owner-bound mapping currently published for this file.
    pub(crate) fn current_mapping_generation_id(&self) -> Option<u64> {
        self.physical_owners
            .mapping
            .current_generation_id()
            .map(|generation| generation.get())
    }

    fn try_new_inner(
        file_name: CheetahString,
        file_size: u64,
        transient_store_pool: Option<TransientStorePool>,
        lazy_mmap_enabled: bool,
    ) -> io::Result<Self> {
        let transient_buffer = borrow_transient_buffer(transient_store_pool.as_ref(), file_size)?;
        let path_buf = Path::new(file_name.as_str()).to_path_buf();
        let dir = path_buf
            .parent()
            .ok_or_else(|| invalid_input_error(format!("file path is invalid: {file_name}")))?;
        std::fs::create_dir_all(dir)?;

        let metrics = Arc::new(MappedFileMetrics::new());
        // Wave A keeps the existing mapped-file construction path explicitly unmanaged. A later
        // retirement activation must use the proof-gated managed storage constructor instead.
        let (storage, preallocate_outcome) =
            MappedFileStorage::open_with_metrics(path_buf, file_size, Arc::clone(&metrics))?;
        if let Some(preallocate_outcome) = preallocate_outcome {
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

        Self::try_from_storage(
            file_name,
            file_size,
            transient_store_pool,
            transient_buffer,
            lazy_mmap_enabled,
            storage,
            metrics,
        )
    }

    fn try_from_storage(
        file_name: CheetahString,
        file_size: u64,
        transient_store_pool: Option<TransientStorePool>,
        transient_buffer: Option<PoolLease>,
        lazy_mmap_enabled: bool,
        storage: MappedFileStorage,
        metrics: Arc<MappedFileMetrics>,
    ) -> io::Result<Self> {
        let mapping = if lazy_mmap_enabled {
            MappedFileMapping::new_lazy(Arc::clone(&metrics))
        } else {
            let file_owner = storage.owner().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotConnected,
                    "mapped-file owner detached during construction",
                )
            })?;
            // SAFETY: storage established the configured length and the mapping immediately enters
            // a generation retaining the canonical FileOwner. All later mutation is serialized by
            // mapped-file admission and `write_state`.
            let mapping = unsafe { file_owner.map_mut::<M>() }?;
            MappedFileMapping::new_eager(mapping, file_owner, Arc::clone(&metrics))
        };

        let physical_owners = Arc::new(MappedFilePhysicalOwners {
            storage: Mutex::new(storage),
            mapping,
            metrics: Arc::clone(&metrics),
            detach_report: Mutex::new(None),
        });
        let reference_resource = ReferenceResourceCounter::new();
        let detach_hook: Arc<dyn PhysicalDetachHook> = physical_owners.clone();
        if !reference_resource.lifecycle().install_physical_detach_hook(detach_hook) {
            return Err(io::Error::other(
                "mapped-file physical detach hook was already installed",
            ));
        }

        Ok(Self {
            reference_resource,
            physical_owners,
            file_name,
            raw_core: MappedFileRawCore::new(file_size),
            write_state: Mutex::new(MappedWriteState {
                staging: Vec::new(),
                transient_buffer,
            }),
            first_create_in_queue: false,
            swap_state: Mutex::new(SwapLifecycleState {
                time_ms: current_millis(),
                generation: 1,
                retired: Vec::new(),
            }),
            mapped_byte_buffer_access_count_since_last_swap: Default::default(),
            transient_store_pool,
            metrics: Some(metrics),
            seal_lock: Mutex::new(()),
            flush_strategy: FlushStrategy::Async,
            #[cfg(feature = "observability")]
            store_metrics: rocketmq_observability::metrics::store::StoreMetricsRecorder::noop(),
        })
    }

    /// Binds mapped-file observations to the owning Store telemetry instance.
    #[cfg(feature = "observability")]
    #[doc(hidden)]
    pub fn with_store_metrics(
        mut self,
        store_metrics: rocketmq_observability::metrics::store::StoreMetricsRecorder,
    ) -> Self {
        self.store_metrics = store_metrics;
        self
    }

    fn record_linux_storage_degradation(&self, event: LinuxStorageDegradationEvent) {
        #[cfg(feature = "observability")]
        self.store_metrics
            .record_linux_storage_degradation(event.operation, event.reason, event.errno, event.count);

        #[cfg(not(feature = "observability"))]
        let _ = event;
    }

    #[inline]
    fn acquire_owned(&self, operation: MappedFileOperation) -> MappedFileResult<MappedFileLease> {
        self.reference_resource.try_acquire(operation)
    }

    #[inline]
    fn acquire_borrowed(&self, operation: MappedFileOperation) -> MappedFileResult<BorrowedMappedFileLease<'_>> {
        self.reference_resource.try_acquire_borrowed(operation)
    }

    fn validate_lease<L: MappedFileLeaseProof + ?Sized>(
        &self,
        lease: &L,
        required: MappedFileOperation,
    ) -> MappedFileResult<()> {
        let valid_operation = matches!(
            (lease.operation(), required),
            (
                MappedFileOperation::Write,
                MappedFileOperation::Read | MappedFileOperation::Write
            ) | (
                MappedFileOperation::Maintenance,
                MappedFileOperation::Read | MappedFileOperation::Maintenance
            ) | (MappedFileOperation::Read, MappedFileOperation::Read)
        );
        if ptr::eq(lease.lifecycle(), self.reference_resource.lifecycle().as_ref()) && valid_operation {
            return Ok(());
        }
        Err(MappedFileError::Unavailable {
            state: self.reference_resource.lifecycle().state(),
            operation: required,
        })
    }

    fn copy_to_writable_generation(
        generation: &WritableMappingGeneration<M>,
        start: usize,
        end: usize,
        data: &[u8],
    ) -> MappedFileResult<()> {
        generation.with_mapping(|mapped_memory| {
            if end > mapped_memory.as_slice().len() {
                return Err(MappedFileError::out_of_bounds(
                    start,
                    data.len(),
                    mapped_memory.as_slice().len() as u64,
                ));
            }

            // SAFETY: the caller holds `write_state`, which is the only mapped-file mutation
            // sequencer. The staging allocation cannot overlap the mapped allocation.
            unsafe {
                mapped_memory.copy_from_slice(start, data)?;
            }
            Ok(())
        })
    }

    fn copy_to_mapping<L: MappedFileLeaseProof + ?Sized>(
        &self,
        lease: &L,
        start: usize,
        data: &[u8],
    ) -> MappedFileResult<()> {
        self.copy_to_mapping_for_operation(lease, MappedFileOperation::Write, start, data)
    }

    fn copy_to_mapping_for_operation<L: MappedFileLeaseProof + ?Sized>(
        &self,
        lease: &L,
        operation: MappedFileOperation,
        start: usize,
        data: &[u8],
    ) -> MappedFileResult<()> {
        let end = start
            .checked_add(data.len())
            .filter(|end| *end <= self.raw_core.file_size() as usize)
            .ok_or_else(|| MappedFileError::out_of_bounds(start, data.len(), self.raw_core.file_size()))?;
        self.validate_lease(lease, operation)?;

        if let Some(result) = self
            .physical_owners
            .mapping
            .with_writable_scoped(|generation| Self::copy_to_writable_generation(generation, start, end, data))
        {
            return result;
        }

        // A missing writable generation is the lazy-initialization path. It intentionally keeps
        // the owned publication flow so candidate construction remains serialized with close and
        // terminal detach. Already-published generations never reach this branch.
        let generation = self.try_get_writable_generation(lease, operation)?;
        Self::copy_to_writable_generation(&generation, start, end, data)
    }

    fn ensure_transient_buffer<'a>(&self, state: &'a mut MappedWriteState) -> MappedFileResult<&'a mut PoolLease> {
        let pool = self
            .transient_store_pool
            .as_ref()
            .ok_or_else(|| MappedFileError::Configuration("mapped file is not transient".to_owned()))?;
        if state.transient_buffer.is_none() {
            state.transient_buffer = Some(pool.borrow_lease().ok_or(MappedFileError::TransientStoreExhausted)?);
        }
        let buffer = state
            .transient_buffer
            .as_mut()
            .ok_or(MappedFileError::TransientStoreExhausted)?;
        if buffer.len() != self.raw_core.file_size() as usize {
            return Err(MappedFileError::Configuration(format!(
                "transient buffer size mismatch: expected {}, got {}",
                self.raw_core.file_size(),
                buffer.len()
            )));
        }
        Ok(buffer)
    }

    fn commit_transient_buffer<L: MappedFileLeaseProof + ?Sized>(
        &self,
        state: &mut MappedWriteState,
        admission: &L,
    ) -> MappedFileResult<i32> {
        if self.transient_store_pool.is_none() {
            return Ok(self.raw_core.committed_position());
        }

        let committed_position = self.raw_core.committed_position();
        let wrote_position = self.raw_core.wrote_position();
        let committed = usize::try_from(committed_position).map_err(|_| MappedFileError::InvalidWritePosition {
            position: committed_position,
            capacity: self.raw_core.file_size(),
        })?;
        let wrote = usize::try_from(wrote_position).map_err(|_| MappedFileError::InvalidWritePosition {
            position: wrote_position,
            capacity: self.raw_core.file_size(),
        })?;
        if wrote < committed || wrote > self.raw_core.file_size() as usize {
            return Err(MappedFileError::InvalidWritePosition {
                position: wrote_position,
                capacity: self.raw_core.file_size(),
            });
        }
        if wrote != committed {
            let buffer = state
                .transient_buffer
                .as_ref()
                .ok_or(MappedFileError::TransientStoreExhausted)?;
            let data = buffer
                .get(committed..wrote)
                .ok_or_else(|| MappedFileError::out_of_bounds(committed, wrote - committed, buffer.len() as u64))?;
            self.copy_to_mapping_for_operation(admission, MappedFileOperation::Maintenance, committed, data)?;
            self.raw_core.set_committed_position_release(wrote_position);
        }
        if let Some(buffer) = state.transient_buffer.take() {
            buffer.return_now();
        }
        Ok(wrote_position)
    }

    fn try_write_at(&self, start: usize, data: &[u8]) -> MappedFileResult<()> {
        let admission = self.acquire_borrowed(MappedFileOperation::Write)?;
        let mut state = self.write_state.lock();
        if self.transient_store_pool.is_some() {
            let end = start
                .checked_add(data.len())
                .filter(|end| *end <= self.raw_core.file_size() as usize)
                .ok_or_else(|| MappedFileError::out_of_bounds(start, data.len(), self.raw_core.file_size()))?;
            self.ensure_transient_buffer(&mut state)?[start..end].copy_from_slice(data);
            Ok(())
        } else {
            self.copy_to_mapping(&admission, start, data)
        }
    }

    #[inline]
    fn write_at(&self, start: usize, data: &[u8]) -> bool {
        self.try_write_at(start, data).is_ok()
    }

    fn copy_range_admitted<L: MappedFileLeaseProof + ?Sized>(
        &self,
        admission: &L,
        pos: usize,
        size: usize,
        readable_position: Option<i32>,
    ) -> MappedFileResult<Option<Bytes>> {
        let Some(end) = pos.checked_add(size) else {
            return Ok(None);
        };
        if end > self.raw_core.file_size() as usize {
            return Ok(None);
        }
        if let Some(readable_position) = readable_position {
            let Ok(readable_position) = usize::try_from(readable_position) else {
                return Ok(None);
            };
            if end > readable_position {
                return Ok(None);
            }
        }

        let _writer = self.write_state.lock();
        let generation = self.try_get_read_generation(admission, MappedFileOperation::Read)?;
        Ok(generation.with_slice(|mapped| mapped.get(pos..end).map(Bytes::copy_from_slice)))
    }

    fn try_copy_range(
        &self,
        pos: usize,
        size: usize,
        readable_position: Option<i32>,
    ) -> MappedFileResult<Option<Bytes>> {
        let admission = self.acquire_borrowed(MappedFileOperation::Read)?;
        self.copy_range_admitted(&admission, pos, size, readable_position)
    }

    #[inline]
    pub fn is_lazy_mmap_enabled(&self) -> bool {
        self.physical_owners.mapping.is_lazy_enabled()
    }

    #[inline]
    pub fn is_mapped(&self) -> bool {
        self.physical_owners.mapping.is_mapped()
    }

    pub fn lazy_mmap_stats(&self) -> LazyMmapStats {
        self.physical_owners.mapping.stats()
    }

    /// Attempts the exactly-once transition that removes mapping and file owners from their slots.
    ///
    /// Existing owner-bearing aliases, if any, keep their physical resources alive until their
    /// own final `Arc` drops. The result therefore reports slot detach, not forced unmap/close.
    pub fn try_detach_physical_owners(&self) -> MappedFileDetachOutcome {
        match self.reference_resource.lifecycle().try_claim_physical_detach() {
            PhysicalDetachClaimResult::Claimed(claim) => {
                let (mapping_generation, had_file_owner) = self.physical_owners.detach_owner_slots();
                claim.complete();
                MappedFileDetachOutcome::Detached {
                    mapping_generation,
                    had_file_owner,
                }
            }
            PhysicalDetachClaimResult::AlreadyDetached => MappedFileDetachOutcome::AlreadyDetached,
            PhysicalDetachClaimResult::InProgress => MappedFileDetachOutcome::InProgress,
            PhysicalDetachClaimResult::Pending { state, active_leases } => {
                MappedFileDetachOutcome::Pending { state, active_leases }
            }
        }
    }

    /// Attempts shutdown, physical slot detach, and namespace removal.
    ///
    /// Namespace removal is attempted only after both physical owner slots are detached. Existing
    /// external aliases, if any, still release their operating-system owners from final `Drop`.
    pub fn try_destroy(&self, interval_forcibly: u64) -> MappedFileDestroyOutcome {
        MappedFile::shutdown(self, interval_forcibly);
        if !ReferenceResource::is_cleanup_over(self) {
            return MappedFileDestroyOutcome::CleanupPending {
                ref_count: ReferenceResource::get_ref_count(self),
            };
        }

        match self.physical_owners.storage.lock().delete() {
            Ok(()) => {
                info!(file_name = %self.file_name, "mapped-file namespace removal succeeded");
                MappedFileDestroyOutcome::NamespaceRemoved
            }
            Err(error) => {
                // A bare NotFound is not an incarnation-safe absence proof. Until the durable
                // retirement protocol lands, keep it retryable instead of authorizing untracking.
                error!(
                    file_name = %self.file_name,
                    error = ?error,
                    "mapped-file namespace removal failed"
                );
                MappedFileDestroyOutcome::DeleteFailed {
                    kind: error.kind(),
                    raw_os_error: error.raw_os_error(),
                }
            }
        }
    }

    fn try_get_writable_generation<L: MappedFileLeaseProof + ?Sized>(
        &self,
        admission: &L,
        required: MappedFileOperation,
    ) -> MappedFileResult<Arc<WritableMappingGeneration<M>>> {
        self.validate_lease(admission, required)?;
        if let Some(generation) = self.physical_owners.mapping.load_writable() {
            return Ok(generation);
        }
        let lifecycle = self.reference_resource.lifecycle();
        self.physical_owners
            .mapping
            .get_or_try_init(
                || {
                    let owner = self
                        .physical_owners
                        .storage
                        .lock()
                        .owner()
                        .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "mapped-file owner detached"))?;
                    // SAFETY: the owner retains the already-sized segment, this operation is
                    // admitted and serialized against detach publication, and the candidate
                    // immediately enters an owner-bound writable generation.
                    let mapping = unsafe { owner.map_mut::<M>() }?;
                    Ok((mapping, owner))
                },
                || self.validate_lease(admission, required).is_ok(),
                |publication| {
                    lifecycle
                        .try_publish_before_close(admission, required, || publication.publish())
                        .ok()
                },
            )
            .map_err(|error| self.map_publication_error(error, required))
    }

    fn try_get_read_generation<L: MappedFileLeaseProof + ?Sized>(
        &self,
        admission: &L,
        required: MappedFileOperation,
    ) -> MappedFileResult<AdmittedReadGeneration<M>> {
        self.validate_lease(admission, MappedFileOperation::Read)?;
        if let Some(generation) = self.physical_owners.mapping.load_read_only() {
            return Ok(AdmittedReadGeneration::ReadOnly(generation));
        }
        match self.try_get_writable_generation(admission, required) {
            Ok(generation) => Ok(AdmittedReadGeneration::Writable(generation)),
            Err(error) => self
                .physical_owners
                .mapping
                .load_read_only()
                .map(AdmittedReadGeneration::ReadOnly)
                .ok_or(error),
        }
    }

    fn map_publication_error(
        &self,
        error: MappingPublicationError<io::Error>,
        operation: MappedFileOperation,
    ) -> MappedFileError {
        match error {
            MappingPublicationError::Initialization(error) => MappedFileError::MmapFailed(error),
            MappingPublicationError::PublicationRejected
            | MappingPublicationError::Detached
            | MappingPublicationError::AlreadyReadOnly => MappedFileError::Unavailable {
                state: self.reference_resource.lifecycle().state(),
                operation,
            },
            other => MappedFileError::Custom(other.to_string()),
        }
    }

    pub(crate) fn file_owner_admitted(&self, admission: &MappedFileLease) -> MappedFileResult<Arc<FileOwner>> {
        self.validate_lease(admission, MappedFileOperation::Read)?;
        self.physical_owners.storage.lock().owner().ok_or_else(|| {
            MappedFileError::Io(io::Error::new(
                io::ErrorKind::NotConnected,
                "mapped-file owner detached",
            ))
        })
    }

    pub(crate) fn mapped_snapshot_matches_admitted(
        &self,
        admission: &MappedFileLease,
        start_offset: u64,
        file_offset: u64,
        snapshot: &[u8],
    ) -> MappedFileResult<bool> {
        if self
            .physical_owners
            .storage
            .lock()
            .file_from_offset()
            .checked_add(file_offset)
            != Some(start_offset)
        {
            return Ok(false);
        }
        let Ok(start) = usize::try_from(file_offset) else {
            return Ok(false);
        };
        let Some(end) = start.checked_add(snapshot.len()) else {
            return Ok(false);
        };
        let Ok(file_size) = usize::try_from(self.raw_core.file_size()) else {
            return Ok(false);
        };
        if end > file_size {
            return Ok(false);
        }

        let _writer = self.write_state.lock();
        let generation = self.try_get_read_generation(admission, MappedFileOperation::Read)?;
        Ok(generation.with_slice(|mapped| mapped.get(start..end) == Some(snapshot)))
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
    fn record_cache_residency_admitted<L: MappedFileLeaseProof + ?Sized>(
        &self,
        admission: &L,
        position: i64,
        size: usize,
    ) -> bool {
        let is_in_cache = self.is_loaded_admitted(admission, position, size);
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
        F: FnOnce(&Self, &BorrowedMappedFileLease<'_>, i32, i32) -> MappedFileResult<()>,
    {
        let admission = self.acquire_borrowed(MappedFileOperation::Maintenance)?;
        let _writer = self.write_state.lock();
        if !self.is_able_to_flush(flush_least_pages) {
            return Ok(self.get_flushed_position());
        }

        let value = self.get_read_position();
        self.mapped_byte_buffer_access_count_since_last_swap
            .fetch_add(1, Ordering::AcqRel);
        let should_flush = !matches!(self.flush_strategy, FlushStrategy::Never);
        if !should_flush {
            return Ok(self.get_flushed_position());
        }

        let flush_started = Instant::now();
        let flushed_position = self.raw_core.flushed_position();
        flush(self, &admission, flushed_position, value)?;

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
    type WriteLease<'a>
        = DefaultMappedWriteLease<'a, M>
    where
        Self: 'a;

    #[inline]
    fn get_file_name(&self) -> &CheetahString {
        &self.file_name
    }

    fn rename_to(&mut self, file_name: &str) -> bool {
        let Ok(_admission) = self.acquire_owned(MappedFileOperation::Maintenance) else {
            return false;
        };
        let new_file = Path::new(file_name);
        let mut storage = self.physical_owners.storage.lock();
        match storage.rename(new_file) {
            Ok(_) => {
                self.file_name = CheetahString::from(file_name);
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
        self.try_get_bytes(pos, size).ok().flatten()
    }

    fn try_get_bytes(&self, pos: usize, size: usize) -> MappedFileResult<Option<bytes::Bytes>> {
        self.try_copy_range(pos, size, None)
    }

    #[inline]
    fn get_bytes_readable_checked(&self, pos: usize, size: usize) -> Option<bytes::Bytes> {
        self.try_copy_range(pos, size, Some(self.get_read_position()))
            .ok()
            .flatten()
    }

    fn append_message_offset_length(&self, data: &[u8], offset: usize, length: usize) -> bool {
        let Some(end) = offset.checked_add(length) else {
            return false;
        };
        let Some(source) = data.get(offset..end) else {
            return false;
        };
        let Ok(mut lease) = self.reserve_write(length) else {
            return false;
        };
        if lease.capacity() != length {
            return false;
        }
        lease.buffer_mut().copy_from_slice(source);
        lease.commit(length, None).is_ok()
    }

    fn reserve_write(&self, required_space: usize) -> MappedFileResult<Self::WriteLease<'_>> {
        if required_space == 0 {
            return Err(MappedFileError::InvalidWriteCommit { reserved: 0, actual: 0 });
        }
        let admission = self.acquire_borrowed(MappedFileOperation::Write)?;
        let mut state = self.write_state.lock();
        let wrote_position = self.raw_core.wrote_position();
        let start_position = usize::try_from(wrote_position).map_err(|_| MappedFileError::InvalidWritePosition {
            position: wrote_position,
            capacity: self.raw_core.file_size(),
        })?;
        let file_size = self.raw_core.file_size() as usize;
        if start_position > file_size {
            return Err(MappedFileError::InvalidWritePosition {
                position: wrote_position,
                capacity: self.raw_core.file_size(),
            });
        }
        if start_position == file_size {
            return Err(MappedFileError::file_full(start_position, self.raw_core.file_size()));
        }

        let capacity = required_space.min(file_size - start_position);
        if self.transient_store_pool.is_some() {
            self.ensure_transient_buffer(&mut state)?;
        }
        state.staging.resize(capacity, 0);
        state.staging[..capacity].fill(0);
        Ok(DefaultMappedWriteLease {
            owner: self,
            state,
            admission,
            start_position,
            capacity,
        })
    }

    fn write_bytes_segment(&self, data: &[u8], start: usize, offset: usize, length: usize) -> bool {
        if data.len() == length {
            return self.write_at(start, data);
        }
        let Some(end) = offset.checked_add(length) else {
            return false;
        };
        let Some(source) = data.get(offset..end) else {
            return false;
        };
        self.write_at(start, source)
    }

    fn put_slice(&self, data: &[u8], index: usize) -> bool {
        !data.is_empty() && self.write_at(index, data)
    }

    #[inline]
    fn get_file_from_offset(&self) -> u64 {
        self.physical_owners.storage.lock().file_from_offset()
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
        self.try_flush_with(flush_least_pages, |mapped_file, admission, flushed_position, value| {
            if mapped_file.physical_owners.mapping.load_read_only().is_some() {
                return Ok(());
            }
            let generation = mapped_file.try_get_writable_generation(admission, MappedFileOperation::Maintenance)?;
            let flush_size = value - flushed_position;
            if flush_size > 0 && flush_size < (mapped_file.raw_core.file_size() as i32) / 2 {
                generation.with_mapping(|mapping| {
                    mapping
                        .flush_range(flushed_position as usize, flush_size as usize)
                        .map_err(MappedFileError::FlushFailed)
                })
            } else {
                generation.with_mapping(|mapping| mapping.flush().map_err(MappedFileError::FlushFailed))
            }
        })
    }

    #[inline]
    fn commit(&self, commit_least_pages: i32) -> i32 {
        self.try_commit(commit_least_pages)
            .unwrap_or_else(|_| self.raw_core.committed_position())
    }

    fn try_commit(&self, commit_least_pages: i32) -> MappedFileResult<i32> {
        let admission = self.acquire_borrowed(MappedFileOperation::Maintenance)?;
        let mut state = self.write_state.lock();
        if self.transient_store_pool.is_none() {
            return Ok(self.raw_core.commit(commit_least_pages));
        }
        if self.raw_core.is_able_to_commit(commit_least_pages)
            || self.raw_core.wrote_position() == self.raw_core.committed_position()
        {
            return self.commit_transient_buffer(&mut state, &admission);
        }
        Ok(self.raw_core.committed_position())
    }

    fn select_mapped_buffer(&self, pos: i32, size: i32) -> Option<SelectMappedBufferResult<M>> {
        self.try_select_mapped_buffer(pos, size).ok().flatten()
    }

    fn try_select_mapped_buffer(&self, pos: i32, size: i32) -> MappedFileResult<Option<SelectMappedBufferResult<M>>> {
        let admission = self.acquire_owned(MappedFileOperation::Read)?;
        let read_position = self.get_read_position();
        if self.raw_core.is_readable_range(pos, size, read_position) {
            self.mapped_byte_buffer_access_count_since_last_swap
                .fetch_add(1, Ordering::AcqRel);

            let is_in_cache = self.record_cache_residency_admitted(&admission, pos as i64, size as usize);
            Ok(self
                .copy_range_admitted(&admission, pos as usize, size as usize, None)?
                .and_then(|bytes| {
                    SelectMappedBufferResult::from_bytes_with_metadata(
                        self.physical_owners.storage.lock().file_from_offset() + pos as u64,
                        pos as u64,
                        bytes,
                        is_in_cache,
                        SelectMappedBufferCacheState::from_residency(is_in_cache),
                    )
                }))
        } else {
            warn!(
                "selectMappedBuffer request pos invalid, request pos: {}, size:{}, fileFromOffset: {}",
                pos,
                size,
                self.physical_owners.storage.lock().file_from_offset()
            );
            Ok(None)
        }
    }

    #[inline]
    fn get_mapped_byte_buffer(&self) -> Bytes {
        self.mapped_byte_buffer_access_count_since_last_swap
            .fetch_add(1, Ordering::AcqRel);

        if let Some(metrics) = &self.metrics {
            metrics.record_read(self.raw_core.file_size() as usize, false);
        }

        self.try_copy_range(0, self.raw_core.file_size() as usize, None)
            .ok()
            .flatten()
            .unwrap_or_default()
    }

    #[inline]
    fn slice_byte_buffer(&self) -> Bytes {
        self.mapped_byte_buffer_access_count_since_last_swap
            .fetch_add(1, Ordering::AcqRel);

        if let Some(metrics) = &self.metrics {
            metrics.record_read(self.raw_core.file_size() as usize, false);
        }

        self.try_copy_range(0, self.raw_core.file_size() as usize, None)
            .ok()
            .flatten()
            .unwrap_or_default()
    }

    #[inline]
    fn get_store_timestamp(&self) -> u64 {
        self.raw_core.store_timestamp()
    }

    #[inline]
    fn get_last_modified_timestamp(&self) -> u64 {
        match self.try_last_modified_time() {
            Ok(modified) => modified
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
                .unwrap_or_default(),
            Err(error) => {
                warn!(file_name = %self.file_name, error = ?error, "failed to read mapped-file modification time");
                0
            }
        }
    }

    #[inline]
    fn try_last_modified_time(&self) -> io::Result<SystemTime> {
        self.physical_owners.storage.lock().modified()
    }

    fn get_data(&self, pos: usize, size: usize) -> Option<bytes::Bytes> {
        self.try_get_data(pos, size).ok().flatten()
    }

    fn try_get_data(&self, pos: usize, size: usize) -> MappedFileResult<Option<bytes::Bytes>> {
        let admission = self.acquire_borrowed(MappedFileOperation::Read)?;
        let read_position = self.get_read_position();
        if self.raw_core.is_readable_byte_range(pos, size, read_position) {
            self.copy_range_admitted(&admission, pos, size, Some(read_position))
        } else {
            warn!(
                "selectMappedBuffer request pos invalid, request pos: {}, size:{}, fileFromOffset: {}",
                pos,
                size,
                self.physical_owners.storage.lock().file_from_offset()
            );
            Ok(None)
        }
    }

    #[inline]
    fn destroy(&self, interval_forcibly: u64) -> bool {
        self.try_destroy(interval_forcibly).is_namespace_removed()
    }

    #[inline]
    fn shutdown(&self, interval_forcibly: u64) {
        ReferenceResource::shutdown(self, interval_forcibly);
    }

    #[inline]
    fn release(&self) {
        ReferenceResource::release(self);
    }

    #[inline]
    fn hold(&self) -> bool {
        ReferenceResource::hold(self)
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
        let _writer = self.write_state.lock();
        self.raw_core.set_wrote_position(wrote_position)
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
        if let Err(error) = self.try_mlock() {
            warn!(file_name = %self.file_name, error = %error, "failed to mlock mapped file");
        }
    }

    fn try_mlock(&self) -> MappedFileResult<()> {
        let admission = self.acquire_borrowed(MappedFileOperation::Maintenance)?;
        let _writer = self.write_state.lock();
        let generation = self.try_get_read_generation(&admission, MappedFileOperation::Maintenance)?;
        generation.with_slice(|mapped| lock_memory_region(mapped).map_err(MappedFileError::MemoryLockFailed))
    }

    #[inline]
    fn munlock(&self) {
        if let Err(error) = self.try_munlock() {
            warn!(file_name = %self.file_name, error = %error, "failed to munlock mapped file");
        }
    }

    fn try_munlock(&self) -> MappedFileResult<()> {
        let admission = self.acquire_borrowed(MappedFileOperation::Maintenance)?;
        let _writer = self.write_state.lock();
        let generation = self.try_get_read_generation(&admission, MappedFileOperation::Maintenance)?;
        generation.with_slice(|mapped| unlock_memory_region(mapped).map_err(MappedFileError::MemoryUnlockFailed))
    }

    #[inline]
    fn warm_mapped_file(&self, flush_disk_type: FlushDiskType, pages: usize) {
        if let Err(error) = self.try_warm_mapped_file(flush_disk_type, pages) {
            warn!(file_name = %self.file_name, error = %error, "failed to warm mapped file");
        }
    }

    fn try_warm_mapped_file(&self, flush_disk_type: FlushDiskType, pages: usize) -> MappedFileResult<()> {
        // Page touching performs volatile writes, so it belongs to writable admission even though
        // its business purpose is maintenance.
        let admission = self.acquire_borrowed(MappedFileOperation::Write)?;
        self.warm_mapped_file_with_ops(
            &admission,
            flush_disk_type,
            pages,
            Self::touch_mapped_page,
            Self::flush_mapped_file_range,
            Self::advise_mapped_file,
            |event| self.record_linux_storage_degradation(event),
        )
    }

    #[inline]
    fn swap_map(&self) -> bool {
        match self.try_swap_read_only_generation() {
            Ok(swapped) => swapped,
            Err(error) => {
                warn!(file_name = %self.file_name, error = %error, "failed to replace sealed mapped generation");
                false
            }
        }
    }

    #[inline]
    fn clean_swaped_map(&self, force: bool) {
        let snapshot = self.swap_generation_snapshot();
        let _ = self.try_clean_swapped_generation(snapshot, force);
    }

    #[inline]
    fn get_recent_swap_map_time(&self) -> i64 {
        self.swap_generation_snapshot().time_millis()
    }

    #[inline]
    fn get_mapped_byte_buffer_access_count_since_last_swap(&self) -> i64 {
        self.mapped_byte_buffer_access_count_since_last_swap
            .load(Ordering::Acquire)
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
        let Ok(admission) = self.acquire_borrowed(MappedFileOperation::Read) else {
            return false;
        };
        self.is_loaded_admitted(&admission, position, size)
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

        let Ok(admission) = self.acquire_borrowed(MappedFileOperation::Read) else {
            return false;
        };
        self.is_loaded_admitted(&admission, position, size)
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

        let Ok(admission) = self.acquire_borrowed(MappedFileOperation::Read) else {
            return false;
        };
        self.is_loaded_admitted(&admission, position, size)
    }

    fn select_mapped_buffer_with_position(&self, pos: i32) -> Option<SelectMappedBufferResult<M>> {
        self.try_select_mapped_buffer_with_position(pos).ok().flatten()
    }

    fn try_select_mapped_buffer_with_position(
        &self,
        pos: i32,
    ) -> MappedFileResult<Option<SelectMappedBufferResult<M>>> {
        let admission = self.acquire_owned(MappedFileOperation::Read)?;
        let read_position = self.get_read_position();
        let Some(size) = self.raw_core.readable_tail_size(pos, read_position) else {
            return Ok(None);
        };
        self.mapped_byte_buffer_access_count_since_last_swap
            .fetch_add(1, Ordering::AcqRel);
        let is_in_cache = self.record_cache_residency_admitted(&admission, pos as i64, size as usize);

        Ok(self
            .copy_range_admitted(&admission, pos as usize, size as usize, None)?
            .and_then(|bytes| {
                SelectMappedBufferResult::from_bytes_with_metadata(
                    self.get_file_from_offset() + pos as u64,
                    pos as u64,
                    bytes,
                    is_in_cache,
                    SelectMappedBufferCacheState::from_residency(is_in_cache),
                )
            }))
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

    fn get_slice(&self, pos: usize, size: usize) -> Option<Bytes> {
        self.try_get_slice(pos, size).ok().flatten()
    }

    fn try_get_slice(&self, pos: usize, size: usize) -> MappedFileResult<Option<Bytes>> {
        let admission = self.acquire_borrowed(MappedFileOperation::Read)?;
        let Some(end) = pos.checked_add(size) else {
            return Ok(None);
        };
        if pos >= self.raw_core.file_size() as usize || end >= self.raw_core.file_size() as usize {
            return Ok(None);
        }
        let Some(slice) = self.copy_range_admitted(&admission, pos, size, None)? else {
            return Ok(None);
        };
        if let Some(metrics) = &self.metrics {
            metrics.record_read(size, false);
        }
        Ok(Some(slice))
    }
}

#[allow(unused_variables)]
impl<M: MappedMemory> DefaultMappedFile<M> {
    #[cfg(target_os = "linux")]
    fn is_loaded_admitted<L: MappedFileLeaseProof + ?Sized>(&self, admission: &L, position: i64, size: usize) -> bool {
        if !self.is_valid_cache_range(position, size) {
            return false;
        }
        let _writer = self.write_state.lock();
        let Ok(generation) = self.try_get_read_generation(admission, MappedFileOperation::Read) else {
            return false;
        };
        let page_size = get_page_size();
        generation.with_slice(|mapped| {
            let base_addr = mapped.as_ptr() as usize;
            let Some(plan) =
                crate::mapped_file::kernel::plan_mapped_file_cache_residency(base_addr, position, size, page_size)
            else {
                return false;
            };
            let Some(offset) = plan.aligned_start.checked_sub(base_addr) else {
                return false;
            };
            let Some(end) = offset.checked_add(plan.checked_len) else {
                return false;
            };
            let Some(region) = mapped.get(offset..end) else {
                return false;
            };
            match memory_residency(region) {
                Ok(residency) => residency.len() == plan.page_count && residency.iter().all(|page| page & 1 == 1),
                Err(_) => false,
            }
        })
    }

    #[cfg(any(target_os = "windows", target_os = "macos"))]
    fn is_loaded_admitted<L: MappedFileLeaseProof + ?Sized>(&self, admission: &L, position: i64, size: usize) -> bool {
        self.validate_lease(admission, MappedFileOperation::Read).is_ok() && self.is_valid_cache_range(position, size)
    }

    /// Runs `callback` while a borrowed read lease keeps the mapping admitted.
    ///
    /// The callback's return type cannot borrow from the provided slice, so mapped bytes cannot
    /// escape the lease scope through this safe API.
    ///
    /// # Errors
    ///
    /// Returns an unavailable error after lifecycle closing begins, or a mapping error when the
    /// backing mapping cannot be initialized.
    pub fn with_mapped_slice<R>(&self, callback: impl FnOnce(&[u8]) -> R) -> MappedFileResult<R> {
        let admission = self.acquire_borrowed(MappedFileOperation::Read)?;
        let _writer = self.write_state.lock();
        let generation = self.try_get_read_generation(&admission, MappedFileOperation::Read)?;
        Ok(generation.with_slice(callback))
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

    fn advise_mapped_file(memory: &[u8], advice: MemoryAdvice) -> io::Result<()> {
        advise_memory(memory, advice)
    }

    /// Applies an operating-system memory-access hint while excluding mapped-file mutation.
    ///
    /// # Errors
    ///
    /// Returns the platform error reported by the advice operation.
    pub fn apply_memory_advice(&self, advice: MemoryAdvice) -> io::Result<()> {
        let admission = self
            .acquire_borrowed(MappedFileOperation::Maintenance)
            .map_err(|error| io::Error::new(io::ErrorKind::NotConnected, error))?;
        let _writer = self.write_state.lock();
        let generation = self
            .try_get_read_generation(&admission, MappedFileOperation::Maintenance)
            .map_err(io::Error::other)?;
        generation.with_slice(|mapped| Self::advise_mapped_file(mapped, advice))
    }

    fn warm_mapped_file_with_ops<L, T, F, A, R>(
        &self,
        admission: &L,
        flush_disk_type: FlushDiskType,
        pages: usize,
        mut touch_page: T,
        mut flush_range: F,
        mut advise: A,
        mut record_degradation: R,
    ) -> MappedFileResult<()>
    where
        L: MappedFileLeaseProof + ?Sized,
        T: FnMut(*mut u8, usize) -> io::Result<()>,
        F: FnMut(&M, usize, usize) -> io::Result<()>,
        A: FnMut(&[u8], MemoryAdvice) -> io::Result<()>,
        R: FnMut(LinuxStorageDegradationEvent),
    {
        let _writer = self.write_state.lock();
        let file_size = self.raw_core.file_size() as usize;
        if file_size == 0 {
            return Ok(());
        }

        let warmup_started = Instant::now();
        let generation = self.try_get_writable_generation(admission, MappedFileOperation::Write)?;
        generation.with_mapping(|mapped_file| {
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

            if let Err(error) = advise(mapped_file.as_slice(), MemoryAdvice::WillNeed) {
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
        });
        let warmup_duration = warmup_started.elapsed();
        let warmup_millis = u64::try_from(warmup_duration.as_millis()).unwrap_or(u64::MAX);
        #[cfg(feature = "observability")]
        self.store_metrics.record_linux_page_cache_warmup_millis(warmup_millis);
        #[cfg(not(feature = "observability"))]
        let _ = warmup_millis;
        if let Some(metrics) = &self.metrics {
            metrics.record_warm_with_latency(file_size, warmup_duration);
        }
        Ok(())
    }

    pub fn lock_region(
        &self,
        memory_lock_manager: &MemoryLockManager,
        category: MemoryLockCategory,
        offset: u64,
        len: usize,
    ) -> RocketMQResult<Option<MemoryLockHandle>> {
        let Some((region, admission)) = self.lock_region_owner(offset, len) else {
            return Ok(None);
        };
        let _writer = self.write_state.lock();
        let mut handle = memory_lock_manager.lock_owned_region(category, region)?;
        if let Some(handle) = handle.as_mut() {
            handle.attach_mapped_file_lease(admission);
        }
        Ok(handle)
    }

    pub fn lock_region_with<F>(
        &self,
        memory_lock_manager: &MemoryLockManager,
        category: MemoryLockCategory,
        offset: u64,
        len: usize,
        mut locker: F,
    ) -> RocketMQResult<Option<MemoryLockHandle>>
    where
        F: FnMut(&[u8]) -> RocketMQResult<()>,
    {
        let Some((region, admission)) = self.lock_region_owner(offset, len) else {
            return Ok(None);
        };
        let _writer = self.write_state.lock();
        let mut handle = memory_lock_manager.lock_owned_region_with(category, region, |address, len| {
            // SAFETY: `region` owns the complete checked generation range, and `write_state`
            // excludes pointer-based mutation for this compatibility callback.
            locker(unsafe { std::slice::from_raw_parts(address, len) })
        })?;
        if let Some(handle) = handle.as_mut() {
            handle.attach_mapped_file_lease(admission);
        }
        Ok(handle)
    }

    pub fn unlock_region(
        &self,
        memory_lock_manager: &MemoryLockManager,
        handle: &mut MemoryLockHandle,
    ) -> RocketMQResult<()> {
        memory_lock_manager.unlock_region(handle)
    }

    pub fn unlock_region_with<F>(
        &self,
        memory_lock_manager: &MemoryLockManager,
        handle: &mut MemoryLockHandle,
        mut unlocker: F,
    ) -> RocketMQResult<()>
    where
        F: FnMut(&[u8]) -> RocketMQResult<()>,
    {
        let _writer = self.write_state.lock();
        memory_lock_manager.unlock_owned_region_with(handle, |address, len| {
            // SAFETY: the armed handle retains the same checked generation range, and
            // `write_state` excludes pointer-based mutation for this compatibility callback.
            unlocker(unsafe { std::slice::from_raw_parts(address, len) })
        })
    }

    fn lock_region_owner(
        &self,
        offset: u64,
        requested_len: usize,
    ) -> Option<(MappedMemoryLockRegion<M>, MappedFileLease)> {
        let (offset, len) = self.raw_core.lock_region_range(offset, requested_len)?;
        let admission = self.acquire_owned(MappedFileOperation::Maintenance).ok()?;
        let region = match self
            .try_get_read_generation(&admission, MappedFileOperation::Maintenance)
            .ok()?
        {
            AdmittedReadGeneration::Writable(generation) => {
                MappedMemoryLockRegion::Writable(generation.maintenance_region(offset, len).ok()?)
            }
            AdmittedReadGeneration::ReadOnly(generation) => {
                MappedMemoryLockRegion::ReadOnly(generation.region(offset, len).ok()?)
            }
        };
        Some((region, admission))
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
                "physical detach rejected for available file[REF:{}] {}",
                current_ref, self.file_name
            );
            return false;
        }

        match self.try_detach_physical_owners() {
            MappedFileDetachOutcome::Detached {
                mapping_generation,
                had_file_owner,
            } => {
                info!(
                    file_name = %self.file_name,
                    current_ref,
                    mapping_generation,
                    had_file_owner,
                    "mapped-file physical owner slots detached"
                );
                true
            }
            MappedFileDetachOutcome::AlreadyDetached => true,
            MappedFileDetachOutcome::InProgress => false,
            MappedFileDetachOutcome::Pending { state, active_leases } => {
                error!(
                    file_name = %self.file_name,
                    current_ref,
                    state = %state,
                    active_leases,
                    "mapped-file physical detach is still pending"
                );
                false
            }
        }
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
        self.metrics.as_deref()
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

    /// Copies a mapped range into an immutable snapshot.
    ///
    /// # Arguments
    ///
    /// * `pos` - Starting position in the file
    /// * `size` - Number of bytes to read
    ///
    /// # Returns
    ///
    /// `Some(Bytes)` containing the requested data, or `None` if out of bounds.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let data = mapped_file.get_bytes_snapshot(0, 16384)?;
    /// process_message(&data);
    /// ```
    pub fn get_bytes_snapshot(&self, pos: usize, size: usize) -> Option<Bytes> {
        if let Some(metrics) = &self.metrics {
            metrics.record_read(size, false);
        }
        self.try_copy_range(pos, size, None).ok().flatten()
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
        self.try_flush_range(start, end).unwrap_or(0)
    }

    /// Flushes a specific range while preserving lifecycle and I/O failures.
    pub fn try_flush_range(&self, start: usize, end: usize) -> MappedFileResult<i32> {
        use std::time::Instant;

        let admission = self.acquire_borrowed(MappedFileOperation::Maintenance)?;
        let Some((start, len)) = self.raw_core.prepare_flush_range(start, end) else {
            return Ok(0);
        };
        let _writer = self.write_state.lock();

        let flush_start = Instant::now();
        if self.physical_owners.mapping.load_read_only().is_none() {
            self.try_get_writable_generation(&admission, MappedFileOperation::Maintenance)?
                .with_mapping(|mapping| mapping.flush_range(start, len).map_err(MappedFileError::FlushFailed))?;
        }

        if self.transient_store_pool.is_some() {
            self.raw_core.record_transient_flush_range(end);
        }

        if let Some(metrics) = &self.metrics {
            metrics.record_flush(flush_start.elapsed());
        }

        Ok(len as i32)
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
    use std::sync::Arc;
    use std::time::Duration;

    use memmap2::Mmap;
    use memmap2::MmapMut;
    use tempfile::TempDir;

    use super::*;
    use crate::base::memory_lock_manager::MemoryLockCategory;
    use crate::base::memory_lock_manager::MemoryLockManager;

    struct TestMappedMemory {
        mmap: MmapMut,
    }

    struct TestReadOnlyMappedMemory {
        mmap: Mmap,
    }

    // SAFETY: the backend owns one stable mapping and tests serialize mutable access through
    // `DefaultMappedFile`.
    unsafe impl MappedMemory for TestMappedMemory {
        type ReadOnly = TestReadOnlyMappedMemory;

        unsafe fn map_mut(file: &File) -> io::Result<Self> {
            // SAFETY: `MappedFileStorage` sizes the file before invoking the backend and never
            // resizes it while this mapping is live.
            let mmap = unsafe { MmapMut::map_mut(file)? };
            Ok(Self { mmap })
        }

        fn as_slice(&self) -> &[u8] {
            &self.mmap
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
    }

    // SAFETY: the mapping is immutable, stable, and owned for the complete value lifetime.
    unsafe impl ReadOnlyMappedMemory for TestReadOnlyMappedMemory {
        unsafe fn map(file: &File) -> io::Result<Self> {
            // SAFETY: test storage keeps the file size stable while this mapping is live.
            let mmap = unsafe { Mmap::map(file)? };
            Ok(Self { mmap })
        }

        fn as_slice(&self) -> &[u8] {
            &self.mmap
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
        transient_store_pool
            .init_with_locker(|_| Ok(()))
            .expect("transient pool initializes");
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
    fn test_get_bytes_snapshot() {
        let (_temp_dir, mapped_file) = create_test_file();

        let data = mapped_file.get_bytes_snapshot(0, 100);
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

        assert_eq!(mapped_file.with_mapped_slice(<[u8]>::len).unwrap(), 4096);

        let stats = mapped_file.lazy_mmap_stats();
        assert!(mapped_file.is_mapped());
        assert_eq!(stats.eligible_files, 1);
        assert_eq!(stats.mapped_files, 1);
        assert_eq!(stats.map_operations, 1);
        assert_eq!(stats.map_failures, 0);
    }

    #[test]
    fn lazy_write_initializes_owned_generation_once_then_reuses_published_slot() {
        let (_temp_dir, mapped_file) = create_lazy_test_file();

        assert!(mapped_file.append_message_bytes(b"lazy"));
        assert!(mapped_file.append_message_bytes(b"-mapped"));

        assert_eq!(mapped_file.lazy_mmap_stats().map_operations, 1);
        assert_eq!(mapped_file.get_wrote_position(), 11);
        assert_eq!(
            mapped_file.get_bytes_readable_checked(0, 11).as_deref(),
            Some(&b"lazy-mapped"[..])
        );
    }

    #[test]
    fn lazy_mmap_accepts_owned_selection_lease() {
        let (_temp_dir, mapped_file) = create_lazy_test_file();
        mapped_file.set_wrote_position(1);

        assert!(mapped_file
            .try_select_mapped_buffer(0, 1)
            .expect("owned selection admission")
            .is_some());
        assert!(mapped_file.is_mapped());
        assert_eq!(mapped_file.lazy_mmap_stats().map_operations, 1);
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
            assert!(MappedFile::reserve_write(&mapped_file, 0).is_err());
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
        assert_eq!(mapped_file.with_mapped_slice(<[u8]>::len).unwrap(), 4096);
        assert_eq!(mapped_file.lazy_mmap_stats(), LazyMmapStats::default());
    }

    #[test]
    fn with_mapped_slice_holds_read_admission_only_for_callback_scope() {
        let (_temp_dir, mapped_file) = create_test_file();
        let baseline = mapped_file.lifecycle_snapshot().active_leases;

        let mapped_len = mapped_file
            .with_mapped_slice(|mapped| {
                assert_eq!(mapped_file.lifecycle_snapshot().active_leases, baseline + 1);
                mapped.len()
            })
            .expect("active mapped file admits scoped read");

        assert_eq!(mapped_len, 4096);
        assert_eq!(mapped_file.lifecycle_snapshot().active_leases, baseline);
    }

    #[test]
    fn with_mapped_slice_and_rename_fail_closed_after_shutdown() {
        let (temp_dir, mut mapped_file) = create_test_file();
        let original = temp_dir.path().join("00000000000000000000");
        let renamed = temp_dir.path().join("renamed-after-close");
        MappedFile::shutdown(&mapped_file, 0);

        let error = mapped_file
            .with_mapped_slice(|_| ())
            .expect_err("closing mapped file must reject scoped reads");
        assert!(matches!(
            error,
            MappedFileError::Unavailable {
                operation: MappedFileOperation::Read,
                ..
            }
        ));
        assert!(!mapped_file.rename_to(renamed.to_str().expect("UTF-8 path")));
        assert!(original.exists());
        assert!(!renamed.exists());
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
        assert_eq!(
            std::fs::metadata(mapped_file.get_file_name().as_str()).unwrap().len(),
            4096
        );
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
        assert_eq!(
            std::fs::metadata(mapped_file.get_file_name().as_str()).unwrap().len(),
            4096
        );
    }

    #[test]
    fn test_get_bytes_snapshot_out_of_bounds() {
        let (_temp_dir, mapped_file) = create_test_file();

        let data = mapped_file.get_bytes_snapshot(0, 10000);
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

        mapped_file.get_bytes_snapshot(0, 100);
        mapped_file.get_bytes_snapshot(100, 200);

        // Check metrics
        let metrics = mapped_file.get_metrics().unwrap();
        assert_eq!(metrics.total_reads(), 2);
        assert_eq!(metrics.total_bytes_read(), 300);

        assert_eq!(metrics.zero_copy_read_percentage(), 0.0);
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
        mapped_file.get_bytes_snapshot(0, 1024);
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
        let (_temp_dir, pool, mapped_file) = create_transient_test_file();

        assert!(mapped_file.append_message_bytes(b"hello transient"));
        assert_eq!(mapped_file.get_wrote_position(), 15);
        assert_eq!(mapped_file.get_committed_position(), 0);
        assert_eq!(mapped_file.get_read_position(), 0);
        assert_eq!(mapped_file.get_bytes(0, 15), Some(Bytes::from_static(&[0; 15])));
        assert_eq!(pool.outstanding_lease_count(), 1);

        assert_eq!(mapped_file.commit(0), 15);
        assert_eq!(mapped_file.get_committed_position(), 15);
        assert_eq!(mapped_file.get_read_position(), 15);
        assert_eq!(pool.outstanding_lease_count(), 0);
        assert_eq!(pool.available_buffer_nums(), 1);

        assert_eq!(mapped_file.flush(0), 15);
        assert_eq!(mapped_file.get_flushed_position(), 15);
        assert_eq!(
            mapped_file.get_bytes_readable_checked(0, 15).unwrap(),
            Bytes::from_static(b"hello transient")
        );
    }

    #[test]
    fn transient_constructor_failure_returns_the_borrowed_pool_lease() {
        let temp_dir = TempDir::new().expect("temporary directory");
        let not_a_directory = temp_dir.path().join("not-a-directory");
        std::fs::write(&not_a_directory, b"file").expect("blocking parent file");
        let file_name = CheetahString::from(
            not_a_directory
                .join("00000000000000000000")
                .to_str()
                .expect("UTF-8 test path"),
        );
        let pool = TransientStorePool::new(1, 4096);
        pool.init_with_locker(|_| Ok(())).expect("pool initializes");

        let result = TestDefaultMappedFile::try_new_with_transient_store_pool(file_name, 4096, pool.clone());

        assert!(result.is_err());
        assert_eq!(pool.outstanding_lease_count(), 0);
        assert_eq!(pool.available_buffer_nums(), 1);
    }

    #[test]
    fn transient_mapped_file_drop_returns_an_uncommitted_pool_lease() {
        let (_temp_dir, pool, mapped_file) = create_transient_test_file();
        assert!(mapped_file.append_message_bytes(b"uncommitted"));
        assert_eq!(pool.outstanding_lease_count(), 1);

        drop(mapped_file);

        assert_eq!(pool.outstanding_lease_count(), 0);
        assert_eq!(pool.available_buffer_nums(), 1);
    }

    #[test]
    fn transient_mapped_file_late_drop_does_not_requeue_after_pool_shutdown() {
        let (_temp_dir, pool, mapped_file) = create_transient_test_file();
        assert_eq!(pool.outstanding_lease_count(), 1);

        let report = pool.shutdown(Duration::ZERO).expect("pool enters shutdown");
        assert_eq!(report.outstanding_leases(), 1);
        assert!(report.timed_out());

        drop(mapped_file);

        assert_eq!(pool.outstanding_lease_count(), 0);
        assert_eq!(pool.available_buffer_nums(), 0);
    }

    #[test]
    fn failed_flush_keeps_the_last_durable_position_and_preserves_io_cause() {
        let (_temp_dir, mapped_file) = create_test_file();
        assert!(mapped_file.append_message_bytes(b"not-yet-durable"));
        let durable_before = mapped_file.get_flushed_position();

        let error = mapped_file
            .try_flush_with(0, |_, _, _, _| {
                Err(MappedFileError::FlushFailed(io::Error::from_raw_os_error(5)))
            })
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
    fn active_writable_swap_is_rejected_without_fake_bookkeeping() {
        let (_temp_dir, mapped_file) = create_test_file();
        mapped_file
            .mapped_byte_buffer_access_count_since_last_swap
            .store(3, Ordering::Release);
        let before = mapped_file.get_recent_swap_map_time();

        assert!(!mapped_file.swap_map());
        assert_eq!(mapped_file.get_mapped_byte_buffer_access_count_since_last_swap(), 3);
        assert_eq!(mapped_file.get_recent_swap_map_time(), before);
        assert_eq!(mapped_file.get_metrics().unwrap().swap_operations(), 0);
    }

    #[test]
    fn sealed_swap_publishes_once_and_old_generation_lives_until_last_lease_drop() {
        let (_temp_dir, mapped_file) = create_test_file();
        assert!(mapped_file.append_message_bytes(b"generation"));
        assert!(mapped_file.try_seal_readable().expect("segment seals"));
        let old = mapped_file
            .try_mapped_read_lease(0, 10)
            .expect("read admission")
            .expect("sealed range");
        let old_generation = old.generation_id();

        assert!(mapped_file.swap_map());

        let current = mapped_file
            .try_mapped_read_lease(0, 10)
            .expect("read admission")
            .expect("replacement range");
        assert!(current.generation_id().get() > old_generation.get());
        assert_eq!(old.as_ref(), b"generation");
        assert_eq!(current.as_ref(), b"generation");
        assert_eq!(mapped_file.retired_swap_observation_count(), 1);
        assert_eq!(mapped_file.get_metrics().unwrap().mapped_generations_live(), 2);

        let snapshot = mapped_file.swap_generation_snapshot();
        assert!(!mapped_file.try_clean_swapped_generation(snapshot, true));
        drop(old);
        assert!(mapped_file.try_clean_swapped_generation(snapshot, true));
        assert_eq!(mapped_file.retired_swap_observation_count(), 0);
        assert_eq!(mapped_file.get_metrics().unwrap().mapped_generations_live(), 1);
    }

    #[test]
    fn stale_swap_snapshot_cannot_clean_new_generation() {
        let (_temp_dir, mapped_file) = create_test_file();
        assert!(mapped_file.append_message_bytes(b"generation"));
        assert!(mapped_file.try_seal_readable().expect("segment seals"));
        let stale = mapped_file.swap_generation_snapshot();
        assert!(mapped_file.swap_map());
        mapped_file
            .mapped_byte_buffer_access_count_since_last_swap
            .store(7, Ordering::Release);

        assert!(!mapped_file.try_clean_swapped_generation(stale, true));
        assert_eq!(mapped_file.get_mapped_byte_buffer_access_count_since_last_swap(), 7);
        assert_eq!(mapped_file.get_metrics().unwrap().clean_swap_operations(), 0);

        let current = mapped_file.swap_generation_snapshot();
        assert!(mapped_file.try_clean_swapped_generation(current, true));
        assert_eq!(mapped_file.get_mapped_byte_buffer_access_count_since_last_swap(), 0);
        assert_eq!(mapped_file.get_metrics().unwrap().clean_swap_operations(), 1);
    }

    #[test]
    fn concurrent_cleanup_claim_is_exactly_once_per_generation() {
        let (_temp_dir, mapped_file) = create_test_file();
        assert!(mapped_file.append_message_bytes(b"generation"));
        assert!(mapped_file.try_seal_readable().expect("segment seals"));
        assert!(mapped_file.swap_map());
        let mapped_file = Arc::new(mapped_file);
        let snapshot = mapped_file.swap_generation_snapshot();
        let workers = (0..8)
            .map(|_| {
                let mapped_file = Arc::clone(&mapped_file);
                std::thread::spawn(move || mapped_file.try_clean_swapped_generation(snapshot, true))
            })
            .collect::<Vec<_>>();

        let winners = workers
            .into_iter()
            .map(|worker| worker.join().expect("cleanup worker"))
            .filter(|cleaned| *cleaned)
            .count();
        assert_eq!(winners, 1);
        assert_eq!(mapped_file.get_metrics().unwrap().clean_swap_operations(), 1);
    }

    #[test]
    fn new_swap_reopens_cleanup_after_prior_generation_was_cleaned() {
        let (_temp_dir, mapped_file) = create_test_file();
        assert!(mapped_file.append_message_bytes(b"generation"));
        assert!(mapped_file.try_seal_readable().expect("segment seals"));
        assert!(mapped_file.swap_map());
        let first = mapped_file.swap_generation_snapshot();
        assert!(mapped_file.try_clean_swapped_generation(first, true));
        assert!(!mapped_file.try_clean_swapped_generation(first, true));

        assert!(mapped_file.swap_map());
        let next = mapped_file.swap_generation_snapshot();
        assert!(mapped_file.try_clean_swapped_generation(next, true));
        assert_eq!(mapped_file.get_metrics().unwrap().clean_swap_operations(), 2);
    }

    #[test]
    fn select_mapped_buffer_records_page_cache_residency() {
        let (_temp_dir, mapped_file) = create_test_file();
        assert!(mapped_file.append_message_bytes(b"cache-residency"));
        let reference_count = ReferenceResource::get_ref_count(&mapped_file);

        let result = mapped_file
            .select_mapped_buffer(0, "cache-residency".len() as i32)
            .expect("selected buffer should exist");

        assert_eq!(ReferenceResource::get_ref_count(&mapped_file), reference_count);
        let metrics = mapped_file.get_metrics().unwrap();
        assert_eq!(metrics.cache_hits() + metrics.cache_misses(), 1);
        assert_eq!(result.is_in_cache(), metrics.cache_hits() == 1);
        assert_eq!(
            result.source_kind(),
            crate::mapped_file::SelectMappedBufferSourceKind::Bytes
        );
        assert_eq!(result.file_offset(), 0);
        assert_eq!(
            result.cache_state(),
            SelectMappedBufferCacheState::from_residency(result.is_in_cache())
        );
    }

    #[test]
    fn attached_selection_owns_and_releases_exactly_one_hold() {
        let (_temp_dir, mapped_file) = create_test_file();
        let mapped_file = Arc::new(mapped_file);
        assert!(mapped_file.append_message_bytes(b"attached"));
        let reference_count = ReferenceResource::get_ref_count(mapped_file.as_ref());
        let mut result = mapped_file
            .select_mapped_buffer(0, "attached".len() as i32)
            .expect("selected buffer should exist");

        assert!(result.try_attach_mapped_file(Arc::clone(&mapped_file)));
        assert_eq!(
            ReferenceResource::get_ref_count(mapped_file.as_ref()),
            reference_count + 1
        );
        assert_eq!(
            result.source_kind(),
            crate::mapped_file::SelectMappedBufferSourceKind::MappedFile
        );

        drop(result);
        assert_eq!(ReferenceResource::get_ref_count(mapped_file.as_ref()), reference_count);
    }

    #[test]
    fn warm_and_clean_swap_are_reflected_in_metrics() {
        let (_temp_dir, mapped_file) = create_test_file();

        mapped_file.warm_mapped_file(FlushDiskType::AsyncFlush, 1);
        assert!(mapped_file.append_message_bytes(b"generation"));
        assert!(mapped_file.try_seal_readable().expect("segment seals"));
        assert!(mapped_file.swap_map());
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

        let admission = mapped_file.acquire_borrowed(MappedFileOperation::Write).unwrap();
        mapped_file
            .warm_mapped_file_with_ops(
                &admission,
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
                |_, _| Err(io::Error::from_raw_os_error(12)),
                |event| events.push(event),
            )
            .unwrap();

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
        let expected_addr = mapped_file
            .with_mapped_slice(|mapped| mapped.as_ptr().wrapping_add(3072))
            .unwrap();

        let mut handle = mapped_file
            .lock_region_with(
                &manager,
                MemoryLockCategory::CommitLogActiveWindow,
                3072,
                4096,
                |memory| {
                    assert_eq!(memory.as_ptr(), expected_addr);
                    assert_eq!(memory.len(), 1024);
                    Ok(())
                },
            )
            .expect("range lock should not fail")
            .expect("clamped non-empty range should return handle");

        assert_eq!(handle.category(), MemoryLockCategory::CommitLogActiveWindow);
        assert_eq!(handle.len(), 1024);
        assert_eq!(manager.locked_bytes(), 1024);

        mapped_file
            .unlock_region_with(&manager, &mut handle, |memory| {
                assert_eq!(memory.as_ptr(), expected_addr);
                assert_eq!(memory.len(), 1024);
                Ok(())
            })
            .expect("range unlock should not fail");
        assert_eq!(manager.locked_bytes(), 0);
    }

    #[test]
    fn mapped_lock_handle_retains_maintenance_admission_until_unlock() {
        let (_temp_dir, mapped_file) = create_test_file();
        let manager = MemoryLockManager::warn_only_with_budget(4096);
        let mut handle = mapped_file
            .lock_region_with(&manager, MemoryLockCategory::CommitLogActiveWindow, 0, 4096, |_| Ok(()))
            .expect("range lock should not fail")
            .expect("successful mapped lock returns a handle");

        assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 1);
        MappedFile::shutdown(&mapped_file, u64::MAX);
        assert!(!mapped_file.lifecycle_snapshot().logical_cleanup_marked);

        mapped_file
            .unlock_region_with(&manager, &mut handle, |_| {
                Err(rocketmq_error::RocketMQError::StorageLockFailed {
                    path: "retryable unlock failure".to_string(),
                })
            })
            .expect_err("failed unlock must retain mapped-file admission");
        assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 1);
        assert!(!mapped_file.lifecycle_snapshot().logical_cleanup_marked);

        mapped_file
            .unlock_region_with(&manager, &mut handle, |_| Ok(()))
            .expect("range unlock should release admission");
        assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 0);
        assert!(mapped_file.lifecycle_snapshot().logical_cleanup_marked);
        assert_eq!(manager.locked_bytes(), 0);
    }

    #[test]
    fn dropping_mapped_lock_handle_releases_maintenance_admission() {
        let (_temp_dir, mapped_file) = create_test_file();
        let manager = MemoryLockManager::warn_only_with_budget(4096);
        let handle = mapped_file
            .lock_region_with(&manager, MemoryLockCategory::CommitLogActiveWindow, 0, 4096, |_| Ok(()))
            .expect("range lock should not fail")
            .expect("successful mapped lock returns a handle");

        MappedFile::shutdown(&mapped_file, u64::MAX);
        assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 1);
        drop(handle);
        assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 0);
        assert!(mapped_file.lifecycle_snapshot().logical_cleanup_marked);
    }

    #[test]
    fn closing_rejects_new_mapped_locks_without_advancing_manager_metrics() {
        let (_temp_dir, mapped_file) = create_test_file();
        let manager = MemoryLockManager::warn_only_with_budget(4096);
        MappedFile::shutdown(&mapped_file, 0);
        let before = (
            manager.lock_attempt_count(),
            manager.locked_buffer_count(),
            manager.lock_failed_buffer_count(),
            manager.lock_skipped_buffer_count(),
            manager.locked_bytes(),
            manager.lock_failed_bytes(),
            manager.lock_skipped_bytes(),
        );
        let mut locker_called = false;

        let handle = mapped_file
            .lock_region_with(&manager, MemoryLockCategory::CommitLogActiveWindow, 0, 4096, |_| {
                locker_called = true;
                Ok(())
            })
            .expect("compatibility lock reports lifecycle rejection as no handle");

        assert!(handle.is_none());
        assert!(!locker_called);
        assert_eq!(
            before,
            (
                manager.lock_attempt_count(),
                manager.locked_buffer_count(),
                manager.lock_failed_buffer_count(),
                manager.lock_skipped_buffer_count(),
                manager.locked_bytes(),
                manager.lock_failed_bytes(),
                manager.lock_skipped_bytes(),
            )
        );
    }

    #[test]
    fn lock_region_skips_zero_length_and_out_of_range_requests() {
        let (_temp_dir, mapped_file) = create_test_file();
        let manager = MemoryLockManager::warn_only_with_budget(4096);

        let zero_len = mapped_file
            .lock_region_with(&manager, MemoryLockCategory::CommitLogActiveWindow, 0, 0, |_| {
                panic!("zero-length request must not call locker")
            })
            .expect("zero-length request should be accepted as a no-op");
        let out_of_range = mapped_file
            .lock_region_with(
                &manager,
                MemoryLockCategory::CommitLogActiveWindow,
                mapped_file.get_file_size(),
                1024,
                |_| panic!("out-of-range request must not call locker"),
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
