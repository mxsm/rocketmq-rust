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

use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::ops::Deref;
use std::ops::Range;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwapOption;
use parking_lot::Mutex;

use super::file::FileOwner;
use super::lifecycle::MappedFileLease;
use super::mapping::LazyMmapStats;
use super::memory::checked_mmap_range;
use super::memory::MappedMemory;
use super::memory::MmapRangeError;
use super::memory::ReadOnlyMappedMemory;
use super::metrics::MappedFileMetrics;
use super::metrics::MappingGenerationGaugeGuard;

/// Stable, non-zero identity of one published or fully built mapping generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MappingGenerationId(NonZeroU64);

impl MappingGenerationId {
    /// First identity assigned by a fresh mapping slot.
    pub const FIRST: Self = Self(NonZeroU64::MIN);

    /// Returns the numeric generation identity.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// A mapping generation identity could not be assigned without reusing zero or an older value.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
#[error("mapping generation identity space is exhausted")]
pub struct MappingGenerationIdExhausted;

/// Monotonic identity source owned by one authoritative mapping slot.
struct MappingGenerationIdSequence {
    last_assigned: AtomicU64,
}

impl MappingGenerationIdSequence {
    const fn new() -> Self {
        Self::with_last_assigned(0)
    }

    const fn with_last_assigned(last_assigned: u64) -> Self {
        Self {
            last_assigned: AtomicU64::new(last_assigned),
        }
    }

    fn next(&self) -> Result<MappingGenerationId, MappingGenerationIdExhausted> {
        let previous = self
            .last_assigned
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |last| last.checked_add(1))
            .map_err(|_| MappingGenerationIdExhausted)?;
        let next = previous.checked_add(1).ok_or(MappingGenerationIdExhausted)?;
        NonZeroU64::new(next)
            .map(MappingGenerationId)
            .ok_or(MappingGenerationIdExhausted)
    }
}

/// Type-state marker for a generation that may be mutated under the mapped-file writer fence.
pub enum WritableAccess {}

/// Type-state marker for a sealed generation that exposes immutable owner-bound regions.
pub enum ReadOnlyAccess {}

/// Physical owner of one mmap generation.
///
/// The generation owns the concrete mapping, the canonical file owner, and its live-gauge guard.
/// It intentionally does not implement `Clone`; consumers clone the enclosing [`Arc`] instead.
pub struct MappingGeneration<M, Access> {
    id: MappingGenerationId,
    mapping: M,
    file_owner: Arc<FileOwner>,
    mapped_bytes: usize,
    _gauge: MappingGenerationGaugeGuard,
    _access: PhantomData<Access>,
}

/// Writable mapping generation used while a segment accepts writers.
pub type WritableMappingGeneration<W> = MappingGeneration<W, WritableAccess>;

/// Read-only mapping generation used after a segment has been sealed.
pub type ReadOnlyMappingGeneration<R> = MappingGeneration<R, ReadOnlyAccess>;

impl<M, Access> MappingGeneration<M, Access> {
    /// Returns this generation's stable identity.
    #[inline]
    pub const fn id(&self) -> MappingGenerationId {
        self.id
    }

    /// Returns the number of bytes owned by this mapping generation.
    #[inline]
    pub const fn mapped_bytes(&self) -> usize {
        self.mapped_bytes
    }

    #[inline]
    pub(crate) fn file_owner(&self) -> &Arc<FileOwner> {
        &self.file_owner
    }
}

impl<W: MappedMemory> WritableMappingGeneration<W> {
    pub(crate) fn new(
        id: MappingGenerationId,
        mapping: W,
        file_owner: Arc<FileOwner>,
        metrics: Arc<MappedFileMetrics>,
    ) -> Self {
        let mapped_bytes = mapping.as_slice().len();
        let gauge = metrics.track_mapping_generation(mapped_bytes);
        Self {
            id,
            mapping,
            file_owner,
            mapped_bytes,
            _gauge: gauge,
            _access: PhantomData,
        }
    }

    /// Runs one synchronous operation against the writable mapping without allowing a borrowed
    /// mapping reference to become the closure's return value.
    #[inline]
    pub(crate) fn with_mapping<T>(&self, operation: impl for<'a> FnOnce(&'a W) -> T) -> T {
        operation(&self.mapping)
    }

    /// Creates an owner-bound range for internal maintenance such as memory locking.
    ///
    /// Unlike read-only regions, this range exposes no safe byte slice. Callers that need an
    /// address must keep the mapped-file writer fence for the complete native operation.
    pub(crate) fn maintenance_region(
        self: &Arc<Self>,
        offset: usize,
        len: usize,
    ) -> Result<GenerationRegion<W, WritableAccess>, MmapRangeError> {
        GenerationRegion::try_new_writable(Arc::clone(self), offset, len)
    }
}

impl<R: ReadOnlyMappedMemory> ReadOnlyMappingGeneration<R> {
    pub(crate) fn new(
        id: MappingGenerationId,
        mapping: R,
        file_owner: Arc<FileOwner>,
        metrics: Arc<MappedFileMetrics>,
    ) -> Self {
        let mapped_bytes = mapping.as_slice().len();
        let gauge = metrics.track_mapping_generation(mapped_bytes);
        Self {
            id,
            mapping,
            file_owner,
            mapped_bytes,
            _gauge: gauge,
            _access: PhantomData,
        }
    }

    /// Creates an immutable checked region that retains this generation owner.
    pub fn region(
        self: &Arc<Self>,
        offset: usize,
        len: usize,
    ) -> Result<GenerationRegion<R, ReadOnlyAccess>, MmapRangeError> {
        GenerationRegion::try_new_read_only(Arc::clone(self), offset, len)
    }

    /// Runs one synchronous operation against the immutable mapping without allowing the mapping
    /// reference to escape the generation owner.
    #[inline]
    pub(crate) fn with_mapping<T>(&self, operation: impl for<'a> FnOnce(&'a R) -> T) -> T {
        operation(&self.mapping)
    }
}

/// Checked range bound to the mapping generation that owns its backing allocation.
///
/// The default access state is read-only. Writable maintenance ranges are constructed only by
/// crate-internal code and do not implement `Deref` or `AsRef<[u8]>`.
pub struct GenerationRegion<M, Access = ReadOnlyAccess> {
    generation: Arc<MappingGeneration<M, Access>>,
    range: Range<usize>,
}

impl<M, Access> GenerationRegion<M, Access> {
    /// Returns the checked range length.
    #[inline]
    pub fn len(&self) -> usize {
        self.range.len()
    }
}

impl<R: ReadOnlyMappedMemory> GenerationRegion<R, ReadOnlyAccess> {
    fn try_new_read_only(
        generation: Arc<ReadOnlyMappingGeneration<R>>,
        offset: usize,
        len: usize,
    ) -> Result<Self, MmapRangeError> {
        let range = checked_mmap_range(generation.mapped_bytes, offset, len)?;
        Ok(Self { generation, range })
    }
}

impl<W: MappedMemory> GenerationRegion<W, WritableAccess> {
    fn try_new_writable(
        generation: Arc<WritableMappingGeneration<W>>,
        offset: usize,
        len: usize,
    ) -> Result<Self, MmapRangeError> {
        let range = checked_mmap_range(generation.mapped_bytes, offset, len)?;
        Ok(Self { generation, range })
    }

    /// Returns the first byte address for an internal native maintenance operation.
    ///
    /// Dereferencing this pointer is unsafe. The caller must retain the writer fence and this range
    /// owner for the complete operation.
    pub(crate) fn as_ptr(&self) -> *const u8 {
        // SAFETY: the checked range is within the live mapping retained by `generation`.
        unsafe { self.generation.mapping.as_slice().as_ptr().add(self.range.start) }
    }
}

impl<R: ReadOnlyMappedMemory> Deref for GenerationRegion<R, ReadOnlyAccess> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.generation.mapping.as_slice()[self.range.clone()]
    }
}

impl<R: ReadOnlyMappedMemory> AsRef<[u8]> for GenerationRegion<R, ReadOnlyAccess> {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

/// Immutable mapped range that inseparably owns one admitted read operation.
///
/// Clones and split ranges share one inner owner and therefore one admission count. The final alias
/// drops the read-only generation before releasing its operation lease, so physical unmap cannot
/// race ahead of the last safe slice user.
pub struct MappedReadLease<R: ReadOnlyMappedMemory> {
    inner: Arc<MappedReadLeaseInner<R>>,
    range: Range<usize>,
}

struct MappedReadLeaseInner<R: ReadOnlyMappedMemory> {
    generation: Option<Arc<ReadOnlyMappingGeneration<R>>>,
    operation: Option<MappedFileLease>,
}

impl<R: ReadOnlyMappedMemory> MappedReadLeaseInner<R> {
    /// The fields are present for every observable inner value and are taken only by final `Drop`,
    /// after the last `Arc` alias has gone away.
    #[inline]
    fn generation(&self) -> &Arc<ReadOnlyMappingGeneration<R>> {
        self.generation
            .as_ref()
            .expect("mapped read generation exists until final inner drop")
    }
}

impl<R: ReadOnlyMappedMemory> Drop for MappedReadLeaseInner<R> {
    fn drop(&mut self) {
        drop(self.generation.take());
        drop(self.operation.take());
    }
}

impl<R: ReadOnlyMappedMemory> MappedReadLease<R> {
    /// Constructs an owner-bound read range from one already-admitted operation.
    ///
    /// This constructor is crate-private so external safe code cannot combine an unrelated
    /// generation and operation lease. On range failure it preserves the same generation-before-
    /// operation release order as the successfully constructed owner.
    pub(crate) fn try_new(
        generation: Arc<ReadOnlyMappingGeneration<R>>,
        operation: MappedFileLease,
        offset: usize,
        len: usize,
    ) -> Result<Self, MmapRangeError> {
        let range = match checked_mmap_range(generation.mapped_bytes(), offset, len) {
            Ok(range) => range,
            Err(error) => {
                drop(generation);
                drop(operation);
                return Err(error);
            }
        };
        Ok(Self {
            inner: Arc::new(MappedReadLeaseInner {
                generation: Some(generation),
                operation: Some(operation),
            }),
            range,
        })
    }

    /// Returns the identity of the generation backing this range.
    #[inline]
    pub fn generation_id(&self) -> MappingGenerationId {
        self.inner.generation().id()
    }

    /// Returns the mapped range length.
    #[inline]
    pub fn len(&self) -> usize {
        self.range.len()
    }

    /// Returns whether this mapped range is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    /// Creates two aliases split at `mid` bytes relative to this range's start.
    ///
    /// The original remains valid. Both returned aliases share its single inner generation and
    /// operation lease. Returns `None` when `mid` exceeds this range's length.
    pub fn split_at(&self, mid: usize) -> Option<(Self, Self)> {
        let split = self.range.start.checked_add(mid)?;
        if split > self.range.end {
            return None;
        }
        Some((
            Self {
                inner: Arc::clone(&self.inner),
                range: self.range.start..split,
            },
            Self {
                inner: Arc::clone(&self.inner),
                range: split..self.range.end,
            },
        ))
    }
}

impl<R: ReadOnlyMappedMemory> Clone for MappedReadLease<R> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            range: self.range.clone(),
        }
    }
}

impl<R: ReadOnlyMappedMemory> Deref for MappedReadLease<R> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.inner.generation().mapping.as_slice()[self.range.clone()]
    }
}

impl<R: ReadOnlyMappedMemory> AsRef<[u8]> for MappedReadLease<R> {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

/// Currently published writable or read-only generation.
pub enum PublishedGeneration<W, R> {
    /// Active writable generation.
    Writable(Arc<WritableMappingGeneration<W>>),
    /// Sealed read-only generation.
    ReadOnly(Arc<ReadOnlyMappingGeneration<R>>),
}

impl<W, R> PublishedGeneration<W, R> {
    /// Returns the published generation identity.
    pub fn id(&self) -> MappingGenerationId {
        match self {
            Self::Writable(generation) => generation.id(),
            Self::ReadOnly(generation) => generation.id(),
        }
    }

    #[inline]
    fn file_owner(&self) -> &Arc<FileOwner> {
        match self {
            Self::Writable(generation) => generation.file_owner(),
            Self::ReadOnly(generation) => generation.file_owner(),
        }
    }
}

/// Failure to initialize or atomically publish a mapping generation.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MappingPublicationError<E> {
    /// The concrete mapping initializer failed.
    #[error("mapping initialization failed: {0}")]
    Initialization(E),
    /// Admission changed while the fully built candidate was awaiting publication.
    #[error("mapping publication was rejected by lifecycle admission")]
    PublicationRejected,
    /// The mapping slot has been terminally detached.
    #[error("mapping slot is detached")]
    Detached,
    /// No generation is available for replacement.
    #[error("mapping slot has no published generation")]
    NoPublishedGeneration,
    /// A writable initializer observed an already sealed read-only generation.
    #[error("mapping slot already contains a read-only generation")]
    AlreadyReadOnly,
    /// The expected generation is stale.
    #[error("expected mapping generation {expected:?}, but current generation is {actual:?}")]
    ExpectedGenerationMismatch {
        /// Generation expected by the caller.
        expected: MappingGenerationId,
        /// Generation observed while publication was serialized.
        actual: MappingGenerationId,
    },
    /// A fresh non-zero generation identity cannot be assigned.
    #[error(transparent)]
    GenerationExhausted(#[from] MappingGenerationIdExhausted),
}

/// Result of terminally detaching the authoritative mapping slot.
pub(crate) enum MappingSlotDetach<W, R> {
    /// This caller won detach; the slot may have been empty for a never-initialized lazy mapping.
    Detached {
        generation: Option<Arc<PublishedGeneration<W, R>>>,
    },
    /// A prior caller already detached the slot.
    AlreadyDetached,
}

impl<W, R> MappingSlotDetach<W, R> {
    pub(crate) fn into_generation(self) -> Option<Arc<PublishedGeneration<W, R>>> {
        match self {
            Self::Detached { generation } => generation,
            Self::AlreadyDetached => None,
        }
    }

    #[cfg(test)]
    fn is_winner(&self) -> bool {
        matches!(self, Self::Detached { .. })
    }
}

/// Proof that one fully built generation was stored in the authoritative slot.
pub(crate) struct PublishedGenerationToken<M, Access> {
    generation: Arc<MappingGeneration<M, Access>>,
}

impl<M, Access> PublishedGenerationToken<M, Access> {
    fn into_generation(self) -> Arc<MappingGeneration<M, Access>> {
        self.generation
    }
}

/// Consuming publication capability for one fully built writable candidate.
///
/// The lifecycle gate receives this value and may call [`Self::publish`] only while holding its
/// close-control lock. Dropping it rejects and releases the unpublished candidate.
pub(crate) struct WritableGenerationPublication<'a, W, R> {
    owner: &'a MappedFileMapping<W, R>,
    candidate: Arc<WritableMappingGeneration<W>>,
}

impl<W, R> WritableGenerationPublication<'_, W, R> {
    pub(crate) fn publish(self) -> PublishedGenerationToken<W, WritableAccess> {
        self.owner
            .slot
            .store(Some(Arc::new(PublishedGeneration::Writable(Arc::clone(
                &self.candidate,
            )))));
        PublishedGenerationToken {
            generation: self.candidate,
        }
    }
}

/// Consuming publication capability for one fully built read-only candidate.
pub(crate) struct ReadOnlyGenerationPublication<'a, W, R> {
    owner: &'a MappedFileMapping<W, R>,
    candidate: Arc<ReadOnlyMappingGeneration<R>>,
}

impl<W, R> ReadOnlyGenerationPublication<'_, W, R> {
    pub(crate) fn publish(self) -> PublishedGenerationToken<R, ReadOnlyAccess> {
        self.owner
            .slot
            .store(Some(Arc::new(PublishedGeneration::ReadOnly(Arc::clone(
                &self.candidate,
            )))));
        PublishedGenerationToken {
            generation: self.candidate,
        }
    }
}

/// Atomic owner slot and serialized candidate-publication controller for one mapped file.
pub struct MappedFileMapping<W, R> {
    slot: ArcSwapOption<PublishedGeneration<W, R>>,
    init_lock: Mutex<()>,
    generation_ids: MappingGenerationIdSequence,
    detached: AtomicBool,
    lazy_enabled: bool,
    metrics: Arc<MappedFileMetrics>,
    map_operations: AtomicU64,
    map_failures: AtomicU64,
    total_millis: AtomicU64,
    last_millis: AtomicU64,
}

impl<W: MappedMemory, R> MappedFileMapping<W, R> {
    /// Creates an eagerly initialized writable generation.
    pub(crate) fn new_eager(mapping: W, file_owner: Arc<FileOwner>, metrics: Arc<MappedFileMetrics>) -> Self {
        let generation = Arc::new(WritableMappingGeneration::new(
            MappingGenerationId::FIRST,
            mapping,
            file_owner,
            Arc::clone(&metrics),
        ));
        Self {
            slot: ArcSwapOption::from(Some(Arc::new(PublishedGeneration::Writable(generation)))),
            init_lock: Mutex::new(()),
            generation_ids: MappingGenerationIdSequence::with_last_assigned(MappingGenerationId::FIRST.get()),
            detached: AtomicBool::new(false),
            lazy_enabled: false,
            metrics,
            map_operations: AtomicU64::new(0),
            map_failures: AtomicU64::new(0),
            total_millis: AtomicU64::new(0),
            last_millis: AtomicU64::new(0),
        }
    }
}

impl<W, R> MappedFileMapping<W, R> {
    /// Creates an uninitialized mapping eligible for lazy initialization.
    pub(crate) fn new_lazy(metrics: Arc<MappedFileMetrics>) -> Self {
        Self {
            slot: ArcSwapOption::empty(),
            init_lock: Mutex::new(()),
            generation_ids: MappingGenerationIdSequence::new(),
            detached: AtomicBool::new(false),
            lazy_enabled: true,
            metrics,
            map_operations: AtomicU64::new(0),
            map_failures: AtomicU64::new(0),
            total_millis: AtomicU64::new(0),
            last_millis: AtomicU64::new(0),
        }
    }

    /// Returns whether this slot was configured for lazy initialization.
    #[inline]
    pub fn is_lazy_enabled(&self) -> bool {
        self.lazy_enabled
    }

    /// Returns whether a generation is currently published.
    #[inline]
    pub fn is_mapped(&self) -> bool {
        self.slot.load().is_some()
    }

    /// Returns whether the owner slot has been terminally detached.
    #[inline]
    pub fn is_detached(&self) -> bool {
        self.detached.load(Ordering::Acquire)
    }

    /// Returns lazy-initialization counters.
    pub fn stats(&self) -> LazyMmapStats {
        LazyMmapStats {
            eligible_files: u64::from(self.lazy_enabled),
            mapped_files: u64::from(self.lazy_enabled && self.is_mapped()),
            map_operations: self.map_operations.load(Ordering::Acquire),
            map_failures: self.map_failures.load(Ordering::Acquire),
            total_millis: self.total_millis.load(Ordering::Acquire),
            last_millis: self.last_millis.load(Ordering::Acquire),
        }
    }

    pub(crate) fn load_writable(&self) -> Option<Arc<WritableMappingGeneration<W>>> {
        let current = self.slot.load_full()?;
        match current.as_ref() {
            PublishedGeneration::Writable(generation) => Some(Arc::clone(generation)),
            PublishedGeneration::ReadOnly(_) => None,
        }
    }

    /// Runs one synchronous operation against the currently published writable generation.
    ///
    /// The ArcSwap guard retains the published owner for the complete closure call without the
    /// eager outer clone performed by `load_full`; this method also does not clone the inner
    /// generation `Arc`. The higher-ranked callback and independently chosen result type prevent
    /// a generation borrow from escaping this scope. Callers that need an owner beyond the
    /// callback must use [`Self::load_writable`] instead.
    #[inline]
    pub(crate) fn with_writable_scoped<T>(
        &self,
        operation: impl for<'generation> FnOnce(&'generation WritableMappingGeneration<W>) -> T,
    ) -> Option<T> {
        let current = self.slot.load();
        let published = current.as_ref()?;
        match published.as_ref() {
            PublishedGeneration::Writable(generation) => Some(operation(generation.as_ref())),
            PublishedGeneration::ReadOnly(_) => None,
        }
    }

    pub(crate) fn load_read_only(&self) -> Option<Arc<ReadOnlyMappingGeneration<R>>> {
        let current = self.slot.load_full()?;
        match current.as_ref() {
            PublishedGeneration::Writable(_) => None,
            PublishedGeneration::ReadOnly(generation) => Some(Arc::clone(generation)),
        }
    }

    /// Terminally detaches the authoritative slot exactly once.
    pub(crate) fn detach(&self) -> MappingSlotDetach<W, R> {
        let _guard = self.init_lock.lock();
        if self.detached.swap(true, Ordering::AcqRel) {
            return MappingSlotDetach::AlreadyDetached;
        }
        MappingSlotDetach::Detached {
            generation: self.slot.swap(None),
        }
    }

    fn record_map_failure(&self) {
        if self.lazy_enabled {
            self.map_failures.fetch_add(1, Ordering::AcqRel);
        }
    }
}

impl<W: MappedMemory, R> MappedFileMapping<W, R> {
    /// Loads the current writable generation or lazily builds and publishes it.
    ///
    /// `can_build` checks the caller's admission before construction. The fully built candidate is
    /// then passed to `publish_gate`; that closure must call the consuming publication capability
    /// while holding the lifecycle close-control lock. Detach uses this method's serialization lock,
    /// so the slot store has one ordering against both close and terminal detach.
    pub(crate) fn get_or_try_init<E, F, V, P>(
        &self,
        initializer: F,
        can_build: V,
        publish_gate: P,
    ) -> Result<Arc<WritableMappingGeneration<W>>, MappingPublicationError<E>>
    where
        F: FnOnce() -> Result<(W, Arc<FileOwner>), E>,
        V: FnOnce() -> bool,
        P: FnOnce(WritableGenerationPublication<'_, W, R>) -> Option<PublishedGenerationToken<W, WritableAccess>>,
    {
        if self.is_detached() {
            return Err(MappingPublicationError::Detached);
        }
        if let Some(current) = self.slot.load_full() {
            return match current.as_ref() {
                PublishedGeneration::Writable(generation) => Ok(Arc::clone(generation)),
                PublishedGeneration::ReadOnly(_) => Err(MappingPublicationError::AlreadyReadOnly),
            };
        }

        let _guard = self.init_lock.lock();
        if self.is_detached() {
            return Err(MappingPublicationError::Detached);
        }
        if let Some(current) = self.slot.load_full() {
            return match current.as_ref() {
                PublishedGeneration::Writable(generation) => Ok(Arc::clone(generation)),
                PublishedGeneration::ReadOnly(_) => Err(MappingPublicationError::AlreadyReadOnly),
            };
        }
        if !can_build() {
            self.record_map_failure();
            return Err(MappingPublicationError::PublicationRejected);
        }

        let started = Instant::now();
        let id = self.generation_ids.next().map_err(|error| {
            self.record_map_failure();
            MappingPublicationError::GenerationExhausted(error)
        })?;
        let (mapping, file_owner) = initializer().map_err(|error| {
            self.record_map_failure();
            MappingPublicationError::Initialization(error)
        })?;
        let candidate = Arc::new(WritableMappingGeneration::new(
            id,
            mapping,
            file_owner,
            Arc::clone(&self.metrics),
        ));

        let publication = WritableGenerationPublication { owner: self, candidate };
        let candidate = publish_gate(publication)
            .ok_or_else(|| {
                self.record_map_failure();
                MappingPublicationError::PublicationRejected
            })?
            .into_generation();
        if self.lazy_enabled {
            let elapsed_millis = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
            self.map_operations.fetch_add(1, Ordering::AcqRel);
            self.total_millis.fetch_add(elapsed_millis, Ordering::AcqRel);
            self.last_millis.store(elapsed_millis, Ordering::Release);
        }
        Ok(candidate)
    }
}

impl<W, R: ReadOnlyMappedMemory> MappedFileMapping<W, R> {
    /// Builds and atomically publishes a read-only replacement for the expected generation.
    ///
    /// Existing owners retain the old generation through their `Arc`; only new loads observe the
    /// replacement. The initializer runs while publication and detach are serialized. The complete
    /// owner-bound candidate is published only if `publish_gate` invokes its consuming capability
    /// while holding the lifecycle close-control lock.
    pub(crate) fn replace_with_read_only<E, F, V, P>(
        &self,
        expected: MappingGenerationId,
        initializer: F,
        can_build: V,
        publish_gate: P,
    ) -> Result<Arc<ReadOnlyMappingGeneration<R>>, MappingPublicationError<E>>
    where
        F: FnOnce() -> Result<R, E>,
        V: FnOnce() -> bool,
        P: FnOnce(ReadOnlyGenerationPublication<'_, W, R>) -> Option<PublishedGenerationToken<R, ReadOnlyAccess>>,
    {
        let _guard = self.init_lock.lock();
        if self.is_detached() {
            return Err(MappingPublicationError::Detached);
        }
        let current = self
            .slot
            .load_full()
            .ok_or(MappingPublicationError::NoPublishedGeneration)?;
        if current.id() != expected {
            return Err(MappingPublicationError::ExpectedGenerationMismatch {
                expected,
                actual: current.id(),
            });
        }
        if !can_build() {
            return Err(MappingPublicationError::PublicationRejected);
        }

        let id = self.generation_ids.next()?;
        let mapping = initializer().map_err(MappingPublicationError::Initialization)?;
        let candidate = Arc::new(ReadOnlyMappingGeneration::new(
            id,
            mapping,
            Arc::clone(current.file_owner()),
            Arc::clone(&self.metrics),
        ));
        publish_gate(ReadOnlyGenerationPublication { owner: self, candidate })
            .ok_or(MappingPublicationError::PublicationRejected)
            .map(PublishedGenerationToken::into_generation)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::sync::Barrier;

    use super::*;
    use crate::mapped_file::file::FileOwner;
    use crate::mapped_file::lifecycle::SegmentLifecycle;
    use crate::mapped_file::memory::MappedMemory;
    use crate::mapped_file::memory::ReadOnlyMappedMemory;
    use crate::mapped_file::metrics::MappedFileMetrics;
    use crate::mapped_file::MappedFileOperation;

    struct TestWritableMemory(Vec<u8>);

    struct TestReadOnlyMemory(Vec<u8>);

    struct DropObservedReadOnlyMemory {
        bytes: Vec<u8>,
        lifecycle: Arc<SegmentLifecycle>,
        drops: Arc<AtomicUsize>,
        dropped_before_operation: Arc<AtomicBool>,
    }

    // SAFETY: tests serialize all mutation, ranges are checked by the generation owner, and the
    // backing vector remains stable for the complete value lifetime.
    unsafe impl MappedMemory for TestWritableMemory {
        type ReadOnly = TestReadOnlyMemory;

        unsafe fn map_mut(file: &File) -> io::Result<Self> {
            Ok(Self(vec![0; file.metadata()?.len() as usize]))
        }

        fn as_slice(&self) -> &[u8] {
            &self.0
        }

        fn as_mut_ptr(&self) -> *mut u8 {
            self.0.as_ptr().cast_mut()
        }

        fn flush(&self) -> io::Result<()> {
            Ok(())
        }

        fn flush_range(&self, _offset: usize, _len: usize) -> io::Result<()> {
            Ok(())
        }
    }

    // SAFETY: the backing vector is immutable and remains live for the complete value lifetime.
    unsafe impl ReadOnlyMappedMemory for TestReadOnlyMemory {
        unsafe fn map(file: &File) -> io::Result<Self> {
            Ok(Self(vec![0; file.metadata()?.len() as usize]))
        }

        fn as_slice(&self) -> &[u8] {
            &self.0
        }
    }

    // SAFETY: the backing vector is immutable and remains live for the complete value lifetime.
    unsafe impl ReadOnlyMappedMemory for DropObservedReadOnlyMemory {
        unsafe fn map(_file: &File) -> io::Result<Self> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "drop-observed test memory is constructed directly",
            ))
        }

        fn as_slice(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for DropObservedReadOnlyMemory {
        fn drop(&mut self) {
            self.dropped_before_operation
                .store(self.lifecycle.snapshot().active_leases == 1, Ordering::Release);
            self.drops.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn file_owner(metrics: &Arc<MappedFileMetrics>) -> Arc<FileOwner> {
        Arc::new(FileOwner::new(
            tempfile::tempfile().expect("temporary file opens"),
            metrics.track_file_owner(),
        ))
    }

    fn observed_read_lease(
        bytes: Vec<u8>,
        lifecycle: &Arc<SegmentLifecycle>,
        metrics: &Arc<MappedFileMetrics>,
        drops: Arc<AtomicUsize>,
        dropped_before_operation: Arc<AtomicBool>,
    ) -> MappedReadLease<DropObservedReadOnlyMemory> {
        let operation = lifecycle
            .try_acquire(MappedFileOperation::Read)
            .expect("read admission succeeds");
        let len = bytes.len();
        let generation = Arc::new(ReadOnlyMappingGeneration::new(
            MappingGenerationId::FIRST,
            DropObservedReadOnlyMemory {
                bytes,
                lifecycle: Arc::clone(lifecycle),
                drops,
                dropped_before_operation,
            },
            file_owner(metrics),
            Arc::clone(metrics),
        ));
        MappedReadLease::try_new(generation, operation, 0, len).expect("complete range is valid")
    }

    #[test]
    fn generation_ids_are_non_zero_monotonic_and_exhaustion_fails_closed() {
        let sequence = MappingGenerationIdSequence::new();
        assert_eq!(sequence.next().expect("first id").get(), 1);
        assert_eq!(sequence.next().expect("second id").get(), 2);

        let almost_exhausted = MappingGenerationIdSequence::with_last_assigned(u64::MAX - 1);
        assert_eq!(almost_exhausted.next().expect("last id").get(), u64::MAX);
        assert_eq!(almost_exhausted.next(), Err(MappingGenerationIdExhausted));
        assert_eq!(almost_exhausted.next(), Err(MappingGenerationIdExhausted));
    }

    #[test]
    fn exhausted_slot_keeps_current_generation_and_does_not_build_candidate() {
        let metrics = Arc::new(MappedFileMetrics::new());
        let owner = file_owner(&metrics);
        let mapping = MappedFileMapping::<TestWritableMemory, TestReadOnlyMemory>::new_eager(
            TestWritableMemory(vec![1, 2, 3, 4]),
            owner,
            Arc::clone(&metrics),
        );
        mapping.generation_ids.last_assigned.store(u64::MAX, Ordering::Release);
        let initializer_called = AtomicBool::new(false);

        let result = mapping.replace_with_read_only(
            MappingGenerationId::FIRST,
            || {
                initializer_called.store(true, Ordering::Release);
                Ok::<_, &'static str>(TestReadOnlyMemory(vec![5, 6, 7, 8]))
            },
            || true,
            |publication| Some(publication.publish()),
        );

        assert!(matches!(
            result,
            Err(MappingPublicationError::GenerationExhausted(
                MappingGenerationIdExhausted
            ))
        ));
        assert!(!initializer_called.load(Ordering::Acquire));
        assert_eq!(
            mapping.load_writable().expect("original remains").id(),
            MappingGenerationId::FIRST
        );
        assert_eq!(metrics.mapped_generations_live(), 1);
    }

    #[test]
    fn old_generation_survives_atomic_read_only_replacement() {
        let metrics = Arc::new(MappedFileMetrics::new());
        let owner = file_owner(&metrics);
        let mapping = MappedFileMapping::<TestWritableMemory, TestReadOnlyMemory>::new_eager(
            TestWritableMemory(vec![1, 2, 3, 4]),
            Arc::clone(&owner),
            Arc::clone(&metrics),
        );
        let old = mapping.load_writable().expect("eager writable generation");

        let current = mapping
            .replace_with_read_only(
                old.id(),
                || Ok::<_, &'static str>(TestReadOnlyMemory(vec![5, 6, 7, 8])),
                || true,
                |publication| Some(publication.publish()),
            )
            .expect("replacement publishes");

        assert!(current.id().get() > old.id().get());
        assert_eq!(mapping.load_read_only().expect("new generation").id(), current.id());
        assert_eq!(metrics.mapped_generations_live(), 2);
        assert_eq!(metrics.mapped_bytes_live(), 8);
        assert_eq!(old.with_mapping(|memory| memory.as_slice().to_vec()), vec![1, 2, 3, 4]);

        drop(old);
        assert_eq!(metrics.mapped_generations_live(), 1);
        assert_eq!(metrics.mapped_bytes_live(), 4);
        assert_eq!(metrics.physical_mapping_drop_total(), 1);
        assert_eq!(current.region(0, 4).expect("checked region").as_ref(), &[5, 6, 7, 8]);
    }

    #[test]
    fn scoped_writable_access_borrows_slot_without_arc_clones() {
        let metrics = Arc::new(MappedFileMetrics::new());
        let mapping = MappedFileMapping::<TestWritableMemory, TestReadOnlyMemory>::new_eager(
            TestWritableMemory(vec![1, 2, 3, 4]),
            file_owner(&metrics),
            Arc::clone(&metrics),
        );
        let published = mapping.slot.load_full().expect("eager generation is published");
        let PublishedGeneration::Writable(generation) = published.as_ref() else {
            panic!("eager generation remains writable");
        };
        let published_owners = Arc::strong_count(&published);
        let generation_owners = Arc::strong_count(generation);

        let observed = mapping
            .with_writable_scoped(|scoped| {
                assert!(std::ptr::eq(scoped, generation.as_ref()));
                assert_eq!(Arc::strong_count(&published), published_owners);
                assert_eq!(Arc::strong_count(generation), generation_owners);
                scoped.with_mapping(|memory| (scoped.id(), memory.as_slice().to_vec()))
            })
            .expect("writable generation is available");

        assert_eq!(observed.0, MappingGenerationId::FIRST);
        assert_eq!(observed.1, vec![1, 2, 3, 4]);
        fn assert_owned_result<T: 'static>(_: &T) {}
        assert_owned_result(&observed);
        assert_eq!(Arc::strong_count(&published), published_owners);
        assert_eq!(Arc::strong_count(generation), generation_owners);
    }

    #[test]
    fn scoped_writable_access_survives_concurrent_replace_and_detach() {
        let metrics = Arc::new(MappedFileMetrics::new());
        let owner = file_owner(&metrics);
        let mapping = Arc::new(MappedFileMapping::<TestWritableMemory, TestReadOnlyMemory>::new_eager(
            TestWritableMemory(vec![1, 2, 3, 4]),
            Arc::clone(&owner),
            Arc::clone(&metrics),
        ));
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));

        let worker = {
            let mapping = Arc::clone(&mapping);
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            std::thread::spawn(move || {
                mapping
                    .with_writable_scoped(|generation| {
                        entered.wait();
                        release.wait();
                        generation.with_mapping(|memory| memory.as_slice().to_vec())
                    })
                    .expect("worker captures the writable generation")
            })
        };

        entered.wait();
        let writable_id = mapping
            .load_writable()
            .expect("writable generation remains published")
            .id();
        mapping
            .replace_with_read_only(
                writable_id,
                || Ok::<_, &'static str>(TestReadOnlyMemory(vec![5, 6, 7, 8])),
                || true,
                |publication| Some(publication.publish()),
            )
            .expect("read-only replacement publishes");
        assert!(mapping.with_writable_scoped(|_| ()).is_none());
        let detached = mapping
            .detach()
            .into_generation()
            .expect("read-only generation detaches");
        assert!(mapping.is_detached());
        assert!(mapping.with_writable_scoped(|_| ()).is_none());
        assert_eq!(metrics.mapped_generations_live(), 2);
        drop(detached);
        assert_eq!(metrics.mapped_generations_live(), 1);

        release.wait();
        assert_eq!(worker.join().expect("scoped worker does not panic"), vec![1, 2, 3, 4]);
        assert_eq!(metrics.mapped_generations_live(), 0);
    }

    #[test]
    fn generation_region_rejects_overflow_and_out_of_bounds() {
        let metrics = Arc::new(MappedFileMetrics::new());
        let owner = file_owner(&metrics);
        let generation = Arc::new(ReadOnlyMappingGeneration::new(
            MappingGenerationId::FIRST,
            TestReadOnlyMemory(vec![1, 2, 3, 4]),
            owner,
            Arc::clone(&metrics),
        ));

        assert_eq!(generation.region(1, 2).expect("valid region").as_ref(), &[2, 3]);
        assert!(matches!(
            generation.region(4, 1),
            Err(MmapRangeError::OutOfBounds {
                offset: 4,
                len: 1,
                mapping_len: 4,
            })
        ));
        assert!(matches!(
            generation.region(usize::MAX, 1),
            Err(MmapRangeError::Overflow {
                offset: usize::MAX,
                len: 1,
            })
        ));

        let region = generation.region(0, 4).expect("owner-bound region");
        drop(generation);
        assert_eq!(metrics.mapped_generations_live(), 1);
        assert_eq!(region.as_ref(), &[1, 2, 3, 4]);
        drop(region);
        assert_eq!(metrics.mapped_generations_live(), 0);
    }

    #[test]
    fn close_racing_lazy_init_never_publishes() {
        let metrics = Arc::new(MappedFileMetrics::new());
        let owner = file_owner(&metrics);
        let mapping = Arc::new(MappedFileMapping::<TestWritableMemory, TestReadOnlyMemory>::new_lazy(
            Arc::clone(&metrics),
        ));
        let publication_allowed = Arc::new(AtomicBool::new(true));
        let initializer_started = Arc::new(Barrier::new(2));
        let release_initializer = Arc::new(Barrier::new(2));

        let worker = {
            let mapping = Arc::clone(&mapping);
            let owner = Arc::clone(&owner);
            let publication_allowed = Arc::clone(&publication_allowed);
            let initializer_started = Arc::clone(&initializer_started);
            let release_initializer = Arc::clone(&release_initializer);
            std::thread::spawn(move || {
                mapping.get_or_try_init(
                    || {
                        initializer_started.wait();
                        release_initializer.wait();
                        Ok::<_, &'static str>((TestWritableMemory(vec![0; 8]), owner))
                    },
                    || true,
                    |publication| {
                        publication_allowed
                            .load(Ordering::Acquire)
                            .then(|| publication.publish())
                    },
                )
            })
        };

        initializer_started.wait();
        publication_allowed.store(false, Ordering::Release);
        release_initializer.wait();

        assert!(matches!(
            worker.join().expect("initializer thread does not panic"),
            Err(MappingPublicationError::PublicationRejected)
        ));
        assert!(!mapping.is_mapped());
        assert_eq!(mapping.stats().map_failures, 1);
        assert_eq!(metrics.mapped_generations_live(), 0);
        assert_eq!(metrics.physical_mapping_drop_total(), 1);
    }

    #[test]
    fn detach_is_exactly_once_and_terminal() {
        let metrics = Arc::new(MappedFileMetrics::new());
        let owner = file_owner(&metrics);
        let mapping = MappedFileMapping::<TestWritableMemory, TestReadOnlyMemory>::new_eager(
            TestWritableMemory(vec![0; 8]),
            Arc::clone(&owner),
            Arc::clone(&metrics),
        );

        let first = mapping.detach();
        assert!(first.is_winner());
        let detached = first.into_generation().expect("eager generation was detached");
        assert!(!mapping.detach().is_winner());
        assert!(mapping.is_detached());
        assert!(!mapping.is_mapped());
        assert!(matches!(
            mapping.get_or_try_init(
                || Ok::<_, &'static str>((TestWritableMemory(vec![0; 8]), owner)),
                || true,
                |publication| Some(publication.publish()),
            ),
            Err(MappingPublicationError::Detached)
        ));

        assert_eq!(metrics.mapped_generations_live(), 1);
        drop(detached);
        assert_eq!(metrics.mapped_generations_live(), 0);
    }

    #[test]
    fn mapped_read_lease_drops_generation_before_operation_admission() {
        let lifecycle = SegmentLifecycle::shared();
        let metrics = Arc::new(MappedFileMetrics::new());
        let drops = Arc::new(AtomicUsize::new(0));
        let dropped_before_operation = Arc::new(AtomicBool::new(false));
        let lease = observed_read_lease(
            vec![1, 2, 3, 4],
            &lifecycle,
            &metrics,
            Arc::clone(&drops),
            Arc::clone(&dropped_before_operation),
        );

        assert_eq!(lifecycle.snapshot().active_leases, 1);
        assert_eq!(lease.as_ref(), &[1, 2, 3, 4]);
        drop(lease);

        assert_eq!(drops.load(Ordering::Acquire), 1);
        assert!(dropped_before_operation.load(Ordering::Acquire));
        assert_eq!(lifecycle.snapshot().active_leases, 0);
        assert_eq!(metrics.mapped_generations_live(), 0);
    }

    #[test]
    fn mapped_read_lease_rejects_invalid_range_and_releases_both_owners() {
        let lifecycle = SegmentLifecycle::shared();
        let metrics = Arc::new(MappedFileMetrics::new());
        let drops = Arc::new(AtomicUsize::new(0));
        let dropped_before_operation = Arc::new(AtomicBool::new(false));
        let operation = lifecycle
            .try_acquire(MappedFileOperation::Read)
            .expect("read admission succeeds");
        let generation = Arc::new(ReadOnlyMappingGeneration::new(
            MappingGenerationId::FIRST,
            DropObservedReadOnlyMemory {
                bytes: vec![1, 2, 3, 4],
                lifecycle: Arc::clone(&lifecycle),
                drops: Arc::clone(&drops),
                dropped_before_operation: Arc::clone(&dropped_before_operation),
            },
            file_owner(&metrics),
            Arc::clone(&metrics),
        ));

        assert!(matches!(
            MappedReadLease::try_new(generation, operation, 4, 1),
            Err(MmapRangeError::OutOfBounds {
                offset: 4,
                len: 1,
                mapping_len: 4,
            })
        ));
        assert_eq!(drops.load(Ordering::Acquire), 1);
        assert!(dropped_before_operation.load(Ordering::Acquire));
        assert_eq!(lifecycle.snapshot().active_leases, 0);

        let operation = lifecycle
            .try_acquire(MappedFileOperation::Read)
            .expect("second read admission succeeds");
        let generation = Arc::new(ReadOnlyMappingGeneration::new(
            MappingGenerationId::FIRST,
            DropObservedReadOnlyMemory {
                bytes: vec![1, 2, 3, 4],
                lifecycle: Arc::clone(&lifecycle),
                drops: Arc::clone(&drops),
                dropped_before_operation: Arc::clone(&dropped_before_operation),
            },
            file_owner(&metrics),
            Arc::clone(&metrics),
        ));
        assert!(matches!(
            MappedReadLease::try_new(generation, operation, usize::MAX, 1),
            Err(MmapRangeError::Overflow {
                offset: usize::MAX,
                len: 1,
            })
        ));
        assert_eq!(drops.load(Ordering::Acquire), 2);
        assert_eq!(lifecycle.snapshot().active_leases, 0);
    }

    #[test]
    fn mapped_read_lease_clone_and_split_share_one_inner_admission() {
        let lifecycle = SegmentLifecycle::shared();
        let metrics = Arc::new(MappedFileMetrics::new());
        let drops = Arc::new(AtomicUsize::new(0));
        let dropped_before_operation = Arc::new(AtomicBool::new(false));
        let lease = observed_read_lease(
            vec![1, 2, 3, 4],
            &lifecycle,
            &metrics,
            Arc::clone(&drops),
            Arc::clone(&dropped_before_operation),
        );
        let cloned = lease.clone();
        let (left, right) = lease.split_at(2).expect("middle split is valid");

        assert!(Arc::ptr_eq(&lease.inner, &cloned.inner));
        assert!(Arc::ptr_eq(&lease.inner, &left.inner));
        assert!(Arc::ptr_eq(&lease.inner, &right.inner));
        assert_eq!(lifecycle.snapshot().active_leases, 1);
        assert_eq!(left.as_ref(), &[1, 2]);
        assert_eq!(right.as_ref(), &[3, 4]);

        drop(lease);
        drop(cloned);
        drop(left);
        assert_eq!(lifecycle.snapshot().active_leases, 1);
        assert_eq!(drops.load(Ordering::Acquire), 0);
        drop(right);

        assert_eq!(lifecycle.snapshot().active_leases, 0);
        assert_eq!(drops.load(Ordering::Acquire), 1);
        assert!(dropped_before_operation.load(Ordering::Acquire));
    }
}
