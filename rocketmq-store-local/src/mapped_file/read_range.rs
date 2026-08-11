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

use std::sync::Arc;

use bytes::Bytes;

use super::file::FileOwner;
use super::MappedFileMetrics;
use super::MappedReadLease;
use super::MappingGenerationId;
use super::ReadOnlyMappedMemory;

/// Immutable mapped bytes with authoritative store and file-local coordinates.
///
/// The underlying mapping generation and its read admission are inseparable from this value.
/// Clones and derived ranges share that protection, so retirement cannot unmap the storage before
/// the final alias is dropped. Byte access is closure-scoped to keep the mapping borrow from
/// escaping independently of its owner.
#[must_use = "dropping the range immediately releases its mapped-file read ownership"]
pub struct MappedReadRange<R: ReadOnlyMappedMemory> {
    lease: MappedReadLease<R>,
    start_offset: u64,
    file_offset: u64,
    metrics: Option<Arc<MappedFileMetrics>>,
}

impl<R: ReadOnlyMappedMemory> MappedReadRange<R> {
    pub(crate) fn try_new(
        lease: MappedReadLease<R>,
        start_offset: u64,
        file_offset: u64,
        metrics: Option<Arc<MappedFileMetrics>>,
    ) -> Option<Self> {
        start_offset.checked_sub(file_offset)?;
        Some(Self {
            lease,
            start_offset,
            file_offset,
            metrics,
        })
    }

    /// Returns the absolute CommitLog-style offset of the first selected byte.
    #[inline]
    pub const fn start_offset(&self) -> u64 {
        self.start_offset
    }

    /// Returns the selected position inside the mapped file.
    #[inline]
    pub const fn file_offset(&self) -> u64 {
        self.file_offset
    }

    /// Returns the immutable base offset identifying the mapped file.
    #[inline]
    pub const fn file_from_offset(&self) -> u64 {
        self.start_offset - self.file_offset
    }

    /// Returns the mapping generation retained by this range.
    #[inline]
    pub fn generation_id(&self) -> MappingGenerationId {
        self.lease.generation_id()
    }

    /// Returns the selected byte length.
    #[inline]
    pub fn len(&self) -> usize {
        self.lease.len()
    }

    /// Returns whether this range contains no bytes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.lease.is_empty()
    }

    /// Runs one synchronous operation against the selected bytes.
    ///
    /// The higher-ranked callback prevents the mapped slice from being returned independently of
    /// this owner-bearing range.
    #[inline]
    pub fn with_slice<T>(&self, operation: impl for<'a> FnOnce(&'a [u8]) -> T) -> T {
        operation(self.lease.as_ref())
    }

    /// Copies exactly this range into an immutable compatibility buffer.
    #[inline]
    pub fn to_bytes(&self) -> Bytes {
        let bytes = self.with_slice(Bytes::copy_from_slice);
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.record_selection_copy(bytes.len());
        }
        bytes
    }

    /// Returns a checked subrange relative to this range's first byte.
    ///
    /// Returns `None` when `offset + len` overflows or exceeds this range.
    pub fn slice(&self, offset: usize, len: usize) -> Option<Self> {
        let end = offset.checked_add(len)?;
        if end > self.len() {
            return None;
        }
        let (_, tail) = self.lease.split_at(offset)?;
        let (selected, _) = tail.split_at(len)?;
        let relative = u64::try_from(offset).ok()?;
        Some(Self {
            lease: selected,
            start_offset: self.start_offset.checked_add(relative)?,
            file_offset: self.file_offset.checked_add(relative)?,
            metrics: self.metrics.clone(),
        })
    }

    /// Creates two checked aliases split at `mid` bytes relative to this range.
    ///
    /// The original remains valid. Returns `None` when `mid` exceeds this range.
    pub fn split_at(&self, mid: usize) -> Option<(Self, Self)> {
        let (left, right) = self.lease.split_at(mid)?;
        let relative = u64::try_from(mid).ok()?;
        let right_start = self.start_offset.checked_add(relative)?;
        let right_file_offset = self.file_offset.checked_add(relative)?;
        Some((
            Self {
                lease: left,
                start_offset: self.start_offset,
                file_offset: self.file_offset,
                metrics: self.metrics.clone(),
            },
            Self {
                lease: right,
                start_offset: right_start,
                file_offset: right_file_offset,
                metrics: self.metrics.clone(),
            },
        ))
    }

    #[inline]
    pub(crate) fn as_slice(&self) -> &[u8] {
        self.lease.as_ref()
    }

    #[inline]
    pub(crate) fn file_owner(&self) -> Arc<FileOwner> {
        self.lease.file_owner()
    }
}

impl<R: ReadOnlyMappedMemory> Clone for MappedReadRange<R> {
    fn clone(&self) -> Self {
        Self {
            lease: self.lease.clone(),
            start_offset: self.start_offset,
            file_offset: self.file_offset,
            metrics: self.metrics.clone(),
        }
    }
}
