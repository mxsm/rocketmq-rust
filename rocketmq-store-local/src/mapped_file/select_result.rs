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

use super::lifecycle::MappedFileLease;
use super::DefaultMappedFile;
use super::MappedFile;
use super::MappedFileOperation;
use super::MappedMemory;
use super::NativeMappedMemory;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectMappedBufferSourceKind {
    MappedFile,
    Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectMappedBufferCacheState {
    Unknown,
    Hot,
    Cold,
}

pub(crate) type SelectMappedBufferTransferParts<M> = (
    u64,
    Option<Bytes>,
    i32,
    Option<(MappedFileLease, Arc<DefaultMappedFile<M>>)>,
    u64,
    SelectMappedBufferCacheState,
);

impl SelectMappedBufferCacheState {
    pub fn from_residency(is_in_cache: bool) -> Self {
        if is_in_cache {
            Self::Hot
        } else {
            Self::Cold
        }
    }
}

/// Represents the result of selecting a mapped buffer.
pub struct SelectMappedBufferResult<M: MappedMemory = NativeMappedMemory> {
    /// The start offset.
    start_offset: u64,
    bytes: Option<Bytes>,
    /// The size.
    size: i32,
    /// The mapped file and the admission lease protecting it.
    // Keep the lease before the `Arc` so field drop order releases admission while the mapped
    // file object is still alive.
    mapped_source: Option<(MappedFileLease, Arc<DefaultMappedFile<M>>)>,
    /// Whether the buffer is in cache.
    is_in_cache: bool,
    /// Source kind used for observability and future transfer planning.
    source_kind: SelectMappedBufferSourceKind,
    /// Offset within the mapped file, when the result comes from a mapped file.
    file_offset: u64,
    /// Page-cache state observed when the result was selected.
    cache_state: SelectMappedBufferCacheState,
}

impl<M: MappedMemory> Default for SelectMappedBufferResult<M> {
    fn default() -> Self {
        Self {
            start_offset: 0,
            bytes: None,
            size: 0,
            mapped_source: None,
            is_in_cache: true,
            source_kind: SelectMappedBufferSourceKind::Bytes,
            file_offset: 0,
            cache_state: SelectMappedBufferCacheState::Unknown,
        }
    }
}

impl<M: MappedMemory> SelectMappedBufferResult<M> {
    /// Creates a selection backed by an immutable byte snapshot.
    ///
    /// Returns `None` when the snapshot length cannot be represented by the compatibility
    /// `i32` size field.
    pub fn from_bytes(start_offset: u64, bytes: Bytes) -> Option<Self> {
        Self::from_bytes_with_metadata(start_offset, 0, bytes, true, SelectMappedBufferCacheState::Unknown)
    }

    /// Creates a byte-backed selection while preserving source metadata.
    ///
    /// Returns `None` when the snapshot length cannot be represented by the compatibility
    /// `i32` size field.
    pub fn from_bytes_with_metadata(
        start_offset: u64,
        file_offset: u64,
        bytes: Bytes,
        is_in_cache: bool,
        cache_state: SelectMappedBufferCacheState,
    ) -> Option<Self> {
        let size = i32::try_from(bytes.len()).ok()?;
        Some(Self {
            start_offset,
            bytes: Some(bytes),
            size,
            mapped_source: None,
            is_in_cache,
            source_kind: SelectMappedBufferSourceKind::Bytes,
            file_offset,
            cache_state,
        })
    }

    /// Creates a mapped-file selection with an owned read lease protecting the snapshot source.
    ///
    /// Returns `None` when the snapshot is too large or the mapped file no longer admits reads.
    pub fn try_from_mapped_snapshot(
        start_offset: u64,
        file_offset: u64,
        bytes: Bytes,
        mapped_file: Arc<DefaultMappedFile<M>>,
        is_in_cache: bool,
    ) -> Option<Self> {
        let mut result = Self::from_bytes_with_metadata(
            start_offset,
            file_offset,
            bytes,
            is_in_cache,
            SelectMappedBufferCacheState::from_residency(is_in_cache),
        )?;
        result.try_attach_mapped_file(mapped_file).then_some(result)
    }

    /// Retains the mapped file as an optional transfer source.
    ///
    /// The immutable byte snapshot remains available when the mapped file can no longer be held,
    /// so callers can safely fall back to the copied representation.
    pub fn try_attach_mapped_file(&mut self, mapped_file: Arc<DefaultMappedFile<M>>) -> bool {
        if self.mapped_source.is_some() {
            return false;
        }
        let Some(bytes) = self.bytes.as_ref() else {
            return false;
        };
        let Ok(size) = usize::try_from(self.size) else {
            return false;
        };
        if bytes.len() != size {
            return false;
        }
        let Ok(lease) = mapped_file.try_acquire_owned_lease(MappedFileOperation::Read) else {
            return false;
        };
        if !mapped_file
            .mapped_snapshot_matches_admitted(&lease, self.start_offset, self.file_offset, bytes)
            .unwrap_or(false)
        {
            return false;
        }
        self.source_kind = SelectMappedBufferSourceKind::MappedFile;
        self.mapped_source = Some((lease, mapped_file));
        true
    }

    #[inline]
    pub fn start_offset(&self) -> u64 {
        self.start_offset
    }

    #[inline]
    pub fn size(&self) -> i32 {
        self.size
    }

    #[inline]
    pub fn is_in_cache(&self) -> bool {
        self.is_in_cache
    }

    #[inline]
    pub fn source_kind(&self) -> SelectMappedBufferSourceKind {
        self.source_kind
    }

    #[inline]
    pub fn file_offset(&self) -> u64 {
        self.file_offset
    }

    #[inline]
    pub fn cache_state(&self) -> SelectMappedBufferCacheState {
        self.cache_state
    }

    /// Returns the buffer.
    ///
    /// # Panics
    ///
    /// Panics when an internal producer constructs a selection without its required immutable
    /// byte snapshot.
    pub fn get_buffer(&self) -> &[u8] {
        self.bytes
            .as_deref()
            .expect("selected mapped buffers must own an immutable byte snapshot")
    }

    #[inline]
    pub fn get_bytes(&self) -> Option<Bytes> {
        self.bytes.clone()
    }

    #[inline]
    pub fn get_bytes_ref(&self) -> Option<&Bytes> {
        self.bytes.as_ref()
    }

    #[inline]
    pub fn bytes_mut(&mut self) -> Option<&mut Bytes> {
        if self.mapped_source.take().is_some() {
            self.source_kind = SelectMappedBufferSourceKind::Bytes;
        }
        self.bytes.as_mut()
    }

    /// Removes and returns the immutable byte snapshot.
    #[inline]
    pub fn take(&mut self) -> Option<Bytes> {
        self.bytes.take()
    }

    /// Truncates the byte snapshot and keeps the compatibility size synchronized.
    pub fn try_truncate(&mut self, new_len: usize) -> bool {
        let Ok(size) = i32::try_from(new_len) else {
            return false;
        };
        let Some(bytes) = self.bytes.as_mut() else {
            return false;
        };
        if new_len > bytes.len() {
            return false;
        }
        bytes.truncate(new_len);
        self.size = size;
        true
    }

    pub fn is_in_mem(&self) -> bool {
        match self.mapped_source.as_ref() {
            None => true,
            Some((_lease, inner)) => {
                let Some(pos) = self.start_offset.checked_sub(inner.get_file_from_offset()) else {
                    return false;
                };
                inner.is_loaded(pos as i64, self.size as usize)
            }
        }
    }

    pub(crate) fn into_transfer_parts(mut self) -> SelectMappedBufferTransferParts<M> {
        (
            self.start_offset,
            self.bytes.take(),
            self.size,
            self.mapped_source.take(),
            self.file_offset,
            self.cache_state,
        )
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;
    use crate::mapped_file::kernel::ReferenceResource;

    fn mapped_file() -> (tempfile::TempDir, Arc<DefaultMappedFile<NativeMappedMemory>>) {
        let directory = tempfile::tempdir().expect("temporary mapped-file directory");
        let path = directory.path().join("00000000000000000000");
        let mapped_file = DefaultMappedFile::try_new(CheetahString::from(path.to_string_lossy().into_owned()), 64)
            .expect("mapped file");
        (directory, Arc::new(mapped_file))
    }

    #[test]
    fn byte_snapshot_accessors_and_truncation_keep_size_consistent() {
        let mut result = SelectMappedBufferResult::<NativeMappedMemory>::from_bytes_with_metadata(
            42,
            7,
            Bytes::from_static(b"snapshot"),
            false,
            SelectMappedBufferCacheState::Cold,
        )
        .expect("valid snapshot");

        assert_eq!(result.start_offset(), 42);
        assert_eq!(result.file_offset(), 7);
        assert_eq!(result.size(), 8);
        assert!(!result.is_in_cache());
        assert_eq!(result.source_kind(), SelectMappedBufferSourceKind::Bytes);
        assert_eq!(result.cache_state(), SelectMappedBufferCacheState::Cold);
        assert!(!result.try_truncate(9));
        assert!(result.try_truncate(4));
        assert_eq!(result.size(), 4);
        assert_eq!(result.get_buffer(), b"snap");
        assert_eq!(result.take().as_deref(), Some(&b"snap"[..]));
        assert!(result.bytes_mut().is_none());
    }

    #[test]
    fn mapped_attachment_validates_identity_and_owns_exactly_one_read_lease() {
        let (_directory, mapped_file) = mapped_file();
        assert!(mapped_file.append_message_bytes(b"mapped"));
        let baseline = ReferenceResource::get_ref_count(mapped_file.as_ref());

        let mut wrong_bytes = SelectMappedBufferResult::from_bytes_with_metadata(
            0,
            0,
            Bytes::from_static(b"forged"),
            true,
            SelectMappedBufferCacheState::Hot,
        )
        .expect("valid snapshot");
        assert!(!wrong_bytes.try_attach_mapped_file(Arc::clone(&mapped_file)));
        assert_eq!(ReferenceResource::get_ref_count(mapped_file.as_ref()), baseline);

        let mut wrong_identity = SelectMappedBufferResult::from_bytes_with_metadata(
            1,
            0,
            Bytes::from_static(b"mapped"),
            true,
            SelectMappedBufferCacheState::Hot,
        )
        .expect("valid snapshot");
        assert!(!wrong_identity.try_attach_mapped_file(Arc::clone(&mapped_file)));
        assert_eq!(ReferenceResource::get_ref_count(mapped_file.as_ref()), baseline);

        let mut mutable = SelectMappedBufferResult::from_bytes_with_metadata(
            0,
            0,
            Bytes::from_static(b"mapped"),
            true,
            SelectMappedBufferCacheState::Hot,
        )
        .expect("valid snapshot");
        assert!(mutable.try_attach_mapped_file(Arc::clone(&mapped_file)));
        *mutable.bytes_mut().expect("byte snapshot") = Bytes::from_static(b"forged");
        assert_eq!(mutable.source_kind(), SelectMappedBufferSourceKind::Bytes);
        assert_eq!(ReferenceResource::get_ref_count(mapped_file.as_ref()), baseline);

        let mut selected = SelectMappedBufferResult::from_bytes_with_metadata(
            0,
            0,
            Bytes::from_static(b"mapped"),
            true,
            SelectMappedBufferCacheState::Hot,
        )
        .expect("valid snapshot");
        assert!(selected.try_attach_mapped_file(Arc::clone(&mapped_file)));
        assert!(!selected.try_attach_mapped_file(Arc::clone(&mapped_file)));
        assert_eq!(ReferenceResource::get_ref_count(mapped_file.as_ref()), baseline + 1);

        MappedFile::shutdown(mapped_file.as_ref(), u64::MAX);
        let mut late = SelectMappedBufferResult::from_bytes(0, Bytes::from_static(b"mapped")).expect("valid snapshot");
        assert!(!late.try_attach_mapped_file(Arc::clone(&mapped_file)));
        assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 1);

        drop(selected);
        assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 0);
    }
}
