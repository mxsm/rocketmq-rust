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

use super::DefaultMappedFile;
use super::MappedFile;
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
    pub start_offset: u64,
    pub bytes: Option<Bytes>,
    /// The size.
    pub size: i32,
    /// The mapped file.
    pub mapped_file: Option<Arc<DefaultMappedFile<M>>>,
    /// Whether the buffer is in cache.
    pub is_in_cache: bool,
    /// Source kind used for observability and future transfer planning.
    pub source_kind: SelectMappedBufferSourceKind,
    /// Offset within the mapped file, when the result comes from a mapped file.
    pub file_offset: u64,
    /// Page-cache state observed when the result was selected.
    pub cache_state: SelectMappedBufferCacheState,
}

impl<M: MappedMemory> Default for SelectMappedBufferResult<M> {
    fn default() -> Self {
        Self {
            start_offset: 0,
            bytes: None,
            size: 0,
            mapped_file: None,
            is_in_cache: true,
            source_kind: SelectMappedBufferSourceKind::Bytes,
            file_offset: 0,
            cache_state: SelectMappedBufferCacheState::Unknown,
        }
    }
}

impl<M: MappedMemory> SelectMappedBufferResult<M> {
    /// Retains the mapped file as an optional transfer source.
    ///
    /// The immutable byte snapshot remains available when the mapped file can no longer be held,
    /// so callers can safely fall back to the copied representation.
    pub fn try_attach_mapped_file(&mut self, mapped_file: Arc<DefaultMappedFile<M>>) -> bool {
        if self.mapped_file.is_some() || !mapped_file.hold() {
            return false;
        }
        self.source_kind = SelectMappedBufferSourceKind::MappedFile;
        self.mapped_file = Some(mapped_file);
        true
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

    pub fn is_in_mem(&self) -> bool {
        match self.mapped_file.as_ref() {
            None => true,
            Some(inner) => {
                let pos = self.start_offset - inner.get_file_from_offset();
                inner.is_loaded(pos as i64, self.size as usize)
            }
        }
    }
}

impl<M: MappedMemory> Drop for SelectMappedBufferResult<M> {
    fn drop(&mut self) {
        if let Some(mapped_file) = self.mapped_file.take() {
            mapped_file.release()
        }
    }
}
