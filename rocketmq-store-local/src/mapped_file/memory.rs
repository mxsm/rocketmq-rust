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
use std::ops::Deref;
use std::sync::Arc;

use memmap2::MmapMut;

/// Memory-mapping backend used by the canonical local mapped-file owner.
///
/// # Safety
///
/// Implementors must keep returned slices and regions backed by the same live mapping, serialize
/// mutable access so it cannot race with reads or writes, and keep the mapping valid independently
/// of compatible file-handle rename/reopen operations.
pub unsafe trait MappedMemory: Clone + Send + Sync + 'static {
    /// Owned immutable region suitable for zero-copy byte ownership.
    type Region: AsRef<[u8]> + Send + Sync + 'static;

    /// Maps the complete file as writable memory.
    fn map_mut(file: &File) -> io::Result<Self>;

    /// Returns the complete mapping as bytes.
    fn as_slice(&self) -> &[u8];

    /// Returns a writable pointer to the first mapped byte.
    ///
    /// Dereferencing the pointer remains unsafe; the caller must serialize all mutable access.
    fn as_mut_ptr(&self) -> *mut u8;

    /// Flushes the complete mapping.
    fn flush(&self) -> io::Result<()>;

    /// Flushes one mapped range.
    fn flush_range(&self, offset: usize, len: usize) -> io::Result<()>;

    /// Creates an owned immutable view over one mapped range.
    fn region(&self, offset: usize, len: usize) -> Self::Region;
}

/// Native writable mmap backend used by the default Local mapped-file owner.
#[derive(Clone)]
pub struct NativeMappedMemory {
    mmap: Arc<MmapMut>,
}

impl NativeMappedMemory {
    /// Returns another owner for the live native mapping.
    pub fn clone_mmap(&self) -> Arc<MmapMut> {
        self.mmap.clone()
    }
}

// SAFETY: construction maps an already-sized segment and `Arc` keeps that mapping alive across
// cloned regions. Mutable access follows the mapped-file contract requiring callers to serialize
// writes through the CommitLog/mapped-file ownership boundary.
unsafe impl MappedMemory for NativeMappedMemory {
    type Region = MmapRegionSlice;

    fn map_mut(file: &File) -> io::Result<Self> {
        // SAFETY: callers size the segment before mapping and do not resize it while the mapping is
        // live. NativeMappedMemory keeps the mapping alive independently of the file handle.
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
        MmapRegionSlice::new(self.mmap.clone(), offset, len)
    }
}

/// Immutable owner for one region of a native mapping.
pub struct MmapRegionSlice {
    mmap: Arc<MmapMut>,
    offset: usize,
    len: usize,
}

impl MmapRegionSlice {
    /// Creates a region backed by a live native mapping.
    pub fn new(mmap: Arc<MmapMut>, offset: usize, len: usize) -> Self {
        Self { mmap, offset, len }
    }
}

impl Deref for MmapRegionSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.mmap[self.offset..self.offset + self.len]
    }
}

impl AsRef<[u8]> for MmapRegionSlice {
    fn as_ref(&self) -> &[u8] {
        self
    }
}
