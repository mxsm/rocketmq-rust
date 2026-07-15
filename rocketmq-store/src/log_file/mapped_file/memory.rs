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
use rocketmq_store_local::mapped_file::MappedMemory;

/// Compatibility mapping backend retained by the Store facade during owner migration.
#[derive(Clone)]
pub struct StoreMappedMemory {
    mmap: Arc<MmapMut>,
}

impl StoreMappedMemory {
    pub fn clone_mmap(&self) -> Arc<MmapMut> {
        self.mmap.clone()
    }
}

// SAFETY: construction maps an already-sized segment and `Arc` keeps that mapping alive across
// cloned regions. Mutable access preserves the legacy Store requirement that callers serialize
// writes through the mapped-file/CommitLog locks.
unsafe impl MappedMemory for StoreMappedMemory {
    type Region = MmapRegionSlice;

    fn map_mut(file: &File) -> io::Result<Self> {
        // SAFETY: callers size the segment before mapping and do not resize it while the mapping is
        // live. StoreMappedMemory keeps the mapping alive independently of the file handle.
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

/// Immutable owner for one region of the compatibility mapping backend.
pub struct MmapRegionSlice {
    mmap: Arc<MmapMut>,
    offset: usize,
    len: usize,
}

impl MmapRegionSlice {
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
