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
use std::ops::Range;
use std::sync::Arc;

use memmap2::MmapMut;

/// Failure to construct a safe view over a mapped range.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MmapRangeError {
    /// `offset + len` cannot be represented by [`usize`].
    #[error("mapped range offset {offset} + length {len} overflowed")]
    Overflow { offset: usize, len: usize },
    /// The representable range extends beyond the live mapping.
    #[error("mapped range {offset}..+{len} exceeds mapping length {mapping_len}")]
    OutOfBounds {
        offset: usize,
        len: usize,
        mapping_len: usize,
    },
}

/// Memory-mapping backend used by the canonical local mapped-file owner.
///
/// # Safety
///
/// Implementors must keep returned slices and regions backed by the same live mapping, serialize
/// mutable access so it cannot race with reads or writes, reject overflowing or out-of-bounds
/// regions before construction, and keep the mapping valid independently of compatible file-handle
/// rename/reopen operations.
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

    /// Copies one disjoint source slice into a checked mapped range.
    ///
    /// # Safety
    ///
    /// The caller must serialize mutable access to the mapping and ensure `source` does not
    /// overlap the mapped allocation. Implementors must uphold the pointer validity contract of
    /// [`Self::as_mut_ptr`].
    unsafe fn copy_from_slice(&self, offset: usize, source: &[u8]) -> io::Result<()> {
        let mapping_len = self.as_slice().len();
        let end = offset
            .checked_add(source.len())
            .filter(|end| *end <= mapping_len)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "mapped write range {offset}..+{} exceeds mapping length {mapping_len}",
                        source.len()
                    ),
                )
            })?;
        debug_assert!(end <= mapping_len);

        // SAFETY: the trait's caller contract requires exclusive mutation and disjoint source and
        // destination allocations. The checked range proves the destination is live and in bounds.
        unsafe {
            std::ptr::copy_nonoverlapping(source.as_ptr(), self.as_mut_ptr().add(offset), source.len());
        }
        Ok(())
    }

    /// Flushes the complete mapping.
    fn flush(&self) -> io::Result<()>;

    /// Flushes one mapped range.
    fn flush_range(&self, offset: usize, len: usize) -> io::Result<()>;

    /// Creates an owned immutable view over one mapped range.
    ///
    /// # Errors
    ///
    /// Returns [`MmapRangeError::Overflow`] when `offset + len` cannot be
    /// represented, or [`MmapRangeError::OutOfBounds`] when it exceeds the
    /// mapping.
    fn region(&self, offset: usize, len: usize) -> Result<Self::Region, MmapRangeError>;
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

    fn region(&self, offset: usize, len: usize) -> Result<Self::Region, MmapRangeError> {
        MmapRegionSlice::try_new(self.mmap.clone(), offset, len)
    }
}

/// Immutable owner for one region of a native mapping.
pub struct MmapRegionSlice {
    mmap: Arc<MmapMut>,
    range: Range<usize>,
}

impl MmapRegionSlice {
    /// Creates a checked region backed by a live native mapping.
    ///
    /// # Errors
    ///
    /// Returns [`MmapRangeError::Overflow`] when `offset + len` cannot be
    /// represented, or [`MmapRangeError::OutOfBounds`] when the resulting range
    /// exceeds the mapping.
    pub fn try_new(mmap: Arc<MmapMut>, offset: usize, len: usize) -> Result<Self, MmapRangeError> {
        let range = checked_mmap_range(mmap.len(), offset, len)?;
        Ok(Self { mmap, range })
    }
}

fn checked_mmap_range(mapping_len: usize, offset: usize, len: usize) -> Result<Range<usize>, MmapRangeError> {
    let end = offset
        .checked_add(len)
        .ok_or(MmapRangeError::Overflow { offset, len })?;
    if end > mapping_len {
        return Err(MmapRangeError::OutOfBounds {
            offset,
            len,
            mapping_len,
        });
    }
    Ok(offset..end)
}

impl Deref for MmapRegionSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.mmap[self.range.clone()]
    }
}

impl AsRef<[u8]> for MmapRegionSlice {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_ranges_distinguish_valid_overflow_and_out_of_bounds() {
        assert_eq!(checked_mmap_range(8, 0, 0), Ok(0..0));
        assert_eq!(checked_mmap_range(8, 0, 8), Ok(0..8));
        assert_eq!(checked_mmap_range(8, 8, 0), Ok(8..8));
        assert_eq!(
            checked_mmap_range(8, 8, 1),
            Err(MmapRangeError::OutOfBounds {
                offset: 8,
                len: 1,
                mapping_len: 8,
            })
        );
        assert_eq!(
            checked_mmap_range(8, usize::MAX, 1),
            Err(MmapRangeError::Overflow {
                offset: usize::MAX,
                len: 1,
            })
        );
        assert_eq!(
            checked_mmap_range(8, 1, usize::MAX),
            Err(MmapRangeError::Overflow {
                offset: 1,
                len: usize::MAX,
            })
        );
    }
}
