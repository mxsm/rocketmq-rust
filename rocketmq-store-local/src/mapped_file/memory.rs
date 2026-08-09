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
use std::ops::Range;

use memmap2::Mmap;
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

/// Writable memory-mapping backend used by an active mapped-file generation.
///
/// # Safety
///
/// Implementors must keep returned slices backed by the same live mapping, keep the mapped address
/// stable for the complete value lifetime, serialize mutable access so it cannot race with safe
/// reads, and keep the mapping valid independently of compatible file-handle rename operations.
pub unsafe trait MappedMemory: Send + Sync + Sized + 'static {
    /// Read-only mapping created when the writable generation is sealed.
    type ReadOnly: ReadOnlyMappedMemory;

    /// Maps the complete file as writable memory.
    ///
    /// # Safety
    ///
    /// The caller must keep the file at least as large as the returned mapping and prevent
    /// uncoordinated writes or truncation for the complete mapping lifetime. The returned value
    /// must immediately enter an owner-bound generation before safe access is exposed.
    unsafe fn map_mut(file: &File) -> io::Result<Self>;

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
}

/// Read-only memory-mapping backend used by a sealed mapped-file generation.
///
/// # Safety
///
/// Implementors must keep the mapped address stable for the complete value lifetime and must not
/// expose any mutation path for the mapped allocation. The file must not be resized or modified by
/// another writable mapping while a value of this type is live.
pub unsafe trait ReadOnlyMappedMemory: Send + Sync + Sized + 'static {
    /// Maps the complete file as read-only memory.
    ///
    /// # Safety
    ///
    /// The caller must keep the file at least as large as the returned mapping and prevent writes
    /// through every writable mapping, direct file write, and truncation for the complete mapping
    /// lifetime. The returned value must immediately enter an owner-bound read-only generation.
    unsafe fn map(file: &File) -> io::Result<Self>;

    /// Returns the complete immutable mapping as bytes.
    fn as_slice(&self) -> &[u8];
}

/// Native writable mmap backend used by the default Local mapped-file owner.
pub struct NativeMappedMemory {
    mmap: MmapMut,
}

// SAFETY: construction maps an already-sized segment and the enclosing mapping generation keeps
// this value alive. Mutable access follows the mapped-file contract requiring callers to serialize
// writes through the CommitLog/mapped-file ownership boundary; this type is not cloneable.
unsafe impl MappedMemory for NativeMappedMemory {
    type ReadOnly = NativeReadOnlyMappedMemory;

    unsafe fn map_mut(file: &File) -> io::Result<Self> {
        // SAFETY: callers size the segment before mapping and do not resize it while the mapping is
        // live. NativeMappedMemory keeps the mapping alive independently of the file handle.
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

/// Native read-only mmap backend used after a writable generation is sealed.
pub struct NativeReadOnlyMappedMemory {
    mmap: Mmap,
}

// SAFETY: construction creates a read-only mapping for an already-sized segment. The generation
// owner keeps it alive, and this type exposes no mutation path.
unsafe impl ReadOnlyMappedMemory for NativeReadOnlyMappedMemory {
    unsafe fn map(file: &File) -> io::Result<Self> {
        // SAFETY: callers keep the file sized and stable while the read-only generation is live.
        let mmap = unsafe { Mmap::map(file)? };
        Ok(Self { mmap })
    }

    fn as_slice(&self) -> &[u8] {
        &self.mmap
    }
}

pub(crate) fn checked_mmap_range(
    mapping_len: usize,
    offset: usize,
    len: usize,
) -> Result<Range<usize>, MmapRangeError> {
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
