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

use std::alloc::alloc_zeroed;
use std::alloc::dealloc;
use std::alloc::Layout;
use std::fmt;
use std::ptr::NonNull;
use std::slice;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectIoValidationViolation {
    InvalidAlignment { alignment: usize },
    ZeroLength,
    UnalignedLength { len: usize, alignment: usize },
    UnalignedFileOffset { offset: u64, alignment: usize },
    AllocationFailed { len: usize, alignment: usize },
}

impl fmt::Display for DirectIoValidationViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DirectIoValidationViolation::InvalidAlignment { alignment } => {
                write!(
                    formatter,
                    "direct I/O alignment must be a non-zero power of two: {alignment}"
                )
            }
            DirectIoValidationViolation::ZeroLength => write!(formatter, "direct I/O buffer length must be non-zero"),
            DirectIoValidationViolation::UnalignedLength { len, alignment } => write!(
                formatter,
                "direct I/O buffer length must be aligned: len={len}, alignment={alignment}"
            ),
            DirectIoValidationViolation::UnalignedFileOffset { offset, alignment } => write!(
                formatter,
                "direct I/O file offset must be aligned: offset={offset}, alignment={alignment}"
            ),
            DirectIoValidationViolation::AllocationFailed { len, alignment } => write!(
                formatter,
                "failed to allocate direct I/O buffer: len={len}, alignment={alignment}"
            ),
        }
    }
}

impl std::error::Error for DirectIoValidationViolation {}

#[derive(Debug)]
pub struct DirectIoBuffer {
    ptr: NonNull<u8>,
    len: usize,
    alignment: usize,
}

impl DirectIoBuffer {
    pub fn new(len: usize, alignment: usize) -> Result<Self, DirectIoValidationViolation> {
        validate_alignment(alignment)?;
        validate_len(len, alignment)?;

        let layout = Layout::from_size_align(len, alignment)
            .map_err(|_| DirectIoValidationViolation::InvalidAlignment { alignment })?;
        // SAFETY: `layout` has a non-zero size and a validated power-of-two alignment.
        let ptr = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(ptr).ok_or(DirectIoValidationViolation::AllocationFailed { len, alignment })?;

        Ok(Self { ptr, len, alignment })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn alignment(&self) -> usize {
        self.alignment
    }

    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` owns an initialized allocation of exactly `len` bytes for `self`'s lifetime.
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: `&mut self` provides exclusive access to the initialized `len`-byte allocation.
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for DirectIoBuffer {
    fn drop(&mut self) {
        if let Ok(layout) = Layout::from_size_align(self.len, self.alignment) {
            // SAFETY: `ptr` was allocated with this exact layout and has not been deallocated.
            unsafe {
                dealloc(self.ptr.as_ptr(), layout);
            }
        }
    }
}

#[derive(Debug)]
pub struct DirectIoRequest {
    file_offset: u64,
    buffer: DirectIoBuffer,
}

impl DirectIoRequest {
    pub fn new(file_offset: u64, buffer: DirectIoBuffer) -> Result<Self, DirectIoValidationViolation> {
        let alignment = buffer.alignment();
        if !file_offset.is_multiple_of(alignment as u64) {
            return Err(DirectIoValidationViolation::UnalignedFileOffset {
                offset: file_offset,
                alignment,
            });
        }

        Ok(Self { file_offset, buffer })
    }

    #[inline]
    pub fn file_offset(&self) -> u64 {
        self.file_offset
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    #[inline]
    pub fn alignment(&self) -> usize {
        self.buffer.alignment()
    }

    #[inline]
    pub fn buffer(&self) -> &DirectIoBuffer {
        &self.buffer
    }

    #[inline]
    pub fn buffer_mut(&mut self) -> &mut DirectIoBuffer {
        &mut self.buffer
    }
}

fn validate_alignment(alignment: usize) -> Result<(), DirectIoValidationViolation> {
    if alignment == 0 || !alignment.is_power_of_two() {
        Err(DirectIoValidationViolation::InvalidAlignment { alignment })
    } else {
        Ok(())
    }
}

fn validate_len(len: usize, alignment: usize) -> Result<(), DirectIoValidationViolation> {
    if len == 0 {
        Err(DirectIoValidationViolation::ZeroLength)
    } else if !len.is_multiple_of(alignment) {
        Err(DirectIoValidationViolation::UnalignedLength { len, alignment })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_io_buffer_allocates_aligned_memory() {
        let buffer = DirectIoBuffer::new(8192, 4096).expect("aligned buffer");

        assert_eq!(buffer.len(), 8192);
        assert_eq!(buffer.alignment(), 4096);
        assert_eq!((buffer.as_ptr() as usize) % 4096, 0);
        assert_eq!(buffer.as_slice().len(), 8192);
    }

    #[test]
    fn direct_io_buffer_rejects_invalid_alignment_and_length() {
        assert_eq!(
            DirectIoBuffer::new(4096, 1000).expect_err("alignment must be power of two"),
            DirectIoValidationViolation::InvalidAlignment { alignment: 1000 }
        );
        assert_eq!(
            DirectIoBuffer::new(4097, 4096).expect_err("length must be aligned"),
            DirectIoValidationViolation::UnalignedLength {
                len: 4097,
                alignment: 4096,
            }
        );
    }

    #[test]
    fn direct_io_request_requires_aligned_file_offset() {
        let buffer = DirectIoBuffer::new(4096, 4096).expect("aligned buffer");
        let request = DirectIoRequest::new(8192, buffer).expect("aligned request");

        assert_eq!(request.file_offset(), 8192);
        assert_eq!(request.len(), 4096);

        let buffer = DirectIoBuffer::new(4096, 4096).expect("aligned buffer");
        assert_eq!(
            DirectIoRequest::new(1, buffer).expect_err("file offset must be aligned"),
            DirectIoValidationViolation::UnalignedFileOffset {
                offset: 1,
                alignment: 4096,
            }
        );
    }
}
