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

use thiserror::Error;

/// Errors that can occur during mapped file operations.
///
/// This enum provides detailed error information for various failure scenarios
/// when working with memory-mapped files, including I/O errors, bounds violations,
/// and resource exhaustion.
#[derive(Error, Debug)]
pub enum MappedFileError {
    /// Standard I/O error occurred during file operations.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Attempted to access memory outside the valid bounds of the mapped file.
    ///
    /// This error occurs when trying to read or write at an offset + size that
    /// exceeds the file's mapped region.
    #[error("Out of bounds access: offset={offset}, size={size}, file_size={file_size}")]
    OutOfBounds {
        /// The starting offset of the attempted access
        offset: usize,
        /// The size of the attempted access in bytes
        size: usize,
        /// The total size of the mapped file in bytes
        file_size: u64,
    },

    /// The mapped file has reached its capacity and cannot accept more writes.
    ///
    /// This error indicates that the write position has reached the file size limit.
    /// To continue writing, the file must be expanded or a new file created.
    #[error("File full: wrote={wrote}, capacity={capacity}")]
    FileFull {
        /// Current write position in bytes
        wrote: usize,
        /// Maximum capacity of the file in bytes
        capacity: u64,
    },

    /// Memory mapping operation failed.
    ///
    /// This can occur during initial mmap creation, remapping after file expansion,
    /// or when the system runs out of virtual address space.
    #[error("Memory mapping failed: {0}")]
    MmapFailed(String),

    /// File synchronization (fsync/msync) failed.
    ///
    /// This error indicates that persisting data to disk failed, which may result
    /// in data loss if the system crashes.
    #[error("Flush operation failed: {0}")]
    FlushFailed(String),

    /// Invalid file name or path provided.
    ///
    /// The file name must be a valid path with a parseable numeric offset.
    #[error("Invalid file name: {0}")]
    InvalidFileName(String),

    /// File expansion failed.
    ///
    /// This occurs when attempting to grow the file size via `set_len()` or
    /// similar operations.
    #[error("File expansion failed: current_size={current_size}, requested_size={requested_size}")]
    ExpansionFailed {
        /// Current file size in bytes
        current_size: u64,
        /// Requested new size in bytes
        requested_size: u64,
    },

    /// Reference counting error - attempted to use a file after all references dropped.
    #[error("Reference resource unavailable")]
    ReferenceUnavailable,

    /// Transient store pool exhausted.
    ///
    /// No buffers available in the transient store pool for write operations.
    #[error("Transient store pool exhausted")]
    TransientStoreExhausted,

    /// Configuration error.
    ///
    /// Indicates invalid configuration parameters were provided
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Generic error with custom message.
    #[error("{0}")]
    Custom(String),
}

/// Type alias for Results using `MappedFileError`.
pub type MappedFileResult<T> = Result<T, MappedFileError>;

impl MappedFileError {
    /// Creates an `OutOfBounds` error from the given parameters.
    ///
    /// # Arguments
    ///
    /// * `offset` - The starting offset of the access attempt
    /// * `size` - The size of the access attempt
    /// * `file_size` - The total file size
    ///
    /// # Returns
    ///
    /// A new `MappedFileError::OutOfBounds` variant
    #[inline]
    pub fn out_of_bounds(offset: usize, size: usize, file_size: u64) -> Self {
        Self::OutOfBounds {
            offset,
            size,
            file_size,
        }
    }

    /// Creates a `FileFull` error from the given parameters.
    ///
    /// # Arguments
    ///
    /// * `wrote` - Current write position
    /// * `capacity` - Maximum file capacity
    ///
    /// # Returns
    ///
    /// A new `MappedFileError::FileFull` variant
    #[inline]
    pub fn file_full(wrote: usize, capacity: u64) -> Self {
        Self::FileFull { wrote, capacity }
    }

    /// Checks if this error is recoverable.
    ///
    /// Some errors like `OutOfBounds` or `FileFull` are expected and recoverable,
    /// while others like `MmapFailed` typically indicate fatal system issues.
    ///
    /// # Returns
    ///
    /// `true` if the error is recoverable, `false` otherwise
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Self::OutOfBounds { .. } | Self::FileFull { .. } | Self::TransientStoreExhausted
        )
    }

    /// Checks if this error is an I/O error.
    ///
    /// # Returns
    ///
    /// `true` if the underlying cause is an I/O error
    pub fn is_io_error(&self) -> bool {
        matches!(self, Self::Io(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_out_of_bounds_error() {
        let err = MappedFileError::out_of_bounds(1000, 500, 1024);
        assert!(err.is_recoverable());
        assert!(err.to_string().contains("Out of bounds"));
    }

    #[test]
    fn test_file_full_error() {
        let err = MappedFileError::file_full(1024, 1024);
        assert!(err.is_recoverable());
        assert!(err.to_string().contains("File full"));
    }

    #[test]
    fn test_io_error() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err = MappedFileError::from(io_err);
        assert!(err.is_io_error());
    }

    #[test]
    fn test_unrecoverable_error() {
        let err = MappedFileError::MmapFailed("out of memory".to_string());
        assert!(!err.is_recoverable());
    }
}
