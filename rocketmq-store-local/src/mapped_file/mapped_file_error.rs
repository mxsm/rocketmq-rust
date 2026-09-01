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

use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use thiserror::Error;

use crate::transfer::segment::FileRangeFailure;
use crate::transfer::segment::FileRangeViolation;

use super::MappedFileOperation;

/// Errors that can occur during mapped file operations.
///
/// This enum provides detailed error information for various failure scenarios
/// when working with memory-mapped files, including I/O errors, bounds violations,
/// and resource exhaustion.
#[derive(Error, Debug)]
pub(crate) enum MappedFileFailure {
    /// Standard I/O error occurred during file operations.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Attempted to access memory outside the valid bounds of the mapped file.
    ///
    /// This error occurs when trying to read or write at an offset + size that
    /// exceeds the file's mapped region.
    #[error("{0}")]
    Violation(MappedFileViolation),

    /// A checked file range could not be constructed for the mapped file.
    #[error("File range operation failed: {0}")]
    FileRange(#[source] FileRangeFailure),

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
    MmapFailed(#[source] io::Error),

    /// File synchronization (fsync/msync) failed.
    ///
    /// This error indicates that persisting data to disk failed, which may result
    /// in data loss if the system crashes.
    #[error("Flush operation failed: {0}")]
    FlushFailed(#[source] io::Error),

    /// Locking the mapped region in physical memory failed.
    #[error("Mapped-memory lock failed: {0}")]
    MemoryLockFailed(#[source] rocketmq_error::RocketMQError),

    /// Unlocking the mapped region from physical memory failed.
    #[error("Mapped-memory unlock failed: {0}")]
    MemoryUnlockFailed(#[source] rocketmq_error::RocketMQError),

    /// Transient store pool exhausted.
    ///
    /// No buffers available in the transient store pool for write operations.
    #[error("Transient store pool exhausted")]
    TransientStoreExhausted,
}

/// Deterministic mapped-file contract rejection retained inside store-local.
#[derive(Debug)]
pub(crate) enum MappedFileViolation {
    OutOfBounds { offset: usize, size: usize, file_size: u64 },
    FileRange(FileRangeViolation),
    InvalidWritePosition { position: i32, capacity: u64 },
    InvalidWriteCommit { reserved: usize, actual: usize },
    WritePositionOverflow { position: usize },
    InvalidLease { operation: MappedFileOperation },
    Configuration(String),
}

impl std::fmt::Display for MappedFileViolation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutOfBounds {
                offset,
                size,
                file_size,
            } => write!(
                formatter,
                "out of bounds access: offset={offset}, size={size}, file_size={file_size}"
            ),
            Self::FileRange(error) => error.fmt(formatter),
            Self::InvalidWritePosition { position, capacity } => {
                write!(
                    formatter,
                    "invalid write position: position={position}, capacity={capacity}"
                )
            }
            Self::InvalidWriteCommit { reserved, actual } => {
                write!(
                    formatter,
                    "invalid write lease commit: reserved={reserved}, actual={actual}"
                )
            }
            Self::WritePositionOverflow { position } => {
                write!(formatter, "write position cannot be represented: position={position}")
            }
            Self::InvalidLease { operation } => {
                write!(formatter, "lease does not admit mapped-file operation {operation}")
            }
            Self::Configuration(message) => write!(formatter, "configuration error: {message}"),
        }
    }
}

impl MappedFileViolation {
    pub(crate) fn out_of_bounds(offset: usize, size: usize, file_size: u64) -> Self {
        Self::OutOfBounds {
            offset,
            size,
            file_size,
        }
    }
}

impl From<MappedFileViolation> for MappedFileFailure {
    fn from(error: MappedFileViolation) -> Self {
        Self::Violation(error)
    }
}

impl From<FileRangeFailure> for MappedFileFailure {
    fn from(error: FileRangeFailure) -> Self {
        match error {
            FileRangeFailure::Violation(error) => Self::Violation(MappedFileViolation::FileRange(error)),
            error @ FileRangeFailure::Metadata(_) => Self::FileRange(error),
        }
    }
}

impl MappedFileFailure {
    pub(crate) fn is_contract(&self) -> bool {
        matches!(
            self,
            Self::Violation(_) | Self::FileRange(FileRangeFailure::Violation(_))
        )
    }

    pub(crate) fn into_public_option<T>(
        result: Result<Option<T>, Self>,
        operation: StoreOperation,
    ) -> Result<Option<T>, StoreError> {
        match result {
            Ok(value) => Ok(value),
            Err(Self::Violation(_) | Self::FileRange(FileRangeFailure::Violation(_))) => Ok(None),
            Err(error) => Err(error.into_store_error(operation)),
        }
    }

    pub(crate) fn into_public_bool(result: Result<bool, Self>, operation: StoreOperation) -> Result<bool, StoreError> {
        match result {
            Ok(value) => Ok(value),
            Err(error) if error.is_contract() => Ok(false),
            Err(error) => Err(error.into_store_error(operation)),
        }
    }

    pub(crate) fn into_public_watermark(
        result: Result<i32, Self>,
        watermark: i32,
        operation: StoreOperation,
    ) -> Result<i32, StoreError> {
        match result {
            Ok(value) => Ok(value),
            Err(error) if error.is_contract() => Ok(watermark),
            Err(error) => Err(error.into_store_error(operation)),
        }
    }

    pub(crate) fn into_store_error(self, operation: StoreOperation) -> StoreError {
        match self {
            error @ (Self::Io(_)
            | Self::MmapFailed(_)
            | Self::FlushFailed(_)
            | Self::MemoryLockFailed(_)
            | Self::MemoryUnlockFailed(_)) => StoreError::new(&rocketmq_error::STORAGE_IO_FAILED, operation)
                .in_component(StoreComponent::MappedFile)
                .with_source(error),
            Self::FileRange(error @ FileRangeFailure::Metadata(_)) => {
                StoreError::new(&rocketmq_error::STORAGE_IO_FAILED, operation)
                    .in_component(StoreComponent::MappedFile)
                    .with_source(Self::FileRange(error))
            }
            error @ (Self::FileFull { .. } | Self::TransientStoreExhausted) => {
                StoreError::new(&rocketmq_error::STORAGE_CAPACITY_EXHAUSTED, operation)
                    .in_component(StoreComponent::MappedFile)
                    .with_source(error)
            }
            Self::Violation(_) | Self::FileRange(FileRangeFailure::Violation(_)) => {
                unreachable!("mapped-file violations do not cross the public contract projection")
            }
        }
    }

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
    /// A new `MappedFileFailure::OutOfBounds` variant
    #[inline]
    pub fn out_of_bounds(offset: usize, size: usize, file_size: u64) -> Self {
        Self::Violation(MappedFileViolation::out_of_bounds(offset, size, file_size))
    }

    pub(crate) fn invalid_write_position(position: i32, capacity: u64) -> Self {
        Self::Violation(MappedFileViolation::InvalidWritePosition { position, capacity })
    }

    pub(crate) fn invalid_write_commit(reserved: usize, actual: usize) -> Self {
        Self::Violation(MappedFileViolation::InvalidWriteCommit { reserved, actual })
    }

    pub(crate) fn write_position_overflow(position: usize) -> Self {
        Self::Violation(MappedFileViolation::WritePositionOverflow { position })
    }

    pub(crate) fn invalid_lease(operation: MappedFileOperation) -> Self {
        Self::Violation(MappedFileViolation::InvalidLease { operation })
    }

    pub(crate) fn configuration(message: String) -> Self {
        Self::Violation(MappedFileViolation::Configuration(message))
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
    /// A new `MappedFileFailure::FileFull` variant
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
    #[cfg(test)]
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Self::Violation(MappedFileViolation::OutOfBounds { .. })
                | Self::FileFull { .. }
                | Self::Violation(MappedFileViolation::InvalidWritePosition { .. })
                | Self::Violation(MappedFileViolation::InvalidWriteCommit { .. })
                | Self::TransientStoreExhausted
        )
    }

    /// Checks if this error is an I/O error.
    ///
    /// # Returns
    ///
    /// `true` if the underlying cause is an I/O error
    #[cfg(test)]
    pub fn is_io_error(&self) -> bool {
        matches!(self, Self::Io(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn test_out_of_bounds_error() {
        let err = MappedFileFailure::out_of_bounds(1000, 500, 1024);
        assert!(err.is_recoverable());
        assert!(err.to_string().contains("out of bounds"));
    }

    #[test]
    fn test_file_full_error() {
        let err = MappedFileFailure::file_full(1024, 1024);
        assert!(err.is_recoverable());
        assert!(err.to_string().contains("File full"));
    }

    #[test]
    fn test_io_error() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err = MappedFileFailure::from(io_err);
        assert!(err.is_io_error());
    }

    #[test]
    fn test_unrecoverable_error() {
        let err = MappedFileFailure::MmapFailed(io::Error::other("out of memory"));

        assert!(!err.is_recoverable());
        assert!(err.source().is_some());
    }

    #[test]
    fn file_range_errors_preserve_the_typed_source() {
        let err = MappedFileFailure::from(FileRangeFailure::Metadata(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "short metadata read",
        )));

        assert!(matches!(err, MappedFileFailure::FileRange(_)));
        assert!(err.source().is_some());
    }

    #[test]
    fn memory_lock_errors_preserve_the_typed_source() {
        let lock = MappedFileFailure::MemoryLockFailed(rocketmq_error::RocketMQError::internal(
            "lock mapped memory",
            io::Error::other("lock failed"),
        ));
        let unlock = MappedFileFailure::MemoryUnlockFailed(rocketmq_error::RocketMQError::internal(
            "unlock mapped memory",
            io::Error::other("unlock failed"),
        ));

        assert!(lock.source().is_some());
        assert!(unlock.source().is_some());
    }

    #[test]
    fn deterministic_mapped_file_violation_stays_on_the_contract_channel() {
        let result = MappedFileFailure::into_public_option::<()>(
            Err(MappedFileFailure::out_of_bounds(8, 4, 10)),
            StoreOperation::Read,
        );

        assert!(matches!(result, Ok(None)));
    }

    #[test]
    fn mapped_file_io_mapping_retains_the_typed_cause() {
        let error = MappedFileFailure::Io(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"))
            .into_store_error(StoreOperation::Read);

        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_IO_FAILED);
        let mapped = error
            .source()
            .and_then(|source| source.downcast_ref::<MappedFileFailure>())
            .expect("mapped-file failure remains typed");
        assert_eq!(
            mapped
                .source()
                .and_then(|source| source.downcast_ref::<io::Error>())
                .map(io::Error::kind),
            Some(io::ErrorKind::UnexpectedEof)
        );
    }

    #[test]
    fn file_full_mapping_retains_the_typed_capacity_cause() {
        let error = MappedFileFailure::file_full(8, 8).into_store_error(StoreOperation::Append);

        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_CAPACITY_EXHAUSTED);
        assert_eq!(error.operation(), StoreOperation::Append);
        assert_eq!(error.component(), StoreComponent::MappedFile);
        assert!(matches!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<MappedFileFailure>()),
            Some(MappedFileFailure::FileFull { wrote: 8, capacity: 8 })
        ));
    }
}
