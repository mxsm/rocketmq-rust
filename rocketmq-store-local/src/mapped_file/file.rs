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

//! File-handle and path ownership for local mapped-file segments.
//!
//! This module owns file lifecycle operations but does not own the configured segment size or
//! memory mapping. Callers retain responsibility for mapping and observability policy.

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::path::PathBuf;

/// Legacy error number used when file preallocation is unavailable.
pub const PREALLOCATE_UNSUPPORTED_ERRNO: i32 = 95;

/// Describes the result of an optional file preallocation attempt.
///
/// The file length is established separately with [`File::set_len`] before preallocation. A
/// non-successful outcome therefore reports degraded allocation behavior rather than an invalid
/// file length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilePreallocateOutcome {
    /// The requested range was preallocated or no allocation was required.
    Allocated,
    /// The platform or filesystem does not support the requested preallocation operation.
    Unsupported {
        /// Operating-system error number returned by the preallocation operation.
        errno: i32,
    },
    /// The preallocation operation failed for a reason other than known lack of support.
    Failed {
        /// Operating-system error number returned by the preallocation operation.
        errno: i32,
    },
}

impl FilePreallocateOutcome {
    /// Returns whether the outcome represents an unsupported optimization.
    ///
    /// A hard preallocation failure is not classified as degraded because callers preserve its
    /// distinct observability and logging path.
    pub fn is_degraded(self) -> bool {
        matches!(self, Self::Unsupported { .. })
    }
}

/// Classifies a native preallocation return value and error number.
///
/// A zero return value is successful. Known unsupported error numbers map to
/// [`FilePreallocateOutcome::Unsupported`]; all other nonzero results map to
/// [`FilePreallocateOutcome::Failed`].
pub fn classify_file_preallocate_result(result: i32, errno: i32) -> FilePreallocateOutcome {
    if result == 0 {
        FilePreallocateOutcome::Allocated
    } else if is_unsupported_preallocate_errno(errno) {
        FilePreallocateOutcome::Unsupported { errno }
    } else {
        FilePreallocateOutcome::Failed { errno }
    }
}

/// Attempts to reserve physical storage for the first `len` bytes of `file`.
///
/// On Linux this function invokes `fallocate`. Other platforms report
/// [`FilePreallocateOutcome::Unsupported`]. A zero length is treated as already allocated. Native
/// failures are returned as values so callers can preserve file creation after [`File::set_len`]
/// has succeeded.
pub fn preallocate_file(file: &File, len: u64) -> FilePreallocateOutcome {
    if len == 0 {
        return FilePreallocateOutcome::Allocated;
    }

    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;

        if len > i64::MAX as u64 {
            return FilePreallocateOutcome::Failed { errno: libc::EINVAL };
        }

        // SAFETY: `file.as_raw_fd()` is valid for the duration of this call, the offset is zero,
        // and `len` was checked to fit the platform `off_t` representation.
        let result = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, len as libc::off_t) };
        let errno = io::Error::last_os_error().raw_os_error().unwrap_or(0);
        classify_file_preallocate_result(result, errno)
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = file;
        FilePreallocateOutcome::Unsupported {
            errno: PREALLOCATE_UNSUPPORTED_ERRNO,
        }
    }
}

#[cfg(unix)]
fn is_unsupported_preallocate_errno(errno: i32) -> bool {
    errno == PREALLOCATE_UNSUPPORTED_ERRNO || errno == libc::ENOSYS || errno == libc::EINVAL
}

#[cfg(not(unix))]
fn is_unsupported_preallocate_errno(errno: i32) -> bool {
    errno == PREALLOCATE_UNSUPPORTED_ERRNO
}

fn invalid_input_error(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// Parses the numeric final path component as a mapped-file segment offset.
///
/// # Panics
///
/// Panics with the legacy compatibility message if the path has no UTF-8 final component or the
/// component is not a valid `u64`.
#[inline]
pub fn parse_file_from_offset(file_name: &Path) -> u64 {
    try_parse_file_from_offset(file_name).expect("File name parse to offset is invalid")
}

/// Tries to parse the numeric final path component as a mapped-file segment offset.
///
/// # Errors
///
/// Returns [`io::ErrorKind::InvalidInput`] if the path has no UTF-8 final component or the
/// component is not a valid `u64`.
#[inline]
pub fn try_parse_file_from_offset(file_name: &Path) -> io::Result<u64> {
    file_name
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.parse::<u64>().ok())
        .ok_or_else(|| invalid_input_error(format!("file name parse to offset is invalid: {}", file_name.display())))
}

/// Owns the operating-system file and canonical identity of one mapped-file segment.
///
/// The path is authoritative and changes only after a successful rename. The segment offset is
/// parsed once when the storage is opened and remains stable across renames. Configured file size
/// is intentionally owned by the mapped-file progress state and is not duplicated here.
#[derive(Debug)]
pub struct MappedFileStorage {
    file: File,
    path: PathBuf,
    file_from_offset: u64,
}

impl MappedFileStorage {
    /// Opens or creates a mapped-file segment and establishes its requested length.
    ///
    /// Existing bytes are preserved because the file is opened without truncation. The returned
    /// optional outcome is `Some` only when the previous file length was smaller than `file_size`
    /// and a preallocation attempt was made; `None` means no preallocation was attempted. The file
    /// length is set before any preallocation attempt.
    ///
    /// # Errors
    ///
    /// Returns an error if the final path component is not a numeric offset, the file cannot be
    /// opened or created, or its length cannot be set.
    pub fn open(path: PathBuf, file_size: u64) -> io::Result<(Self, Option<FilePreallocateOutcome>)> {
        Self::open_with_preallocator(path, file_size, preallocate_file)
    }

    fn open_with_preallocator<P>(
        path: PathBuf,
        file_size: u64,
        preallocator: P,
    ) -> io::Result<(Self, Option<FilePreallocateOutcome>)>
    where
        P: FnOnce(&File, u64) -> FilePreallocateOutcome,
    {
        let file_from_offset = try_parse_file_from_offset(&path)?;
        let existing_len = fs::metadata(&path).map(|metadata| metadata.len()).unwrap_or(0);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        file.set_len(file_size)?;
        let preallocation = (existing_len < file_size).then(|| preallocator(&file, file_size));

        Ok((
            Self {
                file,
                path,
                file_from_offset,
            },
            preallocation,
        ))
    }

    /// Returns the currently owned file handle.
    ///
    /// After a failed [`Self::reopen`], this remains the handle that was valid before the reopen
    /// attempt.
    #[inline]
    pub fn file(&self) -> &File {
        &self.file
    }

    /// Returns the authoritative path for this segment.
    ///
    /// A successful [`Self::rename`] updates this path before any later reopen attempt. A failed
    /// rename leaves it unchanged.
    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the segment offset parsed when the storage was opened.
    ///
    /// Renaming the file does not change this value.
    #[inline]
    pub fn file_from_offset(&self) -> u64 {
        self.file_from_offset
    }

    /// Renames the segment and updates its authoritative path.
    ///
    /// This operation does not reopen the file. Callers that require the legacy reopen sequence
    /// must update their path projection after this method succeeds and then call [`Self::reopen`].
    ///
    /// # Errors
    ///
    /// Returns the filesystem rename error. On error, the path and file handle remain unchanged.
    pub fn rename(&mut self, path: &Path) -> io::Result<()> {
        fs::rename(&self.path, path)?;
        self.path = path.to_path_buf();
        Ok(())
    }

    /// Reopens the authoritative path with [`File::open`] and replaces the current handle.
    ///
    /// The authoritative path is not changed by this operation. If reopening fails after a
    /// successful rename, the renamed path remains authoritative and the previous file handle is
    /// retained.
    ///
    /// # Errors
    ///
    /// Returns an error if the authoritative path cannot be opened.
    pub fn reopen(&mut self) -> io::Result<()> {
        let file = File::open(&self.path)?;
        self.file = file;
        Ok(())
    }

    /// Removes the segment at its authoritative path.
    ///
    /// # Errors
    ///
    /// Returns the filesystem removal error, including when the path does not exist.
    #[inline]
    pub fn delete(&self) -> io::Result<()> {
        fs::remove_file(&self.path)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use tempfile::tempdir;

    use super::FilePreallocateOutcome;
    use super::MappedFileStorage;

    #[test]
    fn preallocator_runs_only_when_previous_length_is_smaller() {
        let directory = tempdir().expect("create temporary directory");
        let path = directory.path().join("16");
        let calls = Cell::new(0);

        let (_, outcome) = MappedFileStorage::open_with_preallocator(path.clone(), 8, |_, len| {
            calls.set(calls.get() + 1);
            assert_eq!(len, 8);
            FilePreallocateOutcome::Failed { errno: 28 }
        })
        .expect("open new storage");
        assert_eq!(calls.get(), 1);
        assert_eq!(outcome, Some(FilePreallocateOutcome::Failed { errno: 28 }));

        let (_, outcome) = MappedFileStorage::open_with_preallocator(path, 4, |_, _| {
            calls.set(calls.get() + 1);
            FilePreallocateOutcome::Allocated
        })
        .expect("shrink storage");
        assert_eq!(calls.get(), 1);
        assert_eq!(outcome, None);
    }
}
