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

use std::fmt;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use super::metrics::FileOwnerGaugeGuard;
use super::metrics::MappedFileMetrics;

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

/// Namespace identity for one mapped-file segment.
///
/// Identity deliberately excludes the operating-system handle. It remains available after the
/// physical owner has been detached so namespace removal can still be retried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileIdentity {
    path: PathBuf,
    file_from_offset: u64,
}

impl FileIdentity {
    /// Returns the authoritative namespace path.
    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the immutable base offset parsed when this identity was created.
    #[inline]
    pub fn file_from_offset(&self) -> u64 {
        self.file_from_offset
    }
}

enum FileHandle {
    Owned(File),
    // This exists only for the legacy standalone sendfile constructor. Mapped-file production
    // paths publish `Owned` handles and never share a naked `Arc<File>`.
    SharedCompatibility(Arc<File>),
}

impl FileHandle {
    #[inline]
    fn as_file(&self) -> &File {
        match self {
            Self::Owned(file) => file,
            Self::SharedCompatibility(file) => file.as_ref(),
        }
    }
}

/// Canonical strong owner of an operating-system file handle.
///
/// Mapped-file mappings and transfer ranges retain this value through `Arc<FileOwner>`. The live
/// gauge guard is declared after the handle so its `Drop` records physical release only after the
/// owned handle has been dropped.
pub struct FileOwner {
    handle: FileHandle,
    _gauge: FileOwnerGaugeGuard,
}

impl fmt::Debug for FileOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileOwner")
            .field("len", &self.len().ok())
            .finish_non_exhaustive()
    }
}

impl FileOwner {
    /// Creates the canonical owner from an owner-bound metrics guard.
    pub(crate) fn new(file: File, gauge: FileOwnerGaugeGuard) -> Self {
        Self {
            handle: FileHandle::Owned(file),
            _gauge: gauge,
        }
    }

    /// Creates a canonical owner and binds its lifetime to `metrics`.
    pub(crate) fn new_tracked(file: File, metrics: Arc<MappedFileMetrics>) -> Self {
        Self::new(file, metrics.track_file_owner())
    }

    /// Adapts a legacy standalone transfer handle without cloning the operating-system handle.
    ///
    /// Mapped-file production paths must use [`Self::new_tracked`] and capture the storage owner
    /// instead.
    pub(crate) fn from_shared_compatibility(file: Arc<File>, metrics: Arc<MappedFileMetrics>) -> Self {
        let gauge = metrics.track_file_owner();
        Self {
            handle: FileHandle::SharedCompatibility(file),
            _gauge: gauge,
        }
    }

    /// Runs an operation while this owner keeps the file handle alive.
    ///
    /// The higher-ranked callback prevents the borrowed `File` from escaping the call.
    #[inline]
    pub(crate) fn with_file<R>(&self, operation: impl for<'file> FnOnce(&'file File) -> R) -> R {
        operation(self.handle.as_file())
    }

    /// Returns the current file length without exposing the handle.
    #[inline]
    pub fn len(&self) -> io::Result<u64> {
        self.with_file(|file| file.metadata().map(|metadata| metadata.len()))
    }

    /// Returns whether the underlying file is empty.
    #[inline]
    pub fn is_empty(&self) -> io::Result<bool> {
        self.len().map(|len| len == 0)
    }

    /// Returns a descriptor whose validity is bounded by the borrowed owner capability.
    #[cfg(unix)]
    #[inline]
    pub(crate) fn raw_fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;

        self.with_file(File::as_raw_fd)
    }
}

/// Owns mutable namespace identity and an exactly-once takeable physical-owner slot.
///
/// Configured file size remains in mapped-file progress state. Taking the owner never removes the
/// path identity, allowing namespace retirement to be retried independently of physical release.
#[derive(Debug)]
pub struct MappedFileStorage {
    identity: FileIdentity,
    owner: Mutex<Option<Arc<FileOwner>>>,
    metrics: Arc<MappedFileMetrics>,
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
        Self::open_with_metrics(path, file_size, Arc::new(MappedFileMetrics::new()))
    }

    /// Opens storage whose physical owners report into `metrics`.
    pub(crate) fn open_with_metrics(
        path: PathBuf,
        file_size: u64,
        metrics: Arc<MappedFileMetrics>,
    ) -> io::Result<(Self, Option<FilePreallocateOutcome>)> {
        Self::open_with_preallocator_and_metrics(path, file_size, preallocate_file, metrics)
    }

    #[cfg(test)]
    fn open_with_preallocator<P>(
        path: PathBuf,
        file_size: u64,
        preallocator: P,
    ) -> io::Result<(Self, Option<FilePreallocateOutcome>)>
    where
        P: FnOnce(&File, u64) -> FilePreallocateOutcome,
    {
        Self::open_with_preallocator_and_metrics(path, file_size, preallocator, Arc::new(MappedFileMetrics::new()))
    }

    fn open_with_preallocator_and_metrics<P>(
        path: PathBuf,
        file_size: u64,
        preallocator: P,
        metrics: Arc<MappedFileMetrics>,
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
        let owner = Arc::new(FileOwner::new_tracked(file, Arc::clone(&metrics)));

        Ok((
            Self {
                identity: FileIdentity { path, file_from_offset },
                owner: Mutex::new(Some(owner)),
                metrics,
            },
            preallocation,
        ))
    }

    fn owner_slot(&self) -> MutexGuard<'_, Option<Arc<FileOwner>>> {
        self.owner.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Captures the currently published physical owner.
    ///
    /// A captured owner remains valid after a concurrent [`Self::take_owner`].
    #[inline]
    pub(crate) fn owner(&self) -> Option<Arc<FileOwner>> {
        self.owner_slot().clone()
    }

    /// Takes slot ownership exactly once without changing namespace identity.
    #[inline]
    pub(crate) fn take_owner(&self) -> Option<Arc<FileOwner>> {
        self.owner_slot().take()
    }

    /// Returns namespace identity independently of physical-owner attachment.
    #[inline]
    pub fn identity(&self) -> &FileIdentity {
        &self.identity
    }

    /// Runs a scoped operation against the currently published owner.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::NotConnected`] after the owner slot has been detached.
    pub fn with_file<R>(&self, operation: impl for<'file> FnOnce(&'file File) -> R) -> io::Result<R> {
        self.owner()
            .ok_or_else(owner_detached_error)
            .map(|owner| owner.with_file(operation))
    }

    /// Returns the authoritative path for this segment.
    ///
    /// A successful [`Self::rename`] updates this path. A failed rename leaves it unchanged.
    #[inline]
    pub fn path(&self) -> &Path {
        self.identity.path()
    }

    /// Returns the segment offset parsed when the storage was opened.
    ///
    /// Renaming the file does not change this value.
    #[inline]
    pub fn file_from_offset(&self) -> u64 {
        self.identity.file_from_offset()
    }

    /// Renames the segment and updates its authoritative path.
    ///
    /// This operation does not reopen the file. Existing owner-bound mappings and ranges retain
    /// the same operating-system handle across the namespace rename.
    ///
    /// # Errors
    ///
    /// Returns the filesystem rename error. On error, the path and file handle remain unchanged.
    pub fn rename(&mut self, path: &Path) -> io::Result<()> {
        fs::rename(self.identity.path(), path)?;
        self.identity.path = path.to_path_buf();
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
    /// Returns an error if the authoritative path cannot be opened or if detach won before the
    /// replacement could be published. In the latter case the candidate owner is dropped and the
    /// slot remains empty.
    pub fn reopen(&mut self) -> io::Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .open(self.identity.path())?;
        let candidate = Arc::new(FileOwner::new_tracked(file, Arc::clone(&self.metrics)));
        let mut slot = self.owner_slot();
        if slot.is_none() {
            return Err(owner_detached_error());
        }
        *slot = Some(candidate);
        Ok(())
    }

    /// Removes the segment at its authoritative path.
    ///
    /// # Errors
    ///
    /// Returns the filesystem removal error, including when the path does not exist.
    #[inline]
    pub fn delete(&self) -> io::Result<()> {
        fs::remove_file(self.identity.path())
    }
}

fn owner_detached_error() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "mapped-file owner has been detached")
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::sync::Arc;
    use std::sync::Barrier;

    use tempfile::tempdir;

    use super::FilePreallocateOutcome;
    use super::MappedFileStorage;
    use crate::mapped_file::MappedFileMetrics;

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

    #[test]
    fn identity_survives_exactly_once_owner_detach() {
        let directory = tempdir().expect("create temporary directory");
        let path = directory.path().join("16");
        let metrics = Arc::new(MappedFileMetrics::new());
        let (storage, _) =
            MappedFileStorage::open_with_metrics(path.clone(), 8, Arc::clone(&metrics)).expect("open tracked storage");

        assert_eq!(storage.path(), path);
        assert_eq!(storage.file_from_offset(), 16);
        assert_eq!(metrics.file_owners_live(), 1);

        let external_owner = storage.owner().expect("published file owner");
        let detached_owner = storage.take_owner().expect("detach file owner");
        assert!(Arc::ptr_eq(&external_owner, &detached_owner));
        assert!(storage.owner().is_none());
        assert!(storage.take_owner().is_none());

        assert_eq!(storage.path(), path);
        assert_eq!(storage.file_from_offset(), 16);
        assert_eq!(metrics.file_owners_live(), 1);
        assert_eq!(metrics.physical_file_owner_drop_total(), 0);

        drop(external_owner);
        assert_eq!(metrics.file_owners_live(), 1);
        drop(detached_owner);
        assert_eq!(metrics.file_owners_live(), 0);
        assert_eq!(metrics.physical_file_owner_drop_total(), 1);
    }

    #[test]
    fn reopen_cannot_republish_after_owner_detach() {
        let directory = tempdir().expect("create temporary directory");
        let path = directory.path().join("32");
        let metrics = Arc::new(MappedFileMetrics::new());
        let (mut storage, _) =
            MappedFileStorage::open_with_metrics(path, 8, Arc::clone(&metrics)).expect("open tracked storage");
        let owner = storage.take_owner().expect("detach file owner");

        let error = storage.reopen().expect_err("detached storage must stay detached");

        assert_eq!(error.kind(), std::io::ErrorKind::NotConnected);
        assert!(storage.owner().is_none());
        assert_eq!(metrics.file_owners_live(), 1);
        drop(owner);
        assert_eq!(metrics.file_owners_live(), 0);
        assert_eq!(metrics.physical_file_owner_drop_total(), 2);
    }

    #[test]
    fn reopen_preserves_length_and_publishes_a_writable_owner() {
        let directory = tempdir().expect("create temporary directory");
        let path = directory.path().join("40");
        let metrics = Arc::new(MappedFileMetrics::new());
        let (mut storage, _) =
            MappedFileStorage::open_with_metrics(path, 8, Arc::clone(&metrics)).expect("open tracked storage");
        storage
            .with_file(|file| file.set_len(12))
            .expect("attached owner")
            .expect("extend initial owner");

        storage.reopen().expect("reopen writable owner");

        assert_eq!(storage.owner().expect("replacement owner").len().unwrap(), 12);
        storage
            .with_file(|file| file.set_len(16))
            .expect("attached replacement")
            .expect("replacement remains writable");
        assert_eq!(storage.owner().expect("replacement owner").len().unwrap(), 16);
        assert_eq!(metrics.file_owners_live(), 1);
        assert_eq!(metrics.physical_file_owner_drop_total(), 1);
    }

    #[test]
    fn concurrent_owner_detach_has_one_winner() {
        let directory = tempdir().expect("create temporary directory");
        let path = directory.path().join("48");
        let metrics = Arc::new(MappedFileMetrics::new());
        let (storage, _) =
            MappedFileStorage::open_with_metrics(path, 8, Arc::clone(&metrics)).expect("open tracked storage");
        let storage = Arc::new(storage);
        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let storage = Arc::clone(&storage);
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                storage.take_owner()
            }));
        }

        barrier.wait();
        let detached = workers
            .into_iter()
            .filter_map(|worker| worker.join().expect("detach worker"))
            .collect::<Vec<_>>();

        assert_eq!(detached.len(), 1);
        assert!(storage.owner().is_none());
        assert_eq!(metrics.file_owners_live(), 1);
        drop(detached);
        assert_eq!(metrics.file_owners_live(), 0);
        assert_eq!(metrics.physical_file_owner_drop_total(), 1);
    }
}
