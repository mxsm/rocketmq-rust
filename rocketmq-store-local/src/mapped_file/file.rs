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
use std::time::SystemTime;

use super::memory::MappedMemory;
use super::memory::ReadOnlyMappedMemory;
use super::metrics::FileOwnerGaugeGuard;
use super::metrics::MappedFileMetrics;
use super::retirement::identity::PhysicalFileKey;
use super::retirement::platform::physical_file_key;

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

fn open_segment_path(path: &Path, create: bool) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(create).truncate(false);
    configure_no_follow(&mut options);
    let file = options.open(path)?;
    reject_reparse_point(&file)?;
    Ok(file)
}

#[cfg(unix)]
fn configure_no_follow(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;

    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(windows)]
fn configure_no_follow(options: &mut OpenOptions) {
    use std::os::windows::fs::OpenOptionsExt;

    use windows::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;

    options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT.0);
}

#[cfg(not(any(unix, windows)))]
fn configure_no_follow(_options: &mut OpenOptions) {}

#[cfg(windows)]
fn reject_reparse_point(file: &File) -> io::Result<()> {
    use std::os::windows::fs::MetadataExt;

    use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

    let attributes = file.metadata()?.file_attributes();
    if attributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "mapped-file path must not be a reparse point",
        ));
    }
    Ok(())
}

#[cfg(not(windows))]
fn reject_reparse_point(_file: &File) -> io::Result<()> {
    Ok(())
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

/// Namespace and physical identity for one mapped-file segment.
///
/// Identity deliberately excludes the operating-system handle while retaining the key captured
/// from that handle. It remains available after the physical owner has been detached so namespace
/// retirement can reject a same-path replacement and remain retryable.
#[derive(Debug, Clone)]
pub struct FileIdentity {
    path: PathBuf,
    file_from_offset: u64,
    physical_key: Option<PhysicalFileKey>,
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

    /// Returns the physical-file key captured from the original open handle.
    #[inline]
    pub(crate) fn physical_key(&self) -> Option<PhysicalFileKey> {
        self.physical_key
    }

    /// Compares the complete verified binding used by managed namespace operations.
    ///
    /// Unlike the public compatibility equality implementation, an absent physical key never
    /// constitutes an exact managed binding.
    pub(crate) fn has_exact_physical_binding(&self, other: &Self) -> bool {
        self.path == other.path
            && self.file_from_offset == other.file_from_offset
            && matches!((self.physical_key, other.physical_key), (Some(left), Some(right)) if left == right)
    }
}

impl PartialEq for FileIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path && self.file_from_offset == other.file_from_offset
    }
}

impl Eq for FileIdentity {}

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
    physical_key: Option<PhysicalFileKey>,
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
    /// Creates an explicitly unverified owner for unit tests.
    ///
    /// This constructor never captures a physical key and cannot produce a managed storage owner.
    #[cfg(test)]
    pub(crate) fn new(file: File, gauge: FileOwnerGaugeGuard) -> Self {
        Self::new_with_physical_key(file, gauge, None)
    }

    fn new_with_physical_key(file: File, gauge: FileOwnerGaugeGuard, physical_key: Option<PhysicalFileKey>) -> Self {
        Self {
            handle: FileHandle::Owned(file),
            physical_key,
            _gauge: gauge,
        }
    }

    /// Creates an explicitly unverified tracked owner for unit tests.
    #[cfg(test)]
    pub(crate) fn new_tracked(file: File, metrics: Arc<MappedFileMetrics>) -> Self {
        Self::new(file, metrics.track_file_owner())
    }

    /// Creates a managed owner whose physical identity must be captured successfully.
    ///
    /// Unlike the legacy constructor, this function preserves identity-query failures for the
    /// activation and reconciliation layers to classify. It never publishes an unverified owner.
    pub(crate) fn try_new_managed(file: File, metrics: Arc<MappedFileMetrics>) -> io::Result<Self> {
        Self::try_new_managed_with(file, metrics, physical_file_key)
    }

    fn try_new_managed_with(
        file: File,
        metrics: Arc<MappedFileMetrics>,
        capture_key: impl FnOnce(&File) -> io::Result<PhysicalFileKey>,
    ) -> io::Result<Self> {
        let physical_key = capture_key(&file)?;
        Ok(Self::new_with_physical_key(
            file,
            metrics.track_file_owner(),
            Some(physical_key),
        ))
    }

    /// Adapts a legacy standalone transfer handle without cloning the operating-system handle.
    ///
    /// This compatibility owner is intentionally unverified: transfer ranges need lifetime
    /// ownership, not retirement authority. Managed mapped-file paths must capture their storage
    /// owner through [`Self::try_new_managed`] instead.
    pub(crate) fn from_shared_compatibility(file: Arc<File>, metrics: Arc<MappedFileMetrics>) -> Self {
        let gauge = metrics.track_file_owner();
        Self {
            handle: FileHandle::SharedCompatibility(file),
            physical_key: None,
            _gauge: gauge,
        }
    }

    /// Returns the physical-file key captured from this open handle.
    #[inline]
    pub(crate) fn physical_key(&self) -> Option<PhysicalFileKey> {
        self.physical_key
    }

    #[inline]
    fn with_file<R>(&self, operation: impl for<'file> FnOnce(&'file File) -> R) -> R {
        operation(self.handle.as_file())
    }

    /// Creates a writable mapping without exposing or cloning the owned file handle.
    ///
    /// # Safety
    ///
    /// The caller must uphold [`MappedMemory::map_mut`]'s file-stability contract and immediately
    /// place the result in a generation retaining this owner.
    pub(crate) unsafe fn map_mut<M: MappedMemory>(&self) -> io::Result<M> {
        // SAFETY: the caller assumes the complete `MappedMemory::map_mut` contract above.
        unsafe { M::map_mut(self.handle.as_file()) }
    }

    /// Creates a read-only mapping without exposing or cloning the owned file handle.
    ///
    /// # Safety
    ///
    /// The caller must uphold [`ReadOnlyMappedMemory::map`]'s immutability contract and immediately
    /// place the result in a generation retaining this owner.
    pub(crate) unsafe fn map_read_only<M: MappedMemory>(&self) -> io::Result<M::ReadOnly> {
        // SAFETY: the caller assumes the complete `ReadOnlyMappedMemory::map` contract above.
        unsafe { M::ReadOnly::map(self.handle.as_file()) }
    }

    #[inline]
    pub(crate) fn set_len(&self, len: u64) -> io::Result<()> {
        self.handle.as_file().set_len(len)
    }

    #[inline]
    pub(crate) fn modified(&self) -> io::Result<SystemTime> {
        self.handle.as_file().metadata()?.modified()
    }

    #[inline]
    fn preallocate_with(
        &self,
        len: u64,
        operation: impl FnOnce(&File, u64) -> FilePreallocateOutcome,
    ) -> FilePreallocateOutcome {
        operation(self.handle.as_file(), len)
    }

    #[cfg(test)]
    pub(crate) fn read_exact_at_for_test(&self, offset: u64, output: &mut [u8]) -> io::Result<()> {
        use std::io::Read;
        use std::io::Seek;
        use std::io::SeekFrom;

        let mut file = self.handle.as_file();
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(output)
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
    fn raw_fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;

        self.handle.as_file().as_raw_fd()
    }

    /// Runs one native sendfile call while this owner keeps the input handle alive.
    #[cfg(all(unix, target_os = "linux"))]
    pub(crate) fn sendfile_to(&self, out_fd: std::os::fd::RawFd, offset: u64, len: usize) -> io::Result<usize> {
        let mut raw_offset = libc::off_t::try_from(offset).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("sendfile offset exceeds libc::off_t: {offset}"),
            )
        })?;
        // SAFETY: both descriptors remain live for this call, `raw_offset` is writable and is not
        // retained, and native failures are returned as `io::Error`.
        let written = unsafe { libc::sendfile(out_fd, self.raw_fd(), &mut raw_offset, len) };
        if written < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(written as usize)
        }
    }

    /// Reports native sendfile as unsupported on non-Linux Unix targets.
    #[cfg(all(unix, not(target_os = "linux")))]
    pub(crate) fn sendfile_to(&self, _out_fd: std::os::fd::RawFd, _offset: u64, _len: usize) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "sendfile transfer is only enabled on Linux",
        ))
    }

    /// Invokes the legacy injected operation only for an explicitly standalone owner.
    #[cfg(unix)]
    pub(crate) fn sendfile_to_unmanaged(
        &self,
        out_fd: std::os::fd::RawFd,
        offset: u64,
        len: usize,
        operation: impl FnOnce(std::os::fd::RawFd, std::os::fd::RawFd, u64, usize) -> io::Result<usize>,
    ) -> io::Result<usize> {
        if !matches!(&self.handle, FileHandle::SharedCompatibility(_)) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "managed file owner rejects the unmanaged sendfile seam",
            ));
        }
        operation(out_fd, self.raw_fd(), offset, len)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageMode {
    LegacyCompatibility,
    Managed,
}

/// Unforgeable staging permit for construction of managed storage in direct module tests.
///
/// Production retirement activation remains disabled in M3, so there is deliberately no
/// production constructor for this permit yet.
#[allow(dead_code, reason = "M3 stages managed storage before retirement activation wiring")]
pub(crate) struct ManagedStorageOpenPermit {
    _private: (),
}

impl ManagedStorageOpenPermit {
    #[cfg(test)]
    fn for_test() -> Self {
        Self { _private: () }
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
    mode: StorageMode,
}

impl MappedFileStorage {
    /// Opens or creates an explicitly unmanaged Wave-A compatibility segment.
    ///
    /// Existing bytes are preserved because the file is opened without truncation. The returned
    /// optional outcome is `Some` only when the previous file length was smaller than `file_size`
    /// and a preallocation attempt was made; `None` means no preallocation was attempted. The file
    /// length is set before any preallocation attempt.
    ///
    /// # Errors
    ///
    /// Returns an error if the final path component is not a numeric offset, the file cannot be
    /// opened or created, its physical identity cannot be captured, or its length cannot be set.
    pub fn open(path: PathBuf, file_size: u64) -> io::Result<(Self, Option<FilePreallocateOutcome>)> {
        Self::open_with_metrics(path, file_size, Arc::new(MappedFileMetrics::new()))
    }

    /// Opens storage whose physical owners report into `metrics`.
    pub(crate) fn open_with_metrics(
        path: PathBuf,
        file_size: u64,
        metrics: Arc<MappedFileMetrics>,
    ) -> io::Result<(Self, Option<FilePreallocateOutcome>)> {
        Self::open_with_preallocator_and_metrics(
            path,
            file_size,
            preallocate_file,
            metrics,
            StorageMode::LegacyCompatibility,
        )
    }

    /// Constructs managed storage from the exact handle retained by startup reconciliation.
    ///
    /// This path never opens, creates, resizes, or preallocates the namespace path. The durable
    /// offset, length, and physical key must all agree with the retained handle before it is
    /// published as the canonical owner.
    pub(crate) fn from_reconciled_file(
        path: PathBuf,
        expected_file_offset: u64,
        expected_file_size: u64,
        expected_physical_key: PhysicalFileKey,
        file: File,
        metrics: Arc<MappedFileMetrics>,
    ) -> io::Result<Self> {
        let file_from_offset = try_parse_file_from_offset(&path)?;
        if file_from_offset != expected_file_offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "reconciled mapped-file offset mismatch: path={file_from_offset}, durable={expected_file_offset}"
                ),
            ));
        }

        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "reconciled mapped-file handle is not a regular file",
            ));
        }
        if metadata.len() != expected_file_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "reconciled mapped-file length mismatch: actual={}, durable={expected_file_size}",
                    metadata.len()
                ),
            ));
        }

        let owner = Arc::new(FileOwner::try_new_managed(file, Arc::clone(&metrics))?);
        if owner.physical_key() != Some(expected_physical_key) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "reconciled mapped-file handle has a different physical identity",
            ));
        }

        Ok(Self {
            identity: FileIdentity {
                path,
                file_from_offset,
                physical_key: Some(expected_physical_key),
            },
            owner: Mutex::new(Some(owner)),
            metrics,
            mode: StorageMode::Managed,
        })
    }

    /// Opens storage that rejects every legacy handle and namespace escape.
    #[allow(dead_code, reason = "M3 stages managed storage before retirement activation wiring")]
    pub(crate) fn open_managed(
        path: PathBuf,
        file_size: u64,
        metrics: Arc<MappedFileMetrics>,
        _permit: ManagedStorageOpenPermit,
    ) -> io::Result<(Self, Option<FilePreallocateOutcome>)> {
        Self::open_with_preallocator_and_metrics(path, file_size, preallocate_file, metrics, StorageMode::Managed)
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
        Self::open_with_preallocator_and_metrics(
            path,
            file_size,
            preallocator,
            Arc::new(MappedFileMetrics::new()),
            StorageMode::LegacyCompatibility,
        )
    }

    fn open_with_preallocator_and_metrics<P>(
        path: PathBuf,
        file_size: u64,
        preallocator: P,
        metrics: Arc<MappedFileMetrics>,
        mode: StorageMode,
    ) -> io::Result<(Self, Option<FilePreallocateOutcome>)>
    where
        P: FnOnce(&File, u64) -> FilePreallocateOutcome,
    {
        let file_from_offset = try_parse_file_from_offset(&path)?;
        let file = open_segment_path(&path, true)?;
        let existing_len = file.metadata()?.len();
        let owner = Arc::new(FileOwner::try_new_managed(file, Arc::clone(&metrics))?);
        owner.set_len(file_size)?;
        let preallocation = (existing_len < file_size).then(|| owner.preallocate_with(file_size, preallocator));
        let physical_key = owner.physical_key();

        Ok((
            Self {
                identity: FileIdentity {
                    path,
                    file_from_offset,
                    physical_key,
                },
                owner: Mutex::new(Some(owner)),
                metrics,
                mode,
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

    /// Runs an unmanaged compatibility operation against the currently published owner.
    ///
    /// This API can duplicate the underlying file handle and is therefore rejected for managed
    /// storage. Retirement code must use narrow owner-bound operations instead.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::NotConnected`] after the owner slot has been detached.
    /// Returns [`io::ErrorKind::PermissionDenied`] for managed storage.
    pub fn with_file<R>(&self, operation: impl for<'file> FnOnce(&'file File) -> R) -> io::Result<R> {
        self.require_legacy_compatibility("with_file")?;
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

    /// Renames an unmanaged compatibility segment and updates its authoritative path.
    ///
    /// This operation does not reopen the file. Existing owner-bound mappings and ranges retain
    /// the same operating-system handle across the namespace rename.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::PermissionDenied`] for managed storage or the filesystem rename
    /// error for legacy storage. On error, the path and file handle remain unchanged. Retirement
    /// code must use its verified namespace transition instead of this compatibility method.
    pub fn rename(&mut self, path: &Path) -> io::Result<()> {
        self.require_legacy_compatibility("rename")?;
        fs::rename(self.identity.path(), path)?;
        self.identity.path = path.to_path_buf();
        Ok(())
    }

    /// Reopens the authoritative path without following its final component and replaces the
    /// current handle only when its physical key still matches this storage identity.
    ///
    /// The authoritative path is not changed by this operation. If reopening fails after a
    /// successful rename, the renamed path remains authoritative and the previous file handle is
    /// retained.
    ///
    /// # Errors
    ///
    /// Returns an error if the authoritative path cannot be opened, a managed physical key cannot
    /// be captured, the reopened file has a different key, or detach wins before the replacement
    /// can be published. No error path changes the storage identity.
    pub fn reopen(&mut self) -> io::Result<()> {
        if self.owner().is_none() {
            return Err(owner_detached_error());
        }
        self.identity
            .physical_key()
            .ok_or_else(unverified_physical_identity_error)?;
        let file = open_segment_path(self.identity.path(), false)?;
        let candidate = Arc::new(FileOwner::try_new_managed(file, Arc::clone(&self.metrics))?);
        let candidate_identity = FileIdentity {
            path: self.identity.path.clone(),
            file_from_offset: self.identity.file_from_offset,
            physical_key: candidate.physical_key(),
        };
        if !self.identity.has_exact_physical_binding(&candidate_identity) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "mapped-file path now refers to a different physical file",
            ));
        }
        let mut slot = self.owner_slot();
        if slot.is_none() {
            return Err(owner_detached_error());
        }
        *slot = Some(candidate);
        Ok(())
    }

    /// Removes an unmanaged compatibility segment at its authoritative path.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::PermissionDenied`] for managed storage or the filesystem removal
    /// error for legacy storage, including when the path does not exist. Retirement code must use
    /// its verified deletion capability instead of this compatibility method.
    #[inline]
    pub fn delete(&self) -> io::Result<()> {
        self.require_legacy_compatibility("delete")?;
        fs::remove_file(self.identity.path())
    }

    /// Returns the current modification time without exposing the owned file handle.
    pub(crate) fn modified(&self) -> io::Result<SystemTime> {
        self.owner().ok_or_else(owner_detached_error)?.modified()
    }

    fn require_legacy_compatibility(&self, operation: &'static str) -> io::Result<()> {
        if self.mode == StorageMode::Managed {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("managed mapped-file storage rejects legacy {operation}"),
            ));
        }
        Ok(())
    }
}

fn owner_detached_error() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "mapped-file owner has been detached")
}

fn unverified_physical_identity_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "mapped-file storage has no verified physical-file key",
    )
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::io;
    use std::sync::Arc;
    use std::sync::Barrier;

    use tempfile::tempdir;

    use super::FileOwner;
    use super::FilePreallocateOutcome;
    use super::ManagedStorageOpenPermit;
    use super::MappedFileStorage;
    use super::PhysicalFileKey;
    use crate::mapped_file::retirement::platform::physical_file_key;
    use crate::mapped_file::MappedFileMetrics;

    #[test]
    fn public_file_identity_equality_preserves_the_legacy_namespace_contract() {
        let path = std::path::PathBuf::from("commitlog/00000000000000000064");
        let first = super::FileIdentity {
            path: path.clone(),
            file_from_offset: 64,
            physical_key: Some(PhysicalFileKey::unix(1, 2)),
        };
        let replacement = super::FileIdentity {
            path,
            file_from_offset: 64,
            physical_key: Some(PhysicalFileKey::unix(1, 3)),
        };

        assert_eq!(first, replacement);
        assert!(!first.has_exact_physical_binding(&replacement));
    }

    #[test]
    fn managed_storage_rejects_every_legacy_handle_and_namespace_escape() {
        let directory = tempdir().expect("create temporary directory");
        let path = directory.path().join("8");
        let renamed = directory.path().join("managed-renamed");
        let callback_called = Cell::new(false);
        let (mut storage, _) = MappedFileStorage::open_managed(
            path.clone(),
            8,
            Arc::new(MappedFileMetrics::new()),
            ManagedStorageOpenPermit::for_test(),
        )
        .expect("open managed storage");

        let with_file_error = storage
            .with_file(|_| callback_called.set(true))
            .expect_err("managed storage must not expose its owner handle");
        let rename_error = storage
            .rename(&renamed)
            .expect_err("managed storage must not use the legacy rename path");
        let delete_error = storage
            .delete()
            .expect_err("managed storage must not use the legacy delete path");

        assert_eq!(with_file_error.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(rename_error.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(delete_error.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(!callback_called.get());
        assert!(path.exists());
        assert!(!renamed.exists());
    }

    #[test]
    fn public_storage_open_remains_an_explicit_legacy_compatibility_path() {
        let directory = tempdir().expect("create temporary directory");
        let original = directory.path().join("9");
        let renamed = directory.path().join("legacy-renamed");
        let (mut storage, _) = MappedFileStorage::open(original.clone(), 8).expect("open legacy storage");

        let len = storage
            .with_file(|file| file.metadata().map(|metadata| metadata.len()))
            .expect("legacy owner remains exposed")
            .expect("inspect legacy owner");
        assert_eq!(len, 8);
        storage.rename(&renamed).expect("legacy rename remains available");
        storage.delete().expect("legacy delete remains available");

        assert!(!original.exists());
        assert!(!renamed.exists());
    }

    #[test]
    fn managed_owner_handle_primitives_stay_private_to_the_file_module() {
        let source = include_str!("file.rs").replace("\r\n", "\n");
        let production = source
            .split_once("\n#[cfg(test)]\nmod tests {")
            .expect("unit tests follow production file-owner code")
            .0;

        assert!(production.contains("fn with_file<R>"));
        assert!(!production.contains("pub(crate) fn with_file<R>"));
        assert!(!production.contains("pub(crate) fn raw_fd"));
    }

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
        let physical_key = storage.identity().physical_key();
        assert!(physical_key.is_some());
        assert_eq!(metrics.file_owners_live(), 1);

        let external_owner = storage.owner().expect("published file owner");
        assert_eq!(external_owner.physical_key(), physical_key);
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
        assert_eq!(metrics.physical_file_owner_drop_total(), 1);
    }

    #[test]
    fn failed_legacy_reopen_never_upgrades_the_storage_identity() {
        let directory = tempdir().expect("create temporary directory");
        let path = directory.path().join("36");
        let metrics = Arc::new(MappedFileMetrics::new());
        let (mut storage, _) =
            MappedFileStorage::open_with_metrics(path, 8, Arc::clone(&metrics)).expect("open tracked storage");
        storage.identity.physical_key = None;
        let owner = storage.take_owner().expect("detach legacy file owner");

        let error = storage
            .reopen()
            .expect_err("detached legacy storage must stay detached");

        assert_eq!(error.kind(), std::io::ErrorKind::NotConnected);
        assert_eq!(storage.identity().physical_key(), None);
        drop(owner);
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
    fn reopen_rejects_a_same_path_replacement() {
        let directory = tempdir().expect("create temporary directory");
        let path = directory.path().join("44");
        let metrics = Arc::new(MappedFileMetrics::new());
        let (mut storage, _) =
            MappedFileStorage::open_with_metrics(path.clone(), 8, Arc::clone(&metrics)).expect("open tracked storage");
        let original_key = storage.identity().physical_key();

        let displaced = directory.path().join("original-44");
        std::fs::rename(&path, displaced).expect("move original out of the canonical namespace");
        let replacement = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("create same-path replacement");
        replacement.set_len(8).expect("size replacement");

        let error = storage.reopen().expect_err("replacement identity must be rejected");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(storage.identity().physical_key(), original_key);
        assert_eq!(
            storage
                .owner()
                .expect("original owner remains published")
                .physical_key(),
            original_key
        );
    }

    #[test]
    fn unverified_reopen_rejects_a_same_path_replacement_and_retains_the_owner() {
        let directory = tempdir().expect("create temporary directory");
        let path = directory.path().join("45");
        let metrics = Arc::new(MappedFileMetrics::new());
        let (mut storage, _) =
            MappedFileStorage::open_with_metrics(path.clone(), 8, Arc::clone(&metrics)).expect("open tracked storage");
        let original_owner = storage.owner().expect("capture original owner");
        let original_key = original_owner.physical_key();
        storage.identity.physical_key = None;

        let displaced = directory.path().join("original-45");
        std::fs::rename(&path, displaced).expect("move original out of the canonical namespace");
        let replacement = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("create same-path replacement");
        replacement.set_len(8).expect("size replacement");

        let error = storage
            .reopen()
            .expect_err("an unverified identity must never authorize reopen");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(storage.identity().physical_key(), None);
        let retained_owner = storage.owner().expect("original owner remains published");
        assert!(Arc::ptr_eq(&retained_owner, &original_owner));
        assert_eq!(retained_owner.physical_key(), original_key);
    }

    #[test]
    fn managed_owner_requires_and_retains_a_physical_key() {
        let directory = tempdir().expect("create temporary directory");
        let path = directory.path().join("47");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .expect("create managed file");

        let owner = FileOwner::try_new_managed(file, Arc::new(MappedFileMetrics::new()))
            .expect("capture managed physical identity");

        assert!(owner.physical_key().is_some());
    }

    #[test]
    fn managed_owner_preserves_a_physical_key_capture_error() {
        let file = tempfile::tempfile().expect("create managed file");
        let metrics = Arc::new(MappedFileMetrics::new());

        let error = FileOwner::try_new_managed_with(file, Arc::clone(&metrics), |_| {
            Err(std::io::Error::from_raw_os_error(12_345))
        })
        .expect_err("identity capture must fail");

        assert_eq!(error.raw_os_error(), Some(12_345));
        assert_eq!(metrics.file_owners_live(), 0);
    }

    #[test]
    fn reconciled_storage_consumes_the_exact_retained_handle_without_path_reopen() {
        let directory = tempdir().expect("create temporary directory");
        let path = directory.path().join("49");
        let retained = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("create reconciled segment");
        retained.set_len(8).expect("size reconciled segment");
        let expected_key = physical_file_key(&retained).expect("capture reconciled identity");

        let displaced = directory.path().join("retained-49");
        std::fs::rename(&path, &displaced).expect("move retained segment after inventory");
        let replacement = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("create same-path replacement");
        replacement.set_len(8).expect("size replacement");

        let storage = MappedFileStorage::from_reconciled_file(
            path.clone(),
            49,
            8,
            expected_key,
            retained,
            Arc::new(MappedFileMetrics::new()),
        )
        .expect("retained handle is authoritative");

        assert_eq!(storage.path(), path);
        assert_eq!(storage.file_from_offset(), 49);
        assert_eq!(storage.identity().physical_key(), Some(expected_key));
        assert_eq!(
            storage.owner().expect("retained owner").physical_key(),
            Some(expected_key)
        );
        assert_eq!(
            storage
                .with_file(|_| ())
                .expect_err("managed storage never exposes a legacy handle")
                .kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn reconciled_storage_rejects_offset_length_and_physical_key_mismatches() {
        let directory = tempdir().expect("create temporary directory");
        let create_file = || {
            let file = tempfile::tempfile().expect("create retained handle");
            file.set_len(8).expect("size retained handle");
            file
        };
        let offset_file = create_file();
        let offset_key = physical_file_key(&offset_file).expect("capture offset identity");

        let offset_error = MappedFileStorage::from_reconciled_file(
            directory.path().join("50"),
            49,
            8,
            offset_key,
            offset_file,
            Arc::new(MappedFileMetrics::new()),
        )
        .expect_err("durable offset must match the canonical basename");
        assert_eq!(offset_error.kind(), io::ErrorKind::InvalidData);

        let length_file = create_file();
        let length_key = physical_file_key(&length_file).expect("capture length identity");
        let length_error = MappedFileStorage::from_reconciled_file(
            directory.path().join("49"),
            49,
            9,
            length_key,
            length_file,
            Arc::new(MappedFileMetrics::new()),
        )
        .expect_err("durable length must match the retained handle");
        assert_eq!(length_error.kind(), io::ErrorKind::InvalidData);

        let wrong_key_file = create_file();
        let foreign_key_file = create_file();
        let foreign_key = physical_file_key(&foreign_key_file).expect("capture foreign identity");
        let wrong_key_error = MappedFileStorage::from_reconciled_file(
            directory.path().join("49"),
            49,
            8,
            foreign_key,
            wrong_key_file,
            Arc::new(MappedFileMetrics::new()),
        )
        .expect_err("durable key must match the retained handle");
        assert_eq!(wrong_key_error.kind(), io::ErrorKind::InvalidData);
    }

    #[cfg(unix)]
    #[test]
    fn open_rejects_a_final_component_symlink_without_touching_its_target() {
        use std::os::unix::fs::symlink;

        let directory = tempdir().expect("create temporary directory");
        let target = directory.path().join("target");
        std::fs::write(&target, [1_u8, 2, 3]).expect("write symlink target");
        let mapped_path = directory.path().join("46");
        symlink(&target, &mapped_path).expect("create mapped-file symlink");

        assert!(MappedFileStorage::open(mapped_path, 8).is_err());
        assert_eq!(std::fs::read(&target).expect("read untouched target"), [1_u8, 2, 3]);
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
