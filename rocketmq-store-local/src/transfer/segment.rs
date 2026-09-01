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

use std::fmt;
use std::fs::File;
use std::io;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;

use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;

use crate::mapped_file::file::FileOwner;
use crate::mapped_file::lifecycle::MappedFileLease;
#[cfg(test)]
use crate::mapped_file::DefaultMappedFile;
use crate::mapped_file::MappedFile;
use crate::mapped_file::MappedFileMetrics;
use crate::mapped_file::MappedReadRange;
use crate::mapped_file::NativeMappedMemory;
use crate::mapped_file::NativeReadOnlyMappedMemory;
use crate::mapped_file::SelectMappedBufferCacheState;
use crate::mapped_file::SelectMappedBufferResult;

#[cfg(test)]
type NativeDefaultMappedFile = DefaultMappedFile<NativeMappedMemory>;
type NativeSelectMappedBufferResult = SelectMappedBufferResult<NativeMappedMemory>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferCacheState {
    Unknown,
    Hot,
    Cold,
}

impl From<SelectMappedBufferCacheState> for TransferCacheState {
    fn from(value: SelectMappedBufferCacheState) -> Self {
        match value {
            SelectMappedBufferCacheState::Unknown => Self::Unknown,
            SelectMappedBufferCacheState::Hot => Self::Hot,
            SelectMappedBufferCacheState::Cold => Self::Cold,
        }
    }
}

enum SegmentSource {
    FileRange { lease: FileRangeLease },
    Bytes,
}

pub struct CommitLogSegment {
    global_offset: i64,
    file_offset: u64,
    position_in_file: u64,
    len: usize,
    source: SegmentSource,
    cache_state: TransferCacheState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckedFileRange {
    bounds: Range<u64>,
}

enum FileRangeOperationLease {
    Mapped {
        _lease: MappedFileLease,
    },
    MappedRead {
        _range: MappedReadRange<NativeReadOnlyMappedMemory>,
    },
    Standalone,
    #[cfg(test)]
    Probe {
        _probe: OperationDropProbe,
    },
}

#[cfg(test)]
struct OperationDropProbe {
    metrics: Arc<MappedFileMetrics>,
    observed_file_owners: Arc<std::sync::atomic::AtomicU64>,
}

#[cfg(test)]
impl Drop for OperationDropProbe {
    fn drop(&mut self) {
        self.observed_file_owners
            .store(self.metrics.file_owners_live(), std::sync::atomic::Ordering::Relaxed);
    }
}

struct FileRangeAliasInner {
    owner: Option<Arc<FileOwner>>,
    operation: Option<FileRangeOperationLease>,
}

impl FileRangeAliasInner {
    #[inline]
    fn owner(&self) -> &FileOwner {
        self.owner
            .as_deref()
            .expect("file-range owner is present until alias Drop")
    }

    #[cfg(unix)]
    fn is_standalone_compatibility(&self) -> bool {
        matches!(self.operation.as_ref(), Some(FileRangeOperationLease::Standalone))
    }
}

impl Drop for FileRangeAliasInner {
    fn drop(&mut self) {
        // The final operation release may run mapped-file cleanup synchronously. Drop the external
        // file owner first so cleanup can detach the slot and remove the namespace on Windows.
        drop(self.owner.take());
        drop(self.operation.take());
    }
}

/// Owner-bearing, admission-fenced capability for one checked file range.
///
/// Clones and splits share one owned M1 admission token and one canonical `FileOwner`. Admission
/// is therefore released once after the final derived range drops, while the physical handle
/// remains alive for every in-flight range.
#[derive(Clone)]
pub struct FileRangeLease {
    aliases: Arc<FileRangeAliasInner>,
    range: CheckedFileRange,
}

/// Duplicated transfer descriptor that cannot outlive its mapped-file range guard.
pub struct FileRangeTransferHandle {
    file: File,
    _range: FileRangeLease,
}

impl FileRangeTransferHandle {
    /// Returns the duplicated descriptor protected by the retained mapped-file range admission.
    #[inline]
    pub fn file(&self) -> &File {
        &self.file
    }
}

/// Legacy type name for the checked owner-bearing range capability.
///
/// Construction is intentionally fallible through [`SegmentLease::try_from_file_range`].
pub type FileRange = FileRangeLease;

/// Failure to construct or split a checked file-range capability.
#[derive(Debug)]
pub(crate) enum FileRangeError {
    LengthOverflow { len: usize },
    EndOverflow { position: u64, len: usize },
    OutOfBounds { position: u64, len: usize, file_len: u64 },
    InvalidSplit { at: usize, len: usize },
    Metadata(io::Error),
}

impl FileRangeError {
    /// Promotes this leaf into the canonical storage facade exactly once.
    ///
    /// Metadata faults keep their typed I/O source; range violations are
    /// invalid requests.
    pub(crate) fn into_store_error(self, operation: StoreOperation) -> StoreError {
        let descriptor = match &self {
            Self::Metadata(_) => &rocketmq_error::STORAGE_IO_FAILED,
            _ => &rocketmq_error::STORAGE_REQUEST_INVALID,
        };
        let component = if matches!(operation, StoreOperation::Replicate) {
            rocketmq_store_api::StoreComponent::HighAvailability
        } else {
            rocketmq_store_api::StoreComponent::CommitLog
        };
        StoreError::new(descriptor, operation)
            .in_component(component)
            .with_source(self)
    }
}

impl fmt::Display for FileRangeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LengthOverflow { len } => write!(formatter, "file range length does not fit u64: {len}"),
            Self::EndOverflow { position, len } => {
                write!(formatter, "file range end overflows: position={position}, len={len}")
            }
            Self::OutOfBounds {
                position,
                len,
                file_len,
            } => write!(
                formatter,
                "file range exceeds owner length: position={position}, len={len}, file_len={file_len}"
            ),
            Self::InvalidSplit { at, len } => {
                write!(formatter, "file range split exceeds range: at={at}, len={len}")
            }
            Self::Metadata(error) => write!(formatter, "failed to inspect file owner: {error}"),
        }
    }
}

impl std::error::Error for FileRangeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Metadata(error) => Some(error),
            _ => None,
        }
    }
}

pub struct SegmentLease {
    segment: CommitLogSegment,
    bytes: Option<Bytes>,
}

impl SegmentLease {
    /// Converts a selection using its authoritative global start offset.
    pub fn from_selection(result: NativeSelectMappedBufferResult) -> Option<Self> {
        let global_offset = i64::try_from(result.start_offset()).ok()?;
        Self::from_select_result(global_offset, result)
    }

    pub fn from_bytes(
        global_offset: i64,
        position_in_file: u64,
        bytes: Bytes,
        cache_state: TransferCacheState,
    ) -> Self {
        let len = bytes.len();
        let file_offset = global_offset.saturating_sub(position_in_file as i64) as u64;
        Self {
            segment: CommitLogSegment {
                global_offset,
                file_offset,
                position_in_file,
                len,
                source: SegmentSource::Bytes,
                cache_state,
            },
            bytes: Some(bytes),
        }
    }

    /// Creates a checked standalone range for compatibility tests and benchmarks.
    ///
    /// This constructor never manufactures mapped-file admission or retirement authority. Mapped-file
    /// production paths must use the internal `try_from_file_owner_range` constructor so the range carries
    /// the mapped file's owned operation admission.
    pub fn try_from_file_range(
        global_offset: i64,
        file_offset: u64,
        position_in_file: u64,
        len: usize,
        file: Arc<File>,
        cache_state: TransferCacheState,
    ) -> Result<Self, StoreError> {
        Self::try_from_file_range_typed(global_offset, file_offset, position_in_file, len, file, cache_state)
            .map_err(|error| error.into_store_error(StoreOperation::Replicate))
    }

    fn try_from_file_range_typed(
        global_offset: i64,
        file_offset: u64,
        position_in_file: u64,
        len: usize,
        file: Arc<File>,
        cache_state: TransferCacheState,
    ) -> Result<Self, FileRangeError> {
        let owner = Arc::new(FileOwner::from_shared_compatibility(
            file,
            Arc::new(MappedFileMetrics::new()),
        ));
        let lease = FileRangeLease::try_new(owner, position_in_file, len, FileRangeOperationLease::Standalone)?;
        Ok(Self {
            segment: CommitLogSegment {
                global_offset,
                file_offset,
                position_in_file,
                len,
                source: SegmentSource::FileRange { lease },
                cache_state,
            },
            bytes: None,
        })
    }

    /// Composes a canonical file owner with one owned mapped-file operation admission.
    pub(crate) fn try_from_file_owner_range(
        global_offset: i64,
        file_offset: u64,
        position_in_file: u64,
        len: usize,
        owner: Arc<FileOwner>,
        operation: MappedFileLease,
        cache_state: TransferCacheState,
    ) -> Result<Self, FileRangeError> {
        let lease = FileRangeLease::try_new(
            owner,
            position_in_file,
            len,
            FileRangeOperationLease::Mapped { _lease: operation },
        )?;
        Ok(Self {
            segment: CommitLogSegment {
                global_offset,
                file_offset,
                position_in_file,
                len,
                source: SegmentSource::FileRange { lease },
                cache_state,
            },
            bytes: None,
        })
    }

    /// Compatibility adapter for callers that already carry a separate global offset.
    pub fn from_select_result(global_offset: i64, result: NativeSelectMappedBufferResult) -> Option<Self> {
        let (_start_offset, bytes, mapped_range, file_range, size, mapped_source, position_in_file, cache_state) =
            result.into_transfer_parts();
        if size <= 0 {
            return None;
        }
        let len = size as usize;
        let file_offset = mapped_source
            .as_ref()
            .map(|(_lease, mapped_file)| mapped_file.get_file_from_offset())
            .unwrap_or_else(|| global_offset.saturating_sub(position_in_file as i64) as u64);
        let cache_state = cache_state.into();
        if let Some(lease) = file_range {
            return Some(Self {
                segment: CommitLogSegment {
                    global_offset,
                    file_offset,
                    position_in_file,
                    len,
                    source: SegmentSource::FileRange { lease },
                    cache_state,
                },
                bytes,
            });
        }
        if let Some(range) = mapped_range.as_ref() {
            if let Ok(lease) = range.clone().try_into_file_range() {
                return Some(Self {
                    segment: CommitLogSegment {
                        global_offset,
                        file_offset: range.file_from_offset(),
                        position_in_file: range.file_offset(),
                        len,
                        source: SegmentSource::FileRange { lease },
                        cache_state,
                    },
                    bytes,
                });
            }
        }
        let source = match mapped_source {
            Some((lease, mapped_file)) => match mapped_file.file_owner_admitted(&lease) {
                Ok(owner) => match Self::try_from_file_owner_range(
                    global_offset,
                    file_offset,
                    position_in_file,
                    len,
                    owner,
                    lease,
                    cache_state,
                ) {
                    Ok(mut segment) => {
                        segment.bytes = bytes;
                        return Some(segment);
                    }
                    Err(_) => SegmentSource::Bytes,
                },
                Err(_) => {
                    // The byte snapshot is independent. Drop the mapped-file owner before the
                    // admission in case final release runs cleanup synchronously.
                    drop(mapped_file);
                    drop(lease);
                    SegmentSource::Bytes
                }
            },
            None => SegmentSource::Bytes,
        };

        let bytes = bytes.or_else(|| mapped_range.map(|range| range.to_bytes()));
        Some(Self {
            segment: CommitLogSegment {
                global_offset,
                file_offset,
                position_in_file,
                len,
                source,
                cache_state,
            },
            bytes,
        })
    }

    pub fn segment(&self) -> &CommitLogSegment {
        &self.segment
    }

    pub fn as_bytes(&self) -> Option<Bytes> {
        self.bytes.clone()
    }

    pub fn as_file_range(&self) -> Option<FileRange> {
        match &self.segment.source {
            SegmentSource::FileRange { lease } => Some(lease.clone()),
            _ => None,
        }
    }

    pub fn len(&self) -> usize {
        self.segment.len
    }

    pub fn is_empty(&self) -> bool {
        self.segment.len == 0
    }
}

impl CommitLogSegment {
    #[inline]
    pub fn global_offset(&self) -> i64 {
        self.global_offset
    }

    #[inline]
    pub fn file_offset(&self) -> u64 {
        self.file_offset
    }

    #[inline]
    pub fn position_in_file(&self) -> u64 {
        self.position_in_file
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
    pub fn cache_state(&self) -> TransferCacheState {
        self.cache_state
    }
}

impl CheckedFileRange {
    fn try_new(position: u64, len: usize, file_len: u64) -> Result<Self, FileRangeError> {
        let range_len = u64::try_from(len).map_err(|_| FileRangeError::LengthOverflow { len })?;
        let end = position
            .checked_add(range_len)
            .ok_or(FileRangeError::EndOverflow { position, len })?;
        if end > file_len {
            return Err(FileRangeError::OutOfBounds {
                position,
                len,
                file_len,
            });
        }
        Ok(Self { bounds: position..end })
    }

    #[inline]
    fn position(&self) -> u64 {
        self.bounds.start
    }

    #[inline]
    fn len(&self) -> usize {
        // Every bound is derived from an input `usize` and later operations only shorten it.
        (self.bounds.end - self.bounds.start) as usize
    }
}

impl FileRangeLease {
    fn try_new(
        owner: Arc<FileOwner>,
        position: u64,
        len: usize,
        operation: FileRangeOperationLease,
    ) -> Result<Self, FileRangeError> {
        let file_len = match owner.len() {
            Ok(file_len) => file_len,
            Err(error) => {
                drop(owner);
                drop(operation);
                return Err(FileRangeError::Metadata(error));
            }
        };
        let range = match CheckedFileRange::try_new(position, len, file_len) {
            Ok(range) => range,
            Err(error) => {
                drop(owner);
                drop(operation);
                return Err(error);
            }
        };
        Ok(Self {
            aliases: Arc::new(FileRangeAliasInner {
                owner: Some(owner),
                operation: Some(operation),
            }),
            range,
        })
    }

    pub(crate) fn try_from_mapped_file(
        owner: Arc<FileOwner>,
        position: u64,
        len: usize,
        operation: MappedFileLease,
    ) -> Result<Self, FileRangeError> {
        Self::try_new(
            owner,
            position,
            len,
            FileRangeOperationLease::Mapped { _lease: operation },
        )
    }

    #[inline]
    pub fn position(&self) -> u64 {
        self.range.position()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.range.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.range.bounds.is_empty()
    }

    /// Copies exactly this checked range for compatibility engines that cannot consume file
    /// regions directly.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the validated file range can no longer be read completely.
    pub fn to_bytes(&self) -> io::Result<Bytes> {
        let mut output = vec![0_u8; self.len()];
        self.aliases.owner().read_exact_at(self.position(), &mut output)?;
        Ok(Bytes::from(output))
    }

    /// Creates a descriptor adapter while retaining this range's admission and owner guard.
    ///
    /// The descriptor field intentionally precedes the range guard so Windows closes the duplicate
    /// before releasing the final retirement admission.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the operating-system descriptor cannot be duplicated.
    pub fn try_transfer_handle(&self) -> io::Result<FileRangeTransferHandle> {
        let file = self.aliases.owner().try_clone_for_transfer()?;
        Ok(FileRangeTransferHandle {
            file,
            _range: self.clone(),
        })
    }

    /// Splits this capability at a relative byte offset.
    ///
    /// Both returned ranges share the same physical owner and admission. The operation admission
    /// is released once after the final derived range drops.
    #[allow(dead_code, reason = "exercised by the in-crate transfer scenarios")]
    pub(crate) fn split_at(mut self, at: usize) -> Result<(Self, Self), FileRangeError> {
        let len = self.len();
        if at > len {
            return Err(FileRangeError::InvalidSplit { at, len });
        }
        let relative = u64::try_from(at).map_err(|_| FileRangeError::LengthOverflow { len: at })?;
        let middle = self
            .range
            .bounds
            .start
            .checked_add(relative)
            .ok_or(FileRangeError::EndOverflow {
                position: self.range.bounds.start,
                len: at,
            })?;
        let right = Self {
            aliases: Arc::clone(&self.aliases),
            range: CheckedFileRange {
                bounds: middle..self.range.bounds.end,
            },
        };
        self.range.bounds.end = middle;
        Ok((self, right))
    }

    #[inline]
    pub(crate) fn truncate_to(&mut self, maximum_len: usize) {
        let retained = self.len().min(maximum_len);
        self.range.bounds.end = self.range.bounds.start + retained as u64;
    }

    /// Runs one sendfile operation without exposing a managed input descriptor.
    ///
    /// The injected operation is retained solely for the explicitly standalone compatibility
    /// constructor. Owner-bound mapped ranges always execute the native syscall inside `FileOwner`
    /// while both the physical owner and mapped-file admission remain held.
    #[cfg(unix)]
    pub(crate) fn sendfile_to(
        &self,
        out_fd: std::os::fd::RawFd,
        offset: u64,
        len: usize,
        unmanaged_operation: impl FnOnce(std::os::fd::RawFd, std::os::fd::RawFd, u64, usize) -> io::Result<usize>,
    ) -> io::Result<usize> {
        let requested_end = offset
            .checked_add(
                u64::try_from(len)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "sendfile length overflow"))?,
            )
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "sendfile range end overflow"))?;
        if offset < self.range.bounds.start || requested_end > self.range.bounds.end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sendfile request exceeds the owned file range",
            ));
        }
        if self.aliases.is_standalone_compatibility() {
            return self
                .aliases
                .owner()
                .sendfile_to_unmanaged(out_fd, offset, len, unmanaged_operation);
        }
        self.aliases.owner().sendfile_to(out_fd, offset, len)
    }

    #[cfg(test)]
    fn read_exact_for_test(&self, output: &mut [u8]) -> io::Result<()> {
        self.aliases.owner().read_exact_at_for_test(self.position(), output)
    }
}

impl MappedReadRange<NativeReadOnlyMappedMemory> {
    /// Converts this mapped range into a checked file-transfer range without copying payload.
    ///
    /// The returned capability retains this range's generation and read admission, so native
    /// transfer cannot outlive the mapped-file lifecycle protection.
    pub(crate) fn try_into_file_range(self) -> Result<FileRangeLease, FileRangeError> {
        let owner = self.file_owner();
        let position = self.file_offset();
        let len = self.len();
        FileRangeLease::try_new(
            owner,
            position,
            len,
            FileRangeOperationLease::MappedRead { _range: self },
        )
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use cheetah_string::CheetahString;

    use super::*;
    fn mapped_file() -> (tempfile::TempDir, Arc<NativeDefaultMappedFile>) {
        let directory = tempfile::tempdir().expect("temporary mapped-file directory");
        let path = directory.path().join("00000000000000000000");
        let mapped_file =
            NativeDefaultMappedFile::try_new(CheetahString::from(path.to_string_lossy().into_owned()), 64)
                .expect("mapped file");
        (directory, Arc::new(mapped_file))
    }

    fn selected_lease(mapped_file: &Arc<NativeDefaultMappedFile>, len: usize) -> SegmentLease {
        let bytes = mapped_file.get_bytes_readable_checked(0, len).expect("selected bytes");
        let selected =
            NativeSelectMappedBufferResult::try_from_mapped_snapshot(0, 0, bytes, Arc::clone(mapped_file), true)
                .expect("mapped snapshot");
        SegmentLease::from_selection(selected).expect("owning segment lease")
    }

    fn read_file_range(range: &FileRange) -> Vec<u8> {
        let mut bytes = vec![0; range.len()];
        range.read_exact_for_test(&mut bytes).expect("read range");
        bytes
    }

    #[test]
    fn canonical_selection_conversion_uses_checked_start_offset() {
        let selected = NativeSelectMappedBufferResult::from_bytes_with_metadata(
            123,
            7,
            Bytes::from_static(b"x"),
            true,
            SelectMappedBufferCacheState::Hot,
        )
        .expect("valid selection");
        let lease = SegmentLease::from_selection(selected).expect("representable global offset");
        assert_eq!(lease.segment().global_offset(), 123);
        assert_eq!(lease.segment().position_in_file(), 7);
        assert_eq!(lease.segment().file_offset(), 116);

        let overflow = NativeSelectMappedBufferResult::from_bytes((i64::MAX as u64) + 1, Bytes::from_static(b"x"))
            .expect("valid byte selection");
        assert!(SegmentLease::from_selection(overflow).is_none());
    }

    #[test]
    fn compatibility_conversion_preserves_explicit_global_offset() {
        let selected =
            NativeSelectMappedBufferResult::from_bytes(123, Bytes::from_static(b"x")).expect("valid byte selection");
        let lease = SegmentLease::from_select_result(9, selected).expect("compatibility conversion");
        assert_eq!(lease.segment().global_offset(), 9);
    }

    #[test]
    fn copy_lease_and_file_range_have_identical_bytes() {
        let (_directory, mapped_file) = mapped_file();
        let payload = b"owning-segment";
        assert!(mapped_file.append_message_bytes(payload));
        let copied = mapped_file
            .get_bytes_readable_checked(0, payload.len())
            .expect("copied bytes");
        let lease = selected_lease(&mapped_file, payload.len());
        let leased = lease.as_bytes().expect("leased fallback bytes");
        let ranged = read_file_range(&lease.as_file_range().expect("file range"));

        assert_eq!(&copied[..], payload);
        assert_eq!(&leased[..], payload);
        assert_eq!(&ranged[..], payload);
    }

    #[test]
    fn published_segment_is_immutable_while_later_bytes_are_appended() {
        let (_directory, mapped_file) = mapped_file();
        assert!(mapped_file.append_message_bytes(b"published"));
        let lease = selected_lease(&mapped_file, 9);

        assert!(mapped_file.append_message_bytes(b"-tail"));

        assert_eq!(lease.as_bytes().as_deref(), Some(&b"published"[..]));
        assert_eq!(
            read_file_range(&lease.as_file_range().expect("file range")),
            b"published"
        );
    }

    #[test]
    fn lease_drop_releases_exactly_one_mapped_file_hold() {
        let (_directory, mapped_file) = mapped_file();
        assert!(mapped_file.append_message_bytes(b"lease"));
        let baseline = mapped_file.lifecycle_snapshot().active_leases;

        let lease = selected_lease(&mapped_file, 5);
        assert_eq!(mapped_file.lifecycle_snapshot().active_leases, baseline + 1);

        drop(lease);
        assert_eq!(mapped_file.lifecycle_snapshot().active_leases, baseline);
    }

    #[test]
    fn live_lease_fences_destroy_until_drop() {
        let (_directory, mapped_file) = mapped_file();
        assert!(mapped_file.append_message_bytes(b"fenced"));
        let lease = selected_lease(&mapped_file, 6);

        assert!(!mapped_file.destroy(1_000));
        assert!(!mapped_file.lifecycle_snapshot().logical_cleanup_marked);

        drop(lease);
        assert!(mapped_file.lifecycle_snapshot().logical_cleanup_marked);
    }

    #[test]
    fn exported_file_range_keeps_admission_after_segment_drop() {
        let (_directory, mapped_file) = mapped_file();
        assert!(mapped_file.append_message_bytes(b"range"));
        let lease = selected_lease(&mapped_file, 5);
        let range = lease.as_file_range().expect("file range");

        drop(lease);
        MappedFile::shutdown(mapped_file.as_ref(), u64::MAX);
        assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 1);
        assert_eq!(read_file_range(&range), b"range");

        drop(range);
        assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 0);
        assert!(mapped_file.lifecycle_snapshot().logical_cleanup_marked);
    }

    #[test]
    fn checked_file_range_rejects_overflow_and_out_of_bounds() {
        let mut file = tempfile::tempfile().expect("temporary file");
        file.write_all(b"four").expect("write file");
        let file = Arc::new(file);

        assert!(matches!(
            SegmentLease::try_from_file_range_typed(0, 0, 3, 2, Arc::clone(&file), TransferCacheState::Cold),
            Err(FileRangeError::OutOfBounds { .. })
        ));
        assert!(matches!(
            SegmentLease::try_from_file_range_typed(0, 0, u64::MAX, 2, file, TransferCacheState::Cold),
            Err(FileRangeError::EndOverflow { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn managed_range_sendfile_never_invokes_the_unmanaged_descriptor_seam() {
        let file = tempfile::tempfile().expect("temporary file");
        file.set_len(1).expect("size temporary file");
        let metrics = Arc::new(MappedFileMetrics::new());
        let owner =
            Arc::new(FileOwner::try_new_managed(file, Arc::clone(&metrics)).expect("capture managed physical owner"));
        let observed_file_owners = Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX));
        let range = FileRangeLease::try_new(
            owner,
            0,
            1,
            FileRangeOperationLease::Probe {
                _probe: OperationDropProbe {
                    metrics,
                    observed_file_owners,
                },
            },
        )
        .expect("managed checked range");
        let unmanaged_called = std::cell::Cell::new(false);

        let error = range
            .sendfile_to(-1, 0, 1, |_, _, _, _| {
                unmanaged_called.set(true);
                Ok(1)
            })
            .expect_err("invalid output descriptor must fail inside managed sendfile");

        assert!(!unmanaged_called.get());
        assert_ne!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[cfg(unix)]
    #[test]
    fn standalone_range_retains_only_the_explicit_unmanaged_sendfile_seam() {
        let file = tempfile::tempfile().expect("temporary file");
        file.set_len(1).expect("size temporary file");
        let segment = SegmentLease::try_from_file_range(0, 0, 0, 1, Arc::new(file), TransferCacheState::Cold)
            .expect("standalone checked range");
        let range = segment.as_file_range().expect("file range");
        let unmanaged_called = std::cell::Cell::new(false);

        let written = range
            .sendfile_to(-1, 0, 1, |_, _, offset, len| {
                unmanaged_called.set(true);
                assert_eq!((offset, len), (0, 1));
                Ok(1)
            })
            .expect("standalone compatibility operation");

        assert!(unmanaged_called.get());
        assert_eq!(written, 1);
    }

    #[test]
    fn final_range_alias_drops_file_owner_before_operation() {
        let metrics = Arc::new(MappedFileMetrics::new());
        let owner = Arc::new(FileOwner::new_tracked(
            tempfile::tempfile().expect("temporary file"),
            Arc::clone(&metrics),
        ));
        let observed_file_owners = Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX));
        let range = FileRangeLease::try_new(
            owner,
            0,
            0,
            FileRangeOperationLease::Probe {
                _probe: OperationDropProbe {
                    metrics: Arc::clone(&metrics),
                    observed_file_owners: Arc::clone(&observed_file_owners),
                },
            },
        )
        .expect("empty checked range");
        assert_eq!(metrics.file_owners_live(), 1);

        drop(range);

        assert_eq!(observed_file_owners.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(metrics.file_owners_live(), 0);
        assert_eq!(metrics.physical_file_owner_drop_total(), 1);
    }

    #[test]
    fn split_ranges_share_one_admission_until_the_last_piece_drops() {
        let (_directory, mapped_file) = mapped_file();
        assert!(mapped_file.append_message_bytes(b"split"));
        let segment = selected_lease(&mapped_file, 5);
        let range = segment.as_file_range().expect("file range");
        let (left, right) = range.split_at(2).expect("checked split");

        assert_eq!((left.position(), left.len()), (0, 2));
        assert_eq!((right.position(), right.len()), (2, 3));
        drop(segment);
        MappedFile::shutdown(mapped_file.as_ref(), u64::MAX);
        assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 1);

        drop(left);
        assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 1);
        drop(right);
        assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 0);
        assert!(mapped_file.lifecycle_snapshot().logical_cleanup_marked);
    }
}
