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
use std::sync::Arc;

use bytes::Bytes;

use crate::mapped_file::lifecycle::MappedFileLease;
use crate::mapped_file::DefaultMappedFile;
use crate::mapped_file::MappedFile;
use crate::mapped_file::NativeMappedMemory;
use crate::mapped_file::SelectMappedBufferCacheState;
use crate::mapped_file::SelectMappedBufferResult;

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
    Mmap {
        _mapped_file: Arc<NativeDefaultMappedFile>,
    },
    FileRange {
        file: Arc<File>,
        _mapped_file: Option<Arc<NativeDefaultMappedFile>>,
    },
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

#[derive(Clone)]
pub struct FileRange {
    #[cfg_attr(
        not(unix),
        allow(dead_code, reason = "sendfile consumes the file handle only on Unix targets")
    )]
    file: Arc<File>,
    position: u64,
    len: usize,
    _mapped_file_lease: Option<Arc<MappedFileLease>>,
}

pub struct SegmentLease {
    segment: CommitLogSegment,
    bytes: Option<Bytes>,
    // Drop source handles before releasing the admission that fences their use.
    _mapped_file_lease: Option<Arc<MappedFileLease>>,
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
            _mapped_file_lease: None,
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

    pub fn from_file_range(
        global_offset: i64,
        file_offset: u64,
        position_in_file: u64,
        len: usize,
        file: Arc<File>,
        cache_state: TransferCacheState,
    ) -> Self {
        Self {
            _mapped_file_lease: None,
            segment: CommitLogSegment {
                global_offset,
                file_offset,
                position_in_file,
                len,
                source: SegmentSource::FileRange {
                    file,
                    _mapped_file: None,
                },
                cache_state,
            },
            bytes: None,
        }
    }

    /// Compatibility adapter for callers that already carry a separate global offset.
    pub fn from_select_result(global_offset: i64, result: NativeSelectMappedBufferResult) -> Option<Self> {
        let (_start_offset, bytes, size, mapped_source, position_in_file, cache_state) = result.into_transfer_parts();
        if size <= 0 {
            return None;
        }
        let len = size as usize;
        let file_offset = mapped_source
            .as_ref()
            .map(|(_lease, mapped_file)| mapped_file.get_file_from_offset())
            .unwrap_or_else(|| global_offset.saturating_sub(position_in_file as i64) as u64);
        let (source, mapped_file_lease) = match mapped_source {
            Some((lease, mapped_file)) => match mapped_file.try_clone_file_admitted(&lease) {
                Ok(file) => (
                    SegmentSource::FileRange {
                        file: Arc::new(file),
                        _mapped_file: Some(mapped_file),
                    },
                    Some(Arc::new(lease)),
                ),
                Err(_) => (
                    SegmentSource::Mmap {
                        _mapped_file: mapped_file,
                    },
                    Some(Arc::new(lease)),
                ),
            },
            None => (SegmentSource::Bytes, None),
        };

        Some(Self {
            _mapped_file_lease: mapped_file_lease,
            segment: CommitLogSegment {
                global_offset,
                file_offset,
                position_in_file,
                len,
                source,
                cache_state: cache_state.into(),
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
            SegmentSource::FileRange { file, .. } => Some(FileRange {
                file: file.clone(),
                position: self.segment.position_in_file,
                len: self.segment.len,
                _mapped_file_lease: self._mapped_file_lease.clone(),
            }),
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

impl FileRange {
    #[inline]
    pub fn position(&self) -> u64 {
        self.position
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[cfg(any(unix, test))]
    #[inline]
    pub(crate) fn file(&self) -> &File {
        self.file.as_ref()
    }

    #[cfg(unix)]
    #[inline]
    pub(crate) fn truncate_to(&mut self, maximum_len: usize) {
        self.len = self.len.min(maximum_len);
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::io::Seek;
    use std::io::SeekFrom;

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
        let mut file = range.file().try_clone().expect("clone range file");
        file.seek(SeekFrom::Start(range.position())).expect("seek range");
        let mut bytes = vec![0; range.len()];
        file.read_exact(&mut bytes).expect("read range");
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
}
