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

use crate::base::select_result::SelectMappedBufferCacheState;
use crate::base::select_result::SelectMappedBufferResult;
use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::MappedFile;

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

#[derive(Clone)]
pub enum SegmentSource {
    Mmap {
        mapped_file: Arc<DefaultMappedFile>,
    },
    FileRange {
        file: Arc<File>,
        mapped_file: Option<Arc<DefaultMappedFile>>,
    },
    Bytes,
}

#[derive(Clone)]
pub struct CommitLogSegment {
    pub global_offset: i64,
    pub file_offset: u64,
    pub position_in_file: u64,
    pub len: usize,
    pub source: SegmentSource,
    pub cache_state: TransferCacheState,
}

#[derive(Clone)]
pub struct FileRange {
    pub file: Arc<File>,
    pub position: u64,
    pub len: usize,
}

pub struct SegmentLease {
    segment: CommitLogSegment,
    bytes: Option<Bytes>,
}

impl SegmentLease {
    pub fn new(segment: CommitLogSegment, bytes: Option<Bytes>) -> Self {
        Self { segment, bytes }
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

    pub fn from_file_range(
        global_offset: i64,
        file_offset: u64,
        position_in_file: u64,
        len: usize,
        file: Arc<File>,
        cache_state: TransferCacheState,
    ) -> Self {
        Self {
            segment: CommitLogSegment {
                global_offset,
                file_offset,
                position_in_file,
                len,
                source: SegmentSource::FileRange {
                    file,
                    mapped_file: None,
                },
                cache_state,
            },
            bytes: None,
        }
    }

    pub fn from_select_result(global_offset: i64, mut result: SelectMappedBufferResult) -> Option<Self> {
        if result.size <= 0 {
            return None;
        }
        let len = result.size as usize;
        let bytes = result.bytes.take();
        let mapped_file = result.mapped_file.take();
        let position_in_file = result.file_offset;
        let file_offset = mapped_file
            .as_ref()
            .map(|mapped_file| mapped_file.get_file_from_offset())
            .unwrap_or_else(|| global_offset.saturating_sub(position_in_file as i64) as u64);
        let source = match mapped_file {
            Some(mapped_file) => match mapped_file.get_file().try_clone() {
                Ok(file) => SegmentSource::FileRange {
                    file: Arc::new(file),
                    mapped_file: Some(mapped_file),
                },
                Err(_) => SegmentSource::Mmap { mapped_file },
            },
            None => SegmentSource::Bytes,
        };

        Some(Self {
            segment: CommitLogSegment {
                global_offset,
                file_offset,
                position_in_file,
                len,
                source,
                cache_state: result.cache_state.into(),
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

impl Drop for SegmentLease {
    fn drop(&mut self) {
        match &self.segment.source {
            SegmentSource::Mmap { mapped_file }
            | SegmentSource::FileRange {
                mapped_file: Some(mapped_file),
                ..
            } => mapped_file.release(),
            SegmentSource::FileRange { mapped_file: None, .. } | SegmentSource::Bytes => {}
        }
    }
}
