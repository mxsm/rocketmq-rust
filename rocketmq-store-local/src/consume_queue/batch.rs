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

//! Runtime-neutral BatchConsumeQueue record and scan kernels.

use bytes::Buf;
use bytes::BufMut;
use bytes::BytesMut;

use crate::consume_queue::single::ConsumeQueueTimeBoundary;

/// Fixed persisted size of one BatchConsumeQueue record.
pub const BATCH_CQ_STORE_UNIT_SIZE: i32 = 46;
/// Sentinel used when a batch record has no compacted offset.
pub const INVALID_COMPACTED_OFFSET: i32 = -1;

/// Canonical persisted 46-byte BatchConsumeQueue record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchConsumeQueueRecord {
    pub physical_offset: i64,
    pub message_size: i32,
    pub tags_code: i64,
    pub store_time: i64,
    pub message_base_offset: i64,
    pub batch_size: i16,
    pub compacted_offset: i32,
    pub reserved: i32,
}

impl BatchConsumeQueueRecord {
    #[allow(
        clippy::too_many_arguments,
        reason = "the constructor mirrors the frozen seven-field persisted BatchCQ format"
    )]
    pub fn new(
        physical_offset: i64,
        message_size: i32,
        tags_code: i64,
        store_time: i64,
        message_base_offset: i64,
        batch_size: i16,
        compacted_offset: i32,
    ) -> Self {
        Self {
            physical_offset,
            message_size,
            tags_code,
            store_time,
            message_base_offset,
            batch_size,
            compacted_offset,
            reserved: 0,
        }
    }

    /// Encodes the record using the Java-compatible big-endian layout.
    pub fn encode(self) -> [u8; BATCH_CQ_STORE_UNIT_SIZE as usize] {
        let mut buffer = BytesMut::with_capacity(BATCH_CQ_STORE_UNIT_SIZE as usize);
        buffer.put_i64(self.physical_offset);
        buffer.put_i32(self.message_size);
        buffer.put_i64(self.tags_code);
        buffer.put_i64(self.store_time);
        buffer.put_i64(self.message_base_offset);
        buffer.put_i16(self.batch_size);
        buffer.put_i32(self.compacted_offset);
        buffer.put_i32(self.reserved);
        let mut encoded = [0_u8; BATCH_CQ_STORE_UNIT_SIZE as usize];
        encoded.copy_from_slice(&buffer);
        encoded
    }

    /// Decodes one record from the leading 46 bytes.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut bytes = bytes.get(..BATCH_CQ_STORE_UNIT_SIZE as usize)?;
        Some(Self {
            physical_offset: bytes.get_i64(),
            message_size: bytes.get_i32(),
            tags_code: bytes.get_i64(),
            store_time: bytes.get_i64(),
            message_base_offset: bytes.get_i64(),
            batch_size: bytes.get_i16(),
            compacted_offset: bytes.get_i32(),
            reserved: bytes.get_i32(),
        })
    }

    /// Returns whether the record is a readable BatchCQ entry.
    pub fn is_valid(self) -> bool {
        self.physical_offset >= 0 && self.message_size > 0 && self.message_base_offset >= 0 && self.batch_size > 0
    }

    /// Returns the exclusive CommitLog end offset.
    pub fn physical_end_offset(self) -> i64 {
        self.physical_offset + i64::from(self.message_size)
    }

    /// Returns the exclusive logical message offset of the batch.
    pub fn queue_end_offset(self) -> i64 {
        self.message_base_offset + i64::from(self.batch_size)
    }
}

/// Location of one BatchCQ record in a mapped-file snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchRecordPosition {
    pub file_index: usize,
    pub relative_offset: i32,
}

/// Recovery progress for one BatchCQ mapped file.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BatchRecoveryScan {
    pub valid_bytes: i32,
    pub max_physical_offset: Option<i64>,
}

/// Minimum logical offsets selected after CommitLog expiration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchMinimumOffsets {
    pub queue_offset: i64,
    pub logic_offset: i64,
}

/// One-file BatchCQ truncation decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchTruncatePlan {
    RetryFile,
    DeleteFile,
    Retain {
        valid_bytes: i32,
        max_physical_offset: Option<i64>,
    },
}

/// Finds the first valid record in one mapped file.
pub fn find_first_batch_record<ReadRecord>(
    read_position: i32,
    mut read_record: ReadRecord,
) -> Option<(i32, BatchConsumeQueueRecord)>
where
    ReadRecord: FnMut(i32) -> Option<BatchConsumeQueueRecord>,
{
    let mut position = 0;
    while position + BATCH_CQ_STORE_UNIT_SIZE <= read_position {
        if let Some(record) = read_record(position) {
            return Some((position, record));
        }
        position += BATCH_CQ_STORE_UNIT_SIZE;
    }
    None
}

/// Finds the last valid record in one mapped file.
pub fn find_last_batch_record<ReadRecord>(
    read_position: i32,
    mut read_record: ReadRecord,
) -> Option<(i32, BatchConsumeQueueRecord)>
where
    ReadRecord: FnMut(i32) -> Option<BatchConsumeQueueRecord>,
{
    let mut position = read_position - BATCH_CQ_STORE_UNIT_SIZE;
    while position >= 0 {
        if let Some(record) = read_record(position) {
            return Some((position, record));
        }
        if position < BATCH_CQ_STORE_UNIT_SIZE {
            break;
        }
        position -= BATCH_CQ_STORE_UNIT_SIZE;
    }
    None
}

/// Finds the last BatchCQ record whose base offset is not greater than the requested offset.
pub fn find_batch_record_in_files<ReadPosition, ReadRecord>(
    file_count: usize,
    queue_offset: i64,
    mut read_position: ReadPosition,
    mut read_record: ReadRecord,
) -> Option<(BatchRecordPosition, BatchConsumeQueueRecord)>
where
    ReadPosition: FnMut(usize) -> i32,
    ReadRecord: FnMut(usize, i32) -> Option<BatchConsumeQueueRecord>,
{
    let mut candidate = None;
    for file_index in 0..file_count {
        let mut position = 0;
        while position + BATCH_CQ_STORE_UNIT_SIZE <= read_position(file_index) {
            let Some(record) = read_record(file_index, position) else {
                break;
            };
            if record.message_base_offset <= queue_offset {
                candidate = Some((
                    BatchRecordPosition {
                        file_index,
                        relative_offset: position,
                    },
                    record,
                ));
                position += BATCH_CQ_STORE_UNIT_SIZE;
                continue;
            }
            return candidate;
        }
    }
    candidate
}

/// Scans the valid BatchCQ prefix of one mapped file during recovery.
pub fn scan_batch_recovery<ReadRecord>(read_position: i32, mut read_record: ReadRecord) -> BatchRecoveryScan
where
    ReadRecord: FnMut(i32) -> Option<BatchConsumeQueueRecord>,
{
    let mut scan = BatchRecoveryScan::default();
    while scan.valid_bytes + BATCH_CQ_STORE_UNIT_SIZE <= read_position {
        let Some(record) = read_record(scan.valid_bytes) else {
            break;
        };
        scan.valid_bytes += BATCH_CQ_STORE_UNIT_SIZE;
        scan.max_physical_offset = Some(record.physical_end_offset());
    }
    scan
}

/// Chooses the retained prefix of one BatchCQ truncation candidate.
pub fn plan_batch_truncate<ReadRecord>(
    mapped_file_size: i32,
    max_commit_log_offset: i64,
    mut read_record: ReadRecord,
) -> BatchTruncatePlan
where
    ReadRecord: FnMut(i32) -> Option<BatchConsumeQueueRecord>,
{
    let mut position = 0;
    let mut max_physical_offset = None;
    while position + BATCH_CQ_STORE_UNIT_SIZE <= mapped_file_size {
        let Some(record) = read_record(position) else {
            return BatchTruncatePlan::Retain {
                valid_bytes: position,
                max_physical_offset,
            };
        };
        if position == 0 && record.physical_offset >= max_commit_log_offset {
            return BatchTruncatePlan::DeleteFile;
        }
        if record.physical_offset >= max_commit_log_offset {
            return BatchTruncatePlan::Retain {
                valid_bytes: position,
                max_physical_offset,
            };
        }
        position += BATCH_CQ_STORE_UNIT_SIZE;
        max_physical_offset = Some(record.physical_end_offset());
        if position == mapped_file_size {
            return BatchTruncatePlan::Retain {
                valid_bytes: position,
                max_physical_offset,
            };
        }
    }
    BatchTruncatePlan::RetryFile
}

/// Selects the next minimum logical and queue offsets after CommitLog expiration.
#[allow(
    clippy::too_many_arguments,
    reason = "callbacks keep mapped-file ownership out of the BatchCQ kernel"
)]
pub fn correct_batch_min_offsets<ReadPosition, FileOffset, ReadRecord>(
    file_count: usize,
    min_commit_log_offset: i64,
    current_queue_offset: i64,
    current_logic_offset: i64,
    mut read_position: ReadPosition,
    mut file_offset: FileOffset,
    mut read_record: ReadRecord,
) -> BatchMinimumOffsets
where
    ReadPosition: FnMut(usize) -> i32,
    FileOffset: FnMut(usize) -> i64,
    ReadRecord: FnMut(usize, i32) -> Option<BatchConsumeQueueRecord>,
{
    let mut selected = BatchMinimumOffsets {
        queue_offset: current_queue_offset,
        logic_offset: current_logic_offset,
    };
    for file_index in 0..file_count {
        let mut position = 0;
        while position + BATCH_CQ_STORE_UNIT_SIZE <= read_position(file_index) {
            let Some(record) = read_record(file_index, position) else {
                break;
            };
            if record.physical_offset < min_commit_log_offset {
                selected.queue_offset = record.queue_end_offset();
                selected.logic_offset =
                    file_offset(file_index) + i64::from(position) + i64::from(BATCH_CQ_STORE_UNIT_SIZE);
                position += BATCH_CQ_STORE_UNIT_SIZE;
                continue;
            }
            return BatchMinimumOffsets {
                queue_offset: record.message_base_offset,
                logic_offset: file_offset(file_index) + i64::from(position),
            };
        }
    }
    selected
}

/// Searches BatchCQ records by stored timestamp using the legacy boundary semantics.
pub fn search_batch_offset_by_time<ReadPosition, ReadRecord>(
    file_count: usize,
    timestamp: i64,
    boundary: ConsumeQueueTimeBoundary,
    mut read_position: ReadPosition,
    mut read_record: ReadRecord,
) -> i64
where
    ReadPosition: FnMut(usize) -> i32,
    ReadRecord: FnMut(usize, i32) -> Option<BatchConsumeQueueRecord>,
{
    let mut candidate = None;
    let mut last_seen = None;
    for file_index in 0..file_count {
        let mut position = 0;
        while position + BATCH_CQ_STORE_UNIT_SIZE <= read_position(file_index) {
            let Some(record) = read_record(file_index, position) else {
                break;
            };
            last_seen = Some(record.message_base_offset);
            if record.store_time < timestamp {
                position += BATCH_CQ_STORE_UNIT_SIZE;
                continue;
            }
            match boundary {
                ConsumeQueueTimeBoundary::Lower => return record.message_base_offset,
                ConsumeQueueTimeBoundary::Upper => {
                    candidate = Some(record.message_base_offset);
                    position += BATCH_CQ_STORE_UNIT_SIZE;
                }
            }
        }
        if let Some(offset) = candidate {
            return offset;
        }
    }
    candidate.or(last_seen).unwrap_or(-1)
}
