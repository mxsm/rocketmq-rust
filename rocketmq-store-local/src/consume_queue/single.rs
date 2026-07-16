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

//! Runtime-neutral SingleConsumeQueue scan and search kernels.

use crate::consume_queue::record::ConsumeQueueRecord;
use crate::consume_queue::record::CQ_STORE_UNIT_SIZE;

/// Boundary selected when several queue records share or surround one timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumeQueueTimeBoundary {
    /// Return the first queue offset whose timestamp is not less than the target.
    Lower,
    /// Return the last queue offset whose timestamp is not greater than the target.
    Upper,
}

/// Result of scanning one mapped file during SingleConsumeQueue recovery.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConsumeQueueRecoveryScan {
    /// Number of leading bytes occupied by written records.
    pub valid_bytes: i32,
    /// Exclusive CommitLog offset of the last written record.
    pub max_physical_offset: Option<i64>,
    /// Last extension address observed in the valid prefix.
    pub max_extension_address: Option<i64>,
    /// First decoded record that stopped recovery because it was not written.
    pub stopped_record: Option<ConsumeQueueRecord>,
}

/// First valid record selected while correcting the minimum logical offset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsumeQueueMinOffsetMatch {
    pub relative_offset: i32,
    pub record: ConsumeQueueRecord,
}

/// One-file action selected while truncating ConsumeQueue records by CommitLog offset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumeQueueTruncatePlan {
    /// Retry the same mapped file when a fixed-width record cannot be read.
    RetryFile,
    /// The first record starts at or beyond the truncation boundary.
    DeleteFile,
    /// Retain the leading bytes and stop truncation at this file.
    Retain {
        valid_bytes: i32,
        max_physical_offset: Option<i64>,
        max_extension_address: Option<i64>,
    },
}

impl ConsumeQueueRecoveryScan {
    /// Returns whether the valid prefix occupies the complete mapped file.
    pub fn fills_file(self, mapped_file_size: i32) -> bool {
        self.valid_bytes == mapped_file_size
    }
}

/// Scans the written prefix of one SingleConsumeQueue mapped file.
///
/// The Store adapter supplies mapped-file reads and the existing CQExt address predicate. A
/// missing or short record stops the scan without synthesizing progress.
pub fn scan_recovery_records<ReadRecord, IsExtensionAddress>(
    mapped_file_size: i32,
    mut read_record: ReadRecord,
    mut is_extension_address: IsExtensionAddress,
) -> ConsumeQueueRecoveryScan
where
    ReadRecord: FnMut(i32) -> Option<ConsumeQueueRecord>,
    IsExtensionAddress: FnMut(i64) -> bool,
{
    let mut scan = ConsumeQueueRecoveryScan::default();
    for index in 0..(mapped_file_size / CQ_STORE_UNIT_SIZE) {
        let Some(record) = read_record(index * CQ_STORE_UNIT_SIZE) else {
            break;
        };
        if !record.is_written() {
            scan.stopped_record = Some(record);
            break;
        }
        scan.valid_bytes = index * CQ_STORE_UNIT_SIZE + CQ_STORE_UNIT_SIZE;
        scan.max_physical_offset = Some(record.physical_end_offset());
        if is_extension_address(record.tags_code) {
            scan.max_extension_address = Some(record.tags_code);
        }
    }
    scan
}

/// Finds the first written record at or beyond the minimum CommitLog offset.
pub fn find_min_offset_record<ReadRecord>(
    read_position: i32,
    min_commit_log_offset: i64,
    mut read_record: ReadRecord,
) -> Option<ConsumeQueueMinOffsetMatch>
where
    ReadRecord: FnMut(i32) -> Option<ConsumeQueueRecord>,
{
    let mut relative_offset = 0;
    while relative_offset + CQ_STORE_UNIT_SIZE <= read_position {
        let record = read_record(relative_offset)?;
        if record.physical_offset >= min_commit_log_offset && record.message_size > 0 {
            return Some(ConsumeQueueMinOffsetMatch {
                relative_offset,
                record,
            });
        }
        relative_offset += CQ_STORE_UNIT_SIZE;
    }
    None
}

/// Selects the retained prefix or file deletion for one truncation candidate.
pub fn plan_truncate_records<ReadRecord, IsExtensionAddress>(
    mapped_file_size: i32,
    truncate_physical_offset: i64,
    mut read_record: ReadRecord,
    mut is_extension_address: IsExtensionAddress,
) -> ConsumeQueueTruncatePlan
where
    ReadRecord: FnMut(i32) -> Option<ConsumeQueueRecord>,
    IsExtensionAddress: FnMut(i64) -> bool,
{
    let mut valid_bytes = 0;
    let mut max_physical_offset = None;
    let mut max_extension_address = None;

    for index in 0..(mapped_file_size / CQ_STORE_UNIT_SIZE) {
        let Some(record) = read_record(index * CQ_STORE_UNIT_SIZE) else {
            return ConsumeQueueTruncatePlan::RetryFile;
        };
        if index == 0 {
            if record.physical_offset >= truncate_physical_offset {
                return ConsumeQueueTruncatePlan::DeleteFile;
            }
        } else {
            if !record.is_written() || record.physical_offset >= truncate_physical_offset {
                break;
            }
        }

        valid_bytes = index * CQ_STORE_UNIT_SIZE + CQ_STORE_UNIT_SIZE;
        max_physical_offset = Some(record.physical_end_offset());
        if is_extension_address(record.tags_code) {
            max_extension_address = Some(record.tags_code);
        }
    }

    ConsumeQueueTruncatePlan::Retain {
        valid_bytes,
        max_physical_offset,
        max_extension_address,
    }
}

fn middle_queue_offset(low: i32, high: i32) -> i32 {
    let low_unit = low / CQ_STORE_UNIT_SIZE;
    let high_unit = high / CQ_STORE_UNIT_SIZE;
    ((low_unit + high_unit) / 2) * CQ_STORE_UNIT_SIZE
}

fn read_queue_time_entry(buffer: &[u8], relative_offset: i32) -> Option<ConsumeQueueRecord> {
    ConsumeQueueRecord::decode_at(buffer, usize::try_from(relative_offset).ok()?)
}

/// Searches one mapped SingleConsumeQueue file by CommitLog store timestamp.
///
/// `pickup_store_time` is the Store-owned CommitLog lookup port. The failure callback preserves
/// the legacy warning point without moving logging or Store types into Local.
#[allow(
    clippy::too_many_arguments,
    reason = "The frozen search boundary is clearer as explicit scalar inputs until the SingleConsumeQueue root moves"
)]
pub fn search_queue_offset_by_time<Lookup, LookupFailure>(
    buffer: &[u8],
    mapped_file_from_offset: i64,
    min_logic_offset: i64,
    min_physical_offset: i64,
    timestamp: i64,
    boundary: ConsumeQueueTimeBoundary,
    mut pickup_store_time: Lookup,
    mut on_lookup_failure: LookupFailure,
) -> i64
where
    Lookup: FnMut(i64, i32) -> i64,
    LookupFailure: FnMut(i64),
{
    let queue_unit_count = buffer.len() as i32 / CQ_STORE_UNIT_SIZE;
    if queue_unit_count <= 0 {
        return 0;
    }

    let ceiling = (queue_unit_count - 1) * CQ_STORE_UNIT_SIZE;
    let mut floor = if min_logic_offset > mapped_file_from_offset {
        (min_logic_offset - mapped_file_from_offset) as i32
    } else {
        0
    };
    let floor_remainder = floor % CQ_STORE_UNIT_SIZE;
    if floor_remainder != 0 {
        floor += CQ_STORE_UNIT_SIZE - floor_remainder;
    }
    if floor > ceiling {
        return 0;
    }

    let mut timestamp_cache = Vec::with_capacity(16);
    let mut cached_store_time = |relative_offset: i32, physical_offset: i64, message_size: i32| {
        if let Some((_, store_time)) = timestamp_cache.iter().find(|(offset, _)| *offset == relative_offset) {
            return *store_time;
        }
        let store_time = pickup_store_time(physical_offset, message_size);
        timestamp_cache.push((relative_offset, store_time));
        store_time
    };

    let mut low = floor;
    let mut high = ceiling;
    let mut target_offset = -1_i32;
    let mut left_offset = -1_i32;
    let mut right_offset = -1_i32;

    if let Some(record) = read_queue_time_entry(buffer, ceiling) {
        if record.physical_offset >= min_physical_offset && record.message_size > 0 {
            let store_time = cached_store_time(ceiling, record.physical_offset, record.message_size);
            if store_time >= 0 && store_time < timestamp {
                return match boundary {
                    ConsumeQueueTimeBoundary::Lower => {
                        (mapped_file_from_offset + ceiling as i64 + CQ_STORE_UNIT_SIZE as i64)
                            / CQ_STORE_UNIT_SIZE as i64
                    }
                    ConsumeQueueTimeBoundary::Upper => {
                        (mapped_file_from_offset + ceiling as i64) / CQ_STORE_UNIT_SIZE as i64
                    }
                };
            }
        }
    }

    if let Some(record) = read_queue_time_entry(buffer, floor) {
        if record.physical_offset >= min_physical_offset && record.message_size > 0 {
            let store_time = cached_store_time(floor, record.physical_offset, record.message_size);
            if store_time >= 0 && store_time > timestamp {
                return match boundary {
                    ConsumeQueueTimeBoundary::Lower => mapped_file_from_offset / CQ_STORE_UNIT_SIZE as i64,
                    ConsumeQueueTimeBoundary::Upper => 0,
                };
            }
        }
    }

    while high >= low {
        let mid_offset = middle_queue_offset(low, high);
        let Some(record) = read_queue_time_entry(buffer, mid_offset) else {
            break;
        };

        if record.physical_offset < min_physical_offset {
            low = mid_offset + CQ_STORE_UNIT_SIZE;
            left_offset = mid_offset;
            continue;
        }

        let logic_offset = mapped_file_from_offset + mid_offset as i64;
        if record.message_size <= 0 || logic_offset < min_logic_offset {
            return 0;
        }

        let store_time = cached_store_time(mid_offset, record.physical_offset, record.message_size);
        if store_time < 0 {
            on_lookup_failure(record.physical_offset);
            return 0;
        }

        match store_time.cmp(&timestamp) {
            std::cmp::Ordering::Equal => {
                target_offset = mid_offset;
                break;
            }
            std::cmp::Ordering::Less => {
                low = mid_offset + CQ_STORE_UNIT_SIZE;
                left_offset = mid_offset;
            }
            std::cmp::Ordering::Greater => {
                high = mid_offset - CQ_STORE_UNIT_SIZE;
                right_offset = mid_offset;
            }
        }
    }

    let offset = if target_offset != -1 {
        match boundary {
            ConsumeQueueTimeBoundary::Lower => {
                let mut duplicate_low = floor;
                let mut duplicate_high = target_offset;
                let mut first_match = target_offset;
                while duplicate_high >= duplicate_low {
                    let attempt = middle_queue_offset(duplicate_low, duplicate_high);
                    let Some(record) = read_queue_time_entry(buffer, attempt) else {
                        break;
                    };
                    if record.physical_offset < min_physical_offset {
                        duplicate_low = attempt + CQ_STORE_UNIT_SIZE;
                        continue;
                    }
                    let logic_offset = mapped_file_from_offset + attempt as i64;
                    if record.message_size <= 0 || logic_offset < min_logic_offset {
                        duplicate_low = attempt + CQ_STORE_UNIT_SIZE;
                        continue;
                    }
                    let store_time = cached_store_time(attempt, record.physical_offset, record.message_size);
                    if store_time < 0 {
                        break;
                    }
                    if store_time < timestamp {
                        duplicate_low = attempt + CQ_STORE_UNIT_SIZE;
                    } else {
                        first_match = attempt;
                        duplicate_high = attempt - CQ_STORE_UNIT_SIZE;
                    }
                }
                first_match as i64
            }
            ConsumeQueueTimeBoundary::Upper => {
                let mut duplicate_low = target_offset;
                let mut duplicate_high = ceiling;
                let mut last_match = target_offset;
                while duplicate_high >= duplicate_low {
                    let attempt = middle_queue_offset(duplicate_low, duplicate_high);
                    let Some(record) = read_queue_time_entry(buffer, attempt) else {
                        break;
                    };
                    if record.physical_offset < min_physical_offset {
                        duplicate_low = attempt + CQ_STORE_UNIT_SIZE;
                        continue;
                    }
                    let logic_offset = mapped_file_from_offset + attempt as i64;
                    if record.message_size <= 0 || logic_offset < min_logic_offset {
                        duplicate_high = attempt - CQ_STORE_UNIT_SIZE;
                        continue;
                    }
                    let store_time = cached_store_time(attempt, record.physical_offset, record.message_size);
                    if store_time < 0 {
                        break;
                    }
                    if store_time <= timestamp {
                        last_match = attempt;
                        duplicate_low = attempt + CQ_STORE_UNIT_SIZE;
                    } else {
                        duplicate_high = attempt - CQ_STORE_UNIT_SIZE;
                    }
                }
                last_match as i64
            }
        }
    } else {
        match boundary {
            ConsumeQueueTimeBoundary::Lower => {
                if right_offset != -1 {
                    right_offset as i64
                } else {
                    return 0;
                }
            }
            ConsumeQueueTimeBoundary::Upper => {
                if left_offset != -1 {
                    left_offset as i64
                } else {
                    return 0;
                }
            }
        }
    };

    (mapped_file_from_offset + offset) / CQ_STORE_UNIT_SIZE as i64
}
