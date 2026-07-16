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

//! Backend-neutral IndexFile put and query drivers.

use crate::index::codec::hash_slot_position;
use crate::index::codec::index_entry_position;
use crate::index::codec::IndexEntry;
use crate::index::codec::IndexSlot;
use crate::index::codec::INVALID_INDEX;

/// Immutable IndexFile state needed by the Local drivers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexFileSnapshot {
    pub hash_slot_num: usize,
    pub index_num: usize,
    pub index_count: i32,
    pub begin_timestamp: i64,
}

impl IndexFileSnapshot {
    pub const fn new(hash_slot_num: usize, index_num: usize, index_count: i32, begin_timestamp: i64) -> Self {
        Self {
            hash_slot_num,
            index_num,
            index_count,
            begin_timestamp,
        }
    }
}

/// Ordered header mutation emitted after a successful index write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexHeaderUpdate {
    SetBeginPhyOffset(i64),
    SetBeginTimestamp(i64),
    IncrementHashSlotCount,
    IncrementIndexCount,
    SetEndPhyOffset(i64),
    SetEndTimestamp(i64),
}

/// Result of one put-driver invocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexPutOutcome {
    Written,
    Full,
    SlotUnavailable,
    LayoutOverflow,
}

/// Terminal condition of one query-driver invocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexQueryOutcome {
    Completed,
    SlotUnavailable,
    EntryUnavailable,
    LayoutOverflow,
}

/// Converts the shared Java string hash result into the non-negative IndexFile hash domain.
pub const fn normalize_index_key_hash(hash_code: i32) -> i32 {
    if hash_code == i32::MIN {
        0
    } else {
        hash_code.abs()
    }
}

/// Applies the legacy inclusive overlap predicate for an IndexFile time range.
pub const fn is_index_time_matched(file_begin: i64, file_end: i64, query_begin: i64, query_end: i64) -> bool {
    query_begin < file_begin && query_end > file_end
        || query_begin >= file_begin && query_begin <= file_end
        || query_end >= file_begin && query_end <= file_end
}

/// Owns capacity gating, chain-head normalization, entry encoding, and header update order.
pub fn drive_index_put<ReadSlot, WriteBytes, ApplyHeader>(
    snapshot: IndexFileSnapshot,
    key_hash: i32,
    physical_offset: i64,
    store_timestamp: i64,
    mut read_slot: ReadSlot,
    mut write_bytes: WriteBytes,
    mut apply_header: ApplyHeader,
) -> IndexPutOutcome
where
    ReadSlot: FnMut(usize) -> Option<[u8; 4]>,
    WriteBytes: FnMut(usize, &[u8]),
    ApplyHeader: FnMut(IndexHeaderUpdate),
{
    if snapshot.index_count >= snapshot.index_num as i32 {
        return IndexPutOutcome::Full;
    }
    if snapshot.hash_slot_num == 0 || snapshot.index_count < 0 {
        return IndexPutOutcome::LayoutOverflow;
    }

    let slot_index = key_hash as usize % snapshot.hash_slot_num;
    let Some(slot_position) = hash_slot_position(slot_index) else {
        return IndexPutOutcome::LayoutOverflow;
    };
    let Some(slot_bytes) = read_slot(slot_position) else {
        return IndexPutOutcome::SlotUnavailable;
    };
    let Some(IndexSlot(mut previous_index)) = IndexSlot::decode(&slot_bytes) else {
        return IndexPutOutcome::SlotUnavailable;
    };
    if previous_index <= INVALID_INDEX || previous_index > snapshot.index_count {
        previous_index = INVALID_INDEX;
    }

    let time_diff = index_time_diff_seconds(snapshot.begin_timestamp, store_timestamp);
    let Some(entry_position) = index_entry_position(snapshot.hash_slot_num, snapshot.index_count as usize) else {
        return IndexPutOutcome::LayoutOverflow;
    };

    let entry = IndexEntry::new(key_hash, physical_offset, time_diff, previous_index).encode();
    write_bytes(entry_position, &entry);
    let slot = IndexSlot(snapshot.index_count).encode();
    write_bytes(slot_position, &slot);

    if snapshot.index_count <= 1 {
        apply_header(IndexHeaderUpdate::SetBeginPhyOffset(physical_offset));
        apply_header(IndexHeaderUpdate::SetBeginTimestamp(store_timestamp));
    }
    if previous_index == INVALID_INDEX {
        apply_header(IndexHeaderUpdate::IncrementHashSlotCount);
    }
    apply_header(IndexHeaderUpdate::IncrementIndexCount);
    apply_header(IndexHeaderUpdate::SetEndPhyOffset(physical_offset));
    apply_header(IndexHeaderUpdate::SetEndTimestamp(store_timestamp));
    IndexPutOutcome::Written
}

/// Owns slot lookup, collision-chain traversal, time filtering, and result limiting.
pub fn query_index_offsets<ReadSlot, ReadEntry>(
    snapshot: IndexFileSnapshot,
    key_hash: i32,
    max_num: usize,
    begin: i64,
    end: i64,
    physical_offsets: &mut Vec<i64>,
    mut read_slot: ReadSlot,
    mut read_entry: ReadEntry,
) -> IndexQueryOutcome
where
    ReadSlot: FnMut(usize) -> Option<[u8; 4]>,
    ReadEntry: FnMut(usize) -> Option<[u8; 20]>,
{
    if snapshot.hash_slot_num == 0 {
        return IndexQueryOutcome::LayoutOverflow;
    }

    let slot_index = key_hash as usize % snapshot.hash_slot_num;
    let Some(slot_position) = hash_slot_position(slot_index) else {
        return IndexQueryOutcome::LayoutOverflow;
    };
    let Some(slot_bytes) = read_slot(slot_position) else {
        return IndexQueryOutcome::SlotUnavailable;
    };
    let Some(IndexSlot(slot_value)) = IndexSlot::decode(&slot_bytes) else {
        return IndexQueryOutcome::SlotUnavailable;
    };
    if slot_value <= INVALID_INDEX || slot_value > snapshot.index_count || snapshot.index_count <= 1 {
        return IndexQueryOutcome::Completed;
    }

    let mut next_index_to_read = slot_value;
    while physical_offsets.len() < max_num {
        let Some(entry_position) = index_entry_position(snapshot.hash_slot_num, next_index_to_read as usize) else {
            return IndexQueryOutcome::LayoutOverflow;
        };
        let Some(entry_bytes) = read_entry(entry_position) else {
            return IndexQueryOutcome::EntryUnavailable;
        };
        let Some(entry) = IndexEntry::decode(&entry_bytes) else {
            return IndexQueryOutcome::EntryUnavailable;
        };
        if entry.time_diff < 0 {
            break;
        }

        let time_read = snapshot.begin_timestamp + entry.time_diff as i64 * 1000;
        if key_hash == entry.key_hash && (time_read >= begin && time_read <= end) {
            physical_offsets.push(entry.physical_offset);
        }
        if entry.previous_index <= INVALID_INDEX
            || entry.previous_index > snapshot.index_count
            || entry.previous_index == next_index_to_read
            || time_read < begin
        {
            break;
        }
        next_index_to_read = entry.previous_index;
    }
    IndexQueryOutcome::Completed
}

fn index_time_diff_seconds(begin_timestamp: i64, store_timestamp: i64) -> i32 {
    if begin_timestamp <= 0 {
        return 0;
    }
    ((store_timestamp - begin_timestamp) / 1000).clamp(0, i32::MAX as i64) as i32
}
