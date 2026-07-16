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

//! Java-compatible index-file records and checked layout calculations.

/// Serialized size of one index-file header.
pub const INDEX_HEADER_SIZE: usize = 40;
/// Byte offset of the header's begin timestamp.
pub const BEGIN_TIMESTAMP_OFFSET: usize = 0;
/// Byte offset of the header's end timestamp.
pub const END_TIMESTAMP_OFFSET: usize = 8;
/// Byte offset of the header's begin physical offset.
pub const BEGIN_PHY_OFFSET: usize = 16;
/// Byte offset of the header's end physical offset.
pub const END_PHY_OFFSET: usize = 24;
/// Byte offset of the header's populated hash-slot count.
pub const HASH_SLOT_COUNT_OFFSET: usize = 32;
/// Byte offset of the header's next index-entry position.
pub const INDEX_COUNT_OFFSET: usize = 36;

/// Serialized size of one hash slot.
pub const INDEX_HASH_SLOT_SIZE: usize = 4;
/// Serialized size of one index entry.
pub const INDEX_ENTRY_SIZE: usize = 20;
/// Sentinel representing an empty slot or the end of an index chain.
pub const INVALID_INDEX: i32 = 0;

const END_TIMESTAMP_END: usize = 16;
const BEGIN_PHY_OFFSET_END: usize = 24;
const END_PHY_OFFSET_END: usize = 32;
const HASH_SLOT_COUNT_END: usize = 36;
const INDEX_COUNT_END: usize = INDEX_HEADER_SIZE;

const KEY_HASH_END: usize = 4;
const PHYSICAL_OFFSET_END: usize = 12;
const TIME_DIFF_END: usize = 16;
const PREVIOUS_INDEX_END: usize = INDEX_ENTRY_SIZE;

/// One canonical 40-byte index-file header.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IndexHeaderRecord {
    pub begin_timestamp: i64,
    pub end_timestamp: i64,
    pub begin_phy_offset: i64,
    pub end_phy_offset: i64,
    pub hash_slot_count: i32,
    pub index_count: i32,
}

impl IndexHeaderRecord {
    /// Encodes the exact Java-compatible big-endian header representation.
    pub fn encode(self) -> [u8; INDEX_HEADER_SIZE] {
        let mut encoded = [0_u8; INDEX_HEADER_SIZE];
        encoded[BEGIN_TIMESTAMP_OFFSET..END_TIMESTAMP_OFFSET].copy_from_slice(&self.begin_timestamp.to_be_bytes());
        encoded[END_TIMESTAMP_OFFSET..END_TIMESTAMP_END].copy_from_slice(&self.end_timestamp.to_be_bytes());
        encoded[BEGIN_PHY_OFFSET..BEGIN_PHY_OFFSET_END].copy_from_slice(&self.begin_phy_offset.to_be_bytes());
        encoded[END_PHY_OFFSET..END_PHY_OFFSET_END].copy_from_slice(&self.end_phy_offset.to_be_bytes());
        encoded[HASH_SLOT_COUNT_OFFSET..HASH_SLOT_COUNT_END].copy_from_slice(&self.hash_slot_count.to_be_bytes());
        encoded[INDEX_COUNT_OFFSET..INDEX_COUNT_END].copy_from_slice(&self.index_count.to_be_bytes());
        encoded
    }

    /// Decodes the first complete header and preserves the legacy minimum index count of one.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let index_count = i32::from_be_bytes(bytes.get(INDEX_COUNT_OFFSET..INDEX_COUNT_END)?.try_into().ok()?);
        Some(Self {
            begin_timestamp: i64::from_be_bytes(
                bytes
                    .get(BEGIN_TIMESTAMP_OFFSET..END_TIMESTAMP_OFFSET)?
                    .try_into()
                    .ok()?,
            ),
            end_timestamp: i64::from_be_bytes(bytes.get(END_TIMESTAMP_OFFSET..END_TIMESTAMP_END)?.try_into().ok()?),
            begin_phy_offset: i64::from_be_bytes(bytes.get(BEGIN_PHY_OFFSET..BEGIN_PHY_OFFSET_END)?.try_into().ok()?),
            end_phy_offset: i64::from_be_bytes(bytes.get(END_PHY_OFFSET..END_PHY_OFFSET_END)?.try_into().ok()?),
            hash_slot_count: i32::from_be_bytes(
                bytes
                    .get(HASH_SLOT_COUNT_OFFSET..HASH_SLOT_COUNT_END)?
                    .try_into()
                    .ok()?,
            ),
            index_count: index_count.max(1),
        })
    }
}

/// One canonical 4-byte index hash slot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IndexSlot(pub i32);

impl IndexSlot {
    /// Encodes the exact big-endian slot representation.
    pub const fn encode(self) -> [u8; INDEX_HASH_SLOT_SIZE] {
        self.0.to_be_bytes()
    }

    /// Decodes the first complete slot.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        Some(Self(i32::from_be_bytes(
            bytes.get(..INDEX_HASH_SLOT_SIZE)?.try_into().ok()?,
        )))
    }
}

/// One canonical 20-byte index entry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IndexEntry {
    pub key_hash: i32,
    pub physical_offset: i64,
    pub time_diff: i32,
    pub previous_index: i32,
}

impl IndexEntry {
    /// Creates one entry without changing signed legacy values.
    pub const fn new(key_hash: i32, physical_offset: i64, time_diff: i32, previous_index: i32) -> Self {
        Self {
            key_hash,
            physical_offset,
            time_diff,
            previous_index,
        }
    }

    /// Encodes the exact Java-compatible big-endian entry representation.
    pub fn encode(self) -> [u8; INDEX_ENTRY_SIZE] {
        let mut encoded = [0_u8; INDEX_ENTRY_SIZE];
        encoded[..KEY_HASH_END].copy_from_slice(&self.key_hash.to_be_bytes());
        encoded[KEY_HASH_END..PHYSICAL_OFFSET_END].copy_from_slice(&self.physical_offset.to_be_bytes());
        encoded[PHYSICAL_OFFSET_END..TIME_DIFF_END].copy_from_slice(&self.time_diff.to_be_bytes());
        encoded[TIME_DIFF_END..PREVIOUS_INDEX_END].copy_from_slice(&self.previous_index.to_be_bytes());
        encoded
    }

    /// Decodes the first complete entry.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        Some(Self {
            key_hash: i32::from_be_bytes(bytes.get(..KEY_HASH_END)?.try_into().ok()?),
            physical_offset: i64::from_be_bytes(bytes.get(KEY_HASH_END..PHYSICAL_OFFSET_END)?.try_into().ok()?),
            time_diff: i32::from_be_bytes(bytes.get(PHYSICAL_OFFSET_END..TIME_DIFF_END)?.try_into().ok()?),
            previous_index: i32::from_be_bytes(bytes.get(TIME_DIFF_END..PREVIOUS_INDEX_END)?.try_into().ok()?),
        })
    }
}

/// Cause of an invalid or overflowing index-file layout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexLayoutError {
    ZeroHashSlots,
    ZeroIndexEntries,
    HashSlotSectionOverflow,
    IndexEntrySectionOverflow,
    TotalSizeOverflow,
}

/// Calculates the complete file size using checked arithmetic.
pub fn index_file_total_size(hash_slot_num: usize, index_num: usize) -> Result<usize, IndexLayoutError> {
    if hash_slot_num == 0 {
        return Err(IndexLayoutError::ZeroHashSlots);
    }
    if index_num == 0 {
        return Err(IndexLayoutError::ZeroIndexEntries);
    }

    let hash_slots_size = hash_slot_num
        .checked_mul(INDEX_HASH_SLOT_SIZE)
        .ok_or(IndexLayoutError::HashSlotSectionOverflow)?;
    let indexes_size = index_num
        .checked_mul(INDEX_ENTRY_SIZE)
        .ok_or(IndexLayoutError::IndexEntrySectionOverflow)?;

    INDEX_HEADER_SIZE
        .checked_add(hash_slots_size)
        .and_then(|size| size.checked_add(indexes_size))
        .ok_or(IndexLayoutError::TotalSizeOverflow)
}

/// Calculates the absolute byte position of one hash slot.
pub fn hash_slot_position(slot_index: usize) -> Option<usize> {
    slot_index
        .checked_mul(INDEX_HASH_SLOT_SIZE)
        .and_then(|offset| INDEX_HEADER_SIZE.checked_add(offset))
}

/// Calculates the absolute byte position of one index entry.
pub fn index_entry_position(hash_slot_num: usize, index: usize) -> Option<usize> {
    let slots_end = hash_slot_num
        .checked_mul(INDEX_HASH_SLOT_SIZE)
        .and_then(|size| INDEX_HEADER_SIZE.checked_add(size))?;
    index
        .checked_mul(INDEX_ENTRY_SIZE)
        .and_then(|offset| slots_end.checked_add(offset))
}
