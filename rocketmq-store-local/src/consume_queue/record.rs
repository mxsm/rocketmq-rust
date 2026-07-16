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

//! Canonical 20-byte ConsumeQueue record format.

/// Serialized size of one R0 ConsumeQueue record.
pub const CQ_STORE_UNIT_SIZE: i32 = 20;

/// Byte offset of the tag hash or ConsumeQueue extension address.
pub const MSG_TAG_OFFSET_INDEX: i32 = 12;

const PHYSICAL_OFFSET_END: usize = 8;
const MESSAGE_SIZE_END: usize = 12;
const TAGS_CODE_END: usize = CQ_STORE_UNIT_SIZE as usize;

/// One canonical ConsumeQueue record.
///
/// The persisted representation is Java-compatible big-endian
/// `physical_offset(i64) + message_size(i32) + tags_code(i64)`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConsumeQueueRecord {
    pub physical_offset: i64,
    pub message_size: i32,
    pub tags_code: i64,
}

impl ConsumeQueueRecord {
    /// Creates one record without changing signed legacy values.
    pub const fn new(physical_offset: i64, message_size: i32, tags_code: i64) -> Self {
        Self {
            physical_offset,
            message_size,
            tags_code,
        }
    }

    /// Returns the legacy pre-blank record used before the first logical offset.
    pub const fn pre_blank() -> Self {
        Self::new(0, i32::MAX, 0)
    }

    /// Encodes the exact 20-byte big-endian storage representation.
    pub fn encode(self) -> [u8; CQ_STORE_UNIT_SIZE as usize] {
        let mut encoded = [0_u8; CQ_STORE_UNIT_SIZE as usize];
        encoded[..PHYSICAL_OFFSET_END].copy_from_slice(&self.physical_offset.to_be_bytes());
        encoded[PHYSICAL_OFFSET_END..MESSAGE_SIZE_END].copy_from_slice(&self.message_size.to_be_bytes());
        encoded[MESSAGE_SIZE_END..TAGS_CODE_END].copy_from_slice(&self.tags_code.to_be_bytes());
        encoded
    }

    /// Decodes the first complete record and rejects a short input without consuming it.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        Some(Self {
            physical_offset: i64::from_be_bytes(bytes.get(..PHYSICAL_OFFSET_END)?.try_into().ok()?),
            message_size: i32::from_be_bytes(bytes.get(PHYSICAL_OFFSET_END..MESSAGE_SIZE_END)?.try_into().ok()?),
            tags_code: i64::from_be_bytes(bytes.get(MESSAGE_SIZE_END..TAGS_CODE_END)?.try_into().ok()?),
        })
    }

    /// Decodes one record at `relative_offset` with checked bounds.
    pub fn decode_at(bytes: &[u8], relative_offset: usize) -> Option<Self> {
        let end = relative_offset.checked_add(CQ_STORE_UNIT_SIZE as usize)?;
        Self::decode(bytes.get(relative_offset..end)?)
    }

    /// Returns whether recovery treats the record as a written queue entry.
    pub const fn is_written(self) -> bool {
        self.physical_offset >= 0 && self.message_size > 0
    }

    /// Returns the exclusive CommitLog offset represented by this record.
    pub const fn physical_end_offset(self) -> i64 {
        self.physical_offset + self.message_size as i64
    }
}
