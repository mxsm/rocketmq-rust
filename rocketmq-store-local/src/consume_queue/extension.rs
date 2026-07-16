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

//! Runtime-neutral ConsumeQueue extension record and layout kernels.

use std::fmt;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;

/// Minimum extension unit size: size, tags, store time, and bitmap size.
pub const MIN_EXT_UNIT_SIZE: i16 = 2 + 8 * 2 + 2;
/// Maximum extension unit size supported by the persisted signed length.
pub const MAX_EXT_UNIT_SIZE: i16 = i16::MAX;
/// Bytes reserved at the end of an extension mapped file.
pub const CQ_EXT_END_BLANK_DATA_LENGTH: i32 = 4;
/// Greatest decorated extension address retained for Java compatibility.
pub const MAX_CQ_EXT_ADDRESS: i64 = i32::MIN as i64 - 1;
/// Greatest undecorated extension offset.
pub const MAX_CQ_EXT_REAL_OFFSET: i64 = MAX_CQ_EXT_ADDRESS - i64::MIN;

/// Persisted ConsumeQueue extension unit.
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct CqExtUnit {
    size: i16,
    tags_code: i64,
    msg_store_time: i64,
    bit_map_size: i16,
    filter_bit_map: Option<Vec<u8>>,
}

impl CqExtUnit {
    /// Creates an extension unit.
    pub fn new(tags_code: i64, msg_store_time: i64, filter_bit_map: Option<Vec<u8>>) -> Self {
        let bit_map_size = filter_bit_map.as_ref().map_or(0, |value| value.len() as i16);
        let size = MIN_EXT_UNIT_SIZE + bit_map_size;
        Self {
            size,
            tags_code,
            msg_store_time,
            bit_map_size,
            filter_bit_map,
        }
    }

    /// Reads a unit from the current buffer position.
    pub fn read(&mut self, buffer: &mut Bytes) -> bool {
        if buffer.remaining() < 2 {
            return false;
        }
        self.size = buffer.get_i16();
        if self.size < 1 || buffer.remaining() < (self.size - 2) as usize {
            return false;
        }
        self.tags_code = buffer.get_i64();
        self.msg_store_time = buffer.get_i64();
        self.bit_map_size = buffer.get_i16();
        if self.bit_map_size < 1 {
            self.filter_bit_map = None;
            return true;
        }
        if buffer.remaining() < self.bit_map_size as usize {
            return false;
        }
        let mut bit_map = vec![0_u8; self.bit_map_size as usize];
        buffer.copy_to_slice(&mut bit_map);
        self.filter_bit_map = Some(bit_map);
        true
    }

    /// Reads the signed size and advances past a complete unit.
    pub fn read_by_skip(&mut self, buffer: &mut Bytes) {
        if buffer.remaining() < 2 {
            return;
        }
        self.size = (&buffer[..2]).get_i16();
        if self.size > 0 && buffer.remaining() >= self.size as usize {
            buffer.advance(self.size as usize);
        }
    }

    /// Serializes the unit using the persisted big-endian layout.
    pub fn write(&mut self) -> Vec<u8> {
        let mut buffer = BytesMut::with_capacity(self.calc_unit_size() as usize);
        self.write_to(&mut buffer);
        buffer.to_vec()
    }

    /// Serializes the unit into an existing buffer and returns bytes written.
    pub fn write_to(&mut self, buffer: &mut BytesMut) -> usize {
        self.bit_map_size = self.filter_bit_map.as_ref().map_or(0, |value| value.len() as i16);
        self.size = MIN_EXT_UNIT_SIZE + self.bit_map_size;
        buffer.put_i16(self.size);
        buffer.put_i64(self.tags_code);
        buffer.put_i64(self.msg_store_time);
        buffer.put_i16(self.bit_map_size);
        if let Some(bit_map) = &self.filter_bit_map {
            buffer.put_slice(bit_map);
        }
        self.size as usize
    }

    /// Calculates the encoded size from the current bitmap.
    pub fn calc_unit_size(&self) -> i32 {
        i32::from(MIN_EXT_UNIT_SIZE) + self.filter_bit_map.as_ref().map_or(0, |value| value.len() as i32)
    }

    #[inline]
    pub fn size(&self) -> i16 {
        self.size
    }

    #[inline]
    pub fn tags_code(&self) -> i64 {
        self.tags_code
    }

    #[inline]
    pub fn msg_store_time(&self) -> i64 {
        self.msg_store_time
    }

    #[inline]
    pub fn bit_map_size(&self) -> i16 {
        self.bit_map_size
    }

    #[inline]
    pub fn filter_bit_map(&self) -> Option<&[u8]> {
        if self.bit_map_size < 1 {
            None
        } else {
            self.filter_bit_map.as_deref()
        }
    }

    #[inline]
    pub fn set_tags_code(&mut self, tags_code: i64) {
        self.tags_code = tags_code;
    }

    #[inline]
    pub fn set_msg_store_time(&mut self, msg_store_time: i64) {
        self.msg_store_time = msg_store_time;
    }

    pub fn set_filter_bit_map(&mut self, filter_bit_map: Option<Vec<u8>>) {
        self.bit_map_size = filter_bit_map.as_ref().map_or(0, |value| value.len() as i16);
        self.filter_bit_map = filter_bit_map;
    }
}

impl fmt::Debug for CqExtUnit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CqExtUnit")
            .field("size", &self.size)
            .field("tags_code", &self.tags_code)
            .field("msg_store_time", &self.msg_store_time)
            .field("bit_map_size", &self.bit_map_size)
            .field("filter_bit_map", &self.filter_bit_map)
            .finish()
    }
}

impl fmt::Display for CqExtUnit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "CqExtUnit{{size={}, tagsCode={}, msgStoreTime={}, bitMapSize={}, filterBitMap={:?}}}",
            self.size, self.tags_code, self.msg_store_time, self.bit_map_size, self.filter_bit_map
        )
    }
}

/// Returns whether an address uses the decorated CQExt range.
pub fn is_cq_ext_address(address: i64) -> bool {
    address <= MAX_CQ_EXT_ADDRESS
}

/// Converts a real file offset to its persisted CQExt address.
pub fn decorate_cq_ext_offset(offset: i64) -> i64 {
    if is_cq_ext_address(offset) {
        offset
    } else {
        offset + i64::MIN
    }
}

/// Converts a persisted CQExt address to a real file offset.
pub fn undecorate_cq_ext_address(address: i64) -> i64 {
    if is_cq_ext_address(address) {
        address - i64::MIN
    } else {
        address
    }
}

/// Returns whether another extension unit fits in the compatible address range.
pub fn cq_ext_capacity_available(current_max_offset: i64, unit_size: i32) -> bool {
    current_max_offset + i64::from(unit_size) <= MAX_CQ_EXT_REAL_OFFSET
}

/// Placement selected for an extension append attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CqExtAppendPlan {
    FillToEnd { blank_size: i32 },
    Append,
}

/// Selects whether to append or roll after reserving the legacy four-byte end blank.
pub fn plan_cq_ext_append(mapped_file_size: i32, wrote_position: i32, unit_size: i32) -> CqExtAppendPlan {
    let blank_size = mapped_file_size - wrote_position - CQ_EXT_END_BLANK_DATA_LENGTH;
    if unit_size > blank_size {
        CqExtAppendPlan::FillToEnd { blank_size }
    } else {
        CqExtAppendPlan::Append
    }
}

/// Scans the complete CQExt prefix and returns its byte length.
pub fn scan_cq_ext_recovery(bytes: &[u8]) -> usize {
    let mut offset = 0;
    while offset + 2 <= bytes.len() {
        let size = i16::from_be_bytes([bytes[offset], bytes[offset + 1]]);
        if size <= 0 {
            break;
        }
        let Ok(size) = usize::try_from(size) else {
            break;
        };
        if offset + size > bytes.len() {
            break;
        }
        offset += size;
    }
    offset
}

/// Returns whether a mapped file lies completely before the minimum real offset.
pub fn cq_ext_file_before_min(file_from_offset: i64, mapped_file_size: i32, min_real_offset: i64) -> bool {
    file_from_offset + i64::from(mapped_file_size) < min_real_offset
}
