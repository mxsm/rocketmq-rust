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

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;

/// Minimum size of extension unit:
/// - 2 bytes: size (32k max)
/// - 8 bytes: tags_code
/// - 8 bytes: msg_store_time
/// - 2 bytes: bit_map_size
pub const MIN_EXT_UNIT_SIZE: i16 = 2 + 8 * 2 + 2;

/// Maximum size of extension unit
pub const MAX_EXT_UNIT_SIZE: i16 = i16::MAX;

#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct CqExtUnit {
    size: i16,
    tags_code: i64,
    msg_store_time: i64,
    bit_map_size: i16,
    filter_bit_map: Option<Vec<u8>>,
}

impl CqExtUnit {
    /// Create a new CqExtUnit with the specified values
    pub fn new(tags_code: i64, msg_store_time: i64, filter_bit_map: Option<Vec<u8>>) -> Self {
        let bit_map_size = filter_bit_map.as_ref().map_or(0, |v| v.len() as i16);
        let size = MIN_EXT_UNIT_SIZE + bit_map_size;
        Self {
            size,
            tags_code,
            msg_store_time,
            bit_map_size,
            filter_bit_map,
        }
    }

    /// Build unit from buffer from current position.
    ///
    /// # Returns
    /// `true` if read successfully, `false` otherwise
    pub fn read(&mut self, buffer: &mut Bytes) -> bool {
        if buffer.remaining() < 2 {
            return false;
        }

        self.size = buffer.get_i16();

        if self.size < 1 {
            return false;
        }

        if buffer.remaining() < (self.size - 2) as usize {
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

        let mut bit_map = vec![0u8; self.bit_map_size as usize];
        buffer.copy_to_slice(&mut bit_map);
        self.filter_bit_map = Some(bit_map);
        true
    }

    /// Only read first 2 bytes to get unit size, then skip.
    ///
    /// If size > 0, advances buffer position by size.
    /// If size <= 0, nothing is done.
    pub fn read_by_skip(&mut self, buffer: &mut Bytes) {
        if buffer.remaining() < 2 {
            return;
        }

        // Peek the size without consuming
        let temp_size = (&buffer[..2]).get_i16();
        self.size = temp_size;

        if temp_size > 0 && buffer.remaining() >= temp_size as usize {
            buffer.advance(temp_size as usize);
        }
    }

    /// Transform unit data to byte array.
    ///
    /// # Returns
    /// The serialized byte array
    pub fn write(&mut self) -> Vec<u8> {
        self.bit_map_size = self.filter_bit_map.as_ref().map_or(0, |v| v.len() as i16);
        self.size = MIN_EXT_UNIT_SIZE + self.bit_map_size;

        let mut buffer = BytesMut::with_capacity(self.size as usize);

        buffer.put_i16(self.size);
        buffer.put_i64(self.tags_code);
        buffer.put_i64(self.msg_store_time);
        buffer.put_i16(self.bit_map_size);

        if let Some(ref bit_map) = self.filter_bit_map {
            buffer.put_slice(bit_map);
        }

        buffer.to_vec()
    }

    /// Write unit data to the provided buffer.
    ///
    /// # Returns
    /// The number of bytes written
    pub fn write_to(&mut self, buffer: &mut BytesMut) -> usize {
        self.bit_map_size = self.filter_bit_map.as_ref().map_or(0, |v| v.len() as i16);
        self.size = MIN_EXT_UNIT_SIZE + self.bit_map_size;

        buffer.put_i16(self.size);
        buffer.put_i64(self.tags_code);
        buffer.put_i64(self.msg_store_time);
        buffer.put_i16(self.bit_map_size);

        if let Some(ref bit_map) = self.filter_bit_map {
            buffer.put_slice(bit_map);
        }

        self.size as usize
    }

    /// Calculate unit size by current data.
    pub fn calc_unit_size(&self) -> i32 {
        MIN_EXT_UNIT_SIZE as i32 + self.filter_bit_map.as_ref().map_or(0, |v| v.len() as i32)
    }

    // ========== Getters ==========

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

    /// Get the filter bit map.
    /// Returns None if bit_map_size < 1.
    #[inline]
    pub fn filter_bit_map(&self) -> Option<&[u8]> {
        if self.bit_map_size < 1 {
            None
        } else {
            self.filter_bit_map.as_deref()
        }
    }

    // ========== Setters ==========

    #[inline]
    pub fn set_tags_code(&mut self, tags_code: i64) {
        self.tags_code = tags_code;
    }

    #[inline]
    pub fn set_msg_store_time(&mut self, msg_store_time: i64) {
        self.msg_store_time = msg_store_time;
    }

    /// Set the filter bit map.
    /// Note: size will be recalculated by `write()` or `calc_unit_size()`.
    pub fn set_filter_bit_map(&mut self, filter_bit_map: Option<Vec<u8>>) {
        self.bit_map_size = filter_bit_map.as_ref().map_or(0, |v| v.len() as i16);
        self.filter_bit_map = filter_bit_map;
    }
}

impl fmt::Debug for CqExtUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CqExtUnit")
            .field("size", &self.size)
            .field("tags_code", &self.tags_code)
            .field("msg_store_time", &self.msg_store_time)
            .field("bit_map_size", &self.bit_map_size)
            .field("filter_bit_map", &self.filter_bit_map)
            .finish()
    }
}

impl fmt::Display for CqExtUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CqExtUnit{{size={}, tagsCode={}, msgStoreTime={}, bitMapSize={}, filterBitMap={:?}}}",
            self.size, self.tags_code, self.msg_store_time, self.bit_map_size, self.filter_bit_map
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_without_bitmap() {
        let unit = CqExtUnit::new(12345, 1000000, None);
        assert_eq!(unit.size(), MIN_EXT_UNIT_SIZE);
        assert_eq!(unit.tags_code(), 12345);
        assert_eq!(unit.msg_store_time(), 1000000);
        assert_eq!(unit.bit_map_size(), 0);
        assert!(unit.filter_bit_map().is_none());
    }

    #[test]
    fn test_new_with_bitmap() {
        let bitmap = vec![1, 2, 3, 4];
        let unit = CqExtUnit::new(12345, 1000000, Some(bitmap.clone()));
        assert_eq!(unit.size(), MIN_EXT_UNIT_SIZE + 4);
        assert_eq!(unit.tags_code(), 12345);
        assert_eq!(unit.msg_store_time(), 1000000);
        assert_eq!(unit.bit_map_size(), 4);
        assert_eq!(unit.filter_bit_map(), Some(bitmap.as_slice()));
    }

    #[test]
    fn test_write_and_read() {
        let bitmap = vec![0xAB, 0xCD, 0xEF];
        let mut original = CqExtUnit::new(9876543210, 1234567890123, Some(bitmap));

        let bytes = original.write();
        let mut buffer = Bytes::from(bytes);

        let mut restored = CqExtUnit::default();
        assert!(restored.read(&mut buffer));

        assert_eq!(original, restored);
    }

    #[test]
    fn test_write_and_read_without_bitmap() {
        let mut original = CqExtUnit::new(12345, 67890, None);

        let bytes = original.write();
        let mut buffer = Bytes::from(bytes);

        let mut restored = CqExtUnit::default();
        assert!(restored.read(&mut buffer));

        assert_eq!(original, restored);
    }

    #[test]
    fn test_calc_unit_size() {
        let unit_no_bitmap = CqExtUnit::new(0, 0, None);
        assert_eq!(unit_no_bitmap.calc_unit_size(), MIN_EXT_UNIT_SIZE as i32);

        let unit_with_bitmap = CqExtUnit::new(0, 0, Some(vec![1, 2, 3, 4, 5]));
        assert_eq!(unit_with_bitmap.calc_unit_size(), MIN_EXT_UNIT_SIZE as i32 + 5);
    }

    #[test]
    fn test_setters() {
        let mut unit = CqExtUnit::default();

        unit.set_tags_code(999);
        assert_eq!(unit.tags_code(), 999);

        unit.set_msg_store_time(888);
        assert_eq!(unit.msg_store_time(), 888);

        unit.set_filter_bit_map(Some(vec![1, 2]));
        assert_eq!(unit.bit_map_size(), 2);
        assert_eq!(unit.filter_bit_map(), Some([1u8, 2u8].as_slice()));
    }

    #[test]
    fn test_read_empty_buffer() {
        let mut buffer = Bytes::new();
        let mut unit = CqExtUnit::default();
        assert!(!unit.read(&mut buffer));
    }

    #[test]
    fn test_read_invalid_size() {
        let mut buffer = Bytes::from(vec![0u8, 0u8]); // size = 0
        let mut unit = CqExtUnit::default();
        assert!(!unit.read(&mut buffer));
    }
}
