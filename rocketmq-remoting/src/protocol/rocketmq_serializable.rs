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

use std::collections::HashMap;
use std::str;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_error::RocketmqError;

use crate::protocol::remoting_command::RemotingCommand;
use crate::protocol::LanguageCode;

pub struct RocketMQSerializable;

impl RocketMQSerializable {
    /// Optimized string write with inline hint for better performance
    #[inline]
    pub fn write_str(buf: &mut BytesMut, use_short_length: bool, s: &str) -> usize {
        let bytes = s.as_bytes();
        let len = bytes.len();

        let length_size = if use_short_length {
            buf.put_u16(len as u16);
            2
        } else {
            buf.put_u32(len as u32);
            4
        };

        buf.put_slice(bytes); // Use put_slice for better performance
        length_size + len
    }

    /// Optimized string read with enhanced boundary checks and zero-copy
    #[inline]
    pub fn read_str(
        buf: &mut BytesMut,
        use_short_length: bool,
        limit: usize,
    ) -> rocketmq_error::RocketMQResult<Option<CheetahString>> {
        // Read length prefix
        let len = if use_short_length {
            if buf.remaining() < 2 {
                return Err(RocketmqError::DecodingError(2, buf.remaining()).into());
            }
            buf.get_u16() as usize
        } else {
            if buf.remaining() < 4 {
                return Err(RocketmqError::DecodingError(4, buf.remaining()).into());
            }
            buf.get_u32() as usize
        };

        // Empty string
        if len == 0 {
            return Ok(None);
        }

        // Boundary check
        if len > limit {
            return Err(RocketmqError::DecodingError(len, limit).into());
        }

        // Ensure buffer has enough data
        if buf.remaining() < len {
            return Err(RocketmqError::DecodingError(len, buf.remaining()).into());
        }

        // Zero-copy split and freeze
        let bytes = buf.split_to(len).freeze();
        Ok(Some(CheetahString::from_bytes(bytes)))
    }

    /// Optimized ROCKETMQ protocol encoding with reduced allocations
    #[inline]
    pub fn rocketmq_protocol_encode(cmd: &mut RemotingCommand, buf: &mut BytesMut) -> usize {
        let begin_index = buf.len();

        // Estimate required capacity and reserve upfront to reduce reallocations
        let estimated_size = Self::estimate_encode_size(cmd);
        buf.reserve(estimated_size);

        // Write fixed-size header fields (total: 15 bytes)
        buf.put_u16(cmd.code() as u16); // 2 bytes
        buf.put_u8(cmd.language().get_code()); // 1 byte
        buf.put_u16(cmd.version() as u16); // 2 bytes
        buf.put_i32(cmd.opaque()); // 4 bytes
        buf.put_i32(cmd.flag()); // 4 bytes

        // Write remark (variable length with 4-byte prefix or 0)
        if let Some(remark) = cmd.remark() {
            Self::write_str(buf, false, remark.as_str());
        } else {
            buf.put_i32(0);
        }

        // Reserve space for ext_fields length (will be updated later)
        let map_len_index = buf.len();
        buf.put_i32(0);

        // Encode custom header if it supports fast codec
        if let Some(header) = cmd.command_custom_header_mut() {
            if header.support_fast_codec() {
                header.encode_fast(buf);
            }
        }

        // Encode ext_fields map
        if let Some(ext_fields) = cmd.ext_fields() {
            for (k, v) in ext_fields.iter() {
                // Skip empty keys/values
                if !k.is_empty() && !v.is_empty() {
                    Self::write_str(buf, true, k.as_str());
                    Self::write_str(buf, true, v.as_str());
                }
            }
        }

        // Update ext_fields length in-place
        let current_length = buf.len();
        let ext_fields_length = (current_length - map_len_index - 4) as i32;
        buf[map_len_index..map_len_index + 4].copy_from_slice(&ext_fields_length.to_be_bytes());

        buf.len() - begin_index
    }

    /// Estimate the size needed for encoding to reduce reallocations
    #[inline]
    fn estimate_encode_size(cmd: &RemotingCommand) -> usize {
        let mut size = 15; // Fixed header: code(2) + language(1) + version(2) + opaque(4) + flag(4) + map_len(4)

        // Remark size
        if let Some(remark) = cmd.remark() {
            size += 4 + remark.len(); // length prefix + data
        } else {
            size += 4; // just the length prefix (0)
        }

        // Ext fields size (approximate)
        if let Some(ext) = cmd.ext_fields() {
            for (k, v) in ext.iter() {
                if !k.is_empty() && !v.is_empty() {
                    size += 2 + k.len() + 2 + v.len(); // short length prefix for both
                }
            }
        }

        size
    }

    pub fn rocket_mq_protocol_encode_bytes(cmd: &RemotingCommand) -> Bytes {
        let remark_bytes = cmd.remark().map(|remark| remark.as_bytes().to_vec());
        let remark_len = remark_bytes.as_ref().map_or(0, |v| v.len());

        let ext_fields_bytes = if let Some(ext) = cmd.get_ext_fields() {
            Self::map_serialize(ext)
        } else {
            None
        };
        let ext_len = ext_fields_bytes.as_ref().map_or(0, |v| v.len());

        let total_len = Self::cal_total_len(remark_len, ext_len);
        let mut header_buffer = BytesMut::with_capacity(total_len);

        // int code (~32767)
        header_buffer.put_i16(cmd.code() as i16);

        // LanguageCode language
        header_buffer.put_u8(cmd.language().get_code());

        // int version (~32767)
        header_buffer.put_i16(cmd.version() as i16);

        // int opaque
        header_buffer.put_i32(cmd.opaque());

        // int flag
        header_buffer.put_i32(cmd.flag());

        // String remark
        if let Some(remark_bytes) = remark_bytes {
            header_buffer.put_i32(remark_bytes.len() as i32);
            header_buffer.put(remark_bytes.as_ref());
        } else {
            header_buffer.put_i32(0);
        }

        // HashMap<String, String> extFields
        if let Some(ext_fields_bytes) = ext_fields_bytes {
            header_buffer.put_i32(ext_fields_bytes.len() as i32);
            header_buffer.put(ext_fields_bytes.as_ref());
        } else {
            header_buffer.put_i32(0);
        }

        header_buffer.freeze()
    }

    /// Optimized map serialization with pre-calculated capacity
    #[inline]
    pub fn map_serialize(map: &HashMap<CheetahString, CheetahString>) -> Option<BytesMut> {
        if map.is_empty() {
            return None;
        }

        // Pre-calculate total length in a single pass
        let mut total_length = 0;
        let mut valid_entries = 0;

        for (key, value) in map.iter() {
            if !key.is_empty() && !value.is_empty() {
                total_length += 2 + key.len() + 4 + value.len();
                valid_entries += 1;
            }
        }

        if valid_entries == 0 {
            return None;
        }

        // Allocate exact capacity (avoid reallocations)
        let mut content = BytesMut::with_capacity(total_length);

        // Serialize entries
        for (key, value) in map.iter() {
            if !key.is_empty() && !value.is_empty() {
                // Write key: u16 length + bytes
                content.put_u16(key.len() as u16);
                content.put_slice(key.as_bytes());

                // Write value: i32 length + bytes
                content.put_i32(value.len() as i32);
                content.put_slice(value.as_bytes());
            }
        }

        Some(content)
    }

    pub fn cal_total_len(remark_len: usize, ext_len: usize) -> usize {
        // int code(~32767): 2 bytes
        // LanguageCode language: 1 byte
        // int version(~32767): 2 bytes
        // int opaque: 4 bytes
        // int flag: 4 bytes
        // String remark length: 4 bytes + actual length of remark
        // HashMap<String, String> extFields length: 4 bytes + actual length of extFields

        2   // int code
             + 1          // LanguageCode language
             + 2          // int version
             + 4          // int opaque
             + 4          // int flag
             + 4 + remark_len   // String remark
             + 4 + ext_len // HashMap<String, String> extFields
    }

    pub fn rocket_mq_protocol_decode(
        header_buffer: &mut BytesMut,
        header_len: usize,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let cmd = RemotingCommand::default()
            .set_code(header_buffer.get_i16())
            .set_language(LanguageCode::value_of(header_buffer.get_u8()).unwrap())
            .set_version(header_buffer.get_i16() as i32)
            .set_opaque(header_buffer.get_i32())
            .set_flag(header_buffer.get_i32());

        let remark = Self::read_str(header_buffer, false, header_len)?;

        // HashMap<String, String> extFields
        let ext_fields_length = header_buffer.get_i32() as usize;
        let ext = if ext_fields_length > 0 {
            if ext_fields_length > header_len {
                return Err(RocketmqError::DecodingError(ext_fields_length, header_len).into());
            }
            Self::map_deserialize(header_buffer, ext_fields_length)?
        } else {
            HashMap::new()
        };

        Ok(cmd.set_remark_option(remark).set_ext_fields(ext))
    }

    /// Optimized map deserialization with capacity hint and better error handling
    #[inline]
    pub fn map_deserialize(
        buffer: &mut BytesMut,
        len: usize,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, CheetahString>> {
        if len == 0 {
            return Ok(HashMap::new());
        }

        // Pre-allocate HashMap with estimated capacity (assume ~50 bytes per entry)
        let estimated_entries = (len / 50).clamp(4, 1024);
        let mut map = HashMap::with_capacity(estimated_entries);

        let target_remaining = buffer.remaining().saturating_sub(len);

        while buffer.remaining() > target_remaining {
            // Read key (short length prefix)
            let key = Self::read_str(buffer, true, len)?.ok_or_else(|| RocketmqError::DecodingError(0, 0))?;

            // Read value (long length prefix)
            let value = Self::read_str(buffer, false, len)?.ok_or_else(|| RocketmqError::DecodingError(0, 0))?;

            map.insert(key, value);
        }

        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn write_str_short_length() {
        let mut buf = BytesMut::new();
        let written = RocketMQSerializable::write_str(&mut buf, true, "test");
        assert_eq!(written, 6);
        assert_eq!(buf, BytesMut::from(&[0, 4, 116, 101, 115, 116][..]));
    }

    #[test]
    fn write_str_long_length() {
        let mut buf = BytesMut::new();
        let written = RocketMQSerializable::write_str(&mut buf, false, "test");
        assert_eq!(written, 8);
        assert_eq!(buf, BytesMut::from(&[0, 0, 0, 4, 116, 101, 115, 116][..]));
    }

    #[test]
    fn read_str_short_length() {
        let mut buf = BytesMut::from(&[0, 4, 116, 101, 115, 116][..]);
        let read = RocketMQSerializable::read_str(&mut buf, true, 10).unwrap();
        assert_eq!(read, Some("test".into()));
    }

    #[test]
    fn read_str_long_length() {
        let mut buf = BytesMut::from(&[0, 0, 0, 4, 116, 101, 115, 116][..]);
        let read = RocketMQSerializable::read_str(&mut buf, false, 10).unwrap();
        assert_eq!(read, Some("test".into()));
    }

    #[test]
    fn read_str_exceeds_limit() {
        let mut buf = BytesMut::from(&[0, 0, 0, 4, 116, 101, 115, 116][..]);
        let read = RocketMQSerializable::read_str(&mut buf, false, 2);
        assert!(read.is_err());
    }

    #[test]
    fn map_serialize_empty() {
        let map = HashMap::new();
        let serialized = RocketMQSerializable::map_serialize(&map);
        assert!(serialized.is_none());
    }

    #[test]
    fn map_serialize_non_empty() {
        let mut map = HashMap::new();
        map.insert("key".into(), "value".into());
        let serialized = RocketMQSerializable::map_serialize(&map).unwrap();
        assert_eq!(
            serialized,
            BytesMut::from(&[0, 3, 107, 101, 121, 0, 0, 0, 5, 118, 97, 108, 117, 101][..])
        );
    }

    #[test]
    fn map_deserialize_empty() {
        let mut buf = BytesMut::new();
        let deserialized = RocketMQSerializable::map_deserialize(&mut buf, 0).unwrap();
        assert!(deserialized.is_empty());
    }

    #[test]
    fn map_deserialize_non_empty() {
        let mut buf = BytesMut::from(&[0, 3, 107, 101, 121, 0, 0, 0, 5, 118, 97, 108, 117, 101][..]);
        let deserialized = RocketMQSerializable::map_deserialize(&mut buf, 14).unwrap();
        assert_eq!(deserialized, [("key".into(), "value".into())].iter().cloned().collect());
    }
}
