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

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::HeaderEncodeCapability;
use crate::protocol::header_codec::BinaryHeaderFields;
use crate::protocol::header_codec::HeaderCodecError;
use crate::protocol::header_field_merge::has_custom_ext_collision;
use crate::protocol::header_field_merge::merge_header_and_dynamic;
use crate::protocol::remoting_command::RemotingCommand;
use crate::protocol::LanguageCode;

const MAP_ENTRY_ENCODED_BYTES_ESTIMATE: usize = 16;
const MIN_MAP_CAPACITY: usize = 4;
const MAX_MAP_CAPACITY: usize = 1024;

fn decoding_error(required: usize, available: usize) -> rocketmq_error::RocketMQError {
    rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
        format: "binary",
        message: format!("required {required} bytes, got {available}"),
    })
}

fn trailing_header_error(remaining: usize) -> rocketmq_error::RocketMQError {
    rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
        format: "binary",
        message: format!("ROCKETMQ header has {remaining} trailing bytes after extension fields"),
    })
}

fn sorted_ext_fields(map: &HashMap<CheetahString, CheetahString>) -> Vec<(&CheetahString, &CheetahString)> {
    let mut entries = map.iter().filter(|(key, _)| !key.is_empty()).collect::<Vec<_>>();
    entries.sort_unstable_by(|(left, _), (right, _)| left.as_str().cmp(right.as_str()));
    entries
}

pub struct RocketMQSerializable;

impl RocketMQSerializable {
    pub(crate) const INITIAL_ENCODE_CAPACITY: usize = 256;

    #[inline]
    fn estimated_map_capacity(encoded_len: usize) -> usize {
        (encoded_len / MAP_ENTRY_ENCODED_BYTES_ESTIMATE).clamp(MIN_MAP_CAPACITY, MAX_MAP_CAPACITY)
    }

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

    /// Optimized string read with enhanced boundary checks
    #[inline]
    pub fn read_str(
        buf: &mut BytesMut,
        use_short_length: bool,
        limit: usize,
    ) -> rocketmq_error::RocketMQResult<Option<CheetahString>> {
        // Read length prefix
        let len = if use_short_length {
            if buf.remaining() < 2 {
                return Err(decoding_error(2, buf.remaining()));
            }
            buf.get_u16() as usize
        } else {
            if buf.remaining() < 4 {
                return Err(decoding_error(4, buf.remaining()));
            }
            buf.get_u32() as usize
        };

        // Empty string
        if len == 0 {
            return Ok(None);
        }

        // Boundary check
        if len > limit {
            return Err(decoding_error(len, limit));
        }

        // Ensure buffer has enough data
        if buf.remaining() < len {
            return Err(decoding_error(len, buf.remaining()));
        }

        // Checked UTF-8 decode with CheetahString storage optimization
        let bytes = buf.split_to(len).freeze();
        Ok(Some(CheetahString::try_from_bytes_buf(bytes)?))
    }

    /// Optimized ROCKETMQ protocol encoding with reduced allocations
    #[inline]
    pub fn rocketmq_protocol_encode(cmd: &mut RemotingCommand, buf: &mut BytesMut) -> usize {
        let checkpoint = buf.len();
        match Self::try_rocketmq_protocol_encode(cmd, buf) {
            Ok(encoded) => encoded,
            Err(_) => {
                buf.truncate(checkpoint);
                0
            }
        }
    }

    /// Fallible ROCKETMQ protocol encoding with reduced allocations.
    ///
    /// # Errors
    ///
    /// Returns the direct custom-header encoding failure when the selected
    /// header cannot be represented in the ROCKETMQ extension-field payload.
    #[inline]
    pub fn try_rocketmq_protocol_encode(cmd: &RemotingCommand, buf: &mut BytesMut) -> Result<usize, HeaderCodecError> {
        Self::try_rocketmq_protocol_encode_with_capability(cmd, buf, cmd.custom_header_encode_capability())
    }

    pub(crate) fn try_rocketmq_protocol_encode_with_capability(
        cmd: &RemotingCommand,
        buf: &mut BytesMut,
        capability: HeaderEncodeCapability,
    ) -> Result<usize, HeaderCodecError> {
        // A bounded initial allocation is cheaper than walking every typed and
        // dynamic field before immediately walking them again to encode. Large
        // headers retain BytesMut's normal growth behavior.
        buf.reserve(Self::INITIAL_ENCODE_CAPACITY);
        let checkpoint = buf.len();
        let result = Self::try_rocketmq_protocol_encode_inner(cmd, buf, capability);
        if result.is_err() {
            buf.truncate(checkpoint);
        }
        result
    }

    fn try_rocketmq_protocol_encode_inner(
        cmd: &RemotingCommand,
        buf: &mut BytesMut,
        capability: HeaderEncodeCapability,
    ) -> Result<usize, HeaderCodecError> {
        let begin_index = buf.len();

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

        // Keep the common direct path map-free. Semantic overlaps use the
        // single authoritative typed/dynamic merge contract.
        match cmd.command_custom_header_ref() {
            Some(header)
                if capability == HeaderEncodeCapability::DirectBinary
                    && !has_custom_ext_collision(header, cmd.ext_fields()) =>
            {
                header.encode_direct_binary(buf)?;
                if let Some(ext_fields) = cmd.ext_fields() {
                    Self::write_ext_fields(buf, ext_fields)?;
                }
            }
            Some(header) => {
                let merged = merge_header_and_dynamic(header, cmd.ext_fields())?;
                Self::write_ext_fields(buf, &merged)?;
            }
            None => {
                if let Some(ext_fields) = cmd.ext_fields() {
                    Self::write_ext_fields(buf, ext_fields)?;
                }
            }
        }

        // Update ext_fields length in-place
        let current_length = buf.len();
        let ext_fields_length = Self::checked_ext_fields_length(current_length - map_len_index - 4)?;
        buf[map_len_index..map_len_index + 4].copy_from_slice(&ext_fields_length.to_be_bytes());

        Ok(buf.len() - begin_index)
    }

    /// Encodes the common direct-header shape without map materialization or
    /// collision checks. Callers must establish that no remark or dynamic
    /// extension fields are present.
    pub(crate) fn try_rocketmq_protocol_encode_direct(
        cmd: &RemotingCommand,
        header: &dyn CommandCustomHeader,
        buf: &mut BytesMut,
    ) -> Result<usize, HeaderCodecError> {
        const FIXED_HEADER_LENGTH: usize = 21;
        const EXT_FIELDS_LENGTH_OFFSET: usize = 17;

        let begin_index = buf.len();
        let mut fixed = [0u8; FIXED_HEADER_LENGTH];
        fixed[..2].copy_from_slice(&(cmd.code() as u16).to_be_bytes());
        fixed[2] = cmd.language().get_code();
        fixed[3..5].copy_from_slice(&(cmd.version() as u16).to_be_bytes());
        fixed[5..9].copy_from_slice(&cmd.opaque().to_be_bytes());
        fixed[9..13].copy_from_slice(&cmd.flag().to_be_bytes());
        buf.extend_from_slice(&fixed);

        header.encode_direct_binary(buf)?;

        let ext_fields_length = Self::checked_ext_fields_length(buf.len() - begin_index - FIXED_HEADER_LENGTH)?;
        let map_len_index = begin_index + EXT_FIELDS_LENGTH_OFFSET;
        buf[map_len_index..map_len_index + 4].copy_from_slice(&ext_fields_length.to_be_bytes());
        Ok(buf.len() - begin_index)
    }

    #[inline]
    fn write_ext_fields(
        buf: &mut BytesMut,
        fields: &HashMap<CheetahString, CheetahString>,
    ) -> Result<(), HeaderCodecError> {
        for (key, value) in sorted_ext_fields(fields) {
            let key_length = Self::checked_dynamic_key_length(key.len())?;
            let value_length = Self::checked_dynamic_value_length(value.len())?;
            buf.put_u16(key_length);
            buf.put_slice(key.as_bytes());
            buf.put_i32(value_length);
            buf.put_slice(value.as_bytes());
        }
        Ok(())
    }

    #[inline]
    fn checked_ext_fields_length(length: usize) -> Result<i32, HeaderCodecError> {
        i32::try_from(length).map_err(|_| HeaderCodecError::ExtensionFieldsLengthOverflow)
    }

    #[inline]
    fn checked_dynamic_key_length(length: usize) -> Result<u16, HeaderCodecError> {
        u16::try_from(length).map_err(|_| HeaderCodecError::DynamicKeyLengthOverflow)
    }

    #[inline]
    fn checked_dynamic_value_length(length: usize) -> Result<i32, HeaderCodecError> {
        i32::try_from(length).map_err(|_| HeaderCodecError::DynamicValueLengthOverflow)
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
            if !key.is_empty() {
                total_length += 2 + key.len() + 4 + value.len();
                valid_entries += 1;
            }
        }

        if valid_entries == 0 {
            return None;
        }

        // Allocate exact capacity (avoid reallocations)
        let mut content = BytesMut::with_capacity(total_length);

        // Serialize entries in canonical key order so equivalent maps produce identical frames.
        for (key, value) in sorted_ext_fields(map) {
            // Write key: u16 length + bytes
            content.put_u16(key.len() as u16);
            content.put_slice(key.as_bytes());

            // Write value: i32 length + bytes
            content.put_i32(value.len() as i32);
            content.put_slice(value.as_bytes());
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
        let available = header_buffer.remaining();
        if available < header_len {
            return Err(decoding_error(header_len, available));
        }
        if available > header_len {
            return Err(trailing_header_error(available - header_len));
        }
        Self::rocket_mq_protocol_decode_bytes(header_buffer.split().freeze())
    }

    /// Decodes an immutable ROCKETMQ header without intermediate buffer splits.
    pub(crate) fn rocket_mq_protocol_decode_bytes(header: Bytes) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        const FIXED_HEADER_LEN: usize = 13;
        const LENGTH_FIELD_LEN: usize = 4;

        if header.len() < FIXED_HEADER_LEN {
            return Err(decoding_error(FIXED_HEADER_LEN, header.len()));
        }

        let mut cmd = RemotingCommand::default();
        cmd.set_code_ref(i16::from_be_bytes([header[0], header[1]]));
        cmd.set_language_ref(LanguageCode::from(header[2]));
        cmd.set_version_ref(i16::from_be_bytes([header[3], header[4]]) as i32);
        cmd.set_opaque_mut(i32::from_be_bytes([header[5], header[6], header[7], header[8]]));
        cmd.set_flag_ref(i32::from_be_bytes([header[9], header[10], header[11], header[12]]));

        let mut cursor = FIXED_HEADER_LEN;
        if header.len() - cursor < LENGTH_FIELD_LEN {
            return Err(decoding_error(LENGTH_FIELD_LEN, header.len() - cursor));
        }
        let remark_length = u32::from_be_bytes([
            header[cursor],
            header[cursor + 1],
            header[cursor + 2],
            header[cursor + 3],
        ]) as usize;
        cursor += LENGTH_FIELD_LEN;
        if remark_length > header.len() - cursor {
            return Err(decoding_error(remark_length, header.len() - cursor));
        }
        let remark = if remark_length == 0 {
            None
        } else {
            let end = cursor + remark_length;
            let value =
                CheetahString::try_copy_from_bytes(header.slice(cursor..end)).map_err(|error| error.into_parts().1)?;
            cursor = end;
            Some(value)
        };

        if header.len() - cursor < LENGTH_FIELD_LEN {
            return Err(decoding_error(LENGTH_FIELD_LEN, header.len() - cursor));
        }
        let ext_fields_length = i32::from_be_bytes([
            header[cursor],
            header[cursor + 1],
            header[cursor + 2],
            header[cursor + 3],
        ]);
        cursor += LENGTH_FIELD_LEN;
        let payload = if ext_fields_length > 0 {
            let ext_fields_length = ext_fields_length as usize;
            if ext_fields_length > header.len() - cursor {
                return Err(decoding_error(ext_fields_length, header.len() - cursor));
            }
            let end = cursor + ext_fields_length;
            let payload = header.slice(cursor..end);
            cursor = end;
            payload
        } else {
            Bytes::new()
        };
        if cursor != header.len() {
            return Err(trailing_header_error(header.len() - cursor));
        }

        let ext = BinaryHeaderFields::new(payload)?;
        cmd.set_remark_option_mut(remark);
        cmd.set_binary_ext_fields_ref(ext);
        Ok(cmd)
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
        if len > buffer.remaining() {
            return Err(decoding_error(len, buffer.remaining()));
        }

        // Request-header pairs are compact; this bounded estimate avoids the
        // predictable rehashes caused by the previous 50-byte assumption.
        let estimated_entries = Self::estimated_map_capacity(len);
        let mut map = HashMap::with_capacity(estimated_entries);

        let target_remaining = buffer.remaining().saturating_sub(len);

        while buffer.remaining() > target_remaining {
            // Read key (short length prefix)
            let key = Self::read_str(buffer, true, len)?.ok_or_else(|| decoding_error(0, 0))?;

            // Read value (long length prefix)
            if let Some(value) = Self::read_str(buffer, false, len)? {
                map.insert(key, value);
            }
        }

        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use bytes::BytesMut;

    use super::*;

    fn minimal_header_without_ext_len() -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_u8(LanguageCode::JAVA.get_code());
        buf.put_i16(0);
        buf.put_i32(0);
        buf.put_i32(0);
        buf.put_i32(0);
        buf
    }

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
    fn read_str_rejects_invalid_utf8() {
        let mut buf = BytesMut::from(&[0, 2, 0xff, 0xfe][..]);
        let read = RocketMQSerializable::read_str(&mut buf, true, 10);
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
    fn map_serialize_preserves_present_empty_value() {
        let map = HashMap::from([("key".into(), CheetahString::new())]);

        let serialized = RocketMQSerializable::map_serialize(&map).unwrap();

        assert_eq!(serialized, BytesMut::from(&[0, 3, 107, 101, 121, 0, 0, 0, 0][..]));
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

    #[test]
    fn map_capacity_estimate_covers_compact_request_headers_and_stays_bounded() {
        assert_eq!(RocketMQSerializable::estimated_map_capacity(1), 4);
        assert_eq!(RocketMQSerializable::estimated_map_capacity(292), 18);
        assert_eq!(RocketMQSerializable::estimated_map_capacity(385), 24);
        assert_eq!(RocketMQSerializable::estimated_map_capacity(usize::MAX), 1024);
    }

    #[test]
    fn map_deserialize_normalizes_zero_length_value_to_absent() {
        let mut buf = BytesMut::from(&[0, 3, 107, 101, 121, 0, 0, 0, 0][..]);

        let deserialized = RocketMQSerializable::map_deserialize(&mut buf, 9).unwrap();

        assert!(deserialized.is_empty());
        assert!(buf.is_empty());
    }

    #[test]
    fn extension_field_length_checks_limit_and_limit_plus_one() {
        assert_eq!(
            RocketMQSerializable::checked_ext_fields_length(i32::MAX as usize).unwrap(),
            i32::MAX
        );
        assert!(matches!(
            RocketMQSerializable::checked_ext_fields_length(i32::MAX as usize + 1),
            Err(HeaderCodecError::ExtensionFieldsLengthOverflow)
        ));
        assert_eq!(
            RocketMQSerializable::checked_dynamic_key_length(u16::MAX as usize).unwrap(),
            u16::MAX
        );
        assert!(matches!(
            RocketMQSerializable::checked_dynamic_key_length(u16::MAX as usize + 1),
            Err(HeaderCodecError::DynamicKeyLengthOverflow)
        ));
        assert_eq!(
            RocketMQSerializable::checked_dynamic_value_length(i32::MAX as usize).unwrap(),
            i32::MAX
        );
        assert!(matches!(
            RocketMQSerializable::checked_dynamic_value_length(i32::MAX as usize + 1),
            Err(HeaderCodecError::DynamicValueLengthOverflow)
        ));
    }

    #[test]
    fn rocketmq_protocol_decode_rejects_short_fixed_header_without_panic() {
        let mut buf = BytesMut::from(&[0_u8; 12][..]);
        if RocketMQSerializable::rocket_mq_protocol_decode(&mut buf, 12).is_ok() {
            panic!("short fixed header should decode to error");
        }
    }

    #[test]
    fn rocketmq_protocol_decode_rejects_missing_ext_length_without_panic() {
        let mut buf = minimal_header_without_ext_len();
        let header_len = buf.len();
        if RocketMQSerializable::rocket_mq_protocol_decode(&mut buf, header_len).is_ok() {
            panic!("missing ext length should decode to error");
        }
    }

    #[test]
    fn rocketmq_protocol_decode_rejects_truncated_ext_fields_without_panic() {
        let mut buf = minimal_header_without_ext_len();
        buf.put_i32(10);
        let header_len = buf.len();
        if RocketMQSerializable::rocket_mq_protocol_decode(&mut buf, header_len).is_ok() {
            panic!("truncated ext fields should decode to error");
        }
    }

    #[test]
    fn rocketmq_protocol_decode_rejects_bytes_after_declared_ext_fields() {
        let mut buf = minimal_header_without_ext_len();
        buf.put_i32(0);
        buf.put_u8(1);
        let header_len = buf.len();

        assert!(RocketMQSerializable::rocket_mq_protocol_decode(&mut buf, header_len).is_err());
    }

    #[test]
    fn bytes_decoder_preserves_remark_and_extension_fields() {
        let mut buf = BytesMut::new();
        buf.put_i16(10);
        buf.put_u8(LanguageCode::RUST.get_code());
        buf.put_i16(501);
        buf.put_i32(7);
        buf.put_i32(1);
        RocketMQSerializable::write_str(&mut buf, false, "remark");
        let ext_length_offset = buf.len();
        buf.put_i32(0);
        let ext_start = buf.len();
        buf.put_u16(3);
        buf.extend_from_slice(b"key");
        buf.put_i32(5);
        buf.extend_from_slice(b"value");
        let ext_length = (buf.len() - ext_start) as i32;
        buf[ext_length_offset..ext_length_offset + 4].copy_from_slice(&ext_length.to_be_bytes());

        let command = RocketMQSerializable::rocket_mq_protocol_decode_bytes(buf.freeze()).unwrap();

        assert_eq!(command.code(), 10);
        assert_eq!(command.language(), LanguageCode::RUST);
        assert_eq!(command.version(), 501);
        assert_eq!(command.opaque(), 7);
        assert_eq!(command.flag(), 1);
        assert_eq!(command.remark().map(CheetahString::as_str), Some("remark"));
        assert_eq!(command.ext_fields().unwrap().get("key").unwrap(), "value");
    }
}
