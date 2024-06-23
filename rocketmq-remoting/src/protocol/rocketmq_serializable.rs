/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;
use std::str;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;

use crate::error::RemotingCommandError;
use crate::protocol::remoting_command::RemotingCommand;
use crate::protocol::LanguageCode;

pub struct RocketMQSerializable;

impl RocketMQSerializable {
    pub fn write_str(buf: &mut BytesMut, use_short_length: bool, s: &str) -> usize {
        let mut total = 0;
        let bytes = s.as_bytes();
        total += bytes.len();
        if use_short_length {
            buf.put_u16(bytes.len() as u16);
            total += 2;
        } else {
            buf.put_u32(bytes.len() as u32);
            total += 4;
        }
        buf.put(bytes);
        total
    }

    pub fn read_str(
        buf: &mut BytesMut,
        use_short_length: bool,
        limit: usize,
    ) -> Result<Option<String>, RemotingCommandError> {
        let len = if use_short_length {
            buf.get_u16() as usize
        } else {
            buf.get_u32() as usize
        };

        if len == 0 {
            return Ok(None);
        }

        if len > limit {
            return Err(RemotingCommandError::DecodingError(len, limit));
        }

        let bytes = buf.split_to(len).freeze(); // Convert BytesMut to Bytes
        str::from_utf8(&bytes)
            .map(|s| Some(s.to_string()))
            .map_err(RemotingCommandError::Utf8Error)
    }

    pub fn rocketmq_protocol_encode(cmd: &mut RemotingCommand, buf: &mut BytesMut) -> usize {
        let begin_index = buf.len();
        buf.put_u16(cmd.code() as u16);
        buf.put_u8(cmd.language().get_code());
        buf.put_u16(cmd.version() as u16);
        buf.put_i32(cmd.opaque());
        buf.put_i32(cmd.flag());
        if let Some(remark) = cmd.remark() {
            Self::write_str(buf, true, remark.as_str());
        } else {
            buf.put_i32(0);
        }
        let map_len_index = buf.len();
        buf.put_i32(0);
        let header = cmd.command_custom_header_mut().unwrap();
        if header.support_fast_codec() {
            header.encode_fast(buf);
        }
        if let Some(ext_fields) = cmd.ext_fields() {
            ext_fields.iter().for_each(|(k, v)| {
                if k.is_empty() || v.is_empty() {
                    return;
                }
                Self::write_str(buf, true, k.as_str());
                Self::write_str(buf, true, v.as_str());
            });
        }
        let current_length = buf.len();
        buf[map_len_index..map_len_index + 4]
            .copy_from_slice(&((current_length - map_len_index - 4) as i32).to_be_bytes());
        buf.len() - begin_index
    }

    fn rocket_mq_protocol_encode_bytes(cmd: &RemotingCommand) -> Bytes {
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

    pub fn map_serialize(map: &HashMap<String, String>) -> Option<BytesMut> {
        // Calculate the total length of the serialized map
        let mut total_length = 0;

        for (key, value) in map.iter() {
            if key.is_empty() || value.is_empty() {
                continue;
            }
            total_length += 2 + key.len() + 4 + value.len();
        }

        if total_length == 0 {
            return None;
        }

        // Allocate the BytesMut buffer with the calculated length
        let mut content = BytesMut::with_capacity(total_length);

        for (key, value) in map.iter() {
            if key.is_empty() || value.is_empty() {
                continue;
            }
            // Serialize key
            content.put_u16(key.len() as u16);
            content.put_slice(key.as_bytes());

            // Serialize value
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
    ) -> Result<RemotingCommand, RemotingCommandError> {
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
                return Err(RemotingCommandError::DecodingError(
                    ext_fields_length,
                    header_len,
                ));
            }
            Self::map_deserialize(header_buffer, ext_fields_length)?
        } else {
            HashMap::new()
        };

        Ok(cmd.set_remark(remark).set_ext_fields(ext))
    }

    pub fn map_deserialize(
        buffer: &mut BytesMut,
        len: usize,
    ) -> Result<HashMap<String, String>, RemotingCommandError> {
        let mut map = HashMap::new();
        let end_index = buffer.len() - len;

        while buffer.remaining() > end_index {
            let key = Self::read_str(buffer, true, len)?.unwrap();
            let value = Self::read_str(buffer, false, len)?.unwrap();
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
        assert_eq!(read, Some("test".to_string()));
    }

    #[test]
    fn read_str_long_length() {
        let mut buf = BytesMut::from(&[0, 0, 0, 4, 116, 101, 115, 116][..]);
        let read = RocketMQSerializable::read_str(&mut buf, false, 10).unwrap();
        assert_eq!(read, Some("test".to_string()));
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
        map.insert("key".to_string(), "value".to_string());
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
        let mut buf =
            BytesMut::from(&[0, 3, 107, 101, 121, 0, 0, 0, 5, 118, 97, 108, 117, 101][..]);
        let deserialized = RocketMQSerializable::map_deserialize(&mut buf, 14).unwrap();
        assert_eq!(
            deserialized,
            [("key".to_string(), "value".to_string())]
                .iter()
                .cloned()
                .collect()
        );
    }
}
