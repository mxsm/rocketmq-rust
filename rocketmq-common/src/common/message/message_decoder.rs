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

use std::borrow::Cow;
use std::collections::HashMap;
use std::io::Cursor;
use std::io::Write;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::net::SocketAddrV6;
use std::str;

use byteorder::BigEndian;
use byteorder::ByteOrder;
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahBuilder;
use cheetah_string::CheetahString;

use crate::common::compression::compressor_factory::CompressorFactory;
use crate::common::message::message_client_ext::MessageClientExt;
use crate::common::message::message_ext::MessageExt;
use crate::common::message::message_id::MessageId;
use crate::common::message::message_property::MessageProperties;
use crate::common::message::message_single::Message;
use crate::common::message::MessageConst;
use crate::common::message::MessageTrait;
use crate::common::message::MessageVersion;
use crate::common::sys_flag::message_sys_flag::MessageSysFlag;
use crate::utils::util_all;
use crate::CRC32Utils::crc32;
use crate::MessageAccessor::MessageAccessor;
use crate::MessageUtils::build_message_id;

pub const CHARSET_UTF8: &str = "UTF-8";
pub const MESSAGE_MAGIC_CODE_POSITION: usize = 4;
pub const MESSAGE_FLAG_POSITION: usize = 16;
pub const MESSAGE_PHYSIC_OFFSET_POSITION: usize = 28;
pub const MESSAGE_STORE_TIMESTAMP_POSITION: usize = 56;
pub const MESSAGE_MAGIC_CODE: i32 = -626843481;
pub const MESSAGE_MAGIC_CODE_V2: i32 = -626843477;
pub const BLANK_MAGIC_CODE: i32 = -875286124;
pub const NAME_VALUE_SEPARATOR: char = '\u{0001}';
pub const PROPERTY_SEPARATOR: char = '\u{0002}';
pub const PHY_POS_POSITION: usize = 4 + 4 + 4 + 4 + 4 + 8;
pub const QUEUE_OFFSET_POSITION: usize = 4 + 4 + 4 + 4 + 4;
pub const SYSFLAG_POSITION: usize = 4 + 4 + 4 + 4 + 4 + 8 + 8;
pub const BORN_TIMESTAMP_POSITION: usize = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8;

fn get_i16_checked(buffer: &mut Bytes) -> Option<i16> {
    if buffer.remaining() < 2 {
        None
    } else {
        Some(buffer.get_i16())
    }
}

fn get_u8_checked(buffer: &mut Bytes) -> Option<u8> {
    if buffer.remaining() < 1 {
        None
    } else {
        Some(buffer.get_u8())
    }
}

fn get_i32_checked(buffer: &mut Bytes) -> Option<i32> {
    if buffer.remaining() < 4 {
        None
    } else {
        Some(buffer.get_i32())
    }
}

fn get_u32_checked(buffer: &mut Bytes) -> Option<u32> {
    if buffer.remaining() < 4 {
        None
    } else {
        Some(buffer.get_u32())
    }
}

fn get_i64_checked(buffer: &mut Bytes) -> Option<i64> {
    if buffer.remaining() < 8 {
        None
    } else {
        Some(buffer.get_i64())
    }
}

fn split_to_checked(buffer: &mut Bytes, len: usize) -> Option<Bytes> {
    if buffer.remaining() < len {
        None
    } else {
        Some(buffer.split_to(len))
    }
}

fn copy_to_array_checked<const N: usize>(buffer: &mut Bytes) -> Option<[u8; N]> {
    if buffer.remaining() < N {
        None
    } else {
        let mut bytes = [0; N];
        buffer.copy_to_slice(&mut bytes);
        Some(bytes)
    }
}

fn get_topic_length_checked(version: MessageVersion, buffer: &mut Bytes) -> Option<usize> {
    match version {
        MessageVersion::V1 => get_u8_checked(buffer).map(usize::from),
        MessageVersion::V2 => {
            get_i16_checked(buffer).and_then(|value| if value < 0 { None } else { Some(value as usize) })
        }
    }
}

pub fn string_to_message_properties(properties: Option<&CheetahString>) -> HashMap<CheetahString, CheetahString> {
    let mut map = HashMap::new();
    if let Some(properties) = properties {
        let mut index = 0;
        let len = properties.len();
        while index < len {
            let new_index = properties[index..].find(PROPERTY_SEPARATOR).map_or(len, |i| index + i);
            if new_index - index >= 3 {
                if let Some(kv_sep_index) = properties[index..new_index].find(NAME_VALUE_SEPARATOR) {
                    let kv_sep_index = index + kv_sep_index;
                    if kv_sep_index > index && kv_sep_index < new_index - 1 {
                        let k = &properties[index..kv_sep_index];
                        let v = &properties[kv_sep_index + 1..new_index];
                        map.insert(CheetahString::from_slice(k), CheetahString::from_slice(v));
                    }
                }
            }
            index = new_index + 1;
        }
    }
    map
}

pub fn str_to_message_properties(properties: Option<&str>) -> HashMap<CheetahString, CheetahString> {
    let mut map = HashMap::new();
    if let Some(properties) = properties {
        let mut index = 0;
        let len = properties.len();
        while index < len {
            let new_index = properties[index..].find(PROPERTY_SEPARATOR).map_or(len, |i| index + i);
            if new_index - index >= 3 {
                if let Some(kv_sep_index) = properties[index..new_index].find(NAME_VALUE_SEPARATOR) {
                    let kv_sep_index = index + kv_sep_index;
                    if kv_sep_index > index && kv_sep_index < new_index - 1 {
                        let k = &properties[index..kv_sep_index];
                        let v = &properties[kv_sep_index + 1..new_index];
                        map.insert(CheetahString::from_slice(k), CheetahString::from_slice(v));
                    }
                }
            }
            index = new_index + 1;
        }
    }
    map
}

pub fn message_properties_to_string(properties: &HashMap<CheetahString, CheetahString>) -> CheetahString {
    let mut len = 0;
    for (name, value) in properties.iter() {
        len += name.len();

        len += value.len();
        len += 2; // separator
    }

    let mut builder = CheetahBuilder::with_capacity(len);
    for (name, value) in properties.iter() {
        builder.push_str(name.as_str());
        builder.push(NAME_VALUE_SEPARATOR);

        builder.push_str(value.as_str());
        builder.push(PROPERTY_SEPARATOR);
    }
    builder.finish_string()
}

fn cheetah_from_utf8_lossy(bytes: &[u8]) -> CheetahString {
    match String::from_utf8_lossy(bytes) {
        Cow::Borrowed(value) => CheetahString::from_slice(value),
        Cow::Owned(value) => CheetahString::from_string_owned(value),
    }
}

pub fn decode_client(
    byte_buffer: &mut Bytes,
    read_body: bool,
    de_compress_body: bool,
    is_set_properties_string: bool,
    check_crc: bool,
) -> Option<MessageClientExt> {
    /*if let Some(msg_ext) = decode(
        byte_buffer,
        read_body,
        de_compress_body,
        false,
        is_set_properties_string,
        check_crc,
    ) {
        Some(MessageClientExt {
            message_ext_inner: msg_ext,
        })
    } else {
        None
    }*/
    decode(
        byte_buffer,
        read_body,
        de_compress_body,
        false,
        is_set_properties_string,
        check_crc,
    )
    .map(|msg_ext| MessageClientExt {
        message_ext_inner: msg_ext,
    })
}

//this method will optimize later
pub fn decode(
    byte_buffer: &mut Bytes,
    read_body: bool,
    de_compress_body: bool,
    is_client: bool,
    is_set_properties_string: bool,
    check_crc: bool,
) -> Option<MessageExt> {
    let initial_remaining = byte_buffer.remaining();
    let mut msg_ext = MessageExt::default();

    // 1 TOTALSIZE
    let store_size = get_i32_checked(byte_buffer)?;
    if store_size <= 0 || store_size as usize > initial_remaining {
        return None;
    }
    msg_ext.set_store_size(store_size);

    // 2 MAGICCODE
    let magic_code = get_i32_checked(byte_buffer)?;
    let version = MessageVersion::value_of_magic_code(magic_code).ok()?;

    // 3 BODYCRC
    let body_crc = get_u32_checked(byte_buffer)?;
    msg_ext.set_body_crc(body_crc);

    // 4 QUEUEID
    let queue_id = get_i32_checked(byte_buffer)?;
    msg_ext.set_queue_id(queue_id);

    // 5 FLAG
    let flag = get_i32_checked(byte_buffer)?;
    msg_ext.message.set_flag(flag);

    // 6 QUEUEOFFSET
    let queue_offset = get_i64_checked(byte_buffer)?;
    msg_ext.set_queue_offset(queue_offset);

    // 7 PHYSICALOFFSET
    let physic_offset = get_i64_checked(byte_buffer)?;
    msg_ext.set_commit_log_offset(physic_offset);

    // 8 SYSFLAG
    let sys_flag = get_i32_checked(byte_buffer)?;
    msg_ext.set_sys_flag(sys_flag);

    // 9 BORNTIMESTAMP
    let born_time_stamp = get_i64_checked(byte_buffer)?;
    msg_ext.set_born_timestamp(born_time_stamp);

    // 10 BORNHOST
    let (born_host_address, born_host_ip_length) = if sys_flag & MessageSysFlag::BORNHOST_V6_FLAG != 0 {
        let born_host = copy_to_array_checked::<16>(byte_buffer)?;
        let port = get_i32_checked(byte_buffer)?;
        (
            SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::from(born_host), port as u16, 0, 0)),
            16,
        )
    } else {
        let born_host = copy_to_array_checked::<4>(byte_buffer)?;
        let port = get_i32_checked(byte_buffer)?;
        (
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from(born_host), port as u16)),
            4,
        )
    };
    msg_ext.set_born_host(born_host_address);

    // 11 STORETIMESTAMP
    let store_timestamp = get_i64_checked(byte_buffer)?;
    msg_ext.set_store_timestamp(store_timestamp);

    // 12 STOREHOST
    let (store_host_address, store_host_ip_length) = if sys_flag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG != 0 {
        let store_host = copy_to_array_checked::<16>(byte_buffer)?;
        let port = get_i32_checked(byte_buffer)?;
        (
            SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::from(store_host), port as u16, 0, 0)),
            16,
        )
    } else {
        let store_host = copy_to_array_checked::<4>(byte_buffer)?;
        let port = get_i32_checked(byte_buffer)?;
        (
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from(store_host), port as u16)),
            4,
        )
    };
    msg_ext.set_store_host(store_host_address);

    // 13 RECONSUMETIMES
    let reconsume_times = get_i32_checked(byte_buffer)?;
    msg_ext.set_reconsume_times(reconsume_times);

    // 14 Prepared Transaction Offset
    let prepared_transaction_offset = get_i64_checked(byte_buffer)?;
    msg_ext.set_prepared_transaction_offset(prepared_transaction_offset);

    // 15 BODY
    let body_len = get_i32_checked(byte_buffer)?;
    if body_len < 0 {
        return None;
    }
    let body_len = body_len as usize;
    if body_len > 0 {
        // Handle reading and processing body
        if read_body {
            let body = split_to_checked(byte_buffer, body_len)?;
            if check_crc {
                let crc = crc32(&body);
                if crc != body_crc {
                    return None;
                }
            }
            let mut body_bytes = body;
            if de_compress_body && (sys_flag & MessageSysFlag::COMPRESSED_FLAG) == MessageSysFlag::COMPRESSED_FLAG {
                let compression_type = MessageSysFlag::try_get_compression_type(sys_flag).ok()?;
                body_bytes = compression_type.try_decompression(&body_bytes).ok()?;
            }
            msg_ext.message.set_body(Some(body_bytes));
        } else {
            let _ = split_to_checked(byte_buffer, body_len)?;
        }
    }

    // 16 TOPIC
    let _address_len = born_host_ip_length + store_host_ip_length;
    let topic_len = get_topic_length_checked(version, byte_buffer)?;
    let topic = split_to_checked(byte_buffer, topic_len)?;
    let topic_str = str::from_utf8(&topic).ok()?;
    msg_ext.message.set_topic(CheetahString::from_slice(topic_str));

    // 17 properties
    let properties_length = get_i16_checked(byte_buffer)?;
    if properties_length < 0 {
        return None;
    }
    if properties_length > 0 {
        // Handle reading and processing properties
        let properties = split_to_checked(byte_buffer, properties_length as usize)?;
        if !is_set_properties_string {
            let properties_string = cheetah_from_utf8_lossy(properties.as_ref());
            let message_properties = string_to_message_properties(Some(&properties_string));
            *msg_ext.message.properties_mut() = MessageProperties::from_map(message_properties);
        } else {
            let properties_string = cheetah_from_utf8_lossy(properties.as_ref());
            let mut message_properties = string_to_message_properties(Some(&properties_string));
            message_properties.insert(CheetahString::from_static_str("propertiesString"), properties_string);
            *msg_ext.message.properties_mut() = MessageProperties::from_map(message_properties);
        }
    }
    let msg_id = build_message_id(store_host_address, physic_offset);
    msg_ext.set_msg_id(CheetahString::from_string(msg_id));

    Some(msg_ext)
}

pub fn count_inner_msg_num(bytes: Option<Bytes>) -> u32 {
    match bytes {
        None => 0,
        Some(mut bytes) => {
            let mut count = 0;
            while bytes.has_remaining() {
                let size = bytes.slice(0..4).get_i32();
                if size as usize > bytes.len() {
                    break;
                }
                let _ = bytes.split_to(size as usize);
                count += 1;
            }
            count
        }
    }
}

pub fn encode_messages(messages: &[Message]) -> Bytes {
    let mut bytes = BytesMut::new();
    //let mut all_size = 0;
    for message in messages {
        let message_bytes = encode_message(message);
        //all_size += message_bytes.len();
        bytes.put_slice(&message_bytes);
    }
    bytes.freeze()
}

pub fn encode_message(message: &Message) -> Bytes {
    let empty_body = Bytes::new();
    let body = message.get_body().unwrap_or(&empty_body);
    let body_len = body.len();
    let properties = message_properties_to_string(message.properties().as_map());
    let properties_bytes = properties.as_bytes();
    let properties_length = properties_bytes.len();

    let store_size = 4 // 1 TOTALSIZE
             + 4 // 2 MAGICCOD
             + 4 // 3 BODYCRC
             + 4 // 4 FLAG
             + 4 + body_len // 4 BODY
             + 2 + properties_length;

    let mut bytes = BytesMut::with_capacity(store_size);

    // 1 TOTALSIZE
    bytes.put_i32(store_size as i32);

    // 2 MAGICCODE
    bytes.put_i32(0);

    // 3 BODYCRC
    bytes.put_u32(0);

    // 4 FLAG
    bytes.put_i32(message.flag());

    // 5 BODY
    bytes.put_i32(body_len as i32);
    bytes.put_slice(body);

    // 6 PROPERTIES
    bytes.put_i16(properties_length as i16);
    bytes.put_slice(properties_bytes);

    bytes.freeze()
}

pub fn decodes_batch(byte_buffer: &mut Bytes, read_body: bool, decompress_body: bool) -> Vec<MessageExt> {
    let mut messages = Vec::new();
    while byte_buffer.has_remaining() {
        if let Some(msg_ext) = decode(byte_buffer, read_body, decompress_body, false, false, false) {
            messages.push(msg_ext);
        } else {
            break;
        }
    }
    messages
}

pub fn decodes_batch_client(byte_buffer: &mut Bytes, read_body: bool, decompress_body: bool) -> Vec<MessageClientExt> {
    let mut messages = Vec::new();
    while byte_buffer.has_remaining() {
        if let Some(msg_ext) = decode_client(byte_buffer, read_body, decompress_body, false, false) {
            messages.push(msg_ext);
        } else {
            break;
        }
    }
    messages
}

pub fn decode_messages_from(mut message_ext: MessageExt, vec_: &mut Vec<MessageExt>) {
    let Some(body) = message_ext.message.body_mut().raw_mut().as_mut() else {
        return;
    };
    let messages = decode_messages(body);
    for message in messages {
        let mut message_ext_inner = MessageExt {
            message,
            ..MessageExt::default()
        };
        message_ext_inner.set_topic(message_ext.topic().to_owned());
        message_ext_inner.queue_offset = message_ext.queue_offset;
        message_ext_inner.queue_id = message_ext.queue_id;
        message_ext_inner.set_flag(message_ext.get_flag());
        //MessageAccessor::set_properties(&mut
        // message_client_ext,message.get_properties().clone()); messageClientExt.
        // setBody(message.getBody())
        message_ext_inner.store_host = message_ext.store_host;
        message_ext_inner.born_host = message_ext.born_host;
        message_ext_inner.store_timestamp = message_ext.store_timestamp;
        message_ext_inner.born_timestamp = message_ext.born_timestamp;
        message_ext_inner.sys_flag = message_ext.sys_flag;
        message_ext_inner.commit_log_offset = message_ext.commit_log_offset;
        message_ext_inner.set_wait_store_msg_ok(message_ext.is_wait_store_msg_ok());
        vec_.push(message_ext_inner);
    }
}

pub fn decode_messages(buffer: &mut Bytes) -> Vec<Message> {
    let mut messages = Vec::new();
    while buffer.has_remaining() {
        let Some(message) = try_decode_message(buffer) else {
            break;
        };
        messages.push(message);
    }
    messages
}

pub fn decode_message(buffer: &mut Bytes) -> Message {
    try_decode_message(buffer).unwrap_or_default()
}

fn try_decode_message(buffer: &mut Bytes) -> Option<Message> {
    // 1 TOTALSIZE
    let store_size = get_i32_checked(buffer)?;
    if store_size <= 0 {
        return None;
    }

    // 2 MAGICCODE
    let _ = get_i32_checked(buffer)?;

    // 3 BODYCRC
    let _ = get_i32_checked(buffer)?;

    // 4 FLAG
    let flag = get_i32_checked(buffer)?;

    // 5 BODY
    let body_len = get_i32_checked(buffer)?;
    if body_len < 0 {
        return None;
    }
    let body = split_to_checked(buffer, body_len as usize)?;

    // 6 properties
    let properties_length = get_i16_checked(buffer)?;
    if properties_length < 0 {
        return None;
    }
    let properties = split_to_checked(buffer, properties_length as usize)?;
    //string_to_message_properties(Some(&String::from_utf8_lossy(properties.as_ref()).
    // to_string()));
    let message_properties = str_to_message_properties(Some(str::from_utf8(&properties).ok()?));

    let mut message = Message::default();
    message.set_body(Some(body));
    message.set_properties(message_properties);
    message.set_flag(flag);
    Some(message)
}

const MSG_ID_IPV4_LEN: usize = 32;
const MSG_ID_IPV6_LEN: usize = 56;

pub fn validate_message_id(msg_id: &str) -> Result<(), String> {
    let msg_id = msg_id.trim();

    if msg_id.is_empty() {
        return Err("Message ID cannot be empty".to_string());
    }

    let len = msg_id.len();
    if len != MSG_ID_IPV4_LEN && len != MSG_ID_IPV6_LEN {
        return Err(format!(
            "Invalid message ID length: {len}. Expected {MSG_ID_IPV4_LEN} characters (IPv4) or {MSG_ID_IPV6_LEN} \
             characters (IPv6)"
        ));
    }

    if !msg_id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("Message ID must be a valid hexadecimal string".to_string());
    }

    Ok(())
}

pub fn decode_message_id(msg_id: &str) -> Result<MessageId, String> {
    validate_message_id(msg_id)?;
    let bytes = util_all::string_to_bytes(msg_id)
        .ok_or_else(|| "Failed to decode message ID: invalid hex string".to_string())?;
    let mut buffer = Bytes::from(bytes);
    let address = if msg_id.len() == 32 {
        let mut ip = [0u8; 4];
        buffer.copy_to_slice(&mut ip);
        let port = buffer.get_i32();
        SocketAddr::new(IpAddr::V4(Ipv4Addr::from(ip)), port as u16)
    } else {
        let mut ip = [0u8; 16];
        buffer.copy_to_slice(&mut ip);
        let port = buffer.get_i32();
        SocketAddr::new(IpAddr::V6(Ipv6Addr::from(ip)), port as u16)
    };
    Ok(MessageId {
        address,
        offset: buffer.get_i64(),
    })
}

pub fn encode(message_ext: &MessageExt, need_compress: bool) -> rocketmq_error::RocketMQResult<Bytes> {
    let body = message_ext
        .get_body()
        .ok_or_else(|| rocketmq_error::RocketMQError::illegal_argument("message body is required"))?;
    let topic = message_ext.topic().as_bytes();
    let topic_len = topic.len();
    let properties = message_properties_to_string(message_ext.get_properties());
    let properties_bytes = properties.as_bytes();
    let properties_length = properties_bytes.len();
    let sys_flag = message_ext.sys_flag;
    let born_host_length = if (sys_flag & MessageSysFlag::BORNHOST_V6_FLAG) == 0 {
        8
    } else {
        20
    };
    let store_host_address_length = if (sys_flag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG) == 0 {
        8
    } else {
        20
    };
    let new_body = if need_compress && (sys_flag & MessageSysFlag::COMPRESSED_FLAG) == MessageSysFlag::COMPRESSED_FLAG {
        let compressor = CompressorFactory::get_compressor(MessageSysFlag::try_get_compression_type(sys_flag)?);
        let compressed_body = compressor.compress(body, 5)?;
        Some(compressed_body)
    } else {
        None
    };
    let body_len = new_body.as_ref().map_or(body.len(), |b| b.len());
    let store_size = if message_ext.store_size > 0 {
        message_ext.store_size as usize
    } else {
        4 // 1 TOTALSIZE
             + 4 // 2 MAGICCODE
             + 4 // 3 BODYCRC
             + 4 // 4 QUEUEID
             + 4 // 5 FLAG
             + 8 // 6 QUEUEOFFSET
             + 8 // 7 PHYSICALOFFSET
             + 4 // 8 SYSFLAG
             + 8 // 9 BORNTIMESTAMP
             + born_host_length // 10 BORNHOST
             + 8 // 11 STORETIMESTAMP
             + store_host_address_length // 12 STOREHOSTADDRESS
             + 4 // 13 RECONSUMETIMES
             + 8 // 14 Prepared Transaction Offset
             + 4 + body_len // 14 BODY
             + 1 + topic_len // 15 TOPIC
             + 2 + properties_length // 16 propertiesLength
    };
    let mut byte_buffer = BytesMut::with_capacity(store_size);

    // 1 TOTALSIZE
    byte_buffer.put_i32(store_size as i32);

    // 2 MAGICCODE
    byte_buffer.put_i32(MESSAGE_MAGIC_CODE);

    // 3 BODYCRC
    byte_buffer.put_u32(message_ext.body_crc);

    // 4 QUEUEID
    byte_buffer.put_i32(message_ext.queue_id);

    // 5 FLAG
    byte_buffer.put_i32(message_ext.message.flag());

    // 6 QUEUEOFFSET
    byte_buffer.put_i64(message_ext.queue_offset);

    // 7 PHYSICALOFFSET
    byte_buffer.put_i64(message_ext.commit_log_offset);

    // 8 SYSFLAG
    byte_buffer.put_i32(message_ext.sys_flag);

    // 9 BORNTIMESTAMP
    byte_buffer.put_i64(message_ext.born_timestamp);

    // 10 BORNHOST

    let born_host = message_ext.born_host;
    match born_host {
        SocketAddr::V4(value) => byte_buffer.extend(value.ip().octets()),
        SocketAddr::V6(value) => byte_buffer.extend(value.ip().octets()),
    };

    byte_buffer.put_i32(born_host.port() as i32);

    // 11 STORETIMESTAMP
    byte_buffer.put_i64(message_ext.store_timestamp);

    // 12 STOREHOST

    let store_host = message_ext.store_host;
    match store_host {
        SocketAddr::V4(value) => byte_buffer.extend(value.ip().octets()),
        SocketAddr::V6(value) => byte_buffer.extend(value.ip().octets()),
    };

    byte_buffer.put_i32(store_host.port() as i32);

    // 13 RECONSUMETIMES
    byte_buffer.put_i32(message_ext.reconsume_times);

    // 14 Prepared Transaction Offset
    byte_buffer.put_i64(message_ext.prepared_transaction_offset);

    // 15 BODY
    byte_buffer.put_i32(body_len as i32);
    if let Some(new_body) = new_body {
        byte_buffer.put_slice(&new_body);
    } else {
        byte_buffer.put_slice(body);
    }

    // 16 TOPIC
    byte_buffer.put_u8(topic_len as u8);
    byte_buffer.put_slice(topic);

    // 17 properties
    byte_buffer.put_i16(properties_length as i16);
    byte_buffer.put_slice(properties_bytes);

    Ok(byte_buffer.freeze())
}

pub fn encode_uniquely(message_ext: &MessageExt, need_compress: bool) -> rocketmq_error::RocketMQResult<Bytes> {
    let body = message_ext
        .get_body()
        .ok_or_else(|| rocketmq_error::RocketMQError::illegal_argument("message body is required"))?;
    let topics = message_ext.topic().as_bytes();
    let topic_len = topics.len();
    let properties = message_properties_to_string(message_ext.get_properties());
    let properties_bytes = properties.as_bytes();
    let properties_length = properties_bytes.len();
    let sys_flag = message_ext.sys_flag;
    let born_host_length = if (sys_flag & MessageSysFlag::BORNHOST_V6_FLAG) == 0 {
        8
    } else {
        20
    };
    let new_body = if need_compress && (sys_flag & MessageSysFlag::COMPRESSED_FLAG) == MessageSysFlag::COMPRESSED_FLAG {
        let compressor = CompressorFactory::get_compressor(MessageSysFlag::try_get_compression_type(sys_flag)?);
        let compressed_body = compressor.compress(body, 5)?;
        Some(compressed_body)
    } else {
        None
    };
    let body_len = new_body.as_ref().map_or(body.len(), |b| b.len());
    let store_size = if message_ext.store_size > 0 {
        message_ext.store_size as usize
    } else {
        4 // 1 TOTALSIZE
             + 4 // 2 MAGICCODE
             + 4 // 3 BODYCRC
             + 4 // 4 QUEUEID
             + 4 // 5 FLAG
             + 8 // 6 QUEUEOFFSET
             + 8 // 7 PHYSICALOFFSET
             + 4 // 8 SYSFLAG
             + 8 // 9 BORNTIMESTAMP
             + born_host_length // 10 BORNHOST
             + 4 // 11 RECONSUMETIMES
             + 8 // 12 Prepared Transaction Offset
             + 4 + body_len // 13 BODY
             + 1 + topic_len // 14 TOPIC
             + 2 + properties_length // 15 propertiesLength
    };
    let capacity = if message_ext.store_size > 0 {
        store_size.saturating_sub(8)
    } else {
        store_size
    };
    let mut byte_buffer = BytesMut::with_capacity(capacity);

    // 1 TOTALSIZE
    byte_buffer.put_i32(store_size as i32);

    // 2 MAGICCODE
    byte_buffer.put_i32(MESSAGE_MAGIC_CODE);

    // 3 BODYCRC
    byte_buffer.put_u32(message_ext.body_crc);

    // 4 QUEUEID
    byte_buffer.put_i32(message_ext.queue_id);

    // 5 FLAG
    byte_buffer.put_i32(message_ext.message.flag());

    // 6 QUEUEOFFSET
    byte_buffer.put_i64(message_ext.queue_offset);

    // 7 PHYSICALOFFSET
    byte_buffer.put_i64(message_ext.commit_log_offset);

    // 8 SYSFLAG
    byte_buffer.put_i32(message_ext.sys_flag);

    // 9 BORNTIMESTAMP
    byte_buffer.put_i64(message_ext.born_timestamp);

    // 10 BORNHOST

    let born_host = message_ext.born_host;
    match born_host {
        SocketAddr::V4(value) => byte_buffer.extend(value.ip().octets()),
        SocketAddr::V6(value) => byte_buffer.extend(value.ip().octets()),
    };
    byte_buffer.put_i32(born_host.port() as i32);

    // 11 RECONSUMETIMES
    byte_buffer.put_i32(message_ext.reconsume_times);

    // 12 Prepared Transaction Offset
    byte_buffer.put_i64(message_ext.prepared_transaction_offset);

    // 13 BODY
    byte_buffer.put_i32(body_len as i32);
    if let Some(new_body) = new_body {
        byte_buffer.put_slice(&new_body);
    } else {
        byte_buffer.put_slice(body);
    }

    // 14 TOPIC
    byte_buffer.put_i16(topic_len as i16);
    byte_buffer.put_slice(topics);

    // 15 properties
    byte_buffer.put_i16(properties_length as i16);
    byte_buffer.put_slice(properties_bytes);

    Ok(byte_buffer.freeze())
}

pub fn create_crc32(mut input: &mut [u8], crc32: u32) {
    input.put(MessageConst::PROPERTY_CRC32.as_bytes());
    input.put_u8(NAME_VALUE_SEPARATOR as u8);
    let mut crc32 = crc32;
    for _ in 0..10 {
        let mut b = b'0';
        if crc32 > 0 {
            b += (crc32 % 10) as u8;
            crc32 /= 10;
        }
        input.put_u8(b);
    }
    input.put_u8(PROPERTY_SEPARATOR as u8);
}

pub fn decode_properties(bytes: &mut Bytes) -> Option<HashMap<CheetahString, CheetahString>> {
    // Ensure we have enough bytes to read SYSFLAG and MAGICCODE.
    if bytes.len() < SYSFLAG_POSITION + 4 {
        return None;
    }

    // Read sysFlag and magicCode using fixed positions.
    let sys_flag = BigEndian::read_i32(&bytes[SYSFLAG_POSITION..SYSFLAG_POSITION + 4]);
    let magic_code = BigEndian::read_i32(&bytes[MESSAGE_MAGIC_CODE_POSITION..MESSAGE_MAGIC_CODE_POSITION + 4]);
    let version = match MessageVersion::value_of_magic_code(magic_code) {
        Ok(value) => value,
        Err(_) => return None,
    };

    // Determine address lengths.
    let bornhost_length = if (sys_flag & MessageSysFlag::BORNHOST_V6_FLAG) == 0 {
        8
    } else {
        20
    };
    let storehost_address_length = if (sys_flag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG) == 0 {
        8
    } else {
        20
    };

    // Calculate the bodySizePosition as in Java.
    let body_size_position = 4   // TOTALSIZE
        + 4   // MAGICCODE
        + 4   // BODYCRC
        + 4   // QUEUEID
        + 4   // FLAG
        + 8   // QUEUEOFFSET
        + 8   // PHYSICALOFFSET
        + 4   // SYSFLAG
        + 8   // BORNTIMESTAMP
        + bornhost_length // BORNHOST
        + 8   // STORETIMESTAMP
        + storehost_address_length // STOREHOSTADDRESS
        + 4   // RECONSUMETIMES
        + 8; // Prepared Transaction Offset

    if bytes.len() < body_size_position + 4 {
        return None;
    }

    // Read the body size stored as an int.
    let body_size = BigEndian::read_i32(&bytes[body_size_position..body_size_position + 4]) as usize;

    // Compute the topic length position.
    let topic_length_position = body_size_position + 4 + body_size;
    if bytes.len() < topic_length_position {
        return None;
    }

    // Create a Cursor over the slice starting at the topic length position.
    let slice = &bytes[topic_length_position..];
    let cursor = Cursor::new(slice);
    let topic_length_size = version.get_topic_length_size();
    bytes.advance(topic_length_position);
    let topic_length = version.get_topic_length(bytes);

    // Calculate the properties position.
    let properties_position = topic_length_position + topic_length_size + topic_length;
    if bytes.len() < properties_position + 2 {
        return None;
    }

    // Read a short (2 bytes) as propertiesLength.
    let properties_length = BigEndian::read_i16(&bytes[properties_position..properties_position + 2]);

    // Advance past the short value.
    let properties_start = properties_position + 2;
    if properties_length > 0 {
        let end = properties_start + (properties_length as usize);
        if bytes.len() < end {
            return None;
        }
        let properties_bytes = &bytes[properties_start..end];
        if let Ok(properties_string) = String::from_utf8(properties_bytes.to_vec()) {
            Some(string_to_message_properties(Some(&CheetahString::from_string(
                properties_string,
            ))))
        } else {
            None
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn message_properties_to_string_preserves_separator_format() {
        let mut properties = HashMap::new();
        properties.insert(
            CheetahString::from_static_str("key"),
            CheetahString::from_static_str("value"),
        );

        let encoded = message_properties_to_string(&properties);

        assert_eq!(encoded, "key\u{0001}value\u{0002}");
    }

    #[test]
    fn cheetah_from_utf8_lossy_borrows_valid_utf8() {
        let encoded = b"key\x01value\x02";

        let decoded = cheetah_from_utf8_lossy(encoded);

        assert_eq!(decoded, "key\u{0001}value\u{0002}");
    }

    #[test]
    fn cheetah_from_utf8_lossy_preserves_lossy_invalid_utf8_behavior() {
        let encoded = b"key\x01\xff\x02";

        let decoded = cheetah_from_utf8_lossy(encoded);

        assert_eq!(decoded.as_str(), String::from_utf8_lossy(encoded).as_ref());
    }

    #[test]
    fn count_inner_msg_num_counts_correctly_for_multiple_messages() {
        let mut bytes = BytesMut::new();
        bytes.put_i32(8);
        bytes.put_slice(&[0, 0, 0, 0]);
        bytes.put_i32(8);
        bytes.put_slice(&[0, 0, 0, 0]);
        assert_eq!(count_inner_msg_num(Some(bytes.freeze())), 2);
    }

    #[test]
    fn count_inner_msg_num_counts_correctly_for_single_message() {
        let mut bytes = BytesMut::new();
        bytes.put_i32(8);
        bytes.put_slice(&[0, 0, 0, 0]);
        assert_eq!(count_inner_msg_num(Some(bytes.freeze())), 1);
    }

    #[test]
    fn count_inner_msg_num_counts_zero_for_no_messages() {
        let bytes = BytesMut::new();
        assert_eq!(count_inner_msg_num(Some(bytes.freeze())), 0);
    }

    #[test]
    fn count_inner_msg_num_ignores_incomplete_messages() {
        let mut bytes = BytesMut::new();
        bytes.put_i32(4);
        assert_eq!(count_inner_msg_num(Some(bytes.freeze())), 1);
    }

    #[test]
    fn decode_message_id_ipv4() {
        let msg_id = "7F0000010007D8260BF075769D36C348";
        let message_id = decode_message_id(msg_id).unwrap();
        assert_eq!(message_id.address, "127.0.0.1:55334".parse().unwrap());
        assert_eq!(message_id.offset, 860316681131967304);
    }

    #[test]
    fn encode_with_compression() {
        let mut message_ext = MessageExt::default();
        message_ext.set_body(Bytes::from("Hello, World!"));
        let result = encode(&message_ext, true);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn encode_with_unknown_compression_type_returns_error() {
        let mut message_ext = MessageExt::default();
        message_ext.set_body(Bytes::from_static(b"Hello, World!"));
        message_ext.set_sys_flag(MessageSysFlag::COMPRESSED_FLAG | (0x7 << 8));

        let result = encode(&message_ext, true);

        assert!(result.is_err());
    }

    #[test]
    fn decode_compressed_body_uses_sys_flag_compression_type() {
        let body = Bytes::from_static(b"Hello from compressed RocketMQ body");

        for compression_flag in [
            MessageSysFlag::COMPRESSION_LZ4_TYPE,
            MessageSysFlag::COMPRESSION_ZSTD_TYPE,
            MessageSysFlag::COMPRESSION_ZLIB_TYPE,
        ] {
            let mut message_ext = MessageExt::default();
            message_ext.set_body(body.clone());
            message_ext.set_sys_flag(MessageSysFlag::COMPRESSED_FLAG | compression_flag);
            let mut encoded = encode(&message_ext, true).expect("compressed message should encode");

            let decoded =
                decode(&mut encoded, true, true, false, false, false).expect("compressed message should decode");

            assert_eq!(decoded.get_body(), Some(&body));
        }
    }

    #[test]
    fn encode_without_compression() {
        let mut message_ext = MessageExt::default();
        message_ext.set_body(Bytes::from("Hello, World!"));
        let result = encode(&message_ext, false);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn encode_with_empty_body() {
        let mut message_ext = MessageExt::default();
        message_ext.set_body(Bytes::new());
        let result = encode(&message_ext, false);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn encode_with_large_body() {
        let mut message_ext = MessageExt::default();
        let large_body = vec![0u8; 1024 * 1024];
        message_ext.set_body(Bytes::from(large_body));
        let result = encode(&message_ext, false);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn encode_uniquely_with_compression() {
        let mut message_ext = MessageExt::default();
        message_ext.set_body(Bytes::from("Hello, World!"));
        let result = encode_uniquely(&message_ext, true);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn encode_uniquely_without_compression() {
        let mut message_ext = MessageExt::default();
        message_ext.set_body(Bytes::from("Hello, World!"));
        let result = encode_uniquely(&message_ext, false);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn encode_uniquely_with_empty_body() {
        let mut message_ext = MessageExt::default();
        message_ext.set_body(Bytes::new());
        let result = encode_uniquely(&message_ext, false);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn encode_uniquely_with_large_body() {
        let mut message_ext = MessageExt::default();
        let large_body = vec![0u8; 1024 * 1024];
        message_ext.set_body(Bytes::from(large_body));
        let result = encode_uniquely(&message_ext, false);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn decode_properties_returns_none_if_bytes_length_is_insufficient() {
        let mut bytes = Bytes::from(vec![0; SYSFLAG_POSITION + 3]);
        assert!(decode_properties(&mut bytes).is_none());
    }

    #[test]
    fn decode_properties_returns_none_if_magic_code_is_invalid() {
        let mut bytes = BytesMut::from_iter(vec![0u8; SYSFLAG_POSITION + 4 + MESSAGE_MAGIC_CODE_POSITION + 4]);
        BigEndian::write_i32(&mut bytes[SYSFLAG_POSITION..SYSFLAG_POSITION + 4], 0);
        BigEndian::write_i32(
            &mut bytes[MESSAGE_MAGIC_CODE_POSITION..MESSAGE_MAGIC_CODE_POSITION + 4],
            -1,
        );
        assert!(decode_properties(&mut bytes.freeze()).is_none());
    }

    #[test]
    fn decode_properties_returns_none_if_body_size_is_insufficient() {
        let mut bytes = BytesMut::from_iter(vec![0u8; SYSFLAG_POSITION + 4 + MESSAGE_MAGIC_CODE_POSITION + 4 + 4]);
        BigEndian::write_i32(&mut bytes[SYSFLAG_POSITION..SYSFLAG_POSITION + 4], 0);
        BigEndian::write_i32(
            &mut bytes[MESSAGE_MAGIC_CODE_POSITION..MESSAGE_MAGIC_CODE_POSITION + 4],
            MESSAGE_MAGIC_CODE,
        );
        assert!(decode_properties(&mut bytes.freeze()).is_none());
    }

    #[test]
    fn decode_properties_returns_none_if_topic_length_is_insufficient() {
        let mut bytes = BytesMut::from_iter(vec![
            0u8;
            SYSFLAG_POSITION + 4 + MESSAGE_MAGIC_CODE_POSITION + 4 + 4 + 4
        ]);
        BigEndian::write_i32(&mut bytes[SYSFLAG_POSITION..SYSFLAG_POSITION + 4], 0);
        BigEndian::write_i32(
            &mut bytes[MESSAGE_MAGIC_CODE_POSITION..MESSAGE_MAGIC_CODE_POSITION + 4],
            MESSAGE_MAGIC_CODE,
        );
        BigEndian::write_i32(
            &mut bytes[SYSFLAG_POSITION + 4 + MESSAGE_MAGIC_CODE_POSITION + 4
                ..SYSFLAG_POSITION + 4 + MESSAGE_MAGIC_CODE_POSITION + 4 + 4],
            0,
        );
        assert!(decode_properties(&mut bytes.freeze()).is_none());
    }

    #[test]
    fn decode_properties_returns_none_if_properties_length_is_insufficient() {
        let mut bytes = BytesMut::from_iter(vec![
            0u8;
            SYSFLAG_POSITION + 4 + MESSAGE_MAGIC_CODE_POSITION + 4 + 4 + 4 + 2
        ]);
        BigEndian::write_i32(&mut bytes[SYSFLAG_POSITION..SYSFLAG_POSITION + 4], 0);
        BigEndian::write_i32(
            &mut bytes[MESSAGE_MAGIC_CODE_POSITION..MESSAGE_MAGIC_CODE_POSITION + 4],
            MESSAGE_MAGIC_CODE,
        );
        BigEndian::write_i32(
            &mut bytes[SYSFLAG_POSITION + 4 + MESSAGE_MAGIC_CODE_POSITION + 4
                ..SYSFLAG_POSITION + 4 + MESSAGE_MAGIC_CODE_POSITION + 4 + 4],
            0,
        );
        BigEndian::write_i16(
            &mut bytes[SYSFLAG_POSITION + 4 + MESSAGE_MAGIC_CODE_POSITION + 4 + 4 + 4
                ..SYSFLAG_POSITION + 4 + MESSAGE_MAGIC_CODE_POSITION + 4 + 4 + 4 + 2],
            1,
        );
        assert!(decode_properties(&mut bytes.freeze()).is_none());
    }

    #[test]
    fn validate_message_id_ipv4_32_chars() {
        let result = validate_message_id("AC11000100002A9F0000000000000001");
        assert!(result.is_ok());
    }

    #[test]
    fn validate_message_id_ipv6_56_chars() {
        let result = validate_message_id("20010db800000000000000000000000100002A9F0000000000000001");
        assert!(result.is_ok());
    }

    #[test]
    fn validate_message_id_ipv6_40_chars_rejected() {
        let result = validate_message_id("20010db800000000000000000000000100000001");
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.contains("Invalid message ID length"));
            assert!(e.contains("56 characters (IPv6)"));
        }
    }

    #[test]
    fn decode_message_id_ipv6() {
        let msg_id = "20010db800000000000000000000000100002A9F0000000000000001";
        let message_id = decode_message_id(msg_id).unwrap();
        assert_eq!(message_id.address, "[2001:db8::1]:10911".parse().unwrap());
        assert_eq!(message_id.offset, 1);
    }

    #[test]
    fn decode_message_id_ipv6_full_address() {
        let msg_id = "20010db81234567800000000abcdef0100002A9F0000000000001234";
        let message_id = decode_message_id(msg_id).unwrap();
        assert_eq!(
            message_id.address,
            "[2001:db8:1234:5678::abcd:ef01]:10911".parse().unwrap()
        );
        assert_eq!(message_id.offset, 4660);
    }
}
