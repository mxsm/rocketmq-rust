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
use cheetah_string::CheetahString;

use crate::common::compression::compression_type::CompressionType;
use crate::common::compression::compressor_factory::CompressorFactory;
use crate::common::message::message_client_ext::MessageClientExt;
use crate::common::message::message_ext::MessageExt;
use crate::common::message::message_id::MessageId;
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

    let mut sb = String::with_capacity(len);
    for (name, value) in properties.iter() {
        sb.push_str(name);
        sb.push(NAME_VALUE_SEPARATOR);

        sb.push_str(value);
        sb.push(PROPERTY_SEPARATOR);
    }
    CheetahString::from_string(sb)
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
    let mut msg_ext = if is_client {
        unimplemented!()
    } else {
        MessageExt::default()
    };

    // 1 TOTALSIZE
    let store_size = byte_buffer.get_i32();
    msg_ext.set_store_size(store_size);

    // 2 MAGICCODE
    let magic_code = byte_buffer.get_i32();
    let version = MessageVersion::value_of_magic_code(magic_code).unwrap();

    // 3 BODYCRC
    let body_crc = byte_buffer.get_u32();
    msg_ext.set_body_crc(body_crc);

    // 4 QUEUEID
    let queue_id = byte_buffer.get_i32();
    msg_ext.set_queue_id(queue_id);

    // 5 FLAG
    let flag = byte_buffer.get_i32();
    msg_ext.message.flag = flag;

    // 6 QUEUEOFFSET
    let queue_offset = byte_buffer.get_i64();
    msg_ext.set_queue_offset(queue_offset);

    // 7 PHYSICALOFFSET
    let physic_offset = byte_buffer.get_i64();
    msg_ext.set_commit_log_offset(physic_offset);

    // 8 SYSFLAG
    let sys_flag = byte_buffer.get_i32();
    msg_ext.set_sys_flag(sys_flag);

    // 9 BORNTIMESTAMP
    let born_time_stamp = byte_buffer.get_i64();
    msg_ext.set_born_timestamp(born_time_stamp);

    // 10 BORNHOST
    let (born_host_address, born_host_ip_length) = if sys_flag & MessageSysFlag::BORNHOST_V6_FLAG != 0 {
        let mut born_host = [0; 16];
        byte_buffer.copy_to_slice(&mut born_host);
        let port = byte_buffer.get_i32();
        (
            SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::from(born_host), port as u16, 0, 0)),
            16,
        )
    } else {
        let mut born_host = [0; 4];
        byte_buffer.copy_to_slice(&mut born_host);
        let port = byte_buffer.get_i32();
        (
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from(born_host), port as u16)),
            4,
        )
    };
    msg_ext.set_born_host(born_host_address);

    // 11 STORETIMESTAMP
    let store_timestamp = byte_buffer.get_i64();
    msg_ext.set_store_timestamp(store_timestamp);

    // 12 STOREHOST
    let (store_host_address, store_host_ip_length) = if sys_flag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG != 0 {
        let mut store_host = [0; 16];
        byte_buffer.copy_to_slice(&mut store_host);
        let port = byte_buffer.get_i32();
        (
            SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::from(store_host), port as u16, 0, 0)),
            16,
        )
    } else {
        let mut store_host = [0; 4];
        byte_buffer.copy_to_slice(&mut store_host);
        let port = byte_buffer.get_i32();
        (
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from(store_host), port as u16)),
            4,
        )
    };
    msg_ext.set_store_host(store_host_address);

    // 13 RECONSUMETIMES
    let reconsume_times = byte_buffer.get_i32();
    msg_ext.set_reconsume_times(reconsume_times);

    // 14 Prepared Transaction Offset
    let prepared_transaction_offset = byte_buffer.get_i64();
    msg_ext.set_prepared_transaction_offset(prepared_transaction_offset);

    // 15 BODY
    let body_len = byte_buffer.get_i32();
    if body_len > 0 {
        // Handle reading and processing body
        if read_body {
            let mut body = vec![0; body_len as usize];
            byte_buffer.copy_to_slice(&mut body);
            if check_crc {
                let crc = crc32(&body);
                if crc != body_crc {
                    return None;
                }
            }
            let mut body_bytes = Bytes::from(body);
            if de_compress_body && (sys_flag & MessageSysFlag::COMPRESSED_FLAG) == MessageSysFlag::COMPRESSED_FLAG {
                let compression_type =
                    CompressionType::find_by_value((flag & MessageSysFlag::COMPRESSION_TYPE_COMPARATOR) >> 8);
                body_bytes = compression_type.decompression(&body_bytes)
            }
            msg_ext.message.body = Some(body_bytes);
        } else {
            let _ = byte_buffer.split_to(
                BORN_TIMESTAMP_POSITION
                    + born_host_ip_length
                    + 4
                    + 8
                    + store_host_ip_length
                    + 4
                    + 4
                    + 8
                    + 4
                    + body_len as usize,
            );
        }
    }

    // 16 TOPIC
    let topic_len = version.get_topic_length(byte_buffer);
    let mut topic = vec![0; topic_len];
    byte_buffer.copy_to_slice(&mut topic);
    let topic_str = str::from_utf8(&topic).unwrap();
    msg_ext.message.topic = CheetahString::from_slice(topic_str);

    // 17 properties
    let properties_length = byte_buffer.get_i16();
    if properties_length > 0 {
        // Handle reading and processing properties
        let mut properties = vec![0; properties_length as usize];
        byte_buffer.copy_to_slice(&mut properties);
        if !is_set_properties_string {
            //can optimize later
            let properties_string =
                CheetahString::from_string(String::from_utf8_lossy(properties.as_slice()).to_string());
            let message_properties = string_to_message_properties(Some(&properties_string));
            msg_ext.message.properties = message_properties;
        } else {
            let properties_string =
                CheetahString::from_string(String::from_utf8_lossy(properties.as_slice()).to_string());
            let mut message_properties = string_to_message_properties(Some(&properties_string));
            message_properties.insert(CheetahString::from_static_str("propertiesString"), properties_string);
            msg_ext.message.properties = message_properties;
        }
    }
    let msg_id = build_message_id(store_host_address, physic_offset);
    msg_ext.set_msg_id(CheetahString::from_string(msg_id));

    if is_client {
        unimplemented!()
    }

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
    let body = message.body.as_ref().unwrap();
    let body_len = body.len();
    let properties = message_properties_to_string(&message.properties);
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
    bytes.put_i32(message.flag);

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
    let messages = decode_messages(message_ext.message.body.as_mut().unwrap());
    for message in messages {
        let mut message_ext_inner = MessageExt {
            message,
            ..MessageExt::default()
        };
        message_ext_inner.set_topic(message_ext.get_topic().to_owned());
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
        let message = decode_message(buffer);
        messages.push(message);
    }
    messages
}

pub fn decode_message(buffer: &mut Bytes) -> Message {
    // 1 TOTALSIZE
    let _ = buffer.get_i32();

    // 2 MAGICCODE
    let _ = buffer.get_i32();

    // 3 BODYCRC
    let _ = buffer.get_i32();

    // 4 FLAG
    let flag = buffer.get_i32();

    // 5 BODY
    let body_len = buffer.get_i32();
    let body = buffer.split_to(body_len as usize);

    // 6 properties
    let properties_length = buffer.get_i16();
    let properties = buffer.split_to(properties_length as usize);
    //string_to_message_properties(Some(&String::from_utf8_lossy(properties.as_ref()).
    // to_string()));
    let message_properties = str_to_message_properties(Some(str::from_utf8(&properties).unwrap()));

    Message {
        body: Some(body),
        properties: message_properties,
        flag,
        ..Message::default()
    }
}

pub fn decode_message_id(msg_id: &str) -> MessageId {
    let bytes = util_all::string_to_bytes(msg_id).unwrap();
    let mut buffer = Bytes::from(bytes);
    let len = if msg_id.len() == 32 {
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
    MessageId {
        address: len,
        offset: buffer.get_i64(),
    }
}

pub fn encode(message_ext: &MessageExt, need_compress: bool) -> rocketmq_error::RocketMQResult<Bytes> {
    let body = message_ext.get_body().unwrap();
    let topic = message_ext.get_topic().as_bytes();
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
        let compressor = CompressorFactory::get_compressor(MessageSysFlag::get_compression_type(sys_flag));
        let compressed_body = compressor.compress(body, 5)?;
        Some(compressed_body)
    } else {
        None
    };
    let body_len = new_body.as_ref().map_or(body.len(), |b| b.len());
    let store_size = message_ext.store_size;
    let mut byte_buffer = if store_size > 0 {
        BytesMut::with_capacity(store_size as usize)
    } else {
        let store_size = 4 // 1 TOTALSIZE
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
             + 2 + properties_length; // 16 propertiesLength
        BytesMut::with_capacity(store_size)
    };

    // 1 TOTALSIZE
    byte_buffer.put_i32(store_size);

    // 2 MAGICCODE
    byte_buffer.put_i32(MESSAGE_MAGIC_CODE);

    // 3 BODYCRC
    byte_buffer.put_u32(message_ext.body_crc);

    // 4 QUEUEID
    byte_buffer.put_i32(message_ext.queue_id);

    // 5 FLAG
    byte_buffer.put_i32(message_ext.message.flag);

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
    let body = message_ext.get_body().unwrap();
    let topics = message_ext.get_topic().as_bytes();
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
        let compressor = CompressorFactory::get_compressor(MessageSysFlag::get_compression_type(sys_flag));
        let compressed_body = compressor.compress(body, 5)?;
        Some(compressed_body)
    } else {
        None
    };
    let body_len = new_body.as_ref().map_or(body.len(), |b| b.len());
    let store_size = message_ext.store_size;
    let mut byte_buffer = if store_size > 0 {
        BytesMut::with_capacity((store_size - 8) as usize)
    } else {
        let store_size = 4 // 1 TOTALSIZE
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
             + 2 + properties_length; // 15 propertiesLength
        BytesMut::with_capacity(store_size)
    };

    // 1 TOTALSIZE
    byte_buffer.put_i32(store_size);

    // 2 MAGICCODE
    byte_buffer.put_i32(MESSAGE_MAGIC_CODE);

    // 3 BODYCRC
    byte_buffer.put_u32(message_ext.body_crc);

    // 4 QUEUEID
    byte_buffer.put_i32(message_ext.queue_id);

    // 5 FLAG
    byte_buffer.put_i32(message_ext.message.flag);

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
        let message_id = decode_message_id(msg_id);
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
}
