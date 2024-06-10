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
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::net::SocketAddrV6;
use std::str;

use bytes::Buf;
use bytes::Bytes;

use crate::common::compression::compression_type::CompressionType;
use crate::common::message::message_single::MessageExt;
use crate::common::message::MessageVersion;
use crate::common::sys_flag::message_sys_flag::MessageSysFlag;
use crate::CRC32Utils::crc32;
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

pub fn string_to_message_properties(properties: Option<&String>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Some(properties) = properties {
        let mut index = 0;
        let len = properties.len();
        while index < len {
            let new_index = properties[index..]
                .find(PROPERTY_SEPARATOR)
                .map_or(len, |i| index + i);
            if new_index - index >= 3 {
                if let Some(kv_sep_index) = properties[index..new_index].find(NAME_VALUE_SEPARATOR)
                {
                    let kv_sep_index = index + kv_sep_index;
                    if kv_sep_index > index && kv_sep_index < new_index - 1 {
                        let k = &properties[index..kv_sep_index];
                        let v = &properties[kv_sep_index + 1..new_index];
                        map.insert(k.to_string(), v.to_string());
                    }
                }
            }
            index = new_index + 1;
        }
    }
    map
}

pub fn message_properties_to_string(properties: &HashMap<String, String>) -> String {
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
    sb
}

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
    let (born_host_address, born_host_ip_length) =
        if sys_flag & MessageSysFlag::BORNHOST_V6_FLAG != 0 {
            let mut born_host = [0; 16];
            byte_buffer.copy_to_slice(&mut born_host);
            let port = byte_buffer.get_i32();
            (
                SocketAddr::V6(SocketAddrV6::new(
                    Ipv6Addr::from(born_host),
                    port as u16,
                    0,
                    0,
                )),
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
    let (store_host_address, store_host_ip_length) =
        if sys_flag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG != 0 {
            let mut store_host = [0; 16];
            byte_buffer.copy_to_slice(&mut store_host);
            let port = byte_buffer.get_i32();
            (
                SocketAddr::V6(SocketAddrV6::new(
                    Ipv6Addr::from(store_host),
                    port as u16,
                    0,
                    0,
                )),
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
            if de_compress_body
                && (sys_flag & MessageSysFlag::COMPRESSED_FLAG) == MessageSysFlag::COMPRESSED_FLAG
            {
                let compression_type = CompressionType::find_by_value(
                    (flag & MessageSysFlag::COMPRESSION_TYPE_COMPARATOR) >> 8,
                );
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
    msg_ext.message.topic = topic_str.to_string();

    // 17 properties
    let properties_length = byte_buffer.get_i16();
    if properties_length > 0 {
        // Handle reading and processing properties
        let mut properties = vec![0; properties_length as usize];
        byte_buffer.copy_to_slice(&mut properties);
        if !is_set_properties_string {
            let properties_string = String::from_utf8_lossy(properties.as_slice()).to_string();
            let message_properties = string_to_message_properties(Some(&properties_string));
            msg_ext.message.properties = message_properties;
        } else {
            let properties_string = String::from_utf8_lossy(properties.as_slice()).to_string();
            let mut message_properties = string_to_message_properties(Some(&properties_string));
            message_properties.insert("propertiesString".to_string(), properties_string);
            msg_ext.message.properties = message_properties;
        }
    }
    let msg_id = build_message_id(store_host_address, physic_offset);
    msg_ext.set_msg_id(msg_id);

    if is_client {
        unimplemented!()
    }

    Some(msg_ext)
}
