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
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::Hash;
use std::hash::Hasher;
use std::net::SocketAddr;

use bytes::BufMut;

use crate::common::message::message_ext::MessageExt;
use crate::common::message::MessageConst;
use crate::MessageDecoder::PROPERTY_SEPARATOR;
use crate::UtilAll::bytes_to_string;

pub fn get_sharding_key_index(sharding_key: &str, index_size: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    sharding_key.hash(&mut hasher);
    let hash = hasher.finish() as usize;
    hash % index_size
}

pub fn get_sharding_key_index_by_msg(msg: &MessageExt, index_size: usize) -> usize {
    let sharding_key = msg
        .message
        .properties
        .get(MessageConst::PROPERTY_SHARDING_KEY)
        .cloned()
        .unwrap_or_default();

    get_sharding_key_index(sharding_key.as_str(), index_size)
}

pub fn get_sharding_key_indexes(msgs: &[MessageExt], index_size: usize) -> HashSet<usize> {
    let mut index_set = HashSet::with_capacity(index_size);
    for msg in msgs {
        let idx = get_sharding_key_index_by_msg(msg, index_size);
        index_set.insert(idx);
    }
    index_set
}

//need refactor
pub fn delete_property(properties_string: &str, name: &str) -> String {
    if properties_string.is_empty() {
        return properties_string.to_owned();
    }
    let index1 = properties_string.find(name);
    if index1.is_none() {
        return properties_string.to_owned();
    }
    properties_string
        .split(PROPERTY_SEPARATOR)
        .map(|s| s.to_owned())
        .filter(|s| s.starts_with(name))
        .collect::<Vec<String>>()
        .join(PROPERTY_SEPARATOR.to_string().as_str())
}

pub fn build_message_id(socket_addr: SocketAddr, wrote_offset: i64) -> String {
    let mut msg_id_vec = match socket_addr {
        SocketAddr::V4(addr) => {
            let mut msg_id_vec = Vec::<u8>::with_capacity(16);
            msg_id_vec.extend_from_slice(&addr.ip().octets());
            msg_id_vec.put_i32(addr.port() as i32);
            msg_id_vec
        }
        SocketAddr::V6(addr) => {
            let mut msg_id_vec = Vec::<u8>::with_capacity(28);
            msg_id_vec.extend_from_slice(&addr.ip().octets());
            msg_id_vec.put_i32(addr.port() as i32);
            msg_id_vec
        }
    };
    msg_id_vec.put_i64(wrote_offset);
    bytes_to_string(msg_id_vec.as_slice())
}

pub fn socket_address_to_vec(socket_addr: SocketAddr) -> Vec<u8> {
    match socket_addr {
        SocketAddr::V4(addr) => {
            let mut msg_id_vec = Vec::<u8>::with_capacity(8);
            msg_id_vec.extend_from_slice(&addr.ip().octets());
            msg_id_vec.put_i32(addr.port() as i32);
            msg_id_vec
        }
        SocketAddr::V6(addr) => {
            let mut msg_id_vec = Vec::<u8>::with_capacity(20);
            msg_id_vec.extend_from_slice(&addr.ip().octets());
            msg_id_vec.put_i32(addr.port() as i32);
            msg_id_vec
        }
    }
}

pub fn build_batch_message_id(
    socket_addr: SocketAddr,
    store_host_length: i32,
    batch_size: usize,
    phy_pos: &[i64],
) -> String {
    if batch_size == 0 {
        return String::new();
    }
    let msg_id_len = (store_host_length + 8) as usize;
    let mut msg_id_vec = vec![0u8; msg_id_len];
    msg_id_vec[..store_host_length as usize].copy_from_slice(&socket_address_to_vec(socket_addr));
    let mut message_id = String::with_capacity(batch_size * msg_id_len * 2 + batch_size - 1);
    for (index, value) in phy_pos.iter().enumerate() {
        msg_id_vec[msg_id_len - 8..msg_id_len].copy_from_slice(&value.to_be_bytes());
        if index != 0 {
            message_id.push(',');
        }
        message_id.push_str(&bytes_to_string(&msg_id_vec));
    }
    message_id
}

pub fn parse_message_id(_msg_id: impl Into<String>) -> (SocketAddr, i64) {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use bytes::Bytes;
    use bytes::BytesMut;

    use super::*;
    use crate::common::message::message_ext::MessageExt;

    #[test]
    fn test_get_sharding_key_index() {
        let sharding_key = "example_key";
        let index_size = 10;
        let result = get_sharding_key_index(sharding_key, index_size);
        assert!(result < index_size);
    }

    #[test]
    fn test_get_sharding_key_index_by_msg() {
        let mut message = MessageExt::default();
        message.message.properties.insert(
            MessageConst::PROPERTY_SHARDING_KEY.into(),
            "example_key".into(),
        );
        let index_size = 10;
        let result = get_sharding_key_index_by_msg(&message, index_size);
        assert!(result < index_size);
    }

    #[test]
    fn test_get_sharding_key_indexes() {
        let mut messages = Vec::new();
        let mut message1 = MessageExt::default();
        message1
            .message
            .properties
            .insert(MessageConst::PROPERTY_SHARDING_KEY.into(), "key1".into());
        messages.push(message1);
        let mut message2 = MessageExt::default();
        message2
            .message
            .properties
            .insert(MessageConst::PROPERTY_SHARDING_KEY.into(), "key2".into());
        messages.push(message2);
        let index_size = 10;
        let result = get_sharding_key_indexes(&messages, index_size);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_delete_property() {
        let properties_string = "aa\u{0001}bb\u{0002}cc\u{0001}bb\u{0002}";
        let name = "a";
        let result = delete_property(properties_string, name);
        assert_eq!(result, "aa\u{0001}bb");
    }

    #[test]
    fn test_build_message_id() {
        let socket_addr = "127.0.0.1:12".parse().unwrap();
        let wrote_offset = 1;
        let result = build_message_id(socket_addr, wrote_offset);
        assert_eq!(result, "7F0000010000000C0000000000000001");
    }

    #[test]
    fn build_batch_message_id_creates_correct_id_for_single_position() {
        let socket_addr = SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 8080);
        let store_host_length = 8;
        let batch_size = 1;
        let phy_pos = vec![12345];

        let result = build_batch_message_id(socket_addr, store_host_length, batch_size, &phy_pos);

        assert_eq!(result, "7F00000100001F900000000000003039");
    }

    #[test]
    fn build_batch_message_id_creates_correct_id_for_multiple_positions() {
        let socket_addr = SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 8080);
        let store_host_length = 8;
        let batch_size = 2;
        let phy_pos = vec![12345, 67890];

        let result = build_batch_message_id(socket_addr, store_host_length, batch_size, &phy_pos);

        assert_eq!(
            result,
            "7F00000100001F900000000000003039,7F00000100001F900000000000010932"
        );
    }

    #[test]
    fn build_batch_message_id_creates_empty_id_for_no_positions() {
        let socket_addr = SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 8080);
        let store_host_length = 8;
        let batch_size = 0;
        let phy_pos = vec![];

        let result = build_batch_message_id(socket_addr, store_host_length, batch_size, &phy_pos);

        assert_eq!(result, "");
    }
}
