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

use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::Hash;
use std::hash::Hasher;
use std::net::SocketAddr;

use bytes::BufMut;

use crate::common::message::message_ext::MessageExt;
use crate::common::message::MessageConst;
use crate::MessageDecoder::NAME_VALUE_SEPARATOR;
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

pub fn delete_property(properties_string: &str, name: &str) -> String {
    if !properties_string.is_empty() {
        let mut idx0 = 0;
        let mut idx1;
        let mut idx2;
        idx1 = properties_string.find(name);
        if idx1.is_some() {
            // cropping may be required
            let mut string_builder = String::with_capacity(properties_string.len());
            loop {
                let mut start_idx = idx0;
                loop {
                    idx1 = properties_string[start_idx..].find(name).map(|i| i + start_idx);
                    if idx1.is_none() {
                        break;
                    }
                    let idx1 = idx1.unwrap();
                    start_idx = idx1 + name.len();
                    if (idx1 == 0 || properties_string.chars().nth(idx1 - 1) == Some(PROPERTY_SEPARATOR))
                        && (properties_string.len() > idx1 + name.len())
                        && properties_string.chars().nth(idx1 + name.len()) == Some(NAME_VALUE_SEPARATOR)
                    {
                        break;
                    }
                }
                if idx1.is_none() {
                    // there are no characters that need to be skipped. Append all remaining
                    // characters.
                    string_builder.push_str(&properties_string[idx0..]);
                    break;
                }
                let idx1 = idx1.unwrap();
                // there are characters that need to be cropped
                string_builder.push_str(&properties_string[idx0..idx1]);
                // move idx2 to the end of the cropped character
                idx2 = properties_string[idx1 + name.len() + 1..]
                    .find(PROPERTY_SEPARATOR)
                    .map(|i| i + idx1 + name.len() + 1);
                // all subsequent characters will be cropped
                if idx2.is_none() {
                    break;
                }
                idx0 = idx2.unwrap() + 1;
            }
            return string_builder;
        }
    }
    properties_string.to_string()
}

#[allow(unused_assignments)]
pub fn delete_property_v2(properties_str: &str, name: &str) -> String {
    if properties_str.is_empty() {
        return String::new();
    }
    if let Some(mut idx1) = properties_str.find(name) {
        let mut idx0 = 0;
        let mut result = String::with_capacity(properties_str.len());

        loop {
            let mut start_idx = idx0;
            loop {
                match properties_str[start_idx..].find(name) {
                    Some(offset) => {
                        idx1 = start_idx + offset;
                        start_idx = idx1 + name.len();
                        let before_ok = idx1 == 0 || properties_str.chars().nth(idx1 - 1) == Some(PROPERTY_SEPARATOR);
                        let after_ok = properties_str.chars().nth(idx1 + name.len()) == Some(NAME_VALUE_SEPARATOR);
                        if before_ok && after_ok {
                            break;
                        }
                    }
                    None => {
                        idx1 = usize::MAX;
                        break;
                    }
                }
            }

            if idx1 == usize::MAX {
                result.push_str(&properties_str[idx0..]);
                break;
            }

            result.push_str(&properties_str[idx0..idx1]);

            let idx2_opt = properties_str[idx1 + name.len() + 1..].find(PROPERTY_SEPARATOR);

            match idx2_opt {
                Some(rel) => idx0 = idx1 + name.len() + 1 + rel + 1, //
                None => break,
            }
        }
        result
    } else {
        properties_str.to_string()
    }
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
        message
            .message
            .properties
            .insert(MessageConst::PROPERTY_SHARDING_KEY.into(), "example_key".into());
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
        let name = "aa";
        let result = delete_property(properties_string, name);
        assert_eq!(result, "cc\u{0001}bb\u{0002}");

        let result = delete_property_v2(properties_string, name);
        assert_eq!(result, "cc\u{0001}bb\u{0002}");
    }

    #[test]
    fn delete_property_removes_property_correctly() {
        let properties_string = "key1\u{0001}value1\u{0002}key2\u{0001}value2\u{0002}key3\u{0001}value3";
        let name = "key2";
        let result = delete_property(properties_string, name);
        assert_eq!(result, "key1\u{0001}value1\u{0002}key3\u{0001}value3");

        let result = delete_property_v2(properties_string, name);
        assert_eq!(result, "key1\u{0001}value1\u{0002}key3\u{0001}value3");
    }

    #[test]
    fn delete_property_handles_empty_string() {
        let properties_string = "";
        let name = "key";
        let result = delete_property(properties_string, name);
        assert_eq!(result, "");
    }

    #[test]
    fn delete_property_handles_non_existent_key() {
        let properties_string = "key1\u{0001}value1\u{0002}key2\u{0001}value2";
        let name = "key3";
        let result = delete_property(properties_string, name);
        assert_eq!(result, "key1\u{0001}value1\u{0002}key2\u{0001}value2");
    }

    #[test]
    fn delete_property_handles_key_at_start() {
        let properties_string = "key1\u{0001}value1\u{0002}key2\u{0001}value2";
        let name = "key1";
        let result = delete_property(properties_string, name);
        assert_eq!(result, "key2\u{0001}value2");
    }

    #[test]
    fn delete_property_handles_key_at_end() {
        let properties_string = "key1\u{0001}value1\u{0002}key2\u{0001}value2";
        let name = "key2";
        let result = delete_property(properties_string, name);
        assert_eq!(result, "key1\u{0001}value1\u{0002}");
    }

    #[test]
    fn delete_property_handles_multiple_occurrences() {
        let properties_string = "key1\u{0001}value1\u{0002}key2\u{0001}value2\u{0002}key1\u{0001}value3";
        let name = "key1";
        let result = delete_property(properties_string, name);
        assert_eq!(result, "key2\u{0001}value2\u{0002}");
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
