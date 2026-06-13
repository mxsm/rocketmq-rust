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

use std::collections::HashSet;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;

use bytes::BufMut;

use crate::common::message::message_ext::MessageExt;
use crate::common::message::MessageConst;
use crate::MessageDecoder::NAME_VALUE_SEPARATOR;
use crate::MessageDecoder::PROPERTY_SEPARATOR;
use crate::UtilAll::bytes_to_string;
use crate::UtilAll::string_to_bytes;

pub fn get_sharding_key_index(sharding_key: &str, index_size: usize) -> usize {
    if index_size == 0 {
        return 0;
    }

    let hash = murmur3_32_java_hash(sharding_key.as_bytes()) as i64;
    (hash % index_size as i64).unsigned_abs() as usize
}

fn murmur3_32_java_hash(bytes: &[u8]) -> i32 {
    const C1: u32 = 0xcc9e2d51;
    const C2: u32 = 0x1b873593;

    let mut h1 = 0u32;
    let block_count = bytes.len() / 4;

    for block_index in 0..block_count {
        let offset = block_index * 4;
        let mut k1 = u32::from_le_bytes([bytes[offset], bytes[offset + 1], bytes[offset + 2], bytes[offset + 3]]);

        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);

        h1 ^= k1;
        h1 = h1.rotate_left(13);
        h1 = h1.wrapping_mul(5).wrapping_add(0xe6546b64);
    }

    let tail = &bytes[block_count * 4..];
    let mut k1 = 0u32;
    match tail.len() {
        3 => {
            k1 ^= (tail[2] as u32) << 16;
            k1 ^= (tail[1] as u32) << 8;
            k1 ^= tail[0] as u32;
        }
        2 => {
            k1 ^= (tail[1] as u32) << 8;
            k1 ^= tail[0] as u32;
        }
        1 => {
            k1 ^= tail[0] as u32;
        }
        _ => {}
    }

    if !tail.is_empty() {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= bytes.len() as u32;
    h1 ^= h1 >> 16;
    h1 = h1.wrapping_mul(0x85ebca6b);
    h1 ^= h1 >> 13;
    h1 = h1.wrapping_mul(0xc2b2ae35);
    h1 ^= h1 >> 16;

    h1 as i32
}

pub fn get_sharding_key_index_by_msg(msg: &MessageExt, index_size: usize) -> usize {
    let sharding_key = msg
        .message
        .properties()
        .as_map()
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
    if properties_string.is_empty() || name.is_empty() {
        return properties_string.to_string();
    }

    fn find_from(haystack: &str, needle: &str, start: usize) -> Option<usize> {
        haystack.get(start..)?.find(needle).map(|offset| start + offset)
    }

    let Some(_) = find_from(properties_string, name, 0) else {
        return properties_string.to_string();
    };

    let bytes = properties_string.as_bytes();
    let property_separator = PROPERTY_SEPARATOR as u8;
    let name_value_separator = NAME_VALUE_SEPARATOR as u8;
    let mut idx0 = 0;
    let mut string_builder = String::with_capacity(properties_string.len());

    loop {
        let mut start_idx = idx0;
        let idx1 = loop {
            let Some(candidate_idx) = find_from(properties_string, name, start_idx) else {
                break None;
            };
            start_idx = candidate_idx + name.len();
            let before_ok = candidate_idx == 0 || bytes.get(candidate_idx - 1) == Some(&property_separator);
            let after_idx = candidate_idx + name.len();
            let after_ok = bytes.get(after_idx) == Some(&name_value_separator);
            if before_ok && after_ok {
                break Some(candidate_idx);
            }
        };

        let Some(idx1) = idx1 else {
            // there are no characters that need to be skipped. Append all remaining characters.
            string_builder.push_str(&properties_string[idx0..]);
            break;
        };

        // there are characters that need to be cropped
        string_builder.push_str(&properties_string[idx0..idx1]);
        // move idx2 to the end of the cropped character
        let value_start = idx1 + name.len() + 1;
        match properties_string
            .get(value_start..)
            .and_then(|rest| rest.find(PROPERTY_SEPARATOR))
            .map(|offset| value_start + offset)
        {
            Some(idx2) => {
                idx0 = idx2 + 1;
            }
            None => {
                break;
            }
        }
    }

    string_builder
}

pub fn delete_property_v2(properties_str: &str, name: &str) -> String {
    delete_property(properties_str, name)
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

pub fn parse_message_id(msg_id: impl Into<String>) -> (SocketAddr, i64) {
    fn invalid_message_id() -> (SocketAddr, i64) {
        (SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0), -1)
    }

    let msg_id = msg_id.into();
    let Some(bytes) = string_to_bytes(msg_id) else {
        return invalid_message_id();
    };

    let ip_len = match bytes.len() {
        16 => 4,
        28 => 16,
        _ => return invalid_message_id(),
    };

    let port_start = ip_len;
    let offset_start = port_start + 4;
    let Some(port_bytes) = bytes.get(port_start..offset_start) else {
        return invalid_message_id();
    };
    let Some(offset_bytes) = bytes.get(offset_start..offset_start + 8) else {
        return invalid_message_id();
    };

    let port = i32::from_be_bytes([port_bytes[0], port_bytes[1], port_bytes[2], port_bytes[3]]);
    if !(0..=u16::MAX as i32).contains(&port) {
        return invalid_message_id();
    }

    let addr = if ip_len == 4 {
        SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3])),
            port as u16,
        )
    } else {
        let mut ip = [0u8; 16];
        ip.copy_from_slice(&bytes[..16]);
        SocketAddr::new(IpAddr::V6(Ipv6Addr::from(ip)), port as u16)
    };

    let offset = i64::from_be_bytes([
        offset_bytes[0],
        offset_bytes[1],
        offset_bytes[2],
        offset_bytes[3],
        offset_bytes[4],
        offset_bytes[5],
        offset_bytes[6],
        offset_bytes[7],
    ]);

    (addr, offset)
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
    fn murmur3_32_java_hash_matches_guava_hash_bytes() {
        let cases = [
            ("", 0),
            ("example_key", 1_493_934_834),
            ("key1", -1_684_602_587),
            ("key2", 1_936_800_180),
            ("order-123", -1_381_100_355),
            ("\u{4e2d}\u{6587}key", 1_259_819_139),
            ("rocketmq", -184_929_330),
            ("sharding-999", 467_656_710),
        ];

        for (key, expected_hash) in cases {
            assert_eq!(murmur3_32_java_hash(key.as_bytes()), expected_hash, "key={key}");
        }
    }

    #[test]
    fn get_sharding_key_index_matches_java_remainder_semantics() {
        let cases = [
            ("example_key", 10, 4),
            ("example_key", 32, 18),
            ("example_key", 1024, 754),
            ("key1", 10, 7),
            ("key1", 16, 11),
            ("key1", 1024, 731),
            ("key2", 10, 0),
            ("order-123", 10, 5),
            ("\u{4e2d}\u{6587}key", 10, 9),
            ("\u{4e2d}\u{6587}key", 1024, 131),
            ("rocketmq", 1024, 50),
            ("sharding-999", 1024, 6),
        ];

        for (key, index_size, expected_index) in cases {
            assert_eq!(
                get_sharding_key_index(key, index_size),
                expected_index,
                "key={key}, index_size={index_size}"
            );
        }
    }

    #[test]
    fn get_sharding_key_index_zero_size_returns_zero() {
        assert_eq!(get_sharding_key_index("key1", 0), 0);
    }

    #[test]
    fn test_get_sharding_key_index_by_msg() {
        let mut message = MessageExt::default();
        message
            .message
            .properties_mut()
            .as_map_mut()
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
            .properties_mut()
            .as_map_mut()
            .insert(MessageConst::PROPERTY_SHARDING_KEY.into(), "key1".into());
        messages.push(message1);
        let mut message2 = MessageExt::default();
        message2
            .message
            .properties_mut()
            .as_map_mut()
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
    fn delete_property_matches_java_edge_cases() {
        let cases = [
            ("", "KeyA", ""),
            ("KeyA\u{0001}ValueA", "KeyA", ""),
            ("KeyA\u{0001}ValueA\u{0002}", "KeyA", ""),
            ("KeyA\u{0001}ValueA\u{0002}KeyA\u{0001}ValueA", "KeyA", ""),
            ("KeyA\u{0001}ValueA\u{0002}KeyA\u{0001}ValueA\u{0002}", "KeyA", ""),
            (
                "KeyB\u{0001}ValueB\u{0002}KeyA\u{0001}ValueA",
                "KeyA",
                "KeyB\u{0001}ValueB\u{0002}",
            ),
            (
                "KeyB\u{0001}ValueB\u{0002}KeyA\u{0001}ValueA\u{0002}",
                "KeyA",
                "KeyB\u{0001}ValueB\u{0002}",
            ),
            (
                "KeyB\u{0001}ValueB\u{0002}KeyA\u{0001}ValueA\u{0002}KeyB\u{0001}ValueB\u{0002}",
                "KeyA",
                "KeyB\u{0001}ValueB\u{0002}KeyB\u{0001}ValueB\u{0002}",
            ),
            (
                "KeyA\u{0001}ValueA\u{0002}KeyB\u{0001}ValueB\u{0002}",
                "KeyA",
                "KeyB\u{0001}ValueB\u{0002}",
            ),
            (
                "KeyA\u{0001}ValueA\u{0002}KeyB\u{0001}ValueB",
                "KeyA",
                "KeyB\u{0001}ValueB",
            ),
            (
                "KeyA\u{0001}ValueA\u{0002}KeyB\u{0001}ValueBKeyA\u{0001}ValueA\u{0002}",
                "KeyA",
                "KeyB\u{0001}ValueBKeyA\u{0001}ValueA\u{0002}",
            ),
            (
                "KeyA\u{0001}ValueA\u{0002}KeyB\u{0001}ValueBKeyA\u{0001}",
                "KeyA",
                "KeyB\u{0001}ValueBKeyA\u{0001}",
            ),
            (
                "KeyA\u{0001}ValueA\u{0002}KeyB\u{0001}ValueBKeyA",
                "KeyA",
                "KeyB\u{0001}ValueBKeyA",
            ),
            ("KeyA\u{0001}ValueA", "", "KeyA\u{0001}ValueA"),
        ];

        for (properties, name, expected) in cases {
            assert_eq!(delete_property(properties, name), expected);
            assert_eq!(delete_property_v2(properties, name), expected);
        }
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
