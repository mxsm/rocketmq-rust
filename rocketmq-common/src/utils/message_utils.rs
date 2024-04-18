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
use std::{
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
    hash::{Hash, Hasher},
    net::SocketAddr,
};

use bytes::BufMut;

use crate::{
    common::{
        message::{
            message_single::{MessageExt, MessageExtBrokerInner},
            MessageConst,
        },
        sys_flag::message_sys_flag::MessageSysFlag,
    },
    MessageDecoder::{NAME_VALUE_SEPARATOR, PROPERTY_SEPARATOR},
    UtilAll::bytes_to_string,
};

pub fn get_sharding_key_index(sharding_key: &str, index_size: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    sharding_key.hash(&mut hasher);
    let hash = hasher.finish() as usize;
    hash % index_size
}

#[allow(clippy::manual_unwrap_or_default)]
pub fn get_sharding_key_index_by_msg(msg: &MessageExt, index_size: usize) -> usize {
    let sharding_key = match msg
        .message
        .properties
        .get(MessageConst::PROPERTY_SHARDING_KEY)
    {
        Some(key) => key,
        None => "",
    };
    get_sharding_key_index(sharding_key, index_size)
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

pub fn parse_message_id(msg_id: impl Into<String>) -> (SocketAddr, i64) {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delete_property() {
        assert_eq!(
            delete_property("aa\u{0001}bb\u{0002}cc\u{0001}bb\u{0002}", "a"),
            "aa\u{0001}bb"
        );
    }

    #[test]
    fn test_build_message_id() {
        println!("{}", build_message_id("127.0.0.1:12".parse().unwrap(), 1))
    }
}
