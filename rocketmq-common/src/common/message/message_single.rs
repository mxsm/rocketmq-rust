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

use std::{collections::HashMap, net::SocketAddr};

use crate::{
    common::{
        message::{MessageTrait, MessageVersion, MESSAGE_MAGIC_CODE_V1},
        sys_flag::message_sys_flag::MessageSysFlag,
    },
    MessageUtils,
};

#[derive(Clone, Debug, Default)]
pub struct Message {
    pub topic: String,
    pub flag: i32,
    pub properties: HashMap<String, String>,
    pub body: bytes::Bytes,
    pub transaction_id: Option<String>,
}

impl Message {
    pub fn clear_property(&mut self, name: impl Into<String>) {
        self.properties.remove(name.into().as_str());
    }
}

#[allow(unused_variables)]
impl MessageTrait for Message {
    fn topic(&self) -> &str {
        todo!()
    }

    fn with_topic(&mut self, topic: impl Into<String>) {
        todo!()
    }

    fn tags(&self) -> Option<&str> {
        todo!()
    }

    fn with_tags(&mut self, tags: impl Into<String>) {
        todo!()
    }

    fn put_property(&mut self, key: impl Into<String>, value: impl Into<String>) {
        todo!()
    }

    fn properties(&self) -> &HashMap<String, String> {
        todo!()
    }

    fn put_user_property(&mut self, name: impl Into<String>, value: impl Into<String>) {
        todo!()
    }

    fn delay_time_level(&self) -> i32 {
        todo!()
    }

    fn with_delay_time_level(&self, level: i32) -> i32 {
        todo!()
    }
}

#[derive(Clone, Debug)]
pub struct MessageExt {
    pub message_inner: Message,
    pub broker_name: String,
    pub queue_id: i32,
    pub store_size: i32,
    pub queue_offset: i64,
    pub sys_flag: i32,
    pub born_timestamp: i64,
    pub born_host: SocketAddr,
    pub store_timestamp: i64,
    pub store_host: SocketAddr,
    pub msg_id: String,
    pub commit_log_offset: i64,
    pub body_crc: u32,
    pub reconsume_times: i32,
    pub prepared_transaction_offset: i64,
}

impl MessageExt {
    pub fn topic(&self) -> &str {
        self.message_inner.topic()
    }

    pub fn born_host(&self) -> SocketAddr {
        self.born_host
    }

    pub fn store_host(&self) -> SocketAddr {
        self.store_host
    }

    pub fn with_born_host_v6_flag(&mut self) {
        self.sys_flag |= MessageSysFlag::BORNHOST_V6_FLAG;
    }

    pub fn with_store_host_v6_flag(&mut self) {
        self.sys_flag |= MessageSysFlag::STOREHOSTADDRESS_V6_FLAG;
    }
}

impl Default for MessageExt {
    fn default() -> Self {
        Self {
            message_inner: Default::default(),
            broker_name: "".to_string(),
            queue_id: 0,
            store_size: 0,
            queue_offset: 0,
            sys_flag: 0,
            born_timestamp: 0,
            born_host: "127.0.0.1".parse().unwrap(),
            store_timestamp: 0,
            store_host: "127.0.0.1".parse().unwrap(),
            msg_id: "".to_string(),
            commit_log_offset: 0,
            body_crc: 0,
            reconsume_times: 0,
            prepared_transaction_offset: 0,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct MessageClientExt {
    pub message_ext_inner: MessageExt,
}

#[derive(Debug, Default)]
pub struct MessageExtBrokerInner {
    pub message_ext_inner: MessageExt,
    pub properties_string: String,
    pub tags_code: i64,
    pub encoded_buff: bytes::Bytes,
    pub encode_completed: bool,
    pub version: MessageVersion,
}

impl MessageExtBrokerInner {
    const VERSION: MessageVersion = MessageVersion::V1(MESSAGE_MAGIC_CODE_V1);

    pub fn delete_property(&mut self, name: impl Into<String>) {
        let name = name.into();
        self.message_ext_inner
            .message_inner
            .clear_property(name.as_str());
        self.properties_string =
            MessageUtils::delete_property(self.properties_string.as_str(), name.as_str());
    }

    pub fn with_version(&mut self, version: MessageVersion) {
        self.version = version;
    }

    pub fn topic(&self) -> &str {
        self.message_ext_inner.topic()
    }

    pub fn born_host(&self) -> SocketAddr {
        self.message_ext_inner.born_host()
    }

    pub fn store_host(&self) -> SocketAddr {
        self.message_ext_inner.store_host()
    }

    pub fn with_born_host_v6_flag(&mut self) {
        self.message_ext_inner.with_born_host_v6_flag()
    }

    pub fn with_store_host_v6_flag(&mut self) {
        self.message_ext_inner.with_store_host_v6_flag()
    }
}
