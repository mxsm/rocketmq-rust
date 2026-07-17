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
use std::net::SocketAddr;

/// Provider-neutral message payload passed across the Proxy Core boundary.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProxyMessage {
    topic: String,
    flag: i32,
    properties: HashMap<String, String>,
    body: Option<Vec<u8>>,
    transaction_id: Option<String>,
}

impl ProxyMessage {
    pub fn new(topic: impl Into<String>, body: impl Into<Vec<u8>>) -> Self {
        Self {
            topic: topic.into(),
            body: Some(body.into()),
            ..Self::default()
        }
    }

    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    pub fn set_topic(&mut self, topic: impl Into<String>) {
        self.topic = topic.into();
    }

    pub fn flag(&self) -> i32 {
        self.flag
    }

    pub fn set_flag(&mut self, flag: i32) {
        self.flag = flag;
    }

    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    pub fn properties_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.properties
    }

    pub fn set_properties(&mut self, properties: HashMap<String, String>) {
        self.properties = properties;
    }

    pub fn property(&self, key: &str) -> Option<&str> {
        self.properties.get(key).map(String::as_str)
    }

    pub fn put_property(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.properties.insert(key.into(), value.into());
    }

    pub fn body(&self) -> Option<&[u8]> {
        self.body.as_deref()
    }

    pub fn set_body(&mut self, body: Option<Vec<u8>>) {
        self.body = body;
    }

    pub fn transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }

    pub fn set_transaction_id(&mut self, transaction_id: Option<String>) {
        self.transaction_id = transaction_id;
    }
}

/// Provider-neutral stored-message metadata returned by pull and pop ports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyMessageExt {
    pub message: ProxyMessage,
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

impl Default for ProxyMessageExt {
    fn default() -> Self {
        Self {
            message: ProxyMessage::default(),
            broker_name: String::new(),
            queue_id: 0,
            store_size: 0,
            queue_offset: 0,
            sys_flag: 0,
            born_timestamp: 0,
            born_host: SocketAddr::from(([127, 0, 0, 1], 10911)),
            store_timestamp: 0,
            store_host: SocketAddr::from(([127, 0, 0, 1], 10911)),
            msg_id: String::new(),
            commit_log_offset: 0,
            body_crc: 0,
            reconsume_times: 0,
            prepared_transaction_offset: 0,
        }
    }
}

impl ProxyMessageExt {
    pub fn topic(&self) -> &str {
        self.message.topic()
    }

    pub fn body(&self) -> Option<&[u8]> {
        self.message.body()
    }

    pub fn properties(&self) -> &HashMap<String, String> {
        self.message.properties()
    }

    pub fn property(&self, key: &str) -> Option<&str> {
        self.message.property(key)
    }

    pub fn flag(&self) -> i32 {
        self.message.flag()
    }

    pub fn broker_name(&self) -> &str {
        self.broker_name.as_str()
    }

    pub fn queue_id(&self) -> i32 {
        self.queue_id
    }

    pub fn store_size(&self) -> i32 {
        self.store_size
    }

    pub fn queue_offset(&self) -> i64 {
        self.queue_offset
    }

    pub fn sys_flag(&self) -> i32 {
        self.sys_flag
    }

    pub fn born_timestamp(&self) -> i64 {
        self.born_timestamp
    }

    pub fn born_host(&self) -> SocketAddr {
        self.born_host
    }

    pub fn store_timestamp(&self) -> i64 {
        self.store_timestamp
    }

    pub fn store_host(&self) -> SocketAddr {
        self.store_host
    }

    pub fn msg_id(&self) -> &str {
        self.msg_id.as_str()
    }

    pub fn commit_log_offset(&self) -> i64 {
        self.commit_log_offset
    }

    pub fn body_crc(&self) -> u32 {
        self.body_crc
    }

    pub fn reconsume_times(&self) -> i32 {
        self.reconsume_times
    }

    pub fn prepared_transaction_offset(&self) -> i64 {
        self.prepared_transaction_offset
    }
}
