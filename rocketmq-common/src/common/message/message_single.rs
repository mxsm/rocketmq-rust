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

use bytes::{Buf, BufMut};

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
    pub body: Option<bytes::Bytes>,
    pub transaction_id: Option<String>,
}

impl Message {
    pub fn clear_property(&mut self, name: impl Into<String>) {
        self.properties.remove(name.into().as_str());
    }

    pub fn body(&self) -> Option<bytes::Bytes> {
        self.body.as_ref().cloned()
    }

    pub fn flag(&self) -> i32 {
        self.flag
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
    pub fn transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
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
    pub fn socket_address_2_byte_buffer(ip: &SocketAddr) -> bytes::Bytes {
        match ip {
            SocketAddr::V4(value) => {
                let mut byte_buffer = bytes::BytesMut::with_capacity(4 + 4);
                byte_buffer.put_slice(&value.ip().octets());
                byte_buffer.put_i32(value.port() as i32);
                byte_buffer.copy_to_bytes(byte_buffer.len())
            }
            SocketAddr::V6(value) => {
                let mut byte_buffer = bytes::BytesMut::with_capacity(16 + 4);
                byte_buffer.put_slice(&value.ip().octets());
                byte_buffer.put_i32(value.port() as i32);
                byte_buffer.copy_to_bytes(byte_buffer.len())
            }
        }
    }

    pub fn born_host_bytes(&self) -> bytes::Bytes {
        Self::socket_address_2_byte_buffer(&self.born_host)
    }

    pub fn born_store_bytes(&self) -> bytes::Bytes {
        Self::socket_address_2_byte_buffer(&self.store_host)
    }

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

    pub fn body(&self) -> Option<bytes::Bytes> {
        self.message_inner.body()
    }

    #[inline]
    pub fn sys_flag(&self) -> i32 {
        self.sys_flag
    }
    #[inline]
    pub fn body_crc(&self) -> u32 {
        self.body_crc
    }
    #[inline]
    pub fn queue_id(&self) -> i32 {
        self.queue_id
    }

    pub fn flag(&self) -> i32 {
        self.message_inner.flag()
    }

    pub fn message_inner(&self) -> &Message {
        &self.message_inner
    }
    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }
    pub fn store_size(&self) -> i32 {
        self.store_size
    }
    pub fn queue_offset(&self) -> i64 {
        self.queue_offset
    }
    pub fn born_timestamp(&self) -> i64 {
        self.born_timestamp
    }
    pub fn store_timestamp(&self) -> i64 {
        self.store_timestamp
    }
    pub fn msg_id(&self) -> &str {
        &self.msg_id
    }
    pub fn commit_log_offset(&self) -> i64 {
        self.commit_log_offset
    }
    pub fn reconsume_times(&self) -> i32 {
        self.reconsume_times
    }
    pub fn prepared_transaction_offset(&self) -> i64 {
        self.prepared_transaction_offset
    }

    pub fn set_message_inner(&mut self, message_inner: Message) {
        self.message_inner = message_inner;
    }
    pub fn set_broker_name(&mut self, broker_name: String) {
        self.broker_name = broker_name;
    }
    pub fn set_queue_id(&mut self, queue_id: i32) {
        self.queue_id = queue_id;
    }
    pub fn set_store_size(&mut self, store_size: i32) {
        self.store_size = store_size;
    }
    pub fn set_queue_offset(&mut self, queue_offset: i64) {
        self.queue_offset = queue_offset;
    }
    pub fn set_sys_flag(&mut self, sys_flag: i32) {
        self.sys_flag = sys_flag;
    }
    pub fn set_born_timestamp(&mut self, born_timestamp: i64) {
        self.born_timestamp = born_timestamp;
    }
    pub fn set_born_host(&mut self, born_host: SocketAddr) {
        self.born_host = born_host;
    }
    pub fn set_store_timestamp(&mut self, store_timestamp: i64) {
        self.store_timestamp = store_timestamp;
    }
    pub fn set_store_host(&mut self, store_host: SocketAddr) {
        self.store_host = store_host;
    }
    pub fn set_msg_id(&mut self, msg_id: String) {
        self.msg_id = msg_id;
    }
    pub fn set_commit_log_offset(&mut self, commit_log_offset: i64) {
        self.commit_log_offset = commit_log_offset;
    }
    pub fn set_body_crc(&mut self, body_crc: u32) {
        self.body_crc = body_crc;
    }
    pub fn set_reconsume_times(&mut self, reconsume_times: i32) {
        self.reconsume_times = reconsume_times;
    }
    pub fn set_prepared_transaction_offset(&mut self, prepared_transaction_offset: i64) {
        self.prepared_transaction_offset = prepared_transaction_offset;
    }

    pub fn properties(&self) -> &HashMap<String, String> {
        self.message_inner.properties()
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
    const VERSION: MessageVersion = MessageVersion::V1;

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

    pub fn version(&self) -> MessageVersion {
        self.version
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

    pub fn body(&self) -> Option<bytes::Bytes> {
        self.message_ext_inner.body()
    }

    pub fn sys_flag(&self) -> i32 {
        self.message_ext_inner.sys_flag()
    }
    pub fn body_crc(&self) -> u32 {
        self.message_ext_inner.body_crc()
    }
    pub fn queue_id(&self) -> i32 {
        self.message_ext_inner.queue_id()
    }
    pub fn flag(&self) -> i32 {
        self.message_ext_inner.flag()
    }
    pub fn born_timestamp(&self) -> i64 {
        self.message_ext_inner.born_timestamp()
    }

    pub fn store_timestamp(&self) -> i64 {
        self.message_ext_inner.store_timestamp()
    }

    pub fn born_host_bytes(&self) -> bytes::Bytes {
        self.message_ext_inner.born_host_bytes()
    }

    pub fn store_host_bytes(&self) -> bytes::Bytes {
        self.message_ext_inner.born_store_bytes()
    }

    pub fn reconsume_times(&self) -> i32 {
        self.message_ext_inner.reconsume_times()
    }
    pub fn prepared_transaction_offset(&self) -> i64 {
        self.message_ext_inner.prepared_transaction_offset()
    }

    pub fn property(&self, name: &str) -> Option<String> {
        self.message_ext_inner.properties().get(name).cloned()
    }

    pub fn properties_string(&self) -> &str {
        self.properties_string.as_str()
    }

    pub fn queue_offset(&self) -> i64 {
        self.message_ext_inner.queue_offset()
    }
}
