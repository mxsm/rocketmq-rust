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

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::net::SocketAddr;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use cheetah_string::CheetahString;

use crate::common::message::message_single::Message;
use crate::common::message::MessageTrait;
use crate::common::sys_flag::message_sys_flag::MessageSysFlag;

#[derive(Clone, Debug)]
pub struct MessageExt {
    pub message: Message,
    pub broker_name: CheetahString,
    pub queue_id: i32,
    pub store_size: i32,
    pub queue_offset: i64,
    pub sys_flag: i32,
    pub born_timestamp: i64,
    pub born_host: SocketAddr,
    pub store_timestamp: i64,
    pub store_host: SocketAddr,
    pub msg_id: CheetahString,
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

    #[inline]
    pub fn born_host_bytes(&self) -> bytes::Bytes {
        Self::socket_address_2_byte_buffer(&self.born_host)
    }

    #[inline]
    pub fn born_store_bytes(&self) -> bytes::Bytes {
        Self::socket_address_2_byte_buffer(&self.store_host)
    }

    #[inline]
    pub fn topic(&self) -> &CheetahString {
        self.message.topic()
    }

    #[inline]
    pub fn born_host(&self) -> SocketAddr {
        self.born_host
    }

    #[inline]
    pub fn store_host(&self) -> SocketAddr {
        self.store_host
    }

    #[inline]
    pub fn with_born_host_v6_flag(&mut self) {
        self.sys_flag |= MessageSysFlag::BORNHOST_V6_FLAG;
    }

    #[inline]
    pub fn with_store_host_v6_flag(&mut self) {
        self.sys_flag |= MessageSysFlag::STOREHOSTADDRESS_V6_FLAG;
    }

    #[inline]
    pub fn body(&self) -> Option<bytes::Bytes> {
        self.message.body()
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

    #[inline]
    pub fn flag(&self) -> i32 {
        self.message.flag()
    }

    #[inline]
    pub fn message_inner(&self) -> &Message {
        &self.message
    }

    #[inline]
    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    #[inline]
    pub fn store_size(&self) -> i32 {
        self.store_size
    }

    #[inline]
    pub fn queue_offset(&self) -> i64 {
        self.queue_offset
    }

    #[inline]
    pub fn born_timestamp(&self) -> i64 {
        self.born_timestamp
    }

    #[inline]
    pub fn store_timestamp(&self) -> i64 {
        self.store_timestamp
    }

    #[inline]
    pub fn msg_id(&self) -> &CheetahString {
        &self.msg_id
    }

    #[inline]
    pub fn commit_log_offset(&self) -> i64 {
        self.commit_log_offset
    }

    #[inline]
    pub fn reconsume_times(&self) -> i32 {
        self.reconsume_times
    }

    #[inline]
    pub fn prepared_transaction_offset(&self) -> i64 {
        self.prepared_transaction_offset
    }

    pub fn set_message_inner(&mut self, message_inner: Message) {
        self.message = message_inner;
    }

    pub fn set_broker_name(&mut self, broker_name: CheetahString) {
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

    pub fn set_msg_id(&mut self, msg_id: CheetahString) {
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

    pub fn properties(&self) -> &HashMap<CheetahString, CheetahString> {
        self.message.properties()
    }

    pub fn get_tags(&self) -> Option<CheetahString> {
        self.message.get_tags()
    }
}

impl Default for MessageExt {
    fn default() -> Self {
        Self {
            message: Message::default(),
            broker_name: CheetahString::default(),
            queue_id: 0,
            store_size: 0,
            queue_offset: 0,
            sys_flag: 0,
            born_timestamp: 0,
            born_host: "127.0.0.1:10911".parse().unwrap(),
            store_timestamp: 0,
            store_host: "127.0.0.1:10911".parse().unwrap(),
            msg_id: CheetahString::default(),
            commit_log_offset: 0,
            body_crc: 0,
            reconsume_times: 0,
            prepared_transaction_offset: 0,
        }
    }
}

impl fmt::Display for MessageExt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MessageExt {{ message: {}, broker_name: {}, queue_id: {}, store_size: {}, queue_offset: {}, sys_flag: \
             {}, born_timestamp: {}, born_host: {}, store_timestamp: {}, store_host: {}, msg_id: {}, \
             commit_log_offset: {}, body_crc: {}, reconsume_times: {}, prepared_transaction_offset: {} }}",
            self.message,
            self.broker_name,
            self.queue_id,
            self.store_size,
            self.queue_offset,
            self.sys_flag,
            self.born_timestamp,
            self.born_host,
            self.store_timestamp,
            self.store_host,
            self.msg_id,
            self.commit_log_offset,
            self.body_crc,
            self.reconsume_times,
            self.prepared_transaction_offset
        )
    }
}
impl MessageTrait for MessageExt {
    fn put_property(&mut self, key: CheetahString, value: CheetahString) {
        self.message.put_property(key, value);
    }

    fn clear_property(&mut self, name: &str) {
        self.message.clear_property(name);
    }

    fn get_property(&self, name: &CheetahString) -> Option<CheetahString> {
        self.message.get_property(name)
    }

    fn get_topic(&self) -> &CheetahString {
        self.message.get_topic()
    }

    fn set_topic(&mut self, topic: CheetahString) {
        self.message.set_topic(topic);
    }

    fn get_flag(&self) -> i32 {
        self.message.get_flag()
    }

    fn set_flag(&mut self, flag: i32) {
        self.message.set_flag(flag);
    }

    fn get_body(&self) -> Option<&Bytes> {
        self.message.get_body()
    }

    fn set_body(&mut self, body: Bytes) {
        self.message.set_body(body);
    }

    fn get_properties(&self) -> &HashMap<CheetahString, CheetahString> {
        self.message.get_properties()
    }

    fn set_properties(&mut self, properties: HashMap<CheetahString, CheetahString>) {
        self.message.set_properties(properties);
    }

    #[inline]
    fn get_transaction_id(&self) -> Option<&CheetahString> {
        self.message.get_transaction_id()
    }

    fn set_transaction_id(&mut self, transaction_id: CheetahString) {
        self.message.set_transaction_id(transaction_id);
    }

    fn get_compressed_body_mut(&mut self) -> &mut Option<Bytes> {
        self.message.get_compressed_body_mut()
    }

    fn get_compressed_body(&self) -> Option<&Bytes> {
        self.message.get_compressed_body()
    }

    fn set_compressed_body_mut(&mut self, compressed_body: Bytes) {
        self.message.set_compressed_body_mut(compressed_body);
    }

    #[inline]
    fn take_body(&mut self) -> Option<Bytes> {
        self.message.take_body()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
