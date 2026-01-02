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
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::net::SocketAddr;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_rust::ArcMut;

use crate::common::hasher::string_hasher::JavaStringHasher;
use crate::common::message::message_ext::MessageExt;
use crate::common::message::message_single::Message;
use crate::common::message::MessageTrait;
use crate::common::message::MessageVersion;
use crate::common::TopicFilterType;
use crate::MessageUtils;

#[derive(Default)]
pub struct MessageExtBrokerInner {
    pub message_ext_inner: MessageExt,
    pub properties_string: CheetahString,
    pub tags_code: i64,
    pub encoded_buff: Option<bytes::BytesMut>,
    pub encode_completed: bool,
    pub version: MessageVersion,
}

impl MessageExtBrokerInner {
    const VERSION: MessageVersion = MessageVersion::V1;

    #[inline]
    pub fn delete_property(&mut self, name: impl Into<CheetahString>) {
        let name = name.into();
        self.message_ext_inner.message.clear_property(name.as_str());
        self.properties_string = CheetahString::from_string(MessageUtils::delete_property(
            self.properties_string.as_str(),
            name.as_str(),
        ));
    }

    #[inline]
    pub fn with_version(&mut self, version: MessageVersion) {
        self.version = version;
    }

    #[inline]
    pub fn version(&self) -> MessageVersion {
        self.version
    }

    #[inline]
    pub fn topic(&self) -> &CheetahString {
        self.message_ext_inner.topic()
    }

    #[inline]
    pub fn get_topic(&self) -> &CheetahString {
        self.message_ext_inner.get_topic()
    }

    #[inline]
    pub fn born_host(&self) -> SocketAddr {
        self.message_ext_inner.born_host()
    }

    #[inline]
    pub fn store_host(&self) -> SocketAddr {
        self.message_ext_inner.store_host()
    }

    #[inline]
    pub fn with_born_host_v6_flag(&mut self) {
        self.message_ext_inner.with_born_host_v6_flag()
    }

    #[inline]
    pub fn with_store_host_v6_flag(&mut self) {
        self.message_ext_inner.with_store_host_v6_flag()
    }

    #[inline]
    pub fn body(&self) -> Option<bytes::Bytes> {
        self.message_ext_inner.body()
    }

    #[inline]
    pub fn sys_flag(&self) -> i32 {
        self.message_ext_inner.sys_flag()
    }

    #[inline]
    pub fn body_crc(&self) -> u32 {
        self.message_ext_inner.body_crc()
    }

    #[inline]
    pub fn queue_id(&self) -> i32 {
        self.message_ext_inner.queue_id()
    }

    #[inline]
    pub fn flag(&self) -> i32 {
        self.message_ext_inner.flag()
    }

    #[inline]
    pub fn born_timestamp(&self) -> i64 {
        self.message_ext_inner.born_timestamp()
    }

    #[inline]
    pub fn store_timestamp(&self) -> i64 {
        self.message_ext_inner.store_timestamp()
    }

    #[inline]
    pub fn born_host_bytes(&self) -> bytes::Bytes {
        self.message_ext_inner.born_host_bytes()
    }

    #[inline]
    pub fn store_host_bytes(&self) -> bytes::Bytes {
        self.message_ext_inner.born_store_bytes()
    }

    #[inline]
    pub fn reconsume_times(&self) -> i32 {
        self.message_ext_inner.reconsume_times()
    }

    #[inline]
    pub fn prepared_transaction_offset(&self) -> i64 {
        self.message_ext_inner.prepared_transaction_offset()
    }

    #[inline]
    pub fn property(&self, name: &str) -> Option<CheetahString> {
        self.message_ext_inner.properties().get(name).cloned()
    }

    #[inline]
    pub fn properties_string(&self) -> &str {
        self.properties_string.as_str()
    }

    #[inline]
    pub fn queue_offset(&self) -> i64 {
        self.message_ext_inner.queue_offset()
    }

    #[inline]
    pub fn tags_string2tags_code(_filter: &TopicFilterType, tags: &str) -> i64 {
        if tags.is_empty() {
            return 0;
        }
        JavaStringHasher::hash_str(tags) as i64
    }

    #[inline]
    pub fn tags_string_to_tags_code(tags: &str) -> i64 {
        if tags.is_empty() {
            return 0;
        }
        JavaStringHasher::hash_str(tags) as i64
    }

    #[inline]
    pub fn get_tags(&self) -> Option<CheetahString> {
        self.message_ext_inner.get_tags()
    }

    #[inline]
    pub fn is_wait_store_msg_ok(&self) -> bool {
        self.message_ext_inner.message.is_wait_store_msg_ok()
    }

    #[inline]
    pub fn body_len(&self) -> usize {
        self.message_ext_inner.message.body.as_ref().unwrap().len()
    }
}

impl fmt::Display for MessageExtBrokerInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let encoded_buff_str = match &self.encoded_buff {
            Some(encoded_buff) =>
            /* format!("Some({:?})", encoded_buff) */
            {
                "****".to_string()
            }
            None => "None".to_string(),
        };

        write!(
            f,
            "MessageExtBrokerInner {{ message_ext_inner: {}, properties_string: {}, tags_code: {}, encoded_buff: {}, \
             encode_completed: {}, version: {} }}",
            self.message_ext_inner,
            self.properties_string,
            self.tags_code,
            encoded_buff_str,
            self.encode_completed,
            self.version
        )
    }
}

impl Debug for MessageExtBrokerInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let encoded_buff_str = match &self.encoded_buff {
            Some(encoded_buff) =>
            /* format!("Some({:?})", encoded_buff) */
            {
                "****".to_string()
            }
            None => "None".to_string(),
        };

        write!(
            f,
            "MessageExtBrokerInner {{ message_ext_inner: {:?}, properties_string: {}, tags_code: {}, encoded_buff: \
             {}, encode_completed: {}, version: {} }}",
            self.message_ext_inner,
            self.properties_string,
            self.tags_code,
            encoded_buff_str,
            self.encode_completed,
            self.version
        )
    }
}

impl MessageTrait for MessageExtBrokerInner {
    #[inline]
    fn put_property(&mut self, key: CheetahString, value: CheetahString) {
        self.message_ext_inner.put_property(key, value);
    }

    #[inline]
    fn clear_property(&mut self, name: &str) {
        self.message_ext_inner.clear_property(name);
    }

    #[inline]
    fn get_property(&self, name: &CheetahString) -> Option<CheetahString> {
        self.message_ext_inner.get_property(name)
    }

    #[inline]
    fn get_topic(&self) -> &CheetahString {
        self.message_ext_inner.get_topic()
    }

    #[inline]
    fn set_topic(&mut self, topic: CheetahString) {
        self.message_ext_inner.set_topic(topic);
    }

    #[inline]
    fn get_flag(&self) -> i32 {
        self.message_ext_inner.get_flag()
    }

    #[inline]
    fn set_flag(&mut self, flag: i32) {
        self.message_ext_inner.set_flag(flag);
    }

    #[inline]
    fn get_body(&self) -> Option<&Bytes> {
        self.message_ext_inner.get_body()
    }

    #[inline]
    fn set_body(&mut self, body: Bytes) {
        self.message_ext_inner.set_body(body);
    }

    #[inline]
    fn get_properties(&self) -> &HashMap<CheetahString, CheetahString> {
        self.message_ext_inner.get_properties()
    }

    #[inline]
    fn set_properties(&mut self, properties: HashMap<CheetahString, CheetahString>) {
        self.message_ext_inner.set_properties(properties);
    }

    #[inline]
    fn get_transaction_id(&self) -> Option<&CheetahString> {
        self.message_ext_inner.get_transaction_id()
    }

    #[inline]
    fn set_transaction_id(&mut self, transaction_id: CheetahString) {
        self.message_ext_inner.set_transaction_id(transaction_id);
    }

    #[inline]
    fn get_compressed_body_mut(&mut self) -> &mut Option<Bytes> {
        self.message_ext_inner.get_compressed_body_mut()
    }

    #[inline]
    fn get_compressed_body(&self) -> Option<&Bytes> {
        self.message_ext_inner.get_compressed_body()
    }

    #[inline]
    fn set_compressed_body_mut(&mut self, compressed_body: Bytes) {
        self.message_ext_inner.set_compressed_body_mut(compressed_body);
    }

    #[inline]
    fn take_body(&mut self) -> Option<Bytes> {
        self.message_ext_inner.take_body()
    }

    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
