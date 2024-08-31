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
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::net::SocketAddr;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;

use crate::common::hasher::string_hasher::JavaStringHasher;
use crate::common::message::message_ext::MessageExt;
use crate::common::message::MessageConst;
use crate::common::message::MessageTrait;
use crate::common::message::MessageVersion;
use crate::common::sys_flag::message_sys_flag::MessageSysFlag;
use crate::common::TopicFilterType;
use crate::MessageUtils;

#[derive(Clone, Debug, Default)]
pub struct Message {
    pub topic: String,
    pub flag: i32,
    pub properties: HashMap<String, String>,
    // original bytes
    pub body: Option<bytes::Bytes>,
    // compressed bytes, maybe none, if no need to compress
    pub compressed_body: Option<bytes::Bytes>,
    pub transaction_id: Option<String>,
}

impl Message {
    pub fn new(topic: impl Into<String>, body: &[u8]) -> Self {
        Self::with_details(topic, String::new(), String::new(), 0, body, true)
    }

    pub fn with_tags(topic: impl Into<String>, tags: impl Into<String>, body: &[u8]) -> Self {
        Self::with_details(topic, tags, String::new(), 0, body, true)
    }

    pub fn with_keys(
        topic: impl Into<String>,
        tags: impl Into<String>,
        keys: impl Into<String>,
        body: &[u8],
    ) -> Self {
        Self::with_details(topic, tags, keys, 0, body, true)
    }

    pub fn with_details(
        topic: impl Into<String>,
        tags: impl Into<String>,
        keys: impl Into<String>,
        flag: i32,
        body: &[u8],
        wait_store_msg_ok: bool,
    ) -> Self {
        let topic = topic.into();
        let tags = tags.into();
        let keys = keys.into();
        let mut message = Message {
            topic,
            flag,
            body: Some(bytes::Bytes::copy_from_slice(body)),
            ..Default::default()
        };

        if !tags.is_empty() {
            message.set_tags(tags);
        }

        if !keys.is_empty() {
            message.set_keys(keys);
        }

        message.set_wait_store_msg_ok(wait_store_msg_ok);
        message
    }

    pub fn set_tags(&mut self, tags: String) {
        self.properties
            .insert(MessageConst::PROPERTY_TAGS.to_string(), tags);
    }

    pub fn set_keys(&mut self, keys: String) {
        self.properties
            .insert(MessageConst::PROPERTY_KEYS.to_string(), keys);
    }

    pub fn clear_property(&mut self, name: impl Into<String>) {
        self.properties.remove(name.into().as_str());
    }

    pub fn set_properties(&mut self, properties: HashMap<String, String>) {
        self.properties = properties;
    }

    pub fn get_property(&self, key: impl Into<String>) -> Option<String> {
        self.properties.get(key.into().as_str()).cloned()
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

    pub fn get_tags(&self) -> Option<String> {
        self.get_property(MessageConst::PROPERTY_TAGS)
    }

    pub fn is_wait_store_msg_ok(&self) -> bool {
        match self.get_property(MessageConst::PROPERTY_WAIT_STORE_MSG_OK) {
            None => true,
            Some(value) => value.parse().unwrap_or(true),
        }
    }

    pub fn get_delay_time_level(&self) -> i32 {
        match self.properties.get(MessageConst::PROPERTY_DELAY_TIME_LEVEL) {
            Some(t) => t.parse::<i32>().unwrap_or(0),
            None => 0,
        }
    }

    pub fn set_delay_time_level(&mut self, level: i32) {
        self.properties.insert(
            MessageConst::PROPERTY_DELAY_TIME_LEVEL.to_string(),
            level.to_string(),
        );
    }

    pub fn get_user_property(&self, name: impl Into<String>) -> Option<String> {
        self.properties.get(name.into().as_str()).cloned()
    }

    pub fn as_any(&self) -> &dyn Any {
        self
    }

    pub fn set_instance_id(&mut self, instance_id: impl Into<String>) {
        self.properties.insert(
            MessageConst::PROPERTY_INSTANCE_ID.to_string(),
            instance_id.into(),
        );
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let properties_str = self
            .properties
            .iter()
            .map(|(k, v)| format!("{}: {}", k, v))
            .collect::<Vec<_>>()
            .join(", ");

        let body_str = match &self.body {
            Some(body) => format!("Some({:?})", body),
            None => "None".to_string(),
        };

        let compressed_body_str = match &self.compressed_body {
            Some(compressed_body) => format!("Some({:?})", compressed_body),
            None => "None".to_string(),
        };

        let transaction_id_str = match &self.transaction_id {
            Some(transaction_id) => transaction_id.to_string(),
            None => "None".to_string(),
        };

        write!(
            f,
            "Message {{ topic: {}, flag: {}, properties: {{ {} }}, body: {}, compressed_body: {}, \
             transaction_id: {} }}",
            self.topic,
            self.flag,
            properties_str,
            body_str,
            compressed_body_str,
            transaction_id_str
        )
    }
}

#[allow(unused_variables)]
impl MessageTrait for Message {
    fn put_property(&mut self, key: &str, value: &str) {
        self.properties.insert(key.to_string(), value.to_string());
    }

    fn clear_property(&mut self, name: &str) {
        self.properties.remove(name);
    }

    fn get_property(&self, name: &str) -> Option<String> {
        self.properties.get(name).cloned()
    }

    fn get_topic(&self) -> &str {
        &self.topic
    }

    fn set_topic(&mut self, topic: &str) {
        self.topic = topic.to_string();
    }

    #[inline]
    fn get_flag(&self) -> i32 {
        self.flag
    }

    #[inline]
    fn set_flag(&mut self, flag: i32) {
        self.flag = flag;
    }

    #[inline]
    fn get_body(&self) -> Option<&Bytes> {
        self.body.as_ref()
    }

    #[inline]
    fn set_body(&mut self, body: Bytes) {
        self.body = Some(body);
    }

    fn get_properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    fn set_properties(&mut self, properties: HashMap<String, String>) {
        self.properties = properties;
    }

    fn get_transaction_id(&self) -> &str {
        self.transaction_id.as_deref().unwrap()
    }

    fn set_transaction_id(&mut self, transaction_id: &str) {
        self.transaction_id = Some(transaction_id.to_string());
    }

    fn get_compressed_body_mut(&mut self) -> &mut Option<Bytes> {
        &mut self.compressed_body
    }

    fn get_compressed_body(&self) -> Option<&Bytes> {
        self.compressed_body.as_ref()
    }

    fn set_compressed_body_mut(&mut self, compressed_body: Bytes) {
        self.compressed_body = Some(compressed_body);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Default)]
pub struct MessageExtBrokerInner {
    pub message_ext_inner: MessageExt,
    pub properties_string: String,
    pub tags_code: i64,
    pub encoded_buff: Option<bytes::BytesMut>,
    pub encode_completed: bool,
    pub version: MessageVersion,
}

impl MessageExtBrokerInner {
    const VERSION: MessageVersion = MessageVersion::V1;

    pub fn delete_property(&mut self, name: impl Into<String>) {
        let name = name.into();
        self.message_ext_inner.message.clear_property(name.as_str());
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

    pub fn tags_string2tags_code(_filter: &TopicFilterType, tags: &str) -> i64 {
        if tags.is_empty() {
            return 0;
        }
        JavaStringHasher::new().hash_str(tags) as i64
    }

    pub fn get_tags(&self) -> Option<String> {
        self.message_ext_inner.get_tags()
    }

    pub fn is_wait_store_msg_ok(&self) -> bool {
        self.message_ext_inner.message.is_wait_store_msg_ok()
    }

    pub fn body_len(&self) -> usize {
        self.message_ext_inner.message.body.as_ref().unwrap().len()
    }
}

pub fn parse_topic_filter_type(sys_flag: i32) -> TopicFilterType {
    if (sys_flag & MessageSysFlag::MULTI_TAGS_FLAG) == MessageSysFlag::MULTI_TAGS_FLAG {
        TopicFilterType::MultiTag
    } else {
        TopicFilterType::SingleTag
    }
}

pub fn tags_string2tags_code(tags: Option<&String>) -> i64 {
    if tags.is_none() {
        return 0;
    }
    let tags = tags.unwrap();
    if tags.is_empty() {
        return 0;
    }
    JavaStringHasher::new().hash_str(tags.as_str()) as i64
}
