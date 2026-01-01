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
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::net::SocketAddr;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use cheetah_string::CheetahString;

use crate::common::hasher::string_hasher::JavaStringHasher;
use crate::common::message::message_ext::MessageExt;
use crate::common::message::MessageConst;
use crate::common::message::MessageTrait;
use crate::common::message::MessageVersion;
use crate::common::sys_flag::message_sys_flag::MessageSysFlag;
use crate::common::TopicFilterType;
use crate::MessageUtils;

#[derive(Clone, Debug)]
pub struct Message {
    pub topic: CheetahString,
    pub flag: i32,
    pub properties: HashMap<CheetahString, CheetahString>,
    // original bytes
    pub body: Option<bytes::Bytes>,
    // compressed bytes, maybe none, if no need to compress
    pub compressed_body: Option<bytes::Bytes>,
    pub transaction_id: Option<CheetahString>,
}

impl Default for Message {
    fn default() -> Self {
        Self {
            topic: CheetahString::new(),
            flag: 0,
            properties: HashMap::new(),
            body: None,
            compressed_body: None,
            transaction_id: None,
        }
    }
}

impl Message {
    pub fn new(topic: impl Into<CheetahString>, body: &[u8]) -> Self {
        Self::with_details(topic, CheetahString::new(), CheetahString::new(), 0, body, true)
    }

    pub fn new_body(topic: impl Into<CheetahString>, body: Option<Bytes>) -> Self {
        Self::with_details_body(topic, CheetahString::new(), CheetahString::new(), 0, body, true)
    }

    pub fn with_tags(topic: impl Into<CheetahString>, tags: impl Into<CheetahString>, body: &[u8]) -> Self {
        Self::with_details(topic, tags, String::new(), 0, body, true)
    }

    pub fn with_keys(
        topic: impl Into<CheetahString>,
        tags: impl Into<CheetahString>,
        keys: impl Into<CheetahString>,
        body: &[u8],
    ) -> Self {
        Self::with_details(topic, tags, keys, 0, body, true)
    }

    pub fn with_details(
        topic: impl Into<CheetahString>,
        tags: impl Into<CheetahString>,
        keys: impl Into<CheetahString>,
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

    pub fn with_details_body(
        topic: impl Into<CheetahString>,
        tags: impl Into<CheetahString>,
        keys: impl Into<CheetahString>,
        flag: i32,
        body: Option<Bytes>,
        wait_store_msg_ok: bool,
    ) -> Self {
        let topic = topic.into();
        let tags = tags.into();
        let keys = keys.into();
        let mut message = Message {
            topic,
            flag,
            body,
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

    #[inline]
    pub fn set_tags(&mut self, tags: CheetahString) {
        self.properties
            .insert(CheetahString::from_static_str(MessageConst::PROPERTY_TAGS), tags);
    }

    #[inline]
    pub fn set_keys(&mut self, keys: CheetahString) {
        self.properties
            .insert(CheetahString::from_static_str(MessageConst::PROPERTY_KEYS), keys);
    }

    #[inline]
    pub fn clear_property(&mut self, name: impl Into<CheetahString>) {
        self.properties.remove(name.into().as_str());
    }

    #[inline]
    pub fn set_properties(&mut self, properties: HashMap<CheetahString, CheetahString>) {
        self.properties = properties;
    }

    #[inline]
    pub fn get_property(&self, key: &CheetahString) -> Option<CheetahString> {
        self.properties.get(key).cloned()
    }

    #[inline]
    pub fn body(&self) -> Option<bytes::Bytes> {
        self.body.as_ref().cloned()
    }

    #[inline]
    pub fn flag(&self) -> i32 {
        self.flag
    }

    #[inline]
    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    #[inline]
    pub fn properties(&self) -> &HashMap<CheetahString, CheetahString> {
        &self.properties
    }

    #[inline]
    pub fn transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }

    #[inline]
    pub fn get_tags(&self) -> Option<CheetahString> {
        self.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_TAGS))
    }

    #[inline]
    pub fn is_wait_store_msg_ok(&self) -> bool {
        match self.get_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_WAIT_STORE_MSG_OK,
        )) {
            None => true,
            Some(value) => value.parse().unwrap_or(true),
        }
    }

    #[inline]
    pub fn get_delay_time_level(&self) -> i32 {
        match self.properties.get(MessageConst::PROPERTY_DELAY_TIME_LEVEL) {
            Some(t) => t.parse::<i32>().unwrap_or(0),
            None => 0,
        }
    }

    #[inline]
    pub fn set_delay_time_level(&mut self, level: i32) {
        self.properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_DELAY_TIME_LEVEL),
            CheetahString::from(level.to_string()),
        );
    }

    #[inline]
    pub fn get_user_property(&self, name: impl Into<CheetahString>) -> Option<CheetahString> {
        self.properties.get(name.into().as_str()).cloned()
    }

    #[inline]
    pub fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    pub fn set_instance_id(&mut self, instance_id: impl Into<CheetahString>) {
        self.properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_INSTANCE_ID),
            instance_id.into(),
        );
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let properties_str = self
            .properties
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<_>>()
            .join(", ");

        let body_str = match &self.body {
            Some(body) => format!("Some({body:?})"),
            None => "None".to_string(),
        };

        let compressed_body_str = match &self.compressed_body {
            Some(compressed_body) => format!("Some({compressed_body:?})"),
            None => "None".to_string(),
        };

        let transaction_id_str = match &self.transaction_id {
            Some(transaction_id) => transaction_id.to_string(),
            None => "None".to_string(),
        };

        write!(
            f,
            "Message {{ topic: {}, flag: {}, properties: {{ {} }}, body: {}, compressed_body: {}, transaction_id: {} \
             }}",
            self.topic, self.flag, properties_str, body_str, compressed_body_str, transaction_id_str
        )
    }
}

#[allow(unused_variables)]
impl MessageTrait for Message {
    #[inline]
    fn put_property(&mut self, key: CheetahString, value: CheetahString) {
        self.properties.insert(key, value);
    }

    #[inline]
    fn clear_property(&mut self, name: &str) {
        self.properties.remove(name);
    }

    #[inline]
    fn get_property(&self, name: &CheetahString) -> Option<CheetahString> {
        self.properties.get(name).cloned()
    }

    #[inline]
    fn get_topic(&self) -> &CheetahString {
        &self.topic
    }

    #[inline]
    fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
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

    #[inline]
    fn get_properties(&self) -> &HashMap<CheetahString, CheetahString> {
        &self.properties
    }

    #[inline]
    fn set_properties(&mut self, properties: HashMap<CheetahString, CheetahString>) {
        self.properties = properties;
    }

    #[inline]
    fn get_transaction_id(&self) -> Option<&CheetahString> {
        self.transaction_id.as_ref()
    }

    #[inline]
    fn set_transaction_id(&mut self, transaction_id: CheetahString) {
        self.transaction_id = Some(transaction_id);
    }

    #[inline]
    fn get_compressed_body_mut(&mut self) -> &mut Option<Bytes> {
        &mut self.compressed_body
    }

    #[inline]
    fn get_compressed_body(&self) -> Option<&Bytes> {
        self.compressed_body.as_ref()
    }

    #[inline]
    fn set_compressed_body_mut(&mut self, compressed_body: Bytes) {
        self.compressed_body = Some(compressed_body);
    }

    #[inline]
    fn take_body(&mut self) -> Option<Bytes> {
        self.body.take()
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

pub fn parse_topic_filter_type(sys_flag: i32) -> TopicFilterType {
    if (sys_flag & MessageSysFlag::MULTI_TAGS_FLAG) == MessageSysFlag::MULTI_TAGS_FLAG {
        TopicFilterType::MultiTag
    } else {
        TopicFilterType::SingleTag
    }
}

pub fn tags_string2tags_code(tags: Option<&CheetahString>) -> i64 {
    if tags.is_none() {
        return 0;
    }
    let tags = tags.unwrap();
    if tags.is_empty() {
        return 0;
    }
    JavaStringHasher::hash_str(tags.as_str()) as i64
}
