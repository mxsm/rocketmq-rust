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
use crate::common::message::message_body::MessageBody;
use crate::common::message::message_builder::MessageBuilder;
use crate::common::message::message_ext::MessageExt;
use crate::common::message::message_flag::MessageFlag;
use crate::common::message::message_property::MessageProperties;
use crate::common::message::message_property::MessagePropertyKey;
use crate::common::message::MessageConst;
use crate::common::message::MessageTrait;
use crate::common::message::MessageVersion;
use crate::common::sys_flag::message_sys_flag::MessageSysFlag;
use crate::common::TopicFilterType;
use crate::MessageUtils;

#[derive(Clone, Debug)]
pub struct Message {
    topic: CheetahString,
    flag: MessageFlag,
    properties: MessageProperties,
    body: MessageBody,
    transaction_id: Option<CheetahString>,
}

// Private internal constructor used by builder
impl Message {
    pub(crate) fn from_builder(
        topic: CheetahString,
        body: MessageBody,
        properties: MessageProperties,
        flag: MessageFlag,
        transaction_id: Option<CheetahString>,
    ) -> Self {
        Self {
            topic,
            flag,
            properties,
            body,
            transaction_id,
        }
    }
}

impl Default for Message {
    fn default() -> Self {
        Self {
            topic: CheetahString::new(),
            flag: MessageFlag::empty(),
            properties: MessageProperties::new(),
            body: MessageBody::empty(),
            transaction_id: None,
        }
    }
}

impl Message {
    /// Creates a new message builder.
    ///
    /// This is the recommended way to create messages.
    ///
    /// # Examples
    ///
    /// ```
    /// use rocketmq_common::common::message::message_single::Message;
    ///
    /// let msg = Message::builder()
    ///     .topic("test-topic")
    ///     .body_slice(b"hello world")
    ///     .tags("important")
    ///     .build_unchecked();
    /// ```
    pub fn builder() -> MessageBuilder {
        MessageBuilder::new()
    }

    /// Create a new message with topic and body slice (will copy)
    #[deprecated(since = "0.8.0", note = "Use Message::builder() instead")]
    #[allow(deprecated)]
    #[inline]
    pub fn new(topic: impl Into<CheetahString>, body: &[u8]) -> Self {
        Self::with_details(topic, CheetahString::empty(), CheetahString::empty(), 0, body, true)
    }

    /// Create a new message with topic and Bytes (zero-copy)
    #[deprecated(since = "0.8.0", note = "Use Message::builder() instead")]
    #[allow(deprecated)]
    #[inline]
    pub fn new_with_bytes(topic: impl Into<CheetahString>, body: Bytes) -> Self {
        Self::with_details_bytes(topic, CheetahString::empty(), CheetahString::empty(), 0, body, true)
    }

    /// Create a new message with topic and Vec<u8> (zero-copy conversion to Bytes)
    #[deprecated(since = "0.8.0", note = "Use Message::builder() instead")]
    #[allow(deprecated)]
    #[inline]
    pub fn new_with_vec(topic: impl Into<CheetahString>, body: Vec<u8>) -> Self {
        Self::with_details_bytes(
            topic,
            CheetahString::empty(),
            CheetahString::empty(),
            0,
            Bytes::from(body),
            true,
        )
    }

    #[deprecated(since = "0.8.0", note = "Use Message::builder() instead")]
    #[allow(deprecated)]
    #[inline]
    pub fn new_body(topic: impl Into<CheetahString>, body: Option<Bytes>) -> Self {
        Self::with_details_body(topic, CheetahString::empty(), CheetahString::empty(), 0, body, true)
    }

    /// Create a message with tags and body slice (will copy)
    #[deprecated(since = "0.8.0", note = "Use Message::builder() instead")]
    #[allow(deprecated)]
    #[inline]
    pub fn with_tags(topic: impl Into<CheetahString>, tags: impl Into<CheetahString>, body: &[u8]) -> Self {
        Self::with_details(topic, tags, CheetahString::empty(), 0, body, true)
    }

    /// Create a message with tags and Bytes (zero-copy)
    #[deprecated(since = "0.8.0", note = "Use Message::builder() instead")]
    #[allow(deprecated)]
    #[inline]
    pub fn with_tags_bytes(topic: impl Into<CheetahString>, tags: impl Into<CheetahString>, body: Bytes) -> Self {
        Self::with_details_bytes(topic, tags, CheetahString::empty(), 0, body, true)
    }

    /// Create a message with keys and body slice (will copy)
    #[deprecated(since = "0.8.0", note = "Use Message::builder() instead")]
    #[allow(deprecated)]
    #[inline]
    pub fn with_keys(
        topic: impl Into<CheetahString>,
        tags: impl Into<CheetahString>,
        keys: impl Into<CheetahString>,
        body: &[u8],
    ) -> Self {
        Self::with_details(topic, tags, keys, 0, body, true)
    }

    /// Create a message with keys and Bytes (zero-copy)
    #[deprecated(since = "0.8.0", note = "Use Message::builder() instead")]
    #[allow(deprecated)]
    #[inline]
    pub fn with_keys_bytes(
        topic: impl Into<CheetahString>,
        tags: impl Into<CheetahString>,
        keys: impl Into<CheetahString>,
        body: Bytes,
    ) -> Self {
        Self::with_details_bytes(topic, tags, keys, 0, body, true)
    }

    /// Create message with body slice (will copy data)
    #[deprecated(since = "0.8.0", note = "Use Message::builder() instead")]
    #[allow(deprecated)]
    pub fn with_details(
        topic: impl Into<CheetahString>,
        tags: impl Into<CheetahString>,
        keys: impl Into<CheetahString>,
        flag: i32,
        body: &[u8],
        wait_store_msg_ok: bool,
    ) -> Self {
        Self::with_details_bytes(topic, tags, keys, flag, Bytes::copy_from_slice(body), wait_store_msg_ok)
    }

    /// Create message with Bytes (zero-copy)
    #[deprecated(since = "0.8.0", note = "Use Message::builder() instead")]
    pub fn with_details_bytes(
        topic: impl Into<CheetahString>,
        tags: impl Into<CheetahString>,
        keys: impl Into<CheetahString>,
        flag: i32,
        body: Bytes,
        wait_store_msg_ok: bool,
    ) -> Self {
        let topic = topic.into();
        let tags = tags.into();
        let keys = keys.into();

        // Pre-allocate HashMap with estimated capacity to avoid reallocation
        let has_tags = !tags.is_empty();
        let has_keys = !keys.is_empty();
        let initial_capacity = (has_tags as usize) + (has_keys as usize) + 1;
        let mut properties = HashMap::with_capacity(initial_capacity);

        // Use static strings for keys to avoid allocations
        if has_tags {
            properties.insert(CheetahString::from_static_str(MessageConst::PROPERTY_TAGS), tags);
        }

        if has_keys {
            properties.insert(CheetahString::from_static_str(MessageConst::PROPERTY_KEYS), keys);
        }

        if !wait_store_msg_ok {
            properties.insert(
                CheetahString::from_static_str(MessageConst::PROPERTY_WAIT_STORE_MSG_OK),
                CheetahString::from_static_str("false"),
            );
        }

        Message {
            topic,
            flag: MessageFlag::from_bits(flag),
            properties: MessageProperties::from_map(properties),
            body: MessageBody::from(body),
            transaction_id: None,
        }
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

        // Pre-allocate HashMap with estimated capacity
        let has_tags = !tags.is_empty();
        let has_keys = !keys.is_empty();
        let initial_capacity = (has_tags as usize) + (has_keys as usize) + 1;
        let mut properties = HashMap::with_capacity(initial_capacity);

        if has_tags {
            properties.insert(CheetahString::from_static_str(MessageConst::PROPERTY_TAGS), tags);
        }

        if has_keys {
            properties.insert(CheetahString::from_static_str(MessageConst::PROPERTY_KEYS), keys);
        }

        if !wait_store_msg_ok {
            properties.insert(
                CheetahString::from_static_str(MessageConst::PROPERTY_WAIT_STORE_MSG_OK),
                CheetahString::from_static_str("false"),
            );
        }

        Message {
            topic,
            flag: MessageFlag::from_bits(flag),
            properties: MessageProperties::from_map(properties),
            body: if let Some(b) = body {
                MessageBody::from(b)
            } else {
                MessageBody::empty()
            },
            transaction_id: None,
        }
    }

    #[inline]
    pub fn set_tags(&mut self, tags: CheetahString) {
        self.properties
            .as_map_mut()
            .insert(CheetahString::from_static_str(MessageConst::PROPERTY_TAGS), tags);
    }

    #[inline]
    pub fn set_keys(&mut self, keys: CheetahString) {
        self.properties
            .as_map_mut()
            .insert(CheetahString::from_static_str(MessageConst::PROPERTY_KEYS), keys);
    }

    #[inline]
    pub fn clear_property(&mut self, name: impl Into<CheetahString>) {
        self.properties.as_map_mut().remove(name.into().as_str());
    }

    #[inline]
    pub fn set_properties(&mut self, properties: HashMap<CheetahString, CheetahString>) {
        self.properties = MessageProperties::from_map(properties);
    }

    #[inline]
    pub fn get_property(&self, key: &CheetahString) -> Option<CheetahString> {
        self.properties.as_map().get(key).cloned()
    }

    #[inline]
    pub fn body(&self) -> Option<bytes::Bytes> {
        self.body.raw().cloned()
    }

    #[inline]
    pub fn flag(&self) -> i32 {
        self.flag.bits()
    }

    #[inline]
    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    #[inline]
    pub fn properties(&self) -> &MessageProperties {
        &self.properties
    }

    #[inline]
    pub fn transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }

    #[inline]
    pub fn get_tags(&self) -> Option<CheetahString> {
        self.properties.as_map().get(MessageConst::PROPERTY_TAGS).cloned()
    }

    #[inline]
    pub fn is_wait_store_msg_ok(&self) -> bool {
        self.properties.wait_store_msg_ok()
    }

    #[inline]
    fn set_wait_store_msg_ok(&mut self, wait_store_msg_ok: bool) {
        if !wait_store_msg_ok {
            self.properties.as_map_mut().insert(
                CheetahString::from_static_str(MessageConst::PROPERTY_WAIT_STORE_MSG_OK),
                CheetahString::from_static_str("false"),
            );
        }
    }

    #[inline]
    pub fn get_delay_time_level(&self) -> i32 {
        self.properties.delay_level().unwrap_or(0)
    }

    #[inline]
    pub fn set_delay_time_level(&mut self, level: i32) {
        self.properties.as_map_mut().insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_DELAY_TIME_LEVEL),
            CheetahString::from(level.to_string()),
        );
    }

    #[inline]
    pub fn get_user_property(&self, name: impl Into<CheetahString>) -> Option<CheetahString> {
        self.properties.as_map().get(name.into().as_str()).cloned()
    }

    #[inline]
    pub fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    pub fn set_instance_id(&mut self, instance_id: impl Into<CheetahString>) {
        self.properties.as_map_mut().insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_INSTANCE_ID),
            instance_id.into(),
        );
    }

    // ===== New Rust-idiomatic API methods =====

    /// Returns the message body as a byte slice (borrows).
    ///
    /// This is the recommended way to access the message body.
    #[inline]
    pub fn body_slice(&self) -> &[u8] {
        self.body.as_slice()
    }

    /// Consumes the message and returns the body.
    #[inline]
    pub fn into_body(self) -> MessageBody {
        self.body
    }

    /// Returns the message tags.
    #[inline]
    pub fn tags(&self) -> Option<&str> {
        self.properties.tags()
    }

    /// Returns the message keys as a vector.
    #[inline]
    pub fn keys(&self) -> Option<Vec<String>> {
        self.properties.keys()
    }

    /// Returns a property value by key.
    #[inline]
    pub fn property(&self, key: &str) -> Option<&str> {
        self.properties.as_map().get(key).map(|s| s.as_str())
    }

    /// Returns the message flag as a type-safe MessageFlag.
    #[inline]
    pub fn message_flag(&self) -> MessageFlag {
        self.flag
    }

    /// Returns whether to wait for store confirmation.
    #[inline]
    pub fn wait_store_msg_ok(&self) -> bool {
        self.is_wait_store_msg_ok()
    }

    /// Returns the delay time level.
    #[inline]
    pub fn delay_level(&self) -> i32 {
        self.get_delay_time_level()
    }

    /// Returns the buyer ID.
    #[inline]
    pub fn buyer_id(&self) -> Option<&str> {
        self.properties.buyer_id()
    }

    /// Returns the instance ID.
    #[inline]
    pub fn instance_id(&self) -> Option<&str> {
        self.properties.instance_id()
    }

    // ===== Internal accessors for other modules =====

    /// Returns a mutable reference to the topic (internal use only).
    #[doc(hidden)]
    #[inline]
    pub fn topic_mut(&mut self) -> &mut CheetahString {
        &mut self.topic
    }

    /// Returns a mutable reference to the flag (internal use only).
    #[doc(hidden)]
    #[inline]
    pub fn flag_mut(&mut self) -> &mut MessageFlag {
        &mut self.flag
    }

    /// Returns a mutable reference to properties (internal use only).
    #[doc(hidden)]
    #[inline]
    pub fn properties_mut(&mut self) -> &mut MessageProperties {
        &mut self.properties
    }

    /// Returns a mutable reference to the body (internal use only).
    #[doc(hidden)]
    #[inline]
    pub fn body_mut(&mut self) -> &mut MessageBody {
        &mut self.body
    }

    /// Returns a mutable reference to the transaction ID (internal use only).
    #[doc(hidden)]
    #[inline]
    pub fn transaction_id_mut(&mut self) -> &mut Option<CheetahString> {
        &mut self.transaction_id
    }

    /// Sets the topic (internal use only).
    #[doc(hidden)]
    #[inline]
    pub fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }

    /// Sets the flag (internal use only).
    #[doc(hidden)]
    #[inline]
    pub fn set_flag(&mut self, flag: i32) {
        self.flag = MessageFlag::from_bits(flag);
    }

    /// Sets the body (internal use only).
    #[doc(hidden)]
    #[inline]
    pub fn set_body(&mut self, body: Option<Bytes>) {
        self.body = if let Some(b) = body {
            MessageBody::from(b)
        } else {
            MessageBody::empty()
        };
    }

    /// Takes ownership of the body, leaving empty (internal use only).
    #[doc(hidden)]
    #[inline]
    pub fn take_body(&mut self) -> Option<Bytes> {
        let old = std::mem::take(&mut self.body);
        old.raw().cloned()
    }

    /// Returns a reference to the compressed body (internal use only).
    #[doc(hidden)]
    #[inline]
    pub fn compressed_body(&self) -> Option<&Bytes> {
        self.body.compressed()
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let properties_str = self
            .properties
            .as_map()
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<_>>()
            .join(", ");

        let body_len = self.body.len();
        let compressed = if self.body.is_compressed() {
            "compressed"
        } else {
            "uncompressed"
        };

        let transaction_id_str = match &self.transaction_id {
            Some(transaction_id) => format!("Some({transaction_id:?})"),
            None => "None".to_string(),
        };

        write!(
            f,
            "Message {{ topic: {}, flag: {:?}, properties: {{ {} }}, body: {} bytes ({}), transaction_id: {} }}",
            self.topic, self.flag, properties_str, body_len, compressed, transaction_id_str
        )
    }
}

#[allow(unused_variables)]
impl MessageTrait for Message {
    #[inline]
    fn put_property(&mut self, key: CheetahString, value: CheetahString) {
        self.properties.as_map_mut().insert(key, value);
    }

    #[inline]
    fn clear_property(&mut self, name: &str) {
        self.properties.as_map_mut().remove(name);
    }

    #[inline]
    fn property(&self, name: &CheetahString) -> Option<CheetahString> {
        self.properties.as_map().get(name).cloned()
    }

    fn property_ref(&self, name: &CheetahString) -> Option<&CheetahString> {
        self.properties.as_map().get(name)
    }

    #[inline]
    fn topic(&self) -> &CheetahString {
        &self.topic
    }

    #[inline]
    fn set_topic(&mut self, topic: CheetahString) {
        self.set_topic(topic);
    }

    #[inline]
    fn get_flag(&self) -> i32 {
        self.flag.bits()
    }

    #[inline]
    fn set_flag(&mut self, flag: i32) {
        self.set_flag(flag);
    }

    #[inline]
    fn get_body(&self) -> Option<&Bytes> {
        self.body.raw()
    }

    #[inline]
    fn set_body(&mut self, body: Bytes) {
        self.set_body(Some(body));
    }

    #[inline]
    fn get_properties(&self) -> &HashMap<CheetahString, CheetahString> {
        self.properties.as_map()
    }

    #[inline]
    fn set_properties(&mut self, properties: HashMap<CheetahString, CheetahString>) {
        self.properties = MessageProperties::from_map(properties);
    }

    #[inline]
    fn get_transaction_id(&self) -> Option<&CheetahString> {
        self.transaction_id.as_ref()
    }

    #[inline]
    fn set_transaction_id(&mut self, transaction_id: CheetahString) {
        *self.transaction_id_mut() = Some(transaction_id);
    }

    #[inline]
    fn get_compressed_body_mut(&mut self) -> Option<&mut Bytes> {
        self.body.compressed_mut().as_mut()
    }

    #[inline]
    fn get_compressed_body(&self) -> Option<&Bytes> {
        self.compressed_body()
    }

    #[inline]
    fn set_compressed_body_mut(&mut self, compressed_body: Bytes) {
        self.body.set_compressed(compressed_body);
    }

    #[inline]
    fn take_body(&mut self) -> Option<Bytes> {
        self.take_body()
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

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_message_new() {
        let msg = Message::new("test_topic", b"test_body");
        assert_eq!(msg.topic().as_str(), "test_topic");
        assert_eq!(msg.body().unwrap().as_ref(), b"test_body");
    }

    #[test]
    fn test_message_new_with_bytes() {
        let body = Bytes::from_static(b"test_body");
        let msg = Message::new_with_bytes("test_topic", body.clone());
        assert_eq!(msg.topic().as_str(), "test_topic");
        assert_eq!(msg.body().unwrap(), body);
    }

    #[test]
    fn test_message_new_with_vec() {
        let body = vec![1u8, 2, 3, 4, 5];
        let msg = Message::new_with_vec("test_topic", body.clone());
        assert_eq!(msg.topic().as_str(), "test_topic");
        assert_eq!(msg.body().unwrap().as_ref(), body.as_slice());
    }

    #[test]
    fn test_message_with_tags() {
        let msg = Message::with_tags("test_topic", "tag1", b"test_body");
        assert_eq!(msg.topic().as_str(), "test_topic");
        assert_eq!(msg.get_tags().unwrap().as_str(), "tag1");
    }

    #[test]
    fn test_message_with_tags_bytes() {
        let body = Bytes::from_static(b"test_body");
        let msg = Message::with_tags_bytes("test_topic", "tag1", body.clone());
        assert_eq!(msg.topic().as_str(), "test_topic");
        assert_eq!(msg.get_tags().unwrap().as_str(), "tag1");
        assert_eq!(msg.body().unwrap(), body);
    }

    #[test]
    fn test_message_with_keys() {
        let msg = Message::with_keys("test_topic", "tag1", "key1", b"test_body");
        assert_eq!(msg.topic().as_str(), "test_topic");
        assert_eq!(msg.get_tags().unwrap().as_str(), "tag1");
        assert_eq!(
            msg.properties()
                .as_map()
                .get(MessageConst::PROPERTY_KEYS)
                .unwrap()
                .as_str(),
            "key1"
        );
    }

    #[test]
    fn test_message_with_keys_bytes() {
        let body = Bytes::from_static(b"test_body");
        let msg = Message::with_keys_bytes("test_topic", "tag1", "key1", body.clone());
        assert_eq!(msg.topic().as_str(), "test_topic");
        assert_eq!(msg.get_tags().unwrap().as_str(), "tag1");
        assert_eq!(msg.body().unwrap(), body);
    }

    #[test]
    fn test_message_with_details() {
        let msg = Message::with_details("test_topic", "tag1", "key1", 0, b"test_body", true);
        assert_eq!(msg.topic().as_str(), "test_topic");
        assert_eq!(msg.get_tags().unwrap().as_str(), "tag1");
        assert!(msg.is_wait_store_msg_ok());
    }

    #[test]
    fn test_message_with_details_bytes() {
        let body = Bytes::from_static(b"test_body");
        let msg = Message::with_details_bytes("test_topic", "tag1", "key1", 0, body.clone(), false);
        assert_eq!(msg.topic().as_str(), "test_topic");
        assert!(!msg.is_wait_store_msg_ok());
        assert_eq!(msg.body().unwrap(), body);
    }

    #[test]
    fn test_properties_capacity_optimization() {
        // Test with no tags or keys
        let msg1 = Message::with_details_bytes(
            "topic",
            CheetahString::empty(),
            CheetahString::empty(),
            0,
            Bytes::from_static(b"body"),
            true,
        );
        assert_eq!(msg1.properties().len(), 0);

        // Test with tags
        let msg2 = Message::with_details_bytes(
            "topic",
            "tag1",
            CheetahString::empty(),
            0,
            Bytes::from_static(b"body"),
            true,
        );
        assert_eq!(msg2.properties().len(), 1);

        // Test with tags and keys
        let msg3 = Message::with_details_bytes("topic", "tag1", "key1", 0, Bytes::from_static(b"body"), true);
        assert_eq!(msg3.properties().len(), 2);

        // Test with wait_store_msg_ok = false
        let msg4 = Message::with_details_bytes("topic", "tag1", "key1", 0, Bytes::from_static(b"body"), false);
        assert_eq!(msg4.properties().len(), 3);
    }

    #[test]
    fn test_zero_copy_bytes() {
        let original_bytes = Bytes::from_static(b"test_data");
        let bytes_clone = original_bytes.clone();

        // Creating message with Bytes should not copy
        let msg = Message::new_with_bytes("topic", bytes_clone);

        // The body should share the same underlying data
        let body = msg.body().unwrap();
        assert_eq!(body.as_ptr(), original_bytes.as_ptr());
    }

    #[test]
    fn test_put_user_property_error_handling() {
        use rocketmq_error::RocketMQError;

        let mut msg = Message::new("test_topic", b"test body");

        // Test empty name
        let result = msg.put_user_property(CheetahString::empty(), CheetahString::from_slice("value"));
        assert!(result.is_err());
        if let Err(RocketMQError::InvalidProperty(e)) = result {
            assert!(e.contains("null or blank"));
        }

        // Test empty value
        let result = msg.put_user_property(CheetahString::from_slice("name"), CheetahString::empty());
        assert!(result.is_err());
        if let Err(RocketMQError::InvalidProperty(e)) = result {
            assert!(e.contains("null or blank"));
        }

        // Test system reserved property
        let result = msg.put_user_property(CheetahString::from_slice("KEYS"), CheetahString::from_slice("value"));
        assert!(result.is_err());
        if let Err(RocketMQError::InvalidProperty(e)) = result {
            assert!(e.contains("used by system"));
        }

        // Test valid user property
        let result = msg.put_user_property(
            CheetahString::from_slice("my_custom_key"),
            CheetahString::from_slice("my_value"),
        );
        assert!(result.is_ok());
        assert_eq!(
            msg.get_user_property(CheetahString::from_slice("my_custom_key"))
                .unwrap()
                .as_str(),
            "my_value"
        );
    }
}
