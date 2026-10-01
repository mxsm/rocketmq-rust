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

use bytes::Bytes;
use cheetah_string::CheetahString;

use crate::common::hasher::string_hasher::JavaStringHasher;
use crate::common::message::message_body::MessageBody;
use crate::common::message::message_builder::MessageBuilder;
use crate::common::message::message_flag::MessageFlag;
use crate::common::message::message_property::MessageProperties;
use crate::common::message::MessageConst;
use crate::common::message::MessageTrait;
use crate::common::sys_flag::message_sys_flag::MessageSysFlag;
use crate::common::TopicFilterType;

#[derive(Clone)]
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
    /// use rocketmq_model::common::message::message_single::Message;
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
    pub fn get_transaction_id(&self) -> Option<&CheetahString> {
        self.transaction_id.as_ref()
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
    pub fn delay_time_level(&self) -> i32 {
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
        self.delay_time_level()
    }

    /// Returns the buyer ID.
    #[inline]
    pub fn buyer_id(&self) -> Option<&str> {
        self.properties.buyer_id()
    }

    /// Returns the message priority, or `-1` if not set or not parseable.
    #[inline]
    pub fn priority(&self) -> i32 {
        self.properties.priority()
    }

    /// Sets the message priority with Java-compatible non-negative validation.
    #[inline]
    pub fn try_set_priority(&mut self, priority: i32) -> rocketmq_error::Result<()> {
        if priority < 0 {
            return Err(crate::error::invalid_argument(
                "The priority must be greater than or equal to 0",
            ));
        }
        self.properties.as_map_mut().insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_PRIORITY),
            CheetahString::from(priority.to_string()),
        );
        Ok(())
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

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Message")
            .field("topic", &self.topic)
            .field("flag", &self.flag.bits())
            .field("body_len", &self.body_slice().len())
            .field("property_count", &self.properties.len())
            .field("transaction_id_present", &self.transaction_id.is_some())
            .finish()
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Message{{topic='{}', flag={}, bodyLen={}, propertyCount={}, transactionIdPresent={}}}",
            self.topic,
            self.flag.bits(),
            self.body_slice().len(),
            self.properties.len(),
            self.transaction_id.is_some()
        )
    }
}

#[allow(unused_variables)]
impl MessageTrait for Message {
    #[inline]
    fn into_message(self) -> Message {
        self
    }

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
    fn transaction_id(&self) -> Option<&CheetahString> {
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
    match tags {
        Some(tags) if !tags.is_empty() => JavaStringHasher::hash_str(tags.as_str()) as i64,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_message_builder_with_vec() {
        let body = vec![1u8, 2, 3, 4, 5];
        let msg = Message::builder()
            .topic("test_topic")
            .body(body.clone())
            .build()
            .unwrap();
        assert_eq!(msg.topic().as_str(), "test_topic");
        assert_eq!(msg.body().unwrap().as_ref(), body.as_slice());
    }

    #[test]
    fn test_message_priority_matches_java_semantics() {
        let mut msg = Message::builder()
            .topic("test_topic")
            .body_slice(b"test_body")
            .build()
            .unwrap();
        assert_eq!(msg.priority(), -1);

        msg.try_set_priority(3).unwrap();
        assert_eq!(msg.priority(), 3);
        assert_eq!(
            msg.properties().as_map().get(MessageConst::PROPERTY_PRIORITY),
            Some(&CheetahString::from_static_str("3"))
        );

        let error = msg
            .try_set_priority(-1)
            .expect_err("negative priority should be rejected");
        assert_eq!(error.descriptor(), &rocketmq_error::CORE_ARGUMENT_INVALID);
    }

    #[test]
    fn test_get_transaction_id_returns_cheetah_string_ref() {
        let mut msg = Message::builder()
            .topic("test_topic")
            .body_slice(b"test_body")
            .build()
            .unwrap();
        msg.set_transaction_id(CheetahString::from_static_str("tx-123"));

        let transaction_id = msg.get_transaction_id();

        assert_eq!(transaction_id, Some(&CheetahString::from_static_str("tx-123")));
    }

    #[test]
    fn message_display_redacts_body_properties_and_transaction_id() {
        let mut msg = Message::builder()
            .topic("TopicA")
            .tags("TagA")
            .body_slice(&[1, 255])
            .build()
            .unwrap();
        msg.set_flag(7);
        msg.set_transaction_id(CheetahString::from_static_str("tx-1"));

        assert_eq!(
            msg.to_string(),
            "Message{topic='TopicA', flag=7, bodyLen=2, propertyCount=2, transactionIdPresent=true}"
        );
    }

    #[test]
    fn empty_message_display_contains_only_safe_metadata() {
        let msg = Message::default();

        assert_eq!(
            msg.to_string(),
            "Message{topic='', flag=0, bodyLen=0, propertyCount=0, transactionIdPresent=false}"
        );
    }

    #[test]
    fn test_builder_properties() {
        // Test with no tags or keys
        let msg1 = Message::builder()
            .topic("topic")
            .body(Bytes::from_static(b"body"))
            .build()
            .unwrap();
        assert_eq!(msg1.properties().len(), 1);
        assert_eq!(
            msg1.properties().as_map().get(MessageConst::PROPERTY_WAIT_STORE_MSG_OK),
            Some(&CheetahString::from_static_str("true"))
        );

        // Test with tags
        let msg2 = Message::builder()
            .topic("topic")
            .tags("tag1")
            .body(Bytes::from_static(b"body"))
            .build()
            .unwrap();
        assert_eq!(msg2.properties().len(), 2);

        // Test with tags and keys
        let msg3 = Message::builder()
            .topic("topic")
            .tags("tag1")
            .key("key1")
            .body(Bytes::from_static(b"body"))
            .build()
            .unwrap();
        assert_eq!(msg3.properties().len(), 3);

        // Test with wait_store_msg_ok = false
        let msg4 = Message::builder()
            .topic("topic")
            .tags("tag1")
            .key("key1")
            .body(Bytes::from_static(b"body"))
            .wait_store_msg_ok(false)
            .build()
            .unwrap();
        assert_eq!(msg4.properties().len(), 3);
        assert_eq!(
            msg4.properties().as_map().get(MessageConst::PROPERTY_WAIT_STORE_MSG_OK),
            Some(&CheetahString::from_static_str("false"))
        );
    }

    #[test]
    fn test_zero_copy_bytes() {
        let original_bytes = Bytes::from_static(b"test_data");
        let bytes_clone = original_bytes.clone();

        // Creating message with Bytes should not copy
        let msg = Message::builder().topic("topic").body(bytes_clone).build().unwrap();

        // The body should share the same underlying data
        let body = msg.body().unwrap();
        assert_eq!(body.as_ptr(), original_bytes.as_ptr());
    }

    #[test]
    fn test_put_user_property_error_handling() {
        let mut msg = Message::builder()
            .topic("test_topic")
            .body_slice(b"test body")
            .build()
            .unwrap();

        // Test empty name
        let result = msg.put_user_property(CheetahString::empty(), CheetahString::from_slice("value"));
        assert_eq!(
            result.expect_err("empty property name must fail").descriptor(),
            &rocketmq_error::PROTOCOL_MESSAGE_PROPERTY_INVALID
        );

        // Test empty value
        let result = msg.put_user_property(CheetahString::from_slice("name"), CheetahString::empty());
        assert_eq!(
            result.expect_err("empty property value must fail").descriptor(),
            &rocketmq_error::PROTOCOL_MESSAGE_PROPERTY_INVALID
        );

        // Test system reserved property
        let result = msg.put_user_property(CheetahString::from_slice("KEYS"), CheetahString::from_slice("value"));
        assert_eq!(
            result.expect_err("reserved property must fail").descriptor(),
            &rocketmq_error::PROTOCOL_MESSAGE_PROPERTY_INVALID
        );

        // Test Java-reserved priority property
        let result = msg.put_user_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_PRIORITY),
            CheetahString::from_slice("value"),
        );
        assert_eq!(
            result.expect_err("priority property must fail").descriptor(),
            &rocketmq_error::PROTOCOL_MESSAGE_PROPERTY_INVALID
        );

        // Test Java-reserved origin group property
        let result = msg.put_user_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_ORIGIN_GROUP),
            CheetahString::from_slice("value"),
        );
        assert_eq!(
            result.expect_err("origin group property must fail").descriptor(),
            &rocketmq_error::PROTOCOL_MESSAGE_PROPERTY_INVALID
        );

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
