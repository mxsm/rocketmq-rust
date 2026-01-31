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

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

use crate::common::message::message_body::MessageBody;
use crate::common::message::message_flag::MessageFlag;
use crate::common::message::message_property::MessageProperties;
use crate::common::message::message_property::MessagePropertyKey;
use crate::common::message::MessageConst;

/// Builder for constructing messages.
///
/// # Examples
///
/// ```
/// use rocketmq_common::common::message::message_builder::MessageBuilder;
///
/// let msg = MessageBuilder::new()
///     .topic("test-topic")
///     .body_slice(b"hello world")
///     .tags("important")
///     .keys(vec!["key1".to_string(), "key2".to_string()])
///     .build();
/// ```
#[derive(Default)]
pub struct MessageBuilder {
    topic: Option<CheetahString>,
    body: MessageBody,
    properties: MessageProperties,
    flag: MessageFlag,
    transaction_id: Option<CheetahString>,
}

impl MessageBuilder {
    /// Creates a new message builder.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the topic name (required).
    #[inline]
    pub fn topic(mut self, topic: impl Into<CheetahString>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// Sets the message body from bytes.
    #[inline]
    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = MessageBody::new(body);
        self
    }

    /// Sets the message body from a byte slice.
    #[inline]
    pub fn body_slice(mut self, body: &[u8]) -> Self {
        self.body = MessageBody::new(Bytes::copy_from_slice(body));
        self
    }

    /// Sets an empty body.
    #[inline]
    pub fn empty_body(mut self) -> Self {
        self.body = MessageBody::empty();
        self
    }

    /// Sets the message tags.
    #[inline]
    pub fn tags(mut self, tags: impl Into<CheetahString>) -> Self {
        self.properties.insert(MessagePropertyKey::Tags, tags.into());
        self
    }

    /// Sets the message keys from a vector.
    #[inline]
    pub fn keys(mut self, keys: Vec<String>) -> Self {
        if !keys.is_empty() {
            let keys_str = keys.join(MessageConst::KEY_SEPARATOR);
            self.properties.insert(MessagePropertyKey::Keys, keys_str);
        }
        self
    }

    /// Sets a single message key.
    #[inline]
    pub fn key(mut self, key: impl Into<CheetahString>) -> Self {
        self.properties.insert(MessagePropertyKey::Keys, key.into());
        self
    }

    /// Sets the message flag.
    #[inline]
    pub fn flag(mut self, flag: MessageFlag) -> Self {
        self.flag = flag;
        self
    }

    /// Sets the message flag from raw bits.
    #[inline]
    pub fn flag_bits(mut self, bits: i32) -> Self {
        self.flag = MessageFlag::from_bits(bits);
        self
    }

    /// Sets the delay time level (1-18 for predefined levels).
    #[inline]
    pub fn delay_level(mut self, level: i32) -> Self {
        self.properties
            .insert(MessagePropertyKey::DelayTimeLevel, level.to_string());
        self
    }

    /// Sets the delay time in seconds.
    #[inline]
    pub fn delay_secs(mut self, secs: u64) -> Self {
        self.properties
            .insert(MessagePropertyKey::DelayTimeSec, secs.to_string());
        self
    }

    /// Sets the delay time in milliseconds.
    #[inline]
    pub fn delay_millis(mut self, millis: u64) -> Self {
        self.properties
            .insert(MessagePropertyKey::DelayTimeMs, millis.to_string());
        self
    }

    /// Sets the delivery time in milliseconds.
    #[inline]
    pub fn deliver_time_ms(mut self, time_ms: u64) -> Self {
        self.properties
            .insert(MessagePropertyKey::DeliverTimeMs, time_ms.to_string());
        self
    }

    /// Sets whether to wait for store confirmation.
    #[inline]
    pub fn wait_store_msg_ok(mut self, wait: bool) -> Self {
        if !wait {
            self.properties.insert(MessagePropertyKey::WaitStoreMsgOk, "false");
        }
        self
    }

    /// Sets the transaction ID.
    #[inline]
    pub fn transaction_id(mut self, id: impl Into<CheetahString>) -> Self {
        self.transaction_id = Some(id.into());
        self
    }

    /// Sets the buyer ID.
    #[inline]
    pub fn buyer_id(mut self, buyer_id: impl Into<CheetahString>) -> Self {
        self.properties.insert(MessagePropertyKey::BuyerId, buyer_id.into());
        self
    }

    /// Sets the instance ID.
    #[inline]
    pub fn instance_id(mut self, instance_id: impl Into<CheetahString>) -> Self {
        self.properties
            .insert(MessagePropertyKey::InstanceId, instance_id.into());
        self
    }

    /// Sets the correlation ID.
    #[inline]
    pub fn correlation_id(mut self, correlation_id: impl Into<CheetahString>) -> Self {
        self.properties
            .insert(MessagePropertyKey::CorrelationId, correlation_id.into());
        self
    }

    /// Sets the sharding key.
    #[inline]
    pub fn sharding_key(mut self, sharding_key: impl Into<CheetahString>) -> Self {
        self.properties
            .insert(MessagePropertyKey::ShardingKey, sharding_key.into());
        self
    }

    /// Sets the trace switch.
    #[inline]
    pub fn trace_switch(mut self, enabled: bool) -> Self {
        self.properties
            .insert(MessagePropertyKey::TraceSwitch, if enabled { "true" } else { "false" });
        self
    }

    /// Sets a custom property directly using MessagePropertyKey.
    #[inline]
    pub fn property(mut self, key: MessagePropertyKey, value: impl Into<CheetahString>) -> Self {
        self.properties.insert(key, value.into());
        self
    }

    /// Sets a raw property with string key (for advanced use).
    ///
    /// # Errors
    ///
    /// Returns an error if the property name is reserved by the system.
    pub fn raw_property(
        mut self,
        key: impl Into<CheetahString>,
        value: impl Into<CheetahString>,
    ) -> RocketMQResult<Self> {
        let key = key.into();
        let value = value.into();

        // Validate not a reserved key
        if crate::common::message::STRING_HASH_SET.contains(key.as_str()) {
            return Err(RocketMQError::InvalidProperty(format!(
                "The Property<{key}> is used by system, input another please"
            )));
        }

        self.properties.as_map_mut().insert(key, value);
        Ok(self)
    }

    /// Builds the message.
    ///
    /// # Errors
    ///
    /// Returns an error if required fields (topic) are not set.
    pub fn build(self) -> RocketMQResult<super::message_single::Message> {
        let topic = self
            .topic
            .ok_or_else(|| RocketMQError::InvalidProperty("Topic is required for message".to_string()))?;

        Ok(super::message_single::Message::from_builder(
            topic,
            self.body,
            self.properties,
            self.flag,
            self.transaction_id,
        ))
    }

    /// Builds the message, panicking if validation fails.
    ///
    /// This is convenient for test code where you know the message is valid.
    #[track_caller]
    pub fn build_unchecked(self) -> super::message_single::Message {
        self.build().expect("message validation failed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_basic() {
        let msg = MessageBuilder::new()
            .topic("test-topic")
            .body_slice(b"hello world")
            .build_unchecked();

        assert_eq!(msg.topic().as_str(), "test-topic");
        assert_eq!(msg.body_slice(), b"hello world");
    }

    #[test]
    fn test_builder_with_tags_and_keys() {
        let msg = MessageBuilder::new()
            .topic("test-topic")
            .body_slice(b"test")
            .tags("tag1")
            .keys(vec!["key1".to_string(), "key2".to_string()])
            .build_unchecked();

        assert_eq!(msg.tags(), Some("tag1"));
        assert_eq!(msg.keys(), Some(vec!["key1".to_string(), "key2".to_string()]));
    }

    #[test]
    fn test_builder_with_delay() {
        let msg = MessageBuilder::new()
            .topic("test-topic")
            .body_slice(b"test")
            .delay_level(3)
            .build_unchecked();

        assert_eq!(msg.get_delay_time_level(), 3);
    }

    #[test]
    fn test_builder_missing_topic() {
        let result = MessageBuilder::new().body_slice(b"test").build();

        assert!(result.is_err());
    }

    #[test]
    fn test_builder_with_transaction_id() {
        let msg = MessageBuilder::new()
            .topic("test-topic")
            .body_slice(b"test")
            .transaction_id("tx-123")
            .build_unchecked();

        assert_eq!(msg.transaction_id(), Some("tx-123"));
    }
}
