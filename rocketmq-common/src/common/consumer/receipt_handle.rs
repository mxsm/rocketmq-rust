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

//! Receipt handle implementation for consumer message acknowledgment
//!
//! This module provides the `ReceiptHandle` type which represents a handle
//! for acknowledging messages in a pop consumer scenario.

use std::fmt;

use cheetah_string::CheetahString;

use crate::common::key_builder::KeyBuilder;
use crate::common::message::MessageConst;
use crate::TimeUtils;

/// Topic type constants
pub const NORMAL_TOPIC: &str = "0";
pub const RETRY_TOPIC: &str = "1";
pub const RETRY_TOPIC_V2: &str = "2";

/// Receipt handle for consumer message acknowledgment
///
/// Contains metadata about a consumed message that needs to be acknowledged,
/// including timing information, queue details, and broker information.
///
/// # Example
/// ```
/// use rocketmq_common::common::consumer::receipt_handle::ReceiptHandle;
///
/// let handle = ReceiptHandle::builder()
///     .start_offset(100)
///     .retrieve_time(1000000)
///     .invisible_time(30000)
///     .revive_queue_id(0)
///     .topic_type("0")
///     .broker_name("broker-a")
///     .queue_id(1)
///     .offset(200)
///     .commit_log_offset(5000)
///     .receipt_handle("encoded_handle")
///     .build();
///
/// assert_eq!(handle.queue_id(), 1);
/// assert_eq!(handle.broker_name(), "broker-a");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptHandle {
    start_offset: i64,
    retrieve_time: i64,
    invisible_time: i64,
    next_visible_time: i64,
    revive_queue_id: i32,
    topic_type: CheetahString,
    broker_name: CheetahString,
    queue_id: i32,
    offset: i64,
    commit_log_offset: i64,
    receipt_handle: CheetahString,
}

impl ReceiptHandle {
    /// Create a new builder for constructing a ReceiptHandle
    ///
    /// # Returns
    /// A new `ReceiptHandleBuilder` instance
    ///
    /// # Example
    /// ```
    /// use rocketmq_common::common::consumer::receipt_handle::ReceiptHandle;
    ///
    /// let builder = ReceiptHandle::builder();
    /// ```
    pub fn builder() -> ReceiptHandleBuilder {
        ReceiptHandleBuilder::default()
    }

    /// Encode the receipt handle into a string representation
    ///
    /// Encodes all fields (except next_visible_time and receipt_handle itself)
    /// into a separator-delimited string.
    ///
    /// # Returns
    /// The encoded string representation
    ///
    /// # Example
    /// ```
    /// use rocketmq_common::common::consumer::receipt_handle::ReceiptHandle;
    ///
    /// let handle = ReceiptHandle::builder()
    ///     .start_offset(100)
    ///     .retrieve_time(1000000)
    ///     .invisible_time(30000)
    ///     .revive_queue_id(0)
    ///     .topic_type("0")
    ///     .broker_name("broker-a")
    ///     .queue_id(1)
    ///     .offset(200)
    ///     .commit_log_offset(5000)
    ///     .receipt_handle("")
    ///     .build();
    ///
    /// let encoded = handle.encode();
    /// assert!(encoded.contains("broker-a"));
    /// ```
    pub fn encode(&self) -> String {
        format!(
            "{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
            self.start_offset,
            MessageConst::KEY_SEPARATOR,
            self.retrieve_time,
            MessageConst::KEY_SEPARATOR,
            self.invisible_time,
            MessageConst::KEY_SEPARATOR,
            self.revive_queue_id,
            MessageConst::KEY_SEPARATOR,
            self.topic_type,
            MessageConst::KEY_SEPARATOR,
            self.broker_name,
            MessageConst::KEY_SEPARATOR,
            self.queue_id,
            MessageConst::KEY_SEPARATOR,
            self.offset,
            MessageConst::KEY_SEPARATOR,
            self.commit_log_offset
        )
    }

    /// Check if the receipt handle has expired
    ///
    /// A handle is expired if the next visible time is less than or equal
    /// to the current system time.
    ///
    /// # Returns
    /// `true` if expired, `false` otherwise
    ///
    /// # Example
    /// ```
    /// use rocketmq_common::common::consumer::receipt_handle::ReceiptHandle;
    ///
    /// let handle = ReceiptHandle::builder()
    ///     .start_offset(100)
    ///     .retrieve_time(1000)
    ///     .invisible_time(1000)
    ///     .revive_queue_id(0)
    ///     .topic_type("0")
    ///     .broker_name("broker-a")
    ///     .queue_id(1)
    ///     .offset(200)
    ///     .commit_log_offset(5000)
    ///     .receipt_handle("")
    ///     .build();
    ///
    /// // This handle will likely be expired since retrieve_time is very old
    /// let expired = handle.is_expired();
    /// ```
    pub fn is_expired(&self) -> bool {
        self.next_visible_time <= TimeUtils::get_current_millis() as i64
    }

    /// Decode a receipt handle from its string representation
    ///
    /// Parses a separator-delimited string into a ReceiptHandle instance.
    ///
    /// # Arguments
    /// * `receipt_handle` - The encoded receipt handle string
    ///
    /// # Returns
    /// * `Ok(ReceiptHandle)` - Successfully decoded handle
    /// * `Err(String)` - Parse error with description
    ///
    /// # Errors
    /// Returns an error if:
    /// - The string has fewer than 8 fields
    /// - Any numeric field fails to parse
    ///
    /// # Example
    /// ```
    /// use rocketmq_common::common::consumer::receipt_handle::ReceiptHandle;
    ///
    /// let encoded = "100 1000000 30000 0 0 broker-a 1 200 5000";
    /// let handle = ReceiptHandle::decode(encoded).unwrap();
    /// assert_eq!(handle.start_offset(), 100);
    /// assert_eq!(handle.broker_name(), "broker-a");
    /// ```
    pub fn decode(receipt_handle: &str) -> Result<Self, String> {
        let data_list: Vec<&str> = receipt_handle.split(MessageConst::KEY_SEPARATOR).collect();

        if data_list.len() < 8 {
            return Err(format!("Parse failed, dataList size {}", data_list.len()));
        }

        let start_offset = data_list[0]
            .parse::<i64>()
            .map_err(|e| format!("Failed to parse start_offset: {}", e))?;

        let retrieve_time = data_list[1]
            .parse::<i64>()
            .map_err(|e| format!("Failed to parse retrieve_time: {}", e))?;

        let invisible_time = data_list[2]
            .parse::<i64>()
            .map_err(|e| format!("Failed to parse invisible_time: {}", e))?;

        let revive_queue_id = data_list[3]
            .parse::<i32>()
            .map_err(|e| format!("Failed to parse revive_queue_id: {}", e))?;

        let topic_type = CheetahString::from_string(data_list[4].to_string());
        let broker_name = CheetahString::from_string(data_list[5].to_string());

        let queue_id = data_list[6]
            .parse::<i32>()
            .map_err(|e| format!("Failed to parse queue_id: {}", e))?;

        let offset = data_list[7]
            .parse::<i64>()
            .map_err(|e| format!("Failed to parse offset: {}", e))?;

        let commit_log_offset = if data_list.len() >= 9 {
            data_list[8]
                .parse::<i64>()
                .map_err(|e| format!("Failed to parse commit_log_offset: {}", e))?
        } else {
            -1
        };

        Ok(ReceiptHandle::builder()
            .start_offset(start_offset)
            .retrieve_time(retrieve_time)
            .invisible_time(invisible_time)
            .revive_queue_id(revive_queue_id)
            .topic_type(topic_type.as_str())
            .broker_name(broker_name.as_str())
            .queue_id(queue_id)
            .offset(offset)
            .commit_log_offset(commit_log_offset)
            .receipt_handle(receipt_handle)
            .build())
    }

    /// Get the start offset
    pub fn start_offset(&self) -> i64 {
        self.start_offset
    }

    /// Get the retrieve time (when message was retrieved)
    pub fn retrieve_time(&self) -> i64 {
        self.retrieve_time
    }

    /// Get the invisible time (how long message should be invisible)
    pub fn invisible_time(&self) -> i64 {
        self.invisible_time
    }

    /// Get the next visible time (retrieve_time + invisible_time)
    pub fn next_visible_time(&self) -> i64 {
        self.next_visible_time
    }

    /// Get the revive queue ID
    pub fn revive_queue_id(&self) -> i32 {
        self.revive_queue_id
    }

    /// Get the topic type (NORMAL_TOPIC, RETRY_TOPIC, or RETRY_TOPIC_V2)
    pub fn topic_type(&self) -> &str {
        self.topic_type.as_str()
    }

    /// Get the broker name
    pub fn broker_name(&self) -> &str {
        self.broker_name.as_str()
    }

    /// Get the queue ID
    pub fn queue_id(&self) -> i32 {
        self.queue_id
    }

    /// Get the offset
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Get the commit log offset
    pub fn commit_log_offset(&self) -> i64 {
        self.commit_log_offset
    }

    /// Get the receipt handle string
    pub fn receipt_handle(&self) -> &str {
        self.receipt_handle.as_str()
    }

    /// Check if this is a retry topic
    ///
    /// # Returns
    /// `true` if topic type is RETRY_TOPIC or RETRY_TOPIC_V2
    ///
    /// # Example
    /// ```
    /// use rocketmq_common::common::consumer::receipt_handle::ReceiptHandle;
    /// use rocketmq_common::common::consumer::receipt_handle::RETRY_TOPIC;
    ///
    /// let handle = ReceiptHandle::builder()
    ///     .start_offset(100)
    ///     .retrieve_time(1000000)
    ///     .invisible_time(30000)
    ///     .revive_queue_id(0)
    ///     .topic_type(RETRY_TOPIC)
    ///     .broker_name("broker-a")
    ///     .queue_id(1)
    ///     .offset(200)
    ///     .commit_log_offset(5000)
    ///     .receipt_handle("")
    ///     .build();
    ///
    /// assert!(handle.is_retry_topic());
    /// ```
    pub fn is_retry_topic(&self) -> bool {
        self.topic_type == RETRY_TOPIC || self.topic_type == RETRY_TOPIC_V2
    }

    /// Get the real topic name
    ///
    /// For retry topics, constructs the retry topic name using KeyBuilder.
    /// For normal topics, returns the provided topic unchanged.
    ///
    /// # Arguments
    /// * `topic` - The base topic name
    /// * `group_name` - The consumer group name
    ///
    /// # Returns
    /// The real topic name (which may be a retry topic name)
    ///
    /// # Example
    /// ```
    /// use rocketmq_common::common::consumer::receipt_handle::ReceiptHandle;
    /// use rocketmq_common::common::consumer::receipt_handle::NORMAL_TOPIC;
    ///
    /// let handle = ReceiptHandle::builder()
    ///     .start_offset(100)
    ///     .retrieve_time(1000000)
    ///     .invisible_time(30000)
    ///     .revive_queue_id(0)
    ///     .topic_type(NORMAL_TOPIC)
    ///     .broker_name("broker-a")
    ///     .queue_id(1)
    ///     .offset(200)
    ///     .commit_log_offset(5000)
    ///     .receipt_handle("")
    ///     .build();
    ///
    /// let real_topic = handle.get_real_topic("my-topic", "my-group");
    /// assert_eq!(real_topic, "my-topic");
    /// ```
    pub fn get_real_topic(&self, topic: &str, group_name: &str) -> String {
        if self.topic_type == RETRY_TOPIC {
            return KeyBuilder::build_pop_retry_topic_v1(topic, group_name);
        }
        if self.topic_type == RETRY_TOPIC_V2 {
            return KeyBuilder::build_pop_retry_topic_v2(topic, group_name);
        }
        topic.to_string()
    }
}

impl fmt::Display for ReceiptHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ReceiptHandle{{startOffset={}, retrieveTime={}, invisibleTime={}, nextVisibleTime={}, reviveQueueId={}, \
             topicType={}, brokerName={}, queueId={}, offset={}, commitLogOffset={}, receiptHandle={}}}",
            self.start_offset,
            self.retrieve_time,
            self.invisible_time,
            self.next_visible_time,
            self.revive_queue_id,
            self.topic_type,
            self.broker_name,
            self.queue_id,
            self.offset,
            self.commit_log_offset,
            self.receipt_handle
        )
    }
}

/// Builder for constructing ReceiptHandle instances
///
/// Provides a fluent API for building ReceiptHandle objects with
/// optional field validation.
///
/// # Example
/// ```
/// use rocketmq_common::common::consumer::receipt_handle::ReceiptHandle;
///
/// let handle = ReceiptHandle::builder()
///     .start_offset(100)
///     .retrieve_time(1000000)
///     .invisible_time(30000)
///     .revive_queue_id(0)
///     .topic_type("0")
///     .broker_name("broker-a")
///     .queue_id(1)
///     .offset(200)
///     .commit_log_offset(5000)
///     .receipt_handle("encoded_handle")
///     .build();
/// ```
#[derive(Debug, Clone, Default)]
pub struct ReceiptHandleBuilder {
    start_offset: i64,
    retrieve_time: i64,
    invisible_time: i64,
    revive_queue_id: i32,
    topic_type: CheetahString,
    broker_name: CheetahString,
    queue_id: i32,
    offset: i64,
    commit_log_offset: i64,
    receipt_handle: CheetahString,
}

impl ReceiptHandleBuilder {
    /// Set the start offset
    pub fn start_offset(mut self, start_offset: i64) -> Self {
        self.start_offset = start_offset;
        self
    }

    /// Set the retrieve time
    pub fn retrieve_time(mut self, retrieve_time: i64) -> Self {
        self.retrieve_time = retrieve_time;
        self
    }

    /// Set the invisible time
    pub fn invisible_time(mut self, invisible_time: i64) -> Self {
        self.invisible_time = invisible_time;
        self
    }

    /// Set the revive queue ID
    pub fn revive_queue_id(mut self, revive_queue_id: i32) -> Self {
        self.revive_queue_id = revive_queue_id;
        self
    }

    /// Set the topic type
    pub fn topic_type(mut self, topic_type: &str) -> Self {
        self.topic_type = CheetahString::from_slice(topic_type);
        self
    }

    /// Set the broker name
    pub fn broker_name(mut self, broker_name: &str) -> Self {
        self.broker_name = CheetahString::from_slice(broker_name);
        self
    }

    /// Set the queue ID
    pub fn queue_id(mut self, queue_id: i32) -> Self {
        self.queue_id = queue_id;
        self
    }

    /// Set the offset
    pub fn offset(mut self, offset: i64) -> Self {
        self.offset = offset;
        self
    }

    /// Set the commit log offset
    pub fn commit_log_offset(mut self, commit_log_offset: i64) -> Self {
        self.commit_log_offset = commit_log_offset;
        self
    }

    /// Set the receipt handle string
    pub fn receipt_handle(mut self, receipt_handle: &str) -> Self {
        self.receipt_handle = CheetahString::from_slice(receipt_handle);
        self
    }

    /// Build the ReceiptHandle
    ///
    /// Calculates next_visible_time as retrieve_time + invisible_time.
    ///
    /// # Returns
    /// A new ReceiptHandle instance
    pub fn build(self) -> ReceiptHandle {
        ReceiptHandle {
            start_offset: self.start_offset,
            retrieve_time: self.retrieve_time,
            invisible_time: self.invisible_time,
            next_visible_time: self.retrieve_time + self.invisible_time,
            revive_queue_id: self.revive_queue_id,
            topic_type: self.topic_type,
            broker_name: self.broker_name,
            queue_id: self.queue_id,
            offset: self.offset,
            commit_log_offset: self.commit_log_offset,
            receipt_handle: self.receipt_handle,
        }
    }
}

impl fmt::Display for ReceiptHandleBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ReceiptHandle.ReceiptHandleBuilder(startOffset={}, retrieveTime={}, invisibleTime={}, reviveQueueId={}, \
             topic={}, brokerName={}, queueId={}, offset={}, commitLogOffset={}, receiptHandle={})",
            self.start_offset,
            self.retrieve_time,
            self.invisible_time,
            self.revive_queue_id,
            self.topic_type,
            self.broker_name,
            self.queue_id,
            self.offset,
            self.commit_log_offset,
            self.receipt_handle
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_receipt_handle_builder() {
        let handle = ReceiptHandle::builder()
            .start_offset(100)
            .retrieve_time(1000000)
            .invisible_time(30000)
            .revive_queue_id(0)
            .topic_type(NORMAL_TOPIC)
            .broker_name("broker-a")
            .queue_id(1)
            .offset(200)
            .commit_log_offset(5000)
            .receipt_handle("test-handle")
            .build();

        assert_eq!(handle.start_offset(), 100);
        assert_eq!(handle.retrieve_time(), 1000000);
        assert_eq!(handle.invisible_time(), 30000);
        assert_eq!(handle.next_visible_time(), 1030000);
        assert_eq!(handle.revive_queue_id(), 0);
        assert_eq!(handle.topic_type(), NORMAL_TOPIC);
        assert_eq!(handle.broker_name(), "broker-a");
        assert_eq!(handle.queue_id(), 1);
        assert_eq!(handle.offset(), 200);
        assert_eq!(handle.commit_log_offset(), 5000);
        assert_eq!(handle.receipt_handle(), "test-handle");
    }

    #[test]
    fn test_encode() {
        let handle = ReceiptHandle::builder()
            .start_offset(100)
            .retrieve_time(1000000)
            .invisible_time(30000)
            .revive_queue_id(0)
            .topic_type(NORMAL_TOPIC)
            .broker_name("broker-a")
            .queue_id(1)
            .offset(200)
            .commit_log_offset(5000)
            .receipt_handle("")
            .build();

        let encoded = handle.encode();
        assert_eq!(encoded, "100 1000000 30000 0 0 broker-a 1 200 5000");
    }

    #[test]
    fn test_decode() {
        let encoded = "100 1000000 30000 0 0 broker-a 1 200 5000";
        let handle = ReceiptHandle::decode(encoded).unwrap();

        assert_eq!(handle.start_offset(), 100);
        assert_eq!(handle.retrieve_time(), 1000000);
        assert_eq!(handle.invisible_time(), 30000);
        assert_eq!(handle.revive_queue_id(), 0);
        assert_eq!(handle.topic_type(), NORMAL_TOPIC);
        assert_eq!(handle.broker_name(), "broker-a");
        assert_eq!(handle.queue_id(), 1);
        assert_eq!(handle.offset(), 200);
        assert_eq!(handle.commit_log_offset(), 5000);
    }

    #[test]
    fn test_decode_without_commit_log_offset() {
        let encoded = "100 1000000 30000 0 0 broker-a 1 200";
        let handle = ReceiptHandle::decode(encoded).unwrap();

        assert_eq!(handle.commit_log_offset(), -1);
    }

    #[test]
    fn test_decode_invalid() {
        let encoded = "100 1000000 30000";
        let result = ReceiptHandle::decode(encoded);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_expired() {
        // Create a handle with very old retrieve time
        let handle = ReceiptHandle::builder()
            .start_offset(100)
            .retrieve_time(1000)
            .invisible_time(1000)
            .revive_queue_id(0)
            .topic_type(NORMAL_TOPIC)
            .broker_name("broker-a")
            .queue_id(1)
            .offset(200)
            .commit_log_offset(5000)
            .receipt_handle("")
            .build();

        assert!(handle.is_expired());
    }

    #[test]
    fn test_is_retry_topic() {
        let handle_normal = ReceiptHandle::builder()
            .start_offset(100)
            .retrieve_time(1000000)
            .invisible_time(30000)
            .revive_queue_id(0)
            .topic_type(NORMAL_TOPIC)
            .broker_name("broker-a")
            .queue_id(1)
            .offset(200)
            .commit_log_offset(5000)
            .receipt_handle("")
            .build();

        assert!(!handle_normal.is_retry_topic());

        let handle_retry = ReceiptHandle::builder()
            .start_offset(100)
            .retrieve_time(1000000)
            .invisible_time(30000)
            .revive_queue_id(0)
            .topic_type(RETRY_TOPIC)
            .broker_name("broker-a")
            .queue_id(1)
            .offset(200)
            .commit_log_offset(5000)
            .receipt_handle("")
            .build();

        assert!(handle_retry.is_retry_topic());

        let handle_retry_v2 = ReceiptHandle::builder()
            .start_offset(100)
            .retrieve_time(1000000)
            .invisible_time(30000)
            .revive_queue_id(0)
            .topic_type(RETRY_TOPIC_V2)
            .broker_name("broker-a")
            .queue_id(1)
            .offset(200)
            .commit_log_offset(5000)
            .receipt_handle("")
            .build();

        assert!(handle_retry_v2.is_retry_topic());
    }

    #[test]
    fn test_get_real_topic() {
        let handle_normal = ReceiptHandle::builder()
            .start_offset(100)
            .retrieve_time(1000000)
            .invisible_time(30000)
            .revive_queue_id(0)
            .topic_type(NORMAL_TOPIC)
            .broker_name("broker-a")
            .queue_id(1)
            .offset(200)
            .commit_log_offset(5000)
            .receipt_handle("")
            .build();

        assert_eq!(handle_normal.get_real_topic("my-topic", "my-group"), "my-topic");

        let handle_retry = ReceiptHandle::builder()
            .start_offset(100)
            .retrieve_time(1000000)
            .invisible_time(30000)
            .revive_queue_id(0)
            .topic_type(RETRY_TOPIC)
            .broker_name("broker-a")
            .queue_id(1)
            .offset(200)
            .commit_log_offset(5000)
            .receipt_handle("")
            .build();

        let retry_topic = handle_retry.get_real_topic("my-topic", "my-group");
        assert!(retry_topic.contains("my-topic"));
        assert!(retry_topic.contains("my-group"));
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let original = ReceiptHandle::builder()
            .start_offset(100)
            .retrieve_time(1000000)
            .invisible_time(30000)
            .revive_queue_id(0)
            .topic_type(NORMAL_TOPIC)
            .broker_name("broker-a")
            .queue_id(1)
            .offset(200)
            .commit_log_offset(5000)
            .receipt_handle("original-handle")
            .build();

        let encoded = original.encode();
        let decoded = ReceiptHandle::decode(&encoded).unwrap();

        assert_eq!(original.start_offset(), decoded.start_offset());
        assert_eq!(original.retrieve_time(), decoded.retrieve_time());
        assert_eq!(original.invisible_time(), decoded.invisible_time());
        assert_eq!(original.revive_queue_id(), decoded.revive_queue_id());
        assert_eq!(original.topic_type(), decoded.topic_type());
        assert_eq!(original.broker_name(), decoded.broker_name());
        assert_eq!(original.queue_id(), decoded.queue_id());
        assert_eq!(original.offset(), decoded.offset());
        assert_eq!(original.commit_log_offset(), decoded.commit_log_offset());
    }
}
