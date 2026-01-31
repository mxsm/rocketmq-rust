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

use std::fmt;
use std::fmt::Display;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

use crate::common::message::message_decoder;
use crate::common::message::message_single::Message;
use crate::common::mix_all;

/// Batch message metadata
#[derive(Clone, Debug)]
struct BatchMetadata {
    /// Shared topic for all messages
    topic: CheetahString,

    /// Whether to wait for storage completion
    wait_store_msg_ok: bool,
}

/// Batch message container
///
/// Used for sending multiple messages in batch. Requires all messages to have the same topic and
/// send properties.
///
/// # Constraints
///
/// - All messages must have the same topic
/// - All messages must have the same `wait_store_msg_ok` property
/// - Delayed messages (delay_time_level > 0) are not supported
/// - Retry topics are not supported
///
/// # Examples
///
/// ```rust
/// use rocketmq_common::common::message::message_batch_v2::MessageBatchV2;
/// use rocketmq_common::common::message::message_single::Message;
///
/// let msg1 = Message::builder()
///     .topic("TopicTest")
///     .body_slice(b"Hello")
///     .build_unchecked();
/// let msg2 = Message::builder()
///     .topic("TopicTest")
///     .body_slice(b"World")
///     .build_unchecked();
///
/// let batch = MessageBatchV2::new(vec![msg1, msg2]).unwrap();
/// assert_eq!(batch.len(), 2);
/// assert_eq!(batch.topic().as_str(), "TopicTest");
/// ```
#[derive(Clone, Debug)]
pub struct MessageBatchV2 {
    /// Batch message metadata
    metadata: BatchMetadata,

    /// Batch message list
    messages: Vec<Message>,
}

impl MessageBatchV2 {
    /// Creates a batch message
    ///
    /// # Errors
    ///
    /// - Message list is empty
    /// - Messages have different topics
    /// - Contains delayed messages
    /// - Contains retry topics
    /// - `wait_store_msg_ok` properties are inconsistent
    pub fn new(messages: Vec<Message>) -> RocketMQResult<Self> {
        if messages.is_empty() {
            return Err(RocketMQError::illegal_argument("MessageBatch cannot be empty"));
        }

        // Validate all messages
        let first = &messages[0];
        Self::validate_batch_messages(&messages, first)?;

        let metadata = BatchMetadata {
            topic: first.topic().clone(),
            wait_store_msg_ok: first.is_wait_store_msg_ok(),
        };

        Ok(Self { metadata, messages })
    }

    /// Validates batch message consistency
    fn validate_batch_messages(messages: &[Message], first: &Message) -> RocketMQResult<()> {
        for message in messages {
            // Delayed messages not supported
            if message.get_delay_time_level() > 0 {
                return Err(RocketMQError::illegal_argument(
                    "TimeDelayLevel is not supported for batching",
                ));
            }

            // Retry topics not supported
            if message.topic().starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
                return Err(RocketMQError::illegal_argument(
                    "Retry group topic is not supported for batching",
                ));
            }

            // Topics must be consistent
            if first.topic() != message.topic() {
                return Err(RocketMQError::illegal_argument(
                    "The topic of the messages in one batch should be the same",
                ));
            }

            // wait_store_msg_ok must be consistent
            if first.is_wait_store_msg_ok() != message.is_wait_store_msg_ok() {
                return Err(RocketMQError::illegal_argument(
                    "The waitStoreMsgOK of the messages in one batch should be the same",
                ));
            }
        }
        Ok(())
    }

    /// Gets the batch message topic
    #[inline]
    pub fn topic(&self) -> &CheetahString {
        &self.metadata.topic
    }

    /// Whether to wait for storage completion
    #[inline]
    pub fn wait_store_msg_ok(&self) -> bool {
        self.metadata.wait_store_msg_ok
    }

    /// Gets the number of batch messages
    #[inline]
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// Checks if the batch is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Iterates over all messages
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &Message> {
        self.messages.iter()
    }

    /// Gets a reference to the message list
    #[inline]
    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    /// Encodes to network transmission format
    ///
    /// Encodes all messages to a byte stream for network transmission
    pub fn encode(&self) -> Bytes {
        message_decoder::encode_messages(&self.messages)
    }

    /// Consumes the batch and returns the internal message list
    #[inline]
    pub fn into_messages(self) -> Vec<Message> {
        self.messages
    }
}

impl IntoIterator for MessageBatchV2 {
    type IntoIter = std::vec::IntoIter<Message>;
    type Item = Message;

    fn into_iter(self) -> Self::IntoIter {
        self.messages.into_iter()
    }
}

impl<'a> IntoIterator for &'a MessageBatchV2 {
    type IntoIter = std::slice::Iter<'a, Message>;
    type Item = &'a Message;

    fn into_iter(self) -> Self::IntoIter {
        self.messages.iter()
    }
}

impl Display for MessageBatchV2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MessageBatchV2 {{ topic: {}, count: {}, wait_store_msg_ok: {} }}",
            self.metadata.topic,
            self.messages.len(),
            self.metadata.wait_store_msg_ok
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::common::message::MessageTrait;

    fn create_test_message(topic: &str) -> Message {
        Message::builder()
            .topic(topic)
            .body_slice(b"test body")
            .build_unchecked()
    }

    #[test]
    fn test_batch_creation_ok() {
        let messages = vec![create_test_message("topic1"), create_test_message("topic1")];

        let batch = MessageBatchV2::new(messages).unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.topic().as_str(), "topic1");
        assert!(!batch.is_empty());
    }

    #[test]
    fn test_batch_empty_error() {
        let messages: Vec<Message> = vec![];
        let result = MessageBatchV2::new(messages);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_batch_different_topic_error() {
        let messages = vec![
            create_test_message("topic1"),
            create_test_message("topic2"), // Different topic
        ];

        let result = MessageBatchV2::new(messages);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("topic"));
    }

    #[test]
    fn test_batch_delay_message_error() {
        let msg1 = create_test_message("topic1");
        let mut msg2 = create_test_message("topic1");
        msg2.set_delay_time_level(1); // Set delay level

        let messages = vec![msg1, msg2];
        let result = MessageBatchV2::new(messages);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("TimeDelayLevel"));
    }

    #[test]
    fn test_batch_retry_group_error() {
        let messages = vec![
            create_test_message("topic1"),
            create_test_message("%RETRY%topic1"), // Retry group
        ];

        let result = MessageBatchV2::new(messages);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Retry"));
    }

    #[test]
    fn test_batch_different_wait_store_msg_ok_error() {
        let mut msg1 = create_test_message("topic1");
        let mut msg2 = create_test_message("topic1");
        msg1.set_wait_store_msg_ok(true);
        msg2.set_wait_store_msg_ok(false); // Different waitStoreMsgOK

        let messages = vec![msg1, msg2];
        let result = MessageBatchV2::new(messages);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("waitStoreMsgOK"));
    }

    #[test]
    fn test_batch_encode() {
        let messages = vec![create_test_message("topic1"), create_test_message("topic1")];

        let batch = MessageBatchV2::new(messages).unwrap();
        let encoded = batch.encode();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_batch_iterator_slice() {
        let messages = vec![
            create_test_message("topic1"),
            create_test_message("topic1"),
            create_test_message("topic1"),
        ];

        let batch = MessageBatchV2::new(messages).unwrap();
        let count = batch.iter().count();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_batch_into_iterator() {
        let messages = vec![
            create_test_message("topic1"),
            create_test_message("topic1"),
            create_test_message("topic1"),
        ];

        let batch = MessageBatchV2::new(messages).unwrap();
        let count = batch.into_iter().count();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_batch_into_iterator_ref() {
        let messages = vec![
            create_test_message("topic1"),
            create_test_message("topic1"),
            create_test_message("topic1"),
        ];

        let batch = MessageBatchV2::new(messages).unwrap();
        let count = (&batch).into_iter().count();
        assert_eq!(count, 3);

        // Can still use batch after borrowing
        assert_eq!(batch.len(), 3);
    }

    #[test]
    fn test_batch_properties_inherited() {
        let mut msg1 = create_test_message("topic1");
        msg1.set_wait_store_msg_ok(true);

        let messages = vec![msg1.clone(), msg1];
        let batch = MessageBatchV2::new(messages).unwrap();

        assert_eq!(batch.topic().as_str(), "topic1");
        assert!(batch.wait_store_msg_ok());
    }

    #[test]
    fn test_batch_display_format() {
        let messages = vec![create_test_message("topic1"), create_test_message("topic1")];

        let batch = MessageBatchV2::new(messages).unwrap();
        let display = format!("{}", batch);
        assert!(display.contains("MessageBatchV2"));
        assert!(display.contains("topic1"));
        assert!(display.contains("count: 2"));
    }

    #[test]
    fn test_batch_into_messages() {
        let messages = vec![create_test_message("topic1"), create_test_message("topic1")];

        let batch = MessageBatchV2::new(messages).unwrap();
        let extracted = batch.into_messages();
        assert_eq!(extracted.len(), 2);
    }
}
