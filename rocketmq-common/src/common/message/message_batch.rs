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

use bytes::Bytes;
use cheetah_string::CheetahString;
// Use new unified error system
use rocketmq_error::RocketMQError;

use crate::common::message::message_decoder;
use crate::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use crate::common::message::message_single::Message;
use crate::common::message::MessageTrait;
use crate::common::mix_all;

#[derive(Clone, Default, Debug)]
pub struct MessageBatch {
    ///`final_message` stores the batch-encoded messages.
    pub final_message: Message,

    ///`messages` stores the batch of initialized messages.
    pub messages: Vec<Message>,
}

impl MessageBatch {
    /// Encode all messages in the batch.
    #[inline]
    pub fn encode(&self) -> Bytes {
        message_decoder::encode_messages(&self.messages)
    }

    /// Get an iterator over the messages in the batch.
    #[inline]
    pub fn iter(&self) -> std::slice::Iter<'_, Message> {
        self.messages.iter()
    }

    /// Get the number of messages in the batch.
    #[inline]
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// Check if the batch is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub fn generate_from_vec<M>(messages: Vec<M>) -> rocketmq_error::RocketMQResult<MessageBatch>
    where
        M: MessageTrait,
    {
        if messages.is_empty() {
            return Err(RocketMQError::illegal_argument(
                "MessageBatch::generate_from_vec: messages is empty",
            ));
        }

        let mut message_list = Vec::with_capacity(messages.len());
        for msg in messages {
            if let Some(m) = msg.as_any().downcast_ref::<Message>() {
                message_list.push(m.clone());
            } else {
                let mut m = Message::default();
                m.set_topic(msg.get_topic().clone());
                if let Some(body) = msg.get_body() {
                    m.set_body(body.clone());
                }
                m.set_flag(msg.get_flag());
                if let Some(transaction_id) = msg.get_transaction_id() {
                    m.set_transaction_id(transaction_id.clone());
                }
                m.set_properties(msg.get_properties().clone());
                message_list.push(m);
            }
        }

        let mut first: Option<&Message> = None;
        for message in &message_list {
            if message.get_delay_time_level() > 0 {
                return Err(RocketMQError::illegal_argument(
                    "TimeDelayLevel is not supported for batching",
                ));
            }
            if message.get_topic().starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
                return Err(RocketMQError::illegal_argument(
                    "Retry group topic is not supported for batching",
                ));
            }

            if let Some(first_message) = first {
                if first_message.get_topic() != message.get_topic() {
                    return Err(RocketMQError::illegal_argument(
                        "The topic of the messages in one batch should be the same",
                    ));
                }
                if first_message.is_wait_store_msg_ok() != message.is_wait_store_msg_ok() {
                    return Err(RocketMQError::illegal_argument(
                        "The waitStoreMsgOK of the messages in one batch should the same",
                    ));
                }
            } else {
                first = Some(message);
            }
        }
        let first = first.unwrap();
        let mut final_message = Message {
            topic: first.topic.clone(),
            ..Message::default()
        };
        final_message.set_wait_store_msg_ok(first.is_wait_store_msg_ok());
        Ok(MessageBatch {
            final_message,
            messages: message_list,
        })
    }
}

impl IntoIterator for MessageBatch {
    type Item = Message;
    type IntoIter = std::vec::IntoIter<Message>;

    fn into_iter(self) -> Self::IntoIter {
        self.messages.into_iter()
    }
}

impl<'a> IntoIterator for &'a MessageBatch {
    type Item = &'a Message;
    type IntoIter = std::slice::Iter<'a, Message>;

    fn into_iter(self) -> Self::IntoIter {
        self.messages.iter()
    }
}

impl fmt::Display for MessageBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let messages_str = self
            .messages
            .iter()
            .map(|msg| msg.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        write!(
            f,
            "MessageBatch {{ final_message: {}, messages: {} }}",
            self.final_message, messages_str
        )
    }
}

#[allow(unused_variables)]
impl MessageTrait for MessageBatch {
    #[inline]
    fn put_property(&mut self, key: CheetahString, value: CheetahString) {
        self.final_message.properties.insert(key, value);
    }

    #[inline]
    fn clear_property(&mut self, name: &str) {
        self.final_message.properties.remove(name);
    }

    #[inline]
    fn get_property(&self, name: &CheetahString) -> Option<CheetahString> {
        self.final_message.properties.get(name).cloned()
    }

    #[inline]
    fn get_topic(&self) -> &CheetahString {
        &self.final_message.topic
    }

    #[inline]
    fn set_topic(&mut self, topic: CheetahString) {
        self.final_message.topic = topic;
    }

    #[inline]
    fn get_flag(&self) -> i32 {
        self.final_message.flag
    }

    #[inline]
    fn set_flag(&mut self, flag: i32) {
        self.final_message.flag = flag;
    }

    #[inline]
    fn get_body(&self) -> Option<&Bytes> {
        self.final_message.body.as_ref()
    }

    #[inline]
    fn set_body(&mut self, body: Bytes) {
        self.final_message.body = Some(body);
    }

    #[inline]
    fn get_properties(&self) -> &HashMap<CheetahString, CheetahString> {
        &self.final_message.properties
    }

    #[inline]
    fn set_properties(&mut self, properties: HashMap<CheetahString, CheetahString>) {
        self.final_message.properties = properties;
    }

    #[inline]
    fn get_transaction_id(&self) -> Option<&CheetahString> {
        self.final_message.transaction_id.as_ref()
    }

    #[inline]
    fn set_transaction_id(&mut self, transaction_id: CheetahString) {
        self.final_message.transaction_id = Some(transaction_id);
    }

    #[inline]
    fn get_compressed_body_mut(&mut self) -> &mut Option<Bytes> {
        &mut self.final_message.compressed_body
    }

    #[inline]
    fn get_compressed_body(&self) -> Option<&Bytes> {
        self.final_message.compressed_body.as_ref()
    }

    #[inline]
    fn set_compressed_body_mut(&mut self, compressed_body: Bytes) {
        self.final_message.compressed_body = Some(compressed_body);
    }

    #[inline]
    fn take_body(&mut self) -> Option<Bytes> {
        self.final_message.take_body()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use cheetah_string::CheetahString;

    use super::*;

    fn create_test_message(topic: &str) -> Message {
        let mut msg = Message::default();
        msg.set_topic(CheetahString::from_string(topic.to_string()));
        msg.set_body(Bytes::from_static(b"test body"));
        msg
    }

    #[test]
    fn test_generate_from_vec_ok() {
        let messages = vec![create_test_message("topic1"), create_test_message("topic1")];
        let result = MessageBatch::generate_from_vec(messages);
        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
    }

    #[test]
    fn test_generate_from_vec_empty() {
        let messages: Vec<Message> = vec![];
        let result = MessageBatch::generate_from_vec(messages);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_generate_from_vec_different_topic() {
        let messages = vec![
            create_test_message("topic1"),
            create_test_message("topic2"), // Different topic
        ];
        let result = MessageBatch::generate_from_vec(messages);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("topic"));
    }

    #[test]
    fn test_generate_from_vec_delay_message() {
        let msg1 = create_test_message("topic1");
        let mut msg2 = create_test_message("topic1");
        msg2.set_delay_time_level(1); // Set delay level

        let messages = vec![msg1, msg2];
        let result = MessageBatch::generate_from_vec(messages);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("TimeDelayLevel"));
    }

    #[test]
    fn test_generate_from_vec_retry_group() {
        let messages = vec![
            create_test_message("topic1"),
            create_test_message("%RETRY%topic1"), // Retry group
        ];
        let result = MessageBatch::generate_from_vec(messages);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Retry"));
    }

    #[test]
    fn test_generate_from_vec_different_wait_store_msg_ok() {
        let mut msg1 = create_test_message("topic1");
        let mut msg2 = create_test_message("topic1");
        msg1.set_wait_store_msg_ok(true);
        msg2.set_wait_store_msg_ok(false); // Different waitStoreMsgOK

        let messages = vec![msg1, msg2];
        let result = MessageBatch::generate_from_vec(messages);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("waitStoreMsgOK"));
    }

    #[test]
    fn test_encode() {
        let messages = vec![create_test_message("topic1"), create_test_message("topic1")];
        let batch = MessageBatch::generate_from_vec(messages).unwrap();
        let encoded = batch.encode();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_iterator_slice() {
        let messages = vec![
            create_test_message("topic1"),
            create_test_message("topic1"),
            create_test_message("topic1"),
        ];
        let batch = MessageBatch::generate_from_vec(messages).unwrap();

        let count = batch.iter().count();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_into_iterator() {
        let messages = vec![
            create_test_message("topic1"),
            create_test_message("topic1"),
            create_test_message("topic1"),
        ];
        let batch = MessageBatch::generate_from_vec(messages).unwrap();

        let count = batch.into_iter().count();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_into_iterator_ref() {
        let messages = vec![
            create_test_message("topic1"),
            create_test_message("topic1"),
            create_test_message("topic1"),
        ];
        let batch = MessageBatch::generate_from_vec(messages).unwrap();

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
        let batch = MessageBatch::generate_from_vec(messages).unwrap();

        assert_eq!(batch.final_message.get_topic().as_str(), "topic1");
        assert!(batch.final_message.is_wait_store_msg_ok());
    }

    #[test]
    fn test_display_format() {
        let messages = vec![create_test_message("topic1"), create_test_message("topic1")];
        let batch = MessageBatch::generate_from_vec(messages).unwrap();

        let display = format!("{}", batch);
        assert!(display.contains("MessageBatch"));
        assert!(display.contains("topic1"));
    }
}

#[derive(Debug, Default)]
pub struct MessageExtBatch {
    pub message_ext_broker_inner: MessageExtBrokerInner,
    pub is_inner_batch: bool,
    pub encoded_buff: Option<bytes::BytesMut>,
}

impl MessageExtBatch {
    pub fn wrap(&self) -> Option<Bytes> {
        self.message_ext_broker_inner.body()
    }

    pub fn get_tags(&self) -> Option<CheetahString> {
        self.message_ext_broker_inner.get_tags()
    }
}
