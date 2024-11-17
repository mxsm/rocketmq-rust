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
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::path::Iter;

use bytes::Bytes;
use cheetah_string::CheetahString;

use crate::common::message::message_decoder;
use crate::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use crate::common::message::message_single::Message;
use crate::common::message::MessageTrait;
use crate::common::mix_all;
use crate::error::Error::UnsupportedOperationException;
use crate::Result;

#[derive(Clone, Default, Debug)]
pub struct MessageBatch {
    ///`final_message` stores the batch-encoded messages.
    pub final_message: Message,

    ///`messages` stores the batch of initialized messages.
    pub messages: Option<Vec<Message>>,
}

impl Iterator for MessageBatch {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.messages {
            Some(messages) => {
                if let Some(message) = messages.iter().next() {
                    return Some(message.clone());
                }
                None
            }
            None => None,
        }
    }
}

impl MessageBatch {
    pub fn encode(&self) -> Bytes {
        message_decoder::encode_messages(self.messages.as_ref().unwrap())
    }

    pub fn generate_from_vec(messages: Vec<Message>) -> Result<MessageBatch> {
        if messages.is_empty() {
            return Err(UnsupportedOperationException(
                "MessageBatch::generate_from_vec: messages is empty".to_string(),
            ));
        }
        let mut first: Option<&Message> = None;
        for message in &messages {
            if message.get_delay_time_level() > 0 {
                return Err(UnsupportedOperationException(
                    "TimeDelayLevel is not supported for batching".to_string(),
                ));
            }
            if message
                .get_topic()
                .starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX)
            {
                return Err(UnsupportedOperationException(
                    "Retry group topic is not supported for batching".to_string(),
                ));
            }
            if first.is_none() {
                first = Some(message);
            } else {
                let first_message = first.unwrap();
                if first_message.get_topic() != message.get_topic() {
                    return Err(UnsupportedOperationException(
                        "The topic of the messages in one batch should be the same".to_string(),
                    ));
                }
                if first_message.is_wait_store_msg_ok() != message.is_wait_store_msg_ok() {
                    return Err(UnsupportedOperationException(
                        "The waitStoreMsgOK of the messages in one batch should the same"
                            .to_string(),
                    ));
                }
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
            messages: Some(messages),
        })
    }
}

impl fmt::Display for MessageBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let messages_str = match &self.messages {
            Some(messages) => messages
                .iter()
                .map(|msg| msg.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            None => "None".to_string(),
        };

        write!(
            f,
            "MessageBatch {{ final_message: {}, messages: {} }}",
            self.final_message, messages_str
        )
    }
}

#[allow(unused_variables)]
impl MessageTrait for MessageBatch {
    fn put_property(&mut self, key: CheetahString, value: CheetahString) {
        self.final_message.properties.insert(key, value);
    }

    fn clear_property(&mut self, name: &str) {
        self.final_message.properties.remove(name);
    }

    fn get_property(&self, name: &CheetahString) -> Option<CheetahString> {
        self.final_message.properties.get(name).cloned()
    }

    fn get_topic(&self) -> &CheetahString {
        &self.final_message.topic
    }

    fn set_topic(&mut self, topic: CheetahString) {
        self.final_message.topic = topic;
    }

    fn get_flag(&self) -> i32 {
        self.final_message.flag
    }

    fn set_flag(&mut self, flag: i32) {
        self.final_message.flag = flag;
    }

    fn get_body(&self) -> Option<&Bytes> {
        self.final_message.body.as_ref()
    }

    fn set_body(&mut self, body: Bytes) {
        self.final_message.body = Some(body);
    }

    fn get_properties(&self) -> &HashMap<CheetahString, CheetahString> {
        &self.final_message.properties
    }

    fn set_properties(&mut self, properties: HashMap<CheetahString, CheetahString>) {
        self.final_message.properties = properties;
    }

    fn get_transaction_id(&self) -> &CheetahString {
        self.final_message
            .transaction_id
            .as_ref()
            .expect("transaction_id is None")
    }

    fn set_transaction_id(&mut self, transaction_id: CheetahString) {
        self.final_message.transaction_id = Some(transaction_id);
    }

    fn get_compressed_body_mut(&mut self) -> &mut Option<Bytes> {
        &mut self.final_message.compressed_body
    }

    fn get_compressed_body(&self) -> Option<&Bytes> {
        self.final_message.compressed_body.as_ref()
    }

    fn set_compressed_body_mut(&mut self, compressed_body: Bytes) {
        self.final_message.compressed_body = Some(compressed_body);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
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
