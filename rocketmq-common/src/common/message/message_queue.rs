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

use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;

use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug)]
pub struct MessageQueue {
    topic: String,
    broker_name: String,
    queue_id: i32,
}

impl MessageQueue {
    pub fn new() -> Self {
        MessageQueue {
            topic: String::new(),
            broker_name: String::new(),
            queue_id: 0,
        }
    }

    pub fn from_other(other: &MessageQueue) -> Self {
        MessageQueue {
            topic: other.topic.clone(),
            broker_name: other.broker_name.clone(),
            queue_id: other.queue_id,
        }
    }

    pub fn with_params(topic: &str, broker_name: &str, queue_id: i32) -> Self {
        MessageQueue {
            topic: topic.to_string(),
            broker_name: broker_name.to_string(),
            queue_id,
        }
    }

    pub fn get_topic(&self) -> &str {
        &self.topic
    }

    pub fn set_topic(&mut self, topic: &str) {
        self.topic = topic.to_string();
    }

    pub fn get_broker_name(&self) -> &str {
        &self.broker_name
    }

    pub fn set_broker_name(&mut self, broker_name: &str) {
        self.broker_name = broker_name.to_string();
    }

    pub fn get_queue_id(&self) -> i32 {
        self.queue_id
    }

    pub fn set_queue_id(&mut self, queue_id: i32) {
        self.queue_id = queue_id;
    }
}

impl PartialEq for MessageQueue {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic
            && self.broker_name == other.broker_name
            && self.queue_id == other.queue_id
    }
}

impl Eq for MessageQueue {}

impl Hash for MessageQueue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.topic.hash(state);
        self.broker_name.hash(state);
        self.queue_id.hash(state);
    }
}

impl Ord for MessageQueue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.topic
            .cmp(&other.topic)
            .then_with(|| self.broker_name.cmp(&other.broker_name))
            .then_with(|| self.queue_id.cmp(&other.queue_id))
    }
}

impl PartialOrd for MessageQueue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for MessageQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MessageQueue [topic={}, broker_name={}, queue_id={}]",
            self.topic, self.broker_name, self.queue_id
        )
    }
}

#[cfg(test)]
mod message_queue_tests {
    use super::*;

    #[test]
    fn new_creates_empty_message_queue() {
        let mq = MessageQueue::new();
        assert_eq!(mq.topic, "");
        assert_eq!(mq.broker_name, "");
        assert_eq!(mq.queue_id, 0);
    }

    #[test]
    fn from_other_copies_all_fields() {
        let original = MessageQueue::with_params("topic1", "broker1", 1);
        let copy = MessageQueue::from_other(&original);
        assert_eq!(copy.topic, "topic1");
        assert_eq!(copy.broker_name, "broker1");
        assert_eq!(copy.queue_id, 1);
    }

    #[test]
    fn with_params_sets_all_fields() {
        let mq = MessageQueue::with_params("topic2", "broker2", 2);
        assert_eq!(mq.topic, "topic2");
        assert_eq!(mq.broker_name, "broker2");
        assert_eq!(mq.queue_id, 2);
    }

    #[test]
    fn set_topic_updates_topic() {
        let mut mq = MessageQueue::new();
        mq.set_topic("new_topic");
        assert_eq!(mq.topic, "new_topic");
    }

    #[test]
    fn set_broker_name_updates_broker_name() {
        let mut mq = MessageQueue::new();
        mq.set_broker_name("new_broker");
        assert_eq!(mq.broker_name, "new_broker");
    }

    #[test]
    fn set_queue_id_updates_queue_id() {
        let mut mq = MessageQueue::new();
        mq.set_queue_id(3);
        assert_eq!(mq.queue_id, 3);
    }

    #[test]
    fn equals_self() {
        let mq1 = MessageQueue::with_params("topic", "broker", 1);
        assert!(mq1.eq(&mq1));
    }

    #[test]
    fn not_equal_different_topic() {
        let mq1 = MessageQueue::with_params("topic1", "broker", 1);
        let mq2 = MessageQueue::with_params("topic2", "broker", 1);
        assert!(!mq1.eq(&mq2));
    }

    #[test]
    fn not_equal_different_broker_name() {
        let mq1 = MessageQueue::with_params("topic", "broker1", 1);
        let mq2 = MessageQueue::with_params("topic", "broker2", 1);
        assert!(!mq1.eq(&mq2));
    }
}
