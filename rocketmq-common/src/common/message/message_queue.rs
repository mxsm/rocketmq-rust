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

use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageQueue {
    topic: CheetahString,
    broker_name: CheetahString,
    queue_id: i32,
}

impl MessageQueue {
    pub fn new() -> Self {
        MessageQueue {
            topic: CheetahString::new(),
            broker_name: CheetahString::new(),
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

    pub fn from_parts(topic: impl Into<CheetahString>, broker_name: impl Into<CheetahString>, queue_id: i32) -> Self {
        MessageQueue {
            topic: topic.into(),
            broker_name: broker_name.into(),
            queue_id,
        }
    }

    #[inline]
    pub fn get_topic(&self) -> &str {
        &self.topic
    }

    #[inline]
    pub fn get_topic_cs(&self) -> &CheetahString {
        &self.topic
    }

    #[inline]
    pub fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }

    #[inline]
    pub fn get_broker_name(&self) -> &CheetahString {
        &self.broker_name
    }

    #[inline]
    pub fn set_broker_name(&mut self, broker_name: CheetahString) {
        self.broker_name = broker_name;
    }

    #[inline]
    pub fn get_queue_id(&self) -> i32 {
        self.queue_id
    }

    #[inline]
    pub fn set_queue_id(&mut self, queue_id: i32) {
        self.queue_id = queue_id;
    }
}

impl PartialEq for MessageQueue {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic && self.broker_name == other.broker_name && self.queue_id == other.queue_id
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
        match self.topic.cmp(&other.topic) {
            Ordering::Equal => match self.broker_name.cmp(&other.broker_name) {
                Ordering::Equal => self.queue_id.cmp(&other.queue_id),
                other => other,
            },
            other => other,
        }
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

impl Default for MessageQueue {
    fn default() -> Self {
        MessageQueue::new()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn new_message_queue_has_default_values() {
        let mq = MessageQueue::new();
        assert_eq!(mq.get_topic(), "");
        assert_eq!(mq.get_broker_name(), "");
        assert_eq!(mq.get_queue_id(), 0);
    }

    #[test]
    fn from_other_creates_identical_copy() {
        let mq1 = MessageQueue::from_parts("topic1", "broker1", 1);
        let mq2 = MessageQueue::from_other(&mq1);
        assert_eq!(mq1, mq2);
    }

    #[test]
    fn from_parts_creates_message_queue_with_given_values() {
        let mq = MessageQueue::from_parts("topic1", "broker1", 1);
        assert_eq!(mq.get_topic(), "topic1");
        assert_eq!(mq.get_broker_name(), "broker1");
        assert_eq!(mq.get_queue_id(), 1);
    }

    #[test]
    fn set_topic_updates_topic() {
        let mut mq = MessageQueue::new();
        mq.set_topic(CheetahString::from("new_topic"));
        assert_eq!(mq.get_topic(), "new_topic");
    }

    #[test]
    fn set_broker_name_updates_broker_name() {
        let mut mq = MessageQueue::new();
        mq.set_broker_name(CheetahString::from("new_broker"));
        assert_eq!(mq.get_broker_name(), "new_broker");
    }

    #[test]
    fn set_queue_id_updates_queue_id() {
        let mut mq = MessageQueue::new();
        mq.set_queue_id(10);
        assert_eq!(mq.get_queue_id(), 10);
    }

    #[test]
    fn message_queue_equality() {
        let mq1 = MessageQueue::from_parts("topic1", "broker1", 1);
        let mq2 = MessageQueue::from_parts("topic1", "broker1", 1);
        assert_eq!(mq1, mq2);
    }

    #[test]
    fn message_queue_inequality() {
        let mq1 = MessageQueue::from_parts("topic1", "broker1", 1);
        let mq2 = MessageQueue::from_parts("topic2", "broker2", 2);
        assert_ne!(mq1, mq2);
    }

    #[test]
    fn message_queue_ordering() {
        let mq1 = MessageQueue::from_parts("topic1", "broker1", 1);
        let mq2 = MessageQueue::from_parts("topic1", "broker1", 2);
        assert!(mq1 < mq2);
    }

    #[test]
    fn message_queue_display_format() {
        let mq = MessageQueue::from_parts("topic1", "broker1", 1);
        assert_eq!(
            format!("{}", mq),
            "MessageQueue [topic=topic1, broker_name=broker1, queue_id=1]"
        );
    }
}
