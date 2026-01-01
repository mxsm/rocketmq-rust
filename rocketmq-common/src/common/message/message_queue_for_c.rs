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

use super::message_queue::MessageQueue;

/// MessageQueue structure for C++ client compatibility.
/// Includes offset field that the standard MessageQueue doesn't have.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageQueueForC {
    pub topic: CheetahString,
    pub broker_name: CheetahString,
    pub queue_id: i32,
    pub offset: i64,
}

impl MessageQueueForC {
    pub fn new(
        topic: impl Into<CheetahString>,
        broker_name: impl Into<CheetahString>,
        queue_id: i32,
        offset: i64,
    ) -> Self {
        Self {
            topic: topic.into(),
            broker_name: broker_name.into(),
            queue_id,
            offset,
        }
    }

    /// Create from MessageQueue with offset
    pub fn from_message_queue(mq: &MessageQueue, offset: i64) -> Self {
        Self {
            topic: mq.get_topic().into(),
            broker_name: mq.get_broker_name().into(),
            queue_id: mq.get_queue_id(),
            offset,
        }
    }
}

impl Default for MessageQueueForC {
    fn default() -> Self {
        Self {
            topic: CheetahString::empty(),
            broker_name: CheetahString::empty(),
            queue_id: 0,
            offset: 0,
        }
    }
}

impl PartialEq for MessageQueueForC {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic
            && self.broker_name == other.broker_name
            && self.queue_id == other.queue_id
            && self.offset == other.offset
    }
}

impl Eq for MessageQueueForC {}

impl Hash for MessageQueueForC {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.topic.hash(state);
        self.broker_name.hash(state);
        self.queue_id.hash(state);
    }
}

impl Ord for MessageQueueForC {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.topic.cmp(&other.topic) {
            Ordering::Equal => {}
            ord => return ord,
        }
        match self.broker_name.cmp(&other.broker_name) {
            Ordering::Equal => {}
            ord => return ord,
        }
        match self.queue_id.cmp(&other.queue_id) {
            Ordering::Equal => {}
            ord => return ord,
        }
        self.offset.cmp(&other.offset)
    }
}

impl PartialOrd for MessageQueueForC {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for MessageQueueForC {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MessageQueueForC [topic={}, brokerName={}, queueId={}, offset={}]",
            self.topic, self.broker_name, self.queue_id, self.offset
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_queue_for_c_new() {
        let mq = MessageQueueForC::new("test_topic", "broker-a", 1, 100);
        assert_eq!(mq.topic.as_str(), "test_topic");
        assert_eq!(mq.broker_name.as_str(), "broker-a");
        assert_eq!(mq.queue_id, 1);
        assert_eq!(mq.offset, 100);
    }

    #[test]
    fn test_message_queue_for_c_ordering() {
        let mq1 = MessageQueueForC::new("topic_a", "broker", 1, 100);
        let mq2 = MessageQueueForC::new("topic_b", "broker", 1, 100);
        assert!(mq1 < mq2);

        let mq3 = MessageQueueForC::new("topic_a", "broker", 1, 200);
        assert!(mq1 < mq3);
    }

    #[test]
    fn test_message_queue_for_c_serialization() {
        let mq = MessageQueueForC::new("test_topic", "broker-a", 1, 100);
        let json = serde_json::to_string(&mq).unwrap();
        assert!(json.contains("\"topic\":\"test_topic\""));
        assert!(json.contains("\"offset\":100"));

        let deserialized: MessageQueueForC = serde_json::from_str(&json).unwrap();
        assert_eq!(mq, deserialized);
    }
}
