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

/// Identifies a queue within a broker for a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageQueue {
    topic: CheetahString,
    broker_name: CheetahString,
    queue_id: i32,
}

impl MessageQueue {
    /// Creates an empty queue identifier.
    pub fn new() -> Self {
        Self {
            topic: CheetahString::new(),
            broker_name: CheetahString::new(),
            queue_id: 0,
        }
    }

    /// Copies an existing queue identifier.
    pub fn from_other(other: &Self) -> Self {
        other.clone()
    }

    /// Creates a queue identifier from its wire-level parts.
    pub fn from_parts(topic: impl Into<CheetahString>, broker_name: impl Into<CheetahString>, queue_id: i32) -> Self {
        Self {
            topic: topic.into(),
            broker_name: broker_name.into(),
            queue_id,
        }
    }

    #[inline]
    pub fn topic_str(&self) -> &str {
        &self.topic
    }

    #[inline]
    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    #[inline]
    pub fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }

    #[inline]
    pub fn broker_name(&self) -> &CheetahString {
        &self.broker_name
    }

    #[inline]
    pub fn set_broker_name(&mut self, broker_name: CheetahString) {
        self.broker_name = broker_name;
    }

    #[inline]
    pub fn queue_id(&self) -> i32 {
        self.queue_id
    }

    #[inline]
    pub fn set_queue_id(&mut self, queue_id: i32) {
        self.queue_id = queue_id;
    }
}

impl Default for MessageQueue {
    fn default() -> Self {
        Self::new()
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
            "MessageQueue [topic={}, brokerName={}, queueId={}]",
            self.topic, self.broker_name, self.queue_id
        )
    }
}
