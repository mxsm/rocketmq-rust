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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

    pub fn from_parts(
        topic: impl Into<String>,
        broker_name: impl Into<String>,
        queue_id: i32,
    ) -> Self {
        MessageQueue {
            topic: topic.into(),
            broker_name: broker_name.into(),
            queue_id,
        }
    }

    pub fn get_topic(&self) -> &str {
        &self.topic
    }

    pub fn set_topic(&mut self, topic: String) {
        self.topic = topic;
    }

    #[inline]
    pub fn get_broker_name(&self) -> &str {
        &self.broker_name
    }

    pub fn set_broker_name(&mut self, broker_name: String) {
        self.broker_name = broker_name;
    }

    #[inline]
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
