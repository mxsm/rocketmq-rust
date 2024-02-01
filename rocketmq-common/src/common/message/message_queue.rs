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

use std::{cmp::Ordering, fmt};

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct MessageQueue {
    topic: String,
    broker_name: String,
    queue_id: i32,
}

impl MessageQueue {
    fn new(topic: &str, broker_name: &str, queue_id: i32) -> Self {
        MessageQueue {
            topic: topic.to_string(),
            broker_name: broker_name.to_string(),
            queue_id,
        }
    }
}

impl fmt::Display for MessageQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MessageQueue {{ topic: {}, broker_name: {}, queue_id: {} }}",
            self.topic, self.broker_name, self.queue_id
        )
    }
}

impl Ord for MessageQueue {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.topic.cmp(&other.topic) {
            Ordering::Equal => match self.broker_name.cmp(&other.broker_name) {
                Ordering::Equal => self.queue_id.cmp(&other.queue_id),
                ord => ord,
            },
            ord => ord,
        }
    }
}

impl PartialOrd for MessageQueue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
