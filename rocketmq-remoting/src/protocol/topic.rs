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

use rocketmq_common::common::message::message_queue::MessageQueue;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug)]
pub struct OffsetMovedEvent {
    pub consumer_group: String,
    pub message_queue: MessageQueue,
    pub offset_request: i64,
    pub offset_new: i64,
}

impl OffsetMovedEvent {
    pub fn get_consumer_group(&self) -> &str {
        &self.consumer_group
    }

    pub fn set_consumer_group(&mut self, consumer_group: String) {
        self.consumer_group = consumer_group;
    }

    pub fn get_message_queue(&self) -> &MessageQueue {
        &self.message_queue
    }

    pub fn set_message_queue(&mut self, message_queue: MessageQueue) {
        self.message_queue = message_queue;
    }

    pub fn get_offset_request(&self) -> i64 {
        self.offset_request
    }

    pub fn set_offset_request(&mut self, offset_request: i64) {
        self.offset_request = offset_request;
    }

    pub fn get_offset_new(&self) -> i64 {
        self.offset_new
    }

    pub fn set_offset_new(&mut self, offset_new: i64) {
        self.offset_new = offset_new;
    }
}

impl std::fmt::Display for OffsetMovedEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "OffsetMovedEvent [consumer_group={}, message_queue={:?}, offset_request={}, offset_new={}]",
            self.consumer_group, self.message_queue, self.offset_request, self.offset_new
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_moved_event_field_initialization() {
        let body: OffsetMovedEvent = OffsetMovedEvent {
            consumer_group: "test_group".to_string(),
            message_queue: MessageQueue::new(),
            offset_request: 100,
            offset_new: 200,
        };
        assert_eq!(body.get_consumer_group(), "test_group");
        assert_eq!(body.get_offset_request(), 100);
        assert_eq!(body.get_offset_new(), 200);
        assert_eq!(body.get_message_queue(), &MessageQueue::new());
    }

    #[test]
    fn offset_moved_event_setters() {
        let mut body = OffsetMovedEvent {
            consumer_group: "test_group".to_string(),
            message_queue: MessageQueue::new(),
            offset_request: 100,
            offset_new: 200,
        };
        body.set_consumer_group("new_group".to_string());
        body.set_message_queue(MessageQueue::new());
        body.set_offset_request(150);
        body.set_offset_new(250);
        assert_eq!(body.get_consumer_group(), "new_group");
        assert_eq!(body.get_message_queue(), &MessageQueue::new());
        assert_eq!(body.get_offset_request(), 150);
        assert_eq!(body.get_offset_new(), 250);
    }

    #[test]
    fn offset_moved_event_getters() {
        let body = OffsetMovedEvent {
            consumer_group: "another_group".to_string(),
            message_queue: MessageQueue::new(),
            offset_request: 300,
            offset_new: 400,
        };
        assert_eq!(body.get_consumer_group(), "another_group");
        assert_eq!(body.get_offset_request(), 300);
        assert_eq!(body.get_offset_new(), 400);
    }

    #[test]
    fn offset_moved_event_display() {
        let body = OffsetMovedEvent {
            consumer_group: "test_group".to_string(),
            message_queue: MessageQueue::new(),
            offset_request: 100,
            offset_new: 200,
        };
        let display = format!("{}", body);
        assert!(display.contains("OffsetMovedEvent"));
        assert!(display.contains("consumer_group=test_group"));
        assert!(display.contains("offset_request=100"));
        assert!(display.contains("offset_new=200"));
    }
    #[test]
    fn offset_moved_event_serialization() {
        let body = OffsetMovedEvent {
            consumer_group: "test_group".to_string(),
            message_queue: MessageQueue::new(),
            offset_request: 100,
            offset_new: 200,
        };
        let json = serde_json::to_string(&body).unwrap();
        assert!(json.contains("\"consumer_group\":\"test_group\""));
        assert!(json.contains("\"offset_request\":100"));
        assert!(json.contains("\"offset_new\":200"));
    }

    #[test]
    fn offset_moved_event_deserialization() {
        let json = r#"{"consumer_group":"test_group","message_queue":{"topic":"","brokerName":"","queueId":0},"offset_request":100,"offset_new":200}"#;
        let body: OffsetMovedEvent = serde_json::from_str(json).unwrap();
        assert_eq!(body.get_consumer_group(), "test_group");
        assert_eq!(body.get_offset_request(), 100);
        assert_eq!(body.get_offset_new(), 200);
        assert_eq!(body.get_message_queue(), &MessageQueue::new());
    }
}
