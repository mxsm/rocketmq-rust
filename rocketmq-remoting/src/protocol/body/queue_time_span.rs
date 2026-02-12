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
use rocketmq_common::UtilAll::time_millis_to_human_string2;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueueTimeSpan {
    pub message_queue: Option<MessageQueue>,
    pub min_time_stamp: i64,
    pub max_time_stamp: i64,
    pub consume_time_stamp: i64,
    pub delay_time: i64,
}

impl QueueTimeSpan {
    pub fn get_message_queue(&self) -> Option<MessageQueue> {
        self.message_queue.clone()
    }

    pub fn set_message_queue(&mut self, message_queue: MessageQueue) {
        self.message_queue = Some(message_queue);
    }

    pub fn get_min_time_stamp(&self) -> i64 {
        self.min_time_stamp
    }

    pub fn set_min_time_stamp(&mut self, min_time_stamp: i64) {
        self.min_time_stamp = min_time_stamp;
    }

    pub fn get_max_time_stamp(&self) -> i64 {
        self.max_time_stamp
    }

    pub fn set_max_time_stamp(&mut self, max_time_stamp: i64) {
        self.max_time_stamp = max_time_stamp;
    }

    pub fn get_consume_time_stamp(&self) -> i64 {
        self.consume_time_stamp
    }

    pub fn set_consume_time_stamp(&mut self, consume_time_stamp: i64) {
        self.consume_time_stamp = consume_time_stamp;
    }

    pub fn get_delay_time(&self) -> i64 {
        self.delay_time
    }

    pub fn set_delay_time(&mut self, delay_time: i64) {
        self.delay_time = delay_time;
    }

    pub fn get_min_time_stamp_str(&self) -> String {
        time_millis_to_human_string2(self.min_time_stamp)
    }

    pub fn get_max_time_stamp_str(&self) -> String {
        time_millis_to_human_string2(self.max_time_stamp)
    }

    pub fn get_consume_time_stamp_str(&self) -> String {
        time_millis_to_human_string2(self.consume_time_stamp)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn queue_span_init() {
        let body: QueueTimeSpan = Default::default();
        assert_eq!(body.consume_time_stamp, 0);
        assert_eq!(body.delay_time, 0);
        assert_eq!(body.max_time_stamp, 0);
        assert_eq!(body.min_time_stamp, 0);
        assert_eq!(body.message_queue, None);
    }
    #[test]
    fn queue_span_clone() {
        let body: QueueTimeSpan = QueueTimeSpan {
            message_queue: Some(MessageQueue::new()),
            min_time_stamp: 1,
            max_time_stamp: 2,
            consume_time_stamp: 3,
            delay_time: 4,
        };
        let body_clone = body.clone();
        assert_eq!(body_clone.consume_time_stamp, 3);
        assert_eq!(body_clone.delay_time, 4);
        assert_eq!(body_clone.max_time_stamp, 2);
        assert_eq!(body_clone.min_time_stamp, 1);
        assert_eq!(body_clone.message_queue, Some(MessageQueue::new()),);
    }
    #[test]
    fn queue_span_deserialise() {
        let body: QueueTimeSpan = QueueTimeSpan {
            message_queue: Some(MessageQueue::new()),
            min_time_stamp: 1,
            max_time_stamp: 2,
            consume_time_stamp: 3,
            delay_time: 4,
        };

        let json = serde_json::to_string(&body).unwrap();
        let deserialized: QueueTimeSpan = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.consume_time_stamp, 3);
        assert_eq!(deserialized.delay_time, 4);
        assert_eq!(deserialized.max_time_stamp, 2);
        assert_eq!(deserialized.min_time_stamp, 1);
        assert_eq!(deserialized.message_queue, Some(MessageQueue::new()),);
    }
    #[test]
    fn queue_span_serialise() {
        let body: QueueTimeSpan = QueueTimeSpan {
            message_queue: Some(MessageQueue::new()),
            min_time_stamp: 1,
            max_time_stamp: 2,
            consume_time_stamp: 3,
            delay_time: 4,
        };

        let json = serde_json::to_string(&body).unwrap();
        assert_eq!(
            json,
            "{\"message_queue\":{\"topic\":\"\",\"brokerName\":\"\",\"queueId\":0},\"min_time_stamp\":1,\"\
             max_time_stamp\":2,\"consume_time_stamp\":3,\"delay_time\":4}"
        );
    }
    #[test]
    fn queue_span_setters() {
        let mut body: QueueTimeSpan = Default::default();
        body.set_message_queue(MessageQueue::new());
        body.set_min_time_stamp(1);
        body.set_max_time_stamp(2);
        body.set_consume_time_stamp(3);
        body.set_delay_time(4);

        assert_eq!(body.consume_time_stamp, 3);
        assert_eq!(body.delay_time, 4);
        assert_eq!(body.max_time_stamp, 2);
        assert_eq!(body.min_time_stamp, 1);
        assert_eq!(body.message_queue, Some(MessageQueue::new()),);
    }
    #[test]
    fn queue_span_getters() {
        let body: QueueTimeSpan = QueueTimeSpan {
            message_queue: Some(MessageQueue::new()),
            min_time_stamp: 1,
            max_time_stamp: 2,
            consume_time_stamp: 3,
            delay_time: 4,
        };

        assert_eq!(body.get_consume_time_stamp(), 3);
        assert_eq!(body.get_delay_time(), 4);
        assert_eq!(body.get_max_time_stamp(), 2);
        assert_eq!(body.get_min_time_stamp(), 1);
        assert_eq!(body.get_message_queue(), Some(MessageQueue::new()),);
    }

    #[test]
    fn queue_span_time_str() {
        let body: QueueTimeSpan = QueueTimeSpan {
            message_queue: Some(MessageQueue::new()),
            min_time_stamp: 1,
            max_time_stamp: 2,
            consume_time_stamp: 3,
            delay_time: 4,
        };

        assert_eq!(body.get_min_time_stamp_str(), "1970-01-01 01:00:00,001".to_string());
        assert_eq!(body.get_max_time_stamp_str(), "1970-01-01 01:00:00,002".to_string());
        assert_eq!(body.get_consume_time_stamp_str(), "1970-01-01 01:00:00,003".to_string());
    }
}
