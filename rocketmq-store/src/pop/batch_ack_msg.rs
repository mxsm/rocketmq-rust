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

use std::fmt::Display;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::pop::ack_msg::AckMsg;
use crate::pop::AckMessage;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct BatchAckMsg {
    #[serde(flatten)]
    pub ack_msg: AckMsg,

    #[serde(rename = "aol", alias = "ackOffsetList")]
    pub ack_offset_list: Vec<i64>,
}

impl AckMessage for BatchAckMsg {
    fn ack_offset(&self) -> i64 {
        self.ack_msg.ack_offset
    }

    fn set_ack_offset(&mut self, ack_offset: i64) {
        self.ack_msg.ack_offset = ack_offset;
    }

    fn start_offset(&self) -> i64 {
        self.ack_msg.start_offset
    }

    fn set_start_offset(&mut self, start_offset: i64) {
        self.ack_msg.start_offset = start_offset;
    }

    fn consumer_group(&self) -> &CheetahString {
        &self.ack_msg.consumer_group
    }

    fn set_consumer_group(&mut self, consumer_group: CheetahString) {
        self.ack_msg.consumer_group = consumer_group;
    }

    fn topic(&self) -> &CheetahString {
        &self.ack_msg.topic
    }

    fn set_topic(&mut self, topic: CheetahString) {
        self.ack_msg.topic = topic;
    }

    fn queue_id(&self) -> i32 {
        self.ack_msg.queue_id
    }

    fn set_queue_id(&mut self, queue_id: i32) {
        self.ack_msg.queue_id = queue_id;
    }

    fn pop_time(&self) -> i64 {
        self.ack_msg.pop_time
    }

    fn set_pop_time(&mut self, pop_time: i64) {
        self.ack_msg.pop_time = pop_time;
    }

    fn broker_name(&self) -> &CheetahString {
        &self.ack_msg.broker_name
    }

    fn set_broker_name(&mut self, broker_name: CheetahString) {
        self.ack_msg.broker_name = broker_name;
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl Display for BatchAckMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BatchAckMsg [ack_msg={}, ack_offset_list={:?}]",
            self.ack_msg, self.ack_offset_list
        )
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn batch_ack_msg_display_formats_correctly() {
        let ack_msg = AckMsg {
            ack_offset: 123,
            start_offset: 456,
            consumer_group: CheetahString::from_static_str("test_group"),
            topic: CheetahString::from_static_str("test_topic"),
            queue_id: 1,
            pop_time: 789,
            broker_name: CheetahString::from_static_str("test_broker"),
        };
        let batch_ack_msg = BatchAckMsg {
            ack_msg,
            ack_offset_list: vec![1, 2, 3],
        };
        let expected = "BatchAckMsg [ack_msg=AckMsg [ack_offset=123, start_offset=456, consumer_group=test_group, \
                        topic=test_topic, queue_id=1, pop_time=789, broker_name=test_broker], ack_offset_list=[1, 2, \
                        3]]";
        assert_eq!(format!("{}", batch_ack_msg), expected);
    }

    #[test]
    fn batch_ack_msg_serializes_correctly() {
        let ack_msg = AckMsg {
            ack_offset: 123,
            start_offset: 456,
            consumer_group: CheetahString::from_static_str("test_group"),
            topic: CheetahString::from_static_str("test_topic"),
            queue_id: 1,
            pop_time: 789,
            broker_name: CheetahString::from_static_str("test_broker"),
        };
        let batch_ack_msg = BatchAckMsg {
            ack_msg,
            ack_offset_list: vec![1, 2, 3],
        };
        let json = serde_json::to_string(&batch_ack_msg).unwrap();
        let expected =
            r#"{"ao":123,"so":456,"c":"test_group","t":"test_topic","q":1,"pt":789,"bn":"test_broker","aol":[1,2,3]}"#;
        assert_eq!(json, expected);
    }

    #[test]
    fn batch_ack_msg_deserializes_correctly() {
        let json =
            r#"{"ao":123,"so":456,"c":"test_group","t":"test_topic","q":1,"pt":789,"bn":"test_broker","aol":[1,2,3]}"#;
        let batch_ack_msg: BatchAckMsg = serde_json::from_str(json).unwrap();
        assert_eq!(batch_ack_msg.ack_msg.ack_offset, 123);
        assert_eq!(batch_ack_msg.ack_msg.start_offset, 456);
        assert_eq!(
            batch_ack_msg.ack_msg.consumer_group,
            CheetahString::from_static_str("test_group")
        );
        assert_eq!(
            batch_ack_msg.ack_msg.topic,
            CheetahString::from_static_str("test_topic")
        );
        assert_eq!(batch_ack_msg.ack_msg.queue_id, 1);
        assert_eq!(batch_ack_msg.ack_msg.pop_time, 789);
        assert_eq!(
            batch_ack_msg.ack_msg.broker_name,
            CheetahString::from_static_str("test_broker")
        );
        assert_eq!(batch_ack_msg.ack_offset_list, vec![1, 2, 3]);
    }
}
