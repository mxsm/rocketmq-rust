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
use std::fmt::Display;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize)]
pub struct AckMsg {
    #[serde(rename = "ao", alias = "ackOffset")]
    pub ack_offset: i64,

    #[serde(rename = "so", alias = "startOffset")]
    pub start_offset: i64,

    #[serde(rename = "c", alias = "consumerGroup")]
    pub consumer_group: CheetahString,

    #[serde(rename = "t", alias = "topic")]
    pub topic: CheetahString,

    #[serde(rename = "q", alias = "queueId")]
    pub queue_id: i32,

    #[serde(rename = "pt", alias = "popTime")]
    pub pop_time: i64,

    #[serde(rename = "bn", alias = "brokerName")]
    pub broker_name: CheetahString,
}

impl Display for AckMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AckMsg [ack_offset={}, start_offset={}, consumer_group={}, topic={}, queue_id={}, \
             pop_time={}, broker_name={}]",
            self.ack_offset,
            self.start_offset,
            self.consumer_group,
            self.topic,
            self.queue_id,
            self.pop_time,
            self.broker_name
        )
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn display_ack_msg_formats_correctly() {
        let ack_msg = AckMsg {
            ack_offset: 123,
            start_offset: 456,
            consumer_group: CheetahString::from_static_str("test_group"),
            topic: CheetahString::from_static_str("test_topic"),
            queue_id: 1,
            pop_time: 789,
            broker_name: CheetahString::from_static_str("test_broker"),
        };
        let expected = "AckMsg [ack_offset=123, start_offset=456, consumer_group=test_group, \
                        topic=test_topic, queue_id=1, pop_time=789, broker_name=test_broker]";
        assert_eq!(format!("{}", ack_msg), expected);
    }

    #[test]
    fn ack_msg_serializes_correctly() {
        let ack_msg = AckMsg {
            ack_offset: 123,
            start_offset: 456,
            consumer_group: CheetahString::from_static_str("test_group"),
            topic: CheetahString::from_static_str("test_topic"),
            queue_id: 1,
            pop_time: 789,
            broker_name: CheetahString::from_static_str("test_broker"),
        };
        let json = serde_json::to_string(&ack_msg).unwrap();
        let expected = r#"{"ao":123,"so":456,"c":"test_group","t":"test_topic","q":1,"pt":789,"bn":"test_broker"}"#;
        assert_eq!(json, expected);
    }

    #[test]
    fn ack_msg_deserializes_correctly() {
        let json = r#"{"ao":123,"so":456,"c":"test_group","t":"test_topic","q":1,"pt":789,"bn":"test_broker"}"#;
        let ack_msg: AckMsg = serde_json::from_str(json).unwrap();
        assert_eq!(ack_msg.ack_offset, 123);
        assert_eq!(ack_msg.start_offset, 456);
        assert_eq!(
            ack_msg.consumer_group,
            CheetahString::from_static_str("test_group")
        );
        assert_eq!(ack_msg.topic, CheetahString::from_static_str("test_topic"));
        assert_eq!(ack_msg.queue_id, 1);
        assert_eq!(ack_msg.pop_time, 789);
        assert_eq!(
            ack_msg.broker_name,
            CheetahString::from_static_str("test_broker")
        );
    }
}
