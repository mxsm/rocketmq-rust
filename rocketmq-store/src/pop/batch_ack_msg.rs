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

use serde::Deserialize;
use serde::Serialize;

use crate::pop::ack_msg::AckMsg;

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchAckMsg {
    #[serde(flatten)]
    pub ack_msg: AckMsg,

    #[serde(rename = "aol", alias = "ackOffsetList")]
    pub ack_offset_list: Vec<i64>,
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
        let expected = "BatchAckMsg [ack_msg=AckMsg [ack_offset=123, start_offset=456, \
                        consumer_group=test_group, topic=test_topic, queue_id=1, pop_time=789, \
                        broker_name=test_broker], ack_offset_list=[1, 2, 3]]";
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
        let expected = r#"{"ao":123,"so":456,"c":"test_group","t":"test_topic","q":1,"pt":789,"bn":"test_broker","aol":[1,2,3]}"#;
        assert_eq!(json, expected);
    }

    #[test]
    fn batch_ack_msg_deserializes_correctly() {
        let json = r#"{"ao":123,"so":456,"c":"test_group","t":"test_topic","q":1,"pt":789,"bn":"test_broker","aol":[1,2,3]}"#;
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
