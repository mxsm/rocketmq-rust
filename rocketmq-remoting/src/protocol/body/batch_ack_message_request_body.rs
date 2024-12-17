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
use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::body::batch_ack::BatchAck;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchAckMessageRequestBody {
    pub broker_name: CheetahString,
    pub acks: Vec<BatchAck>,
}

#[cfg(test)]
mod tests {
    use bit_vec::BitVec;
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn batch_ack_message_request_body_serialization() {
        let body = BatchAckMessageRequestBody {
            broker_name: CheetahString::from("broker1"),
            acks: vec![BatchAck {
                consumer_group: String::from("group1"),
                topic: String::from("topic1"),
                retry: String::from("1"),
                start_offset: 100,
                queue_id: 1,
                revive_queue_id: 2,
                pop_time: 123456789,
                invisible_time: 987654321,
                bit_set: BitVec::from_elem(8, true),
            }],
        };
        let serialized = serde_json::to_string(&body).unwrap();
        let deserialized: BatchAckMessageRequestBody = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.broker_name, CheetahString::from("broker1"));
        assert_eq!(deserialized.acks.len(), 1);
        assert_eq!(deserialized.acks[0].consumer_group, "group1");
    }

    #[test]
    fn batch_ack_message_request_body_default_values() {
        let body = BatchAckMessageRequestBody {
            broker_name: CheetahString::new(),
            acks: vec![],
        };
        assert_eq!(body.broker_name, CheetahString::new());
        assert!(body.acks.is_empty());
    }

    #[test]
    fn batch_ack_message_request_body_edge_case_empty_strings() {
        let body = BatchAckMessageRequestBody {
            broker_name: CheetahString::from(""),
            acks: vec![BatchAck {
                consumer_group: String::from(""),
                topic: String::from(""),
                retry: String::from(""),
                start_offset: -1,
                queue_id: -1,
                revive_queue_id: -1,
                pop_time: -1,
                invisible_time: -1,
                bit_set: BitVec::new(),
            }],
        };
        assert_eq!(body.broker_name, CheetahString::from(""));
        assert_eq!(body.acks.len(), 1);
        assert_eq!(body.acks[0].consumer_group, "");
    }
}
