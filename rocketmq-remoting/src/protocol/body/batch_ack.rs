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
use bit_vec::BitVec;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchAck {
    #[serde(rename = "c", alias = "consumerGroup")]
    pub consumer_group: String,

    #[serde(rename = "t", alias = "topic")]
    pub topic: String,

    #[serde(rename = "r", alias = "retry")]
    pub retry: String, // "1" if it's retry topic

    #[serde(rename = "so", alias = "startOffset")]
    pub start_offset: i64,

    #[serde(rename = "q", alias = "queueId")]
    pub queue_id: i32,

    #[serde(rename = "rq", alias = "reviveQueueId")]
    pub revive_queue_id: i32,

    #[serde(rename = "pt", alias = "popTime")]
    pub pop_time: i64,

    #[serde(rename = "it", alias = "invisibleTime")]
    pub invisible_time: i64,

    #[serde(rename = "b", alias = "bitSet")]
    pub bit_set: BitVec,
}

#[cfg(test)]
mod tests {
    use bit_vec::BitVec;

    use super::*;

    #[test]
    fn batch_ack_serialization() {
        let bit_set = BitVec::from_elem(8, true);
        let batch_ack = BatchAck {
            consumer_group: String::from("group1"),
            topic: String::from("topic1"),
            retry: String::from("1"),
            start_offset: 100,
            queue_id: 1,
            revive_queue_id: 2,
            pop_time: 123456789,
            invisible_time: 987654321,
            bit_set: bit_set.clone(),
        };
        let serialized = serde_json::to_string(&batch_ack).unwrap();
        let deserialized: BatchAck = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.consumer_group, "group1");
        assert_eq!(deserialized.topic, "topic1");
        assert_eq!(deserialized.retry, "1");
        assert_eq!(deserialized.start_offset, 100);
        assert_eq!(deserialized.queue_id, 1);
        assert_eq!(deserialized.revive_queue_id, 2);
        assert_eq!(deserialized.pop_time, 123456789);
        assert_eq!(deserialized.invisible_time, 987654321);
        assert_eq!(deserialized.bit_set, bit_set);
    }

    #[test]
    fn batch_ack_default_values() {
        let bit_set = BitVec::from_elem(8, false);
        let batch_ack = BatchAck {
            consumer_group: String::new(),
            topic: String::new(),
            retry: String::new(),
            start_offset: 0,
            queue_id: 0,
            revive_queue_id: 0,
            pop_time: 0,
            invisible_time: 0,
            bit_set: bit_set.clone(),
        };
        assert_eq!(batch_ack.consumer_group, "");
        assert_eq!(batch_ack.topic, "");
        assert_eq!(batch_ack.retry, "");
        assert_eq!(batch_ack.start_offset, 0);
        assert_eq!(batch_ack.queue_id, 0);
        assert_eq!(batch_ack.revive_queue_id, 0);
        assert_eq!(batch_ack.pop_time, 0);
        assert_eq!(batch_ack.invisible_time, 0);
        assert_eq!(batch_ack.bit_set, bit_set);
    }

    #[test]
    fn batch_ack_edge_case_empty_strings() {
        let bit_set = BitVec::new();
        let batch_ack = BatchAck {
            consumer_group: String::from(""),
            topic: String::from(""),
            retry: String::from(""),
            start_offset: -1,
            queue_id: -1,
            revive_queue_id: -1,
            pop_time: -1,
            invisible_time: -1,
            bit_set: bit_set.clone(),
        };
        assert_eq!(batch_ack.consumer_group, "");
        assert_eq!(batch_ack.topic, "");
        assert_eq!(batch_ack.retry, "");
        assert_eq!(batch_ack.start_offset, -1);
        assert_eq!(batch_ack.queue_id, -1);
        assert_eq!(batch_ack.revive_queue_id, -1);
        assert_eq!(batch_ack.pop_time, -1);
        assert_eq!(batch_ack.invisible_time, -1);
        assert_eq!(batch_ack.bit_set, bit_set);
    }
}
