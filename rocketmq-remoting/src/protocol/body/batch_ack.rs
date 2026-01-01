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

use bitvec::prelude::BitVec;
use bitvec::prelude::Lsb0;
use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

#[derive(Serialize, Deserialize)]
pub struct BatchAck {
    #[serde(rename = "c", alias = "consumerGroup")]
    pub consumer_group: CheetahString,

    #[serde(rename = "t", alias = "topic")]
    pub topic: CheetahString,

    #[serde(rename = "r", alias = "retry")]
    pub retry: CheetahString, // "1" if it's retry topic

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
    pub bit_set: SerializableBitVec,
}

pub struct SerializableBitVec(pub BitVec<u64, Lsb0>);

impl Serialize for SerializableBitVec {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let slice = bytemuck::cast_slice(self.0.as_raw_slice());
        serializer.serialize_bytes(slice)
    }
}

impl<'de> Deserialize<'de> for SerializableBitVec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: Vec<u8> = Vec::deserialize(deserializer)?;
        let inner: &[u64] = bytemuck::cast_slice(bytes.as_slice());
        Ok(SerializableBitVec(BitVec::<u64, Lsb0>::from_slice(inner)))
    }
}

#[cfg(test)]
mod tests {
    use bitvec::prelude::*;
    use cheetah_string::CheetahString;
    use serde_json;

    use super::*;

    #[test]
    fn batch_ack_serialization() {
        let bit_set = BitVec::from_vec(vec![0u64; 8]);
        let batch_ack = BatchAck {
            consumer_group: CheetahString::from("group1"),
            topic: CheetahString::from("topic1"),
            retry: CheetahString::from("1"),
            start_offset: 100,
            queue_id: 1,
            revive_queue_id: 2,
            pop_time: 123456789,
            invisible_time: 987654321,
            bit_set: SerializableBitVec(bit_set.clone()),
        };
        let serialized = serde_json::to_string(&batch_ack).unwrap();
        let deserialized: BatchAck = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.consumer_group, CheetahString::from("group1"));
        assert_eq!(deserialized.topic, CheetahString::from("topic1"));
        assert_eq!(deserialized.retry, CheetahString::from("1"));
        assert_eq!(deserialized.start_offset, 100);
        assert_eq!(deserialized.queue_id, 1);
        assert_eq!(deserialized.revive_queue_id, 2);
        assert_eq!(deserialized.pop_time, 123456789);
        assert_eq!(deserialized.invisible_time, 987654321);
        assert_eq!(deserialized.bit_set.0, bit_set);
    }

    #[test]
    fn batch_ack_default_values() {
        let bit_set = BitVec::from_element(8);
        let batch_ack = BatchAck {
            consumer_group: CheetahString::new(),
            topic: CheetahString::new(),
            retry: CheetahString::new(),
            start_offset: 0,
            queue_id: 0,
            revive_queue_id: 0,
            pop_time: 0,
            invisible_time: 0,
            bit_set: SerializableBitVec(bit_set.clone()),
        };
        assert_eq!(batch_ack.consumer_group, CheetahString::new());
        assert_eq!(batch_ack.topic, CheetahString::new());
        assert_eq!(batch_ack.retry, CheetahString::new());
        assert_eq!(batch_ack.start_offset, 0);
        assert_eq!(batch_ack.queue_id, 0);
        assert_eq!(batch_ack.revive_queue_id, 0);
        assert_eq!(batch_ack.pop_time, 0);
        assert_eq!(batch_ack.invisible_time, 0);
        assert_eq!(batch_ack.bit_set.0, bit_set);
    }

    #[test]
    fn batch_ack_edge_case_empty_strings() {
        let bit_set = BitVec::new();
        let batch_ack = BatchAck {
            consumer_group: CheetahString::from(""),
            topic: CheetahString::from(""),
            retry: CheetahString::from(""),
            start_offset: -1,
            queue_id: -1,
            revive_queue_id: -1,
            pop_time: -1,
            invisible_time: -1,
            bit_set: SerializableBitVec(bit_set.clone()),
        };
        assert_eq!(batch_ack.consumer_group, CheetahString::from(""));
        assert_eq!(batch_ack.topic, CheetahString::from(""));
        assert_eq!(batch_ack.retry, CheetahString::from(""));
        assert_eq!(batch_ack.start_offset, -1);
        assert_eq!(batch_ack.queue_id, -1);
        assert_eq!(batch_ack.revive_queue_id, -1);
        assert_eq!(batch_ack.pop_time, -1);
        assert_eq!(batch_ack.invisible_time, -1);
        assert_eq!(batch_ack.bit_set.0, bit_set);
    }
}
