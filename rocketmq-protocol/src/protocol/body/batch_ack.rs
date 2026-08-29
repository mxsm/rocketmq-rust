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

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug)]
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
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn serde_round_trip_preserves_the_batch_ack() {
        let bit_set = BitVec::from_vec(vec![0u64; 8]);
        let batch_ack = BatchAck {
            consumer_group: CheetahString::from_static_str("group1"),
            topic: CheetahString::from_static_str("topic1"),
            retry: CheetahString::from_static_str("1"),
            start_offset: 100,
            queue_id: 1,
            revive_queue_id: 2,
            pop_time: 123456789,
            invisible_time: 987654321,
            bit_set: SerializableBitVec(bit_set.clone()),
        };

        let encoded = serde_json::to_vec(&batch_ack).expect("serialize batch ACK");
        let decoded: BatchAck = serde_json::from_slice(&encoded).expect("deserialize batch ACK");
        assert_eq!(decoded.consumer_group, batch_ack.consumer_group);
        assert_eq!(decoded.topic, batch_ack.topic);
        assert_eq!(decoded.retry, batch_ack.retry);
        assert_eq!(decoded.start_offset, batch_ack.start_offset);
        assert_eq!(decoded.queue_id, batch_ack.queue_id);
        assert_eq!(decoded.revive_queue_id, batch_ack.revive_queue_id);
        assert_eq!(decoded.pop_time, batch_ack.pop_time);
        assert_eq!(decoded.invisible_time, batch_ack.invisible_time);
        assert_eq!(decoded.bit_set.0, bit_set);
    }
}
