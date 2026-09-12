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

use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_protocol::protocol::RemotingDeserializable;
use serde::Deserialize;
use serde::Serialize;
use tracing::warn;

use crate::consumer::store::offset_serialize::OffsetSerialize;

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct OffsetSerializeWrapper {
    #[serde(serialize_with = "serialize_atomic_i64", deserialize_with = "deserialize_atomic_i64")]
    pub offset_table: HashMap<MessageQueue, AtomicI64>,
}

fn serialize_atomic_i64<S>(map: &HashMap<MessageQueue, AtomicI64>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let map_as_i64: HashMap<_, _> = map.iter().map(|(k, v)| (k, v.load(Ordering::Relaxed))).collect();
    map_as_i64.serialize(serializer)
}

fn deserialize_atomic_i64<'de, D>(deserializer: D) -> Result<HashMap<MessageQueue, AtomicI64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let map_as_i64: HashMap<MessageQueue, i64> = HashMap::deserialize(deserializer)?;
    let map_as_atomic: HashMap<_, _> = map_as_i64.into_iter().map(|(k, v)| (k, AtomicI64::new(v))).collect();
    Ok(map_as_atomic)
}

impl From<OffsetSerialize> for OffsetSerializeWrapper {
    fn from(offset_serialize: OffsetSerialize) -> Self {
        let offset_table = offset_serialize
            .offset_table
            .into_iter()
            .filter_map(|(k, v)| match MessageQueue::decode(k.as_bytes()) {
                Ok(message_queue) => Some((message_queue, AtomicI64::new(v))),
                Err(err) => {
                    warn!("skip offset entry because message queue decode failed: {err}");
                    None
                }
            })
            .collect();
        Self { offset_table }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_protocol::protocol::RemotingSerializable;

    fn queue_key(topic: &str, broker: &str, queue_id: i32) -> String {
        MessageQueue::from_parts(topic, broker, queue_id)
            .serialize_json()
            .expect("queue should serialize to JSON")
    }

    #[test]
    fn offset_table_round_trips_through_json_with_signed_offsets_and_keys() {
        let queue_a = MessageQueue::from_parts("topic-a", "broker-a", 0);
        let queue_b = MessageQueue::from_parts("topic-b", "broker-b", 1);

        let mut wrapper = OffsetSerializeWrapper::default();
        wrapper.offset_table.insert(queue_a.clone(), AtomicI64::new(-42));
        wrapper.offset_table.insert(queue_b.clone(), AtomicI64::new(100));

        let serialize: OffsetSerialize = wrapper.into();
        let json = serialize.serialize_json().expect("offset serialize should encode");
        let decoded = OffsetSerialize::decode_str(&json).expect("offset serialize should decode");
        let round_tripped: OffsetSerializeWrapper = decoded.into();

        assert_eq!(round_tripped.offset_table.len(), 2);
        assert_eq!(
            round_tripped
                .offset_table
                .get(&queue_a)
                .map(|v| v.load(Ordering::Relaxed)),
            Some(-42)
        );
        assert_eq!(
            round_tripped
                .offset_table
                .get(&queue_b)
                .map(|v| v.load(Ordering::Relaxed)),
            Some(100)
        );
    }

    #[test]
    fn offset_larger_than_i32_max_is_not_narrowed() {
        let queue = MessageQueue::from_parts("topic-a", "broker-a", 0);
        let large_offset = i64::from(i32::MAX) + 1000;

        let mut wrapper = OffsetSerializeWrapper::default();
        wrapper.offset_table.insert(queue.clone(), AtomicI64::new(large_offset));

        let serialize: OffsetSerialize = wrapper.into();
        let json = serialize.serialize_json().expect("offset serialize should encode");
        let decoded = OffsetSerialize::decode_str(&json).expect("offset serialize should decode");
        let round_tripped: OffsetSerializeWrapper = decoded.into();

        assert_eq!(
            round_tripped
                .offset_table
                .get(&queue)
                .map(|v| v.load(Ordering::Relaxed)),
            Some(large_offset)
        );
    }

    #[test]
    fn invalid_queue_key_is_skipped_while_valid_entries_are_kept() {
        let queue = MessageQueue::from_parts("topic-a", "broker-a", 0);
        let mut offset_table = HashMap::new();
        offset_table.insert(queue_key("topic-a", "broker-a", 0), 7);
        offset_table.insert("not a valid message queue key".to_string(), 99);
        let serialize = OffsetSerialize { offset_table };

        let wrapper: OffsetSerializeWrapper = serialize.into();

        assert_eq!(wrapper.offset_table.len(), 1);
        assert_eq!(
            wrapper.offset_table.get(&queue).map(|v| v.load(Ordering::Relaxed)),
            Some(7)
        );
    }

    #[test]
    fn empty_offset_table_round_trips_with_persisted_field_name() {
        let wrapper = OffsetSerializeWrapper::default();

        let serialize: OffsetSerialize = wrapper.into();
        let json = serialize.serialize_json().expect("offset serialize should encode");

        assert!(json.contains("\"offsetTable\""));
        let decoded = OffsetSerialize::decode_str(&json).expect("offset serialize should decode");
        let round_tripped: OffsetSerializeWrapper = decoded.into();

        assert!(round_tripped.offset_table.is_empty());
    }
}
