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

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::protocol::RemotingDeserializable;
use serde::Deserialize;
use serde::Serialize;

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
            .map(|(k, v)| {
                (
                    MessageQueue::decode(k.as_bytes()).expect("Failed to decode message queue"),
                    AtomicI64::new(v),
                )
            })
            .collect();
        Self { offset_table }
    }
}
