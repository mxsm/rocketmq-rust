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

use std::collections::HashSet;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::heartbeat::consumer_data::ConsumerData;
use crate::protocol::heartbeat::producer_data::ProducerData;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatData {
    #[serde(rename = "clientID")]
    pub client_id: CheetahString,
    #[serde(default)]
    pub producer_data_set: HashSet<ProducerData>,
    #[serde(default)]
    pub consumer_data_set: HashSet<ConsumerData>,
    #[serde(default)]
    pub heartbeat_fingerprint: i32,
    #[serde(rename = "withoutSub", default)]
    pub is_without_sub: bool,
}
#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;

    use super::*;
    use crate::protocol::RemotingSerializable;

    #[test]
    fn heartbeat_data_serialization_deserialization() {
        let mut producer_data_set = HashSet::new();
        producer_data_set.insert(ProducerData::default());
        let mut consumer_data_set = HashSet::new();
        consumer_data_set.insert(ConsumerData::default());

        let original = HeartbeatData {
            client_id: "client1".into(),
            producer_data_set,
            consumer_data_set,
            heartbeat_fingerprint: 123,
            is_without_sub: false,
        };

        let serialized = original.encode().expect("encode");
        let deserialized = SerdeJsonUtils::from_json_bytes::<HeartbeatData>(serialized.as_slice()).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn heartbeat_data_without_sub_serialization_deserialization() {
        let original = HeartbeatData {
            client_id: "client1".into(),
            producer_data_set: HashSet::new(),
            consumer_data_set: HashSet::new(),
            heartbeat_fingerprint: 123,
            is_without_sub: true,
        };

        let serialized = original.encode().expect("encode");
        let deserialized = SerdeJsonUtils::from_json_bytes::<HeartbeatData>(serialized.as_slice()).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn heartbeat_data_with_empty_sets_serialization_deserialization() {
        let original = HeartbeatData {
            client_id: "client1".into(),
            producer_data_set: HashSet::new(),
            consumer_data_set: HashSet::new(),
            heartbeat_fingerprint: 123,
            is_without_sub: false,
        };

        let serialized = original.encode().expect("encode");
        let deserialized = SerdeJsonUtils::from_json_bytes::<HeartbeatData>(serialized.as_slice()).unwrap();

        assert_eq!(original, deserialized);
    }
}
