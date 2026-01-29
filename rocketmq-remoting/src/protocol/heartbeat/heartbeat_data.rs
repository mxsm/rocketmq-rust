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
use rocketmq_common::common::hasher::string_hasher::JavaStringHasher;
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

impl HeartbeatData {
    /// Compute fingerprint for HeartbeatV2 protocol
    pub fn compute_heartbeat_fingerprint(&self) -> i32 {
        // Clone via JSON to ensure deep copy
        let json_str = match serde_json::to_string(self) {
            Ok(s) => s,
            Err(_) => return 0,
        };

        let mut copy: HeartbeatData = match serde_json::from_str(&json_str) {
            Ok(c) => c,
            Err(_) => return 0,
        };

        // Reset subVersion to 0 for all consumer subscriptions
        let mut new_consumer_set = HashSet::new();
        for mut consumer_data in copy.consumer_data_set {
            let mut new_subscription_set = HashSet::new();
            for mut subscription_data in consumer_data.subscription_data_set {
                subscription_data.sub_version = 0;
                new_subscription_set.insert(subscription_data);
            }
            consumer_data.subscription_data_set = new_subscription_set;
            new_consumer_set.insert(consumer_data);
        }
        copy.consumer_data_set = new_consumer_set;

        // Reset fields that should not affect fingerprint
        copy.is_without_sub = false;
        copy.heartbeat_fingerprint = 0;
        copy.client_id = CheetahString::new();

        // Serialize to JSON and compute Java String hashCode
        match serde_json::to_string(&copy) {
            Ok(final_json) => JavaStringHasher::hash_str(&final_json),
            Err(_) => 0,
        }
    }
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

    #[test]
    fn test_compute_heartbeat_fingerprint_empty() {
        let heartbeat_data = HeartbeatData {
            client_id: "client1".into(),
            producer_data_set: HashSet::new(),
            consumer_data_set: HashSet::new(),
            heartbeat_fingerprint: 0,
            is_without_sub: false,
        };

        let fingerprint = heartbeat_data.compute_heartbeat_fingerprint();
        assert_ne!(fingerprint, 0);
    }

    #[test]
    fn test_compute_heartbeat_fingerprint_consistency() {
        let mut producer_set = HashSet::new();
        producer_set.insert(ProducerData {
            group_name: "producer_group1".into(),
        });

        let heartbeat_data = HeartbeatData {
            client_id: "client1".into(),
            producer_data_set: producer_set.clone(),
            consumer_data_set: HashSet::new(),
            heartbeat_fingerprint: 0,
            is_without_sub: false,
        };

        let fingerprint1 = heartbeat_data.compute_heartbeat_fingerprint();
        let fingerprint2 = heartbeat_data.compute_heartbeat_fingerprint();

        // Same data should produce same fingerprint
        assert_eq!(fingerprint1, fingerprint2);
    }

    #[test]
    fn test_compute_heartbeat_fingerprint_changes_with_data() {
        let mut producer_set1 = HashSet::new();
        producer_set1.insert(ProducerData {
            group_name: "producer_group1".into(),
        });

        let mut producer_set2 = HashSet::new();
        producer_set2.insert(ProducerData {
            group_name: "producer_group2".into(),
        });

        let heartbeat_data1 = HeartbeatData {
            client_id: "client1".into(),
            producer_data_set: producer_set1,
            consumer_data_set: HashSet::new(),
            heartbeat_fingerprint: 0,
            is_without_sub: false,
        };

        let heartbeat_data2 = HeartbeatData {
            client_id: "client1".into(),
            producer_data_set: producer_set2,
            consumer_data_set: HashSet::new(),
            heartbeat_fingerprint: 0,
            is_without_sub: false,
        };

        let fingerprint1 = heartbeat_data1.compute_heartbeat_fingerprint();
        let fingerprint2 = heartbeat_data2.compute_heartbeat_fingerprint();

        // Different data should produce different fingerprints
        assert_ne!(fingerprint1, fingerprint2);
    }
}
