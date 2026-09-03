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
use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_model::common::hasher::string_hasher::JavaStringHasher;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::heartbeat::consume_type::ConsumeType;
use crate::protocol::heartbeat::consumer_data::ConsumerData;
use crate::protocol::heartbeat::message_model::MessageModel;
use crate::protocol::heartbeat::producer_data::ProducerData;
use crate::protocol::heartbeat::subscription_data::SubscriptionData;

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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CanonicalHeartbeatData {
    #[serde(rename = "clientID")]
    client_id: &'static str,
    producer_data_set: Vec<ProducerData>,
    consumer_data_set: Vec<CanonicalConsumerData>,
    heartbeat_fingerprint: i32,
    #[serde(rename = "withoutSub")]
    is_without_sub: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CanonicalConsumerData {
    group_name: CheetahString,
    consume_type: ConsumeType,
    message_model: MessageModel,
    consume_from_where: ConsumeFromWhere,
    subscription_data_set: Vec<SubscriptionData>,
    unit_mode: bool,
}

impl From<&ConsumerData> for CanonicalConsumerData {
    fn from(value: &ConsumerData) -> Self {
        let mut subscription_data_set = value.subscription_data_set.iter().cloned().collect::<Vec<_>>();
        for subscription in &mut subscription_data_set {
            subscription.sub_version = 0;
        }
        subscription_data_set.sort_unstable();

        Self {
            group_name: value.group_name.clone(),
            consume_type: value.consume_type,
            message_model: value.message_model,
            consume_from_where: value.consume_from_where,
            subscription_data_set,
            unit_mode: value.unit_mode,
        }
    }
}

impl HeartbeatData {
    /// Compute fingerprint for HeartbeatV2 protocol
    pub fn compute_heartbeat_fingerprint(&self) -> i32 {
        match self.canonical_fingerprint_json() {
            Ok(json) => JavaStringHasher::hash_str(&json),
            Err(_) => 0,
        }
    }

    fn canonical_fingerprint_json(&self) -> serde_json::Result<String> {
        let mut producer_data_set = self.producer_data_set.iter().cloned().collect::<Vec<_>>();
        producer_data_set.sort_unstable_by(|left, right| left.group_name.cmp(&right.group_name));

        let mut keyed_consumers = Vec::with_capacity(self.consumer_data_set.len());
        for consumer in &self.consumer_data_set {
            let canonical = CanonicalConsumerData::from(consumer);
            let sort_key = serde_json::to_vec(&canonical)?;
            keyed_consumers.push((sort_key, canonical));
        }
        keyed_consumers.sort_unstable_by(|left, right| left.0.cmp(&right.0));

        serde_json::to_string(&CanonicalHeartbeatData {
            client_id: "",
            producer_data_set,
            consumer_data_set: keyed_consumers.into_iter().map(|(_, consumer)| consumer).collect(),
            heartbeat_fingerprint: 0,
            is_without_sub: false,
        })
    }
}
#[cfg(test)]
mod tests {
    use std::collections::HashSet;

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
        let deserialized = serde_json::from_slice::<HeartbeatData>(serialized.as_slice()).unwrap();

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
        let deserialized = serde_json::from_slice::<HeartbeatData>(serialized.as_slice()).unwrap();

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
        let deserialized = serde_json::from_slice::<HeartbeatData>(serialized.as_slice()).unwrap();

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
        let producer_data_set = ["producer-c", "producer-a", "producer-b"]
            .into_iter()
            .map(|group_name| ProducerData {
                group_name: group_name.into(),
            })
            .collect();
        let consumer_data_set = ["consumer-b", "consumer-a"]
            .into_iter()
            .map(|group_name| ConsumerData {
                group_name: group_name.into(),
                subscription_data_set: ["topic-c", "topic-a", "topic-b"]
                    .into_iter()
                    .map(|topic| SubscriptionData {
                        topic: topic.into(),
                        sub_version: 123,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect();

        let heartbeat_data = HeartbeatData {
            client_id: "client1".into(),
            producer_data_set,
            consumer_data_set,
            heartbeat_fingerprint: 0,
            is_without_sub: false,
        };

        let fingerprints = (0..32)
            .map(|_| heartbeat_data.compute_heartbeat_fingerprint())
            .collect::<HashSet<_>>();
        assert_eq!(fingerprints.len(), 1);
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
