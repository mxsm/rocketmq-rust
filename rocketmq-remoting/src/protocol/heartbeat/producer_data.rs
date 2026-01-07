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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Hash, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProducerData {
    pub group_name: CheetahString,
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn producer_data_default_values() {
        let producer_data = ProducerData::default();
        assert_eq!(producer_data.group_name, CheetahString::new());
    }

    #[test]
    fn producer_data_equality() {
        let producer_data1 = ProducerData {
            group_name: CheetahString::from("group1"),
        };

        let producer_data2 = ProducerData {
            group_name: CheetahString::from("group1"),
        };

        assert_eq!(producer_data1, producer_data2);
    }

    #[test]
    fn producer_data_inequality() {
        let producer_data1 = ProducerData {
            group_name: CheetahString::from("group1"),
        };

        let producer_data2 = ProducerData {
            group_name: CheetahString::from("group2"),
        };

        assert_ne!(producer_data1, producer_data2);
    }

    #[test]
    fn serialize_producer_data() {
        let producer_data = ProducerData {
            group_name: CheetahString::from("group1"),
        };
        let serialized = serde_json::to_string(&producer_data).unwrap();
        assert_eq!(serialized, r#"{"groupName":"group1"}"#);
    }

    #[test]
    fn deserialize_producer_data() {
        let json = r#"{"groupName":"group1"}"#;
        let deserialized: ProducerData = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.group_name, CheetahString::from("group1"));
    }

    #[test]
    fn producer_data_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hash;
        use std::hash::Hasher;

        let producer_data = ProducerData {
            group_name: CheetahString::from("group1"),
        };

        let mut hasher = DefaultHasher::new();
        producer_data.hash(&mut hasher);
        let hash1 = hasher.finish();

        let mut hasher = DefaultHasher::new();
        producer_data.hash(&mut hasher);
        let hash2 = hasher.finish();

        assert_eq!(hash1, hash2);
    }
}
