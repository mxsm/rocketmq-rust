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

use std::collections::BTreeSet;
use std::hash::Hash;
use std::hash::Hasher;

use cheetah_string::CheetahString;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::TimeUtils::get_current_millis;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionData {
    pub class_filter_mode: bool,
    pub topic: CheetahString,
    pub sub_string: CheetahString,
    pub tags_set: BTreeSet<CheetahString>,
    pub code_set: BTreeSet<i32>,
    pub sub_version: i64,
    pub expression_type: CheetahString,
    // In Rust, attributes like `@JSONField(serialize = false)` are typically handled through
    // documentation or external crates.
    #[serde(skip)]
    pub filter_class_source: CheetahString, // This field is not used in this example.
}

impl Default for SubscriptionData {
    fn default() -> Self {
        SubscriptionData {
            class_filter_mode: false,
            topic: CheetahString::new(),
            sub_string: CheetahString::new(),
            tags_set: BTreeSet::new(),
            code_set: BTreeSet::new(),
            sub_version: get_current_millis() as i64,
            expression_type: CheetahString::from_static_str(ExpressionType::TAG),
            filter_class_source: CheetahString::new(),
        }
    }
}

impl SubscriptionData {
    pub const SUB_ALL: &'static str = "*";
}

impl Hash for SubscriptionData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.class_filter_mode.hash(state);
        self.topic.hash(state);
        self.sub_string.hash(state);
        self.tags_set.iter().for_each(|tag| tag.hash(state));
        self.code_set.iter().for_each(|code| code.hash(state));
        self.sub_version.hash(state);
        self.expression_type.hash(state);
        self.filter_class_source.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    #[test]
    fn subscription_data_default() {
        let before = get_current_millis() as i64;
        let data = SubscriptionData::default();
        let after = get_current_millis() as i64;

        assert!(!data.class_filter_mode);
        assert!(data.topic.is_empty());
        assert!(data.sub_string.is_empty());
        assert!(data.tags_set.is_empty());
        assert!(data.code_set.is_empty());
        assert!(data.sub_version >= before && data.sub_version <= after);
        assert_eq!(data.expression_type, ExpressionType::TAG);
        assert!(data.filter_class_source.is_empty());
    }

    #[test]
    fn subscription_data_sub_all_constant() {
        assert_eq!(SubscriptionData::SUB_ALL, "*");
    }

    #[test]
    fn subscription_data_hash_and_eq() {
        let data1 = SubscriptionData {
            topic: CheetahString::from("topic"),
            sub_version: 12345,
            ..Default::default()
        };

        let mut data2 = data1.clone();
        assert_eq!(data1, data2);

        let mut hasher1 = DefaultHasher::new();
        data1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        data2.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2);

        data2.sub_version = 54321;
        assert_ne!(data1, data2);
    }

    #[test]
    fn subscription_data_serde() {
        let mut data = SubscriptionData {
            topic: CheetahString::from("test_topic"),
            filter_class_source: CheetahString::from("source"),
            ..Default::default()
        };
        data.tags_set.insert(CheetahString::from("tag1"));
        data.code_set.insert(1);

        let json = serde_json::to_string(&data).unwrap();
        assert!(json.contains("\"topic\":\"test_topic\""));
        assert!(json.contains("\"tagsSet\":[\"tag1\"]"));
        assert!(json.contains("\"codeSet\":[1]"));
        assert!(!json.contains("filterClassSource"));
        assert!(!json.contains("source"));

        let deserialized: SubscriptionData = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.topic, data.topic);
        assert_eq!(deserialized.tags_set, data.tags_set);
        assert_eq!(deserialized.code_set, data.code_set);
        assert_eq!(deserialized.sub_version, data.sub_version);
        assert!(deserialized.filter_class_source.is_empty());
    }
}
