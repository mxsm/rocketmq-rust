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
use std::hash::Hash;

use cheetah_string::CheetahString;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::heartbeat::consume_type::ConsumeType;
use crate::protocol::heartbeat::message_model::MessageModel;
use crate::protocol::heartbeat::subscription_data::SubscriptionData;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerData {
    pub group_name: CheetahString,
    pub consume_type: ConsumeType,
    pub message_model: MessageModel,
    pub consume_from_where: ConsumeFromWhere,
    pub subscription_data_set: HashSet<SubscriptionData>,
    pub unit_mode: bool,
}

impl Hash for ConsumerData {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.group_name.hash(state);
        self.consume_type.hash(state);
        self.message_model.hash(state);
        self.consume_from_where.hash(state);
        self.subscription_data_set.iter().for_each(|code| code.hash(state));
        self.unit_mode.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;

    use super::*;
    use crate::protocol::heartbeat::consume_type::ConsumeType;
    use crate::protocol::heartbeat::message_model::MessageModel;
    use crate::protocol::heartbeat::subscription_data::SubscriptionData;

    #[test]
    fn consumer_data_default_values() {
        let consumer_data = ConsumerData::default();
        assert_eq!(consumer_data.group_name, CheetahString::new());
        assert_eq!(consumer_data.consume_type, ConsumeType::default());
        assert_eq!(consumer_data.message_model, MessageModel::default());
        assert_eq!(consumer_data.consume_from_where, ConsumeFromWhere::default());
        assert!(consumer_data.subscription_data_set.is_empty());
        assert!(!consumer_data.unit_mode);
    }

    #[test]
    fn consumer_data_equality() {
        let mut subscription_data_set = HashSet::new();
        subscription_data_set.insert(SubscriptionData::default());

        let consumer_data1 = ConsumerData {
            group_name: CheetahString::from("group1"),
            consume_type: ConsumeType::default(),
            message_model: MessageModel::default(),
            consume_from_where: ConsumeFromWhere::default(),
            subscription_data_set: subscription_data_set.clone(),
            unit_mode: false,
        };

        let consumer_data2 = ConsumerData {
            group_name: CheetahString::from("group1"),
            consume_type: ConsumeType::default(),
            message_model: MessageModel::default(),
            consume_from_where: ConsumeFromWhere::default(),
            subscription_data_set,
            unit_mode: false,
        };

        assert_eq!(consumer_data1, consumer_data2);
    }

    #[test]
    fn consumer_data_inequality() {
        let consumer_data1 = ConsumerData {
            group_name: CheetahString::from("group1"),
            consume_type: ConsumeType::default(),
            message_model: MessageModel::default(),
            consume_from_where: ConsumeFromWhere::default(),
            subscription_data_set: HashSet::new(),
            unit_mode: false,
        };

        let consumer_data2 = ConsumerData {
            group_name: CheetahString::from("group2"),
            consume_type: ConsumeType::default(),
            message_model: MessageModel::default(),
            consume_from_where: ConsumeFromWhere::default(),
            subscription_data_set: HashSet::new(),
            unit_mode: false,
        };

        assert_ne!(consumer_data1, consumer_data2);
    }

    #[test]
    fn consumer_data_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hash;
        use std::hash::Hasher;

        let consumer_data = ConsumerData {
            group_name: CheetahString::from("group1"),
            consume_type: ConsumeType::default(),
            message_model: MessageModel::default(),
            consume_from_where: ConsumeFromWhere::default(),
            subscription_data_set: HashSet::new(),
            unit_mode: false,
        };

        let mut hasher = DefaultHasher::new();
        consumer_data.hash(&mut hasher);
        let hash1 = hasher.finish();

        let mut hasher = DefaultHasher::new();
        consumer_data.hash(&mut hasher);
        let hash2 = hasher.finish();

        assert_eq!(hash1, hash2);
    }
}
