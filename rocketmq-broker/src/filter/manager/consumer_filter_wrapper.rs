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

use serde::Deserialize;
use serde::Serialize;

use crate::filter::consumer_filter_data::ConsumerFilterData;

#[derive(Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerFilterWrapper {
    pub(crate) filter_data_by_topic: HashMap<String /* Topic */, FilterDataMapByTopic>,
}

#[derive(Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct FilterDataMapByTopic {
    pub(crate) filter_data_map: HashMap<String /* consumer group */, ConsumerFilterData>,
    pub(crate) topic: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_filter_wrapper_defaults_to_empty_usable_maps() {
        let wrapper = ConsumerFilterWrapper::default();

        assert!(wrapper.filter_data_by_topic.is_empty());
    }

    #[test]
    fn consumer_filter_wrapper_round_trips_camel_case_json() {
        let by_topic = FilterDataMapByTopic {
            filter_data_map: HashMap::from([("group-a".to_owned(), ConsumerFilterData::default())]),
            topic: "orders".to_owned(),
        };
        let wrapper = ConsumerFilterWrapper {
            filter_data_by_topic: HashMap::from([("orders".to_owned(), by_topic)]),
        };

        let json = serde_json::to_string(&wrapper).expect("wrapper should serialize");
        assert!(json.contains("\"filterDataByTopic\""));
        assert!(json.contains("\"filterDataMap\""));
        assert!(json.contains("\"topic\""));

        let decoded: ConsumerFilterWrapper = serde_json::from_str(&json).expect("wrapper should deserialize");
        let decoded_topic = decoded.filter_data_by_topic.get("orders").expect("topic entry");
        assert_eq!(decoded_topic.topic, "orders");
        assert!(decoded_topic.filter_data_map.contains_key("group-a"));
    }
}
