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

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_rust::ArcMut;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use crate::protocol::static_topic::topic_queue_mapping_info::TopicQueueMappingInfo;
use crate::protocol::DataVersion;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TopicConfigAndMappingSerializeWrapper {
    #[serde(rename = "topicQueueMappingInfoMap")]
    pub topic_queue_mapping_info_map: DashMap<CheetahString /* topic */, ArcMut<TopicQueueMappingInfo>>,

    #[serde(rename = "topicQueueMappingDetailMap")]
    pub topic_queue_mapping_detail_map: DashMap<CheetahString /* topic */, ArcMut<TopicQueueMappingDetail>>,

    #[serde(rename = "mappingDataVersion")]
    pub mapping_data_version: DataVersion,

    #[serde(flatten)]
    pub topic_config_serialize_wrapper: TopicConfigSerializeWrapper,
}

impl TopicConfigAndMappingSerializeWrapper {
    pub fn topic_queue_mapping_info_map(&self) -> &DashMap<CheetahString, ArcMut<TopicQueueMappingInfo>> {
        &self.topic_queue_mapping_info_map
    }

    pub fn topic_queue_mapping_detail_map(
        &self,
    ) -> &DashMap<CheetahString /* topic */, ArcMut<TopicQueueMappingDetail>> {
        &self.topic_queue_mapping_detail_map
    }

    pub fn mapping_data_version(&self) -> &DataVersion {
        &self.mapping_data_version
    }
    pub fn topic_config_serialize_wrapper(&self) -> &TopicConfigSerializeWrapper {
        &self.topic_config_serialize_wrapper
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TopicConfigSerializeWrapper {
    #[serde(rename = "topicConfigTable")]
    pub topic_config_table: HashMap<CheetahString, TopicConfig>,
    #[serde(rename = "dataVersion")]
    pub data_version: DataVersion,
}

impl TopicConfigSerializeWrapper {
    pub fn topic_config_table(&self) -> &HashMap<CheetahString, TopicConfig> {
        &self.topic_config_table
    }

    pub fn data_version(&self) -> &DataVersion {
        &self.data_version
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_config_and_mapping_serialize_wrapper_default() {
        let wrapper = TopicConfigAndMappingSerializeWrapper::default();
        assert!(wrapper.topic_queue_mapping_info_map.is_empty());
        assert!(wrapper.topic_queue_mapping_detail_map.is_empty());
        assert_eq!(wrapper.mapping_data_version, DataVersion::new());
        assert!(wrapper.topic_config_serialize_wrapper().topic_config_table().is_empty());
        assert_eq!(
            wrapper.topic_config_serialize_wrapper().data_version(),
            &DataVersion::new()
        );
    }

    #[test]
    fn topic_config_and_mapping_serialize_wrapper_getters() {
        let mut wrapper = TopicConfigAndMappingSerializeWrapper::default();
        let _topic_config = TopicConfig::default();
        let topic_queue_mapping_info = ArcMut::new(TopicQueueMappingInfo::default());
        let topic_queue_mapping_detail = ArcMut::<TopicQueueMappingDetail>::default();
        let data_version = DataVersion::default();

        let topic_config_serialize_wrapper = TopicConfigSerializeWrapper::default();
        wrapper.topic_config_serialize_wrapper = topic_config_serialize_wrapper.clone();
        wrapper
            .topic_queue_mapping_info_map
            .insert("test".into(), topic_queue_mapping_info.clone());
        wrapper
            .topic_queue_mapping_detail_map
            .insert("test".into(), topic_queue_mapping_detail.clone());

        assert_eq!(wrapper.mapping_data_version(), &data_version);
        assert_eq!(
            wrapper.topic_config_serialize_wrapper(),
            &topic_config_serialize_wrapper
        );
    }

    #[test]
    fn topic_config_serialize_wrapper_methods() {
        let mut wrapper = TopicConfigSerializeWrapper::default();
        let topic_config = TopicConfig::default();
        wrapper
            .topic_config_table
            .insert("test_topic".into(), topic_config.clone());
        let data_version = DataVersion::default();
        wrapper.data_version = data_version.clone();

        assert_eq!(
            wrapper
                .topic_config_table()
                .get(&CheetahString::from_static_str("test_topic")),
            Some(&topic_config)
        );
        assert_eq!(wrapper.data_version(), &data_version);
    }

    fn create_topic_config(name: &str) -> (CheetahString, TopicConfig) {
        (CheetahString::from(name), TopicConfig::default())
    }

    #[test]
    fn test_default_initialization() {
        let wrapper = TopicConfigSerializeWrapper::default();
        assert!(wrapper.topic_config_table.is_empty());
        assert_eq!(wrapper.data_version, DataVersion::default());
    }

    #[test]
    fn test_clone_trait() {
        let mut wrapper = TopicConfigSerializeWrapper::default();
        let (k, v) = create_topic_config("topic_a");
        wrapper.topic_config_table.insert(k.clone(), v.clone());

        let cloned = wrapper.clone();

        assert_eq!(wrapper, cloned);
        assert_eq!(cloned.topic_config_table.get(&k), Some(&v));
    }

    #[test]
    fn test_partial_eq_identical() {
        let mut w1 = TopicConfigSerializeWrapper::default();
        let mut w2 = TopicConfigSerializeWrapper::default();

        let (k, v) = create_topic_config("topic_eq");

        w1.topic_config_table.insert(k.clone(), v.clone());
        w2.topic_config_table.insert(k, v);

        assert_eq!(w1, w2);
    }

    #[test]
    fn test_partial_eq_different() {
        let mut w1 = TopicConfigSerializeWrapper::default();
        let mut w2 = TopicConfigSerializeWrapper::default();

        let (k1, v1) = create_topic_config("topic1");
        let (k2, v2) = create_topic_config("topic2");

        w1.topic_config_table.insert(k1, v1);
        w2.topic_config_table.insert(k2, v2);

        assert_ne!(w1, w2);
    }

    #[test]
    fn test_getters() {
        let mut wrapper = TopicConfigSerializeWrapper::default();

        let (k, v) = create_topic_config("topic_getter");
        wrapper.topic_config_table.insert(k.clone(), v.clone());

        let dv = DataVersion::default();
        wrapper.data_version = dv.clone();

        assert_eq!(wrapper.topic_config_table().get(&k), Some(&v));
        assert_eq!(wrapper.data_version(), &dv);
    }

    #[test]
    fn test_empty_topic_config_table() {
        let wrapper = TopicConfigSerializeWrapper::default();
        assert!(wrapper.topic_config_table().is_empty());
    }

    #[test]
    fn test_single_topic_config() {
        let mut wrapper = TopicConfigSerializeWrapper::default();
        let (k, v) = create_topic_config("single_topic");

        wrapper.topic_config_table.insert(k.clone(), v.clone());

        assert_eq!(wrapper.topic_config_table().len(), 1);
        assert_eq!(wrapper.topic_config_table().get(&k), Some(&v));
    }

    #[test]
    fn test_multiple_topic_configs() {
        let mut wrapper = TopicConfigSerializeWrapper::default();

        for i in 0..5 {
            let name = format!("topic_{i}");
            let (k, v) = create_topic_config(&name);
            wrapper.topic_config_table.insert(k, v);
        }

        assert_eq!(wrapper.topic_config_table().len(), 5);
    }

    #[test]
    fn test_data_version_behavior() {
        let mut wrapper = TopicConfigSerializeWrapper::default();
        let dv = DataVersion::default();

        wrapper.data_version = dv.clone();

        assert_eq!(wrapper.data_version(), &dv);
    }

    #[test]
    fn test_json_serialization_field_names() {
        let mut wrapper = TopicConfigSerializeWrapper::default();
        let (k, v) = create_topic_config("serde_topic");

        wrapper.topic_config_table.insert(k, v);

        let json = serde_json::to_string(&wrapper).unwrap();

        assert!(json.contains("topicConfigTable"));
        assert!(json.contains("dataVersion"));
    }

    #[test]
    fn test_json_deserialization_field_names() {
        let json = r#"
    {
        "topicConfigTable": {},
        "dataVersion": {
            "stateVersion": 0,
            "timestamp": 0,
            "counter": 0
        }
    }
    "#;

        let wrapper: TopicConfigSerializeWrapper = serde_json::from_str(json).unwrap();

        assert!(wrapper.topic_config_table.is_empty());
    }

    #[test]
    fn test_serde_roundtrip() {
        let mut wrapper = TopicConfigSerializeWrapper::default();

        let (k1, v1) = create_topic_config("topic_round_1");
        let (k2, v2) = create_topic_config("topic_round_2");

        wrapper.topic_config_table.insert(k1, v1);
        wrapper.topic_config_table.insert(k2, v2);

        let json = serde_json::to_string(&wrapper).unwrap();
        let decoded: TopicConfigSerializeWrapper = serde_json::from_str(&json).unwrap();

        assert_eq!(wrapper, decoded);
    }

    #[test]
    fn test_debug_trait_output() {
        let wrapper = TopicConfigSerializeWrapper::default();
        let debug_str = format!("{:?}", wrapper);

        assert!(debug_str.contains("TopicConfigSerializeWrapper"));
    }

    #[test]
    fn test_create_with_populated_fields() {
        let mut table = HashMap::new();
        let (k, v) = create_topic_config("populated_topic");

        table.insert(k.clone(), v.clone());

        let dv = DataVersion::default();

        let wrapper = TopicConfigSerializeWrapper {
            topic_config_table: table,
            data_version: dv.clone(),
        };

        assert_eq!(wrapper.topic_config_table().get(&k), Some(&v));
        assert_eq!(wrapper.data_version(), &dv);
    }
}
