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

use crate::protocol::static_topic::topic_queue_info::TopicQueueMappingInfo;
use crate::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TopicConfigSerializeWrapper {
    #[serde(rename = "topicConfigTable")]
    pub topic_config_table: HashMap<CheetahString, TopicConfig>,
    #[serde(rename = "dataVersion")]
    pub data_version: DataVersion,
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
        //assert_eq!(wrapper.mapping_data_version, DataVersion::new());
        assert!(wrapper.topic_config_serialize_wrapper().topic_config_table().is_empty());
        // assert_eq!(
        //     wrapper.topic_config_serialize_wrapper().data_version(),
        //     &DataVersion::new()
        // );
    }

    #[test]
    fn topic_config_and_mapping_serialize_wrapper_getters() {
        let mut wrapper = TopicConfigAndMappingSerializeWrapper::default();
        let _topic_config = TopicConfig::default();
        let topic_queue_mapping_info = ArcMut::new(TopicQueueMappingInfo::default());
        let topic_queue_mapping_detail = ArcMut::<TopicQueueMappingDetail>::default();
        //let data_version = DataVersion::default();

        let topic_config_serialize_wrapper = TopicConfigSerializeWrapper::default();
        wrapper.topic_config_serialize_wrapper = topic_config_serialize_wrapper.clone();
        wrapper
            .topic_queue_mapping_info_map
            .insert("test".into(), topic_queue_mapping_info.clone());
        wrapper
            .topic_queue_mapping_detail_map
            .insert("test".into(), topic_queue_mapping_detail.clone());

        //assert_eq!(wrapper.mapping_data_version(), &data_version);
        assert_eq!(
            wrapper.topic_config_serialize_wrapper(),
            &topic_config_serialize_wrapper
        );
    }
}
