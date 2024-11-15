/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::collections::HashMap;

use rocketmq_common::common::config::TopicConfig;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::static_topic::topic_queue_info::TopicQueueMappingInfo;
use crate::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use crate::protocol::DataVersion;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TopicConfigAndMappingSerializeWrapper {
    #[serde(rename = "topicQueueMappingInfoMap")]
    pub topic_queue_mapping_info_map: HashMap<String /* topic */, TopicQueueMappingInfo>,

    #[serde(rename = "topicQueueMappingDetailMap")]
    pub topic_queue_mapping_detail_map: HashMap<String /* topic */, TopicQueueMappingDetail>,

    #[serde(rename = "mappingDataVersion")]
    pub mapping_data_version: DataVersion,

    #[serde(flatten)]
    pub topic_config_serialize_wrapper: TopicConfigSerializeWrapper,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TopicConfigSerializeWrapper {
    #[serde(rename = "topicConfigTable")]
    pub topic_config_table: HashMap<String, TopicConfig>,
    #[serde(rename = "dataVersion")]
    pub data_version: DataVersion,
}

impl TopicConfigAndMappingSerializeWrapper {
    pub fn topic_queue_mapping_info_map(&self) -> &HashMap<String, TopicQueueMappingInfo> {
        &self.topic_queue_mapping_info_map
    }

    pub fn topic_queue_mapping_detail_map(&self) -> &HashMap<String, TopicQueueMappingDetail> {
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
    pub fn topic_config_table(&self) -> &HashMap<String, TopicConfig> {
        &self.topic_config_table
    }

    pub fn data_version(&self) -> &DataVersion {
        &self.data_version
    }
}

impl Default for TopicConfigAndMappingSerializeWrapper {
    fn default() -> Self {
        Self {
            topic_queue_mapping_info_map: HashMap::new(),
            topic_queue_mapping_detail_map: HashMap::new(),
            mapping_data_version: DataVersion::new(),
            topic_config_serialize_wrapper: TopicConfigSerializeWrapper::default(),
        }
    }
}

impl Default for TopicConfigSerializeWrapper {
    fn default() -> Self {
        Self {
            topic_config_table: HashMap::new(),
            data_version: DataVersion::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn topic_config_and_mapping_serialize_wrapper_default() {
        let wrapper = TopicConfigAndMappingSerializeWrapper::default();
        assert!(wrapper.topic_queue_mapping_info_map.is_empty());
        assert!(wrapper.topic_queue_mapping_detail_map.is_empty());
        assert_eq!(wrapper.mapping_data_version, DataVersion::new());
        assert_eq!(
            wrapper
                .topic_config_serialize_wrapper()
                .topic_config_table()
                .is_empty(),
            true
        );
        assert_eq!(
            wrapper.topic_config_serialize_wrapper().data_version(),
            &DataVersion::new()
        );
    }

    #[test]
    fn topic_config_and_mapping_serialize_wrapper_getters() {
        let mut wrapper = TopicConfigAndMappingSerializeWrapper::default();
        let _topic_config = TopicConfig::default();
        let topic_queue_mapping_info = TopicQueueMappingInfo::default();
        let topic_queue_mapping_detail = TopicQueueMappingDetail::default();
        //let data_version = DataVersion::default();

        let topic_config_serialize_wrapper = TopicConfigSerializeWrapper::default();
        wrapper.topic_config_serialize_wrapper = topic_config_serialize_wrapper.clone();
        wrapper
            .topic_queue_mapping_info_map
            .insert("test".to_string(), topic_queue_mapping_info.clone());
        wrapper
            .topic_queue_mapping_detail_map
            .insert("test".to_string(), topic_queue_mapping_detail.clone());

        assert_eq!(
            wrapper.topic_queue_mapping_info_map(),
            &HashMap::from([("test".to_string(), topic_queue_mapping_info)])
        );
        assert_eq!(
            wrapper.topic_queue_mapping_detail_map(),
            &HashMap::from([("test".to_string(), topic_queue_mapping_detail)])
        );
        //assert_eq!(wrapper.mapping_data_version(), &data_version);
        assert_eq!(
            wrapper.topic_config_serialize_wrapper(),
            &topic_config_serialize_wrapper
        );
    }
}
