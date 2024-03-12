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
use serde::{Deserialize, Serialize};

use crate::protocol::{
    static_topic::{
        topic_queue_info::TopicQueueMappingInfo,
        topic_queue_mapping_detail::TopicQueueMappingDetail,
    },
    DataVersion, RemotingSerializable,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfigAndMappingSerializeWrapper {
    #[serde(rename = "topicQueueMappingInfoMap")]
    pub topic_queue_mapping_info_map: HashMap<String /* topic */, TopicQueueMappingInfo>,
    #[serde(rename = "topicQueueMappingDetailMap")]
    pub topic_queue_mapping_detail_map: HashMap<String /* topic */, TopicQueueMappingDetail>,
    #[serde(rename = "mappingDataVersion")]
    pub mapping_data_version: DataVersion,

    #[serde(rename = "topicConfigTable")]
    pub topic_config_table: Option<HashMap<String, TopicConfig>>,

    #[serde(rename = "dataVersion")]
    pub data_version: Option<DataVersion>,
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
    pub fn topic_config_table(&self) -> &Option<HashMap<String, TopicConfig>> {
        &self.topic_config_table
    }
    pub fn data_version(&self) -> &Option<DataVersion> {
        &self.data_version
    }
}

impl Default for TopicConfigAndMappingSerializeWrapper {
    fn default() -> Self {
        Self {
            topic_queue_mapping_info_map: HashMap::new(),
            topic_queue_mapping_detail_map: HashMap::new(),
            mapping_data_version: DataVersion::new(),
            topic_config_table: None,
            data_version: None,
        }
    }
}

impl RemotingSerializable for TopicConfigAndMappingSerializeWrapper {
    type Output = TopicConfigAndMappingSerializeWrapper;

    /*fn decode(bytes: &[u8]) -> Self::Output {
        serde_json::from_slice::<Self::Output>(bytes).unwrap()
    }

    fn encode(&self, _compress: bool) -> Vec<u8> {
        todo!()
    }*/
}
