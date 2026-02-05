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
use dashmap::DashMap;
use rocketmq_rust::ArcMut;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use crate::protocol::DataVersion;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TopicQueueMappingSerializeWrapper {
    topic_queue_mapping_info_map: Option<DashMap<CheetahString, ArcMut<TopicQueueMappingDetail>>>,
    data_version: Option<DataVersion>,
}

impl TopicQueueMappingSerializeWrapper {
    pub fn new(
        topic_queue_mapping_info_map: Option<DashMap<CheetahString, ArcMut<TopicQueueMappingDetail>>>,
        data_version: Option<DataVersion>,
    ) -> Self {
        Self {
            topic_queue_mapping_info_map,
            data_version,
        }
    }
}

impl TopicQueueMappingSerializeWrapper {
    pub fn topic_queue_mapping_info_map(&self) -> Option<&DashMap<CheetahString, ArcMut<TopicQueueMappingDetail>>> {
        self.topic_queue_mapping_info_map.as_ref()
    }

    pub fn take_topic_queue_mapping_info_map(
        &mut self,
    ) -> Option<DashMap<CheetahString, ArcMut<TopicQueueMappingDetail>>> {
        self.topic_queue_mapping_info_map.take()
    }

    pub fn data_version(&self) -> Option<&DataVersion> {
        match &self.data_version {
            None => None,
            Some(value) => Some(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_map() -> DashMap<CheetahString, ArcMut<TopicQueueMappingDetail>> {
        let map = DashMap::new();
        let detail = TopicQueueMappingDetail::default();
        map.insert(CheetahString::from("test_topic"), ArcMut::new(detail));
        map
    }

    #[test]
    fn test_new_and_getters() {
        let map = create_test_map();
        let version = DataVersion::default();

        let wrapper = TopicQueueMappingSerializeWrapper::new(Some(map), Some(version.clone()));

        assert!(wrapper.topic_queue_mapping_info_map().is_some());
        assert_eq!(wrapper.topic_queue_mapping_info_map().unwrap().len(), 1);
        assert!(wrapper.data_version().is_some());
    }

    #[test]
    fn test_default_values() {
        let wrapper = TopicQueueMappingSerializeWrapper::default();

        assert!(wrapper.topic_queue_mapping_info_map().is_none());
        assert!(wrapper.data_version().is_none());
    }

    #[test]
    fn test_take_topic_queue_mapping_info_map() {
        let map = create_test_map();
        let mut wrapper = TopicQueueMappingSerializeWrapper::new(Some(map), None);

        let taken_map = wrapper.take_topic_queue_mapping_info_map();

        assert!(taken_map.is_some());
        assert_eq!(taken_map.unwrap().len(), 1);
        assert!(wrapper.topic_queue_mapping_info_map().is_none());
    }

    #[test]
    fn test_serialization_deserialization() {
        let map = create_test_map();
        let version = DataVersion::default();
        let wrapper = TopicQueueMappingSerializeWrapper::new(Some(map), Some(version));

        let json = serde_json::to_string(&wrapper).expect("Should serialize");

        assert!(json.contains("topicQueueMappingInfoMap"));
        assert!(json.contains("dataVersion"));

        let deserialized: TopicQueueMappingSerializeWrapper = serde_json::from_str(&json).expect("Should deserialize");

        assert_eq!(deserialized.topic_queue_mapping_info_map().unwrap().len(), 1);
    }
}
