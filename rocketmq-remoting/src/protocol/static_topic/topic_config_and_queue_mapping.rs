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

use rocketmq_common::common::config::TopicConfig;
use rocketmq_rust::ArcMut;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct TopicConfigAndQueueMapping {
    #[serde(flatten)]
    pub topic_config: TopicConfig,

    #[serde(rename = "mappingDetail")]
    pub topic_queue_mapping_detail: Option<ArcMut<TopicQueueMappingDetail>>,
}

impl TopicConfigAndQueueMapping {
    pub fn new(topic_config: TopicConfig, topic_queue_mapping_detail: Option<ArcMut<TopicQueueMappingDetail>>) -> Self {
        Self {
            topic_config,
            topic_queue_mapping_detail,
        }
    }

    pub fn get_topic_queue_mapping_detail(&self) -> Option<&ArcMut<TopicQueueMappingDetail>> {
        self.topic_queue_mapping_detail.as_ref()
    }

    pub fn set_topic_queue_mapping_detail(
        &mut self,
        topic_queue_mapping_detail: Option<ArcMut<TopicQueueMappingDetail>>,
    ) {
        self.topic_queue_mapping_detail = topic_queue_mapping_detail;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::static_topic::topic_queue_mapping_info::TopicQueueMappingInfo;

    #[test]
    fn topic_config_and_queue_mapping_default() {
        let mapping = TopicConfigAndQueueMapping::default();
        assert_eq!(mapping.topic_config, TopicConfig::default());
        assert!(mapping.topic_queue_mapping_detail.is_none());
    }

    #[test]
    fn topic_config_and_queue_mapping_serde() {
        let topic_config = TopicConfig::new("test_topic");
        let mapping_detail = TopicQueueMappingDetail {
            topic_queue_mapping_info: TopicQueueMappingInfo {
                topic: Some("test_topic".into()),
                ..TopicQueueMappingInfo::default()
            },
            ..Default::default()
        };
        let mapping = TopicConfigAndQueueMapping::new(topic_config, Some(ArcMut::new(mapping_detail)));

        let json = serde_json::to_string(&mapping).unwrap();
        let expected = r#"{"topicName":"test_topic","readQueueNums":16,"writeQueueNums":16,"perm":6,"topicFilterType":"SINGLE_TAG","topicSysFlag":0,"order":false,"attributes":{},"mappingDetail":{"topic":"test_topic","scope":"__global__","totalQueues":0,"bname":null,"epoch":0,"dirty":false,"currIdMap":null,"hostedQueues":null}}"#;
        assert_eq!(json, expected);

        let deserialized: TopicConfigAndQueueMapping = serde_json::from_str(&json).unwrap();
        assert_eq!(mapping.topic_config.topic_name, deserialized.topic_config.topic_name);
        assert_eq!(
            mapping
                .topic_queue_mapping_detail
                .as_ref()
                .unwrap()
                .topic_queue_mapping_info
                .topic,
            deserialized
                .topic_queue_mapping_detail
                .as_ref()
                .unwrap()
                .topic_queue_mapping_info
                .topic
        );
    }

    #[test]
    fn topic_config_and_queue_mapping_new() {
        let topic_config = TopicConfig::new("test_topic");
        let mapping_detail = ArcMut::new(TopicQueueMappingDetail::default());
        let mapping = TopicConfigAndQueueMapping::new(topic_config.clone(), Some(mapping_detail.clone()));
        assert_eq!(mapping.topic_config, topic_config);
        assert_eq!(mapping.topic_queue_mapping_detail.as_ref().unwrap(), &mapping_detail);
    }

    #[test]
    fn get_set_topic_queue_mapping_detail() {
        let mut mapping = TopicConfigAndQueueMapping::default();
        let mapping_detail = ArcMut::new(TopicQueueMappingDetail::default());
        mapping.set_topic_queue_mapping_detail(Some(mapping_detail.clone()));
        assert_eq!(mapping.get_topic_queue_mapping_detail().unwrap(), &mapping_detail);

        mapping.set_topic_queue_mapping_detail(None);
        assert!(mapping.get_topic_queue_mapping_detail().is_none());
    }
}
