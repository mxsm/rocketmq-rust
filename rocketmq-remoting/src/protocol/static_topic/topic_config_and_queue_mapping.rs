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
