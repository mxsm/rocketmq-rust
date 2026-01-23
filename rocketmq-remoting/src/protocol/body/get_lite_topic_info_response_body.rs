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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::admin::topic_offset::TopicOffset;
use rocketmq_common::common::entity::ClientGroup;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetLiteTopicInfoResponseBody {
    #[serde(default)]
    parent_topic: CheetahString,

    #[serde(default)]
    lite_topic: CheetahString,

    #[serde(default)]
    subscriber: HashSet<ClientGroup>,

    #[serde(skip_serializing_if = "Option::is_none")]
    topic_offset: Option<TopicOffset>,

    #[serde(default)]
    sharding_to_broker: bool,
}

impl GetLiteTopicInfoResponseBody {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn parent_topic(&self) -> &CheetahString {
        &self.parent_topic
    }

    pub fn with_parent_topic(&mut self, parent_topic: CheetahString) -> &mut Self {
        self.parent_topic = parent_topic;
        self
    }

    #[must_use]
    pub fn lite_topic(&self) -> &CheetahString {
        &self.lite_topic
    }

    pub fn with_lite_topic(&mut self, lite_topic: CheetahString) -> &mut Self {
        self.lite_topic = lite_topic;
        self
    }

    #[must_use]
    pub fn subscriber(&self) -> &HashSet<ClientGroup> {
        &self.subscriber
    }

    pub fn with_subscriber(&mut self, subscriber: HashSet<ClientGroup>) -> &mut Self {
        self.subscriber = subscriber;
        self
    }

    pub fn add_subscriber(&mut self, subscriber: ClientGroup) -> &mut Self {
        self.subscriber.insert(subscriber);
        self
    }

    #[must_use]
    pub fn topic_offset(&self) -> Option<&TopicOffset> {
        self.topic_offset.as_ref()
    }

    pub fn with_topic_offset(&mut self, topic_offset: TopicOffset) -> &mut Self {
        self.topic_offset = Some(topic_offset);
        self
    }

    #[must_use]
    pub fn sharding_to_broker(&self) -> bool {
        self.sharding_to_broker
    }

    pub fn with_sharding_to_broker(&mut self, sharding_to_broker: bool) -> &mut Self {
        self.sharding_to_broker = sharding_to_broker;
        self
    }
}
