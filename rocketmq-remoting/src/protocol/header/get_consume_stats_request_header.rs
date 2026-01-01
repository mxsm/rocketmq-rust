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
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Debug, Serialize, Deserialize, RequestHeaderCodecV2)]
pub struct GetConsumeStatsRequestHeader {
    #[serde(rename = "consumerGroup")]
    pub consumer_group: CheetahString,
    #[serde(rename = "topic")]
    pub topic: CheetahString,
    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl GetConsumeStatsRequestHeader {
    // pub const CONSUMER_GROUP: &'static str = "consumerGroup";
    // pub const TOPIC: &'static str = "topic";

    pub fn get_consumer_group(&self) -> &CheetahString {
        &self.consumer_group
    }
    pub fn set_consumer_group(&mut self, consumer_group: CheetahString) {
        self.consumer_group = consumer_group;
    }

    pub fn get_topic(&self) -> &CheetahString {
        &self.topic
    }
    pub fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }
}
