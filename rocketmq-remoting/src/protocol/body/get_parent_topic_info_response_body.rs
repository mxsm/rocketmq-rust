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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetParentTopicInfoResponseBody {
    topic: Option<CheetahString>,

    #[serde(default)]
    ttl: i32,

    #[serde(default)]
    groups: HashSet<CheetahString>,

    #[serde(default)]
    lmq_num: i32,

    #[serde(default)]
    lite_topic_count: i32,
}

impl GetParentTopicInfoResponseBody {
    pub fn new() -> Self {
        GetParentTopicInfoResponseBody::default()
    }

    pub fn get_topic(&self) -> Option<&CheetahString> {
        self.topic.as_ref()
    }

    pub fn set_topic(&mut self, topic: CheetahString) {
        self.topic = Some(topic);
    }

    pub fn get_ttl(&self) -> i32 {
        self.ttl
    }

    pub fn set_ttl(&mut self, ttl: i32) {
        self.ttl = ttl;
    }

    pub fn get_groups(&self) -> &HashSet<CheetahString> {
        &self.groups
    }

    pub fn set_groups(&mut self, groups: HashSet<CheetahString>) {
        self.groups = groups;
    }

    pub fn get_lmq_num(&self) -> i32 {
        self.lmq_num
    }

    pub fn set_lmq_num(&mut self, lmq_num: i32) {
        self.lmq_num = lmq_num;
    }

    pub fn get_lite_topic_count(&self) -> i32 {
        self.lite_topic_count
    }

    pub fn set_lite_topic_count(&mut self, lite_topic_count: i32) {
        self.lite_topic_count = lite_topic_count;
    }
}
