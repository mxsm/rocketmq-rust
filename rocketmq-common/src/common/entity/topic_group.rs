//  Copyright 2023 The RocketMQ Rust Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::hash::Hash;
use std::hash::Hasher;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default, Eq)]
pub struct TopicGroup {
    #[serde(default)]
    pub topic: CheetahString,

    #[serde(default)]
    pub group: CheetahString,
}

impl TopicGroup {
    pub fn from_parts(topic: CheetahString, group: CheetahString) -> Self {
        Self { topic, group }
    }

    #[must_use]
    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn with_topic(&mut self, topic: CheetahString) -> &mut Self {
        self.topic = topic;
        self
    }

    #[must_use]
    pub fn group(&self) -> &CheetahString {
        &self.group
    }

    pub fn with_group(&mut self, group: CheetahString) -> &mut Self {
        self.group = group;
        self
    }
}

impl PartialEq for TopicGroup {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic && self.group == other.group
    }
}

impl Hash for TopicGroup {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.topic.hash(state);
        self.group.hash(state);
    }
}

impl std::fmt::Display for TopicGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TopicGroup{{topic='{}', group='{}'}}",
            self.topic, self.group
        )
    }
}
