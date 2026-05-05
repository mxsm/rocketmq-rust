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

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    pub topic_id: i32,
    pub topic: String,
    pub reserve_time_millis: i64,
    pub status: i32,
    pub update_timestamp: i64,
}

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicQueueMetadata {
    pub topic: String,
    pub queue_id: i32,
    pub min_offset: i64,
    pub max_offset: i64,
    pub update_timestamp: i64,
}

impl TopicQueueMetadata {
    pub fn update_max_offset(&mut self, max_offset: i64, update_timestamp: i64) {
        self.max_offset = self.max_offset.max(max_offset);
        self.update_timestamp = update_timestamp;
    }
}
