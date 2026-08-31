// Copyright 2026 The RocketMQ Rust Authors
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

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GetTopicConfigArgs {
    pub cluster: String,
    pub topic: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TopicConfigDifferenceField {
    ReadQueueNums,
    WriteQueueNums,
    Perm,
    Order,
    MessageType,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct TopicConfigObservationRow {
    pub broker_name: String,
    pub version: u64,
    pub read_queue_nums: u32,
    pub write_queue_nums: u32,
    pub perm: u32,
    pub order: bool,
    pub message_type: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct GetTopicConfigOutput {
    pub cluster: String,
    pub topic: String,
    pub brokers: Vec<TopicConfigObservationRow>,
    pub inconsistent_fields: Vec<TopicConfigDifferenceField>,
    pub generated_at: String,
}
