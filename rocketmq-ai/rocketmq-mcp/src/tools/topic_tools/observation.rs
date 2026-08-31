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

use crate::model::contract::Page;
use crate::model::contract::PageRequest;

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GetTopicStatsArgs {
    pub cluster: String,
    pub topic: String,
    #[serde(flatten)]
    pub page: PageRequest,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct TopicStatsQueueRow {
    pub broker_name: String,
    pub queue_id: i32,
    pub min_offset: i64,
    pub max_offset: i64,
    pub message_count: u64,
    pub last_update_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct GetTopicStatsOutput {
    pub cluster: String,
    pub topic: String,
    pub total_message_count: u64,
    pub queue_count: usize,
    pub truncated: bool,
    #[serde(flatten)]
    #[schemars(flatten)]
    pub page: Page<TopicStatsQueueRow>,
    pub generated_at: String,
}
