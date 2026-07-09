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

pub const LIST_CONSUMER_GROUPS_TOOL: &str = "mq_list_consumer_groups";
pub const QUERY_CONSUMER_LAG_TOOL: &str = "mq_query_consumer_lag";

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ListConsumerGroupsArgs {
    #[serde(default)]
    pub cluster: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct ConsumerGroupSummary {
    pub group: String,
    pub version: i32,
    pub client_count: i32,
    pub consume_type: String,
    pub message_model: String,
    pub consume_tps: f64,
    pub diff_total: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct ListConsumerGroupsOutput {
    pub cluster: String,
    pub namesrv_addr: String,
    pub consumer_group_count: usize,
    pub groups: Vec<ConsumerGroupSummary>,
    pub generated_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct QueryConsumerLagArgs {
    pub cluster: String,
    pub topic: String,
    pub consumer_group: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct QueueLag {
    pub topic: String,
    pub broker_name: String,
    pub queue_id: i32,
    pub broker_offset: i64,
    pub consumer_offset: i64,
    pub lag: i64,
    pub inflight: i64,
    pub last_timestamp: i64,
    pub client_ip: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct QueryConsumerLagOutput {
    pub cluster: String,
    pub namesrv_addr: String,
    pub topic: String,
    pub consumer_group: String,
    pub total_lag: i64,
    pub max_queue_lag: i64,
    pub queue_count: usize,
    pub consume_tps: f64,
    pub inflight_total: i64,
    pub queues: Vec<QueueLag>,
    pub generated_at: String,
}
