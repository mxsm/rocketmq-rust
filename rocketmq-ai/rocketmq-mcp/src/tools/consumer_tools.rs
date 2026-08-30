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
pub struct ListConsumerGroupsArgs {
    #[serde(default)]
    pub cluster: Option<String>,
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(flatten)]
    pub page: PageRequest,
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
    #[serde(skip_serializing)]
    #[schemars(skip)]
    pub namesrv_addr: String,
    #[serde(flatten)]
    #[schemars(flatten)]
    pub page: Page<ConsumerGroupSummary>,
    pub generated_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QueryConsumerLagArgs {
    pub cluster: String,
    pub topic: String,
    pub consumer_group: String,
    #[serde(flatten)]
    pub page: PageRequest,
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
    pub last_observed_at: Option<String>,
    #[serde(skip_serializing)]
    #[schemars(skip)]
    pub client_ip: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct QueryConsumerLagOutput {
    pub cluster: String,
    #[serde(skip_serializing)]
    #[schemars(skip)]
    pub namesrv_addr: String,
    pub topic: String,
    pub consumer_group: String,
    pub total_lag: i64,
    pub max_queue_lag: i64,
    pub consume_tps: f64,
    pub inflight_total: i64,
    #[serde(flatten)]
    #[schemars(flatten)]
    pub page: Page<QueueLag>,
    pub generated_at: String,
}
