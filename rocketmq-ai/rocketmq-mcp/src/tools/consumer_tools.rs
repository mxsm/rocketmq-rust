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

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GetConsumerGroupDetailsArgs {
    pub cluster: String,
    pub consumer_group: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerGroupConfigPresence {
    Present,
    Absent,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerConnectionState {
    Online,
    Offline,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerConsumeType {
    Pull,
    Push,
    Pop,
    Unknown,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerMessageModel {
    Broadcasting,
    Clustering,
    Unknown,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerConsumeFromWhere {
    LastOffset,
    LastOffsetAndMinFirst,
    MinOffset,
    MaxOffset,
    FirstOffset,
    Timestamp,
    Unknown,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ConsumerGroupDetailsBrokerRow {
    pub broker_name: String,
    pub config_state: ConsumerGroupConfigPresence,
    pub config_version: Option<u64>,
    pub consume_enable: Option<bool>,
    pub consume_from_min_enable: Option<bool>,
    pub consume_broadcast_enable: Option<bool>,
    pub consume_message_orderly: Option<bool>,
    pub retry_queue_nums: Option<i32>,
    pub retry_max_times: Option<i32>,
    pub notify_consumer_ids_changed_enable: Option<bool>,
    pub consume_timeout_minutes: Option<i32>,
    pub connection_state: Option<ConsumerConnectionState>,
    pub connection_count: u64,
    pub consume_type: Option<ConsumerConsumeType>,
    pub message_model: Option<ConsumerMessageModel>,
    pub consume_from_where: Option<ConsumerConsumeFromWhere>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct GetConsumerGroupDetailsOutput {
    pub cluster: String,
    pub consumer_group: String,
    pub total_connection_count: u64,
    pub brokers: Vec<ConsumerGroupDetailsBrokerRow>,
    pub generated_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GetConsumerProgressArgs {
    pub cluster: String,
    pub consumer_group: String,
    #[serde(flatten)]
    pub page: PageRequest,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerProgressState {
    NoConsumption,
    Observed,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ConsumerProgressQueueRow {
    pub topic: String,
    pub broker_name: String,
    pub queue_id: i32,
    pub broker_offset: i64,
    pub consumer_offset: i64,
    pub pull_offset: i64,
    pub lag: u64,
    pub inflight: u64,
    pub last_observed_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct GetConsumerProgressOutput {
    pub cluster: String,
    pub consumer_group: String,
    pub state: ConsumerProgressState,
    pub topic_count: usize,
    pub queue_count: usize,
    pub total_lag: u64,
    pub max_queue_lag: u64,
    pub total_inflight: u64,
    pub consume_tps: f64,
    pub truncated: bool,
    #[serde(flatten)]
    #[schemars(flatten)]
    pub page: Page<ConsumerProgressQueueRow>,
    pub generated_at: String,
}
