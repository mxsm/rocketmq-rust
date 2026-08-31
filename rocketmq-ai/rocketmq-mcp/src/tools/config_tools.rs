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
pub struct BrokerConfigSummaryArgs {
    pub cluster: String,
    pub broker_name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct BrokerConfigSummaryRow {
    pub broker_name: String,
    pub broker_id: u64,
    pub generation: u64,
    pub send_message_thread_pool_nums: Option<u32>,
    pub pull_message_thread_pool_nums: Option<u32>,
    pub flush_delay_offset_interval_ms: Option<u64>,
    pub max_client_event_count: Option<i32>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct BrokerConfigSummaryOutput {
    pub cluster: String,
    pub broker_name: String,
    pub brokers: Vec<BrokerConfigSummaryRow>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TopicConfigStateArgs {
    pub cluster: String,
    pub topic: String,
    pub broker_names: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct TopicConfigStateRow {
    pub broker_name: String,
    pub version: u64,
    pub read_queue_nums: u32,
    pub write_queue_nums: u32,
    pub order: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct TopicConfigStateOutput {
    pub cluster: String,
    pub topic: String,
    pub brokers: Vec<TopicConfigStateRow>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConsumerGroupConfigStateArgs {
    pub cluster: String,
    pub group: String,
    pub broker_names: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ConsumerGroupConfigStateRow {
    pub broker_name: String,
    pub version: u64,
    pub retry_max_times: u32,
    pub retry_queue_nums: u32,
    pub consume_timeout_minutes: u32,
    pub consume_enable: bool,
    pub consume_from_min_enable: bool,
    pub consume_broadcast_enable: bool,
    pub consume_message_orderly: bool,
    pub broker_id: u64,
    pub which_broker_when_consume_slowly: u64,
    pub notify_consumer_ids_changed_enable: bool,
    pub group_sys_flag: i32,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ConsumerGroupConfigStateOutput {
    pub cluster: String,
    pub group: String,
    pub brokers: Vec<ConsumerGroupConfigStateRow>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BrokerLogFilterStateArgs {
    pub cluster: String,
    pub broker_name: String,
    pub logger: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum BrokerLogLevel {
    Info,
    Debug,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct BrokerLogFilterStateRow {
    pub broker_name: String,
    pub broker_id: u64,
    pub state_schema_version: String,
    pub supported: bool,
    pub logger: String,
    pub level: Option<BrokerLogLevel>,
    pub active_operation_id: Option<String>,
    pub last_completed_operation_id: Option<String>,
    pub expires_at_millis: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct BrokerLogFilterStateOutput {
    pub cluster: String,
    pub broker_name: String,
    pub logger: String,
    pub brokers: Vec<BrokerLogFilterStateRow>,
}
