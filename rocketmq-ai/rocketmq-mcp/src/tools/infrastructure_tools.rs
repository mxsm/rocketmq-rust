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

//! Closed MCP contracts for infrastructure observations.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GetHaStatusArgs {
    pub cluster: String,
    #[serde(default)]
    pub broker_names: Vec<String>,
    #[serde(default)]
    pub include_sync_state: bool,
    #[serde(default)]
    pub controller_names: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LogicalBrokerInstance {
    pub broker_name: String,
    pub broker_id: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HaConnectionObservation {
    pub replica: LogicalBrokerInstance,
    pub slave_ack_offset: u64,
    pub diff: i64,
    pub in_sync: bool,
    pub transferred_bytes_per_second: u64,
    pub transfer_from_where: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BrokerHaObservation {
    pub broker_name: String,
    pub broker_id: u64,
    pub master_commit_log_max_offset: u64,
    #[schemars(range(max = 64))]
    pub in_sync_slave_count: u32,
    pub pending_group_transfer_request_count: u64,
    pub pending_group_transfer_oldest_wait_millis: u64,
    pub group_transfer_ack_notify_count: u64,
    pub connections: Vec<HaConnectionObservation>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BrokerSyncStateObservation {
    pub broker_name: String,
    pub master_broker_id: u64,
    pub master_epoch: i32,
    pub sync_state_set_epoch: i32,
    pub in_sync_replicas: Vec<LogicalBrokerInstance>,
    pub not_in_sync_replicas: Vec<LogicalBrokerInstance>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ControllerSyncStateObservation {
    pub controller_name: String,
    pub brokers: Vec<BrokerSyncStateObservation>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GetHaStatusOutput {
    pub cluster: String,
    pub brokers: Vec<BrokerHaObservation>,
    pub controller_sync_states: Vec<ControllerSyncStateObservation>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GetControllerMetadataArgs {
    pub cluster: String,
    #[serde(default)]
    pub controller_names: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ControllerMetadataObservation {
    pub controller_name: String,
    pub group: Option<String>,
    pub leader_id: Option<String>,
    pub is_leader: Option<bool>,
    #[schemars(range(max = 32))]
    pub peer_count: Option<usize>,
    pub last_log_index: Option<u64>,
    pub committed_log_index: Option<u64>,
    pub applied_log_index: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GetControllerMetadataOutput {
    pub cluster: String,
    pub controllers: Vec<ControllerMetadataObservation>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GetNameserverConfigSummaryArgs {
    pub cluster: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NameserverConfigValues {
    pub cluster_test: Option<bool>,
    pub order_message_enable: Option<bool>,
    pub return_order_topic_config_to_broker: Option<bool>,
    #[schemars(range(min = 1, max = 4096))]
    pub client_request_thread_pool_nums: Option<i32>,
    #[schemars(range(min = 1, max = 10_000_000))]
    pub client_request_thread_pool_queue_capacity: Option<i32>,
    #[schemars(range(min = 1, max = 3_600_000))]
    pub scan_not_active_broker_interval_ms: Option<u64>,
    #[schemars(range(min = 1, max = 10_000_000))]
    pub unregister_broker_queue_capacity: Option<i32>,
    pub support_acting_master: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NameserverConfigObservation {
    pub nameserver_name: String,
    pub values: NameserverConfigValues,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NameserverConfigDifferenceField {
    ClusterTest,
    OrderMessageEnable,
    ReturnOrderTopicConfigToBroker,
    ClientRequestThreadPoolNums,
    ClientRequestThreadPoolQueueCapacity,
    ScanNotActiveBrokerIntervalMs,
    UnregisterBrokerQueueCapacity,
    SupportActingMaster,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GetNameserverConfigSummaryOutput {
    pub cluster: String,
    pub nameservers: Vec<NameserverConfigObservation>,
    pub inconsistent_fields: Vec<NameserverConfigDifferenceField>,
}
