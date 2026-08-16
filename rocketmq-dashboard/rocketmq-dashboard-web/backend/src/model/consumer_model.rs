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
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerGroupInfo {
    pub group: String,
    pub consume_type: String,
    pub message_model: String,
    pub client_count: usize,
    pub diff_total: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerListView {
    pub items: Vec<ConsumerGroupInfo>,
    pub total: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerProgress {
    pub group: String,
    pub topic_count: usize,
    pub diff_total: i64,
    pub queues: Vec<ConsumerQueueProgress>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerQueueProgress {
    pub topic: String,
    pub broker_name: String,
    pub queue_id: i32,
    pub broker_offset: i64,
    pub consumer_offset: i64,
    pub diff: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerBrokerInfo {
    pub broker_name: String,
    pub broker_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerBrokerListView {
    pub items: Vec<ConsumerBrokerInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerResetOffsetRequest {
    pub topic: String,
    pub reset_timestamp: i64,
    pub force: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConsumerQueryMode {
    #[default]
    NameServer,
    Proxy,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ConsumerQuery {
    pub mode: ConsumerQueryMode,
    pub proxy_address: Option<String>,
    pub skip_system: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerQueryScope {
    pub mode: ConsumerQueryMode,
    pub address: Option<String>,
}

impl ConsumerQueryScope {
    #[must_use]
    pub fn address(&self) -> Option<&str> {
        self.address.as_deref()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerCapabilities {
    pub connections: bool,
    pub progress: bool,
    pub configuration: bool,
    pub running_info: bool,
    pub jstack: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerClientCapabilities {
    pub running_info: bool,
    pub jstack: bool,
    pub running_info_reason: Option<String>,
    pub jstack_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerGroupListItem {
    pub display_group_name: String,
    pub raw_group_name: String,
    pub category: String,
    pub connection_count: usize,
    pub consume_tps: i64,
    pub diff_total: i64,
    pub message_model: String,
    pub consume_type: String,
    pub version: Option<i32>,
    pub version_desc: String,
    pub broker_names: Vec<String>,
    pub broker_addresses: Vec<String>,
    pub update_timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerGroupListView {
    pub items: Vec<ConsumerGroupListItem>,
    pub total: usize,
    pub query_scope: ConsumerQueryScope,
    pub capabilities: ConsumerCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerSummaryView {
    pub group: String,
    pub display_group_name: String,
    pub category: String,
    pub connection_count: usize,
    pub consume_tps: i64,
    pub diff_total: i64,
    pub message_model: String,
    pub consume_type: String,
    pub version: Option<i32>,
    pub version_desc: String,
    pub broker_names: Vec<String>,
    pub broker_addresses: Vec<String>,
    pub update_timestamp: i64,
    pub query_scope: ConsumerQueryScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerConnectionItem {
    pub client_id: String,
    pub client_addr: String,
    pub language: String,
    pub version: i32,
    pub version_desc: String,
    pub capabilities: ConsumerClientCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerSubscriptionItem {
    pub topic: String,
    pub sub_string: String,
    pub expression_type: String,
    pub tags_set: Vec<String>,
    pub code_set: Vec<i32>,
    pub sub_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerConnectionView {
    pub group: String,
    pub connection_count: usize,
    pub consume_type: String,
    pub message_model: String,
    pub consume_from_where: String,
    pub connections: Vec<ConsumerConnectionItem>,
    pub subscriptions: Vec<ConsumerSubscriptionItem>,
    pub query_scope: ConsumerQueryScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerProgressTopicQueue {
    pub broker_name: String,
    pub queue_id: i32,
    pub broker_offset: i64,
    pub consumer_offset: i64,
    pub diff_total: i64,
    pub client_info: String,
    pub last_timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerProgressTopic {
    pub topic: String,
    pub diff_total: i64,
    pub last_timestamp: i64,
    pub queues: Vec<ConsumerProgressTopicQueue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerProgressView {
    pub group: String,
    pub topic_count: usize,
    pub total_diff: i64,
    pub topics: Vec<ConsumerProgressTopic>,
    pub query_scope: ConsumerQueryScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerConfigAttribute {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerConfigValue {
    pub consume_enable: bool,
    pub consume_from_min_enable: bool,
    pub consume_broadcast_enable: bool,
    pub consume_message_orderly: bool,
    pub retry_queue_nums: i32,
    pub retry_max_times: i32,
    pub broker_id: u64,
    pub which_broker_when_consume_slowly: u64,
    pub notify_consumer_ids_changed_enable: bool,
    pub group_sys_flag: i32,
    pub consume_timeout_minute: i32,
    pub group_retry_policy_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerConfigTarget {
    pub broker_name: String,
    pub broker_address: String,
    pub config: Option<ConsumerConfigValue>,
    pub subscription_topics: Vec<String>,
    pub attributes: Vec<ConsumerConfigAttribute>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerConfigView {
    pub group: String,
    pub effective: Option<ConsumerConfigValue>,
    pub inconsistent_fields: Vec<String>,
    pub targets: Vec<ConsumerConfigTarget>,
    pub query_scope: ConsumerQueryScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerProcessQueue {
    pub topic: String,
    pub broker_name: String,
    pub queue_id: i32,
    pub cached_message_count: i64,
    pub cached_message_size_in_mib: i64,
    pub commit_offset: i64,
    pub dropped: bool,
    pub last_consume_timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerRunningInfoView {
    pub consumer_group: String,
    pub client_id: String,
    pub properties: Vec<ConsumerConfigAttribute>,
    pub subscriptions: Vec<ConsumerSubscriptionItem>,
    pub process_queues: Vec<ConsumerProcessQueue>,
    pub jstack: Option<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerJStackView {
    pub consumer_group: String,
    pub client_id: String,
    pub jstack: Option<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerTargetResult {
    pub target: String,
    pub kind: String,
    pub success: bool,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerOperationResult {
    pub operation: String,
    pub consumer_group: String,
    pub success: bool,
    pub target_count: usize,
    pub message: String,
    pub targets: Vec<ConsumerTargetResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerUpsertView {
    pub consumer_group: Option<String>,
    pub cluster_name_list: Vec<String>,
    pub broker_name_list: Vec<String>,
    pub consume_enable: bool,
    pub consume_from_min_enable: bool,
    pub consume_broadcast_enable: bool,
    pub consume_message_orderly: bool,
    pub retry_queue_nums: i32,
    pub retry_max_times: i32,
    pub broker_id: u64,
    pub which_broker_when_consume_slowly: u64,
    pub notify_consumer_ids_changed_enable: bool,
    pub group_sys_flag: i32,
    pub consume_timeout_minute: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerDeleteView {
    pub broker_names: Vec<String>,
}
