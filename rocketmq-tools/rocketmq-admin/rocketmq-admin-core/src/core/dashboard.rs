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

//! Admin-owned contracts required by Dashboard control-plane adapters.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde::Serialize;

use crate::core::AdminFuture;

pub use crate::core::queue::QueueRef;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetSelector {
    pub cluster_name: Option<String>,
    pub broker_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminMutationResult {
    pub message: String,
    pub target_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardTopicInfo {
    pub topic: String,
    pub broker_name: Option<String>,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    pub perm: u32,
    pub category: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardTopicList {
    pub items: Vec<DashboardTopicInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardTopicRouteBroker {
    pub broker_name: String,
    pub broker_addrs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardTopicRouteQueue {
    pub broker_name: String,
    pub read_queue_nums: u32,
    pub write_queue_nums: u32,
    pub perm: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardTopicRoute {
    pub topic: String,
    pub brokers: Vec<DashboardTopicRouteBroker>,
    pub queues: Vec<DashboardTopicRouteQueue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardTopicStats {
    pub topic: String,
    pub queue_count: usize,
    pub total_min_offset: i64,
    pub total_max_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardTopicMutationRequest {
    pub topic: String,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    pub perm: u32,
    pub broker_name_list: Vec<String>,
    pub cluster_name_list: Vec<String>,
    pub order: bool,
    pub message_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DashboardBrokerInfo {
    pub cluster_name: String,
    pub broker_name: String,
    pub broker_id: u64,
    pub address: String,
    pub role: String,
    pub version: String,
    pub produce_tps: f64,
    pub consume_tps: f64,
    #[serde(default)]
    pub runtime_entries: BTreeMap<String, String>,
    #[serde(default)]
    pub runtime_error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DashboardBrokerList {
    #[serde(default)]
    pub clusters: Vec<String>,
    pub items: Vec<DashboardBrokerInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardBrokerRuntime {
    pub broker_name: String,
    pub address: String,
    pub entries: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardBrokerConfig {
    pub broker_name: String,
    pub address: String,
    pub entries: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardBrokerConfigUpdateRequest {
    pub broker_name: String,
    pub broker_addr: Option<String>,
    pub entries: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardBrokerTarget {
    pub broker_name: String,
    pub broker_addr: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerGroup {
    pub group: String,
    pub consume_type: String,
    pub message_model: String,
    pub client_count: usize,
    pub diff_total: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerList {
    pub items: Vec<DashboardConsumerGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerQueueProgress {
    pub topic: String,
    pub broker_name: String,
    pub queue_id: i32,
    pub broker_offset: i64,
    pub consumer_offset: i64,
    pub diff: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerProgress {
    pub group: String,
    pub topic_count: usize,
    pub diff_total: i64,
    pub queues: Vec<DashboardConsumerQueueProgress>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerResetRequest {
    pub group: String,
    pub topic: String,
    pub reset_timestamp: u64,
    pub force: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardProducerInfo {
    pub topic: String,
    pub producer_group: String,
    pub connection_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardProducerConnection {
    pub client_id: String,
    pub client_addr: String,
    pub language: String,
    pub version: i32,
    pub version_desc: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardProducerConnections {
    pub topic: String,
    pub producer_group: String,
    pub connections: Vec<DashboardProducerConnection>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardAclUser {
    pub broker_name: String,
    pub broker_addr: String,
    pub username: String,
    pub user_type: Option<String>,
    pub user_status: Option<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardAclUserMutationRequest {
    pub selector: TargetSelector,
    pub username: String,
    pub password: String,
    pub user_type: String,
    pub user_status: Option<String>,
}

impl std::fmt::Debug for DashboardAclUserMutationRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DashboardAclUserMutationRequest")
            .field("selector", &self.selector)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("user_type", &self.user_type)
            .field("user_status", &self.user_status)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardAclPolicyEntry {
    pub resource: Option<String>,
    pub actions: Vec<String>,
    pub source_ips: Vec<String>,
    pub decision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardAclPolicy {
    pub broker_name: String,
    pub broker_addr: String,
    pub subject: Option<String>,
    pub policy_type: Option<String>,
    pub entries: Vec<DashboardAclPolicyEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardAclPolicyMutationEntry {
    pub resources: Vec<String>,
    pub actions: Vec<String>,
    pub source_ips: Vec<String>,
    pub decision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardAclPolicyMutation {
    pub policy_type: String,
    pub entries: Vec<DashboardAclPolicyMutationEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardAclPolicyMutationRequest {
    pub selector: TargetSelector,
    pub subject: String,
    pub policies: Vec<DashboardAclPolicyMutation>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardAclQuery {
    pub selector: TargetSelector,
    pub search: String,
    pub resource: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardMessage {
    pub topic: String,
    pub message_id: String,
    pub keys: Option<String>,
    pub tags: Option<String>,
    pub born_timestamp: i64,
    pub store_timestamp: i64,
    pub born_host: String,
    pub store_host: String,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub store_size: i32,
    pub reconsume_times: i32,
    pub body_crc: u32,
    pub sys_flag: i32,
    pub flag: i32,
    pub prepared_transaction_offset: i64,
    pub body: Vec<u8>,
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardMessageList {
    pub items: Vec<DashboardMessage>,
    pub total: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardMessageQuery {
    pub topic: Option<String>,
    pub key: Option<String>,
    pub message_id: Option<String>,
    pub begin: Option<i64>,
    pub end: Option<i64>,
    pub page_num: Option<u32>,
    pub page_size: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardDlqMessageQuery {
    pub consumer_group: String,
    pub key: Option<String>,
    pub message_id: Option<String>,
    pub begin: Option<i64>,
    pub end: Option<i64>,
    pub page_num: Option<u32>,
    pub page_size: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardDirectConsumeRequest {
    pub message_id: String,
    pub topic: String,
    pub consumer_group: String,
    pub client_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardMessageTraceNode {
    pub node_type: String,
    pub name: String,
    pub status: String,
    pub timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardMessageTrace {
    pub message_id: String,
    pub trace_topic: String,
    pub nodes: Vec<DashboardMessageTraceNode>,
}

pub trait DashboardAdmin: Send {
    fn dashboard_list_topics(&mut self) -> AdminFuture<'_, DashboardTopicList>;
    fn dashboard_topic_route<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, DashboardTopicRoute>;
    fn dashboard_topic_stats<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, DashboardTopicStats>;
    fn dashboard_upsert_topic<'a>(
        &'a mut self,
        request: &'a DashboardTopicMutationRequest,
    ) -> AdminFuture<'a, AdminMutationResult>;
    fn dashboard_delete_topic<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, AdminMutationResult>;

    fn dashboard_list_consumers(&mut self) -> AdminFuture<'_, DashboardConsumerList>;
    fn dashboard_consumer_progress<'a>(&'a mut self, group: &'a str) -> AdminFuture<'a, DashboardConsumerProgress>;
    fn dashboard_reset_consumer<'a>(
        &'a mut self,
        request: &'a DashboardConsumerResetRequest,
    ) -> AdminFuture<'a, AdminMutationResult>;

    fn dashboard_list_producers(&mut self) -> AdminFuture<'_, Vec<DashboardProducerInfo>>;
    fn dashboard_producer_connections<'a>(
        &'a mut self,
        topic: &'a str,
        producer_group: &'a str,
    ) -> AdminFuture<'a, DashboardProducerConnections>;

    fn dashboard_list_brokers(&mut self) -> AdminFuture<'_, DashboardBrokerList>;
    fn dashboard_broker_runtime<'a>(
        &'a mut self,
        target: &'a DashboardBrokerTarget,
    ) -> AdminFuture<'a, DashboardBrokerRuntime>;
    fn dashboard_broker_config<'a>(
        &'a mut self,
        target: &'a DashboardBrokerTarget,
    ) -> AdminFuture<'a, DashboardBrokerConfig>;
    fn dashboard_update_broker_config<'a>(
        &'a mut self,
        request: &'a DashboardBrokerConfigUpdateRequest,
    ) -> AdminFuture<'a, AdminMutationResult>;

    fn dashboard_list_acl_users<'a>(
        &'a mut self,
        query: &'a DashboardAclQuery,
    ) -> AdminFuture<'a, Vec<DashboardAclUser>>;
    fn dashboard_create_acl_user<'a>(
        &'a mut self,
        request: &'a DashboardAclUserMutationRequest,
    ) -> AdminFuture<'a, AdminMutationResult>;
    fn dashboard_update_acl_user<'a>(
        &'a mut self,
        request: &'a DashboardAclUserMutationRequest,
    ) -> AdminFuture<'a, AdminMutationResult>;
    fn dashboard_delete_acl_user<'a>(
        &'a mut self,
        selector: &'a TargetSelector,
        username: &'a str,
    ) -> AdminFuture<'a, AdminMutationResult>;
    fn dashboard_list_acl_policies<'a>(
        &'a mut self,
        query: &'a DashboardAclQuery,
    ) -> AdminFuture<'a, Vec<DashboardAclPolicy>>;
    fn dashboard_create_acl_policy<'a>(
        &'a mut self,
        request: &'a DashboardAclPolicyMutationRequest,
    ) -> AdminFuture<'a, AdminMutationResult>;
    fn dashboard_update_acl_policy<'a>(
        &'a mut self,
        request: &'a DashboardAclPolicyMutationRequest,
    ) -> AdminFuture<'a, AdminMutationResult>;
    fn dashboard_delete_acl_policy<'a>(
        &'a mut self,
        selector: &'a TargetSelector,
        subject: &'a str,
        resource: &'a str,
    ) -> AdminFuture<'a, AdminMutationResult>;

    fn dashboard_query_messages<'a>(
        &'a mut self,
        request: &'a DashboardMessageQuery,
    ) -> AdminFuture<'a, DashboardMessageList>;
    fn dashboard_query_dlq_messages<'a>(
        &'a mut self,
        request: &'a DashboardDlqMessageQuery,
    ) -> AdminFuture<'a, DashboardMessageList>;
    fn dashboard_message_trace<'a>(
        &'a mut self,
        topic: &'a str,
        message_id: &'a str,
        trace_topic: &'a str,
    ) -> AdminFuture<'a, DashboardMessageTrace>;
    fn dashboard_consume_message_directly<'a>(
        &'a mut self,
        request: &'a DashboardDirectConsumeRequest,
    ) -> AdminFuture<'a, AdminMutationResult>;
}
