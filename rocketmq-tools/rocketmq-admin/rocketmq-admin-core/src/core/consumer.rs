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

//! Consumer capability contracts.

use serde::Deserialize;
use serde::Serialize;

use crate::core::error::required;
use crate::core::AdminFuture;
use crate::core::AdminResult;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListConsumerGroupsRequest;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConsumerGroupSummary {
    pub group: String,
    pub version: i32,
    pub client_count: i32,
    pub consume_type: String,
    pub message_model: String,
    pub consume_tps: f64,
    pub diff_total: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ListConsumerGroupsResult {
    pub groups: Vec<ConsumerGroupSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryConsumerLagRequest {
    pub topic: String,
    pub consumer_group: String,
    pub include_client_ip: bool,
}

impl QueryConsumerLagRequest {
    pub fn try_new(
        topic: impl Into<String>,
        consumer_group: impl Into<String>,
        include_client_ip: bool,
    ) -> AdminResult<Self> {
        Ok(Self {
            topic: required("topic", topic)?,
            consumer_group: required("consumerGroup", consumer_group)?,
            include_client_ip,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerLagRow {
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

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QueryConsumerLagResult {
    pub rows: Vec<ConsumerLagRow>,
    pub total_lag: i64,
    pub consume_tps: f64,
    pub inflight_total: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerGroupListRequest {
    pub skip_sys_group: bool,
    pub address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerGroupItem {
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerGroupListResult {
    pub items: Vec<DashboardConsumerGroupItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerConnectionRequest {
    pub consumer_group: String,
    pub address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerConnectionItem {
    pub client_id: String,
    pub client_addr: String,
    pub language: String,
    pub version: i32,
    pub version_desc: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerSubscriptionItem {
    pub topic: String,
    pub sub_string: String,
    pub expression_type: String,
    pub tags_set: Vec<String>,
    pub code_set: Vec<i32>,
    pub sub_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerConnection {
    pub consumer_group: String,
    pub connection_count: usize,
    pub consume_type: String,
    pub message_model: String,
    pub consume_from_where: String,
    pub connections: Vec<DashboardConsumerConnectionItem>,
    pub subscriptions: Vec<DashboardConsumerSubscriptionItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerProgressRequest {
    pub consumer_group: String,
    pub address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerTopicQueue {
    pub broker_name: String,
    pub queue_id: i32,
    pub broker_offset: i64,
    pub consumer_offset: i64,
    pub diff_total: i64,
    pub client_info: String,
    pub last_timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerTopicDetail {
    pub topic: String,
    pub diff_total: i64,
    pub last_timestamp: i64,
    pub queues: Vec<DashboardConsumerTopicQueue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerProgress {
    pub consumer_group: String,
    pub topic_count: usize,
    pub total_diff: i64,
    pub topics: Vec<DashboardConsumerTopicDetail>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerConfigRequest {
    pub consumer_group: String,
    pub address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerConfigAttribute {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerConfig {
    pub consumer_group: String,
    pub broker_name: String,
    pub broker_address: String,
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
    pub subscription_topics: Vec<String>,
    pub attributes: Vec<DashboardConsumerConfigAttribute>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerUpsertRequest {
    pub cluster_name_list: Vec<String>,
    pub broker_name_list: Vec<String>,
    pub consumer_group: String,
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
pub struct DashboardConsumerDeleteRequest {
    pub consumer_group: String,
    pub broker_name_list: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerMutationResult {
    pub consumer_group: String,
    pub broker_names: Vec<String>,
    pub updated: bool,
}

pub trait ConsumerAdmin: Send {
    fn list_consumer_groups<'a>(
        &'a mut self,
        request: &'a ListConsumerGroupsRequest,
    ) -> AdminFuture<'a, ListConsumerGroupsResult>;

    fn query_consumer_lag<'a>(
        &'a mut self,
        request: &'a QueryConsumerLagRequest,
    ) -> AdminFuture<'a, QueryConsumerLagResult>;

    fn query_dashboard_consumer_groups<'a>(
        &'a mut self,
        request: &'a DashboardConsumerGroupListRequest,
    ) -> AdminFuture<'a, DashboardConsumerGroupListResult>;

    fn query_dashboard_consumer_connection<'a>(
        &'a mut self,
        request: &'a DashboardConsumerConnectionRequest,
    ) -> AdminFuture<'a, DashboardConsumerConnection>;

    fn query_dashboard_consumer_progress<'a>(
        &'a mut self,
        request: &'a DashboardConsumerProgressRequest,
    ) -> AdminFuture<'a, DashboardConsumerProgress>;

    fn query_dashboard_consumer_config<'a>(
        &'a mut self,
        request: &'a DashboardConsumerConfigRequest,
    ) -> AdminFuture<'a, DashboardConsumerConfig>;

    fn upsert_dashboard_consumer_group<'a>(
        &'a mut self,
        request: &'a DashboardConsumerUpsertRequest,
    ) -> AdminFuture<'a, DashboardConsumerMutationResult>;

    fn delete_dashboard_consumer_group<'a>(
        &'a mut self,
        request: &'a DashboardConsumerDeleteRequest,
    ) -> AdminFuture<'a, DashboardConsumerMutationResult>;
}

#[cfg(feature = "legacy-common-compat")]
pub use crate::client_adapter::legacy::core::consumer::*;
