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

//! Topic capability contracts.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde::Serialize;

use crate::core::error::required;
use crate::core::AdminFuture;
use crate::core::AdminResult;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListTopicsRequest {
    pub cluster: Option<String>,
}

impl ListTopicsRequest {
    pub fn new(cluster: Option<String>) -> Self {
        Self {
            cluster: cluster.and_then(|value| {
                let value = value.trim().to_string();
                (!value.is_empty()).then_some(value)
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicSummary {
    pub topic: String,
    pub cluster: Option<String>,
    pub consumer_group: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListTopicsResult {
    pub topics: Vec<TopicSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetTopicRouteRequest {
    pub topic: String,
}

impl GetTopicRouteRequest {
    pub fn try_new(topic: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            topic: required("topic", topic)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicBroker {
    pub cluster: String,
    pub broker_name: String,
    pub broker_addrs: BTreeMap<u64, String>,
    pub zone_name: Option<String>,
    pub enable_acting_master: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicQueue {
    pub broker_name: String,
    pub read_queue_nums: u32,
    pub write_queue_nums: u32,
    pub perm: u32,
    pub topic_sys_flag: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicRoute {
    pub brokers: Vec<TopicBroker>,
    pub queues: Vec<TopicQueue>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicCatalogRequest {
    pub skip_system_topics: bool,
    pub skip_retry_and_dlq_topics: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicTargetOption {
    pub cluster_name: String,
    pub broker_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicCatalogItem {
    pub topic: String,
    pub category: String,
    pub message_type: String,
    pub clusters: Vec<String>,
    pub brokers: Vec<String>,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    pub perm: i32,
    pub order: bool,
    pub system_topic: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicCatalog {
    pub items: Vec<TopicCatalogItem>,
    pub targets: Vec<TopicTargetOption>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopicCurrentStatsItem {
    pub topic: String,
    pub total_msg: u64,
    pub produced_msg_count_24h: u64,
    pub consumed_msg_count_24h: u64,
    pub in_tps: f64,
    pub out_tps: f64,
    pub consumer_group_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicCurrentStatsFailure {
    pub topic: String,
    pub error: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TopicCurrentStats {
    pub items: Vec<TopicCurrentStatsItem>,
    pub failures: Vec<TopicCurrentStatsFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicQueueOffset {
    pub broker_name: String,
    pub queue_id: i32,
    pub min_offset: i64,
    pub max_offset: i64,
    pub last_update_timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicStats {
    pub topic: String,
    pub total_message_count: i64,
    pub queue_count: usize,
    pub offsets: Vec<TopicQueueOffset>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetTopicConfigRequest {
    pub topic: String,
    pub broker_name: Option<String>,
}

impl GetTopicConfigRequest {
    pub fn try_new(topic: impl Into<String>, broker_name: Option<String>) -> AdminResult<Self> {
        Ok(Self {
            topic: required("topic", topic)?,
            broker_name: broker_name.and_then(|value| {
                let value = value.trim().to_string();
                (!value.is_empty()).then_some(value)
            }),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicConfigDetail {
    pub topic_name: String,
    pub broker_name: String,
    pub cluster_name: Option<String>,
    pub broker_name_list: Vec<String>,
    pub cluster_name_list: Vec<String>,
    pub read_queue_nums: i32,
    pub write_queue_nums: i32,
    pub perm: i32,
    pub order: bool,
    pub message_type: String,
    pub attributes: BTreeMap<String, String>,
    pub inconsistent_fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpsertTopicRequest {
    pub cluster_names: Vec<String>,
    pub broker_names: Vec<String>,
    pub topic: String,
    pub write_queue_nums: u32,
    pub read_queue_nums: u32,
    pub perm: u32,
    pub order: bool,
    pub message_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteTopicAdminRequest {
    pub topic: String,
    pub cluster_name: Option<String>,
    pub broker_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicMutationOutcome {
    pub message: String,
    pub target_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryTopicConfigCasRequest {
    pub broker_addr: String,
    pub topic: String,
}

impl QueryTopicConfigCasRequest {
    pub fn try_new(broker_addr: impl Into<String>, topic: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            broker_addr: required("broker_addr", broker_addr)?,
            topic: required("topic", topic)?,
        })
    }
}

/// Closed Topic state returned for a supervised version-CAS precheck.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicConfigCasState {
    pub version: u64,
    pub read_queue_nums: u32,
    pub write_queue_nums: u32,
    pub order: bool,
}

/// Closed Topic fields supported by supervised execution.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicConfigCasPatch {
    pub read_queue_nums: Option<u32>,
    pub write_queue_nums: Option<u32>,
    pub order: Option<bool>,
}

impl TopicConfigCasPatch {
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.read_queue_nums.is_none() && self.write_queue_nums.is_none() && self.order.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchTopicConfigRequest {
    pub broker_addr: String,
    pub topic: String,
    pub expected_version: u64,
    pub patch: TopicConfigCasPatch,
}

impl PatchTopicConfigRequest {
    pub fn try_new(
        broker_addr: impl Into<String>,
        topic: impl Into<String>,
        expected_version: u64,
        patch: TopicConfigCasPatch,
    ) -> AdminResult<Self> {
        if patch.is_empty() {
            return Err(crate::core::AdminError::invalid_argument("patch", "must not be empty"));
        }
        for (field, value) in [
            ("read_queue_nums", patch.read_queue_nums),
            ("write_queue_nums", patch.write_queue_nums),
        ] {
            if value.is_some_and(|value| !(1..=128).contains(&value)) {
                return Err(crate::core::AdminError::invalid_argument(
                    field,
                    "must be between 1 and 128",
                ));
            }
        }
        Ok(Self {
            broker_addr: required("broker_addr", broker_addr)?,
            topic: required("topic", topic)?,
            expected_version,
            patch,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PatchTopicConfigOutcome {
    Applied { previous_version: u64, version: u64 },
    VersionConflict { expected_version: u64, actual_version: u64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopicConsumerInfo {
    pub consumer_group: String,
    pub total_diff: i64,
    pub inflight_diff: i64,
    pub consume_tps: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicConsumerGroups {
    pub groups: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TopicConsumers {
    pub items: Vec<TopicConsumerInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetTopicConsumerOffsetRequest {
    pub consumer_group: String,
    pub topic: String,
    pub reset_timestamp: u64,
    pub force: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicSendRequest {
    pub topic: String,
    pub key: String,
    pub tag: String,
    pub message_body: String,
    pub trace_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicSendResult {
    pub topic: String,
    pub send_status: String,
    pub message_id: Option<String>,
    pub broker_name: Option<String>,
    pub queue_id: Option<i32>,
    pub queue_offset: u64,
    pub transaction_id: Option<String>,
    pub region_id: Option<String>,
    pub local_transaction_state: Option<String>,
}

pub trait TopicAdmin: Send {
    fn list_topics<'a>(&'a mut self, request: &'a ListTopicsRequest) -> AdminFuture<'a, ListTopicsResult>;

    fn get_topic_route<'a>(&'a mut self, request: &'a GetTopicRouteRequest) -> AdminFuture<'a, Option<TopicRoute>>;

    fn get_topic_catalog<'a>(&'a mut self, request: &'a TopicCatalogRequest) -> AdminFuture<'a, TopicCatalog>;

    fn get_topic_current_stats(&mut self) -> AdminFuture<'_, TopicCurrentStats>;

    fn get_topic_stats<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, TopicStats>;

    fn get_topic_config<'a>(&'a mut self, request: &'a GetTopicConfigRequest) -> AdminFuture<'a, TopicConfigDetail>;

    fn upsert_topic<'a>(&'a mut self, request: &'a UpsertTopicRequest) -> AdminFuture<'a, TopicMutationOutcome>;

    fn delete_topic<'a>(&'a mut self, request: &'a DeleteTopicAdminRequest) -> AdminFuture<'a, TopicMutationOutcome>;

    fn get_topic_consumer_groups<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, TopicConsumerGroups>;

    fn get_topic_consumers<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, TopicConsumers>;

    fn reset_topic_consumer_offset<'a>(
        &'a mut self,
        request: &'a ResetTopicConsumerOffsetRequest,
    ) -> AdminFuture<'a, TopicMutationOutcome>;

    fn send_topic_test_message<'a>(&'a mut self, request: &'a TopicSendRequest) -> AdminFuture<'a, TopicSendResult>;
}

/// Topic queries that are safe for read-only SRE integrations.
pub trait TopicQueryAdmin: Send {
    fn list_topics<'a>(&'a mut self, request: &'a ListTopicsRequest) -> AdminFuture<'a, ListTopicsResult>;
    fn get_topic_route<'a>(&'a mut self, request: &'a GetTopicRouteRequest) -> AdminFuture<'a, Option<TopicRoute>>;
    fn get_topic_catalog<'a>(&'a mut self, request: &'a TopicCatalogRequest) -> AdminFuture<'a, TopicCatalog>;
    fn get_topic_current_stats(&mut self) -> AdminFuture<'_, TopicCurrentStats>;
    fn get_topic_stats<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, TopicStats>;
    fn get_topic_config<'a>(&'a mut self, request: &'a GetTopicConfigRequest) -> AdminFuture<'a, TopicConfigDetail>;
    fn get_topic_consumer_groups<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, TopicConsumerGroups>;
    fn get_topic_consumers<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, TopicConsumers>;
}

/// Topic mutations require the explicit mutation adapter feature.
pub trait TopicMutationAdmin: Send {
    fn query_config_cas_state<'a>(
        &'a mut self,
        _request: &'a QueryTopicConfigCasRequest,
    ) -> AdminFuture<'a, TopicConfigCasState> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "query_topic_config_cas_state",
                "Topic config CAS is not implemented by this adapter",
            ))
        })
    }

    fn patch_config_if_version<'a>(
        &'a mut self,
        _request: &'a PatchTopicConfigRequest,
    ) -> AdminFuture<'a, PatchTopicConfigOutcome> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "patch_topic_config_if_version",
                "Topic config CAS is not implemented by this adapter",
            ))
        })
    }

    fn upsert_topic<'a>(&'a mut self, request: &'a UpsertTopicRequest) -> AdminFuture<'a, TopicMutationOutcome>;
    fn delete_topic<'a>(&'a mut self, request: &'a DeleteTopicAdminRequest) -> AdminFuture<'a, TopicMutationOutcome>;
    fn reset_topic_consumer_offset<'a>(
        &'a mut self,
        request: &'a ResetTopicConsumerOffsetRequest,
    ) -> AdminFuture<'a, TopicMutationOutcome>;
    fn send_topic_test_message<'a>(&'a mut self, request: &'a TopicSendRequest) -> AdminFuture<'a, TopicSendResult>;
}

impl<T: TopicAdmin + ?Sized> TopicQueryAdmin for T {
    fn list_topics<'a>(&'a mut self, request: &'a ListTopicsRequest) -> AdminFuture<'a, ListTopicsResult> {
        TopicAdmin::list_topics(self, request)
    }
    fn get_topic_route<'a>(&'a mut self, request: &'a GetTopicRouteRequest) -> AdminFuture<'a, Option<TopicRoute>> {
        TopicAdmin::get_topic_route(self, request)
    }
    fn get_topic_catalog<'a>(&'a mut self, request: &'a TopicCatalogRequest) -> AdminFuture<'a, TopicCatalog> {
        TopicAdmin::get_topic_catalog(self, request)
    }
    fn get_topic_current_stats(&mut self) -> AdminFuture<'_, TopicCurrentStats> {
        TopicAdmin::get_topic_current_stats(self)
    }
    fn get_topic_stats<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, TopicStats> {
        TopicAdmin::get_topic_stats(self, topic)
    }
    fn get_topic_config<'a>(&'a mut self, request: &'a GetTopicConfigRequest) -> AdminFuture<'a, TopicConfigDetail> {
        TopicAdmin::get_topic_config(self, request)
    }
    fn get_topic_consumer_groups<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, TopicConsumerGroups> {
        TopicAdmin::get_topic_consumer_groups(self, topic)
    }
    fn get_topic_consumers<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, TopicConsumers> {
        TopicAdmin::get_topic_consumers(self, topic)
    }
}

impl<T: TopicAdmin + ?Sized> TopicMutationAdmin for T {
    fn upsert_topic<'a>(&'a mut self, request: &'a UpsertTopicRequest) -> AdminFuture<'a, TopicMutationOutcome> {
        TopicAdmin::upsert_topic(self, request)
    }
    fn delete_topic<'a>(&'a mut self, request: &'a DeleteTopicAdminRequest) -> AdminFuture<'a, TopicMutationOutcome> {
        TopicAdmin::delete_topic(self, request)
    }
    fn reset_topic_consumer_offset<'a>(
        &'a mut self,
        request: &'a ResetTopicConsumerOffsetRequest,
    ) -> AdminFuture<'a, TopicMutationOutcome> {
        TopicAdmin::reset_topic_consumer_offset(self, request)
    }
    fn send_topic_test_message<'a>(&'a mut self, request: &'a TopicSendRequest) -> AdminFuture<'a, TopicSendResult> {
        TopicAdmin::send_topic_test_message(self, request)
    }
}

#[cfg(test)]
mod tests {
    use super::PatchTopicConfigRequest;
    use super::TopicConfigCasPatch;

    #[test]
    fn topic_config_cas_request_accepts_only_a_non_empty_bounded_patch() {
        let valid = PatchTopicConfigRequest::try_new(
            "127.0.0.1:10911",
            "orders",
            7,
            TopicConfigCasPatch {
                read_queue_nums: Some(8),
                write_queue_nums: None,
                order: Some(true),
            },
        )
        .expect("bounded patch");
        assert_eq!(valid.expected_version, 7);

        assert!(
            PatchTopicConfigRequest::try_new("127.0.0.1:10911", "orders", 7, TopicConfigCasPatch::default(),).is_err()
        );
        assert!(PatchTopicConfigRequest::try_new(
            "127.0.0.1:10911",
            "orders",
            7,
            TopicConfigCasPatch {
                read_queue_nums: Some(129),
                ..Default::default()
            },
        )
        .is_err());
    }
}
