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
use std::collections::BTreeSet;

use rocketmq_model::common::topic::TopicValidator;
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

/// An atomic admin-owned Topic update sequence for canonical broker targets.
///
/// The batch owns the complete broker-local update and global `ORDER_TOPIC_CONFIG`
/// reconciliation sequence. Its fields are deliberately private so that callers cannot skip
/// validation or create a partial operation through deserialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicBatchUpsertRequest {
    topic: String,
    broker_names: Vec<String>,
    write_queue_nums: u32,
    read_queue_nums: u32,
    perm: u32,
    order: bool,
    message_type: Option<String>,
}

impl TopicBatchUpsertRequest {
    /// Creates a fully validated batch with a trimmed, deduplicated, sorted broker set.
    ///
    /// # Errors
    ///
    /// Returns an error when the Topic violates RocketMQ's topic-name rule, a queue count is
    /// outside `1..=128`, permissions are invalid, no broker is supplied, a broker name could
    /// corrupt an order configuration, or the message type is unsupported.
    pub fn try_new(
        topic: impl Into<String>,
        broker_names: Vec<String>,
        write_queue_nums: u32,
        read_queue_nums: u32,
        perm: u32,
        order: bool,
        message_type: Option<String>,
    ) -> AdminResult<Self> {
        let canonical = CanonicalTopicBatchUpsertRequest::try_new(
            topic,
            broker_names,
            write_queue_nums,
            read_queue_nums,
            perm,
            order,
            message_type,
        )?;
        Ok(Self {
            topic: canonical.topic,
            broker_names: canonical.broker_names,
            write_queue_nums: canonical.write_queue_nums,
            read_queue_nums: canonical.read_queue_nums,
            perm: canonical.perm,
            order: canonical.order,
            message_type: canonical.message_type,
        })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn broker_names(&self) -> &[String] {
        &self.broker_names
    }

    pub const fn write_queue_nums(&self) -> u32 {
        self.write_queue_nums
    }

    pub const fn read_queue_nums(&self) -> u32 {
        self.read_queue_nums
    }

    pub const fn perm(&self) -> u32 {
        self.perm
    }

    pub const fn order(&self) -> bool {
        self.order
    }

    pub fn message_type(&self) -> Option<&str> {
        self.message_type.as_deref()
    }

    pub(crate) fn canonical_for_execution(&self) -> AdminResult<CanonicalTopicBatchUpsertRequest> {
        CanonicalTopicBatchUpsertRequest::try_new(
            self.topic.clone(),
            self.broker_names.clone(),
            self.write_queue_nums,
            self.read_queue_nums,
            self.perm,
            self.order,
            self.message_type.clone(),
        )
    }

    #[cfg(test)]
    pub(crate) fn unchecked_for_execution_test(
        topic: String,
        broker_names: Vec<String>,
        write_queue_nums: u32,
        read_queue_nums: u32,
        perm: u32,
        order: bool,
        message_type: Option<String>,
    ) -> Self {
        Self {
            topic,
            broker_names,
            write_queue_nums,
            read_queue_nums,
            perm,
            order,
            message_type,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalTopicBatchUpsertRequest {
    pub(crate) topic: String,
    pub(crate) broker_names: Vec<String>,
    pub(crate) write_queue_nums: u32,
    pub(crate) read_queue_nums: u32,
    pub(crate) perm: u32,
    pub(crate) order: bool,
    pub(crate) message_type: Option<String>,
}

impl CanonicalTopicBatchUpsertRequest {
    fn try_new(
        topic: impl Into<String>,
        broker_names: Vec<String>,
        write_queue_nums: u32,
        read_queue_nums: u32,
        perm: u32,
        order: bool,
        message_type: Option<String>,
    ) -> AdminResult<Self> {
        let topic = required("topic", topic)?;
        let topic_validation = TopicValidator::validate_topic(&topic);
        if !topic_validation.valid() {
            return Err(crate::core::AdminError::invalid_argument(
                "topic",
                topic_validation.remark().to_string(),
            ));
        }
        for (field, value) in [("writeQueueNums", write_queue_nums), ("readQueueNums", read_queue_nums)] {
            if !(1..=128).contains(&value) {
                return Err(crate::core::AdminError::invalid_argument(
                    field,
                    "must be between 1 and 128",
                ));
            }
        }
        if !(1..=7).contains(&perm) || perm & 0b110 == 0 {
            return Err(crate::core::AdminError::invalid_argument(
                "perm",
                "must be between 1 and 7 and include read or write access",
            ));
        }
        let broker_names = broker_names
            .into_iter()
            .map(|broker_name| broker_name.trim().to_string())
            .map(|broker_name| {
                if broker_name.is_empty()
                    || broker_name.contains([':', ';'])
                    || broker_name.chars().any(char::is_whitespace)
                {
                    Err(crate::core::AdminError::invalid_argument(
                        "brokerNames",
                        "must not contain whitespace, ':' or ';'",
                    ))
                } else {
                    Ok(broker_name)
                }
            })
            .collect::<AdminResult<BTreeSet<_>>>()?
            .into_iter()
            .collect::<Vec<_>>();
        if broker_names.is_empty() {
            return Err(crate::core::AdminError::invalid_argument(
                "brokerNames",
                "must not be empty",
            ));
        }
        let message_type = message_type
            .map(|message_type| message_type.trim().to_string())
            .filter(|message_type| !message_type.is_empty());
        if let Some(message_type) = message_type.as_deref() {
            if !matches!(message_type, "NORMAL" | "FIFO" | "DELAY" | "TRANSACTION") {
                return Err(crate::core::AdminError::invalid_argument(
                    "messageType",
                    "must be NORMAL, FIFO, DELAY, or TRANSACTION",
                ));
            }
        }
        Ok(Self {
            topic,
            broker_names,
            write_queue_nums,
            read_queue_nums,
            perm,
            order,
            message_type,
        })
    }
}

/// One broker-local result from a batch Topic mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicBatchTargetOutcome {
    pub broker_name: String,
    pub success: bool,
    pub message: String,
}

/// The global order-configuration result from a batch Topic mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicBatchOrderConfigOutcome {
    pub success: bool,
    pub message: String,
}

/// Results for the complete batch-owned Topic mutation sequence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicBatchMutationOutcome {
    pub targets: Vec<TopicBatchTargetOutcome>,
    pub order_config: Option<TopicBatchOrderConfigOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteTopicAdminRequest {
    pub topic: String,
    pub cluster_name: Option<String>,
    pub broker_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteTopicsInBrokerRequest {
    pub broker_addr: String,
    pub topics: Vec<String>,
}

impl DeleteTopicsInBrokerRequest {
    pub fn try_new(broker_addr: impl Into<String>, topics: Vec<String>) -> AdminResult<Self> {
        let broker_addr = required("brokerAddr", broker_addr)?;
        if topics.is_empty() {
            return Err(crate::core::AdminError::invalid_argument("topics", "must not be empty"));
        }
        let topics = topics
            .into_iter()
            .map(|topic| required("topic", topic))
            .collect::<AdminResult<Vec<_>>>()?;
        Ok(Self { broker_addr, topics })
    }
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

    fn delete_topics_in_broker<'a>(
        &'a mut self,
        request: &'a DeleteTopicsInBrokerRequest,
    ) -> AdminFuture<'a, TopicMutationOutcome>;

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
    fn query_config_cas_state<'a>(
        &'a mut self,
        _request: &'a QueryTopicConfigCasRequest,
    ) -> AdminFuture<'a, TopicConfigCasState> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "query_topic_config_cas_state",
                "Topic config CAS state is not implemented by this adapter",
            ))
        })
    }
    fn get_topic_consumer_groups<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, TopicConsumerGroups>;
    fn get_topic_consumers<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, TopicConsumers>;
}

/// Topic mutations require the explicit mutation adapter feature.
pub trait TopicMutationAdmin: Send {
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
    fn delete_topics_in_broker<'a>(
        &'a mut self,
        request: &'a DeleteTopicsInBrokerRequest,
    ) -> AdminFuture<'a, TopicMutationOutcome>;
    fn reset_topic_consumer_offset<'a>(
        &'a mut self,
        request: &'a ResetTopicConsumerOffsetRequest,
    ) -> AdminFuture<'a, TopicMutationOutcome>;
    fn send_topic_test_message<'a>(&'a mut self, request: &'a TopicSendRequest) -> AdminFuture<'a, TopicSendResult>;
}

/// Admin-owned complete multi-broker Topic mutation workflow.
///
/// Implementations apply each canonical broker-local update, continue after individual failures,
/// and reconcile the NameServer-wide `ORDER_TOPIC_CONFIG` exactly once for successful brokers.
/// The workflow deletes that entry for unordered Topics and never exposes a safe half-operation.
pub trait TopicBatchMutationAdmin: Send {
    /// Executes the complete broker-local and global-order Topic mutation sequence.
    ///
    /// # Errors
    ///
    /// Returns an error before side effects when the request is invalid or the admin session is
    /// unavailable. Individual broker and order-configuration failures are represented in the
    /// returned batch outcome.
    fn upsert_topic_batch<'a>(
        &'a mut self,
        request: &'a TopicBatchUpsertRequest,
    ) -> AdminFuture<'a, TopicBatchMutationOutcome>;
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
    fn delete_topics_in_broker<'a>(
        &'a mut self,
        request: &'a DeleteTopicsInBrokerRequest,
    ) -> AdminFuture<'a, TopicMutationOutcome> {
        TopicAdmin::delete_topics_in_broker(self, request)
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
    use super::DeleteTopicAdminRequest;
    use super::DeleteTopicsInBrokerRequest;
    use super::GetTopicConfigRequest;
    use super::GetTopicRouteRequest;
    use super::ListTopicsRequest;
    use super::ListTopicsResult;
    use super::PatchTopicConfigRequest;
    use super::ResetTopicConsumerOffsetRequest;
    use super::TopicAdmin;
    use super::TopicBatchMutationAdmin;
    use super::TopicBatchMutationOutcome;
    use super::TopicBatchUpsertRequest;
    use super::TopicCatalog;
    use super::TopicCatalogRequest;
    use super::TopicConfigCasPatch;
    use super::TopicConfigDetail;
    use super::TopicConsumerGroups;
    use super::TopicConsumers;
    use super::TopicCurrentStats;
    use super::TopicMutationOutcome;
    use super::TopicRoute;
    use super::TopicSendRequest;
    use super::TopicSendResult;
    use super::TopicStats;
    use super::UpsertTopicRequest;
    use crate::core::AdminError;
    use crate::core::AdminFuture;

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

    #[test]
    fn batch_request_canonicalizes_brokers_and_rejects_order_conf_delimiters() {
        let request = TopicBatchUpsertRequest::try_new(
            " orders ",
            vec![" broker-b ".into(), "broker-a".into(), "broker-a".into()],
            8,
            8,
            6,
            true,
            Some(" NORMAL ".into()),
        )
        .expect("canonical batch request");

        assert_eq!(request.topic(), "orders");
        assert_eq!(request.broker_names(), ["broker-a", "broker-b"]);
        assert_eq!(request.message_type(), Some("NORMAL"));
        for invalid in ["orders topic", &"a".repeat(128)] {
            assert!(TopicBatchUpsertRequest::try_new(invalid, vec!["broker-a".into()], 8, 8, 6, true, None,).is_err());
        }
        for invalid in ["broker:a", "broker;a", "broker a"] {
            assert!(TopicBatchUpsertRequest::try_new("orders", vec![invalid.into()], 8, 8, 6, true, None,).is_err());
        }
        assert!(TopicBatchUpsertRequest::try_new("orders", vec!["broker-a".into()], 0, 8, 6, true, None,).is_err());
    }

    #[test]
    fn topic_admin_implementors_do_not_need_the_batch_extension() {
        fn uses_topic_admin<T: TopicAdmin>() {}
        fn uses_batch_extension<T: TopicBatchMutationAdmin>() {}

        uses_topic_admin::<ExistingTopicAdminFake>();
        let _ = uses_batch_extension::<BatchOnlyFake>;
    }

    struct ExistingTopicAdminFake;

    impl TopicAdmin for ExistingTopicAdminFake {
        fn list_topics<'a>(&'a mut self, _: &'a ListTopicsRequest) -> AdminFuture<'a, ListTopicsResult> {
            Box::pin(async { Err(AdminError::SessionClosed) })
        }

        fn get_topic_route<'a>(&'a mut self, _: &'a GetTopicRouteRequest) -> AdminFuture<'a, Option<TopicRoute>> {
            Box::pin(async { Err(AdminError::SessionClosed) })
        }

        fn get_topic_catalog<'a>(&'a mut self, _: &'a TopicCatalogRequest) -> AdminFuture<'a, TopicCatalog> {
            Box::pin(async { Err(AdminError::SessionClosed) })
        }

        fn get_topic_current_stats(&mut self) -> AdminFuture<'_, TopicCurrentStats> {
            Box::pin(async { Err(AdminError::SessionClosed) })
        }

        fn get_topic_stats<'a>(&'a mut self, _: &'a str) -> AdminFuture<'a, TopicStats> {
            Box::pin(async { Err(AdminError::SessionClosed) })
        }

        fn get_topic_config<'a>(&'a mut self, _: &'a GetTopicConfigRequest) -> AdminFuture<'a, TopicConfigDetail> {
            Box::pin(async { Err(AdminError::SessionClosed) })
        }

        fn upsert_topic<'a>(&'a mut self, _: &'a UpsertTopicRequest) -> AdminFuture<'a, TopicMutationOutcome> {
            Box::pin(async { Err(AdminError::SessionClosed) })
        }

        fn delete_topic<'a>(&'a mut self, _: &'a DeleteTopicAdminRequest) -> AdminFuture<'a, TopicMutationOutcome> {
            Box::pin(async { Err(AdminError::SessionClosed) })
        }

        fn delete_topics_in_broker<'a>(
            &'a mut self,
            _: &'a DeleteTopicsInBrokerRequest,
        ) -> AdminFuture<'a, TopicMutationOutcome> {
            Box::pin(async { Err(AdminError::SessionClosed) })
        }

        fn get_topic_consumer_groups<'a>(&'a mut self, _: &'a str) -> AdminFuture<'a, TopicConsumerGroups> {
            Box::pin(async { Err(AdminError::SessionClosed) })
        }

        fn get_topic_consumers<'a>(&'a mut self, _: &'a str) -> AdminFuture<'a, TopicConsumers> {
            Box::pin(async { Err(AdminError::SessionClosed) })
        }

        fn reset_topic_consumer_offset<'a>(
            &'a mut self,
            _: &'a ResetTopicConsumerOffsetRequest,
        ) -> AdminFuture<'a, TopicMutationOutcome> {
            Box::pin(async { Err(AdminError::SessionClosed) })
        }

        fn send_topic_test_message<'a>(&'a mut self, _: &'a TopicSendRequest) -> AdminFuture<'a, TopicSendResult> {
            Box::pin(async { Err(AdminError::SessionClosed) })
        }
    }

    struct BatchOnlyFake;

    impl TopicBatchMutationAdmin for BatchOnlyFake {
        fn upsert_topic_batch<'a>(
            &'a mut self,
            _: &'a TopicBatchUpsertRequest,
        ) -> AdminFuture<'a, TopicBatchMutationOutcome> {
            Box::pin(async { Err(AdminError::SessionClosed) })
        }
    }
}
