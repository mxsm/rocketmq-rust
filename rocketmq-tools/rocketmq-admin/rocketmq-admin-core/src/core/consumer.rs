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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DashboardConsumerRunningInfoRequest {
    consumer_group: String,
    client_id: String,
    include_jstack: bool,
    max_output_bytes: usize,
}

impl<'de> Deserialize<'de> for DashboardConsumerRunningInfoRequest {
    fn deserialize<Deserializer>(deserializer: Deserializer) -> Result<Self, Deserializer::Error>
    where
        Deserializer: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct SerializedRequest {
            consumer_group: String,
            client_id: String,
            include_jstack: bool,
            max_output_bytes: usize,
        }

        let request = SerializedRequest::deserialize(deserializer)?;
        Self::try_new(
            request.consumer_group,
            request.client_id,
            request.include_jstack,
            request.max_output_bytes,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl DashboardConsumerRunningInfoRequest {
    const MAX_OUTPUT_BYTES: usize = 1024 * 1024;

    /// Creates a bounded consumer diagnostic request.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer group or client ID is blank, or
    /// when `max_output_bytes` is outside the inclusive range 1 byte to 1 MiB.
    pub fn try_new(
        consumer_group: impl Into<String>,
        client_id: impl Into<String>,
        include_jstack: bool,
        max_output_bytes: usize,
    ) -> AdminResult<Self> {
        if !(1..=Self::MAX_OUTPUT_BYTES).contains(&max_output_bytes) {
            return Err(crate::core::AdminError::invalid_argument(
                "maxOutputBytes",
                "must be between 1 and 1048576 bytes",
            ));
        }
        Ok(Self {
            consumer_group: required("consumerGroup", consumer_group)?,
            client_id: required("clientId", client_id)?,
            include_jstack,
            max_output_bytes,
        })
    }

    #[must_use]
    pub fn consumer_group(&self) -> &str {
        &self.consumer_group
    }

    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    #[must_use]
    pub const fn include_jstack(&self) -> bool {
        self.include_jstack
    }

    #[must_use]
    /// Returns the aggregate byte budget for sorted property keys/values,
    /// followed by JStack when requested.
    pub const fn max_output_bytes(&self) -> usize {
        self.max_output_bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerProcessQueue {
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
pub struct DashboardConsumerRunningInfo {
    pub consumer_group: String,
    pub client_id: String,
    pub properties: Vec<DashboardConsumerConfigAttribute>,
    pub subscriptions: Vec<DashboardConsumerSubscriptionItem>,
    pub process_queues: Vec<DashboardConsumerProcessQueue>,
    pub jstack: Option<String>,
    /// True when requested property or JStack text was shortened at a UTF-8
    /// character boundary, omitted by the budget, or unavailable.
    pub truncated: bool,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteSubscriptionGroupsRequest {
    pub broker_addr: String,
    pub group_names: Vec<String>,
    pub clean_offset: bool,
}

impl DeleteSubscriptionGroupsRequest {
    pub fn try_new(broker_addr: impl Into<String>, group_names: Vec<String>, clean_offset: bool) -> AdminResult<Self> {
        let broker_addr = required("brokerAddr", broker_addr)?;
        if group_names.is_empty() {
            return Err(crate::core::AdminError::invalid_argument(
                "groupNames",
                "must not be empty",
            ));
        }
        let group_names = group_names
            .into_iter()
            .map(|group_name| required("groupName", group_name))
            .collect::<AdminResult<Vec<_>>>()?;
        Ok(Self {
            broker_addr,
            group_names,
            clean_offset,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerBatchMutationOutcome {
    pub message: String,
    pub broker_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsumerRequestMode {
    Pull,
    Pop,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetConsumerRequestModeRequest {
    pub topic: String,
    pub consumer_group: String,
    pub mode: ConsumerRequestMode,
    pub pop_share_queue_num: i32,
    pub timeout_millis: u64,
}

impl SetConsumerRequestModeRequest {
    pub fn try_new(
        topic: impl Into<String>,
        consumer_group: impl Into<String>,
        mode: ConsumerRequestMode,
        pop_share_queue_num: i32,
        timeout_millis: u64,
    ) -> AdminResult<Self> {
        if pop_share_queue_num < 0 {
            return Err(crate::core::AdminError::invalid_argument(
                "popShareQueueNum",
                "must be greater than or equal to zero",
            ));
        }
        if timeout_millis == 0 {
            return Err(crate::core::AdminError::invalid_argument(
                "timeoutMillis",
                "must be greater than zero",
            ));
        }
        Ok(Self {
            topic: crate::core::error::required("topic", topic)?,
            consumer_group: crate::core::error::required("consumerGroup", consumer_group)?,
            mode,
            pop_share_queue_num,
            timeout_millis,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetConsumerRequestModeResult {
    pub broker_addrs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuerySubscriptionGroupConfigCasRequest {
    pub broker_addr: String,
    pub group: String,
}

impl QuerySubscriptionGroupConfigCasRequest {
    pub fn try_new(broker_addr: impl Into<String>, group: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            broker_addr: required("broker_addr", broker_addr)?,
            group: required("group", group)?,
        })
    }
}

/// Closed Subscription Group state returned for supervised version-CAS
/// prechecks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionGroupConfigCasState {
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

/// Closed Subscription Group fields supported by supervised execution.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionGroupConfigCasPatch {
    pub retry_max_times: Option<u32>,
    pub retry_queue_nums: Option<u32>,
    pub consume_timeout_minutes: Option<u32>,
}

impl SubscriptionGroupConfigCasPatch {
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.retry_max_times.is_none() && self.retry_queue_nums.is_none() && self.consume_timeout_minutes.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchSubscriptionGroupConfigRequest {
    pub broker_addr: String,
    pub group: String,
    pub expected_version: u64,
    pub patch: SubscriptionGroupConfigCasPatch,
}

impl PatchSubscriptionGroupConfigRequest {
    pub fn try_new(
        broker_addr: impl Into<String>,
        group: impl Into<String>,
        expected_version: u64,
        patch: SubscriptionGroupConfigCasPatch,
    ) -> AdminResult<Self> {
        if patch.is_empty() {
            return Err(crate::core::AdminError::invalid_argument("patch", "must not be empty"));
        }
        for (field, value, maximum) in [
            ("retry_max_times", patch.retry_max_times, 16),
            ("retry_queue_nums", patch.retry_queue_nums, 8),
            ("consume_timeout_minutes", patch.consume_timeout_minutes, 1_440),
        ] {
            if value.is_some_and(|value| !(1..=maximum).contains(&value)) {
                return Err(crate::core::AdminError::invalid_argument(
                    field,
                    format!("must be between 1 and {maximum}"),
                ));
            }
        }
        Ok(Self {
            broker_addr: required("broker_addr", broker_addr)?,
            group: required("group", group)?,
            expected_version,
            patch,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PatchSubscriptionGroupConfigOutcome {
    Applied { previous_version: u64, version: u64 },
    VersionConflict { expected_version: u64, actual_version: u64 },
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

    fn delete_subscription_groups<'a>(
        &'a mut self,
        request: &'a DeleteSubscriptionGroupsRequest,
    ) -> AdminFuture<'a, ConsumerBatchMutationOutcome>;

    fn set_consumer_request_mode<'a>(
        &'a mut self,
        request: &'a SetConsumerRequestModeRequest,
    ) -> AdminFuture<'a, SetConsumerRequestModeResult>;
}

/// Consumer queries available to read-only integrations.
pub trait ConsumerQueryAdmin: Send {
    fn query_config_cas_state<'a>(
        &'a mut self,
        _request: &'a QuerySubscriptionGroupConfigCasRequest,
    ) -> AdminFuture<'a, SubscriptionGroupConfigCasState> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "query_subscription_group_config_cas_state",
                "Subscription Group config CAS state is not implemented by this adapter",
            ))
        })
    }

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
}

/// Bounded diagnostic queries exposed independently from existing consumer
/// query and mutation capabilities.
pub trait ConsumerDiagnosticAdmin: Send {
    fn query_dashboard_consumer_running_info<'a>(
        &'a mut self,
        request: &'a DashboardConsumerRunningInfoRequest,
    ) -> AdminFuture<'a, DashboardConsumerRunningInfo>;
}

/// Consumer mutations require the explicit mutation adapter feature.
pub trait ConsumerMutationAdmin: Send {
    fn patch_config_if_version<'a>(
        &'a mut self,
        _request: &'a PatchSubscriptionGroupConfigRequest,
    ) -> AdminFuture<'a, PatchSubscriptionGroupConfigOutcome> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "patch_subscription_group_config_if_version",
                "Subscription Group config CAS is not implemented by this adapter",
            ))
        })
    }

    fn upsert_dashboard_consumer_group<'a>(
        &'a mut self,
        request: &'a DashboardConsumerUpsertRequest,
    ) -> AdminFuture<'a, DashboardConsumerMutationResult>;
    fn delete_dashboard_consumer_group<'a>(
        &'a mut self,
        request: &'a DashboardConsumerDeleteRequest,
    ) -> AdminFuture<'a, DashboardConsumerMutationResult>;
    fn delete_subscription_groups<'a>(
        &'a mut self,
        request: &'a DeleteSubscriptionGroupsRequest,
    ) -> AdminFuture<'a, ConsumerBatchMutationOutcome>;
    fn set_consumer_request_mode<'a>(
        &'a mut self,
        request: &'a SetConsumerRequestModeRequest,
    ) -> AdminFuture<'a, SetConsumerRequestModeResult>;
}

impl<T: ConsumerAdmin + ?Sized> ConsumerQueryAdmin for T {
    fn list_consumer_groups<'a>(
        &'a mut self,
        request: &'a ListConsumerGroupsRequest,
    ) -> AdminFuture<'a, ListConsumerGroupsResult> {
        ConsumerAdmin::list_consumer_groups(self, request)
    }
    fn query_consumer_lag<'a>(
        &'a mut self,
        request: &'a QueryConsumerLagRequest,
    ) -> AdminFuture<'a, QueryConsumerLagResult> {
        ConsumerAdmin::query_consumer_lag(self, request)
    }
    fn query_dashboard_consumer_groups<'a>(
        &'a mut self,
        request: &'a DashboardConsumerGroupListRequest,
    ) -> AdminFuture<'a, DashboardConsumerGroupListResult> {
        ConsumerAdmin::query_dashboard_consumer_groups(self, request)
    }
    fn query_dashboard_consumer_connection<'a>(
        &'a mut self,
        request: &'a DashboardConsumerConnectionRequest,
    ) -> AdminFuture<'a, DashboardConsumerConnection> {
        ConsumerAdmin::query_dashboard_consumer_connection(self, request)
    }
    fn query_dashboard_consumer_progress<'a>(
        &'a mut self,
        request: &'a DashboardConsumerProgressRequest,
    ) -> AdminFuture<'a, DashboardConsumerProgress> {
        ConsumerAdmin::query_dashboard_consumer_progress(self, request)
    }
    fn query_dashboard_consumer_config<'a>(
        &'a mut self,
        request: &'a DashboardConsumerConfigRequest,
    ) -> AdminFuture<'a, DashboardConsumerConfig> {
        ConsumerAdmin::query_dashboard_consumer_config(self, request)
    }
}

impl<T: ConsumerAdmin + ?Sized> ConsumerMutationAdmin for T {
    fn upsert_dashboard_consumer_group<'a>(
        &'a mut self,
        request: &'a DashboardConsumerUpsertRequest,
    ) -> AdminFuture<'a, DashboardConsumerMutationResult> {
        ConsumerAdmin::upsert_dashboard_consumer_group(self, request)
    }
    fn delete_dashboard_consumer_group<'a>(
        &'a mut self,
        request: &'a DashboardConsumerDeleteRequest,
    ) -> AdminFuture<'a, DashboardConsumerMutationResult> {
        ConsumerAdmin::delete_dashboard_consumer_group(self, request)
    }
    fn delete_subscription_groups<'a>(
        &'a mut self,
        request: &'a DeleteSubscriptionGroupsRequest,
    ) -> AdminFuture<'a, ConsumerBatchMutationOutcome> {
        ConsumerAdmin::delete_subscription_groups(self, request)
    }
    fn set_consumer_request_mode<'a>(
        &'a mut self,
        request: &'a SetConsumerRequestModeRequest,
    ) -> AdminFuture<'a, SetConsumerRequestModeResult> {
        ConsumerAdmin::set_consumer_request_mode(self, request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::AdminFuture;

    struct ExistingConsumerAdmin;

    macro_rules! existing_consumer_admin {
        ($($name:ident: $request:ty => $result:ty;)*) => {
            impl ConsumerAdmin for ExistingConsumerAdmin {
                $(
                    fn $name<'a>(&'a mut self, _request: &'a $request) -> AdminFuture<'a, $result> {
                        unused_admin_future()
                    }
                )*
            }
        };
    }

    existing_consumer_admin! {
        list_consumer_groups: ListConsumerGroupsRequest => ListConsumerGroupsResult;
        query_consumer_lag: QueryConsumerLagRequest => QueryConsumerLagResult;
        query_dashboard_consumer_groups: DashboardConsumerGroupListRequest => DashboardConsumerGroupListResult;
        query_dashboard_consumer_connection: DashboardConsumerConnectionRequest => DashboardConsumerConnection;
        query_dashboard_consumer_progress: DashboardConsumerProgressRequest => DashboardConsumerProgress;
        query_dashboard_consumer_config: DashboardConsumerConfigRequest => DashboardConsumerConfig;
        upsert_dashboard_consumer_group: DashboardConsumerUpsertRequest => DashboardConsumerMutationResult;
        delete_dashboard_consumer_group: DashboardConsumerDeleteRequest => DashboardConsumerMutationResult;
        delete_subscription_groups: DeleteSubscriptionGroupsRequest => ConsumerBatchMutationOutcome;
        set_consumer_request_mode: SetConsumerRequestModeRequest => SetConsumerRequestModeResult;
    }

    fn unused_admin_future<'a, T>() -> AdminFuture<'a, T> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "compile_only_consumer_admin",
                "compile-only consumer admin fake must not be called",
            ))
        })
    }

    #[test]
    fn running_info_request_requires_canonical_group_and_client_and_bounds_output() {
        let request =
            DashboardConsumerRunningInfoRequest::try_new(" orders-consumer ", " 10.0.0.8@client-a ", true, 256 * 1024)
                .expect("valid request");
        assert_eq!(request.consumer_group(), "orders-consumer");
        assert_eq!(request.client_id(), "10.0.0.8@client-a");
        assert!(request.include_jstack());
        assert_eq!(request.max_output_bytes(), 256 * 1024);
        assert!(DashboardConsumerRunningInfoRequest::try_new(" ", "client-a", false, 1024).is_err());
        assert!(DashboardConsumerRunningInfoRequest::try_new("orders", " ", false, 1024).is_err());
        assert!(DashboardConsumerRunningInfoRequest::try_new("orders", "client-a", false, 0).is_err());
        assert!(DashboardConsumerRunningInfoRequest::try_new("orders", "client-a", false, 1_048_577).is_err());
    }

    #[test]
    fn running_info_request_deserialization_cannot_bypass_validation() {
        let valid = serde_json::from_str::<DashboardConsumerRunningInfoRequest>(
            r#"{"consumer_group":" orders-consumer ","client_id":" client-a ","include_jstack":true,"max_output_bytes":1024}"#,
        )
        .expect("valid serialized request");
        assert_eq!(valid.consumer_group(), "orders-consumer");
        assert_eq!(valid.client_id(), "client-a");

        for invalid in [
            r#"{"consumer_group":" ","client_id":"client-a","include_jstack":false,"max_output_bytes":1024}"#,
            r#"{"consumer_group":"orders","client_id":"client-a","include_jstack":false,"max_output_bytes":0}"#,
            r#"{"consumer_group":"orders","client_id":"client-a","include_jstack":false,"max_output_bytes":1048577}"#,
        ] {
            assert!(serde_json::from_str::<DashboardConsumerRunningInfoRequest>(invalid).is_err());
        }
    }

    #[test]
    fn existing_consumer_admin_implementation_does_not_require_diagnostics() {
        let mut admin = ExistingConsumerAdmin;
        let _: &mut dyn ConsumerAdmin = &mut admin;
    }

    #[test]
    fn subscription_group_cas_request_accepts_only_a_non_empty_bounded_patch() {
        let valid = PatchSubscriptionGroupConfigRequest::try_new(
            "127.0.0.1:10911",
            "orders-consumer",
            7,
            SubscriptionGroupConfigCasPatch {
                retry_max_times: Some(8),
                retry_queue_nums: Some(4),
                consume_timeout_minutes: Some(30),
            },
        )
        .expect("bounded patch");
        assert_eq!(valid.expected_version, 7);

        assert!(PatchSubscriptionGroupConfigRequest::try_new(
            "127.0.0.1:10911",
            "orders-consumer",
            7,
            SubscriptionGroupConfigCasPatch::default(),
        )
        .is_err());
        assert!(PatchSubscriptionGroupConfigRequest::try_new(
            "127.0.0.1:10911",
            "orders-consumer",
            7,
            SubscriptionGroupConfigCasPatch {
                retry_max_times: Some(17),
                ..SubscriptionGroupConfigCasPatch::default()
            },
        )
        .is_err());
    }
}
