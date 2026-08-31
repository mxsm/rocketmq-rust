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

use rocketmq_protocol::protocol::subscription::subscription_group_config::validate_subscription_group_name;
use serde::Deserialize;
use serde::Serialize;

use crate::core::error::required;
use crate::core::query::AdminQueryResult;
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

/// Cheap, ordered consumer-group inventory without per-group enrichment.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerGroupInventoryResult {
    pub groups: Vec<String>,
}

/// Validated logical groups selected for bounded exact enrichment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExactConsumerGroupEnrichmentRequest {
    groups: Vec<String>,
}

impl<'de> Deserialize<'de> for ExactConsumerGroupEnrichmentRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireRequest {
            groups: Vec<String>,
        }

        let request = WireRequest::deserialize(deserializer)?;
        Self::try_new(request.groups).map_err(serde::de::Error::custom)
    }
}

impl ExactConsumerGroupEnrichmentRequest {
    /// Maximum number of logical groups accepted by one enrichment request.
    pub const MAX_GROUPS: usize = 200;

    /// Normalizes, sorts, deduplicates, and bounds exact logical group names.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error when a name is blank or the unique
    /// group count exceeds [`Self::MAX_GROUPS`].
    pub fn try_new<I, S>(groups: I) -> AdminResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut groups = groups
            .into_iter()
            .map(|group| {
                let group = required("consumerGroup", group)?;
                validate_subscription_group_name(&group)
                    .map_err(|error| crate::core::AdminError::invalid_argument("consumerGroup", error.to_string()))?;
                Ok(group)
            })
            .collect::<AdminResult<Vec<_>>>()?;
        groups.sort();
        groups.dedup();
        if groups.len() > Self::MAX_GROUPS {
            return Err(crate::core::AdminError::invalid_argument(
                "consumerGroups",
                format!("must contain at most {} unique groups", Self::MAX_GROUPS),
            ));
        }
        Ok(Self { groups })
    }

    #[must_use]
    pub fn groups(&self) -> &[String] {
        &self.groups
    }
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
    const MAX_OUTPUT_BYTES: usize = 256 * 1024;

    /// Creates a bounded consumer diagnostic request.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer group or client ID is blank, or
    /// when `max_output_bytes` is outside the inclusive range 1 byte to 256 KiB.
    pub fn try_new(
        consumer_group: impl Into<String>,
        client_id: impl Into<String>,
        include_jstack: bool,
        max_output_bytes: usize,
    ) -> AdminResult<Self> {
        if !(1..=Self::MAX_OUTPUT_BYTES).contains(&max_output_bytes) {
            return Err(crate::core::AdminError::invalid_argument(
                "maxOutputBytes",
                "must be between 1 and 262144 bytes",
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

pub struct DashboardConsumerRunningInfo {
    consumer_group: String,
    client_id: String,
    properties: Vec<DashboardConsumerConfigAttribute>,
    subscriptions: Vec<DashboardConsumerSubscriptionItem>,
    process_queues: Vec<DashboardConsumerProcessQueue>,
    jstack: Option<String>,
    /// True when requested property or JStack text was shortened at a UTF-8
    /// character boundary, omitted by the budget, or unavailable.
    truncated: bool,
}

/// Move-only fields for an explicitly selected transport boundary. The wire
/// decoder remains RocketMQ protocol's `ConsumerRunningInfo`; this type never
/// implements Serde itself.
pub struct DashboardConsumerRunningInfoParts {
    pub consumer_group: String,
    pub client_id: String,
    pub properties: Vec<DashboardConsumerConfigAttribute>,
    pub subscriptions: Vec<DashboardConsumerSubscriptionItem>,
    pub process_queues: Vec<DashboardConsumerProcessQueue>,
    pub jstack: Option<String>,
    pub truncated: bool,
}

impl DashboardConsumerRunningInfo {
    pub fn new(
        consumer_group: String,
        client_id: String,
        properties: Vec<DashboardConsumerConfigAttribute>,
        subscriptions: Vec<DashboardConsumerSubscriptionItem>,
        process_queues: Vec<DashboardConsumerProcessQueue>,
        jstack: Option<String>,
        truncated: bool,
    ) -> Self {
        Self {
            consumer_group,
            client_id,
            properties,
            subscriptions,
            process_queues,
            jstack,
            truncated,
        }
    }

    pub fn into_parts(mut self) -> DashboardConsumerRunningInfoParts {
        DashboardConsumerRunningInfoParts {
            consumer_group: std::mem::take(&mut self.consumer_group),
            client_id: std::mem::take(&mut self.client_id),
            properties: std::mem::take(&mut self.properties),
            subscriptions: std::mem::take(&mut self.subscriptions),
            process_queues: std::mem::take(&mut self.process_queues),
            jstack: std::mem::take(&mut self.jstack),
            truncated: self.truncated,
        }
    }

    pub fn into_diagnostic_parts(mut self) -> (Vec<DashboardConsumerConfigAttribute>, Option<String>, bool) {
        (
            std::mem::take(&mut self.properties),
            std::mem::take(&mut self.jstack),
            self.truncated,
        )
    }
}

impl Drop for DashboardConsumerRunningInfo {
    fn drop(&mut self) {
        for property in &mut self.properties {
            scrub_string(&mut property.key);
            scrub_string(&mut property.value);
        }
        if let Some(jstack) = &mut self.jstack {
            scrub_string(jstack);
        }
    }
}

fn scrub_string(value: &mut String) {
    let byte_len = value.len();
    if byte_len > 0 {
        value.replace_range(.., &"\0".repeat(byte_len));
        value.clear();
    }
}

impl std::fmt::Debug for DashboardConsumerRunningInfo {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DashboardConsumerRunningInfo")
            .field("property_count", &self.properties.len())
            .field("subscription_count", &self.subscriptions.len())
            .field("process_queue_count", &self.process_queues.len())
            .field("jstack_loaded", &self.jstack.is_some())
            .field("truncated", &self.truncated)
            .finish_non_exhaustive()
    }
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

const CID_RMQ_SYS_PREFIX: &str = "CID_RMQ_SYS_";
const SYSTEM_CONSUMER_GROUPS: &[&str] = &[
    "TOOLS_CONSUMER",
    "FILTERSRV_CONSUMER",
    "SELF_TEST_C_GROUP",
    "CID_ONS-HTTP-PROXY",
    "CID_ONSAPI_PULL",
    "CID_ONSAPI_PERMISSION",
    "CID_ONSAPI_OWNER",
    "CID_RMQ_SYS_TRANS",
    "CID_DefaultHeartBeatSyncerTopic",
];

/// Validated create-or-update request for a complete multi-broker workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerBatchUpsertRequest {
    inner: DashboardConsumerUpsertRequest,
}

impl ConsumerBatchUpsertRequest {
    /// Normalizes target names and rejects invalid or protected groups.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error when the group, target selection, or
    /// retry limits are invalid.
    pub fn try_new(mut inner: DashboardConsumerUpsertRequest) -> AdminResult<Self> {
        inner.consumer_group = validate_batch_consumer_group(inner.consumer_group)?;
        inner.cluster_name_list = canonical_names(inner.cluster_name_list);
        inner.broker_name_list = canonical_names(inner.broker_name_list);
        if inner.cluster_name_list.is_empty() && inner.broker_name_list.is_empty() {
            return Err(crate::core::AdminError::invalid_argument(
                "brokerNameList",
                "Select at least one cluster or broker before saving the consumer group.",
            ));
        }
        validate_batch_consumer_limits(&inner)?;
        Ok(Self { inner })
    }

    #[cfg(feature = "mutation-client-adapter")]
    pub(crate) fn inner(&self) -> &DashboardConsumerUpsertRequest {
        &self.inner
    }
}

/// Validated delete request carrying both selected and authoritative brokers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerBatchDeleteRequest {
    consumer_group: String,
    selected_broker_names: Vec<String>,
    all_broker_names: Vec<String>,
}

impl ConsumerBatchDeleteRequest {
    /// Normalizes both broker sets and verifies that selection is a subset.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error when the group is protected, either
    /// broker set is empty, or a selected broker is outside the authoritative
    /// set.
    pub fn try_new<Selected, SelectedName, All, AllName>(
        consumer_group: impl Into<String>,
        selected_broker_names: Selected,
        all_broker_names: All,
    ) -> AdminResult<Self>
    where
        Selected: IntoIterator<Item = SelectedName>,
        SelectedName: Into<String>,
        All: IntoIterator<Item = AllName>,
        AllName: Into<String>,
    {
        let consumer_group = validate_batch_consumer_group(consumer_group)?;
        let selected_broker_names = canonical_names(selected_broker_names);
        if selected_broker_names.is_empty() {
            return Err(crate::core::AdminError::invalid_argument(
                "selectedBrokerNames",
                "Select at least one broker before deleting the consumer group.",
            ));
        }
        let all_broker_names = canonical_names(all_broker_names);
        if all_broker_names.is_empty() {
            return Err(crate::core::AdminError::invalid_argument(
                "allBrokerNames",
                "The authoritative broker set must not be empty.",
            ));
        }
        if let Some(broker_name) = selected_broker_names
            .iter()
            .find(|broker_name| all_broker_names.binary_search(broker_name).is_err())
        {
            return Err(crate::core::AdminError::invalid_argument(
                "selectedBrokerNames",
                format!("Broker `{broker_name}` is outside the authoritative broker set."),
            ));
        }
        Ok(Self {
            consumer_group,
            selected_broker_names,
            all_broker_names,
        })
    }

    #[cfg(feature = "mutation-client-adapter")]
    pub(crate) fn consumer_group(&self) -> &str {
        &self.consumer_group
    }

    #[cfg(feature = "mutation-client-adapter")]
    pub(crate) fn selected_broker_names(&self) -> &[String] {
        &self.selected_broker_names
    }

    #[cfg(feature = "mutation-client-adapter")]
    pub(crate) fn all_broker_names(&self) -> &[String] {
        &self.all_broker_names
    }
}

/// One immutable broker identity confirmed by a read-only preflight.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConsumerExactBatchDeleteTarget {
    cluster_name: String,
    broker_name: String,
    broker_address: String,
}

impl ConsumerExactBatchDeleteTarget {
    /// Builds a non-empty exact broker identity.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error when any identity component is empty.
    pub fn try_new(
        cluster_name: impl Into<String>,
        broker_name: impl Into<String>,
        broker_address: impl Into<String>,
    ) -> AdminResult<Self> {
        Ok(Self {
            cluster_name: required_identity("clusterName", cluster_name.into())?,
            broker_name: required_identity("brokerName", broker_name.into())?,
            broker_address: required_identity("brokerAddress", broker_address.into())?,
        })
    }

    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    pub fn broker_address(&self) -> &str {
        &self.broker_address
    }
}

/// Exact broker identity used by create-or-update mutations.
pub type ConsumerExactBatchUpsertTarget = ConsumerExactBatchDeleteTarget;

/// Create-or-update request whose targets retain the exact cluster, broker,
/// and address observed before confirmation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerExactBatchUpsertRequest {
    inner: DashboardConsumerUpsertRequest,
    targets: Vec<ConsumerExactBatchUpsertTarget>,
}

impl ConsumerExactBatchUpsertRequest {
    /// Validates the consumer configuration and exact target identities.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error for protected groups, invalid retry
    /// limits, empty targets, or ambiguous broker identities.
    pub fn try_new(
        mut inner: DashboardConsumerUpsertRequest,
        targets: impl IntoIterator<Item = ConsumerExactBatchUpsertTarget>,
    ) -> AdminResult<Self> {
        inner.consumer_group = validate_batch_consumer_group(inner.consumer_group)?;
        let targets = canonical_exact_targets("targets", targets)?;
        if targets.is_empty() {
            return Err(crate::core::AdminError::invalid_argument(
                "targets",
                "Select at least one exact broker target before saving the consumer group.",
            ));
        }
        validate_batch_consumer_limits(&inner)?;
        inner.cluster_name_list.clear();
        inner.broker_name_list = targets.iter().map(|target| target.broker_name.clone()).collect();
        Ok(Self { inner, targets })
    }

    pub fn targets(&self) -> &[ConsumerExactBatchUpsertTarget] {
        &self.targets
    }

    #[cfg(feature = "mutation-client-adapter")]
    pub(crate) fn inner(&self) -> &DashboardConsumerUpsertRequest {
        &self.inner
    }
}

/// Delete request whose selected and authoritative targets retain the exact
/// cluster, broker, and address observed before confirmation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerExactBatchDeleteRequest {
    consumer_group: String,
    selected_targets: Vec<ConsumerExactBatchDeleteTarget>,
    authoritative_targets: Vec<ConsumerExactBatchDeleteTarget>,
}

impl ConsumerExactBatchDeleteRequest {
    /// Validates the exact target identities and selection relationship.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error for protected groups, empty or
    /// ambiguous target sets, or a selection outside the authoritative set.
    pub fn try_new(
        consumer_group: impl Into<String>,
        selected_targets: impl IntoIterator<Item = ConsumerExactBatchDeleteTarget>,
        authoritative_targets: impl IntoIterator<Item = ConsumerExactBatchDeleteTarget>,
    ) -> AdminResult<Self> {
        let consumer_group = validate_batch_consumer_group(consumer_group)?;
        let selected_targets = canonical_exact_targets("selectedTargets", selected_targets)?;
        if selected_targets.is_empty() {
            return Err(crate::core::AdminError::invalid_argument(
                "selectedTargets",
                "Select at least one exact broker target before deleting the consumer group.",
            ));
        }
        let authoritative_targets = canonical_exact_targets("authoritativeTargets", authoritative_targets)?;
        if authoritative_targets.is_empty() {
            return Err(crate::core::AdminError::invalid_argument(
                "authoritativeTargets",
                "The authoritative exact target set must not be empty.",
            ));
        }
        if let Some(target) = selected_targets
            .iter()
            .find(|target| authoritative_targets.binary_search(target).is_err())
        {
            return Err(crate::core::AdminError::invalid_argument(
                "selectedTargets",
                format!(
                    "Broker `{}` is outside the authoritative exact target set.",
                    target.broker_name
                ),
            ));
        }
        Ok(Self {
            consumer_group,
            selected_targets,
            authoritative_targets,
        })
    }

    pub fn consumer_group(&self) -> &str {
        &self.consumer_group
    }

    pub fn selected_targets(&self) -> &[ConsumerExactBatchDeleteTarget] {
        &self.selected_targets
    }

    pub fn authoritative_targets(&self) -> &[ConsumerExactBatchDeleteTarget] {
        &self.authoritative_targets
    }
}

fn required_identity(field: &'static str, value: String) -> AdminResult<String> {
    let value = value.trim();
    if value.is_empty() {
        Err(crate::core::AdminError::invalid_argument(field, "must not be empty"))
    } else {
        Ok(value.to_owned())
    }
}

fn canonical_exact_targets(
    field: &'static str,
    targets: impl IntoIterator<Item = ConsumerExactBatchDeleteTarget>,
) -> AdminResult<Vec<ConsumerExactBatchDeleteTarget>> {
    let mut targets = targets.into_iter().collect::<Vec<_>>();
    targets.sort();
    targets.dedup();
    let mut broker_names = std::collections::BTreeSet::new();
    if let Some(target) = targets
        .iter()
        .find(|target| !broker_names.insert(target.broker_name.as_str()))
    {
        return Err(crate::core::AdminError::invalid_argument(
            field,
            format!("Broker `{}` has more than one exact identity.", target.broker_name),
        ));
    }
    Ok(targets)
}

/// Outcome of one broker mutation or one internal-topic cleanup target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerTargetOutcome {
    pub target: String,
    pub kind: String,
    pub success: bool,
    pub message: String,
}

/// Stable ordered outcomes for a closed consumer batch workflow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConsumerBatchResult {
    pub consumer_group: String,
    pub success: bool,
    pub targets: Vec<DashboardConsumerTargetOutcome>,
}

fn canonical_names<Names, Name>(names: Names) -> Vec<String>
where
    Names: IntoIterator<Item = Name>,
    Name: Into<String>,
{
    let mut names = names
        .into_iter()
        .map(Into::into)
        .map(|name: String| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

fn validate_batch_consumer_group(value: impl Into<String>) -> AdminResult<String> {
    let value = required("consumerGroup", value)?;
    if value.starts_with("%SYS%") || is_system_consumer_group(&value) {
        return Err(crate::core::AdminError::invalid_argument(
            "consumerGroup",
            "System consumer groups cannot be mutated.",
        ));
    }
    validate_subscription_group_name(&value)
        .map_err(|error| crate::core::AdminError::invalid_argument("consumerGroup", error.to_string()))?;
    Ok(value)
}

fn validate_batch_consumer_limits(request: &DashboardConsumerUpsertRequest) -> AdminResult<()> {
    if request.retry_queue_nums < 0 {
        return Err(crate::core::AdminError::invalid_argument(
            "retryQueueNums",
            "Retry queues must be zero or greater.",
        ));
    }
    if request.retry_max_times < -1 {
        return Err(crate::core::AdminError::invalid_argument(
            "retryMaxTimes",
            "Max retries must be -1 or greater.",
        ));
    }
    if request.consume_timeout_minute <= 0 {
        return Err(crate::core::AdminError::invalid_argument(
            "consumeTimeoutMinute",
            "Consume timeout must be greater than zero.",
        ));
    }
    Ok(())
}

pub(crate) fn is_system_consumer_group(group: &str) -> bool {
    group.starts_with(CID_RMQ_SYS_PREFIX) || SYSTEM_CONSUMER_GROUPS.contains(&group)
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

    fn list_consumer_groups_with_evidence<'a>(
        &'a mut self,
        request: &'a ListConsumerGroupsRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ListConsumerGroupsResult>> {
        Box::pin(async move { self.list_consumer_groups(request).await.map(AdminQueryResult::complete) })
    }

    /// Evidence-aware cheap inventory sibling of [`Self::list_consumer_groups`].
    ///
    /// Implementations must use a single inventory source and perform no
    /// per-group enrichment. The default fails closed when that capability is
    /// unavailable.
    fn list_consumer_group_inventory_with_evidence<'a>(
        &'a mut self,
        _request: &'a ListConsumerGroupsRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ConsumerGroupInventoryResult>> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "list_consumer_group_inventory_with_evidence",
                "cheap consumer group inventory is not implemented by this adapter",
            ))
        })
    }

    /// Evidence-aware bounded enrichment for an exact logical group set.
    ///
    /// Implementations must contact only the selected groups and must not
    /// obtain an unbounded consumer-group inventory. The default fails closed
    /// when bounded enrichment is unavailable.
    fn enrich_consumer_groups_exact_with_evidence<'a>(
        &'a mut self,
        _request: &'a ExactConsumerGroupEnrichmentRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ListConsumerGroupsResult>> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "enrich_consumer_groups_exact_with_evidence",
                "bounded exact consumer group enrichment is not implemented by this adapter",
            ))
        })
    }

    fn query_consumer_lag_with_evidence<'a>(
        &'a mut self,
        request: &'a QueryConsumerLagRequest,
    ) -> AdminFuture<'a, AdminQueryResult<QueryConsumerLagResult>> {
        Box::pin(async move { self.query_consumer_lag(request).await.map(AdminQueryResult::complete) })
    }

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
    /// Evidence-aware sibling of [`Self::list_consumer_groups`].
    fn list_consumer_groups_with_evidence<'a>(
        &'a mut self,
        request: &'a ListConsumerGroupsRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ListConsumerGroupsResult>> {
        Box::pin(async move { self.list_consumer_groups(request).await.map(AdminQueryResult::complete) })
    }
    /// Evidence-aware cheap inventory sibling of [`Self::list_consumer_groups`].
    ///
    /// Implementations must use a single inventory source and perform no
    /// per-group enrichment. The default fails closed when that capability is
    /// unavailable.
    fn list_consumer_group_inventory_with_evidence<'a>(
        &'a mut self,
        _request: &'a ListConsumerGroupsRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ConsumerGroupInventoryResult>> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "list_consumer_group_inventory_with_evidence",
                "cheap consumer group inventory is not implemented by this adapter",
            ))
        })
    }
    /// Evidence-aware bounded enrichment for an exact logical group set.
    ///
    /// Implementations must contact only the selected groups and must not
    /// obtain an unbounded consumer-group inventory. The default fails closed
    /// when bounded enrichment is unavailable.
    fn enrich_consumer_groups_exact_with_evidence<'a>(
        &'a mut self,
        _request: &'a ExactConsumerGroupEnrichmentRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ListConsumerGroupsResult>> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "enrich_consumer_groups_exact_with_evidence",
                "bounded exact consumer group enrichment is not implemented by this adapter",
            ))
        })
    }
    /// Evidence-aware sibling of [`Self::query_consumer_lag`].
    fn query_consumer_lag_with_evidence<'a>(
        &'a mut self,
        request: &'a QueryConsumerLagRequest,
    ) -> AdminFuture<'a, AdminQueryResult<QueryConsumerLagResult>> {
        Box::pin(async move { self.query_consumer_lag(request).await.map(AdminQueryResult::complete) })
    }
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

/// Complete consumer batch mutations owned by one leased admin session.
pub trait ConsumerBatchMutationAdmin: Send {
    fn upsert_consumer_group_batch<'a>(
        &'a mut self,
        request: &'a ConsumerBatchUpsertRequest,
    ) -> AdminFuture<'a, DashboardConsumerBatchResult>;

    fn delete_consumer_group_batch<'a>(
        &'a mut self,
        request: &'a ConsumerBatchDeleteRequest,
    ) -> AdminFuture<'a, DashboardConsumerBatchResult>;
}

/// Exact-target consumer deletion owned by one leased mutation session.
pub trait ConsumerExactBatchMutationAdmin: Send {
    fn delete_consumer_group_exact_batch<'a>(
        &'a mut self,
        request: &'a ConsumerExactBatchDeleteRequest,
    ) -> AdminFuture<'a, DashboardConsumerBatchResult>;
}

/// Exact-target consumer create-or-update owned by one leased mutation
/// session. Implementations must revalidate the confirmed address set before
/// attempting any write.
pub trait ConsumerExactBatchUpsertMutationAdmin: Send {
    fn upsert_consumer_group_exact_batch<'a>(
        &'a mut self,
        request: &'a ConsumerExactBatchUpsertRequest,
    ) -> AdminFuture<'a, DashboardConsumerBatchResult>;
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
    fn list_consumer_groups_with_evidence<'a>(
        &'a mut self,
        request: &'a ListConsumerGroupsRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ListConsumerGroupsResult>> {
        ConsumerAdmin::list_consumer_groups_with_evidence(self, request)
    }
    fn list_consumer_group_inventory_with_evidence<'a>(
        &'a mut self,
        request: &'a ListConsumerGroupsRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ConsumerGroupInventoryResult>> {
        ConsumerAdmin::list_consumer_group_inventory_with_evidence(self, request)
    }
    fn enrich_consumer_groups_exact_with_evidence<'a>(
        &'a mut self,
        request: &'a ExactConsumerGroupEnrichmentRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ListConsumerGroupsResult>> {
        ConsumerAdmin::enrich_consumer_groups_exact_with_evidence(self, request)
    }
    fn query_consumer_lag_with_evidence<'a>(
        &'a mut self,
        request: &'a QueryConsumerLagRequest,
    ) -> AdminFuture<'a, AdminQueryResult<QueryConsumerLagResult>> {
        ConsumerAdmin::query_consumer_lag_with_evidence(self, request)
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
pub(crate) mod batch_test_support {
    use super::*;

    pub(crate) fn unchecked_upsert_request(inner: DashboardConsumerUpsertRequest) -> ConsumerBatchUpsertRequest {
        ConsumerBatchUpsertRequest { inner }
    }

    pub(crate) fn unchecked_delete_request(
        consumer_group: impl Into<String>,
        selected_broker_names: Vec<String>,
        all_broker_names: Vec<String>,
    ) -> ConsumerBatchDeleteRequest {
        ConsumerBatchDeleteRequest {
            consumer_group: consumer_group.into(),
            selected_broker_names,
            all_broker_names,
        }
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
        assert!(DashboardConsumerRunningInfoRequest::try_new("orders", "client-a", false, 262_145).is_err());
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
            r#"{"consumer_group":"orders","client_id":"client-a","include_jstack":false,"max_output_bytes":262145}"#,
        ] {
            assert!(serde_json::from_str::<DashboardConsumerRunningInfoRequest>(invalid).is_err());
        }
    }

    #[test]
    fn exact_consumer_group_enrichment_request_is_sorted_unique_and_bounded() {
        let request = ExactConsumerGroupEnrichmentRequest::try_new([" group-b ", "group-a", "group-b"]).unwrap();
        assert_eq!(request.groups(), ["group-a", "group-b"]);
        assert!(ExactConsumerGroupEnrichmentRequest::try_new([" "]).is_err());
        assert!(ExactConsumerGroupEnrichmentRequest::try_new(["group.with.dot"]).is_err());
        assert!(ExactConsumerGroupEnrichmentRequest::try_new(["g".repeat(256)]).is_err());
        assert!(ExactConsumerGroupEnrichmentRequest::try_new(["g".repeat(255)]).is_ok());
        assert!(ExactConsumerGroupEnrichmentRequest::try_new(
            (0..=ExactConsumerGroupEnrichmentRequest::MAX_GROUPS).map(|index| format!("group-{index}"))
        )
        .is_err());
        assert!(
            serde_json::from_value::<ExactConsumerGroupEnrichmentRequest>(serde_json::json!({
                "groups": [" "]
            }))
            .is_err()
        );
    }

    #[test]
    fn existing_consumer_admin_implementation_does_not_require_diagnostics() {
        let mut admin = ExistingConsumerAdmin;
        let _: &mut dyn ConsumerAdmin = &mut admin;
    }

    #[tokio::test]
    async fn cheap_inventory_defaults_fail_closed_without_full_enrichment() {
        let mut admin = ExistingConsumerAdmin;
        let inventory =
            ConsumerAdmin::list_consumer_group_inventory_with_evidence(&mut admin, &ListConsumerGroupsRequest)
                .await
                .unwrap_err();
        assert!(matches!(
            inventory,
            crate::core::AdminError::Backend {
                operation: "list_consumer_group_inventory_with_evidence",
                ..
            }
        ));

        let exact = ExactConsumerGroupEnrichmentRequest::try_new(["orders"]).unwrap();
        let enrichment = ConsumerQueryAdmin::enrich_consumer_groups_exact_with_evidence(&mut admin, &exact)
            .await
            .unwrap_err();
        assert!(matches!(
            enrichment,
            crate::core::AdminError::Backend {
                operation: "enrich_consumer_groups_exact_with_evidence",
                ..
            }
        ));
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
