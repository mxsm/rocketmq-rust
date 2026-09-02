// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Narrow administration mutation surface.
//!
//! Method names are intentionally distinct from the full administration
//! capability set, allowing mutation-only consumers to depend on an explicit
//! supervised capability.

use std::collections::HashMap;
use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::message_enum::MessageRequestMode;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_protocol::protocol::admin::rollback_stats::RollbackStats;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_protocol::protocol::body::proxy_drain::ProxyDrainStateResponseBody;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

use super::default_mq_admin_ext::DefaultMQAdminExt;

/// Result of one generation-checked broker configuration patch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerConfigPatchOutcome {
    /// The patch was committed as the generation immediately after precheck.
    Applied { previous_generation: u64, generation: u64 },
    /// The broker changed after precheck; callers must stop and re-plan.
    GenerationConflict {
        expected_generation: u64,
        actual_generation: u64,
    },
}

/// Closed Topic fields accepted by the supervised version-CAS operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TopicConfigPatch {
    pub read_queue_nums: Option<u32>,
    pub write_queue_nums: Option<u32>,
    pub order: Option<bool>,
}

impl TopicConfigPatch {
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.read_queue_nums.is_none() && self.write_queue_nums.is_none() && self.order.is_none()
    }
}

/// Result of one version-checked Topic configuration patch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopicConfigPatchOutcome {
    /// The patch was committed as the version immediately after precheck.
    Applied { previous_version: u64, version: u64 },
    /// Topic metadata changed after precheck; callers must stop and re-plan.
    VersionConflict { expected_version: u64, actual_version: u64 },
}

/// Allowlisted Topic state returned to an isolated mutation preflight.
#[derive(Clone, Debug, PartialEq)]
pub struct MutationTopicConfigVersioned {
    pub version: u64,
    pub config: TopicConfig,
}

/// Closed presence/version condition for full supervised metadata replacement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationExpectedState {
    Absent,
    Present { version: u64 },
}

/// Closed Topic message types supported by supervised replacement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationTopicMessageType {
    Normal,
    Fifo,
    Delay,
    Transaction,
    Unspecified,
}

impl MutationTopicMessageType {
    pub(crate) const fn wire_name(self) -> &'static str {
        match self {
            Self::Normal => "NORMAL",
            Self::Fifo => "FIFO",
            Self::Delay => "DELAY",
            Self::Transaction => "TRANSACTION",
            Self::Unspecified => "UNSPECIFIED",
        }
    }
}

/// Complete allowlisted Topic state used by a supervised replace operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationTopicConfig {
    pub read_queue_nums: u32,
    pub write_queue_nums: u32,
    pub perm: u32,
    pub order: bool,
    pub message_type: MutationTopicMessageType,
}

/// Presence-aware Topic preflight result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationTopicConfigState {
    pub state: MutationExpectedState,
    pub config: Option<MutationTopicConfig>,
}

/// Complete allowlisted Subscription Group state used by supervised replacement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationSubscriptionGroupConfig {
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

/// Presence-aware Subscription Group preflight result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationSubscriptionGroupConfigState {
    pub state: MutationExpectedState,
    pub config: Option<MutationSubscriptionGroupConfig>,
}

/// Result of one presence/version conditional replacement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MutationStateCasOutcome {
    pub applied: bool,
    pub changed: bool,
    pub state: MutationExpectedState,
    pub persistence: MutationPersistenceState,
}

/// Durable state of an accepted supervised mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationPersistenceState {
    NotRequired,
    Persisted,
    Failed,
}

/// Six-key Broker state allowed into mutation preflight.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BrokerMutationConfigState {
    pub generation: u64,
    pub auto_create_topic_enable: bool,
    pub auto_create_subscription_group: bool,
    pub broker_permission: u32,
    pub default_topic_queue_nums: u32,
    pub message_index_enable: bool,
    pub trace_topic_enable: bool,
}

/// Exact conditional consumer-offset update result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConditionalConsumerOffsetOutcome {
    pub applied: bool,
    pub actual_offset: i64,
}

/// Pure preflight row for one logical Broker queue; no network address escapes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationConsumerOffsetPreview {
    pub broker_name: String,
    pub queue_id: i32,
    pub current_offset: i64,
    pub planned_offset: i64,
}

/// Exact request-mode value returned by a selected Broker master.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MutationMessageRequestMode {
    pub mode: MessageRequestMode,
    pub pop_share_queue_num: i32,
}

/// Closed request-mode precondition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationExpectedMessageRequestMode {
    Absent,
    Present(MutationMessageRequestMode),
}

/// Conditional request-mode result containing only the current typed value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MutationMessageRequestModeOutcome {
    pub applied: bool,
    pub changed: bool,
    pub current: Option<MutationMessageRequestMode>,
    pub persistence: MutationPersistenceState,
}

/// Closed failure categories for one detailed consumer-offset target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopicOffsetMutationFailureCode {
    InvalidData,
    Unavailable,
}

/// One broker or queue result from a detailed reset/skip workflow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicOffsetMutationTargetOutcome {
    pub broker_name: String,
    pub queue_id: Option<i32>,
    pub applied: bool,
    pub offset: Option<u64>,
    pub failure: Option<TopicOffsetMutationFailureCode>,
    pub retryable: bool,
}

/// Failure-aware results for all exact broker/queue targets reached by one offset mutation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TopicOffsetMutationOutcome {
    pub targets: Vec<TopicOffsetMutationTargetOutcome>,
}

/// Closed Subscription Group fields accepted by the supervised version-CAS
/// operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SubscriptionGroupConfigPatch {
    pub retry_max_times: Option<u32>,
    pub retry_queue_nums: Option<u32>,
    pub consume_timeout_minutes: Option<u32>,
}

impl SubscriptionGroupConfigPatch {
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.retry_max_times.is_none() && self.retry_queue_nums.is_none() && self.consume_timeout_minutes.is_none()
    }
}

/// Result of one version-checked Subscription Group configuration patch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubscriptionGroupConfigPatchOutcome {
    /// The patch was committed as the version immediately after precheck.
    Applied { previous_version: u64, version: u64 },
    /// Subscription Group metadata changed after precheck; callers must stop
    /// and re-plan.
    VersionConflict { expected_version: u64, actual_version: u64 },
}

/// Explicit RocketMQ mutation capability.
///
/// New integrations should request this trait deliberately and keep it out of
/// read-only process dependency graphs.
#[allow(async_fn_in_trait)]
pub trait MQAdminMutationExt: Send {
    /// Begins one authenticated, reversible drain operation for a Proxy.
    async fn begin_proxy_drain(
        &self,
        proxy_addr: CheetahString,
        operation_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProxyDrainStateResponseBody>;

    /// Cancels a timed-out drain and restores Proxy admission/readiness.
    async fn cancel_proxy_drain(
        &self,
        proxy_addr: CheetahString,
        operation_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProxyDrainStateResponseBody>;

    /// Reads the broker configuration generation used by supervised prechecks.
    async fn broker_config_generation(&self, broker_addr: CheetahString) -> rocketmq_error::RocketMQResult<u64>;

    /// Applies a broker configuration patch only when `expected_generation`
    /// still matches the broker's current generation.
    async fn patch_broker_config_if_generation(
        &self,
        broker_addr: CheetahString,
        expected_generation: u64,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> rocketmq_error::RocketMQResult<BrokerConfigPatchOutcome>;

    /// Changes only the three fields in [`TopicConfigPatch`] when the Broker's
    /// current Topic metadata version still matches `expected_version`.
    async fn patch_topic_config_if_version(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        expected_version: u64,
        patch: TopicConfigPatch,
    ) -> rocketmq_error::RocketMQResult<TopicConfigPatchOutcome>;

    /// Reads only the Topic fields and version needed to prepare a supervised
    /// queue-count compare-and-set mutation.
    async fn mutation_topic_config_with_version(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MutationTopicConfigVersioned> {
        let _ = (broker_addr, topic);
        Err(rocketmq_error::RocketMQError::illegal_argument(
            "versioned Topic mutation preflight is not implemented by this admin client",
        ))
    }

    /// Reads a presence-aware, allowlisted Topic state from one exact Broker.
    async fn mutation_topic_config_state(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MutationTopicConfigState> {
        let _ = (broker_addr, topic);
        Err(rocketmq_error::RocketMQError::illegal_argument(
            "presence-aware Topic mutation preflight is not implemented by this admin client",
        ))
    }

    /// Creates or fully replaces the allowlisted Topic state only when its
    /// presence/version precondition still matches.
    async fn replace_topic_config_if_state(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        expected_state: MutationExpectedState,
        replacement: MutationTopicConfig,
    ) -> rocketmq_error::RocketMQResult<MutationStateCasOutcome> {
        let _ = (broker_addr, topic, expected_state, replacement);
        Err(rocketmq_error::RocketMQError::illegal_argument(
            "presence-aware Topic replacement is not implemented by this admin client",
        ))
    }

    /// Changes only the three fields in [`SubscriptionGroupConfigPatch`] when
    /// the Broker's current Subscription Group metadata version still matches
    /// `expected_version`.
    async fn patch_subscription_group_config_if_version(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
        expected_version: u64,
        patch: SubscriptionGroupConfigPatch,
    ) -> rocketmq_error::RocketMQResult<SubscriptionGroupConfigPatchOutcome>;

    /// Reads presence-aware, allowlisted Subscription Group state from one exact Broker.
    async fn mutation_subscription_group_config_state(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MutationSubscriptionGroupConfigState> {
        let _ = (broker_addr, group);
        Err(rocketmq_error::RocketMQError::illegal_argument(
            "presence-aware Subscription Group mutation preflight is not implemented by this admin client",
        ))
    }

    /// Creates or fully replaces the allowlisted Subscription Group state under one state CAS.
    async fn replace_subscription_group_config_if_state(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
        expected_state: MutationExpectedState,
        replacement: MutationSubscriptionGroupConfig,
    ) -> rocketmq_error::RocketMQResult<MutationStateCasOutcome> {
        let _ = (broker_addr, group, expected_state, replacement);
        Err(rocketmq_error::RocketMQError::illegal_argument(
            "presence-aware Subscription Group replacement is not implemented by this admin client",
        ))
    }

    /// Reads only the six Broker settings accepted by supervised control preflight.
    async fn broker_mutation_config_state(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<BrokerMutationConfigState> {
        let _ = broker_addr;
        Err(rocketmq_error::RocketMQError::illegal_argument(
            "allowlisted Broker mutation preflight is not implemented by this admin client",
        ))
    }

    /// Applies one exact queue offset transition without route re-resolution or retry.
    async fn reset_consumer_offset_if_current(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        topic: CheetahString,
        queue_id: i32,
        expected_offset: i64,
        new_offset: i64,
    ) -> rocketmq_error::RocketMQResult<ConditionalConsumerOffsetOutcome> {
        let _ = (
            broker_addr,
            consumer_group,
            topic,
            queue_id,
            expected_offset,
            new_offset,
        );
        Err(rocketmq_error::RocketMQError::illegal_argument(
            "conditional consumer-offset mutation is not implemented by this admin client",
        ))
    }

    /// Computes exact queue offsets for one already-selected Broker master without mutation.
    async fn preview_consumer_offset_reset_on_broker(
        &self,
        broker_addr: CheetahString,
        broker_name: CheetahString,
        read_queue_nums: u32,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: i64,
    ) -> rocketmq_error::RocketMQResult<Vec<MutationConsumerOffsetPreview>> {
        let _ = (
            broker_addr,
            broker_name,
            read_queue_nums,
            consumer_group,
            topic,
            timestamp,
        );
        Err(rocketmq_error::RocketMQError::illegal_argument(
            "consumer-offset mutation preview is not implemented by this admin client",
        ))
    }

    /// Re-reads one exact queue offset for postcondition verification.
    async fn mutation_consumer_offset(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        topic: CheetahString,
        queue_id: i32,
    ) -> rocketmq_error::RocketMQResult<i64> {
        let _ = (broker_addr, consumer_group, topic, queue_id);
        Err(rocketmq_error::RocketMQError::illegal_argument(
            "consumer-offset mutation verification is not implemented by this admin client",
        ))
    }

    /// Reads one exact Topic/group request-mode entry from a selected Broker master.
    async fn mutation_message_request_mode(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<MutationMessageRequestMode>> {
        let _ = (broker_addr, topic, consumer_group);
        Err(rocketmq_error::RocketMQError::illegal_argument(
            "request-mode mutation preflight is not implemented by this admin client",
        ))
    }

    /// Conditionally replaces one exact Topic/group request-mode entry.
    async fn replace_message_request_mode_if_current(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        expected: MutationExpectedMessageRequestMode,
        replacement: MutationMessageRequestMode,
    ) -> rocketmq_error::RocketMQResult<MutationMessageRequestModeOutcome> {
        let _ = (broker_addr, topic, consumer_group, expected, replacement);
        Err(rocketmq_error::RocketMQError::illegal_argument(
            "conditional request-mode mutation is not implemented by this admin client",
        ))
    }

    /// Applies one bounded Broker logger override with an automatic TTL.
    ///
    /// Implementations must reject arbitrary filter expressions. `logger`
    /// identifies one `rocketmq_broker::` target, `level` is limited to
    /// `INFO` or `DEBUG`, and the TTL is limited to the server-supported
    /// diagnostic window.
    async fn set_broker_log_filter_ttl(
        &self,
        broker_addr: CheetahString,
        logger: CheetahString,
        level: CheetahString,
        ttl_seconds: u32,
        operation_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let _ = (broker_addr, logger, level, ttl_seconds, operation_id);
        Err(rocketmq_error::RocketMQError::illegal_argument(
            "typed broker log-filter mutation is not implemented by this admin client",
        ))
    }

    /// Restores the Broker logger baseline for one bounded operation.
    async fn restore_broker_log_filter(
        &self,
        broker_addr: CheetahString,
        operation_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let _ = (broker_addr, operation_id);
        Err(rocketmq_error::RocketMQError::illegal_argument(
            "typed broker log-filter restoration is not implemented by this admin client",
        ))
    }

    async fn upsert_topic_config(
        &self,
        broker_addr: CheetahString,
        config: TopicConfig,
    ) -> rocketmq_error::RocketMQResult<()>;

    async fn remove_topic(
        &self,
        topic_name: CheetahString,
        cluster_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()>;

    async fn reset_consumer_offset(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        consumer_group: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<HashMap<MessageQueue, u64>>;

    /// Resets each exact broker/queue target independently and retains partial outcomes.
    async fn reset_consumer_offset_detailed(
        &self,
        cluster_name: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<TopicOffsetMutationOutcome> {
        let _ = (cluster_name, topic, consumer_group, timestamp, force);
        Err(rocketmq_error::RocketMQError::illegal_argument(
            "detailed reset-offset mutation is not implemented by this admin client",
        ))
    }

    /// Advances one exact consumer group and Topic to the latest available
    /// offsets. This is deliberately separate from timestamp reset because the
    /// RocketMQ protocol represents latest with the signed `-1` sentinel.
    async fn skip_accumulated_message(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        consumer_group: CheetahString,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<usize> {
        let _ = (cluster_name, topic, consumer_group, force);
        Err(rocketmq_error::RocketMQError::illegal_argument(
            "skip-accumulated mutation is not implemented by this admin client",
        ))
    }

    /// Advances each exact broker/queue target to latest while retaining partial outcomes.
    async fn skip_accumulated_message_detailed(
        &self,
        cluster_name: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<TopicOffsetMutationOutcome> {
        let _ = (cluster_name, topic, consumer_group, force);
        Err(rocketmq_error::RocketMQError::illegal_argument(
            "detailed skip-accumulated mutation is not implemented by this admin client",
        ))
    }

    async fn upsert_subscription_group(
        &self,
        broker_addr: CheetahString,
        config: SubscriptionGroupConfig,
    ) -> rocketmq_error::RocketMQResult<()>;

    async fn remove_subscription_group(
        &self,
        broker_addr: CheetahString,
        group_name: CheetahString,
        remove_offset: Option<bool>,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Removes multiple subscription groups from one broker in a single request.
    async fn remove_subscription_groups(
        &self,
        broker_addr: CheetahString,
        group_names: Vec<CheetahString>,
        clean_offset: bool,
    ) -> rocketmq_error::RocketMQResult<()>;

    async fn configure_message_request_mode(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        mode: MessageRequestMode,
        pop_work_group_size: i32,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()>;

    async fn consume_directly(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        message_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumeMessageDirectlyResult>;

    async fn clone_consumer_group_offset(
        &self,
        source_group: CheetahString,
        destination_group: CheetahString,
        topic: CheetahString,
        offline: bool,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Returns the cluster topology needed to resolve mutation targets.
    async fn mutation_cluster_info(&self) -> rocketmq_error::RocketMQResult<ClusterInfo>;

    /// Returns route data used to scope a mutation to the topic's brokers.
    async fn mutation_topic_route(
        &self,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>>;

    /// Reads a broker's current topic configuration before a bounded test send.
    async fn mutation_topic_config(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfig>;

    /// Removes a topic from an explicitly resolved broker set.
    async fn remove_topic_from_brokers(
        &self,
        broker_addrs: HashSet<CheetahString>,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Removes multiple topics from one broker in a single request.
    async fn remove_topics_from_broker(
        &self,
        broker_addr: CheetahString,
        topics: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Removes a topic from an explicitly resolved NameServer set.
    async fn remove_topic_from_name_servers(
        &self,
        namesrv_addrs: HashSet<CheetahString>,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Returns the NameServer targets owned by this started admin session.
    async fn mutation_name_server_addresses(&self) -> rocketmq_error::RocketMQResult<Vec<CheetahString>>;

    /// Updates the order-topic configuration associated with a topic mutation.
    async fn upsert_order_topic_config(
        &self,
        topic: CheetahString,
        value: CheetahString,
        cluster_wide: bool,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Reads the exact NameServer-wide order-topic value used by a supervised mutation guard.
    async fn mutation_order_topic_config(
        &self,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<CheetahString>> {
        let _ = topic;
        Err(rocketmq_error::RocketMQError::illegal_argument(
            "order Topic mutation preflight is not implemented by this admin client",
        ))
    }

    /// Deletes the global order-topic entry after an unordered update or a
    /// complete Topic deletion.
    async fn delete_order_topic_config(&self, topic: CheetahString) -> rocketmq_error::RocketMQResult<()> {
        let _ = topic;
        Err(rocketmq_error::RocketMQError::illegal_argument(
            "order Topic configuration deletion is not implemented by this admin client",
        ))
    }

    /// Performs the broker-by-broker offset reset fallback used for offline consumers.
    async fn reset_consumer_offset_legacy(
        &self,
        cluster_name: Option<CheetahString>,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<Vec<RollbackStats>>;

    /// Views the message metadata required to validate a DLQ resend.
    async fn view_message_for_mutation(
        &self,
        topic: CheetahString,
        message_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MessageExt>;
}

impl MQAdminMutationExt for DefaultMQAdminExt {
    async fn begin_proxy_drain(
        &self,
        proxy_addr: CheetahString,
        operation_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProxyDrainStateResponseBody> {
        MQAdminMutationExt::begin_proxy_drain(self.inner(), proxy_addr, operation_id).await
    }

    async fn cancel_proxy_drain(
        &self,
        proxy_addr: CheetahString,
        operation_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProxyDrainStateResponseBody> {
        MQAdminMutationExt::cancel_proxy_drain(self.inner(), proxy_addr, operation_id).await
    }

    async fn broker_config_generation(&self, broker_addr: CheetahString) -> rocketmq_error::RocketMQResult<u64> {
        MQAdminMutationExt::broker_config_generation(self.inner(), broker_addr).await
    }

    async fn patch_broker_config_if_generation(
        &self,
        broker_addr: CheetahString,
        expected_generation: u64,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> rocketmq_error::RocketMQResult<BrokerConfigPatchOutcome> {
        MQAdminMutationExt::patch_broker_config_if_generation(
            self.inner(),
            broker_addr,
            expected_generation,
            properties,
        )
        .await
    }

    async fn preview_consumer_offset_reset_on_broker(
        &self,
        broker_addr: CheetahString,
        broker_name: CheetahString,
        read_queue_nums: u32,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: i64,
    ) -> rocketmq_error::RocketMQResult<Vec<MutationConsumerOffsetPreview>> {
        MQAdminMutationExt::preview_consumer_offset_reset_on_broker(
            self.inner(),
            broker_addr,
            broker_name,
            read_queue_nums,
            consumer_group,
            topic,
            timestamp,
        )
        .await
    }

    async fn mutation_consumer_offset(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        topic: CheetahString,
        queue_id: i32,
    ) -> rocketmq_error::RocketMQResult<i64> {
        MQAdminMutationExt::mutation_consumer_offset(self.inner(), broker_addr, consumer_group, topic, queue_id).await
    }

    async fn patch_topic_config_if_version(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        expected_version: u64,
        patch: TopicConfigPatch,
    ) -> rocketmq_error::RocketMQResult<TopicConfigPatchOutcome> {
        MQAdminMutationExt::patch_topic_config_if_version(self.inner(), broker_addr, topic, expected_version, patch)
            .await
    }

    async fn mutation_topic_config_with_version(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MutationTopicConfigVersioned> {
        MQAdminMutationExt::mutation_topic_config_with_version(self.inner(), broker_addr, topic).await
    }

    async fn mutation_topic_config_state(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MutationTopicConfigState> {
        MQAdminMutationExt::mutation_topic_config_state(self.inner(), broker_addr, topic).await
    }

    async fn replace_topic_config_if_state(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        expected_state: MutationExpectedState,
        replacement: MutationTopicConfig,
    ) -> rocketmq_error::RocketMQResult<MutationStateCasOutcome> {
        MQAdminMutationExt::replace_topic_config_if_state(self.inner(), broker_addr, topic, expected_state, replacement)
            .await
    }

    async fn patch_subscription_group_config_if_version(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
        expected_version: u64,
        patch: SubscriptionGroupConfigPatch,
    ) -> rocketmq_error::RocketMQResult<SubscriptionGroupConfigPatchOutcome> {
        MQAdminMutationExt::patch_subscription_group_config_if_version(
            self.inner(),
            broker_addr,
            group,
            expected_version,
            patch,
        )
        .await
    }

    async fn mutation_subscription_group_config_state(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MutationSubscriptionGroupConfigState> {
        MQAdminMutationExt::mutation_subscription_group_config_state(self.inner(), broker_addr, group).await
    }

    async fn replace_subscription_group_config_if_state(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
        expected_state: MutationExpectedState,
        replacement: MutationSubscriptionGroupConfig,
    ) -> rocketmq_error::RocketMQResult<MutationStateCasOutcome> {
        MQAdminMutationExt::replace_subscription_group_config_if_state(
            self.inner(),
            broker_addr,
            group,
            expected_state,
            replacement,
        )
        .await
    }

    async fn broker_mutation_config_state(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<BrokerMutationConfigState> {
        MQAdminMutationExt::broker_mutation_config_state(self.inner(), broker_addr).await
    }

    async fn reset_consumer_offset_if_current(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        topic: CheetahString,
        queue_id: i32,
        expected_offset: i64,
        new_offset: i64,
    ) -> rocketmq_error::RocketMQResult<ConditionalConsumerOffsetOutcome> {
        MQAdminMutationExt::reset_consumer_offset_if_current(
            self.inner(),
            broker_addr,
            consumer_group,
            topic,
            queue_id,
            expected_offset,
            new_offset,
        )
        .await
    }

    async fn mutation_message_request_mode(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<MutationMessageRequestMode>> {
        MQAdminMutationExt::mutation_message_request_mode(self.inner(), broker_addr, topic, consumer_group).await
    }

    async fn replace_message_request_mode_if_current(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        expected: MutationExpectedMessageRequestMode,
        replacement: MutationMessageRequestMode,
    ) -> rocketmq_error::RocketMQResult<MutationMessageRequestModeOutcome> {
        MQAdminMutationExt::replace_message_request_mode_if_current(
            self.inner(),
            broker_addr,
            topic,
            consumer_group,
            expected,
            replacement,
        )
        .await
    }

    async fn set_broker_log_filter_ttl(
        &self,
        broker_addr: CheetahString,
        logger: CheetahString,
        level: CheetahString,
        ttl_seconds: u32,
        operation_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::set_broker_log_filter_ttl(
            self.inner(),
            broker_addr,
            logger,
            level,
            ttl_seconds,
            operation_id,
        )
        .await
    }

    async fn restore_broker_log_filter(
        &self,
        broker_addr: CheetahString,
        operation_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::restore_broker_log_filter(self.inner(), broker_addr, operation_id).await
    }

    async fn upsert_topic_config(
        &self,
        broker_addr: CheetahString,
        config: TopicConfig,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::upsert_topic_config(self.inner(), broker_addr, config).await
    }

    async fn remove_topic(
        &self,
        topic_name: CheetahString,
        cluster_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::remove_topic(self.inner(), topic_name, cluster_name).await
    }

    async fn reset_consumer_offset(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        consumer_group: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<HashMap<MessageQueue, u64>> {
        MQAdminMutationExt::reset_consumer_offset(self.inner(), cluster_name, topic, consumer_group, timestamp, force)
            .await
    }

    async fn reset_consumer_offset_detailed(
        &self,
        cluster_name: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<TopicOffsetMutationOutcome> {
        MQAdminMutationExt::reset_consumer_offset_detailed(
            self.inner(),
            cluster_name,
            topic,
            consumer_group,
            timestamp,
            force,
        )
        .await
    }

    async fn skip_accumulated_message(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        consumer_group: CheetahString,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<usize> {
        MQAdminMutationExt::skip_accumulated_message(self.inner(), cluster_name, topic, consumer_group, force).await
    }

    async fn skip_accumulated_message_detailed(
        &self,
        cluster_name: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<TopicOffsetMutationOutcome> {
        MQAdminMutationExt::skip_accumulated_message_detailed(self.inner(), cluster_name, topic, consumer_group, force)
            .await
    }

    async fn upsert_subscription_group(
        &self,
        broker_addr: CheetahString,
        config: SubscriptionGroupConfig,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::upsert_subscription_group(self.inner(), broker_addr, config).await
    }

    async fn remove_subscription_group(
        &self,
        broker_addr: CheetahString,
        group_name: CheetahString,
        remove_offset: Option<bool>,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::remove_subscription_group(self.inner(), broker_addr, group_name, remove_offset).await
    }

    async fn remove_subscription_groups(
        &self,
        broker_addr: CheetahString,
        group_names: Vec<CheetahString>,
        clean_offset: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::remove_subscription_groups(self.inner(), broker_addr, group_names, clean_offset).await
    }

    async fn configure_message_request_mode(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        mode: MessageRequestMode,
        pop_work_group_size: i32,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::configure_message_request_mode(
            self.inner(),
            broker_addr,
            topic,
            consumer_group,
            mode,
            pop_work_group_size,
            timeout_millis,
        )
        .await
    }

    async fn consume_directly(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        message_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumeMessageDirectlyResult> {
        MQAdminMutationExt::consume_directly(self.inner(), consumer_group, client_id, topic, message_id).await
    }

    async fn clone_consumer_group_offset(
        &self,
        source_group: CheetahString,
        destination_group: CheetahString,
        topic: CheetahString,
        offline: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::clone_consumer_group_offset(self.inner(), source_group, destination_group, topic, offline)
            .await
    }

    async fn mutation_cluster_info(&self) -> rocketmq_error::RocketMQResult<ClusterInfo> {
        MQAdminMutationExt::mutation_cluster_info(self.inner()).await
    }

    async fn mutation_topic_route(
        &self,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        MQAdminMutationExt::mutation_topic_route(self.inner(), topic).await
    }

    async fn mutation_topic_config(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfig> {
        MQAdminMutationExt::mutation_topic_config(self.inner(), broker_addr, topic).await
    }

    async fn remove_topic_from_brokers(
        &self,
        broker_addrs: HashSet<CheetahString>,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::remove_topic_from_brokers(self.inner(), broker_addrs, topic).await
    }

    async fn remove_topics_from_broker(
        &self,
        broker_addr: CheetahString,
        topics: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::remove_topics_from_broker(self.inner(), broker_addr, topics).await
    }

    async fn remove_topic_from_name_servers(
        &self,
        namesrv_addrs: HashSet<CheetahString>,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::remove_topic_from_name_servers(self.inner(), namesrv_addrs, cluster_name, topic).await
    }

    async fn mutation_name_server_addresses(&self) -> rocketmq_error::RocketMQResult<Vec<CheetahString>> {
        MQAdminMutationExt::mutation_name_server_addresses(self.inner()).await
    }

    async fn upsert_order_topic_config(
        &self,
        topic: CheetahString,
        value: CheetahString,
        cluster_wide: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::upsert_order_topic_config(self.inner(), topic, value, cluster_wide).await
    }

    async fn mutation_order_topic_config(
        &self,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<CheetahString>> {
        MQAdminMutationExt::mutation_order_topic_config(self.inner(), topic).await
    }

    async fn delete_order_topic_config(&self, topic: CheetahString) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::delete_order_topic_config(self.inner(), topic).await
    }

    async fn reset_consumer_offset_legacy(
        &self,
        cluster_name: Option<CheetahString>,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<Vec<RollbackStats>> {
        MQAdminMutationExt::reset_consumer_offset_legacy(
            self.inner(),
            cluster_name,
            consumer_group,
            topic,
            timestamp,
            force,
        )
        .await
    }

    async fn view_message_for_mutation(
        &self,
        topic: CheetahString,
        message_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MessageExt> {
        MQAdminMutationExt::view_message_for_mutation(self.inner(), topic, message_id).await
    }
}
