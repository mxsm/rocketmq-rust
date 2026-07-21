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

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;

use arc_swap::ArcSwap;
use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_remoting::rpc::rpc_client_impl::RpcClientImpl;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::filter::ArcMessageFilter;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;

use crate::broker::broker_pre_online_capability::BrokerOnlineRoleState;
use crate::client::manager::consumer_manager::ConsumerManager;
use crate::coldctr::cold_data_cg_ctr_service::ColdDataCgCtrService;
use crate::coldctr::cold_data_pull_request_hold_service::ColdDataPullRequest;
use crate::coldctr::cold_data_pull_request_hold_service::ColdDataPullRequestHoldService;
use crate::failover::escape_bridge::EscapeBridge;
use crate::failover::escape_bridge::MessageStoreUnavailable;
use crate::filter::manager::consumer_filter_manager::ConsumerFilterManager;
use crate::long_polling::long_polling_service::pull_request_hold_service::PullRequestHoldService;
use crate::long_polling::pull_request::PullRequest;
use crate::offset::manager::broadcast_offset_manager::BroadcastOffsetCapability;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupConfigLookup;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;

/// Immutable request-path projection of the Broker and Store configuration used by pull.
#[derive(Clone, Debug)]
pub(crate) struct PullMessagePolicy {
    pub(crate) broker_name: CheetahString,
    pub(crate) broker_id: u64,
    pub(crate) broker_ip: CheetahString,
    pub(crate) broker_permission: u32,
    pub(crate) is_in_broker_container: bool,
    pub(crate) slave_read_enable: bool,
    pub(crate) forward_timeout: u64,
    pub(crate) filter_support_retry: bool,
    pub(crate) lite_pull_message_enable: bool,
    pub(crate) enable_property_filter: bool,
    pub(crate) use_server_side_reset_offset: bool,
    pub(crate) enable_broadcast_offset_store: bool,
    pub(crate) long_polling_enable: bool,
    pub(crate) short_polling_time_millis: u64,
    pub(crate) transfer_msg_by_heap: bool,
    pub(crate) commercial_base_count: i32,
    pub(crate) broker_role: BrokerRole,
    pub(crate) offset_check_in_slave: bool,
}

impl PullMessagePolicy {
    pub(crate) fn from_configs(broker_config: &BrokerConfig, store_config: &MessageStoreConfig) -> Self {
        let mut policy = Self {
            broker_name: CheetahString::new(),
            broker_id: 0,
            broker_ip: CheetahString::new(),
            broker_permission: 0,
            is_in_broker_container: false,
            slave_read_enable: false,
            forward_timeout: 0,
            filter_support_retry: false,
            lite_pull_message_enable: false,
            enable_property_filter: false,
            use_server_side_reset_offset: false,
            enable_broadcast_offset_store: false,
            long_polling_enable: false,
            short_polling_time_millis: 0,
            transfer_msg_by_heap: false,
            commercial_base_count: 0,
            broker_role: store_config.broker_role,
            offset_check_in_slave: store_config.offset_check_in_slave,
        };
        policy.apply_broker_config(broker_config);
        policy
    }

    fn apply_broker_config(&mut self, config: &BrokerConfig) {
        self.broker_name = config.broker_name().clone();
        self.broker_id = config.broker_identity.broker_id;
        self.broker_ip = config.broker_ip1.clone();
        self.broker_permission = config.broker_permission;
        self.is_in_broker_container = config.broker_identity.is_in_broker_container;
        self.slave_read_enable = config.slave_read_enable;
        self.forward_timeout = config.forward_timeout;
        self.filter_support_retry = config.filter_support_retry;
        self.lite_pull_message_enable = config.lite_pull_message_enable;
        self.enable_property_filter = config.enable_property_filter;
        self.use_server_side_reset_offset = config.use_server_side_reset_offset;
        self.enable_broadcast_offset_store = config.enable_broadcast_offset_store;
        self.long_polling_enable = config.long_polling_enable;
        self.short_polling_time_millis = config.short_polling_time_mills;
        self.transfer_msg_by_heap = config.transfer_msg_by_heap;
        self.commercial_base_count = config.commercial_base_count;
    }

    fn apply_store_config(&mut self, config: &MessageStoreConfig) {
        self.broker_role = config.broker_role;
        self.offset_check_in_slave = config.offset_check_in_slave;
    }
}

/// Atomically publishes complete pull-policy generations.
#[derive(Clone)]
pub(crate) struct PullMessagePolicyState {
    current: Arc<ArcSwap<PullMessagePolicy>>,
}

impl PullMessagePolicyState {
    pub(crate) fn from_configs(broker_config: &BrokerConfig, store_config: &MessageStoreConfig) -> Self {
        Self {
            current: Arc::new(ArcSwap::from_pointee(PullMessagePolicy::from_configs(
                broker_config,
                store_config,
            ))),
        }
    }

    pub(crate) fn snapshot(&self) -> Arc<PullMessagePolicy> {
        self.current.load_full()
    }

    pub(crate) fn update_broker_config(&self, broker_config: &BrokerConfig) {
        self.current.rcu(|current| {
            let mut replacement = current.as_ref().clone();
            replacement.apply_broker_config(broker_config);
            Arc::new(replacement)
        });
    }

    pub(crate) fn update_store_config(&self, store_config: &MessageStoreConfig) {
        self.current.rcu(|current| {
            let mut replacement = current.as_ref().clone();
            replacement.apply_store_config(store_config);
            Arc::new(replacement)
        });
    }
}

/// Non-owning access to the local Store operations used by pull paths.
pub(crate) struct PullMessageStoreCapability<MS: MessageStore> {
    provider: Weak<EscapeBridge<MS>>,
}

impl<MS: MessageStore> PullMessageStoreCapability<MS> {
    pub(crate) fn new(provider: &Arc<EscapeBridge<MS>>) -> Self {
        Self {
            provider: Arc::downgrade(provider),
        }
    }

    fn provider(&self) -> Result<Arc<EscapeBridge<MS>>, MessageStoreUnavailable> {
        self.provider.upgrade().ok_or(MessageStoreUnavailable)
    }

    pub(crate) fn min_offset(&self, topic: &CheetahString, queue_id: i32) -> Result<i64, MessageStoreUnavailable> {
        self.provider()?.get_min_offset_from_local_store(topic, queue_id)
    }

    pub(crate) fn max_offset(&self, topic: &CheetahString, queue_id: i32) -> Result<i64, MessageStoreUnavailable> {
        self.provider()?.get_max_offset_from_local_store(topic, queue_id)
    }

    pub(crate) fn now(&self) -> Result<u64, MessageStoreUnavailable> {
        self.provider()?.local_store_now()
    }

    pub(crate) async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_msg_bytes: i32,
        message_filter: ArcMessageFilter,
    ) -> Result<Option<GetMessageResult>, MessageStoreUnavailable> {
        self.provider()?
            .get_message_with_size_limit_from_local_store(
                group,
                topic,
                queue_id,
                offset,
                max_msg_nums,
                max_msg_bytes,
                message_filter,
            )
            .await
    }

    #[cfg(feature = "local_file_store")]
    pub(crate) fn is_message_in_cold_area(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        queue_offset: i64,
    ) -> Result<bool, MessageStoreUnavailable> {
        self.provider()?
            .is_message_in_cold_area(group, topic, queue_id, queue_offset)
    }
}

/// Complete dependency set shared by pull processing and result handling.
pub(crate) struct PullMessageProcessorContext<MS: MessageStore> {
    policy: PullMessagePolicyState,
    rpc_client: RpcClientImpl,
    consumer_manager: ConsumerManager,
    consumer_filter_manager: Arc<ConsumerFilterManager>,
    subscription_groups: SubscriptionGroupConfigLookup,
    topics: Arc<TopicConfigManager>,
    topic_mappings: Arc<TopicQueueMappingManager>,
    consumer_offsets: Weak<ConsumerOffsetManager<MS>>,
    broadcast_offsets: BroadcastOffsetCapability,
    broker_stats: Arc<BrokerStatsManager>,
    online_role_state: Arc<BrokerOnlineRoleState>,
    store: PullMessageStoreCapability<MS>,
    cold_data_flow: Option<Arc<ColdDataCgCtrService>>,
    cold_data_hold: Option<Arc<ColdDataPullRequestHoldService>>,
    pull_request_hold: Arc<OnceLock<Arc<PullRequestHoldService<MS>>>>,
}

impl<MS: MessageStore> PullMessageProcessorContext<MS> {
    #[allow(
        clippy::too_many_arguments,
        reason = "composition root enumerates the complete pull processor capability boundary"
    )]
    pub(crate) fn new(
        policy: PullMessagePolicyState,
        rpc_client: RpcClientImpl,
        consumer_manager: ConsumerManager,
        consumer_filter_manager: Arc<ConsumerFilterManager>,
        subscription_groups: SubscriptionGroupConfigLookup,
        topics: Arc<TopicConfigManager>,
        topic_mappings: Arc<TopicQueueMappingManager>,
        consumer_offsets: &Arc<ConsumerOffsetManager<MS>>,
        broadcast_offsets: BroadcastOffsetCapability,
        broker_stats: Arc<BrokerStatsManager>,
        online_role_state: Arc<BrokerOnlineRoleState>,
        store: PullMessageStoreCapability<MS>,
        cold_data_flow: Option<Arc<ColdDataCgCtrService>>,
        cold_data_hold: Option<Arc<ColdDataPullRequestHoldService>>,
    ) -> Self {
        Self {
            policy,
            rpc_client,
            consumer_manager,
            consumer_filter_manager,
            subscription_groups,
            topics,
            topic_mappings,
            consumer_offsets: Arc::downgrade(consumer_offsets),
            broadcast_offsets,
            broker_stats,
            online_role_state,
            store,
            cold_data_flow,
            cold_data_hold,
            pull_request_hold: Arc::new(OnceLock::new()),
        }
    }

    pub(crate) fn policy(&self) -> Arc<PullMessagePolicy> {
        self.policy.snapshot()
    }

    pub(crate) fn rpc_client(&self) -> &RpcClientImpl {
        &self.rpc_client
    }

    pub(crate) fn consumers(&self) -> &ConsumerManager {
        &self.consumer_manager
    }

    pub(crate) fn filters(&self) -> &Arc<ConsumerFilterManager> {
        &self.consumer_filter_manager
    }

    pub(crate) fn subscription_groups(&self) -> &SubscriptionGroupConfigLookup {
        &self.subscription_groups
    }

    pub(crate) fn topics(&self) -> &TopicConfigManager {
        &self.topics
    }

    pub(crate) fn topic_mappings(&self) -> &TopicQueueMappingManager {
        &self.topic_mappings
    }

    pub(crate) fn broker_stats(&self) -> &BrokerStatsManager {
        &self.broker_stats
    }

    pub(crate) fn min_broker_id(&self) -> u64 {
        self.online_role_state.min_broker_id()
    }

    pub(crate) fn store(&self) -> &PullMessageStoreCapability<MS> {
        &self.store
    }

    pub(crate) fn cold_data_flow(&self) -> Option<&ColdDataCgCtrService> {
        self.cold_data_flow.as_deref()
    }

    pub(crate) fn suspend_cold_data_pull(&self, request: ColdDataPullRequest) -> bool {
        let Some(service) = self.cold_data_hold.as_ref() else {
            return false;
        };
        service.suspend_cold_data_read_request(request);
        true
    }

    pub(crate) fn install_pull_request_hold_service(&self, service: Arc<PullRequestHoldService<MS>>) -> bool {
        self.pull_request_hold.set(service).is_ok()
    }

    pub(crate) fn suspend_pull_request(&self, topic: &str, queue_id: i32, request: PullRequest) -> bool {
        self.pull_request_hold
            .get()
            .is_some_and(|service| service.suspend_pull_request(topic, queue_id, request))
    }

    pub(crate) fn query_reset_offset(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
    ) -> Option<i64> {
        self.consumer_offsets
            .upgrade()
            .and_then(|manager| manager.query_then_erase_reset_offset(topic, group, queue_id))
    }

    pub(crate) fn commit_pull_offset(
        &self,
        client_address: std::net::SocketAddr,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
    ) {
        if let Some(manager) = self.consumer_offsets.upgrade() {
            manager.commit_pull_offset(client_address, group, topic, queue_id, offset);
        }
    }

    pub(crate) fn commit_offset(
        &self,
        client_address: CheetahString,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
    ) {
        if let Some(manager) = self.consumer_offsets.upgrade() {
            manager.commit_offset(client_address, group, topic, queue_id, offset);
        }
    }

    pub(crate) fn query_broadcast_offset(
        &self,
        topic: &str,
        group: &str,
        queue_id: i32,
        client_id: &str,
        request_offset: i64,
        from_proxy: bool,
    ) -> i64 {
        self.broadcast_offsets
            .query_init_offset(topic, group, queue_id, client_id, request_offset, from_proxy)
    }

    pub(crate) fn update_broadcast_offset(
        &self,
        topic: &str,
        group: &str,
        queue_id: i32,
        offset: i64,
        client_id: &str,
        from_proxy: bool,
    ) {
        self.broadcast_offsets
            .update_offset(topic, group, queue_id, offset, client_id, from_proxy);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Weak;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::broker::broker_role::BrokerRole;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use rocketmq_store::message_store::GenericMessageStore;

    use super::PullMessagePolicyState;
    use super::PullMessageStoreCapability;

    #[test]
    fn pull_policy_state_publishes_complete_updated_generations() {
        let mut broker_config = BrokerConfig::default();
        let mut store_config = MessageStoreConfig::default();
        let state = PullMessagePolicyState::from_configs(&broker_config, &store_config);

        broker_config.enable_property_filter = true;
        broker_config.broker_identity.broker_id = 7;
        state.update_broker_config(&broker_config);
        store_config.broker_role = BrokerRole::Slave;
        store_config.offset_check_in_slave = true;
        state.update_store_config(&store_config);

        let policy = state.snapshot();
        assert!(policy.enable_property_filter);
        assert_eq!(policy.broker_id, 7);
        assert_eq!(policy.broker_role, BrokerRole::Slave);
        assert!(policy.offset_check_in_slave);
    }

    #[test]
    fn capability_source_has_no_complete_runtime_or_shared_mutation_owner() {
        let source = include_str!("capability.rs");

        assert!(!source.contains(concat!("Broker", "RuntimeInner")));
        assert!(!source.contains(concat!("Arc", "Mut")));
        assert!(!source.contains(concat!("Weak", "Arc", "Mut")));
    }

    #[test]
    fn pull_store_capability_fails_closed_without_provider() {
        let capability = PullMessageStoreCapability::<GenericMessageStore> { provider: Weak::new() };

        assert!(capability
            .max_offset(&CheetahString::from_static_str("topic"), 0)
            .is_err());
        assert!(capability.now().is_err());
    }
}
