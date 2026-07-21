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

use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Instant;

use arc_swap::ArcSwap;
use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigSerializeWrapper;
use rocketmq_remoting::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use rocketmq_store::store_api_adapter::LegacyAppendReceipt;
use rocketmq_store::store_api_adapter::LegacyStoreHealthSnapshot;
use rocketmq_store_api::MessageAppender;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreErrorKind;
use rocketmq_store_api::StoreOperation;
use tracing::debug;
use tracing::info;

use crate::broker_runtime::complete_topic_config_creation;
use crate::client::manager::producer_manager::ProducerReplyChannelRegistry;
use crate::client::rebalance::rebalance_lock_manager::RebalanceLockManager;
use crate::failover::escape_bridge::EscapeBridge;
use crate::failover::escape_bridge::MessageStoreUnavailable;
use crate::out_api::broker_outer_api::BrokerOuterAPI;
use crate::slave::slave_synchronize::SlaveMasterAddress;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupConfigLookup;
use crate::topic::manager::topic_config_coordinator::TopicConfigCoordinator;
use crate::topic::manager::topic_config_manager::TopicConfigCreation;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::topic::manager::topic_config_manager::TopicConfigUpdate;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;
use crate::transaction::queue::transaction_message_store::TransactionMessageStore;

/// Request-path configuration projected from the Broker and Store configurations.
///
/// Keeping the projection explicit prevents request processors from becoming implicit
/// configuration or lifecycle owners. A generation is immutable after publication.
#[derive(Clone, Debug)]
pub(crate) struct SendMessagePolicy {
    pub(crate) broker_name: CheetahString,
    pub(crate) broker_cluster_name: CheetahString,
    pub(crate) broker_id: u64,
    pub(crate) broker_ip: CheetahString,
    pub(crate) broker_addr: CheetahString,
    pub(crate) store_host: SocketAddr,
    pub(crate) broker_permission: u32,
    pub(crate) is_in_broker_container: bool,
    pub(crate) region_id: CheetahString,
    pub(crate) trace_on: bool,
    pub(crate) start_accept_send_request_time_stamp: i64,
    pub(crate) enable_slave_acting_master: bool,
    pub(crate) reject_transaction_message: bool,
    pub(crate) store_reply_message_enable: bool,
    pub(crate) commercial_size_per_msg: i32,
    pub(crate) commercial_base_count: i32,
    pub(crate) sync_flush_backlog_reject_depth: u64,
    pub(crate) sync_flush_backlog_reject_wait_millis: u64,
    pub(crate) ha_pending_reject_count: u64,
    pub(crate) ha_pending_reject_wait_millis: u64,
    pub(crate) reput_lag_reject_bytes: i64,
    pub(crate) broker_role: BrokerRole,
    pub(crate) max_message_size: i32,
    pub(crate) timer_max_delay_sec: u64,
    pub(crate) timer_precision_ms: u64,
    pub(crate) auto_create_topic_enable: bool,
    pub(crate) async_topic_create_persist_enable: bool,
    pub(crate) enable_single_topic_register: bool,
    pub(crate) register_broker_timeout_millis: u64,
    pub(crate) compressed_register: bool,
    pub(crate) broker_not_active_timeout_millis: i64,
}

impl SendMessagePolicy {
    pub(crate) fn from_configs(
        broker_config: &BrokerConfig,
        message_store_config: &MessageStoreConfig,
        store_host: SocketAddr,
    ) -> Self {
        let mut policy = Self {
            broker_name: CheetahString::new(),
            broker_cluster_name: CheetahString::new(),
            broker_id: 0,
            broker_ip: CheetahString::new(),
            broker_addr: CheetahString::new(),
            store_host,
            broker_permission: 0,
            is_in_broker_container: false,
            region_id: CheetahString::new(),
            trace_on: false,
            start_accept_send_request_time_stamp: 0,
            enable_slave_acting_master: false,
            reject_transaction_message: false,
            store_reply_message_enable: false,
            commercial_size_per_msg: 0,
            commercial_base_count: 0,
            sync_flush_backlog_reject_depth: 0,
            sync_flush_backlog_reject_wait_millis: 0,
            ha_pending_reject_count: 0,
            ha_pending_reject_wait_millis: 0,
            reput_lag_reject_bytes: 0,
            broker_role: message_store_config.broker_role,
            max_message_size: message_store_config.max_message_size,
            timer_max_delay_sec: message_store_config.timer_max_delay_sec,
            timer_precision_ms: message_store_config.timer_precision_ms,
            auto_create_topic_enable: false,
            async_topic_create_persist_enable: false,
            enable_single_topic_register: false,
            register_broker_timeout_millis: 0,
            compressed_register: false,
            broker_not_active_timeout_millis: 0,
        };
        policy.apply_broker_config(broker_config);
        policy
    }

    fn apply_broker_config(&mut self, config: &BrokerConfig) {
        self.broker_name = config.broker_identity.broker_name.clone();
        self.broker_cluster_name = config.broker_identity.broker_cluster_name.clone();
        self.broker_id = config.broker_identity.broker_id;
        self.broker_ip = config.broker_ip1.clone();
        self.broker_addr = CheetahString::from_string(config.get_broker_addr());
        self.broker_permission = config.broker_permission;
        self.is_in_broker_container = config.broker_identity.is_in_broker_container;
        self.region_id = config.region_id.clone();
        self.trace_on = config.trace_on;
        self.start_accept_send_request_time_stamp = config.start_accept_send_request_time_stamp;
        self.enable_slave_acting_master = config.enable_slave_acting_master;
        self.reject_transaction_message = config.reject_transaction_message;
        self.store_reply_message_enable = config.store_reply_message_enable;
        self.commercial_size_per_msg = config.commercial_size_per_msg;
        self.commercial_base_count = config.commercial_base_count;
        self.sync_flush_backlog_reject_depth = config.sync_flush_backlog_reject_depth;
        self.sync_flush_backlog_reject_wait_millis = config.sync_flush_backlog_reject_wait_millis;
        self.ha_pending_reject_count = config.ha_pending_reject_count;
        self.ha_pending_reject_wait_millis = config.ha_pending_reject_wait_millis;
        self.reput_lag_reject_bytes = config.reput_lag_reject_bytes;
        self.auto_create_topic_enable = config.auto_create_topic_enable;
        self.async_topic_create_persist_enable = config.async_topic_create_persist_enable;
        self.enable_single_topic_register = config.enable_single_topic_register;
        self.register_broker_timeout_millis = config.register_broker_timeout_mills.max(0) as u64;
        self.compressed_register = config.compressed_register;
        self.broker_not_active_timeout_millis = config.broker_not_active_timeout_millis;
    }

    fn apply_message_store_config(&mut self, config: &MessageStoreConfig) {
        self.broker_role = config.broker_role;
        self.max_message_size = config.max_message_size;
        self.timer_max_delay_sec = config.timer_max_delay_sec;
        self.timer_precision_ms = config.timer_precision_ms;
    }
}

/// Atomically published send/reply policy generations.
#[derive(Clone)]
pub(crate) struct SendMessagePolicyState {
    current: Arc<ArcSwap<SendMessagePolicy>>,
}

impl SendMessagePolicyState {
    pub(crate) fn from_configs(
        broker_config: &BrokerConfig,
        message_store_config: &MessageStoreConfig,
        store_host: SocketAddr,
    ) -> Self {
        Self {
            current: Arc::new(ArcSwap::from_pointee(SendMessagePolicy::from_configs(
                broker_config,
                message_store_config,
                store_host,
            ))),
        }
    }

    #[inline]
    pub(crate) fn snapshot(&self) -> Arc<SendMessagePolicy> {
        self.current.load_full()
    }

    pub(crate) fn update_broker_config(&self, broker_config: &BrokerConfig) {
        self.current.rcu(|current| {
            let mut replacement = current.as_ref().clone();
            replacement.apply_broker_config(broker_config);
            Arc::new(replacement)
        });
    }

    pub(crate) fn update_message_store_config(&self, message_store_config: &MessageStoreConfig) {
        self.current.rcu(|current| {
            let mut replacement = current.as_ref().clone();
            replacement.apply_message_store_config(message_store_config);
            Arc::new(replacement)
        });
    }

    pub(crate) fn update_store_host(&self, store_host: SocketAddr) {
        self.current.rcu(|current| {
            let mut replacement = current.as_ref().clone();
            replacement.store_host = store_host;
            Arc::new(replacement)
        });
    }
}

/// Non-owning access to the local Store operations used by send and reply paths.
pub(crate) struct SendMessageStoreCapability<MS: MessageStore> {
    provider: Weak<EscapeBridge<MS>>,
}

impl<MS: MessageStore> Clone for SendMessageStoreCapability<MS> {
    fn clone(&self) -> Self {
        Self {
            provider: Weak::clone(&self.provider),
        }
    }
}

impl<MS: MessageStore> SendMessageStoreCapability<MS> {
    pub(crate) fn new(provider: &Arc<EscapeBridge<MS>>) -> Self {
        Self {
            provider: Arc::downgrade(provider),
        }
    }

    fn provider(&self) -> Result<Arc<EscapeBridge<MS>>, MessageStoreUnavailable> {
        self.provider.upgrade().ok_or(MessageStoreUnavailable)
    }

    pub(crate) fn health_snapshot(&self) -> Result<LegacyStoreHealthSnapshot, MessageStoreUnavailable> {
        self.provider()?.send_message_store_health_snapshot()
    }

    pub(crate) fn now(&self) -> Result<u64, MessageStoreUnavailable> {
        self.provider()?.local_store_now()
    }

    pub(crate) fn append_progress(&self) -> Result<(i64, i64), MessageStoreUnavailable> {
        self.provider()?.send_append_progress()
    }

    pub(crate) fn look_message_by_offset(&self, offset: i64) -> Result<Option<MessageExt>, MessageStoreUnavailable> {
        self.provider()?.look_message_by_offset_from_local_store(offset)
    }

    pub(crate) async fn put_message(
        &self,
        message: MessageExtBrokerInner,
    ) -> Result<PutMessageResult, MessageStoreUnavailable> {
        self.provider()?.put_message_to_local_store(message).await
    }
}

fn append_store_unavailable() -> StoreError {
    StoreError::new(StoreErrorKind::NotStarted, StoreOperation::Append)
}

impl<MS: MessageStore> MessageAppender<MessageExtBrokerInner> for SendMessageStoreCapability<MS> {
    type Receipt = LegacyAppendReceipt;
    type Error = StoreError;

    async fn append_message(&mut self, message: MessageExtBrokerInner) -> Result<Self::Receipt, Self::Error> {
        self.provider()
            .map_err(|_| append_store_unavailable())?
            .send_append_message(message)
            .await
    }
}

impl<MS: MessageStore> MessageAppender<MessageExtBatch> for SendMessageStoreCapability<MS> {
    type Receipt = LegacyAppendReceipt;
    type Error = StoreError;

    async fn append_message(&mut self, message: MessageExtBatch) -> Result<Self::Receipt, Self::Error> {
        self.provider()
            .map_err(|_| append_store_unavailable())?
            .send_append_batch(message)
            .await
    }
}

/// Topic lookup, creation, persistence, and registration boundary for send paths.
pub(crate) struct SendMessageTopicCapability<MS: MessageStore> {
    policy: SendMessagePolicyState,
    topic_config_manager: Arc<TopicConfigManager>,
    topic_config_coordinator: Arc<TopicConfigCoordinator>,
    topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    broker_outer_api: BrokerOuterAPI,
    message_store: TransactionMessageStore<MS>,
    slave_master_addr: Option<Arc<SlaveMasterAddress>>,
    update_master_haserver_addr_periodically: bool,
    shutdown: Arc<AtomicBool>,
}

impl<MS: MessageStore> Clone for SendMessageTopicCapability<MS> {
    fn clone(&self) -> Self {
        Self {
            policy: self.policy.clone(),
            topic_config_manager: Arc::clone(&self.topic_config_manager),
            topic_config_coordinator: Arc::clone(&self.topic_config_coordinator),
            topic_queue_mapping_manager: Arc::clone(&self.topic_queue_mapping_manager),
            broker_outer_api: self.broker_outer_api.clone(),
            message_store: self.message_store.clone(),
            slave_master_addr: self.slave_master_addr.as_ref().map(Arc::clone),
            update_master_haserver_addr_periodically: self.update_master_haserver_addr_periodically,
            shutdown: Arc::clone(&self.shutdown),
        }
    }
}

impl<MS> SendMessageTopicCapability<MS>
where
    MS: MessageStore + Send + Sync + 'static,
{
    #[allow(
        clippy::too_many_arguments,
        reason = "constructor enumerates the complete topic creation and registration boundary"
    )]
    pub(crate) fn new(
        policy: SendMessagePolicyState,
        topic_config_manager: Arc<TopicConfigManager>,
        topic_config_coordinator: Arc<TopicConfigCoordinator>,
        topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
        broker_outer_api: BrokerOuterAPI,
        message_store: TransactionMessageStore<MS>,
        slave_master_addr: Option<Arc<SlaveMasterAddress>>,
        update_master_haserver_addr_periodically: bool,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            policy,
            topic_config_manager,
            topic_config_coordinator,
            topic_queue_mapping_manager,
            broker_outer_api,
            message_store,
            slave_master_addr,
            update_master_haserver_addr_periodically,
            shutdown,
        }
    }

    pub(crate) fn select_topic_config(&self, topic: &CheetahString) -> Option<Arc<TopicConfig>> {
        self.topic_config_manager.select_topic_config(topic)
    }

    pub(crate) fn is_order_topic(&self, topic: &str) -> bool {
        self.topic_config_manager.is_order_topic(topic)
    }

    pub(crate) fn build_topic_queue_mapping_context(
        &self,
        request_header: &impl TopicRequestHeaderTrait,
        select_one_when_miss: bool,
    ) -> TopicQueueMappingContext {
        self.topic_queue_mapping_manager
            .build_topic_queue_mapping_context(request_header, select_one_when_miss)
    }

    pub(crate) async fn create_topic_in_send_message(
        &self,
        topic: &CheetahString,
        default_topic: &CheetahString,
        remote_address: SocketAddr,
        queue_nums: i32,
        topic_sys_flag: u32,
    ) -> Option<Arc<TopicConfig>> {
        let start_time = Instant::now();
        let policy = self.policy.snapshot();
        let state_machine_version = self.message_store.state_machine_version()?;
        let creation = self.topic_config_manager.create_topic_in_send_message_method(
            topic,
            default_topic,
            remote_address,
            queue_nums,
            topic_sys_flag,
            state_machine_version,
            policy.auto_create_topic_enable,
        )?;
        Some(self.complete_creation(creation, start_time, policy).await)
    }

    pub(crate) async fn create_topic_in_send_message_back(
        &self,
        topic: &CheetahString,
        queue_nums: i32,
        perm: u32,
        is_order: bool,
        topic_sys_flag: u32,
    ) -> Option<Arc<TopicConfig>> {
        let start_time = Instant::now();
        let policy = self.policy.snapshot();
        let state_machine_version = self.message_store.state_machine_version()?;
        let creation = self.topic_config_manager.create_topic_in_send_message_back_method(
            topic,
            queue_nums,
            perm,
            is_order,
            topic_sys_flag,
            state_machine_version,
        )?;
        Some(self.complete_creation(creation, start_time, policy).await)
    }

    async fn complete_creation(
        &self,
        creation: TopicConfigCreation,
        start_time: Instant,
        policy: Arc<SendMessagePolicy>,
    ) -> Arc<TopicConfig> {
        let registration = self.clone();
        complete_topic_config_creation(
            Arc::clone(&self.topic_config_coordinator),
            creation,
            start_time,
            policy.async_topic_create_persist_enable,
            move |update| {
                let registration = registration.clone();
                async move { registration.register_update(update).await }
            },
        )
        .await
    }

    async fn register_update(&self, update: TopicConfigUpdate) {
        let policy = self.policy.snapshot();
        if policy.enable_single_topic_register {
            self.register_single_topic(update.topic_config, &policy).await;
        } else {
            self.register_incremental_topic(update, &policy).await;
        }
    }

    fn topic_config_for_registration(policy: &SendMessagePolicy, topic_config: &TopicConfig) -> TopicConfig {
        let mut topic_config = topic_config.clone();
        if !PermName::is_writeable(policy.broker_permission) || !PermName::is_readable(policy.broker_permission) {
            topic_config.perm &= policy.broker_permission;
        }
        topic_config
    }

    async fn register_single_topic(&self, topic_config: Arc<TopicConfig>, policy: &SendMessagePolicy) {
        let Some(topic_name) = topic_config.topic_name.clone() else {
            return;
        };
        let (mut current_configs, _) = self
            .topic_config_manager
            .topic_registration_snapshot(std::slice::from_ref(&topic_name));
        let Some(current) = current_configs.pop() else {
            info!(topic = %topic_name, "Skip stale send topic registration after topic removal");
            return;
        };
        self.broker_outer_api
            .register_single_topic_all(
                policy.broker_name.clone(),
                Self::topic_config_for_registration(policy, current.as_ref()),
                3000,
            )
            .await;
    }

    async fn register_incremental_topic(&self, update: TopicConfigUpdate, policy: &SendMessagePolicy) {
        if self.shutdown.load(Ordering::Acquire) {
            info!("Skip send topic registration after broker shutdown");
            return;
        }
        let Some(topic_name) = update.topic_config.topic_name.clone() else {
            return;
        };
        let (topic_config_list, current_data_version) = self
            .topic_config_manager
            .topic_registration_snapshot(std::slice::from_ref(&topic_name));
        if current_data_version != update.data_version {
            debug!(
                requested = ?update.data_version,
                current = ?current_data_version,
                "Resample send topic registration after a newer metadata commit"
            );
        }

        let mut topic_config_table = std::collections::HashMap::new();
        let mut topic_queue_mapping_info_map = std::collections::HashMap::new();
        for topic_config in &topic_config_list {
            let register_topic_config = Self::topic_config_for_registration(policy, topic_config.as_ref());
            if let Some(topic_name) = register_topic_config.topic_name.clone() {
                topic_config_table.insert(topic_name.clone(), register_topic_config);
                if let Some(mapping) = self
                    .topic_queue_mapping_manager
                    .get_topic_queue_mapping(topic_name.as_str())
                {
                    topic_queue_mapping_info_map.insert(
                        topic_name,
                        TopicQueueMappingDetail::clone_as_mapping_info(mapping.as_ref()),
                    );
                }
            }
        }
        let wrapper = TopicConfigAndMappingSerializeWrapper {
            topic_config_serialize_wrapper: TopicConfigSerializeWrapper {
                data_version: current_data_version,
                topic_config_table,
            },
            topic_queue_mapping_info_map,
            ..Default::default()
        };
        let results = self
            .broker_outer_api
            .register_broker_all(
                policy.broker_cluster_name.clone(),
                policy.broker_addr.clone(),
                policy.broker_name.clone(),
                policy.broker_id,
                policy.broker_addr.clone(),
                wrapper,
                vec![],
                false,
                policy.register_broker_timeout_millis,
                policy.enable_slave_acting_master,
                policy.compressed_register,
                policy
                    .enable_slave_acting_master
                    .then_some(policy.broker_not_active_timeout_millis),
                Default::default(),
            )
            .await;
        self.handle_register_result(results).await;
    }

    async fn handle_register_result(
        &self,
        register_broker_result: Vec<rocketmq_remoting::protocol::namesrv::RegisterBrokerResult>,
    ) {
        let Some(result) = register_broker_result.into_iter().next() else {
            return;
        };
        if self.update_master_haserver_addr_periodically {
            self.message_store.update_master_address(&result.master_addr).await;
        }
        if let Some(master_addr) = &self.slave_master_addr {
            master_addr.store(Some(&result.master_addr));
        }
        if let Some(state_machine_version) = self.message_store.state_machine_version() {
            self.topic_config_manager
                .update_order_topic_config(&result.kv_table, state_machine_version);
        }
    }
}

/// Complete dependency set shared by send and reply request processors.
pub(crate) struct SendMessageProcessorContext<MS: MessageStore> {
    pub(crate) policy: SendMessagePolicyState,
    pub(crate) store: SendMessageStoreCapability<MS>,
    pub(crate) topics: Arc<SendMessageTopicCapability<MS>>,
    pub(crate) subscription_groups: SubscriptionGroupConfigLookup,
    pub(crate) rebalance_locks: RebalanceLockManager,
    pub(crate) broker_stats_manager: Arc<BrokerStatsManager>,
    pub(crate) producer_reply_channels: ProducerReplyChannelRegistry,
}

impl<MS: MessageStore> Clone for SendMessageProcessorContext<MS> {
    fn clone(&self) -> Self {
        Self {
            policy: self.policy.clone(),
            store: self.store.clone(),
            topics: Arc::clone(&self.topics),
            subscription_groups: self.subscription_groups.clone(),
            rebalance_locks: self.rebalance_locks.clone(),
            broker_stats_manager: Arc::clone(&self.broker_stats_manager),
            producer_reply_channels: self.producer_reply_channels.clone(),
        }
    }
}

impl<MS: MessageStore> SendMessageProcessorContext<MS> {
    #[allow(
        clippy::too_many_arguments,
        reason = "constructor enumerates the complete send/reply processor capability boundary"
    )]
    pub(crate) fn new(
        policy: SendMessagePolicyState,
        store: SendMessageStoreCapability<MS>,
        topics: Arc<SendMessageTopicCapability<MS>>,
        subscription_groups: SubscriptionGroupConfigLookup,
        rebalance_locks: RebalanceLockManager,
        broker_stats_manager: Arc<BrokerStatsManager>,
        producer_reply_channels: ProducerReplyChannelRegistry,
    ) -> Self {
        Self {
            policy,
            store,
            topics,
            subscription_groups,
            rebalance_locks,
            broker_stats_manager,
            producer_reply_channels,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::net::SocketAddr;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::broker::broker_role::BrokerRole;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    use super::SendMessagePolicyState;

    #[test]
    fn policy_state_publishes_complete_updated_generations() {
        let mut broker_config = BrokerConfig::default();
        let mut store_config = MessageStoreConfig::default();
        let initial_store_host = SocketAddr::from((Ipv4Addr::LOCALHOST, 10911));
        let state = SendMessagePolicyState::from_configs(&broker_config, &store_config, initial_store_host);

        broker_config.region_id = CheetahString::from_static_str("updated-region");
        broker_config.broker_identity.broker_id = 7;
        state.update_broker_config(&broker_config);
        store_config.broker_role = BrokerRole::Slave;
        store_config.max_message_size = 1024;
        state.update_message_store_config(&store_config);
        let updated_store_host = SocketAddr::from((Ipv4Addr::LOCALHOST, 20911));
        state.update_store_host(updated_store_host);

        let snapshot = state.snapshot();
        assert_eq!(snapshot.region_id, "updated-region");
        assert_eq!(snapshot.broker_id, 7);
        assert_eq!(snapshot.broker_role, BrokerRole::Slave);
        assert_eq!(snapshot.max_message_size, 1024);
        assert_eq!(snapshot.store_host, updated_store_host);
    }

    #[test]
    fn capability_source_has_no_complete_runtime_or_arc_mut_owner() {
        let source = include_str!("capability.rs");

        assert!(!source.contains(concat!("Broker", "RuntimeInner")));
        assert!(!source.contains(concat!("Arc", "Mut")));
        assert!(!source.contains(concat!("Weak", "Arc", "Mut")));
    }
}
