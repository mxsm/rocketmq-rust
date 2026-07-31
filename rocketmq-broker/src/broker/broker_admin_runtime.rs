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

use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::Weak;

use crate::config::broker_config::BrokerConfig;
use crate::config::error::BrokerConfigError;
use crate::config::transaction::ConfigUpdateTransaction;
use crate::config::validated::ConfigGeneration;
use cheetah_string::CheetahString;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_protocol::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_protocol::protocol::DataVersion;
use rocketmq_store::BrokerStats;
use rocketmq_store::BrokerStatsManager;
use rocketmq_store::MessageStore;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::PutMessageResult;
use rocketmq_store::StoreError;
use rocketmq_store::StoreErrorKind;
use rocketmq_store::StoreOperation;
use rocketmq_store::TimerMessageStore;
use rocketmq_store::MADV_NORMAL;
use rocketmq_transport::ServerConfig;
use tracing::warn;

use crate::broker::broker_control_plane::BrokerControllerRuntime;
use crate::broker::broker_control_plane::BrokerMembershipState;
use crate::broker::broker_pre_online_capability::BrokerOnlineRoleState;
use crate::broker::broker_pre_online_capability::BrokerSpecialServiceCapability;
use crate::broker::broker_registration_runtime::BrokerRegistrationRuntime;
use crate::broker::broker_runtime_config_state::BrokerRuntimeConfigGeneration;
use crate::broker::broker_runtime_config_state::BrokerRuntimeConfigState;
use crate::broker::log_filter_control::BrokerLogFilterControl;
use crate::client::manager::consumer_manager::ConsumerManager;
use crate::client::manager::producer_manager::ProducerManager;
use crate::client::rebalance::rebalance_lock_manager::RebalanceLockManager;
use crate::coldctr::cold_data_cg_ctr_service::ColdDataCgCtrService;
use crate::controller::replicas_manager::ReplicasManager;
use crate::failover::escape_bridge::EscapeBridge;
use crate::failover::escape_bridge::MessageStoreUnavailable;
use crate::failover::escape_bridge_capability::EscapeBridgePolicyState;
use crate::failover::escape_bridge_capability::EscapeStoreReadLease;
use crate::filter::manager::consumer_filter_manager::ConsumerFilterManager;
use crate::long_polling::long_polling_service::pull_request_hold_service::PullRequestHoldService;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::out_api::broker_outer_api::BrokerOuterAPI;
use crate::processor::pop_inflight_message_counter::PopInflightMessageCounter;
use crate::processor::pop_message_processor::capability::PopPolicyState;
use crate::processor::pop_message_processor::PopMessageProcessor;
use crate::processor::pull_message_processor::capability::PullMessagePolicyState;
use crate::processor::query_assignment_processor::QueryAssignmentProcessor;
use crate::processor::send_message_processor::capability::SendMessagePolicyState;
use crate::schedule::schedule_message_service::ScheduleMessageService;
use crate::slave::slave_synchronize::SlaveSynchronize;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupManager;
use crate::topic::manager::topic_config_coordinator::TopicConfigCoordinator;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CommitLogReadModeUpdateError {
    #[error(transparent)]
    Store(#[from] StoreError),

    #[error(transparent)]
    Config(#[from] BrokerConfigError),
}

/// Explicit capability carrier for the serialized Admin request surface.
///
/// The carrier shares only independently synchronized managers and narrow
/// control-plane capabilities. It never owns or dereferences the complete
/// broker runtime composition root.
pub(crate) struct BrokerAdminRuntime<MS: MessageStore> {
    config: BrokerRuntimeConfigState,
    store_host: SocketAddr,
    broker_addr: CheetahString,
    message_store_provider: Weak<EscapeBridge<MS>>,
    topic_config_manager: Arc<TopicConfigManager>,
    topic_config_coordinator: Arc<TopicConfigCoordinator>,
    topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    consumer_offset_manager: Arc<ConsumerOffsetManager<MS>>,
    subscription_group_manager: SubscriptionGroupManager,
    consumer_filter_manager: ConsumerFilterManager,
    broker_stats: Option<Arc<BrokerStats<MS>>>,
    schedule_message_service: Arc<ScheduleMessageService<MS>>,
    timer_message_store: Option<Arc<TimerMessageStore>>,
    broker_outer_api: BrokerOuterAPI,
    registration: BrokerRegistrationRuntime<MS>,
    producer_manager: ProducerManager,
    consumer_manager: ConsumerManager,
    broker_stats_manager: Arc<BrokerStatsManager>,
    role_state: Arc<BrokerOnlineRoleState>,
    pull_request_hold_service: Option<Arc<PullRequestHoldService<MS>>>,
    rebalance_lock_manager: RebalanceLockManager,
    membership: BrokerMembershipState,
    controller: Arc<BrokerControllerRuntime<MS>>,
    special_services: BrokerSpecialServiceCapability<MS>,
    pop_message_processor: Option<Arc<PopMessageProcessor<MS>>>,
    pop_inflight_message_counter: PopInflightMessageCounter,
    query_assignment_processor: Option<Arc<QueryAssignmentProcessor>>,
    slave_synchronize: Option<Arc<SlaveSynchronize<MS>>>,
    cold_data_cg_ctr_service: Option<Arc<ColdDataCgCtrService>>,
    shutdown: Arc<AtomicBool>,
    send_policy: SendMessagePolicyState,
    pull_policy: PullMessagePolicyState,
    pop_policy: PopPolicyState,
    escape_policy: EscapeBridgePolicyState,
    log_filter_control: Option<Arc<BrokerLogFilterControl>>,
    config_update_lock: Arc<parking_lot::Mutex<()>>,
}

impl<MS: MessageStore> Clone for BrokerAdminRuntime<MS> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            store_host: self.store_host,
            broker_addr: self.broker_addr.clone(),
            message_store_provider: self.message_store_provider.clone(),
            topic_config_manager: Arc::clone(&self.topic_config_manager),
            topic_config_coordinator: Arc::clone(&self.topic_config_coordinator),
            topic_queue_mapping_manager: Arc::clone(&self.topic_queue_mapping_manager),
            consumer_offset_manager: Arc::clone(&self.consumer_offset_manager),
            subscription_group_manager: self.subscription_group_manager.clone(),
            consumer_filter_manager: self.consumer_filter_manager.clone(),
            broker_stats: self.broker_stats.clone(),
            schedule_message_service: Arc::clone(&self.schedule_message_service),
            timer_message_store: self.timer_message_store.clone(),
            broker_outer_api: self.broker_outer_api.clone(),
            registration: self.registration.clone(),
            producer_manager: self.producer_manager.clone_shared_state(),
            consumer_manager: self.consumer_manager.clone_shared_state(),
            broker_stats_manager: Arc::clone(&self.broker_stats_manager),
            role_state: Arc::clone(&self.role_state),
            pull_request_hold_service: self.pull_request_hold_service.clone(),
            rebalance_lock_manager: self.rebalance_lock_manager.clone(),
            membership: self.membership.clone(),
            controller: Arc::clone(&self.controller),
            special_services: self.special_services.clone(),
            pop_message_processor: self.pop_message_processor.clone(),
            pop_inflight_message_counter: self.pop_inflight_message_counter.clone(),
            query_assignment_processor: self.query_assignment_processor.clone(),
            slave_synchronize: self.slave_synchronize.clone(),
            cold_data_cg_ctr_service: self.cold_data_cg_ctr_service.clone(),
            shutdown: Arc::clone(&self.shutdown),
            send_policy: self.send_policy.clone(),
            pull_policy: self.pull_policy.clone(),
            pop_policy: self.pop_policy.clone(),
            escape_policy: self.escape_policy.clone(),
            log_filter_control: self.log_filter_control.clone(),
            config_update_lock: Arc::clone(&self.config_update_lock),
        }
    }
}

impl<MS: MessageStore> BrokerAdminRuntime<MS> {
    #[allow(
        clippy::too_many_arguments,
        reason = "the broker composition root enumerates the complete Admin boundary"
    )]
    pub(crate) fn new(
        config: BrokerRuntimeConfigState,
        store_host: SocketAddr,
        broker_addr: CheetahString,
        message_store_provider: Weak<EscapeBridge<MS>>,
        topic_config_manager: Arc<TopicConfigManager>,
        topic_config_coordinator: Arc<TopicConfigCoordinator>,
        topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
        consumer_offset_manager: Arc<ConsumerOffsetManager<MS>>,
        subscription_group_manager: SubscriptionGroupManager,
        consumer_filter_manager: ConsumerFilterManager,
        broker_stats: Option<Arc<BrokerStats<MS>>>,
        schedule_message_service: Arc<ScheduleMessageService<MS>>,
        timer_message_store: Option<Arc<TimerMessageStore>>,
        broker_outer_api: BrokerOuterAPI,
        registration: BrokerRegistrationRuntime<MS>,
        producer_manager: ProducerManager,
        consumer_manager: ConsumerManager,
        broker_stats_manager: Arc<BrokerStatsManager>,
        role_state: Arc<BrokerOnlineRoleState>,
        pull_request_hold_service: Option<Arc<PullRequestHoldService<MS>>>,
        rebalance_lock_manager: RebalanceLockManager,
        membership: BrokerMembershipState,
        controller: Arc<BrokerControllerRuntime<MS>>,
        special_services: BrokerSpecialServiceCapability<MS>,
        pop_message_processor: Option<Arc<PopMessageProcessor<MS>>>,
        pop_inflight_message_counter: PopInflightMessageCounter,
        query_assignment_processor: Option<Arc<QueryAssignmentProcessor>>,
        slave_synchronize: Option<Arc<SlaveSynchronize<MS>>>,
        cold_data_cg_ctr_service: Option<Arc<ColdDataCgCtrService>>,
        shutdown: Arc<AtomicBool>,
        send_policy: SendMessagePolicyState,
        pull_policy: PullMessagePolicyState,
        pop_policy: PopPolicyState,
        escape_policy: EscapeBridgePolicyState,
        log_filter_control: Option<Arc<BrokerLogFilterControl>>,
    ) -> Self {
        Self {
            config,
            store_host,
            broker_addr,
            message_store_provider,
            topic_config_manager,
            topic_config_coordinator,
            topic_queue_mapping_manager,
            consumer_offset_manager,
            subscription_group_manager,
            consumer_filter_manager,
            broker_stats,
            schedule_message_service,
            timer_message_store,
            broker_outer_api,
            registration,
            producer_manager,
            consumer_manager,
            broker_stats_manager,
            role_state,
            pull_request_hold_service,
            rebalance_lock_manager,
            membership,
            controller,
            special_services,
            pop_message_processor,
            pop_inflight_message_counter,
            query_assignment_processor,
            slave_synchronize,
            cold_data_cg_ctr_service,
            shutdown,
            send_policy,
            pull_policy,
            pop_policy,
            escape_policy,
            log_filter_control,
            config_update_lock: Arc::new(parking_lot::Mutex::new(())),
        }
    }

    pub(crate) fn broker_config(&self) -> Arc<BrokerConfig> {
        self.config.broker_snapshot()
    }

    pub(crate) fn message_store_config(&self) -> Arc<MessageStoreConfig> {
        self.config.store_snapshot()
    }

    pub(crate) fn runtime_config_snapshot(&self) -> Arc<BrokerRuntimeConfigGeneration> {
        self.config.snapshot()
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn log_filter_control(&self) -> Option<&Arc<BrokerLogFilterControl>> {
        self.log_filter_control.as_ref()
    }

    pub(crate) fn server_config(&self) -> ServerConfig {
        self.broker_config().broker_server_config.clone()
    }

    pub(crate) fn message_store(&self) -> Option<EscapeStoreReadLease<MS>> {
        self.message_store_provider.upgrade()?.lease_message_store().ok()
    }

    pub(crate) async fn put_message(
        &self,
        message: MessageExtBrokerInner,
    ) -> Result<PutMessageResult, MessageStoreUnavailable> {
        let message_store = self.message_store_provider.upgrade().ok_or(MessageStoreUnavailable)?;
        message_store.put_message_to_local_store(message).await
    }

    pub(crate) fn set_commitlog_read_mode(&self, read_ahead_mode: i32) -> Result<(), CommitLogReadModeUpdateError> {
        self.message_store_provider
            .upgrade()
            .ok_or(StoreError::new(StoreErrorKind::NotStarted, StoreOperation::Admin))?
            .set_commitlog_read_mode(read_ahead_mode)?;
        self.config.apply_data_read_ahead(read_ahead_mode == MADV_NORMAL)?;
        Ok(())
    }

    pub(crate) fn delete_topics(&self, delete_topics: Vec<&CheetahString>) -> Result<i32, MessageStoreUnavailable> {
        self.message_store_provider
            .upgrade()
            .ok_or(MessageStoreUnavailable)?
            .delete_topics(delete_topics)
    }

    pub(crate) fn topic_config_manager(&self) -> &TopicConfigManager {
        &self.topic_config_manager
    }

    pub(crate) fn topic_config_coordinator(&self) -> &TopicConfigCoordinator {
        &self.topic_config_coordinator
    }

    pub(crate) fn topic_queue_mapping_manager(&self) -> &TopicQueueMappingManager {
        &self.topic_queue_mapping_manager
    }

    pub(crate) fn consumer_offset_manager(&self) -> &ConsumerOffsetManager<MS> {
        &self.consumer_offset_manager
    }

    pub(crate) fn subscription_group_manager(&self) -> &SubscriptionGroupManager {
        &self.subscription_group_manager
    }

    pub(crate) fn consumer_filter_manager(&self) -> &ConsumerFilterManager {
        &self.consumer_filter_manager
    }

    pub(crate) fn broker_stats(&self) -> Option<&BrokerStats<MS>> {
        self.broker_stats.as_deref()
    }

    pub(crate) fn schedule_message_service(&self) -> &Arc<ScheduleMessageService<MS>> {
        &self.schedule_message_service
    }

    pub(crate) fn timer_message_store(&self) -> Option<Arc<TimerMessageStore>> {
        self.timer_message_store.clone().or_else(|| {
            self.message_store()
                .and_then(|message_store| message_store.get_timer_message_store().cloned())
        })
    }

    pub(crate) fn broker_outer_api(&self) -> &BrokerOuterAPI {
        &self.broker_outer_api
    }

    pub(crate) fn producer_manager(&self) -> &ProducerManager {
        &self.producer_manager
    }

    pub(crate) fn consumer_manager(&self) -> &ConsumerManager {
        &self.consumer_manager
    }

    pub(crate) fn broker_stats_manager(&self) -> &BrokerStatsManager {
        &self.broker_stats_manager
    }

    pub(crate) fn broker_stats_manager_handle(&self) -> Arc<BrokerStatsManager> {
        Arc::clone(&self.broker_stats_manager)
    }

    pub(crate) fn pull_request_hold_service(&self) -> Option<&PullRequestHoldService<MS>> {
        self.pull_request_hold_service.as_deref()
    }

    pub(crate) fn rebalance_lock_manager(&self) -> &RebalanceLockManager {
        &self.rebalance_lock_manager
    }

    pub(crate) fn broker_member_group(&self) -> BrokerMemberGroup {
        self.membership.snapshot()
    }

    pub(crate) fn replicas_manager(&self) -> Option<ReplicasManager> {
        self.controller.replicas_snapshot()
    }

    pub(crate) fn pop_message_processor(&self) -> Option<&Arc<PopMessageProcessor<MS>>> {
        self.pop_message_processor.as_ref()
    }

    pub(crate) fn pop_inflight_message_counter(&self) -> &PopInflightMessageCounter {
        &self.pop_inflight_message_counter
    }

    pub(crate) fn query_assignment_processor(&self) -> Option<&Arc<QueryAssignmentProcessor>> {
        self.query_assignment_processor.as_ref()
    }

    pub(crate) fn slave_synchronize(&self) -> Option<&SlaveSynchronize<MS>> {
        self.slave_synchronize.as_deref()
    }

    pub(crate) fn cold_data_cg_ctr_service(&self) -> Option<&ColdDataCgCtrService> {
        self.cold_data_cg_ctr_service.as_deref()
    }

    pub(crate) fn cold_data_cg_ctr_service_handle(&self) -> Option<Arc<ColdDataCgCtrService>> {
        self.cold_data_cg_ctr_service.clone()
    }

    pub(crate) fn get_broker_addr(&self) -> &CheetahString {
        &self.broker_addr
    }

    pub(crate) fn get_ha_server_addr(&self) -> CheetahString {
        const LOCALHOST: &str = "127.0.0.1";
        let broker = self.broker_config();
        let store = self.message_store_config();
        format!(
            "{}:{}",
            broker
                .broker_ip2
                .as_ref()
                .unwrap_or(&CheetahString::from_static_str(LOCALHOST)),
            store.ha_listen_port
        )
        .into()
    }

    pub(crate) fn store_host(&self) -> SocketAddr {
        self.store_host
    }

    pub(crate) fn topic_config_state_machine_version(&self) -> i64 {
        self.message_store()
            .map(|message_store| message_store.get_state_machine_version())
            .unwrap_or_default()
    }

    pub(crate) fn get_min_broker_id_in_group(&self) -> u64 {
        self.role_state.min_broker_id()
    }

    pub(crate) fn update_slave_master_addr(&self, master_addr: Option<CheetahString>) {
        if let Some(slave) = self.slave_synchronize.as_ref() {
            slave.set_master_addr(master_addr.as_ref());
        }
    }

    pub(crate) async fn change_special_service_status(&self, should_start: bool) {
        if let Err(error) = self.special_services.change_status(should_start).await {
            warn!(?error, should_start, "failed to change role-sensitive Admin services");
        }
    }

    pub(crate) async fn apply_controller_role_change(
        &self,
        controller_leader_address: Option<CheetahString>,
        new_master_broker_id: Option<u64>,
        new_master_address: Option<CheetahString>,
        new_master_epoch: Option<i32>,
        sync_state_set_epoch: Option<i32>,
        sync_state_set: HashSet<i64>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.controller
            .apply_controller_role_change(
                controller_leader_address,
                new_master_broker_id,
                new_master_address,
                new_master_epoch,
                sync_state_set_epoch,
                sync_state_set,
            )
            .await
    }

    pub(crate) fn set_broker_config(&self, broker_config: BrokerConfig) -> Result<(), BrokerConfigError> {
        let update_lock = Arc::clone(&self.config_update_lock);
        let _update_guard = update_lock.lock();
        let generation = self.config.replace_broker(broker_config)?;
        self.apply_broker_config_generation(&generation);
        Ok(())
    }

    pub(crate) fn commit_broker_config_patch(
        &self,
        properties: &HashMap<CheetahString, CheetahString>,
    ) -> Result<ConfigGeneration, BrokerConfigError> {
        self.commit_broker_config_patch_inner(None, properties)
    }

    /// Commits a broker configuration patch only when the caller's generation
    /// still matches the current configuration.
    ///
    /// The generation comparison, candidate validation, atomic publication,
    /// and legacy capability projection are serialized by one lock. A stale
    /// supervised operation therefore cannot overwrite a newer configuration.
    pub(crate) fn commit_broker_config_patch_if_generation(
        &self,
        expected_generation: u64,
        properties: &HashMap<CheetahString, CheetahString>,
    ) -> Result<ConfigGeneration, BrokerConfigError> {
        self.commit_broker_config_patch_inner(Some(expected_generation), properties)
    }

    fn commit_broker_config_patch_inner(
        &self,
        expected_generation: Option<u64>,
        properties: &HashMap<CheetahString, CheetahString>,
    ) -> Result<ConfigGeneration, BrokerConfigError> {
        // Request handlers own cloned capability carriers. Serialize the
        // publish-and-project sequence so a slower request cannot overwrite
        // policy views with an older, though valid, generation.
        let update_lock = Arc::clone(&self.config_update_lock);
        let _update_guard = update_lock.lock();
        let current = self.config.snapshot();
        if let Some(expected) = expected_generation {
            let actual = current.id().value();
            if expected != actual {
                return Err(BrokerConfigError::GenerationConflict { expected, actual });
            }
        }
        let transaction = ConfigUpdateTransaction::from_broker_patch(current.id(), current.validated(), properties)?;
        let generation = self.config.commit(transaction)?;
        self.apply_broker_config_generation(&generation);
        Ok(generation.id())
    }

    fn apply_broker_config_generation(
        &self,
        generation: &crate::broker::broker_runtime_config_state::BrokerRuntimeConfigGeneration,
    ) {
        self.role_state
            .set_local_broker_id(generation.broker().broker_identity.broker_id);
        self.send_policy.update_broker_config(generation.broker());
        self.pull_policy.update_broker_config(generation.broker());
        self.pop_policy.update_broker_config(generation.broker());
        self.escape_policy.update_broker_config(generation.broker());
        self.producer_manager.set_broker_config(Arc::clone(generation.broker()));
    }

    pub(crate) async fn register_increment_broker_data(
        &self,
        topic_config_list: Vec<Arc<TopicConfig>>,
        data_version: DataVersion,
    ) {
        self.registration
            .register_increment_broker_data(topic_config_list, data_version)
            .await;
    }

    pub(crate) async fn register_single_topic_all(&self, topic_config: Arc<TopicConfig>) {
        self.registration.register_single_topic_all(topic_config).await;
    }
}
