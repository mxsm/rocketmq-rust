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

use super::control_plane::BrokerControlPlane;
use super::data_plane::BrokerDataPlane;
use super::metadata::BrokerMetadata;
use super::request_pipeline::BrokerRequestPipeline;
use super::*;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_store::BrokerReadStore;

pub(super) struct BrokerComposition {
    pub(super) state: Box<BrokerRuntimeState<BrokerMessageStore>>,
    pub(super) request_pipeline: BrokerRequestPipeline,
    pub(super) data_plane: BrokerDataPlane,
    pub(super) control_plane: BrokerControlPlane,
    pub(super) metadata: BrokerMetadata,
}

impl BrokerComposition {
    pub(super) fn new(
        state: Box<BrokerRuntimeState<BrokerMessageStore>>,
        escape_bridge_owner: Arc<EscapeBridge<BrokerMessageStore>>,
        consumer_ids_change_listener: Arc<dyn ConsumerIdsChangeListener + Send + Sync + 'static>,
        configuration_error: Option<String>,
        #[cfg(feature = "rocksdb_store")] rocksdb_config_managers: Option<BrokerRocksDbConfigManagers>,
    ) -> Self {
        Self {
            state,
            request_pipeline: BrokerRequestPipeline::new(consumer_ids_change_listener),
            data_plane: BrokerDataPlane::new(escape_bridge_owner),
            control_plane: BrokerControlPlane::new(),
            metadata: BrokerMetadata::new(
                configuration_error,
                #[cfg(feature = "rocksdb_store")]
                rocksdb_config_managers,
            ),
        }
    }
}

impl<MS: BrokerStorePort> BrokerRuntimeState<MS> {
    #[inline]
    pub(crate) const fn command_factory(&self) -> RemotingCommandFactory {
        self.command_factory
    }

    pub(super) fn build_special_service_capability(&self) -> BrokerSpecialServiceCapability<MS> {
        BrokerSpecialServiceCapability::new(
            self.schedule_message_service(),
            self.timer_message_store(),
            self.transactional_message_check_service.as_ref(),
            self.ack_message_processor.as_ref(),
            &self.broker_attached_plugins,
            Arc::clone(&self.is_schedule_service_start),
            Arc::clone(&self.is_transaction_check_service_start),
            Arc::clone(&self.shutdown),
        )
    }

    pub(super) fn build_controller_runtime(&self) -> Arc<BrokerControllerRuntime<MS>> {
        let policy = BrokerPreOnlinePolicy::from_configs(
            &self.broker_config(),
            &self.message_store_config(),
            self.get_broker_addr().clone(),
            self.get_ha_server_addr(),
        );
        let special_services = self.build_special_service_capability();
        let registration = BrokerRegistrationCapability::new(
            policy,
            Arc::clone(&self.online_role_state),
            &self.topic_config_manager_handle(),
            &self.topic_config_coordinator_handle(),
            &self.topic_queue_mapping_manager_handle(),
            self.broker_outer_api().clone(),
            Arc::clone(&self.shutdown),
        );
        Arc::new(BrokerControllerRuntime::new(
            self.controller_state.clone(),
            self.broker_member_group.clone(),
            self.config_state.clone(),
            Arc::clone(&self.online_role_state),
            self.escape_bridge().store_capability(),
            special_services,
            registration,
            self.get_broker_addr().clone(),
            Arc::clone(&self.slave_master_addr),
            self.broker_outer_api().clone(),
            Arc::clone(&self.shutdown),
            self.pull_request_hold_service.as_ref(),
            &self.topic_config_manager_handle(),
            self.send_message_policy_state.clone(),
            self.pull_message_policy_state.clone(),
            self.pop_policy_state.clone(),
            self.escape_bridge_policy_state.clone(),
            self.metadata_io
                .as_ref()
                .and_then(|result| result.as_ref().ok())
                .cloned(),
            self.service_context
                .as_ref()
                .map(|context| context.metadata_io().clone()),
        ))
    }

    pub(super) fn build_admin_runtime(&self) -> BrokerAdminRuntime<MS> {
        BrokerAdminRuntime::new(
            self.config_state.clone(),
            self.store_host,
            self.broker_addr.clone(),
            self.escape_bridge
                .as_ref()
                .cloned()
                .expect("EscapeBridge provider must be configured before Admin runtime construction"),
            self.topic_config_manager_handle(),
            self.topic_config_coordinator_handle(),
            self.topic_queue_mapping_manager_handle(),
            self.consumer_offset_manager_handle(),
            self.subscription_group_manager().clone(),
            self.consumer_filter_manager().clone(),
            self.broker_stats.clone(),
            Arc::clone(self.schedule_message_service()),
            self.timer_message_store().cloned(),
            self.broker_outer_api.clone(),
            self.build_registration_runtime(),
            self.producer_manager.clone_shared_state(),
            self.consumer_manager.clone_shared_state(),
            self.broker_stats_manager_handle(),
            Arc::clone(&self.online_role_state),
            self.pull_request_hold_service.clone(),
            self.rebalance_lock_manager.clone(),
            self.broker_member_group.clone(),
            self.build_controller_runtime(),
            self.build_special_service_capability(),
            self.pop_message_processor.clone(),
            self.pop_inflight_message_counter.clone(),
            self.query_assignment_processor.clone(),
            self.slave_synchronize.clone(),
            self.cold_data_cg_ctr_service.clone(),
            Arc::clone(&self.shutdown),
            self.send_message_policy_state.clone(),
            self.pull_message_policy_state.clone(),
            self.pop_policy_state.clone(),
            self.escape_bridge_policy_state.clone(),
            self.log_filter_control.clone(),
        )
    }

    pub(super) fn build_registration_runtime(&self) -> BrokerRegistrationRuntime<MS> {
        BrokerRegistrationRuntime::new(
            self.config_state.clone(),
            self.escape_bridge().store_capability(),
            self.topic_config_manager_handle(),
            self.topic_config_coordinator_handle(),
            self.topic_queue_mapping_manager_handle(),
            self.broker_outer_api.clone(),
            self.get_ha_server_addr(),
            self.slave_synchronize.clone(),
            self.update_master_haserver_addr_periodically,
            Arc::clone(&self.shutdown),
        )
    }
}

impl<MS: BrokerStorePort> BrokerRuntimeState<MS> {
    pub(crate) fn pop_policy_state(&self) -> PopPolicyState {
        self.pop_policy_state.clone()
    }

    pub(super) fn build_pull_message_context(&self) -> Arc<PullMessageProcessorContext<MS>> {
        let escape_bridge = self.escape_bridge();
        Arc::new(
            PullMessageProcessorContext::new(
                self.pull_message_policy_state.clone(),
                self.broker_outer_api().rpc_client().clone(),
                self.consumer_manager().clone_shared_state(),
                Arc::new(self.consumer_filter_manager().clone()),
                self.subscription_group_manager().config_lookup(),
                self.topic_config_manager_handle(),
                self.topic_queue_mapping_manager_handle(),
                &self.consumer_offset_manager,
                self.broadcast_offset_manager().pull_capability(),
                self.broker_stats_manager_handle(),
                Arc::clone(&self.online_role_state),
                PullMessageStoreCapability::new(&escape_bridge),
                self.cold_data_cg_ctr_service_handle(),
            )
            .with_command_factory(self.command_factory),
        )
    }

    pub(super) fn build_pop_message_processor(
        &self,
    ) -> Result<Arc<PopMessageProcessor<MS>>, rocketmq_error::RocketMQError> {
        let topics = self.topic_config_manager_handle();
        let subscriptions = self.subscription_group_manager().config_lookup();
        let offsets = self.consumer_offset_manager_handle().request_capability();
        let order = self.consumer_order_info_manager_handle();
        let escape_bridge = self.escape_bridge();
        let service_context = self.service_context.clone();
        let queue_lock_manager = service_context
            .clone()
            .map(QueueLockManager::new_with_service_context)
            .expect("BrokerRuntime always has an injected ChildServiceContext");
        let context = Arc::new(
            PopMessageProcessorContext::new(
                self.pop_policy_state.clone(),
                Arc::clone(&topics),
                subscriptions.clone(),
                PopConsumerCapability::new(self.consumer_manager()),
                Arc::new(self.consumer_filter_manager().clone()),
                offsets.clone(),
                PopOrderCapability::new(&order),
                PopStoreCapability::new(&escape_bridge),
                self.broker_stats_manager_handle(),
                self.pop_inflight_message_counter().clone(),
                service_context
                    .as_ref()
                    .expect("BrokerRuntime always has an injected ChildServiceContext")
                    .metadata_io()
                    .clone(),
            )
            .with_command_factory(self.command_factory),
        );
        let buffer_context = Arc::new(PopBufferMergeContext::new(
            self.pop_policy_state.clone(),
            Arc::clone(&topics),
            subscriptions.clone(),
            offsets,
            PopStoreCapability::new(&escape_bridge),
            service_context.clone(),
        ));
        let long_polling_context = PopLongPollingServiceContext::new(
            PopLongPollingPolicy::from_config(&self.broker_config()),
            topics,
            subscriptions,
            service_context,
        );
        PopMessageProcessor::new(context, buffer_context, long_polling_context, queue_lock_manager)
    }

    pub(super) fn build_pop_revive_context(&self) -> Arc<PopReviveContext<MS>> {
        let escape_bridge = self.escape_bridge();
        Arc::new(PopReviveContext::new(
            self.pop_policy_state.clone(),
            self.topic_config_manager_handle(),
            self.topic_config_coordinator_handle(),
            self.subscription_group_manager().config_lookup(),
            self.consumer_offset_manager_handle().request_capability(),
            PopStoreCapability::new(&escape_bridge),
            self.broker_stats_manager_handle(),
            self.pop_inflight_message_counter().clone(),
            Arc::clone(&self.should_start_time),
            self.pop_metrics_manager.clone(),
            self.broker_service_context(),
        ))
    }

    pub(super) fn build_consumer_manage_processor(&self) -> ConsumerManageProcessor<MS> {
        ConsumerManageProcessor::new(ConsumerManageProcessorContext {
            command_factory: self.command_factory,
            consumer_view: self.consumer_manager().assignment_view(),
            consumer_offset: self.consumer_offset_manager_handle().request_capability(),
            topic_queue_mapping_manager: self.topic_queue_mapping_manager_handle(),
            subscription_group_lookup: self.subscription_group_manager().config_lookup(),
            topic_config_manager: self.topic_config_manager_handle(),
            rpc_client: self.broker_outer_api().rpc_client().clone(),
            use_server_side_reset_offset: self.broker_config().use_server_side_reset_offset,
            forward_timeout: self.broker_config().forward_timeout,
        })
    }

    pub(super) fn build_transaction_topic_registration(
        &self,
        message_store: TransactionMessageStore<MS>,
    ) -> Arc<TransactionTopicRegistration<MS>> {
        Arc::new(TransactionTopicRegistration::new(TransactionTopicRegistrationContext {
            broker_config: self.broker_config_arc(),
            topic_config_manager: self.topic_config_manager_handle(),
            topic_config_coordinator: self.topic_config_coordinator_handle(),
            topic_queue_mapping_manager: self.topic_queue_mapping_manager_handle(),
            broker_outer_api: self.broker_outer_api().clone(),
            ha_server_addr: self.get_ha_server_addr(),
            message_store,
            slave_master_addr: self.slave_synchronize().map(SlaveSynchronize::master_addr_handle),
            update_master_haserver_addr_periodically: self.update_master_haserver_addr_periodically,
            shutdown: Arc::clone(&self.shutdown),
        }))
    }

    pub(super) fn build_broker_pre_online_service(
        &self,
        escape_bridge: &Arc<EscapeBridge<MS>>,
    ) -> BrokerPreOnlineService<MS> {
        let policy = BrokerPreOnlinePolicy::from_configs(
            &self.broker_config(),
            &self.message_store_config(),
            self.get_broker_addr().clone(),
            self.get_ha_server_addr(),
        );
        let role_state = Arc::clone(&self.online_role_state);
        let schedule = self.schedule_message_service().clone();
        let topic_config_manager = self.topic_config_manager_handle();
        let topic_config_coordinator = self.topic_config_coordinator_handle();
        let topic_queue_mapping_manager = self.topic_queue_mapping_manager_handle();
        let shutdown = Arc::clone(&self.shutdown);
        let special_services = BrokerSpecialServiceCapability::new(
            &schedule,
            self.timer_message_store(),
            self.transactional_message_check_service.as_ref(),
            self.ack_message_processor.as_ref(),
            &self.broker_attached_plugins,
            Arc::clone(&self.is_schedule_service_start),
            Arc::clone(&self.is_transaction_check_service_start),
            Arc::clone(&shutdown),
        );
        let registration = BrokerRegistrationCapability::new(
            policy.clone(),
            Arc::clone(&role_state),
            &topic_config_manager,
            &topic_config_coordinator,
            &topic_queue_mapping_manager,
            self.broker_outer_api().clone(),
            Arc::clone(&shutdown),
        );
        let transition = BrokerOnlineTransitionCapability::new(
            policy.clone(),
            Arc::clone(&role_state),
            special_services,
            registration,
            Arc::clone(&shutdown),
        );
        let context = BrokerPreOnlineContext::new(
            policy,
            role_state,
            self.broker_outer_api().clone(),
            BrokerPreOnlineStoreCapability::new(escape_bridge),
            &self.consumer_offset_manager,
            &schedule,
            self.timer_message_store(),
            &self.broker_attached_plugins,
            transition,
            self.metadata_io
                .as_ref()
                .and_then(|result| result.as_ref().ok())
                .cloned(),
            self.service_context
                .as_ref()
                .map(|context| context.metadata_io().clone()),
        );
        BrokerPreOnlineService::new(context, self.broker_service_task_group())
    }

    pub(crate) fn build_client_manage_processor(&self) -> ClientManageProcessor<MS> {
        let escape_bridge = self.escape_bridge();
        let retry_topic_registration =
            self.build_transaction_topic_registration(TransactionMessageStore::new(&escape_bridge));
        ClientManageProcessor::new(ClientManageProcessorContext {
            command_factory: self.command_factory,
            broker_config: self.broker_config_arc(),
            topic_config_manager: self.topic_config_manager_handle(),
            subscription_group_lookup: self.subscription_group_manager().config_lookup(),
            producer_registration: self.producer_manager().client_registration(),
            consumer_registration: self.consumer_manager().client_registration(),
            retry_topic_registration,
        })
    }

    pub(super) fn build_topic_queue_mapping_clean_service(&self) -> TopicQueueMappingCleanService {
        let broker_config = self.broker_config();
        let message_store_config = self.message_store_config();
        let config = TopicQueueMappingCleanConfig::new(
            broker_config.broker_name().clone(),
            broker_config.forward_timeout,
            message_store_config.delete_when.clone(),
        );
        TopicQueueMappingCleanService::new(
            config,
            Arc::clone(&self.topic_queue_mapping_manager),
            self.broker_outer_api.clone(),
            self.broker_service_context(),
        )
    }

    pub(crate) fn broker_task_group_or_current(
        &self,
        name: impl Into<Arc<str>>,
        no_runtime_warning: &'static str,
    ) -> Option<TaskGroup> {
        crate::broker_runtime::broker_task_group_or_current(self.service_context.as_ref(), name, no_runtime_warning)
    }

    pub(crate) fn broker_service_task_group(&self) -> Option<TaskGroup> {
        self.service_context
            .as_ref()
            .map(|service_context| service_context.task_group().clone())
    }

    pub(crate) fn broker_service_context(&self) -> Option<ChildServiceContext> {
        self.service_context.clone()
    }

    pub(crate) fn resource_budget(&self) -> &ResourceBudget {
        &self.resource_budget
    }

    #[inline]
    pub fn store_host_mut(&mut self) -> &mut SocketAddr {
        &mut self.store_host
    }

    #[inline]
    pub fn topic_queue_mapping_manager_mut(&mut self) -> &mut TopicQueueMappingManager {
        Arc::get_mut(&mut self.topic_queue_mapping_manager)
            .expect("topic queue mapping manager mutation requires an unpublished owner")
    }

    #[inline]
    pub fn subscription_group_manager_mut(&mut self) -> &mut SubscriptionGroupManager {
        self.subscription_group_manager.as_mut().unwrap()
    }

    #[inline]
    pub fn consumer_filter_manager_mut(&mut self) -> &mut ConsumerFilterManager {
        self.consumer_filter_manager.as_mut().unwrap()
    }

    #[inline]
    pub fn message_store_mut(&mut self) -> Option<&mut MS> {
        self.message_store.as_mut().and_then(Arc::get_mut)
    }

    #[inline]
    pub fn broker_stats_mut(&mut self) -> &mut Option<Arc<BrokerStats<MS>>> {
        &mut self.broker_stats
    }

    #[inline]
    pub fn timer_message_store_mut(&mut self) -> &mut Option<Arc<TimerMessageStore>> {
        &mut self.timer_message_store
    }

    #[inline]
    pub fn broker_outer_api_mut(&mut self) -> &mut BrokerOuterAPI {
        &mut self.broker_outer_api
    }

    #[inline]
    pub fn producer_manager_mut(&mut self) -> &mut ProducerManager {
        &mut self.producer_manager
    }

    #[inline]
    pub fn consumer_manager_mut(&mut self) -> &mut ConsumerManager {
        &mut self.consumer_manager
    }

    #[inline]
    pub fn broadcast_offset_manager_mut(&mut self) -> &mut BroadcastOffsetManager {
        &mut self.broadcast_offset_manager
    }

    #[inline]
    pub fn topic_queue_mapping_clean_service_mut(&mut self) -> Option<&mut TopicQueueMappingCleanService> {
        self.topic_queue_mapping_clean_service.as_mut()
    }

    #[inline]
    pub fn update_master_haserver_addr_periodically_mut(&mut self) -> &mut bool {
        &mut self.update_master_haserver_addr_periodically
    }

    #[inline]
    pub fn rebalance_lock_manager_mut(&mut self) -> &mut RebalanceLockManager {
        &mut self.rebalance_lock_manager
    }

    #[inline]
    pub fn broker_member_group_mut(&mut self) -> impl std::ops::DerefMut<Target = BrokerMemberGroup> + '_ {
        self.broker_member_group.write()
    }

    #[inline]
    pub fn transactional_message_check_listener_mut(
        &mut self,
    ) -> &mut Option<DefaultTransactionalMessageCheckListener> {
        &mut self.transactional_message_check_listener
    }

    #[inline]
    pub fn transactional_message_check_service_mut(
        &mut self,
    ) -> Option<&mut Arc<TransactionalMessageCheckService<MS>>> {
        self.transactional_message_check_service.as_mut()
    }

    #[inline]
    pub fn pop_inflight_message_counter_mut(&mut self) -> &mut PopInflightMessageCounter {
        &mut self.pop_inflight_message_counter
    }

    #[inline]
    pub fn replicas_manager(&self) -> Option<ReplicasManager> {
        self.controller_state.replicas_snapshot()
    }

    #[inline]
    pub fn store_host(&self) -> SocketAddr {
        self.store_host
    }

    #[inline]
    pub fn broker_config(&self) -> Arc<BrokerConfig> {
        self.config_state.broker_snapshot()
    }

    #[inline]
    pub fn broker_config_arc(&self) -> Arc<BrokerConfig> {
        self.config_state.broker_snapshot()
    }

    #[inline]
    pub fn message_store_config(&self) -> Arc<MessageStoreConfig> {
        self.config_state.store_snapshot()
    }

    #[inline]
    pub(crate) fn message_store_config_arc(&self) -> Arc<MessageStoreConfig> {
        self.config_state.store_snapshot()
    }

    #[inline]
    pub fn server_config(&self) -> ServerConfig {
        self.broker_config().broker_server_config.clone()
    }

    #[inline]
    pub fn topic_config_manager(&self) -> &TopicConfigManager {
        self.topic_config_manager.as_deref().unwrap()
    }

    #[inline]
    pub(crate) fn topic_config_manager_handle(&self) -> Arc<TopicConfigManager> {
        Arc::clone(self.topic_config_manager.as_ref().unwrap())
    }

    pub(crate) fn topic_config_coordinator(&self) -> &TopicConfigCoordinator {
        self.topic_config_coordinator.as_deref().unwrap()
    }

    pub(crate) fn topic_config_coordinator_handle(&self) -> Arc<TopicConfigCoordinator> {
        Arc::clone(self.topic_config_coordinator.as_ref().unwrap())
    }

    #[inline]
    pub fn topic_queue_mapping_manager(&self) -> &TopicQueueMappingManager {
        self.topic_queue_mapping_manager.as_ref()
    }

    #[inline]
    pub fn consumer_offset_manager(&self) -> &ConsumerOffsetManager<MS> {
        self.consumer_offset_manager.as_ref()
    }

    #[inline]
    pub(crate) fn consumer_offset_manager_handle(&self) -> Arc<ConsumerOffsetManager<MS>> {
        Arc::clone(&self.consumer_offset_manager)
    }

    #[inline]
    pub(crate) fn topic_queue_mapping_manager_handle(&self) -> Arc<TopicQueueMappingManager> {
        Arc::clone(&self.topic_queue_mapping_manager)
    }

    #[inline]
    pub fn subscription_group_manager(&self) -> &SubscriptionGroupManager {
        self.subscription_group_manager.as_ref().unwrap()
    }

    #[inline]
    pub fn consumer_filter_manager(&self) -> &ConsumerFilterManager {
        self.consumer_filter_manager.as_ref().unwrap()
    }

    #[inline]
    pub fn consumer_order_info_manager(&self) -> &ConsumerOrderInfoManager {
        self.consumer_order_info_manager.as_ref().unwrap()
    }

    pub(crate) fn consumer_order_info_manager_handle(&self) -> Arc<ConsumerOrderInfoManager> {
        Arc::clone(
            self.consumer_order_info_manager
                .as_ref()
                .expect("consumer order info manager should be initialized before request processors"),
        )
    }

    #[inline]
    pub fn message_store(&self) -> Option<&MS> {
        self.message_store.as_deref()
    }

    #[inline]
    pub(crate) fn message_store_ref(&self) -> Option<&MS> {
        self.message_store.as_deref()
    }

    pub(crate) fn message_store_weak(&self) -> Option<Weak<MS>> {
        self.message_store.as_ref().map(Arc::downgrade)
    }

    #[inline]
    pub fn lite_subscription_registry(&self) -> &LiteSubscriptionRegistry {
        self.lite_subscription_registry.as_ref()
    }

    #[inline]
    pub fn lite_event_dispatcher(&self) -> &LiteEventDispatcher {
        self.lite_event_dispatcher.as_ref()
    }

    #[inline]
    pub fn lite_lifecycle_manager(&self) -> &LiteLifecycleManager {
        self.lite_lifecycle_manager.as_ref()
    }

    #[inline]
    pub fn pop_lite_message_processor(&self) -> Option<&Arc<PopLiteMessageProcessor<MS>>> {
        self.pop_lite_message_processor.as_ref()
    }

    #[inline]
    pub fn message_store_exclusive_mut(&mut self) -> Option<&mut MS> {
        self.message_store.as_mut().and_then(Arc::get_mut)
    }

    #[inline]
    pub fn broker_stats(&self) -> Option<&BrokerStats<MS>> {
        self.broker_stats.as_deref()
    }

    #[inline]
    pub fn schedule_message_service(&self) -> &Arc<ScheduleMessageService<MS>> {
        self.schedule_message_service.as_ref().unwrap()
    }

    pub(crate) fn schedule_message_service_for_test(&self) -> Option<&Arc<ScheduleMessageService<MS>>> {
        self.schedule_message_service.as_ref()
    }

    #[inline]
    pub fn timer_message_store(&self) -> Option<&Arc<TimerMessageStore>> {
        self.timer_message_store
            .as_ref()
            .or_else(|| self.message_store().and_then(BrokerReadStore::get_timer_message_store))
    }

    #[inline]
    pub fn broker_attached_plugins(&self) -> &[Arc<dyn BrokerAttachedPlugin>] {
        &self.broker_attached_plugins
    }

    #[inline]
    pub fn broker_outer_api(&self) -> &BrokerOuterAPI {
        &self.broker_outer_api
    }

    #[inline]
    pub fn producer_manager(&self) -> &ProducerManager {
        &self.producer_manager
    }

    #[inline]
    pub fn consumer_manager(&self) -> &ConsumerManager {
        &self.consumer_manager
    }

    #[inline]
    pub fn broadcast_offset_manager(&self) -> &BroadcastOffsetManager {
        &self.broadcast_offset_manager
    }

    #[inline]
    pub fn broker_stats_manager(&self) -> &BrokerStatsManager {
        self.broker_stats_manager.as_ref().unwrap()
    }

    #[inline]
    pub(crate) fn broker_stats_manager_handle(&self) -> Arc<BrokerStatsManager> {
        self.broker_stats_manager
            .as_ref()
            .expect("broker_stats_manager should be initialized before request processors")
            .clone()
    }

    #[inline]
    pub fn topic_queue_mapping_clean_service(&self) -> &Option<TopicQueueMappingCleanService> {
        &self.topic_queue_mapping_clean_service
    }

    pub(crate) fn topic_queue_mapping_clean_service_for_test(&self) -> Option<&TopicQueueMappingCleanService> {
        self.topic_queue_mapping_clean_service.as_ref()
    }

    #[inline]
    pub fn update_master_haserver_addr_periodically(&self) -> bool {
        self.update_master_haserver_addr_periodically
    }

    #[inline]
    pub fn should_start_time(&self) -> &AtomicU64 {
        &self.should_start_time
    }

    #[inline]
    pub fn is_isolated(&self) -> &AtomicBool {
        self.online_role_state.isolated_flag()
    }

    #[inline]
    pub fn pull_request_hold_service(&self) -> Option<&PullRequestHoldService<MS>> {
        self.pull_request_hold_service.as_deref()
    }

    #[inline]
    pub fn rebalance_lock_manager(&self) -> &RebalanceLockManager {
        &self.rebalance_lock_manager
    }

    #[inline]
    pub fn broker_member_group(&self) -> BrokerMemberGroup {
        self.broker_member_group.snapshot()
    }

    #[inline]
    pub fn transactional_message_check_listener(&self) -> &Option<DefaultTransactionalMessageCheckListener> {
        &self.transactional_message_check_listener
    }

    #[inline]
    pub fn transactional_message_service(&self) -> Option<&Arc<DefaultTransactionalMessageService<MS>>> {
        self.transactional_message_service.as_ref()
    }

    #[inline]
    pub fn transactional_message_check_service(&self) -> &Option<Arc<TransactionalMessageCheckService<MS>>> {
        &self.transactional_message_check_service
    }

    #[inline]
    pub fn topic_route_info_manager(&self) -> &TopicRouteInfoManager {
        self.topic_route_info_manager.as_ref().unwrap()
    }

    #[inline]
    pub fn escape_bridge(&self) -> Arc<EscapeBridge<MS>> {
        self.escape_bridge
            .as_ref()
            .and_then(Weak::upgrade)
            .expect("EscapeBridge owner must outlive BrokerRuntimeState")
    }

    #[inline]
    pub fn pop_inflight_message_counter(&self) -> &PopInflightMessageCounter {
        &self.pop_inflight_message_counter
    }

    #[inline]
    pub fn cold_data_cg_ctr_service(&self) -> Option<&ColdDataCgCtrService> {
        self.cold_data_cg_ctr_service.as_deref()
    }

    #[inline]
    pub(crate) fn cold_data_cg_ctr_service_handle(&self) -> Option<Arc<ColdDataCgCtrService>> {
        self.cold_data_cg_ctr_service.clone()
    }

    #[inline]
    pub fn slave_synchronize(&self) -> Option<&SlaveSynchronize<MS>> {
        self.slave_synchronize.as_deref()
    }

    #[inline]
    pub fn slave_synchronize_mut(&mut self) -> Option<&SlaveSynchronize<MS>> {
        self.slave_synchronize.as_deref()
    }

    #[inline]
    pub fn update_slave_master_addr(&mut self, master_addr: Option<CheetahString>) {
        self.slave_master_addr.store(master_addr.as_ref());
    }

    #[inline]
    pub fn set_store_host(&mut self, store_host: SocketAddr) {
        self.store_host = store_host;
        self.send_message_policy_state.update_store_host(store_host);
        self.pop_policy_state.update_store_host(store_host);
    }

    #[inline]
    pub fn set_broker_config(&mut self, broker_config: BrokerConfig) -> Result<(), BrokerConfigError> {
        let generation = self.config_state.replace_broker(broker_config)?;
        let broker_config = generation.broker();
        self.online_role_state
            .set_local_broker_id(broker_config.broker_identity.broker_id);
        self.send_message_policy_state.update_broker_config(broker_config);
        self.pull_message_policy_state.update_broker_config(broker_config);
        self.pop_policy_state.update_broker_config(broker_config);
        self.escape_bridge_policy_state.update_broker_config(broker_config);
        Ok(())
    }

    #[inline]
    pub fn set_message_store_config(
        &mut self,
        message_store_config: MessageStoreConfig,
    ) -> Result<(), BrokerConfigError> {
        let generation = self.config_state.replace_store(message_store_config)?;
        let message_store_config = generation.store();
        if let Some(topic_config_manager) = self.topic_config_manager.as_ref() {
            topic_config_manager.update_message_store_policy(message_store_config);
        }
        self.send_message_policy_state
            .update_message_store_config(message_store_config);
        self.pull_message_policy_state.update_store_config(message_store_config);
        self.pop_policy_state.update_store_config(message_store_config);
        self.escape_bridge_policy_state
            .update_message_store_config(message_store_config);
        Ok(())
    }

    #[inline]
    pub fn set_topic_queue_mapping_manager(&mut self, topic_queue_mapping_manager: TopicQueueMappingManager) {
        self.topic_queue_mapping_manager = Arc::new(topic_queue_mapping_manager);
    }

    #[inline]
    pub fn set_subscription_group_manager(&mut self, subscription_group_manager: SubscriptionGroupManager) {
        self.subscription_group_manager = Some(subscription_group_manager);
    }

    #[inline]
    pub fn set_consumer_filter_manager(&mut self, consumer_filter_manager: ConsumerFilterManager) {
        self.consumer_filter_manager = Some(consumer_filter_manager);
    }

    #[inline]
    pub fn set_broker_stats(&mut self, broker_stats: BrokerStats<MS>) {
        self.broker_stats = Some(Arc::new(broker_stats));
    }

    #[inline]
    pub fn set_timer_message_store(&mut self, timer_message_store: TimerMessageStore) {
        self.timer_message_store = Some(Arc::new(timer_message_store));
    }

    #[inline]
    pub fn set_broker_outer_api(&mut self, broker_outer_api: BrokerOuterAPI) {
        self.broker_outer_api = broker_outer_api;
    }

    #[inline]
    pub fn set_producer_manager(&mut self, producer_manager: ProducerManager) {
        self.producer_manager = producer_manager;
    }

    #[inline]
    pub fn set_consumer_manager(&mut self, consumer_manager: ConsumerManager) {
        self.consumer_manager = consumer_manager;
    }

    #[inline]
    pub fn set_broadcast_offset_manager(&mut self, broadcast_offset_manager: BroadcastOffsetManager) {
        self.broadcast_offset_manager = broadcast_offset_manager;
    }

    #[inline]
    pub fn set_broker_stats_manager(&mut self, broker_stats_manager: Arc<BrokerStatsManager>) {
        self.broker_stats_manager = Some(broker_stats_manager);
    }

    #[inline]
    pub fn set_topic_queue_mapping_clean_service(
        &mut self,
        topic_queue_mapping_clean_service: TopicQueueMappingCleanService,
    ) {
        self.topic_queue_mapping_clean_service = Some(topic_queue_mapping_clean_service);
    }

    #[inline]
    pub fn set_update_master_haserver_addr_periodically(&mut self, update_master_haserver_addr_periodically: bool) {
        self.update_master_haserver_addr_periodically = update_master_haserver_addr_periodically;
    }

    #[inline]
    pub fn set_should_start_time(&mut self, should_start_time: Arc<AtomicU64>) {
        self.should_start_time = should_start_time;
    }

    #[inline]
    pub fn set_pull_request_hold_service(&mut self, pull_request_hold_service: Arc<PullRequestHoldService<MS>>) {
        self.pull_request_hold_service = Some(pull_request_hold_service);
    }

    #[inline]
    pub fn set_rebalance_lock_manager(&mut self, rebalance_lock_manager: RebalanceLockManager) {
        self.rebalance_lock_manager = rebalance_lock_manager;
    }

    #[inline]
    pub fn set_broker_member_group(&mut self, broker_member_group: BrokerMemberGroup) {
        self.broker_member_group.publish(broker_member_group);
    }

    pub fn get_min_broker_id_in_group(&self) -> u64 {
        self.online_role_state.min_broker_id()
    }

    #[inline]
    pub fn set_transactional_message_check_listener(
        &mut self,
        transactional_message_check_listener: DefaultTransactionalMessageCheckListener,
    ) {
        self.transactional_message_check_listener = Some(transactional_message_check_listener);
    }

    #[inline]
    pub fn set_transactional_message_check_service(
        &mut self,
        transactional_message_check_service: TransactionalMessageCheckService<MS>,
    ) {
        self.transactional_message_check_service = Some(Arc::new(transactional_message_check_service));
    }

    pub fn set_pop_inflight_message_counter(&mut self, pop_inflight_message_counter: PopInflightMessageCounter) {
        self.pop_inflight_message_counter = pop_inflight_message_counter;
    }

    pub(super) async fn update_namesrv_addr_inner(&mut self) {
        let broker_config = self.broker_config();
        if broker_config.fetch_name_srv_addr_by_dns_lookup {
            if let Some(namesrv_addr) = &broker_config.namesrv_addr {
                self.broker_outer_api
                    .update_name_server_address_list_by_dns_lookup(namesrv_addr.clone())
                    .await;
            }
        } else if let Some(namesrv_addr) = &broker_config.namesrv_addr {
            self.broker_outer_api
                .update_name_server_address_list(namesrv_addr.clone())
                .await;
        }
    }

    #[inline]
    pub fn get_broker_addr(&self) -> &CheetahString {
        &self.broker_addr
    }

    #[inline]
    pub fn get_ha_server_addr(&self) -> CheetahString {
        const LOCALHOST: &str = "127.0.0.1";
        let broker_config = self.broker_config();
        let message_store_config = self.message_store_config();
        let addr = format!(
            "{}:{}",
            broker_config
                .broker_ip2
                .as_ref()
                .unwrap_or(&CheetahString::from_static_str(LOCALHOST)),
            message_store_config.ha_listen_port
        );
        CheetahString::from_string(addr)
    }

    pub(super) fn initialize_controller_mode(&mut self) {
        if !self.controller_state.is_initialized() {
            let broker_config = self.broker_config();
            let message_store_config = self.message_store_config();
            self.controller_state.install(ReplicasManager::new(
                &broker_config,
                &message_store_config,
                self.broker_addr.clone(),
            ));
        }
        self.online_role_state.set_isolated(true);
    }

    pub fn pop_message_processor(&self) -> Option<&Arc<PopMessageProcessor<MS>>> {
        self.pop_message_processor.as_ref()
    }

    pub fn query_assignment_processor(&self) -> Option<&Arc<QueryAssignmentProcessor>> {
        self.query_assignment_processor.as_ref()
    }

    pub async fn change_special_service_status(&mut self, should_start: bool) {
        if let Err(error) = self.change_schedule_service_status(should_start).await {
            error!(?error, should_start, "Failed to change ScheduleMessageService status");
            return;
        }
        for plugin in self.broker_attached_plugins.iter() {
            plugin.status_changed(should_start);
        }
        self.change_transaction_check_service_status(should_start).await;

        if let Some(ack_message_processor) = &self.ack_message_processor {
            info!("Set PopReviveService Status to {}", should_start);
            ack_message_processor.set_pop_revive_service_status(should_start);
        }
    }

    pub async fn change_schedule_service_status(&mut self, should_start: bool) -> rocketmq_error::RocketMQResult<()> {
        if self.is_schedule_service_start.load(Ordering::Relaxed) != should_start {
            info!("change_schedule_service_status changed to {}", should_start);
            if should_start {
                if let Some(schedule_message_service) = &self.schedule_message_service {
                    ScheduleMessageService::start(schedule_message_service.clone()).await?;
                }
            } else if let Some(schedule_message_service) = &self.schedule_message_service {
                schedule_message_service.stop().await?;
            }

            self.is_schedule_service_start.store(should_start, Ordering::Release);

            if let Some(timer) = &mut self.timer_message_store {
                timer.sync_last_read_time_ms();
                timer.set_should_running_dequeue(should_start);
            }
        }
        Ok(())
    }

    pub async fn change_transaction_check_service_status(&mut self, should_start: bool) {
        if self.is_transaction_check_service_start.load(Ordering::Relaxed) != should_start {
            info!("TransactionCheckService status changed to {}", should_start);
            if should_start {
                if let Some(transactional_message_check_service) = &self.transactional_message_check_service {
                    if let Err(error) = transactional_message_check_service.start().await {
                        error!("Failed to start transactional message check service: {error}");
                        return;
                    }
                }
            } else if let Some(transactional_message_check_service) = &self.transactional_message_check_service {
                transactional_message_check_service.shutdown_interrupt(true).await;
            }

            self.is_transaction_check_service_start
                .store(should_start, Ordering::Release);
        }
    }
}

impl BrokerRuntime {
    pub(crate) fn new_with_validated_config(
        validated_config: Arc<ValidatedBrokerConfig>,
        service_context: ChildServiceContext,
    ) -> Self {
        Self::new_with_validated_config_and_telemetry(validated_config, service_context, TelemetryHandle::noop())
    }

    pub(crate) fn new_with_validated_config_and_telemetry(
        validated_config: Arc<ValidatedBrokerConfig>,
        service_context: ChildServiceContext,
        telemetry_handle: TelemetryHandle,
    ) -> Self {
        Self::new_with_validated_config_telemetry_and_factory(
            validated_config,
            service_context,
            telemetry_handle,
            application_remoting_command_factory(),
        )
    }

    pub(crate) fn new_with_validated_config_telemetry_and_factory(
        validated_config: Arc<ValidatedBrokerConfig>,
        service_context: ChildServiceContext,
        telemetry_handle: TelemetryHandle,
        command_factory: RemotingCommandFactory,
    ) -> Self {
        let broker_config = validated_config.broker_arc();
        let message_store_config = validated_config.store_arc();
        #[cfg(feature = "otel-metrics")]
        let broker_metrics_manager = crate::metrics::broker_metrics_manager::BrokerMetricsManager::from_telemetry(
            telemetry_handle.child(rocketmq_observability::BROKER_METER_SCOPE),
            Arc::new(crate::metrics::broker_metrics_manager::BrokerAttributesSupplier::new(
                broker_config.broker_identity.broker_cluster_name.to_string(),
                broker_config.broker_identity.get_canonical_name(),
            )),
            crate::metrics::broker_metrics_manager::BrokerMetricsSamplingConfig::default(),
        )
        .map(Arc::new);
        #[cfg(feature = "otel-metrics")]
        let pop_metrics_manager = crate::metrics::pop_metrics_manager::PopMetricsManager::from_telemetry(
            telemetry_handle.child(rocketmq_observability::BROKER_METER_SCOPE),
            Arc::new(crate::metrics::pop_metrics_manager::BrokerAttributesSupplier::new(
                broker_config.broker_identity.broker_cluster_name.to_string(),
                broker_config.broker_identity.broker_name.to_string(),
                i64::try_from(broker_config.broker_identity.broker_id).unwrap_or(i64::MAX),
            )),
        )
        .map(Arc::new);
        #[cfg(not(feature = "otel-metrics"))]
        let broker_metrics_manager = None;
        #[cfg(not(feature = "otel-metrics"))]
        let pop_metrics_manager = None;
        #[cfg(any(feature = "otel-metrics", feature = "otel-traces"))]
        let transport_telemetry = rocketmq_transport::api::v1::TransportTelemetry::from_handle(&telemetry_handle);
        #[cfg(not(any(feature = "otel-metrics", feature = "otel-traces")))]
        let transport_telemetry = rocketmq_transport::api::v1::TransportTelemetry::noop();
        let store_telemetry = rocketmq_store::StoreTelemetry::from_handle(&telemetry_handle);
        let resource_budget = validated_config
            .sections()
            .resources()
            .budget_tree()
            .expect("validated Broker resources must produce a budget tree")
            .root();
        let lite_event_dispatcher = LiteEventDispatcher::try_with_resource_budget(
            &resource_budget,
            usize::try_from(validated_config.sections().resources().max_lite_subscriptions()).unwrap_or(usize::MAX),
            usize::try_from(validated_config.sections().resources().max_lite_subscriptions()).unwrap_or(usize::MAX),
        )
        .expect("validated Broker Lite event limits must fit the root resource budget");
        let broker_address = broker_config.get_broker_addr();
        let network = validated_config.sections().network();
        let store_host = SocketAddr::new(network.bind_address(), network.listen_port());
        let scheduled_task_manager = BrokerScheduledTasks::new_with_task_group(service_context.task_group().clone());
        let metadata_io = Some(MetadataIoActor::start(
            &service_context.component("broker.metadata-io"),
            MetadataIoConfig::default(),
        ));
        let broker_outer_api = BrokerOuterAPI::new_with_remoting_command_factory(
            Arc::new(TransportClientConfig::default()),
            service_context.component("broker.outer-api"),
            transport_telemetry.clone(),
            command_factory,
        );

        let mut topic_queue_mapping_manager = TopicQueueMappingManager::new_with_service_context(
            broker_config.clone(),
            service_context.component("broker.topic-queue-mapping"),
        );
        if let Some(actor) = metadata_io.as_ref().and_then(|result| result.as_ref().ok()) {
            topic_queue_mapping_manager.set_metadata_io_actor(actor.clone());
        }
        let mut broker_member_group = BrokerMemberGroup::new(
            broker_config.broker_identity.broker_cluster_name.clone(),
            broker_config.broker_identity.broker_name.clone(),
        );
        broker_member_group.broker_addrs.insert(
            broker_config.broker_identity.broker_id,
            broker_config.get_broker_addr().into(),
        );
        let producer_manager = ProducerManager::new();
        let consumer_filter_manager = ConsumerFilterManager::new(broker_config.clone(), message_store_config.clone());
        let consumer_ids_change_listener: Arc<dyn ConsumerIdsChangeListener + Send + Sync + 'static> =
            Arc::new(DefaultConsumerIdsChangeListener::new(consumer_filter_manager.clone()));
        let consumer_manager =
            ConsumerManager::new_with_broker_stats(consumer_ids_change_listener.clone(), broker_config.clone());

        let should_start_time = Arc::new(AtomicU64::new(0));
        let pop_inflight_message_counter = PopInflightMessageCounter::new(should_start_time);
        let broker_fast_failure = BrokerFastFailure::new_with_service_context_and_telemetry(
            broker_config.clone(),
            service_context.clone(),
            telemetry_handle.clone(),
            command_factory,
        );
        #[cfg(feature = "rocksdb_store")]
        let rocksdb_config_managers =
            open_broker_rocksdb_config_managers(broker_config.as_ref(), message_store_config.as_ref());
        #[cfg(feature = "rocksdb_store")]
        let consumer_offset_manager = Arc::new(match rocksdb_config_managers.as_ref() {
            Some(managers) => ConsumerOffsetManager::new_with_rocksdb_config_manager(
                broker_config.clone(),
                message_store_config.clone(),
                Arc::clone(&managers.consumer_offset),
            ),
            None => ConsumerOffsetManager::new(broker_config.clone(), message_store_config.clone()),
        });
        #[cfg(not(feature = "rocksdb_store"))]
        let consumer_offset_manager = Arc::new(ConsumerOffsetManager::new(
            broker_config.clone(),
            message_store_config.clone(),
        ));
        let online_role_state = Arc::new(BrokerOnlineRoleState::new(broker_config.broker_identity.broker_id));
        let send_message_policy_state =
            SendMessagePolicyState::from_configs(&broker_config, &message_store_config, store_host);
        let pull_message_policy_state = PullMessagePolicyState::from_configs(&broker_config, &message_store_config);
        let pop_policy_state = PopPolicyState::from_configs(&broker_config, &message_store_config, store_host);
        let escape_bridge_policy_state = EscapeBridgePolicyState::from_configs(&broker_config, &message_store_config);
        let config_state = BrokerRuntimeConfigState::new(validated_config);
        let slave_master_addr = Arc::new(SlaveMasterAddress::default());

        let mut state = Box::new(BrokerRuntimeState::<BrokerMessageStore> {
            shutdown: Arc::new(AtomicBool::new(false)),
            store_host,
            broker_addr: CheetahString::from(broker_address),
            config_state,
            command_factory,
            resource_budget,
            send_message_policy_state,
            pull_message_policy_state,
            pop_policy_state,
            escape_bridge_policy_state,
            //server_config,
            topic_config_manager: None,
            topic_config_coordinator: None,
            topic_queue_mapping_manager: Arc::new(topic_queue_mapping_manager),
            consumer_offset_manager,
            subscription_group_manager: None,
            consumer_filter_manager: Some(consumer_filter_manager),

            consumer_order_info_manager: None,
            message_store: None,
            broker_stats: None,
            schedule_message_service: None,
            timer_message_store: None,
            lite_event_dispatcher: Arc::new(lite_event_dispatcher),
            lite_lifecycle_manager: Arc::new(LiteLifecycleManager),
            lite_subscription_registry: Arc::new(LiteSubscriptionRegistry::default()),
            broker_outer_api,
            producer_manager,
            consumer_manager,
            broadcast_offset_manager: Default::default(),
            broker_stats_manager: None,
            topic_queue_mapping_clean_service: None,
            update_master_haserver_addr_periodically: false,
            should_start_time: Default::default(),
            online_role_state,
            pull_request_hold_service: None,
            rebalance_lock_manager: Default::default(),
            broker_member_group: BrokerMembershipState::new(broker_member_group),
            transactional_message_check_listener: None,
            transactional_message_check_service: None,
            topic_route_info_manager: None,
            escape_bridge: None,
            pop_inflight_message_counter,
            controller_state: BrokerControllerState::default(),
            broker_fast_failure,
            log_filter_control: None,
            telemetry_handle,
            transport_telemetry,
            store_telemetry,
            broker_metrics_manager,
            pop_metrics_manager,
            observability_guard: None,
            #[cfg(feature = "otel-metrics")]
            observability_metrics_initialized: false,
            cold_data_cg_ctr_service: Some(Arc::new(ColdDataCgCtrService::new(
                message_store_config.cold_data_flow_control_enable,
            ))),
            is_schedule_service_start: Arc::new(Default::default()),
            is_transaction_check_service_start: Arc::new(Default::default()),
            client_housekeeping_service: None,
            pop_message_processor: None,
            pop_lite_message_processor: None,
            ack_message_processor: None,
            notification_processor: None,
            query_assignment_processor: None,
            metadata_io,
            broker_attached_plugins: vec![],
            transactional_message_service: None,
            slave_synchronize: None,
            slave_master_addr,
            broker_pre_online_service: None,
            service_context: Some(service_context.clone()),
            lock: Default::default(),
        });
        let broker_config_snapshot = state.broker_config_arc();
        let message_store_config_snapshot = state.message_store_config_arc();
        let store_runtime_config = Arc::new(broker_config_snapshot.store_runtime_config());
        let mut stats_manager = BrokerStatsManager::new_with_scheduler(
            Arc::clone(&store_runtime_config),
            Some(Arc::new(scheduled_task_manager.clone())),
            service_context.component("broker.statistics").task_group().clone(),
        );
        #[cfg(feature = "rocksdb_store")]
        {
            state.topic_config_manager = Some(Arc::new(match rocksdb_config_managers.as_ref() {
                Some(managers) => TopicConfigManager::new_with_rocksdb_config_manager(
                    broker_config_snapshot.as_ref(),
                    message_store_config_snapshot.as_ref(),
                    true,
                    Arc::clone(&managers.topic),
                    state.broker_metrics_manager.clone(),
                ),
                None => TopicConfigManager::new(
                    broker_config_snapshot.as_ref(),
                    message_store_config_snapshot.as_ref(),
                    true,
                    state.broker_metrics_manager.clone(),
                ),
            }));
        }
        #[cfg(not(feature = "rocksdb_store"))]
        {
            state.topic_config_manager = Some(Arc::new(TopicConfigManager::new(
                broker_config_snapshot.as_ref(),
                message_store_config_snapshot.as_ref(),
                true,
                state.broker_metrics_manager.clone(),
            )));
        }
        stats_manager.set_producer_state_getter(Arc::new(ProducerStateGetter::new(
            state.topic_config_manager_handle(),
            state.producer_manager.clone_shared_state(),
        )));
        stats_manager.set_consumer_state_getter(Arc::new(ConsumerStateGetter::new(
            state.topic_config_manager_handle(),
            state.consumer_manager.clone_shared_state(),
        )));
        let stats_manager = Arc::new(stats_manager);
        state.topic_config_coordinator = Some(Arc::new(TopicConfigCoordinator::new_with_metadata_io(
            state.topic_config_manager_handle(),
            state
                .service_context
                .clone()
                .expect("BrokerRuntime always owns an injected service context")
                .component("broker.topic-config"),
            state
                .metadata_io
                .as_ref()
                .and_then(|result| result.as_ref().ok())
                .cloned(),
        )));
        state.topic_route_info_manager = Some(TopicRouteInfoManager::new(
            state.broker_outer_api.clone(),
            broker_config_snapshot.load_balance_poll_name_server_interval,
            state.broker_service_context(),
        ));
        let escape_bridge = Arc::new(EscapeBridge::new(
            state.escape_bridge_policy_state.clone(),
            state.topic_route_info_manager().clone(),
            state.broker_outer_api.clone(),
        ));
        state.escape_bridge = Some(Arc::downgrade(&escape_bridge));
        state.consumer_offset_manager.bind_message_store(&escape_bridge);
        let broadcast_query_offsets = state.consumer_offset_manager_handle();
        let broadcast_commit_offsets = state.consumer_offset_manager_handle();
        let broadcast_store = escape_bridge.store_capability();
        let broadcast_consumers = state.consumer_manager.clone_shared_state();
        state.broadcast_offset_manager = BroadcastOffsetManager::new(
            move |topic, group, queue_id| {
                broadcast_query_offsets.query_offset(
                    &CheetahString::from_slice(group),
                    &CheetahString::from_slice(topic),
                    queue_id,
                )
            },
            move |topic, group, queue_id, offset| {
                broadcast_commit_offsets.commit_offset(
                    CheetahString::from_static_str("BroadcastOffset"),
                    &CheetahString::from_slice(group),
                    &CheetahString::from_slice(topic),
                    queue_id,
                    offset,
                );
            },
            move |topic, queue_id| {
                let topic = CheetahString::from_slice(topic);
                if broadcast_store
                    .check_in_mem_by_consume_offset(&topic, queue_id)
                    .unwrap_or(false)
                {
                    0
                } else {
                    broadcast_store.max_offset_in_queue(&topic, queue_id).unwrap_or(-1)
                }
            },
            move |group, client_id| {
                broadcast_consumers
                    .find_channel_by_client_id(group, client_id)
                    .is_some()
            },
        );
        let subscription_group_manager_config = SubscriptionGroupManagerConfig::from_configs(
            broker_config_snapshot.as_ref(),
            message_store_config_snapshot.as_ref(),
        );
        let state_machine_version = state
            .message_store()
            .map(BrokerReadStore::state_machine_version_view)
            .unwrap_or_default();
        #[cfg(feature = "rocksdb_store")]
        {
            state.subscription_group_manager = Some(match rocksdb_config_managers.as_ref() {
                Some(managers) => SubscriptionGroupManager::new_with_rocksdb_config_manager(
                    subscription_group_manager_config,
                    state_machine_version,
                    Arc::clone(&managers.subscription_group),
                    state.broker_metrics_manager.clone(),
                ),
                None => SubscriptionGroupManager::new(
                    subscription_group_manager_config,
                    state_machine_version,
                    state.broker_metrics_manager.clone(),
                ),
            });
        }
        #[cfg(not(feature = "rocksdb_store"))]
        {
            state.subscription_group_manager = Some(SubscriptionGroupManager::new(
                subscription_group_manager_config,
                state_machine_version,
                state.broker_metrics_manager.clone(),
            ));
        }
        if let Some(actor) = state
            .metadata_io
            .as_ref()
            .and_then(|result| result.as_ref().ok())
            .cloned()
        {
            state
                .subscription_group_manager
                .as_mut()
                .expect("subscription group manager is initialized above")
                .set_metadata_io_actor(actor);
        }
        let consumer_order_info_manager = Arc::new(ConsumerOrderInfoManager::new(
            broker_config_snapshot.store_path_root_dir.clone(),
            state.topic_config_manager_handle(),
            Arc::clone(state.subscription_group_manager().subscription_group_table()),
        ));
        state.consumer_order_info_manager = Some(consumer_order_info_manager);
        state.producer_manager.set_broker_stats_manager(stats_manager.clone());
        state
            .consumer_manager
            .set_broker_stats_manager(Arc::downgrade(&stats_manager));
        state.broker_stats_manager = Some(stats_manager.clone());
        state.schedule_message_service = Some(Arc::new(ScheduleMessageService::new(
            Arc::clone(&broker_config_snapshot),
            Arc::clone(&message_store_config_snapshot),
            Arc::downgrade(&escape_bridge),
            state
                .service_context
                .clone()
                .expect("BrokerRuntime always owns an injected service context"),
        )));
        state.client_housekeeping_service = Some(Arc::new(ClientHousekeepingService::new(
            state.producer_manager.connection_housekeeping(),
            state.consumer_manager.connection_housekeeping(),
            stats_manager,
            state.broker_service_context(),
        )));
        state.slave_synchronize = Some(Arc::new(SlaveSynchronize::new_with_master_addr(
            SlaveSynchronizeContext::new(
                SlaveSynchronizePolicy::from_config(state.get_broker_addr().clone(), &state.message_store_config()),
                state.broker_outer_api().clone(),
                state.topic_config_manager_handle(),
                state.topic_config_coordinator_handle(),
                state.topic_queue_mapping_manager_handle(),
                state.schedule_message_service().clone(),
                state.subscription_group_manager().clone(),
                SlaveTimerStoreCapability::new(&escape_bridge),
                state
                    .metadata_io
                    .as_ref()
                    .and_then(|result| result.as_ref().ok())
                    .cloned(),
                state
                    .service_context
                    .as_ref()
                    .map(|context| context.metadata_io().clone()),
            ),
            Arc::clone(&state.slave_master_addr),
        )));
        state.topic_queue_mapping_clean_service = Some(state.build_topic_queue_mapping_clean_service());
        Self {
            composition: BrokerComposition::new(
                state,
                escape_bridge,
                consumer_ids_change_listener,
                None,
                #[cfg(feature = "rocksdb_store")]
                rocksdb_config_managers,
            ),
            lifecycle: BrokerLifecycle::new(scheduled_task_manager),
        }
    }

    pub(crate) fn set_telemetry_runtime_guard(&mut self, guard: rocketmq_observability::TelemetryRuntimeGuard) {
        self.composition.state.log_filter_control = guard.log_filter_handle().and_then(|handle| {
            let service_context = self.composition.state.service_context.as_ref()?;
            match crate::broker::log_filter_control::BrokerLogFilterControl::start(
                handle,
                service_context,
                self.composition.state.broker_config().store_path_root_dir.as_str(),
            ) {
                Ok(control) => Some(control),
                Err(error) => {
                    tracing::error!(error = %error, "broker remote log filter reload disabled because TTL control initialization failed");
                    None
                }
            }
        });
        self.composition.state.observability_guard = Some(guard);
    }

    pub(crate) fn release_identity_registered(&self) -> bool {
        self.composition.state.telemetry_handle.release_identity_registered()
    }

    pub(crate) fn broker_config(&self) -> Arc<BrokerConfig> {
        self.composition.state.broker_config_arc()
    }

    pub(crate) fn message_store_config(&self) -> Arc<MessageStoreConfig> {
        self.composition.state.message_store_config_arc()
    }

    pub(crate) fn scheduled_task_manager(&self) -> &BrokerScheduledTasks {
        &self.lifecycle.scheduled_task_manager
    }

    pub(crate) fn runtime_state_mut(&mut self) -> &mut BrokerRuntimeState<BrokerMessageStore> {
        self.composition.state.as_mut()
    }

    pub(super) fn admin_runtime(&self) -> BrokerAdminRuntime<BrokerMessageStore> {
        self.composition.state.build_admin_runtime()
    }

    pub(crate) fn auth_metrics_snapshot(&self) -> Option<AuthMetricsSnapshot> {
        self.composition
            .request_pipeline
            .auth_runtime
            .as_ref()
            .map(|runtime| runtime.metrics_snapshot())
    }

    pub(crate) fn topic_config(&self, topic: &CheetahString) -> Option<Arc<TopicConfig>> {
        self.composition.state.topic_config_manager().select_topic_config(topic)
    }

    pub(crate) fn subscription_group(&self, group: &CheetahString) -> Option<Arc<SubscriptionGroupConfig>> {
        self.composition
            .state
            .subscription_group_manager()
            .find_subscription_group_config(group)
    }
}
