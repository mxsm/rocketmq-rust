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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_auth::config::AuthConfig;
use rocketmq_auth::AuthRuntime;
use rocketmq_auth::AuthRuntimeBuilder;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_common::common::statistics::state_getter::StateGetter;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::UtilAll::compute_next_morning_time_millis;
use rocketmq_remoting::base::channel_event_listener::ChannelEventListener;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigSerializeWrapper;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::protocol::namesrv::RegisterBrokerResult;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_remoting::remoting_server::rocketmq_tokio_server::RocketMQServer;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::schedule::simple_scheduler::ScheduledTaskManager;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::commit_log_dispatcher::CommitLogDispatcher;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::base::store_enum::StoreType;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::ha::ha_service::HAService;
use rocketmq_store::message_store::local_file_message_store::LocalFileMessageStore;
use rocketmq_store::stats::broker_stats::BrokerStats;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use rocketmq_store::timer::timer_message_store::TimerMessageStore;
use tokio::sync::Mutex;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::auth::auth_admin_service::AuthAdminService;
use crate::broker::broker_hook::BrokerShutdownHook;
use crate::broker::broker_pre_online_service::BrokerPreOnlineService;
use crate::client::client_housekeeping_service::ClientHousekeepingService;
use crate::client::consumer_ids_change_listener::ConsumerIdsChangeListener;
use crate::client::default_consumer_ids_change_listener::DefaultConsumerIdsChangeListener;
use crate::client::manager::consumer_manager::ConsumerManager;
use crate::client::manager::producer_manager::ProducerManager;
use crate::client::net::broker_to_client::Broker2Client;
use crate::client::rebalance::rebalance_lock_manager::RebalanceLockManager;
use crate::coldctr::cold_data_cg_ctr_service::ColdDataCgCtrService;
use crate::coldctr::cold_data_pull_request_hold_service::ColdDataPullRequestHoldService;
use crate::controller::replicas_manager::BrokerReplicaRole;
use crate::controller::replicas_manager::ControllerBrokerIdAction;
use crate::controller::replicas_manager::ControllerRegisterFollowup;
use crate::controller::replicas_manager::ControllerReplicaInfoFollowup;
use crate::controller::replicas_manager::ControllerReplicaSyncFollowup;
use crate::controller::replicas_manager::ReplicasManager;
use crate::failover::escape_bridge::EscapeBridge;
use crate::filter::commit_log_dispatcher_calc_bit_map::CommitLogDispatcherCalcBitMap;
use crate::filter::manager::consumer_filter_manager::ConsumerFilterManager;
use crate::hook::batch_check_before_put_message::BatchCheckBeforePutMessageHook;
use crate::hook::check_before_put_message::CheckBeforePutMessageHook;
use crate::hook::schedule_message_hook::ScheduleMessageHook;
use crate::latency::broker_fast_failure::BrokerFastFailure;
use crate::lite::lite_event_dispatcher::LiteEventDispatcher;
use crate::lite::lite_lifecycle_manager::LiteLifecycleManager;
use crate::long_polling::long_polling_service::pull_request_hold_service::PullRequestHoldService;
use crate::long_polling::notify_message_arriving_listener::NotifyMessageArrivingListener;
use crate::offset::manager::broadcast_offset_manager::BroadcastOffsetManager;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::offset::manager::consumer_order_info_manager::ConsumerOrderInfoManager;
use crate::out_api::broker_outer_api::BrokerOuterAPI;
use crate::plugin::broker_attached_plugin::BrokerAttachedPlugin;
use crate::processor::ack_message_processor::AckMessageProcessor;
use crate::processor::admin_broker_processor::AdminBrokerProcessor;
use crate::processor::change_invisible_time_processor::ChangeInvisibleTimeProcessor;
use crate::processor::client_manage_processor::ClientManageProcessor;
use crate::processor::consumer_manage_processor::ConsumerManageProcessor;
use crate::processor::default_pull_message_result_handler::DefaultPullMessageResultHandler;
use crate::processor::end_transaction_processor::EndTransactionProcessor;
use crate::processor::lite_manager_processor::LiteManagerProcessor;
use crate::processor::lite_subscription_ctl_processor::LiteSubscriptionCtlProcessor;
use crate::processor::notification_processor::NotificationProcessor;
use crate::processor::peek_message_processor::PeekMessageProcessor;
use crate::processor::polling_info_processor::PollingInfoProcessor;
use crate::processor::pop_inflight_message_counter::PopInflightMessageCounter;
use crate::processor::pop_lite_message_processor::PopLiteMessageProcessor;
use crate::processor::pop_message_processor::PopMessageProcessor;
use crate::processor::pull_message_processor::PullMessageProcessor;
use crate::processor::query_assignment_processor::QueryAssignmentProcessor;
use crate::processor::query_message_processor::QueryMessageProcessor;
use crate::processor::recall_message_processor::RecallMessageProcessor;
use crate::processor::reply_message_processor::ReplyMessageProcessor;
use crate::processor::send_message_processor::SendMessageProcessor;
use crate::processor::BrokerProcessorType;
use crate::processor::BrokerRequestProcessor;
use crate::schedule::schedule_message_service::ScheduleMessageService;
use crate::slave::slave_synchronize::SlaveSynchronize;
use crate::subscription::lite_subscription_registry::LiteSubscriptionRegistry;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupManager;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;
use crate::topic::manager::topic_route_info_manager::TopicRouteInfoManager;
use crate::topic::topic_queue_mapping_clean_service::TopicQueueMappingCleanService;
use crate::transaction::queue::default_transactional_message_check_listener::DefaultTransactionalMessageCheckListener;
use crate::transaction::queue::default_transactional_message_service::DefaultTransactionalMessageService;
use crate::transaction::queue::transactional_message_bridge::TransactionalMessageBridge;
use crate::transaction::transaction_metrics_flush_service::TransactionMetricsFlushService;
use crate::transaction::transactional_message_check_service::TransactionalMessageCheckService;

type DefaultServerProcessor =
    BrokerRequestProcessor<LocalFileMessageStore, DefaultTransactionalMessageService<LocalFileMessageStore>>;

type FasterServerProcessor =
    BrokerRequestProcessor<LocalFileMessageStore, DefaultTransactionalMessageService<LocalFileMessageStore>>;

fn build_auth_config(broker_config: &BrokerConfig) -> AuthConfig {
    AuthConfig {
        config_name: broker_config.broker_identity.broker_name.clone(),
        cluster_name: broker_config.broker_identity.broker_cluster_name.clone(),
        auth_config_path: broker_config.auth_config_path.clone(),
        authentication_enabled: broker_config.authentication_enabled,
        authentication_whitelist: broker_config.authentication_whitelist.clone(),
        init_authentication_user: broker_config.init_authentication_user.clone(),
        inner_client_authentication_credentials: broker_config.inner_client_authentication_credentials.clone(),
        authorization_enabled: broker_config.authorization_enabled,
        authorization_whitelist: broker_config.authorization_whitelist.clone(),
        ..AuthConfig::default()
    }
}

pub(crate) struct BrokerRuntime {
    #[cfg(feature = "local_file_store")]
    inner: ArcMut<BrokerRuntimeInner<LocalFileMessageStore>>,
    broker_runtime: Option<RocketMQRuntime>,
    shutdown_hook: Option<BrokerShutdownHook>,
    proxy_request_processor: Option<DefaultServerProcessor>,
    consumer_ids_change_listener: Arc<dyn ConsumerIdsChangeListener + Send + Sync + 'static>,
    topic_queue_mapping_clean_service: TopicQueueMappingCleanService,
    scheduled_task_manager: ScheduledTaskManager,
}

impl Drop for BrokerRuntime {
    fn drop(&mut self) {
        if let Some(broker_runtime) = self.broker_runtime.take() {
            broker_runtime.shutdown();
        }
    }
}

impl BrokerRuntime {
    pub(crate) fn new(
        broker_config: Arc<BrokerConfig>,
        message_store_config: Arc<MessageStoreConfig>,
        //server_config: Arc<ServerConfig>,
    ) -> Self {
        let broker_address = format!("{}:{}", broker_config.broker_ip1, broker_config.listen_port);
        let store_host = broker_address.parse::<SocketAddr>().expect("parse store_host failed");
        let runtime = RocketMQRuntime::new_multi(10, "broker-thread");
        let broker_outer_api = BrokerOuterAPI::new(Arc::new(TokioClientConfig::default()));

        let topic_queue_mapping_manager = TopicQueueMappingManager::new(broker_config.clone());
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

        let mut inner = ArcMut::new(BrokerRuntimeInner::<LocalFileMessageStore> {
            shutdown: Arc::new(AtomicBool::new(false)),
            store_host,
            broker_addr: CheetahString::from(broker_address),
            broker_config: broker_config.clone(),
            message_store_config: message_store_config.clone(),
            //server_config,
            topic_config_manager: None,
            topic_queue_mapping_manager,
            consumer_offset_manager: ConsumerOffsetManager::new(
                broker_config.clone(),
                message_store_config.clone(),
                None,
            ),
            subscription_group_manager: None,
            consumer_filter_manager: Some(consumer_filter_manager),

            consumer_order_info_manager: None,
            message_store: None,
            broker_stats: None,
            schedule_message_service: None,
            timer_message_store: None,
            lite_event_dispatcher: Arc::new(LiteEventDispatcher::default()),
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
            is_isolated: Default::default(),
            pull_request_hold_service: None,
            rebalance_lock_manager: Default::default(),
            broker_member_group,
            transactional_message_check_listener: None,
            transactional_message_check_service: None,
            transaction_metrics_flush_service: None,
            topic_route_info_manager: None,
            escape_bridge: None,
            pop_inflight_message_counter,
            replicas_manager: None,
            broker_fast_failure: BrokerFastFailure,
            cold_data_pull_request_hold_service: Some(ColdDataPullRequestHoldService::default()),
            cold_data_cg_ctr_service: Some(ColdDataCgCtrService::new(
                message_store_config.cold_data_flow_control_enable,
            )),
            is_schedule_service_start: Arc::new(Default::default()),
            is_transaction_check_service_start: Arc::new(Default::default()),
            client_housekeeping_service: None,
            pop_message_processor: None,
            pop_lite_message_processor: None,
            ack_message_processor: None,
            notification_processor: None,
            query_assignment_processor: None,
            auth_runtime: None,
            broker_attached_plugins: vec![],
            transactional_message_service: None,
            slave_synchronize: None,
            last_sync_time_ms: AtomicU64::new(current_millis()),
            broker_pre_online_service: None,
            min_broker_id_in_group: AtomicU64::new(0),
            min_broker_addr_in_group: Default::default(),
            lock: Default::default(),
        });
        let mut stats_manager = BrokerStatsManager::new(inner.broker_config.clone());
        stats_manager.set_producer_state_getter(Arc::new(ProducerStateGetter {
            broker_runtime_inner: inner.clone(),
        }));
        stats_manager.set_consumer_state_getter(Arc::new(ConsumerStateGetter {
            broker_runtime_inner: inner.clone(),
        }));
        let stats_manager = Arc::new(stats_manager);
        inner.topic_config_manager = Some(TopicConfigManager::new(inner.clone(), true));
        inner.topic_route_info_manager = Some(TopicRouteInfoManager::new(inner.clone()));
        inner.escape_bridge = Some(EscapeBridge::new(inner.clone()));
        inner.subscription_group_manager = Some(SubscriptionGroupManager::new(inner.clone()));
        inner.consumer_order_info_manager = Some(ConsumerOrderInfoManager::new(inner.clone()));
        inner.producer_manager.set_broker_stats_manager(stats_manager.clone());
        inner
            .consumer_manager
            .set_broker_stats_manager(Arc::downgrade(&stats_manager));
        inner.broker_stats_manager = Some(stats_manager);
        inner.schedule_message_service = Some(ArcMut::new(ScheduleMessageService::new(inner.clone())));
        inner.client_housekeeping_service = Some(Arc::new(ClientHousekeepingService::new(inner.clone())));
        inner.slave_synchronize = Some(SlaveSynchronize::new(inner.clone()));
        inner.broker_pre_online_service = Some(BrokerPreOnlineService::new(inner.clone()));
        Self {
            inner,
            broker_runtime: Some(runtime),
            shutdown_hook: None,
            proxy_request_processor: None,
            consumer_ids_change_listener,
            topic_queue_mapping_clean_service: TopicQueueMappingCleanService,
            scheduled_task_manager: Default::default(),
        }
    }

    pub(crate) fn broker_config(&self) -> &BrokerConfig {
        self.inner.broker_config()
    }

    pub(crate) fn message_store_config(&self) -> &MessageStoreConfig {
        self.inner.message_store_config()
    }

    #[cfg(test)]
    pub(crate) fn inner_for_test(&mut self) -> &mut ArcMut<BrokerRuntimeInner<LocalFileMessageStore>> {
        &mut self.inner
    }

    #[cfg(test)]
    pub(crate) fn init_processor_for_test(&mut self) {
        let _ = self.init_processor();
    }

    pub(crate) fn topic_config(&self, topic: &CheetahString) -> Option<ArcMut<TopicConfig>> {
        self.inner.topic_config_manager().select_topic_config(topic)
    }

    pub(crate) fn subscription_group(&self, group: &CheetahString) -> Option<Arc<SubscriptionGroupConfig>> {
        self.inner
            .subscription_group_manager()
            .find_subscription_group_config(group)
    }

    pub async fn shutdown(&mut self) {
        self.shutdown_basic_service().await;

        self.inner.broker_outer_api.shutdown();

        self.scheduled_task_manager.cancel_all();

        if let Some(runtime) = self.broker_runtime.take() {
            runtime.shutdown();
        }

        if let Some(client_housekeeping_service) = self.inner.client_housekeeping_service.take() {
            client_housekeeping_service.shutdown();
        }
    }

    async fn unregister_broker(&mut self) {
        self.inner
            .broker_outer_api
            .unregister_broker_all(
                &self.inner.broker_config.broker_identity.broker_cluster_name,
                &self.inner.broker_config.broker_identity.broker_name,
                self.inner.get_broker_addr(),
                self.inner.broker_config.broker_identity.broker_id,
            )
            .await;
    }

    pub(crate) async fn shutdown_basic_service(&mut self) {
        self.inner.shutdown.store(true, Ordering::SeqCst);

        self.unregister_broker().await;

        if let Some(hook) = self.shutdown_hook.as_ref() {
            hook.before_shutdown();
        }

        if let Some(broker_stats_manager) = self.inner.broker_stats_manager.as_ref() {
            broker_stats_manager.shutdown();
        }

        if let Some(pull_request_hold_service) = self.inner.pull_request_hold_service.as_mut() {
            pull_request_hold_service.shutdown();
        }

        if let Some(pop_message_processor) = self.inner.pop_message_processor.as_mut() {
            pop_message_processor.shutdown();
        }

        if let Some(pop_lite_message_processor) = self.inner.pop_lite_message_processor.as_mut() {
            pop_lite_message_processor.shutdown();
        }

        if let Some(ack_message_processor) = self.inner.ack_message_processor.as_mut() {
            ack_message_processor.shutdown();
        }

        if let Some(transactional_message_service) = self.inner.transactional_message_service.as_mut() {
            transactional_message_service.shutdown().await;
        }

        if let Some(notification_processor) = self.inner.notification_processor.as_mut() {
            notification_processor.shutdown();
        }
        self.consumer_ids_change_listener.shutdown();
        self.topic_queue_mapping_clean_service.shutdown();

        self.inner.broadcast_offset_manager.shutdown();

        if let Some(message_store) = self.inner.message_store.as_mut() {
            message_store.shutdown().await;
        }

        if let Some(replicas_manager) = self.inner.replicas_manager.as_mut() {
            replicas_manager.shutdown();
        }

        self.inner.broker_fast_failure.shutdown();

        if let Some(consumer_filter_manager) = self.inner.consumer_filter_manager.as_ref() {
            consumer_filter_manager.persist();
        }
        if let Some(consumer_order_info_manager) = self.inner.consumer_order_info_manager.as_ref() {
            consumer_order_info_manager.persist();
        }

        if let Some(schedule_message_service) = self.inner.schedule_message_service.as_mut() {
            schedule_message_service.persist();
            schedule_message_service.shutdown().await;
        }
        if let Some(transactional_message_check_service) = self.inner.transactional_message_check_service.as_mut() {
            transactional_message_check_service.shutdown().await;
        }
        if let Some(transaction_metrics_flush_service) = self.inner.transaction_metrics_flush_service.as_mut() {
            transaction_metrics_flush_service.shutdown();
        }
        if let Some(escape_bridge) = self.inner.escape_bridge.as_mut() {
            escape_bridge.shutdown();
        }
        if let Some(topic_route_info_manager) = self.inner.topic_route_info_manager.as_mut() {
            topic_route_info_manager.shutdown();
        }

        if let Some(topic_route_info_manager) = self.inner.topic_route_info_manager.as_mut() {
            topic_route_info_manager.shutdown();
        }

        if let Some(broker_pre_online_service) = self.inner.broker_pre_online_service.as_mut() {
            broker_pre_online_service.shutdown().await;
        }

        if let Some(cold_data_pull_request_hold_service) = self.inner.cold_data_pull_request_hold_service.as_mut() {
            cold_data_pull_request_hold_service.shutdown();
        }

        if let Some(cold_data_cg_ctr_service) = self.inner.cold_data_cg_ctr_service.as_mut() {
            cold_data_cg_ctr_service.shutdown();
        }

        if let Some(topic_config_manager) = self.inner.topic_config_manager.as_mut() {
            topic_config_manager.persist();
            topic_config_manager.stop();
        }

        if let Some(subscription_group_manager) = self.inner.subscription_group_manager.as_mut() {
            subscription_group_manager.persist();
            subscription_group_manager.stop();
        }

        self.inner.consumer_offset_manager.persist();
        self.inner.consumer_offset_manager.stop();
    }
}

impl BrokerRuntime {
    pub(crate) async fn initialize(&mut self) -> bool {
        let mut result = self.initialize_metadata();
        if !result {
            warn!("Initialize metadata failed");
            return false;
        }
        info!("====== initialize metadata Success========");
        result = self.initialize_message_store().await;
        if !result {
            return false;
        }
        self.recover_initialize_service().await
    }

    /// Load the original configuration data from the corresponding configuration files
    /// located under the `${HOME}\config` directory.
    ///
    /// This function initializes broker metadata by loading several manager components
    /// in sequence:
    /// - Topic configuration manager
    /// - Topic queue mapping manager
    /// - Consumer offset manager
    /// - Subscription group manager
    /// - Consumer filter manager
    /// - Consumer order information manager
    ///
    /// The loaders are invoked in order and combined using logical AND. If all loaders
    /// return `true`, the function returns `true`. If any loader fails (returns `false`),
    /// the whole initialization is considered failed and the function returns `false`.
    fn initialize_metadata(&self) -> bool {
        info!("======Starting initialize metadata========");
        self.inner.topic_config_manager().load()
            && self.inner.topic_queue_mapping_manager().load()
            && self.inner.consumer_offset_manager().load()
            && self.inner.subscription_group_manager().load()
            && self.inner.consumer_filter_manager().load()
            && self.inner.consumer_order_info_manager().load()
    }

    async fn initialize_message_store(&mut self) -> bool {
        let mut flag = true;
        if self.inner.message_store_config.store_type == StoreType::LocalFile {
            info!("Use local file as message store");
            let mut message_store = ArcMut::new(LocalFileMessageStore::new(
                self.inner.message_store_config.clone(),
                self.inner.broker_config.clone(),
                self.inner.topic_config_manager().topic_config_table(),
                self.inner.broker_stats_manager.clone(),
                false,
            ));
            let message_store_clone = message_store.clone();
            message_store.set_message_store_arc(message_store_clone);
            self.inner.timer_message_store = message_store.get_timer_message_store().cloned();
            self.inner.broker_stats = Some(BrokerStats::new(message_store.clone()));
            self.inner.message_store = Some(message_store.clone());
            self.inner
                .consumer_offset_manager
                .set_message_store(Some(message_store));
            if let Some(message_store) = &mut self.inner.message_store {
                match message_store.init().await {
                    Ok(_) => {
                        info!("Initialize message store success");
                    }
                    Err(e) => {
                        warn!("Initialize message store failed, error: {:?}", e);
                        flag = false;
                    }
                }
            }
        } else if self.inner.message_store_config.store_type == StoreType::RocksDB {
            unimplemented!("Use RocksDB as message store unimplemented");
        } else {
            warn!("Unknown store type");
            return false;
        }
        let filter: Arc<dyn CommitLogDispatcher> = Arc::new(CommitLogDispatcherCalcBitMap::new(
            self.inner.broker_config.clone(),
            self.inner.consumer_filter_manager.clone().unwrap(),
        ));
        self.inner.message_store_unchecked_mut().add_first_dispatcher(filter);
        flag
    }

    #[allow(clippy::unnecessary_unwrap)]
    async fn recover_initialize_service(&mut self) -> bool {
        let mut result: bool = true;

        if self.inner.broker_config().enable_controller_mode {
            self.inner.initialize_controller_mode();
        }
        if self.inner.message_store.is_some() {
            self.register_message_store_hook();
            // load message store
            result &= self.inner.message_store.as_mut().unwrap().load().await;
            if !result {
                warn!("Load message store failed");
                return false;
            }
        }

        //scheduleMessageService load after messageStore load success
        if let Some(schedule_message_service) = &mut self.inner.schedule_message_service {
            info!("Load schedule message service");
            result &= schedule_message_service.load();
            if !result {
                warn!("Load schedule message service failed");
                return false;
            }
        } else {
            warn!("Schedule message service is None");
            return false;
        }
        if result {
            self.initialize_remoting_server();
            self.initialize_resources();
            self.initialize_scheduled_tasks().await;
            self.initial_transaction().await;
            result &= self.initial_acl().await;
            if result {
                self.initial_rpc_hooks();
                self.initial_request_pipeline();
            }
        }
        result
    }

    #[inline(always)]
    pub fn register_message_store_hook(&mut self) {
        let config = self.inner.message_store_config.clone();
        let topic_config_table = self.inner.topic_config_manager().topic_config_table();
        let broker_runtime_inner = ArcMut::clone(&self.inner);
        if let Some(ref mut message_store) = self.inner.message_store {
            let message_store_clone = message_store.clone();
            message_store.set_put_message_hook(Box::new(CheckBeforePutMessageHook::new(message_store_clone, config)));
            message_store.set_put_message_hook(Box::new(BatchCheckBeforePutMessageHook::new(topic_config_table)));
            message_store.set_put_message_hook(Box::new(ScheduleMessageHook::new(broker_runtime_inner)))
        }
    }

    fn initialize_remoting_server(&mut self) {

        // fast broker remoting_server implementation in future versions
    }

    fn initialize_resources(&mut self) {
        self.inner.topic_queue_mapping_clean_service = Some(TopicQueueMappingCleanService);
    }

    fn init_processor(&mut self) -> (DefaultServerProcessor, FasterServerProcessor) {
        let send_message_processor = SendMessageProcessor::new(
            self.inner.transactional_message_service.as_ref().unwrap().clone(),
            self.inner.clone(),
        );
        let reply_message_processor = ReplyMessageProcessor::new(
            self.inner.transactional_message_service.as_ref().unwrap().clone(),
            self.inner.clone(),
        );
        let pull_message_result_handler = ArcMut::new(DefaultPullMessageResultHandler::new(
            Arc::new(Default::default()), //optimize
            self.inner.clone(),
        ));

        let pull_message_processor = ArcMut::new(PullMessageProcessor::new(
            pull_message_result_handler,
            self.inner.clone(),
        ));

        let consumer_manage_processor = ConsumerManageProcessor::new(self.inner.clone());
        self.inner.pull_request_hold_service = Some(PullRequestHoldService::new(
            pull_message_processor.clone(),
            self.inner.clone(),
        ));

        let inner = self.inner.clone();
        self.inner
            .message_store
            .as_mut()
            .unwrap()
            .set_message_arriving_listener(Some(Arc::new(Box::new(NotifyMessageArrivingListener::new(inner)))));

        let pop_message_processor = PopMessageProcessor::new_arc_mut(self.inner.clone());
        self.inner.pop_message_processor = Some(pop_message_processor.clone());
        let pop_lite_message_processor = PopLiteMessageProcessor::new_arc_mut(self.inner.clone());
        self.inner.pop_lite_message_processor = Some(pop_lite_message_processor.clone());
        let ack_message_processor = ArcMut::new(AckMessageProcessor::new(
            self.inner.clone(),
            pop_message_processor.clone(),
        ));
        self.inner.ack_message_processor = Some(ack_message_processor.clone());
        let query_assignment_processor = ArcMut::new(QueryAssignmentProcessor::new(self.inner.clone()));
        self.inner.query_assignment_processor = Some(query_assignment_processor.clone());

        let notification_processor = NotificationProcessor::new(self.inner.clone());
        self.inner.notification_processor = Some(notification_processor.clone());
        let mut broker_request_processor = BrokerRequestProcessor::new();
        if let Some(auth_runtime) = &self.inner.auth_runtime {
            broker_request_processor.set_auth_runtime(auth_runtime.clone());
        }
        let send_message_processor = ArcMut::new(send_message_processor);

        broker_request_processor.register_processor(
            RequestCode::SendMessage as i32,
            BrokerProcessorType::Send(send_message_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::SendMessageV2 as i32,
            BrokerProcessorType::Send(send_message_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::SendBatchMessage as i32,
            BrokerProcessorType::Send(send_message_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::ConsumerSendMsgBack as i32,
            BrokerProcessorType::Send(send_message_processor),
        );

        //PullMessageProcessor
        broker_request_processor.register_processor(
            RequestCode::PullMessage as i32,
            BrokerProcessorType::Pull(pull_message_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::LitePullMessage as i32,
            BrokerProcessorType::Pull(pull_message_processor),
        );

        //PeekMessageProcessor
        let peek_message_processor = ArcMut::new(PeekMessageProcessor::new(self.inner.clone()));
        broker_request_processor.register_processor(
            RequestCode::PeekMessage as i32,
            BrokerProcessorType::Peek(peek_message_processor),
        );

        //PopMessageProcessor
        broker_request_processor.register_processor(
            RequestCode::PopMessage as i32,
            BrokerProcessorType::Pop(pop_message_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::PopLiteMessage as i32,
            BrokerProcessorType::PopLite(pop_lite_message_processor),
        );

        //AckMessageProcessor
        broker_request_processor.register_processor(
            RequestCode::AckMessage as i32,
            BrokerProcessorType::Ack(ack_message_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::BatchAckMessage as i32,
            BrokerProcessorType::Ack(ack_message_processor),
        );
        //ChangeInvisibleTimeProcessor
        broker_request_processor.register_processor(
            RequestCode::ChangeMessageInvisibleTime as i32,
            BrokerProcessorType::ChangeInvisible(ArcMut::new(ChangeInvisibleTimeProcessor::new(
                pop_message_processor,
                self.inner.clone(),
            ))),
        );
        //notificationProcessor
        broker_request_processor.register_processor(
            RequestCode::Notification as i32,
            BrokerProcessorType::Notification(notification_processor),
        );

        //pollingInfoProcessor
        broker_request_processor.register_processor(
            RequestCode::PollingInfo as i32,
            BrokerProcessorType::PollingInfo(ArcMut::new(PollingInfoProcessor::new(self.inner.clone()))),
        );

        //ReplyMessageProcessor
        let reply_message_processor = ArcMut::new(reply_message_processor);
        broker_request_processor.register_processor(
            RequestCode::SendReplyMessage as i32,
            BrokerProcessorType::Reply(reply_message_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::SendReplyMessageV2 as i32,
            BrokerProcessorType::Reply(reply_message_processor),
        );

        //RecallMessageProcessor
        let recall_message_processor = ArcMut::new(RecallMessageProcessor::new(self.inner.clone()));
        broker_request_processor.register_processor(
            RequestCode::RecallMessage as i32,
            BrokerProcessorType::Recall(recall_message_processor),
        );

        //QueryMessageProcessor
        let query_message_processor = ArcMut::new(QueryMessageProcessor::new(self.inner.clone()));
        broker_request_processor.register_processor(
            RequestCode::QueryMessage as i32,
            BrokerProcessorType::QueryMessage(query_message_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::ViewMessageById as i32,
            BrokerProcessorType::QueryMessage(query_message_processor),
        );
        //ClientManageProcessor
        let client_manage_processor = ArcMut::new(ClientManageProcessor::new(self.inner.clone()));
        broker_request_processor.register_processor(
            RequestCode::HeartBeat as i32,
            BrokerProcessorType::ClientManage(client_manage_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::UnregisterClient as i32,
            BrokerProcessorType::ClientManage(client_manage_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::CheckClientConfig as i32,
            BrokerProcessorType::ClientManage(client_manage_processor),
        );

        //ConsumerManageProcessor
        let consumer_manage_processor = ArcMut::new(consumer_manage_processor);

        broker_request_processor.register_processor(
            RequestCode::GetConsumerListByGroup as i32,
            BrokerProcessorType::ConsumerManage(consumer_manage_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::UpdateConsumerOffset as i32,
            BrokerProcessorType::ConsumerManage(consumer_manage_processor.clone()),
        );

        broker_request_processor.register_processor(
            RequestCode::QueryConsumerOffset as i32,
            BrokerProcessorType::ConsumerManage(consumer_manage_processor),
        );

        //QueryAssignmentProcessor
        broker_request_processor.register_processor(
            RequestCode::QueryAssignment as i32,
            BrokerProcessorType::QueryAssignment(query_assignment_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::SetMessageRequestMode as i32,
            BrokerProcessorType::QueryAssignment(query_assignment_processor),
        );

        broker_request_processor.register_processor(
            RequestCode::GetBrokerLiteInfo as i32,
            BrokerProcessorType::LiteManager(ArcMut::new(LiteManagerProcessor::new(self.inner.clone()))),
        );
        broker_request_processor.register_processor(
            RequestCode::GetParentTopicInfo as i32,
            BrokerProcessorType::LiteManager(ArcMut::new(LiteManagerProcessor::new(self.inner.clone()))),
        );
        broker_request_processor.register_processor(
            RequestCode::GetLiteTopicInfo as i32,
            BrokerProcessorType::LiteManager(ArcMut::new(LiteManagerProcessor::new(self.inner.clone()))),
        );
        broker_request_processor.register_processor(
            RequestCode::GetLiteClientInfo as i32,
            BrokerProcessorType::LiteManager(ArcMut::new(LiteManagerProcessor::new(self.inner.clone()))),
        );
        broker_request_processor.register_processor(
            RequestCode::GetLiteGroupInfo as i32,
            BrokerProcessorType::LiteManager(ArcMut::new(LiteManagerProcessor::new(self.inner.clone()))),
        );
        broker_request_processor.register_processor(
            RequestCode::TriggerLiteDispatch as i32,
            BrokerProcessorType::LiteManager(ArcMut::new(LiteManagerProcessor::new(self.inner.clone()))),
        );
        broker_request_processor.register_processor(
            RequestCode::LiteSubscriptionCtl as i32,
            BrokerProcessorType::LiteSubscriptionCtl(ArcMut::new(LiteSubscriptionCtlProcessor::new(
                self.inner.clone(),
            ))),
        );

        //EndTransactionProcessor
        broker_request_processor.register_processor(
            RequestCode::EndTransaction as i32,
            BrokerProcessorType::EndTransaction(ArcMut::new(EndTransactionProcessor::new(
                self.inner.transactional_message_service.as_ref().unwrap().clone(),
                self.inner.clone(),
            ))),
        );
        let auth_admin_service = Arc::new(match &self.inner.auth_runtime {
            Some(auth_runtime) => AuthAdminService::with_provider_registry(auth_runtime.provider_registry().clone()),
            None => AuthAdminService::new(build_auth_config(self.inner.broker_config()))
                .expect("broker auth admin service initialization must succeed"),
        });
        let admin_broker_processor = ArcMut::new(AdminBrokerProcessor::new(self.inner.clone(), auth_admin_service));
        broker_request_processor.register_default_processor(BrokerProcessorType::AdminBroker(admin_broker_processor));

        (broker_request_processor.clone(), broker_request_processor)
    }

    #[allow(clippy::incompatible_msrv)]
    async fn initialize_scheduled_tasks(&mut self) {
        let initial_delay = compute_next_morning_time_millis() - current_millis();
        let period = Duration::from_secs(24 * 60 * 60);
        let broker_stats_ = self.inner.clone();
        self.scheduled_task_manager.add_fixed_rate_task_async(
            Duration::from_millis(initial_delay),
            period,
            async move |_ctx| {
                if let Some(broker_stats) = broker_stats_.broker_stats() {
                    broker_stats.record();
                } else {
                    warn!("BrokerStats is not initialized");
                }
                Ok(())
            },
        );

        //need to optimize
        let consumer_offset_manager_inner = self.inner.clone();
        let flush_consumer_offset_interval = self.inner.broker_config.flush_consumer_offset_interval;

        self.scheduled_task_manager.add_fixed_rate_task_async(
            Duration::from_secs(10),
            Duration::from_millis(flush_consumer_offset_interval),
            async move |_ctx| {
                consumer_offset_manager_inner.consumer_offset_manager.persist();
                Ok(())
            },
        );

        //need to optimize
        let mut _inner = self.inner.clone();
        self.scheduled_task_manager.add_fixed_rate_task_async(
            Duration::from_secs(10),
            Duration::from_secs(10),
            async move |_ctx| {
                if let Some(consumer_filter_manager_) = &mut _inner.consumer_filter_manager {
                    consumer_filter_manager_.persist();
                } else {
                    warn!("ConsumerFilterManager is not initialized");
                }
                if let Some(consumer_order_info_manager_) = &mut _inner.consumer_order_info_manager {
                    consumer_order_info_manager_.persist();
                } else {
                    warn!("ConsumerOrderInfoManager is not initialized");
                }

                Ok(())
            },
        );

        let mut runtime = self.inner.clone();
        self.scheduled_task_manager.add_fixed_rate_task_async(
            Duration::from_mins(3),
            Duration::from_mins(3),
            async move |_ctx| {
                runtime.protect_broker();
                Ok(())
            },
        );

        let message_store_inner = self.inner.clone();
        self.scheduled_task_manager.add_fixed_rate_task_async(
            Duration::from_secs(10),
            Duration::from_secs(60),
            async move |_ctx| {
                if let Some(message_store) = message_store_inner.message_store.as_ref() {
                    let behind = message_store.dispatch_behind_bytes();
                    info!("Dispatch task fall behind commit log {behind}bytes");
                } else {
                    warn!("MessageStore is not initialized");
                }
                Ok(())
            },
        );

        if !self.inner.message_store_config.enable_dledger_commit_log
            && !self.inner.message_store_config.duplication_enable
            && !self.inner.message_store_config.enable_controller_mode
        {
            if BrokerRole::Slave == self.inner.broker_config.broker_role {
                info!("Broker is Slave, start replicas manager");
                let ha_master_address = self.inner.message_store_config.ha_master_address.as_ref();
                if let Some(ha_master_address) = ha_master_address {
                    if ha_master_address.len() > 6 {
                        self.inner
                            .message_store
                            .as_ref()
                            .unwrap()
                            .update_ha_master_address(ha_master_address.as_str())
                            .await;
                        self.inner.update_master_haserver_addr_periodically = false;
                    } else {
                        self.inner.update_master_haserver_addr_periodically = true;
                    }
                } else {
                    self.inner.update_master_haserver_addr_periodically = true;
                }
                let inner_clone = self.inner.clone();
                self.scheduled_task_manager.add_fixed_rate_task_async(
                    Duration::from_secs(10),
                    Duration::from_secs(3),
                    async move |_ctx| {
                        if current_millis() - inner_clone.last_sync_time_ms.load(Ordering::Relaxed) > 10_000 {
                            if let Some(slave_synchronize) = &inner_clone.slave_synchronize {
                                slave_synchronize.sync_all().await;
                            }
                            inner_clone.last_sync_time_ms.store(current_millis(), Ordering::Relaxed);
                        }
                        if inner_clone.message_store_config.timer_wheel_enable {
                            if let Some(slave_synchronize) = &inner_clone.slave_synchronize {
                                slave_synchronize.sync_timer_check_point().await
                            }
                        }
                        Ok(())
                    },
                );
            } else {
                let inner_clone = self.inner.clone();
                self.scheduled_task_manager.add_fixed_rate_task_async(
                    Duration::from_secs(10),
                    Duration::from_secs(60),
                    async move |_ctx| {
                        inner_clone.print_master_and_slave_diff();
                        Ok(())
                    },
                );
            }
        }

        if self.inner.broker_config.enable_controller_mode {
            self.inner.update_master_haserver_addr_periodically = true;
        }

        if let Some(ref namesrv_address) = self.inner.broker_config.namesrv_addr.clone() {
            self.update_namesrv_addr().await;
            info!("Set user specified name remoting_server address: {}", namesrv_address);
            let mut broker_runtime = self.inner.clone();
            self.broker_runtime.as_ref().unwrap().get_handle().spawn(async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                loop {
                    let current_execution_time = tokio::time::Instant::now();
                    broker_runtime.update_namesrv_addr_inner().await;
                    let next_execution_time = current_execution_time + Duration::from_secs(60);
                    let delay = next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(delay).await;
                }
            });
        }
    }

    async fn initial_transaction(&mut self) {
        cfg_if::cfg_if! {
            if #[cfg(feature = "local_file_store")] {
                let bridge = TransactionalMessageBridge::new(
                    self.inner.clone()
                );
                let mut service = ArcMut::new(DefaultTransactionalMessageService::new(bridge));
                                let weak_service = ArcMut::downgrade(&service);
                service.set_transactional_op_batch_service_start(weak_service).await;
                self.inner.transactional_message_service = Some(service);
            }
        }
        self.inner.transactional_message_check_listener = Some(DefaultTransactionalMessageCheckListener::new(
            Broker2Client,
            self.inner.clone(),
        ));
        self.inner.transactional_message_check_service =
            Some(TransactionalMessageCheckService::new(self.inner.clone()));
        self.inner.transaction_metrics_flush_service = Some(TransactionMetricsFlushService);
    }

    async fn initial_acl(&mut self) -> bool {
        if !self.inner.broker_config.authentication_enabled && !self.inner.broker_config.authorization_enabled {
            self.inner.auth_runtime = None;
            return true;
        }

        let auth_config = build_auth_config(self.inner.broker_config());
        match AuthRuntimeBuilder::new(auth_config).build().await {
            Ok(auth_runtime) => {
                self.inner.auth_runtime = Some(Arc::new(auth_runtime));
                true
            }
            Err(error) => {
                error!("Initialize auth runtime failed: {error}");
                false
            }
        }
    }

    fn initial_rpc_hooks(&mut self) {}

    fn initial_request_pipeline(&mut self) {}

    async fn start_basic_service(&mut self) {
        if let Some(ref mut message_store) = self.inner.message_store {
            message_store
                .start()
                .await
                .unwrap_or_else(|e| panic!("Failed to start message store: {e}"));
        } else {
            panic!("Message store is not initialized");
        }

        if let Some(replicas_manager) = self.inner.replicas_manager.as_mut() {
            replicas_manager.start();
        }

        let (request_processor, fast_request_processor) = self.init_processor();
        self.proxy_request_processor = Some(request_processor.clone());

        let mut server = RocketMQServer::new(Arc::new(self.inner.broker_config.broker_server_config.clone()));
        //start nomarl broker remoting_server
        let client_housekeeping_service_main = self
            .inner
            .client_housekeeping_service
            .clone()
            .map(|item| item as Arc<dyn ChannelEventListener>);
        let client_housekeeping_service_fast = client_housekeeping_service_main.clone();
        tokio::spawn(async move { server.run(request_processor, client_housekeeping_service_main).await });
        //start fast broker remoting_server
        let mut fast_server_config = self.inner.broker_config.broker_server_config.clone();
        fast_server_config.listen_port = self.inner.broker_config.broker_server_config.listen_port - 2;
        let mut fast_server = RocketMQServer::new(Arc::new(fast_server_config));
        tokio::spawn(async move {
            fast_server
                .run(fast_request_processor, client_housekeeping_service_fast)
                .await
        });

        if let Some(pop_message_processor) = self.inner.pop_message_processor.as_mut() {
            pop_message_processor.start();
        }
        if let Some(pop_lite_message_processor) = self.inner.pop_lite_message_processor.as_mut() {
            pop_lite_message_processor.start();
        }
        if let Some(ack_message_processor) = self.inner.ack_message_processor.as_mut() {
            ack_message_processor.start();
        }

        if let Some(notification_processor) = self.inner.notification_processor.as_mut() {
            notification_processor.start();
        }

        if let Some(topic_queue_mapping_clean_service) = self.inner.topic_queue_mapping_clean_service.as_mut() {
            topic_queue_mapping_clean_service.start();
        }

        let inner = self.inner.clone();
        if let Some(pull_request_hold_service) = self.inner.pull_request_hold_service.as_mut() {
            pull_request_hold_service.start(inner);
        }

        if let Some(broker_stats_manager) = self.inner.broker_stats_manager.as_mut() {
            broker_stats_manager.start();
        }

        self.inner.broker_fast_failure.start();

        self.inner.broadcast_offset_manager.start();

        if let Some(escape_bridge) = self.inner.escape_bridge.as_mut() {
            escape_bridge.start();
        }
        if let Some(topic_route_info_manager) = self.inner.topic_route_info_manager.as_mut() {
            topic_route_info_manager.start();
        }

        if let Some(broker_pre_online_service) = self.inner.broker_pre_online_service.as_mut() {
            broker_pre_online_service.start().await
        }

        if let Some(cold_data_pull_request_hold_service) = self.inner.cold_data_pull_request_hold_service.as_mut() {
            cold_data_pull_request_hold_service.start();
        }
        if let Some(cold_data_cg_ctr_service) = self.inner.cold_data_cg_ctr_service.as_mut() {
            cold_data_cg_ctr_service.start();
        }

        if let Some(client_housekeeping_service) = self.inner.client_housekeeping_service.as_mut() {
            client_housekeeping_service.start();
        }
    }

    async fn update_namesrv_addr(&mut self) {
        self.inner.update_namesrv_addr_inner().await;
    }

    pub async fn start(&mut self) {
        self.inner.should_start_time.store(
            (current_millis() as i64 + self.inner.message_store_config.disappear_time_after_start) as u64,
            Ordering::Release,
        );
        if self.inner.broker_config.enable_controller_mode {
            self.inner.is_isolated.store(true, Ordering::Release);
        }
        if self.inner.message_store_config.total_replicas > 1 && self.inner.broker_config.enable_slave_acting_master {
            self.inner.is_isolated.store(true, Ordering::Release);
        }

        self.inner.broker_outer_api.start().await;
        if self.inner.broker_config.namesrv_addr.is_some() {
            self.update_namesrv_addr().await;
        }
        self.start_basic_service().await;

        if self.inner.broker_config.enable_controller_mode {
            BrokerRuntimeInner::bootstrap_controller_mode(self.inner.clone()).await;
        }

        if !self.inner.is_isolated.load(Ordering::Acquire)
            && !self.inner.message_store_config.enable_dledger_commit_log
            && !self.inner.broker_config.duplication_enable
        {
            let is_master = self.inner.broker_config.broker_identity.broker_id == mix_all::MASTER_ID;
            self.inner.change_special_service_status(is_master).await;
            self.register_broker_all(true, false, true).await;
        }

        //start register broker to name server scheduled task
        let broker_runtime_inner = self.inner.clone();
        let period =
            Duration::from_millis(10000.max(60000.min(broker_runtime_inner.broker_config.register_name_server_period)));
        let initial_delay = Duration::from_secs(10);
        self.scheduled_task_manager
            .add_fixed_rate_task_async(initial_delay, period, async move |_ctx| {
                let start_time = broker_runtime_inner.should_start_time.load(Ordering::Relaxed);
                if current_millis() < start_time {
                    info!("Register to namesrv after {}", start_time);
                    return Ok(());
                }
                if broker_runtime_inner.is_isolated.load(Ordering::Relaxed) {
                    info!("Skip register for broker is isolated");
                    return Ok(());
                }
                let this = broker_runtime_inner.clone();
                broker_runtime_inner
                    .register_broker_all_inner(this, true, false, broker_runtime_inner.broker_config.force_register)
                    .await;
                Ok(())
            });

        if self.inner.broker_config.enable_slave_acting_master {
            self.schedule_send_heartbeat();
            let sync_broker_member_group_period = self.inner.broker_config.sync_broker_member_group_period;
            let inner_ = self.inner.clone();
            self.scheduled_task_manager.add_fixed_rate_task_async(
                Duration::from_millis(1000),
                Duration::from_millis(sync_broker_member_group_period),
                async move |_ctx| {
                    BrokerRuntimeInner::sync_broker_member_group(&inner_).await;
                    Ok(())
                },
            );
        }

        if self.inner.broker_config.enable_controller_mode {
            self.schedule_send_heartbeat();
            self.schedule_sync_controller_metadata();
            self.schedule_sync_controller_replica_info();
        }

        if self.inner.broker_config.skip_pre_online && !self.inner.broker_config.enable_controller_mode {
            self.start_service_without_condition().await;
        }

        let inner = self.inner.clone();
        let period = Duration::from_secs(5);
        let initial_delay = Duration::from_secs(10);
        self.scheduled_task_manager
            .add_fixed_rate_task_async(initial_delay, period, async move |_ctx| {
                inner.broker_outer_api.refresh_metadata();
                Ok(())
            });
        info!(
            "RocketMQ Broker({}) started successfully",
            self.inner.broker_config.broker_identity.broker_name
        );
    }

    pub(crate) fn schedule_send_heartbeat(&mut self) {
        let broker_heartbeat_interval = self.inner.broker_config.broker_heartbeat_interval;
        let inner_ = self.inner.clone();
        self.scheduled_task_manager.add_fixed_rate_task_async(
            Duration::from_millis(1000),
            Duration::from_millis(broker_heartbeat_interval),
            async move |_ctx| {
                if inner_.is_isolated.load(Ordering::Acquire) {
                    if inner_.broker_config.enable_controller_mode {
                        BrokerRuntimeInner::bootstrap_controller_mode(inner_.clone()).await;
                    }
                    info!("Skip send heartbeat for broker is isolated");
                    return Ok(());
                }
                if inner_.broker_config.enable_controller_mode {
                    BrokerRuntimeInner::refresh_controller_leader(inner_.clone()).await;
                }
                inner_.send_heartbeat().await;
                Ok(())
            },
        );
    }

    pub(crate) fn schedule_sync_controller_metadata(&mut self) {
        let period = self.inner.broker_config.sync_controller_metadata_period;
        let inner_ = self.inner.clone();
        self.scheduled_task_manager.add_fixed_rate_task_async(
            Duration::from_millis(1000),
            Duration::from_millis(period),
            async move |_ctx| {
                BrokerRuntimeInner::refresh_controller_leader(inner_.clone()).await;
                Ok(())
            },
        );
    }

    pub(crate) fn schedule_sync_controller_replica_info(&mut self) {
        let period = self.inner.broker_config.sync_broker_metadata_period;
        let inner_ = self.inner.clone();
        self.scheduled_task_manager.add_fixed_rate_task_async(
            Duration::from_millis(3000),
            Duration::from_millis(period),
            async move |_ctx| {
                BrokerRuntimeInner::sync_controller_replica_info(inner_.clone()).await;
                Ok(())
            },
        );
    }

    pub(crate) async fn start_service_without_condition(&mut self) {
        info!(
            "{} start service",
            self.inner.broker_config.broker_identity.get_canonical_name()
        );
        let is_master = self.inner.broker_config.broker_identity.broker_id == mix_all::MASTER_ID;
        self.inner.change_special_service_status(is_master).await;
        self.register_broker_all(true, false, self.inner.broker_config.force_register)
            .await;
        self.inner.is_isolated.store(false, Ordering::Release);
    }

    /// Register broker to name remoting_server
    pub(crate) async fn register_broker_all(&mut self, check_order_config: bool, oneway: bool, force_register: bool) {
        self.inner
            .register_broker_all_inner(self.inner.clone(), check_order_config, oneway, force_register)
            .await;
    }

    async fn do_register_broker_all(
        &mut self,
        _check_order_config: bool,
        oneway: bool,
        topic_config_wrapper: TopicConfigAndMappingSerializeWrapper,
    ) {
        let cluster_name = self.inner.broker_config.broker_identity.broker_cluster_name.clone();
        let broker_name = self.inner.broker_config.broker_identity.broker_name.clone();
        let broker_addr = CheetahString::from_string(format!(
            "{}:{}",
            self.inner.broker_config.broker_ip1, self.inner.broker_config.broker_server_config.listen_port
        ));
        let broker_id = self.inner.broker_config.broker_identity.broker_id;
        //  let weak = Arc::downgrade(&self.inner.broker_outer_api);

        self.inner
            .broker_outer_api
            .register_broker_all(
                cluster_name,
                broker_addr.clone(),
                broker_name,
                broker_id,
                broker_addr,
                topic_config_wrapper,
                vec![],
                oneway,
                10000,
                false,
                false,
                None,
                Default::default(),
                self.inner.clone(),
            )
            .await;
    }

    pub(crate) fn proxy_request_processor(&self) -> Option<DefaultServerProcessor> {
        self.proxy_request_processor.clone()
    }
}

impl<MS: MessageStore> BrokerRuntimeInner<MS> {
    pub async fn register_single_topic_all(&self, topic_config: ArcMut<TopicConfig>) {
        let mut topic_config = topic_config;
        if !PermName::is_writeable(self.broker_config.broker_permission)
            || !PermName::is_readable(self.broker_config.broker_permission)
        {
            topic_config.perm &= self.broker_config.broker_permission;
        }
        self.broker_outer_api
            .register_single_topic_all(
                self.broker_config.broker_identity.broker_name.clone(),
                topic_config,
                3000,
            )
            .await;
    }

    pub async fn register_increment_broker_data(
        this: ArcMut<BrokerRuntimeInner<MS>>,
        topic_config_list: Vec<ArcMut<TopicConfig>>,
        data_version: DataVersion,
    ) {
        let mut serialize_wrapper = TopicConfigAndMappingSerializeWrapper {
            topic_config_serialize_wrapper: TopicConfigSerializeWrapper {
                data_version: data_version.clone(),
                topic_config_table: Default::default(),
            },
            ..Default::default()
        };

        let mut topic_config_table = HashMap::new();
        for topic_config in topic_config_list.iter() {
            let register_topic_config = if !PermName::is_writeable(this.broker_config().broker_permission)
                || !PermName::is_readable(this.broker_config().broker_permission)
            {
                TopicConfig {
                    perm: topic_config.perm & this.broker_config().broker_permission,
                    ..topic_config.as_ref().clone()
                }
            } else {
                topic_config.as_ref().clone()
            };
            topic_config_table.insert(
                register_topic_config.topic_name.as_ref().unwrap().clone(),
                register_topic_config,
            );
        }
        serialize_wrapper.topic_config_serialize_wrapper.topic_config_table = topic_config_table;
        let topic_queue_mapping_info_map = DashMap::new();
        for topic_config in topic_config_list {
            if let Some(ref value) = this
                .topic_queue_mapping_manager
                .get_topic_queue_mapping(topic_config.topic_name.as_ref().unwrap().as_str())
            {
                topic_queue_mapping_info_map.insert(
                    topic_config.topic_name.as_ref().unwrap().clone(),
                    ArcMut::new(TopicQueueMappingDetail::clone_as_mapping_info(value.as_ref())),
                );
            }
        }
        serialize_wrapper.topic_queue_mapping_info_map = topic_queue_mapping_info_map;
        Self::do_register_broker_all(this, true, false, serialize_wrapper).await;
    }

    async fn do_register_broker_all(
        mut this: ArcMut<BrokerRuntimeInner<MS>>,
        check_order_config: bool,
        oneway: bool,
        topic_config_wrapper: TopicConfigAndMappingSerializeWrapper,
    ) {
        if this.shutdown.load(Ordering::Acquire) {
            info!("BrokerRuntimeInner#do_register_broker_all: broker has shutdown, no need to register any more.");
            return;
        }

        let cluster_name = this.broker_config.broker_identity.broker_cluster_name.clone();
        let broker_name = this.broker_config.broker_identity.broker_name.clone();
        let broker_addr = CheetahString::from_string(format!(
            "{}:{}",
            this.broker_config.broker_ip1, this.broker_config.broker_server_config.listen_port
        ));
        let broker_id = this.broker_config.broker_identity.broker_id;
        let result = this
            .broker_outer_api
            .register_broker_all(
                cluster_name,
                broker_addr.clone(),
                broker_name,
                broker_id,
                broker_addr,
                topic_config_wrapper,
                vec![],
                oneway,
                this.broker_config.register_broker_timeout_mills as u64,
                this.broker_config.enable_slave_acting_master,
                this.broker_config.compressed_register,
                this.broker_config
                    .enable_slave_acting_master
                    .then_some(this.broker_config.broker_not_active_timeout_millis),
                Default::default(), //optimize
                this.clone(),
            )
            .await;
        this.handle_register_broker_result(result, check_order_config).await;
    }

    pub(self) async fn handle_register_broker_result(
        &mut self,
        register_broker_result: Vec<RegisterBrokerResult>,
        check_order_config: bool,
    ) {
        if let Some(result) = register_broker_result.into_iter().next() {
            if self.update_master_haserver_addr_periodically {
                if let Some(message_store) = &self.message_store {
                    message_store
                        .update_ha_master_address(result.master_addr.as_str())
                        .await;
                    message_store.update_master_address(&result.master_addr);
                }
            }
            if let Some(slave_synchronize) = &mut self.slave_synchronize {
                slave_synchronize.set_master_addr(Some(&result.master_addr));
            }
            if check_order_config {
                if let Some(topic_config_manager) = &mut self.topic_config_manager {
                    topic_config_manager.update_order_topic_config(&result.kv_table);
                }
            }
        }
    }
}

struct ProducerStateGetter<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}
impl<MS: MessageStore> StateGetter for ProducerStateGetter<MS> {
    fn online(&self, instance_id: &CheetahString, group: &CheetahString, topic: &CheetahString) -> bool {
        if self
            .broker_runtime_inner
            .topic_config_manager()
            .topic_config_table()
            .contains_key(NamespaceUtil::wrap_namespace(instance_id, topic).as_str())
        {
            self.broker_runtime_inner
                .producer_manager
                .group_online(&NamespaceUtil::wrap_namespace(instance_id, group))
        } else {
            self.broker_runtime_inner.producer_manager.group_online(group)
        }
    }
}

struct ConsumerStateGetter<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> StateGetter for ConsumerStateGetter<MS> {
    fn online(&self, instance_id: &CheetahString, group: &CheetahString, topic: &CheetahString) -> bool {
        if self
            .broker_runtime_inner
            .topic_config_manager()
            .topic_config_table()
            .contains_key(topic)
        {
            let topic_full_name = NamespaceUtil::wrap_namespace(instance_id, topic);
            self.broker_runtime_inner
                .consumer_manager
                .find_subscription_data(&NamespaceUtil::wrap_namespace(instance_id, group), &topic_full_name)
                .is_some()
        } else {
            self.broker_runtime_inner
                .consumer_manager
                .find_subscription_data(group, topic)
                .is_some()
        }
    }
}

pub(crate) struct BrokerRuntimeInner<MS: MessageStore> {
    shutdown: Arc<AtomicBool>,
    store_host: SocketAddr,
    broker_addr: CheetahString,
    broker_config: Arc<BrokerConfig>,
    message_store_config: Arc<MessageStoreConfig>,
    topic_config_manager: Option<TopicConfigManager<MS>>,
    topic_queue_mapping_manager: TopicQueueMappingManager,
    consumer_offset_manager: ConsumerOffsetManager<MS>,
    subscription_group_manager: Option<SubscriptionGroupManager<MS>>,
    consumer_filter_manager: Option<ConsumerFilterManager>,
    consumer_order_info_manager: Option<ConsumerOrderInfoManager<MS>>,
    message_store: Option<ArcMut<MS>>,
    broker_stats: Option<BrokerStats<MS>>,
    schedule_message_service: Option<ArcMut<ScheduleMessageService<MS>>>,
    timer_message_store: Option<Arc<TimerMessageStore>>,
    lite_event_dispatcher: Arc<LiteEventDispatcher>,
    lite_lifecycle_manager: Arc<LiteLifecycleManager>,
    lite_subscription_registry: Arc<LiteSubscriptionRegistry>,
    broker_outer_api: BrokerOuterAPI,
    producer_manager: ProducerManager,
    consumer_manager: ConsumerManager,
    broadcast_offset_manager: BroadcastOffsetManager,
    broker_stats_manager: Option<Arc<BrokerStatsManager>>,
    topic_queue_mapping_clean_service: Option<TopicQueueMappingCleanService>,
    update_master_haserver_addr_periodically: bool,
    should_start_time: Arc<AtomicU64>,
    is_isolated: Arc<AtomicBool>,
    pull_request_hold_service: Option<PullRequestHoldService<MS>>,
    rebalance_lock_manager: RebalanceLockManager,
    broker_member_group: BrokerMemberGroup,
    transactional_message_check_listener: Option<DefaultTransactionalMessageCheckListener<MS>>,
    transactional_message_check_service: Option<TransactionalMessageCheckService<MS>>,
    transaction_metrics_flush_service: Option<TransactionMetricsFlushService>,
    topic_route_info_manager: Option<TopicRouteInfoManager<MS>>,
    escape_bridge: Option<EscapeBridge<MS>>,
    pop_inflight_message_counter: PopInflightMessageCounter,
    replicas_manager: Option<ReplicasManager>,
    broker_fast_failure: BrokerFastFailure,
    cold_data_pull_request_hold_service: Option<ColdDataPullRequestHoldService>,
    cold_data_cg_ctr_service: Option<ColdDataCgCtrService>,
    is_schedule_service_start: Arc<AtomicBool>,
    is_transaction_check_service_start: Arc<AtomicBool>,
    client_housekeeping_service: Option<Arc<ClientHousekeepingService<MS>>>,
    //Processor
    pop_message_processor: Option<ArcMut<PopMessageProcessor<MS>>>,
    pop_lite_message_processor: Option<ArcMut<PopLiteMessageProcessor<MS>>>,
    ack_message_processor: Option<ArcMut<AckMessageProcessor<MS>>>,
    notification_processor: Option<ArcMut<NotificationProcessor<MS>>>,
    query_assignment_processor: Option<ArcMut<QueryAssignmentProcessor<MS>>>,
    auth_runtime: Option<Arc<AuthRuntime>>,
    broker_attached_plugins: Vec<Arc<dyn BrokerAttachedPlugin>>,
    transactional_message_service: Option<ArcMut<DefaultTransactionalMessageService<MS>>>,
    slave_synchronize: Option<SlaveSynchronize<MS>>,
    last_sync_time_ms: AtomicU64,
    broker_pre_online_service: Option<BrokerPreOnlineService<MS>>,
    min_broker_id_in_group: AtomicU64,
    min_broker_addr_in_group: Mutex<Option<CheetahString>>,
    lock: Mutex<()>,
}

impl<MS: MessageStore> BrokerRuntimeInner<MS> {
    #[inline]
    pub fn store_host_mut(&mut self) -> &mut SocketAddr {
        &mut self.store_host
    }

    #[inline]
    pub fn topic_config_manager_mut(&mut self) -> &mut TopicConfigManager<MS> {
        self.topic_config_manager.as_mut().unwrap()
    }

    #[inline]
    pub fn topic_config_manager_unchecked_mut(&mut self) -> &mut TopicConfigManager<MS> {
        unsafe { self.topic_config_manager.as_mut().unwrap_unchecked() }
    }

    #[inline]
    pub fn topic_queue_mapping_manager_mut(&mut self) -> &mut TopicQueueMappingManager {
        &mut self.topic_queue_mapping_manager
    }

    #[inline]
    pub fn consumer_offset_manager_mut(&mut self) -> &mut ConsumerOffsetManager<MS> {
        &mut self.consumer_offset_manager
    }

    #[inline]
    pub fn subscription_group_manager_mut(&mut self) -> &mut SubscriptionGroupManager<MS> {
        self.subscription_group_manager.as_mut().unwrap()
    }

    #[inline]
    pub fn subscription_group_manager_unchecked_mut(&mut self) -> &mut SubscriptionGroupManager<MS> {
        unsafe { self.subscription_group_manager.as_mut().unwrap_unchecked() }
    }

    #[inline]
    pub fn consumer_filter_manager_mut(&mut self) -> &mut ConsumerFilterManager {
        self.consumer_filter_manager.as_mut().unwrap()
    }

    #[inline]
    pub fn consumer_filter_manager_unchecked_mut(&mut self) -> &mut ConsumerFilterManager {
        unsafe { self.consumer_filter_manager.as_mut().unwrap_unchecked() }
    }

    #[inline]
    pub fn consumer_order_info_manager_mut(&mut self) -> &mut ConsumerOrderInfoManager<MS> {
        self.consumer_order_info_manager.as_mut().unwrap()
    }

    #[inline]
    pub fn consumer_order_info_manager_unchecked_mut(&mut self) -> &mut ConsumerOrderInfoManager<MS> {
        unsafe { self.consumer_order_info_manager.as_mut().unwrap_unchecked() }
    }

    #[inline]
    pub fn message_store_mut(&mut self) -> &mut Option<ArcMut<MS>> {
        &mut self.message_store
    }

    #[inline]
    pub fn broker_stats_mut(&mut self) -> &mut Option<BrokerStats<MS>> {
        &mut self.broker_stats
    }

    #[inline]
    pub fn schedule_message_service_mut(&mut self) -> &mut ArcMut<ScheduleMessageService<MS>> {
        self.schedule_message_service.as_mut().unwrap()
    }

    #[inline]
    pub fn schedule_message_service_unchecked_mut(&mut self) -> &mut ArcMut<ScheduleMessageService<MS>> {
        unsafe { self.schedule_message_service.as_mut().unwrap_unchecked() }
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
    pub fn broker_stats_manager_mut(&mut self) -> &mut BrokerStatsManager {
        unimplemented!("broker_stats_manager_mut")
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
    pub fn pull_request_hold_service_mut(&mut self) -> Option<&mut PullRequestHoldService<MS>> {
        self.pull_request_hold_service.as_mut()
    }

    #[inline]
    pub fn rebalance_lock_manager_mut(&mut self) -> &mut RebalanceLockManager {
        &mut self.rebalance_lock_manager
    }

    #[inline]
    pub fn broker_member_group_mut(&mut self) -> &mut BrokerMemberGroup {
        &mut self.broker_member_group
    }

    #[inline]
    pub fn transactional_message_service_mut(&mut self) -> &mut Option<ArcMut<DefaultTransactionalMessageService<MS>>> {
        &mut self.transactional_message_service
    }

    #[inline]
    pub fn transactional_message_service_unchecked_mut(&mut self) -> &mut DefaultTransactionalMessageService<MS> {
        unsafe { self.transactional_message_service.as_mut().unwrap_unchecked() }
    }

    #[inline]
    pub fn transactional_message_check_listener_mut(
        &mut self,
    ) -> &mut Option<DefaultTransactionalMessageCheckListener<MS>> {
        &mut self.transactional_message_check_listener
    }

    #[inline]
    pub fn transactional_message_check_service_mut(&mut self) -> Option<&mut TransactionalMessageCheckService<MS>> {
        self.transactional_message_check_service.as_mut()
    }

    #[inline]
    pub fn transactional_message_check_service_unchecked_mut(&mut self) -> &mut TransactionalMessageCheckService<MS> {
        unsafe { self.transactional_message_check_service.as_mut().unwrap_unchecked() }
    }

    #[inline]
    pub fn transaction_metrics_flush_service_mut(&mut self) -> Option<&mut TransactionMetricsFlushService> {
        self.transaction_metrics_flush_service.as_mut()
    }

    #[inline]
    pub fn topic_route_info_manager_mut(&mut self) -> &mut TopicRouteInfoManager<MS> {
        self.topic_route_info_manager.as_mut().unwrap()
    }

    #[inline]
    pub fn topic_route_info_manager_unchecked_mut(&mut self) -> &mut TopicRouteInfoManager<MS> {
        unsafe { self.topic_route_info_manager.as_mut().unwrap_unchecked() }
    }

    #[inline]
    pub fn escape_bridge_mut(&mut self) -> &mut EscapeBridge<MS> {
        self.escape_bridge.as_mut().unwrap()
    }

    #[inline]
    pub fn escape_bridge_unchecked_mut(&mut self) -> &mut EscapeBridge<MS> {
        unsafe { self.escape_bridge.as_mut().unwrap_unchecked() }
    }

    #[inline]
    pub fn pop_inflight_message_counter_mut(&mut self) -> &mut PopInflightMessageCounter {
        &mut self.pop_inflight_message_counter
    }

    #[inline]
    pub fn replicas_manager(&self) -> Option<&ReplicasManager> {
        self.replicas_manager.as_ref()
    }

    #[inline]
    pub fn replicas_manager_mut(&mut self) -> Option<&mut ReplicasManager> {
        self.replicas_manager.as_mut()
    }

    #[inline]
    pub fn store_host(&self) -> SocketAddr {
        self.store_host
    }

    #[inline]
    pub fn broker_config(&self) -> &BrokerConfig {
        &self.broker_config
    }

    #[inline]
    pub fn broker_config_arc(&self) -> Arc<BrokerConfig> {
        self.broker_config.clone()
    }

    #[inline]
    pub fn message_store_config(&self) -> &MessageStoreConfig {
        &self.message_store_config
    }

    #[inline]
    pub fn server_config(&self) -> &ServerConfig {
        &self.broker_config.broker_server_config
    }

    #[inline]
    pub fn topic_config_manager(&self) -> &TopicConfigManager<MS> {
        self.topic_config_manager.as_ref().unwrap()
    }

    #[inline]
    pub fn topic_config_manager_unchecked(&self) -> &TopicConfigManager<MS> {
        unsafe { self.topic_config_manager.as_ref().unwrap_unchecked() }
    }

    #[inline]
    pub fn topic_queue_mapping_manager(&self) -> &TopicQueueMappingManager {
        &self.topic_queue_mapping_manager
    }

    #[inline]
    pub fn consumer_offset_manager(&self) -> &ConsumerOffsetManager<MS> {
        &self.consumer_offset_manager
    }

    #[inline]
    pub fn subscription_group_manager(&self) -> &SubscriptionGroupManager<MS> {
        self.subscription_group_manager.as_ref().unwrap()
    }

    #[inline]
    pub fn subscription_group_manager_unchecked(&self) -> &SubscriptionGroupManager<MS> {
        unsafe { self.subscription_group_manager.as_ref().unwrap_unchecked() }
    }

    #[inline]
    pub fn consumer_filter_manager(&self) -> &ConsumerFilterManager {
        self.consumer_filter_manager.as_ref().unwrap()
    }

    #[inline]
    pub fn consumer_filter_manager_unchecked(&self) -> &ConsumerFilterManager {
        unsafe { self.consumer_filter_manager.as_ref().unwrap_unchecked() }
    }

    #[inline]
    pub fn consumer_order_info_manager(&self) -> &ConsumerOrderInfoManager<MS> {
        self.consumer_order_info_manager.as_ref().unwrap()
    }

    #[inline]
    pub fn consumer_order_info_manager_unchecked(&self) -> &ConsumerOrderInfoManager<MS> {
        unsafe { self.consumer_order_info_manager.as_ref().unwrap_unchecked() }
    }

    #[inline]
    pub fn message_store(&self) -> Option<&ArcMut<MS>> {
        self.message_store.as_ref()
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
    pub fn pop_lite_message_processor(&self) -> Option<&ArcMut<PopLiteMessageProcessor<MS>>> {
        self.pop_lite_message_processor.as_ref()
    }

    #[inline]
    pub fn message_store_unchecked(&self) -> &ArcMut<MS> {
        unsafe { self.message_store.as_ref().unwrap_unchecked() }
    }

    #[inline]
    pub fn message_store_unchecked_mut(&mut self) -> &mut ArcMut<MS> {
        unsafe { self.message_store.as_mut().unwrap_unchecked() }
    }

    #[inline]
    pub fn broker_stats(&self) -> Option<&BrokerStats<MS>> {
        self.broker_stats.as_ref()
    }

    #[inline]
    pub fn schedule_message_service(&self) -> &ArcMut<ScheduleMessageService<MS>> {
        self.schedule_message_service.as_ref().unwrap()
    }

    #[inline]
    pub fn schedule_message_service_unchecked(&self) -> &ArcMut<ScheduleMessageService<MS>> {
        unsafe { self.schedule_message_service.as_ref().unwrap_unchecked() }
    }

    #[inline]
    pub fn timer_message_store(&self) -> Option<&Arc<TimerMessageStore>> {
        self.timer_message_store.as_ref().or_else(|| {
            self.message_store
                .as_ref()
                .and_then(|message_store| message_store.get_timer_message_store())
        })
    }

    #[inline]
    pub fn timer_message_store_unchecked(&self) -> &TimerMessageStore {
        self.timer_message_store()
            .map(Arc::as_ref)
            .expect("timer_message_store should be initialized before use")
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
    pub fn broker_stats_manager_unchecked(&self) -> &BrokerStatsManager {
        unsafe { self.broker_stats_manager.as_ref().unwrap_unchecked() }
    }

    #[inline]
    pub fn topic_queue_mapping_clean_service(&self) -> &Option<TopicQueueMappingCleanService> {
        &self.topic_queue_mapping_clean_service
    }

    #[inline]
    pub fn topic_queue_mapping_clean_service_unchecked(&self) -> &TopicQueueMappingCleanService {
        unsafe { self.topic_queue_mapping_clean_service.as_ref().unwrap_unchecked() }
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
        &self.is_isolated
    }

    #[inline]
    pub fn pull_request_hold_service(&self) -> Option<&PullRequestHoldService<MS>> {
        self.pull_request_hold_service.as_ref()
    }

    #[inline]
    pub fn pull_request_hold_service_unchecked(&self) -> &PullRequestHoldService<MS> {
        unsafe { self.pull_request_hold_service.as_ref().unwrap_unchecked() }
    }

    #[inline]
    pub fn rebalance_lock_manager(&self) -> &RebalanceLockManager {
        &self.rebalance_lock_manager
    }

    #[inline]
    pub fn broker_member_group(&self) -> &BrokerMemberGroup {
        &self.broker_member_group
    }

    #[inline]
    pub fn transactional_message_check_listener(&self) -> &Option<DefaultTransactionalMessageCheckListener<MS>> {
        &self.transactional_message_check_listener
    }

    #[inline]
    pub fn transactional_message_check_service(&self) -> &Option<TransactionalMessageCheckService<MS>> {
        &self.transactional_message_check_service
    }

    #[inline]
    pub fn transaction_metrics_flush_service(&self) -> &Option<TransactionMetricsFlushService> {
        &self.transaction_metrics_flush_service
    }

    #[inline]
    pub fn topic_route_info_manager(&self) -> &TopicRouteInfoManager<MS> {
        self.topic_route_info_manager.as_ref().unwrap()
    }

    #[inline]
    pub fn escape_bridge(&self) -> &EscapeBridge<MS> {
        self.escape_bridge.as_ref().unwrap()
    }

    #[inline]
    pub fn pop_inflight_message_counter(&self) -> &PopInflightMessageCounter {
        &self.pop_inflight_message_counter
    }

    #[inline]
    pub fn cold_data_cg_ctr_service(&self) -> Option<&ColdDataCgCtrService> {
        self.cold_data_cg_ctr_service.as_ref()
    }

    #[inline]
    pub fn cold_data_pull_request_hold_service(&self) -> Option<&ColdDataPullRequestHoldService> {
        self.cold_data_pull_request_hold_service.as_ref()
    }

    #[inline]
    pub fn slave_synchronize(&self) -> Option<&SlaveSynchronize<MS>> {
        self.slave_synchronize.as_ref()
    }

    #[inline]
    pub fn slave_synchronize_unchecked(&self) -> &SlaveSynchronize<MS> {
        unsafe { self.slave_synchronize.as_ref().unwrap_unchecked() }
    }

    #[inline]
    pub fn slave_synchronize_mut(&mut self) -> Option<&mut SlaveSynchronize<MS>> {
        self.slave_synchronize.as_mut()
    }

    #[inline]
    pub fn slave_synchronize_mut_unchecked(&mut self) -> &mut SlaveSynchronize<MS> {
        unsafe { self.slave_synchronize.as_mut().unwrap_unchecked() }
    }

    #[inline]
    pub fn update_slave_master_addr(&mut self, master_addr: Option<CheetahString>) {
        if let Some(ref mut slave) = self.slave_synchronize {
            slave.set_master_addr(master_addr.as_ref());
        };
    }

    #[inline]
    pub fn set_store_host(&mut self, store_host: SocketAddr) {
        self.store_host = store_host;
    }

    #[inline]
    pub fn set_broker_config(&mut self, broker_config: BrokerConfig) {
        self.broker_config = Arc::new(broker_config);
    }

    #[inline]
    pub fn set_message_store_config(&mut self, message_store_config: MessageStoreConfig) {
        self.message_store_config = Arc::new(message_store_config);
    }

    #[inline]
    pub fn set_topic_config_manager(&mut self, topic_config_manager: TopicConfigManager<MS>) {
        self.topic_config_manager = Some(topic_config_manager);
    }

    #[inline]
    pub fn set_topic_queue_mapping_manager(&mut self, topic_queue_mapping_manager: TopicQueueMappingManager) {
        self.topic_queue_mapping_manager = topic_queue_mapping_manager;
    }

    #[inline]
    pub fn set_consumer_offset_manager(&mut self, consumer_offset_manager: ConsumerOffsetManager<MS>) {
        self.consumer_offset_manager = consumer_offset_manager;
    }

    #[inline]
    pub fn set_subscription_group_manager(&mut self, subscription_group_manager: SubscriptionGroupManager<MS>) {
        self.subscription_group_manager = Some(subscription_group_manager);
    }

    #[inline]
    pub fn set_consumer_filter_manager(&mut self, consumer_filter_manager: ConsumerFilterManager) {
        self.consumer_filter_manager = Some(consumer_filter_manager);
    }

    #[inline]
    pub fn set_consumer_order_info_manager(&mut self, consumer_order_info_manager: ConsumerOrderInfoManager<MS>) {
        self.consumer_order_info_manager = Some(consumer_order_info_manager);
    }

    #[inline]
    pub fn set_message_store(&mut self, message_store: MS) {
        self.message_store = Some(ArcMut::new(message_store));
    }

    #[inline]
    pub fn set_broker_stats(&mut self, broker_stats: BrokerStats<MS>) {
        self.broker_stats = Some(broker_stats);
    }

    #[inline]
    pub fn set_schedule_message_service(&mut self, schedule_message_service: ScheduleMessageService<MS>) {
        self.schedule_message_service = Some(ArcMut::new(schedule_message_service));
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
    pub fn set_is_isolated(&mut self, is_isolated: Arc<AtomicBool>) {
        self.is_isolated = is_isolated;
    }

    #[inline]
    pub fn set_pull_request_hold_service(&mut self, pull_request_hold_service: PullRequestHoldService<MS>) {
        self.pull_request_hold_service = Some(pull_request_hold_service);
    }

    #[inline]
    pub fn set_rebalance_lock_manager(&mut self, rebalance_lock_manager: RebalanceLockManager) {
        self.rebalance_lock_manager = rebalance_lock_manager;
    }

    #[inline]
    pub fn set_broker_member_group(&mut self, broker_member_group: BrokerMemberGroup) {
        self.broker_member_group = broker_member_group;
    }

    pub fn get_min_broker_id_in_group(&self) -> u64 {
        self.broker_config.broker_identity.broker_id
    }

    #[inline]
    pub fn set_transactional_message_check_listener(
        &mut self,
        transactional_message_check_listener: DefaultTransactionalMessageCheckListener<MS>,
    ) {
        self.transactional_message_check_listener = Some(transactional_message_check_listener);
    }

    #[inline]
    pub fn set_transactional_message_check_service(
        &mut self,
        transactional_message_check_service: TransactionalMessageCheckService<MS>,
    ) {
        self.transactional_message_check_service = Some(transactional_message_check_service);
    }

    #[inline]
    pub fn set_transaction_metrics_flush_service(
        &mut self,
        transaction_metrics_flush_service: TransactionMetricsFlushService,
    ) {
        self.transaction_metrics_flush_service = Some(transaction_metrics_flush_service);
    }

    #[inline]
    pub fn set_topic_route_info_manager(&mut self, topic_route_info_manager: TopicRouteInfoManager<MS>) {
        self.topic_route_info_manager = Some(topic_route_info_manager);
    }

    #[inline]
    pub fn set_escape_bridge(&mut self, escape_bridge: EscapeBridge<MS>) {
        self.escape_bridge = Some(escape_bridge);
    }

    #[inline]
    pub fn set_pop_inflight_message_counter(&mut self, pop_inflight_message_counter: PopInflightMessageCounter) {
        self.pop_inflight_message_counter = pop_inflight_message_counter;
    }

    fn protect_broker(&mut self) {}

    async fn update_namesrv_addr_inner(&mut self) {
        if self.broker_config.fetch_name_srv_addr_by_dns_lookup {
            if let Some(namesrv_addr) = &self.broker_config.namesrv_addr {
                self.broker_outer_api
                    .update_name_server_address_list_by_dns_lookup(namesrv_addr.clone())
                    .await;
            }
        } else if let Some(namesrv_addr) = &self.broker_config.namesrv_addr {
            self.broker_outer_api
                .update_name_server_address_list(namesrv_addr.clone())
                .await;
        }
    }

    fn print_master_and_slave_diff(&self) {
        warn!("print_master_and_slave_diff not implemented");
    }

    /// Register broker to name remoting_server
    pub(crate) async fn register_broker_all_inner(
        &self,
        this: ArcMut<BrokerRuntimeInner<MS>>,
        check_order_config: bool,
        oneway: bool,
        force_register: bool,
    ) {
        let mut topic_config_table = HashMap::new();
        let table = self.topic_config_manager().topic_config_table();
        for topic_config in table.iter() {
            let new_topic_config = if !PermName::is_writeable(self.broker_config.broker_permission)
                || !PermName::is_readable(self.broker_config.broker_permission)
            {
                TopicConfig {
                    topic_name: topic_config.topic_name.clone(),
                    read_queue_nums: topic_config.read_queue_nums,
                    write_queue_nums: topic_config.write_queue_nums,
                    perm: topic_config.perm & self.broker_config.broker_permission,
                    ..TopicConfig::default()
                }
            } else {
                topic_config.as_ref().clone()
            };
            topic_config_table.insert(new_topic_config.topic_name.as_ref().unwrap().clone(), new_topic_config);
        }

        // Handle split registration logic
        if self.broker_config.enable_split_registration
            && topic_config_table.len() as i32 >= self.broker_config.split_registration_size
        {
            let topic_config_wrapper = this
                .topic_config_manager()
                .build_serialize_wrapper(topic_config_table.clone());
            BrokerRuntimeInner::<MS>::do_register_broker_all(
                this.clone(),
                check_order_config,
                oneway,
                topic_config_wrapper,
            )
            .await;
            topic_config_table.clear();
        }

        // Collect topicQueueMappingInfoMap
        let topic_queue_mapping_info_map = self
            .topic_queue_mapping_manager
            .topic_queue_mapping_table
            .clone()
            .iter()
            .map(|kv| {
                (
                    kv.key().clone(),
                    ArcMut::new(TopicQueueMappingDetail::clone_as_mapping_info(kv.value().as_ref())),
                )
            })
            .collect();

        let topic_config_wrapper = this
            .topic_config_manager()
            .build_serialize_wrapper_with_topic_queue_map(topic_config_table, topic_queue_mapping_info_map);

        let should_register = self.broker_config.enable_split_registration
            || force_register
            || need_register(
                &self
                    .broker_outer_api
                    .need_register(
                        self.broker_config.broker_identity.broker_cluster_name.clone(),
                        self.get_broker_addr().clone(),
                        self.broker_config.broker_identity.broker_name.clone(),
                        self.broker_config.broker_identity.broker_id,
                        &topic_config_wrapper,
                        self.broker_config.register_broker_timeout_mills as u64,
                        self.broker_config.is_in_broker_container,
                    )
                    .await,
            );
        if should_register {
            BrokerRuntimeInner::<MS>::do_register_broker_all(this, check_order_config, oneway, topic_config_wrapper)
                .await;
        }
    }

    async fn do_register_broker_all_inner(
        this: ArcMut<BrokerRuntimeInner<MS>>,
        _check_order_config: bool,
        oneway: bool,
        topic_config_wrapper: TopicConfigAndMappingSerializeWrapper,
    ) {
        let cluster_name = this.broker_config.broker_identity.broker_cluster_name.clone();
        let broker_name = this.broker_config.broker_identity.broker_name.clone();
        let broker_addr = CheetahString::from_string(format!(
            "{}:{}",
            this.broker_config.broker_ip1, this.broker_config.broker_server_config.listen_port
        ));
        let broker_id = this.broker_config.broker_identity.broker_id;
        let this_ = this.clone();
        this.broker_outer_api
            .register_broker_all(
                cluster_name,
                broker_addr.clone(),
                broker_name,
                broker_id,
                broker_addr,
                topic_config_wrapper,
                vec![],
                oneway,
                10000,
                false,
                false,
                None,
                Default::default(),
                this_,
            )
            .await;
    }

    #[inline]
    pub fn get_broker_addr(&self) -> &CheetahString {
        &self.broker_addr
    }

    #[inline]
    pub fn get_ha_server_addr(&self) -> CheetahString {
        const LOCALHOST: &str = "127.0.0.1";
        let addr = format!(
            "{}:{}",
            self.broker_config
                .broker_ip2
                .as_ref()
                .unwrap_or(&CheetahString::from_static_str(LOCALHOST)),
            self.message_store_config.ha_listen_port
        );
        CheetahString::from_string(addr)
    }

    fn initialize_controller_mode(&mut self) {
        if self.replicas_manager.is_none() {
            self.replicas_manager = Some(ReplicasManager::new(
                &self.broker_config,
                &self.message_store_config,
                self.broker_addr.clone(),
            ));
        }
        self.is_isolated.store(true, Ordering::Release);
    }

    pub async fn bootstrap_controller_mode(mut this: ArcMut<Self>) {
        let Some(controller_leader) = BrokerRuntimeInner::discover_controller_leader(&this).await else {
            warn!(
                "Skip controller mode bootstrap because controller leader is unavailable, broker={}",
                this.broker_config.broker_identity.get_canonical_name()
            );
            return;
        };

        let broker_config = this.broker_config.clone();
        if let Some(replicas_manager) = this.replicas_manager_mut() {
            replicas_manager.set_controller_leader_address(controller_leader.clone());
            if let Err(error) = replicas_manager.validate_registration_state(&broker_config) {
                warn!("Controller mode registration state is invalid: {}", error);
                return;
            }
        }

        let controller_broker_id =
            match BrokerRuntimeInner::ensure_controller_broker_id(this.clone(), &controller_leader).await {
                Ok(controller_broker_id) => controller_broker_id,
                Err(error) => {
                    warn!("Ensure controller broker id failed: {}", error);
                    return;
                }
            };

        if this.replicas_manager().is_none() {
            warn!("ReplicasManager is not initialized for controller mode bootstrap");
            return;
        }

        let cluster_name = this.broker_config.broker_identity.broker_cluster_name.clone();
        let broker_name = this.broker_config.broker_identity.broker_name.clone();
        let broker_addr = this.get_broker_addr().clone();
        match this
            .broker_outer_api
            .register_broker_to_controller(
                cluster_name.clone(),
                broker_name.clone(),
                controller_broker_id as i64,
                broker_addr,
                &controller_leader,
            )
            .await
        {
            Ok((register_header, sync_state_set)) => {
                if let Some(replicas_manager) = this.replicas_manager_mut() {
                    replicas_manager.mark_registered();
                }
                let sync_state_set = sync_state_set.unwrap_or_default();
                let register_followup = this
                    .replicas_manager()
                    .map(|replicas_manager| {
                        replicas_manager.register_followup(
                            register_header.master_broker_id.and_then(|id| u64::try_from(id).ok()),
                            register_header.master_epoch,
                        )
                    })
                    .unwrap_or(ControllerRegisterFollowup::HeartbeatThenQueryReplicaInfo);
                if register_followup == ControllerRegisterFollowup::ApplyRoleChange {
                    if let Err(error) = BrokerRuntimeInner::apply_controller_role_change(
                        this.clone(),
                        Some(controller_leader.clone()),
                        register_header.master_broker_id.and_then(|id| u64::try_from(id).ok()),
                        register_header.master_address,
                        register_header.master_epoch,
                        register_header.sync_state_set_epoch,
                        sync_state_set,
                    )
                    .await
                    {
                        warn!("Apply controller register result failed: {}", error);
                    }
                    return;
                }
            }
            Err(error) => {
                warn!("Register broker to controller failed: {}", error);
                return;
            }
        }

        if let Err(error) = this.send_heartbeat_to_controller_leader(&controller_leader).await {
            warn!("Send bootstrap heartbeat to controller failed: {}", error);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;

        if let Ok((replica_info_header, sync_state_set)) = this
            .broker_outer_api
            .get_replica_info(&controller_leader, broker_name.clone())
            .await
        {
            let replica_followup = this
                .replicas_manager()
                .map(|replicas_manager| {
                    replicas_manager.replica_info_followup(
                        replica_info_header
                            .master_broker_id
                            .and_then(|id| u64::try_from(id).ok()),
                        replica_info_header.master_epoch,
                    )
                })
                .unwrap_or(ControllerReplicaInfoFollowup::ElectMaster);
            if replica_followup == ControllerReplicaInfoFollowup::ApplyRoleChange
                && BrokerRuntimeInner::apply_controller_replica_info(
                    this.clone(),
                    controller_leader.clone(),
                    replica_info_header
                        .master_broker_id
                        .and_then(|id| u64::try_from(id).ok()),
                    replica_info_header.master_address.map(CheetahString::from_string),
                    replica_info_header.master_epoch,
                    Some(sync_state_set.get_sync_state_set_epoch()),
                    sync_state_set.get_sync_state_set().cloned().unwrap_or_default(),
                )
                .await
            {
                return;
            }
        }

        match this
            .broker_outer_api
            .broker_elect(
                &controller_leader,
                cluster_name,
                broker_name.clone(),
                controller_broker_id as i64,
            )
            .await
        {
            Ok((elect_header, sync_state_set)) => {
                if let Err(error) = BrokerRuntimeInner::apply_controller_role_change(
                    this,
                    Some(controller_leader),
                    elect_header.master_broker_id.and_then(|id| u64::try_from(id).ok()),
                    elect_header.master_address,
                    elect_header.master_epoch,
                    elect_header.sync_state_set_epoch,
                    sync_state_set,
                )
                .await
                {
                    warn!("Apply controller elect-master result failed: {}", error);
                }
            }
            Err(error) => {
                if let Ok((replica_info_header, sync_state_set)) = this
                    .broker_outer_api
                    .get_replica_info(&controller_leader, broker_name.clone())
                    .await
                {
                    if BrokerRuntimeInner::apply_controller_replica_info(
                        this,
                        controller_leader,
                        replica_info_header
                            .master_broker_id
                            .and_then(|id| u64::try_from(id).ok()),
                        replica_info_header.master_address.map(CheetahString::from_string),
                        replica_info_header.master_epoch,
                        Some(sync_state_set.get_sync_state_set_epoch()),
                        sync_state_set.get_sync_state_set().cloned().unwrap_or_default(),
                    )
                    .await
                    {
                        return;
                    }
                }
                warn!("Elect master during controller mode bootstrap failed: {}", error);
            }
        }
    }

    async fn apply_controller_replica_info(
        this: ArcMut<Self>,
        controller_leader: CheetahString,
        master_broker_id: Option<u64>,
        master_address: Option<CheetahString>,
        master_epoch: Option<i32>,
        sync_state_set_epoch: Option<i32>,
        sync_state_set: HashSet<i64>,
    ) -> bool {
        if master_broker_id.is_none() || master_epoch.is_none() {
            return false;
        }

        if let Err(error) = BrokerRuntimeInner::apply_controller_role_change(
            this,
            Some(controller_leader),
            master_broker_id,
            master_address,
            master_epoch,
            sync_state_set_epoch,
            sync_state_set,
        )
        .await
        {
            warn!("Apply controller replica info failed: {}", error);
            return false;
        }

        true
    }

    async fn ensure_controller_broker_id(
        mut this: ArcMut<Self>,
        controller_leader: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<u64> {
        let cluster_name = this.broker_config.broker_identity.broker_cluster_name.clone();
        let broker_name = this.broker_config.broker_identity.broker_name.clone();
        let broker_config = this.broker_config.clone();
        let next_broker_id = if this
            .replicas_manager()
            .is_some_and(|replicas_manager| replicas_manager.needs_broker_id_application())
            && this
                .replicas_manager()
                .is_some_and(|replicas_manager| replicas_manager.pending_registration().is_none())
        {
            let next_broker_id_response = this
                .broker_outer_api
                .get_next_broker_id(cluster_name.clone(), broker_name.clone(), controller_leader)
                .await?;
            Some(next_broker_id_response.next_broker_id.ok_or_else(|| {
                rocketmq_error::RocketMQError::illegal_argument(
                    "controller get_next_broker_id returned empty next_broker_id",
                )
            })?)
        } else {
            None
        };
        let action = this
            .replicas_manager_mut()
            .ok_or_else(|| {
                rocketmq_error::RocketMQError::illegal_argument(
                    "replicas manager missing while preparing controller broker id",
                )
            })?
            .prepare_controller_broker_id_action(&broker_config, next_broker_id)?;

        let (broker_id, register_check_code) = match action {
            ControllerBrokerIdAction::UseCurrent(broker_id) => return Ok(broker_id),
            ControllerBrokerIdAction::ApplyBrokerId {
                broker_id,
                register_check_code,
            } => (broker_id, register_check_code),
        };

        if let Err(error) = this
            .broker_outer_api
            .apply_broker_id(
                cluster_name,
                broker_name,
                broker_id as i64,
                register_check_code,
                controller_leader,
            )
            .await
        {
            if let Some(replicas_manager) = this.replicas_manager_mut() {
                let _ = replicas_manager.clear_temp_metadata();
            }
            return Err(error);
        }

        let replicas_manager = this.replicas_manager_mut().ok_or_else(|| {
            rocketmq_error::RocketMQError::illegal_argument(
                "replicas manager missing while committing controller broker id",
            )
        })?;
        replicas_manager.complete_controller_broker_id_application(&broker_config)
    }

    async fn discover_controller_leader(this: &ArcMut<Self>) -> Option<CheetahString> {
        let targets = this
            .replicas_manager()
            .map(|replicas_manager| replicas_manager.heartbeat_targets())
            .unwrap_or_default();
        for address in targets {
            match this.broker_outer_api.get_controller_metadata(&address).await {
                Ok(metadata) => {
                    if let Some(controller_leader_address) = metadata.controller_leader_address {
                        return Some(controller_leader_address);
                    }
                    return Some(address);
                }
                Err(error) => {
                    warn!("Discover controller leader failed via {}: {}", address, error);
                }
            }
        }
        None
    }

    async fn refresh_controller_leader(mut this: ArcMut<Self>) -> Option<CheetahString> {
        let controller_leader = BrokerRuntimeInner::discover_controller_leader(&this).await?;
        if let Some(replicas_manager) = this.replicas_manager_mut() {
            replicas_manager.set_controller_leader_address(controller_leader.clone());
        }
        Some(controller_leader)
    }

    async fn sync_controller_replica_info(this: ArcMut<Self>) {
        if !this.broker_config.enable_controller_mode || this.is_isolated.load(Ordering::Acquire) {
            return;
        }

        let controller_leader = if let Some(controller_leader) = this
            .replicas_manager()
            .and_then(|replicas_manager| replicas_manager.controller_leader_address().cloned())
        {
            controller_leader
        } else if let Some(controller_leader) = BrokerRuntimeInner::refresh_controller_leader(this.clone()).await {
            controller_leader
        } else {
            return;
        };

        let broker_name = this.broker_config.broker_identity.broker_name.clone();
        match BrokerRuntimeInner::fetch_controller_replica_info(this.clone(), &controller_leader, broker_name.clone())
            .await
        {
            Ok((leader, response_header, sync_state_set_body)) => {
                let sync_state_set_epoch = sync_state_set_body.get_sync_state_set_epoch();
                let sync_state_set = sync_state_set_body.get_sync_state_set().cloned().unwrap_or_default();
                let sync_followup = this
                    .replicas_manager()
                    .map(|replicas_manager| {
                        replicas_manager.replica_sync_followup(
                            response_header.master_broker_id.and_then(|id| u64::try_from(id).ok()),
                            response_header.master_epoch,
                        )
                    })
                    .unwrap_or(ControllerReplicaSyncFollowup::Bootstrap);
                if sync_followup == ControllerReplicaSyncFollowup::ApplyRoleChange {
                    if let Err(error) = BrokerRuntimeInner::apply_controller_role_change(
                        this,
                        Some(leader),
                        response_header.master_broker_id.and_then(|id| u64::try_from(id).ok()),
                        response_header.master_address.map(CheetahString::from),
                        response_header.master_epoch,
                        Some(sync_state_set_epoch),
                        sync_state_set,
                    )
                    .await
                    {
                        warn!("Apply controller replica info failed: {}", error);
                    }
                } else {
                    BrokerRuntimeInner::bootstrap_controller_mode(this).await;
                }
            }
            Err(rocketmq_error::RocketMQError::BrokerOperationFailed { code, .. })
                if this
                    .replicas_manager()
                    .map(|replicas_manager| {
                        replicas_manager.replica_sync_error_followup(Some(code))
                            == ControllerReplicaSyncFollowup::Bootstrap
                    })
                    .unwrap_or(true) =>
            {
                BrokerRuntimeInner::bootstrap_controller_mode(this).await;
            }
            Err(error) => {
                warn!("Sync controller replica info failed: {}", error);
            }
        }
    }

    async fn fetch_controller_replica_info(
        this: ArcMut<Self>,
        controller_leader: &CheetahString,
        broker_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<(
        CheetahString,
        rocketmq_remoting::protocol::header::controller::get_replica_info_response_header::GetReplicaInfoResponseHeader,
        rocketmq_remoting::protocol::body::sync_state_set_body::SyncStateSet,
    )> {
        match this
            .broker_outer_api
            .get_replica_info(controller_leader, broker_name.clone())
            .await
        {
            Ok(result) => Ok((controller_leader.clone(), result.0, result.1)),
            Err(error) => {
                let Some(refreshed_controller_leader) =
                    BrokerRuntimeInner::refresh_controller_leader(this.clone()).await
                else {
                    return Err(error);
                };

                if refreshed_controller_leader == *controller_leader {
                    return Err(error);
                }

                this.broker_outer_api
                    .get_replica_info(&refreshed_controller_leader, broker_name)
                    .await
                    .map(|(response_header, sync_state_set)| {
                        (refreshed_controller_leader, response_header, sync_state_set)
                    })
            }
        }
    }

    async fn sync_broker_member_group(this: &ArcMut<Self>) {
        let broker_cluster_name = &this.broker_config.broker_identity.broker_cluster_name;
        let broker_name = &this.broker_config.broker_identity.broker_name;
        let compatible_with_old_name_srv = this.broker_config.compatible_with_old_name_srv;
        let broker_member_group = this
            .broker_outer_api
            .sync_broker_member_group(broker_cluster_name, broker_name, compatible_with_old_name_srv)
            .await;

        if let Err(ref e) = broker_member_group {
            error!("syncBrokerMemberGroup from namesrv failed, error={}", e);
            return;
        }
        let broker_member_group = broker_member_group.unwrap();
        if broker_member_group.is_none() || broker_member_group.as_ref().unwrap().broker_addrs.is_empty() {
            warn!(
                "Couldn't find any broker member from namesrv in {}/{}",
                broker_cluster_name, broker_name
            );
            return;
        }
        fn calc_alive_broker_num_in_group(
            broker_addr_table: &HashMap<u64 /* brokerId */, CheetahString /* broker address */>,
            broker_id: u64,
        ) -> usize {
            if broker_addr_table.contains_key(&broker_id) {
                broker_addr_table.len()
            } else {
                broker_addr_table.len() + 1
            }
        }
        let broker_member_group = broker_member_group.unwrap();
        this.message_store_unchecked()
            .set_alive_replica_num_in_group(calc_alive_broker_num_in_group(
                &broker_member_group.broker_addrs,
                this.broker_config.broker_identity.broker_id,
            ) as i32);
        if !this.is_isolated.load(Ordering::Acquire) {
            let min_broker_id = broker_member_group.minimum_broker_id();
            let min_broker_addr = broker_member_group.broker_addrs.get(&min_broker_id).cloned();
            BrokerRuntimeInner::update_min_broker(this, min_broker_id, min_broker_addr).await;
        }
    }

    pub async fn update_min_broker(this: &ArcMut<Self>, min_broker_id: u64, min_broker_addr: Option<CheetahString>) {
        if this.broker_config.enable_slave_acting_master && this.broker_config.broker_identity.broker_id != MASTER_ID {
            let mut this_clone = this.clone();
            if let Ok(lock) = this.lock.try_lock() {
                let min_broker_id_in_group = this.min_broker_id_in_group.load(Ordering::SeqCst);
                if min_broker_id != min_broker_id_in_group {
                    let mut offline_broker_addr = None;
                    if min_broker_id > min_broker_id_in_group {
                        offline_broker_addr = this.min_broker_addr_in_group.lock().await.clone();
                    }
                    this_clone
                        .on_min_broker_change(min_broker_id, min_broker_addr, offline_broker_addr, None)
                        .await;
                }
                drop(lock);
            }
        }
    }

    pub fn pop_message_processor(&self) -> Option<&ArcMut<PopMessageProcessor<MS>>> {
        self.pop_message_processor.as_ref()
    }

    pub fn pop_message_processor_unchecked(&self) -> &ArcMut<PopMessageProcessor<MS>> {
        unsafe { self.pop_message_processor.as_ref().unwrap_unchecked() }
    }

    pub fn ack_message_processor_unchecked(&self) -> &ArcMut<AckMessageProcessor<MS>> {
        unsafe { self.ack_message_processor.as_ref().unwrap_unchecked() }
    }

    pub fn notification_processor_unchecked(&self) -> &ArcMut<NotificationProcessor<MS>> {
        unsafe { self.notification_processor.as_ref().unwrap_unchecked() }
    }

    pub fn query_assignment_processor_unchecked(&self) -> &ArcMut<QueryAssignmentProcessor<MS>> {
        unsafe { self.query_assignment_processor.as_ref().unwrap_unchecked() }
    }

    pub fn query_assignment_processor(&self) -> Option<&ArcMut<QueryAssignmentProcessor<MS>>> {
        self.query_assignment_processor.as_ref()
    }

    pub fn query_assignment_processor_mut(&mut self) -> Option<&mut ArcMut<QueryAssignmentProcessor<MS>>> {
        self.query_assignment_processor.as_mut()
    }

    pub fn query_assignment_processor_unchecked_mut(&mut self) -> &mut ArcMut<QueryAssignmentProcessor<MS>> {
        unsafe { self.query_assignment_processor.as_mut().unwrap_unchecked() }
    }

    pub async fn change_special_service_status(&mut self, should_start: bool) {
        for plugin in self.broker_attached_plugins.iter() {
            plugin.status_changed(should_start);
        }
        self.change_schedule_service_status(should_start);
        self.change_transaction_check_service_status(should_start).await;

        if let Some(ack_message_processor) = &mut self.ack_message_processor {
            info!("Set PopReviveService Status to {}", should_start);
            ack_message_processor.set_pop_revive_service_status(should_start);
        }
    }

    pub fn change_schedule_service_status(&mut self, should_start: bool) {
        if self.is_schedule_service_start.load(Ordering::Relaxed) != should_start {
            info!("change_schedule_service_status changed to {}", should_start);
            if should_start {
                if let Some(schedule_message_service) = &self.schedule_message_service {
                    let _ = ScheduleMessageService::start(schedule_message_service.clone());
                }
            } else if let Some(schedule_message_service) = &mut self.schedule_message_service {
                schedule_message_service.stop();
            }

            self.is_schedule_service_start.store(should_start, Ordering::Release);

            if let Some(timer) = &mut self.timer_message_store {
                timer.sync_last_read_time_ms();
                timer.set_should_running_dequeue(should_start);
            }
        }
    }

    pub async fn change_transaction_check_service_status(&mut self, should_start: bool) {
        if self.is_transaction_check_service_start.load(Ordering::Relaxed) != should_start {
            info!("TransactionCheckService status changed to {}", should_start);
            if should_start {
                if let Some(transactional_message_check_service) = &mut self.transactional_message_check_service {
                    transactional_message_check_service.start().await;
                }
            } else if let Some(transactional_message_check_service) = &mut self.transactional_message_check_service {
                transactional_message_check_service.shutdown_interrupt(true).await;
            }

            self.is_transaction_check_service_start
                .store(should_start, Ordering::Release);
        }
    }

    pub async fn start_service(
        mut this: ArcMut<BrokerRuntimeInner<MS>>,
        min_broker_id: u64,
        min_broker_addr: Option<CheetahString>,
    ) {
        info!(
            "{} start service, min broker id is {}, min broker addr: {:?}",
            this.broker_config.broker_identity.get_canonical_name(),
            min_broker_id,
            min_broker_addr
        );

        this.min_broker_id_in_group.store(min_broker_id, Ordering::SeqCst);
        let mut guard = this.min_broker_addr_in_group.lock().await;
        *guard = min_broker_addr;
        drop(guard);
        let flag = this.broker_config.broker_identity.broker_id == min_broker_id;
        this.change_special_service_status(flag).await;
        let this_clone = this.clone();
        this.register_broker_all_inner(this_clone, true, false, this.broker_config.force_register)
            .await;
        this.is_isolated.store(false, Ordering::SeqCst);
    }

    pub async fn apply_controller_role_change(
        mut this: ArcMut<BrokerRuntimeInner<MS>>,
        controller_leader_address: Option<CheetahString>,
        new_master_broker_id: Option<u64>,
        new_master_address: Option<CheetahString>,
        new_master_epoch: Option<i32>,
        sync_state_set_epoch: Option<i32>,
        sync_state_set: HashSet<i64>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let outcome = {
            let replicas_manager = this.replicas_manager_mut().ok_or_else(|| {
                rocketmq_error::RocketMQError::illegal_argument(
                    "controller mode role change received before replicas manager initialization",
                )
            })?;
            replicas_manager.change_broker_role(
                controller_leader_address,
                new_master_broker_id,
                new_master_address,
                new_master_epoch,
                sync_state_set_epoch,
                Some(&sync_state_set),
            )?
        };

        let Some(role) = outcome.role else {
            if let Some(message_store) = this.message_store.as_ref() {
                message_store.sync_controller_sync_state_set(outcome.local_broker_id as i64, &outcome.sync_state_set);
            }
            return Ok(());
        };

        if let Some(message_store) = this.message_store.as_ref() {
            message_store.sync_controller_sync_state_set(outcome.local_broker_id as i64, &outcome.sync_state_set);
        }
        let previous_store_role = this.message_store_config.broker_role;
        if let Some(target_broker_role) = outcome.target_broker_role() {
            Arc::make_mut(&mut this.message_store_config).broker_role = target_broker_role;
        }
        Arc::make_mut(&mut this.broker_config).broker_identity.broker_id = outcome.local_broker_id;

        match role {
            BrokerReplicaRole::Master => {
                this.apply_message_store_role_change(
                    previous_store_role,
                    BrokerReplicaRole::Master,
                    outcome.local_broker_id,
                    None,
                    outcome.master_epoch,
                )
                .await?;
                this.change_special_service_status(outcome.should_start_special_service)
                    .await;
                if outcome.should_clear_slave_master_address() {
                    if let Some(slave_synchronize) = this.slave_synchronize_mut() {
                        slave_synchronize.set_master_addr(None);
                    }
                }
            }
            BrokerReplicaRole::Slave => {
                this.apply_message_store_role_change(
                    previous_store_role,
                    BrokerReplicaRole::Slave,
                    outcome.local_broker_id,
                    outcome.slave_master_address(),
                    outcome.master_epoch,
                )
                .await?;
                this.change_special_service_status(outcome.should_start_special_service)
                    .await;
                if let Some(master_address) = outcome.slave_master_address() {
                    if let Some(slave_synchronize) = this.slave_synchronize_mut() {
                        slave_synchronize.set_master_addr(Some(master_address));
                    }
                    if outcome.should_sync_master_online() && this.message_store.is_some() {
                        this.on_master_on_line(Some(master_address.clone()), None).await;
                    }
                }
            }
        }

        this.is_isolated.store(false, Ordering::Release);
        if outcome.should_register_to_namesrv {
            let this_clone = this.clone();
            this.register_broker_all_inner(this_clone, true, false, this.broker_config.force_register)
                .await;
        }
        Ok(())
    }

    async fn apply_message_store_role_change(
        &mut self,
        previous_store_role: BrokerRole,
        target_role: BrokerReplicaRole,
        controller_broker_id: u64,
        master_address: Option<&CheetahString>,
        master_epoch: i32,
    ) -> rocketmq_error::RocketMQResult<()> {
        let Some(message_store) = self.message_store.as_ref() else {
            return Ok(());
        };
        let Some(ha_service) = message_store.get_ha_service() else {
            return Ok(());
        };

        let result = match target_role {
            BrokerReplicaRole::Master => {
                if previous_store_role == BrokerRole::SyncMaster {
                    ha_service.change_to_master_when_last_role_is_master(master_epoch).await
                } else {
                    ha_service.change_to_master(master_epoch).await
                }
            }
            BrokerReplicaRole::Slave => {
                let master_address = master_address.ok_or_else(|| {
                    rocketmq_error::RocketMQError::illegal_argument(
                        "controller role change missing master address for store transition",
                    )
                })?;
                let current_master_address = ha_service.get_runtime_info(0).ha_client_runtime_info.master_addr;
                if previous_store_role == BrokerRole::Slave && current_master_address == master_address.as_str() {
                    ha_service
                        .change_to_slave_when_master_not_change(master_address.as_str(), master_epoch)
                        .await
                } else {
                    ha_service
                        .change_to_slave(master_address.as_str(), master_epoch, Some(controller_broker_id as i64))
                        .await
                }
            }
        };

        result.map_err(|error| {
            rocketmq_error::RocketMQError::illegal_argument(format!(
                "apply controller role change to message store failed: {}",
                error
            ))
        })?;
        if let Some(message_store) = self.message_store.as_mut() {
            message_store.sync_broker_role(match target_role {
                BrokerReplicaRole::Master => BrokerRole::SyncMaster,
                BrokerReplicaRole::Slave => BrokerRole::Slave,
            });
        }
        Ok(())
    }

    async fn on_min_broker_change(
        &mut self,
        min_broker_id: u64,
        min_broker_addr: Option<CheetahString>,
        offline_broker_addr: Option<CheetahString>,
        master_ha_addr: Option<CheetahString>,
    ) {
        let min_broker_id_in_group_old = self.min_broker_id_in_group.load(Ordering::SeqCst);
        let mut min_broker_addr_in_group_old = self.min_broker_addr_in_group.lock().await;
        info!(
            "Min broker changed, old: {}-{:?}, new {}-{:?}",
            min_broker_id_in_group_old, min_broker_addr_in_group_old, min_broker_id, min_broker_addr
        );
        self.min_broker_id_in_group.store(min_broker_id, Ordering::SeqCst);
        *min_broker_addr_in_group_old = min_broker_addr.clone();
        drop(min_broker_addr_in_group_old);
        self.change_special_service_status(
            self.broker_config.broker_identity.broker_id == self.min_broker_id_in_group.load(Ordering::SeqCst),
        )
        .await;
        if offline_broker_addr.is_some()
            && offline_broker_addr.as_ref() == self.slave_synchronize().unwrap().master_addr()
        {
            //master offline
            self.on_master_offline().await;
        }

        if min_broker_id == MASTER_ID && min_broker_addr.is_some() {
            //master online
            self.on_master_on_line(min_broker_addr, master_ha_addr).await;
        }

        // notify PullRequest on hold to pull from master.
        if self.min_broker_id_in_group.load(Ordering::SeqCst) == MASTER_ID {
            self.pull_request_hold_service_unchecked().notify_master_online().await;
        }
    }

    async fn on_master_on_line(
        &mut self,
        min_broker_addr: Option<CheetahString>,
        master_ha_addr: Option<CheetahString>,
    ) {
        let need_sync_master_flush_offset = self.message_store_unchecked().get_master_flushed_offset() == 0
            && self.message_store_config.all_ack_in_sync_state_set;
        if master_ha_addr.is_none() || need_sync_master_flush_offset {
            match self
                .broker_outer_api
                .retrieve_broker_ha_info(min_broker_addr.as_ref())
                .await
            {
                Ok(broker_sync_info) => {
                    if need_sync_master_flush_offset {
                        info!(
                            "Set master flush offset in slave to {}",
                            broker_sync_info.master_flush_offset,
                        );
                        self.message_store_unchecked()
                            .set_master_flushed_offset(broker_sync_info.master_flush_offset);
                    }
                    if master_ha_addr.is_none() {
                        let message_store = self.message_store_unchecked();
                        if let Some(master_ha_address) = &broker_sync_info.master_ha_address {
                            message_store.update_ha_master_address(master_ha_address.as_str()).await;
                        }
                        if let Some(master_address) = &broker_sync_info.master_address {
                            message_store.update_master_address(master_address);
                        }
                    }
                }
                Err(e) => {
                    error!("retrieve master ha info exception, {}", e);
                }
            }
        }
        if let Some(master_ha_addr_) = master_ha_addr {
            self.message_store_unchecked_mut()
                .update_ha_master_address(master_ha_addr_.as_str())
                .await;
        }
        self.message_store_unchecked().wakeup_ha_client();
    }

    async fn on_master_offline(&mut self) {
        let slave_synchronize = self.slave_synchronize_unchecked();
        if let Some(master_addr) = slave_synchronize.master_addr() {
            if !master_addr.is_empty() {
                //close channels
            }
        }
        self.slave_synchronize_mut_unchecked().set_master_addr(None);
        self.message_store_unchecked_mut().update_ha_master_address("").await
    }

    async fn send_heartbeat(&self) {
        let Some(replicas_manager) = self.replicas_manager() else {
            return;
        };
        let heartbeat_state = replicas_manager.controller_heartbeat_state();
        let controller_targets = heartbeat_state.controller_targets;
        if controller_targets.is_empty() {
            warn!(
                "Skip controller heartbeat because no controller address is configured, broker={}",
                self.broker_config.broker_identity.get_canonical_name()
            );
            return;
        }

        let cluster_name = self.broker_config.broker_identity.broker_cluster_name.clone();
        let broker_addr = self.get_broker_addr().clone();
        let broker_name = self.broker_config.broker_identity.broker_name.clone();
        let broker_id = heartbeat_state.broker_id;
        let epoch = heartbeat_state.epoch;
        let max_offset = self
            .message_store
            .as_ref()
            .map(|message_store| message_store.get_max_phy_offset());
        let confirm_offset = self
            .message_store
            .as_ref()
            .map(|message_store| message_store.get_confirm_offset());

        for controller_address in controller_targets {
            self.broker_outer_api
                .send_heartbeat_to_controller(
                    controller_address,
                    cluster_name.clone(),
                    broker_addr.clone(),
                    broker_name.clone(),
                    broker_id,
                    self.broker_config.send_heartbeat_timeout_millis,
                    epoch,
                    max_offset,
                    confirm_offset,
                    Some(self.broker_config.controller_heartbeat_timeout_mills),
                    Some(self.broker_config.broker_election_priority),
                )
                .await;
        }
    }

    async fn send_heartbeat_to_controller_leader(
        &self,
        controller_leader: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let heartbeat_state = self
            .replicas_manager()
            .map(|replicas_manager| replicas_manager.controller_heartbeat_state())
            .ok_or_else(|| {
                rocketmq_error::RocketMQError::illegal_argument(
                    "replicas manager missing while sending controller leader heartbeat",
                )
            })?;
        let max_offset = self
            .message_store
            .as_ref()
            .map(|message_store| message_store.get_max_phy_offset());
        let confirm_offset = self
            .message_store
            .as_ref()
            .map(|message_store| message_store.get_confirm_offset());

        self.broker_outer_api
            .send_heartbeat_to_controller_sync(
                controller_leader,
                self.broker_config.broker_identity.broker_cluster_name.clone(),
                self.get_broker_addr().clone(),
                self.broker_config.broker_identity.broker_name.clone(),
                heartbeat_state.broker_id,
                self.broker_config.send_heartbeat_timeout_millis,
                heartbeat_state.epoch,
                max_offset,
                confirm_offset,
                Some(self.broker_config.controller_heartbeat_timeout_mills),
                Some(self.broker_config.broker_election_priority),
            )
            .await
    }
}

fn need_register(change_list: &[bool]) -> bool {
    change_list.iter().copied().any(|changed| changed)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::net::TcpListener;
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU16;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::controller::replicas_manager::RegisterState;
    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_client_rust::producer::producer_impl::topic_publish_info::TopicPublishInfo;
    use rocketmq_common::common::attribute::subscription_group_attributes::LITE_BIND_TOPIC_ATTRIBUTE_NAME;
    use rocketmq_common::common::attribute::Attribute;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_common::common::entity::ClientGroup;
    use rocketmq_common::common::lite::to_lmq_name;
    use rocketmq_common::common::message::message_decoder;
    use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
    use rocketmq_common::common::message::message_queue::MessageQueue;
    use rocketmq_common::common::message::MessageConst;
    use rocketmq_common::common::message::MessageTrait;
    use rocketmq_common::common::mix_all::MASTER_ID;
    use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
    use rocketmq_common::common::server::config::ServerConfig;
    use rocketmq_common::TimeUtils::current_millis;
    use rocketmq_common::TopicAttributes;
    use rocketmq_controller::config::RaftPeer;
    use rocketmq_controller::config::StorageBackendType;
    use rocketmq_controller::controller::broker_heartbeat_manager::BrokerHeartbeatManager;
    use rocketmq_controller::typ::Node;
    use rocketmq_controller::Controller;
    use rocketmq_controller::ControllerConfig as TestControllerConfig;
    use rocketmq_controller::ControllerManager as TestControllerManager;
    use rocketmq_namesrv::bootstrap::Builder as NameServerBuilder;
    use rocketmq_remoting::base::response_future::ResponseFuture;
    use rocketmq_remoting::clients::rocketmq_tokio_client::RocketmqDefaultClient;
    use rocketmq_remoting::clients::RemotingClient;
    use rocketmq_remoting::code::request_code::RequestCode;
    use rocketmq_remoting::code::response_code::ResponseCode;
    use rocketmq_remoting::connection::Connection;
    use rocketmq_remoting::net::channel::Channel;
    use rocketmq_remoting::net::channel::ChannelInner;
    use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
    use rocketmq_remoting::protocol::body::broker_body::broker_member_group::GetBrokerMemberGroupResponseBody;
    use rocketmq_remoting::protocol::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;
    use rocketmq_remoting::protocol::body::get_lite_client_info_response_body::GetLiteClientInfoResponseBody;
    use rocketmq_remoting::protocol::body::get_lite_group_info_response_body::GetLiteGroupInfoResponseBody;
    use rocketmq_remoting::protocol::body::get_lite_topic_info_response_body::GetLiteTopicInfoResponseBody;
    use rocketmq_remoting::protocol::body::get_parent_topic_info_response_body::GetParentTopicInfoResponseBody;
    use rocketmq_remoting::protocol::header::controller::apply_broker_id_request_header::ApplyBrokerIdRequestHeader;
    use rocketmq_remoting::protocol::header::empty_header::EmptyHeader;
    use rocketmq_remoting::protocol::header::get_lite_client_info_request_header::GetLiteClientInfoRequestHeader;
    use rocketmq_remoting::protocol::header::get_lite_group_info_request_header::GetLiteGroupInfoRequestHeader;
    use rocketmq_remoting::protocol::header::get_lite_topic_info_request_header::GetLiteTopicInfoRequestHeader;
    use rocketmq_remoting::protocol::header::get_parent_topic_info_request_header::GetParentTopicInfoRequestHeader;
    use rocketmq_remoting::protocol::header::namesrv::broker_request::GetBrokerMemberGroupRequestHeader;
    use rocketmq_remoting::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
    use rocketmq_remoting::protocol::header::pop_lite_message_response_header::PopLiteMessageResponseHeader;
    use rocketmq_remoting::protocol::header::trigger_lite_dispatch_request_header::TriggerLiteDispatchRequestHeader;
    use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
    use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
    use rocketmq_remoting::protocol::RemotingDeserializable;
    use rocketmq_remoting::remoting::RemotingService;
    use rocketmq_remoting::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
    use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
    use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
    use rocketmq_remoting::runtime::processor::RequestProcessor;
    use rocketmq_store::base::message_status_enum::GetMessageStatus;
    use rocketmq_store::base::message_store::MessageStore;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use rocketmq_store::message_store::local_file_message_store::LocalFileMessageStore;
    use rocketmq_store::queue::consume_queue_store::ConsumeQueueStoreTrait;
    use rocketmq_store::timer::timer_message_store::TimerMessageStore;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio::time::sleep;

    use super::*;

    const CONTROLLER_TEST_PORT_BLOCK_SIZE: u16 = 128;
    static NEXT_CONTROLLER_TEST_BASE_PORT: AtomicU16 = AtomicU16::new(20_000);

    struct TestNameServer {
        addr: CheetahString,
        shutdown_tx: Option<oneshot::Sender<()>>,
        handle: Option<JoinHandle<()>>,
    }

    impl TestNameServer {
        fn addr(&self) -> CheetahString {
            self.addr.clone()
        }

        async fn shutdown(&mut self) {
            if let Some(shutdown_tx) = self.shutdown_tx.take() {
                let _ = shutdown_tx.send(());
            }
            if let Some(handle) = self.handle.take() {
                match tokio::time::timeout(Duration::from_secs(15), handle).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => panic!("namesrv task should not panic: {error}"),
                    Err(_) => panic!("timed out waiting for namesrv shutdown"),
                }
            }
        }
    }

    fn new_controller_mode_message_store(
        root: &Path,
        broker_config: Arc<BrokerConfig>,
        message_store_config: Arc<MessageStoreConfig>,
    ) -> ArcMut<LocalFileMessageStore> {
        std::fs::create_dir_all(root).expect("create temp store dir");
        let topic_table: Arc<DashMap<CheetahString, ArcMut<TopicConfig>>> = Arc::new(DashMap::new());
        let mut store = ArcMut::new(LocalFileMessageStore::new(
            message_store_config,
            broker_config,
            topic_table,
            None,
            false,
        ));
        let store_clone = store.clone();
        store.set_message_store_arc(store_clone);
        store
    }

    fn controller_addr_list(peers: &[RaftPeer]) -> CheetahString {
        CheetahString::from_string(
            peers
                .iter()
                .map(|peer| peer.addr.to_string())
                .collect::<Vec<String>>()
                .join(";"),
        )
    }

    fn controller_cluster_root(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("rocketmq-rust-{prefix}-{}", current_millis()))
    }

    fn allocate_controller_test_base_port() -> u16 {
        loop {
            let base_port =
                NEXT_CONTROLLER_TEST_BASE_PORT.fetch_add(CONTROLLER_TEST_PORT_BLOCK_SIZE, Ordering::Relaxed);
            let required_ports = [
                base_port + 1,
                base_port + 2,
                base_port + 3,
                base_port + 11,
                base_port + 12,
                base_port + 13,
                base_port + 21,
                base_port + 22,
                base_port + 31,
                base_port + 32,
                base_port + 90,
            ];
            if required_ports
                .into_iter()
                .all(|port| TcpListener::bind(("127.0.0.1", port)).is_ok())
            {
                return base_port;
            }
        }
    }

    async fn create_test_channel() -> Channel {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local test listener");
        let local_addr = listener.local_addr().expect("local listener addr");
        let std_stream = std::net::TcpStream::connect(local_addr).expect("connect local test listener");
        std_stream.set_nonblocking(true).expect("set nonblocking");
        drop(listener);
        let tcp_stream = tokio::net::TcpStream::from_std(std_stream).expect("convert tcp stream");
        let connection = Connection::new(tcp_stream);
        let response_table = ArcMut::new(HashMap::<i32, ResponseFuture>::new());
        let inner = ArcMut::new(ChannelInner::new(connection, response_table));
        Channel::new(inner, local_addr, local_addr)
    }

    fn lite_test_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "rocketmq-rust-broker-lite-manager-{label}-{}",
            current_millis()
        ))
    }

    async fn new_lite_test_runtime(label: &str) -> BrokerRuntime {
        let temp_root = lite_test_root(label);
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            enable_lmq: true,
            enable_multi_dispatch: true,
            max_lmq_consume_queue_num: 32,
            read_uncommitted: true,
            ..MessageStoreConfig::default()
        });
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        assert!(runtime.initialize().await);
        runtime
    }

    fn seed_lite_query_state(runtime: &mut BrokerRuntime) {
        let inner = runtime.inner_for_test();
        let mut topic_config = TopicConfig::with_queues("parent-topic", 1, 1);
        topic_config.attributes.insert(
            CheetahString::from_string(format!(
                "+{}",
                TopicAttributes::TopicAttributes::topic_message_type_attribute().name()
            )),
            CheetahString::from_static_str("LITE"),
        );
        inner
            .topic_config_manager_mut()
            .update_topic_config(ArcMut::new(topic_config));

        for group in ["group-a", "group-b"] {
            let mut config = SubscriptionGroupConfig::new(CheetahString::from_static_str(group));
            config.set_attributes(HashMap::from([(
                CheetahString::from_string(format!("+{LITE_BIND_TOPIC_ATTRIBUTE_NAME}")),
                CheetahString::from_static_str("parent-topic"),
            )]));
            inner
                .subscription_group_manager_mut()
                .update_subscription_group_config(&mut config);
        }

        let registry = inner.lite_subscription_registry();
        let client_one = CheetahString::from_static_str("client-1");
        let client_two = CheetahString::from_static_str("client-2");
        let parent_topic = CheetahString::from_static_str("parent-topic");
        let group_a = CheetahString::from_static_str("group-a");
        let group_b = CheetahString::from_static_str("group-b");

        registry.add_complete_subscription(
            &client_one,
            &group_a,
            &parent_topic,
            &HashSet::from([
                CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("child-a lmq")),
                CheetahString::from_string(to_lmq_name("parent-topic", "child-b").expect("child-b lmq")),
            ]),
            1,
        );
        registry.add_complete_subscription(
            &client_two,
            &group_b,
            &parent_topic,
            &HashSet::from([CheetahString::from_string(
                to_lmq_name("parent-topic", "child-b").expect("child-b lmq"),
            )]),
            1,
        );
    }

    fn seed_lmq_offsets(runtime: &mut BrokerRuntime, offsets: &[(&str, i64)]) {
        let inner = runtime.inner_for_test();
        let mut topic_queue_table = HashMap::new();
        for (lite_topic, offset) in offsets {
            let lmq_name = to_lmq_name("parent-topic", lite_topic).expect("lmq name");
            topic_queue_table.insert(CheetahString::from_string(format!("{lmq_name}-0")), *offset);
        }
        inner
            .message_store_mut()
            .as_mut()
            .expect("message store should be initialized")
            .consume_queue_store_mut()
            .set_topic_queue_table(topic_queue_table);
    }

    fn set_parent_topic_message_type(runtime: &mut BrokerRuntime, message_type: &str) {
        let topic_config = runtime
            .inner_for_test()
            .topic_config_manager()
            .select_topic_config(&CheetahString::from_static_str("parent-topic"))
            .expect("parent topic should exist");
        topic_config.mut_from_ref().attributes.insert(
            TopicAttributes::TopicAttributes::topic_message_type_attribute()
                .name()
                .clone(),
            CheetahString::from_string(message_type.to_string()),
        );
    }

    fn set_parent_topic_lite_expiration(runtime: &mut BrokerRuntime, expiration: i32) {
        let topic_config = runtime
            .inner_for_test()
            .topic_config_manager()
            .select_topic_config(&CheetahString::from_static_str("parent-topic"))
            .expect("parent topic should exist");
        topic_config.mut_from_ref().attributes.insert(
            TopicAttributes::TopicAttributes::lite_topic_expiration_attribute()
                .name()
                .clone(),
            CheetahString::from_string(expiration.to_string()),
        );
    }

    fn seed_lite_bound_group(runtime: &mut BrokerRuntime, group: &str) {
        let inner = runtime.inner_for_test();
        let mut config = SubscriptionGroupConfig::new(CheetahString::from(group));
        config.set_attributes(HashMap::from([(
            CheetahString::from_string(format!("+{LITE_BIND_TOPIC_ATTRIBUTE_NAME}")),
            CheetahString::from_static_str("parent-topic"),
        )]));
        inner
            .subscription_group_manager_mut()
            .update_subscription_group_config(&mut config);
    }

    fn seed_lite_topic_publish_route(runtime: &mut BrokerRuntime, broker_names: &[CheetahString]) {
        let mut publish_info = TopicPublishInfo::new();
        publish_info.have_topic_router_info = true;
        publish_info.message_queue_list = broker_names
            .iter()
            .enumerate()
            .map(|(queue_id, broker_name)| {
                MessageQueue::from_parts("parent-topic", broker_name.clone(), queue_id as i32)
            })
            .collect();
        runtime
            .inner_for_test()
            .topic_route_info_manager_mut()
            .topic_publish_info_table
            .mut_from_ref()
            .insert(CheetahString::from_static_str("parent-topic"), publish_info);
    }

    fn seed_lmq_consumer_offset(runtime: &mut BrokerRuntime, group: &str, lite_topic: &str, offset: i64) {
        let inner = runtime.inner_for_test();
        let lmq_name = CheetahString::from_string(to_lmq_name("parent-topic", lite_topic).expect("lmq name"));
        inner.consumer_offset_manager().commit_offset(
            CheetahString::from_static_str("127.0.0.1"),
            &CheetahString::from(group),
            &lmq_name,
            0,
            offset,
        );
    }

    async fn seed_lmq_message(runtime: &mut BrokerRuntime, lite_topic: &str, body: &'static [u8]) -> i64 {
        let inner = runtime.inner_for_test();
        let lmq_name = CheetahString::from_string(to_lmq_name("parent-topic", lite_topic).expect("lmq name"));
        let mut message = MessageExtBrokerInner::default();
        message.set_topic(CheetahString::from_static_str("parent-topic"));
        message.message_ext_inner.set_queue_id(0);
        message.set_body(Bytes::from_static(body));
        message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_DISPATCH),
            lmq_name,
        );

        let put_result = inner
            .message_store_mut()
            .as_mut()
            .expect("message store should be initialized")
            .put_message(message)
            .await;
        assert!(put_result.is_ok(), "seed lmq message should succeed");
        let wrote_offset = put_result
            .append_message_result()
            .expect("seed message should expose append result")
            .wrote_offset;
        inner
            .message_store_mut()
            .as_mut()
            .expect("message store should be initialized")
            .reput_once()
            .await;
        wrote_offset
    }

    async fn start_namesrv(port: u16, root: &Path) -> TestNameServer {
        let namesrv_root = root.join(format!("namesrv-{port}"));
        std::fs::create_dir_all(&namesrv_root).expect("create namesrv test root");
        let namesrv_config = NamesrvConfig {
            rocketmq_home: root.to_string_lossy().into_owned(),
            kv_config_path: namesrv_root.join("kvConfig.json").to_string_lossy().into_owned(),
            config_store_path: namesrv_root.join("namesrv.properties").to_string_lossy().into_owned(),
            use_route_info_manager_v2: true,
            ..NamesrvConfig::default()
        };
        let server_config = ServerConfig {
            bind_address: "127.0.0.1".to_string(),
            listen_port: port as u32,
        };
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            NameServerBuilder::new()
                .set_name_server_config(namesrv_config)
                .set_server_config(server_config)
                .build()
                .boot_with_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("boot namesrv");
        });

        wait_until(
            Duration::from_secs(10),
            || std::net::TcpStream::connect(("127.0.0.1", port)).is_ok(),
            "namesrv to listen on its remoting port",
        )
        .await;

        TestNameServer {
            addr: CheetahString::from_string(format!("127.0.0.1:{port}")),
            shutdown_tx: Some(shutdown_tx),
            handle: Some(handle),
        }
    }

    async fn configure_namesrv(runtime: &mut BrokerRuntime, namesrv_addr: &CheetahString) {
        Arc::make_mut(&mut runtime.inner.broker_config).namesrv_addr = Some(namesrv_addr.clone());
        runtime
            .inner
            .broker_outer_api
            .update_name_server_address_list(namesrv_addr.clone())
            .await;
    }

    async fn sync_namesrv_member_group(
        namesrv_addr: &CheetahString,
        cluster_name: &CheetahString,
        broker_name: &CheetahString,
    ) -> BrokerMemberGroup {
        let mut client = ArcMut::new(RocketmqDefaultClient::new(
            Arc::new(TokioClientConfig::default()),
            DefaultRemotingRequestProcessor,
        ));
        let weak_client = ArcMut::downgrade(&client);
        client.start(weak_client).await;
        let request_header = GetBrokerMemberGroupRequestHeader::new(cluster_name.clone(), broker_name.clone());
        let request = RemotingCommand::create_request_command(RequestCode::GetBrokerMemberGroup, request_header);
        let mut response = client
            .invoke_request(Some(namesrv_addr), request, 3000)
            .await
            .expect("query broker member group from namesrv");
        assert_eq!(
            ResponseCode::from(response.code()),
            ResponseCode::Success,
            "namesrv should accept GetBrokerMemberGroup requests"
        );
        let response_body = GetBrokerMemberGroupResponseBody::decode(
            response
                .take_body()
                .expect("GetBrokerMemberGroup response should contain a body")
                .as_ref(),
        )
        .expect("decode GetBrokerMemberGroup response body");
        client.shutdown();
        response_body
            .broker_member_group
            .expect("namesrv should return broker member group body")
    }

    async fn wait_for_namesrv_member_group<F>(
        namesrv_addr: &CheetahString,
        cluster_name: &CheetahString,
        broker_name: &CheetahString,
        timeout: Duration,
        context: &str,
        mut predicate: F,
    ) -> BrokerMemberGroup
    where
        F: FnMut(&BrokerMemberGroup) -> bool,
    {
        let start = std::time::Instant::now();
        loop {
            let member_group = sync_namesrv_member_group(namesrv_addr, cluster_name, broker_name).await;
            if predicate(&member_group) {
                return member_group;
            }
            if start.elapsed() >= timeout {
                panic!("Timed out waiting for {context}, last member group: {:?}", member_group);
            }
            sleep(Duration::from_millis(200)).await;
        }
    }

    async fn wait_until<F>(timeout: Duration, mut predicate: F, context: &str)
    where
        F: FnMut() -> bool,
    {
        let start = std::time::Instant::now();
        loop {
            if predicate() {
                return;
            }
            if start.elapsed() >= timeout {
                panic!("Timed out waiting for {context}");
            }
            sleep(Duration::from_millis(200)).await;
        }
    }

    async fn new_test_controller_manager(
        controller_peer: RaftPeer,
        controller_peers: Vec<RaftPeer>,
        raft_peers: Vec<RaftPeer>,
        root: &Path,
    ) -> ArcMut<TestControllerManager> {
        let node_id = controller_peer.id;
        let config = TestControllerConfig::default()
            .with_node_info(node_id, controller_peer.addr)
            .with_controller_peers(controller_peers)
            .with_raft_peers(raft_peers)
            .with_storage_backend(StorageBackendType::Memory)
            .with_storage_path(
                root.join(format!("controller-{node_id}"))
                    .to_string_lossy()
                    .into_owned(),
            )
            .with_election_timeout_ms(800)
            .with_heartbeat_interval_ms(200);
        let config = config
            .with_enable_elect_unclean_master(true)
            .with_enable_elect_unclean_master_local(true);

        let manager = ArcMut::new(
            TestControllerManager::new(config)
                .await
                .expect("create controller manager"),
        );
        assert!(
            manager
                .clone()
                .initialize()
                .await
                .expect("initialize controller manager"),
            "controller manager should initialize exactly once"
        );
        manager.clone().start().await.expect("start controller manager");
        manager
    }

    async fn start_controller_cluster(
        base_port: u16,
        root: &Path,
    ) -> (Vec<ArcMut<TestControllerManager>>, Vec<RaftPeer>) {
        let controller_peers = vec![
            RaftPeer {
                id: 1,
                addr: format!("127.0.0.1:{}", base_port + 1).parse().expect("controller addr"),
            },
            RaftPeer {
                id: 2,
                addr: format!("127.0.0.1:{}", base_port + 2).parse().expect("controller addr"),
            },
            RaftPeer {
                id: 3,
                addr: format!("127.0.0.1:{}", base_port + 3).parse().expect("controller addr"),
            },
        ];
        let raft_peers = vec![
            RaftPeer {
                id: 1,
                addr: format!("127.0.0.1:{}", base_port + 11).parse().expect("raft addr"),
            },
            RaftPeer {
                id: 2,
                addr: format!("127.0.0.1:{}", base_port + 12).parse().expect("raft addr"),
            },
            RaftPeer {
                id: 3,
                addr: format!("127.0.0.1:{}", base_port + 13).parse().expect("raft addr"),
            },
        ];

        let bootstrap_manager = new_test_controller_manager(
            controller_peers[0].clone(),
            controller_peers.clone(),
            raft_peers.clone(),
            root,
        )
        .await;
        let mut managers = vec![bootstrap_manager.clone()];

        sleep(Duration::from_secs(1)).await;

        let mut initial_cluster = BTreeMap::new();
        initial_cluster.insert(
            controller_peers[0].id,
            Node {
                node_id: controller_peers[0].id,
                rpc_addr: raft_peers[0].addr.to_string(),
            },
        );
        bootstrap_manager
            .raft()
            .initialize_cluster(initial_cluster)
            .await
            .expect("initialize single-node controller cluster");

        wait_until(
            Duration::from_secs(10),
            || bootstrap_manager.is_leader(),
            "controller node 1 to become leader",
        )
        .await;

        wait_until(
            Duration::from_secs(10),
            || bootstrap_manager.raft().has_committed_log().unwrap_or(false),
            "controller leader to commit its first log entry",
        )
        .await;

        bootstrap_manager
            .controller()
            .apply_broker_id(&ApplyBrokerIdRequestHeader {
                cluster_name: CheetahString::from_static_str("bootstrap-cluster"),
                broker_name: CheetahString::from_static_str("bootstrap-broker"),
                applied_broker_id: 0,
                register_check_code: CheetahString::from_static_str("127.0.0.1:0;bootstrap"),
            })
            .await
            .expect("commit bootstrap controller write")
            .expect("bootstrap controller write response");

        for (controller_peer, raft_peer) in controller_peers.iter().zip(raft_peers.iter()).skip(1) {
            managers.push(
                new_test_controller_manager(
                    controller_peer.clone(),
                    controller_peers.clone(),
                    vec![raft_peer.clone()],
                    root,
                )
                .await,
            );
            let learner_manager = managers.last().expect("new learner controller manager");
            learner_manager
                .set_raft_runtime_heartbeat_enabled(false)
                .expect("disable learner heartbeat during bootstrap");
            learner_manager
                .set_raft_runtime_elect_enabled(false)
                .expect("disable learner election during bootstrap");
            learner_manager
                .set_raft_runtime_tick_enabled(false)
                .expect("disable learner tick during bootstrap");
            sleep(Duration::from_millis(300)).await;
            bootstrap_manager
                .raft()
                .add_learner(
                    controller_peer.id,
                    Node {
                        node_id: controller_peer.id,
                        rpc_addr: raft_peer.addr.to_string(),
                    },
                    true,
                )
                .await
                .expect("add controller learner");
        }

        let expected_voters = controller_peers.iter().map(|peer| peer.id).collect::<BTreeSet<_>>();
        match tokio::time::timeout(
            Duration::from_secs(20),
            bootstrap_manager.raft().change_membership(expected_voters, false),
        )
        .await
        {
            Ok(result) => result.expect("promote controller learners to voters"),
            Err(_) => {
                panic!(
                    "Timed out promoting controller learners; states={:?}",
                    managers
                        .iter()
                        .map(|manager| (manager.is_leader(), manager.raft().has_committed_log().unwrap_or(false)))
                        .collect::<Vec<_>>()
                );
            }
        }

        wait_until(
            Duration::from_secs(15),
            || {
                managers.iter().filter(|manager| manager.is_leader()).count() == 1
                    && managers
                        .iter()
                        .all(|manager| manager.raft().has_committed_log().unwrap_or(false))
            },
            "controller cluster to replicate voter membership and elect a single leader",
        )
        .await;

        for manager in managers.iter().skip(1) {
            manager
                .set_raft_runtime_tick_enabled(true)
                .expect("re-enable learner tick after bootstrap");
            manager
                .set_raft_runtime_heartbeat_enabled(true)
                .expect("re-enable learner heartbeat after bootstrap");
            manager
                .set_raft_runtime_elect_enabled(true)
                .expect("re-enable learner election after bootstrap");
        }

        sleep(Duration::from_secs(1)).await;

        wait_until(
            Duration::from_secs(15),
            || managers.iter().filter(|manager| manager.is_leader()).count() == 1,
            "controller cluster to elect a single leader",
        )
        .await;

        sleep(Duration::from_secs(1)).await;
        (managers, controller_peers)
    }

    fn new_controller_mode_runtime(
        root: &Path,
        broker_name: &str,
        listen_port: u16,
        ha_listen_port: u16,
        controller_addrs: CheetahString,
    ) -> BrokerRuntime {
        new_controller_mode_runtime_with_store_key(
            root,
            &format!("broker-{listen_port}"),
            broker_name,
            listen_port,
            ha_listen_port,
            controller_addrs,
        )
    }

    fn new_controller_mode_runtime_with_store_key(
        root: &Path,
        store_key: &str,
        broker_name: &str,
        listen_port: u16,
        ha_listen_port: u16,
        controller_addrs: CheetahString,
    ) -> BrokerRuntime {
        let store_root = root.join(store_key);
        std::fs::create_dir_all(&store_root).expect("create broker store root");

        let broker_config = Arc::new(BrokerConfig {
            broker_identity: rocketmq_common::common::broker::broker_config::BrokerIdentity {
                broker_name: CheetahString::from_string(broker_name.to_owned()),
                broker_cluster_name: CheetahString::from_static_str("controller-test-cluster"),
                broker_id: mix_all::MASTER_ID,
                is_broker_container: false,
                is_in_broker_container: false,
            },
            broker_server_config: ServerConfig {
                listen_port: listen_port as u32,
                ..ServerConfig::default()
            },
            broker_ip1: CheetahString::from_static_str("127.0.0.1"),
            broker_ip2: Some(CheetahString::from_static_str("127.0.0.1")),
            listen_port: listen_port as u32,
            enable_controller_mode: true,
            controller_addr: controller_addrs,
            sync_broker_metadata_period: 500,
            sync_controller_metadata_period: 500,
            broker_heartbeat_interval: 500,
            send_heartbeat_timeout_millis: 1000,
            controller_heartbeat_timeout_mills: 2000,
            broker_election_priority: 1,
            namesrv_addr: None,
            store_path_root_dir: store_root.to_string_lossy().into_owned().into(),
            auth_config_path: store_root.join("auth.json").to_string_lossy().into_owned().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: store_root.to_string_lossy().into_owned().into(),
            ha_listen_port: ha_listen_port as usize,
            broker_role: BrokerRole::Slave,
            total_replicas: 2,
            in_sync_replicas: 2,
            min_in_sync_replicas: 1,
            all_ack_in_sync_state_set: true,
            enable_controller_mode: true,
            ..MessageStoreConfig::default()
        });

        BrokerRuntime::new(broker_config, message_store_config)
    }

    async fn bootstrap_broker_against_controller(
        runtime: &mut BrokerRuntime,
        controller_leader_manager: &ArcMut<TestControllerManager>,
    ) {
        let controller_leader = BrokerRuntimeInner::discover_controller_leader(&runtime.inner)
            .await
            .expect("discover controller leader");
        let cluster_name = runtime.inner.broker_config.broker_identity.broker_cluster_name.clone();
        let broker_name = runtime.inner.broker_config.broker_identity.broker_name.clone();
        let broker_addr = runtime.inner.get_broker_addr().clone();
        let controller_broker_id =
            BrokerRuntimeInner::ensure_controller_broker_id(runtime.inner.clone(), &controller_leader)
                .await
                .expect("ensure controller broker id");

        let (register_header, sync_state_set) = runtime
            .inner
            .broker_outer_api
            .register_broker_to_controller(
                cluster_name.clone(),
                broker_name.clone(),
                controller_broker_id as i64,
                broker_addr,
                &controller_leader,
            )
            .await
            .expect("register broker to controller");
        if let Some(replicas_manager) = runtime.inner.replicas_manager_mut() {
            replicas_manager.mark_registered();
        }

        let sync_state_set = sync_state_set.unwrap_or_default();
        if register_header.master_broker_id.is_some() && register_header.master_epoch.is_some() {
            BrokerRuntimeInner::apply_controller_role_change(
                runtime.inner.clone(),
                Some(controller_leader),
                register_header.master_broker_id.and_then(|id| u64::try_from(id).ok()),
                register_header.master_address,
                register_header.master_epoch,
                register_header.sync_state_set_epoch,
                sync_state_set,
            )
            .await
            .expect("apply controller register result");
            return;
        }

        runtime
            .inner
            .send_heartbeat_to_controller_leader(&controller_leader)
            .await
            .expect("send bootstrap heartbeat to controller");
        sleep(Duration::from_millis(300)).await;

        let (pre_elect_header, pre_elect_body) = runtime
            .inner
            .broker_outer_api
            .get_replica_info(
                &controller_leader,
                runtime.inner.broker_config.broker_identity.broker_name.clone(),
            )
            .await
            .expect("query replica info before elect");
        if pre_elect_header.master_broker_id.is_some_and(|master_broker_id| {
            controller_leader_manager.heartbeat_manager().is_broker_active(
                cluster_name.as_str(),
                broker_name.as_str(),
                master_broker_id,
            )
        }) {
            let applied = BrokerRuntimeInner::apply_controller_replica_info(
                runtime.inner.clone(),
                controller_leader,
                pre_elect_header.master_broker_id.and_then(|id| u64::try_from(id).ok()),
                pre_elect_header.master_address.map(CheetahString::from_string),
                pre_elect_header.master_epoch,
                Some(pre_elect_body.get_sync_state_set_epoch()),
                pre_elect_body.get_sync_state_set().cloned().unwrap_or_default(),
            )
            .await;
            assert!(applied, "apply controller replica info before elect should succeed");
            return;
        }

        let (elect_header, sync_state_set) = runtime
            .inner
            .broker_outer_api
            .broker_elect(
                &controller_leader,
                cluster_name.clone(),
                broker_name.clone(),
                controller_broker_id as i64,
            )
            .await
            .unwrap_or_else(|error| {
                let pre_elect_master_id = pre_elect_header.master_broker_id.unwrap_or_default();
                let pre_elect_master_active = pre_elect_header.master_broker_id.is_some_and(|master_broker_id| {
                    controller_leader_manager.heartbeat_manager().is_broker_active(
                        cluster_name.as_str(),
                        broker_name.as_str(),
                        master_broker_id,
                    )
                });
                panic!(
                    "controller elect should succeed, got error={}, pre_elect_master={:?}, pre_elect_epoch={:?}, \
                     pre_elect_sync_state={:?}, register_master={:?}, register_epoch={:?}, \
                     pre_elect_master_active={}, queried_master_id={}",
                    error,
                    pre_elect_header.master_broker_id,
                    pre_elect_header.master_epoch,
                    pre_elect_body.get_sync_state_set(),
                    register_header.master_broker_id,
                    register_header.master_epoch,
                    pre_elect_master_active,
                    pre_elect_master_id
                )
            });
        BrokerRuntimeInner::apply_controller_role_change(
            runtime.inner.clone(),
            Some(controller_leader),
            elect_header.master_broker_id.and_then(|id| u64::try_from(id).ok()),
            elect_header.master_address,
            elect_header.master_epoch,
            elect_header.sync_state_set_epoch,
            sync_state_set,
        )
        .await
        .expect("apply controller elect result");
    }

    async fn initialize_controller_mode_broker(runtime: &mut BrokerRuntime, broker_label: &str) {
        assert!(
            runtime.initialize_metadata(),
            "{broker_label} metadata init should succeed"
        );
        assert!(
            runtime.initialize_message_store().await,
            "{broker_label} message store init should succeed"
        );
        runtime.inner.initialize_controller_mode();
        runtime.register_message_store_hook();
        assert!(
            runtime
                .inner
                .message_store
                .as_mut()
                .expect("controller mode message store")
                .load()
                .await,
            "{broker_label} message store load should succeed"
        );
        assert!(
            runtime
                .inner
                .schedule_message_service
                .as_mut()
                .expect("controller mode schedule service")
                .load(),
            "{broker_label} schedule service load should succeed"
        );
        runtime.initialize_remoting_server();
        runtime.initialize_resources();
        runtime.initialize_scheduled_tasks().await;
        runtime.initial_transaction().await;
        assert!(runtime.initial_acl().await, "{broker_label} acl init should succeed");
        runtime.initial_rpc_hooks();
        runtime.initial_request_pipeline();
    }

    #[tokio::test]
    async fn timer_message_store_unchecked_returns_configured_store() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        runtime.inner.set_timer_message_store(TimerMessageStore::new_empty());

        let timer_store = runtime.inner.timer_message_store_unchecked();
        let configured_store = runtime
            .inner
            .timer_message_store()
            .expect("timer store should be present")
            .as_ref();

        assert!(std::ptr::eq(timer_store, configured_store));
    }

    #[tokio::test]
    async fn initialize_message_store_reuses_store_owned_timer_message_store() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-broker-runtime-timer-{}", current_millis()));
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            ..MessageStoreConfig::default()
        });
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        assert!(runtime.initialize_metadata());
        assert!(runtime.initialize_message_store().await);

        let store_timer = runtime
            .inner
            .message_store()
            .expect("message store should be initialized")
            .get_timer_message_store()
            .cloned()
            .expect("store should own timer store");
        let runtime_timer = runtime
            .inner
            .timer_message_store()
            .cloned()
            .expect("runtime should expose timer store");

        assert!(Arc::ptr_eq(&store_timer, &runtime_timer));

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn init_processor_routes_lite_subscription_ctl_requests_to_lite_processor() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-broker-runtime-lite-{}", current_millis()));
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            ..MessageStoreConfig::default()
        });
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        assert!(runtime.initialize().await);

        let (mut processor, _) = runtime.init_processor();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut request = RemotingCommand::create_request_command(RequestCode::LiteSubscriptionCtl, EmptyHeader {})
            .set_body(Bytes::from_static(b""));
        let response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed")
            .expect("lite subscription control should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::IllegalOperation);

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn get_broker_lite_info_returns_registry_aggregates() {
        let mut runtime = new_lite_test_runtime("broker-lite-info").await;
        seed_lite_query_state(&mut runtime);
        set_parent_topic_lite_expiration(&mut runtime, 600);
        seed_lite_bound_group(&mut runtime, "group-c");

        let (mut processor, _) = runtime.init_processor();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut request = RemotingCommand::create_request_command(RequestCode::GetBrokerLiteInfo, EmptyHeader {});
        let mut response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed")
            .expect("lite manager should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = GetBrokerLiteInfoResponseBody::decode(
            response
                .take_body()
                .expect("GetBrokerLiteInfo response should contain a body")
                .as_ref(),
        )
        .expect("decode broker lite info response body");
        assert_eq!(
            body.get_store_type(),
            Some(&CheetahString::from_static_str("LocalFile"))
        );
        assert_eq!(body.get_max_lmq_num(), 32);
        assert_eq!(body.get_current_lmq_num(), 2);
        assert_eq!(body.get_lite_subscription_count(), 3);
        assert_eq!(
            body.get_topic_meta()
                .get(&CheetahString::from_static_str("parent-topic")),
            Some(&600)
        );
        assert_eq!(
            body.get_group_meta()
                .get(&CheetahString::from_static_str("parent-topic")),
            Some(&HashSet::from([
                CheetahString::from_static_str("group-a"),
                CheetahString::from_static_str("group-b"),
                CheetahString::from_static_str("group-c"),
            ]))
        );

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn get_broker_lite_info_reports_pop_lite_order_info_count() {
        let mut runtime = new_lite_test_runtime("broker-lite-order-info-count").await;
        seed_lite_query_state(&mut runtime);
        seed_lmq_message(&mut runtime, "child-a", b"lite-body").await;
        let lmq_name = CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("child-a lmq"));

        let (mut processor, _) = runtime.init_processor();
        runtime.inner.lite_event_dispatcher().do_full_dispatch(
            &CheetahString::from_static_str("client-1"),
            &CheetahString::from_static_str("group-a"),
            &HashSet::from([lmq_name]),
        );

        let pop_channel = create_test_channel().await;
        let pop_ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(pop_channel.clone()));
        let pop_header = PopLiteMessageRequestHeader {
            client_id: CheetahString::from_static_str("client-1"),
            consumer_group: CheetahString::from_static_str("group-a"),
            topic: CheetahString::from_static_str("parent-topic"),
            max_msg_num: 1,
            invisible_time: 60_000,
            poll_time: 0,
            born_time: current_millis() as i64,
            attempt_id: Some(CheetahString::from_static_str("attempt-1")),
            rpc: None,
        };
        let mut pop_request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, pop_header);
        pop_request.make_custom_header_to_net();
        let pop_response = processor
            .process_request(pop_channel, pop_ctx, &mut pop_request)
            .await
            .expect("pop lite should succeed")
            .expect("pop lite should return a response");
        assert_eq!(ResponseCode::from(pop_response.code()), ResponseCode::Success);

        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut request = RemotingCommand::create_request_command(RequestCode::GetBrokerLiteInfo, EmptyHeader {});
        let mut response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed")
            .expect("lite manager should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = GetBrokerLiteInfoResponseBody::decode(
            response
                .take_body()
                .expect("GetBrokerLiteInfo response should contain a body")
                .as_ref(),
        )
        .expect("decode broker lite info response body");
        assert_eq!(body.get_order_info_count(), 1);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn get_parent_topic_info_returns_group_and_lite_counts() {
        let mut runtime = new_lite_test_runtime("parent-topic-info").await;
        seed_lite_query_state(&mut runtime);
        set_parent_topic_lite_expiration(&mut runtime, 600);

        let (mut processor, _) = runtime.init_processor();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let header = GetParentTopicInfoRequestHeader {
            topic: CheetahString::from_static_str("parent-topic"),
            rpc: None,
        };
        let mut request = RemotingCommand::create_request_command(RequestCode::GetParentTopicInfo, header);
        request.make_custom_header_to_net();
        let mut response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed")
            .expect("lite manager should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = GetParentTopicInfoResponseBody::decode(
            response
                .take_body()
                .expect("GetParentTopicInfo response should contain a body")
                .as_ref(),
        )
        .expect("decode parent topic info response body");
        assert_eq!(body.get_topic(), Some(&CheetahString::from_static_str("parent-topic")));
        assert_eq!(body.get_ttl(), 600);
        assert_eq!(body.get_lmq_num(), 2);
        assert_eq!(body.get_lite_topic_count(), 2);
        assert_eq!(
            body.get_groups(),
            &HashSet::from([
                CheetahString::from_static_str("group-a"),
                CheetahString::from_static_str("group-b"),
            ])
        );

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn get_parent_topic_info_rejects_non_lite_parent_topic() {
        let mut runtime = new_lite_test_runtime("parent-topic-info-non-lite").await;
        seed_lite_query_state(&mut runtime);
        set_parent_topic_message_type(&mut runtime, "NORMAL");

        let (mut processor, _) = runtime.init_processor();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let header = GetParentTopicInfoRequestHeader {
            topic: CheetahString::from_static_str("parent-topic"),
            rpc: None,
        };
        let mut request = RemotingCommand::create_request_command(RequestCode::GetParentTopicInfo, header);
        request.make_custom_header_to_net();
        let response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed")
            .expect("lite manager should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn get_lite_topic_info_returns_subscribers_for_matching_lite_topic() {
        let mut runtime = new_lite_test_runtime("lite-topic-info").await;
        seed_lite_query_state(&mut runtime);
        seed_lite_topic_publish_route(&mut runtime, &[CheetahString::from_static_str("other-broker")]);

        let (mut processor, _) = runtime.init_processor();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let header = GetLiteTopicInfoRequestHeader {
            parent_topic: CheetahString::from_static_str("parent-topic"),
            lite_topic: CheetahString::from_static_str("child-b"),
        };
        let mut request = RemotingCommand::create_request_command(RequestCode::GetLiteTopicInfo, header);
        request.make_custom_header_to_net();
        let mut response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed")
            .expect("lite manager should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = GetLiteTopicInfoResponseBody::decode(
            response
                .take_body()
                .expect("GetLiteTopicInfo response should contain a body")
                .as_ref(),
        )
        .expect("decode lite topic info response body");
        assert_eq!(body.parent_topic(), &CheetahString::from_static_str("parent-topic"));
        assert_eq!(body.lite_topic(), &CheetahString::from_static_str("child-b"));
        assert!(!body.sharding_to_broker());
        assert_eq!(body.subscriber().len(), 2);
        assert!(body.subscriber().contains(&ClientGroup::from_parts(
            CheetahString::from_static_str("client-1"),
            CheetahString::from_static_str("group-a"),
        )));
        assert!(body.subscriber().contains(&ClientGroup::from_parts(
            CheetahString::from_static_str("client-2"),
            CheetahString::from_static_str("group-b"),
        )));

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn get_lite_topic_info_marks_current_broker_when_sharding_route_points_local_broker() {
        let mut runtime = new_lite_test_runtime("lite-topic-info-local-shard").await;
        seed_lite_query_state(&mut runtime);
        let broker_name = runtime.inner.broker_config.broker_identity.broker_name.clone();
        seed_lite_topic_publish_route(&mut runtime, &[broker_name]);

        let (mut processor, _) = runtime.init_processor();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let header = GetLiteTopicInfoRequestHeader {
            parent_topic: CheetahString::from_static_str("parent-topic"),
            lite_topic: CheetahString::from_static_str("child-b"),
        };
        let mut request = RemotingCommand::create_request_command(RequestCode::GetLiteTopicInfo, header);
        request.make_custom_header_to_net();
        let mut response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed")
            .expect("lite manager should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = GetLiteTopicInfoResponseBody::decode(
            response
                .take_body()
                .expect("GetLiteTopicInfo response should contain a body")
                .as_ref(),
        )
        .expect("decode lite topic info response body");
        assert!(body.sharding_to_broker());

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn get_lite_topic_info_rejects_non_lite_parent_topic() {
        let mut runtime = new_lite_test_runtime("lite-topic-info-non-lite").await;
        seed_lite_query_state(&mut runtime);
        set_parent_topic_message_type(&mut runtime, "NORMAL");

        let (mut processor, _) = runtime.init_processor();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let header = GetLiteTopicInfoRequestHeader {
            parent_topic: CheetahString::from_static_str("parent-topic"),
            lite_topic: CheetahString::from_static_str("child-b"),
        };
        let mut request = RemotingCommand::create_request_command(RequestCode::GetLiteTopicInfo, header);
        request.make_custom_header_to_net();
        let response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed")
            .expect("lite manager should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn get_lite_client_info_returns_topics_for_bound_client() {
        let mut runtime = new_lite_test_runtime("lite-client-info").await;
        seed_lite_query_state(&mut runtime);

        let (mut processor, _) = runtime.init_processor();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let header = GetLiteClientInfoRequestHeader {
            parent_topic: Some(CheetahString::from_static_str("parent-topic")),
            group: Some(CheetahString::from_static_str("group-a")),
            client_id: Some(CheetahString::from_static_str("client-1")),
            max_count: 32,
        };
        let mut request = RemotingCommand::create_request_command(RequestCode::GetLiteClientInfo, header);
        request.make_custom_header_to_net();
        let mut response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed")
            .expect("lite manager should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = GetLiteClientInfoResponseBody::decode(
            response
                .take_body()
                .expect("GetLiteClientInfo response should contain a body")
                .as_ref(),
        )
        .expect("decode lite client info response body");
        assert_eq!(
            body.parent_topic(),
            Some(&CheetahString::from_static_str("parent-topic"))
        );
        assert_eq!(body.group(), &CheetahString::from_static_str("group-a"));
        assert_eq!(body.client_id(), &CheetahString::from_static_str("client-1"));
        assert!(body.last_access_time() > 0);
        assert_eq!(body.last_consume_time(), 0);
        assert_eq!(body.lite_topic_count(), 2);
        assert_eq!(
            body.lite_topic_set(),
            &HashSet::from([
                CheetahString::from_static_str("child-a"),
                CheetahString::from_static_str("child-b"),
            ])
        );

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn get_lite_client_info_rejects_non_lite_parent_topic() {
        let mut runtime = new_lite_test_runtime("lite-client-info-non-lite").await;
        seed_lite_query_state(&mut runtime);
        set_parent_topic_message_type(&mut runtime, "NORMAL");

        let (mut processor, _) = runtime.init_processor();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let header = GetLiteClientInfoRequestHeader {
            parent_topic: Some(CheetahString::from_static_str("parent-topic")),
            group: Some(CheetahString::from_static_str("group-a")),
            client_id: Some(CheetahString::from_static_str("client-1")),
            max_count: 32,
        };
        let mut request = RemotingCommand::create_request_command(RequestCode::GetLiteClientInfo, header);
        request.make_custom_header_to_net();
        let response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed")
            .expect("lite manager should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn get_lite_group_info_returns_offset_wrapper_for_specific_lite_topic() {
        let mut runtime = new_lite_test_runtime("lite-group-info-topic").await;
        seed_lite_query_state(&mut runtime);
        seed_lmq_offsets(&mut runtime, &[("child-a", 8), ("child-b", 12)]);
        seed_lmq_consumer_offset(&mut runtime, "group-a", "child-b", 0);

        let (mut processor, _) = runtime.init_processor();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let header = GetLiteGroupInfoRequestHeader {
            group: CheetahString::from_static_str("group-a"),
            lite_topic: CheetahString::from_static_str("child-b"),
            top_k: 10,
            rpc: None,
        };
        let mut request = RemotingCommand::create_request_command(RequestCode::GetLiteGroupInfo, header);
        request.make_custom_header_to_net();
        let mut response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed")
            .expect("lite manager should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = GetLiteGroupInfoResponseBody::decode(
            response
                .take_body()
                .expect("GetLiteGroupInfo response should contain a body")
                .as_ref(),
        )
        .expect("decode lite group info response body");
        assert_eq!(body.group(), &CheetahString::from_static_str("group-a"));
        assert_eq!(body.parent_topic(), &CheetahString::from_static_str("parent-topic"));
        assert_eq!(body.lite_topic(), &CheetahString::from_static_str("child-b"));
        assert_eq!(body.total_lag_count(), 12);
        assert_eq!(body.earliest_unconsumed_timestamp(), 0);
        let offset_wrapper = body
            .lite_topic_offset_wrapper()
            .expect("specific lite topic should include offset wrapper");
        assert_eq!(offset_wrapper.get_broker_offset(), 12);
        assert_eq!(offset_wrapper.get_consumer_offset(), 0);
        assert_eq!(offset_wrapper.get_last_timestamp(), 0);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn get_lite_group_info_returns_topk_aggregates_for_group() {
        let mut runtime = new_lite_test_runtime("lite-group-info-topk").await;
        seed_lite_query_state(&mut runtime);
        seed_lmq_offsets(&mut runtime, &[("child-a", 8), ("child-b", 12)]);
        seed_lmq_consumer_offset(&mut runtime, "group-a", "child-a", 3);
        seed_lmq_consumer_offset(&mut runtime, "group-a", "child-b", 0);

        let (mut processor, _) = runtime.init_processor();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let header = GetLiteGroupInfoRequestHeader {
            group: CheetahString::from_static_str("group-a"),
            lite_topic: CheetahString::from_static_str(""),
            top_k: 1,
            rpc: None,
        };
        let mut request = RemotingCommand::create_request_command(RequestCode::GetLiteGroupInfo, header);
        request.make_custom_header_to_net();
        let mut response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed")
            .expect("lite manager should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = GetLiteGroupInfoResponseBody::decode(
            response
                .take_body()
                .expect("GetLiteGroupInfo response should contain a body")
                .as_ref(),
        )
        .expect("decode lite group info response body");
        assert_eq!(body.group(), &CheetahString::from_static_str("group-a"));
        assert_eq!(body.parent_topic(), &CheetahString::from_static_str("parent-topic"));
        assert!(body.lite_topic().is_empty());
        assert_eq!(body.total_lag_count(), 17);
        assert_eq!(body.earliest_unconsumed_timestamp(), 0);
        assert_eq!(body.lag_count_top_k().len(), 1);
        assert_eq!(
            body.lag_count_top_k()[0].lite_topic(),
            &CheetahString::from_static_str("child-b")
        );
        assert_eq!(body.lag_count_top_k()[0].lag_count(), 12);
        assert_eq!(body.lag_timestamp_top_k().len(), 1);
        assert_eq!(body.lag_timestamp_top_k()[0].earliest_unconsumed_timestamp(), 0);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn get_lite_group_info_uses_offset_table_entries_not_present_in_registry() {
        let mut runtime = new_lite_test_runtime("lite-group-info-offset-table").await;
        seed_lite_query_state(&mut runtime);
        seed_lmq_offsets(&mut runtime, &[("child-a", 8), ("child-b", 12), ("child-c", 20)]);
        seed_lmq_consumer_offset(&mut runtime, "group-a", "child-b", 0);
        seed_lmq_consumer_offset(&mut runtime, "group-a", "child-c", 5);

        let (mut processor, _) = runtime.init_processor();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let header = GetLiteGroupInfoRequestHeader {
            group: CheetahString::from_static_str("group-a"),
            lite_topic: CheetahString::from_static_str(""),
            top_k: 2,
            rpc: None,
        };
        let mut request = RemotingCommand::create_request_command(RequestCode::GetLiteGroupInfo, header);
        request.make_custom_header_to_net();
        let mut response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed")
            .expect("lite manager should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = GetLiteGroupInfoResponseBody::decode(
            response
                .take_body()
                .expect("GetLiteGroupInfo response should contain a body")
                .as_ref(),
        )
        .expect("decode lite group info response body");
        assert_eq!(body.total_lag_count(), 27);
        assert_eq!(body.lag_count_top_k().len(), 2);
        let lite_topics = body
            .lag_count_top_k()
            .iter()
            .map(|lag_info| lag_info.lite_topic().clone())
            .collect::<HashSet<_>>();
        assert_eq!(
            lite_topics,
            HashSet::from([
                CheetahString::from_static_str("child-b"),
                CheetahString::from_static_str("child-c"),
            ])
        );
        assert!(!lite_topics.contains(&CheetahString::from_static_str("child-a")));

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn trigger_lite_dispatch_enqueues_events_for_target_client() {
        let mut runtime = new_lite_test_runtime("trigger-lite-dispatch").await;
        seed_lite_query_state(&mut runtime);
        seed_lmq_offsets(&mut runtime, &[("child-a", 8), ("child-b", 12)]);

        let (mut processor, _) = runtime.init_processor();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let header = TriggerLiteDispatchRequestHeader {
            group: CheetahString::from_static_str("group-a"),
            client_id: Some(CheetahString::from_static_str("client-1")),
        };
        let mut request = RemotingCommand::create_request_command(RequestCode::TriggerLiteDispatch, header);
        request.make_custom_header_to_net();
        let response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed")
            .expect("lite manager should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);

        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut request = RemotingCommand::create_request_command(RequestCode::GetBrokerLiteInfo, EmptyHeader {});
        let mut response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed")
            .expect("broker lite info should return a response");
        let body = GetBrokerLiteInfoResponseBody::decode(
            response
                .take_body()
                .expect("GetBrokerLiteInfo response should contain a body")
                .as_ref(),
        )
        .expect("decode broker lite info response body");
        assert_eq!(body.get_event_map_size(), 1);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn trigger_lite_dispatch_without_client_id_enqueues_events_for_group_subscribers() {
        let mut runtime = new_lite_test_runtime("trigger-lite-dispatch-group").await;
        seed_lite_query_state(&mut runtime);
        seed_lmq_offsets(&mut runtime, &[("child-a", 8), ("child-b", 12)]);

        let (mut processor, _) = runtime.init_processor();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let header = TriggerLiteDispatchRequestHeader {
            group: CheetahString::from_static_str("group-a"),
            client_id: None,
        };
        let mut request = RemotingCommand::create_request_command(RequestCode::TriggerLiteDispatch, header);
        request.make_custom_header_to_net();
        let response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed")
            .expect("lite manager should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);

        let dispatcher = runtime.inner.lite_event_dispatcher();
        assert_eq!(dispatcher.event_map_size(), 1);
        assert_eq!(
            dispatcher
                .pending_events(&CheetahString::from_static_str("client-1"))
                .into_iter()
                .collect::<HashSet<_>>(),
            HashSet::from([
                CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("child-a lmq")),
                CheetahString::from_string(to_lmq_name("parent-topic", "child-b").expect("child-b lmq")),
            ])
        );
        assert!(dispatcher
            .pending_events(&CheetahString::from_static_str("client-2"))
            .is_empty());

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn trigger_lite_dispatch_respects_broker_max_client_event_count_fallback() {
        let mut runtime = new_lite_test_runtime("trigger-lite-dispatch-max-client-event-count").await;
        seed_lite_query_state(&mut runtime);
        seed_lmq_offsets(&mut runtime, &[("child-a", 8), ("child-b", 12)]);
        let mut broker_config = runtime.inner_for_test().broker_config().clone();
        broker_config.max_client_event_count = 1;
        runtime.inner_for_test().set_broker_config(broker_config);

        let (mut processor, _) = runtime.init_processor();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let header = TriggerLiteDispatchRequestHeader {
            group: CheetahString::from_static_str("group-a"),
            client_id: Some(CheetahString::from_static_str("client-1")),
        };
        let mut request = RemotingCommand::create_request_command(RequestCode::TriggerLiteDispatch, header);
        request.make_custom_header_to_net();
        let response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed")
            .expect("lite manager should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);

        let pending_events = runtime
            .inner
            .lite_event_dispatcher()
            .pending_events(&CheetahString::from_static_str("client-1"));
        assert_eq!(pending_events.len(), 1);
        assert!(pending_events[0].ends_with("child-a") || pending_events[0].ends_with("child-b"));

        let drained_events = runtime
            .inner
            .lite_event_dispatcher()
            .take_pending_events(&CheetahString::from_static_str("client-1"));
        assert_eq!(drained_events.len(), 2);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn pop_lite_message_without_events_returns_polling_timeout() {
        let mut runtime = new_lite_test_runtime("pop-lite-route").await;
        seed_lite_query_state(&mut runtime);

        let (mut processor, _) = runtime.init_processor();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let header = PopLiteMessageRequestHeader {
            client_id: CheetahString::from_static_str("client-1"),
            consumer_group: CheetahString::from_static_str("group-a"),
            topic: CheetahString::from_static_str("parent-topic"),
            max_msg_num: 1,
            invisible_time: 60_000,
            poll_time: 0,
            born_time: current_millis() as i64,
            attempt_id: None,
            rpc: None,
        };
        let mut request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, header);
        request.make_custom_header_to_net();
        let response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed")
            .expect("pop lite should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::PollingTimeout);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn pop_lite_message_without_events_suspends_when_polling_enabled() {
        let mut runtime = new_lite_test_runtime("pop-lite-suspend").await;
        seed_lite_query_state(&mut runtime);

        let (mut processor, _) = runtime.init_processor();
        runtime
            .inner
            .pop_lite_message_processor
            .as_mut()
            .expect("pop lite processor should be initialized")
            .start();

        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let header = PopLiteMessageRequestHeader {
            client_id: CheetahString::from_static_str("client-1"),
            consumer_group: CheetahString::from_static_str("group-a"),
            topic: CheetahString::from_static_str("parent-topic"),
            max_msg_num: 1,
            invisible_time: 60_000,
            poll_time: 3_000,
            born_time: current_millis() as i64,
            attempt_id: None,
            rpc: None,
        };
        let mut request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, header);
        request.make_custom_header_to_net();
        let response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor dispatch should succeed");

        assert!(
            response.is_none(),
            "polling pop-lite should suspend instead of responding"
        );
        assert!(request.suspended());
        assert_eq!(
            runtime
                .inner
                .pop_lite_message_processor
                .as_ref()
                .expect("pop lite processor should be initialized")
                .pop_lite_long_polling_service()
                .get_polling_num("client-1"),
            1
        );

        runtime
            .inner
            .pop_lite_message_processor
            .as_mut()
            .expect("pop lite processor should be initialized")
            .shutdown();
        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn trigger_lite_dispatch_wakes_suspended_pop_lite_request_and_advances_offset() {
        let mut runtime = new_lite_test_runtime("pop-lite-trigger-wakeup").await;
        seed_lite_query_state(&mut runtime);
        seed_lmq_message(&mut runtime, "child-a", b"lite-body").await;
        let lmq_name = CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("child-a lmq"));

        let (mut processor, _) = runtime.init_processor();
        runtime
            .inner
            .pop_lite_message_processor
            .as_mut()
            .expect("pop lite processor should be initialized")
            .start();

        let pop_channel = create_test_channel().await;
        let pop_ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(pop_channel.clone()));
        let pop_header = PopLiteMessageRequestHeader {
            client_id: CheetahString::from_static_str("client-1"),
            consumer_group: CheetahString::from_static_str("group-a"),
            topic: CheetahString::from_static_str("parent-topic"),
            max_msg_num: 1,
            invisible_time: 60_000,
            poll_time: 3_000,
            born_time: current_millis() as i64,
            attempt_id: None,
            rpc: None,
        };
        let mut pop_request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, pop_header);
        pop_request.make_custom_header_to_net();
        let response = processor
            .process_request(pop_channel, pop_ctx, &mut pop_request)
            .await
            .expect("pop lite should suspend cleanly");
        assert!(
            response.is_none(),
            "suspended pop-lite should not produce an immediate response"
        );

        let trigger_channel = create_test_channel().await;
        let trigger_ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(trigger_channel.clone()));
        let trigger_header = TriggerLiteDispatchRequestHeader {
            group: CheetahString::from_static_str("group-a"),
            client_id: Some(CheetahString::from_static_str("client-1")),
        };
        let mut trigger_request =
            RemotingCommand::create_request_command(RequestCode::TriggerLiteDispatch, trigger_header);
        trigger_request.make_custom_header_to_net();
        let trigger_response = processor
            .process_request(trigger_channel, trigger_ctx, &mut trigger_request)
            .await
            .expect("trigger lite dispatch should succeed")
            .expect("trigger lite dispatch should return a response");
        assert_eq!(ResponseCode::from(trigger_response.code()), ResponseCode::Success);

        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            if runtime.inner.consumer_offset_manager().query_offset(
                &CheetahString::from_static_str("group-a"),
                &lmq_name,
                0,
            ) == 1
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "suspended pop-lite request should be woken and advance offset"
            );
            sleep(Duration::from_millis(20)).await;
        }

        assert_eq!(
            runtime
                .inner
                .pop_lite_message_processor
                .as_ref()
                .expect("pop lite processor should be initialized")
                .pop_lite_long_polling_service()
                .get_polling_num("client-1"),
            0
        );
        assert!(runtime
            .inner
            .lite_event_dispatcher()
            .pending_events(&CheetahString::from_static_str("client-1"))
            .is_empty());

        runtime
            .inner
            .pop_lite_message_processor
            .as_mut()
            .expect("pop lite processor should be initialized")
            .shutdown();
        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn pop_lite_message_returns_dispatched_lmq_payload_and_advances_offset() {
        let mut runtime = new_lite_test_runtime("pop-lite-consume").await;
        seed_lite_query_state(&mut runtime);
        let commit_log_offset = seed_lmq_message(&mut runtime, "child-a", b"lite-body").await;
        let expected_lmq_name = to_lmq_name("parent-topic", "child-a").expect("child-a lmq");
        let seeded = runtime
            .inner
            .message_store()
            .expect("message store should be initialized")
            .look_message_by_offset(commit_log_offset)
            .expect("seeded parent message should be readable");
        assert_eq!(seeded.topic(), &CheetahString::from_static_str("parent-topic"));
        assert_eq!(
            seeded
                .property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_INNER_MULTI_DISPATCH
                ))
                .as_deref(),
            Some(expected_lmq_name.as_str())
        );
        assert_eq!(
            seeded
                .property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET
                ))
                .as_deref(),
            Some("0")
        );
        assert_eq!(
            runtime
                .inner
                .message_store()
                .expect("message store should be initialized")
                .get_max_offset_in_queue(&CheetahString::from_static_str("parent-topic"), 0),
            1
        );
        let lmq_name = CheetahString::from_string(expected_lmq_name);
        assert_eq!(
            runtime
                .inner
                .message_store()
                .expect("message store should be initialized")
                .get_max_offset_in_queue(&lmq_name, 0),
            1
        );
        let direct_read = runtime
            .inner
            .message_store()
            .expect("message store should be initialized")
            .get_message(&CheetahString::from_static_str("group-a"), &lmq_name, 0, 0, 1, None)
            .await
            .expect("direct lmq read should return a result");
        assert_eq!(direct_read.status(), Some(GetMessageStatus::Found));
        runtime.inner.lite_event_dispatcher().do_full_dispatch(
            &CheetahString::from_static_str("client-1"),
            &CheetahString::from_static_str("group-a"),
            &HashSet::from([lmq_name.clone()]),
        );

        let (mut processor, _) = runtime.init_processor();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let pop_header = PopLiteMessageRequestHeader {
            client_id: CheetahString::from_static_str("client-1"),
            consumer_group: CheetahString::from_static_str("group-a"),
            topic: CheetahString::from_static_str("parent-topic"),
            max_msg_num: 1,
            invisible_time: 60_000,
            poll_time: 0,
            born_time: current_millis() as i64,
            attempt_id: None,
            rpc: None,
        };
        let mut pop_request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, pop_header);
        pop_request.make_custom_header_to_net();
        let mut pop_response = processor
            .process_request(channel, ctx, &mut pop_request)
            .await
            .expect("pop lite should succeed")
            .expect("pop lite should return a response");

        assert_eq!(ResponseCode::from(pop_response.code()), ResponseCode::Success);
        let body = pop_response
            .take_body()
            .expect("pop lite success response should contain a body");
        let mut bytes = body;
        let message = message_decoder::decode(&mut bytes, true, false, false, false, false)
            .expect("decode pop lite response message");
        assert_eq!(message.topic(), &CheetahString::from_static_str("parent-topic"));
        assert_eq!(
            message
                .property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_INNER_MULTI_DISPATCH
                ))
                .as_deref(),
            Some(lmq_name.as_str())
        );
        assert_eq!(message.body(), Some(Bytes::from_static(b"lite-body")));
        assert_eq!(
            runtime.inner.consumer_offset_manager().query_offset(
                &CheetahString::from_static_str("group-a"),
                &lmq_name,
                0,
            ),
            1
        );
        assert!(runtime
            .inner
            .lite_event_dispatcher()
            .pending_events(&CheetahString::from_static_str("client-1"))
            .is_empty());

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn pop_lite_message_blocks_fifo_for_different_attempt_id() {
        let mut runtime = new_lite_test_runtime("pop-lite-fifo-block").await;
        seed_lite_query_state(&mut runtime);
        seed_lmq_message(&mut runtime, "child-a", b"lite-body-1").await;
        seed_lmq_message(&mut runtime, "child-a", b"lite-body-2").await;
        let lmq_name = CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("child-a lmq"));

        let (mut processor, _) = runtime.init_processor();
        runtime.inner.lite_event_dispatcher().do_full_dispatch(
            &CheetahString::from_static_str("client-1"),
            &CheetahString::from_static_str("group-a"),
            &HashSet::from([lmq_name.clone()]),
        );

        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let first_header = PopLiteMessageRequestHeader {
            client_id: CheetahString::from_static_str("client-1"),
            consumer_group: CheetahString::from_static_str("group-a"),
            topic: CheetahString::from_static_str("parent-topic"),
            max_msg_num: 1,
            invisible_time: 5_000,
            poll_time: 0,
            born_time: current_millis() as i64,
            attempt_id: Some(CheetahString::from_static_str("attempt-1")),
            rpc: None,
        };
        let mut first_request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, first_header);
        first_request.make_custom_header_to_net();
        let first_response = processor
            .process_request(channel, ctx, &mut first_request)
            .await
            .expect("first pop lite should succeed")
            .expect("first pop lite should return a response");
        assert_eq!(ResponseCode::from(first_response.code()), ResponseCode::Success);
        assert_eq!(
            runtime.inner.consumer_offset_manager().query_offset(
                &CheetahString::from_static_str("group-a"),
                &lmq_name,
                0
            ),
            1
        );

        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let second_header = PopLiteMessageRequestHeader {
            client_id: CheetahString::from_static_str("client-1"),
            consumer_group: CheetahString::from_static_str("group-a"),
            topic: CheetahString::from_static_str("parent-topic"),
            max_msg_num: 1,
            invisible_time: 5_000,
            poll_time: 0,
            born_time: current_millis() as i64,
            attempt_id: Some(CheetahString::from_static_str("attempt-2")),
            rpc: None,
        };
        let mut second_request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, second_header);
        second_request.make_custom_header_to_net();
        let second_response = processor
            .process_request(channel, ctx, &mut second_request)
            .await
            .expect("second pop lite should succeed")
            .expect("second pop lite should return a response");
        assert_eq!(ResponseCode::from(second_response.code()), ResponseCode::PollingTimeout);
        assert_eq!(
            runtime.inner.consumer_offset_manager().query_offset(
                &CheetahString::from_static_str("group-a"),
                &lmq_name,
                0
            ),
            1
        );
        assert_eq!(
            runtime
                .inner
                .lite_event_dispatcher()
                .pending_events(&CheetahString::from_static_str("client-1")),
            vec![lmq_name.clone()]
        );

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn pop_lite_message_allows_same_attempt_id_to_continue_fifo_consumption() {
        let mut runtime = new_lite_test_runtime("pop-lite-fifo-same-attempt").await;
        seed_lite_query_state(&mut runtime);
        seed_lmq_message(&mut runtime, "child-a", b"lite-body-1").await;
        seed_lmq_message(&mut runtime, "child-a", b"lite-body-2").await;
        let lmq_name = CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("child-a lmq"));

        let (mut processor, _) = runtime.init_processor();
        runtime.inner.lite_event_dispatcher().do_full_dispatch(
            &CheetahString::from_static_str("client-1"),
            &CheetahString::from_static_str("group-a"),
            &HashSet::from([lmq_name.clone()]),
        );

        let first_channel = create_test_channel().await;
        let first_ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(first_channel.clone()));
        let first_header = PopLiteMessageRequestHeader {
            client_id: CheetahString::from_static_str("client-1"),
            consumer_group: CheetahString::from_static_str("group-a"),
            topic: CheetahString::from_static_str("parent-topic"),
            max_msg_num: 1,
            invisible_time: 5_000,
            poll_time: 0,
            born_time: current_millis() as i64,
            attempt_id: Some(CheetahString::from_static_str("attempt-1")),
            rpc: None,
        };
        let mut first_request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, first_header);
        first_request.make_custom_header_to_net();
        let _ = processor
            .process_request(first_channel, first_ctx, &mut first_request)
            .await
            .expect("first pop lite should succeed")
            .expect("first pop lite should return a response");

        let second_channel = create_test_channel().await;
        let second_ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(second_channel.clone()));
        let second_header = PopLiteMessageRequestHeader {
            client_id: CheetahString::from_static_str("client-1"),
            consumer_group: CheetahString::from_static_str("group-a"),
            topic: CheetahString::from_static_str("parent-topic"),
            max_msg_num: 1,
            invisible_time: 5_000,
            poll_time: 0,
            born_time: current_millis() as i64,
            attempt_id: Some(CheetahString::from_static_str("attempt-1")),
            rpc: None,
        };
        let mut second_request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, second_header);
        second_request.make_custom_header_to_net();
        let second_response = processor
            .process_request(second_channel, second_ctx, &mut second_request)
            .await
            .expect("second pop lite should succeed")
            .expect("second pop lite should return a response");
        assert_eq!(ResponseCode::from(second_response.code()), ResponseCode::Success);
        let second_header = second_response
            .read_custom_header_ref::<PopLiteMessageResponseHeader>()
            .expect("pop lite success response should keep an in-memory response header");
        assert!(second_header.order_count_info.is_some());
        assert_eq!(
            runtime.inner.consumer_offset_manager().query_offset(
                &CheetahString::from_static_str("group-a"),
                &lmq_name,
                0
            ),
            2
        );

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn apply_message_store_role_change_promotes_store_to_master() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-broker-runtime-master-{}", current_millis()));
        let broker_config = Arc::new(BrokerConfig {
            enable_controller_mode: true,
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            enable_controller_mode: true,
            broker_role: BrokerRole::Slave,
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            ..MessageStoreConfig::default()
        });
        let mut runtime = BrokerRuntime::new(broker_config.clone(), message_store_config.clone());
        let mut store = new_controller_mode_message_store(&temp_root, broker_config, message_store_config);
        store.init().await.expect("init message store");
        runtime.inner.message_store = Some(store.clone());

        runtime
            .inner
            .apply_message_store_role_change(BrokerRole::Slave, BrokerReplicaRole::Master, 0, None, 2)
            .await
            .expect("promote store to master");

        let ha_service = store.get_ha_service().expect("ha service should exist");
        let runtime_info = ha_service.get_runtime_info(0);
        assert!(runtime_info.master);
        assert_eq!(runtime_info.ha_client_runtime_info.master_addr, "");
        assert_eq!(store.message_store_config_ref().broker_role, BrokerRole::SyncMaster);

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn apply_message_store_role_change_demotes_store_to_slave() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-broker-runtime-slave-{}", current_millis()));
        let broker_config = Arc::new(BrokerConfig {
            enable_controller_mode: true,
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            enable_controller_mode: true,
            broker_role: BrokerRole::SyncMaster,
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            ..MessageStoreConfig::default()
        });
        let mut runtime = BrokerRuntime::new(broker_config.clone(), message_store_config.clone());
        let mut store = new_controller_mode_message_store(&temp_root, broker_config, message_store_config);
        store.init().await.expect("init message store");
        runtime.inner.message_store = Some(store.clone());

        runtime
            .inner
            .apply_message_store_role_change(
                BrokerRole::SyncMaster,
                BrokerReplicaRole::Slave,
                7,
                Some(&CheetahString::from_static_str("127.0.0.1:10911")),
                3,
            )
            .await
            .expect("demote store to slave");

        let ha_service = store.get_ha_service().expect("ha service should exist");
        let runtime_info = ha_service.get_runtime_info(0);
        assert!(!runtime_info.master);
        assert_eq!(runtime_info.ha_client_runtime_info.master_addr, "127.0.0.1:10911");
        assert_eq!(store.message_store_config_ref().broker_role, BrokerRole::Slave);

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[test]
    fn need_register_returns_true_when_any_namesrv_requests_reregister() {
        assert!(super::need_register(&[false, true, false]));
    }

    #[test]
    fn need_register_returns_false_when_all_namesrvs_are_in_sync() {
        assert!(!super::need_register(&[false, false, false]));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn three_controller_two_broker_controller_mode_bootstrap() {
        let base_port = allocate_controller_test_base_port();
        let root = controller_cluster_root("controller-mode-integration");

        let (controllers, controller_peers) = start_controller_cluster(base_port, &root).await;
        let controller_addrs = controller_addr_list(&controller_peers);
        let controller_leader_manager = controllers
            .iter()
            .find(|manager| manager.is_leader())
            .expect("controller cluster should elect a leader")
            .clone();

        let mut broker_a = new_controller_mode_runtime(
            &root,
            "controller-mode-broker",
            base_port + 21,
            base_port + 22,
            controller_addrs.clone(),
        );
        let mut broker_b = new_controller_mode_runtime(
            &root,
            "controller-mode-broker",
            base_port + 31,
            base_port + 32,
            controller_addrs,
        );

        initialize_controller_mode_broker(&mut broker_a, "broker A").await;
        initialize_controller_mode_broker(&mut broker_b, "broker B").await;
        broker_a.start().await;
        broker_b.start().await;
        bootstrap_broker_against_controller(&mut broker_a, &controller_leader_manager).await;
        broker_a.inner.send_heartbeat().await;
        let broker_a_controller_id = broker_a
            .inner
            .replicas_manager()
            .expect("broker A replicas manager should exist after bootstrap")
            .broker_controller_id();
        let broker_cluster_name = broker_a.inner.broker_config.broker_identity.broker_cluster_name.clone();
        let broker_name = broker_a.inner.broker_config.broker_identity.broker_name.clone();
        wait_until(
            Duration::from_secs(5),
            || {
                controller_leader_manager.heartbeat_manager().is_broker_active(
                    broker_cluster_name.as_str(),
                    broker_name.as_str(),
                    broker_a_controller_id as i64,
                )
            },
            "controller leader to mark broker A active",
        )
        .await;
        bootstrap_broker_against_controller(&mut broker_b, &controller_leader_manager).await;

        wait_until(
            Duration::from_secs(10),
            || {
                let manager_a = broker_a.inner.replicas_manager();
                let manager_b = broker_b.inner.replicas_manager();
                match (manager_a, manager_b) {
                    (Some(manager_a), Some(manager_b)) => {
                        manager_a.register_state() == RegisterState::Registered
                            && manager_b.register_state() == RegisterState::Registered
                            && manager_a.master_broker_id().is_some()
                            && manager_b.master_broker_id().is_some()
                            && manager_a.master_epoch() > 0
                            && manager_b.master_epoch() > 0
                    }
                    _ => false,
                }
            },
            "brokers to finish controller bootstrap",
        )
        .await;

        let manager_a = broker_a
            .inner
            .replicas_manager()
            .expect("broker A replicas manager should exist");
        let manager_b = broker_b
            .inner
            .replicas_manager()
            .expect("broker B replicas manager should exist");

        assert_ne!(
            manager_a.broker_controller_id(),
            manager_b.broker_controller_id(),
            "controller should allocate distinct broker ids"
        );
        assert_eq!(
            manager_a.master_broker_id(),
            manager_b.master_broker_id(),
            "both brokers should converge on the same controller master"
        );

        let master_broker_id = manager_a.master_broker_id().expect("master broker id");
        let broker_a_is_master = manager_a.broker_controller_id() == master_broker_id;
        let broker_b_is_master = manager_b.broker_controller_id() == master_broker_id;
        assert_ne!(
            broker_a_is_master, broker_b_is_master,
            "exactly one broker should become master"
        );
        assert!(
            manager_a.controller_leader_address().is_some() && manager_b.controller_leader_address().is_some(),
            "controller leader discovery should complete for both brokers"
        );

        let broker_a_sync_state = manager_a.sync_state_set().clone();
        let broker_b_sync_state = manager_b.sync_state_set().clone();
        assert_eq!(
            broker_a_sync_state, broker_b_sync_state,
            "brokers should observe the same sync state set"
        );
        assert!(
            broker_a_sync_state.contains(&(master_broker_id as i64)),
            "sync state set should contain the elected master"
        );

        let controller_metadata_target = broker_a
            .inner
            .replicas_manager()
            .expect("broker A replicas manager should exist for metadata lookup")
            .heartbeat_targets()
            .into_iter()
            .next()
            .expect("controller metadata lookup should have at least one target");
        let controller_leader = broker_a
            .inner
            .broker_outer_api
            .get_controller_metadata(&controller_metadata_target)
            .await
            .expect("query controller metadata")
            .controller_leader_address
            .expect("controller metadata should include leader address");
        let (response_header, response_body) = broker_a
            .inner
            .broker_outer_api
            .get_replica_info(
                &controller_leader,
                CheetahString::from_static_str("controller-mode-broker"),
            )
            .await
            .expect("query controller replica info");

        assert_eq!(
            response_header.master_broker_id,
            Some(master_broker_id as i64),
            "controller leader should expose the same elected master"
        );
        assert_eq!(
            response_body.get_sync_state_set().cloned().unwrap_or_default(),
            broker_a_sync_state,
            "controller leader should expose the same sync state set as brokers"
        );

        if broker_a_is_master {
            assert_eq!(broker_a.inner.message_store_config.broker_role, BrokerRole::SyncMaster);
            assert_eq!(broker_b.inner.message_store_config.broker_role, BrokerRole::Slave);
        } else {
            assert_eq!(broker_a.inner.message_store_config.broker_role, BrokerRole::Slave);
            assert_eq!(broker_b.inner.message_store_config.broker_role, BrokerRole::SyncMaster);
        }

        broker_a.shutdown().await;
        broker_b.shutdown().await;
        for controller in &controllers {
            controller.shutdown().await.expect("shutdown controller manager");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn three_controller_two_broker_controller_mode_failover_and_rejoin() {
        let base_port = allocate_controller_test_base_port();
        let root = controller_cluster_root("controller-mode-failover");

        let (controllers, controller_peers) = start_controller_cluster(base_port, &root).await;
        let controller_addrs = controller_addr_list(&controller_peers);
        let controller_leader_manager = controllers
            .iter()
            .find(|manager| manager.is_leader())
            .expect("controller cluster should elect a leader")
            .clone();

        let mut broker_a = new_controller_mode_runtime(
            &root,
            "controller-mode-broker",
            base_port + 21,
            base_port + 22,
            controller_addrs.clone(),
        );
        let mut broker_b = new_controller_mode_runtime(
            &root,
            "controller-mode-broker",
            base_port + 31,
            base_port + 32,
            controller_addrs.clone(),
        );

        initialize_controller_mode_broker(&mut broker_a, "broker A").await;
        initialize_controller_mode_broker(&mut broker_b, "broker B").await;
        broker_a.start().await;
        broker_b.start().await;
        bootstrap_broker_against_controller(&mut broker_a, &controller_leader_manager).await;
        broker_a.inner.send_heartbeat().await;
        let broker_a_controller_id = broker_a
            .inner
            .replicas_manager()
            .expect("broker A replicas manager should exist after bootstrap")
            .broker_controller_id();
        wait_until(
            Duration::from_secs(5),
            || {
                controller_leader_manager.heartbeat_manager().is_broker_active(
                    "controller-test-cluster",
                    "controller-mode-broker",
                    broker_a_controller_id as i64,
                )
            },
            "controller leader to mark broker A active before broker B bootstrap",
        )
        .await;
        bootstrap_broker_against_controller(&mut broker_b, &controller_leader_manager).await;

        wait_until(
            Duration::from_secs(10),
            || {
                let manager_a = broker_a.inner.replicas_manager();
                let manager_b = broker_b.inner.replicas_manager();
                match (manager_a, manager_b) {
                    (Some(manager_a), Some(manager_b)) => {
                        manager_a.register_state() == RegisterState::Registered
                            && manager_b.register_state() == RegisterState::Registered
                            && manager_a.master_broker_id().is_some()
                            && manager_b.master_broker_id().is_some()
                    }
                    _ => false,
                }
            },
            "brokers to finish controller bootstrap",
        )
        .await;

        let initial_master_id = broker_a
            .inner
            .replicas_manager()
            .expect("broker A replicas manager should exist")
            .master_broker_id()
            .expect("controller should elect an initial master");
        let broker_a_is_master = broker_a
            .inner
            .replicas_manager()
            .expect("broker A replicas manager should exist")
            .broker_controller_id()
            == initial_master_id;
        let old_master_controller_id = initial_master_id;
        let surviving_controller_id = if broker_a_is_master {
            broker_b
                .inner
                .replicas_manager()
                .expect("broker B replicas manager should exist")
                .broker_controller_id()
        } else {
            broker_a
                .inner
                .replicas_manager()
                .expect("broker A replicas manager should exist")
                .broker_controller_id()
        };

        if broker_a_is_master {
            broker_a.shutdown().await;
            broker_b.inner.send_heartbeat().await;
        } else {
            broker_b.shutdown().await;
            broker_a.inner.send_heartbeat().await;
        }

        wait_until(
            Duration::from_secs(15),
            || {
                let surviving_broker = if broker_a_is_master { &broker_b } else { &broker_a };
                surviving_broker.inner.replicas_manager().is_some_and(|manager| {
                    manager.master_broker_id() == Some(surviving_controller_id)
                        && manager.master_epoch() > 0
                        && manager.sync_state_set().contains(&(surviving_controller_id as i64))
                }) && surviving_broker.inner.message_store_config.broker_role == BrokerRole::SyncMaster
            },
            "surviving broker to be promoted to master",
        )
        .await;

        let current_controller_leader = if broker_a_is_master {
            BrokerRuntimeInner::discover_controller_leader(&broker_b.inner)
                .await
                .expect("discover controller leader from surviving broker")
        } else {
            BrokerRuntimeInner::discover_controller_leader(&broker_a.inner)
                .await
                .expect("discover controller leader from surviving broker")
        };
        let (replica_header_after_failover, replica_body_after_failover) = if broker_a_is_master {
            broker_b
                .inner
                .broker_outer_api
                .get_replica_info(
                    &current_controller_leader,
                    CheetahString::from_static_str("controller-mode-broker"),
                )
                .await
                .expect("query replica info after failover")
        } else {
            broker_a
                .inner
                .broker_outer_api
                .get_replica_info(
                    &current_controller_leader,
                    CheetahString::from_static_str("controller-mode-broker"),
                )
                .await
                .expect("query replica info after failover")
        };
        assert_eq!(
            replica_header_after_failover.master_broker_id,
            Some(surviving_controller_id as i64),
            "controller should expose the promoted broker as new master"
        );
        assert!(
            replica_body_after_failover
                .get_sync_state_set()
                .cloned()
                .unwrap_or_default()
                .contains(&(surviving_controller_id as i64)),
            "controller sync state set should contain the promoted master"
        );

        let rejoining_store_key = if broker_a_is_master {
            format!("broker-{}", base_port + 21)
        } else {
            format!("broker-{}", base_port + 31)
        };
        let mut rejoining_broker = new_controller_mode_runtime_with_store_key(
            &root,
            &rejoining_store_key,
            "controller-mode-broker",
            base_port + 41,
            base_port + 42,
            controller_addrs,
        );
        initialize_controller_mode_broker(&mut rejoining_broker, "rejoining broker").await;
        rejoining_broker.start().await;
        let current_leader_manager = controllers
            .iter()
            .find(|manager| manager.is_leader())
            .expect("controller cluster should keep a leader")
            .clone();
        bootstrap_broker_against_controller(&mut rejoining_broker, &current_leader_manager).await;
        rejoining_broker.inner.send_heartbeat().await;

        wait_until(
            Duration::from_secs(15),
            || {
                rejoining_broker.inner.replicas_manager().is_some_and(|manager| {
                    manager.register_state() == RegisterState::Registered
                        && manager.broker_controller_id() == old_master_controller_id
                        && manager.master_broker_id() == Some(surviving_controller_id)
                        && manager.sync_state_set().contains(&(surviving_controller_id as i64))
                }) && rejoining_broker.inner.message_store_config.broker_role == BrokerRole::Slave
            },
            "old master to rejoin as slave",
        )
        .await;

        let surviving_broker = if broker_a_is_master { &broker_b } else { &broker_a };
        let surviving_manager = surviving_broker
            .inner
            .replicas_manager()
            .expect("surviving broker replicas manager should exist");
        let rejoining_manager = rejoining_broker
            .inner
            .replicas_manager()
            .expect("rejoining broker replicas manager should exist");
        assert_eq!(
            surviving_manager.master_broker_id(),
            Some(surviving_controller_id),
            "surviving broker should keep master view after rejoin"
        );
        assert_eq!(
            rejoining_manager.master_broker_id(),
            Some(surviving_controller_id),
            "rejoining broker should converge on surviving master"
        );
        assert_eq!(
            rejoining_manager.broker_controller_id(),
            old_master_controller_id,
            "rejoining broker should reuse persisted controller broker id"
        );
        assert_eq!(
            surviving_manager.sync_state_set(),
            rejoining_manager.sync_state_set(),
            "brokers should converge on the same controller sync state set after rejoin"
        );

        rejoining_broker.shutdown().await;
        if broker_a_is_master {
            broker_b.shutdown().await;
        } else {
            broker_a.shutdown().await;
        }
        for controller in &controllers {
            controller.shutdown().await.expect("shutdown controller manager");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn three_controller_two_broker_controller_mode_failover_reregisters_namesrv_and_updates_store_ha() {
        let base_port = allocate_controller_test_base_port();
        let root = controller_cluster_root("controller-mode-namesrv-ha");
        let mut namesrv = start_namesrv(base_port + 90, &root).await;
        let namesrv_addr = namesrv.addr();

        let (controllers, controller_peers) = start_controller_cluster(base_port, &root).await;
        let controller_addrs = controller_addr_list(&controller_peers);
        let controller_leader_manager = controllers
            .iter()
            .find(|manager| manager.is_leader())
            .expect("controller cluster should elect a leader")
            .clone();

        let mut broker_a = new_controller_mode_runtime(
            &root,
            "controller-mode-broker",
            base_port + 21,
            base_port + 22,
            controller_addrs.clone(),
        );
        let mut broker_b = new_controller_mode_runtime(
            &root,
            "controller-mode-broker",
            base_port + 31,
            base_port + 32,
            controller_addrs.clone(),
        );
        configure_namesrv(&mut broker_a, &namesrv_addr).await;
        configure_namesrv(&mut broker_b, &namesrv_addr).await;

        initialize_controller_mode_broker(&mut broker_a, "broker A").await;
        initialize_controller_mode_broker(&mut broker_b, "broker B").await;
        broker_a.start().await;
        broker_b.start().await;
        bootstrap_broker_against_controller(&mut broker_a, &controller_leader_manager).await;
        broker_a.inner.send_heartbeat().await;
        let broker_a_controller_id = broker_a
            .inner
            .replicas_manager()
            .expect("broker A replicas manager should exist after bootstrap")
            .broker_controller_id();
        wait_until(
            Duration::from_secs(5),
            || {
                controller_leader_manager.heartbeat_manager().is_broker_active(
                    "controller-test-cluster",
                    "controller-mode-broker",
                    broker_a_controller_id as i64,
                )
            },
            "controller leader to mark broker A active before broker B bootstrap",
        )
        .await;
        bootstrap_broker_against_controller(&mut broker_b, &controller_leader_manager).await;

        wait_until(
            Duration::from_secs(10),
            || {
                let manager_a = broker_a.inner.replicas_manager();
                let manager_b = broker_b.inner.replicas_manager();
                match (manager_a, manager_b) {
                    (Some(manager_a), Some(manager_b)) => {
                        manager_a.register_state() == RegisterState::Registered
                            && manager_b.register_state() == RegisterState::Registered
                            && manager_a.master_broker_id().is_some()
                            && manager_b.master_broker_id().is_some()
                    }
                    _ => false,
                }
            },
            "brokers to finish controller bootstrap with namesrv enabled",
        )
        .await;

        let manager_a = broker_a
            .inner
            .replicas_manager()
            .expect("broker A replicas manager should exist");
        let manager_b = broker_b
            .inner
            .replicas_manager()
            .expect("broker B replicas manager should exist");
        let initial_master_id = manager_a
            .master_broker_id()
            .expect("controller should elect an initial master");
        let broker_a_controller_id = manager_a.broker_controller_id();
        let broker_b_controller_id = manager_b.broker_controller_id();
        let broker_a_is_master = broker_a_controller_id == initial_master_id;
        let initial_master_addr = if broker_a_is_master {
            broker_a.inner.get_broker_addr().clone()
        } else {
            broker_b.inner.get_broker_addr().clone()
        };
        let initial_slave_addr = if broker_a_is_master {
            broker_b.inner.get_broker_addr().clone()
        } else {
            broker_a.inner.get_broker_addr().clone()
        };
        let initial_slave_controller_id = if broker_a_is_master {
            broker_b_controller_id
        } else {
            broker_a_controller_id
        };
        let broker_cluster_name = broker_a.inner.broker_config.broker_identity.broker_cluster_name.clone();
        let broker_name = broker_a.inner.broker_config.broker_identity.broker_name.clone();

        let initial_member_group = wait_for_namesrv_member_group(
            &namesrv_addr,
            &broker_cluster_name,
            &broker_name,
            Duration::from_secs(15),
            "namesrv to reflect initial master/slave registration",
            |member_group| {
                member_group.broker_addrs.len() == 2
                    && member_group.broker_addrs.get(&MASTER_ID) == Some(&initial_master_addr)
                    && member_group.broker_addrs.get(&initial_slave_controller_id) == Some(&initial_slave_addr)
            },
        )
        .await;
        assert_eq!(initial_member_group.broker_addrs.len(), 2);

        let old_master_controller_id = initial_master_id;
        let surviving_controller_id = if broker_a_is_master {
            broker_b_controller_id
        } else {
            broker_a_controller_id
        };
        let surviving_broker_addr = if broker_a_is_master {
            broker_b.inner.get_broker_addr().clone()
        } else {
            broker_a.inner.get_broker_addr().clone()
        };

        if broker_a_is_master {
            broker_a.shutdown().await;
            broker_b.inner.send_heartbeat().await;
        } else {
            broker_b.shutdown().await;
            broker_a.inner.send_heartbeat().await;
        }

        let surviving_broker = if broker_a_is_master { &broker_b } else { &broker_a };
        let surviving_store = surviving_broker
            .inner
            .message_store
            .as_ref()
            .expect("surviving broker message store should exist")
            .clone();

        wait_until(
            Duration::from_secs(15),
            || {
                let Some(manager) = surviving_broker.inner.replicas_manager() else {
                    return false;
                };
                let Some(ha_service) = surviving_store.get_ha_service() else {
                    return false;
                };
                let ha_runtime_info = ha_service.get_runtime_info(0);
                manager.master_broker_id() == Some(surviving_controller_id)
                    && manager.master_epoch() > 0
                    && manager.sync_state_set() == &HashSet::from([surviving_controller_id as i64])
                    && surviving_store.get_alive_replica_num_in_group() == 1
                    && ha_runtime_info.master
                    && ha_runtime_info.in_sync_slave_nums == 0
                    && ha_runtime_info.ha_client_runtime_info.master_addr.is_empty()
                    && surviving_broker.inner.message_store_config.broker_role == BrokerRole::SyncMaster
            },
            "surviving broker store/HA view to converge after controller failover",
        )
        .await;

        let member_group_after_failover = wait_for_namesrv_member_group(
            &namesrv_addr,
            &broker_cluster_name,
            &broker_name,
            Duration::from_secs(15),
            "namesrv to re-register the promoted master without stale slave entry",
            |member_group| {
                member_group.broker_addrs.len() == 1
                    && member_group.broker_addrs.get(&MASTER_ID) == Some(&surviving_broker_addr)
                    && !member_group.broker_addrs.contains_key(&surviving_controller_id)
            },
        )
        .await;
        assert_eq!(
            member_group_after_failover.broker_addrs,
            std::collections::HashMap::from([(MASTER_ID, surviving_broker_addr.clone())]),
            "namesrv should only retain the promoted master after old master shutdown",
        );

        let rejoining_store_key = if broker_a_is_master {
            format!("broker-{}", base_port + 21)
        } else {
            format!("broker-{}", base_port + 31)
        };
        let mut rejoining_broker = new_controller_mode_runtime_with_store_key(
            &root,
            &rejoining_store_key,
            "controller-mode-broker",
            base_port + 41,
            base_port + 42,
            controller_addrs,
        );
        configure_namesrv(&mut rejoining_broker, &namesrv_addr).await;
        initialize_controller_mode_broker(&mut rejoining_broker, "rejoining broker").await;
        rejoining_broker.start().await;
        let current_leader_manager = controllers
            .iter()
            .find(|manager| manager.is_leader())
            .expect("controller cluster should keep a leader")
            .clone();
        bootstrap_broker_against_controller(&mut rejoining_broker, &current_leader_manager).await;
        rejoining_broker.inner.send_heartbeat().await;

        let rejoining_store = rejoining_broker
            .inner
            .message_store
            .as_ref()
            .expect("rejoining broker message store should exist")
            .clone();
        let rejoining_addr = rejoining_broker.inner.get_broker_addr().clone();
        wait_until(
            Duration::from_secs(15),
            || {
                let Some(rejoining_manager) = rejoining_broker.inner.replicas_manager() else {
                    return false;
                };
                let Some(surviving_manager) = surviving_broker.inner.replicas_manager() else {
                    return false;
                };
                let Some(surviving_ha_service) = surviving_store.get_ha_service() else {
                    return false;
                };
                let Some(rejoining_ha_service) = rejoining_store.get_ha_service() else {
                    return false;
                };
                let surviving_runtime_info = surviving_ha_service.get_runtime_info(0);
                let rejoining_runtime_info = rejoining_ha_service.get_runtime_info(0);
                rejoining_manager.register_state() == RegisterState::Registered
                    && rejoining_manager.broker_controller_id() == old_master_controller_id
                    && rejoining_manager.master_broker_id() == Some(surviving_controller_id)
                    && surviving_manager.sync_state_set() == rejoining_manager.sync_state_set()
                    && surviving_store.get_alive_replica_num_in_group()
                        == surviving_manager.sync_state_set().len().max(1) as i32
                    && rejoining_store.get_alive_replica_num_in_group()
                        == rejoining_manager.sync_state_set().len().max(1) as i32
                    && surviving_runtime_info.master
                    && surviving_runtime_info.in_sync_slave_nums
                        == (surviving_manager.sync_state_set().len().max(1) as i32 - 1).max(0)
                    && !rejoining_runtime_info.master
                    && rejoining_runtime_info.ha_client_runtime_info.master_addr == surviving_broker_addr.as_str()
                    && rejoining_broker.inner.message_store_config.broker_role == BrokerRole::Slave
            },
            "rejoining broker namesrv/store/HA view to converge as slave",
        )
        .await;

        let member_group_after_rejoin = wait_for_namesrv_member_group(
            &namesrv_addr,
            &broker_cluster_name,
            &broker_name,
            Duration::from_secs(15),
            "namesrv to re-register the returning slave under its controller broker id",
            |member_group| {
                member_group.broker_addrs.len() == 2
                    && member_group.broker_addrs.get(&MASTER_ID) == Some(&surviving_broker_addr)
                    && member_group.broker_addrs.get(&old_master_controller_id) == Some(&rejoining_addr)
                    && !member_group.broker_addrs.contains_key(&surviving_controller_id)
            },
        )
        .await;
        assert_eq!(
            member_group_after_rejoin.broker_addrs.get(&MASTER_ID),
            Some(&surviving_broker_addr),
            "namesrv should advertise the promoted broker as master after rejoin",
        );
        assert_eq!(
            member_group_after_rejoin.broker_addrs.get(&old_master_controller_id),
            Some(&rejoining_addr),
            "namesrv should advertise the returning broker under its controller-assigned slave id",
        );

        rejoining_broker.shutdown().await;
        if broker_a_is_master {
            broker_b.shutdown().await;
        } else {
            broker_a.shutdown().await;
        }
        for controller in &controllers {
            let _ = controller.shutdown().await;
        }
        namesrv.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn three_controller_two_broker_controller_leader_failover_keeps_broker_view_consistent() {
        let base_port = allocate_controller_test_base_port();
        let root = controller_cluster_root("controller-leader-failover");

        let (controllers, controller_peers) = start_controller_cluster(base_port, &root).await;
        let controller_addrs = controller_addr_list(&controller_peers);
        let controller_leader_manager = controllers
            .iter()
            .find(|manager| manager.is_leader())
            .expect("controller cluster should elect a leader")
            .clone();

        let mut broker_a = new_controller_mode_runtime(
            &root,
            "controller-mode-broker",
            base_port + 21,
            base_port + 22,
            controller_addrs.clone(),
        );
        let mut broker_b = new_controller_mode_runtime(
            &root,
            "controller-mode-broker",
            base_port + 31,
            base_port + 32,
            controller_addrs,
        );

        initialize_controller_mode_broker(&mut broker_a, "broker A").await;
        initialize_controller_mode_broker(&mut broker_b, "broker B").await;
        broker_a.start().await;
        broker_b.start().await;
        bootstrap_broker_against_controller(&mut broker_a, &controller_leader_manager).await;
        broker_a.inner.send_heartbeat().await;
        bootstrap_broker_against_controller(&mut broker_b, &controller_leader_manager).await;

        wait_until(
            Duration::from_secs(10),
            || {
                let manager_a = broker_a.inner.replicas_manager();
                let manager_b = broker_b.inner.replicas_manager();
                match (manager_a, manager_b) {
                    (Some(manager_a), Some(manager_b)) => {
                        manager_a.register_state() == RegisterState::Registered
                            && manager_b.register_state() == RegisterState::Registered
                            && manager_a.master_broker_id().is_some()
                            && manager_b.master_broker_id().is_some()
                            && manager_a.controller_leader_address().is_some()
                            && manager_b.controller_leader_address().is_some()
                    }
                    _ => false,
                }
            },
            "brokers to finish controller bootstrap before controller leader failover",
        )
        .await;

        let expected_master_broker_id = broker_a
            .inner
            .replicas_manager()
            .expect("broker A replicas manager should exist")
            .master_broker_id()
            .expect("controller should elect a broker master");
        let broker_a_controller_leader = broker_a
            .inner
            .replicas_manager()
            .and_then(|manager| manager.controller_leader_address().cloned())
            .expect("broker A should know controller leader");
        let broker_b_controller_leader = broker_b
            .inner
            .replicas_manager()
            .and_then(|manager| manager.controller_leader_address().cloned())
            .expect("broker B should know controller leader");
        assert_eq!(
            broker_a_controller_leader, broker_b_controller_leader,
            "brokers should agree on controller leader before failover"
        );

        let old_controller_leader = broker_a_controller_leader;
        let old_controller_leader_manager = controllers
            .iter()
            .find(|manager| manager.controller_config().listen_addr.to_string() == old_controller_leader.as_str())
            .expect("find old controller leader manager")
            .clone();
        old_controller_leader_manager
            .shutdown()
            .await
            .expect("shutdown old controller leader");

        wait_until(
            Duration::from_secs(15),
            || {
                controllers.iter().filter(|manager| manager.is_leader()).count() == 1
                    && controllers.iter().any(|manager| {
                        manager.is_leader()
                            && manager.controller_config().listen_addr.to_string() != old_controller_leader.as_str()
                    })
            },
            "remaining controller nodes to elect a new leader",
        )
        .await;

        broker_a.inner.send_heartbeat().await;
        broker_b.inner.send_heartbeat().await;

        wait_until(
            Duration::from_secs(15),
            || {
                let manager_a = broker_a.inner.replicas_manager();
                let manager_b = broker_b.inner.replicas_manager();
                match (manager_a, manager_b) {
                    (Some(manager_a), Some(manager_b)) => {
                        let leader_a = manager_a.controller_leader_address().cloned();
                        let leader_b = manager_b.controller_leader_address().cloned();
                        leader_a.is_some()
                            && leader_a == leader_b
                            && leader_a
                                .as_ref()
                                .is_some_and(|leader| leader.as_str() != old_controller_leader.as_str())
                            && manager_a.master_broker_id() == Some(expected_master_broker_id)
                            && manager_b.master_broker_id() == Some(expected_master_broker_id)
                    }
                    _ => false,
                }
            },
            "brokers to refresh controller leader and preserve broker master view",
        )
        .await;

        let refreshed_controller_leader = broker_a
            .inner
            .replicas_manager()
            .and_then(|manager| manager.controller_leader_address().cloned())
            .expect("broker A should refresh controller leader");
        let (replica_header, replica_body) = broker_a
            .inner
            .broker_outer_api
            .get_replica_info(
                &refreshed_controller_leader,
                CheetahString::from_static_str("controller-mode-broker"),
            )
            .await
            .expect("query replica info from new controller leader");
        assert_eq!(
            replica_header.master_broker_id,
            Some(expected_master_broker_id as i64),
            "controller leader failover should not change broker master ownership"
        );
        assert!(
            replica_body
                .get_sync_state_set()
                .cloned()
                .unwrap_or_default()
                .contains(&(expected_master_broker_id as i64)),
            "new controller leader should expose a sync state set containing the elected broker master"
        );

        let broker_a_manager = broker_a
            .inner
            .replicas_manager()
            .expect("broker A replicas manager should exist");
        let broker_b_manager = broker_b
            .inner
            .replicas_manager()
            .expect("broker B replicas manager should exist");
        let broker_a_is_master = broker_a_manager.broker_controller_id() == expected_master_broker_id;
        let broker_b_is_master = broker_b_manager.broker_controller_id() == expected_master_broker_id;
        assert_ne!(
            broker_a_is_master, broker_b_is_master,
            "controller leader failover should keep exactly one broker master"
        );
        if broker_a_is_master {
            assert_eq!(broker_a.inner.message_store_config.broker_role, BrokerRole::SyncMaster);
            assert_eq!(broker_b.inner.message_store_config.broker_role, BrokerRole::Slave);
        } else {
            assert_eq!(broker_a.inner.message_store_config.broker_role, BrokerRole::Slave);
            assert_eq!(broker_b.inner.message_store_config.broker_role, BrokerRole::SyncMaster);
        }

        broker_a.shutdown().await;
        broker_b.shutdown().await;
        for controller in &controllers {
            let _ = controller.shutdown().await;
        }
        let _ = std::fs::remove_dir_all(root);
    }
}
