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
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_common::common::statistics::state_getter::StateGetter;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_common::UtilAll::compute_next_morning_time_millis;
use rocketmq_remoting::base::channel_event_listener::ChannelEventListener;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigSerializeWrapper;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::protocol::namesrv::RegisterBrokerResult;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
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
use rocketmq_store::message_store::local_file_message_store::LocalFileMessageStore;
use rocketmq_store::stats::broker_stats::BrokerStats;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use rocketmq_store::timer::timer_message_store::TimerMessageStore;
use tokio::sync::Mutex;
use tracing::error;
use tracing::info;
use tracing::warn;

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
use crate::controller::replicas_manager::ReplicasManager;
use crate::failover::escape_bridge::EscapeBridge;
use crate::filter::commit_log_dispatcher_calc_bit_map::CommitLogDispatcherCalcBitMap;
use crate::filter::manager::consumer_filter_manager::ConsumerFilterManager;
use crate::hook::batch_check_before_put_message::BatchCheckBeforePutMessageHook;
use crate::hook::check_before_put_message::CheckBeforePutMessageHook;
use crate::hook::schedule_message_hook::ScheduleMessageHook;
use crate::latency::broker_fast_failure::BrokerFastFailure;
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
use crate::processor::notification_processor::NotificationProcessor;
use crate::processor::peek_message_processor::PeekMessageProcessor;
use crate::processor::polling_info_processor::PollingInfoProcessor;
use crate::processor::pop_inflight_message_counter::PopInflightMessageCounter;
use crate::processor::pop_message_processor::PopMessageProcessor;
use crate::processor::pull_message_processor::PullMessageProcessor;
use crate::processor::query_assignment_processor::QueryAssignmentProcessor;
use crate::processor::query_message_processor::QueryMessageProcessor;
use crate::processor::reply_message_processor::ReplyMessageProcessor;
use crate::processor::send_message_processor::SendMessageProcessor;
use crate::processor::BrokerProcessorType;
use crate::processor::BrokerRequestProcessor;
use crate::schedule::schedule_message_service::ScheduleMessageService;
use crate::slave::slave_synchronize::SlaveSynchronize;
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

pub(crate) struct BrokerRuntime {
    #[cfg(feature = "local_file_store")]
    inner: ArcMut<BrokerRuntimeInner<LocalFileMessageStore>>,
    broker_runtime: Option<RocketMQRuntime>,
    shutdown_hook: Option<BrokerShutdownHook>,
    consumer_ids_change_listener: Arc<Box<dyn ConsumerIdsChangeListener + Send + Sync + 'static>>,
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
        let consumer_ids_change_listener: Arc<Box<dyn ConsumerIdsChangeListener + Send + Sync + 'static>> =
            Arc::new(Box::new(DefaultConsumerIdsChangeListener {}));
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
            consumer_filter_manager: Some(ConsumerFilterManager::new(broker_config, message_store_config)),

            consumer_order_info_manager: None,
            message_store: None,
            broker_stats: None,
            schedule_message_service: None,
            timer_message_store: None,
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
            cold_data_pull_request_hold_service: None,
            cold_data_cg_ctr_service: None,
            is_schedule_service_start: Arc::new(Default::default()),
            is_transaction_check_service_start: Arc::new(Default::default()),
            client_housekeeping_service: None,
            pop_message_processor: None,
            ack_message_processor: None,
            notification_processor: None,
            query_assignment_processor: None,
            broker_attached_plugins: vec![],
            transactional_message_service: None,
            slave_synchronize: None,
            last_sync_time_ms: AtomicU64::new(get_current_millis()),
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
        /* if let Some(message_store) = &mut self.inner.message_store {
            message_store.shutdown()
        }

        self.inner.topic_config_manager().persist();
        info!("[Broker shutdown]TopicConfigManager persist success");
        let _ = self.inner.topic_config_manager_mut().stop();

        if let Some(pull_request_hold_service) = self.inner.pull_request_hold_service.as_mut() {
            pull_request_hold_service.shutdown();
        }

        if let Some(runtime) = self.broker_runtime.take() {
            runtime.shutdown();
        }*/
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
        if let Some(timer_message_store) = self.inner.timer_message_store.as_mut() {
            timer_message_store.shutdown();
        }

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
            if self.inner.message_store_config.is_timer_wheel_enable() {
                let time_message_store = TimerMessageStore::new(Some(message_store.clone()));
                message_store.set_timer_message_store(Arc::new(time_message_store));
            }
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
            unimplemented!("Start controller mode(Support for future versions)");
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

        if self.inner.broker_config.timer_wheel_config.timer_wheel_enable {
            if let Some(timer_message_store) = &mut self.inner.timer_message_store {
                info!("Timer wheel is enabled, load timer message store");
                result &= timer_message_store.load();
                if !result {
                    warn!("Load timer message store failed");
                    return false;
                }
            } else {
                warn!("Timer wheel is enabled, but timer message store is None");
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
            self.initial_acl();
            self.initial_rpc_hooks();
            self.initial_request_pipeline();
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

        //EndTransactionProcessor
        broker_request_processor.register_processor(
            RequestCode::EndTransaction as i32,
            BrokerProcessorType::EndTransaction(ArcMut::new(EndTransactionProcessor::new(
                self.inner.transactional_message_service.as_ref().unwrap().clone(),
                self.inner.clone(),
            ))),
        );
        let admin_broker_processor = ArcMut::new(AdminBrokerProcessor::new(self.inner.clone()));
        broker_request_processor.register_default_processor(BrokerProcessorType::AdminBroker(admin_broker_processor));

        (broker_request_processor.clone(), broker_request_processor)
    }

    #[allow(clippy::incompatible_msrv)]
    async fn initialize_scheduled_tasks(&mut self) {
        let initial_delay = compute_next_morning_time_millis() - get_current_millis();
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
                        if get_current_millis() - inner_clone.last_sync_time_ms.load(Ordering::Relaxed) > 10_000 {
                            if let Some(slave_synchronize) = &inner_clone.slave_synchronize {
                                slave_synchronize.sync_all().await;
                            }
                            inner_clone
                                .last_sync_time_ms
                                .store(get_current_millis(), Ordering::Relaxed);
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

    fn initial_acl(&mut self) {}

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

        if let Some(timer_message_store) = self.inner.timer_message_store.as_mut() {
            timer_message_store.start();
        }
        if let Some(replicas_manager) = self.inner.replicas_manager.as_mut() {
            replicas_manager.start();
        }

        let (request_processor, fast_request_processor) = self.init_processor();

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
            (get_current_millis() as i64 + self.inner.message_store_config.disappear_time_after_start) as u64,
            Ordering::Release,
        );
        if self.inner.message_store_config.total_replicas > 1 && self.inner.broker_config.enable_slave_acting_master {
            self.inner.is_isolated.store(true, Ordering::Release);
        }

        self.inner.broker_outer_api.start().await;
        self.start_basic_service().await;

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
                if get_current_millis() < start_time {
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
        }

        if self.inner.broker_config.skip_pre_online {
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
                    info!("Skip send heartbeat for broker is isolated");
                    return Ok(());
                }
                inner_.send_heartbeat().await;
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
            let topic_full_name = CheetahString::from_string(NamespaceUtil::wrap_namespace(instance_id, topic));
            self.broker_runtime_inner
                .consumer_manager
                .find_subscription_data(
                    CheetahString::from_string(NamespaceUtil::wrap_namespace(instance_id, group)).as_ref(),
                    topic_full_name.as_ref(),
                )
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
    timer_message_store: Option<TimerMessageStore>,
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
    ack_message_processor: Option<ArcMut<AckMessageProcessor<MS>>>,
    notification_processor: Option<ArcMut<NotificationProcessor<MS>>>,
    query_assignment_processor: Option<ArcMut<QueryAssignmentProcessor<MS>>>,
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
    pub fn timer_message_store_mut(&mut self) -> &mut Option<TimerMessageStore> {
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
    pub fn timer_message_store(&self) -> &Option<TimerMessageStore> {
        &self.timer_message_store
    }

    #[inline]
    pub fn timer_message_store_unchecked(&self) -> &TimerMessageStore {
        unsafe { self.timer_message_store.as_ref().unwrap_unchecked() }
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
        self.timer_message_store = Some(timer_message_store);
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

        if self.broker_config.enable_split_registration
            || force_register
            || need_register(
                self.broker_config.broker_identity.broker_cluster_name.clone().as_str(),
                self.broker_config.broker_ip1.clone().as_str(),
                self.broker_config.broker_identity.broker_name.clone().as_str(),
                self.broker_config.broker_identity.broker_id,
                self.broker_config.register_broker_timeout_mills,
                self.broker_config.is_in_broker_container,
            )
        {
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
        error!("unimplemented")
    }
}

fn need_register(
    _cluster_name: &str,
    _broker_addr: &str,
    _broker_name: &str,
    _broker_id: u64,
    _register_timeout_mills: i32,
    _in_broker_container: bool,
) -> bool {
    unimplemented!()
}
