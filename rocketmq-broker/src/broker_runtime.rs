/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_common::common::statistics::state_getter::StateGetter;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_common::UtilAll::compute_next_morning_time_millis;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigSerializeWrapper;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_remoting::remoting_server::server::RocketMQServer;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::store_enum::StoreType;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::log_file::MessageStore;
use rocketmq_store::message_store::default_message_store::DefaultMessageStore;
use rocketmq_store::stats::broker_stats::BrokerStats;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use rocketmq_store::timer::timer_message_store::TimerMessageStore;
use tracing::info;
use tracing::warn;

use crate::broker::broker_hook::BrokerShutdownHook;
use crate::client::default_consumer_ids_change_listener::DefaultConsumerIdsChangeListener;
use crate::client::manager::consumer_manager::ConsumerManager;
use crate::client::manager::producer_manager::ProducerManager;
use crate::client::net::broker_to_client::Broker2Client;
use crate::client::rebalance::rebalance_lock_manager::RebalanceLockManager;
use crate::failover::escape_bridge::EscapeBridge;
use crate::filter::manager::consumer_filter_manager::ConsumerFilterManager;
use crate::hook::batch_check_before_put_message::BatchCheckBeforePutMessageHook;
use crate::hook::check_before_put_message::CheckBeforePutMessageHook;
use crate::long_polling::long_polling_service::pull_request_hold_service::PullRequestHoldService;
use crate::long_polling::notify_message_arriving_listener::NotifyMessageArrivingListener;
use crate::offset::manager::broadcast_offset_manager::BroadcastOffsetManager;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::offset::manager::consumer_order_info_manager::ConsumerOrderInfoManager;
use crate::out_api::broker_outer_api::BrokerOuterAPI;
use crate::processor::ack_message_processor::AckMessageProcessor;
use crate::processor::admin_broker_processor::AdminBrokerProcessor;
use crate::processor::change_invisible_time_processor::ChangeInvisibleTimeProcessor;
use crate::processor::client_manage_processor::ClientManageProcessor;
use crate::processor::consumer_manage_processor::ConsumerManageProcessor;
use crate::processor::default_pull_message_result_handler::DefaultPullMessageResultHandler;
use crate::processor::end_transaction_processor::EndTransactionProcessor;
use crate::processor::pop_inflight_message_counter::PopInflightMessageCounter;
use crate::processor::pop_message_processor::PopMessageProcessor;
use crate::processor::pull_message_processor::PullMessageProcessor;
use crate::processor::pull_message_result_handler::PullMessageResultHandler;
use crate::processor::query_assignment_processor::QueryAssignmentProcessor;
use crate::processor::query_message_processor::QueryMessageProcessor;
use crate::processor::reply_message_processor::ReplyMessageProcessor;
use crate::processor::send_message_processor::SendMessageProcessor;
use crate::processor::BrokerRequestProcessor;
use crate::schedule::schedule_message_service::ScheduleMessageService;
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

pub(crate) struct BrokerRuntime {
    /*    store_host: SocketAddr,
    broker_config: Arc<BrokerConfig>,
    message_store_config: Arc<MessageStoreConfig>,
    server_config: Arc<ServerConfig>,
    topic_config_manager: TopicConfigManager,
    topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    consumer_offset_manager: ConsumerOffsetManager,
    #[cfg(feature = "local_file_store")]
    subscription_group_manager: Arc<SubscriptionGroupManager<DefaultMessageStore>>,
    consumer_filter_manager: Arc<ConsumerFilterManager>,
    consumer_order_info_manager: Arc<ConsumerOrderInfoManager<DefaultMessageStore>>,
    #[cfg(feature = "local_file_store")]
    message_store: Option<ArcMut<DefaultMessageStore>>,
    #[cfg(feature = "local_file_store")]
    broker_stats: Option<Arc<BrokerStats<DefaultMessageStore>>>,
    //message_store: Option<Arc<Mutex<LocalFileMessageStore>>>,
    schedule_message_service: ScheduleMessageService,
    timer_message_store: Option<TimerMessageStore>,

    broker_outer_api: Arc<BrokerOuterAPI>,

    broker_runtime: Option<RocketMQRuntime>,
    producer_manager: Arc<ProducerManager>,
    consumer_manager: Arc<ConsumerManager>,
    broadcast_offset_manager: Arc<BroadcastOffsetManager>,
    drop: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    shutdown_hook: Option<BrokerShutdownHook>,
    broker_stats_manager: Arc<BrokerStatsManager>,
    topic_queue_mapping_clean_service: Option<Arc<TopicQueueMappingCleanService>>,
    update_master_haserver_addr_periodically: bool,
    should_start_time: Arc<AtomicU64>,
    is_isolated: Arc<AtomicBool>,
    #[cfg(feature = "local_file_store")]
    pull_request_hold_service: Option<ArcMut<PullRequestHoldService<DefaultMessageStore>>>,
    rebalance_lock_manager: Arc<RebalanceLockManager>,
    broker_member_group: Arc<BrokerMemberGroup>,

    #[cfg(feature = "local_file_store")]
    transactional_message_check_listener:
        Option<Arc<DefaultTransactionalMessageCheckListener<DefaultMessageStore>>>,
    transactional_message_check_service: Option<Arc<TransactionalMessageCheckService>>,
    transaction_metrics_flush_service: Option<Arc<TransactionMetricsFlushService>>,
    topic_route_info_manager: Arc<TopicRouteInfoManager>,
    #[cfg(feature = "local_file_store")]
    escape_bridge: ArcMut<EscapeBridge<DefaultMessageStore>>,
    pop_inflight_message_counter: Arc<PopInflightMessageCounter>,*/
    #[cfg(feature = "local_file_store")]
    inner: ArcMut<BrokerRuntimeInner<DefaultMessageStore>>,
    #[cfg(feature = "local_file_store")]
    transactional_message_service:
        Option<ArcMut<DefaultTransactionalMessageService<DefaultMessageStore>>>,
    broker_runtime: Option<RocketMQRuntime>,
    shutdown: Arc<AtomicBool>,
    shutdown_hook: Option<BrokerShutdownHook>,
    // receiver for shutdown signal
    pub(crate) shutdown_rx: Option<tokio::sync::broadcast::Receiver<()>>,
}

impl BrokerRuntime {
    pub(crate) fn new(
        broker_config: BrokerConfig,
        message_store_config: MessageStoreConfig,
        server_config: ServerConfig,
    ) -> Self {
        let store_host = format!("{}:{}", broker_config.broker_ip1, broker_config.listen_port)
            .parse::<SocketAddr>()
            .expect("parse store_host failed");
        let runtime = RocketMQRuntime::new_multi(10, "broker-thread");
        let broker_outer_api = BrokerOuterAPI::new(Arc::new(TokioClientConfig::default()));

        let topic_queue_mapping_manager =
            TopicQueueMappingManager::new(Arc::new(broker_config.clone()));
        let mut broker_member_group = BrokerMemberGroup::new(
            broker_config.broker_identity.broker_cluster_name.clone(),
            broker_config.broker_identity.broker_name.clone(),
        );
        broker_member_group.broker_addrs.insert(
            broker_config.broker_identity.broker_id,
            broker_config.get_broker_addr().into(),
        );
        let producer_manager = ProducerManager::new();
        let consumer_manager = ConsumerManager::new_with_broker_stats(
            Box::new(DefaultConsumerIdsChangeListener {}),
            Arc::new(broker_config.clone()),
        );

        let should_start_time = Arc::new(AtomicU64::new(0));
        let pop_inflight_message_counter =
            PopInflightMessageCounter::new(should_start_time.clone());

        let mut inner = ArcMut::new(BrokerRuntimeInner::<DefaultMessageStore> {
            store_host,
            broker_config,
            message_store_config,
            server_config,
            topic_config_manager: None,
            topic_queue_mapping_manager,
            consumer_offset_manager: Default::default(),
            subscription_group_manager: None,
            consumer_filter_manager: Some(Default::default()),
            consumer_order_info_manager: None,
            message_store: None,
            broker_stats: None,
            schedule_message_service: Default::default(),
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
        });
        let mut stats_manager = BrokerStatsManager::new(Arc::new(inner.broker_config.clone()));
        stats_manager.set_producer_state_getter(Arc::new(ProducerStateGetter {
            broker_runtime_inner: inner.clone(),
        }));
        stats_manager.set_consumer_state_getter(Arc::new(ConsumerStateGetter {
            broker_runtime_inner: inner.clone(),
        }));
        let stats_manager = Arc::new(stats_manager);
        inner.topic_config_manager = Some(TopicConfigManager::new(inner.clone()));
        inner.topic_route_info_manager = Some(TopicRouteInfoManager::new(inner.clone()));
        inner.escape_bridge = Some(EscapeBridge::new(inner.clone()));
        inner.subscription_group_manager = Some(SubscriptionGroupManager::new(inner.clone()));
        inner.consumer_order_info_manager = Some(ConsumerOrderInfoManager::new(inner.clone()));
        inner.broker_stats_manager = Some(stats_manager);

        Self {
            inner,
            transactional_message_service: None,
            broker_runtime: Some(runtime),
            shutdown: Arc::new(AtomicBool::new(false)),
            shutdown_hook: None,
            shutdown_rx: None,
        }
    }

    pub(crate) fn broker_config(&self) -> &BrokerConfig {
        self.inner.broker_config()
    }

    pub(crate) fn message_store_config(&self) -> &MessageStoreConfig {
        self.inner.message_store_config()
    }

    pub fn shutdown(&mut self) {
        self.inner.broker_outer_api.shutdown();
        if let Some(message_store) = &mut self.inner.message_store {
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
        }
    }

    pub(crate) fn shutdown_basic_service(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);

        if let Some(hook) = self.shutdown_hook.as_ref() {
            hook.before_shutdown();
        }
    }
}

/*impl Drop for BrokerRuntime {
    fn drop(&mut self) {
        let result =
            self.drop
                .clone()
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed);
        if result.is_ok() {
            self.shutdown();
        }
    }
}*/

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

    ///Load the original configuration data from the corresponding configuration files located
    /// under the ${HOME}\config directory.
    fn initialize_metadata(&self) -> bool {
        info!("======Starting initialize metadata========");
        self.inner.topic_config_manager().load()
            && self.inner.topic_queue_mapping_manager.load()
            && self.inner.consumer_offset_manager.load()
            && self.inner.subscription_group_manager().load()
            && self.inner.consumer_filter_manager().load()
            && self.inner.consumer_order_info_manager().load()
    }

    async fn initialize_message_store(&mut self) -> bool {
        if self.inner.message_store_config.store_type == StoreType::LocalFile {
            info!("Use local file as message store");
            let mut message_store = ArcMut::new(DefaultMessageStore::new(
                Arc::new(self.inner.message_store_config.clone()),
                Arc::new(self.inner.broker_config.clone()),
                self.inner.topic_config_manager().topic_config_table(),
                self.inner.broker_stats_manager.clone(),
                false,
            ));
            let message_store_clone = message_store.clone();
            message_store.set_message_store_arc(Some(message_store_clone));
            if self.inner.message_store_config.is_timer_wheel_enable() {
                let time_message_store = TimerMessageStore::new(Some(message_store.clone()));
                message_store.set_timer_message_store(Arc::new(time_message_store));
            }
            //Maybe need to set message store to other components
            /*self.consumer_offset_manager
                .set_message_store(Some(message_store.clone()));
            self.topic_config_manager
                .set_message_store(Some(message_store.clone()));*/
            self.inner.broker_stats = Some(BrokerStats::new(message_store.clone()));
            self.inner.message_store = Some(message_store);
        } else if self.inner.message_store_config.store_type == StoreType::RocksDB {
            info!("Use RocksDB as message store");
        } else {
            warn!("Unknown store type");
            return false;
        }
        true
    }

    async fn recover_initialize_service(&mut self) -> bool {
        let mut result: bool = true;

        if self.inner.broker_config().enable_controller_mode {
            info!("Start controller mode(Support for future versions)");
            unimplemented!()
        }
        if self.inner.message_store.is_some() {
            self.register_message_store_hook();
            self.inner.message_store.as_mut().unwrap().load().await;
        }

        if self
            .inner
            .broker_config
            .timer_wheel_config
            .timer_wheel_enable
        {
            result &= self.inner.timer_message_store.as_mut().unwrap().load();
        }
        result &= self.inner.schedule_message_service.load();

        if result {
            self.initialize_remoting_server();
            self.initialize_resources();
            self.initialize_scheduled_tasks().await;
            self.initial_transaction();
            self.initial_acl();
            self.initial_rpc_hooks();
            self.initial_request_pipeline();
        }
        result
    }

    pub fn register_message_store_hook(&mut self) {
        let config = self.inner.message_store_config.clone();
        let arc = self.inner.topic_config_manager().topic_config_table();
        if let Some(ref mut message_store) = self.inner.message_store {
            message_store.set_put_message_hook(Box::new(CheckBeforePutMessageHook::new(
                message_store.clone(),
                Arc::new(config),
            )));
            message_store.set_put_message_hook(Box::new(BatchCheckBeforePutMessageHook::new(arc)));
        }
    }

    fn initialize_remoting_server(&mut self) {

        // fast broker remoting_server implementation in future versions
    }

    fn initialize_resources(&mut self) {
        self.inner.topic_queue_mapping_clean_service = Some(TopicQueueMappingCleanService);
    }

    fn init_processor(
        &mut self,
    ) -> BrokerRequestProcessor<
        DefaultMessageStore,
        DefaultTransactionalMessageService<DefaultMessageStore>,
    > {
        let send_message_processor = SendMessageProcessor::new(
            /*self.topic_queue_mapping_manager.clone(),
            self.subscription_group_manager.clone(),
            self.topic_config_manager.clone(),
            self.broker_config.clone(),
            self.message_store.clone().unwrap(),*/
            self.transactional_message_service.as_ref().unwrap().clone(),
            /* self.rebalance_lock_manager.clone(),
            self.broker_stats_manager.clone(),
            self.store_host,*/
            self.inner.clone(),
        );
        let reply_message_processor = ReplyMessageProcessor::new(
            /*self.topic_queue_mapping_manager.clone(),
            self.subscription_group_manager.clone(),
            self.topic_config_manager.clone(),
            self.broker_config.clone(),
            self.message_store.clone().unwrap(),
            self.rebalance_lock_manager.clone(),
            self.broker_stats_manager.clone(),
            Some(self.producer_manager.clone()),
            self.transactional_message_service.as_ref().unwrap().clone(),
            self.store_host,*/
            self.transactional_message_service.as_ref().unwrap().clone(),
            self.inner.clone(),
        );
        let mut pull_message_result_handler =
            ArcMut::new(Box::new(DefaultPullMessageResultHandler::new(
                /*self.message_store_config.clone(),
                Arc::new(self.topic_config_manager.clone()),
                Arc::new(self.consumer_offset_manager.clone()),
                self.consumer_manager.clone(),
                self.broadcast_offset_manager.clone(),
                self.broker_stats_manager.clone(),
                self.broker_config.clone(),
                Arc::new(Default::default()),*/
                Arc::new(Default::default()), //optimize
                self.inner.clone(),
            )) as Box<dyn PullMessageResultHandler>);
        //let message_store = self.message_store.clone().unwrap();
        let pull_message_processor = ArcMut::new(PullMessageProcessor::new(
            pull_message_result_handler.clone(),
            /*self.broker_config.clone(),
            self.subscription_group_manager.clone(),
            Arc::new(self.topic_config_manager.clone()),
            self.topic_queue_mapping_manager.clone(),
            self.consumer_manager.clone(),
            self.consumer_filter_manager.clone(),
            Arc::new(self.consumer_offset_manager.clone()),
            Arc::new(BroadcastOffsetManager::default()),
            message_store.clone(),
            self.broker_outer_api.clone(),*/
            self.inner.clone(),
        ));

        let consumer_manage_processor = ConsumerManageProcessor::new(
            /*self.broker_config.clone(),
            self.consumer_manager.clone(),
            self.topic_queue_mapping_manager.clone(),
            self.subscription_group_manager.clone(),
            Arc::new(self.consumer_offset_manager.clone()),
            Arc::new(self.topic_config_manager.clone()),
            self.message_store.clone().unwrap(),*/
            self.inner.clone(),
        );
        self.inner.pull_request_hold_service = Some(PullRequestHoldService::new(
            /* message_store.clone(), */
            pull_message_processor.clone(),
            /* self.broker_config.clone(), */
            self.inner.clone(),
        ));

        let pull_message_result_handler = pull_message_result_handler.as_mut().as_mut();
        pull_message_result_handler
            .as_any_mut()
            .downcast_mut::<DefaultPullMessageResultHandler<DefaultMessageStore>>()
            .expect("downcast DefaultPullMessageResultHandler failed")
            .set_pull_request_hold_service(self.inner.clone());

        let inner = self.inner.clone();
        self.inner
            .message_store
            .as_mut()
            .unwrap()
            .set_message_arriving_listener(Some(Arc::new(Box::new(
                NotifyMessageArrivingListener::new(inner),
            ))));
        let query_message_processor = QueryMessageProcessor::new(
            /*self.inner.message_store_config.clone(),
            message_store.clone(),*/
            self.inner.clone(),
        );

        let admin_broker_processor = AdminBrokerProcessor::new(
            /*self.broker_config.clone(),
            self.server_config.clone(),
            self.message_store_config.clone(),
            self.topic_config_manager.clone(),
            self.consumer_offset_manager.clone(),
            self.topic_queue_mapping_manager.clone(),
            self.message_store.as_ref().unwrap().clone(),
            self.schedule_message_service.clone(),
            self.broker_stats.clone(),
            self.consumer_manager.clone(),
            self.broker_outer_api.clone(),
            self.broker_stats_manager.clone(),
            self.rebalance_lock_manager.clone(),
            self.broker_member_group.clone(),
            self.pop_inflight_message_counter.clone(),*/
            self.inner.clone(),
        );
        let pop_message_processor = ArcMut::new(PopMessageProcessor::new(
            /*self.consumer_manager.clone(),
            Arc::new(self.consumer_offset_manager.clone()),
            self.consumer_order_info_manager.clone(),
            self.broker_config.clone(),
            self.message_store.clone().unwrap(),
            self.message_store_config.clone(),
            Arc::new(self.topic_config_manager.clone()),
            self.subscription_group_manager.clone(),
            self.consumer_filter_manager.clone(),
            self.pop_inflight_message_counter.clone(),
            self.store_host,
            self.escape_bridge.clone(),*/
            self.inner.clone(),
        ));
        let ack_message_processor = ArcMut::new(AckMessageProcessor::new(
            /*self.topic_config_manager.clone(),
            self.message_store.as_ref().unwrap().clone(),
            self.escape_bridge.clone(),
            self.broker_config.clone(),
            self.pop_inflight_message_counter.clone(),
            self.store_host,
            Arc::new(self.consumer_offset_manager.clone()),
            pop_message_processor.clone(),
            self.consumer_order_info_manager.clone(),*/
            self.inner.clone(),
            pop_message_processor.clone(),
        ));
        BrokerRequestProcessor {
            send_message_processor: ArcMut::new(send_message_processor),
            pull_message_processor,
            peek_message_processor: Default::default(),
            pop_message_processor: pop_message_processor.clone(),
            ack_message_processor,
            change_invisible_time_processor: ArcMut::new(ChangeInvisibleTimeProcessor::new(
                /*self.broker_config.clone(),
                self.topic_config_manager.clone(),
                self.message_store.clone().unwrap(),
                Arc::new(self.consumer_offset_manager.clone()),
                self.consumer_order_info_manager.clone(),
                self.broker_stats_manager.clone(),
                self.escape_bridge.clone(),
                pop_message_processor,*/
                pop_message_processor,
                self.inner.clone(),
            )),
            notification_processor: Default::default(),
            polling_info_processor: Default::default(),
            reply_message_processor: ArcMut::new(reply_message_processor),
            admin_broker_processor: ArcMut::new(admin_broker_processor),
            client_manage_processor: ArcMut::new(ClientManageProcessor::new(
                /*self.broker_config.clone(),
                self.producer_manager.clone(),
                self.consumer_manager.clone(),
                self.topic_config_manager.clone(),
                self.subscription_group_manager.clone(),*/
                self.inner.clone(),
            )),
            consumer_manage_processor: ArcMut::new(consumer_manage_processor),
            query_assignment_processor: ArcMut::new(QueryAssignmentProcessor::new(
                /* self.message_store_config.clone(),
                self.broker_config.clone(),
                self.topic_route_info_manager.clone(),
                self.consumer_manager.clone(),*/
                self.inner.clone(),
            )),
            query_message_processor: ArcMut::new(query_message_processor),
            end_transaction_processor: ArcMut::new(EndTransactionProcessor::new(
                /*self.message_store_config.clone(),
                self.broker_config.clone(),
                self.transactional_message_service.as_ref().unwrap().clone(),
                self.message_store.as_ref().unwrap().clone(),*/
                self.transactional_message_service.as_ref().unwrap().clone(),
                self.inner.clone(),
            )),
        }
    }

    async fn initialize_scheduled_tasks(&mut self) {
        let initial_delay = compute_next_morning_time_millis() - get_current_millis();
        let period = Duration::from_days(1).as_millis() as u64;
        let broker_stats_ = self.inner.clone();
        self.broker_runtime
            .as_ref()
            .unwrap()
            .get_handle()
            .spawn(async move {
                info!("BrokerStats Start scheduled task");
                tokio::time::sleep(Duration::from_millis(initial_delay)).await;
                loop {
                    let current_execution_time = tokio::time::Instant::now();
                    broker_stats_.broker_stats().as_ref().unwrap().record();
                    let next_execution_time =
                        current_execution_time + Duration::from_millis(period);
                    let delay =
                        next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(delay).await;
                }
            });

        //need to optimize
        let consumer_offset_manager_inner = self.inner.clone();
        let flush_consumer_offset_interval =
            self.inner.broker_config.flush_consumer_offset_interval;
        self.broker_runtime
            .as_ref()
            .unwrap()
            .get_handle()
            .spawn(async move {
                info!("Consumer offset manager Start scheduled task");
                tokio::time::sleep(Duration::from_millis(1000 * 10)).await;
                loop {
                    let current_execution_time = tokio::time::Instant::now();
                    consumer_offset_manager_inner
                        .consumer_offset_manager
                        .persist();
                    let next_execution_time = current_execution_time
                        + Duration::from_millis(flush_consumer_offset_interval);
                    let delay =
                        next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(delay).await;
                }
            });

        //need to optimize
        let mut _inner = self.inner.clone();
        //let mut  consumer_order_info_manager = self.inner.consumer_order_info_manager.clone();
        self.broker_runtime
            .as_ref()
            .unwrap()
            .get_handle()
            .spawn(async move {
                info!("consumer filter manager Start scheduled task");
                info!("consumer order info manager Start scheduled task");
                tokio::time::sleep(Duration::from_millis(1000 * 10)).await;
                loop {
                    let current_execution_time = tokio::time::Instant::now();
                    _inner.consumer_filter_manager.as_mut().unwrap().persist();
                    _inner
                        .consumer_order_info_manager
                        .as_mut()
                        .unwrap()
                        .persist();
                    let next_execution_time =
                        current_execution_time + Duration::from_millis(1000 * 10);
                    let delay =
                        next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(delay).await;
                }
            });

        let mut runtime = self.inner.clone();
        self.broker_runtime
            .as_ref()
            .unwrap()
            .get_handle()
            .spawn(async move {
                info!("Protect broker Start scheduled task");
                tokio::time::sleep(Duration::from_mins(3)).await;
                loop {
                    let current_execution_time = tokio::time::Instant::now();
                    runtime.protect_broker();
                    let next_execution_time = current_execution_time + Duration::from_mins(3);
                    let delay =
                        next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(delay).await;
                }
            });

        let message_store_inner = self.inner.clone();
        self.broker_runtime
            .as_ref()
            .unwrap()
            .get_handle()
            .spawn(async move {
                info!("Message store dispatch_behind_bytes Start scheduled task");
                tokio::time::sleep(Duration::from_secs(10)).await;
                loop {
                    let current_execution_time = tokio::time::Instant::now();
                    message_store_inner
                        .message_store
                        .as_ref()
                        .unwrap()
                        .dispatch_behind_bytes();
                    let next_execution_time = current_execution_time + Duration::from_secs(60);
                    let delay =
                        next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(delay).await;
                }
            });

        if self.inner.broker_config.enable_controller_mode {
            self.inner.update_master_haserver_addr_periodically = true;
        }

        if let Some(ref namesrv_address) = self.inner.broker_config.namesrv_addr.clone() {
            self.update_namesrv_addr().await;
            info!(
                "Set user specified name remoting_server address: {}",
                namesrv_address
            );
            let mut broker_runtime = self.inner.clone();
            self.broker_runtime
                .as_ref()
                .unwrap()
                .get_handle()
                .spawn(async move {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    loop {
                        let current_execution_time = tokio::time::Instant::now();
                        broker_runtime.update_namesrv_addr_inner().await;
                        let next_execution_time = current_execution_time + Duration::from_secs(60);
                        let delay = next_execution_time
                            .saturating_duration_since(tokio::time::Instant::now());
                        tokio::time::sleep(delay).await;
                    }
                });
        }
    }

    fn initial_transaction(&mut self) {
        cfg_if::cfg_if! {
            if #[cfg(feature = "local_file_store")] {
                let bridge = TransactionalMessageBridge::new(
                    /*self.message_store.as_ref().unwrap().clone(),
                    self.broker_stats_manager.clone(),
                    self.consumer_offset_manager.clone(),
                    self.broker_config.clone(),
                    self.topic_config_manager.clone(),
                    self.store_host*/
                    self.inner.clone()
                );
                let service = DefaultTransactionalMessageService::new(bridge);
                self.transactional_message_service = Some(ArcMut::new(service));
            }
        }
        self.inner.transactional_message_check_listener =
            Some(DefaultTransactionalMessageCheckListener::new(
                /*self.broker_config.clone(),
                self.producer_manager.clone(),*/
                Broker2Client,
                /* self.topic_config_manager.clone(),
                self.message_store.as_ref().cloned().unwrap(),*/
                self.inner.clone(),
            ));
        self.inner.transactional_message_check_service = Some(TransactionalMessageCheckService);
        self.inner.transaction_metrics_flush_service = Some(TransactionMetricsFlushService);
    }

    fn initial_acl(&mut self) {}

    fn initial_rpc_hooks(&mut self) {}

    fn initial_request_pipeline(&mut self) {}

    fn start_basic_service(&mut self) {
        let request_processor = self.init_processor();
        let fast_request_processor = request_processor.clone();
        self.inner
            .message_store
            .as_mut()
            .unwrap()
            .start()
            .expect("Message store start error");

        let server = RocketMQServer::new(Arc::new(self.inner.server_config.clone()));
        //start nomarl broker remoting_server
        tokio::spawn(async move { server.run(request_processor).await });
        //start fast broker remoting_server
        let mut fast_server_config = self.inner.server_config.clone();
        fast_server_config.listen_port = self.inner.server_config.listen_port - 2;
        let fast_server = RocketMQServer::new(Arc::new(fast_server_config));
        tokio::spawn(async move { fast_server.run(fast_request_processor).await });

        let inner = self.inner.clone();
        if let Some(pull_request_hold_service) = self.inner.pull_request_hold_service.as_mut() {
            pull_request_hold_service.start(inner);
        }

        self.inner.topic_route_info_manager().start();

        //self.inner.escape_bridge().start(self.message_store.clone());
        self.inner.escape_bridge_mut().start();
    }

    async fn update_namesrv_addr(&mut self) {
        self.inner.update_namesrv_addr_inner().await;
    }

    pub async fn start(&mut self) {
        self.inner.should_start_time.store(
            (get_current_millis() as i64
                + self.inner.message_store_config.disappear_time_after_start) as u64,
            Ordering::Release,
        );
        if self.inner.message_store_config.total_replicas > 1
            && self.inner.broker_config.enable_slave_acting_master
        {
            self.inner.is_isolated.store(true, Ordering::Release);
        }

        self.inner.broker_outer_api.start().await;
        self.start_basic_service();

        if !self.inner.is_isolated.load(Ordering::Acquire)
            && !self.inner.message_store_config.enable_dledger_commit_log
            && !self.inner.broker_config.duplication_enable
        {
            self.register_broker_all(true, false, true).await;
        }

        let cloned_broker_runtime = self.inner.clone();
        let should_start_time = self.inner.should_start_time.clone();
        let is_isolated = self.inner.is_isolated.clone();
        let broker_config = self.inner.broker_config.clone();
        self.broker_runtime
            .as_ref()
            .unwrap()
            .get_handle()
            .spawn(async move {
                let period = Duration::from_millis(
                    10000.max(60000.min(broker_config.register_name_server_period)),
                );
                let initial_delay = Duration::from_secs(10);
                tokio::time::sleep(initial_delay).await;
                loop {
                    let start_time = should_start_time.load(Ordering::Relaxed);
                    if get_current_millis() < start_time {
                        info!("Register to namesrv after {}", start_time);
                        continue;
                    }
                    if is_isolated.load(Ordering::Relaxed) {
                        info!("Skip register for broker is isolated");
                        continue;
                    }
                    // record current execution time
                    let current_execution_time = tokio::time::Instant::now();
                    // execute task
                    let this = cloned_broker_runtime.clone();
                    cloned_broker_runtime
                        .register_broker_all_inner(this, true, false, broker_config.force_register)
                        .await;
                    // Calculate the time of the next execution
                    let next_execution_time = current_execution_time + period;

                    // Wait until the next execution
                    let delay =
                        next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(delay).await;
                }
            });

        if self.inner.broker_config.enable_slave_acting_master {
            self.schedule_send_heartbeat();
        }

        if self.inner.broker_config.enable_controller_mode {
            self.schedule_send_heartbeat();
        }

        if self.inner.broker_config.skip_pre_online {
            self.start_service_without_condition();
        }

        let broker_out_api_inner = self.inner.clone();
        self.broker_runtime
            .as_ref()
            .unwrap()
            .get_handle()
            .spawn(async move {
                let period = Duration::from_secs(5);
                let initial_delay = Duration::from_secs(10);
                tokio::time::sleep(initial_delay).await;
                loop {
                    // record current execution time
                    let current_execution_time = tokio::time::Instant::now();
                    // execute task
                    broker_out_api_inner.broker_outer_api.refresh_metadata();
                    // Calculate the time of the next execution
                    let next_execution_time = current_execution_time + period;

                    // Wait until the next execution
                    let delay =
                        next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(delay).await;
                }
            });
        info!(
            "Rocketmq Broker({} ----Rust) start success",
            self.inner.broker_config.broker_identity.broker_name
        );
        tokio::select! {
            _ = self.shutdown_rx.as_mut().unwrap().recv() => {
                info!("Broker Shutdown received, initiating graceful shutdown...");
                self.shutdown();
                info!("Broker Shutdown complete");
            }
        }
    }

    pub(crate) fn schedule_send_heartbeat(&mut self) {}

    pub(crate) fn start_service_without_condition(&mut self) {}

    /// Register broker to name remoting_server
    pub(crate) async fn register_broker_all(
        &mut self,
        check_order_config: bool,
        oneway: bool,
        force_register: bool,
    ) {
        self.inner
            .register_broker_all_inner(
                self.inner.clone(),
                check_order_config,
                oneway,
                force_register,
            )
            .await;
    }

    async fn do_register_broker_all(
        &mut self,
        _check_order_config: bool,
        oneway: bool,
        topic_config_wrapper: TopicConfigAndMappingSerializeWrapper,
    ) {
        let cluster_name = self
            .inner
            .broker_config
            .broker_identity
            .broker_cluster_name
            .clone();
        let broker_name = self.inner.broker_config.broker_identity.broker_name.clone();
        let broker_addr = CheetahString::from_string(format!(
            "{}:{}",
            self.inner.broker_config.broker_ip1, self.inner.server_config.listen_port
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

/*#[derive(Clone)]
pub(crate) struct BrokerRuntimeInner {
    pub(crate) broker_out_api: Arc<BrokerOuterAPI>,
    pub(crate) broker_config: Arc<BrokerConfig>,
    pub(crate) message_store_config: Arc<MessageStoreConfig>,
    pub(crate) server_config: Arc<ServerConfig>,
    pub(crate) topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
}*/

impl<MS: MessageStore> BrokerRuntimeInner<MS> {
    pub async fn register_single_topic_all(&self, topic_config: TopicConfig) {
        let mut topic_config = topic_config;
        if !PermName::is_writeable(self.broker_config.broker_permission)
            || !PermName::is_readable(self.broker_config.broker_permission)
        {
            topic_config.perm &= self.broker_config.broker_permission;
        }
        self.broker_outer_api
            .register_single_topic_all(
                self.broker_config
                    .broker_identity
                    .broker_cluster_name
                    .clone(),
                topic_config,
                3000,
            )
            .await;
    }

    pub async fn register_increment_broker_data(
        this: ArcMut<BrokerRuntimeInner<MS>>,
        topic_config_list: Vec<TopicConfig>,
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
            let register_topic_config =
                if !PermName::is_writeable(this.broker_config().broker_permission)
                    || !PermName::is_readable(this.broker_config().broker_permission)
                {
                    TopicConfig {
                        perm: topic_config.perm & this.broker_config().broker_permission,
                        ..topic_config.clone()
                    }
                } else {
                    topic_config.clone()
                };
            topic_config_table.insert(
                register_topic_config.topic_name.as_ref().unwrap().clone(),
                register_topic_config,
            );
        }
        serialize_wrapper
            .topic_config_serialize_wrapper
            .topic_config_table = topic_config_table;
        let mut topic_queue_mapping_info_map = HashMap::new();
        for topic_config in topic_config_list {
            if let Some(ref value) = this
                .topic_queue_mapping_manager
                .get_topic_queue_mapping(topic_config.topic_name.as_ref().unwrap().as_str())
            {
                topic_queue_mapping_info_map.insert(
                    topic_config.topic_name.as_ref().unwrap().clone(),
                    TopicQueueMappingDetail::clone_as_mapping_info(value),
                );
            }
        }
        serialize_wrapper.topic_queue_mapping_info_map = topic_queue_mapping_info_map;
        Self::do_register_broker_all(this, true, false, serialize_wrapper).await;
    }

    async fn do_register_broker_all(
        this: ArcMut<BrokerRuntimeInner<MS>>,
        _check_order_config: bool,
        oneway: bool,
        topic_config_wrapper: TopicConfigAndMappingSerializeWrapper,
    ) {
        let cluster_name = this
            .broker_config
            .broker_identity
            .broker_cluster_name
            .clone();
        let broker_name = this.broker_config.broker_identity.broker_name.clone();
        let broker_addr = CheetahString::from_string(format!(
            "{}:{}",
            this.broker_config.broker_ip1, this.server_config.listen_port
        ));
        let broker_id = this.broker_config.broker_identity.broker_id;
        //let weak = Arc::downgrade(&self.broker_out_api);
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
                this.clone(),
            )
            .await;
    }
}

/*struct ProducerStateGetter {
    topic_config_manager: TopicConfigManager,
    producer_manager: Arc<ProducerManager>,
}*/
struct ProducerStateGetter<MS> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}
impl<MS: MessageStore> StateGetter for ProducerStateGetter<MS> {
    fn online(
        &self,
        instance_id: &CheetahString,
        group: &CheetahString,
        topic: &CheetahString,
    ) -> bool {
        if self
            .broker_runtime_inner
            .topic_config_manager()
            .topic_config_table()
            .lock()
            .contains_key(NamespaceUtil::wrap_namespace(instance_id, topic).as_str())
        {
            self.broker_runtime_inner
                .producer_manager
                .group_online(NamespaceUtil::wrap_namespace(instance_id, group))
        } else {
            self.broker_runtime_inner
                .producer_manager
                .group_online(group.to_string())
        }
    }
}

/*struct ConsumerStateGetter {
    topic_config_manager: TopicConfigManager,
    consumer_manager: Arc<ConsumerManager>,
}*/
struct ConsumerStateGetter<MS> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> StateGetter for ConsumerStateGetter<MS> {
    fn online(
        &self,
        instance_id: &CheetahString,
        group: &CheetahString,
        topic: &CheetahString,
    ) -> bool {
        if self
            .broker_runtime_inner
            .topic_config_manager()
            .topic_config_table()
            .lock()
            .contains_key(topic)
        {
            let topic_full_name =
                CheetahString::from_string(NamespaceUtil::wrap_namespace(instance_id, topic));
            self.broker_runtime_inner
                .consumer_manager
                .find_subscription_data(
                    CheetahString::from_string(NamespaceUtil::wrap_namespace(instance_id, group))
                        .as_ref(),
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

pub(crate) struct BrokerRuntimeInner<MS> {
    store_host: SocketAddr,
    broker_config: BrokerConfig,
    message_store_config: MessageStoreConfig,
    server_config: ServerConfig,
    topic_config_manager: Option<TopicConfigManager<MS>>,
    topic_queue_mapping_manager: TopicQueueMappingManager,
    consumer_offset_manager: ConsumerOffsetManager,
    subscription_group_manager: Option<SubscriptionGroupManager<MS>>,
    consumer_filter_manager: Option<ConsumerFilterManager>,
    consumer_order_info_manager: Option<ConsumerOrderInfoManager<MS>>,
    message_store: Option<ArcMut<MS>>,
    broker_stats: Option<BrokerStats<MS>>,
    schedule_message_service: ScheduleMessageService,
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
    transactional_message_check_service: Option<TransactionalMessageCheckService>,
    transaction_metrics_flush_service: Option<TransactionMetricsFlushService>,
    topic_route_info_manager: Option<TopicRouteInfoManager<MS>>,
    escape_bridge: Option<EscapeBridge<MS>>,
    pop_inflight_message_counter: PopInflightMessageCounter,
}

impl<MS: MessageStore> BrokerRuntimeInner<MS> {
    #[inline]
    pub fn store_host_mut(&mut self) -> &mut SocketAddr {
        &mut self.store_host
    }

    #[inline]
    pub fn broker_config_mut(&mut self) -> &mut BrokerConfig {
        &mut self.broker_config
    }

    #[inline]
    pub fn message_store_config_mut(&mut self) -> &mut MessageStoreConfig {
        &mut self.message_store_config
    }

    #[inline]
    pub fn server_config_mut(&mut self) -> &mut ServerConfig {
        &mut self.server_config
    }

    #[inline]
    pub fn topic_config_manager_mut(&mut self) -> &mut TopicConfigManager<MS> {
        self.topic_config_manager.as_mut().unwrap()
    }

    #[inline]
    pub fn topic_queue_mapping_manager_mut(&mut self) -> &mut TopicQueueMappingManager {
        &mut self.topic_queue_mapping_manager
    }

    #[inline]
    pub fn consumer_offset_manager_mut(&mut self) -> &mut ConsumerOffsetManager {
        &mut self.consumer_offset_manager
    }

    #[inline]
    pub fn subscription_group_manager_mut(&mut self) -> &mut SubscriptionGroupManager<MS> {
        self.subscription_group_manager.as_mut().unwrap()
    }

    #[inline]
    pub fn consumer_filter_manager_mut(&mut self) -> &mut ConsumerFilterManager {
        self.consumer_filter_manager.as_mut().unwrap()
    }

    #[inline]
    pub fn consumer_order_info_manager_mut(&mut self) -> &mut ConsumerOrderInfoManager<MS> {
        self.consumer_order_info_manager.as_mut().unwrap()
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
    pub fn schedule_message_service_mut(&mut self) -> &mut ScheduleMessageService {
        &mut self.schedule_message_service
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
        /* &mut self.broker_stats_manager */
        unimplemented!("broker_stats_manager_mut")
    }

    #[inline]
    pub fn topic_queue_mapping_clean_service_mut(
        &mut self,
    ) -> &mut Option<TopicQueueMappingCleanService> {
        &mut self.topic_queue_mapping_clean_service
    }

    #[inline]
    pub fn update_master_haserver_addr_periodically_mut(&mut self) -> &mut bool {
        &mut self.update_master_haserver_addr_periodically
    }

    /*    #[inline]
    pub fn should_start_time_mut(&mut self) -> &mut AtomicU64 {
        &mut self.should_start_time
    }*/

    /*    #[inline]
    pub fn is_isolated_mut(&mut self) -> &mut AtomicBool {
        &mut self.is_isolated
    }*/

    #[inline]
    pub fn pull_request_hold_service_mut(&mut self) -> &mut Option<PullRequestHoldService<MS>> {
        &mut self.pull_request_hold_service
    }

    #[inline]
    pub fn rebalance_lock_manager_mut(&mut self) -> &mut RebalanceLockManager {
        &mut self.rebalance_lock_manager
    }

    #[inline]
    pub fn broker_member_group_mut(&mut self) -> &mut BrokerMemberGroup {
        &mut self.broker_member_group
    }

    /*    #[inline]
    pub fn transactional_message_service_mut(
        &mut self,
    ) -> &mut Option<DefaultTransactionalMessageService<MS>> {
        &mut self.transactional_message_service
    }*/

    #[inline]
    pub fn transactional_message_check_listener_mut(
        &mut self,
    ) -> &mut Option<DefaultTransactionalMessageCheckListener<MS>> {
        &mut self.transactional_message_check_listener
    }

    #[inline]
    pub fn transactional_message_check_service_mut(
        &mut self,
    ) -> &mut Option<TransactionalMessageCheckService> {
        &mut self.transactional_message_check_service
    }

    #[inline]
    pub fn transaction_metrics_flush_service_mut(
        &mut self,
    ) -> &mut Option<TransactionMetricsFlushService> {
        &mut self.transaction_metrics_flush_service
    }

    #[inline]
    pub fn topic_route_info_manager_mut(&mut self) -> &mut TopicRouteInfoManager<MS> {
        self.topic_route_info_manager.as_mut().unwrap()
    }

    #[inline]
    pub fn escape_bridge_mut(&mut self) -> &mut EscapeBridge<MS> {
        self.escape_bridge.as_mut().unwrap()
    }

    #[inline]
    pub fn pop_inflight_message_counter_mut(&mut self) -> &mut PopInflightMessageCounter {
        &mut self.pop_inflight_message_counter
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
    pub fn message_store_config(&self) -> &MessageStoreConfig {
        &self.message_store_config
    }

    #[inline]
    pub fn server_config(&self) -> &ServerConfig {
        &self.server_config
    }

    #[inline]
    pub fn topic_config_manager(&self) -> &TopicConfigManager<MS> {
        self.topic_config_manager.as_ref().unwrap()
    }

    #[inline]
    pub fn topic_queue_mapping_manager(&self) -> &TopicQueueMappingManager {
        &self.topic_queue_mapping_manager
    }

    #[inline]
    pub fn consumer_offset_manager(&self) -> &ConsumerOffsetManager {
        &self.consumer_offset_manager
    }

    #[inline]
    pub fn subscription_group_manager(&self) -> &SubscriptionGroupManager<MS> {
        self.subscription_group_manager.as_ref().unwrap()
    }

    #[inline]
    pub fn consumer_filter_manager(&self) -> &ConsumerFilterManager {
        self.consumer_filter_manager.as_ref().unwrap()
    }

    #[inline]
    pub fn consumer_order_info_manager(&self) -> &ConsumerOrderInfoManager<MS> {
        self.consumer_order_info_manager.as_ref().unwrap()
    }

    #[inline]
    pub fn message_store(&self) -> &Option<ArcMut<MS>> {
        &self.message_store
    }

    #[inline]
    pub fn broker_stats(&self) -> &Option<BrokerStats<MS>> {
        &self.broker_stats
    }

    #[inline]
    pub fn schedule_message_service(&self) -> &ScheduleMessageService {
        &self.schedule_message_service
    }

    #[inline]
    pub fn timer_message_store(&self) -> &Option<TimerMessageStore> {
        &self.timer_message_store
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
    pub fn topic_queue_mapping_clean_service(&self) -> &Option<TopicQueueMappingCleanService> {
        &self.topic_queue_mapping_clean_service
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
    pub fn pull_request_hold_service(&self) -> &Option<PullRequestHoldService<MS>> {
        &self.pull_request_hold_service
    }

    #[inline]
    pub fn rebalance_lock_manager(&self) -> &RebalanceLockManager {
        &self.rebalance_lock_manager
    }

    #[inline]
    pub fn broker_member_group(&self) -> &BrokerMemberGroup {
        &self.broker_member_group
    }

    /*    #[inline]
    pub fn transactional_message_service(&self) -> &Option<DefaultTransactionalMessageService<MS>> {
        &self.transactional_message_service
    }*/

    #[inline]
    pub fn transactional_message_check_listener(
        &self,
    ) -> &Option<DefaultTransactionalMessageCheckListener<MS>> {
        &self.transactional_message_check_listener
    }

    #[inline]
    pub fn transactional_message_check_service(&self) -> &Option<TransactionalMessageCheckService> {
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
    pub fn set_store_host(&mut self, store_host: SocketAddr) {
        self.store_host = store_host;
    }

    #[inline]
    pub fn set_broker_config(&mut self, broker_config: BrokerConfig) {
        self.broker_config = broker_config;
    }

    #[inline]
    pub fn set_message_store_config(&mut self, message_store_config: MessageStoreConfig) {
        self.message_store_config = message_store_config;
    }

    #[inline]
    pub fn set_server_config(&mut self, server_config: ServerConfig) {
        self.server_config = server_config;
    }

    #[inline]
    pub fn set_topic_config_manager(&mut self, topic_config_manager: TopicConfigManager<MS>) {
        self.topic_config_manager = Some(topic_config_manager);
    }

    #[inline]
    pub fn set_topic_queue_mapping_manager(
        &mut self,
        topic_queue_mapping_manager: TopicQueueMappingManager,
    ) {
        self.topic_queue_mapping_manager = topic_queue_mapping_manager;
    }

    #[inline]
    pub fn set_consumer_offset_manager(&mut self, consumer_offset_manager: ConsumerOffsetManager) {
        self.consumer_offset_manager = consumer_offset_manager;
    }

    #[inline]
    pub fn set_subscription_group_manager(
        &mut self,
        subscription_group_manager: SubscriptionGroupManager<MS>,
    ) {
        self.subscription_group_manager = Some(subscription_group_manager);
    }

    #[inline]
    pub fn set_consumer_filter_manager(&mut self, consumer_filter_manager: ConsumerFilterManager) {
        self.consumer_filter_manager = Some(consumer_filter_manager);
    }

    #[inline]
    pub fn set_consumer_order_info_manager(
        &mut self,
        consumer_order_info_manager: ConsumerOrderInfoManager<MS>,
    ) {
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
    pub fn set_schedule_message_service(
        &mut self,
        schedule_message_service: ScheduleMessageService,
    ) {
        self.schedule_message_service = schedule_message_service;
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
    pub fn set_broadcast_offset_manager(
        &mut self,
        broadcast_offset_manager: BroadcastOffsetManager,
    ) {
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
    pub fn set_update_master_haserver_addr_periodically(
        &mut self,
        update_master_haserver_addr_periodically: bool,
    ) {
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
    pub fn set_pull_request_hold_service(
        &mut self,
        pull_request_hold_service: PullRequestHoldService<MS>,
    ) {
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

    /*    #[inline]
    pub fn set_transactional_message_service(
        &mut self,
        transactional_message_service: DefaultTransactionalMessageService<MS>,
    ) {
        self.transactional_message_service = Some(transactional_message_service);
    }*/

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
        transactional_message_check_service: TransactionalMessageCheckService,
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
    pub fn set_topic_route_info_manager(
        &mut self,
        topic_route_info_manager: TopicRouteInfoManager<MS>,
    ) {
        self.topic_route_info_manager = Some(topic_route_info_manager);
    }

    #[inline]
    pub fn set_escape_bridge(&mut self, escape_bridge: EscapeBridge<MS>) {
        self.escape_bridge = Some(escape_bridge);
    }

    #[inline]
    pub fn set_pop_inflight_message_counter(
        &mut self,
        pop_inflight_message_counter: PopInflightMessageCounter,
    ) {
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
        for topic_config in table.lock().values() {
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
                topic_config.clone()
            };
            topic_config_table.insert(
                new_topic_config.topic_name.as_ref().unwrap().clone(),
                new_topic_config,
            );
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
            .lock()
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    TopicQueueMappingDetail::clone_as_mapping_info(value),
                )
            })
            .collect();

        let topic_config_wrapper = this
            .topic_config_manager()
            .build_serialize_wrapper_with_topic_queue_map(
                topic_config_table,
                topic_queue_mapping_info_map,
            );

        if self.broker_config.enable_split_registration
            || force_register
            || need_register(
                self.broker_config
                    .broker_identity
                    .broker_cluster_name
                    .clone()
                    .as_str(),
                self.broker_config.broker_ip1.clone().as_str(),
                self.broker_config
                    .broker_identity
                    .broker_name
                    .clone()
                    .as_str(),
                self.broker_config.broker_identity.broker_id,
                self.broker_config.register_broker_timeout_mills,
                self.broker_config.is_in_broker_container,
            )
        {
            BrokerRuntimeInner::<MS>::do_register_broker_all(
                this,
                check_order_config,
                oneway,
                topic_config_wrapper,
            )
            .await;
        }
    }

    async fn do_register_broker_all_inner(
        this: ArcMut<BrokerRuntimeInner<MS>>,
        _check_order_config: bool,
        oneway: bool,
        topic_config_wrapper: TopicConfigAndMappingSerializeWrapper,
    ) {
        let cluster_name = this
            .broker_config
            .broker_identity
            .broker_cluster_name
            .clone();
        let broker_name = this.broker_config.broker_identity.broker_name.clone();
        let broker_addr = CheetahString::from_string(format!(
            "{}:{}",
            this.broker_config.broker_ip1, this.server_config.listen_port
        ));
        let broker_id = this.broker_config.broker_identity.broker_id;
        //  let weak = Arc::downgrade(&self.inner.broker_outer_api);
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
