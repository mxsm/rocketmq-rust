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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_common::common::statistics::state_getter::StateGetter;
use rocketmq_common::ArcRefCellWrapper;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_common::UtilAll::compute_next_morning_time_millis;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigSerializeWrapper;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_remoting::remoting_server::server::RocketMQServer;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_runtime::RocketMQRuntime;
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
use crate::client::rebalance::rebalance_lock_manager::RebalanceLockManager;
use crate::filter::manager::consumer_filter_manager::ConsumerFilterManager;
use crate::hook::batch_check_before_put_message::BatchCheckBeforePutMessageHook;
use crate::hook::check_before_put_message::CheckBeforePutMessageHook;
use crate::long_polling::long_polling_service::pull_request_hold_service::PullRequestHoldService;
use crate::long_polling::notify_message_arriving_listener::NotifyMessageArrivingListener;
use crate::offset::manager::broadcast_offset_manager::BroadcastOffsetManager;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::offset::manager::consumer_order_info_manager::ConsumerOrderInfoManager;
use crate::out_api::broker_outer_api::BrokerOuterAPI;
use crate::processor::admin_broker_processor::AdminBrokerProcessor;
use crate::processor::client_manage_processor::ClientManageProcessor;
use crate::processor::consumer_manage_processor::ConsumerManageProcessor;
use crate::processor::default_pull_message_result_handler::DefaultPullMessageResultHandler;
use crate::processor::pull_message_processor::PullMessageProcessor;
use crate::processor::pull_message_result_handler::PullMessageResultHandler;
use crate::processor::query_message_processor::QueryMessageProcessor;
use crate::processor::reply_message_processor::ReplyMessageProcessor;
use crate::processor::send_message_processor::SendMessageProcessor;
use crate::processor::BrokerRequestProcessor;
use crate::schedule::schedule_message_service::ScheduleMessageService;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupManager;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;
use crate::topic::topic_queue_mapping_clean_service::TopicQueueMappingCleanService;

pub(crate) struct BrokerRuntime {
    broker_config: Arc<BrokerConfig>,
    message_store_config: Arc<MessageStoreConfig>,
    server_config: Arc<ServerConfig>,
    topic_config_manager: TopicConfigManager,
    topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    consumer_offset_manager: ConsumerOffsetManager,
    #[cfg(feature = "local_file_store")]
    subscription_group_manager: Arc<SubscriptionGroupManager<DefaultMessageStore>>,
    consumer_filter_manager: Arc<ConsumerFilterManager>,
    consumer_order_info_manager: Arc<ConsumerOrderInfoManager>,
    #[cfg(feature = "local_file_store")]
    message_store: Option<DefaultMessageStore>,
    #[cfg(feature = "local_file_store")]
    broker_stats: Option<Arc<BrokerStats<DefaultMessageStore>>>,
    //message_store: Option<Arc<Mutex<LocalFileMessageStore>>>,
    schedule_message_service: ScheduleMessageService,
    timer_message_store: Option<TimerMessageStore>,

    broker_out_api: Arc<BrokerOuterAPI>,

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
    pull_request_hold_service: Option<PullRequestHoldService<DefaultMessageStore>>,
    rebalance_lock_manager: Arc<RebalanceLockManager>,
}

impl Clone for BrokerRuntime {
    fn clone(&self) -> Self {
        Self {
            broker_config: self.broker_config.clone(),
            message_store_config: self.message_store_config.clone(),
            server_config: self.server_config.clone(),
            topic_config_manager: self.topic_config_manager.clone(),
            topic_queue_mapping_manager: self.topic_queue_mapping_manager.clone(),
            consumer_offset_manager: self.consumer_offset_manager.clone(),
            subscription_group_manager: self.subscription_group_manager.clone(),
            consumer_filter_manager: Arc::new(Default::default()),
            consumer_order_info_manager: Arc::new(Default::default()),
            message_store: self.message_store.clone(),
            broker_stats: self.broker_stats.clone(),
            schedule_message_service: self.schedule_message_service.clone(),
            timer_message_store: self.timer_message_store.clone(),
            broker_out_api: self.broker_out_api.clone(),
            broker_runtime: None,
            producer_manager: self.producer_manager.clone(),
            consumer_manager: self.consumer_manager.clone(),
            broadcast_offset_manager: self.broadcast_offset_manager.clone(),
            drop: self.drop.clone(),
            shutdown: self.shutdown.clone(),
            shutdown_hook: self.shutdown_hook.clone(),
            broker_stats_manager: self.broker_stats_manager.clone(),
            topic_queue_mapping_clean_service: self.topic_queue_mapping_clean_service.clone(),
            update_master_haserver_addr_periodically: self.update_master_haserver_addr_periodically,
            should_start_time: self.should_start_time.clone(),
            is_isolated: self.is_isolated.clone(),
            pull_request_hold_service: self.pull_request_hold_service.clone(),
            rebalance_lock_manager: self.rebalance_lock_manager.clone(),
        }
    }
}

impl BrokerRuntime {
    pub(crate) fn new(
        broker_config: BrokerConfig,
        message_store_config: MessageStoreConfig,
        server_config: ServerConfig,
    ) -> Self {
        let broker_config = Arc::new(broker_config);
        let runtime = RocketMQRuntime::new_multi(10, "broker-thread");
        let broker_outer_api =
            Arc::new(BrokerOuterAPI::new(Arc::new(TokioClientConfig::default())));
        let server_config = Arc::new(server_config);
        let message_store_config = Arc::new(message_store_config);
        let topic_queue_mapping_manager =
            Arc::new(TopicQueueMappingManager::new(broker_config.clone()));
        let broker_runtime_inner = Arc::new(BrokerRuntimeInner {
            broker_out_api: broker_outer_api.clone(),
            broker_config: broker_config.clone(),
            message_store_config: message_store_config.clone(),
            server_config: server_config.clone(),
            topic_queue_mapping_manager: topic_queue_mapping_manager.clone(),
        });
        let topic_config_manager =
            TopicConfigManager::new(broker_config.clone(), broker_runtime_inner);
        let mut stats_manager = BrokerStatsManager::new(broker_config.clone());
        let producer_manager = Arc::new(ProducerManager::new());
        let consumer_manager = Arc::new(ConsumerManager::new_with_broker_stats(
            Box::new(DefaultConsumerIdsChangeListener {}),
            broker_config.clone(),
        ));
        stats_manager.set_producer_state_getter(Arc::new(ProducerStateGetter {
            topic_config_manager: topic_config_manager.clone(),
            producer_manager: producer_manager.clone(),
        }));
        stats_manager.set_consumer_state_getter(Arc::new(ConsumerStateGetter {
            topic_config_manager: topic_config_manager.clone(),
            consumer_manager: consumer_manager.clone(),
        }));
        let broker_stats_manager = Arc::new(stats_manager);
        consumer_manager.set_broker_stats_manager(Some(Arc::downgrade(&broker_stats_manager)));
        Self {
            broker_config: broker_config.clone(),
            message_store_config,
            server_config,
            topic_config_manager,
            topic_queue_mapping_manager,
            consumer_offset_manager: ConsumerOffsetManager::new(broker_config.clone(), None),
            subscription_group_manager: Arc::new(SubscriptionGroupManager::new(
                broker_config.clone(),
                None,
            )),
            consumer_filter_manager: Arc::new(Default::default()),
            consumer_order_info_manager: Arc::new(Default::default()),
            message_store: None,
            broker_stats: None,
            schedule_message_service: Default::default(),
            timer_message_store: None,
            broker_out_api: broker_outer_api,
            broker_runtime: Some(runtime),
            producer_manager,
            consumer_manager,
            broadcast_offset_manager: Arc::new(Default::default()),
            drop: Arc::new(AtomicBool::new(false)),
            shutdown: Arc::new(AtomicBool::new(false)),
            shutdown_hook: None,
            broker_stats_manager,
            topic_queue_mapping_clean_service: None,
            update_master_haserver_addr_periodically: false,
            should_start_time: Arc::new(AtomicU64::new(0)),
            is_isolated: Arc::new(AtomicBool::new(false)),
            pull_request_hold_service: None,
            rebalance_lock_manager: Arc::new(Default::default()),
        }
    }

    pub(crate) fn broker_config(&self) -> &BrokerConfig {
        &self.broker_config
    }

    pub(crate) fn message_store_config(&self) -> &MessageStoreConfig {
        &self.message_store_config
    }

    pub fn shutdown(&mut self) {
        self.broker_out_api.shutdown();
        if let Some(message_store) = &mut self.message_store {
            message_store.shutdown()
        }

        self.topic_config_manager.persist();
        info!("[Broker shutdown]TopicConfigManager persist success");
        let _ = self.topic_config_manager.stop();

        if let Some(pull_request_hold_service) = self.pull_request_hold_service.as_mut() {
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

impl Drop for BrokerRuntime {
    fn drop(&mut self) {
        let result =
            self.drop
                .clone()
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed);
        if result.is_ok() {
            self.shutdown();
        }
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

    ///Load the original configuration data from the corresponding configuration files located
    /// under the ${HOME}\config directory.
    fn initialize_metadata(&self) -> bool {
        info!("======Starting initialize metadata========");
        self.topic_config_manager.load()
            && self.topic_queue_mapping_manager.load()
            && self.consumer_offset_manager.load()
            && self.subscription_group_manager.load()
            && self.consumer_filter_manager.load()
            && self.consumer_order_info_manager.load()
    }

    async fn initialize_message_store(&mut self) -> bool {
        if self.message_store_config.store_type == StoreType::LocalFile {
            info!("Use local file as message store");
            let mut message_store = DefaultMessageStore::new(
                self.message_store_config.clone(),
                self.broker_config.clone(),
                self.topic_config_manager.topic_config_table(),
                Some(self.broker_stats_manager.clone()),
                false,
            );
            if self.message_store_config.is_timer_wheel_enable() {
                let time_message_store = TimerMessageStore::new(Some(message_store.clone()));
                message_store.set_timer_message_store(Arc::new(time_message_store));
            }
            self.consumer_offset_manager
                .set_message_store(Some(Arc::new(message_store.clone())));
            self.topic_config_manager
                .set_message_store(Some(message_store.clone()));
            self.broker_stats = Some(Arc::new(BrokerStats::new(Arc::new(message_store.clone()))));
            self.message_store = Some(message_store);
        } else if self.message_store_config.store_type == StoreType::RocksDB {
            info!("Use RocksDB as message store");
        } else {
            warn!("Unknown store type");
            return false;
        }
        true
    }

    async fn recover_initialize_service(&mut self) -> bool {
        let mut result: bool = true;

        if self.broker_config.enable_controller_mode {
            info!("Start controller mode(Support for future versions)");
            unimplemented!()
        }
        if self.message_store.is_some() {
            self.register_message_store_hook();
            self.message_store.as_mut().unwrap().load().await;
        }

        if self.broker_config.timer_wheel_config.timer_wheel_enable {
            result &= self.timer_message_store.as_mut().unwrap().load();
        }
        result &= self.schedule_message_service.load();

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
        if let Some(ref mut message_store) = self.message_store {
            message_store.set_put_message_hook(Box::new(CheckBeforePutMessageHook::new(
                message_store.clone(),
                self.message_store_config.clone(),
            )));
            message_store.set_put_message_hook(Box::new(BatchCheckBeforePutMessageHook::new(
                self.topic_config_manager.topic_config_table(),
            )));
        }
    }

    fn initialize_remoting_server(&mut self) {

        // fast broker remoting_server implementation in future versions
    }

    fn initialize_resources(&mut self) {
        self.topic_queue_mapping_clean_service = Some(Arc::new(TopicQueueMappingCleanService));
    }

    fn init_processor(&mut self) -> BrokerRequestProcessor<DefaultMessageStore> {
        let send_message_processor = SendMessageProcessor::<DefaultMessageStore>::new(
            self.topic_queue_mapping_manager.clone(),
            self.subscription_group_manager.clone(),
            self.topic_config_manager.clone(),
            self.broker_config.clone(),
            self.message_store.as_ref().unwrap(),
            self.rebalance_lock_manager.clone(),
            self.broker_stats_manager.clone(),
        );
        let reply_message_processor = ReplyMessageProcessor::new(
            self.topic_queue_mapping_manager.clone(),
            self.subscription_group_manager.clone(),
            self.topic_config_manager.clone(),
            self.broker_config.clone(),
            self.message_store.as_ref().unwrap(),
            self.rebalance_lock_manager.clone(),
            self.broker_stats_manager.clone(),
            Some(self.producer_manager.clone()),
        );
        let mut pull_message_result_handler =
            ArcRefCellWrapper::new(Box::new(DefaultPullMessageResultHandler::new(
                self.message_store_config.clone(),
                Arc::new(self.topic_config_manager.clone()),
                Arc::new(self.consumer_offset_manager.clone()),
                self.consumer_manager.clone(),
                self.broadcast_offset_manager.clone(),
                self.broker_stats_manager.clone(),
                self.broker_config.clone(),
                Arc::new(Default::default()),
            )) as Box<dyn PullMessageResultHandler>);
        let message_store = Arc::new(self.message_store.as_ref().unwrap().clone());
        let pull_message_processor = PullMessageProcessor::new(
            pull_message_result_handler.clone(),
            self.broker_config.clone(),
            self.subscription_group_manager.clone(),
            Arc::new(self.topic_config_manager.clone()),
            self.topic_queue_mapping_manager.clone(),
            self.consumer_manager.clone(),
            self.consumer_filter_manager.clone(),
            Arc::new(self.consumer_offset_manager.clone()),
            Arc::new(BroadcastOffsetManager::default()),
            message_store.clone(),
            self.broker_out_api.clone(),
        );

        let consumer_manage_processor = ConsumerManageProcessor::new(
            self.broker_config.clone(),
            self.consumer_manager.clone(),
            self.topic_queue_mapping_manager.clone(),
            self.subscription_group_manager.clone(),
            Arc::new(self.consumer_offset_manager.clone()),
            Arc::new(self.topic_config_manager.clone()),
            self.message_store.clone().unwrap(),
        );
        self.pull_request_hold_service = Some(PullRequestHoldService::new(
            message_store.clone(),
            Arc::new(pull_message_processor.clone()),
            self.broker_config.clone(),
        ));

        let pull_message_result_handler = pull_message_result_handler.as_mut().as_mut();
        pull_message_result_handler
            .as_any_mut()
            .downcast_mut::<DefaultPullMessageResultHandler>()
            .expect("downcast DefaultPullMessageResultHandler failed")
            .set_pull_request_hold_service(Some(Arc::new(
                self.pull_request_hold_service.clone().unwrap(),
            )));

        self.message_store
            .as_mut()
            .unwrap()
            .set_message_arriving_listener(Some(Arc::new(Box::new(
                NotifyMessageArrivingListener::new(self.pull_request_hold_service.clone().unwrap()),
            ))));
        let query_message_processor =
            QueryMessageProcessor::new(self.message_store_config.clone(), message_store.clone());

        let admin_broker_processor = AdminBrokerProcessor::new(
            self.broker_config.clone(),
            self.server_config.clone(),
            self.message_store_config.clone(),
            self.topic_config_manager.clone(),
            self.consumer_offset_manager.clone(),
            self.topic_queue_mapping_manager.clone(),
            self.message_store.as_ref().unwrap().clone(),
            self.schedule_message_service.clone(),
            self.broker_stats.clone(),
            self.consumer_manager.clone(),
            self.broker_out_api.clone(),
        );

        BrokerRequestProcessor {
            send_message_processor,
            pull_message_processor,
            peek_message_processor: Default::default(),
            pop_message_processor: Default::default(),
            ack_message_processor: Default::default(),
            change_invisible_time_processor: Default::default(),
            notification_processor: Default::default(),
            polling_info_processor: Default::default(),
            reply_message_processor,
            admin_broker_processor,
            client_manage_processor: ClientManageProcessor::new(
                self.broker_config.clone(),
                self.producer_manager.clone(),
                self.consumer_manager.clone(),
                self.topic_config_manager.clone(),
                self.subscription_group_manager.clone(),
            ),
            consumer_manage_processor,
            query_assignment_processor: Default::default(),
            query_message_processor,
            end_transaction_processor: Default::default(),
        }
    }

    async fn initialize_scheduled_tasks(&mut self) {
        let initial_delay = compute_next_morning_time_millis() - get_current_millis();
        let period = Duration::from_days(1).as_millis() as u64;
        let broker_stats = self.broker_stats.clone();
        self.broker_runtime
            .as_ref()
            .unwrap()
            .get_handle()
            .spawn(async move {
                info!("BrokerStats Start scheduled task");
                tokio::time::sleep(Duration::from_millis(initial_delay)).await;
                loop {
                    let current_execution_time = tokio::time::Instant::now();
                    broker_stats.as_ref().unwrap().record();
                    let next_execution_time =
                        current_execution_time + Duration::from_millis(period);
                    let delay =
                        next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(delay).await;
                }
            });

        let consumer_offset_manager = self.consumer_offset_manager.clone();
        let flush_consumer_offset_interval = self.broker_config.flush_consumer_offset_interval;
        self.broker_runtime
            .as_ref()
            .unwrap()
            .get_handle()
            .spawn(async move {
                info!("Consumer offset manager Start scheduled task");
                tokio::time::sleep(Duration::from_millis(1000 * 10)).await;
                loop {
                    let current_execution_time = tokio::time::Instant::now();
                    consumer_offset_manager.persist();
                    let next_execution_time = current_execution_time
                        + Duration::from_millis(flush_consumer_offset_interval);
                    let delay =
                        next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(delay).await;
                }
            });

        let consumer_filter_manager = self.consumer_filter_manager.clone();
        let consumer_order_info_manager = self.consumer_order_info_manager.clone();
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
                    consumer_filter_manager.persist();
                    consumer_order_info_manager.persist();
                    let next_execution_time =
                        current_execution_time + Duration::from_millis(1000 * 10);
                    let delay =
                        next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(delay).await;
                }
            });

        let mut runtime = self.clone();
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

        let message_store = self.message_store.clone();
        self.broker_runtime
            .as_ref()
            .unwrap()
            .get_handle()
            .spawn(async move {
                info!("Message store dispatch_behind_bytes Start scheduled task");
                tokio::time::sleep(Duration::from_secs(10)).await;
                loop {
                    let current_execution_time = tokio::time::Instant::now();
                    message_store.as_ref().unwrap().dispatch_behind_bytes();
                    let next_execution_time = current_execution_time + Duration::from_secs(60);
                    let delay =
                        next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(delay).await;
                }
            });

        if self.broker_config.enable_controller_mode {
            self.update_master_haserver_addr_periodically = true;
        }

        if let Some(ref namesrv_address) = self.broker_config.namesrv_addr.clone() {
            self.update_namesrv_addr().await;
            info!(
                "Set user specified name remoting_server address: {}",
                namesrv_address
            );
            let mut broker_runtime = self.clone();
            self.broker_runtime
                .as_ref()
                .unwrap()
                .get_handle()
                .spawn(async move {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    loop {
                        let current_execution_time = tokio::time::Instant::now();
                        broker_runtime.update_namesrv_addr().await;
                        let next_execution_time = current_execution_time + Duration::from_secs(60);
                        let delay = next_execution_time
                            .saturating_duration_since(tokio::time::Instant::now());
                        tokio::time::sleep(delay).await;
                    }
                });
        }
    }

    fn initial_transaction(&mut self) {}

    fn initial_acl(&mut self) {}

    fn initial_rpc_hooks(&mut self) {}

    fn initial_request_pipeline(&mut self) {}

    fn protect_broker(&mut self) {}

    fn start_basic_service(&mut self) {
        let request_processor = self.init_processor();
        let fast_request_processor = request_processor.clone();
        self.message_store
            .as_mut()
            .unwrap()
            .start()
            .expect("Message store start error");

        let server = RocketMQServer::new(self.server_config.clone());
        //start nomarl broker remoting_server
        tokio::spawn(async move { server.run(request_processor).await });
        //start fast broker remoting_server
        let mut fast_server_config = (*self.server_config).clone();
        fast_server_config.listen_port = self.server_config.listen_port - 2;
        let fast_server = RocketMQServer::new(Arc::new(fast_server_config));
        tokio::spawn(async move { fast_server.run(fast_request_processor).await });

        if let Some(pull_request_hold_service) = self.pull_request_hold_service.as_mut() {
            pull_request_hold_service.start();
        }
    }

    async fn update_namesrv_addr(&mut self) {
        if self.broker_config.fetch_name_srv_addr_by_dns_lookup {
            if let Some(namesrv_addr) = &self.broker_config.namesrv_addr {
                self.broker_out_api
                    .update_name_server_address_list_by_dns_lookup(namesrv_addr.clone())
                    .await;
            }
        } else if let Some(namesrv_addr) = &self.broker_config.namesrv_addr {
            self.broker_out_api
                .update_name_server_address_list(namesrv_addr.clone())
                .await;
        }
    }

    pub async fn start(&mut self) {
        self.should_start_time.store(
            (get_current_millis() as i64 + self.message_store_config.disappear_time_after_start)
                as u64,
            Ordering::Release,
        );
        if self.message_store_config.total_replicas > 1
            && self.broker_config.enable_slave_acting_master
        {
            self.is_isolated.store(true, Ordering::Release);
        }

        self.broker_out_api.start().await;
        self.start_basic_service();

        if !self.is_isolated.load(Ordering::Acquire)
            && !self.message_store_config.enable_dledger_commit_log
            && !self.broker_config.duplication_enable
        {
            self.register_broker_all(true, false, true).await;
        }

        let mut cloned_broker_runtime = self.clone();
        let should_start_time = self.should_start_time.clone();
        let is_isolated = self.is_isolated.clone();
        let broker_config = self.broker_config.clone();
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
                    cloned_broker_runtime
                        .register_broker_all(true, false, broker_config.force_register)
                        .await;
                    // Calculate the time of the next execution
                    let next_execution_time = current_execution_time + period;

                    // Wait until the next execution
                    let delay =
                        next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(delay).await;
                }
            });

        if self.broker_config.enable_slave_acting_master {
            self.schedule_send_heartbeat();
        }

        if self.broker_config.enable_controller_mode {
            self.schedule_send_heartbeat();
        }

        if self.broker_config.skip_pre_online {
            self.start_service_without_condition();
        }

        let broker_out_api = self.broker_out_api.clone();
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
                    broker_out_api.refresh_metadata();
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
            self.broker_config.broker_identity.broker_name
        );
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
        let mut topic_config_table = HashMap::new();
        let table = self.topic_config_manager.topic_config_table();
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
            let topic_config_wrapper = self
                .topic_config_manager
                .build_serialize_wrapper(topic_config_table.clone());
            self.do_register_broker_all(check_order_config, oneway, topic_config_wrapper)
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

        let topic_config_wrapper = self
            .topic_config_manager
            .build_serialize_wrapper_with_topic_queue_map(
                topic_config_table,
                topic_queue_mapping_info_map,
            );

        if self.broker_config.enable_split_registration
            || force_register
            || Self::need_register(
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
            self.do_register_broker_all(check_order_config, oneway, topic_config_wrapper)
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

    async fn do_register_broker_all(
        &mut self,
        _check_order_config: bool,
        oneway: bool,
        topic_config_wrapper: TopicConfigAndMappingSerializeWrapper,
    ) {
        let cluster_name = self
            .broker_config
            .broker_identity
            .broker_cluster_name
            .clone();
        let broker_name = self.broker_config.broker_identity.broker_name.clone();
        let broker_addr = format!(
            "{}:{}",
            self.broker_config.broker_ip1, self.server_config.listen_port
        );
        let broker_id = self.broker_config.broker_identity.broker_id;
        self.broker_out_api
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
            )
            .await;
    }
}

#[derive(Clone)]
pub(crate) struct BrokerRuntimeInner {
    pub(crate) broker_out_api: Arc<BrokerOuterAPI>,
    pub(crate) broker_config: Arc<BrokerConfig>,
    pub(crate) message_store_config: Arc<MessageStoreConfig>,
    pub(crate) server_config: Arc<ServerConfig>,
    pub(crate) topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
}

impl BrokerRuntimeInner {
    pub async fn register_single_topic_all(&self, topic_config: TopicConfig) {
        let mut topic_config = topic_config;
        if !PermName::is_writeable(self.broker_config.broker_permission)
            || !PermName::is_readable(self.broker_config.broker_permission)
        {
            topic_config.perm &= self.broker_config.broker_permission;
        }
        self.broker_out_api
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
        &self,
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
                if !PermName::is_writeable(self.broker_config.broker_permission)
                    || !PermName::is_readable(self.broker_config.broker_permission)
                {
                    TopicConfig {
                        perm: topic_config.perm & self.broker_config.broker_permission,
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
            if let Some(ref value) = self
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
        self.do_register_broker_all(true, false, serialize_wrapper)
            .await;
    }

    async fn do_register_broker_all(
        &self,
        _check_order_config: bool,
        oneway: bool,
        topic_config_wrapper: TopicConfigAndMappingSerializeWrapper,
    ) {
        let cluster_name = self
            .broker_config
            .broker_identity
            .broker_cluster_name
            .clone();
        let broker_name = self.broker_config.broker_identity.broker_name.clone();
        let broker_addr = format!(
            "{}:{}",
            self.broker_config.broker_ip1, self.server_config.listen_port
        );
        let broker_id = self.broker_config.broker_identity.broker_id;
        self.broker_out_api
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
            )
            .await;
    }
}

struct ProducerStateGetter {
    topic_config_manager: TopicConfigManager,
    producer_manager: Arc<ProducerManager>,
}
impl StateGetter for ProducerStateGetter {
    fn online(&self, instance_id: &str, group: &str, topic: &str) -> bool {
        if self
            .topic_config_manager
            .topic_config_table()
            .lock()
            .contains_key(NamespaceUtil::wrap_namespace(instance_id, topic).as_str())
        {
            self.producer_manager
                .group_online(NamespaceUtil::wrap_namespace(instance_id, group))
        } else {
            self.producer_manager.group_online(group.to_string())
        }
    }
}

struct ConsumerStateGetter {
    topic_config_manager: TopicConfigManager,
    consumer_manager: Arc<ConsumerManager>,
}
impl StateGetter for ConsumerStateGetter {
    fn online(&self, instance_id: &str, group: &str, topic: &str) -> bool {
        if self
            .topic_config_manager
            .topic_config_table()
            .lock()
            .contains_key(topic)
        {
            let topic_full_name = NamespaceUtil::wrap_namespace(instance_id, topic);
            self.consumer_manager
                .find_subscription_data(
                    NamespaceUtil::wrap_namespace(instance_id, group).as_str(),
                    topic_full_name.as_str(),
                )
                .is_some()
        } else {
            self.consumer_manager
                .find_subscription_data(group, topic)
                .is_some()
        }
    }
}
