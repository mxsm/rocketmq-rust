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

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use rocketmq_common::{
    common::{
        broker::broker_config::BrokerConfig, config::TopicConfig, config_manager::ConfigManager,
        constant::PermName, server::config::ServerConfig,
    },
    TimeUtils::get_current_millis,
    UtilAll::compute_next_morning_time_millis,
};
use rocketmq_remoting::{
    protocol::{
        body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper,
        static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail, DataVersion,
    },
    runtime::{config::client_config::TokioClientConfig, server::RocketMQServer},
};
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_store::{
    base::store_enum::StoreType,
    config::message_store_config::MessageStoreConfig,
    log_file::MessageStore,
    message_store::default_message_store::DefaultMessageStore,
    stats::{broker_stats::BrokerStats, broker_stats_manager::BrokerStatsManager},
    timer::timer_message_store::TimerMessageStore,
};
use tracing::{info, warn};

use crate::{
    broker::broker_hook::BrokerShutdownHook,
    client::manager::producer_manager::ProducerManager,
    filter::manager::consumer_filter_manager::ConsumerFilterManager,
    hook::{
        batch_check_before_put_message::BatchCheckBeforePutMessageHook,
        check_before_put_message::CheckBeforePutMessageHook,
    },
    offset::manager::{
        consumer_offset_manager::ConsumerOffsetManager,
        consumer_order_info_manager::ConsumerOrderInfoManager,
    },
    out_api::broker_outer_api::BrokerOuterAPI,
    processor::{
        client_manage_processor::ClientManageProcessor,
        send_message_processor::SendMessageProcessor, BrokerRequestProcessor,
    },
    schedule::schedule_message_service::ScheduleMessageService,
    subscription::manager::subscription_group_manager::SubscriptionGroupManager,
    topic::{
        manager::{
            topic_config_manager::TopicConfigManager,
            topic_queue_mapping_manager::TopicQueueMappingManager,
        },
        topic_queue_mapping_clean_service::TopicQueueMappingCleanService,
    },
};

pub(crate) struct BrokerRuntime {
    broker_config: Arc<BrokerConfig>,
    message_store_config: Arc<MessageStoreConfig>,
    server_config: Arc<ServerConfig>,
    topic_config_manager: TopicConfigManager,
    topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    consumer_offset_manager: Arc<ConsumerOffsetManager>,
    subscription_group_manager: Arc<SubscriptionGroupManager>,
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
    drop: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    shutdown_hook: Option<BrokerShutdownHook>,
    broker_stats_manager: Arc<BrokerStatsManager>,
    topic_queue_mapping_clean_service: Option<Arc<TopicQueueMappingCleanService>>,
    update_master_haserver_addr_periodically: bool,
    should_start_time: Arc<AtomicU64>,
    is_isolated: Arc<AtomicBool>,
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
            subscription_group_manager: Arc::new(Default::default()),
            consumer_filter_manager: Arc::new(Default::default()),
            consumer_order_info_manager: Arc::new(Default::default()),
            message_store: self.message_store.clone(),
            broker_stats: self.broker_stats.clone(),
            schedule_message_service: Default::default(),
            timer_message_store: self.timer_message_store.clone(),
            broker_out_api: self.broker_out_api.clone(),
            broker_runtime: None,
            producer_manager: self.producer_manager.clone(),
            drop: self.drop.clone(),
            shutdown: self.shutdown.clone(),
            shutdown_hook: self.shutdown_hook.clone(),
            broker_stats_manager: self.broker_stats_manager.clone(),
            topic_queue_mapping_clean_service: self.topic_queue_mapping_clean_service.clone(),
            update_master_haserver_addr_periodically: self.update_master_haserver_addr_periodically,
            should_start_time: self.should_start_time.clone(),
            is_isolated: self.is_isolated.clone(),
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
        let broker_stats_manager = Arc::new(BrokerStatsManager::new(broker_config.clone()));
        Self {
            broker_config: broker_config.clone(),
            message_store_config,
            server_config,
            topic_config_manager: TopicConfigManager::new(
                broker_config.clone(),
                broker_runtime_inner,
            ),
            topic_queue_mapping_manager,
            consumer_offset_manager: Arc::new(Default::default()),
            subscription_group_manager: Arc::new(Default::default()),
            consumer_filter_manager: Arc::new(Default::default()),
            consumer_order_info_manager: Arc::new(Default::default()),
            message_store: None,
            broker_stats: None,
            schedule_message_service: Default::default(),
            timer_message_store: None,
            broker_out_api: broker_outer_api,
            broker_runtime: Some(runtime),
            producer_manager: Arc::new(ProducerManager::new()),
            drop: Arc::new(AtomicBool::new(false)),
            shutdown: Arc::new(AtomicBool::new(false)),
            shutdown_hook: None,
            broker_stats_manager,
            topic_queue_mapping_clean_service: None,
            update_master_haserver_addr_periodically: false,
            should_start_time: Arc::new(AtomicU64::new(0)),
            is_isolated: Arc::new(AtomicBool::new(false)),
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
            let message_store = DefaultMessageStore::new(
                self.message_store_config.clone(),
                self.broker_config.clone(),
                self.topic_config_manager.topic_config_table(),
                Some(self.broker_stats_manager.clone()),
            );
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
            self.initialize_scheduled_tasks();
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

        // fast broker server implementation in future versions
    }

    fn initialize_resources(&mut self) {
        self.topic_queue_mapping_clean_service = Some(Arc::new(TopicQueueMappingCleanService));
    }

    fn init_processor(&self) -> BrokerRequestProcessor<DefaultMessageStore> {
        let send_message_processor = SendMessageProcessor::<DefaultMessageStore>::new(
            self.topic_queue_mapping_manager.clone(),
            self.topic_config_manager.clone(),
            self.broker_config.clone(),
            self.message_store.as_ref().unwrap(),
        );
        BrokerRequestProcessor {
            send_message_processor,
            pull_message_processor: Default::default(),
            peek_message_processor: Default::default(),
            pop_message_processor: Default::default(),
            ack_message_processor: Default::default(),
            change_invisible_time_processor: Default::default(),
            notification_processor: Default::default(),
            polling_info_processor: Default::default(),
            reply_message_processor: Default::default(),
            admin_broker_processor: Default::default(),
            client_manage_processor: ClientManageProcessor::new(self.producer_manager.clone()),
            consumer_manage_processor: Default::default(),
            query_assignment_processor: Default::default(),
            query_message_processor: Default::default(),
            end_transaction_processor: Default::default(),
        }
    }

    fn initialize_scheduled_tasks(&mut self) {
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
    }

    fn initial_transaction(&mut self) {}

    fn initial_acl(&mut self) {}

    fn initial_rpc_hooks(&mut self) {}
    fn initial_request_pipeline(&mut self) {}

    fn protect_broker(&mut self) {}

    fn start_basic_service(&mut self) {
        self.message_store
            .as_mut()
            .unwrap()
            .start()
            .expect("Message store start error");

        //start broker server
        let request_processor = self.init_processor();
        let server = RocketMQServer::new(self.server_config.clone());
        tokio::spawn(async move { server.run(request_processor).await });
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
        log::info!(
            "Rocketmq Broker({} ----Rust) start success",
            self.broker_config.broker_identity.broker_name
        );
    }

    pub(crate) fn schedule_send_heartbeat(&mut self) {}

    pub(crate) fn start_service_without_condition(&mut self) {}

    /// Register broker to name server
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
            data_version: Some(data_version),
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
        serialize_wrapper.topic_config_table = Some(topic_config_table);
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
