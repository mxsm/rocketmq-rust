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

use std::{collections::HashMap, sync::Arc, time::Duration};

use rocketmq_common::common::{
    config::TopicConfig, config_manager::ConfigManager, constant::PermName,
};
use rocketmq_remoting::{
    protocol::{
        body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper,
        static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail,
    },
    runtime::{config::client_config::TokioClientConfig, server::RocketMQServer},
    server::config::ServerConfig,
};
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_store::{
    base::store_enum::StoreType, config::message_store_config::MessageStoreConfig,
    message_store::local_file_store::LocalFileMessageStore,
    timer::timer_message_store::TimerMessageStore,
};
use tracing::{info, warn};

use crate::{
    broker_config::BrokerConfig,
    filter::manager::consumer_filter_manager::ConsumerFilterManager,
    offset::manager::{
        consumer_offset_manager::ConsumerOffsetManager,
        consumer_order_info_manager::ConsumerOrderInfoManager,
    },
    out_api::broker_outer_api::BrokerOuterAPI,
    processor::{send_message_processor::SendMessageProcessor, BrokerRequestProcessor},
    schedule::schedule_message_service::ScheduleMessageService,
    subscription::manager::subscription_group_manager::SubscriptionGroupManager,
    topic::manager::{
        topic_config_manager::TopicConfigManager,
        topic_queue_mapping_manager::TopicQueueMappingManager,
    },
};

pub(crate) struct BrokerRuntime {
    broker_config: Arc<BrokerConfig>,
    message_store_config: Arc<MessageStoreConfig>,
    server_config: Arc<ServerConfig>,
    topic_config_manager: Arc<TopicConfigManager>,
    topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    consumer_offset_manager: Arc<ConsumerOffsetManager>,
    subscription_group_manager: Arc<SubscriptionGroupManager>,
    consumer_filter_manager: Arc<ConsumerFilterManager>,
    consumer_order_info_manager: Arc<ConsumerOrderInfoManager>,
    #[cfg(feature = "local_file_store")]
    message_store: Option<Arc<LocalFileMessageStore>>,
    schedule_message_service: ScheduleMessageService,
    timer_message_store: Option<TimerMessageStore>,

    broker_out_api: Arc<BrokerOuterAPI>,

    broker_runtime: Option<RocketMQRuntime>,
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
            schedule_message_service: Default::default(),
            timer_message_store: self.timer_message_store.clone(),
            broker_out_api: self.broker_out_api.clone(),
            broker_runtime: None,
        }
    }
}

impl BrokerRuntime {
    pub(crate) fn new(
        broker_config: BrokerConfig,
        message_store_config: MessageStoreConfig,
        server_config: ServerConfig,
    ) -> Self {
        let runtime = RocketMQRuntime::new_multi(10, "broker-thread");
        Self {
            broker_config: Arc::new(broker_config),
            message_store_config: Arc::new(message_store_config),
            server_config: Arc::new(server_config),
            topic_config_manager: Arc::new(Default::default()),
            topic_queue_mapping_manager: Arc::new(Default::default()),
            consumer_offset_manager: Arc::new(Default::default()),
            subscription_group_manager: Arc::new(Default::default()),
            consumer_filter_manager: Arc::new(Default::default()),
            consumer_order_info_manager: Arc::new(Default::default()),
            message_store: None,
            schedule_message_service: Default::default(),
            timer_message_store: None,
            broker_out_api: Arc::new(BrokerOuterAPI::new(TokioClientConfig::default())),
            broker_runtime: Some(runtime),
        }
    }

    pub(crate) fn broker_config(&self) -> &BrokerConfig {
        &self.broker_config
    }

    pub(crate) fn message_store_config(&self) -> &MessageStoreConfig {
        &self.message_store_config
    }
}

impl Drop for BrokerRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.broker_runtime.take() {
            runtime.shutdown();
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
        result = self.initialize_message_store();
        if !result {
            return false;
        }
        self.recover_initialize_service()
    }

    fn initialize_metadata(&self) -> bool {
        info!("======Starting initialize metadata========");

        self.topic_config_manager.load()
            & self.topic_queue_mapping_manager.load()
            & self.consumer_offset_manager.load()
            & self.subscription_group_manager.load()
            & self.consumer_filter_manager.load()
            & self.consumer_order_info_manager.load()
    }

    fn initialize_message_store(&mut self) -> bool {
        if self.message_store_config.store_type == StoreType::LocalFile {
            info!("Use local file as message store");
            self.message_store = Some(Arc::new(LocalFileMessageStore::new(self.message_store_config.clone())));
        } else if self.message_store_config.store_type == StoreType::RocksDB {
            info!("Use RocksDB as message store");
        } else {
            warn!("Unknown store type");
            return false;
        }
        true
    }

    fn recover_initialize_service(&mut self) -> bool {
        let mut result = true;

        if self.broker_config.enable_controller_mode {
            info!("Start controller mode(Support for future versions)");
            todo!()
        }
        if self.message_store.is_some() {
            // result = self.message_store.as_ref().unwrap().write().load();
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
        }
        result
    }

    fn initialize_remoting_server(&mut self) {

        // fast broker server implementation in future versions
    }

    fn initialize_resources(&mut self) {}

    fn init_processor(&self) -> Arc<BrokerRequestProcessor<LocalFileMessageStore>> {
        let send_message_processor = SendMessageProcessor::<LocalFileMessageStore>::new(
            self.topic_queue_mapping_manager.clone(),
            self.topic_config_manager.clone(),
            self.broker_config.clone(),
            self.message_store.clone().unwrap(),
        );
        Arc::new(BrokerRequestProcessor {
            send_message_processor,
            admin_broker_processor: Default::default(),
        })
    }

    fn initialize_scheduled_tasks(&mut self) {}

    fn initial_transaction(&mut self) {}

    fn initial_acl(&mut self) {}

    fn initial_rpc_hooks(&mut self) {}

    pub async fn start(&mut self) {
        let request_processor = self.init_processor();
        let server = RocketMQServer::new(self.server_config.clone(), request_processor);
        let server_future = server.run();
        self.register_broker_all(true, false, true).await;
        let mut cloned_broker_runtime = self.clone();

        self.broker_runtime
            .as_ref()
            .unwrap()
            .get_handle()
            .spawn(async move {
                let period = Duration::from_secs(10);
                let initial_delay = Some(Duration::from_secs(60));
                // initial delay
                if let Some(initial_delay_inner) = initial_delay {
                    tokio::time::sleep(initial_delay_inner).await;
                }

                loop {
                    // record current execution time
                    let current_execution_time = tokio::time::Instant::now();
                    // execute task
                    cloned_broker_runtime
                        .register_broker_all(true, false, true)
                        .await;
                    // Calculate the time of the next execution
                    let next_execution_time = current_execution_time + period;

                    // Wait until the next execution
                    let delay =
                        next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(delay).await;
                }
            });

        server_future.await;
    }

    /// Register broker to name server
    pub(crate) async fn register_broker_all(
        &mut self,
        check_order_config: bool,
        oneway: bool,
        force_register: bool,
    ) {
        let mut topic_config_table = HashMap::new();

        for topic_config in self.topic_config_manager.topic_config_table.lock().values() {
            let new_topic_config = if !PermName::is_writeable(self.broker_config.broker_permission)
                || !PermName::is_readable(self.broker_config.broker_permission)
            {
                TopicConfig {
                    topic_name: topic_config.topic_name.clone(),
                    read_queue_nums: topic_config.read_queue_nums,
                    write_queue_nums: topic_config.write_queue_nums,
                    perm: topic_config.perm & self.broker_config.broker_permission as u32,
                    ..TopicConfig::default()
                }
            } else {
                topic_config.clone()
            };
            topic_config_table.insert(new_topic_config.topic_name.clone(), new_topic_config);
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
