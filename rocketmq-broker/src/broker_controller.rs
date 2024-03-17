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
use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use rocketmq_common::{
    common::{config::TopicConfig, config_manager::ConfigManager, constant::PermName},
    TokioExecutorService,
};
use rocketmq_remoting::{
    code::request_code::RequestCode,
    protocol::{
        body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper,
        static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail,
    },
    remoting::RemotingService,
    runtime::config::client_config::TokioClientConfig,
    server::{rocketmq_server::RocketmqDefaultServer, RemotingServer},
};
use rocketmq_store::{
    base::store_enum::StoreType, config::message_store_config::MessageStoreConfig,
    log_file::MessageStore, message_store::local_file_store::LocalFileMessageStore,
    status::manager::broker_stats_manager::BrokerStatsManager,
    timer::timer_message_store::TimerMessageStore,
};
use tracing::{info, warn};

use crate::{
    broker_config::BrokerConfig,
    client::{
        default_consumer_ids_change_listener::DefaultConsumerIdsChangeListener,
        manager::{consumer_manager::ConsumerManager, producer_manager::ProducerManager},
        net::broker_to_client::Broker2Client,
    },
    coldctr::{
        cold_data_cg_ctr_service::ColdDataCgCtrService,
        cold_data_pull_request_hold_service::ColdDataPullRequestHoldService,
    },
    controller::replicas_manager::ReplicasManager,
    filter::manager::consumer_filter_manager::ConsumerFilterManager,
    longpolling::{
        longpolling_service::pull_request_hold_service::PullRequestHoldService,
        notify_message_arriving_listener::NotifyMessageArrivingListener,
    },
    offset::manager::{
        broadcast_offset_manager::BroadcastOffsetManager,
        consumer_offset_manager::ConsumerOffsetManager,
        consumer_order_info_manager::ConsumerOrderInfoManager,
    },
    out_api::broker_outer_api::BrokerOuterAPI,
    processor::{
        ack_message_processor::AckMessageProcessor, admin_broker_processor::AdminBrokerProcessor,
        change_invisible_time_processor::ChangeInvisibleTimeProcessor,
        notification_processor::NotificationProcessor,
        peek_message_processor::PeekMessageProcessor, polling_info_processor::PollingInfoProcessor,
        pop_message_processor::PopMessageProcessor, pull_message_processor::PullMessageProcessor,
        reply_message_processor::ReplyMessageProcessor,
        send_message_processor::SendMessageProcessor,
    },
    schedule::schedule_message_service::ScheduleMessageService,
    subscription::manager::subscription_group_manager::SubscriptionGroupManager,
    topic::manager::{
        topic_config_manager::TopicConfigManager,
        topic_queue_mapping_manager::TopicQueueMappingManager,
    },
};

pub struct BrokerController {
    pub broker_config: BrokerConfig,
    pub store_config: MessageStoreConfig,
    pub(crate) topic_config_manager_inner: TopicConfigManager,
    pub(crate) topic_queue_mapping_manager: TopicQueueMappingManager,
    pub(crate) consumer_offset_manager: ConsumerOffsetManager,
    pub(crate) subscription_group_manager: SubscriptionGroupManager,
    pub(crate) consumer_filter_manager: ConsumerFilterManager,
    pub(crate) consumer_order_info_manager: ConsumerOrderInfoManager,
    pub(crate) broker_stats_manager: BrokerStatsManager,
    pub(crate) store_host: SocketAddr,
    pub(crate) broadcast_offset_manager: BroadcastOffsetManager,
    pub(crate) pull_message_processor: PullMessageProcessor,
    pub(crate) peek_message_processor: PeekMessageProcessor,
    pub(crate) pull_request_hold_service: PullRequestHoldService,
    pub(crate) pop_message_processor: PopMessageProcessor,
    pub(crate) notification_processor: NotificationProcessor,
    pub(crate) polling_info_processor: PollingInfoProcessor,
    pub(crate) ack_message_processor: AckMessageProcessor,
    pub(crate) change_invisible_time_processor: ChangeInvisibleTimeProcessor,
    #[cfg(feature = "local_file_store")]
    pub(crate) send_message_processor: SendMessageProcessor<LocalFileMessageStore>,
    pub(crate) reply_message_processor: ReplyMessageProcessor,
    pub(crate) notify_message_arriving_listener: NotifyMessageArrivingListener,
    pub(crate) default_consumer_ids_change_listener: DefaultConsumerIdsChangeListener,
    pub(crate) consumer_manager: ConsumerManager,
    pub(crate) producer_manager: ProducerManager,
    pub(crate) broker_to_client: Broker2Client,
    pub(crate) schedule_message_service: ScheduleMessageService,
    pub(crate) cold_data_pull_request_hold_service: ColdDataPullRequestHoldService,
    pub(crate) cold_data_cg_ctr_service: ColdDataCgCtrService,
    #[cfg(feature = "local_file_store")]
    pub(crate) message_store: Option<LocalFileMessageStore>,
    pub(crate) timer_message_store: Option<TimerMessageStore>,
    pub(crate) replicas_manager: Option<ReplicasManager>,
    pub(crate) broker_server: Option<RocketmqDefaultServer>,
    pub(crate) fast_broker_server: Option<RocketmqDefaultServer>,

    //executors
    pub(crate) send_message_executor: Option<TokioExecutorService>,

    pub(crate) broker_out_api: BrokerOuterAPI,
}

impl BrokerController {
    pub fn new(broker_config: BrokerConfig, store_config: MessageStoreConfig) -> Self {
        let broker_ip1 = broker_config.broker_ip1.clone();
        let listen_port = broker_config.listen_port;
        Self {
            broker_config,
            store_config,
            topic_config_manager_inner: TopicConfigManager::default(),
            topic_queue_mapping_manager: TopicQueueMappingManager::default(),
            consumer_offset_manager: ConsumerOffsetManager::default(),
            subscription_group_manager: SubscriptionGroupManager::default(),
            consumer_filter_manager: ConsumerFilterManager::default(),
            consumer_order_info_manager: ConsumerOrderInfoManager::default(),
            broker_stats_manager: BrokerStatsManager::default(),
            store_host: format!("{}:{}", broker_ip1, listen_port)
                .parse::<SocketAddr>()
                .unwrap(),
            broadcast_offset_manager: BroadcastOffsetManager::default(),
            pull_message_processor: PullMessageProcessor::default(),
            peek_message_processor: PeekMessageProcessor::default(),
            pull_request_hold_service: PullRequestHoldService::default(),
            pop_message_processor: PopMessageProcessor::default(),
            notification_processor: NotificationProcessor::default(),
            polling_info_processor: PollingInfoProcessor::default(),
            ack_message_processor: AckMessageProcessor::default(),
            change_invisible_time_processor: ChangeInvisibleTimeProcessor::default(),
            send_message_processor: SendMessageProcessor::default(),
            reply_message_processor: ReplyMessageProcessor::default(),
            notify_message_arriving_listener: NotifyMessageArrivingListener::default(),
            default_consumer_ids_change_listener: DefaultConsumerIdsChangeListener::default(),
            consumer_manager: ConsumerManager::default(),
            producer_manager: ProducerManager::default(),
            broker_to_client: Broker2Client::default(),
            schedule_message_service: ScheduleMessageService::default(),
            cold_data_pull_request_hold_service: ColdDataPullRequestHoldService::default(),
            cold_data_cg_ctr_service: Default::default(),
            message_store: None,
            timer_message_store: None,
            replicas_manager: None,
            broker_server: None,
            fast_broker_server: None,
            send_message_executor: Some(TokioExecutorService::new()),
            broker_out_api: BrokerOuterAPI::new(TokioClientConfig::default()),
        }
    }
}

#[allow(unused_variables)]
impl BrokerController {
    pub async fn start(&mut self) {
        if self.message_store.as_mut().is_some() {
            let _ = self.message_store.as_mut().unwrap().start();
        }

        if let Some(ref mut timer_message_store) = self.timer_message_store {
            timer_message_store.start();
        }

        if let Some(ref mut replicas_manager) = self.replicas_manager {
            replicas_manager.start();
        }

        self.register_broker_all(true, false, true).await;

        if let Some(ref mut broker_server) = self.broker_server {
            broker_server.start().await;
        }

        //other service start
    }

    pub fn initialize(&mut self) -> bool {
        let mut result = self.initialize_metadata();
        if !result {
            warn!("Initialize metadata failed");
            return false;
        }
        info!("======Ending initialize metadata Success========");
        result = self.initialize_message_store();
        if !result {
            return false;
        }
        self.recover_and_init_service()
    }

    pub fn initialize_metadata(&mut self) -> bool {
        info!("======Starting initialize metadata========");
        self.topic_config_manager_inner.load()
            & self.topic_queue_mapping_manager.load()
            & self.consumer_offset_manager.load()
            & self.subscription_group_manager.load()
            & self.consumer_filter_manager.load()
            & self.consumer_order_info_manager.load()
    }

    pub fn initialize_message_store(&mut self) -> bool {
        if self.store_config.store_type == StoreType::LocalFile {
            info!("Use local file as message store");
            self.message_store = Some(LocalFileMessageStore::default());
        } else if self.store_config.store_type == StoreType::RocksDB {
            info!("Use RocksDB as message store");
        } else {
            warn!("Unknown store type");
            return false;
        }
        true
    }

    pub fn recover_and_init_service(&mut self) -> bool {
        let mut result = true;

        if self.broker_config.enable_controller_mode {
            info!("Start controller mode(Support for future versions)");
            todo!()
        }
        if self.message_store.is_some() {
            result = self.message_store.as_mut().unwrap().load();
        }

        if self.broker_config.timer_wheel_config.timer_wheel_enable {
            result &= self.timer_message_store.as_mut().unwrap().load();
        }
        result &= self.schedule_message_service.load();

        if result {
            self.initialize_remoting_server();
            self.initialize_resources();
            self.register_processor();
            self.initialize_scheduled_tasks();
            self.initial_transaction();
            self.initial_acl();
            self.initial_rpc_hooks();
        }
        result
    }

    fn initialize_remoting_server(&mut self) {
        let broker_server =
            RocketmqDefaultServer::new(self.broker_config.broker_server_config.clone());
        self.broker_server = Some(broker_server);

        // fast broker server implementation in future versions
    }

    fn initialize_resources(&mut self) {}

    fn register_processor(&mut self) {
        let broker_server = self.broker_server.as_mut().unwrap();
        let send_message_processor =
            Arc::new(SendMessageProcessor::<LocalFileMessageStore>::default());
        broker_server.register_processor(RequestCode::SendMessage, send_message_processor.clone());
        broker_server
            .register_processor(RequestCode::SendMessageV2, send_message_processor.clone());
        broker_server.register_processor(
            RequestCode::SendBatchMessage,
            send_message_processor.clone(),
        );
        broker_server.register_processor(RequestCode::ConsumerSendMsgBack, send_message_processor);
        broker_server.register_default_processor(AdminBrokerProcessor::default());
    }

    fn initialize_scheduled_tasks(&mut self) {}

    fn initial_transaction(&mut self) {}

    fn initial_acl(&mut self) {}

    fn initial_rpc_hooks(&mut self) {}

    /// Register broker to name server
    pub(crate) async fn register_broker_all(
        &mut self,
        check_order_config: bool,
        oneway: bool,
        force_register: bool,
    ) {
        let mut topic_config_table = HashMap::new();

        for topic_config in self.topic_config_manager_inner.topic_config_table.values() {
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
                .topic_config_manager_inner
                .build_serialize_wrapper(topic_config_table.clone());
            self.do_register_broker_all(check_order_config, oneway, topic_config_wrapper)
                .await;
            topic_config_table.clear();
        }

        // Collect topicQueueMappingInfoMap
        let topic_queue_mapping_info_map = self
            .topic_queue_mapping_manager
            .topic_queue_mapping_table
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    TopicQueueMappingDetail::clone_as_mapping_info(value),
                )
            })
            .collect();

        let topic_config_wrapper = self
            .topic_config_manager_inner
            .build_serialize_wrapper_with_topic_queue_map(
                topic_config_table,
                topic_queue_mapping_info_map,
            );

        if self.broker_config.enable_split_registration
            || force_register
            || self.need_register(
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
        &mut self,
        cluster_name: &str,
        broker_addr: &str,
        broker_name: &str,
        broker_id: u64,
        register_timeout_mills: i32,
        in_broker_container: bool,
    ) -> bool {
        unimplemented!()
    }

    async fn do_register_broker_all(
        &mut self,
        check_order_config: bool,
        oneway: bool,
        topic_config_wrapper: TopicConfigAndMappingSerializeWrapper,
    ) {
        /*let broker = Arc::new(tokio::sync::RwLock::new(self));
        let cluster_name = broker
            .read()
            .await
            .broker_config
            .broker_identity
            .broker_cluster_name
            .clone();
        let broker_name = broker
            .read()
            .await
            .broker_config
            .broker_identity
            .broker_name
            .clone();
        let broker_addr = broker.read().await.broker_config.broker_ip1.clone();
        let broker_id = broker.read().await.broker_config.broker_identity.broker_id;
        broker
            .write()
            .await
            .broker_out_api
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
            .await;*/

        let cluster_name = self
            .broker_config
            .broker_identity
            .broker_cluster_name
            .clone();
        let broker_name = self.broker_config.broker_identity.broker_name.clone();
        let broker_addr = self.broker_config.broker_ip1.clone();
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

impl Drop for BrokerController {
    fn drop(&mut self) {
        if let Some(ref mut broker_server) = self.broker_server {
            broker_server.shutdown();
        }

        match self.send_message_executor.take() {
            None => {}
            Some(value) => {
                info!("Shutdown send message executor");
                value.shutdown()
            }
        }
    }
}
