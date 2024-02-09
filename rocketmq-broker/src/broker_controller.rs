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
use std::net::SocketAddr;

use rocketmq_common::common::{broker::broker_config::BrokerConfig, config_manager::ConfigManager};
use rocketmq_store::{
    config::message_store_config::MessageStoreConfig,
    status::manager::broker_stats_manager::BrokerStatsManager,
};

use crate::{
    broker_outer_api::BrokerOuterAPI,
    client::{
        default_consumer_ids_change_listener::DefaultConsumerIdsChangeListener,
        manager::{consumer_manager::ConsumerManager, producer_manager::ProducerManager},
        net::broker_to_client::Broker2Client,
    },
    coldctr::{
        cold_data_cg_ctr_service::ColdDataCgCtrService,
        cold_data_pull_request_hold_service::ColdDataPullRequestHoldService,
    },
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
    processor::{
        ack_message_processor::AckMessageProcessor,
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
    pub(crate) send_message_processor: SendMessageProcessor,
    pub(crate) reply_message_processor: ReplyMessageProcessor,
    pub(crate) notify_message_arriving_listener: NotifyMessageArrivingListener,
    pub(crate) default_consumer_ids_change_listener: DefaultConsumerIdsChangeListener,
    pub(crate) consumer_manager: ConsumerManager,
    pub(crate) producer_manager: ProducerManager,
    pub(crate) broker_to_client: Broker2Client,
    pub(crate) schedule_message_service: ScheduleMessageService,
    pub(crate) cold_data_pull_request_hold_service: ColdDataPullRequestHoldService,
    pub(crate) cold_data_cg_ctr_service: ColdDataCgCtrService,
    pub(crate) broker_outer_api: BrokerOuterAPI,
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
            broker_outer_api: Default::default(),
        }
    }
}

impl BrokerController {
    pub async fn start(&mut self) {}

    pub fn initialize(&mut self) -> bool {
        let mut result = self.initialize_metadata();
        if !result {
            return false;
        }
        result = self.initialize_message_store();
        if !result {
            return false;
        }
        self.recover_and_init_service()
    }

    pub fn initialize_metadata(&mut self) -> bool {
        self.topic_config_manager_inner.load()
            & self.topic_queue_mapping_manager.load()
            & self.consumer_offset_manager.load()
            & self.subscription_group_manager.load()
            & self.consumer_filter_manager.load()
            & self.consumer_order_info_manager.load()
    }

    pub fn initialize_message_store(&mut self) -> bool {
        true
    }

    pub fn recover_and_init_service(&mut self) -> bool {
        true
    }
}
