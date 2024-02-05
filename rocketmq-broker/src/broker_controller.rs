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
use rocketmq_common::common::{broker::broker_config::BrokerConfig, config_manager::ConfigManager};
use rocketmq_store::config::message_store_config::MessageStoreConfig;

use crate::{
    filter::manager::consumer_filter_manager::ConsumerFilterManager,
    offset::manager::{
        consumer_offset_manager::ConsumerOffsetManager,
        consumer_order_info_manager::ConsumerOrderInfoManager,
    },
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
}

impl BrokerController {
    pub fn new(broker_config: BrokerConfig, store_config: MessageStoreConfig) -> Self {
        Self {
            broker_config,
            store_config,
            topic_config_manager_inner: TopicConfigManager::default(),
            topic_queue_mapping_manager: TopicQueueMappingManager::default(),
            consumer_offset_manager: ConsumerOffsetManager::default(),
            subscription_group_manager: SubscriptionGroupManager::default(),
            consumer_filter_manager: ConsumerFilterManager::default(),
            consumer_order_info_manager: ConsumerOrderInfoManager::default(),
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
