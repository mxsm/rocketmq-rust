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
use std::sync::Arc;
use std::sync::Weak;

use parking_lot::RwLock;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;

use crate::client::consumer_group_info::ConsumerGroupInfo;
use crate::client::consumer_ids_change_listener::ConsumerIdsChangeListener;

pub struct ConsumerManager {
    consumer_table: Arc<RwLock<HashMap<String, ConsumerGroupInfo>>>,
    consumer_compensation_table: Arc<RwLock<HashMap<String, ConsumerGroupInfo>>>,
    consumer_ids_change_listener_list:
        Arc<RwLock<Vec<Arc<dyn ConsumerIdsChangeListener + Send + Sync>>>>,
    broker_stats_manager: Arc<RwLock<Option<Weak<BrokerStatsManager>>>>,
    channel_expired_timeout: u64,
    subscription_expired_timeout: u64,
}

impl ConsumerManager {
    pub fn new(
        consumer_ids_change_listener: Arc<dyn ConsumerIdsChangeListener + Send + Sync>,
        expired_timeout: u64,
    ) -> Self {
        let consumer_ids_change_listener_list = vec![consumer_ids_change_listener];
        ConsumerManager {
            consumer_table: Arc::new(RwLock::new(HashMap::new())),
            consumer_compensation_table: Arc::new(RwLock::new(HashMap::new())),
            consumer_ids_change_listener_list: Arc::new(RwLock::new(
                consumer_ids_change_listener_list,
            )),
            broker_stats_manager: Arc::new(Default::default()),
            channel_expired_timeout: expired_timeout,
            subscription_expired_timeout: expired_timeout,
        }
    }

    pub fn new_with_broker_stats(
        consumer_ids_change_listener: Arc<dyn ConsumerIdsChangeListener + Send + Sync>,
        broker_config: Arc<BrokerConfig>,
    ) -> Self {
        let consumer_ids_change_listener_list = vec![consumer_ids_change_listener];
        ConsumerManager {
            consumer_table: Arc::new(RwLock::new(HashMap::new())),
            consumer_compensation_table: Arc::new(RwLock::new(HashMap::new())),
            consumer_ids_change_listener_list: Arc::new(RwLock::new(
                consumer_ids_change_listener_list,
            )),

            broker_stats_manager: Arc::new(Default::default()),
            channel_expired_timeout: broker_config.channel_expired_timeout,
            subscription_expired_timeout: broker_config.subscription_expired_timeout,
        }
    }
}

impl ConsumerManager {
    pub fn set_broker_stats_manager(&self, broker_stats_manager: Option<Weak<BrokerStatsManager>>) {
        *self.broker_stats_manager.write() = broker_stats_manager;
    }
}

impl ConsumerManager {
    pub fn find_subscription_data(&self, group: &str, topic: &str) -> Option<SubscriptionData> {
        self.find_subscription_data_internal(group, topic, true)
    }

    pub fn find_subscription_data_internal(
        &self,
        group: &str,
        topic: &str,
        from_compensation_table: bool,
    ) -> Option<SubscriptionData> {
        if let Some(consumer_group_info) = self.get_consumer_group_info_internal(group, false) {
            if let Some(subscription_data) = consumer_group_info.find_subscription_data(topic) {
                return Some(subscription_data);
            }
        }

        if from_compensation_table {
            if let Some(consumer_group_info) = self.consumer_compensation_table.read().get(group) {
                return consumer_group_info.find_subscription_data(topic);
            }
        }
        None
    }
    pub fn get_consumer_group_info(&self, group: &str) -> Option<ConsumerGroupInfo> {
        self.get_consumer_group_info_internal(group, false)
    }

    pub fn get_consumer_group_info_internal(
        &self,
        group: &str,
        from_compensation_table: bool,
    ) -> Option<ConsumerGroupInfo> {
        if let Some(consumer_group_info) = self.consumer_table.read().get(group) {
            return Some(consumer_group_info.clone());
        }
        if from_compensation_table {
            if let Some(consumer_group_info) = self.consumer_compensation_table.read().get(group) {
                return Some(consumer_group_info.clone());
            }
        }
        None
    }

    pub fn compensate_subscribe_data(
        &self,
        group: &str,
        topic: &str,
        subscription_data: &SubscriptionData,
    ) {
        let mut write_guard = self.consumer_compensation_table.write();
        let consumer_group_info = write_guard
            .entry(group.to_string())
            .or_insert_with(|| ConsumerGroupInfo::with_group_name(group.to_string()));
        consumer_group_info
            .get_subscription_table()
            .write()
            .insert(topic.to_string(), subscription_data.clone());
    }
}
