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
use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Weak;

use parking_lot::RwLock;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;

use crate::client::client_channel_info::ClientChannelInfo;
use crate::client::consumer_group_event::ConsumerGroupEvent;
use crate::client::consumer_group_info::ConsumerGroupInfo;
use crate::client::consumer_ids_change_listener::ConsumerIdsChangeListener;

pub struct ConsumerManager {
    consumer_table: Arc<RwLock<HashMap<String, ConsumerGroupInfo>>>,
    consumer_compensation_table: Arc<RwLock<HashMap<String, ConsumerGroupInfo>>>,
    consumer_ids_change_listener_list:
        Vec<Box<dyn ConsumerIdsChangeListener + Send + Sync + 'static>>,
    broker_stats_manager: Arc<RwLock<Option<Weak<BrokerStatsManager>>>>,
    channel_expired_timeout: u64,
    subscription_expired_timeout: u64,
}

impl ConsumerManager {
    pub fn new(
        consumer_ids_change_listener: Box<dyn ConsumerIdsChangeListener + Send + Sync + 'static>,
        expired_timeout: u64,
    ) -> Self {
        let consumer_ids_change_listener_list = vec![consumer_ids_change_listener];
        ConsumerManager {
            consumer_table: Arc::new(RwLock::new(HashMap::new())),
            consumer_compensation_table: Arc::new(RwLock::new(HashMap::new())),
            consumer_ids_change_listener_list,
            broker_stats_manager: Arc::new(Default::default()),
            channel_expired_timeout: expired_timeout,
            subscription_expired_timeout: expired_timeout,
        }
    }

    pub fn new_with_broker_stats(
        consumer_ids_change_listener: Box<dyn ConsumerIdsChangeListener + Send + Sync + 'static>,
        broker_config: Arc<BrokerConfig>,
    ) -> Self {
        let consumer_ids_change_listener_list = vec![consumer_ids_change_listener];
        ConsumerManager {
            consumer_table: Arc::new(RwLock::new(HashMap::new())),
            consumer_compensation_table: Arc::new(RwLock::new(HashMap::new())),
            consumer_ids_change_listener_list,
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

    pub fn find_subscription_data_count(&self, group: &str) -> usize {
        if let Some(consumer_group_info) = self.get_consumer_group_info(group) {
            return consumer_group_info.get_subscription_table().read().len();
        }
        0
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

    pub fn compensate_basic_consumer_info(
        &self,
        group: &str,
        consume_type: ConsumeType,
        message_model: MessageModel,
    ) {
        let mut write_guard = self.consumer_compensation_table.write();
        let consumer_group_info = write_guard
            .entry(group.to_string())
            .or_insert_with(|| ConsumerGroupInfo::with_group_name(group.to_string()));
        consumer_group_info.set_consume_type(consume_type);
        consumer_group_info.set_message_model(message_model);
    }

    pub fn register_consumer(
        &self,
        group: &str,
        client_channel_info: ClientChannelInfo,
        consume_type: ConsumeType,
        message_model: MessageModel,
        consume_from_where: ConsumeFromWhere,
        sub_list: HashSet<SubscriptionData>,
        is_notify_consumer_ids_changed_enable: bool,
    ) -> bool {
        self.register_consumer_ext(
            group,
            client_channel_info,
            consume_type,
            message_model,
            consume_from_where,
            sub_list,
            is_notify_consumer_ids_changed_enable,
            true,
        )
    }

    fn register_consumer_ext(
        &self,
        group: &str,
        client_channel_info: ClientChannelInfo,
        consume_type: ConsumeType,
        message_model: MessageModel,
        consume_from_where: ConsumeFromWhere,
        sub_list: HashSet<SubscriptionData>,
        is_notify_consumer_ids_changed_enable: bool,
        update_subscription: bool,
    ) -> bool {
        let mut write_guard = self.consumer_table.write();
        let consumer_group_info = write_guard.entry(group.to_string()).or_insert_with(|| {
            ConsumerGroupInfo::new(
                group.to_string(),
                consume_type,
                message_model,
                consume_from_where,
            )
        });
        let r1 = consumer_group_info.update_channel(
            client_channel_info.clone(),
            consume_type,
            message_model,
            consume_from_where,
        );
        let r2 = if update_subscription {
            consumer_group_info.update_subscription(&sub_list)
        } else {
            false
        };

        if (r1 || r2) && is_notify_consumer_ids_changed_enable {
            let all_channel = consumer_group_info.get_all_channels();
            self.call_consumer_ids_change_listener(
                ConsumerGroupEvent::Change,
                group,
                &[&all_channel as &dyn Any],
            );
        }

        /*if (null != this.brokerStatsManager) {
            this.brokerStatsManager.incConsumerRegisterTime((int) (System.currentTimeMillis() - start));
        }*/

        self.call_consumer_ids_change_listener(
            ConsumerGroupEvent::Register,
            group,
            &[&sub_list as &dyn Any, &client_channel_info as &dyn Any],
        );

        r1 || r2
    }

    pub fn call_consumer_ids_change_listener(
        &self,
        event: ConsumerGroupEvent,
        group: &str,
        args: &[&dyn Any],
    ) {
        for listener in self.consumer_ids_change_listener_list.iter() {
            listener.handle(event, group, args);
        }
    }

    pub fn query_topic_consume_by_who(&self, topic: &str) -> HashSet<String> {
        let mut groups = HashSet::new();
        for (group, consumer_group_info) in self.consumer_table.read().iter() {
            if consumer_group_info.find_subscription_data(topic).is_some() {
                groups.insert(group.clone());
            }
        }
        groups
    }
}
