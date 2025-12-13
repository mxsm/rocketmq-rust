//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Instant;

use cheetah_string::CheetahString;
use parking_lot::RwLock;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use tracing::info;
use tracing::warn;

use crate::client::client_channel_info::ClientChannelInfo;
use crate::client::consumer_group_event::ConsumerGroupEvent;
use crate::client::consumer_group_info::ConsumerGroupInfo;
use crate::client::consumer_ids_change_listener::ConsumerIdsChangeListener;

pub struct ConsumerManager {
    consumer_table: Arc<RwLock<HashMap<CheetahString, ConsumerGroupInfo>>>,
    consumer_compensation_table: Arc<RwLock<HashMap<CheetahString, ConsumerGroupInfo>>>,
    consumer_ids_change_listener_list:
        Vec<Arc<Box<dyn ConsumerIdsChangeListener + Send + Sync + 'static>>>,
    broker_stats_manager: Option<Weak<BrokerStatsManager>>,
    channel_expired_timeout: u64,
    subscription_expired_timeout: u64,
}

impl ConsumerManager {
    pub fn new(
        consumer_ids_change_listener: Arc<
            Box<dyn ConsumerIdsChangeListener + Send + Sync + 'static>,
        >,
        expired_timeout: u64,
    ) -> Self {
        let consumer_ids_change_listener_list = vec![consumer_ids_change_listener];
        ConsumerManager {
            consumer_table: Arc::new(RwLock::new(HashMap::new())),
            consumer_compensation_table: Arc::new(RwLock::new(HashMap::new())),
            consumer_ids_change_listener_list,
            broker_stats_manager: None,
            channel_expired_timeout: expired_timeout,
            subscription_expired_timeout: expired_timeout,
        }
    }

    pub fn new_with_broker_stats(
        consumer_ids_change_listener: Arc<
            Box<dyn ConsumerIdsChangeListener + Send + Sync + 'static>,
        >,
        broker_config: Arc<BrokerConfig>,
    ) -> Self {
        let consumer_ids_change_listener_list = vec![consumer_ids_change_listener];
        ConsumerManager {
            consumer_table: Arc::new(RwLock::new(HashMap::new())),
            consumer_compensation_table: Arc::new(RwLock::new(HashMap::new())),
            consumer_ids_change_listener_list,
            broker_stats_manager: None,
            channel_expired_timeout: broker_config.channel_expired_timeout,
            subscription_expired_timeout: broker_config.subscription_expired_timeout,
        }
    }
}

impl ConsumerManager {
    pub fn set_broker_stats_manager(&mut self, broker_stats_manager: Weak<BrokerStatsManager>) {
        self.broker_stats_manager = Some(broker_stats_manager);
    }

    pub fn find_channel_by_client_id(
        &self,
        group: &str,
        client_id: &str,
    ) -> Option<ClientChannelInfo> {
        let consumer_table = self.consumer_table.read();
        if let Some(consumer_group_info) = consumer_table.get(group) {
            return consumer_group_info.find_channel_by_client_id(client_id);
        }
        None
    }

    pub fn find_channel_by_channel(
        &self,
        group: &str,
        channel: &Channel,
    ) -> Option<ClientChannelInfo> {
        let consumer_table = self.consumer_table.read();
        if let Some(consumer_group_info) = consumer_table.get(group) {
            return consumer_group_info.find_channel_by_channel(channel);
        }
        None
    }

    pub fn find_subscription_data(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
    ) -> Option<SubscriptionData> {
        self.find_subscription_data_internal(group, topic, true)
    }

    pub fn find_subscription_data_internal(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
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

    pub fn find_subscription_data_count(&self, group: &CheetahString) -> usize {
        if let Some(consumer_group_info) = self.get_consumer_group_info(group) {
            return consumer_group_info.get_subscription_table().len();
        }
        0
    }

    pub fn get_consumer_group_info(&self, group: &CheetahString) -> Option<ConsumerGroupInfo> {
        self.get_consumer_group_info_internal(group, false)
    }

    pub fn get_consumer_group_info_internal(
        &self,
        group: &CheetahString,
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
        group: &CheetahString,
        topic: &CheetahString,
        subscription_data: &SubscriptionData,
    ) {
        let mut write_guard = self.consumer_compensation_table.write();
        let consumer_group_info = write_guard
            .entry(group.clone())
            .or_insert_with(|| ConsumerGroupInfo::with_group_name(group.clone()));
        consumer_group_info
            .get_subscription_table()
            .insert(topic.into(), subscription_data.clone());
    }

    pub fn compensate_basic_consumer_info(
        &self,
        group: &CheetahString,
        consume_type: ConsumeType,
        message_model: MessageModel,
    ) {
        let mut write_guard = self.consumer_compensation_table.write();
        let consumer_group_info = write_guard
            .entry(group.clone())
            .or_insert_with(|| ConsumerGroupInfo::with_group_name(group.clone()));
        consumer_group_info.set_consume_type(consume_type);
        consumer_group_info.set_message_model(message_model);
    }

    pub fn register_consumer(
        &self,
        group: &CheetahString,
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

    pub fn register_consumer_ext(
        &self,
        group: &CheetahString,
        client_channel_info: ClientChannelInfo,
        consume_type: ConsumeType,
        message_model: MessageModel,
        consume_from_where: ConsumeFromWhere,
        sub_list: HashSet<SubscriptionData>,
        is_notify_consumer_ids_changed_enable: bool,
        update_subscription: bool,
    ) -> bool {
        let start = Instant::now();
        let mut write_guard = self.consumer_table.write();
        let consumer_group_info = write_guard.entry(group.clone()).or_insert_with(|| {
            ConsumerGroupInfo::new(
                group.clone(),
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

        if r1 {
            let topics: HashSet<CheetahString> =
                sub_list.iter().map(|item| item.topic.clone()).collect();
            self.call_consumer_ids_change_listener(
                ConsumerGroupEvent::ClientRegister,
                group,
                &[&client_channel_info as &dyn Any, &topics as &dyn Any],
            );
        }

        let r2 = if update_subscription {
            consumer_group_info.update_subscription(&sub_list)
        } else {
            false
        };

        if (r1 || r2)
            && is_notify_consumer_ids_changed_enable
            && consumer_group_info.get_message_model() != MessageModel::Broadcasting
        {
            let all_channel = consumer_group_info.get_all_channels();
            self.call_consumer_ids_change_listener(
                ConsumerGroupEvent::Change,
                group,
                &[&all_channel as &dyn Any],
            );
        }

        if let Some(broker_stats_manager) = self.broker_stats_manager.as_ref() {
            if let Some(broker_stats_manager) = broker_stats_manager.upgrade() {
                broker_stats_manager.inc_consumer_register_time(start.elapsed().as_millis() as i32);
            }
        }
        self.call_consumer_ids_change_listener(
            ConsumerGroupEvent::Register,
            group,
            &[&sub_list as &dyn Any, &client_channel_info as &dyn Any],
        );

        r1 || r2
    }

    pub fn register_consumer_without_sub(
        &self,
        group: &CheetahString,
        client_channel_info: ClientChannelInfo,
        consume_type: ConsumeType,
        message_model: MessageModel,
        consume_from_where: ConsumeFromWhere,
        is_notify_consumer_ids_changed_enable: bool,
    ) -> bool {
        let start = Instant::now();
        let mut write_guard = self.consumer_table.write();
        let consumer_group_info = write_guard.entry(group.clone()).or_insert_with(|| {
            ConsumerGroupInfo::new(
                group.clone(),
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

        if r1
            && is_notify_consumer_ids_changed_enable
            && is_broadcast_mode(consumer_group_info.get_message_model())
        {
            let channels = consumer_group_info.get_all_channels();
            self.call_consumer_ids_change_listener(
                ConsumerGroupEvent::Change,
                group,
                &[&channels as &dyn Any],
            );
        }

        if let Some(broker_stats_manager) = self.broker_stats_manager.as_ref() {
            if let Some(broker_stats_manager) = broker_stats_manager.upgrade() {
                broker_stats_manager.inc_consumer_register_time(start.elapsed().as_millis() as i32);
            }
        }
        r1
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

    pub fn query_topic_consume_by_who(&self, topic: &CheetahString) -> HashSet<CheetahString> {
        let mut groups = HashSet::new();
        for (group, consumer_group_info) in self.consumer_table.read().iter() {
            if consumer_group_info.find_subscription_data(topic).is_some() {
                groups.insert(group.clone());
            }
        }
        groups
    }

    pub fn unregister_consumer(
        &self,
        group: &str,
        client_channel_info: &ClientChannelInfo,
        is_notify_consumer_ids_changed_enable: bool,
    ) {
        let mut consumer_table = self.consumer_table.write();
        let consumer_group_info = consumer_table.get_mut(group);
        if consumer_group_info.is_none() {
            return;
        }
        let consumer_group_info = consumer_group_info.unwrap();
        let removed = consumer_group_info.unregister_channel(client_channel_info);
        if removed {
            self.call_consumer_ids_change_listener(
                ConsumerGroupEvent::ClientUnregister,
                group,
                &[
                    client_channel_info as &dyn Any,
                    &consumer_group_info.get_subscribe_topics() as &dyn Any,
                ],
            );
        }
        let message_model = consumer_group_info.get_message_model();
        let channels = consumer_group_info.get_all_channels();
        if consumer_group_info.get_channel_info_table().is_empty()
            && consumer_table.remove(group).is_some()
        {
            info!(
                "unregister consumer ok, no any connection, and remove consumer group, {}",
                group
            );

            self.call_consumer_ids_change_listener(ConsumerGroupEvent::Unregister, group, &[]);
        }

        if is_notify_consumer_ids_changed_enable && !is_broadcast_mode(message_model) {
            self.call_consumer_ids_change_listener(
                ConsumerGroupEvent::Change,
                group,
                &[&channels as &dyn Any],
            );
        }
    }

    pub fn remove_expire_consumer_group_info(&self) {
        let mut remove_list = Vec::new();

        // Using entries() to get mutable access to the map values
        let mut consumer_compensation_table = self.consumer_compensation_table.write();
        for (group, consumer_group_info) in &mut consumer_compensation_table.iter_mut() {
            let mut remove_topic_list = Vec::new();
            let subscription_table = consumer_group_info.get_subscription_table();

            // First collect topics that need to be removed
            for subscription_data in subscription_table.iter() {
                let diff = get_current_millis() as i64 - subscription_data.sub_version;

                if diff > self.subscription_expired_timeout as i64 {
                    remove_topic_list.push(subscription_data.key().clone());
                }
            }

            // Then remove topics and check if the group should be removed
            for topic in remove_topic_list {
                subscription_table.remove(&topic);
                if subscription_table.is_empty() {
                    remove_list.push(group.clone());
                }
            }
        }

        // Finally remove groups
        for group in remove_list {
            consumer_compensation_table.remove(&group);
        }
    }

    pub fn scan_not_active_channel(&self) {
        // Use drain_filter pattern for outer map
        let mut groups_to_remove = Vec::new();

        let mut consumer_table = self.consumer_table.write();
        for (group, consumer_group_info) in consumer_table.iter_mut() {
            let channel_info_table = consumer_group_info.get_channel_info_table();

            // Use drain_filter pattern for inner map
            let mut channels_to_remove = Vec::new();
            for client_channel_info in channel_info_table.iter() {
                let diff = get_current_millis() as i64
                    - client_channel_info.last_update_timestamp() as i64;

                if diff > self.channel_expired_timeout as i64 {
                    warn!(
                        "SCAN: remove expired channel from ConsumerManager consumerTable. \
                         channel={}, consumerGroup={}",
                        client_channel_info.key().channel_id(),
                        group
                    );

                    self.call_consumer_ids_change_listener(
                        ConsumerGroupEvent::ClientUnregister,
                        group,
                        &[
                            client_channel_info.key() as &dyn Any,
                            &consumer_group_info.get_subscribe_topics() as &dyn Any,
                        ],
                    );
                    // Remove the channel from the consumer group info
                    channels_to_remove.push(client_channel_info.key().clone());
                }
            }

            // Remove expired channels
            for channel in channels_to_remove {
                channel_info_table.remove(&channel);
            }

            // If group has no channels, mark for removal
            if channel_info_table.is_empty() {
                warn!(
                    "SCAN: remove expired channel from ConsumerManager consumerTable, all clear, \
                     consumerGroup={}",
                    group
                );
                groups_to_remove.push(group.clone());
            }
        }

        // Remove empty groups
        for group in groups_to_remove {
            consumer_table.remove(&group);
        }

        self.remove_expire_consumer_group_info();
    }

    pub fn do_channel_close_event(&self, _remote_addr: &str, channel: &Channel) -> bool {
        let mut removed = false;
        let mut consumer_table = self.consumer_table.write();
        let mut remove_list = Vec::new();
        for (group, info) in consumer_table.iter_mut() {
            if let Some(client_channel_info) = info.handle_channel_close_event(channel) {
                self.call_consumer_ids_change_listener(
                    ConsumerGroupEvent::ClientUnregister,
                    group,
                    &[
                        &client_channel_info as &dyn Any,
                        &info.get_subscribe_topics() as &dyn Any,
                    ],
                );

                if info.get_channel_info_table().is_empty() {
                    remove_list.push(group.clone());
                }

                if !is_broadcast_mode(info.get_message_model()) {
                    self.call_consumer_ids_change_listener(
                        ConsumerGroupEvent::Change,
                        group,
                        &[&info.get_all_channels() as &dyn Any],
                    );
                }

                removed = true;
            }
        }
        for group in remove_list {
            if consumer_table.remove(&group).is_some() {
                info!(
                    "unregister consumer ok, no any connection, and remove consumer group, {}",
                    group
                );
                self.call_consumer_ids_change_listener(
                    ConsumerGroupEvent::Unregister,
                    group.as_str(),
                    &[],
                );
            }
        }

        removed
    }
}

fn is_broadcast_mode(message_model: MessageModel) -> bool {
    message_model == MessageModel::Broadcasting
}
