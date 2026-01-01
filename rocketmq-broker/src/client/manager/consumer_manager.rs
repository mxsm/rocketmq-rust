// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Instant;

use cheetah_string::CheetahString;
use dashmap::DashMap;
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

/// Manages consumer client connections and their lifecycle.
///
/// This manager maintains:
/// - Consumer group registrations and subscription relationships
/// - Channel heartbeat status and expiration detection
/// - Automatic cleanup of inactive consumers
///
/// # Thread Safety
/// All operations are thread-safe using concurrent data structures.
pub struct ConsumerManager {
    /// Consumer group name -> ConsumerGroupInfo mapping
    consumer_table: Arc<DashMap<CheetahString, ConsumerGroupInfo>>,
    /// Compensation table for consumers without heartbeat
    consumer_compensation_table: Arc<DashMap<CheetahString, ConsumerGroupInfo>>,
    /// Listeners notified on consumer registration/unregistration events
    consumer_ids_change_listener_list: Vec<Arc<Box<dyn ConsumerIdsChangeListener + Send + Sync + 'static>>>,
    /// Optional broker statistics manager (set once during initialization)
    broker_stats_manager: Option<Weak<BrokerStatsManager>>,
    /// Timeout for considering a consumer channel as expired (in milliseconds)
    channel_expired_timeout: u64,
    /// Timeout for subscription data expiration (in milliseconds)
    subscription_expired_timeout: u64,
}

impl ConsumerManager {
    /// Creates a new ConsumerManager instance.
    ///
    /// # Arguments
    /// * `consumer_ids_change_listener` - Listener for consumer change events
    /// * `expired_timeout` - Timeout for channel and subscription expiration (milliseconds)
    pub fn new(
        consumer_ids_change_listener: Arc<Box<dyn ConsumerIdsChangeListener + Send + Sync + 'static>>,
        expired_timeout: u64,
    ) -> Self {
        let consumer_ids_change_listener_list = vec![consumer_ids_change_listener];
        ConsumerManager {
            consumer_table: Arc::new(DashMap::new()),
            consumer_compensation_table: Arc::new(DashMap::new()),
            consumer_ids_change_listener_list,
            broker_stats_manager: None,
            channel_expired_timeout: expired_timeout,
            subscription_expired_timeout: expired_timeout,
        }
    }

    /// Creates a new ConsumerManager with broker configuration.
    ///
    /// # Arguments
    /// * `consumer_ids_change_listener` - Listener for consumer change events
    /// * `broker_config` - Broker configuration containing timeout settings
    pub fn new_with_broker_stats(
        consumer_ids_change_listener: Arc<Box<dyn ConsumerIdsChangeListener + Send + Sync + 'static>>,
        broker_config: Arc<BrokerConfig>,
    ) -> Self {
        let consumer_ids_change_listener_list = vec![consumer_ids_change_listener];
        ConsumerManager {
            consumer_table: Arc::new(DashMap::new()),
            consumer_compensation_table: Arc::new(DashMap::new()),
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

    /// Finds a consumer channel by client ID within a consumer group.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `client_id` - Client identifier
    ///
    /// # Returns
    /// Client channel info if found
    pub fn find_channel_by_client_id(&self, group: &str, client_id: &str) -> Option<ClientChannelInfo> {
        if let Some(consumer_group_info) = self.consumer_table.get(group) {
            return consumer_group_info.find_channel_by_client_id(client_id);
        }
        None
    }

    /// Finds a consumer channel by channel reference within a consumer group.
    ///
    /// # Arguments  
    /// * `group` - Consumer group name
    /// * `channel` - Channel reference
    ///
    /// # Returns
    /// Client channel info if found
    pub fn find_channel_by_channel(&self, group: &str, channel: &Channel) -> Option<ClientChannelInfo> {
        if let Some(consumer_group_info) = self.consumer_table.get(group) {
            return consumer_group_info.find_channel_by_channel(channel);
        }
        None
    }

    /// Finds subscription data for a topic within a consumer group.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `topic` - Topic name
    ///
    /// # Returns
    /// Subscription data if found
    pub fn find_subscription_data(&self, group: &CheetahString, topic: &CheetahString) -> Option<SubscriptionData> {
        self.find_subscription_data_internal(group, topic, true)
    }

    /// Finds subscription data for a topic within a consumer group.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `topic` - Topic name
    /// * `from_compensation_table` - Whether to check compensation table
    ///
    /// # Returns
    /// Subscription data if found
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
            if let Some(consumer_group_info) = self.consumer_compensation_table.get(group) {
                return consumer_group_info.find_subscription_data(topic);
            }
        }
        None
    }

    /// Counts the number of subscription data entries for a consumer group.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    ///
    /// # Returns
    /// Number of subscriptions
    pub fn find_subscription_data_count(&self, group: &CheetahString) -> usize {
        if let Some(consumer_group_info) = self.get_consumer_group_info(group) {
            return consumer_group_info.get_subscription_table().len();
        }
        0
    }

    /// Gets consumer group info for a specific group.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    ///
    /// # Returns
    /// Consumer group info if found
    pub fn get_consumer_group_info(&self, group: &CheetahString) -> Option<ConsumerGroupInfo> {
        self.get_consumer_group_info_internal(group, false)
    }

    /// Gets consumer group info with optional compensation table fallback.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `from_compensation_table` - Whether to check compensation table
    ///
    /// # Returns
    /// Consumer group info if found
    pub fn get_consumer_group_info_internal(
        &self,
        group: &CheetahString,
        from_compensation_table: bool,
    ) -> Option<ConsumerGroupInfo> {
        if let Some(consumer_group_info) = self.consumer_table.get(group) {
            return Some(consumer_group_info.clone());
        }
        if from_compensation_table {
            if let Some(consumer_group_info) = self.consumer_compensation_table.get(group) {
                return Some(consumer_group_info.clone());
            }
        }
        None
    }

    /// Compensates subscription data for consumers without heartbeat.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `topic` - Topic name
    /// * `subscription_data` - Subscription data to compensate
    pub fn compensate_subscribe_data(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        subscription_data: &SubscriptionData,
    ) {
        let consumer_group_info = self
            .consumer_compensation_table
            .entry(group.clone())
            .or_insert_with(|| ConsumerGroupInfo::with_group_name(group.clone()));
        consumer_group_info
            .get_subscription_table()
            .insert(topic.into(), subscription_data.clone());
    }

    /// Compensates basic consumer info (consume type and message model).
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `consume_type` - Consume type (push/pull)
    /// * `message_model` - Message model (clustering/broadcasting)
    pub fn compensate_basic_consumer_info(
        &self,
        group: &CheetahString,
        consume_type: ConsumeType,
        message_model: MessageModel,
    ) {
        let mut consumer_group_info = self
            .consumer_compensation_table
            .entry(group.clone())
            .or_insert_with(|| ConsumerGroupInfo::with_group_name(group.clone()));
        consumer_group_info.set_consume_type(consume_type);
        consumer_group_info.set_message_model(message_model);
    }

    /// Registers a consumer in a consumer group.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `client_channel_info` - Client channel information
    /// * `consume_type` - Consume type (push/pull)
    /// * `message_model` - Message model (clustering/broadcasting)
    /// * `consume_from_where` - Where to start consuming
    /// * `sub_list` - Set of subscription data
    /// * `is_notify_consumer_ids_changed_enable` - Whether to notify listeners
    ///
    /// # Returns
    /// `true` if registration changed consumer state
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

    /// Registers a consumer with extended options.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `client_channel_info` - Client channel information
    /// * `consume_type` - Consume type (push/pull)
    /// * `message_model` - Message model (clustering/broadcasting)
    /// * `consume_from_where` - Where to start consuming
    /// * `sub_list` - Set of subscription data
    /// * `is_notify_consumer_ids_changed_enable` - Whether to notify listeners
    /// * `update_subscription` - Whether to update subscription data
    ///
    /// # Returns
    /// `true` if registration changed consumer state
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
        let mut consumer_group_info = self
            .consumer_table
            .entry(group.clone())
            .or_insert_with(|| ConsumerGroupInfo::new(group.clone(), consume_type, message_model, consume_from_where));
        let r1 = consumer_group_info.update_channel(
            client_channel_info.clone(),
            consume_type,
            message_model,
            consume_from_where,
        );

        if r1 {
            let topics: HashSet<CheetahString> = sub_list.iter().map(|item| item.topic.clone()).collect();
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
            self.call_consumer_ids_change_listener(ConsumerGroupEvent::Change, group, &[&all_channel as &dyn Any]);
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

    /// Registers a consumer without subscription data.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `client_channel_info` - Client channel information
    /// * `consume_type` - Consume type (push/pull)
    /// * `message_model` - Message model (clustering/broadcasting)
    /// * `consume_from_where` - Where to start consuming
    /// * `is_notify_consumer_ids_changed_enable` - Whether to notify listeners
    ///
    /// # Returns
    /// `true` if registration changed consumer state
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
        let mut consumer_group_info = self
            .consumer_table
            .entry(group.clone())
            .or_insert_with(|| ConsumerGroupInfo::new(group.clone(), consume_type, message_model, consume_from_where));
        let r1 =
            consumer_group_info.update_channel(client_channel_info, consume_type, message_model, consume_from_where);

        if r1 && is_notify_consumer_ids_changed_enable && !is_broadcast_mode(consumer_group_info.get_message_model()) {
            let channels = consumer_group_info.get_all_channels();
            self.call_consumer_ids_change_listener(ConsumerGroupEvent::Change, group, &[&channels as &dyn Any]);
        }

        if let Some(broker_stats_manager) = self.broker_stats_manager.as_ref() {
            if let Some(broker_stats_manager) = broker_stats_manager.upgrade() {
                broker_stats_manager.inc_consumer_register_time(start.elapsed().as_millis() as i32);
            }
        }
        r1
    }

    /// Notifies all registered consumer IDs change listeners of an event.
    ///
    /// # Arguments
    /// * `event` - The type of event
    /// * `group` - The affected consumer group
    /// * `args` - Additional event arguments
    pub fn call_consumer_ids_change_listener(&self, event: ConsumerGroupEvent, group: &str, args: &[&dyn Any]) {
        for listener in self.consumer_ids_change_listener_list.iter() {
            listener.handle(event, group, args);
        }
    }

    /// Queries which consumer groups are consuming a topic.
    ///
    /// # Arguments
    /// * `topic` - Topic name
    ///
    /// # Returns
    /// Set of consumer group names consuming this topic
    pub fn query_topic_consume_by_who(&self, topic: &CheetahString) -> HashSet<CheetahString> {
        let mut groups = HashSet::new();
        for entry in self.consumer_table.iter() {
            let (group, consumer_group_info) = (entry.key(), entry.value());
            if consumer_group_info.find_subscription_data(topic).is_some() {
                groups.insert(group.clone());
            }
        }
        groups
    }

    /// Unregisters a consumer from a consumer group.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `client_channel_info` - Client channel information
    /// * `is_notify_consumer_ids_changed_enable` - Whether to notify listeners
    pub fn unregister_consumer(
        &self,
        group: &str,
        client_channel_info: &ClientChannelInfo,
        is_notify_consumer_ids_changed_enable: bool,
    ) {
        let consumer_group_info = self.consumer_table.get_mut(group);
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
        if consumer_group_info.get_channel_info_table().is_empty() {
            drop(consumer_group_info); // Release the reference
            if self.consumer_table.remove(group).is_some() {
                info!(
                    "unregister consumer ok, no any connection, and remove consumer group, {}",
                    group
                );

                self.call_consumer_ids_change_listener(ConsumerGroupEvent::Unregister, group, &[]);
            }
        }

        if is_notify_consumer_ids_changed_enable && !is_broadcast_mode(message_model) {
            self.call_consumer_ids_change_listener(ConsumerGroupEvent::Change, group, &[&channels as &dyn Any]);
        }
    }

    /// Removes expired consumer group info from compensation table.
    ///
    /// Cleans up subscriptions that have not been updated for longer than
    /// subscription_expired_timeout.
    pub fn remove_expire_consumer_group_info(&self) {
        let mut groups_to_remove = Vec::new();

        for mut entry in self.consumer_compensation_table.iter_mut() {
            let group = entry.key().clone();
            let consumer_group_info = entry.value_mut();
            let mut topics_to_remove = Vec::new();
            let subscription_table = consumer_group_info.get_subscription_table();

            // Find expired subscriptions
            for subscription_data in subscription_table.iter() {
                let diff = get_current_millis() as i64 - subscription_data.sub_version;
                if diff > self.subscription_expired_timeout as i64 {
                    topics_to_remove.push(subscription_data.key().clone());
                }
            }

            // Remove expired subscriptions
            for topic in topics_to_remove {
                subscription_table.remove(&topic);
                if subscription_table.is_empty() {
                    groups_to_remove.push(group.clone());
                }
            }
        }

        // Remove empty groups
        for group in groups_to_remove {
            self.consumer_compensation_table.remove(&group);
        }
    }

    /// Scans and removes inactive consumer channels that have exceeded the timeout.
    ///
    /// This method should be called periodically to clean up expired consumers.
    ///
    /// # Timeout
    /// Consumers that haven't sent heartbeat for more than channel_expired_timeout
    /// will be removed.
    pub fn scan_not_active_channel(&self) {
        let mut groups_to_remove = Vec::new();

        for mut entry in self.consumer_table.iter_mut() {
            let group = entry.key().clone();
            let consumer_group_info = entry.value_mut();
            let channel_info_table = consumer_group_info.get_channel_info_table();

            // Collect expired channels
            let mut channels_to_remove = Vec::new();
            for client_channel_info in channel_info_table.iter() {
                let diff = get_current_millis() as i64 - client_channel_info.last_update_timestamp() as i64;

                if diff > self.channel_expired_timeout as i64 {
                    warn!(
                        "SCAN: remove expired channel from ConsumerManager consumerTable. channel={}, consumerGroup={}",
                        client_channel_info.key().channel_id(),
                        group
                    );

                    self.call_consumer_ids_change_listener(
                        ConsumerGroupEvent::ClientUnregister,
                        &group,
                        &[
                            client_channel_info.key() as &dyn Any,
                            &consumer_group_info.get_subscribe_topics() as &dyn Any,
                        ],
                    );
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
                    "SCAN: remove expired channel from ConsumerManager consumerTable, all clear, consumerGroup={}",
                    group
                );
                groups_to_remove.push(group.clone());
            } else if !is_broadcast_mode(consumer_group_info.get_message_model()) {
                // Notify remaining channels about the change
                self.call_consumer_ids_change_listener(
                    ConsumerGroupEvent::Change,
                    &group,
                    &[&consumer_group_info.get_all_channels() as &dyn Any],
                );
            }
        }

        // Remove empty groups
        for group in groups_to_remove {
            if self.consumer_table.remove(&group).is_some() {
                self.call_consumer_ids_change_listener(ConsumerGroupEvent::Unregister, group.as_str(), &[]);
            }
        }

        self.remove_expire_consumer_group_info();
    }

    /// Handles channel close events and removes the associated consumer.
    ///
    /// This method is typically called by the connection layer when a channel is closed.
    ///
    /// # Arguments
    /// * `_remote_addr` - The remote address of the closed channel
    /// * `channel` - The closed channel
    ///
    /// # Returns
    /// `true` if at least one consumer was removed
    pub fn do_channel_close_event(&self, _remote_addr: &str, channel: &Channel) -> bool {
        let mut removed = false;
        let mut remove_list = Vec::new();

        for mut entry in self.consumer_table.iter_mut() {
            let group = entry.key().clone();
            let info = entry.value_mut();
            if let Some(client_channel_info) = info.handle_channel_close_event(channel) {
                self.call_consumer_ids_change_listener(
                    ConsumerGroupEvent::ClientUnregister,
                    &group,
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
                        &group,
                        &[&info.get_all_channels() as &dyn Any],
                    );
                }

                removed = true;
            }
        }

        for group in remove_list {
            if self.consumer_table.remove(&group).is_some() {
                info!(
                    "unregister consumer ok, no any connection, and remove consumer group, {}",
                    group
                );
                self.call_consumer_ids_change_listener(ConsumerGroupEvent::Unregister, group.as_str(), &[]);
            }
        }

        removed
    }
}

/// Checks if the message model is broadcasting mode.
///
/// # Arguments
/// * `message_model` - Message model to check
///
/// # Returns
/// `true` if broadcasting mode
fn is_broadcast_mode(message_model: MessageModel) -> bool {
    message_model == MessageModel::Broadcasting
}
