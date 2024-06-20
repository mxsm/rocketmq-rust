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
use std::collections::HashSet;
use std::sync::Arc;

use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::client::client_channel_info::ClientChannelInfo;

#[derive(Debug)]
pub struct ConsumerGroupInfo {
    group_name: String,
    subscription_table: Arc<RwLock<HashMap<String, SubscriptionData>>>,
    channel_info_table: Arc<RwLock<HashMap<String, ClientChannelInfo>>>,
    consume_type: Arc<RwLock<ConsumeType>>,
    message_model: Arc<RwLock<MessageModel>>,
    consume_from_where: Arc<RwLock<ConsumeFromWhere>>,

    last_update_timestamp: Arc<Mutex<u64>>,
}

impl ConsumerGroupInfo {
    pub fn new(
        group_name: String,
        consume_type: ConsumeType,
        message_model: MessageModel,
        consume_from_where: ConsumeFromWhere,
    ) -> Self {
        ConsumerGroupInfo {
            group_name,
            subscription_table: Arc::new(RwLock::new(HashMap::new())),
            channel_info_table: Arc::new(RwLock::new(HashMap::new())),
            consume_type: Arc::new(RwLock::new(consume_type)),
            message_model: Arc::new(RwLock::new(message_model)),
            consume_from_where: Arc::new(RwLock::new(consume_from_where)),
            last_update_timestamp: Arc::new(Mutex::new(get_current_millis())),
        }
    }

    pub fn with_group_name(group_name: String) -> Self {
        ConsumerGroupInfo {
            group_name,
            subscription_table: Arc::new(RwLock::new(HashMap::new())),
            channel_info_table: Arc::new(RwLock::new(HashMap::new())),
            consume_type: Arc::new(RwLock::new(ConsumeType::ConsumePassively)),
            message_model: Arc::new(RwLock::new(MessageModel::Clustering)),
            consume_from_where: Arc::new(RwLock::new(ConsumeFromWhere::ConsumeFromLastOffset)),
            last_update_timestamp: Arc::new(Mutex::new(get_current_millis())),
        }
    }

    pub async fn find_channel_by_client_id(&self, client_id: &str) -> Option<ClientChannelInfo> {
        let channel_info_table = self.channel_info_table.read().await;
        for (_, client_channel_info) in channel_info_table.iter() {
            if client_channel_info.client_id() == client_id {
                return Some(client_channel_info.clone());
            }
        }
        None
    }

    pub async fn get_subscription_table(&self) -> Arc<RwLock<HashMap<String, SubscriptionData>>> {
        Arc::clone(&self.subscription_table)
    }

    pub async fn find_channel_by_channel(&self, channel: &str) -> Option<ClientChannelInfo> {
        let channel_info_table = self.channel_info_table.read().await;
        channel_info_table.get(channel).cloned()
    }

    pub async fn get_channel_info_table(&self) -> Arc<RwLock<HashMap<String, ClientChannelInfo>>> {
        Arc::clone(&self.channel_info_table)
    }

    pub async fn get_all_channels(&self) -> Vec<String> {
        let channel_info_table = self.channel_info_table.read().await;
        channel_info_table.keys().cloned().collect()
    }

    pub async fn get_all_client_ids(&self) -> Vec<String> {
        let channel_info_table = self.channel_info_table.read().await;
        channel_info_table
            .values()
            .map(|info| info.client_id().clone())
            .collect()
    }

    pub async fn unregister_channel(&self, client_channel_info: &ClientChannelInfo) -> bool {
        let mut channel_info_table = self.channel_info_table.write().await;
        if channel_info_table
            .remove(client_channel_info.client_id())
            .is_some()
        {
            info!(
                "Unregister a consumer [{}] from consumerGroupInfo {}",
                self.group_name,
                client_channel_info.client_id()
            );
            true
        } else {
            false
        }
    }

    pub async fn handle_channel_close_event(&self, channel: &str) -> Option<ClientChannelInfo> {
        let mut channel_info_table = self.channel_info_table.write().await;
        if let Some(info) = channel_info_table.remove(channel) {
            warn!(
                "NETTY EVENT: remove not active channel [{}] from ConsumerGroupInfo \
                 groupChannelTable, consumer group: {}",
                info.socket_addr(),
                self.group_name
            );
            Some(info)
        } else {
            None
        }
    }

    pub async fn update_channel(
        &self,
        info_new: ClientChannelInfo,
        consume_type: ConsumeType,
        message_model: MessageModel,
        consume_from_where: ConsumeFromWhere,
    ) -> bool {
        let mut updated = false;

        {
            let mut consume_type_lock = self.consume_type.write().await;
            *consume_type_lock = consume_type;
        }

        {
            let mut message_model_lock = self.message_model.write().await;
            *message_model_lock = message_model;
        }

        {
            let mut consume_from_where_lock = self.consume_from_where.write().await;
            *consume_from_where_lock = consume_from_where;
        }

        let mut channel_info_table = self.channel_info_table.write().await;
        if let Some(info_old) = channel_info_table.get_mut(info_new.socket_addr()) {
            if info_old.client_id() != info_new.client_id() {
                error!(
                    "ConsumerGroupInfo: consumer channel exists in broker, but clientId is not \
                     the same one, group={}, old clientChannelInfo={}, new clientChannelInfo={}",
                    self.group_name,
                    info_old.client_id(),
                    info_new.client_id()
                );
                *info_old = info_new;
            }
            info_old.set_last_update_timestamp(get_current_millis() as i64);
        } else {
            channel_info_table.insert(info_new.socket_addr().clone(), info_new.clone());
            info!(
                "New consumer connected, group: {} channel: {}",
                self.group_name,
                info_new.socket_addr()
            );
            updated = true;
        }

        *self.last_update_timestamp.lock().await = get_current_millis();

        updated
    }

    pub async fn update_subscription(&self, sub_list: HashSet<SubscriptionData>) -> bool {
        let mut updated = false;
        let mut topic_set: HashSet<String> = HashSet::new();

        let mut subscription_table = self.subscription_table.write().await;
        for sub in sub_list.iter() {
            if let Some(old) = subscription_table.get(sub.topic.as_str()) {
                if sub.sub_version > old.sub_version {
                    if *self.consume_type.read().await == ConsumeType::ConsumePassively {
                        info!(
                            "Subscription changed, group: {} OLD: {:?} NEW: {:?}",
                            self.group_name, old, sub
                        );
                    }
                    subscription_table.insert(sub.topic.clone(), sub.clone());
                }
            } else {
                subscription_table.insert(sub.topic.clone(), sub.clone());
                info!(
                    "Subscription changed, add new topic, group: {} {}",
                    self.group_name, sub.topic
                );
                updated = true;
            }
            topic_set.insert(sub.topic.clone());
        }

        subscription_table.retain(|old_topic, _| {
            if !topic_set.contains(old_topic) {
                warn!(
                    "Subscription changed, group: {} remove topic {}",
                    self.group_name, old_topic
                );
                updated = true;
                false
            } else {
                true
            }
        });

        *self.last_update_timestamp.lock().await = self.get_last_update_timestamp().await;

        updated
    }

    pub async fn get_subscribe_topics(&self) -> HashSet<String> {
        let subscription_table = self.subscription_table.read().await;
        subscription_table.keys().cloned().collect()
    }

    pub async fn find_subscription_data(&self, topic: &str) -> Option<SubscriptionData> {
        let subscription_table = self.subscription_table.read().await;
        subscription_table.get(topic).cloned()
    }

    pub async fn get_consume_type(&self) -> ConsumeType {
        *self.consume_type.read().await
    }

    pub async fn set_consume_type(&self, consume_type: ConsumeType) {
        let mut consume_type_lock = self.consume_type.write().await;
        *consume_type_lock = consume_type;
    }

    pub async fn get_message_model(&self) -> MessageModel {
        *self.message_model.read().await
    }

    pub async fn set_message_model(&self, message_model: MessageModel) {
        let mut message_model_lock = self.message_model.write().await;
        *message_model_lock = message_model;
    }

    pub fn get_group_name(&self) -> &String {
        &self.group_name
    }

    pub async fn get_last_update_timestamp(&self) -> u64 {
        *self.last_update_timestamp.lock().await
    }

    pub async fn set_last_update_timestamp(&self, timestamp: u64) {
        let mut last_update_timestamp_lock = self.last_update_timestamp.lock().await;
        *last_update_timestamp_lock = timestamp;
    }

    pub async fn get_consume_from_where(&self) -> ConsumeFromWhere {
        *self.consume_from_where.read().await
    }

    pub async fn set_consume_from_where(&self, consume_from_where: ConsumeFromWhere) {
        let mut consume_from_where_lock = self.consume_from_where.write().await;
        *consume_from_where_lock = consume_from_where;
    }
}
