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

use std::collections::HashSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::client::client_channel_info::ClientChannelInfo;

#[derive(Clone)]
pub struct ConsumerGroupInfo {
    group_name: CheetahString,
    subscription_table: Arc<DashMap<CheetahString, SubscriptionData>>,
    channel_info_table: Arc<DashMap<Channel, ClientChannelInfo>>,
    consume_type: ConsumeType,
    message_model: MessageModel,
    consume_from_where: ConsumeFromWhere,
    last_update_timestamp: u64,
}

impl ConsumerGroupInfo {
    pub fn new(
        group_name: impl Into<CheetahString>,
        consume_type: ConsumeType,
        message_model: MessageModel,
        consume_from_where: ConsumeFromWhere,
    ) -> Self {
        ConsumerGroupInfo {
            group_name: group_name.into(),
            subscription_table: Arc::new(DashMap::new()),
            channel_info_table: Arc::new(DashMap::new()),
            consume_type,
            message_model,
            consume_from_where,
            last_update_timestamp: get_current_millis(),
        }
    }

    pub fn with_group_name(group_name: impl Into<CheetahString>) -> Self {
        ConsumerGroupInfo {
            group_name: group_name.into(),
            subscription_table: Arc::new(DashMap::new()),
            channel_info_table: Arc::new(DashMap::new()),
            consume_type: ConsumeType::ConsumePassively,
            message_model: MessageModel::Clustering,
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            last_update_timestamp: get_current_millis(),
        }
    }

    pub fn find_channel_by_client_id(&self, client_id: &str) -> Option<ClientChannelInfo> {
        for client_channel_info in self.channel_info_table.iter() {
            if client_channel_info.client_id() == client_id {
                return Some(client_channel_info.clone());
            }
        }
        None
    }

    pub fn get_subscription_table(&self) -> Arc<DashMap<CheetahString, SubscriptionData>> {
        Arc::clone(&self.subscription_table)
    }

    pub fn find_channel_by_channel(&self, channel: &Channel) -> Option<ClientChannelInfo> {
        self.channel_info_table.get(channel).map(|item| item.value().clone())
    }

    pub fn get_channel_info_table(&self) -> Arc<DashMap<Channel, ClientChannelInfo>> {
        Arc::clone(&self.channel_info_table)
    }

    pub fn get_all_channels(&self) -> Vec<Channel> {
        self.channel_info_table
            .iter()
            .map(|item| item.key().clone())
            .collect::<Vec<Channel>>()
    }

    pub fn get_all_client_ids(&self) -> Vec<CheetahString> {
        self.channel_info_table
            .iter()
            .map(|info| info.value().client_id().clone())
            .collect()
    }

    pub fn unregister_channel(&self, client_channel_info: &ClientChannelInfo) -> bool {
        if self.channel_info_table.remove(client_channel_info.channel()).is_some() {
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

    pub fn handle_channel_close_event(&self, channel: &Channel) -> Option<ClientChannelInfo> {
        if let Some((_, info)) = self.channel_info_table.remove(channel) {
            warn!(
                "Channel close event: remove not active channel [{}] from ConsumerGroupInfo groupChannelTable, \
                 consumer group: {}",
                info.channel().remote_address(),
                self.group_name
            );
            Some(info)
        } else {
            None
        }
    }

    pub fn update_channel(
        &mut self,
        info_new: ClientChannelInfo,
        consume_type: ConsumeType,
        message_model: MessageModel,
        consume_from_where: ConsumeFromWhere,
    ) -> bool {
        let mut updated = false;

        self.consume_type = consume_type;

        self.message_model = message_model;

        self.consume_from_where = consume_from_where;

        if let Some(mut info_old) = self.channel_info_table.get_mut(info_new.channel()) {
            if info_old.client_id() != info_new.client_id() {
                error!(
                    "ConsumerGroupInfo: consumer channel exists in broker, but clientId is not the same one, \
                     group={}, old clientChannelInfo={}, new clientChannelInfo={}",
                    self.group_name,
                    info_old.client_id(),
                    info_new.client_id()
                );
                *info_old = info_new;
            }
            info_old.set_last_update_timestamp(get_current_millis());
        } else {
            self.channel_info_table
                .insert(info_new.channel().clone(), info_new.clone());
            info!(
                "New consumer connected, group: {} channel: {}",
                self.group_name,
                info_new.channel().remote_address()
            );
            updated = true;
        }

        self.last_update_timestamp = get_current_millis();

        updated
    }

    pub fn update_subscription(&mut self, sub_list: &HashSet<SubscriptionData>) -> bool {
        let mut updated = false;
        let mut topic_set = HashSet::new();
        for sub in sub_list.iter() {
            if let Some(old) = self.subscription_table.get(sub.topic.as_str()) {
                if sub.sub_version > old.sub_version {
                    if self.consume_type == ConsumeType::ConsumePassively {
                        info!(
                            "Subscription changed, group: {} OLD: {:?} NEW: {:?}",
                            self.group_name, old, sub
                        );
                    }
                    drop(old); //release lock
                    self.subscription_table.insert(sub.topic.clone(), sub.clone());
                }
            } else {
                self.subscription_table.insert(sub.topic.clone(), sub.clone());
                info!(
                    "Subscription changed, add new topic, group: {} {}",
                    self.group_name, sub.topic
                );
                updated = true;
            }
            topic_set.insert(sub.topic.clone());
        }
        self.subscription_table.retain(|old_topic, _| {
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
        self.last_update_timestamp = get_current_millis();
        updated
    }

    pub fn get_subscribe_topics(&self) -> HashSet<CheetahString> {
        self.subscription_table.iter().map(|item| item.key().clone()).collect()
    }

    pub fn find_subscription_data(&self, topic: &CheetahString) -> Option<SubscriptionData> {
        self.subscription_table.get(topic).map(|item| item.value().clone())
    }

    pub fn get_consume_type(&self) -> ConsumeType {
        self.consume_type
    }

    pub fn set_consume_type(&mut self, consume_type: ConsumeType) {
        self.consume_type = consume_type;
    }

    pub fn get_message_model(&self) -> MessageModel {
        self.message_model
    }

    pub fn set_message_model(&mut self, message_model: MessageModel) {
        self.message_model = message_model;
    }

    pub fn get_group_name(&self) -> &CheetahString {
        &self.group_name
    }

    pub fn get_last_update_timestamp(&self) -> u64 {
        self.last_update_timestamp
    }

    pub fn set_last_update_timestamp(&mut self, timestamp: u64) {
        self.last_update_timestamp = timestamp;
    }

    pub fn get_consume_from_where(&self) -> ConsumeFromWhere {
        self.consume_from_where
    }

    pub fn set_consume_from_where(&mut self, consume_from_where: ConsumeFromWhere) {
        self.consume_from_where = consume_from_where;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
    use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
    use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
    use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;

    use super::*;

    #[test]
    fn consumer_group_info_new() {
        let group_name = "test_group".to_string();
        let consume_type = ConsumeType::ConsumePassively;
        let message_model = MessageModel::Clustering;
        let consume_from_where = ConsumeFromWhere::ConsumeFromLastOffset;

        let consumer_group_info =
            ConsumerGroupInfo::new(group_name.clone(), consume_type, message_model, consume_from_where);

        assert_eq!(consumer_group_info.get_group_name(), &group_name);
        assert_eq!(consumer_group_info.get_consume_type(), consume_type);
        assert_eq!(consumer_group_info.get_message_model(), message_model);
        assert_eq!(consumer_group_info.get_consume_from_where(), consume_from_where);
    }

    #[test]
    fn consumer_group_info_with_group_name() {
        let group_name = "test_group".to_string();

        let consumer_group_info = ConsumerGroupInfo::with_group_name(group_name.clone());

        assert_eq!(consumer_group_info.get_group_name(), &group_name);
        assert_eq!(consumer_group_info.get_consume_type(), ConsumeType::ConsumePassively);
        assert_eq!(consumer_group_info.get_message_model(), MessageModel::Clustering);
        assert_eq!(
            consumer_group_info.get_consume_from_where(),
            ConsumeFromWhere::ConsumeFromLastOffset
        );
    }

    #[test]
    fn consumer_group_info_update_channel() {
        /* let group_name = "test_group".to_string();
        let consume_type = ConsumeType::ConsumePassively;
        let message_model = MessageModel::Clustering;
        let consume_from_where = ConsumeFromWhere::ConsumeFromLastOffset;

        let mut consumer_group_info = ConsumerGroupInfo::new(
            group_name.clone(),
            consume_type,
            message_model,
            consume_from_where,
        );

        let channel = Channel::new(
            "127.0.0.1:8080".parse().unwrap(),
            "192.168.0.1:8080".parse().unwrap(),
        );
        let client_channel_info = ClientChannelInfo::new(
            channel.clone(),
            "client_id".to_string(),
            LanguageCode::RUST,
            1,
        );

        assert!(consumer_group_info.update_channel(
            client_channel_info,
            consume_type,
            message_model,
            consume_from_where
        )); */
    }

    #[test]
    fn consumer_group_info_update_subscription() {
        let group_name = "test_group".to_string();
        let consume_type = ConsumeType::ConsumePassively;
        let message_model = MessageModel::Clustering;
        let consume_from_where = ConsumeFromWhere::ConsumeFromLastOffset;

        let mut consumer_group_info =
            ConsumerGroupInfo::new(group_name.clone(), consume_type, message_model, consume_from_where);

        let mut sub_list = HashSet::new();
        let subscription_data = SubscriptionData {
            topic: "topic".into(),
            sub_string: "sub_string".into(),
            ..Default::default()
        };
        sub_list.insert(subscription_data);

        assert!(consumer_group_info.update_subscription(&sub_list));
    }
}
