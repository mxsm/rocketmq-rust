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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_transport::api::SessionId;
use tracing::info;
use tracing::warn;

use crate::client::client_session_info::ClientSessionInfo;
use crate::client::consumer_ids_change_listener::ConsumerConnectionIdentity;

#[derive(Clone)]
pub struct ConsumerGroupInfo {
    group_name: CheetahString,
    subscription_table: Arc<DashMap<CheetahString, Arc<SubscriptionData>>>,
    session_info_table: Arc<DashMap<SessionId, ClientSessionInfo>>,
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
            session_info_table: Arc::new(DashMap::new()),
            consume_type,
            message_model,
            consume_from_where,
            last_update_timestamp: current_millis(),
        }
    }

    pub fn with_group_name(group_name: impl Into<CheetahString>) -> Self {
        ConsumerGroupInfo {
            group_name: group_name.into(),
            subscription_table: Arc::new(DashMap::new()),
            session_info_table: Arc::new(DashMap::new()),
            consume_type: ConsumeType::ConsumePassively,
            message_model: MessageModel::Clustering,
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            last_update_timestamp: current_millis(),
        }
    }

    /// Returns an owned, weakly consistent snapshot of current subscriptions.
    ///
    /// Concurrent updates may be observed per entry; callers that require one atomic transition
    /// must coordinate that transition through the consumer manager.
    pub fn subscription_snapshot(&self) -> HashMap<CheetahString, Arc<SubscriptionData>> {
        self.subscription_table
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// Returns the number of current subscriptions.
    pub fn subscription_count(&self) -> usize {
        self.subscription_table.len()
    }

    /// Returns whether the group has no current subscriptions.
    pub fn subscriptions_is_empty(&self) -> bool {
        self.subscription_table.is_empty()
    }

    /// Inserts or replaces one subscription through the group boundary.
    pub fn upsert_subscription(&self, subscription: SubscriptionData) -> Option<Arc<SubscriptionData>> {
        self.subscription_table
            .insert(subscription.topic.clone(), Arc::new(subscription))
    }

    /// Removes one subscription through the group boundary.
    pub fn remove_subscription(&self, topic: &str) -> Option<Arc<SubscriptionData>> {
        self.subscription_table
            .remove(topic)
            .map(|(_, subscription)| subscription)
    }

    /// Returns the shared mutable subscription table for legacy integrations.
    ///
    /// Use the owned snapshot, query, and controlled command methods instead. This compatibility
    /// API will be removed in 2.0.0.
    #[deprecated(note = "use subscription_snapshot/query/command methods; removal in 2.0.0")]
    pub fn get_subscription_table(&self) -> Arc<DashMap<CheetahString, Arc<SubscriptionData>>> {
        Arc::clone(&self.subscription_table)
    }

    /// Returns whether the group has no live sessions.
    pub fn channels_is_empty(&self) -> bool {
        self.session_info_table.is_empty()
    }

    pub fn get_all_client_ids(&self) -> Vec<CheetahString> {
        let client_ids = self
            .session_info_table
            .iter()
            .map(|info| info.value().client_id().clone())
            .collect::<HashSet<_>>();
        client_ids.into_iter().collect()
    }

    pub(crate) fn session_client_id(&self, session_id: SessionId) -> Option<CheetahString> {
        self.session_info_table
            .get(&session_id)
            .map(|info| info.client_id().clone())
    }

    pub(crate) fn session_info_snapshot(&self) -> Vec<ClientSessionInfo> {
        self.session_info_table
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub(crate) fn connection_identity_snapshot(&self) -> Vec<ConsumerConnectionIdentity> {
        self.session_info_table
            .iter()
            .map(|entry| ConsumerConnectionIdentity::Session {
                session_id: *entry.key(),
                client_id: entry.client_id().clone(),
            })
            .collect()
    }

    pub(crate) fn update_session(
        &mut self,
        info_new: ClientSessionInfo,
        consume_type: ConsumeType,
        message_model: MessageModel,
        consume_from_where: ConsumeFromWhere,
    ) -> bool {
        self.consume_type = consume_type;
        self.message_model = message_model;
        self.consume_from_where = consume_from_where;

        let is_new = if let Some(mut info_old) = self.session_info_table.get_mut(&info_new.session_id()) {
            info_old.refresh_from(&info_new);
            false
        } else {
            self.session_info_table.insert(info_new.session_id(), info_new);
            true
        };
        self.last_update_timestamp = current_millis();
        is_new
    }

    pub(crate) fn unregister_session(&self, session_id: SessionId) -> Option<ClientSessionInfo> {
        self.session_info_table.remove(&session_id).map(|(_, info)| info)
    }

    pub(crate) fn unregister_session_if_expired(
        &self,
        session_id: SessionId,
        now: u64,
        timeout: u64,
    ) -> Option<ClientSessionInfo> {
        self.session_info_table
            .remove_if(&session_id, |_, info| {
                now.saturating_sub(info.last_update_timestamp()) > timeout
            })
            .map(|(_, info)| info)
    }

    #[cfg(test)]
    pub(crate) fn set_session_last_update_timestamp_for_test(&self, session_id: SessionId, timestamp: u64) {
        if let Some(mut info) = self.session_info_table.get_mut(&session_id) {
            info.set_last_update_timestamp_for_test(timestamp);
        }
    }

    pub(crate) fn remove_expired_sessions(&self, now: u64, timeout: u64) -> Vec<ClientSessionInfo> {
        let expired = self
            .session_info_table
            .iter()
            .filter(|entry| now.saturating_sub(entry.last_update_timestamp()) > timeout)
            .map(|entry| entry.session_id())
            .collect::<Vec<_>>();
        expired
            .into_iter()
            .filter_map(|session_id| {
                self.session_info_table
                    .remove_if(&session_id, |_, info| {
                        now.saturating_sub(info.last_update_timestamp()) > timeout
                    })
                    .map(|(_, info)| info)
            })
            .collect()
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
                    self.subscription_table.insert(sub.topic.clone(), Arc::new(sub.clone()));
                }
            } else {
                self.subscription_table.insert(sub.topic.clone(), Arc::new(sub.clone()));
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
        self.last_update_timestamp = current_millis();
        updated
    }

    pub fn get_subscribe_topics(&self) -> HashSet<CheetahString> {
        self.subscription_table.iter().map(|item| item.key().clone()).collect()
    }

    /// Returns subscription data wrapped in Arc to avoid cloning.
    /// This is the preferred method for high-frequency access.
    pub fn find_subscription_data_arc(&self, topic: &CheetahString) -> Option<Arc<SubscriptionData>> {
        self.subscription_table.get(topic).map(|item| item.value().clone())
    }

    /// Returns cloned subscription data for backward compatibility.
    /// Consider using `find_subscription_data_arc` to avoid cloning overhead.
    pub fn find_subscription_data(&self, topic: &CheetahString) -> Option<SubscriptionData> {
        self.find_subscription_data_arc(topic)
            .map(|arc_data| (*arc_data).clone())
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

    use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;
    use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
    use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
    use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;

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

    #[test]
    fn snapshots_are_owned_and_commands_preserve_table_invariants() {
        let info = ConsumerGroupInfo::with_group_name("test_group");
        let subscription = SubscriptionData {
            topic: "topic".into(),
            ..Default::default()
        };
        info.upsert_subscription(subscription.clone());

        let mut snapshot = info.subscription_snapshot();
        snapshot.clear();
        assert_eq!(info.subscription_count(), 1);
        assert_eq!(info.find_subscription_data(&subscription.topic), Some(subscription));
        assert!(info.remove_subscription("topic").is_some());
        assert!(info.subscriptions_is_empty());
    }
}
