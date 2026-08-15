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
use rocketmq_model::common::lite::get_parent_topic;
use rocketmq_model::common::lite::LiteSubscription;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_transport::api::v1::Channel;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct LiteSubscriptionKey {
    client_id: CheetahString,
    group: CheetahString,
    topic: CheetahString,
}

impl LiteSubscriptionKey {
    pub(crate) fn new(client_id: CheetahString, group: CheetahString, topic: CheetahString) -> Self {
        Self {
            client_id,
            group,
            topic,
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct LiteSubscriptionRegistry {
    subscriptions: Arc<DashMap<LiteSubscriptionKey, LiteSubscription>>,
    client_channels: Arc<DashMap<CheetahString, Channel>>,
    wildcard_groups: Arc<DashMap<CheetahString, HashSet<CheetahString>>>,
    wildcard_clients: Arc<DashMap<WildcardGroupKey, HashSet<CheetahString>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WildcardGroupKey {
    topic: CheetahString,
    group: CheetahString,
}

impl WildcardGroupKey {
    fn new(topic: &CheetahString, group: &CheetahString) -> Self {
        Self {
            topic: topic.clone(),
            group: group.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiteSubscriptionRecord {
    pub(crate) client_id: CheetahString,
    pub(crate) group: CheetahString,
    pub(crate) topic: CheetahString,
    pub(crate) lite_topic_set: HashSet<CheetahString>,
    pub(crate) update_time: i64,
    pub(crate) version: i64,
}

impl LiteSubscriptionRegistry {
    pub(crate) fn update_client_channel(&self, client_id: &CheetahString, channel: Channel) {
        self.client_channels.insert(client_id.clone(), channel);
    }

    pub(crate) fn add_partial_subscription(
        &self,
        client_id: &CheetahString,
        group: &CheetahString,
        topic: &CheetahString,
        lmq_name_set: &HashSet<CheetahString>,
    ) -> LiteSubscription {
        let update_time = current_millis() as i64;
        let key = LiteSubscriptionKey::new(client_id.clone(), group.clone(), topic.clone());
        if lmq_name_set.is_empty() {
            return self
                .lite_subscription(client_id, group, topic)
                .unwrap_or_else(|| LiteSubscription::new(group.clone(), topic.clone(), update_time));
        }

        let mut entry = self
            .subscriptions
            .entry(key)
            .or_insert_with(|| LiteSubscription::new(group.clone(), topic.clone(), update_time));
        entry.add_lite_topic_set(lmq_name_set, update_time);
        entry.clone()
    }

    pub(crate) fn remove_partial_subscription(
        &self,
        client_id: &CheetahString,
        group: &CheetahString,
        topic: &CheetahString,
        lmq_name_set: &HashSet<CheetahString>,
    ) -> Option<LiteSubscription> {
        let update_time = current_millis() as i64;
        let key = LiteSubscriptionKey::new(client_id.clone(), group.clone(), topic.clone());
        let mut entry = self.subscriptions.get_mut(&key)?;
        entry.remove_lite_topic_set(lmq_name_set, update_time);
        let snapshot = entry.clone();
        let empty = entry.lite_topic_set().is_empty();
        drop(entry);

        if empty {
            self.subscriptions.remove(&key);
            self.cleanup_client_channel_if_unused(client_id);
            None
        } else {
            Some(snapshot)
        }
    }

    pub(crate) fn add_complete_subscription(
        &self,
        client_id: &CheetahString,
        group: &CheetahString,
        topic: &CheetahString,
        lmq_name_set: &HashSet<CheetahString>,
        version: i64,
        wildcard: bool,
    ) -> LiteSubscription {
        let update_time = current_millis() as i64;
        let key = LiteSubscriptionKey::new(client_id.clone(), group.clone(), topic.clone());
        if let Some(existing) = self.subscriptions.get(&key) {
            if version < existing.version() {
                return existing.clone();
            }
        }

        let mut subscription = LiteSubscription::new(group.clone(), topic.clone(), update_time);
        let effective_topics = if wildcard {
            HashSet::from([wildcard_marker(topic, group)])
        } else {
            lmq_name_set.clone()
        };
        subscription.set_lite_topic_set(effective_topics.clone(), update_time);
        subscription.set_version(version);

        self.set_wildcard_client(client_id, group, topic, wildcard);
        if effective_topics.is_empty() {
            self.subscriptions.remove(&key);
            self.cleanup_client_channel_if_unused(client_id);
        } else {
            self.subscriptions.insert(key, subscription.clone());
        }

        subscription
    }

    pub(crate) fn remove_complete_subscription(&self, client_id: &CheetahString) -> usize {
        let keys = self
            .subscriptions
            .iter()
            .filter(|entry| entry.key().client_id == *client_id)
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        let removed = keys.len();
        for key in keys {
            self.set_wildcard_client(&key.client_id, &key.group, &key.topic, false);
            self.subscriptions.remove(&key);
        }
        self.cleanup_client_channel_if_unused(client_id);
        removed
    }

    pub(crate) fn lite_subscription(
        &self,
        client_id: &CheetahString,
        group: &CheetahString,
        topic: &CheetahString,
    ) -> Option<LiteSubscription> {
        self.subscriptions
            .get(&LiteSubscriptionKey::new(
                client_id.clone(),
                group.clone(),
                topic.clone(),
            ))
            .map(|entry| entry.clone())
    }

    pub(crate) fn active_subscription_num(&self) -> usize {
        self.subscriptions
            .iter()
            .map(|entry| entry.value().lite_topic_set().len())
            .sum()
    }

    pub(crate) fn all_subscriptions(&self) -> Vec<LiteSubscriptionRecord> {
        self.subscriptions
            .iter()
            .map(|entry| LiteSubscriptionRecord {
                client_id: entry.key().client_id.clone(),
                group: entry.key().group.clone(),
                topic: entry.key().topic.clone(),
                lite_topic_set: entry.value().lite_topic_set().clone(),
                update_time: entry.value().update_time(),
                version: entry.value().version(),
            })
            .collect()
    }

    pub(crate) fn is_wildcard_group(&self, topic: &CheetahString, group: &CheetahString) -> bool {
        self.wildcard_groups
            .get(topic)
            .is_some_and(|groups| groups.contains(group))
    }

    pub(crate) fn wildcard_clients(&self, topic: &CheetahString, group: &CheetahString) -> HashSet<CheetahString> {
        self.wildcard_clients
            .get(&WildcardGroupKey::new(topic, group))
            .map(|clients| clients.clone())
            .unwrap_or_default()
    }

    pub(crate) fn subscribers_for_lmq(
        &self,
        lmq_name: &CheetahString,
        requested_group: Option<&CheetahString>,
    ) -> HashSet<(CheetahString, CheetahString)> {
        let Some(parent_topic) = get_parent_topic(lmq_name.as_str()).map(CheetahString::from_string) else {
            return HashSet::new();
        };
        let mut subscribers = self
            .all_subscriptions()
            .into_iter()
            .filter(|subscription| {
                subscription.topic == parent_topic
                    && requested_group.is_none_or(|group| subscription.group == *group)
                    && !self.is_wildcard_group(&subscription.topic, &subscription.group)
                    && subscription.lite_topic_set.contains(lmq_name)
            })
            .map(|subscription| (subscription.client_id, subscription.group))
            .collect::<HashSet<_>>();

        if let Some(group) = requested_group {
            for client_id in self.wildcard_clients(&parent_topic, group) {
                subscribers.insert((client_id, group.clone()));
            }
        } else if let Some(groups) = self.wildcard_groups.get(&parent_topic) {
            for group in groups.iter() {
                for client_id in self.wildcard_clients(&parent_topic, group) {
                    subscribers.insert((client_id, group.clone()));
                }
            }
        }
        subscribers
    }

    fn set_wildcard_client(
        &self,
        client_id: &CheetahString,
        group: &CheetahString,
        topic: &CheetahString,
        wildcard: bool,
    ) {
        let key = WildcardGroupKey::new(topic, group);
        if wildcard {
            self.wildcard_groups
                .entry(topic.clone())
                .or_default()
                .insert(group.clone());
            self.wildcard_clients.entry(key).or_default().insert(client_id.clone());
            return;
        }

        let remove_client_entry = self.wildcard_clients.get_mut(&key).is_some_and(|mut clients| {
            clients.remove(client_id);
            clients.is_empty()
        });
        if remove_client_entry {
            self.wildcard_clients.remove(&key);
            let remove_topic_entry = self.wildcard_groups.get_mut(topic).is_some_and(|mut groups| {
                groups.remove(group);
                groups.is_empty()
            });
            if remove_topic_entry {
                self.wildcard_groups.remove(topic);
            }
        }
    }

    fn cleanup_client_channel_if_unused(&self, client_id: &CheetahString) {
        if self
            .subscriptions
            .iter()
            .all(|entry| entry.key().client_id != *client_id)
        {
            self.client_channels.remove(client_id);
        }
    }
}

fn wildcard_marker(topic: &CheetahString, group: &CheetahString) -> CheetahString {
    CheetahString::from_string(format!("{topic}@{group}"))
}

#[cfg(test)]
mod tests {
    use rocketmq_model::common::lite::to_lmq_name;

    use super::*;

    fn lmq_names(parent_topic: &str, lite_topics: &[&str]) -> HashSet<CheetahString> {
        lite_topics
            .iter()
            .map(|lite_topic| CheetahString::from_string(to_lmq_name(parent_topic, lite_topic).unwrap()))
            .collect()
    }

    #[test]
    fn complete_add_replaces_partial_topics_and_partial_remove_cleans_up_empty_subscription() {
        let registry = LiteSubscriptionRegistry::default();
        let client_id = CheetahString::from_static_str("client-id");
        let group = CheetahString::from_static_str("group-a");
        let topic = CheetahString::from_static_str("parent-topic");

        registry.add_partial_subscription(&client_id, &group, &topic, &lmq_names("parent-topic", &["a", "b"]));
        registry.add_complete_subscription(&client_id, &group, &topic, &lmq_names("parent-topic", &["b"]), 1, false);
        registry.remove_partial_subscription(&client_id, &group, &topic, &lmq_names("parent-topic", &["b"]));

        assert!(registry.lite_subscription(&client_id, &group, &topic).is_none());
        assert_eq!(registry.active_subscription_num(), 0);
    }

    #[test]
    fn active_subscription_num_counts_lite_topic_references() {
        let registry = LiteSubscriptionRegistry::default();
        let client_id_a = CheetahString::from_static_str("client-a");
        let client_id_b = CheetahString::from_static_str("client-b");
        let group = CheetahString::from_static_str("group-a");
        let topic = CheetahString::from_static_str("parent-topic");

        registry.add_complete_subscription(
            &client_id_a,
            &group,
            &topic,
            &lmq_names("parent-topic", &["a", "b"]),
            1,
            false,
        );
        registry.add_complete_subscription(
            &client_id_b,
            &group,
            &topic,
            &lmq_names("parent-topic", &["b"]),
            1,
            false,
        );

        assert_eq!(registry.active_subscription_num(), 3);
    }

    #[test]
    fn complete_add_ignores_stale_version_updates() {
        let registry = LiteSubscriptionRegistry::default();
        let client_id = CheetahString::from_static_str("client-a");
        let group = CheetahString::from_static_str("group-a");
        let topic = CheetahString::from_static_str("parent-topic");

        registry.add_complete_subscription(
            &client_id,
            &group,
            &topic,
            &lmq_names("parent-topic", &["a", "b"]),
            5,
            false,
        );
        let ignored = registry.add_complete_subscription(
            &client_id,
            &group,
            &topic,
            &lmq_names("parent-topic", &["c"]),
            4,
            false,
        );

        assert_eq!(ignored.version(), 5);
        assert_eq!(ignored.lite_topic_set(), &lmq_names("parent-topic", &["a", "b"]));

        let stored = registry
            .lite_subscription(&client_id, &group, &topic)
            .expect("subscription should remain stored");
        assert_eq!(stored.version(), 5);
        assert_eq!(stored.lite_topic_set(), &lmq_names("parent-topic", &["a", "b"]));
    }

    #[test]
    fn wildcard_complete_subscription_uses_synthetic_marker_and_matches_actual_lmq() {
        let registry = LiteSubscriptionRegistry::default();
        let client_id = CheetahString::from_static_str("client-a");
        let group = CheetahString::from_static_str("wildcard-group");
        let topic = CheetahString::from_static_str("parent-topic");
        registry.add_complete_subscription(&client_id, &group, &topic, &HashSet::new(), 1, true);

        assert!(registry.is_wildcard_group(&topic, &group));
        assert_eq!(
            registry.wildcard_clients(&topic, &group),
            HashSet::from([client_id.clone()])
        );
        assert_eq!(
            registry.subscribers_for_lmq(
                &CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("lmq")),
                Some(&group),
            ),
            HashSet::from([(client_id, group)])
        );
    }

    #[test]
    fn removing_wildcard_client_invalidates_group_indexes() {
        let registry = LiteSubscriptionRegistry::default();
        let client_id = CheetahString::from_static_str("client-a");
        let group = CheetahString::from_static_str("wildcard-group");
        let topic = CheetahString::from_static_str("parent-topic");
        registry.add_complete_subscription(&client_id, &group, &topic, &HashSet::new(), 1, true);

        registry.remove_complete_subscription(&client_id);

        assert!(!registry.is_wildcard_group(&topic, &group));
        assert!(registry.wildcard_clients(&topic, &group).is_empty());
    }
}
