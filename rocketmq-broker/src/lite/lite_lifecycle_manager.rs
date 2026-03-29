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

use cheetah_string::CheetahString;
use rocketmq_common::common::lite::get_lite_topic;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::queue::consume_queue_store::ConsumeQueueStoreTrait;
use rocketmq_store::queue::local_file_consume_queue_store::ConsumeQueueStore;

use crate::subscription::lite_subscription_registry::LiteSubscriptionRecord;

#[derive(Clone, Default)]
pub(crate) struct LiteLifecycleManager;

impl LiteLifecycleManager {
    pub(crate) fn get_max_offset_in_queue<MS: MessageStore>(
        &self,
        message_store: Option<&ArcMut<MS>>,
        lmq_name: &CheetahString,
    ) -> i64 {
        let Some(message_store) = message_store else {
            return 0;
        };
        if let Some(queue_store) = message_store.get_queue_store().downcast_ref::<ConsumeQueueStore>() {
            let lmq_offset = queue_store.get_lmq_queue_offset(format!("{lmq_name}-0").as_str());
            if lmq_offset > 0 {
                return lmq_offset;
            }
        }
        message_store.get_max_offset_in_queue(lmq_name, 0)
    }

    pub(crate) fn is_lmq_exist<MS: MessageStore>(
        &self,
        message_store: Option<&ArcMut<MS>>,
        lmq_name: &CheetahString,
    ) -> bool {
        let Some(message_store) = message_store else {
            return false;
        };
        if let Some(queue_store) = message_store.get_queue_store().downcast_ref::<ConsumeQueueStore>() {
            return queue_store.is_lmq_exist(lmq_name.as_str())
                || message_store.get_max_offset_in_queue(lmq_name, 0) > 0;
        }
        message_store.get_max_offset_in_queue(lmq_name, 0) > 0
    }

    pub(crate) fn get_lite_topic_count(
        &self,
        subscriptions: &[LiteSubscriptionRecord],
        parent_topic: &CheetahString,
    ) -> i32 {
        subscriptions
            .iter()
            .filter(|subscription| subscription.topic == *parent_topic)
            .flat_map(|subscription| subscription.lite_topic_set.iter())
            .filter_map(|lmq_name| get_lite_topic(lmq_name.as_str()))
            .collect::<HashSet<_>>()
            .len() as i32
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rocketmq_common::common::lite::to_lmq_name;

    use super::*;

    #[test]
    fn get_lite_topic_count_counts_unique_topics_under_parent() {
        let lifecycle_manager = LiteLifecycleManager;
        let parent_topic = CheetahString::from_static_str("parent-topic");
        let subscriptions = vec![
            LiteSubscriptionRecord {
                client_id: CheetahString::from_static_str("client-a"),
                group: CheetahString::from_static_str("group-a"),
                topic: parent_topic.clone(),
                lite_topic_set: HashSet::from([
                    CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("lmq")),
                    CheetahString::from_string(to_lmq_name("parent-topic", "child-b").expect("lmq")),
                ]),
                update_time: 1,
                version: 0,
            },
            LiteSubscriptionRecord {
                client_id: CheetahString::from_static_str("client-b"),
                group: CheetahString::from_static_str("group-b"),
                topic: parent_topic.clone(),
                lite_topic_set: HashSet::from([CheetahString::from_string(
                    to_lmq_name("parent-topic", "child-b").expect("lmq"),
                )]),
                update_time: 2,
                version: 0,
            },
        ];

        assert_eq!(lifecycle_manager.get_lite_topic_count(&subscriptions, &parent_topic), 2);
    }
}
