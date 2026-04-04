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

use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;

use crate::client::consumer_group_event::ConsumerGroupEvent;
use crate::client::consumer_ids_change_listener::ConsumerIdsChangeListener;
use crate::filter::manager::consumer_filter_manager::ConsumerFilterManager;

#[derive(Clone)]
pub struct DefaultConsumerIdsChangeListener {
    consumer_filter_manager: ConsumerFilterManager,
}

impl DefaultConsumerIdsChangeListener {
    pub fn new(consumer_filter_manager: ConsumerFilterManager) -> Self {
        Self {
            consumer_filter_manager,
        }
    }
}

impl ConsumerIdsChangeListener for DefaultConsumerIdsChangeListener {
    fn handle(&self, event: ConsumerGroupEvent, group: &str, args: &[&dyn Any]) {
        match event {
            ConsumerGroupEvent::Register => {
                let Some(subscriptions) = args
                    .first()
                    .and_then(|argument| argument.downcast_ref::<HashSet<SubscriptionData>>())
                else {
                    return;
                };
                self.consumer_filter_manager.register(group, subscriptions);
            }
            ConsumerGroupEvent::Unregister => {
                self.consumer_filter_manager.unregister(group);
            }
            ConsumerGroupEvent::Change | ConsumerGroupEvent::ClientRegister | ConsumerGroupEvent::ClientUnregister => {}
        }
    }

    fn shutdown(&self) {}
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::filter::expression_type::ExpressionType;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    use super::*;

    #[test]
    fn register_event_populates_consumer_filter_manager() {
        let manager = ConsumerFilterManager::new(
            Arc::new(BrokerConfig::default()),
            Arc::new(MessageStoreConfig::default()),
        );
        let listener = DefaultConsumerIdsChangeListener::new(manager.clone());
        let subscriptions = HashSet::from([SubscriptionData {
            topic: CheetahString::from_slice("TopicTest"),
            sub_string: CheetahString::from_slice("color = 'blue'"),
            expression_type: CheetahString::from_static_str(ExpressionType::SQL92),
            sub_version: 3,
            ..Default::default()
        }]);

        listener.handle(ConsumerGroupEvent::Register, "GroupTest", &[&subscriptions as &dyn Any]);

        assert!(manager
            .get_consumer_filter_data(
                &CheetahString::from_slice("TopicTest"),
                &CheetahString::from_slice("GroupTest"),
            )
            .is_some());

        listener.handle(ConsumerGroupEvent::Unregister, "GroupTest", &[]);
        assert!(manager
            .get_consumer_filter_data(
                &CheetahString::from_slice("TopicTest"),
                &CheetahString::from_slice("GroupTest"),
            )
            .unwrap()
            .is_dead());
    }
}
