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

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::statistics::state_getter::StateGetter;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;

use crate::client::manager::consumer_manager::ConsumerManager;
use crate::client::manager::producer_manager::ProducerManager;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

pub(crate) struct ProducerStateGetter {
    topic_config_manager: Arc<TopicConfigManager>,
    producer_manager: ProducerManager,
}

impl ProducerStateGetter {
    pub(crate) fn new(topic_config_manager: Arc<TopicConfigManager>, producer_manager: ProducerManager) -> Self {
        Self {
            topic_config_manager,
            producer_manager,
        }
    }
}

impl StateGetter for ProducerStateGetter {
    fn online(&self, instance_id: &CheetahString, group: &CheetahString, topic: &CheetahString) -> bool {
        if self
            .topic_config_manager
            .topic_config_table()
            .contains_key(NamespaceUtil::wrap_namespace(instance_id, topic).as_str())
        {
            self.producer_manager
                .group_online(&NamespaceUtil::wrap_namespace(instance_id, group))
        } else {
            self.producer_manager.group_online(group)
        }
    }
}

pub(crate) struct ConsumerStateGetter {
    topic_config_manager: Arc<TopicConfigManager>,
    consumer_manager: ConsumerManager,
}

impl ConsumerStateGetter {
    pub(crate) fn new(topic_config_manager: Arc<TopicConfigManager>, consumer_manager: ConsumerManager) -> Self {
        Self {
            topic_config_manager,
            consumer_manager,
        }
    }
}

impl StateGetter for ConsumerStateGetter {
    fn online(&self, instance_id: &CheetahString, group: &CheetahString, topic: &CheetahString) -> bool {
        if self.topic_config_manager.topic_config_table().contains_key(topic) {
            let topic_full_name = NamespaceUtil::wrap_namespace(instance_id, topic);
            self.consumer_manager
                .find_subscription_data(&NamespaceUtil::wrap_namespace(instance_id, group), &topic_full_name)
                .is_some()
        } else {
            self.consumer_manager.find_subscription_data(group, topic).is_some()
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn state_observers_do_not_retain_the_broker_root() {
        let source = include_str!("broker_state_observer.rs");

        assert!(!source.contains(&["Arc", "Mut"].concat()));
        assert!(!source.contains(&["BrokerRuntime", "Inner"].concat()));
    }
}
