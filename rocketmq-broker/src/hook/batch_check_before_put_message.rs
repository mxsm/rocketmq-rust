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
use dashmap::DashMap;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_store::PutMessageHook;
use rocketmq_store::PutMessageResult;
use tracing::warn;

use crate::util::hook_utils::HookUtils;

pub struct BatchCheckBeforePutMessageHook {
    topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
}

impl BatchCheckBeforePutMessageHook {
    pub fn new(topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>) -> Self {
        Self { topic_config_table }
    }
}

impl PutMessageHook for BatchCheckBeforePutMessageHook {
    fn hook_name(&self) -> &'static str {
        "batchCheckBeforePutMessage"
    }

    fn execute_before_put_message(&self, msg: &mut dyn MessageTrait) -> Option<PutMessageResult> {
        if let Some(msg) = msg.as_any_mut().downcast_mut::<MessageExtBrokerInner>() {
            HookUtils::check_inner_batch(&self.topic_config_table, &msg.message_ext_inner)
        } else {
            // This should not happen, but just in case
            warn!("Message is not of type MessageExtBrokerInner");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_model::common::config::TopicConfig;
    use rocketmq_model::common::message::message_ext::MessageExt;
    use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
    use rocketmq_model::common::message::MessageTrait;
    use rocketmq_model::common::sys_flag::message_sys_flag::MessageSysFlag;
    use rocketmq_store::PutMessageHook;
    use rocketmq_store::PutMessageStatus;

    use super::BatchCheckBeforePutMessageHook;

    #[test]
    fn batch_check_before_put_message_hook_has_stable_identity() {
        let hook = BatchCheckBeforePutMessageHook::new(Arc::new(DashMap::new()));

        assert_eq!(hook.hook_name(), "batchCheckBeforePutMessage");
    }

    #[test]
    fn batch_check_before_put_message_hook_delegates_inner_batch_rejection() {
        let topic_configs = Arc::new(DashMap::new());
        topic_configs.insert(
            CheetahString::from_static_str("orders"),
            Arc::new(TopicConfig::with_queues("orders", 1, 1)),
        );
        let hook = BatchCheckBeforePutMessageHook::new(topic_configs);
        let mut message = MessageExtBrokerInner::default();
        message.set_topic(CheetahString::from_static_str("orders"));
        message.message_ext_inner.set_sys_flag(MessageSysFlag::INNER_BATCH_FLAG);

        let rejection = hook
            .execute_before_put_message(&mut message)
            .expect("inner batch on a simple consume queue should be rejected");
        assert_eq!(rejection.put_message_status(), PutMessageStatus::MessageIllegal);
    }

    #[test]
    fn batch_check_before_put_message_hook_ignores_other_message_types() {
        let hook = BatchCheckBeforePutMessageHook::new(Arc::new(DashMap::new()));
        let mut message = MessageExt::default();

        assert!(hook.execute_before_put_message(&mut message).is_none());
    }
}
