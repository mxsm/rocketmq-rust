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

use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::hook::put_message_hook::PutMessageHook;
use rocketmq_store::timer::timer_message_store::TimerMessageStore;
use tracing::warn;

use crate::schedule::schedule_message_service::ScheduleMessageService;
use crate::util::hook_utils::HookUtils;

pub struct ScheduleMessageHook<MS: MessageStore> {
    message_store_config: Arc<MessageStoreConfig>,
    timer_message_store: Option<Arc<TimerMessageStore>>,
    schedule_message_service: Arc<ScheduleMessageService<MS>>,
}

impl<MS: MessageStore> ScheduleMessageHook<MS> {
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        timer_message_store: Option<Arc<TimerMessageStore>>,
        schedule_message_service: Arc<ScheduleMessageService<MS>>,
    ) -> Self {
        Self {
            message_store_config,
            timer_message_store,
            schedule_message_service,
        }
    }
}

impl<MS: MessageStore> PutMessageHook for ScheduleMessageHook<MS> {
    fn hook_name(&self) -> &'static str {
        "ScheduleMessageHook"
    }

    fn execute_before_put_message(&self, msg: &mut dyn MessageTrait) -> Option<PutMessageResult> {
        // Attempt to downcast the generic MessageTrait to MessageExtBrokerInner.
        // This is necessary because the schedule message handling logic requires
        // specific fields and methods available only in MessageExtBrokerInner.
        // If the downcast fails, it means the message is not of the expected type,
        // so we log a warning and return None to indicate that no further processing
        // can be performed for this message.
        if let Some(msg) = msg.as_any_mut().downcast_mut::<MessageExtBrokerInner>() {
            HookUtils::handle_schedule_message(
                self.message_store_config.as_ref(),
                self.timer_message_store.as_deref(),
                self.schedule_message_service.get_max_delay_level(),
                msg,
            )
        } else {
            warn!("Message is not of type MessageExtBrokerInner");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn schedule_hook_source_depends_only_on_explicit_capabilities() {
        let hook_source = include_str!("schedule_message_hook.rs");
        let helper_source = include_str!("../util/hook_utils.rs");
        let forbidden = [
            concat!("rocketmq_", "rust"),
            concat!("Broker", "Runtime", "Inner"),
            concat!("Arc", "Mut"),
        ];

        for source in [hook_source, helper_source] {
            for identifier in forbidden {
                assert!(
                    !source.contains(identifier),
                    "schedule hook boundary contains forbidden dependency {identifier}"
                );
            }
        }
        assert!(hook_source.contains("message_store_config: Arc<MessageStoreConfig>"));
        assert!(hook_source.contains("timer_message_store: Option<Arc<TimerMessageStore>>"));
        assert!(hook_source.contains("schedule_message_service: Arc<ScheduleMessageService<MS>>"));
        assert!(hook_source.contains("self.schedule_message_service.get_max_delay_level()"));
    }
}
