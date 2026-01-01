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

use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::hook::put_message_hook::PutMessageHook;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::util::hook_utils::HookUtils;

pub struct ScheduleMessageHook<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> ScheduleMessageHook<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
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
            HookUtils::handle_schedule_message(&self.broker_runtime_inner, msg)
        } else {
            warn!("Message is not of type MessageExtBrokerInner");
            None
        }
    }
}
