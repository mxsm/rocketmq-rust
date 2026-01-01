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
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::hook::put_message_hook::PutMessageHook;
use tracing::warn;

use crate::util::hook_utils::HookUtils;

pub struct BatchCheckBeforePutMessageHook {
    topic_config_table: Arc<DashMap<CheetahString, ArcMut<TopicConfig>>>,
}

impl BatchCheckBeforePutMessageHook {
    pub fn new(topic_config_table: Arc<DashMap<CheetahString, ArcMut<TopicConfig>>>) -> Self {
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
