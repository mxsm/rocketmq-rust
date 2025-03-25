/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::hook::put_message_hook::PutMessageHook;

use crate::util::hook_utils::HookUtils;

pub struct BatchCheckBeforePutMessageHook {
    topic_config_table: Arc<parking_lot::Mutex<HashMap<CheetahString, TopicConfig>>>,
}

impl BatchCheckBeforePutMessageHook {
    pub fn new(
        topic_config_table: Arc<parking_lot::Mutex<HashMap<CheetahString, TopicConfig>>>,
    ) -> Self {
        Self { topic_config_table }
    }
}

impl PutMessageHook for BatchCheckBeforePutMessageHook {
    fn hook_name(&self) -> String {
        "batchCheckBeforePutMessage".to_string()
    }

    fn execute_before_put_message(&self, msg: &mut dyn MessageTrait) -> Option<PutMessageResult> {
        HookUtils::check_inner_batch(
            &self.topic_config_table,
            msg.as_any().downcast_ref().unwrap(),
        )
    }
}
