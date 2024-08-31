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

use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::hook::put_message_hook::PutMessageHook;

use crate::util::hook_utils::HookUtils;

pub struct BatchCheckBeforePutMessageHook {
    topic_config_table: Arc<parking_lot::Mutex<HashMap<String, TopicConfig>>>,
}

impl BatchCheckBeforePutMessageHook {
    pub fn new(topic_config_table: Arc<parking_lot::Mutex<HashMap<String, TopicConfig>>>) -> Self {
        Self { topic_config_table }
    }
}

impl PutMessageHook for BatchCheckBeforePutMessageHook {
    fn hook_name(&self) -> String {
        "batchCheckBeforePutMessage".to_string()
    }

    fn execute_before_put_message(&self, msg: &MessageExt) -> Option<PutMessageResult> {
        HookUtils::check_inner_batch(&self.topic_config_table, msg)
    }
}
