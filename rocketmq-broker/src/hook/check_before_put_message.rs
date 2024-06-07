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
use std::sync::Arc;

use rocketmq_common::common::message::message_single::MessageExt;
use rocketmq_store::{
    base::message_result::PutMessageResult, config::message_store_config::MessageStoreConfig,
    hook::put_message_hook::PutMessageHook, log_file::MessageStore,
};

use crate::util::hook_utils::HookUtils;

pub struct CheckBeforePutMessageHook<MS> {
    message_store: MS,
    message_store_config: Arc<MessageStoreConfig>,
}

impl<MS: MessageStore> CheckBeforePutMessageHook<MS> {
    pub fn new(message_store: MS, message_store_config: Arc<MessageStoreConfig>) -> Self {
        Self {
            message_store,
            message_store_config,
        }
    }
}

impl<MS: MessageStore> PutMessageHook for CheckBeforePutMessageHook<MS> {
    fn hook_name(&self) -> String {
        "checkBeforePutMessage".to_string()
    }

    fn execute_before_put_message(&self, msg: &MessageExt) -> Option<PutMessageResult> {
        HookUtils::check_before_put_message(&self.message_store, &self.message_store_config, msg)
    }
}
