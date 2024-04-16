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

use std::{collections::HashMap, error::Error, sync::Arc};

use rocketmq_common::{
    common::{
        config::TopicConfig,
        message::{message_single::MessageExtBrokerInner, MessageConst},
        sys_flag::message_sys_flag::MessageSysFlag,
    },
    utils::queue_type_utils::QueueTypeUtils,
};
use rocketmq_runtime::RocketMQRuntime;
use tracing::{error, warn};

use crate::{
    base::{message_result::PutMessageResult, message_status_enum::PutMessageStatus},
    config::message_store_config::MessageStoreConfig,
    hook::put_message_hook::BoxedPutMessageHook,
    log_file::{commit_log::CommitLog, MessageStore},
};

///Using local files to store message data, which is also the default method.
#[derive(Default)]
pub struct LocalFileMessageStore {
    message_store_config: Arc<MessageStoreConfig>,
    put_message_hook_list: Vec<BoxedPutMessageHook>,
    topic_config_table: HashMap<String, TopicConfig>,
    message_store_runtime: Option<RocketMQRuntime>,
    commit_log: Arc<CommitLog>,
}

impl LocalFileMessageStore {
    pub fn new(message_store_config: Arc<MessageStoreConfig>) -> Self {
        Self {
            message_store_config: message_store_config.clone(),
            put_message_hook_list: vec![],
            topic_config_table: HashMap::new(),
            message_store_runtime: Some(RocketMQRuntime::new_multi(10, "message-store-thread")),
            commit_log: Arc::new(CommitLog::new(message_store_config)),
        }
    }
}

impl Drop for LocalFileMessageStore {
    fn drop(&mut self) {
        if let Some(runtime) = self.message_store_runtime.take() {
            runtime.shutdown();
        }
    }
}

impl LocalFileMessageStore {
    pub fn get_topic_config(&self, topic: &str) -> Option<TopicConfig> {
        if self.topic_config_table.is_empty() {
            return None;
        }
        self.topic_config_table.get(topic).cloned()
    }
}

impl MessageStore for LocalFileMessageStore {
    fn load(&mut self) -> bool {
        todo!()
    }

    fn start(&mut self) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    async fn put_message(&self, msg: MessageExtBrokerInner) -> PutMessageResult {
        for hook in self.put_message_hook_list.iter() {
            if let Some(result) = hook.execute_before_put_message(&msg.message_ext_inner) {
                return result;
            }
        }

        if msg
            .message_ext_inner
            .properties()
            .contains_key(MessageConst::PROPERTY_INNER_NUM)
            && !MessageSysFlag::check(msg.sys_flag(), MessageSysFlag::INNER_BATCH_FLAG)
        {
            warn!(
                "[BUG]The message had property {} but is not an inner batch",
                MessageConst::PROPERTY_INNER_NUM
            );
            return PutMessageResult::new_default(PutMessageStatus::MessageIllegal);
        }

        if MessageSysFlag::check(msg.sys_flag(), MessageSysFlag::INNER_BATCH_FLAG) {
            let topic_config = self.get_topic_config(msg.topic());
            if !QueueTypeUtils::is_batch_cq(&topic_config) {
                error!("[BUG]The message is an inner batch but cq type is not batch cq");
                return PutMessageResult::new_default(PutMessageStatus::MessageIllegal);
            }
        }
        self.commit_log.put_message(msg).await
    }
}
