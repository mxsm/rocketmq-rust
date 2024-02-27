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

use std::{collections::HashMap, error::Error};

use rocketmq_common::common::{
    config::TopicConfig,
    message::{
        message_batch::MessageExtBatch, message_single::MessageExtBrokerInner, MessageConst,
        MessageTrait,
    },
    sys_flag::message_sys_flag::MessageSysFlag,
};

use crate::{
    base::{message_result::PutMessageResult, message_status_enum::PutMessageStatus},
    config::{message_store_config::MessageStoreConfig, store_path_config_helper},
    hook::put_message_hook::BoxedPutMessageHook,
    log_file::MessageStore,
};

///Using local files to store message data, which is also the default method.
#[derive(Default)]
pub struct LocalFileMessageStore {
    pub message_store_config: MessageStoreConfig,
    pub put_message_hook_list: Vec<BoxedPutMessageHook>,
    pub topic_config_table: Option<HashMap<String, TopicConfig>>,
}

impl MessageStoreConfig {
    pub fn new() -> Self {
        MessageStoreConfig::default()
    }
}

impl MessageStore for LocalFileMessageStore {
    fn load(&mut self) -> bool {
        // let mut reuslt = true;
        //check abort file exists
        let last_exit_ok = self.is_temp_file_exist();

        tracing::info!(
            "last shutdown {}, store path root dir: {}",
            if last_exit_ok {
                "normally"
            } else {
                "abnormally"
            },
            self.message_store_config.store_path_root_dir.clone()
        );
        //load commit log file

        // load consume queue file

        // if compaction is enabled, load compaction file

        // check point and confirm the max offset

        true
    }

    fn start(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    async fn async_put_messages(&self, message_ext_batch: MessageExtBatch) -> PutMessageResult {
        for put_message_hook in self.put_message_hook_list.iter() {
            let _handle_result = put_message_hook.execute_before_put_message(&message_ext_batch);
        }
        PutMessageResult::default()
    }

    fn put_message(&self, _msg: MessageExtBrokerInner) -> PutMessageResult {
        todo!()
    }

    fn get_max_offset_in_queue_with_commit(
        &self,
        _topic: &str,
        _queue_id: i32,
        _committed: bool,
    ) -> i64 {
        todo!()
    }

    async fn async_put_message(&self, msg: MessageExtBrokerInner) -> PutMessageResult {
        /*        for put_message_hook in self.put_message_hook_list.iter() {
            let _handle_result = put_message_hook.execute_before_put_message(&message_ext_batch);
        }*/
        if msg
            .message_ext_inner
            .message_inner
            .properties()
            .contains_key(MessageConst::PROPERTY_INNER_NUM)
            && MessageSysFlag::check(
                msg.message_ext_inner.sys_flag,
                MessageSysFlag::TRANSACTION_PREPARED_TYPE,
            )
        {
            return PutMessageResult::new(PutMessageStatus::MessageIllegal, None, false);
        }

        if MessageSysFlag::check(
            msg.message_ext_inner.sys_flag,
            MessageSysFlag::INNER_BATCH_FLAG,
        ) {
            self.get_topic_config(msg.message_ext_inner.message_inner.topic());
        }
        todo!()
    }

    fn put_messages(&self, _message_ext_batch: MessageExtBatch) -> PutMessageResult {
        todo!()
    }
}

impl LocalFileMessageStore {
    pub fn new(message_store_config: MessageStoreConfig) -> Self {
        LocalFileMessageStore {
            message_store_config,
            put_message_hook_list: vec![],
            topic_config_table: None,
        }
    }
}

// private method impl
impl LocalFileMessageStore {
    fn is_temp_file_exist(&self) -> bool {
        let abort_file_path = store_path_config_helper::get_abort_file(
            &self.message_store_config.store_path_root_dir,
        );
        std::path::Path::new(&abort_file_path).exists()
    }

    pub fn get_topic_config(&self, topic: &str) -> Option<&TopicConfig> {
        self.topic_config_table.as_ref()?.get(topic)
    }
}
