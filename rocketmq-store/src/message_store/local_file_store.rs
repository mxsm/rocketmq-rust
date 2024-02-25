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

use std::error::Error;

use rocketmq_common::common::{
    future::CompletableFuture,
    message::{message_batch::MessageExtBatch, message_single::MessageExtBrokerInner},
};

use crate::{
    base::message_result::PutMessageResult,
    config::{message_store_config::MessageStoreConfig, store_path_config_helper},
    hook::put_message_hook::BoxedPutMessageHook,
    log_file::MessageStore,
};

///Using local files to store message data, which is also the default method.
#[derive(Default)]
pub struct LocalFileMessageStore {
    pub message_store_config: MessageStoreConfig,
    pub put_message_hook_list: Vec<BoxedPutMessageHook>,
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

    fn async_put_messages(
        &self,
        message_ext_batch: MessageExtBatch,
    ) -> CompletableFuture<PutMessageResult> {
        for put_message_hook in self.put_message_hook_list.iter() {
            let _handle_result = put_message_hook.execute_before_put_message(&message_ext_batch);
        }
        CompletableFuture::new()
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
}

impl LocalFileMessageStore {
    pub fn new(message_store_config: MessageStoreConfig) -> Self {
        LocalFileMessageStore {
            message_store_config,
            put_message_hook_list: vec![],
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
}
