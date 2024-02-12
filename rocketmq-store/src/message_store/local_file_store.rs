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

use crate::{
    config::{message_store_config::MessageStoreConfig, store_path_config_helper},
    log_file::MessageStore,
};

///Using local files to store message data, which is also the default method.
#[derive(Default)]
pub struct LocalFileMessageStore {
    pub message_store_config: MessageStoreConfig,
}

impl MessageStore for LocalFileMessageStore {
    fn load(&mut self) -> bool {
        //check abort file exists
        self.is_temp_file_exist();
        //load commit log file

        // load consume queue file

        // if compaction is enabled, load compaction file

        // check point and confirm the max offset

        true
    }

    fn start(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
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
