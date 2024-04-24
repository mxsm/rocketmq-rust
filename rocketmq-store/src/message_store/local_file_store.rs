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

use std::{collections::HashMap, error::Error, fs, sync::Arc};

use log::info;
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
    base::{
        allocate_mapped_file_service::AllocateMappedFileService, message_result::PutMessageResult,
        message_status_enum::PutMessageStatus, store_checkpoint::StoreCheckpoint,
    },
    config::message_store_config::MessageStoreConfig,
    hook::put_message_hook::BoxedPutMessageHook,
    index::index_service::IndexService,
    kv::compaction_service::CompactionService,
    log_file::{commit_log::CommitLog, MessageStore},
    store_path_config_helper::{get_abort_file, get_store_checkpoint},
};

///Using local files to store message data, which is also the default method.
pub struct LocalFileMessageStore {
    message_store_config: Arc<MessageStoreConfig>,
    put_message_hook_list: Arc<tokio::sync::Mutex<Vec<BoxedPutMessageHook>>>,
    topic_config_table: HashMap<String, TopicConfig>,
    message_store_runtime: Option<RocketMQRuntime>,
    commit_log: CommitLog,
    compaction_service: CompactionService,
    store_checkpoint: Option<StoreCheckpoint>,
    master_flushed_offset: Arc<parking_lot::Mutex<i64>>,
    index_service: IndexService,
    allocate_mapped_file_service: AllocateMappedFileService,
}

impl Clone for LocalFileMessageStore {
    fn clone(&self) -> Self {
        Self {
            message_store_config: self.message_store_config.clone(),
            put_message_hook_list: self.put_message_hook_list.clone(),
            topic_config_table: self.topic_config_table.clone(),
            message_store_runtime: None,
            commit_log: self.commit_log.clone(),
            compaction_service: self.compaction_service.clone(),
            store_checkpoint: None,
            master_flushed_offset: self.master_flushed_offset.clone(),
            index_service: self.index_service.clone(),
            allocate_mapped_file_service: self.allocate_mapped_file_service.clone(),
        }
    }
}

impl LocalFileMessageStore {
    pub fn new(message_store_config: Arc<MessageStoreConfig>) -> Self {
        Self {
            message_store_config: message_store_config.clone(),
            put_message_hook_list: Arc::new(tokio::sync::Mutex::new(vec![])),
            topic_config_table: HashMap::new(),
            message_store_runtime: Some(RocketMQRuntime::new_multi(10, "message-store-thread")),
            commit_log: CommitLog::new(message_store_config),
            compaction_service: Default::default(),
            store_checkpoint: None,
            master_flushed_offset: Arc::new(parking_lot::Mutex::new(-1)),
            index_service: IndexService {},
            allocate_mapped_file_service: AllocateMappedFileService {},
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

    pub fn is_temp_file_exist(&self) -> bool {
        let file_name = get_abort_file(self.message_store_config.store_path_root_dir.as_str());
        fs::metadata(file_name).is_ok()
    }

    fn recover(&mut self, _last_exit_ok: bool) {}
}

impl MessageStore for LocalFileMessageStore {
    fn load(&mut self) -> bool {
        let last_exit_ok = self.is_temp_file_exist();
        info!(
            "last shutdown {}, store path root dir: {}",
            if last_exit_ok {
                "normally"
            } else {
                "abnormally"
            },
            self.message_store_config.store_path_root_dir
        );
        let mut result = self.commit_log.load();
        if !result {
            return result;
        }

        if self.message_store_config.enable_compaction {
            result |= self.compaction_service.load(last_exit_ok);
            if !result {
                return result;
            }
        }

        if result {
            self.store_checkpoint = Some(StoreCheckpoint::new(get_store_checkpoint(
                self.message_store_config.store_path_root_dir.as_str(),
            )));
            self.master_flushed_offset = Arc::new(parking_lot::Mutex::new(
                self.store_checkpoint
                    .as_ref()
                    .unwrap()
                    .get_master_flushed_offset(),
            ));
            self.set_confirm_offset(
                self.store_checkpoint
                    .as_ref()
                    .unwrap()
                    .get_confirm_phy_offset(),
            );
            result = self.index_service.load(last_exit_ok);
            info!(
                "message store recover end, and the max phy offset = {}",
                self.get_max_phy_offset()
            )
        }

        let max_offset = self.get_max_phy_offset();
        self.set_broker_init_max_offset(max_offset);
        info!("load over, and the max phy offset = {}", max_offset);

        if !result {
            self.allocate_mapped_file_service.shutdown();
        }
        result
    }

    fn start(&mut self) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    fn set_confirm_offset(&mut self, phy_offset: i64) {
        self.commit_log.set_confirm_offset(phy_offset);
    }

    fn get_max_phy_offset(&self) -> i64 {
        -1
    }

    fn set_broker_init_max_offset(&mut self, _broker_init_max_offset: i64) {}

    async fn put_message(&self, msg: MessageExtBrokerInner) -> PutMessageResult {
        let guard = self.put_message_hook_list.lock().await;
        for hook in guard.iter() {
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
