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
#![allow(unused_variables)]

use std::{collections::HashMap, error::Error, fs, sync::Arc, time::Instant};

use log::info;
use rocketmq_common::{
    common::{
        broker::broker_config::BrokerConfig,
        config::TopicConfig,
        message::{message_single::MessageExtBrokerInner, MessageConst},
        sys_flag::message_sys_flag::MessageSysFlag,
    },
    utils::queue_type_utils::QueueTypeUtils,
};
use tokio::sync::Mutex;
use tracing::{error, warn};

use crate::{
    base::{
        allocate_mapped_file_service::AllocateMappedFileService,
        commit_log_dispatcher::CommitLogDispatcher, dispatch_request::DispatchRequest,
        message_result::PutMessageResult, message_status_enum::PutMessageStatus,
        store_checkpoint::StoreCheckpoint,
    },
    config::message_store_config::MessageStoreConfig,
    hook::put_message_hook::BoxedPutMessageHook,
    index::{index_dispatch::CommitLogDispatcherBuildIndex, index_service::IndexService},
    kv::compaction_service::CompactionService,
    log_file::{commit_log::CommitLog, MessageStore},
    queue::{
        build_consume_queue::CommitLogDispatcherBuildConsumeQueue,
        consume_queue::ConsumeQueueStoreTrait,
        local_file_consume_queue_store::{LocalFileConsumeQueue, LocalFileConsumeQueueStore},
    },
    store_path_config_helper::{get_abort_file, get_store_checkpoint},
};

///Using local files to store message data, which is also the default method.
pub struct LocalFileMessageStore {
    message_store_config: Arc<MessageStoreConfig>,
    broker_config: Arc<BrokerConfig>,
    put_message_hook_list: Arc<Vec<BoxedPutMessageHook>>,
    topic_config_table: Arc<Mutex<HashMap<String, TopicConfig>>>,
    //message_store_runtime: Option<RocketMQRuntime>,
    commit_log: CommitLog,
    compaction_service: CompactionService,
    store_checkpoint: Option<StoreCheckpoint>,
    master_flushed_offset: Arc<parking_lot::Mutex<i64>>,
    index_service: Arc<Mutex<IndexService>>,
    allocate_mapped_file_service: AllocateMappedFileService,
    consume_queue_store: LocalFileConsumeQueueStore<LocalFileConsumeQueue>,
    dispatcher: CommitLogDispatcherDefault,
}

impl Clone for LocalFileMessageStore {
    fn clone(&self) -> Self {
        Self {
            message_store_config: self.message_store_config.clone(),
            broker_config: self.broker_config.clone(),
            put_message_hook_list: self.put_message_hook_list.clone(),
            topic_config_table: self.topic_config_table.clone(),
            commit_log: self.commit_log.clone(),
            compaction_service: self.compaction_service.clone(),
            store_checkpoint: self.store_checkpoint.clone(),
            master_flushed_offset: self.master_flushed_offset.clone(),
            index_service: self.index_service.clone(),
            allocate_mapped_file_service: self.allocate_mapped_file_service.clone(),
            consume_queue_store: self.consume_queue_store.clone(),
            dispatcher: self.dispatcher.clone(),
        }
    }
}

impl LocalFileMessageStore {
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        broker_config: Arc<BrokerConfig>,
    ) -> Self {
        let index_service = Arc::new(Mutex::new(IndexService {}));
        let build_index =
            CommitLogDispatcherBuildIndex::new(index_service.clone(), message_store_config.clone());

        let consume_queue_store = Arc::new(Mutex::new(LocalFileConsumeQueueStore::new(
            message_store_config.clone(),
        )));
        let build_consume_queue =
            CommitLogDispatcherBuildConsumeQueue::new(consume_queue_store.clone());
        let dispatcher = CommitLogDispatcherDefault {
            build_index,
            build_consume_queue,
        };
        let store_checkpoint = StoreCheckpoint {};
        let commit_log = CommitLog::new(
            message_store_config.clone(),
            broker_config.clone(),
            &dispatcher,
            store_checkpoint.clone(),
        );
        Self {
            message_store_config: message_store_config.clone(),
            broker_config,
            put_message_hook_list: Arc::new(vec![]),
            topic_config_table: Arc::new(Mutex::new(HashMap::new())),
            // message_store_runtime: Some(RocketMQRuntime::new_multi(10, "message-store-thread")),
            commit_log: commit_log.clone(),
            compaction_service: Default::default(),
            store_checkpoint: Some(store_checkpoint),
            master_flushed_offset: Arc::new(parking_lot::Mutex::new(-1)),
            index_service,
            allocate_mapped_file_service: AllocateMappedFileService {},
            consume_queue_store: LocalFileConsumeQueueStore::new(message_store_config.clone()),
            dispatcher,
        }
    }
}

impl Drop for LocalFileMessageStore {
    fn drop(&mut self) {
        // if let Some(runtime) = self.message_store_runtime.take() {
        //     runtime.shutdown();
        // }
    }
}

impl LocalFileMessageStore {
    pub async fn get_topic_config(&self, topic: &str) -> Option<TopicConfig> {
        if self.topic_config_table.lock().await.is_empty() {
            return None;
        }
        self.topic_config_table.lock().await.get(topic).cloned()
    }

    pub fn is_temp_file_exist(&self) -> bool {
        let file_name = get_abort_file(self.message_store_config.store_path_root_dir.as_str());
        fs::metadata(file_name).is_ok()
    }

    async fn recover(&mut self, last_exit_ok: bool) {
        let recover_concurrently = self.is_recover_concurrently();
        info!(
            "message store recover mode: {}",
            if recover_concurrently {
                "concurrent"
            } else {
                "normal"
            },
        );
        let recover_consume_queue_start = Instant::now();
        self.recover_consume_queue().await;
        let max_phy_offset_of_consume_queue = self
            .consume_queue_store
            .get_max_phy_offset_in_consume_queue();
        let recover_consume_queue = Instant::now()
            .saturating_duration_since(recover_consume_queue_start)
            .as_millis();

        let recover_commit_log_start = Instant::now();
        if last_exit_ok {
            self.recover_normally(max_phy_offset_of_consume_queue).await;
        } else {
            self.recover_abnormally(max_phy_offset_of_consume_queue);
        }
        let recover_commit_log = Instant::now()
            .saturating_duration_since(recover_commit_log_start)
            .as_millis();

        let recover_topic_queue_table_start = Instant::now();
        self.recover_topic_queue_table();
        let recover_topic_queue_table = Instant::now()
            .saturating_duration_since(recover_topic_queue_table_start)
            .as_millis();
        info!(
            "message store recover total cost: {} ms, recoverConsumeQueue: {} ms, \
             recoverCommitLog: {} ms, recoverOffsetTable: {} ms",
            recover_consume_queue + recover_commit_log + recover_topic_queue_table,
            recover_consume_queue,
            recover_commit_log,
            recover_topic_queue_table
        );
    }

    pub fn recover_topic_queue_table(&mut self) {}

    pub async fn recover_normally(&mut self, max_phy_offset_of_consume_queue: i64) {
        self.commit_log
            .recover_normally(max_phy_offset_of_consume_queue)
            .await;
    }

    pub fn recover_abnormally(&mut self, max_phy_offset_of_consume_queue: i64) {}

    fn is_recover_concurrently(&self) -> bool {
        self.broker_config.recover_concurrently
            & self.message_store_config.is_enable_rocksdb_store()
    }

    async fn recover_consume_queue(&mut self) {
        if self.is_recover_concurrently() {
            self.consume_queue_store.recover_concurrently();
        } else {
            self.consume_queue_store.recover();
        }
    }

    pub async fn on_commit_log_dispatch(
        &mut self,
        dispatch_request: &DispatchRequest,
        do_dispatch: bool,
        is_recover: bool,
        _is_file_end: bool,
    ) {
        if do_dispatch && !is_recover {
            self.do_dispatch(dispatch_request).await;
        }
    }

    pub async fn do_dispatch(&mut self, dispatch_request: &DispatchRequest) {
        self.dispatcher.dispatch(dispatch_request).await
    }

    pub fn truncate_dirty_logic_files(&mut self, phy_offset: i64) {}

    pub fn get_queue_store_mut(
        &mut self,
    ) -> &mut LocalFileConsumeQueueStore<LocalFileConsumeQueue> {
        &mut self.consume_queue_store
    }
}

impl MessageStore for LocalFileMessageStore {
    async fn load(&mut self) -> bool {
        let last_exit_ok = !self.is_temp_file_exist();
        info!(
            "last shutdown {}, store path root dir: {}",
            if last_exit_ok {
                "normally"
            } else {
                "abnormally"
            },
            self.message_store_config.store_path_root_dir
        );
        /*let handle = Handle::current();
        let commitlog_inner = self.commit_log.clone();
        let mut result = std::thread::spawn(move || {
            handle.block_on(async move { commitlog_inner.lock().await.load() })
        })
        .join()
        .unwrap_or_default();*/
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
            result = self.index_service.lock().await.load(last_exit_ok);
            self.recover(last_exit_ok).await;
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

        // self.commit_log.set_confirm_offset(phy_offset);
    }

    fn get_max_phy_offset(&self) -> i64 {
        -1
    }

    fn set_broker_init_max_offset(&mut self, _broker_init_max_offset: i64) {}

    async fn put_message(&mut self, msg: MessageExtBrokerInner) -> PutMessageResult {
        // let guard = self.put_message_hook_list.lock().await;
        // for hook in guard.iter() {
        //     if let Some(result) = hook.execute_before_put_message(&msg.message_ext_inner) {
        //         return result;
        //     }
        // }
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
            let topic_config = self.get_topic_config(msg.topic()).await;
            if !QueueTypeUtils::is_batch_cq(&topic_config) {
                error!("[BUG]The message is an inner batch but cq type is not batch cq");
                return PutMessageResult::new_default(PutMessageStatus::MessageIllegal);
            }
        }
        self.commit_log.put_message(msg).await
    }
}

#[derive(Clone)]
pub struct CommitLogDispatcherDefault {
    build_index: CommitLogDispatcherBuildIndex,
    build_consume_queue: CommitLogDispatcherBuildConsumeQueue,
}

impl CommitLogDispatcher for CommitLogDispatcherDefault {
    async fn dispatch(&mut self, dispatch_request: &DispatchRequest) {
        self.build_index.dispatch(dispatch_request).await;
        self.build_consume_queue.dispatch(dispatch_request).await;
    }
}
