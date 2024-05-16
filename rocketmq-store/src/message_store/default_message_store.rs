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

use std::{
    collections::HashMap,
    error::Error,
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc,
    },
    time::Instant,
};

use log::info;
use rocketmq_common::{
    common::{
        broker::broker_config::BrokerConfig,
        config::TopicConfig,
        message::{message_single::MessageExtBrokerInner, MessageConst},
        sys_flag::message_sys_flag::MessageSysFlag,
        thread::thread_service::ThreadService,
    },
    utils::queue_type_utils::QueueTypeUtils,
    FileUtils::string_to_file,
};
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
        local_file_consume_queue_store::ConsumeQueueStore, ConsumeQueueStoreTrait,
    },
    store::running_flags::RunningFlags,
    store_path_config_helper::{get_abort_file, get_store_checkpoint},
};

///Using local files to store message data, which is also the default method.
pub struct DefaultMessageStore {
    message_store_config: Arc<MessageStoreConfig>,
    broker_config: Arc<BrokerConfig>,
    put_message_hook_list: Arc<Vec<BoxedPutMessageHook>>,
    topic_config_table: Arc<parking_lot::Mutex<HashMap<String, TopicConfig>>>,
    //message_store_runtime: Option<RocketMQRuntime>,
    commit_log: CommitLog,
    compaction_service: CompactionService,
    store_checkpoint: Option<Arc<StoreCheckpoint>>,
    master_flushed_offset: Arc<AtomicI64>,
    index_service: IndexService,
    allocate_mapped_file_service: Arc<AllocateMappedFileService>,
    consume_queue_store: ConsumeQueueStore,
    dispatcher: CommitLogDispatcherDefault,
    broker_init_max_offset: Arc<AtomicI64>,
    state_machine_version: Arc<AtomicI64>,
    shutdown: Arc<AtomicBool>,
    running_flags: Arc<RunningFlags>,
}

impl Clone for DefaultMessageStore {
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
            broker_init_max_offset: self.broker_init_max_offset.clone(),
            state_machine_version: self.state_machine_version.clone(),
            shutdown: self.shutdown.clone(),
            running_flags: self.running_flags.clone(),
        }
    }
}

impl DefaultMessageStore {
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        broker_config: Arc<BrokerConfig>,
    ) -> Self {
        let index_service = IndexService {};
        let running_flags = Arc::new(RunningFlags::new());
        let store_checkpoint = Arc::new(
            StoreCheckpoint::new(get_store_checkpoint(
                message_store_config.store_path_root_dir.as_str(),
            ))
            .unwrap(),
        );
        let build_index =
            CommitLogDispatcherBuildIndex::new(index_service.clone(), message_store_config.clone());
        let topic_config_table = Arc::new(parking_lot::Mutex::new(HashMap::new()));
        let consume_queue_store = ConsumeQueueStore::new(
            message_store_config.clone(),
            topic_config_table.clone(),
            running_flags.clone(),
            store_checkpoint.clone(),
        );
        let build_consume_queue =
            CommitLogDispatcherBuildConsumeQueue::new(consume_queue_store.clone());
        let dispatcher = CommitLogDispatcherDefault {
            build_index,
            build_consume_queue,
        };

        let commit_log = CommitLog::new(
            message_store_config.clone(),
            broker_config.clone(),
            &dispatcher,
            store_checkpoint.clone(),
            topic_config_table.clone(),
        );
        Self {
            message_store_config: message_store_config.clone(),
            broker_config,
            put_message_hook_list: Arc::new(vec![]),
            topic_config_table,
            // message_store_runtime: Some(RocketMQRuntime::new_multi(10, "message-store-thread")),
            commit_log: commit_log.clone(),
            compaction_service: Default::default(),
            store_checkpoint: Some(store_checkpoint),
            master_flushed_offset: Arc::new(AtomicI64::new(-1)),
            index_service,
            allocate_mapped_file_service: Arc::new(AllocateMappedFileService::new()),
            consume_queue_store,
            dispatcher,
            broker_init_max_offset: Arc::new(AtomicI64::new(-1)),
            state_machine_version: Arc::new(AtomicI64::new(0)),
            shutdown: Arc::new(AtomicBool::new(false)),
            running_flags,
        }
    }
}

impl Drop for DefaultMessageStore {
    fn drop(&mut self) {
        // if let Some(runtime) = self.message_store_runtime.take() {
        //     runtime.shutdown();
        // }
    }
}

impl DefaultMessageStore {
    pub fn get_topic_config(&self, topic: &str) -> Option<TopicConfig> {
        if self.topic_config_table.lock().is_empty() {
            return None;
        }
        self.topic_config_table.lock().get(topic).cloned()
    }

    fn is_temp_file_exist(&self) -> bool {
        let file_name = get_abort_file(self.message_store_config.store_path_root_dir.as_str());
        fs::metadata(file_name).is_ok()
    }

    fn create_temp_file(&self) {
        let file_name = get_abort_file(self.message_store_config.store_path_root_dir.as_str());
        let pid = std::process::id();
        match fs::File::create(file_name.as_str()) {
            Ok(_) => {}
            Err(e) => {
                error!("create temp file error: {}", e);
            }
        }
        let _ = string_to_file(pid.to_string().as_str(), file_name.as_str());
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
            self.recover_abnormally(max_phy_offset_of_consume_queue)
                .await;
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
            .recover_normally(max_phy_offset_of_consume_queue, self.clone())
            .await;
    }

    pub async fn recover_abnormally(&mut self, max_phy_offset_of_consume_queue: i64) {
        self.commit_log
            .recover_abnormally(max_phy_offset_of_consume_queue, self.clone())
            .await;
    }

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

    pub fn on_commit_log_dispatch(
        &mut self,
        dispatch_request: &DispatchRequest,
        do_dispatch: bool,
        is_recover: bool,
        _is_file_end: bool,
    ) {
        if do_dispatch && !is_recover {
            self.do_dispatch(dispatch_request);
        }
    }

    pub fn do_dispatch(&mut self, dispatch_request: &DispatchRequest) {
        self.dispatcher.dispatch(dispatch_request)
    }

    pub fn truncate_dirty_logic_files(&mut self, phy_offset: i64) {
        self.consume_queue_store.truncate_dirty(phy_offset);
    }

    pub fn consume_queue_store_mut(&mut self) -> &mut ConsumeQueueStore {
        &mut self.consume_queue_store
    }

    fn delete_file(&mut self, file_name: String) {
        match fs::remove_file(PathBuf::from(file_name.as_str())) {
            Ok(_) => {
                info!("delete OK, file:{}", file_name);
            }
            Err(err) => {
                error!("delete error, file:{}, {:?}", file_name, err);
            }
        }
    }
}

impl MessageStore for DefaultMessageStore {
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
        //load Commit log-- init commit mapped file queue
        let mut result = self.commit_log.load();
        if !result {
            return result;
        }
        // load Consume Queue-- init Consume log mapped file queue
        result &= self.consume_queue_store.load();

        if self.message_store_config.enable_compaction {
            result &= self.compaction_service.load(last_exit_ok);
            if !result {
                return result;
            }
        }

        if result {
            /*self.store_checkpoint = Some(StoreCheckpoint::new(get_store_checkpoint(
                self.message_store_config.store_path_root_dir.as_str(),
            )));*/
            let checkpoint = self.store_checkpoint.as_ref().unwrap();
            self.master_flushed_offset =
                Arc::new(AtomicI64::new(checkpoint.master_flushed_offset() as i64));
            self.set_confirm_offset(checkpoint.confirm_phy_offset() as i64);
            result = self.index_service.load(last_exit_ok);

            //recover commit log and consume queue
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
        self.create_temp_file();
        Ok(())
    }

    fn shutdown(&mut self) {
        if !self.shutdown.load(Ordering::Relaxed) {
            self.shutdown.store(true, Ordering::SeqCst);
            self.commit_log.shutdown();

            if self.running_flags.is_writeable() {
                //delete abort file
                self.delete_file(get_abort_file(
                    self.message_store_config.store_path_root_dir.as_str(),
                ))
            }
        }
    }

    fn set_confirm_offset(&mut self, phy_offset: i64) {

        // self.commit_log.set_confirm_offset(phy_offset);
    }

    fn get_max_phy_offset(&self) -> i64 {
        self.commit_log.get_max_offset()
    }

    fn set_broker_init_max_offset(&mut self, broker_init_max_offset: i64) {
        self.broker_init_max_offset
            .store(broker_init_max_offset, Ordering::SeqCst);
    }

    fn get_state_machine_version(&self) -> i64 {
        self.state_machine_version.load(Ordering::Relaxed)
    }

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
            let topic_config = self.get_topic_config(msg.topic());
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
    fn dispatch(&mut self, dispatch_request: &DispatchRequest) {
        self.build_index.dispatch(dispatch_request);
        self.build_consume_queue.dispatch(dispatch_request);
    }
}
