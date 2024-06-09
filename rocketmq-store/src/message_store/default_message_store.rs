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
    thread,
    time::{Duration, Instant},
};

use bytes::Buf;
use log::info;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_common::{
    common::{
        broker::broker_config::BrokerConfig,
        config::TopicConfig,
        message::{message_single::MessageExtBrokerInner, MessageConst},
        sys_flag::message_sys_flag::MessageSysFlag,
        //thread::thread_service_tokio::ThreadService,
    },
    utils::queue_type_utils::QueueTypeUtils,
    FileUtils::string_to_file,
    UtilAll::ensure_dir_ok,
};
use tokio::{runtime::Handle, sync::mpsc::Sender};
use tracing::{error, warn};

use crate::{
    base::{
        allocate_mapped_file_service::AllocateMappedFileService,
        commit_log_dispatcher::CommitLogDispatcher, dispatch_request::DispatchRequest,
        message_result::PutMessageResult, message_status_enum::PutMessageStatus,
        store_checkpoint::StoreCheckpoint,
    },
    config::{broker_role::BrokerRole, message_store_config::MessageStoreConfig},
    hook::put_message_hook::BoxedPutMessageHook,
    index::{index_dispatch::CommitLogDispatcherBuildIndex, index_service::IndexService},
    kv::compaction_service::CompactionService,
    log_file::{commit_log, commit_log::CommitLog, mapped_file::MappedFile, MessageStore},
    queue::{
        build_consume_queue::CommitLogDispatcherBuildConsumeQueue,
        local_file_consume_queue_store::ConsumeQueueStore, ConsumeQueueStoreTrait,
    },
    stats::broker_stats_manager::BrokerStatsManager,
    store::running_flags::RunningFlags,
    store_path_config_helper::{
        get_abort_file, get_store_checkpoint, get_store_path_consume_queue,
    },
};

///Using local files to store message data, which is also the default method.
pub struct DefaultMessageStore {
    message_store_config: Arc<MessageStoreConfig>,
    broker_config: Arc<BrokerConfig>,
    put_message_hook_list: Arc<parking_lot::RwLock<Vec<BoxedPutMessageHook>>>,
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
    //reput_message_service: Arc<parking_lot::Mutex<ReputMessageService>>,
    reput_message_service: ReputMessageService,
    clean_commit_log_service: Arc<CleanCommitLogService>,
    correct_logic_offset_service: Arc<CorrectLogicOffsetService>,
    clean_consume_queue_service: Arc<CleanConsumeQueueService>,
    broker_stats_manager: Option<Arc<BrokerStatsManager>>,
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
            reput_message_service: self.reput_message_service.clone(),
            clean_commit_log_service: self.clean_commit_log_service.clone(),
            correct_logic_offset_service: self.correct_logic_offset_service.clone(),
            clean_consume_queue_service: self.clean_consume_queue_service.clone(),
            broker_stats_manager: self.broker_stats_manager.clone(),
        }
    }
}

impl DefaultMessageStore {
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        broker_config: Arc<BrokerConfig>,
        topic_config_table: Arc<parking_lot::Mutex<HashMap<String, TopicConfig>>>,
        broker_stats_manager: Option<Arc<BrokerStatsManager>>,
    ) -> Self {
        let running_flags = Arc::new(RunningFlags::new());
        let store_checkpoint = Arc::new(
            StoreCheckpoint::new(get_store_checkpoint(
                message_store_config.store_path_root_dir.as_str(),
            ))
            .unwrap(),
        );
        let index_service =
            IndexService::new(message_store_config.clone(), store_checkpoint.clone());
        let build_index =
            CommitLogDispatcherBuildIndex::new(index_service.clone(), message_store_config.clone());
        // let topic_config_table = Arc::new(parking_lot::Mutex::new(HashMap::new()));
        let consume_queue_store = ConsumeQueueStore::new(
            message_store_config.clone(),
            broker_config.clone(),
            topic_config_table.clone(),
            running_flags.clone(),
            store_checkpoint.clone(),
        );
        let build_consume_queue =
            CommitLogDispatcherBuildConsumeQueue::new(consume_queue_store.clone());

        let dispatcher = CommitLogDispatcherDefault {
            dispatcher_vec: Arc::new(vec![Box::new(build_consume_queue), Box::new(build_index)]),
        };

        let commit_log = CommitLog::new(
            message_store_config.clone(),
            broker_config.clone(),
            &dispatcher,
            store_checkpoint.clone(),
            topic_config_table.clone(),
            consume_queue_store.clone(),
        );

        ensure_dir_ok(message_store_config.store_path_root_dir.as_str());
        ensure_dir_ok(Self::get_store_path_physic(&message_store_config).as_str());
        ensure_dir_ok(Self::get_store_path_logic(&message_store_config).as_str());

        Self {
            message_store_config: message_store_config.clone(),
            broker_config,
            put_message_hook_list: Arc::new(parking_lot::RwLock::new(vec![])),
            topic_config_table,
            // message_store_runtime: Some(RocketMQRuntime::new_multi(10, "message-store-thread")),
            commit_log,
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
            reput_message_service: ReputMessageService {
                tx: None,
                reput_from_offset: None,
            },
            clean_commit_log_service: Arc::new(CleanCommitLogService {}),
            correct_logic_offset_service: Arc::new(CorrectLogicOffsetService {}),
            clean_consume_queue_service: Arc::new(CleanConsumeQueueService {}),
            broker_stats_manager,
        }
    }

    pub fn get_store_path_physic(message_store_config: &Arc<MessageStoreConfig>) -> String {
        match message_store_config.enable_dledger_commit_log {
            true => {
                unimplemented!("dledger commit log is not supported yet")
            }
            false => message_store_config.get_store_path_commit_log(),
        }
    }

    pub fn get_store_path_logic(message_store_config: &Arc<MessageStoreConfig>) -> String {
        get_store_path_consume_queue(message_store_config.store_path_root_dir.as_str())
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

    pub fn recover_topic_queue_table(&mut self) {
        let min_phy_offset = self.commit_log.get_min_offset();
        self.consume_queue_store
            .recover_offset_table(min_phy_offset);
    }

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

    fn add_schedule_task(&self) {
        // clean files  Periodically
        let clean_commit_log_service_arc = self.clean_commit_log_service.clone();
        let clean_resource_interval = self.message_store_config.clean_resource_interval as u64;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(1000 * 60));
            interval.tick().await;
            let mut interval =
                tokio::time::interval(Duration::from_millis(clean_resource_interval));
            loop {
                clean_commit_log_service_arc.run();
                interval.tick().await;
            }
        });

        let message_store = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.tick().await;
            let mut interval = tokio::time::interval(Duration::from_secs(10 * 60));
            loop {
                message_store.check_self();
                interval.tick().await;
            }
        });

        // store check point flush
        let store_checkpoint_arc = self.store_checkpoint.clone().unwrap();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.tick().await;
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                let _ = store_checkpoint_arc.flush();
                interval.tick().await;
            }
        });

        let correct_logic_offset_service_arc = self.correct_logic_offset_service.clone();
        let clean_consume_queue_service_arc = self.clean_consume_queue_service.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(1000 * 60));
            interval.tick().await;
            let mut interval =
                tokio::time::interval(Duration::from_millis(clean_resource_interval));
            loop {
                correct_logic_offset_service_arc.run();
                clean_consume_queue_service_arc.run();
                interval.tick().await;
            }
        });
    }

    fn check_self(&self) {
        self.commit_log.check_self();
        self.consume_queue_store.check_self();
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
            // self.allocate_mapped_file_service.shutdown();
        }
        result
    }

    fn start(&mut self) -> Result<(), Box<dyn Error>> {
        self.create_temp_file();

        self.reput_message_service
            .set_reput_from_offset(self.commit_log.get_confirm_offset());
        self.reput_message_service.start(
            Arc::new(self.commit_log.clone()),
            self.message_store_config.clone(),
            self.dispatcher.clone(),
        );

        self.commit_log.start();

        //self.add_schedule_task();

        Ok(())
    }

    fn shutdown(&mut self) {
        if !self.shutdown.load(Ordering::Acquire) {
            self.shutdown.store(true, Ordering::SeqCst);
            self.reput_message_service.shutdown();
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
        self.commit_log.set_confirm_offset(phy_offset);
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
        for hook in self.put_message_hook_list.read().iter() {
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

    fn truncate_files(&mut self, offset_to_truncate: i64) -> bool {
        unimplemented!()
    }

    fn is_os_page_cache_busy(&self) -> bool {
        let begin = self.commit_log.begin_time_in_lock().load(Ordering::Relaxed);
        let diff = get_current_millis() - begin;
        diff < 10000000 && diff > self.message_store_config.os_page_cache_busy_timeout_mills
    }

    fn get_running_flags(&self) -> &RunningFlags {
        self.running_flags.as_ref()
    }

    fn is_shutdown(&self) -> bool {
        // todo!()
        false
    }

    fn get_put_message_hook_list(&self) -> Arc<parking_lot::RwLock<Vec<BoxedPutMessageHook>>> {
        self.put_message_hook_list.clone()
    }

    fn set_put_message_hook(&self, put_message_hook: BoxedPutMessageHook) {
        self.put_message_hook_list.write().push(put_message_hook);
    }

    fn get_broker_stats_manager(&self) -> Option<Arc<BrokerStatsManager>> {
        self.broker_stats_manager.clone()
    }

    fn dispatch_behind_bytes(&self) {}
}

#[derive(Clone)]
pub struct CommitLogDispatcherDefault {
    /*build_index: CommitLogDispatcherBuildIndex,
    build_consume_queue: CommitLogDispatcherBuildConsumeQueue,*/
    dispatcher_vec: Arc<Vec<Box<dyn CommitLogDispatcher>>>,
}

impl CommitLogDispatcher for CommitLogDispatcherDefault {
    fn dispatch(&self, dispatch_request: &DispatchRequest) {
        /*self.build_index.dispatch(dispatch_request);
        self.build_consume_queue.dispatch(dispatch_request);*/
        for dispatcher in self.dispatcher_vec.iter() {
            dispatcher.dispatch(dispatch_request);
        }
    }
}
#[derive(Clone)]
struct ReputMessageService {
    tx: Option<Arc<Sender<()>>>,
    reput_from_offset: Option<Arc<AtomicI64>>,
}

impl ReputMessageService {
    pub fn set_reput_from_offset(&mut self, reput_from_offset: i64) {
        self.reput_from_offset = Some(Arc::new(AtomicI64::new(reput_from_offset)));
    }

    pub fn start(
        &mut self,
        commit_log: Arc<CommitLog>,
        message_store_config: Arc<MessageStoreConfig>,
        dispatcher: CommitLogDispatcherDefault,
    ) {
        let mut inner = ReputMessageServiceInner {
            reput_from_offset: self.reput_from_offset.clone().unwrap(),
            commit_log,
            message_store_config,
            dispatcher,
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        self.tx = Some(Arc::new(tx));
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(2000));
            let mut break_flag = false;
            loop {
                tokio::select! {
                    _ = inner.do_reput() => {}
                    _ = rx.recv() => {
                        let mut index = 0;
                        while index < 50 && inner.is_commit_log_available() {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            if inner.is_commit_log_available() {
                                warn!(
                                    "shutdown ReputMessageService, but CommitLog have not finish to be \
                                     dispatched, CommitLog max offset={}, reputFromOffset={}",
                                    inner.commit_log.get_max_offset(),
                                    inner.reput_from_offset.load(Ordering::Relaxed)
                                );
                            }
                            index += 1;
                        }
                        info!("ReputMessageService shutdown now......");
                        break_flag = true;
                    }
                }
                if break_flag {
                    break;
                }
                interval.tick().await;
            }
        });
    }

    pub fn shutdown(&mut self) {
        let handle = Handle::current();
        let tx_option = self.tx.take();
        let _ = thread::spawn(move || {
            handle.block_on(async move {
                if let Some(tx) = tx_option {
                    tokio::select! {
                      _ =  tx.send(()) =>{

                        }
                    }
                }
            });
        })
        .join();
    }
}

//Construct a consumer queue and index file.
struct ReputMessageServiceInner {
    reput_from_offset: Arc<AtomicI64>,
    commit_log: Arc<CommitLog>,
    message_store_config: Arc<MessageStoreConfig>,
    dispatcher: CommitLogDispatcherDefault,
}

impl ReputMessageServiceInner {
    pub async fn do_reput(&mut self) {
        let reput_from_offset = self.reput_from_offset.load(Ordering::Acquire);
        if reput_from_offset < self.commit_log.get_min_offset() {
            warn!(
                "The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate \
                 that the dispatch behind too much and the commitlog has expired.",
                reput_from_offset,
                self.commit_log.get_min_offset()
            );
            self.reput_from_offset
                .store(self.commit_log.get_min_offset(), Ordering::Release);
        }
        let mut do_next = true;
        while do_next && self.is_commit_log_available() {
            let result = self
                .commit_log
                .get_data(self.reput_from_offset.load(Ordering::Acquire));
            if result.is_none() {
                break;
            }
            let result = result.unwrap();
            self.reput_from_offset
                .store(result.start_offset as i64, Ordering::SeqCst);
            let mut read_size = 0i32;
            let mapped_file = result.mapped_file.as_ref().unwrap();
            let start_pos = (result.start_offset % mapped_file.get_file_size()) as i32;
            loop {
                let size = mapped_file.get_bytes((start_pos + read_size) as usize, 4);
                if size.is_none() {
                    do_next = false;
                    break;
                }
                let mut bytes = mapped_file.get_data(
                    (start_pos + read_size) as usize,
                    size.unwrap().get_i32() as usize,
                );
                if bytes.is_none() {
                    do_next = false;
                    break;
                }

                let dispatch_request = commit_log::check_message_and_return_size(
                    bytes.as_mut().unwrap(),
                    false,
                    false,
                    false,
                    &self.message_store_config,
                );
                if self.reput_from_offset.load(Ordering::Acquire) + dispatch_request.msg_size as i64
                    > self.commit_log.get_confirm_offset()
                {
                    do_next = false;
                    break;
                }
                if dispatch_request.success {
                    match dispatch_request.msg_size.cmp(&0) {
                        std::cmp::Ordering::Greater => {
                            self.dispatcher.dispatch(&dispatch_request);
                            self.reput_from_offset
                                .fetch_add(dispatch_request.msg_size as i64, Ordering::AcqRel);
                            read_size += dispatch_request.msg_size;
                            if !self.message_store_config.duplication_enable
                                && self.message_store_config.broker_role == BrokerRole::Slave
                            {
                                unimplemented!()
                            }
                        }
                        std::cmp::Ordering::Equal => {
                            self.reput_from_offset.store(
                                self.commit_log
                                    .roll_next_file(self.reput_from_offset.load(Ordering::Relaxed)),
                                Ordering::SeqCst,
                            );
                            read_size = result.size;
                        }
                        std::cmp::Ordering::Less => {}
                    }
                } else if dispatch_request.msg_size > 0 {
                    error!(
                        "[BUG]read total count not equals msg total size. reputFromOffset={}",
                        self.reput_from_offset.load(Ordering::Relaxed)
                    );
                    self.reput_from_offset
                        .fetch_add(dispatch_request.msg_size as i64, Ordering::SeqCst);
                } else {
                    do_next = false;
                    if self.message_store_config.enable_dledger_commit_log {
                        unimplemented!()
                    }
                }

                if !(read_size < result.size
                    && self.reput_from_offset.load(Ordering::Acquire)
                        < self.commit_log.get_confirm_offset()
                    && do_next)
                {
                    break;
                }
            }
        }
    }

    fn is_commit_log_available(&self) -> bool {
        self.reput_from_offset.load(Ordering::Relaxed) < self.commit_log.get_confirm_offset()
    }
    pub fn reput_from_offset(&self) -> i64 {
        self.reput_from_offset.load(Ordering::Relaxed)
    }

    pub fn set_reput_from_offset(&mut self, reput_from_offset: i64) {
        self.reput_from_offset
            .store(reput_from_offset, Ordering::SeqCst);
    }
}

struct CleanCommitLogService {}

impl CleanCommitLogService {
    fn run(&self) {
        info!("clean commit log service run unimplemented!")
    }
}

struct CleanConsumeQueueService {}

impl CleanConsumeQueueService {
    fn run(&self) {
        println!("clean consume queue service run unimplemented!")
    }
}

struct CorrectLogicOffsetService {}

impl CorrectLogicOffsetService {
    fn run(&self) {
        println!("correct logic offset service run unimplemented!")
    }
}
