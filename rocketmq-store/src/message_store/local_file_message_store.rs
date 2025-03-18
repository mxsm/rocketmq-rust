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
#![allow(unused_imports)]

use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use bytes::Buf;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_common::common::attribute::cleanup_policy::CleanupPolicy;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::mix_all::is_lmq;
use rocketmq_common::common::mix_all::is_sys_consumer_group_for_no_cold_read_limit;
use rocketmq_common::common::mix_all::MULTI_DISPATCH_QUEUE_SPLITTER;
use rocketmq_common::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::common::system_clock::SystemClock;
use rocketmq_common::utils::queue_type_utils::QueueTypeUtils;
use rocketmq_common::utils::util_all;
use rocketmq_common::CleanupPolicyUtils::get_delete_policy;
use rocketmq_common::FileUtils::string_to_file;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_common::UtilAll::ensure_dir_ok;
use rocketmq_rust::ArcMut;
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::allocate_mapped_file_service::AllocateMappedFileService;
use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::base::get_message_result::GetMessageResult;
use crate::base::message_arriving_listener::MessageArrivingListener;
use crate::base::message_result::AppendMessageResult;
use crate::base::message_result::PutMessageResult;
use crate::base::message_status_enum::GetMessageStatus;
use crate::base::message_status_enum::PutMessageStatus;
use crate::base::message_store::MessageStoreRefactor;
use crate::base::query_message_result::QueryMessageResult;
use crate::base::select_result::SelectMappedBufferResult;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::base::store_stats_service::StoreStatsService;
use crate::base::transient_store_pool::TransientStorePool;
use crate::config::message_store_config::MessageStoreConfig;
use crate::config::store_path_config_helper::get_store_path_batch_consume_queue;
use crate::config::store_path_config_helper::get_store_path_consume_queue_ext;
use crate::filter::MessageFilter;
use crate::ha::general_ha_service::GeneralHAService;
use crate::ha::ha_service::HAService;
use crate::hook::put_message_hook::BoxedPutMessageHook;
use crate::hook::put_message_hook::PutMessageHook;
use crate::hook::send_message_back_hook::SendMessageBackHook;
use crate::index::index_dispatch::CommitLogDispatcherBuildIndex;
use crate::index::index_service::IndexService;
use crate::kv::compaction_service::CompactionService;
use crate::kv::compaction_store::CompactionStore;
use crate::log_file::commit_log;
use crate::log_file::commit_log::CommitLog;
use crate::log_file::mapped_file::MappedFile;
use crate::log_file::MessageStore;
use crate::log_file::MAX_PULL_MSG_SIZE;
use crate::queue::build_consume_queue::CommitLogDispatcherBuildConsumeQueue;
use crate::queue::local_file_consume_queue_store::ConsumeQueueStore;
use crate::queue::ArcConsumeQueue;
use crate::queue::ConsumeQueueStoreTrait;
use crate::queue::ConsumeQueueTrait;
use crate::stats::broker_stats_manager::BrokerStatsManager;
use crate::store::running_flags::RunningFlags;
use crate::store_error::StoreError;
use crate::store_path_config_helper::get_abort_file;
use crate::store_path_config_helper::get_store_checkpoint;
use crate::store_path_config_helper::get_store_path_consume_queue;
use crate::timer::timer_message_store::TimerMessageStore;
use crate::utils::store_util::TOTAL_PHYSICAL_MEMORY_SIZE;

///Using local files to store message data, which is also the default method.
pub struct LocalFileMessageStore {
    message_store_config: Arc<MessageStoreConfig>,
    broker_config: Arc<BrokerConfig>,
    put_message_hook_list: Arc<parking_lot::RwLock<Vec<BoxedPutMessageHook>>>,
    topic_config_table: Arc<parking_lot::Mutex<HashMap<CheetahString, TopicConfig>>>,
    commit_log: CommitLog,
    compaction_service: Option<CompactionService>,
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
    reput_message_service: ReputMessageService,
    clean_commit_log_service: Arc<CleanCommitLogService>,
    correct_logic_offset_service: Arc<CorrectLogicOffsetService>,
    clean_consume_queue_service: Arc<CleanConsumeQueueService>,
    broker_stats_manager: Option<Arc<BrokerStatsManager>>,
    message_arriving_listener:
        Option<Arc<Box<dyn MessageArrivingListener + Sync + Send + 'static>>>,
    notify_message_arrive_in_batch: bool,
    store_stats_service: Arc<StoreStatsService>,
    compaction_store: Arc<CompactionStore>,
    timer_message_store: Arc<TimerMessageStore>,
    transient_store_pool: TransientStorePool,
    message_store_arc: Option<ArcMut<LocalFileMessageStore>>,
    ha_service: Option<ArcMut<GeneralHAService>>,
    flush_consume_queue_service: FlushConsumeQueueService,
}

impl LocalFileMessageStore {
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        broker_config: Arc<BrokerConfig>,
        topic_config_table: Arc<parking_lot::Mutex<HashMap<CheetahString, TopicConfig>>>,
        broker_stats_manager: Option<Arc<BrokerStatsManager>>,
        notify_message_arrive_in_batch: bool,
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

        /*let dispatcher = CommitLogDispatcherDefault {
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

        let identity = broker_config.broker_identity.clone();
        let transient_store_pool = TransientStorePool::new(
            message_store_config.transient_store_pool_size,
            message_store_config.mapped_file_size_commit_log,
        );
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
                shutdown: Arc::new(Notify::new()),
                reput_from_offset: None,
                message_store_config,
                inner: None,
            },
            clean_commit_log_service: Arc::new(CleanCommitLogService {}),
            correct_logic_offset_service: Arc::new(CorrectLogicOffsetService {}),
            clean_consume_queue_service: Arc::new(CleanConsumeQueueService {}),
            broker_stats_manager,
            message_arriving_listener: None,
            notify_message_arrive_in_batch,
            store_stats_service: Arc::new(StoreStatsService::new(Some(identity))),
            compaction_store: Arc::new(CompactionStore),
            timer_message_store: Arc::new(TimerMessageStore::new_empty()),
            transient_store_pool,
            message_store_arc: None,
        }*/
        unimplemented!("LocalFileMessageStore::new not implemented yet")
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

    pub fn message_store_config(&self) -> Arc<MessageStoreConfig> {
        self.message_store_config.clone()
    }

    pub fn is_transient_store_pool_enable(&self) -> bool {
        self.message_store_config.transient_store_pool_enable
            && (self.broker_config.enable_controller_mode
                || self.message_store_config().broker_role != BrokerRole::Slave)
    }

    pub fn set_message_store_arc(
        &mut self,
        message_store_arc: Option<ArcMut<LocalFileMessageStore>>,
    ) {
        self.message_store_arc = message_store_arc;
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
    #[inline]
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
        unimplemented!("recover_normally not implemented yet");
        /*self.commit_log
        .recover_normally(
            max_phy_offset_of_consume_queue,
            self.message_store_arc.clone().unwrap(),
        )
        .await;*/
    }

    pub async fn recover_abnormally(&mut self, max_phy_offset_of_consume_queue: i64) {
        unimplemented!("recover_abnormally not implemented yet");
        /*self.commit_log
        .recover_abnormally(
            max_phy_offset_of_consume_queue,
            self.message_store_arc.clone().unwrap(),
        )
        .await;*/
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

        let message_store = self.message_store_arc.clone().unwrap();
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

    pub fn next_offset_correction(&self, old_offset: i64, new_offset: i64) -> i64 {
        let mut next_offset = old_offset;
        if self.message_store_config.broker_role != BrokerRole::Slave
            || self.message_store_config.offset_check_in_slave
        {
            next_offset = new_offset;
        }
        next_offset
    }

    fn check_in_mem_by_commit_offset(&self, offset_py: i64, size: i32) -> bool {
        let message = self.commit_log.get_message(offset_py, size);
        match message {
            None => false,
            Some(msg) => msg.is_in_mem(),
        }
    }

    pub fn set_message_arriving_listener(
        &mut self,
        message_arriving_listener: Option<
            Arc<Box<dyn MessageArrivingListener + Sync + Send + 'static>>,
        >,
    ) {
        self.message_arriving_listener = message_arriving_listener;
    }

    fn do_recheck_reput_offset_from_cq(&self) {
        error!("do_recheck_reput_offset_from_cq called, not implemented yet");
    }
}

fn estimate_in_mem_by_commit_offset(
    offset_py: i64,
    max_offset_py: i64,
    message_store_config: &Arc<MessageStoreConfig>,
) -> bool {
    let memory = (*TOTAL_PHYSICAL_MEMORY_SIZE as f64)
        * (message_store_config.access_message_in_memory_max_ratio as f64 / 100.0);
    (max_offset_py - offset_py) <= memory as i64
}

fn is_the_batch_full(
    size_py: i32,
    unit_batch_num: i32,
    max_msg_nums: i32,
    max_msg_size: i64,
    buffer_total: i32,
    message_total: i32,
    is_in_mem: bool,
    message_store_config: &Arc<MessageStoreConfig>,
) -> bool {
    if buffer_total == 0 || message_total == 0 {
        return false;
    }

    if message_total + unit_batch_num > max_msg_nums {
        return true;
    }

    if buffer_total as i64 + size_py as i64 > max_msg_size {
        return true;
    }

    if is_in_mem {
        if (buffer_total + size_py) as u64
            > message_store_config.max_transfer_bytes_on_message_in_memory
        {
            return true;
        }

        message_total as u64 > message_store_config.max_transfer_count_on_message_in_memory - 1
    } else {
        if (buffer_total + size_py) as u64
            > message_store_config.max_transfer_bytes_on_message_in_disk
        {
            return true;
        }

        message_total as u64 > message_store_config.max_transfer_count_on_message_in_disk - 1
    }
}

#[allow(unused_variables)]
#[allow(unused_assignments)]
impl MessageStoreRefactor for LocalFileMessageStore {
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
            result &= self.compaction_service.as_mut().unwrap().load(last_exit_ok);
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

    fn start(&mut self) -> Result<(), StoreError> {
        if !self.message_store_config.enable_dleger_commit_log
            && !self.message_store_config.duplication_enable
        {
            if let Some(ha_service) = self.ha_service.as_mut() {
                ha_service
                    .init(self.message_store_arc.clone().unwrap())
                    .map_err(|e| {
                        error!("HA service start failed: {:?}", e);
                        StoreError::General(e.to_string())
                    })?;
            }
        }

        if self.is_transient_store_pool_enable() {
            self.transient_store_pool.init();
        }

        self.allocate_mapped_file_service.start();

        self.index_service.start();

        self.reput_message_service
            .set_reput_from_offset(self.commit_log.get_confirm_offset());
        self.reput_message_service.start(
            Arc::new(self.commit_log.clone()),
            self.message_store_config.clone(),
            self.dispatcher.clone(),
            self.notify_message_arrive_in_batch,
            self.message_store_arc.clone().unwrap(),
        );
        self.do_recheck_reput_offset_from_cq();
        self.flush_consume_queue_service.start();
        self.commit_log.start();
        self.consume_queue_store.start();
        self.store_stats_service.start();

        if let Some(ha_service) = self.ha_service.as_mut() {
            ha_service.start().map_err(|e| {
                error!("HA service start failed: {:?}", e);
                StoreError::General(e.to_string())
            })?;
        }
        self.create_temp_file();
        self.add_schedule_task();
        // self.perfs.start();
        self.shutdown.store(false, Ordering::Release);
        Ok(())
    }

    fn shutdown(&mut self) {
        if !self.shutdown.load(Ordering::Acquire) {
            self.shutdown.store(true, Ordering::Release);

            if let Some(ha_service) = self.ha_service.as_ref() {
                ha_service.shutdown();
            }

            self.store_stats_service.shutdown();
            self.commit_log.shutdown();

            self.reput_message_service.shutdown();
            self.consume_queue_store.shutdown();

            // dispatch-related services must be shut down after reputMessageService
            self.index_service.shutdown();

            if let Some(compaction_service) = self.compaction_service.as_ref() {
                compaction_service.shutdown();
            }

            if self.message_store_config.rocksdb_cq_double_write_enable {
                // this.rocksDBMessageStore.consumeQueueStore.shutdown();
            }
            self.flush_consume_queue_service.shutdown();
            self.allocate_mapped_file_service.shutdown();
            if let Some(store_checkpoint) = self.store_checkpoint.as_ref() {
                let _ = store_checkpoint.shutdown();
            }
            if self.running_flags.is_writeable() {
                //delete abort file
                self.delete_file(get_abort_file(
                    self.message_store_config.store_path_root_dir.as_str(),
                ))
            }
        }

        self.transient_store_pool.destroy();
    }

    fn destroy(&self) {
        todo!()
    }

    async fn async_put_message(
        &self,
        msg: MessageExtBrokerInner,
    ) -> Result<PutMessageResult, StoreError> {
        todo!()
    }

    async fn async_put_messages(
        &self,
        message_ext_batch: MessageExtBatch,
    ) -> Result<PutMessageResult, StoreError> {
        todo!()
    }

    async fn put_message(
        &self,
        msg: MessageExtBrokerInner,
    ) -> Result<PutMessageResult, StoreError> {
        todo!()
    }

    async fn put_messages(
        &self,
        message_ext_batch: MessageExtBatch,
    ) -> Result<PutMessageResult, StoreError> {
        todo!()
    }

    fn get_message(
        &self,
        group: &str,
        topic: &str,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        message_filter: &dyn MessageFilter,
    ) -> Result<GetMessageResult, StoreError> {
        todo!()
    }

    async fn get_message_async(
        &self,
        group: &str,
        topic: &str,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        message_filter: &dyn MessageFilter,
    ) -> Result<GetMessageResult, StoreError> {
        todo!()
    }

    fn get_message_with_size_limit(
        &self,
        group: &str,
        topic: &str,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: &dyn MessageFilter,
    ) -> Result<GetMessageResult, StoreError> {
        todo!()
    }

    async fn get_message_with_size_limit_async(
        &self,
        group: &str,
        topic: &str,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: &dyn MessageFilter,
    ) -> Result<GetMessageResult, StoreError> {
        todo!()
    }

    fn get_max_offset_in_queue(&self, topic: &str, queue_id: i32) -> i64 {
        todo!()
    }

    fn get_max_offset_in_queue_committed(
        &self,
        topic: &str,
        queue_id: i32,
        committed: bool,
    ) -> i64 {
        todo!()
    }

    fn get_min_offset_in_queue(&self, topic: &str, queue_id: i32) -> i64 {
        todo!()
    }

    fn get_timer_message_store(&self) -> Option<Arc<TimerMessageStore>> {
        todo!()
    }

    fn set_timer_message_store(&self, timer_message_store: Arc<TimerMessageStore>) {
        todo!()
    }

    fn get_commit_log_offset_in_queue(
        &self,
        topic: &str,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> i64 {
        todo!()
    }

    fn get_offset_in_queue_by_time(&self, topic: &str, queue_id: i32, timestamp: i64) -> i64 {
        todo!()
    }

    fn get_offset_in_queue_by_time_with_boundary(
        &self,
        topic: &str,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64 {
        todo!()
    }

    fn look_message_by_offset(&self, commit_log_offset: i64) -> Result<MessageExt, StoreError> {
        todo!()
    }

    fn look_message_by_offset_and_size(
        &self,
        commit_log_offset: i64,
        size: i32,
    ) -> Result<MessageExt, StoreError> {
        todo!()
    }

    fn select_one_message_by_offset(
        &self,
        commit_log_offset: i64,
    ) -> Result<SelectMappedBufferResult, StoreError> {
        todo!()
    }

    fn select_one_message_by_offset_and_size(
        &self,
        commit_log_offset: i64,
        msg_size: i32,
    ) -> Result<SelectMappedBufferResult, StoreError> {
        todo!()
    }

    fn get_running_data_info(&self) -> String {
        todo!()
    }

    fn get_timing_message_count(&self, topic: &str) -> i64 {
        todo!()
    }

    fn get_runtime_info(&self) -> HashMap<String, String> {
        todo!()
    }

    fn get_max_phy_offset(&self) -> i64 {
        todo!()
    }

    fn get_min_phy_offset(&self) -> i64 {
        todo!()
    }

    fn get_earliest_message_time(&self, topic: &str, queue_id: i32) -> i64 {
        todo!()
    }

    fn get_earliest_message_time_store(&self) -> i64 {
        todo!()
    }

    async fn get_earliest_message_time_async(
        &self,
        topic: &str,
        queue_id: i32,
    ) -> Result<i64, StoreError> {
        todo!()
    }

    fn get_message_store_time_stamp(
        &self,
        topic: &str,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> i64 {
        todo!()
    }

    async fn get_message_store_time_stamp_async(
        &self,
        topic: &str,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> Result<i64, StoreError> {
        todo!()
    }

    fn get_message_total_in_queue(&self, topic: &str, queue_id: i32) -> i64 {
        todo!()
    }

    fn get_commit_log_data(&self, offset: i64) -> Result<SelectMappedBufferResult, StoreError> {
        todo!()
    }

    fn get_bulk_commit_log_data(&self, offset: i64, size: i32) -> Vec<SelectMappedBufferResult> {
        todo!()
    }

    fn append_to_commit_log(
        &self,
        start_offset: i64,
        data: &[u8],
        data_start: i32,
        data_length: i32,
    ) -> Result<bool, StoreError> {
        todo!()
    }

    fn execute_delete_files_manually(&self) {
        todo!()
    }

    fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Result<QueryMessageResult, StoreError> {
        todo!()
    }

    async fn query_message_async(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Result<QueryMessageResult, StoreError> {
        todo!()
    }

    fn update_ha_master_address(&self, new_addr: &str) {
        todo!()
    }

    fn update_master_address(&self, new_addr: &str) {
        todo!()
    }

    fn slave_fall_behind_much(&self) -> i64 {
        todo!()
    }

    fn delete_topics(&self, delete_topics: &HashSet<String>) -> i32 {
        todo!()
    }

    fn clean_unused_topic(&self, retain_topics: &HashSet<String>) -> i32 {
        todo!()
    }

    fn clean_expired_consumer_queue(&self) {
        todo!()
    }

    fn check_in_mem_by_consume_offset(
        &self,
        topic: &str,
        queue_id: i32,
        consume_offset: i64,
        batch_size: i32,
    ) -> bool {
        todo!()
    }

    fn check_in_store_by_consume_offset(
        &self,
        topic: &str,
        queue_id: i32,
        consume_offset: i64,
    ) -> bool {
        todo!()
    }

    fn dispatch_behind_bytes(&self) -> i64 {
        todo!()
    }

    fn flush(&self) -> i64 {
        todo!()
    }

    fn get_flushed_where(&self) -> i64 {
        todo!()
    }

    fn reset_write_offset(&self, phy_offset: i64) -> bool {
        todo!()
    }

    fn get_confirm_offset(&self) -> i64 {
        todo!()
    }

    fn set_confirm_offset(&self, phy_offset: i64) {
        todo!()
    }

    fn is_os_page_cache_busy(&self) -> bool {
        todo!()
    }

    fn lock_time_millis(&self) -> i64 {
        todo!()
    }

    fn is_transient_store_pool_deficient(&self) -> bool {
        todo!()
    }

    fn get_dispatcher_list(&self) -> Vec<Arc<dyn CommitLogDispatcher>> {
        todo!()
    }

    fn add_dispatcher(&self, dispatcher: Arc<dyn CommitLogDispatcher>) {
        todo!()
    }

    fn get_consume_queue(&self, topic: &str, queue_id: i32) -> Option<Arc<dyn ConsumeQueueTrait>> {
        todo!()
    }

    fn find_consume_queue(&self, topic: &str, queue_id: i32) -> Arc<dyn ConsumeQueueTrait> {
        todo!()
    }

    fn get_broker_stats_manager(&self) -> Arc<BrokerStatsManager> {
        todo!()
    }

    fn on_commit_log_append<MF: MappedFile>(
        &self,
        msg: &MessageExtBrokerInner,
        result: &AppendMessageResult,
        commit_log_file: &MF,
    ) {
        todo!()
    }

    fn on_commit_log_dispatch<MF: MappedFile>(
        &self,
        dispatch_request: &DispatchRequest,
        do_dispatch: bool,
        commit_log_file: &MF,
        is_recover: bool,
        is_file_end: bool,
    ) -> Result<(), StoreError> {
        todo!()
    }

    fn finish_commit_log_dispatch(&self) {
        todo!()
    }

    fn get_message_store_config(&self) -> Arc<MessageStoreConfig> {
        todo!()
    }

    fn get_store_stats_service(&self) -> Arc<StoreStatsService> {
        todo!()
    }

    fn get_store_checkpoint(&self) -> Arc<StoreCheckpoint> {
        todo!()
    }

    fn get_system_clock(&self) -> Arc<SystemClock> {
        todo!()
    }

    fn get_commit_log(&self) -> Arc<CommitLog> {
        todo!()
    }

    fn get_running_flags(&self) -> Arc<RunningFlags> {
        todo!()
    }

    fn get_transient_store_pool(&self) -> Arc<TransientStorePool> {
        todo!()
    }

    fn get_allocate_mapped_file_service(&self) -> Arc<AllocateMappedFileService> {
        todo!()
    }

    fn truncate_dirty_logic_files(&self, phy_offset: i64) -> Result<(), StoreError> {
        todo!()
    }

    fn unlock_mapped_file<MF: MappedFile>(&self, unlock_mapped_file: &MF) {
        todo!()
    }

    fn get_queue_store(&self) -> Arc<dyn ConsumeQueueStoreTrait> {
        todo!()
    }

    fn is_sync_disk_flush(&self) -> bool {
        todo!()
    }

    fn is_sync_master(&self) -> bool {
        todo!()
    }

    fn assign_offset(&self, msg: &mut MessageExtBrokerInner) -> Result<(), StoreError> {
        todo!()
    }

    fn increase_offset(&self, msg: &MessageExtBrokerInner, message_num: i16) {
        todo!()
    }

    fn get_master_store_in_process<M: MessageStoreRefactor>(&self) -> Option<Arc<M>> {
        todo!()
    }

    fn set_master_store_in_process<M: MessageStoreRefactor>(
        &self,
        master_store_in_process: Arc<M>,
    ) {
        todo!()
    }

    fn get_data(&self, offset: i64, size: i32, byte_buffer: &mut BytesMut) -> bool {
        todo!()
    }

    fn set_alive_replica_num_in_group(&self, alive_replica_nums: i32) {
        todo!()
    }

    fn get_alive_replica_num_in_group(&self) -> i32 {
        todo!()
    }

    fn wakeup_ha_client(&self) {
        todo!()
    }

    fn get_master_flushed_offset(&self) -> i64 {
        todo!()
    }

    fn get_broker_init_max_offset(&self) -> i64 {
        todo!()
    }

    fn set_master_flushed_offset(&self, master_flushed_offset: i64) {
        todo!()
    }

    fn set_broker_init_max_offset(&self, broker_init_max_offset: i64) {
        todo!()
    }

    fn calc_delta_checksum(&self, from: i64, to: i64) -> Vec<u8> {
        todo!()
    }

    fn truncate_files(&self, offset_to_truncate: i64) -> Result<bool, StoreError> {
        todo!()
    }

    fn is_offset_aligned(&self, offset: i64) -> bool {
        todo!()
    }

    fn get_put_message_hook_list(&self) -> Vec<Arc<dyn PutMessageHook>> {
        todo!()
    }

    fn set_send_message_back_hook(&self, send_message_back_hook: Arc<dyn SendMessageBackHook>) {
        todo!()
    }

    fn get_send_message_back_hook(&self) -> Option<Arc<dyn SendMessageBackHook>> {
        todo!()
    }

    fn get_last_file_from_offset(&self) -> i64 {
        todo!()
    }

    fn get_last_mapped_file(&self, start_offset: i64) -> bool {
        todo!()
    }

    fn set_physical_offset(&self, phy_offset: i64) {
        todo!()
    }

    fn is_mapped_files_empty(&self) -> bool {
        todo!()
    }

    fn get_state_machine_version(&self) -> i64 {
        todo!()
    }

    fn check_message_and_return_size(
        &self,
        bytes: &mut Bytes,
        check_crc: bool,
        check_dup_info: bool,
        read_body: bool,
    ) -> DispatchRequest {
        todo!()
    }

    fn remain_transient_store_buffer_numbs(&self) -> i32 {
        todo!()
    }

    fn remain_how_many_data_to_commit(&self) -> i64 {
        todo!()
    }

    fn remain_how_many_data_to_flush(&self) -> i64 {
        todo!()
    }

    fn is_shutdown(&self) -> bool {
        todo!()
    }

    fn estimate_message_count(
        &self,
        topic: &str,
        queue_id: i32,
        from: i64,
        to: i64,
        filter: &dyn MessageFilter,
    ) -> i64 {
        todo!()
    }

    fn recover_topic_queue_table(&self) {
        todo!()
    }

    fn notify_message_arrive_if_necessary(&self, dispatch_request: &mut DispatchRequest) {
        todo!()
    }
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

struct ReputMessageService {
    shutdown: Arc<Notify>,
    reput_from_offset: Option<Arc<AtomicI64>>,
    message_store_config: Arc<MessageStoreConfig>,
    inner: Option<ReputMessageServiceInner>,
}

impl ReputMessageService {
    fn notify_message_arrive4multi_queue(&self, dispatch_request: &mut DispatchRequest) {
        if dispatch_request.properties_map.is_none()
            || dispatch_request
                .topic
                .as_str()
                .starts_with(RETRY_GROUP_TOPIC_PREFIX)
        {
            return;
        }
        let prop = dispatch_request.properties_map.as_ref().unwrap();
        let multi_dispatch_queue = prop.get(MessageConst::PROPERTY_INNER_MULTI_DISPATCH);
        let multi_queue_offset = prop.get(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        if multi_dispatch_queue.is_none()
            || multi_queue_offset.is_none()
            || multi_dispatch_queue.as_ref().unwrap().is_empty()
            || multi_queue_offset.as_ref().unwrap().is_empty()
        {
            return;
        }
        let queues: Vec<&str> = multi_dispatch_queue
            .unwrap()
            .split(MULTI_DISPATCH_QUEUE_SPLITTER)
            .collect();
        let queue_offsets: Vec<&str> = multi_dispatch_queue
            .unwrap()
            .split(MULTI_DISPATCH_QUEUE_SPLITTER)
            .collect();
        if queues.len() != queue_offsets.len() {
            return;
        }
        let reput_message_service_inner = self.inner.as_ref().unwrap();
        for i in 0..queues.len() {
            let queue_name = CheetahString::from_slice(queues[i]);
            let queue_offset: i64 = queue_offsets[i].parse().unwrap();
            let mut queue_id = dispatch_request.queue_id;
            if self.message_store_config.enable_lmq && is_lmq(Some(queue_name.as_str())) {
                queue_id = 0;
            }
            reput_message_service_inner
                .message_store
                .message_arriving_listener
                .as_ref()
                .unwrap()
                .arriving(
                    &queue_name,
                    queue_id,
                    queue_offset + 1,
                    Some(dispatch_request.tags_code),
                    dispatch_request.store_timestamp,
                    dispatch_request.bit_map.clone(),
                    dispatch_request.properties_map.as_ref(),
                );
        }
    }

    pub fn set_reput_from_offset(&mut self, reput_from_offset: i64) {
        self.reput_from_offset = Some(Arc::new(AtomicI64::new(reput_from_offset)));
    }

    pub fn start(
        &mut self,
        commit_log: Arc<CommitLog>,
        message_store_config: Arc<MessageStoreConfig>,
        dispatcher: CommitLogDispatcherDefault,
        notify_message_arrive_in_batch: bool,
        message_store: ArcMut<LocalFileMessageStore>,
    ) {
        let mut inner = ReputMessageServiceInner {
            reput_from_offset: self.reput_from_offset.clone().unwrap(),
            commit_log,
            message_store_config,
            dispatcher,
            notify_message_arrive_in_batch,
            message_store,
        };
        self.inner = Some(inner.clone());
        let shutdown = self.shutdown.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(1));
            loop {
                tokio::select! {
                    _ = inner.do_reput() => {}
                    _ = shutdown.notified() => {
                        break;
                    }
                }
                interval.tick().await;
            }
        });
    }

    pub fn shutdown(&mut self) {
        let handle = Handle::current();
        let inner = self.inner.as_ref().unwrap().clone();
        let _ = thread::spawn(move || {
            handle.block_on(async move {
                let mut index = 0;
                while index < 50 && inner.is_commit_log_available() {
                    tokio::time::sleep(Duration::from_millis(100)).await;
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
            });
        })
        .join();
        self.shutdown.notify_waiters();
    }
}

//Construct a consumer queue and index file.
#[derive(Clone)]
struct ReputMessageServiceInner {
    reput_from_offset: Arc<AtomicI64>,
    commit_log: Arc<CommitLog>,
    message_store_config: Arc<MessageStoreConfig>,
    dispatcher: CommitLogDispatcherDefault,
    notify_message_arrive_in_batch: bool,
    message_store: ArcMut<LocalFileMessageStore>,
}

impl ReputMessageServiceInner {
    fn notify_message_arrive4multi_queue(&self, dispatch_request: &mut DispatchRequest) {
        let prop = dispatch_request.properties_map.as_ref();
        if prop.is_none() || dispatch_request.topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
            return;
        }
        let prop = prop.unwrap();
        let multi_dispatch_queue = prop.get(MessageConst::PROPERTY_INNER_MULTI_DISPATCH);
        let multi_queue_offset = prop.get(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        if multi_dispatch_queue.is_none()
            || multi_queue_offset.is_none()
            || multi_dispatch_queue.as_ref().unwrap().is_empty()
            || multi_queue_offset.as_ref().unwrap().is_empty()
        {
            return;
        }
        let queues: Vec<&str> = multi_dispatch_queue
            .unwrap()
            .split(MULTI_DISPATCH_QUEUE_SPLITTER)
            .collect();
        let queue_offsets: Vec<&str> = multi_queue_offset
            .unwrap()
            .split(MULTI_DISPATCH_QUEUE_SPLITTER)
            .collect();
        if queues.len() != queue_offsets.len() {
            return;
        }
        for i in 0..queues.len() {
            let queue_name = CheetahString::from_slice(queues[i]);
            let queue_offset: i64 = queue_offsets[i].parse().unwrap();
            let mut queue_id = dispatch_request.queue_id;
            if self.message_store_config.enable_lmq && is_lmq(Some(queue_name.as_str())) {
                queue_id = 0;
            }
            self.message_store
                .message_arriving_listener
                .as_ref()
                .unwrap()
                .arriving(
                    &queue_name,
                    queue_id,
                    queue_offset + 1,
                    Some(dispatch_request.tags_code),
                    dispatch_request.store_timestamp,
                    dispatch_request.bit_map.clone(),
                    dispatch_request.properties_map.as_ref(),
                );
        }
    }

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

                let mut dispatch_request = commit_log::check_message_and_return_size(
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
                            if !self.notify_message_arrive_in_batch {
                                self.message_store
                                    .notify_message_arrive_if_necessary(&mut dispatch_request);
                            }
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

struct FlushConsumeQueueService;

impl FlushConsumeQueueService {
    fn start(&self) {
        error!("flush consume queue service start unimplemented!")
    }

    fn shutdown(&self) {
        error!("flush consume queue service run unimplemented!")
    }
}
