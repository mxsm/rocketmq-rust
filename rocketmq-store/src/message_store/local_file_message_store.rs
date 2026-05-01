// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)]
#![allow(unused_imports)]

use std::any::Any;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt::Write as FmtWrite;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::future::Future;
use std::io::Write;
use std::net::IpAddr;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use bytes::Buf;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use fs2::FileExt;
use rocketmq_common::common::attribute::cleanup_policy::CleanupPolicy;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::is_lmq;
use rocketmq_common::common::mix_all::is_sys_consumer_group_for_no_cold_read_limit;
use rocketmq_common::common::mix_all::LMQ_QUEUE_ID;
use rocketmq_common::common::mix_all::MULTI_DISPATCH_QUEUE_SPLITTER;
use rocketmq_common::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_common::common::running::running_stats::RunningStats;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::common::system_clock::SystemClock;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::utils::queue_type_utils::QueueTypeUtils;
use rocketmq_common::utils::util_all;
use rocketmq_common::CleanupPolicyUtils::get_delete_policy;
use rocketmq_common::CleanupPolicyUtils::get_delete_policy_arc_mut;
use rocketmq_common::FileUtils::string_to_file;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::UtilAll::ensure_dir_ok;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_rust::ArcMut;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
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
use crate::base::message_store::MessageStore;
use crate::base::query_message_result::QueryMessageResult;
use crate::base::select_result::SelectMappedBufferResult;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::base::store_stats_service::StoreStatsService;
use crate::base::transient_store_pool::TransientStorePool;
use crate::config::flush_disk_type::FlushDiskType;
use crate::config::message_store_config::MessageStoreConfig;
use crate::config::store_path_config_helper::get_store_path_batch_consume_queue;
use crate::config::store_path_config_helper::get_store_path_consume_queue_ext;
use crate::filter::ArcMessageFilter;
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
use crate::log_file::MAX_PULL_MSG_SIZE;
use crate::queue::build_consume_queue::CommitLogDispatcherBuildConsumeQueue;
use crate::queue::consume_queue_store::ConsumeQueueStoreTrait;
use crate::queue::local_file_consume_queue_store::ConsumeQueueStore;
use crate::queue::ArcConsumeQueue;
use crate::stats::broker_stats_manager::BrokerStatsManager;
use crate::store::running_flags::RunningFlags;
use crate::store_error::StoreError;
use crate::store_path_config_helper::get_abort_file;
use crate::store_path_config_helper::get_lock_file;
use crate::store_path_config_helper::get_store_checkpoint;
use crate::store_path_config_helper::get_store_path_consume_queue;
use crate::timer::timer_message_store::TimerMessageStore;
use crate::utils::ffi::MADV_NORMAL;
use crate::utils::store_util::TOTAL_PHYSICAL_MEMORY_SIZE;

fn murmur3_x64_128(bytes: &[u8], seed: u32) -> (u64, u64) {
    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;

    let mut h1 = seed as u64;
    let mut h2 = seed as u64;

    let block_count = bytes.len() / 16;
    for index in 0..block_count {
        let offset = index * 16;
        let mut k1 = u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("slice is exactly 8 bytes"));
        let mut k2 = u64::from_le_bytes(
            bytes[offset + 8..offset + 16]
                .try_into()
                .expect("slice is exactly 8 bytes"),
        );

        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;

        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dc_e729);

        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;

        h2 = h2.rotate_left(31);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x3849_5ab5);
    }

    let tail = &bytes[block_count * 16..];
    let mut k1 = 0u64;
    let mut k2 = 0u64;

    for (index, byte) in tail.iter().enumerate() {
        if index < 8 {
            k1 |= (*byte as u64) << (index * 8);
        } else {
            k2 |= (*byte as u64) << ((index - 8) * 8);
        }
    }

    if tail.len() > 8 {
        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;
    }

    if !tail.is_empty() {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= bytes.len() as u64;
    h2 ^= bytes.len() as u64;

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    (h1, h2)
}

fn fmix64(mut value: u64) -> u64 {
    value ^= value >> 33;
    value = value.wrapping_mul(0xff51_afd7_ed55_8ccd);
    value ^= value >> 33;
    value = value.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    value ^= value >> 33;
    value
}

fn murmur3_x64_128_bytes(bytes: &[u8], seed: u32) -> [u8; 16] {
    let (h1, h2) = murmur3_x64_128(bytes, seed);
    let mut result = [0u8; 16];
    result[..8].copy_from_slice(&h1.to_le_bytes());
    result[8..].copy_from_slice(&h2.to_le_bytes());
    result
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum StoreLifecycleState {
    Created = 0,
    Initialized = 1,
    Started = 2,
    Shutdown = 3,
}

impl StoreLifecycleState {
    #[inline]
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Initialized,
            2 => Self::Started,
            3 => Self::Shutdown,
            _ => Self::Created,
        }
    }

    #[inline]
    fn as_u8(self) -> u8 {
        self as u8
    }
}

struct StoreLockGuard {
    file: File,
}

impl Drop for StoreLockGuard {
    fn drop(&mut self) {
        let _ = self.file.sync_all();
        let _ = FileExt::unlock(&self.file);
    }
}

///Using local files to store message data, which is also the default method.
pub struct LocalFileMessageStore {
    message_store_config: Arc<MessageStoreConfig>,
    broker_config: Arc<BrokerConfig>,
    put_message_hook_list: Vec<Arc<dyn PutMessageHook + Send + Sync>>,
    topic_config_table: Arc<DashMap<CheetahString, ArcMut<TopicConfig>>>,
    commit_log: ArcMut<CommitLog>,

    store_checkpoint: Option<Arc<StoreCheckpoint>>,
    master_flushed_offset: Arc<AtomicI64>,
    alive_replica_num_in_group: Arc<AtomicI32>,
    index_service: IndexService,
    allocate_mapped_file_service: Arc<AllocateMappedFileService>,
    consume_queue_store: ConsumeQueueStore,
    dispatcher: ArcMut<CommitLogDispatcherDefault>,
    broker_init_max_offset: Arc<AtomicI64>,
    state_machine_version: Arc<AtomicI64>,
    lifecycle_state: Arc<AtomicU8>,
    controller_epoch_start_offset: Arc<AtomicI64>,
    shutdown: Arc<AtomicBool>,
    store_lock_guard: Option<StoreLockGuard>,
    running_flags: Arc<RunningFlags>,
    reput_message_service: ReputMessageService,
    clean_commit_log_service: Arc<CleanCommitLogService>,
    correct_logic_offset_service: Arc<CorrectLogicOffsetService>,
    clean_consume_queue_service: Arc<CleanConsumeQueueService>,
    broker_stats_manager: Option<Arc<BrokerStatsManager>>,
    message_arriving_listener: Option<Arc<Box<dyn MessageArrivingListener + Sync + Send + 'static>>>,
    notify_message_arrive_in_batch: bool,
    store_stats_service: Arc<StoreStatsService>,

    compaction_store: Arc<CompactionStore>,
    compaction_service: Option<CompactionService>,

    timer_message_store: Option<Arc<TimerMessageStore>>,
    transient_store_pool: TransientStorePool,
    message_store_arc: Option<ArcMut<LocalFileMessageStore>>,
    master_store_in_process: StdRwLock<Option<Arc<dyn Any + Send + Sync>>>,
    send_message_back_hook: StdRwLock<Option<Arc<dyn SendMessageBackHook>>>,
    ha_service: Option<GeneralHAService>,
    flush_consume_queue_service: FlushConsumeQueueService,
    scheduled_task_shutdown: CancellationToken,
    scheduled_task_handles: Vec<JoinHandle<()>>,
    delay_level_table: ArcMut<BTreeMap<i32 /* level */, i64 /* delay timeMillis */>>,
    max_delay_level: i32,
}

fn notify_message_arrive_for_multi_dispatch(
    message_store_config: &MessageStoreConfig,
    message_arriving_listener: &(dyn MessageArrivingListener + Sync + Send + 'static),
    dispatch_request: &mut DispatchRequest,
) {
    let Some(properties) = dispatch_request.properties_map.as_ref() else {
        return;
    };
    if dispatch_request.topic.as_str().starts_with(RETRY_GROUP_TOPIC_PREFIX) {
        return;
    }
    let Some(multi_dispatch_queue) = properties.get(MessageConst::PROPERTY_INNER_MULTI_DISPATCH) else {
        return;
    };
    let Some(multi_queue_offset) = properties.get(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET) else {
        return;
    };
    if multi_dispatch_queue.is_empty() || multi_queue_offset.is_empty() {
        return;
    }

    let mut queue_iter = multi_dispatch_queue.split(MULTI_DISPATCH_QUEUE_SPLITTER);
    let mut offset_iter = multi_queue_offset.split(MULTI_DISPATCH_QUEUE_SPLITTER);
    loop {
        match (queue_iter.next(), offset_iter.next()) {
            (None, None) => break,
            (Some(queue_name), Some(queue_offset)) => {
                if queue_name.is_empty() || queue_offset.parse::<i64>().is_err() {
                    return;
                }
            }
            _ => return,
        }
    }

    for (queue_name, queue_offset) in multi_dispatch_queue
        .split(MULTI_DISPATCH_QUEUE_SPLITTER)
        .zip(multi_queue_offset.split(MULTI_DISPATCH_QUEUE_SPLITTER))
    {
        let Ok(queue_offset) = queue_offset.parse::<i64>() else {
            return;
        };
        let queue_name = CheetahString::from_slice(queue_name);
        let mut queue_id = dispatch_request.queue_id;
        if message_store_config.enable_lmq && is_lmq(Some(queue_name.as_str())) {
            queue_id = 0;
        }
        message_arriving_listener.arriving(
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

impl LocalFileMessageStore {
    fn is_dledger_commit_log_enabled_config(message_store_config: &MessageStoreConfig) -> bool {
        message_store_config.enable_dledger_commit_log || message_store_config.enable_dleger_commit_log
    }

    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        broker_config: Arc<BrokerConfig>,
        topic_config_table: Arc<DashMap<CheetahString, ArcMut<TopicConfig>>>,
        broker_stats_manager: Option<Arc<BrokerStatsManager>>,
        notify_message_arrive_in_batch: bool,
    ) -> Self {
        let (delay_level_table, max_delay_level) = parse_delay_level(message_store_config.message_delay_level.as_str());
        let running_flags = Arc::new(RunningFlags::new());
        let store_checkpoint = Arc::new(
            StoreCheckpoint::new(get_store_checkpoint(message_store_config.store_path_root_dir.as_str())).unwrap(),
        );
        let index_service = IndexService::new(
            message_store_config.clone(),
            store_checkpoint.clone(),
            running_flags.clone(),
        );
        let build_index: Arc<dyn CommitLogDispatcher> = Arc::new(CommitLogDispatcherBuildIndex::new(
            index_service.clone(),
            message_store_config.clone(),
        ));
        let consume_queue_store = ConsumeQueueStore::new(message_store_config.clone(), broker_config.clone());
        let build_consume_queue: Arc<dyn CommitLogDispatcher> =
            Arc::new(CommitLogDispatcherBuildConsumeQueue::new(consume_queue_store.clone()));

        let dispatcher = ArcMut::new(CommitLogDispatcherDefault {
            dispatcher_vec: vec![build_consume_queue, build_index],
        });

        let commit_log = ArcMut::new(CommitLog::new(
            message_store_config.clone(),
            broker_config.clone(),
            dispatcher.clone(),
            store_checkpoint.clone(),
            topic_config_table.clone(),
            consume_queue_store.clone(),
        ));

        ensure_dir_ok(message_store_config.store_path_root_dir.as_str());
        ensure_dir_ok(Self::get_store_path_physic(&message_store_config).as_str());
        ensure_dir_ok(Self::get_store_path_logic(&message_store_config).as_str());

        let identity = broker_config.broker_identity.clone();
        let transient_store_pool = TransientStorePool::new(
            message_store_config.transient_store_pool_size,
            message_store_config.mapped_file_size_commit_log,
        );
        let compaction_service = message_store_config.enable_compaction.then(CompactionService::new);
        Self {
            message_store_config: message_store_config.clone(),
            broker_config,
            put_message_hook_list: vec![],
            topic_config_table,
            // message_store_runtime: Some(RocketMQRuntime::new_multi(10, "message-store-thread")),
            commit_log: commit_log.clone(),
            compaction_service,
            store_checkpoint: Some(store_checkpoint.clone()),
            master_flushed_offset: Arc::new(AtomicI64::new(-1)),
            alive_replica_num_in_group: Arc::new(AtomicI32::new(1)),
            index_service: index_service.clone(),
            allocate_mapped_file_service: Arc::new(AllocateMappedFileService::new()),
            consume_queue_store: consume_queue_store.clone(),
            dispatcher,
            broker_init_max_offset: Arc::new(AtomicI64::new(-1)),
            state_machine_version: Arc::new(AtomicI64::new(0)),
            lifecycle_state: Arc::new(AtomicU8::new(StoreLifecycleState::Created.as_u8())),
            controller_epoch_start_offset: Arc::new(AtomicI64::new(-1)),
            shutdown: Arc::new(AtomicBool::new(false)),
            store_lock_guard: None,
            running_flags: running_flags.clone(),
            reput_message_service: ReputMessageService {
                shutdown_token: CancellationToken::new(),
                new_message_notify: Arc::new(Notify::new()),
                pending_messages: Arc::new(AtomicI64::new(0)),
                reput_from_offset: None,
                dispatch_tx: None,
                inner: None,
                reader_handle: None,
                dispatcher_handle: None,
            },
            clean_commit_log_service: Arc::new(CleanCommitLogService::new(
                message_store_config.clone(),
                commit_log.clone(),
                running_flags.clone(),
            )),
            correct_logic_offset_service: Arc::new(CorrectLogicOffsetService::new(
                commit_log.clone(),
                consume_queue_store.clone(),
            )),
            clean_consume_queue_service: Arc::new(CleanConsumeQueueService::new(
                commit_log.clone(),
                consume_queue_store.clone(),
                index_service.clone(),
            )),
            broker_stats_manager,
            message_arriving_listener: None,
            notify_message_arrive_in_batch,
            store_stats_service: Arc::new(StoreStatsService::new(Some(identity))),
            compaction_store: Arc::new(CompactionStore::new()),
            timer_message_store: None,
            transient_store_pool,
            message_store_arc: None,
            master_store_in_process: StdRwLock::new(None),
            send_message_back_hook: StdRwLock::new(None),
            ha_service: None,
            flush_consume_queue_service: FlushConsumeQueueService::new(
                message_store_config.clone(),
                consume_queue_store.clone(),
                store_checkpoint,
            ),
            scheduled_task_shutdown: CancellationToken::new(),
            scheduled_task_handles: Vec::new(),
            delay_level_table: ArcMut::new(delay_level_table),
            max_delay_level,
        }
    }

    pub fn get_store_path_physic(message_store_config: &Arc<MessageStoreConfig>) -> String {
        message_store_config.get_store_path_commit_log()
    }

    pub fn get_store_path_logic(message_store_config: &Arc<MessageStoreConfig>) -> String {
        get_store_path_consume_queue(message_store_config.store_path_root_dir.as_str())
    }

    pub fn message_store_config(&self) -> Arc<MessageStoreConfig> {
        self.message_store_config.clone()
    }

    pub fn message_store_config_ref(&self) -> &MessageStoreConfig {
        self.message_store_config.as_ref()
    }

    pub fn is_transient_store_pool_enable(&self) -> bool {
        self.message_store_config.transient_store_pool_enable
            && (self.broker_config.enable_controller_mode
                || self.message_store_config().broker_role != BrokerRole::Slave)
    }

    pub fn set_message_store_arc(&mut self, message_store_arc: ArcMut<LocalFileMessageStore>) {
        self.message_store_arc = Some(message_store_arc.clone());
        self.commit_log.set_local_file_message_store(message_store_arc.clone());
        self.consume_queue_store.set_message_store(message_store_arc);
        if self.message_store_config.is_timer_wheel_enable() && self.timer_message_store.is_none() {
            let timer_message_store = Arc::new(TimerMessageStore::new(self.message_store_arc.clone()));
            self.set_timer_message_store(timer_message_store);
        }
    }

    #[inline]
    pub fn delay_level_table(&self) -> &ArcMut<BTreeMap<i32, i64>> {
        &self.delay_level_table
    }

    #[inline]
    pub fn delay_level_table_ref(&self) -> &BTreeMap<i32, i64> {
        self.delay_level_table.as_ref()
    }

    #[inline]
    pub fn max_delay_level(&self) -> i32 {
        self.max_delay_level
    }

    fn enabled_rocksdb_specific_options(message_store_config: &MessageStoreConfig) -> Vec<&'static str> {
        let mut enabled = Vec::new();
        if message_store_config.clean_rocksdb_dirty_cq_interval_min > 0 {
            enabled.push("clean_rocksdb_dirty_cq_interval_min");
        }
        if message_store_config.stat_rocksdb_cq_interval_sec > 0 {
            enabled.push("stat_rocksdb_cq_interval_sec");
        }
        if message_store_config.real_time_persist_rocksdb_config {
            enabled.push("real_time_persist_rocksdb_config");
        }
        if message_store_config.enable_rocksdb_log {
            enabled.push("enable_rocksdb_log");
        }
        if message_store_config.rocksdb_cq_double_write_enable {
            enabled.push("rocksdb_cq_double_write_enable");
        }
        enabled
    }

    fn validate_supported_configuration(&self) -> Result<(), StoreError> {
        if Self::is_dledger_commit_log_enabled_config(self.message_store_config.as_ref()) {
            return Err(StoreError::General(
                "DLedger commit log is Java-specific and is intentionally unsupported in rocketmq-rust".to_string(),
            ));
        }

        let enabled_rocksdb_options = Self::enabled_rocksdb_specific_options(self.message_store_config.as_ref());
        if !self.message_store_config.is_enable_rocksdb_store() && !enabled_rocksdb_options.is_empty() {
            return Err(StoreError::General(format!(
                "RocksDB-specific configuration requires store_type=RocksDB: {}",
                enabled_rocksdb_options.join(", ")
            )));
        }
        Ok(())
    }

    #[inline]
    fn lifecycle_state(&self) -> StoreLifecycleState {
        StoreLifecycleState::from_u8(self.lifecycle_state.load(Ordering::Acquire))
    }

    #[inline]
    fn set_lifecycle_state(&self, state: StoreLifecycleState) {
        self.lifecycle_state.store(state.as_u8(), Ordering::Release);
    }

    #[inline]
    fn is_store_available_for_io(&self) -> bool {
        self.lifecycle_state() != StoreLifecycleState::Shutdown && !self.shutdown.load(Ordering::Acquire)
    }

    fn message_store_arc_or_error(&self, operation: &str) -> Result<ArcMut<LocalFileMessageStore>, StoreError> {
        self.message_store_arc.clone().ok_or_else(|| {
            StoreError::General(format!(
                "message store arc is not set; call set_message_store_arc before {operation}"
            ))
        })
    }

    fn acquire_store_lock(&mut self) -> Result<(), StoreError> {
        if self.store_lock_guard.is_some() {
            return Ok(());
        }

        let lock_path = PathBuf::from(get_lock_file(self.message_store_config.store_path_root_dir.as_str()));
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                StoreError::General(format!(
                    "failed to create store lock parent directory {}: {error}",
                    parent.display()
                ))
            })?;
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|error| {
                StoreError::General(format!(
                    "failed to open store lock file {}: {error}",
                    lock_path.display()
                ))
            })?;
        file.try_lock_exclusive().map_err(|error| {
            StoreError::General(format!(
                "message store lock file is held by another instance: {} ({error})",
                lock_path.display()
            ))
        })?;
        file.set_len(0).map_err(|error| {
            StoreError::General(format!(
                "failed to truncate store lock file {}: {error}",
                lock_path.display()
            ))
        })?;
        writeln!(file, "pid={}", std::process::id()).map_err(|error| {
            StoreError::General(format!(
                "failed to write store lock file {}: {error}",
                lock_path.display()
            ))
        })?;

        self.store_lock_guard = Some(StoreLockGuard { file });
        Ok(())
    }

    fn release_store_lock(&mut self) {
        self.store_lock_guard.take();
    }

    fn should_run_timer_dequeue(&self) -> bool {
        self.message_store_config.is_timer_wheel_enable() && self.message_store_config.broker_role != BrokerRole::Slave
    }

    fn sync_timer_message_store_role(&self) {
        if let Some(timer_message_store) = self.timer_message_store.as_ref() {
            timer_message_store.set_should_running_dequeue(self.should_run_timer_dequeue());
        }
    }

    fn refresh_controller_confirm_offset_after_role_change(&mut self) {
        if !self.broker_config.enable_controller_mode {
            return;
        }

        let min_phy_offset = self.get_min_phy_offset();
        let max_phy_offset = self.get_max_phy_offset().max(min_phy_offset);
        let next_confirm_offset = match self.message_store_config.broker_role {
            BrokerRole::Slave => self.commit_log.get_confirm_offset_directly(),
            _ => self.commit_log.get_confirm_offset(),
        };
        self.set_confirm_offset(next_confirm_offset.clamp(min_phy_offset, max_phy_offset));
    }
}

impl Drop for LocalFileMessageStore {
    fn drop(&mut self) {
        self.release_store_lock();
        // if let Some(runtime) = self.message_store_runtime.take() {
        //     runtime.shutdown();
        // }
    }
}

impl LocalFileMessageStore {
    #[inline]
    pub fn get_topic_config(&self, topic: &CheetahString) -> Option<ArcMut<TopicConfig>> {
        if self.topic_config_table.is_empty() {
            return None;
        }
        self.topic_config_table.get(topic).as_deref().cloned()
    }

    fn delete_topics_inner(&self, delete_topics: &[CheetahString]) -> i32 {
        if delete_topics.is_empty() {
            return 0;
        }

        let mut consume_queue_store = self.consume_queue_store.clone();
        let mut delete_count = 0;
        for topic in delete_topics {
            let queue_table = consume_queue_store.find_consume_queue_map(topic);
            if queue_table.is_none() {
                continue;
            }
            let queue_table = queue_table.unwrap();
            for (queue_id, consume_queue) in queue_table {
                consume_queue_store.destroy_queue(consume_queue.as_ref().deref());
                consume_queue_store.remove_topic_queue_table(topic, queue_id);
            }
            let consume_queue_table = consume_queue_store.get_consume_queue_table();
            consume_queue_table.lock().remove(topic);

            if self.broker_config.auto_delete_unused_stats {
                if let Some(broker_stats_manager) = self.broker_stats_manager.as_ref() {
                    broker_stats_manager.on_topic_deleted(topic);
                }
            }

            let root_dir = self.message_store_config.store_path_root_dir.as_str();
            let consume_queue_dir = PathBuf::from(get_store_path_consume_queue(root_dir)).join(topic.as_str());
            let consume_queue_ext_dir = PathBuf::from(get_store_path_consume_queue_ext(root_dir)).join(topic.as_str());
            let batch_consume_queue_dir =
                PathBuf::from(get_store_path_batch_consume_queue(root_dir)).join(topic.as_str());

            util_all::delete_empty_directory(consume_queue_dir);
            util_all::delete_empty_directory(consume_queue_ext_dir);
            util_all::delete_empty_directory(batch_consume_queue_dir);
            info!("DeleteTopic: Topic has been destroyed, topic={}", topic);
            delete_count += 1;
        }

        delete_count
    }

    fn prepare_lmq_dispatch(&self, msg: &mut MessageExtBrokerInner) -> Vec<String> {
        if !self.message_store_config.enable_multi_dispatch {
            return Vec::new();
        }
        let Some(multi_dispatch_queue) = msg.property(MessageConst::PROPERTY_INNER_MULTI_DISPATCH) else {
            return Vec::new();
        };
        if multi_dispatch_queue.is_empty() {
            return Vec::new();
        }
        let (queue_keys, is_all_lmq_dispatch) =
            self.collect_lmq_dispatch_queue_keys_from_value(multi_dispatch_queue.as_str());
        if msg
            .property(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET)
            .is_some_and(|queue_offset| !queue_offset.is_empty())
        {
            return queue_keys;
        }
        if !is_all_lmq_dispatch {
            return Vec::new();
        }

        let mut queue_offsets = String::new();
        for (index, queue_key) in queue_keys.iter().enumerate() {
            if index > 0 {
                queue_offsets.push_str(MULTI_DISPATCH_QUEUE_SPLITTER);
            }
            let _ = write!(
                &mut queue_offsets,
                "{}",
                self.consume_queue_store.get_lmq_queue_offset(queue_key.as_str())
            );
        }
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET),
            CheetahString::from_string(queue_offsets),
        );
        queue_keys
    }

    fn collect_lmq_dispatch_queue_keys_from_value(&self, multi_dispatch_queue: &str) -> (Vec<String>, bool) {
        if !self.message_store_config.enable_lmq {
            return (Vec::new(), false);
        }

        let mut queue_keys = Vec::new();
        let mut saw_queue = false;
        let mut is_all_lmq_dispatch = true;
        for queue_name in multi_dispatch_queue.split(MULTI_DISPATCH_QUEUE_SPLITTER) {
            if queue_name.is_empty() {
                is_all_lmq_dispatch = false;
                continue;
            }
            saw_queue = true;
            if is_lmq(Some(queue_name)) {
                queue_keys.push(format!("{queue_name}-{LMQ_QUEUE_ID}"));
            } else {
                is_all_lmq_dispatch = false;
            }
        }
        (queue_keys, saw_queue && is_all_lmq_dispatch)
    }

    fn update_lmq_offsets(&self, queue_keys: &[String], message_num: i16) {
        for queue_key in queue_keys {
            self.consume_queue_store
                .increase_lmq_offset(queue_key.as_str(), message_num);
        }
    }

    fn get_lmq_dispatch_message_num(&self, msg: &MessageExtBrokerInner) -> i16 {
        msg.property(MessageConst::PROPERTY_INNER_NUM)
            .and_then(|message_num| message_num.parse::<i16>().ok())
            .unwrap_or(1)
    }

    fn is_temp_file_exist(&self) -> bool {
        let file_name = get_abort_file(self.message_store_config.store_path_root_dir.as_str());
        Path::new(&file_name).exists()
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
            if recover_concurrently { "concurrent" } else { "normal" },
        );
        let recover_consume_queue_start = Instant::now();
        self.recover_consume_queue().await;
        let dispatch_recovery_offset = self.get_dispatch_recovery_offset();
        let recover_consume_queue = Instant::now()
            .saturating_duration_since(recover_consume_queue_start)
            .as_millis();

        let recover_commit_log_start = Instant::now();
        if last_exit_ok {
            self.recover_normally(dispatch_recovery_offset).await;
        } else {
            self.recover_abnormally(dispatch_recovery_offset).await;
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
            "message store recover total cost: {} ms, recoverConsumeQueue: {} ms, recoverCommitLog: {} ms, \
             recoverOffsetTable: {} ms",
            recover_consume_queue + recover_commit_log + recover_topic_queue_table,
            recover_consume_queue,
            recover_commit_log,
            recover_topic_queue_table
        );
    }

    pub async fn recover_normally(&mut self, max_phy_offset_of_consume_queue: i64) {
        // Check if optimized recovery is enabled (default: true)
        let use_optimized = std::env::var("ROCKETMQ_USE_OPTIMIZED_RECOVERY")
            .unwrap_or_else(|_| "true".to_string())
            .parse::<bool>()
            .unwrap_or(true);

        if use_optimized {
            self.commit_log
                .recover_normally_optimized(max_phy_offset_of_consume_queue, self.message_store_arc.clone().unwrap())
                .await;
        } else {
            self.commit_log
                .recover_normally(max_phy_offset_of_consume_queue, self.message_store_arc.clone().unwrap())
                .await;
        }
    }

    pub async fn recover_abnormally(&mut self, max_phy_offset_of_consume_queue: i64) {
        // Check if optimized recovery is enabled (default: true)
        let use_optimized = std::env::var("ROCKETMQ_USE_OPTIMIZED_RECOVERY")
            .unwrap_or_else(|_| "true".to_string())
            .parse::<bool>()
            .unwrap_or(true);

        if use_optimized {
            self.commit_log
                .recover_abnormally_optimized(max_phy_offset_of_consume_queue, self.message_store_arc.clone().unwrap())
                .await;
        } else {
            self.commit_log
                .recover_abnormally(max_phy_offset_of_consume_queue, self.message_store_arc.clone().unwrap())
                .await;
        }
    }

    fn is_recover_concurrently(&self) -> bool {
        self.broker_config.recover_concurrently & self.message_store_config.is_enable_rocksdb_store()
    }

    async fn recover_consume_queue(&mut self) {
        if self.is_recover_concurrently() {
            self.consume_queue_store.recover_concurrently().await;
        } else {
            self.consume_queue_store.recover().await;
        }
    }

    pub fn on_commit_log_dispatch(
        &mut self,
        dispatch_request: &mut DispatchRequest,
        do_dispatch: bool,
        is_recover: bool,
        _is_file_end: bool,
    ) {
        if do_dispatch && !is_recover {
            self.do_dispatch(dispatch_request);
        }
    }

    pub fn do_dispatch(&mut self, dispatch_request: &mut DispatchRequest) {
        self.dispatcher.dispatch(dispatch_request)
    }

    /*    pub fn truncate_dirty_logic_files(&mut self, phy_offset: i64) {
        self.consume_queue_store.truncate_dirty(phy_offset);
    }*/

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

    fn add_schedule_task(&mut self) {
        if !self.scheduled_task_handles.is_empty() {
            return;
        }

        self.scheduled_task_shutdown = CancellationToken::new();

        // clean files  Periodically
        let clean_commit_log_service_arc = self.clean_commit_log_service.clone();
        let clean_resource_interval = self.message_store_config.clean_resource_interval as u64;
        let shutdown_token = self.scheduled_task_shutdown.clone();
        self.scheduled_task_handles.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(clean_resource_interval.max(1)));
            loop {
                tokio::select! {
                    _ = shutdown_token.cancelled() => break,
                    _ = interval.tick() => {
                        let service = Arc::clone(&clean_commit_log_service_arc);
                        if !run_blocking_scheduled_task("clean commit log", move || service.run()).await {
                            break;
                        }
                    },
                }
            }
        }));

        let message_store = self.message_store_arc.clone().unwrap();
        let shutdown_token = self.scheduled_task_shutdown.clone();
        self.scheduled_task_handles.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10 * 60));
            loop {
                tokio::select! {
                    _ = shutdown_token.cancelled() => break,
                    _ = interval.tick() => {
                        let message_store = message_store.clone();
                        if !run_blocking_scheduled_task("store self check", move || message_store.check_self()).await {
                            break;
                        }
                    },
                }
            }
        }));

        // store check point flush
        let store_checkpoint_arc = self.store_checkpoint.clone().unwrap();
        let shutdown_token = self.scheduled_task_shutdown.clone();
        self.scheduled_task_handles.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = shutdown_token.cancelled() => break,
                    _ = interval.tick() => {
                        let checkpoint = Arc::clone(&store_checkpoint_arc);
                        if !run_blocking_scheduled_task("store checkpoint flush", move || {
                            let _ = checkpoint.flush();
                        }).await {
                            break;
                        }
                    }
                }
            }
        }));

        let correct_logic_offset_service_arc = self.correct_logic_offset_service.clone();
        let clean_consume_queue_service_arc = self.clean_consume_queue_service.clone();
        let shutdown_token = self.scheduled_task_shutdown.clone();
        self.scheduled_task_handles.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(clean_resource_interval.max(1)));
            loop {
                tokio::select! {
                    _ = shutdown_token.cancelled() => break,
                    _ = interval.tick() => {
                        let correct_service = Arc::clone(&correct_logic_offset_service_arc);
                        let clean_service = Arc::clone(&clean_consume_queue_service_arc);
                        if !run_blocking_scheduled_task("clean consume queue", move || {
                            correct_service.run();
                            clean_service.run();
                        }).await {
                            break;
                        }
                    }
                }
            }
        }));
    }

    async fn shutdown_schedule_tasks(&mut self) {
        self.scheduled_task_shutdown.cancel();
        for handle in self.scheduled_task_handles.drain(..) {
            if let Err(error) = handle.await {
                if !error.is_cancelled() {
                    error!("scheduled store task failed during shutdown: {error}");
                }
            }
        }
    }

    fn check_self(&self) {
        self.commit_log.check_self();
        ConsumeQueueStoreTrait::check_self(&self.consume_queue_store);
    }

    fn get_dispatch_recovery_offset(&self) -> i64 {
        let commit_log_min_offset = self.commit_log.get_min_offset();
        let dispatch_recovery_offset = self
            .dispatcher
            .min_dispatch_progress_offset(commit_log_min_offset)
            .unwrap_or(commit_log_min_offset)
            .max(commit_log_min_offset);
        let controller_epoch_start_offset = self.controller_epoch_start_offset.load(Ordering::SeqCst);
        if controller_epoch_start_offset >= 0 {
            dispatch_recovery_offset.max(controller_epoch_start_offset.max(commit_log_min_offset))
        } else {
            dispatch_recovery_offset
        }
    }

    pub fn set_state_machine_version(&mut self, state_machine_version: i64) {
        self.state_machine_version
            .store(state_machine_version, Ordering::SeqCst);
    }

    pub fn set_controller_epoch_start_offset(&mut self, epoch_start_offset: i64) {
        self.controller_epoch_start_offset
            .store(epoch_start_offset, Ordering::SeqCst);
    }

    pub fn get_controller_epoch_start_offset(&self) -> i64 {
        self.controller_epoch_start_offset.load(Ordering::SeqCst)
    }

    pub fn next_offset_correction(&self, old_offset: i64, new_offset: i64) -> i64 {
        let mut next_offset = old_offset;
        if self.message_store_config.broker_role != BrokerRole::Slave || self.message_store_config.offset_check_in_slave
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
        message_arriving_listener: Option<Arc<Box<dyn MessageArrivingListener + Sync + Send + 'static>>>,
    ) {
        self.message_arriving_listener = message_arriving_listener;
    }

    fn do_recheck_reput_offset_from_dispatchers(&self) {
        let Some(reput_from_offset) = self.reput_message_service.reput_from_offset.as_ref() else {
            return;
        };

        let commit_log_confirm_offset = self.commit_log.get_confirm_offset();
        let dispatch_recovery_offset = self.get_dispatch_recovery_offset();
        let target_reput_from_offset = dispatch_recovery_offset
            .min(commit_log_confirm_offset)
            .max(self.get_controller_epoch_start_offset().max(0));
        let previous_reput_from_offset = reput_from_offset.swap(target_reput_from_offset, Ordering::SeqCst);

        if previous_reput_from_offset != target_reput_from_offset {
            info!(
                "rechecked reputFromOffset from {} to {} using dispatch recovery offset {}",
                previous_reput_from_offset, target_reput_from_offset, dispatch_recovery_offset
            );
        }
    }

    pub fn get_message_store_config(&self) -> Arc<MessageStoreConfig> {
        self.message_store_config.clone()
    }

    pub async fn reput_once(&mut self) {
        if self.reput_message_service.reput_from_offset.is_none() {
            let start_offset = self.get_dispatch_recovery_offset().max(0);
            self.reput_message_service.set_reput_from_offset(start_offset);
        }
        let Some(message_store) = self.message_store_arc.clone() else {
            return;
        };
        self.reput_message_service
            .run_once(
                self.commit_log.clone(),
                self.message_store_config.clone(),
                self.dispatcher.clone(),
                self.notify_message_arrive_in_batch,
                message_store,
            )
            .await;
    }
}

fn estimate_in_mem_by_commit_offset(
    offset_py: i64,
    max_offset_py: i64,
    message_store_config: &Arc<MessageStoreConfig>,
) -> bool {
    let memory =
        (*TOTAL_PHYSICAL_MEMORY_SIZE as f64) * (message_store_config.access_message_in_memory_max_ratio as f64 / 100.0);
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
        if (buffer_total + size_py) as u64 > message_store_config.max_transfer_bytes_on_message_in_memory {
            return true;
        }

        message_total as u64 > message_store_config.max_transfer_count_on_message_in_memory - 1
    } else {
        if (buffer_total + size_py) as u64 > message_store_config.max_transfer_bytes_on_message_in_disk {
            return true;
        }

        message_total as u64 > message_store_config.max_transfer_count_on_message_in_disk - 1
    }
}

#[allow(unused_variables)]
#[allow(unused_assignments)]
impl MessageStore for LocalFileMessageStore {
    async fn load(&mut self) -> bool {
        let last_exit_ok = !self.is_temp_file_exist();
        info!(
            "last shutdown {}, store path root dir: {}",
            if last_exit_ok { "normally" } else { "abnormally" },
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
            let Some(compaction_service) = self.compaction_service.as_mut() else {
                error!("compaction is enabled but compaction service is not initialized");
                return false;
            };
            result &= compaction_service.load(last_exit_ok);
            if !result {
                return result;
            }
        }

        if result {
            let checkpoint = self.store_checkpoint.as_ref().unwrap();
            self.master_flushed_offset = Arc::new(AtomicI64::new(checkpoint.master_flushed_offset() as i64));
            self.set_confirm_offset(checkpoint.confirm_phy_offset() as i64);
            result = self.index_service.load(last_exit_ok);

            //recover commit log and consume queue
            self.recover(last_exit_ok).await;
            info!(
                "message store recover end, and the max phy offset = {}",
                self.get_max_phy_offset()
            );
        }

        if result {
            if let Some(timer_message_store) = self.timer_message_store.as_ref() {
                result &= timer_message_store.load();
            }
        }

        let max_offset = self.get_max_phy_offset();
        self.set_broker_init_max_offset(max_offset);
        info!("load over, and the max phy offset = {}", max_offset);

        if !result {
            // self.allocate_mapped_file_service.shutdown();
        }
        result
    }

    async fn start(&mut self) -> Result<(), StoreError> {
        self.validate_supported_configuration()?;
        match self.lifecycle_state() {
            StoreLifecycleState::Initialized => {}
            StoreLifecycleState::Created => {
                return Err(StoreError::General(
                    "message store must be initialized before start".to_string(),
                ));
            }
            StoreLifecycleState::Started => {
                return Err(StoreError::General("message store is already started".to_string()));
            }
            StoreLifecycleState::Shutdown => {
                return Err(StoreError::General(
                    "message store is shutdown; call init before start".to_string(),
                ));
            }
        }

        self.acquire_store_lock()?;
        let start_result: Result<(), StoreError> = async {
            self.allocate_mapped_file_service.start();

            self.index_service.start();

            self.reput_message_service
                .set_reput_from_offset(self.commit_log.get_confirm_offset());
            let message_store_arc = self.message_store_arc_or_error("start")?;
            self.reput_message_service.start(
                self.commit_log.clone(),
                self.message_store_config.clone(),
                self.dispatcher.clone(),
                self.notify_message_arrive_in_batch,
                message_store_arc,
            );
            self.do_recheck_reput_offset_from_dispatchers();
            self.flush_consume_queue_service.start();
            self.commit_log.start();
            self.consume_queue_store.start();
            self.store_stats_service.start();
            if let Some(timer_message_store) = self.timer_message_store.as_ref() {
                timer_message_store.start();
            }
            self.sync_timer_message_store_role();

            if let Some(ha_service) = self.ha_service.as_mut() {
                ha_service.start().await.map_err(|e| {
                    error!("HA service start failed: {:?}", e);
                    StoreError::General(e.to_string())
                })?;
            }
            self.create_temp_file();
            self.add_schedule_task();
            // self.perfs.start();
            Ok(())
        }
        .await;

        match start_result {
            Ok(()) => {
                self.shutdown.store(false, Ordering::Release);
                self.set_lifecycle_state(StoreLifecycleState::Started);
                Ok(())
            }
            Err(error) => {
                self.release_store_lock();
                Err(error)
            }
        }
    }

    async fn init(&mut self) -> Result<(), StoreError> {
        self.validate_supported_configuration()?;
        match self.lifecycle_state() {
            StoreLifecycleState::Created | StoreLifecycleState::Shutdown => {}
            StoreLifecycleState::Initialized => return Ok(()),
            StoreLifecycleState::Started => {
                return Err(StoreError::General(
                    "message store cannot be initialized while started".to_string(),
                ));
            }
        }

        if !Self::is_dledger_commit_log_enabled_config(self.message_store_config.as_ref())
            && !self.message_store_config.duplication_enable
        {
            let message_store_arc = self.message_store_arc_or_error("init")?;
            if self.message_store_config.enable_controller_mode {
                let mut auto_switch_ha_service = GeneralHAService::AutoSwitchHAService(ArcMut::new(
                    crate::ha::auto_switch::auto_switch_ha_service::AutoSwitchHAService::new(message_store_arc),
                ));
                let _ = auto_switch_ha_service.init();
                self.ha_service = Some(auto_switch_ha_service);
            } else {
                let mut default_ha_service = GeneralHAService::DefaultHAService(ArcMut::new(
                    crate::ha::default_ha_service::DefaultHAService::new(message_store_arc),
                ));
                let _ = default_ha_service.init();
                self.ha_service = Some(default_ha_service);
            }
        }

        if self.is_transient_store_pool_enable() {
            match self.transient_store_pool.init() {
                Ok(_) => {}
                Err(e) => return Err(StoreError::General(e.to_string())),
            }
        }
        self.shutdown.store(false, Ordering::Release);
        self.set_lifecycle_state(StoreLifecycleState::Initialized);
        Ok(())
    }

    async fn shutdown(&mut self) {
        let previous_state = self.lifecycle_state();
        if !self.shutdown.load(Ordering::Acquire) {
            self.shutdown.store(true, Ordering::Release);
            self.set_lifecycle_state(StoreLifecycleState::Shutdown);

            if matches!(
                previous_state,
                StoreLifecycleState::Created | StoreLifecycleState::Shutdown
            ) {
                self.release_store_lock();
                let _ = self.transient_store_pool.destroy();
                return;
            }

            if let Some(ha_service) = self.ha_service.as_ref() {
                ha_service.shutdown().await;
            }

            self.shutdown_schedule_tasks().await;
            self.store_stats_service.shutdown_gracefully().await;
            self.commit_log.shutdown_gracefully().await;

            self.reput_message_service.shutdown().await;
            self.consume_queue_store.shutdown();

            // dispatch-related services must be shut down after reputMessageService
            self.index_service.shutdown();

            if let Some(compaction_service) = self.compaction_service.as_ref() {
                compaction_service.shutdown();
            }

            if self.message_store_config.rocksdb_cq_double_write_enable {
                // this.rocksDBMessageStore.consumeQueueStore.shutdown();
            }
            if let Some(timer_message_store) = self.timer_message_store.as_ref() {
                timer_message_store.shutdown_gracefully().await;
            }
            self.flush_consume_queue_service.shutdown();
            self.allocate_mapped_file_service.shutdown().await;
            if let Some(store_checkpoint) = self.store_checkpoint.as_ref() {
                let _ = store_checkpoint.shutdown();
            }
            if self.running_flags.is_writeable() && self.dispatch_behind_bytes() == 0 {
                //delete abort file
                self.delete_file(get_abort_file(self.message_store_config.store_path_root_dir.as_str()))
            }
        }

        self.release_store_lock();
        let _ = self.transient_store_pool.destroy();
    }

    fn destroy(&mut self) {
        self.release_store_lock();
        self.consume_queue_store.destroy();
        self.commit_log.destroy();
        self.index_service.destroy();
        self.delete_file(get_abort_file(self.message_store_config.store_path_root_dir.as_str()));
        self.delete_file(get_store_checkpoint(
            self.message_store_config.store_path_root_dir.as_str(),
        ));
    }

    async fn put_message(&mut self, mut msg: MessageExtBrokerInner) -> PutMessageResult {
        if !self.is_store_available_for_io() {
            warn!("message store has shutdown, so putMessage is forbidden");
            return PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable);
        }

        for hook in self.put_message_hook_list.iter() {
            if let Some(result) = hook.execute_before_put_message(&mut msg) {
                return result;
            }
        }
        let lmq_dispatch_queue_keys = self.prepare_lmq_dispatch(&mut msg);
        let lmq_dispatch_message_num = self.get_lmq_dispatch_message_num(&msg);

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
            if !QueueTypeUtils::is_batch_cq_arc_mut(topic_config.as_ref()) {
                error!("[BUG]The message is an inner batch but cq type is not batch cq");
                return PutMessageResult::new_default(PutMessageStatus::MessageIllegal);
            }
        }
        let begin_time = Instant::now();
        //put message to commit log
        let result = self.commit_log.put_message(msg).await;
        let elapsed_time = begin_time.elapsed().as_millis();
        if elapsed_time > 500 {
            warn!(
                "DefaultMessageStore#putMessage: CommitLog#putMessage cost {}ms",
                elapsed_time,
            );
        }
        self.store_stats_service
            .set_put_message_entire_time_max(elapsed_time as u64);
        if !result.is_ok() {
            self.store_stats_service
                .get_put_message_failed_times()
                .fetch_add(1, Ordering::AcqRel);
        }

        // Notify ReputMessageService that new message has arrived
        if result.is_ok() {
            if !lmq_dispatch_queue_keys.is_empty() {
                self.update_lmq_offsets(&lmq_dispatch_queue_keys, lmq_dispatch_message_num);
            }
            self.reput_message_service.notify_new_message();
        }

        result
    }

    async fn put_messages(&mut self, mut message_ext_batch: MessageExtBatch) -> PutMessageResult {
        if !self.is_store_available_for_io() {
            warn!("message store has shutdown, so putMessages is forbidden");
            return PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable);
        }

        for hook in self.put_message_hook_list.iter() {
            if let Some(result) = hook.execute_before_put_message(&mut message_ext_batch.message_ext_broker_inner) {
                return result;
            }
        }
        let lmq_dispatch_queue_keys = self.prepare_lmq_dispatch(&mut message_ext_batch.message_ext_broker_inner);
        let lmq_dispatch_message_num = self.get_lmq_dispatch_message_num(&message_ext_batch.message_ext_broker_inner);

        let begin_time = Instant::now();
        //put message to commit log
        let result = self.commit_log.put_messages(message_ext_batch).await;
        let elapsed_time = begin_time.elapsed().as_millis();
        if elapsed_time > 500 {
            warn!("not in lock eclipse time(ms) {}ms", elapsed_time,);
        }
        self.store_stats_service
            .set_put_message_entire_time_max(elapsed_time as u64);
        if !result.is_ok() {
            self.store_stats_service
                .get_put_message_failed_times()
                .fetch_add(1, Ordering::Relaxed);
        }

        // Notify ReputMessageService that new messages have arrived
        if result.is_ok() {
            if !lmq_dispatch_queue_keys.is_empty() {
                self.update_lmq_offsets(&lmq_dispatch_queue_keys, lmq_dispatch_message_num);
            }
            self.reput_message_service.notify_new_message();
        }

        result
    }

    async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Option<GetMessageResult> {
        self.get_message_with_size_limit(
            group,
            topic,
            queue_id,
            offset,
            max_msg_nums,
            MAX_PULL_MSG_SIZE,
            message_filter,
        )
        .await
    }

    async fn get_message_with_size_limit(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Option<GetMessageResult> {
        if self.lifecycle_state() == StoreLifecycleState::Shutdown {
            warn!("message store has shutdown, so getMessage is forbidden");
            return None;
        }

        if self.shutdown.load(Ordering::Relaxed) {
            warn!("message store has shutdown, so getMessage is forbidden");
            return None;
        }

        if !self.running_flags.is_readable() {
            warn!(
                "message store is not readable, so getMessage is forbidden {}",
                self.running_flags.get_flag_bits()
            );
            return None;
        }
        let topic_config = self.get_topic_config(topic);
        let policy = get_delete_policy_arc_mut(topic_config.as_ref());
        if policy == CleanupPolicy::COMPACTION && self.message_store_config.enable_compaction {
            if let Some(result) =
                self.compaction_store
                    .get_message(group, topic, queue_id, offset, max_msg_nums, max_total_msg_size)
            {
                return Some(result);
            }
        }
        let begin_time = Instant::now();

        let mut status = GetMessageStatus::NoMessageInQueue;

        let mut next_begin_offset = offset;
        let mut min_offset = 0;
        let mut max_offset = 0;
        let mut get_result = Some(GetMessageResult::new());
        let max_offset_py = self.commit_log.get_max_offset();
        let consume_queue = self.find_consume_queue(topic, queue_id);
        if let Some(consume_queue) = consume_queue {
            min_offset = consume_queue.get_min_offset_in_queue();
            max_offset = consume_queue.get_max_offset_in_queue();
            if max_offset == 0 {
                status = GetMessageStatus::NoMessageInQueue;
                next_begin_offset = self.next_offset_correction(offset, 0);
            } else if offset < min_offset {
                status = GetMessageStatus::OffsetTooSmall;
                next_begin_offset = self.next_offset_correction(offset, min_offset);
            } else if offset == max_offset {
                status = GetMessageStatus::OffsetOverflowOne;
                next_begin_offset = self.next_offset_correction(offset, offset);
            } else if offset > max_offset {
                status = GetMessageStatus::OffsetOverflowBadly;
                next_begin_offset = self.next_offset_correction(offset, max_offset);
            } else {
                let max_filter_message_size = self
                    .message_store_config
                    .max_filter_message_size
                    .max(max_msg_nums * consume_queue.get_unit_size());
                let disk_fall_recorded = self.message_store_config.disk_fall_recorded;
                let mut max_pull_size = max_total_msg_size.max(100);
                if max_pull_size > MAX_PULL_MSG_SIZE {
                    warn!(
                        "The max pull size is too large maxPullSize={} topic={} queueId={}",
                        max_pull_size, topic, queue_id
                    );
                    max_pull_size = MAX_PULL_MSG_SIZE;
                }
                status = GetMessageStatus::NoMatchedMessage;
                let mut max_phy_offset_pulling = 0;
                let mut cq_file_num = 0;
                while get_result.as_ref().unwrap().buffer_total_size() <= 0
                    && next_begin_offset < max_offset
                    && cq_file_num < self.message_store_config.travel_cq_file_num_when_get_message
                {
                    cq_file_num += 1;
                    let buffer_consume_queue = consume_queue.iterate_from_with_count(next_begin_offset, max_msg_nums);
                    if buffer_consume_queue.is_none() {
                        status = GetMessageStatus::OffsetFoundNull;
                        next_begin_offset = self.next_offset_correction(
                            next_begin_offset,
                            self.consume_queue_store
                                .roll_next_file(&**consume_queue, next_begin_offset),
                        );
                        warn!(
                            "consumer request topic: {}, offset: {}, minOffset: {}, maxOffset: {}, but access logic \
                             queue failed. Correct nextBeginOffset to {}",
                            topic, offset, min_offset, max_offset, next_begin_offset
                        );
                        break;
                    }
                    let mut next_phy_file_start_offset = i64::MIN;
                    let mut buffer_consume_queue = buffer_consume_queue.unwrap();
                    loop {
                        if next_begin_offset >= max_offset {
                            break;
                        }
                        if let Some(cq_unit) = buffer_consume_queue.next() {
                            let offset_py = cq_unit.pos;
                            let size_py = cq_unit.size;
                            let is_in_mem =
                                estimate_in_mem_by_commit_offset(offset_py, max_offset_py, &self.message_store_config);
                            if (cq_unit.queue_offset - offset) * consume_queue.get_unit_size() as i64
                                > max_filter_message_size as i64
                            {
                                break;
                            }
                            let get_result_ref = get_result.as_mut().unwrap();
                            if is_the_batch_full(
                                size_py,
                                cq_unit.batch_num as i32,
                                max_msg_nums,
                                max_pull_size as i64,
                                get_result_ref.buffer_total_size(),
                                get_result_ref.message_count(),
                                is_in_mem,
                                &self.message_store_config,
                            ) {
                                break;
                            }
                            if get_result_ref.buffer_total_size() >= max_pull_size {
                                break;
                            }
                            max_phy_offset_pulling = offset_py;
                            next_begin_offset = cq_unit.queue_offset + cq_unit.batch_num as i64;
                            if next_phy_file_start_offset != i64::MIN && offset_py < next_phy_file_start_offset {
                                continue;
                            }

                            if let Some(filter) = message_filter.as_ref() {
                                if !filter.is_matched_by_consume_queue(
                                    cq_unit.get_valid_tags_code_as_long(),
                                    cq_unit.cq_ext_unit.as_ref(),
                                ) {
                                    if get_result_ref.buffer_total_size() == 0 {
                                        status = GetMessageStatus::NoMatchedMessage;
                                    }
                                    continue;
                                }
                            }

                            let select_result = self.commit_log.get_message(offset_py, size_py);
                            if select_result.is_none() {
                                if get_result_ref.buffer_total_size() == 0 {
                                    status = GetMessageStatus::MessageWasRemoving;
                                }
                                next_phy_file_start_offset = self.commit_log.roll_next_file(offset_py);
                                continue;
                            }
                            if self.message_store_config.cold_data_flow_control_enable
                                && !is_sys_consumer_group_for_no_cold_read_limit(group)
                                && !select_result.as_ref().unwrap().is_in_cache
                            {
                                get_result_ref.set_cold_data_sum(get_result_ref.cold_data_sum() + size_py as i64);
                            }

                            if message_filter.is_some()
                                && !message_filter
                                    .as_ref()
                                    .as_ref()
                                    .unwrap()
                                    .is_matched_by_commit_log(Some(select_result.as_ref().unwrap().get_buffer()), None)
                            {
                                if get_result_ref.buffer_total_size() == 0 {
                                    status = GetMessageStatus::NoMatchedMessage;
                                }
                                drop(select_result);
                                continue;
                            }
                            self.store_stats_service
                                .get_message_transferred_msg_count()
                                .fetch_add(cq_unit.batch_num as usize, Ordering::Relaxed);
                            get_result.as_mut().unwrap().add_message(
                                select_result.unwrap(),
                                cq_unit.queue_offset as u64,
                                cq_unit.batch_num as i32,
                            );
                            status = GetMessageStatus::Found;
                            next_phy_file_start_offset = i64::MIN;
                        }
                    }
                }
                if disk_fall_recorded {
                    let fall_behind = max_offset_py - max_phy_offset_pulling;
                    self.broker_stats_manager
                        .as_ref()
                        .unwrap()
                        .record_disk_fall_behind_size(group, topic, queue_id, fall_behind);
                }
                let diff = max_offset_py - max_phy_offset_pulling;
                let memory = ((*TOTAL_PHYSICAL_MEMORY_SIZE as f64)
                    * (self.message_store_config.access_message_in_memory_max_ratio as f64 / 100.0))
                    as i64;
                get_result
                    .as_mut()
                    .unwrap()
                    .set_suggest_pulling_from_slave(diff > memory);
            }
        } else {
            status = GetMessageStatus::NoMatchedLogicQueue;
            next_begin_offset = self.next_offset_correction(offset, 0);
        }

        if GetMessageStatus::Found == status {
            self.store_stats_service
                .get_message_times_total_found()
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.store_stats_service
                .get_message_times_total_miss()
                .fetch_add(1, Ordering::Relaxed);
        }
        let elapsed_time = begin_time.elapsed().as_millis() as u64;
        self.store_stats_service.set_get_message_entire_time_max(elapsed_time);
        if get_result.is_none() {
            get_result = Some(GetMessageResult::new_result_size(0));
        }
        let result = get_result.as_mut().unwrap();
        result.set_status(Some(status));
        result.set_next_begin_offset(next_begin_offset);
        result.set_max_offset(max_offset);
        result.set_min_offset(min_offset);

        get_result
    }

    /*    async fn get_message_with_size_limit_async(
        &self,
        group: &str,
        topic: &str,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: &dyn MessageFilter,
    ) -> Result<GetMessageResult, StoreError> {

    }*/

    fn get_max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        self.get_max_offset_in_queue_committed(topic, queue_id, true)
    }

    fn get_max_offset_in_queue_committed(&self, topic: &CheetahString, queue_id: i32, committed: bool) -> i64 {
        if committed {
            let queue = self.consume_queue_store.find_or_create_consume_queue(topic, queue_id);

            queue.get_max_offset_in_queue()
        } else {
            self.consume_queue_store
                .get_max_offset(topic, queue_id)
                .unwrap_or_default()
        }
    }

    #[inline]
    fn get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        self.consume_queue_store.get_min_offset_in_queue(topic, queue_id)
    }

    #[inline]
    fn get_timer_message_store(&self) -> Option<&Arc<TimerMessageStore>> {
        self.timer_message_store.as_ref()
    }

    #[inline]
    fn set_timer_message_store(&mut self, timer_message_store: Arc<TimerMessageStore>) {
        self.timer_message_store = Some(timer_message_store);
    }

    fn get_commit_log_offset_in_queue(&self, topic: &CheetahString, queue_id: i32, consume_queue_offset: i64) -> i64 {
        self.get_consume_queue(topic, queue_id)
            .and_then(|consume_queue| consume_queue.get(consume_queue_offset).map(|cq_unit| cq_unit.pos))
            .unwrap_or_default()
    }

    fn get_offset_in_queue_by_time(&self, topic: &CheetahString, queue_id: i32, timestamp: i64) -> i64 {
        self.get_offset_in_queue_by_time_with_boundary(topic, queue_id, timestamp, BoundaryType::Lower)
    }

    fn get_offset_in_queue_by_time_with_boundary(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64 {
        self.consume_queue_store
            .get_offset_in_queue_by_time(topic, queue_id, timestamp, boundary_type)
    }

    fn look_message_by_offset(&self, commit_log_offset: i64) -> Option<MessageExt> {
        if let Some(sbr) = self.commit_log.get_message(commit_log_offset, 4) {
            let size = sbr.get_buffer().get_i32();
            self.look_message_by_offset_with_size(commit_log_offset, size)
        } else {
            None
        }
    }

    fn look_message_by_offset_with_size(&self, commit_log_offset: i64, size: i32) -> Option<MessageExt> {
        let sbr = self.commit_log.get_message(commit_log_offset, size);
        if let Some(sbr) = sbr {
            if let Some(mut value) = sbr.get_bytes() {
                MessageDecoder::decode(&mut value, true, false, false, false, false)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn select_one_message_by_offset(&self, commit_log_offset: i64) -> Option<SelectMappedBufferResult> {
        let sbr = self.commit_log.get_message(commit_log_offset, 4);
        if let Some(sbr) = sbr {
            let size = sbr.get_buffer().get_i32();
            self.commit_log.get_message(commit_log_offset, size)
        } else {
            None
        }
    }

    fn select_one_message_by_offset_with_size(
        &self,
        commit_log_offset: i64,
        msg_size: i32,
    ) -> Option<SelectMappedBufferResult> {
        self.commit_log.get_message(commit_log_offset, msg_size)
    }

    fn get_running_data_info(&self) -> String {
        format!("{}", self.store_stats_service)
    }

    fn get_timing_message_count(&self, topic: &CheetahString) -> i64 {
        if let Some(timer_message_store) = self.timer_message_store.as_ref() {
            timer_message_store.timer_metrics.get_timing_count(topic)
        } else {
            0
        }
    }

    fn get_runtime_info(&self) -> HashMap<String, String> {
        // First get the base runtime info from the store stats service
        let mut result = self.store_stats_service.get_runtime_info();

        // Add disk space usage for commit log
        {
            let mut min_physics_used_ratio = f64::MAX;
            let commit_log_store_path = Self::get_store_path_physic(&self.message_store_config);
            let paths = commit_log_store_path.split(mix_all::MULTI_PATH_SPLITTER.as_str());

            for cl_path in paths {
                let cl_path = cl_path.trim();
                let physic_ratio = if util_all::is_path_exists(cl_path) {
                    store_path_disk_used_ratio(cl_path)
                } else {
                    -1.0
                };

                result.insert(
                    format!("{}_{}", RunningStats::CommitLogDiskRatio.as_str(), cl_path),
                    physic_ratio.to_string(),
                );

                min_physics_used_ratio = min_physics_used_ratio.min(physic_ratio);
            }

            result.insert(
                RunningStats::CommitLogDiskRatio.as_str().to_string(),
                min_physics_used_ratio.to_string(),
            );
        }

        // Add disk space usage for consume queue
        {
            let logics_ratio =
                store_path_disk_used_ratio(Self::get_store_path_logic(&self.message_store_config).as_str());
            result.insert(
                RunningStats::ConsumeQueueDiskRatio.as_str().to_string(),
                logics_ratio.to_string(),
            );
        }

        // Add commit log offset info
        result.insert(
            RunningStats::CommitLogMinOffset.as_str().to_string(),
            self.get_min_phy_offset().to_string(),
        );

        result.insert(
            RunningStats::CommitLogMaxOffset.as_str().to_string(),
            self.get_max_phy_offset().to_string(),
        );

        if let Some(timer_message_store) = self.timer_message_store.as_ref() {
            result.insert(
                "timerReadBehind".to_string(),
                timer_message_store.get_dequeue_behind().to_string(),
            );
            result.insert(
                "timerOffsetBehind".to_string(),
                timer_message_store.get_enqueue_behind_messages().to_string(),
            );
            result.insert(
                "timerCongestNum".to_string(),
                timer_message_store.get_all_congest_num().to_string(),
            );
            result.insert(
                "timerEnqueueTps".to_string(),
                timer_message_store.get_enqueue_tps().to_string(),
            );
            result.insert(
                "timerDequeueTps".to_string(),
                timer_message_store.get_dequeue_tps().to_string(),
            );
            let (topic_backlog_distribution, timer_backlog_distribution) =
                timer_message_store.runtime_backlog_metrics();
            if let Ok(topic_backlog_distribution) = serde_json::to_string(&topic_backlog_distribution) {
                result.insert("timerTopicBacklogDistribution".to_string(), topic_backlog_distribution);
            }
            if let Ok(timer_backlog_distribution) = serde_json::to_string(&timer_backlog_distribution) {
                result.insert("timerBacklogDistribution".to_string(), timer_backlog_distribution);
            }
        } else {
            result.insert("timerReadBehind".to_string(), "0".to_string());
            result.insert("timerOffsetBehind".to_string(), "0".to_string());
            result.insert("timerCongestNum".to_string(), "0".to_string());
            result.insert("timerEnqueueTps".to_string(), "0.0".to_string());
            result.insert("timerDequeueTps".to_string(), "0.0".to_string());
            result.insert("timerTopicBacklogDistribution".to_string(), "{}".to_string());
            result.insert("timerBacklogDistribution".to_string(), "{}".to_string());
        }

        result
    }

    #[inline]
    fn get_max_phy_offset(&self) -> i64 {
        self.commit_log.get_max_offset()
    }

    #[inline]
    fn get_min_phy_offset(&self) -> i64 {
        self.commit_log.get_min_offset()
    }

    fn get_earliest_message_time(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        if let Some(logic_queue) = self.get_consume_queue(topic, queue_id) {
            if let Some(cq) = logic_queue.get_earliest_unit_and_store_time() {
                return cq.1;
            }
        }
        -1
    }

    fn get_earliest_message_time_store(&self) -> i64 {
        let min_phy_offset = self.get_min_phy_offset();

        //Rust not support DLedgerCommitLog
        /*if (this.getCommitLog() instanceof DLedgerCommitLog) {
            minPhyOffset += DLedgerEntry.BODY_OFFSET;
        }*/

        let mut size = MessageDecoder::MESSAGE_STORE_TIMESTAMP_POSITION + 8;
        let result = self.broker_config.broker_ip1.to_string().parse::<IpAddr>().unwrap();
        if result.is_ipv6() {
            size = MessageDecoder::MESSAGE_STORE_TIMESTAMP_POSITION + 20;
        }
        self.commit_log.pickup_store_timestamp(min_phy_offset, size as i32)
    }

    /*    async fn get_earliest_message_time_async(
        &self,
        topic: &str,
        queue_id: i32,
    ) -> Result<i64, StoreError> {

    }*/

    fn get_message_store_timestamp(&self, topic: &CheetahString, queue_id: i32, consume_queue_offset: i64) -> i64 {
        if let Some(logic_queue) = self.get_consume_queue(topic, queue_id) {
            if let Some(cq) = logic_queue.get_cq_unit_and_store_time(consume_queue_offset) {
                return cq.1;
            }
        }
        -1
    }

    async fn get_message_store_timestamp_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> Result<i64, StoreError> {
        if let Some(logic_queue) = self.get_consume_queue(topic, queue_id) {
            if let Some(cq) = logic_queue.get_cq_unit_and_store_time(consume_queue_offset) {
                return Ok(cq.1);
            }
        }
        Ok(-1)
    }

    fn get_message_total_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        if let Some(logic_queue) = self.get_consume_queue(topic, queue_id) {
            return logic_queue.get_message_total_in_queue();
        }
        0
    }

    fn get_commit_log_data(&self, offset: i64) -> Option<SelectMappedBufferResult> {
        if self.shutdown.load(Ordering::Acquire) {
            return None;
        }
        self.commit_log.get_data(offset)
    }

    fn get_bulk_commit_log_data(&self, offset: i64, size: i32) -> Option<Vec<SelectMappedBufferResult>> {
        if self.shutdown.load(Ordering::Acquire) {
            return None;
        }
        self.commit_log.get_bulk_data(offset, size)
    }

    async fn append_to_commit_log(
        &mut self,
        start_offset: i64,
        data: &[u8],
        data_start: i32,
        data_length: i32,
    ) -> Result<bool, StoreError> {
        if self.shutdown.load(Ordering::Acquire) {
            warn!("message store has shutdown, so appendToCommitLog is forbidden");
            return Ok(false);
        }

        let result = self
            .commit_log
            .append_data(start_offset, data, data_start, data_length)
            .await?;
        if result {
            // TODO weak up to do commit log flush
        } else {
            error!(
                "DefaultMessageStore#appendToCommitLog: failed to append data to commitLog, physical offset={}, data \
                 length={}",
                start_offset, data_length
            )
        }
        Ok(result)
    }

    fn execute_delete_files_manually(&self) {
        self.clean_commit_log_service.execute_delete_files_manually()
    }

    async fn query_message(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Option<QueryMessageResult> {
        let mut query_message_result = QueryMessageResult::default();
        let mut last_query_msg_time = end_timestamp;
        for i in 1..3 {
            let mut query_offset_result =
                self.index_service
                    .query_offset(topic, key, max_num, begin_timestamp, end_timestamp);
            if query_offset_result.get_phy_offsets().is_empty() {
                break;
            }

            query_offset_result.get_phy_offsets_mut().sort();

            query_message_result.index_last_update_timestamp = query_offset_result.get_index_last_update_timestamp();
            query_message_result.index_last_update_phyoffset = query_offset_result.get_index_last_update_phyoffset();
            let phy_offsets = query_offset_result.get_phy_offsets();
            for m in 0..phy_offsets.len() {
                let offset = *phy_offsets.get(m).unwrap();
                let msg = self.look_message_by_offset(offset);
                if m == 0 {
                    last_query_msg_time = msg.as_ref().unwrap().store_timestamp;
                }
                let result = self.commit_log.get_data_with_option(offset, false);
                if let Some(sbr) = result {
                    query_message_result.add_message(sbr);
                }
            }
            if query_message_result.buffer_total_size > 0 {
                break;
            }
            if last_query_msg_time < begin_timestamp {
                break;
            }
        }

        Some(query_message_result)
    }

    /*async fn query_message_async(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Result<QueryMessageResult, StoreError> {

    }*/

    async fn update_ha_master_address(&self, new_addr: &str) {
        if let Some(ha_service) = self.ha_service.as_ref() {
            ha_service.update_ha_master_address(new_addr).await;
        }
    }

    fn update_master_address(&self, new_addr: &CheetahString) {
        if let Some(ha_service) = self.ha_service.as_ref().cloned() {
            let new_addr = new_addr.clone();
            tokio::spawn(async move {
                ha_service.update_master_address(new_addr.as_str()).await;
            });
        }
    }

    fn slave_fall_behind_much(&self) -> i64 {
        if self.ha_service.is_none()
            || self.message_store_config.duplication_enable
            || self.message_store_config.enable_dledger_commit_log
        {
            warn!("haService is None or duplication/dledger commit log is enabled");
            -1
        } else {
            match self.ha_service.as_ref() {
                Some(ha_service) => self.commit_log.get_max_offset() - ha_service.get_push_to_slave_max_offset(),
                None => -1,
            }
        }
    }

    fn delete_topics(&mut self, delete_topics: Vec<&CheetahString>) -> i32 {
        let delete_topics = delete_topics.into_iter().cloned().collect::<Vec<_>>();
        self.delete_topics_inner(&delete_topics)
    }

    fn clean_unused_topic(&self, retain_topics: &HashSet<String>) -> i32 {
        let topics_to_delete = self
            .consume_queue_store
            .get_consume_queue_table()
            .lock()
            .keys()
            .filter(|topic| {
                !retain_topics.contains(topic.as_str())
                    && !TopicValidator::is_system_topic(topic.as_str())
                    && !is_lmq(Some(topic.as_str()))
            })
            .cloned()
            .collect::<Vec<_>>();
        self.delete_topics_inner(&topics_to_delete)
    }

    fn clean_expired_consumer_queue(&self) {
        let min_commit_log_offset = self.get_min_phy_offset();
        self.consume_queue_store.clean_expired_sync(min_commit_log_offset);
    }

    fn check_in_mem_by_consume_offset(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_offset: i64,
        batch_size: i32,
    ) -> bool {
        let consume_queue = self.consume_queue_store.find_or_create_consume_queue(topic, queue_id);
        let first_cqitem = consume_queue.get(consume_offset);
        if first_cqitem.is_none() {
            return false;
        }
        let cq = first_cqitem.as_ref().unwrap();
        let start_offset_py = cq.pos;
        if batch_size <= 1 {
            let size = cq.size;
            return self.check_in_mem_by_commit_offset(start_offset_py, size);
        }
        let last_cqitem = consume_queue.get(consume_offset + batch_size as i64);
        if last_cqitem.is_none() {
            let size = cq.size;
            return self.check_in_mem_by_commit_offset(start_offset_py, size);
        }
        let last_cqitem = last_cqitem.as_ref().unwrap();
        let end_offset_py = last_cqitem.pos;
        let size = (end_offset_py - start_offset_py) + last_cqitem.size as i64;
        self.check_in_mem_by_commit_offset(start_offset_py, size as i32)
    }

    fn check_in_store_by_consume_offset(&self, topic: &CheetahString, queue_id: i32, consume_offset: i64) -> bool {
        let commit_log_offset = self.get_commit_log_offset_in_queue(topic, queue_id, consume_offset);
        commit_log_offset >= self.commit_log.get_min_offset()
    }

    #[inline]
    fn dispatch_behind_bytes(&self) -> i64 {
        self.reput_message_service.behind()
    }

    fn flush(&self) -> i64 {
        self.commit_log.flush()
    }

    fn get_flushed_where(&self) -> i64 {
        self.commit_log.get_flushed_where()
    }

    fn reset_write_offset(&self, phy_offset: i64) -> bool {
        self.commit_log.mut_from_ref().reset_offset(phy_offset)
    }

    fn get_confirm_offset(&self) -> i64 {
        self.commit_log.get_confirm_offset()
    }

    fn set_confirm_offset(&mut self, phy_offset: i64) {
        self.commit_log.set_confirm_offset(phy_offset);
    }

    fn is_os_page_cache_busy(&self) -> bool {
        let begin = self.commit_log.begin_time_in_lock().load(Ordering::Relaxed);
        let diff = current_millis() - begin;
        diff < 10000000 && diff > self.message_store_config.os_page_cache_busy_timeout_mills
    }

    fn lock_time_millis(&self) -> i64 {
        self.commit_log.lock_time_mills()
    }

    #[inline]
    fn is_transient_store_pool_deficient(&self) -> bool {
        self.remain_transient_store_buffer_numbs() == 0
    }

    #[inline]
    fn get_dispatcher_list(&self) -> &[Arc<dyn CommitLogDispatcher>] {
        self.dispatcher.dispatcher_vec.as_slice()
    }

    #[inline]
    fn add_dispatcher(&mut self, dispatcher: Arc<dyn CommitLogDispatcher>) {
        self.dispatcher.add_dispatcher(dispatcher);
    }

    #[inline]
    fn add_first_dispatcher(&mut self, dispatcher: Arc<dyn CommitLogDispatcher>) {
        self.dispatcher.add_first_dispatcher(dispatcher);
    }

    fn get_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> Option<ArcConsumeQueue> {
        let binding = self.consume_queue_store.get_consume_queue_table();
        let table = binding.lock();
        let map = table.get(topic)?;
        map.get(&queue_id).cloned()
    }

    fn find_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> Option<ArcConsumeQueue> {
        Some(self.consume_queue_store.find_or_create_consume_queue(topic, queue_id))
    }

    fn get_broker_stats_manager(&self) -> Option<&Arc<BrokerStatsManager>> {
        self.broker_stats_manager.as_ref()
    }

    fn on_commit_log_append<MF: MappedFile>(
        &self,
        msg: &MessageExtBrokerInner,
        result: &AppendMessageResult,
        commit_log_file: &MF,
    ) {
        let _ = (msg, result, commit_log_file);
    }

    fn on_commit_log_dispatch<MF: MappedFile>(
        &self,
        dispatch_request: &DispatchRequest,
        do_dispatch: bool,
        commit_log_file: &MF,
        is_recover: bool,
        is_file_end: bool,
    ) -> Result<(), StoreError> {
        let _ = (commit_log_file, is_recover);
        if do_dispatch && !is_file_end {
            let mut request = dispatch_request.clone();
            self.dispatcher.dispatch(&mut request);
        }
        Ok(())
    }

    fn finish_commit_log_dispatch(&self) {
        // Local file mode dispatches consume queue/index updates immediately.
    }

    fn get_message_store_config(&self) -> &MessageStoreConfig {
        self.message_store_config.as_ref()
    }

    fn get_store_stats_service(&self) -> Arc<StoreStatsService> {
        self.store_stats_service.clone()
    }

    fn get_store_checkpoint(&self) -> &StoreCheckpoint {
        self.store_checkpoint.as_ref().unwrap()
    }

    fn get_store_checkpoint_arc(&self) -> Arc<StoreCheckpoint> {
        self.store_checkpoint.clone().unwrap()
    }

    fn get_system_clock(&self) -> Arc<SystemClock> {
        Arc::new(SystemClock)
    }

    fn get_commit_log(&self) -> &CommitLog {
        self.commit_log.as_ref()
    }

    fn get_commit_log_mut_from_ref(&self) -> &mut CommitLog {
        self.commit_log.mut_from_ref()
    }

    fn get_commit_log_mut(&mut self) -> &mut CommitLog {
        self.commit_log.as_mut()
    }

    fn set_commitlog_read_mode(&mut self, read_ahead_mode: i32) -> Result<(), StoreError> {
        let data_read_ahead_enable = read_ahead_mode == MADV_NORMAL;
        Arc::make_mut(&mut self.message_store_config).data_read_ahead_enable = data_read_ahead_enable;
        self.commit_log.set_data_read_ahead_enable(data_read_ahead_enable);
        self.commit_log.scan_file_and_set_read_mode(read_ahead_mode);
        Ok(())
    }

    fn get_running_flags(&self) -> &RunningFlags {
        self.running_flags.as_ref()
    }

    fn get_running_flags_arc(&self) -> Arc<RunningFlags> {
        self.running_flags.clone()
    }

    fn get_transient_store_pool(&self) -> Arc<TransientStorePool> {
        Arc::new(self.transient_store_pool.clone())
    }

    fn get_allocate_mapped_file_service(&self) -> Arc<AllocateMappedFileService> {
        self.allocate_mapped_file_service.clone()
    }

    fn truncate_dirty_logic_files(&self, phy_offset: i64) {
        self.consume_queue_store.truncate_dirty(phy_offset);
    }

    fn unlock_mapped_file<MF: MappedFile>(&self, unlock_mapped_file: &MF) {
        warn!("unlock_mapped_file: not implemented");
    }

    fn get_queue_store(&self) -> &dyn Any {
        self.consume_queue_store.as_any()
    }

    fn is_sync_disk_flush(&self) -> bool {
        self.message_store_config.flush_disk_type == FlushDiskType::SyncFlush
    }

    fn is_sync_master(&self) -> bool {
        self.message_store_config.broker_role == BrokerRole::SyncMaster
    }

    fn assign_offset(&self, msg: &mut MessageExtBrokerInner) -> Result<(), StoreError> {
        let tran_type = MessageSysFlag::get_transaction_value(msg.sys_flag());
        if tran_type == MessageSysFlag::TRANSACTION_NOT_TYPE || tran_type == MessageSysFlag::TRANSACTION_COMMIT_TYPE {
            self.consume_queue_store.assign_queue_offset(msg);
        }
        Ok(())
    }

    fn increase_offset(&self, msg: &MessageExtBrokerInner, message_num: i16) {
        let tran_type = MessageSysFlag::get_transaction_value(msg.sys_flag());
        if tran_type == MessageSysFlag::TRANSACTION_NOT_TYPE || tran_type == MessageSysFlag::TRANSACTION_COMMIT_TYPE {
            self.consume_queue_store.increase_queue_offset(msg, message_num);
        }
    }

    fn get_master_store_in_process<M: MessageStore + Send + Sync + 'static>(&self) -> Option<Arc<M>> {
        let guard = self
            .master_store_in_process
            .read()
            .expect("master_store_in_process lock poisoned");
        let erased = guard.as_ref()?.clone();
        let boxed_master_store = erased.downcast::<Arc<M>>().ok()?;
        Some(Arc::clone(boxed_master_store.as_ref()))
    }

    fn set_master_store_in_process<M: MessageStore + Send + Sync + 'static>(&self, master_store_in_process: Arc<M>) {
        let mut guard = self
            .master_store_in_process
            .write()
            .expect("master_store_in_process lock poisoned");
        *guard = Some(Arc::new(master_store_in_process) as Arc<dyn Any + Send + Sync>);
    }

    fn get_data(&self, offset: i64, size: i32, byte_buffer: &mut BytesMut) -> bool {
        let Some(result) = self.commit_log.get_message(offset, size) else {
            return false;
        };
        let Some(bytes) = result.get_bytes_ref() else {
            return false;
        };
        if bytes.len() < size as usize {
            return false;
        }
        byte_buffer.extend_from_slice(&bytes[..size as usize]);
        true
    }

    fn set_alive_replica_num_in_group(&self, alive_replica_nums: i32) {
        self.alive_replica_num_in_group
            .store(alive_replica_nums.max(1), Ordering::SeqCst);
    }

    fn get_alive_replica_num_in_group(&self) -> i32 {
        self.alive_replica_num_in_group.load(Ordering::SeqCst)
    }

    fn sync_controller_sync_state_set(&self, local_broker_id: i64, sync_state_set: &HashSet<i64>) {
        self.set_alive_replica_num_in_group(sync_state_set.len() as i32);
        if let Some(ha_service) = self.ha_service.as_ref() {
            ha_service.sync_controller_sync_state_set(local_broker_id, sync_state_set);
        }
    }

    fn wakeup_ha_client(&self) {
        if let Some(ha_service) = self.ha_service.as_ref() {
            ha_service.get_wait_notify_object().notify_waiters();
        }
    }

    fn get_master_flushed_offset(&self) -> i64 {
        self.master_flushed_offset.load(Ordering::SeqCst)
    }

    fn get_broker_init_max_offset(&self) -> i64 {
        self.broker_init_max_offset.load(Ordering::SeqCst)
    }

    fn set_master_flushed_offset(&self, master_flushed_offset: i64) {
        self.master_flushed_offset
            .store(master_flushed_offset, Ordering::SeqCst);
        if let Some(store_checkpoint) = self.store_checkpoint.as_ref() {
            store_checkpoint.set_master_flushed_offset(master_flushed_offset.max(0) as u64);
        }
    }

    fn set_broker_init_max_offset(&mut self, broker_init_max_offset: i64) {
        self.broker_init_max_offset
            .store(broker_init_max_offset, Ordering::SeqCst);
    }

    fn sync_broker_role(&mut self, broker_role: BrokerRole) {
        Arc::make_mut(&mut self.message_store_config).broker_role = broker_role;
        self.commit_log.sync_broker_role(broker_role);
        self.refresh_controller_confirm_offset_after_role_change();
        self.sync_timer_message_store_role();
    }

    fn calc_delta_checksum(&self, from: i64, to: i64) -> Vec<u8> {
        if from < 0 || to <= from {
            return Vec::new();
        }

        let Ok(size) = usize::try_from(to - from) else {
            return Vec::new();
        };
        if size == 0 || size > i32::MAX as usize {
            return Vec::new();
        }

        let max_checksum_range = self.message_store_config.max_checksum_range;
        if max_checksum_range > 0 && size > max_checksum_range {
            error!(
                "checksum range from {} with size {} exceeds threshold {}",
                from, size, max_checksum_range
            );
            return Vec::new();
        }

        let Some(buffer_results) = self.get_bulk_commit_log_data(from, size as i32) else {
            return Vec::new();
        };

        let mut encoded_messages = BytesMut::with_capacity(size);
        for buffer_result in buffer_results {
            let Some(mut bytes) = buffer_result.get_bytes() else {
                continue;
            };

            for message in MessageDecoder::decodes_batch(&mut bytes, true, false) {
                match MessageDecoder::encode_uniquely(&message, false) {
                    Ok(encoded) => encoded_messages.extend_from_slice(encoded.as_ref()),
                    Err(error) => warn!("skip uniquely encoding message while calculating checksum: {}", error),
                }
            }
        }

        if encoded_messages.is_empty() {
            return Vec::new();
        }

        murmur3_x64_128_bytes(encoded_messages.as_ref(), 0).to_vec()
    }

    fn truncate_files(&self, offset_to_truncate: i64) -> Result<bool, StoreError> {
        if offset_to_truncate >= self.get_max_phy_offset() {
            info!(
                "no need to truncate files, truncate offset is {}, max physical offset is {}",
                offset_to_truncate,
                self.get_max_phy_offset()
            );
            return Ok(true);
        }

        if !self.is_offset_aligned(offset_to_truncate) {
            error!("offset {} is not aligned, truncate failed", offset_to_truncate);
            return Ok(false);
        }

        self.consume_queue_store.truncate_dirty(offset_to_truncate);
        self.commit_log.mut_from_ref().truncate_dirty_files(offset_to_truncate);
        let mut consume_queue_store = self.consume_queue_store.clone();
        consume_queue_store.recover_offset_table(self.commit_log.get_min_offset());
        Ok(true)
    }

    fn is_offset_aligned(&self, offset: i64) -> bool {
        let Some(mapped_buffer_result) = self.get_commit_log_data(offset) else {
            return true;
        };
        let Some(mut bytes) = mapped_buffer_result.get_bytes() else {
            return true;
        };
        self.check_message_and_return_size(&mut bytes, true, false, false)
            .success
    }

    fn get_put_message_hook_list(&self) -> Vec<Arc<dyn PutMessageHook>> {
        self.put_message_hook_list
            .iter()
            .cloned()
            .map(|hook| hook as Arc<dyn PutMessageHook>)
            .collect()
    }

    fn set_send_message_back_hook(&self, send_message_back_hook: Arc<dyn SendMessageBackHook>) {
        let mut guard = self
            .send_message_back_hook
            .write()
            .expect("send_message_back_hook lock poisoned");
        *guard = Some(send_message_back_hook);
    }

    fn get_send_message_back_hook(&self) -> Option<Arc<dyn SendMessageBackHook>> {
        self.send_message_back_hook
            .read()
            .expect("send_message_back_hook lock poisoned")
            .clone()
    }

    fn get_last_file_from_offset(&self) -> i64 {
        self.commit_log.get_last_file_from_offset()
    }

    fn get_last_mapped_file(&self, start_offset: i64) -> bool {
        self.commit_log.mut_from_ref().get_last_mapped_file(start_offset)
    }

    fn set_physical_offset(&self, phy_offset: i64) {
        self.commit_log.set_mapped_file_queue_offset(phy_offset);
    }

    fn is_mapped_files_empty(&self) -> bool {
        self.commit_log.is_mapped_files_empty()
    }

    fn get_state_machine_version(&self) -> i64 {
        self.state_machine_version.load(Ordering::SeqCst)
    }

    fn check_message_and_return_size(
        &self,
        bytes: &mut Bytes,
        check_crc: bool,
        check_dup_info: bool,
        read_body: bool,
    ) -> DispatchRequest {
        commit_log::check_message_and_return_size(
            bytes,
            check_crc,
            check_dup_info,
            read_body,
            &self.message_store_config,
            self.max_delay_level,
            self.delay_level_table_ref(),
        )
    }
    #[inline]
    fn remain_transient_store_buffer_numbs(&self) -> i32 {
        if self.is_transient_store_pool_enable() {
            return self.transient_store_pool.available_buffer_nums() as i32;
        }
        i32::MAX
    }

    #[inline]
    fn remain_how_many_data_to_commit(&self) -> i64 {
        self.commit_log.remain_how_many_data_to_commit()
    }

    #[inline]
    fn remain_how_many_data_to_flush(&self) -> i64 {
        self.commit_log.remain_how_many_data_to_flush()
    }

    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    fn estimate_message_count(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        from: i64,
        to: i64,
        filter: &dyn MessageFilter,
    ) -> i64 {
        self.get_consume_queue(topic, queue_id)
            .map(|logic_queue| logic_queue.estimate_message_count(from, to, filter))
            .unwrap_or(0)
    }

    fn recover_topic_queue_table(&mut self) {
        let min_phy_offset = self.commit_log.get_min_offset();
        self.consume_queue_store.recover_offset_table(min_phy_offset);
    }

    fn notify_message_arrive_if_necessary(&self, dispatch_request: &mut DispatchRequest) {
        if self.broker_config.long_polling_enable {
            if let Some(ref message_arriving_listener) = self.message_arriving_listener {
                message_arriving_listener.arriving(
                    dispatch_request.topic.as_ref(),
                    dispatch_request.queue_id,
                    dispatch_request.consume_queue_offset + 1,
                    Some(dispatch_request.tags_code),
                    dispatch_request.store_timestamp,
                    dispatch_request.bit_map.clone(),
                    dispatch_request.properties_map.as_ref(),
                );
                self.reput_message_service
                    .notify_message_arrive4multi_queue(dispatch_request);
            }
        }
    }

    fn set_put_message_hook(&mut self, put_message_hook: BoxedPutMessageHook) {
        self.put_message_hook_list.push(Arc::from(put_message_hook));
    }

    fn get_ha_service(&self) -> Option<&GeneralHAService> {
        self.ha_service.as_ref()
    }

    fn get_ha_runtime_info(&self) -> Option<HARuntimeInfo> {
        self.ha_service
            .as_ref()
            .map(|ha_service| ha_service.get_runtime_info(self.commit_log.get_max_offset()))
    }
}

#[derive(Default)]
pub struct CommitLogDispatcherDefault {
    dispatcher_vec: Vec<Arc<dyn CommitLogDispatcher>>,
}

impl CommitLogDispatcherDefault {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn add_dispatcher(&mut self, dispatcher: Arc<dyn CommitLogDispatcher>) {
        self.dispatcher_vec.push(dispatcher);
    }

    #[inline]
    pub fn add_first_dispatcher(&mut self, dispatcher: Arc<dyn CommitLogDispatcher>) {
        self.dispatcher_vec.insert(0, dispatcher);
    }

    #[inline]
    pub fn min_dispatch_progress_offset(&self, commit_log_min_offset: i64) -> Option<i64> {
        self.dispatcher_vec
            .iter()
            .filter_map(|dispatcher| dispatcher.dispatch_progress_offset(commit_log_min_offset))
            .min()
    }
}

impl CommitLogDispatcher for CommitLogDispatcherDefault {
    fn dispatch(&self, dispatch_request: &mut DispatchRequest) {
        for dispatcher in self.dispatcher_vec.iter() {
            dispatcher.dispatch(dispatch_request);
        }
    }

    fn dispatch_batch(&self, dispatch_requests: &mut [DispatchRequest]) {
        for dispatcher in self.dispatcher_vec.iter() {
            dispatcher.dispatch_batch(dispatch_requests);
        }
    }
}

struct ReputMessageService {
    shutdown_token: CancellationToken,
    new_message_notify: Arc<Notify>,
    pending_messages: Arc<AtomicI64>,
    reput_from_offset: Option<Arc<AtomicI64>>,
    dispatch_tx: Option<tokio::sync::mpsc::Sender<Vec<DispatchRequest>>>,
    inner: Option<ReputMessageServiceInner>,
    reader_handle: Option<tokio::task::JoinHandle<()>>,
    dispatcher_handle: Option<tokio::task::JoinHandle<()>>,
}

impl ReputMessageService {
    fn notify_message_arrive4multi_queue(&self, dispatch_request: &mut DispatchRequest) {
        if let Some(inner) = self.inner.as_ref() {
            inner.notify_message_arrive4multi_queue(dispatch_request);
        }
    }

    pub fn set_reput_from_offset(&mut self, reput_from_offset: i64) {
        self.reput_from_offset = Some(Arc::new(AtomicI64::new(reput_from_offset)));
    }

    /// Notify that new messages have arrived and need to be reput
    pub fn notify_new_message(&self) {
        // Increment pending counter to prevent notification loss
        self.pending_messages.fetch_add(1, Ordering::Relaxed);
        self.new_message_notify.notify_one();
    }

    pub fn start(
        &mut self,
        commit_log: ArcMut<CommitLog>,
        message_store_config: Arc<MessageStoreConfig>,
        dispatcher: ArcMut<CommitLogDispatcherDefault>,
        notify_message_arrive_in_batch: bool,
        message_store: ArcMut<LocalFileMessageStore>,
    ) {
        // Create channel for decoupling read and dispatch
        let (dispatch_tx, mut dispatch_rx) = tokio::sync::mpsc::channel::<Vec<DispatchRequest>>(128);
        self.dispatch_tx = Some(dispatch_tx.clone());
        self.shutdown_token = CancellationToken::new();

        let mut inner = ReputMessageServiceInner {
            reput_from_offset: self.reput_from_offset.clone().unwrap(),
            commit_log,
            message_store_config,
            dispatcher: dispatcher.clone(),
            notify_message_arrive_in_batch,
            message_store: message_store.clone(),
        };
        self.inner = Some(inner.clone());

        let shutdown = self.shutdown_token.clone();
        let new_message_notify = self.new_message_notify.clone();
        let pending_messages = self.pending_messages.clone();

        // Task 1: Read messages from CommitLog and send to channel
        let shutdown_reader = shutdown.clone();
        let reader_handle = tokio::spawn(async move {
            let mut fallback_interval = tokio::time::interval(Duration::from_millis(1));
            fallback_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = new_message_notify.notified() => {
                        // Process all available messages when notified
                        loop {
                            // Check if there are messages to process
                            if !inner.is_commit_log_available() {
                                break;
                            }

                            // Read and parse messages, send to dispatch channel
                            match inner.read_and_parse_batch().await {
                                Some(batch) => {
                                    // Successfully read a batch, try to send
                                    if dispatch_tx.send(batch).await.is_err() {
                                        error!("Failed to send dispatch batch to channel, channel closed");
                                        break;
                                    }

                                    // Decrement pending counter after successful send
                                    // Use saturating_sub to prevent underflow
                                    pending_messages.fetch_update(
                                        Ordering::Relaxed,
                                        Ordering::Relaxed,
                                        |x| if x > 0 { Some(x - 1) } else { Some(0) }
                                    ).ok();
                                }
                                None => {
                                    // No more messages available at this offset
                                    break;
                                }
                            }

                            // Check if there are still pending messages
                            // If no pending and no available, exit loop
                            if pending_messages.load(Ordering::Relaxed) == 0
                                && !inner.is_commit_log_available() {
                                break;
                            }
                        }
                    }
                    _ = fallback_interval.tick() => {
                        // Fallback: periodic check
                        if pending_messages.load(Ordering::Relaxed) > 0 || inner.is_commit_log_available() {
                            if let Some(batch) = inner.read_and_parse_batch().await {
                                if dispatch_tx.send(batch).await.is_ok() {
                                    pending_messages.fetch_update(
                                        Ordering::Relaxed,
                                        Ordering::Relaxed,
                                        |x| if x > 0 { Some(x - 1) } else { Some(0) }
                                    ).ok();
                                }
                            }
                        }
                    }
                    _ = shutdown_reader.cancelled() => {
                        break;
                    }
                }
            }
        });

        // Task 2: Receive from channel and dispatch
        let shutdown_dispatcher = shutdown;
        let dispatcher_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(mut batch) = dispatch_rx.recv() => {
                        // Dispatch the batch
                        dispatcher.dispatch_batch(&mut batch);

                        // Notify message arrival if needed
                        if !notify_message_arrive_in_batch {
                            for req in batch.iter_mut() {
                                message_store.notify_message_arrive_if_necessary(req);
                            }
                        }
                    }
                    _ = shutdown_dispatcher.cancelled() => {
                        // Process remaining messages in channel before shutdown
                        while let Ok(mut batch) = dispatch_rx.try_recv() {
                            dispatcher.dispatch_batch(&mut batch);
                            if !notify_message_arrive_in_batch {
                                for req in batch.iter_mut() {
                                    message_store.notify_message_arrive_if_necessary(req);
                                }
                            }
                        }
                        break;
                    }
                }
            }
        });

        // Store task handles for graceful shutdown
        self.reader_handle = Some(reader_handle);
        self.dispatcher_handle = Some(dispatcher_handle);
    }

    pub async fn run_once(
        &mut self,
        commit_log: ArcMut<CommitLog>,
        message_store_config: Arc<MessageStoreConfig>,
        dispatcher: ArcMut<CommitLogDispatcherDefault>,
        notify_message_arrive_in_batch: bool,
        message_store: ArcMut<LocalFileMessageStore>,
    ) {
        if self.reput_from_offset.is_none() {
            self.reput_from_offset = Some(Arc::new(AtomicI64::new(0)));
        }
        if self.inner.is_none() {
            self.inner = Some(ReputMessageServiceInner {
                reput_from_offset: self.reput_from_offset.clone().unwrap(),
                commit_log,
                message_store_config,
                dispatcher,
                notify_message_arrive_in_batch,
                message_store,
            });
        }
        if let Some(inner) = self.inner.as_mut() {
            inner.do_reput().await;
        }
    }

    pub async fn shutdown(&mut self) {
        // Step 1: Wait for pending messages to be dispatched (max 5 seconds, 50 * 100ms)
        if let Some(inner) = self.inner.as_ref() {
            for i in 0..50 {
                if !inner.is_commit_log_available() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            // Warn if there are still undispatched messages
            if inner.is_commit_log_available() {
                warn!(
                    "shutdown ReputMessageService, but CommitLog have not finish to be dispatched, CommitLog max \
                     offset={}, reputFromOffset={}",
                    inner.commit_log.get_max_offset(),
                    inner.reput_from_offset.load(Ordering::Relaxed)
                );
            }
        }

        // Step 2: Notify tasks to shutdown
        self.shutdown_token.cancel();

        // Step 3: Wait for tasks to complete with timeout (3 seconds)
        if let Some(handle) = self.reader_handle.take() {
            match tokio::time::timeout(Duration::from_secs(3), handle).await {
                Ok(Ok(())) => info!("Reader task shut down successfully"),
                Ok(Err(e)) => warn!("Reader task panicked during shutdown: {:?}", e),
                Err(_) => warn!("Reader task shutdown timeout after 3s"),
            }
        }

        if let Some(handle) = self.dispatcher_handle.take() {
            match tokio::time::timeout(Duration::from_secs(3), handle).await {
                Ok(Ok(())) => info!("Dispatcher task shut down successfully"),
                Ok(Err(e)) => warn!("Dispatcher task panicked during shutdown: {:?}", e),
                Err(_) => warn!("Dispatcher task shutdown timeout after 3s"),
            }
        }

        info!("ReputMessageService shutdown complete");
    }

    #[inline]
    pub fn behind(&self) -> i64 {
        let Some(inner) = self.inner.as_ref() else {
            return 0;
        };
        inner.message_store.get_confirm_offset() - inner.reput_from_offset.load(Ordering::Relaxed)
    }
}

//Construct a consumer queue and index file.
#[derive(Clone)]
struct ReputMessageServiceInner {
    reput_from_offset: Arc<AtomicI64>,
    commit_log: ArcMut<CommitLog>,
    message_store_config: Arc<MessageStoreConfig>,
    dispatcher: ArcMut<CommitLogDispatcherDefault>,
    notify_message_arrive_in_batch: bool,
    message_store: ArcMut<LocalFileMessageStore>,
}

impl ReputMessageServiceInner {
    fn notify_message_arrive4multi_queue(&self, dispatch_request: &mut DispatchRequest) {
        if let Some(message_arriving_listener) = self.message_store.message_arriving_listener.as_ref() {
            notify_message_arrive_for_multi_dispatch(
                self.message_store_config.as_ref(),
                message_arriving_listener.as_ref().as_ref(),
                dispatch_request,
            );
        }
    }

    pub async fn do_reput(&mut self) {
        let reput_from_offset = self.reput_from_offset.load(Ordering::Relaxed);
        if reput_from_offset < self.commit_log.get_min_offset() {
            warn!(
                "The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate that the dispatch \
                 behind too much and the commitlog has expired.",
                reput_from_offset,
                self.commit_log.get_min_offset()
            );
            self.reput_from_offset
                .store(self.commit_log.get_min_offset(), Ordering::Release);
        }
        let mut do_next = true;
        let mut dispatch_batch: Vec<DispatchRequest> = Vec::with_capacity(64);

        while do_next && self.is_commit_log_available() {
            let result = self.commit_log.get_data(self.reput_from_offset.load(Ordering::Acquire));
            if result.is_none() {
                break;
            }
            let mut result = result.unwrap();
            self.reput_from_offset
                .store(result.start_offset as i64, Ordering::Release);
            let mut read_size = 0i32;
            while read_size < result.size
                && self.reput_from_offset.load(Ordering::Acquire) < self.get_reput_end_offset()
                && do_next
            {
                let dispatch_request = commit_log::check_message_and_return_size(
                    result.bytes.as_mut().unwrap(),
                    false,
                    false,
                    false,
                    &self.message_store_config,
                    self.message_store.max_delay_level,
                    self.message_store.delay_level_table.as_ref(),
                );
                let size = if dispatch_request.buffer_size == -1 {
                    dispatch_request.msg_size
                } else {
                    dispatch_request.buffer_size
                };
                if self.reput_from_offset.load(Ordering::Acquire) + size as i64 > self.get_reput_end_offset() {
                    do_next = false;
                    break;
                }
                if dispatch_request.success {
                    match dispatch_request.msg_size.cmp(&0) {
                        std::cmp::Ordering::Greater => {
                            // Update stats before moving dispatch_request
                            if !self.message_store_config.duplication_enable
                                && self.message_store_config.broker_role == BrokerRole::Slave
                            {
                                self.message_store
                                    .store_stats_service
                                    .add_single_put_message_topic_times_total(
                                        dispatch_request.topic.as_str(),
                                        dispatch_request.batch_size as usize,
                                    );
                                self.message_store
                                    .store_stats_service
                                    .add_single_put_message_topic_size_total(
                                        dispatch_request.topic.as_str(),
                                        dispatch_request.msg_size as usize,
                                    );
                            }

                            // Batch dispatch: accumulate requests (no clone needed)
                            dispatch_batch.push(dispatch_request);

                            // Dispatch batch when reaching threshold or at end
                            if dispatch_batch.len() >= 32 {
                                self.dispatcher.dispatch_batch(&mut dispatch_batch);
                                if !self.notify_message_arrive_in_batch {
                                    for req in dispatch_batch.iter_mut() {
                                        self.message_store.notify_message_arrive_if_necessary(req);
                                    }
                                }
                                dispatch_batch.clear();
                            }

                            self.reput_from_offset.fetch_add(size as i64, Ordering::AcqRel);
                            read_size += size;
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
                } else if size > 0 {
                    error!(
                        "[BUG]read total count not equals msg total size. reputFromOffset={}",
                        self.reput_from_offset.load(Ordering::Relaxed)
                    );
                    self.reput_from_offset.fetch_add(size as i64, Ordering::SeqCst);
                } else {
                    do_next = false;
                    if LocalFileMessageStore::is_dledger_commit_log_enabled_config(self.message_store_config.as_ref()) {
                        warn!("reput reached an unsupported DLedger branch; stopping batch dispatch for this tick");
                    }
                }
            }
        }

        // Dispatch remaining messages in batch
        if !dispatch_batch.is_empty() {
            self.dispatcher.dispatch_batch(&mut dispatch_batch);
            if !self.notify_message_arrive_in_batch {
                for req in dispatch_batch.iter_mut() {
                    self.message_store.notify_message_arrive_if_necessary(req);
                }
            }
        }
        self.record_dispatch_behind_bytes();
    }

    fn is_commit_log_available(&self) -> bool {
        self.reput_from_offset.load(Ordering::Relaxed) < self.get_reput_end_offset()
    }

    fn get_reput_end_offset(&self) -> i64 {
        match self.message_store_config.read_uncommitted {
            true => self.commit_log.get_max_offset(),
            false => self.commit_log.get_confirm_offset(),
        }
    }

    fn record_dispatch_behind_bytes(&self) {
        let behind = self
            .get_reput_end_offset()
            .saturating_sub(self.reput_from_offset.load(Ordering::Relaxed));
        self.message_store
            .store_stats_service
            .set_dispatch_max_buffer(behind.max(0) as u64);
    }

    pub fn reput_from_offset(&self) -> i64 {
        self.reput_from_offset.load(Ordering::Relaxed)
    }

    pub fn set_reput_from_offset(&mut self, reput_from_offset: i64) {
        self.reput_from_offset.store(reput_from_offset, Ordering::SeqCst);
    }

    /// Read and parse a batch of messages from CommitLog (for channel-based dispatch)
    pub async fn read_and_parse_batch(&mut self) -> Option<Vec<DispatchRequest>> {
        let reput_from_offset = self.reput_from_offset.load(Ordering::Relaxed);
        if reput_from_offset < self.commit_log.get_min_offset() {
            warn!(
                "The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate that the dispatch \
                 behind too much and the commitlog has expired.",
                reput_from_offset,
                self.commit_log.get_min_offset()
            );
            self.reput_from_offset
                .store(self.commit_log.get_min_offset(), Ordering::Release);
        }

        if !self.is_commit_log_available() {
            self.record_dispatch_behind_bytes();
            return None;
        }

        let mut dispatch_batch: Vec<DispatchRequest> = Vec::with_capacity(64);

        let result = self.commit_log.get_data(self.reput_from_offset.load(Ordering::Acquire));
        result.as_ref()?;
        let mut result = result.unwrap();
        self.reput_from_offset
            .store(result.start_offset as i64, Ordering::Release);
        let mut read_size = 0i32;

        while read_size < result.size
            && self.reput_from_offset.load(Ordering::Acquire) < self.get_reput_end_offset()
            && dispatch_batch.len() < 64
        {
            let dispatch_request = commit_log::check_message_and_return_size(
                result.bytes.as_mut().unwrap(),
                false,
                false,
                false,
                &self.message_store_config,
                self.message_store.max_delay_level,
                self.message_store.delay_level_table.as_ref(),
            );
            let size = if dispatch_request.buffer_size == -1 {
                dispatch_request.msg_size
            } else {
                dispatch_request.buffer_size
            };

            if self.reput_from_offset.load(Ordering::Acquire) + size as i64 > self.get_reput_end_offset() {
                break;
            }

            if dispatch_request.success {
                match dispatch_request.msg_size.cmp(&0) {
                    std::cmp::Ordering::Greater => {
                        // Update stats before moving dispatch_request
                        if !self.message_store_config.duplication_enable
                            && self.message_store_config.broker_role == BrokerRole::Slave
                        {
                            self.message_store
                                .store_stats_service
                                .add_single_put_message_topic_times_total(
                                    dispatch_request.topic.as_str(),
                                    dispatch_request.batch_size as usize,
                                );
                            self.message_store
                                .store_stats_service
                                .add_single_put_message_topic_size_total(
                                    dispatch_request.topic.as_str(),
                                    dispatch_request.msg_size as usize,
                                );
                        }

                        // Move dispatch_request into batch (no clone needed)
                        dispatch_batch.push(dispatch_request);
                        self.reput_from_offset.fetch_add(size as i64, Ordering::AcqRel);
                        read_size += size;
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
            } else if size > 0 {
                error!(
                    "[BUG]read total count not equals msg total size. reputFromOffset={}",
                    self.reput_from_offset.load(Ordering::Relaxed)
                );
                self.reput_from_offset.fetch_add(size as i64, Ordering::SeqCst);
            } else {
                if LocalFileMessageStore::is_dledger_commit_log_enabled_config(self.message_store_config.as_ref()) {
                    warn!(
                        "read_and_parse_batch reached an unsupported DLedger branch; stopping batch dispatch for this \
                         tick"
                    );
                }
                break;
            }
        }

        self.record_dispatch_behind_bytes();

        if dispatch_batch.is_empty() {
            None
        } else {
            Some(dispatch_batch)
        }
    }
}

async fn run_blocking_scheduled_task<F>(task_name: &'static str, task: F) -> bool
where
    F: FnOnce() + Send + 'static,
{
    match tokio::task::spawn_blocking(task).await {
        Ok(()) => true,
        Err(error) => {
            error!("scheduled store task {task_name} failed: {error}");
            false
        }
    }
}

fn store_path_disk_used_ratio(path: &str) -> f64 {
    let path = path.trim();
    if path.is_empty() {
        error!("Error when measuring disk space usage, path is null or empty");
        return -1.0;
    }

    let path = Path::new(path);
    if !path.exists() {
        error!(
            "Error when measuring disk space usage, file doesn't exist on this path: {}",
            path.to_string_lossy()
        );
        return -1.0;
    }

    match (fs2::total_space(path), fs2::available_space(path)) {
        (Ok(total_space), Ok(available_space)) if total_space > 0 => {
            total_space.saturating_sub(available_space) as f64 / total_space as f64
        }
        (Ok(_), Ok(_)) => -1.0,
        (Err(error), _) | (_, Err(error)) => {
            error!("Error when measuring disk space usage, got exception: {:?}", error);
            -1.0
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DiskCleanDecision {
    should_delete: bool,
    clean_immediately: bool,
}

struct CleanCommitLogService {
    message_store_config: Arc<MessageStoreConfig>,
    commit_log: ArcMut<CommitLog>,
    running_flags: Arc<RunningFlags>,
    manual_delete_requests: AtomicI32,
}

impl CleanCommitLogService {
    const MAX_MANUAL_DELETE_FILE_TIMES: i32 = 20;

    fn new(
        message_store_config: Arc<MessageStoreConfig>,
        commit_log: ArcMut<CommitLog>,
        running_flags: Arc<RunningFlags>,
    ) -> Self {
        Self {
            message_store_config,
            commit_log,
            running_flags,
            manual_delete_requests: AtomicI32::new(0),
        }
    }

    fn run(&self) {
        let expired_time = (self.message_store_config.file_reserved_time as i64)
            .saturating_mul(60)
            .saturating_mul(60)
            .saturating_mul(1000);
        let is_time_up = util_all::is_it_time_to_do(&self.message_store_config.delete_when);
        let disk_decision = self.is_space_to_delete();
        let is_manual_delete = self.consume_manual_delete_request();
        let clean_at_once = self.message_store_config.clean_file_forcibly_enable && disk_decision.clean_immediately;
        let delete_count = if is_time_up || disk_decision.should_delete || is_manual_delete {
            self.commit_log.mut_from_ref().delete_expired_files_by_time(
                expired_time,
                self.message_store_config.delete_commit_log_files_interval as i32,
                self.message_store_config.destroy_mapped_file_interval_forcibly as i64,
                clean_at_once,
                self.message_store_config.delete_file_batch_max as i32,
            )
        } else {
            0
        };
        if delete_count > 0 {
            info!(
                "clean commit log service deleted {} expired commitlog file(s), is_time_up={}, is_space_to_delete={}, \
                 is_manual_delete={}, clean_at_once={}",
                delete_count, is_time_up, disk_decision.should_delete, is_manual_delete, clean_at_once
            );
        } else if disk_decision.should_delete {
            warn!("disk space will be full soon, but delete commitlog file failed");
        }

        let _ = self
            .commit_log
            .mut_from_ref()
            .retry_delete_first_file(self.message_store_config.redelete_hanged_file_interval as i64);
    }

    fn execute_delete_files_manually(&self) {
        self.manual_delete_requests
            .store(Self::MAX_MANUAL_DELETE_FILE_TIMES, Ordering::SeqCst);
        info!("executeDeleteFilesManually was invoked");
    }

    fn consume_manual_delete_request(&self) -> bool {
        self.manual_delete_requests
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |requests| {
                (requests > 0).then_some(requests - 1)
            })
            .is_ok()
    }

    fn is_space_to_delete(&self) -> DiskCleanDecision {
        let warning_ratio = self.disk_space_warning_level_ratio();
        let clean_forcibly_ratio = self.disk_space_clean_forcibly_ratio();
        let (min_physic_ratio, min_store_path) = self.min_physic_disk_ratio();

        if min_physic_ratio > warning_ratio {
            if self.running_flags.get_and_make_disk_full() {
                error!(
                    "physic disk maybe full soon {}, so mark disk full, storePathPhysic={}",
                    min_physic_ratio,
                    min_store_path.as_deref().unwrap_or("")
                );
            }
            return DiskCleanDecision {
                should_delete: true,
                clean_immediately: true,
            };
        } else if min_physic_ratio > clean_forcibly_ratio {
            return DiskCleanDecision {
                should_delete: true,
                clean_immediately: true,
            };
        } else if !self.running_flags.get_and_make_disk_ok() {
            info!(
                "physic disk space OK {}, so mark disk ok, storePathPhysic={}",
                min_physic_ratio,
                min_store_path.as_deref().unwrap_or("")
            );
        }

        let store_path_logics = LocalFileMessageStore::get_store_path_logic(&self.message_store_config);
        let logics_ratio = store_path_disk_used_ratio(store_path_logics.as_str());
        if logics_ratio > warning_ratio {
            if self.running_flags.get_and_make_logic_disk_full() {
                error!("logics disk maybe full soon {}, so mark disk full", logics_ratio);
            }
            return DiskCleanDecision {
                should_delete: true,
                clean_immediately: true,
            };
        } else if logics_ratio > clean_forcibly_ratio {
            return DiskCleanDecision {
                should_delete: true,
                clean_immediately: true,
            };
        } else if !self.running_flags.get_and_make_logic_disk_ok() {
            info!("logics disk space OK {}, so mark disk ok", logics_ratio);
        }

        let max_used_ratio = self.disk_max_used_space_ratio();
        if min_physic_ratio < 0.0 || min_physic_ratio > max_used_ratio {
            info!("commitLog disk maybe full soon, so reclaim space, {}", min_physic_ratio);
            return DiskCleanDecision {
                should_delete: true,
                clean_immediately: false,
            };
        }

        if logics_ratio < 0.0 || logics_ratio > max_used_ratio {
            info!("consumeQueue disk maybe full soon, so reclaim space, {}", logics_ratio);
            return DiskCleanDecision {
                should_delete: true,
                clean_immediately: false,
            };
        }

        DiskCleanDecision::default()
    }

    fn min_physic_disk_ratio(&self) -> (f64, Option<String>) {
        let commit_log_store_path = LocalFileMessageStore::get_store_path_physic(&self.message_store_config);
        let mut min_ratio = f64::MAX;
        let mut min_store_path = None;

        for store_path in commit_log_store_path.split(mix_all::MULTI_PATH_SPLITTER.as_str()) {
            let store_path = store_path.trim();
            if store_path.is_empty() {
                continue;
            }

            let ratio = store_path_disk_used_ratio(store_path);
            if min_ratio > ratio {
                min_ratio = ratio;
                min_store_path = Some(store_path.to_string());
            }
        }

        if min_ratio == f64::MAX {
            (-1.0, None)
        } else {
            (min_ratio, min_store_path)
        }
    }

    fn disk_space_warning_level_ratio(&self) -> f64 {
        (self.message_store_config.disk_space_warning_level_ratio as f64 / 100.0).clamp(0.35, 0.90)
    }

    fn disk_space_clean_forcibly_ratio(&self) -> f64 {
        (self.message_store_config.disk_space_clean_forcibly_ratio as f64 / 100.0).clamp(0.30, 0.85)
    }

    fn disk_max_used_space_ratio(&self) -> f64 {
        let ratio = self.message_store_config.disk_max_used_space_ratio.clamp(10, 95);
        ratio as f64 / 100.0
    }
}

struct CleanConsumeQueueService {
    commit_log: ArcMut<CommitLog>,
    consume_queue_store: ConsumeQueueStore,
    index_service: IndexService,
}

impl CleanConsumeQueueService {
    fn new(commit_log: ArcMut<CommitLog>, consume_queue_store: ConsumeQueueStore, index_service: IndexService) -> Self {
        Self {
            commit_log,
            consume_queue_store,
            index_service,
        }
    }

    fn run(&self) {
        let min_commit_log_offset = self.commit_log.get_min_offset();
        if min_commit_log_offset < 0 {
            return;
        }

        let consume_queue_table = self.consume_queue_store.get_consume_queue_table().lock().clone();
        for queue_table in consume_queue_table.values() {
            for consume_queue in queue_table.values() {
                let consume_queue = &***consume_queue;
                let _ = self
                    .consume_queue_store
                    .delete_expired_file(consume_queue, min_commit_log_offset);
                self.consume_queue_store
                    .correct_min_offset(consume_queue, min_commit_log_offset);
            }
        }

        self.index_service
            .delete_expired_file(min_commit_log_offset.max(0) as u64);

        self.consume_queue_store.clean_expired_sync(min_commit_log_offset);
    }
}

struct CorrectLogicOffsetService {
    commit_log: ArcMut<CommitLog>,
    consume_queue_store: ConsumeQueueStore,
}

impl CorrectLogicOffsetService {
    fn new(commit_log: ArcMut<CommitLog>, consume_queue_store: ConsumeQueueStore) -> Self {
        Self {
            commit_log,
            consume_queue_store,
        }
    }

    fn run(&self) {
        let min_commit_log_offset = self.commit_log.get_min_offset();
        if min_commit_log_offset < 0 {
            return;
        }

        let consume_queue_table = self.consume_queue_store.get_consume_queue_table().lock().clone();
        for queue_table in consume_queue_table.values() {
            for consume_queue in queue_table.values() {
                self.consume_queue_store
                    .correct_min_offset(&***consume_queue, min_commit_log_offset);
            }
        }
    }
}

struct FlushConsumeQueueService {
    message_store_config: Arc<MessageStoreConfig>,
    consume_queue_store: ConsumeQueueStore,
    store_checkpoint: Arc<StoreCheckpoint>,
    worker_handle: parking_lot::Mutex<Option<thread::JoinHandle<()>>>,
    shutdown_tx: parking_lot::Mutex<Option<mpsc::Sender<()>>>,
}

impl FlushConsumeQueueService {
    fn new(
        message_store_config: Arc<MessageStoreConfig>,
        consume_queue_store: ConsumeQueueStore,
        store_checkpoint: Arc<StoreCheckpoint>,
    ) -> Self {
        Self {
            message_store_config,
            consume_queue_store,
            store_checkpoint,
            worker_handle: parking_lot::Mutex::new(None),
            shutdown_tx: parking_lot::Mutex::new(None),
        }
    }

    fn flush_once(consume_queue_store: &ConsumeQueueStore, store_checkpoint: &StoreCheckpoint, flush_least_pages: i32) {
        let consume_queue_table = consume_queue_store.get_consume_queue_table().lock().clone();
        for consume_queue_table in consume_queue_table.values() {
            for consume_queue in consume_queue_table.values() {
                let _ = consume_queue_store.flush(&***consume_queue, flush_least_pages);
            }
        }

        if let Err(error) = store_checkpoint.flush() {
            error!("flush consume queue service failed to flush store checkpoint: {error}");
        }
    }

    fn start(&self) {
        let mut worker_handle = self.worker_handle.lock();
        if worker_handle.is_some() {
            return;
        }

        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        *self.shutdown_tx.lock() = Some(shutdown_tx);

        let message_store_config = self.message_store_config.clone();
        let consume_queue_store = self.consume_queue_store.clone();
        let store_checkpoint = self.store_checkpoint.clone();

        *worker_handle = Some(
            thread::Builder::new()
                .name("flush-consume-queue".to_string())
                .spawn(move || {
                    let interval = message_store_config.flush_interval_consume_queue.max(1) as u64;
                    let thorough_interval = message_store_config.flush_consume_queue_thorough_interval as u64;
                    let default_least_pages = message_store_config.flush_consume_queue_least_pages as i32;
                    let mut last_thorough_flush_timestamp = current_millis();

                    loop {
                        let now = current_millis();
                        let flush_least_pages =
                            if thorough_interval == 0 || now >= last_thorough_flush_timestamp + thorough_interval {
                                last_thorough_flush_timestamp = now;
                                0
                            } else {
                                default_least_pages
                            };

                        Self::flush_once(&consume_queue_store, &store_checkpoint, flush_least_pages);

                        match shutdown_rx.recv_timeout(Duration::from_millis(interval)) {
                            Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                            Err(mpsc::RecvTimeoutError::Timeout) => {}
                        }
                    }

                    Self::flush_once(&consume_queue_store, &store_checkpoint, 0);
                })
                .unwrap(),
        );
    }

    fn shutdown(&self) {
        if let Some(shutdown_tx) = self.shutdown_tx.lock().take() {
            let _ = shutdown_tx.send(());
        }

        if let Some(worker_handle) = self.worker_handle.lock().take() {
            if worker_handle.join().is_err() {
                error!("flush consume queue service thread panicked during shutdown");
            }
        }
    }
}

pub fn parse_delay_level(level_string: &str) -> (BTreeMap<i32, i64>, i32) {
    let mut time_unit_table = HashMap::new();
    time_unit_table.insert("s", 1000);
    time_unit_table.insert("m", 1000 * 60);
    time_unit_table.insert("h", 1000 * 60 * 60);
    time_unit_table.insert("d", 1000 * 60 * 60 * 24);

    let mut delay_level_table = BTreeMap::new();

    let level_array: Vec<&str> = level_string.split(' ').collect();
    let mut max_delay_level = 0;

    for (i, value) in level_array.iter().enumerate() {
        let ch = value.chars().last().unwrap().to_string();
        let tu = time_unit_table
            .get(&ch.as_str())
            .ok_or(format!("Unknown time unit: {ch}"));
        if tu.is_err() {
            continue;
        }
        let tu = *tu.unwrap();

        let level = i as i32 + 1;
        if level > max_delay_level {
            max_delay_level = level;
        }

        let num_str = &value[0..value.len() - 1];
        let num = num_str.parse::<i64>();
        if num.is_err() {
            continue;
        }
        let num = num.unwrap();
        let delay_time_millis = tu * num;
        delay_level_table.insert(level, delay_time_millis);
    }

    (delay_level_table, max_delay_level)
}

#[cfg(test)]
mod tests {
    use bytes::Buf;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::fs;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicI64;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use std::time::Instant;

    use bytes::BufMut;
    use bytes::Bytes;
    use bytes::BytesMut;
    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_common::common::attribute::cleanup_policy::CleanupPolicy;
    use rocketmq_common::common::attribute::Attribute;
    use rocketmq_common::common::boundary_type::BoundaryType;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::broker::broker_role::BrokerRole;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_common::common::message::message_batch::MessageExtBatch;
    use rocketmq_common::common::message::message_ext::MessageExt;
    use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
    use rocketmq_common::common::message::MessageConst;
    use rocketmq_common::common::message::MessageTrait;
    use rocketmq_common::common::message::MessageVersion;
    use rocketmq_common::common::running::running_stats::RunningStats;
    use rocketmq_common::common::topic::TopicValidator;
    use rocketmq_common::CRC32Utils::crc32;
    use rocketmq_common::TopicAttributes::TopicAttributes;
    use rocketmq_rust::ArcMut;
    use tempfile::tempdir;

    use super::run_blocking_scheduled_task;
    use super::CleanCommitLogService;
    use super::LocalFileMessageStore;
    use super::ReputMessageServiceInner;
    use crate::base::dispatch_request::DispatchRequest;
    use crate::base::message_arriving_listener::MessageArrivingListener;
    use crate::base::message_result::PutMessageResult;
    use crate::base::message_status_enum::GetMessageStatus;
    use crate::base::message_status_enum::PutMessageStatus;
    use crate::base::message_store::MessageStore;
    use crate::base::store_checkpoint::StoreCheckpoint;
    use crate::base::store_enum::StoreType;
    use crate::config::flush_disk_type::FlushDiskType;
    use crate::config::message_store_config::MessageStoreConfig;
    use crate::filter::MessageFilter;
    use crate::hook::put_message_hook::PutMessageHook;
    use crate::hook::send_message_back_hook::SendMessageBackHook;
    use crate::message_encoder::message_ext_encoder::MessageExtEncoder;
    use crate::queue::consume_queue::ConsumeQueueTrait;
    use crate::queue::consume_queue_store::ConsumeQueueStoreTrait;
    use crate::store_error::StoreError;
    use crate::store_path_config_helper::get_store_checkpoint;

    fn new_test_store(temp_dir: &tempfile::TempDir) -> ArcMut<LocalFileMessageStore> {
        new_configured_test_store(temp_dir, MessageStoreConfig::default())
    }

    #[tokio::test]
    async fn scheduled_blocking_task_reports_completion_and_join_failure() {
        let completed = Arc::new(AtomicBool::new(false));
        let completed_task = Arc::clone(&completed);

        assert!(
            run_blocking_scheduled_task("test scheduled success", move || {
                completed_task.store(true, Ordering::Release);
            })
            .await
        );
        assert!(completed.load(Ordering::Acquire));

        assert!(
            !run_blocking_scheduled_task("test scheduled panic", || panic!("scheduled task panic")).await,
            "panic in a scheduled blocking task should stop that scheduled loop"
        );
    }

    fn new_configured_test_store(
        temp_dir: &tempfile::TempDir,
        mut message_store_config: MessageStoreConfig,
    ) -> ArcMut<LocalFileMessageStore> {
        message_store_config.store_path_root_dir = temp_dir.path().to_string_lossy().to_string().into();
        let mut store = ArcMut::new(LocalFileMessageStore::new(
            Arc::new(message_store_config),
            Arc::new(BrokerConfig::default()),
            Arc::new(DashMap::<CheetahString, ArcMut<TopicConfig>>::new()),
            None,
            false,
        ));
        let store_clone = store.clone();
        store.set_message_store_arc(store_clone);
        store
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RecordedArrival {
        topic: CheetahString,
        queue_id: i32,
        logic_offset: i64,
        filter_bit_map: Option<Vec<u8>>,
    }

    struct RecordingArrivingListener {
        arrivals: Arc<std::sync::Mutex<Vec<RecordedArrival>>>,
    }

    impl MessageArrivingListener for RecordingArrivingListener {
        fn arriving(
            &self,
            topic: &CheetahString,
            queue_id: i32,
            logic_offset: i64,
            _tags_code: Option<i64>,
            _msg_store_time: i64,
            filter_bit_map: Option<Vec<u8>>,
            _properties: Option<&HashMap<CheetahString, CheetahString>>,
        ) {
            self.arrivals.lock().unwrap().push(RecordedArrival {
                topic: topic.clone(),
                queue_id,
                logic_offset,
                filter_bit_map,
            });
        }
    }

    fn install_recording_arriving_listener(
        store: &mut LocalFileMessageStore,
    ) -> Arc<std::sync::Mutex<Vec<RecordedArrival>>> {
        let arrivals = Arc::new(std::sync::Mutex::new(Vec::new()));
        let listener: Box<dyn MessageArrivingListener + Sync + Send + 'static> = Box::new(RecordingArrivingListener {
            arrivals: Arc::clone(&arrivals),
        });
        store.message_arriving_listener = Some(Arc::new(listener));
        arrivals
    }

    fn reput_inner_for_store(store: ArcMut<LocalFileMessageStore>) -> ReputMessageServiceInner {
        ReputMessageServiceInner {
            reput_from_offset: Arc::new(AtomicI64::new(0)),
            commit_log: store.commit_log.clone(),
            message_store_config: store.message_store_config.clone(),
            dispatcher: store.dispatcher.clone(),
            notify_message_arrive_in_batch: false,
            message_store: store,
        }
    }

    fn new_async_flush_test_store(temp_dir: &tempfile::TempDir) -> ArcMut<LocalFileMessageStore> {
        new_configured_test_store(
            temp_dir,
            MessageStoreConfig {
                flush_disk_type: FlushDiskType::AsyncFlush,
                ..MessageStoreConfig::default()
            },
        )
    }

    fn decode_cq_bytes(bytes: Bytes) -> (i64, i32, i64) {
        let mut bytes = bytes;
        let commit_log_offset = bytes.get_i64();
        let msg_size = bytes.get_i32();
        let tags_code = bytes.get_i64();
        (commit_log_offset, msg_size, tags_code)
    }

    fn build_test_message(topic: &CheetahString, body: Bytes) -> MessageExtBrokerInner {
        let mut msg = MessageExtBrokerInner::default();
        msg.set_topic(topic.clone());
        msg.message_ext_inner.set_queue_id(0);
        msg.set_body(body);
        msg
    }

    async fn append_encoded_test_message(
        store: &mut ArcMut<LocalFileMessageStore>,
        topic: &CheetahString,
        commit_log_offset: i64,
        store_timestamp: i64,
        body: Bytes,
    ) -> i32 {
        let mut msg = build_test_message(topic, body);
        msg.with_version(MessageVersion::V1);
        msg.message_ext_inner.set_store_timestamp(store_timestamp);

        let mut encoder = MessageExtEncoder::new(store.message_store_config());
        assert!(encoder.encode(&msg).is_none());
        let encoded = encoder.byte_buf();
        let msg_size = encoded.len() as i32;
        let appended = store
            .append_to_commit_log(commit_log_offset, encoded.as_ref(), 0, msg_size)
            .await
            .expect("append encoded commitlog message");
        assert!(appended);
        msg_size
    }

    fn build_test_batch(topic: &CheetahString, bodies: &[Bytes]) -> MessageExtBatch {
        let mut batch_body = BytesMut::new();
        for body in bodies {
            let record_size = 4 + 4 + 4 + 4 + 4 + body.len() + 2;
            batch_body.put_i32(record_size as i32);
            batch_body.put_i32(0);
            batch_body.put_i32(crc32(body.as_ref()) as i32);
            batch_body.put_i32(0);
            batch_body.put_i32(body.len() as i32);
            batch_body.put_slice(body.as_ref());
            batch_body.put_i16(0);
        }

        let mut inner = MessageExtBrokerInner::default();
        inner.set_topic(topic.clone());
        inner.message_ext_inner.set_queue_id(0);
        inner.set_body(batch_body.freeze());

        MessageExtBatch {
            message_ext_broker_inner: inner,
            is_inner_batch: false,
            encoded_buff: None,
        }
    }

    #[test]
    fn set_message_store_arc_initializes_timer_message_store_when_timer_wheel_enabled() {
        let temp_dir = tempdir().unwrap();
        let message_store_config = MessageStoreConfig {
            store_path_root_dir: temp_dir.path().to_string_lossy().to_string().into(),
            timer_wheel_enable: true,
            ..MessageStoreConfig::default()
        };

        let mut store = ArcMut::new(LocalFileMessageStore::new(
            Arc::new(message_store_config),
            Arc::new(BrokerConfig::default()),
            Arc::new(DashMap::<CheetahString, ArcMut<TopicConfig>>::new()),
            None,
            false,
        ));

        assert!(store.get_timer_message_store().is_none());

        let store_clone = store.clone();
        store.set_message_store_arc(store_clone);

        assert!(store.get_timer_message_store().is_some());
    }

    #[tokio::test]
    async fn init_rejects_dledger_commit_log_configuration() {
        let temp_dir = tempdir().unwrap();

        for message_store_config in [
            MessageStoreConfig {
                enable_dledger_commit_log: true,
                ..MessageStoreConfig::default()
            },
            MessageStoreConfig {
                enable_dleger_commit_log: true,
                ..MessageStoreConfig::default()
            },
        ] {
            let mut store = new_configured_test_store(&temp_dir, message_store_config);
            let error = store.init().await.expect_err("DLedger should be rejected explicitly");
            assert!(matches!(error, StoreError::General(message) if message.contains("DLedger commit log")));
        }
    }

    #[tokio::test]
    async fn init_rejects_rocksdb_specific_flags_without_rocksdb_store_type() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                clean_rocksdb_dirty_cq_interval_min: 1,
                stat_rocksdb_cq_interval_sec: 2,
                real_time_persist_rocksdb_config: true,
                enable_rocksdb_log: true,
                rocksdb_cq_double_write_enable: true,
                ..MessageStoreConfig::default()
            },
        );

        let error = store
            .init()
            .await
            .expect_err("local file store should reject rocksdb-only configuration");
        assert!(matches!(
            error,
            StoreError::General(message)
            if message.contains("store_type=RocksDB")
                && message.contains("clean_rocksdb_dirty_cq_interval_min")
                && message.contains("stat_rocksdb_cq_interval_sec")
                && message.contains("real_time_persist_rocksdb_config")
                && message.contains("enable_rocksdb_log")
                && message.contains("rocksdb_cq_double_write_enable")
        ));
    }

    #[tokio::test]
    async fn init_allows_rocksdb_specific_flags_when_store_type_is_rocksdb() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                store_type: StoreType::RocksDB,
                enable_rocksdb_log: true,
                ..MessageStoreConfig::default()
            },
        );

        store
            .init()
            .await
            .expect("rocksdb-typed store should accept rocksdb flags");
    }

    #[tokio::test]
    async fn compaction_topic_loads_and_reads_from_commitlog_until_compaction_backend_has_data() {
        let temp_dir = tempdir().unwrap();
        let topic = CheetahString::from_static_str("compaction-fallback-topic");
        let group = CheetahString::from_static_str("compaction-fallback-group");
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                enable_compaction: true,
                flush_disk_type: FlushDiskType::AsyncFlush,
                ..MessageStoreConfig::default()
            },
        );
        let mut topic_config = TopicConfig::new(topic.clone());
        topic_config.attributes.insert(
            TopicAttributes::cleanup_policy_attribute().name().clone(),
            CleanupPolicy::COMPACTION.to_string().into(),
        );
        store
            .topic_config_table
            .insert(topic.clone(), ArcMut::new(topic_config));

        store.init().await.expect("init compaction-enabled store");
        assert!(store.load().await, "load compaction-enabled store");

        let put_result = store
            .put_message(build_test_message(
                &topic,
                Bytes::from_static(b"compaction-fallback-body"),
            ))
            .await;
        assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
        store.reput_once().await;

        let result = store
            .get_message(&group, &topic, 0, 0, 32, None)
            .await
            .expect("compaction topic should fall back to commitlog read");
        assert_eq!(result.status(), Some(GetMessageStatus::Found));
        assert_eq!(result.message_count(), 1);
    }

    #[tokio::test]
    async fn start_requires_init_before_services_begin() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                duplication_enable: true,
                ..MessageStoreConfig::default()
            },
        );

        let error = store.start().await.expect_err("start should require init first");

        assert!(matches!(
            error,
            StoreError::General(message) if message.contains("initialized before start")
        ));
        assert!(!temp_dir.path().join("lock").exists());
    }

    #[tokio::test]
    async fn start_holds_store_root_lock_until_shutdown() {
        let temp_dir = tempdir().unwrap();
        let config = MessageStoreConfig {
            duplication_enable: true,
            ..MessageStoreConfig::default()
        };

        let mut first = new_configured_test_store(&temp_dir, config.clone());
        first.init().await.expect("init first store");
        first.start().await.expect("start first store");
        assert!(temp_dir.path().join("lock").exists());

        let mut second = new_configured_test_store(&temp_dir, config);
        second.init().await.expect("init second store");
        let error = second
            .start()
            .await
            .expect_err("second store should not start while lock is held");
        assert!(matches!(
            error,
            StoreError::General(message) if message.contains("lock file is held")
        ));

        first.shutdown().await;

        second.start().await.expect("lock should be reusable after shutdown");
        second.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_waits_for_stats_and_timer_background_tasks() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                duplication_enable: true,
                timer_wheel_enable: true,
                ..MessageStoreConfig::default()
            },
        );

        store.init().await.expect("init store");
        store.start().await.expect("start store");

        assert!(store.store_stats_service.has_worker_handle());
        assert!(store.reput_message_service.reader_handle.is_some());
        assert!(store.reput_message_service.dispatcher_handle.is_some());
        assert!(store
            .timer_message_store
            .as_ref()
            .expect("timer store should be initialized")
            .has_scheduler_handle());

        store.shutdown().await;

        assert!(!store.store_stats_service.has_worker_handle());
        assert!(store.reput_message_service.reader_handle.is_none());
        assert!(store.reput_message_service.dispatcher_handle.is_none());
        assert!(!store
            .timer_message_store
            .as_ref()
            .expect("timer store should be initialized")
            .has_scheduler_handle());
    }

    #[tokio::test]
    async fn read_and_write_are_rejected_after_shutdown() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_test_store(&temp_dir);
        store.shutdown().await;

        let topic = CheetahString::from_static_str("shutdown-io-topic");
        let group = CheetahString::from_static_str("shutdown-io-group");

        let result = store
            .put_message(build_test_message(&topic, Bytes::from_static(b"after-shutdown")))
            .await;

        assert_eq!(result.put_message_status(), PutMessageStatus::ServiceNotAvailable);
        assert!(store.get_message(&group, &topic, 0, 0, 32, None).await.is_none());
    }

    #[test]
    fn sync_broker_role_updates_timer_dequeue_state() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                timer_wheel_enable: true,
                ..MessageStoreConfig::default()
            },
        );

        let timer_message_store = store
            .get_timer_message_store()
            .cloned()
            .expect("timer message store should exist");

        assert!(!timer_message_store.is_should_running_dequeue());

        store.sync_broker_role(BrokerRole::SyncMaster);
        assert!(timer_message_store.is_should_running_dequeue());

        store.sync_broker_role(BrokerRole::Slave);
        assert!(!timer_message_store.is_should_running_dequeue());

        store.sync_broker_role(BrokerRole::AsyncMaster);
        assert!(timer_message_store.is_should_running_dequeue());
    }

    #[test]
    fn set_master_flushed_offset_updates_store_checkpoint() {
        let temp_dir = tempdir().unwrap();
        let store = new_test_store(&temp_dir);
        let checkpoint_path = get_store_checkpoint(store.message_store_config_ref().store_path_root_dir.as_str());

        store.set_master_flushed_offset(1024);
        store
            .store_checkpoint
            .as_ref()
            .expect("store checkpoint")
            .flush()
            .expect("flush checkpoint");

        let checkpoint = StoreCheckpoint::new(&checkpoint_path).expect("reload checkpoint");
        assert_eq!(checkpoint.master_flushed_offset(), 1024);
    }

    #[tokio::test]
    async fn sync_broker_role_in_controller_mode_refreshes_confirm_offset_for_master() {
        let temp_dir = tempdir().unwrap();
        let mut store = ArcMut::new(LocalFileMessageStore::new(
            Arc::new(MessageStoreConfig {
                store_path_root_dir: temp_dir.path().to_string_lossy().to_string().into(),
                enable_controller_mode: true,
                all_ack_in_sync_state_set: true,
                ..MessageStoreConfig::default()
            }),
            Arc::new(BrokerConfig {
                enable_controller_mode: true,
                ..BrokerConfig::default()
            }),
            Arc::new(DashMap::<CheetahString, ArcMut<TopicConfig>>::new()),
            None,
            false,
        ));
        let store_clone = store.clone();
        store.set_message_store_arc(store_clone);
        store.init().await.expect("init store");
        store.sync_controller_sync_state_set(7, &HashSet::from([7_i64]));

        store
            .get_commit_log_mut()
            .append_data(0, &[1, 2, 3, 4], 0, 4)
            .await
            .expect("append data");
        store.set_confirm_offset(0);
        store.sync_broker_role(BrokerRole::Slave);
        assert_eq!(store.get_commit_log().get_confirm_offset_directly(), 0);

        store.sync_broker_role(BrokerRole::SyncMaster);

        assert_eq!(store.get_commit_log().get_confirm_offset_directly(), 4);
        assert_eq!(store.get_confirm_offset(), 4);
    }

    #[test]
    fn master_store_in_process_round_trips_concrete_store_reference() {
        let temp_dir = tempdir().unwrap();
        let store = new_test_store(&temp_dir);
        let master_store = Arc::new(LocalFileMessageStore::new(
            Arc::new(MessageStoreConfig {
                store_path_root_dir: temp_dir
                    .path()
                    .join("master-store")
                    .to_string_lossy()
                    .to_string()
                    .into(),
                ..MessageStoreConfig::default()
            }),
            Arc::new(BrokerConfig::default()),
            Arc::new(DashMap::<CheetahString, ArcMut<TopicConfig>>::new()),
            None,
            false,
        ));

        store.set_master_store_in_process(master_store.clone());

        let restored = store
            .get_master_store_in_process::<LocalFileMessageStore>()
            .expect("master store should be present");

        assert!(Arc::ptr_eq(&master_store, &restored));
    }

    #[test]
    fn send_message_back_hook_round_trips_registered_hook() {
        struct MockSendBackHook;

        impl SendMessageBackHook for MockSendBackHook {
            fn execute_send_message_back(
                &self,
                _msg_list: &mut [MessageExt],
                broker_name: &CheetahString,
                broker_addr: &CheetahString,
            ) -> bool {
                !broker_name.is_empty() && !broker_addr.is_empty()
            }
        }

        let temp_dir = tempdir().unwrap();
        let store = new_test_store(&temp_dir);
        let hook = Arc::new(MockSendBackHook) as Arc<dyn SendMessageBackHook>;

        store.set_send_message_back_hook(hook.clone());

        let restored = store
            .get_send_message_back_hook()
            .expect("send message back hook should be present");

        assert!(Arc::ptr_eq(&hook, &restored));
    }

    #[test]
    fn clean_unused_topic_deletes_only_non_retained_non_system_non_lmq_topics() {
        let temp_dir = tempdir().unwrap();
        let store = new_test_store(&temp_dir);
        let deletable_topic = CheetahString::from_static_str("delete-me");
        let retained_topic = CheetahString::from_static_str("retain-me");
        let system_topic = CheetahString::from_static_str(TopicValidator::RMQ_SYS_TRACE_TOPIC);
        let lmq_topic = CheetahString::from_static_str("%LMQ%lite-group");

        store
            .consume_queue_store
            .find_or_create_consume_queue(&deletable_topic, 0);
        store
            .consume_queue_store
            .find_or_create_consume_queue(&retained_topic, 0);
        store.consume_queue_store.find_or_create_consume_queue(&system_topic, 0);
        store.consume_queue_store.find_or_create_consume_queue(&lmq_topic, 0);

        let mut retain_topics = HashSet::new();
        retain_topics.insert(retained_topic.to_string());

        let deleted_count = store.clean_unused_topic(&retain_topics);

        assert_eq!(deleted_count, 1);
        assert!(store
            .consume_queue_store
            .find_consume_queue_map(&deletable_topic)
            .is_none());
        assert!(store
            .consume_queue_store
            .find_consume_queue_map(&retained_topic)
            .is_some());
        assert!(store
            .consume_queue_store
            .find_consume_queue_map(&system_topic)
            .is_some());
        assert!(store.consume_queue_store.find_consume_queue_map(&lmq_topic).is_some());
    }

    #[test]
    fn get_commit_log_offset_in_queue_returns_consume_queue_entry_position() {
        let temp_dir = tempdir().unwrap();
        let store = new_test_store(&temp_dir);
        let topic = CheetahString::from_static_str("offset-topic");

        store
            .consume_queue_store
            .put_message_position_info_wrapper(&DispatchRequest {
                topic: topic.clone(),
                queue_id: 1,
                commit_log_offset: 123,
                msg_size: 32,
                consume_queue_offset: 0,
                success: true,
                ..DispatchRequest::default()
            });

        assert_eq!(store.get_commit_log_offset_in_queue(&topic, 1, 0), 123);
    }

    #[test]
    fn assign_offset_and_increase_offset_delegate_to_consume_queue_store() {
        let temp_dir = tempdir().unwrap();
        let store = new_test_store(&temp_dir);
        let topic = CheetahString::from_static_str("assign-offset-topic");

        let mut first = MessageExtBrokerInner::default();
        first.set_topic(topic.clone());
        first.message_ext_inner.set_queue_id(2);
        first.set_body(Bytes::from_static(b"first"));

        store.assign_offset(&mut first).unwrap();
        assert_eq!(first.queue_offset(), 0);

        store.increase_offset(&first, 1);

        let mut second = MessageExtBrokerInner::default();
        second.set_topic(topic);
        second.message_ext_inner.set_queue_id(2);
        second.set_body(Bytes::from_static(b"second"));

        store.assign_offset(&mut second).unwrap();
        assert_eq!(second.queue_offset(), 1);
    }

    #[test]
    fn commit_log_accessors_copy_data_and_allow_offset_reset() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_test_store(&temp_dir);
        let payload = b"rust";

        assert!(store.is_mapped_files_empty());

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let appended = runtime
            .block_on(store.append_to_commit_log(0, payload, 0, payload.len() as i32))
            .unwrap();

        assert!(appended);
        assert!(!store.is_mapped_files_empty());
        assert_eq!(store.get_last_file_from_offset(), 0);
        assert!(store.get_last_mapped_file(0));

        let mut buffer = BytesMut::new();
        assert!(store.get_data(0, payload.len() as i32, &mut buffer));
        assert_eq!(buffer.as_ref(), payload);

        let flushed_where = store.flush();
        assert!(flushed_where >= payload.len() as i64);
        assert_eq!(store.get_flushed_where(), flushed_where);

        assert!(store.reset_write_offset(2));

        let mut truncated = BytesMut::new();
        assert!(!store.get_data(0, payload.len() as i32, &mut truncated));

        let mut remaining = BytesMut::new();
        assert!(store.get_data(0, 2, &mut remaining));
        assert_eq!(remaining.as_ref(), &payload[..2]);
    }

    #[test]
    fn get_put_message_hook_list_returns_registered_hooks() {
        struct TestHook;

        impl PutMessageHook for TestHook {
            fn hook_name(&self) -> &'static str {
                "test-hook"
            }

            fn execute_before_put_message(&self, _msg: &mut dyn MessageTrait) -> Option<PutMessageResult> {
                None
            }
        }

        let temp_dir = tempdir().unwrap();
        let mut store = new_test_store(&temp_dir);
        store.set_put_message_hook(Box::new(TestHook));

        let hooks = store.get_put_message_hook_list();

        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks[0].hook_name(), "test-hook");
    }

    #[test]
    fn consume_queue_reports_basic_runtime_metadata() {
        let temp_dir = tempdir().unwrap();
        let store = new_test_store(&temp_dir);
        let topic = CheetahString::from_static_str("consume-queue-metadata-topic");

        store
            .consume_queue_store
            .put_message_position_info_wrapper(&DispatchRequest {
                topic: topic.clone(),
                queue_id: 1,
                commit_log_offset: 123,
                msg_size: 32,
                consume_queue_offset: 0,
                store_timestamp: 5678,
                success: true,
                ..DispatchRequest::default()
            });

        let consume_queue = store.consume_queue_store.find_or_create_consume_queue(&topic, 1);
        let expected_roll = store.message_store_config_ref().get_mapped_file_size_consume_queue() as i64
            / consume_queue.get_unit_size() as i64;

        assert_eq!(consume_queue.get_message_total_in_queue(), 1);
        assert_eq!(consume_queue.get_last_offset(), 155);
        assert_eq!(consume_queue.roll_next_file(1), expected_roll);
        assert!(consume_queue.is_first_file_exist());
        assert!(consume_queue.is_first_file_available());
        assert_eq!(
            consume_queue.get_total_size(),
            store.message_store_config_ref().get_mapped_file_size_consume_queue() as i64
        );
    }

    #[test]
    fn flush_consume_queue_service_start_persists_logic_checkpoint_to_disk() {
        let temp_dir = tempdir().unwrap();
        let mut store = ArcMut::new(LocalFileMessageStore::new(
            Arc::new(MessageStoreConfig {
                store_path_root_dir: temp_dir.path().to_string_lossy().to_string().into(),
                flush_interval_consume_queue: 10,
                flush_consume_queue_thorough_interval: 10,
                ..MessageStoreConfig::default()
            }),
            Arc::new(BrokerConfig::default()),
            Arc::new(DashMap::<CheetahString, ArcMut<TopicConfig>>::new()),
            None,
            false,
        ));
        let store_clone = store.clone();
        store.set_message_store_arc(store_clone);

        let expected_timestamp = 4321i64;
        store
            .consume_queue_store
            .put_message_position_info_wrapper(&DispatchRequest {
                topic: CheetahString::from_static_str("flush-cq-service-topic"),
                queue_id: 0,
                commit_log_offset: 128,
                msg_size: 32,
                consume_queue_offset: 0,
                store_timestamp: expected_timestamp,
                success: true,
                ..DispatchRequest::default()
            });

        let checkpoint_path = get_store_checkpoint(store.message_store_config_ref().store_path_root_dir.as_str());
        assert_eq!(
            StoreCheckpoint::new(&checkpoint_path).unwrap().logics_msg_timestamp(),
            0
        );

        store.flush_consume_queue_service.start();

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let checkpoint = StoreCheckpoint::new(&checkpoint_path).unwrap();
            if checkpoint.logics_msg_timestamp() == expected_timestamp as u64 {
                break;
            }

            assert!(
                Instant::now() < deadline,
                "flush consume queue service did not persist checkpoint in time"
            );
            thread::sleep(Duration::from_millis(20));
        }

        store.flush_consume_queue_service.shutdown();
    }

    #[tokio::test]
    async fn default_local_path_supports_consume_queue_time_queries_and_estimation() {
        struct MatchAllFilter;

        impl MessageFilter for MatchAllFilter {
            fn is_matched_by_consume_queue(
                &self,
                _tags_code: Option<i64>,
                _cq_ext_unit: Option<&crate::consume_queue::cq_ext_unit::CqExtUnit>,
            ) -> bool {
                true
            }

            fn is_matched_by_commit_log(
                &self,
                _msg_buffer: Option<&[u8]>,
                _properties: Option<&HashMap<CheetahString, CheetahString>>,
            ) -> bool {
                true
            }
        }

        let temp_dir = tempdir().unwrap();
        let mut store = new_async_flush_test_store(&temp_dir);
        let topic = CheetahString::from_static_str("consume-queue-time-query-topic");

        let mut first = MessageExtBrokerInner::default();
        first.set_topic(topic.clone());
        first.message_ext_inner.set_queue_id(0);
        first.set_body(Bytes::from_static(b"first-body"));

        let first_result = store.put_message(first).await;
        assert_eq!(first_result.put_message_status(), PutMessageStatus::PutOk);

        tokio::time::sleep(Duration::from_millis(5)).await;

        let mut second = MessageExtBrokerInner::default();
        second.set_topic(topic.clone());
        second.message_ext_inner.set_queue_id(0);
        second.set_body(Bytes::from_static(b"second-body"));

        let second_result = store.put_message(second).await;
        assert_eq!(second_result.put_message_status(), PutMessageStatus::PutOk);

        store.reput_once().await;

        let first_store_time = store.get_message_store_timestamp(&topic, 0, 0);
        let second_store_time = store.get_message_store_timestamp(&topic, 0, 1);

        assert!(first_store_time > 0);
        assert!(second_store_time >= first_store_time);
        assert_eq!(store.get_earliest_message_time(&topic, 0), first_store_time);
        assert_eq!(store.get_offset_in_queue_by_time(&topic, 0, first_store_time), 0);

        let consume_queue = store.consume_queue_store.find_or_create_consume_queue(&topic, 0);
        assert_eq!(
            consume_queue.get_offset_in_queue_by_time(second_store_time),
            if second_store_time == first_store_time { 0 } else { 1 }
        );
        assert_eq!(store.estimate_message_count(&topic, 0, 0, 1, &MatchAllFilter), 2);

        let latest = consume_queue.get_latest_unit().expect("latest cq unit");
        assert_eq!(latest.queue_offset, 1);
    }

    #[tokio::test]
    async fn consume_queue_time_query_resolves_duplicate_timestamp_boundaries() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_test_store(&temp_dir);
        let topic = CheetahString::from_static_str("consume-queue-duplicate-time-topic");

        let mut next_commit_log_offset = 0;
        for (queue_offset, store_timestamp, body) in [
            (0_i64, 1_000_i64, Bytes::from_static(b"first")),
            (1, 1_000, Bytes::from_static(b"second")),
            (2, 2_000, Bytes::from_static(b"third")),
        ] {
            let msg_size =
                append_encoded_test_message(&mut store, &topic, next_commit_log_offset, store_timestamp, body).await;
            store
                .consume_queue_store
                .put_message_position_info_wrapper(&DispatchRequest {
                    topic: topic.clone(),
                    queue_id: 0,
                    commit_log_offset: next_commit_log_offset,
                    msg_size,
                    consume_queue_offset: queue_offset,
                    store_timestamp,
                    success: true,
                    ..DispatchRequest::default()
                });
            next_commit_log_offset += msg_size as i64;
        }

        let consume_queue = store.consume_queue_store.find_or_create_consume_queue(&topic, 0);

        assert_eq!(
            consume_queue.get_offset_in_queue_by_time_with_boundary(1_000, BoundaryType::Lower),
            0
        );
        assert_eq!(
            consume_queue.get_offset_in_queue_by_time_with_boundary(1_000, BoundaryType::Upper),
            1
        );
        assert_eq!(
            consume_queue.get_offset_in_queue_by_time_with_boundary(1_500, BoundaryType::Lower),
            2
        );
        assert_eq!(
            consume_queue.get_offset_in_queue_by_time_with_boundary(1_500, BoundaryType::Upper),
            1
        );
    }

    #[tokio::test]
    async fn local_file_consume_queue_store_range_query_and_get_return_encoded_units() {
        let temp_dir = tempdir().unwrap();
        let store = new_test_store(&temp_dir);
        let topic = CheetahString::from_static_str("local-cq-store-range-query-topic");

        for (queue_offset, commit_log_offset, tags_code) in [(0_i64, 100_i64, 11_i64), (1, 132, 22), (2, 164, 33)] {
            store
                .consume_queue_store
                .put_message_position_info_wrapper(&DispatchRequest {
                    topic: topic.clone(),
                    queue_id: 0,
                    commit_log_offset,
                    msg_size: 32,
                    tags_code,
                    consume_queue_offset: queue_offset,
                    store_timestamp: 1000 + queue_offset,
                    success: true,
                    ..DispatchRequest::default()
                });
        }

        let batch = store.consume_queue_store.range_query(&topic, 0, 1, 2).await;
        assert_eq!(batch.len(), 2);
        assert_eq!(decode_cq_bytes(batch[0].clone()), (132, 32, 22));
        assert_eq!(decode_cq_bytes(batch[1].clone()), (164, 32, 33));

        let single = store.consume_queue_store.get(&topic, 0, 0).await;
        assert_eq!(decode_cq_bytes(single), (100, 32, 11));
    }

    #[tokio::test]
    async fn get_message_returns_dispatched_messages_after_reput() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_async_flush_test_store(&temp_dir);
        let topic = CheetahString::from_static_str("get-message-after-reput-topic");
        let group = CheetahString::from_static_str("test-group");

        for body in [Bytes::from_static(b"first-body"), Bytes::from_static(b"second-body")] {
            let mut msg = MessageExtBrokerInner::default();
            msg.set_topic(topic.clone());
            msg.message_ext_inner.set_queue_id(0);
            msg.set_body(body);

            let result = store.put_message(msg).await;
            assert_eq!(result.put_message_status(), PutMessageStatus::PutOk);
        }

        store.reput_once().await;

        let result = store
            .get_message(&group, &topic, 0, 0, 32, None)
            .await
            .expect("get message result");

        assert_eq!(result.status(), Some(GetMessageStatus::Found));
        assert_eq!(result.message_count(), 2);
        assert_eq!(result.message_queue_offset(), &vec![0, 1]);
        assert_eq!(result.next_begin_offset(), 2);
        assert_eq!(result.min_offset(), 0);
        assert_eq!(result.max_offset(), 2);
        assert_eq!(result.message_mapped_list().len(), 2);
    }

    #[tokio::test]
    async fn store_stats_records_single_put_append_totals() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_async_flush_test_store(&temp_dir);
        let topic = CheetahString::from_static_str("single-put-store-stats-topic");
        let stats = store.get_store_stats_service();

        let put_result = store
            .put_message(build_test_message(&topic, Bytes::from_static(b"single-put-body")))
            .await;

        assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
        let append_result = put_result.append_message_result().expect("single put append result");
        assert_eq!(append_result.msg_num, 1);
        assert_eq!(stats.get_put_message_times_total(), append_result.msg_num as u64);
        assert_eq!(stats.get_put_message_size_total(), append_result.wrote_bytes as u64);

        let runtime_info = stats.get_runtime_info();
        assert_eq!(runtime_info["putMessageTimesTotal"], "1");
        assert_eq!(
            runtime_info["putMessageSizeTotal"],
            append_result.wrote_bytes.to_string()
        );
    }

    #[tokio::test]
    async fn store_stats_records_batch_put_append_totals() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_async_flush_test_store(&temp_dir);
        let topic = CheetahString::from_static_str("batch-put-store-stats-topic");
        let stats = store.get_store_stats_service();
        let batch = build_test_batch(
            &topic,
            &[
                Bytes::from_static(b"batch-body-1"),
                Bytes::from_static(b"batch-body-2"),
                Bytes::from_static(b"batch-body-3"),
            ],
        );

        let put_result = store.put_messages(batch).await;

        assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
        let append_result = put_result.append_message_result().expect("batch put append result");
        assert_eq!(append_result.msg_num, 3);
        assert_eq!(stats.get_put_message_times_total(), 3);
        assert_eq!(stats.get_put_message_size_total(), append_result.wrote_bytes as u64);
    }

    #[tokio::test]
    async fn store_stats_records_get_found_miss_and_transferred_counts() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_async_flush_test_store(&temp_dir);
        let topic = CheetahString::from_static_str("get-store-stats-topic");
        let group = CheetahString::from_static_str("get-store-stats-group");
        let stats = store.get_store_stats_service();

        for body in [Bytes::from_static(b"first-body"), Bytes::from_static(b"second-body")] {
            let put_result = store.put_message(build_test_message(&topic, body)).await;
            assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
        }
        store.reput_once().await;

        let found_result = store
            .get_message(&group, &topic, 0, 0, 32, None)
            .await
            .expect("found get result");
        assert_eq!(found_result.status(), Some(GetMessageStatus::Found));
        assert_eq!(found_result.message_count(), 2);
        assert_eq!(stats.get_message_times_total_found().load(Ordering::Relaxed), 1);
        assert_eq!(stats.get_message_times_total_miss().load(Ordering::Relaxed), 0);
        assert_eq!(stats.get_message_transferred_msg_count().load(Ordering::Relaxed), 2);

        let miss_result = store
            .get_message(&group, &topic, 0, 2, 32, None)
            .await
            .expect("miss get result");
        assert_ne!(miss_result.status(), Some(GetMessageStatus::Found));
        assert_eq!(stats.get_message_times_total_found().load(Ordering::Relaxed), 1);
        assert_eq!(stats.get_message_times_total_miss().load(Ordering::Relaxed), 1);
        assert_eq!(stats.get_message_transferred_msg_count().load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn runtime_info_includes_store_offsets_and_timer_defaults_when_timer_disabled() {
        let temp_dir = tempdir().unwrap();
        let store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                timer_wheel_enable: false,
                ..MessageStoreConfig::default()
            },
        );

        let runtime_info = store.get_runtime_info();

        assert!(runtime_info.contains_key("putMessageTimesTotal"));
        assert!(runtime_info.contains_key(RunningStats::CommitLogMinOffset.as_str()));
        assert!(runtime_info.contains_key(RunningStats::CommitLogMaxOffset.as_str()));
        assert_eq!(runtime_info["timerReadBehind"], "0");
        assert_eq!(runtime_info["timerOffsetBehind"], "0");
        assert_eq!(runtime_info["timerCongestNum"], "0");
        assert_eq!(runtime_info["timerEnqueueTps"], "0.0");
        assert_eq!(runtime_info["timerDequeueTps"], "0.0");
        assert_eq!(runtime_info["timerTopicBacklogDistribution"], "{}");
        assert_eq!(runtime_info["timerBacklogDistribution"], "{}");
    }

    #[tokio::test]
    async fn get_ha_runtime_info_reports_current_commitlog_max_offset() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                mapped_file_size_commit_log: 32,
                ..MessageStoreConfig::default()
            },
        );
        store.init().await.expect("init store");

        let data = b"ha";
        assert!(store
            .get_commit_log_mut()
            .append_data(0, data, 0, data.len() as i32)
            .await
            .expect("append commitlog data"));

        let ha_runtime_info = store.get_ha_runtime_info().expect("HA service should be initialized");

        assert!(ha_runtime_info.master);
        assert_eq!(ha_runtime_info.master_commit_log_max_offset, data.len() as u64);
        assert_eq!(ha_runtime_info.in_sync_slave_nums, 0);
    }

    #[tokio::test]
    async fn query_message_returns_indexed_message_after_reput() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_async_flush_test_store(&temp_dir);
        let topic = CheetahString::from_static_str("query-message-after-reput-topic");
        let key = CheetahString::from_static_str("lookup-key");

        let mut msg = MessageExtBrokerInner::default();
        msg.set_topic(topic.clone());
        msg.message_ext_inner.set_queue_id(0);
        msg.set_body(Bytes::from_static(b"query-body"));
        msg.set_keys(key.clone());

        let put_result = store.put_message(msg).await;
        assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);

        store.reput_once().await;

        let result = store
            .query_message(&topic, &key, 10, 0, i64::MAX)
            .await
            .expect("query message result");

        assert_eq!(result.message_maped_list.len(), 1);
        assert!(result.buffer_total_size > 0);
        assert!(result.index_last_update_timestamp > 0);
        assert!(result.get_message_data().is_some());
    }

    #[test]
    fn reput_once_initializes_from_minimum_dispatcher_progress() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_test_store(&temp_dir);
        let topic = CheetahString::from_static_str("reput-init-offset-topic");

        for (queue_offset, commit_log_offset) in [(0_i64, 100_i64), (1, 132_i64)] {
            store
                .consume_queue_store
                .put_message_position_info_wrapper(&DispatchRequest {
                    topic: topic.clone(),
                    queue_id: 0,
                    commit_log_offset,
                    msg_size: 32,
                    consume_queue_offset: queue_offset,
                    store_timestamp: 1000 + queue_offset,
                    success: true,
                    ..DispatchRequest::default()
                });
        }

        store.index_service.build_index(&DispatchRequest {
            topic,
            queue_id: 0,
            commit_log_offset: 100,
            msg_size: 32,
            store_timestamp: 1000,
            keys: CheetahString::from_static_str("lookup-key"),
            success: true,
            ..DispatchRequest::default()
        });

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(store.reput_once());

        let reput_from_offset = store
            .reput_message_service
            .reput_from_offset
            .as_ref()
            .expect("reput offset should exist")
            .load(Ordering::SeqCst);
        assert_eq!(reput_from_offset, 100);
    }

    #[test]
    fn do_recheck_reput_offset_from_dispatchers_rewinds_to_dispatched_commitlog_offset() {
        let temp_dir = tempdir().unwrap();
        let mut store = ArcMut::new(LocalFileMessageStore::new(
            Arc::new(MessageStoreConfig {
                store_path_root_dir: temp_dir.path().to_string_lossy().to_string().into(),
                enable_controller_mode: true,
                ..MessageStoreConfig::default()
            }),
            Arc::new(BrokerConfig {
                enable_controller_mode: true,
                ..BrokerConfig::default()
            }),
            Arc::new(DashMap::<CheetahString, ArcMut<TopicConfig>>::new()),
            None,
            false,
        ));
        let store_clone = store.clone();
        store.set_message_store_arc(store_clone);
        let topic = CheetahString::from_static_str("recheck-reput-offset-topic");

        store.set_confirm_offset(256);
        store.reput_message_service.set_reput_from_offset(256);

        for (queue_offset, commit_log_offset) in [(0_i64, 100_i64), (1, 132_i64)] {
            store
                .consume_queue_store
                .put_message_position_info_wrapper(&DispatchRequest {
                    topic: topic.clone(),
                    queue_id: 0,
                    commit_log_offset,
                    msg_size: 32,
                    consume_queue_offset: queue_offset,
                    store_timestamp: 1000 + queue_offset,
                    success: true,
                    ..DispatchRequest::default()
                });
        }

        store.do_recheck_reput_offset_from_dispatchers();

        let reput_from_offset = store
            .reput_message_service
            .reput_from_offset
            .as_ref()
            .expect("reput offset should exist")
            .load(Ordering::SeqCst);
        assert_eq!(reput_from_offset, 164);
    }

    #[test]
    fn do_recheck_reput_offset_from_dispatchers_uses_minimum_progress_across_cq_and_index() {
        let temp_dir = tempdir().unwrap();
        let mut store = ArcMut::new(LocalFileMessageStore::new(
            Arc::new(MessageStoreConfig {
                store_path_root_dir: temp_dir.path().to_string_lossy().to_string().into(),
                enable_controller_mode: true,
                ..MessageStoreConfig::default()
            }),
            Arc::new(BrokerConfig {
                enable_controller_mode: true,
                ..BrokerConfig::default()
            }),
            Arc::new(DashMap::<CheetahString, ArcMut<TopicConfig>>::new()),
            None,
            false,
        ));
        let store_clone = store.clone();
        store.set_message_store_arc(store_clone);
        let topic = CheetahString::from_static_str("recheck-reput-offset-with-index-topic");

        store.set_confirm_offset(256);
        store.reput_message_service.set_reput_from_offset(256);

        for (queue_offset, commit_log_offset) in [(0_i64, 100_i64), (1, 132_i64)] {
            store
                .consume_queue_store
                .put_message_position_info_wrapper(&DispatchRequest {
                    topic: topic.clone(),
                    queue_id: 0,
                    commit_log_offset,
                    msg_size: 32,
                    consume_queue_offset: queue_offset,
                    store_timestamp: 1000 + queue_offset,
                    success: true,
                    ..DispatchRequest::default()
                });
        }

        store.index_service.build_index(&DispatchRequest {
            topic,
            queue_id: 0,
            commit_log_offset: 100,
            msg_size: 32,
            store_timestamp: 1000,
            keys: CheetahString::from_static_str("lookup-key"),
            success: true,
            ..DispatchRequest::default()
        });

        store.do_recheck_reput_offset_from_dispatchers();

        let reput_from_offset = store
            .reput_message_service
            .reput_from_offset
            .as_ref()
            .expect("reput offset should exist")
            .load(Ordering::SeqCst);
        assert_eq!(reput_from_offset, 100);
    }

    #[test]
    fn get_dispatch_recovery_offset_respects_controller_epoch_start_offset() {
        let temp_dir = tempdir().unwrap();
        let mut store = ArcMut::new(LocalFileMessageStore::new(
            Arc::new(MessageStoreConfig {
                store_path_root_dir: temp_dir.path().to_string_lossy().to_string().into(),
                enable_controller_mode: true,
                ..MessageStoreConfig::default()
            }),
            Arc::new(BrokerConfig {
                enable_controller_mode: true,
                ..BrokerConfig::default()
            }),
            Arc::new(DashMap::<CheetahString, ArcMut<TopicConfig>>::new()),
            None,
            false,
        ));
        let store_clone = store.clone();
        store.set_message_store_arc(store_clone);

        store.set_controller_epoch_start_offset(180);

        assert_eq!(store.get_dispatch_recovery_offset(), 180);
    }

    #[test]
    fn do_recheck_reput_offset_from_dispatchers_respects_controller_epoch_start_offset_floor() {
        let temp_dir = tempdir().unwrap();
        let mut store = ArcMut::new(LocalFileMessageStore::new(
            Arc::new(MessageStoreConfig {
                store_path_root_dir: temp_dir.path().to_string_lossy().to_string().into(),
                enable_controller_mode: true,
                ..MessageStoreConfig::default()
            }),
            Arc::new(BrokerConfig {
                enable_controller_mode: true,
                ..BrokerConfig::default()
            }),
            Arc::new(DashMap::<CheetahString, ArcMut<TopicConfig>>::new()),
            None,
            false,
        ));
        let store_clone = store.clone();
        store.set_message_store_arc(store_clone);
        let topic = CheetahString::from_static_str("recheck-reput-offset-epoch-floor-topic");

        store.set_confirm_offset(256);
        store.set_controller_epoch_start_offset(180);
        store.reput_message_service.set_reput_from_offset(256);

        for (queue_offset, commit_log_offset) in [(0_i64, 100_i64), (1, 132_i64)] {
            store
                .consume_queue_store
                .put_message_position_info_wrapper(&DispatchRequest {
                    topic: topic.clone(),
                    queue_id: 0,
                    commit_log_offset,
                    msg_size: 32,
                    consume_queue_offset: queue_offset,
                    store_timestamp: 1000 + queue_offset,
                    success: true,
                    ..DispatchRequest::default()
                });
        }

        store.do_recheck_reput_offset_from_dispatchers();

        let reput_from_offset = store
            .reput_message_service
            .reput_from_offset
            .as_ref()
            .expect("reput offset should exist")
            .load(Ordering::SeqCst);
        assert_eq!(reput_from_offset, 180);
    }

    #[tokio::test]
    async fn calc_delta_checksum_matches_truncated_range() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_async_flush_test_store(&temp_dir);
        let topic = CheetahString::from_static_str("delta-checksum-truncate-topic");

        let mut first = MessageExtBrokerInner::default();
        first.set_topic(topic.clone());
        first.message_ext_inner.set_queue_id(0);
        first.set_keys(CheetahString::from_static_str("first-key"));
        first.set_body(Bytes::from_static(b"first-checksum-body"));

        let first_result = store.put_message(first).await;
        assert_eq!(first_result.put_message_status(), PutMessageStatus::PutOk);
        let first_append = first_result.append_message_result().expect("first append result");
        let first_end = first_append.wrote_offset + first_append.wrote_bytes as i64;

        let mut second = MessageExtBrokerInner::default();
        second.set_topic(topic);
        second.message_ext_inner.set_queue_id(0);
        second.set_keys(CheetahString::from_static_str("second-key"));
        second.set_body(Bytes::from_static(b"second-checksum-body"));

        let second_result = store.put_message(second).await;
        assert_eq!(second_result.put_message_status(), PutMessageStatus::PutOk);
        let second_append = second_result.append_message_result().expect("second append result");
        assert_eq!(second_append.wrote_offset, first_end);

        let checksum_before_truncate = store.calc_delta_checksum(0, second_append.wrote_offset);
        assert!(!checksum_before_truncate.is_empty());

        assert!(store
            .truncate_files(second_append.wrote_offset)
            .expect("truncate succeeds"));

        let checksum_after_truncate = store.calc_delta_checksum(0, store.get_max_phy_offset());
        assert_eq!(store.get_max_phy_offset(), second_append.wrote_offset);
        assert_eq!(checksum_before_truncate, checksum_after_truncate);
    }

    #[tokio::test]
    async fn truncate_files_keeps_checksum_boundary_consistent() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_async_flush_test_store(&temp_dir);
        let topic = CheetahString::from_static_str("truncate-checksum-boundary-topic");

        for (index, body) in [
            Bytes::from_static(b"boundary-body-1"),
            Bytes::from_static(b"boundary-body-2"),
        ]
        .into_iter()
        .enumerate()
        {
            let mut msg = MessageExtBrokerInner::default();
            msg.set_topic(topic.clone());
            msg.message_ext_inner.set_queue_id(0);
            msg.set_keys(CheetahString::from_string(format!("boundary-key-{index}")));
            msg.set_body(body);

            let result = store.put_message(msg).await;
            assert_eq!(result.put_message_status(), PutMessageStatus::PutOk);
        }

        let max_phy_offset_before_truncate = store.get_max_phy_offset();
        let first_message_end = store
            .look_message_by_offset(0)
            .map(|message| message.commit_log_offset + message.store_size as i64)
            .expect("first message");

        let full_checksum = store.calc_delta_checksum(0, max_phy_offset_before_truncate);
        let first_range_checksum = store.calc_delta_checksum(0, first_message_end);
        assert!(!full_checksum.is_empty());
        assert!(!first_range_checksum.is_empty());
        assert_ne!(full_checksum, first_range_checksum);

        assert!(store.truncate_files(first_message_end).expect("truncate succeeds"));

        let truncated_checksum = store.calc_delta_checksum(0, store.get_max_phy_offset());
        assert_eq!(store.get_max_phy_offset(), first_message_end);
        assert_eq!(truncated_checksum, first_range_checksum);
    }

    #[tokio::test]
    async fn put_message_with_multi_dispatch_properties_dispatches_lmq_queues_after_reput() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                enable_multi_dispatch: true,
                enable_lmq: true,
                flush_disk_type: FlushDiskType::AsyncFlush,
                ..MessageStoreConfig::default()
            },
        );
        let source_topic = CheetahString::from_static_str("multi-dispatch-source-topic");
        let lmq_alpha = CheetahString::from_static_str("%LMQ%alpha");
        let lmq_beta = CheetahString::from_static_str("%LMQ%beta");

        let mut msg = MessageExtBrokerInner::default();
        msg.set_topic(source_topic);
        msg.message_ext_inner.set_queue_id(0);
        msg.set_body(Bytes::from_static(b"multi-dispatch-body"));
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_DISPATCH),
            CheetahString::from_static_str("%LMQ%alpha,%LMQ%beta"),
        );

        let put_result = store.put_message(msg).await;
        assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);

        store.reput_once().await;

        let alpha_queue = store.consume_queue_store.find_or_create_consume_queue(&lmq_alpha, 0);
        let beta_queue = store.consume_queue_store.find_or_create_consume_queue(&lmq_beta, 0);

        assert_eq!(alpha_queue.get_message_total_in_queue(), 1);
        assert_eq!(beta_queue.get_message_total_in_queue(), 1);
        assert_eq!(store.consume_queue_store.get_lmq_queue_offset("%LMQ%alpha-0"), 1);
        assert_eq!(store.consume_queue_store.get_lmq_queue_offset("%LMQ%beta-0"), 1);
    }

    #[tokio::test]
    async fn put_message_with_existing_multi_queue_offsets_still_updates_lmq_offsets() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                enable_multi_dispatch: true,
                enable_lmq: true,
                flush_disk_type: FlushDiskType::AsyncFlush,
                ..MessageStoreConfig::default()
            },
        );
        let source_topic = CheetahString::from_static_str("multi-dispatch-existing-offset-topic");

        let mut msg = MessageExtBrokerInner::default();
        msg.set_topic(source_topic);
        msg.message_ext_inner.set_queue_id(0);
        msg.set_body(Bytes::from_static(b"multi-dispatch-existing-offset-body"));
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_DISPATCH),
            CheetahString::from_static_str("%LMQ%alpha,%LMQ%beta"),
        );
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET),
            CheetahString::from_static_str("3,5"),
        );

        let put_result = store.put_message(msg).await;

        assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
        assert_eq!(store.consume_queue_store.get_lmq_queue_offset("%LMQ%alpha-0"), 1);
        assert_eq!(store.consume_queue_store.get_lmq_queue_offset("%LMQ%beta-0"), 1);
    }

    #[tokio::test]
    async fn put_message_with_existing_mixed_multi_queue_offsets_updates_lmq_offsets() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                enable_multi_dispatch: true,
                enable_lmq: true,
                flush_disk_type: FlushDiskType::AsyncFlush,
                ..MessageStoreConfig::default()
            },
        );
        let source_topic = CheetahString::from_static_str("multi-dispatch-mixed-offset-topic");

        let mut msg = MessageExtBrokerInner::default();
        msg.set_topic(source_topic);
        msg.message_ext_inner.set_queue_id(0);
        msg.set_body(Bytes::from_static(b"multi-dispatch-mixed-offset-body"));
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_DISPATCH),
            CheetahString::from_static_str("%LMQ%alpha,normal-queue"),
        );
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET),
            CheetahString::from_static_str("3,5"),
        );

        let put_result = store.put_message(msg).await;

        assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
        assert_eq!(store.consume_queue_store.get_lmq_queue_offset("%LMQ%alpha-0"), 1);
        assert_eq!(store.consume_queue_store.get_lmq_queue_offset("normal-queue-0"), 0);
    }

    #[test]
    fn multi_dispatch_arrival_uses_multi_queue_offsets_without_allocating_vectors() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                enable_lmq: true,
                ..MessageStoreConfig::default()
            },
        );
        let arrivals = install_recording_arriving_listener(&mut store);
        let inner = reput_inner_for_store(store.clone());
        let mut properties = HashMap::new();
        properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_DISPATCH),
            CheetahString::from_static_str("%LMQ%alpha,%LMQ%beta"),
        );
        properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET),
            CheetahString::from_static_str("3,5"),
        );
        let mut dispatch_request = DispatchRequest {
            topic: CheetahString::from_static_str("multi-arrival-source-topic"),
            queue_id: 7,
            tags_code: 11,
            store_timestamp: 99,
            bit_map: Some(vec![1, 2]),
            properties_map: Some(properties),
            ..DispatchRequest::default()
        };

        inner.notify_message_arrive4multi_queue(&mut dispatch_request);

        let arrivals = arrivals.lock().unwrap();
        assert_eq!(arrivals.len(), 2);
        assert_eq!(arrivals[0].topic, CheetahString::from_static_str("%LMQ%alpha"));
        assert_eq!(arrivals[0].queue_id, 0);
        assert_eq!(arrivals[0].logic_offset, 4);
        assert_eq!(arrivals[0].filter_bit_map, Some(vec![1, 2]));
        assert_eq!(arrivals[1].topic, CheetahString::from_static_str("%LMQ%beta"));
        assert_eq!(arrivals[1].queue_id, 0);
        assert_eq!(arrivals[1].logic_offset, 6);
    }

    #[test]
    fn multi_dispatch_arrival_ignores_invalid_offsets_without_panicking() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(&temp_dir, MessageStoreConfig::default());
        let arrivals = install_recording_arriving_listener(&mut store);
        let inner = reput_inner_for_store(store.clone());
        let mut properties = HashMap::new();
        properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_DISPATCH),
            CheetahString::from_static_str("queue-a,queue-b"),
        );
        properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET),
            CheetahString::from_static_str("1,not-a-number"),
        );
        let mut dispatch_request = DispatchRequest {
            topic: CheetahString::from_static_str("multi-arrival-invalid-offset-topic"),
            queue_id: 3,
            properties_map: Some(properties),
            ..DispatchRequest::default()
        };

        inner.notify_message_arrive4multi_queue(&mut dispatch_request);

        assert!(arrivals.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn clean_commit_log_service_run_deletes_expired_files_and_advances_min_offset() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                mapped_file_size_commit_log: 32,
                file_reserved_time: 0,
                delete_commit_log_files_interval: 0,
                destroy_mapped_file_interval_forcibly: 0,
                redelete_hanged_file_interval: 0,
                delete_file_batch_max: 10,
                delete_when: "99".to_string(),
                disk_max_used_space_ratio: 95,
                clean_file_forcibly_enable: false,
                ..MessageStoreConfig::default()
            },
        );
        store.init().await.expect("init store");

        for (offset, fill) in [(0_i64, b'A'), (32, b'B'), (64, b'C')] {
            let data = vec![fill; 32];
            assert!(store
                .get_commit_log_mut()
                .append_data(offset, &data, 0, data.len() as i32)
                .await
                .expect("append commitlog file"));
        }

        assert_eq!(store.get_min_phy_offset(), 0);
        assert_eq!(store.get_last_file_from_offset(), 64);

        store.clean_commit_log_service.execute_delete_files_manually();
        store.clean_commit_log_service.run();

        assert_eq!(store.get_min_phy_offset(), 64);
        assert_eq!(store.get_last_file_from_offset(), 64);
        assert_eq!(store.get_commit_log().get_max_offset(), 96);
    }

    #[tokio::test]
    async fn clean_commit_log_service_run_skips_expired_files_outside_delete_window() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                mapped_file_size_commit_log: 32,
                file_reserved_time: 0,
                delete_commit_log_files_interval: 0,
                destroy_mapped_file_interval_forcibly: 0,
                redelete_hanged_file_interval: 0,
                delete_file_batch_max: 10,
                delete_when: "99".to_string(),
                disk_max_used_space_ratio: 95,
                ..MessageStoreConfig::default()
            },
        );
        store.init().await.expect("init store");

        for (offset, fill) in [(0_i64, b'A'), (32, b'B'), (64, b'C')] {
            let data = vec![fill; 32];
            assert!(store
                .get_commit_log_mut()
                .append_data(offset, &data, 0, data.len() as i32)
                .await
                .expect("append commitlog file"));
        }

        store.clean_commit_log_service.run();

        assert_eq!(store.get_min_phy_offset(), 0);
        assert_eq!(store.get_last_file_from_offset(), 64);
        assert_eq!(store.get_commit_log().get_max_offset(), 96);
    }

    #[test]
    fn clean_commit_log_service_clamps_disk_cleanup_ratios_like_java() {
        let temp_dir = tempdir().unwrap();
        let store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                disk_space_warning_level_ratio: 1,
                disk_space_clean_forcibly_ratio: 100,
                disk_max_used_space_ratio: 1,
                ..MessageStoreConfig::default()
            },
        );

        assert_eq!(store.clean_commit_log_service.disk_space_warning_level_ratio(), 0.35);
        assert_eq!(store.clean_commit_log_service.disk_space_clean_forcibly_ratio(), 0.85);
        assert_eq!(store.clean_commit_log_service.disk_max_used_space_ratio(), 0.10);
    }

    #[test]
    fn clean_commit_log_service_manual_delete_uses_java_retry_budget() {
        let temp_dir = tempdir().unwrap();
        let store = new_test_store(&temp_dir);

        store.clean_commit_log_service.execute_delete_files_manually();

        assert_eq!(
            store
                .clean_commit_log_service
                .manual_delete_requests
                .load(Ordering::SeqCst),
            CleanCommitLogService::MAX_MANUAL_DELETE_FILE_TIMES
        );
        assert!(store.clean_commit_log_service.consume_manual_delete_request());
        assert_eq!(
            store
                .clean_commit_log_service
                .manual_delete_requests
                .load(Ordering::SeqCst),
            CleanCommitLogService::MAX_MANUAL_DELETE_FILE_TIMES - 1
        );
    }

    #[tokio::test]
    async fn clean_commit_log_service_run_deletes_when_disk_usage_is_unavailable() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                mapped_file_size_commit_log: 32,
                file_reserved_time: 0,
                delete_commit_log_files_interval: 0,
                destroy_mapped_file_interval_forcibly: 0,
                redelete_hanged_file_interval: 0,
                delete_file_batch_max: 10,
                delete_when: "99".to_string(),
                disk_max_used_space_ratio: 95,
                clean_file_forcibly_enable: false,
                ..MessageStoreConfig::default()
            },
        );
        store.init().await.expect("init store");
        let logic_path = LocalFileMessageStore::get_store_path_logic(&store.message_store_config);
        fs::remove_dir_all(logic_path).expect("remove logic store path");

        for (offset, fill) in [(0_i64, b'A'), (32, b'B'), (64, b'C')] {
            let data = vec![fill; 32];
            assert!(store
                .get_commit_log_mut()
                .append_data(offset, &data, 0, data.len() as i32)
                .await
                .expect("append commitlog file"));
        }

        store.clean_commit_log_service.run();

        assert_eq!(store.get_min_phy_offset(), 64);
        assert_eq!(store.get_last_file_from_offset(), 64);
    }

    #[tokio::test]
    async fn correct_logic_offset_service_run_updates_min_offset_after_commitlog_cleanup() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                mapped_file_size_commit_log: 32,
                mapped_file_size_consume_queue: 40,
                file_reserved_time: 0,
                delete_commit_log_files_interval: 0,
                destroy_mapped_file_interval_forcibly: 0,
                redelete_hanged_file_interval: 0,
                delete_file_batch_max: 10,
                delete_when: "99".to_string(),
                disk_max_used_space_ratio: 95,
                clean_file_forcibly_enable: false,
                ..MessageStoreConfig::default()
            },
        );
        store.init().await.expect("init store");

        for (offset, fill) in [(0_i64, b'A'), (32, b'B'), (64, b'C')] {
            let data = vec![fill; 32];
            assert!(store
                .get_commit_log_mut()
                .append_data(offset, &data, 0, data.len() as i32)
                .await
                .expect("append commitlog file"));
        }

        let topic = CheetahString::from_static_str("correct-logic-offset-topic");
        for (queue_offset, commit_log_offset) in [(0_i64, 0_i64), (1, 32), (2, 64)] {
            store
                .consume_queue_store
                .put_message_position_info_wrapper(&DispatchRequest {
                    topic: topic.clone(),
                    queue_id: 0,
                    commit_log_offset,
                    msg_size: 32,
                    consume_queue_offset: queue_offset,
                    ..Default::default()
                });
        }

        let consume_queue = store.consume_queue_store.find_or_create_consume_queue(&topic, 0);
        assert_eq!(consume_queue.get_min_offset_in_queue(), 0);

        store.clean_commit_log_service.execute_delete_files_manually();
        store.clean_commit_log_service.run();
        assert_eq!(store.get_min_phy_offset(), 64);

        store.correct_logic_offset_service.run();

        let consume_queue = store.consume_queue_store.find_or_create_consume_queue(&topic, 0);
        assert_eq!(consume_queue.get_min_offset_in_queue(), 2);
        assert_eq!(consume_queue.get_message_total_in_queue(), 1);
    }

    #[tokio::test]
    async fn clean_consume_queue_service_run_cleans_files_and_removes_fully_expired_queue() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                mapped_file_size_commit_log: 32,
                mapped_file_size_consume_queue: 40,
                file_reserved_time: 0,
                delete_commit_log_files_interval: 0,
                destroy_mapped_file_interval_forcibly: 0,
                redelete_hanged_file_interval: 0,
                delete_file_batch_max: 10,
                delete_when: "99".to_string(),
                disk_max_used_space_ratio: 95,
                clean_file_forcibly_enable: false,
                ..MessageStoreConfig::default()
            },
        );
        store.init().await.expect("init store");

        for (offset, fill) in [(0_i64, b'A'), (32, b'B'), (64, b'C')] {
            let data = vec![fill; 32];
            assert!(store
                .get_commit_log_mut()
                .append_data(offset, &data, 0, data.len() as i32)
                .await
                .expect("append commitlog file"));
        }

        let active_topic = CheetahString::from_static_str("active-clean-topic");
        for (queue_offset, commit_log_offset) in [(0_i64, 0_i64), (1, 32), (2, 64)] {
            store
                .consume_queue_store
                .put_message_position_info_wrapper(&DispatchRequest {
                    topic: active_topic.clone(),
                    queue_id: 0,
                    commit_log_offset,
                    msg_size: 32,
                    consume_queue_offset: queue_offset,
                    ..Default::default()
                });
        }

        let expired_topic = CheetahString::from_static_str("expired-clean-topic");
        store
            .consume_queue_store
            .put_message_position_info_wrapper(&DispatchRequest {
                topic: expired_topic.clone(),
                queue_id: 0,
                commit_log_offset: 0,
                msg_size: 32,
                consume_queue_offset: 0,
                ..Default::default()
            });

        store.clean_commit_log_service.execute_delete_files_manually();
        store.clean_commit_log_service.run();
        assert_eq!(store.get_min_phy_offset(), 64);

        store.clean_consume_queue_service.run();

        let active_consume_queue = store.consume_queue_store.find_or_create_consume_queue(&active_topic, 0);
        assert_eq!(active_consume_queue.get_min_offset_in_queue(), 2);
        assert_eq!(active_consume_queue.get_message_total_in_queue(), 1);
        assert!(store
            .consume_queue_store
            .find_consume_queue_map(&expired_topic)
            .is_none());
    }

    #[tokio::test]
    async fn clean_expired_removes_trimmed_queue_directly() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                mapped_file_size_commit_log: 32,
                mapped_file_size_consume_queue: 40,
                file_reserved_time: 0,
                delete_commit_log_files_interval: 0,
                destroy_mapped_file_interval_forcibly: 0,
                redelete_hanged_file_interval: 0,
                delete_file_batch_max: 10,
                delete_when: "99".to_string(),
                disk_max_used_space_ratio: 95,
                clean_file_forcibly_enable: false,
                ..MessageStoreConfig::default()
            },
        );
        store.init().await.expect("init store");

        for (offset, fill) in [(0_i64, b'A'), (32, b'B'), (64, b'C')] {
            let data = vec![fill; 32];
            assert!(store
                .get_commit_log_mut()
                .append_data(offset, &data, 0, data.len() as i32)
                .await
                .expect("append commitlog file"));
        }

        let expired_topic = CheetahString::from_static_str("expired-clean-direct-topic");
        store
            .consume_queue_store
            .put_message_position_info_wrapper(&DispatchRequest {
                topic: expired_topic.clone(),
                queue_id: 0,
                commit_log_offset: 0,
                msg_size: 32,
                consume_queue_offset: 0,
                ..Default::default()
            });

        store.clean_commit_log_service.execute_delete_files_manually();
        store.clean_commit_log_service.run();
        store.correct_logic_offset_service.run();

        let expired_consume_queue = store
            .consume_queue_store
            .find_or_create_consume_queue(&expired_topic, 0);
        assert_eq!(expired_consume_queue.get_min_offset_in_queue(), 1);
        assert_eq!(expired_consume_queue.get_message_total_in_queue(), 0);

        store.consume_queue_store.clean_expired(64).await;

        assert!(store
            .consume_queue_store
            .find_consume_queue_map(&expired_topic)
            .is_none());
    }
}
