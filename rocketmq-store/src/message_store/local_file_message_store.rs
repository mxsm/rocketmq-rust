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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::RwLock as StdRwLock;
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
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_rust::ArcMut;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::fetcher::TieredGetMessageResult;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::fetcher::TieredGetMessageStatus;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::fetcher::TieredQueryResult;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::TieredLifecycle;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::TieredMessageFetcher;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::TieredStore;
use tokio::sync::Mutex;
use tokio::sync::Notify;
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
use crate::base::message_store::StoreHealthSnapshot;
use crate::base::query_message_result::QueryMessageResult;
#[cfg(feature = "tieredstore")]
use crate::base::select_result::SelectMappedBufferCacheState;
use crate::base::select_result::SelectMappedBufferResult;
#[cfg(feature = "tieredstore")]
use crate::base::select_result::SelectMappedBufferSourceKind;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::base::store_stats_service::StoreStatsService;
use crate::base::transient_store_pool::TransientStorePool;
use crate::config::flush_disk_type::FlushDiskType;
use crate::config::message_store_config::LinuxMemoryLockMode;
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
use crate::kv::compaction_dispatch::CommitLogDispatcherCompaction;
use crate::kv::compaction_service::CompactionService;
use crate::kv::compaction_store::CompactionStore;
use crate::log_file::commit_log;
use crate::log_file::commit_log::CommitLog;
use crate::log_file::mapped_file::MappedFile;
use crate::log_file::MAX_PULL_MSG_SIZE;
use crate::message_store::recovery::ConsumeQueueRecoveryConcurrency;
use crate::message_store::recovery::RecoveryCrcPolicy;
use crate::message_store::recovery::RecoveryExecutor;
use crate::message_store::recovery::RecoveryExit;
use crate::message_store::recovery::RecoveryIndexRepairPolicy;
use crate::message_store::recovery::RecoveryPhase;
use crate::message_store::recovery::RecoveryPlan;
use crate::message_store::recovery::RecoveryReport;
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
#[cfg(feature = "tieredstore")]
use crate::tieredstore::TieredCommitLogDispatcher;
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
    RecoveringConsumeQueue = 4,
    RecoveringCommitLog = 5,
    RecoveringTopicQueueTable = 6,
}

impl StoreLifecycleState {
    #[inline]
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Initialized,
            2 => Self::Started,
            3 => Self::Shutdown,
            4 => Self::RecoveringConsumeQueue,
            5 => Self::RecoveringCommitLog,
            6 => Self::RecoveringTopicQueueTable,
            _ => Self::Created,
        }
    }

    #[inline]
    fn as_u8(self) -> u8 {
        self as u8
    }

    #[inline]
    fn is_recovering(self) -> bool {
        matches!(
            self,
            Self::RecoveringConsumeQueue | Self::RecoveringCommitLog | Self::RecoveringTopicQueueTable
        )
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
    #[cfg(feature = "tieredstore")]
    tiered_store: Option<Arc<TieredStore>>,
    broker_init_max_offset: Arc<AtomicI64>,
    state_machine_version: Arc<AtomicI64>,
    lifecycle_state: Arc<AtomicU8>,
    controller_epoch_start_offset: Arc<AtomicI64>,
    shutdown: Arc<AtomicBool>,
    background_index_query_degradation_total: Arc<AtomicU64>,
    store_lock_guard: Option<StoreLockGuard>,
    running_flags: Arc<RunningFlags>,
    reput_message_service: ReputMessageService,
    background_index_rebuild_service: BackgroundIndexRebuildService,
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
    scheduled_task_group: Option<rocketmq_runtime::TaskGroup>,
    scheduled_tasks: Option<ScheduledTaskGroup>,
    ha_update_master_group: Arc<StdMutex<Option<rocketmq_runtime::TaskGroup>>>,
    delay_level_table: ArcMut<BTreeMap<i32 /* level */, i64 /* delay timeMillis */>>,
    max_delay_level: i32,
    last_recovery_report: Option<RecoveryReport>,
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
        Self::try_new(
            message_store_config,
            broker_config,
            topic_config_table,
            broker_stats_manager,
            notify_message_arrive_in_batch,
        )
        .unwrap_or_else(|error| panic!("failed to create local file message store: {error}"))
    }

    pub fn try_new(
        message_store_config: Arc<MessageStoreConfig>,
        broker_config: Arc<BrokerConfig>,
        topic_config_table: Arc<DashMap<CheetahString, ArcMut<TopicConfig>>>,
        broker_stats_manager: Option<Arc<BrokerStatsManager>>,
        notify_message_arrive_in_batch: bool,
    ) -> Result<Self, StoreError> {
        let (delay_level_table, max_delay_level) = parse_delay_level(message_store_config.message_delay_level.as_str());
        let running_flags = Arc::new(RunningFlags::new());
        let store_checkpoint = Arc::new(
            StoreCheckpoint::new(get_store_checkpoint(message_store_config.store_path_root_dir.as_str())).map_err(
                |error| {
                    StoreError::General(format!(
                        "failed to create store checkpoint under {}: {error}",
                        message_store_config.store_path_root_dir
                    ))
                },
            )?,
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

        let mut dispatcher = ArcMut::new(CommitLogDispatcherDefault {
            dispatcher_vec: vec![build_consume_queue, build_index],
        });

        let memory_lock_budget_bytes = Self::effective_linux_memory_lock_budget_bytes(message_store_config.as_ref());
        let transient_store_pool = TransientStorePool::new_with_memory_lock_budget(
            message_store_config.transient_store_pool_size,
            message_store_config.mapped_file_size_commit_log,
            memory_lock_budget_bytes,
        );
        let transient_store_pool_enable = message_store_config.transient_store_pool_enable
            && (broker_config.enable_controller_mode || message_store_config.broker_role != BrokerRole::Slave);
        let allocate_transient_store_pool = transient_store_pool_enable.then(|| Arc::new(transient_store_pool.clone()));
        let allocate_mapped_file_service = Arc::new(AllocateMappedFileService::new_with_message_store_config(
            allocate_transient_store_pool,
            transient_store_pool_enable,
            message_store_config.fast_fail_if_no_buffer_in_store_pool,
            message_store_config.as_ref(),
        ));

        let commit_log = ArcMut::new(CommitLog::new(
            message_store_config.clone(),
            broker_config.clone(),
            dispatcher.clone(),
            store_checkpoint.clone(),
            topic_config_table.clone(),
            consume_queue_store.clone(),
            (*allocate_mapped_file_service).clone(),
        ));
        let compaction_store = Arc::new(CompactionStore::new());
        if message_store_config.enable_compaction {
            dispatcher.add_dispatcher(Arc::new(CommitLogDispatcherCompaction::new(
                compaction_store.clone(),
                commit_log.clone(),
                message_store_config.clone(),
                topic_config_table.clone(),
            )));
        }
        #[cfg(feature = "tieredstore")]
        let tiered_store = Self::build_tiered_store(message_store_config.clone(), commit_log.clone(), &mut dispatcher)?;

        ensure_dir_ok(message_store_config.store_path_root_dir.as_str());
        ensure_dir_ok(Self::get_store_path_physic(&message_store_config).as_str());
        ensure_dir_ok(Self::get_store_path_logic(&message_store_config).as_str());

        let identity = broker_config.broker_identity.clone();
        let compaction_service = message_store_config.enable_compaction.then(|| {
            CompactionService::new(
                compaction_store.clone(),
                message_store_config.compaction_schedule_internal,
            )
        });
        Ok(Self {
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
            allocate_mapped_file_service,
            consume_queue_store: consume_queue_store.clone(),
            dispatcher,
            #[cfg(feature = "tieredstore")]
            tiered_store,
            broker_init_max_offset: Arc::new(AtomicI64::new(-1)),
            state_machine_version: Arc::new(AtomicI64::new(0)),
            lifecycle_state: Arc::new(AtomicU8::new(StoreLifecycleState::Created.as_u8())),
            controller_epoch_start_offset: Arc::new(AtomicI64::new(-1)),
            shutdown: Arc::new(AtomicBool::new(false)),
            background_index_query_degradation_total: Arc::new(AtomicU64::new(0)),
            store_lock_guard: None,
            running_flags: running_flags.clone(),
            reput_message_service: ReputMessageService {
                shutdown_token: CancellationToken::new(),
                new_message_notify: Arc::new(Notify::new()),
                dispatch_progress_notify: Arc::new(Notify::new()),
                pending_messages: Arc::new(AtomicI64::new(0)),
                reput_from_offset: None,
                dispatch_tx: None,
                inner: None,
                task_group: None,
            },
            background_index_rebuild_service: BackgroundIndexRebuildService::new(),
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
            compaction_store,
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
            scheduled_task_group: None,
            scheduled_tasks: None,
            ha_update_master_group: Arc::new(StdMutex::new(None)),
            delay_level_table: ArcMut::new(delay_level_table),
            max_delay_level,
            last_recovery_report: None,
        })
    }

    fn effective_linux_memory_lock_budget_bytes(message_store_config: &MessageStoreConfig) -> u64 {
        message_store_config.effective_linux_memory_lock_budget_bytes(
            crate::platform::current_store_platform_capability().memory_lock_limit_bytes,
        )
    }

    #[cfg(feature = "tieredstore")]
    fn build_tiered_store(
        message_store_config: Arc<MessageStoreConfig>,
        commit_log: ArcMut<CommitLog>,
        dispatcher: &mut CommitLogDispatcherDefault,
    ) -> Result<Option<Arc<TieredStore>>, StoreError> {
        let Some(tiered_store_config) = message_store_config.tiered_store_config.clone() else {
            return Ok(None);
        };
        if !tiered_store_config.storage_level.enabled() {
            return Ok(None);
        }

        let tiered_store = Arc::new(TieredStore::new(tiered_store_config).map_err(|error| {
            StoreError::General(format!(
                "failed to create tieredstore for local file message store: {error}"
            ))
        })?);
        let commit_log_for_dispatch = commit_log;
        let body_resolver = Arc::new(move |request: &DispatchRequest| -> Option<Bytes> {
            Self::resolve_tiered_dispatch_body(&commit_log_for_dispatch, request)
        });
        dispatcher.add_dispatcher(Arc::new(TieredCommitLogDispatcher::new(
            tiered_store.dispatcher(),
            body_resolver,
        )));

        Ok(Some(tiered_store))
    }

    #[cfg(feature = "tieredstore")]
    fn resolve_tiered_dispatch_body(commit_log: &CommitLog, request: &DispatchRequest) -> Option<Bytes> {
        if request.commit_log_offset < 0 || request.msg_size <= 0 {
            return None;
        }

        let size = usize::try_from(request.msg_size).ok()?;
        let result = commit_log.get_message(request.commit_log_offset, request.msg_size)?;
        let bytes = result.get_bytes()?;
        if bytes.len() < size {
            return None;
        }
        Some(bytes.slice(..size))
    }

    #[cfg(feature = "tieredstore")]
    fn select_result_from_tiered_message(message: Bytes) -> SelectMappedBufferResult {
        SelectMappedBufferResult {
            start_offset: 0,
            size: message.len() as i32,
            bytes: Some(message),
            mapped_file: None,
            is_in_cache: false,
            source_kind: SelectMappedBufferSourceKind::Bytes,
            file_offset: 0,
            cache_state: SelectMappedBufferCacheState::Unknown,
        }
    }

    #[cfg(feature = "tieredstore")]
    fn map_tiered_get_status(status: TieredGetMessageStatus) -> GetMessageStatus {
        match status {
            TieredGetMessageStatus::Found => GetMessageStatus::Found,
            TieredGetMessageStatus::NoMatchedMessage => GetMessageStatus::NoMatchedMessage,
            TieredGetMessageStatus::OffsetFoundNull => GetMessageStatus::OffsetFoundNull,
            TieredGetMessageStatus::OffsetOverflowBadly => GetMessageStatus::OffsetOverflowBadly,
            TieredGetMessageStatus::OffsetOverflowOne => GetMessageStatus::OffsetOverflowOne,
            TieredGetMessageStatus::OffsetTooSmall => GetMessageStatus::OffsetTooSmall,
            TieredGetMessageStatus::NoMatchedLogicQueue => GetMessageStatus::NoMatchedLogicQueue,
        }
    }

    #[cfg(feature = "tieredstore")]
    fn should_try_tiered_get_message(status: GetMessageStatus) -> bool {
        matches!(
            status,
            GetMessageStatus::NoMatchedLogicQueue
                | GetMessageStatus::NoMessageInQueue
                | GetMessageStatus::OffsetTooSmall
                | GetMessageStatus::OffsetFoundNull
                | GetMessageStatus::MessageWasRemoving
        )
    }

    #[cfg(feature = "tieredstore")]
    fn should_try_tiered_offset_by_time(&self, topic: &CheetahString, queue_id: i32, timestamp: i64) -> bool {
        let Some(logic_queue) = self.get_consume_queue(topic, queue_id) else {
            return true;
        };
        if logic_queue.get_message_total_in_queue() <= 0 {
            return true;
        }
        logic_queue
            .get_earliest_unit_and_store_time()
            .map(|(_, earliest_store_time)| timestamp < earliest_store_time)
            .unwrap_or(true)
    }

    #[cfg(feature = "tieredstore")]
    fn to_store_get_message_result(
        fetched: TieredGetMessageResult,
        requested_offset: i64,
        max_total_msg_size: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> GetMessageResult {
        let mut result = GetMessageResult::new_result_size(fetched.messages.len());
        result.set_min_offset(fetched.min_offset);
        result.set_max_offset(fetched.max_offset);
        result.set_next_begin_offset(fetched.next_begin_offset);

        let status = Self::map_tiered_get_status(fetched.status);
        if status != GetMessageStatus::Found {
            result.set_status(Some(status));
            return result;
        }

        let max_total_msg_size = max_total_msg_size.max(0);
        let mut next_queue_offset = requested_offset;
        for message in fetched.messages {
            if let Some(filter) = message_filter.as_ref() {
                if !filter.is_matched_by_commit_log(Some(message.as_ref()), None) {
                    next_queue_offset = next_queue_offset.saturating_add(1);
                    continue;
                }
            }
            if result.buffer_total_size() > 0
                && result.buffer_total_size().saturating_add(message.len() as i32) > max_total_msg_size
            {
                break;
            }
            let queue_offset = next_queue_offset.max(0) as u64;
            result.add_message(Self::select_result_from_tiered_message(message), queue_offset, 1);
            next_queue_offset = next_queue_offset.saturating_add(1);
        }

        result.set_status(Some(if result.message_count() > 0 {
            GetMessageStatus::Found
        } else {
            GetMessageStatus::NoMatchedMessage
        }));
        result
    }

    #[cfg(feature = "tieredstore")]
    async fn get_message_from_tiered_store(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Option<GetMessageResult> {
        let tiered_store = self.tiered_store.as_ref()?;
        let metrics = tiered_store.metrics();
        metrics.record_get_message_fallback(topic.as_str(), group.as_str());
        match tiered_store
            .fetcher()
            .get_message(topic.to_string(), queue_id, offset, max_msg_nums)
            .await
        {
            Ok(fetched) => {
                let result = Self::to_store_get_message_result(fetched, offset, max_total_msg_size, message_filter);
                if result.status() == Some(GetMessageStatus::Found) {
                    metrics.record_messages_out(topic.as_str(), group.as_str(), result.message_count().max(0) as u64);
                }
                Some(result)
            }
            Err(error) => {
                warn!(
                    group = %group,
                    topic = %topic,
                    queue_id,
                    offset,
                    error = %error,
                    "tieredstore get_message fallback failed"
                );
                None
            }
        }
    }

    #[cfg(feature = "tieredstore")]
    fn to_store_query_message_result(fetched: TieredQueryResult<Bytes>, fallback_timestamp: i64) -> QueryMessageResult {
        let mut result = QueryMessageResult {
            index_last_update_timestamp: fallback_timestamp,
            ..QueryMessageResult::default()
        };
        for message in fetched.values {
            result.add_message(Self::select_result_from_tiered_message(message));
        }
        result
    }

    #[cfg(feature = "tieredstore")]
    async fn query_message_from_tiered_store(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Option<QueryMessageResult> {
        let tiered_store = self.tiered_store.as_ref()?;
        match tiered_store
            .fetcher()
            .query_message(
                topic.to_string(),
                key.to_string(),
                max_num,
                begin_timestamp,
                end_timestamp,
            )
            .await
        {
            Ok(fetched) if !fetched.values.is_empty() => {
                Some(Self::to_store_query_message_result(fetched, end_timestamp))
            }
            Ok(_) => None,
            Err(error) => {
                warn!(
                    topic = %topic,
                    key = %key,
                    error = %error,
                    "tieredstore query_message fallback failed"
                );
                None
            }
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

    #[cfg(feature = "tieredstore")]
    pub fn tiered_store_metrics(
        &self,
    ) -> Option<Arc<rocketmq_observability::metrics::tiered_store::TieredStoreMetrics>> {
        self.tiered_store.as_ref().map(|tiered_store| tiered_store.metrics())
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
        if message_store_config.trans_rocksdb_enable {
            enabled.push("trans_rocksdb_enable");
        }
        enabled
    }

    fn validate_supported_configuration(&self) -> Result<(), StoreError> {
        if Self::is_dledger_commit_log_enabled_config(self.message_store_config.as_ref()) {
            return Err(StoreError::General(
                "DLedger commit log is Java-specific and is intentionally unsupported in rocketmq-rust".to_string(),
            ));
        }
        if self.message_store_config.timer_rocksdb_enable && !self.message_store_config.is_enable_rocksdb_store() {
            return Err(StoreError::General(
                "Timer RocksDB backend is not implemented in rocketmq-rust; keep timer_rocksdb_enable=false"
                    .to_string(),
            ));
        }

        let enabled_rocksdb_options = Self::enabled_rocksdb_specific_options(self.message_store_config.as_ref());
        if !self.message_store_config.is_enable_rocksdb_store() && !enabled_rocksdb_options.is_empty() {
            return Err(StoreError::General(format!(
                "RocksDB-specific configuration requires store_type=RocksDB: {}",
                enabled_rocksdb_options.join(", ")
            )));
        }
        if self.message_store_config.effective_linux_memory_lock_mode() == LinuxMemoryLockMode::ActiveFile
            && self.message_store_config.linux_memory_lock_budget_bytes == 0
            && !self.message_store_config.linux_memory_lock_warn_only
        {
            return Err(StoreError::General(
                "linux_memory_lock_mode=active_file requires explicit linux_memory_lock_budget_bytes when \
                 linux_memory_lock_warn_only=false; set linux_memory_lock_budget_bytes or \
                 linux_memory_lock_warn_only=true"
                    .to_string(),
            ));
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
        let state = self.lifecycle_state();
        state != StoreLifecycleState::Shutdown && !state.is_recovering() && !self.shutdown.load(Ordering::Acquire)
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
            let Some(queue_table) = consume_queue_store.find_consume_queue_map(topic) else {
                continue;
            };
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
        let previous_state = self.lifecycle_state();
        let recover_concurrently = self.is_recover_concurrently();
        let mut recovery_plan = RecoveryPlan::new(
            self.message_store_config.recovery_mode,
            RecoveryExit::from_last_exit_ok(last_exit_ok),
            recover_concurrently,
            self.message_store_config.max_recovery_commit_log_files,
        );
        recovery_plan.crc_policy = RecoveryCrcPolicy::new(
            self.message_store_config.check_crc_on_recover,
            self.message_store_config.force_verify_prop_crc,
        );
        recovery_plan.index_repair_policy = if self.message_store_config.message_index_enable {
            RecoveryIndexRepairPolicy::Synchronous
        } else {
            RecoveryIndexRepairPolicy::Disabled
        };
        recovery_plan.set_commit_log_offsets(
            self.get_min_phy_offset(),
            self.get_max_phy_offset(),
            self.get_confirm_offset(),
        );
        recovery_plan.set_index_safe_offset(self.current_index_safe_offset());
        recovery_plan.set_consume_queue_recovery_concurrency(ConsumeQueueRecoveryConcurrency::new(
            self.message_store_config
                .enable_local_file_consume_queue_recovery_concurrently,
            self.message_store_config
                .effective_local_file_consume_queue_recovery_parallelism(),
        ));
        let mut recovery_executor = RecoveryExecutor::new(recovery_plan);
        info!(
            "message store recover mode: {}, recoveryMode: {}, lastExit: {}, maxRecoveryCommitLogFiles: {}, \
             crcPolicy: message={}, property={}, indexRepairPolicy: {}, localFileCqRecoveryConcurrent: {}, \
             localFileCqRecoveryParallelism: {}",
            if recover_concurrently { "concurrent" } else { "normal" },
            self.message_store_config.recovery_mode.as_str(),
            recovery_executor.plan().exit.as_str(),
            recovery_executor.plan().max_recovery_commit_log_files,
            recovery_executor.plan().crc_policy.check_message_crc,
            recovery_executor.plan().crc_policy.check_property_crc,
            recovery_executor.plan().index_repair_policy.as_str(),
            recovery_executor
                .plan()
                .consume_queue_recovery_concurrency
                .local_file_enabled,
            recovery_executor
                .plan()
                .consume_queue_recovery_concurrency
                .local_file_parallelism
        );
        self.set_lifecycle_state(StoreLifecycleState::RecoveringConsumeQueue);
        let recover_consume_queue = recovery_executor
            .run_phase(RecoveryPhase::ConsumeQueue, self.recover_consume_queue())
            .await;
        let dispatch_recovery_offset = self.get_dispatch_recovery_offset();
        recovery_executor
            .plan_mut()
            .set_dispatch_recovery_offset(dispatch_recovery_offset);
        recovery_executor
            .plan_mut()
            .set_max_consume_queue_physical_offset(dispatch_recovery_offset);

        self.set_lifecycle_state(StoreLifecycleState::RecoveringCommitLog);
        let recover_commit_log = if last_exit_ok {
            recovery_executor
                .run_phase(
                    RecoveryPhase::CommitLog,
                    self.recover_normally(dispatch_recovery_offset),
                )
                .await
        } else {
            recovery_executor
                .run_phase(
                    RecoveryPhase::CommitLog,
                    self.recover_abnormally(dispatch_recovery_offset),
                )
                .await
        };

        self.set_lifecycle_state(StoreLifecycleState::RecoveringTopicQueueTable);
        let recover_topic_queue_table = recovery_executor
            .run_phase(RecoveryPhase::TopicQueueTable, async {
                self.recover_topic_queue_table();
            })
            .await;
        if self.lifecycle_state() != StoreLifecycleState::Shutdown {
            self.set_lifecycle_state(previous_state);
        }
        recovery_executor.plan_mut().set_commit_log_offsets(
            self.get_min_phy_offset(),
            self.get_max_phy_offset(),
            self.get_confirm_offset(),
        );
        recovery_executor
            .plan_mut()
            .set_index_safe_offset(self.current_index_safe_offset());
        let recovery_report = recovery_executor.finish();
        info!(
            "message store recover total cost: {} ms, recoverConsumeQueue: {} ms, recoverCommitLog: {} ms, \
             recoverOffsetTable: {} ms, recoveryMode: {}, lastExit: {}, dispatchRecoveryOffset: {:?}, commitLogRange: \
             {:?}-{:?}, confirmOffset: {:?}, indexSafeOffset: {:?}",
            recovery_report.total_duration_ms,
            recover_consume_queue,
            recover_commit_log,
            recover_topic_queue_table,
            recovery_report.plan.mode.as_str(),
            recovery_report.plan.exit.as_str(),
            recovery_report.plan.dispatch_recovery_offset,
            recovery_report.plan.offsets.commit_log_min_offset,
            recovery_report.plan.offsets.commit_log_max_offset,
            recovery_report.plan.offsets.confirm_offset,
            recovery_report.plan.offsets.index_safe_offset
        );
        self.last_recovery_report = Some(recovery_report);
    }

    fn current_index_safe_offset(&self) -> i64 {
        let Some(store_checkpoint) = self.store_checkpoint.as_ref() else {
            return 0;
        };
        let checkpoint_safe_offset = store_checkpoint.index_safe_phy_offset();
        let confirm_offset = self.get_confirm_offset().max(0) as u64;
        checkpoint_safe_offset.min(confirm_offset) as i64
    }

    pub async fn recover_normally(&mut self, max_phy_offset_of_consume_queue: i64) {
        // Check if optimized recovery is enabled (default: true)
        let use_optimized = std::env::var("ROCKETMQ_USE_OPTIMIZED_RECOVERY")
            .unwrap_or_else(|_| "true".to_string())
            .parse::<bool>()
            .unwrap_or(true);
        let message_store = match self.message_store_arc_or_error("normal recovery") {
            Ok(message_store) => message_store,
            Err(error) => {
                error!("skip normal recovery: {error}");
                return;
            }
        };

        if use_optimized {
            self.commit_log
                .recover_normally_optimized(max_phy_offset_of_consume_queue, message_store)
                .await;
        } else {
            self.commit_log
                .recover_normally(max_phy_offset_of_consume_queue, message_store)
                .await;
        }
    }

    pub async fn recover_abnormally(&mut self, max_phy_offset_of_consume_queue: i64) {
        // Check if optimized recovery is enabled (default: true)
        let use_optimized = std::env::var("ROCKETMQ_USE_OPTIMIZED_RECOVERY")
            .unwrap_or_else(|_| "true".to_string())
            .parse::<bool>()
            .unwrap_or(true);
        let message_store = match self.message_store_arc_or_error("abnormal recovery") {
            Ok(message_store) => message_store,
            Err(error) => {
                error!("skip abnormal recovery: {error}");
                return;
            }
        };

        if use_optimized {
            self.commit_log
                .recover_abnormally_optimized(max_phy_offset_of_consume_queue, message_store)
                .await;
        } else {
            self.commit_log
                .recover_abnormally(max_phy_offset_of_consume_queue, message_store)
                .await;
        }
    }

    fn is_recover_concurrently(&self) -> bool {
        self.broker_config.recover_concurrently
            && (self.message_store_config.is_enable_rocksdb_store()
                || self.is_local_file_consume_queue_recover_concurrently())
    }

    fn is_local_file_consume_queue_recover_concurrently(&self) -> bool {
        !self.message_store_config.is_enable_rocksdb_store()
            && self
                .message_store_config
                .enable_local_file_consume_queue_recovery_concurrently
    }

    async fn recover_consume_queue(&mut self) {
        if self.broker_config.recover_concurrently && self.message_store_config.is_enable_rocksdb_store() {
            self.consume_queue_store.recover_concurrently().await;
        } else if self.broker_config.recover_concurrently && self.is_local_file_consume_queue_recover_concurrently() {
            let parallelism = self
                .message_store_config
                .effective_local_file_consume_queue_recovery_parallelism();
            let summary = self
                .consume_queue_store
                .recover_concurrently_with_summary(parallelism)
                .await;
            if !summary.is_success() {
                warn!(
                    "local file consume queue concurrent recovery failed, fallback to serial recovery, \
                     parallelism={}, queues={}, success={}, failed={}, failures={}",
                    parallelism,
                    summary.queue_count,
                    summary.success_count,
                    summary.failure_count,
                    summary.failure_description()
                );
                self.consume_queue_store.recover().await;
            }
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
        if self.scheduled_task_group.is_some() {
            return;
        }

        let message_store = match self.message_store_arc_or_error("starting scheduled tasks") {
            Ok(message_store) => message_store,
            Err(error) => {
                error!("scheduled store tasks not started: {error}");
                return;
            }
        };
        let Some(store_checkpoint_arc) = self.store_checkpoint.clone() else {
            error!("scheduled store tasks not started: store checkpoint is not initialized");
            return;
        };

        self.scheduled_task_shutdown = CancellationToken::new();
        let task_group = match crate::runtime::task_group("rocketmq-store.local-file.scheduled") {
            Ok(task_group) => task_group,
            Err(error) => {
                error!("scheduled store tasks not started: {error}");
                return;
            }
        };
        let scheduled_tasks = ScheduledTaskGroup::new(task_group.child("scheduled"));

        // clean files  Periodically
        let clean_commit_log_service_arc = self.clean_commit_log_service.clone();
        let clean_resource_interval = self.message_store_config.clean_resource_interval as u64;
        let clean_commit_log_active = Arc::new(AtomicBool::new(true));
        if let Err(error) = scheduled_tasks.schedule_fixed_delay(
            ScheduledTaskConfig::fixed_delay(
                "clean-commit-log-scheduler",
                Duration::from_millis(clean_resource_interval.max(1)),
            ),
            move || {
                let service = Arc::clone(&clean_commit_log_service_arc);
                let active = Arc::clone(&clean_commit_log_active);
                async move {
                    if !active.load(Ordering::Acquire) {
                        return;
                    }
                    if !run_blocking_scheduled_task("clean commit log", move || service.run()).await {
                        active.store(false, Ordering::Release);
                    }
                }
            },
        ) {
            self.scheduled_task_shutdown.cancel();
            task_group.cancel();
            error!("failed to schedule store task clean commit log: {error}");
            return;
        }

        let store_self_check_active = Arc::new(AtomicBool::new(true));
        if let Err(error) = scheduled_tasks.schedule_fixed_delay(
            ScheduledTaskConfig::fixed_delay("store-self-check-scheduler", Duration::from_secs(10 * 60)),
            move || {
                let message_store = message_store.clone();
                let active = Arc::clone(&store_self_check_active);
                async move {
                    if !active.load(Ordering::Acquire) {
                        return;
                    }
                    if !run_blocking_scheduled_task("store self check", move || message_store.check_self()).await {
                        active.store(false, Ordering::Release);
                    }
                }
            },
        ) {
            self.scheduled_task_shutdown.cancel();
            task_group.cancel();
            error!("failed to schedule store task self check: {error}");
            return;
        }

        // store check point flush
        let checkpoint_flush_active = Arc::new(AtomicBool::new(true));
        if let Err(error) = scheduled_tasks.schedule_fixed_delay(
            ScheduledTaskConfig::fixed_delay("store-checkpoint-flush-scheduler", Duration::from_secs(1)),
            move || {
                let checkpoint = Arc::clone(&store_checkpoint_arc);
                let active = Arc::clone(&checkpoint_flush_active);
                async move {
                    if !active.load(Ordering::Acquire) {
                        return;
                    }
                    if !run_blocking_scheduled_task("store checkpoint flush", move || {
                        let _ = checkpoint.flush();
                    })
                    .await
                    {
                        active.store(false, Ordering::Release);
                    }
                }
            },
        ) {
            self.scheduled_task_shutdown.cancel();
            task_group.cancel();
            error!("failed to schedule store task checkpoint flush: {error}");
            return;
        }

        let correct_logic_offset_service_arc = self.correct_logic_offset_service.clone();
        let clean_consume_queue_service_arc = self.clean_consume_queue_service.clone();
        let clean_consume_queue_active = Arc::new(AtomicBool::new(true));
        if let Err(error) = scheduled_tasks.schedule_fixed_delay(
            ScheduledTaskConfig::fixed_delay(
                "clean-consume-queue-scheduler",
                Duration::from_millis(clean_resource_interval.max(1)),
            ),
            move || {
                let correct_service = Arc::clone(&correct_logic_offset_service_arc);
                let clean_service = Arc::clone(&clean_consume_queue_service_arc);
                let active = Arc::clone(&clean_consume_queue_active);
                async move {
                    if !active.load(Ordering::Acquire) {
                        return;
                    }
                    if !run_blocking_scheduled_task("clean consume queue", move || {
                        correct_service.run();
                        clean_service.run();
                    })
                    .await
                    {
                        active.store(false, Ordering::Release);
                    }
                }
            },
        ) {
            self.scheduled_task_shutdown.cancel();
            task_group.cancel();
            error!("failed to schedule store task clean consume queue: {error}");
            return;
        }

        self.scheduled_tasks = Some(scheduled_tasks);
        self.scheduled_task_group = Some(task_group);
    }

    async fn shutdown_schedule_tasks(&mut self) {
        self.scheduled_task_shutdown.cancel();
        self.scheduled_tasks.take();
        if let Some(task_group) = self.scheduled_task_group.take() {
            let report = task_group.shutdown(Duration::from_secs(5)).await;
            if let Err(error) = crate::runtime::shutdown_report_result("LocalFileMessageStore scheduled tasks", report)
            {
                error!("scheduled store task failed during shutdown: {error}");
            }
        }
    }

    #[cfg(test)]
    fn has_scheduled_task_group(&self) -> bool {
        self.scheduled_task_group.is_some()
    }

    pub(crate) fn scheduled_task_count(&self) -> usize {
        let root_count = self
            .scheduled_task_group
            .as_ref()
            .map(rocketmq_runtime::TaskGroup::task_count)
            .unwrap_or_default();
        let scheduled_count = self
            .scheduled_tasks
            .as_ref()
            .map(|scheduled_tasks| scheduled_tasks.group().task_count())
            .unwrap_or_default();
        root_count + scheduled_count
    }

    pub(crate) fn scheduled_task_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.scheduled_tasks
            .as_ref()
            .map(ScheduledTaskGroup::snapshot)
            .unwrap_or_default()
    }

    async fn shutdown_ha_update_master_tasks(&self) {
        let task_group = match self.ha_update_master_group.lock() {
            Ok(mut task_group) => task_group.take(),
            Err(error) => {
                error!("failed to take HA master address update task group during shutdown: {error}");
                return;
            }
        };

        if let Some(task_group) = task_group {
            let report = task_group.shutdown(Duration::from_secs(3)).await;
            if let Err(error) = crate::runtime::shutdown_report_result("HA master address update tasks", report) {
                error!("HA master address update task failed during shutdown: {error}");
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

    pub fn last_recovery_report(&self) -> Option<&RecoveryReport> {
        self.last_recovery_report.as_ref()
    }

    pub fn background_index_rebuild_snapshot(&self) -> BackgroundIndexRebuildSnapshot {
        self.background_index_rebuild_service.snapshot()
    }

    pub fn pause_background_index_rebuild(&self) {
        self.background_index_rebuild_service.pause();
    }

    pub fn resume_background_index_rebuild(&self) {
        self.background_index_rebuild_service.resume();
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
            let Some(checkpoint) = self.store_checkpoint.as_ref() else {
                error!("message store checkpoint is not initialized");
                return false;
            };
            self.master_flushed_offset = Arc::new(AtomicI64::new(checkpoint.master_flushed_offset() as i64));
            self.set_confirm_offset(checkpoint.confirm_phy_offset() as i64);
            result = self.index_service.load(last_exit_ok);
            #[cfg(feature = "tieredstore")]
            if result {
                if let Some(tiered_store) = self.tiered_store.as_ref() {
                    if let Err(error) = tiered_store.load().await {
                        error!("tieredstore load failed: {}", error);
                        return false;
                    }
                }
            }

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
            StoreLifecycleState::RecoveringConsumeQueue
            | StoreLifecycleState::RecoveringCommitLog
            | StoreLifecycleState::RecoveringTopicQueueTable => {
                return Err(StoreError::General(
                    "message store is recovering; start is not allowed".to_string(),
                ));
            }
        }

        self.acquire_store_lock()?;
        let start_result: Result<(), StoreError> = async {
            self.allocate_mapped_file_service.start();

            self.index_service.start();

            #[cfg(feature = "tieredstore")]
            if let Some(tiered_store) = self.tiered_store.as_ref() {
                tiered_store
                    .start()
                    .await
                    .map_err(|error| StoreError::General(format!("failed to start tieredstore: {error}")))?;
            }

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
            self.background_index_rebuild_service.start(
                self.commit_log.clone(),
                self.message_store_config.clone(),
                self.index_service.clone(),
                self.delay_level_table_ref().clone(),
                self.max_delay_level,
            );
            self.consume_queue_store.start();
            self.store_stats_service.start();
            if let Some(compaction_service) = self.compaction_service.as_mut() {
                compaction_service.start();
            }
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
                if self.background_index_rebuild_service.has_task_group() {
                    self.background_index_rebuild_service.shutdown().await;
                }
                #[cfg(feature = "tieredstore")]
                if let Some(tiered_store) = self.tiered_store.as_ref() {
                    if let Err(shutdown_error) = tiered_store.shutdown().await {
                        warn!("tieredstore shutdown after start failure failed: {}", shutdown_error);
                    }
                }
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
            StoreLifecycleState::RecoveringConsumeQueue
            | StoreLifecycleState::RecoveringCommitLog
            | StoreLifecycleState::RecoveringTopicQueueTable => {
                return Err(StoreError::General(
                    "message store cannot be initialized while recovering".to_string(),
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

        let storage_capability = crate::platform::current_store_platform_capability();
        info!(
            "Store platform capability snapshot: os={} page_size={} memory_lock_limit_bytes={} \
             effective_memory_lock_budget_bytes={} file_preallocate_supported={} io_hint_branch={} \
             mmap_advice_supported={} file_prefetch_supported={} lazy_mmap_supported={} \
             hint_failure_affects_correctness={}",
            storage_capability.os_name,
            storage_capability.page_size,
            storage_capability
                .memory_lock_limit_bytes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            Self::effective_linux_memory_lock_budget_bytes(self.message_store_config.as_ref()),
            storage_capability.file_preallocate_supported,
            storage_capability.optimization.io_hint_branch.as_str(),
            storage_capability.optimization.mmap_advice_supported,
            storage_capability.optimization.file_prefetch_supported,
            storage_capability.optimization.lazy_mmap_supported,
            storage_capability.optimization.hint_failure_affects_correctness
        );

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
                self.shutdown_ha_update_master_tasks().await;
                ha_service.shutdown().await;
            }

            self.shutdown_schedule_tasks().await;
            self.store_stats_service.shutdown_gracefully().await;
            self.background_index_rebuild_service.shutdown().await;
            self.commit_log.shutdown_gracefully().await;

            self.reput_message_service.shutdown().await;
            #[cfg(feature = "tieredstore")]
            if let Some(tiered_store) = self.tiered_store.as_ref() {
                if let Err(error) = tiered_store.shutdown().await {
                    error!("tieredstore shutdown failed: {}", error);
                }
            }
            self.consume_queue_store.shutdown();

            // dispatch-related services must be shut down after reputMessageService
            self.index_service.shutdown();

            if let Some(compaction_service) = self.compaction_service.as_mut() {
                compaction_service.shutdown_gracefully().await;
            }

            if self.message_store_config.rocksdb_cq_double_write_enable {
                // this.rocksDBMessageStore.consumeQueueStore.shutdown();
            }
            if let Some(timer_message_store) = self.timer_message_store.as_ref() {
                timer_message_store.shutdown_gracefully().await;
            }
            self.flush_consume_queue_service.shutdown().await;
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
        let lifecycle_state = self.lifecycle_state();
        if lifecycle_state == StoreLifecycleState::Shutdown || lifecycle_state.is_recovering() {
            warn!("message store is not available, so getMessage is forbidden");
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
        let mut get_result = GetMessageResult::new();
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
                while get_result.buffer_total_size() <= 0
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
                    let Some(mut buffer_consume_queue) = buffer_consume_queue else {
                        break;
                    };
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
                            let get_result_ref = &mut get_result;
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

                            let Some(select_result) = self.commit_log.get_message(offset_py, size_py) else {
                                if get_result_ref.buffer_total_size() == 0 {
                                    status = GetMessageStatus::MessageWasRemoving;
                                }
                                next_phy_file_start_offset = self.commit_log.roll_next_file(offset_py);
                                continue;
                            };
                            if self.message_store_config.cold_data_flow_control_enable
                                && !is_sys_consumer_group_for_no_cold_read_limit(group)
                                && !select_result.is_in_cache
                            {
                                get_result_ref.set_cold_data_sum(get_result_ref.cold_data_sum() + size_py as i64);
                            }

                            if let Some(filter) = message_filter.as_ref() {
                                if !filter.is_matched_by_commit_log(Some(select_result.get_buffer()), None) {
                                    if get_result_ref.buffer_total_size() == 0 {
                                        status = GetMessageStatus::NoMatchedMessage;
                                    }
                                    continue;
                                }
                            }
                            self.store_stats_service
                                .get_message_transferred_msg_count()
                                .fetch_add(cq_unit.batch_num as usize, Ordering::Relaxed);
                            get_result.add_message(
                                select_result,
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
                    if let Some(broker_stats_manager) = self.broker_stats_manager.as_ref() {
                        broker_stats_manager.record_disk_fall_behind_size(group, topic, queue_id, fall_behind);
                    } else {
                        warn!("disk fall behind recording is enabled but BrokerStatsManager is not initialized");
                    }
                }
                let diff = max_offset_py - max_phy_offset_pulling;
                let memory = ((*TOTAL_PHYSICAL_MEMORY_SIZE as f64)
                    * (self.message_store_config.access_message_in_memory_max_ratio as f64 / 100.0))
                    as i64;
                get_result.set_suggest_pulling_from_slave(diff > memory);
            }
        } else {
            status = GetMessageStatus::NoMatchedLogicQueue;
            next_begin_offset = self.next_offset_correction(offset, 0);
        }

        let mut result = get_result;
        result.set_status(Some(status));
        result.set_next_begin_offset(next_begin_offset);
        result.set_max_offset(max_offset);
        result.set_min_offset(min_offset);

        #[cfg(feature = "tieredstore")]
        if Self::should_try_tiered_get_message(status) {
            if let Some(tiered_result) = self
                .get_message_from_tiered_store(
                    group,
                    topic,
                    queue_id,
                    offset,
                    max_msg_nums,
                    max_total_msg_size,
                    message_filter.clone(),
                )
                .await
            {
                if tiered_result.status() != Some(GetMessageStatus::NoMatchedLogicQueue) {
                    result = tiered_result;
                    status = result.status().unwrap_or(status);
                }
            }
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

        Some(result)
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

    async fn get_offset_in_queue_by_time_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
    ) -> Result<i64, StoreError> {
        self.get_offset_in_queue_by_time_with_boundary_async(topic, queue_id, timestamp, BoundaryType::Lower)
            .await
    }

    async fn get_offset_in_queue_by_time_with_boundary_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> Result<i64, StoreError> {
        #[cfg(feature = "tieredstore")]
        if self.should_try_tiered_offset_by_time(topic, queue_id, timestamp) {
            if let Some(tiered_store) = self.tiered_store.as_ref() {
                match tiered_store
                    .fetcher()
                    .get_offset_by_time_with_boundary(topic.to_string(), queue_id, timestamp, boundary_type)
                    .await
                {
                    Ok(offset) if offset >= 0 => return Ok(offset),
                    Ok(_) => {}
                    Err(error) => {
                        return Err(StoreError::General(format!(
                            "tieredstore offset by time lookup failed: {error}"
                        )));
                    }
                }
            }
        }

        Ok(self.get_offset_in_queue_by_time_with_boundary(topic, queue_id, timestamp, boundary_type))
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
        let put_message_lock_runtime_info = self.commit_log.put_message_lock_runtime_info();
        result.insert(
            "putMessageLockAcquireTotal".to_string(),
            put_message_lock_runtime_info.acquire_total.to_string(),
        );
        result.insert(
            "putMessageLockWaitTotalMillis".to_string(),
            put_message_lock_runtime_info.wait_total_millis.to_string(),
        );
        result.insert(
            "putMessageLockWaitMaxMillis".to_string(),
            put_message_lock_runtime_info.wait_max_millis.to_string(),
        );
        result.insert(
            "putMessageLockHoldTotalMillis".to_string(),
            put_message_lock_runtime_info.hold_total_millis.to_string(),
        );
        result.insert(
            "putMessageLockHoldMaxMillis".to_string(),
            put_message_lock_runtime_info.hold_max_millis.to_string(),
        );
        let sync_flush_runtime_info = self.commit_log.sync_flush_runtime_info();
        result.insert(
            "syncFlushQueueDepth".to_string(),
            sync_flush_runtime_info.queue_depth.to_string(),
        );
        result.insert(
            "syncFlushEnqueueTotal".to_string(),
            sync_flush_runtime_info.enqueue_total.to_string(),
        );
        result.insert(
            "syncFlushCompletedTotal".to_string(),
            sync_flush_runtime_info.completed_total.to_string(),
        );
        result.insert(
            "syncFlushTimeoutTotal".to_string(),
            sync_flush_runtime_info.timeout_total.to_string(),
        );
        result.insert(
            "syncFlushOldestWaitMillis".to_string(),
            sync_flush_runtime_info.oldest_wait_millis.to_string(),
        );
        result.insert(
            "syncFlushMaxWaitMillis".to_string(),
            sync_flush_runtime_info.max_wait_millis.to_string(),
        );
        result.insert(
            "syncFlushWaitTotalMillis".to_string(),
            sync_flush_runtime_info.wait_total_millis.to_string(),
        );
        result.insert(
            "storeType".to_string(),
            self.message_store_config.store_type.get_store_type().to_string(),
        );
        result.insert(
            "rocksdbCqDoubleWriteEnable".to_string(),
            self.message_store_config.rocksdb_cq_double_write_enable.to_string(),
        );
        result.insert(
            "rocksdbCompatibilityMode".to_string(),
            if self.message_store_config.is_enable_rocksdb_store() {
                "local_file_compat"
            } else {
                "disabled"
            }
            .to_string(),
        );
        result.insert(
            "ioUringBackendStatus".to_string(),
            crate::log_file::mapped_file::io_uring_backend_status()
                .as_str()
                .to_string(),
        );
        let storage_capability = crate::platform::current_store_platform_capability();
        let platform_optimization = storage_capability.optimization;
        result.insert("linuxStorageOs".to_string(), storage_capability.os_name.to_string());
        result.insert(
            "storePlatformIoHintBranch".to_string(),
            platform_optimization.io_hint_branch.as_str().to_string(),
        );
        result.insert(
            "storePlatformMmapAdviceSupported".to_string(),
            platform_optimization.mmap_advice_supported.to_string(),
        );
        result.insert(
            "storePlatformFilePrefetchSupported".to_string(),
            platform_optimization.file_prefetch_supported.to_string(),
        );
        result.insert(
            "storePlatformLazyMmapSupported".to_string(),
            platform_optimization.lazy_mmap_supported.to_string(),
        );
        result.insert(
            "storePlatformIoHintFailureAffectsCorrectness".to_string(),
            platform_optimization.hint_failure_affects_correctness.to_string(),
        );
        result.insert(
            "storeIoHintEnable".to_string(),
            self.message_store_config.store_io_hint_enable.to_string(),
        );
        result.insert(
            "storeLazyMmapEnable".to_string(),
            self.message_store_config.store_lazy_mmap_enable.to_string(),
        );
        result.insert(
            "storeEffectiveIoHintEnable".to_string(),
            (self.message_store_config.store_io_hint_enable
                && (platform_optimization.mmap_advice_supported || platform_optimization.file_prefetch_supported))
                .to_string(),
        );
        result.insert(
            "storeEffectiveLazyMmapEnable".to_string(),
            (self.message_store_config.store_lazy_mmap_enable && platform_optimization.lazy_mmap_supported).to_string(),
        );
        result.insert(
            "linuxStoragePageSize".to_string(),
            storage_capability.page_size.to_string(),
        );
        result.insert(
            "linuxStorageMemoryLockLimitBytes".to_string(),
            storage_capability
                .memory_lock_limit_bytes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
        );
        result.insert(
            "linuxStorageEffectiveMemoryLockBudgetBytes".to_string(),
            self.message_store_config
                .effective_linux_memory_lock_budget_bytes(storage_capability.memory_lock_limit_bytes)
                .to_string(),
        );
        result.insert(
            "linuxStorageFilePreallocateSupported".to_string(),
            storage_capability.file_preallocate_supported.to_string(),
        );
        let linux_profile_settings = self.message_store_config.effective_linux_storage_profile_settings();
        let commit_log_load_stats = self.commit_log.load_statistics();
        result.insert(
            "linuxStorageOptimizationEnable".to_string(),
            self.message_store_config.linux_storage_optimization_enable.to_string(),
        );
        result.insert(
            "linuxStorageProfile".to_string(),
            self.message_store_config.linux_storage_profile.as_str().to_string(),
        );
        result.insert(
            "linuxStorageTransferEngine".to_string(),
            self.message_store_config
                .effective_linux_transfer_engine()
                .as_str()
                .to_string(),
        );
        result.insert(
            "linuxStorageMappedFileWarmMode".to_string(),
            linux_profile_settings.mapped_file_warm_mode.as_str().to_string(),
        );
        let mapped_file_warmup_stats = self.commit_log.mapped_file_warmup_stats();
        result.insert(
            "linuxStorageMappedFileWarmOperations".to_string(),
            mapped_file_warmup_stats.operations.to_string(),
        );
        result.insert(
            "linuxStorageMappedFileWarmBytes".to_string(),
            mapped_file_warmup_stats.bytes.to_string(),
        );
        result.insert(
            "linuxStorageMappedFileWarmTotalMillis".to_string(),
            mapped_file_warmup_stats.total_millis.to_string(),
        );
        result.insert(
            "linuxStorageMappedFileWarmLastMillis".to_string(),
            mapped_file_warmup_stats.last_millis.to_string(),
        );
        let lazy_mmap_stats = self.commit_log.lazy_mmap_stats();
        result.insert(
            "storeLazyMmapEligibleFiles".to_string(),
            lazy_mmap_stats.eligible_files.to_string(),
        );
        result.insert(
            "storeLazyMmapMappedFiles".to_string(),
            lazy_mmap_stats.mapped_files.to_string(),
        );
        result.insert(
            "storeLazyMmapOperations".to_string(),
            lazy_mmap_stats.map_operations.to_string(),
        );
        result.insert(
            "storeLazyMmapFailures".to_string(),
            lazy_mmap_stats.map_failures.to_string(),
        );
        result.insert(
            "storeLazyMmapTotalMillis".to_string(),
            lazy_mmap_stats.total_millis.to_string(),
        );
        result.insert(
            "storeLazyMmapLastMillis".to_string(),
            lazy_mmap_stats.last_millis.to_string(),
        );
        result.insert(
            "linuxStorageMemoryLockMode".to_string(),
            self.message_store_config
                .effective_linux_memory_lock_mode()
                .as_str()
                .to_string(),
        );
        result.insert(
            "linuxStorageMemoryLockWarnOnly".to_string(),
            linux_profile_settings.memory_lock_warn_only.to_string(),
        );
        result.insert(
            "linuxStorageRecoveryFadvise".to_string(),
            self.message_store_config
                .effective_linux_recovery_fadvise()
                .as_str()
                .to_string(),
        );
        result.insert(
            "linuxStorageRecoveryMmapAdvice".to_string(),
            commit_log_load_stats.recovery_mmap_advice.as_str().to_string(),
        );
        result.insert(
            "linuxStorageRecoveryMmapAdviceAttempts".to_string(),
            commit_log_load_stats.mmap_advice_attempts.to_string(),
        );
        result.insert(
            "linuxStorageRecoveryMmapAdviceSuccesses".to_string(),
            commit_log_load_stats.mmap_advice_successes.to_string(),
        );
        result.insert(
            "linuxStorageRecoveryMmapAdviceFailures".to_string(),
            commit_log_load_stats.mmap_advice_failures.to_string(),
        );
        result.insert(
            "linuxStorageRecoveryMmapAdviceElapsedMs".to_string(),
            commit_log_load_stats.mmap_advice_elapsed_ms.to_string(),
        );
        result.insert(
            "windowsStorageRecoveryFilePrefetch".to_string(),
            commit_log_load_stats.recovery_file_prefetch.as_str().to_string(),
        );
        result.insert(
            "windowsStorageRecoveryFilePrefetchAttempts".to_string(),
            commit_log_load_stats.file_prefetch_attempts.to_string(),
        );
        result.insert(
            "windowsStorageRecoveryFilePrefetchSuccesses".to_string(),
            commit_log_load_stats.file_prefetch_successes.to_string(),
        );
        result.insert(
            "windowsStorageRecoveryFilePrefetchFailures".to_string(),
            commit_log_load_stats.file_prefetch_failures.to_string(),
        );
        result.insert(
            "windowsStorageRecoveryFilePrefetchElapsedMs".to_string(),
            commit_log_load_stats.file_prefetch_elapsed_ms.to_string(),
        );
        result.insert(
            "linuxStorageHaSendfileEnable".to_string(),
            self.message_store_config
                .effective_linux_ha_sendfile_enable()
                .to_string(),
        );
        result.insert(
            "linuxStorageIoUringEnable".to_string(),
            linux_profile_settings.io_uring_enable.to_string(),
        );
        result.insert(
            "transientStorePoolLockedBuffers".to_string(),
            self.transient_store_pool.locked_buffer_count().to_string(),
        );
        result.insert(
            "transientStorePoolLockAttempts".to_string(),
            self.transient_store_pool.lock_attempt_count().to_string(),
        );
        result.insert(
            "transientStorePoolLockFailedBuffers".to_string(),
            self.transient_store_pool.lock_failed_buffer_count().to_string(),
        );
        result.insert(
            "transientStorePoolLockSkippedBuffers".to_string(),
            self.transient_store_pool.lock_skipped_buffer_count().to_string(),
        );
        result.insert(
            "transientStorePoolLockedBytes".to_string(),
            self.transient_store_pool.locked_bytes().to_string(),
        );
        result.insert(
            "transientStorePoolLockFailedBytes".to_string(),
            self.transient_store_pool.lock_failed_bytes().to_string(),
        );
        result.insert(
            "transientStorePoolLockSkippedBytes".to_string(),
            self.transient_store_pool.lock_skipped_bytes().to_string(),
        );
        result.insert(
            "warmMappedFileEnable".to_string(),
            self.message_store_config.warm_mapped_file_enable.to_string(),
        );
        result.insert(
            "flushLeastPagesWhenWarmMappedFile".to_string(),
            self.message_store_config
                .flush_least_pages_when_warm_mapped_file
                .to_string(),
        );
        let background_index_rebuild = self.background_index_rebuild_snapshot();
        result.insert(
            "backgroundIndexRebuildState".to_string(),
            background_index_rebuild.state.as_str().to_string(),
        );
        result.insert(
            "backgroundIndexRebuildEffectiveEnable".to_string(),
            self.message_store_config
                .effective_background_index_rebuild_enable()
                .to_string(),
        );
        result.insert(
            "backgroundIndexRebuildGrayMode".to_string(),
            self.message_store_config
                .background_index_rebuild_gray_mode()
                .to_string(),
        );
        result.insert(
            "backgroundIndexRebuildRollbackHint".to_string(),
            MessageStoreConfig::background_index_rebuild_rollback_hint().to_string(),
        );
        result.insert(
            "backgroundIndexRebuildQueryDegradationTotal".to_string(),
            self.background_index_query_degradation_total
                .load(Ordering::Relaxed)
                .to_string(),
        );
        result.insert(
            "backgroundIndexRebuildCurrentSafeOffset".to_string(),
            background_index_rebuild.current_safe_offset.to_string(),
        );
        result.insert(
            "backgroundIndexRebuildTargetOffset".to_string(),
            background_index_rebuild.target_offset.to_string(),
        );
        result.insert(
            "backgroundIndexRebuildBacklogBytes".to_string(),
            background_index_rebuild.backlog_bytes.to_string(),
        );
        result.insert(
            "backgroundIndexRebuildRebuiltBytes".to_string(),
            background_index_rebuild.rebuilt_bytes.to_string(),
        );
        result.insert(
            "backgroundIndexRebuildRebuiltMessages".to_string(),
            background_index_rebuild.rebuilt_messages.to_string(),
        );
        result.insert(
            "backgroundIndexRebuildFailureCount".to_string(),
            background_index_rebuild.failure_count.to_string(),
        );
        result.insert(
            "backgroundIndexRebuildBytesPerSecond".to_string(),
            background_index_rebuild.bytes_per_second.to_string(),
        );
        result.insert(
            "backgroundIndexRebuildLastError".to_string(),
            background_index_rebuild.last_error.unwrap_or_default(),
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
        match self.broker_config.broker_ip1.to_string().parse::<IpAddr>() {
            Ok(result) if result.is_ipv6() => {
                size = MessageDecoder::MESSAGE_STORE_TIMESTAMP_POSITION + 20;
            }
            Ok(_) => {}
            Err(error) => {
                warn!("failed to parse broker_ip1 when computing earliest message time: {error}");
            }
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
        #[cfg(feature = "tieredstore")]
        if let Some(tiered_store) = self.tiered_store.as_ref() {
            return tiered_store
                .fetcher()
                .get_message_timestamp(topic.to_string(), queue_id, consume_queue_offset)
                .await
                .map_err(|error| StoreError::General(format!("tieredstore timestamp lookup failed: {error}")));
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
        let index_safe_phyoffset = self.current_index_safe_offset();
        let index_confirm_phyoffset = self.get_confirm_offset().max(0);
        query_message_result.set_index_query_safety(
            !self.message_store_config.message_index_enable || index_safe_phyoffset >= index_confirm_phyoffset,
            index_safe_phyoffset,
            index_confirm_phyoffset,
        );
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
            for (m, offset) in phy_offsets.iter().copied().enumerate() {
                if m == 0 {
                    if let Some(msg) = self.look_message_by_offset(offset) {
                        last_query_msg_time = msg.store_timestamp;
                    } else {
                        warn!("index query returned unreadable message offset {offset}");
                    }
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

        #[cfg(feature = "tieredstore")]
        if query_message_result.buffer_total_size == 0 {
            if let Some(tiered_result) = self
                .query_message_from_tiered_store(topic, key, max_num, begin_timestamp, end_timestamp)
                .await
            {
                return Some(tiered_result);
            }
        }

        if query_message_result.buffer_total_size == 0 && !query_message_result.index_query_safe {
            self.background_index_query_degradation_total
                .fetch_add(1, Ordering::Relaxed);
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
            let task_group = match self.ha_update_master_group.lock() {
                Ok(mut task_group) => {
                    if task_group.is_none() {
                        match crate::runtime::task_group("rocketmq-store.local-file.ha-master-update") {
                            Ok(group) => *task_group = Some(group),
                            Err(error) => {
                                error!("failed to create HA master address update task group: {error}");
                                return;
                            }
                        }
                    }
                    task_group.as_ref().expect("task group must exist").clone()
                }
                Err(error) => {
                    error!("failed to lock HA master address update task group: {error}");
                    return;
                }
            };

            if let Err(error) = task_group.spawn_service("ha-master-address-update", async move {
                ha_service.update_master_address(new_addr.as_str()).await;
            }) {
                error!("failed to spawn HA master address update task: {error}");
            }
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
        let Some(cq) = first_cqitem.as_ref() else {
            return false;
        };
        let start_offset_py = cq.pos;
        if batch_size <= 1 {
            let size = cq.size;
            return self.check_in_mem_by_commit_offset(start_offset_py, size);
        }
        let Some(last_cqitem) = consume_queue.get(consume_offset + batch_size as i64) else {
            let size = cq.size;
            return self.check_in_mem_by_commit_offset(start_offset_py, size);
        };
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

    fn sync_flush_runtime_info(&self) -> crate::base::flush_manager::SyncFlushRuntimeInfo {
        self.commit_log.sync_flush_runtime_info()
    }

    fn health_snapshot(&self) -> StoreHealthSnapshot {
        let ha_runtime_info = self
            .ha_service
            .as_ref()
            .map_or_else(Default::default, GeneralHAService::group_transfer_runtime_info);
        StoreHealthSnapshot {
            os_page_cache_busy: self.is_os_page_cache_busy(),
            transient_store_pool_deficient: self.is_transient_store_pool_deficient(),
            sync_flush: self.sync_flush_runtime_info(),
            dispatch_behind_bytes: self.dispatch_behind_bytes(),
            shutdown: self.is_shutdown(),
            ha_pending_request_count: ha_runtime_info.pending_request_count,
            ha_pending_oldest_wait_millis: ha_runtime_info.pending_request_oldest_wait_millis,
        }
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
        unlock_mapped_file.munlock();
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
        let guard = match self.master_store_in_process.read() {
            Ok(guard) => guard,
            Err(error) => {
                error!("master_store_in_process lock poisoned: {error}");
                return None;
            }
        };
        let erased = guard.as_ref()?.clone();
        let boxed_master_store = erased.downcast::<Arc<M>>().ok()?;
        Some(Arc::clone(boxed_master_store.as_ref()))
    }

    fn set_master_store_in_process<M: MessageStore + Send + Sync + 'static>(&self, master_store_in_process: Arc<M>) {
        let mut guard = match self.master_store_in_process.write() {
            Ok(guard) => guard,
            Err(error) => {
                error!("master_store_in_process lock poisoned: {error}");
                return;
            }
        };
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
        let mut guard = match self.send_message_back_hook.write() {
            Ok(guard) => guard,
            Err(error) => {
                error!("send_message_back_hook lock poisoned: {error}");
                return;
            }
        };
        *guard = Some(send_message_back_hook);
    }

    fn get_send_message_back_hook(&self) -> Option<Arc<dyn SendMessageBackHook>> {
        match self.send_message_back_hook.read() {
            Ok(guard) => guard.clone(),
            Err(error) => {
                error!("send_message_back_hook lock poisoned: {error}");
                None
            }
        }
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BackgroundIndexRebuildState {
    #[default]
    Idle,
    Running,
    Paused,
    Completed,
    Retrying,
    Failed,
    Shutdown,
}

impl BackgroundIndexRebuildState {
    const fn as_u8(self) -> u8 {
        match self {
            Self::Idle => 0,
            Self::Running => 1,
            Self::Paused => 2,
            Self::Completed => 3,
            Self::Retrying => 4,
            Self::Failed => 5,
            Self::Shutdown => 6,
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Running,
            2 => Self::Paused,
            3 => Self::Completed,
            4 => Self::Retrying,
            5 => Self::Failed,
            6 => Self::Shutdown,
            _ => Self::Idle,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Retrying => "retrying",
            Self::Failed => "failed",
            Self::Shutdown => "shutdown",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackgroundIndexRebuildSnapshot {
    pub state: BackgroundIndexRebuildState,
    pub current_safe_offset: i64,
    pub target_offset: i64,
    pub backlog_bytes: i64,
    pub rebuilt_bytes: u64,
    pub rebuilt_messages: u64,
    pub failure_count: u64,
    pub last_error: Option<String>,
    pub bytes_per_second: u64,
}

struct BackgroundIndexRebuildProgress {
    state: AtomicU8,
    paused: AtomicBool,
    current_safe_offset: AtomicI64,
    target_offset: AtomicI64,
    rebuilt_bytes: AtomicU64,
    rebuilt_messages: AtomicU64,
    failure_count: AtomicU64,
    bytes_per_second: AtomicU64,
    last_error: StdMutex<Option<String>>,
    resume_notify: Notify,
}

impl BackgroundIndexRebuildProgress {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(BackgroundIndexRebuildState::Idle.as_u8()),
            paused: AtomicBool::new(false),
            current_safe_offset: AtomicI64::new(0),
            target_offset: AtomicI64::new(0),
            rebuilt_bytes: AtomicU64::new(0),
            rebuilt_messages: AtomicU64::new(0),
            failure_count: AtomicU64::new(0),
            bytes_per_second: AtomicU64::new(0),
            last_error: StdMutex::new(None),
            resume_notify: Notify::new(),
        }
    }

    fn reset(&self, current_safe_offset: i64, target_offset: i64, bytes_per_second: u64) {
        self.paused.store(false, Ordering::Release);
        self.current_safe_offset
            .store(current_safe_offset.max(0), Ordering::Release);
        self.target_offset.store(target_offset.max(0), Ordering::Release);
        self.rebuilt_bytes.store(0, Ordering::Release);
        self.rebuilt_messages.store(0, Ordering::Release);
        self.failure_count.store(0, Ordering::Release);
        self.bytes_per_second.store(bytes_per_second, Ordering::Release);
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = None;
        }
        self.set_state(BackgroundIndexRebuildState::Idle);
    }

    fn set_state(&self, state: BackgroundIndexRebuildState) {
        self.state.store(state.as_u8(), Ordering::Release);
    }

    fn state(&self) -> BackgroundIndexRebuildState {
        BackgroundIndexRebuildState::from_u8(self.state.load(Ordering::Acquire))
    }

    fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Release);
        if paused {
            self.set_state(BackgroundIndexRebuildState::Paused);
        } else {
            if self.state() == BackgroundIndexRebuildState::Paused {
                self.set_state(BackgroundIndexRebuildState::Idle);
            }
            self.resume_notify.notify_waiters();
        }
    }

    fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    fn update_current_safe_offset(&self, current_safe_offset: i64) {
        self.current_safe_offset
            .store(current_safe_offset.max(0), Ordering::Release);
    }

    fn record_rebuild(&self, bytes: u64, messages: u64) {
        self.rebuilt_bytes.fetch_add(bytes, Ordering::AcqRel);
        self.rebuilt_messages.fetch_add(messages, Ordering::AcqRel);
    }

    fn record_error(&self, error: impl Into<String>) {
        self.failure_count.fetch_add(1, Ordering::AcqRel);
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = Some(error.into());
        }
    }

    fn snapshot(&self) -> BackgroundIndexRebuildSnapshot {
        let current_safe_offset = self.current_safe_offset.load(Ordering::Acquire);
        let target_offset = self.target_offset.load(Ordering::Acquire);
        let last_error = self.last_error.lock().ok().and_then(|error| error.clone());
        BackgroundIndexRebuildSnapshot {
            state: self.state(),
            current_safe_offset,
            target_offset,
            backlog_bytes: target_offset.saturating_sub(current_safe_offset).max(0),
            rebuilt_bytes: self.rebuilt_bytes.load(Ordering::Acquire),
            rebuilt_messages: self.rebuilt_messages.load(Ordering::Acquire),
            failure_count: self.failure_count.load(Ordering::Acquire),
            last_error,
            bytes_per_second: self.bytes_per_second.load(Ordering::Acquire),
        }
    }
}

struct BackgroundIndexRebuildService {
    shutdown_token: CancellationToken,
    progress: Arc<BackgroundIndexRebuildProgress>,
    task_group: Option<rocketmq_runtime::TaskGroup>,
}

impl BackgroundIndexRebuildService {
    fn new() -> Self {
        Self {
            shutdown_token: CancellationToken::new(),
            progress: Arc::new(BackgroundIndexRebuildProgress::new()),
            task_group: None,
        }
    }

    fn start(
        &mut self,
        commit_log: ArcMut<CommitLog>,
        message_store_config: Arc<MessageStoreConfig>,
        index_service: IndexService,
        delay_level_table: BTreeMap<i32, i64>,
        max_delay_level: i32,
    ) {
        if self.task_group.is_some() || !message_store_config.effective_background_index_rebuild_enable() {
            return;
        }
        if !message_store_config.message_index_enable {
            return;
        }

        let current_safe_offset = index_service.index_safe_phy_offset().min(i64::MAX as u64) as i64;
        let target_offset = commit_log.get_confirm_offset().max(0);
        let bytes_per_second = message_store_config.background_index_rebuild_bytes_per_second as u64;
        self.progress
            .reset(current_safe_offset, target_offset, bytes_per_second);

        if current_safe_offset >= target_offset {
            self.progress.set_state(BackgroundIndexRebuildState::Completed);
            return;
        }

        let task_group = match crate::runtime::task_group("rocketmq-store.local-file.background-index-rebuild") {
            Ok(task_group) => task_group,
            Err(error) => {
                self.progress.record_error(error.to_string());
                self.progress.set_state(BackgroundIndexRebuildState::Failed);
                error!("BackgroundIndexRebuildService not started: {error}");
                return;
            }
        };

        self.shutdown_token = CancellationToken::new();
        let batch_size = message_store_config.background_index_rebuild_batch_size.max(1);
        let max_retries = message_store_config.background_index_rebuild_max_retries;
        let worker = BackgroundIndexRebuildWorker {
            commit_log,
            message_store_config,
            index_service,
            delay_level_table,
            max_delay_level,
            progress: self.progress.clone(),
            shutdown_token: self.shutdown_token.clone(),
            batch_size,
            bytes_per_second,
            max_retries,
        };

        if let Err(error) = task_group.spawn_service("background-index-rebuild", async move {
            worker.run().await;
        }) {
            self.shutdown_token.cancel();
            self.progress.record_error(error.to_string());
            self.progress.set_state(BackgroundIndexRebuildState::Failed);
            error!("failed to spawn BackgroundIndexRebuildService: {error}");
            return;
        }

        self.task_group = Some(task_group);
    }

    fn pause(&self) {
        self.progress.set_paused(true);
    }

    fn resume(&self) {
        self.progress.set_paused(false);
    }

    fn snapshot(&self) -> BackgroundIndexRebuildSnapshot {
        self.progress.snapshot()
    }

    async fn shutdown(&mut self) {
        self.shutdown_token.cancel();
        self.progress.resume_notify.notify_waiters();
        if let Some(task_group) = self.task_group.take() {
            let report = task_group.shutdown(Duration::from_secs(3)).await;
            match crate::runtime::shutdown_report_result("BackgroundIndexRebuildService", report) {
                Ok(()) => info!("BackgroundIndexRebuildService tasks shut down successfully"),
                Err(error) => warn!("BackgroundIndexRebuildService task shutdown reported an error: {error}"),
            }
        }
        self.progress.set_state(BackgroundIndexRebuildState::Shutdown);
    }

    fn has_task_group(&self) -> bool {
        self.task_group.is_some()
    }
}

struct BackgroundIndexRebuildWorker {
    commit_log: ArcMut<CommitLog>,
    message_store_config: Arc<MessageStoreConfig>,
    index_service: IndexService,
    delay_level_table: BTreeMap<i32, i64>,
    max_delay_level: i32,
    progress: Arc<BackgroundIndexRebuildProgress>,
    shutdown_token: CancellationToken,
    batch_size: usize,
    bytes_per_second: u64,
    max_retries: usize,
}

struct BackgroundIndexRebuildBatch {
    bytes: u64,
    messages: u64,
    completed: bool,
}

impl BackgroundIndexRebuildWorker {
    async fn run(self) {
        let mut retry_count = 0usize;
        loop {
            if !self.wait_if_paused().await {
                return;
            }
            self.progress.set_state(BackgroundIndexRebuildState::Running);
            let started = Instant::now();
            match self.rebuild_batch() {
                Ok(batch) => {
                    retry_count = 0;
                    if batch.bytes > 0 || batch.messages > 0 {
                        self.progress.record_rebuild(batch.bytes, batch.messages);
                    }
                    if batch.completed {
                        if let Err(error) = self.index_service.flush_index_safe_offset() {
                            self.progress.record_error(error.to_string());
                            self.progress.set_state(BackgroundIndexRebuildState::Failed);
                            return;
                        }
                        self.progress.set_state(BackgroundIndexRebuildState::Completed);
                        return;
                    }
                    self.throttle(batch.bytes, started).await;
                }
                Err(error) => {
                    self.progress.record_error(error);
                    if retry_count >= self.max_retries {
                        self.progress.set_state(BackgroundIndexRebuildState::Failed);
                        return;
                    }
                    retry_count += 1;
                    self.progress.set_state(BackgroundIndexRebuildState::Retrying);
                    tokio::select! {
                        _ = self.shutdown_token.cancelled() => {
                            self.progress.set_state(BackgroundIndexRebuildState::Shutdown);
                            return;
                        }
                        _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                    }
                }
            }
        }
    }

    async fn wait_if_paused(&self) -> bool {
        while self.progress.is_paused() {
            self.progress.set_state(BackgroundIndexRebuildState::Paused);
            tokio::select! {
                _ = self.shutdown_token.cancelled() => {
                    self.progress.set_state(BackgroundIndexRebuildState::Shutdown);
                    return false;
                }
                _ = self.progress.resume_notify.notified() => {}
            }
        }
        if self.shutdown_token.is_cancelled() {
            self.progress.set_state(BackgroundIndexRebuildState::Shutdown);
            return false;
        }
        true
    }

    fn rebuild_batch(&self) -> Result<BackgroundIndexRebuildBatch, String> {
        let target_offset = self.progress.target_offset.load(Ordering::Acquire);
        let mut current_offset = self.progress.current_safe_offset.load(Ordering::Acquire);
        if current_offset >= target_offset {
            return Ok(BackgroundIndexRebuildBatch {
                bytes: 0,
                messages: 0,
                completed: true,
            });
        }

        let min_offset = self.commit_log.get_min_offset();
        if current_offset < min_offset {
            info!(
                "background index rebuild offset {current_offset} is smaller than commitlog min offset {min_offset}, \
                 advancing to retained range"
            );
            current_offset = min_offset;
            self.index_service.advance_index_safe_offset_to(current_offset);
            self.progress.update_current_safe_offset(current_offset);
            if current_offset >= target_offset {
                return Ok(BackgroundIndexRebuildBatch {
                    bytes: 0,
                    messages: 0,
                    completed: true,
                });
            }
        }

        let mut result = self
            .commit_log
            .get_data(current_offset)
            .ok_or_else(|| format!("commitlog data unavailable at offset {current_offset}"))?;
        current_offset = result.start_offset as i64;
        self.progress.update_current_safe_offset(current_offset);

        let mut read_size = 0i32;
        let mut rebuilt_bytes = 0u64;
        let mut rebuilt_messages = 0u64;
        while read_size < result.size
            && current_offset < target_offset
            && rebuilt_messages < self.batch_size as u64
            && !self.shutdown_token.is_cancelled()
        {
            let Some(bytes) = result.bytes.as_mut() else {
                return Err("commitlog data buffer is missing during background index rebuild".to_string());
            };
            let mut dispatch_request = commit_log::check_message_and_return_size(
                bytes,
                false,
                false,
                false,
                &self.message_store_config,
                self.max_delay_level,
                &self.delay_level_table,
            );
            let size = if dispatch_request.buffer_size == -1 {
                dispatch_request.msg_size
            } else {
                dispatch_request.buffer_size
            };

            if dispatch_request.success && dispatch_request.msg_size == 0 {
                current_offset = self.commit_log.roll_next_file(current_offset);
                self.index_service.advance_index_safe_offset_to(current_offset);
                self.progress.update_current_safe_offset(current_offset);
                read_size = result.size;
                continue;
            }
            if size <= 0 {
                return Err(format!("invalid message size {size} at offset {current_offset}"));
            }
            if current_offset.saturating_add(i64::from(size)) > target_offset {
                break;
            }

            if dispatch_request.success {
                match dispatch_request.msg_size.cmp(&0) {
                    std::cmp::Ordering::Greater => {
                        dispatch_request.commit_log_offset = current_offset;
                        self.index_service.build_index(&dispatch_request);
                        current_offset = current_offset.saturating_add(i64::from(size));
                        self.progress.update_current_safe_offset(current_offset);
                        rebuilt_bytes = rebuilt_bytes.saturating_add(size as u64);
                        rebuilt_messages = rebuilt_messages.saturating_add(1);
                        read_size += size;
                    }
                    std::cmp::Ordering::Equal => {}
                    std::cmp::Ordering::Less => {
                        return Err(format!(
                            "negative message size {} at offset {current_offset}",
                            dispatch_request.msg_size
                        ));
                    }
                }
            } else {
                return Err(format!("invalid message at offset {current_offset}"));
            }
        }

        Ok(BackgroundIndexRebuildBatch {
            bytes: rebuilt_bytes,
            messages: rebuilt_messages,
            completed: current_offset >= target_offset,
        })
    }

    async fn throttle(&self, bytes: u64, started: Instant) {
        if self.bytes_per_second == 0 || bytes == 0 {
            return;
        }
        let expected = Duration::from_secs_f64(bytes as f64 / self.bytes_per_second as f64);
        let elapsed = started.elapsed();
        if expected > elapsed {
            tokio::select! {
                _ = self.shutdown_token.cancelled() => {
                    self.progress.set_state(BackgroundIndexRebuildState::Shutdown);
                }
                _ = tokio::time::sleep(expected - elapsed) => {}
            }
        }
    }
}

struct ReputMessageService {
    shutdown_token: CancellationToken,
    new_message_notify: Arc<Notify>,
    dispatch_progress_notify: Arc<Notify>,
    pending_messages: Arc<AtomicI64>,
    reput_from_offset: Option<Arc<AtomicI64>>,
    dispatch_tx: Option<tokio::sync::mpsc::Sender<Vec<DispatchRequest>>>,
    inner: Option<ReputMessageServiceInner>,
    task_group: Option<rocketmq_runtime::TaskGroup>,
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
        if self.task_group.is_some() {
            return;
        }

        let task_group = match crate::runtime::task_group("rocketmq-store.local-file.reput") {
            Ok(task_group) => task_group,
            Err(error) => {
                error!("ReputMessageService not started: {error}");
                return;
            }
        };

        // Create channel for decoupling read and dispatch
        let (dispatch_tx, mut dispatch_rx) = tokio::sync::mpsc::channel::<Vec<DispatchRequest>>(128);
        self.dispatch_tx = Some(dispatch_tx.clone());
        self.shutdown_token = CancellationToken::new();
        let reput_from_offset = self
            .reput_from_offset
            .get_or_insert_with(|| Arc::new(AtomicI64::new(0)))
            .clone();

        let mut inner = ReputMessageServiceInner {
            reput_from_offset,
            commit_log,
            message_store_config,
            dispatcher: dispatcher.clone(),
            notify_message_arrive_in_batch,
            message_store: message_store.clone(),
        };
        self.inner = Some(inner.clone());

        let shutdown = self.shutdown_token.clone();
        let new_message_notify = self.new_message_notify.clone();
        let dispatch_progress_notify = self.dispatch_progress_notify.clone();
        let pending_messages = self.pending_messages.clone();

        // Task 1: Read messages from CommitLog and send to channel
        let shutdown_reader = shutdown.clone();
        if let Err(error) = task_group.spawn_service("reput-reader", async move {
            loop {
                tokio::select! {
                    _ = new_message_notify.notified() => {
                        // Process all available messages when notified
                        loop {
                            // Check if there are messages to process
                            if !inner.is_commit_log_available() {
                                if inner.has_unconfirmed_commit_log() {
                                    dispatch_progress_notify.notify_waiters();
                                    tokio::select! {
                                        _ = shutdown_reader.cancelled() => return,
                                        _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                                    }
                                    continue;
                                }
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
                                    pending_messages
                                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |x| {
                                            if x > 0 { Some(x - 1) } else { Some(0) }
                                        })
                                        .ok();
                                    dispatch_progress_notify.notify_waiters();
                                }
                                None => {
                                    // No more messages available at this offset
                                    dispatch_progress_notify.notify_waiters();
                                    if inner.has_unconfirmed_commit_log() {
                                        tokio::select! {
                                            _ = shutdown_reader.cancelled() => return,
                                            _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                                        }
                                        continue;
                                    }
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
                    _ = shutdown_reader.cancelled() => {
                        break;
                    }
                }
            }
        }) {
            self.shutdown_token.cancel();
            self.dispatch_tx.take();
            error!("failed to spawn ReputMessageService reader: {error}");
            return;
        }

        // Task 2: Receive from channel and dispatch
        let shutdown_dispatcher = shutdown;
        if let Err(error) = task_group.spawn_service("reput-dispatcher", async move {
            loop {
                tokio::select! {
                    Some(mut batch) = dispatch_rx.recv() => {
                        dispatch_reput_batch(
                            &dispatcher,
                            &message_store,
                            notify_message_arrive_in_batch,
                            &mut batch,
                        );
                    }
                    _ = shutdown_dispatcher.cancelled() => {
                        // Process remaining messages in channel before shutdown
                        while let Ok(mut batch) = dispatch_rx.try_recv() {
                            dispatch_reput_batch(
                                &dispatcher,
                                &message_store,
                                notify_message_arrive_in_batch,
                                &mut batch,
                            );
                        }
                        break;
                    }
                }
            }
        }) {
            self.shutdown_token.cancel();
            self.dispatch_tx.take();
            task_group.cancel();
            error!("failed to spawn ReputMessageService dispatcher: {error}");
            self.task_group = Some(task_group);
            return;
        }

        self.task_group = Some(task_group);
        self.new_message_notify.notify_one();
    }

    async fn wait_until_commit_log_dispatched(&self, inner: &ReputMessageServiceInner, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let progress = self.dispatch_progress_notify.notified();
            if !inner.is_commit_log_available() {
                return true;
            }
            if tokio::time::timeout_at(deadline, progress).await.is_err() {
                return !inner.is_commit_log_available();
            }
        }
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
            let reput_from_offset = self
                .reput_from_offset
                .get_or_insert_with(|| Arc::new(AtomicI64::new(0)))
                .clone();
            self.inner = Some(ReputMessageServiceInner {
                reput_from_offset,
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
        // Step 1: Wait for pending messages to be dispatched (max 5 seconds)
        if let Some(inner) = self.inner.as_ref() {
            self.wait_until_commit_log_dispatched(inner, Duration::from_secs(5))
                .await;

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
        self.dispatch_tx.take();

        // Step 3: Wait for tasks to complete with timeout (3 seconds)
        if let Some(task_group) = self.task_group.take() {
            let report = task_group.shutdown(Duration::from_secs(3)).await;
            match crate::runtime::shutdown_report_result("ReputMessageService", report) {
                Ok(()) => info!("ReputMessageService tasks shut down successfully"),
                Err(error) => warn!("ReputMessageService task shutdown reported an error: {error}"),
            }
        }

        info!("ReputMessageService shutdown complete");
    }

    #[cfg(test)]
    fn has_task_group(&self) -> bool {
        self.task_group.is_some()
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

fn dispatch_reput_batch(
    dispatcher: &ArcMut<CommitLogDispatcherDefault>,
    message_store: &ArcMut<LocalFileMessageStore>,
    notify_message_arrive_in_batch: bool,
    dispatch_batch: &mut [DispatchRequest],
) {
    let batch_size = dispatch_batch.len();
    let started = Instant::now();
    dispatcher.dispatch_batch(dispatch_batch);
    message_store
        .store_stats_service
        .record_reput_dispatch_batch(batch_size, started.elapsed());

    if !notify_message_arrive_in_batch {
        for req in dispatch_batch.iter_mut() {
            message_store.notify_message_arrive_if_necessary(req);
        }
    }
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
            let Some(mut result) = self.commit_log.get_data(self.reput_from_offset.load(Ordering::Acquire)) else {
                break;
            };
            self.reput_from_offset
                .store(result.start_offset as i64, Ordering::Release);
            let mut read_size = 0i32;
            while read_size < result.size
                && self.reput_from_offset.load(Ordering::Acquire) < self.get_reput_end_offset()
                && do_next
            {
                let Some(bytes) = result.bytes.as_mut() else {
                    warn!("commitlog data is missing bytes during reput dispatch");
                    break;
                };
                let dispatch_request = commit_log::check_message_and_return_size(
                    bytes,
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
                                dispatch_reput_batch(
                                    &self.dispatcher,
                                    &self.message_store,
                                    self.notify_message_arrive_in_batch,
                                    &mut dispatch_batch,
                                );
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
            dispatch_reput_batch(
                &self.dispatcher,
                &self.message_store,
                self.notify_message_arrive_in_batch,
                &mut dispatch_batch,
            );
        }
        self.record_dispatch_behind_bytes();
    }

    fn is_commit_log_available(&self) -> bool {
        self.reput_from_offset.load(Ordering::Relaxed) < self.get_reput_end_offset()
    }

    fn has_unconfirmed_commit_log(&self) -> bool {
        !self.message_store_config.read_uncommitted
            && self.reput_from_offset.load(Ordering::Relaxed) < self.commit_log.get_max_offset()
            && !self.is_commit_log_available()
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
            .set_reput_dispatch_behind_bytes(behind.max(0) as u64);
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

        let mut result = self
            .commit_log
            .get_data(self.reput_from_offset.load(Ordering::Acquire))?;
        self.reput_from_offset
            .store(result.start_offset as i64, Ordering::Release);
        let mut read_size = 0i32;

        while read_size < result.size
            && self.reput_from_offset.load(Ordering::Acquire) < self.get_reput_end_offset()
            && dispatch_batch.len() < 64
        {
            let Some(bytes) = result.bytes.as_mut() else {
                warn!("commitlog data is missing bytes during batch reput dispatch");
                break;
            };
            let dispatch_request = commit_log::check_message_and_return_size(
                bytes,
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
    match crate::runtime::spawn_io(task_name, task).await {
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
    #[cfg(test)]
    disk_clean_decision_override: StdMutex<Option<DiskCleanDecision>>,
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
            #[cfg(test)]
            disk_clean_decision_override: StdMutex::new(None),
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

    #[cfg(test)]
    fn set_disk_clean_decision_override(&self, decision: Option<DiskCleanDecision>) {
        *self
            .disk_clean_decision_override
            .lock()
            .expect("lock disk clean decision override") = decision;
    }

    fn is_space_to_delete(&self) -> DiskCleanDecision {
        #[cfg(test)]
        if let Some(decision) = *self
            .disk_clean_decision_override
            .lock()
            .expect("lock disk clean decision override")
        {
            return decision;
        }

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
    worker_group: parking_lot::Mutex<Option<rocketmq_runtime::TaskGroup>>,
    shutdown_token: parking_lot::Mutex<CancellationToken>,
    wakeup: Arc<Notify>,
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
            worker_group: parking_lot::Mutex::new(None),
            shutdown_token: parking_lot::Mutex::new(CancellationToken::new()),
            wakeup: Arc::new(Notify::new()),
        }
    }

    fn flush_once_blocking(
        consume_queue_store: &ConsumeQueueStore,
        store_checkpoint: &StoreCheckpoint,
        flush_least_pages: i32,
    ) {
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

    async fn flush_once(
        consume_queue_store: ConsumeQueueStore,
        store_checkpoint: Arc<StoreCheckpoint>,
        flush_least_pages: i32,
    ) {
        if let Err(error) = crate::runtime::spawn_io("flush-consume-queue", move || {
            Self::flush_once_blocking(&consume_queue_store, &store_checkpoint, flush_least_pages);
        })
        .await
        {
            error!("flush consume queue service task failed: {error}");
        }
    }

    fn start(&self) {
        let mut worker_group = self.worker_group.lock();
        if worker_group.is_some() {
            return;
        }

        let group = match crate::runtime::task_group("rocketmq-store.flush-consume-queue") {
            Ok(group) => group,
            Err(error) => {
                error!("failed to start flush consume queue service: {error}");
                return;
            }
        };

        let message_store_config = self.message_store_config.clone();
        let consume_queue_store = self.consume_queue_store.clone();
        let store_checkpoint = self.store_checkpoint.clone();
        let shutdown_token = CancellationToken::new();
        *self.shutdown_token.lock() = shutdown_token.clone();
        let wakeup = self.wakeup.clone();

        match group.spawn_service("flush-consume-queue", async move {
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

                Self::flush_once(consume_queue_store.clone(), store_checkpoint.clone(), flush_least_pages).await;

                tokio::select! {
                    _ = shutdown_token.cancelled() => break,
                    _ = wakeup.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(interval)) => {}
                }
            }

            Self::flush_once(consume_queue_store, store_checkpoint, 0).await;
        }) {
            Ok(_) => {
                *worker_group = Some(group);
            }
            Err(error) => {
                error!("failed to start flush consume queue service task: {error}");
            }
        }
    }

    async fn shutdown(&self) {
        self.shutdown_token.lock().cancel();
        self.wakeup.notify_waiters();

        let worker_group = self.worker_group.lock().take();
        if let Some(worker_group) = worker_group {
            let report = worker_group.shutdown(Duration::from_secs(5)).await;
            if let Err(error) = crate::runtime::shutdown_report_result("FlushConsumeQueueService", report) {
                error!("flush consume queue service task failed during shutdown: {error}");
            }
        }
    }
}

pub fn parse_delay_level(level_string: &str) -> (BTreeMap<i32, i64>, i32) {
    let mut delay_level_table = BTreeMap::new();

    let level_array: Vec<&str> = level_string.split(' ').collect();
    let mut max_delay_level = 0;

    for (i, value) in level_array.iter().enumerate() {
        let Some(ch) = value.chars().last() else {
            continue;
        };
        let tu = match ch {
            's' => 1000,
            'm' => 1000 * 60,
            'h' => 1000 * 60 * 60,
            'd' => 1000 * 60 * 60 * 24,
            _ => continue,
        };

        let level = i as i32 + 1;
        if level > max_delay_level {
            max_delay_level = level;
        }

        let num_str = &value[0..value.len() - 1];
        let Ok(num) = num_str.parse::<i64>() else {
            continue;
        };
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
    use std::net::TcpListener;
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
    use rocketmq_common::common::attribute::cq_type::CQType;
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
    use rocketmq_common::common::mix_all;
    use rocketmq_common::common::running::running_stats::RunningStats;
    use rocketmq_common::common::topic::TopicValidator;
    use rocketmq_common::CRC32Utils::crc32;
    use rocketmq_common::TopicAttributes::TopicAttributes;
    use rocketmq_rust::ArcMut;
    #[cfg(feature = "tieredstore")]
    use rocketmq_tieredstore::fetcher::TieredGetMessageStatus;
    #[cfg(feature = "tieredstore")]
    use rocketmq_tieredstore::TieredDispatchRequest;
    #[cfg(feature = "tieredstore")]
    use rocketmq_tieredstore::TieredDispatcher;
    #[cfg(feature = "tieredstore")]
    use rocketmq_tieredstore::TieredLifecycle;
    #[cfg(feature = "tieredstore")]
    use rocketmq_tieredstore::TieredMessageFetcher;
    #[cfg(feature = "tieredstore")]
    use rocketmq_tieredstore::TieredStorageLevel;
    #[cfg(feature = "tieredstore")]
    use rocketmq_tieredstore::TieredStoreConfig;
    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    use super::run_blocking_scheduled_task;
    use super::BackgroundIndexRebuildService;
    use super::BackgroundIndexRebuildState;
    use super::CleanCommitLogService;
    use super::DiskCleanDecision;
    use super::LocalFileMessageStore;
    use super::ReputMessageService;
    use super::ReputMessageServiceInner;
    use super::StoreLifecycleState;
    use crate::base::dispatch_request::DispatchRequest;
    use crate::base::message_arriving_listener::MessageArrivingListener;
    use crate::base::message_result::PutMessageResult;
    use crate::base::message_status_enum::GetMessageStatus;
    use crate::base::message_status_enum::PutMessageStatus;
    use crate::base::message_store::MessageStore;
    use crate::base::store_checkpoint::StoreCheckpoint;
    use crate::base::store_enum::StoreType;
    use crate::config::flush_disk_type::FlushDiskType;
    use crate::config::message_store_config::LinuxMemoryLockMode;
    use crate::config::message_store_config::MessageStoreConfig;
    use crate::config::message_store_config::RecoveryMode;
    use crate::filter::MessageFilter;
    use crate::hook::put_message_hook::PutMessageHook;
    use crate::hook::send_message_back_hook::SendMessageBackHook;
    use crate::kv::compaction_service::CompactionService;
    use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
    use crate::message_encoder::message_ext_encoder::MessageExtEncoder;
    use crate::message_store::recovery::RecoveryCrcPolicy;
    use crate::message_store::recovery::RecoveryExit;
    use crate::message_store::recovery::RecoveryIndexRepairPolicy;
    use crate::message_store::recovery::RecoveryPhase;
    use crate::message_store::recovery::RecoveryPhaseStatus;
    use crate::message_store::recovery::RecoveryReportStats;
    use crate::queue::consume_queue::ConsumeQueueTrait;
    use crate::queue::consume_queue_store::ConsumeQueueStoreTrait;
    use crate::store_error::StoreError;
    use crate::store_path_config_helper::get_store_checkpoint;

    fn new_test_store(temp_dir: &tempfile::TempDir) -> ArcMut<LocalFileMessageStore> {
        new_configured_test_store(temp_dir, MessageStoreConfig::default())
    }

    fn allocate_local_test_port() -> u16 {
        TcpListener::bind(("127.0.0.1", 0))
            .expect("allocate local test port")
            .local_addr()
            .expect("read local test port")
            .port()
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
        message_store_config: MessageStoreConfig,
    ) -> ArcMut<LocalFileMessageStore> {
        new_configured_test_store_with_broker(temp_dir, message_store_config, BrokerConfig::default())
    }

    fn new_configured_test_store_with_broker(
        temp_dir: &tempfile::TempDir,
        mut message_store_config: MessageStoreConfig,
        broker_config: BrokerConfig,
    ) -> ArcMut<LocalFileMessageStore> {
        message_store_config.store_path_root_dir = temp_dir.path().to_string_lossy().to_string().into();
        let mut store = ArcMut::new(LocalFileMessageStore::new(
            Arc::new(message_store_config),
            Arc::new(broker_config),
            Arc::new(DashMap::<CheetahString, ArcMut<TopicConfig>>::new()),
            None,
            false,
        ));
        let store_clone = store.clone();
        store.set_message_store_arc(store_clone);
        store
    }

    #[test]
    fn commitlog_uses_allocate_mapped_file_service_for_file_creation() {
        let temp_dir = tempdir().unwrap();
        let store = new_configured_test_store(&temp_dir, MessageStoreConfig::default());

        assert!(store.commit_log.has_allocate_mapped_file_service());
    }

    #[test]
    fn unlock_mapped_file_is_safe_for_unlocked_and_repeated_calls() {
        let store_dir = tempdir().unwrap();
        let mapped_file_dir = tempdir().unwrap();
        let store = new_configured_test_store(&store_dir, MessageStoreConfig::default());
        let mapped_file_path = mapped_file_dir.path().join("00000000000000000000");
        let mapped_file =
            DefaultMappedFile::new(CheetahString::from(mapped_file_path.to_string_lossy().as_ref()), 4096);

        store.unlock_mapped_file(&mapped_file);
        store.unlock_mapped_file(&mapped_file);
    }

    fn new_unwired_test_store(temp_dir: &tempfile::TempDir) -> ArcMut<LocalFileMessageStore> {
        let message_store_config = MessageStoreConfig {
            store_path_root_dir: temp_dir.path().to_string_lossy().to_string().into(),
            ..MessageStoreConfig::default()
        };
        ArcMut::new(LocalFileMessageStore::new(
            Arc::new(message_store_config),
            Arc::new(BrokerConfig::default()),
            Arc::new(DashMap::<CheetahString, ArcMut<TopicConfig>>::new()),
            None,
            false,
        ))
    }

    #[tokio::test]
    async fn recovery_without_message_store_arc_returns_without_panicking() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_unwired_test_store(&temp_dir);

        store.recover_normally(0).await;
        store.recover_abnormally(0).await;
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

    #[tokio::test]
    async fn reput_shutdown_wait_uses_dispatch_progress_notification() {
        let temp_dir = tempdir().unwrap();
        let store = new_configured_test_store(&temp_dir, MessageStoreConfig::default());
        let mut inner = reput_inner_for_store(store);
        inner.set_reput_from_offset(-1);

        let service = ReputMessageService {
            shutdown_token: CancellationToken::new(),
            new_message_notify: Arc::new(tokio::sync::Notify::new()),
            dispatch_progress_notify: Arc::new(tokio::sync::Notify::new()),
            pending_messages: Arc::new(AtomicI64::new(0)),
            reput_from_offset: None,
            dispatch_tx: None,
            inner: None,
            task_group: None,
        };
        let reput_from_offset = inner.reput_from_offset.clone();
        let dispatch_progress_notify = service.dispatch_progress_notify.clone();

        let (dispatched, _) = tokio::time::timeout(Duration::from_millis(100), async {
            tokio::join!(
                service.wait_until_commit_log_dispatched(&inner, Duration::from_secs(5)),
                async move {
                    tokio::task::yield_now().await;
                    reput_from_offset.store(0, Ordering::Release);
                    dispatch_progress_notify.notify_waiters();
                }
            )
        })
        .await
        .expect("dispatch progress notification should wake shutdown wait");

        assert!(dispatched);
    }

    #[tokio::test]
    async fn reput_service_start_wakes_existing_commitlog_backlog() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                read_uncommitted: true,
                ..MessageStoreConfig::default()
            },
        );
        let topic = CheetahString::from_static_str("reput-start-backlog-topic");
        let msg_size =
            append_encoded_test_message(&mut store, &topic, 0, 1_000, Bytes::from_static(b"backlog-body")).await;

        store.reput_message_service.set_reput_from_offset(0);
        let commit_log = store.commit_log.clone();
        let message_store_config = store.message_store_config.clone();
        let dispatcher = store.dispatcher.clone();
        let message_store = store.clone();
        store
            .reput_message_service
            .start(commit_log, message_store_config, dispatcher, false, message_store);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let offset = store
                    .reput_message_service
                    .reput_from_offset
                    .as_ref()
                    .expect("reput offset should exist")
                    .load(Ordering::Acquire);
                if offset >= i64::from(msg_size) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("initial reput wakeup should process existing backlog");

        store.reput_message_service.shutdown().await;
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

    #[cfg(feature = "tieredstore")]
    fn encode_tiered_test_message(
        store: &LocalFileMessageStore,
        topic: &CheetahString,
        queue_offset: i64,
        commit_log_offset: i64,
        store_timestamp: i64,
        body: Bytes,
        key: Option<CheetahString>,
    ) -> Bytes {
        let mut msg = build_test_message(topic, body);
        msg.with_version(MessageVersion::V1);
        msg.message_ext_inner.set_queue_offset(queue_offset);
        msg.message_ext_inner.set_commit_log_offset(commit_log_offset);
        msg.message_ext_inner.set_store_timestamp(store_timestamp);
        if let Some(key) = key {
            msg.set_keys(key);
        }

        let mut encoder = MessageExtEncoder::new(store.message_store_config());
        assert!(encoder.encode(&msg).is_none());
        Bytes::copy_from_slice(encoder.byte_buf().as_ref())
    }

    async fn append_encoded_test_message(
        store: &mut ArcMut<LocalFileMessageStore>,
        topic: &CheetahString,
        commit_log_offset: i64,
        store_timestamp: i64,
        body: Bytes,
    ) -> i32 {
        append_encoded_test_message_with_key(store, topic, commit_log_offset, store_timestamp, body, None).await
    }

    async fn append_encoded_test_message_with_key(
        store: &mut ArcMut<LocalFileMessageStore>,
        topic: &CheetahString,
        commit_log_offset: i64,
        store_timestamp: i64,
        body: Bytes,
        key: Option<CheetahString>,
    ) -> i32 {
        let mut msg = build_test_message(topic, body);
        msg.with_version(MessageVersion::V1);
        msg.message_ext_inner.set_store_timestamp(store_timestamp);
        if let Some(key) = key {
            msg.set_keys(key);
        }

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
                trans_rocksdb_enable: true,
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
                && message.contains("trans_rocksdb_enable")
        ));
    }

    #[tokio::test]
    async fn init_rejects_strict_active_file_memory_lock_without_explicit_budget() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                linux_memory_lock_mode: LinuxMemoryLockMode::ActiveFile,
                linux_memory_lock_budget_bytes: 0,
                linux_memory_lock_warn_only: false,
                ..MessageStoreConfig::default()
            },
        );

        let error = store
            .init()
            .await
            .expect_err("strict active_file memory locking should require explicit budget");
        assert!(matches!(
            error,
            StoreError::General(message)
                if message.contains("active_file")
                    && message.contains("linux_memory_lock_budget_bytes")
                    && message.contains("linux_memory_lock_warn_only=true")
        ));

        let warn_only_dir = tempdir().unwrap();
        let mut warn_only_store = new_configured_test_store(
            &warn_only_dir,
            MessageStoreConfig {
                linux_memory_lock_mode: LinuxMemoryLockMode::ActiveFile,
                linux_memory_lock_budget_bytes: 0,
                linux_memory_lock_warn_only: true,
                ..MessageStoreConfig::default()
            },
        );
        warn_only_store
            .init()
            .await
            .expect("warn-only active_file memory locking should degrade without explicit budget");

        let explicit_budget_dir = tempdir().unwrap();
        let mut explicit_budget_store = new_configured_test_store(
            &explicit_budget_dir,
            MessageStoreConfig {
                linux_memory_lock_mode: LinuxMemoryLockMode::ActiveFile,
                linux_memory_lock_budget_bytes: 64 * 1024 * 1024,
                linux_memory_lock_warn_only: false,
                ..MessageStoreConfig::default()
            },
        );
        explicit_budget_store
            .init()
            .await
            .expect("strict active_file memory locking should accept an explicit budget");
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

    #[cfg(feature = "tieredstore")]
    #[tokio::test]
    async fn tieredstore_write_path_dispatches_commitlog_reput_messages() {
        let temp_dir = tempdir().unwrap();
        let topic = CheetahString::from_static_str("tieredstore-write-path-topic");
        let body = Bytes::from_static(b"tiered-write-body");
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                duplication_enable: true,
                flush_disk_type: FlushDiskType::AsyncFlush,
                read_uncommitted: true,
                timer_wheel_enable: false,
                tiered_store_config: Some(TieredStoreConfig {
                    storage_level: TieredStorageLevel::Force,
                    backend_provider: "memory".to_string(),
                    store_path_root_dir: temp_dir.path().join("tieredstore"),
                    max_pending_tasks: 16,
                    ..TieredStoreConfig::default()
                }),
                ..MessageStoreConfig::default()
            },
        );

        store.init().await.expect("init tieredstore-enabled store");
        assert!(store.load().await, "load tieredstore-enabled store");
        let tiered_store = store
            .tiered_store
            .as_ref()
            .expect("tieredstore should be initialized")
            .clone();
        tiered_store.start().await.expect("start tieredstore dispatcher");

        let put_result = store.put_message(build_test_message(&topic, body.clone())).await;
        assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
        let append_result = put_result
            .append_message_result()
            .expect("put result should include append result");
        let queue_offset = append_result.logics_offset;
        store
            .reput_message_service
            .set_reput_from_offset(append_result.wrote_offset);
        store.reput_once().await;
        tiered_store.shutdown().await.expect("shutdown tieredstore dispatcher");

        let fetched = tiered_store
            .fetcher()
            .get_message(topic.to_string(), 0, queue_offset, 1)
            .await
            .expect("fetch tiered message after store shutdown drains dispatcher");
        assert_eq!(fetched.status, TieredGetMessageStatus::Found);
        assert_eq!(fetched.messages.len(), 1);
        assert!(fetched.messages[0]
            .windows(body.len())
            .any(|window| window == body.as_ref()));
        assert_eq!(tiered_store.metrics().dispatch_requests(), 1);
        assert_eq!(tiered_store.metrics().messages_dispatch_total(), 1);
    }

    #[cfg(feature = "tieredstore")]
    #[tokio::test]
    async fn tieredstore_read_path_falls_back_when_local_queue_is_missing() {
        let temp_dir = tempdir().unwrap();
        let topic = CheetahString::from_static_str("tieredstore-read-fallback-topic");
        let group = CheetahString::from_static_str("tieredstore-read-fallback-group");
        let key = CheetahString::from_static_str("tiered-fallback-key");
        let body = Bytes::from_static(b"tiered-read-fallback-body");
        let queue_offset = 7;
        let store_timestamp = 1_700_000;
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                duplication_enable: true,
                timer_wheel_enable: false,
                tiered_store_config: Some(TieredStoreConfig {
                    storage_level: TieredStorageLevel::Force,
                    backend_provider: "memory".to_string(),
                    store_path_root_dir: temp_dir.path().join("tieredstore"),
                    max_pending_tasks: 16,
                    ..TieredStoreConfig::default()
                }),
                ..MessageStoreConfig::default()
            },
        );

        store.init().await.expect("init tieredstore-enabled store");
        assert!(store.load().await, "load tieredstore-enabled store");
        store.start().await.expect("start tieredstore-enabled store");

        let encoded = encode_tiered_test_message(
            &store,
            &topic,
            queue_offset,
            0,
            store_timestamp,
            body.clone(),
            Some(key.clone()),
        );
        let tiered_store = store
            .tiered_store
            .as_ref()
            .expect("tieredstore should be initialized")
            .clone();
        tiered_store
            .dispatcher()
            .dispatch(TieredDispatchRequest {
                topic: topic.to_string(),
                queue_id: 0,
                queue_offset,
                commit_log_offset: 0,
                message_size: encoded.len() as i32,
                tags_code: 0,
                store_timestamp,
                keys: Some(key.to_string()),
                uniq_key: None,
                offset_id: None,
                sys_flag: 0,
                body: Some(encoded),
            })
            .await
            .expect("dispatch directly to tieredstore");
        tiered_store
            .shutdown()
            .await
            .expect("shutdown tieredstore dispatcher after direct dispatch");

        let mut get_result = None;
        for _ in 0..50 {
            if let Some(result) = store.get_message(&group, &topic, 0, queue_offset, 1, None).await {
                if result.status() == Some(GetMessageStatus::Found) {
                    get_result = Some(result);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let get_result = get_result.expect("tieredstore get_message fallback should find message");
        assert_eq!(get_result.message_count(), 1);
        assert!(get_result.message_mapped_list()[0]
            .get_buffer()
            .windows(body.len())
            .any(|window| window == body.as_ref()));
        assert!(tiered_store.metrics().get_message_fallback_total() >= 1);
        assert!(tiered_store.metrics().messages_out_total() >= 1);

        let timestamp = store
            .get_message_store_timestamp_async(&topic, 0, queue_offset)
            .await
            .expect("tieredstore timestamp fallback");
        assert_eq!(timestamp, store_timestamp);

        assert_eq!(
            store
                .get_offset_in_queue_by_time_async(&topic, 0, store_timestamp - 1)
                .await
                .expect("tieredstore offset lower fallback"),
            queue_offset
        );
        assert_eq!(
            store
                .get_offset_in_queue_by_time_with_boundary_async(&topic, 0, store_timestamp, BoundaryType::Upper)
                .await
                .expect("tieredstore offset upper fallback"),
            queue_offset
        );
        assert_eq!(
            store
                .get_offset_in_queue_by_time_async(&topic, 0, store_timestamp + 1)
                .await
                .expect("tieredstore offset overflow fallback"),
            queue_offset + 1
        );

        let mut query_data = None;
        for _ in 0..50 {
            if let Some(result) = store.query_message(&topic, &key, 10, 0, i64::MAX).await {
                if let Some(data) = result.get_message_data() {
                    query_data = Some(data);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let query_data = query_data.expect("tieredstore query fallback data");
        assert!(query_data.windows(body.len()).any(|window| window == body.as_ref()));

        store.shutdown().await;
    }

    #[tokio::test]
    async fn init_rejects_timer_rocksdb_backend_until_native_store_exists() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                timer_rocksdb_enable: true,
                ..MessageStoreConfig::default()
            },
        );

        let error = store
            .init()
            .await
            .expect_err("timer rocksdb backend is not implemented");
        assert!(matches!(
            error,
            StoreError::General(message)
                if message.contains("Timer RocksDB backend")
                    && message.contains("timer_rocksdb_enable=false")
        ));
    }

    #[tokio::test]
    async fn init_allows_timer_rocksdb_backend_when_store_type_is_rocksdb() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                store_type: StoreType::RocksDB,
                timer_rocksdb_enable: true,
                ..MessageStoreConfig::default()
            },
        );

        store
            .init()
            .await
            .expect("rocksdb-typed store should accept timer rocksdb flag");
    }

    #[tokio::test]
    async fn compaction_topic_dispatches_and_reads_from_compaction_store() {
        let temp_dir = tempdir().unwrap();
        let topic = CheetahString::from_static_str("compaction-dispatch-topic");
        let group = CheetahString::from_static_str("compaction-dispatch-group");
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
            .expect("compaction topic should read from compaction store");
        assert_eq!(result.status(), Some(GetMessageStatus::Found));
        assert_eq!(result.message_count(), 1);
        assert_eq!(store.compaction_store.message_count(&topic, 0), 1);
        assert!(result.message_mapped_list()[0].mapped_file.is_none());
        assert!(result.message_mapped_list()[0].get_bytes_ref().is_some());
    }

    #[tokio::test]
    async fn rocksdb_store_type_with_rocksdb_cq_topic_uses_compat_local_queue_path() {
        let temp_dir = tempdir().unwrap();
        let topic = CheetahString::from_static_str("rocksdb-compat-topic");
        let group = CheetahString::from_static_str("rocksdb-compat-group");
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                store_type: StoreType::RocksDB,
                rocksdb_cq_double_write_enable: true,
                flush_disk_type: FlushDiskType::AsyncFlush,
                ..MessageStoreConfig::default()
            },
        );
        let mut topic_config = TopicConfig::new(topic.clone());
        topic_config.attributes.insert(
            TopicAttributes::queue_type_attribute().name().clone(),
            CQType::RocksDBCQ.to_string().into(),
        );
        store
            .topic_config_table
            .insert(topic.clone(), ArcMut::new(topic_config));

        store.init().await.expect("init rocksdb compatibility store");
        assert!(store.load().await, "load rocksdb compatibility store");
        let put_result = store
            .put_message(build_test_message(&topic, Bytes::from_static(b"rocksdb-compat-body")))
            .await;
        assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
        store.reput_once().await;

        let queue = store
            .get_consume_queue(&topic, 0)
            .expect("compat queue should be present");
        assert_eq!(queue.get_cq_type(), CQType::SimpleCQ);

        let result = store
            .get_message(&group, &topic, 0, 0, 32, None)
            .await
            .expect("rocksdb compatibility get result");
        assert_eq!(result.status(), Some(GetMessageStatus::Found));
        assert_eq!(result.message_count(), 1);
        assert_eq!(result.next_begin_offset(), 1);

        let runtime_info = store.get_runtime_info();
        assert_eq!(runtime_info["storeType"], "RocksDB");
        assert_eq!(runtime_info["rocksdbCqDoubleWriteEnable"], "true");
        assert_eq!(runtime_info["rocksdbCompatibilityMode"], "local_file_compat");
    }

    #[test]
    fn runtime_info_reports_linux_storage_lifecycle_fields() {
        let temp_dir = tempdir().unwrap();
        let store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                mapped_file_size_commit_log: 1024,
                ..MessageStoreConfig::default()
            },
        );
        assert!(store.get_last_mapped_file(0));
        let mapped_file = store
            .commit_log
            .last_mapped_file_for_testing()
            .expect("runtime info test should create commitlog mapped file");
        mapped_file
            .get_metrics()
            .expect("mapped file metrics should be enabled")
            .record_warm_with_latency(4096, Duration::from_millis(12));

        let runtime_info = store.get_runtime_info();

        assert!(runtime_info.contains_key("linuxStorageOs"));
        assert!(runtime_info.contains_key("linuxStoragePageSize"));
        assert!(runtime_info.contains_key("linuxStorageMemoryLockLimitBytes"));
        let platform_capability = crate::platform::current_store_platform_capability();
        assert_eq!(
            runtime_info["storePlatformIoHintBranch"],
            platform_capability.optimization.io_hint_branch.as_str()
        );
        assert_eq!(
            runtime_info["storePlatformMmapAdviceSupported"],
            platform_capability.optimization.mmap_advice_supported.to_string()
        );
        assert_eq!(
            runtime_info["storePlatformFilePrefetchSupported"],
            platform_capability.optimization.file_prefetch_supported.to_string()
        );
        assert_eq!(
            runtime_info["storePlatformLazyMmapSupported"],
            platform_capability.optimization.lazy_mmap_supported.to_string()
        );
        assert_eq!(runtime_info["storePlatformIoHintFailureAffectsCorrectness"], "false");
        assert_eq!(runtime_info["storeIoHintEnable"], "false");
        assert_eq!(runtime_info["storeLazyMmapEnable"], "false");
        assert_eq!(runtime_info["storeEffectiveIoHintEnable"], "false");
        assert_eq!(runtime_info["storeEffectiveLazyMmapEnable"], "false");
        assert_eq!(runtime_info["transientStorePoolLockAttempts"], "0");
        assert_eq!(runtime_info["transientStorePoolLockedBuffers"], "0");
        assert_eq!(runtime_info["transientStorePoolLockFailedBuffers"], "0");
        assert_eq!(runtime_info["transientStorePoolLockSkippedBuffers"], "0");
        assert_eq!(runtime_info["transientStorePoolLockedBytes"], "0");
        assert_eq!(runtime_info["warmMappedFileEnable"], "false");
        assert_eq!(runtime_info["linuxStorageProfile"], "balanced");
        assert_eq!(runtime_info["linuxStorageTransferEngine"], "vectored");
        assert_eq!(runtime_info["linuxStorageMappedFileWarmMode"], "madvise");
        assert_eq!(runtime_info["linuxStorageMappedFileWarmOperations"], "1");
        assert_eq!(runtime_info["linuxStorageMappedFileWarmBytes"], "4096");
        assert_eq!(runtime_info["linuxStorageMappedFileWarmTotalMillis"], "12");
        assert_eq!(runtime_info["linuxStorageMappedFileWarmLastMillis"], "12");
        assert_eq!(runtime_info["storeLazyMmapEligibleFiles"], "0");
        assert_eq!(runtime_info["storeLazyMmapMappedFiles"], "0");
        assert_eq!(runtime_info["storeLazyMmapOperations"], "0");
        assert_eq!(runtime_info["storeLazyMmapFailures"], "0");
        assert_eq!(runtime_info["storeLazyMmapTotalMillis"], "0");
        assert_eq!(runtime_info["storeLazyMmapLastMillis"], "0");
        assert_eq!(runtime_info["linuxStorageMemoryLockMode"], "off");
        assert_eq!(runtime_info["linuxStorageRecoveryFadvise"], "disabled");
        assert_eq!(runtime_info["linuxStorageRecoveryMmapAdvice"], "disabled");
        assert_eq!(runtime_info["linuxStorageRecoveryMmapAdviceAttempts"], "0");
        assert_eq!(runtime_info["linuxStorageRecoveryMmapAdviceSuccesses"], "0");
        assert_eq!(runtime_info["linuxStorageRecoveryMmapAdviceFailures"], "0");
        assert_eq!(runtime_info["linuxStorageRecoveryMmapAdviceElapsedMs"], "0");
        assert_eq!(runtime_info["windowsStorageRecoveryFilePrefetch"], "disabled");
        assert_eq!(runtime_info["windowsStorageRecoveryFilePrefetchAttempts"], "0");
        assert_eq!(runtime_info["windowsStorageRecoveryFilePrefetchSuccesses"], "0");
        assert_eq!(runtime_info["windowsStorageRecoveryFilePrefetchFailures"], "0");
        assert_eq!(runtime_info["windowsStorageRecoveryFilePrefetchElapsedMs"], "0");
        assert_eq!(runtime_info["linuxStorageHaSendfileEnable"], "false");
        assert_eq!(runtime_info["linuxStorageIoUringEnable"], "false");
    }

    #[test]
    fn runtime_info_reports_effective_linux_memory_lock_budget() {
        let temp_dir = tempdir().unwrap();
        let store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                linux_memory_lock_budget_bytes: 4096,
                ..Default::default()
            },
        );

        let runtime_info = store.get_runtime_info();

        assert_eq!(runtime_info["linuxStorageEffectiveMemoryLockBudgetBytes"], "4096");
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
    async fn started_store_reput_dispatches_schedule_topic_messages_after_normal_message() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                flush_disk_type: FlushDiskType::AsyncFlush,
                ..MessageStoreConfig::default()
            },
        );
        let normal_topic = CheetahString::from_static_str("started-store-normal-before-schedule-topic");
        let schedule_topic = CheetahString::from_static_str(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC);
        let schedule_queue_id = 2;
        let normal_message = build_test_message(&normal_topic, Bytes::from_static(b"normal-before-schedule-body"));
        let mut schedule_message = build_test_message(&schedule_topic, Bytes::from_static(b"scheduled-retry-body"));
        schedule_message.message_ext_inner.set_queue_id(schedule_queue_id);
        schedule_message.set_delay_time_level(3);

        store.init().await.expect("init store");
        assert!(store.load().await, "load store");
        store.start().await.expect("start store");

        let normal_put_result = store.put_message(normal_message).await;
        assert_eq!(normal_put_result.put_message_status(), PutMessageStatus::PutOk);
        let schedule_put_result = store.put_message(schedule_message).await;
        assert_eq!(schedule_put_result.put_message_status(), PutMessageStatus::PutOk);

        let mut normal_max_offset = 0;
        let mut max_offset = 0;
        for _ in 0..100 {
            normal_max_offset = store.get_max_offset_in_queue(&normal_topic, 0);
            max_offset = store.get_max_offset_in_queue(&schedule_topic, schedule_queue_id);
            if normal_max_offset == 1 && max_offset == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        store.shutdown().await;

        assert_eq!(
            normal_max_offset, 1,
            "started store should dispatch the normal message before the scheduled message"
        );
        assert_eq!(
            max_offset, 1,
            "started store should dispatch scheduled messages into SCHEDULE_TOPIC_XXXX consume queue"
        );
    }

    #[tokio::test]
    async fn sync_flush_store_dispatches_wait_false_schedule_topic_messages() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                ha_listen_port: allocate_local_test_port() as usize,
                ..MessageStoreConfig::default()
            },
        );
        let normal_topic = CheetahString::from_static_str("sync-flush-normal-before-schedule-topic");
        let schedule_topic = CheetahString::from_static_str(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC);
        let schedule_queue_id = 2;
        let normal_message = build_test_message(&normal_topic, Bytes::from_static(b"sync-flush-normal-body"));
        let mut schedule_message = build_test_message(&schedule_topic, Bytes::from_static(b"sync-flush-schedule-body"));
        schedule_message.message_ext_inner.set_queue_id(schedule_queue_id);
        schedule_message.set_delay_time_level(3);
        schedule_message.set_wait_store_msg_ok(false);

        store.init().await.expect("init store");
        assert!(store.load().await, "load store");
        store.start().await.expect("start store");

        let normal_put_result = store.put_message(normal_message).await;
        assert_eq!(normal_put_result.put_message_status(), PutMessageStatus::PutOk);
        let schedule_put_result = store.put_message(schedule_message).await;
        assert_eq!(schedule_put_result.put_message_status(), PutMessageStatus::PutOk);

        let mut normal_max_offset = 0;
        let mut schedule_max_offset = 0;
        for _ in 0..100 {
            normal_max_offset = store.get_max_offset_in_queue(&normal_topic, 0);
            schedule_max_offset = store.get_max_offset_in_queue(&schedule_topic, schedule_queue_id);
            if normal_max_offset == 1 && schedule_max_offset == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        store.shutdown().await;

        assert_eq!(
            normal_max_offset, 1,
            "sync flush store should dispatch the preceding normal message"
        );
        assert_eq!(
            schedule_max_offset, 1,
            "sync flush store should dispatch wait=false scheduled messages after wakeup flush"
        );
    }

    #[tokio::test]
    async fn shutdown_waits_for_stats_and_timer_background_tasks() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                duplication_enable: true,
                enable_compaction: true,
                timer_wheel_enable: true,
                ..MessageStoreConfig::default()
            },
        );

        store.init().await.expect("init store");
        store.start().await.expect("start store");

        assert!(store.has_scheduled_task_group());
        let mut snapshots = store.scheduled_task_snapshot();
        for _ in 0..100 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            snapshots = store.scheduled_task_snapshot();
        }
        assert_eq!(snapshots.len(), 4, "{snapshots:?}");
        assert!(store.scheduled_task_count() >= 4);
        assert!(snapshots.iter().map(|snapshot| snapshot.runs).sum::<u64>() > 0);
        assert_eq!(snapshots.iter().map(|snapshot| snapshot.overlaps).sum::<u64>(), 0);
        assert_eq!(snapshots.iter().map(|snapshot| snapshot.failures).sum::<u64>(), 0);
        assert!(store.store_stats_service.has_worker_handle());
        assert!(store
            .compaction_service
            .as_ref()
            .is_some_and(CompactionService::has_worker_handle));
        assert!(store.reput_message_service.has_task_group());
        assert!(store
            .timer_message_store
            .as_ref()
            .expect("timer store should be initialized")
            .has_scheduler_handle());

        store.shutdown().await;

        assert!(!store.has_scheduled_task_group());
        assert_eq!(store.scheduled_task_count(), 0);
        assert!(!store.store_stats_service.has_worker_handle());
        assert!(store
            .compaction_service
            .as_ref()
            .is_some_and(|service| !service.has_worker_handle()));
        assert!(!store.reput_message_service.has_task_group());
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

    #[tokio::test]
    async fn read_and_write_are_rejected_while_recovering() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_test_store(&temp_dir);
        store.set_lifecycle_state(StoreLifecycleState::RecoveringCommitLog);

        let topic = CheetahString::from_static_str("recovering-io-topic");
        let group = CheetahString::from_static_str("recovering-io-group");
        let result = store
            .put_message(build_test_message(&topic, Bytes::from_static(b"during-recovery")))
            .await;

        assert_eq!(result.put_message_status(), PutMessageStatus::ServiceNotAvailable);
        assert!(store.get_message(&group, &topic, 0, 0, 32, None).await.is_none());
    }

    #[tokio::test]
    async fn recover_restores_lifecycle_state_after_recovery_phases() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_test_store(&temp_dir);

        assert_eq!(store.lifecycle_state(), StoreLifecycleState::Created);
        store.recover(true).await;
        assert_eq!(store.lifecycle_state(), StoreLifecycleState::Created);

        store.init().await.expect("init store");
        store.recover(false).await;
        assert_eq!(store.lifecycle_state(), StoreLifecycleState::Initialized);
    }

    #[tokio::test]
    async fn recover_records_structured_recovery_report() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                max_recovery_commit_log_files: 7,
                recovery_mode: RecoveryMode::Strict,
                check_crc_on_recover: true,
                force_verify_prop_crc: true,
                enable_local_file_consume_queue_recovery_concurrently: true,
                local_file_consume_queue_recovery_parallelism: 4,
                ..Default::default()
            },
        );

        store.recover(false).await;

        let report = store.last_recovery_report().expect("recovery report");
        assert_eq!(report.plan.mode, RecoveryMode::Strict);
        assert_eq!(report.plan.exit, RecoveryExit::Abnormal);
        assert!(!report.plan.recover_concurrently);
        assert_eq!(report.plan.max_recovery_commit_log_files, 7);
        assert_eq!(report.plan.scan_range.file_count_limit, Some(7));
        assert!(report.plan.dispatch_recovery_offset.is_some());
        assert_eq!(
            report.plan.offsets.dispatch_recovery_offset,
            report.plan.dispatch_recovery_offset
        );
        assert_eq!(
            report.plan.scan_range.start_offset,
            report.plan.dispatch_recovery_offset
        );
        assert!(report.plan.offsets.commit_log_min_offset.is_some());
        assert!(report.plan.offsets.commit_log_max_offset.is_some());
        assert!(report.plan.offsets.confirm_offset.is_some());
        assert_eq!(report.plan.offsets.index_safe_offset, Some(0));
        assert_eq!(
            report.plan.scan_range.end_offset,
            report.plan.offsets.commit_log_max_offset
        );
        assert_eq!(report.plan.crc_policy, RecoveryCrcPolicy::new(true, true));
        assert_eq!(report.plan.index_repair_policy, RecoveryIndexRepairPolicy::Synchronous);
        assert!(report.plan.consume_queue_recovery_concurrency.local_file_enabled);
        assert_eq!(report.plan.consume_queue_recovery_concurrency.local_file_parallelism, 4);
        assert_eq!(report.phases.len(), 3);
        assert!(report.phase_duration_ms(RecoveryPhase::ConsumeQueue).is_some());
        assert!(report.phase_duration_ms(RecoveryPhase::CommitLog).is_some());
        assert!(report.phase_duration_ms(RecoveryPhase::TopicQueueTable).is_some());
        assert!(report
            .phases
            .iter()
            .all(|phase| phase.status == RecoveryPhaseStatus::Success));
        assert_eq!(report.stats, RecoveryReportStats::default());
        assert_eq!(
            report.total_duration_ms,
            report.phases.iter().map(|phase| phase.duration_ms).sum()
        );
    }

    #[test]
    fn current_index_safe_offset_is_bounded_by_confirm_offset() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store_with_broker(
            &temp_dir,
            MessageStoreConfig::default(),
            BrokerConfig {
                duplication_enable: true,
                ..BrokerConfig::default()
            },
        );
        let checkpoint = store.store_checkpoint.as_ref().expect("checkpoint");

        checkpoint.set_index_safe_phy_offset(512);
        store.set_confirm_offset(128);

        assert_eq!(store.current_index_safe_offset(), 128);
    }

    #[tokio::test]
    async fn background_index_rebuild_pause_resume_and_shutdown_update_state() {
        let mut service = BackgroundIndexRebuildService::new();

        assert_eq!(service.snapshot().state, BackgroundIndexRebuildState::Idle);

        service.pause();
        assert_eq!(service.snapshot().state, BackgroundIndexRebuildState::Paused);

        service.resume();
        assert_eq!(service.snapshot().state, BackgroundIndexRebuildState::Idle);

        service.shutdown().await;
        assert_eq!(service.snapshot().state, BackgroundIndexRebuildState::Shutdown);
    }

    #[test]
    fn background_index_rebuild_is_disabled_by_default_for_store() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_test_store(&temp_dir);
        let commit_log = store.commit_log.clone();
        let message_store_config = store.message_store_config.clone();
        let index_service = store.index_service.clone();
        let delay_level_table = store.delay_level_table_ref().clone();
        let max_delay_level = store.max_delay_level;

        store.background_index_rebuild_service.start(
            commit_log,
            message_store_config,
            index_service,
            delay_level_table,
            max_delay_level,
        );

        let snapshot = store.background_index_rebuild_snapshot();
        assert_eq!(snapshot.state, BackgroundIndexRebuildState::Idle);
        assert!(!store.background_index_rebuild_service.has_task_group());

        let runtime_info = store.get_runtime_info();
        assert_eq!(runtime_info["backgroundIndexRebuildState"], "idle");
        assert_eq!(runtime_info["backgroundIndexRebuildEffectiveEnable"], "false");
        assert_eq!(runtime_info["backgroundIndexRebuildGrayMode"], "disabled");
        assert_eq!(
            runtime_info["backgroundIndexRebuildRollbackHint"],
            MessageStoreConfig::background_index_rebuild_rollback_hint()
        );
        assert_eq!(runtime_info["backgroundIndexRebuildQueryDegradationTotal"], "0");
        assert_eq!(runtime_info["backgroundIndexRebuildBacklogBytes"], "0");
    }

    #[test]
    fn background_index_rebuild_strict_mode_blocks_gray_start() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                enable_background_index_rebuild: true,
                recovery_mode: RecoveryMode::Strict,
                ..MessageStoreConfig::default()
            },
        );
        store.set_confirm_offset(128);

        let commit_log = store.commit_log.clone();
        let message_store_config = store.message_store_config.clone();
        let index_service = store.index_service.clone();
        let delay_level_table = store.delay_level_table_ref().clone();
        let max_delay_level = store.max_delay_level;
        store.background_index_rebuild_service.start(
            commit_log,
            message_store_config,
            index_service,
            delay_level_table,
            max_delay_level,
        );

        let snapshot = store.background_index_rebuild_snapshot();
        assert_eq!(snapshot.state, BackgroundIndexRebuildState::Idle);
        assert!(!store.background_index_rebuild_service.has_task_group());

        let runtime_info = store.get_runtime_info();
        assert_eq!(runtime_info["backgroundIndexRebuildEffectiveEnable"], "false");
        assert_eq!(runtime_info["backgroundIndexRebuildGrayMode"], "strict_blocked");
        assert_eq!(runtime_info["backgroundIndexRebuildBacklogBytes"], "0");
    }

    #[tokio::test]
    async fn background_index_rebuild_completes_and_indexes_commitlog_messages() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                flush_disk_type: FlushDiskType::AsyncFlush,
                enable_background_index_rebuild: true,
                recovery_mode: RecoveryMode::Balanced,
                background_index_rebuild_batch_size: 1,
                background_index_rebuild_bytes_per_second: 0,
                background_index_rebuild_max_retries: 1,
                ..MessageStoreConfig::default()
            },
        );
        let topic = CheetahString::from_static_str("background-index-rebuild-topic");
        let key = CheetahString::from_static_str("background-index-rebuild-key");
        let msg_size = append_encoded_test_message_with_key(
            &mut store,
            &topic,
            0,
            1_000,
            Bytes::from_static(b"background-index-rebuild-body"),
            Some(key.clone()),
        )
        .await;
        let target_offset = i64::from(msg_size);
        let checkpoint = store.store_checkpoint.as_ref().expect("store checkpoint");
        checkpoint.set_index_safe_phy_offset(0);

        let before_rebuild = store
            .query_message(&topic, &key, 10, 0, i64::MAX)
            .await
            .expect("query result before background rebuild");
        assert!(
            before_rebuild.message_maped_list.is_empty(),
            "raw commitlog append should not populate the index before background rebuild"
        );
        assert!(!before_rebuild.index_query_safe);
        assert_eq!(before_rebuild.index_safe_phyoffset, 0);
        assert_eq!(before_rebuild.index_confirm_phyoffset, target_offset);
        assert_eq!(
            store.get_runtime_info()["backgroundIndexRebuildQueryDegradationTotal"],
            "1"
        );

        let commit_log = store.commit_log.clone();
        let message_store_config = store.message_store_config.clone();
        let index_service = store.index_service.clone();
        let delay_level_table = store.delay_level_table_ref().clone();
        let max_delay_level = store.max_delay_level;
        store.background_index_rebuild_service.start(
            commit_log,
            message_store_config,
            index_service,
            delay_level_table,
            max_delay_level,
        );

        let snapshot = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = store.background_index_rebuild_snapshot();
                if snapshot.state == BackgroundIndexRebuildState::Completed {
                    break snapshot;
                }
                assert_ne!(
                    snapshot.state,
                    BackgroundIndexRebuildState::Failed,
                    "background index rebuild failed: {:?}",
                    snapshot.last_error
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("background index rebuild should complete");

        assert!(snapshot.current_safe_offset >= target_offset);
        assert_eq!(snapshot.target_offset, target_offset);
        assert_eq!(snapshot.backlog_bytes, 0);
        assert_eq!(snapshot.rebuilt_messages, 1);
        assert!(snapshot.rebuilt_bytes > 0);
        assert_eq!(
            store.index_service.index_safe_phy_offset(),
            snapshot.current_safe_offset as u64
        );

        let query_result = store
            .query_message(&topic, &key, 10, 0, i64::MAX)
            .await
            .expect("query result after background rebuild");
        assert_eq!(query_result.message_maped_list.len(), 1);
        assert!(query_result.index_query_safe);
        assert_eq!(query_result.index_safe_phyoffset, target_offset);
        assert_eq!(query_result.index_confirm_phyoffset, target_offset);

        let runtime_info = store.get_runtime_info();
        assert_eq!(runtime_info["backgroundIndexRebuildState"], "completed");
        assert_eq!(runtime_info["backgroundIndexRebuildEffectiveEnable"], "true");
        assert_eq!(runtime_info["backgroundIndexRebuildGrayMode"], "balanced_gray");
        assert_eq!(
            runtime_info["backgroundIndexRebuildCurrentSafeOffset"],
            snapshot.current_safe_offset.to_string()
        );
        assert_eq!(runtime_info["backgroundIndexRebuildBacklogBytes"], "0");
        assert_eq!(runtime_info["backgroundIndexRebuildFailureCount"], "0");
        assert_eq!(runtime_info["backgroundIndexRebuildQueryDegradationTotal"], "1");

        store.background_index_rebuild_service.shutdown().await;
    }

    #[tokio::test]
    async fn query_message_marks_empty_result_unsafe_when_index_safe_offset_lags_confirm() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store(
            &temp_dir,
            MessageStoreConfig {
                flush_disk_type: FlushDiskType::AsyncFlush,
                ..MessageStoreConfig::default()
            },
        );
        let topic = CheetahString::from_static_str("index-query-safe-range-topic");
        let key = CheetahString::from_static_str("index-query-safe-range-key");
        let msg_size = append_encoded_test_message_with_key(
            &mut store,
            &topic,
            0,
            1_000,
            Bytes::from_static(b"index-query-safe-range-body"),
            Some(key.clone()),
        )
        .await;

        let result = store
            .query_message(&topic, &key, 10, 0, i64::MAX)
            .await
            .expect("query result");

        assert!(result.message_maped_list.is_empty());
        assert!(!result.index_query_safe);
        assert_eq!(result.index_safe_phyoffset, 0);
        assert_eq!(result.index_confirm_phyoffset, i64::from(msg_size));
        assert_eq!(
            store.get_runtime_info()["backgroundIndexRebuildQueryDegradationTotal"],
            "1"
        );
    }

    #[tokio::test]
    async fn background_index_rebuild_retries_then_fails_when_commitlog_data_missing() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store_with_broker(
            &temp_dir,
            MessageStoreConfig {
                enable_background_index_rebuild: true,
                recovery_mode: RecoveryMode::Balanced,
                background_index_rebuild_bytes_per_second: 0,
                background_index_rebuild_max_retries: 1,
                ..MessageStoreConfig::default()
            },
            BrokerConfig {
                duplication_enable: true,
                ..BrokerConfig::default()
            },
        );
        store.set_confirm_offset(128);

        let commit_log = store.commit_log.clone();
        let message_store_config = store.message_store_config.clone();
        let index_service = store.index_service.clone();
        let delay_level_table = store.delay_level_table_ref().clone();
        let max_delay_level = store.max_delay_level;
        store.background_index_rebuild_service.start(
            commit_log,
            message_store_config,
            index_service,
            delay_level_table,
            max_delay_level,
        );

        let snapshot = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = store.background_index_rebuild_snapshot();
                if snapshot.state == BackgroundIndexRebuildState::Failed {
                    break snapshot;
                }
                assert_ne!(
                    snapshot.state,
                    BackgroundIndexRebuildState::Completed,
                    "background index rebuild should not complete without commitlog data"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("background index rebuild should fail after retry budget");

        assert!(snapshot.failure_count >= 2);
        assert_eq!(snapshot.target_offset, 128);
        assert_eq!(snapshot.current_safe_offset, 0);
        assert_eq!(snapshot.backlog_bytes, 128);
        assert!(snapshot
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("commitlog data unavailable")));

        let runtime_info = store.get_runtime_info();
        assert_eq!(runtime_info["backgroundIndexRebuildState"], "failed");
        assert_eq!(runtime_info["backgroundIndexRebuildEffectiveEnable"], "true");
        assert_eq!(runtime_info["backgroundIndexRebuildGrayMode"], "balanced_gray");
        assert_eq!(
            runtime_info["backgroundIndexRebuildFailureCount"],
            snapshot.failure_count.to_string()
        );
        assert!(runtime_info["backgroundIndexRebuildLastError"].contains("commitlog data unavailable"));

        store.background_index_rebuild_service.shutdown().await;
    }

    #[tokio::test]
    async fn recover_enables_local_file_consume_queue_concurrency_when_configured() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_configured_test_store_with_broker(
            &temp_dir,
            MessageStoreConfig {
                enable_local_file_consume_queue_recovery_concurrently: true,
                local_file_consume_queue_recovery_parallelism: 2,
                ..Default::default()
            },
            BrokerConfig {
                recover_concurrently: true,
                ..Default::default()
            },
        );

        store.recover(false).await;

        let report = store.last_recovery_report().expect("recovery report");
        assert!(report.plan.recover_concurrently);
        assert!(report.plan.consume_queue_recovery_concurrency.local_file_enabled);
        assert_eq!(report.plan.consume_queue_recovery_concurrency.local_file_parallelism, 2);
    }

    #[tokio::test]
    async fn start_and_init_are_rejected_while_recovering() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_test_store(&temp_dir);

        store.set_lifecycle_state(StoreLifecycleState::RecoveringConsumeQueue);
        let start_error = store
            .start()
            .await
            .expect_err("start should be rejected during recovery");
        assert!(matches!(
            start_error,
            StoreError::General(message) if message.contains("recovering")
        ));

        store.set_lifecycle_state(StoreLifecycleState::RecoveringTopicQueueTable);
        let init_error = store.init().await.expect_err("init should be rejected during recovery");
        assert!(matches!(
            init_error,
            StoreError::General(message) if message.contains("recovering")
        ));
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

    #[tokio::test]
    async fn flush_consume_queue_service_start_persists_logic_checkpoint_to_disk() {
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
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        store.flush_consume_queue_service.shutdown().await;
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
        assert_eq!(runtime_info["storeType"], "LocalFile");
        assert_eq!(runtime_info["rocksdbCqDoubleWriteEnable"], "false");
        assert_eq!(runtime_info["rocksdbCompatibilityMode"], "disabled");
        assert!(runtime_info.contains_key("ioUringBackendStatus"));
        assert_eq!(runtime_info["timerReadBehind"], "0");
        assert_eq!(runtime_info["timerOffsetBehind"], "0");
        assert_eq!(runtime_info["timerCongestNum"], "0");
        assert_eq!(runtime_info["timerEnqueueTps"], "0.0");
        assert_eq!(runtime_info["timerDequeueTps"], "0.0");
        assert_eq!(runtime_info["timerTopicBacklogDistribution"], "{}");
        assert_eq!(runtime_info["timerBacklogDistribution"], "{}");
        assert_eq!(runtime_info["putMessageLockAcquireTotal"], "0");
        assert_eq!(runtime_info["putMessageLockWaitTotalMillis"], "0");
        assert_eq!(runtime_info["putMessageLockWaitMaxMillis"], "0");
        assert_eq!(runtime_info["putMessageLockHoldTotalMillis"], "0");
        assert_eq!(runtime_info["putMessageLockHoldMaxMillis"], "0");
        assert_eq!(runtime_info["syncFlushQueueDepth"], "0");
        assert_eq!(runtime_info["syncFlushEnqueueTotal"], "0");
        assert_eq!(runtime_info["syncFlushCompletedTotal"], "0");
        assert_eq!(runtime_info["syncFlushTimeoutTotal"], "0");
        assert_eq!(runtime_info["syncFlushOldestWaitMillis"], "0");
        assert_eq!(runtime_info["syncFlushMaxWaitMillis"], "0");
        assert_eq!(runtime_info["syncFlushWaitTotalMillis"], "0");
        assert_eq!(runtime_info["reputDispatchBehindBytes"], "0");
        assert_eq!(runtime_info["reputDispatchBatchCountTotal"], "0");
        assert_eq!(runtime_info["reputDispatchRequestTotal"], "0");
        assert_eq!(runtime_info["reputDispatchBatchSizeMax"], "0");
        assert_eq!(runtime_info["reputDispatchDurationTotalMillis"], "0");
        assert_eq!(runtime_info["reputDispatchDurationMaxMillis"], "0");
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

    #[tokio::test]
    async fn reput_once_records_dispatch_batch_runtime_info() {
        let temp_dir = tempdir().unwrap();
        let mut store = new_async_flush_test_store(&temp_dir);
        let topic = CheetahString::from_static_str("reput-dispatch-runtime-topic");

        for index in 0..33 {
            let body = Bytes::from(format!("reput-dispatch-body-{index}"));
            let put_result = store.put_message(build_test_message(&topic, body)).await;
            assert_eq!(put_result.put_message_status(), PutMessageStatus::PutOk);
        }

        store.reput_once().await;

        let runtime_info = store.get_runtime_info();
        assert_eq!(runtime_info["reputDispatchBehindBytes"], "0");
        assert_eq!(runtime_info["reputDispatchBatchCountTotal"], "2");
        assert_eq!(runtime_info["reputDispatchRequestTotal"], "33");
        assert_eq!(runtime_info["reputDispatchBatchSizeMax"], "32");
        assert!(runtime_info.contains_key("reputDispatchDurationTotalMillis"));
        assert!(runtime_info.contains_key("reputDispatchDurationMaxMillis"));
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

        store
            .clean_commit_log_service
            .set_disk_clean_decision_override(Some(DiskCleanDecision::default()));
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

    #[test]
    fn clean_commit_log_service_uses_lowest_commitlog_path_ratio_like_java() {
        let temp_dir = tempdir().unwrap();
        let store = new_test_store(&temp_dir);
        let missing_path = temp_dir.path().join("missing-commitlog");
        let existing_path = temp_dir.path().join("existing-commitlog");
        fs::create_dir_all(&existing_path).expect("create existing commitlog path");
        let store_path_commit_log = format!(
            "{}{}{}",
            missing_path.display(),
            mix_all::MULTI_PATH_SPLITTER.as_str(),
            existing_path.display()
        );
        let service = CleanCommitLogService::new(
            Arc::new(MessageStoreConfig {
                store_path_root_dir: temp_dir.path().to_string_lossy().to_string().into(),
                store_path_commit_log: Some(store_path_commit_log.into()),
                ..MessageStoreConfig::default()
            }),
            store.commit_log.clone(),
            store.running_flags.clone(),
        );

        let (ratio, selected_path) = service.min_physic_disk_ratio();

        assert!(ratio < 0.0);
        assert_eq!(selected_path, Some(missing_path.to_string_lossy().into_owned()));
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
