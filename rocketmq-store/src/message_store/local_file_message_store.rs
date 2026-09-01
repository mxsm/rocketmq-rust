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
use std::future::Future;
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

use crate::config::store_runtime_config::StoreRuntimeConfig;
use arc_swap::ArcSwap;
use bytes::Buf;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::attribute::cleanup_policy::CleanupPolicy;
use rocketmq_model::common::boundary_type::BoundaryType;
use rocketmq_model::common::broker::broker_role::BrokerRole;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::message_batch::MessageExtBatch;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::mix_all;
use rocketmq_model::common::mix_all::is_lmq;
use rocketmq_model::common::mix_all::is_sys_consumer_group_for_no_cold_read_limit;
use rocketmq_model::common::mix_all::LMQ_QUEUE_ID;
use rocketmq_model::common::mix_all::MULTI_DISPATCH_QUEUE_SPLITTER;
use rocketmq_model::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_model::common::running::running_stats::RunningStats;
use rocketmq_model::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_model::common::topic::TopicValidator;
use rocketmq_model::utils::cleanup_policy_utils::get_delete_policy;
use rocketmq_model::utils::cleanup_policy_utils::get_delete_policy_arc_mut;
use rocketmq_model::utils::queue_type_utils::QueueTypeUtils;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_runtime::common::file_utils::string_to_file;
use rocketmq_runtime::common::system_clock::SystemClock;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::common::util_all;
use rocketmq_runtime::common::util_all::ensure_dir_ok;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_store_api::checkpoint::CheckpointOffsets as ReleaseCheckpointOffsets;
use rocketmq_store_api::TimerRecallRequest;
use rocketmq_store_api::TimerRecallStatus;
use rocketmq_store_api::TimerStoreMode;
use rocketmq_store_api::WriteLeaseToken;
#[cfg(feature = "extended_timeline")]
use rocketmq_store_rocksdb::store::KeyValueStore;
#[cfg(feature = "extended_timeline")]
use rocketmq_store_rocksdb::timer::codec::RecallLookupKeyV1;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::OwnedMutexGuard;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::info;
use tracing::warn;

use rocketmq_store_local::commit_log::recovery_orchestration::drive_commit_log_recovery;
use rocketmq_store_local::commit_log::recovery_orchestration::optimized_recovery_requested;
use rocketmq_store_local::commit_log::recovery_orchestration::CommitLogRecoveryStep;
use rocketmq_store_local::hook::HookRegistry;
use rocketmq_store_local::message_store::cleanup::CleanupPolicy as LocalCleanupPolicy;
use rocketmq_store_local::message_store::cleanup::DiskCleanDecision;
use rocketmq_store_local::message_store::cleanup::DiskUsageState;
use rocketmq_store_local::message_store::cleanup::ManualDeleteTracker;
use rocketmq_store_local::message_store::lifecycle::LocalStoreLifecycle;
use rocketmq_store_local::message_store::lifecycle::LocalStoreState;
use rocketmq_store_local::message_store::reput::ReputPolicy;
use rocketmq_store_local::message_store::LocalStoreComposition;

use crate::base::allocate_mapped_file_service::AllocateMappedFileService;
use crate::base::backend_ops::canonical_health_snapshot;
use crate::base::backend_ops::BackendHealthSnapshot;
use crate::base::backend_ops::BackendOps;
use crate::base::backend_ops::MessageStoreShutdownReport;
use crate::base::backend_ops::PutMessagePreflight;
use crate::base::backend_ops::StateMachineVersionView;
use crate::base::backend_ops::StoreHealthRecorder;
use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::base::get_message_result::GetMessageResult;
use crate::base::message_arriving_listener::MessageArrivingListener;
use crate::base::message_result::AppendMessageResult;
use crate::base::message_result::PutMessageResult;
use crate::base::message_status_enum::GetMessageStatus;
use crate::base::message_status_enum::PutMessageStatus;
use crate::base::query_message_request::QueryMessageRequest;
use crate::base::query_message_result::QueryMessageResult;
use crate::base::select_result::SelectMappedBufferResult;
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
use crate::log_file::commit_log::CommitLogCleanupHandle;
use crate::log_file::commit_log::CommitLogInternalMessageWriteHandle;
use crate::log_file::commit_log::CommitLogReadHandle;
use crate::log_file::commit_log::CommitLogReplicaHandle;
use crate::log_file::commit_log::CommitLogStoreContext;
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
use crate::message_store::runtime_state::StoreRuntimeState;
use crate::queue::build_consume_queue::CommitLogDispatcherBuildConsumeQueue;
use crate::queue::consume_queue_store::ConsumeQueueStoreTrait;
use crate::queue::local_file_consume_queue_store::ConsumeQueueLookupHandle;
use crate::queue::local_file_consume_queue_store::ConsumeQueueStore;
use crate::queue::local_file_consume_queue_store::ConsumeQueueStoreContext;
use crate::queue::ArcConsumeQueue;
use crate::runtime::StoreRuntimeScope;
use crate::stats::broker_stats_manager::BrokerStatsManager;
use crate::store::running_flags::RunningFlags;
use crate::store_error::StoreComponent;
use crate::store_error::StoreError;
use crate::store_error::StoreOperation;
use crate::store_path_config_helper::get_abort_file;
use crate::store_path_config_helper::get_store_checkpoint;
use crate::store_path_config_helper::get_store_path_consume_queue;
#[cfg(feature = "tieredstore")]
use crate::tieredstore::resolve_tiered_dispatch_body_with_reader;
#[cfg(feature = "tieredstore")]
use crate::tieredstore::TieredStoreDecorator;
#[cfg(feature = "extended_timeline")]
use crate::timer::clock::SystemTimerClock;
#[cfg(feature = "extended_timeline")]
use crate::timer::clock::TimerClockSafety;
#[cfg(feature = "extended_timeline")]
use crate::timer::delivery::TimelineDeliveryCoordinator;
#[cfg(feature = "extended_timeline")]
use crate::timer::engine::TimerEngine;
#[cfg(feature = "extended_timeline")]
use crate::timer::engine::WorkBudget;
#[cfg(feature = "extended_timeline")]
use crate::timer::role::TimerRoleState;
#[cfg(feature = "extended_timeline")]
use crate::timer::timeline::ExtendedTimelineEngine;
#[cfg(feature = "extended_timeline")]
use crate::timer::timeline::ShadowTimelineMaterializer;
#[cfg(feature = "extended_timeline")]
use crate::timer::timeline::TimelineAdmissionController;
#[cfg(feature = "extended_timeline")]
use crate::timer::timeline::TimelineAdmissionOutcome;
#[cfg(feature = "extended_timeline")]
use crate::timer::timeline::TimelineCompletionReconciler;
#[cfg(feature = "extended_timeline")]
use crate::timer::timeline::TimelineCompletionWake;
#[cfg(feature = "extended_timeline")]
use crate::timer::timeline::TimelineDueScanner;
#[cfg(feature = "extended_timeline")]
use crate::timer::timeline::TimelineGcService;
#[cfg(feature = "extended_timeline")]
use crate::timer::timeline::TimelinePromotionGate;
#[cfg(feature = "extended_timeline")]
use crate::timer::timeline::TimelineRecallService;
#[cfg(feature = "extended_timeline")]
use crate::timer::timeline::TimelineSnapshotManager;
use crate::timer::timer_message_store::TimerMessageStore;
use crate::timer::timer_message_store::TimerStoreContext;
use crate::transfer::segment::SegmentLease;
use crate::utils::store_util::TOTAL_PHYSICAL_MEMORY_SIZE;
#[cfg(feature = "extended_timeline")]
use rocketmq_store_api::TimerEngineEpoch;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::TieredLocalResidency;

mod composition;
mod dispatch;
mod health;
mod lifecycle;
mod lmq_quota;
mod managed_recovery;
mod mapped_file_retirement_service;
mod read_path;
mod recovery;
mod reput_pipeline;
mod root_lock;
mod write_path;

use lmq_quota::LmqQuotaController;
use lmq_quota::LmqQuotaReservation;

pub use composition::parse_delay_level;
pub use dispatch::CommitLogDispatcherDefault;
pub use recovery::BackgroundIndexRebuildSnapshot;
pub use recovery::BackgroundIndexRebuildState;

pub(crate) use dispatch::CommitLogDispatchHandle;
use dispatch::MessageArrivalCapability;
use dispatch::MessageArrivingListenerHandle;
use dispatch::ReputMessageService;
use dispatch::ReputMessageServiceInner;
use dispatch::ReputNotifyHandle;
use dispatch::ReputRuntimeContext;
use health::run_blocking_scheduled_task;
use health::store_path_disk_used_ratio;
use health::CleanCommitLogService;
use health::CleanConsumeQueueService;
use health::CommitLogWalPin;
use health::CorrectLogicOffsetService;
use lifecycle::FlushConsumeQueueService;
use managed_recovery::inspect_and_reconcile_managed_root;
use managed_recovery::require_wave_b_ready;
use managed_recovery::validate_wave_b_configuration;
use managed_recovery::wave_b_activation_fence;
use mapped_file_retirement_service::MappedFileRetirementService;
use read_path::estimate_in_mem_by_commit_offset;
use read_path::is_the_batch_full;
use recovery::BackgroundIndexRebuildService;
use rocketmq_store_local::mapped_file::ManagedLifecycleRuntime;
use rocketmq_store_local::mapped_file::PreparedManagedLifecycleActivation;
use root_lock::StoreRootLease;
use root_lock::StoreRootMode;
use write_path::murmur3_x64_128_bytes;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StoreRootLeaseState {
    Operational,
    DestroyRetryPending,
    Destroyed,
}

enum PendingHAService {
    Default(Box<crate::ha::default_ha_service::DefaultHAService>),
    AutoSwitch(crate::ha::auto_switch::auto_switch_ha_service::AutoSwitchHAService),
}

/// Narrow Local Store capability used by HA replica readers.
///
/// The handle deliberately excludes queue, index, dispatcher, and lifecycle
/// mutation APIs while retaining replica append/read, confirm publication, and
/// replication progress state shared by the owning Store.
#[derive(Clone)]
pub(crate) struct HAReplicaStoreHandle {
    message_store_config: Arc<MessageStoreConfig>,
    shutdown: Arc<AtomicBool>,
    running_flags: Arc<RunningFlags>,
    master_flushed_offset: Arc<AtomicI64>,
    alive_replica_num_in_group: Arc<AtomicI32>,
    state_machine_version: Arc<AtomicI64>,
    controller_epoch_start_offset: Arc<AtomicI64>,
    commit_log: CommitLogReplicaHandle,
}

impl HAReplicaStoreHandle {
    #[inline]
    pub(crate) fn message_store_config(&self) -> Arc<MessageStoreConfig> {
        self.message_store_config.clone()
    }

    #[inline]
    pub(crate) fn message_store_config_ref(&self) -> &MessageStoreConfig {
        self.message_store_config.as_ref()
    }

    #[inline]
    pub(crate) fn get_max_phy_offset(&self) -> i64 {
        self.commit_log.get_max_offset()
    }

    #[inline]
    pub(crate) fn get_min_phy_offset(&self) -> i64 {
        self.commit_log.get_min_offset()
    }

    #[inline]
    pub(crate) fn get_flushed_where(&self) -> i64 {
        self.commit_log.get_flushed_where()
    }

    #[inline]
    pub(crate) fn get_confirm_offset(&self) -> i64 {
        self.commit_log.get_confirm_offset()
    }

    #[inline]
    pub(crate) fn get_confirm_offset_directly(&self) -> i64 {
        self.commit_log.get_confirm_offset_directly()
    }

    pub(crate) fn select_segments(
        &self,
        offset: i64,
        max_bytes: usize,
        allow_cross_file: bool,
    ) -> Result<Option<Vec<SegmentLease>>, StoreError> {
        self.commit_log.select_segments(offset, max_bytes, allow_cross_file)
    }

    #[inline]
    pub(crate) fn get_master_flushed_offset(&self) -> i64 {
        self.master_flushed_offset.load(Ordering::SeqCst)
    }

    #[inline]
    pub(crate) fn get_alive_replica_num_in_group(&self) -> i32 {
        self.alive_replica_num_in_group.load(Ordering::SeqCst)
    }

    #[inline]
    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    #[inline]
    pub(crate) fn publish_state_machine_version(&self, state_machine_version: i64) {
        self.state_machine_version
            .store(state_machine_version, Ordering::SeqCst);
    }

    #[inline]
    pub(crate) fn publish_controller_epoch_start_offset(&self, epoch_start_offset: i64) {
        self.controller_epoch_start_offset
            .store(epoch_start_offset, Ordering::SeqCst);
    }

    pub(crate) async fn append_replica_data(
        &self,
        start_offset: i64,
        data: &[u8],
        data_start: i32,
        data_length: i32,
    ) -> Result<bool, StoreError> {
        if self.shutdown.load(Ordering::Acquire) || !self.running_flags.is_writeable() {
            warn!("message store is unavailable for writes, so replica append is forbidden");
            return Ok(false);
        }

        let appended = self
            .commit_log
            .append_data(start_offset, data, data_start, data_length)
            .await?;
        if !appended {
            error!(
                "HA replica append failed, physical offset={}, data length={}",
                start_offset, data_length
            );
        }
        Ok(appended)
    }

    #[inline]
    pub(crate) fn publish_confirm_offset(&self, phy_offset: i64) {
        self.commit_log.publish_confirm_offset(phy_offset);
    }
}

/// Narrow Local Store write capability used by Timer redelivery.
///
/// The handle shares only lifecycle state, hook snapshots, queue-offset state, CommitLog's
/// internal-message append port, statistics, and reput notification. It cannot recover, start,
/// stop, or otherwise mutate the owning Store root.
#[derive(Clone)]
pub(crate) struct TimerMessageWriteHandle {
    message_store_config: Arc<MessageStoreConfig>,
    lifecycle: Arc<LocalStoreLifecycle>,
    shutdown: Arc<AtomicBool>,
    running_flags: Arc<RunningFlags>,
    put_message_hooks: HookRegistry<dyn PutMessageHook + Send + Sync>,
    topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
    commit_log: CommitLogInternalMessageWriteHandle,
    consume_queue_store: ConsumeQueueStore,
    store_stats_service: Arc<StoreStatsService>,
    reput_notify: ReputNotifyHandle,
    lmq_quota_controller: Arc<LmqQuotaController>,
}

impl TimerMessageWriteHandle {
    pub(crate) async fn put_message(&self, mut msg: MessageExtBrokerInner) -> PutMessageResult {
        if !self
            .lifecycle
            .is_available_for_io(self.shutdown.load(Ordering::Acquire))
            || !self.running_flags.is_writeable()
        {
            warn!("message store is unavailable for writes, so Timer putMessage is forbidden");
            return PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable);
        }

        for hook in self.put_message_hooks.snapshot() {
            if let Some(result) = hook.execute_before_put_message(&mut msg) {
                return result;
            }
        }
        let lmq_dispatch_queue_keys = self.prepare_lmq_dispatch(&mut msg);
        let lmq_quota_reservation = match self.reserve_lmq_quota(&lmq_dispatch_queue_keys) {
            Some(reservation) => reservation,
            None => return PutMessageResult::new_default(PutMessageStatus::LmqConsumeQueueNumExceeded),
        };
        let lmq_dispatch_message_num = Self::lmq_dispatch_message_num(&msg);

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
            let topic_config = self.topic_config_table.get(msg.topic()).as_deref().cloned();
            if !QueueTypeUtils::is_batch_cq_arc_mut(topic_config.as_ref()) {
                error!("[BUG]The message is an inner batch but cq type is not batch cq");
                return PutMessageResult::new_default(PutMessageStatus::MessageIllegal);
            }
        }

        let begin_time = Instant::now();
        let result = self.commit_log.put_message(msg).await;
        let elapsed_time = begin_time.elapsed().as_millis();
        if elapsed_time > 500 {
            warn!("Timer putMessage: CommitLog put cost {}ms", elapsed_time);
        }
        self.store_stats_service
            .set_put_message_entire_time_max(elapsed_time as u64);
        if !result.is_ok() {
            self.store_stats_service
                .get_put_message_failed_times()
                .fetch_add(1, Ordering::AcqRel);
        } else {
            for queue_key in &lmq_dispatch_queue_keys {
                self.consume_queue_store
                    .increase_lmq_offset(queue_key.as_str(), lmq_dispatch_message_num);
            }
            if let Some(reservation) = lmq_quota_reservation {
                reservation.commit();
            }
            self.reput_notify.notify_new_message();
        }
        result
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

        let mut queue_keys = Vec::new();
        let mut unique_queue_keys = HashSet::new();
        let mut saw_queue = false;
        let mut is_all_lmq_dispatch = true;
        for queue_name in multi_dispatch_queue.split_str(MULTI_DISPATCH_QUEUE_SPLITTER) {
            if queue_name.is_empty() {
                is_all_lmq_dispatch = false;
                continue;
            }
            saw_queue = true;
            if self.message_store_config.enable_lmq && is_lmq(Some(queue_name)) {
                let queue_key = format!("{queue_name}-{LMQ_QUEUE_ID}");
                if unique_queue_keys.insert(queue_key.clone()) {
                    queue_keys.push(queue_key);
                }
            } else {
                is_all_lmq_dispatch = false;
            }
        }
        if msg
            .property(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET)
            .is_some_and(|queue_offset| !queue_offset.is_empty())
        {
            return queue_keys;
        }
        if !(saw_queue && is_all_lmq_dispatch) {
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

    fn reserve_lmq_quota(&self, queue_keys: &[String]) -> Option<Option<LmqQuotaReservation>> {
        if !self.message_store_config.enable_lmq_quota
            || !self.message_store_config.enable_lmq
            || !self.message_store_config.enable_multi_dispatch
            || queue_keys.is_empty()
        {
            return Some(None);
        }
        let existing_queue_keys = self
            .consume_queue_store
            .get_lmq_topic_names()
            .into_iter()
            .map(|topic| format!("{topic}-{LMQ_QUEUE_ID}"));
        self.lmq_quota_controller
            .reserve(
                queue_keys,
                existing_queue_keys,
                self.message_store_config.max_lmq_consume_queue_num,
            )
            .map(Some)
    }

    fn lmq_dispatch_message_num(msg: &MessageExtBrokerInner) -> i16 {
        msg.property(MessageConst::PROPERTY_INNER_NUM)
            .and_then(|message_num| message_num.parse::<i16>().ok())
            .unwrap_or(1)
    }
}

///Using local files to store message data, which is also the default method.
pub struct LocalFileMessageStore {
    runtime_scope: StoreRuntimeScope,
    telemetry: crate::telemetry::StoreTelemetry,
    message_store_config: Arc<MessageStoreConfig>,
    store_runtime_state: Arc<StoreRuntimeState>,
    composition: LocalStoreComposition,
    broker_config: Arc<StoreRuntimeConfig>,
    put_message_hook_list: HookRegistry<dyn PutMessageHook + Send + Sync>,
    topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
    commit_log: CommitLog,

    store_checkpoint: Option<Arc<StoreCheckpoint>>,
    master_flushed_offset: Arc<AtomicI64>,
    alive_replica_num_in_group: Arc<AtomicI32>,
    index_service: IndexService,
    allocate_mapped_file_service: Arc<AllocateMappedFileService>,
    consume_queue_store: ConsumeQueueStore,
    lmq_quota_controller: Arc<LmqQuotaController>,
    dispatcher: CommitLogDispatcherDefault,
    #[cfg(feature = "tieredstore")]
    tiered_store: Option<Arc<TieredStoreDecorator>>,
    broker_init_max_offset: Arc<AtomicI64>,
    state_machine_version: Arc<AtomicI64>,
    controller_epoch_start_offset: Arc<AtomicI64>,
    shutdown: Arc<AtomicBool>,
    background_index_query_degradation_total: Arc<AtomicU64>,
    running_flags: Arc<RunningFlags>,
    store_health_recorder: StoreHealthRecorder,
    reput_message_service: ReputMessageService,
    background_index_rebuild_service: BackgroundIndexRebuildService,
    clean_commit_log_service: Arc<CleanCommitLogService>,
    correct_logic_offset_service: Arc<CorrectLogicOffsetService>,
    clean_consume_queue_service: Arc<CleanConsumeQueueService>,
    broker_stats_manager: Option<Arc<BrokerStatsManager>>,
    message_arrival: MessageArrivalCapability,
    notify_message_arrive_in_batch: bool,
    store_stats_service: Arc<StoreStatsService>,

    compaction_store: Arc<CompactionStore>,
    compaction_service: Option<CompactionService>,

    timer_message_store: Option<Arc<TimerMessageStore>>,
    #[cfg(feature = "extended_timeline")]
    extended_timeline_cleanup_pin: Arc<AtomicI64>,
    #[cfg(feature = "extended_timeline")]
    extended_timeline_materializer: Option<Arc<ShadowTimelineMaterializer>>,
    #[cfg(feature = "extended_timeline")]
    extended_timeline_engine: Option<ExtendedTimelineEngine>,
    #[cfg(feature = "extended_timeline")]
    extended_timeline_due_scanner: Option<Arc<TimelineDueScanner>>,
    #[cfg(feature = "extended_timeline")]
    extended_timeline_recall_service: Option<Arc<TimelineRecallService>>,
    #[cfg(feature = "extended_timeline")]
    extended_timeline_completion_wake: Arc<TimelineCompletionWake>,
    #[cfg(feature = "extended_timeline")]
    extended_timeline_completion_reconciler: Option<Arc<TimelineCompletionReconciler>>,
    #[cfg(feature = "extended_timeline")]
    extended_timeline_delivery: Option<Arc<TimelineDeliveryCoordinator>>,
    #[cfg(feature = "extended_timeline")]
    extended_timeline_snapshot: Option<Arc<TimelineSnapshotManager>>,
    #[cfg(feature = "extended_timeline")]
    extended_timeline_promotion_gate: Option<Arc<TimelinePromotionGate>>,
    #[cfg(feature = "extended_timeline")]
    extended_timeline_admission: Option<Arc<TimelineAdmissionController>>,
    #[cfg(feature = "extended_timeline")]
    extended_timeline_gc: Option<Arc<TimelineGcService>>,
    #[cfg(feature = "extended_timeline")]
    extended_timeline_role: Arc<TimerRoleState>,
    #[cfg(feature = "extended_timeline")]
    extended_timeline_clock: Arc<TimerClockSafety>,
    transient_store_pool: TransientStorePool,
    root_dependencies_wired: bool,
    master_store_in_process: StdRwLock<Option<Arc<dyn Any + Send + Sync>>>,
    send_message_back_hook: StdRwLock<Option<Arc<dyn SendMessageBackHook>>>,
    pending_ha_service: Option<PendingHAService>,
    ha_service: Option<GeneralHAService>,
    flush_consume_queue_service: FlushConsumeQueueService,
    scheduled_task_shutdown: CancellationToken,
    scheduled_task_group: Option<rocketmq_runtime::TaskGroup>,
    scheduled_tasks: Option<ScheduledTaskGroup>,
    mapped_file_retirement_service: Option<MappedFileRetirementService>,
    managed_lifecycle_activation: Option<PreparedManagedLifecycleActivation>,
    managed_lifecycle_runtime: Option<ManagedLifecycleRuntime>,
    ha_update_master_group: Arc<StdMutex<Option<rocketmq_runtime::TaskGroup>>>,
    delay_level_table: Arc<BTreeMap<i32 /* level */, i64 /* delay timeMillis */>>,
    max_delay_level: i32,
    last_recovery_report: Option<RecoveryReport>,
    store_root_lease_state: StoreRootLeaseState,
    store_root_mode: StoreRootMode,
    // Declared last so the verified Store-root boundary outlives every component field during
    // ordinary Rust field destruction, including mapped-file and allocator owner teardown.
    store_root_lease: StoreRootLease,
}

pub(crate) struct LocalReleaseCheckpointWriteLease {
    _write_guard: OwnedMutexGuard<()>,
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

    let mut queue_iter = multi_dispatch_queue.split_str(MULTI_DISPATCH_QUEUE_SPLITTER);
    let mut offset_iter = multi_queue_offset.split_str(MULTI_DISPATCH_QUEUE_SPLITTER);
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
        .split_str(MULTI_DISPATCH_QUEUE_SPLITTER)
        .zip(multi_queue_offset.split_str(MULTI_DISPATCH_QUEUE_SPLITTER))
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
    pub(crate) async fn begin_release_checkpoint(
        &self,
        deadline: ShutdownDeadline,
    ) -> Result<(ReleaseCheckpointOffsets, LocalReleaseCheckpointWriteLease), StoreError> {
        let write_lock = self.commit_log.release_checkpoint_write_lock();
        let write_guard = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline.instant()),
            write_lock.lock_owned(),
        )
        .await
        .map_err(|_| {
            StoreError::new(&rocketmq_error::STORAGE_OPERATION_TIMED_OUT, StoreOperation::Flush)
                .in_component(StoreComponent::CommitLog)
                .with_detail("release-checkpoint write barrier deadline expired")
        })?;

        self.reput_message_service.new_message_notify.notify_one();
        if !self
            .reput_message_service
            .wait_until_release_checkpoint_drained(deadline)
            .await
        {
            return Err(
                StoreError::new(&rocketmq_error::STORAGE_OPERATION_TIMED_OUT, StoreOperation::Flush)
                    .with_detail("release-checkpoint derived-state barrier deadline expired"),
            );
        }

        let appended_offset = self.commit_log.get_max_offset();
        let commit_log_flush = self.commit_log.release_checkpoint_flush_handle();
        let consume_queue_store = self.consume_queue_store.clone();
        let store_checkpoint = self.store_checkpoint.clone().ok_or_else(|| {
            StoreError::new(&rocketmq_error::STORAGE_INTERNAL_FAILURE, StoreOperation::Flush)
                .with_detail("Store checkpoint is unavailable")
        })?;
        let index_service = self.index_service.clone();
        let (durable_offset, index_offset) = self
            .runtime_scope
            .spawn_io_until("local-store.release-checkpoint-flush", deadline, move || {
                let durable_offset = commit_log_flush
                    .try_flush(0)
                    .map_err(|error| {
                        StoreError::new(&rocketmq_error::STORAGE_IO_FAILED, StoreOperation::Flush)
                            .in_component(StoreComponent::MappedFile)
                            .with_source(error)
                    })?
                    .durable;
                FlushConsumeQueueService::flush_once_blocking(&consume_queue_store, &store_checkpoint, 0);
                let index_offset = index_service.flush_release_checkpoint().map_err(|error| {
                    StoreError::new(&rocketmq_error::STORAGE_IO_FAILED, StoreOperation::Flush)
                        .in_component(StoreComponent::MappedFile)
                        .with_source(error)
                })?;
                Ok::<_, StoreError>((durable_offset, index_offset))
            })
            .await
            .map_err(|error| {
                StoreError::new(&rocketmq_error::STORAGE_OPERATION_TIMED_OUT, StoreOperation::Flush)
                    .with_detail("release-checkpoint blocking flush failed")
                    .with_source(error)
            })??;
        if durable_offset != appended_offset {
            return Err(
                StoreError::new(&rocketmq_error::STORAGE_WRITE_FAILED, StoreOperation::Flush).with_detail(format!(
                    "release-checkpoint flush stopped at {durable_offset}, expected {appended_offset}"
                )),
            );
        }

        let consume_queue_offset = self
            .dispatcher
            .min_dispatch_progress_offset(self.commit_log.get_min_offset().max(0))
            .unwrap_or(durable_offset);
        let offsets = ReleaseCheckpointOffsets {
            appended_offset,
            durable_offset,
            consume_queue_offset,
            index_offset,
        };
        offsets.validate().map_err(|error| {
            StoreError::new(&rocketmq_error::STORAGE_STATE_CORRUPTED, StoreOperation::Flush)
                .with_detail("release-checkpoint offsets violate Store ordering")
                .with_source(error)
        })?;
        Ok((
            offsets,
            LocalReleaseCheckpointWriteLease {
                _write_guard: write_guard,
            },
        ))
    }
}

#[allow(unused_variables)]
#[allow(unused_assignments)]
impl BackendOps for LocalFileMessageStore {
    async fn load(&mut self) -> bool {
        self.load_store().await
    }

    async fn start(&mut self) -> Result<(), StoreError> {
        self.start_store().await
    }

    async fn init(&mut self) -> Result<(), StoreError> {
        self.initialize_store().await
    }

    async fn shutdown_gracefully(&mut self) -> Result<MessageStoreShutdownReport, StoreError> {
        self.shutdown_store_gracefully().await
    }

    async fn shutdown(&mut self) {
        self.shutdown_store().await;
    }

    async fn destroy_gracefully(&mut self) -> Result<bool, StoreError> {
        self.destroy_store_gracefully().await
    }

    fn destroy(&mut self) {
        self.destroy_store();
    }

    async fn put_message(&mut self, msg: MessageExtBrokerInner) -> PutMessageResult {
        self.put_message_shared(msg).await
    }

    async fn put_messages(&mut self, message_ext_batch: MessageExtBatch) -> PutMessageResult {
        self.put_messages_shared(message_ext_batch).await
    }

    fn recall_extended_timer(&self, request: &TimerRecallRequest) -> Result<TimerRecallStatus, StoreError> {
        #[cfg(feature = "extended_timeline")]
        {
            if self.message_store_config.timer_store_mode != TimerStoreMode::ExtendedTimeline {
                return Ok(TimerRecallStatus::Unsupported);
            }
            if !self.extended_timeline_role.is_active() {
                return Ok(TimerRecallStatus::Retry);
            }
            let Some(recall) = self.extended_timeline_recall_service.as_ref() else {
                return Ok(TimerRecallStatus::Retry);
            };
            let result = recall
                .recall(&RecallLookupKeyV1 {
                    engine: rocketmq_store_api::TimerEngineId::ExtendedTimeline,
                    topic: request.topic.clone(),
                    unique_key: request.unique_key.clone(),
                })
                .map_err(|error| {
                    StoreError::new(&rocketmq_error::STORAGE_WRITE_FAILED, StoreOperation::Admin)
                        .with_detail("Extended Timer Recall failed")
                        .with_source(error)
                })?;
            Ok(match result {
                crate::timer::timeline::RecallResult::Cancelled { .. } => TimerRecallStatus::Cancelled,
                crate::timer::timeline::RecallResult::AlreadyCancelled => TimerRecallStatus::AlreadyCancelled,
                crate::timer::timeline::RecallResult::TooLate => TimerRecallStatus::TooLate,
                crate::timer::timeline::RecallResult::NotFound => TimerRecallStatus::NotFound,
                crate::timer::timeline::RecallResult::Retry | crate::timer::timeline::RecallResult::StaleGeneration => {
                    TimerRecallStatus::Retry
                }
                crate::timer::timeline::RecallResult::Quarantined => TimerRecallStatus::Quarantined,
            })
        }
        #[cfg(not(feature = "extended_timeline"))]
        {
            let _ = request;
            Ok(TimerRecallStatus::Unsupported)
        }
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
        self.read_messages(group, topic, queue_id, offset, max_msg_nums, message_filter)
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
        self.read_messages_with_size_limit(
            group,
            topic,
            queue_id,
            offset,
            max_msg_nums,
            max_total_msg_size,
            message_filter,
        )
        .await
    }

    fn get_max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        self.get_max_offset_in_queue_committed(topic, queue_id, true)
    }

    fn get_max_offset_in_queue_committed(&self, topic: &CheetahString, queue_id: i32, committed: bool) -> i64 {
        if committed {
            let queue = self.consume_queue_store.find_or_create_consume_queue(topic, queue_id);
            let queue = queue.read();
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
            .and_then(|consume_queue| {
                consume_queue
                    .read()
                    .get(consume_queue_offset)
                    .map(|cq_unit| cq_unit.pos)
            })
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
        if let Some(tiered_store) = self.tiered_store.as_ref() {
            let local_range_missing = self.should_try_tiered_offset_by_time(topic, queue_id, timestamp);
            if tiered_store.should_try_offset_by_time(local_range_missing) {
                if let Some(offset) = tiered_store
                    .offset_by_time(topic, queue_id, timestamp, boundary_type)
                    .await?
                {
                    return Ok(offset);
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
            self.composition
                .config()
                .recovery
                .background_index_rebuild_enabled
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
        if let Some(recovery) = self.last_recovery_report() {
            let failed_phases = recovery
                .phases
                .iter()
                .filter(|phase| phase.status == crate::message_store::recovery::RecoveryPhaseStatus::Failed)
                .count();
            let fallback_phases = recovery
                .phases
                .iter()
                .filter(|phase| phase.status == crate::message_store::recovery::RecoveryPhaseStatus::Fallback)
                .count();
            result.insert("recoveryReportAvailable".to_string(), "true".to_string());
            result.insert(
                "recoveryTotalDurationMillis".to_string(),
                recovery.total_duration_ms.to_string(),
            );
            result.insert("recoveryPhaseCount".to_string(), recovery.phases.len().to_string());
            result.insert("recoveryFailedPhaseCount".to_string(), failed_phases.to_string());
            result.insert("recoveryFallbackPhaseCount".to_string(), fallback_phases.to_string());
            result.insert(
                "recoveryFallbackReasonPresent".to_string(),
                recovery.fallback_reason.is_some().to_string(),
            );
            result.insert(
                "recoveryScannedBytes".to_string(),
                recovery.stats.scanned_bytes.to_string(),
            );
            result.insert(
                "recoveryRecoveredMessages".to_string(),
                recovery.stats.recovered_messages.to_string(),
            );
            result.insert(
                "recoveryInvalidMessages".to_string(),
                recovery.stats.invalid_messages.to_string(),
            );
            result.insert(
                "recoveryTruncatedFiles".to_string(),
                recovery.stats.truncated_files.to_string(),
            );
            result.insert(
                "recoveryIndexFilesRemoved".to_string(),
                recovery.stats.index_files_removed.to_string(),
            );
            result.insert(
                "recoveryIndexFilesRebuilt".to_string(),
                recovery.stats.index_files_rebuilt.to_string(),
            );
        } else {
            result.insert("recoveryReportAvailable".to_string(), "false".to_string());
        }

        #[cfg(feature = "tieredstore")]
        if let Some(tiered_store) = self.tiered_store.as_ref() {
            result.insert("tieredStoreConfigured".to_string(), "true".to_string());
            result.insert(
                "tieredDispatchReady".to_string(),
                tiered_store.is_dispatch_ready().to_string(),
            );
            result.insert(
                "tieredMinimumPinnedWalSegment".to_string(),
                tiered_store
                    .minimum_pinned_wal_segment()
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
            );
        } else {
            result.insert("tieredStoreConfigured".to_string(), "false".to_string());
            result.insert("tieredDispatchReady".to_string(), "false".to_string());
            result.insert("tieredMinimumPinnedWalSegment".to_string(), String::new());
        }
        #[cfg(not(feature = "tieredstore"))]
        {
            result.insert("tieredStoreConfigured".to_string(), "false".to_string());
            result.insert("tieredDispatchReady".to_string(), "false".to_string());
            result.insert("tieredMinimumPinnedWalSegment".to_string(), String::new());
        }

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
            if let Ok(storage_metrics) =
                serde_json::to_string(&crate::timer::timer_metrics::TimerMetrics::storage_runtime_metrics(
                    timer_message_store.storage_metrics_snapshot(),
                ))
            {
                result.insert("timerStorageMetrics".to_string(), storage_metrics);
            }
            if let Some(pipeline_snapshot) = timer_message_store.pipeline_diagnostics() {
                if let Ok(pipeline_metrics) = serde_json::to_string(
                    &crate::timer::timer_metrics::TimerMetrics::pipeline_runtime_metrics(pipeline_snapshot),
                ) {
                    result.insert("timerPipelineMetrics".to_string(), pipeline_metrics);
                }
            }
        } else {
            result.insert("timerReadBehind".to_string(), "0".to_string());
            result.insert("timerOffsetBehind".to_string(), "0".to_string());
            result.insert("timerCongestNum".to_string(), "0".to_string());
            result.insert("timerEnqueueTps".to_string(), "0.0".to_string());
            result.insert("timerDequeueTps".to_string(), "0.0".to_string());
            result.insert("timerTopicBacklogDistribution".to_string(), "{}".to_string());
            result.insert("timerBacklogDistribution".to_string(), "{}".to_string());
            result.insert("timerStorageMetrics".to_string(), "{}".to_string());
            result.insert("timerPipelineMetrics".to_string(), "{}".to_string());
        }
        result.insert(
            "timerStoreMode".to_string(),
            self.message_store_config.timer_store_mode.as_str().to_string(),
        );
        result.insert(
            "timerMaximumHorizonDays".to_string(),
            self.message_store_config
                .timer_extended_admission_horizon_days
                .to_string(),
        );
        result.insert(
            "timerPrecisionMillis".to_string(),
            self.message_store_config.timer_precision_ms.to_string(),
        );
        result.insert(
            "timerExtendedCapabilityVersion".to_string(),
            u16::from(cfg!(feature = "extended_timeline")).to_string(),
        );
        result.insert(
            "timerExtendedFormatVersion".to_string(),
            rocketmq_store_api::EXTENDED_TIMELINE_FORMAT_VERSION.to_string(),
        );
        #[cfg(feature = "extended_timeline")]
        if let Some(materializer) = self.extended_timeline_materializer.as_ref() {
            let metrics = materializer.metrics();
            result.insert(
                "timerExtendedMaterializationLag".to_string(),
                metrics.materialization_lag.to_string(),
            );
            result.insert(
                "timerExtendedMaterializedRecords".to_string(),
                metrics.materialized_records.to_string(),
            );
            result.insert(
                "timerExtendedMaterializedBytes".to_string(),
                metrics.materialized_bytes.to_string(),
            );
            result.insert(
                "timerExtendedMaterializationFailures".to_string(),
                metrics.materialization_failures.to_string(),
            );
            result.insert(
                "timerExtendedPayloadLiveBytes".to_string(),
                metrics.payload_live_bytes.to_string(),
            );
            result.insert(
                "timerExtendedPayloadRecords".to_string(),
                metrics.payload_records.to_string(),
            );
            result.insert(
                "timerExtendedPayloadPartitions".to_string(),
                metrics.payload_partitions.to_string(),
            );
            result.insert(
                "timerExtendedPayloadOpenHandles".to_string(),
                metrics.payload_open_handles.to_string(),
            );
            result.insert(
                "timerExtendedTimelineBytesWritten".to_string(),
                metrics.timeline_bytes_written.to_string(),
            );
            result.insert(
                "timerExtendedTimelineBytesRead".to_string(),
                metrics.timeline_bytes_read.to_string(),
            );
            result.insert(
                "timerExtendedTimelineErrors".to_string(),
                metrics.timeline_errors.to_string(),
            );
            result.insert(
                "timerExtendedShadowCompared".to_string(),
                metrics.reconciliation.compared.to_string(),
            );
            result.insert(
                "timerExtendedShadowDifferences".to_string(),
                metrics.reconciliation.differences.to_string(),
            );
            result.insert(
                "timerExtendedShadowDueObserved".to_string(),
                metrics.reconciliation.due_observed.to_string(),
            );
            result.insert(
                "timerExtendedShadowRetainedSamples".to_string(),
                metrics.reconciliation.retained_samples.to_string(),
            );
            result.insert(
                "timerExtendedAdmissionActive".to_string(),
                (self.message_store_config.timer_extended_admission_enable
                    && self.extended_timeline_role.accepts_admission())
                .to_string(),
            );
            result.insert(
                "timerExtendedRoleEpoch".to_string(),
                self.extended_timeline_role.epoch().to_string(),
            );
            let clock = self.extended_timeline_clock.snapshot();
            result.insert(
                "timerExtendedClockState".to_string(),
                match clock.state {
                    crate::timer::clock::TimerClockState::Safe => "SAFE",
                    crate::timer::clock::TimerClockState::Unsafe => "CLOCK_UNSAFE",
                }
                .to_string(),
            );
            result.insert(
                "timerExtendedClockBackwardJumps".to_string(),
                clock.backward_jumps.to_string(),
            );
            result.insert(
                "timerExtendedLargestForwardJumpMillis".to_string(),
                clock.largest_forward_jump_ms.to_string(),
            );
            if let Some(completion) = self.extended_timeline_completion_reconciler.as_ref() {
                if let Ok(cursor) = completion.completion_physical_cursor() {
                    result.insert("timerExtendedCompletionPhysicalCursor".to_string(), cursor.to_string());
                }
            }
            if let Some(gate) = self.extended_timeline_promotion_gate.as_ref() {
                result.insert(
                    "timerExtendedInstalledSnapshotGeneration".to_string(),
                    gate.snapshot_generation().to_string(),
                );
            }
            if let Ok(page) = materializer
                .timeline()
                .range_scan(i64::MIN, i64::MAX, None, 1, 4 * 1024)
            {
                let oldest = page.entries.first().map_or(0, |entry| entry.key.due_time_ms);
                result.insert("timerExtendedOldestPendingDueMillis".to_string(), oldest.to_string());
                if let Some(entry) = page.entries.first() {
                    if let Ok(Some(state)) = materializer
                        .timeline()
                        .state_index()
                        .get(entry.key.timer_id, entry.key.generation)
                    {
                        result.insert(
                            "timerExtendedOldestPendingState".to_string(),
                            format!("{:?}", state.state),
                        );
                    }
                    if let Ok(ready) = materializer.timeline().store().get_cf(
                        rocketmq_store_rocksdb::timer::READY_CF,
                        &rocketmq_store_rocksdb::timer::codec::encode_ready_key(entry.key),
                    ) {
                        result.insert(
                            "timerExtendedOldestPendingReady".to_string(),
                            ready.is_some().to_string(),
                        );
                    }
                }
            }
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
            if let Some(cq) = logic_queue.read().get_earliest_unit_and_store_time() {
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
            if let Some(cq) = logic_queue.read().get_cq_unit_and_store_time(consume_queue_offset) {
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
            if let Some(cq) = logic_queue.read().get_cq_unit_and_store_time(consume_queue_offset) {
                return Ok(cq.1);
            }
        }
        #[cfg(feature = "tieredstore")]
        if let Some(tiered_store) = self.tiered_store.as_ref() {
            return tiered_store
                .message_timestamp(topic, queue_id, consume_queue_offset)
                .await;
        }
        Ok(-1)
    }

    fn get_message_total_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        if let Some(logic_queue) = self.get_consume_queue(topic, queue_id) {
            return logic_queue.read().get_message_total_in_queue();
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
        self.append_replica_bytes(start_offset, data, data_start, data_length)
            .await
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
        self.query_messages(topic, key, max_num, begin_timestamp, end_timestamp)
            .await
    }

    async fn query_message_with_options(&self, request: &QueryMessageRequest) -> Option<QueryMessageResult> {
        let key = request.legacy_backend_key();
        self.query_messages(&request.topic, &key, request.max_num, request.begin, request.end)
            .await
    }

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
                        *task_group = Some(crate::runtime::task_group(
                            &self.runtime_scope,
                            "rocketmq-store.local-file.ha-master-update",
                        ));
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

    fn delete_topics(&self, delete_topics: Vec<&CheetahString>) -> i32 {
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
        let consume_queue = consume_queue.read();
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
        match self.try_flush() {
            Ok(progress) => progress.durable,
            Err(error) => {
                warn!(error = %error, "message store flush failed; store is no longer writeable");
                self.get_flushed_where()
            }
        }
    }

    fn try_flush(&self) -> Result<crate::consume_queue::mapped_file_queue::FlushProgress, StoreError> {
        let result = self.commit_log.try_flush();
        if let Err(error) = &result {
            self.record_flush_failure(error);
        }
        result
    }

    fn get_flushed_where(&self) -> i64 {
        self.commit_log.get_flushed_where()
    }

    fn reset_write_offset(&self, phy_offset: i64) -> bool {
        self.commit_log.reset_offset(phy_offset)
    }

    fn get_confirm_offset(&self) -> i64 {
        self.commit_log.get_confirm_offset()
    }

    fn set_confirm_offset(&mut self, phy_offset: i64) {
        self.publish_confirm_offset(phy_offset);
    }

    fn is_os_page_cache_busy(&self) -> bool {
        let begin = self.commit_log.begin_time_in_lock().load(Ordering::Relaxed);
        let diff = current_millis() - begin;
        diff < 10000000 && diff > self.message_store_config.os_page_cache_busy_timeout_mills
    }

    fn put_message_preflight(&self) -> PutMessagePreflight {
        PutMessagePreflight::new(
            self.shutdown.clone(),
            self.running_flags.clone(),
            self.commit_log.begin_time_in_lock().clone(),
            self.commit_log.controller_write_lease_state(),
            self.commit_log.mapped_file_queue_runtime_state(),
        )
    }

    fn sync_flush_runtime_info(&self) -> crate::base::flush_manager::SyncFlushRuntimeInfo {
        self.commit_log.sync_flush_runtime_info()
    }

    fn health_snapshot(&self) -> rocketmq_store_api::StoreHealthSnapshot {
        canonical_health_snapshot(
            self.store_health_snapshot(),
            self.get_max_phy_offset(),
            self.get_flushed_where(),
        )
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

    fn current_broker_role(&self) -> BrokerRole {
        self.store_runtime_state.broker_role()
    }

    fn data_read_ahead_enabled(&self) -> bool {
        self.store_runtime_state.data_read_ahead_enable()
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
        &self.commit_log
    }

    fn get_commit_log_mut(&mut self) -> &mut CommitLog {
        &mut self.commit_log
    }

    fn set_commitlog_read_mode(&self, read_ahead_mode: crate::capability::CommitLogReadMode) -> Result<(), StoreError> {
        let data_read_ahead_enable = read_ahead_mode == crate::capability::CommitLogReadMode::Normal;
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
        self.store_runtime_state.broker_role() == BrokerRole::SyncMaster
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

    fn get_master_store_in_process<M: BackendOps + Send + Sync + 'static>(&self) -> Option<Arc<M>> {
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

    fn set_master_store_in_process<M: BackendOps + Send + Sync + 'static>(&self, master_store_in_process: Arc<M>) {
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

    fn sync_broker_role(&self, broker_role: BrokerRole) {
        let previous_role = self.store_runtime_state.broker_role();
        self.commit_log.sync_broker_role(broker_role);
        self.refresh_controller_confirm_offset_after_role_change();
        self.sync_timer_message_store_role();
        #[cfg(feature = "extended_timeline")]
        if let Err(error) = self.sync_extended_timeline_controller_role(previous_role, broker_role, 0) {
            error!("Extended Timeline role change failed closed: {error}");
        }
    }

    fn sync_broker_role_with_term(&self, broker_role: BrokerRole, external_term: u64) -> Result<bool, StoreError> {
        let previous_role = self.store_runtime_state.broker_role();
        #[cfg(feature = "extended_timeline")]
        if !self.sync_extended_timeline_controller_role(previous_role, broker_role, external_term)? {
            return Ok(false);
        }
        self.commit_log.sync_broker_role(broker_role);
        self.refresh_controller_confirm_offset_after_role_change();
        self.sync_timer_message_store_role();
        Ok(true)
    }

    fn install_controller_write_lease(&self, token: WriteLeaseToken, valid_for: Duration) -> bool {
        self.commit_log.install_controller_write_lease(token, valid_for)
    }

    fn fence_controller_writes(&self) {
        self.commit_log.fence_controller_writes();
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

        if !self.consume_queue_store.truncate_dirty_with_outcome(offset_to_truncate) {
            warn!(offset_to_truncate, "consume-queue truncation remains pending");
            return Ok(false);
        }
        if !self.commit_log.try_truncate_dirty_files(offset_to_truncate) {
            warn!(offset_to_truncate, "CommitLog truncation remains pending");
            return Ok(false);
        }
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
            .snapshot()
            .into_iter()
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
        self.commit_log.get_last_mapped_file(start_offset)
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

    fn state_machine_version_view(&self) -> StateMachineVersionView {
        StateMachineVersionView::from_shared(Arc::clone(&self.state_machine_version))
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
            .map(|logic_queue| logic_queue.read().estimate_message_count(from, to, filter))
            .unwrap_or(0)
    }

    fn recover_topic_queue_table(&mut self) {
        let min_phy_offset = self.commit_log.get_min_offset();
        self.consume_queue_store.recover_offset_table(min_phy_offset);
    }

    fn notify_message_arrive_if_necessary(&self, dispatch_request: &mut DispatchRequest) {
        if self.broker_config.long_polling_enable {
            if let Some(message_arriving_listener) = self.message_arrival.snapshot() {
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
#[cfg(test)]
#[path = "../../tests/message_store/local_file_message_store/unit.rs"]
mod tests;
