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

mod append_sequencer;
mod context;
mod handles;

pub(crate) use context::CommitLogStoreContext;
pub(crate) use handles::CommitLogCleanupHandle;
pub(crate) use handles::CommitLogInternalMessageWriteHandle;
pub(crate) use handles::CommitLogReadHandle;
pub(crate) use handles::CommitLogReplicaHandle;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use crate::config::store_runtime_config::StoreRuntimeConfig;
use bytes::Buf;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use parking_lot::Mutex as ParkingMutex;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::attribute::cq_type::CQType;
use rocketmq_model::common::broker::broker_role::BrokerRole;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::message_batch::MessageExtBatch;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::message_single::tags_string2tags_code;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::message::MessageVersion;
use rocketmq_model::common::mix_all;
use rocketmq_model::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_model::common::topic::TopicValidator;
use rocketmq_model::utils::crc32_utils::crc32;
use rocketmq_model::utils::crc32_utils::crc32_bytes;
use rocketmq_model::utils::queue_type_utils::QueueTypeUtils;
use rocketmq_protocol::common::message::message_decoder::cheetah_from_utf8_lossy;
use rocketmq_protocol::common::message::message_decoder::string_to_message_properties;
use rocketmq_runtime::common::system_clock::SystemClock;
use rocketmq_runtime::resource_budget::QueueSnapshot;
use tracing::error;
use tracing::info;
use tracing::warn;
use tracing::Instrument;

use crate::base::allocate_mapped_file_service::AllocateMappedFileService;
use crate::base::append_message_callback::DefaultAppendMessageCallback;
use crate::base::backend_ops::StoreHealthRecorder;
use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::base::flush_manager::FlushManager;
use crate::base::memory_lock_manager::MemoryLockHandle;
use crate::base::memory_lock_manager::MemoryLockManager;
use crate::base::message_encoder_pool;
use crate::base::message_result::AppendMessageResult;
use crate::base::message_result::PutMessageResult;
use crate::base::message_status_enum::PutMessageStatus;
use crate::base::put_message_context::PutMessageContext;
use crate::base::select_result::SelectMappedBufferResult;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::base::swappable::Swappable;
use crate::capability::CommitLogReadMode;
use crate::config::flush_disk_type::FlushDiskType;
use crate::config::message_store_config::LinuxMemoryLockMode;
use crate::config::message_store_config::LinuxRecoveryFadviseMode;
use crate::config::message_store_config::MessageStoreConfig;
use crate::consume_queue::mapped_file_queue::MappedFileIoStats;
use crate::consume_queue::mapped_file_queue::MappedFileQueue;
use crate::consume_queue::mapped_file_queue::MappedFileQueueAppendHandle;
use crate::consume_queue::mapped_file_queue::MappedFileWarmupStats;
use crate::ha::general_ha_service::GeneralHAService;
use crate::ha::ha_service::HAService;
use crate::log_file::cold_data_check_service::ColdDataCheckService;
use crate::log_file::commit_log_path_set::CommitLogPathSet;
use rocketmq_store_local::mapped_file::ManagedLifecycleRuntime;
use rocketmq_store_local::mapped_file::ManagedMappedFileQueueGeneration;
// Import the optimized loader module
use crate::base::flush_manager::SyncFlushRuntimeInfo;
use crate::log_file::commit_log_loader::CommitLogLoader;
use crate::log_file::commit_log_loader::LoadStatistics;
use crate::log_file::commit_log_loader::RecoveryFilePrefetch;
use crate::log_file::commit_log_loader::RecoveryMmapAdvice;
use crate::log_file::flush_manager_impl::default_flush_manager::CommitLogFlushWakeup;
use crate::log_file::flush_manager_impl::default_flush_manager::DefaultFlushManager;
use crate::log_file::group_commit_request::GroupCommitRequest;
use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::default_mapped_file_impl::LazyMmapStats;
use crate::log_file::mapped_file::MappedFile;
use crate::message_store::local_file_message_store::CommitLogDispatchHandle;
use crate::message_store::runtime_state::StoreRuntimeState;
use crate::queue::consume_queue_store::ConsumeQueueStoreTrait;
use crate::queue::local_file_consume_queue_store::ConsumeQueueStore;
use crate::store_error::StoreError;
use crate::store_error::StoreOperation;
use crate::transfer::error::TransferResult;
use crate::transfer::segment::SegmentLease;
use crate::utils::ffi::MemoryAdvice;

use rocketmq_store_local::commit_log::abnormal_recovery::AbnormalRecoveryObservation;
use rocketmq_store_local::commit_log::abnormal_recovery::AbnormalRecoveryRecord;
use rocketmq_store_local::commit_log::abnormal_recovery::AbnormalRecoverySegmentOutcome;
use rocketmq_store_local::commit_log::append::micro_batch::MicroBatchPolicy;
use rocketmq_store_local::commit_log::append::sequencer::AppendSequencerConfig;
use rocketmq_store_local::commit_log::append_attempt::CommitLogAppendStatus;
use rocketmq_store_local::commit_log::load_orchestration::drive_commit_log_load;
use rocketmq_store_local::commit_log::load_orchestration::parallel_commit_log_load_enabled;
use rocketmq_store_local::commit_log::load_orchestration::safe_load_requested;
use rocketmq_store_local::commit_log::load_orchestration::CommitLogLoadObservation;
use rocketmq_store_local::commit_log::load_orchestration::CommitLogLoadStep;
use rocketmq_store_local::commit_log::memory_lock::plan_commit_log_memory_lock_target;
use rocketmq_store_local::commit_log::memory_lock::CommitLogMemoryLockMode;
use rocketmq_store_local::commit_log::memory_lock::CommitLogMemoryLockTarget;
use rocketmq_store_local::commit_log::normal_recovery::NormalRecoveryObservation;
use rocketmq_store_local::commit_log::normal_recovery::NormalRecoveryRecord;
use rocketmq_store_local::commit_log::normal_recovery::NormalRecoverySegmentOutcome;
use rocketmq_store_local::commit_log::record::read_declared_frame;
use rocketmq_store_local::commit_log::record_parser::decode_commit_log_record;
use rocketmq_store_local::commit_log::record_parser::CommitLogRecordBodyMode;
use rocketmq_store_local::commit_log::record_parser::CommitLogRecordChecksum;
use rocketmq_store_local::commit_log::record_parser::CommitLogRecordErrorKind;
use rocketmq_store_local::commit_log::record_parser::CommitLogRecordOutcome;
use rocketmq_store_local::commit_log::recovery::abnormal_confirm_candidate_end;
use rocketmq_store_local::commit_log::recovery::plan_normal_recovery_file_window;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryConfirmCandidateError;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryDispatchGate;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryPolicy;
use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryState;
use rocketmq_store_local::commit_log::recovery::CommitLogRecoveryCompletion;
use rocketmq_store_local::commit_log::recovery::NormalRecoveryPolicy;
use rocketmq_store_local::commit_log::recovery::NormalRecoveryState;
use rocketmq_store_local::commit_log::root::CommitLogRoot;
use rocketmq_store_local::commit_log::runtime_state::CommitLogActiveMemoryLock;
use rocketmq_store_local::commit_log::runtime_state::CommitLogRuntimeState;

use self::append_sequencer::CommitLogAppendDependencies;
use self::append_sequencer::CommitLogAppendProcessor;
use self::append_sequencer::CommitLogAppendRuntime;
use self::handles::publish_confirm_offset;
use self::handles::resolve_commit_log_confirm_offset;

pub use rocketmq_store_local::commit_log::record::BLANK_MAGIC_CODE;
pub use rocketmq_store_local::commit_log::record::MESSAGE_MAGIC_CODE;
pub use rocketmq_store_local::commit_log::runtime_state::CommitLogPutMessageLockRuntimeInfo;

//CRC32 Format: [PROPERTY_CRC32 + NAME_VALUE_SEPARATOR + 10-digit fixed-length string +
// PROPERTY_SEPARATOR]
pub const CRC32_RESERVED_LEN: i32 = (MessageConst::PROPERTY_CRC32.len() + 1 + 10 + 1) as i32;

#[derive(Debug)]
enum NormalRecoveryAdapterError {
    FramePositionOverflow { position: usize, size: usize },
    RelativeOffsetConversion(std::num::TryFromIntError),
    MessageSizeConversion(std::num::TryFromIntError),
}

#[derive(Debug)]
enum AbnormalRecoveryAdapterError {
    ConfirmCandidate(AbnormalRecoveryConfirmCandidateError),
    ConfirmLimitConversion(std::num::TryFromIntError),
    FramePositionOverflow { position: usize, size: usize },
    RelativeOffsetConversion(std::num::TryFromIntError),
    ValidatedSizeConversion(std::num::TryFromIntError),
}

fn log_abnormal_recovery_window(
    window: &crate::log_file::commit_log_recovery::AbnormalRecoveryWindow,
    recovery_path: &str,
) {
    info!(
        "Starting abnormal recovery from file index {} ({}), checkpointIndex: {:?}, dispatchProgressIndex: {:?}, \
         confirmOffsetIndex: {:?}, fileCountLimit: {:?}, expandedFiles: {}, scannedFiles: {}, scannedBytes: {}, \
         endOffset: {:?}, fallbackReason: {:?}",
        window.start_index,
        recovery_path,
        window.checkpoint_index,
        window.dispatch_progress_index,
        window.confirm_offset_index,
        window.file_count_limit,
        window.expanded_files,
        window.scanned_file_count,
        window.scanned_bytes,
        window.end_offset,
        window.fallback_reason
    );
}

macro_rules! apply_recovery_completion {
    ($commit_log:ident, $completion:expr, $max_consume_queue_offset:expr $(,)?) => {{
        match $completion {
            CommitLogRecoveryCompletion::Empty => {
                if $commit_log.mapped_file_queue.is_managed() {
                    if $commit_log.consume_queue_store.get_total_size() == 0 {
                        $commit_log.mapped_file_queue.set_flushed_where(0);
                        $commit_log.mapped_file_queue.set_committed_where(0);
                        true
                    } else {
                        warn!(
                            "managed empty CommitLog recovery found consume-queue data that requires durable retirement"
                        );
                        false
                    }
                } else if $commit_log.consume_queue_store.destroy_with_outcome() {
                    if $commit_log.consume_queue_store.load_after_destroy() {
                        $commit_log.mapped_file_queue.set_flushed_where(0);
                        $commit_log.mapped_file_queue.set_committed_where(0);
                        true
                    } else {
                        warn!(
                            "consume-queue reload failed after empty CommitLog recovery; recovery cannot continue"
                        );
                        false
                    }
                } else {
                    warn!(
                        "consume-queue cleanup remains pending after empty CommitLog recovery; retaining queue identities and progress for retry"
                    );
                    false
                }
            }
            CommitLogRecoveryCompletion::Recovered {
                confirm_offset,
                controller_confirm_offset,
                process_offset,
                truncate_consume_queue,
            } => {
                let consume_queue_truncated = if truncate_consume_queue {
                    warn!(
                        "maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files",
                        $max_consume_queue_offset, process_offset
                    );
                    $commit_log
                        .consume_queue_store
                        .truncate_dirty_with_outcome(process_offset)
                } else {
                    true
                };

                if !consume_queue_truncated {
                    warn!(
                        process_offset,
                        "consume-queue truncation remains pending after CommitLog recovery; recovery cannot continue"
                    );
                    false
                } else if $commit_log.mapped_file_queue.try_truncate_dirty_files(process_offset) {
                    if $commit_log.broker_config.enable_controller_mode {
                        $commit_log.clamp_controller_recover_confirm_offset(
                            $commit_log.get_min_offset(),
                            controller_confirm_offset,
                        );
                    } else {
                        $commit_log.set_confirm_offset(confirm_offset);
                    }
                    $commit_log.mapped_file_queue.set_flushed_where(process_offset);
                    $commit_log.mapped_file_queue.set_committed_where(process_offset);
                    true
                } else {
                    warn!(
                        process_offset,
                        "CommitLog tail cleanup remains pending after recovery; recovery cannot continue"
                    );
                    false
                }
            }
        }
    }};
}

// This reduces heap allocations by ~50% by reusing encoder instances
fn encode_message_ext(
    message_ext: &MessageExtBrokerInner,
    message_store_config: &Arc<MessageStoreConfig>,
) -> (Option<PutMessageResult>, BytesMut) {
    message_encoder_pool::encode_message_with_pool(message_ext, message_store_config)
}

fn parse_property_crc(properties_map: &HashMap<CheetahString, CheetahString>) -> Result<Option<u32>, ()> {
    let Some(crc_32) = properties_map.get(MessageConst::PROPERTY_CRC32) else {
        return Ok(None);
    };

    let mut expected_crc = 0u32;
    for ch in crc_32.chars().rev() {
        if !ch.is_ascii_digit() {
            return Err(());
        }

        expected_crc = expected_crc
            .checked_mul(10)
            .and_then(|value| value.checked_add((ch as u8 - b'0') as u32))
            .ok_or(())?;
    }

    Ok(Some(expected_crc))
}

pub fn get_cq_type(
    topic_config_table: &Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
    msg_inner: &MessageExtBrokerInner,
) -> CQType {
    let binding = topic_config_table.get(msg_inner.topic());
    QueueTypeUtils::get_cq_type_arc_mut(binding.as_deref())
}

pub fn get_message_num(
    topic_config_table: &Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
    msg_inner: &MessageExtBrokerInner,
) -> i16 {
    let mut message_num = 1i16;
    let cq_type = get_cq_type(topic_config_table, msg_inner);
    if MessageSysFlag::check(msg_inner.sys_flag(), MessageSysFlag::INNER_BATCH_FLAG) || cq_type == CQType::BatchCQ {
        if let Some(num) = msg_inner
            .message_ext_inner
            .message
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_INNER_NUM))
        {
            message_num = num.parse().unwrap_or(1i16);
        }
    }
    // message_num
    message_num
}

macro_rules! lock_active_mapped_file_parts {
    (
        $active_memory_lock:expr,
        $active_memory_lock_present:expr,
        $mapped_file:expr,
        $target:expr,
        $locker:expr,
        $unlocker:expr $(,)?
    ) => {{
        let mapped_file = $mapped_file;
        let file_from_offset = mapped_file.get_file_from_offset();
        CommitLog::ensure_active_mapped_file_locked_parts(
            $active_memory_lock,
            $active_memory_lock_present,
            $target,
            file_from_offset,
            |manager, target| {
                mapped_file.lock_region_with(manager, target.category, target.offset, target.len, $locker)
            },
            |manager, handle| manager.unlock_owned_region_with(handle, $unlocker),
        )
    }};
}

pub struct CommitLog {
    root: CommitLogRoot<CommitLogAdapter>,
}

mod adapter {
    /// Store-owned composition dependencies used by the legacy CommitLog facade.
    #[doc(hidden)]
    pub struct CommitLog {
        pub(super) mapped_file_queue: super::MappedFileQueue,
        pub(super) message_store_config: super::Arc<super::MessageStoreConfig>,
        pub(super) store_runtime_state: super::Arc<super::StoreRuntimeState>,
        pub(super) broker_config: super::Arc<super::StoreRuntimeConfig>,
        pub(super) enabled_append_prop_crc: bool,
        pub(super) store_context: super::CommitLogStoreContext,
        pub(super) dispatcher: super::CommitLogDispatchHandle,
        pub(super) runtime_state: super::Arc<super::CommitLogRuntimeState>,
        pub(super) store_checkpoint: super::Arc<super::StoreCheckpoint>,
        pub(super) append_runtime: super::CommitLogAppendRuntime,
        pub(super) put_message_lock: super::Arc<tokio::sync::Mutex<()>>,
        pub(super) consume_queue_store: super::ConsumeQueueStore,
        pub(super) flush_manager: super::DefaultFlushManager,
        pub(super) cold_data_check_service: super::Arc<super::ColdDataCheckService>,
        pub(super) telemetry_handle: rocketmq_observability::TelemetryHandle,
        pub(super) store_metrics: rocketmq_observability::metrics::store::StoreMetricsRecorder,
    }
}

#[doc(hidden)]
pub use adapter::CommitLog as CommitLogAdapter;

impl Deref for CommitLog {
    type Target = CommitLogAdapter;

    fn deref(&self) -> &Self::Target {
        self.root.adapter()
    }
}

impl DerefMut for CommitLog {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.root.adapter_mut()
    }
}

impl CommitLog {
    /// Installs the reconciled managed CommitLog generation before Store workers start.
    pub(crate) fn install_reconciled_generation(
        &self,
        generation: ManagedMappedFileQueueGeneration<DefaultMappedFile>,
        runtime: ManagedLifecycleRuntime,
    ) -> bool {
        self.mapped_file_queue.install_reconciled_generation(generation)
            && self.mapped_file_queue.bind_managed_runtime(runtime)
    }

    pub(crate) fn try_new(
        runtime_scope: crate::runtime::StoreRuntimeScope,
        message_store_config: Arc<MessageStoreConfig>,
        store_runtime_state: Arc<StoreRuntimeState>,
        broker_config: Arc<StoreRuntimeConfig>,
        store_context: CommitLogStoreContext,
        dispatcher: CommitLogDispatchHandle,
        store_checkpoint: Arc<StoreCheckpoint>,
        topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
        consume_queue_store: ConsumeQueueStore,
        allocate_mapped_file_service: AllocateMappedFileService,
        telemetry_handle: rocketmq_observability::TelemetryHandle,
        store_metrics: rocketmq_observability::metrics::store::StoreMetricsRecorder,
    ) -> Result<Self, StoreError> {
        let enabled_append_prop_crc = message_store_config.enabled_append_prop_crc;
        let mapped_file_size = message_store_config.mapped_file_size_commit_log;
        let memory_lock_budget_bytes = message_store_config.effective_linux_memory_lock_budget_bytes(
            crate::platform::current_store_platform_capability().memory_lock_limit_bytes,
        );
        let writable_paths = message_store_config.commit_log_writable_paths();
        let readonly_paths = message_store_config.commit_log_readonly_paths();
        let mapped_file_queue = if message_store_config.enable_mapped_file_lifecycle_wave_b {
            if writable_paths.len() != 1 || !readonly_paths.is_empty() {
                return Err(StoreError::storage(
                    StoreOperation::Start,
                    "managed mapped-file lifecycle does not support multipath CommitLog roots",
                ));
            }
            MappedFileQueue::new(
                writable_paths[0].to_string_lossy().into_owned(),
                mapped_file_size as u64,
                Some(allocate_mapped_file_service),
            )
        } else {
            let commit_log_paths = CommitLogPathSet::try_new(writable_paths, readonly_paths, mapped_file_size as u64)
                .map_err(|error| {
                StoreError::storage(StoreOperation::Start, "invalid CommitLog path set").with_source(error)
            })?;
            MappedFileQueue::new_commit_log(
                Arc::new(commit_log_paths),
                mapped_file_size as u64,
                Some(allocate_mapped_file_service),
            )
        };
        let mapped_file_flush = mapped_file_queue.flush_handle();
        #[cfg(feature = "observability")]
        let runtime_state = Arc::new(CommitLogRuntimeState::new_with_store_metrics(
            message_store_config.linux_memory_lock_warn_only,
            memory_lock_budget_bytes,
            store_metrics.clone(),
        ));
        #[cfg(not(feature = "observability"))]
        let runtime_state = Arc::new(CommitLogRuntimeState::new(
            message_store_config.linux_memory_lock_warn_only,
            memory_lock_budget_bytes,
        ));
        let append_message_callback = Arc::new(DefaultAppendMessageCallback::new(
            message_store_config.clone(),
            topic_config_table.clone(),
        ));
        let put_message_lock = Arc::new(tokio::sync::Mutex::new(()));
        let flush_manager = DefaultFlushManager::new(
            runtime_scope.clone(),
            message_store_config.clone(),
            mapped_file_flush,
            store_checkpoint.clone(),
        );
        let micro_batch = if message_store_config.commit_log_micro_batch_enabled {
            MicroBatchPolicy::try_new(
                message_store_config.commit_log_micro_batch_max_items,
                message_store_config.commit_log_micro_batch_max_bytes,
                std::time::Duration::from_micros(message_store_config.commit_log_micro_batch_max_wait_micros),
            )
        } else {
            MicroBatchPolicy::disabled(message_store_config.commit_log_append_queue_bytes)
        }
        .map_err(|error| {
            StoreError::storage(
                StoreOperation::Start,
                format!("invalid CommitLog micro-batch policy: {error}"),
            )
        })?;
        let append_processor = CommitLogAppendProcessor::new(CommitLogAppendDependencies {
            append: mapped_file_queue.append_handle(),
            message_store_config: Arc::clone(&message_store_config),
            store_context: store_context.clone(),
            runtime_state: Arc::clone(&runtime_state),
            append_callback: Arc::clone(&append_message_callback),
            put_message_lock: Arc::clone(&put_message_lock),
            consume_queue_store: consume_queue_store.clone(),
            flush: flush_manager.commit_log_flush_wakeup(),
            store_metrics: store_metrics.clone(),
        });
        let append_runtime = CommitLogAppendRuntime::start(
            runtime_scope,
            AppendSequencerConfig {
                queue_capacity: message_store_config.commit_log_append_queue_capacity,
                queue_bytes: message_store_config.commit_log_append_queue_bytes,
                micro_batch,
            },
            append_processor,
        )
        .map_err(|error| {
            StoreError::storage(
                StoreOperation::Start,
                format!("failed to initialize CommitLog append sequencer: {error}"),
            )
        })?;
        Ok(Self {
            root: CommitLogRoot::new(CommitLogAdapter {
                mapped_file_queue,
                message_store_config: message_store_config.clone(),
                store_runtime_state,
                broker_config,
                enabled_append_prop_crc,
                store_context,
                dispatcher,
                runtime_state,
                store_checkpoint: store_checkpoint.clone(),
                append_runtime,
                put_message_lock,
                consume_queue_store,
                flush_manager,
                cold_data_check_service: Arc::new(Default::default()),
                telemetry_handle,
                store_metrics,
            }),
        })
    }

    #[inline]
    pub(crate) fn cleanup_handle(&self) -> CommitLogCleanupHandle {
        CommitLogCleanupHandle {
            mapped_file_queue: self.mapped_file_queue.cleanup_handle(),
        }
    }

    #[inline]
    pub(crate) fn read_handle(&self) -> CommitLogReadHandle {
        CommitLogReadHandle {
            mapped_file_queue: self.mapped_file_queue.read_handle(),
            message_store_config: self.message_store_config.clone(),
            store_runtime_state: Arc::clone(&self.store_runtime_state),
            broker_config: self.broker_config.clone(),
            store_context: self.store_context.clone(),
            runtime_state: self.runtime_state.clone(),
        }
    }

    pub(crate) fn release_checkpoint_write_lock(&self) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(&self.put_message_lock)
    }

    pub(crate) fn release_checkpoint_flush_handle(
        &self,
    ) -> crate::consume_queue::mapped_file_queue::MappedFileQueueFlushHandle {
        self.mapped_file_queue.flush_handle()
    }

    pub(crate) fn internal_message_write_handle(&self) -> CommitLogInternalMessageWriteHandle {
        CommitLogInternalMessageWriteHandle {
            message_store_config: Arc::clone(&self.message_store_config),
            store_runtime_state: Arc::clone(&self.store_runtime_state),
            enabled_append_prop_crc: self.enabled_append_prop_crc,
            append_port: self.append_runtime.port(),
            telemetry_handle: self.telemetry_handle.clone(),
        }
    }

    #[inline]
    pub(crate) fn replica_handle(&self) -> CommitLogReplicaHandle {
        CommitLogReplicaHandle {
            read: self.read_handle(),
            append: self.mapped_file_queue.append_handle(),
            put_message_lock: self.put_message_lock.clone(),
            runtime_state: self.runtime_state.clone(),
            store_checkpoint: self.store_checkpoint.clone(),
        }
    }

    pub(crate) fn set_store_health_recorder(&mut self, store_health_recorder: StoreHealthRecorder) {
        self.flush_manager.set_store_health_recorder(store_health_recorder);
    }

    pub(crate) fn publish_ha_service(&self, ha_service: GeneralHAService) {
        self.store_context.publish_ha_service(ha_service);
    }

    #[cfg(test)]
    pub(crate) fn has_allocate_mapped_file_service(&self) -> bool {
        self.mapped_file_queue.allocate_mapped_file_service.is_some()
    }

    fn active_memory_lock_target(&self, mapped_file: &DefaultMappedFile) -> Option<CommitLogMemoryLockTarget> {
        Self::active_memory_lock_target_for_config(
            self.message_store_config.as_ref(),
            mapped_file.get_wrote_position().max(0) as u64,
            mapped_file.get_file_size(),
        )
    }

    fn active_memory_lock_target_for_config(
        message_store_config: &MessageStoreConfig,
        wrote_position: u64,
        file_size: u64,
    ) -> Option<CommitLogMemoryLockTarget> {
        let mode = match message_store_config.effective_linux_memory_lock_mode() {
            LinuxMemoryLockMode::Off => CommitLogMemoryLockMode::Off,
            LinuxMemoryLockMode::ActiveWindow => CommitLogMemoryLockMode::ActiveWindow,
            LinuxMemoryLockMode::ActiveFile => CommitLogMemoryLockMode::ActiveFile,
        };
        plan_commit_log_memory_lock_target(
            mode,
            message_store_config.linux_memory_lock_active_window_bytes,
            wrote_position,
            file_size,
        )
    }

    fn ensure_active_mapped_file_locked(&self, mapped_file: &DefaultMappedFile) -> RocketMQResult<()> {
        let target = self.active_memory_lock_target(mapped_file);
        let (active_memory_lock, active_memory_lock_present) = self.runtime_state.active_memory_lock_parts();
        Self::ensure_active_mapped_file_locked_parts(
            active_memory_lock,
            active_memory_lock_present,
            target,
            mapped_file.get_file_from_offset(),
            |manager, target| mapped_file.lock_region(manager, target.category, target.offset, target.len),
            MemoryLockManager::unlock_region,
        )
    }

    fn ensure_active_mapped_file_locked_with<F, G>(
        &self,
        mapped_file: &DefaultMappedFile,
        mut locker: F,
        mut unlocker: G,
    ) -> RocketMQResult<()>
    where
        F: FnMut(&[u8]) -> RocketMQResult<()>,
        G: FnMut(*const u8, usize) -> RocketMQResult<()>,
    {
        let target = self.active_memory_lock_target(mapped_file);
        let (active_memory_lock, active_memory_lock_present) = self.runtime_state.active_memory_lock_parts();
        lock_active_mapped_file_parts!(
            active_memory_lock,
            active_memory_lock_present,
            mapped_file,
            target,
            &mut locker,
            &mut unlocker,
        )
    }

    fn ensure_active_mapped_file_locked_parts<L, U>(
        active_memory_lock: &ParkingMutex<CommitLogActiveMemoryLock>,
        active_memory_lock_present: &AtomicBool,
        target: Option<CommitLogMemoryLockTarget>,
        file_from_offset: u64,
        mut lock_region: L,
        mut unlock_region: U,
    ) -> RocketMQResult<()>
    where
        L: FnMut(&MemoryLockManager, CommitLogMemoryLockTarget) -> RocketMQResult<Option<MemoryLockHandle>>,
        U: FnMut(&MemoryLockManager, &mut MemoryLockHandle) -> RocketMQResult<()>,
    {
        let Some(target) = target else {
            return Self::release_active_memory_lock_if_present_parts(
                active_memory_lock,
                active_memory_lock_present,
                &mut unlock_region,
            );
        };
        let mut active_memory_lock_guard = active_memory_lock.lock();
        if active_memory_lock_guard.is_current(file_from_offset, target) {
            active_memory_lock_present.store(true, Ordering::Release);
            return Ok(());
        }

        Self::release_active_memory_lock_locked(&mut active_memory_lock_guard, &mut unlock_region)?;
        active_memory_lock_present.store(false, Ordering::Release);
        if let Some(handle) = lock_region(active_memory_lock_guard.manager(), target)? {
            active_memory_lock_guard.set_current(file_from_offset, target, handle);
            active_memory_lock_present.store(true, Ordering::Release);
        } else {
            active_memory_lock_present.store(false, Ordering::Release);
        }
        Ok(())
    }

    fn release_active_memory_lock_if_present<G>(&self, unlocker: G) -> RocketMQResult<()>
    where
        G: FnMut(*const u8, usize) -> RocketMQResult<()>,
    {
        let (active_memory_lock, active_memory_lock_present) = self.runtime_state.active_memory_lock_parts();
        let mut unlocker = unlocker;
        Self::release_active_memory_lock_if_present_parts(
            active_memory_lock,
            active_memory_lock_present,
            |manager, handle| manager.unlock_owned_region_with(handle, &mut unlocker),
        )
    }

    fn release_active_memory_lock(&self) -> RocketMQResult<()> {
        self.release_active_memory_lock_with(MemoryLockManager::unlock_region)
    }

    fn release_active_memory_lock_with<U>(&self, unlock_region: U) -> RocketMQResult<()>
    where
        U: FnMut(&MemoryLockManager, &mut MemoryLockHandle) -> RocketMQResult<()>,
    {
        let (active_memory_lock, active_memory_lock_present) = self.runtime_state.active_memory_lock_parts();
        Self::release_active_memory_lock_if_present_parts(active_memory_lock, active_memory_lock_present, unlock_region)
    }

    fn release_active_memory_lock_if_present_parts<U>(
        active_memory_lock: &ParkingMutex<CommitLogActiveMemoryLock>,
        active_memory_lock_present: &AtomicBool,
        unlock_region: U,
    ) -> RocketMQResult<()>
    where
        U: FnMut(&MemoryLockManager, &mut MemoryLockHandle) -> RocketMQResult<()>,
    {
        if !active_memory_lock_present.load(Ordering::Acquire) {
            return Ok(());
        }

        Self::release_active_memory_lock_parts(active_memory_lock, active_memory_lock_present, unlock_region)
    }

    fn release_active_memory_lock_parts<U>(
        active_memory_lock: &ParkingMutex<CommitLogActiveMemoryLock>,
        active_memory_lock_present: &AtomicBool,
        mut unlock_region: U,
    ) -> RocketMQResult<()>
    where
        U: FnMut(&MemoryLockManager, &mut MemoryLockHandle) -> RocketMQResult<()>,
    {
        let mut active_memory_lock = active_memory_lock.lock();
        Self::release_active_memory_lock_locked(&mut active_memory_lock, &mut unlock_region)?;
        active_memory_lock_present.store(false, Ordering::Release);
        Ok(())
    }

    fn release_active_memory_lock_locked<U>(
        active_memory_lock: &mut CommitLogActiveMemoryLock,
        mut unlock_region: U,
    ) -> RocketMQResult<()>
    where
        U: FnMut(&MemoryLockManager, &mut MemoryLockHandle) -> RocketMQResult<()>,
    {
        active_memory_lock.unlock_current_with(&mut unlock_region)?;
        Ok(())
    }

    fn release_active_memory_lock_for_drop(&self) {
        self.release_active_memory_lock_for_drop_with(MemoryLockManager::unlock_region);
    }

    fn release_active_memory_lock_for_drop_with<U>(&self, unlock_region: U)
    where
        U: FnMut(&MemoryLockManager, &mut MemoryLockHandle) -> RocketMQResult<()>,
    {
        if let Err(error) = self.release_active_memory_lock_with(unlock_region) {
            warn!(
                %error,
                "Failed to unlock the active CommitLog memory region during drop; the owned mapping will remain live until runtime state teardown"
            );
        }
    }
}

impl Drop for CommitLog {
    fn drop(&mut self) {
        self.append_runtime.close();
        self.release_active_memory_lock_for_drop();
    }
}

#[allow(unused_variables)]
impl CommitLog {
    /// Load CommitLog files with optimized parallel I/O strategy.
    ///
    /// This implementation provides significant performance improvements over the original:
    /// - **Parallel metadata collection**: Reduces I/O latency on multi-file scenarios
    /// - **Batched validation**: Validates all files before mmap creation
    /// - **Memory hints**: Applies platform-specific optimizations (madvise, etc.)
    /// - **Zero-copy reuse**: Minimizes allocations during load
    ///
    /// # Feature Flag
    /// The `fast-load` cargo feature enables parallel loading (enabled by default).
    /// To disable and use safe sequential loading, compile with `--no-default-features`.
    ///
    /// # Behavior Equivalence
    /// This implementation maintains exact semantic equivalence with the original:
    /// - File ordering is preserved (sorted by filename)
    /// - Size validation is identical
    /// - Empty last file removal logic is preserved
    /// - All positions (wrote/flushed/committed) are set identically
    ///
    /// # Returns
    /// `true` if load succeeded, `false` otherwise
    pub fn load(&mut self) -> bool {
        let safe_load_value = std::env::var("ROCKETMQ_SAFE_LOAD").ok();
        let force_sequential = safe_load_requested(safe_load_value.as_deref());
        drive_commit_log_load(
            force_sequential,
            |step| match step {
                CommitLogLoadStep::Optimized => self.load_optimized(),
                CommitLogLoadStep::Sequential => Ok(self.load_sequential()),
            },
            |observation| match observation {
                CommitLogLoadObservation::ForcedSequential => {
                    info!("Using safe sequential CommitLog load (ROCKETMQ_SAFE_LOAD=true)");
                }
                CommitLogLoadObservation::OptimizedLoaded => {
                    info!("load commit log Ok (optimized)");
                }
                CommitLogLoadObservation::OptimizedRejected => {
                    error!("load commit log failed (optimized)");
                }
                CommitLogLoadObservation::OptimizedFailed(error) => {
                    error!("Optimized load failed: {}, falling back to sequential load", error);
                }
                CommitLogLoadObservation::SequentialFailed(error) => {
                    error!("Sequential CommitLog load adapter failed: {}", error);
                }
            },
        )
    }

    /// Optimized load implementation with parallel I/O and batching.
    ///
    /// # Performance Characteristics
    /// - **Parallel metadata**: ~70% faster on 10+ files (SSD)
    /// - **Reduced syscalls**: Batch validation reduces overhead
    /// - **Memory efficient**: Pre-allocated vectors, minimal copies
    ///
    /// # Errors
    /// Returns `Err` if:
    /// - Directory access fails
    /// - File size validation fails
    /// - mmap creation fails
    fn load_optimized(&mut self) -> Result<bool, std::io::Error> {
        if self.mapped_file_queue.is_multipath_commit_log() {
            return Ok(self.load_sequential());
        }
        let store_path = self.message_store_config.get_store_path_commit_log();
        let mapped_file_size = self.message_store_config.mapped_file_size_commit_log as u64;

        let enable_parallel = parallel_commit_log_load_enabled();

        let recovery_mmap_advice = self.recovery_mmap_advice();
        let recovery_file_prefetch = self.recovery_file_prefetch();
        let loader = CommitLogLoader::new_with_recovery_hints(
            store_path,
            mapped_file_size,
            enable_parallel,
            recovery_mmap_advice,
            recovery_file_prefetch,
        )
        .with_lazy_mmap(self.effective_lazy_mmap_enable());

        match loader.load_optimized() {
            Ok((mapped_files, stats)) => {
                self.runtime_state.set_load_statistics(stats.clone());
                self.mapped_file_queue.replace_mapped_files_exclusive(mapped_files);

                // Log detailed statistics
                info!(
                    "CommitLog loaded: {} files, {:.2} MB total, parallel phase: {}ms, total: {}ms",
                    stats.total_files,
                    stats.total_size_bytes as f64 / 1024.0 / 1024.0,
                    stats.parallel_load_time_ms,
                    stats.total_load_time_ms
                );
                self.mapped_file_queue.check_self();

                Ok(true)
            }
            Err(e) => Err(e),
        }
    }

    /// Fallback: Original sequential load implementation.
    ///
    /// This method preserves the exact original behavior for compatibility
    /// and serves as a fallback if optimized loading encounters errors.
    fn load_sequential(&mut self) -> bool {
        let result = self.mapped_file_queue.load();
        self.mapped_file_queue.check_self();
        if result {
            info!("load commit log Ok (sequential fallback)");
        } else {
            error!("load commit log failed (sequential fallback)");
        }
        result
    }

    pub fn start(&mut self) {
        self.flush_manager.start();
    }

    pub fn shutdown(&mut self) {
        self.shutdown_with(MemoryLockManager::unlock_region);
    }

    fn shutdown_with<U>(&mut self, mut unlock_region: U)
    where
        U: FnMut(&MemoryLockManager, &mut MemoryLockHandle) -> RocketMQResult<()>,
    {
        self.append_runtime.close();
        if let Err(error) = self.release_active_memory_lock_with(&mut unlock_region) {
            warn!(
                %error,
                "Failed to unlock the active CommitLog memory region during shutdown; retaining it for retry"
            );
        }
        self.flush_manager.shutdown();
    }

    pub fn sync_flush_runtime_info(&self) -> SyncFlushRuntimeInfo {
        self.flush_manager.sync_flush_runtime_info()
    }

    pub fn put_message_lock_runtime_info(&self) -> CommitLogPutMessageLockRuntimeInfo {
        self.runtime_state.put_message_lock_runtime_info()
    }

    /// Returns bounded admission and saturation state for the CommitLog append sequencer.
    pub fn append_sequencer_runtime_info(&self) -> QueueSnapshot {
        self.append_runtime.port().snapshot()
    }

    pub async fn shutdown_gracefully(
        &mut self,
    ) -> Result<crate::consume_queue::mapped_file_queue::FlushProgress, StoreError> {
        self.shutdown_gracefully_with(MemoryLockManager::unlock_region).await
    }

    async fn shutdown_gracefully_with<U>(
        &mut self,
        mut unlock_region: U,
    ) -> Result<crate::consume_queue::mapped_file_queue::FlushProgress, StoreError>
    where
        U: FnMut(&MemoryLockManager, &mut MemoryLockHandle) -> RocketMQResult<()>,
    {
        self.append_runtime.shutdown_gracefully().await;
        let unlock_result = self.release_active_memory_lock_with(&mut unlock_region);
        let flush_result = self.flush_manager.shutdown_gracefully().await;

        match (unlock_result, flush_result) {
            (Ok(()), flush_result) => flush_result,
            (Err(unlock_error), Err(flush_error)) => {
                warn!(
                    error = %unlock_error,
                    "Failed to unlock the active CommitLog memory region during graceful shutdown; retaining it for retry while preserving the flush failure"
                );
                Err(flush_error)
            }
            (Err(unlock_error), Ok(_)) => Err(StoreError::mapped_file(StoreOperation::Shutdown, unlock_error)),
        }
    }

    pub fn destroy(&mut self) {
        if !self.destroy_with_outcome() {
            warn!("CommitLog mapped-file cleanup remains pending; retaining queue progress for retry");
        }
    }

    #[must_use]
    pub fn destroy_with_outcome(&mut self) -> bool {
        self.destroy_with(MemoryLockManager::unlock_region)
    }

    fn destroy_with<U>(&mut self, mut unlock_region: U) -> bool
    where
        U: FnMut(&MemoryLockManager, &mut MemoryLockHandle) -> RocketMQResult<()>,
    {
        self.shutdown_with(&mut unlock_region);
        if let Err(error) = self.release_active_memory_lock_with(&mut unlock_region) {
            warn!(
                %error,
                "Failed to unlock the active CommitLog memory region before destroying the mapped-file queue; retaining its mapping owner for the drop fallback"
            );
        }
        let destroyed = self.mapped_file_queue.destroy_with_outcome();
        if destroyed {
            self.mapped_file_queue.set_committed_where(0);
        }
        destroyed
    }

    pub fn get_message(&self, offset: i64, size: i32) -> Option<SelectMappedBufferResult> {
        let mapped_file_size = self.message_store_config.mapped_file_size_commit_log;
        let mapped_file = self.mapped_file_queue.find_mapped_file_by_offset(offset, offset == 0);
        match mapped_file {
            None => None,
            Some(mmap_file) => {
                let pos = offset % mapped_file_size as i64;
                let mut select_mapped_buffer_result = mmap_file.select_mapped_buffer(pos as i32, size);
                if let Some(ref mut result) = select_mapped_buffer_result {
                    result.try_attach_mapped_file(mmap_file);
                }
                select_mapped_buffer_result
            }
        }
    }

    pub(crate) fn get_message_for_transfer(&self, offset: i64, size: i32) -> Option<SelectMappedBufferResult> {
        self.read_handle().get_message_for_transfer(offset, size)
    }

    pub fn set_confirm_offset(&mut self, phy_offset: i64) {
        self.publish_confirm_offset(phy_offset);
    }

    pub(crate) fn publish_confirm_offset(&self, phy_offset: i64) {
        publish_confirm_offset(&self.runtime_state, &self.store_checkpoint, phy_offset);
    }

    fn clamp_controller_recover_confirm_offset(&mut self, min_phy_offset: i64, upper_bound: i64) {
        let upper_bound = upper_bound.max(min_phy_offset);
        let confirm_offset = self.get_confirm_offset();

        if confirm_offset < min_phy_offset {
            error!(
                "confirmOffset {} is less than minPhyOffset {}, correct confirmOffset to minPhyOffset",
                confirm_offset, min_phy_offset
            );
            self.set_confirm_offset(min_phy_offset);
        } else if confirm_offset > upper_bound {
            error!(
                "confirmOffset {} is larger than recovery upper bound {}, correct confirmOffset to upper bound",
                confirm_offset, upper_bound
            );
            self.set_confirm_offset(upper_bound);
        } else {
            self.set_confirm_offset(confirm_offset);
        }
    }

    /// Handle HA service validation and calculate need_ack_nums
    /// Returns (need_ack_nums, should_continue) where should_continue indicates if processing can
    /// continue
    fn handle_ha_service(&self, curr_offset: u64, need_handle_ha: bool) -> Result<i32, PutMessageResult> {
        let mut need_ack_nums = self.message_store_config.in_sync_replicas;

        if !need_handle_ha {
            return Ok(need_ack_nums);
        }

        let ha_service = self.store_context.ha_service().ok_or_else(|| {
            error!("HA Service is None");
            PutMessageResult::new_default(PutMessageStatus::UnknownError)
        })?;

        if self.broker_config.enable_controller_mode {
            if ha_service.in_sync_replicas_nums(curr_offset as i64)
                < self.message_store_config.min_in_sync_replicas as i32
            {
                return Err(PutMessageResult::new_default(PutMessageStatus::InSyncReplicasNotEnough));
            }
            if self.message_store_config.all_ack_in_sync_state_set {
                need_ack_nums = mix_all::ALL_ACK_IN_SYNC_STATE_SET;
            }
        } else if self.broker_config.enable_slave_acting_master {
            let in_sync_replicas = self
                .store_context
                .alive_replica_num_in_group
                .load(Ordering::SeqCst)
                .min(ha_service.in_sync_replicas_nums(curr_offset as i64));
            need_ack_nums = self.calc_need_ack_nums(in_sync_replicas);
            if need_ack_nums > in_sync_replicas {
                return Err(PutMessageResult::new_default(PutMessageStatus::InSyncReplicasNotEnough));
            }
            if self.message_store_config.all_ack_in_sync_state_set {
                need_ack_nums = mix_all::ALL_ACK_IN_SYNC_STATE_SET;
            }
        }

        Ok(need_ack_nums)
    }

    fn put_message_status(status: CommitLogAppendStatus) -> PutMessageStatus {
        match status {
            CommitLogAppendStatus::PutOk => PutMessageStatus::PutOk,
            CommitLogAppendStatus::UnknownError => PutMessageStatus::UnknownError,
            CommitLogAppendStatus::CreateSegmentFailed => PutMessageStatus::CreateMappedFileFailed,
            CommitLogAppendStatus::MessageIllegal => PutMessageStatus::MessageIllegal,
        }
    }

    pub async fn put_messages(&self, mut msg_batch: MessageExtBatch) -> PutMessageResult {
        let append_span = rocketmq_observability::trace::store::append_span(&self.telemetry_handle);
        #[cfg(any(feature = "observability", feature = "observability-traces"))]
        rocketmq_observability::trace::record_message_properties_with_handle(
            &self.telemetry_handle,
            &append_span,
            msg_batch.message_ext_broker_inner.get_properties(),
            msg_batch.message_ext_broker_inner.get_body().map(|body| body.len()),
        );
        let tran_type = MessageSysFlag::get_transaction_value(msg_batch.message_ext_broker_inner.sys_flag());
        if MessageSysFlag::TRANSACTION_NOT_TYPE != tran_type {
            return PutMessageResult::new_default(PutMessageStatus::MessageIllegal);
        }
        if msg_batch
            .message_ext_broker_inner
            .message_ext_inner
            .message
            .delay_time_level()
            > 0
        {
            return PutMessageResult::new_default(PutMessageStatus::MessageIllegal);
        }

        //setting ip type:IPV4 OR IPV6, default is ipv4
        let born_host = msg_batch.message_ext_broker_inner.born_host();
        if born_host.is_ipv6() {
            msg_batch.message_ext_broker_inner.with_born_host_v6_flag();
        }

        let store_host = msg_batch.message_ext_broker_inner.store_host();
        if store_host.is_ipv6() {
            msg_batch.message_ext_broker_inner.with_store_host_v6_flag();
        }

        let curr_offset = self.mapped_file_queue.append_handle().current_append_offset();
        let need_handle_ha = self.need_handle_ha(&msg_batch.message_ext_broker_inner);
        let need_ack_nums = match self.handle_ha_service(curr_offset, need_handle_ha) {
            Ok(ack_nums) => ack_nums,
            Err(result) => return result,
        };
        msg_batch.message_ext_broker_inner.version = MessageVersion::V1;
        let auto_message_version_on_topic_len = self.message_store_config.auto_message_version_on_topic_len;
        if auto_message_version_on_topic_len && msg_batch.message_ext_broker_inner.topic().len() > i8::MAX as usize {
            msg_batch.message_ext_broker_inner.version = MessageVersion::V2;
        }
        let mut put_message_context = PutMessageContext::default();
        let prepared = match message_encoder_pool::prepare_message_batch_with_pool(
            &msg_batch,
            &mut put_message_context,
            &self.message_store_config,
        ) {
            Ok(prepared) => prepared,
            Err(result) => return result,
        };
        let sequenced = self
            .append_runtime
            .port()
            .append_batch(msg_batch, prepared, put_message_context)
            .instrument(append_span)
            .await;
        if sequenced.result.put_message_status() != PutMessageStatus::PutOk {
            return sequenced.result;
        }
        self.handle_disk_flush_and_ha(
            sequenced.result,
            sequenced.batch.message_ext_broker_inner,
            need_ack_nums,
            need_handle_ha,
        )
        .await
    }

    pub async fn put_message(&self, mut msg: MessageExtBrokerInner) -> PutMessageResult {
        let append_span = rocketmq_observability::trace::store::append_span(&self.telemetry_handle);
        #[cfg(any(feature = "observability", feature = "observability-traces"))]
        rocketmq_observability::trace::record_message_properties_with_handle(
            &self.telemetry_handle,
            &append_span,
            msg.get_properties(),
            msg.get_body().map(|body| body.len()),
        );
        // Set the message body CRC (consider the most appropriate setting on the client)
        msg.message_ext_inner.body_crc = crc32_bytes(msg.message_ext_inner.message.get_body());
        if self.enabled_append_prop_crc {
            // delete crc32 properties if exist
            msg.delete_property(MessageConst::PROPERTY_CRC32);
        }

        //setting message version
        msg.with_version(MessageVersion::V1);
        let topic = msg.topic();
        // setting auto message on topic length
        if self.message_store_config.auto_message_version_on_topic_len && topic.len() > i8::MAX as usize {
            msg.with_version(MessageVersion::V2);
        }

        //setting ip type:IPV4 OR IPV6, default is ipv4
        let born_host = msg.born_host();
        if born_host.is_ipv6() {
            msg.with_born_host_v6_flag();
        }

        let store_host = msg.store_host();
        if store_host.is_ipv6() {
            msg.with_store_host_v6_flag();
        }

        //get last mapped file from mapped file queue
        // current offset is physical offset
        let curr_offset = self.mapped_file_queue.append_handle().current_append_offset();
        let need_handle_ha = self.need_handle_ha(&msg);
        let need_ack_nums = match self.handle_ha_service(curr_offset, need_handle_ha) {
            Ok(ack_nums) => ack_nums,
            Err(result) => return result,
        };

        let need_assign_offset = !(self.message_store_config.duplication_enable
            && self.store_runtime_state.broker_role() != BrokerRole::Slave);

        let prepared = match message_encoder_pool::prepare_message_with_pool(&msg, &self.message_store_config) {
            Ok(prepared) => prepared,
            Err(result) => return result,
        };
        let sequenced = self
            .append_runtime
            .port()
            .append_message(msg, prepared, need_assign_offset)
            .instrument(append_span)
            .await;
        if sequenced.result.put_message_status() != PutMessageStatus::PutOk {
            return sequenced.result;
        }
        self.handle_disk_flush_and_ha(sequenced.result, sequenced.message, need_ack_nums, need_handle_ha)
            .await
    }

    #[inline]
    fn calc_need_ack_nums(&self, in_sync_replicas: i32) -> i32 {
        let mut need_ack_nums = self.message_store_config.in_sync_replicas;
        if self.message_store_config.enable_auto_in_sync_replicas {
            need_ack_nums = need_ack_nums.min(in_sync_replicas);
            need_ack_nums = need_ack_nums.max(self.message_store_config.min_in_sync_replicas as i32);
        }
        need_ack_nums
    }

    /// Handles disk flushing and high availability (HA) operations for a message.
    ///
    /// This function determines the appropriate actions to take based on the flush disk type
    /// and whether HA handling is required. It performs disk flushing and HA replication
    /// either synchronously or asynchronously, depending on the configuration.
    ///
    /// # Arguments
    ///
    /// * `put_message_result` - The result of the message put operation, which may be updated based
    ///   on the outcomes of disk flushing and HA.
    /// * `msg` - The message being processed.
    /// * `need_ack_nums` - The number of acknowledgments required for HA.
    /// * `need_handle_ha` - A boolean indicating whether HA handling is required.
    ///
    /// # Returns
    ///
    /// Returns the updated `PutMessageResult` after handling disk flushing and HA.
    async fn handle_disk_flush_and_ha(
        &self,
        mut put_message_result: PutMessageResult,
        msg: MessageExtBrokerInner,
        need_ack_nums: i32,
        need_handle_ha: bool,
    ) -> PutMessageResult {
        let append_message_result = put_message_result.append_message_result().unwrap();

        // Use efficient branching based on actual requirements
        match (self.message_store_config.flush_disk_type, need_handle_ha) {
            // Sync flush + HA: Must wait for both in parallel
            (FlushDiskType::SyncFlush, true) => {
                let (flush_status, replica_status) = tokio::join!(
                    self.handle_disk_flush(append_message_result, &msg),
                    self.handle_ha(append_message_result, need_ack_nums)
                );
                if flush_status != PutMessageStatus::PutOk {
                    put_message_result.set_put_message_status(flush_status);
                }
                if replica_status != PutMessageStatus::PutOk {
                    put_message_result.set_put_message_status(replica_status);
                }
            }
            // Sync flush only: Wait for flush only
            (FlushDiskType::SyncFlush, false) => {
                let flush_status = self.handle_disk_flush(append_message_result, &msg).await;
                if flush_status != PutMessageStatus::PutOk {
                    put_message_result.set_put_message_status(flush_status);
                }
            }
            // The sequencer already issued one aggregate async-flush wake-up for this micro-batch.
            (FlushDiskType::AsyncFlush, true) => {
                let replica_status = self.handle_ha(append_message_result, need_ack_nums).await;
                if replica_status != PutMessageStatus::PutOk {
                    put_message_result.set_put_message_status(replica_status);
                }
            }
            (FlushDiskType::AsyncFlush, false) => {}
        }

        put_message_result
    }

    async fn handle_ha(&self, put_message_result: &AppendMessageResult, need_ack_nums: i32) -> PutMessageStatus {
        if need_ack_nums <= 1 {
            return PutMessageStatus::PutOk;
        }
        let next_offset = put_message_result.wrote_offset + put_message_result.wrote_bytes as i64;
        let Some(ha_service) = self.store_context.ha_service() else {
            error!("HA service is not initialized for commit-log replication");
            return PutMessageStatus::UnknownError;
        };
        let Some(authority) = ha_service.write_authority() else {
            error!("HA write authority is unavailable for commit-log replication");
            return PutMessageStatus::FlushSlaveTimeout;
        };
        let (request, mut response) = GroupCommitRequest::with_ack_nums_and_authority(
            next_offset,
            self.message_store_config.slave_timeout as u64,
            need_ack_nums,
            authority,
        );
        ha_service.put_request(request).await;
        match response.wait_for_result_with_timeout().await {
            Ok(PutMessageStatus::FlushDiskTimeout) => PutMessageStatus::FlushSlaveTimeout,
            Ok(status) => status,
            Err(e) => {
                error!("Failed to wait for HA result: {:?}", e);
                PutMessageStatus::UnknownError
            }
        }
    }

    async fn handle_disk_flush(
        &self,
        put_message_result: &AppendMessageResult,
        msg: &MessageExtBrokerInner,
    ) -> PutMessageStatus {
        let start_time = Instant::now();

        let status = self
            .flush_manager
            .handle_disk_flush_shared(put_message_result, msg)
            .instrument(rocketmq_observability::trace::store::flush_span(&self.telemetry_handle))
            .await;

        self.store_metrics
            .record_flush_latency(start_time.elapsed().as_millis() as u64);

        if status == PutMessageStatus::PutOk {
            if let Some(ha_service) = self.store_context.ha_service() {
                ha_service.notify_transfer_progress();
            }
        }

        status
    }

    fn need_handle_ha(&self, msg_inner: &MessageExtBrokerInner) -> bool {
        if !msg_inner.is_wait_store_msg_ok() {
            /*
             No need to sync messages that special config to extra broker slaves.
             @see MessageConst.PROPERTY_WAIT_STORE_MSG_OK
            */
            return false;
        }
        if self.message_store_config.duplication_enable {
            return false;
        }
        if BrokerRole::SyncMaster != self.store_runtime_state.broker_role() {
            // No need to check ha in async or slave broker
            return false;
        }

        true
    }

    fn on_commit_log_dispatch(
        &mut self,
        request: &mut DispatchRequest,
        do_dispatch: bool,
        is_recover: bool,
        is_file_end: bool,
    ) {
        if do_dispatch && !is_file_end {
            let start_time = Instant::now();
            rocketmq_observability::trace::store::dispatch_span(&self.telemetry_handle)
                .in_scope(|| self.dispatcher.dispatch(request));
            self.store_metrics
                .record_dispatch_latency(start_time.elapsed().as_millis() as u64);
        }
    }

    pub fn is_multi_dispatch_msg(msg_inner: &MessageExtBrokerInner) -> bool {
        msg_inner
            .property(MessageConst::PROPERTY_INNER_MULTI_DISPATCH)
            .is_some_and(|s| !s.is_empty())
            && msg_inner.topic().starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX)
    }

    /// Optimized normal recovery with batched I/O
    ///
    /// Performance improvements:
    /// - Batched message reading (64KB chunks) reduces syscalls
    /// - Zero-copy parsing using memory-mapped regions
    /// - Pre-allocated buffers reduce allocation overhead
    /// - Optimized iteration pattern minimizes redundant checks
    ///
    /// Runs optimized normal recovery and reports whether destructive completion succeeded.
    #[must_use]
    pub async fn try_recover_normally_optimized(&mut self, max_phy_offset_of_consume_queue: i64) -> bool {
        use crate::log_file::commit_log_recovery::BatchMessageIterator;
        use crate::log_file::commit_log_recovery::RecoveryContext;

        let start = std::time::Instant::now();
        let check_crc_on_recover = self.message_store_config.check_crc_on_recover;
        let check_dup_info = self.message_store_config.duplication_enable;
        let message_store_config = self.message_store_config.clone();
        let mapped_files = self.mapped_file_queue.get_mapped_files();
        let mapped_files_inner = mapped_files;

        if mapped_files_inner.is_empty() {
            warn!("The commitlog files are deleted, and delete the consume queue files");
            return apply_recovery_completion!(
                self,
                CommitLogRecoveryCompletion::Empty,
                max_phy_offset_of_consume_queue,
            );
        }

        let recovery_window = plan_normal_recovery_file_window(
            mapped_files_inner.len(),
            self.message_store_config.max_recovery_commit_log_files,
        );
        let recovery_file_limit = recovery_window.file_count_limit;
        let mut index = recovery_window.start_index;
        info!(
            "Starting normal recovery from file index {} using up to {} commitlog files (optimized)",
            index, recovery_file_limit
        );

        let initial_offset = match u64::try_from(self.get_confirm_offset().max(0)) {
            Ok(offset) => offset,
            Err(error) => {
                warn!("normal optimized recovery initial offset conversion failed: {error}");
                return false;
            }
        };
        let mut normal_recovery = match NormalRecoveryState::try_new(initial_offset, NormalRecoveryPolicy::Optimized) {
            Ok(state) => state,
            Err(error) => {
                warn!("normal optimized recovery initial state failed: {error}");
                return false;
            }
        };
        let do_dispatch = false;

        let mut recovery_ctx = RecoveryContext::new(
            check_crc_on_recover,
            check_dup_info,
            message_store_config,
            self.store_context.max_delay_level,
            self.store_context.delay_level_table.as_ref().clone(),
        );
        'segments: while index < mapped_files_inner.len() {
            let Some(mapped_file) = mapped_files_inner.get(index) else {
                break;
            };
            let process_offset = mapped_file.get_file_from_offset();
            let mut iterator = BatchMessageIterator::new(mapped_file);
            let mut file_processed = false;
            let outcome = normal_recovery.drive_segment(
                process_offset,
                || {
                    let Some((mut msg_bytes, absolute_offset, _msg_size)) = iterator.next_message() else {
                        return Ok(NormalRecoveryRecord::SourceEnded);
                    };
                    let dispatch_request = recovery_ctx.process_message(&mut msg_bytes, absolute_offset);
                    if dispatch_request.success && dispatch_request.msg_size > 0 {
                        let relative_start = u64::try_from(absolute_offset)
                            .map_err(NormalRecoveryAdapterError::RelativeOffsetConversion)?;
                        let frame_size = u64::try_from(dispatch_request.msg_size)
                            .map_err(NormalRecoveryAdapterError::MessageSizeConversion)?;
                        Ok(NormalRecoveryRecord::Message {
                            relative_start,
                            size: frame_size,
                            record: dispatch_request,
                        })
                    } else if dispatch_request.success && dispatch_request.msg_size == 0 {
                        Ok(NormalRecoveryRecord::Blank {
                            record: dispatch_request,
                        })
                    } else {
                        Ok(NormalRecoveryRecord::Invalid {
                            relative_start: u64::try_from(absolute_offset).ok(),
                            record: dispatch_request,
                        })
                    }
                },
                || {
                    info!(
                        "Recovering physics file: {} (optimized batch mode)",
                        mapped_file.get_file_name()
                    );
                },
                |observation, dispatch_request| match observation {
                    NormalRecoveryObservation::MessageAccepted => {
                        self.on_commit_log_dispatch(dispatch_request, do_dispatch, true, false);
                        file_processed = true;
                    }
                    NormalRecoveryObservation::Blank => {
                        self.on_commit_log_dispatch(dispatch_request, do_dispatch, true, true);
                    }
                    NormalRecoveryObservation::Invalid { relative_start } => {
                        if dispatch_request.msg_size > 0 {
                            let warning_offset =
                                relative_start.and_then(|relative| process_offset.checked_add(relative));
                            if let Some(warning_offset) = warning_offset {
                                warn!("found a half message at {warning_offset}, it will be truncated.");
                            } else {
                                warn!("found a half message with an invalid offset; it will be truncated.");
                            }
                        }
                        info!("recover physics file end: {}", mapped_file.get_file_name());
                    }
                },
            );
            match outcome {
                NormalRecoverySegmentOutcome::ContinueNextSegment => {
                    if file_processed {
                        recovery_ctx.stats.files_processed += 1;
                    }
                    index += 1;
                }
                NormalRecoverySegmentOutcome::StopRecovery => break 'segments,
                NormalRecoverySegmentOutcome::AdapterFailed(NormalRecoveryAdapterError::RelativeOffsetConversion(
                    error,
                )) => {
                    warn!("normal optimized recovery relative offset conversion failed: {error}");
                    break 'segments;
                }
                NormalRecoverySegmentOutcome::AdapterFailed(NormalRecoveryAdapterError::MessageSizeConversion(
                    error,
                )) => {
                    warn!("normal optimized recovery message size conversion failed: {error}");
                    break 'segments;
                }
                NormalRecoverySegmentOutcome::AdapterFailed(NormalRecoveryAdapterError::FramePositionOverflow {
                    position,
                    size,
                }) => {
                    warn!("normal optimized recovery frame position overflow at {position} with size {size}");
                    break 'segments;
                }
                NormalRecoverySegmentOutcome::StateFailed(error) => {
                    warn!("normal optimized recovery offset state failed: {error}");
                    break 'segments;
                }
            }
        }

        let completion = normal_recovery.completion(max_phy_offset_of_consume_queue);
        let recovery_succeeded = apply_recovery_completion!(self, completion, max_phy_offset_of_consume_queue);

        recovery_ctx.stats.recovery_time_ms = start.elapsed().as_millis();
        recovery_ctx.stats.log_summary("Normal");
        recovery_succeeded
    }

    pub async fn recover_normally_optimized(&mut self, max_phy_offset_of_consume_queue: i64) {
        let _ = self
            .try_recover_normally_optimized(max_phy_offset_of_consume_queue)
            .await;
    }

    /// Runs compatibility normal recovery and reports whether destructive completion succeeded.
    #[must_use]
    pub async fn try_recover_normally(&mut self, max_phy_offset_of_consume_queue: i64) -> bool {
        let check_crc_on_recover = self.message_store_config.check_crc_on_recover;
        let check_dup_info = self.message_store_config.duplication_enable;
        let message_store_config = self.message_store_config.clone();
        // let mut mapped_file_queue = mapped_files.write().await;
        let mapped_files = self.mapped_file_queue.get_mapped_files();
        let mapped_files_inner = mapped_files;
        if !mapped_files_inner.is_empty() {
            let recovery_window = plan_normal_recovery_file_window(
                mapped_files_inner.len(),
                self.message_store_config.max_recovery_commit_log_files,
            );
            let recovery_file_limit = recovery_window.file_count_limit;
            let mut index = recovery_window.start_index;
            info!(
                "Starting normal recovery from file index {} using up to {} commitlog files",
                index, recovery_file_limit
            );
            let initial_offset = match u64::try_from(self.get_confirm_offset().max(0)) {
                Ok(offset) => offset,
                Err(error) => {
                    warn!("normal recovery initial offset conversion failed: {error}");
                    return false;
                }
            };
            let mut normal_recovery = match NormalRecoveryState::try_new(initial_offset, NormalRecoveryPolicy::Standard)
            {
                Ok(state) => state,
                Err(error) => {
                    warn!("normal recovery initial state failed: {error}");
                    return false;
                }
            };
            let do_dispatch = false;
            let max_delay_level = self.store_context.max_delay_level;
            let delay_level_table = self.store_context.delay_level_table.clone();
            'segments: while index < mapped_files_inner.len() {
                let Some(mapped_file) = mapped_files_inner.get(index) else {
                    break;
                };
                let process_offset = mapped_file.get_file_from_offset();
                let mut current_pos = 0usize;
                let outcome = normal_recovery.drive_segment(
                    process_offset,
                    || {
                        let frame_position = current_pos;
                        let (msg, size) =
                            read_declared_frame(current_pos, |position, size| mapped_file.get_bytes(position, size));
                        let Some(mut msg_bytes) = msg else {
                            return Ok(NormalRecoveryRecord::SourceEnded);
                        };
                        let next_position =
                            current_pos
                                .checked_add(size)
                                .ok_or(NormalRecoveryAdapterError::FramePositionOverflow {
                                    position: current_pos,
                                    size,
                                })?;
                        current_pos = next_position;
                        let dispatch_request = check_message_and_return_size(
                            &mut msg_bytes,
                            check_crc_on_recover,
                            check_dup_info,
                            true,
                            &message_store_config,
                            max_delay_level,
                            delay_level_table.as_ref(),
                        );
                        if dispatch_request.success && dispatch_request.msg_size > 0 {
                            let relative_start = u64::try_from(frame_position)
                                .map_err(NormalRecoveryAdapterError::RelativeOffsetConversion)?;
                            let frame_size = u64::try_from(dispatch_request.msg_size)
                                .map_err(NormalRecoveryAdapterError::MessageSizeConversion)?;
                            Ok(NormalRecoveryRecord::Message {
                                relative_start,
                                size: frame_size,
                                record: dispatch_request,
                            })
                        } else if dispatch_request.success && dispatch_request.msg_size == 0 {
                            Ok(NormalRecoveryRecord::Blank {
                                record: dispatch_request,
                            })
                        } else {
                            Ok(NormalRecoveryRecord::Invalid {
                                relative_start: u64::try_from(frame_position).ok(),
                                record: dispatch_request,
                            })
                        }
                    },
                    || {},
                    |observation, dispatch_request| match observation {
                        NormalRecoveryObservation::MessageAccepted => {
                            self.on_commit_log_dispatch(dispatch_request, do_dispatch, true, false);
                        }
                        NormalRecoveryObservation::Blank => {
                            self.on_commit_log_dispatch(dispatch_request, do_dispatch, true, true);
                        }
                        NormalRecoveryObservation::Invalid { relative_start } => {
                            if dispatch_request.msg_size > 0 {
                                let warning_offset =
                                    relative_start.and_then(|relative| process_offset.checked_add(relative));
                                if let Some(warning_offset) = warning_offset {
                                    warn!("found a half message at {warning_offset}, it will be truncated.");
                                } else {
                                    warn!("found a half message with an invalid offset; it will be truncated.");
                                }
                            }
                            info!("recover physics file end,{} ", mapped_file.get_file_name());
                        }
                    },
                );
                match outcome {
                    NormalRecoverySegmentOutcome::ContinueNextSegment => {
                        index += 1;
                        if index < mapped_files_inner.len() {
                            if let Some(next_file) = mapped_files_inner.get(index) {
                                info!("recover next physics file:{}", next_file.get_file_name());
                            }
                        } else {
                            info!(
                                "recover last {} physics file over, last mapped file:{} ",
                                recovery_file_limit,
                                mapped_file.get_file_name(),
                            );
                        }
                    }
                    NormalRecoverySegmentOutcome::StopRecovery => break 'segments,
                    NormalRecoverySegmentOutcome::AdapterFailed(
                        NormalRecoveryAdapterError::FramePositionOverflow { position, size },
                    ) => {
                        warn!("normal recovery frame position overflow at {position} with size {size}");
                        break 'segments;
                    }
                    NormalRecoverySegmentOutcome::AdapterFailed(
                        NormalRecoveryAdapterError::RelativeOffsetConversion(error),
                    ) => {
                        warn!("normal recovery relative offset conversion failed: {error}");
                        break 'segments;
                    }
                    NormalRecoverySegmentOutcome::AdapterFailed(NormalRecoveryAdapterError::MessageSizeConversion(
                        error,
                    )) => {
                        warn!("normal recovery message size conversion failed: {error}");
                        break 'segments;
                    }
                    NormalRecoverySegmentOutcome::StateFailed(error) => {
                        warn!("normal recovery offset state failed: {error}");
                        break 'segments;
                    }
                }
            }

            let completion = normal_recovery.completion(max_phy_offset_of_consume_queue);
            apply_recovery_completion!(self, completion, max_phy_offset_of_consume_queue)
        } else {
            warn!(
                "The commitlog files are deleted, and delete the consume queue
                                        files"
            );
            apply_recovery_completion!(
                self,
                CommitLogRecoveryCompletion::Empty,
                max_phy_offset_of_consume_queue,
            )
        }
    }

    pub async fn recover_normally(&mut self, max_phy_offset_of_consume_queue: i64) {
        let _ = self.try_recover_normally(max_phy_offset_of_consume_queue).await;
    }

    //Fetch and compute the newest confirmOffset.
    pub fn get_confirm_offset(&self) -> i64 {
        resolve_commit_log_confirm_offset(
            self.message_store_config.as_ref(),
            self.store_runtime_state.broker_role(),
            self.broker_config.as_ref(),
            &self.store_context,
            self.runtime_state.confirm_offset(),
            self.get_max_offset(),
            self.get_flushed_where(),
        )
    }

    pub fn get_confirm_offset_directly(&self) -> i64 {
        if self.broker_config.enable_controller_mode {
            if self.store_runtime_state.broker_role() != BrokerRole::Slave
                && !self.store_context.running_flags.is_fenced()
            {
                let max_phy_offset = self.get_max_offset();
                if let Some(ha_service) = self.store_context.ha_service() {
                    if ha_service.local_sync_state_set_size(max_phy_offset) <= 1 {
                        return max_phy_offset;
                    }
                }
            }

            self.runtime_state.confirm_offset()
        } else if self.broker_config.duplication_enable {
            self.runtime_state.confirm_offset()
        } else {
            self.get_max_offset()
        }
    }

    /// Optimized abnormal recovery with batched I/O
    ///
    /// Performance improvements:
    /// - Fast checkpoint-based file scanning
    /// - Batched message reading (64KB chunks)
    /// - Zero-copy validation using mmap regions
    /// - Reduced lock contention through buffered dispatch
    ///
    /// Runs optimized abnormal recovery and reports whether destructive completion succeeded.
    #[must_use]
    pub async fn try_recover_abnormally_optimized(&mut self, max_phy_offset_of_consume_queue: i64) -> bool {
        use crate::log_file::commit_log_recovery::plan_abnormal_recovery_window;
        use crate::log_file::commit_log_recovery::BatchMessageIterator;
        use crate::log_file::commit_log_recovery::RecoveryContext;

        let start = std::time::Instant::now();
        let check_crc_on_recover = self.message_store_config.check_crc_on_recover;
        let check_dup_info = self.message_store_config.duplication_enable;
        let broker_config = self.broker_config.clone();

        let binding = self.mapped_file_queue.get_mapped_files();
        let mapped_files_inner = binding;

        if mapped_files_inner.is_empty() {
            warn!("The commitlog files are deleted, and delete the consume queue files");
            return apply_recovery_completion!(
                self,
                CommitLogRecoveryCompletion::Empty,
                max_phy_offset_of_consume_queue,
            );
        }

        let recovery_window = plan_abnormal_recovery_window(
            &mapped_files_inner,
            &self.message_store_config,
            &self.store_checkpoint,
            max_phy_offset_of_consume_queue,
            self.get_confirm_offset(),
            self.get_min_offset(),
            self.get_max_offset(),
        );
        log_abnormal_recovery_window(&recovery_window, "optimized");

        let mut index = recovery_window.start_index;
        let Some(first_recovery_file) = mapped_files_inner.get(index) else {
            warn!("optimized abnormal recovery window starts outside mapped files: {index}");
            return false;
        };
        let initial_offset = if index == 0 {
            first_recovery_file.get_file_from_offset()
        } else {
            0
        };
        let mut abnormal_recovery =
            match AbnormalRecoveryState::try_new(initial_offset, AbnormalRecoveryPolicy::Optimized) {
                Ok(state) => state,
                Err(error) => {
                    warn!("optimized abnormal recovery initial state failed: {error}");
                    return false;
                }
            };
        let do_dispatch = true;

        let mut recovery_ctx = RecoveryContext::new(
            check_crc_on_recover,
            check_dup_info,
            self.message_store_config.clone(),
            self.store_context.max_delay_level,
            self.store_context.delay_level_table.as_ref().clone(),
        );
        let confirm_offset = self.get_confirm_offset();
        let confirm_bounded = self.message_store_config.duplication_enable || broker_config.enable_controller_mode;

        'segments: while index < mapped_files_inner.len() {
            let Some(mapped_file) = mapped_files_inner.get(index) else {
                break;
            };
            let process_offset = mapped_file.get_file_from_offset();
            let mut iterator = BatchMessageIterator::new(mapped_file);
            let mut file_processed = false;
            let outcome = abnormal_recovery.drive_abnormal_segment(
                process_offset,
                || {
                    let Some((mut msg_bytes, absolute_offset, msg_size)) = iterator.next_message() else {
                        return Ok(AbnormalRecoveryRecord::SourceEnded);
                    };
                    let dispatch_request = recovery_ctx.process_message(&mut msg_bytes, absolute_offset);
                    if dispatch_request.success && dispatch_request.msg_size > 0 {
                        let confirm_candidate_end =
                            abnormal_confirm_candidate_end(dispatch_request.commit_log_offset, msg_size)
                                .map_err(AbnormalRecoveryAdapterError::ConfirmCandidate)?;
                        let validated_size = u64::try_from(dispatch_request.msg_size)
                            .map_err(AbnormalRecoveryAdapterError::ValidatedSizeConversion)?;
                        let relative_start = u64::try_from(absolute_offset)
                            .map_err(AbnormalRecoveryAdapterError::RelativeOffsetConversion)?;
                        let dispatch_gate = if confirm_bounded {
                            let confirm_offset = u64::try_from(confirm_offset.max(0))
                                .map_err(AbnormalRecoveryAdapterError::ConfirmLimitConversion)?;
                            AbnormalRecoveryDispatchGate::ConfirmBounded { confirm_offset }
                        } else {
                            AbnormalRecoveryDispatchGate::Ungated
                        };
                        Ok(AbnormalRecoveryRecord::Message {
                            relative_start,
                            validated_size,
                            confirm_candidate_end,
                            dispatch_gate,
                            record: dispatch_request,
                        })
                    } else if dispatch_request.success && dispatch_request.msg_size == 0 {
                        Ok(AbnormalRecoveryRecord::Blank {
                            record: dispatch_request,
                        })
                    } else {
                        Ok(AbnormalRecoveryRecord::Invalid {
                            relative_start: u64::try_from(absolute_offset).ok(),
                            record: dispatch_request,
                        })
                    }
                },
                || {
                    info!(
                        "Recovering physics file: {} (optimized batch mode)",
                        mapped_file.get_file_name()
                    );
                },
                |observation, dispatch_request| {
                    let message_accepted = matches!(
                        observation,
                        AbnormalRecoveryObservation::DispatchMessage | AbnormalRecoveryObservation::SkipMessageDispatch
                    );
                    match observation {
                        AbnormalRecoveryObservation::DispatchMessage => {
                            self.on_commit_log_dispatch(dispatch_request, do_dispatch, true, false);
                        }
                        AbnormalRecoveryObservation::SkipMessageDispatch => {}
                        AbnormalRecoveryObservation::Blank => {
                            self.on_commit_log_dispatch(dispatch_request, do_dispatch, true, true);
                        }
                        AbnormalRecoveryObservation::Invalid { relative_start } => {
                            if dispatch_request.msg_size > 0 {
                                let warning_offset =
                                    relative_start.and_then(|relative| process_offset.checked_add(relative));
                                if let Some(warning_offset) = warning_offset {
                                    warn!("found a half message at {warning_offset}, it will be truncated.");
                                } else {
                                    warn!("found a half message with an invalid offset; it will be truncated.");
                                }
                            }
                            info!("recover physics file end: {}", mapped_file.get_file_name());
                        }
                    }
                    if message_accepted {
                        file_processed = true;
                    }
                },
            );
            match outcome {
                AbnormalRecoverySegmentOutcome::ContinueNextSegment => {
                    if file_processed {
                        recovery_ctx.stats.files_processed += 1;
                    }
                    index += 1;
                }
                AbnormalRecoverySegmentOutcome::StopRecovery => break 'segments,
                AbnormalRecoverySegmentOutcome::AdapterFailed(AbnormalRecoveryAdapterError::ConfirmCandidate(
                    error,
                )) => {
                    warn!("optimized abnormal recovery confirm candidate failed: {error}");
                    break 'segments;
                }
                AbnormalRecoverySegmentOutcome::AdapterFailed(
                    AbnormalRecoveryAdapterError::ConfirmLimitConversion(error),
                ) => {
                    warn!("optimized abnormal recovery confirm limit conversion failed: {error}");
                    break 'segments;
                }
                AbnormalRecoverySegmentOutcome::AdapterFailed(
                    AbnormalRecoveryAdapterError::RelativeOffsetConversion(error),
                ) => {
                    warn!("optimized abnormal recovery relative offset conversion failed: {error}");
                    break 'segments;
                }
                AbnormalRecoverySegmentOutcome::AdapterFailed(
                    AbnormalRecoveryAdapterError::ValidatedSizeConversion(error),
                ) => {
                    warn!("optimized abnormal recovery validated size conversion failed: {error}");
                    break 'segments;
                }
                AbnormalRecoverySegmentOutcome::AdapterFailed(
                    AbnormalRecoveryAdapterError::FramePositionOverflow { position, size },
                ) => {
                    warn!("optimized abnormal recovery frame position overflow at {position} with size {size}");
                    break 'segments;
                }
                AbnormalRecoverySegmentOutcome::StateFailed(error) => {
                    warn!("optimized abnormal recovery offset state failed: {error}");
                    break 'segments;
                }
                AbnormalRecoverySegmentOutcome::UnexpectedAction(action) => {
                    warn!("optimized abnormal recovery unexpected action: {action:?}");
                    break 'segments;
                }
            }
        }

        let completion = abnormal_recovery.completion(max_phy_offset_of_consume_queue);
        let recovery_succeeded = apply_recovery_completion!(self, completion, max_phy_offset_of_consume_queue);

        recovery_ctx.stats.recovery_time_ms = start.elapsed().as_millis();
        recovery_ctx.stats.log_summary("Abnormal");
        recovery_succeeded
    }

    pub async fn recover_abnormally_optimized(&mut self, max_phy_offset_of_consume_queue: i64) {
        let _ = self
            .try_recover_abnormally_optimized(max_phy_offset_of_consume_queue)
            .await;
    }

    /// Runs compatibility abnormal recovery and reports whether destructive completion succeeded.
    #[must_use]
    pub async fn try_recover_abnormally(&mut self, max_phy_offset_of_consume_queue: i64) -> bool {
        use crate::log_file::commit_log_recovery::plan_abnormal_recovery_window;

        let check_crc_on_recover = self.message_store_config.check_crc_on_recover;
        let check_dup_info = self.message_store_config.duplication_enable;
        let broker_config = self.broker_config.clone();
        // let mut mapped_file_queue = mapped_files.write().await;
        let binding = self.mapped_file_queue.get_mapped_files();
        let mapped_files_inner = binding;
        if !mapped_files_inner.is_empty() {
            let recovery_window = plan_abnormal_recovery_window(
                &mapped_files_inner,
                &self.message_store_config,
                &self.store_checkpoint,
                max_phy_offset_of_consume_queue,
                self.get_confirm_offset(),
                self.get_min_offset(),
                self.get_max_offset(),
            );
            log_abnormal_recovery_window(&recovery_window, "standard");

            let mut index = recovery_window.start_index;
            let Some(first_recovery_file) = mapped_files_inner.get(index) else {
                warn!("standard abnormal recovery window starts outside mapped files: {index}");
                return false;
            };
            let initial_offset = first_recovery_file.get_file_from_offset();
            let mut abnormal_recovery =
                match AbnormalRecoveryState::try_new(initial_offset, AbnormalRecoveryPolicy::Standard) {
                    Ok(state) => state,
                    Err(error) => {
                        warn!("standard abnormal recovery initial state failed: {error}");
                        return false;
                    }
                };
            let do_dispatch = true;
            let max_delay_level = self.store_context.max_delay_level;
            let delay_level_table = self.store_context.delay_level_table.clone();
            let message_store_config = self.message_store_config.clone();
            let confirm_offset = self.get_confirm_offset();
            let confirm_bounded = self.message_store_config.duplication_enable || broker_config.enable_controller_mode;

            'segments: while index < mapped_files_inner.len() {
                let Some(mapped_file) = mapped_files_inner.get(index) else {
                    break;
                };
                let process_offset = mapped_file.get_file_from_offset();
                let mut current_pos = 0usize;
                let outcome = abnormal_recovery.drive_abnormal_segment(
                    process_offset,
                    || {
                        let frame_position = current_pos;
                        let (msg, input_size) =
                            read_declared_frame(current_pos, |position, size| mapped_file.get_bytes(position, size));
                        let Some(mut msg_bytes) = msg else {
                            return Ok(AbnormalRecoveryRecord::SourceEnded);
                        };
                        current_pos = current_pos.checked_add(input_size).ok_or(
                            AbnormalRecoveryAdapterError::FramePositionOverflow {
                                position: current_pos,
                                size: input_size,
                            },
                        )?;
                        let dispatch_request = check_message_and_return_size(
                            &mut msg_bytes,
                            check_crc_on_recover,
                            check_dup_info,
                            true,
                            &message_store_config,
                            max_delay_level,
                            delay_level_table.as_ref(),
                        );
                        if dispatch_request.success && dispatch_request.msg_size > 0 {
                            let confirm_candidate_end =
                                abnormal_confirm_candidate_end(dispatch_request.commit_log_offset, input_size)
                                    .map_err(AbnormalRecoveryAdapterError::ConfirmCandidate)?;
                            let validated_size = u64::try_from(dispatch_request.msg_size)
                                .map_err(AbnormalRecoveryAdapterError::ValidatedSizeConversion)?;
                            let relative_start = u64::try_from(frame_position)
                                .map_err(AbnormalRecoveryAdapterError::RelativeOffsetConversion)?;
                            let dispatch_gate = if confirm_bounded {
                                let confirm_offset = u64::try_from(confirm_offset.max(0))
                                    .map_err(AbnormalRecoveryAdapterError::ConfirmLimitConversion)?;
                                AbnormalRecoveryDispatchGate::ConfirmBounded { confirm_offset }
                            } else {
                                AbnormalRecoveryDispatchGate::Ungated
                            };
                            Ok(AbnormalRecoveryRecord::Message {
                                relative_start,
                                validated_size,
                                confirm_candidate_end,
                                dispatch_gate,
                                record: dispatch_request,
                            })
                        } else if dispatch_request.success && dispatch_request.msg_size == 0 {
                            Ok(AbnormalRecoveryRecord::Blank {
                                record: dispatch_request,
                            })
                        } else {
                            Ok(AbnormalRecoveryRecord::Invalid {
                                relative_start: u64::try_from(frame_position).ok(),
                                record: dispatch_request,
                            })
                        }
                    },
                    || {},
                    |observation, dispatch_request| match observation {
                        AbnormalRecoveryObservation::DispatchMessage => {
                            self.on_commit_log_dispatch(dispatch_request, do_dispatch, true, false);
                        }
                        AbnormalRecoveryObservation::SkipMessageDispatch => {}
                        AbnormalRecoveryObservation::Blank => {
                            self.on_commit_log_dispatch(dispatch_request, do_dispatch, true, true);
                        }
                        AbnormalRecoveryObservation::Invalid { relative_start } => {
                            if dispatch_request.msg_size > 0 {
                                let warning_offset =
                                    relative_start.and_then(|relative| process_offset.checked_add(relative));
                                if let Some(warning_offset) = warning_offset {
                                    warn!("found a half message at {warning_offset}, it will be truncated.");
                                } else {
                                    warn!("found a half message with an invalid offset; it will be truncated.");
                                }
                            }
                            info!("recover physics file end,{} ", mapped_file.get_file_name());
                        }
                    },
                );
                match outcome {
                    AbnormalRecoverySegmentOutcome::ContinueNextSegment => {
                        index += 1;
                        if index < mapped_files_inner.len() {
                            if let Some(next_file) = mapped_files_inner.get(index) {
                                info!("recover next physics file:{}", next_file.get_file_name());
                            }
                        } else {
                            info!(
                                "recover last physics file over, last mapped file:{} ",
                                mapped_file.get_file_name()
                            );
                        }
                    }
                    AbnormalRecoverySegmentOutcome::StopRecovery => break 'segments,
                    AbnormalRecoverySegmentOutcome::AdapterFailed(AbnormalRecoveryAdapterError::ConfirmCandidate(
                        error,
                    )) => {
                        warn!("standard abnormal recovery confirm candidate failed: {error}");
                        break 'segments;
                    }
                    AbnormalRecoverySegmentOutcome::AdapterFailed(
                        AbnormalRecoveryAdapterError::ConfirmLimitConversion(error),
                    ) => {
                        warn!("standard abnormal recovery confirm limit conversion failed: {error}");
                        break 'segments;
                    }
                    AbnormalRecoverySegmentOutcome::AdapterFailed(
                        AbnormalRecoveryAdapterError::FramePositionOverflow { position, size },
                    ) => {
                        warn!("standard abnormal recovery frame position overflow at {position} with size {size}");
                        break 'segments;
                    }
                    AbnormalRecoverySegmentOutcome::AdapterFailed(
                        AbnormalRecoveryAdapterError::RelativeOffsetConversion(error),
                    ) => {
                        warn!("standard abnormal recovery relative offset conversion failed: {error}");
                        break 'segments;
                    }
                    AbnormalRecoverySegmentOutcome::AdapterFailed(
                        AbnormalRecoveryAdapterError::ValidatedSizeConversion(error),
                    ) => {
                        warn!("standard abnormal recovery validated size conversion failed: {error}");
                        break 'segments;
                    }
                    AbnormalRecoverySegmentOutcome::StateFailed(error) => {
                        warn!("standard abnormal recovery offset state failed: {error}");
                        break 'segments;
                    }
                    AbnormalRecoverySegmentOutcome::UnexpectedAction(action) => {
                        warn!("standard abnormal recovery unexpected action: {action:?}");
                        break 'segments;
                    }
                }
            }

            let completion = abnormal_recovery.completion(max_phy_offset_of_consume_queue);
            apply_recovery_completion!(self, completion, max_phy_offset_of_consume_queue)
        } else {
            warn!(
                "The commitlog files are deleted, and delete the consume queue
                                        files"
            );
            apply_recovery_completion!(
                self,
                CommitLogRecoveryCompletion::Empty,
                max_phy_offset_of_consume_queue,
            )
        }
    }

    pub async fn recover_abnormally(&mut self, max_phy_offset_of_consume_queue: i64) {
        let _ = self.try_recover_abnormally(max_phy_offset_of_consume_queue).await;
    }

    #[inline]
    pub fn get_max_offset(&self) -> i64 {
        self.mapped_file_queue.get_max_offset()
    }

    #[inline]
    pub fn get_flushed_where(&self) -> i64 {
        self.mapped_file_queue.get_flushed_where()
    }

    #[inline]
    pub fn mapped_file_warmup_stats(&self) -> MappedFileWarmupStats {
        self.mapped_file_queue.warmup_stats()
    }

    /// Returns a read-only aggregate of mapped-file I/O counters.
    pub fn mapped_file_io_stats(&self) -> MappedFileIoStats {
        self.mapped_file_queue.io_stats()
    }

    #[inline]
    pub fn lazy_mmap_stats(&self) -> LazyMmapStats {
        self.mapped_file_queue.lazy_mmap_stats()
    }

    #[inline]
    pub fn load_statistics(&self) -> LoadStatistics {
        self.runtime_state.load_statistics()
    }

    fn recovery_mmap_advice(&self) -> RecoveryMmapAdvice {
        let platform_capability = crate::platform::current_store_platform_capability();
        if !platform_capability.optimization.mmap_advice_supported {
            return RecoveryMmapAdvice::Disabled;
        }

        match self.message_store_config.effective_linux_recovery_fadvise() {
            LinuxRecoveryFadviseMode::Disabled => RecoveryMmapAdvice::Disabled,
            LinuxRecoveryFadviseMode::Sequential => RecoveryMmapAdvice::Sequential,
        }
    }

    fn recovery_file_prefetch(&self) -> RecoveryFilePrefetch {
        let platform_capability = crate::platform::current_store_platform_capability();
        if !self.message_store_config.store_io_hint_enable || !platform_capability.optimization.file_prefetch_supported
        {
            return RecoveryFilePrefetch::Disabled;
        }

        RecoveryFilePrefetch::Sequential
    }

    fn effective_lazy_mmap_enable(&self) -> bool {
        let platform_capability = crate::platform::current_store_platform_capability();
        self.message_store_config.store_lazy_mmap_enable && platform_capability.optimization.lazy_mmap_supported
    }

    #[cfg(test)]
    pub(crate) fn last_mapped_file_for_testing(&self) -> Option<Arc<DefaultMappedFile>> {
        self.mapped_file_queue.get_last_mapped_file()
    }

    #[cfg(test)]
    pub(crate) fn try_delete_last_mapped_file_for_testing(&mut self) -> bool {
        self.mapped_file_queue.try_delete_last_mapped_file()
    }

    #[inline]
    pub fn flush(&self) -> i64 {
        match self.try_flush() {
            Ok(progress) => progress.durable,
            Err(error) => {
                warn!(error = %error, "commit log flush failed; returning last durable watermark");
                self.mapped_file_queue.get_flushed_where()
            }
        }
    }

    /// Flushes the commit log and reports appended and durable watermarks.
    pub fn try_flush(&self) -> Result<crate::consume_queue::mapped_file_queue::FlushProgress, StoreError> {
        self.mapped_file_queue
            .try_flush(0)
            .map_err(|source| StoreError::mapped_file(StoreOperation::Flush, source))
    }

    pub fn get_min_offset(&self) -> i64 {
        match self.mapped_file_queue.get_first_mapped_file() {
            None => -1,
            Some(mapped_file) => {
                if mapped_file.is_available() {
                    mapped_file.get_file_from_offset() as i64
                } else {
                    self.roll_next_file(mapped_file.get_file_from_offset() as i64)
                }
            }
        }
    }

    pub fn roll_next_file(&self, offset: i64) -> i64 {
        let mapped_file_size = self.message_store_config.mapped_file_size_commit_log as i64;
        offset + mapped_file_size - (offset % mapped_file_size)
    }

    pub fn get_data(&self, offset: i64) -> Option<SelectMappedBufferResult> {
        self.get_data_with_option(offset, offset == 0)
    }

    pub fn get_last_file_from_offset(&self) -> i64 {
        self.mapped_file_queue
            .get_last_mapped_file()
            .filter(|mapped_file| mapped_file.is_available())
            .map(|mapped_file| mapped_file.get_file_from_offset() as i64)
            .unwrap_or(-1)
    }

    pub fn get_last_mapped_file(&self, start_offset: i64) -> bool {
        self.mapped_file_queue
            .get_last_mapped_file_mut_start_offset(start_offset as u64, true)
            .is_some()
    }

    pub fn reset_offset(&self, offset: i64) -> bool {
        self.mapped_file_queue.reset_offset(offset)
    }

    pub fn set_mapped_file_queue_offset(&self, phy_offset: i64) {
        self.mapped_file_queue.set_flushed_where(phy_offset);
        self.mapped_file_queue.set_committed_where(phy_offset);
    }

    pub fn is_mapped_files_empty(&self) -> bool {
        self.mapped_file_queue.is_mapped_files_empty()
    }

    pub fn truncate_dirty_files(&self, offset_to_truncate: i64) {
        let _ = self.try_truncate_dirty_files(offset_to_truncate);
    }

    /// Truncates the CommitLog tail and reports whether every removed segment left the namespace.
    #[must_use]
    pub fn try_truncate_dirty_files(&self, offset_to_truncate: i64) -> bool {
        self.mapped_file_queue.try_truncate_dirty_files(offset_to_truncate)
    }

    pub fn delete_expired_files_by_time(
        &mut self,
        expired_time: i64,
        delete_files_interval: i32,
        interval_forcibly: i64,
        clean_immediately: bool,
        delete_file_batch_max: i32,
    ) -> i32 {
        self.delete_expired_files_by_time_before(
            expired_time,
            delete_files_interval,
            interval_forcibly,
            clean_immediately,
            delete_file_batch_max,
            None,
        )
    }

    pub fn delete_expired_files_by_time_before(
        &mut self,
        expired_time: i64,
        delete_files_interval: i32,
        interval_forcibly: i64,
        clean_immediately: bool,
        delete_file_batch_max: i32,
        pinned_file_offset: Option<u64>,
    ) -> i32 {
        self.mapped_file_queue.delete_expired_file_by_time_before(
            expired_time,
            delete_files_interval,
            interval_forcibly,
            clean_immediately,
            delete_file_batch_max,
            pinned_file_offset,
        )
    }

    pub fn retry_delete_first_file(&mut self, interval_forcibly: i64) -> bool {
        self.mapped_file_queue.retry_delete_first_file(interval_forcibly)
    }

    pub fn get_bulk_data(&self, offset: i64, size: i32) -> Option<Vec<SelectMappedBufferResult>> {
        self.read_handle().get_bulk_data(offset, size)
    }

    /// Returns cumulative heap-copy and attachment-comparison bytes for mapped-file selections.
    pub fn selection_stats(&self) -> crate::consume_queue::mapped_file_queue::MappedFileSelectionStats {
        self.mapped_file_queue.selection_stats()
    }

    pub fn select_segments(
        &self,
        offset: i64,
        max_bytes: usize,
        allow_cross_file: bool,
    ) -> TransferResult<Vec<SegmentLease>> {
        self.read_handle().select_segments(offset, max_bytes, allow_cross_file)
    }

    pub fn get_data_with_option(
        &self,
        offset: i64,
        return_first_on_not_found: bool,
    ) -> Option<SelectMappedBufferResult> {
        let mapped_file_size = self.message_store_config.mapped_file_size_commit_log as i64;
        let mapped_file = self
            .mapped_file_queue
            .find_mapped_file_by_offset(offset, return_first_on_not_found);
        if let Some(mapped_file) = mapped_file {
            let pos = (offset % mapped_file_size) as i32;
            let mut result = mapped_file.select_mapped_buffer_with_position(pos);
            if let Some(ref mut result) = result {
                result.try_attach_mapped_file(mapped_file);
            }
            result
        } else {
            None
        }
    }

    pub fn check_self(&self) {
        self.mapped_file_queue.check_self();
    }

    pub fn lock_time_mills(&self) -> i64 {
        let begin = self
            .runtime_state
            .begin_time_in_lock()
            .load(std::sync::atomic::Ordering::Acquire);
        if begin > 0 {
            (SystemClock::now() - (begin as u128)) as i64
        } else {
            0
        }
    }

    pub fn begin_time_in_lock(&self) -> &Arc<AtomicU64> {
        self.runtime_state.begin_time_in_lock()
    }

    pub fn remain_how_many_data_to_commit(&self) -> i64 {
        self.mapped_file_queue.remain_how_many_data_to_commit()
    }

    pub fn remain_how_many_data_to_flush(&self) -> i64 {
        self.mapped_file_queue.remain_how_many_data_to_flush()
    }

    pub fn pickup_store_timestamp(&self, offset: i64, size: i32) -> i64 {
        if offset >= self.get_min_offset() && (offset + size as i64) <= self.get_max_offset() {
            let result = self.get_message(offset, size);
            if let Some(result) = result {
                let buffer = result.get_buffer();
                rocketmq_store_local::commit_log::header::store_timestamp_from_frame(buffer)
            } else {
                -1
            }
        } else {
            -1
        }
    }

    /// Get the cold data check service for checking if message data is in cold storage area
    #[inline]
    pub fn get_cold_data_check_service(&self) -> &ColdDataCheckService {
        &self.cold_data_check_service
    }

    pub async fn append_data(
        &mut self,
        start_offset: i64,
        data: &[u8],
        data_start: i32,
        data_length: i32,
    ) -> Result<bool, StoreError> {
        self.replica_handle()
            .append_data(start_offset, data, data_start, data_length)
            .await
    }

    pub fn sync_broker_role(&self, broker_role: BrokerRole) {
        self.store_runtime_state.set_broker_role(broker_role);
    }

    pub fn set_data_read_ahead_enable(&self, enabled: bool) {
        self.store_runtime_state.set_data_read_ahead_enable(enabled);
    }

    pub fn is_data_read_ahead_enable(&self) -> bool {
        self.store_runtime_state.data_read_ahead_enable()
    }

    pub fn scan_file_and_set_read_mode(&self, read_ahead_mode: CommitLogReadMode) -> usize {
        if !self.message_store_config.store_io_hint_enable {
            return 0;
        }

        let advice = match read_ahead_mode {
            CommitLogReadMode::Normal => MemoryAdvice::Normal,
            CommitLogReadMode::Random => MemoryAdvice::Random,
        };

        let mapped_files = self.mapped_file_queue.get_mapped_files();
        let mut updated = 0;
        for mapped_file in mapped_files.iter() {
            if mapped_file.apply_memory_advice(advice).is_ok() {
                updated += 1;
            } else {
                warn!(
                    "failed to apply read mode {} for {}",
                    read_ahead_mode.wire_value(),
                    mapped_file.get_file_name()
                );
            }
        }
        updated
    }
}

pub fn check_message_and_return_size(
    bytes: &mut Bytes,
    check_crc: bool,
    check_dup_info: bool,
    read_body: bool,
    message_store_config: &Arc<MessageStoreConfig>,
    max_delay_level: i32,
    delay_level_table: &BTreeMap<i32 /* level */, i64 /* delay timeMillis */>,
) -> DispatchRequest {
    struct CommonCommitLogChecksum;

    impl CommitLogRecordChecksum for CommonCommitLogChecksum {
        fn checksum(&self, bytes: &[u8]) -> u32 {
            crc32(bytes)
        }
    }

    let force_verify_prop_crc = check_crc && message_store_config.force_verify_prop_crc;
    let body_mode = if !read_body {
        CommitLogRecordBodyMode::Skip
    } else if check_crc && !force_verify_prop_crc {
        CommitLogRecordBodyMode::ReadAndVerify
    } else {
        CommitLogRecordBodyMode::Read
    };
    let record = match decode_commit_log_record(bytes, body_mode, &CommonCommitLogChecksum) {
        Ok(CommitLogRecordOutcome::Blank { .. }) => {
            bytes.advance(8);
            return DispatchRequest {
                msg_size: 0,
                success: true,
                ..Default::default()
            };
        }
        Ok(CommitLogRecordOutcome::Message(record)) => record,
        Err(error) => {
            match error.kind {
                CommitLogRecordErrorKind::IllegalMagic { magic_code } => {
                    warn!("found a illegal magic code 0x{}", format!("{:X}", magic_code));
                }
                CommitLogRecordErrorKind::BodyCrcMismatch { computed, stored } => {
                    warn!("CRC check failed. bodyCRC={}, currentCRC={}", computed, stored);
                }
                CommitLogRecordErrorKind::Truncated {
                    field,
                    needed,
                    remaining,
                } => {
                    warn!(
                        "truncated commitlog record at {:?}: needed={}, remaining={}",
                        field, needed, remaining
                    );
                }
                CommitLogRecordErrorKind::NegativeLength { field, value } => {
                    warn!("negative commitlog record length at {:?}: value={}", field, value);
                }
            }
            return DispatchRequest {
                msg_size: -1,
                success: false,
                ..Default::default()
            };
        }
    };

    let total_size = record.declared_size;
    let body_len = record.body_len;
    let topic_len = record.topic.len();
    let properties_length = record.properties_len;
    let topic = cheetah_from_utf8_lossy(record.topic.as_ref());
    let (tags_code, keys, uniq_key, properties_map) = if properties_length > 0 {
        let properties_content = cheetah_from_utf8_lossy(record.properties.as_ref());
        let properties_map = string_to_message_properties(Some(&properties_content));
        let keys = properties_map.get(MessageConst::PROPERTY_KEYS).cloned();
        let uniq_key = properties_map
            .get(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            .cloned();
        if check_dup_info {
            let dup_info = properties_map.get(MessageConst::DUP_INFO).cloned();
            if let Some(content) = dup_info {
                let vec = content.split_char('_').collect::<Vec<&str>>();
                if vec.len() != 2 {
                    warn!("DupInfo in properties check failed. dupInfo={}", content);
                    return DispatchRequest {
                        msg_size: -1,
                        success: false,
                        ..Default::default()
                    };
                }
            } else {
                warn!("DupInfo in properties check failed. dupInfo=null");
                return DispatchRequest {
                    msg_size: -1,
                    success: false,
                    ..Default::default()
                };
            }
        }
        let tags = properties_map.get(MessageConst::PROPERTY_TAGS);
        let mut tags_code = tags_string2tags_code(tags);

        {
            // Timing message processing
            let delay_time_level = properties_map.get(MessageConst::PROPERTY_DELAY_TIME_LEVEL);
            if let (Some(delay_time_level_str), true) =
                (delay_time_level, TopicValidator::RMQ_SYS_SCHEDULE_TOPIC == topic)
            {
                if let Ok(mut delay_level) = delay_time_level_str.parse::<i32>() {
                    if delay_level > max_delay_level {
                        delay_level = max_delay_level;
                    }
                    if delay_level > 0 {
                        if let Some(delay_time) = delay_level_table.get(&delay_level) {
                            tags_code = *delay_time + record.store_timestamp;
                        } else {
                            tags_code = record.store_timestamp + 1000;
                        }
                    }
                }
            }
        }
        (tags_code, keys.unwrap_or_default(), uniq_key, properties_map)
    } else {
        (0, CheetahString::new(), None, HashMap::new())
    };

    if force_verify_prop_crc {
        match parse_property_crc(&properties_map) {
            Ok(Some(expected_crc)) => {
                if total_size < CRC32_RESERVED_LEN {
                    warn!(
                        "property CRC check failed because total size {} is smaller than reserved CRC length {}",
                        total_size, CRC32_RESERVED_LEN
                    );
                    return DispatchRequest {
                        msg_size: -1,
                        success: false,
                        ..Default::default()
                    };
                }

                let check_size = total_size as usize - CRC32_RESERVED_LEN as usize;
                if record.raw_frame.len() < check_size {
                    warn!(
                        "property CRC check failed because original message length {} is smaller than check size {}",
                        record.raw_frame.len(),
                        check_size
                    );
                    return DispatchRequest {
                        msg_size: -1,
                        success: false,
                        ..Default::default()
                    };
                }

                let current_crc = crc32(&record.raw_frame[..check_size]);
                if current_crc != expected_crc {
                    warn!(
                        "property CRC check failed. expectedCRC={}, currentCRC={}",
                        expected_crc, current_crc
                    );
                    return DispatchRequest {
                        msg_size: -1,
                        success: false,
                        ..Default::default()
                    };
                }
            }
            Ok(None) => {}
            Err(()) => {
                warn!("property CRC check failed because the CRC property is malformed");
                return DispatchRequest {
                    msg_size: -1,
                    success: false,
                    ..Default::default()
                };
            }
        }
    }

    if !record.has_exact_declared_size() {
        error!(
            "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, \
             topicLen={}, propertiesLength={}",
            total_size, record.computed_size, body_len, topic_len, properties_length
        );
        bytes.advance(total_size as usize);
        return DispatchRequest {
            msg_size: total_size,
            success: false,
            ..Default::default()
        };
    }
    let mut dispatch_request = DispatchRequest {
        success: true,
        topic,
        queue_id: record.queue_id,
        commit_log_offset: record.physical_offset,
        msg_size: total_size,
        body_size: body_len,
        tags_code,
        store_timestamp: record.store_timestamp,
        consume_queue_offset: record.queue_offset,
        keys,
        uniq_key,
        sys_flag: record.sys_flag,
        prepared_transaction_offset: record.prepared_transaction_offset,
        ..DispatchRequest::default()
    };
    if !set_batch_size_if_needed(&properties_map, &mut dispatch_request) {
        return DispatchRequest {
            msg_size: -1,
            success: false,
            ..Default::default()
        };
    }
    dispatch_request.properties_map = Some(properties_map);
    bytes.advance(total_size as usize);
    dispatch_request
}

fn set_batch_size_if_needed(
    properties_map: &HashMap<CheetahString, CheetahString>,
    dispatch_request: &mut DispatchRequest,
) -> bool {
    let (Some(inner_base), Some(inner_num)) = (
        properties_map.get(MessageConst::PROPERTY_INNER_BASE),
        properties_map.get(MessageConst::PROPERTY_INNER_NUM),
    ) else {
        return true;
    };

    let msg_base_offset = match inner_base.parse::<i64>() {
        Ok(value) => value,
        Err(error) => {
            warn!(
                "malformed inner batch base offset. {}={}, error={}",
                MessageConst::PROPERTY_INNER_BASE,
                inner_base,
                error
            );
            return false;
        }
    };

    let batch_size = match inner_num.parse::<i16>() {
        Ok(value) if value > 0 => value,
        Ok(value) => {
            warn!(
                "malformed inner batch size. {}={} must be positive",
                MessageConst::PROPERTY_INNER_NUM,
                value
            );
            return false;
        }
        Err(error) => {
            warn!(
                "malformed inner batch size. {}={}, error={}",
                MessageConst::PROPERTY_INNER_NUM,
                inner_num,
                error
            );
            return false;
        }
    };

    dispatch_request.msg_base_offset = msg_base_offset;
    dispatch_request.batch_size = batch_size;
    true
}

impl Swappable for CommitLog {
    fn swap_map(&self, reserve_num: i32, force_swap_interval_ms: i64, normal_swap_interval_ms: i64) {
        self.mapped_file_queue
            .swap_map(reserve_num, force_swap_interval_ms, normal_swap_interval_ms);
    }

    fn clean_swapped_map(&self, force_clean_swap_interval_ms: i64) {
        self.mapped_file_queue.clean_swapped_map(force_clean_swap_interval_ms);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::base::backend_ops::BackendOps;
    use crate::base::memory_lock_manager::MemoryLockCategory;
    use crate::config::message_store_config::LinuxMemoryLockMode;
    use crate::config::message_store_config::LinuxStorageProfile;
    use crate::config::message_store_config::MessageStoreConfig;
    use crate::config::store_runtime_config::StoreRuntimeConfig;
    use crate::message_encoder::message_ext_encoder::MessageExtEncoder;
    use crate::message_store::local_file_message_store::LocalFileMessageStore;
    use bytes::BufMut;
    use bytes::Bytes;
    use bytes::BytesMut;
    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_model::common::config::TopicConfig;
    use rocketmq_model::common::message::MessageTrait;
    use rocketmq_model::utils::crc32_utils::crc32;
    use rocketmq_protocol::common::message::message_decoder::create_crc32;
    use rocketmq_runtime::common::time_utils::current_millis;

    #[test]
    fn put_message_lock_stats_records_totals_and_maxes() {
        let state = CommitLogRuntimeState::new(true, 0);

        state.record_put_message_lock(3, 7);
        state.record_put_message_lock(5, 2);

        let snapshot = state.put_message_lock_runtime_info();
        assert_eq!(snapshot.acquire_total, 2);
        assert_eq!(snapshot.wait_total_millis, 8);
        assert_eq!(snapshot.wait_max_millis, 5);
        assert_eq!(snapshot.hold_total_millis, 9);
        assert_eq!(snapshot.hold_max_millis, 7);
    }

    fn new_test_message_store(
        root: &Path,
        broker_role: BrokerRole,
        all_ack_in_sync_state_set: bool,
    ) -> LocalFileMessageStore {
        new_test_message_store_with_config(
            root,
            MessageStoreConfig::default(),
            broker_role,
            all_ack_in_sync_state_set,
        )
    }

    fn new_test_message_store_with_config(
        root: &Path,
        mut message_store_config: MessageStoreConfig,
        broker_role: BrokerRole,
        all_ack_in_sync_state_set: bool,
    ) -> LocalFileMessageStore {
        std::fs::create_dir_all(root).expect("create temp store dir");
        let broker_config = Arc::new(StoreRuntimeConfig {
            enable_controller_mode: true,
            ..StoreRuntimeConfig::default()
        });
        message_store_config.enable_controller_mode = true;
        message_store_config.broker_role = broker_role;
        message_store_config.all_ack_in_sync_state_set = all_ack_in_sync_state_set;
        message_store_config.timer_wheel_enable = false;
        message_store_config.store_path_root_dir = root.to_string_lossy().into_owned().into();
        let message_store_config = Arc::new(message_store_config);
        let topic_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>> = Arc::new(DashMap::new());
        let mut store = LocalFileMessageStore::new(
            message_store_config,
            broker_config,
            topic_table,
            None,
            false,
            crate::runtime::test_service_context("commit-log-store-test"),
        );
        store
            .wire_owned_root_dependencies()
            .expect("commit-log tests should wire owned Store capabilities");
        store
    }

    fn new_test_mapped_file(root: &Path, offset: u64, file_size: u64) -> DefaultMappedFile {
        fs::create_dir_all(root).expect("create mapped file dir");
        let file_path = root.join(format!("{offset:020}"));
        DefaultMappedFile::new(CheetahString::from(file_path.to_string_lossy().as_ref()), file_size)
    }

    fn append_test_message(topic: &'static str, body: &'static [u8]) -> MessageExtBrokerInner {
        let mut message = MessageExtBrokerInner::default();
        message.set_topic(CheetahString::from_static_str(topic));
        message.message_ext_inner.set_queue_id(0);
        message.set_body(Bytes::from_static(body));
        message.set_wait_store_msg_ok(false);
        message
    }

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|window| window == needle)
    }

    #[test]
    fn cheetah_from_utf8_lossy_borrows_valid_commitlog_text() {
        let decoded = cheetah_from_utf8_lossy(b"commitlog-topic");

        assert_eq!(decoded, "commitlog-topic");
    }

    #[test]
    fn cheetah_from_utf8_lossy_preserves_invalid_commitlog_text_compatibility() {
        let encoded = b"topic-\xff";
        let decoded = cheetah_from_utf8_lossy(encoded);

        assert_eq!(decoded.as_str(), String::from_utf8_lossy(encoded).as_ref());
    }

    fn build_message_with_property_crc(body: &[u8], topic: &str) -> Bytes {
        let total_size = MessageExtEncoder::cal_msg_length(
            MessageVersion::V1,
            0,
            body.len() as i32,
            topic.len() as i32,
            CRC32_RESERVED_LEN,
        ) as usize;
        let crc_start = total_size - CRC32_RESERVED_LEN as usize;
        let mut encoded = BytesMut::with_capacity(total_size);
        encoded.put_i32(total_size as i32);
        encoded.put_i32(MESSAGE_MAGIC_CODE);
        encoded.put_i32(0);
        encoded.put_i32(0);
        encoded.put_i32(0);
        encoded.put_i64(0);
        encoded.put_i64(0);
        encoded.put_i32(0);
        encoded.put_i64(0);
        encoded.put_bytes(0, 8);
        encoded.put_i64(0);
        encoded.put_bytes(0, 8);
        encoded.put_i32(0);
        encoded.put_i64(0);
        encoded.put_i32(body.len() as i32);
        encoded.extend_from_slice(body);
        encoded.put_u8(topic.len() as u8);
        encoded.extend_from_slice(topic.as_bytes());
        encoded.put_i16(CRC32_RESERVED_LEN as i16);
        encoded.put_bytes(0, CRC32_RESERVED_LEN as usize);

        let property_crc = crc32(&encoded[..crc_start]);
        create_crc32(&mut encoded[crc_start..total_size], property_crc);
        encoded.freeze()
    }

    #[tokio::test]
    async fn controller_mode_confirm_offset_prefers_max_offset_when_all_ack_is_disabled() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-commitlog-confirm-offset-{}", current_millis()));
        let mut store = new_test_message_store(&temp_root, BrokerRole::SyncMaster, false);
        store.init().await.expect("init message store");
        let read_handle = store.get_commit_log().read_handle();

        store
            .get_commit_log_mut()
            .append_data(0, &[1, 2, 3, 4], 0, 4)
            .await
            .expect("append data");
        store.set_confirm_offset(1);

        assert_eq!(store.get_confirm_offset(), 4);
        assert_eq!(read_handle.get_confirm_offset(), 4);

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn controller_mode_confirm_offset_keeps_uninitialized_value_when_sync_state_set_exceeds_connections() {
        let temp_root = std::env::temp_dir().join(format!(
            "rocketmq-rust-commitlog-confirm-offset-missing-connection-{}",
            current_millis()
        ));
        let mut store = new_test_message_store(&temp_root, BrokerRole::SyncMaster, true);
        store.init().await.expect("init message store");
        let read_handle = store.get_commit_log().read_handle();
        store.set_alive_replica_num_in_group(3);

        store
            .get_commit_log_mut()
            .append_data(0, &[1, 2, 3, 4], 0, 4)
            .await
            .expect("append data");
        store.get_commit_log_mut().runtime_state.set_confirm_offset(-1);

        assert_eq!(store.get_confirm_offset(), -1);
        assert_eq!(read_handle.get_confirm_offset(), -1);

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn controller_mode_recovery_clamp_persists_confirm_offset() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-commitlog-recovery-clamp-{}", current_millis()));
        let mut store = new_test_message_store(&temp_root, BrokerRole::SyncMaster, true);
        store.init().await.expect("init message store");
        store.set_alive_replica_num_in_group(2);

        store
            .get_commit_log_mut()
            .append_data(0, &[1, 2, 3, 4, 5, 6, 7, 8], 0, 8)
            .await
            .expect("append data");

        store.set_confirm_offset(8);

        store.get_commit_log_mut().clamp_controller_recover_confirm_offset(0, 6);

        assert_eq!(store.get_commit_log().runtime_state.confirm_offset(), 6);
        assert_eq!(store.get_confirm_offset(), 6);

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn replica_handle_shares_append_lock_generation_and_confirm_checkpoint() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-commitlog-replica-handle-{}", current_millis()));
        let mut store = new_test_message_store(&temp_root, BrokerRole::Slave, false);
        store.init().await.expect("init message store");
        let handle = store.get_commit_log().replica_handle();
        assert!(Arc::ptr_eq(
            &handle.put_message_lock,
            &store.get_commit_log().put_message_lock
        ));

        let first = handle.clone();
        let second = handle.clone();
        let (first_result, second_result) = tokio::join!(
            first.append_data(0, &[1, 2, 3, 4], 0, 4),
            second.append_data(4, &[5, 6, 7, 8], 0, 4),
        );
        assert!(first_result.expect("first replica append"));
        assert!(second_result.expect("second replica append"));
        assert_eq!(handle.get_max_offset(), 8);
        assert_eq!(store.get_commit_log().mapped_file_queue.get_mapped_files_size(), 1);

        handle.publish_confirm_offset(6);
        assert_eq!(store.get_commit_log().runtime_state.confirm_offset(), 6);
        assert_eq!(store.get_store_checkpoint().confirm_phy_offset(), 6);

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[test]
    fn auto_in_sync_replicas_clamps_required_acks_between_minimum_and_configured_value() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-commitlog-auto-isr-acks-{}", current_millis()));
        let store = new_test_message_store_with_config(
            &temp_root,
            MessageStoreConfig {
                enable_auto_in_sync_replicas: true,
                in_sync_replicas: 3,
                min_in_sync_replicas: 2,
                ..MessageStoreConfig::default()
            },
            BrokerRole::SyncMaster,
            false,
        );

        assert_eq!(store.get_commit_log().calc_need_ack_nums(1), 2);
        assert_eq!(store.get_commit_log().calc_need_ack_nums(2), 2);
        assert_eq!(store.get_commit_log().calc_need_ack_nums(4), 3);

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[test]
    fn configured_in_sync_replicas_takes_priority_when_auto_in_sync_replicas_is_disabled() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-commitlog-fixed-isr-acks-{}", current_millis()));
        let store = new_test_message_store_with_config(
            &temp_root,
            MessageStoreConfig {
                enable_auto_in_sync_replicas: false,
                in_sync_replicas: 3,
                min_in_sync_replicas: 2,
                ..MessageStoreConfig::default()
            },
            BrokerRole::SyncMaster,
            false,
        );

        assert_eq!(store.get_commit_log().calc_need_ack_nums(1), 3);
        assert_eq!(store.get_commit_log().calc_need_ack_nums(4), 3);

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[test]
    fn active_memory_lock_target_uses_low_latency_profile_effective_mode() {
        let config = MessageStoreConfig {
            linux_storage_optimization_enable: true,
            linux_storage_profile: LinuxStorageProfile::LowLatency,
            linux_memory_lock_mode: LinuxMemoryLockMode::Off,
            linux_memory_lock_active_window_bytes: 1024,
            ..MessageStoreConfig::default()
        };

        let target = CommitLog::active_memory_lock_target_for_config(&config, 0, 4096)
            .expect("low latency profile should enable active window locking");

        assert_eq!(target.category, MemoryLockCategory::CommitLogActiveWindow);
        assert_eq!(target.offset, 0);
        assert_eq!(target.len, 1024);
    }

    #[test]
    fn active_file_memory_lock_lifecycle_unlocks_old_file_before_locking_new_file() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-commitlog-active-file-lock-{}", current_millis()));
        let mut store = new_test_message_store_with_config(
            &temp_root,
            MessageStoreConfig {
                mapped_file_size_commit_log: 4096,
                linux_memory_lock_mode: LinuxMemoryLockMode::ActiveFile,
                linux_memory_lock_budget_bytes: 8192,
                linux_memory_lock_warn_only: true,
                ..MessageStoreConfig::default()
            },
            BrokerRole::SyncMaster,
            false,
        );
        let first = new_test_mapped_file(&temp_root, 0, 4096);
        let second = new_test_mapped_file(&temp_root, 4096, 4096);
        let events = RefCell::new(Vec::<(&'static str, usize)>::new());

        store
            .get_commit_log_mut()
            .ensure_active_mapped_file_locked_with(
                &first,
                |memory| {
                    events.borrow_mut().push(("lock", memory.len()));
                    Ok(())
                },
                |_, len| {
                    events.borrow_mut().push(("unlock", len));
                    Ok(())
                },
            )
            .expect("first active file lock should succeed");
        store
            .get_commit_log_mut()
            .ensure_active_mapped_file_locked_with(
                &second,
                |memory| {
                    events.borrow_mut().push(("lock", memory.len()));
                    Ok(())
                },
                |_, len| {
                    events.borrow_mut().push(("unlock", len));
                    Ok(())
                },
            )
            .expect("second active file lock should succeed");

        assert_eq!(
            events.into_inner(),
            vec![("lock", 4096), ("unlock", 4096), ("lock", 4096)]
        );

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn active_memory_lock_present_tracks_release_fast_path() {
        let temp_root = std::env::temp_dir().join(format!(
            "rocketmq-rust-commitlog-active-lock-fast-path-{}",
            current_millis()
        ));
        let mut store = new_test_message_store_with_config(
            &temp_root,
            MessageStoreConfig {
                mapped_file_size_commit_log: 4096,
                linux_memory_lock_mode: LinuxMemoryLockMode::ActiveFile,
                linux_memory_lock_budget_bytes: 8192,
                linux_memory_lock_warn_only: true,
                ..MessageStoreConfig::default()
            },
            BrokerRole::SyncMaster,
            false,
        );
        let mapped_file = new_test_mapped_file(&temp_root, 0, 4096);
        let events = RefCell::new(Vec::<(&'static str, usize)>::new());

        store
            .get_commit_log_mut()
            .ensure_active_mapped_file_locked_with(
                &mapped_file,
                |memory| {
                    events.borrow_mut().push(("lock", memory.len()));
                    Ok(())
                },
                |_, len| {
                    events.borrow_mut().push(("unlock", len));
                    Ok(())
                },
            )
            .expect("active file lock should succeed");
        assert!(store
            .get_commit_log()
            .runtime_state
            .active_memory_lock_parts()
            .1
            .load(Ordering::Acquire));

        store
            .get_commit_log()
            .release_active_memory_lock_if_present(|_, len| {
                events.borrow_mut().push(("unlock", len));
                Ok(())
            })
            .expect("active file lock release should succeed");
        assert!(!store
            .get_commit_log()
            .runtime_state
            .active_memory_lock_parts()
            .1
            .load(Ordering::Acquire));

        store
            .get_commit_log()
            .release_active_memory_lock_if_present(|_, len| {
                events.borrow_mut().push(("unexpected_unlock", len));
                Ok(())
            })
            .expect("inactive release should be a fast no-op");

        assert_eq!(events.into_inner(), vec![("lock", 4096), ("unlock", 4096)]);

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn failed_active_memory_unlock_retains_handle_for_retry() {
        let temp_root = std::env::temp_dir().join(format!(
            "rocketmq-rust-commitlog-active-lock-retry-{}",
            current_millis()
        ));
        let mut store = new_test_message_store_with_config(
            &temp_root,
            MessageStoreConfig {
                mapped_file_size_commit_log: 4096,
                linux_memory_lock_mode: LinuxMemoryLockMode::ActiveFile,
                linux_memory_lock_budget_bytes: 8192,
                linux_memory_lock_warn_only: true,
                ..MessageStoreConfig::default()
            },
            BrokerRole::SyncMaster,
            false,
        );
        let mapped_file = new_test_mapped_file(&temp_root, 0, 4096);
        let commit_log = store.get_commit_log_mut();
        commit_log
            .ensure_active_mapped_file_locked_with(&mapped_file, |_| Ok(()), |_, _| Ok(()))
            .expect("active file lock should succeed");

        let error = commit_log
            .release_active_memory_lock_if_present(|_, _| {
                Err(rocketmq_error::RocketMQError::StorageLockFailed {
                    path: "retryable active unlock".to_string(),
                })
            })
            .expect_err("warn-only unlock failure must be returned");
        assert!(matches!(
            error,
            rocketmq_error::RocketMQError::StorageLockFailed { path }
                if path == "retryable active unlock"
        ));
        let (active_memory_lock, active_memory_lock_present) = commit_log.runtime_state.active_memory_lock_parts();
        assert!(active_memory_lock_present.load(Ordering::Acquire));
        assert_eq!(active_memory_lock.lock().manager().locked_bytes(), 4096);

        commit_log
            .release_active_memory_lock_if_present(|_, len| {
                assert_eq!(len, 4096);
                Ok(())
            })
            .expect("retry should release the retained handle");
        assert!(!active_memory_lock_present.load(Ordering::Acquire));
        assert_eq!(active_memory_lock.lock().manager().locked_bytes(), 0);

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn shutdown_and_destroy_release_active_lock_after_closing_append_admission() {
        let temp_root = std::env::temp_dir().join(format!(
            "rocketmq-rust-commitlog-active-lock-destroy-{}",
            current_millis()
        ));
        let mut store = new_test_message_store_with_config(
            &temp_root,
            MessageStoreConfig {
                mapped_file_size_commit_log: 4096,
                linux_memory_lock_mode: LinuxMemoryLockMode::ActiveFile,
                linux_memory_lock_budget_bytes: 8192,
                linux_memory_lock_warn_only: true,
                ..MessageStoreConfig::default()
            },
            BrokerRole::SyncMaster,
            false,
        );
        let mapped_file = new_test_mapped_file(&temp_root, 0, 4096);
        let commit_log = store.get_commit_log_mut();
        commit_log
            .ensure_active_mapped_file_locked_with(&mapped_file, |_| Ok(()), |_, _| Ok(()))
            .expect("active file lock should succeed");
        let append_port = commit_log.append_runtime.port();
        let mut unlock_calls = 0;

        let destroyed = commit_log.destroy_with(|manager, handle| {
            assert!(
                append_port.snapshot().closed,
                "append admission must close before unlock"
            );
            manager.unlock_owned_region_with(handle, |_, len| {
                unlock_calls += 1;
                assert_eq!(len, 4096);
                Ok(())
            })
        });

        assert!(destroyed);
        assert_eq!(unlock_calls, 1);
        assert!(!commit_log
            .runtime_state
            .active_memory_lock_parts()
            .1
            .load(Ordering::Acquire));
        assert_eq!(commit_log.mapped_file_queue.get_mapped_files_size(), 0);

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn drop_fallback_retains_failed_handle_and_can_release_it_without_panicking() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-commitlog-active-lock-drop-{}", current_millis()));
        let mut store = new_test_message_store_with_config(
            &temp_root,
            MessageStoreConfig {
                mapped_file_size_commit_log: 4096,
                linux_memory_lock_mode: LinuxMemoryLockMode::ActiveFile,
                linux_memory_lock_budget_bytes: 8192,
                linux_memory_lock_warn_only: true,
                ..MessageStoreConfig::default()
            },
            BrokerRole::SyncMaster,
            false,
        );
        let mapped_file = new_test_mapped_file(&temp_root, 0, 4096);
        let commit_log = store.get_commit_log_mut();
        commit_log
            .ensure_active_mapped_file_locked_with(&mapped_file, |_| Ok(()), |_, _| Ok(()))
            .expect("active file lock should succeed");

        commit_log.release_active_memory_lock_for_drop_with(|manager, handle| {
            manager.unlock_owned_region_with(handle, |_, _| {
                Err(rocketmq_error::RocketMQError::StorageLockFailed {
                    path: "drop retry".to_string(),
                })
            })
        });
        assert!(commit_log
            .runtime_state
            .active_memory_lock_parts()
            .1
            .load(Ordering::Acquire));

        commit_log.release_active_memory_lock_for_drop_with(|manager, handle| {
            manager.unlock_owned_region_with(handle, |_, _| Ok(()))
        });
        assert!(!commit_log
            .runtime_state
            .active_memory_lock_parts()
            .1
            .load(Ordering::Acquire));

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn active_window_memory_lock_reuses_current_window_until_write_position_leaves_it() {
        let temp_root = std::env::temp_dir().join(format!(
            "rocketmq-rust-commitlog-active-window-lock-{}",
            current_millis()
        ));
        let mut store = new_test_message_store_with_config(
            &temp_root,
            MessageStoreConfig {
                mapped_file_size_commit_log: 4096,
                linux_memory_lock_mode: LinuxMemoryLockMode::ActiveWindow,
                linux_memory_lock_active_window_bytes: 1024,
                linux_memory_lock_budget_bytes: 2048,
                linux_memory_lock_warn_only: true,
                ..MessageStoreConfig::default()
            },
            BrokerRole::SyncMaster,
            false,
        );
        let mapped_file = new_test_mapped_file(&temp_root, 0, 4096);
        let events = RefCell::new(Vec::<(&'static str, usize)>::new());

        store
            .get_commit_log_mut()
            .ensure_active_mapped_file_locked_with(
                &mapped_file,
                |memory| {
                    events.borrow_mut().push(("lock", memory.len()));
                    Ok(())
                },
                |_, len| {
                    events.borrow_mut().push(("unlock", len));
                    Ok(())
                },
            )
            .expect("initial active window lock should succeed");
        mapped_file.set_wrote_position(512);
        store
            .get_commit_log_mut()
            .ensure_active_mapped_file_locked_with(
                &mapped_file,
                |memory| {
                    events.borrow_mut().push(("lock", memory.len()));
                    Ok(())
                },
                |_, len| {
                    events.borrow_mut().push(("unlock", len));
                    Ok(())
                },
            )
            .expect("in-window active lock refresh should succeed");

        assert_eq!(events.into_inner(), vec![("lock", 1024)]);

        let _ = fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn shutdown_flushes_pending_commitlog_data() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-commitlog-shutdown-{}", current_millis()));
        let mut store = new_test_message_store_with_config(
            &temp_root,
            MessageStoreConfig {
                mapped_file_size_commit_log: 4096,
                linux_memory_lock_mode: LinuxMemoryLockMode::ActiveFile,
                linux_memory_lock_budget_bytes: 8192,
                linux_memory_lock_warn_only: true,
                ..MessageStoreConfig::default()
            },
            BrokerRole::SyncMaster,
            false,
        );
        store.init().await.expect("init message store");

        store
            .get_commit_log_mut()
            .append_data(0, &[1, 2, 3, 4], 0, 4)
            .await
            .expect("append data");

        assert_eq!(store.get_commit_log().get_flushed_where(), 0);
        assert_eq!(store.get_commit_log().get_max_offset(), 4);

        let mapped_file = new_test_mapped_file(&temp_root, 4096, 4096);
        store
            .get_commit_log_mut()
            .ensure_active_mapped_file_locked_with(&mapped_file, |_| Ok(()), |_, _| Ok(()))
            .expect("active file lock should succeed");
        let append_port = store.get_commit_log().append_runtime.port();
        let mut unlock_calls = 0;

        store
            .get_commit_log_mut()
            .shutdown_gracefully_with(|manager, handle| {
                assert!(
                    append_port.snapshot().closed,
                    "append worker admission must close before unlock"
                );
                manager.unlock_owned_region_with(handle, |_, len| {
                    unlock_calls += 1;
                    assert_eq!(len, 4096);
                    Ok(())
                })
            })
            .await
            .expect("commitlog shutdown flush should succeed");

        assert_eq!(unlock_calls, 1);
        assert_eq!(store.get_commit_log().get_flushed_where(), 4);
        assert_eq!(
            store.get_commit_log().get_flushed_where(),
            store.get_commit_log().get_max_offset()
        );

        let _ = fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn append_sequencer_projects_fifo_offsets_across_segment_rollover() {
        let temp_dir = tempfile::tempdir().expect("create append sequencer temp dir");
        let mut store = new_test_message_store_with_config(
            temp_dir.path(),
            MessageStoreConfig {
                flush_disk_type: FlushDiskType::AsyncFlush,
                mapped_file_size_commit_log: 256,
                ha_listen_port: 0,
                commit_log_micro_batch_max_items: 4,
                commit_log_micro_batch_max_bytes: 4096,
                commit_log_micro_batch_max_wait_micros: 1000,
                ..MessageStoreConfig::default()
            },
            BrokerRole::AsyncMaster,
            false,
        );
        store.init().await.expect("init append sequencer store");
        assert!(store.load().await, "load append sequencer store");
        store.start().await.expect("start append sequencer store");

        let commit_log = store.get_commit_log();
        let (first, second, third) = tokio::join!(
            commit_log.put_message(append_test_message("append-sequencer-rollover", b"first-message-body")),
            commit_log.put_message(append_test_message("append-sequencer-rollover", b"second-message-body")),
            commit_log.put_message(append_test_message("append-sequencer-rollover", b"third-message-body")),
        );
        let results = [first, second, third];

        assert!(results
            .iter()
            .all(|result| result.put_message_status() == PutMessageStatus::PutOk));
        assert_eq!(
            results
                .iter()
                .map(|result| {
                    result
                        .append_message_result()
                        .expect("successful append result")
                        .logics_offset
                })
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        let physical_offsets = results
            .iter()
            .map(|result| {
                result
                    .append_message_result()
                    .expect("successful append result")
                    .wrote_offset
            })
            .collect::<Vec<_>>();
        assert!(physical_offsets.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(
            physical_offsets
                .iter()
                .map(|offset| offset / 256)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                >= 2,
            "three appends should cross the configured CommitLog segment boundary"
        );

        let queue = commit_log.append_sequencer_runtime_info();
        assert_eq!(queue.depth, 0);
        assert_eq!(queue.reserved_count, 0);
        assert_eq!(commit_log.put_message_lock_runtime_info().acquire_total, 1);

        store.shutdown().await;
    }

    #[tokio::test]
    async fn caller_drop_does_not_cancel_admitted_append_and_capacity_stays_bounded() {
        let temp_dir = tempfile::tempdir().expect("create caller-drop temp dir");
        let mut store = new_test_message_store_with_config(
            temp_dir.path(),
            MessageStoreConfig {
                flush_disk_type: FlushDiskType::AsyncFlush,
                mapped_file_size_commit_log: 4096,
                ha_listen_port: 0,
                commit_log_append_queue_capacity: 1,
                commit_log_append_queue_bytes: 4096,
                commit_log_micro_batch_max_items: 1,
                commit_log_micro_batch_max_bytes: 4096,
                commit_log_micro_batch_max_wait_micros: 0,
                ..MessageStoreConfig::default()
            },
            BrokerRole::AsyncMaster,
            false,
        );
        store.init().await.expect("init caller-drop store");
        assert!(store.load().await, "load caller-drop store");
        store.start().await.expect("start caller-drop store");
        let store = Arc::new(store);

        let writer_lock = Arc::clone(&store.get_commit_log().put_message_lock).lock_owned().await;
        let caller_store = Arc::clone(&store);
        let caller = tokio::spawn(async move {
            caller_store
                .get_commit_log()
                .put_message(append_test_message(
                    "append-sequencer-caller-drop",
                    b"admitted-before-caller-drop",
                ))
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if store.get_commit_log().append_sequencer_runtime_info().reserved_count == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first append should retain one bounded queue permit");

        let saturated = store
            .get_commit_log()
            .put_message(append_test_message(
                "append-sequencer-caller-drop",
                b"rejected-while-capacity-is-held",
            ))
            .await;
        assert_eq!(saturated.put_message_status(), PutMessageStatus::OsPageCacheBusy);

        caller.abort();
        let caller_error = match caller.await {
            Ok(_) => panic!("caller task should be cancelled"),
            Err(error) => error,
        };
        assert!(caller_error.is_cancelled());
        drop(writer_lock);

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let commit_log = store.get_commit_log();
                if commit_log.get_max_offset() > 0 && commit_log.append_sequencer_runtime_info().reserved_count == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("admitted append should finish after its caller is dropped");

        let mut store = match Arc::try_unwrap(store) {
            Ok(store) => store,
            Err(_) => panic!("caller-drop test should release every LocalFileMessageStore owner"),
        };
        store.shutdown().await;
    }

    #[tokio::test]
    async fn destroy_removes_commitlog_files_and_resets_offsets() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-commitlog-destroy-{}", current_millis()));
        let mut store = new_test_message_store(&temp_root, BrokerRole::SyncMaster, false);
        store.init().await.expect("init message store");

        store
            .get_commit_log_mut()
            .append_data(0, &[1, 2, 3, 4], 0, 4)
            .await
            .expect("append data");

        let commitlog_dir = PathBuf::from(store.get_message_store_config().get_store_path_commit_log());
        assert!(commitlog_dir.exists());
        assert!(!store.get_commit_log().is_mapped_files_empty());

        store.get_commit_log_mut().destroy();

        assert!(store.get_commit_log().is_mapped_files_empty());
        assert_eq!(store.get_commit_log().get_flushed_where(), 0);
        assert_eq!(store.get_commit_log().get_max_offset(), 0);
        assert!(!commitlog_dir.exists());

        let _ = fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn destroy_outcome_retains_progress_and_file_identity_until_retry_succeeds() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-commitlog-destroy-retry-{}", current_millis()));
        let config = MessageStoreConfig {
            mapped_file_size_commit_log: 64,
            ..MessageStoreConfig::default()
        };
        let mut store = new_test_message_store_with_config(&temp_root, config, BrokerRole::SyncMaster, false);
        store.init().await.expect("init message store");
        let commitlog_dir = PathBuf::from(store.get_message_store_config().get_store_path_commit_log());
        drop(new_test_mapped_file(&commitlog_dir, 0, 64));
        assert!(store.get_commit_log_mut().load());

        let commit_log = store.get_commit_log_mut();
        commit_log.set_mapped_file_queue_offset(4);
        let mapped_file = commit_log.last_mapped_file_for_testing().expect("mapped file");
        assert!(mapped_file.hold());

        assert!(!commit_log.destroy_with_outcome());
        assert_ne!(commit_log.get_flushed_where(), 0);
        assert_ne!(commit_log.mapped_file_queue.get_committed_where(), 0);
        assert!(commit_log
            .last_mapped_file_for_testing()
            .is_some_and(|current| Arc::ptr_eq(&current, &mapped_file)));

        mapped_file.release();
        assert!(commit_log.destroy_with_outcome());
        assert_eq!(commit_log.get_flushed_where(), 0);
        assert_eq!(commit_log.mapped_file_queue.get_committed_where(), 0);

        let _ = fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn empty_recovery_reports_consume_queue_cleanup_failure_and_allows_retry() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-empty-recovery-retry-{}", current_millis()));
        let mut store = new_test_message_store(&temp_root, BrokerRole::SyncMaster, false);
        store.init().await.expect("init message store");

        let topic = CheetahString::from_static_str("PendingRecoveryTopic");
        let queue_root = PathBuf::from(crate::store_path_config_helper::get_store_path_consume_queue(
            store.get_message_store_config().store_path_root_dir.as_str(),
        ));
        let queue_dir = queue_root.join(topic.as_str()).join("0");
        let queue = ConsumeQueueStoreTrait::find_or_create_consume_queue(store.consume_queue_store_mut(), &topic, 0);
        drop(queue);
        fs::create_dir_all(&queue_dir).expect("create consume queue directory");
        let sentinel = queue_dir.join("unknown-entry");
        fs::write(&sentinel, b"retain").expect("create unknown queue entry");

        assert!(!store.get_commit_log_mut().try_recover_normally(0).await);
        assert!(queue_dir.exists());

        fs::remove_file(sentinel).expect("remove unknown queue entry");
        assert!(store.get_commit_log_mut().try_recover_normally(0).await);
        assert!(!queue_dir.exists());

        let _ = fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn recovered_completion_preserves_progress_until_tail_cleanup_succeeds() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-recovery-tail-retry-{}", current_millis()));
        let config = MessageStoreConfig {
            mapped_file_size_commit_log: 64,
            ..MessageStoreConfig::default()
        };
        let mut store = new_test_message_store_with_config(&temp_root, config, BrokerRole::SyncMaster, false);
        store.init().await.expect("init message store");
        let commitlog_dir = PathBuf::from(store.get_message_store_config().get_store_path_commit_log());
        drop(new_test_mapped_file(&commitlog_dir, 0, 64));
        drop(new_test_mapped_file(&commitlog_dir, 64, 64));
        assert!(store.get_commit_log_mut().load());

        let commit_log = store.get_commit_log_mut();
        commit_log.set_mapped_file_queue_offset(128);
        let tail = commit_log.last_mapped_file_for_testing().expect("tail mapped file");
        assert!(tail.hold());
        let completion = CommitLogRecoveryCompletion::Recovered {
            confirm_offset: 32,
            controller_confirm_offset: 32,
            process_offset: 32,
            truncate_consume_queue: false,
        };

        assert!(!apply_recovery_completion!(commit_log, completion, -1));
        assert_eq!(commit_log.get_flushed_where(), 128);
        assert_eq!(commit_log.mapped_file_queue.get_committed_where(), 128);
        assert!(commit_log
            .last_mapped_file_for_testing()
            .is_some_and(|current| Arc::ptr_eq(&current, &tail)));

        tail.release();
        assert!(apply_recovery_completion!(commit_log, completion, -1));
        assert_eq!(commit_log.get_flushed_where(), 32);
        assert_eq!(commit_log.mapped_file_queue.get_committed_where(), 32);
        assert_eq!(commit_log.mapped_file_queue.get_mapped_files().len(), 1);

        let _ = fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn check_message_and_return_size_verifies_property_crc_when_enabled() {
        let message_store_config = Arc::new(MessageStoreConfig {
            enabled_append_prop_crc: true,
            force_verify_prop_crc: true,
            ..MessageStoreConfig::default()
        });
        let body = Bytes::from_static(b"phase3-prop-crc-body");
        let delay_level_table = BTreeMap::new();
        let encoded = build_message_with_property_crc(body.as_ref(), "prop-crc-topic");

        let mut valid_bytes = encoded.clone();
        let dispatch_request = check_message_and_return_size(
            &mut valid_bytes,
            true,
            false,
            false,
            &message_store_config,
            0,
            &delay_level_table,
        );
        assert!(dispatch_request.success);
        assert_eq!(dispatch_request.msg_size as usize, encoded.len());

        let mut corrupted = encoded.to_vec();
        let body_pos = find_subslice(&corrupted, body.as_ref()).expect("body bytes should exist in encoded message");
        corrupted[body_pos] ^= 0x01;
        let mut corrupted_bytes = Bytes::from(corrupted);
        let dispatch_request = check_message_and_return_size(
            &mut corrupted_bytes,
            true,
            false,
            false,
            &message_store_config,
            0,
            &delay_level_table,
        );
        assert!(!dispatch_request.success);
        assert_eq!(dispatch_request.msg_size, -1);
    }

    #[test]
    fn check_message_and_return_size_rejects_malformed_inner_batch_metadata() {
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let delay_level_table = BTreeMap::new();

        for (inner_base, inner_num) in [("not-a-number", "1"), ("0", "not-a-number"), ("0", "0")] {
            let mut msg = MessageExtBrokerInner::default();
            msg.with_version(MessageVersion::V1);
            msg.set_topic(CheetahString::from_static_str("malformed-inner-batch-topic"));
            msg.message_ext_inner.set_queue_id(0);
            msg.set_body(Bytes::from_static(b"malformed-inner-batch-body"));
            msg.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_INNER_BASE),
                CheetahString::from_static_str(inner_base),
            );
            msg.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_INNER_NUM),
                CheetahString::from_static_str(inner_num),
            );

            let (put_message_result, encoded) = encode_message_ext(&msg, &message_store_config);
            assert!(put_message_result.is_none());

            let mut encoded = encoded.freeze();
            let dispatch_request = check_message_and_return_size(
                &mut encoded,
                true,
                false,
                false,
                &message_store_config,
                0,
                &delay_level_table,
            );

            assert!(
                !dispatch_request.success,
                "INNER_BASE={inner_base}, INNER_NUM={inner_num} should be rejected"
            );
            assert_eq!(dispatch_request.msg_size, -1);
        }
    }

    #[tokio::test]
    async fn get_bulk_data_returns_segments_across_mapped_files() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-commitlog-bulk-data-{}", current_millis()));
        let mut store = new_test_message_store_with_config(
            &temp_root,
            MessageStoreConfig {
                mapped_file_size_commit_log: 16,
                ..MessageStoreConfig::default()
            },
            BrokerRole::SyncMaster,
            false,
        );
        store.init().await.expect("init message store");

        let first = *b"0123456789ABCDEF";
        let second = *b"ghijklmnopqrstuv";
        assert!(store
            .get_commit_log_mut()
            .append_data(0, &first, 0, first.len() as i32)
            .await
            .expect("append first chunk"));
        assert!(store
            .get_commit_log_mut()
            .append_data(first.len() as i64, &second, 0, second.len() as i32)
            .await
            .expect("append second chunk"));

        let segments = store
            .get_commit_log()
            .get_bulk_data(12, 8)
            .expect("bulk data across mapped files");
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].start_offset(), 12);
        assert_eq!(segments[0].size(), 4);
        assert_eq!(segments[1].start_offset(), 16);
        assert_eq!(segments[1].size(), 4);

        let combined: Vec<u8> = segments
            .iter()
            .flat_map(|segment| {
                segment
                    .get_bytes_ref()
                    .expect("segment bytes")
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(combined, b"CDEFghij");

        let selection_stats = store.get_commit_log().selection_stats();
        assert_eq!(selection_stats.copied_bytes, 8);
        assert_eq!(selection_stats.compared_bytes, 8);

        let _ = fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn select_segments_caps_single_file_selection_at_mapped_file_boundary() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-commitlog-select-segments-{}", current_millis()));
        let mut store = new_test_message_store_with_config(
            &temp_root,
            MessageStoreConfig {
                mapped_file_size_commit_log: 16,
                ..MessageStoreConfig::default()
            },
            BrokerRole::SyncMaster,
            false,
        );
        store.init().await.expect("init message store");

        let first = *b"0123456789ABCDEF";
        let second = *b"ghijklmnopqrstuv";
        assert!(store
            .get_commit_log_mut()
            .append_data(0, &first, 0, first.len() as i32)
            .await
            .expect("append first chunk"));
        assert!(store
            .get_commit_log_mut()
            .append_data(first.len() as i64, &second, 0, second.len() as i32)
            .await
            .expect("append second chunk"));

        let before = store.get_commit_log().selection_stats();
        let segments = store
            .get_commit_log()
            .select_segments(12, 8, false)
            .expect("single-file transfer segments");

        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].segment().global_offset(), 12);
        assert_eq!(segments[0].segment().position_in_file(), 12);
        assert_eq!(segments[0].len(), 4);
        assert!(segments[0].as_bytes().is_none());
        let file_range = segments[0].as_file_range().expect("file range");
        assert_eq!(file_range.position(), 12);
        assert_eq!(file_range.len(), 4);
        assert_eq!(
            file_range.to_bytes().expect("exact fallback"),
            Bytes::from_static(b"CDEF")
        );
        let after = store.get_commit_log().selection_stats();
        assert_eq!(after.copied_bytes, before.copied_bytes);
        assert_eq!(after.compared_bytes, before.compared_bytes);

        let _ = fs::remove_dir_all(temp_root);
    }
}
