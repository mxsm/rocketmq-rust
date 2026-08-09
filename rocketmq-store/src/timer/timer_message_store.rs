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

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cheetah_string::CheetahString;
use parking_lot::Mutex;
use parking_lot::RwLock;
use rocketmq_model::common::message::message_accessor::MessageAccessor;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::message_single;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_observability::metrics::store::StoreMetricsRecorder;
use rocketmq_observability::metrics::timer::TimerMetricsRecorder;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::common::util_all::is_it_time_to_do;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ShutdownReport;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerStoreMode;
use rocketmq_store_api::JAVA_COMPAT_TIMER_FORMAT_VERSION;
use rocketmq_store_local::timer::metrics::TimerStorageMetrics;
use rocketmq_store_local::timer::metrics::TimerStorageMetricsSnapshot;
use rocketmq_store_local::timer::segmented_timer_log::TIMER_LOG_V2_PHYSICAL_RECORD_SIZE;
use rocketmq_store_local::timer::service::clamp_queue_offset;
use rocketmq_store_local::timer::service::is_due_not_before;
use rocketmq_store_local::timer::service::recover_timer_log_len as plan_recovered_timer_log_len;
use rocketmq_store_local::timer::service::timer_slot_is_valid;
use rocketmq_store_local::timer::service::TimerBacklogMetrics;
use rocketmq_store_local::timer::service::TimerLogRecord;
use rocketmq_store_local::timer::service::TimerSchedulePolicy;
use rocketmq_store_local::timer::service::TimerTpsCounter;
use rocketmq_store_local::timer::storage_format::TimerStorageFingerprint;
use rocketmq_store_local::timer::storage_format::TIMER_LOG_RECORD_VERSION;
use tokio::sync::Mutex as AsyncMutex;
use tracing::error;
use tracing::warn;

use crate::base::message_result::PutMessageResult;
use crate::base::message_status_enum::PutMessageStatus;
use crate::config::message_store_config::MessageStoreConfig;
use crate::log_file::commit_log::CommitLogReadHandle;
use crate::message_store::local_file_message_store::TimerMessageWriteHandle;
use crate::queue::local_file_consume_queue_store::ConsumeQueueLookupHandle;
use crate::queue::ArcConsumeQueue;
use crate::queue::CqUnit;
use crate::store_path_config_helper::get_timer_check_path;
use crate::store_path_config_helper::get_timer_log_path;
use crate::store_path_config_helper::get_timer_metrics_path;
use crate::store_path_config_helper::get_timer_wheel_path;
use crate::timer::clock::SystemTimerClock;
use crate::timer::clock::TimerClock;
use crate::timer::delivery::classify_delivery_status;
use crate::timer::error::CorruptionReason;
use crate::timer::error::QuarantineManifest;
use crate::timer::error::QuarantineRecord;
use crate::timer::error::TimerWorkResult;
use crate::timer::java_compat::JavaCompatEngine;
use crate::timer::pipeline::TimerPipeline;
use crate::timer::pipeline::TimerPipelineDiagnostics;
use crate::timer::role::TimerRoleState;
use crate::timer::slot::Slot;
use crate::timer::slot_drain::slot_drain_generation;
use crate::timer::slot_drain::SlotDrainEntry as TimerLogEntry;
use crate::timer::slot_drain::SlotDrainPlan;
use crate::timer::slot_drain::SlotDrainPlanBuilder;
use crate::timer::slot_drain::DEFAULT_IN_MEMORY_DRAIN_ENTRIES;
use crate::timer::timer_checkpoint::TimerCheckpoint;
use crate::timer::timer_checkpoint::TimerCheckpointSnapshot;
use crate::timer::timer_log::TimerLog;
use crate::timer::timer_metrics::TimerMetrics;
use crate::timer::timer_metrics::TimerMetricsSerializeWrapper;
use crate::timer::timer_wheel::TimerWheel;

pub const TIMER_TOPIC: &str = concat!("rmq_sys_", "wheel_timer");
pub const TIMER_OUT_MS: &str = MessageConst::PROPERTY_TIMER_OUT_MS;
pub const TIMER_ENQUEUE_MS: &str = MessageConst::PROPERTY_TIMER_ENQUEUE_MS;
pub const TIMER_DEQUEUE_MS: &str = MessageConst::PROPERTY_TIMER_DEQUEUE_MS;
pub const TIMER_ROLL_TIMES: &str = MessageConst::PROPERTY_TIMER_ROLL_TIMES;
pub const TIMER_DELETE_UNIQUE_KEY: &str = MessageConst::PROPERTY_TIMER_DEL_UNIQKEY;

pub const PUT_OK: i32 = 0;
pub const PUT_NEED_RETRY: i32 = 1;
pub const PUT_NO_RETRY: i32 = 2;
pub const DAY_SECS: i32 = 24 * 3600;
pub const DEFAULT_CAPACITY: usize = 1024;

// The total days in the timer wheel when precision is 1000ms.
// If the broker shutdown last more than the configured days, will cause message loss
pub const TIMER_WHEEL_TTL_DAY: i32 = 7;
pub const TIMER_BLANK_SLOTS: i32 = 60;
pub const MAGIC_DEFAULT: i32 = 1;
pub const MAGIC_ROLL: i32 = 1 << 1;
pub const MAGIC_DELETE: i32 = 1 << 2;
const EMPTY_TIMER_LOG_POS: i64 = -1;
const MIN_SCHEDULER_INTERVAL_MS: u64 = 100;
const MAX_FUTURE_CURSOR_SKEW_SLOTS: i64 = 2;

#[derive(Clone, Copy, Debug)]
struct RecoveredTimerState {
    read_time_ms: i64,
    queue_offset: i64,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct TimerDeleteIdentity {
    key: CheetahString,
    generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TimerPayloadReadError {
    InvalidLocator,
    Missing,
    ShortRead,
    Decode,
}

struct ActiveSlotDrain {
    plan: SlotDrainPlan,
    delete_keys: HashSet<TimerDeleteIdentity>,
}

struct AtomicActivityGuard<'a> {
    counter: &'a AtomicUsize,
    gate: &'a RwLock<()>,
}

impl<'a> AtomicActivityGuard<'a> {
    fn new(counter: &'a AtomicUsize, gate: &'a RwLock<()>) -> Self {
        let _gate = gate.read();
        counter.fetch_add(1, Ordering::AcqRel);
        Self { counter, gate }
    }
}

impl Drop for AtomicActivityGuard<'_> {
    fn drop(&mut self) {
        let _gate = self.gate.read();
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PersistenceStage {
    BeforeTimerLog,
    AfterTimerLog,
    AfterTimerWheel,
    AfterCheckpoint,
}

#[cfg(test)]
type PersistenceObserver = Arc<dyn Fn(PersistenceStage) -> std::io::Result<()> + Send + Sync>;

/// Narrow Store capabilities used by Timer's active runtime path.
#[derive(Clone)]
pub(crate) struct TimerStoreContext {
    consume_queues: ConsumeQueueLookupHandle,
    commit_log: CommitLogReadHandle,
    message_write: TimerMessageWriteHandle,
}

impl TimerStoreContext {
    pub(crate) fn new(
        consume_queues: ConsumeQueueLookupHandle,
        commit_log: CommitLogReadHandle,
        message_write: TimerMessageWriteHandle,
    ) -> Self {
        Self {
            consume_queues,
            commit_log,
            message_write,
        }
    }

    fn find_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> Option<ArcConsumeQueue> {
        self.consume_queues.find_or_create_consume_queue(topic, queue_id)
    }

    fn look_message_by_offset_with_size(&self, commit_log_offset: i64, size: i32) -> Option<MessageExt> {
        let result = self.commit_log.get_message(commit_log_offset, size)?;
        let mut bytes = result.get_bytes()?;
        MessageDecoder::decode(&mut bytes, true, false, false, false, false)
    }

    async fn put_message(&self, message: MessageExtBrokerInner) -> crate::base::message_result::PutMessageResult {
        self.message_write.put_message(message).await
    }
}

pub struct TimerMessageStore {
    runtime_scope: crate::runtime::StoreRuntimeScope,
    otel_timer_metrics: TimerMetricsRecorder,
    otel_store_metrics: StoreMetricsRecorder,
    pub curr_read_time_ms: AtomicI64,
    pub curr_queue_offset: AtomicI64,
    pub last_enqueue_but_expired_time: u64,
    pub last_enqueue_but_expired_store_time: u64,
    pub timer_metrics: TimerMetrics,
    store_context: Option<TimerStoreContext>,
    message_store_config: Arc<MessageStoreConfig>,
    timer_checkpoint: Mutex<Option<TimerCheckpoint>>,
    timer_log: Mutex<Option<TimerLog>>,
    timer_wheel: Mutex<Option<TimerWheel>>,
    storage_metrics: Arc<TimerStorageMetrics>,
    slot_drains: Mutex<HashMap<i64, ActiveSlotDrain>>,
    clock: RwLock<Arc<dyn TimerClock>>,
    role_state: TimerRoleState,
    should_running_dequeue: AtomicBool,
    enqueue_suspended: AtomicBool,
    process_lock: AsyncMutex<()>,
    enqueue_stage_lock: Mutex<()>,
    due_stage_lock: AsyncMutex<()>,
    checkpoint_lock: Mutex<()>,
    persistence_gate: RwLock<()>,
    active_source_mutations: AtomicUsize,
    active_due_mutations: AtomicUsize,
    pending_progress: AtomicBool,
    scheduler_group: Mutex<Option<rocketmq_runtime::TaskGroup>>,
    scheduler_tasks: Mutex<Option<ScheduledTaskGroup>>,
    pipeline: Mutex<Option<Arc<TimerPipeline>>>,
    quarantine_manifest: QuarantineManifest,
    enqueue_tps_counter: TimerTpsCounter,
    dequeue_tps_counter: TimerTpsCounter,
    #[cfg(test)]
    persistence_observer: Mutex<Option<PersistenceObserver>>,
}

impl TimerMessageStore {
    pub fn load(&self) -> bool {
        if self.message_store_config.timer_store_mode != TimerStoreMode::JavaCompat {
            error!("extended timer timeline is configured but its storage capability is not installed");
            return false;
        }
        if self.message_store_config.timer_skip_unknown_error {
            error!("timerSkipUnknownError is unsupported because timer corruption must fail closed");
            return false;
        }
        let root_dir = self.message_store_config.store_path_root_dir.as_str();
        if let Err(error) = self.quarantine_manifest.load() {
            error!("load timer quarantine manifest failed: {error}");
            return false;
        }
        if let Err(error) = self.message_store_config.timer_policy_snapshot() {
            error!("load timer store failed because timer configuration is invalid: {error}");
            return false;
        }
        if let Err(error) = self.role_state.load() {
            error!("load timer role epoch failed: {error}");
            return false;
        }
        let fingerprint = match self.timer_storage_fingerprint() {
            Ok(fingerprint) => fingerprint,
            Err(error) => {
                error!("load timer storage format failed: {error}");
                return false;
            }
        };
        let format_path = PathBuf::from(root_dir).join("timer-v2").join("FORMAT");
        if let Err(error) = fingerprint.load_or_create(&format_path) {
            error!("load timer storage fingerprint failed: {error}");
            return false;
        }
        let timer_checkpoint =
            match TimerCheckpoint::new_with_policy(get_timer_check_path(root_dir), fingerprint.policy_hash()) {
                Ok(timer_checkpoint) => timer_checkpoint,
                Err(err) => {
                    error!("load timer checkpoint failed: {err}");
                    return false;
                }
            };

        let timer_log = TimerLog::with_metrics(
            get_timer_log_path(root_dir),
            self.message_store_config.mapped_file_size_timer_log,
            Arc::clone(&self.storage_metrics),
        );
        if let Err(err) = timer_log.load() {
            error!("load timer log failed: {err}");
            return false;
        }
        let timer_log_len = match timer_log.durable_length() {
            Ok(length) => length as i64,
            Err(error) => {
                error!("read timer log length during checkpoint selection failed: {error}");
                return false;
            }
        };
        if timer_checkpoint.local_generation() > 0 {
            match timer_checkpoint.select_for_storage(timer_log_len, None) {
                Ok(true) => {}
                Ok(false) => {
                    error!("no V2 timer checkpoint references available durable log data");
                    return false;
                }
                Err(error) => {
                    error!("select timer checkpoint against durable log failed: {error}");
                    return false;
                }
            }
        }

        let timer_wheel = TimerWheel::with_page_size_and_metrics(
            get_timer_wheel_path(root_dir),
            TIMER_WHEEL_TTL_DAY as usize * DAY_SECS as usize,
            self.message_store_config.timer_precision_ms,
            4_096,
            Arc::clone(&self.storage_metrics),
        );
        if let Err(first_error) = timer_wheel.load_at_generation(timer_checkpoint.wheel_generation()) {
            let rejected_generation = timer_checkpoint.local_generation();
            let fallback_loaded = rejected_generation > 0
                && timer_checkpoint
                    .select_for_storage(timer_log_len, Some(rejected_generation))
                    .ok()
                    .unwrap_or(false)
                && timer_wheel
                    .load_at_generation(timer_checkpoint.wheel_generation())
                    .is_ok();
            if !fallback_loaded {
                let rebuilt_slots = match self.rebuild_timer_wheel_from_log(&timer_checkpoint, &timer_log) {
                    Ok(slots) => slots,
                    Err(rebuild_error) => {
                        error!(
                            "load timer wheel failed ({first_error}) and rebuilding from committed timer log failed: \
                             {rebuild_error}"
                        );
                        return false;
                    }
                };
                if let Err(rebuild_error) =
                    timer_wheel.load_rebuilt(timer_checkpoint.wheel_generation(), &rebuilt_slots)
                {
                    error!(
                        "load timer wheel failed ({first_error}) and installing rebuilt pages failed: {rebuild_error}"
                    );
                    return false;
                }
                warn!(
                    "rebuilt {} pending timer slots from the committed timer log because wheel pages were invalid: {}",
                    rebuilt_slots.len(),
                    first_error
                );
            } else {
                warn!(
                    "fallback from timer checkpoint generation {} because its wheel pages are invalid: {}",
                    rejected_generation, first_error
                );
            }
        }

        let recovered_state = match self.recover_and_revise(&timer_checkpoint, &timer_log, &timer_wheel) {
            Ok(recovered_state) => recovered_state,
            Err(err) => {
                error!("recover timer state failed: {err}");
                return false;
            }
        };

        self.curr_read_time_ms
            .store(recovered_state.read_time_ms, Ordering::Relaxed);
        self.curr_queue_offset
            .store(recovered_state.queue_offset, Ordering::Relaxed);
        *self.timer_checkpoint.lock() = Some(timer_checkpoint);
        *self.timer_log.lock() = Some(timer_log);
        *self.timer_wheel.lock() = Some(timer_wheel);
        let _ = self.timer_metrics.load();
        self.refresh_timer_backlog_distribution();
        if self.should_check_and_revise_metrics() {
            self.check_and_revise_metrics();
            if let Err(error) = self.timer_metrics.persist() {
                error!("persist revised timer metrics failed: {error}");
                return false;
            }
        }
        true
    }

    pub fn start(self: &Arc<Self>) {
        let mut scheduler_group_guard = self.scheduler_group.lock();
        if scheduler_group_guard.is_some() {
            return;
        }

        let interval_ms = self
            .message_store_config
            .timer_precision_ms
            .max(MIN_SCHEDULER_INTERVAL_MS);
        let scheduler_group = crate::runtime::task_group(&self.runtime_scope, "rocketmq-store.timer.scheduler");
        let pipeline = match TimerPipeline::new(&self.runtime_scope, &self.message_store_config) {
            Ok(pipeline) => pipeline,
            Err(error) => {
                error!("failed to create TimerMessageStore pipeline: {error}");
                return;
            }
        };
        if let Err(error) = pipeline.spawn(
            &scheduler_group,
            JavaCompatEngine::new(Arc::clone(self)),
            self.message_store_config.timer_put_message_thread_num,
            self.message_store_config.timer_get_message_thread_num,
            self.message_store_config.timer_completion_gap_limit,
        ) {
            error!("failed to spawn TimerMessageStore pipeline: {error}");
            pipeline.close();
            scheduler_group.cancel();
            return;
        }
        let scheduler_tasks = ScheduledTaskGroup::new(scheduler_group.clone());
        let scheduler = Arc::clone(self);
        let scheduled_pipeline = Arc::clone(&pipeline);
        if let Err(error) = scheduler_tasks.schedule_fixed_delay(
            ScheduledTaskConfig::fixed_delay("timer-message-scheduler", Duration::from_millis(interval_ms)),
            move || {
                let scheduler = scheduler.clone();
                let pipeline = Arc::clone(&scheduled_pipeline);
                async move {
                    pipeline.submit_tick(scheduler.capture_delivery_epoch());
                }
            },
        ) {
            error!("failed to spawn TimerMessageStore scheduler: {error}");
            pipeline.close();
            scheduler_group.cancel();
            return;
        }
        *self.pipeline.lock() = Some(pipeline);
        *self.scheduler_tasks.lock() = Some(scheduler_tasks);
        *scheduler_group_guard = Some(scheduler_group);
    }

    pub fn is_reject(&self, deliver_ms: u64) -> bool {
        if self.message_store_config.timer_congest_num_each_slot == 0 {
            return false;
        }
        self.get_timer_wheel_slot(deliver_ms as i64)
            .map(|slot| slot.num as usize > self.message_store_config.timer_congest_num_each_slot)
            .unwrap_or(false)
    }

    pub fn get_dequeue_behind(&self) -> i64 {
        self.get_dequeue_behind_millis() / 1000
    }

    pub fn get_dequeue_behind_millis(&self) -> i64 {
        self.clock
            .read()
            .wall_time_ms()
            .saturating_sub(self.curr_read_time_ms.load(Ordering::Relaxed))
    }

    pub fn get_enqueue_behind_millis(&self) -> i64 {
        if current_millis() - self.last_enqueue_but_expired_time < 2000 {
            ((current_millis() - self.last_enqueue_but_expired_store_time) / 1000) as i64
        } else {
            0
        }
    }

    pub fn get_enqueue_behind(&self) -> i64 {
        self.get_enqueue_behind_millis() / 1000
    }

    pub fn get_enqueue_behind_messages(&self) -> i64 {
        let temp_queue_offset = self.curr_queue_offset.load(Ordering::Relaxed);
        let consume_queue = self.find_store_consume_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0);
        let max_offset_in_queue = match consume_queue {
            Some(queue) => queue.read().get_max_offset_in_queue(),
            None => 0,
        };
        max_offset_in_queue - temp_queue_offset
    }

    pub fn get_all_congest_num(&self) -> i64 {
        self.timer_wheel
            .lock()
            .as_ref()
            .map(|timer_wheel| timer_wheel.get_all_num(self.curr_read_time_ms.load(Ordering::Relaxed)))
            .unwrap_or_default()
    }

    pub fn runtime_backlog_metrics(&self) -> (HashMap<String, i64>, HashMap<i32, i64>) {
        let backlog_metrics = self.collect_backlog_metrics();
        self.timer_metrics
            .replace_timing_distribution_snapshot(backlog_metrics.distribution_snapshot());
        (
            backlog_metrics.topic_snapshot(),
            backlog_metrics.distribution_snapshot(),
        )
    }

    pub fn get_enqueue_tps(&self) -> f32 {
        self.enqueue_tps_counter.get_tps(current_millis() as i64)
    }

    pub fn get_dequeue_tps(&self) -> f32 {
        self.dequeue_tps_counter.get_tps(current_millis() as i64)
    }

    pub fn storage_metrics_snapshot(&self) -> TimerStorageMetricsSnapshot {
        self.storage_metrics.snapshot()
    }

    fn look_messages_by_locator(
        &self,
        locators: &[(i64, i32)],
        max_bytes: usize,
    ) -> Vec<Result<MessageExt, TimerPayloadReadError>> {
        let Some(store_context) = self.store_context.as_ref() else {
            return locators.iter().map(|_| Err(TimerPayloadReadError::Missing)).collect();
        };
        let mut output = Vec::with_capacity(locators.len());
        let mut cursor = 0usize;
        let mut retained_bytes = 0usize;
        while cursor < locators.len() {
            let (start, first_size) = locators[cursor];
            let Ok(first_size) = usize::try_from(first_size) else {
                output.push(Err(TimerPayloadReadError::InvalidLocator));
                cursor += 1;
                continue;
            };
            if start < 0 || first_size == 0 || retained_bytes.saturating_add(first_size) > max_bytes {
                output.push(Err(TimerPayloadReadError::InvalidLocator));
                cursor += 1;
                continue;
            }

            let run_start = cursor;
            let mut run_bytes = first_size;
            let mut expected_next = start.saturating_add(first_size as i64);
            cursor += 1;
            while cursor < locators.len() {
                let (next_offset, next_size) = locators[cursor];
                let Ok(next_size) = usize::try_from(next_size) else {
                    break;
                };
                if next_offset != expected_next
                    || next_size == 0
                    || retained_bytes.saturating_add(run_bytes).saturating_add(next_size) > max_bytes
                    || run_bytes.saturating_add(next_size) > i32::MAX as usize
                {
                    break;
                }
                run_bytes += next_size;
                expected_next = expected_next.saturating_add(next_size as i64);
                cursor += 1;
            }

            let Some(segments) = store_context.commit_log.get_bulk_data(start, run_bytes as i32) else {
                output.extend((run_start..cursor).map(|_| Err(TimerPayloadReadError::Missing)));
                retained_bytes = retained_bytes.saturating_add(run_bytes);
                continue;
            };
            let mut contiguous = Vec::with_capacity(run_bytes);
            for segment in segments {
                contiguous.extend_from_slice(segment.get_buffer());
            }
            if contiguous.len() != run_bytes {
                output.extend((run_start..cursor).map(|_| Err(TimerPayloadReadError::ShortRead)));
                retained_bytes = retained_bytes.saturating_add(run_bytes);
                continue;
            }
            let mut relative = 0usize;
            for (_, size) in &locators[run_start..cursor] {
                let size = *size as usize;
                let mut bytes = Bytes::copy_from_slice(&contiguous[relative..relative + size]);
                output.push(
                    MessageDecoder::decode(&mut bytes, true, false, false, false, false)
                        .ok_or(TimerPayloadReadError::Decode),
                );
                relative += size;
            }
            retained_bytes = retained_bytes.saturating_add(run_bytes);
        }
        output
    }

    /// Returns the immutable normalization policy fingerprint written at timer admission.
    pub fn normalization_policy_fingerprint(&self) -> Option<u64> {
        self.message_store_config.timer_policy_fingerprint().ok()
    }

    pub fn is_should_running_dequeue(&self) -> bool {
        self.should_running_dequeue.load(Ordering::Acquire) && self.role_state.is_active()
    }

    pub fn timer_metrics_wrapper(&self) -> TimerMetricsSerializeWrapper {
        self.timer_metrics.to_wrapper()
    }

    pub fn timer_metrics_payload(&self) -> Vec<u8> {
        self.timer_metrics.encode().into_bytes()
    }

    pub fn timer_checkpoint_payload(&self) -> Option<Vec<u8>> {
        self.timer_checkpoint
            .lock()
            .as_ref()
            .map(|timer_checkpoint| timer_checkpoint.snapshot().encode())
    }

    pub fn timer_checkpoint_snapshot(&self) -> Option<TimerCheckpointSnapshot> {
        self.timer_checkpoint
            .lock()
            .as_ref()
            .map(|timer_checkpoint| timer_checkpoint.snapshot())
    }

    pub fn sync_checkpoint_from_master(&self, snapshot: &TimerCheckpointSnapshot) -> std::io::Result<bool> {
        let _checkpoint_guard = self.checkpoint_lock.lock();
        let _persistence_guard = self.persistence_gate.write();
        if self.active_source_mutations.load(Ordering::Acquire) > 0
            || self.active_due_mutations.load(Ordering::Acquire) > 0
            || !self.slot_drains.lock().is_empty()
        {
            return Ok(false);
        }
        let checkpoint_guard = self.timer_checkpoint.lock();
        let Some(timer_checkpoint) = checkpoint_guard.as_ref() else {
            return Ok(false);
        };
        timer_checkpoint.sync_from_master_snapshot(snapshot);
        let caught_up = self.curr_queue_offset.load(Ordering::Relaxed) >= snapshot.master_timer_queue_offset();
        if caught_up {
            timer_checkpoint.set_last_read_time_ms(snapshot.last_read_time_ms());
        }
        timer_checkpoint.flush()?;
        Ok(true)
    }

    /// Creates a standalone Timer store from immutable configuration.
    ///
    /// Store-owned Timer instances should instead be created by
    /// [`crate::message_store::local_file_message_store::LocalFileMessageStore::wire_owned_root_dependencies`],
    /// which injects
    /// narrow queue, CommitLog-read, and message-write capabilities.
    pub fn new_with_message_store_config(
        message_store_config: Arc<MessageStoreConfig>,
        service_context: rocketmq_runtime::ChildServiceContext,
    ) -> Self {
        Self::new_with_runtime_scope(
            message_store_config,
            crate::runtime::StoreRuntimeScope::new(service_context),
        )
    }

    fn new_with_runtime_scope(
        message_store_config: Arc<MessageStoreConfig>,
        runtime_scope: crate::runtime::StoreRuntimeScope,
    ) -> Self {
        Self::new_with_runtime_scope_and_telemetry(
            message_store_config,
            runtime_scope,
            TimerMetricsRecorder::noop(),
            StoreMetricsRecorder::noop(),
        )
    }

    fn new_with_runtime_scope_and_telemetry(
        message_store_config: Arc<MessageStoreConfig>,
        runtime_scope: crate::runtime::StoreRuntimeScope,
        otel_timer_metrics: TimerMetricsRecorder,
        otel_store_metrics: StoreMetricsRecorder,
    ) -> Self {
        let timer_metrics_path = get_timer_metrics_path(message_store_config.store_path_root_dir.as_str());
        let role_state = TimerRoleState::new(message_store_config.store_path_root_dir.as_str());
        let quarantine_manifest = QuarantineManifest::new(message_store_config.store_path_root_dir.as_str());
        Self {
            runtime_scope,
            otel_timer_metrics,
            otel_store_metrics,
            curr_read_time_ms: AtomicI64::new(0),
            curr_queue_offset: AtomicI64::new(0),
            last_enqueue_but_expired_time: 0,
            last_enqueue_but_expired_store_time: 0,
            timer_metrics: TimerMetrics::new(Some(timer_metrics_path)),
            store_context: None,
            message_store_config,
            timer_checkpoint: Mutex::new(None),
            timer_log: Mutex::new(None),
            timer_wheel: Mutex::new(None),
            storage_metrics: Arc::new(TimerStorageMetrics::default()),
            slot_drains: Mutex::new(HashMap::new()),
            clock: RwLock::new(Arc::new(SystemTimerClock::default())),
            role_state,
            should_running_dequeue: AtomicBool::new(false),
            enqueue_suspended: AtomicBool::new(false),
            process_lock: AsyncMutex::new(()),
            enqueue_stage_lock: Mutex::new(()),
            due_stage_lock: AsyncMutex::new(()),
            checkpoint_lock: Mutex::new(()),
            persistence_gate: RwLock::new(()),
            active_source_mutations: AtomicUsize::new(0),
            active_due_mutations: AtomicUsize::new(0),
            pending_progress: AtomicBool::new(false),
            scheduler_group: Mutex::new(None),
            scheduler_tasks: Mutex::new(None),
            pipeline: Mutex::new(None),
            quarantine_manifest,
            enqueue_tps_counter: TimerTpsCounter::default(),
            dequeue_tps_counter: TimerTpsCounter::default(),
            #[cfg(test)]
            persistence_observer: Mutex::new(None),
        }
    }

    #[cfg(test)]
    fn set_clock(&self, clock: Arc<dyn TimerClock>) {
        *self.clock.write() = clock;
    }

    pub(crate) fn new_with_store_context(
        store_context: TimerStoreContext,
        message_store_config: Arc<MessageStoreConfig>,
        runtime_scope: crate::runtime::StoreRuntimeScope,
        otel_timer_metrics: TimerMetricsRecorder,
        otel_store_metrics: StoreMetricsRecorder,
    ) -> Self {
        let mut store = Self::new_with_runtime_scope_and_telemetry(
            message_store_config,
            runtime_scope,
            otel_timer_metrics,
            otel_store_metrics,
        );
        store.store_context = Some(store_context);
        store
    }

    pub fn new_empty(service_context: rocketmq_runtime::ChildServiceContext) -> Self {
        Self::new_with_message_store_config(Arc::new(MessageStoreConfig::default()), service_context)
    }

    fn find_store_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> Option<ArcConsumeQueue> {
        self.store_context
            .as_ref()
            .and_then(|context| context.find_consume_queue(topic, queue_id))
    }

    fn look_store_message(&self, commit_log_offset: i64, size: i32) -> Option<MessageExt> {
        self.store_context
            .as_ref()
            .and_then(|context| context.look_message_by_offset_with_size(commit_log_offset, size))
    }

    async fn put_store_message(&self, message: MessageExtBrokerInner) -> PutMessageResult {
        match self.store_context.as_ref() {
            Some(context) => context.put_message(message).await,
            None => PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable),
        }
    }

    pub fn shutdown(&self) {
        if let Some(pipeline) = self.pipeline.lock().take() {
            pipeline.close();
        }
        self.scheduler_tasks.lock().take();
        if let Some(scheduler_group) = self.scheduler_group.lock().take() {
            let _ = scheduler_group.shutdown_now();
        }
        self.shutdown_storage();
    }

    pub async fn shutdown_gracefully(&self) {
        let _ = self.shutdown_gracefully_with_report().await;
    }

    pub async fn shutdown_gracefully_with_report(&self) -> Option<ShutdownReport> {
        if let Some(pipeline) = self.pipeline.lock().take() {
            pipeline.close();
        }
        self.scheduler_tasks.lock().take();
        let scheduler_group = self.scheduler_group.lock().take();
        if let Some(scheduler_group) = scheduler_group {
            let report = scheduler_group.shutdown(Duration::from_secs(5)).await;
            if let Err(error) = crate::runtime::shutdown_report_result("TimerMessageStore scheduler", report.clone()) {
                warn!("TimerMessageStore scheduler failed during shutdown: {error}");
                self.shutdown_storage();
                return None;
            }
            self.shutdown_storage();
            return Some(report);
        }
        self.shutdown_storage();
        None
    }

    #[cfg(test)]
    pub(crate) fn has_scheduler_handle(&self) -> bool {
        self.scheduler_group.lock().is_some()
    }

    pub(crate) fn scheduler_task_count(&self) -> usize {
        let root_count = self
            .scheduler_group
            .lock()
            .as_ref()
            .map(rocketmq_runtime::TaskGroup::task_count)
            .unwrap_or_default();
        let scheduled_count = self
            .scheduler_tasks
            .lock()
            .as_ref()
            .map(|scheduler_tasks| scheduler_tasks.group().task_count())
            .unwrap_or_default();
        // ScheduledTaskGroup is backed by the same root TaskGroup; summing both snapshots counts
        // every scheduled task twice.
        root_count.max(scheduled_count)
    }

    pub(crate) fn scheduler_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.scheduler_tasks
            .lock()
            .as_ref()
            .map(ScheduledTaskGroup::snapshot)
            .unwrap_or_default()
    }

    fn shutdown_storage(&self) {
        self.sync_last_read_time_ms();
        if let Err(error) = self.timer_metrics.persist() {
            error!("persist timer metrics during shutdown failed: {error}");
        }
        if let Some(timer_log) = self.timer_log.lock().take() {
            let _ = timer_log.shutdown();
        }
        if let Some(timer_wheel) = self.timer_wheel.lock().take() {
            let _ = timer_wheel.shutdown(true);
        }
        if let Some(timer_checkpoint) = self.timer_checkpoint.lock().take() {
            let _ = timer_checkpoint.shutdown();
        }
    }

    pub fn sync_last_read_time_ms(&self) {
        if let Err(error) = self.commit_durable_progress() {
            error!("commit durable timer progress failed: {error}");
        }
    }

    fn commit_durable_progress(&self) -> std::io::Result<bool> {
        let _checkpoint_guard = self.checkpoint_lock.lock();
        let _persistence_guard = self.persistence_gate.write();
        // A partial slot depends on the complete tombstone summary built from its original chain.
        // Until that slot is drained, keep the previous checkpoint authoritative. A crash may
        // replay already delivered entries (with the same delivery token), but cannot lose a
        // tombstone by publishing only the remaining suffix.
        if self.active_source_mutations.load(Ordering::Acquire) > 0
            || self.active_due_mutations.load(Ordering::Acquire) > 0
            || !self.slot_drains.lock().is_empty()
        {
            return Ok(false);
        }
        // The checkpoint is a commit record, never a write-ahead declaration. Advancing it before
        // either data file is durable can permanently skip Timer CQ entries after a crash.
        self.observe_persistence_stage(PersistenceStage::BeforeTimerLog)?;
        let durable_timer_log_len = {
            let timer_log_guard = self.timer_log.lock();
            match timer_log_guard.as_ref() {
                Some(timer_log) => {
                    timer_log.flush()?;
                    timer_log.len()? as i64
                }
                None => 0,
            }
        };
        self.observe_persistence_stage(PersistenceStage::AfterTimerLog)?;

        let wheel_generation = match self.timer_wheel.lock().as_ref() {
            Some(timer_wheel) => timer_wheel.flush_generation()?,
            None => 0,
        };
        self.observe_persistence_stage(PersistenceStage::AfterTimerWheel)?;

        let active_drain_cursor = self
            .slot_drains
            .lock()
            .iter()
            .min_by_key(|(slot_time, _)| *slot_time)
            .map(|(_, active)| {
                (
                    active.plan.generation(),
                    (active.plan.cursor() / DEFAULT_IN_MEMORY_DRAIN_ENTRIES) as u32,
                    (active.plan.cursor() % DEFAULT_IN_MEMORY_DRAIN_ENTRIES) as u32,
                )
            });
        let drain_cursor = active_drain_cursor.or_else(|| {
            let slot_time_ms = self.curr_read_time_ms.load(Ordering::Acquire);
            self.get_timer_wheel_slot(slot_time_ms)
                .filter(|slot| slot.num > 0)
                .map(|slot| (slot_drain_generation(slot), 0, 0))
        });
        let checkpoint_generation = if let Some(timer_checkpoint) = self.timer_checkpoint.lock().as_ref() {
            let queue_offset = self.curr_queue_offset.load(Ordering::Acquire);
            let enqueue_suspended = self.enqueue_suspended.load(Ordering::Acquire);
            let master_queue_offset = if enqueue_suspended {
                timer_checkpoint.master_timer_queue_offset()
            } else {
                queue_offset
            };
            timer_checkpoint.set_last_read_time_ms(self.curr_read_time_ms.load(Ordering::Acquire));
            timer_checkpoint.set_last_timer_queue_offset(queue_offset.min(master_queue_offset));
            timer_checkpoint.set_master_timer_queue_offset(master_queue_offset);
            timer_checkpoint.set_last_timer_log_flush_pos(durable_timer_log_len);
            timer_checkpoint.set_wheel_generation(wheel_generation);
            timer_checkpoint.set_role_epoch(self.role_state.epoch());
            if let Some((generation, page, record)) = drain_cursor {
                timer_checkpoint.set_drain_cursor(generation, page, record);
            } else {
                timer_checkpoint.set_drain_cursor(0, 0, 0);
            }
            timer_checkpoint.flush()?;
            timer_checkpoint.local_generation()
        } else {
            0
        };
        if let Some(timer_wheel) = self.timer_wheel.lock().as_ref() {
            timer_wheel.commit_generation(wheel_generation)?;
        }
        if checkpoint_generation > 0 {
            if let Some(timer_log) = self.timer_log.lock().as_ref() {
                timer_log.mark_clean_checkpoint(checkpoint_generation)?;
            }
        }
        self.observe_persistence_stage(PersistenceStage::AfterCheckpoint)?;
        self.pending_progress.store(false, Ordering::Release);
        Ok(true)
    }

    fn observe_persistence_stage(&self, stage: PersistenceStage) -> std::io::Result<()> {
        #[cfg(test)]
        if let Some(observer) = self.persistence_observer.lock().as_ref() {
            observer(stage)?;
        }
        #[cfg(not(test))]
        let _ = stage;
        Ok(())
    }

    pub fn set_should_running_dequeue(&self, should_start: bool) {
        let previous = self.should_running_dequeue.load(Ordering::Acquire);
        if previous == should_start {
            return;
        }

        if should_start {
            self.restore_progress_on_dequeue_resume();
            self.enqueue_suspended.store(false, Ordering::Relaxed);
            match self.role_state.transition(true) {
                Ok(_) => self.should_running_dequeue.store(true, Ordering::Release),
                Err(error) => {
                    self.should_running_dequeue.store(false, Ordering::Release);
                    error!("keep timer delivery stopped because activating role epoch failed: {error}");
                }
            }
        } else if previous {
            self.should_running_dequeue.store(false, Ordering::Release);
            if let Err(error) = self.role_state.transition(false) {
                error!("persist timer role downgrade epoch failed: {error}");
            }
            self.enqueue_suspended.store(true, Ordering::Relaxed);
            self.sync_last_read_time_ms();
        }
    }

    pub fn append_timer_log(&self, payload: &[u8]) -> std::io::Result<u64> {
        self.timer_log
            .lock()
            .as_ref()
            .ok_or_else(|| std::io::Error::other("timer log is not loaded"))?
            .append(payload)
    }

    pub fn read_timer_log(&self, offset: u64, length: usize) -> std::io::Result<Vec<u8>> {
        self.timer_log
            .lock()
            .as_ref()
            .ok_or_else(|| std::io::Error::other("timer log is not loaded"))?
            .read_at(offset, length)
    }

    pub fn put_timer_wheel_slot(
        &self,
        time_ms: i64,
        first_pos: i64,
        last_pos: i64,
        num: i32,
        magic: i32,
    ) -> std::io::Result<()> {
        self.timer_wheel
            .lock()
            .as_ref()
            .ok_or_else(|| std::io::Error::other("timer wheel is not loaded"))?
            .put_slot(time_ms, first_pos, last_pos, num, magic)
    }

    pub fn get_timer_wheel_slot(&self, time_ms: i64) -> Option<Slot> {
        self.timer_wheel
            .lock()
            .as_ref()
            .and_then(|timer_wheel| timer_wheel.get_slot(time_ms))
    }

    fn restore_progress_on_dequeue_resume(&self) {
        let timer_checkpoint_guard = self.timer_checkpoint.lock();
        let Some(timer_checkpoint) = timer_checkpoint_guard.as_ref() else {
            return;
        };
        let master_queue_offset = timer_checkpoint.master_timer_queue_offset().max(0);
        let restored_queue_offset = self.curr_queue_offset.load(Ordering::Relaxed).min(master_queue_offset);
        self.curr_queue_offset.store(restored_queue_offset, Ordering::Relaxed);

        let last_read_time_ms = timer_checkpoint.last_read_time_ms();
        if last_read_time_ms > 0 {
            let now_floor = self.floor_time_ms(self.clock.read().wall_time_ms());
            self.curr_read_time_ms
                .store(self.floor_time_ms(last_read_time_ms).min(now_floor), Ordering::Relaxed);
        }
    }

    fn should_check_and_revise_metrics(&self) -> bool {
        if !self.message_store_config.timer_enable_check_metrics
            || self.message_store_config.timer_metric_small_threshold == 0
        {
            return false;
        }

        let when = self.message_store_config.timer_check_metrics_when.as_str();
        when.is_empty() || is_it_time_to_do(when)
    }

    fn refresh_timer_backlog_distribution(&self) {
        let backlog_metrics = self.collect_backlog_metrics();
        self.timer_metrics
            .replace_timing_distribution_snapshot(backlog_metrics.distribution_snapshot());
    }

    fn check_and_revise_metrics(&self) {
        let threshold = self.message_store_config.timer_metric_small_threshold as i64;
        if threshold <= 0 {
            return;
        }

        let backlog_metrics = self.collect_backlog_metrics();
        let actual_topic_backlog = backlog_metrics.topic_snapshot();
        let current_topic_backlog = self.timer_metrics.get_timing_count_snapshot();
        let mut revised_topic_backlog = current_topic_backlog.clone();
        let mut candidate_topics = HashSet::new();

        for (topic, count) in &current_topic_backlog {
            if *count < threshold {
                candidate_topics.insert(topic.clone());
            }
        }
        for (topic, count) in &actual_topic_backlog {
            if *count < threshold {
                candidate_topics.insert(topic.clone());
            }
        }

        for topic in candidate_topics {
            match actual_topic_backlog.get(&topic).copied().unwrap_or_default() {
                0 => {
                    revised_topic_backlog.remove(&topic);
                }
                count => {
                    revised_topic_backlog.insert(topic, count);
                }
            }
        }

        if revised_topic_backlog != current_topic_backlog {
            self.timer_metrics.replace_timing_count_snapshot(revised_topic_backlog);
        }
        self.timer_metrics
            .replace_timing_distribution_snapshot(backlog_metrics.distribution_snapshot());
    }

    fn collect_backlog_metrics(&self) -> TimerBacklogMetrics {
        let now_ms = self.floor_time_ms(current_millis() as i64);
        let mut backlog_metrics = TimerBacklogMetrics::new(self.timer_metrics.timer_dist_list());
        self.collect_unindexed_timer_queue_backlog(now_ms, &mut backlog_metrics);
        self.collect_indexed_timer_wheel_backlog(now_ms, &mut backlog_metrics);
        backlog_metrics
    }

    fn collect_unindexed_timer_queue_backlog(&self, now_ms: i64, backlog_metrics: &mut TimerBacklogMetrics) {
        let Some(consume_queue) = self.find_store_consume_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0) else {
            return;
        };

        let max_offset = consume_queue.read().get_max_offset_in_queue();
        let mut queue_offset = self.curr_queue_offset.load(Ordering::Relaxed);
        while queue_offset < max_offset {
            let Some(cq_unit) = consume_queue.read().get(queue_offset) else {
                break;
            };
            let Some(message) = self.look_store_message(cq_unit.pos, cq_unit.size) else {
                warn!(
                    "skip backlog metrics for timer queue offset {} because commitlog message {}:{} is missing",
                    queue_offset, cq_unit.pos, cq_unit.size
                );
                queue_offset = cq_unit.queue_offset + 1;
                continue;
            };
            observe_backlog_message(backlog_metrics, &message, now_ms);
            queue_offset = cq_unit.queue_offset + 1;
        }
    }

    fn collect_indexed_timer_wheel_backlog(&self, now_ms: i64, backlog_metrics: &mut TimerBacklogMetrics) {
        let read_cursor = self.curr_read_time_ms.load(Ordering::Relaxed);
        let slots = self
            .timer_wheel
            .lock()
            .as_ref()
            .map(TimerWheel::slots_snapshot)
            .unwrap_or_default();

        for slot in slots {
            if slot.num <= 0 || slot.time_ms <= 0 || slot.time_ms < read_cursor {
                continue;
            }
            let entries = match self.load_slot_entries(slot) {
                Ok(entries) => entries,
                Err(err) => {
                    warn!(
                        "skip backlog metrics for timer slot {} because loading entries failed: {}",
                        slot.time_ms, err
                    );
                    continue;
                }
            };

            for entry in entries {
                let Some(message) = self.look_store_message(entry.record.commit_log_offset, entry.record.size) else {
                    warn!(
                        "skip backlog metrics for timer log position {} because commitlog message {}:{} is missing",
                        entry.position, entry.record.commit_log_offset, entry.record.size
                    );
                    continue;
                };
                observe_backlog_message(backlog_metrics, &message, now_ms);
            }
        }
    }

    fn recover_and_revise(
        &self,
        timer_checkpoint: &TimerCheckpoint,
        timer_log: &TimerLog,
        timer_wheel: &TimerWheel,
    ) -> std::io::Result<RecoveredTimerState> {
        let recovered_log_len = self.recover_timer_log_len(timer_checkpoint, timer_log)?;
        let recovered_read_time_ms = self.recover_read_time_ms(timer_checkpoint.last_read_time_ms());
        let recovered_queue_offset = self.recover_queue_offset(timer_checkpoint.last_timer_queue_offset());
        self.repair_timer_wheel(timer_wheel, recovered_log_len)?;
        self.persist_recovered_state(
            timer_checkpoint,
            timer_log,
            timer_wheel,
            recovered_read_time_ms,
            recovered_queue_offset,
            recovered_log_len,
        )?;
        Ok(RecoveredTimerState {
            read_time_ms: recovered_read_time_ms,
            queue_offset: recovered_queue_offset,
        })
    }

    fn recover_timer_log_len(&self, timer_checkpoint: &TimerCheckpoint, timer_log: &TimerLog) -> std::io::Result<i64> {
        let current_len = timer_log.len()? as i64;
        let checkpoint_len = timer_checkpoint.last_timer_log_flush_pos();
        let target_len = plan_recovered_timer_log_len(current_len, checkpoint_len);
        if target_len != current_len {
            warn!(
                "revise timer log from {} to {} based on checkpoint flush position {}",
                current_len, target_len, checkpoint_len
            );
            timer_log.truncate(target_len as u64)?;
        }
        Ok(target_len)
    }

    fn recover_read_time_ms(&self, checkpoint_read_time_ms: i64) -> i64 {
        self.timer_policy()
            .recover_read_time_ms(checkpoint_read_time_ms, self.clock.read().wall_time_ms())
    }

    fn recover_queue_offset(&self, checkpoint_queue_offset: i64) -> i64 {
        let consume_queue = self.find_store_consume_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0);
        let Some(consume_queue) = consume_queue else {
            return clamp_queue_offset(checkpoint_queue_offset, None, None);
        };
        let consume_queue = consume_queue.read();
        let min_offset = consume_queue.get_min_offset_in_queue();
        let max_offset = consume_queue.get_max_offset_in_queue();
        let recovered_offset = clamp_queue_offset(checkpoint_queue_offset, Some(min_offset), Some(max_offset));
        if recovered_offset == min_offset && checkpoint_queue_offset < min_offset {
            warn!(
                "revise timer queue offset from {} to consume queue min {}",
                checkpoint_queue_offset, min_offset
            );
        } else if recovered_offset == max_offset && checkpoint_queue_offset > max_offset {
            warn!(
                "revise timer queue offset from {} to consume queue max {}",
                checkpoint_queue_offset, max_offset
            );
        }
        recovered_offset
    }

    fn repair_timer_wheel(&self, timer_wheel: &TimerWheel, recovered_log_len: i64) -> std::io::Result<()> {
        timer_wheel.revise_slots(|slot| {
            if slot.num <= 0 {
                return Slot::new_with_num_magic(0, 0, 0, 0, 0);
            }
            if !timer_slot_is_valid(slot, recovered_log_len) {
                warn!(
                    "clear invalid timer wheel slot time={} first={} last={} num={} against log len {}",
                    slot.time_ms, slot.first_pos, slot.last_pos, slot.num, recovered_log_len
                );
                Slot::new_with_num_magic(0, 0, 0, 0, 0)
            } else {
                slot
            }
        })
    }

    fn rebuild_timer_wheel_from_log(
        &self,
        timer_checkpoint: &TimerCheckpoint,
        timer_log: &TimerLog,
    ) -> std::io::Result<Vec<Slot>> {
        const REBUILD_BATCH_RECORDS: usize = 4_096;

        let committed_length = u64::try_from(timer_checkpoint.last_timer_log_flush_pos()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "timer checkpoint has a negative committed log length",
            )
        })?;
        let read_slot_ms = timer_checkpoint.last_read_time_ms();
        let mut records_by_slot = HashMap::<i64, Vec<(i64, i32)>>::new();
        let mut cursor = timer_log.min_live_offset()?.min(committed_length);
        while cursor < committed_length {
            let batch = timer_log.read_batch(
                cursor,
                REBUILD_BATCH_RECORDS,
                REBUILD_BATCH_RECORDS * TimerLogRecord::SIZE,
            )?;
            self.storage_metrics.record_recovery_replay(batch.entries.len() as u64);
            for entry in batch.entries {
                if entry.offset.get() >= committed_length {
                    break;
                }
                if entry.record.slot_time_ms >= read_slot_ms {
                    let logical_offset = i64::try_from(entry.offset.get()).map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, "timer log logical offset exceeds i64")
                    })?;
                    records_by_slot
                        .entry(entry.record.slot_time_ms)
                        .or_default()
                        .push((logical_offset, entry.record.timer_magic));
                }
            }
            let next_cursor = batch.next_cursor.get().min(committed_length);
            if next_cursor <= cursor {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("timer log rebuild did not advance from logical offset {cursor}"),
                ));
            }
            cursor = next_cursor;
        }

        let (drain_generation, drain_page, drain_record) = timer_checkpoint.drain_cursor();
        let processed = (drain_page as usize)
            .checked_mul(DEFAULT_IN_MEMORY_DRAIN_ENTRIES)
            .and_then(|value| value.checked_add(drain_record as usize))
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "timer drain cursor overflow"))?;
        let mut rebuilt = Vec::with_capacity(records_by_slot.len());
        for (slot_time_ms, entries) in records_by_slot {
            let last_position = entries.last().map(|entry| entry.0).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "timer rebuild produced an empty slot")
            })?;
            let mut start = 0usize;
            if slot_time_ms == read_slot_ms && !entries.is_empty() {
                if drain_generation == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("active timer slot {slot_time_ms} has no durable continuation identity"),
                    ));
                }
                let mut suffix_magic = 0;
                let mut matched = None;
                for candidate in (0..entries.len()).rev() {
                    suffix_magic |= entries[candidate].1;
                    let candidate_slot = Slot::new_with_num_magic(
                        slot_time_ms,
                        entries[candidate].0,
                        last_position,
                        i32::try_from(entries.len() - candidate).map_err(|_| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("timer slot {slot_time_ms} contains more than i32::MAX records"),
                            )
                        })?,
                        suffix_magic,
                    );
                    if slot_drain_generation(candidate_slot) == drain_generation {
                        matched = Some(candidate);
                        break;
                    }
                }
                let suffix_start = matched.ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "timer drain generation {drain_generation} does not identify a suffix of slot {slot_time_ms}"
                        ),
                    )
                })?;
                start = suffix_start.checked_add(processed).ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "timer drain continuation overflow")
                })?;
                if start > entries.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "timer drain cursor {processed} exceeds the {} records in slot {slot_time_ms}",
                            entries.len() - suffix_start
                        ),
                    ));
                }
            }
            if start == entries.len() {
                continue;
            }
            let magic = entries[start..].iter().fold(0, |combined, (_, magic)| combined | magic);
            rebuilt.push(Slot::new_with_num_magic(
                slot_time_ms,
                entries[start].0,
                last_position,
                i32::try_from(entries.len() - start).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("timer slot {slot_time_ms} contains more than i32::MAX records"),
                    )
                })?,
                magic,
            ));
        }
        Ok(rebuilt)
    }

    fn persist_recovered_state(
        &self,
        timer_checkpoint: &TimerCheckpoint,
        timer_log: &TimerLog,
        timer_wheel: &TimerWheel,
        recovered_read_time_ms: i64,
        recovered_queue_offset: i64,
        recovered_log_len: i64,
    ) -> std::io::Result<()> {
        timer_checkpoint.set_last_read_time_ms(recovered_read_time_ms);
        timer_checkpoint.set_last_timer_queue_offset(recovered_queue_offset);
        timer_checkpoint.set_master_timer_queue_offset(recovered_queue_offset);
        timer_checkpoint.set_last_timer_log_flush_pos(recovered_log_len);
        timer_log.flush()?;
        let wheel_generation = timer_wheel.flush_generation()?;
        timer_checkpoint.set_wheel_generation(wheel_generation);
        timer_checkpoint.set_role_epoch(self.role_state.epoch());
        timer_checkpoint.flush()?;
        timer_wheel.commit_generation(wheel_generation)?;
        timer_log.mark_clean_checkpoint(timer_checkpoint.local_generation())
    }

    pub async fn process_once(&self) -> usize {
        let _guard = self.process_lock.lock().await;
        let delivery_epoch = self.role_state.capture_delivery_epoch();
        let recall_visibility_watermark = delivery_epoch.and_then(|_| self.timer_queue_high_watermark());
        let mut indexed = self.process_enqueue_stage(self.enqueue_batch_limit());
        let recall_visible = if let Some(watermark) = recall_visibility_watermark {
            while self.curr_queue_offset.load(Ordering::Acquire) < watermark {
                let indexed_now = self.process_enqueue_stage(self.enqueue_batch_limit());
                if indexed_now == 0 {
                    break;
                }
                indexed = indexed.saturating_add(indexed_now);
            }
            self.curr_queue_offset.load(Ordering::Acquire) >= watermark
        } else {
            false
        };
        let delivered = match delivery_epoch {
            Some(epoch) if recall_visible && self.role_state.is_current_delivery_epoch(epoch) => {
                self.process_due_stage(self.dequeue_batch_limit(), epoch).await
            }
            _ => 0,
        };
        if indexed > 0 || delivered > 0 {
            self.sync_last_read_time_ms();
        }
        indexed + delivered
    }

    pub fn pipeline_diagnostics(&self) -> Option<TimerPipelineDiagnostics> {
        self.pipeline.lock().as_ref().map(|pipeline| pipeline.snapshot())
    }

    pub(crate) fn process_enqueue_stage(&self, limit: usize) -> usize {
        match self.process_enqueue_stage_with_durability(limit) {
            Ok((messages, _)) => messages,
            Err(error) => {
                error!("commit timer source stage failed: {error}");
                0
            }
        }
    }

    pub(crate) fn process_enqueue_stage_with_durability(&self, limit: usize) -> std::io::Result<(usize, bool)> {
        let _stage_guard = self.enqueue_stage_lock.lock();
        let indexed = {
            let _mutation = AtomicActivityGuard::new(&self.active_source_mutations, &self.persistence_gate);
            self.enqueue_from_timer_topic(limit.max(1))
        };
        if indexed > 0 {
            self.pending_progress.store(true, Ordering::Release);
        }
        let durable = if self.pending_progress.load(Ordering::Acquire) {
            self.commit_durable_progress()?
        } else {
            true
        };
        Ok((indexed, durable))
    }

    pub(crate) async fn process_due_stage(&self, limit: usize, delivery_epoch: u64) -> usize {
        match self.process_due_stage_with_durability(limit, delivery_epoch).await {
            Ok((messages, _)) => messages,
            Err(error) => {
                error!("commit timer due stage failed: {error}");
                0
            }
        }
    }

    pub(crate) async fn process_due_stage_with_durability(
        &self,
        limit: usize,
        delivery_epoch: u64,
    ) -> std::io::Result<(usize, bool)> {
        let delivered = self.process_due_stage_mutation(limit, delivery_epoch).await;
        let durable = if self.pending_progress.load(Ordering::Acquire) {
            self.commit_durable_progress()?
        } else {
            true
        };
        Ok((delivered, durable))
    }

    async fn process_due_stage_mutation(&self, limit: usize, delivery_epoch: u64) -> usize {
        let _stage_guard = self.due_stage_lock.lock().await;
        if !self.role_state.is_current_delivery_epoch(delivery_epoch) {
            return 0;
        }
        // A Recall tombstone already visible in the Timer CQ must be materialized before
        // delivery starts. The source workers retain a bounded share of every tick; this stage
        // simply waits for their contiguous watermark instead of bypassing it.
        if self
            .timer_queue_high_watermark()
            .is_some_and(|watermark| self.curr_queue_offset.load(Ordering::Acquire) < watermark)
        {
            return 0;
        }
        let delivered = {
            let _mutation = AtomicActivityGuard::new(&self.active_due_mutations, &self.persistence_gate);
            self.dequeue_due_messages(limit.max(1), delivery_epoch).await
        };
        if delivered > 0 {
            self.pending_progress.store(true, Ordering::Release);
        }
        delivered
    }

    pub(crate) async fn process_pipeline_enqueue_stage(
        self: &Arc<Self>,
        limit: usize,
    ) -> std::io::Result<(usize, bool)> {
        let _pipeline_guard = self.process_lock.lock().await;
        let store = Arc::clone(self);
        self.runtime_scope
            .storage_io()
            .spawn_io("timer-source-materialization", move || {
                store.process_enqueue_stage_with_durability(limit)
            })
            .await
            .map_err(|error| std::io::Error::other(format!("timer source materialization task failed: {error}")))?
    }

    pub(crate) async fn process_pipeline_due_stage(
        self: &Arc<Self>,
        limit: usize,
        delivery_epoch: u64,
    ) -> std::io::Result<(usize, bool)> {
        let _pipeline_guard = self.process_lock.lock().await;
        let delivered = self.process_due_stage_mutation(limit, delivery_epoch).await;
        let durable = if self.pending_progress.load(Ordering::Acquire) {
            let store = Arc::clone(self);
            self.runtime_scope
                .storage_io()
                .spawn_io("timer-due-checkpoint", move || store.commit_durable_progress())
                .await
                .map_err(|error| std::io::Error::other(format!("timer due checkpoint task failed: {error}")))??
        } else {
            true
        };
        Ok((delivered, durable))
    }

    pub(crate) fn commit_pipeline_progress(&self) -> std::io::Result<bool> {
        self.commit_durable_progress()
    }

    pub(crate) fn is_storage_loaded(&self) -> bool {
        self.timer_checkpoint.lock().is_some() && self.timer_log.lock().is_some() && self.timer_wheel.lock().is_some()
    }

    pub(crate) fn is_current_delivery_epoch(&self, epoch: u64) -> bool {
        self.role_state.is_current_delivery_epoch(epoch)
    }

    pub(crate) fn capture_delivery_epoch(&self) -> Option<u64> {
        self.role_state.capture_delivery_epoch()
    }

    pub fn quarantine_count(&self) -> usize {
        self.quarantine_manifest.snapshot().len()
    }

    fn enqueue_from_timer_topic(&self, limit: usize) -> usize {
        if !self.should_running_enqueue() {
            return 0;
        }
        let timer_topic = CheetahString::from_static_str(TIMER_TOPIC);
        let Some(consume_queue) = self.find_store_consume_queue(&timer_topic, 0) else {
            return 0;
        };

        let now_ms = self.floor_time_ms(self.clock.read().wall_time_ms());
        let dequeue_cursor = self.ensure_dequeue_cursor(now_ms);
        let enqueue_reference_time_ms = dequeue_cursor.max(now_ms);
        let max_offset = consume_queue.read().get_max_offset_in_queue();
        let mut queue_offset = self.curr_queue_offset.load(Ordering::Relaxed);
        let mut batch = Vec::with_capacity(limit.min(1_024));
        let mut batch_bytes = 0usize;
        while queue_offset < max_offset && batch.len() < limit {
            let Some(cq_unit) = consume_queue.read().get(queue_offset) else {
                break;
            };
            let Ok(size) = usize::try_from(cq_unit.size) else {
                break;
            };
            if !batch.is_empty()
                && batch_bytes.saturating_add(size) > self.message_store_config.timer_pipeline_queue_bytes
            {
                break;
            }
            batch_bytes = batch_bytes.saturating_add(size);
            queue_offset = cq_unit.queue_offset + 1;
            batch.push(cq_unit);
        }
        let locators: Vec<_> = batch.iter().map(|unit| (unit.pos, unit.size)).collect();
        let messages = self.look_messages_by_locator(&locators, self.message_store_config.timer_pipeline_queue_bytes);
        let mut indexed = 0usize;

        for (cq_unit, message) in batch.into_iter().zip(messages) {
            let message = match message {
                Ok(message) => message,
                Err(reason) => {
                    let corruption = match reason {
                        TimerPayloadReadError::Missing => CorruptionReason::MissingPayload,
                        TimerPayloadReadError::ShortRead => CorruptionReason::ShortRead,
                        TimerPayloadReadError::Decode => CorruptionReason::UnsupportedRecord,
                        TimerPayloadReadError::InvalidLocator => CorruptionReason::UnsupportedRecord,
                    };
                    self.quarantine_source(cq_unit.queue_offset, corruption);
                    warn!(
                        "timer queue offset {} is blocked because commitlog message {}:{} cannot be read: {:?}",
                        cq_unit.queue_offset, cq_unit.pos, cq_unit.size, reason
                    );
                    break;
                }
            };
            if let Err(reason) = validate_timer_source_route(&message) {
                self.quarantine_source(cq_unit.queue_offset, reason);
                warn!(
                    "timer queue offset {} is blocked because its persisted route is invalid: {:?}",
                    cq_unit.queue_offset, reason
                );
                break;
            }
            let Some(deliver_time_ms) = parse_deliver_time_ms(&message) else {
                warn!(
                    "skip timer queue offset {} because TIMER_OUT_MS is invalid",
                    queue_offset
                );
                self.curr_queue_offset
                    .store(cq_unit.queue_offset + 1, Ordering::Relaxed);
                continue;
            };

            let (slot_time_ms, magic) = self.plan_timer_slot(
                deliver_time_ms,
                enqueue_reference_time_ms,
                dequeue_cursor,
                is_delete_timer_message(&message),
            );
            match self.index_timer_message(slot_time_ms, magic, timer_generation(&message), &cq_unit) {
                Ok(()) => {
                    let real_topic =
                        message.property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC));
                    self.otel_timer_metrics
                        .record_enqueue_total(real_topic.as_ref().map(|topic| topic.as_str()));
                    self.otel_store_metrics.record_delay_message_latency_from_timestamps(
                        deliver_time_ms,
                        message.born_timestamp(),
                        real_topic.as_ref().map(|topic| topic.as_str()),
                    );
                    if let Some(real_topic) = real_topic {
                        if !is_delete_timer_message(&message) {
                            self.timer_metrics.add_timing_count(&real_topic, 1);
                        }
                    }
                    self.enqueue_tps_counter.record(1, current_millis() as i64);
                    self.curr_queue_offset
                        .store(cq_unit.queue_offset + 1, Ordering::Relaxed);
                    indexed += 1;
                }
                Err(err) => {
                    error!(
                        "index timer queue offset {} to slot {} failed: {}",
                        queue_offset, slot_time_ms, err
                    );
                    break;
                }
            }
        }

        indexed
    }

    fn quarantine_source(&self, source_offset: i64, reason: CorruptionReason) {
        if let Err(error) = self.quarantine_manifest.record(QuarantineRecord {
            timer_id: TimerId::new(source_offset.max(0) as u128),
            reason,
            source_offset,
            attempts: 0,
        }) {
            error!("persist timer quarantine record failed: {error}");
        }
    }

    async fn dequeue_due_messages(&self, limit: usize, delivery_epoch: u64) -> usize {
        let now_ms = self.clock.read().wall_time_ms();
        let current_tick_ms = self.floor_time_ms(now_ms);
        let mut cursor = self.ensure_dequeue_cursor(current_tick_ms);
        let mut remaining_budget = limit;
        let mut delivered = 0usize;

        while cursor < current_tick_ms && remaining_budget > 0 {
            if let Some(slot) = self.get_timer_wheel_slot(cursor).filter(|slot| slot.num > 0) {
                let delivered_now = self.deliver_slot(cursor, slot, remaining_budget, delivery_epoch).await;
                delivered += delivered_now;
                remaining_budget = remaining_budget.saturating_sub(delivered_now);
                if self
                    .get_timer_wheel_slot(cursor)
                    .is_some_and(|remaining| remaining.num > 0)
                {
                    self.curr_read_time_ms.store(cursor, Ordering::Relaxed);
                    break;
                }
                if remaining_budget == 0 {
                    cursor += self.precision_ms();
                    self.curr_read_time_ms.store(cursor, Ordering::Relaxed);
                    break;
                }
            }
            cursor += self.precision_ms();
            self.curr_read_time_ms.store(cursor, Ordering::Relaxed);
        }

        delivered
    }

    fn timer_queue_high_watermark(&self) -> Option<i64> {
        self.find_store_consume_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0)
            .map(|queue| queue.read().get_max_offset_in_queue())
    }

    fn index_timer_message(
        &self,
        slot_time_ms: i64,
        magic: i32,
        generation: u64,
        cq_unit: &CqUnit,
    ) -> std::io::Result<()> {
        let current_slot = self.get_timer_wheel_slot(slot_time_ms);
        let prev_pos = current_slot
            .filter(|slot| slot.num > 0)
            .map(|slot| slot.last_pos)
            .unwrap_or(EMPTY_TIMER_LOG_POS);
        let record = TimerLogRecord {
            deliver_time_ms: slot_time_ms,
            commit_log_offset: cq_unit.pos,
            size: cq_unit.size,
            queue_offset: cq_unit.queue_offset,
            prev_pos,
            magic,
        };
        let record_pos = self
            .timer_log
            .lock()
            .as_ref()
            .ok_or_else(|| std::io::Error::other("timer log is not loaded"))?
            .append_record(record, generation)? as i64;
        let first_pos = current_slot
            .filter(|slot| slot.num > 0)
            .map(|slot| slot.first_pos)
            .unwrap_or(record_pos);
        let num = current_slot.map(|slot| slot.num + 1).unwrap_or(1);
        let slot_magic = current_slot.map(|slot| slot.magic | magic).unwrap_or(magic);
        self.put_timer_wheel_slot(slot_time_ms, first_pos, record_pos, num, slot_magic)?;
        Ok(())
    }

    async fn deliver_slot(&self, slot_time_ms: i64, slot: Slot, limit: usize, delivery_epoch: u64) -> usize {
        let mut active_drain = match self.take_or_build_slot_drain(slot) {
            Ok(active_drain) => active_drain,
            Err(err) => {
                error!("prepare timer slot {} drain plan failed: {}", slot_time_ms, err);
                return 0;
            }
        };
        let entries = match active_drain.plan.read_batch(limit) {
            Ok(entries) => entries,
            Err(error) => {
                error!("read timer slot {} continuation failed: {}", slot_time_ms, error);
                self.slot_drains.lock().insert(slot_time_ms, active_drain);
                return 0;
            }
        };

        let mut processed = 0usize;
        for entry in &entries {
            let Some(message) = self.look_store_message(entry.record.commit_log_offset, entry.record.size) else {
                self.quarantine_source(entry.record.queue_offset, CorruptionReason::MissingPayload);
                warn!(
                    "delay delivery blocked at timer log position {} because commitlog {}:{} is missing",
                    entry.position, entry.record.commit_log_offset, entry.record.size
                );
                break;
            };
            if need_roll(entry.record.magic) {
                let now_ms = self.floor_time_ms(self.clock.read().wall_time_ms());
                if parse_deliver_time_ms(&message).is_some_and(|deliver_time_ms| deliver_time_ms > now_ms) {
                    if !self.role_state.is_current_delivery_epoch(delivery_epoch) {
                        warn!("reject stale timer roll batch for role epoch {delivery_epoch}");
                        break;
                    }
                    let rolled_topic =
                        message.property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC));
                    let Some(rolled_message) = self.convert_timer_message(message, true) else {
                        self.quarantine_source(entry.record.queue_offset, CorruptionReason::UnsupportedRecord);
                        break;
                    };
                    let put_result = self.put_store_message(rolled_message).await;
                    if !self.accept_delivery_result(entry.record.queue_offset, "roll", &put_result) {
                        break;
                    }
                    if let Some(real_topic) = rolled_topic {
                        self.timer_metrics.add_timing_count(&real_topic, -1);
                        self.otel_timer_metrics.record_dequeue_total(real_topic.as_str());
                    }
                    self.dequeue_tps_counter.record(1, current_millis() as i64);
                    processed += 1;
                    continue;
                }
            }
            if is_delete_timer_message(&message) {
                processed += 1;
                continue;
            }
            if delete_identities_for_message(&message)
                .iter()
                .any(|identity| active_drain.delete_keys.contains(identity))
            {
                if let Some(real_topic) =
                    message.property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC))
                {
                    self.timer_metrics.add_timing_count(&real_topic, -1);
                    self.otel_timer_metrics.record_dequeue_total(real_topic.as_str());
                }
                processed += 1;
                continue;
            }

            let Some(original_deliver_ms) = original_deliver_time_ms(&message, self.precision_ms()) else {
                self.quarantine_source(entry.record.queue_offset, CorruptionReason::UnsupportedRecord);
                warn!("delay delivery blocked because the original timer deadline is invalid");
                break;
            };
            let now_ms = self.clock.read().wall_time_ms();
            if !is_due_not_before(original_deliver_ms, slot_time_ms, self.floor_time_ms(now_ms), now_ms) {
                break;
            }
            let generation = timer_generation(&message);
            let Some(mut deliver_message) = self.convert_timer_message(message, false) else {
                self.quarantine_source(entry.record.queue_offset, CorruptionReason::UnsupportedRecord);
                break;
            };
            if deliver_message
                .property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN,
                ))
                .is_none()
            {
                // V1 records predate admission-time tokens. The fallback is deterministic for
                // replay; new records always persist the token before entering the timer topic.
                MessageAccessor::put_property(
                    &mut deliver_message,
                    CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN),
                    stable_delivery_token(entry.record.queue_offset, generation),
                );
            }
            deliver_message.properties_string =
                MessageDecoder::message_properties_to_string(deliver_message.get_properties());
            let delivered_topic = deliver_message.get_topic().clone();

            // Recheck both time and ownership immediately before the externally visible write.
            // A backward wall-clock jump may delay delivery, but it must never make it early.
            let final_now_ms = self.clock.read().wall_time_ms();
            if !is_due_not_before(
                original_deliver_ms,
                slot_time_ms,
                self.floor_time_ms(final_now_ms),
                final_now_ms,
            ) {
                break;
            }
            if !self.role_state.is_current_delivery_epoch(delivery_epoch) {
                warn!("reject stale timer delivery batch for role epoch {delivery_epoch}");
                break;
            }
            let put_result = self.put_store_message(deliver_message).await;
            if !self.accept_delivery_result(entry.record.queue_offset, "deliver", &put_result) {
                break;
            }
            self.timer_metrics.add_timing_count(&delivered_topic, -1);
            self.otel_timer_metrics.record_dequeue_total(delivered_topic.as_str());
            self.dequeue_tps_counter.record(1, current_millis() as i64);
            processed += 1;
        }

        active_drain.plan.advance(&entries[..processed]);
        let remaining_slot = match active_drain.plan.remaining_slot() {
            Ok(remaining_slot) => remaining_slot,
            Err(error) => {
                error!("read remaining timer slot {} plan failed: {}", slot_time_ms, error);
                self.slot_drains.lock().insert(slot_time_ms, active_drain);
                return 0;
            }
        };
        let rewrite_result = match remaining_slot {
            Some(remaining) => self.put_timer_wheel_slot(
                remaining.time_ms,
                remaining.first_pos,
                remaining.last_pos,
                remaining.num,
                remaining.magic,
            ),
            None => self.put_timer_wheel_slot(slot_time_ms, 0, 0, 0, 0),
        };
        if let Err(error) = rewrite_result {
            error!("rewrite timer slot {} continuation failed: {}", slot_time_ms, error);
            self.slot_drains.lock().insert(slot_time_ms, active_drain);
            return 0;
        }
        if active_drain.plan.remaining() == 0 {
            if let Err(error) = active_drain.plan.remove() {
                warn!(
                    "remove completed timer slot {} drain plan failed: {}",
                    slot_time_ms, error
                );
            }
        } else {
            self.slot_drains.lock().insert(slot_time_ms, active_drain);
        }
        processed
    }

    fn accept_delivery_result(&self, source_offset: i64, operation: &'static str, result: &PutMessageResult) -> bool {
        let status = result.put_message_status();
        match classify_delivery_status(status) {
            TimerWorkResult::Complete if result.is_ok() => true,
            TimerWorkResult::Complete | TimerWorkResult::Retry(_) => {
                warn!("timer {operation} will retry after put status {status:?}");
                false
            }
            TimerWorkResult::Quarantine(reason) => {
                self.quarantine_source(source_offset, reason);
                warn!("timer {operation} is quarantined after put status {status:?}");
                false
            }
            TimerWorkResult::Cancelled | TimerWorkResult::StaleGeneration => false,
        }
    }

    fn take_or_build_slot_drain(&self, slot: Slot) -> std::io::Result<ActiveSlotDrain> {
        if let Some(active) = self.slot_drains.lock().remove(&slot.time_ms) {
            if active.plan.matches_slot(slot)? {
                return Ok(active);
            }
            active.plan.remove()?;
        }

        let mut builder = SlotDrainPlanBuilder::new(
            self.message_store_config.store_path_root_dir.as_str(),
            slot,
            DEFAULT_IN_MEMORY_DRAIN_ENTRIES,
            Arc::clone(&self.storage_metrics),
        )?;
        let mut delete_keys = HashSet::new();
        let mut current_pos = slot.last_pos;
        for _ in 0..slot.num.max(0) {
            if current_pos < 0 {
                break;
            }
            let (record, generation) = self
                .timer_log
                .lock()
                .as_ref()
                .ok_or_else(|| std::io::Error::other("timer log is not loaded"))?
                .read_record(current_pos as u64)?;
            let entry = TimerLogEntry {
                position: current_pos,
                record,
                generation,
            };
            builder.push_reverse(entry)?;
            if record.magic & MAGIC_DELETE != 0 {
                let message = self
                    .look_store_message(record.commit_log_offset, record.size)
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!(
                                "recall scan cannot read commitlog {}:{} at timer log {}",
                                record.commit_log_offset, record.size, current_pos
                            ),
                        )
                    })?;
                if let Some(identity) = extract_delete_timer_identity(&message) {
                    delete_keys.insert(identity);
                }
            }
            current_pos = record.prev_pos;
        }
        let plan = builder.finish()?;
        if plan.remaining() != slot.num.max(0) as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "timer slot {} declares {} records but its chain contains {}",
                    slot.time_ms,
                    slot.num,
                    plan.remaining()
                ),
            ));
        }
        Ok(ActiveSlotDrain { plan, delete_keys })
    }

    fn load_slot_entries(&self, slot: Slot) -> std::io::Result<Vec<TimerLogEntry>> {
        let mut entries = Vec::with_capacity(slot.num.max(0) as usize);
        let mut current_pos = slot.last_pos;
        for _ in 0..slot.num.max(0) {
            if current_pos < 0 {
                break;
            }
            let (record, generation) = self
                .timer_log
                .lock()
                .as_ref()
                .ok_or_else(|| std::io::Error::other("timer log is not loaded"))?
                .read_record(current_pos as u64)?;
            entries.push(TimerLogEntry {
                position: current_pos,
                record,
                generation,
            });
            current_pos = record.prev_pos;
        }
        entries.reverse();
        Ok(entries)
    }

    fn convert_timer_message(&self, message: MessageExt, need_roll: bool) -> Option<MessageExtBrokerInner> {
        let mut inner = MessageExtBrokerInner::default();
        let sys_flag = message.sys_flag();
        let born_timestamp = message.born_timestamp();
        let born_host = message.born_host();
        let store_host = message.store_host();
        let reconsume_times = message.reconsume_times();
        let store_timestamp = message.store_timestamp();
        let timer_topic = message.topic().clone();
        let timer_queue_id = message.queue_id();
        let message_inner = message.message;
        let message_properties = message_inner.properties().as_map().clone();

        if let Some(body) = message_inner.get_body() {
            inner.set_body(body.clone());
        }
        inner.set_flag(message_inner.flag());
        MessageAccessor::set_properties(&mut inner, message_properties);
        if store_timestamp > 0 {
            MessageAccessor::put_property(
                &mut inner,
                CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_ENQUEUE_MS),
                CheetahString::from_string(store_timestamp.to_string()),
            );
        }
        MessageAccessor::put_property(
            &mut inner,
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DEQUEUE_MS),
            CheetahString::from_string(current_millis().to_string()),
        );
        if need_roll {
            let next_roll_times = inner
                .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_ROLL_TIMES))
                .and_then(|times| times.parse::<i32>().ok())
                .unwrap_or_default()
                + 1;
            MessageAccessor::put_property(
                &mut inner,
                CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_ROLL_TIMES),
                CheetahString::from_string(next_roll_times.to_string()),
            );
        }
        let topic_filter_type = message_single::parse_topic_filter_type(inner.sys_flag());
        inner.tags_code = MessageExtBrokerInner::tags_string2tags_code(
            &topic_filter_type,
            inner.tags().as_ref().unwrap_or(&CheetahString::empty()),
        );
        inner.message_ext_inner.sys_flag = sys_flag;
        inner.message_ext_inner.born_timestamp = born_timestamp;
        inner.message_ext_inner.born_host = born_host;
        inner.message_ext_inner.store_host = store_host;
        inner.message_ext_inner.reconsume_times = reconsume_times;
        inner.set_wait_store_msg_ok(false);

        if need_roll {
            inner.set_topic(timer_topic);
            inner.message_ext_inner.queue_id = timer_queue_id;
        } else {
            let Some(topic) = inner.property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC)) else {
                warn!("drop timer message because REAL_TOPIC is missing");
                return None;
            };
            inner.set_topic(topic);
            let queue_id = inner.property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_QUEUE_ID));
            if let Some(queue_id) = queue_id {
                match queue_id.parse::<i32>() {
                    Ok(queue_id) => {
                        inner.message_ext_inner.queue_id = queue_id;
                    }
                    Err(err) => {
                        warn!("drop timer message because REAL_QID is invalid: {}", err);
                        return None;
                    }
                }
            }
            MessageAccessor::clear_property(&mut inner, MessageConst::PROPERTY_REAL_TOPIC);
            MessageAccessor::clear_property(&mut inner, MessageConst::PROPERTY_REAL_QUEUE_ID);
            for property in [
                MessageConst::PROPERTY_TIMER_OUT_MS,
                MessageConst::PROPERTY_TIMER_ORIGINAL_DELIVER_MS,
                MessageConst::PROPERTY_TIMER_DELAY_SEC,
                MessageConst::PROPERTY_TIMER_DELAY_MS,
                MessageConst::PROPERTY_TIMER_DELIVER_MS,
                MessageConst::PROPERTY_TIMER_ENQUEUE_MS,
                MessageConst::PROPERTY_TIMER_DEQUEUE_MS,
                MessageConst::PROPERTY_TIMER_ROLL_TIMES,
                MessageConst::PROPERTY_TIMER_ROLL_LABEL,
                MessageConst::PROPERTY_TIMER_DEL_UNIQKEY,
                MessageConst::PROPERTY_TIMER_GENERATION,
                MessageConst::TIMER_ENGINE_TYPE,
                MessageConst::PROPERTY_TIMER_FORMAT_VERSION,
                MessageConst::PROPERTY_TIMER_POLICY_FINGERPRINT,
            ] {
                MessageAccessor::clear_property(&mut inner, property);
            }
        }
        inner.properties_string = MessageDecoder::message_properties_to_string(inner.get_properties());
        Some(inner)
    }

    fn ensure_dequeue_cursor(&self, fallback_time_ms: i64) -> i64 {
        let current = self.curr_read_time_ms.load(Ordering::Relaxed);
        let aligned = self.floor_time_ms(fallback_time_ms);
        let max_allowed_cursor = aligned.saturating_add(self.precision_ms() * MAX_FUTURE_CURSOR_SKEW_SLOTS);
        if current > max_allowed_cursor {
            warn!(
                "rewind timer read cursor from {} to {} because it is ahead of system time by more than {} slots",
                current, aligned, MAX_FUTURE_CURSOR_SKEW_SLOTS
            );
            self.curr_read_time_ms.store(aligned, Ordering::Relaxed);
            return aligned;
        }
        if current > 0 {
            return self.floor_time_ms(current);
        }

        match self
            .curr_read_time_ms
            .compare_exchange(0, aligned, Ordering::AcqRel, Ordering::Relaxed)
        {
            Ok(_) => aligned,
            Err(existing) if existing > 0 => self.floor_time_ms(existing),
            Err(_) => aligned,
        }
    }

    fn plan_timer_slot(
        &self,
        deliver_time_ms: i64,
        reference_time_ms: i64,
        lower_bound_ms: i64,
        is_delete: bool,
    ) -> (i64, i32) {
        self.timer_policy().plan_slot(
            deliver_time_ms,
            reference_time_ms,
            lower_bound_ms,
            if is_delete { MAGIC_DELETE } else { MAGIC_DEFAULT },
            MAGIC_ROLL,
        )
    }

    fn floor_time_ms(&self, time_ms: i64) -> i64 {
        self.timer_policy().floor_time_ms(time_ms)
    }

    fn ceil_time_ms(&self, time_ms: i64) -> i64 {
        self.timer_policy().ceil_time_ms(time_ms)
    }

    fn precision_ms(&self) -> i64 {
        self.timer_policy().precision_ms()
    }

    fn timer_roll_window_ms(&self) -> i64 {
        self.timer_policy().roll_window_ms()
    }

    fn timer_policy(&self) -> TimerSchedulePolicy {
        TimerSchedulePolicy::new(
            self.message_store_config.timer_precision_ms,
            TIMER_WHEEL_TTL_DAY as usize * DAY_SECS as usize,
            TIMER_BLANK_SLOTS as usize,
            self.message_store_config.timer_roll_window_slot,
        )
    }

    fn timer_storage_fingerprint(
        &self,
    ) -> Result<TimerStorageFingerprint, rocketmq_store_local::timer::storage_format::TimerStorageFormatError> {
        TimerStorageFingerprint {
            precision_ms: self.message_store_config.timer_precision_ms,
            wheel_slots: (TIMER_WHEEL_TTL_DAY as u64) * (DAY_SECS as u64) * 2,
            segment_size: self.message_store_config.mapped_file_size_timer_log as u64,
            page_size: 4_096,
            record_version: TIMER_LOG_RECORD_VERSION,
            delete_key_mode: u8::from(self.message_store_config.timer_delete_key_with_topic),
        }
        .validate(TIMER_LOG_V2_PHYSICAL_RECORD_SIZE)
    }

    fn should_running_enqueue(&self) -> bool {
        if !self.enqueue_suspended.load(Ordering::Relaxed) {
            return true;
        }

        self.timer_checkpoint
            .lock()
            .as_ref()
            .map(|timer_checkpoint| {
                self.curr_queue_offset.load(Ordering::Relaxed) < timer_checkpoint.master_timer_queue_offset()
            })
            .unwrap_or(false)
    }

    fn enqueue_batch_limit(&self) -> usize {
        self.message_store_config.timer_source_batch_messages.max(1)
    }

    fn dequeue_batch_limit(&self) -> usize {
        self.message_store_config.timer_due_batch_messages.max(1)
    }
}

pub fn build_delete_key(real_topic: &str, unique_key: &str) -> CheetahString {
    CheetahString::from_string(format!("{}_{}", real_topic, unique_key))
}

/// Builds the canonical key written by new Recall records.
///
/// Java compatibility uses the unique key alone. The opt-in topic-aware form is length-prefixed,
/// avoiding the collisions inherent in the legacy `topic_uniqKey` concatenation.
pub fn build_canonical_delete_key(real_topic: &str, unique_key: &str, include_topic: bool) -> CheetahString {
    if include_topic {
        CheetahString::from_string(format!("T:{}:{real_topic}{unique_key}", real_topic.len()))
    } else {
        CheetahString::from_slice(unique_key)
    }
}

fn observe_backlog_message(backlog: &mut TimerBacklogMetrics, message: &MessageExt, now_ms: i64) {
    let Some(deliver_time_ms) = parse_deliver_time_ms(message) else {
        return;
    };
    let topic = message
        .property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC))
        .map(|topic| topic.to_string())
        .unwrap_or_else(|| message.topic().to_string());
    backlog.observe(topic, deliver_time_ms, now_ms, is_delete_timer_message(message));
}

fn parse_deliver_time_ms(message: &MessageExt) -> Option<i64> {
    message
        .property(&CheetahString::from_static_str(TIMER_OUT_MS))
        .and_then(|value| value.parse::<i64>().ok())
}

fn is_delete_timer_message(message: &MessageExt) -> bool {
    extract_delete_timer_identity(message).is_some()
}

fn extract_delete_timer_identity(message: &MessageExt) -> Option<TimerDeleteIdentity> {
    Some(TimerDeleteIdentity {
        key: message.property(&CheetahString::from_static_str(TIMER_DELETE_UNIQUE_KEY))?,
        generation: timer_generation(message),
    })
}

fn delete_identities_for_message(message: &MessageExt) -> Vec<TimerDeleteIdentity> {
    let Some(unique_key) = message.property(&CheetahString::from_static_str(
        MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
    )) else {
        return Vec::new();
    };
    let real_topic = message
        .property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC))
        .unwrap_or_else(|| CheetahString::from_string(message.topic().to_string()));
    let generation = timer_generation(message);
    [
        CheetahString::from_slice(unique_key.as_str()),
        build_delete_key(real_topic.as_str(), unique_key.as_str()),
        build_canonical_delete_key(real_topic.as_str(), unique_key.as_str(), true),
    ]
    .into_iter()
    .map(|key| TimerDeleteIdentity { key, generation })
    .collect()
}

fn timer_generation(message: &MessageExt) -> u64 {
    message
        .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_GENERATION))
        .and_then(|generation| generation.parse::<u64>().ok())
        .unwrap_or_default()
}

fn validate_timer_source_route(message: &MessageExt) -> Result<(), CorruptionReason> {
    let engine_key = CheetahString::from_static_str(MessageConst::TIMER_ENGINE_TYPE);
    let Some(engine_id) = message.property(&engine_key) else {
        // V1 and Java-originated records predate the extended route envelope.
        return Ok(());
    };
    if TimerEngineId::parse(engine_id.as_str()).ok() != Some(TimerEngineId::JavaCompat) {
        return Err(CorruptionReason::UnsupportedRecord);
    }
    let version = message
        .property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TIMER_FORMAT_VERSION,
        ))
        .and_then(|value| value.parse::<u16>().ok());
    if version != Some(JAVA_COMPAT_TIMER_FORMAT_VERSION) {
        return Err(CorruptionReason::UnsupportedRecord);
    }
    let fingerprint_valid = message
        .property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TIMER_POLICY_FINGERPRINT,
        ))
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|value| value != 0);
    let generation_valid = message
        .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_GENERATION))
        .and_then(|value| value.parse::<u64>().ok())
        .is_some();
    let token_valid = message
        .property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN,
        ))
        .is_some_and(|value| !value.is_empty());
    if fingerprint_valid && generation_valid && token_valid {
        Ok(())
    } else {
        Err(CorruptionReason::UnsupportedRecord)
    }
}

fn original_deliver_time_ms(message: &MessageExt, precision_ms: i64) -> Option<i64> {
    message
        .property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TIMER_ORIGINAL_DELIVER_MS,
        ))
        .and_then(|deadline| deadline.parse::<i64>().ok())
        .or_else(|| parse_deliver_time_ms(message)?.checked_add(precision_ms))
}

fn stable_delivery_token(queue_offset: i64, generation: u64) -> CheetahString {
    CheetahString::from_string(format!("F:1:0:{queue_offset}:{generation}"))
}

fn need_roll(magic: i32) -> bool {
    (magic & MAGIC_ROLL) != 0
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::OpenOptions;
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicI64;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use crate::config::store_runtime_config::StoreRuntimeConfig;
    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use parking_lot::Mutex as ParkingMutex;
    use rocketmq_model::common::config::TopicConfig;
    use rocketmq_model::common::message::message_ext::MessageExt;
    use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
    use rocketmq_model::common::message::MessageConst;
    use rocketmq_model::common::message::MessageTrait;
    use rocketmq_protocol::common::message::message_decoder::message_properties_to_string;
    use rocketmq_protocol::protocol::data_version_facade::DataVersionExt;
    use tempfile::tempdir;

    use super::build_delete_key;
    use super::current_millis;
    use super::get_timer_check_path;
    use super::get_timer_log_path;
    use super::get_timer_wheel_path;
    use super::validate_timer_source_route;
    use super::CorruptionReason;
    use super::MessageStoreConfig;
    use super::PersistenceStage;
    use super::PutMessageStatus;
    use super::TimerCheckpoint;
    use super::TimerCheckpointSnapshot;
    use super::TimerClock;
    use super::TimerLog;
    use super::TimerLogRecord;
    use super::TimerMessageStore;
    use super::TimerWheel;
    use super::DAY_SECS;
    use super::EMPTY_TIMER_LOG_POS;
    use super::MAGIC_DEFAULT;
    use super::MAGIC_ROLL;
    use super::TIMER_TOPIC;
    use super::TIMER_WHEEL_TTL_DAY;
    use crate::base::backend_ops::BackendOps;
    use crate::message_store::local_file_message_store::LocalFileMessageStore;
    struct ManualTimerClock {
        wall_time_ms: AtomicI64,
    }

    #[test]
    fn persisted_timer_route_fails_closed_for_unknown_or_partial_envelopes() {
        let mut legacy = MessageExt::default();
        assert_eq!(validate_timer_source_route(&legacy), Ok(()));

        legacy.message.put_property(
            CheetahString::from_static_str(MessageConst::TIMER_ENGINE_TYPE),
            CheetahString::from_static_str("unknown"),
        );
        assert_eq!(
            validate_timer_source_route(&legacy),
            Err(CorruptionReason::UnsupportedRecord)
        );

        let mut canonical = MessageExt::default();
        for (key, value) in [
            (MessageConst::TIMER_ENGINE_TYPE, "F"),
            (MessageConst::PROPERTY_TIMER_FORMAT_VERSION, "1"),
            (MessageConst::PROPERTY_TIMER_POLICY_FINGERPRINT, "7"),
            (MessageConst::PROPERTY_TIMER_GENERATION, "0"),
            (MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN, "token"),
        ] {
            canonical.message.put_property(
                CheetahString::from_static_str(key),
                CheetahString::from_static_str(value),
            );
        }
        assert_eq!(validate_timer_source_route(&canonical), Ok(()));
    }

    impl ManualTimerClock {
        fn new(wall_time_ms: i64) -> Self {
            Self {
                wall_time_ms: AtomicI64::new(wall_time_ms),
            }
        }

        fn advance(&self, delta_ms: i64) {
            self.wall_time_ms.fetch_add(delta_ms, Ordering::AcqRel);
        }
    }

    impl TimerClock for ManualTimerClock {
        fn wall_time_ms(&self) -> i64 {
            self.wall_time_ms.load(Ordering::Acquire)
        }

        fn monotonic_elapsed_ms(&self) -> u64 {
            self.wall_time_ms().max(0) as u64
        }
    }

    fn config_with_root(root_dir: &str) -> Arc<MessageStoreConfig> {
        Arc::new(MessageStoreConfig {
            store_path_root_dir: CheetahString::from_string(root_dir.to_owned()),
            read_uncommitted: true,
            ..MessageStoreConfig::default()
        })
    }

    fn standalone_timer_store(config: Arc<MessageStoreConfig>) -> TimerMessageStore {
        TimerMessageStore::new_with_message_store_config(
            config,
            crate::runtime::test_service_context("timer-message-store-test"),
        )
    }

    fn config_with_root_and_limits(
        root_dir: &str,
        max_msgs_num_batch: usize,
        timer_get_message_thread_num: usize,
        timer_put_message_thread_num: usize,
    ) -> Arc<MessageStoreConfig> {
        Arc::new(MessageStoreConfig {
            store_path_root_dir: CheetahString::from_string(root_dir.to_owned()),
            read_uncommitted: true,
            max_msgs_num_batch,
            timer_get_message_thread_num,
            timer_put_message_thread_num,
            timer_source_batch_messages: max_msgs_num_batch,
            timer_due_batch_messages: max_msgs_num_batch,
            ..MessageStoreConfig::default()
        })
    }

    fn config_with_root_precision_and_roll_window(
        root_dir: &str,
        timer_precision_ms: u64,
        timer_roll_window_slot: usize,
    ) -> Arc<MessageStoreConfig> {
        Arc::new(MessageStoreConfig {
            store_path_root_dir: CheetahString::from_string(root_dir.to_owned()),
            read_uncommitted: true,
            timer_precision_ms,
            timer_roll_window_slot,
            ..MessageStoreConfig::default()
        })
    }

    fn config_with_metrics_check(root_dir: &str, timer_metric_small_threshold: usize) -> Arc<MessageStoreConfig> {
        Arc::new(MessageStoreConfig {
            store_path_root_dir: CheetahString::from_string(root_dir.to_owned()),
            read_uncommitted: true,
            timer_enable_check_metrics: true,
            timer_metric_small_threshold,
            timer_check_metrics_when: "0;1;2;3;4;5;6;7;8;9;10;11;12;13;14;15;16;17;18;19;20;21;22;23".to_string(),
            ..MessageStoreConfig::default()
        })
    }

    #[test]
    fn canonical_config_constructor_preserves_standalone_state() {
        let root = tempfile::tempdir().expect("Timer constructor test root should be created");
        let config = config_with_root(root.path().to_string_lossy().as_ref());
        let canonical = standalone_timer_store(Arc::clone(&config));

        assert!(canonical.store_context.is_none());
        assert_eq!(
            canonical.message_store_config.store_path_root_dir,
            config.store_path_root_dir
        );
    }

    #[tokio::test]
    async fn standalone_config_constructor_fails_closed_without_store_context() {
        let root = tempfile::tempdir().expect("Timer fail-closed test root should be created");
        let config = config_with_root(root.path().to_string_lossy().as_ref());
        let standalone = standalone_timer_store(config);

        assert!(standalone
            .find_store_consume_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0)
            .is_none());
        assert!(standalone.look_store_message(0, 1).is_none());
        let result = standalone.put_store_message(MessageExtBrokerInner::default()).await;
        assert_eq!(result.put_message_status(), PutMessageStatus::ServiceNotAvailable);
    }

    fn build_store_with_timer(root_dir: &str) -> (LocalFileMessageStore, Arc<TimerMessageStore>, CheetahString) {
        build_store_with_timer_and_config(config_with_root(root_dir))
    }

    fn build_store_with_timer_and_config(
        config: Arc<MessageStoreConfig>,
    ) -> (LocalFileMessageStore, Arc<TimerMessageStore>, CheetahString) {
        let broker_config = Arc::new(StoreRuntimeConfig::default());
        let real_topic = CheetahString::from_static_str("phase3_topic");
        let topic_config_table = Arc::new(DashMap::new());
        topic_config_table.insert(real_topic.clone(), Arc::new(TopicConfig::default()));
        topic_config_table.insert(
            CheetahString::from_static_str(TIMER_TOPIC),
            Arc::new(TopicConfig::default()),
        );

        let mut store = LocalFileMessageStore::new(
            Arc::clone(&config),
            broker_config,
            topic_config_table,
            None,
            false,
            crate::runtime::test_service_context("timer-local-file-store-test"),
        );
        store
            .wire_owned_root_dependencies()
            .expect("Timer tests should wire owned Store capabilities");
        let timer_message_store = store
            .get_timer_message_store()
            .cloned()
            .expect("Timer should be enabled for Timer tests");
        (store, timer_message_store, real_topic)
    }

    fn build_timer_message(real_topic: &CheetahString, deliver_ms: u64) -> MessageExtBrokerInner {
        build_timer_message_with_queue_id(real_topic, 0, deliver_ms)
    }

    fn build_timer_message_with_queue_id(
        real_topic: &CheetahString,
        real_queue_id: i32,
        deliver_ms: u64,
    ) -> MessageExtBrokerInner {
        let mut msg = MessageExtBrokerInner::default();
        msg.set_topic(CheetahString::from_static_str(TIMER_TOPIC));
        msg.message_ext_inner.queue_id = 0;
        msg.set_body(Bytes::from_static(b"phase3-body"));
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC),
            real_topic.clone(),
        );
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_REAL_QUEUE_ID),
            CheetahString::from_string(real_queue_id.to_string()),
        );
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_OUT_MS),
            CheetahString::from_string(deliver_ms.to_string()),
        );
        msg.properties_string = message_properties_to_string(msg.get_properties());
        msg
    }

    fn build_delete_timer_message(
        real_topic: &CheetahString,
        unique_key: &str,
        deliver_ms: u64,
    ) -> MessageExtBrokerInner {
        let mut msg = build_timer_message_with_queue_id(real_topic, 0, deliver_ms);
        msg.set_body(Bytes::from_static(b"0"));
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DEL_UNIQKEY),
            build_delete_key(real_topic.as_str(), unique_key),
        );
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from_string(unique_key.to_owned()),
        );
        msg.properties_string = message_properties_to_string(msg.get_properties());
        msg
    }

    fn build_java_delete_timer_message(
        real_topic: &CheetahString,
        unique_key: &str,
        deliver_ms: u64,
    ) -> MessageExtBrokerInner {
        let mut message = build_delete_timer_message(real_topic, unique_key, deliver_ms);
        message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DEL_UNIQKEY),
            CheetahString::from_slice(unique_key),
        );
        message.properties_string = message_properties_to_string(message.get_properties());
        message
    }

    #[test]
    fn load_creates_durable_timer_artifacts_and_restores_checkpoint() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let config = config_with_root(root_dir.as_str());

        let timer_message_store = standalone_timer_store(config.clone());
        assert!(timer_message_store.load());
        assert!(Path::new(get_timer_check_path(root_dir.as_str()).as_str()).exists());
        assert!(Path::new(get_timer_log_path(root_dir.as_str()).as_str()).exists());
        assert!(Path::new(&format!("{}.v2", get_timer_wheel_path(root_dir.as_str()))).exists());

        let read_time_ms = timer_message_store.floor_time_ms(current_millis() as i64);
        timer_message_store
            .curr_read_time_ms
            .store(read_time_ms, Ordering::Relaxed);
        timer_message_store.curr_queue_offset.store(9, Ordering::Relaxed);
        timer_message_store.sync_last_read_time_ms();
        timer_message_store.shutdown();

        let reloaded_store = standalone_timer_store(config);
        assert!(reloaded_store.load());
        assert_eq!(reloaded_store.curr_read_time_ms.load(Ordering::Relaxed), read_time_ms);
        assert_eq!(reloaded_store.curr_queue_offset.load(Ordering::Relaxed), 9);
    }

    #[test]
    fn commit_durable_progress_orders_data_before_checkpoint_and_fails_closed() {
        let expected_order = [
            PersistenceStage::BeforeTimerLog,
            PersistenceStage::AfterTimerLog,
            PersistenceStage::AfterTimerWheel,
            PersistenceStage::AfterCheckpoint,
        ];

        let order_root = tempdir().unwrap();
        let order_store = standalone_timer_store(config_with_root(order_root.path().to_string_lossy().as_ref()));
        assert!(order_store.load());
        let observed = Arc::new(ParkingMutex::new(Vec::new()));
        let observed_for_hook = Arc::clone(&observed);
        *order_store.persistence_observer.lock() = Some(Arc::new(move |stage| {
            observed_for_hook.lock().push(stage);
            Ok(())
        }));
        order_store.commit_durable_progress().unwrap();
        assert_eq!(observed.lock().as_slice(), expected_order.as_slice());

        for (failed_stage, checkpoint_may_advance) in [
            (PersistenceStage::BeforeTimerLog, false),
            (PersistenceStage::AfterTimerLog, false),
            (PersistenceStage::AfterTimerWheel, false),
            (PersistenceStage::AfterCheckpoint, true),
        ] {
            let root = tempdir().unwrap();
            let root_path = root.path().to_string_lossy().to_string();
            let store = standalone_timer_store(config_with_root(root_path.as_str()));
            assert!(store.load());
            store.curr_queue_offset.store(7, Ordering::Release);
            let failure = failed_stage;
            *store.persistence_observer.lock() = Some(Arc::new(move |stage| {
                if stage == failure {
                    Err(std::io::Error::other(format!("fail at {stage:?}")))
                } else {
                    Ok(())
                }
            }));

            assert!(store.commit_durable_progress().is_err());

            let recovered = TimerCheckpoint::new(get_timer_check_path(root_path.as_str())).unwrap();
            assert_eq!(
                recovered.last_timer_queue_offset(),
                if checkpoint_may_advance { 7 } else { 0 },
                "unexpected checkpoint state after {failed_stage:?}"
            );
        }
    }

    #[tokio::test]
    async fn scheduler_shutdown_gracefully_waits_for_task() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let config = config_with_root(root_dir.as_str());
        let timer_message_store = Arc::new(standalone_timer_store(config));

        assert!(timer_message_store.load());
        timer_message_store.start();
        assert!(timer_message_store.has_scheduler_handle());
        assert_eq!(timer_message_store.scheduler_task_count(), 8);

        let report = timer_message_store
            .shutdown_gracefully_with_report()
            .await
            .expect("timer scheduler shutdown should return report");

        assert!(report.is_healthy(), "{}", report.to_json());
        assert!(!timer_message_store.has_scheduler_handle());
        assert_eq!(timer_message_store.scheduler_task_count(), 0);
    }

    #[test]
    fn load_restores_timer_log_and_wheel_state() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let config = config_with_root(root_dir.as_str());
        let deliver_time_ms = 30_000;

        let timer_message_store = standalone_timer_store(config.clone());
        assert!(timer_message_store.load());

        let record = TimerLogRecord {
            deliver_time_ms,
            commit_log_offset: 11,
            size: 12,
            queue_offset: 13,
            prev_pos: EMPTY_TIMER_LOG_POS,
            magic: MAGIC_DEFAULT,
        };
        let log_offset = timer_message_store.append_timer_log(&record.encode()).unwrap();
        timer_message_store
            .put_timer_wheel_slot(deliver_time_ms, log_offset as i64, log_offset as i64, 1, MAGIC_DEFAULT)
            .unwrap();
        timer_message_store.shutdown();

        let reloaded_store = standalone_timer_store(config);
        assert!(reloaded_store.load());
        assert_eq!(
            reloaded_store.read_timer_log(log_offset, TimerLogRecord::SIZE).unwrap(),
            record.encode()
        );
        assert_eq!(reloaded_store.get_timer_wheel_slot(deliver_time_ms).unwrap().num, 1);
    }

    #[test]
    fn load_truncates_timer_log_tail_and_clears_invalid_wheel_slot() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let config = config_with_root(root_dir.as_str());
        let valid_time_ms = 10_000;
        let invalid_time_ms = 20_000;

        let timer_message_store = standalone_timer_store(config.clone());
        assert!(timer_message_store.load());
        let first_record = TimerLogRecord {
            deliver_time_ms: valid_time_ms,
            commit_log_offset: 1,
            size: 2,
            queue_offset: 3,
            prev_pos: EMPTY_TIMER_LOG_POS,
            magic: MAGIC_DEFAULT,
        };
        let second_record = TimerLogRecord {
            deliver_time_ms: valid_time_ms,
            commit_log_offset: 4,
            size: 5,
            queue_offset: 6,
            prev_pos: 0,
            magic: MAGIC_DEFAULT,
        };
        let first_offset = timer_message_store.append_timer_log(&first_record.encode()).unwrap();
        let second_offset = timer_message_store.append_timer_log(&second_record.encode()).unwrap();
        timer_message_store
            .put_timer_wheel_slot(
                valid_time_ms,
                first_offset as i64,
                second_offset as i64,
                2,
                MAGIC_DEFAULT,
            )
            .unwrap();
        timer_message_store.sync_last_read_time_ms();
        timer_message_store.shutdown();

        let timer_log = TimerLog::new(get_timer_log_path(root_dir.as_str()), config.mapped_file_size_timer_log);
        assert!(timer_log.load().unwrap());
        let tail_offset = timer_log.append(&first_record.encode()).unwrap();
        let timer_wheel = TimerWheel::new(
            get_timer_wheel_path(root_dir.as_str()),
            TIMER_WHEEL_TTL_DAY as usize * DAY_SECS as usize,
            config.timer_precision_ms,
        );
        timer_wheel.load().unwrap();
        timer_wheel
            .put_slot(
                invalid_time_ms,
                tail_offset as i64,
                tail_offset as i64,
                1,
                MAGIC_DEFAULT,
            )
            .unwrap();
        timer_wheel.flush().unwrap();

        let reloaded_store = standalone_timer_store(config);
        assert!(reloaded_store.load());
        assert_eq!(
            reloaded_store.timer_log.lock().as_ref().unwrap().len().unwrap(),
            (TimerLogRecord::SIZE * 2) as u64
        );
        assert_eq!(reloaded_store.get_timer_wheel_slot(valid_time_ms).unwrap().num, 2);
        assert!(reloaded_store.get_timer_wheel_slot(invalid_time_ms).is_none());
    }

    #[test]
    fn sync_checkpoint_from_master_keeps_local_read_cursor_until_caught_up() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let config = config_with_root(root_dir.as_str());
        let timer_message_store = standalone_timer_store(config);
        assert!(timer_message_store.load());
        let local_read_time = timer_message_store.curr_read_time_ms.load(Ordering::Relaxed);

        let mut data_version = rocketmq_protocol::protocol::data_version_facade::new_data_version();
        data_version.next_version_with(88);
        let snapshot = TimerCheckpointSnapshot::new(12_000, 34, 56, 78, data_version.clone());

        assert!(timer_message_store.sync_checkpoint_from_master(&snapshot).unwrap());
        assert_eq!(
            timer_message_store.curr_read_time_ms.load(Ordering::Relaxed),
            local_read_time
        );

        let checkpoint_guard = timer_message_store.timer_checkpoint.lock();
        let checkpoint = checkpoint_guard.as_ref().expect("checkpoint should exist");
        assert_eq!(checkpoint.last_read_time_ms(), local_read_time);
        assert_eq!(checkpoint.master_timer_queue_offset(), 78);
        assert_eq!(checkpoint.data_version(), data_version);

        drop(checkpoint_guard);
        let reloaded_checkpoint = TimerCheckpoint::new(get_timer_check_path(root_dir.as_str())).unwrap();
        assert_eq!(reloaded_checkpoint.last_read_time_ms(), local_read_time);
        assert_eq!(reloaded_checkpoint.master_timer_queue_offset(), 78);
        assert_eq!(reloaded_checkpoint.data_version(), data_version);
    }

    #[test]
    fn sync_checkpoint_from_master_waits_for_active_timer_mutations() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let timer_message_store = standalone_timer_store(config_with_root(root_dir.as_str()));
        assert!(timer_message_store.load());

        let snapshot = TimerCheckpointSnapshot::new(
            12_000,
            34,
            56,
            78,
            rocketmq_protocol::protocol::data_version_facade::new_data_version(),
        );
        timer_message_store.active_source_mutations.store(1, Ordering::Release);
        assert!(!timer_message_store.sync_checkpoint_from_master(&snapshot).unwrap());
        assert_ne!(
            timer_message_store
                .timer_checkpoint
                .lock()
                .as_ref()
                .expect("checkpoint")
                .master_timer_queue_offset(),
            78
        );
        timer_message_store.active_source_mutations.store(0, Ordering::Release);
        assert!(timer_message_store.sync_checkpoint_from_master(&snapshot).unwrap());
    }

    #[tokio::test]
    async fn sync_checkpoint_from_master_does_not_rebucket_pending_timer_messages() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (mut store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.load().await);
        let local_read_time = timer_message_store.curr_read_time_ms.load(Ordering::Relaxed);
        let deliver_time_ms = (local_read_time as u64).saturating_add(60_000);
        let master_read_time_ms = local_read_time + 120_000;

        let mut data_version = rocketmq_protocol::protocol::data_version_facade::new_data_version();
        data_version.next_version_with(99);
        let snapshot = TimerCheckpointSnapshot::new(master_read_time_ms, 0, 0, 10, data_version);

        assert!(timer_message_store.sync_checkpoint_from_master(&snapshot).unwrap());
        assert!(store
            .put_message(build_timer_message(&real_topic, deliver_time_ms))
            .await
            .is_ok());
        store.reput_once().await;

        let indexed = timer_message_store.process_once().await;
        let expected_slot_time = timer_message_store.ceil_time_ms(deliver_time_ms as i64);

        assert_eq!(indexed, 1);
        assert_eq!(
            timer_message_store.curr_read_time_ms.load(Ordering::Relaxed),
            local_read_time
        );
        assert_eq!(
            timer_message_store
                .get_timer_wheel_slot(expected_slot_time)
                .unwrap()
                .num,
            1
        );
        assert!(timer_message_store.get_timer_wheel_slot(master_read_time_ms).is_none());
    }

    #[tokio::test]
    async fn process_once_indexes_timer_topic_messages_without_delivery_when_dequeue_disabled() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (mut store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.load().await);
        let deliver_ms = current_millis() + 60_000;
        let put_result = store.put_message(build_timer_message(&real_topic, deliver_ms)).await;
        assert!(put_result.is_ok());
        store.reput_once().await;

        let indexed = timer_message_store.process_once().await;
        let slot_time_ms = timer_message_store.ceil_time_ms(deliver_ms as i64);

        assert_eq!(indexed, 1);
        assert_eq!(timer_message_store.curr_queue_offset.load(Ordering::Relaxed), 1);
        assert_eq!(timer_message_store.get_timer_wheel_slot(slot_time_ms).unwrap().num, 1);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 0);
    }

    #[tokio::test]
    async fn process_once_redelivers_due_timer_message_when_dequeue_enabled() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (mut store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.load().await);
        timer_message_store.set_should_running_dequeue(true);
        let deliver_ms = current_millis().saturating_sub(2_000);
        let put_result = store.put_message(build_timer_message(&real_topic, deliver_ms)).await;
        assert!(put_result.is_ok());
        store.reput_once().await;

        let processed = timer_message_store.process_once().await;
        store.reput_once().await;

        assert_eq!(processed, 2);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 1);
        let queue = store.find_consume_queue(&real_topic, 0).unwrap();
        let queue_unit = queue.read().get(0).unwrap();
        let delivered = store
            .look_message_by_offset_with_size(queue_unit.pos, queue_unit.size)
            .unwrap();
        assert_eq!(delivered.topic(), &real_topic);
        assert_eq!(delivered.body().unwrap(), Bytes::from_static(b"phase3-body"));
        assert!(delivered
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_OUT_MS))
            .is_none());
        assert!(delivered
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_ENQUEUE_MS))
            .is_none());
        assert!(delivered
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DEQUEUE_MS))
            .is_none());
        assert!(delivered
            .property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN,
            ))
            .is_some());
    }

    #[tokio::test]
    async fn process_once_does_not_redeliver_future_timer_message_before_due_time() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (mut store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.load().await);
        timer_message_store.set_should_running_dequeue(true);
        let deliver_ms = current_millis() + 60_000;
        let put_result = store.put_message(build_timer_message(&real_topic, deliver_ms)).await;
        assert!(put_result.is_ok());
        store.reput_once().await;

        let processed = timer_message_store.process_once().await;
        let slot_time_ms = timer_message_store.ceil_time_ms(deliver_ms as i64);

        assert_eq!(processed, 1);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 0);
        assert_eq!(timer_message_store.get_timer_wheel_slot(slot_time_ms).unwrap().num, 1);
    }

    #[tokio::test]
    async fn process_once_restores_real_queue_and_removes_internal_routing_properties() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (mut store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.load().await);
        timer_message_store.set_should_running_dequeue(true);
        let deliver_ms = current_millis().saturating_sub(2_000);
        let put_result = store
            .put_message(build_timer_message_with_queue_id(&real_topic, 3, deliver_ms))
            .await;
        assert!(put_result.is_ok());
        store.reput_once().await;

        assert_eq!(timer_message_store.process_once().await, 2);
        store.reput_once().await;

        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 0);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 3), 1);
        let queue = store.find_consume_queue(&real_topic, 3).unwrap();
        let queue_unit = queue.read().get(0).unwrap();
        let delivered = store
            .look_message_by_offset_with_size(queue_unit.pos, queue_unit.size)
            .unwrap();
        assert_eq!(delivered.topic(), &real_topic);
        assert_eq!(delivered.queue_id(), 3);
        assert!(delivered
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC))
            .is_none());
        assert!(delivered
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_QUEUE_ID))
            .is_none());
        assert!(delivered
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_OUT_MS))
            .is_none());
        assert!(delivered
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_ENQUEUE_MS))
            .is_none());
        assert!(delivered
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DEQUEUE_MS))
            .is_none());
        assert!(delivered
            .property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN,
            ))
            .is_some());
    }

    #[tokio::test]
    async fn process_once_pauses_enqueue_after_role_change_when_master_progress_is_caught_up() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (mut store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.load().await);
        timer_message_store.set_should_running_dequeue(true);
        let first_deliver_ms = current_millis() + 60_000;
        assert!(store
            .put_message(build_timer_message(&real_topic, first_deliver_ms))
            .await
            .is_ok());
        store.reput_once().await;
        assert_eq!(timer_message_store.process_once().await, 1);
        timer_message_store.set_should_running_dequeue(false);

        let second_deliver_ms = current_millis() + 120_000;
        assert!(store
            .put_message(build_timer_message(&real_topic, second_deliver_ms))
            .await
            .is_ok());
        store.reput_once().await;

        let paused = timer_message_store.process_once().await;

        assert_eq!(paused, 0);
        assert_eq!(timer_message_store.curr_queue_offset.load(Ordering::Relaxed), 1);
        let first_slot_time = timer_message_store.ceil_time_ms(first_deliver_ms as i64);
        let second_slot_time = timer_message_store.ceil_time_ms(second_deliver_ms as i64);
        assert_eq!(
            timer_message_store.get_timer_wheel_slot(first_slot_time).unwrap().num,
            1
        );
        assert!(timer_message_store.get_timer_wheel_slot(second_slot_time).is_none());
    }

    #[tokio::test]
    async fn process_once_resume_after_role_change_processes_pending_due_messages() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (mut store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        let clock = Arc::new(ManualTimerClock::new(1_700_000_000_000));
        timer_message_store.set_clock(clock.clone());
        assert!(store.load().await);
        timer_message_store.set_should_running_dequeue(true);
        let first_deliver_ms = clock.wall_time_ms() as u64 - 2_000;
        assert!(store
            .put_message(build_timer_message(&real_topic, first_deliver_ms))
            .await
            .is_ok());
        store.reput_once().await;
        assert_eq!(timer_message_store.process_once().await, 2);
        store.reput_once().await;
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 1);

        timer_message_store.set_should_running_dequeue(false);
        let second_deliver_ms = clock.wall_time_ms() as u64 - 1_000;
        assert!(store
            .put_message(build_timer_message(&real_topic, second_deliver_ms))
            .await
            .is_ok());
        store.reput_once().await;
        assert_eq!(timer_message_store.process_once().await, 0);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 1);

        timer_message_store.set_should_running_dequeue(true);
        let resumed_indexed = timer_message_store.process_once().await;
        clock.advance(timer_message_store.precision_ms());
        let resumed_delivered = timer_message_store.process_once().await;
        store.reput_once().await;

        assert_eq!(resumed_indexed, 1);
        assert_eq!(resumed_delivered, 1);
        assert_eq!(timer_message_store.curr_queue_offset.load(Ordering::Relaxed), 2);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 2);
    }

    #[test]
    fn set_should_running_dequeue_clamps_future_read_cursor_on_resume() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let timer_message_store = standalone_timer_store(config_with_root(root_dir.as_str()));

        assert!(timer_message_store.load());
        let now_floor = timer_message_store.floor_time_ms(current_millis() as i64);
        let future_read_time = now_floor + timer_message_store.precision_ms() * 120;
        {
            let checkpoint_guard = timer_message_store.timer_checkpoint.lock();
            let checkpoint = checkpoint_guard.as_ref().unwrap();
            checkpoint.set_last_read_time_ms(future_read_time);
            checkpoint.flush().unwrap();
        }

        timer_message_store.set_should_running_dequeue(true);

        let restored_read_time = timer_message_store.curr_read_time_ms.load(Ordering::Relaxed);
        let now_after_resume = timer_message_store.floor_time_ms(current_millis() as i64);
        assert!(restored_read_time >= now_floor);
        assert!(restored_read_time <= now_after_resume);
        assert!(restored_read_time < future_read_time);
    }

    #[test]
    fn role_change_fences_a_stale_delivery_epoch() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let timer_store = standalone_timer_store(config_with_root(root_dir.as_str()));
        assert!(timer_store.load());

        timer_store.set_should_running_dequeue(true);
        let captured_epoch = timer_store
            .role_state
            .capture_delivery_epoch()
            .expect("active delivery epoch");
        timer_store.set_should_running_dequeue(false);

        assert!(!timer_store.role_state.is_current_delivery_epoch(captured_epoch));
        assert!(!timer_store.is_should_running_dequeue());
    }

    #[tokio::test]
    async fn process_once_delete_tombstone_skips_matching_timer_message_delivery() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (mut store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.load().await);
        timer_message_store.set_should_running_dequeue(true);
        let deliver_ms = current_millis().saturating_sub(2_000);
        let unique_key = "delete-me";
        let mut timer_message = build_timer_message(&real_topic, deliver_ms);
        timer_message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from_static_str(unique_key),
        );
        timer_message.properties_string = message_properties_to_string(timer_message.get_properties());
        assert!(store.put_message(timer_message).await.is_ok());
        assert!(store
            .put_message(build_delete_timer_message(&real_topic, unique_key, deliver_ms))
            .await
            .is_ok());
        store.reput_once().await;

        let processed = timer_message_store.process_once().await;
        store.reput_once().await;

        assert_eq!(processed, 4);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 0);
        let slot_time = timer_message_store.curr_read_time_ms.load(Ordering::Relaxed);
        assert!(timer_message_store
            .get_timer_wheel_slot(slot_time)
            .is_none_or(|slot| slot.num == 0));
    }

    #[tokio::test]
    async fn process_once_delete_tombstone_only_cancels_matching_unique_key() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (mut store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.load().await);
        timer_message_store.set_should_running_dequeue(true);
        let deliver_ms = current_millis().saturating_sub(2_000);
        let mut deleted_message = build_timer_message(&real_topic, deliver_ms);
        deleted_message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from_static_str("deleted-key"),
        );
        deleted_message.properties_string = message_properties_to_string(deleted_message.get_properties());
        assert!(store.put_message(deleted_message).await.is_ok());

        let mut survivor_message = build_timer_message(&real_topic, deliver_ms);
        survivor_message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from_static_str("survivor-key"),
        );
        survivor_message.properties_string = message_properties_to_string(survivor_message.get_properties());
        assert!(store.put_message(survivor_message).await.is_ok());

        assert!(store
            .put_message(build_delete_timer_message(&real_topic, "deleted-key", deliver_ms))
            .await
            .is_ok());
        store.reput_once().await;

        let processed = timer_message_store.process_once().await;
        store.reput_once().await;

        assert_eq!(processed, 6);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 1);
    }

    #[tokio::test]
    async fn recall_cross_batch_scans_the_complete_slot_before_delivery() {
        for tombstone_position in [2usize, 10, 100] {
            let temp_dir = tempdir().unwrap();
            let root_dir = temp_dir.path().to_string_lossy().to_string();
            let config = config_with_root_and_limits(root_dir.as_str(), 1, 1, 1);
            let (mut store, timer_store, real_topic) = build_store_with_timer_and_config(config);
            assert!(store.load().await);
            let deliver_ms = current_millis().saturating_sub(2_000);

            let mut target = build_timer_message(&real_topic, deliver_ms);
            target.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                CheetahString::from_static_str("cross-batch-target"),
            );
            target.properties_string = message_properties_to_string(target.get_properties());
            assert!(store.put_message(target).await.is_ok());

            for filler_index in 1..tombstone_position - 1 {
                let mut filler = build_timer_message(&real_topic, deliver_ms);
                filler.put_property(
                    CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                    CheetahString::from_string(format!("filler-{filler_index}")),
                );
                filler.properties_string = message_properties_to_string(filler.get_properties());
                assert!(store.put_message(filler).await.is_ok());
            }
            assert!(store
                .put_message(build_java_delete_timer_message(
                    &real_topic,
                    "cross-batch-target",
                    deliver_ms,
                ))
                .await
                .is_ok());
            store.reput_once().await;

            timer_store.set_should_running_dequeue(true);
            let _ = timer_store.process_once().await;
            store.reput_once().await;

            assert_eq!(
                store.get_max_offset_in_queue(&real_topic, 0),
                0,
                "target was delivered before tombstone at slot position {tombstone_position}"
            );
        }
    }

    #[tokio::test]
    async fn restart_before_slot_completion_replays_earlier_tombstone() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let config = config_with_root_and_limits(root_dir.as_str(), 1, 1, 1);
        let (mut store, timer_store, real_topic) = build_store_with_timer_and_config(config.clone());
        assert!(store.load().await);
        let deliver_ms = current_millis().saturating_sub(2_000);

        assert!(store
            .put_message(build_java_delete_timer_message(
                &real_topic,
                "restart-recall-target",
                deliver_ms,
            ))
            .await
            .is_ok());
        let mut target = build_timer_message(&real_topic, deliver_ms);
        target.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from_static_str("restart-recall-target"),
        );
        target.properties_string = message_properties_to_string(target.get_properties());
        assert!(store.put_message(target).await.is_ok());
        store.reput_once().await;

        timer_store.set_should_running_dequeue(true);
        assert_eq!(timer_store.process_once().await, 3);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 0);
        drop(timer_store);
        store.shutdown().await;
        drop(store);

        let (mut reloaded, reloaded_timer, _) = build_store_with_timer_and_config(config);
        assert!(reloaded.load().await);
        reloaded_timer.set_should_running_dequeue(true);
        // Source materialization was durably checkpointed before the partial slot drain. Restart
        // therefore replays only the slot, not the already indexed source records.
        assert_eq!(reloaded_timer.process_once().await, 1);
        assert_eq!(reloaded_timer.process_once().await, 1);
        reloaded.reput_once().await;
        assert_eq!(reloaded.get_max_offset_in_queue(&real_topic, 0), 0);
    }

    #[tokio::test]
    async fn hot_slot_with_fifty_percent_recall_delivers_only_survivors() {
        const MESSAGE_COUNT: usize = 100;

        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (mut store, timer_store, real_topic) = build_store_with_timer(root_dir.as_str());
        assert!(store.load().await);
        let deliver_ms = current_millis().saturating_sub(2_000);

        for index in 0..MESSAGE_COUNT {
            let unique_key = format!("recall-half-{index}");
            let mut message = build_timer_message(&real_topic, deliver_ms);
            message.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                CheetahString::from_string(unique_key),
            );
            message.properties_string = message_properties_to_string(message.get_properties());
            assert!(store.put_message(message).await.is_ok());
        }
        for index in 0..MESSAGE_COUNT / 2 {
            assert!(store
                .put_message(build_java_delete_timer_message(
                    &real_topic,
                    format!("recall-half-{index}").as_str(),
                    deliver_ms,
                ))
                .await
                .is_ok());
        }
        store.reput_once().await;

        timer_store.set_should_running_dequeue(true);
        let mut processed = 0usize;
        loop {
            let processed_now = timer_store.process_once().await;
            if processed_now == 0 {
                break;
            }
            processed += processed_now;
        }
        assert_eq!(processed, (MESSAGE_COUNT + MESSAGE_COUNT / 2) * 2);
        store.reput_once().await;

        assert_eq!(
            store.get_max_offset_in_queue(&real_topic, 0),
            (MESSAGE_COUNT / 2) as i64
        );
        assert_eq!(
            timer_store.storage_metrics_snapshot().hot_slot_scanned_records,
            (MESSAGE_COUNT + MESSAGE_COUNT / 2) as u64
        );
    }

    #[tokio::test]
    async fn restart_recovers_indexed_due_timer_message_from_persisted_wheel() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (mut store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.load().await);
        let deliver_ms = current_millis().saturating_sub(2_000);
        let put_result = store.put_message(build_timer_message(&real_topic, deliver_ms)).await;
        assert!(put_result.is_ok());
        store.reput_once().await;

        assert_eq!(timer_message_store.process_once().await, 1);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 0);
        drop(timer_message_store);
        store.shutdown().await;
        drop(store);

        let (mut reloaded_store, reloaded_timer_message_store, reloaded_topic) =
            build_store_with_timer(root_dir.as_str());
        assert_eq!(reloaded_topic, real_topic);
        assert!(reloaded_store.load().await);
        reloaded_timer_message_store.set_should_running_dequeue(true);

        let delivered = reloaded_timer_message_store.process_once().await;
        reloaded_store.reput_once().await;

        assert_eq!(delivered, 1);
        assert_eq!(
            reloaded_timer_message_store.curr_queue_offset.load(Ordering::Relaxed),
            1
        );
        assert_eq!(reloaded_store.get_max_offset_in_queue(&real_topic, 0), 1);
    }

    #[tokio::test]
    async fn restart_allows_duplicate_delivery_when_checkpoint_lags_after_delivery() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (mut store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.load().await);
        timer_message_store.set_should_running_dequeue(true);
        let deliver_ms = current_millis().saturating_sub(2_000);
        let put_result = store.put_message(build_timer_message(&real_topic, deliver_ms)).await;
        assert!(put_result.is_ok());
        store.reput_once().await;
        assert_eq!(timer_message_store.process_once().await, 2);
        store.reput_once().await;
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 1);
        drop(timer_message_store);
        store.shutdown().await;
        drop(store);

        let checkpoint = TimerCheckpoint::new(get_timer_check_path(root_dir.as_str())).unwrap();
        checkpoint.set_last_timer_queue_offset(0);
        checkpoint.set_master_timer_queue_offset(0);
        checkpoint.flush().unwrap();

        let (mut reloaded_store, reloaded_timer_message_store, reloaded_topic) =
            build_store_with_timer(root_dir.as_str());
        assert_eq!(reloaded_topic, real_topic);
        assert!(reloaded_store.load().await);
        reloaded_timer_message_store.set_should_running_dequeue(true);
        let restart_clock = Arc::new(ManualTimerClock::new(
            reloaded_timer_message_store.curr_read_time_ms.load(Ordering::Relaxed)
                + reloaded_timer_message_store.precision_ms() * 2,
        ));
        reloaded_timer_message_store.set_clock(restart_clock);

        let reprocessed = reloaded_timer_message_store.process_once().await;
        reloaded_store.reput_once().await;

        assert_eq!(reprocessed, 2);
        assert_eq!(reloaded_store.get_max_offset_in_queue(&real_topic, 0), 2);
    }

    #[tokio::test]
    async fn restart_rebuilds_committed_timer_wheel_from_segmented_log() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (mut store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.load().await);
        let deliver_ms = current_millis() + 60_000;
        assert!(store
            .put_message(build_timer_message(&real_topic, deliver_ms))
            .await
            .is_ok());
        store.reput_once().await;
        assert_eq!(timer_message_store.process_once().await, 1);
        timer_message_store.sync_last_read_time_ms();
        timer_message_store.sync_last_read_time_ms();
        drop(timer_message_store);
        store.shutdown().await;
        drop(store);

        let wheel_directory = PathBuf::from(format!("{}.v2", get_timer_wheel_path(root_dir.as_str())));
        for copy in ["pages.a", "pages.b"] {
            OpenOptions::new()
                .write(true)
                .open(wheel_directory.join(copy))
                .unwrap()
                .set_len(0)
                .unwrap();
        }

        let (mut reloaded_store, reloaded_timer_message_store, _) = build_store_with_timer(root_dir.as_str());
        assert!(reloaded_store.load().await);
        let metrics = reloaded_timer_message_store.storage_metrics_snapshot();
        let rebuilt_slot = reloaded_timer_message_store
            .get_timer_wheel_slot(reloaded_timer_message_store.ceil_time_ms(deliver_ms as i64))
            .unwrap_or_else(|| panic!("pending timer slot should be rebuilt; metrics={metrics:?}"));
        assert_eq!(rebuilt_slot.num, 1);
        assert!(metrics.recovery_replay_records >= 1);
        assert!(metrics.wheel_repair_pages >= 1);
    }

    #[tokio::test]
    async fn load_revises_checkpoint_queue_offset_to_timer_queue_max() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (mut store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.load().await);
        let deliver_ms = current_millis() + 60_000;
        let put_result = store.put_message(build_timer_message(&real_topic, deliver_ms)).await;
        assert!(put_result.is_ok());
        store.reput_once().await;
        assert_eq!(timer_message_store.process_once().await, 1);
        drop(timer_message_store);
        store.shutdown().await;
        drop(store);

        let checkpoint = TimerCheckpoint::new(get_timer_check_path(root_dir.as_str())).unwrap();
        checkpoint.set_last_timer_queue_offset(99);
        checkpoint.set_master_timer_queue_offset(99);
        checkpoint.flush().unwrap();

        let (mut reloaded_store, reloaded_timer_message_store, _) = build_store_with_timer(root_dir.as_str());
        assert!(reloaded_store.load().await);
        assert_eq!(
            reloaded_timer_message_store.curr_queue_offset.load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn process_once_limits_enqueue_work_per_tick() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let config = config_with_root_and_limits(root_dir.as_str(), 1, 1, 1);
        let (mut store, timer_message_store, real_topic) = build_store_with_timer_and_config(config);

        assert!(store.load().await);
        let first_put = store
            .put_message(build_timer_message(&real_topic, current_millis() + 60_000))
            .await;
        assert!(first_put.is_ok());
        let second_put = store
            .put_message(build_timer_message(&real_topic, current_millis() + 61_000))
            .await;
        assert!(second_put.is_ok());
        store.reput_once().await;

        let first_tick = timer_message_store.process_once().await;
        let second_tick = timer_message_store.process_once().await;

        assert_eq!(first_tick, 1);
        assert_eq!(timer_message_store.curr_queue_offset.load(Ordering::Relaxed), 2);
        assert_eq!(second_tick, 1);
    }

    #[tokio::test]
    async fn process_once_limits_delivery_work_per_tick_for_hot_slot() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let config = config_with_root_and_limits(root_dir.as_str(), 1, 1, 1);
        let (mut store, timer_message_store, real_topic) = build_store_with_timer_and_config(config);

        assert!(store.load().await);
        let deliver_ms = current_millis().saturating_sub(2_000);
        let first_put = store.put_message(build_timer_message(&real_topic, deliver_ms)).await;
        assert!(first_put.is_ok());
        let second_put = store.put_message(build_timer_message(&real_topic, deliver_ms)).await;
        assert!(second_put.is_ok());
        store.reput_once().await;

        assert_eq!(timer_message_store.process_once().await, 1);
        assert_eq!(timer_message_store.process_once().await, 1);
        let hot_slot_time = timer_message_store.curr_read_time_ms.load(Ordering::Relaxed);
        timer_message_store.set_should_running_dequeue(true);

        let first_delivery_tick = timer_message_store.process_once().await;
        store.reput_once().await;

        assert_eq!(first_delivery_tick, 1);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 1);
        let remaining_timer_messages = timer_message_store
            .timer_wheel
            .lock()
            .as_ref()
            .map(TimerWheel::slots_snapshot)
            .unwrap_or_default()
            .into_iter()
            .map(|slot| slot.num.max(0))
            .sum::<i32>();
        assert_eq!(remaining_timer_messages, 1);

        let second_delivery_tick = timer_message_store.process_once().await;
        store.reput_once().await;

        assert_eq!(second_delivery_tick, 1);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 2);
        assert!(timer_message_store
            .get_timer_wheel_slot(hot_slot_time)
            .is_none_or(|slot| slot.num == 0));
    }

    #[tokio::test]
    async fn process_once_rolls_far_future_timer_message_into_near_window_slot() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let config = config_with_root_precision_and_roll_window(root_dir.as_str(), 1_000, 4);
        let (mut store, timer_message_store, real_topic) = build_store_with_timer_and_config(config);

        assert!(store.load().await);
        let now_floor = timer_message_store.floor_time_ms(current_millis() as i64);
        let deliver_ms = (now_floor + 20_000) as u64;
        assert!(store
            .put_message(build_timer_message(&real_topic, deliver_ms))
            .await
            .is_ok());
        store.reput_once().await;

        assert_eq!(timer_message_store.process_once().await, 1);

        let original_slot_time = timer_message_store.ceil_time_ms(deliver_ms as i64);
        let active_slots = timer_message_store
            .timer_wheel
            .lock()
            .as_ref()
            .map(TimerWheel::slots_snapshot)
            .unwrap_or_default()
            .into_iter()
            .filter(|slot| slot.num > 0)
            .collect::<Vec<_>>();
        assert_eq!(active_slots.len(), 1);

        let slot = active_slots[0];
        let rolled_slot_time = slot.time_ms;
        let entries = timer_message_store.load_slot_entries(slot).unwrap();
        let now_after_process = timer_message_store.floor_time_ms(current_millis() as i64);

        assert!(timer_message_store.get_timer_wheel_slot(original_slot_time).is_none());
        assert_eq!(slot.num, 1);
        assert_eq!(entries.len(), 1);
        assert_ne!(entries[0].record.magic & MAGIC_ROLL, 0);
        assert!(rolled_slot_time >= now_floor + timer_message_store.timer_roll_window_ms());
        assert!(rolled_slot_time <= now_after_process + timer_message_store.timer_roll_window_ms());
        assert!(rolled_slot_time < original_slot_time);
        assert_eq!(entries[0].record.deliver_time_ms, rolled_slot_time);
    }

    #[tokio::test]
    async fn process_once_clamps_future_read_cursor_when_clock_moves_backward() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let config = config_with_root_precision_and_roll_window(root_dir.as_str(), 1_000, 60);
        let (mut store, timer_message_store, real_topic) = build_store_with_timer_and_config(config);

        assert!(store.load().await);
        let now_floor = timer_message_store.floor_time_ms(current_millis() as i64);
        timer_message_store
            .curr_read_time_ms
            .store(now_floor + 10_000, Ordering::Relaxed);

        let deliver_ms = (now_floor + 2_000) as u64;
        assert!(store
            .put_message(build_timer_message(&real_topic, deliver_ms))
            .await
            .is_ok());
        store.reput_once().await;

        assert_eq!(timer_message_store.process_once().await, 1);

        let slot = timer_message_store
            .get_timer_wheel_slot(timer_message_store.ceil_time_ms(deliver_ms as i64))
            .unwrap();
        let entries = timer_message_store.load_slot_entries(slot).unwrap();

        assert_eq!(
            entries[0].record.deliver_time_ms,
            timer_message_store.ceil_time_ms(deliver_ms as i64)
        );
    }

    #[tokio::test]
    async fn process_once_rolls_due_message_back_to_timer_topic_with_tracking_properties() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let config = config_with_root_precision_and_roll_window(root_dir.as_str(), 100, 4);
        let (mut store, timer_message_store, real_topic) = build_store_with_timer_and_config(config);

        let clock = Arc::new(ManualTimerClock::new(1_700_000_000_000));
        timer_message_store.set_clock(clock.clone());
        assert!(store.load().await);
        let deliver_ms = clock.wall_time_ms() as u64 + 3_000;
        assert!(store
            .put_message(build_timer_message(&real_topic, deliver_ms))
            .await
            .is_ok());
        store.reput_once().await;
        assert_eq!(timer_message_store.process_once().await, 1);

        timer_message_store.set_should_running_dequeue(true);
        clock.advance(500);

        let processed = timer_message_store.process_once().await;
        store.reput_once().await;

        assert_eq!(processed, 1);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 0);
        assert_eq!(
            timer_message_store.timer_log.lock().as_ref().unwrap().len().unwrap(),
            TimerLogRecord::SIZE as u64
        );
        assert_eq!(
            store.get_max_offset_in_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0),
            2
        );
        let timer_queue = store
            .find_consume_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0)
            .unwrap();
        let rolled_queue_unit = timer_queue.read().get(1).unwrap();
        let rolled_message = store
            .look_message_by_offset_with_size(rolled_queue_unit.pos, rolled_queue_unit.size)
            .unwrap();
        assert_eq!(rolled_message.topic().as_str(), TIMER_TOPIC);
        assert_eq!(
            rolled_message
                .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_ROLL_TIMES))
                .as_ref()
                .map(CheetahString::as_str),
            Some("1")
        );
        assert!(rolled_message
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_ENQUEUE_MS))
            .is_some());
        assert!(rolled_message
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DEQUEUE_MS))
            .is_some());
    }

    #[tokio::test]
    async fn process_once_reports_enqueue_tps_after_indexing_timer_messages() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (mut store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.load().await);
        assert!(store
            .put_message(build_timer_message(&real_topic, current_millis() + 60_000))
            .await
            .is_ok());
        store.reput_once().await;

        assert_eq!(timer_message_store.process_once().await, 1);
        assert!(timer_message_store.get_enqueue_tps() > 0.0);
    }

    #[tokio::test]
    async fn process_once_reports_dequeue_tps_after_redelivery() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (mut store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.load().await);
        timer_message_store.set_should_running_dequeue(true);
        assert!(store
            .put_message(build_timer_message(&real_topic, current_millis().saturating_sub(2_000)))
            .await
            .is_ok());
        store.reput_once().await;

        assert_eq!(timer_message_store.process_once().await, 2);
        assert!(timer_message_store.get_dequeue_tps() > 0.0);
    }

    #[tokio::test]
    async fn get_runtime_info_reports_timer_topic_backlog_distribution() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (mut store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.load().await);
        assert!(store
            .put_message(build_timer_message(&real_topic, current_millis() + 60_000))
            .await
            .is_ok());
        assert!(store
            .put_message(build_timer_message(&real_topic, current_millis() + 120_000))
            .await
            .is_ok());
        store.reput_once().await;
        assert_eq!(timer_message_store.process_once().await, 2);

        let runtime_info = store.get_runtime_info();
        let topic_distribution: HashMap<String, i64> =
            serde_json::from_str(runtime_info.get("timerTopicBacklogDistribution").unwrap()).unwrap();
        let timer_backlog_distribution: HashMap<String, i64> =
            serde_json::from_str(runtime_info.get("timerBacklogDistribution").unwrap()).unwrap();

        assert_eq!(topic_distribution.get(real_topic.as_str()).copied(), Some(2));
        assert_eq!(timer_backlog_distribution.values().sum::<i64>(), 2);
    }

    #[tokio::test]
    async fn load_with_metrics_check_enabled_revises_small_topic_metrics() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let config = config_with_metrics_check(root_dir.as_str(), 10);
        let (mut store, timer_message_store, real_topic) = build_store_with_timer_and_config(config.clone());

        assert!(store.load().await);
        let deliver_ms = current_millis() + 60_000;
        let unique_key = "revise-key";
        let mut timer_message = build_timer_message(&real_topic, deliver_ms);
        timer_message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from_static_str(unique_key),
        );
        timer_message.properties_string = message_properties_to_string(timer_message.get_properties());
        assert!(store.put_message(timer_message).await.is_ok());
        assert!(store
            .put_message(build_delete_timer_message(&real_topic, unique_key, deliver_ms))
            .await
            .is_ok());
        store.reput_once().await;
        assert_eq!(timer_message_store.process_once().await, 2);
        assert_eq!(timer_message_store.timer_metrics.get_timing_count(&real_topic), 1);
        drop(timer_message_store);
        store.shutdown().await;
        drop(store);

        let (mut reloaded_store, reloaded_timer_message_store, _) = build_store_with_timer_and_config(config);
        assert!(reloaded_store.load().await);

        assert_eq!(
            reloaded_timer_message_store.timer_metrics.get_timing_count(&real_topic),
            0
        );
    }

    #[tokio::test]
    async fn timer_processor_does_not_touch_non_timer_messages() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (mut store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.load().await);
        timer_message_store.set_should_running_dequeue(true);
        let mut msg = MessageExtBrokerInner::default();
        msg.set_topic(real_topic.clone());
        msg.message_ext_inner.queue_id = 0;
        msg.set_body(Bytes::from_static(b"ordinary-body"));

        let put_result = store.put_message(msg).await;
        assert!(put_result.is_ok());
        store.reput_once().await;

        assert_eq!(timer_message_store.process_once().await, 0);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 1);
    }
}
