// Copyright 2026 The RocketMQ Rust Authors
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

//! The `native` engine as the store runs it, on the shared timer pipeline.
//!
//! A Source batch reads the Timer CQ from where the engine has indexed up to, and indexes the
//! timers and recalls the Broker tagged for the native engine when it admitted them; records of
//! the other engines are passed over, since their engines read the Timer CQ themselves. A Due
//! batch writes each lane's due timers to their real topics while this Broker holds a current
//! delivery epoch. A checkpoint makes both durable and moves the CommitLog cleanup pin to the
//! engine's fence.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cheetah_string::CheetahString;
use futures_util::future::join_all;
use parking_lot::Mutex;
use parking_lot::RwLock;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_runtime::ScheduledExecutionPolicy;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_store_api::TimerEngineEpoch;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerId;
use thiserror::Error;
use tracing::error;
use tracing::warn;

use super::config::NativeTimerConfig;
use super::delivery::NativeCommitLogRead;
use super::delivery::NativeCommitLogReader;
use super::delivery::NativeDueBatch;
use super::delivery::NativeTimerWriter;
use super::delivery::NativeWriteOutcome;
use super::engine::NativeEngineError;
use super::engine::NativeRecallVerdict;
use super::engine::NativeRejection;
use super::engine::NativeSourceKind;
use super::engine::NativeTimerEngine;
use super::engine::NativeTimerSource;
use crate::base::message_result::PutMessageResult;
use crate::config::message_store_config::MessageStoreConfig;
use crate::queue::CqUnit;
use crate::runtime::StoreRuntimeScope;
use crate::timer::clock::SystemTimerClock;
use crate::timer::clock::TimerClock;
use crate::timer::delivery::classify_delivery_status;
use crate::timer::engine::TimerEngine;
use crate::timer::engine::WorkBudget;
use crate::timer::error::TimerEngineError;
use crate::timer::error::TimerWorkResult;
use crate::timer::pipeline::TimerPipeline;
use crate::timer::request::EngineBatchProgress;
use crate::timer::role::TimerRoleState;
use crate::timer::source::read_timer_source_frames;
use crate::timer::source::TimerSourceRead;
use crate::timer::timer_message_store::parse_deliver_time_ms;
use crate::timer::timer_message_store::timer_generation;
use crate::timer::timer_message_store::TimerStoreContext;
use crate::timer::timer_message_store::MIN_SCHEDULER_INTERVAL_MS;
use crate::timer::timer_message_store::TIMER_DELETE_UNIQUE_KEY;
use crate::timer::timer_message_store::TIMER_TOPIC;
use crate::timer::timer_message_store::TIMER_WRITE_ATTEMPTS;

/// The native engine's delivery lease, beside the Java-compatible engine's.
const ROLE_FILE: &str = "timer-native-role.meta";

/// Why the service could not open.
#[derive(Debug, Error)]
pub(crate) enum NativeServiceError {
    #[error(transparent)]
    Engine(#[from] NativeEngineError),
    #[error("native timer delivery lease cannot be loaded")]
    Role(#[source] std::io::Error),
}

/// What the service takes from the store's settings.
#[derive(Clone, Copy)]
struct Settings {
    lane_count: u16,
    batch_messages: usize,
    inflight_writes: usize,
    skip_unknown_error: bool,
    retry_until_success: bool,
    tick_ms: u64,
}

/// A wall time and the Timer CQ max offset read after it: once the Source has indexed through
/// that offset, every record visible at that time is indexed.
#[derive(Clone, Copy)]
struct VisibilitySnapshot {
    observed_at_ms: i64,
    queue_offset: i64,
}

struct Running {
    group: TaskGroup,
    pipeline: Arc<TimerPipeline>,
    _ticks: ScheduledTaskGroup,
}

/// The `native` engine and what the store gives it: the Timer CQ and CommitLog to read, the
/// write path to deliver through, its delivery lease, and the CommitLog cleanup pin.
pub(crate) struct NativeTimerService {
    engine: NativeTimerEngine,
    context: TimerStoreContext,
    runtime_scope: StoreRuntimeScope,
    role: TimerRoleState,
    cleanup_pin: Arc<AtomicI64>,
    clock: RwLock<Arc<dyn TimerClock>>,
    settings: Settings,
    /// The store's settings with the native engine's checkpoint interval and batch size, for
    /// its pipeline.
    pipeline_config: MessageStoreConfig,
    /// Source batches index the Timer CQ in order, one at a time.
    source: Mutex<()>,
    visibility: Mutex<Option<VisibilitySnapshot>>,
    /// Lanes with a batch out; a lane delivers one batch at a time.
    busy_lanes: Vec<AtomicBool>,
    next_lane: AtomicUsize,
    /// The timer each lane was last blocked at, so the warning is given once.
    blocked_at: Mutex<Vec<Option<i64>>>,
    /// Failed writes per delivery token, kept only while `timerEnableRetryUntilSuccess` is off.
    write_failures: Mutex<HashMap<CheetahString, u32>>,
    running: Mutex<Option<Running>>,
}

impl NativeTimerService {
    /// Opens the engine's files under the store root and its delivery lease, and pins the
    /// CommitLog at the fence of the checkpoint it opened from, or at its start before the
    /// first checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`NativeServiceError`] for an invalid setting, files the engine cannot open, or
    /// a lease that cannot be loaded.
    pub(crate) fn open(
        config: &MessageStoreConfig,
        context: TimerStoreContext,
        runtime_scope: StoreRuntimeScope,
        cleanup_pin: Arc<AtomicI64>,
    ) -> Result<Self, NativeServiceError> {
        let clock: Arc<dyn TimerClock> = Arc::new(SystemTimerClock::default());
        let store_root = config.store_path_root_dir.as_str();
        let (engine, checkpoint) = NativeTimerEngine::open(
            Path::new(store_root),
            NativeTimerConfig::from_store(config),
            clock.wall_time_ms(),
        )?;
        let role = TimerRoleState::named(store_root, ROLE_FILE);
        role.load().map_err(NativeServiceError::Role)?;
        cleanup_pin.store(
            checkpoint.map_or(0, |checkpoint| checkpoint.commit_log_fence),
            Ordering::Release,
        );
        let native = &config.timer_native;
        let lane_count = native.lane_count;
        Ok(Self {
            engine,
            context,
            runtime_scope,
            role,
            cleanup_pin,
            clock: RwLock::new(clock),
            settings: Settings {
                lane_count,
                batch_messages: native.delivery_batch_messages.max(1),
                inflight_writes: native.delivery_inflight_writes.max(1),
                skip_unknown_error: config.timer_skip_unknown_error,
                retry_until_success: config.timer_enable_retry_until_success,
                tick_ms: (config.timer_precision_ms / 10).max(MIN_SCHEDULER_INTERVAL_MS),
            },
            pipeline_config: MessageStoreConfig {
                timer_flush_interval_ms: usize::try_from(native.checkpoint_interval_ms).unwrap_or(usize::MAX),
                timer_due_batch_messages: native
                    .delivery_batch_messages
                    .saturating_mul(usize::from(lane_count))
                    .max(1),
                ..config.clone()
            },
            source: Mutex::new(()),
            visibility: Mutex::new(None),
            busy_lanes: (0..lane_count).map(|_| AtomicBool::new(false)).collect(),
            next_lane: AtomicUsize::new(0),
            blocked_at: Mutex::new(vec![None; usize::from(lane_count)]),
            write_failures: Mutex::new(HashMap::new()),
            running: Mutex::new(None),
        })
    }

    /// Starts the pipeline that runs the Source, Due and checkpoint stages, woken by Reput and
    /// by a tick ten times per precision interval.
    pub(crate) fn start(self: &Arc<Self>) {
        let mut running = self.running.lock();
        if running.is_some() {
            return;
        }
        let group = crate::runtime::task_group(&self.runtime_scope, "rocketmq-store.timer.native");
        let Some(pipeline) = TimerPipeline::new(&self.runtime_scope, &self.pipeline_config) else {
            error!("failed to create the native timer pipeline: invalid configuration");
            return;
        };
        if !pipeline.spawn(
            &group,
            NativeTimerHandle(Arc::clone(self)),
            1,
            self.pipeline_config.timer_get_message_thread_num,
            self.pipeline_config.timer_completion_gap_limit,
            Some(self.context.source_wake().subscribe()),
        ) {
            error!("failed to spawn the native timer pipeline: scheduler closed");
            pipeline.close();
            group.cancel();
            return;
        }
        let ticks = ScheduledTaskGroup::new(group.clone());
        let service = Arc::clone(self);
        let ticked = Arc::clone(&pipeline);
        if let Err(_error) = ticks.schedule(
            ScheduledTaskConfig::fixed_delay("timer-native-scheduler", Duration::from_millis(self.settings.tick_ms)),
            ScheduledExecutionPolicy::default(),
            move || {
                let service = Arc::clone(&service);
                let pipeline = Arc::clone(&ticked);
                async move {
                    pipeline.submit_tick(service.role.capture_delivery_epoch());
                }
            },
        ) {
            error!("failed to schedule the native timer pipeline");
            pipeline.close();
            group.cancel();
            return;
        }
        *running = Some(Running {
            group,
            pipeline,
            _ticks: ticks,
        });
    }

    /// Fences delivery, stops the pipeline and awaits its work.
    pub(crate) async fn stop_gracefully(&self) -> Option<ShutdownReport> {
        let running = self.running.lock().take();
        self.role.fence_in_memory();
        let running = running?;
        running.pipeline.close();
        let report = running.group.shutdown(Duration::from_secs(5)).await;
        if crate::runtime::shutdown_report_result("native timer pipeline", report.clone()).is_err() {
            warn!("the native timer pipeline failed during shutdown");
            return None;
        }
        Some(report)
    }

    /// Makes what the stopped pipeline did durable. Blocking.
    pub(crate) fn persist_at_shutdown(&self) {
        if let Err(error) = self.checkpoint_blocking() {
            error!("checkpoint the native timer engine at shutdown failed: {error}");
        }
    }

    /// Opens or closes delivery for this Broker; only a master delivers.
    pub(crate) fn set_delivering(&self, delivering: bool) {
        if let Err(error) = self.role.transition(delivering) {
            error!("native timer role transition failed closed; delivery remains fenced: {error}");
        }
    }

    pub(crate) fn engine(&self) -> &NativeTimerEngine {
        &self.engine
    }

    /// Registers a recall of the timer due at `due_ms` that `key` names, before its tombstone
    /// is written; see [`NativeTimerEngine::register_recall`].
    ///
    /// # Errors
    ///
    /// Returns the rejection the engine would give a timer due at `due_ms`.
    pub(crate) fn register_recall(
        &self,
        key: &[u8],
        generation: u64,
        due_ms: i64,
    ) -> Result<NativeRecallVerdict, NativeRejection> {
        self.engine.register_recall(key, generation, due_ms, self.now_ms())
    }

    /// Withdraws a registration whose tombstone could not be written.
    pub(crate) fn withdraw_recall(&self, key: &[u8], generation: u64) {
        self.engine.withdraw_recall(key, generation);
    }

    fn now_ms(&self) -> i64 {
        self.clock.read().wall_time_ms()
    }

    /// Indexes the next Timer CQ records the native engine owns, up to `max_messages` records
    /// and `max_bytes`, and returns how many records it passed. Blocking.
    ///
    /// A record that cannot be read stops the batch, unless `timerSkipUnknownError` lets it be
    /// passed over; records whose CommitLog file cleanup deleted are passed over.
    fn index_source_batch(&self, max_messages: usize, max_bytes: usize) -> Result<usize, NativeEngineError> {
        let _source = self.source.lock();
        let Some(queue) = self
            .context
            .find_consume_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0)
        else {
            return Ok(0);
        };
        let (min_offset, max_offset) = {
            let queue = queue.read();
            (queue.get_min_offset_in_queue(), queue.get_max_offset_in_queue())
        };
        let now_ms = self.now_ms();
        let frontier = self.engine.source_frontier();
        let mut next = frontier.max(min_offset);
        let mut units: Vec<CqUnit> = Vec::new();
        let mut bytes = 0usize;
        while next < max_offset && units.len() < max_messages {
            let Some(unit) = queue.read().get(next) else {
                break;
            };
            // Cleaned records lead the batch; reading one would read the oldest file left.
            if units.is_empty() && self.context.is_cleaned_up(unit.pos) {
                next = unit.queue_offset.saturating_add(1);
                continue;
            }
            let size = usize::try_from(unit.size).unwrap_or(usize::MAX);
            if !units.is_empty() && bytes.saturating_add(size) > max_bytes {
                break;
            }
            bytes = bytes.saturating_add(size);
            next = unit.queue_offset.saturating_add(1);
            units.push(unit);
        }
        let locators: Vec<_> = units.iter().map(|unit| (unit.pos, unit.size)).collect();
        let frames = read_timer_source_frames(self.context.commit_log(), &locators, bytes.max(max_bytes));
        let mut sources = Vec::with_capacity(units.len());
        let mut passed_through = units.first().map_or(next, |unit| unit.queue_offset);
        for (unit, frame) in units.iter().zip(frames) {
            match frame {
                TimerSourceRead::Frame { bytes, message } => sources.extend(native_source(unit, bytes, &message)),
                _ if self.settings.skip_unknown_error => {
                    warn!("skip an unreadable native timer source; timerSkipUnknownError is on");
                }
                _ => {
                    warn!("native timer indexing is blocked by an unreadable Timer CQ record");
                    break;
                }
            }
            passed_through = unit.queue_offset.saturating_add(1);
        }
        let report = self.engine.ingest(&sources, passed_through, now_ms)?;
        if !report.rejected.is_empty() {
            warn!(
                rejected = report.rejected.len(),
                "skip native timers whose deadline the engine does not accept"
            );
        }
        self.advance_visibility();
        Ok(usize::try_from(passed_through.saturating_sub(frontier)).unwrap_or(usize::MAX))
    }

    /// Moves the engine's visibility watermark to the wall time of the last snapshot the Source
    /// has indexed through, and takes a new one. A snapshot taken when every record is indexed
    /// resolves at once, so an idle Timer CQ adds no delay.
    fn advance_visibility(&self) {
        let queue = self
            .context
            .find_consume_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0);
        let mut pending = self.visibility.lock();
        let frontier = self.engine.source_frontier();
        if let Some(snapshot) = *pending {
            if frontier < snapshot.queue_offset {
                return;
            }
            self.engine.set_visible_through(snapshot.observed_at_ms);
        }
        let observed_at_ms = self.now_ms();
        let max_offset = queue.map_or(0, |queue| queue.read().get_max_offset_in_queue());
        *pending = if frontier < max_offset {
            Some(VisibilitySnapshot {
                observed_at_ms,
                queue_offset: max_offset,
            })
        } else {
            self.engine.set_visible_through(observed_at_ms);
            None
        };
    }

    /// Delivers the due timers of each lane without a batch out, up to `max_messages` in all,
    /// while `epoch` stays current, and returns how many timers left the wheel.
    async fn deliver_due(self: &Arc<Self>, epoch: u64, max_messages: usize) -> Result<usize, TimerEngineError> {
        if !self.role.is_current_delivery_epoch(epoch) {
            return Ok(0);
        }
        self.advance_visibility();
        let now_ms = self.now_ms();
        if self.engine.next_deadline_ms().is_none_or(|deadline| deadline > now_ms) {
            return Ok(0);
        }
        let claimed = self.claim_lanes();
        let service = Arc::clone(self);
        let lanes: Vec<u16> = claimed.iter().map(|claim| claim.lane).collect();
        let prepared = self
            .runtime_scope
            .spawn_io("timer-native-prepare", move || {
                service.prepare_lanes(&lanes, now_ms, max_messages)
            })
            .await
            .map_err(|error| TimerEngineError::Storage(std::io::Error::other(error)))?
            .map_err(|error| TimerEngineError::Storage(std::io::Error::other(error)))?;
        let writer = NativeWriter { service: self };
        let written = join_all(
            prepared
                .into_iter()
                .map(|batch| batch.write(&writer, epoch, self.settings.inflight_writes)),
        )
        .await;
        let mut done = 0usize;
        for batch in written {
            let report = self.engine.settle(batch);
            done = done
                .saturating_add(report.written)
                .saturating_add(report.dropped)
                .saturating_add(report.recalled)
                .saturating_add(report.cleaned_up)
                .saturating_add(report.skipped);
        }
        drop(claimed);
        Ok(done)
    }

    /// Prepares a batch of each of `lanes`, up to `max_messages` in all. Blocking.
    fn prepare_lanes(
        &self,
        lanes: &[u16],
        now_ms: i64,
        max_messages: usize,
    ) -> Result<Vec<NativeDueBatch>, NativeEngineError> {
        let mut remaining = max_messages;
        let mut batches = Vec::new();
        for &lane in lanes {
            if remaining == 0 {
                break;
            }
            let batch = self.engine.prepare_due(
                lane,
                now_ms,
                remaining.min(self.settings.batch_messages),
                &self.context,
                self.settings.skip_unknown_error,
            )?;
            self.note_blocked(lane, batch.blocked_at);
            remaining = remaining.saturating_sub(batch.len());
            if !batch.is_empty() {
                batches.push(batch);
            }
        }
        Ok(batches)
    }

    /// Claims each lane without a batch out, starting after the lane the last call started at,
    /// so no lane waits behind the others for the batch budget.
    fn claim_lanes(&self) -> Vec<LaneClaim<'_>> {
        let lane_count = usize::from(self.settings.lane_count);
        let start = self.next_lane.fetch_add(1, Ordering::Relaxed) % lane_count.max(1);
        (0..lane_count)
            .map(|step| (start + step) % lane_count)
            .filter_map(|lane| LaneClaim::try_new(&self.busy_lanes, lane))
            .collect()
    }

    fn note_blocked(&self, lane: u16, blocked_at: Option<i64>) {
        let mut blocked = self.blocked_at.lock();
        let Some(last) = blocked.get_mut(usize::from(lane)) else {
            return;
        };
        if blocked_at.is_some() && *last != blocked_at {
            warn!(
                lane,
                "native timer delivery is blocked by a timer whose record cannot be read or routed"
            );
        }
        *last = blocked_at;
    }

    /// What a delivery write means for its timer, applying `timerSkipUnknownError` to terminal
    /// Store responses and `timerEnableRetryUntilSuccess` to transient ones, as the
    /// Java-compatible engine does.
    fn accept(&self, token: Option<CheetahString>, result: &PutMessageResult) -> NativeWriteOutcome {
        match classify_delivery_status(result.put_message_status()) {
            TimerWorkResult::Complete if result.is_ok() => {
                if let Some(token) = token.filter(|_| !self.settings.retry_until_success) {
                    self.write_failures.lock().remove(&token);
                }
                NativeWriteOutcome::Written
            }
            TimerWorkResult::Complete | TimerWorkResult::Retry(_) => {
                let Some(token) = token.filter(|_| !self.settings.retry_until_success) else {
                    return NativeWriteOutcome::Stays;
                };
                let mut failures = self.write_failures.lock();
                let attempts = failures.entry(token.clone()).or_insert(0);
                *attempts += 1;
                if *attempts < TIMER_WRITE_ATTEMPTS {
                    return NativeWriteOutcome::Stays;
                }
                failures.remove(&token);
                error!("drop a native timer whose delivery write kept failing; timerEnableRetryUntilSuccess is off");
                NativeWriteOutcome::Dropped
            }
            TimerWorkResult::Quarantine(_) if self.settings.skip_unknown_error => {
                warn!("skip a native timer whose delivery the Store rejected; timerSkipUnknownError is on");
                NativeWriteOutcome::Dropped
            }
            TimerWorkResult::Quarantine(_) | TimerWorkResult::Cancelled | TimerWorkResult::StaleGeneration => {
                NativeWriteOutcome::Stays
            }
        }
    }

    /// Makes indexed timers and delivery progress durable and moves the CommitLog cleanup pin
    /// to the engine's fence. Blocking.
    fn checkpoint_blocking(&self) -> Result<(), NativeEngineError> {
        // A Source batch may index past this frontier before the checkpoint takes its own; the
        // fence then stays lower than it needs to, which only keeps more of the CommitLog.
        let frontier = self.engine.source_frontier();
        let source_fence = self
            .context
            .find_consume_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0)
            .and_then(|queue| queue.read().get(frontier))
            .map_or_else(|| self.context.commit_log().get_max_offset(), |unit| unit.pos);
        let checkpoint = self.engine.checkpoint(source_fence, self.now_ms())?;
        self.cleanup_pin.store(checkpoint.commit_log_fence, Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
impl NativeTimerService {
    pub(crate) fn set_clock(&self, clock: Arc<dyn TimerClock>) {
        *self.clock.write() = clock;
    }

    /// Indexes every Timer CQ record, then delivers what is due while this Broker delivers, and
    /// returns how many timers left the wheel.
    pub(crate) async fn process_once(self: &Arc<Self>) -> usize {
        let handle = NativeTimerHandle(Arc::clone(self));
        let budget = || {
            WorkBudget::try_new(
                1_024,
                64 * 1024 * 1024,
                std::time::Instant::now() + Duration::from_secs(10),
            )
            .expect("a non-empty budget")
        };
        while handle.enqueue_source(budget()).await.expect("index sources").messages > 0 {}
        let Some(epoch) = self.role.capture_delivery_epoch() else {
            return 0;
        };
        let mut delivered = 0;
        loop {
            let progress = handle
                .roll_due(TimerEngineEpoch::new(epoch), budget())
                .await
                .expect("deliver due timers");
            if progress.messages == 0 {
                return delivered;
            }
            delivered += progress.messages;
        }
    }

    pub(crate) fn checkpoint_now(&self) {
        self.checkpoint_blocking().expect("checkpoint the native engine");
    }
}

/// The source of a Timer CQ record the native engine owns, or `None` for a record of another
/// engine or one without a deadline.
fn native_source(unit: &CqUnit, frame: Bytes, message: &MessageExt) -> Option<NativeTimerSource> {
    let property = |name: &'static str| message.property(&CheetahString::from_static_str(name));
    if property(MessageConst::TIMER_ENGINE_TYPE)?.as_str() != TimerEngineId::Native.as_str() {
        return None;
    }
    let Some(due_ms) = parse_deliver_time_ms(message) else {
        warn!("skip a native timer source without a valid deadline");
        return None;
    };
    let (kind, frame) = match property(TIMER_DELETE_UNIQUE_KEY) {
        Some(key) => (NativeSourceKind::Recall { key: key.to_string() }, Bytes::new()),
        None => (
            NativeSourceKind::Timer {
                unique_key: property(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                    .map(|key| key.to_string())
                    .unwrap_or_default(),
            },
            frame,
        ),
    };
    Some(NativeTimerSource {
        cq_offset: unit.queue_offset,
        commit_log_offset: unit.pos,
        size: unit.size,
        due_ms,
        generation: timer_generation(message),
        real_topic: property(MessageConst::PROPERTY_REAL_TOPIC)
            .map(|topic| topic.to_string())
            .unwrap_or_default(),
        real_queue_id: property(MessageConst::PROPERTY_REAL_QUEUE_ID)
            .and_then(|queue_id| queue_id.parse().ok())
            .unwrap_or(0),
        kind,
        frame,
    })
}

impl NativeCommitLogReader for TimerStoreContext {
    fn read(&self, offset: i64, size: i32) -> NativeCommitLogRead {
        // Checked before the read: offset 0 of a cleaned CommitLog reads the oldest file left.
        if self.is_cleaned_up(offset) {
            return NativeCommitLogRead::CleanedUp;
        }
        match self.look_message_by_offset_with_size(offset, size) {
            Some(message) => NativeCommitLogRead::Message(message),
            None => NativeCommitLogRead::Unreadable,
        }
    }
}

struct NativeWriter<'a> {
    service: &'a NativeTimerService,
}

impl NativeTimerWriter for NativeWriter<'_> {
    async fn write(&self, message: MessageExtBrokerInner, epoch: u64) -> NativeWriteOutcome {
        // Checked again right before the write: a demotion fences every batch still out.
        if !self.service.role.is_current_delivery_epoch(epoch) {
            return NativeWriteOutcome::Stays;
        }
        let token = message.property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN,
        ));
        let result = self.service.context.put_message(message).await;
        self.service.accept(token, &result)
    }
}

/// A lane claimed for one Due batch, released when dropped.
struct LaneClaim<'a> {
    busy: &'a AtomicBool,
    lane: u16,
}

impl<'a> LaneClaim<'a> {
    fn try_new(busy_lanes: &'a [AtomicBool], lane: usize) -> Option<Self> {
        let busy = busy_lanes.get(lane)?;
        busy.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        Some(Self {
            busy,
            lane: u16::try_from(lane).ok()?,
        })
    }
}

impl Drop for LaneClaim<'_> {
    fn drop(&mut self) {
        self.busy.store(false, Ordering::Release);
    }
}

/// The service as the pipeline drives it.
#[derive(Clone)]
pub(crate) struct NativeTimerHandle(pub(crate) Arc<NativeTimerService>);

impl NativeTimerHandle {
    async fn run_io<T: Send + 'static>(
        &self,
        name: &'static str,
        operation: impl FnOnce(&NativeTimerService) -> Result<T, NativeEngineError> + Send + 'static,
    ) -> Result<T, TimerEngineError> {
        let service = Arc::clone(&self.0);
        self.0
            .runtime_scope
            .spawn_io(name, move || operation(&service))
            .await
            .map_err(|error| TimerEngineError::Storage(std::io::Error::other(error)))?
            .map_err(|error| TimerEngineError::Storage(std::io::Error::other(error)))
    }
}

impl TimerEngine for NativeTimerHandle {
    fn engine_id(&self) -> TimerEngineId {
        TimerEngineId::Native
    }

    async fn load(&self) -> Result<(), TimerEngineError> {
        Ok(())
    }

    async fn enqueue_source(&self, budget: WorkBudget) -> Result<EngineBatchProgress, TimerEngineError> {
        if budget.is_exhausted(0, 0) {
            return Ok(EngineBatchProgress {
                durable: false,
                ..EngineBatchProgress::empty()
            });
        }
        let messages = self
            .run_io("timer-native-source", move |service| {
                service.index_source_batch(budget.max_messages, budget.max_bytes)
            })
            .await?;
        Ok(EngineBatchProgress {
            messages,
            bytes: 0,
            continuation: None,
            durable: false,
        })
    }

    async fn roll_due(
        &self,
        epoch: TimerEngineEpoch,
        budget: WorkBudget,
    ) -> Result<EngineBatchProgress, TimerEngineError> {
        if budget.is_exhausted(0, 0) {
            return Ok(EngineBatchProgress {
                durable: false,
                ..EngineBatchProgress::empty()
            });
        }
        let messages = self.0.deliver_due(epoch.get(), budget.max_messages).await?;
        Ok(EngineBatchProgress {
            messages,
            bytes: 0,
            continuation: None,
            durable: false,
        })
    }

    async fn complete(&self, _timer_id: TimerId, _epoch: TimerEngineEpoch) -> Result<(), TimerEngineError> {
        Ok(())
    }

    async fn cancel(&self, _timer_id: TimerId) -> Result<(), TimerEngineError> {
        Ok(())
    }

    async fn checkpoint(&self) -> Result<bool, TimerEngineError> {
        self.run_io("timer-native-checkpoint", NativeTimerService::checkpoint_blocking)
            .await
            .map(|()| true)
    }

    async fn on_role_change(&self, active: bool, epoch: TimerEngineEpoch) -> Result<(), TimerEngineError> {
        self.0
            .role
            .transition_with_term(active, epoch.get())
            .map(|_| ())
            .map_err(TimerEngineError::Storage)
    }

    async fn shutdown(&self) -> Result<(), TimerEngineError> {
        self.checkpoint().await.map(|_| ())
    }
}
