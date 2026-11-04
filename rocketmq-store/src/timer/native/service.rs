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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
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
use rocketmq_observability::metrics::timer::TimerMetricsRecorder;
use rocketmq_runtime::ScheduledExecutionPolicy;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_store_api::TimerEngineEpoch;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerStoreMode;
use rocketmq_store_local::timer::native_format::NATIVE_TIMER_DIR;
use thiserror::Error;
use tracing::error;
use tracing::info;
use tracing::warn;

use super::config::NativeTimerConfig;
use super::delivery::NativeCommitLogRead;
use super::delivery::NativeCommitLogReader;
use super::delivery::NativeTimerWriter;
use super::delivery::NativeWriteOutcome;
use super::engine::NativeEngineError;
use super::engine::NativeRecallVerdict;
use super::engine::NativeRejection;
use super::engine::NativeSourceKind;
use super::engine::NativeTimerEngine;
use super::engine::NativeTimerSource;
use super::feed::NativeDeliveryFeed;
use super::feed::NativeFeedMark;
use super::feed::NativeTimerOwner;
use super::shadow::NativeShadowReport;
use super::shadow::NativeShadowSample;
use super::shadow::NativeShadowVerdict;
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
use crate::timer::pipeline::TimerPipelineDiagnostics;
use crate::timer::request::EngineBatchProgress;
use crate::timer::role::TimerRoleState;
use crate::timer::source::read_timer_source_frames;
use crate::timer::source::TimerSourceRead;
use crate::timer::timer_message_store::original_deliver_time_ms;
use crate::timer::timer_message_store::timer_generation;
use crate::timer::timer_message_store::TimerStoreContext;
use crate::timer::timer_message_store::MIN_SCHEDULER_INTERVAL_MS;
use crate::timer::timer_message_store::TIMER_DELETE_UNIQUE_KEY;
use crate::timer::timer_message_store::TIMER_TOPIC;
use crate::timer::timer_message_store::TIMER_WRITE_ATTEMPTS;

/// The native engine's delivery lease, beside the Java-compatible engine's.
const ROLE_FILE: &str = "timer-native-role.meta";
/// Marks a native engine that drained while another mode was active, so the store no longer
/// opens it in that mode.
const RETIRED_FILE: &str = "timer-native.retired";
/// Directory under the store root that a shadow keeps all its files in.
const SHADOW_DIR: &str = "timer-native-shadow";
/// Deliveries the shadow never found due that one sweep keeps as samples.
const SHADOW_SWEEP_SAMPLES: usize = 8;

/// What the native engine does in a store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NativeServiceRole {
    /// `timerStoreMode = native`: the Broker admits new timers to it.
    Active,
    /// Another mode is active, and the engine still delivers the timers it admitted before.
    Draining,
    /// Another mode is active, and a second engine follows its timers to compare itself with it,
    /// delivering nothing: `[store.timerNative] shadowEnable`.
    Shadow,
}

impl NativeServiceRole {
    /// The role of the native engine in a store opened with `config`, or `None` when the store
    /// does not run it: another mode is active and the engine never ran, or it drained.
    pub(crate) fn of(config: &MessageStoreConfig) -> Option<Self> {
        if config.timer_store_mode == TimerStoreMode::Native {
            return Some(Self::Active);
        }
        let store_root = Path::new(config.store_path_root_dir.as_str());
        (store_root.join(NATIVE_TIMER_DIR).exists() && !retired_marker(store_root).exists()).then_some(Self::Draining)
    }

    /// The role of the second native engine of a store opened with `config`: a shadow, when one
    /// is set up beside another mode. Under `native` there is nothing to compare with.
    pub(crate) fn shadow_of(config: &MessageStoreConfig) -> Option<Self> {
        (config.timer_native.shadow_enable && config.timer_store_mode != TimerStoreMode::Native).then_some(Self::Shadow)
    }
}

fn retired_marker(store_root: &Path) -> PathBuf {
    store_root.join("config").join(RETIRED_FILE)
}

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
    precision_ms: i64,
}

/// A wall time and the Timer CQ max offset read after it: once the Source has indexed through
/// that offset, every record visible at that time is indexed.
#[derive(Clone, Copy)]
struct VisibilitySnapshot {
    observed_at_ms: i64,
    queue_offset: i64,
}

/// What one round of Due batches did.
#[derive(Default)]
struct Round {
    /// Timers that left the wheel.
    settled: usize,
    /// Whether a lane took a full batch, so more may be due.
    full_lane: bool,
}

/// What the service did since the store started.
#[derive(Default)]
struct Counters {
    indexed: AtomicU64,
    indexed_recalls: AtomicU64,
    rejected: AtomicU64,
    written: AtomicU64,
    dropped: AtomicU64,
    recalled: AtomicU64,
    cleaned_up: AtomicU64,
    skipped: AtomicU64,
    followed: AtomicU64,
}

impl Counters {
    fn add(counter: &AtomicU64, amount: usize) {
        counter.fetch_add(u64::try_from(amount).unwrap_or(u64::MAX), Ordering::Relaxed);
    }
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
    service_role: NativeServiceRole,
    /// The engine never ran, or it drained: no Timer CQ record already there when the store
    /// loads is a native timer.
    starts_at_queue_end: bool,
    retired_marker: PathBuf,
    /// Due batches taken and not yet settled. A batch whose task was cancelled stays counted,
    /// since its timers left the wheel without being delivered.
    batches_out: AtomicUsize,
    /// The deliveries of the master, while this Broker does not deliver.
    feed: Arc<NativeDeliveryFeed>,
    /// Where the next start reads the deliveries back from.
    feed_mark: NativeFeedMark,
    /// What this engine found, when it is a shadow.
    shadow: Option<NativeShadowReport>,
    /// The mark this run wrote last.
    written_feed_mark: AtomicI64,
    /// The end of what every CommitLog dispatcher has seen, -1 before the first dispatch.
    dispatched: Arc<AtomicI64>,
    metrics: TimerMetricsRecorder,
    counters: Counters,
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
        service_role: NativeServiceRole,
        feed: Arc<NativeDeliveryFeed>,
        dispatched: Arc<AtomicI64>,
        metrics: TimerMetricsRecorder,
    ) -> Result<Self, NativeServiceError> {
        let clock: Arc<dyn TimerClock> = Arc::new(SystemTimerClock::default());
        let store_root = match service_role {
            NativeServiceRole::Shadow => Path::new(config.store_path_root_dir.as_str()).join(SHADOW_DIR),
            NativeServiceRole::Active | NativeServiceRole::Draining => {
                PathBuf::from(config.store_path_root_dir.as_str())
            }
        };
        let retired = retired_marker(&store_root);
        let starts_at_queue_end = !store_root.join(NATIVE_TIMER_DIR).exists() || retired.exists();
        let (engine, checkpoint) =
            NativeTimerEngine::open(&store_root, NativeTimerConfig::from_store(config), clock.wall_time_ms())?;
        let role = TimerRoleState::named(&store_root.to_string_lossy(), ROLE_FILE);
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
                precision_ms: i64::try_from(config.timer_precision_ms).unwrap_or(i64::MAX),
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
            service_role,
            starts_at_queue_end,
            retired_marker: retired,
            batches_out: AtomicUsize::new(0),
            feed,
            feed_mark: NativeFeedMark::new(&store_root),
            shadow: (service_role == NativeServiceRole::Shadow).then(|| {
                NativeShadowReport::new(
                    i64::try_from(native.shadow_grace_ms).unwrap_or(i64::MAX),
                    i64::try_from(config.timer_precision_ms).unwrap_or(i64::MAX),
                )
            }),
            written_feed_mark: AtomicI64::new(-1),
            dispatched,
            metrics,
            counters: Counters::default(),
        })
    }

    /// Finishes opening once the Timer CQ is recovered. An engine that never ran, or that
    /// drained, starts after the records already there, which other engines admitted. An
    /// active engine stops being retired, so another mode drains it again.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of removing the retirement marker; the store must not load then,
    /// or another mode would leave the timers this engine admits undelivered.
    pub(crate) fn load(&self) -> std::io::Result<()> {
        if self.starts_at_queue_end {
            self.engine.start_source_at(self.timer_queue_end());
        }
        if self.service_role == NativeServiceRole::Active {
            match fs::remove_file(&self.retired_marker) {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error),
                _ => {}
            }
        }
        Ok(())
    }

    /// The CommitLog offset this Broker's deliveries are read back from, if an earlier run left
    /// one.
    pub(crate) fn replay_start(&self) -> Option<i64> {
        self.feed_mark.read()
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

    /// Makes what the stopped pipeline did durable. A draining engine is retired once it holds
    /// no timers and has indexed every Timer CQ record, with nothing left for Reput to dispatch
    /// into the Timer CQ (`dispatched_everything`); the store no longer opens it in another
    /// mode. Blocking.
    pub(crate) fn persist_at_shutdown(&self, dispatched_everything: bool) {
        if let Err(error) = self.checkpoint_blocking() {
            error!("checkpoint the native timer engine at shutdown failed: {error}");
            return;
        }
        let drained = self.service_role == NativeServiceRole::Draining
            && dispatched_everything
            && self.batches_out.load(Ordering::Acquire) == 0
            && self.engine.holds_no_timers()
            && self.engine.source_frontier() >= self.timer_queue_end();
        if !drained {
            return;
        }
        match write_marker(&self.retired_marker) {
            Ok(()) => info!("the native timer engine delivered every timer it admitted and is retired"),
            Err(error) => warn!("retire the drained native timer engine failed: {error}"),
        }
    }

    fn timer_queue_end(&self) -> i64 {
        self.context
            .find_consume_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0)
            .map_or(0, |queue| queue.read().get_max_offset_in_queue())
    }

    /// Opens or closes delivery for this Broker; only a master delivers. A Broker that does not
    /// deliver follows the deliveries its master writes.
    pub(crate) fn set_delivering(&self, delivering: bool) {
        if let Err(error) = self.change_delivery_role(delivering, 0) {
            error!("native timer role transition failed closed; delivery remains fenced: {error}");
        }
    }

    /// Opens or closes delivery under the controller's `external_term`, so the delivery epoch
    /// follows the controller's terms. A new term fences every batch still out, also when this
    /// Broker stays master.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of persisting the epoch; delivery stays fenced then.
    pub(crate) fn change_delivery_role(&self, delivering: bool, external_term: u64) -> std::io::Result<u64> {
        self.feed.set_observing(!delivering);
        self.role.transition_with_term(delivering, external_term)
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

    /// `active` while the Broker admits new timers to the engine, `draining` otherwise.
    pub(crate) fn role_name(&self) -> &'static str {
        match self.service_role {
            NativeServiceRole::Active => "active",
            NativeServiceRole::Draining => "draining",
            NativeServiceRole::Shadow => "shadow",
        }
    }

    /// Whose timers this engine indexes.
    fn timer_owner(&self) -> NativeTimerOwner {
        match self.service_role {
            NativeServiceRole::Shadow => NativeTimerOwner::OtherEngines,
            NativeServiceRole::Active | NativeServiceRole::Draining => NativeTimerOwner::Native,
        }
    }

    /// The newest differences a shadow found, oldest first; none for another role.
    pub(crate) fn shadow_samples(&self) -> Vec<NativeShadowSample> {
        self.shadow
            .as_ref()
            .map(NativeShadowReport::samples)
            .unwrap_or_default()
    }

    /// Counts the active engine's deliveries this shadow never found due. A shadow behind its
    /// own deadlines by more than the grace period would only find them late, so it waits.
    fn sweep_unindexed(&self) {
        let Some(report) = self.shadow.as_ref() else {
            return;
        };
        let now_ms = self.now_ms();
        let grace_ms = report.grace_ms();
        if self
            .engine
            .next_deadline_ms()
            .is_some_and(|deadline| deadline < now_ms.saturating_sub(grace_ms))
        {
            return;
        }
        let (total, samples) = self
            .feed
            .expire_seen_before(now_ms.saturating_sub(grace_ms.saturating_mul(2)), SHADOW_SWEEP_SAMPLES);
        if total > 0 {
            report.record_unindexed(&samples, total);
        }
    }

    /// What the engine holds, how far it got, and what the service did since the store started,
    /// by runtime-info name.
    pub(crate) fn runtime_metrics(&self) -> BTreeMap<&'static str, i64> {
        let counts = self.engine.counts();
        let count = |value: usize| i64::try_from(value).unwrap_or(i64::MAX);
        let total = |counter: &AtomicU64| i64::try_from(counter.load(Ordering::Relaxed)).unwrap_or(i64::MAX);
        let counters = &self.counters;
        let blocked_lanes = self
            .blocked_at
            .lock()
            .iter()
            .filter(|blocked| blocked.is_some())
            .count();
        let mut metrics = BTreeMap::from([
            ("wheelTimers", count(counts.wheel)),
            ("dayBuckets", count(counts.days)),
            ("hourBuckets", count(counts.hours)),
            ("minuteRuns", count(counts.minutes)),
            ("recalls", count(counts.recalls)),
            ("payloadDays", count(self.engine.payload_days())),
            (
                "sourceBehind",
                self.timer_queue_end()
                    .saturating_sub(self.engine.source_frontier())
                    .max(0),
            ),
            ("visibleThroughMs", counts.visible_through_ms),
            ("collectedUntilMs", counts.collected_until_ms),
            ("commitLogPin", self.cleanup_pin.load(Ordering::Acquire)),
            ("blockedLanes", count(blocked_lanes)),
            ("followedTokens", count(self.feed.len())),
            ("indexed", total(&counters.indexed)),
            ("indexedRecalls", total(&counters.indexed_recalls)),
            ("rejected", total(&counters.rejected)),
            ("written", total(&counters.written)),
            ("dropped", total(&counters.dropped)),
            ("recalled", total(&counters.recalled)),
            ("cleanedUp", total(&counters.cleaned_up)),
            ("skipped", total(&counters.skipped)),
            ("followed", total(&counters.followed)),
        ]);
        if let Some(report) = self.shadow.as_ref() {
            metrics.extend(report.metrics());
        }
        metrics
    }

    pub(crate) fn pipeline_diagnostics(&self) -> Option<TimerPipelineDiagnostics> {
        self.running.lock().as_ref().map(|running| running.pipeline.snapshot())
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
                TimerSourceRead::Frame { bytes, message } => {
                    sources.extend(native_source(
                        unit,
                        bytes,
                        &message,
                        self.settings.precision_ms,
                        self.timer_owner(),
                    ));
                }
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
        Counters::add(&self.counters.indexed, report.indexed);
        Counters::add(&self.counters.indexed_recalls, report.recalls);
        Counters::add(&self.counters.rejected, report.rejected.len());
        for source in &sources {
            let rejected = report
                .rejected
                .iter()
                .any(|(cq_offset, _)| *cq_offset == source.cq_offset);
            // A shadow's timers were counted by the engine that admitted them.
            if matches!(source.kind, NativeSourceKind::Timer { .. }) && !rejected && self.shadow.is_none() {
                self.metrics.record_enqueue_total(Some(&source.real_topic));
            }
        }
        if !report.rejected.is_empty() {
            warn!(
                rejected = report.rejected.len(),
                "skip native timers whose deadline the engine does not accept"
            );
        }
        self.advance_visibility();
        Ok(usize::try_from(passed_through.saturating_sub(frontier)).unwrap_or(usize::MAX))
    }

    /// Takes back what came from the CommitLog at or past `offset`, before the Timer CQ and the
    /// CommitLog lose it. Delivery tokens go, the Source goes back to read the records that take
    /// their place, and the timers already indexed from those records stay away from delivery.
    /// A timer this Broker delivered there stays delivered: its new master delivers it as well.
    ///
    /// The cut must wait for Reput to rest, and for this to return.
    ///
    /// # Errors
    ///
    /// Returns the error of saving the cut timers or the checkpoint that makes the Source's new
    /// position durable. Nothing depends on a cut that failed: the next one starts over.
    pub(crate) fn cut_back(&self, offset: i64) -> Result<(), NativeEngineError> {
        let source = self.source.lock();
        self.feed.forget_from(offset);
        let queue = self
            .context
            .find_consume_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0);
        if let Some(queue) = queue {
            let (min_offset, mut first_cut) = {
                let queue = queue.read();
                (queue.get_min_offset_in_queue(), queue.get_max_offset_in_queue())
            };
            while first_cut > min_offset && queue.read().get(first_cut - 1).is_some_and(|unit| unit.pos >= offset) {
                first_cut -= 1;
            }
            let frontier = self.engine.source_frontier();
            let mut timers = Vec::new();
            let now_ms = self.now_ms();
            let mut next = first_cut;
            while next < frontier {
                let units: Vec<CqUnit> = (next..frontier.min(next.saturating_add(CUT_READ_BATCH)))
                    .map_while(|cq_offset| queue.read().get(cq_offset))
                    .collect();
                let Some(last) = units.last() else {
                    break;
                };
                next = last.queue_offset.saturating_add(1);
                let locators: Vec<_> = units.iter().map(|unit| (unit.pos, unit.size)).collect();
                let bytes = units.iter().map(|unit| usize::try_from(unit.size).unwrap_or(0)).sum();
                let frames = read_timer_source_frames(self.context.commit_log(), &locators, bytes);
                for (unit, frame) in units.iter().zip(frames) {
                    let TimerSourceRead::Frame { bytes, message } = frame else {
                        warn!("a native timer source a cut removes cannot be read");
                        continue;
                    };
                    timers.extend(
                        native_source(unit, bytes, &message, self.settings.precision_ms, self.timer_owner())
                            .and_then(|source| self.engine.cut_timer_of(&source, now_ms)),
                    );
                }
            }
            self.engine.cut_source(first_cut, &timers)?;
            *self.visibility.lock() = None;
        }
        drop(source);
        self.checkpoint_blocking()
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

    /// Takes the due timers of each lane without a batch out, up to `max_messages` in all, and
    /// returns how many left the wheel: written while `epoch` stays current, or, on a Broker
    /// that does not deliver, counted delivered as far as its master wrote them.
    async fn deliver_due(self: &Arc<Self>, mode: DueMode, max_messages: usize) -> Result<usize, TimerEngineError> {
        let mut done = 0usize;
        // Rounds go on while a lane comes back with a full batch: the pipeline runs another Due
        // batch only after a full one, and fewer lanes than configured may hold due timers.
        while done < max_messages {
            let round = self.deliver_round(mode, max_messages - done).await?;
            done = done.saturating_add(round.settled);
            if !round.full_lane || round.settled == 0 {
                break;
            }
        }
        Ok(done)
    }

    /// One batch of each lane without a batch out, the lanes prepared in parallel.
    async fn deliver_round(self: &Arc<Self>, mode: DueMode, max_messages: usize) -> Result<Round, TimerEngineError> {
        if let DueMode::Write { epoch } = mode {
            if !self.role.is_current_delivery_epoch(epoch) {
                return Ok(Round::default());
            }
        }
        self.advance_visibility();
        let now_ms = self.now_ms();
        if self.engine.next_deadline_ms().is_none_or(|deadline| deadline > now_ms) {
            return Ok(Round::default());
        }
        let claimed = self.claim_lanes();
        if claimed.is_empty() {
            return Ok(Round::default());
        }
        let per_lane = self
            .settings
            .batch_messages
            .min(max_messages.div_ceil(claimed.len()))
            .max(1);
        // Counted before any timer leaves the wheel; only a settled batch is uncounted.
        self.batches_out.fetch_add(1, Ordering::AcqRel);
        let prepared = join_all(claimed.iter().map(|claim| {
            let service = Arc::clone(self);
            let lane = claim.lane;
            self.runtime_scope.spawn_io("timer-native-prepare", move || {
                let batch = service.engine.prepare_due(
                    lane,
                    now_ms,
                    per_lane,
                    &service.context,
                    service.settings.skip_unknown_error,
                );
                if let Ok(batch) = &batch {
                    service.note_blocked(lane, batch.blocked_at);
                }
                batch
            })
        }))
        .await;
        let mut batches = Vec::with_capacity(prepared.len());
        let mut failure = None;
        for result in prepared {
            // A lane that failed to load took nothing from the wheel; the others still deliver.
            match result {
                Ok(Ok(batch)) if !batch.is_empty() => batches.push(batch),
                Ok(Ok(_)) => {}
                Ok(Err(error)) => failure = Some(TimerEngineError::Storage(std::io::Error::other(error))),
                Err(error) => failure = Some(TimerEngineError::Storage(std::io::Error::other(error))),
            }
        }
        let full_lane = batches.iter().any(|batch| batch.len() >= per_lane);
        let written = match mode {
            DueMode::Write { epoch } => {
                let writer = NativeWriter { service: self };
                join_all(
                    batches
                        .into_iter()
                        .map(|batch| batch.write(&writer, epoch, self.settings.inflight_writes)),
                )
                .await
            }
            DueMode::Observe => {
                // One at a time: a token taken behind a timer the master did not write would be
                // lost when that timer goes back into the wheel.
                let writer = ObservingWriter { feed: &self.feed };
                join_all(batches.into_iter().map(|batch| batch.write(&writer, 0, 1))).await
            }
            DueMode::Shadow => {
                // One at a time for the same reason.
                let writer = ShadowWriter { service: self, now_ms };
                join_all(batches.into_iter().map(|batch| batch.write(&writer, 0, 1))).await
            }
        };
        let mut settled_total = 0usize;
        for batch in written {
            let report = self.engine.settle(batch);
            let settled = report
                .written
                .saturating_add(report.dropped)
                .saturating_add(report.recalled)
                .saturating_add(report.cleaned_up)
                .saturating_add(report.skipped);
            settled_total = settled_total.saturating_add(settled);
            let counters = &self.counters;
            match mode {
                DueMode::Write { .. } => {
                    Counters::add(&counters.written, report.written);
                    Counters::add(&counters.dropped, report.dropped);
                    Counters::add(&counters.recalled, report.recalled);
                    Counters::add(&counters.cleaned_up, report.cleaned_up);
                    Counters::add(&counters.skipped, report.skipped);
                }
                DueMode::Observe | DueMode::Shadow => Counters::add(&counters.followed, settled),
            }
        }
        self.batches_out.fetch_sub(1, Ordering::AcqRel);
        drop(claimed);
        match failure {
            Some(error) if settled_total == 0 => Err(error),
            _ => Ok(Round {
                settled: settled_total,
                full_lane,
            }),
        }
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
        // Read in this order and before the checkpoint: a token kept after the frontier lies at
        // or past it, and a token taken before the checkpoint is behind its watermark.
        let dispatched = self.dispatched.load(Ordering::Acquire);
        let feed_mark = self
            .feed
            .lowest_offset()
            .into_iter()
            .chain((dispatched >= 0).then_some(dispatched))
            .min();
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
        self.engine.forget_cut_timers(&checkpoint.lane_watermarks);
        // Only now: the mark must not pass what the checkpoint on disk has not settled.
        if let Some(feed_mark) = feed_mark {
            if self.written_feed_mark.swap(feed_mark, Ordering::AcqRel) != feed_mark {
                if let Err(error) = self.feed_mark.write(feed_mark) {
                    warn!(%error, "save where the native timer deliveries restart failed");
                }
            }
        }
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
        self.index_all().await;
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

    /// Indexes every Timer CQ record.
    pub(crate) async fn index_all(self: &Arc<Self>) {
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
    }

    /// Runs one Source batch of up to `max_messages` records.
    pub(crate) async fn source_once(self: &Arc<Self>, max_messages: usize) {
        let budget = WorkBudget::try_new(
            max_messages,
            64 * 1024 * 1024,
            std::time::Instant::now() + Duration::from_secs(10),
        )
        .expect("a non-empty budget");
        NativeTimerHandle(Arc::clone(self))
            .enqueue_source(budget)
            .await
            .expect("run a Source batch");
    }

    /// Runs one Due batch of up to `max_messages` while this Broker delivers.
    pub(crate) async fn deliver_once(self: &Arc<Self>, max_messages: usize) -> usize {
        let Some(epoch) = self.role.capture_delivery_epoch() else {
            return 0;
        };
        self.deliver_due(DueMode::Write { epoch }, max_messages)
            .await
            .expect("deliver due timers")
    }

    pub(crate) fn delivery_epoch(&self) -> u64 {
        self.role.epoch()
    }

    /// Runs one Due batch under `epoch`, current or not.
    pub(crate) async fn deliver_under(self: &Arc<Self>, epoch: u64, max_messages: usize) -> usize {
        self.deliver_due(DueMode::Write { epoch }, max_messages)
            .await
            .expect("deliver due timers")
    }

    pub(crate) fn checkpoint_now(&self) {
        self.checkpoint_blocking().expect("checkpoint the native engine");
    }
}

/// Writes the retirement marker and makes it durable.
fn write_marker(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::File::create(path)?.sync_all()
}

/// The source of a Timer CQ record the native engine indexes, or `None` for a timer of another
/// engine or a record without a deadline. Recalls of every engine are indexed: Recall tags a
/// tombstone for the engine that admits new timers, which after a switch of `timerStoreMode` is
/// not always the engine that admitted the timer.
fn native_source(
    unit: &CqUnit,
    frame: Bytes,
    message: &MessageExt,
    precision_ms: i64,
    owner: NativeTimerOwner,
) -> Option<NativeTimerSource> {
    let property = |name: &'static str| message.property(&CheetahString::from_static_str(name));
    let recall_key = property(TIMER_DELETE_UNIQUE_KEY);
    let engine = property(MessageConst::TIMER_ENGINE_TYPE);
    if recall_key.is_none() && !owner.owns(engine.as_ref().map(|engine| engine.as_str())) {
        return None;
    }
    // `TIMER_OUT_MS` is the start of the precision slot, which can come before the deadline.
    let Some(due_ms) = original_deliver_time_ms(message, precision_ms) else {
        warn!("skip a native timer source without a valid deadline");
        return None;
    };
    let (kind, frame) = match recall_key {
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
    async fn write(&self, message: MessageExtBrokerInner, epoch: u64, _due_ms: i64) -> NativeWriteOutcome {
        // Checked again right before the write: a demotion fences every batch still out.
        if !self.service.role.is_current_delivery_epoch(epoch) {
            return NativeWriteOutcome::Stays;
        }
        let token = message.property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN,
        ));
        // Written by the old master before this Broker was promoted.
        if token.as_ref().is_some_and(|token| self.service.feed.take(token)) {
            return NativeWriteOutcome::Written;
        }
        let topic = message.get_topic().clone();
        let result = self.service.context.put_message(message).await;
        let outcome = self.service.accept(token, &result);
        if outcome == NativeWriteOutcome::Written {
            self.service.metrics.record_dequeue_total(topic.as_str());
        }
        outcome
    }
}

/// Takes a due timer out of the wheel once the active engine delivered it, or its grace period
/// ran out, and records how the two compare. A shadow writes nothing.
struct ShadowWriter<'a> {
    service: &'a NativeTimerService,
    now_ms: i64,
}

impl NativeTimerWriter for ShadowWriter<'_> {
    async fn write(&self, message: MessageExtBrokerInner, _epoch: u64, due_ms: i64) -> NativeWriteOutcome {
        let Some(report) = self.service.shadow.as_ref() else {
            return NativeWriteOutcome::Stays;
        };
        let Some(token) = message.property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN,
        )) else {
            report.record_untracked();
            return NativeWriteOutcome::Dropped;
        };
        let delivered_ms = self.service.feed.take_seen(&token);
        match report.judge(token.as_str(), due_ms, delivered_ms, self.now_ms) {
            NativeShadowVerdict::Matched => NativeWriteOutcome::Written,
            NativeShadowVerdict::Waits => NativeWriteOutcome::Stays,
            NativeShadowVerdict::NotDelivered => NativeWriteOutcome::Dropped,
        }
    }
}

/// Counts a due timer delivered once its master's message for it was dispatched here.
struct ObservingWriter<'a> {
    feed: &'a NativeDeliveryFeed,
}

impl NativeTimerWriter for ObservingWriter<'_> {
    async fn write(&self, message: MessageExtBrokerInner, _epoch: u64, _due_ms: i64) -> NativeWriteOutcome {
        let token = message.property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN,
        ));
        if token.is_some_and(|token| self.feed.take(&token)) {
            NativeWriteOutcome::Written
        } else {
            NativeWriteOutcome::Stays
        }
    }
}

/// Timer CQ records a cut reads at a time.
const CUT_READ_BATCH: i64 = 1024;

/// How a Due batch takes its timers out of the wheel.
#[derive(Clone, Copy)]
enum DueMode {
    /// Write them, while this Broker holds delivery epoch `epoch`.
    Write { epoch: u64 },
    /// Follow the master's writes.
    Observe,
    /// Compare with the active engine's deliveries, as a shadow.
    Shadow,
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
        // Only a master runs Due batches; a slave follows its master's deliveries here, by a
        // Due batch at a time: a Source batch holds far fewer timers than a master delivers.
        if !self.0.role.is_active() {
            let mode = if self.0.shadow.is_some() {
                DueMode::Shadow
            } else {
                DueMode::Observe
            };
            self.0
                .deliver_due(mode, self.0.pipeline_config.timer_due_batch_messages)
                .await?;
            self.0.sweep_unindexed();
        }
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
        let messages = self
            .0
            .deliver_due(DueMode::Write { epoch: epoch.get() }, budget.max_messages)
            .await?;
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
