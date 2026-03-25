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

use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::message::message_accessor::MessageAccessor;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::message_single;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::system_clock::SystemClock;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_rust::ArcMut;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::error;
use tracing::warn;

use crate::base::message_store::MessageStore;
use crate::config::message_store_config::MessageStoreConfig;
use crate::message_store::local_file_message_store::LocalFileMessageStore;
use crate::queue::CqUnit;
use crate::store_path_config_helper::get_timer_check_path;
use crate::store_path_config_helper::get_timer_log_path;
use crate::store_path_config_helper::get_timer_metrics_path;
use crate::store_path_config_helper::get_timer_wheel_path;
use crate::timer::slot::Slot;
use crate::timer::timer_checkpoint::TimerCheckpoint;
use crate::timer::timer_log::TimerLog;
use crate::timer::timer_metrics::TimerMetrics;
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
const TPS_WINDOW_MS: i64 = 1_000;

#[derive(Clone, Copy, Debug)]
struct TimerLogRecord {
    deliver_time_ms: i64,
    commit_log_offset: i64,
    size: i32,
    queue_offset: i64,
    prev_pos: i64,
    magic: i32,
}

impl TimerLogRecord {
    const SIZE: usize = 40;

    fn encode(self) -> [u8; Self::SIZE] {
        let mut buffer = [0u8; Self::SIZE];
        buffer[0..8].copy_from_slice(&self.deliver_time_ms.to_be_bytes());
        buffer[8..16].copy_from_slice(&self.commit_log_offset.to_be_bytes());
        buffer[16..20].copy_from_slice(&self.size.to_be_bytes());
        buffer[20..28].copy_from_slice(&self.queue_offset.to_be_bytes());
        buffer[28..36].copy_from_slice(&self.prev_pos.to_be_bytes());
        buffer[36..40].copy_from_slice(&self.magic.to_be_bytes());
        buffer
    }

    fn decode(buffer: &[u8]) -> std::io::Result<Self> {
        if buffer.len() != Self::SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid timer log record length {}", buffer.len()),
            ));
        }

        Ok(Self {
            deliver_time_ms: read_i64(&buffer[0..8])?,
            commit_log_offset: read_i64(&buffer[8..16])?,
            size: read_i32(&buffer[16..20])?,
            queue_offset: read_i64(&buffer[20..28])?,
            prev_pos: read_i64(&buffer[28..36])?,
            magic: read_i32(&buffer[36..40])?,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct TimerLogEntry {
    position: i64,
    record: TimerLogRecord,
}

#[derive(Clone, Copy, Debug)]
struct RecoveredTimerState {
    read_time_ms: i64,
    queue_offset: i64,
}

#[derive(Default)]
struct TpsCounter {
    state: Mutex<TpsCounterState>,
}

#[derive(Default)]
struct TpsCounterState {
    buckets: VecDeque<(i64, usize)>,
    total: usize,
}

impl TpsCounter {
    fn record(&self, delta: usize) {
        if delta == 0 {
            return;
        }
        let now_ms = current_millis() as i64;
        let mut state = self.state.lock();
        Self::evict_expired(&mut state, now_ms);
        if state.buckets.back().is_some_and(|(bucket_ms, _)| *bucket_ms == now_ms) {
            if let Some((_, count)) = state.buckets.back_mut() {
                *count += delta;
            }
            state.total += delta;
            return;
        }
        state.buckets.push_back((now_ms, delta));
        state.total += delta;
    }

    fn get_tps(&self) -> f32 {
        let now_ms = current_millis() as i64;
        let mut state = self.state.lock();
        Self::evict_expired(&mut state, now_ms);
        state.total as f32
    }

    fn evict_expired(state: &mut TpsCounterState, now_ms: i64) {
        while let Some((bucket_ms, count)) = state.buckets.front().copied() {
            if now_ms - bucket_ms < TPS_WINDOW_MS {
                break;
            }
            state.total = state.total.saturating_sub(count);
            state.buckets.pop_front();
        }
    }
}

pub struct TimerMessageStore {
    pub curr_read_time_ms: AtomicI64,
    pub curr_queue_offset: AtomicI64,
    pub last_enqueue_but_expired_time: u64,
    pub last_enqueue_but_expired_store_time: u64,
    pub default_message_store: Option<ArcMut<LocalFileMessageStore>>,
    pub timer_metrics: TimerMetrics,
    message_store_config: Arc<MessageStoreConfig>,
    timer_checkpoint: Mutex<Option<TimerCheckpoint>>,
    timer_log: Mutex<Option<TimerLog>>,
    timer_wheel: Mutex<Option<TimerWheel>>,
    should_running_dequeue: AtomicBool,
    enqueue_suspended: AtomicBool,
    process_lock: AsyncMutex<()>,
    scheduler_shutdown: Notify,
    scheduler_handle: Mutex<Option<JoinHandle<()>>>,
    enqueue_tps_counter: TpsCounter,
    dequeue_tps_counter: TpsCounter,
}

impl TimerMessageStore {
    pub fn load(&self) -> bool {
        let root_dir = self.message_store_config.store_path_root_dir.as_str();
        let timer_checkpoint = match TimerCheckpoint::new(get_timer_check_path(root_dir)) {
            Ok(timer_checkpoint) => timer_checkpoint,
            Err(err) => {
                error!("load timer checkpoint failed: {err}");
                return false;
            }
        };

        let timer_log = TimerLog::new(
            get_timer_log_path(root_dir),
            self.message_store_config.mapped_file_size_timer_log,
        );
        if let Err(err) = timer_log.load() {
            error!("load timer log failed: {err}");
            return false;
        }

        let timer_wheel = TimerWheel::new(
            get_timer_wheel_path(root_dir),
            TIMER_WHEEL_TTL_DAY as usize * DAY_SECS as usize,
            self.message_store_config.timer_precision_ms,
        );
        if let Err(err) = timer_wheel.load() {
            error!("load timer wheel failed: {err}");
            return false;
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
        true
    }

    pub fn start(self: &Arc<Self>) {
        let mut scheduler_handle = self.scheduler_handle.lock();
        if scheduler_handle.is_some() {
            return;
        }

        let scheduler = self.clone();
        let interval_ms = scheduler
            .message_store_config
            .timer_precision_ms
            .max(MIN_SCHEDULER_INTERVAL_MS);
        *scheduler_handle = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = scheduler.scheduler_shutdown.notified() => break,
                    _ = interval.tick() => {
                        let _ = scheduler.process_once().await;
                    }
                }
            }
        }));
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
        (SystemClock::now() as i64) - self.curr_read_time_ms.load(Ordering::Relaxed)
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
        let consume_queue = self.default_message_store.as_ref().and_then(|message_store| {
            message_store.find_consume_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0)
        });
        let max_offset_in_queue = match consume_queue {
            Some(queue) => queue.get_max_offset_in_queue(),
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

    pub fn get_enqueue_tps(&self) -> f32 {
        self.enqueue_tps_counter.get_tps()
    }

    pub fn get_dequeue_tps(&self) -> f32 {
        self.dequeue_tps_counter.get_tps()
    }

    pub fn new(default_message_store: Option<ArcMut<LocalFileMessageStore>>) -> Self {
        let message_store_config = default_message_store
            .as_ref()
            .map(|message_store| message_store.message_store_config())
            .unwrap_or_else(|| Arc::new(MessageStoreConfig::default()));
        Self::new_with_config(default_message_store, message_store_config)
    }

    pub fn new_with_config(
        default_message_store: Option<ArcMut<LocalFileMessageStore>>,
        message_store_config: Arc<MessageStoreConfig>,
    ) -> Self {
        let timer_metrics_path = get_timer_metrics_path(message_store_config.store_path_root_dir.as_str());
        Self {
            curr_read_time_ms: AtomicI64::new(0),
            curr_queue_offset: AtomicI64::new(0),
            last_enqueue_but_expired_time: 0,
            last_enqueue_but_expired_store_time: 0,
            default_message_store,
            timer_metrics: TimerMetrics::new(Some(timer_metrics_path)),
            message_store_config,
            timer_checkpoint: Mutex::new(None),
            timer_log: Mutex::new(None),
            timer_wheel: Mutex::new(None),
            should_running_dequeue: AtomicBool::new(false),
            enqueue_suspended: AtomicBool::new(false),
            process_lock: AsyncMutex::new(()),
            scheduler_shutdown: Notify::new(),
            scheduler_handle: Mutex::new(None),
            enqueue_tps_counter: TpsCounter::default(),
            dequeue_tps_counter: TpsCounter::default(),
        }
    }

    pub fn new_empty() -> Self {
        Self::new_with_config(None, Arc::new(MessageStoreConfig::default()))
    }

    pub fn set_default_message_store(&mut self, default_message_store: Option<ArcMut<LocalFileMessageStore>>) {
        if let Some(message_store) = default_message_store.as_ref() {
            self.message_store_config = message_store.message_store_config();
            self.timer_metrics.set_config_path(Some(get_timer_metrics_path(
                self.message_store_config.store_path_root_dir.as_str(),
            )));
        }
        self.default_message_store = default_message_store;
    }

    pub fn shutdown(&self) {
        self.scheduler_shutdown.notify_waiters();
        if let Some(handle) = self.scheduler_handle.lock().take() {
            handle.abort();
        }
        self.sync_last_read_time_ms();
        self.timer_metrics.persist();
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
        let timer_log_len = self
            .timer_log
            .lock()
            .as_ref()
            .and_then(|timer_log| timer_log.len().ok())
            .unwrap_or_default();

        if let Some(timer_checkpoint) = self.timer_checkpoint.lock().as_ref() {
            let queue_offset = self.curr_queue_offset.load(Ordering::Relaxed);
            let enqueue_suspended = self.enqueue_suspended.load(Ordering::Relaxed);
            let master_queue_offset = if enqueue_suspended {
                timer_checkpoint.master_timer_queue_offset()
            } else {
                queue_offset
            };
            timer_checkpoint.set_last_read_time_ms(self.curr_read_time_ms.load(Ordering::Relaxed));
            timer_checkpoint.set_last_timer_queue_offset(queue_offset.min(master_queue_offset));
            timer_checkpoint.set_master_timer_queue_offset(master_queue_offset);
            timer_checkpoint.set_last_timer_log_flush_pos(timer_log_len as i64);
            if let Err(err) = timer_checkpoint.flush() {
                error!("flush timer checkpoint failed: {err}");
            }
        }

        if let Some(timer_log) = self.timer_log.lock().as_ref() {
            if let Err(err) = timer_log.flush() {
                error!("flush timer log failed: {err}");
            }
        }

        if let Some(timer_wheel) = self.timer_wheel.lock().as_ref() {
            if let Err(err) = timer_wheel.flush() {
                error!("flush timer wheel failed: {err}");
            }
        }
    }

    pub fn set_should_running_dequeue(&self, should_start: bool) {
        let previous = self.should_running_dequeue.swap(should_start, Ordering::Relaxed);
        if previous == should_start {
            return;
        }

        if should_start {
            self.restore_progress_on_dequeue_resume();
            self.enqueue_suspended.store(false, Ordering::Relaxed);
        } else if previous {
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
            self.curr_read_time_ms
                .store(self.floor_time_ms(last_read_time_ms), Ordering::Relaxed);
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
        let checkpoint_len = timer_checkpoint.last_timer_log_flush_pos().clamp(0, current_len);
        let recovered_len = checkpoint_len - checkpoint_len.rem_euclid(TimerLogRecord::SIZE as i64);
        let aligned_current_len = current_len - current_len.rem_euclid(TimerLogRecord::SIZE as i64);
        let target_len = recovered_len.min(aligned_current_len);
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
        let now_floor = self.floor_time_ms(current_millis() as i64);
        let ttl_floor = now_floor.saturating_sub(self.timer_wheel_window_ms());
        let base = if checkpoint_read_time_ms <= 0 {
            now_floor
        } else {
            self.floor_time_ms(checkpoint_read_time_ms)
        };
        base.max(ttl_floor)
    }

    fn recover_queue_offset(&self, checkpoint_queue_offset: i64) -> i64 {
        let Some(message_store) = self.default_message_store.as_ref() else {
            return checkpoint_queue_offset.max(0);
        };
        let consume_queue = message_store.find_consume_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0);
        let Some(consume_queue) = consume_queue else {
            return checkpoint_queue_offset.max(0);
        };
        let min_offset = consume_queue.get_min_offset_in_queue();
        let max_offset = consume_queue.get_max_offset_in_queue();
        if checkpoint_queue_offset < min_offset {
            warn!(
                "revise timer queue offset from {} to consume queue min {}",
                checkpoint_queue_offset, min_offset
            );
            min_offset
        } else if checkpoint_queue_offset > max_offset {
            warn!(
                "revise timer queue offset from {} to consume queue max {}",
                checkpoint_queue_offset, max_offset
            );
            max_offset
        } else {
            checkpoint_queue_offset
        }
    }

    fn repair_timer_wheel(&self, timer_wheel: &TimerWheel, recovered_log_len: i64) -> std::io::Result<()> {
        timer_wheel.revise_slots(|slot| {
            if slot.num <= 0 {
                return Slot::new_with_num_magic(0, 0, 0, 0, 0);
            }
            let invalid = slot.first_pos < 0
                || slot.last_pos < 0
                || slot.first_pos >= recovered_log_len
                || slot.last_pos >= recovered_log_len
                || slot.first_pos > slot.last_pos
                || slot.first_pos.rem_euclid(TimerLogRecord::SIZE as i64) != 0
                || slot.last_pos.rem_euclid(TimerLogRecord::SIZE as i64) != 0;
            if invalid {
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
        timer_wheel.flush()?;
        timer_checkpoint.flush()
    }

    pub async fn process_once(&self) -> usize {
        let _guard = self.process_lock.lock().await;
        let indexed = self.enqueue_from_timer_topic(self.enqueue_batch_limit());
        let delivered = if self.should_running_dequeue.load(Ordering::Relaxed) {
            self.dequeue_due_messages(self.dequeue_batch_limit()).await
        } else {
            0
        };
        if indexed > 0 || delivered > 0 {
            self.sync_last_read_time_ms();
        }
        indexed + delivered
    }

    fn enqueue_from_timer_topic(&self, limit: usize) -> usize {
        if !self.should_running_enqueue() {
            return 0;
        }
        let Some(message_store) = self.default_message_store.clone() else {
            return 0;
        };
        let timer_topic = CheetahString::from_static_str(TIMER_TOPIC);
        let Some(consume_queue) = message_store.find_consume_queue(&timer_topic, 0) else {
            return 0;
        };

        let now_ms = self.floor_time_ms(current_millis() as i64);
        let dequeue_cursor = self.ensure_dequeue_cursor(now_ms);
        let enqueue_reference_time_ms = dequeue_cursor.max(now_ms);
        let max_offset = consume_queue.get_max_offset_in_queue();
        let mut queue_offset = self.curr_queue_offset.load(Ordering::Relaxed);
        let mut indexed = 0usize;

        while queue_offset < max_offset && indexed < limit {
            let Some(cq_unit) = consume_queue.get(queue_offset) else {
                break;
            };
            let Some(message) = message_store.look_message_by_offset_with_size(cq_unit.pos, cq_unit.size) else {
                warn!(
                    "skip timer queue offset {} because commitlog message {}:{} is missing",
                    queue_offset, cq_unit.pos, cq_unit.size
                );
                break;
            };
            let Some(deliver_time_ms) = parse_deliver_time_ms(&message) else {
                warn!(
                    "skip timer queue offset {} because TIMER_OUT_MS is invalid",
                    queue_offset
                );
                queue_offset = cq_unit.queue_offset + 1;
                self.curr_queue_offset.store(queue_offset, Ordering::Relaxed);
                continue;
            };

            let (slot_time_ms, magic) = self.plan_timer_slot(
                deliver_time_ms,
                enqueue_reference_time_ms,
                dequeue_cursor,
                is_delete_timer_message(&message),
            );
            match self.index_timer_message(slot_time_ms, magic, &cq_unit) {
                Ok(()) => {
                    if let Some(real_topic) =
                        message.property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC))
                    {
                        if !is_delete_timer_message(&message) {
                            self.timer_metrics.add_timing_count(&real_topic, 1);
                        }
                    }
                    self.enqueue_tps_counter.record(1);
                    queue_offset = cq_unit.queue_offset + 1;
                    self.curr_queue_offset.store(queue_offset, Ordering::Relaxed);
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

    async fn dequeue_due_messages(&self, limit: usize) -> usize {
        let now_ms = self.floor_time_ms(current_millis() as i64);
        let mut cursor = self.ensure_dequeue_cursor(now_ms);
        let mut remaining_budget = limit;
        let mut delivered = 0usize;

        while cursor <= now_ms && remaining_budget > 0 {
            if let Some(slot) = self.get_timer_wheel_slot(cursor).filter(|slot| slot.num > 0) {
                let delivered_now = self.deliver_slot(cursor, slot, remaining_budget).await;
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

    fn index_timer_message(&self, slot_time_ms: i64, magic: i32, cq_unit: &CqUnit) -> std::io::Result<()> {
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
        let record_pos = self.append_timer_log(&record.encode())? as i64;
        let first_pos = current_slot
            .filter(|slot| slot.num > 0)
            .map(|slot| slot.first_pos)
            .unwrap_or(record_pos);
        let num = current_slot.map(|slot| slot.num + 1).unwrap_or(1);
        let slot_magic = current_slot.map(|slot| slot.magic | magic).unwrap_or(magic);
        self.put_timer_wheel_slot(slot_time_ms, first_pos, record_pos, num, slot_magic)?;
        Ok(())
    }

    async fn deliver_slot(&self, slot_time_ms: i64, slot: Slot, limit: usize) -> usize {
        let Some(message_store) = self.default_message_store.clone() else {
            return 0;
        };
        let entries = match self.load_slot_entries(slot) {
            Ok(entries) => entries,
            Err(err) => {
                error!("load timer slot {} entries failed: {}", slot_time_ms, err);
                return 0;
            }
        };
        let mut loaded_messages = Vec::with_capacity(limit.min(entries.len()));
        let mut delete_keys = HashSet::new();

        for entry in entries.iter().take(limit) {
            let Some(message) =
                message_store.look_message_by_offset_with_size(entry.record.commit_log_offset, entry.record.size)
            else {
                warn!(
                    "delay delivery blocked at timer log position {} because commitlog {}:{} is missing",
                    entry.position, entry.record.commit_log_offset, entry.record.size
                );
                break;
            };
            if let Some(delete_key) = extract_delete_timer_key(&message) {
                delete_keys.insert(delete_key);
            }
            loaded_messages.push((*entry, message));
        }

        let mut processed = 0usize;
        for (entry, message) in loaded_messages {
            if need_roll(entry.record.magic) {
                let now_ms = self.floor_time_ms(current_millis() as i64);
                if parse_deliver_time_ms(&message).is_some_and(|deliver_time_ms| deliver_time_ms > now_ms) {
                    let rolled_topic =
                        message.property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC));
                    let Some(rolled_message) = self.convert_timer_message(message, true) else {
                        processed += 1;
                        continue;
                    };
                    let put_result = message_store.clone().mut_from_ref().put_message(rolled_message).await;
                    if !put_result.is_ok() {
                        warn!(
                            "roll timer message for slot {} failed with status {:?}",
                            slot_time_ms,
                            put_result.put_message_status()
                        );
                        break;
                    }
                    if let Some(real_topic) = rolled_topic {
                        self.timer_metrics.add_timing_count(&real_topic, -1);
                    }
                    self.dequeue_tps_counter.record(1);
                    processed += 1;
                    continue;
                }
            }
            if is_delete_timer_message(&message) {
                processed += 1;
                continue;
            }
            if let Some(delete_key) = build_delete_key_for_message(&message) {
                if delete_keys.contains(&delete_key) {
                    if let Some(real_topic) =
                        message.property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC))
                    {
                        self.timer_metrics.add_timing_count(&real_topic, -1);
                    }
                    processed += 1;
                    continue;
                }
            }
            let Some(deliver_message) = self.convert_timer_message(message, false) else {
                processed += 1;
                continue;
            };
            let delivered_topic = deliver_message.get_topic().clone();
            let put_result = message_store.clone().mut_from_ref().put_message(deliver_message).await;
            if !put_result.is_ok() {
                warn!(
                    "delay delivery for slot {} failed with status {:?}",
                    slot_time_ms,
                    put_result.put_message_status()
                );
                break;
            }
            self.timer_metrics.add_timing_count(&delivered_topic, -1);
            self.dequeue_tps_counter.record(1);
            processed += 1;
        }

        if let Err(err) = self.rewrite_slot(slot_time_ms, &entries[processed..]) {
            error!("rewrite timer slot {} after delivery failed: {}", slot_time_ms, err);
            return 0;
        }
        processed
    }

    fn load_slot_entries(&self, slot: Slot) -> std::io::Result<Vec<TimerLogEntry>> {
        let mut entries = Vec::with_capacity(slot.num.max(0) as usize);
        let mut current_pos = slot.last_pos;
        for _ in 0..slot.num.max(0) {
            if current_pos < 0 {
                break;
            }
            let buffer = self.read_timer_log(current_pos as u64, TimerLogRecord::SIZE)?;
            let record = TimerLogRecord::decode(&buffer)?;
            entries.push(TimerLogEntry {
                position: current_pos,
                record,
            });
            current_pos = record.prev_pos;
        }
        entries.reverse();
        Ok(entries)
    }

    fn rewrite_slot(&self, slot_time_ms: i64, remaining_entries: &[TimerLogEntry]) -> std::io::Result<()> {
        if remaining_entries.is_empty() {
            self.put_timer_wheel_slot(slot_time_ms, 0, 0, 0, 0)
        } else {
            let slot_magic = remaining_entries
                .iter()
                .fold(0, |combined, entry| combined | entry.record.magic);
            self.put_timer_wheel_slot(
                slot_time_ms,
                remaining_entries.first().expect("non-empty slice").position,
                remaining_entries.last().expect("non-empty slice").position,
                remaining_entries.len() as i32,
                slot_magic,
            )
        }
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

    fn align_delivery_time_ms(&self, deliver_time_ms: i64, dequeue_cursor: i64) -> i64 {
        self.ceil_time_ms(deliver_time_ms).max(dequeue_cursor)
    }

    fn plan_timer_slot(
        &self,
        deliver_time_ms: i64,
        reference_time_ms: i64,
        lower_bound_ms: i64,
        is_delete: bool,
    ) -> (i64, i32) {
        let mut target_time_ms = deliver_time_ms;
        let mut magic = if is_delete { MAGIC_DELETE } else { MAGIC_DEFAULT };
        let roll_window_ms = self.timer_roll_window_ms();

        if deliver_time_ms.saturating_sub(reference_time_ms) >= roll_window_ms {
            magic |= MAGIC_ROLL;
            let overflow_ms = deliver_time_ms
                .saturating_sub(reference_time_ms)
                .saturating_sub(roll_window_ms);
            let near_boundary_ms = self.timer_roll_window_half_ms();
            if overflow_ms < self.timer_roll_window_third_ms() {
                target_time_ms = reference_time_ms.saturating_add(near_boundary_ms);
            } else {
                target_time_ms = reference_time_ms.saturating_add(roll_window_ms);
            }
        }

        (self.align_delivery_time_ms(target_time_ms, lower_bound_ms), magic)
    }

    fn floor_time_ms(&self, time_ms: i64) -> i64 {
        let precision_ms = self.precision_ms();
        time_ms.div_euclid(precision_ms) * precision_ms
    }

    fn ceil_time_ms(&self, time_ms: i64) -> i64 {
        let precision_ms = self.precision_ms();
        if time_ms.rem_euclid(precision_ms) == 0 {
            time_ms
        } else {
            (time_ms.div_euclid(precision_ms) + 1) * precision_ms
        }
    }

    fn precision_ms(&self) -> i64 {
        self.message_store_config.timer_precision_ms.max(1) as i64
    }

    fn timer_wheel_window_ms(&self) -> i64 {
        self.precision_ms() * (TIMER_WHEEL_TTL_DAY as i64 * DAY_SECS as i64)
    }

    fn timer_roll_window_slots(&self) -> i64 {
        let max_slots = (TIMER_WHEEL_TTL_DAY as usize * DAY_SECS as usize).saturating_sub(TIMER_BLANK_SLOTS as usize);
        let configured = self.message_store_config.timer_roll_window_slot;
        if configured < 2 || configured > max_slots {
            max_slots as i64
        } else {
            configured as i64
        }
    }

    fn timer_roll_window_ms(&self) -> i64 {
        self.timer_roll_window_slots().saturating_mul(self.precision_ms())
    }

    fn timer_roll_window_half_ms(&self) -> i64 {
        (self.timer_roll_window_slots() / 2).saturating_mul(self.precision_ms())
    }

    fn timer_roll_window_third_ms(&self) -> i64 {
        (self.timer_roll_window_slots() / 3).saturating_mul(self.precision_ms())
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
        self.message_store_config
            .max_msgs_num_batch
            .max(1)
            .saturating_mul(self.message_store_config.timer_put_message_thread_num.max(1))
    }

    fn dequeue_batch_limit(&self) -> usize {
        self.message_store_config
            .max_msgs_num_batch
            .max(1)
            .saturating_mul(self.message_store_config.timer_get_message_thread_num.max(1))
    }
}

pub fn build_delete_key(real_topic: &str, unique_key: &str) -> CheetahString {
    CheetahString::from_string(format!("{}_{}", real_topic, unique_key))
}

fn parse_deliver_time_ms(message: &MessageExt) -> Option<i64> {
    message
        .property(&CheetahString::from_static_str(TIMER_OUT_MS))
        .and_then(|value| value.parse::<i64>().ok())
}

fn is_delete_timer_message(message: &MessageExt) -> bool {
    extract_delete_timer_key(message).is_some()
}

fn extract_delete_timer_key(message: &MessageExt) -> Option<CheetahString> {
    message.property(&CheetahString::from_static_str(TIMER_DELETE_UNIQUE_KEY))
}

fn build_delete_key_for_message(message: &MessageExt) -> Option<CheetahString> {
    let unique_key = message.property(&CheetahString::from_static_str(
        MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
    ))?;
    let real_topic = message
        .property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC))
        .unwrap_or_else(|| CheetahString::from_string(message.topic().to_string()));
    Some(build_delete_key(real_topic.as_str(), unique_key.as_str()))
}

fn need_roll(magic: i32) -> bool {
    (magic & MAGIC_ROLL) != 0
}

fn read_i64(buffer: &[u8]) -> std::io::Result<i64> {
    let bytes: [u8; 8] = buffer
        .try_into()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "timer log i64 field size must be 8"))?;
    Ok(i64::from_be_bytes(bytes))
}

fn read_i32(buffer: &[u8]) -> std::io::Result<i32> {
    let bytes: [u8; 4] = buffer
        .try_into()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "timer log i32 field size must be 4"))?;
    Ok(i32::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
    use rocketmq_common::common::message::MessageConst;
    use rocketmq_common::common::message::MessageTrait;
    use rocketmq_common::MessageDecoder::message_properties_to_string;
    use tempfile::tempdir;

    use super::*;
    use crate::base::message_store::MessageStore;
    use crate::message_store::local_file_message_store::LocalFileMessageStore;

    fn config_with_root(root_dir: &str) -> Arc<MessageStoreConfig> {
        Arc::new(MessageStoreConfig {
            store_path_root_dir: CheetahString::from_string(root_dir.to_owned()),
            read_uncommitted: true,
            ..MessageStoreConfig::default()
        })
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

    fn build_store_with_timer(
        root_dir: &str,
    ) -> (ArcMut<LocalFileMessageStore>, Arc<TimerMessageStore>, CheetahString) {
        build_store_with_timer_and_config(config_with_root(root_dir))
    }

    fn build_store_with_timer_and_config(
        config: Arc<MessageStoreConfig>,
    ) -> (ArcMut<LocalFileMessageStore>, Arc<TimerMessageStore>, CheetahString) {
        let broker_config = Arc::new(BrokerConfig::default());
        let real_topic = CheetahString::from_static_str("phase3_topic");
        let topic_config_table = Arc::new(DashMap::new());
        topic_config_table.insert(real_topic.clone(), ArcMut::new(TopicConfig::default()));
        topic_config_table.insert(
            CheetahString::from_static_str(TIMER_TOPIC),
            ArcMut::new(TopicConfig::default()),
        );

        let mut store = ArcMut::new(LocalFileMessageStore::new(
            config,
            broker_config,
            topic_config_table,
            None,
            false,
        ));
        let store_clone = store.clone();
        store.set_message_store_arc(store_clone);
        let timer_message_store = Arc::new(TimerMessageStore::new(Some(store.clone())));
        store.set_timer_message_store(timer_message_store.clone());
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

    #[test]
    fn load_creates_durable_timer_artifacts_and_restores_checkpoint() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let config = config_with_root(root_dir.as_str());

        let timer_message_store = TimerMessageStore::new_with_config(None, config.clone());
        assert!(timer_message_store.load());
        assert!(Path::new(get_timer_check_path(root_dir.as_str()).as_str()).exists());
        assert!(Path::new(get_timer_log_path(root_dir.as_str()).as_str()).exists());
        assert!(Path::new(get_timer_wheel_path(root_dir.as_str()).as_str()).exists());

        let read_time_ms = timer_message_store.floor_time_ms(current_millis() as i64);
        timer_message_store
            .curr_read_time_ms
            .store(read_time_ms, Ordering::Relaxed);
        timer_message_store.curr_queue_offset.store(9, Ordering::Relaxed);
        timer_message_store.sync_last_read_time_ms();
        timer_message_store.shutdown();

        let reloaded_store = TimerMessageStore::new_with_config(None, config);
        assert!(reloaded_store.load());
        assert_eq!(reloaded_store.curr_read_time_ms.load(Ordering::Relaxed), read_time_ms);
        assert_eq!(reloaded_store.curr_queue_offset.load(Ordering::Relaxed), 9);
    }

    #[test]
    fn load_restores_timer_log_and_wheel_state() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let config = config_with_root(root_dir.as_str());
        let deliver_time_ms = 30_000;

        let timer_message_store = TimerMessageStore::new_with_config(None, config.clone());
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

        let reloaded_store = TimerMessageStore::new_with_config(None, config);
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

        let timer_message_store = TimerMessageStore::new_with_config(None, config.clone());
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

        let reloaded_store = TimerMessageStore::new_with_config(None, config);
        assert!(reloaded_store.load());
        assert_eq!(
            reloaded_store.timer_log.lock().as_ref().unwrap().len().unwrap(),
            (TimerLogRecord::SIZE * 2) as u64
        );
        assert_eq!(reloaded_store.get_timer_wheel_slot(valid_time_ms).unwrap().num, 2);
        assert!(reloaded_store.get_timer_wheel_slot(invalid_time_ms).is_none());
    }

    #[tokio::test]
    async fn process_once_indexes_timer_topic_messages_without_delivery_when_dequeue_disabled() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.mut_from_ref().load().await);
        let deliver_ms = current_millis() + 60_000;
        let put_result = store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, deliver_ms))
            .await;
        assert!(put_result.is_ok());
        store.mut_from_ref().reput_once().await;

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
        let (store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.mut_from_ref().load().await);
        timer_message_store.set_should_running_dequeue(true);
        let deliver_ms = current_millis().saturating_sub(2_000);
        let put_result = store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, deliver_ms))
            .await;
        assert!(put_result.is_ok());
        store.mut_from_ref().reput_once().await;

        let processed = timer_message_store.process_once().await;
        store.mut_from_ref().reput_once().await;

        assert_eq!(processed, 2);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 1);
        let queue = store.find_consume_queue(&real_topic, 0).unwrap();
        let queue_unit = queue.get(0).unwrap();
        let delivered = store
            .look_message_by_offset_with_size(queue_unit.pos, queue_unit.size)
            .unwrap();
        assert_eq!(delivered.topic(), &real_topic);
        assert_eq!(delivered.body().unwrap(), Bytes::from_static(b"phase3-body"));
        assert!(delivered
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_OUT_MS))
            .is_some());
        assert!(delivered
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_ENQUEUE_MS))
            .is_some());
        assert!(delivered
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DEQUEUE_MS))
            .is_some());
    }

    #[tokio::test]
    async fn process_once_does_not_redeliver_future_timer_message_before_due_time() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.mut_from_ref().load().await);
        timer_message_store.set_should_running_dequeue(true);
        let deliver_ms = current_millis() + 60_000;
        let put_result = store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, deliver_ms))
            .await;
        assert!(put_result.is_ok());
        store.mut_from_ref().reput_once().await;

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
        let (store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.mut_from_ref().load().await);
        timer_message_store.set_should_running_dequeue(true);
        let deliver_ms = current_millis().saturating_sub(2_000);
        let put_result = store
            .mut_from_ref()
            .put_message(build_timer_message_with_queue_id(&real_topic, 3, deliver_ms))
            .await;
        assert!(put_result.is_ok());
        store.mut_from_ref().reput_once().await;

        assert_eq!(timer_message_store.process_once().await, 2);
        store.mut_from_ref().reput_once().await;

        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 0);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 3), 1);
        let queue = store.find_consume_queue(&real_topic, 3).unwrap();
        let queue_unit = queue.get(0).unwrap();
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
            .is_some());
        assert!(delivered
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_ENQUEUE_MS))
            .is_some());
        assert!(delivered
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DEQUEUE_MS))
            .is_some());
    }

    #[tokio::test]
    async fn process_once_pauses_enqueue_after_role_change_when_master_progress_is_caught_up() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.mut_from_ref().load().await);
        timer_message_store.set_should_running_dequeue(true);
        let first_deliver_ms = current_millis() + 60_000;
        assert!(store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, first_deliver_ms))
            .await
            .is_ok());
        store.mut_from_ref().reput_once().await;
        assert_eq!(timer_message_store.process_once().await, 1);
        timer_message_store.set_should_running_dequeue(false);

        let second_deliver_ms = current_millis() + 120_000;
        assert!(store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, second_deliver_ms))
            .await
            .is_ok());
        store.mut_from_ref().reput_once().await;

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
        let (store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.mut_from_ref().load().await);
        timer_message_store.set_should_running_dequeue(true);
        let first_deliver_ms = current_millis().saturating_sub(2_000);
        assert!(store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, first_deliver_ms))
            .await
            .is_ok());
        store.mut_from_ref().reput_once().await;
        assert_eq!(timer_message_store.process_once().await, 2);
        store.mut_from_ref().reput_once().await;
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 1);

        timer_message_store.set_should_running_dequeue(false);
        let second_deliver_ms = current_millis().saturating_sub(1_000);
        assert!(store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, second_deliver_ms))
            .await
            .is_ok());
        store.mut_from_ref().reput_once().await;
        assert_eq!(timer_message_store.process_once().await, 0);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 1);

        timer_message_store.set_should_running_dequeue(true);
        let resumed = timer_message_store.process_once().await;
        store.mut_from_ref().reput_once().await;

        assert_eq!(resumed, 2);
        assert_eq!(timer_message_store.curr_queue_offset.load(Ordering::Relaxed), 2);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 2);
    }

    #[tokio::test]
    async fn process_once_delete_tombstone_skips_matching_timer_message_delivery() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.mut_from_ref().load().await);
        timer_message_store.set_should_running_dequeue(true);
        let deliver_ms = current_millis().saturating_sub(2_000);
        let unique_key = "delete-me";
        let mut timer_message = build_timer_message(&real_topic, deliver_ms);
        timer_message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from_static_str(unique_key),
        );
        timer_message.properties_string = message_properties_to_string(timer_message.get_properties());
        assert!(store.mut_from_ref().put_message(timer_message).await.is_ok());
        assert!(store
            .mut_from_ref()
            .put_message(build_delete_timer_message(&real_topic, unique_key, deliver_ms))
            .await
            .is_ok());
        store.mut_from_ref().reput_once().await;

        let processed = timer_message_store.process_once().await;
        store.mut_from_ref().reput_once().await;

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
        let (store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.mut_from_ref().load().await);
        timer_message_store.set_should_running_dequeue(true);
        let deliver_ms = current_millis().saturating_sub(2_000);
        let mut deleted_message = build_timer_message(&real_topic, deliver_ms);
        deleted_message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from_static_str("deleted-key"),
        );
        deleted_message.properties_string = message_properties_to_string(deleted_message.get_properties());
        assert!(store.mut_from_ref().put_message(deleted_message).await.is_ok());

        let mut survivor_message = build_timer_message(&real_topic, deliver_ms);
        survivor_message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from_static_str("survivor-key"),
        );
        survivor_message.properties_string = message_properties_to_string(survivor_message.get_properties());
        assert!(store.mut_from_ref().put_message(survivor_message).await.is_ok());

        assert!(store
            .mut_from_ref()
            .put_message(build_delete_timer_message(&real_topic, "deleted-key", deliver_ms))
            .await
            .is_ok());
        store.mut_from_ref().reput_once().await;

        let processed = timer_message_store.process_once().await;
        store.mut_from_ref().reput_once().await;

        assert_eq!(processed, 6);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 1);
    }

    #[tokio::test]
    async fn restart_recovers_indexed_due_timer_message_from_persisted_wheel() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.mut_from_ref().load().await);
        let deliver_ms = current_millis().saturating_sub(2_000);
        let put_result = store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, deliver_ms))
            .await;
        assert!(put_result.is_ok());
        store.mut_from_ref().reput_once().await;

        assert_eq!(timer_message_store.process_once().await, 1);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 0);
        store.mut_from_ref().shutdown().await;

        let (reloaded_store, reloaded_timer_message_store, reloaded_topic) = build_store_with_timer(root_dir.as_str());
        assert_eq!(reloaded_topic, real_topic);
        assert!(reloaded_store.mut_from_ref().load().await);
        reloaded_timer_message_store.set_should_running_dequeue(true);

        let delivered = reloaded_timer_message_store.process_once().await;
        reloaded_store.mut_from_ref().reput_once().await;

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
        let (store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.mut_from_ref().load().await);
        timer_message_store.set_should_running_dequeue(true);
        let deliver_ms = current_millis().saturating_sub(2_000);
        let put_result = store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, deliver_ms))
            .await;
        assert!(put_result.is_ok());
        store.mut_from_ref().reput_once().await;
        assert_eq!(timer_message_store.process_once().await, 2);
        store.mut_from_ref().reput_once().await;
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 1);
        store.mut_from_ref().shutdown().await;

        let checkpoint = TimerCheckpoint::new(get_timer_check_path(root_dir.as_str())).unwrap();
        checkpoint.set_last_timer_queue_offset(0);
        checkpoint.set_master_timer_queue_offset(0);
        checkpoint.flush().unwrap();

        let (reloaded_store, reloaded_timer_message_store, reloaded_topic) = build_store_with_timer(root_dir.as_str());
        assert_eq!(reloaded_topic, real_topic);
        assert!(reloaded_store.mut_from_ref().load().await);
        reloaded_timer_message_store.set_should_running_dequeue(true);

        let reprocessed = reloaded_timer_message_store.process_once().await;
        reloaded_store.mut_from_ref().reput_once().await;

        assert_eq!(reprocessed, 2);
        assert_eq!(reloaded_store.get_max_offset_in_queue(&real_topic, 0), 2);
    }

    #[tokio::test]
    async fn load_revises_checkpoint_queue_offset_to_timer_queue_max() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.mut_from_ref().load().await);
        let deliver_ms = current_millis() + 60_000;
        let put_result = store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, deliver_ms))
            .await;
        assert!(put_result.is_ok());
        store.mut_from_ref().reput_once().await;
        assert_eq!(timer_message_store.process_once().await, 1);
        store.mut_from_ref().shutdown().await;

        let checkpoint = TimerCheckpoint::new(get_timer_check_path(root_dir.as_str())).unwrap();
        checkpoint.set_last_timer_queue_offset(99);
        checkpoint.set_master_timer_queue_offset(99);
        checkpoint.flush().unwrap();

        let (reloaded_store, reloaded_timer_message_store, _) = build_store_with_timer(root_dir.as_str());
        assert!(reloaded_store.mut_from_ref().load().await);
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
        let (store, timer_message_store, real_topic) = build_store_with_timer_and_config(config);

        assert!(store.mut_from_ref().load().await);
        let first_put = store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, current_millis() + 60_000))
            .await;
        assert!(first_put.is_ok());
        let second_put = store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, current_millis() + 61_000))
            .await;
        assert!(second_put.is_ok());
        store.mut_from_ref().reput_once().await;

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
        let (store, timer_message_store, real_topic) = build_store_with_timer_and_config(config);

        assert!(store.mut_from_ref().load().await);
        let deliver_ms = current_millis().saturating_sub(2_000);
        let first_put = store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, deliver_ms))
            .await;
        assert!(first_put.is_ok());
        let second_put = store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, deliver_ms))
            .await;
        assert!(second_put.is_ok());
        store.mut_from_ref().reput_once().await;

        assert_eq!(timer_message_store.process_once().await, 1);
        assert_eq!(timer_message_store.process_once().await, 1);
        let hot_slot_time = timer_message_store.curr_read_time_ms.load(Ordering::Relaxed);
        timer_message_store.set_should_running_dequeue(true);

        let first_delivery_tick = timer_message_store.process_once().await;
        store.mut_from_ref().reput_once().await;

        assert_eq!(first_delivery_tick, 1);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 1);
        assert_eq!(timer_message_store.get_timer_wheel_slot(hot_slot_time).unwrap().num, 1);

        let second_delivery_tick = timer_message_store.process_once().await;
        store.mut_from_ref().reput_once().await;

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
        let (store, timer_message_store, real_topic) = build_store_with_timer_and_config(config);

        assert!(store.mut_from_ref().load().await);
        let now_floor = timer_message_store.floor_time_ms(current_millis() as i64);
        let deliver_ms = (now_floor + 20_000) as u64;
        assert!(store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, deliver_ms))
            .await
            .is_ok());
        store.mut_from_ref().reput_once().await;

        assert_eq!(timer_message_store.process_once().await, 1);

        let rolled_slot_time = now_floor + 4_000;
        let original_slot_time = timer_message_store.ceil_time_ms(deliver_ms as i64);
        let slot = timer_message_store.get_timer_wheel_slot(rolled_slot_time).unwrap();
        let entries = timer_message_store.load_slot_entries(slot).unwrap();

        assert!(timer_message_store.get_timer_wheel_slot(original_slot_time).is_none());
        assert_eq!(slot.num, 1);
        assert_ne!(entries[0].record.magic & MAGIC_ROLL, 0);
        assert_eq!(entries[0].record.deliver_time_ms, rolled_slot_time);
    }

    #[tokio::test]
    async fn process_once_clamps_future_read_cursor_when_clock_moves_backward() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let config = config_with_root_precision_and_roll_window(root_dir.as_str(), 1_000, 60);
        let (store, timer_message_store, real_topic) = build_store_with_timer_and_config(config);

        assert!(store.mut_from_ref().load().await);
        let now_floor = timer_message_store.floor_time_ms(current_millis() as i64);
        timer_message_store
            .curr_read_time_ms
            .store(now_floor + 10_000, Ordering::Relaxed);

        let deliver_ms = (now_floor + 2_000) as u64;
        assert!(store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, deliver_ms))
            .await
            .is_ok());
        store.mut_from_ref().reput_once().await;

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
        let config = config_with_root_precision_and_roll_window(root_dir.as_str(), 50, 4);
        let (store, timer_message_store, real_topic) = build_store_with_timer_and_config(config);

        assert!(store.mut_from_ref().load().await);
        let deliver_ms = current_millis() + 1_000;
        assert!(store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, deliver_ms))
            .await
            .is_ok());
        store.mut_from_ref().reput_once().await;
        assert_eq!(timer_message_store.process_once().await, 1);

        timer_message_store.set_should_running_dequeue(true);
        tokio::time::sleep(Duration::from_millis(250)).await;

        let processed = timer_message_store.process_once().await;
        store.mut_from_ref().reput_once().await;

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
        let rolled_queue_unit = timer_queue.get(1).unwrap();
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
        let (store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.mut_from_ref().load().await);
        assert!(store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, current_millis() + 60_000))
            .await
            .is_ok());
        store.mut_from_ref().reput_once().await;

        assert_eq!(timer_message_store.process_once().await, 1);
        assert!(timer_message_store.get_enqueue_tps() > 0.0);
    }

    #[tokio::test]
    async fn process_once_reports_dequeue_tps_after_redelivery() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.mut_from_ref().load().await);
        timer_message_store.set_should_running_dequeue(true);
        assert!(store
            .mut_from_ref()
            .put_message(build_timer_message(&real_topic, current_millis().saturating_sub(2_000)))
            .await
            .is_ok());
        store.mut_from_ref().reput_once().await;

        assert_eq!(timer_message_store.process_once().await, 2);
        assert!(timer_message_store.get_dequeue_tps() > 0.0);
    }

    #[tokio::test]
    async fn timer_processor_does_not_touch_non_timer_messages() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let (store, timer_message_store, real_topic) = build_store_with_timer(root_dir.as_str());

        assert!(store.mut_from_ref().load().await);
        timer_message_store.set_should_running_dequeue(true);
        let mut msg = MessageExtBrokerInner::default();
        msg.set_topic(real_topic.clone());
        msg.message_ext_inner.queue_id = 0;
        msg.set_body(Bytes::from_static(b"ordinary-body"));

        let put_result = store.mut_from_ref().put_message(msg).await;
        assert!(put_result.is_ok());
        store.mut_from_ref().reput_once().await;

        assert_eq!(timer_message_store.process_once().await, 0);
        assert_eq!(store.get_max_offset_in_queue(&real_topic, 0), 1);
    }
}
