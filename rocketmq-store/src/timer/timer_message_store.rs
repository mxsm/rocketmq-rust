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
    process_lock: AsyncMutex<()>,
    scheduler_shutdown: Notify,
    scheduler_handle: Mutex<Option<JoinHandle<()>>>,
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

        self.curr_read_time_ms
            .store(timer_checkpoint.last_read_time_ms(), Ordering::Relaxed);
        self.curr_queue_offset
            .store(timer_checkpoint.last_timer_queue_offset(), Ordering::Relaxed);
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
        0.0
    }

    pub fn get_dequeue_tps(&self) -> f32 {
        0.0
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
            process_lock: AsyncMutex::new(()),
            scheduler_shutdown: Notify::new(),
            scheduler_handle: Mutex::new(None),
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
            timer_checkpoint.set_last_read_time_ms(self.curr_read_time_ms.load(Ordering::Relaxed));
            timer_checkpoint.set_last_timer_queue_offset(self.curr_queue_offset.load(Ordering::Relaxed));
            timer_checkpoint.set_master_timer_queue_offset(self.curr_queue_offset.load(Ordering::Relaxed));
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
        self.should_running_dequeue.store(should_start, Ordering::Relaxed);
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

    pub async fn process_once(&self) -> usize {
        let _guard = self.process_lock.lock().await;
        let indexed = self.enqueue_from_timer_topic();
        let delivered = if self.should_running_dequeue.load(Ordering::Relaxed) {
            self.dequeue_due_messages().await
        } else {
            0
        };
        if indexed > 0 || delivered > 0 {
            self.sync_last_read_time_ms();
        }
        indexed + delivered
    }

    fn enqueue_from_timer_topic(&self) -> usize {
        let Some(message_store) = self.default_message_store.clone() else {
            return 0;
        };
        let timer_topic = CheetahString::from_static_str(TIMER_TOPIC);
        let Some(consume_queue) = message_store.find_consume_queue(&timer_topic, 0) else {
            return 0;
        };

        let now_ms = self.floor_time_ms(current_millis() as i64);
        let dequeue_cursor = self.ensure_dequeue_cursor(now_ms);
        let max_offset = consume_queue.get_max_offset_in_queue();
        let mut queue_offset = self.curr_queue_offset.load(Ordering::Relaxed);
        let mut indexed = 0usize;

        while queue_offset < max_offset {
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

            let slot_time_ms = self.align_delivery_time_ms(deliver_time_ms, dequeue_cursor);
            match self.index_timer_message(slot_time_ms, &cq_unit) {
                Ok(()) => {
                    if let Some(real_topic) =
                        message.property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC))
                    {
                        self.timer_metrics.add_timing_count(&real_topic, 1);
                    }
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

    async fn dequeue_due_messages(&self) -> usize {
        let now_ms = self.floor_time_ms(current_millis() as i64);
        let mut cursor = self.ensure_dequeue_cursor(now_ms);
        let mut delivered = 0usize;

        while cursor <= now_ms {
            if let Some(slot) = self.get_timer_wheel_slot(cursor).filter(|slot| slot.num > 0) {
                delivered += self.deliver_slot(cursor, slot).await;
                if self
                    .get_timer_wheel_slot(cursor)
                    .is_some_and(|remaining| remaining.num > 0)
                {
                    self.curr_read_time_ms.store(cursor, Ordering::Relaxed);
                    break;
                }
            }
            cursor += self.precision_ms();
            self.curr_read_time_ms.store(cursor, Ordering::Relaxed);
        }

        delivered
    }

    fn index_timer_message(&self, slot_time_ms: i64, cq_unit: &CqUnit) -> std::io::Result<()> {
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
            magic: MAGIC_DEFAULT,
        };
        let record_pos = self.append_timer_log(&record.encode())? as i64;
        let first_pos = current_slot
            .filter(|slot| slot.num > 0)
            .map(|slot| slot.first_pos)
            .unwrap_or(record_pos);
        let num = current_slot.map(|slot| slot.num + 1).unwrap_or(1);
        self.put_timer_wheel_slot(slot_time_ms, first_pos, record_pos, num, MAGIC_DEFAULT)?;
        Ok(())
    }

    async fn deliver_slot(&self, slot_time_ms: i64, slot: Slot) -> usize {
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
        let mut delivered = 0usize;

        for entry in &entries {
            let Some(message) =
                message_store.look_message_by_offset_with_size(entry.record.commit_log_offset, entry.record.size)
            else {
                warn!(
                    "delay delivery blocked at timer log position {} because commitlog {}:{} is missing",
                    entry.position, entry.record.commit_log_offset, entry.record.size
                );
                break;
            };
            let Some(deliver_message) = self.restore_timer_message(message) else {
                delivered += 1;
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
            delivered += 1;
        }

        if let Err(err) = self.rewrite_slot(slot_time_ms, &entries[delivered..]) {
            error!("rewrite timer slot {} after delivery failed: {}", slot_time_ms, err);
            return 0;
        }
        delivered
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
            self.put_timer_wheel_slot(
                slot_time_ms,
                remaining_entries.first().expect("non-empty slice").position,
                remaining_entries.last().expect("non-empty slice").position,
                remaining_entries.len() as i32,
                MAGIC_DEFAULT,
            )
        }
    }

    fn restore_timer_message(&self, message: MessageExt) -> Option<MessageExtBrokerInner> {
        let mut inner = MessageExtBrokerInner::default();
        let sys_flag = message.sys_flag();
        let born_timestamp = message.born_timestamp();
        let born_host = message.born_host();
        let store_host = message.store_host();
        let reconsume_times = message.reconsume_times();
        let message_inner = message.message;
        let message_properties = message_inner.properties().as_map().clone();

        if let Some(body) = message_inner.get_body() {
            inner.set_body(body.clone());
        }
        inner.set_flag(message_inner.flag());
        MessageAccessor::set_properties(&mut inner, message_properties);
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

        MessageAccessor::clear_property(&mut inner, MessageConst::PROPERTY_DELAY_TIME_LEVEL);
        MessageAccessor::clear_property(&mut inner, MessageConst::PROPERTY_TIMER_DELIVER_MS);
        MessageAccessor::clear_property(&mut inner, MessageConst::PROPERTY_TIMER_DELAY_SEC);
        MessageAccessor::clear_property(&mut inner, MessageConst::PROPERTY_TIMER_DELAY_MS);
        MessageAccessor::clear_property(&mut inner, MessageConst::PROPERTY_TIMER_OUT_MS);
        MessageAccessor::clear_property(&mut inner, MessageConst::PROPERTY_TIMER_ENQUEUE_MS);
        MessageAccessor::clear_property(&mut inner, MessageConst::PROPERTY_TIMER_DEQUEUE_MS);
        MessageAccessor::clear_property(&mut inner, MessageConst::PROPERTY_TIMER_ROLL_TIMES);
        MessageAccessor::clear_property(&mut inner, MessageConst::PROPERTY_TIMER_DEL_UNIQKEY);

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
        inner.properties_string = MessageDecoder::message_properties_to_string(inner.get_properties());
        Some(inner)
    }

    fn ensure_dequeue_cursor(&self, fallback_time_ms: i64) -> i64 {
        let current = self.curr_read_time_ms.load(Ordering::Relaxed);
        if current > 0 {
            return self.floor_time_ms(current);
        }

        let aligned = self.floor_time_ms(fallback_time_ms);
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
}

fn parse_deliver_time_ms(message: &MessageExt) -> Option<i64> {
    message
        .property(&CheetahString::from_static_str(TIMER_OUT_MS))
        .and_then(|value| value.parse::<i64>().ok())
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

    fn build_store_with_timer(
        root_dir: &str,
    ) -> (ArcMut<LocalFileMessageStore>, Arc<TimerMessageStore>, CheetahString) {
        let config = config_with_root(root_dir);
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
            CheetahString::from_static_str("0"),
        );
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_OUT_MS),
            CheetahString::from_string(deliver_ms.to_string()),
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

        timer_message_store.curr_read_time_ms.store(12_345, Ordering::Relaxed);
        timer_message_store.curr_queue_offset.store(9, Ordering::Relaxed);
        timer_message_store.sync_last_read_time_ms();
        timer_message_store.shutdown();

        let reloaded_store = TimerMessageStore::new_with_config(None, config);
        assert!(reloaded_store.load());
        assert_eq!(reloaded_store.curr_read_time_ms.load(Ordering::Relaxed), 12_345);
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

        let log_offset = timer_message_store.append_timer_log(b"phase2").unwrap();
        timer_message_store
            .put_timer_wheel_slot(
                deliver_time_ms,
                log_offset as i64,
                (log_offset + 5) as i64,
                1,
                MAGIC_DEFAULT,
            )
            .unwrap();
        timer_message_store.shutdown();

        let reloaded_store = TimerMessageStore::new_with_config(None, config);
        assert!(reloaded_store.load());
        assert_eq!(reloaded_store.read_timer_log(log_offset, 6).unwrap(), b"phase2");
        assert_eq!(reloaded_store.get_timer_wheel_slot(deliver_time_ms).unwrap().num, 1);
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
            .is_none());
    }
}
