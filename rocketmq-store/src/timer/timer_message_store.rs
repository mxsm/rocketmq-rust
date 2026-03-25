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

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::system_clock::SystemClock;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_rust::ArcMut;
use tracing::error;
use tracing::warn;

use crate::base::message_store::MessageStore;
use crate::config::message_store_config::MessageStoreConfig;
use crate::message_store::local_file_message_store::LocalFileMessageStore;
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

    pub fn start(&self) {
        warn!("TimerMessageStore scheduler is not implemented yet, start only enables lifecycle hooks");
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
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use cheetah_string::CheetahString;
    use tempfile::tempdir;

    use super::*;

    fn config_with_root(root_dir: &str) -> Arc<MessageStoreConfig> {
        Arc::new(MessageStoreConfig {
            store_path_root_dir: CheetahString::from_string(root_dir.to_owned()),
            ..MessageStoreConfig::default()
        })
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
}
