/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::sync::atomic::AtomicI64;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::system_clock::SystemClock;
use rocketmq_rust::ArcMut;
use tracing::warn;

use crate::log_file::MessageStore;
use crate::message_store::default_message_store::DefaultMessageStore;

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
    pub default_message_store: Option<ArcMut<DefaultMessageStore>>,
}

impl Clone for TimerMessageStore {
    fn clone(&self) -> Self {
        Self {
            curr_read_time_ms: AtomicI64::new(
                self.curr_read_time_ms
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            curr_queue_offset: AtomicI64::new(
                self.curr_queue_offset
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            default_message_store: self.default_message_store.clone(),
        }
    }
}

impl TimerMessageStore {
    pub fn load(&mut self) -> bool {
        true
    }

    pub fn start(&mut self) {}

    pub fn is_reject(&self, _deliver_ms: u64) -> bool {
        false
    }

    pub fn get_dequeue_behind(&self) -> i64 {
        self.get_dequeue_behind_millis() / 1000
    }

    pub fn get_dequeue_behind_millis(&self) -> i64 {
        (SystemClock::now() as i64)
            - self
                .curr_read_time_ms
                .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn get_enqueue_behind_millis(&self) -> i64 {
        unimplemented!("getEnqueueBehindMillis")
    }

    pub fn get_enqueue_behind(&self) -> i64 {
        self.get_enqueue_behind_millis() / 1000
    }

    pub fn get_enqueue_behind_messages(&self) -> i64 {
        let temp_queue_offset = self
            .curr_queue_offset
            .load(std::sync::atomic::Ordering::Relaxed);
        let consume_queue = self
            .default_message_store
            .as_ref()
            .unwrap()
            .find_consume_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0);
        let max_offset_in_queue = match consume_queue {
            Some(queue) => queue.get_max_offset_in_queue(),
            None => 0,
        };
        max_offset_in_queue - temp_queue_offset
    }

    pub fn get_all_congest_num(&self) -> i64 {
        0
    }

    pub fn get_enqueue_tps(&self) -> f32 {
        0.0
    }

    pub fn get_dequeue_tps(&self) -> f32 {
        0.0
    }

    pub fn new(default_message_store: Option<ArcMut<DefaultMessageStore>>) -> Self {
        Self {
            curr_read_time_ms: AtomicI64::new(0),
            curr_queue_offset: AtomicI64::new(0),
            default_message_store,
        }
    }

    pub fn new_empty() -> Self {
        Self {
            curr_read_time_ms: AtomicI64::new(0),
            curr_queue_offset: AtomicI64::new(0),
            default_message_store: None,
        }
    }

    pub fn set_default_message_store(
        &mut self,
        default_message_store: Option<ArcMut<DefaultMessageStore>>,
    ) {
        self.default_message_store = default_message_store;
    }

    pub fn shutdown(&mut self) {
        warn!("TimerMessageStore shutdown unimplemented, do nothing");
    }
}
