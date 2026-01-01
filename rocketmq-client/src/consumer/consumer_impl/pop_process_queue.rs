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

use std::fmt::Display;
use std::hash::Hash;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::atomic::Ordering::Acquire;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::body::pop_process_queue_info::PopProcessQueueInfo;

use crate::consumer::consumer_impl::PULL_MAX_IDLE_TIME;

#[derive(Clone)]
pub(crate) struct PopProcessQueue {
    last_pop_timestamp: Arc<AtomicU64>,
    wait_ack_counter: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
}

impl Hash for PopProcessQueue {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.last_pop_timestamp.load(Relaxed).hash(state);
        self.wait_ack_counter.load(Relaxed).hash(state);
        self.dropped.load(Relaxed).hash(state);
    }
}

impl Default for PopProcessQueue {
    fn default() -> Self {
        PopProcessQueue {
            last_pop_timestamp: Arc::new(AtomicU64::new(get_current_millis())),
            wait_ack_counter: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl PopProcessQueue {
    #[inline]
    pub(crate) fn new() -> Self {
        PopProcessQueue::default()
    }

    #[inline]
    pub(crate) fn get_last_pop_timestamp(&self) -> u64 {
        self.last_pop_timestamp.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn set_last_pop_timestamp(&self, last_pop_timestamp: u64) {
        self.last_pop_timestamp.store(last_pop_timestamp, Ordering::Release);
    }

    #[inline]
    pub(crate) fn inc_found_msg(&self, count: usize) {
        self.wait_ack_counter.fetch_add(count, Ordering::AcqRel);
    }

    #[inline]
    pub(crate) fn ack(&self) -> usize {
        self.wait_ack_counter.fetch_sub(1, Ordering::AcqRel)
    }

    #[inline]
    pub(crate) fn dec_found_msg(&self, count: usize) {
        self.wait_ack_counter.fetch_sub(count, Ordering::AcqRel);
    }

    #[inline]
    pub(crate) fn get_wai_ack_msg_count(&self) -> usize {
        self.wait_ack_counter.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn is_dropped(&self) -> bool {
        self.dropped.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn set_dropped(&self, dropped: bool) {
        self.dropped.store(dropped, Ordering::Release);
    }

    #[inline]
    pub(crate) fn fill_pop_process_queue_info(&self, info: &mut PopProcessQueueInfo) {
        info.set_wait_ack_count(self.get_wai_ack_msg_count() as i32);
        info.set_droped(self.is_dropped());
        info.set_last_pop_timestamp(self.get_last_pop_timestamp());
    }

    #[inline]
    pub(crate) fn is_pull_expired(&self) -> bool {
        let current_time = get_current_millis();
        current_time.saturating_sub(self.last_pop_timestamp.load(Acquire)) > *PULL_MAX_IDLE_TIME
    }
}

impl Display for PopProcessQueue {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PopProcessQueue [last_pop_timestamp={:?}, wait_ack_counter={}, dropped={}]",
            self.last_pop_timestamp,
            self.wait_ack_counter.load(Ordering::Relaxed),
            self.dropped.load(Ordering::Relaxed)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pop_process_queue_initializes_correctly() {
        let queue = PopProcessQueue::new();
        assert!(queue.get_last_pop_timestamp() > 0);
        assert_eq!(queue.get_wai_ack_msg_count(), 0);
        assert!(!queue.is_dropped());
    }

    #[test]
    fn pop_process_queue_updates_last_pop_timestamp() {
        let queue = PopProcessQueue::new();
        let new_timestamp = queue.get_last_pop_timestamp() + 1000;
        queue.set_last_pop_timestamp(new_timestamp);
        assert_eq!(queue.get_last_pop_timestamp(), new_timestamp);
    }

    #[test]
    fn pop_process_queue_increments_found_msg() {
        let queue = PopProcessQueue::new();
        queue.inc_found_msg(5);
        assert_eq!(queue.get_wai_ack_msg_count(), 5);
    }

    #[test]
    fn pop_process_queue_decrements_found_msg() {
        let queue = PopProcessQueue::new();
        queue.inc_found_msg(5);
        queue.dec_found_msg(3);
        assert_eq!(queue.get_wai_ack_msg_count(), 2);
    }

    #[test]
    fn pop_process_queue_acknowledges_msg() {
        let queue = PopProcessQueue::new();
        queue.inc_found_msg(5);
        queue.ack();
        assert_eq!(queue.get_wai_ack_msg_count(), 4);
    }

    #[test]
    fn pop_process_queue_sets_dropped_flag() {
        let queue = PopProcessQueue::new();
        queue.set_dropped(true);
        assert!(queue.is_dropped());
    }

    #[test]
    fn pop_process_queue_detects_pull_expired() {
        let queue = PopProcessQueue::new();
        queue.set_last_pop_timestamp(queue.get_last_pop_timestamp() - *PULL_MAX_IDLE_TIME - 1);
        assert!(queue.is_pull_expired());
    }
}
