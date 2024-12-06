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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::body::pop_process_queue_info::PopProcessQueueInfo;

use crate::consumer::consumer_impl::PULL_MAX_IDLE_TIME;

#[derive(Clone)]
pub(crate) struct PopProcessQueue {
    last_pop_timestamp: u64,
    wait_ack_counter: Arc<AtomicUsize>,
    dropped: bool,
}

impl PopProcessQueue {
    pub(crate) fn new() -> Self {
        PopProcessQueue {
            last_pop_timestamp: get_current_millis(),
            wait_ack_counter: Arc::new(AtomicUsize::new(0)),
            dropped: false,
        }
    }

    pub(crate) fn get_last_pop_timestamp(&self) -> u64 {
        self.last_pop_timestamp
    }

    pub(crate) fn set_last_pop_timestamp(&mut self, last_pop_timestamp: u64) {
        self.last_pop_timestamp = last_pop_timestamp;
    }

    pub(crate) fn inc_found_msg(&self, count: isize) {
        self.wait_ack_counter
            .fetch_add(count as usize, Ordering::Relaxed);
    }

    pub(crate) fn ack(&self) -> usize {
        self.wait_ack_counter.fetch_sub(1, Ordering::Relaxed)
    }
    pub(crate) fn dec_found_msg(&self, count: isize) {
        self.wait_ack_counter
            .fetch_add(-count as usize, Ordering::Relaxed);
    }

    pub(crate) fn get_wai_ack_msg_count(&self) -> usize {
        self.wait_ack_counter.load(Ordering::Relaxed)
    }

    pub(crate) fn is_dropped(&self) -> bool {
        self.dropped
    }

    pub(crate) fn set_dropped(&self, dropped: bool) {
        //self.dropped = dropped;
    }

    pub(crate) fn fill_pop_process_queue_info(&self, info: &mut PopProcessQueueInfo) {
        info.set_wait_ack_count(self.get_wai_ack_msg_count() as i32);
        info.set_droped(self.is_dropped());
        info.set_last_pop_timestamp(self.get_last_pop_timestamp());
    }

    pub(crate) fn is_pull_expired(&self) -> bool {
        let current_time = get_current_millis();
        (current_time - self.last_pop_timestamp) > *PULL_MAX_IDLE_TIME
    }
}
