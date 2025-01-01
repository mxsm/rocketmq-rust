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
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::pop_ack_constants::PopAckConstants;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_store::pop::pop_check_point::PopCheckPoint;
use rocketmq_store::pop::AckMessage;

use crate::processor::pop_message_processor::QueueLockManager;

pub(crate) struct PopBufferMergeService {
    buffer: DashMap<CheetahString /* mergeKey */, PopCheckPointWrapper>,
    commit_offsets:
        DashMap<CheetahString /* topic@cid@queueId */, QueueWithTime<PopCheckPointWrapper>>,
    serving: AtomicBool,
    counter: AtomicI32,
    scan_times: i32,
    revive_topic: CheetahString,
    queue_lock_manager: QueueLockManager,
    interval: u64,
    minute5: u64,
    count_of_minute1: u64,
    count_of_second1: u64,
    count_of_second30: u64,
    batch_ack_index_list: Vec<u8>,
    master: AtomicBool,
}

impl PopBufferMergeService {
    pub fn new(revive_topic: CheetahString, queue_lock_manager: QueueLockManager) -> Self {
        let interval = 5;
        Self {
            buffer: DashMap::with_capacity(1024 * 16),
            commit_offsets: DashMap::with_capacity(1024),
            serving: AtomicBool::new(true),
            counter: AtomicI32::new(0),
            scan_times: 0,
            revive_topic,
            queue_lock_manager,
            interval,
            minute5: 5 * 60 * 1000,
            count_of_minute1: 60 * 1000 / interval,
            count_of_second1: 1000 / interval,
            count_of_second30: 30 * 1000 / interval,
            batch_ack_index_list: Vec::with_capacity(32),
            master: AtomicBool::new(false),
        }
    }
}

impl PopBufferMergeService {
    pub fn add_ack(&mut self, _revive_qid: i32, _ack_msg: &dyn AckMessage) -> bool {
        unimplemented!("Not implemented yet");
    }

    pub fn get_latest_offset(&self, _lock_key: &str) -> i64 {
        unimplemented!("Not implemented yet");
    }

    pub fn clear_offset_queue(&self, _lock_key: &str) {
        unimplemented!("Not implemented yet");
    }
}

pub struct QueueWithTime<T> {
    queue: VecDeque<T>,
    time: u64,
}

impl<T> QueueWithTime<T> {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            time: get_current_millis(),
        }
    }

    pub fn set_time(&mut self, pop_time: u64) {
        self.time = pop_time;
    }

    pub fn get_time(&self) -> u64 {
        self.time
    }

    pub fn get_queue(&self) -> &VecDeque<T> {
        &self.queue
    }

    pub fn get_queue_mut(&mut self) -> &mut VecDeque<T> {
        &mut self.queue
    }
}

pub struct PopCheckPointWrapper {
    revive_queue_id: i32,
    // -1: not stored, >=0: stored, Long.MAX: storing.
    revive_queue_offset: AtomicI32,
    ck: Arc<PopCheckPoint>,
    // bit for concurrent
    bits: AtomicI32,
    // bit for stored buffer ak
    to_store_bits: AtomicI32,
    next_begin_offset: i64,
    lock_key: CheetahString,
    merge_key: CheetahString,
    just_offset: bool,
    ck_stored: AtomicBool,
}

impl PopCheckPointWrapper {
    pub fn new(
        revive_queue_id: i32,
        revive_queue_offset: i64,
        ck: Arc<PopCheckPoint>,
        next_begin_offset: i64,
    ) -> Self {
        let lock_key = format!(
            "{}{}{}{}{}",
            ck.topic,
            PopAckConstants::SPLIT,
            ck.cid,
            PopAckConstants::SPLIT,
            ck.queue_id
        );
        let merge_key = format!(
            "{}{}{}{}{}{}",
            ck.topic,
            ck.cid,
            ck.queue_id,
            ck.start_offset,
            ck.pop_time,
            ck.broker_name.clone().unwrap_or_default()
        );
        Self {
            revive_queue_id,
            revive_queue_offset: AtomicI32::new(revive_queue_offset as i32),
            ck,
            bits: AtomicI32::new(0),
            to_store_bits: AtomicI32::new(0),
            next_begin_offset,
            lock_key: CheetahString::from(lock_key),
            merge_key: CheetahString::from(merge_key),
            just_offset: false,
            ck_stored: AtomicBool::new(false),
        }
    }

    pub fn new_with_offset(
        revive_queue_id: i32,
        revive_queue_offset: i64,
        ck: Arc<PopCheckPoint>,
        next_begin_offset: i64,
        just_offset: bool,
    ) -> Self {
        let lock_key = format!(
            "{}{}{}{}{}",
            ck.topic,
            PopAckConstants::SPLIT,
            ck.cid,
            PopAckConstants::SPLIT,
            ck.queue_id
        );
        let merge_key = format!(
            "{}{}{}{}{}{}",
            ck.topic,
            ck.cid,
            ck.queue_id,
            ck.start_offset,
            ck.pop_time,
            ck.broker_name.clone().unwrap_or_default()
        );
        Self {
            revive_queue_id,
            revive_queue_offset: AtomicI32::new(revive_queue_offset as i32),
            ck,
            bits: AtomicI32::new(0),
            to_store_bits: AtomicI32::new(0),
            next_begin_offset,
            lock_key: CheetahString::from(lock_key),
            merge_key: CheetahString::from(merge_key),
            just_offset,
            ck_stored: AtomicBool::new(false),
        }
    }

    pub fn get_revive_queue_id(&self) -> i32 {
        self.revive_queue_id
    }

    pub fn get_revive_queue_offset(&self) -> i64 {
        self.revive_queue_offset.load(Ordering::SeqCst) as i64
    }

    pub fn is_ck_stored(&self) -> bool {
        self.ck_stored.load(Ordering::SeqCst)
    }

    pub fn set_revive_queue_offset(&self, revive_queue_offset: i64) {
        self.revive_queue_offset
            .store(revive_queue_offset as i32, Ordering::SeqCst);
    }

    pub fn get_ck(&self) -> &Arc<PopCheckPoint> {
        &self.ck
    }

    pub fn get_bits(&self) -> &AtomicI32 {
        &self.bits
    }

    pub fn get_to_store_bits(&self) -> &AtomicI32 {
        &self.to_store_bits
    }

    pub fn get_next_begin_offset(&self) -> i64 {
        self.next_begin_offset
    }

    pub fn get_lock_key(&self) -> &str {
        &self.lock_key
    }

    pub fn get_merge_key(&self) -> &str {
        &self.merge_key
    }

    pub fn is_just_offset(&self) -> bool {
        self.just_offset
    }

    pub fn set_ck_stored(&self, ck_stored: bool) {
        self.ck_stored.store(ck_stored, Ordering::SeqCst);
    }
}

impl std::fmt::Display for PopCheckPointWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CkWrap{{rq={}, rqo={}, ck={:?}, bits={}, sBits={}, nbo={}, cks={}, jo={}}}",
            self.revive_queue_id,
            self.revive_queue_offset.load(Ordering::Relaxed),
            self.ck,
            self.bits.load(Ordering::Relaxed),
            self.to_store_bits.load(Ordering::Relaxed),
            self.next_begin_offset,
            self.ck_stored.load(Ordering::Relaxed),
            self.just_offset
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocketmq_store::pop::pop_check_point::PopCheckPoint;

    use super::*;

    #[test]
    fn new_creates_instance_correctly() {
        let ck = Arc::new(PopCheckPoint::default());
        let wrapper = PopCheckPointWrapper::new(1, 100, ck.clone(), 200);
        assert_eq!(wrapper.get_revive_queue_id(), 1);
        assert_eq!(wrapper.get_revive_queue_offset(), 100);
        assert_eq!(wrapper.get_next_begin_offset(), 200);
        assert_eq!(wrapper.get_ck(), &ck);
    }

    #[test]
    fn new_with_offset_creates_instance_correctly() {
        let ck = Arc::new(PopCheckPoint::default());
        let wrapper = PopCheckPointWrapper::new_with_offset(1, 100, ck.clone(), 200, true);
        assert_eq!(wrapper.get_revive_queue_id(), 1);
        assert_eq!(wrapper.get_revive_queue_offset(), 100);
        assert_eq!(wrapper.get_next_begin_offset(), 200);
        assert_eq!(wrapper.get_ck(), &ck);
        assert!(wrapper.is_just_offset());
    }

    #[test]
    fn set_revive_queue_offset_updates_offset() {
        let ck = Arc::new(PopCheckPoint::default());
        let wrapper = PopCheckPointWrapper::new(1, 100, ck, 200);
        wrapper.set_revive_queue_offset(300);
        assert_eq!(wrapper.get_revive_queue_offset(), 300);
    }

    #[test]
    fn set_ck_stored_updates_flag() {
        let ck = Arc::new(PopCheckPoint::default());
        let wrapper = PopCheckPointWrapper::new(1, 100, ck, 200);
        wrapper.set_ck_stored(true);
        assert!(wrapper.is_ck_stored());
    }

    #[test]
    fn display_formats_correctly() {
        let ck = Arc::new(PopCheckPoint::default());
        let wrapper = PopCheckPointWrapper::new(1, 100, ck, 200);
        let display = format!("{}", wrapper);
        assert!(display.contains("CkWrap{rq=1, rqo=100"));
    }

    #[test]
    fn queue_with_time_initializes_correctly() {
        let queue_with_time: QueueWithTime<i32> = QueueWithTime::new();
        assert!(queue_with_time.get_queue().is_empty());
        assert!(queue_with_time.get_time() > 0);
    }

    #[test]
    fn set_time_updates_time() {
        let mut queue_with_time: QueueWithTime<i32> = QueueWithTime::new();
        queue_with_time.set_time(123456789);
        assert_eq!(queue_with_time.get_time(), 123456789);
    }

    #[test]
    fn get_queue_mut_returns_mutable_reference() {
        let mut queue_with_time: QueueWithTime<i32> = QueueWithTime::new();
        queue_with_time.get_queue_mut().push_back(1);
        assert_eq!(queue_with_time.get_queue().len(), 1);
    }
}
