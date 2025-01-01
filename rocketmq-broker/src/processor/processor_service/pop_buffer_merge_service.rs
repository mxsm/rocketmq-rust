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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_common::common::pop_ack_constants::PopAckConstants;
use rocketmq_store::pop::pop_check_point::PopCheckPoint;
use rocketmq_store::pop::AckMessage;

pub(crate) struct PopBufferMergeService;

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
    lock_key: String,
    merge_key: String,
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
            lock_key,
            merge_key,
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
            lock_key,
            merge_key,
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
}
