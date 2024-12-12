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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use rocketmq_common::common::pop_ack_constants::PopAckConstants;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_store::pop::ack_msg::AckMsg;
use rocketmq_store::pop::batch_ack_msg::BatchAckMsg;
use rocketmq_store::pop::pop_check_point::PopCheckPoint;

#[derive(Default)]
pub struct PopMessageProcessor {}

impl PopMessageProcessor {
    pub async fn process_request(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: RemotingCommand,
    ) -> crate::Result<Option<RemotingCommand>> {
        unimplemented!("PopMessageProcessor process_request")
    }
}

impl PopMessageProcessor {
    pub fn gen_ack_unique_id(ack_msg: &AckMsg) -> String {
        format!(
            "{}{}{}{}{}{}{}{}{}{}{}{}{}",
            ack_msg.topic,
            PopAckConstants::SPLIT,
            ack_msg.queue_id,
            PopAckConstants::SPLIT,
            ack_msg.ack_offset,
            PopAckConstants::SPLIT,
            ack_msg.consumer_group,
            PopAckConstants::SPLIT,
            ack_msg.pop_time,
            PopAckConstants::SPLIT,
            ack_msg.broker_name,
            PopAckConstants::SPLIT,
            PopAckConstants::ACK_TAG
        )
    }

    pub fn gen_batch_ack_unique_id(batch_ack_msg: &BatchAckMsg) -> String {
        format!(
            "{}{}{}{}{:?}{}{}{}{}{}{}",
            batch_ack_msg.ack_msg.topic,
            PopAckConstants::SPLIT,
            batch_ack_msg.ack_msg.queue_id,
            PopAckConstants::SPLIT,
            batch_ack_msg.ack_offset_list,
            PopAckConstants::SPLIT,
            batch_ack_msg.ack_msg.consumer_group,
            PopAckConstants::SPLIT,
            batch_ack_msg.ack_msg.pop_time,
            PopAckConstants::SPLIT,
            PopAckConstants::BATCH_ACK_TAG
        )
    }

    pub fn gen_ck_unique_id(ck: &PopCheckPoint) -> String {
        format!(
            "{}{}{}{}{}{}{}{}{}{}{}{}{}",
            ck.topic,
            PopAckConstants::SPLIT,
            ck.queue_id,
            PopAckConstants::SPLIT,
            ck.start_offset,
            PopAckConstants::SPLIT,
            ck.cid,
            PopAckConstants::SPLIT,
            ck.pop_time,
            PopAckConstants::SPLIT,
            ck.broker_name
                .as_ref()
                .map_or("null".to_string(), |x| x.to_string()),
            PopAckConstants::SPLIT,
            PopAckConstants::CK_TAG
        )
    }
}

struct TimedLock {
    lock: AtomicBool,
    lock_time: AtomicU64,
}

impl TimedLock {
    pub fn new() -> Self {
        TimedLock {
            lock: AtomicBool::new(true),
            lock_time: AtomicU64::new(get_current_millis()),
        }
    }

    pub fn try_lock(&self) -> bool {
        match self
            .lock
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
        {
            Ok(_) => {
                self.lock_time
                    .store(get_current_millis(), Ordering::Relaxed);
                true
            }
            Err(_) => false,
        }
    }

    pub fn unlock(&self) {
        self.lock.store(true, Ordering::Release);
    }

    pub fn is_locked(&self) -> bool {
        !self.lock.load(Ordering::Acquire)
    }

    pub fn get_lock_time(&self) -> u64 {
        self.lock_time.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn gen_ack_unique_id_formats_correctly() {
        let ack_msg = AckMsg {
            ack_offset: 123,
            start_offset: 456,
            consumer_group: CheetahString::from_static_str("test_group"),
            topic: CheetahString::from_static_str("test_topic"),
            queue_id: 1,
            pop_time: 789,
            broker_name: CheetahString::from_static_str("test_broker"),
        };
        let result = PopMessageProcessor::gen_ack_unique_id(&ack_msg);
        let expected = "test_topic@1@123@test_group@789@test_broker@ack";
        assert_eq!(result, expected);
    }

    #[test]
    fn gen_batch_ack_unique_id_formats_correctly() {
        let ack_msg = AckMsg {
            ack_offset: 123,
            start_offset: 456,
            consumer_group: CheetahString::from_static_str("test_group"),
            topic: CheetahString::from_static_str("test_topic"),
            queue_id: 1,
            pop_time: 789,
            broker_name: CheetahString::from_static_str("test_broker"),
        };
        let batch_ack_msg = BatchAckMsg {
            ack_msg,
            ack_offset_list: vec![1, 2, 3],
        };
        let result = PopMessageProcessor::gen_batch_ack_unique_id(&batch_ack_msg);
        let expected = "test_topic@1@[1, 2, 3]@test_group@789@bAck";
        assert_eq!(result, expected);
    }

    #[test]
    fn gen_ck_unique_id_formats_correctly() {
        let ck = PopCheckPoint {
            topic: CheetahString::from("test_topic"),
            queue_id: 1,
            start_offset: 456,
            cid: CheetahString::from("test_cid"),
            revive_offset: 0,
            pop_time: 789,
            invisible_time: 0,
            bit_map: 0,
            broker_name: Some(CheetahString::from("test_broker")),
            num: 0,
            queue_offset_diff: vec![],
            re_put_times: None,
        };
        let result = PopMessageProcessor::gen_ck_unique_id(&ck);
        let expected = "test_topic@1@456@test_cid@789@test_broker@ck";
        assert_eq!(result, expected);
    }

    #[test]
    fn new_timed_lock_is_unlocked() {
        let lock = TimedLock::new();
        assert!(!lock.is_locked());
    }

    #[test]
    fn try_lock_locks_successfully() {
        let lock = TimedLock::new();
        assert!(lock.try_lock());
        assert!(lock.is_locked());
    }

    #[test]
    fn try_lock_fails_when_already_locked() {
        let lock = TimedLock::new();
        lock.try_lock();
        assert!(!lock.try_lock());
    }

    #[test]
    fn unlock_unlocks_successfully() {
        let lock = TimedLock::new();
        lock.try_lock();
        lock.unlock();
        assert!(!lock.is_locked());
    }

    #[test]
    fn get_lock_time_returns_correct_time() {
        let lock = TimedLock::new();
        let initial_time = lock.get_lock_time();
        lock.try_lock();
        let lock_time = lock.get_lock_time();
        assert!(lock_time >= initial_time);
    }
}
