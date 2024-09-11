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
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;

use once_cell::sync::Lazy;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::body::process_queue_info::ProcessQueueInfo;

use crate::consumer::consumer_impl::PULL_MAX_IDLE_TIME;
use crate::consumer::default_mq_push_consumer::DefaultMQPushConsumer;

static REBALANCE_LOCK_MAX_LIVE_TIME: Lazy<u64> = Lazy::new(|| {
    std::env::var("rocketmq.client.rebalance.lockMaxLiveTime")
        .unwrap_or_else(|_| "30000".into())
        .parse()
        .unwrap_or(30000)
});

static REBALANCE_LOCK_INTERVAL: Lazy<u64> = Lazy::new(|| {
    std::env::var("rocketmq.client.rebalance.lockInterval")
        .unwrap_or_else(|_| "20000".into())
        .parse()
        .unwrap_or(20000)
});

#[derive(Clone)]
pub(crate) struct ProcessQueue {
    tree_map_lock: Arc<RwLock<()>>,
    msg_tree_map: Arc<RwLock<std::collections::BTreeMap<i64, MessageExt>>>,
    msg_count: Arc<AtomicI64>,
    msg_size: Arc<AtomicI64>,
    consume_lock: Arc<RwLock<()>>,
    consuming_msg_orderly_tree_map: Arc<RwLock<std::collections::BTreeMap<i64, MessageExt>>>,
    try_unlock_times: Arc<AtomicI64>,
    queue_offset_max: Arc<AtomicU64>,
    dropped: Arc<AtomicBool>,
    last_pull_timestamp: Arc<AtomicU64>,
    last_consume_timestamp: Arc<AtomicU64>,
    locked: Arc<AtomicBool>,
    last_lock_timestamp: Arc<AtomicU64>,
    consuming: Arc<AtomicBool>,
    msg_acc_cnt: Arc<AtomicI64>,
}

impl ProcessQueue {
    pub(crate) fn new() -> Self {
        ProcessQueue {
            tree_map_lock: Arc::new(RwLock::new(())),
            msg_tree_map: Arc::new(RwLock::new(std::collections::BTreeMap::new())),
            msg_count: Arc::new(AtomicI64::new(0)),
            msg_size: Arc::new(AtomicI64::new(0)),
            consume_lock: Arc::new(RwLock::new(())),
            consuming_msg_orderly_tree_map: Arc::new(
                RwLock::new(std::collections::BTreeMap::new()),
            ),
            try_unlock_times: Arc::new(AtomicI64::new(0)),
            queue_offset_max: Arc::new(AtomicU64::new(0)),
            dropped: Arc::new(AtomicBool::new(false)),
            last_pull_timestamp: Arc::new(AtomicU64::new(get_current_millis())),
            last_consume_timestamp: Arc::new(AtomicU64::new(get_current_millis())),
            locked: Arc::new(AtomicBool::new(false)),
            last_lock_timestamp: Arc::new(AtomicU64::new(get_current_millis())),
            consuming: Arc::new(AtomicBool::new(false)),
            msg_acc_cnt: Arc::new(AtomicI64::new(0)),
        }
    }
}

impl ProcessQueue {
    pub(crate) fn set_dropped(&self, dropped: bool) {
        self.dropped
            .store(dropped, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn is_dropped(&self) -> bool {
        self.dropped.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn get_last_lock_timestamp(&self) -> u64 {
        self.last_lock_timestamp
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn set_locked(&self, locked: bool) {
        self.locked
            .store(locked, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn is_pull_expired(&self) -> bool {
        (get_current_millis() - self.last_pull_timestamp.load(Ordering::Acquire))
            > *PULL_MAX_IDLE_TIME
    }

    pub(crate) fn is_lock_expired(&self) -> bool {
        (get_current_millis() - self.last_lock_timestamp.load(Ordering::Acquire))
            > *REBALANCE_LOCK_MAX_LIVE_TIME
    }

    pub(crate) fn clean_expired_msg(&self, push_consumer: &DefaultMQPushConsumer) {
        unimplemented!("clean_expired_msg")
    }

    pub(crate) fn put_message(&self, messages: Vec<MessageExt>) -> bool {
        unimplemented!("put_message")
    }

    pub(crate) fn get_max_span(&self) -> u64 {
        unimplemented!("get_max_span")
    }

    pub(crate) fn remove_message(&self, messages: Vec<MessageExt>) -> u64 {
        unimplemented!("remove_message")
    }

    pub(crate) fn rollback(&self) {
        unimplemented!("rollback")
    }

    pub(crate) fn commit(&self) -> u64 {
        unimplemented!("commit")
    }

    pub(crate) fn make_message_to_consume_again(&self, messages: Vec<MessageExt>) {
        unimplemented!("make_message_to_consume_again")
    }

    pub(crate) fn take_messages(&self, batch_size: u32) -> Vec<MessageExt> {
        unimplemented!("take_messages")
    }

    pub(crate) fn contains_message(&self, message_ext: MessageExt) -> bool {
        unimplemented!("contains_message")
    }

    pub(crate) fn clear(&self) {
        unimplemented!("clear")
    }

    pub(crate) fn fill_process_queue_info(&self, info: ProcessQueueInfo) {
        unimplemented!("fill_process_queue_info")
    }
}
