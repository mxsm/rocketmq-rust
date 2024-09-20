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

use once_cell::sync::Lazy;
use rocketmq_common::common::message::message_client_ext::MessageClientExt;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::ArcRefCellWrapper;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::body::process_queue_info::ProcessQueueInfo;
use tokio::sync::RwLock;

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
    pub(crate) tree_map_lock: Arc<RwLock<()>>,
    pub(crate) msg_tree_map:
        Arc<RwLock<std::collections::BTreeMap<i64, ArcRefCellWrapper<MessageClientExt>>>>,
    pub(crate) msg_count: Arc<AtomicU64>,
    pub(crate) msg_size: Arc<AtomicU64>,
    pub(crate) consume_lock: Arc<RwLock<()>>,
    pub(crate) consuming_msg_orderly_tree_map:
        Arc<RwLock<std::collections::BTreeMap<i64, ArcRefCellWrapper<MessageClientExt>>>>,
    pub(crate) try_unlock_times: Arc<AtomicI64>,
    pub(crate) queue_offset_max: Arc<AtomicU64>,
    pub(crate) dropped: Arc<AtomicBool>,
    pub(crate) last_pull_timestamp: Arc<AtomicU64>,
    pub(crate) last_consume_timestamp: Arc<AtomicU64>,
    pub(crate) locked: Arc<AtomicBool>,
    pub(crate) last_lock_timestamp: Arc<AtomicU64>,
    pub(crate) consuming: Arc<AtomicBool>,
    pub(crate) msg_acc_cnt: Arc<AtomicI64>,
}

impl ProcessQueue {
    pub(crate) fn new() -> Self {
        ProcessQueue {
            tree_map_lock: Arc::new(RwLock::new(())),
            msg_tree_map: Arc::new(RwLock::new(std::collections::BTreeMap::new())),
            msg_count: Arc::new(AtomicU64::new(0)),
            msg_size: Arc::new(AtomicU64::new(0)),
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

    pub(crate) async fn put_message(
        &self,
        messages: Vec<ArcRefCellWrapper<MessageClientExt>>,
    ) -> bool {
        let mut dispatch_to_consume = false;
        let mut msg_tree_map = self.msg_tree_map.write().await;
        let mut valid_msg_cnt = 0;

        let acc_total = if !messages.is_empty() {
            let message_ext = messages.last().unwrap();
            if let Some(property) = message_ext.get_property(MessageConst::PROPERTY_MAX_OFFSET) {
                property.parse::<i64>().unwrap() - message_ext.message_ext_inner.queue_offset
            } else {
                0
            }
        } else {
            0
        };

        for message in messages {
            if msg_tree_map
                .insert(message.message_ext_inner.queue_offset, message.clone())
                .is_none()
            {
                valid_msg_cnt += 1;
                self.queue_offset_max.store(
                    message.message_ext_inner.queue_offset as u64,
                    std::sync::atomic::Ordering::Release,
                );
                self.msg_size.fetch_add(
                    message.message_ext_inner.body().as_ref().unwrap().len() as u64,
                    Ordering::AcqRel,
                );
            }
        }
        self.msg_count.fetch_add(valid_msg_cnt, Ordering::AcqRel);
        if !msg_tree_map.is_empty() && !self.consuming.load(Ordering::Acquire) {
            dispatch_to_consume = true;
            self.consuming.store(true, Ordering::Release);
        }
        if acc_total > 0 {
            self.msg_acc_cnt
                .store(acc_total, std::sync::atomic::Ordering::Release);
        }
        dispatch_to_consume
    }

    pub(crate) async fn get_max_span(&self) -> u64 {
        let msg_tree_map = self.msg_tree_map.read().await;
        if msg_tree_map.is_empty() {
            return 0;
        }
        let first = msg_tree_map.first_key_value().unwrap();
        let last = msg_tree_map.last_key_value().unwrap();
        (last.0 - first.0) as u64
    }

    pub(crate) async fn remove_message(
        &self,
        messages: &[ArcRefCellWrapper<MessageClientExt>],
    ) -> i64 {
        let mut result = -1;
        let mut msg_tree_map = self.msg_tree_map.write().await;
        if msg_tree_map.is_empty() {
            return result;
        }
        result = self.queue_offset_max.load(Ordering::Acquire) as i64 + 1;
        let mut removed_cnt = 0;
        for message in messages {
            let prev = msg_tree_map.remove(&message.message_ext_inner.queue_offset);
            if let Some(prev) = prev {
                removed_cnt += 1;
                self.msg_size.fetch_sub(
                    message.message_ext_inner.body().as_ref().unwrap().len() as u64,
                    Ordering::AcqRel,
                );
            }
            self.msg_count.fetch_sub(removed_cnt, Ordering::AcqRel);
            if self.msg_count.load(Ordering::Acquire) == 0 {
                self.msg_size.store(0, Ordering::Release);
            }
            if !msg_tree_map.is_empty() {
                result = *msg_tree_map.first_key_value().unwrap().0;
            }
        }
        result
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

    pub(crate) fn contains_message(&self, message_ext: &MessageExt) -> bool {
        unimplemented!("contains_message")
    }

    pub(crate) fn clear(&self) {
        unimplemented!("clear")
    }

    pub(crate) fn fill_process_queue_info(&self, info: ProcessQueueInfo) {
        unimplemented!("fill_process_queue_info")
    }

    pub(crate) fn set_last_pull_timestamp(&self, last_pull_timestamp: u64) {
        self.last_pull_timestamp
            .store(last_pull_timestamp, std::sync::atomic::Ordering::Release);
    }

    pub fn msg_count(&self) -> u64 {
        self.msg_count.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn msg_size(&self) -> u64 {
        self.msg_size.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn is_locked(&self) -> bool {
        self.locked.load(std::sync::atomic::Ordering::Acquire)
    }
}
