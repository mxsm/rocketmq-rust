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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::body::process_queue_info::ProcessQueueInfo;
use rocketmq_rust::ArcMut;
use rocketmq_rust::RocketMQTokioRwLock;
use std::sync::LazyLock;
use tokio::sync::RwLock;

use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::PULL_MAX_IDLE_TIME;

pub static REBALANCE_LOCK_MAX_LIVE_TIME: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("rocketmq.client.rebalance.lockMaxLiveTime")
        .unwrap_or_else(|_| "30000".into())
        .parse()
        .unwrap_or(30000)
});

pub static REBALANCE_LOCK_INTERVAL: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("rocketmq.client.rebalance.lockInterval")
        .unwrap_or_else(|_| "20000".into())
        .parse()
        .unwrap_or(20000)
});

pub struct ProcessQueue {
    pub(crate) msg_tree_map: Arc<RwLock<std::collections::BTreeMap<i64, ArcMut<MessageExt>>>>,
    pub(crate) consuming_msg_orderly_tree_map: Arc<RwLock<std::collections::BTreeMap<i64, ArcMut<MessageExt>>>>,
    pub(crate) consume_lock: Arc<RocketMQTokioRwLock<()>>,

    msg_count: AtomicU64,
    msg_size: AtomicU64,
    queue_offset_max: AtomicI64,
    msg_acc_cnt: AtomicI64,
    try_unlock_times: AtomicI64,

    dropped: AtomicBool,
    locked: AtomicBool,
    consuming: AtomicBool,

    last_pull_timestamp: AtomicU64,
    last_consume_timestamp: AtomicU64,
    last_lock_timestamp: AtomicU64,
}

impl Default for ProcessQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessQueue {
    pub fn new() -> Self {
        let now = get_current_millis();
        ProcessQueue {
            msg_tree_map: Arc::new(RwLock::new(std::collections::BTreeMap::new())),
            consuming_msg_orderly_tree_map: Arc::new(RwLock::new(std::collections::BTreeMap::new())),
            consume_lock: Arc::new(RocketMQTokioRwLock::new(())),

            msg_count: AtomicU64::new(0),
            msg_size: AtomicU64::new(0),
            queue_offset_max: AtomicI64::new(0),
            msg_acc_cnt: AtomicI64::new(0),
            try_unlock_times: AtomicI64::new(0),

            dropped: AtomicBool::new(false),
            locked: AtomicBool::new(false),
            consuming: AtomicBool::new(false),

            last_pull_timestamp: AtomicU64::new(now),
            last_consume_timestamp: AtomicU64::new(now),
            last_lock_timestamp: AtomicU64::new(now),
        }
    }
}

impl ProcessQueue {
    pub(crate) fn set_dropped(&self, dropped: bool) {
        self.dropped.store(dropped, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn is_dropped(&self) -> bool {
        self.dropped.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn get_last_lock_timestamp(&self) -> u64 {
        self.last_lock_timestamp.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn set_locked(&self, locked: bool) {
        self.locked.store(locked, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn is_pull_expired(&self) -> bool {
        (get_current_millis() - self.last_pull_timestamp.load(Ordering::Acquire)) > *PULL_MAX_IDLE_TIME
    }

    pub(crate) fn is_lock_expired(&self) -> bool {
        (get_current_millis() - self.last_lock_timestamp.load(Ordering::Acquire)) > *REBALANCE_LOCK_MAX_LIVE_TIME
    }

    pub(crate) fn inc_try_unlock_times(&self) {
        self.try_unlock_times.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) async fn clean_expired_msg(&self, push_consumer: Option<ArcMut<DefaultMQPushConsumerImpl>>) {
        if push_consumer.is_none() {
            return;
        }
        let mut push_consumer = push_consumer.unwrap();

        if push_consumer.is_consume_orderly() {
            return;
        }
        let loop_ = 16.min(self.msg_tree_map.read().await.len());
        for _ in 0..loop_ {
            let msg = {
                let msg_tree_map = self.msg_tree_map.read().await;
                if !msg_tree_map.is_empty() {
                    let value = msg_tree_map.first_key_value().unwrap().1;
                    let consume_start_time_stamp = MessageAccessor::get_consume_start_time_stamp(value.as_ref());
                    if let Some(consume_start_time_stamp) = consume_start_time_stamp {
                        if get_current_millis() - consume_start_time_stamp.parse::<u64>().unwrap()
                            > push_consumer.consumer_config.consume_timeout * 1000 * 60
                        {
                            Some(value.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            };
            if msg.is_none() {
                break;
            }

            let mut msg = msg.unwrap();
            let msg_inner = msg.as_mut();
            msg_inner.set_topic(push_consumer.client_config.with_namespace(msg_inner.topic()));
            let _ = push_consumer
                .send_message_back_with_broker_name(msg_inner, 3, None, None)
                .await;
            let msg_tree_map = self.msg_tree_map.write().await;
            if !msg_tree_map.is_empty() && msg.queue_offset == *msg_tree_map.first_key_value().unwrap().0 {
                drop(msg_tree_map);
                self.remove_message(&[msg]).await;
            }
        }
    }

    pub(crate) async fn put_message(&self, messages: Vec<ArcMut<MessageExt>>) -> bool {
        let mut dispatch_to_consume = false;
        let mut msg_tree_map = self.msg_tree_map.write().await;
        let mut valid_msg_cnt = 0;

        let acc_total = if !messages.is_empty() {
            let message_ext = messages.last().unwrap();
            if let Some(property) =
                message_ext.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_MAX_OFFSET))
            {
                property.parse::<i64>().unwrap() - message_ext.queue_offset
            } else {
                0
            }
        } else {
            0
        };

        for message in messages {
            if msg_tree_map.insert(message.queue_offset, message.clone()).is_none() {
                valid_msg_cnt += 1;
                self.queue_offset_max
                    .fetch_max(message.queue_offset, std::sync::atomic::Ordering::AcqRel);
                self.msg_size
                    .fetch_add(message.body().as_ref().unwrap().len() as u64, Ordering::AcqRel);
            }
        }
        self.msg_count.fetch_add(valid_msg_cnt, Ordering::AcqRel);
        if !msg_tree_map.is_empty() && !self.consuming.load(Ordering::Acquire) {
            dispatch_to_consume = true;
            self.consuming.store(true, Ordering::Release);
        }
        if acc_total > 0 {
            self.msg_acc_cnt.store(acc_total, std::sync::atomic::Ordering::Release);
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

    pub(crate) async fn remove_message(&self, messages: &[ArcMut<MessageExt>]) -> i64 {
        let now = get_current_millis();
        let mut msg_tree_map = self.msg_tree_map.write().await;

        if msg_tree_map.is_empty() {
            return -1;
        }

        let mut result = self.queue_offset_max.load(Ordering::Acquire) + 1;
        let mut removed_cnt = 0u64;

        for message in messages {
            if let Some(_prev) = msg_tree_map.remove(&message.queue_offset) {
                removed_cnt += 1;
                if let Some(body) = message.body() {
                    self.msg_size.fetch_sub(body.len() as u64, Ordering::AcqRel);
                }
            }
        }

        if removed_cnt > 0 {
            let current_count = self.msg_count.fetch_sub(removed_cnt, Ordering::AcqRel);
            if current_count == removed_cnt {
                self.msg_size.store(0, Ordering::Release);
            }
        }

        self.last_consume_timestamp.store(now, Ordering::Release);

        if !msg_tree_map.is_empty() {
            result = *msg_tree_map.first_key_value().unwrap().0;
        }

        result
    }

    pub(crate) async fn rollback(&self) {
        let mut msg_tree_map = self.msg_tree_map.write().await;
        let mut consuming_msg_orderly_tree_map = self.consuming_msg_orderly_tree_map.write().await;
        consuming_msg_orderly_tree_map.iter().for_each(|(k, v)| {
            msg_tree_map.insert(*k, v.clone());
        });
        consuming_msg_orderly_tree_map.clear();
    }

    pub(crate) async fn commit(&self) -> i64 {
        let mut consuming_msg_orderly_tree_map = self.consuming_msg_orderly_tree_map.write().await;
        let key_value = consuming_msg_orderly_tree_map.last_key_value();
        let offset = if let Some((key, _)) = key_value { *key + 1 } else { -1 };

        let prev_count = self
            .msg_count
            .fetch_sub(consuming_msg_orderly_tree_map.len() as u64, Ordering::AcqRel);

        if prev_count == consuming_msg_orderly_tree_map.len() as u64 {
            self.msg_size.store(0, Ordering::Release);
        } else {
            for message in consuming_msg_orderly_tree_map.values() {
                self.msg_size
                    .fetch_sub(message.body().as_ref().unwrap().len() as u64, Ordering::AcqRel);
            }
        }
        consuming_msg_orderly_tree_map.clear();
        offset
    }

    pub(crate) async fn make_message_to_consume_again(&self, messages: &[ArcMut<MessageExt>]) {
        let mut consuming_msg_orderly_tree_map = self.consuming_msg_orderly_tree_map.write().await;
        let mut msg_tree_map = self.msg_tree_map.write().await;
        for message in messages {
            consuming_msg_orderly_tree_map.remove(&message.queue_offset);
            msg_tree_map.insert(message.queue_offset, message.clone());
        }
    }

    pub(crate) async fn take_messages(&self, batch_size: u32) -> Vec<ArcMut<MessageExt>> {
        let mut messages = Vec::with_capacity(batch_size as usize);
        let now = get_current_millis();

        let mut msg_tree_map = self.msg_tree_map.write().await;
        let mut consuming_map = self.consuming_msg_orderly_tree_map.write().await;

        self.last_consume_timestamp.store(now, Ordering::Release);

        if !msg_tree_map.is_empty() {
            for _ in 0..batch_size {
                if let Some((offset, message)) = msg_tree_map.pop_first() {
                    consuming_map.insert(offset, message.clone());
                    messages.push(message);
                } else {
                    break;
                }
            }
        }

        if messages.is_empty() {
            self.consuming.store(false, Ordering::Release);
        }

        messages
    }

    pub(crate) async fn contains_message(&self, message_ext: &MessageExt) -> bool {
        let msg_tree_map = self.msg_tree_map.read().await;
        msg_tree_map.contains_key(&message_ext.queue_offset)
    }

    pub(crate) async fn clear(&self) {
        self.msg_tree_map.write().await.clear();
        self.consuming_msg_orderly_tree_map.write().await.clear();
        self.msg_count.store(0, Ordering::Release);
        self.msg_size.store(0, Ordering::Release);
        self.queue_offset_max.store(0, Ordering::Release);
    }

    pub(crate) async fn fill_process_queue_info(&self, info: &mut ProcessQueueInfo) {
        let msg_tree_map = self.msg_tree_map.read().await;
        let consuming_map = self.consuming_msg_orderly_tree_map.read().await;

        if !msg_tree_map.is_empty() {
            if let Some((min_offset, _)) = msg_tree_map.first_key_value() {
                info.cached_msg_min_offset = *min_offset as u64;
            }
            if let Some((max_offset, _)) = msg_tree_map.last_key_value() {
                info.cached_msg_max_offset = *max_offset as u64;
            }
            info.cached_msg_count = msg_tree_map.len() as u32;
        }

        info.cached_msg_size_in_mib = (self.msg_size.load(Ordering::Acquire) / (1024 * 1024)) as u32;

        if !consuming_map.is_empty() {
            if let Some((min_offset, _)) = consuming_map.first_key_value() {
                info.transaction_msg_min_offset = *min_offset as u64;
            }
            if let Some((max_offset, _)) = consuming_map.last_key_value() {
                info.transaction_msg_max_offset = *max_offset as u64;
            }
            info.transaction_msg_count = consuming_map.len() as u32;
        }

        info.locked = self.locked.load(Ordering::Acquire);
        info.try_unlock_times = self.try_unlock_times.load(Ordering::Acquire) as u64;
        info.last_lock_timestamp = self.last_lock_timestamp.load(Ordering::Acquire);
        info.droped = self.dropped.load(Ordering::Acquire);
        info.last_pull_timestamp = self.last_pull_timestamp.load(Ordering::Acquire);
        info.last_consume_timestamp = self.last_consume_timestamp.load(Ordering::Acquire);
    }

    pub(crate) fn set_last_pull_timestamp(&self, last_pull_timestamp: u64) {
        self.last_pull_timestamp
            .store(last_pull_timestamp, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn set_last_lock_timestamp(&self, last_lock_timestamp: u64) {
        self.last_lock_timestamp
            .store(last_lock_timestamp, std::sync::atomic::Ordering::Release);
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

    pub(crate) async fn has_temp_message(&self) -> bool {
        let msg_tree_map = self.msg_tree_map.read().await;
        !msg_tree_map.is_empty()
    }

    pub(crate) fn get_last_pull_timestamp(&self) -> u64 {
        self.last_pull_timestamp.load(Ordering::Acquire)
    }

    pub(crate) fn get_last_consume_timestamp(&self) -> u64 {
        self.last_consume_timestamp.load(Ordering::Acquire)
    }

    pub(crate) fn get_msg_acc_cnt(&self) -> i64 {
        self.msg_acc_cnt.load(Ordering::Acquire)
    }

    pub(crate) fn get_try_unlock_times(&self) -> i64 {
        self.try_unlock_times.load(Ordering::Acquire)
    }

    pub(crate) fn is_consuming(&self) -> bool {
        self.consuming.load(Ordering::Acquire)
    }

    pub(crate) fn set_consuming(&self, consuming: bool) {
        self.consuming.store(consuming, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::MessageTrait;

    fn create_test_messages(count: usize) -> Vec<ArcMut<MessageExt>> {
        let mut messages = Vec::with_capacity(count);
        for i in 0..count {
            let mut msg = MessageExt {
                queue_offset: i as i64,
                ..Default::default()
            };
            msg.set_body(Bytes::from(vec![0u8; 100]));
            msg.set_topic(CheetahString::from_static_str("test_topic"));
            messages.push(ArcMut::new(msg));
        }
        messages
    }

    #[tokio::test]
    async fn test_remove_message_count_correct() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(10);
        pq.put_message(msgs.clone()).await;

        assert_eq!(pq.msg_count(), 10);

        pq.remove_message(&msgs[0..5]).await;

        assert_eq!(pq.msg_count(), 5);
        assert_eq!(pq.msg_size(), 500);
    }

    #[tokio::test]
    async fn test_remove_message_all() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(5);
        pq.put_message(msgs.clone()).await;

        assert_eq!(pq.msg_count(), 5);

        pq.remove_message(&msgs).await;

        assert_eq!(pq.msg_count(), 0);
        assert_eq!(pq.msg_size(), 0);
    }

    #[tokio::test]
    async fn test_take_messages_updates_consuming_map() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(10);
        pq.put_message(msgs).await;

        let taken = pq.take_messages(5).await;

        assert_eq!(taken.len(), 5);

        let consuming_map = pq.consuming_msg_orderly_tree_map.read().await;
        assert_eq!(consuming_map.len(), 5);

        for msg in &taken {
            assert!(consuming_map.contains_key(&msg.queue_offset));
        }
    }

    #[tokio::test]
    async fn test_take_messages_updates_timestamp() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(5);
        pq.put_message(msgs).await;

        let timestamp_before = pq.last_consume_timestamp.load(Ordering::Acquire);

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        pq.take_messages(3).await;

        let timestamp_after = pq.last_consume_timestamp.load(Ordering::Acquire);

        assert!(timestamp_after > timestamp_before);
    }

    #[tokio::test]
    async fn test_take_messages_empty_sets_consuming_false() {
        let pq = ProcessQueue::new();

        pq.consuming.store(true, Ordering::Release);

        let taken = pq.take_messages(5).await;

        assert_eq!(taken.len(), 0);
        assert!(!pq.consuming.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_fill_process_queue_info() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(10);
        pq.put_message(msgs).await;

        pq.take_messages(3).await;

        let mut info = ProcessQueueInfo {
            commit_offset: 0,
            cached_msg_min_offset: 0,
            cached_msg_max_offset: 0,
            cached_msg_count: 0,
            cached_msg_size_in_mib: 0,
            transaction_msg_min_offset: 0,
            transaction_msg_max_offset: 0,
            transaction_msg_count: 0,
            locked: false,
            try_unlock_times: 0,
            last_lock_timestamp: 0,
            droped: false,
            last_pull_timestamp: 0,
            last_consume_timestamp: 0,
        };

        pq.fill_process_queue_info(&mut info).await;

        assert_eq!(info.cached_msg_count, 7);
        assert_eq!(info.cached_msg_min_offset, 3);
        assert_eq!(info.cached_msg_max_offset, 9);

        assert_eq!(info.transaction_msg_count, 3);
        assert_eq!(info.transaction_msg_min_offset, 0);
        assert_eq!(info.transaction_msg_max_offset, 2);
    }

    #[tokio::test]
    async fn test_has_temp_message() {
        let pq = ProcessQueue::new();

        assert!(!pq.has_temp_message().await);

        let msgs = create_test_messages(5);
        pq.put_message(msgs.clone()).await;

        assert!(pq.has_temp_message().await);

        pq.remove_message(&msgs).await;

        assert!(!pq.has_temp_message().await);
    }

    #[tokio::test]
    async fn test_rollback_after_take_messages() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(10);
        pq.put_message(msgs).await;

        assert_eq!(pq.msg_count(), 10);

        let taken = pq.take_messages(5).await;
        assert_eq!(taken.len(), 5);

        let msg_tree_map_len = pq.msg_tree_map.read().await.len();
        assert_eq!(msg_tree_map_len, 5);

        pq.rollback().await;

        let msg_tree_map_len_after = pq.msg_tree_map.read().await.len();
        assert_eq!(msg_tree_map_len_after, 10);

        let consuming_map_len = pq.consuming_msg_orderly_tree_map.read().await.len();
        assert_eq!(consuming_map_len, 0);
    }

    #[tokio::test]
    async fn test_commit_after_take_messages() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(10);
        pq.put_message(msgs).await;

        pq.take_messages(5).await;

        assert_eq!(pq.msg_count(), 10);

        let offset = pq.commit().await;

        assert_eq!(offset, 5);
        assert_eq!(pq.msg_count(), 5);

        let consuming_map_len = pq.consuming_msg_orderly_tree_map.read().await.len();
        assert_eq!(consuming_map_len, 0);
    }

    #[tokio::test]
    async fn test_getter_methods() {
        let pq = ProcessQueue::new();

        assert_eq!(pq.get_try_unlock_times(), 0);
        assert!(!pq.is_consuming());
        assert!(!pq.is_dropped());
        assert!(!pq.is_locked());

        pq.inc_try_unlock_times();
        assert_eq!(pq.get_try_unlock_times(), 1);

        pq.set_consuming(true);
        assert!(pq.is_consuming());

        pq.set_dropped(true);
        assert!(pq.is_dropped());

        pq.set_locked(true);
        assert!(pq.is_locked());
    }

    #[tokio::test]
    async fn test_timestamp_getters() {
        let pq = ProcessQueue::new();

        let pull_ts = pq.get_last_pull_timestamp();
        let consume_ts = pq.get_last_consume_timestamp();
        let lock_ts = pq.get_last_lock_timestamp();

        assert!(pull_ts > 0);
        assert!(consume_ts > 0);
        assert!(lock_ts > 0);

        let new_ts = get_current_millis() + 1000;
        pq.set_last_pull_timestamp(new_ts);
        assert_eq!(pq.get_last_pull_timestamp(), new_ts);

        pq.set_last_lock_timestamp(new_ts);
        assert_eq!(pq.get_last_lock_timestamp(), new_ts);
    }

    #[tokio::test]
    async fn test_msg_acc_cnt() {
        let pq = ProcessQueue::new();

        assert_eq!(pq.get_msg_acc_cnt(), 0);

        let mut msgs = Vec::new();
        for i in 0..5 {
            let mut msg = MessageExt {
                queue_offset: i as i64,
                ..Default::default()
            };
            msg.set_body(Bytes::from(vec![0u8; 100]));
            msg.set_topic(CheetahString::from_static_str("test_topic"));
            msg.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_MAX_OFFSET),
                CheetahString::from(format!("{}", i + 100)),
            );
            msgs.push(ArcMut::new(msg));
        }

        pq.put_message(msgs).await;

        assert!(pq.get_msg_acc_cnt() > 0);
    }

    #[tokio::test]
    async fn test_clear_resets_all_state() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(10);
        pq.put_message(msgs).await;
        pq.take_messages(5).await;

        assert_eq!(pq.msg_count(), 10);
        assert!(pq.msg_size() > 0);

        pq.clear().await;

        assert_eq!(pq.msg_count(), 0);
        assert_eq!(pq.msg_size(), 0);

        let msg_tree_map_len = pq.msg_tree_map.read().await.len();
        assert_eq!(msg_tree_map_len, 0);

        let consuming_map_len = pq.consuming_msg_orderly_tree_map.read().await.len();
        assert_eq!(consuming_map_len, 0);
    }

    #[tokio::test]
    async fn test_get_max_span() {
        let pq = ProcessQueue::new();

        assert_eq!(pq.get_max_span().await, 0);

        let msgs = create_test_messages(10);
        pq.put_message(msgs).await;

        let span = pq.get_max_span().await;
        assert_eq!(span, 9);
    }

    #[tokio::test]
    async fn test_consuming_flag_behavior() {
        let pq = ProcessQueue::new();

        assert!(!pq.is_consuming());

        let msgs = create_test_messages(5);
        let should_dispatch = pq.put_message(msgs).await;

        assert!(should_dispatch);
        assert!(pq.is_consuming());

        pq.set_consuming(false);
        assert!(!pq.is_consuming());
    }

    #[tokio::test]
    async fn test_queue_offset_max_with_unordered_messages() {
        let pq = ProcessQueue::new();

        let mut msgs = Vec::new();
        for offset in [5, 2, 8, 1, 9, 3].iter() {
            let mut msg = MessageExt {
                queue_offset: *offset,
                ..Default::default()
            };
            msg.set_body(Bytes::from(vec![0u8; 100]));
            msg.set_topic(CheetahString::from_static_str("test_topic"));
            msgs.push(ArcMut::new(msg));
        }

        pq.put_message(msgs).await;

        let max_offset = pq.queue_offset_max.load(Ordering::Acquire);
        assert_eq!(max_offset, 9);
    }

    #[tokio::test]
    async fn test_commit_preserves_msg_size_correctly() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(10);
        pq.put_message(msgs).await;

        assert_eq!(pq.msg_size(), 1000);

        pq.take_messages(3).await;

        assert_eq!(pq.msg_count(), 10);
        assert_eq!(pq.msg_size(), 1000);

        pq.commit().await;

        assert_eq!(pq.msg_count(), 7);
        assert_eq!(pq.msg_size(), 700);
    }

    #[tokio::test]
    async fn test_commit_when_all_messages_consumed() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(5);
        pq.put_message(msgs).await;

        assert_eq!(pq.msg_size(), 500);

        pq.take_messages(5).await;

        pq.commit().await;

        assert_eq!(pq.msg_count(), 0);
        assert_eq!(pq.msg_size(), 0);
    }
}
