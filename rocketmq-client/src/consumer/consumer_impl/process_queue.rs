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
use std::time::Instant;

use cheetah_string::CheetahString;
use once_cell::sync::Lazy;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::body::process_queue_info::ProcessQueueInfo;
use rocketmq_rust::ArcMut;
use rocketmq_rust::RocketMQTokioRwLock;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::PULL_MAX_IDLE_TIME;

pub static REBALANCE_LOCK_MAX_LIVE_TIME: Lazy<u64> = Lazy::new(|| {
    std::env::var("rocketmq.client.rebalance.lockMaxLiveTime")
        .unwrap_or_else(|_| "30000".into())
        .parse()
        .unwrap_or(30000)
});

pub static REBALANCE_LOCK_INTERVAL: Lazy<u64> = Lazy::new(|| {
    std::env::var("rocketmq.client.rebalance.lockInterval")
        .unwrap_or_else(|_| "20000".into())
        .parse()
        .unwrap_or(20000)
});

#[derive(Clone)]
pub(crate) struct ProcessQueue {
    pub(crate) tree_map_lock: Arc<RwLock<()>>,
    pub(crate) msg_tree_map: Arc<RwLock<std::collections::BTreeMap<i64, ArcMut<MessageExt>>>>,
    pub(crate) msg_count: Arc<AtomicU64>,
    pub(crate) msg_size: Arc<AtomicU64>,
    pub(crate) consume_lock: Arc<RocketMQTokioRwLock<()>>,
    pub(crate) consuming_msg_orderly_tree_map: Arc<RwLock<std::collections::BTreeMap<i64, ArcMut<MessageExt>>>>,
    pub(crate) try_unlock_times: Arc<AtomicI64>,
    pub(crate) queue_offset_max: Arc<AtomicU64>,
    pub(crate) dropped: Arc<AtomicBool>,
    pub(crate) last_pull_timestamp: Arc<AtomicU64>,
    pub(crate) last_consume_timestamp: Arc<AtomicU64>,
    pub(crate) locked: Arc<AtomicBool>,
    pub(crate) last_lock_timestamp: Arc<AtomicU64>,
    pub(crate) consuming: Arc<AtomicBool>,
    pub(crate) msg_acc_cnt: Arc<AtomicI64>,
    /// Serializes per-queue commit operations so that concurrent chunk tasks
    /// cannot interleave offset advancement or ProcessQueue removal.
    pub(crate) commit_lock: Arc<Mutex<()>>,
}

impl ProcessQueue {
    pub(crate) fn new() -> Self {
        ProcessQueue {
            tree_map_lock: Arc::new(RwLock::new(())),
            msg_tree_map: Arc::new(RwLock::new(std::collections::BTreeMap::new())),
            msg_count: Arc::new(AtomicU64::new(0)),
            msg_size: Arc::new(AtomicU64::new(0)),
            consume_lock: Arc::new(RocketMQTokioRwLock::new(())),
            consuming_msg_orderly_tree_map: Arc::new(RwLock::new(std::collections::BTreeMap::new())),
            try_unlock_times: Arc::new(AtomicI64::new(0)),
            queue_offset_max: Arc::new(AtomicU64::new(0)),
            dropped: Arc::new(AtomicBool::new(false)),
            last_pull_timestamp: Arc::new(AtomicU64::new(get_current_millis())),
            last_consume_timestamp: Arc::new(AtomicU64::new(get_current_millis())),
            locked: Arc::new(AtomicBool::new(false)),
            last_lock_timestamp: Arc::new(AtomicU64::new(get_current_millis())),
            consuming: Arc::new(AtomicBool::new(false)),
            msg_acc_cnt: Arc::new(AtomicI64::new(0)),
            commit_lock: Arc::new(Mutex::new(())),
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
                    .store(message.queue_offset as u64, std::sync::atomic::Ordering::Release);
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
        let mut result = -1;
        let mut msg_tree_map = self.msg_tree_map.write().await;
        if msg_tree_map.is_empty() {
            return result;
        }
        result = self.queue_offset_max.load(Ordering::Acquire) as i64 + 1;
        let mut removed_cnt: u64 = 0;
        for message in messages {
            let prev = msg_tree_map.remove(&message.queue_offset);
            if prev.is_some() {
                removed_cnt += 1;
                self.msg_size
                    .fetch_sub(message.body().as_ref().unwrap().len() as u64, Ordering::AcqRel);
            }
        }
        if removed_cnt > 0 {
            self.msg_count.fetch_sub(removed_cnt, Ordering::AcqRel);
        }
        if self.msg_count.load(Ordering::Acquire) == 0 {
            self.msg_size.store(0, Ordering::Release);
        }
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
        self.msg_count
            .fetch_sub(consuming_msg_orderly_tree_map.len() as u64, Ordering::AcqRel);
        if self.msg_count.load(Ordering::Acquire) == 0 {
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
        let now = Instant::now();
        let mut msg_tree_map = self.msg_tree_map.write().await;
        if !msg_tree_map.is_empty() {
            for _ in 0..batch_size {
                if let Some((_, message)) = msg_tree_map.pop_first() {
                    messages.push(message);
                } else {
                    break;
                }
            }
        }
        messages
    }

    pub(crate) async fn contains_message(&self, message_ext: &MessageExt) -> bool {
        let msg_tree_map = self.msg_tree_map.read().await;
        msg_tree_map.contains_key(&message_ext.queue_offset)
    }

    pub(crate) async fn clear(&self) {
        let lock = self.tree_map_lock.write().await;
        self.msg_tree_map.write().await.clear();
        self.consuming_msg_orderly_tree_map.write().await.clear();
        self.msg_count.store(0, Ordering::Release);
        self.msg_size.store(0, Ordering::Release);
        self.queue_offset_max.store(0, Ordering::Release);
        drop(lock);
    }

    pub(crate) fn fill_process_queue_info(&self, info: ProcessQueueInfo) {
        unimplemented!("fill_process_queue_info")
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
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rocketmq_common::common::message::message_ext::MessageExt;
    use rocketmq_rust::ArcMut;

    use super::ProcessQueue;

    fn make_msg(offset: i64, body_size: usize) -> ArcMut<MessageExt> {
        let mut msg = MessageExt::default();
        msg.queue_offset = offset;
        msg.message.body = Some(Bytes::from(vec![0u8; body_size]));
        ArcMut::new(msg)
    }

    async fn put_msgs(pq: &ProcessQueue, offsets: &[i64], body_size: usize) {
        let msgs = offsets.iter().map(|&o| make_msg(o, body_size)).collect();
        pq.put_message(msgs).await;
    }

    #[tokio::test]
    async fn test_remove_single_message_correct_count() {
        let pq = ProcessQueue::new();
        put_msgs(&pq, &[0], 10).await;
        assert_eq!(pq.msg_count(), 1);
        assert_eq!(pq.msg_size(), 10);

        let msgs = vec![make_msg(0, 10)];
        pq.remove_message(&msgs).await;

        assert_eq!(pq.msg_count(), 0);
        assert_eq!(pq.msg_size(), 0);
    }

    #[tokio::test]
    async fn test_remove_two_messages_correct_count() {
        let pq = ProcessQueue::new();
        put_msgs(&pq, &[0, 1], 10).await;
        assert_eq!(pq.msg_count(), 2);

        let msgs = vec![make_msg(0, 10), make_msg(1, 10)];
        pq.remove_message(&msgs).await;

        assert_eq!(pq.msg_count(), 0);
        assert_eq!(pq.msg_size(), 0);
    }

    #[tokio::test]
    async fn test_remove_16_messages_correct_count() {
        let pq = ProcessQueue::new();
        let offsets: Vec<i64> = (0..16).collect();
        put_msgs(&pq, &offsets, 5).await;
        assert_eq!(pq.msg_count(), 16);
        assert_eq!(pq.msg_size(), 80);

        let msgs: Vec<ArcMut<MessageExt>> = offsets.iter().map(|&o| make_msg(o, 5)).collect();
        pq.remove_message(&msgs).await;

        assert_eq!(pq.msg_count(), 0);
        assert_eq!(pq.msg_size(), 0);
    }

    #[tokio::test]
    async fn test_remove_subset_correct_count() {
        let pq = ProcessQueue::new();
        put_msgs(&pq, &[0, 1, 2, 3, 4], 4).await;
        assert_eq!(pq.msg_count(), 5);

        let to_remove: Vec<ArcMut<MessageExt>> = vec![0, 1, 2].iter().map(|&o| make_msg(o, 4)).collect();
        pq.remove_message(&to_remove).await;

        let tree = pq.msg_tree_map.read().await;
        assert_eq!(pq.msg_count(), 2, "count should match retained map size");
        assert_eq!(tree.len(), 2);
    }

    #[tokio::test]
    async fn test_msg_count_no_underflow() {
        let pq = ProcessQueue::new();
        put_msgs(&pq, &[0], 4).await;

        let msgs = vec![make_msg(0, 4), make_msg(99, 4)];
        pq.remove_message(&msgs).await;

        assert_eq!(pq.msg_count(), 0, "count must not underflow");
    }

    #[tokio::test]
    async fn test_partial_remove_offset_is_lowest_retained() {
        let pq = ProcessQueue::new();
        let offsets: Vec<i64> = (0..16).collect();
        put_msgs(&pq, &offsets, 4).await;

        let to_remove: Vec<ArcMut<MessageExt>> = (0..5).map(|o| make_msg(o, 4)).collect();
        let offset = pq.remove_message(&to_remove).await;

        assert_eq!(offset, 5, "next offset should be the lowest retained key");
        assert_eq!(pq.msg_count(), 11);
    }

    #[tokio::test]
    async fn test_full_remove_returns_max_plus_one() {
        let pq = ProcessQueue::new();
        put_msgs(&pq, &[0, 1, 2], 4).await;

        let to_remove: Vec<ArcMut<MessageExt>> = (0..3).map(|o| make_msg(o, 4)).collect();
        let offset = pq.remove_message(&to_remove).await;

        assert_eq!(offset, 3, "when empty, offset should be queue_offset_max + 1");
        assert_eq!(pq.msg_count(), 0);
        assert_eq!(pq.msg_size(), 0);
    }

    #[tokio::test]
    async fn test_commit_lock_serializes_concurrent_removals() {
        use std::sync::Arc;
        let pq = Arc::new(ProcessQueue::new());
        let offsets: Vec<i64> = (0..32).collect();
        put_msgs(&pq, &offsets, 4).await;

        let pq1 = pq.clone();
        let pq2 = pq.clone();

        let first_chunk: Vec<ArcMut<MessageExt>> = (0..16).map(|o| make_msg(o, 4)).collect();
        let second_chunk: Vec<ArcMut<MessageExt>> = (16..32).map(|o| make_msg(o, 4)).collect();

        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let b1 = barrier.clone();
        let b2 = barrier.clone();

        let h1 = tokio::spawn(async move {
            let _g = pq1.commit_lock.lock().await;
            b1.wait().await;
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            pq1.remove_message(&first_chunk).await
        });
        let h2 = tokio::spawn(async move {
            b2.wait().await;
            let _g = pq2.commit_lock.lock().await;
            pq2.remove_message(&second_chunk).await
        });

        let (r1, r2) = tokio::join!(h1, h2);
        let _o1 = r1.unwrap();
        let _o2 = r2.unwrap();

        assert_eq!(pq.msg_count(), 0, "all messages must be removed");
    }
}
