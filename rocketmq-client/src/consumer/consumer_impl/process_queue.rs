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

use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::LazyLock;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::body::process_queue_info::ProcessQueueInfo;
use rocketmq_rust::ArcMut;
use serde::Serialize;
use tokio::sync::RwLock;
use tracing::info;

use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::process_queue_store::ProcessQueueMessageStore;
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

struct ProcessQueueStore {
    messages: ProcessQueueMessageStore,
    consuming_msg_orderly_tree_map: BTreeMap<i64, ArcMut<MessageExt>>,
    queue_offset_max: i64,
}

impl ProcessQueueStore {
    fn new() -> Self {
        ProcessQueueStore {
            messages: ProcessQueueMessageStore::new(),
            consuming_msg_orderly_tree_map: BTreeMap::new(),
            queue_offset_max: 0,
        }
    }
}

pub struct ProcessQueue {
    store: RwLock<ProcessQueueStore>,
    pub(crate) consume_lock: Arc<RwLock<()>>,

    msg_count: AtomicI64,
    msg_size: AtomicU64,
    msg_acc_cnt: AtomicI64,
    min_offset_snapshot: AtomicI64,
    max_offset_snapshot: AtomicI64,
    try_unlock_times: AtomicI64,

    dropped: AtomicBool,
    locked: AtomicBool,
    consuming: AtomicBool,

    last_pull_timestamp: AtomicU64,
    last_consume_timestamp: AtomicU64,
    last_lock_timestamp: AtomicU64,
}

#[doc(hidden)]
#[derive(Debug, Clone, Serialize)]
pub struct ProcessQueueOperationProbe {
    pub message_count: usize,
    pub body_size: usize,
    pub dispatch_to_consume: bool,
    pub max_span: u64,
    pub taken_count: usize,
    pub next_offset: i64,
    pub final_msg_count: u64,
    pub final_msg_size: u64,
}

#[doc(hidden)]
pub struct ProcessQueueOperationFixture {
    process_queue: ProcessQueue,
    messages: Vec<ArcMut<MessageExt>>,
    message_count: usize,
    body_size: usize,
    dispatch_to_consume: bool,
}

impl ProcessQueueOperationFixture {
    pub fn new(message_count: usize, body_size: usize) -> Self {
        Self {
            process_queue: ProcessQueue::new(),
            messages: benchmark_messages(message_count, body_size),
            message_count,
            body_size,
            dispatch_to_consume: false,
        }
    }

    pub async fn seeded(message_count: usize, body_size: usize) -> Self {
        let mut fixture = Self::new(message_count, body_size);
        fixture.dispatch_to_consume = fixture.process_queue.put_message(&fixture.messages).await;
        fixture
    }
}

impl Default for ProcessQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessQueue {
    pub fn new() -> Self {
        let now = current_millis();
        ProcessQueue {
            store: RwLock::new(ProcessQueueStore::new()),
            consume_lock: Arc::new(RwLock::new(())),

            msg_count: AtomicI64::new(0),
            msg_size: AtomicU64::new(0),
            msg_acc_cnt: AtomicI64::new(0),
            min_offset_snapshot: AtomicI64::new(-1),
            max_offset_snapshot: AtomicI64::new(-1),
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
        self.dropped.store(dropped, Ordering::Release);
    }

    pub(crate) fn is_dropped(&self) -> bool {
        self.dropped.load(Ordering::Acquire)
    }

    pub(crate) fn get_last_lock_timestamp(&self) -> u64 {
        self.last_lock_timestamp.load(Ordering::Acquire)
    }

    pub(crate) fn set_locked(&self, locked: bool) {
        self.locked.store(locked, Ordering::Release);
    }

    pub(crate) fn is_pull_expired(&self) -> bool {
        (current_millis() - self.last_pull_timestamp.load(Ordering::Acquire)) > *PULL_MAX_IDLE_TIME
    }

    pub(crate) fn is_lock_expired(&self) -> bool {
        (current_millis() - self.last_lock_timestamp.load(Ordering::Acquire)) > *REBALANCE_LOCK_MAX_LIVE_TIME
    }

    pub(crate) fn inc_try_unlock_times(&self) {
        self.try_unlock_times.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) async fn clean_expired_msg(&self, push_consumer: Option<ArcMut<DefaultMQPushConsumerImpl>>) {
        let mut push_consumer = match push_consumer {
            Some(pc) => pc,
            None => return,
        };

        if push_consumer.is_consume_orderly() {
            return;
        }

        let loop_ = 16.min(self.store.read().await.messages.len());
        for _ in 0..loop_ {
            let msg = {
                let store = self.store.read().await;
                if let Some((_, value)) = store.messages.first() {
                    let consume_start_time_stamp = MessageAccessor::get_consume_start_time_stamp(value.as_ref());
                    if let Some(ts_str) = consume_start_time_stamp {
                        if let Ok(ts) = ts_str.parse::<u64>() {
                            if current_millis() - ts > push_consumer.consumer_config.consume_timeout * 1000 * 60 {
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
                } else {
                    None
                }
            };

            let mut msg = match msg {
                Some(m) => m,
                None => break,
            };

            let msg_inner = msg.as_mut();
            let topic = push_consumer.client_config.with_namespace(msg_inner.topic());
            let queue_id = msg_inner.queue_id();
            let msg_id = msg_inner.msg_id().to_string();
            let store_host = format!("{:?}", msg_inner.store_host());
            let queue_offset = msg_inner.queue_offset;
            msg_inner.set_topic(topic);
            let topic_name = msg_inner.topic().to_string();
            let _ = push_consumer
                .send_message_back_with_broker_name(msg_inner, 3, None, None)
                .await;
            info!(
                "send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}",
                topic_name, msg_id, store_host, queue_id, queue_offset
            );

            let should_remove = {
                let store = self.store.read().await;
                store
                    .messages
                    .first_offset()
                    .is_some_and(|offset| queue_offset == offset)
            };
            if should_remove {
                self.remove_message(&[msg]).await;
            }
        }
    }

    pub(crate) async fn put_message(&self, messages: &[ArcMut<MessageExt>]) -> bool {
        let acc_total = if let Some(last_msg) = messages.last() {
            if let Some(property) =
                last_msg.property(&CheetahString::from_static_str(MessageConst::PROPERTY_MAX_OFFSET))
            {
                property.parse::<i64>().unwrap_or(0) - last_msg.queue_offset
            } else {
                0
            }
        } else {
            0
        };

        let mut dispatch_to_consume = false;
        let mut valid_msg_cnt = 0i64;
        let mut added_body_size = 0u64;
        let mut removed_body_size = 0u64;
        let mut inserted_min_offset = i64::MAX;
        let mut inserted_max_offset = i64::MIN;
        {
            let mut store = self.store.write().await;
            store.messages.reserve(messages.len());
            for message in messages {
                let queue_offset = message.queue_offset;
                let body_size = message.body().map_or(0, |body| body.len()) as u64;
                match store.messages.insert(queue_offset, message.clone()) {
                    None => {
                        valid_msg_cnt += 1;
                        added_body_size += body_size;
                        inserted_min_offset = inserted_min_offset.min(queue_offset);
                        inserted_max_offset = inserted_max_offset.max(queue_offset);
                        if queue_offset > store.queue_offset_max {
                            store.queue_offset_max = queue_offset;
                        }
                    }
                    Some(previous) => {
                        added_body_size += body_size;
                        removed_body_size += previous.body().map_or(0, |body| body.len()) as u64;
                    }
                }
            }

            if !store.messages.is_empty() && !self.consuming.load(Ordering::Acquire) {
                dispatch_to_consume = true;
                self.consuming.store(true, Ordering::Release);
            }

            if valid_msg_cnt > 0 {
                self.update_min_offset_snapshot(inserted_min_offset);
                self.update_max_offset_snapshot(inserted_max_offset);
                self.msg_count.fetch_add(valid_msg_cnt, Ordering::AcqRel);
            }
            if added_body_size > 0 {
                self.msg_size.fetch_add(added_body_size, Ordering::AcqRel);
            }
            if removed_body_size > 0 {
                self.msg_size.fetch_sub(removed_body_size, Ordering::AcqRel);
            }
        }

        if acc_total > 0 {
            self.msg_acc_cnt.store(acc_total, Ordering::Release);
        }
        dispatch_to_consume
    }

    pub(crate) async fn get_max_span(&self) -> u64 {
        let min_offset = self.min_offset_snapshot.load(Ordering::Acquire);
        let max_offset = self.max_offset_snapshot.load(Ordering::Acquire);
        if min_offset >= 0 && max_offset >= min_offset {
            (max_offset - min_offset) as u64
        } else {
            0
        }
    }

    pub(crate) async fn remove_message(&self, messages: &[ArcMut<MessageExt>]) -> i64 {
        let now = current_millis();
        let mut store = self.store.write().await;

        self.last_consume_timestamp.store(now, Ordering::Release);

        if store.messages.is_empty() {
            return -1;
        }

        let mut result = store.queue_offset_max + 1;
        let mut removed_cnt = 0i64;
        let mut removed_body_size = 0u64;
        let current_min_offset = self.min_offset_snapshot.load(Ordering::Acquire);
        let current_max_offset = self.max_offset_snapshot.load(Ordering::Acquire);
        let mut snapshot_touched = false;

        for message in messages {
            let queue_offset = message.queue_offset;
            if let Some(removed) = store.messages.remove(&queue_offset) {
                removed_cnt += 1;
                removed_body_size += removed.body().map_or(0, |body| body.len()) as u64;
                if !snapshot_touched {
                    snapshot_touched = queue_offset == current_min_offset || queue_offset == current_max_offset;
                }
            }
        }

        if removed_cnt > 0 {
            if removed_body_size > 0 {
                self.msg_size.fetch_sub(removed_body_size, Ordering::AcqRel);
            }
            let prev = self.msg_count.fetch_sub(removed_cnt, Ordering::AcqRel);
            if prev == removed_cnt {
                self.msg_size.store(0, Ordering::Release);
            }
            if store.messages.is_empty() {
                self.reset_offset_snapshot();
            } else if snapshot_touched {
                self.refresh_offset_snapshot(&store);
            }
        }

        if let Some(first_offset) = store.messages.first_offset() {
            result = first_offset;
        }

        result
    }

    pub(crate) async fn rollback(&self) {
        let mut store = self.store.write().await;
        let mut consuming = std::mem::take(&mut store.consuming_msg_orderly_tree_map);
        store.messages.append_from_btree_map(&mut consuming);
        self.refresh_offset_snapshot(&store);
    }

    pub(crate) async fn commit(&self) -> i64 {
        let mut store = self.store.write().await;
        let offset = store
            .consuming_msg_orderly_tree_map
            .last_key_value()
            .map_or(-1, |(k, _)| *k + 1);

        let consumed_count = store.consuming_msg_orderly_tree_map.len() as i64;
        let prev_count = self.msg_count.fetch_sub(consumed_count, Ordering::AcqRel);

        if prev_count == consumed_count {
            self.msg_size.store(0, Ordering::Release);
        } else {
            for message in store.consuming_msg_orderly_tree_map.values() {
                let body_len = message.body().map_or(0, |b| b.len()) as u64;
                if body_len > 0 {
                    self.msg_size.fetch_sub(body_len, Ordering::AcqRel);
                }
            }
        }
        store.consuming_msg_orderly_tree_map.clear();
        offset
    }

    pub(crate) async fn make_message_to_consume_again(&self, messages: &[ArcMut<MessageExt>]) {
        let mut store = self.store.write().await;
        for message in messages {
            store.consuming_msg_orderly_tree_map.remove(&message.queue_offset);
            store.messages.insert(message.queue_offset, message.clone());
        }
        self.refresh_offset_snapshot(&store);
    }

    pub(crate) async fn take_messages(&self, batch_size: u32) -> Vec<ArcMut<MessageExt>> {
        let mut messages = Vec::with_capacity(batch_size as usize);
        let now = current_millis();
        let mut store = self.store.write().await;

        self.last_consume_timestamp.store(now, Ordering::Release);

        for _ in 0..batch_size {
            if let Some((offset, message)) = store.messages.pop_first() {
                store.consuming_msg_orderly_tree_map.insert(offset, message.clone());
                messages.push(message);
            } else {
                break;
            }
        }

        if messages.is_empty() {
            self.consuming.store(false, Ordering::Release);
            if store.messages.is_empty() {
                self.reset_offset_snapshot();
            }
        } else if let Some(min_offset) = store.messages.first_offset() {
            self.min_offset_snapshot.store(min_offset, Ordering::Release);
        } else {
            self.reset_offset_snapshot();
        }

        messages
    }

    pub(crate) async fn contains_message(&self, message_ext: &MessageExt) -> bool {
        self.store.read().await.messages.contains_key(&message_ext.queue_offset)
    }

    pub(crate) async fn clear(&self) {
        let mut store = self.store.write().await;
        store.messages.clear();
        store.consuming_msg_orderly_tree_map.clear();
        store.queue_offset_max = 0;
        self.msg_count.store(0, Ordering::Release);
        self.msg_size.store(0, Ordering::Release);
        self.msg_acc_cnt.store(0, Ordering::Release);
        self.reset_offset_snapshot();
        self.consuming.store(false, Ordering::Release);
    }

    fn update_min_offset_snapshot(&self, candidate: i64) {
        let current = self.min_offset_snapshot.load(Ordering::Acquire);
        if current < 0 || candidate < current {
            self.min_offset_snapshot.store(candidate, Ordering::Release);
        }
    }

    fn update_max_offset_snapshot(&self, candidate: i64) {
        let current = self.max_offset_snapshot.load(Ordering::Acquire);
        if current < 0 || candidate > current {
            self.max_offset_snapshot.store(candidate, Ordering::Release);
        }
    }

    fn refresh_offset_snapshot(&self, store: &ProcessQueueStore) {
        match store.messages.offset_span() {
            Some((min_offset, max_offset)) => {
                self.min_offset_snapshot.store(min_offset, Ordering::Release);
                self.max_offset_snapshot.store(max_offset, Ordering::Release);
            }
            None => self.reset_offset_snapshot(),
        }
    }

    fn reset_offset_snapshot(&self) {
        self.min_offset_snapshot.store(-1, Ordering::Release);
        self.max_offset_snapshot.store(-1, Ordering::Release);
    }

    pub(crate) async fn fill_process_queue_info(&self, info: &mut ProcessQueueInfo) {
        let store = self.store.read().await;

        if !store.messages.is_empty() {
            if let Some((min_offset, max_offset)) = store.messages.offset_span() {
                info.cached_msg_min_offset = min_offset as u64;
                info.cached_msg_max_offset = max_offset as u64;
            }
            info.cached_msg_count = store.messages.len() as u32;
        }

        info.cached_msg_size_in_mib = (self.msg_size.load(Ordering::Acquire) / (1024 * 1024)) as u32;

        if !store.consuming_msg_orderly_tree_map.is_empty() {
            if let Some((min_offset, _)) = store.consuming_msg_orderly_tree_map.first_key_value() {
                info.transaction_msg_min_offset = *min_offset as u64;
            }
            if let Some((max_offset, _)) = store.consuming_msg_orderly_tree_map.last_key_value() {
                info.transaction_msg_max_offset = *max_offset as u64;
            }
            info.transaction_msg_count = store.consuming_msg_orderly_tree_map.len() as u32;
        }

        info.locked = self.locked.load(Ordering::Acquire);
        info.try_unlock_times = self.try_unlock_times.load(Ordering::Acquire) as u64;
        info.last_lock_timestamp = self.last_lock_timestamp.load(Ordering::Acquire);
        info.droped = self.dropped.load(Ordering::Acquire);
        info.last_pull_timestamp = self.last_pull_timestamp.load(Ordering::Acquire);
        info.last_consume_timestamp = self.last_consume_timestamp.load(Ordering::Acquire);
    }
    /// Returns `Some((min_offset, max_offset))` of the pending message tree, or `None` if empty.
    pub(crate) async fn get_offset_span(&self) -> Option<(i64, i64)> {
        let store = self.store.read().await;
        store.messages.offset_span()
    }

    pub(crate) fn set_last_pull_timestamp(&self, last_pull_timestamp: u64) {
        self.last_pull_timestamp.store(last_pull_timestamp, Ordering::Release);
    }

    pub(crate) fn set_last_lock_timestamp(&self, last_lock_timestamp: u64) {
        self.last_lock_timestamp.store(last_lock_timestamp, Ordering::Release);
    }

    pub fn msg_count(&self) -> u64 {
        self.msg_count.load(Ordering::Acquire).max(0) as u64
    }

    pub(crate) fn msg_size(&self) -> u64 {
        self.msg_size.load(Ordering::Acquire)
    }

    pub(crate) fn is_locked(&self) -> bool {
        self.locked.load(Ordering::Acquire)
    }

    pub(crate) async fn has_temp_message(&self) -> bool {
        if self.msg_count.load(Ordering::Acquire) <= 0 {
            return false;
        }
        !self.store.read().await.messages.is_empty()
    }

    pub(crate) fn get_last_pull_timestamp(&self) -> u64 {
        self.last_pull_timestamp.load(Ordering::Acquire)
    }

    pub(crate) fn get_last_consume_timestamp(&self) -> u64 {
        self.last_consume_timestamp.load(Ordering::Acquire)
    }

    pub(crate) fn update_last_consume_timestamp(&self) {
        self.last_consume_timestamp.store(current_millis(), Ordering::Release);
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

fn benchmark_messages(message_count: usize, body_size: usize) -> Vec<ArcMut<MessageExt>> {
    (0..message_count)
        .map(|index| {
            let message = Message::builder()
                .topic("BenchmarkTopic")
                .body(vec![b'x'; body_size])
                .build()
                .expect("benchmark message should be valid");
            let mut message_ext = MessageExt::default();
            message_ext.set_message_inner(message);
            message_ext.set_broker_name(CheetahString::from_static_str("benchmark-broker"));
            message_ext.set_queue_id(0);
            message_ext.set_queue_offset(index as i64);
            ArcMut::new(message_ext)
        })
        .collect()
}

#[doc(hidden)]
pub async fn run_process_queue_put_probe(fixture: ProcessQueueOperationFixture) -> ProcessQueueOperationProbe {
    let ProcessQueueOperationFixture {
        process_queue,
        messages,
        message_count,
        body_size,
        ..
    } = fixture;
    let dispatch_to_consume = process_queue.put_message(&messages).await;

    ProcessQueueOperationProbe {
        message_count,
        body_size,
        dispatch_to_consume,
        max_span: process_queue.get_max_span().await,
        taken_count: 0,
        next_offset: -1,
        final_msg_count: process_queue.msg_count(),
        final_msg_size: process_queue.msg_size(),
    }
}

#[doc(hidden)]
pub async fn run_process_queue_take_probe(fixture: ProcessQueueOperationFixture) -> ProcessQueueOperationProbe {
    let ProcessQueueOperationFixture {
        process_queue,
        message_count,
        body_size,
        dispatch_to_consume,
        ..
    } = fixture;
    let taken = process_queue.take_messages(message_count as u32).await;

    ProcessQueueOperationProbe {
        message_count,
        body_size,
        dispatch_to_consume,
        max_span: process_queue.get_max_span().await,
        taken_count: taken.len(),
        next_offset: -1,
        final_msg_count: process_queue.msg_count(),
        final_msg_size: process_queue.msg_size(),
    }
}

#[doc(hidden)]
pub async fn run_process_queue_remove_probe(fixture: ProcessQueueOperationFixture) -> ProcessQueueOperationProbe {
    let ProcessQueueOperationFixture {
        process_queue,
        messages,
        message_count,
        body_size,
        dispatch_to_consume,
    } = fixture;
    let next_offset = process_queue.remove_message(&messages).await;

    ProcessQueueOperationProbe {
        message_count,
        body_size,
        dispatch_to_consume,
        max_span: process_queue.get_max_span().await,
        taken_count: 0,
        next_offset,
        final_msg_count: process_queue.msg_count(),
        final_msg_size: process_queue.msg_size(),
    }
}

#[doc(hidden)]
pub async fn run_process_queue_max_span_probe(fixture: ProcessQueueOperationFixture) -> ProcessQueueOperationProbe {
    let ProcessQueueOperationFixture {
        process_queue,
        message_count,
        body_size,
        dispatch_to_consume,
        ..
    } = fixture;
    let max_span = process_queue.get_max_span().await;

    ProcessQueueOperationProbe {
        message_count,
        body_size,
        dispatch_to_consume,
        max_span,
        taken_count: 0,
        next_offset: -1,
        final_msg_count: process_queue.msg_count(),
        final_msg_size: process_queue.msg_size(),
    }
}

#[doc(hidden)]
pub async fn run_process_queue_max_span_only_probe(fixture: &ProcessQueueOperationFixture) -> u64 {
    fixture.process_queue.get_max_span().await
}

#[doc(hidden)]
pub async fn run_process_queue_has_temp_message_probe(fixture: &ProcessQueueOperationFixture) -> bool {
    fixture.process_queue.has_temp_message().await
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

    fn create_test_messages_with_offsets(offsets: &[i64]) -> Vec<ArcMut<MessageExt>> {
        offsets
            .iter()
            .map(|offset| {
                let mut msg = MessageExt {
                    queue_offset: *offset,
                    ..Default::default()
                };
                msg.set_body(Bytes::from(vec![0u8; 100]));
                msg.set_topic(CheetahString::from_static_str("test_topic"));
                ArcMut::new(msg)
            })
            .collect()
    }

    fn create_test_message_with_body_size(offset: i64, body_size: usize) -> ArcMut<MessageExt> {
        let mut msg = MessageExt {
            queue_offset: offset,
            ..Default::default()
        };
        msg.set_body(Bytes::from(vec![0u8; body_size]));
        msg.set_topic(CheetahString::from_static_str("test_topic"));
        ArcMut::new(msg)
    }

    #[tokio::test]
    async fn test_process_queue_operation_probes_report_expected_metrics() {
        let put = run_process_queue_put_probe(ProcessQueueOperationFixture::new(4, 64)).await;
        assert_eq!(put.message_count, 4);
        assert_eq!(put.body_size, 64);
        assert!(put.dispatch_to_consume);
        assert_eq!(put.max_span, 3);
        assert_eq!(put.final_msg_count, 4);
        assert_eq!(put.final_msg_size, 256);

        let taken = run_process_queue_take_probe(ProcessQueueOperationFixture::seeded(4, 64).await).await;
        assert_eq!(taken.taken_count, 4);
        assert_eq!(taken.final_msg_count, 4);

        let removed = run_process_queue_remove_probe(ProcessQueueOperationFixture::seeded(4, 64).await).await;
        assert_eq!(removed.next_offset, 4);
        assert_eq!(removed.final_msg_count, 0);
        assert_eq!(removed.final_msg_size, 0);

        let max_span = run_process_queue_max_span_probe(ProcessQueueOperationFixture::seeded(4, 64).await).await;
        assert_eq!(max_span.max_span, 3);
        assert_eq!(max_span.final_msg_count, 4);

        let read_only_fixture = ProcessQueueOperationFixture::seeded(4, 64).await;
        assert_eq!(run_process_queue_max_span_only_probe(&read_only_fixture).await, 3);
        assert!(run_process_queue_has_temp_message_probe(&read_only_fixture).await);

        let empty_fixture = ProcessQueueOperationFixture::new(0, 64);
        assert!(!run_process_queue_has_temp_message_probe(&empty_fixture).await);
    }

    #[tokio::test]
    async fn test_remove_message_count_correct() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(10);
        pq.put_message(&msgs).await;

        assert_eq!(pq.msg_count(), 10);

        pq.remove_message(&msgs[0..5]).await;

        assert_eq!(pq.msg_count(), 5);
        assert_eq!(pq.msg_size(), 500);
    }

    #[tokio::test]
    async fn test_remove_message_all() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(5);
        pq.put_message(&msgs).await;

        assert_eq!(pq.msg_count(), 5);

        pq.remove_message(&msgs).await;

        assert_eq!(pq.msg_count(), 0);
        assert_eq!(pq.msg_size(), 0);
    }

    #[tokio::test]
    async fn test_clear_resets_consuming_state_for_new_dispatch() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(5);
        assert!(pq.put_message(&msgs).await);
        assert!(pq.is_consuming());

        pq.clear().await;

        assert_eq!(pq.msg_count(), 0);
        assert_eq!(pq.msg_size(), 0);
        assert!(!pq.is_consuming());
        assert!(pq.put_message(&msgs).await);
    }

    #[tokio::test]
    async fn test_take_messages_updates_consuming_map() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(10);
        pq.put_message(&msgs).await;

        let taken = pq.take_messages(5).await;

        assert_eq!(taken.len(), 5);

        let store = pq.store.read().await;
        assert_eq!(store.consuming_msg_orderly_tree_map.len(), 5);

        for msg in &taken {
            assert!(store.consuming_msg_orderly_tree_map.contains_key(&msg.queue_offset));
        }
    }

    #[tokio::test]
    async fn test_take_messages_updates_timestamp() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(5);
        pq.put_message(&msgs).await;

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
    async fn test_take_zero_keeps_offset_snapshot() {
        let pq = ProcessQueue::new();
        let msgs = create_test_messages(4);

        pq.put_message(&msgs).await;
        assert_eq!(pq.get_max_span().await, 3);

        let taken = pq.take_messages(0).await;

        assert!(taken.is_empty());
        assert_eq!(pq.get_max_span().await, 3);
    }

    #[tokio::test]
    async fn test_fill_process_queue_info() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(10);
        pq.put_message(&msgs).await;

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
        pq.put_message(&msgs).await;

        assert!(pq.has_temp_message().await);

        pq.remove_message(&msgs).await;

        assert!(!pq.has_temp_message().await);
    }

    #[tokio::test]
    async fn test_rollback_after_take_messages() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(10);
        pq.put_message(&msgs).await;

        assert_eq!(pq.msg_count(), 10);

        let taken = pq.take_messages(5).await;
        assert_eq!(taken.len(), 5);

        let pending_len = pq.store.read().await.messages.len();
        assert_eq!(pending_len, 5);

        pq.rollback().await;

        let pending_len_after = pq.store.read().await.messages.len();
        assert_eq!(pending_len_after, 10);

        let consuming_map_len = pq.store.read().await.consuming_msg_orderly_tree_map.len();
        assert_eq!(consuming_map_len, 0);
    }

    #[tokio::test]
    async fn test_commit_after_take_messages() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(10);
        pq.put_message(&msgs).await;

        pq.take_messages(5).await;

        assert_eq!(pq.msg_count(), 10);

        let offset = pq.commit().await;

        assert_eq!(offset, 5);
        assert_eq!(pq.msg_count(), 5);

        let consuming_map_len = pq.store.read().await.consuming_msg_orderly_tree_map.len();
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

        let new_ts = current_millis() + 1000;
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

        pq.put_message(&msgs).await;

        assert!(pq.get_msg_acc_cnt() > 0);
    }

    #[tokio::test]
    async fn test_clear_resets_all_state() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(10);
        pq.put_message(&msgs).await;
        pq.take_messages(5).await;

        assert_eq!(pq.msg_count(), 10);
        assert!(pq.msg_size() > 0);

        pq.clear().await;

        assert_eq!(pq.msg_count(), 0);
        assert_eq!(pq.msg_size(), 0);

        let pending_len = pq.store.read().await.messages.len();
        assert_eq!(pending_len, 0);

        let consuming_map_len = pq.store.read().await.consuming_msg_orderly_tree_map.len();
        assert_eq!(consuming_map_len, 0);
    }

    #[tokio::test]
    async fn test_get_max_span() {
        let pq = ProcessQueue::new();

        assert_eq!(pq.get_max_span().await, 0);

        let msgs = create_test_messages(10);
        pq.put_message(&msgs).await;

        let span = pq.get_max_span().await;
        assert_eq!(span, 9);
    }

    #[tokio::test]
    async fn test_get_max_span_tracks_out_of_order_offsets_after_removal() {
        let pq = ProcessQueue::new();
        let msgs = create_test_messages_with_offsets(&[10, 5, 7]);

        pq.put_message(&msgs).await;
        assert_eq!(pq.get_max_span().await, 5);

        pq.remove_message(&[msgs[1].clone()]).await;
        assert_eq!(pq.get_max_span().await, 3);

        pq.remove_message(&[msgs[0].clone()]).await;
        assert_eq!(pq.get_max_span().await, 0);
    }

    #[tokio::test]
    async fn test_duplicate_offset_keeps_single_count_and_span() {
        let pq = ProcessQueue::new();
        let msgs = create_test_messages_with_offsets(&[3, 3, 8]);

        pq.put_message(&msgs).await;

        assert_eq!(pq.msg_count(), 2);
        assert_eq!(pq.get_max_span().await, 5);
    }

    #[tokio::test]
    async fn test_duplicate_offset_replacement_updates_msg_size() {
        let pq = ProcessQueue::new();

        assert!(pq.put_message(&[create_test_message_with_body_size(3, 100)]).await);
        assert!(!pq.put_message(&[create_test_message_with_body_size(3, 40)]).await);

        assert_eq!(pq.msg_count(), 1);
        assert_eq!(pq.msg_size(), 40);
        assert_eq!(pq.get_max_span().await, 0);
    }

    #[tokio::test]
    async fn test_remove_message_uses_stored_body_size_for_stats() {
        let pq = ProcessQueue::new();
        let stored = vec![
            create_test_message_with_body_size(0, 100),
            create_test_message_with_body_size(1, 100),
        ];

        pq.put_message(&stored).await;
        pq.remove_message(&[create_test_message_with_body_size(0, 1)]).await;

        assert_eq!(pq.msg_count(), 1);
        assert_eq!(pq.msg_size(), 100);
        assert_eq!(pq.get_offset_span().await, Some((1, 1)));
    }

    #[tokio::test]
    async fn test_put_message_keeps_snapshot_for_inner_offsets() {
        let pq = ProcessQueue::new();
        let outer_msgs = create_test_messages_with_offsets(&[10, 20]);
        let inner_msgs = create_test_messages_with_offsets(&[12, 18]);

        pq.put_message(&outer_msgs).await;
        assert_eq!(pq.get_max_span().await, 10);

        pq.put_message(&inner_msgs).await;
        assert_eq!(pq.get_max_span().await, 10);
    }

    #[tokio::test]
    async fn test_contiguous_offsets_use_vecdeque_store() {
        let pq = ProcessQueue::new();
        let msgs = create_test_messages(8);

        pq.put_message(&msgs).await;

        let store = pq.store.read().await;
        assert_eq!(store.messages.storage_kind(), "contiguous");
        assert_eq!(store.messages.len(), 8);
        assert_eq!(store.messages.offset_span(), Some((0, 7)));
    }

    #[tokio::test]
    async fn test_unordered_offsets_fallback_to_btree_store() {
        let pq = ProcessQueue::new();
        let msgs = create_test_messages_with_offsets(&[10, 5, 7]);

        pq.put_message(&msgs).await;

        let store = pq.store.read().await;
        assert_eq!(store.messages.storage_kind(), "btree");
        assert_eq!(store.messages.len(), 3);
        assert_eq!(store.messages.offset_span(), Some((5, 10)));
    }

    #[tokio::test]
    async fn test_middle_remove_downgrades_and_preserves_offsets() {
        let pq = ProcessQueue::new();
        let msgs = create_test_messages(5);

        pq.put_message(&msgs).await;
        pq.remove_message(&[msgs[2].clone()]).await;

        assert_eq!(pq.msg_count(), 4);
        assert_eq!(pq.get_max_span().await, 4);

        let store = pq.store.read().await;
        assert_eq!(store.messages.storage_kind(), "btree");
        assert_eq!(store.messages.offset_span(), Some((0, 4)));
        assert!(!store.messages.contains_key(&2));
    }

    #[tokio::test]
    async fn test_consuming_flag_behavior() {
        let pq = ProcessQueue::new();

        assert!(!pq.is_consuming());

        let msgs = create_test_messages(5);
        let should_dispatch = pq.put_message(&msgs).await;

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

        pq.put_message(&msgs).await;

        let max_offset = pq.store.read().await.queue_offset_max;
        assert_eq!(max_offset, 9);
    }

    #[tokio::test]
    async fn test_commit_preserves_msg_size_correctly() {
        let pq = ProcessQueue::new();

        let msgs = create_test_messages(10);
        pq.put_message(&msgs).await;

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
        pq.put_message(&msgs).await;

        assert_eq!(pq.msg_size(), 500);

        pq.take_messages(5).await;

        pq.commit().await;

        assert_eq!(pq.msg_count(), 0);
        assert_eq!(pq.msg_size(), 0);
    }
}
