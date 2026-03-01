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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_common::common::message::message_queue::MessageQueue;
use tokio::sync::RwLock;

use crate::consumer::consumer_impl::process_queue::ProcessQueue;

/// Represents a message queue with its associated state for lite pull consumer.
/// Contains process queue reference, pause flag, and offset tracking for pull, consume, and seek
/// operations.
#[derive(Clone)]
pub(crate) struct AssignedQueue {
    pub(crate) message_queue: MessageQueue,
    pub(crate) process_queue: Arc<ProcessQueue>,
    pub(crate) paused: Arc<AtomicBool>,
    pub(crate) pull_offset: Arc<AtomicI64>,
    pub(crate) consume_offset: Arc<AtomicI64>,
    pub(crate) seek_offset: Arc<AtomicI64>,
}

impl AssignedQueue {
    pub fn new(message_queue: MessageQueue) -> Self {
        Self {
            message_queue,
            process_queue: Arc::new(ProcessQueue::new()),
            paused: Arc::new(AtomicBool::new(false)),
            pull_offset: Arc::new(AtomicI64::new(-1)),
            consume_offset: Arc::new(AtomicI64::new(-1)),
            seek_offset: Arc::new(AtomicI64::new(-1)),
        }
    }
}

/// Manages assigned message queues and their process queues for lite pull consumer.
#[derive(Clone)]
pub struct AssignedMessageQueue {
    queue_map: Arc<RwLock<HashMap<MessageQueue, AssignedQueue>>>,
}

impl AssignedMessageQueue {
    /// Creates a new empty AssignedMessageQueue.
    pub fn new() -> Self {
        Self {
            queue_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Assigns a new message queue.
    pub async fn put(&self, mq: MessageQueue) {
        let mut map = self.queue_map.write().await;
        map.entry(mq.clone()).or_insert_with(|| AssignedQueue::new(mq));
    }

    /// Removes a message queue and returns its process queue.
    pub async fn remove(&self, mq: &MessageQueue) -> Option<Arc<ProcessQueue>> {
        let mut map = self.queue_map.write().await;
        map.remove(mq).map(|aq| {
            aq.process_queue.set_dropped(true);
            aq.process_queue
        })
    }

    /// Removes all message queues for the specified topic and returns their process queues.
    pub async fn remove_by_topic(&self, topic: &str) -> Vec<Arc<ProcessQueue>> {
        let mut map = self.queue_map.write().await;
        let mut removed_pqs = Vec::new();
        map.retain(|mq, aq| {
            if mq.topic() == topic {
                aq.process_queue.set_dropped(true);
                removed_pqs.push(aq.process_queue.clone());
                false
            } else {
                true
            }
        });
        removed_pqs
    }

    /// Returns the process queue for the specified message queue.
    pub async fn get_process_queue(&self, mq: &MessageQueue) -> Option<Arc<ProcessQueue>> {
        let map = self.queue_map.read().await;
        map.get(mq).map(|aq| aq.process_queue.clone())
    }

    /// Checks if the specified queue is paused.
    pub async fn is_paused(&self, mq: &MessageQueue) -> bool {
        let map = self.queue_map.read().await;
        map.get(mq).map(|aq| aq.paused.load(Ordering::Acquire)).unwrap_or(false)
    }

    /// Sets the pause state for the specified queue.
    pub async fn set_paused(&self, mq: &MessageQueue, paused: bool) {
        let map = self.queue_map.read().await;
        if let Some(aq) = map.get(mq) {
            aq.paused.store(paused, Ordering::Release);
        }
    }

    /// Returns the seek offset for the specified queue (-1 if not seeking).
    pub async fn get_seek_offset(&self, mq: &MessageQueue) -> i64 {
        let map = self.queue_map.read().await;
        map.get(mq)
            .map(|aq| aq.seek_offset.load(Ordering::Acquire))
            .unwrap_or(-1)
    }

    /// Sets the seek offset for the specified queue.
    pub async fn set_seek_offset(&self, mq: &MessageQueue, offset: i64) {
        let map = self.queue_map.read().await;
        if let Some(aq) = map.get(mq) {
            aq.seek_offset.store(offset, Ordering::Release);
        }
    }

    /// Clears the seek offset for the specified queue (sets to -1).
    pub async fn clear_seek_offset(&self, mq: &MessageQueue) {
        self.set_seek_offset(mq, -1).await;
    }

    /// Returns the pull offset for the specified queue.
    pub async fn get_pull_offset(&self, mq: &MessageQueue) -> i64 {
        let map = self.queue_map.read().await;
        map.get(mq)
            .map(|aq| aq.pull_offset.load(Ordering::Acquire))
            .unwrap_or(-1)
    }

    /// Updates the pull offset for the specified queue.
    pub async fn update_pull_offset(&self, mq: &MessageQueue, offset: i64, process_queue: &Arc<ProcessQueue>) {
        let map = self.queue_map.read().await;
        if let Some(aq) = map.get(mq) {
            // Only update if the process queue instance matches to prevent race conditions
            if Arc::ptr_eq(&aq.process_queue, process_queue) {
                aq.pull_offset.store(offset, Ordering::Release);
            }
        }
    }

    /// Returns the consume offset for the specified queue.
    pub async fn get_consume_offset(&self, mq: &MessageQueue) -> i64 {
        let map = self.queue_map.read().await;
        map.get(mq)
            .map(|aq| aq.consume_offset.load(Ordering::Acquire))
            .unwrap_or(-1)
    }

    /// Updates the consume offset for the specified queue.
    pub async fn update_consume_offset(&self, mq: &MessageQueue, offset: i64) {
        let map = self.queue_map.read().await;
        if let Some(aq) = map.get(mq) {
            aq.consume_offset.store(offset, Ordering::Release);
        }
    }

    /// Returns all assigned message queues.
    pub async fn message_queues(&self) -> HashSet<MessageQueue> {
        let map = self.queue_map.read().await;
        map.keys().cloned().collect()
    }

    /// Returns the number of assigned queues.
    pub async fn size(&self) -> usize {
        let map = self.queue_map.read().await;
        map.len()
    }

    /// Clears all assigned queues.
    pub async fn clear(&self) {
        let mut map = self.queue_map.write().await;
        for (_, aq) in map.iter() {
            aq.process_queue.set_dropped(true);
        }
        map.clear();
    }

    /// Returns the total cached message count across all queues.
    pub async fn total_msg_count(&self) -> i64 {
        let map = self.queue_map.read().await;
        map.values().map(|aq| aq.process_queue.msg_count() as i64).sum()
    }

    /// Returns the total cached message size in MiB across all queues.
    pub async fn total_msg_size_in_mib(&self) -> i64 {
        let map = self.queue_map.read().await;
        map.values()
            .map(|aq| (aq.process_queue.msg_size() / (1024 * 1024)) as i64)
            .sum()
    }
}

impl Default for AssignedMessageQueue {
    fn default() -> Self {
        Self::new()
    }
}
