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

use std::sync::Arc;

use dashmap::DashMap;
use rocketmq_common::common::message::message_queue::MessageQueue;
use tokio::sync::Mutex;

type LockObject = Arc<Mutex<()>>;

#[derive(Default)]
pub struct MessageQueueLock {
    mq_lock_table: Arc<DashMap<MessageQueue, Arc<DashMap<i32, LockObject>>>>,
}

impl MessageQueueLock {
    pub fn new() -> Self {
        MessageQueueLock {
            mq_lock_table: Arc::new(DashMap::new()),
        }
    }

    pub async fn fetch_lock_object(&self, mq: &MessageQueue) -> Arc<Mutex<()>> {
        self.fetch_lock_object_with_sharding_key(mq, -1).await
    }

    pub async fn fetch_lock_object_with_sharding_key(
        &self,
        mq: &MessageQueue,
        sharding_key_index: i32,
    ) -> Arc<Mutex<()>> {
        // Get or create the inner DashMap for this message queue
        let inner_map = self
            .mq_lock_table
            .entry(mq.clone())
            .or_insert_with(|| Arc::new(DashMap::new()))
            .value()
            .clone();

        // Get or create the lock for this sharding key
        let lock = inner_map
            .entry(sharding_key_index)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .value()
            .clone();

        lock
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message_queue() -> MessageQueue {
        MessageQueue::from_parts("TopicA", "BrokerA", 0)
    }

    #[tokio::test]
    async fn fetch_lock_object_reuses_lock_for_same_queue_like_java() {
        let lock = MessageQueueLock::new();
        let mq = message_queue();

        let first = lock.fetch_lock_object(&mq).await;
        let second = lock.fetch_lock_object(&mq).await;

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn fetch_lock_object_uses_independent_locks_per_sharding_key_like_java() {
        let lock = MessageQueueLock::new();
        let mq = message_queue();

        let first = lock.fetch_lock_object_with_sharding_key(&mq, 0).await;
        let second = lock.fetch_lock_object_with_sharding_key(&mq, 1).await;

        assert!(!Arc::ptr_eq(&first, &second));
    }
}
