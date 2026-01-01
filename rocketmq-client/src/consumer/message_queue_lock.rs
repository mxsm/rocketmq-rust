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
use std::sync::Arc;

use rocketmq_common::common::message::message_queue::MessageQueue;
use tokio::sync::Mutex;

type LockObject = Arc<Mutex<()>>;
type LockTable = Arc<Mutex<HashMap<MessageQueue, Arc<Mutex<HashMap<i32, LockObject>>>>>>;

#[derive(Default)]
pub(crate) struct MessageQueueLock {
    mq_lock_table: LockTable,
}

impl MessageQueueLock {
    pub fn new() -> Self {
        MessageQueueLock {
            mq_lock_table: Arc::new(Mutex::new(HashMap::new())),
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
        let mut mq_lock_table = self.mq_lock_table.lock().await;
        let obj_map = mq_lock_table
            .entry(mq.clone())
            .or_insert_with(|| Arc::new(Mutex::new(HashMap::new())));
        let mut obj_map = obj_map.lock().await;
        let lock = obj_map
            .entry(sharding_key_index)
            .or_insert_with(|| Arc::new(Mutex::new(())));
        lock.clone()
    }
}
