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
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_rust::RocketMQBlockingQueue;
use tokio::sync::Mutex;

pub struct MessageQueueOpContext {
    total_size: AtomicI32,
    last_write_timestamp: Arc<Mutex<u64>>,
    context_queue: RocketMQBlockingQueue<String>,
}

impl MessageQueueOpContext {
    pub fn new(timestamp: u64, queue_length: usize) -> Self {
        MessageQueueOpContext {
            total_size: AtomicI32::new(0),
            last_write_timestamp: Arc::new(Mutex::new(timestamp)),
            context_queue: RocketMQBlockingQueue::new(queue_length),
        }
    }

    pub fn get_total_size(&self) -> i32 {
        self.total_size.load(Ordering::Relaxed)
    }

    pub fn total_size_add_and_get(&self, delta: i32) -> i32 {
        self.total_size.fetch_add(delta, Ordering::AcqRel) + delta
    }

    pub async fn get_last_write_timestamp(&self) -> u64 {
        *self.last_write_timestamp.lock().await
    }

    pub async fn set_last_write_timestamp(&self, timestamp: u64) {
        let mut last_timestamp = self.last_write_timestamp.lock().await;
        *last_timestamp = timestamp;
    }

    pub fn context_queue(&self) -> &RocketMQBlockingQueue<String> {
        &self.context_queue
    }
}
