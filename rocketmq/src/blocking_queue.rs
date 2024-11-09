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

use std::collections::VecDeque;

use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::time;

pub struct BlockingQueue<T> {
    queue: Mutex<VecDeque<T>>,
    capacity: usize,
    notify: Notify,
}

impl<T> BlockingQueue<T> {
    pub fn new(capacity: usize) -> Self {
        BlockingQueue {
            queue: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            notify: Notify::new(),
        }
    }

    pub async fn put(&self, item: T) {
        loop {
            {
                let mut queue = self.queue.lock().await;
                if queue.len() < self.capacity {
                    queue.push_back(item);
                    self.notify.notify_one(); // Notify only after successful push
                    return;
                }
            }
            self.notify.notified().await;
        }
    }

    pub async fn offer(&self, item: T, timeout: std::time::Duration) -> bool {
        time::timeout(timeout, self.put(item)).await.is_ok()
    }

    pub async fn take(&self) -> T {
        loop {
            {
                let mut queue = self.queue.lock().await;
                if let Some(item) = queue.pop_front() {
                    self.notify.notify_one(); // Notify only after successful pop
                    return item;
                }
            }
            self.notify.notified().await;
        }
    }

    pub async fn poll(&self, timeout: std::time::Duration) -> Option<T> {
        time::timeout(timeout, self.take()).await.ok()
    }
}
