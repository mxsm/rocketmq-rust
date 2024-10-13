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
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::sync::Notify;

/// A synchronization aid that allows one or more tasks to wait until a set of operations being
/// performed in other tasks completes.
pub struct CountDownLatch {
    /// The current count of the latch.
    count: Mutex<u32>,
    /// A notification mechanism to wake up waiting tasks.
    notify: Notify,
}

impl CountDownLatch {
    /// Creates a new `CountDownLatch` initialized with the given count.
    ///
    /// # Arguments
    ///
    /// * `count` - The number of times `count_down` must be invoked before tasks can pass through
    ///   `wait`.
    ///
    /// # Returns
    ///
    /// An `Arc` to the newly created `CountDownLatch`.
    pub fn new(count: u32) -> Arc<Self> {
        Arc::new(CountDownLatch {
            count: Mutex::new(count),
            notify: Notify::new(),
        })
    }

    /// Decrements the count of the latch, releasing all waiting tasks if the count reaches zero.
    ///
    /// This method is asynchronous and will lock the internal count before decrementing it.
    ///
    /// # Arguments
    ///
    /// * `self` - An `Arc` to the `CountDownLatch`.
    pub async fn count_down(self: Arc<Self>) {
        let mut count = self.count.lock().await;
        *count -= 1;
        if *count == 0 {
            self.notify.notify_waiters();
        }
    }

    /// Waits until the count reaches zero.
    ///
    /// This method is asynchronous and will block the current task until the count reaches zero.
    ///
    /// # Arguments
    ///
    /// * `self` - An `Arc` to the `CountDownLatch`.
    pub async fn wait(self: Arc<Self>) {
        let count = self.count.lock().await;
        if *count > 0 {
            drop(count);
            self.notify.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn count_down_latch_initial_count() {
        let latch = CountDownLatch::new(3);
        let count = latch.count.lock().await;
        assert_eq!(*count, 3);
    }

    #[tokio::test]
    async fn count_down_latch_count_down() {
        let latch = CountDownLatch::new(3);
        latch.clone().count_down().await;
        let count = latch.count.lock().await;
        assert_eq!(*count, 2);
    }

    #[tokio::test]
    async fn count_down_latch_multiple_waiters() {
        let latch = CountDownLatch::new(2);
        let latch_clone1 = latch.clone();
        let latch_clone2 = latch.clone();

        let waiter1 = tokio::spawn(async move {
            latch_clone1.wait().await;
        });

        let waiter2 = tokio::spawn(async move {
            latch_clone2.wait().await;
        });

        latch.clone().count_down().await;
        latch.clone().count_down().await;

        waiter1.await.unwrap();
        waiter2.await.unwrap();

        let count = latch.count.lock().await;
        assert_eq!(*count, 0);
    }
}
