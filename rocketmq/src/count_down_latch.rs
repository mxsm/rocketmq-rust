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
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::sync::Notify;

/// A synchronization aid that allows one or more tasks to wait until a set of operations being
/// performed in other tasks completes.
#[derive(Clone)]
pub struct CountDownLatch {
    /// The current count of the latch.
    count: Arc<Mutex<u32>>,
    /// A notification mechanism to wake up waiting tasks.
    notify: Arc<Notify>,
}

impl CountDownLatch {
    /// A new `CountDownLatch`.
    #[inline]
    pub fn new(count: u32) -> Self {
        CountDownLatch {
            count: Arc::new(Mutex::new(count)),
            notify: Arc::new(Notify::new()),
        }
    }

    #[inline]
    pub async fn count_down(&self) {
        let mut count = self.count.lock().await;
        *count -= 1;
        if *count == 0 {
            self.notify.notify_waiters();
        }
    }

    #[inline]
    pub async fn wait(&self) {
        let count = self.count.lock().await;
        if *count > 0 {
            drop(count);
            self.notify.notified().await;
        }
    }

    #[inline]
    pub async fn wait_timeout(&self, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, self.wait()).await.is_ok()
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
    async fn wait_timeout_reaches_zero_before_timeout() {
        let latch = CountDownLatch::new(1);
        latch.count_down().await;
        let result = latch.wait_timeout(Duration::from_secs(1)).await;
        assert!(result);
    }

    #[tokio::test]
    async fn wait_timeout_exceeds_timeout() {
        let latch = CountDownLatch::new(1);
        let result = latch.wait_timeout(Duration::from_millis(10)).await;
        assert!(!result);
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
