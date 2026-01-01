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

use std::collections::VecDeque;

use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::time;

/// A thread-safe bounded blocking queue. To replace Java `LinkedBlockingQueue`.
///
/// This queue allows multiple producers and consumers to add and remove items
/// concurrently. It uses a `tokio::sync::Mutex` to ensure mutual exclusion and
/// a `tokio::sync::Notify` to notify waiting tasks.
pub struct BlockingQueue<T> {
    /// The underlying queue storing the items.
    queue: Mutex<VecDeque<T>>,
    /// The maximum capacity of the queue.
    capacity: usize,
    /// A notification mechanism to wake up waiting tasks.
    notify: Notify,
}

impl<T> BlockingQueue<T> {
    /// Creates a new `BlockingQueue` with the specified capacity.
    ///
    /// # Arguments
    ///
    /// * `capacity` - The maximum number of items the queue can hold.
    ///
    /// # Returns
    ///
    /// A new instance of `BlockingQueue`.
    pub fn new(capacity: usize) -> Self {
        BlockingQueue {
            queue: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            notify: Notify::new(),
        }
    }

    /// Adds an item to the queue, waiting if necessary for space to become available.
    ///
    /// This method will block the current task until space is available in the queue.
    ///
    /// # Arguments
    ///
    /// * `item` - The item to be added to the queue.
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

    /// Attempts to add an item to the queue within a specified timeout.
    ///
    /// This method will block the current task until space is available in the queue
    /// or the timeout is reached.
    ///
    /// # Arguments
    ///
    /// * `item` - The item to be added to the queue.
    /// * `timeout` - The maximum duration to wait for space to become available.
    ///
    /// # Returns
    ///
    /// `true` if the item was added to the queue, `false` if the timeout was reached.
    pub async fn offer(&self, item: T, timeout: std::time::Duration) -> bool {
        time::timeout(timeout, self.put(item)).await.is_ok()
    }

    /// Removes and returns an item from the queue, waiting if necessary until an item is available.
    ///
    /// This method will block the current task until an item is available in the queue.
    ///
    /// # Returns
    ///
    /// The item removed from the queue.
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

    /// Attempts to remove and return an item from the queue within a specified timeout.
    ///
    /// This method will block the current task until an item is available in the queue
    /// or the timeout is reached.
    ///
    /// # Arguments
    ///
    /// * `timeout` - The maximum duration to wait for an item to become available.
    ///
    /// # Returns
    ///
    /// `Some(item)` if an item was removed from the queue, `None` if the timeout was reached.
    pub async fn poll(&self, timeout: std::time::Duration) -> Option<T> {
        time::timeout(timeout, self.take()).await.ok()
    }

    /// Attempts to remove and return an item from the queue without waiting.
    ///
    /// This method acquires a lock on the queue and attempts to remove an item.
    /// If the queue is empty, it will notify waiting tasks.
    ///
    /// # Returns
    ///
    /// `Some(item)` if an item was removed from the queue, `None` if the queue was empty.
    pub async fn try_poll(&self) -> Option<T> {
        let mut queue = self.queue.lock().await;
        let item = queue.pop_front();
        if item.is_none() {
            self.notify.notify_one(); // Notify only after successful pop
        }
        item
    }

    /// Checks if the queue is empty.
    ///
    /// This method acquires a lock on the queue and checks if it contains any items.
    ///
    /// # Returns
    ///
    /// `true` if the queue is empty, `false` otherwise.
    pub async fn is_empty(&self) -> bool {
        let queue = self.queue.lock().await;
        queue.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::Duration;

    use super::*;

    #[tokio::test]
    async fn put_item_in_queue() {
        let queue = BlockingQueue::new(2);
        queue.put(1).await;
        let item = queue.take().await;
        assert_eq!(item, 1);
    }

    #[tokio::test]
    async fn offer_item_within_timeout() {
        let queue = BlockingQueue::new(1);
        let result = queue.offer(1, Duration::from_millis(100)).await;
        assert!(result);
    }

    #[tokio::test]
    async fn offer_item_exceeds_timeout() {
        let queue = BlockingQueue::new(1);
        queue.put(1).await;
        let result = queue.offer(2, Duration::from_millis(100)).await;
        assert!(!result);
    }

    #[tokio::test]
    async fn poll_item_within_timeout() {
        let queue = BlockingQueue::new(1);
        queue.put(1).await;
        let item = queue.poll(Duration::from_millis(100)).await;
        assert_eq!(item, Some(1));
    }

    #[tokio::test]
    async fn poll_item_exceeds_timeout() {
        let queue = BlockingQueue::<()>::new(1);
        let item = queue.poll(Duration::from_millis(100)).await;
        assert_eq!(item, None);
    }
}
