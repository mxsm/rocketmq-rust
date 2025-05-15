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
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::sync::MutexGuard;

const MAXIMUM_CAPACITY: usize = 1 << 6;

#[derive(Clone)]
pub struct TopicQueueLock {
    size: usize,
    locks: Vec<Arc<Mutex<()>>>,
}

impl Default for TopicQueueLock {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicQueueLock {
    pub fn new() -> Self {
        Self::with_size(32)
    }

    pub fn with_size(size: usize) -> Self {
        let size = table_size_for(size);
        let locks = (0..size)
            .map(|_| Arc::new(Mutex::new(())))
            .collect::<Vec<_>>();
        Self { size, locks }
    }

    pub async fn lock<'a, K>(&'a self, key: &K) -> MutexGuard<'a, ()>
    where
        K: Hash + ?Sized,
    {
        let index = self.index_for_key(key);
        self.locks[index].lock().await
    }

    fn index_for_key<K: Hash + ?Sized>(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() & 0x7FFF_FFFF) as usize % self.size
    }
}

//copy from Java HashMap
#[inline]
fn table_size_for(cap: usize) -> usize {
    let n = cap.saturating_sub(1);
    let n = n | (n >> 1);
    let n = n | (n >> 2);
    let n = n | (n >> 4);
    let n = n | (n >> 8);
    let n = n | (n >> 16);
    if n >= MAXIMUM_CAPACITY {
        MAXIMUM_CAPACITY
    } else {
        n + 1
    }
}

#[inline]
fn calculate_hash(key: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculates_table_size_for_zero_capacity() {
        assert_eq!(table_size_for(0), 1);
    }

    #[test]
    fn calculates_table_size_for_one_capacity() {
        assert_eq!(table_size_for(1), 1);
    }

    #[test]
    fn calculates_table_size_for_exact_power_of_two() {
        assert_eq!(table_size_for(64), 64);
    }

    #[test]
    fn calculates_table_size_for_just_above_power_of_two() {
        assert_eq!(table_size_for(65), 64);
    }

    #[test]
    fn calculates_table_size_for_maximum_capacity() {
        assert_eq!(table_size_for(MAXIMUM_CAPACITY), MAXIMUM_CAPACITY);
    }

    #[test]
    fn calculates_table_size_for_above_maximum_capacity() {
        assert_eq!(table_size_for(MAXIMUM_CAPACITY + 1), MAXIMUM_CAPACITY);
    }

    #[test]
    fn calculates_table_size_for_large_number() {
        assert_eq!(table_size_for(1_000_000), 64);
    }

    #[tokio::test]
    async fn locks_same_key_exclusively() {
        let lock = TopicQueueLock::new();
        let key = "key";

        let guard1 = lock.lock(&key).await;
        let lock_clone = lock.clone();

        let handle = tokio::spawn(async move {
            let _ = lock_clone.lock(&key).await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(!handle.is_finished());

        drop(guard1);
        handle.await.unwrap();
    }

    #[test]
    fn calculates_index_for_key_correctly() {
        let lock = TopicQueueLock::with_size(16);
        let key1 = "key1";
        let key2 = "key21";

        let index1 = lock.index_for_key(&key1);
        let index2 = lock.index_for_key(&key2);

        assert!(index1 < 16);
        assert!(index2 < 16);
        assert_ne!(index1, index2);
    }

    #[test]
    fn creates_correct_number_of_locks() {
        let lock = TopicQueueLock::with_size(32);
        assert_eq!(lock.locks.len(), 32);
    }

    #[test]
    fn handles_zero_capacity_gracefully() {
        let lock = TopicQueueLock::with_size(0);
        assert_eq!(lock.locks.len(), 1);
    }
}
