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

use std::hash::BuildHasher;
use std::hash::Hash;
use std::sync::Arc;

use hashbrown::DefaultHashBuilder;
use tokio::sync::Mutex;
use tokio::sync::MutexGuard;

const MAXIMUM_CAPACITY: usize = 1 << 6;

#[derive(Clone)]
pub struct TopicQueueLock {
    mask: usize,
    locks: Arc<Vec<Mutex<()>>>,
    hasher_builder: DefaultHashBuilder,
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
        let capacity = size.next_power_of_two();
        let locks = (0..capacity).map(|_| Mutex::new(())).collect::<Vec<_>>();
        Self {
            mask: capacity - 1,
            locks: Arc::new(locks),
            hasher_builder: DefaultHashBuilder::default(),
        }
    }

    #[inline(always)]
    pub async fn lock<'a, K>(&'a self, key: &K) -> MutexGuard<'a, ()>
    where
        K: Hash + ?Sized,
    {
        let index = self.index_for_key(key);
        self.locks[index].lock().await
    }

    #[inline(always)]
    fn index_for_key<K: Hash + ?Sized>(&self, key: &K) -> usize {
        (self.hasher_builder.hash_one(key) as usize) & self.mask
    }
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
