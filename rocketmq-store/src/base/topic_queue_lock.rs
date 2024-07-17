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

use tokio::sync::Mutex;

const MAXIMUM_CAPACITY: usize = 1 << 6;

pub(crate) struct TopicQueueLock {
    pub(crate) size: usize,
    pub(crate) size_: usize,
    pub(crate) lock_vec: Vec<Mutex<()>>,
}

impl TopicQueueLock {
    pub(crate) fn new(size: usize) -> Self {
        let size = table_size_for(size);
        let mut lock_vec = Vec::with_capacity(size);
        for _ in 0..size {
            lock_vec.push(Mutex::new(()));
        }
        TopicQueueLock {
            size,
            size_: size - 1,
            lock_vec,
        }
    }
}

impl TopicQueueLock {
    #[inline]
    pub(crate) fn lock(&self, topic_queue_key: &str) -> &Mutex<()> {
        let hash = calculate_hash(topic_queue_key);
        let index = (hash & self.size_ as u64) as usize;
        &self.lock_vec[index]
    }
}

//copy from Java HashMap
#[inline]
fn table_size_for(cap: usize) -> usize {
    let n = cap.saturating_sub(1);
    let n = n | n >> 1;
    let n = n | n >> 2;
    let n = n | n >> 4;
    let n = n | n >> 8;
    let n = n | n >> 16;
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
}
