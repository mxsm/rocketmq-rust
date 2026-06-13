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

#![allow(clippy::missing_const_for_thread_local)]
use std::cell::RefCell;
use std::fmt;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use rand::RngExt;
use rand::SeedableRng;

thread_local! {
    static THREAD_LOCAL_INDEXES: RefCell<Vec<Option<i32>>> = const { RefCell::new(Vec::new()) };
    static THREAD_LOCAL_RNG: RefCell<rand::rngs::SmallRng> = RefCell::new(rand::rngs::SmallRng::seed_from_u64(rand::random()));
}

static NEXT_INDEX_ID: AtomicUsize = AtomicUsize::new(0);

const POSITIVE_MASK: i32 = 0x7FFFFFFF;
const MAX: i32 = i32::MAX;

#[derive(Clone)]
pub struct ThreadLocalIndex {
    id: usize,
}

impl ThreadLocalIndex {
    pub fn new() -> Self {
        Self {
            id: NEXT_INDEX_ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub fn increment_and_get(&self) -> i32 {
        THREAD_LOCAL_INDEXES.with(|indexes| {
            let mut indexes = indexes.borrow_mut();
            let slot = thread_local_slot(&mut indexes, self.id);
            let random = || THREAD_LOCAL_RNG.with(|rng| rng.borrow_mut().random::<i32>());
            let new_value = next_java_index_value(*slot, random);
            *slot = Some(new_value);
            new_value & POSITIVE_MASK
        })
    }

    pub fn reset(&self) {
        THREAD_LOCAL_INDEXES.with(|indexes| {
            let mut indexes = indexes.borrow_mut();
            let slot = thread_local_slot(&mut indexes, self.id);
            let new_value = THREAD_LOCAL_RNG.with(|rng| rng.borrow_mut().random_range(0..MAX));
            *slot = Some(new_value);
        });
    }
}

impl Default for ThreadLocalIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn thread_local_slot(indexes: &mut Vec<Option<i32>>, id: usize) -> &mut Option<i32> {
    if indexes.len() <= id {
        indexes.resize(id + 1, None);
    }
    &mut indexes[id]
}

#[inline]
fn next_java_index_value<F>(current: Option<i32>, mut random: F) -> i32
where
    F: FnMut() -> i32,
{
    current.unwrap_or_else(&mut random).wrapping_add(1)
}

impl fmt::Display for ThreadLocalIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        THREAD_LOCAL_INDEXES.with(|indexes| {
            write!(
                f,
                "ThreadLocalIndex {{ thread_local_index={} }}",
                indexes.borrow().get(self.id).and_then(|value| *value).unwrap_or(0)
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_java_index_value_increments_existing_value_like_java() {
        let value = next_java_index_value(Some(41), || 100);

        assert_eq!(value, 42);
    }

    #[test]
    fn next_java_index_value_increments_initial_random_value_like_java() {
        let value = next_java_index_value(None, || 41);

        assert_eq!(value, 42);
    }

    #[test]
    fn next_java_index_value_wraps_before_positive_mask_like_java_int_overflow() {
        let value = next_java_index_value(None, || i32::MAX);

        assert_eq!(value, i32::MIN);
        assert_eq!(value & POSITIVE_MASK, 0);
    }

    #[test]
    fn different_thread_local_index_instances_keep_independent_java_state() {
        let first = ThreadLocalIndex::new();
        let second = ThreadLocalIndex::new();

        first.reset();
        second.reset();
        let first_value = first.increment_and_get();
        let second_value = second.increment_and_get();

        assert_eq!(first.increment_and_get(), first_value.wrapping_add(1) & POSITIVE_MASK);
        assert_eq!(second.increment_and_get(), second_value.wrapping_add(1) & POSITIVE_MASK);
    }

    #[test]
    fn cloned_thread_local_index_shares_the_same_java_instance_state() {
        let original = ThreadLocalIndex::new();
        original.reset();
        let cloned = original.clone();

        let first = original.increment_and_get();
        let second = cloned.increment_and_get();

        assert_eq!(second, first.wrapping_add(1) & POSITIVE_MASK);
    }
}
