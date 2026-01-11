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

use rand::Rng;
use rand::SeedableRng;

thread_local! {
    static THREAD_LOCAL_INDEX: RefCell<Option<i32>> = const {RefCell::new(None)};
    static THREAD_LOCAL_RNG: RefCell<rand::rngs::SmallRng> = RefCell::new(rand::rngs::SmallRng::seed_from_u64(rand::random()));
}

const POSITIVE_MASK: i32 = 0x7FFFFFFF;
const MAX: i32 = i32::MAX;

#[derive(Default, Clone)]
pub struct ThreadLocalIndex;

impl ThreadLocalIndex {
    pub fn increment_and_get(&self) -> i32 {
        THREAD_LOCAL_INDEX.with(|index| {
            let mut index = index.borrow_mut();
            let new_value = match *index {
                Some(val) => val.wrapping_add(1),
                None => THREAD_LOCAL_RNG.with(|rng| rng.borrow_mut().random::<i32>()),
            };
            *index = Some(new_value);
            new_value & POSITIVE_MASK
        })
    }

    pub fn reset(&self) {
        THREAD_LOCAL_INDEX.with(|index| {
            let new_value = THREAD_LOCAL_RNG.with(|rng| rng.borrow_mut().random_range(0..MAX));
            *index.borrow_mut() = Some(new_value);
        });
    }
}

impl fmt::Display for ThreadLocalIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        THREAD_LOCAL_INDEX.with(|index| {
            write!(
                f,
                "ThreadLocalIndex {{ thread_local_index={} }}",
                index.borrow().unwrap_or(0)
            )
        })
    }
}
