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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct ControllableOffset {
    value: Arc<AtomicI64>,
    allow_to_update: Arc<AtomicBool>,
}

impl ControllableOffset {
    pub fn new(value: i64) -> Self {
        Self {
            value: Arc::new(AtomicI64::new(value)),
            allow_to_update: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn new_atomic(value: AtomicI64) -> Self {
        Self {
            value: Arc::new(value),
            allow_to_update: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn update(&self, target: i64, increase_only: bool) {
        if self.allow_to_update.load(Ordering::SeqCst) {
            self.value
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |val| {
                    if self.allow_to_update.load(Ordering::SeqCst) {
                        if increase_only {
                            Some(std::cmp::max(target, val))
                        } else {
                            Some(target)
                        }
                    } else {
                        Some(val)
                    }
                })
                .ok();
        }
    }

    pub fn update_unconditionally(&self, target: i64) {
        self.update(target, false);
    }

    pub fn update_and_freeze(&self, target: i64) {
        self.value
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |val| {
                self.allow_to_update.store(false, Ordering::SeqCst);
                Some(target)
            })
            .ok();
    }

    pub fn get_offset(&self) -> i64 {
        self.value.load(Ordering::SeqCst)
    }
}
