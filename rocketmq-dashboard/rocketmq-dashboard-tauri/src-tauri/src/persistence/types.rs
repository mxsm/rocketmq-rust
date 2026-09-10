// Copyright 2026 The RocketMQ Rust Authors
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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::sync::Notify;

#[derive(Default)]
pub(super) struct StorageStats {
    pub(super) active: AtomicUsize,
    pub(super) completed: AtomicU64,
    pub(super) failed: AtomicU64,
    pub(super) drained: Notify,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct StorageHealth {
    pub(crate) active: usize,
    pub(crate) completed: u64,
    pub(crate) failed: u64,
}

impl StorageStats {
    pub(super) fn snapshot(&self) -> StorageHealth {
        StorageHealth {
            active: self.active.load(Ordering::Acquire),
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }
}
