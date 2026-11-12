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

use std::hash::BuildHasher;
use std::hash::RandomState;

use tokio::sync::Mutex;
use tokio::sync::MutexGuard;

/// Lets one route-miss lookup per topic query the NameServer at a time.
///
/// A producer that starts many sends before its first route reply arrives would otherwise send
/// one route query per send. Those queries share the NameServer connection's bounded control
/// queue, so a burst can reject sends that would have found the route a moment later. A caller
/// that waited here reuses the route the previous holder applied instead of querying again.
///
/// Topics share a fixed set of locks, so memory stays bounded however many topics a client
/// uses; two topics on one lock only take turns, and each lookup is one short request.
pub(super) struct RouteMissLookups {
    hasher: RandomState,
    locks: Box<[Mutex<()>]>,
}

impl RouteMissLookups {
    const LOCKS: usize = 64;

    pub(super) fn new() -> Self {
        Self {
            hasher: RandomState::new(),
            locks: (0..Self::LOCKS).map(|_| Mutex::new(())).collect(),
        }
    }

    /// Waits until no other route-miss lookup for `topic` is in progress.
    pub(super) async fn lock(&self, topic: &str) -> MutexGuard<'_, ()> {
        // The remainder is below `LOCKS`, so the conversion back to `usize` cannot truncate.
        let index = (self.hasher.hash_one(topic) % Self::LOCKS as u64) as usize;
        self.locks[index].lock().await
    }
}
