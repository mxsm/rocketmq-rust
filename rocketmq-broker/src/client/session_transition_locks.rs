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

use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use parking_lot::Mutex;
use parking_lot::MutexGuard;
use rocketmq_transport::api::v2::SessionId;

const SESSION_TRANSITION_STRIPES: usize = 64;

/// Serializes the multi-index transition for one client identity without placing a global lock
/// on unrelated client heartbeats.
pub(crate) struct ClientSessionTransitionLocks {
    stripes: Box<[Mutex<()>]>,
}

pub(crate) struct ClientSessionTransitionGuard<'a> {
    owner: &'a ClientSessionTransitionLocks,
    client_stripe: usize,
    session_stripe: usize,
    _first: MutexGuard<'a, ()>,
    _second: Option<MutexGuard<'a, ()>>,
}

impl Default for ClientSessionTransitionLocks {
    fn default() -> Self {
        let stripes = (0..SESSION_TRANSITION_STRIPES)
            .map(|_| Mutex::new(()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { stripes }
    }
}

impl ClientSessionTransitionLocks {
    fn stripe_for(&self, key: &impl Hash) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.stripes.len()
    }

    pub(crate) fn lock(&self, client_id: &str, session_id: SessionId) -> ClientSessionTransitionGuard<'_> {
        let client_stripe = self.stripe_for(&(0_u8, client_id));
        let session_stripe = self.stripe_for(&(1_u8, session_id));
        let (first_stripe, second_stripe) = if client_stripe <= session_stripe {
            (client_stripe, session_stripe)
        } else {
            (session_stripe, client_stripe)
        };
        let first = self.stripes[first_stripe].lock();
        let second = (first_stripe != second_stripe).then(|| self.stripes[second_stripe].lock());
        ClientSessionTransitionGuard {
            owner: self,
            client_stripe,
            session_stripe,
            _first: first,
            _second: second,
        }
    }

    pub(crate) fn owns(&self, guard: &ClientSessionTransitionGuard<'_>) -> bool {
        std::ptr::eq(self, guard.owner)
    }

    pub(crate) fn covers(
        &self,
        guard: &ClientSessionTransitionGuard<'_>,
        client_id: &str,
        session_id: SessionId,
    ) -> bool {
        self.owns(guard)
            && guard.client_stripe == self.stripe_for(&(0_u8, client_id))
            && guard.session_stripe == self.stripe_for(&(1_u8, session_id))
    }
}
