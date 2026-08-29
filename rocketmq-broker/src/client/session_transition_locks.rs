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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use parking_lot::Mutex;
use parking_lot::MutexGuard;
use rocketmq_transport::api::v2::SessionId;

const SESSION_TRANSITION_STRIPES: usize = 64;

/// Serializes the multi-index transition for one client identity without placing a global lock
/// on unrelated client heartbeats.
pub(crate) struct ClientSessionTransitionLocks {
    stripes: Box<[Mutex<()>]>,
    next_binding_order: AtomicU64,
    session_binding_order: DashMap<SessionId, u64>,
    session_clients: DashMap<SessionId, CheetahString>,
    latest_client_sessions: DashMap<CheetahString, (SessionId, u64)>,
    retiring_sessions: DashMap<SessionId, ()>,
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
        Self {
            stripes,
            next_binding_order: AtomicU64::new(1),
            session_binding_order: DashMap::new(),
            session_clients: DashMap::new(),
            latest_client_sessions: DashMap::new(),
            retiring_sessions: DashMap::new(),
        }
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

    /// Claims the canonical binding for a heartbeat while its client/session transition is locked.
    ///
    /// A session receives its Broker-local order on its first heartbeat. Once a newer session
    /// replaces the same client identity, a delayed heartbeat from the older session cannot
    /// reclaim the binding while its graceful retirement is still in flight.
    pub(crate) fn claim_binding(
        &self,
        guard: &ClientSessionTransitionGuard<'_>,
        client_id: &CheetahString,
        session_id: SessionId,
    ) -> bool {
        assert!(
            self.covers(guard, client_id, session_id),
            "session binding claim requires the matching transition guard"
        );
        if self
            .session_clients
            .get(&session_id)
            .is_some_and(|current| current.as_str() != client_id.as_str())
            || self.retiring_sessions.contains_key(&session_id)
        {
            return false;
        }
        let binding_order = self
            .session_binding_order
            .get(&session_id)
            .map(|entry| *entry)
            .unwrap_or_else(|| self.next_binding_order.fetch_add(1, Ordering::AcqRel));
        if self
            .latest_client_sessions
            .get(client_id)
            .is_some_and(|current| current.0 != session_id && current.1 > binding_order)
        {
            return false;
        }
        self.session_binding_order.insert(session_id, binding_order);
        self.session_clients.insert(session_id, client_id.clone());
        self.latest_client_sessions
            .insert(client_id.clone(), (session_id, binding_order));
        true
    }

    /// Prevents an expired session from being admitted again while its asynchronous close drains.
    pub(crate) fn mark_retiring(
        &self,
        guard: &ClientSessionTransitionGuard<'_>,
        client_id: &CheetahString,
        session_id: SessionId,
    ) -> bool {
        assert!(
            self.covers(guard, client_id, session_id),
            "session retirement requires the matching transition guard"
        );
        if self
            .session_clients
            .get(&session_id)
            .is_none_or(|current| current.as_str() != client_id.as_str())
        {
            return false;
        }
        self.retiring_sessions.insert(session_id, ());
        true
    }

    /// Releases one exact session generation without disturbing a newer client binding.
    pub(crate) fn release_binding(&self, session_id: SessionId) {
        let Some(client_id) = self.session_clients.get(&session_id).map(|entry| entry.clone()) else {
            return;
        };
        let guard = self.lock(&client_id, session_id);
        self.session_clients.remove(&session_id);
        self.session_binding_order.remove(&session_id);
        self.retiring_sessions.remove(&session_id);
        let has_older_generation = self
            .session_clients
            .iter()
            .any(|entry| entry.value().as_str() == client_id.as_str());
        if !has_older_generation {
            self.latest_client_sessions.remove(&client_id);
        }
        drop(guard);
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_transport::test_support::session_id_for_test;

    use super::*;

    #[test]
    fn stale_session_cannot_reclaim_replacement_and_old_release_is_exact() {
        let bindings = ClientSessionTransitionLocks::default();
        let client_id = CheetahString::from_static_str("stable-client");
        let old_session = session_id_for_test(1);
        let replacement = session_id_for_test(2);

        let old_guard = bindings.lock(&client_id, old_session);
        assert!(bindings.claim_binding(&old_guard, &client_id, old_session));
        drop(old_guard);
        let replacement_guard = bindings.lock(&client_id, replacement);
        assert!(bindings.claim_binding(&replacement_guard, &client_id, replacement));
        drop(replacement_guard);

        let stale_guard = bindings.lock(&client_id, old_session);
        assert!(!bindings.claim_binding(&stale_guard, &client_id, old_session));
        drop(stale_guard);
        bindings.release_binding(old_session);

        let replacement_guard = bindings.lock(&client_id, replacement);
        assert!(bindings.claim_binding(&replacement_guard, &client_id, replacement));
    }

    #[test]
    fn latest_disconnect_keeps_high_water_until_older_generation_releases() {
        let bindings = ClientSessionTransitionLocks::default();
        let client_id = CheetahString::from_static_str("stable-client");
        let old_session = session_id_for_test(11);
        let replacement = session_id_for_test(12);

        let old_guard = bindings.lock(&client_id, old_session);
        assert!(bindings.claim_binding(&old_guard, &client_id, old_session));
        drop(old_guard);
        let replacement_guard = bindings.lock(&client_id, replacement);
        assert!(bindings.claim_binding(&replacement_guard, &client_id, replacement));
        drop(replacement_guard);

        bindings.release_binding(replacement);
        let stale_guard = bindings.lock(&client_id, old_session);
        assert!(!bindings.claim_binding(&stale_guard, &client_id, old_session));
        drop(stale_guard);

        bindings.release_binding(old_session);
        let next_session = session_id_for_test(13);
        let next_guard = bindings.lock(&client_id, next_session);
        assert!(bindings.claim_binding(&next_guard, &client_id, next_session));
    }

    #[test]
    fn retiring_session_cannot_refresh_before_disconnect_cleanup() {
        let bindings = ClientSessionTransitionLocks::default();
        let client_id = CheetahString::from_static_str("expiring-client");
        let session_id = session_id_for_test(21);

        let transition = bindings.lock(&client_id, session_id);
        assert!(bindings.claim_binding(&transition, &client_id, session_id));
        assert!(bindings.mark_retiring(&transition, &client_id, session_id));
        assert!(!bindings.claim_binding(&transition, &client_id, session_id));
        drop(transition);

        bindings.release_binding(session_id);
    }
}
