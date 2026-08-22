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

use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use parking_lot::Mutex;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

use super::endpoint_state::EndpointLease;
use crate::clients::TransportSession;
use crate::deadline::RequestDeadline;
use crate::runtime::processor::RequestProcessor;

/// Session ownership separates direct broker sessions from nameserver sessions
/// that happen to use the same socket address.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
enum RegistryScope {
    Direct,
    NameServer,
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct RegistryKey {
    identity: CheetahString,
    scope: RegistryScope,
}

impl RegistryKey {
    fn from_lease(identity: CheetahString, lease: Option<&EndpointLease>) -> Self {
        Self {
            identity,
            scope: if lease.is_some() {
                RegistryScope::NameServer
            } else {
                RegistryScope::Direct
            },
        }
    }

    fn direct(identity: CheetahString) -> Self {
        Self {
            identity,
            scope: RegistryScope::Direct,
        }
    }

    fn nameserver(identity: CheetahString) -> Self {
        Self {
            identity,
            scope: RegistryScope::NameServer,
        }
    }
}

/// A cached session and, for nameservers, the endpoint generation that owns it.
#[derive(Clone)]
pub(super) struct RegisteredSession<PR> {
    session: TransportSession<PR>,
    lease: Option<EndpointLease>,
}

impl<PR> RegisteredSession<PR> {
    pub(super) fn session(&self) -> &TransportSession<PR> {
        &self.session
    }

    pub(super) fn into_session(self) -> TransportSession<PR> {
        self.session
    }

    pub(super) fn belongs_to(&self, lease: &EndpointLease) -> bool {
        self.lease
            .as_ref()
            .is_some_and(|candidate| candidate.same_generation(lease))
    }
}

enum ConnectFlightState<PR> {
    Connecting,
    Complete(Box<Result<Option<TransportSession<PR>>, Arc<str>>>),
}

/// A shared connection attempt associated with one endpoint generation.
pub(super) struct ConnectFlight<PR> {
    lease: Option<EndpointLease>,
    state: Mutex<ConnectFlightState<PR>>,
    changed: tokio::sync::Notify,
}

impl<PR> ConnectFlight<PR>
where
    PR: RequestProcessor + Sync + Clone + Send + 'static,
{
    fn new(lease: Option<EndpointLease>) -> Self {
        Self {
            lease,
            state: Mutex::new(ConnectFlightState::Connecting),
            changed: tokio::sync::Notify::new(),
        }
    }

    pub(super) fn belongs_to(&self, lease: Option<&EndpointLease>) -> bool {
        match (self.lease.as_ref(), lease) {
            (None, None) => true,
            (Some(current), Some(expected)) => current.same_generation(expected),
            _ => false,
        }
    }

    pub(super) fn complete(&self, result: RocketMQResult<Option<TransportSession<PR>>>) {
        *self.state.lock() =
            ConnectFlightState::Complete(Box::new(result.map_err(|error| Arc::<str>::from(error.to_string()))));
        self.changed.notify_waiters();
    }

    pub(super) async fn wait(
        &self,
        deadline: RequestDeadline,
        target: &CheetahString,
    ) -> RocketMQResult<Option<TransportSession<PR>>> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let ConnectFlightState::Complete(result) = &*self.state.lock() {
                return (**result).clone().map_err(|message| {
                    RocketMQError::network_connection_failed(target.to_string(), message.to_string())
                });
            }
            deadline
                .timeout(changed)
                .await
                .map_err(|_| RocketMQError::network_connection_timeout(target.to_string(), deadline.budget_millis()))?;
        }
    }
}

/// Endpoint-session and connect-flight maps with identity-conditional removal.
pub(super) struct ConnectionRegistry<PR> {
    sessions: DashMap<RegistryKey, RegisteredSession<PR>>,
    flights: DashMap<RegistryKey, Arc<ConnectFlight<PR>>>,
}

impl<PR> ConnectionRegistry<PR>
where
    PR: RequestProcessor + Sync + Clone + Send + 'static,
{
    pub(super) fn new() -> Self {
        Self {
            sessions: DashMap::with_capacity(64),
            flights: DashMap::with_capacity(64),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.sessions.len()
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    pub(super) fn flight_count(&self) -> usize {
        self.flights.len()
    }

    #[cfg(test)]
    pub(super) fn contains(&self, identity: &CheetahString) -> bool {
        self.sessions.contains_key(&RegistryKey::direct(identity.clone()))
            || self.sessions.contains_key(&RegistryKey::nameserver(identity.clone()))
    }

    pub(super) fn healthy_session(
        &self,
        identity: &CheetahString,
        lease: Option<&EndpointLease>,
    ) -> Option<TransportSession<PR>> {
        let key = RegistryKey::from_lease(identity.clone(), lease);
        self.sessions.get(&key).and_then(|entry| {
            let registered = entry.value();
            let generation_matches = lease.is_none_or(|expected| registered.belongs_to(expected));
            (generation_matches && registered.session().connection().is_healthy()).then(|| registered.session().clone())
        })
    }

    pub(super) fn session_identity(&self, session: &TransportSession<PR>) -> Option<CheetahString> {
        self.sessions
            .iter()
            .find(|entry| entry.value().session().is_same_registry_session(session))
            .map(|entry| entry.key().identity.clone())
    }

    pub(super) fn insert_session(
        &self,
        identity: CheetahString,
        session: TransportSession<PR>,
        lease: Option<EndpointLease>,
        can_commit: impl Fn() -> bool,
    ) -> Option<TransportSession<PR>> {
        let key = RegistryKey::from_lease(identity, lease.as_ref());
        match self.sessions.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let current = entry.get();
                if current.session().connection().is_healthy() {
                    let same_generation = match (lease.as_ref(), current.lease.as_ref()) {
                        (None, _) => true,
                        (Some(expected), Some(current)) => current.same_generation(expected),
                        (Some(_), None) => false,
                    };
                    if same_generation {
                        return Some(current.session().clone());
                    }
                    if !can_commit() {
                        return None;
                    }
                }
                if !can_commit() {
                    return None;
                }
                entry.insert(RegisteredSession {
                    session: session.clone(),
                    lease,
                });
                Some(session)
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                if !can_commit() {
                    return None;
                }
                entry.insert(RegisteredSession {
                    session: session.clone(),
                    lease,
                });
                Some(session)
            }
        }
    }

    pub(super) fn remove_session_if_matches(
        &self,
        identity: &CheetahString,
        expected: &TransportSession<PR>,
    ) -> Option<RegisteredSession<PR>> {
        [
            RegistryKey::direct(identity.clone()),
            RegistryKey::nameserver(identity.clone()),
        ]
        .into_iter()
        .find_map(|key| {
            self.sessions
                .remove_if(&key, |_, current| current.session().is_same_registry_session(expected))
                .map(|(_, registered)| registered)
        })
    }

    pub(super) fn remove_unhealthy_session(&self, identity: &CheetahString) -> bool {
        self.sessions
            .remove_if(&RegistryKey::direct(identity.clone()), |_, current| {
                !current.session().connection().is_healthy()
            })
            .is_some()
    }

    #[cfg(test)]
    pub(super) fn remove_session_by_identity(&self, identity: &CheetahString) -> Option<RegisteredSession<PR>> {
        [
            RegistryKey::direct(identity.clone()),
            RegistryKey::nameserver(identity.clone()),
        ]
        .into_iter()
        .find_map(|key| {
            let expected = self.sessions.get(&key).map(|entry| entry.value().clone())?;
            self.sessions
                .remove_if(&key, |_, current| {
                    current.session().is_same_registry_session(expected.session())
                })
                .map(|(_, registered)| registered)
        })
    }

    pub(super) fn remove_sessions_by_identity(&self, identity: &CheetahString) -> Vec<RegisteredSession<PR>> {
        [
            RegistryKey::direct(identity.clone()),
            RegistryKey::nameserver(identity.clone()),
        ]
        .into_iter()
        .filter_map(|key| {
            let expected = self.sessions.get(&key).map(|entry| entry.value().clone())?;
            self.sessions
                .remove_if(&key, |_, current| {
                    current.session().is_same_registry_session(expected.session())
                })
                .map(|(_, registered)| registered)
        })
        .collect()
    }

    pub(super) fn remove_session_for_lease(
        &self,
        identity: &CheetahString,
        lease: &EndpointLease,
    ) -> Option<RegisteredSession<PR>> {
        self.sessions
            .remove_if(&RegistryKey::from_lease(identity.clone(), Some(lease)), |_, current| {
                current.belongs_to(lease)
            })
            .map(|(_, registered)| registered)
    }

    pub(super) fn acquire_flight(
        &self,
        identity: CheetahString,
        lease: Option<EndpointLease>,
    ) -> (Arc<ConnectFlight<PR>>, bool) {
        let key = RegistryKey::from_lease(identity, lease.as_ref());
        match self.flights.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(entry) if entry.get().belongs_to(lease.as_ref()) => {
                (Arc::clone(entry.get()), false)
            }
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let flight = Arc::new(ConnectFlight::new(lease));
                entry.insert(Arc::clone(&flight));
                (flight, true)
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let flight = Arc::new(ConnectFlight::new(lease));
                entry.insert(Arc::clone(&flight));
                (flight, true)
            }
        }
    }

    pub(super) fn remove_flight_if_matches(&self, identity: &CheetahString, expected: &Arc<ConnectFlight<PR>>) {
        self.flights.remove_if(
            &RegistryKey::from_lease(identity.clone(), expected.lease.as_ref()),
            |_, current| Arc::ptr_eq(current, expected),
        );
    }

    pub(super) fn remove_flight_for_lease(&self, identity: &CheetahString, lease: &EndpointLease) {
        self.flights
            .remove_if(&RegistryKey::from_lease(identity.clone(), Some(lease)), |_, current| {
                current.belongs_to(Some(lease))
            });
    }

    pub(super) fn remove_unhealthy_or_idle(
        &self,
        idle_for: impl Fn(&TransportSession<PR>) -> bool,
    ) -> Vec<CheetahString> {
        let entries = self
            .sessions
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect::<Vec<_>>();
        let mut removed = Vec::new();
        for (key, expected) in entries {
            let is_stale = !expected.session().connection().is_healthy() || idle_for(expected.session());
            if is_stale
                && self
                    .sessions
                    .remove_if(&key, |_, current| {
                        current.session().is_same_registry_session(expected.session())
                    })
                    .is_some()
            {
                removed.push(key.identity);
            }
        }
        removed
    }

    pub(super) fn take_all_sessions(&self) -> Vec<(CheetahString, TransportSession<PR>)> {
        let entries = self
            .sessions
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect::<Vec<_>>();
        entries
            .into_iter()
            .filter_map(|(key, expected)| {
                self.sessions
                    .remove_if(&key, |_, current| {
                        current.session().is_same_registry_session(expected.session())
                    })
                    .map(|(_, registered)| (key.identity, registered.into_session()))
            })
            .collect()
    }

    pub(super) fn clear_flights(&self) {
        let entries = self
            .flights
            .iter()
            .map(|entry| (entry.key().clone(), Arc::clone(entry.value())))
            .collect::<Vec<_>>();
        for (key, expected) in entries {
            self.remove_flight_if_matches(&key.identity, &expected);
        }
    }

    #[cfg(test)]
    pub(super) fn has_session_for_lease(&self, identity: &CheetahString, lease: &EndpointLease) -> bool {
        self.sessions
            .get(&RegistryKey::from_lease(identity.clone(), Some(lease)))
            .is_some_and(|entry| entry.value().belongs_to(lease))
    }
}
