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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use arc_swap::ArcSwap;
use cheetah_string::CheetahString;

use crate::clients::nameserver_endpoint::diff_name_server_endpoints;
use crate::clients::nameserver_endpoint::NameServerEndpoint;
use crate::clients::nameserver_endpoint::NameServerEndpointDiff;

/// Immutable endpoint metadata published as one coherent selection view.
///
/// Availability and the cached choice are published with the endpoint list.
/// Each configured endpoint owns a lease that survives topology publications
/// while its identity is retained, but is replaced after removal and re-add.
#[derive(Debug)]
pub(super) struct EndpointState {
    endpoints: Vec<NameServerEndpoint>,
    endpoint_leases: HashMap<CheetahString, EndpointLease>,
    available: HashSet<CheetahString>,
    chosen: Option<CheetahString>,
    generation: u64,
}

/// A generation lease held by nameserver selection, flights, and sessions.
#[derive(Clone, Debug)]
pub(super) struct EndpointLease {
    identity: CheetahString,
    generation: u64,
    identity_token: Arc<()>,
}

impl EndpointLease {
    fn new(identity: CheetahString, generation: u64) -> Self {
        Self {
            identity,
            generation,
            identity_token: Arc::new(()),
        }
    }

    pub(super) fn identity(&self) -> &CheetahString {
        &self.identity
    }

    pub(super) fn same_generation(&self, other: &Self) -> bool {
        self.identity == other.identity
            && self.generation == other.generation
            && Arc::ptr_eq(&self.identity_token, &other.identity_token)
    }
}

impl EndpointState {
    pub(super) fn empty() -> Self {
        Self {
            endpoints: Vec::new(),
            endpoint_leases: HashMap::new(),
            available: HashSet::new(),
            chosen: None,
            generation: 0,
        }
    }

    pub(super) fn endpoints(&self) -> &[NameServerEndpoint] {
        &self.endpoints
    }

    pub(super) fn available(&self) -> &HashSet<CheetahString> {
        &self.available
    }

    pub(super) fn chosen(&self) -> Option<&CheetahString> {
        self.chosen.as_ref()
    }

    pub(super) fn generation(&self) -> u64 {
        self.generation
    }

    pub(super) fn lease_for(&self, identity: &CheetahString) -> Option<EndpointLease> {
        self.endpoint_leases.get(identity).cloned()
    }

    pub(super) fn contains_lease(&self, lease: &EndpointLease) -> bool {
        self.endpoint_leases
            .get(lease.identity())
            .is_some_and(|current| current.same_generation(lease))
    }

    fn next_topology(&self, endpoints: Vec<NameServerEndpoint>) -> Self {
        let identities = endpoints
            .iter()
            .map(|endpoint| endpoint.identity().clone())
            .collect::<HashSet<_>>();
        let generation = self.generation.wrapping_add(1);
        let endpoint_leases = endpoints
            .iter()
            .map(|endpoint| {
                let identity = endpoint.identity().clone();
                let lease = self
                    .lease_for(&identity)
                    .unwrap_or_else(|| EndpointLease::new(identity.clone(), generation));
                (identity, lease)
            })
            .collect();
        Self {
            available: self
                .available
                .iter()
                .filter(|identity| identities.contains(*identity))
                .cloned()
                .collect(),
            chosen: self.chosen.clone().filter(|identity| identities.contains(identity)),
            endpoints,
            endpoint_leases,
            generation,
        }
    }

    fn with_availability(&self, lease: &EndpointLease, identity: &CheetahString, is_available: bool) -> Option<Self> {
        if lease.identity() != identity || !self.contains_lease(lease) {
            return None;
        }

        let mut available = self.available.clone();
        if is_available {
            available.insert(identity.clone());
        } else {
            available.remove(identity);
        }
        Some(self.clone_with(available, self.chosen.clone()))
    }

    fn with_chosen(&self, lease: &EndpointLease) -> Option<Self> {
        if !self.contains_lease(lease) {
            return None;
        }
        let chosen = Some(lease.identity().clone());
        Some(self.clone_with(self.available.clone(), chosen))
    }

    fn without_chosen_if_matches(&self, expected: &EndpointLease) -> Option<Self> {
        if !self.contains_lease(expected) || self.chosen.as_ref() != Some(expected.identity()) {
            return None;
        }
        Some(self.clone_with(self.available.clone(), None))
    }

    fn clone_with(&self, available: HashSet<CheetahString>, chosen: Option<CheetahString>) -> Self {
        Self {
            endpoints: self.endpoints.clone(),
            endpoint_leases: self.endpoint_leases.clone(),
            available,
            chosen,
            generation: self.generation,
        }
    }
}

/// The result of atomically replacing the endpoint topology.
pub(super) struct EndpointTopologyUpdate {
    pub(super) previous: Arc<EndpointState>,
    pub(super) current: Arc<EndpointState>,
    pub(super) diff: NameServerEndpointDiff,
}

/// One ArcSwap publication point for all nameserver selection metadata.
pub(super) struct EndpointStateStore {
    state: ArcSwap<EndpointState>,
}

impl EndpointStateStore {
    pub(super) fn new() -> Self {
        Self {
            state: ArcSwap::from_pointee(EndpointState::empty()),
        }
    }

    pub(super) fn load(&self) -> Arc<EndpointState> {
        self.state.load_full()
    }

    pub(super) fn replace_topology(&self, endpoints: Vec<NameServerEndpoint>) -> Option<EndpointTopologyUpdate> {
        loop {
            let previous = self.load();
            let diff = diff_name_server_endpoints(previous.endpoints(), &endpoints);
            if diff.added.is_empty() && diff.removed.is_empty() {
                return None;
            }

            let current = Arc::new(previous.next_topology(endpoints.clone()));
            let observed = self.state.compare_and_swap(&previous, Arc::clone(&current));
            if Arc::ptr_eq(&*observed, &previous) {
                return Some(EndpointTopologyUpdate {
                    previous,
                    current,
                    diff,
                });
            }
        }
    }

    pub(super) fn update_availability(
        &self,
        lease: &EndpointLease,
        identity: &CheetahString,
        is_available: bool,
    ) -> bool {
        self.update_if_current(lease, |state| state.with_availability(lease, identity, is_available))
    }

    pub(super) fn set_chosen(&self, lease: &EndpointLease) -> bool {
        self.update_if_current(lease, |state| state.with_chosen(lease))
    }

    /// Clears the choice only when the captured endpoint lease still owns it.
    pub(super) fn clear_chosen_if_matches(&self, expected: &EndpointLease) -> bool {
        self.update_if_current(expected, |state| state.without_chosen_if_matches(expected))
    }

    pub(super) fn is_current(&self, lease: &EndpointLease) -> bool {
        self.load().contains_lease(lease)
    }

    fn update_if_current(
        &self,
        lease: &EndpointLease,
        update: impl Fn(&EndpointState) -> Option<EndpointState>,
    ) -> bool {
        loop {
            let previous = self.load();
            if !previous.contains_lease(lease) {
                return false;
            }
            let Some(current) = update(&previous) else {
                return false;
            };
            let current = Arc::new(current);
            let observed = self.state.compare_and_swap(&previous, current);
            if Arc::ptr_eq(&*observed, &previous) {
                return true;
            }
        }
    }
}
