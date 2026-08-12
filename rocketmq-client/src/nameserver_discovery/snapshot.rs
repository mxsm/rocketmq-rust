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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use super::NameServerAuthority;

/// Low-cardinality discovery source categories exposed by status snapshots.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameServerDiscoverySourceKind {
    Dns,
}

/// Freshness of the last-known-good discovery snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameServerDiscoveryFreshness {
    Fresh,
    Stale,
    Unavailable,
}

/// Sanitized DNS failure categories. Resolver messages and queried names are not exposed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameServerDiscoveryErrorCategory {
    Empty,
    NxDomain,
    ServFail,
    Timeout,
    Other,
}

/// Read-only, endpoint-free discovery status suitable for diagnostics and readiness policies.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameServerDiscoveryStatus {
    source_kind: NameServerDiscoverySourceKind,
    generation: u64,
    freshness: NameServerDiscoveryFreshness,
    last_successful_refresh_age: Duration,
    snapshot_age: Duration,
    ipv4_endpoint_count: usize,
    ipv6_endpoint_count: usize,
    healthy_endpoint_count: usize,
    probing_endpoint_count: usize,
    draining_endpoint_count: usize,
    circuit_open_endpoint_count: usize,
    next_refresh_delay: Duration,
    last_error_category: Option<NameServerDiscoveryErrorCategory>,
}

impl NameServerDiscoveryStatus {
    #[allow(
        clippy::too_many_arguments,
        reason = "the fields form one immutable diagnostic snapshot"
    )]
    pub(crate) fn new(
        generation: u64,
        freshness: NameServerDiscoveryFreshness,
        last_successful_refresh_age: Duration,
        snapshot_age: Duration,
        ipv4_endpoint_count: usize,
        ipv6_endpoint_count: usize,
        next_refresh_delay: Duration,
        last_error_category: Option<NameServerDiscoveryErrorCategory>,
    ) -> Self {
        Self {
            source_kind: NameServerDiscoverySourceKind::Dns,
            generation,
            freshness,
            last_successful_refresh_age,
            snapshot_age,
            ipv4_endpoint_count,
            ipv6_endpoint_count,
            healthy_endpoint_count: 0,
            probing_endpoint_count: 0,
            draining_endpoint_count: 0,
            circuit_open_endpoint_count: 0,
            next_refresh_delay,
            last_error_category,
        }
    }

    #[must_use]
    pub fn source_kind(&self) -> NameServerDiscoverySourceKind {
        self.source_kind
    }

    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn freshness(&self) -> NameServerDiscoveryFreshness {
        self.freshness
    }

    #[must_use]
    pub fn last_successful_refresh_age(&self) -> Duration {
        self.last_successful_refresh_age
    }

    #[must_use]
    pub fn snapshot_age(&self) -> Duration {
        self.snapshot_age
    }

    #[must_use]
    pub fn ipv4_endpoint_count(&self) -> usize {
        self.ipv4_endpoint_count
    }

    #[must_use]
    pub fn ipv6_endpoint_count(&self) -> usize {
        self.ipv6_endpoint_count
    }

    #[must_use]
    pub fn healthy_endpoint_count(&self) -> usize {
        self.healthy_endpoint_count
    }

    #[must_use]
    pub fn probing_endpoint_count(&self) -> usize {
        self.probing_endpoint_count
    }

    #[must_use]
    pub fn draining_endpoint_count(&self) -> usize {
        self.draining_endpoint_count
    }

    #[must_use]
    pub fn circuit_open_endpoint_count(&self) -> usize {
        self.circuit_open_endpoint_count
    }

    pub(crate) fn with_transport_counts(
        mut self,
        healthy: usize,
        probing: usize,
        draining: usize,
        circuit_open: usize,
    ) -> Self {
        self.healthy_endpoint_count = healthy;
        self.probing_endpoint_count = probing;
        self.draining_endpoint_count = draining;
        self.circuit_open_endpoint_count = circuit_open;
        self
    }

    #[must_use]
    pub fn next_refresh_delay(&self) -> Duration {
        self.next_refresh_delay
    }

    #[must_use]
    pub fn last_error_category(&self) -> Option<NameServerDiscoveryErrorCategory> {
        self.last_error_category
    }
}

/// A physical endpoint resolved from a logical NameServer authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedNameServerEndpoint {
    authority: NameServerAuthority,
    socket_addr: SocketAddr,
}

impl ResolvedNameServerEndpoint {
    pub(crate) fn new(authority: NameServerAuthority, socket_addr: SocketAddr) -> Self {
        Self { authority, socket_addr }
    }

    /// Returns the logical authority used for DNS and future TLS identity checks.
    #[must_use]
    pub fn authority(&self) -> &NameServerAuthority {
        &self.authority
    }

    /// Returns the physical socket address used to establish a connection.
    #[must_use]
    pub fn socket_addr(&self) -> SocketAddr {
        self.socket_addr
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Freshness {
    Fresh,
    Stale,
    Unavailable,
}

#[derive(Clone, Debug)]
pub(crate) struct EndpointSnapshot {
    pub(crate) generation: u64,
    pub(crate) resolved_at: Instant,
    pub(crate) valid_until: Instant,
    pub(crate) freshness: Freshness,
    pub(crate) endpoints: Arc<[ResolvedNameServerEndpoint]>,
}

impl EndpointSnapshot {
    pub(crate) fn unavailable(now: Instant) -> Self {
        Self {
            generation: 0,
            resolved_at: now,
            valid_until: now,
            freshness: Freshness::Unavailable,
            endpoints: Arc::from([]),
        }
    }

    pub(crate) fn same_endpoint_set(&self, other: &[ResolvedNameServerEndpoint]) -> bool {
        if self.endpoints.len() != other.len() {
            return false;
        }
        let mut current = self
            .endpoints
            .iter()
            .map(ResolvedNameServerEndpoint::socket_addr)
            .collect::<Vec<_>>();
        let mut candidate = other
            .iter()
            .map(ResolvedNameServerEndpoint::socket_addr)
            .collect::<Vec<_>>();
        current.sort_unstable();
        candidate.sort_unstable();
        current == candidate
    }
}
