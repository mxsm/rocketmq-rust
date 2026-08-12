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

use tokio::time::Instant;

use super::NameServerAuthority;

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
