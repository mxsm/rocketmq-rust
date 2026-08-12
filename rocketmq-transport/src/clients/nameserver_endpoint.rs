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

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

/// A physical connection address paired with its logical TLS authority.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectTarget {
    socket_addr: SocketAddr,
    authority: CheetahString,
}

impl ConnectTarget {
    /// Creates a resolved target without discarding its logical authority.
    pub fn new(socket_addr: SocketAddr, authority: impl Into<CheetahString>) -> RocketMQResult<Self> {
        let authority = authority.into();
        validate_authority(&authority)?;
        Ok(Self { socket_addr, authority })
    }

    /// Returns the physical TCP destination.
    #[must_use]
    pub fn socket_addr(&self) -> SocketAddr {
        self.socket_addr
    }

    /// Returns the logical `host:port` authority.
    #[must_use]
    pub fn authority(&self) -> &str {
        &self.authority
    }

    /// Returns the DNS name or IP used for TLS SNI and certificate verification.
    #[must_use]
    pub fn tls_server_name(&self) -> &str {
        authority_host(&self.authority)
    }

    /// Returns a stable identity containing both logical and physical components.
    #[must_use]
    pub fn identity(&self) -> CheetahString {
        CheetahString::from_string(format!("{}=>{}", self.authority, self.socket_addr))
    }
}

/// A NameServer selector entry. Legacy strings retain their historical dialing behavior.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NameServerEndpoint {
    identity: CheetahString,
    target: NameServerDialTarget,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum NameServerDialTarget {
    Legacy(CheetahString),
    Resolved(ConnectTarget),
}

impl NameServerEndpoint {
    /// Wraps a legacy `host:port` or `ip:port` address.
    pub fn legacy(address: impl Into<CheetahString>) -> RocketMQResult<Self> {
        let address = address.into();
        if address.trim().is_empty() {
            return Err(invalid_authority(&address, "must not be blank"));
        }
        Ok(Self {
            identity: address.clone(),
            target: NameServerDialTarget::Legacy(address),
        })
    }

    /// Wraps a resolved physical/logical connect target.
    #[must_use]
    pub fn resolved(target: ConnectTarget) -> Self {
        Self {
            identity: target.identity(),
            target: NameServerDialTarget::Resolved(target),
        }
    }

    /// Returns the selector, connection, latency, and circuit-breaker key.
    #[must_use]
    pub fn identity(&self) -> &CheetahString {
        &self.identity
    }

    /// Returns the resolved target, or `None` for a legacy address.
    #[must_use]
    pub fn connect_target(&self) -> Option<&ConnectTarget> {
        match &self.target {
            NameServerDialTarget::Legacy(_) => None,
            NameServerDialTarget::Resolved(target) => Some(target),
        }
    }

    pub(crate) fn compatibility_address(&self) -> CheetahString {
        match &self.target {
            NameServerDialTarget::Legacy(address) => address.clone(),
            NameServerDialTarget::Resolved(target) => CheetahString::from_string(target.socket_addr().to_string()),
        }
    }
}

/// The stable result of applying a complete NameServer endpoint snapshot.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NameServerEndpointDiff {
    /// Endpoints newly eligible for selection.
    pub added: Vec<NameServerEndpoint>,
    /// Endpoints whose connection and health state must be retained.
    pub unchanged: Vec<NameServerEndpoint>,
    /// Endpoints removed from new-request selection and scheduled for drain.
    pub removed: Vec<NameServerEndpoint>,
}

/// Computes an identity-based snapshot diff without publishing intermediate state.
#[must_use]
pub fn diff_name_server_endpoints(
    current: &[NameServerEndpoint],
    next: &[NameServerEndpoint],
) -> NameServerEndpointDiff {
    let current_by_id = current
        .iter()
        .map(|endpoint| (endpoint.identity(), endpoint))
        .collect::<std::collections::HashMap<_, _>>();
    let next_by_id = next
        .iter()
        .map(|endpoint| (endpoint.identity(), endpoint))
        .collect::<std::collections::HashMap<_, _>>();

    NameServerEndpointDiff {
        added: next
            .iter()
            .filter(|endpoint| !current_by_id.contains_key(endpoint.identity()))
            .cloned()
            .collect(),
        unchanged: next
            .iter()
            .filter(|endpoint| current_by_id.contains_key(endpoint.identity()))
            .cloned()
            .collect(),
        removed: current
            .iter()
            .filter(|endpoint| !next_by_id.contains_key(endpoint.identity()))
            .cloned()
            .collect(),
    }
}

fn validate_authority(authority: &str) -> RocketMQResult<()> {
    let authority = authority.trim();
    if authority.is_empty() {
        return Err(invalid_authority(authority, "must not be blank"));
    }
    let host = authority_host(authority);
    if host.is_empty() {
        return Err(invalid_authority(authority, "must contain a host"));
    }
    Ok(())
}

fn authority_host(authority: &str) -> &str {
    let authority = authority.trim();
    if let Some(rest) = authority.strip_prefix('[') {
        return rest.split_once(']').map_or(rest, |(host, _)| host);
    }
    authority
        .rsplit_once(':')
        .filter(|(_, port)| port.parse::<u16>().is_ok())
        .map_or(authority, |(host, _)| host)
}

fn invalid_authority(authority: &str, reason: impl Into<String>) -> RocketMQError {
    RocketMQError::ConfigInvalidValue {
        key: "transport.nameserver.authority",
        value: authority.to_string(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_identity_keeps_authority_when_physical_socket_matches() {
        let socket = "10.0.0.1:9876".parse().unwrap();
        let first = ConnectTarget::new(socket, "namesrv-a.default.svc:9876").unwrap();
        let second = ConnectTarget::new(socket, "namesrv-b.default.svc:9876").unwrap();

        assert_ne!(first.identity(), second.identity());
        assert_eq!(first.tls_server_name(), "namesrv-a.default.svc");
        assert_eq!(first.socket_addr(), socket);
    }

    #[test]
    fn snapshot_diff_is_identity_based_and_order_independent() {
        let first = NameServerEndpoint::resolved(
            ConnectTarget::new("10.0.0.1:9876".parse().unwrap(), "namesrv.default.svc:9876").unwrap(),
        );
        let second = NameServerEndpoint::resolved(
            ConnectTarget::new("10.0.0.2:9876".parse().unwrap(), "namesrv.default.svc:9876").unwrap(),
        );
        let third = NameServerEndpoint::resolved(
            ConnectTarget::new("10.0.0.3:9876".parse().unwrap(), "namesrv.default.svc:9876").unwrap(),
        );

        let diff = diff_name_server_endpoints(&[first.clone(), second.clone()], &[second.clone(), third.clone()]);
        assert_eq!(diff.added, vec![third]);
        assert_eq!(diff.unchanged, vec![second]);
        assert_eq!(diff.removed, vec![first]);
    }
}
