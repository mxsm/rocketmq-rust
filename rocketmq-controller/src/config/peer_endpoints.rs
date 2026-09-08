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

use std::collections::HashSet;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use super::ControllerConfig;

/// An advertised peer endpoint that retains DNS names across Pod replacement.
/// Resolution belongs to the transport when connecting, never to configuration loading.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ControllerPeerEndpoint {
    /// Stable Controller member identity.
    pub id: u64,
    /// DNS name or IP address with an explicit port; IPv6 addresses use brackets.
    pub addr: String,
}

impl ControllerConfig {
    /// Returns the configured bootstrap membership, retaining DNS endpoints when provided.
    pub fn raft_member_endpoints(&self) -> Vec<ControllerPeerEndpoint> {
        if !self.raft_peer_endpoints.is_empty() {
            return self.raft_peer_endpoints.clone();
        }
        self.raft_peers
            .iter()
            .map(|peer| ControllerPeerEndpoint {
                id: peer.id,
                addr: peer.addr.to_string(),
            })
            .collect()
    }

    /// Returns the broker-facing endpoint for leader discovery without resolving DNS.
    pub fn controller_endpoint_for(&self, node_id: u64) -> Option<String> {
        if !self.controller_peer_endpoints.is_empty() {
            return self
                .controller_peer_endpoints
                .iter()
                .find(|peer| peer.id == node_id)
                .map(|peer| peer.addr.clone());
        }
        self.controller_addr_for(node_id).map(|addr| addr.to_string())
    }

    /// Returns the broker-facing discovery endpoints for the configured members.
    pub fn controller_advertised_endpoints(&self) -> Vec<String> {
        if !self.controller_peer_endpoints.is_empty() {
            return self
                .controller_peer_endpoints
                .iter()
                .map(|peer| peer.addr.clone())
                .collect();
        }
        self.controller_peer_addrs()
            .into_iter()
            .map(|addr| addr.to_string())
            .collect()
    }

    pub(super) fn validate_peer_endpoints(&self) -> Result<(), String> {
        if self.raft_peer_endpoints.is_empty() && self.controller_peer_endpoints.is_empty() {
            return Ok(());
        }
        if !self.raft_peers.is_empty() || !self.controller_peers.is_empty() {
            return Err("DNS peer endpoints and legacy peer lists are mutually exclusive".into());
        }
        let Some(bind) = self.raft_listen_addr else {
            return Err("peer endpoints require an explicit raftListenAddr".into());
        };
        if bind.port() == 0 || bind == self.listen_addr {
            return Err("raftListenAddr must have a nonzero port and a separate listener".into());
        }
        let raft = validate_members(&self.raft_peer_endpoints)?;
        let remoting = validate_members(&self.controller_peer_endpoints)?;
        if raft != remoting || !raft.contains(&self.node_id) {
            return Err("Raft and Controller endpoint memberships must match and include the local node".into());
        }
        Ok(())
    }
}

fn validate_members(peers: &[ControllerPeerEndpoint]) -> Result<HashSet<u64>, String> {
    let mut ids = HashSet::new();
    let mut addresses = HashSet::new();
    for peer in peers {
        if peer.id == 0 || !ids.insert(peer.id) || !addresses.insert(peer.addr.as_str()) || !valid_address(&peer.addr) {
            return Err("peer endpoints require unique nonzero identities and valid unique host:port addresses".into());
        }
    }
    Ok(ids)
}

fn valid_address(address: &str) -> bool {
    if let Ok(socket) = address.parse::<SocketAddr>() {
        return socket.port() != 0 && !socket.ip().is_unspecified();
    }
    let Some((host, port)) = address.rsplit_once(':') else {
        return false;
    };
    if !port.parse::<u16>().is_ok_and(|port| port != 0) || host.len() > 253 {
        return false;
    }
    host.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && label.as_bytes()[0].is_ascii_alphanumeric()
            && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
            && label.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ControllerConfig {
        let peers = |port| {
            (1..=3)
                .map(|id| ControllerPeerEndpoint {
                    id,
                    addr: format!(
                        "release-controller-{}.release-controller-peer.ns.svc.cluster.local:{port}",
                        id - 1
                    ),
                })
                .collect()
        };
        let mut config = ControllerConfig::default();
        config.node_id = 2;
        config.listen_addr = "0.0.0.0:9878".parse().unwrap();
        config.raft_listen_addr = Some("0.0.0.0:9879".parse().unwrap());
        config.raft_peer_endpoints = peers(9879);
        config.controller_peer_endpoints = peers(9878);
        config
    }

    #[test]
    fn dns_membership_survives_serialization_without_resolution() {
        let config = config();
        config.validate().unwrap();
        let encoded = serde_json::to_vec(&config).unwrap();
        let decoded: ControllerConfig = serde_json::from_slice(&encoded).unwrap();
        decoded.validate().unwrap();
        assert_eq!(decoded.raft_member_endpoints(), config.raft_member_endpoints());
        assert_eq!(decoded.local_raft_addr(), "0.0.0.0:9879".parse().unwrap());
        assert_eq!(
            decoded.controller_endpoint_for(2).unwrap(),
            "release-controller-1.release-controller-peer.ns.svc.cluster.local:9878"
        );
        assert_eq!(decoded.controller_advertised_endpoints().len(), 3);
    }

    #[test]
    fn conflicting_incomplete_or_invalid_membership_is_rejected() {
        let mut missing_local = config();
        missing_local.node_id = 4;
        assert!(missing_local.validate().is_err());
        let mut mismatched = config();
        mismatched.controller_peer_endpoints.pop();
        assert!(mismatched.validate().is_err());
        let mut duplicate = config();
        duplicate.raft_peer_endpoints[1].id = 1;
        assert!(duplicate.validate().is_err());
        let mut legacy = config();
        legacy.raft_peers.push(super::super::RaftPeer {
            id: 1,
            addr: "127.0.0.1:9879".parse().unwrap(),
        });
        assert!(legacy.validate().is_err());
        for invalid in [
            "host:0",
            "user@host:9879",
            "http://host:9879",
            "bad host:9879",
            "0.0.0.0:9879",
            "host:65536",
        ] {
            let mut config = config();
            config.raft_peer_endpoints[0].addr = invalid.into();
            assert!(config.validate().is_err(), "{invalid}");
        }
    }
}
