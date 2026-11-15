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

use super::AdminSourceConfig;
use crate::ConnectorError;
use hmac::{Hmac, KeyInit, Mac};
use rocketmq_admin_core::core::{broker::canonical_remoting_endpoint, nameserver_route::NameserverRouteView};
use rocketmq_sre_contracts::{ClusterId, TenantId, encode_lower_hex};
use serde::Deserialize;
use sha2::Sha256;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::sync::Arc;

pub(crate) const NAMESRV_ROUTE_CONFIG_ENV: &str = "ROCKETMQ_SRE_NAMESRV_ROUTE_CONFIG_JSON";
pub(crate) const NAMESRV_ROUTE_KEY_ENV: &str = "ROCKETMQ_SRE_NAMESRV_ROUTE_KEY_FILE";

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NamesrvRoutePeer {
    pub peer_id: String,
    pub instance_reference: String,
    pub endpoint: String,
}
impl std::fmt::Debug for NamesrvRoutePeer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamesrvRoutePeer")
            .field("peer_id", &self.peer_id)
            .field("endpoint", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub(crate) struct NamesrvRouteConfig {
    pub cluster_id: ClusterId,
    pub physical_cluster: String,
    pub revision: String,
    pub deployment_reference: String,
    pub expected_view: NameserverRouteView,
    pub peers: Vec<NamesrvRoutePeer>,
    pub key: Arc<NamesrvFingerprintKey>,
}
impl std::fmt::Debug for NamesrvRouteConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamesrvRouteConfig")
            .field("peer_count", &self.peers.len())
            .field("revision", &self.revision)
            .finish_non_exhaustive()
    }
}
#[cfg(test)]
impl NamesrvRouteConfig {
    pub(crate) fn fixture_for_tests(cluster_id: ClusterId) -> Self {
        Self {
            cluster_id,
            physical_cluster: "local".to_owned(),
            revision: "test-v1".to_owned(),
            deployment_reference: "test-deployment".to_owned(),
            expected_view: NameserverRouteView {
                cluster_test: false,
                order_message_enable: false,
                support_acting_master: false,
            },
            peers: vec![
                NamesrvRoutePeer {
                    peer_id: "ns-a".to_owned(),
                    instance_reference: "instance-a".to_owned(),
                    endpoint: "ns-a:9876".to_owned(),
                },
                NamesrvRoutePeer {
                    peer_id: "ns-b".to_owned(),
                    instance_reference: "instance-b".to_owned(),
                    endpoint: "ns-b:9876".to_owned(),
                },
            ],
            key: Arc::new(NamesrvFingerprintKey([17; 32])),
        }
    }
}
pub(crate) struct NamesrvFingerprintKey([u8; 32]);
impl std::fmt::Debug for NamesrvFingerprintKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NamesrvFingerprintKey([REDACTED])")
    }
}
impl NamesrvFingerprintKey {
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, ConnectorError> {
        Ok(Self(bytes.try_into().map_err(|_| invalid())?))
    }
    pub(crate) fn identify(
        &self,
        tenant: TenantId,
        cluster: ClusterId,
        role: &str,
        value: &str,
    ) -> Result<String, ConnectorError> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0).map_err(|_| invalid())?;
        for part in [
            "rocketmq-sre.namesrv-route.hmac-sha256-v1",
            &tenant.to_string(),
            &cluster.to_string(),
            role,
            value,
        ] {
            mac.update(&(part.len() as u64).to_be_bytes());
            mac.update(part.as_bytes());
        }
        Ok(format!("sha256:{}", encode_lower_hex(mac.finalize().into_bytes())))
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawProfile {
    cluster_id: ClusterId,
    physical_cluster: String,
    revision: String,
    deployment_reference: String,
    expected_view: NameserverRouteView,
    peers: Vec<NamesrvRoutePeer>,
}
fn opaque(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_alphabetic()
        && value
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'-' | b'_'))
}
fn invalid() -> ConnectorError {
    ConnectorError::configuration(
        "NameServer route observations require bounded exact-cluster direct peers and an independent persistent 32-byte fingerprint key",
    )
}

pub(super) fn load(
    clusters: &BTreeMap<String, ClusterId>,
    admin: Option<&AdminSourceConfig>,
) -> Result<BTreeMap<ClusterId, NamesrvRouteConfig>, ConnectorError> {
    let raw = match std::env::var(NAMESRV_ROUTE_CONFIG_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(BTreeMap::new()),
        Err(_) => return Err(invalid()),
    };
    let path = std::env::var_os(NAMESRV_ROUTE_KEY_ENV)
        .map(std::path::PathBuf::from)
        .filter(|path| path.is_absolute())
        .ok_or_else(invalid)?;
    let file = std::fs::File::open(&path).map_err(|_| invalid())?;
    let metadata = file.metadata().map_err(|_| invalid())?;
    if !metadata.is_file() || metadata.len() != 32 {
        return Err(invalid());
    }
    let key = Arc::new(read_key(file)?);
    parse(&raw, &clusters.values().copied().collect(), admin, key)
}
fn read_key(reader: impl Read) -> Result<NamesrvFingerprintKey, ConnectorError> {
    // The same opened regular-file handle is checked and read. Even a file
    // changed concurrently cannot allocate/read beyond 32 bytes plus EOF probe.
    let mut bytes = Vec::with_capacity(33);
    reader.take(33).read_to_end(&mut bytes).map_err(|_| invalid())?;
    NamesrvFingerprintKey::from_bytes(&bytes)
}
fn parse(
    raw: &str,
    clusters: &BTreeSet<ClusterId>,
    admin: Option<&AdminSourceConfig>,
    key: Arc<NamesrvFingerprintKey>,
) -> Result<BTreeMap<ClusterId, NamesrvRouteConfig>, ConnectorError> {
    if raw.len() > 32 * 1024 {
        return Err(invalid());
    }
    let admin = admin.ok_or_else(invalid)?;
    let allowed = admin
        .namesrv_addr
        .split(';')
        .map(|value| canonical_remoting_endpoint(value.trim()).ok_or_else(invalid))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let profiles: Vec<RawProfile> = serde_json::from_str(raw).map_err(|_| invalid())?;
    if profiles.len() > 16 {
        return Err(invalid());
    }
    let mut configured = BTreeMap::new();
    for profile in profiles {
        if !clusters.contains(&profile.cluster_id)
            || !opaque(&profile.revision)
            || !opaque(&profile.deployment_reference)
            || profile.expected_view.cluster_test
            || profile.physical_cluster.is_empty()
            || profile.physical_cluster.len() > 255
            || profile.physical_cluster.chars().any(char::is_control)
            || !(2..=4).contains(&profile.peers.len())
        {
            return Err(invalid());
        }
        let mut ids = BTreeSet::new();
        let mut refs = BTreeSet::new();
        let mut targets = BTreeSet::new();
        let mut peers = profile.peers;
        for peer in &mut peers {
            if !opaque(&peer.peer_id)
                || !opaque(&peer.instance_reference)
                || !ids.insert(peer.peer_id.clone())
                || !refs.insert(peer.instance_reference.clone())
            {
                return Err(invalid());
            }
            peer.endpoint = canonical_remoting_endpoint(&peer.endpoint).ok_or_else(invalid)?;
            if !direct_target(&peer.endpoint)
                || !allowed.contains(&peer.endpoint)
                || !targets.insert(peer.endpoint.clone())
            {
                return Err(invalid());
            }
        }
        peers.sort_by(|left, right| left.peer_id.cmp(&right.peer_id));
        let config = NamesrvRouteConfig {
            cluster_id: profile.cluster_id,
            physical_cluster: profile.physical_cluster,
            revision: profile.revision,
            deployment_reference: profile.deployment_reference,
            expected_view: profile.expected_view,
            peers,
            key: key.clone(),
        };
        if configured.insert(profile.cluster_id, config).is_some() {
            return Err(invalid());
        }
    }
    Ok(configured)
}

fn direct_target(endpoint: &str) -> bool {
    match endpoint.parse::<std::net::SocketAddr>().map(|endpoint| endpoint.ip()) {
        Ok(std::net::IpAddr::V4(ip)) => !ip.is_unspecified(),
        Ok(std::net::IpAddr::V6(ip)) => {
            !ip.is_unspecified() && !ip.to_ipv4_mapped().is_some_and(|ip| ip.is_unspecified())
        }
        // A configured DNS name remains an operator declaration. This check
        // neither resolves names nor changes the existing shared canonicalizer.
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn direct_peer_configuration_rejects_unspecified_ip_targets_without_changing_dns_or_aliases() {
        let cluster = ClusterId::new();
        let key = Arc::new(NamesrvFingerprintKey::from_bytes(&[17; 32]).unwrap());
        for target in [
            "0.0.0.0:9876",
            "[::]:9876",
            "[::ffff:0.0.0.0]:9876",
            "host-a:09876",
            "[0:0:0:0:0:0:0:1]:9876",
        ] {
            let admin = AdminSourceConfig {
                namesrv_addr: format!("{target};host-b:9876"),
                use_tls: false,
                request_timeout: std::time::Duration::from_secs(5),
                shutdown_timeout: std::time::Duration::from_secs(5),
                credentials: None,
            };
            let profile = serde_json::json!([{"cluster_id":cluster,"physical_cluster":"native-cluster","revision":"test-v1","deployment_reference":"fixture",
                "expected_view":{"cluster_test":false,"order_message_enable":true,"support_acting_master":false},
                "peers":[{"peer_id":"ns-a","instance_reference":"instance-a","endpoint":target},{"peer_id":"ns-b","instance_reference":"instance-b","endpoint":"host-b:9876"}]}]);
            let valid =
                !target.starts_with("0.0.0.0") && !target.starts_with("[::]") && !target.starts_with("[::ffff:");
            assert_eq!(
                parse(
                    &profile.to_string(),
                    &BTreeSet::from([cluster]),
                    Some(&admin),
                    key.clone()
                )
                .is_ok(),
                valid
            );
        }
    }
    #[test]
    fn fingerprint_key_length_and_growth_are_checked_with_a_bounded_read() {
        for length in [0, 31, 33, 1024 * 1024] {
            assert!(read_key(std::io::Cursor::new(vec![17; length])).is_err());
        }
        assert!(read_key(std::io::Cursor::new([17; 32])).is_ok());
        struct Probe {
            calls: std::rc::Rc<std::cell::Cell<usize>>,
        }
        impl Read for Probe {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                self.calls.set(self.calls.get() + buffer.len());
                buffer.fill(17);
                Ok(buffer.len())
            }
        }
        let calls = std::rc::Rc::new(std::cell::Cell::new(0));
        assert!(read_key(Probe { calls: calls.clone() }).is_err());
        assert_eq!(calls.get(), 33);
    }
    #[test]
    fn fingerprint_domain_is_persistent_scoped_and_independent_of_display_or_credentials() {
        let key = NamesrvFingerprintKey::from_bytes(&[17; 32]).unwrap();
        let tenant = TenantId::new();
        let cluster = ClusterId::new();
        let first = key
            .identify(tenant, cluster, "route-endpoint", "broker-a:10911")
            .unwrap();
        assert_eq!(
            first,
            NamesrvFingerprintKey::from_bytes(&[17; 32])
                .unwrap()
                .identify(tenant, cluster, "route-endpoint", "broker-a:10911")
                .unwrap()
        );
        assert_ne!(
            first,
            key.identify(tenant, cluster, "route-endpoint", "broker-b:10911")
                .unwrap()
        );
        assert_ne!(
            first,
            key.identify(tenant, ClusterId::new(), "route-endpoint", "broker-a:10911")
                .unwrap()
        );
        assert_ne!(
            first,
            key.identify(tenant, cluster, "nameserver-target", "broker-a:10911")
                .unwrap()
        );
        assert_ne!(
            first,
            NamesrvFingerprintKey::from_bytes(&[18; 32])
                .unwrap()
                .identify(tenant, cluster, "route-endpoint", "broker-a:10911")
                .unwrap()
        );
        assert!(!format!("{key:?}").contains("17"));
    }
}
