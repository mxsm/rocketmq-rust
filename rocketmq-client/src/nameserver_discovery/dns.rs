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

use std::future::Future;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use hickory_resolver::net::DnsError;
use hickory_resolver::net::NetError;
use hickory_resolver::proto::op::ResponseCode;
use hickory_resolver::proto::rr::RData;
use hickory_resolver::TokioResolver;

use super::DnsName;
use super::NameServerAuthority;
use super::NameServerDiscoveryConfig;
use super::NameServerSource;
use super::ResolvedNameServerEndpoint;

pub(super) type LookupFuture<'a> =
    Pin<Box<dyn Future<Output = Result<DnsFamilyResult, DnsResolutionError>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum AddressFamily {
    Ipv4,
    Ipv6,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DnsErrorKind {
    Empty,
    NxDomain,
    ServFail,
    Timeout,
    Other,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DnsResolutionError {
    kind: DnsErrorKind,
    message: Arc<str>,
}

impl DnsResolutionError {
    pub(crate) fn new(kind: DnsErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub(crate) fn kind(&self) -> DnsErrorKind {
        self.kind
    }
}

impl std::fmt::Display for DnsResolutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for DnsResolutionError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DnsFamilyResult {
    addresses: Vec<IpAddr>,
    valid_for: Duration,
}

impl DnsFamilyResult {
    #[cfg(any(test, feature = "test-support"))]
    pub(super) fn new(addresses: Vec<IpAddr>, valid_for: Duration) -> Self {
        Self { addresses, valid_for }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedDnsEndpoints {
    pub(crate) endpoints: Vec<ResolvedNameServerEndpoint>,
    pub(crate) ttl: Duration,
}

pub(crate) trait DnsLookup: Send + Sync {
    fn lookup(&self, host: &DnsName, family: AddressFamily) -> LookupFuture<'_>;
}

pub(crate) struct HickoryDnsLookup {
    resolver: Arc<TokioResolver>,
}

impl HickoryDnsLookup {
    pub(crate) fn new() -> Result<Self, DnsResolutionError> {
        let builder = TokioResolver::builder_tokio().map_err(classify_hickory_error)?;
        let resolver = builder.build().map_err(classify_hickory_error)?;
        Ok(Self {
            resolver: Arc::new(resolver),
        })
    }
}

impl DnsLookup for HickoryDnsLookup {
    fn lookup(&self, host: &DnsName, family: AddressFamily) -> LookupFuture<'_> {
        let resolver = self.resolver.clone();
        let query = format!("{}.", host.as_str());
        Box::pin(async move {
            let lookup = match family {
                AddressFamily::Ipv4 => resolver.ipv4_lookup(query).await,
                AddressFamily::Ipv6 => resolver.ipv6_lookup(query).await,
            }
            .map_err(classify_hickory_error)?;

            let addresses = lookup
                .answers()
                .iter()
                .filter_map(|record| match &record.data {
                    RData::A(address) => Some(IpAddr::V4(address.0)),
                    RData::AAAA(address) => Some(IpAddr::V6(address.0)),
                    _ => None,
                })
                .collect();
            let valid_for = lookup
                .valid_until()
                .saturating_duration_since(std::time::Instant::now());
            Ok(DnsFamilyResult { addresses, valid_for })
        })
    }
}

pub(crate) async fn resolve_dns(
    resolver: &dyn DnsLookup,
    config: &NameServerDiscoveryConfig,
) -> Result<ResolvedDnsEndpoints, DnsResolutionError> {
    let NameServerSource::Dns { host, port } = config.source() else {
        return Err(DnsResolutionError::new(
            DnsErrorKind::Other,
            "DNS resolver received a non-DNS source",
        ));
    };

    let (ipv4, ipv6) = tokio::join!(
        resolver.lookup(host, AddressFamily::Ipv4),
        resolver.lookup(host, AddressFamily::Ipv6)
    );

    let mut addresses = Vec::new();
    let mut successful_ttls = Vec::new();
    let mut failures = Vec::new();
    for result in [ipv4, ipv6] {
        match result {
            Ok(result) if !result.addresses.is_empty() => {
                addresses.extend(result.addresses);
                successful_ttls.push(result.valid_for);
            }
            Ok(_) => failures.push(DnsResolutionError::new(
                DnsErrorKind::Empty,
                "DNS family lookup returned no addresses",
            )),
            Err(error) => failures.push(error),
        }
    }

    addresses.sort_unstable();
    addresses.dedup();
    addresses.truncate(config.endpoint_limit());
    if addresses.is_empty() {
        return Err(preferred_error(failures));
    }

    let ttl = successful_ttls
        .into_iter()
        .min()
        .unwrap_or_else(|| config.min_refresh())
        .clamp(config.min_refresh(), config.max_refresh());
    let authority = NameServerAuthority::parse(format!("{}:{}", host.as_str(), port.get()))
        .map_err(|error| DnsResolutionError::new(DnsErrorKind::Other, error.to_string()))?;
    let endpoints = addresses
        .into_iter()
        .map(|address| ResolvedNameServerEndpoint::new(authority.clone(), SocketAddr::new(address, port.get())))
        .collect();

    Ok(ResolvedDnsEndpoints { endpoints, ttl })
}

fn preferred_error(errors: Vec<DnsResolutionError>) -> DnsResolutionError {
    errors
        .into_iter()
        .max_by_key(|error| match error.kind() {
            DnsErrorKind::Timeout => 5,
            DnsErrorKind::ServFail => 4,
            DnsErrorKind::NxDomain => 3,
            DnsErrorKind::Other => 2,
            DnsErrorKind::Empty => 1,
        })
        .unwrap_or_else(|| DnsResolutionError::new(DnsErrorKind::Empty, "DNS lookup returned no addresses"))
}

fn classify_hickory_error(error: NetError) -> DnsResolutionError {
    let kind = match &error {
        NetError::Timeout => DnsErrorKind::Timeout,
        NetError::Dns(DnsError::ResponseCode(ResponseCode::ServFail)) => DnsErrorKind::ServFail,
        NetError::Dns(DnsError::ResponseCode(ResponseCode::NXDomain)) => DnsErrorKind::NxDomain,
        NetError::Dns(DnsError::NoRecordsFound(no_records)) if no_records.response_code == ResponseCode::NXDomain => {
            DnsErrorKind::NxDomain
        }
        NetError::Dns(DnsError::NoRecordsFound(_)) => DnsErrorKind::Empty,
        _ => DnsErrorKind::Other,
    };
    DnsResolutionError::new(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;

    use super::*;

    struct FakeDnsLookup {
        results: HashMap<AddressFamily, Result<DnsFamilyResult, DnsResolutionError>>,
    }

    impl DnsLookup for FakeDnsLookup {
        fn lookup(&self, _host: &DnsName, family: AddressFamily) -> LookupFuture<'_> {
            let result = self.results[&family].clone();
            Box::pin(async move { result })
        }
    }

    fn config(min: Duration, max: Duration) -> NameServerDiscoveryConfig {
        NameServerDiscoveryConfig::new(NameServerSource::dns("namesrv.default.svc", 9876).unwrap())
            .with_refresh_bounds(min, max)
            .unwrap()
    }

    #[tokio::test]
    async fn resolves_a_and_aaaa_with_stable_deduplication_and_minimum_ttl() {
        let resolver = FakeDnsLookup {
            results: HashMap::from([
                (
                    AddressFamily::Ipv4,
                    Ok(DnsFamilyResult::new(
                        vec![
                            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
                            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
                            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
                        ],
                        Duration::from_secs(45),
                    )),
                ),
                (
                    AddressFamily::Ipv6,
                    Ok(DnsFamilyResult::new(
                        vec![IpAddr::V6(Ipv6Addr::LOCALHOST)],
                        Duration::from_secs(30),
                    )),
                ),
            ]),
        };

        let resolved = resolve_dns(&resolver, &config(Duration::from_secs(5), Duration::from_secs(60)))
            .await
            .unwrap();

        assert_eq!(resolved.ttl, Duration::from_secs(30));
        assert_eq!(resolved.endpoints.len(), 3);
        assert_eq!(resolved.endpoints[0].socket_addr(), "10.0.0.1:9876".parse().unwrap());
        assert_eq!(resolved.endpoints[1].socket_addr(), "10.0.0.2:9876".parse().unwrap());
        assert_eq!(resolved.endpoints[2].socket_addr(), "[::1]:9876".parse().unwrap());
    }

    #[tokio::test]
    async fn keeps_successful_family_and_clamps_ttl() {
        let resolver = FakeDnsLookup {
            results: HashMap::from([
                (
                    AddressFamily::Ipv4,
                    Ok(DnsFamilyResult::new(
                        vec![IpAddr::V4(Ipv4Addr::LOCALHOST)],
                        Duration::from_secs(1),
                    )),
                ),
                (
                    AddressFamily::Ipv6,
                    Err(DnsResolutionError::new(DnsErrorKind::Timeout, "timed out")),
                ),
            ]),
        };

        let resolved = resolve_dns(&resolver, &config(Duration::from_secs(5), Duration::from_secs(60)))
            .await
            .unwrap();

        assert_eq!(resolved.ttl, Duration::from_secs(5));
        assert_eq!(resolved.endpoints.len(), 1);
    }

    #[tokio::test]
    async fn supports_aaaa_only_and_clamps_maximum_ttl() {
        let resolver = FakeDnsLookup {
            results: HashMap::from([
                (
                    AddressFamily::Ipv4,
                    Ok(DnsFamilyResult::new(Vec::new(), Duration::from_secs(120))),
                ),
                (
                    AddressFamily::Ipv6,
                    Ok(DnsFamilyResult::new(
                        vec![IpAddr::V6(Ipv6Addr::LOCALHOST)],
                        Duration::from_secs(120),
                    )),
                ),
            ]),
        };

        let resolved = resolve_dns(&resolver, &config(Duration::from_secs(5), Duration::from_secs(60)))
            .await
            .unwrap();
        assert_eq!(resolved.ttl, Duration::from_secs(60));
        assert_eq!(resolved.endpoints[0].socket_addr(), "[::1]:9876".parse().unwrap());
    }

    #[tokio::test]
    async fn caps_resolved_endpoint_count_at_64() {
        let resolver = FakeDnsLookup {
            results: HashMap::from([
                (
                    AddressFamily::Ipv4,
                    Ok(DnsFamilyResult::new(
                        (1..=70).map(|last| IpAddr::V4(Ipv4Addr::new(10, 0, 0, last))).collect(),
                        Duration::from_secs(30),
                    )),
                ),
                (
                    AddressFamily::Ipv6,
                    Ok(DnsFamilyResult::new(Vec::new(), Duration::from_secs(30))),
                ),
            ]),
        };

        let resolved = resolve_dns(&resolver, &config(Duration::from_secs(5), Duration::from_secs(60)))
            .await
            .unwrap();
        assert_eq!(resolved.endpoints.len(), 64);
    }

    #[test]
    fn classifies_hickory_nxdomain_servfail_and_timeout_errors() {
        use hickory_resolver::net::NoRecords;
        use hickory_resolver::proto::op::Query;

        let nxdomain = NetError::Dns(DnsError::NoRecordsFound(NoRecords::new(
            Query::default(),
            ResponseCode::NXDomain,
        )));
        assert_eq!(classify_hickory_error(nxdomain).kind(), DnsErrorKind::NxDomain);
        assert_eq!(
            classify_hickory_error(NetError::Dns(DnsError::ResponseCode(ResponseCode::ServFail))).kind(),
            DnsErrorKind::ServFail
        );
        assert_eq!(classify_hickory_error(NetError::Timeout).kind(), DnsErrorKind::Timeout);
    }

    #[tokio::test]
    async fn classifies_empty_dual_family_result() {
        let resolver = FakeDnsLookup {
            results: HashMap::from([
                (
                    AddressFamily::Ipv4,
                    Ok(DnsFamilyResult::new(Vec::new(), Duration::from_secs(30))),
                ),
                (
                    AddressFamily::Ipv6,
                    Ok(DnsFamilyResult::new(Vec::new(), Duration::from_secs(30))),
                ),
            ]),
        };

        let error = resolve_dns(&resolver, &config(Duration::from_secs(5), Duration::from_secs(60)))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), DnsErrorKind::Empty);
    }
}
