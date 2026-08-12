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

use std::fmt;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

use crate::config_support::name_server_target::parse_legacy_namesrv_addr;

const DEFAULT_MIN_REFRESH: Duration = Duration::from_secs(5);
const DEFAULT_MAX_REFRESH: Duration = Duration::from_secs(60);
const DEFAULT_STALE_MAX: Duration = Duration::from_secs(5 * 60);
const DEFAULT_ENDPOINT_LIMIT: usize = 64;
const MAX_ENDPOINT_LIMIT: usize = 64;

/// A normalized ASCII DNS name used as a logical NameServer authority.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DnsName(Arc<str>);

impl DnsName {
    /// Parses and normalizes a DNS name.
    pub fn parse(value: impl AsRef<str>) -> RocketMQResult<Self> {
        let normalized = value.as_ref().trim().trim_end_matches('.').to_ascii_lowercase();
        validate_dns_name(&normalized)?;
        Ok(Self(Arc::from(normalized)))
    }

    /// Returns the canonical DNS name without a trailing dot.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DnsName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A canonical `host:port` NameServer authority.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NameServerAuthority(Arc<str>);

impl NameServerAuthority {
    /// Parses exactly one canonical NameServer authority.
    pub fn parse(value: impl AsRef<str>) -> RocketMQResult<Self> {
        let parsed = parse_legacy_namesrv_addr(value.as_ref())?;
        let addresses = parsed.into_addresses();
        if addresses.len() != 1 {
            return Err(invalid_config(
                "nameserver_discovery.static",
                value.as_ref(),
                "must contain exactly one host:port authority",
            ));
        }
        Ok(Self(Arc::from(addresses[0].as_str())))
    }

    /// Returns the canonical authority.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NameServerAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Supported NameServer discovery sources.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NameServerSource {
    /// A fixed canonical list. This preserves existing transport behavior.
    Static(Vec<NameServerAuthority>),
    /// DNS A/AAAA discovery for a logical authority.
    Dns { host: DnsName, port: NonZeroU16 },
}

impl NameServerSource {
    /// Creates a DNS A/AAAA source.
    pub fn dns(host: impl AsRef<str>, port: u16) -> RocketMQResult<Self> {
        let port = NonZeroU16::new(port)
            .ok_or_else(|| invalid_config("nameserver_discovery.port", port, "must be between 1 and 65535"))?;
        Ok(Self::Dns {
            host: DnsName::parse(host)?,
            port,
        })
    }

    /// Creates a validated static source.
    pub fn static_endpoints(endpoints: Vec<NameServerAuthority>) -> RocketMQResult<Self> {
        if endpoints.is_empty() {
            return Err(invalid_config(
                "nameserver_discovery.static",
                "[]",
                "must contain at least one endpoint",
            ));
        }
        if endpoints.len() > MAX_ENDPOINT_LIMIT {
            return Err(invalid_config(
                "nameserver_discovery.static",
                endpoints.len(),
                "must not contain more than 64 endpoints",
            ));
        }
        Ok(Self::Static(endpoints))
    }
}

/// Typed NameServer discovery configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameServerDiscoveryConfig {
    source: NameServerSource,
    min_refresh: Duration,
    max_refresh: Duration,
    stale_max: Duration,
    endpoint_limit: usize,
}

impl NameServerDiscoveryConfig {
    /// Creates a discovery configuration with cloud-native defaults.
    #[must_use]
    pub fn new(source: NameServerSource) -> Self {
        Self {
            source,
            min_refresh: DEFAULT_MIN_REFRESH,
            max_refresh: DEFAULT_MAX_REFRESH,
            stale_max: DEFAULT_STALE_MAX,
            endpoint_limit: DEFAULT_ENDPOINT_LIMIT,
        }
    }

    /// Sets the inclusive refresh interval clamp.
    pub fn with_refresh_bounds(mut self, min: Duration, max: Duration) -> RocketMQResult<Self> {
        if min.is_zero() || max < min {
            return Err(invalid_config(
                "nameserver_discovery.refresh",
                format!("{min:?}..={max:?}"),
                "minimum must be non-zero and no greater than maximum",
            ));
        }
        self.min_refresh = min;
        self.max_refresh = max;
        Ok(self)
    }

    /// Sets how long the last-known-good endpoint set may remain usable.
    pub fn with_stale_max(mut self, stale_max: Duration) -> RocketMQResult<Self> {
        if stale_max.is_zero() {
            return Err(invalid_config(
                "nameserver_discovery.stale_max",
                "0",
                "must be non-zero",
            ));
        }
        self.stale_max = stale_max;
        Ok(self)
    }

    /// Sets the resolved endpoint cap. Values above 64 are rejected.
    pub fn with_endpoint_limit(mut self, endpoint_limit: usize) -> RocketMQResult<Self> {
        if !(1..=MAX_ENDPOINT_LIMIT).contains(&endpoint_limit) {
            return Err(invalid_config(
                "nameserver_discovery.endpoint_limit",
                endpoint_limit,
                "must be between 1 and 64",
            ));
        }
        self.endpoint_limit = endpoint_limit;
        Ok(self)
    }

    /// Returns the configured source.
    #[must_use]
    pub fn source(&self) -> &NameServerSource {
        &self.source
    }

    #[must_use]
    pub(crate) fn min_refresh(&self) -> Duration {
        self.min_refresh
    }

    #[must_use]
    pub(crate) fn max_refresh(&self) -> Duration {
        self.max_refresh
    }

    #[must_use]
    pub(crate) fn stale_max(&self) -> Duration {
        self.stale_max
    }

    #[must_use]
    pub(crate) fn endpoint_limit(&self) -> usize {
        self.endpoint_limit
    }

    pub(crate) fn validate(&self) -> RocketMQResult<()> {
        match &self.source {
            NameServerSource::Static(endpoints) if endpoints.is_empty() => Err(invalid_config(
                "nameserver_discovery.static",
                "[]",
                "must contain at least one endpoint",
            )),
            NameServerSource::Static(endpoints) if endpoints.len() > self.endpoint_limit => Err(invalid_config(
                "nameserver_discovery.static",
                endpoints.len(),
                "exceeds the configured endpoint limit",
            )),
            _ => Ok(()),
        }
    }

    pub(crate) fn fingerprint(&self) -> String {
        let source = match &self.source {
            NameServerSource::Static(endpoints) => {
                let mut endpoints = endpoints.iter().map(NameServerAuthority::as_str).collect::<Vec<_>>();
                endpoints.sort_unstable();
                endpoints.dedup();
                format!("static:{}", endpoints.join(";"))
            }
            NameServerSource::Dns { host, port } => format!("dns:{}:{}", host.as_str(), port.get()),
        };
        format!(
            "{source}|refresh={}-{}|stale={}|limit={}",
            self.min_refresh.as_millis(),
            self.max_refresh.as_millis(),
            self.stale_max.as_millis(),
            self.endpoint_limit
        )
    }

    pub(crate) fn static_canonical(&self) -> Option<String> {
        let NameServerSource::Static(endpoints) = &self.source else {
            return None;
        };
        let mut seen = std::collections::HashSet::new();
        Some(
            endpoints
                .iter()
                .filter(|endpoint| seen.insert(endpoint.as_str()))
                .map(NameServerAuthority::as_str)
                .collect::<Vec<_>>()
                .join(";"),
        )
    }
}

fn validate_dns_name(value: &str) -> RocketMQResult<()> {
    if value.is_empty() || value.len() > 253 {
        return Err(invalid_config(
            "nameserver_discovery.dns_name",
            value,
            "must contain between 1 and 253 ASCII characters",
        ));
    }
    for label in value.split('.') {
        if label.is_empty() || label.len() > 63 {
            return Err(invalid_config(
                "nameserver_discovery.dns_name",
                value,
                "each DNS label must contain between 1 and 63 characters",
            ));
        }
        if !label
            .bytes()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, b'-' | b'_'))
            || label.starts_with('-')
            || label.ends_with('-')
        {
            return Err(invalid_config(
                "nameserver_discovery.dns_name",
                value,
                "contains an invalid DNS label",
            ));
        }
    }
    Ok(())
}

fn invalid_config(key: &'static str, value: impl ToString, reason: impl Into<String>) -> RocketMQError {
    RocketMQError::ConfigInvalidValue {
        key,
        value: value.to_string(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dns_source_normalizes_name_and_rejects_invalid_port() {
        let source = NameServerSource::dns("NameSrv.Default.Svc.", 9876).expect("DNS source");
        assert!(matches!(
            source,
            NameServerSource::Dns { ref host, port }
                if host.as_str() == "namesrv.default.svc" && port.get() == 9876
        ));
        assert!(NameServerSource::dns("namesrv.default.svc", 0).is_err());
    }

    #[test]
    fn discovery_fingerprint_is_stable_for_equivalent_static_sets() {
        let first = NameServerDiscoveryConfig::new(
            NameServerSource::static_endpoints(vec![
                NameServerAuthority::parse("ns-b:9876").unwrap(),
                NameServerAuthority::parse("ns-a:9876").unwrap(),
            ])
            .unwrap(),
        );
        let second = NameServerDiscoveryConfig::new(
            NameServerSource::static_endpoints(vec![
                NameServerAuthority::parse("NS-A:9876").unwrap(),
                NameServerAuthority::parse("ns-b:9876").unwrap(),
            ])
            .unwrap(),
        );
        assert_eq!(first.fingerprint(), second.fingerprint());
    }
}
