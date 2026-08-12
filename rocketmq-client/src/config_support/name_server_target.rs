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

use std::collections::HashSet;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::str::FromStr;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct NameServerTarget {
    authority: CheetahString,
}

impl NameServerTarget {
    #[inline]
    pub(crate) fn authority(&self) -> &CheetahString {
        &self.authority
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedNameServerTargets {
    targets: Vec<NameServerTarget>,
    canonical: CheetahString,
}

impl NormalizedNameServerTargets {
    #[inline]
    pub(crate) fn canonical(&self) -> &str {
        self.canonical.as_str()
    }

    pub(crate) fn authorities(&self) -> Vec<&str> {
        self.targets.iter().map(|target| target.authority().as_str()).collect()
    }

    pub(crate) fn into_addresses(self) -> Vec<CheetahString> {
        self.targets.into_iter().map(|target| target.authority).collect()
    }
}

pub(crate) fn parse_legacy_namesrv_addr(value: &str) -> RocketMQResult<NormalizedNameServerTargets> {
    let value = normalize_java_endpoint(value.trim());
    let mut seen = HashSet::new();
    let mut targets = Vec::new();

    for raw_target in value.split(';').map(str::trim).filter(|target| !target.is_empty()) {
        let authority = normalize_authority(raw_target)?;
        if seen.insert(authority.clone()) {
            targets.push(NameServerTarget {
                authority: authority.into(),
            });
        }
    }

    if targets.is_empty() {
        return Err(invalid_namesrv_addr(
            value,
            "must contain at least one host:port target",
        ));
    }

    let canonical = targets
        .iter()
        .map(|target| target.authority().as_str())
        .collect::<Vec<_>>()
        .join(";")
        .into();
    Ok(NormalizedNameServerTargets { targets, canonical })
}

fn normalize_java_endpoint(value: &str) -> &str {
    value
        .strip_prefix("http://")
        .and_then(|_| value.rsplit('/').next())
        .unwrap_or(value)
}

fn normalize_authority(value: &str) -> RocketMQResult<String> {
    if value.contains("//") || value.contains(['/', '?', '#', '@']) || value.chars().any(char::is_whitespace) {
        return Err(invalid_namesrv_addr(
            value,
            "contains unsupported URI or whitespace characters",
        ));
    }

    if let Ok(socket_addr) = SocketAddr::from_str(value) {
        if socket_addr.port() == 0 {
            return Err(invalid_namesrv_addr(value, "port must be between 1 and 65535"));
        }
        return Ok(socket_addr.to_string());
    }

    let Some((host, port)) = value.rsplit_once(':') else {
        return Err(invalid_namesrv_addr(value, "port is required"));
    };
    if host.is_empty() || host.contains(':') || host.starts_with('[') || host.ends_with(']') {
        return Err(invalid_namesrv_addr(
            value,
            "IPv6 targets must use bracketed [address]:port form",
        ));
    }
    let port = port
        .parse::<u16>()
        .ok()
        .and_then(NonZeroU16::new)
        .ok_or_else(|| invalid_namesrv_addr(value, "port must be between 1 and 65535"))?;

    if IpAddr::from_str(host).is_ok() {
        return Ok(format!("{host}:{port}"));
    }

    let host = host.trim_end_matches('.').to_ascii_lowercase();
    validate_dns_name(&host).map_err(|reason| invalid_namesrv_addr(value, reason))?;
    Ok(format!("{host}:{port}"))
}

fn validate_dns_name(host: &str) -> Result<(), &'static str> {
    if host.is_empty() || host.len() > 253 {
        return Err("DNS name must contain between 1 and 253 characters");
    }
    for label in host.split('.') {
        if label.is_empty() || label.len() > 63 {
            return Err("DNS labels must contain between 1 and 63 characters");
        }
        if !label
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, '-' | '_'))
        {
            return Err("DNS labels may only contain ASCII letters, digits, '-' or '_'");
        }
        if label.starts_with('-') || label.ends_with('-') {
            return Err("DNS labels must not start or end with '-'");
        }
    }
    Ok(())
}

fn invalid_namesrv_addr(value: &str, reason: impl Into<String>) -> RocketMQError {
    RocketMQError::ConfigInvalidValue {
        key: "namesrv_addr",
        value: value.to_owned(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_legacy_nameserver_targets() {
        let ipv4 = parse_legacy_namesrv_addr("127.0.0.1:9876").expect("IPv4 target");
        assert_eq!(ipv4.canonical(), "127.0.0.1:9876");

        let dns = parse_legacy_namesrv_addr("NameSrv.Default.Svc.:9876").expect("DNS target");
        assert_eq!(dns.canonical(), "namesrv.default.svc:9876");

        let ipv6 = parse_legacy_namesrv_addr("[::1]:9876").expect("IPv6 target");
        assert_eq!(ipv6.canonical(), "[::1]:9876");
    }

    #[test]
    fn cleans_semicolon_lists_and_stably_deduplicates_targets() {
        let parsed = parse_legacy_namesrv_addr(" ns-a:9876 ; ; ns-b:9876 ; ns-a:9876 ").expect("clean target list");

        assert_eq!(parsed.canonical(), "ns-a:9876;ns-b:9876");
        assert_eq!(parsed.authorities(), ["ns-a:9876", "ns-b:9876"]);
    }

    #[test]
    fn normalizes_java_http_endpoint_shape_without_fetching_http() {
        let parsed = parse_legacy_namesrv_addr("http://MQ_INST_x.example.com:9876").expect("Java-compatible endpoint");

        assert_eq!(parsed.canonical(), "mq_inst_x.example.com:9876");
    }

    #[test]
    fn rejects_malformed_or_ambiguous_targets() {
        for value in [
            "",
            "; ;",
            "namesrv",
            "namesrv:0",
            "namesrv:65536",
            "::1:9876",
            "https://namesrv:9876",
            "http://user@namesrv:9876",
            "http://namesrv:9876/path",
            "namesrv:9876?zone=a",
        ] {
            let error = parse_legacy_namesrv_addr(value).expect_err(value);
            assert!(
                matches!(
                    error,
                    RocketMQError::ConfigInvalidValue {
                        key: "namesrv_addr",
                        ..
                    }
                ),
                "unexpected error for {value}: {error:?}"
            );
        }
    }
}
