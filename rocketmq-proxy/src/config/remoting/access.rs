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

//! Resolution of the address the Proxy advertises to Remoting clients.

use std::net::IpAddr;
use std::net::SocketAddr;

use rocketmq_transport::api::NetworkUtil;

use crate::error::canonical;
use crate::ProxyError;
use crate::ProxyResult;

const ACCESS_ADDR_KEY: &str = "remoting.accessAddr";

/// Configured access address, split into a host and an optional port.
#[derive(Debug, PartialEq, Eq)]
enum AccessAddr {
    Socket(SocketAddr),
    Ip(IpAddr),
    Host { host: String, port: Option<u16> },
}

pub(super) fn validate(access_addr: Option<&str>) -> ProxyResult<()> {
    configured(access_addr).map(|_| ())
}

/// Resolves the advertised `host:port` for a listener bound to `listen`.
///
/// A configured access address wins, and a bare host takes the listen port. Without one,
/// a listener bound to a concrete address advertises it; a listener bound to all
/// interfaces advertises this host's address, as Java Proxy does.
pub(super) fn resolve(access_addr: Option<&str>, listen: SocketAddr) -> ProxyResult<String> {
    let endpoint = match configured(access_addr)? {
        Some(AccessAddr::Socket(addr)) => addr.to_string(),
        Some(AccessAddr::Ip(ip)) => SocketAddr::new(ip, listen.port()).to_string(),
        Some(AccessAddr::Host { host, port }) => format!("{host}:{}", port.unwrap_or(listen.port())),
        None if listen.ip().is_unspecified() => {
            let ip = NetworkUtil::get_local_address()
                .and_then(|address| address.parse::<IpAddr>().ok())
                .ok_or_else(|| {
                    invalid("no local address found for a wildcard listen address; set remoting.accessAddr")
                })?;
            SocketAddr::new(ip, listen.port()).to_string()
        }
        None => listen.to_string(),
    };
    Ok(endpoint)
}

fn configured(access_addr: Option<&str>) -> ProxyResult<Option<AccessAddr>> {
    let Some(value) = access_addr.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    parse(value).map(Some)
}

fn parse(value: &str) -> ProxyResult<AccessAddr> {
    if let Ok(addr) = value.parse::<SocketAddr>() {
        if addr.port() == 0 {
            return Err(invalid("access port must be non-zero"));
        }
        return Ok(AccessAddr::Socket(addr));
    }
    if let Ok(ip) = value.parse::<IpAddr>() {
        return Ok(AccessAddr::Ip(ip));
    }
    let (host, port) = match value.rsplit_once(':') {
        Some((host, port)) => {
            let port = port
                .parse::<u16>()
                .ok()
                .filter(|port| *port != 0)
                .ok_or_else(|| invalid("access port must be a non-zero integer"))?;
            (host, Some(port))
        }
        None => (value, None),
    };
    if !is_host_name(host) {
        return Err(invalid(
            "access address must be an IP address or host name, with an optional port",
        ));
    }
    Ok(AccessAddr::Host {
        host: host.to_owned(),
        port,
    })
}

fn is_host_name(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        })
}

fn invalid(reason: &'static str) -> ProxyError {
    canonical::configuration_invalid(ACCESS_ADDR_KEY, reason).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn listen(addr: &str) -> SocketAddr {
        addr.parse().expect("test listen address")
    }

    #[test]
    fn unset_access_address_advertises_a_concrete_listen_address() {
        for blank in [None, Some(""), Some("  ")] {
            assert_eq!(
                resolve(blank, listen("127.0.0.1:8080")).expect("resolve"),
                "127.0.0.1:8080"
            );
        }
        assert_eq!(resolve(None, listen("[::1]:8081")).expect("resolve"), "[::1]:8081");
    }

    #[test]
    fn unset_access_address_replaces_a_wildcard_listen_address() {
        // Hosts without a usable interface report the configuration error instead.
        if let Ok(endpoint) = resolve(None, listen("0.0.0.0:8080")) {
            let advertised = endpoint.parse::<SocketAddr>().expect("advertised socket address");
            assert!(!advertised.ip().is_unspecified());
            assert_eq!(advertised.port(), 8080);
        }
    }

    #[test]
    fn configured_access_address_keeps_or_overrides_the_listen_port() {
        let wildcard = listen("0.0.0.0:8080");
        let cases = [
            ("10.0.0.5", "10.0.0.5:8080"),
            ("10.0.0.5:18080", "10.0.0.5:18080"),
            ("::1", "[::1]:8080"),
            ("[::1]:18080", "[::1]:18080"),
            ("proxy.example.com", "proxy.example.com:8080"),
            ("proxy-0.proxy:18080", "proxy-0.proxy:18080"),
            ("rocketmq_proxy_1", "rocketmq_proxy_1:8080"),
            (" localhost ", "localhost:8080"),
        ];
        for (configured, expected) in cases {
            assert_eq!(
                resolve(Some(configured), wildcard).expect("resolve"),
                expected,
                "{configured}"
            );
        }
    }

    #[test]
    fn malformed_access_addresses_are_rejected() {
        for value in [
            "10.0.0.5:0",
            "proxy:0",
            "proxy:http",
            "proxy:70000",
            ":8080",
            "[::1",
            "[::1]:",
            "proxy..example",
            "proxy name",
            "http://proxy:8080",
        ] {
            assert!(validate(Some(value)).is_err(), "{value} should be rejected");
            assert!(
                resolve(Some(value), listen("127.0.0.1:8080")).is_err(),
                "{value} should not resolve"
            );
        }
        assert!(validate(None).is_ok());
        assert!(validate(Some("")).is_ok());
    }
}
