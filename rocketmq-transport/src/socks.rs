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
use std::net::IpAddr;
use std::net::SocketAddr;
use std::sync::Arc;

use ipnet::IpNet;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use zeroize::Zeroizing;

use crate::deadline::RequestDeadline;

/// A validated, deterministic routing table parsed from Java's SOCKS proxy JSON shape.
#[derive(Clone, Default)]
pub struct SocksProxyConfig {
    routes: Arc<[RouteEntry]>,
}

impl fmt::Debug for SocksProxyConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SocksProxyConfig")
            .field("route_count", &self.routes.len())
            .finish()
    }
}

/// One selected SOCKS5 route. Credentials are intentionally not observable.
#[derive(Clone)]
pub struct SocksProxyRoute {
    endpoint: ProxyEndpoint,
    credentials: Option<Credentials>,
}

impl fmt::Debug for SocksProxyRoute {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SocksProxyRoute")
            .field("endpoint", &self.endpoint.authority)
            .field("authenticated", &self.credentials.is_some())
            .finish()
    }
}

#[derive(Clone)]
struct Credentials {
    username: String,
    password: Zeroizing<String>,
}

#[derive(Clone)]
struct ProxyEndpoint {
    authority: String,
}

#[derive(Clone)]
struct RouteEntry {
    matcher: RouteMatcher,
    route: SocksProxyRoute,
}

#[derive(Clone)]
enum RouteMatcher {
    ExactDomain(String),
    DomainSuffix(String),
    Network(IpNet),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawProxyConfig {
    addr: String,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
}

impl SocksProxyConfig {
    /// Parses Java's `Map<target-rule, SocksProxyConfig>` JSON representation.
    ///
    /// CIDR keys retain Java compatibility. Exact domains and `*.example.com`
    /// suffix rules are accepted as a Rust extension and take precedence over CIDR rules.
    pub fn parse_java_json(json: &str) -> RocketMQResult<Self> {
        let raw_routes = serde_json::from_str::<std::collections::BTreeMap<String, RawProxyConfig>>(json)
            .map_err(|_| invalid_config("invalid JSON object"))?;
        let mut routes = Vec::with_capacity(raw_routes.len());
        for (rule, raw) in raw_routes {
            let matcher = parse_matcher(&rule)?;
            let endpoint = ProxyEndpoint::parse(raw.addr)?;
            let credentials = match (raw.username, raw.password) {
                (None, None) => None,
                (Some(username), Some(password)) if !username.is_empty() && !password.is_empty() => Some(Credentials {
                    username,
                    password: Zeroizing::new(password),
                }),
                _ => return Err(invalid_config("username and password must be configured together")),
            };
            routes.push(RouteEntry {
                matcher,
                route: SocksProxyRoute { endpoint, credentials },
            });
        }
        routes.sort_by_key(|route| std::cmp::Reverse(route.matcher.specificity()));
        Ok(Self { routes: routes.into() })
    }

    /// Returns whether no proxy routes are configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }

    /// Selects the most specific route for the original target host and optional resolved IP.
    #[must_use]
    pub fn route_for(&self, target_host: &str, resolved_ip: Option<IpAddr>) -> Option<&SocksProxyRoute> {
        let target_host = target_host.trim_matches(['[', ']']).to_ascii_lowercase();
        let target_ip = resolved_ip.or_else(|| target_host.parse().ok());
        self.routes
            .iter()
            .find(|entry| entry.matcher.matches(&target_host, target_ip))
            .map(|entry| &entry.route)
    }
}

impl SocksProxyRoute {
    /// Returns the configured proxy endpoint without exposing credentials.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint.authority
    }

    /// Opens one authenticated SOCKS5 tunnel while preserving the original target DNS name.
    pub async fn connect(
        &self,
        target_host: &str,
        target_port: u16,
        deadline: RequestDeadline,
    ) -> RocketMQResult<TcpStream> {
        let endpoint = self.endpoint.authority.clone();
        let mut stream = deadline
            .timeout(TcpStream::connect(&endpoint))
            .await
            .map_err(|_| RocketMQError::network_connection_timeout(endpoint.clone(), deadline.budget_millis()))?
            .map_err(|error| RocketMQError::network_connection_failed(endpoint.clone(), error.to_string()))?;
        deadline
            .timeout(negotiate(
                &mut stream,
                self.credentials.as_ref(),
                target_host,
                target_port,
            ))
            .await
            .map_err(|_| RocketMQError::network_connection_timeout(endpoint.clone(), deadline.budget_millis()))??;
        Ok(stream)
    }

    /// Opens a SOCKS5 tunnel and negotiates TLS against the original business target name.
    #[cfg(feature = "tls")]
    pub async fn connect_tls(
        &self,
        target_host: &str,
        target_port: u16,
        tls_config: &crate::config::TlsConfig,
        deadline: RequestDeadline,
    ) -> RocketMQResult<tokio_rustls::client::TlsStream<TcpStream>> {
        let stream = self.connect(target_host, target_port, deadline).await?;
        deadline
            .timeout(crate::tls::connect_tls_stream(stream, target_host, tls_config))
            .await
            .map_err(|_| RocketMQError::network_connection_timeout(target_host, deadline.budget_millis()))?
    }
}

pub(crate) async fn connect_target(
    config: &SocksProxyConfig,
    authority: &str,
    resolved_addr: Option<SocketAddr>,
    deadline: RequestDeadline,
) -> RocketMQResult<TcpStream> {
    let (host, port) = split_host_port(authority)?;
    let resolved_ip = match resolved_addr {
        Some(address) => Some(address.ip()),
        None if config.route_for(host, None).is_none() && !config.is_empty() => {
            let addresses = deadline
                .timeout(tokio::net::lookup_host(authority))
                .await
                .map_err(|_| RocketMQError::network_connection_timeout(authority, deadline.budget_millis()))?
                .map_err(|error| RocketMQError::network_connection_failed(authority, error.to_string()))?;
            let mut first_ip = None;
            let mut matching_ip = None;
            for address in addresses {
                let ip = address.ip();
                first_ip.get_or_insert(ip);
                if config.route_for(host, Some(ip)).is_some() {
                    matching_ip = Some(ip);
                    break;
                }
            }
            matching_ip.or(first_ip)
        }
        None => None,
    };
    if let Some(route) = config.route_for(host, resolved_ip) {
        return route.connect(host, port, deadline).await;
    }

    let destination = resolved_addr.map_or_else(|| authority.to_string(), |address| address.to_string());
    deadline
        .timeout(TcpStream::connect(&destination))
        .await
        .map_err(|_| RocketMQError::network_connection_timeout(authority, deadline.budget_millis()))?
        .map_err(|error| RocketMQError::network_connection_failed(authority, error.to_string()))
}

impl ProxyEndpoint {
    fn parse(authority: String) -> RocketMQResult<Self> {
        let (host, port) = split_host_port(&authority)?;
        if host.is_empty() || port == 0 {
            return Err(invalid_config(
                "proxy addr must contain a non-empty host and non-zero port",
            ));
        }
        Ok(Self { authority })
    }
}

impl RouteMatcher {
    fn specificity(&self) -> (u8, usize) {
        match self {
            Self::ExactDomain(domain) => (3, domain.len()),
            Self::DomainSuffix(suffix) => (2, suffix.len()),
            Self::Network(network) => (1, network.prefix_len() as usize),
        }
    }

    fn matches(&self, host: &str, ip: Option<IpAddr>) -> bool {
        match self {
            Self::ExactDomain(domain) => host == domain,
            Self::DomainSuffix(suffix) => host != suffix && host.ends_with(&format!(".{suffix}")),
            Self::Network(network) => ip.is_some_and(|ip| network.contains(&ip)),
        }
    }
}

fn parse_matcher(rule: &str) -> RocketMQResult<RouteMatcher> {
    let rule = rule.trim().to_ascii_lowercase();
    if rule.is_empty() {
        return Err(invalid_config("target rule must not be empty"));
    }
    if let Ok(network) = rule.parse::<IpNet>() {
        return Ok(RouteMatcher::Network(network));
    }
    if let Some(suffix) = rule.strip_prefix("*.") {
        if valid_domain(suffix) {
            return Ok(RouteMatcher::DomainSuffix(suffix.to_string()));
        }
        return Err(invalid_config("invalid wildcard domain rule"));
    }
    if valid_domain(&rule) {
        return Ok(RouteMatcher::ExactDomain(rule));
    }
    Err(invalid_config(
        "target rule must be a CIDR, exact domain, or wildcard domain",
    ))
}

fn valid_domain(domain: &str) -> bool {
    !domain.is_empty()
        && domain.len() <= 253
        && domain.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

fn split_host_port(authority: &str) -> RocketMQResult<(&str, u16)> {
    let authority = authority.trim();
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, port) = rest
            .split_once("]:")
            .ok_or_else(|| invalid_config("proxy addr must use [ipv6]:port syntax"))?;
        (host, port)
    } else {
        authority
            .rsplit_once(':')
            .ok_or_else(|| invalid_config("proxy addr must use host:port syntax"))?
    };
    let port = port
        .parse::<u16>()
        .map_err(|_| invalid_config("proxy addr contains an invalid port"))?;
    Ok((host, port))
}

async fn negotiate(
    stream: &mut TcpStream,
    credentials: Option<&Credentials>,
    target_host: &str,
    target_port: u16,
) -> RocketMQResult<()> {
    let methods: &[u8] = if credentials.is_some() { &[2] } else { &[0] };
    stream
        .write_all(&[5, methods.len() as u8])
        .await
        .map_err(proxy_io_error)?;
    stream.write_all(methods).await.map_err(proxy_io_error)?;
    let mut selection = [0u8; 2];
    stream.read_exact(&mut selection).await.map_err(proxy_io_error)?;
    if selection[0] != 5 || selection[1] == 0xff {
        return Err(proxy_protocol_error("proxy rejected all authentication methods"));
    }
    match (selection[1], credentials) {
        (0, None) => {}
        (2, Some(credentials)) => authenticate(stream, credentials).await?,
        _ => {
            return Err(proxy_protocol_error(
                "proxy selected an unoffered authentication method",
            ))
        }
    }

    let mut request = vec![5, 1, 0];
    encode_target(&mut request, target_host)?;
    request.extend_from_slice(&target_port.to_be_bytes());
    stream.write_all(&request).await.map_err(proxy_io_error)?;

    let mut response = [0u8; 4];
    stream.read_exact(&mut response).await.map_err(proxy_io_error)?;
    if response[0] != 5 || response[1] != 0 {
        return Err(proxy_protocol_error("proxy rejected CONNECT request"));
    }
    discard_bound_address(stream, response[3]).await
}

async fn authenticate(stream: &mut TcpStream, credentials: &Credentials) -> RocketMQResult<()> {
    let username = credentials.username.as_bytes();
    let password = credentials.password.as_bytes();
    if username.len() > u8::MAX as usize || password.len() > u8::MAX as usize {
        return Err(invalid_config("SOCKS5 username and password must not exceed 255 bytes"));
    }
    let mut request = Vec::with_capacity(3 + username.len() + password.len());
    request.extend_from_slice(&[1, username.len() as u8]);
    request.extend_from_slice(username);
    request.push(password.len() as u8);
    request.extend_from_slice(password);
    stream.write_all(&request).await.map_err(proxy_io_error)?;
    let mut response = [0u8; 2];
    stream.read_exact(&mut response).await.map_err(proxy_io_error)?;
    if response != [1, 0] {
        return Err(proxy_protocol_error("proxy authentication failed"));
    }
    Ok(())
}

fn encode_target(request: &mut Vec<u8>, target_host: &str) -> RocketMQResult<()> {
    let target_host = target_host.trim_matches(['[', ']']);
    match target_host.parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) => {
            request.push(1);
            request.extend_from_slice(&ip.octets());
        }
        Ok(IpAddr::V6(ip)) => {
            request.push(4);
            request.extend_from_slice(&ip.octets());
        }
        Err(_) => {
            if target_host.is_empty() || target_host.len() > u8::MAX as usize || !target_host.is_ascii() {
                return Err(invalid_config(
                    "SOCKS5 target host must be a non-empty ASCII name up to 255 bytes",
                ));
            }
            request.extend_from_slice(&[3, target_host.len() as u8]);
            request.extend_from_slice(target_host.as_bytes());
        }
    }
    Ok(())
}

async fn discard_bound_address(stream: &mut TcpStream, address_type: u8) -> RocketMQResult<()> {
    let address_len = match address_type {
        1 => 4,
        4 => 16,
        3 => stream.read_u8().await.map_err(proxy_io_error)? as usize,
        _ => return Err(proxy_protocol_error("proxy returned an invalid address type")),
    };
    let mut address = vec![0u8; address_len + 2];
    stream.read_exact(&mut address).await.map_err(proxy_io_error)?;
    Ok(())
}

fn invalid_config(reason: &'static str) -> RocketMQError {
    RocketMQError::ConfigInvalidValue {
        key: "com.rocketmq.socks.proxy.config",
        value: "<redacted>".to_string(),
        reason: reason.to_string(),
    }
}

fn proxy_io_error(error: std::io::Error) -> RocketMQError {
    RocketMQError::network_connection_failed("socks5-proxy", error.to_string())
}

fn proxy_protocol_error(reason: &'static str) -> RocketMQError {
    RocketMQError::network_connection_failed("socks5-proxy", reason)
}
