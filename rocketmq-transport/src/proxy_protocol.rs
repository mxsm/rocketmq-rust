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

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::time::Duration;

use ipnet::IpNet;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::time::Instant;

const V1_PREFIX: &[u8] = b"PROXY ";
const V1_MAX_HEADER_BYTES: usize = 108;
const V2_SIGNATURE: &[u8; 12] = b"\r\n\r\n\0\r\nQUIT\n";
const V2_FIXED_HEADER_BYTES: usize = 16;

/// Policy for a PROXY v2 TLV type that is not in the configured allowlist.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum UnknownTlvPolicy {
    /// Discard unrecognized TLVs while retaining allowed metadata.
    #[default]
    Ignore,
    /// Reject the connection when any unrecognized TLV is present.
    Reject,
}

/// Bounded trust policy for accepting PROXY protocol metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct ProxyProtocolConfig {
    /// Enables optional PROXY v1/v2 detection before TLS and application decoding.
    pub enabled: bool,
    /// Network peers authorized to submit source identity metadata.
    pub trusted_proxies: Vec<IpNet>,
    /// PROXY v2 TLV type bytes retained in connection metadata.
    pub allowed_tlvs: Vec<u8>,
    /// Handling for a TLV not present in `allowed_tlvs`.
    pub unknown_tlv_policy: UnknownTlvPolicy,
    /// Maximum complete v1/v2 header size, including its fixed prefix.
    pub max_header_bytes: usize,
    /// Absolute time budget for detecting and reading a candidate header.
    pub header_timeout_millis: u64,
}

impl Default for ProxyProtocolConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            trusted_proxies: Vec::new(),
            allowed_tlvs: Vec::new(),
            unknown_tlv_policy: UnknownTlvPolicy::Ignore,
            max_header_bytes: 512,
            header_timeout_millis: 1_000,
        }
    }
}

impl ProxyProtocolConfig {
    /// Validates the bounded parser and source-trust contract.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when enabled without a trust root or with unusable limits.
    pub fn validate(&self) -> RocketMQResult<()> {
        if !self.enabled {
            return Ok(());
        }
        if self.trusted_proxies.is_empty() {
            return Err(config_error(
                "proxyProtocol.trustedProxies",
                "at least one trusted CIDR is required when PROXY protocol is enabled",
            ));
        }
        if self.max_header_bytes < V2_FIXED_HEADER_BYTES || self.max_header_bytes > u16::MAX as usize + 16 {
            return Err(config_error(
                "proxyProtocol.maxHeaderBytes",
                "must be between 16 and 65551 bytes",
            ));
        }
        if self.header_timeout_millis == 0 {
            return Err(config_error(
                "proxyProtocol.headerTimeoutMillis",
                "must be greater than zero",
            ));
        }
        Ok(())
    }

    fn trusts(&self, address: IpAddr) -> bool {
        self.trusted_proxies.iter().any(|network| network.contains(&address))
    }
}

/// Authenticated source/destination identity extracted from one trusted PROXY header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyProtocolMetadata {
    pub transport_peer: SocketAddr,
    pub source: SocketAddr,
    pub destination: SocketAddr,
    pub tlvs: BTreeMap<u8, Vec<u8>>,
}

impl ProxyProtocolMetadata {
    /// Returns the Java-compatible HAProxy channel attribute value when it is textual.
    #[must_use]
    pub fn attribute(&self, key: &str) -> Option<String> {
        use rocketmq_model::common::constant::ha_proxy_constants;

        match key {
            ha_proxy_constants::PROXY_PROTOCOL_ADDR => Some(self.source.ip().to_string()),
            ha_proxy_constants::PROXY_PROTOCOL_PORT => Some(self.source.port().to_string()),
            ha_proxy_constants::PROXY_PROTOCOL_SERVER_ADDR => Some(self.destination.ip().to_string()),
            ha_proxy_constants::PROXY_PROTOCOL_SERVER_PORT => Some(self.destination.port().to_string()),
            _ => key
                .strip_prefix(ha_proxy_constants::PROXY_PROTOCOL_TLV_PREFIX)
                .and_then(|suffix| u8::from_str_radix(suffix, 16).ok())
                .and_then(|kind| self.tlvs.get(&kind))
                .and_then(|value| std::str::from_utf8(value).ok())
                .map(str::to_owned),
        }
    }
}

enum Detection {
    NoHeader,
    NeedMore,
    Header(usize),
}

/// Detects and consumes one optional trusted PROXY v1/v2 header from a TCP stream.
///
/// Disabled mode returns without reading or peeking. When enabled, non-PROXY application bytes
/// remain untouched. A detected header is consumed before the caller performs TLS detection.
///
/// # Errors
///
/// Returns a typed configuration/network error for untrusted, malformed, oversized, truncated, or
/// disallowed metadata.
pub async fn read_proxy_protocol(
    stream: &mut TcpStream,
    transport_peer: SocketAddr,
    config: &ProxyProtocolConfig,
) -> RocketMQResult<Option<ProxyProtocolMetadata>> {
    if !config.enabled {
        return Ok(None);
    }
    config.validate()?;
    let deadline = Instant::now() + Duration::from_millis(config.header_timeout_millis);
    let mut peeked = vec![0_u8; config.max_header_bytes.saturating_add(1)];
    let header_len = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(protocol_error("candidate header timed out"));
        }
        let count = tokio::time::timeout(remaining, stream.peek(&mut peeked))
            .await
            .map_err(|_| protocol_error("candidate header timed out"))??;
        if count == 0 {
            return Err(protocol_error("candidate header was truncated"));
        }
        match detect_header(&peeked[..count], config.max_header_bytes)? {
            Detection::NoHeader => return Ok(None),
            Detection::Header(header_len) => break header_len,
            Detection::NeedMore => tokio::task::yield_now().await,
        }
    };

    if !config.trusts(transport_peer.ip()) {
        return Err(protocol_error(
            "transport peer is not authorized to submit a PROXY header",
        ));
    }
    let mut header = vec![0_u8; header_len];
    let remaining = deadline.saturating_duration_since(Instant::now());
    tokio::time::timeout(remaining, stream.read_exact(&mut header))
        .await
        .map_err(|_| protocol_error("candidate header timed out"))??;
    parse_header(&header, transport_peer, config)
}

fn detect_header(input: &[u8], max_header_bytes: usize) -> RocketMQResult<Detection> {
    if input.starts_with(V1_PREFIX) {
        if let Some(end) = input.windows(2).position(|window| window == b"\r\n") {
            let length = end + 2;
            if length > max_header_bytes || length > V1_MAX_HEADER_BYTES {
                return Err(protocol_error("PROXY v1 header exceeds the configured limit"));
            }
            return Ok(Detection::Header(length));
        }
        if input.len() >= max_header_bytes.min(V1_MAX_HEADER_BYTES) {
            return Err(protocol_error("PROXY v1 header is missing its terminator"));
        }
        return Ok(Detection::NeedMore);
    }
    if input.starts_with(V2_SIGNATURE) {
        if input.len() < V2_FIXED_HEADER_BYTES {
            return Ok(Detection::NeedMore);
        }
        let payload_len = usize::from(u16::from_be_bytes([input[14], input[15]]));
        let length = V2_FIXED_HEADER_BYTES + payload_len;
        if length > max_header_bytes {
            return Err(protocol_error("PROXY v2 header exceeds the configured limit"));
        }
        return Ok(if input.len() >= length {
            Detection::Header(length)
        } else {
            Detection::NeedMore
        });
    }
    if V1_PREFIX.starts_with(input) || V2_SIGNATURE.starts_with(input) {
        return Ok(Detection::NeedMore);
    }
    Ok(Detection::NoHeader)
}

fn parse_header(
    header: &[u8],
    transport_peer: SocketAddr,
    config: &ProxyProtocolConfig,
) -> RocketMQResult<Option<ProxyProtocolMetadata>> {
    if header.starts_with(V1_PREFIX) {
        parse_v1(header, transport_peer).map(Some)
    } else if header.starts_with(V2_SIGNATURE) {
        parse_v2(header, transport_peer, config)
    } else {
        Err(protocol_error("detected header has an invalid signature"))
    }
}

fn parse_v1(header: &[u8], transport_peer: SocketAddr) -> RocketMQResult<ProxyProtocolMetadata> {
    let line = std::str::from_utf8(header).map_err(|_| protocol_error("PROXY v1 header is not UTF-8"))?;
    let fields: Vec<&str> = line.trim_end_matches("\r\n").split_ascii_whitespace().collect();
    if fields.len() != 6 || fields[0] != "PROXY" {
        return Err(protocol_error("PROXY v1 header has an invalid field count"));
    }
    let source_ip = parse_ip(fields[2], fields[1])?;
    let destination_ip = parse_ip(fields[3], fields[1])?;
    let source_port = fields[4]
        .parse::<u16>()
        .map_err(|_| protocol_error("PROXY v1 source port is invalid"))?;
    let destination_port = fields[5]
        .parse::<u16>()
        .map_err(|_| protocol_error("PROXY v1 destination port is invalid"))?;
    Ok(ProxyProtocolMetadata {
        transport_peer,
        source: SocketAddr::new(source_ip, source_port),
        destination: SocketAddr::new(destination_ip, destination_port),
        tlvs: BTreeMap::new(),
    })
}

fn parse_ip(value: &str, family: &str) -> RocketMQResult<IpAddr> {
    let address = value
        .parse::<IpAddr>()
        .map_err(|_| protocol_error("PROXY v1 address is invalid"))?;
    if matches!((family, address), ("TCP4", IpAddr::V4(_)) | ("TCP6", IpAddr::V6(_))) {
        Ok(address)
    } else {
        Err(protocol_error("PROXY v1 address family does not match the address"))
    }
}

fn parse_v2(
    header: &[u8],
    transport_peer: SocketAddr,
    config: &ProxyProtocolConfig,
) -> RocketMQResult<Option<ProxyProtocolMetadata>> {
    let version_command = header[12];
    if version_command >> 4 != 2 {
        return Err(protocol_error("PROXY v2 version is invalid"));
    }
    match version_command & 0x0f {
        0 => return Ok(None),
        1 => {}
        _ => return Err(protocol_error("PROXY v2 command is invalid")),
    }
    let (source, destination, tlv_offset) = match header[13] {
        0x11 => parse_v2_tcp4(header)?,
        0x21 => parse_v2_tcp6(header)?,
        _ => return Err(protocol_error("PROXY v2 requires a TCP4 or TCP6 address family")),
    };
    let tlvs = parse_tlvs(&header[tlv_offset..], config)?;
    Ok(Some(ProxyProtocolMetadata {
        transport_peer,
        source,
        destination,
        tlvs,
    }))
}

fn parse_v2_tcp4(header: &[u8]) -> RocketMQResult<(SocketAddr, SocketAddr, usize)> {
    if header.len() < 28 {
        return Err(protocol_error("PROXY v2 TCP4 address block is truncated"));
    }
    let source = IpAddr::V4(Ipv4Addr::new(header[16], header[17], header[18], header[19]));
    let destination = IpAddr::V4(Ipv4Addr::new(header[20], header[21], header[22], header[23]));
    let source_port = u16::from_be_bytes([header[24], header[25]]);
    let destination_port = u16::from_be_bytes([header[26], header[27]]);
    Ok((
        SocketAddr::new(source, source_port),
        SocketAddr::new(destination, destination_port),
        28,
    ))
}

fn parse_v2_tcp6(header: &[u8]) -> RocketMQResult<(SocketAddr, SocketAddr, usize)> {
    if header.len() < 52 {
        return Err(protocol_error("PROXY v2 TCP6 address block is truncated"));
    }
    let source = IpAddr::V6(Ipv6Addr::from(
        <[u8; 16]>::try_from(&header[16..32])
            .map_err(|_| protocol_error("PROXY v2 TCP6 source address is truncated"))?,
    ));
    let destination =
        IpAddr::V6(Ipv6Addr::from(<[u8; 16]>::try_from(&header[32..48]).map_err(|_| {
            protocol_error("PROXY v2 TCP6 destination address is truncated")
        })?));
    let source_port = u16::from_be_bytes([header[48], header[49]]);
    let destination_port = u16::from_be_bytes([header[50], header[51]]);
    Ok((
        SocketAddr::new(source, source_port),
        SocketAddr::new(destination, destination_port),
        52,
    ))
}

fn parse_tlvs(input: &[u8], config: &ProxyProtocolConfig) -> RocketMQResult<BTreeMap<u8, Vec<u8>>> {
    let mut cursor = 0;
    let mut tlvs = BTreeMap::new();
    while cursor < input.len() {
        if input.len() - cursor < 3 {
            return Err(protocol_error("PROXY v2 TLV header is truncated"));
        }
        let kind = input[cursor];
        let length = usize::from(u16::from_be_bytes([input[cursor + 1], input[cursor + 2]]));
        cursor += 3;
        if input.len() - cursor < length {
            return Err(protocol_error("PROXY v2 TLV value is truncated"));
        }
        let value = &input[cursor..cursor + length];
        cursor += length;
        if config.allowed_tlvs.contains(&kind) {
            tlvs.insert(kind, value.to_vec());
        } else if config.unknown_tlv_policy == UnknownTlvPolicy::Reject {
            return Err(protocol_error(format!("PROXY v2 TLV 0x{kind:02x} is not allowed")));
        }
    }
    Ok(tlvs)
}

fn protocol_error(reason: impl Into<String>) -> RocketMQError {
    RocketMQError::network_connection_failed("proxy-protocol", reason.into())
}

fn config_error(key: &'static str, reason: &'static str) -> RocketMQError {
    RocketMQError::ConfigInvalidValue {
        key,
        value: "<configured>".to_owned(),
        reason: reason.to_owned(),
    }
}

#[cfg(test)]
#[path = "../tests/unit/proxy_protocol.rs"]
mod tests;
