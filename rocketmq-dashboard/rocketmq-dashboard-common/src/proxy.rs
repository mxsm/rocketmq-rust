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

//! Shared Proxy-domain models for dashboard implementations.

use crate::error::DashboardCommonError;
use crate::error::DashboardCommonResult as Result;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashSet;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProxyConfigSnapshot {
    pub current_proxy_addr: Option<String>,
    pub proxy_addr_list: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProxyAddressRequest {
    pub address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProxyMutationResult {
    pub message: String,
    pub snapshot: ProxyConfigSnapshot,
}

pub fn normalize_proxy_address(address: &str) -> Result<String> {
    let trimmed = address.trim();
    let (host_part, port_part) = trimmed
        .rsplit_once(':')
        .ok_or_else(|| DashboardCommonError::validation("Proxy address must be in host:port format"))?;

    let host = host_part.trim().to_ascii_lowercase();
    if host.is_empty() {
        return Err(DashboardCommonError::validation("Proxy host cannot be empty"));
    }
    if host.chars().any(char::is_whitespace) {
        return Err(DashboardCommonError::validation("Proxy host cannot contain whitespace"));
    }

    let port = port_part.trim();
    let port_number: u16 = port
        .parse()
        .map_err(|error| DashboardCommonError::parse_int(format!("Invalid Proxy port `{port}`"), error))?;

    Ok(format!("{host}:{port_number}"))
}

pub fn canonicalize_proxy_snapshot(snapshot: &ProxyConfigSnapshot) -> Result<ProxyConfigSnapshot> {
    let mut seen = HashSet::new();
    let mut addresses = Vec::new();

    for address in &snapshot.proxy_addr_list {
        let normalized = normalize_proxy_address(address)?;
        if !seen.insert(normalized.clone()) {
            return Err(DashboardCommonError::validation("Proxy address already exists"));
        }
        addresses.push(normalized);
    }

    let current_proxy_addr = snapshot
        .current_proxy_addr
        .as_deref()
        .map(normalize_proxy_address)
        .transpose()?;

    if let Some(current) = &current_proxy_addr {
        if !addresses.iter().any(|address| address == current) {
            return Err(DashboardCommonError::validation(
                "Current Proxy must exist in the address list",
            ));
        }
    }

    Ok(ProxyConfigSnapshot {
        current_proxy_addr,
        proxy_addr_list: addresses,
    })
}

#[cfg(test)]
mod tests {
    use super::canonicalize_proxy_snapshot;
    use super::normalize_proxy_address;
    use super::ProxyConfigSnapshot;

    #[test]
    fn normalize_proxy_address_accepts_host_port_and_lowercases_host() {
        let normalized = normalize_proxy_address(" LOCALHOST : 8080 ").expect("address should normalize");

        assert_eq!(normalized, "localhost:8080");
    }

    #[test]
    fn normalize_proxy_address_rejects_missing_port() {
        let error = normalize_proxy_address("localhost").expect_err("missing port should fail");

        assert!(error.to_string().contains("host:port"));
    }

    #[test]
    fn canonicalize_proxy_snapshot_rejects_duplicates() {
        let error = canonicalize_proxy_snapshot(&ProxyConfigSnapshot {
            current_proxy_addr: Some("localhost:8080".to_string()),
            proxy_addr_list: vec!["localhost:8080".to_string(), " LOCALHOST : 8080 ".to_string()],
        })
        .expect_err("duplicate proxy should fail");

        assert!(error.to_string().contains("already exists"));
    }

    #[test]
    fn canonicalize_proxy_snapshot_requires_current_to_exist() {
        let error = canonicalize_proxy_snapshot(&ProxyConfigSnapshot {
            current_proxy_addr: Some("localhost:8081".to_string()),
            proxy_addr_list: vec!["localhost:8080".to_string()],
        })
        .expect_err("missing current proxy should fail");

        assert!(error.to_string().contains("Current Proxy"));
    }
}
