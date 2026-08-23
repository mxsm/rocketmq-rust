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

//! Protocol-independent connection, health, and session contracts.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{normalize_nameserver_address, normalize_proxy_address, DashboardCommonError, DashboardCommonResult};

/// The endpoint family used by an operator-facing connection scope.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionScope {
    /// Admin reads are scoped directly through the configured NameServer.
    #[default]
    NameServer,
    /// Consumer reads use the selected Proxy while Admin health remains unknown.
    Proxy,
}

/// The non-secret source category used to resolve credentials at runtime.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialSourceKind {
    /// No RocketMQ credential is configured.
    #[default]
    None,
    /// Credential values are resolved from the process environment.
    Environment,
}

/// Transport settings that affect construction of a RocketMQ Admin session.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransportSettings {
    /// Enables TLS for the Admin client.
    #[serde(rename = "useTLS")]
    pub use_tls: bool,
    /// Enables the legacy VIP channel port mapping.
    #[serde(rename = "useVIPChannel")]
    pub use_vip_channel: bool,
}

/// A non-sensitive immutable input for one Admin session generation.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionSnapshot {
    /// Persisted configuration revision that owns this session.
    pub revision: u64,
    /// Selected NameServer used to build the read-only Admin client.
    pub nameserver: Option<String>,
    /// Selected Proxy used only when [`Self::scope`] is [`ConnectionScope::Proxy`].
    pub proxy: Option<String>,
    /// Active query scope.
    pub scope: ConnectionScope,
    /// TLS and VIP channel settings.
    pub transport: TransportSettings,
    /// Non-secret credential source category.
    pub credential_source: CredentialSourceKind,
}

impl std::fmt::Debug for ConnectionSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectionSnapshot")
            .field("revision", &self.revision)
            .field("nameserver_configured", &self.nameserver.is_some())
            .field("proxy_configured", &self.proxy.is_some())
            .field("scope", &self.scope)
            .field("transport", &self.transport)
            .field("credential_source", &self.credential_source)
            .finish()
    }
}

/// Stable endpoint availability displayed by connection workflows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointAvailability {
    /// No real check has established availability.
    #[default]
    Unknown,
    /// A read-only Admin request completed successfully.
    Available,
    /// A read-only Admin request failed.
    Unavailable,
}

/// Safe result of checking one endpoint.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointHealth {
    /// Normalized endpoint address.
    pub endpoint: String,
    /// Persisted revision for which the result is valid.
    pub revision: u64,
    /// Availability derived only from success or failure.
    pub availability: EndpointAvailability,
    /// Wall-clock completion time in Unix milliseconds when supplied by the host.
    pub checked_at_epoch_ms: Option<u64>,
    /// Sanitized failure summary. It never contains a backend error body.
    pub failure_summary: Option<String>,
}

impl fmt::Debug for EndpointHealth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EndpointHealth")
            .field("endpoint_configured", &!self.endpoint.is_empty())
            .field("revision", &self.revision)
            .field("availability", &self.availability)
            .field("checked_at_epoch_ms", &self.checked_at_epoch_ms)
            .field("failure_summary_available", &self.failure_summary.is_some())
            .finish()
    }
}

/// Non-sensitive session status for the Security page and Topbar.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminSessionStatus {
    /// A session cannot be built until a NameServer is selected.
    #[default]
    NotConfigured,
    /// Session replacement is in progress.
    Connecting,
    /// A read-only Admin session is ready.
    Connected,
    /// No session is active because startup or replacement failed.
    Failed,
    /// The runtime has shut the session down.
    Closed,
}

/// Safe session summary tied to one configuration revision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminSessionSummary {
    /// Configuration revision that owns the status.
    pub revision: u64,
    /// Current lifecycle state.
    pub status: AdminSessionStatus,
    /// Non-secret credential source category.
    pub credential_source: CredentialSourceKind,
}

/// Normalizes, deduplicates, and validates a NameServer list plus current selection.
pub fn normalize_nameserver_selection(
    endpoints: &[String],
    current: Option<&str>,
) -> DashboardCommonResult<(Vec<String>, Option<String>)> {
    normalize_selection(endpoints, current, normalize_nameserver_address, "NameServer")
}

/// Normalizes, deduplicates, and validates a Proxy list plus current selection.
pub fn normalize_proxy_selection(
    endpoints: &[String],
    current: Option<&str>,
) -> DashboardCommonResult<(Vec<String>, Option<String>)> {
    normalize_selection(endpoints, current, normalize_proxy_address, "Proxy")
}

/// Adds one normalized endpoint without changing an existing current selection.
pub fn add_endpoint(
    endpoints: &mut Vec<String>,
    current: &mut Option<String>,
    address: &str,
    normalize: fn(&str) -> DashboardCommonResult<String>,
) -> DashboardCommonResult<()> {
    let normalized = normalize(address)?;
    if endpoints.iter().any(|endpoint| endpoint == &normalized) {
        return Err(DashboardCommonError::validation("Endpoint already exists"));
    }
    endpoints.push(normalized.clone());
    if current.is_none() {
        *current = Some(normalized);
    }
    Ok(())
}

/// Selects an endpoint that already exists in the normalized list.
pub fn switch_endpoint(
    endpoints: &[String],
    current: &mut Option<String>,
    address: &str,
    normalize: fn(&str) -> DashboardCommonResult<String>,
) -> DashboardCommonResult<()> {
    let normalized = normalize(address)?;
    if !endpoints.iter().any(|endpoint| endpoint == &normalized) {
        return Err(DashboardCommonError::validation("Endpoint is not configured"));
    }
    *current = Some(normalized);
    Ok(())
}

/// Removes an endpoint and requires an explicit replacement when it is active.
pub fn remove_endpoint(
    endpoints: &mut Vec<String>,
    current: &mut Option<String>,
    address: &str,
    replacement: Option<&str>,
    allow_fallback: bool,
    normalize: fn(&str) -> DashboardCommonResult<String>,
) -> DashboardCommonResult<()> {
    let normalized = normalize(address)?;
    if !endpoints.iter().any(|endpoint| endpoint == &normalized) {
        return Err(DashboardCommonError::validation("Endpoint is not configured"));
    }

    if current.as_deref() == Some(normalized.as_str()) {
        match replacement {
            Some(replacement) => {
                let replacement = normalize(replacement)?;
                if replacement == normalized || !endpoints.iter().any(|endpoint| endpoint == &replacement) {
                    return Err(DashboardCommonError::validation(
                        "Active endpoint replacement must be another configured endpoint",
                    ));
                }
                *current = Some(replacement);
            }
            None if allow_fallback => *current = None,
            None => {
                return Err(DashboardCommonError::validation(
                    "Active endpoint removal requires an explicit replacement",
                ));
            }
        }
    }

    endpoints.retain(|endpoint| endpoint != &normalized);
    Ok(())
}

fn normalize_selection(
    endpoints: &[String],
    current: Option<&str>,
    normalize: fn(&str) -> DashboardCommonResult<String>,
    kind: &str,
) -> DashboardCommonResult<(Vec<String>, Option<String>)> {
    let mut normalized = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints {
        let endpoint = normalize(endpoint)?;
        if normalized.iter().any(|existing| existing == &endpoint) {
            return Err(DashboardCommonError::validation(format!(
                "{kind} endpoint already exists"
            )));
        }
        normalized.push(endpoint);
    }
    let current = current.map(normalize).transpose()?;
    if current
        .as_ref()
        .is_some_and(|selected| !normalized.iter().any(|endpoint| endpoint == selected))
    {
        return Err(DashboardCommonError::validation(format!(
            "Current {kind} must exist in the endpoint list"
        )));
    }
    Ok((normalized, current))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_snapshot_debug_omits_endpoint_values() {
        let snapshot = ConnectionSnapshot {
            revision: 7,
            nameserver: Some("sensitive.internal:9876".into()),
            proxy: Some("proxy.internal:8080".into()),
            scope: ConnectionScope::Proxy,
            transport: TransportSettings::default(),
            credential_source: CredentialSourceKind::Environment,
        };

        let debug = format!("{snapshot:?}");
        assert!(!debug.contains("sensitive.internal"));
        assert!(!debug.contains("proxy.internal"));
        assert!(debug.contains("revision: 7"));
    }

    #[test]
    fn normalization_deduplicates_by_rejecting_equivalent_endpoints() {
        let error = normalize_nameserver_selection(
            &["LOCALHOST:9876".into(), " localhost : 9876 ".into()],
            Some("localhost:9876"),
        )
        .expect_err("duplicate must be rejected");

        assert!(error.to_string().contains("already exists"));
    }

    #[test]
    fn active_removal_requires_explicit_replacement_or_allowed_fallback() {
        let mut endpoints = vec!["one:9876".into(), "two:9876".into()];
        let mut current = Some("one:9876".into());

        assert!(remove_endpoint(
            &mut endpoints,
            &mut current,
            "one:9876",
            None,
            false,
            normalize_nameserver_address,
        )
        .is_err());
        remove_endpoint(
            &mut endpoints,
            &mut current,
            "one:9876",
            Some("two:9876"),
            false,
            normalize_nameserver_address,
        )
        .expect("replacement is explicit");
        assert_eq!(current.as_deref(), Some("two:9876"));

        let mut proxies = vec!["proxy:8080".into()];
        let mut current_proxy = Some("proxy:8080".into());
        remove_endpoint(
            &mut proxies,
            &mut current_proxy,
            "proxy:8080",
            None,
            true,
            normalize_proxy_address,
        )
        .expect("NameServer fallback is explicit at the caller");
        assert_eq!(current_proxy, None);
    }

    #[test]
    fn first_add_selects_endpoint_but_later_add_preserves_current() {
        let mut endpoints = Vec::new();
        let mut current = None;
        add_endpoint(
            &mut endpoints,
            &mut current,
            "localhost:9876",
            normalize_nameserver_address,
        )
        .expect("first endpoint");
        add_endpoint(&mut endpoints, &mut current, "other:9876", normalize_nameserver_address)
            .expect("second endpoint");

        assert_eq!(current.as_deref(), Some("localhost:9876"));
    }
}
