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

//! Non-sensitive native configuration and bounded collection settings.

use crate::StorageError;
use rocketmq_dashboard_common::{
    normalize_nameserver_selection, normalize_proxy_selection, ConnectionScope, ConnectionSnapshot,
    CredentialSourceKind, TransportSettings,
};
use serde::{Deserialize, Serialize};
use std::fmt;
#[derive(Debug, thiserror::Error)]
#[error("Proxy scope requires a selected Proxy endpoint")]
struct ConfigValidation;
/// Current on-disk schema understood by this delivery.
pub const CONFIG_SCHEMA_VERSION: u32 = 4;

/// Authentication settings that never contain credential values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct AuthConfig {
    /// Whether local dashboard sign-in is required.
    pub enabled: bool,
    /// Source category for RocketMQ Admin credentials.
    pub credential_source: CredentialSourceKind,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            credential_source: CredentialSourceKind::None,
        }
    }
}

/// Maximum accepted per-series History retention.
pub const HISTORY_MAX_POINTS_CAP: usize = 100_000;
/// Maximum accepted distinct History series.
pub const HISTORY_MAX_SERIES_CAP: usize = 4_096;
/// Maximum accepted observations across the complete History file.
pub const HISTORY_MAX_TOTAL_POINTS_CAP: usize = 100_000;

/// Feature and bounded-lifecycle settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct FoundationFlags {
    /// Enables the owned History collector.
    pub history_enabled: bool,
    /// Collection interval in seconds. Zero disables collection even when the flag is enabled.
    pub history_interval_seconds: u64,
    /// Maximum retained observations for each metric series.
    pub history_max_points_per_series: usize,
    /// Maximum retained metric series across the complete History file.
    pub history_max_series: usize,
    /// Maximum retained observations across the complete History file.
    pub history_max_total_points: usize,
    /// Enables the local Monitor store without exposing a Monitor page in Delivery 02.
    pub monitor_enabled: bool,
}

impl Default for FoundationFlags {
    fn default() -> Self {
        Self {
            history_enabled: false,
            history_interval_seconds: 60,
            history_max_points_per_series: 1_440,
            history_max_series: 512,
            history_max_total_points: 20_000,
            monitor_enabled: false,
        }
    }
}

/// Complete non-sensitive desktop configuration.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct DesktopConfig {
    /// Schema discriminator. Unknown versions are never overwritten.
    pub schema_version: u32,
    /// Monotonic persisted revision used to invalidate sessions and requests.
    pub revision: u64,
    /// Configured normalized NameServer endpoints.
    pub nameservers: Vec<String>,
    /// Selected NameServer endpoint.
    pub current_nameserver: Option<String>,
    /// Admin transport settings.
    pub transport: TransportSettings,
    /// Configured normalized Proxy endpoints.
    pub proxies: Vec<String>,
    /// Selected Proxy endpoint.
    pub current_proxy: Option<String>,
    /// Active query scope.
    pub scope: ConnectionScope,
    /// Non-sensitive authentication configuration.
    pub auth: AuthConfig,
    /// Delivery-foundation feature flags.
    pub foundations: FoundationFlags,
}

impl fmt::Debug for DesktopConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DesktopConfig")
            .field("schema_version", &self.schema_version)
            .field("revision", &self.revision)
            .field("nameserver_count", &self.nameservers.len())
            .field("current_nameserver_configured", &self.current_nameserver.is_some())
            .field("transport", &self.transport)
            .field("proxy_count", &self.proxies.len())
            .field("current_proxy_configured", &self.current_proxy.is_some())
            .field("scope", &self.scope)
            .field("auth", &self.auth)
            .field("foundations", &self.foundations)
            .finish()
    }
}

impl Default for DesktopConfig {
    fn default() -> Self {
        Self {
            schema_version: CONFIG_SCHEMA_VERSION,
            revision: 0,
            nameservers: Vec::new(),
            current_nameserver: None,
            transport: TransportSettings::default(),
            proxies: Vec::new(),
            current_proxy: None,
            scope: ConnectionScope::NameServer,
            auth: AuthConfig::default(),
            foundations: FoundationFlags::default(),
        }
    }
}

impl DesktopConfig {
    /// Validates and normalizes the complete persisted compatibility surface.
    pub fn normalize(mut self) -> Result<Self, StorageError> {
        if self.schema_version != CONFIG_SCHEMA_VERSION {
            return Err(StorageError::unsupported_schema(
                self.schema_version,
                CONFIG_SCHEMA_VERSION,
            ));
        }
        (self.nameservers, self.current_nameserver) =
            normalize_nameserver_selection(&self.nameservers, self.current_nameserver.as_deref())
                .map_err(StorageError::validation)?;
        (self.proxies, self.current_proxy) = normalize_proxy_selection(&self.proxies, self.current_proxy.as_deref())
            .map_err(StorageError::validation)?;
        if self.scope == ConnectionScope::Proxy && self.current_proxy.is_none() {
            return Err(StorageError::validation(ConfigValidation));
        }
        self.foundations.history_max_points_per_series = self
            .foundations
            .history_max_points_per_series
            .min(HISTORY_MAX_POINTS_CAP);
        self.foundations.history_max_series = self.foundations.history_max_series.min(HISTORY_MAX_SERIES_CAP);
        self.foundations.history_max_total_points = self
            .foundations
            .history_max_total_points
            .min(HISTORY_MAX_TOTAL_POINTS_CAP);
        Ok(self)
    }

    /// Projects the immutable provider input without any credential values.
    pub fn connection_snapshot(&self) -> ConnectionSnapshot {
        ConnectionSnapshot {
            revision: self.revision,
            nameserver: self.current_nameserver.clone(),
            proxy: self.current_proxy.clone(),
            scope: self.scope,
            transport: self.transport,
            credential_source: self.auth.credential_source,
        }
    }
}
