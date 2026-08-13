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

use std::env;
use std::sync::Arc;
use std::time::Duration;

use crate::config::TlsConfig;

const CLIENT_CONNECT_TIMEOUT: &str = "com.rocketmq.rocketmq-remoting.client.connect.timeout";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectConfig {
    pub timeout: Duration,
}

impl Default for ConnectConfig {
    fn default() -> Self {
        let timeout_millis = env::var(CLIENT_CONNECT_TIMEOUT)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(3_000)
            .max(1);
        Self {
            timeout: Duration::from_millis(timeout_millis),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceConfig {
    pub idle_scan_interval: Option<Duration>,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            idle_scan_interval: Some(Duration::from_secs(60)),
        }
    }
}

/// Controls whether a response-aware request may reconnect after `GO_AWAY`.
///
/// The retryable request codes are an explicit allowlist. This keeps the
/// default behavior unchanged and prevents side-effecting requests from being
/// repeated unless their owner deliberately opts them in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoAwayPolicy {
    enabled: bool,
    retryable_request_codes: Arc<[i32]>,
}

impl GoAwayPolicy {
    /// Preserves the historical single-attempt behavior.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            retryable_request_codes: Arc::from([]),
        }
    }

    /// Enables one replacement-connection retry for the supplied request codes.
    #[must_use]
    pub fn enabled_for_request_codes(request_codes: impl IntoIterator<Item = i32>) -> Self {
        let mut request_codes = request_codes.into_iter().collect::<Vec<_>>();
        request_codes.sort_unstable();
        request_codes.dedup();
        Self {
            enabled: true,
            retryable_request_codes: request_codes.into(),
        }
    }

    /// Returns whether response-aware retries are enabled.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Returns the sorted, duplicate-free request-code allowlist.
    #[must_use]
    pub fn retryable_request_codes(&self) -> &[i32] {
        &self.retryable_request_codes
    }

    pub(crate) fn allows_request(&self, request_code: i32) -> bool {
        self.enabled && self.retryable_request_codes.binary_search(&request_code).is_ok()
    }
}

impl Default for GoAwayPolicy {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Debug, Clone, Default)]
pub struct TransportClientConfig {
    pub connect: ConnectConfig,
    pub maintenance: MaintenanceConfig,
    pub tls: TlsConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_bounded_typed_durations() {
        let config = TransportClientConfig::default();

        assert!(!config.connect.timeout.is_zero());
        assert_eq!(config.maintenance.idle_scan_interval, Some(Duration::from_secs(60)));
    }

    #[test]
    fn go_away_policy_is_disabled_and_deduplicates_explicit_codes() {
        let disabled = GoAwayPolicy::default();
        assert!(!disabled.is_enabled());
        assert!(!disabled.allows_request(105));

        let enabled = GoAwayPolicy::enabled_for_request_codes([105, 10, 105]);
        assert!(enabled.is_enabled());
        assert_eq!(enabled.retryable_request_codes(), &[10, 105]);
        assert!(enabled.allows_request(105));
        assert!(!enabled.allows_request(106));
    }
}
