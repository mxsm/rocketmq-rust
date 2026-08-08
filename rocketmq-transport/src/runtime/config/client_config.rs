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
}
