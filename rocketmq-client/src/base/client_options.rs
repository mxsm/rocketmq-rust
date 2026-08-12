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

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

use super::client_config::ClientConfig;
use crate::nameserver_discovery::NameServerDiscoveryConfig;

/// Client configuration plus an optional typed NameServer discovery source.
#[derive(Clone)]
pub struct ClientOptions {
    client: ClientConfig,
    nameserver_discovery: Option<NameServerDiscoveryConfig>,
}

impl ClientOptions {
    /// Wraps an existing client configuration with legacy discovery behavior.
    #[must_use]
    pub fn legacy(client: ClientConfig) -> Self {
        Self {
            client,
            nameserver_discovery: None,
        }
    }

    /// Adds a typed NameServer discovery source.
    #[must_use]
    pub fn with_nameserver_discovery(mut self, discovery: NameServerDiscoveryConfig) -> Self {
        self.nameserver_discovery = Some(discovery);
        self
    }

    /// Returns the underlying legacy-compatible client configuration.
    #[must_use]
    pub fn client_config(&self) -> &ClientConfig {
        &self.client
    }

    /// Returns the typed discovery configuration when present.
    #[must_use]
    pub fn nameserver_discovery(&self) -> Option<&NameServerDiscoveryConfig> {
        self.nameserver_discovery.as_ref()
    }

    pub(crate) fn from_parts(client: ClientConfig, nameserver_discovery: Option<NameServerDiscoveryConfig>) -> Self {
        Self {
            client,
            nameserver_discovery,
        }
    }

    pub(crate) fn into_normalized_parts(mut self) -> RocketMQResult<(ClientConfig, Option<NameServerDiscoveryConfig>)> {
        let Some(discovery) = self.nameserver_discovery else {
            self.client.normalize_namesrv_addr()?;
            return Ok((self.client, None));
        };

        if let Some(legacy) = self.client.namesrv_addr.as_ref() {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "nameserver_discovery",
                value: discovery.fingerprint(),
                reason: format!("typed NameServer discovery cannot be combined with legacy namesrv_addr={legacy}"),
            });
        }
        discovery.validate()?;
        if let Some(canonical) = discovery.static_canonical() {
            self.client.namesrv_addr = Some(CheetahString::from_string(canonical));
        }
        Ok((self.client, Some(discovery)))
    }
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self::legacy(ClientConfig::default())
    }
}

impl From<ClientConfig> for ClientOptions {
    fn from(value: ClientConfig) -> Self {
        Self::legacy(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nameserver_discovery::NameServerSource;

    #[test]
    fn legacy_options_preserve_existing_configuration() {
        let options = ClientOptions::legacy(ClientConfig::default());
        assert!(options.nameserver_discovery().is_none());
    }

    #[test]
    fn typed_discovery_conflicts_with_legacy_address() {
        let client = ClientConfig {
            namesrv_addr: Some(CheetahString::from_static_str("ns-a:9876")),
            ..Default::default()
        };
        let discovery = NameServerDiscoveryConfig::new(NameServerSource::dns("namesrv.default.svc", 9876).unwrap());
        assert!(ClientOptions::legacy(client)
            .with_nameserver_discovery(discovery)
            .into_normalized_parts()
            .is_err());
    }
}
