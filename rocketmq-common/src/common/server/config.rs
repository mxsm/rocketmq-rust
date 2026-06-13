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

use serde::Deserialize;
use serde::Serialize;

use crate::common::tls_config::TlsConfig;

/// Default value functions for Serde deserialization
mod defaults {
    pub fn listen_port() -> u32 {
        10911
    }

    pub fn bind_address() -> String {
        "0.0.0.0".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    #[serde(default = "defaults::listen_port")]
    pub listen_port: u32,

    #[serde(default = "defaults::bind_address")]
    pub bind_address: String,

    #[serde(default, alias = "tls")]
    pub tls_config: TlsConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            listen_port: defaults::listen_port(),
            bind_address: defaults::bind_address(),
            tls_config: TlsConfig::default(),
        }
    }
}

impl ServerConfig {
    pub fn bind_address(&self) -> String {
        self.bind_address.clone()
    }

    pub fn listen_port(&self) -> u32 {
        self.listen_port
    }

    pub fn tls_config(&self) -> &TlsConfig {
        &self.tls_config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_config() {
        let config = ServerConfig::default();

        assert_eq!(config.listen_port(), 10911);

        assert_eq!(config.bind_address(), "0.0.0.0".to_string());

        assert_eq!(
            config.tls_config().server.mode,
            crate::common::tls_config::TlsMode::Permissive
        );
    }
}
