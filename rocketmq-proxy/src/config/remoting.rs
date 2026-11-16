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

use crate::error::canonical;
use crate::{ProxyResult, DEFAULT_PROXY_REMOTING_PORT};
use rocketmq_transport::api::ProxyProtocolConfig;
use rocketmq_transport::api::TlsConfig;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;

mod tls;

/// Normalized RocketMQ remoting ingress configuration.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct RemotingConfig {
    pub enabled: bool,
    pub listen_addr: String,
    pub proxy_protocol: ProxyProtocolConfig,
    /// Direct, startup-only TLS for the Remoting listener, independent of gRPC.
    #[serde(deserialize_with = "tls::deserialize")]
    pub tls_config: TlsConfig,
}

impl Default for RemotingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_addr: format!("0.0.0.0:{DEFAULT_PROXY_REMOTING_PORT}"),
            proxy_protocol: ProxyProtocolConfig::default(),
            tls_config: TlsConfig::default(),
        }
    }
}

impl RemotingConfig {
    pub fn validate(&self) -> ProxyResult<()> {
        self.socket_addr()?;
        self.proxy_protocol.validate()?;
        tls::validate(&self.tls_config)?;
        Ok(())
    }

    pub fn socket_addr(&self) -> ProxyResult<SocketAddr> {
        self.listen_addr.parse().map_err(|error| {
            canonical::configuration_parse_failed_with_source("proxy.remoting.listen_addr", error).into()
        })
    }

    pub fn listen_port(&self) -> ProxyResult<u16> {
        Ok(self.socket_addr()?.port())
    }
}

impl fmt::Debug for RemotingConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemotingConfig")
            .field("enabled", &self.enabled)
            .field("listen_addr", &self.listen_addr)
            .field("proxy_protocol", &self.proxy_protocol)
            .field("tls_enabled", &self.tls_config.enable)
            .finish_non_exhaustive()
    }
}
