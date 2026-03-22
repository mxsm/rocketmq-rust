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

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_auth::config::AuthConfig as RocketmqAuthConfig;
use rocketmq_error::RocketMQError;
use serde::Deserialize;
use serde::Serialize;

use crate::error::ProxyResult;
use crate::DEFAULT_PROXY_GRPC_PORT;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ProxyMode {
    #[default]
    Cluster,
    Local,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct GrpcConfig {
    pub listen_addr: String,
    pub max_decoding_message_size: usize,
    pub max_encoding_message_size: usize,
    pub concurrency_limit_per_connection: usize,
    pub use_endpoint_port_from_request: bool,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            listen_addr: format!("0.0.0.0:{DEFAULT_PROXY_GRPC_PORT}"),
            max_decoding_message_size: 8 * 1024 * 1024,
            max_encoding_message_size: 8 * 1024 * 1024,
            concurrency_limit_per_connection: 256,
            use_endpoint_port_from_request: false,
        }
    }
}

impl GrpcConfig {
    pub fn socket_addr(&self) -> ProxyResult<SocketAddr> {
        self.listen_addr.parse().map_err(|error| {
            RocketMQError::illegal_argument(format!(
                "invalid proxy gRPC listen address '{}': {error}",
                self.listen_addr
            ))
            .into()
        })
    }

    pub fn listen_port(&self) -> ProxyResult<u16> {
        Ok(self.socket_addr()?.port())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct ClusterConfig {
    pub namesrv_addr: Option<String>,
    pub instance_name: String,
    pub mq_client_api_timeout_ms: u64,
    pub query_assignment_strategy_name: String,
    pub producer_group_prefix: String,
    pub send_message_timeout_ms: u64,
    pub route_cache_ttl_ms: u64,
    pub metadata_cache_ttl_ms: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            namesrv_addr: None,
            instance_name: "rocketmq-proxy-cluster".to_owned(),
            mq_client_api_timeout_ms: 3_000,
            query_assignment_strategy_name: "AVG".to_owned(),
            producer_group_prefix: "PROXY_SEND".to_owned(),
            send_message_timeout_ms: 3_000,
            route_cache_ttl_ms: 5_000,
            metadata_cache_ttl_ms: 5_000,
        }
    }
}

impl ClusterConfig {
    pub fn route_cache_ttl(&self) -> Duration {
        Duration::from_millis(self.route_cache_ttl_ms)
    }

    pub fn metadata_cache_ttl(&self) -> Duration {
        Duration::from_millis(self.metadata_cache_ttl_ms)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct RuntimeConfig {
    pub route_permits: usize,
    pub producer_permits: usize,
    pub consumer_permits: usize,
    pub client_manager_permits: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            route_permits: 512,
            producer_permits: 1024,
            consumer_permits: 1024,
            client_manager_permits: 512,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct SessionConfig {
    pub client_ttl_ms: u64,
    pub receipt_handle_ttl_ms: u64,
    pub auto_renew_enabled: bool,
    pub min_long_polling_timeout_ms: u64,
    pub max_long_polling_timeout_ms: u64,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            client_ttl_ms: 60_000,
            receipt_handle_ttl_ms: 5 * 60_000,
            auto_renew_enabled: true,
            min_long_polling_timeout_ms: 5_000,
            max_long_polling_timeout_ms: 20_000,
        }
    }
}

impl SessionConfig {
    pub fn client_ttl(&self) -> Duration {
        Duration::from_millis(self.client_ttl_ms.max(1))
    }

    pub fn receipt_handle_ttl(&self) -> Duration {
        Duration::from_millis(self.receipt_handle_ttl_ms.max(1))
    }

    pub fn min_long_polling_timeout(&self) -> Duration {
        Duration::from_millis(self.min_long_polling_timeout_ms)
    }

    pub fn max_long_polling_timeout(&self) -> Duration {
        Duration::from_millis(self.max_long_polling_timeout_ms.max(self.min_long_polling_timeout_ms))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct ProxyAuthConfig {
    pub config_name: String,
    pub cluster_name: String,
    pub auth_config_path: String,
    pub authentication_enabled: bool,
    pub authentication_provider: String,
    pub authentication_metadata_provider: String,
    pub authentication_strategy: String,
    pub authentication_whitelist: Vec<String>,
    pub init_authentication_user: String,
    pub inner_client_authentication_credentials: String,
    pub authorization_enabled: bool,
    pub authorization_provider: String,
    pub authorization_metadata_provider: String,
    pub authorization_strategy: String,
    pub authorization_whitelist: Vec<String>,
}

impl Default for ProxyAuthConfig {
    fn default() -> Self {
        Self {
            config_name: "rocketmq-proxy".to_owned(),
            cluster_name: "DefaultCluster".to_owned(),
            auth_config_path: "store/proxy/auth".to_owned(),
            authentication_enabled: false,
            authentication_provider: String::new(),
            authentication_metadata_provider: String::new(),
            authentication_strategy: String::new(),
            authentication_whitelist: Vec::new(),
            init_authentication_user: String::new(),
            inner_client_authentication_credentials: String::new(),
            authorization_enabled: false,
            authorization_provider: String::new(),
            authorization_metadata_provider: String::new(),
            authorization_strategy: String::new(),
            authorization_whitelist: Vec::new(),
        }
    }
}

impl ProxyAuthConfig {
    pub fn enabled(&self) -> bool {
        self.authentication_enabled || self.authorization_enabled
    }

    pub fn to_auth_config(&self) -> RocketmqAuthConfig {
        RocketmqAuthConfig {
            config_name: CheetahString::from(self.config_name.as_str()),
            cluster_name: CheetahString::from(self.cluster_name.as_str()),
            auth_config_path: CheetahString::from(self.auth_config_path.as_str()),
            authentication_enabled: self.authentication_enabled,
            authentication_provider: CheetahString::from(self.authentication_provider.as_str()),
            authentication_metadata_provider: CheetahString::from(self.authentication_metadata_provider.as_str()),
            authentication_strategy: CheetahString::from(self.authentication_strategy.as_str()),
            authentication_whitelist: CheetahString::from(self.authentication_whitelist.join(",")),
            init_authentication_user: CheetahString::from(self.init_authentication_user.as_str()),
            inner_client_authentication_credentials: CheetahString::from(
                self.inner_client_authentication_credentials.as_str(),
            ),
            authorization_enabled: self.authorization_enabled,
            authorization_provider: CheetahString::from(self.authorization_provider.as_str()),
            authorization_metadata_provider: CheetahString::from(self.authorization_metadata_provider.as_str()),
            authorization_strategy: CheetahString::from(self.authorization_strategy.as_str()),
            authorization_whitelist: CheetahString::from(self.authorization_whitelist.join(",")),
            ..RocketmqAuthConfig::default()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default, rename_all = "camelCase")]
pub struct ProxyConfig {
    pub mode: ProxyMode,
    pub grpc: GrpcConfig,
    pub cluster: ClusterConfig,
    pub runtime: RuntimeConfig,
    pub session: SessionConfig,
    pub auth: ProxyAuthConfig,
}

impl ProxyConfig {
    pub fn load_from_file(path: impl AsRef<Path>) -> ProxyResult<Self> {
        let path = path.as_ref();
        let builder = config::Config::builder().add_source(config::File::from(path));
        let config = builder.build().map_err(|error| {
            RocketMQError::Internal(format!("failed to build proxy config from {}: {error}", path.display()))
        })?;

        config.try_deserialize().map_err(|error| {
            RocketMQError::Internal(format!(
                "failed to deserialize proxy config from {}: {error}",
                path.display()
            ))
            .into()
        })
    }
}
