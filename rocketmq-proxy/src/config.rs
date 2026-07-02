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

use std::fmt;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_auth::config::AuthConfig as RocketmqAuthConfig;
use rocketmq_auth::SignatureAlgorithm;
use rocketmq_error::RocketMQError;
use serde::Deserialize;
use serde::Serialize;

use crate::error::ProxyResult;
use crate::DEFAULT_PROXY_GRPC_PORT;
use crate::DEFAULT_PROXY_REMOTING_PORT;

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
pub struct RemotingConfig {
    pub enabled: bool,
    pub listen_addr: String,
}

impl Default for RemotingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_addr: format!("0.0.0.0:{DEFAULT_PROXY_REMOTING_PORT}"),
        }
    }
}

impl RemotingConfig {
    pub fn socket_addr(&self) -> ProxyResult<SocketAddr> {
        self.listen_addr.parse().map_err(|error| {
            RocketMQError::illegal_argument(format!(
                "invalid proxy remoting listen address '{}': {error}",
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
    pub broker_cluster_name: String,
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
            broker_cluster_name: "DefaultCluster".to_owned(),
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
pub struct LocalConfig {
    pub broker_cluster_name: String,
    pub broker_name: String,
    pub broker_ip: String,
    pub broker_listen_port: u16,
    pub store_root_dir: String,
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            broker_cluster_name: "DefaultCluster".to_owned(),
            broker_name: "rocketmq-proxy-local".to_owned(),
            broker_ip: "127.0.0.1".to_owned(),
            broker_listen_port: 10911,
            store_root_dir: "store/proxy/local-broker".to_owned(),
        }
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

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct ProxyAuthConfig {
    pub config_name: String,
    pub cluster_name: String,
    pub auth_config_path: String,
    pub acl_file: String,
    pub acl_file_watch_enabled: bool,
    pub acl_file_watch_interval_millis: u64,
    pub authentication_enabled: bool,
    pub authentication_provider: String,
    pub authentication_metadata_provider: String,
    pub authentication_strategy: String,
    pub authentication_whitelist: Vec<String>,
    pub init_authentication_user: String,
    pub inner_client_authentication_credentials: String,
    pub signature_algorithm: SignatureAlgorithm,
    pub request_timestamp_expired_millis: u64,
    pub authorization_enabled: bool,
    pub authorization_provider: String,
    pub authorization_metadata_provider: String,
    pub authorization_strategy: String,
    pub authorization_whitelist: Vec<String>,
    pub migrate_auth_from_v1_enabled: bool,
    pub user_cache_max_num: u32,
    pub user_cache_expired_second: u32,
    pub user_cache_refresh_second: u32,
    pub acl_cache_max_num: u32,
    pub acl_cache_expired_second: u32,
    pub acl_cache_refresh_second: u32,
    pub stateful_authentication_cache_max_num: u32,
    pub stateful_authentication_cache_expired_second: u32,
    pub stateful_authorization_cache_max_num: u32,
    pub stateful_authorization_cache_expired_second: u32,
    pub stateful_authorization_cache_negative_enable: bool,
}

impl fmt::Debug for ProxyAuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProxyAuthConfig")
            .field("config_name", &self.config_name)
            .field("cluster_name", &self.cluster_name)
            .field("auth_config_path", &self.auth_config_path)
            .field("acl_file", &self.acl_file)
            .field("acl_file_watch_enabled", &self.acl_file_watch_enabled)
            .field("acl_file_watch_interval_millis", &self.acl_file_watch_interval_millis)
            .field("authentication_enabled", &self.authentication_enabled)
            .field("authentication_provider", &self.authentication_provider)
            .field(
                "authentication_metadata_provider",
                &self.authentication_metadata_provider,
            )
            .field("authentication_strategy", &self.authentication_strategy)
            .field("authentication_whitelist", &self.authentication_whitelist)
            .field(
                "init_authentication_user",
                &redacted_config_value(&self.init_authentication_user),
            )
            .field(
                "inner_client_authentication_credentials",
                &redacted_config_value(&self.inner_client_authentication_credentials),
            )
            .field("signature_algorithm", &self.signature_algorithm)
            .field(
                "request_timestamp_expired_millis",
                &self.request_timestamp_expired_millis,
            )
            .field("authorization_enabled", &self.authorization_enabled)
            .field("authorization_provider", &self.authorization_provider)
            .field("authorization_metadata_provider", &self.authorization_metadata_provider)
            .field("authorization_strategy", &self.authorization_strategy)
            .field("authorization_whitelist", &self.authorization_whitelist)
            .field("migrate_auth_from_v1_enabled", &self.migrate_auth_from_v1_enabled)
            .field("user_cache_max_num", &self.user_cache_max_num)
            .field("user_cache_expired_second", &self.user_cache_expired_second)
            .field("user_cache_refresh_second", &self.user_cache_refresh_second)
            .field("acl_cache_max_num", &self.acl_cache_max_num)
            .field("acl_cache_expired_second", &self.acl_cache_expired_second)
            .field("acl_cache_refresh_second", &self.acl_cache_refresh_second)
            .field(
                "stateful_authentication_cache_max_num",
                &self.stateful_authentication_cache_max_num,
            )
            .field(
                "stateful_authentication_cache_expired_second",
                &self.stateful_authentication_cache_expired_second,
            )
            .field(
                "stateful_authorization_cache_max_num",
                &self.stateful_authorization_cache_max_num,
            )
            .field(
                "stateful_authorization_cache_expired_second",
                &self.stateful_authorization_cache_expired_second,
            )
            .field(
                "stateful_authorization_cache_negative_enable",
                &self.stateful_authorization_cache_negative_enable,
            )
            .finish()
    }
}

fn redacted_config_value(value: &str) -> Option<&'static str> {
    if value.is_empty() {
        None
    } else {
        Some("<redacted>")
    }
}

impl Default for ProxyAuthConfig {
    fn default() -> Self {
        Self {
            config_name: "rocketmq-proxy".to_owned(),
            cluster_name: "DefaultCluster".to_owned(),
            auth_config_path: "store/proxy/auth".to_owned(),
            acl_file: String::new(),
            acl_file_watch_enabled: false,
            acl_file_watch_interval_millis: 5_000,
            authentication_enabled: false,
            authentication_provider: String::new(),
            authentication_metadata_provider: String::new(),
            authentication_strategy: String::new(),
            authentication_whitelist: Vec::new(),
            init_authentication_user: String::new(),
            inner_client_authentication_credentials: String::new(),
            signature_algorithm: SignatureAlgorithm::default(),
            request_timestamp_expired_millis: 0,
            authorization_enabled: false,
            authorization_provider: String::new(),
            authorization_metadata_provider: String::new(),
            authorization_strategy: String::new(),
            authorization_whitelist: Vec::new(),
            migrate_auth_from_v1_enabled: false,
            user_cache_max_num: 1000,
            user_cache_expired_second: 600,
            user_cache_refresh_second: 60,
            acl_cache_max_num: 1000,
            acl_cache_expired_second: 600,
            acl_cache_refresh_second: 60,
            stateful_authentication_cache_max_num: 10000,
            stateful_authentication_cache_expired_second: 60,
            stateful_authorization_cache_max_num: 10000,
            stateful_authorization_cache_expired_second: 60,
            stateful_authorization_cache_negative_enable: false,
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
            acl_file: CheetahString::from(self.acl_file.as_str()),
            acl_file_watch_enabled: self.acl_file_watch_enabled,
            acl_file_watch_interval_millis: self.acl_file_watch_interval_millis,
            authentication_enabled: self.authentication_enabled,
            authentication_provider: CheetahString::from(self.authentication_provider.as_str()),
            authentication_metadata_provider: CheetahString::from(self.authentication_metadata_provider.as_str()),
            authentication_strategy: CheetahString::from(self.authentication_strategy.as_str()),
            authentication_whitelist: CheetahString::from(self.authentication_whitelist.join(",")),
            init_authentication_user: CheetahString::from(self.init_authentication_user.as_str()),
            inner_client_authentication_credentials: CheetahString::from(
                self.inner_client_authentication_credentials.as_str(),
            ),
            signature_algorithm: self.signature_algorithm,
            request_timestamp_expired_millis: self.request_timestamp_expired_millis,
            authorization_enabled: self.authorization_enabled,
            authorization_provider: CheetahString::from(self.authorization_provider.as_str()),
            authorization_metadata_provider: CheetahString::from(self.authorization_metadata_provider.as_str()),
            authorization_strategy: CheetahString::from(self.authorization_strategy.as_str()),
            authorization_whitelist: CheetahString::from(self.authorization_whitelist.join(",")),
            migrate_auth_from_v1_enabled: self.migrate_auth_from_v1_enabled,
            user_cache_max_num: self.user_cache_max_num,
            user_cache_expired_second: self.user_cache_expired_second,
            user_cache_refresh_second: self.user_cache_refresh_second,
            acl_cache_max_num: self.acl_cache_max_num,
            acl_cache_expired_second: self.acl_cache_expired_second,
            acl_cache_refresh_second: self.acl_cache_refresh_second,
            stateful_authentication_cache_max_num: self.stateful_authentication_cache_max_num,
            stateful_authentication_cache_expired_second: self.stateful_authentication_cache_expired_second,
            stateful_authorization_cache_max_num: self.stateful_authorization_cache_max_num,
            stateful_authorization_cache_expired_second: self.stateful_authorization_cache_expired_second,
            stateful_authorization_cache_negative_enable: self.stateful_authorization_cache_negative_enable,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default, rename_all = "camelCase")]
pub struct ProxyConfig {
    pub mode: ProxyMode,
    pub enable_acl_rpc_hook_for_cluster_mode: bool,
    pub grpc: GrpcConfig,
    pub remoting: RemotingConfig,
    pub cluster: ClusterConfig,
    pub local: LocalConfig,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_auth_config_maps_acl_file_fields_to_auth_config() {
        let config = ProxyAuthConfig {
            acl_file: "conf/plain_acl.yml".to_owned(),
            acl_file_watch_enabled: true,
            acl_file_watch_interval_millis: 250,
            authentication_provider: "org.apache.rocketmq.auth.authentication.provider.DefaultAuthenticationProvider"
                .to_owned(),
            authentication_metadata_provider: "org.apache.rocketmq.auth.authentication.provider.\
                                               LocalAuthenticationMetadataProvider"
                .to_owned(),
            authentication_strategy: "org.apache.rocketmq.auth.authentication.strategy.StatefulAuthenticationStrategy"
                .to_owned(),
            authorization_provider: "org.apache.rocketmq.auth.authorization.provider.DefaultAuthorizationProvider"
                .to_owned(),
            authorization_metadata_provider: "org.apache.rocketmq.auth.authorization.provider.\
                                              LocalAuthorizationMetadataProvider"
                .to_owned(),
            authorization_strategy: "org.apache.rocketmq.auth.authorization.strategy.StatefulAuthorizationStrategy"
                .to_owned(),
            signature_algorithm: SignatureAlgorithm::HmacSha256,
            request_timestamp_expired_millis: 300_000,
            migrate_auth_from_v1_enabled: true,
            user_cache_max_num: 11,
            user_cache_expired_second: 12,
            user_cache_refresh_second: 13,
            acl_cache_max_num: 21,
            acl_cache_expired_second: 22,
            acl_cache_refresh_second: 23,
            stateful_authentication_cache_max_num: 31,
            stateful_authentication_cache_expired_second: 32,
            stateful_authorization_cache_max_num: 41,
            stateful_authorization_cache_expired_second: 42,
            stateful_authorization_cache_negative_enable: true,
            ..ProxyAuthConfig::default()
        };

        let auth_config = config.to_auth_config();

        assert_eq!(auth_config.acl_file.as_str(), "conf/plain_acl.yml");
        assert!(auth_config.acl_file_watch_enabled);
        assert_eq!(auth_config.acl_file_watch_interval_millis, 250);
        assert_eq!(
            auth_config.authentication_provider.as_str(),
            config.authentication_provider.as_str()
        );
        assert_eq!(
            auth_config.authentication_metadata_provider.as_str(),
            config.authentication_metadata_provider.as_str()
        );
        assert_eq!(
            auth_config.authentication_strategy.as_str(),
            config.authentication_strategy.as_str()
        );
        assert_eq!(
            auth_config.authorization_provider.as_str(),
            config.authorization_provider.as_str()
        );
        assert_eq!(
            auth_config.authorization_metadata_provider.as_str(),
            config.authorization_metadata_provider.as_str()
        );
        assert_eq!(
            auth_config.authorization_strategy.as_str(),
            config.authorization_strategy.as_str()
        );
        assert_eq!(auth_config.signature_algorithm, SignatureAlgorithm::HmacSha256);
        assert_eq!(auth_config.request_timestamp_expired_millis, 300_000);
        assert!(auth_config.migrate_auth_from_v1_enabled);
        assert_eq!(auth_config.user_cache_max_num, 11);
        assert_eq!(auth_config.user_cache_expired_second, 12);
        assert_eq!(auth_config.user_cache_refresh_second, 13);
        assert_eq!(auth_config.acl_cache_max_num, 21);
        assert_eq!(auth_config.acl_cache_expired_second, 22);
        assert_eq!(auth_config.acl_cache_refresh_second, 23);
        assert_eq!(auth_config.stateful_authentication_cache_max_num, 31);
        assert_eq!(auth_config.stateful_authentication_cache_expired_second, 32);
        assert_eq!(auth_config.stateful_authorization_cache_max_num, 41);
        assert_eq!(auth_config.stateful_authorization_cache_expired_second, 42);
        assert!(auth_config.stateful_authorization_cache_negative_enable);
    }

    #[test]
    fn proxy_auth_config_deserializes_acl_file_camel_case_keys() {
        let config: ProxyAuthConfig = config::Config::builder()
            .add_source(config::File::from_str(
                r#"
aclFile: conf/plain_acl.yml
aclFileWatchEnabled: true
aclFileWatchIntervalMillis: 250
signatureAlgorithm: HmacMD5
requestTimestampExpiredMillis: 300000
migrateAuthFromV1Enabled: true
userCacheMaxNum: 11
userCacheExpiredSecond: 12
userCacheRefreshSecond: 13
aclCacheMaxNum: 21
aclCacheExpiredSecond: 22
aclCacheRefreshSecond: 23
statefulAuthenticationCacheMaxNum: 31
statefulAuthenticationCacheExpiredSecond: 32
statefulAuthorizationCacheMaxNum: 41
statefulAuthorizationCacheExpiredSecond: 42
statefulAuthorizationCacheNegativeEnable: true
"#,
                config::FileFormat::Yaml,
            ))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();

        assert_eq!(config.acl_file, "conf/plain_acl.yml");
        assert!(config.acl_file_watch_enabled);
        assert_eq!(config.acl_file_watch_interval_millis, 250);
        assert_eq!(config.signature_algorithm, SignatureAlgorithm::HmacMd5);
        assert_eq!(config.request_timestamp_expired_millis, 300_000);
        assert!(config.migrate_auth_from_v1_enabled);
        assert_eq!(config.user_cache_max_num, 11);
        assert_eq!(config.user_cache_expired_second, 12);
        assert_eq!(config.user_cache_refresh_second, 13);
        assert_eq!(config.acl_cache_max_num, 21);
        assert_eq!(config.acl_cache_expired_second, 22);
        assert_eq!(config.acl_cache_refresh_second, 23);
        assert_eq!(config.stateful_authentication_cache_max_num, 31);
        assert_eq!(config.stateful_authentication_cache_expired_second, 32);
        assert_eq!(config.stateful_authorization_cache_max_num, 41);
        assert_eq!(config.stateful_authorization_cache_expired_second, 42);
        assert!(config.stateful_authorization_cache_negative_enable);
    }

    #[test]
    fn proxy_config_deserializes_acl_rpc_hook_flag() {
        let config: ProxyConfig = config::Config::builder()
            .add_source(config::File::from_str(
                r#"
enableAclRpcHookForClusterMode: true
"#,
                config::FileFormat::Yaml,
            ))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();

        assert!(config.enable_acl_rpc_hook_for_cluster_mode);
    }

    #[test]
    fn proxy_auth_config_debug_redacts_embedded_credentials() {
        let config = ProxyAuthConfig {
            init_authentication_user: "admin:init-secret".to_owned(),
            inner_client_authentication_credentials: r#"{"accessKey":"inner","secretKey":"inner-secret"}"#.to_owned(),
            ..ProxyAuthConfig::default()
        };

        let output = format!("{config:?}");

        assert!(!output.contains("init-secret"));
        assert!(!output.contains("inner-secret"));
        assert!(output.contains("<redacted>"));
    }
}
