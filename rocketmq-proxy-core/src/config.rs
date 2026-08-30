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
use std::time::Duration;

use rocketmq_error::RocketMQError;
use rocketmq_transport::api::ProxyProtocolConfig;
use serde::Deserialize;
use serde::Serialize;

use crate::ProxyResult;
use crate::DEFAULT_PROXY_GRPC_PORT;
use crate::DEFAULT_PROXY_REMOTING_PORT;

/// Backend mode selected by the Proxy composition layer.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ProxyMode {
    #[default]
    Cluster,
    Local,
}

/// Normalized gRPC ingress configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct GrpcConfig {
    pub listen_addr: String,
    pub max_decoding_message_size: usize,
    pub max_encoding_message_size: usize,
    /// Maximum uncompressed body size accepted for one message.
    pub max_message_body_size: usize,
    pub concurrency_limit_per_connection: usize,
    pub use_endpoint_port_from_request: bool,
    /// Java-compatible delay admission horizon enforced before forwarding to a Broker.
    pub timer_max_delay_ms: u64,
    /// Precision used only to validate and normalize delay admission at the Proxy boundary.
    pub timer_precision_ms: u64,
    /// Built-in TLS policy for the gRPC listener.
    pub tls: GrpcTlsConfig,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            listen_addr: format!("0.0.0.0:{DEFAULT_PROXY_GRPC_PORT}"),
            max_decoding_message_size: 8 * 1024 * 1024,
            max_encoding_message_size: 8 * 1024 * 1024,
            max_message_body_size: 4 * 1024 * 1024,
            concurrency_limit_per_connection: 256,
            use_endpoint_port_from_request: false,
            timer_max_delay_ms: 24 * 60 * 60 * 1_000,
            timer_precision_ms: 1_000,
            tls: GrpcTlsConfig::default(),
        }
    }
}

impl GrpcConfig {
    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(10)
    }

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

/// Client-certificate policy for the built-in Proxy gRPC TLS listener.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum GrpcTlsClientAuth {
    /// Do not request a client certificate.
    #[default]
    None,
    /// Validate a client certificate when one is presented.
    Optional,
    /// Reject clients that do not present a certificate signed by the configured CA.
    Require,
}

/// Reloadable TLS material for the built-in Proxy gRPC listener.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct GrpcTlsConfig {
    pub enabled: bool,
    pub certificate_path: String,
    pub private_key_path: String,
    pub private_key_password: Option<String>,
    pub client_ca_path: Option<String>,
    pub client_auth: GrpcTlsClientAuth,
    pub reload_interval_ms: u64,
}

impl fmt::Debug for GrpcTlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GrpcTlsConfig")
            .field("enabled", &self.enabled)
            .field("certificate_path", &self.certificate_path)
            .field("private_key_path", &self.private_key_path)
            .field(
                "private_key_password",
                &self.private_key_password.as_ref().map(|_| "<redacted>"),
            )
            .field("client_ca_path", &self.client_ca_path)
            .field("client_auth", &self.client_auth)
            .field("reload_interval_ms", &self.reload_interval_ms)
            .finish()
    }
}

impl Default for GrpcTlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            certificate_path: String::new(),
            private_key_path: String::new(),
            private_key_password: None,
            client_ca_path: None,
            client_auth: GrpcTlsClientAuth::None,
            reload_interval_ms: 5_000,
        }
    }
}

impl GrpcTlsConfig {
    /// Validates the complete TLS generation without reading secret material.
    pub fn validate(&self) -> ProxyResult<()> {
        if !self.enabled {
            return Ok(());
        }
        if self.certificate_path.trim().is_empty() {
            return Err(grpc_tls_config_error(
                "grpc.tls.certificatePath",
                "certificatePath is required when gRPC TLS is enabled",
            ));
        }
        if self.private_key_path.trim().is_empty() {
            return Err(grpc_tls_config_error(
                "grpc.tls.privateKeyPath",
                "privateKeyPath is required when gRPC TLS is enabled",
            ));
        }
        if self.reload_interval_ms == 0 {
            return Err(grpc_tls_config_error(
                "grpc.tls.reloadIntervalMs",
                "reloadIntervalMs must be greater than zero",
            ));
        }
        if self.client_auth != GrpcTlsClientAuth::None
            && self.client_ca_path.as_deref().is_none_or(|path| path.trim().is_empty())
        {
            return Err(grpc_tls_config_error(
                "grpc.tls.clientCaPath",
                "clientCaPath is required when client certificate authentication is enabled",
            ));
        }
        Ok(())
    }
}

fn grpc_tls_config_error(key: &'static str, reason: &'static str) -> crate::ProxyError {
    RocketMQError::ConfigInvalidValue {
        key,
        value: "<configured>".to_owned(),
        reason: reason.to_owned(),
    }
    .into()
}

/// Normalized RocketMQ remoting ingress configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct RemotingConfig {
    pub enabled: bool,
    pub listen_addr: String,
    pub proxy_protocol: ProxyProtocolConfig,
}

impl Default for RemotingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_addr: format!("0.0.0.0:{DEFAULT_PROXY_REMOTING_PORT}"),
            proxy_protocol: ProxyProtocolConfig::default(),
        }
    }
}

impl RemotingConfig {
    pub fn validate(&self) -> ProxyResult<()> {
        self.socket_addr()?;
        self.proxy_protocol.validate()?;
        Ok(())
    }

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

/// Runtime admission limits consumed by neutral Proxy services.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct RuntimeConfig {
    pub route_permits: usize,
    pub producer_permits: usize,
    pub consumer_permits: usize,
    /// Maximum number of gRPC consumer response streams retaining broker results.
    pub consumer_response_permits: usize,
    /// Maximum retained bytes across gRPC consumer response streams.
    pub consumer_response_bytes: usize,
    pub client_manager_permits: usize,
    /// Optional route request rate. Zero disables the QPS limiter.
    pub route_rate_per_second: u64,
    /// Optional producer request rate. Zero disables the QPS limiter.
    pub producer_rate_per_second: u64,
    /// Optional consumer request rate. Zero disables the QPS limiter.
    pub consumer_rate_per_second: u64,
    /// Optional client-manager request rate. Zero disables the QPS limiter.
    pub client_manager_rate_per_second: u64,
    /// Process/Pod hard memory limit. Zero selects automatic detection.
    pub process_memory_limit_bytes: u64,
    pub telemetry_queue_capacity: usize,
    pub telemetry_queue_bytes: usize,
    pub telemetry_queue_rate_per_second: u64,
    pub telemetry_queue_max_age_ms: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            route_permits: 512,
            producer_permits: 1024,
            consumer_permits: 1024,
            consumer_response_permits: 1024,
            consumer_response_bytes: 64 * 1024 * 1024,
            client_manager_permits: 512,
            route_rate_per_second: 0,
            producer_rate_per_second: 0,
            consumer_rate_per_second: 0,
            client_manager_rate_per_second: 0,
            process_memory_limit_bytes: 0,
            telemetry_queue_capacity: 1024,
            telemetry_queue_bytes: 16 * 1024 * 1024,
            telemetry_queue_rate_per_second: 4096,
            telemetry_queue_max_age_ms: 30_000,
        }
    }
}

/// Session lifecycle policy consumed by the Core registry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct SessionConfig {
    pub client_ttl_ms: u64,
    pub receipt_handle_ttl_ms: u64,
    pub auto_renew_enabled: bool,
    pub auto_renew_max_inflight: usize,
    pub min_long_polling_timeout_ms: u64,
    pub max_long_polling_timeout_ms: u64,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            client_ttl_ms: 60_000,
            receipt_handle_ttl_ms: 5 * 60_000,
            auto_renew_enabled: true,
            auto_renew_max_inflight: 32,
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

    pub fn auto_renew_max_inflight(&self) -> usize {
        self.auto_renew_max_inflight.max(1)
    }

    pub fn min_long_polling_timeout(&self) -> Duration {
        Duration::from_millis(self.min_long_polling_timeout_ms)
    }

    pub fn max_long_polling_timeout(&self) -> Duration {
        Duration::from_millis(self.max_long_polling_timeout_ms.max(self.min_long_polling_timeout_ms))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingress_defaults_preserve_public_ports() {
        assert_eq!(GrpcConfig::default().listen_port().expect("gRPC port"), 8081);
        assert_eq!(RemotingConfig::default().listen_port().expect("remoting port"), 8080);
    }

    #[test]
    fn grpc_tls_is_plaintext_by_default() {
        let config = GrpcConfig::default();

        assert!(!config.tls.enabled);
        config.tls.validate().expect("plaintext defaults should be valid");
    }

    #[test]
    fn grpc_tls_requires_a_complete_server_identity() {
        let mut config = GrpcTlsConfig {
            enabled: true,
            certificate_path: "server.pem".to_owned(),
            ..GrpcTlsConfig::default()
        };

        let error = config.validate().expect_err("certificate-only TLS must fail");
        assert!(error.to_string().contains("privateKeyPath"), "{error}");

        config.certificate_path.clear();
        config.private_key_path = "server.key".to_owned();
        let error = config.validate().expect_err("key-only TLS must fail");
        assert!(error.to_string().contains("certificatePath"), "{error}");
    }

    #[test]
    fn grpc_mtls_requires_a_client_ca() {
        let config = GrpcTlsConfig {
            enabled: true,
            certificate_path: "server.pem".to_owned(),
            private_key_path: "server.key".to_owned(),
            client_auth: GrpcTlsClientAuth::Require,
            ..GrpcTlsConfig::default()
        };

        let error = config.validate().expect_err("mTLS without client CA must fail");
        assert!(error.to_string().contains("clientCaPath"), "{error}");
    }

    #[test]
    fn session_durations_preserve_bounds() {
        let config = SessionConfig {
            client_ttl_ms: 0,
            receipt_handle_ttl_ms: 0,
            min_long_polling_timeout_ms: 10,
            max_long_polling_timeout_ms: 5,
            ..SessionConfig::default()
        };

        assert_eq!(config.client_ttl(), Duration::from_millis(1));
        assert_eq!(config.receipt_handle_ttl(), Duration::from_millis(1));
        assert_eq!(
            SessionConfig {
                auto_renew_max_inflight: 0,
                ..SessionConfig::default()
            }
            .auto_renew_max_inflight(),
            1
        );
        assert_eq!(config.max_long_polling_timeout(), Duration::from_millis(10));
    }

    #[test]
    fn runtime_rates_default_to_disabled_and_remain_independent() {
        let default = RuntimeConfig::default();
        assert_eq!(default.route_rate_per_second, 0);
        assert_eq!(default.producer_rate_per_second, 0);
        assert_eq!(default.consumer_rate_per_second, 0);
        assert_eq!(default.client_manager_rate_per_second, 0);
        assert_eq!(default.consumer_response_permits, 1024);
        assert_eq!(default.consumer_response_bytes, 64 * 1024 * 1024);

        let config = RuntimeConfig {
            route_permits: 7,
            producer_rate_per_second: 20_000,
            consumer_rate_per_second: 30_000,
            client_manager_rate_per_second: 40_000,
            ..RuntimeConfig::default()
        };

        assert_eq!(config.route_permits, 7);
        assert_eq!(config.route_rate_per_second, 0);
        assert_eq!(config.producer_rate_per_second, 20_000);
        assert_eq!(config.consumer_rate_per_second, 30_000);
        assert_eq!(config.client_manager_rate_per_second, 40_000);
    }
}
