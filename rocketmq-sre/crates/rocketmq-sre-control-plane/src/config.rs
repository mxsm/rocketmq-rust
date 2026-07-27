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

use std::fmt::Debug;
use std::fmt::Formatter;
#[cfg(test)]
use std::net::IpAddr;
#[cfg(test)]
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use url::Url;

use crate::ControlPlaneError;
use crate::DEFAULT_CONNECTOR_CHANNEL_PORT;
use crate::DEFAULT_CONTROL_PLANE_PORT;

/// Process configuration loaded from explicit environment variables.
#[derive(Clone)]
pub struct ControlPlaneConfig {
    bind_addr: SocketAddr,
    connector_bind_addr: SocketAddr,
    database_url: String,
    database_max_connections: u32,
    shutdown_timeout: Duration,
    internal_token: String,
    grant_signing_key: String,
    agent_ack_verification_key: String,
    executor_url: Option<Url>,
    executor_token: Option<String>,
    executor_timeout: Duration,
    executor_allow_insecure_http: bool,
    dev_auth_enabled: bool,
    oidc_issuer: Option<String>,
    oidc_audience: Option<String>,
    oidc_jwks_url: Option<Url>,
    oidc_ca_path: Option<PathBuf>,
    dashboard_deep_link_origins: Vec<String>,
}

impl ControlPlaneConfig {
    /// Loads configuration without logging the database URL.
    ///
    /// # Errors
    ///
    /// Returns [`ControlPlaneError::Configuration`] for missing or malformed
    /// required values.
    pub fn from_env() -> Result<Self, ControlPlaneError> {
        let bind_addr = std::env::var("ROCKETMQ_SRE_BIND_ADDR")
            .unwrap_or_else(|_| format!("0.0.0.0:{DEFAULT_CONTROL_PLANE_PORT}"))
            .parse()
            .map_err(|error| ControlPlaneError::configuration(format!("ROCKETMQ_SRE_BIND_ADDR is invalid: {error}")))?;
        let connector_bind_addr = std::env::var("ROCKETMQ_SRE_CONNECTOR_BIND_ADDR")
            .unwrap_or_else(|_| format!("127.0.0.1:{DEFAULT_CONNECTOR_CHANNEL_PORT}"))
            .parse()
            .map_err(|error| {
                ControlPlaneError::configuration(format!("ROCKETMQ_SRE_CONNECTOR_BIND_ADDR is invalid: {error}"))
            })?;
        if bind_addr == connector_bind_addr {
            return Err(ControlPlaneError::configuration(
                "public and Connector-only listeners must use different addresses",
            ));
        }
        let database_url = std::env::var("DATABASE_URL")
            .map_err(|_| ControlPlaneError::configuration("DATABASE_URL must be configured"))?;
        if database_url.trim().is_empty() {
            return Err(ControlPlaneError::configuration("DATABASE_URL must not be empty"));
        }
        let database_max_connections = parse_env("ROCKETMQ_SRE_DATABASE_MAX_CONNECTIONS", 10)?;
        if database_max_connections == 0 {
            return Err(ControlPlaneError::configuration(
                "ROCKETMQ_SRE_DATABASE_MAX_CONNECTIONS must be greater than zero",
            ));
        }
        let shutdown_seconds = parse_env("ROCKETMQ_SRE_SHUTDOWN_SECONDS", 30)?;
        if shutdown_seconds == 0 {
            return Err(ControlPlaneError::configuration(
                "ROCKETMQ_SRE_SHUTDOWN_SECONDS must be greater than zero",
            ));
        }
        let internal_token = std::env::var("ROCKETMQ_SRE_INTERNAL_TOKEN")
            .map_err(|_| ControlPlaneError::configuration("ROCKETMQ_SRE_INTERNAL_TOKEN must be configured"))?;
        if internal_token.trim().is_empty() {
            return Err(ControlPlaneError::configuration(
                "ROCKETMQ_SRE_INTERNAL_TOKEN must not be empty",
            ));
        }
        let dev_auth_enabled = parse_env("ROCKETMQ_SRE_DEV_AUTH", false)?;
        let grant_signing_key = match optional_env("ROCKETMQ_SRE_GRANT_SIGNING_KEY") {
            Some(value) if value.len() >= 32 => value,
            Some(_) => {
                return Err(ControlPlaneError::configuration(
                    "ROCKETMQ_SRE_GRANT_SIGNING_KEY must contain at least 32 bytes",
                ));
            }
            None if dev_auth_enabled => internal_token.clone(),
            None => {
                return Err(ControlPlaneError::configuration(
                    "production mode requires ROCKETMQ_SRE_GRANT_SIGNING_KEY",
                ));
            }
        };
        let agent_ack_verification_key = match optional_env("ROCKETMQ_SRE_AGENT_ACK_KEY") {
            Some(value) if value.len() >= 32 => value,
            Some(_) => {
                return Err(ControlPlaneError::configuration(
                    "ROCKETMQ_SRE_AGENT_ACK_KEY must contain at least 32 bytes",
                ));
            }
            None if dev_auth_enabled => internal_token.clone(),
            None => {
                return Err(ControlPlaneError::configuration(
                    "production mode requires ROCKETMQ_SRE_AGENT_ACK_KEY",
                ));
            }
        };
        let executor_url: Option<Url> = optional_env("ROCKETMQ_SRE_EXECUTOR_URL")
            .map(|value| {
                value.parse().map_err(|error| {
                    ControlPlaneError::configuration(format!("ROCKETMQ_SRE_EXECUTOR_URL is invalid: {error}"))
                })
            })
            .transpose()?;
        let executor_token = optional_env("ROCKETMQ_SRE_CONTROL_PLANE_EXECUTOR_TOKEN");
        if executor_url.is_some() != executor_token.is_some() {
            return Err(ControlPlaneError::configuration(
                "ROCKETMQ_SRE_EXECUTOR_URL and ROCKETMQ_SRE_CONTROL_PLANE_EXECUTOR_TOKEN must be configured together",
            ));
        }
        let executor_timeout_seconds = parse_env("ROCKETMQ_SRE_EXECUTOR_TIMEOUT_SECONDS", 30_u64)?;
        if executor_timeout_seconds == 0 || executor_timeout_seconds > 300 {
            return Err(ControlPlaneError::configuration(
                "ROCKETMQ_SRE_EXECUTOR_TIMEOUT_SECONDS must be between 1 and 300",
            ));
        }
        let executor_allow_insecure_http = parse_env("ROCKETMQ_SRE_EXECUTOR_ALLOW_INSECURE_HTTP", false)?;
        if executor_allow_insecure_http && !dev_auth_enabled {
            return Err(ControlPlaneError::configuration(
                "plaintext Executor transport is allowed only with development auth",
            ));
        }
        if let Some(url) = &executor_url {
            validate_internal_service_url(url, executor_allow_insecure_http)?;
        }
        let oidc_issuer = optional_env("ROCKETMQ_SRE_OIDC_ISSUER");
        let oidc_audience = optional_env("ROCKETMQ_SRE_OIDC_AUDIENCE");
        let oidc_jwks_url = optional_env("ROCKETMQ_SRE_OIDC_JWKS_URL")
            .map(|value| {
                value.parse().map_err(|error| {
                    ControlPlaneError::configuration(format!("ROCKETMQ_SRE_OIDC_JWKS_URL is invalid: {error}"))
                })
            })
            .transpose()?;
        let oidc_ca_path = optional_env("ROCKETMQ_SRE_OIDC_CA_PATH").map(PathBuf::from);
        let dashboard_deep_link_origins = optional_env("ROCKETMQ_SRE_DASHBOARD_ORIGINS")
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|origin| !origin.is_empty())
                    .map(ToOwned::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        if !dev_auth_enabled && (oidc_issuer.is_none() || oidc_audience.is_none() || oidc_jwks_url.is_none()) {
            return Err(ControlPlaneError::configuration(
                "production mode requires ROCKETMQ_SRE_OIDC_ISSUER, ROCKETMQ_SRE_OIDC_AUDIENCE, and \
                 ROCKETMQ_SRE_OIDC_JWKS_URL",
            ));
        }
        Ok(Self {
            bind_addr,
            connector_bind_addr,
            database_url,
            database_max_connections,
            shutdown_timeout: Duration::from_secs(shutdown_seconds),
            internal_token,
            grant_signing_key,
            agent_ack_verification_key,
            executor_url,
            executor_token,
            executor_timeout: Duration::from_secs(executor_timeout_seconds),
            executor_allow_insecure_http,
            dev_auth_enabled,
            oidc_issuer,
            oidc_audience,
            oidc_jwks_url,
            oidc_ca_path,
            dashboard_deep_link_origins,
        })
    }

    #[must_use]
    pub const fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    /// Returns the private listener used only behind the mTLS Connector proxy.
    #[must_use]
    pub const fn connector_bind_addr(&self) -> SocketAddr {
        self.connector_bind_addr
    }

    #[must_use]
    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    #[must_use]
    pub const fn database_max_connections(&self) -> u32 {
        self.database_max_connections
    }

    #[must_use]
    pub const fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }

    #[must_use]
    pub fn internal_token(&self) -> &str {
        &self.internal_token
    }

    /// Returns the process-local key used to sign short-lived Executor grants.
    ///
    /// The value must never be logged or returned by a public API.
    #[must_use]
    pub fn grant_signing_key(&self) -> &str {
        &self.grant_signing_key
    }

    /// Returns the key used only to verify Execution Agent fence acknowledgements.
    #[must_use]
    pub fn agent_ack_verification_key(&self) -> &str {
        &self.agent_ack_verification_key
    }

    #[must_use]
    pub const fn executor_url(&self) -> Option<&Url> {
        self.executor_url.as_ref()
    }

    #[must_use]
    pub fn executor_token(&self) -> Option<&str> {
        self.executor_token.as_deref()
    }

    #[must_use]
    pub const fn executor_timeout(&self) -> Duration {
        self.executor_timeout
    }

    #[must_use]
    pub const fn executor_allow_insecure_http(&self) -> bool {
        self.executor_allow_insecure_http
    }

    #[must_use]
    pub const fn dev_auth_enabled(&self) -> bool {
        self.dev_auth_enabled
    }

    #[must_use]
    pub fn oidc_issuer(&self) -> Option<&str> {
        self.oidc_issuer.as_deref()
    }

    #[must_use]
    pub fn oidc_audience(&self) -> Option<&str> {
        self.oidc_audience.as_deref()
    }

    #[must_use]
    pub const fn oidc_jwks_url(&self) -> Option<&Url> {
        self.oidc_jwks_url.as_ref()
    }

    #[must_use]
    pub fn oidc_ca_path(&self) -> Option<&std::path::Path> {
        self.oidc_ca_path.as_deref()
    }

    #[must_use]
    pub fn dashboard_deep_link_origins(&self) -> &[String] {
        &self.dashboard_deep_link_origins
    }

    #[cfg(test)]
    pub(crate) fn test_config() -> Self {
        Self {
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            connector_bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1),
            database_url: "postgres://redacted".to_owned(),
            database_max_connections: 1,
            shutdown_timeout: Duration::from_secs(1),
            internal_token: "test-internal-token".to_owned(),
            grant_signing_key: "test-grant-signing-key-at-least-32-bytes".to_owned(),
            agent_ack_verification_key: "test-agent-ack-key-at-least-32-bytes".to_owned(),
            executor_url: None,
            executor_token: None,
            executor_timeout: Duration::from_secs(1),
            executor_allow_insecure_http: false,
            dev_auth_enabled: true,
            oidc_issuer: None,
            oidc_audience: None,
            oidc_jwks_url: None,
            oidc_ca_path: None,
            dashboard_deep_link_origins: Vec::new(),
        }
    }
}

impl Debug for ControlPlaneConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControlPlaneConfig")
            .field("bind_addr", &self.bind_addr)
            .field("connector_bind_addr", &self.connector_bind_addr)
            .field("database_url", &"[REDACTED]")
            .field("database_max_connections", &self.database_max_connections)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field("internal_token", &"[REDACTED]")
            .field("grant_signing_key", &"[REDACTED]")
            .field("agent_ack_verification_key", &"[REDACTED]")
            .field("executor_url", &self.executor_url)
            .field("executor_token", &"[REDACTED]")
            .field("executor_timeout", &self.executor_timeout)
            .field("executor_allow_insecure_http", &self.executor_allow_insecure_http)
            .field("dev_auth_enabled", &self.dev_auth_enabled)
            .field("oidc_issuer", &self.oidc_issuer)
            .field("oidc_audience", &self.oidc_audience)
            .field("oidc_jwks_url", &self.oidc_jwks_url)
            .field("oidc_ca_path", &self.oidc_ca_path)
            .field(
                "dashboard_deep_link_origin_count",
                &self.dashboard_deep_link_origins.len(),
            )
            .finish()
    }
}

fn optional_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.trim().is_empty())
}

pub(crate) fn validate_internal_service_url(url: &Url, allow_insecure_http: bool) -> Result<(), ControlPlaneError> {
    let transport_allowed = if allow_insecure_http {
        matches!(url.scheme(), "http" | "https")
    } else {
        url.scheme() == "https"
    };
    let authority_is_clean = url.host_str().is_some() && url.username().is_empty() && url.password().is_none();
    let root_only = matches!(url.path(), "" | "/") && url.query().is_none() && url.fragment().is_none();
    if transport_allowed && !url.cannot_be_a_base() && authority_is_clean && root_only {
        Ok(())
    } else {
        Err(ControlPlaneError::configuration(
            "internal service URL must be an HTTP(S) origin without credentials, path, query, or fragment",
        ))
    }
}

fn parse_env<T>(name: &str, default: T) -> Result<T, ControlPlaneError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|error| ControlPlaneError::configuration(format!("{name} is invalid: {error}"))),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(ControlPlaneError::configuration(format!(
            "{name} cannot be read: {error}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_output_redacts_database_credentials() {
        let mut config = ControlPlaneConfig::test_config();
        config.dashboard_deep_link_origins = vec!["https://internal-dashboard.example.test".to_owned()];
        config.executor_url = Some("https://executor.example.test".parse().expect("Executor URL"));
        config.executor_token = Some("executor-workload-secret".to_owned());
        let debug = format!("{config:?}");

        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("postgres://redacted"));
        assert!(!debug.contains("test-internal-token"));
        assert!(!debug.contains("test-grant-signing-key-at-least-32-bytes"));
        assert!(!debug.contains("test-agent-ack-key-at-least-32-bytes"));
        assert!(!debug.contains("executor-workload-secret"));
        assert!(!debug.contains("internal-dashboard"));
        assert!(debug.contains("dashboard_deep_link_origin_count"));
    }

    #[test]
    fn internal_service_urls_are_closed_origins() {
        let valid_tls: Url = "https://executor.example.test:8094".parse().expect("valid URL");
        let valid_dev: Url = "http://executor:8094".parse().expect("valid URL");
        assert!(validate_internal_service_url(&valid_tls, false).is_ok());
        assert!(validate_internal_service_url(&valid_dev, true).is_ok());
        assert!(validate_internal_service_url(&valid_dev, false).is_err());

        for value in [
            "https://user:password@executor.example.test:8094",
            "https://executor.example.test:8094/base",
            "https://executor.example.test:8094?token=secret",
            "https://executor.example.test:8094#fragment",
            "file:///internal/executor",
        ] {
            let url: Url = value.parse().expect("syntactically valid URL");
            assert!(
                validate_internal_service_url(&url, true).is_err(),
                "{value} must be rejected"
            );
        }
    }
}
