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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::net::SocketAddr;
use std::time::Duration;

use url::Url;

use crate::ExecutionAgentError;

pub const DEFAULT_EXECUTION_AGENT_PORT: u16 = 8095;

/// Explicit process configuration with redacted workload credentials.
#[derive(Clone)]
pub struct ExecutionAgentConfig {
    pub(crate) bind_addr: SocketAddr,
    pub(crate) database_url: String,
    pub(crate) authority_url: Url,
    pub(crate) authority_token: String,
    pub(crate) executor_token: String,
    pub(crate) agent_subject: String,
    pub(crate) ack_signing_key: String,
    pub(crate) request_timeout: Duration,
    pub(crate) driver_timeout: Duration,
    pub(crate) shutdown_timeout: Duration,
    pub(crate) dev_insecure_http: bool,
}

impl ExecutionAgentConfig {
    /// Loads and validates the closed environment configuration.
    ///
    /// # Errors
    ///
    /// Rejects missing secrets, plaintext authority URLs outside the explicit
    /// development profile, short signing keys, and invalid timeouts.
    pub fn from_env() -> Result<Self, ExecutionAgentError> {
        let bind_addr = env_or(
            "ROCKETMQ_SRE_AGENT_BIND_ADDR",
            &format!("0.0.0.0:{DEFAULT_EXECUTION_AGENT_PORT}"),
        )
        .parse()
        .map_err(|_| ExecutionAgentError::Configuration)?;
        let database_url = required("DATABASE_URL")?;
        let authority_url: Url = required("ROCKETMQ_SRE_LEASE_AUTHORITY_URL")?
            .parse()
            .map_err(|_| ExecutionAgentError::Configuration)?;
        let authority_token = required("ROCKETMQ_SRE_AGENT_AUTHORITY_TOKEN")?;
        let executor_token = required("ROCKETMQ_SRE_EXECUTOR_AGENT_TOKEN")?;
        let agent_subject = required("ROCKETMQ_SRE_AGENT_SUBJECT")?;
        let ack_signing_key = required("ROCKETMQ_SRE_AGENT_ACK_KEY")?;
        if ack_signing_key.len() < 32 {
            return Err(ExecutionAgentError::Configuration);
        }
        let dev_insecure_http = parse_env("ROCKETMQ_SRE_AGENT_DEV_INSECURE_HTTP", false)?;
        validate_internal_service_url(&authority_url, dev_insecure_http)?;
        let request_timeout = duration_env("ROCKETMQ_SRE_AGENT_REQUEST_TIMEOUT_SECONDS", 10)?;
        let driver_timeout = duration_env("ROCKETMQ_SRE_AGENT_DRIVER_TIMEOUT_SECONDS", 30)?;
        let shutdown_timeout = duration_env("ROCKETMQ_SRE_AGENT_SHUTDOWN_SECONDS", 30)?;
        Ok(Self {
            bind_addr,
            database_url,
            authority_url,
            authority_token,
            executor_token,
            agent_subject,
            ack_signing_key,
            request_timeout,
            driver_timeout,
            shutdown_timeout,
            dev_insecure_http,
        })
    }

    #[must_use]
    pub const fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }
}

impl Debug for ExecutionAgentConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutionAgentConfig")
            .field("bind_addr", &self.bind_addr)
            .field("database_url", &"[REDACTED]")
            .field("authority_url", &self.authority_url)
            .field("authority_token", &"[REDACTED]")
            .field("executor_token", &"[REDACTED]")
            .field("agent_subject", &self.agent_subject)
            .field("ack_signing_key", &"[REDACTED]")
            .field("request_timeout", &self.request_timeout)
            .field("driver_timeout", &self.driver_timeout)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field("dev_insecure_http", &self.dev_insecure_http)
            .finish()
    }
}

fn required(name: &str) -> Result<String, ExecutionAgentError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or(ExecutionAgentError::Configuration)
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn parse_env<T>(name: &str, default: T) -> Result<T, ExecutionAgentError>
where
    T: std::str::FromStr,
{
    match std::env::var(name) {
        Ok(value) => value.parse().map_err(|_| ExecutionAgentError::Configuration),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(_) => Err(ExecutionAgentError::Configuration),
    }
}

fn duration_env(name: &str, default: u64) -> Result<Duration, ExecutionAgentError> {
    let seconds = parse_env(name, default)?;
    if seconds == 0 || seconds > 300 {
        return Err(ExecutionAgentError::Configuration);
    }
    Ok(Duration::from_secs(seconds))
}

pub(crate) fn validate_internal_service_url(url: &Url, dev_insecure: bool) -> Result<(), ExecutionAgentError> {
    let transport_allowed = if dev_insecure {
        matches!(url.scheme(), "http" | "https")
    } else {
        url.scheme() == "https"
    };
    let authority_is_clean = url.host_str().is_some() && url.username().is_empty() && url.password().is_none();
    let root_only = matches!(url.path(), "" | "/") && url.query().is_none() && url.fragment().is_none();
    if transport_allowed && !url.cannot_be_a_base() && authority_is_clean && root_only {
        Ok(())
    } else {
        Err(ExecutionAgentError::Configuration)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plaintext_authority_requires_explicit_dev_mode() {
        let loopback: Url = "http://127.0.0.1:8090".parse().expect("url");
        let service: Url = "http://control-plane:8090".parse().expect("url");
        assert!(validate_internal_service_url(&loopback, true).is_ok());
        assert!(validate_internal_service_url(&loopback, false).is_err());
        assert!(validate_internal_service_url(&service, true).is_ok());
    }

    #[test]
    fn authority_url_rejects_ambiguous_or_sensitive_parts() {
        for value in [
            "https://user:password@control-plane.example.test",
            "https://control-plane.example.test/base",
            "https://control-plane.example.test?token=secret",
            "https://control-plane.example.test#fragment",
            "file:///internal/authority",
        ] {
            let url: Url = value.parse().expect("syntactically valid URL");
            assert!(
                validate_internal_service_url(&url, true).is_err(),
                "{value} must be rejected"
            );
        }
    }

    #[test]
    fn debug_output_redacts_all_agent_credentials() {
        let config = ExecutionAgentConfig {
            bind_addr: "127.0.0.1:8095".parse().expect("bind address"),
            database_url: "postgres://user:database-secret@postgres/sre".to_owned(),
            authority_url: "https://control-plane.example.test".parse().expect("URL"),
            authority_token: "authority-workload-secret".to_owned(),
            executor_token: "executor-workload-secret".to_owned(),
            agent_subject: "spiffe://rocketmq-sre/execution-agent".to_owned(),
            ack_signing_key: "agent-ack-signing-secret-at-least-32-bytes".to_owned(),
            request_timeout: Duration::from_secs(1),
            driver_timeout: Duration::from_secs(1),
            shutdown_timeout: Duration::from_secs(1),
            dev_insecure_http: false,
        };
        let debug = format!("{config:?}");
        assert!(debug.contains("[REDACTED]"));
        for secret in [
            "database-secret",
            "authority-workload-secret",
            "executor-workload-secret",
            "agent-ack-signing-secret-at-least-32-bytes",
        ] {
            assert!(!debug.contains(secret));
        }
    }
}
