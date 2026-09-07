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

use crate::ExecutorError;

pub const DEFAULT_EXECUTOR_PORT: u16 = 8094;

/// Explicit Executor process configuration with redacted workload secrets.
#[derive(Clone)]
pub struct ExecutorConfig {
    pub(crate) bind_addr: SocketAddr,
    pub(crate) database_url: String,
    pub(crate) authority_url: Url,
    pub(crate) authority_token: String,
    pub(crate) agent_url: Url,
    pub(crate) agent_token: String,
    pub(crate) control_plane_token: String,
    pub(crate) executor_subject: String,
    pub(crate) request_timeout: Duration,
    pub(crate) lease_ttl_seconds: u32,
    pub(crate) resource_lock_ttl: Duration,
    pub(crate) verification_poll_interval: Duration,
    pub(crate) shutdown_timeout: Duration,
    pub(crate) dev_insecure_http: bool,
}

impl ExecutorConfig {
    /// Loads the closed environment configuration.
    ///
    /// # Errors
    ///
    /// Rejects missing identities/secrets, insecure production URLs, and
    /// unbounded timeout or lease values.
    pub fn from_env() -> Result<Self, ExecutorError> {
        let bind_addr = env_or(
            "ROCKETMQ_SRE_EXECUTOR_BIND_ADDR",
            &format!("0.0.0.0:{DEFAULT_EXECUTOR_PORT}"),
        )?
        .parse()
        .map_err(ExecutorError::configuration_source)?;
        let database_url = required("DATABASE_URL")?;
        let authority_url = url_env("ROCKETMQ_SRE_LEASE_AUTHORITY_URL")?;
        let authority_token = required("ROCKETMQ_SRE_EXECUTOR_AUTHORITY_TOKEN")?;
        let agent_url = url_env("ROCKETMQ_SRE_EXECUTION_AGENT_URL")?;
        let agent_token = required("ROCKETMQ_SRE_EXECUTOR_AGENT_TOKEN")?;
        let control_plane_token = required("ROCKETMQ_SRE_CONTROL_PLANE_EXECUTOR_TOKEN")?;
        let executor_subject = required("ROCKETMQ_SRE_EXECUTOR_SUBJECT")?;
        let dev_insecure_http = parse_env("ROCKETMQ_SRE_EXECUTOR_DEV_INSECURE_HTTP", false)?;
        validate_internal_service_url(&authority_url, dev_insecure_http)?;
        validate_internal_service_url(&agent_url, dev_insecure_http)?;
        let request_timeout = duration_env("ROCKETMQ_SRE_EXECUTOR_REQUEST_TIMEOUT_SECONDS", 30)?;
        let resource_lock_ttl = duration_env("ROCKETMQ_SRE_EXECUTOR_LOCK_TTL_SECONDS", 300)?;
        let verification_poll_interval = duration_env("ROCKETMQ_SRE_EXECUTOR_VERIFICATION_POLL_SECONDS", 5)?;
        if verification_poll_interval > Duration::from_secs(60) {
            return Err(ExecutorError::Configuration);
        }
        let shutdown_timeout = duration_env("ROCKETMQ_SRE_EXECUTOR_SHUTDOWN_SECONDS", 30)?;
        let lease_ttl_seconds = parse_env("ROCKETMQ_SRE_EXECUTOR_LEASE_TTL_SECONDS", 120_u32)?;
        if !(5..=300).contains(&lease_ttl_seconds) {
            return Err(ExecutorError::Configuration);
        }
        Ok(Self {
            bind_addr,
            database_url,
            authority_url,
            authority_token,
            agent_url,
            agent_token,
            control_plane_token,
            executor_subject,
            request_timeout,
            lease_ttl_seconds,
            resource_lock_ttl,
            verification_poll_interval,
            shutdown_timeout,
            dev_insecure_http,
        })
    }

    #[must_use]
    pub const fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }
}

impl Debug for ExecutorConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutorConfig")
            .field("bind_addr", &self.bind_addr)
            .field("database_url", &"[REDACTED]")
            .field("authority_url", &self.authority_url)
            .field("authority_token", &"[REDACTED]")
            .field("agent_url", &self.agent_url)
            .field("agent_token", &"[REDACTED]")
            .field("control_plane_token", &"[REDACTED]")
            .field("executor_subject", &self.executor_subject)
            .field("request_timeout", &self.request_timeout)
            .field("lease_ttl_seconds", &self.lease_ttl_seconds)
            .field("resource_lock_ttl", &self.resource_lock_ttl)
            .field("verification_poll_interval", &self.verification_poll_interval)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field("dev_insecure_http", &self.dev_insecure_http)
            .finish()
    }
}

fn required(name: &str) -> Result<String, ExecutorError> {
    let value = std::env::var(name).map_err(ExecutorError::configuration_source)?;
    if value.trim().is_empty() {
        return Err(ExecutorError::Configuration);
    }
    Ok(value)
}

fn env_or(name: &str, default: &str) -> Result<String, ExecutorError> {
    match std::env::var(name) {
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => Ok(default.to_owned()),
        Err(error) => Err(ExecutorError::configuration_source(error)),
    }
}

fn parse_env<T>(name: &str, default: T) -> Result<T, ExecutorError>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match std::env::var(name) {
        Ok(value) => value.parse().map_err(ExecutorError::configuration_source),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(ExecutorError::configuration_source(error)),
    }
}

fn duration_env(name: &str, default: u64) -> Result<Duration, ExecutorError> {
    let seconds = parse_env(name, default)?;
    if seconds == 0 || seconds > 3_600 {
        return Err(ExecutorError::Configuration);
    }
    Ok(Duration::from_secs(seconds))
}

fn url_env(name: &str) -> Result<Url, ExecutorError> {
    required(name)?.parse().map_err(ExecutorError::configuration_source)
}

pub(crate) fn validate_internal_service_url(url: &Url, dev_insecure_http: bool) -> Result<(), ExecutorError> {
    let transport_allowed = if dev_insecure_http {
        matches!(url.scheme(), "http" | "https")
    } else {
        url.scheme() == "https"
    };
    let authority_is_clean = url.host_str().is_some() && url.username().is_empty() && url.password().is_none();
    let root_only = matches!(url.path(), "" | "/") && url.query().is_none() && url.fragment().is_none();
    if transport_allowed && !url.cannot_be_a_base() && authority_is_clean && root_only {
        Ok(())
    } else {
        Err(ExecutorError::Configuration)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn configuration_sources_remain_typed_and_redacted() {
        let cases = [
            ExecutorError::configuration_source("invalid-secret".parse::<SocketAddr>().unwrap_err()),
            ExecutorError::configuration_source("invalid-secret".parse::<u64>().unwrap_err()),
            ExecutorError::configuration_source(Url::parse("invalid-secret").unwrap_err()),
            ExecutorError::configuration_source(std::env::VarError::NotUnicode("invalid-secret".into())),
        ];
        assert!(cases[0].source().unwrap().is::<std::net::AddrParseError>());
        assert!(cases[1].source().unwrap().is::<std::num::ParseIntError>());
        assert!(cases[2].source().unwrap().is::<url::ParseError>());
        assert!(cases[3].source().unwrap().is::<std::env::VarError>());
        for error in cases {
            assert_eq!(
                error.http_classification(),
                ExecutorError::Configuration.http_classification()
            );
            assert!(!format!("{error} {error:?}").contains("invalid-secret"));
            assert!(matches!(
                crate::ExecutorRequestFailure::from(error),
                crate::ExecutorRequestFailure::Operational(_)
            ));
        }
    }

    #[test]
    fn plaintext_internal_urls_require_explicit_dev_mode() {
        let plain: Url = "http://execution-agent:8095".parse().expect("URL");
        let tls: Url = "https://execution-agent:8095".parse().expect("URL");
        assert!(validate_internal_service_url(&plain, false).is_err());
        assert!(validate_internal_service_url(&plain, true).is_ok());
        assert!(validate_internal_service_url(&tls, false).is_ok());
    }

    #[test]
    fn internal_service_urls_reject_ambiguous_or_sensitive_parts() {
        for value in [
            "https://user:password@execution-agent:8095",
            "https://execution-agent:8095/base",
            "https://execution-agent:8095?token=secret",
            "https://execution-agent:8095#fragment",
            "file:///internal/agent",
        ] {
            let url: Url = value.parse().expect("syntactically valid URL");
            assert!(
                validate_internal_service_url(&url, true).is_err(),
                "{value} must be rejected"
            );
        }
    }

    #[test]
    fn debug_output_redacts_all_executor_credentials() {
        let config = ExecutorConfig {
            bind_addr: "127.0.0.1:8094".parse().expect("bind address"),
            database_url: "postgres://user:database-secret@postgres/sre".to_owned(),
            authority_url: "https://control-plane.example.test".parse().expect("URL"),
            authority_token: "authority-workload-secret".to_owned(),
            agent_url: "https://execution-agent.example.test".parse().expect("URL"),
            agent_token: "agent-workload-secret".to_owned(),
            control_plane_token: "control-plane-workload-secret".to_owned(),
            executor_subject: "spiffe://rocketmq-sre/executor".to_owned(),
            request_timeout: Duration::from_secs(1),
            lease_ttl_seconds: 30,
            resource_lock_ttl: Duration::from_secs(30),
            verification_poll_interval: Duration::from_secs(1),
            shutdown_timeout: Duration::from_secs(1),
            dev_insecure_http: false,
        };
        let debug = format!("{config:?}");
        assert!(debug.contains("[REDACTED]"));
        for secret in [
            "database-secret",
            "authority-workload-secret",
            "agent-workload-secret",
            "control-plane-workload-secret",
        ] {
            assert!(!debug.contains(secret));
        }
    }
}
