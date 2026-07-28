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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::net::SocketAddr;
use std::time::Duration;

use rocketmq_admin_core::core::security::AdminCredentials;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::TenantId;
use url::Url;

use crate::ExecutionAgentError;

pub const DEFAULT_EXECUTION_AGENT_PORT: u16 = 8095;

#[derive(Clone)]
pub(crate) struct BrokerAdminDriverConfig {
    pub(crate) namesrv_addr: String,
    pub(crate) use_tls: bool,
    pub(crate) request_timeout: Duration,
    pub(crate) shutdown_timeout: Duration,
    pub(crate) read_credentials: Option<AdminCredentials>,
    pub(crate) mutation_credentials: Option<AdminCredentials>,
}

impl Debug for BrokerAdminDriverConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BrokerAdminDriverConfig")
            .field("namesrv_addr", &self.namesrv_addr)
            .field("use_tls", &self.use_tls)
            .field("request_timeout", &self.request_timeout)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field(
                "read_credentials",
                &self.read_credentials.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "mutation_credentials",
                &self.mutation_credentials.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct ProxyRestartDriverConfig {
    pub(crate) targets: BTreeMap<String, u16>,
    pub(crate) verification_base_url: Url,
    pub(crate) verification_token: String,
    pub(crate) tenant_id: TenantId,
    pub(crate) cluster_id: ClusterId,
}

impl Debug for ProxyRestartDriverConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProxyRestartDriverConfig")
            .field("target_count", &self.targets.len())
            .field("verification_base_url", &self.verification_base_url)
            .field("verification_token", &"[REDACTED]")
            .field("tenant_id", &self.tenant_id)
            .field("cluster_id", &self.cluster_id)
            .finish()
    }
}

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
    pub(crate) broker_config_patch_enabled: bool,
    pub(crate) logger_ttl_enabled: bool,
    pub(crate) proxy_scale_out_enabled: bool,
    pub(crate) proxy_scale_targets: BTreeSet<String>,
    pub(crate) proxy_restart: Option<ProxyRestartDriverConfig>,
    pub(crate) broker_admin: Option<BrokerAdminDriverConfig>,
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
        let broker_config_patch_enabled = parse_env("ROCKETMQ_SRE_AGENT_ENABLE_BROKER_CONFIG", false)?;
        let logger_ttl_enabled = parse_env("ROCKETMQ_SRE_AGENT_ENABLE_LOGGER_TTL", false)?;
        let proxy_scale_out_enabled = parse_env("ROCKETMQ_SRE_AGENT_ENABLE_PROXY_SCALE_OUT", false)?;
        let proxy_scale_targets = if proxy_scale_out_enabled {
            parse_kubernetes_targets(&required("ROCKETMQ_SRE_AGENT_PROXY_SCALE_TARGETS")?)?
        } else {
            BTreeSet::new()
        };
        let proxy_restart_enabled = parse_env("ROCKETMQ_SRE_AGENT_ENABLE_PROXY_RESTART", false)?;
        let proxy_restart = if proxy_restart_enabled {
            let verification_base_url = required("ROCKETMQ_SRE_AGENT_VERIFICATION_URL")?
                .parse()
                .map_err(|_| ExecutionAgentError::Configuration)?;
            validate_internal_service_url(&verification_base_url, dev_insecure_http)?;
            Some(ProxyRestartDriverConfig {
                targets: parse_proxy_restart_targets(&required("ROCKETMQ_SRE_AGENT_PROXY_RESTART_TARGETS")?)?,
                verification_base_url,
                verification_token: required("ROCKETMQ_SRE_AGENT_VERIFICATION_TOKEN")?,
                tenant_id: required("ROCKETMQ_SRE_AGENT_TENANT_ID")?
                    .parse()
                    .map_err(|_| ExecutionAgentError::Configuration)?,
                cluster_id: required("ROCKETMQ_SRE_AGENT_CLUSTER_ID")?
                    .parse()
                    .map_err(|_| ExecutionAgentError::Configuration)?,
            })
        } else {
            None
        };
        let broker_admin = broker_admin_from_env(
            dev_insecure_http,
            shutdown_timeout,
            broker_config_patch_enabled || logger_ttl_enabled || proxy_restart_enabled,
        )?;
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
            broker_config_patch_enabled,
            logger_ttl_enabled,
            proxy_scale_out_enabled,
            proxy_scale_targets,
            proxy_restart,
            broker_admin,
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
            .field("broker_config_patch_enabled", &self.broker_config_patch_enabled)
            .field("logger_ttl_enabled", &self.logger_ttl_enabled)
            .field("proxy_scale_out_enabled", &self.proxy_scale_out_enabled)
            .field("proxy_scale_target_count", &self.proxy_scale_targets.len())
            .field("proxy_restart", &self.proxy_restart)
            .field("broker_admin", &self.broker_admin)
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

fn broker_admin_from_env(
    dev_insecure: bool,
    shutdown_timeout: Duration,
    enabled: bool,
) -> Result<Option<BrokerAdminDriverConfig>, ExecutionAgentError> {
    if !enabled {
        return Ok(None);
    }
    let read_credentials = admin_credentials_from_env("READ")?;
    let mutation_credentials = admin_credentials_from_env("MUTATION")?;
    if !dev_insecure && (read_credentials.is_none() || mutation_credentials.is_none()) {
        return Err(ExecutionAgentError::Configuration);
    }
    if read_credentials
        .as_ref()
        .zip(mutation_credentials.as_ref())
        .is_some_and(|(read, mutation)| read.0 == mutation.0)
    {
        return Err(ExecutionAgentError::Configuration);
    }
    Ok(Some(BrokerAdminDriverConfig {
        namesrv_addr: required("ROCKETMQ_SRE_AGENT_NAMESRV_ADDR")?,
        use_tls: parse_env("ROCKETMQ_SRE_AGENT_BROKER_ADMIN_USE_TLS", true)?,
        request_timeout: duration_env("ROCKETMQ_SRE_AGENT_BROKER_ADMIN_TIMEOUT_SECONDS", 10)?,
        shutdown_timeout,
        read_credentials: read_credentials.map(|(_, credentials)| credentials),
        mutation_credentials: mutation_credentials.map(|(_, credentials)| credentials),
    }))
}

fn admin_credentials_from_env(identity: &str) -> Result<Option<(String, AdminCredentials)>, ExecutionAgentError> {
    let access_name = format!("ROCKETMQ_SRE_AGENT_BROKER_{identity}_ACCESS_KEY");
    let secret_name = format!("ROCKETMQ_SRE_AGENT_BROKER_{identity}_SECRET_KEY");
    let token_name = format!("ROCKETMQ_SRE_AGENT_BROKER_{identity}_SECURITY_TOKEN");
    let access_key = optional(&access_name)?;
    let secret_key = optional(&secret_name)?;
    let security_token = optional(&token_name)?;
    match (access_key, secret_key) {
        (None, None) if security_token.is_none() => Ok(None),
        (Some(access_key), Some(secret_key)) => {
            let identity = access_key.clone();
            AdminCredentials::try_new(access_key, secret_key, security_token)
                .map(|credentials| Some((identity, credentials)))
                .map_err(|_| ExecutionAgentError::Configuration)
        }
        _ => Err(ExecutionAgentError::Configuration),
    }
}

fn optional(name: &str) -> Result<Option<String>, ExecutionAgentError> {
    match std::env::var(name) {
        Ok(value) => Ok((!value.trim().is_empty()).then(|| value.trim().to_owned())),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => Err(ExecutionAgentError::Configuration),
    }
}

fn parse_kubernetes_targets(value: &str) -> Result<BTreeSet<String>, ExecutionAgentError> {
    let targets = value
        .split(',')
        .map(str::trim)
        .filter(|target| !target.is_empty())
        .map(|target| {
            let Some((namespace, workload)) = target.split_once('/') else {
                return Err(ExecutionAgentError::Configuration);
            };
            if namespace.is_empty()
                || workload.is_empty()
                || workload.contains('/')
                || namespace.len() > 63
                || workload.len() > 253
                || !dns_name(namespace)
                || !dns_name(workload)
            {
                return Err(ExecutionAgentError::Configuration);
            }
            Ok(format!("{namespace}/{workload}"))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if targets.is_empty() {
        Err(ExecutionAgentError::Configuration)
    } else {
        Ok(targets)
    }
}

fn parse_proxy_restart_targets(value: &str) -> Result<BTreeMap<String, u16>, ExecutionAgentError> {
    let targets = value
        .split(',')
        .map(str::trim)
        .filter(|target| !target.is_empty())
        .map(|target| {
            let Some((resource, port)) = target.split_once('=') else {
                return Err(ExecutionAgentError::Configuration);
            };
            let exact = parse_kubernetes_targets(resource)?;
            let resource = exact.into_iter().next().ok_or(ExecutionAgentError::Configuration)?;
            let port = port
                .parse::<u16>()
                .ok()
                .filter(|port| *port != 0)
                .ok_or(ExecutionAgentError::Configuration)?;
            Ok((resource, port))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    if targets.is_empty() {
        Err(ExecutionAgentError::Configuration)
    } else {
        Ok(targets)
    }
}

fn dns_name(value: &str) -> bool {
    value.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && label
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            && label
                .as_bytes()
                .first()
                .zip(label.as_bytes().last())
                .is_some_and(|(first, last)| *first != b'-' && *last != b'-')
    })
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
            broker_config_patch_enabled: false,
            logger_ttl_enabled: false,
            proxy_scale_out_enabled: false,
            proxy_scale_targets: BTreeSet::new(),
            proxy_restart: None,
            broker_admin: None,
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

    #[test]
    fn broker_admin_debug_redacts_both_isolated_identities() {
        let config = BrokerAdminDriverConfig {
            namesrv_addr: "namesrv:9876".to_owned(),
            use_tls: true,
            request_timeout: Duration::from_secs(1),
            shutdown_timeout: Duration::from_secs(1),
            read_credentials: Some(
                AdminCredentials::try_new("reader-access", "reader-secret", Some("reader-token".to_owned()))
                    .expect("reader identity"),
            ),
            mutation_credentials: Some(
                AdminCredentials::try_new("writer-access", "writer-secret", Some("writer-token".to_owned()))
                    .expect("mutation identity"),
            ),
        };

        let debug = format!("{config:?}");
        assert!(debug.contains("[REDACTED]"));
        for secret in [
            "reader-access",
            "reader-secret",
            "reader-token",
            "writer-access",
            "writer-secret",
            "writer-token",
        ] {
            assert!(!debug.contains(secret));
        }
    }

    #[test]
    fn proxy_scale_targets_are_exact_and_wildcard_free() {
        assert_eq!(
            parse_kubernetes_targets("rocketmq-system/rocketmq-proxy,ops/proxy-canary").expect("target allowlist"),
            BTreeSet::from([
                "ops/proxy-canary".to_owned(),
                "rocketmq-system/rocketmq-proxy".to_owned(),
            ])
        );
        for invalid in [
            "",
            "*/*",
            "rocketmq-system",
            "rocketmq-system/",
            "/rocketmq-proxy",
            "RocketMQ/Proxy",
            "rocketmq-system/proxy/extra",
        ] {
            assert!(parse_kubernetes_targets(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn proxy_restart_targets_bind_exact_workloads_to_remoting_ports() {
        assert_eq!(
            parse_proxy_restart_targets("rocketmq-system/rocketmq-proxy=8080,ops/proxy-canary=18080")
                .expect("target allowlist"),
            BTreeMap::from([
                ("ops/proxy-canary".to_owned(), 18_080),
                ("rocketmq-system/rocketmq-proxy".to_owned(), 8_080),
            ])
        );
        for invalid in [
            "",
            "*/*=8080",
            "rocketmq-system/rocketmq-proxy",
            "rocketmq-system/rocketmq-proxy=0",
            "rocketmq-system/rocketmq-proxy=70000",
            "rocketmq-system/rocketmq-proxy=http://proxy:8080",
        ] {
            assert!(parse_proxy_restart_targets(invalid).is_err(), "{invalid}");
        }
    }
}
