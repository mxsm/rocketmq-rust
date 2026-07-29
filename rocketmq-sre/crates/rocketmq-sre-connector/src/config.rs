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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::env;
use std::fmt;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::path::Component;
use std::path::PathBuf;
use std::time::Duration;

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::TenantId;
use url::Url;

use crate::ConnectorError;

const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8091";
const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 15;
const DEFAULT_HANDSHAKE_INTERVAL_SECONDS: u64 = 15;
const DEFAULT_SHUTDOWN_TIMEOUT_SECONDS: u64 = 10;
const DEFAULT_MAX_CONCURRENCY: usize = 16;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const DEFAULT_INTERNAL_TOKEN_ENV: &str = "ROCKETMQ_SRE_INTERNAL_TOKEN";
const DEFAULT_SOURCE_MAX_ROWS: usize = 500;
const DEFAULT_SOURCE_MAX_TIME_RANGE_SECONDS: u64 = 30 * 24 * 60 * 60;
const DEFAULT_SOURCE_MAX_REQUESTS_PER_MINUTE: usize = 120;
const DEFAULT_SOURCE_CACHE_TTL_SECONDS: u64 = 15;
const DEFAULT_CHANNEL_POLL_SECONDS: u64 = 25;
const DEFAULT_CHANNEL_HEARTBEAT_SECONDS: u64 = 15;
const MAX_SECRET_FILE_BYTES: u64 = 64 * 1024;

/// Secret value whose `Debug` implementation never reveals its contents.
#[derive(Clone)]
pub(crate) struct SecretValue(String);

impl SecretValue {
    pub(crate) fn new(value: String) -> Self {
        Self(value)
    }

    fn from_env(reference: &str) -> Result<Self, ConnectorError> {
        validate_env_reference(reference)?;
        let value = env::var(reference).map_err(|_| {
            ConnectorError::configuration(format!("secret environment reference `{reference}` is not set"))
        })?;
        if value.trim().is_empty() {
            return Err(ConnectorError::configuration(format!(
                "secret environment reference `{reference}` is empty"
            )));
        }
        Ok(Self(value))
    }

    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

/// OAuth2 client-credentials settings. The client secret is resolved by
/// environment-variable reference and is never serialized or logged.
#[derive(Clone)]
pub(crate) struct OAuth2Config {
    pub token_endpoint: Url,
    pub client_id: String,
    pub client_secret_env: String,
    pub audience: String,
    pub scopes: Vec<String>,
    pub(crate) client_secret: SecretValue,
}

impl fmt::Debug for OAuth2Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuth2Config")
            .field("token_endpoint", &self.token_endpoint)
            .field("client_id", &self.client_id)
            .field("client_secret_env", &self.client_secret_env)
            .field("client_secret", &"[REDACTED]")
            .field("audience", &self.audience)
            .field("scopes", &self.scopes)
            .finish()
    }
}

/// Authentication mode for the MCP connection.
#[derive(Clone)]
pub(crate) enum ConnectorAuth {
    OAuth2(OAuth2Config),
    /// A development token is accepted only for an MCP URL whose host is a
    /// loopback IP or `localhost`.
    DevelopmentToken {
        token_env: String,
        token: SecretValue,
    },
}

impl fmt::Debug for ConnectorAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OAuth2(config) => formatter.debug_tuple("OAuth2").field(config).finish(),
            Self::DevelopmentToken { token_env, .. } => formatter
                .debug_struct("DevelopmentToken")
                .field("token_env", token_env)
                .field("token", &"[REDACTED]")
                .finish(),
        }
    }
}

/// Optional reverse-channel target in the AI SRE control plane.
#[derive(Clone)]
pub struct ControlPlaneConfig {
    pub base_url: Url,
    pub cluster_id: ClusterId,
    pub connector_subject: String,
    pub connector_issuer: String,
    pub(crate) ca_pem: Vec<u8>,
    pub(crate) client_identity_pem: Vec<u8>,
    pub poll_wait: Duration,
    pub heartbeat_interval: Duration,
}

impl fmt::Debug for ControlPlaneConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneConfig")
            .field("base_url", &self.base_url)
            .field("cluster_id", &self.cluster_id)
            .field("connector_subject", &self.connector_subject)
            .field("connector_issuer", &self.connector_issuer)
            .field("ca_pem", &"[REDACTED]")
            .field("client_identity_pem", &"[REDACTED]")
            .field("poll_wait", &self.poll_wait)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SourceLimits {
    pub max_rows: usize,
    pub max_bytes: usize,
    pub max_time_range: Duration,
    pub max_deadline: Duration,
    pub max_concurrency: usize,
    pub max_requests_per_minute: usize,
    pub cache_ttl: Duration,
    pub label_allowlist: BTreeSet<String>,
}

#[derive(Clone)]
pub(crate) struct AdminCredentialsConfig {
    pub access_key: SecretValue,
    pub secret_key: SecretValue,
    pub security_token: Option<SecretValue>,
}

impl fmt::Debug for AdminCredentialsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminCredentialsConfig")
            .field("access_key", &"[REDACTED]")
            .field("secret_key", &"[REDACTED]")
            .field("security_token", &self.security_token.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AdminSourceConfig {
    pub namesrv_addr: String,
    pub use_tls: bool,
    pub request_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub credentials: Option<AdminCredentialsConfig>,
}

#[derive(Clone)]
pub(crate) struct ProjectedTokenFile {
    path: PathBuf,
    mount_root: PathBuf,
}

impl ProjectedTokenFile {
    pub(crate) fn try_new(path: PathBuf) -> Result<Self, ConnectorError> {
        if !path.is_absolute()
            || path
                .components()
                .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err(ConnectorError::configuration(
                "Kubernetes projected token path must be absolute and normalized",
            ));
        }
        let parent = path
            .parent()
            .filter(|parent| parent.parent().is_some())
            .ok_or_else(|| {
                ConnectorError::configuration("Kubernetes projected token path must have a dedicated mount directory")
            })?;
        let mount_root = std::fs::canonicalize(parent).map_err(|_| {
            ConnectorError::configuration("Kubernetes projected token mount directory cannot be resolved")
        })?;
        let value = Self { path, mount_root };
        value
            .read()
            .map_err(|_| ConnectorError::configuration("Kubernetes projected token file is invalid"))?;
        Ok(value)
    }

    pub(crate) fn read(&self) -> Result<SecretValue, ConnectorError> {
        // Kubernetes projected volumes atomically rotate `token` through the
        // controlled `..data` symlink. Resolve it for this read, but retain the
        // configured path so the next request observes a new generation.
        let resolved = std::fs::canonicalize(&self.path)
            .map_err(|_| ConnectorError::source("Kubernetes projected credential cannot be resolved"))?;
        if !resolved.starts_with(&self.mount_root) || resolved == self.mount_root {
            return Err(ConnectorError::source(
                "Kubernetes projected credential escaped its configured mount",
            ));
        }
        let metadata = std::fs::metadata(&resolved)
            .map_err(|_| ConnectorError::source("Kubernetes projected credential cannot be inspected"))?;
        if !metadata.is_file() || metadata.len() > MAX_SECRET_FILE_BYTES {
            return Err(ConnectorError::source(
                "Kubernetes projected credential must be a bounded regular file",
            ));
        }
        let value = std::fs::read_to_string(&resolved)
            .map_err(|_| ConnectorError::source("Kubernetes projected credential cannot be read"))?;
        let value = value.trim().to_owned();
        if value.is_empty() || u64::try_from(value.len()).map_or(true, |length| length > MAX_SECRET_FILE_BYTES) {
            return Err(ConnectorError::source(
                "Kubernetes projected credential contains an invalid value",
            ));
        }
        Ok(SecretValue::new(value))
    }
}

impl fmt::Debug for ProjectedTokenFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[PROJECTED_FILE]")
    }
}

#[derive(Clone, Debug)]
pub(crate) enum KubernetesBearerToken {
    DevelopmentEnvironment(SecretValue),
    ProjectedFile(ProjectedTokenFile),
}

#[derive(Clone)]
pub(crate) struct KubernetesSourceConfig {
    pub api_url: Url,
    pub namespace: String,
    pub bearer_token: KubernetesBearerToken,
    pub ca_pem: Vec<u8>,
    pub request_timeout: Duration,
    pub label_allowlist: BTreeSet<String>,
}

impl fmt::Debug for KubernetesSourceConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KubernetesSourceConfig")
            .field("api_url", &self.api_url)
            .field("namespace", &self.namespace)
            .field("bearer_token", &"[REDACTED]")
            .field("ca_pem", &"[REDACTED]")
            .field("request_timeout", &self.request_timeout)
            .field("label_allowlist", &self.label_allowlist)
            .finish()
    }
}

/// Complete connector configuration with explicit tenant and cluster bounds.
#[derive(Clone)]
pub struct ConnectorConfig {
    pub bind_addr: SocketAddr,
    pub mcp_url: Url,
    pub mcp_ca_path: Option<PathBuf>,
    pub(crate) mcp_ca_pem: Vec<u8>,
    pub(crate) auth: ConnectorAuth,
    pub tenant_id: TenantId,
    pub cluster_allowlist: BTreeSet<String>,
    pub cluster_ids: BTreeMap<String, ClusterId>,
    pub request_timeout: Duration,
    pub handshake_interval: Duration,
    pub shutdown_timeout: Duration,
    pub max_concurrency: usize,
    pub max_response_bytes: usize,
    pub expected_tool_surface_digest: Option<String>,
    pub prometheus_url: Option<Url>,
    pub alertmanager_url: Option<Url>,
    pub loki_url: Option<Url>,
    pub tempo_url: Option<Url>,
    pub(crate) admin_source: Option<AdminSourceConfig>,
    pub(crate) kubernetes_source: Option<KubernetesSourceConfig>,
    pub(crate) source_limits: SourceLimits,
    pub internal_token_env: String,
    pub(crate) internal_token: SecretValue,
    pub control_plane: Option<ControlPlaneConfig>,
}

impl fmt::Debug for ConnectorConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorConfig")
            .field("bind_addr", &self.bind_addr)
            .field("mcp_url", &self.mcp_url)
            .field("mcp_ca_path", &self.mcp_ca_path)
            .field("mcp_ca_pem", &"[REDACTED]")
            .field("auth", &self.auth)
            .field("tenant_id", &self.tenant_id)
            .field("cluster_allowlist", &self.cluster_allowlist)
            .field("cluster_ids", &self.cluster_ids)
            .field("request_timeout", &self.request_timeout)
            .field("handshake_interval", &self.handshake_interval)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field("max_concurrency", &self.max_concurrency)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("expected_tool_surface_digest", &self.expected_tool_surface_digest)
            .field("prometheus_url", &self.prometheus_url.as_ref().map(|_| "[CONFIGURED]"))
            .field(
                "alertmanager_url",
                &self.alertmanager_url.as_ref().map(|_| "[CONFIGURED]"),
            )
            .field("loki_url", &self.loki_url.as_ref().map(|_| "[CONFIGURED]"))
            .field("tempo_url", &self.tempo_url.as_ref().map(|_| "[CONFIGURED]"))
            .field("admin_source", &self.admin_source)
            .field("kubernetes_source", &self.kubernetes_source)
            .field("source_limits", &self.source_limits)
            .field("internal_token_env", &self.internal_token_env)
            .field("internal_token", &"[REDACTED]")
            .field("control_plane", &self.control_plane)
            .finish()
    }
}

impl ConnectorConfig {
    /// Loads and validates the connector process environment.
    ///
    /// TLS CA material and static secret values are resolved before the Tokio
    /// runtime starts. Kubernetes projected tokens retain only a validated,
    /// fixed path and are refreshed on the runtime-owned metadata I/O lane for
    /// every bounded request.
    ///
    /// # Errors
    ///
    /// Returns a sanitized configuration error when a required value is
    /// missing, malformed, or violates a fail-closed security invariant.
    pub fn from_env() -> Result<Self, ConnectorError> {
        let bind_addr = parse_env(
            "ROCKETMQ_SRE_CONNECTOR_BIND_ADDR",
            DEFAULT_BIND_ADDR
                .parse::<SocketAddr>()
                .map_err(|_| ConnectorError::configuration("default connector bind address is invalid"))?,
        )?;
        let mcp_url = required_url("ROCKETMQ_SRE_MCP_URL")?;
        if !matches!(mcp_url.scheme(), "http" | "https") {
            return Err(ConnectorError::configuration(
                "ROCKETMQ_SRE_MCP_URL must use HTTP or HTTPS",
            ));
        }
        let mcp_ca_path = env::var_os("ROCKETMQ_SRE_MCP_CA_PATH").map(PathBuf::from);
        let mcp_ca_pem = match &mcp_ca_path {
            Some(path) => std::fs::read(path).map_err(|error| {
                ConnectorError::configuration(format!("MCP CA file `{}` cannot be read: {error}", path.display()))
            })?,
            None => Vec::new(),
        };

        if mcp_url.scheme() == "https" && mcp_ca_pem.is_empty() {
            return Err(ConnectorError::configuration(
                "ROCKETMQ_SRE_MCP_CA_PATH is required for an HTTPS MCP endpoint",
            ));
        }
        if mcp_url.scheme() != "https" && !is_loopback_url(&mcp_url) {
            return Err(ConnectorError::configuration(
                "non-loopback MCP endpoints must use HTTPS",
            ));
        }

        let auth = load_auth(&mcp_url)?;
        let tenant_id = required("ROCKETMQ_SRE_TENANT_ID")
            .or_else(|_| required("ROCKETMQ_SRE_TENANT"))?
            .parse()
            .map_err(|_| ConnectorError::configuration("ROCKETMQ_SRE_TENANT_ID must be a UUID"))?;
        let cluster_allowlist = parse_non_empty_set(
            &required("ROCKETMQ_SRE_CLUSTER_ALLOWLIST").or_else(|_| required("ROCKETMQ_SRE_CLUSTER"))?,
        )?;
        let cluster_ids = load_cluster_ids(&cluster_allowlist)?;
        let request_timeout = Duration::from_secs(parse_env(
            "ROCKETMQ_SRE_REQUEST_TIMEOUT_SECONDS",
            DEFAULT_REQUEST_TIMEOUT_SECONDS,
        )?);
        let handshake_interval = Duration::from_secs(parse_env(
            "ROCKETMQ_SRE_HANDSHAKE_INTERVAL_SECONDS",
            DEFAULT_HANDSHAKE_INTERVAL_SECONDS,
        )?);
        let shutdown_timeout = Duration::from_secs(parse_env(
            "ROCKETMQ_SRE_SHUTDOWN_TIMEOUT_SECONDS",
            DEFAULT_SHUTDOWN_TIMEOUT_SECONDS,
        )?);
        let max_concurrency = parse_env("ROCKETMQ_SRE_MAX_CONCURRENCY", DEFAULT_MAX_CONCURRENCY)?;
        if !(1..=256).contains(&max_concurrency) {
            return Err(ConnectorError::configuration(
                "ROCKETMQ_SRE_MAX_CONCURRENCY must be between 1 and 256",
            ));
        }
        let max_response_bytes = parse_env("ROCKETMQ_SRE_MAX_RESPONSE_BYTES", DEFAULT_MAX_RESPONSE_BYTES)?;
        if !(1024..=16 * 1024 * 1024).contains(&max_response_bytes) {
            return Err(ConnectorError::configuration(
                "ROCKETMQ_SRE_MAX_RESPONSE_BYTES must be between 1024 and 16777216",
            ));
        }
        let expected_tool_surface_digest = optional_non_empty("ROCKETMQ_SRE_EXPECTED_TOOL_SURFACE_DIGEST");
        if let Some(digest) = &expected_tool_surface_digest {
            validate_digest(digest)?;
        }
        let prometheus_url = optional_http_url("ROCKETMQ_SRE_PROMETHEUS_URL")?;
        let alertmanager_url = optional_http_url("ROCKETMQ_SRE_ALERTMANAGER_URL")?;
        let loki_url = optional_http_url("ROCKETMQ_SRE_LOKI_URL")?;
        let tempo_url = optional_http_url("ROCKETMQ_SRE_TEMPO_URL")?;
        let source_limits = load_source_limits(request_timeout, max_concurrency, max_response_bytes)?;
        let admin_source = load_admin_source(request_timeout, shutdown_timeout)?;
        let kubernetes_source = load_kubernetes_source(request_timeout, &source_limits.label_allowlist)?;
        let internal_token_env =
            env::var("ROCKETMQ_SRE_INTERNAL_TOKEN_ENV").unwrap_or_else(|_| DEFAULT_INTERNAL_TOKEN_ENV.to_owned());
        let internal_token = SecretValue::from_env(&internal_token_env)?;
        let control_plane = load_control_plane(&cluster_allowlist, &cluster_ids)?;

        Ok(Self {
            bind_addr,
            mcp_url,
            mcp_ca_path,
            mcp_ca_pem,
            auth,
            tenant_id,
            cluster_allowlist,
            cluster_ids,
            request_timeout,
            handshake_interval,
            shutdown_timeout,
            max_concurrency,
            max_response_bytes,
            expected_tool_surface_digest,
            prometheus_url,
            alertmanager_url,
            loki_url,
            tempo_url,
            admin_source,
            kubernetes_source,
            source_limits,
            internal_token_env,
            internal_token,
            control_plane,
        })
    }

    pub(crate) fn internal_token(&self) -> &str {
        self.internal_token.expose()
    }

    pub(crate) fn pseudonymization_key(&self) -> &[u8] {
        self.internal_token.expose().as_bytes()
    }
}

fn load_auth(mcp_url: &Url) -> Result<ConnectorAuth, ConnectorError> {
    if let Ok(token_env) = env::var("ROCKETMQ_SRE_MCP_STATIC_TOKEN_ENV") {
        if !is_loopback_url(mcp_url) {
            return Err(ConnectorError::configuration(
                "development tokens are allowed only for loopback MCP endpoints",
            ));
        }
        let token = SecretValue::from_env(&token_env)?;
        return Ok(ConnectorAuth::DevelopmentToken { token_env, token });
    }

    let token_endpoint = required_url("ROCKETMQ_SRE_OAUTH_TOKEN_ENDPOINT")?;
    if !matches!(token_endpoint.scheme(), "http" | "https") {
        return Err(ConnectorError::configuration(
            "ROCKETMQ_SRE_OAUTH_TOKEN_ENDPOINT must use HTTP or HTTPS",
        ));
    }
    if token_endpoint.scheme() != "https" && !is_loopback_url(&token_endpoint) {
        return Err(ConnectorError::configuration(
            "non-loopback OAuth token endpoints must use HTTPS",
        ));
    }
    let client_id = required("ROCKETMQ_SRE_OAUTH_CLIENT_ID")?;
    let client_secret_env = required("ROCKETMQ_SRE_OAUTH_CLIENT_SECRET_ENV")?;
    let client_secret = SecretValue::from_env(&client_secret_env)?;
    let audience = required("ROCKETMQ_SRE_OAUTH_AUDIENCE")?;
    let scopes = required("ROCKETMQ_SRE_OAUTH_SCOPE")?
        .split_ascii_whitespace()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if scopes.is_empty() {
        return Err(ConnectorError::configuration(
            "ROCKETMQ_SRE_OAUTH_SCOPE must contain at least one scope",
        ));
    }
    Ok(ConnectorAuth::OAuth2(OAuth2Config {
        token_endpoint,
        client_id,
        client_secret_env,
        audience,
        scopes,
        client_secret,
    }))
}

fn load_control_plane(
    cluster_allowlist: &BTreeSet<String>,
    cluster_ids: &BTreeMap<String, ClusterId>,
) -> Result<Option<ControlPlaneConfig>, ConnectorError> {
    let Some(value) = optional_non_empty("ROCKETMQ_SRE_CONTROL_PLANE_URL") else {
        return Ok(None);
    };
    if cluster_allowlist.len() != 1 {
        return Err(ConnectorError::configuration(
            "control-plane reporting currently requires exactly one allowed cluster",
        ));
    }
    let base_url = Url::parse(&value)
        .map_err(|_| ConnectorError::configuration("ROCKETMQ_SRE_CONTROL_PLANE_URL must be an absolute URL"))?;
    if !matches!(base_url.scheme(), "http" | "https") {
        return Err(ConnectorError::configuration(
            "ROCKETMQ_SRE_CONTROL_PLANE_URL must use HTTP or HTTPS",
        ));
    }
    if base_url.scheme() != "https" && !is_loopback_url(&base_url) {
        return Err(ConnectorError::configuration(
            "non-loopback control-plane endpoints must use HTTPS",
        ));
    }
    let ca_pem = read_optional_pem("ROCKETMQ_SRE_CONTROL_PLANE_CA_PATH")?;
    let client_identity_pem = read_optional_pem("ROCKETMQ_SRE_CONTROL_PLANE_CLIENT_IDENTITY_PATH")?;
    if base_url.scheme() == "https" && (ca_pem.is_empty() || client_identity_pem.is_empty()) {
        return Err(ConnectorError::configuration(
            "HTTPS control-plane channels require a CA bundle and combined PEM client identity",
        ));
    }
    let cluster_id = cluster_ids.values().next().copied().ok_or_else(|| {
        ConnectorError::configuration("control-plane reporting requires an internal cluster identifier")
    })?;
    let connector_subject =
        env::var("ROCKETMQ_SRE_CONNECTOR_SUBJECT").unwrap_or_else(|_| "rocketmq-sre-connector".to_owned());
    let connector_issuer =
        env::var("ROCKETMQ_SRE_CONNECTOR_ISSUER").unwrap_or_else(|_| "rocketmq-sre.local".to_owned());
    let poll_wait = Duration::from_secs(parse_env(
        "ROCKETMQ_SRE_CONTROL_PLANE_POLL_SECONDS",
        DEFAULT_CHANNEL_POLL_SECONDS,
    )?);
    let heartbeat_interval = Duration::from_secs(parse_env(
        "ROCKETMQ_SRE_CONTROL_PLANE_HEARTBEAT_SECONDS",
        DEFAULT_CHANNEL_HEARTBEAT_SECONDS,
    )?);
    if !(Duration::from_secs(1)..=Duration::from_secs(55)).contains(&poll_wait)
        || !(Duration::from_secs(1)..=Duration::from_secs(300)).contains(&heartbeat_interval)
    {
        return Err(ConnectorError::configuration(
            "control-plane poll and heartbeat intervals are outside the supported bounds",
        ));
    }
    Ok(Some(ControlPlaneConfig {
        base_url,
        cluster_id,
        connector_subject,
        connector_issuer,
        ca_pem,
        client_identity_pem,
        poll_wait,
        heartbeat_interval,
    }))
}

fn load_source_limits(
    request_timeout: Duration,
    max_concurrency: usize,
    max_response_bytes: usize,
) -> Result<SourceLimits, ConnectorError> {
    let max_rows = parse_env("ROCKETMQ_SRE_SOURCE_MAX_ROWS", DEFAULT_SOURCE_MAX_ROWS)?;
    let max_time_range = Duration::from_secs(parse_env(
        "ROCKETMQ_SRE_SOURCE_MAX_TIME_RANGE_SECONDS",
        DEFAULT_SOURCE_MAX_TIME_RANGE_SECONDS,
    )?);
    let max_requests_per_minute = parse_env(
        "ROCKETMQ_SRE_SOURCE_MAX_REQUESTS_PER_MINUTE",
        DEFAULT_SOURCE_MAX_REQUESTS_PER_MINUTE,
    )?;
    let cache_ttl = Duration::from_secs(parse_env(
        "ROCKETMQ_SRE_SOURCE_CACHE_TTL_SECONDS",
        DEFAULT_SOURCE_CACHE_TTL_SECONDS,
    )?);
    if !(1..=10_000).contains(&max_rows)
        || !(1..=10_000).contains(&max_requests_per_minute)
        || !(Duration::from_secs(1)..=Duration::from_secs(30 * 24 * 60 * 60)).contains(&max_time_range)
        || !(Duration::from_secs(1)..=Duration::from_secs(300)).contains(&cache_ttl)
    {
        return Err(ConnectorError::configuration(
            "source row, rate, time range, or cache limits are outside supported bounds",
        ));
    }
    let labels = env::var("ROCKETMQ_SRE_SOURCE_LABEL_ALLOWLIST").unwrap_or_else(|_| {
        [
            "cluster",
            "component",
            "namespace",
            "node_id",
            "consumer_group",
            "dimension",
            "sli",
            "window_pair",
            "window_role",
            "service.name",
            "service_name",
            "rocketmq_cluster",
            "app.kubernetes.io/name",
            "app.kubernetes.io/instance",
            "app.kubernetes.io/component",
            "rocketmq.apache.org/cluster",
            "rocketmq.apache.org/service",
            "rocketmq.apache.org/broker-name",
        ]
        .join(",")
    });
    let label_allowlist = labels
        .split(',')
        .map(str::trim)
        .filter(|label| !label.is_empty())
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    if label_allowlist.is_empty()
        || label_allowlist.iter().any(|label| {
            label.len() > 255
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'/' | b'-'))
        })
    {
        return Err(ConnectorError::configuration(
            "source label allowlist is empty or contains an invalid label",
        ));
    }
    Ok(SourceLimits {
        max_rows,
        max_bytes: max_response_bytes,
        max_time_range,
        max_deadline: request_timeout,
        max_concurrency,
        max_requests_per_minute,
        cache_ttl,
        label_allowlist,
    })
}

fn load_admin_source(
    request_timeout: Duration,
    shutdown_timeout: Duration,
) -> Result<Option<AdminSourceConfig>, ConnectorError> {
    let Some(namesrv_addr) = optional_non_empty("ROCKETMQ_SRE_ADMIN_NAMESRV_ADDR") else {
        return Ok(None);
    };
    if namesrv_addr.len() > 2048 || namesrv_addr.chars().any(char::is_control) {
        return Err(ConnectorError::configuration(
            "read-only Admin NameServer address is invalid",
        ));
    }
    let access_key_env = optional_non_empty("ROCKETMQ_SRE_ADMIN_ACCESS_KEY_ENV");
    let secret_key_env = optional_non_empty("ROCKETMQ_SRE_ADMIN_SECRET_KEY_ENV");
    let credentials = match (access_key_env, secret_key_env) {
        (None, None) => None,
        (Some(access_key_env), Some(secret_key_env)) => {
            let security_token = optional_non_empty("ROCKETMQ_SRE_ADMIN_SECURITY_TOKEN_ENV")
                .map(|reference| SecretValue::from_env(&reference))
                .transpose()?;
            Some(AdminCredentialsConfig {
                access_key: SecretValue::from_env(&access_key_env)?,
                secret_key: SecretValue::from_env(&secret_key_env)?,
                security_token,
            })
        }
        _ => {
            return Err(ConnectorError::configuration(
                "read-only Admin access and secret key references must be configured together",
            ));
        }
    };
    Ok(Some(AdminSourceConfig {
        namesrv_addr,
        use_tls: parse_env("ROCKETMQ_SRE_ADMIN_USE_TLS", false)?,
        request_timeout,
        shutdown_timeout,
        credentials,
    }))
}

fn load_kubernetes_source(
    request_timeout: Duration,
    label_allowlist: &BTreeSet<String>,
) -> Result<Option<KubernetesSourceConfig>, ConnectorError> {
    let Some(api_url) = optional_http_url("ROCKETMQ_SRE_KUBERNETES_API_URL")? else {
        return Ok(None);
    };
    if api_url.scheme() != "https" && !is_loopback_url(&api_url) {
        return Err(ConnectorError::configuration(
            "non-loopback Kubernetes endpoints must use HTTPS",
        ));
    }
    let namespace = env::var("ROCKETMQ_SRE_KUBERNETES_NAMESPACE").unwrap_or_else(|_| "rocketmq".to_owned());
    if namespace.is_empty()
        || namespace.len() > 63
        || !namespace
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(ConnectorError::configuration(
            "Kubernetes namespace must be a valid DNS label",
        ));
    }
    let token_env = optional_non_empty("ROCKETMQ_SRE_KUBERNETES_TOKEN_ENV");
    let token_path = env::var_os("ROCKETMQ_SRE_KUBERNETES_TOKEN_PATH")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);
    let bearer_token = match (token_env, token_path) {
        (Some(reference), None)
            if parse_env("ROCKETMQ_SRE_KUBERNETES_ALLOW_ENV_TOKEN", false)? && is_loopback_url(&api_url) =>
        {
            KubernetesBearerToken::DevelopmentEnvironment(SecretValue::from_env(&reference)?)
        }
        (Some(_), None) => {
            return Err(ConnectorError::configuration(
                "Kubernetes environment tokens require an explicitly enabled loopback development endpoint",
            ));
        }
        (None, Some(path)) => KubernetesBearerToken::ProjectedFile(ProjectedTokenFile::try_new(path)?),
        (None, None) => {
            return Err(ConnectorError::configuration(
                "Kubernetes source requires exactly one token environment reference or projected token path",
            ));
        }
        (Some(_), Some(_)) => {
            return Err(ConnectorError::configuration(
                "Kubernetes token environment reference and projected token path are mutually exclusive",
            ));
        }
    };
    let ca_pem = read_optional_pem("ROCKETMQ_SRE_KUBERNETES_CA_PATH")?;
    if api_url.scheme() == "https" && ca_pem.is_empty() {
        return Err(ConnectorError::configuration(
            "HTTPS Kubernetes endpoints require an explicit CA bundle",
        ));
    }
    Ok(Some(KubernetesSourceConfig {
        api_url,
        namespace,
        bearer_token,
        ca_pem,
        request_timeout,
        label_allowlist: label_allowlist.clone(),
    }))
}

fn read_optional_pem(name: &str) -> Result<Vec<u8>, ConnectorError> {
    let Some(path) = env::var_os(name).map(PathBuf::from) else {
        return Ok(Vec::new());
    };
    std::fs::read(&path)
        .map_err(|_| ConnectorError::configuration(format!("PEM path configured by `{name}` cannot be read")))
}

fn load_cluster_ids(cluster_allowlist: &BTreeSet<String>) -> Result<BTreeMap<String, ClusterId>, ConnectorError> {
    if let Some(mapping) = optional_non_empty("ROCKETMQ_SRE_CLUSTER_ID_MAP") {
        let wire = serde_json::from_str::<BTreeMap<String, String>>(&mapping).map_err(|_| {
            ConnectorError::configuration("ROCKETMQ_SRE_CLUSTER_ID_MAP must be a JSON object of cluster names to UUIDs")
        })?;
        if wire.keys().collect::<BTreeSet<_>>() != cluster_allowlist.iter().collect::<BTreeSet<_>>() {
            return Err(ConnectorError::configuration(
                "ROCKETMQ_SRE_CLUSTER_ID_MAP keys must exactly match the cluster allowlist",
            ));
        }
        return wire
            .into_iter()
            .map(|(cluster, value)| {
                value
                    .parse()
                    .map(|id| (cluster, id))
                    .map_err(|_| ConnectorError::configuration("ROCKETMQ_SRE_CLUSTER_ID_MAP values must be UUIDs"))
            })
            .collect();
    }
    if cluster_allowlist.len() != 1 {
        return Err(ConnectorError::configuration(
            "multiple allowed clusters require ROCKETMQ_SRE_CLUSTER_ID_MAP",
        ));
    }
    let cluster = cluster_allowlist
        .first()
        .cloned()
        .ok_or_else(|| ConnectorError::configuration("cluster allowlist is empty"))?;
    let cluster_id = required("ROCKETMQ_SRE_CLUSTER_ID")?
        .parse()
        .map_err(|_| ConnectorError::configuration("ROCKETMQ_SRE_CLUSTER_ID must be a UUID"))?;
    Ok(BTreeMap::from([(cluster, cluster_id)]))
}

fn required(name: &str) -> Result<String, ConnectorError> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ConnectorError::configuration(format!("required environment variable `{name}` is missing")))
}

fn required_url(name: &str) -> Result<Url, ConnectorError> {
    let value = required(name)?;
    Url::parse(&value)
        .map_err(|_| ConnectorError::configuration(format!("environment variable `{name}` must be an absolute URL")))
}

fn optional_non_empty(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn optional_http_url(name: &str) -> Result<Option<Url>, ConnectorError> {
    let Some(value) = optional_non_empty(name) else {
        return Ok(None);
    };
    let url = Url::parse(&value)
        .map_err(|_| ConnectorError::configuration(format!("environment variable `{name}` must be an absolute URL")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(ConnectorError::configuration(format!(
            "environment variable `{name}` must use HTTP or HTTPS"
        )));
    }
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err(ConnectorError::configuration(format!(
            "environment variable `{name}` must not contain credentials or a fragment"
        )));
    }
    Ok(Some(url))
}

fn parse_env<T>(name: &str, default: T) -> Result<T, ConnectorError>
where
    T: std::str::FromStr,
{
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| ConnectorError::configuration(format!("environment variable `{name}` has an invalid value"))),
        Err(_) => Ok(default),
    }
}

fn parse_non_empty_set(value: &str) -> Result<BTreeSet<String>, ConnectorError> {
    let clusters = value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    if clusters.is_empty() {
        return Err(ConnectorError::configuration(
            "cluster allowlist must contain at least one cluster",
        ));
    }
    if clusters.iter().any(|cluster| {
        cluster.len() > 255
            || !cluster
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    }) {
        return Err(ConnectorError::configuration(
            "cluster names may contain only ASCII letters, digits, dot, dash, and underscore",
        ));
    }
    Ok(clusters)
}

fn validate_env_reference(reference: &str) -> Result<(), ConnectorError> {
    if reference.is_empty()
        || !reference
            .bytes()
            .all(|byte| byte == b'_' || byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        return Err(ConnectorError::configuration(
            "secret environment references may contain only A-Z, 0-9, and underscore",
        ));
    }
    Ok(())
}

fn validate_digest(digest: &str) -> Result<(), ConnectorError> {
    let Some(hex) = digest.strip_prefix("sha256:") else {
        return Err(ConnectorError::configuration(
            "expected tool surface digest must use sha256:<hex>",
        ));
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ConnectorError::configuration(
            "expected tool surface digest must contain 64 hexadecimal digits",
        ));
    }
    Ok(())
}

fn is_loopback_url(url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<IpAddr>().is_ok_and(|address| address.is_loopback())
}

#[cfg(test)]
pub(crate) fn test_source_limits(max_concurrency: usize, max_bytes: usize) -> SourceLimits {
    SourceLimits {
        max_rows: 100,
        max_bytes,
        max_time_range: Duration::from_secs(3600),
        max_deadline: Duration::from_secs(5),
        max_concurrency,
        max_requests_per_minute: 100,
        cache_ttl: Duration::from_secs(15),
        label_allowlist: BTreeSet::from([
            "cluster".to_owned(),
            "component".to_owned(),
            "consumer_group".to_owned(),
            "dimension".to_owned(),
            "namespace".to_owned(),
            "node_id".to_owned(),
            "sli".to_owned(),
            "service.name".to_owned(),
            "service_name".to_owned(),
            "rocketmq_cluster".to_owned(),
            "window_pair".to_owned(),
            "window_role".to_owned(),
            "app.kubernetes.io/name".to_owned(),
            "app.kubernetes.io/instance".to_owned(),
            "app.kubernetes.io/component".to_owned(),
            "rocketmq.apache.org/cluster".to_owned(),
            "rocketmq.apache.org/service".to_owned(),
            "rocketmq.apache.org/broker-name".to_owned(),
        ]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_output_redacts_all_secret_values() {
        let config = ConnectorConfig {
            bind_addr: "127.0.0.1:8091".parse().expect("socket address"),
            mcp_url: Url::parse("https://127.0.0.1:8089/mcp").expect("URL"),
            mcp_ca_path: Some(PathBuf::from("ca.pem")),
            mcp_ca_pem: b"private-ca-material".to_vec(),
            auth: ConnectorAuth::OAuth2(OAuth2Config {
                token_endpoint: Url::parse("https://127.0.0.1/token").expect("URL"),
                client_id: "connector".to_owned(),
                client_secret_env: "TEST_CLIENT_SECRET".to_owned(),
                audience: "rocketmq-mcp".to_owned(),
                scopes: vec!["rocketmq:read".to_owned()],
                client_secret: SecretValue("client-secret-value".to_owned()),
            }),
            tenant_id: TenantId::new(),
            cluster_allowlist: BTreeSet::from(["local".to_owned()]),
            cluster_ids: BTreeMap::from([("local".to_owned(), ClusterId::new())]),
            request_timeout: Duration::from_secs(10),
            handshake_interval: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(10),
            max_concurrency: 8,
            max_response_bytes: 1024,
            expected_tool_surface_digest: None,
            prometheus_url: None,
            alertmanager_url: None,
            loki_url: None,
            tempo_url: None,
            admin_source: None,
            kubernetes_source: None,
            source_limits: test_source_limits(8, 1024),
            internal_token_env: "TEST_INTERNAL_TOKEN".to_owned(),
            internal_token: SecretValue("internal-token-value".to_owned()),
            control_plane: None,
        };

        let output = format!("{config:?}");
        assert!(!output.contains("client-secret-value"));
        assert!(!output.contains("internal-token-value"));
        assert!(!output.contains("private-ca-material"));
        assert!(output.contains("[REDACTED]"));
    }

    #[test]
    fn development_token_requires_loopback_endpoint() {
        assert!(is_loopback_url(&Url::parse("http://127.0.0.1:8089/mcp").expect("URL")));
        assert!(is_loopback_url(&Url::parse("https://localhost/mcp").expect("URL")));
        assert!(!is_loopback_url(&Url::parse("https://mcp.example/mcp").expect("URL")));
    }

    #[test]
    fn environment_references_are_strict() {
        assert!(validate_env_reference("ROCKETMQ_SECRET_1").is_ok());
        assert!(validate_env_reference("rocketmq_secret").is_err());
        assert!(validate_env_reference("SECRET-NAME").is_err());
    }

    #[test]
    fn projected_secret_file_is_bounded_and_trimmed() {
        let unique_suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "rocketmq-sre-connector-token-{}-{}",
            std::process::id(),
            unique_suffix
        ));
        std::fs::write(&path, "projected-token\n").expect("write projected token");

        let projected = ProjectedTokenFile::try_new(path.clone()).expect("validate projected token");
        let secret = projected.read().expect("read projected token");
        std::fs::remove_file(&path).expect("remove projected token");

        assert_eq!(secret.expose(), "projected-token");
    }

    #[cfg(unix)]
    #[test]
    fn projected_secret_file_follows_atomic_kubernetes_symlink_rotation() {
        use std::os::unix::fs::symlink;

        let unique_suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_nanos();
        let mount = std::env::temp_dir().join(format!(
            "rocketmq-sre-projected-token-{}-{}",
            std::process::id(),
            unique_suffix
        ));
        let first_generation = mount.join("..2026_01");
        let second_generation = mount.join("..2026_02");
        std::fs::create_dir_all(&first_generation).expect("create first generation");
        std::fs::create_dir(&second_generation).expect("create second generation");
        std::fs::write(first_generation.join("token"), "first-token\n").expect("write first token");
        std::fs::write(second_generation.join("token"), "second-token\n").expect("write second token");
        symlink("..2026_01", mount.join("..data")).expect("link first data generation");
        symlink("..data/token", mount.join("token")).expect("link projected token");

        let projected = ProjectedTokenFile::try_new(mount.join("token")).expect("validate projected token");
        assert_eq!(projected.read().expect("first projected token").expose(), "first-token");

        symlink("..2026_02", mount.join("..data-next")).expect("link next data generation");
        std::fs::rename(mount.join("..data-next"), mount.join("..data")).expect("rotate projected data");
        assert_eq!(
            projected.read().expect("rotated projected token").expose(),
            "second-token"
        );

        std::fs::remove_dir_all(&mount).expect("remove projected mount");
    }

    #[test]
    fn cluster_allowlist_cannot_escape_resource_scope() {
        assert_eq!(
            parse_non_empty_set("prod-a,prod_b.example").expect("allowlist"),
            BTreeSet::from(["prod-a".to_owned(), "prod_b.example".to_owned()])
        );
        assert!(parse_non_empty_set("prod/other").is_err());
        assert!(parse_non_empty_set("prod?scope=other").is_err());
    }
}
