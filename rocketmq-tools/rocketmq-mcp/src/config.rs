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

use std::path::Path;
use std::str::FromStr;

use clap::Parser;
use rocketmq_admin_core::core::security::AdminCredentials;
use serde::de;
use serde::Deserialize;

use crate::error::McpError;

const MAX_JWKS_CA_BYTES: usize = 1024 * 1024;
const MAX_CLUSTER_CREDENTIAL_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Parser)]
pub struct Args {
    /// Configuration is explicit so the standalone binary never depends on
    /// the caller's current working directory.
    #[arg(long, env = "ROCKETMQ_MCP_CONFIG")]
    pub config: String,

    #[arg(long, default_value = "stdio", value_parser = parse_transport)]
    pub transport: TransportKind,

    #[arg(long)]
    pub bind: Option<String>,

    #[arg(long)]
    pub endpoint: Option<String>,
}

#[derive(Clone, Deserialize, PartialEq)]
pub struct McpConfig {
    pub server: ServerConfig,
    #[serde(default)]
    pub logging: rocketmq_observability::LoggingOverrideConfig,
    #[serde(default)]
    pub observability: rocketmq_observability::ObservabilityOverrides,
    pub clusters: Vec<ClusterConfig>,
    pub security: SecurityConfig,
    pub audit: AuditConfig,
    pub cache: CacheConfig,
    #[serde(default)]
    pub diagnosis: DiagnosisConfig,
}

impl std::fmt::Debug for McpConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpConfig")
            .field("server", &self.server)
            .field("logging", &self.logging)
            .field("observability", &ObservabilityDebugSummary::from(&self.observability))
            .field("clusters", &self.clusters)
            .field("security", &self.security)
            .field("audit", &self.audit)
            .field("cache", &self.cache)
            .field("diagnosis", &self.diagnosis)
            .finish()
    }
}

struct ObservabilityDebugSummary {
    configured: bool,
    resource_attributes_present: bool,
    metrics_configured: bool,
    traces_configured: bool,
    logs_configured: bool,
    otlp_configured: bool,
    prometheus_configured: bool,
}

impl std::fmt::Debug for ObservabilityDebugSummary {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObservabilityDebugSummary")
            .field("configured", &self.configured)
            .field("resource_attributes_present", &self.resource_attributes_present)
            .field("metrics_configured", &self.metrics_configured)
            .field("traces_configured", &self.traces_configured)
            .field("logs_configured", &self.logs_configured)
            .field("otlp_configured", &self.otlp_configured)
            .field("prometheus_configured", &self.prometheus_configured)
            .finish()
    }
}

impl From<&rocketmq_observability::ObservabilityOverrides> for ObservabilityDebugSummary {
    fn from(overrides: &rocketmq_observability::ObservabilityOverrides) -> Self {
        Self {
            configured: overrides != &rocketmq_observability::ObservabilityOverrides::default(),
            resource_attributes_present: overrides.resource_attributes.is_some(),
            metrics_configured: overrides.metrics != rocketmq_observability::MetricsOverrides::default(),
            traces_configured: overrides.traces != rocketmq_observability::TracesOverrides::default(),
            logs_configured: overrides.logs != rocketmq_observability::LogsOverrides::default(),
            otlp_configured: overrides.otlp != rocketmq_observability::OtlpOverrides::default(),
            prometheus_configured: overrides.prometheus != rocketmq_observability::PrometheusOverrides::default(),
        }
    }
}

impl McpConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, McpError> {
        let requested_path = path.as_ref();
        let path = requested_path.canonicalize().map_err(|error| {
            McpError::InvalidConfig(format!(
                "MCP configuration `{}` cannot be resolved: {error}",
                requested_path.display()
            ))
        })?;
        let config = config::Config::builder()
            .add_source(config::File::from(path.as_path()))
            .build()
            .map_err(|_| McpError::InvalidConfig("MCP configuration file could not be parsed".to_string()))?;
        let mut config = config
            .try_deserialize::<Self>()
            .map_err(redacted_deserialization_error)?;
        config.resolve_paths(&path)?;
        config.validate()?;
        Ok(config)
    }

    pub fn load_with_overrides(args: &Args) -> Result<Self, McpError> {
        let mut config = Self::load(&args.config)?;
        config.apply_overrides(args)?;
        config.validate()?;
        Ok(config)
    }

    pub fn apply_overrides(&mut self, args: &Args) -> Result<(), McpError> {
        self.server.transport = args.transport;

        if let Some(bind) = trimmed_override("bind", args.bind.as_deref())? {
            self.server.http.bind = bind;
        }

        if let Some(endpoint) = trimmed_override("endpoint", args.endpoint.as_deref())? {
            if !endpoint.starts_with('/') {
                return Err(McpError::InvalidConfig(
                    "server.http.endpoint must start with '/'".to_string(),
                ));
            }
            self.server.http.endpoint = endpoint;
        }

        Ok(())
    }

    pub fn validate(&self) -> Result<(), McpError> {
        validate_non_empty("server.name", &self.server.name)?;
        validate_non_empty("server.version", &self.server.version)?;
        validate_non_empty("server.http.bind", &self.server.http.bind)?;
        rocketmq_observability::LogFilterResolver::resolve(rocketmq_observability::LogFilterInputs {
            config: self.logging.filter.as_deref(),
            legacy_config: self.server.log_level.as_deref(),
            ..rocketmq_observability::LogFilterInputs::default()
        })
        .map_err(|error| McpError::InvalidConfig(error.to_string()))?;

        if !self.server.http.endpoint.starts_with('/') {
            return Err(McpError::InvalidConfig(
                "server.http.endpoint must start with '/'".to_string(),
            ));
        }

        if self.clusters.is_empty() {
            return Err(McpError::InvalidConfig(
                "at least one cluster must be configured".to_string(),
            ));
        }

        let mut default_count = 0usize;
        for cluster in &self.clusters {
            validate_non_empty("clusters.name", &cluster.name)?;
            validate_non_empty("clusters.namesrv_addr", &cluster.namesrv_addr)?;
            if let Some(rocketmq_cluster_name) = cluster.rocketmq_cluster_name.as_deref() {
                validate_non_empty("clusters.rocketmq_cluster_name", rocketmq_cluster_name)?;
            }
            if cluster.tenant.as_deref().is_some_and(|tenant| tenant.trim().is_empty()) {
                return Err(McpError::InvalidConfig(
                    "clusters.tenant must not be empty when configured".to_string(),
                ));
            }
            if cluster.default.unwrap_or(false) {
                default_count += 1;
            }
            if let Some(credentials) = &cluster.credentials {
                credentials.validate_reference(&cluster.name)?;
                credentials.resolve(&cluster.name)?;
            }
        }

        if default_count > 1 {
            return Err(McpError::InvalidConfig(
                "only one cluster can be marked as default".to_string(),
            ));
        }

        if matches!(self.server.transport, TransportKind::StreamableHttp) && !cfg!(feature = "streamable-http") {
            return Err(McpError::UnsupportedTransport(
                "streamable-http transport requires the streamable-http feature".to_string(),
            ));
        }

        validate_security_profile(&self.security.profile)?;
        if self.security.rate_limit_per_minute == 0 {
            return Err(McpError::InvalidConfig(
                "security.rate_limit_per_minute must be greater than zero".to_string(),
            ));
        }
        if self.security.max_concurrent_requests_per_cluster == 0 {
            return Err(McpError::InvalidConfig(
                "security.max_concurrent_requests_per_cluster must be greater than zero".to_string(),
            ));
        }
        if self.security.permissions_file.trim().is_empty() {
            return Err(McpError::InvalidConfig(
                "security.permissions_file must not be empty".to_string(),
            ));
        }

        validate_audit_sink(&self.audit.sink)?;
        if self.audit.enabled && self.audit.sink == "file" {
            validate_non_empty("audit.path", &self.audit.path)?;
        }
        if self.audit.queue_capacity == 0 {
            return Err(McpError::InvalidConfig(
                "audit.queue_capacity must be greater than zero".to_string(),
            ));
        }
        if self.audit.max_record_bytes == 0 {
            return Err(McpError::InvalidConfig(
                "audit.max_record_bytes must be greater than zero".to_string(),
            ));
        }
        if self.audit.queue_max_bytes < self.audit.max_record_bytes {
            return Err(McpError::InvalidConfig(
                "audit.queue_max_bytes must be at least audit.max_record_bytes".to_string(),
            ));
        }
        if self.audit.queue_max_bytes > u32::MAX as usize {
            return Err(McpError::InvalidConfig(
                "audit.queue_max_bytes must not exceed u32::MAX".to_string(),
            ));
        }

        self.server.http.auth.validate()?;
        if self.server.transport == TransportKind::StreamableHttp {
            self.server.http.validate_streamable_http()?;
        }

        if self.cache.enabled && self.cache.max_entries == 0 {
            return Err(McpError::InvalidConfig(
                "cache.max_entries must be greater than zero when cache is enabled".to_string(),
            ));
        }
        validate_non_empty(
            "diagnosis.consumer_lag_policy_profile",
            &self.diagnosis.consumer_lag_policy_profile,
        )?;
        if self.diagnosis.consumer_lag_threshold < 0 {
            return Err(McpError::InvalidConfig(
                "diagnosis.consumer_lag_threshold must not be negative".to_string(),
            ));
        }

        Ok(())
    }

    fn resolve_paths(&mut self, config_path: &Path) -> Result<(), McpError> {
        let config_dir = config_path
            .parent()
            .ok_or_else(|| McpError::InvalidConfig("MCP configuration has no parent directory".to_string()))?;
        let resolved = resolve_config_relative(config_dir, &self.security.permissions_file);
        let canonical = resolved.canonicalize().map_err(|error| {
            McpError::InvalidConfig(format!(
                "security.permissions_file `{}` cannot be resolved: {error}",
                resolved.display()
            ))
        })?;
        self.security.permissions_file = canonical.to_string_lossy().into_owned();

        self.server.http.tls.cert_path = resolve_config_relative(config_dir, &self.server.http.tls.cert_path)
            .to_string_lossy()
            .into_owned();
        self.server.http.tls.key_path = resolve_config_relative(config_dir, &self.server.http.tls.key_path)
            .to_string_lossy()
            .into_owned();
        if let Some(jwks_ca_path) = self.server.http.auth.jwks_ca_path.as_deref() {
            validate_non_empty("server.http.auth.jwks_ca_path", jwks_ca_path)?;
            let resolved = resolve_config_relative(config_dir, jwks_ca_path);
            let canonical = resolved.canonicalize().map_err(|error| {
                McpError::InvalidConfig(format!(
                    "server.http.auth.jwks_ca_path `{}` cannot be resolved: {error}",
                    resolved.display()
                ))
            })?;
            validate_jwks_ca_file(&canonical)?;
            self.server.http.auth.jwks_ca_path = Some(canonical.to_string_lossy().into_owned());
        }
        if !self.audit.path.trim().is_empty() {
            self.audit.path = resolve_config_relative(config_dir, &self.audit.path)
                .to_string_lossy()
                .into_owned();
        }
        for cluster in &mut self.clusters {
            if let Some(credentials) = &mut cluster.credentials {
                credentials.resolve_paths(config_dir, &cluster.name)?;
            }
        }
        Ok(())
    }
}

fn redacted_deserialization_error(error: config::ConfigError) -> McpError {
    fn redacted_key(key: &str) -> &str {
        for protected_path in ["observability.otlp.headers", "observability.resourceAttributes"] {
            if key == protected_path
                || key
                    .strip_prefix(protected_path)
                    .is_some_and(|suffix| suffix.starts_with('.'))
            {
                return protected_path;
            }
        }
        key
    }

    fn type_context<'a>(
        error: &'a config::ConfigError,
        inherited_key: Option<&'a str>,
    ) -> Option<(Option<&'a str>, &'static str)> {
        match error {
            config::ConfigError::Type { expected, key, .. } => Some((key.as_deref().or(inherited_key), *expected)),
            config::ConfigError::At { error, key, .. } => type_context(error, key.as_deref().or(inherited_key)),
            _ => None,
        }
    }

    match type_context(&error, None) {
        Some((Some(key), expected)) => {
            let key = redacted_key(key);
            McpError::InvalidConfig(format!("MCP configuration value for `{key}` must be {expected}"))
        }
        Some((None, expected)) => McpError::InvalidConfig(format!("MCP configuration value must be {expected}")),
        None => McpError::InvalidConfig("MCP configuration could not be deserialized".to_string()),
    }
}

fn resolve_config_relative(config_dir: &Path, value: &str) -> std::path::PathBuf {
    let path = Path::new(value);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        config_dir.join(path)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    Stdio,
    StreamableHttp,
}

impl TransportKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stdio => "stdio",
            Self::StreamableHttp => "streamable-http",
        }
    }
}

impl FromStr for TransportKind {
    type Err = McpError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "stdio" => Ok(Self::Stdio),
            "http" | "streamable-http" => Ok(Self::StreamableHttp),
            other => Err(McpError::UnsupportedTransport(other.to_string())),
        }
    }
}

impl<'de> Deserialize<'de> for TransportKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(de::Error::custom)
    }
}

fn parse_transport(value: &str) -> Result<TransportKind, String> {
    TransportKind::from_str(value).map_err(|err| err.to_string())
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ServerConfig {
    pub name: String,
    pub version: String,
    pub transport: TransportKind,
    #[serde(default)]
    pub log_level: Option<String>,
    pub stdio: StdioConfig,
    pub http: HttpConfig,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StdioConfig {
    pub log_to_stderr: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct HttpConfig {
    pub bind: String,
    pub endpoint: String,
    #[serde(default)]
    pub public_base_url: String,
    pub validate_origin: bool,
    pub allowed_origins: Vec<String>,
    #[serde(default)]
    pub tls: HttpTlsConfig,
    pub auth: HttpAuthConfig,
}

impl HttpConfig {
    fn validate_streamable_http(&self) -> Result<(), McpError> {
        let bind = self
            .bind
            .parse::<std::net::SocketAddr>()
            .map_err(|_| McpError::InvalidConfig("server.http.bind must be a socket address".to_string()))?;
        let public_base_url = url::Url::parse(&self.public_base_url).map_err(|_| {
            McpError::InvalidConfig("server.http.public_base_url must be an absolute HTTPS URL".to_string())
        })?;
        if public_base_url.scheme() != "https" || public_base_url.cannot_be_a_base() {
            return Err(McpError::InvalidConfig(
                "server.http.public_base_url must be an absolute HTTPS URL".to_string(),
            ));
        }
        if public_base_url.path() != "/"
            || public_base_url.query().is_some()
            || public_base_url.fragment().is_some()
            || !public_base_url.username().is_empty()
            || public_base_url.password().is_some()
        {
            return Err(McpError::InvalidConfig(
                "server.http.public_base_url must contain only an HTTPS origin".to_string(),
            ));
        }
        self.tls.validate()?;
        if self.auth.mode == HttpAuthMode::DevelopmentToken && !bind.ip().is_loopback() {
            return Err(McpError::InvalidConfig(
                "development-token authentication is restricted to loopback HTTP listeners".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct HttpTlsConfig {
    #[serde(default)]
    pub cert_path: String,
    #[serde(default)]
    pub key_path: String,
}

impl HttpTlsConfig {
    fn validate(&self) -> Result<(), McpError> {
        validate_non_empty("server.http.tls.cert_path", &self.cert_path)?;
        validate_non_empty("server.http.tls.key_path", &self.key_path)
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct HttpAuthConfig {
    pub mode: HttpAuthMode,
    pub development_token_env: String,
    #[serde(default)]
    pub development_tenant: Option<String>,
    pub issuer: String,
    pub audience: String,
    pub required_scopes: Vec<String>,
    pub jwt_algorithm: JwtAlgorithm,
    #[serde(default)]
    pub jwt_key_env: String,
    #[serde(default)]
    pub jwks_url: String,
    #[serde(default)]
    pub jwks_ca_path: Option<String>,
    #[serde(default = "default_jwks_refresh_seconds")]
    pub jwks_refresh_seconds: u64,
    #[serde(default = "default_jwks_max_stale_seconds")]
    pub jwks_max_stale_seconds: u64,
    pub protected_resource_metadata_path: String,
}

impl HttpAuthConfig {
    fn validate(&self) -> Result<(), McpError> {
        validate_non_empty("server.http.auth.development_token_env", &self.development_token_env)?;
        if self
            .development_tenant
            .as_deref()
            .is_some_and(|tenant| tenant.trim().is_empty())
        {
            return Err(McpError::InvalidConfig(
                "server.http.auth.development_tenant must not be empty when configured".to_string(),
            ));
        }
        validate_non_empty(
            "server.http.auth.protected_resource_metadata_path",
            &self.protected_resource_metadata_path,
        )?;
        if !self.protected_resource_metadata_path.starts_with('/') {
            return Err(McpError::InvalidConfig(
                "server.http.auth.protected_resource_metadata_path must start with '/'".to_string(),
            ));
        }
        if self.required_scopes.iter().any(|scope| scope.trim().is_empty()) {
            return Err(McpError::InvalidConfig(
                "server.http.auth.required_scopes must not contain empty values".to_string(),
            ));
        }
        if self.jwks_ca_path.is_some() && self.mode != HttpAuthMode::OAuthJwt {
            return Err(McpError::InvalidConfig(
                "server.http.auth.jwks_ca_path is only valid for OAuth JWT over HTTPS".to_string(),
            ));
        }
        if self.mode == HttpAuthMode::OAuthJwt {
            validate_non_empty("server.http.auth.issuer", &self.issuer)?;
            validate_non_empty("server.http.auth.audience", &self.audience)?;
            if self.required_scopes.is_empty() {
                return Err(McpError::InvalidConfig(
                    "server.http.auth.required_scopes must not be empty for OAuth".to_string(),
                ));
            }
            let issuer = url::Url::parse(&self.issuer).map_err(|_| {
                McpError::InvalidConfig("server.http.auth.issuer must be an absolute HTTPS URL".to_string())
            })?;
            if issuer.scheme() != "https"
                || issuer.cannot_be_a_base()
                || issuer.host_str().is_none()
                || !issuer.username().is_empty()
                || issuer.password().is_some()
                || issuer.fragment().is_some()
            {
                return Err(McpError::InvalidConfig(
                    "server.http.auth.issuer must be an absolute HTTPS URL".to_string(),
                ));
            }
            if self.jwt_algorithm != JwtAlgorithm::Rs256 {
                return Err(McpError::InvalidConfig(
                    "server.http.auth.jwt_algorithm must be rs256 for OAuth".to_string(),
                ));
            }
            let jwks_url = url::Url::parse(&self.jwks_url).map_err(|_| {
                McpError::InvalidConfig("server.http.auth.jwks_url must be an absolute HTTPS URL".to_string())
            })?;
            if jwks_url.scheme() != "https"
                || jwks_url.cannot_be_a_base()
                || jwks_url.host_str().is_none()
                || !jwks_url.username().is_empty()
                || jwks_url.password().is_some()
                || jwks_url.fragment().is_some()
            {
                return Err(McpError::InvalidConfig(
                    "server.http.auth.jwks_url must be an absolute HTTPS URL".to_string(),
                ));
            }
            if let Some(jwks_ca_path) = self.jwks_ca_path.as_deref() {
                validate_non_empty("server.http.auth.jwks_ca_path", jwks_ca_path)?;
                validate_jwks_ca_file(Path::new(jwks_ca_path))?;
            }
            if self.jwks_refresh_seconds == 0 {
                return Err(McpError::InvalidConfig(
                    "server.http.auth.jwks_refresh_seconds must be greater than zero".to_string(),
                ));
            }
            if self.jwks_max_stale_seconds < self.jwks_refresh_seconds {
                return Err(McpError::InvalidConfig(
                    "server.http.auth.jwks_max_stale_seconds must be at least jwks_refresh_seconds".to_string(),
                ));
            }
        }
        Ok(())
    }
}

const fn default_jwks_refresh_seconds() -> u64 {
    300
}

const fn default_jwks_max_stale_seconds() -> u64 {
    900
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum HttpAuthMode {
    DevelopmentToken,
    #[serde(rename = "oauth-jwt", alias = "o-auth-jwt")]
    OAuthJwt,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum JwtAlgorithm {
    Hs256,
    Rs256,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ClusterConfig {
    pub name: String,
    pub namesrv_addr: String,
    pub default: Option<bool>,
    /// Physical cluster name returned by NameServer. Defaults to the logical
    /// MCP cluster name when both are identical.
    #[serde(default)]
    pub rocketmq_cluster_name: Option<String>,
    /// Optional tenant binding. When set, HTTP callers must present the exact
    /// `rocketmq_tenant` JWT claim.
    #[serde(default)]
    pub tenant: Option<String>,
    /// Reference to a RocketMQ request-signing identity.
    ///
    /// Secret values are never accepted inline in the MCP configuration.
    #[serde(default)]
    pub credentials: Option<ClusterCredentialReference>,
}

impl ClusterConfig {
    pub(crate) fn physical_cluster_name(&self) -> &str {
        self.rocketmq_cluster_name.as_deref().unwrap_or(&self.name)
    }

    pub(crate) fn resolve_admin_credentials(&self) -> Result<Option<AdminCredentials>, McpError> {
        self.credentials
            .as_ref()
            .map(|reference| reference.resolve(&self.name))
            .transpose()
    }
}

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClusterCredentialReference {
    #[serde(default)]
    pub access_key_env: Option<String>,
    #[serde(default)]
    pub secret_key_env: Option<String>,
    #[serde(default)]
    pub security_token_env: Option<String>,
    /// Mounted YAML secret containing `access_key`, `secret_key`, and an
    /// optional `security_token`.
    #[serde(default)]
    pub file: Option<String>,
}

impl std::fmt::Debug for ClusterCredentialReference {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let source = if self.file.is_some() {
            "mounted_file"
        } else {
            "environment"
        };
        formatter
            .debug_struct("ClusterCredentialReference")
            .field("source", &source)
            .field("reference", &"[REDACTED]")
            .finish()
    }
}

impl ClusterCredentialReference {
    fn validate_reference(&self, cluster: &str) -> Result<(), McpError> {
        let has_file = self.file.is_some();
        let has_environment =
            self.access_key_env.is_some() || self.secret_key_env.is_some() || self.security_token_env.is_some();
        if has_file == has_environment {
            return Err(cluster_credentials_error(
                cluster,
                "configure exactly one source: file or access_key_env/secret_key_env",
            ));
        }
        if let Some(file) = &self.file {
            if file.trim().is_empty() {
                return Err(cluster_credentials_error(cluster, "file reference must not be empty"));
            }
            return Ok(());
        }

        let access_key_env = self.access_key_env.as_deref().ok_or_else(|| {
            cluster_credentials_error(cluster, "access_key_env and secret_key_env must be configured together")
        })?;
        let secret_key_env = self.secret_key_env.as_deref().ok_or_else(|| {
            cluster_credentials_error(cluster, "access_key_env and secret_key_env must be configured together")
        })?;
        validate_credential_env_name(cluster, "access_key_env", access_key_env)?;
        validate_credential_env_name(cluster, "secret_key_env", secret_key_env)?;
        if let Some(security_token_env) = self.security_token_env.as_deref() {
            validate_credential_env_name(cluster, "security_token_env", security_token_env)?;
        }
        Ok(())
    }

    fn resolve_paths(&mut self, config_dir: &Path, cluster: &str) -> Result<(), McpError> {
        let Some(file) = self.file.as_deref() else {
            return Ok(());
        };
        if file.trim().is_empty() {
            return Err(cluster_credentials_error(cluster, "file reference must not be empty"));
        }
        let resolved = resolve_config_relative(config_dir, file);
        let canonical = resolved
            .canonicalize()
            .map_err(|_| cluster_credentials_error(cluster, "credential file reference cannot be resolved"))?;
        self.file = Some(canonical.to_string_lossy().into_owned());
        Ok(())
    }

    fn resolve(&self, cluster: &str) -> Result<AdminCredentials, McpError> {
        self.validate_reference(cluster)?;
        let (access_key, secret_key, security_token) = if let Some(file) = self.file.as_deref() {
            resolve_credential_file(cluster, Path::new(file))?
        } else {
            let access_key_env = self.access_key_env.as_deref().ok_or_else(|| {
                cluster_credentials_error(cluster, "access_key_env and secret_key_env must be configured together")
            })?;
            let secret_key_env = self.secret_key_env.as_deref().ok_or_else(|| {
                cluster_credentials_error(cluster, "access_key_env and secret_key_env must be configured together")
            })?;
            let access_key = read_credential_env(cluster, access_key_env)?;
            let secret_key = read_credential_env(cluster, secret_key_env)?;
            let security_token = self
                .security_token_env
                .as_deref()
                .map(|name| read_credential_env(cluster, name))
                .transpose()?;
            (access_key, secret_key, security_token)
        };

        AdminCredentials::try_new(access_key, secret_key, security_token)
            .map_err(|_| cluster_credentials_error(cluster, "resolved credentials contain an empty required value"))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterCredentialFile {
    access_key: String,
    secret_key: String,
    #[serde(default)]
    security_token: Option<String>,
}

fn resolve_credential_file(cluster: &str, path: &Path) -> Result<(String, String, Option<String>), McpError> {
    let metadata =
        std::fs::metadata(path).map_err(|_| cluster_credentials_error(cluster, "credential file is unavailable"))?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_CLUSTER_CREDENTIAL_BYTES as u64 {
        return Err(cluster_credentials_error(
            cluster,
            "credential file must be a non-empty regular file no larger than 64 KiB",
        ));
    }
    let bytes =
        std::fs::read(path).map_err(|_| cluster_credentials_error(cluster, "credential file is unavailable"))?;
    let credentials: ClusterCredentialFile = serde_yaml::from_slice(&bytes)
        .map_err(|_| cluster_credentials_error(cluster, "credential file must be valid bounded YAML"))?;
    Ok((
        credentials.access_key,
        credentials.secret_key,
        credentials.security_token,
    ))
}

fn read_credential_env(cluster: &str, name: &str) -> Result<String, McpError> {
    std::env::var(name).map_err(|_| cluster_credentials_error(cluster, "credential environment value is unavailable"))
}

fn validate_credential_env_name(cluster: &str, field: &str, name: &str) -> Result<(), McpError> {
    if name.trim().is_empty() || name.contains('=') || name.contains('\0') {
        return Err(cluster_credentials_error(
            cluster,
            format!("{field} must contain a valid environment variable name"),
        ));
    }
    Ok(())
}

fn cluster_credentials_error(cluster: &str, reason: impl std::fmt::Display) -> McpError {
    McpError::InvalidConfig(format!("clusters `{cluster}` credentials: {reason}"))
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SecurityConfig {
    pub profile: String,
    pub allow_change_planning: bool,
    pub sanitize_output: bool,
    pub rate_limit_per_minute: u32,
    pub permissions_file: String,
    pub max_concurrent_requests_per_cluster: usize,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct AuditConfig {
    pub enabled: bool,
    pub sink: String,
    pub path: String,
    pub queue_capacity: usize,
    #[serde(default = "default_audit_max_record_bytes")]
    pub max_record_bytes: usize,
    #[serde(default = "default_audit_queue_max_bytes")]
    pub queue_max_bytes: usize,
}

const fn default_audit_max_record_bytes() -> usize {
    16 * 1024
}

const fn default_audit_queue_max_bytes() -> usize {
    1024 * 1024
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct CacheConfig {
    pub enabled: bool,
    pub max_entries: usize,
    pub cluster_overview_ttl_ms: u64,
    pub topic_list_ttl_ms: u64,
    pub broker_metrics_ttl_ms: u64,
    pub consumer_lag_ttl_ms: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct DiagnosisConfig {
    pub consumer_lag_policy_profile: String,
    pub consumer_lag_threshold: i64,
}

impl Default for DiagnosisConfig {
    fn default() -> Self {
        Self {
            consumer_lag_policy_profile: "production-default".to_string(),
            consumer_lag_threshold: 1_000,
        }
    }
}

fn validate_non_empty(field: &str, value: &str) -> Result<(), McpError> {
    if value.trim().is_empty() {
        return Err(McpError::InvalidConfig(format!("{field} must not be empty")));
    }
    Ok(())
}

fn validate_jwks_ca_file(path: &Path) -> Result<(), McpError> {
    let bytes = std::fs::read(path).map_err(|error| {
        McpError::InvalidConfig(format!(
            "server.http.auth.jwks_ca_path `{}` is not a readable file: {error}",
            path.display()
        ))
    })?;
    if bytes.is_empty() || bytes.len() > MAX_JWKS_CA_BYTES {
        return Err(McpError::InvalidConfig(
            "server.http.auth.jwks_ca_path must contain a bounded PEM CA bundle".to_string(),
        ));
    }
    let pem = std::str::from_utf8(&bytes).map_err(|_| {
        McpError::InvalidConfig("server.http.auth.jwks_ca_path must contain UTF-8 PEM certificates".to_string())
    })?;
    let certificate_count = pem.matches("-----BEGIN CERTIFICATE-----").count();
    let contains_other_pem_section = pem.lines().map(str::trim).any(|line| {
        (line.starts_with("-----BEGIN ") && line != "-----BEGIN CERTIFICATE-----")
            || (line.starts_with("-----END ") && line != "-----END CERTIFICATE-----")
    });
    if certificate_count == 0
        || certificate_count != pem.matches("-----END CERTIFICATE-----").count()
        || contains_other_pem_section
    {
        return Err(McpError::InvalidConfig(
            "server.http.auth.jwks_ca_path must contain only PEM certificates".to_string(),
        ));
    }
    Ok(())
}

fn validate_security_profile(profile: &str) -> Result<(), McpError> {
    match profile.trim().to_ascii_lowercase().as_str() {
        "read_only" | "readonly" | "read-only" | "diagnose" | "diagnostic" | "operator" => Ok(()),
        other => Err(McpError::InvalidConfig(format!(
            "unsupported security.profile `{other}`"
        ))),
    }
}

fn validate_audit_sink(sink: &str) -> Result<(), McpError> {
    match sink.trim().to_ascii_lowercase().as_str() {
        "memory" | "file" | "tracing" => Ok(()),
        other => Err(McpError::InvalidConfig(format!("unsupported audit.sink `{other}`"))),
    }
}

fn trimmed_override(field: &str, value: Option<&str>) -> Result<Option<String>, McpError> {
    match value.map(str::trim) {
        Some("") => Err(McpError::InvalidConfig(format!("{field} must not be empty"))),
        Some(value) => Ok(Some(value.to_string())),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_accepts_documented_values() {
        assert_eq!(TransportKind::from_str("stdio").unwrap(), TransportKind::Stdio);
        assert_eq!(
            TransportKind::from_str("streamable-http").unwrap(),
            TransportKind::StreamableHttp
        );
        assert_eq!(TransportKind::from_str("http").unwrap(), TransportKind::StreamableHttp);
        assert!(TransportKind::from_str("sse").is_err());
    }

    #[test]
    fn load_parses_example_config() {
        let config = McpConfig::load(example_config_path()).unwrap();

        assert_eq!(config.server.name, "rocketmq-mcp");
        assert_eq!(config.server.transport, TransportKind::Stdio);
        assert_eq!(config.clusters.len(), 1);
        assert_eq!(config.clusters[0].namesrv_addr, "127.0.0.1:9876");
        assert_eq!(config.diagnosis.consumer_lag_policy_profile, "production-default");
        assert_eq!(config.diagnosis.consumer_lag_threshold, 1_000);
        assert_eq!(config.audit.max_record_bytes, 16 * 1024);
        assert_eq!(config.audit.queue_max_bytes, 1024 * 1024);
        assert_eq!(config.logging.filter.as_deref(), Some("info"));
        assert!(config.server.log_level.is_none());
    }

    #[test]
    fn load_preserves_file_observability_overrides() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("mcp.toml");
        std::fs::copy(
            example_config_path()
                .parent()
                .expect("example config has a parent")
                .join("permissions.example.toml"),
            temp.path().join("permissions.example.toml"),
        )
        .unwrap();
        let mut contents = std::fs::read_to_string(example_config_path()).unwrap();
        contents.push_str(
            r#"

[observability.traces]
exporter = "otlp_grpc"
sampleRatio = 0.4

[observability.otlp]
endpoint = "http://file-collector:4317"
protocol = "grpc"
"#,
        );
        std::fs::write(&config_path, contents).unwrap();

        let config = McpConfig::load(&config_path).unwrap();

        assert_eq!(
            config.observability.traces.exporter,
            Some(rocketmq_observability::TraceExporter::OtlpGrpc)
        );
        assert_eq!(config.observability.traces.sample_ratio, Some(0.4));
        assert_eq!(
            config.observability.otlp.endpoint.as_deref(),
            Some("http://file-collector:4317")
        );
        assert_eq!(
            config.observability.otlp.protocol,
            Some(rocketmq_observability::OtlpProtocol::Grpc)
        );
    }

    #[test]
    fn observability_deserialization_errors_redact_configured_values() {
        const ENDPOINT_SENTINEL: &str = "secret-endpoint-sentinel";
        const HEADER_SENTINEL: &str = "secret-header-sentinel";
        const RESOURCE_SENTINEL: &str = "secret-resource-sentinel";
        const INVALID_TYPE_SENTINEL: &str = "secret-invalid-sample-ratio-sentinel";
        let invalid_overrides = [
            (
                format!(r#"resourceAttributes = {{ resource = "{RESOURCE_SENTINEL}" }}"#),
                format!(
                    r#"

[observability.otlp]
endpoint = ["{ENDPOINT_SENTINEL}"]
headers = {{ authorization = "{HEADER_SENTINEL}" }}
"#
                ),
            ),
            (
                format!(r#"resourceAttributes = {{ resource = "{RESOURCE_SENTINEL}" }}"#),
                format!(
                    r#"

[observability.otlp]
endpoint = "{ENDPOINT_SENTINEL}"
headers = "{HEADER_SENTINEL}"
"#
                ),
            ),
            (
                format!(r#"resourceAttributes = "{RESOURCE_SENTINEL}""#),
                format!(
                    r#"

[observability.otlp]
endpoint = "{ENDPOINT_SENTINEL}"
headers = {{ authorization = "{HEADER_SENTINEL}" }}
"#
                ),
            ),
            (
                format!(r#"resourceAttributes = {{ resource = "{RESOURCE_SENTINEL}" }}"#),
                format!(
                    r#"

[observability.metrics]
sampleRatio = "{INVALID_TYPE_SENTINEL}"

[observability.otlp]
endpoint = "{ENDPOINT_SENTINEL}"
headers = {{ authorization = "{HEADER_SENTINEL}" }}
"#
                ),
            ),
        ];

        let mut saw_typed_key_context = false;
        for (observability_root, invalid_override) in invalid_overrides {
            let (_temp, config_path) = write_example_config_with(&observability_root, &invalid_override);
            let error = McpConfig::load(&config_path).expect_err("invalid observability types must be rejected");
            let display = error.to_string();
            let debug = format!("{error:?}");

            saw_typed_key_context |= display.contains("observability") && display.contains("must be");
            for output in [&display, &debug] {
                assert!(!output.contains(ENDPOINT_SENTINEL));
                assert!(!output.contains(HEADER_SENTINEL));
                assert!(!output.contains(RESOURCE_SENTINEL));
                assert!(!output.contains(INVALID_TYPE_SENTINEL));
            }
        }
        assert!(
            saw_typed_key_context,
            "typed errors should retain their non-sensitive key context"
        );
    }

    #[test]
    fn nested_observability_map_type_errors_redact_custom_keys() {
        const HEADER_KEY_SENTINEL: &str = "secret-header-key-sentinel";
        const RESOURCE_KEY_SENTINEL: &str = "secret-resource-key-sentinel";
        const INVALID_VALUE_SENTINEL: &str = "secret-nested-value-sentinel";
        let cases = [
            (
                String::new(),
                format!(
                    r#"

[observability.otlp]
headers = {{ "{HEADER_KEY_SENTINEL}" = ["{INVALID_VALUE_SENTINEL}"] }}
"#
                ),
                "observability.otlp.headers",
                HEADER_KEY_SENTINEL,
            ),
            (
                format!(r#"resourceAttributes = {{ "{RESOURCE_KEY_SENTINEL}" = ["{INVALID_VALUE_SENTINEL}"] }}"#),
                String::new(),
                "observability.resourceAttributes",
                RESOURCE_KEY_SENTINEL,
            ),
        ];

        for (observability_root, nested_override, expected_path, key_sentinel) in cases {
            let (_temp, config_path) = write_example_config_with(&observability_root, &nested_override);
            let error = McpConfig::load(&config_path).expect_err("non-string observability map values must fail");
            let display = error.to_string();
            let debug = format!("{error:?}");

            assert!(display.contains(expected_path));
            assert!(display.contains("must be"));
            for output in [&display, &debug] {
                assert!(!output.contains(key_sentinel));
                assert!(!output.contains(INVALID_VALUE_SENTINEL));
            }
        }
    }

    #[test]
    fn config_debug_redacts_observability_and_cluster_references() {
        const ENDPOINT_SENTINEL: &str = "secret-endpoint-sentinel";
        const HEADER_KEY_SENTINEL: &str = "secret-header-key-sentinel";
        const HEADER_VALUE_SENTINEL: &str = "secret-header-value-sentinel";
        const RESOURCE_KEY_SENTINEL: &str = "secret-resource-key-sentinel";
        const RESOURCE_VALUE_SENTINEL: &str = "secret-resource-value-sentinel";
        const CREDENTIAL_SENTINEL: &str = "SECRET_CREDENTIAL_REFERENCE_SENTINEL";
        let mut config = McpConfig::load(example_config_path()).unwrap();
        config.observability.otlp.endpoint = Some(ENDPOINT_SENTINEL.to_string());
        config.observability.otlp.headers = Some(std::collections::HashMap::from([(
            HEADER_KEY_SENTINEL.to_string(),
            HEADER_VALUE_SENTINEL.to_string(),
        )]));
        config.observability.resource_attributes = Some(std::collections::HashMap::from([(
            RESOURCE_KEY_SENTINEL.to_string(),
            RESOURCE_VALUE_SENTINEL.to_string(),
        )]));
        config.clusters[0].credentials = Some(ClusterCredentialReference {
            access_key_env: Some(CREDENTIAL_SENTINEL.to_string()),
            secret_key_env: Some(CREDENTIAL_SENTINEL.to_string()),
            security_token_env: None,
            file: None,
        });

        let debug = format!("{config:?}");

        assert!(debug.contains("observability"));
        for sentinel in [
            ENDPOINT_SENTINEL,
            HEADER_KEY_SENTINEL,
            HEADER_VALUE_SENTINEL,
            RESOURCE_KEY_SENTINEL,
            RESOURCE_VALUE_SENTINEL,
            CREDENTIAL_SENTINEL,
        ] {
            assert!(!debug.contains(sentinel));
        }
    }

    #[test]
    fn logging_filter_accepts_legacy_alias_but_rejects_conflicts() {
        let mut config = McpConfig::load(example_config_path()).unwrap();
        config.server.log_level = Some("info".to_string());
        config
            .validate()
            .expect("equal modern and legacy values should be accepted");

        config.server.log_level = Some("debug".to_string());
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("conflicts"));

        config.logging.filter = None;
        config
            .validate()
            .expect("legacy-only configuration should remain compatible");
    }

    #[test]
    fn oauth_auth_mode_accepts_the_documented_kebab_case_value() {
        assert_eq!(
            serde_json::from_str::<HttpAuthMode>(r#""oauth-jwt""#).unwrap(),
            HttpAuthMode::OAuthJwt
        );
        assert_eq!(
            serde_json::from_str::<HttpAuthMode>(r#""o-auth-jwt""#).unwrap(),
            HttpAuthMode::OAuthJwt,
            "retain the previously derived acronym spelling as a compatibility alias"
        );
    }

    #[test]
    fn load_rejects_empty_cluster_list() {
        let mut config = McpConfig::load(example_config_path()).unwrap();
        config.clusters.clear();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("at least one cluster"));
    }

    #[test]
    fn mounted_cluster_credentials_resolve_without_exposing_secret_values() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("rocketmq-reader.yml");
        std::fs::write(
            &path,
            "access_key: reader-access-value\nsecret_key: reader-secret-value\n",
        )
        .unwrap();
        let reference = ClusterCredentialReference {
            access_key_env: None,
            secret_key_env: None,
            security_token_env: None,
            file: Some(path.to_string_lossy().into_owned()),
        };
        let reference_debug = format!("{reference:?}");
        assert!(reference_debug.contains("mounted_file"));
        assert!(reference_debug.contains("[REDACTED]"));
        assert!(!reference_debug.contains("rocketmq-reader.yml"));

        let credentials = reference.resolve("local-dev").unwrap();
        let debug = format!("{credentials:?}");

        assert!(!debug.contains("reader-access-value"));
        assert!(!debug.contains("reader-secret-value"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn cluster_credentials_reject_inline_and_ambiguous_secret_sources() {
        assert!(serde_json::from_value::<ClusterCredentialReference>(serde_json::json!({
            "access_key": "inline-access",
            "secret_key": "inline-secret"
        }))
        .is_err());

        let reference = ClusterCredentialReference {
            access_key_env: Some("PRIVATE_READER_ACCESS_REFERENCE".to_string()),
            secret_key_env: Some("PRIVATE_READER_SECRET_REFERENCE".to_string()),
            security_token_env: None,
            file: Some("private-reader-reference.yml".to_string()),
        };
        let debug = format!("{reference:?}");
        assert!(!debug.contains("PRIVATE_READER_ACCESS_REFERENCE"));
        assert!(!debug.contains("PRIVATE_READER_SECRET_REFERENCE"));
        assert!(!debug.contains("private-reader-reference.yml"));
        let error = reference.validate_reference("local-dev").unwrap_err().to_string();
        assert!(error.contains("exactly one source"));
        assert!(!error.contains("PRIVATE_READER_ACCESS_REFERENCE"));
        assert!(!error.contains("PRIVATE_READER_SECRET_REFERENCE"));
        assert!(!error.contains("private-reader-reference.yml"));

        let missing_path = std::path::Path::new("private").join("credential").join("reference.yml");
        let missing = ClusterCredentialReference {
            access_key_env: None,
            secret_key_env: None,
            security_token_env: None,
            file: Some(missing_path.to_string_lossy().into_owned()),
        };
        let error = missing.resolve("local-dev").unwrap_err().to_string();
        assert!(error.contains("credential file is unavailable"));
        assert!(!error.contains("reference.yml"));
        assert!(!error.contains("private"));
    }

    #[test]
    fn command_line_overrides_update_effective_config() {
        let args = Args::try_parse_from([
            "rocketmq-mcp",
            "--config",
            "conf/mcp.example.toml",
            "--transport",
            "stdio",
            "--bind",
            "127.0.0.1:9090",
            "--endpoint",
            "/custom-mcp",
        ])
        .unwrap();
        let mut config = McpConfig::load(example_config_path()).unwrap();

        config.apply_overrides(&args).unwrap();

        assert_eq!(config.server.transport, TransportKind::Stdio);
        assert_eq!(config.server.http.bind, "127.0.0.1:9090");
        assert_eq!(config.server.http.endpoint, "/custom-mcp");
    }

    #[test]
    fn standalone_config_is_explicit_and_relative_paths_are_config_owned() {
        assert!(Args::try_parse_from(["rocketmq-mcp"]).is_err());

        let config = McpConfig::load(example_config_path()).unwrap();
        assert!(Path::new(&config.security.permissions_file).is_absolute());
        assert!(Path::new(&config.server.http.tls.cert_path).is_absolute());
        assert!(Path::new(&config.server.http.tls.key_path).is_absolute());
        assert!(Path::new(&config.audit.path).is_absolute());
    }

    #[test]
    fn endpoint_override_must_be_absolute_path() {
        let args =
            Args::try_parse_from(["rocketmq-mcp", "--config", "conf/mcp.example.toml", "--endpoint", "mcp"]).unwrap();
        let mut config = McpConfig::load(example_config_path()).unwrap();

        let err = config.apply_overrides(&args).unwrap_err();

        assert!(err.to_string().contains("endpoint must start"));
    }

    #[test]
    fn enabled_cache_requires_positive_capacity() {
        let mut config = McpConfig::load(example_config_path()).unwrap();
        config.cache.max_entries = 0;

        let error = config.validate().unwrap_err();

        assert!(error.to_string().contains("cache.max_entries"));
    }

    #[test]
    fn audit_capacity_requires_one_bounded_record_and_u32_byte_accounting() {
        let mut config = McpConfig::load(example_config_path()).unwrap();
        config.audit.max_record_bytes = 0;
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("audit.max_record_bytes"));

        let mut config = McpConfig::load(example_config_path()).unwrap();
        config.audit.queue_max_bytes = config.audit.max_record_bytes - 1;
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("at least audit.max_record_bytes"));

        let mut config = McpConfig::load(example_config_path()).unwrap();
        config.audit.queue_max_bytes = u32::MAX as usize + 1;
        assert!(config.validate().unwrap_err().to_string().contains("u32::MAX"));
    }

    #[cfg(feature = "streamable-http")]
    #[test]
    fn streamable_http_requires_https_origin_tls_material_and_loopback_development_auth() {
        let mut config = McpConfig::load(example_config_path()).unwrap();
        config.server.transport = TransportKind::StreamableHttp;
        config.server.http.public_base_url = "http://mcp.example.test".to_string();
        assert!(config.validate().unwrap_err().to_string().contains("absolute HTTPS"));

        config.server.http.public_base_url = "https://mcp.example.test".to_string();
        config.server.http.tls.cert_path.clear();
        assert!(config.validate().unwrap_err().to_string().contains("tls.cert_path"));

        config.server.http.tls.cert_path = "server-cert.pem".to_string();
        config.server.http.bind = "0.0.0.0:8089".to_string();
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("restricted to loopback"));
    }

    #[test]
    fn oauth_requires_rs256_and_https_jwks_without_static_key_fallback() {
        let mut config = McpConfig::load(example_config_path()).unwrap();
        config.server.http.auth.mode = HttpAuthMode::OAuthJwt;
        config.server.http.auth.issuer = "https://issuer.example.test".to_string();
        config.server.http.auth.audience = "rocketmq-mcp".to_string();
        config.server.http.auth.jwt_algorithm = JwtAlgorithm::Hs256;
        config.server.http.auth.jwt_key_env = "LEGACY_STATIC_KEY".to_string();
        assert!(config.validate().unwrap_err().to_string().contains("must be rs256"));

        config.server.http.auth.jwt_algorithm = JwtAlgorithm::Rs256;
        config.server.http.auth.jwks_url = "http://issuer.example.test/jwks".to_string();
        assert!(config.validate().unwrap_err().to_string().contains("absolute HTTPS"));
    }

    #[test]
    fn custom_jwks_ca_requires_oauth_and_a_readable_pem_certificate() {
        let temp_dir = tempfile::tempdir().unwrap();
        let ca_path = temp_dir.path().join("issuer-ca.pem");
        let mut config = McpConfig::load(example_config_path()).unwrap();
        config.server.http.auth.jwks_ca_path = Some(ca_path.to_string_lossy().into_owned());
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("only valid for OAuth JWT"));

        config.server.http.auth.mode = HttpAuthMode::OAuthJwt;
        config.server.http.auth.issuer = "https://issuer.example.test".to_string();
        config.server.http.auth.audience = "rocketmq-mcp".to_string();
        config.server.http.auth.jwt_algorithm = JwtAlgorithm::Rs256;
        config.server.http.auth.jwks_url = "https://issuer.example.test/jwks".to_string();
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("not a readable file"));

        std::fs::write(&ca_path, b"not a certificate").unwrap();
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("only PEM certificates"));

        let rcgen::CertifiedKey { cert, .. } =
            rcgen::generate_simple_self_signed(vec!["issuer.example.test".to_string()]).unwrap();
        std::fs::write(&ca_path, cert.pem()).unwrap();
        config.validate().unwrap();
    }

    fn example_config_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("mcp.example.toml")
    }

    fn write_example_config_with(observability_root: &str, extra: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        std::fs::copy(
            example_config_path()
                .parent()
                .expect("example config has a parent")
                .join("permissions.example.toml"),
            temp.path().join("permissions.example.toml"),
        )
        .unwrap();
        let config_path = temp.path().join("mcp.toml");
        let mut contents = std::fs::read_to_string(example_config_path()).unwrap().replacen(
            "[observability]",
            &format!("[observability]\n{observability_root}"),
            1,
        );
        contents.push_str(extra);
        std::fs::write(&config_path, contents).unwrap();
        (temp, config_path)
    }
}
