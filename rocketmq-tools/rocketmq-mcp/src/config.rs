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
use serde::de;
use serde::Deserialize;

use crate::error::McpError;

#[derive(Debug, Clone, Parser)]
pub struct Args {
    #[arg(long, default_value = "rocketmq-tools/rocketmq-mcp/conf/mcp.example.toml")]
    pub config: String,

    #[arg(long, default_value = "stdio", value_parser = parse_transport)]
    pub transport: TransportKind,

    #[arg(long)]
    pub bind: Option<String>,

    #[arg(long)]
    pub endpoint: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct McpConfig {
    pub server: ServerConfig,
    pub clusters: Vec<ClusterConfig>,
    pub security: SecurityConfig,
    pub audit: AuditConfig,
    pub cache: CacheConfig,
    #[serde(default)]
    pub diagnosis: DiagnosisConfig,
}

impl McpConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, McpError> {
        let path = path.as_ref();
        let config = config::Config::builder().add_source(config::File::from(path)).build()?;
        let mut config = config.try_deserialize::<Self>()?;
        config.resolve_permissions_path(path)?;
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
        validate_non_empty("server.log_level", &self.server.log_level)?;
        validate_non_empty("server.http.bind", &self.server.http.bind)?;

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
            if cluster.default.unwrap_or(false) {
                default_count += 1;
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

    fn resolve_permissions_path(&mut self, config_path: &Path) -> Result<(), McpError> {
        let permissions_path = Path::new(&self.security.permissions_file);
        let resolved = if permissions_path.is_absolute() {
            permissions_path.to_path_buf()
        } else {
            config_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(permissions_path)
        };
        let canonical = resolved.canonicalize().map_err(|error| {
            McpError::InvalidConfig(format!(
                "security.permissions_file `{}` cannot be resolved: {error}",
                resolved.display()
            ))
        })?;
        self.security.permissions_file = canonical.to_string_lossy().into_owned();
        Ok(())
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
    pub log_level: String,
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
    pub issuer: String,
    pub audience: String,
    pub required_scopes: Vec<String>,
    pub jwt_algorithm: JwtAlgorithm,
    #[serde(default)]
    pub jwt_key_env: String,
    #[serde(default)]
    pub jwks_url: String,
    #[serde(default = "default_jwks_refresh_seconds")]
    pub jwks_refresh_seconds: u64,
    #[serde(default = "default_jwks_max_stale_seconds")]
    pub jwks_max_stale_seconds: u64,
    pub protected_resource_metadata_path: String,
}

impl HttpAuthConfig {
    fn validate(&self) -> Result<(), McpError> {
        validate_non_empty("server.http.auth.development_token_env", &self.development_token_env)?;
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
    }

    #[test]
    fn load_rejects_empty_cluster_list() {
        let mut config = McpConfig::load(example_config_path()).unwrap();
        config.clusters.clear();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("at least one cluster"));
    }

    #[test]
    fn command_line_overrides_update_effective_config() {
        let args = Args::try_parse_from([
            "rocketmq-mcp",
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
    fn endpoint_override_must_be_absolute_path() {
        let args = Args::try_parse_from(["rocketmq-mcp", "--endpoint", "mcp"]).unwrap();
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

    fn example_config_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("mcp.example.toml")
    }
}
