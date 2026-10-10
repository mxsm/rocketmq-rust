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

use std::collections::BTreeSet;
use std::fmt;
use std::net::SocketAddr;
use std::path::Path;

use serde::Deserialize;

use crate::error::ControlError;
use crate::model::ClusterName;
use crate::model::ControlOperation;

pub const REQUIRED_WRITE_SCOPE: &str = "rocketmq:write";

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlConfig {
    pub server: ServerConfig,
    pub oauth: OAuthConfig,
    #[serde(default)]
    pub mutations: MutationPolicyConfig,
    #[serde(default)]
    pub(crate) clusters: Vec<MutationClusterConfig>,
    pub audit: AuditConfig,
    #[serde(default)]
    pub limits: LimitsConfig,
}

impl fmt::Debug for ControlConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ControlConfig { redacted: true }")
    }
}

impl ControlConfig {
    /// Loads and validates a control configuration.
    ///
    /// # Errors
    ///
    /// Returns a stable configuration error for unreadable, malformed, or unsafe input.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ControlError> {
        Self::load_detailed(path).map_err(ControlError::from)
    }

    /// Loads and validates a control configuration and explains a rejection.
    ///
    /// # Errors
    ///
    /// Returns the stage and position of the first problem. The error holds no configured
    /// value, so it can be logged.
    pub fn load_detailed(path: impl AsRef<Path>) -> Result<Self, ConfigLoadError> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| ConfigLoadError(ConfigRejection::Read(ConfigReadFailure::from_kind(error.kind()))))?;
        Self::from_toml(&text)
    }

    fn from_toml(text: &str) -> Result<Self, ConfigLoadError> {
        let config: Self =
            toml::from_str(text).map_err(|error| ConfigLoadError(ConfigRejection::from_toml(text, &error)))?;
        config
            .check()
            .map_err(|violation| ConfigLoadError(ConfigRejection::Validate(violation)))?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ControlError> {
        self.check().map_err(|_| ControlError::invalid_config())
    }

    fn check(&self) -> Result<(), ConfigViolation> {
        self.server.check()?;
        self.oauth.check()?;
        self.mutations.check()?;
        check_cluster_registry(&self.clusters)?;
        if self.mutations.mutations_enabled && !self.mutations.allowed_operations.is_empty() {
            let configured = self
                .clusters
                .iter()
                .map(|cluster| &cluster.name)
                .collect::<BTreeSet<_>>();
            require(
                self.mutations
                    .allowed_clusters
                    .iter()
                    .all(|cluster| configured.contains(cluster)),
                "mutations.allowed_clusters",
            )?;
        }
        self.audit.check()?;
        self.limits.check()?;
        // A new audit segment starts with a copy of every call still in flight, and each of those
        // calls still needs room for its terminal record.
        require(
            self.audit.capacity >= self.limits.max_concurrent_calls.saturating_mul(4),
            "audit.capacity",
        )
    }

    #[cfg(feature = "write-tools")]
    pub(crate) fn mutation_clusters(&self) -> &[MutationClusterConfig] {
        &self.clusters
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MutationClusterConfig {
    name: ClusterName,
    namesrv_addr: String,
    #[serde(default)]
    use_tls: bool,
    #[serde(default)]
    access_key_env: Option<String>,
    #[serde(default)]
    secret_key_env: Option<String>,
    #[serde(default)]
    security_token_env: Option<String>,
}

#[cfg(feature = "write-tools")]
impl MutationClusterConfig {
    pub(crate) fn name(&self) -> &ClusterName {
        &self.name
    }

    pub(crate) fn namesrv_addr(&self) -> &str {
        &self.namesrv_addr
    }

    pub(crate) const fn use_tls(&self) -> bool {
        self.use_tls
    }

    pub(crate) fn credential_envs(&self) -> (Option<&str>, Option<&str>, Option<&str>) {
        (
            self.access_key_env.as_deref(),
            self.secret_key_env.as_deref(),
            self.security_token_env.as_deref(),
        )
    }
}

/// Why a control configuration was rejected.
///
/// The error is safe to log: it names the stage, the position in the file, and key or field
/// names. It never retains a configured value, a path, or parser text that could quote one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigLoadError(ConfigRejection);

impl ConfigLoadError {
    /// Returns the closed name of the stage that rejected the configuration: `read`, `parse`,
    /// or `validate`.
    pub const fn stage(&self) -> &'static str {
        match self.0 {
            ConfigRejection::Read(_) => "read",
            ConfigRejection::Parse { .. } => "parse",
            ConfigRejection::Validate(_) => "validate",
        }
    }
}

impl fmt::Display for ConfigLoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            ConfigRejection::Read(failure) => formatter.write_str(failure.as_str()),
            ConfigRejection::Parse { problem, key, position } => {
                match (problem, key) {
                    (ConfigParseProblem::Syntax, _) => formatter.write_str("file is not valid TOML")?,
                    (ConfigParseProblem::UnknownKey, Some(key)) => write!(formatter, "unknown key `{key}`")?,
                    (ConfigParseProblem::UnknownKey, None) => formatter.write_str("unknown key")?,
                    (ConfigParseProblem::MissingKey, Some(key)) => {
                        write!(formatter, "required key `{key}` is missing")?
                    }
                    (ConfigParseProblem::MissingKey, None) => formatter.write_str("a required key is missing")?,
                    (ConfigParseProblem::InvalidValue, _) => {
                        formatter.write_str("a value has the wrong type or is not allowed")?;
                    }
                }
                match position {
                    Some((line, column)) => write!(formatter, " at line {line}, column {column}"),
                    None => Ok(()),
                }
            }
            ConfigRejection::Validate(violation) => {
                write!(formatter, "`{}` has a value that is not allowed", violation.field)?;
                match violation.entry {
                    Some(entry) => write!(formatter, " in `[[clusters]]` entry {entry}"),
                    None => Ok(()),
                }
            }
        }
    }
}

impl std::error::Error for ConfigLoadError {}

impl From<ConfigLoadError> for ControlError {
    fn from(_: ConfigLoadError) -> Self {
        Self::invalid_config()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ConfigRejection {
    Read(ConfigReadFailure),
    Parse {
        problem: ConfigParseProblem,
        key: Option<String>,
        /// 1-based line and column of the problem.
        position: Option<(usize, usize)>,
    },
    Validate(ConfigViolation),
}

impl ConfigRejection {
    /// Classifies a deserialization failure without copying parser text.
    ///
    /// The parser message and its rendered form quote the file, which can hold addresses. Only
    /// the position and a plain key name are kept.
    fn from_toml(text: &str, error: &toml::de::Error) -> Self {
        let message = error.message();
        let (problem, key) = if toml::from_str::<toml::Table>(text).is_err() {
            (ConfigParseProblem::Syntax, None)
        } else if let Some(quoted) = message.strip_prefix("unknown field `") {
            (ConfigParseProblem::UnknownKey, plain_key(quoted))
        } else if let Some(quoted) = message.strip_prefix("missing field `") {
            (ConfigParseProblem::MissingKey, plain_key(quoted))
        } else {
            (ConfigParseProblem::InvalidValue, None)
        };
        Self::Parse {
            problem,
            key,
            position: error.span().map(|span| line_and_column(text, span.start)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigReadFailure {
    NotFound,
    PermissionDenied,
    InvalidEncoding,
    Unreadable,
}

impl ConfigReadFailure {
    fn from_kind(kind: std::io::ErrorKind) -> Self {
        match kind {
            std::io::ErrorKind::NotFound => Self::NotFound,
            std::io::ErrorKind::PermissionDenied => Self::PermissionDenied,
            std::io::ErrorKind::InvalidData => Self::InvalidEncoding,
            _ => Self::Unreadable,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::NotFound => "file was not found",
            Self::PermissionDenied => "file is not readable by this process",
            Self::InvalidEncoding => "file is not valid UTF-8",
            Self::Unreadable => "file could not be read",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigParseProblem {
    Syntax,
    UnknownKey,
    MissingKey,
    InvalidValue,
}

/// A constraint that a loaded value violates, identified by its static field path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConfigViolation {
    field: &'static str,
    /// 1-based position of the `[[clusters]]` table that holds the field.
    entry: Option<usize>,
}

fn require(valid: bool, field: &'static str) -> Result<(), ConfigViolation> {
    if valid {
        Ok(())
    } else {
        Err(ConfigViolation { field, entry: None })
    }
}

/// Returns the key that serde quoted at the start of `quoted` when it is a plain key name.
///
/// TOML accepts arbitrary text in a quoted key, so any other key is withheld.
fn plain_key(quoted: &str) -> Option<String> {
    let key = quoted.split('`').next()?;
    let plain = (1..=64).contains(&key.len())
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
    plain.then(|| key.to_owned())
}

fn line_and_column(text: &str, offset: usize) -> (usize, usize) {
    let offset = offset.min(text.len());
    let before = &text.as_bytes()[..offset];
    let line_start = before
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |newline| newline + 1);
    let line = before.iter().filter(|byte| **byte == b'\n').count() + 1;
    let column = text
        .get(line_start..offset)
        .map_or(offset - line_start, |prefix| prefix.chars().count())
        + 1;
    (line, column)
}

fn check_cluster_registry(clusters: &[MutationClusterConfig]) -> Result<(), ConfigViolation> {
    let mut names = BTreeSet::new();
    for (index, cluster) in clusters.iter().enumerate() {
        let field = if !names.insert(cluster.name.clone()) {
            "clusters.name"
        } else if !valid_namesrv_addr(&cluster.namesrv_addr) {
            "clusters.namesrv_addr"
        } else if let Some(field) = credential_env_violation(cluster) {
            field
        } else {
            continue;
        };
        return Err(ConfigViolation {
            field,
            entry: Some(index + 1),
        });
    }
    Ok(())
}

fn valid_namesrv_addr(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 1024
        && value.split(';').all(|endpoint| {
            let Some((host, port)) = endpoint.rsplit_once(':') else {
                return false;
            };
            !host.is_empty()
                && host.len() <= 253
                && host
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'[' | b']' | b':'))
                && port.parse::<u16>().is_ok_and(|port| port != 0)
        })
}

/// Returns the credential variable field that breaks the both-or-neither rule or the name grammar.
fn credential_env_violation(cluster: &MutationClusterConfig) -> Option<&'static str> {
    let valid_env = |value: &str| {
        (1..=128).contains(&value.len())
            && value
                .bytes()
                .enumerate()
                .all(|(index, byte)| byte == b'_' || byte.is_ascii_uppercase() || (index > 0 && byte.is_ascii_digit()))
    };
    match (&cluster.access_key_env, &cluster.secret_key_env) {
        (None, None) => cluster
            .security_token_env
            .is_some()
            .then_some("clusters.security_token_env"),
        (Some(access), Some(secret)) => {
            if !valid_env(access) {
                Some("clusters.access_key_env")
            } else if !valid_env(secret) {
                Some("clusters.secret_key_env")
            } else if cluster
                .security_token_env
                .as_deref()
                .is_some_and(|token| !valid_env(token))
            {
                Some("clusters.security_token_env")
            } else {
                None
            }
        }
        (Some(_), None) => Some("clusters.secret_key_env"),
        (None, Some(_)) => Some("clusters.access_key_env"),
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub bind: String,
    pub endpoint: String,
    pub public_base_url: HttpsOrigin,
    pub tls: TlsConfig,
}

impl ServerConfig {
    fn check(&self) -> Result<(), ConfigViolation> {
        require(
            self.bind
                .parse::<SocketAddr>()
                .is_ok_and(|bind| !bind.ip().is_unspecified()),
            "server.bind",
        )?;
        require(valid_endpoint(&self.endpoint), "server.endpoint")?;
        require(!self.tls.cert_path.trim().is_empty(), "server.tls.cert_path")?;
        require(!self.tls.key_path.trim().is_empty(), "server.tls.key_path")
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct HttpsOrigin(String);

impl HttpsOrigin {
    pub fn try_new(value: impl Into<String>) -> Result<Self, ControlError> {
        let value = value.into();
        let url = url::Url::parse(&value).map_err(|_| ControlError::invalid_config())?;
        let host = match url.host() {
            Some(url::Host::Domain(host)) if valid_public_hostname(host) => host,
            _ => return Err(ControlError::invalid_config()),
        };
        let canonical = format!("https://{host}");
        if url.scheme() != "https"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
            || value != canonical
        {
            return Err(ControlError::invalid_config());
        }
        Ok(Self(canonical))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn host(&self) -> &str {
        self.0.strip_prefix("https://").unwrap_or("")
    }
}

impl<'de> Deserialize<'de> for HttpsOrigin {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OAuthConfig {
    pub issuer: String,
    pub audience: String,
    pub jwks_url: String,
    #[serde(default)]
    pub jwks_ca_path: Option<String>,
}

impl OAuthConfig {
    fn check(&self) -> Result<(), ConfigViolation> {
        require(valid_https_endpoint(&self.issuer), "oauth.issuer")?;
        require(valid_https_endpoint(&self.jwks_url), "oauth.jwks_url")?;
        require(
            !self.audience.is_empty() && self.audience.len() <= 256 && !self.audience.chars().any(char::is_control),
            "oauth.audience",
        )?;
        require(
            !self.jwks_ca_path.as_deref().is_some_and(|path| path.trim().is_empty()),
            "oauth.jwks_ca_path",
        )
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationPolicyConfig {
    #[serde(default)]
    pub mutations_enabled: bool,
    #[serde(default = "default_dry_run")]
    pub dry_run: bool,
    #[serde(default)]
    pub allowed_operations: Vec<ControlOperation>,
    #[serde(default)]
    pub allowed_clusters: Vec<ClusterName>,
    #[serde(default = "default_operation_timeout_seconds")]
    pub operation_timeout_seconds: u64,
}

impl Default for MutationPolicyConfig {
    fn default() -> Self {
        Self {
            mutations_enabled: false,
            dry_run: true,
            allowed_operations: Vec::new(),
            allowed_clusters: Vec::new(),
            operation_timeout_seconds: default_operation_timeout_seconds(),
        }
    }
}

impl MutationPolicyConfig {
    fn check(&self) -> Result<(), ConfigViolation> {
        let operations = self.allowed_operations.iter().copied().collect::<BTreeSet<_>>();
        let clusters = self.allowed_clusters.iter().cloned().collect::<BTreeSet<_>>();
        require(
            operations.len() == self.allowed_operations.len(),
            "mutations.allowed_operations",
        )?;
        require(
            clusters.len() == self.allowed_clusters.len(),
            "mutations.allowed_clusters",
        )?;
        require(
            (1..=24).contains(&self.operation_timeout_seconds),
            "mutations.operation_timeout_seconds",
        )
    }

    pub fn operation_allowlist(&self) -> BTreeSet<ControlOperation> {
        self.allowed_operations.iter().copied().collect()
    }

    pub fn cluster_allowlist(&self) -> BTreeSet<ClusterName> {
        self.allowed_clusters.iter().cloned().collect()
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditConfig {
    pub path: String,
    #[serde(default = "default_audit_capacity")]
    pub capacity: usize,
    #[serde(default = "default_audit_record_bytes")]
    pub max_record_bytes: usize,
}

impl AuditConfig {
    fn check(&self) -> Result<(), ConfigViolation> {
        require(!self.path.trim().is_empty(), "audit.path")?;
        require((16..=65_536).contains(&self.capacity), "audit.capacity")?;
        require(
            (crate::audit::MIN_AUDIT_RECORD_BYTES..=16_384).contains(&self.max_record_bytes),
            "audit.max_record_bytes",
        )
    }
}

/// How many mutation calls the server admits. A refused call gets `rate_limited` before its
/// `started` audit record, so it costs no audit capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitsConfig {
    /// Dry runs one principal may start per minute.
    #[serde(default = "default_dry_runs_per_minute")]
    pub dry_runs_per_minute: u32,
    /// Execute calls one principal may start per minute.
    #[serde(default = "default_executes_per_minute")]
    pub executes_per_minute: u32,
    /// Mutation calls of all principals that may be in flight at once.
    #[serde(default = "default_max_concurrent_calls")]
    pub max_concurrent_calls: usize,
}

impl LimitsConfig {
    fn check(&self) -> Result<(), ConfigViolation> {
        require(
            (1..=6_000).contains(&self.dry_runs_per_minute),
            "limits.dry_runs_per_minute",
        )?;
        require(
            (1..=6_000).contains(&self.executes_per_minute),
            "limits.executes_per_minute",
        )?;
        require(
            (1..=64).contains(&self.max_concurrent_calls),
            "limits.max_concurrent_calls",
        )
    }
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            dry_runs_per_minute: default_dry_runs_per_minute(),
            executes_per_minute: default_executes_per_minute(),
            max_concurrent_calls: default_max_concurrent_calls(),
        }
    }
}

const fn default_dry_runs_per_minute() -> u32 {
    60
}

const fn default_executes_per_minute() -> u32 {
    20
}

const fn default_max_concurrent_calls() -> usize {
    8
}

const fn default_dry_run() -> bool {
    true
}

const fn default_operation_timeout_seconds() -> u64 {
    24
}

const fn default_audit_capacity() -> usize {
    4096
}

const fn default_audit_record_bytes() -> usize {
    4096
}

fn valid_endpoint(value: &str) -> bool {
    value.starts_with('/')
        && value.len() > 1
        && value.len() <= 128
        && !value.contains("//")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_'))
}

fn valid_https_endpoint(value: &str) -> bool {
    url::Url::parse(value).is_ok_and(|url| {
        let Some(url::Host::Domain(host)) = url.host() else {
            return false;
        };
        let path = url.path();
        let canonical = if path == "/" {
            format!("https://{host}")
        } else {
            format!("https://{host}{path}")
        };
        url.scheme() == "https"
            && !url.cannot_be_a_base()
            && valid_public_hostname(host)
            && url.username().is_empty()
            && url.password().is_none()
            && url.port().is_none()
            && path.len() <= 512
            && !path.contains("//")
            && !path.split('/').any(|segment| matches!(segment, "." | ".."))
            && path
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.' | b'~'))
            && url.query().is_none()
            && url.fragment().is_none()
            && value == canonical
    })
}

fn valid_public_hostname(host: &str) -> bool {
    let lowercase = host.to_ascii_lowercase();
    if lowercase != host
        || !host.contains('.')
        || host.len() > 253
        || host == "localhost"
        || host.ends_with(".localhost")
        || host.ends_with(".local")
        || host.ends_with(".internal")
        || host.ends_with(".home.arpa")
    {
        return false;
    }
    host.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> ControlConfig {
        ControlConfig {
            server: ServerConfig {
                bind: "127.0.0.1:8090".to_string(),
                endpoint: "/mcp".to_string(),
                public_base_url: HttpsOrigin::try_new("https://control.example.test").unwrap(),
                tls: TlsConfig {
                    cert_path: "server.pem".to_string(),
                    key_path: "server-key.pem".to_string(),
                },
            },
            oauth: OAuthConfig {
                issuer: "https://issuer.example.test".to_string(),
                audience: "rocketmq-mcp-control".to_string(),
                jwks_url: "https://issuer.example.test/jwks".to_string(),
                jwks_ca_path: None,
            },
            mutations: MutationPolicyConfig::default(),
            clusters: Vec::new(),
            audit: AuditConfig {
                path: "audit.jsonl".to_string(),
                capacity: 64,
                max_record_bytes: 4096,
            },
            limits: crate::config::LimitsConfig::default(),
        }
    }

    #[test]
    fn defaults_keep_mutations_off_and_dry_run_on() {
        let policy: MutationPolicyConfig = toml::from_str("").unwrap();
        assert!(!policy.mutations_enabled);
        assert!(policy.dry_run);
        assert!(policy.allowed_operations.is_empty());
        assert!(policy.allowed_clusters.is_empty());
    }

    #[test]
    fn transport_and_oauth_are_https_only_and_closed() {
        let mut config = valid_config();
        assert!(config.validate().is_ok());

        config.oauth.jwks_url = "http://issuer.example.test/jwks".to_string();
        assert_eq!(
            config.validate().unwrap_err().code(),
            crate::error::ControlErrorCode::InvalidConfig
        );
        for invalid in [
            "http://control.example.test",
            "https://control.example.test/path",
            "https://control.example.test?query=1",
            "https://control.example.test#fragment",
            "https://user@control.example.test",
            "https://127.0.0.1",
            "https://[::1]",
            "https://control.example.test:8443",
            "https://LOCALHOST",
            "https://control%2eexample.test",
        ] {
            assert!(HttpsOrigin::try_new(invalid).is_err(), "accepted {invalid}");
        }
        let mut wildcard = valid_config();
        wildcard.server.bind = "0.0.0.0:8090".to_string();
        assert!(wildcard.validate().is_err());

        for invalid in [
            "https://127.0.0.1/jwks",
            "https://[::1]/jwks",
            "https://localhost/jwks",
            "https://identity.local/jwks",
            "https://identity.example.test:8443/jwks",
            "https://identity.example.test/jwks?target=%31%32%37%2e%30%2e%30%2e%31",
            "https://identity.example.test/%74oken%3Dsecret",
            "https://identity.example.test/token=secret",
            "https://identity.example.test/a/../jwks",
        ] {
            config = valid_config();
            config.oauth.jwks_url = invalid.to_string();
            assert!(config.validate().is_err(), "accepted {invalid}");
        }
    }

    #[test]
    fn unknown_configuration_fields_are_rejected() {
        let encoded = format!(
            "{}\ndevelopment_token = 'forbidden'\n",
            include_str!("../conf/mcp-control.example.toml")
        );
        assert!(toml::from_str::<ControlConfig>(&encoded).is_err());
    }

    const EXAMPLE: &str = include_str!("../conf/mcp-control.example.toml");

    fn rejection(text: &str) -> ConfigLoadError {
        ControlConfig::from_toml(text).unwrap_err()
    }

    fn line_of(text: &str, prefix: &str) -> usize {
        text.lines().position(|line| line.starts_with(prefix)).unwrap() + 1
    }

    #[test]
    fn parse_rejections_locate_the_problem_without_quoting_the_file() {
        assert!(ControlConfig::from_toml(EXAMPLE).is_ok());

        let text = format!("{EXAMPLE}\ndevelopment_token = 'forbidden-10.0.0.9'\n");
        let unknown = rejection(&text);
        assert_eq!(unknown.stage(), "parse");
        assert_eq!(
            unknown.to_string(),
            format!(
                "unknown key `development_token` at line {}, column 1",
                line_of(&text, "development_token")
            )
        );

        // TOML accepts any text as a quoted key, so only plain key names are repeated.
        let text = format!("{EXAMPLE}\n\"https://user:pass@10.0.0.9/\" = 1\n");
        let hostile_key = rejection(&text).to_string();
        assert_eq!(
            hostile_key,
            format!("unknown key at line {}, column 1", line_of(&text, "\"https://user"))
        );

        let missing = rejection(&EXAMPLE.replace("bind = \"127.0.0.1:8090\"", ""));
        assert_eq!(missing.stage(), "parse");
        assert!(missing.to_string().starts_with("required key `bind` is missing"));

        for (replaced, replacement) in [
            ("capacity = 4096", "capacity = \"many-10.0.0.9\""),
            (
                "public_base_url = \"https://control.example.test\"",
                "public_base_url = \"http://10.0.0.9\"",
            ),
            ("allowed_operations = []", "allowed_operations = [\"drop-10.0.0.9\"]"),
        ] {
            let text = EXAMPLE.replace(replaced, replacement);
            let key = replacement.split(' ').next().unwrap();
            let invalid = rejection(&text);
            assert_eq!(invalid.stage(), "parse");
            assert!(
                invalid.to_string().starts_with(&format!(
                    "a value has the wrong type or is not allowed at line {}, column ",
                    line_of(&text, key)
                )),
                "{key}: {invalid}"
            );
            assert!(!invalid.to_string().contains("10.0.0.9"));
        }

        let text = format!("{EXAMPLE}\nbroken = = 'token-10.0.0.9'\n");
        let syntax = rejection(&text).to_string();
        assert!(
            syntax.starts_with(&format!(
                "file is not valid TOML at line {}, column ",
                line_of(&text, "broken")
            )),
            "{syntax}"
        );
        assert!(!syntax.contains("10.0.0.9"));
    }

    #[test]
    fn validation_rejections_name_the_static_field_path() {
        for (replaced, replacement, expected) in [
            (
                "bind = \"127.0.0.1:8090\"",
                "bind = \"0.0.0.0:8090\"",
                "`server.bind` has a value that is not allowed",
            ),
            (
                "jwks_url = \"https://identity.example.test/.well-known/jwks.json\"",
                "jwks_url = \"https://10.0.0.9/jwks\"",
                "`oauth.jwks_url` has a value that is not allowed",
            ),
            (
                "operation_timeout_seconds = 24",
                "operation_timeout_seconds = 25",
                "`mutations.operation_timeout_seconds` has a value that is not allowed",
            ),
            (
                "max_record_bytes = 4096",
                "max_record_bytes = 16",
                "`audit.max_record_bytes` has a value that is not allowed",
            ),
            (
                "secret_key_env = \"ROCKETMQ_CONTROL_SECRET_KEY\"",
                "",
                "`clusters.secret_key_env` has a value that is not allowed in `[[clusters]]` entry 1",
            ),
        ] {
            let invalid = rejection(&EXAMPLE.replace(replaced, replacement));
            assert_eq!(invalid.stage(), "validate");
            assert_eq!(invalid.to_string(), expected);
        }

        let second_cluster = rejection(&format!(
            "{EXAMPLE}\n[[clusters]]\nname = \"production-b\"\nnamesrv_addr = \"10.0.0.9\"\n"
        ));
        assert_eq!(
            second_cluster.to_string(),
            "`clusters.namesrv_addr` has a value that is not allowed in `[[clusters]]` entry 2"
        );

        let unregistered = rejection(
            &EXAMPLE
                .replace("mutations_enabled = false", "mutations_enabled = true")
                .replace("allowed_operations = []", "allowed_operations = [\"topic_upsert\"]")
                .replace("allowed_clusters = []", "allowed_clusters = [\"staging\"]"),
        );
        assert_eq!(
            unregistered.to_string(),
            "`mutations.allowed_clusters` has a value that is not allowed"
        );
    }

    #[test]
    fn call_limits_have_defaults_bounds_and_room_in_an_audit_segment() {
        let expected = LimitsConfig {
            dry_runs_per_minute: 60,
            executes_per_minute: 20,
            max_concurrent_calls: 8,
        };
        assert_eq!(ControlConfig::from_toml(EXAMPLE).unwrap().limits, expected);
        // A configuration written before the section existed keeps working with the defaults.
        let without_section = EXAMPLE.split("[limits]").next().unwrap();
        assert_eq!(ControlConfig::from_toml(without_section).unwrap().limits, expected);

        for (replaced, replacement, expected) in [
            (
                "dry_runs_per_minute = 60",
                "dry_runs_per_minute = 0",
                "`limits.dry_runs_per_minute` has a value that is not allowed",
            ),
            (
                "executes_per_minute = 20",
                "executes_per_minute = 6001",
                "`limits.executes_per_minute` has a value that is not allowed",
            ),
            (
                "max_concurrent_calls = 8",
                "max_concurrent_calls = 65",
                "`limits.max_concurrent_calls` has a value that is not allowed",
            ),
            // The smallest record bound holds every audit record, so nothing below it is accepted.
            (
                "max_record_bytes = 4096",
                "max_record_bytes = 4095",
                "`audit.max_record_bytes` has a value that is not allowed",
            ),
            // A segment needs room for the carried start and the terminal record of every call in flight.
            (
                "capacity = 4096",
                "capacity = 31",
                "`audit.capacity` has a value that is not allowed",
            ),
        ] {
            let invalid = rejection(&EXAMPLE.replace(replaced, replacement));
            assert_eq!(invalid.stage(), "validate");
            assert_eq!(invalid.to_string(), expected);
        }
        assert!(ControlConfig::from_toml(&EXAMPLE.replace("capacity = 4096", "capacity = 32")).is_ok());
        let unknown = rejection(&format!("{EXAMPLE}burst = 5\n"));
        assert_eq!(unknown.stage(), "parse");
    }

    #[test]
    fn unreadable_file_is_a_read_stage_rejection_and_keeps_the_stable_error() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("absent.toml");
        let rejected = ControlConfig::load_detailed(&missing).unwrap_err();
        assert_eq!(rejected.stage(), "read");
        assert_eq!(rejected.to_string(), "file was not found");
        assert_eq!(
            ControlConfig::load(&missing).unwrap_err(),
            ControlError::invalid_config()
        );

        let present = directory.path().join("control.toml");
        std::fs::write(&present, EXAMPLE).unwrap();
        assert!(ControlConfig::load(&present).is_ok());
    }

    #[test]
    fn debug_output_is_redacted() {
        let rendered = format!("{:?}", valid_config());
        assert_eq!(rendered, "ControlConfig { redacted: true }");
        assert!(!rendered.contains("127.0.0.1"));
        assert!(!rendered.contains("issuer.example.test"));
    }

    #[test]
    fn mutation_cluster_registry_is_closed_and_required_for_registered_operations() {
        let cluster = MutationClusterConfig {
            name: ClusterName::try_new("cluster-a").unwrap(),
            namesrv_addr: "namesrv.example.test:9876".to_owned(),
            use_tls: true,
            access_key_env: Some("ROCKETMQ_ACCESS_KEY".to_owned()),
            secret_key_env: Some("ROCKETMQ_SECRET_KEY".to_owned()),
            security_token_env: Some("ROCKETMQ_SECURITY_TOKEN".to_owned()),
        };
        let mut config = valid_config();
        config.mutations.mutations_enabled = true;
        config.mutations.allowed_operations = vec![ControlOperation::TopicUpsert];
        config.mutations.allowed_clusters = vec![ClusterName::try_new("cluster-a").unwrap()];
        assert!(config.validate().is_err());
        config.clusters.push(cluster.clone());
        assert!(config.validate().is_ok());
        config.clusters.push(cluster);
        assert!(config.validate().is_err());

        let inline_secret = format!(
            "{}\n[[clusters]]\nname='cluster-a'\nnamesrv_addr='namesrv.example.test:9876'\naccess_key='secret'\n",
            include_str!("../conf/mcp-control.example.toml")
        );
        assert!(toml::from_str::<ControlConfig>(&inline_secret).is_err());
    }
}
