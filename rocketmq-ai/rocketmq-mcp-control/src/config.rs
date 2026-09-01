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
    pub audit: AuditConfig,
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
        let text = std::fs::read_to_string(path).map_err(|_| ControlError::invalid_config())?;
        let config: Self = toml::from_str(&text).map_err(|_| ControlError::invalid_config())?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ControlError> {
        self.server.validate()?;
        self.oauth.validate()?;
        self.mutations.validate()?;
        self.audit.validate()
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
    fn validate(&self) -> Result<(), ControlError> {
        let bind = self
            .bind
            .parse::<SocketAddr>()
            .map_err(|_| ControlError::invalid_config())?;
        if bind.ip().is_unspecified()
            || !valid_endpoint(&self.endpoint)
            || self.tls.cert_path.trim().is_empty()
            || self.tls.key_path.trim().is_empty()
        {
            return Err(ControlError::invalid_config());
        }
        Ok(())
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
    fn validate(&self) -> Result<(), ControlError> {
        if !valid_https_endpoint(&self.issuer)
            || !valid_https_endpoint(&self.jwks_url)
            || self.audience.is_empty()
            || self.audience.len() > 256
            || self.audience.chars().any(char::is_control)
            || self.jwks_ca_path.as_deref().is_some_and(|path| path.trim().is_empty())
        {
            return Err(ControlError::invalid_config());
        }
        Ok(())
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
    fn validate(&self) -> Result<(), ControlError> {
        let operations = self.allowed_operations.iter().copied().collect::<BTreeSet<_>>();
        let clusters = self.allowed_clusters.iter().cloned().collect::<BTreeSet<_>>();
        if operations.len() != self.allowed_operations.len()
            || clusters.len() != self.allowed_clusters.len()
            || self.operation_timeout_seconds == 0
            || self.operation_timeout_seconds > 24
        {
            return Err(ControlError::invalid_config());
        }
        Ok(())
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
    fn validate(&self) -> Result<(), ControlError> {
        if self.path.trim().is_empty()
            || !(16..=65_536).contains(&self.capacity)
            || !(512..=16_384).contains(&self.max_record_bytes)
        {
            return Err(ControlError::invalid_config());
        }
        Ok(())
    }
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
            audit: AuditConfig {
                path: "audit.jsonl".to_string(),
                capacity: 64,
                max_record_bytes: 4096,
            },
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

    #[test]
    fn debug_output_is_redacted() {
        let rendered = format!("{:?}", valid_config());
        assert_eq!(rendered, "ControlConfig { redacted: true }");
        assert!(!rendered.contains("127.0.0.1"));
        assert!(!rendered.contains("issuer.example.test"));
    }
}
