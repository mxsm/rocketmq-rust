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
use std::str::FromStr;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TlsMode {
    Disabled,
    #[default]
    Permissive,
    Enforcing,
}

impl TlsMode {
    pub fn as_str(self) -> &'static str {
        match self {
            TlsMode::Disabled => "disabled",
            TlsMode::Permissive => "permissive",
            TlsMode::Enforcing => "enforcing",
        }
    }
}

impl FromStr for TlsMode {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value.trim().to_ascii_lowercase().as_str() {
            "disabled" => TlsMode::Disabled,
            "enforcing" => TlsMode::Enforcing,
            "permissive" => TlsMode::Permissive,
            _ => TlsMode::Permissive,
        })
    }
}

impl fmt::Display for TlsMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for TlsMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TlsMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(TlsMode::from_str(&value).unwrap_or_default())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TlsClientAuth {
    #[default]
    None,
    Optional,
    Require,
}

impl TlsClientAuth {
    pub fn as_str(self) -> &'static str {
        match self {
            TlsClientAuth::None => "none",
            TlsClientAuth::Optional => "optional",
            TlsClientAuth::Require => "require",
        }
    }
}

impl FromStr for TlsClientAuth {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value.trim().to_ascii_lowercase().as_str() {
            "optional" => TlsClientAuth::Optional,
            "require" | "required" => TlsClientAuth::Require,
            "none" => TlsClientAuth::None,
            _ => TlsClientAuth::None,
        })
    }
}

impl fmt::Display for TlsClientAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for TlsClientAuth {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TlsClientAuth {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(TlsClientAuth::from_str(&value).unwrap_or_default())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    #[serde(default)]
    pub enable: bool,

    #[serde(default)]
    pub test_mode_enable: bool,

    #[serde(default = "defaults::tls_config_file")]
    pub config_file: String,

    #[serde(default)]
    pub server: TlsServerConfig,

    #[serde(default)]
    pub client: TlsClientConfig,

    #[serde(default)]
    pub ciphers: Option<String>,

    #[serde(default)]
    pub protocols: Option<String>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enable: false,
            test_mode_enable: false,
            config_file: defaults::tls_config_file(),
            server: TlsServerConfig::default(),
            client: TlsClientConfig::default(),
            ciphers: None,
            protocols: None,
        }
    }
}

impl TlsConfig {
    pub fn apply_java_property(&mut self, key: &str, value: &str) {
        let value = strip_matching_quotes(strip_inline_comment(value.trim()).trim());
        match key.trim() {
            "tls.enable" | "tlsEnable" => self.enable = parse_bool(value, self.enable),
            "tls.test.mode.enable" | "tlsTestModeEnable" => {
                self.test_mode_enable = parse_bool(value, self.test_mode_enable);
            }
            "tls.config.file" | "tlsConfigFile" => self.config_file = value.to_string(),
            "tls.server.mode" | "tlsServerMode" => {
                self.server.mode = TlsMode::from_str(value).unwrap_or_default();
            }
            "tls.server.need.client.auth" | "tlsServerNeedClientAuth" => {
                self.server.need_client_auth = TlsClientAuth::from_str(value).unwrap_or_default();
            }
            "tls.server.keyPath" | "tlsServerKeyPath" => self.server.key_path = non_empty(value),
            "tls.server.keyPassword" | "tlsServerKeyPassword" => self.server.key_password = non_empty(value),
            "tls.server.certPath" | "tlsServerCertPath" => self.server.cert_path = non_empty(value),
            "tls.server.authClient" | "tlsServerAuthClient" => {
                self.server.auth_client = parse_bool(value, self.server.auth_client);
            }
            "tls.server.trustCertPath" | "tlsServerTrustCertPath" => {
                self.server.trust_cert_path = non_empty(value);
            }
            "tls.client.keyPath" | "tlsClientKeyPath" => self.client.key_path = non_empty(value),
            "tls.client.keyPassword" | "tlsClientKeyPassword" => self.client.key_password = non_empty(value),
            "tls.client.certPath" | "tlsClientCertPath" => self.client.cert_path = non_empty(value),
            "tls.client.authServer" | "tlsClientAuthServer" => {
                self.client.auth_server = parse_bool(value, self.client.auth_server);
            }
            "tls.client.trustCertPath" | "tlsClientTrustCertPath" => {
                self.client.trust_cert_path = non_empty(value);
            }
            "tls.ciphers" | "tlsCiphers" => self.ciphers = non_empty(value),
            "tls.protocols" | "tlsProtocols" => self.protocols = non_empty(value),
            _ => {}
        }
    }

    pub fn apply_java_properties_str(&mut self, content: &str) {
        for raw_line in content.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
                continue;
            }

            let Some((key, value)) = split_property(line) else {
                continue;
            };
            self.apply_java_property(key, value);
        }
    }

    pub fn watched_server_paths(&self) -> Vec<String> {
        let mut paths = Vec::new();
        push_path(&mut paths, &self.config_file);
        if let Some(path) = self.server.cert_path.as_deref() {
            push_path(&mut paths, path);
        }
        if let Some(path) = self.server.key_path.as_deref() {
            push_path(&mut paths, path);
        }
        if let Some(path) = self.server.trust_cert_path.as_deref() {
            push_path(&mut paths, path);
        }
        paths
    }

    pub fn java_property_entries(&self) -> Vec<(&'static str, String)> {
        let mut entries = vec![
            ("tls.enable", self.enable.to_string()),
            ("tls.config.file", self.config_file.clone()),
            ("tls.test.mode.enable", self.test_mode_enable.to_string()),
            ("tls.server.mode", self.server.mode.to_string()),
            ("tls.server.need.client.auth", self.server.need_client_auth.to_string()),
            ("tls.server.authClient", self.server.auth_client.to_string()),
            ("tls.client.authServer", self.client.auth_server.to_string()),
        ];

        push_optional_entry(&mut entries, "tls.server.keyPath", self.server.key_path.as_deref());
        push_optional_entry(
            &mut entries,
            "tls.server.keyPassword",
            self.server.key_password.as_deref(),
        );
        push_optional_entry(&mut entries, "tls.server.certPath", self.server.cert_path.as_deref());
        push_optional_entry(
            &mut entries,
            "tls.server.trustCertPath",
            self.server.trust_cert_path.as_deref(),
        );
        push_optional_entry(&mut entries, "tls.client.keyPath", self.client.key_path.as_deref());
        push_optional_entry(
            &mut entries,
            "tls.client.keyPassword",
            self.client.key_password.as_deref(),
        );
        push_optional_entry(&mut entries, "tls.client.certPath", self.client.cert_path.as_deref());
        push_optional_entry(
            &mut entries,
            "tls.client.trustCertPath",
            self.client.trust_cert_path.as_deref(),
        );
        push_optional_entry(&mut entries, "tls.ciphers", self.ciphers.as_deref());
        push_optional_entry(&mut entries, "tls.protocols", self.protocols.as_deref());

        entries
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TlsServerConfig {
    #[serde(default)]
    pub mode: TlsMode,

    #[serde(default)]
    pub need_client_auth: TlsClientAuth,

    #[serde(default)]
    pub key_path: Option<String>,

    #[serde(default)]
    pub key_password: Option<String>,

    #[serde(default)]
    pub cert_path: Option<String>,

    #[serde(default)]
    pub auth_client: bool,

    #[serde(default)]
    pub trust_cert_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TlsClientConfig {
    #[serde(default)]
    pub key_path: Option<String>,

    #[serde(default)]
    pub key_password: Option<String>,

    #[serde(default)]
    pub cert_path: Option<String>,

    #[serde(default)]
    pub auth_server: bool,

    #[serde(default)]
    pub trust_cert_path: Option<String>,
}

impl Default for TlsClientConfig {
    fn default() -> Self {
        Self {
            key_path: None,
            key_password: None,
            cert_path: None,
            auth_server: true,
            trust_cert_path: None,
        }
    }
}

fn parse_bool(value: &str, default: bool) -> bool {
    value.parse::<bool>().unwrap_or(default)
}

fn non_empty(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn strip_matching_quotes(value: &str) -> &str {
    if value.len() >= 2 {
        let first = value.as_bytes()[0];
        let last = value.as_bytes()[value.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return &value[1..value.len() - 1];
        }
    }
    value
}

fn strip_inline_comment(value: &str) -> &str {
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    for (index, character) in value.char_indices() {
        match character {
            '\'' if !in_double_quote => in_single_quote = !in_single_quote,
            '"' if !in_single_quote => in_double_quote = !in_double_quote,
            '#' | '!'
                if !in_single_quote
                    && !in_double_quote
                    && (index == 0 || value[..index].chars().last().is_some_and(char::is_whitespace)) =>
            {
                return value[..index].trim_end();
            }
            _ => {}
        }
    }

    value
}

fn split_property(line: &str) -> Option<(&str, &str)> {
    let separator_index = line.find(['=', ':'])?;
    let key = line[..separator_index].trim();
    let value = line[separator_index + 1..].trim();
    if key.is_empty() {
        None
    } else {
        Some((key, value))
    }
}

fn push_path(paths: &mut Vec<String>, path: &str) {
    if !path.is_empty() && !paths.iter().any(|existing| existing == path) {
        paths.push(path.to_string());
    }
}

fn push_optional_entry(entries: &mut Vec<(&'static str, String)>, key: &'static str, value: Option<&str>) {
    if let Some(value) = value {
        entries.push((key, value.to_string()));
    }
}

mod defaults {
    pub fn tls_config_file() -> String {
        "/etc/rocketmq/tls.properties".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_mode_parse_defaults_to_permissive_like_java() {
        assert_eq!(TlsMode::from_str("disabled").unwrap(), TlsMode::Disabled);
        assert_eq!(TlsMode::from_str("enforcing").unwrap(), TlsMode::Enforcing);
        assert_eq!(TlsMode::from_str("unknown").unwrap(), TlsMode::Permissive);
    }

    #[test]
    fn java_properties_update_nested_tls_config() {
        let mut config = TlsConfig::default();

        config.apply_java_properties_str(
            r#"
            tls.enable=true
            tls.test.mode.enable=true
            tls.server.mode="enforcing" # inline comment from TOML-style config
            tls.server.need.client.auth=require
            tls.server.certPath=/certs/server.pem
            tls.server.keyPath=/certs/server.key
            tls.server.authClient=true
            tls.server.trustCertPath=/certs/ca.pem
            tls.client.authServer=true
            tls.client.trustCertPath=/certs/ca.pem
            tls.protocols=TLSv1.3,TLSv1.2
            "#,
        );

        assert!(config.enable);
        assert!(config.test_mode_enable);
        assert_eq!(config.server.mode, TlsMode::Enforcing);
        assert_eq!(config.server.need_client_auth, TlsClientAuth::Require);
        assert_eq!(config.server.cert_path.as_deref(), Some("/certs/server.pem"));
        assert!(config.server.auth_client);
        assert!(config.client.auth_server);
        assert_eq!(config.client.trust_cert_path.as_deref(), Some("/certs/ca.pem"));
        assert_eq!(config.protocols.as_deref(), Some("TLSv1.3,TLSv1.2"));
    }

    #[test]
    fn java_property_entries_use_rocketmq_java_tls_keys() {
        let config = TlsConfig {
            enable: true,
            server: TlsServerConfig {
                mode: TlsMode::Enforcing,
                cert_path: Some("/certs/server.pem".to_string()),
                ..TlsServerConfig::default()
            },
            client: TlsClientConfig {
                trust_cert_path: Some("/certs/ca.pem".to_string()),
                ..TlsClientConfig::default()
            },
            ..TlsConfig::default()
        };

        let entries = config.java_property_entries();

        assert!(entries.contains(&("tls.enable", "true".to_string())));
        assert!(entries.contains(&("tls.server.mode", "enforcing".to_string())));
        assert!(entries.contains(&("tls.server.certPath", "/certs/server.pem".to_string())));
        assert!(entries.contains(&("tls.client.trustCertPath", "/certs/ca.pem".to_string())));
    }
}
