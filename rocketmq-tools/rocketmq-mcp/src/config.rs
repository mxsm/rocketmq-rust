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
}

impl McpConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, McpError> {
        let config = config::Config::builder()
            .add_source(config::File::from(path.as_ref()))
            .build()?;
        let config = config.try_deserialize::<Self>()?;
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

        validate_audit_sink(&self.audit.sink)?;
        if self.audit.enabled && self.audit.sink == "file" {
            validate_non_empty("audit.path", &self.audit.path)?;
        }

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
    pub validate_origin: bool,
    pub allowed_origins: Vec<String>,
    pub require_auth: bool,
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
    pub allow_dangerous_tools: bool,
    pub require_confirmation: bool,
    pub sanitize_output: bool,
    pub rate_limit_per_minute: u32,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct AuditConfig {
    pub enabled: bool,
    pub sink: String,
    pub path: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct CacheConfig {
    pub cluster_overview_ttl_ms: u64,
    pub topic_list_ttl_ms: u64,
    pub broker_metrics_ttl_ms: u64,
    pub consumer_lag_ttl_ms: u64,
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
    use std::fs;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

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
    }

    #[test]
    fn load_rejects_empty_cluster_list() {
        let path = write_temp_config(
            r#"
clusters = []

[server]
name = "rocketmq-mcp"
version = "1.0.0"
transport = "stdio"
log_level = "info"

[server.stdio]
log_to_stderr = true

[server.http]
bind = "127.0.0.1:8089"
endpoint = "/mcp"
validate_origin = true
allowed_origins = ["http://localhost"]
require_auth = true

[security]
profile = "read_only"
allow_dangerous_tools = false
require_confirmation = true
sanitize_output = true
rate_limit_per_minute = 60

[audit]
enabled = true
sink = "file"
path = "./logs/rocketmq-mcp-audit.log"

[cache]
cluster_overview_ttl_ms = 3000
topic_list_ttl_ms = 5000
broker_metrics_ttl_ms = 2000
consumer_lag_ttl_ms = 1000
"#,
        );

        let err = McpConfig::load(&path).unwrap_err();
        assert!(err.to_string().contains("at least one cluster"));
        let _ = fs::remove_file(path);
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

    fn write_temp_config(contents: &str) -> std::path::PathBuf {
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!("rocketmq-mcp-config-{suffix}.toml"));
        fs::write(&path, contents).unwrap();
        path
    }

    fn example_config_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("mcp.example.toml")
    }
}
