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

use clap::Parser;
use serde::Deserialize;

#[derive(Debug, Clone, Parser)]
pub struct Args {
    #[arg(long, default_value = "rocketmq-tools/rocketmq-mcp/conf/mcp.example.toml")]
    pub config: String,

    #[arg(long, default_value = "stdio")]
    pub transport: String,

    #[arg(long)]
    pub bind: Option<String>,

    #[arg(long)]
    pub endpoint: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct McpConfig {
    pub server: ServerConfig,
    pub clusters: Vec<ClusterConfig>,
    pub security: SecurityConfig,
    pub audit: AuditConfig,
    pub cache: CacheConfig,
}

impl McpConfig {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let config = config::Config::builder()
            .add_source(config::File::from(path.as_ref()))
            .build()?;
        Ok(config.try_deserialize()?)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub name: String,
    pub version: String,
    pub transport: String,
    pub log_level: String,
    pub stdio: StdioConfig,
    pub http: HttpConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StdioConfig {
    pub log_to_stderr: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpConfig {
    pub bind: String,
    pub endpoint: String,
    pub validate_origin: bool,
    pub allowed_origins: Vec<String>,
    pub require_auth: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClusterConfig {
    pub name: String,
    pub namesrv_addr: String,
    pub default: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SecurityConfig {
    pub profile: String,
    pub allow_dangerous_tools: bool,
    pub require_confirmation: bool,
    pub sanitize_output: bool,
    pub rate_limit_per_minute: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuditConfig {
    pub enabled: bool,
    pub sink: String,
    pub path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    pub cluster_overview_ttl_ms: u64,
    pub topic_list_ttl_ms: u64,
    pub broker_metrics_ttl_ms: u64,
    pub consumer_lag_ttl_ms: u64,
}
