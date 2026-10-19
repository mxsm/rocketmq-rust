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

use super::HttpTlsConfig;
use super::McpConfig;
use crate::guard::policy::BUILTIN_PERMISSIONS_PATH;

pub(super) fn load() -> crate::McpResult<McpConfig> {
    let mut config: McpConfig = config::Config::builder()
        .add_source(config::File::from_str(
            include_str!("../../conf/mcp.example.toml"),
            config::FileFormat::Toml,
        ))
        .build()?
        .try_deserialize()?;
    // Local startup must work from a copied binary without files from the checkout.
    config.security.permissions_file = BUILTIN_PERMISSIONS_PATH.to_string();
    config.audit.sink = "tracing".to_string();
    config.audit.path.clear();
    config.server.http.tls = HttpTlsConfig::default();
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Args;
    use crate::config::TransportKind;
    use crate::guard::context::Principal;
    use crate::guard::policy::PolicyEngine;
    use crate::guard::RiskLevel;
    use std::path::Path;

    fn default_args() -> Args {
        Args {
            config: String::new(),
            transport: TransportKind::Stdio,
            bind: None,
            endpoint: None,
        }
    }

    #[test]
    fn local_defaults_load_without_files_and_enforce_query_policy() {
        let config = McpConfig::load_with_overrides(&default_args()).unwrap();
        let policy = PolicyEngine::load(Path::new(&config.security.permissions_file)).unwrap();
        let principal = Principal::local(&config.security.profile);

        assert!(policy.allows_tool(&principal, "rocketmq_list_clusters", RiskLevel::ReadOnly));
        assert!(policy.allows_tool(&principal, "rocketmq_diagnose_broker_health", RiskLevel::Diagnose));
        assert!(!policy.allows_tool(&principal, "rocketmq_upsert_topic", RiskLevel::Destructive));
        assert!(!policy.allows_tool(&principal, "rocketmq_plan_create_topic", RiskLevel::Plan));
        assert!(config.security.sanitize_output);
        assert!(!config.security.allow_change_planning);
        assert!(config.audit.enabled);
        assert_eq!(config.audit.sink, "tracing");
    }

    #[test]
    fn local_defaults_apply_and_validate_command_line_overrides() {
        let mut args = default_args();
        args.bind = Some("127.0.0.1:9090".to_string());
        args.endpoint = Some("/custom-mcp".to_string());
        let config = McpConfig::load_with_overrides(&args).unwrap();
        assert_eq!(config.server.http.bind, "127.0.0.1:9090");
        assert_eq!(config.server.http.endpoint, "/custom-mcp");

        args.endpoint = Some("invalid-endpoint".to_string());
        assert!(McpConfig::load_with_overrides(&args).is_err());
    }

    #[test]
    fn missing_explicit_configuration_does_not_fall_back_to_defaults() {
        let directory = tempfile::tempdir().unwrap();
        let mut args = default_args();
        args.config = directory.path().join("missing.toml").to_string_lossy().into_owned();
        assert!(McpConfig::load_with_overrides(&args).is_err());
    }

    #[test]
    fn http_requires_explicit_configuration_even_when_compiled() {
        let mut args = default_args();
        args.transport = TransportKind::StreamableHttp;
        let error = McpConfig::load_with_overrides(&args).unwrap_err();
        assert_eq!(error.to_string(), "MCP configuration is invalid");
    }
}
