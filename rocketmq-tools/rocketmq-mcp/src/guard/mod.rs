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

pub mod audit;
pub mod confirmation;
#[cfg(feature = "streamable-http")]
pub mod http_auth;
pub mod rate_limit;
pub mod rbac;
pub mod sanitizer;

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use rmcp::model::CallToolResult;
use rmcp::model::JsonObject;
use serde::Serialize;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;

use crate::config::AuditConfig;
use crate::config::ClusterConfig;
use crate::config::SecurityConfig;
use crate::guard::audit::AuditLog;
use crate::guard::audit::AuditRecord;
use crate::guard::audit::AuditStatus;
use crate::guard::rate_limit::RateLimiter;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    ReadOnly,
    Diagnose,
    Change,
    Destructive,
}

impl RiskLevel {
    pub fn is_dangerous(self) -> bool {
        matches!(self, Self::Change | Self::Destructive)
    }
}

impl std::fmt::Display for RiskLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadOnly => f.write_str("ReadOnly"),
            Self::Diagnose => f.write_str("Diagnose"),
            Self::Change => f.write_str("Change"),
            Self::Destructive => f.write_str("Destructive"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GuardError {
    #[error("invalid guard argument: {0}")]
    InvalidArgument(String),

    #[error("permission denied: {0}")]
    PermissionDenied(String),

    #[error("rate limit exceeded: {0}")]
    RateLimited(String),

    #[error("dangerous tool disabled: {0}")]
    DangerousToolDisabled(String),

    #[error("confirmation required: {0}")]
    ConfirmationRequired(String),
}

#[derive(Debug, Clone)]
pub struct Guard {
    security: SecurityConfig,
    audit_config: AuditConfig,
    allowed_clusters: Arc<[String]>,
    audit_log: AuditLog,
    rate_limiter: RateLimiter,
    next_request_id: Arc<AtomicU64>,
}

impl Guard {
    pub fn new(security: SecurityConfig, audit_config: AuditConfig, clusters: &[ClusterConfig]) -> Self {
        let allowed_clusters = clusters
            .iter()
            .map(|cluster| cluster.name.clone())
            .collect::<Vec<_>>()
            .into();
        Self {
            security,
            audit_config,
            allowed_clusters,
            audit_log: AuditLog::default(),
            rate_limiter: RateLimiter::default(),
            next_request_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn audit_log(&self) -> AuditLog {
        self.audit_log.clone()
    }

    pub fn begin_tool_call(
        &self,
        tool_name: &str,
        risk_level: RiskLevel,
        arguments: &JsonObject,
    ) -> Result<GuardedToolCall, GuardError> {
        let guarded = GuardedToolCall {
            guard: self.clone(),
            request_id: self.allocate_request_id(),
            tool_name: tool_name.to_string(),
            risk_level,
            cluster: extract_cluster(arguments),
            arguments_hash: hash_arguments(arguments),
            started_at: Instant::now(),
        };

        if let Err(error) = self.validate_cluster(arguments) {
            guarded.record_failure(error.to_string());
            return Err(error);
        }

        if let Err(error) = rbac::check_rbac(&self.security, risk_level) {
            guarded.record_failure(error.to_string());
            return Err(error);
        }

        if let Err(error) = self.check_dangerous_tool(tool_name, risk_level) {
            guarded.record_failure(error.to_string());
            return Err(error);
        }

        if let Err(error) = confirmation::check_confirmation(&self.security, risk_level, arguments) {
            guarded.record_failure(error.to_string());
            return Err(error);
        }

        if let Err(error) = self.rate_limiter.check(tool_name, self.security.rate_limit_per_minute) {
            guarded.record_failure(error.to_string());
            return Err(error);
        }

        Ok(guarded)
    }

    fn allocate_request_id(&self) -> String {
        let id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        format!("mcp-{id}")
    }

    fn validate_cluster(&self, arguments: &JsonObject) -> Result<(), GuardError> {
        let Some(cluster) = extract_cluster(arguments) else {
            return Ok(());
        };

        if self.allowed_clusters.iter().any(|allowed| allowed == &cluster) {
            return Ok(());
        }

        Err(GuardError::InvalidArgument(format!(
            "cluster `{cluster}` is not configured"
        )))
    }

    fn check_dangerous_tool(&self, tool_name: &str, risk_level: RiskLevel) -> Result<(), GuardError> {
        if matches!(risk_level, RiskLevel::Destructive) {
            return Err(GuardError::DangerousToolDisabled(format!(
                "{tool_name} is destructive and is not implemented"
            )));
        }

        if risk_level.is_dangerous() && !self.security.allow_dangerous_tools {
            return Err(GuardError::DangerousToolDisabled(format!(
                "{tool_name} requires the dangerous-tools feature and runtime opt-in"
            )));
        }

        Ok(())
    }

    #[cfg(feature = "streamable-http")]
    pub fn check_http_rate_limit(&self) -> Result<(), GuardError> {
        self.rate_limiter
            .check("http_request", self.security.rate_limit_per_minute)
    }

    #[cfg(feature = "streamable-http")]
    pub fn record_http_rejection(&self, error: impl Into<String>) {
        if !self.audit_config.enabled {
            return;
        }

        let record = AuditRecord::new(
            self.allocate_request_id(),
            "http-client".to_string(),
            None,
            None,
            "http_request".to_string(),
            String::new(),
            RiskLevel::ReadOnly,
            AuditStatus::Failure,
            0,
            Some(error.into()),
        );
        self.audit_log.record(&self.audit_config, record);
    }
}

#[derive(Debug, Clone)]
pub struct GuardedToolCall {
    guard: Guard,
    request_id: String,
    tool_name: String,
    risk_level: RiskLevel,
    cluster: Option<String>,
    arguments_hash: String,
    started_at: Instant,
}

impl GuardedToolCall {
    pub fn finish_result(&self, result: CallToolResult) -> CallToolResult {
        let result = if self.guard.security.sanitize_output {
            sanitizer::sanitize_call_tool_result(result)
        } else {
            result
        };

        let is_error = result.is_error.unwrap_or(false);
        if is_error {
            self.record_failure("tool returned an error result");
        } else {
            self.record_success();
        }

        result
    }

    pub fn record_protocol_error(&self, error: impl Into<String>) {
        self.record_failure(error.into());
    }

    fn record_success(&self) {
        self.record(AuditStatus::Success, None);
    }

    fn record_failure(&self, error: impl Into<String>) {
        self.record(AuditStatus::Failure, Some(error.into()));
    }

    fn record(&self, status: AuditStatus, error: Option<String>) {
        if !self.guard.audit_config.enabled {
            return;
        }

        let record = AuditRecord::new(
            self.request_id.clone(),
            "ai-agent".to_string(),
            None,
            self.cluster.clone(),
            self.tool_name.clone(),
            self.arguments_hash.clone(),
            self.risk_level,
            status,
            self.started_at.elapsed().as_millis(),
            error,
        );
        self.guard.audit_log.record(&self.guard.audit_config, record);
    }
}

fn extract_cluster(arguments: &JsonObject) -> Option<String> {
    arguments
        .get("cluster")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn hash_arguments(arguments: &JsonObject) -> String {
    let bytes = serde_json::to_vec(arguments).unwrap_or_default();
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AuditConfig;
    use crate::config::ClusterConfig;
    use crate::config::SecurityConfig;

    #[test]
    fn read_only_profile_denies_diagnose_tool() {
        let guard = test_guard("read_only", false, 60);
        let arguments = serde_json::json!({ "cluster": "local-dev" })
            .as_object()
            .unwrap()
            .clone();

        let err = guard
            .begin_tool_call("mq_diagnose_consumer_lag", RiskLevel::Diagnose, &arguments)
            .unwrap_err();

        assert!(err.to_string().contains("permission denied"));
        let records = guard.audit_log().records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, AuditStatus::Failure);
    }

    #[test]
    fn diagnose_profile_allows_read_only_and_diagnose_tools() {
        let guard = test_guard("diagnose", false, 60);
        let arguments = serde_json::json!({ "cluster": "local-dev" })
            .as_object()
            .unwrap()
            .clone();

        guard
            .begin_tool_call("mq_cluster_overview", RiskLevel::ReadOnly, &arguments)
            .unwrap();
        guard
            .begin_tool_call("mq_diagnose_consumer_lag", RiskLevel::Diagnose, &arguments)
            .unwrap();
    }

    #[test]
    fn dangerous_tools_are_disabled_without_runtime_opt_in() {
        let guard = test_guard("operator", false, 60);
        let arguments = serde_json::json!({ "cluster": "local-dev", "confirm_token": "yes" })
            .as_object()
            .unwrap()
            .clone();

        let err = guard
            .begin_tool_call("mq_reset_consumer_offset", RiskLevel::Change, &arguments)
            .unwrap_err();

        assert!(err.to_string().contains("dangerous tool disabled"));
    }

    #[test]
    fn rate_limit_denial_is_audited() {
        let guard = test_guard("diagnose", false, 1);
        let arguments = serde_json::json!({ "cluster": "local-dev" })
            .as_object()
            .unwrap()
            .clone();

        guard
            .begin_tool_call("mq_cluster_overview", RiskLevel::ReadOnly, &arguments)
            .unwrap();
        let err = guard
            .begin_tool_call("mq_cluster_overview", RiskLevel::ReadOnly, &arguments)
            .unwrap_err();

        assert!(err.to_string().contains("rate limit exceeded"));
        let records = guard.audit_log().records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, AuditStatus::Failure);
    }

    #[test]
    fn security_policy_denies_destructive_tools_even_for_operator() {
        let guard = test_guard("operator", true, 60);
        let arguments = serde_json::json!({ "cluster": "local-dev", "confirm_token": "yes" })
            .as_object()
            .unwrap()
            .clone();

        let err = guard
            .begin_tool_call("mq_delete_topic", RiskLevel::Destructive, &arguments)
            .unwrap_err();

        assert!(err.to_string().to_ascii_lowercase().contains("destructive"));
        let records = guard.audit_log().records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, AuditStatus::Failure);
    }

    fn test_guard(profile: &str, allow_dangerous_tools: bool, rate_limit_per_minute: u32) -> Guard {
        Guard::new(
            SecurityConfig {
                profile: profile.to_string(),
                allow_dangerous_tools,
                require_confirmation: true,
                sanitize_output: true,
                rate_limit_per_minute,
            },
            AuditConfig {
                enabled: true,
                sink: "memory".to_string(),
                path: String::new(),
            },
            &[ClusterConfig {
                name: "local-dev".to_string(),
                namesrv_addr: "127.0.0.1:9876".to_string(),
                default: Some(true),
            }],
        )
    }
}
