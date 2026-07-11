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
pub mod context;
#[cfg(feature = "streamable-http")]
pub mod http_auth;
pub mod policy;
pub mod rate_limit;
pub mod sanitizer;

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

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
use crate::guard::context::RequestContext;
use crate::guard::policy::PolicyEngine;
use crate::guard::rate_limit::RateLimiter;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    ReadOnly,
    Diagnose,
    Plan,
    Destructive,
}

impl RiskLevel {
    pub fn is_planning(self) -> bool {
        matches!(self, Self::Plan)
    }
}

impl std::fmt::Display for RiskLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadOnly => f.write_str("ReadOnly"),
            Self::Diagnose => f.write_str("Diagnose"),
            Self::Plan => f.write_str("Plan"),
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

    #[error("change planning disabled: {0}")]
    ChangePlanningDisabled(String),
}

#[derive(Debug, Clone)]
pub struct Guard {
    security: SecurityConfig,
    audit_config: AuditConfig,
    allowed_clusters: Arc<[String]>,
    audit_log: AuditLog,
    rate_limiter: RateLimiter,
    policy: PolicyEngine,
    cluster_concurrency: Arc<std::collections::HashMap<String, Arc<Semaphore>>>,
    next_request_id: Arc<AtomicU64>,
}

impl Guard {
    pub fn new(
        security: SecurityConfig,
        audit_config: AuditConfig,
        clusters: &[ClusterConfig],
    ) -> Result<Self, GuardError> {
        let allowed_clusters = clusters
            .iter()
            .map(|cluster| cluster.name.clone())
            .collect::<Vec<_>>()
            .into();
        let policy = PolicyEngine::load(std::path::Path::new(&security.permissions_file))?;
        let cluster_concurrency = clusters
            .iter()
            .map(|cluster| {
                (
                    cluster.name.clone(),
                    Arc::new(Semaphore::new(security.max_concurrent_requests_per_cluster)),
                )
            })
            .collect();
        Ok(Self {
            security,
            audit_config,
            allowed_clusters,
            audit_log: AuditLog::default(),
            rate_limiter: RateLimiter::default(),
            policy,
            cluster_concurrency: Arc::new(cluster_concurrency),
            next_request_id: Arc::new(AtomicU64::new(1)),
        })
    }

    pub fn audit_log(&self) -> AuditLog {
        self.audit_log.clone()
    }

    pub fn audit_metrics(&self) -> crate::guard::audit::AuditMetrics {
        self.audit_log.metrics()
    }

    pub fn begin_tool_call(
        &self,
        context: &RequestContext,
        tool_name: &str,
        risk_level: RiskLevel,
        arguments: &JsonObject,
    ) -> Result<GuardedToolCall, GuardError> {
        let mut guarded = GuardedToolCall {
            guard: self.clone(),
            request_id: self.allocate_request_id(),
            tool_name: tool_name.to_string(),
            risk_level,
            principal: context.principal.clone(),
            client: context.client.clone(),
            cluster: extract_cluster(arguments),
            arguments_hash: hash_arguments(arguments),
            started_at: Instant::now(),
            _cluster_permit: None,
        };

        if let Err(error) = self.validate_cluster(arguments) {
            guarded.record_failure(error.to_string());
            return Err(error);
        }

        if let Err(error) = self.check_tool_availability(tool_name, risk_level) {
            guarded.record_failure(error.to_string());
            return Err(error);
        }

        if let Err(error) =
            self.policy
                .authorize_tool(&context.principal, tool_name, guarded.cluster.as_deref(), risk_level)
        {
            guarded.record_failure(error.to_string());
            return Err(error);
        }

        if let Err(error) = self.rate_limiter.check(
            &context.principal.id,
            guarded.cluster.as_deref(),
            tool_name,
            self.security.rate_limit_per_minute,
        ) {
            guarded.record_failure(error.to_string());
            return Err(error);
        }

        match self.acquire_cluster_permit(guarded.cluster.as_deref()) {
            Ok(permit) => guarded._cluster_permit = permit,
            Err(error) => {
                guarded.record_failure(error.to_string());
                return Err(error);
            }
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

    fn check_tool_availability(&self, tool_name: &str, risk_level: RiskLevel) -> Result<(), GuardError> {
        if matches!(risk_level, RiskLevel::Destructive) {
            return Err(GuardError::ChangePlanningDisabled(format!(
                "{tool_name} is destructive and is not implemented"
            )));
        }

        if risk_level.is_planning() && !self.security.allow_change_planning {
            return Err(GuardError::ChangePlanningDisabled(format!(
                "{tool_name} requires the change-planning feature and runtime opt-in"
            )));
        }

        Ok(())
    }

    pub fn local_request_context(&self) -> RequestContext {
        RequestContext::local(&self.security.profile)
    }

    fn acquire_cluster_permit(&self, cluster: Option<&str>) -> Result<Option<OwnedSemaphorePermit>, GuardError> {
        let Some(cluster) = cluster else {
            return Ok(None);
        };
        let semaphore = self
            .cluster_concurrency
            .get(cluster)
            .ok_or_else(|| GuardError::InvalidArgument(format!("cluster `{cluster}` is not configured")))?;
        semaphore
            .clone()
            .try_acquire_owned()
            .map(Some)
            .map_err(|_| GuardError::RateLimited(format!("cluster `{cluster}` has reached its concurrency limit")))
    }

    pub fn authorize_resource(&self, context: &RequestContext, cluster: &str) -> Result<(), GuardError> {
        self.policy.authorize_resource(&context.principal, cluster)
    }

    pub fn begin_resource_read(
        &self,
        context: &RequestContext,
        cluster: &str,
    ) -> Result<GuardedResourceRead, GuardError> {
        self.authorize_resource(context, cluster)?;
        self.rate_limiter.check(
            &context.principal.id,
            Some(cluster),
            "resource_read",
            self.security.rate_limit_per_minute,
        )?;
        Ok(GuardedResourceRead {
            _cluster_permit: self.acquire_cluster_permit(Some(cluster))?,
        })
    }

    pub fn allows_tool(&self, context: &RequestContext, tool_name: &str, risk: RiskLevel) -> bool {
        self.policy.allows_tool(&context.principal, tool_name, risk)
    }

    pub fn allows_resources(&self, context: &RequestContext) -> bool {
        self.policy.allows_resources(&context.principal)
    }

    #[cfg(feature = "streamable-http")]
    pub fn check_http_rate_limit(&self, context: &RequestContext) -> Result<(), GuardError> {
        self.rate_limiter.check(
            &context.principal.id,
            None,
            "http_request",
            self.security.rate_limit_per_minute,
        )
    }

    #[cfg(feature = "streamable-http")]
    pub fn record_http_rejection(&self, context: &RequestContext, error: impl Into<String>) {
        if !self.audit_config.enabled {
            return;
        }

        let record = AuditRecord::new(
            self.allocate_request_id(),
            context.principal.id.clone(),
            context.client.clone(),
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

#[derive(Debug)]
pub struct GuardedToolCall {
    guard: Guard,
    request_id: String,
    principal: crate::guard::context::Principal,
    client: Option<String>,
    tool_name: String,
    risk_level: RiskLevel,
    cluster: Option<String>,
    arguments_hash: String,
    started_at: Instant,
    _cluster_permit: Option<OwnedSemaphorePermit>,
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
            self.principal.id.clone(),
            self.client.clone(),
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
            .begin_tool_call(
                &guard.local_request_context(),
                "rocketmq_diagnose_consumer_lag",
                RiskLevel::Diagnose,
                &arguments,
            )
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
            .begin_tool_call(
                &guard.local_request_context(),
                "rocketmq_get_cluster_overview",
                RiskLevel::ReadOnly,
                &arguments,
            )
            .unwrap();
        guard
            .begin_tool_call(
                &guard.local_request_context(),
                "rocketmq_diagnose_consumer_lag",
                RiskLevel::Diagnose,
                &arguments,
            )
            .unwrap();
    }

    #[test]
    fn change_planning_is_disabled_without_runtime_opt_in() {
        let guard = test_guard("operator", false, 60);
        let arguments = serde_json::json!({ "cluster": "local-dev" })
            .as_object()
            .unwrap()
            .clone();

        let err = guard
            .begin_tool_call(
                &guard.local_request_context(),
                "rocketmq_plan_reset_consumer_offset",
                RiskLevel::Plan,
                &arguments,
            )
            .unwrap_err();

        assert!(err.to_string().contains("change planning disabled"));
    }

    #[test]
    fn rate_limit_denial_is_audited() {
        let guard = test_guard("diagnose", false, 1);
        let arguments = serde_json::json!({ "cluster": "local-dev" })
            .as_object()
            .unwrap()
            .clone();

        guard
            .begin_tool_call(
                &guard.local_request_context(),
                "rocketmq_get_cluster_overview",
                RiskLevel::ReadOnly,
                &arguments,
            )
            .unwrap();
        let err = guard
            .begin_tool_call(
                &guard.local_request_context(),
                "rocketmq_get_cluster_overview",
                RiskLevel::ReadOnly,
                &arguments,
            )
            .unwrap_err();

        assert!(err.to_string().contains("rate limit exceeded"));
        let records = guard.audit_log().records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, AuditStatus::Failure);
    }

    #[test]
    fn security_policy_denies_destructive_tools_even_for_operator() {
        let guard = test_guard("operator", true, 60);
        let arguments = serde_json::json!({ "cluster": "local-dev" })
            .as_object()
            .unwrap()
            .clone();

        let err = guard
            .begin_tool_call(
                &guard.local_request_context(),
                "rocketmq_delete_topic",
                RiskLevel::Destructive,
                &arguments,
            )
            .unwrap_err();

        assert!(err.to_string().to_ascii_lowercase().contains("destructive"));
        let records = guard.audit_log().records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, AuditStatus::Failure);
    }

    #[test]
    fn cluster_concurrency_limit_is_held_for_the_tool_call_lifetime() {
        let guard = test_guard_with_concurrency("diagnose", false, 60, 1);
        let arguments = serde_json::json!({ "cluster": "local-dev" })
            .as_object()
            .unwrap()
            .clone();
        let first = guard
            .begin_tool_call(
                &guard.local_request_context(),
                "rocketmq_get_cluster_overview",
                RiskLevel::ReadOnly,
                &arguments,
            )
            .unwrap();

        let error = guard
            .begin_tool_call(
                &guard.local_request_context(),
                "rocketmq_get_cluster_overview",
                RiskLevel::ReadOnly,
                &arguments,
            )
            .unwrap_err();

        assert!(error.to_string().contains("concurrency limit"));
        drop(first);
    }

    fn test_guard(profile: &str, allow_change_planning: bool, rate_limit_per_minute: u32) -> Guard {
        test_guard_with_concurrency(profile, allow_change_planning, rate_limit_per_minute, 8)
    }

    fn test_guard_with_concurrency(
        profile: &str,
        allow_change_planning: bool,
        rate_limit_per_minute: u32,
        max_concurrent_requests_per_cluster: usize,
    ) -> Guard {
        Guard::new(
            SecurityConfig {
                profile: profile.to_string(),
                allow_change_planning,
                sanitize_output: true,
                rate_limit_per_minute,
                permissions_file: permission_path(),
                max_concurrent_requests_per_cluster,
            },
            AuditConfig {
                enabled: true,
                sink: "memory".to_string(),
                path: String::new(),
                queue_capacity: 16,
            },
            &[ClusterConfig {
                name: "local-dev".to_string(),
                namesrv_addr: "127.0.0.1:9876".to_string(),
                default: Some(true),
            }],
        )
        .unwrap()
    }

    fn permission_path() -> String {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("permissions.example.toml")
            .to_string_lossy()
            .into_owned()
    }
}

#[derive(Debug)]
pub struct GuardedResourceRead {
    _cluster_permit: Option<OwnedSemaphorePermit>,
}
