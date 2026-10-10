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
#[cfg(feature = "streamable-http")]
pub mod jwks;
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
use crate::resources::uri::ResourceAuthorization;
use crate::resources::uri::ResourceKind;
use crate::tools::catalog::ClusterArg;
use crate::tools::catalog::ToolId;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GuardRejection {
    InvalidArgument,
    PermissionDenied,
    UnauthorizedScope,
    TenantMismatch,
    ClusterNotAllowed,
    RateLimited,
    ChangePlanningDisabled,
}

impl std::fmt::Display for GuardRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidArgument => "invalid guard argument",
            Self::PermissionDenied => "permission denied",
            Self::UnauthorizedScope => "required scope is unavailable",
            Self::TenantMismatch => "tenant boundary mismatch",
            Self::ClusterNotAllowed => "cluster is not allowed",
            Self::RateLimited => "rate limit exceeded",
            Self::ChangePlanningDisabled => "change planning disabled by server policy",
        })
    }
}

#[derive(Debug, Clone)]
pub struct Guard {
    security: SecurityConfig,
    audit_config: AuditConfig,
    allowed_clusters: Arc<[String]>,
    default_cluster: Option<Arc<str>>,
    cluster_tenants: Arc<std::collections::HashMap<String, Option<String>>>,
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
    ) -> crate::McpResult<Self> {
        let allowed_clusters = clusters
            .iter()
            .map(|cluster| cluster.name.clone())
            .collect::<Vec<_>>()
            .into();
        let default_cluster = crate::config::default_cluster(clusters).map(|cluster| Arc::from(cluster.name.as_str()));
        let policy = PolicyEngine::load(std::path::Path::new(&security.permissions_file))?;
        let cluster_tenants = clusters
            .iter()
            .map(|cluster| (cluster.name.clone(), cluster.tenant.clone()))
            .collect();
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
            default_cluster,
            cluster_tenants: Arc::new(cluster_tenants),
            audit_log: AuditLog::default(),
            rate_limiter: RateLimiter::default(),
            policy,
            cluster_concurrency: Arc::new(cluster_concurrency),
            next_request_id: Arc::new(AtomicU64::new(1)),
        })
    }

    /// Reports rate-limit decisions and audit backlog, drops, and failures through `metrics`.
    ///
    /// Apply this before the Guard is shared or its audit log is started: the audit log and
    /// the rate limiter are replaced by fresh instances bound to `metrics`.
    pub(crate) fn with_metrics(mut self, metrics: rocketmq_observability::metrics::mcp::McpMetricsRecorder) -> Self {
        self.audit_log = AuditLog::with_metrics(metrics.clone());
        self.rate_limiter = RateLimiter::with_metrics(metrics);
        self
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
    ) -> Result<GuardedToolCall, GuardRejection> {
        let effective_cluster = self.effective_cluster(tool_name, arguments);
        let mut guarded = GuardedToolCall {
            guard: self.clone(),
            request_id: self.allocate_request_id(),
            tool_name: tool_name.to_string(),
            risk_level,
            principal: context.principal.clone(),
            client: context.client.clone(),
            cluster: effective_cluster.as_ref().ok().cloned(),
            arguments_hash: hash_arguments(arguments),
            started_at: Instant::now(),
            _cluster_permit: None,
        };

        let cluster = match effective_cluster {
            Ok(cluster) => cluster,
            Err(error) => {
                guarded.record_failure(error.to_string());
                return Err(error);
            }
        };

        if let Err(error) = self.validate_cluster(context, &cluster) {
            guarded.record_failure(error.to_string());
            return Err(error);
        }

        if let Err(error) = self.check_tool_availability(tool_name, risk_level) {
            guarded.record_failure(error.to_string());
            return Err(error);
        }

        if let Err(error) = self
            .policy
            .authorize_tool(&context.principal, tool_name, Some(&cluster), risk_level)
        {
            guarded.record_failure(error.to_string());
            return Err(error);
        }

        if let Err(error) = self.rate_limiter.check(
            &context.principal.id,
            Some(&cluster),
            tool_name,
            self.security.rate_limit_per_minute,
        ) {
            guarded.record_failure(error.to_string());
            return Err(error);
        }

        match self.acquire_cluster_permit(&cluster) {
            Ok(permit) => guarded._cluster_permit = Some(permit),
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

    /// Resolves the one logical cluster a Tool call reads, before any other check runs.
    ///
    /// The allow-list, tenant, role, rate-limit, and concurrency decisions and the query itself
    /// all use this value, so a blank or omitted `cluster` can never select a cluster that was
    /// not authorized. Names that are not in the catalog are treated as requiring a cluster.
    fn effective_cluster(&self, tool_name: &str, arguments: &JsonObject) -> Result<String, GuardRejection> {
        let cluster_arg = ToolId::resolve(tool_name).map_or(ClusterArg::Required, ToolId::cluster_arg);
        match arguments.get("cluster") {
            Some(Value::String(cluster)) if !cluster.trim().is_empty() => Ok(cluster.trim().to_string()),
            None | Some(Value::Null) if cluster_arg == ClusterArg::OptionalDefault => self
                .default_cluster
                .as_deref()
                .map(ToString::to_string)
                .ok_or(GuardRejection::InvalidArgument),
            _ => Err(GuardRejection::InvalidArgument),
        }
    }

    fn validate_cluster(&self, context: &RequestContext, cluster: &str) -> Result<(), GuardRejection> {
        if !self.allowed_clusters.iter().any(|allowed| allowed == cluster) {
            return Err(GuardRejection::ClusterNotAllowed);
        }

        self.validate_tenant(context, cluster)
    }

    fn validate_tenant(&self, context: &RequestContext, cluster: &str) -> Result<(), GuardRejection> {
        let required = self.cluster_tenants.get(cluster).and_then(Option::as_deref);
        match required {
            None => Ok(()),
            Some(required) if context.principal.tenant.as_deref() == Some(required) => Ok(()),
            Some(_) => Err(GuardRejection::TenantMismatch),
        }
    }

    fn check_tool_availability(&self, _tool_name: &str, risk_level: RiskLevel) -> Result<(), GuardRejection> {
        if matches!(risk_level, RiskLevel::Destructive) {
            return Err(GuardRejection::ChangePlanningDisabled);
        }

        if risk_level.is_planning() && !self.security.allow_change_planning {
            return Err(GuardRejection::ChangePlanningDisabled);
        }

        Ok(())
    }

    pub fn local_request_context(&self) -> RequestContext {
        RequestContext::local(&self.security.profile)
    }

    fn acquire_cluster_permit(&self, cluster: &str) -> Result<OwnedSemaphorePermit, GuardRejection> {
        let semaphore = self
            .cluster_concurrency
            .get(cluster)
            .ok_or(GuardRejection::ClusterNotAllowed)?;
        semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| GuardRejection::RateLimited)
    }

    pub fn authorize_resource(
        &self,
        context: &RequestContext,
        cluster: &str,
        kind: &ResourceKind,
    ) -> Result<(), GuardRejection> {
        match kind.authorization() {
            ResourceAuthorization::Tool(tool) => {
                let descriptor = tool.descriptor();
                self.policy
                    .authorize_tool(&context.principal, descriptor.name, None, descriptor.risk_level)?;
            }
            ResourceAuthorization::Capabilities => {
                if !ToolId::ALL.iter().copied().any(|tool| {
                    let descriptor = tool.descriptor();
                    self.policy
                        .authorize_tool(&context.principal, descriptor.name, None, descriptor.risk_level)
                        .is_ok()
                }) {
                    return Err(GuardRejection::PermissionDenied);
                }
            }
            ResourceAuthorization::SystemDiagnostics => {
                return Err(GuardRejection::InvalidArgument);
            }
        }
        if !self.allowed_clusters.iter().any(|configured| configured == cluster) {
            return Err(GuardRejection::ClusterNotAllowed);
        }
        self.validate_tenant(context, cluster)?;
        match kind.authorization() {
            ResourceAuthorization::Tool(tool) => {
                let descriptor = tool.descriptor();
                self.policy.authorize_tool(
                    &context.principal,
                    descriptor.name,
                    Some(cluster),
                    descriptor.risk_level,
                )
            }
            ResourceAuthorization::Capabilities => {
                if ToolId::ALL.iter().copied().any(|tool| {
                    let descriptor = tool.descriptor();
                    self.policy
                        .authorize_tool(
                            &context.principal,
                            descriptor.name,
                            Some(cluster),
                            descriptor.risk_level,
                        )
                        .is_ok()
                }) {
                    Ok(())
                } else {
                    Err(GuardRejection::PermissionDenied)
                }
            }
            ResourceAuthorization::SystemDiagnostics => Err(GuardRejection::InvalidArgument),
        }
    }

    pub fn begin_resource_read(
        &self,
        context: &RequestContext,
        cluster: &str,
        kind: &ResourceKind,
    ) -> Result<GuardedResourceRead, GuardRejection> {
        let risk_level = match kind.authorization() {
            ResourceAuthorization::Tool(tool) => tool.descriptor().risk_level,
            ResourceAuthorization::Capabilities => RiskLevel::ReadOnly,
            ResourceAuthorization::SystemDiagnostics => RiskLevel::Diagnose,
        };
        let mut guarded = GuardedResourceRead {
            guard: self.clone(),
            request_id: self.allocate_request_id(),
            principal: context.principal.clone(),
            client: context.client.clone(),
            cluster: Some(cluster.to_string()),
            resource_operation: kind.audit_operation(),
            risk_level,
            started_at: Instant::now(),
            _cluster_permit: None,
        };
        if let Err(error) = self.authorize_resource(context, cluster, kind) {
            guarded.record_failure(error.to_string());
            return Err(error);
        }
        if let Err(error) = self.rate_limiter.check(
            &context.principal.id,
            Some(cluster),
            "resource_read",
            self.security.rate_limit_per_minute,
        ) {
            guarded.record_failure(error.to_string());
            return Err(error);
        }
        match self.acquire_cluster_permit(cluster) {
            Ok(permit) => guarded._cluster_permit = Some(permit),
            Err(error) => {
                guarded.record_failure(error.to_string());
                return Err(error);
            }
        }
        Ok(guarded)
    }

    pub fn begin_system_resource_read(
        &self,
        context: &RequestContext,
        kind: &ResourceKind,
    ) -> Result<GuardedResourceRead, GuardRejection> {
        let guarded = GuardedResourceRead {
            guard: self.clone(),
            request_id: self.allocate_request_id(),
            principal: context.principal.clone(),
            client: context.client.clone(),
            cluster: None,
            resource_operation: kind.audit_operation(),
            risk_level: RiskLevel::Diagnose,
            started_at: Instant::now(),
            _cluster_permit: None,
        };
        if let Err(error) = self.policy.authorize_system_resource(&context.principal) {
            guarded.record_failure(error.to_string());
            return Err(error);
        }
        if let Err(error) = self.rate_limiter.check(
            &context.principal.id,
            None,
            "system_resource_read",
            self.security.rate_limit_per_minute,
        ) {
            guarded.record_failure(error.to_string());
            return Err(error);
        }
        Ok(guarded)
    }

    pub fn allows_tool(&self, context: &RequestContext, tool_name: &str, risk: RiskLevel) -> bool {
        self.policy.allows_tool(&context.principal, tool_name, risk)
    }

    pub fn allows_tool_on_cluster(&self, context: &RequestContext, tool: ToolId, cluster: &str) -> bool {
        if !self.allowed_clusters.iter().any(|configured| configured == cluster)
            || self.validate_tenant(context, cluster).is_err()
        {
            return false;
        }
        let descriptor = tool.descriptor();
        self.policy
            .authorize_tool(
                &context.principal,
                descriptor.name,
                Some(cluster),
                descriptor.risk_level,
            )
            .is_ok()
    }

    pub fn allows_resource(&self, context: &RequestContext, cluster: &str, kind: &ResourceKind) -> bool {
        self.authorize_resource(context, cluster, kind).is_ok()
    }

    pub fn allows_resources(&self, context: &RequestContext) -> bool {
        self.policy.allows_resources(&context.principal)
    }

    pub fn allows_system_resources(&self, context: &RequestContext) -> bool {
        self.policy.allows_system_resources(&context.principal)
    }

    pub(crate) fn record_resource_rejection(
        &self,
        context: &RequestContext,
        operation: &'static str,
        reason: &'static str,
    ) {
        if !self.audit_config.enabled {
            return;
        }
        let record = AuditRecord::new(
            self.allocate_request_id(),
            context.principal.id.clone(),
            context.client.clone(),
            None,
            operation.to_string(),
            hash_arguments(&JsonObject::new()),
            RiskLevel::ReadOnly,
            AuditStatus::Failure,
            0,
            Some(reason.to_string()),
        );
        self.audit_log.record(&self.audit_config, record);
    }

    #[cfg(feature = "streamable-http")]
    pub fn check_http_rate_limit(&self, context: &RequestContext) -> Result<(), GuardRejection> {
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
    /// Returns the logical cluster this call was authorized for.
    ///
    /// The query must read exactly this cluster rather than resolving one from the raw arguments.
    pub fn cluster(&self) -> Option<&str> {
        self.cluster.as_deref()
    }

    pub fn finish_result(&self, result: CallToolResult) -> CallToolResult {
        let result = sanitizer::process_call_tool_result(result, &self.request_id, self.guard.security.sanitize_output);

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

        assert_eq!(err, GuardRejection::UnauthorizedScope);
        let records = guard.audit_log().records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, AuditStatus::Failure);
    }

    #[test]
    fn exact_broker_and_proxy_tools_reject_blank_cluster_before_authorization() {
        let guard = test_guard("diagnose", false, 60);
        for tool in [
            "rocketmq_get_broker_diagnostics",
            "rocketmq_get_broker_config_summary",
            "rocketmq_get_broker_log_filter_state",
            "rocketmq_get_proxy_drain_state",
            "rocketmq_get_topic_stats",
            "rocketmq_get_topic_config",
            "rocketmq_get_consumer_group_details",
            "rocketmq_get_consumer_progress",
        ] {
            let arguments = serde_json::json!({ "cluster": "   " }).as_object().unwrap().clone();
            let error = guard
                .begin_tool_call(&guard.local_request_context(), tool, RiskLevel::ReadOnly, &arguments)
                .unwrap_err();
            assert!(matches!(error, GuardRejection::InvalidArgument), "tool={tool}");
        }
    }

    #[test]
    fn blank_cluster_is_rejected_for_every_tool() {
        let guard = two_cluster_guard(8);
        for tool in ToolId::ALL {
            let descriptor = tool.descriptor();
            for cluster in ["", "   "] {
                let arguments = serde_json::json!({ "cluster": cluster }).as_object().unwrap().clone();
                let error = guard
                    .begin_tool_call(
                        &guard.local_request_context(),
                        descriptor.name,
                        descriptor.risk_level,
                        &arguments,
                    )
                    .unwrap_err();
                assert_eq!(error, GuardRejection::InvalidArgument, "tool={}", descriptor.name);
            }
            if tool.cluster_arg() == ClusterArg::Required {
                let error = guard
                    .begin_tool_call(
                        &guard.local_request_context(),
                        descriptor.name,
                        descriptor.risk_level,
                        &JsonObject::new(),
                    )
                    .unwrap_err();
                assert_eq!(error, GuardRejection::InvalidArgument, "tool={}", descriptor.name);
            }
        }
        assert!(guard
            .audit_log()
            .records()
            .iter()
            .all(|record| record.cluster.is_none()));
    }

    #[test]
    fn omitted_cluster_is_authorized_as_the_default_cluster() {
        let guard = two_cluster_guard(1);
        for tool in ["rocketmq_list_topics", "rocketmq_list_consumer_groups"] {
            let mut other_tenant = guard.local_request_context();
            other_tenant.principal.tenant = Some("tenant-a".to_string());
            assert!(
                matches!(
                    guard.begin_tool_call(&other_tenant, tool, RiskLevel::ReadOnly, &JsonObject::new()),
                    Err(GuardRejection::TenantMismatch)
                ),
                "tool={tool}"
            );

            let mut cluster_limited = guard.local_request_context();
            cluster_limited.principal.tenant = Some("tenant-b".to_string());
            cluster_limited.principal.allowed_clusters = Some(["allowed-a".to_string()].into_iter().collect());
            assert!(
                matches!(
                    guard.begin_tool_call(&cluster_limited, tool, RiskLevel::ReadOnly, &JsonObject::new()),
                    Err(GuardRejection::ClusterNotAllowed)
                ),
                "tool={tool}"
            );
            let explicit = serde_json::json!({ "cluster": "allowed-a" })
                .as_object()
                .unwrap()
                .clone();
            assert_eq!(
                guard
                    .begin_tool_call(&cluster_limited, tool, RiskLevel::ReadOnly, &explicit)
                    .unwrap()
                    .cluster(),
                Some("allowed-a"),
                "tool={tool}"
            );

            let mut authorized = guard.local_request_context();
            authorized.principal.tenant = Some("tenant-b".to_string());
            let first = guard
                .begin_tool_call(&authorized, tool, RiskLevel::ReadOnly, &JsonObject::new())
                .unwrap();
            assert_eq!(first.cluster(), Some("secret-b"), "tool={tool}");
            // The default cluster's only concurrency permit is held by `first`.
            assert!(
                matches!(
                    guard.begin_tool_call(&authorized, tool, RiskLevel::ReadOnly, &JsonObject::new()),
                    Err(GuardRejection::RateLimited)
                ),
                "tool={tool}"
            );
            first.record_protocol_error("test completed");
            let records = guard.audit_log().records();
            assert_eq!(
                records.last().unwrap().cluster.as_deref(),
                Some("secret-b"),
                "tool={tool}"
            );
        }
    }

    #[test]
    fn omitted_cluster_without_a_default_cluster_is_rejected() {
        let mut clusters = two_clusters();
        clusters[1].default = Some(false);
        let guard = guard_for_clusters(&clusters, 8);
        for tool in ["rocketmq_list_topics", "rocketmq_list_consumer_groups"] {
            let error = guard
                .begin_tool_call(
                    &guard.local_request_context(),
                    tool,
                    RiskLevel::ReadOnly,
                    &JsonObject::new(),
                )
                .unwrap_err();
            assert_eq!(error, GuardRejection::InvalidArgument, "tool={tool}");
        }
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
    fn binding_metrics_keeps_rate_limiting_and_audit_working() {
        let guard = test_guard("diagnose", false, 1)
            .with_metrics(rocketmq_observability::metrics::mcp::McpMetricsRecorder::noop());
        let arguments = serde_json::json!({ "cluster": "local-dev" })
            .as_object()
            .unwrap()
            .clone();
        let call = || {
            guard.begin_tool_call(
                &guard.local_request_context(),
                "rocketmq_get_cluster_overview",
                RiskLevel::ReadOnly,
                &arguments,
            )
        };

        let first = call().unwrap();
        assert_eq!(call().unwrap_err(), GuardRejection::RateLimited);
        drop(first);

        let records = guard.audit_log().records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, AuditStatus::Failure);
        assert_eq!(guard.audit_metrics().accepted, 1);
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

        assert_eq!(err, GuardRejection::ChangePlanningDisabled);
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

        assert_eq!(error, GuardRejection::RateLimited);
        drop(first);
    }

    #[test]
    fn cluster_tenant_and_system_diagnostic_scope_fail_closed() {
        let guard = test_guard_for_tenant("tenant-a");
        let arguments = serde_json::json!({ "cluster": "local-dev" })
            .as_object()
            .unwrap()
            .clone();
        let mut mismatched = guard.local_request_context();
        mismatched.principal.tenant = Some("tenant-b".to_string());
        assert!(matches!(
            guard.begin_tool_call(
                &mismatched,
                "rocketmq_get_cluster_overview",
                RiskLevel::ReadOnly,
                &arguments,
            ),
            Err(GuardRejection::TenantMismatch)
        ));

        let mut matched = guard.local_request_context();
        matched.principal.tenant = Some("tenant-a".to_string());
        assert!(guard
            .begin_tool_call(
                &matched,
                "rocketmq_get_cluster_overview",
                RiskLevel::ReadOnly,
                &arguments,
            )
            .is_ok());
        assert!(guard.allows_system_resources(&matched));

        let read_only = test_guard("read_only", false, 60);
        assert!(!read_only.allows_system_resources(&read_only.local_request_context()));
    }

    #[test]
    fn scoped_resource_authorization_uses_exact_backing_tool_role_and_cluster() {
        let read_only = test_guard("read_only", false, 60);
        let context = read_only.local_request_context();
        for kind in [
            ResourceKind::BrokerConfigSummary("broker-a".to_string()),
            ResourceKind::TopicStats("orders".to_string()),
            ResourceKind::TopicConfig("orders".to_string()),
            ResourceKind::ConsumerProgress("group-a".to_string()),
        ] {
            assert!(read_only.authorize_resource(&context, "local-dev", &kind).is_ok());
        }
        let diagnostics = ResourceKind::BrokerDiagnostics("broker-a".to_string());
        assert!(matches!(
            read_only.authorize_resource(&context, "local-dev", &diagnostics),
            Err(GuardRejection::UnauthorizedScope | GuardRejection::PermissionDenied)
        ));
        assert!(read_only
            .authorize_resource(
                &context,
                "missing-cluster",
                &ResourceKind::TopicConfig("orders".to_string())
            )
            .is_err());

        let mut cluster_limited = context.clone();
        cluster_limited.principal.allowed_clusters = Some(["other-cluster".to_string()].into_iter().collect());
        assert!(matches!(
            read_only.authorize_resource(
                &cluster_limited,
                "local-dev",
                &ResourceKind::TopicConfig("orders".to_string())
            ),
            Err(GuardRejection::ClusterNotAllowed)
        ));

        let custom = test_guard("custom", false, 60);
        assert!(custom
            .authorize_resource(
                &custom.local_request_context(),
                "local-dev",
                &ResourceKind::TopicConfig("orders".to_string()),
            )
            .is_err());
    }

    #[test]
    fn denied_tool_is_hidden_and_blocks_its_backing_resource() {
        let directory = tempfile::tempdir().unwrap();
        let permissions = directory.path().join("permissions.toml");
        std::fs::write(
            &permissions,
            "[roles.diagnose]\nallowed_clusters = [\"*\"]\nallow_tools = [\"*\"]\ndeny_tools = \
             [\"rocketmq_get_topic_config\"]\n",
        )
        .unwrap();
        let guard = guard_with_permissions(permissions.to_string_lossy().into_owned(), &two_clusters(), 8);
        let context = guard.local_request_context();
        let arguments = serde_json::json!({ "cluster": "allowed-a", "topic": "orders" })
            .as_object()
            .unwrap()
            .clone();

        assert!(!guard.allows_tool(&context, "rocketmq_get_topic_config", RiskLevel::ReadOnly));
        assert_eq!(
            guard
                .begin_tool_call(&context, "rocketmq_get_topic_config", RiskLevel::ReadOnly, &arguments)
                .unwrap_err(),
            GuardRejection::PermissionDenied
        );
        assert!(matches!(
            guard.authorize_resource(&context, "allowed-a", &ResourceKind::TopicConfig("orders".to_string())),
            Err(GuardRejection::PermissionDenied)
        ));

        assert!(guard.allows_tool(&context, "rocketmq_get_topic_stats", RiskLevel::ReadOnly));
        assert!(guard
            .begin_tool_call(&context, "rocketmq_get_topic_stats", RiskLevel::ReadOnly, &arguments)
            .is_ok());
        assert!(guard
            .authorize_resource(&context, "allowed-a", &ResourceKind::TopicStats("orders".to_string()))
            .is_ok());
    }

    #[test]
    fn diagnostic_resource_denial_is_audited_with_diagnose_risk() {
        let guard = test_guard("read_only", false, 60);
        let uri = "rocketmq://clusters/local-dev/brokers/broker-a/diagnostics";
        assert!(guard
            .begin_resource_read(
                &guard.local_request_context(),
                "local-dev",
                &ResourceKind::BrokerDiagnostics("broker-a".to_string()),
            )
            .is_err());
        let records = guard.audit_log().records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].tool, "resource:broker_diagnostics");
        assert!(!records[0].tool.contains(uri));
        assert_eq!(records[0].risk_level, RiskLevel::Diagnose);
        assert_eq!(records[0].status, AuditStatus::Failure);
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
                max_record_bytes: 16 * 1024,
                queue_max_bytes: 1024 * 1024,
            },
            &[ClusterConfig {
                name: "local-dev".to_string(),
                namesrv_addr: "127.0.0.1:9876".to_string(),
                default: Some(true),
                rocketmq_cluster_name: None,
                tenant: None,
                credentials: None,
                proxies: Vec::new(),
                controllers: Vec::new(),
            }],
        )
        .unwrap()
    }

    fn test_guard_for_tenant(tenant: &str) -> Guard {
        Guard::new(
            SecurityConfig {
                profile: "diagnose".to_string(),
                allow_change_planning: false,
                sanitize_output: true,
                rate_limit_per_minute: 60,
                permissions_file: permission_path(),
                max_concurrent_requests_per_cluster: 8,
            },
            AuditConfig {
                enabled: true,
                sink: "memory".to_string(),
                path: String::new(),
                queue_capacity: 16,
                max_record_bytes: 16 * 1024,
                queue_max_bytes: 1024 * 1024,
            },
            &[ClusterConfig {
                name: "local-dev".to_string(),
                namesrv_addr: "127.0.0.1:9876".to_string(),
                default: Some(true),
                rocketmq_cluster_name: None,
                tenant: Some(tenant.to_string()),
                credentials: None,
                proxies: Vec::new(),
                controllers: Vec::new(),
            }],
        )
        .unwrap()
    }

    /// `allowed-a` has no tenant binding; the default cluster `secret-b` is bound to `tenant-b`.
    fn two_clusters() -> Vec<ClusterConfig> {
        let cluster = |name: &str, default: bool, tenant: Option<&str>| ClusterConfig {
            name: name.to_string(),
            namesrv_addr: "127.0.0.1:9876".to_string(),
            default: Some(default),
            rocketmq_cluster_name: None,
            tenant: tenant.map(ToString::to_string),
            credentials: None,
            proxies: Vec::new(),
            controllers: Vec::new(),
        };
        vec![
            cluster("allowed-a", false, None),
            cluster("secret-b", true, Some("tenant-b")),
        ]
    }

    fn two_cluster_guard(max_concurrent_requests_per_cluster: usize) -> Guard {
        guard_for_clusters(&two_clusters(), max_concurrent_requests_per_cluster)
    }

    fn guard_for_clusters(clusters: &[ClusterConfig], max_concurrent_requests_per_cluster: usize) -> Guard {
        guard_with_permissions(permission_path(), clusters, max_concurrent_requests_per_cluster)
    }

    fn guard_with_permissions(
        permissions_file: String,
        clusters: &[ClusterConfig],
        max_concurrent_requests_per_cluster: usize,
    ) -> Guard {
        Guard::new(
            SecurityConfig {
                profile: "diagnose".to_string(),
                allow_change_planning: false,
                sanitize_output: true,
                rate_limit_per_minute: 60,
                permissions_file,
                max_concurrent_requests_per_cluster,
            },
            AuditConfig {
                enabled: true,
                sink: "memory".to_string(),
                path: String::new(),
                queue_capacity: 256,
                max_record_bytes: 16 * 1024,
                queue_max_bytes: 1024 * 1024,
            },
            clusters,
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
    guard: Guard,
    request_id: String,
    principal: crate::guard::context::Principal,
    client: Option<String>,
    cluster: Option<String>,
    resource_operation: String,
    risk_level: RiskLevel,
    started_at: Instant,
    _cluster_permit: Option<OwnedSemaphorePermit>,
}

impl GuardedResourceRead {
    pub fn finish_result(
        &self,
        result: Result<rmcp::model::ReadResourceResult, rmcp::ErrorData>,
    ) -> Result<rmcp::model::ReadResourceResult, rmcp::ErrorData> {
        match result {
            Ok(result) => {
                let result = sanitizer::process_read_resource_result(
                    result,
                    &self.request_id,
                    self.guard.security.sanitize_output,
                );
                match result {
                    Ok(result) => {
                        self.record(AuditStatus::Success, None);
                        Ok(result)
                    }
                    Err(error) => {
                        self.record(AuditStatus::Failure, Some(error.to_string()));
                        Err(error)
                    }
                }
            }
            Err(error) => {
                self.record(AuditStatus::Failure, Some(error.to_string()));
                Err(with_resource_correlation(error, &self.request_id))
            }
        }
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
            self.resource_operation.clone(),
            hash_arguments(&JsonObject::new()),
            self.risk_level,
            status,
            self.started_at.elapsed().as_millis(),
            error,
        );
        self.guard.audit_log.record(&self.guard.audit_config, record);
    }
}

fn with_resource_correlation(mut error: rmcp::ErrorData, request_id: &str) -> rmcp::ErrorData {
    let mut data = match error.data.take() {
        Some(Value::Object(data)) => data,
        Some(data) => serde_json::Map::from_iter([("details".to_string(), data)]),
        None => serde_json::Map::new(),
    };
    data.entry("code".to_string())
        .or_insert_with(|| Value::String("resource_error".to_string()));
    data.entry("retryable".to_string()).or_insert(Value::Bool(false));
    data.insert("correlation_id".to_string(), Value::String(request_id.to_string()));
    error.data = Some(Value::Object(data));
    error
}
