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

//! Admission, audit, and sanitization of argument completion.
//!
//! A completion is answered from the data behind one listing Tool. It is therefore admitted the
//! way a call of that Tool is, and recorded in the same audit log under its own operation name.

use std::time::Instant;

use rmcp::model::JsonObject;
use tokio::sync::OwnedSemaphorePermit;

use super::hash_arguments;
use super::sanitizer;
use super::AuditRecord;
use super::AuditStatus;
use super::Guard;
use super::GuardRejection;
use super::RequestContext;
use super::RiskLevel;
use crate::tools::catalog::ToolId;

/// The rate-limit operation of every completion: typing does not spend the budget of Tool calls.
const RATE_LIMIT_OPERATION: &str = "completion";

impl Guard {
    /// Admits one completion request that is answered from the data behind `tool`.
    ///
    /// `cluster` is the logical cluster the candidates are read from, or `None` when the request
    /// reads none. The checks and their order are those of a Tool call: the cluster's
    /// configuration and tenant binding, the role and scope for `tool`, the rate limit, and the
    /// cluster's concurrency permit. Every outcome is audited as `operation`; `arguments` is
    /// recorded as a hash only.
    pub(crate) fn begin_completion(
        &self,
        context: &RequestContext,
        tool: ToolId,
        cluster: Option<&str>,
        operation: &'static str,
        arguments: &JsonObject,
    ) -> Result<GuardedCompletion, GuardRejection> {
        let descriptor = tool.descriptor();
        let mut guarded = GuardedCompletion {
            guard: self.clone(),
            request_id: self.allocate_request_id(),
            principal_id: context.principal.id.clone(),
            client: context.client.clone(),
            cluster: cluster.map(ToString::to_string),
            operation,
            risk_level: descriptor.risk_level,
            arguments_hash: hash_arguments(arguments),
            started_at: Instant::now(),
            _cluster_permit: None,
        };
        let admitted = self
            .admit_completion(context, descriptor.name, descriptor.risk_level, cluster)
            .and_then(|()| cluster.map(|cluster| self.acquire_cluster_permit(cluster)).transpose());
        match admitted {
            Ok(permit) => {
                guarded._cluster_permit = permit;
                Ok(guarded)
            }
            Err(error) => {
                guarded.record(AuditStatus::Failure, Some(error.to_string()));
                Err(error)
            }
        }
    }

    fn admit_completion(
        &self,
        context: &RequestContext,
        tool_name: &str,
        risk_level: RiskLevel,
        cluster: Option<&str>,
    ) -> Result<(), GuardRejection> {
        if let Some(cluster) = cluster {
            self.validate_cluster(context, cluster)?;
        }
        self.check_tool_availability(tool_name, risk_level)?;
        self.policy
            .authorize_tool(&context.principal, tool_name, cluster, risk_level)?;
        self.rate_limiter.check(
            &context.principal.id,
            cluster,
            RATE_LIMIT_OPERATION,
            self.security.rate_limit_per_minute,
        )
    }
}

/// One admitted completion request. It holds the cluster's concurrency permit until it is dropped.
#[derive(Debug)]
pub(crate) struct GuardedCompletion {
    guard: Guard,
    request_id: String,
    principal_id: String,
    client: Option<String>,
    cluster: Option<String>,
    operation: &'static str,
    risk_level: RiskLevel,
    arguments_hash: String,
    started_at: Instant,
    _cluster_permit: Option<OwnedSemaphorePermit>,
}

impl GuardedCompletion {
    /// Records the request as answered and returns the candidates that may be published.
    ///
    /// A candidate the output sanitizer would rewrite, such as a name that reads as a network
    /// address, is left out: a redaction marker is not a value a client can complete to.
    pub(crate) fn finish(&self, candidates: Vec<String>) -> Vec<String> {
        let candidates = if self.guard.security.sanitize_output {
            candidates
                .into_iter()
                .filter(|candidate| sanitizer::sanitize_text(candidate) == *candidate)
                .collect()
        } else {
            candidates
        };
        self.record(AuditStatus::Success, None);
        candidates
    }

    /// Records that the candidates could not be read. `reason` is a stable code, not source text;
    /// the audit log keeps only that the request failed.
    pub(crate) fn fail(&self, reason: &'static str) {
        self.record(AuditStatus::Failure, Some(reason.to_string()));
    }

    fn record(&self, status: AuditStatus, error: Option<String>) {
        if !self.guard.audit_config.enabled {
            return;
        }
        let record = AuditRecord::new(
            self.request_id.clone(),
            self.principal_id.clone(),
            self.client.clone(),
            self.cluster.clone(),
            self.operation.to_string(),
            self.arguments_hash.clone(),
            self.risk_level,
            status,
            self.started_at.elapsed().as_millis(),
            error,
        );
        self.guard.audit_log.record(&self.guard.audit_config, record);
    }
}
