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

use chrono::DateTime;
use chrono::Utc;
use std::future::Future;
use std::pin::Pin;

use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::PlanStepId;

use super::AgentActionHandler;
use super::DriverFuture;
use crate::ExecutionAgentError;

/// Sanitized logger state used by precheck and effect reconciliation.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct LoggerLevelState {
    pub level: String,
    pub active_operation_id: Option<String>,
    pub last_completed_operation_id: Option<String>,
}

/// Closed logger-level mutation accepted by the configuration client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoggerLevelTtlWrite {
    pub component: String,
    pub logger: String,
    pub level: String,
    pub expires_at: DateTime<Utc>,
    pub operation_id: String,
    pub execution_id: ExecutionId,
    pub plan_step_id: PlanStepId,
}

/// Closed restoration request bound to the original execution step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoggerLevelTtlRestore {
    pub component: String,
    pub logger: String,
    pub execution_id: ExecutionId,
    pub plan_step_id: PlanStepId,
    pub operation_id: String,
}

/// Narrow configuration writer. It has no generic key/value method.
pub trait ConfigWriteClient: Send + Sync {
    fn set_logger_level_ttl<'a>(
        &'a self,
        request: &'a LoggerLevelTtlWrite,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecutionAgentError>> + Send + 'a>>;
}

/// Read/restore companion for the bounded logger-level writer.
///
/// Implementations must durably retain the original logger level when
/// [`ConfigWriteClient::set_logger_level_ttl`] succeeds. The original value is
/// addressed by `(execution_id, plan_step_id)` and is never supplied by an
/// untrusted action parameter.
pub trait LoggerLevelControlClient: ConfigWriteClient {
    fn logger_level_state<'a>(&'a self, component: &'a str, logger: &'a str) -> DriverFuture<'a, LoggerLevelState>;

    fn restore_logger_level<'a>(
        &'a self,
        request: &'a LoggerLevelTtlRestore,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecutionAgentError>> + Send + 'a>>;
}

/// Sanitized lifecycle state for one credential set.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct CredentialRotationState {
    pub active_version: String,
    pub retiring_version: Option<String>,
    pub active_healthy: bool,
    pub candidate_probe_healthy: bool,
    pub overlap_deadline: Option<DateTime<Utc>>,
    pub last_operation_id: Option<String>,
}

/// Closed active-to-retiring overlap transition.
///
/// `candidate_secret_ref` names a workload-owned secret and never contains the
/// credential material itself. This type intentionally has no `Debug`
/// implementation.
#[derive(Clone, Eq, PartialEq)]
pub struct CredentialOverlapWrite {
    pub credential_set: String,
    pub active_version: String,
    pub candidate_version: String,
    pub candidate_secret_ref: String,
    pub overlap_seconds: u32,
    pub validation_probe_topic: String,
    pub operation_id: String,
    pub execution_id: ExecutionId,
    pub plan_step_id: PlanStepId,
}

/// Closed rollback bound to a durable overlap snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialOverlapRestore {
    pub credential_set: String,
    pub operation_id: String,
    pub execution_id: ExecutionId,
    pub plan_step_id: PlanStepId,
}

/// Narrow credential lifecycle controller.
///
/// Implementations must durably record the previously active version before
/// activating the candidate, keep both versions valid for the bounded overlap,
/// validate the candidate through the dedicated synthetic Topic, and revoke
/// only after the verification window. No method accepts a secret value,
/// permission change, generic ACL document, or TLS configuration.
pub trait CredentialRotationClient: Send + Sync {
    fn credential_rotation_state<'a>(&'a self, credential_set: &'a str) -> DriverFuture<'a, CredentialRotationState>;

    fn begin_credential_overlap<'a>(&'a self, request: &'a CredentialOverlapWrite) -> DriverFuture<'a, ()>;

    fn restore_previous_credential<'a>(&'a self, request: &'a CredentialOverlapRestore) -> DriverFuture<'a, ()>;
}

/// Typed configuration-system driver.
pub trait ConfigDriver: AgentActionHandler {}
