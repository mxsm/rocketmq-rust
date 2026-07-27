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
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::ActionPlan;
use crate::ApprovalGrant;
use crate::ClusterId;
use crate::ContractError;
use crate::CorrelationId;
use crate::ExecutionAction;
use crate::ExecutionId;
use crate::ExecutionStepId;
use crate::LeaseFenceGrant;
use crate::PlanStatus;
use crate::PlanStep;
use crate::TenantId;
use crate::VerificationResult;
use crate::is_sha256_digest;

/// Recoverable supervised execution state.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionState {
    Pending,
    Prechecking,
    IntentPersisted,
    Applying,
    Unknown,
    Reconciling,
    Verifying,
    Compensating,
    Succeeded,
    RolledBack,
    Escalated,
}

impl ExecutionState {
    /// Returns whether this state can move to `next`.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Pending, Self::Prechecking)
                | (Self::Prechecking, Self::IntentPersisted)
                | (Self::IntentPersisted, Self::Applying)
                | (Self::Applying, Self::Verifying | Self::Unknown | Self::Compensating)
                | (Self::Unknown, Self::Reconciling)
                | (
                    Self::Reconciling,
                    Self::Verifying | Self::Compensating | Self::Escalated
                )
                | (Self::Verifying, Self::Succeeded | Self::Compensating)
                | (Self::Prechecking, Self::Compensating)
                | (Self::Compensating, Self::RolledBack | Self::Escalated)
        )
    }
}

/// Explicit state transition appended to the execution timeline.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionTransition {
    pub from: ExecutionState,
    pub to: ExecutionState,
    pub reason_code: String,
    pub occurred_at: DateTime<Utc>,
}

impl ExecutionTransition {
    /// Validates the transition against the closed state graph.
    ///
    /// # Errors
    ///
    /// Rejects transitions not represented by the Phase 3 state machine.
    pub fn validate(&self) -> Result<(), ContractError> {
        if !self.from.can_transition_to(self.to) {
            return Err(ContractError::InvalidStateTransition {
                from: format!("{:?}", self.from),
                to: format!("{:?}", self.to),
            });
        }
        Ok(())
    }
}

/// Short-lived immutable request submitted to Change Executor.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRequest {
    pub schema_version: String,
    pub id: ExecutionId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub correlation_id: CorrelationId,
    pub plan: ActionPlan,
    pub approvals: Vec<ApprovalGrant>,
    pub requested_by: String,
    pub idempotency_key: String,
    pub issuer: String,
    pub audience: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub nonce: String,
    pub signature: String,
}

impl ExecutionRequest {
    pub const SCHEMA_VERSION: &'static str = "rocketmq-sre.execution-request.v1";

    /// Validates the fail-closed envelope before cryptographic verification.
    ///
    /// Signature verification and workload identity authorization remain the
    /// responsibility of the Executor boundary.
    ///
    /// # Errors
    ///
    /// Rejects incompatible schemas, invalid validity windows, modified plans,
    /// missing approvals, audience drift, and approval bindings that do not
    /// match the exact plan snapshot.
    pub fn validate_at(&self, now: DateTime<Utc>, expected_audience: &str) -> Result<(), ContractError> {
        if self.schema_version != Self::SCHEMA_VERSION {
            return Err(ContractError::UnsupportedSchemaFamily {
                actual: self.schema_version.clone(),
                supported: Self::SCHEMA_VERSION.to_owned(),
            });
        }
        if self.requested_by.trim().is_empty()
            || self.idempotency_key.trim().is_empty()
            || self.issuer.trim().is_empty()
            || self.audience != expected_audience
            || self.nonce.trim().is_empty()
            || self.signature.trim().is_empty()
            || self.issued_at > now
            || self.expires_at <= now
            || self.expires_at <= self.issued_at
        {
            return Err(ContractError::InvalidDescriptor {
                reason: "execution request identity, audience, and validity window are invalid".to_owned(),
            });
        }
        self.plan.verify_plan_hash()?;
        if self.plan.status != PlanStatus::Approved
            || self.plan.tenant_id != self.tenant_id
            || self.plan.cluster_id != self.cluster_id
            || self.plan.expires_at <= now
            || self.approvals.is_empty()
        {
            return Err(ContractError::InvalidDescriptor {
                reason: "execution request requires an approved, current, same-scope plan".to_owned(),
            });
        }
        let precondition_hash = self.plan.compute_precondition_hash()?;
        if self.approvals.iter().any(|approval| {
            approval.plan_id != self.plan.id
                || approval.plan_hash != self.plan.plan_hash
                || approval.precondition_hash != precondition_hash
                || !is_sha256_digest(&approval.precondition_hash)
                || approval.tenant_id != self.tenant_id
                || approval.cluster_id != self.cluster_id
                || approval.audience != expected_audience
                || approval.issuer.trim().is_empty()
                || approval.approver_subject.trim().is_empty()
                || approval.nonce.trim().is_empty()
                || approval.signature.trim().is_empty()
                || approval.issued_at > now
                || approval.expires_at <= now
                || approval.expires_at <= approval.issued_at
        }) {
            return Err(ContractError::InvalidDescriptor {
                reason: "approval grant does not bind the current plan and precondition hash".to_owned(),
            });
        }
        Ok(())
    }
}

/// Durable intent written before an Agent dispatch.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepIntent {
    pub execution_id: ExecutionId,
    pub step_id: ExecutionStepId,
    pub plan_hash: String,
    pub step: PlanStep,
    pub attempt: u16,
    pub idempotency_key: String,
    pub fence_grant: LeaseFenceGrant,
    pub intended_at: DateTime<Utc>,
    pub compensation: bool,
}

/// Durable Agent effect state.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectState {
    Prepared,
    Dispatched,
    Confirmed,
    Unknown,
}

/// Narrow request accepted by the Execution Agent registry.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStepRequest {
    pub intent: StepIntent,
    pub action: ExecutionAction,
    pub descriptor_version: String,
    pub target: String,
    pub parameters: Value,
}

/// Narrow Agent result without raw target responses.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStepResult {
    pub execution_id: ExecutionId,
    pub step_id: ExecutionStepId,
    pub state: EffectState,
    pub operation_id: String,
    pub outcome_code: String,
    pub sanitized_summary: String,
    pub completed_at: DateTime<Utc>,
}

/// Append-only Executor step result.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepResult {
    pub step_id: ExecutionStepId,
    pub state: ExecutionState,
    pub agent_result: Option<AgentStepResult>,
    pub verification: Option<VerificationResult>,
    pub reason_code: String,
    pub completed_at: DateTime<Utc>,
}

/// Current execution projection derived from the journal.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionResult {
    pub schema_version: String,
    pub id: ExecutionId,
    pub plan_hash: String,
    pub state: ExecutionState,
    pub step_results: Vec<StepResult>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub cluster_mutation_count: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transition_graph_rejects_skipping_intent() {
        let transition = ExecutionTransition {
            from: ExecutionState::Prechecking,
            to: ExecutionState::Applying,
            reason_code: "skip".to_owned(),
            occurred_at: Utc::now(),
        };

        assert!(transition.validate().is_err());
    }

    #[test]
    fn transition_graph_allows_unknown_reconcile_path() {
        let transition = ExecutionTransition {
            from: ExecutionState::Unknown,
            to: ExecutionState::Reconciling,
            reason_code: "live_state_required".to_owned(),
            occurred_at: Utc::now(),
        };

        assert!(transition.validate().is_ok());
    }
}
