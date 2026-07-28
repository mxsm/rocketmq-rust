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
use crate::AutonomyGrant;
use crate::ClusterId;
use crate::ContractError;
use crate::CorrelationId;
use crate::DynamicSafetyDecision;
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
                | (Self::Prechecking, Self::IntentPersisted | Self::Escalated)
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub autonomy_grant: Option<AutonomyGrant>,
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
    /// ambiguous authorization, audience drift, and authorization bindings
    /// that do not match the exact plan snapshot.
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
        let human_authorized = !self.approvals.is_empty();
        let autonomous_authorized = self.autonomy_grant.is_some();
        let authorization_is_exclusive = human_authorized ^ autonomous_authorized;
        let plan_status_is_valid = if autonomous_authorized {
            matches!(self.plan.status, PlanStatus::ReadyForApproval | PlanStatus::Approved)
        } else {
            self.plan.status == PlanStatus::Approved
        };
        if !authorization_is_exclusive
            || !plan_status_is_valid
            || self.plan.tenant_id != self.tenant_id
            || self.plan.cluster_id != self.cluster_id
            || self.plan.expires_at <= now
        {
            return Err(ContractError::InvalidDescriptor {
                reason: "execution request requires exactly one current, same-scope authorization".to_owned(),
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
        if let Some(grant) = &self.autonomy_grant
            && (grant.issuer.trim().is_empty()
                || grant.audience != expected_audience
                || grant.plan_id != self.plan.id
                || grant.plan_hash != self.plan.plan_hash
                || !is_sha256_digest(&grant.plan_hash)
                || grant.diagnosis_revision_id != self.plan.diagnosis_revision
                || grant.tenant_id != self.tenant_id
                || grant.cluster_id != self.cluster_id
                || grant.policy_definition_version == 0
                || grant.lifecycle_revision == 0
                || !is_sha256_digest(&grant.autonomous_cohort_hash)
                || grant.primary_model_invocation_id != self.plan.primary_model_invocation_id
                || grant.primary_model_invocation_id == grant.critic_model_invocation_id
                || grant.nonce.trim().is_empty()
                || grant.signature.trim().is_empty()
                || grant.issued_at > now
                || grant.expires_at <= now
                || grant.expires_at <= grant.issued_at
                || self
                    .plan
                    .steps
                    .iter()
                    .any(|step| step.action != grant.action || step.descriptor_version != grant.action_version))
        {
            return Err(ContractError::InvalidDescriptor {
                reason: "autonomy grant does not bind the current R1 plan scope".to_owned(),
            });
        }
        Ok(())
    }

    /// Returns whether this request uses short-lived autonomy authorization.
    #[must_use]
    pub const fn is_autonomous(&self) -> bool {
        self.autonomy_grant.is_some()
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dynamic_safety: Option<DynamicSafetyDecision>,
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
    use chrono::Duration;
    use serde_json::json;

    use super::*;
    use crate::ActionPlanDraft;
    use crate::AutonomyCohortId;
    use crate::AutonomyPolicyId;
    use crate::CompensationMode;
    use crate::CompensationSpec;
    use crate::CriticReviewId;
    use crate::DiagnosisRevisionId;
    use crate::EvidenceId;
    use crate::ImpactScope;
    use crate::ModelInvocationId;
    use crate::PlanStepId;
    use crate::VerificationSpec;

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

    #[test]
    fn autonomous_request_requires_an_exclusive_exact_grant() {
        let now = Utc::now();
        let plan = ActionPlan::seal(ActionPlanDraft {
            id: crate::ActionPlanId::new(),
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            incident_id: crate::IncidentId::new(),
            diagnosis_revision: DiagnosisRevisionId::new(),
            primary_model_invocation_id: ModelInvocationId::new(),
            diagnosis_execution_eligible: true,
            version: 1,
            created_by: "autonomy-orchestrator".to_owned(),
            created_at: now - Duration::minutes(1),
            expires_at: now + Duration::minutes(10),
            evidence_hash: digest('a'),
            steps: vec![PlanStep {
                id: PlanStepId::new(),
                sequence: 1,
                action: ExecutionAction::ObservabilityLoggerLevelTtl,
                descriptor_version: "1.0.0".to_owned(),
                resource: "broker/broker-a".to_owned(),
                parameters: json!({"logger": "rocketmq", "level": "debug", "ttl_seconds": 60}),
                evidence_ids: vec![EvidenceId::new()],
                precondition_hash: digest('b'),
                max_impact: ImpactScope::SingleInstance,
                verification: VerificationSpec {
                    resource_conditions: vec!["logger_level_applied".to_owned()],
                    technical_slis: vec!["broker_up".to_owned()],
                    stable_window_seconds: 30,
                    max_wait_seconds: 120,
                },
                compensation: CompensationSpec {
                    mode: CompensationMode::Automatic,
                    required_before_fields: vec!["level".to_owned()],
                    timeout_seconds: 60,
                },
            }],
        })
        .expect("plan")
        .submit_for_review(now, false)
        .expect("ready plan");
        let grant = AutonomyGrant {
            issuer: "rocketmq-sre-control-plane".to_owned(),
            audience: "rocketmq-sre-executor".to_owned(),
            plan_id: plan.id,
            plan_hash: plan.plan_hash.clone(),
            diagnosis_revision_id: plan.diagnosis_revision,
            tenant_id: plan.tenant_id,
            cluster_id: plan.cluster_id,
            action: ExecutionAction::ObservabilityLoggerLevelTtl,
            action_version: "1.0.0".to_owned(),
            policy_id: AutonomyPolicyId::new(),
            policy_definition_version: 2,
            lifecycle_revision: 4,
            autonomous_cohort_id: AutonomyCohortId::new(),
            autonomous_cohort_hash: digest('c'),
            critic_review_id: CriticReviewId::new(),
            primary_model_invocation_id: plan.primary_model_invocation_id,
            critic_model_invocation_id: ModelInvocationId::new(),
            issued_at: now - Duration::seconds(1),
            expires_at: now + Duration::seconds(30),
            nonce: "autonomy-grant-nonce".to_owned(),
            signature: "hmac-sha256:test".to_owned(),
        };
        let mut request = ExecutionRequest {
            schema_version: ExecutionRequest::SCHEMA_VERSION.to_owned(),
            id: ExecutionId::new(),
            tenant_id: plan.tenant_id,
            cluster_id: plan.cluster_id,
            correlation_id: CorrelationId::new(),
            plan,
            approvals: Vec::new(),
            autonomy_grant: Some(grant),
            requested_by: "autonomy-orchestrator".to_owned(),
            idempotency_key: "autonomy-execution-1".to_owned(),
            issuer: "rocketmq-sre-control-plane".to_owned(),
            audience: "rocketmq-sre-executor".to_owned(),
            issued_at: now - Duration::seconds(1),
            expires_at: now + Duration::seconds(30),
            nonce: "execution-request-nonce".to_owned(),
            signature: "hmac-sha256:test".to_owned(),
        };

        assert!(request.validate_at(now, "rocketmq-sre-executor").is_ok());
        request.approvals.push(ApprovalGrant {
            issuer: "rocketmq-sre-control-plane".to_owned(),
            audience: "rocketmq-sre-executor".to_owned(),
            approval_id: crate::ApprovalId::new(),
            plan_id: request.plan.id,
            plan_hash: request.plan.plan_hash.clone(),
            precondition_hash: request.plan.compute_precondition_hash().expect("precondition hash"),
            tenant_id: request.tenant_id,
            cluster_id: request.cluster_id,
            approver_subject: "operator-a".to_owned(),
            issued_at: now - Duration::seconds(1),
            expires_at: now + Duration::seconds(30),
            nonce: "approval-nonce".to_owned(),
            signature: "hmac-sha256:test".to_owned(),
        });
        assert!(request.validate_at(now, "rocketmq-sre-executor").is_err());
        request.approvals.clear();
        request.autonomy_grant.as_mut().expect("grant").action_version = "2.0.0".to_owned();
        assert!(request.validate_at(now, "rocketmq-sre-executor").is_err());
    }

    fn digest(value: char) -> String {
        format!("sha256:{}", value.to_string().repeat(64))
    }
}
