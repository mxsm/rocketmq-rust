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

use crate::ActionPlanId;
use crate::AutonomyCohortId;
use crate::AutonomyOutcomeId;
use crate::AutonomyPolicyId;
use crate::AutonomySampleId;
use crate::ClusterId;
use crate::ContractError;
use crate::CriticReviewId;
use crate::DiagnosisRevisionId;
use crate::DynamicSafetyDecisionId;
use crate::ExecutionAction;
use crate::ExecutionId;
use crate::IncidentId;
use crate::ModelInvocationId;
use crate::TenantId;
use crate::is_sha256_digest;

/// Wire version for Phase 4 bounded-autonomy contracts.
pub const AUTONOMY_SCHEMA_VERSION: &str = "rocketmq-sre.autonomy.v1";

/// Operator-controlled lifecycle mode for one exact action and cluster.
#[derive(Clone, Copy, Debug, Default, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutonomyMode {
    #[default]
    Disabled,
    Shadow,
    Supervised,
    Autonomous,
    Paused,
}

impl AutonomyMode {
    /// Returns whether the mode may be selected as a recovery target.
    #[must_use]
    pub const fn is_safe_recovery_target(self) -> bool {
        matches!(self, Self::Shadow | Self::Supervised)
    }
}

/// Versioned, immutable policy content for one action and cluster.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutonomyPolicyDefinition {
    pub id: AutonomyPolicyId,
    pub definition_version: u64,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub action: ExecutionAction,
    pub action_version: String,
    pub descriptor_digest: String,
    pub diagnostic_pack_id: String,
    pub diagnostic_pack_version: String,
    pub owner: String,
    pub minimum_evidence_freshness_seconds: u64,
    #[serde(default)]
    pub required_evidence_sources: Vec<String>,
    pub min_shadow_samples: u32,
    pub min_supervised_successes: u32,
    pub observation_window_days: u16,
    pub max_unresolved_unknown: u32,
    pub max_recent_rollbacks: u32,
    pub max_executions_per_hour: u16,
    pub cooldown_seconds: u64,
    pub max_concurrent_executions: u16,
    pub stable_window_seconds: u64,
    pub created_at: DateTime<Utc>,
}

impl AutonomyPolicyDefinition {
    /// Validates bounded policy content before persistence.
    ///
    /// # Errors
    ///
    /// Rejects zero versions or bounds, non-R1 action versions, invalid
    /// digests, duplicate evidence sources, and unbounded text.
    pub fn validate(&self) -> Result<(), ContractError> {
        const MAX_SOURCES: usize = 32;
        let sources = self
            .required_evidence_sources
            .iter()
            .map(|source| source.trim())
            .collect::<std::collections::BTreeSet<_>>();
        if self.definition_version == 0
            || self.action_version != "1.0.0"
            || !is_sha256_digest(&self.descriptor_digest)
            || self.owner.trim().is_empty()
            || self.owner.chars().count() > 128
            || self.diagnostic_pack_id.trim().is_empty()
            || self.diagnostic_pack_id.chars().count() > 128
            || self.diagnostic_pack_version.trim().is_empty()
            || self.diagnostic_pack_version.chars().count() > 32
            || self.minimum_evidence_freshness_seconds == 0
            || self.min_shadow_samples == 0
            || self.min_supervised_successes == 0
            || self.observation_window_days == 0
            || self.max_executions_per_hour == 0
            || self.cooldown_seconds == 0
            || self.max_concurrent_executions == 0
            || self.stable_window_seconds == 0
            || self.required_evidence_sources.is_empty()
            || self.required_evidence_sources.len() > MAX_SOURCES
            || sources.len() != self.required_evidence_sources.len()
            || sources
                .iter()
                .any(|source| source.is_empty() || source.chars().count() > 128)
        {
            return Err(ContractError::InvalidDescriptor {
                reason: "autonomy policy violates bounded version, threshold, or source rules".to_owned(),
            });
        }
        Ok(())
    }
}

/// Mutable lifecycle state kept separate from qualification policy content.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutonomyLifecycleState {
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub action: ExecutionAction,
    pub mode: AutonomyMode,
    pub previous_mode: Option<AutonomyMode>,
    pub owner: String,
    pub pause_reason: Option<String>,
    pub lifecycle_revision: u64,
    pub updated_by: String,
    pub updated_at: DateTime<Utc>,
}

impl AutonomyLifecycleState {
    /// Validates lifecycle invariants.
    ///
    /// # Errors
    ///
    /// Rejects missing revisions, invalid paused state, unsafe recovery targets,
    /// or unbounded operator-controlled text.
    pub fn validate(&self) -> Result<(), ContractError> {
        let paused_valid = match self.mode {
            AutonomyMode::Paused => {
                self.previous_mode.is_some_and(|mode| {
                    matches!(
                        mode,
                        AutonomyMode::Shadow | AutonomyMode::Supervised | AutonomyMode::Autonomous
                    )
                }) && self
                    .pause_reason
                    .as_ref()
                    .is_some_and(|reason| !reason.trim().is_empty())
            }
            _ => self.previous_mode.is_none() && self.pause_reason.is_none(),
        };
        if self.lifecycle_revision == 0
            || !paused_valid
            || self.owner.trim().is_empty()
            || self.owner.chars().count() > 128
            || self.updated_by.trim().is_empty()
            || self.updated_by.chars().count() > 256
            || self
                .pause_reason
                .as_ref()
                .is_some_and(|reason| reason.chars().count() > 512)
        {
            return Err(ContractError::InvalidDescriptor {
                reason: "autonomy lifecycle state violates revision, pause, or operator bounds".to_owned(),
            });
        }
        Ok(())
    }
}

/// Qualification level represented by a cohort.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutonomyQualificationLevel {
    Shadow,
    Autonomous,
}

/// Immutable key for a qualification cohort.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutonomyQualificationCohort {
    pub id: AutonomyCohortId,
    pub level: AutonomyQualificationLevel,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub action: ExecutionAction,
    pub action_version: String,
    pub policy_definition_version: u64,
    pub descriptor_digest: String,
    pub diagnostic_pack_id: String,
    pub diagnostic_pack_version: String,
    pub primary_actual_model_identity_hash: String,
    pub critic_actual_model_identity_hash: Option<String>,
    pub cohort_hash: String,
    pub created_at: DateTime<Utc>,
}

/// Source of an immutable qualification sample.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutonomySampleKind {
    ShadowOutcome,
    SupervisedSuccess,
}

/// Immutable sample retained even when it does not qualify.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutonomyQualificationSample {
    pub id: AutonomySampleId,
    pub cohort_id: AutonomyCohortId,
    pub kind: AutonomySampleKind,
    pub incident_id: IncidentId,
    pub plan_id: ActionPlanId,
    pub plan_hash: String,
    pub execution_id: Option<ExecutionId>,
    pub qualified: bool,
    #[serde(default)]
    pub reason_codes: Vec<String>,
    pub human_outcome_linked: bool,
    pub evidence_complete: bool,
    pub stable_window_passed: bool,
    pub observed_at: DateTime<Utc>,
    pub reconciled_at: DateTime<Utc>,
}

impl AutonomyQualificationSample {
    /// Validates the immutable qualification result before persistence.
    ///
    /// # Errors
    ///
    /// Rejects incomplete identity bindings, invalid deduplication shapes,
    /// unbounded reason codes, contradictory qualification facts, and
    /// reconciliation timestamps that move backwards.
    pub fn validate(&self) -> Result<(), ContractError> {
        const MAX_REASON_CODES: usize = 32;
        const MAX_REASON_CODE_CHARS: usize = 128;
        let reasons = self
            .reason_codes
            .iter()
            .map(|reason| reason.trim())
            .collect::<std::collections::BTreeSet<_>>();
        let execution_binding_valid = match self.kind {
            AutonomySampleKind::ShadowOutcome => self.execution_id.is_none(),
            AutonomySampleKind::SupervisedSuccess => self.execution_id.is_some(),
        };
        let facts_qualify = self.human_outcome_linked
            && self.evidence_complete
            && self.stable_window_passed
            && self.reason_codes.is_empty();
        if self.id.as_uuid().is_nil()
            || self.cohort_id.as_uuid().is_nil()
            || self.incident_id.as_uuid().is_nil()
            || self.plan_id.as_uuid().is_nil()
            || !is_sha256_digest(&self.plan_hash)
            || !execution_binding_valid
            || self.reason_codes.len() > MAX_REASON_CODES
            || reasons.len() != self.reason_codes.len()
            || reasons
                .iter()
                .any(|reason| reason.is_empty() || reason.chars().count() > MAX_REASON_CODE_CHARS)
            || self.qualified != facts_qualify
            || self.reconciled_at < self.observed_at
        {
            return Err(ContractError::InvalidDescriptor {
                reason: "autonomy qualification sample is incomplete or contradictory".to_owned(),
            });
        }
        Ok(())
    }
}

/// Eligibility phase used for explainable decisions.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EligibilityPhase {
    Base,
    Final,
}

/// Deterministic autonomy eligibility result.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EligibilityDecision {
    pub phase: EligibilityPhase,
    pub allowed: bool,
    #[serde(default)]
    pub reason_codes: Vec<String>,
    pub cohort_id: Option<AutonomyCohortId>,
    pub cohort_hash: Option<String>,
    pub qualified_samples: u32,
    pub required_samples: u32,
    pub evaluated_at: DateTime<Utc>,
}

/// Short-lived grant accepted by the Executor only for an exact R1 plan.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutonomyGrant {
    pub issuer: String,
    pub audience: String,
    pub plan_id: ActionPlanId,
    pub plan_hash: String,
    pub diagnosis_revision_id: DiagnosisRevisionId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub action: ExecutionAction,
    pub action_version: String,
    pub policy_id: AutonomyPolicyId,
    pub policy_definition_version: u64,
    pub lifecycle_revision: u64,
    pub autonomous_cohort_id: AutonomyCohortId,
    pub autonomous_cohort_hash: String,
    pub critic_review_id: CriticReviewId,
    pub primary_model_invocation_id: ModelInvocationId,
    pub critic_model_invocation_id: ModelInvocationId,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub nonce: String,
    pub signature: String,
}

/// Result of authoritative per-step safety evaluation.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicSafetyDecision {
    pub id: DynamicSafetyDecisionId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub action: ExecutionAction,
    pub action_version: String,
    pub plan_id: ActionPlanId,
    pub plan_hash: String,
    pub execution_id: ExecutionId,
    pub execution_step_id: crate::ExecutionStepId,
    pub policy_definition_version: u64,
    pub lifecycle_revision: u64,
    pub error_budget_available: bool,
    pub freeze_revision: u64,
    pub kill_switch_revision: u64,
    pub evidence_fresh: bool,
    pub allowed: bool,
    #[serde(default)]
    pub reason_codes: Vec<String>,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub nonce: String,
    pub signature: String,
}

impl DynamicSafetyDecision {
    /// Validates a positive decision immediately before one Agent dispatch.
    ///
    /// # Errors
    ///
    /// Rejects incomplete bindings, deny decisions, invalid validity windows,
    /// and decisions whose signed facts are not all safe.
    pub fn validate_allow_at(&self, now: DateTime<Utc>) -> Result<(), ContractError> {
        const MAX_REASON_CODES: usize = 32;
        let validity_seconds = self.expires_at.signed_duration_since(self.issued_at).num_seconds();
        if self.id.as_uuid().is_nil()
            || self.tenant_id.as_uuid().is_nil()
            || self.cluster_id.as_uuid().is_nil()
            || self.action_version.trim().is_empty()
            || self.plan_id.as_uuid().is_nil()
            || !is_sha256_digest(&self.plan_hash)
            || self.execution_id.as_uuid().is_nil()
            || self.execution_step_id.as_uuid().is_nil()
            || self.policy_definition_version == 0
            || self.lifecycle_revision == 0
            || !self.error_budget_available
            || !self.evidence_fresh
            || !self.allowed
            || !self.reason_codes.is_empty()
            || self.reason_codes.len() > MAX_REASON_CODES
            || self.issued_at > now
            || self.expires_at <= now
            || !(1..=30).contains(&validity_seconds)
            || self.nonce.trim().is_empty()
            || self.signature.trim().is_empty()
        {
            return Err(ContractError::InvalidDescriptor {
                reason: "dynamic safety allow decision is incomplete, stale, or unsafe".to_owned(),
            });
        }
        Ok(())
    }
}

/// Authoritative request evaluated before every autonomous positive step.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicSafetyEvaluationRequest {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub action: ExecutionAction,
    pub action_version: String,
    pub plan_id: ActionPlanId,
    pub plan_hash: String,
    pub execution_id: ExecutionId,
    pub execution_step_id: crate::ExecutionStepId,
    pub policy_definition_version: u64,
    pub lifecycle_revision: u64,
}

impl DynamicSafetyEvaluationRequest {
    /// Validates the exact step and autonomy-policy binding.
    ///
    /// # Errors
    ///
    /// Rejects unknown schemas, nil identifiers, invalid hashes, and missing
    /// policy or lifecycle versions.
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.schema_version != AUTONOMY_SCHEMA_VERSION {
            return Err(ContractError::UnsupportedSchemaFamily {
                actual: self.schema_version.clone(),
                supported: AUTONOMY_SCHEMA_VERSION.to_owned(),
            });
        }
        if self.tenant_id.as_uuid().is_nil()
            || self.cluster_id.as_uuid().is_nil()
            || self.action_version.trim().is_empty()
            || self.plan_id.as_uuid().is_nil()
            || !is_sha256_digest(&self.plan_hash)
            || self.execution_id.as_uuid().is_nil()
            || self.execution_step_id.as_uuid().is_nil()
            || self.policy_definition_version == 0
            || self.lifecycle_revision == 0
        {
            return Err(ContractError::InvalidDescriptor {
                reason: "dynamic safety evaluation scope is incomplete".to_owned(),
            });
        }
        Ok(())
    }
}

/// Agent-side online introspection request for one signed safety decision.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifyDynamicSafetyDecisionRequest {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub decision: DynamicSafetyDecision,
}

impl VerifyDynamicSafetyDecisionRequest {
    /// Validates the request before signature and live-state verification.
    ///
    /// # Errors
    ///
    /// Rejects incompatible schemas, tenant drift, and stale or deny
    /// decisions.
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ContractError> {
        if self.schema_version != AUTONOMY_SCHEMA_VERSION {
            return Err(ContractError::UnsupportedSchemaFamily {
                actual: self.schema_version.clone(),
                supported: AUTONOMY_SCHEMA_VERSION.to_owned(),
            });
        }
        if self.tenant_id != self.decision.tenant_id {
            return Err(ContractError::InvalidDescriptor {
                reason: "dynamic safety verification tenant does not match the decision".to_owned(),
            });
        }
        self.decision.validate_allow_at(now)
    }
}

/// Positive introspection response bound to the exact signed decision.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicSafetyVerification {
    pub schema_version: String,
    pub valid: bool,
    pub decision_id: DynamicSafetyDecisionId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub plan_id: ActionPlanId,
    pub execution_id: ExecutionId,
    pub execution_step_id: crate::ExecutionStepId,
    pub expires_at: DateTime<Utc>,
}

/// Classification of a bounded autonomy outcome.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutonomyOutcomeClass {
    ExpectedDeny,
    Success,
    AutonomousExecutionFailure,
}

/// Closed failure reasons that force an autonomous cohort into Paused.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutonomousExecutionFailure {
    ApplyFailed,
    VerificationFailed,
    UnknownEffect,
    CompensationStarted,
    RolledBack,
    Escalated,
    SafetyInvalidatedDuringExecution,
    OperatorStopped,
    CriticUnavailable,
    CriticInvalid,
    CriticConflict,
    EvidenceDegraded,
}

/// Durable terminal or denied outcome for one autonomy candidate.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutonomyOutcome {
    pub id: AutonomyOutcomeId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub action: ExecutionAction,
    pub action_version: String,
    pub incident_id: IncidentId,
    pub plan_id: ActionPlanId,
    pub plan_hash: String,
    pub execution_id: Option<ExecutionId>,
    pub cohort_id: Option<AutonomyCohortId>,
    pub class: AutonomyOutcomeClass,
    pub failure: Option<AutonomousExecutionFailure>,
    #[serde(default)]
    pub reason_codes: Vec<String>,
    pub first_positive_intent_persisted: bool,
    pub occurred_at: DateTime<Utc>,
    pub reconciled_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(value: char) -> String {
        format!("sha256:{}", value.to_string().repeat(64))
    }

    #[test]
    fn paused_lifecycle_requires_a_previous_mode_and_reason() {
        let mut state = AutonomyLifecycleState {
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            action: ExecutionAction::ObservabilityLoggerLevelTtl,
            mode: AutonomyMode::Paused,
            previous_mode: Some(AutonomyMode::Autonomous),
            owner: "observability".to_owned(),
            pause_reason: Some("verification_failed".to_owned()),
            lifecycle_revision: 2,
            updated_by: "operator@example.com".to_owned(),
            updated_at: Utc::now(),
        };
        assert!(state.validate().is_ok());

        state.previous_mode = None;
        assert!(state.validate().is_err());
    }

    #[test]
    fn policy_rejects_unbounded_or_duplicate_sources() {
        let mut policy = AutonomyPolicyDefinition {
            id: AutonomyPolicyId::new(),
            definition_version: 1,
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            action: ExecutionAction::ObservabilityLoggerLevelTtl,
            action_version: "1.0.0".to_owned(),
            descriptor_digest: digest('a'),
            diagnostic_pack_id: "broker-runtime".to_owned(),
            diagnostic_pack_version: "1.0.0".to_owned(),
            owner: "observability".to_owned(),
            minimum_evidence_freshness_seconds: 60,
            required_evidence_sources: vec!["prometheus".to_owned()],
            min_shadow_samples: 20,
            min_supervised_successes: 5,
            observation_window_days: 7,
            max_unresolved_unknown: 0,
            max_recent_rollbacks: 0,
            max_executions_per_hour: 2,
            cooldown_seconds: 900,
            max_concurrent_executions: 1,
            stable_window_seconds: 300,
            created_at: Utc::now(),
        };
        assert!(policy.validate().is_ok());

        policy.required_evidence_sources.push("prometheus".to_owned());
        assert!(policy.validate().is_err());
    }

    #[test]
    fn recovery_targets_never_include_autonomous() {
        assert!(AutonomyMode::Shadow.is_safe_recovery_target());
        assert!(AutonomyMode::Supervised.is_safe_recovery_target());
        assert!(!AutonomyMode::Autonomous.is_safe_recovery_target());
    }

    #[test]
    fn qualification_sample_requires_exact_deduplication_shape() {
        let now = Utc::now();
        let mut sample = AutonomyQualificationSample {
            id: AutonomySampleId::new(),
            cohort_id: AutonomyCohortId::new(),
            kind: AutonomySampleKind::SupervisedSuccess,
            incident_id: IncidentId::new(),
            plan_id: ActionPlanId::new(),
            plan_hash: digest('c'),
            execution_id: Some(ExecutionId::new()),
            qualified: true,
            reason_codes: Vec::new(),
            human_outcome_linked: true,
            evidence_complete: true,
            stable_window_passed: true,
            observed_at: now,
            reconciled_at: now,
        };
        assert!(sample.validate().is_ok());

        sample.execution_id = None;
        assert!(sample.validate().is_err());
        sample.execution_id = Some(ExecutionId::new());
        sample.qualified = false;
        assert!(sample.validate().is_err());
        sample.reason_codes = vec!["offline_replay".to_owned()];
        assert!(sample.validate().is_ok());
    }

    #[test]
    fn dynamic_safety_allow_is_short_lived_and_fully_bound() {
        let now = Utc::now();
        let mut decision = DynamicSafetyDecision {
            id: DynamicSafetyDecisionId::new(),
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            action: ExecutionAction::ObservabilityLoggerLevelTtl,
            action_version: "1.0.0".to_owned(),
            plan_id: ActionPlanId::new(),
            plan_hash: digest('d'),
            execution_id: ExecutionId::new(),
            execution_step_id: crate::ExecutionStepId::new(),
            policy_definition_version: 2,
            lifecycle_revision: 4,
            error_budget_available: true,
            freeze_revision: 0,
            kill_switch_revision: 3,
            evidence_fresh: true,
            allowed: true,
            reason_codes: Vec::new(),
            issued_at: now,
            expires_at: now + chrono::Duration::seconds(30),
            nonce: "dynamic-safety-test".to_owned(),
            signature: "signed-dynamic-safety-test".to_owned(),
        };
        assert!(decision.validate_allow_at(now).is_ok());

        decision.expires_at = now + chrono::Duration::seconds(31);
        assert!(decision.validate_allow_at(now).is_err());
        decision.expires_at = now + chrono::Duration::seconds(30);
        decision.allowed = false;
        decision.reason_codes = vec!["kill_switch_active".to_owned()];
        assert!(decision.validate_allow_at(now).is_err());
    }
}
