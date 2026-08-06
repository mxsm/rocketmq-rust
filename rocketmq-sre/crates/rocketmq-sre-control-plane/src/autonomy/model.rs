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
use rocketmq_sre_contracts::ActionPlanId;
use rocketmq_sre_contracts::AutonomousExecutionFailure;
use rocketmq_sre_contracts::AutonomyCohortId;
use rocketmq_sre_contracts::AutonomyLifecycleState;
use rocketmq_sre_contracts::AutonomyMode;
use rocketmq_sre_contracts::AutonomyOutcome;
use rocketmq_sre_contracts::AutonomyOutcomeClass;
use rocketmq_sre_contracts::AutonomyPolicyDefinition;
use rocketmq_sre_contracts::AutonomyQualificationCohort;
use rocketmq_sre_contracts::AutonomySampleKind;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::CriticReviewId;
use rocketmq_sre_contracts::DiagnosisRevisionId;
use rocketmq_sre_contracts::EligibilityDecision;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::ModelInvocationId;
use serde::Deserialize;
use serde::Serialize;

/// Operator request for a new immutable autonomy policy version.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateAutonomyPolicyRequest {
    pub(crate) cluster_id: ClusterId,
    pub(crate) action: ExecutionAction,
    pub(crate) action_version: String,
    pub(crate) descriptor_digest: String,
    pub(crate) diagnostic_pack_id: String,
    pub(crate) diagnostic_pack_version: String,
    pub(crate) owner: String,
    pub(crate) minimum_evidence_freshness_seconds: u64,
    pub(crate) required_evidence_sources: Vec<String>,
    pub(crate) min_shadow_samples: u32,
    pub(crate) min_supervised_successes: u32,
    pub(crate) observation_window_days: u16,
    pub(crate) max_unresolved_unknown: u32,
    pub(crate) max_recent_rollbacks: u32,
    pub(crate) max_executions_per_hour: u16,
    pub(crate) cooldown_seconds: u64,
    pub(crate) max_concurrent_executions: u16,
    pub(crate) stable_window_seconds: u64,
}

/// Human lifecycle change. Autonomous promotion additionally requires the
/// owner-confirmed flag, an opaque approval reference, and current
/// qualification.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutonomyTransitionRequest {
    pub(crate) target_mode: AutonomyMode,
    pub(crate) reason: Option<String>,
    #[serde(default)]
    pub(crate) owner_confirmed: bool,
    #[serde(default)]
    pub(crate) owner_approval_ref: Option<String>,
}

impl AutonomyTransitionRequest {
    pub(crate) fn validated_owner_approval_ref(&self) -> Option<&str> {
        let value = self.owner_approval_ref.as_deref()?;
        if value.trim() != value || value.chars().count() > 160 {
            return None;
        }
        let reference = value.strip_prefix("approval://")?;
        let boundary_valid = reference
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
            && reference
                .chars()
                .next_back()
                .is_some_and(|character| character.is_ascii_lowercase() || character.is_ascii_digit());
        if reference.chars().count() < 3
            || !boundary_valid
            || reference.contains("..")
            || reference.contains("//")
            || !reference.chars().all(|character| {
                character.is_ascii_lowercase()
                    || character.is_ascii_digit()
                    || matches!(character, '.' | '-' | '_' | '/')
            })
        {
            return None;
        }
        Some(value)
    }
}

/// Query one action/cluster scope.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutonomyScopeQuery {
    pub(crate) cluster_id: ClusterId,
    pub(crate) action: ExecutionAction,
    #[serde(default = "default_action_version")]
    pub(crate) action_version: String,
}

/// Query bounded autonomy scopes for one cluster.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutonomyListQuery {
    pub(crate) cluster_id: ClusterId,
    #[serde(default = "default_limit")]
    pub(crate) limit: u16,
}

/// Operator-managed freeze request. Omitted action means cluster-wide.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SetAutonomyFreezeRequest {
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) action: Option<ExecutionAction>,
    pub(crate) action_version: Option<String>,
    pub(crate) active: bool,
    pub(crate) reason: String,
    pub(crate) starts_at: DateTime<Utc>,
    pub(crate) expires_at: Option<DateTime<Utc>>,
}

/// Operator-managed single-action kill-switch request.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SetAutonomyKillSwitchRequest {
    pub(crate) cluster_id: ClusterId,
    pub(crate) action: ExecutionAction,
    #[serde(default = "default_action_version")]
    pub(crate) action_version: String,
    pub(crate) active: bool,
    pub(crate) reason: String,
}

/// Actual primary-model identity used to create one Shadow cohort.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateShadowCohortRequest {
    pub(crate) cluster_id: ClusterId,
    pub(crate) action: ExecutionAction,
    #[serde(default = "default_action_version")]
    pub(crate) action_version: String,
    pub(crate) primary_profile: String,
    pub(crate) primary_model_family: String,
    pub(crate) primary_model_revision: String,
}

/// Persisted Critic and actual model identities used for an Autonomous cohort.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PrepareAutonomousCohortRequest {
    pub(crate) cluster_id: ClusterId,
    pub(crate) action: ExecutionAction,
    #[serde(default = "default_action_version")]
    pub(crate) action_version: String,
    pub(crate) diagnosis_revision_id: DiagnosisRevisionId,
    pub(crate) plan_id: ActionPlanId,
    pub(crate) plan_hash: String,
    pub(crate) critic_review_id: CriticReviewId,
    pub(crate) primary_model_invocation_id: ModelInvocationId,
    pub(crate) critic_model_invocation_id: ModelInvocationId,
    pub(crate) primary_profile: String,
    pub(crate) primary_model_family: String,
    pub(crate) primary_model_revision: String,
    pub(crate) critic_profile: String,
    pub(crate) critic_model_family: String,
    pub(crate) critic_model_revision: String,
}

/// Reconciled qualification result. The server derives `qualified`; callers
/// cannot directly increment counters.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecordQualificationSampleRequest {
    pub(crate) cluster_id: ClusterId,
    pub(crate) action: ExecutionAction,
    #[serde(default = "default_action_version")]
    pub(crate) action_version: String,
    pub(crate) cohort_id: AutonomyCohortId,
    pub(crate) kind: AutonomySampleKind,
    pub(crate) incident_id: IncidentId,
    pub(crate) plan_id: ActionPlanId,
    pub(crate) plan_hash: String,
    pub(crate) execution_id: Option<ExecutionId>,
    #[serde(default)]
    pub(crate) reason_codes: Vec<String>,
    pub(crate) human_outcome_linked: bool,
    pub(crate) evidence_complete: bool,
    pub(crate) stable_window_passed: bool,
    #[serde(default)]
    pub(crate) offline_replay: bool,
    #[serde(default)]
    pub(crate) debug_only: bool,
    pub(crate) observed_at: DateTime<Utc>,
    pub(crate) reconciled_at: DateTime<Utc>,
}

/// Authoritative facts derived from the persisted supervised execution,
/// verification journal, and immutable Critic record.
#[derive(Clone, Debug)]
pub(crate) struct SupervisedExecutionQualificationFacts {
    pub(crate) succeeded: bool,
    pub(crate) human_approved: bool,
    pub(crate) timeline_safe: bool,
    pub(crate) evidence_complete: bool,
    pub(crate) stable_window_passed: bool,
    pub(crate) observed_at: DateTime<Utc>,
    pub(crate) primary_profile: Option<String>,
    pub(crate) primary_model_family: Option<String>,
    pub(crate) primary_model_revision: Option<String>,
    pub(crate) critic_profile: Option<String>,
    pub(crate) critic_model_family: Option<String>,
    pub(crate) critic_model_revision: Option<String>,
}

/// Side-effect-free Shadow candidate. It can create a report and reconciled
/// qualification sample, but never an ExecutionRequest.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecordShadowOutcomeRequest {
    pub(crate) cluster_id: ClusterId,
    pub(crate) action: ExecutionAction,
    #[serde(default = "default_action_version")]
    pub(crate) action_version: String,
    pub(crate) incident_id: IncidentId,
    pub(crate) diagnosis_revision_id: DiagnosisRevisionId,
    pub(crate) plan_id: ActionPlanId,
    pub(crate) plan_hash: String,
    pub(crate) cohort_id: AutonomyCohortId,
    pub(crate) expected_effect: serde_json::Value,
    #[serde(default)]
    pub(crate) evidence_ids: Vec<EvidenceId>,
    pub(crate) human_outcome: Option<serde_json::Value>,
    pub(crate) stable_window: Option<serde_json::Value>,
    #[serde(default)]
    pub(crate) offline_replay: bool,
    #[serde(default)]
    pub(crate) debug_only: bool,
    pub(crate) observed_at: DateTime<Utc>,
    pub(crate) reconciled_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ShadowOutcomeListQuery {
    pub(crate) cluster_id: ClusterId,
    pub(crate) action: ExecutionAction,
    #[serde(default = "default_action_version")]
    pub(crate) action_version: String,
    #[serde(default = "default_limit")]
    pub(crate) limit: u16,
}

/// Reconciled Executor result. Classification is a closed enum and is
/// revalidated before the transactional outcome/pause write.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecordAutonomyOutcomeRequest {
    pub(crate) cluster_id: ClusterId,
    pub(crate) action: ExecutionAction,
    #[serde(default = "default_action_version")]
    pub(crate) action_version: String,
    pub(crate) incident_id: IncidentId,
    pub(crate) plan_id: ActionPlanId,
    pub(crate) plan_hash: String,
    pub(crate) execution_id: Option<ExecutionId>,
    pub(crate) cohort_id: Option<AutonomyCohortId>,
    pub(crate) class: AutonomyOutcomeClass,
    pub(crate) failure: Option<AutonomousExecutionFailure>,
    #[serde(default)]
    pub(crate) reason_codes: Vec<String>,
    pub(crate) first_positive_intent_persisted: bool,
    pub(crate) occurred_at: DateTime<Utc>,
    pub(crate) reconciled_at: DateTime<Utc>,
}

/// Internal candidate request after a valid immutable Critic review.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IssueAutonomyGrantRequest {
    pub(crate) cluster_id: ClusterId,
    pub(crate) action: ExecutionAction,
    pub(crate) action_version: String,
    pub(crate) incident_id: IncidentId,
    pub(crate) diagnosis_revision_id: DiagnosisRevisionId,
    pub(crate) plan_id: ActionPlanId,
    pub(crate) plan_hash: String,
    pub(crate) critic_review_id: CriticReviewId,
    pub(crate) primary_model_invocation_id: ModelInvocationId,
    pub(crate) critic_model_invocation_id: ModelInvocationId,
    pub(crate) primary_profile: String,
    pub(crate) primary_model_family: String,
    pub(crate) primary_model_revision: String,
    pub(crate) critic_profile: String,
    pub(crate) critic_model_family: String,
    pub(crate) critic_model_revision: String,
}

/// Internal request that issues a fresh grant and seals the exact autonomous
/// execution envelope consumed by Change Executor.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PrepareAutonomousExecutionRequest {
    pub(crate) grant: IssueAutonomyGrantRequest,
    pub(crate) correlation_id: CorrelationId,
    pub(crate) idempotency_key: String,
}

/// Current independent freeze state.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct AutonomyFreezeView {
    pub(crate) id: uuid::Uuid,
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) action: Option<ExecutionAction>,
    pub(crate) action_version: Option<String>,
    pub(crate) revision: u64,
    pub(crate) active: bool,
    pub(crate) reason: String,
    pub(crate) starts_at: DateTime<Utc>,
    pub(crate) expires_at: Option<DateTime<Utc>>,
    pub(crate) updated_by: String,
    pub(crate) updated_at: DateTime<Utc>,
}

/// Current independent kill-switch state.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct AutonomyKillSwitchView {
    pub(crate) cluster_id: ClusterId,
    pub(crate) action: ExecutionAction,
    pub(crate) action_version: String,
    pub(crate) revision: u64,
    pub(crate) active: bool,
    pub(crate) reason: String,
    pub(crate) updated_by: String,
    pub(crate) updated_at: DateTime<Utc>,
}

/// Qualification counters for one exact current cohort.
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct AutonomyQualificationView {
    pub(crate) shadow_cohort: Option<AutonomyQualificationCohort>,
    pub(crate) autonomous_cohort: Option<AutonomyQualificationCohort>,
    pub(crate) qualified_shadow_samples: u32,
    pub(crate) unqualified_shadow_samples: u32,
    pub(crate) qualified_supervised_successes: u32,
    pub(crate) unresolved_unknown: u32,
    pub(crate) recent_rollbacks: u32,
    pub(crate) shadow_observation_window_met: bool,
    pub(crate) autonomous_observation_window_met: bool,
}

/// Complete operator view for one action/cluster autonomy scope.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct AutonomyScopeView {
    pub(crate) schema_version: &'static str,
    pub(crate) policy: AutonomyPolicyDefinition,
    pub(crate) lifecycle: AutonomyLifecycleState,
    pub(crate) qualification: AutonomyQualificationView,
    pub(crate) active_freezes: Vec<AutonomyFreezeView>,
    pub(crate) kill_switch: Option<AutonomyKillSwitchView>,
    pub(crate) recent_outcomes: Vec<AutonomyOutcome>,
    pub(crate) reason_codes: Vec<String>,
}

/// Bounded action/cluster list response.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct AutonomyScopePage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<AutonomyScopeView>,
    pub(crate) truncated: bool,
}

/// Shadow outcome projection used by reports without an execution request.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct ShadowOutcomeView {
    pub(crate) id: uuid::Uuid,
    pub(crate) cohort_id: AutonomyCohortId,
    pub(crate) incident_id: IncidentId,
    pub(crate) plan_id: ActionPlanId,
    pub(crate) plan_hash: String,
    pub(crate) qualified: bool,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) observed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct ShadowOutcomeRecord {
    pub(super) view: ShadowOutcomeView,
    pub(super) tenant_id: rocketmq_sre_contracts::TenantId,
    pub(super) cluster_id: ClusterId,
    pub(super) action: ExecutionAction,
    pub(super) action_version: String,
    pub(super) diagnosis_revision_id: DiagnosisRevisionId,
    pub(super) eligibility: EligibilityDecision,
    pub(super) expected_effect: serde_json::Value,
    pub(super) evidence_ids: Vec<EvidenceId>,
    pub(super) human_outcome: Option<serde_json::Value>,
    pub(super) stable_window: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ShadowOutcomePage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<ShadowOutcomeView>,
    pub(crate) truncated: bool,
}

pub(crate) fn default_action_version() -> String {
    "1.0.0".to_owned()
}

const fn default_limit() -> u16 {
    100
}
