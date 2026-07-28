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
use rocketmq_sre_contracts::AutonomyCohortId;
use rocketmq_sre_contracts::AutonomyLifecycleState;
use rocketmq_sre_contracts::AutonomyMode;
use rocketmq_sre_contracts::AutonomyOutcome;
use rocketmq_sre_contracts::AutonomyPolicyDefinition;
use rocketmq_sre_contracts::AutonomyQualificationCohort;
use rocketmq_sre_contracts::AutonomySampleKind;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CriticReviewId;
use rocketmq_sre_contracts::DiagnosisRevisionId;
use rocketmq_sre_contracts::DynamicSafetyDecision;
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
/// owner-confirmed flag and current qualification.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutonomyTransitionRequest {
    pub(crate) target_mode: AutonomyMode,
    pub(crate) reason: Option<String>,
    #[serde(default)]
    pub(crate) owner_confirmed: bool,
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

/// Internal request to evaluate and sign one positive StepIntent.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EvaluateDynamicSafetyRequest {
    pub(crate) cluster_id: ClusterId,
    pub(crate) action: ExecutionAction,
    pub(crate) action_version: String,
    pub(crate) plan_id: ActionPlanId,
    pub(crate) plan_hash: String,
    pub(crate) execution_id: ExecutionId,
    pub(crate) execution_step_id: rocketmq_sre_contracts::ExecutionStepId,
    pub(crate) policy_definition_version: u64,
    pub(crate) lifecycle_revision: u64,
    pub(crate) evidence_fresh: bool,
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

/// Internal response proving safety was evaluated from current state.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct DynamicSafetyView {
    pub(crate) decision: DynamicSafetyDecision,
    pub(crate) execution_id: ExecutionId,
    pub(crate) execution_step_id: rocketmq_sre_contracts::ExecutionStepId,
}

pub(crate) fn default_action_version() -> String {
    "1.0.0".to_owned()
}

const fn default_limit() -> u16 {
    100
}
