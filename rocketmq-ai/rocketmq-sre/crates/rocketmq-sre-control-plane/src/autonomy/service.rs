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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::AUTONOMY_SCHEMA_VERSION;
use rocketmq_sre_contracts::ActionDescriptor;
use rocketmq_sre_contracts::ActionRisk;
use rocketmq_sre_contracts::AutonomousExecutionFailure;
use rocketmq_sre_contracts::AutonomyGrant;
use rocketmq_sre_contracts::AutonomyMode;
use rocketmq_sre_contracts::AutonomyOutcome;
use rocketmq_sre_contracts::AutonomyOutcomeClass;
use rocketmq_sre_contracts::AutonomyOutcomeId;
use rocketmq_sre_contracts::AutonomyPolicyDefinition;
use rocketmq_sre_contracts::AutonomyPolicyId;
use rocketmq_sre_contracts::AutonomyQualificationCohort;
use rocketmq_sre_contracts::AutonomyQualificationLevel;
use rocketmq_sre_contracts::AutonomyQualificationSample;
use rocketmq_sre_contracts::AutonomySampleId;
use rocketmq_sre_contracts::AutonomySampleKind;
use rocketmq_sre_contracts::BurnRateSeverity;
use rocketmq_sre_contracts::DynamicSafetyDecision;
use rocketmq_sre_contracts::DynamicSafetyDecisionId;
use rocketmq_sre_contracts::DynamicSafetyEvaluationRequest;
use rocketmq_sre_contracts::DynamicSafetyVerification;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ExecutionRequest;
use rocketmq_sre_contracts::HealthDataQuality;
use rocketmq_sre_contracts::HealthStatus;
use rocketmq_sre_contracts::PlanStatus;
use rocketmq_sre_contracts::VerifyDynamicSafetyDecisionRequest;
use rocketmq_sre_contracts::canonical_sha256;
use rocketmq_sre_contracts::is_sha256_digest;
use rocketmq_sre_core::ActualModelIdentity;
use rocketmq_sre_core::AutonomyActor;
use rocketmq_sre_core::AutonomyCandidatePath;
use rocketmq_sre_core::AutonomyPolicy;
use rocketmq_sre_core::AutonomyStateMachine;
use rocketmq_sre_core::BaseEligibilityFacts;
use rocketmq_sre_core::DynamicSafetyFacts;
use rocketmq_sre_core::EMBEDDED_ACTION_DESCRIPTOR_YAMLS;
use rocketmq_sre_core::EligibilityEngine;
use rocketmq_sre_core::FinalEligibilityFacts;
use rocketmq_sre_core::PromotionQualification;

use super::model::AutonomyScopePage;
use super::model::AutonomyScopeQuery;
use super::model::AutonomyScopeView;
use super::model::AutonomyTransitionRequest;
use super::model::CreateAutonomyPolicyRequest;
use super::model::CreateShadowCohortRequest;
use super::model::IssueAutonomyGrantRequest;
use super::model::PrepareAutonomousCohortRequest;
use super::model::PrepareAutonomousExecutionRequest;
use super::model::RecordAutonomyOutcomeRequest;
use super::model::RecordQualificationSampleRequest;
use super::model::RecordShadowOutcomeRequest;
use super::model::SetAutonomyFreezeRequest;
use super::model::SetAutonomyKillSwitchRequest;
use super::model::ShadowOutcomeListQuery;
use super::model::ShadowOutcomePage;
use super::model::ShadowOutcomeRecord;
use super::model::ShadowOutcomeView;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::SupervisedRepository;
use crate::auth::AuthContext;
use crate::slo::SloService;
use crate::supervised_execution::signing::GrantSigner;

const AUTONOMY_GRANT_TTL: Duration = Duration::minutes(2);
const DYNAMIC_SAFETY_TTL: Duration = Duration::seconds(30);
const CONTROL_PLANE_ISSUER: &str = "rocketmq-sre-control-plane";
const EXECUTOR_AUDIENCE: &str = "rocketmq-sre-executor";
const MAX_SCOPE_PAGE: u16 = 200;
const MAX_REASON_CODES: usize = 32;

/// Deterministic bounded-autonomy facade. Model output is never treated as
/// authority for lifecycle, policy, qualification counters, or live safety.
#[derive(Clone)]
pub(crate) struct AutonomyService {
    repository: PostgresRepository,
    slo: SloService,
    descriptors: Arc<BTreeMap<ExecutionAction, ActionDescriptor>>,
    signer: GrantSigner,
    clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
}

impl AutonomyService {
    pub(crate) fn new(
        repository: PostgresRepository,
        slo: SloService,
        signing_key: &[u8],
    ) -> Result<Self, ControlPlaneError> {
        Self::new_with_clock(repository, slo, signing_key, Arc::new(Utc::now))
    }

    pub(crate) fn new_with_clock(
        repository: PostgresRepository,
        slo: SloService,
        signing_key: &[u8],
        clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
    ) -> Result<Self, ControlPlaneError> {
        let mut descriptors = BTreeMap::new();
        for yaml in EMBEDDED_ACTION_DESCRIPTOR_YAMLS {
            let descriptor: ActionDescriptor = serde_yaml::from_str(yaml).map_err(|error| {
                ControlPlaneError::configuration(format!("autonomy action descriptor is invalid: {error}"))
            })?;
            let action = ExecutionAction::from_id(&descriptor.id).ok_or_else(|| {
                ControlPlaneError::configuration("autonomy descriptor is outside the closed execution catalog")
            })?;
            if descriptors.insert(action, descriptor).is_some() {
                return Err(ControlPlaneError::configuration(
                    "autonomy descriptor catalog contains duplicate actions",
                ));
            }
        }
        Ok(Self {
            repository,
            slo,
            descriptors: Arc::new(descriptors),
            signer: GrantSigner::new(signing_key)?,
            clock,
        })
    }

    pub(crate) async fn create_policy(
        &self,
        auth: &AuthContext,
        request: &CreateAutonomyPolicyRequest,
    ) -> Result<AutonomyScopeView, ControlPlaneError> {
        require_human_operator(auth)?;
        require_cluster(auth, request.cluster_id)?;
        let descriptor = self.descriptor(request.action, &request.action_version)?;
        let digest = canonical_sha256(descriptor)
            .map_err(|error| ControlPlaneError::validation("invalid_descriptor_digest", error.to_string()))?;
        if request.descriptor_digest != digest || request.owner != descriptor.owner {
            return Err(ControlPlaneError::conflict_code(
                "descriptor_digest_mismatch",
                "autonomy policy must bind the current descriptor digest and owner",
            ));
        }
        let definition = AutonomyPolicyDefinition {
            id: AutonomyPolicyId::new(),
            definition_version: 1,
            tenant_id: auth.tenant_id,
            cluster_id: request.cluster_id,
            action: request.action,
            action_version: request.action_version.clone(),
            descriptor_digest: request.descriptor_digest.clone(),
            diagnostic_pack_id: request.diagnostic_pack_id.clone(),
            diagnostic_pack_version: request.diagnostic_pack_version.clone(),
            owner: request.owner.clone(),
            minimum_evidence_freshness_seconds: request.minimum_evidence_freshness_seconds,
            required_evidence_sources: request.required_evidence_sources.clone(),
            min_shadow_samples: request.min_shadow_samples,
            min_supervised_successes: request.min_supervised_successes,
            observation_window_days: request.observation_window_days,
            max_unresolved_unknown: request.max_unresolved_unknown,
            max_recent_rollbacks: request.max_recent_rollbacks,
            max_executions_per_hour: request.max_executions_per_hour,
            cooldown_seconds: request.cooldown_seconds,
            max_concurrent_executions: request.max_concurrent_executions,
            stable_window_seconds: request.stable_window_seconds,
            created_at: (self.clock)(),
        };
        AutonomyPolicy::validate(&definition, descriptor)
            .map_err(|error| ControlPlaneError::validation("invalid_autonomy_policy", error.to_string()))?;
        let (definition, _) = self.repository.store_autonomy_policy(definition, &auth.subject).await?;
        self.repository
            .autonomy_scope_at(
                auth.tenant_id,
                definition.cluster_id,
                definition.action,
                &definition.action_version,
                (self.clock)(),
            )
            .await
    }

    pub(crate) async fn scope(
        &self,
        auth: &AuthContext,
        query: &AutonomyScopeQuery,
    ) -> Result<AutonomyScopeView, ControlPlaneError> {
        require_cluster(auth, query.cluster_id)?;
        self.repository
            .autonomy_scope_at(
                auth.tenant_id,
                query.cluster_id,
                query.action,
                &query.action_version,
                (self.clock)(),
            )
            .await
    }

    pub(crate) async fn scopes(
        &self,
        auth: &AuthContext,
        cluster_id: rocketmq_sre_contracts::ClusterId,
        limit: u16,
    ) -> Result<AutonomyScopePage, ControlPlaneError> {
        require_cluster(auth, cluster_id)?;
        let limit = limit.clamp(1, MAX_SCOPE_PAGE);
        let mut items = self
            .repository
            .autonomy_scopes_at(auth.tenant_id, cluster_id, i64::from(limit) + 1, (self.clock)())
            .await?;
        let truncated = items.len() > usize::from(limit);
        items.truncate(usize::from(limit));
        Ok(AutonomyScopePage {
            schema_version: rocketmq_sre_contracts::AUTONOMY_SCHEMA_VERSION,
            items,
            truncated,
        })
    }

    pub(crate) async fn transition(
        &self,
        auth: &AuthContext,
        scope: &AutonomyScopeQuery,
        request: &AutonomyTransitionRequest,
    ) -> Result<AutonomyScopeView, ControlPlaneError> {
        require_human_operator(auth)?;
        let owner_approval_ref = transition_owner_approval_ref(request)?;
        let current = self.scope(auth, scope).await?;
        let qualification = PromotionQualification {
            shadow_qualified: current.qualification.shadow_observation_window_met
                && current.qualification.qualified_shadow_samples >= current.policy.min_shadow_samples,
            autonomous_qualified: current.qualification.autonomous_observation_window_met
                && current.qualification.qualified_supervised_successes >= current.policy.min_supervised_successes
                && current.qualification.unresolved_unknown <= current.policy.max_unresolved_unknown
                && current.qualification.recent_rollbacks <= current.policy.max_recent_rollbacks,
            critic_ready: current.qualification.autonomous_cohort.is_some(),
            owner_confirmed: request.owner_confirmed,
            owner_approval_ref_valid: owner_approval_ref.is_some(),
        };
        let next = AutonomyStateMachine::transition(
            &current.lifecycle,
            request.target_mode,
            AutonomyActor::HumanOperator,
            &auth.subject,
            request.reason.as_deref(),
            qualification,
            (self.clock)(),
        )
        .map_err(|error| ControlPlaneError::conflict_code("invalid_autonomy_transition", error.to_string()))?;
        self.repository
            .update_autonomy_lifecycle(
                &current.lifecycle,
                &next,
                &current.policy.action_version,
                request.owner_confirmed,
                owner_approval_ref,
                transition_reason(request.target_mode),
            )
            .await?;
        self.scope(auth, scope).await
    }

    pub(crate) async fn create_shadow_cohort(
        &self,
        auth: &AuthContext,
        request: &CreateShadowCohortRequest,
    ) -> Result<AutonomyQualificationCohort, ControlPlaneError> {
        require_automation_service_or_operator(auth)?;
        let scope = self
            .scope(
                auth,
                &AutonomyScopeQuery {
                    cluster_id: request.cluster_id,
                    action: request.action,
                    action_version: request.action_version.clone(),
                },
            )
            .await?;
        if !matches!(scope.lifecycle.mode, AutonomyMode::Shadow | AutonomyMode::Supervised) {
            return Err(ControlPlaneError::conflict_code(
                "invalid_autonomy_state",
                "Shadow cohort creation requires Shadow or Supervised mode",
            ));
        }
        let cohort = AutonomyPolicy::shadow_cohort(
            &scope.policy,
            &ActualModelIdentity {
                profile: request.primary_profile.clone(),
                model_family: request.primary_model_family.clone(),
                model_revision: request.primary_model_revision.clone(),
            },
            (self.clock)(),
        )
        .map_err(|error| ControlPlaneError::validation("invalid_model_identity", error.to_string()))?;
        self.repository.store_autonomy_cohort(scope.policy.id, &cohort).await
    }

    pub(crate) async fn prepare_autonomous_cohort(
        &self,
        auth: &AuthContext,
        request: &PrepareAutonomousCohortRequest,
    ) -> Result<AutonomyQualificationCohort, ControlPlaneError> {
        require_automation_service_or_operator(auth)?;
        let scope = self
            .scope(
                auth,
                &AutonomyScopeQuery {
                    cluster_id: request.cluster_id,
                    action: request.action,
                    action_version: request.action_version.clone(),
                },
            )
            .await?;
        if !matches!(
            scope.lifecycle.mode,
            AutonomyMode::Supervised | AutonomyMode::Autonomous
        ) {
            return Err(ControlPlaneError::conflict_code(
                "invalid_autonomy_state",
                "Autonomous cohort creation requires Supervised or Autonomous mode",
            ));
        }
        self.validate_plan_and_critic(
            auth,
            &scope,
            request.diagnosis_revision_id,
            request.plan_id,
            &request.plan_hash,
            request.critic_review_id,
            request.primary_model_invocation_id,
            request.critic_model_invocation_id,
            &request.primary_profile,
            &request.primary_model_family,
            &request.primary_model_revision,
            &request.critic_profile,
            &request.critic_model_family,
            &request.critic_model_revision,
        )
        .await?;
        let cohort = AutonomyPolicy::autonomous_cohort(
            &scope.policy,
            &ActualModelIdentity {
                profile: request.primary_profile.clone(),
                model_family: request.primary_model_family.clone(),
                model_revision: request.primary_model_revision.clone(),
            },
            &ActualModelIdentity {
                profile: request.critic_profile.clone(),
                model_family: request.critic_model_family.clone(),
                model_revision: request.critic_model_revision.clone(),
            },
            (self.clock)(),
        )
        .map_err(|error| ControlPlaneError::validation("invalid_model_identity", error.to_string()))?;
        self.repository.store_autonomy_cohort(scope.policy.id, &cohort).await
    }

    pub(crate) async fn record_qualification_sample(
        &self,
        auth: &AuthContext,
        request: &RecordQualificationSampleRequest,
    ) -> Result<AutonomyQualificationSample, ControlPlaneError> {
        require_automation_service_or_operator(auth)?;
        let scope = self
            .scope(
                auth,
                &AutonomyScopeQuery {
                    cluster_id: request.cluster_id,
                    action: request.action,
                    action_version: request.action_version.clone(),
                },
            )
            .await?;
        let cohort = self.repository.autonomy_cohort(request.cohort_id).await?;
        validate_current_cohort(&scope, &cohort, request.kind)?;
        let plan = self.repository.action_plan(request.plan_id).await?.plan;
        validate_plan_scope(&plan, &scope, request.plan_hash.as_str())?;
        if request.kind == AutonomySampleKind::SupervisedSuccess {
            let execution_id = request.execution_id.ok_or_else(|| {
                ControlPlaneError::validation(
                    "execution_missing",
                    "Supervised qualification requires one exact persisted execution",
                )
            })?;
            let facts = self
                .repository
                .supervised_execution_qualification(
                    auth.tenant_id,
                    request.cluster_id,
                    request.action,
                    request.incident_id,
                    request.plan_id,
                    &request.plan_hash,
                    execution_id,
                    scope.policy.stable_window_seconds,
                )
                .await?;
            let mut reason_codes = request.reason_codes.clone();
            add_reason(&mut reason_codes, !facts.succeeded, "execution_not_succeeded");
            add_reason(&mut reason_codes, !facts.human_approved, "human_approval_missing");
            add_reason(&mut reason_codes, !facts.timeline_safe, "unsafe_execution_timeline");
            add_reason(&mut reason_codes, !facts.evidence_complete, "evidence_incomplete");
            add_reason(
                &mut reason_codes,
                !facts.stable_window_passed,
                "stable_window_incomplete",
            );
            add_reason(&mut reason_codes, request.offline_replay, "offline_replay");
            add_reason(&mut reason_codes, request.debug_only, "debug_only");

            let primary_identity = actual_identity(
                facts.primary_profile,
                facts.primary_model_family,
                facts.primary_model_revision,
            );
            let critic_identity = actual_identity(
                facts.critic_profile,
                facts.critic_model_family,
                facts.critic_model_revision,
            );
            add_reason(
                &mut reason_codes,
                primary_identity.is_none() || critic_identity.is_none(),
                "critic_not_ready",
            );
            if let (Some(primary), Some(critic)) = (&primary_identity, &critic_identity) {
                let primary_hash = primary
                    .identity_hash()
                    .map_err(|error| ControlPlaneError::validation("invalid_model_identity", error.to_string()))?;
                let critic_hash = critic
                    .identity_hash()
                    .map_err(|error| ControlPlaneError::validation("invalid_model_identity", error.to_string()))?;
                add_reason(
                    &mut reason_codes,
                    primary.model_family.eq_ignore_ascii_case(&critic.model_family),
                    "critic_family_not_heterogeneous",
                );
                add_reason(
                    &mut reason_codes,
                    primary_hash != cohort.primary_actual_model_identity_hash,
                    "primary_identity_mismatch",
                );
                add_reason(
                    &mut reason_codes,
                    cohort.critic_actual_model_identity_hash.as_deref() != Some(critic_hash.as_str()),
                    "critic_identity_mismatch",
                );
            }
            validate_reason_codes(&reason_codes)?;
            let qualified = reason_codes.is_empty();
            let reconciled_at = (self.clock)();
            let sample = AutonomyQualificationSample {
                id: AutonomySampleId::new(),
                cohort_id: request.cohort_id,
                kind: request.kind,
                incident_id: request.incident_id,
                plan_id: request.plan_id,
                plan_hash: request.plan_hash.clone(),
                execution_id: Some(execution_id),
                qualified,
                reason_codes,
                human_outcome_linked: facts.human_approved,
                evidence_complete: facts.evidence_complete,
                stable_window_passed: facts.stable_window_passed,
                observed_at: facts.observed_at,
                reconciled_at,
            };
            return self.repository.store_qualification_sample(&sample).await;
        }
        let mut reason_codes = request.reason_codes.clone();
        let reconciled_window_valid = request.reconciled_at >= request.observed_at;
        add_reason(
            &mut reason_codes,
            !request.human_outcome_linked,
            "human_outcome_missing",
        );
        add_reason(&mut reason_codes, !request.evidence_complete, "evidence_incomplete");
        add_reason(
            &mut reason_codes,
            !request.stable_window_passed,
            "stable_window_incomplete",
        );
        add_reason(&mut reason_codes, request.offline_replay, "offline_replay");
        add_reason(&mut reason_codes, request.debug_only, "debug_only");
        add_reason(
            &mut reason_codes,
            !reconciled_window_valid,
            "invalid_reconciliation_window",
        );
        add_reason(
            &mut reason_codes,
            request.kind == AutonomySampleKind::SupervisedSuccess && request.execution_id.is_none(),
            "execution_missing",
        );
        validate_reason_codes(&reason_codes)?;
        let qualified = request.human_outcome_linked
            && request.evidence_complete
            && request.stable_window_passed
            && !request.offline_replay
            && !request.debug_only
            && reconciled_window_valid
            && (request.kind == AutonomySampleKind::ShadowOutcome || request.execution_id.is_some());
        let sample = AutonomyQualificationSample {
            id: AutonomySampleId::new(),
            cohort_id: request.cohort_id,
            kind: request.kind,
            incident_id: request.incident_id,
            plan_id: request.plan_id,
            plan_hash: request.plan_hash.clone(),
            execution_id: request.execution_id,
            qualified,
            reason_codes,
            human_outcome_linked: request.human_outcome_linked,
            evidence_complete: request.evidence_complete,
            stable_window_passed: request.stable_window_passed,
            observed_at: request.observed_at,
            reconciled_at: request.reconciled_at,
        };
        self.repository.store_qualification_sample(&sample).await
    }

    pub(crate) async fn record_shadow_outcome(
        &self,
        auth: &AuthContext,
        request: &RecordShadowOutcomeRequest,
    ) -> Result<ShadowOutcomeView, ControlPlaneError> {
        require_automation_service_or_operator(auth)?;
        let scope = self
            .scope(
                auth,
                &AutonomyScopeQuery {
                    cluster_id: request.cluster_id,
                    action: request.action,
                    action_version: request.action_version.clone(),
                },
            )
            .await?;
        if scope.lifecycle.mode != AutonomyMode::Shadow {
            return Err(ControlPlaneError::conflict_code(
                "invalid_autonomy_state",
                "Shadow runner records candidates only while the exact scope is in Shadow mode",
            ));
        }
        let descriptor = self.descriptor(request.action, &request.action_version)?;
        let cohort = self.repository.autonomy_cohort(request.cohort_id).await?;
        validate_current_cohort(&scope, &cohort, AutonomySampleKind::ShadowOutcome)?;
        let plan = self.repository.action_plan(request.plan_id).await?.plan;
        validate_plan_scope(&plan, &scope, &request.plan_hash)?;
        if plan.incident_id != request.incident_id
            || plan.diagnosis_revision != request.diagnosis_revision_id
            || !request.expected_effect.is_object()
            || serde_json::to_vec(&request.expected_effect)
                .map_err(|_| {
                    ControlPlaneError::validation("invalid_expected_effect", "Shadow expected effect is not valid JSON")
                })?
                .len()
                > 64 * 1024
            || request.evidence_ids.len() > 64
        {
            return Err(ControlPlaneError::validation(
                "invalid_shadow_outcome",
                "Shadow outcome plan, expected effect, or Evidence bounds are invalid",
            ));
        }
        let live = self.live_safety(auth, &scope).await;
        let mut eligibility = EligibilityEngine::evaluate_base(
            AutonomyCandidatePath::Shadow,
            &scope.policy,
            &scope.lifecycle,
            descriptor,
            &BaseEligibilityFacts {
                diagnosis_execution_eligible: plan.diagnosis_execution_eligible,
                rules_only: false,
                root_cause_confirmed: true,
                evidence_complete: !request.evidence_ids.is_empty(),
                evidence_fresh: live.authoritative,
                required_sources_present: !request.evidence_ids.is_empty(),
                frequency_available: true,
                cooldown_complete: true,
                concurrency_available: true,
                error_budget_available: live.error_budget_available,
                freeze_active: !scope.active_freezes.is_empty(),
                kill_switch_active: scope.kill_switch.as_ref().is_some_and(|state| state.active),
                authoritative_safety_available: live.authoritative,
            },
            (self.clock)(),
        );
        eligibility.cohort_id = Some(cohort.id);
        eligibility.cohort_hash = Some(cohort.cohort_hash.clone());
        let reconciled_window_valid = request.reconciled_at >= request.observed_at;
        let mut reason_codes = eligibility.reason_codes.clone();
        add_reason(
            &mut reason_codes,
            request.human_outcome.is_none(),
            "human_outcome_missing",
        );
        add_reason(
            &mut reason_codes,
            request.stable_window.is_none(),
            "stable_window_incomplete",
        );
        add_reason(&mut reason_codes, request.offline_replay, "offline_replay");
        add_reason(&mut reason_codes, request.debug_only, "debug_only");
        add_reason(
            &mut reason_codes,
            !reconciled_window_valid,
            "invalid_reconciliation_window",
        );
        validate_reason_codes(&reason_codes)?;
        let qualified = eligibility.allowed
            && request.human_outcome.is_some()
            && request.stable_window.is_some()
            && reconciled_window_valid
            && !request.offline_replay
            && !request.debug_only;
        let view = ShadowOutcomeView {
            id: uuid::Uuid::new_v4(),
            cohort_id: cohort.id,
            incident_id: request.incident_id,
            plan_id: request.plan_id,
            plan_hash: request.plan_hash.clone(),
            qualified,
            reason_codes: reason_codes.clone(),
            observed_at: request.observed_at,
        };
        let record = ShadowOutcomeRecord {
            view,
            tenant_id: auth.tenant_id,
            cluster_id: request.cluster_id,
            action: request.action,
            action_version: request.action_version.clone(),
            diagnosis_revision_id: request.diagnosis_revision_id,
            eligibility,
            expected_effect: request.expected_effect.clone(),
            evidence_ids: request.evidence_ids.clone(),
            human_outcome: request.human_outcome.clone(),
            stable_window: request.stable_window.clone(),
        };
        let sample = AutonomyQualificationSample {
            id: AutonomySampleId::new(),
            cohort_id: cohort.id,
            kind: AutonomySampleKind::ShadowOutcome,
            incident_id: request.incident_id,
            plan_id: request.plan_id,
            plan_hash: request.plan_hash.clone(),
            execution_id: None,
            qualified,
            reason_codes,
            human_outcome_linked: request.human_outcome.is_some(),
            evidence_complete: !request.evidence_ids.is_empty(),
            stable_window_passed: request.stable_window.is_some(),
            observed_at: request.observed_at,
            reconciled_at: request.reconciled_at,
        };
        self.repository.store_shadow_outcome(&record, &sample).await
    }

    pub(crate) async fn shadow_outcomes(
        &self,
        auth: &AuthContext,
        query: &ShadowOutcomeListQuery,
    ) -> Result<ShadowOutcomePage, ControlPlaneError> {
        require_cluster(auth, query.cluster_id)?;
        let limit = query.limit.clamp(1, MAX_SCOPE_PAGE);
        let mut items = self
            .repository
            .shadow_outcomes(
                auth.tenant_id,
                query.cluster_id,
                query.action,
                &query.action_version,
                i64::from(limit) + 1,
            )
            .await?;
        let truncated = items.len() > usize::from(limit);
        items.truncate(usize::from(limit));
        Ok(ShadowOutcomePage {
            schema_version: rocketmq_sre_contracts::AUTONOMY_SCHEMA_VERSION,
            items,
            truncated,
        })
    }

    pub(crate) async fn record_outcome(
        &self,
        auth: &AuthContext,
        request: &RecordAutonomyOutcomeRequest,
    ) -> Result<AutonomyOutcome, ControlPlaneError> {
        require_role(auth, "executor_service")?;
        let scope = self
            .scope(
                auth,
                &AutonomyScopeQuery {
                    cluster_id: request.cluster_id,
                    action: request.action,
                    action_version: request.action_version.clone(),
                },
            )
            .await?;
        let plan = self.repository.action_plan(request.plan_id).await?.plan;
        validate_plan_scope(&plan, &scope, &request.plan_hash)?;
        if plan.incident_id != request.incident_id {
            return Err(ControlPlaneError::forbidden(
                "autonomy_plan_binding_invalid",
                "autonomy outcome incident does not match the immutable plan",
            ));
        }
        if let Some(cohort_id) = request.cohort_id {
            let cohort = self.repository.autonomy_cohort(cohort_id).await?;
            if cohort.tenant_id != auth.tenant_id
                || cohort.cluster_id != request.cluster_id
                || cohort.action != request.action
                || cohort.action_version != request.action_version
                || cohort.level != AutonomyQualificationLevel::Autonomous
            {
                return Err(ControlPlaneError::forbidden(
                    "autonomy_cohort_mismatch",
                    "autonomy outcome cohort does not match the execution scope",
                ));
            }
        }
        validate_outcome_request(request)?;
        let outcome = AutonomyOutcome {
            id: AutonomyOutcomeId::new(),
            tenant_id: auth.tenant_id,
            cluster_id: request.cluster_id,
            action: request.action,
            action_version: request.action_version.clone(),
            incident_id: request.incident_id,
            plan_id: request.plan_id,
            plan_hash: request.plan_hash.clone(),
            execution_id: request.execution_id,
            cohort_id: request.cohort_id,
            class: request.class,
            failure: request.failure,
            reason_codes: request.reason_codes.clone(),
            first_positive_intent_persisted: request.first_positive_intent_persisted,
            occurred_at: request.occurred_at,
            reconciled_at: request.reconciled_at,
        };
        self.repository
            .record_autonomy_outcome(&outcome, "autonomy-pause-reconciler")
            .await?;
        Ok(outcome)
    }

    pub(crate) async fn set_freeze(
        &self,
        auth: &AuthContext,
        request: &SetAutonomyFreezeRequest,
    ) -> Result<super::model::AutonomyFreezeView, ControlPlaneError> {
        require_human_operator(auth)?;
        if let Some(cluster_id) = request.cluster_id {
            require_cluster(auth, cluster_id)?;
        }
        if request.cluster_id.is_none() && request.action.is_some()
            || request.action.is_some() != request.action_version.is_some()
            || request.reason.trim().is_empty()
            || request.reason.chars().count() > 512
            || request.expires_at.is_some_and(|expires| expires <= request.starts_at)
        {
            return Err(ControlPlaneError::validation(
                "invalid_autonomy_freeze",
                "freeze scope, reason, or validity window is invalid",
            ));
        }
        self.repository
            .set_autonomy_freeze(
                auth.tenant_id,
                request.cluster_id,
                request.action,
                request.action_version.as_deref(),
                request.active,
                request.reason.trim(),
                request.starts_at,
                request.expires_at,
                &auth.subject,
            )
            .await
    }

    pub(crate) async fn set_preventive_freeze(
        &self,
        auth: &AuthContext,
        cluster_id: rocketmq_sre_contracts::ClusterId,
        reason: &str,
    ) -> Result<super::model::AutonomyFreezeView, ControlPlaneError> {
        require_role(auth, "automation_service")?;
        require_cluster(auth, cluster_id)?;
        if reason.trim().is_empty() || reason.chars().count() > 512 {
            return Err(ControlPlaneError::validation(
                "invalid_preventive_freeze",
                "preventive freeze reason must be bounded plain text",
            ));
        }
        self.repository
            .set_autonomy_freeze(
                auth.tenant_id,
                Some(cluster_id),
                None,
                None,
                true,
                reason.trim(),
                (self.clock)(),
                None,
                &auth.subject,
            )
            .await
    }

    pub(crate) async fn set_kill_switch(
        &self,
        auth: &AuthContext,
        request: &SetAutonomyKillSwitchRequest,
    ) -> Result<super::model::AutonomyKillSwitchView, ControlPlaneError> {
        require_human_operator(auth)?;
        require_cluster(auth, request.cluster_id)?;
        self.descriptor(request.action, &request.action_version)?;
        if request.reason.trim().is_empty() || request.reason.chars().count() > 512 {
            return Err(ControlPlaneError::validation(
                "invalid_kill_switch",
                "kill-switch reason must be bounded plain text",
            ));
        }
        self.repository
            .set_autonomy_kill_switch(
                auth.tenant_id,
                request.cluster_id,
                request.action,
                &request.action_version,
                request.active,
                request.reason.trim(),
                &auth.subject,
            )
            .await
    }

    pub(crate) async fn evaluate_dynamic_safety(
        &self,
        auth: &AuthContext,
        request: &DynamicSafetyEvaluationRequest,
    ) -> Result<DynamicSafetyDecision, ControlPlaneError> {
        require_role(auth, "executor_service")?;
        request
            .validate()
            .map_err(|error| ControlPlaneError::validation("invalid_dynamic_safety_request", error.to_string()))?;
        if request.tenant_id != auth.tenant_id {
            return Err(ControlPlaneError::forbidden(
                "tenant_mismatch",
                "dynamic safety request belongs to another tenant",
            ));
        }
        let scope = self
            .scope(
                auth,
                &AutonomyScopeQuery {
                    cluster_id: request.cluster_id,
                    action: request.action,
                    action_version: request.action_version.clone(),
                },
            )
            .await?;
        let issued_at = (self.clock)();
        let current = self
            .current_dynamic_safety(auth, &scope, request.plan_id, &request.plan_hash, issued_at)
            .await?;
        let evaluation = EligibilityEngine::evaluate_dynamic_safety(DynamicSafetyFacts {
            authoritative_sources_available: current.live.authoritative,
            error_budget_available: current.live.error_budget_available,
            freeze_active: current.freeze_active,
            kill_switch_active: current.kill_switch_active,
            evidence_fresh: current.evidence_fresh,
            policy_definition_matches: scope.policy.definition_version == request.policy_definition_version,
            lifecycle_revision_matches: scope.lifecycle.lifecycle_revision == request.lifecycle_revision
                && scope.lifecycle.mode == AutonomyMode::Autonomous,
        });
        let mut decision = DynamicSafetyDecision {
            id: DynamicSafetyDecisionId::new(),
            tenant_id: auth.tenant_id,
            cluster_id: request.cluster_id,
            action: request.action,
            action_version: request.action_version.clone(),
            plan_id: request.plan_id,
            plan_hash: request.plan_hash.clone(),
            execution_id: request.execution_id,
            execution_step_id: request.execution_step_id,
            policy_definition_version: request.policy_definition_version,
            lifecycle_revision: request.lifecycle_revision,
            error_budget_available: current.live.error_budget_available,
            freeze_revision: current.freeze_revision,
            kill_switch_revision: current.kill_switch_revision,
            evidence_fresh: current.evidence_fresh,
            allowed: evaluation.allowed,
            reason_codes: evaluation.reason_codes.into_iter().map(str::to_owned).collect(),
            issued_at,
            expires_at: issued_at + DYNAMIC_SAFETY_TTL,
            nonce: uuid::Uuid::new_v4().to_string(),
            signature: String::new(),
        };
        self.signer.sign_dynamic_safety(&mut decision)?;
        self.signer.verify_dynamic_safety(&decision)?;
        self.repository.store_dynamic_safety_decision(&decision).await?;
        Ok(decision)
    }

    pub(crate) async fn verify_dynamic_safety(
        &self,
        auth: &AuthContext,
        request: &VerifyDynamicSafetyDecisionRequest,
    ) -> Result<DynamicSafetyVerification, ControlPlaneError> {
        require_role(auth, "execution_agent")?;
        let now = (self.clock)();
        request
            .validate_at(now)
            .map_err(|error| ControlPlaneError::validation("invalid_dynamic_safety_decision", error.to_string()))?;
        require_cluster(auth, request.decision.cluster_id)?;
        if request.tenant_id != auth.tenant_id {
            return Err(ControlPlaneError::forbidden(
                "tenant_mismatch",
                "dynamic safety decision belongs to another tenant",
            ));
        }
        self.signer.verify_dynamic_safety(&request.decision)?;
        let scope = self
            .scope(
                auth,
                &AutonomyScopeQuery {
                    cluster_id: request.decision.cluster_id,
                    action: request.decision.action,
                    action_version: request.decision.action_version.clone(),
                },
            )
            .await?;
        let current = self
            .current_dynamic_safety(auth, &scope, request.decision.plan_id, &request.decision.plan_hash, now)
            .await?;
        let evaluation = EligibilityEngine::evaluate_dynamic_safety(DynamicSafetyFacts {
            authoritative_sources_available: current.live.authoritative,
            error_budget_available: current.live.error_budget_available,
            freeze_active: current.freeze_active,
            kill_switch_active: current.kill_switch_active,
            evidence_fresh: current.evidence_fresh,
            policy_definition_matches: scope.policy.definition_version == request.decision.policy_definition_version,
            lifecycle_revision_matches: scope.lifecycle.lifecycle_revision == request.decision.lifecycle_revision
                && scope.lifecycle.mode == AutonomyMode::Autonomous,
        });
        let persisted = self
            .repository
            .dynamic_safety_decision_is_persisted(&request.decision)
            .await?;
        if !persisted
            || !evaluation.allowed
            || current.live.error_budget_available != request.decision.error_budget_available
            || current.evidence_fresh != request.decision.evidence_fresh
            || current.freeze_revision != request.decision.freeze_revision
            || current.kill_switch_revision != request.decision.kill_switch_revision
        {
            return Err(ControlPlaneError::forbidden(
                "dynamic_safety_stale",
                "dynamic safety decision no longer matches authoritative live state",
            ));
        }
        Ok(DynamicSafetyVerification {
            schema_version: AUTONOMY_SCHEMA_VERSION.to_owned(),
            valid: true,
            decision_id: request.decision.id,
            tenant_id: request.decision.tenant_id,
            cluster_id: request.decision.cluster_id,
            plan_id: request.decision.plan_id,
            execution_id: request.decision.execution_id,
            execution_step_id: request.decision.execution_step_id,
            expires_at: request.decision.expires_at,
        })
    }

    pub(crate) async fn issue_grant(
        &self,
        auth: &AuthContext,
        request: &IssueAutonomyGrantRequest,
    ) -> Result<AutonomyGrant, ControlPlaneError> {
        require_role(auth, "executor_service")?;
        let descriptor = self.descriptor(request.action, &request.action_version)?;
        let cohort_request = PrepareAutonomousCohortRequest {
            cluster_id: request.cluster_id,
            action: request.action,
            action_version: request.action_version.clone(),
            diagnosis_revision_id: request.diagnosis_revision_id,
            plan_id: request.plan_id,
            plan_hash: request.plan_hash.clone(),
            critic_review_id: request.critic_review_id,
            primary_model_invocation_id: request.primary_model_invocation_id,
            critic_model_invocation_id: request.critic_model_invocation_id,
            primary_profile: request.primary_profile.clone(),
            primary_model_family: request.primary_model_family.clone(),
            primary_model_revision: request.primary_model_revision.clone(),
            critic_profile: request.critic_profile.clone(),
            critic_model_family: request.critic_model_family.clone(),
            critic_model_revision: request.critic_model_revision.clone(),
        };
        let cohort = self.prepare_autonomous_cohort(auth, &cohort_request).await?;
        let refreshed = self
            .scope(
                auth,
                &AutonomyScopeQuery {
                    cluster_id: request.cluster_id,
                    action: request.action,
                    action_version: request.action_version.clone(),
                },
            )
            .await?;
        let live = self.live_safety(auth, &refreshed).await;
        let base = EligibilityEngine::evaluate_base(
            AutonomyCandidatePath::Autonomous,
            &refreshed.policy,
            &refreshed.lifecycle,
            descriptor,
            &BaseEligibilityFacts {
                diagnosis_execution_eligible: true,
                rules_only: false,
                root_cause_confirmed: true,
                evidence_complete: request.plan_hash.starts_with("sha256:"),
                evidence_fresh: live.authoritative,
                required_sources_present: live.authoritative,
                frequency_available: true,
                cooldown_complete: true,
                concurrency_available: true,
                error_budget_available: live.error_budget_available,
                freeze_active: !refreshed.active_freezes.is_empty(),
                kill_switch_active: refreshed.kill_switch.as_ref().is_some_and(|state| state.active),
                authoritative_safety_available: live.authoritative,
            },
            (self.clock)(),
        );
        let final_decision = EligibilityEngine::evaluate_final(
            &refreshed.policy,
            &refreshed.lifecycle,
            descriptor,
            &FinalEligibilityFacts {
                cohort_id: Some(cohort.id),
                cohort_hash: Some(cohort.cohort_hash.clone()),
                critic_review_valid: true,
                critic_is_heterogeneous: true,
                invocation_bindings_valid: true,
                owner_confirmed: true,
                observation_window_met: refreshed.qualification.autonomous_observation_window_met,
                supervised_successes: refreshed.qualification.qualified_supervised_successes,
                unresolved_unknown: refreshed.qualification.unresolved_unknown,
                recent_rollbacks: refreshed.qualification.recent_rollbacks,
            },
            (self.clock)(),
        );
        if !base.allowed || !final_decision.allowed {
            let reasons = base
                .reason_codes
                .into_iter()
                .chain(final_decision.reason_codes)
                .collect::<Vec<_>>()
                .join(",");
            return Err(ControlPlaneError::forbidden(
                "autonomy_not_eligible",
                format!("autonomy grant denied: {reasons}"),
            ));
        }
        let plan = self.repository.action_plan(request.plan_id).await?.plan;
        if plan.incident_id != request.incident_id {
            return Err(ControlPlaneError::forbidden(
                "autonomy_plan_binding_invalid",
                "autonomy incident does not match the immutable plan",
            ));
        }
        let issued_at = (self.clock)();
        let mut grant = AutonomyGrant {
            issuer: "rocketmq-sre-control-plane".to_owned(),
            audience: "rocketmq-sre-executor".to_owned(),
            plan_id: request.plan_id,
            plan_hash: request.plan_hash.clone(),
            diagnosis_revision_id: request.diagnosis_revision_id,
            tenant_id: auth.tenant_id,
            cluster_id: request.cluster_id,
            action: request.action,
            action_version: request.action_version.clone(),
            policy_id: refreshed.policy.id,
            policy_definition_version: refreshed.policy.definition_version,
            lifecycle_revision: refreshed.lifecycle.lifecycle_revision,
            autonomous_cohort_id: cohort.id,
            autonomous_cohort_hash: cohort.cohort_hash,
            critic_review_id: request.critic_review_id,
            primary_model_invocation_id: request.primary_model_invocation_id,
            critic_model_invocation_id: request.critic_model_invocation_id,
            issued_at,
            expires_at: issued_at + AUTONOMY_GRANT_TTL,
            nonce: uuid::Uuid::new_v4().to_string(),
            signature: String::new(),
        };
        self.signer.sign_autonomy(&mut grant)?;
        self.signer.verify_autonomy(&grant)?;
        Ok(grant)
    }

    pub(crate) async fn prepare_execution(
        &self,
        auth: &AuthContext,
        request: &PrepareAutonomousExecutionRequest,
    ) -> Result<ExecutionRequest, ControlPlaneError> {
        require_role(auth, "executor_service")?;
        validate_idempotency_key(&request.idempotency_key)?;
        let grant = self.issue_grant(auth, &request.grant).await?;
        let plan = self.repository.action_plan(grant.plan_id).await?.plan;
        let issued_at = (self.clock)();
        let expires_at = (issued_at + AUTONOMY_GRANT_TTL)
            .min(grant.expires_at)
            .min(plan.expires_at);
        let mut execution = ExecutionRequest {
            schema_version: ExecutionRequest::SCHEMA_VERSION.to_owned(),
            id: ExecutionId::new(),
            tenant_id: grant.tenant_id,
            cluster_id: grant.cluster_id,
            correlation_id: request.correlation_id,
            plan,
            approvals: Vec::new(),
            autonomy_grant: Some(grant),
            requested_by: auth.subject.clone(),
            idempotency_key: request.idempotency_key.clone(),
            issuer: CONTROL_PLANE_ISSUER.to_owned(),
            audience: EXECUTOR_AUDIENCE.to_owned(),
            issued_at,
            expires_at,
            nonce: uuid::Uuid::new_v4().to_string(),
            signature: String::new(),
        };
        self.signer.sign_execution(&mut execution)?;
        self.signer.verify_execution(&execution)?;
        execution
            .validate_at(issued_at, EXECUTOR_AUDIENCE)
            .map_err(|error| ControlPlaneError::validation("invalid_execution_request", error.to_string()))?;
        Ok(execution)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "Critic authority bindings are intentionally explicit"
    )]
    async fn validate_plan_and_critic(
        &self,
        auth: &AuthContext,
        scope: &AutonomyScopeView,
        diagnosis_revision_id: rocketmq_sre_contracts::DiagnosisRevisionId,
        plan_id: rocketmq_sre_contracts::ActionPlanId,
        plan_hash: &str,
        critic_review_id: rocketmq_sre_contracts::CriticReviewId,
        primary_invocation_id: rocketmq_sre_contracts::ModelInvocationId,
        critic_invocation_id: rocketmq_sre_contracts::ModelInvocationId,
        primary_profile: &str,
        primary_family: &str,
        primary_revision: &str,
        critic_profile: &str,
        critic_family: &str,
        critic_revision: &str,
    ) -> Result<(), ControlPlaneError> {
        let plan = self.repository.action_plan(plan_id).await?.plan;
        validate_plan_scope(&plan, scope, plan_hash)?;
        if plan.diagnosis_revision != diagnosis_revision_id
            || plan.primary_model_invocation_id != primary_invocation_id
            || !matches!(plan.status, PlanStatus::ReadyForApproval | PlanStatus::Approved)
        {
            return Err(ControlPlaneError::forbidden(
                "autonomy_plan_binding_invalid",
                "autonomy plan lifecycle or diagnosis binding is invalid",
            ));
        }
        let valid = self
            .repository
            .autonomy_critic_bindings_valid(
                auth.tenant_id,
                scope.policy.cluster_id,
                diagnosis_revision_id,
                plan_id,
                plan_hash,
                critic_review_id,
                primary_invocation_id,
                critic_invocation_id,
                primary_profile,
                primary_family,
                primary_revision,
                critic_profile,
                critic_family,
                critic_revision,
            )
            .await?;
        if !valid || primary_family.trim().eq_ignore_ascii_case(critic_family.trim()) {
            return Err(ControlPlaneError::forbidden(
                "critic_binding_invalid",
                "Critic review and actual heterogeneous model identities are not authoritative",
            ));
        }
        Ok(())
    }

    async fn live_safety(&self, auth: &AuthContext, scope: &AutonomyScopeView) -> LiveSafety {
        let Ok(report) = self.slo.cluster_report(auth, scope.policy.cluster_id).await else {
            return LiveSafety::default();
        };
        let fresh = (self.clock)()
            .signed_duration_since(report.observed_at)
            .to_std()
            .is_ok_and(|age| age <= std::time::Duration::from_secs(120));
        let authoritative = report.data_quality == HealthDataQuality::Complete
            && fresh
            && !matches!(report.status, HealthStatus::Critical | HealthStatus::Unknown);
        let critical_burn = report.slis.iter().flat_map(|sli| &sli.windows).any(|window| {
            window.triggered
                && window.severity == BurnRateSeverity::Critical
                && window.data_quality == HealthDataQuality::Complete
        });
        LiveSafety {
            authoritative,
            error_budget_available: authoritative && !critical_burn,
        }
    }

    async fn current_dynamic_safety(
        &self,
        auth: &AuthContext,
        scope: &AutonomyScopeView,
        plan_id: rocketmq_sre_contracts::ActionPlanId,
        plan_hash: &str,
        now: chrono::DateTime<Utc>,
    ) -> Result<CurrentDynamicSafety, ControlPlaneError> {
        let live = self.live_safety(auth, scope).await;
        let evidence_fresh = self
            .repository
            .plan_evidence_is_current(
                auth.tenant_id,
                scope.policy.cluster_id,
                plan_id,
                plan_hash,
                scope.policy.action,
                &scope.policy.action_version,
                scope.policy.minimum_evidence_freshness_seconds,
                &scope.policy.required_evidence_sources,
                now,
            )
            .await?;
        let (freeze_revision, freeze_active) = self
            .repository
            .autonomy_freeze_state(
                auth.tenant_id,
                scope.policy.cluster_id,
                scope.policy.action,
                &scope.policy.action_version,
                now,
            )
            .await?;
        Ok(CurrentDynamicSafety {
            live,
            evidence_fresh,
            freeze_revision,
            freeze_active,
            kill_switch_revision: scope.kill_switch.as_ref().map_or(0, |state| state.revision),
            kill_switch_active: scope.kill_switch.as_ref().is_some_and(|state| state.active),
        })
    }

    fn descriptor(
        &self,
        action: ExecutionAction,
        action_version: &str,
    ) -> Result<&ActionDescriptor, ControlPlaneError> {
        let descriptor = self.descriptors.get(&action).ok_or_else(|| {
            ControlPlaneError::validation("unknown_action", "action is outside the closed autonomy catalog")
        })?;
        if descriptor.version != action_version || descriptor.risk != ActionRisk::R1 || descriptor.plan_only {
            return Err(ControlPlaneError::forbidden(
                "r1_action_required",
                "bounded autonomy accepts only an exact, non-plan-only R1 descriptor",
            ));
        }
        Ok(descriptor)
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct LiveSafety {
    authoritative: bool,
    error_budget_available: bool,
}

#[derive(Clone, Copy, Debug)]
struct CurrentDynamicSafety {
    live: LiveSafety,
    evidence_fresh: bool,
    freeze_revision: u64,
    freeze_active: bool,
    kill_switch_revision: u64,
    kill_switch_active: bool,
}

fn validate_plan_scope(
    plan: &rocketmq_sre_contracts::ActionPlan,
    scope: &AutonomyScopeView,
    expected_hash: &str,
) -> Result<(), ControlPlaneError> {
    plan.verify_plan_hash()
        .map_err(|error| ControlPlaneError::validation("invalid_plan_hash", error.to_string()))?;
    if plan.tenant_id != scope.policy.tenant_id
        || plan.cluster_id != scope.policy.cluster_id
        || plan.plan_hash != expected_hash
        || !is_sha256_digest(expected_hash)
        || plan.steps.is_empty()
        || plan
            .steps
            .iter()
            .any(|step| step.action != scope.policy.action || step.descriptor_version != scope.policy.action_version)
    {
        return Err(ControlPlaneError::forbidden(
            "autonomy_plan_binding_invalid",
            "immutable plan is not bound to the current autonomy action scope",
        ));
    }
    Ok(())
}

fn validate_current_cohort(
    scope: &AutonomyScopeView,
    cohort: &AutonomyQualificationCohort,
    kind: AutonomySampleKind,
) -> Result<(), ControlPlaneError> {
    let level_matches = matches!(
        (cohort.level, kind),
        (AutonomyQualificationLevel::Shadow, AutonomySampleKind::ShadowOutcome)
            | (
                AutonomyQualificationLevel::Autonomous,
                AutonomySampleKind::SupervisedSuccess
            )
    );
    if !level_matches
        || cohort.tenant_id != scope.policy.tenant_id
        || cohort.cluster_id != scope.policy.cluster_id
        || cohort.action != scope.policy.action
        || cohort.action_version != scope.policy.action_version
        || cohort.policy_definition_version != scope.policy.definition_version
        || cohort.descriptor_digest != scope.policy.descriptor_digest
        || cohort.diagnostic_pack_id != scope.policy.diagnostic_pack_id
        || cohort.diagnostic_pack_version != scope.policy.diagnostic_pack_version
    {
        return Err(ControlPlaneError::forbidden(
            "stale_autonomy_cohort",
            "qualification sample does not belong to the current exact cohort",
        ));
    }
    Ok(())
}

fn add_reason(reasons: &mut Vec<String>, condition: bool, reason: &'static str) {
    if condition && !reasons.iter().any(|existing| existing == reason) {
        reasons.push(reason.to_owned());
    }
}

fn actual_identity(
    profile: Option<String>,
    model_family: Option<String>,
    model_revision: Option<String>,
) -> Option<ActualModelIdentity> {
    Some(ActualModelIdentity {
        profile: profile?,
        model_family: model_family?,
        model_revision: model_revision?,
    })
}

fn validate_idempotency_key(value: &str) -> Result<(), ControlPlaneError> {
    let length = value.chars().count();
    if !(16..=200).contains(&length)
        || value
            .chars()
            .any(|character| !(character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | ':' | '.')))
    {
        return Err(ControlPlaneError::validation(
            "invalid_idempotency_key",
            "idempotency key must contain 16 to 200 allowlisted ASCII characters",
        ));
    }
    Ok(())
}

fn validate_reason_codes(reason_codes: &[String]) -> Result<(), ControlPlaneError> {
    let unique = reason_codes.iter().collect::<BTreeSet<_>>();
    if reason_codes.len() > MAX_REASON_CODES
        || unique.len() != reason_codes.len()
        || reason_codes.iter().any(|reason| {
            reason.trim().is_empty() || reason.chars().count() > 128 || reason.chars().any(char::is_control)
        })
    {
        return Err(ControlPlaneError::validation(
            "invalid_reason_codes",
            "qualification reason codes must be unique bounded plain text",
        ));
    }
    Ok(())
}

fn validate_outcome_request(request: &RecordAutonomyOutcomeRequest) -> Result<(), ControlPlaneError> {
    validate_reason_codes(&request.reason_codes)?;
    let pre_intent_failure = matches!(
        request.failure,
        Some(
            AutonomousExecutionFailure::CriticUnavailable
                | AutonomousExecutionFailure::CriticInvalid
                | AutonomousExecutionFailure::CriticConflict
                | AutonomousExecutionFailure::EvidenceDegraded
        )
    );
    let valid_class = match request.class {
        AutonomyOutcomeClass::ExpectedDeny => {
            request.failure.is_none() && !request.first_positive_intent_persisted && request.execution_id.is_none()
        }
        AutonomyOutcomeClass::Success => {
            request.failure.is_none()
                && request.first_positive_intent_persisted
                && request.execution_id.is_some()
                && request.cohort_id.is_some()
        }
        AutonomyOutcomeClass::AutonomousExecutionFailure => {
            request.failure.is_some()
                && request.cohort_id.is_some()
                && (request.first_positive_intent_persisted || pre_intent_failure)
        }
    };
    if !valid_class || request.reconciled_at < request.occurred_at || !is_sha256_digest(&request.plan_hash) {
        return Err(ControlPlaneError::validation(
            "invalid_autonomy_outcome",
            "autonomy outcome class, intent, failure, or reconciliation invariants are invalid",
        ));
    }
    Ok(())
}

fn require_human_operator(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    require_role(auth, "operator")
}

fn require_automation_service_or_operator(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if auth.roles.contains("executor_service") || auth.roles.contains("operator") {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "autonomy_authority_required",
            "autonomy qualification requires an operator or automation service identity",
        ))
    }
}

fn require_role(auth: &AuthContext, role: &'static str) -> Result<(), ControlPlaneError> {
    if auth.roles.contains(role) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "autonomy_authority_required",
            format!("autonomy operation requires `{role}` authority"),
        ))
    }
}

fn require_cluster(auth: &AuthContext, cluster_id: rocketmq_sre_contracts::ClusterId) -> Result<(), ControlPlaneError> {
    if auth.clusters.contains(&cluster_id) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "autonomy cluster is outside the authenticated scope",
        ))
    }
}

const fn transition_reason(mode: AutonomyMode) -> &'static str {
    match mode {
        AutonomyMode::Disabled => "operator_disabled",
        AutonomyMode::Shadow => "operator_enabled_shadow",
        AutonomyMode::Supervised => "operator_enabled_supervised",
        AutonomyMode::Autonomous => "owner_confirmed_autonomous",
        AutonomyMode::Paused => "operator_paused",
    }
}

fn transition_owner_approval_ref(request: &AutonomyTransitionRequest) -> Result<Option<&str>, ControlPlaneError> {
    match (request.target_mode, request.owner_approval_ref.as_deref()) {
        (AutonomyMode::Autonomous, None) => Err(ControlPlaneError::validation(
            "owner_approval_ref_required",
            "Autonomous promotion requires an opaque action-owner approval reference",
        )),
        (AutonomyMode::Autonomous, Some(_)) => request.validated_owner_approval_ref().map(Some).ok_or_else(|| {
            ControlPlaneError::validation(
                "invalid_owner_approval_ref",
                "owner approval reference must use the bounded approval:// format",
            )
        }),
        (_, Some(_)) => Err(ControlPlaneError::validation(
            "owner_approval_ref_not_applicable",
            "owner approval reference is accepted only for Autonomous promotion",
        )),
        (_, None) => Ok(None),
    }
}
