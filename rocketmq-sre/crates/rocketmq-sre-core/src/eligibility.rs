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

use rocketmq_sre_contracts::ActionDescriptor;
use rocketmq_sre_contracts::ActionRisk;
use rocketmq_sre_contracts::AutonomyCohortId;
use rocketmq_sre_contracts::AutonomyLifecycleState;
use rocketmq_sre_contracts::AutonomyMode;
use rocketmq_sre_contracts::AutonomyPolicyDefinition;
use rocketmq_sre_contracts::EligibilityDecision;
use rocketmq_sre_contracts::EligibilityPhase;
use rocketmq_sre_contracts::SreTimestamp;

/// Candidate evaluation path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AutonomyCandidatePath {
    Shadow,
    Autonomous,
}

/// Deterministic diagnosis, Evidence, and live safety facts.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BaseEligibilityFacts {
    pub diagnosis_execution_eligible: bool,
    pub rules_only: bool,
    pub root_cause_confirmed: bool,
    pub evidence_complete: bool,
    pub evidence_fresh: bool,
    pub required_sources_present: bool,
    pub frequency_available: bool,
    pub cooldown_complete: bool,
    pub concurrency_available: bool,
    pub error_budget_available: bool,
    pub freeze_active: bool,
    pub kill_switch_active: bool,
    pub authoritative_safety_available: bool,
}

/// Current cohort counters used only after a valid Critic review exists.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FinalEligibilityFacts {
    pub cohort_id: Option<AutonomyCohortId>,
    pub cohort_hash: Option<String>,
    pub critic_review_valid: bool,
    pub critic_is_heterogeneous: bool,
    pub invocation_bindings_valid: bool,
    pub owner_confirmed: bool,
    pub observation_window_met: bool,
    pub supervised_successes: u32,
    pub unresolved_unknown: u32,
    pub recent_rollbacks: u32,
}

/// Live per-step safety facts reloaded from authoritative state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DynamicSafetyFacts {
    pub authoritative_sources_available: bool,
    pub error_budget_available: bool,
    pub freeze_active: bool,
    pub kill_switch_active: bool,
    pub evidence_fresh: bool,
    pub policy_definition_matches: bool,
    pub lifecycle_revision_matches: bool,
}

/// Explainable dynamic safety result used before signing a decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DynamicSafetyEvaluation {
    pub allowed: bool,
    pub reason_codes: Vec<&'static str>,
}

/// Stateless fail-closed bounded-autonomy eligibility engine.
pub struct EligibilityEngine;

impl EligibilityEngine {
    /// Performs early qualification without issuing any grant.
    #[must_use]
    pub fn evaluate_base(
        path: AutonomyCandidatePath,
        policy: &AutonomyPolicyDefinition,
        lifecycle: &AutonomyLifecycleState,
        descriptor: &ActionDescriptor,
        facts: &BaseEligibilityFacts,
        evaluated_at: SreTimestamp,
    ) -> EligibilityDecision {
        let mut reasons = Vec::new();
        let expected_mode = match path {
            AutonomyCandidatePath::Shadow => AutonomyMode::Shadow,
            AutonomyCandidatePath::Autonomous => AutonomyMode::Autonomous,
        };
        if lifecycle.mode != expected_mode {
            reasons.push("lifecycle_mode_not_eligible");
        }
        if lifecycle.tenant_id != policy.tenant_id
            || lifecycle.cluster_id != policy.cluster_id
            || lifecycle.action != policy.action
            || descriptor.id != policy.action.id()
            || descriptor.version != policy.action_version
        {
            reasons.push("scope_or_descriptor_mismatch");
        }
        if descriptor.risk != ActionRisk::R1 || descriptor.plan_only {
            reasons.push("r1_action_required");
        }
        if !facts.diagnosis_execution_eligible {
            reasons.push("diagnosis_not_execution_eligible");
        }
        if facts.rules_only {
            reasons.push("rules_only_forbidden");
        }
        if !facts.root_cause_confirmed {
            reasons.push("root_cause_not_confirmed");
        }
        if !facts.evidence_complete || !facts.required_sources_present {
            reasons.push("evidence_incomplete");
        }
        if !facts.evidence_fresh {
            reasons.push("evidence_stale");
        }
        if !facts.frequency_available {
            reasons.push("frequency_limit");
        }
        if !facts.cooldown_complete {
            reasons.push("cooldown_active");
        }
        if !facts.concurrency_available {
            reasons.push("concurrency_limit");
        }
        if !facts.authoritative_safety_available {
            reasons.push("dynamic_safety_unavailable");
        }
        if !facts.error_budget_available {
            reasons.push("error_budget_exhausted");
        }
        if facts.freeze_active {
            reasons.push("freeze_active");
        }
        if facts.kill_switch_active {
            reasons.push("kill_switch_active");
        }
        EligibilityDecision {
            phase: EligibilityPhase::Base,
            allowed: reasons.is_empty(),
            reason_codes: reasons.into_iter().map(str::to_owned).collect(),
            cohort_id: None,
            cohort_hash: None,
            qualified_samples: 0,
            required_samples: 0,
            evaluated_at,
        }
    }

    /// Performs final Autonomous qualification after an immutable Critic
    /// review and actual model identities have been persisted.
    #[must_use]
    pub fn evaluate_final(
        policy: &AutonomyPolicyDefinition,
        lifecycle: &AutonomyLifecycleState,
        descriptor: &ActionDescriptor,
        facts: &FinalEligibilityFacts,
        evaluated_at: SreTimestamp,
    ) -> EligibilityDecision {
        let mut reasons = Vec::new();
        if lifecycle.mode != AutonomyMode::Autonomous {
            reasons.push("lifecycle_not_autonomous");
        }
        if descriptor.risk != ActionRisk::R1 || descriptor.plan_only || !descriptor.execution_supported {
            reasons.push("action_not_autonomy_executable");
        }
        if !facts.critic_review_valid {
            reasons.push("critic_not_ready");
        }
        if !facts.critic_is_heterogeneous {
            reasons.push("critic_family_not_heterogeneous");
        }
        if !facts.invocation_bindings_valid {
            reasons.push("model_invocation_binding_invalid");
        }
        if facts.cohort_id.is_none() || facts.cohort_hash.is_none() {
            reasons.push("autonomous_cohort_missing");
        }
        if !facts.owner_confirmed {
            reasons.push("action_owner_not_confirmed");
        }
        if !facts.observation_window_met {
            reasons.push("observation_window_incomplete");
        }
        if facts.supervised_successes < policy.min_supervised_successes {
            reasons.push("supervised_successes_insufficient");
        }
        if facts.unresolved_unknown > policy.max_unresolved_unknown {
            reasons.push("unresolved_unknown_present");
        }
        if facts.recent_rollbacks > policy.max_recent_rollbacks {
            reasons.push("recent_rollback_present");
        }
        EligibilityDecision {
            phase: EligibilityPhase::Final,
            allowed: reasons.is_empty(),
            reason_codes: reasons.into_iter().map(str::to_owned).collect(),
            cohort_id: facts.cohort_id,
            cohort_hash: facts.cohort_hash.clone(),
            qualified_samples: facts.supervised_successes,
            required_samples: policy.min_supervised_successes,
            evaluated_at,
        }
    }

    /// Re-evaluates safety before each new positive StepIntent.
    #[must_use]
    pub fn evaluate_dynamic_safety(facts: DynamicSafetyFacts) -> DynamicSafetyEvaluation {
        let mut reasons = Vec::new();
        if !facts.authoritative_sources_available {
            reasons.push("dynamic_safety_unavailable");
        }
        if !facts.error_budget_available {
            reasons.push("error_budget_exhausted");
        }
        if facts.freeze_active {
            reasons.push("freeze_active");
        }
        if facts.kill_switch_active {
            reasons.push("kill_switch_active");
        }
        if !facts.evidence_fresh {
            reasons.push("evidence_stale");
        }
        if !facts.policy_definition_matches {
            reasons.push("policy_definition_changed");
        }
        if !facts.lifecycle_revision_matches {
            reasons.push("lifecycle_revision_changed");
        }
        DynamicSafetyEvaluation {
            allowed: reasons.is_empty(),
            reason_codes: reasons,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rocketmq_sre_contracts::AutonomyPolicyId;
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::CompensationSpec;
    use rocketmq_sre_contracts::DescriptorStatus;
    use rocketmq_sre_contracts::ExecutionAction;
    use rocketmq_sre_contracts::ImpactScope;
    use rocketmq_sre_contracts::SchemaVersion;
    use rocketmq_sre_contracts::TenantId;
    use rocketmq_sre_contracts::VerificationSpec;

    use super::*;

    fn digest(value: char) -> String {
        format!("sha256:{}", value.to_string().repeat(64))
    }

    fn fixture() -> (AutonomyPolicyDefinition, AutonomyLifecycleState, ActionDescriptor) {
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let action = ExecutionAction::ObservabilityLoggerLevelTtl;
        (
            AutonomyPolicyDefinition {
                id: AutonomyPolicyId::new(),
                definition_version: 1,
                tenant_id,
                cluster_id,
                action,
                action_version: "1.0.0".to_owned(),
                descriptor_digest: digest('a'),
                diagnostic_pack_id: "runtime-diagnostics".to_owned(),
                diagnostic_pack_version: "1.0.0".to_owned(),
                owner: "messaging-observability".to_owned(),
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
                created_at: chrono::Utc::now(),
            },
            AutonomyLifecycleState {
                tenant_id,
                cluster_id,
                action,
                mode: AutonomyMode::Autonomous,
                previous_mode: None,
                owner: "messaging-observability".to_owned(),
                pause_reason: None,
                lifecycle_revision: 4,
                updated_by: "operator".to_owned(),
                updated_at: chrono::Utc::now(),
            },
            ActionDescriptor {
                id: action.id().to_owned(),
                version: "1.0.0".to_owned(),
                owner: "messaging-observability".to_owned(),
                supported_versions: vec![SchemaVersion::new("rocketmq-sre.action-plan", 1, 0)],
                required_capabilities: BTreeSet::new(),
                config_schema: serde_json::json!({"type": "object"}),
                status: DescriptorStatus::Active,
                deprecation: None,
                risk: ActionRisk::R1,
                execution_supported: true,
                parameter_schema: serde_json::json!({"type": "object"}),
                preconditions: Vec::new(),
                max_impact: ImpactScope::SingleResource,
                verification: VerificationSpec::default(),
                timeout_seconds: 60,
                compensation: CompensationSpec::default(),
                forbidden_fields: BTreeSet::new(),
                plan_only: false,
            },
        )
    }

    fn allow_base() -> BaseEligibilityFacts {
        BaseEligibilityFacts {
            diagnosis_execution_eligible: true,
            rules_only: false,
            root_cause_confirmed: true,
            evidence_complete: true,
            evidence_fresh: true,
            required_sources_present: true,
            frequency_available: true,
            cooldown_complete: true,
            concurrency_available: true,
            error_budget_available: true,
            freeze_active: false,
            kill_switch_active: false,
            authoritative_safety_available: true,
        }
    }

    #[test]
    fn shadow_never_accepts_an_autonomous_lifecycle() {
        let (policy, lifecycle, descriptor) = fixture();
        let decision = EligibilityEngine::evaluate_base(
            AutonomyCandidatePath::Shadow,
            &policy,
            &lifecycle,
            &descriptor,
            &allow_base(),
            chrono::Utc::now(),
        );
        assert!(!decision.allowed);
        assert!(
            decision
                .reason_codes
                .contains(&"lifecycle_mode_not_eligible".to_owned())
        );
    }

    #[test]
    fn r2_and_rules_only_are_always_denied() {
        let (policy, lifecycle, mut descriptor) = fixture();
        descriptor.risk = ActionRisk::R2;
        let mut facts = allow_base();
        facts.rules_only = true;
        let decision = EligibilityEngine::evaluate_base(
            AutonomyCandidatePath::Autonomous,
            &policy,
            &lifecycle,
            &descriptor,
            &facts,
            chrono::Utc::now(),
        );
        assert!(!decision.allowed);
        assert!(decision.reason_codes.contains(&"r1_action_required".to_owned()));
        assert!(decision.reason_codes.contains(&"rules_only_forbidden".to_owned()));
    }

    #[test]
    fn final_eligibility_requires_current_critic_cohort_and_samples() {
        let (policy, lifecycle, descriptor) = fixture();
        let denied = EligibilityEngine::evaluate_final(
            &policy,
            &lifecycle,
            &descriptor,
            &FinalEligibilityFacts::default(),
            chrono::Utc::now(),
        );
        assert!(!denied.allowed);

        let allowed = EligibilityEngine::evaluate_final(
            &policy,
            &lifecycle,
            &descriptor,
            &FinalEligibilityFacts {
                cohort_id: Some(AutonomyCohortId::new()),
                cohort_hash: Some(digest('b')),
                critic_review_valid: true,
                critic_is_heterogeneous: true,
                invocation_bindings_valid: true,
                owner_confirmed: true,
                observation_window_met: true,
                supervised_successes: 5,
                unresolved_unknown: 0,
                recent_rollbacks: 0,
            },
            chrono::Utc::now(),
        );
        assert!(allowed.allowed);
    }

    #[test]
    fn dynamic_safety_fails_closed_for_each_authoritative_control() {
        let allowed = DynamicSafetyFacts {
            authoritative_sources_available: true,
            error_budget_available: true,
            freeze_active: false,
            kill_switch_active: false,
            evidence_fresh: true,
            policy_definition_matches: true,
            lifecycle_revision_matches: true,
        };
        assert!(EligibilityEngine::evaluate_dynamic_safety(allowed).allowed);

        for denied in [
            DynamicSafetyFacts {
                error_budget_available: false,
                ..allowed
            },
            DynamicSafetyFacts {
                freeze_active: true,
                ..allowed
            },
            DynamicSafetyFacts {
                kill_switch_active: true,
                ..allowed
            },
            DynamicSafetyFacts {
                authoritative_sources_available: false,
                ..allowed
            },
        ] {
            assert!(!EligibilityEngine::evaluate_dynamic_safety(denied).allowed);
        }
    }
}
