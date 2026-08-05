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

use std::collections::BTreeSet;

use chrono::DateTime;
use chrono::Timelike;
use chrono::Utc;
use rocketmq_sre_contracts::ActionPlan;
use rocketmq_sre_contracts::ActionRisk;
use rocketmq_sre_contracts::PolicyDecision;
use rocketmq_sre_contracts::PolicyDecisionId;
use rocketmq_sre_contracts::PolicyEffect;
use rocketmq_sre_contracts::canonical_sha256;
use serde::Deserialize;

use super::model::PolicyInputDigest;
use crate::ControlPlaneError;
use crate::auth::AuthContext;

const POLICY: &str = include_str!("../../../../config/policy/supervised-execution.v1.yaml");

pub(super) const RULES_ONLY_NOT_EXECUTABLE: &str = "RulesOnlyDiagnosisNotExecutable";
pub(super) const RESOURCE_QUARANTINED: &str = "ResourceQuarantined";

#[derive(Clone)]
pub(super) struct PolicyEvaluator {
    config: PolicyConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyConfig {
    schema_version: String,
    policy_version: String,
    evaluator: String,
    executor_audience: String,
    plan_ttl_seconds: u64,
    approval_ttl_seconds: u64,
    max_evidence_age_seconds: u64,
    maintenance_window_utc: MaintenanceWindow,
    approval_required: BTreeSet<ActionRisk>,
    required_operator_role: String,
    required_approver_role: String,
    deny_on: BTreeSet<String>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MaintenanceWindow {
    start_minute: u16,
    end_minute: u16,
}

#[derive(Clone, Copy)]
pub(super) struct PolicyFacts {
    pub(super) diagnosis_confirmed: bool,
    pub(super) diagnosis_execution_eligible: bool,
    pub(super) evidence_current: bool,
    pub(super) resource_quarantined: bool,
    pub(super) resource_busy: bool,
    pub(super) maintenance_window_open: bool,
    pub(super) rollback_available: bool,
}

impl PolicyEvaluator {
    pub(super) fn embedded() -> Result<Self, ControlPlaneError> {
        let config: PolicyConfig = serde_yaml::from_str(POLICY)
            .map_err(|error| ControlPlaneError::configuration(format!("supervised policy is invalid: {error}")))?;
        if config.schema_version != "rocketmq-sre.supervised-policy.v1"
            || config.policy_version.trim().is_empty()
            || config.evaluator != "rocketmq-sre-control-plane"
            || config.executor_audience != "rocketmq-sre-executor"
            || config.plan_ttl_seconds == 0
            || config.approval_ttl_seconds == 0
            || config.approval_ttl_seconds > config.plan_ttl_seconds
            || config.max_evidence_age_seconds == 0
            || config.maintenance_window_utc.start_minute >= config.maintenance_window_utc.end_minute
            || config.maintenance_window_utc.end_minute > 1_440
            || !config.approval_required.contains(&ActionRisk::R1)
            || !config.approval_required.contains(&ActionRisk::R2)
            || config.required_operator_role != "operator"
            || config.required_approver_role != "approver"
            || config.deny_on.is_empty()
        {
            return Err(ControlPlaneError::configuration(
                "supervised policy violates the Phase 3 fail-closed invariants",
            ));
        }
        Ok(Self { config })
    }

    pub(super) fn evaluate(
        &self,
        auth: &AuthContext,
        plan: &ActionPlan,
        risks: &[ActionRisk],
        facts: PolicyFacts,
        now: DateTime<Utc>,
    ) -> Result<PolicyDecision, ControlPlaneError> {
        let mut reasons = Vec::new();
        if !auth.roles.contains(&self.config.required_operator_role) {
            reasons.push("OperatorRoleRequired".to_owned());
        }
        if auth.tenant_id != plan.tenant_id || !auth.clusters.contains(&plan.cluster_id) {
            reasons.push("IdentityScopeMismatch".to_owned());
        }
        if !facts.diagnosis_execution_eligible {
            reasons.push(RULES_ONLY_NOT_EXECUTABLE.to_owned());
        }
        if !facts.diagnosis_confirmed {
            reasons.push("DiagnosisNotConfirmed".to_owned());
        }
        if !facts.evidence_current {
            reasons.push("EvidenceMissingOrStale".to_owned());
        }
        if facts.resource_quarantined {
            reasons.push(RESOURCE_QUARANTINED.to_owned());
        }
        if facts.resource_busy {
            reasons.push("ResourceChangeInProgress".to_owned());
        }
        if !facts.maintenance_window_open {
            reasons.push("MaintenanceWindowClosed".to_owned());
        }
        if !facts.rollback_available {
            reasons.push("RollbackUnavailable".to_owned());
        }
        if risks.is_empty() || risks.iter().any(|risk| !self.config.approval_required.contains(risk)) {
            reasons.push("UnsupportedExecutionRisk".to_owned());
        }
        let effect = if reasons.is_empty() {
            PolicyEffect::RequireApproval
        } else {
            PolicyEffect::Deny
        };
        if effect == PolicyEffect::RequireApproval {
            reasons.push("HumanApprovalRequired".to_owned());
        }
        let input_hash = canonical_sha256(&PolicyInputDigest {
            plan_hash: &plan.plan_hash,
            subject: &auth.subject,
            roles: &auth.roles,
            tenant_id: auth.tenant_id,
            cluster_id: plan.cluster_id,
            diagnosis_confirmed: facts.diagnosis_confirmed,
            diagnosis_execution_eligible: facts.diagnosis_execution_eligible,
            evidence_current: facts.evidence_current,
            resource_quarantined: facts.resource_quarantined,
            resource_busy: facts.resource_busy,
            maintenance_window_open: facts.maintenance_window_open,
            rollback_available: facts.rollback_available,
            risks,
        })
        .map_err(|error| ControlPlaneError::validation("invalid_policy_input", error.to_string()))?;
        Ok(PolicyDecision {
            id: PolicyDecisionId::new(),
            tenant_id: plan.tenant_id,
            cluster_id: plan.cluster_id,
            plan_id: plan.id,
            plan_hash: plan.plan_hash.clone(),
            policy_version: self.config.policy_version.clone(),
            input_hash,
            effect,
            reason_codes: reasons,
            evaluated_by: self.config.evaluator.clone(),
            evaluated_at: now,
        })
    }

    pub(super) fn require_operator(&self, auth: &AuthContext) -> Result<(), ControlPlaneError> {
        if !auth.roles.contains(&self.config.required_operator_role) {
            return Err(ControlPlaneError::forbidden(
                "operator_role_required",
                "the authenticated identity is not an operator",
            ));
        }
        Ok(())
    }

    pub(super) fn require_approver(&self, auth: &AuthContext) -> Result<(), ControlPlaneError> {
        if !auth.roles.contains(&self.config.required_approver_role) {
            return Err(ControlPlaneError::forbidden(
                "approver_role_required",
                "the authenticated identity is not an approver",
            ));
        }
        Ok(())
    }

    pub(super) fn maintenance_window_open(&self, now: DateTime<Utc>) -> bool {
        let minute = u16::try_from(now.hour() * 60 + now.minute()).unwrap_or(u16::MAX);
        minute >= self.config.maintenance_window_utc.start_minute
            && minute < self.config.maintenance_window_utc.end_minute
    }

    pub(super) const fn plan_ttl_seconds(&self) -> u64 {
        self.config.plan_ttl_seconds
    }

    pub(super) const fn approval_ttl_seconds(&self) -> u64 {
        self.config.approval_ttl_seconds
    }

    pub(super) const fn max_evidence_age_seconds(&self) -> u64 {
        self.config.max_evidence_age_seconds
    }

    pub(super) fn executor_audience(&self) -> &str {
        &self.config.executor_audience
    }

    pub(super) fn version(&self) -> &str {
        &self.config.policy_version
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Instant;

    use chrono::Duration;
    use rocketmq_sre_contracts::ActionPlanDraft;
    use rocketmq_sre_contracts::ActionPlanId;
    use rocketmq_sre_contracts::CompensationMode;
    use rocketmq_sre_contracts::CompensationSpec;
    use rocketmq_sre_contracts::DiagnosisRevisionId;
    use rocketmq_sre_contracts::EvidenceId;
    use rocketmq_sre_contracts::ExecutionAction;
    use rocketmq_sre_contracts::ImpactScope;
    use rocketmq_sre_contracts::IncidentId;
    use rocketmq_sre_contracts::ModelInvocationId;
    use rocketmq_sre_contracts::PlanStep;
    use rocketmq_sre_contracts::PlanStepId;
    use rocketmq_sre_contracts::TenantId;
    use rocketmq_sre_contracts::VerificationSpec;
    use serde_json::json;

    use super::*;

    fn plan() -> ActionPlan {
        let now = Utc::now();
        ActionPlan::seal(ActionPlanDraft {
            id: ActionPlanId::new(),
            tenant_id: TenantId::new(),
            cluster_id: rocketmq_sre_contracts::ClusterId::new(),
            incident_id: IncidentId::new(),
            diagnosis_revision: DiagnosisRevisionId::new(),
            primary_model_invocation_id: ModelInvocationId::new(),
            diagnosis_execution_eligible: true,
            version: 1,
            created_by: "operator-a".to_owned(),
            created_at: now,
            expires_at: now + Duration::hours(1),
            evidence_hash: format!("sha256:{}", "a".repeat(64)),
            steps: vec![PlanStep {
                id: PlanStepId::new(),
                sequence: 1,
                action: ExecutionAction::ProxyScaleOutOne,
                descriptor_version: "1.0.0".to_owned(),
                resource: "deployment/default/proxy".to_owned(),
                parameters: json!({"namespace":"default","workload":"proxy","expected_replicas":2}),
                evidence_ids: vec![EvidenceId::new()],
                precondition_hash: format!("sha256:{}", "b".repeat(64)),
                max_impact: ImpactScope::OneReplica,
                verification: VerificationSpec {
                    resource_conditions: vec!["ready".to_owned()],
                    technical_slis: vec!["error_ratio".to_owned()],
                    stable_window_seconds: 60,
                    max_wait_seconds: 600,
                },
                compensation: CompensationSpec {
                    mode: CompensationMode::Automatic,
                    required_before_fields: vec!["replicas".to_owned()],
                    timeout_seconds: 300,
                },
            }],
        })
        .expect("plan")
    }

    fn auth(plan: &ActionPlan) -> AuthContext {
        AuthContext {
            tenant_id: plan.tenant_id,
            subject: "operator-a".to_owned(),
            clusters: BTreeSet::from([plan.cluster_id]),
            roles: BTreeSet::from(["operator".to_owned()]),
        }
    }

    fn allow_facts() -> PolicyFacts {
        PolicyFacts {
            diagnosis_confirmed: true,
            diagnosis_execution_eligible: true,
            evidence_current: true,
            resource_quarantined: false,
            resource_busy: false,
            maintenance_window_open: true,
            rollback_available: true,
        }
    }

    #[test]
    fn phase_three_always_requires_human_approval() {
        let policy = PolicyEvaluator::embedded().expect("policy");
        let plan = plan();
        let decision = policy
            .evaluate(&auth(&plan), &plan, &[ActionRisk::R1], allow_facts(), Utc::now())
            .expect("decision");
        assert_eq!(decision.effect, PolicyEffect::RequireApproval);
        assert_eq!(decision.reason_codes, vec!["HumanApprovalRequired"]);
    }

    #[test]
    fn deterministic_deny_cannot_be_overridden_by_model_output() {
        let policy = PolicyEvaluator::embedded().expect("policy");
        let plan = plan();
        let mut facts = allow_facts();
        facts.resource_quarantined = true;
        let first = policy
            .evaluate(&auth(&plan), &plan, &[ActionRisk::R1], facts, Utc::now())
            .expect("decision");
        assert_eq!(first.effect, PolicyEffect::Deny);
        assert!(first.reason_codes.contains(&RESOURCE_QUARANTINED.to_owned()));
    }

    #[test]
    #[ignore = "writes an explicit production-readiness latency fragment"]
    fn policy_evaluation_latency_profile_is_bounded() {
        const SAMPLES: usize = 10_000;

        let policy = PolicyEvaluator::embedded().expect("policy");
        let plan = plan();
        let auth = auth(&plan);
        let now = Utc::now();
        let mut latencies = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let started = Instant::now();
            let decision = policy
                .evaluate(&auth, &plan, &[ActionRisk::R1], allow_facts(), now)
                .expect("policy decision");
            latencies.push(started.elapsed().as_secs_f64() * 1_000.0);
            assert_eq!(decision.effect, PolicyEffect::RequireApproval);
        }
        latencies.sort_by(f64::total_cmp);
        let p99_millis = latencies[(SAMPLES * 99).div_ceil(100) - 1];
        assert!(p99_millis <= 50.0, "policy p99 exceeded 50 ms: {p99_millis}");
        if let Ok(path) = std::env::var("ROCKETMQ_SRE_PRODUCTION_READINESS_POLICY_REPORT") {
            write_latency_report(
                Path::new(&path),
                json!({
                    "schema_version": "rocketmq-sre.production-readiness-policy-fragment.v1",
                    "status": "passed",
                    "samples": SAMPLES,
                    "p99_millis": p99_millis,
                    "unit": "milliseconds",
                    "effect": "require_approval",
                    "model_provider_network_calls": 0,
                    "secrets_recorded": false
                }),
            );
        }
    }

    fn write_latency_report(path: &Path, report: serde_json::Value) {
        std::fs::create_dir_all(path.parent().expect("report parent")).expect("report directory");
        std::fs::write(path, serde_json::to_vec_pretty(&report).expect("report JSON")).expect("policy latency report");
    }
}
