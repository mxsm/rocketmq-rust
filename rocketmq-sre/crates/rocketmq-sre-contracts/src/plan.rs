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
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::ActionPlanId;
use crate::ClusterId;
use crate::CompensationSpec;
use crate::ContractError;
use crate::DiagnosisRevisionId;
use crate::EvidenceId;
use crate::ExecutionAction;
use crate::ImpactScope;
use crate::IncidentId;
use crate::ModelInvocationId;
use crate::PlanStepId;
use crate::TenantId;
use crate::VerificationSpec;
use crate::canonical_precondition_hash;
use crate::canonical_sha256;
use crate::is_sha256_digest;

/// Immutable action-plan lifecycle.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanStatus {
    Draft,
    NeedsCritic,
    ReadyForApproval,
    InReview,
    Approved,
    Rejected,
    Expired,
    Superseded,
}

/// One typed and bounded step in an immutable plan.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanStep {
    pub id: PlanStepId,
    pub sequence: u16,
    pub action: ExecutionAction,
    pub descriptor_version: String,
    pub resource: String,
    pub parameters: Value,
    #[serde(default)]
    pub evidence_ids: Vec<EvidenceId>,
    pub precondition_hash: String,
    pub max_impact: ImpactScope,
    pub verification: VerificationSpec,
    pub compensation: CompensationSpec,
}

/// Validated input used to create a versioned action plan.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionPlanDraft {
    pub id: ActionPlanId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub incident_id: IncidentId,
    pub diagnosis_revision: DiagnosisRevisionId,
    pub primary_model_invocation_id: ModelInvocationId,
    pub diagnosis_execution_eligible: bool,
    pub version: u32,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub evidence_hash: String,
    pub steps: Vec<PlanStep>,
}

/// Immutable supervised action plan bound to one model-backed diagnosis.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionPlan {
    pub schema_version: String,
    pub id: ActionPlanId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub incident_id: IncidentId,
    pub diagnosis_revision: DiagnosisRevisionId,
    pub primary_model_invocation_id: ModelInvocationId,
    pub diagnosis_execution_eligible: bool,
    pub version: u32,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub evidence_hash: String,
    pub steps: Vec<PlanStep>,
    pub status: PlanStatus,
    pub submitted_at: Option<DateTime<Utc>>,
    pub plan_hash: String,
}

#[derive(Serialize)]
struct PlanHashMaterial<'a> {
    schema_version: &'a str,
    id: ActionPlanId,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    incident_id: IncidentId,
    diagnosis_revision: DiagnosisRevisionId,
    primary_model_invocation_id: ModelInvocationId,
    diagnosis_execution_eligible: bool,
    version: u32,
    created_by: &'a str,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    evidence_hash: &'a str,
    steps: &'a [PlanStep],
}

#[derive(Serialize)]
struct PreconditionHashMaterial<'a> {
    step_id: PlanStepId,
    sequence: u16,
    precondition_hash: &'a str,
}

impl ActionPlan {
    pub const SCHEMA_VERSION: &'static str = "rocketmq-sre.action-plan.v1";

    /// Seals a validated draft with a canonical protected-field hash.
    ///
    /// # Errors
    ///
    /// Rejects rules-only diagnoses, missing plan content, invalid windows, or
    /// values that cannot be canonicalized.
    pub fn seal(draft: ActionPlanDraft) -> Result<Self, ContractError> {
        validate_draft(&draft)?;
        let mut plan = Self {
            schema_version: Self::SCHEMA_VERSION.to_owned(),
            id: draft.id,
            tenant_id: draft.tenant_id,
            cluster_id: draft.cluster_id,
            incident_id: draft.incident_id,
            diagnosis_revision: draft.diagnosis_revision,
            primary_model_invocation_id: draft.primary_model_invocation_id,
            diagnosis_execution_eligible: draft.diagnosis_execution_eligible,
            version: draft.version,
            created_by: draft.created_by,
            created_at: draft.created_at,
            expires_at: draft.expires_at,
            evidence_hash: draft.evidence_hash,
            steps: draft.steps,
            status: PlanStatus::Draft,
            submitted_at: None,
            plan_hash: String::new(),
        };
        plan.plan_hash = plan.compute_plan_hash()?;
        Ok(plan)
    }

    /// Computes the canonical hash over every protected plan field.
    ///
    /// # Errors
    ///
    /// Returns a contract error if canonical JSON encoding fails.
    pub fn compute_plan_hash(&self) -> Result<String, ContractError> {
        canonical_sha256(&PlanHashMaterial {
            schema_version: &self.schema_version,
            id: self.id,
            tenant_id: self.tenant_id,
            cluster_id: self.cluster_id,
            incident_id: self.incident_id,
            diagnosis_revision: self.diagnosis_revision,
            primary_model_invocation_id: self.primary_model_invocation_id,
            diagnosis_execution_eligible: self.diagnosis_execution_eligible,
            version: self.version,
            created_by: &self.created_by,
            created_at: self.created_at,
            expires_at: self.expires_at,
            evidence_hash: &self.evidence_hash,
            steps: &self.steps,
        })
    }

    /// Verifies that a serialized plan still matches its protected fields.
    ///
    /// # Errors
    ///
    /// Rejects an empty or mismatched plan hash.
    pub fn verify_plan_hash(&self) -> Result<(), ContractError> {
        if self.schema_version != Self::SCHEMA_VERSION
            || !is_sha256_digest(&self.plan_hash)
            || self.plan_hash != self.compute_plan_hash()?
        {
            return Err(ContractError::InvalidContentHash);
        }
        Ok(())
    }

    /// Computes the aggregate digest bound into an approval grant.
    ///
    /// # Errors
    ///
    /// Returns a contract error when the ordered precondition set cannot be
    /// canonicalized.
    pub fn compute_precondition_hash(&self) -> Result<String, ContractError> {
        let material = self
            .steps
            .iter()
            .map(|step| PreconditionHashMaterial {
                step_id: step.id,
                sequence: step.sequence,
                precondition_hash: &step.precondition_hash,
            })
            .collect::<Vec<_>>();
        canonical_precondition_hash(&material)
    }

    /// Consumes a draft plan and returns its immutable review snapshot.
    ///
    /// No protected plan field is mutated by this operation. A later content
    /// change must be represented by a newly sealed plan version.
    ///
    /// # Errors
    ///
    /// Rejects modified, expired, already submitted, or invalid timestamps.
    pub fn submit_for_review(mut self, submitted_at: DateTime<Utc>, needs_critic: bool) -> Result<Self, ContractError> {
        self.verify_plan_hash()?;
        if self.status != PlanStatus::Draft
            || self.submitted_at.is_some()
            || submitted_at < self.created_at
            || submitted_at >= self.expires_at
        {
            return Err(ContractError::InvalidStateTransition {
                from: format!("{:?}", self.status),
                to: if needs_critic {
                    "NeedsCritic".to_owned()
                } else {
                    "ReadyForApproval".to_owned()
                },
            });
        }
        self.status = if needs_critic {
            PlanStatus::NeedsCritic
        } else {
            PlanStatus::ReadyForApproval
        };
        self.submitted_at = Some(submitted_at);
        Ok(self)
    }
}

fn validate_draft(draft: &ActionPlanDraft) -> Result<(), ContractError> {
    if !draft.diagnosis_execution_eligible {
        return Err(ContractError::InvalidDescriptor {
            reason: "rules-only diagnosis cannot create an executable action plan".to_owned(),
        });
    }
    if draft.version == 0 || draft.steps.is_empty() {
        return Err(ContractError::InvalidDescriptor {
            reason: "action plan version and steps must be non-empty".to_owned(),
        });
    }
    if draft.primary_model_invocation_id.as_uuid().is_nil() {
        return Err(ContractError::InvalidDescriptor {
            reason: "primary model invocation id must be non-empty".to_owned(),
        });
    }
    if draft.created_by.trim().is_empty()
        || !is_sha256_digest(&draft.evidence_hash)
        || draft.expires_at <= draft.created_at
    {
        return Err(ContractError::InvalidDescriptor {
            reason: "action plan actor, SHA-256 evidence hash, and validity window are required".to_owned(),
        });
    }
    let mut step_ids = BTreeSet::new();
    let mut sequences = BTreeSet::new();
    for step in &draft.steps {
        if step.sequence == 0
            || step.resource.trim().is_empty()
            || step.descriptor_version.trim().is_empty()
            || !is_sha256_digest(&step.precondition_hash)
            || !step_ids.insert(step.id)
            || !sequences.insert(step.sequence)
        {
            return Err(ContractError::InvalidDescriptor {
                reason: "every plan step requires a unique id and sequence, resource, descriptor version, and SHA-256 \
                         precondition hash"
                    .to_owned(),
            });
        }
    }
    for (index, sequence) in sequences.iter().enumerate() {
        if usize::from(*sequence) != index + 1 {
            return Err(ContractError::InvalidDescriptor {
                reason: "plan step sequences must be contiguous and start at one".to_owned(),
            });
        }
    }
    Ok(())
}

/// Non-executable fallback produced for rules-only or R3 recommendations.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManualRunbookDraft {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub incident_id: IncidentId,
    pub diagnosis_revision: DiagnosisRevisionId,
    pub title: String,
    pub reason_code: String,
    pub action_id: String,
    pub instructions: Vec<String>,
    pub execution_supported: bool,
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use crate::CompensationMode;

    fn draft() -> ActionPlanDraft {
        let created_at = Utc::now();
        ActionPlanDraft {
            id: ActionPlanId::new(),
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            incident_id: IncidentId::new(),
            diagnosis_revision: DiagnosisRevisionId::new(),
            primary_model_invocation_id: ModelInvocationId::new(),
            diagnosis_execution_eligible: true,
            version: 1,
            created_by: "operator-a".to_owned(),
            created_at,
            expires_at: created_at + Duration::hours(1),
            evidence_hash: format!("sha256:{}", "a".repeat(64)),
            steps: vec![PlanStep {
                id: PlanStepId::new(),
                sequence: 1,
                action: ExecutionAction::ProxyScaleOutOne,
                descriptor_version: "1.0.0".to_owned(),
                resource: "proxy/proxy-a".to_owned(),
                parameters: json!({"namespace": "default", "workload": "proxy", "expected_replicas": 2}),
                evidence_ids: vec![EvidenceId::new()],
                precondition_hash: format!("sha256:{}", "b".repeat(64)),
                max_impact: ImpactScope::OneReplica,
                verification: VerificationSpec {
                    resource_conditions: vec!["ready_replicas".to_owned()],
                    technical_slis: vec!["proxy_error_ratio".to_owned()],
                    stable_window_seconds: 60,
                    max_wait_seconds: 600,
                },
                compensation: CompensationSpec {
                    mode: CompensationMode::Automatic,
                    required_before_fields: vec!["replicas".to_owned()],
                    timeout_seconds: 300,
                },
            }],
        }
    }

    #[test]
    fn protected_field_changes_plan_hash() {
        let first = ActionPlan::seal(draft()).expect("valid plan");
        macro_rules! assert_hash_change {
            ($changed:ident, $mutation:stmt) => {{
                let mut $changed = first.clone();
                $mutation
                assert_ne!(
                    first.plan_hash,
                    $changed.compute_plan_hash().expect("changed plan should hash")
                );
            }};
        }

        assert_hash_change!(
            changed,
            changed.schema_version = "rocketmq-sre.action-plan.v2".to_owned()
        );
        assert_hash_change!(changed, changed.id = ActionPlanId::new());
        assert_hash_change!(changed, changed.tenant_id = TenantId::new());
        assert_hash_change!(changed, changed.cluster_id = ClusterId::new());
        assert_hash_change!(changed, changed.incident_id = IncidentId::new());
        assert_hash_change!(changed, changed.diagnosis_revision = DiagnosisRevisionId::new());
        assert_hash_change!(changed, changed.primary_model_invocation_id = ModelInvocationId::new());
        assert_hash_change!(changed, changed.diagnosis_execution_eligible = false);
        assert_hash_change!(changed, changed.version += 1);
        assert_hash_change!(changed, changed.created_by = "operator-b".to_owned());
        assert_hash_change!(changed, changed.created_at += Duration::seconds(1));
        assert_hash_change!(changed, changed.expires_at += Duration::seconds(1));
        assert_hash_change!(changed, changed.evidence_hash = format!("sha256:{}", "c".repeat(64)));
        assert_hash_change!(changed, changed.steps[0].id = PlanStepId::new());
        assert_hash_change!(changed, changed.steps[0].sequence = 2);
        assert_hash_change!(changed, changed.steps[0].action = ExecutionAction::ProxyRestartOne);
        assert_hash_change!(changed, changed.steps[0].descriptor_version = "1.0.1".to_owned());
        assert_hash_change!(changed, changed.steps[0].resource = "proxy/proxy-b".to_owned());
        assert_hash_change!(changed, changed.steps[0].parameters = json!({"expected_replicas": 3}));
        assert_hash_change!(changed, changed.steps[0].evidence_ids.push(EvidenceId::new()));
        assert_hash_change!(
            changed,
            changed.steps[0].precondition_hash = format!("sha256:{}", "d".repeat(64))
        );
        assert_hash_change!(changed, changed.steps[0].max_impact = ImpactScope::SingleInstance);
        assert_hash_change!(changed, changed.steps[0].verification.stable_window_seconds += 1);
        assert_hash_change!(changed, changed.steps[0].compensation.timeout_seconds += 1);
        assert!(first.verify_plan_hash().is_ok());
    }

    #[test]
    fn identical_plan_input_has_identical_hash() {
        let candidate = draft();

        let first = ActionPlan::seal(candidate.clone()).expect("first plan");
        let second = ActionPlan::seal(candidate).expect("second plan");

        assert_eq!(first.plan_hash, second.plan_hash);
    }

    #[test]
    fn every_identity_binding_changes_plan_hash() {
        let first = ActionPlan::seal(draft()).expect("valid plan");

        let mut changed_incident = first.clone();
        changed_incident.incident_id = IncidentId::new();
        assert_ne!(
            first.plan_hash,
            changed_incident.compute_plan_hash().expect("incident hash")
        );

        let mut changed_revision = first.clone();
        changed_revision.diagnosis_revision = DiagnosisRevisionId::new();
        assert_ne!(
            first.plan_hash,
            changed_revision.compute_plan_hash().expect("revision hash")
        );

        let mut changed_invocation = first.clone();
        changed_invocation.primary_model_invocation_id = ModelInvocationId::new();
        assert_ne!(
            first.plan_hash,
            changed_invocation.compute_plan_hash().expect("invocation hash")
        );
    }

    #[test]
    fn submitted_plan_preserves_hash_and_cannot_be_resubmitted() {
        let plan = ActionPlan::seal(draft()).expect("valid plan");
        let original_hash = plan.plan_hash.clone();
        let submitted_at = plan.created_at + Duration::minutes(1);
        let submitted = plan.submit_for_review(submitted_at, true).expect("draft should submit");

        assert_eq!(submitted.plan_hash, original_hash);
        assert_eq!(submitted.status, PlanStatus::NeedsCritic);
        assert_eq!(submitted.submitted_at, Some(submitted_at));
        assert!(
            submitted
                .submit_for_review(submitted_at + Duration::seconds(1), true)
                .is_err()
        );
    }

    #[test]
    fn rules_only_diagnosis_cannot_be_sealed() {
        let mut candidate = draft();
        candidate.diagnosis_execution_eligible = false;

        assert!(ActionPlan::seal(candidate).is_err());
    }

    #[test]
    fn nil_model_invocation_and_placeholder_hashes_are_rejected() {
        let mut candidate = draft();
        candidate.primary_model_invocation_id = ModelInvocationId::from_uuid(Uuid::nil());
        assert!(ActionPlan::seal(candidate).is_err());

        let mut candidate = draft();
        candidate.evidence_hash = "sha256:evidence".to_owned();
        assert!(ActionPlan::seal(candidate).is_err());

        let mut candidate = draft();
        candidate.steps[0].precondition_hash = "sha256:precondition".to_owned();
        assert!(ActionPlan::seal(candidate).is_err());
    }
}
