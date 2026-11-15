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

use rocketmq_sre_contracts::ACTION_PREPARATION_SCHEMA_VERSION;
use rocketmq_sre_contracts::ActionPreparationBinding;
use rocketmq_sre_contracts::ActionPreparationEvidence;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::EvidenceId;
use serde::Serialize;

use super::GrantSigner;
use crate::ControlPlaneError;
use crate::ControlPlaneRequestFailure;

const PREPARATION_ORIGIN_PURPOSE: &str = "rocketmq-sre.action-preparation.origin.v1";

#[derive(Serialize)]
struct PreparationOrigin<'a> {
    purpose: &'static str,
    schema_version: &'a str,
    evidence_id: EvidenceId,
    candidate_binding: &'a ActionPreparationBinding,
    agent_result: &'a AgentReadResult,
}

impl GrantSigner {
    /// Attests one completed CP-initiated readonly preparation, not an approval.
    pub(crate) fn seal_preparation(
        &self,
        evidence: &mut ActionPreparationEvidence,
    ) -> Result<(), ControlPlaneRequestFailure> {
        evidence.origin_signature = self.sign(&origin_payload(evidence)?)?;
        Ok(())
    }

    /// Verifies origin before callers use any readiness fields from the content.
    pub(crate) fn verify_preparation(
        &self,
        evidence: &ActionPreparationEvidence,
    ) -> Result<(), ControlPlaneRequestFailure> {
        self.verify(&origin_payload(evidence)?, &evidence.origin_signature)
    }
}

fn origin_payload(evidence: &ActionPreparationEvidence) -> Result<Vec<u8>, ControlPlaneRequestFailure> {
    if evidence.schema_version != ACTION_PREPARATION_SCHEMA_VERSION || evidence.evidence_id.as_uuid().is_nil() {
        return Err(ControlPlaneRequestFailure::conflict_code(
            "preparation_origin_invalid",
            "prepared evidence has no valid CP origin binding",
        ));
    }
    serde_jcs::to_vec(&PreparationOrigin {
        purpose: PREPARATION_ORIGIN_PURPOSE,
        schema_version: &evidence.schema_version,
        evidence_id: evidence.evidence_id,
        candidate_binding: &evidence.candidate_binding,
        agent_result: &evidence.agent_result,
    })
    .map_err(|source| ControlPlaneRequestFailure::from(ControlPlaneError::configuration_source(source)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use rocketmq_sre_contracts::{
        ActionPlan, ActionPlanDraft, ActionPlanId, ApprovalGrant, ApprovalId, ClusterId, CorrelationId,
        DiagnosisRevisionId, EXECUTION_AGENT_SCHEMA_VERSION, ExecutionAction, ExecutionId, ExecutionRequest,
        ExecutionStepId, IncidentId, LeaseEpoch, LeaseFenceGrant, LeaseId, ModelInvocationId, PlanStep, PlanStepId,
        TenantId,
    };
    use std::collections::BTreeMap;

    fn proof() -> ActionPreparationEvidence {
        let now = Utc::now();
        ActionPreparationEvidence {
            schema_version: ACTION_PREPARATION_SCHEMA_VERSION.to_owned(),
            evidence_id: EvidenceId::new(),
            candidate_binding: ActionPreparationBinding {
                tenant_id: TenantId::new(),
                cluster_id: ClusterId::new(),
                incident_id: IncidentId::new(),
                diagnosis_revision_id: DiagnosisRevisionId::new(),
                action_id: "observability.logger_level_ttl.v1".to_owned(),
                descriptor_version: "1.0.0".to_owned(),
                resource: "broker/example".to_owned(),
                parameters_digest: format!("sha256:{}", "a".repeat(64)),
                target_scope_digest: format!("sha256:{}", "b".repeat(64)),
                diagnostic_evidence: Vec::new(),
                policy_version: "1.0.0".to_owned(),
                actor_subject: "preparer-a".to_owned(),
                prepared_at: now,
                valid_until: now + Duration::seconds(120),
            },
            agent_result: AgentReadResult {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                action: ExecutionAction::ObservabilityLoggerLevelTtl,
                target: "broker/example".to_owned(),
                precondition_hash: format!("sha256:{}", "c".repeat(64)),
                ready: true,
                reason_codes: Vec::new(),
                resource_conditions: BTreeMap::new(),
                observed_at: now,
            },
            origin_signature: String::new(),
        }
    }

    #[test]
    fn preparation_origin_binds_id_parameters_actor_scope_result_and_time_across_restart() {
        let signer = GrantSigner::new("independent-stable-test-key-for-preparation").unwrap();
        let mut original = proof();
        signer.seal_preparation(&mut original).unwrap();
        GrantSigner::new("independent-stable-test-key-for-preparation")
            .unwrap()
            .verify_preparation(&original)
            .unwrap();
        assert!(
            GrantSigner::new("rotated-stable-test-key-for-preparation")
                .unwrap()
                .verify_preparation(&original)
                .is_err()
        );
        let mut variants = Vec::new();
        let mut changed = original.clone();
        changed.evidence_id = EvidenceId::new();
        variants.push(changed);
        let mut changed = original.clone();
        changed.candidate_binding.parameters_digest.push('a');
        variants.push(changed);
        let mut changed = original.clone();
        changed.candidate_binding.target_scope_digest.push('b');
        variants.push(changed);
        let mut changed = original.clone();
        changed.candidate_binding.actor_subject = "approver-b".to_owned();
        variants.push(changed);
        let mut changed = original.clone();
        changed.agent_result.ready = false;
        variants.push(changed);
        let mut changed = original.clone();
        changed.agent_result.observed_at += Duration::nanoseconds(1);
        variants.push(changed);
        let mut changed = original.clone();
        changed.candidate_binding.valid_until += Duration::nanoseconds(1);
        variants.push(changed);
        for value in variants {
            assert!(signer.verify_preparation(&value).is_err());
        }
    }

    #[test]
    fn origin_and_approval_signatures_are_not_interchangeable() {
        let signer = GrantSigner::new("independent-stable-test-key-for-preparation").unwrap();
        let mut prepared = proof();
        signer.seal_preparation(&mut prepared).unwrap();
        let origin_signature = prepared.origin_signature.clone();
        let now = Utc::now();
        let mut approval = ApprovalGrant {
            issuer: "rocketmq-sre-control-plane".to_owned(),
            audience: "rocketmq-sre-executor".to_owned(),
            approval_id: ApprovalId::new(),
            plan_id: ActionPlanId::new(),
            plan_hash: format!("sha256:{}", "d".repeat(64)),
            precondition_hash: prepared.agent_result.precondition_hash.clone(),
            tenant_id: prepared.candidate_binding.tenant_id,
            cluster_id: prepared.candidate_binding.cluster_id,
            approver_subject: "approver-b".to_owned(),
            issued_at: now,
            expires_at: now + Duration::seconds(120),
            nonce: uuid::Uuid::new_v4().to_string(),
            signature: prepared.origin_signature.clone(),
        };
        assert!(signer.verify_approval(&approval).is_err());
        signer.sign_approval(&mut approval).unwrap();
        prepared.origin_signature = approval.signature.clone();
        assert!(signer.verify_preparation(&prepared).is_err());

        let catalog = crate::supervised_execution::catalog::ActionCatalog::embedded().unwrap();
        let action = ExecutionAction::ObservabilityLoggerLevelTtl;
        let descriptor = catalog.descriptor(action).unwrap();
        let step_id = PlanStepId::new();
        let plan = ActionPlan::seal(ActionPlanDraft {
            id: approval.plan_id, tenant_id: approval.tenant_id, cluster_id: approval.cluster_id,
            incident_id: prepared.candidate_binding.incident_id, diagnosis_revision: prepared.candidate_binding.diagnosis_revision_id,
            primary_model_invocation_id: ModelInvocationId::new(), diagnosis_execution_eligible: true, version: 1,
            created_by: "preparer-a".to_owned(), created_at: now, expires_at: approval.expires_at,
            evidence_hash: format!("sha256:{}", "e".repeat(64)), steps: vec![PlanStep {
                id: step_id, sequence: 1, action, descriptor_version: descriptor.version.clone(),
                resource: prepared.agent_result.target.clone(), parameters: serde_json::json!({"component":"broker","logger":"rocketmq_broker::processor","level":"DEBUG","ttl_seconds":120}),
                evidence_ids: vec![prepared.evidence_id], precondition_hash: prepared.agent_result.precondition_hash.clone(),
                max_impact: descriptor.max_impact, verification: descriptor.verification.clone(), compensation: descriptor.compensation.clone(),
            }],
        }).unwrap();
        approval.plan_hash = plan.plan_hash.clone();
        approval.precondition_hash = plan.compute_precondition_hash().unwrap();
        signer.sign_approval(&mut approval).unwrap();
        let mut execution = ExecutionRequest {
            schema_version: ExecutionRequest::SCHEMA_VERSION.to_owned(),
            id: ExecutionId::new(),
            tenant_id: plan.tenant_id,
            cluster_id: plan.cluster_id,
            correlation_id: CorrelationId::new(),
            plan,
            approvals: vec![approval],
            autonomy_grant: None,
            requested_by: "preparer-a".to_owned(),
            idempotency_key: "cross-purpose-readonly-origin-is-not-dispatch".to_owned(),
            issuer: "rocketmq-sre-control-plane".to_owned(),
            audience: "rocketmq-sre-executor".to_owned(),
            issued_at: now,
            expires_at: now + Duration::seconds(60),
            nonce: uuid::Uuid::new_v4().to_string(),
            signature: origin_signature.clone(),
        };
        assert!(signer.verify_execution(&execution).is_err());
        signer.sign_execution(&mut execution).unwrap();
        prepared.origin_signature = execution.signature;
        assert!(signer.verify_preparation(&prepared).is_err());
        let mut fence = LeaseFenceGrant {
            lease_id: LeaseId::new(),
            owner: "executor".to_owned(),
            cluster_id: execution.cluster_id,
            epoch: LeaseEpoch(1),
            execution_id: execution.id,
            step_id: ExecutionStepId::new(),
            plan_step_id: step_id,
            action,
            resource: prepared.agent_result.target.clone(),
            compensation: false,
            audience: "rocketmq-sre-execution-agent".to_owned(),
            issued_at: now,
            expires_at: now + Duration::seconds(30),
            nonce: uuid::Uuid::new_v4().to_string(),
            signature: origin_signature,
        };
        assert!(signer.verify_fence_grant(&fence).is_err());
        signer.sign_fence_grant(&mut fence).unwrap();
        prepared.origin_signature = fence.signature;
        assert!(signer.verify_preparation(&prepared).is_err());
    }
}
