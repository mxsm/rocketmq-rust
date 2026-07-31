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

use rocketmq_sre_contracts::CriticConclusion;
use rocketmq_sre_contracts::CriticGateState;
use rocketmq_sre_contracts::CriticReview;
use rocketmq_sre_contracts::CriticReviewId;
use rocketmq_sre_contracts::CriticReviewStatus;

use super::*;
use crate::models::ModelCriticDecision;
use crate::supervised_execution::model::CriticReviewRequest;
use crate::supervised_execution::model::CriticReviewResponse;

impl SupervisedExecutionService {
    pub(crate) async fn review_with_critic(
        &self,
        auth: &AuthContext,
        id: ActionPlanId,
        request: &CriticReviewRequest,
        correlation_id: CorrelationId,
    ) -> Result<CriticReviewResponse, ControlPlaneError> {
        self.policy.require_operator(auth)?;
        let projection = self.repository.supervised_plan(auth, id).await?;
        let plan = projection.plan;
        if projection.risk != ActionRisk::R2 {
            return Err(ControlPlaneError::validation(
                "critic_not_required",
                "only R2 plans may enter the heterogeneous Critic gate",
            ));
        }
        if request.plan_hash != plan.plan_hash {
            return Err(ControlPlaneError::conflict_code(
                "plan_hash_mismatch",
                "Critic request does not bind the current immutable plan hash",
            ));
        }
        if plan.status != PlanStatus::NeedsCritic {
            return Err(ControlPlaneError::conflict_code(
                "plan_state_changed",
                "R2 plan is not awaiting a Critic review",
            ));
        }
        let now = self.now();
        if plan.expires_at <= now {
            return Err(ControlPlaneError::conflict_code(
                "plan_expired",
                "plan expired before the Critic review",
            ));
        }
        self.ensure_current_policy(auth, &plan).await?;
        let live = self.live_plan_state(auth, &plan, now).await?;
        if !live.facts.diagnosis_confirmed
            || !live.facts.diagnosis_execution_eligible
            || !live.facts.evidence_current
            || live.facts.resource_quarantined
            || !live.facts.rollback_available
        {
            return Err(ControlPlaneError::conflict_code(
                "critic_preconditions_invalid",
                "local plan, Evidence, rollback, or quarantine checks prevent Critic review",
            ));
        }
        let primary = self.repository.exact_primary_model_invocation(auth, &plan).await?;
        let primary_family = rocketmq_sre_model_gateway::normalize_model_family(&primary.model_family)
            .map_err(|_| ControlPlaneError::configuration("primary model family cannot be normalized"))?;
        let evidence_ids = plan
            .steps
            .iter()
            .flat_map(|step| step.evidence_ids.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let decision = self
            .model_gateway
            .critique_plan(auth, &plan, &primary, &evidence_ids, correlation_id)
            .await?;
        validate_critic_decision(&decision, &primary_family, &evidence_ids)?;
        let invocation = decision.invocation.as_ref();
        let findings = decision
            .assessment
            .as_ref()
            .map_or_else(Vec::new, |assessment| assessment.findings.clone());
        let review = CriticReview {
            id: CriticReviewId::new(),
            plan_id: plan.id,
            plan_hash: plan.plan_hash.clone(),
            diagnosis_revision_id: plan.diagnosis_revision,
            primary_invocation_id: primary.id,
            critic_invocation_id: invocation.map(|identity| identity.id),
            primary_model_family: primary_family,
            critic_model_family: invocation.map(|identity| identity.model_family.clone()),
            critic_provider: invocation.map(|identity| identity.provider_family.clone()),
            critic_profile: invocation.map(|identity| identity.profile.clone()),
            critic_model_revision: invocation.map(|identity| identity.model_revision.clone()),
            endpoint_instance: invocation.map(|identity| identity.endpoint_instance.clone()),
            fallback_chain: invocation.map_or_else(Vec::new, |identity| identity.fallback_chain.clone()),
            prompt_version: decision.prompt_version.to_owned(),
            schema_version: decision.schema_version.to_owned(),
            payload_hash: decision.payload_hash.clone(),
            status: decision.status,
            conclusion: decision.conclusion,
            assessment: decision.assessment,
            findings,
            created_at: now,
        };
        let advance = review.status == CriticReviewStatus::Valid
            && review.conclusion == CriticConclusion::Accept
            && review.critic_invocation_id.is_some();
        let audit = audit_event(
            auth,
            plan.cluster_id,
            correlation_id,
            AuditEventKind::CriticReviewed,
            "operator",
            "action_plan",
            plan.id.to_string(),
            decision.reason_code,
            json!({
                "plan_hash": plan.plan_hash,
                "review_id": review.id,
                "review_status": review.status,
                "conclusion": review.conclusion,
                "primary_model_family": review.primary_model_family,
                "critic_model_family": review.critic_model_family,
                "payload_hash": review.payload_hash,
                "advanced_to_approval": advance
            }),
            now,
        );
        let (plan, review_hash) = self
            .repository
            .persist_critic_review(&plan, &review, &audit, advance)
            .await?;
        self.publish_audits(std::slice::from_ref(&audit));
        Ok(CriticReviewResponse {
            critic_state: critic_gate_state(ActionRisk::R2, Some(&review)),
            plan,
            review,
            review_hash,
        })
    }
}

fn validate_critic_decision(
    decision: &ModelCriticDecision,
    primary_family: &str,
    evidence_ids: &[EvidenceId],
) -> Result<(), ControlPlaneError> {
    if let Some(assessment) = decision.assessment.as_ref() {
        assessment
            .validate(evidence_ids)
            .map_err(|error| ControlPlaneError::validation("critic_output_invalid", error.to_string()))?;
    }
    if let Some(invocation) = decision.invocation.as_ref() {
        let critic_family = rocketmq_sre_model_gateway::normalize_model_family(&invocation.model_family)
            .map_err(|_| ControlPlaneError::configuration("Critic model family cannot be normalized"))?;
        if critic_family == primary_family {
            return Err(ControlPlaneError::conflict_code(
                "critic_model_family_mismatch",
                "Critic actual model family must differ from the primary invocation family",
            ));
        }
    } else if decision.status == CriticReviewStatus::Valid {
        return Err(ControlPlaneError::configuration(
            "a valid Critic decision must contain an actual invocation identity",
        ));
    }
    Ok(())
}

pub(super) fn critic_gate_state(risk: ActionRisk, review: Option<&CriticReview>) -> CriticGateState {
    if risk != ActionRisk::R2 {
        return CriticGateState::UnreviewedNotRequired;
    }
    let Some(review) = review else {
        return CriticGateState::PendingRequired;
    };
    match (review.status, review.conclusion) {
        (CriticReviewStatus::Valid, CriticConclusion::Accept) => CriticGateState::Accepted,
        (CriticReviewStatus::Valid, CriticConclusion::NeedsRevision) => CriticGateState::NeedsRevision,
        (CriticReviewStatus::Valid, CriticConclusion::Reject) => CriticGateState::Rejected,
        (CriticReviewStatus::Pending, _) => CriticGateState::PendingRequired,
        (CriticReviewStatus::Invalid, _) => CriticGateState::Invalid,
        (CriticReviewStatus::Unavailable, _) => CriticGateState::Unavailable,
        (CriticReviewStatus::Conflict, _) => CriticGateState::Conflict,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn r1_explicitly_reports_unreviewed_not_required() {
        assert_eq!(
            critic_gate_state(ActionRisk::R1, None),
            CriticGateState::UnreviewedNotRequired
        );
        assert_eq!(
            critic_gate_state(ActionRisk::R2, None),
            CriticGateState::PendingRequired
        );
    }
}
