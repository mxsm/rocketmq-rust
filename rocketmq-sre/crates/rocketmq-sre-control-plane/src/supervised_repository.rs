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
use rocketmq_sre_contracts::ActionPlan;
use rocketmq_sre_contracts::ActionPlanId;
use rocketmq_sre_contracts::ActionRisk;
use rocketmq_sre_contracts::ApprovalDecision;
use rocketmq_sre_contracts::ApprovalRecord;
use rocketmq_sre_contracts::AuditEvent;
use rocketmq_sre_contracts::CriticConclusion;
use rocketmq_sre_contracts::CriticReview;
use rocketmq_sre_contracts::CriticReviewStatus;
use rocketmq_sre_contracts::ModelInvocationId;
use rocketmq_sre_contracts::PlanStatus;
use rocketmq_sre_contracts::PolicyDecision;
use rocketmq_sre_contracts::PolicyEffect;
use rocketmq_sre_contracts::canonical_sha256;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::Row;

use crate::ControlPlaneError;
use crate::PostgresRepository;

/// Immutable plan snapshot plus its separately mutable lifecycle projection.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredActionPlan {
    pub plan: ActionPlan,
    pub status: PlanStatus,
    pub submitted_at: Option<DateTime<Utc>>,
}

/// PostgreSQL persistence contract for supervised planning metadata.
#[allow(
    async_fn_in_trait,
    reason = "the control plane intentionally exposes a native async repository contract"
)]
pub trait SupervisedRepository: Clone + Send + Sync + 'static {
    async fn store_action_plan(&self, plan: &ActionPlan, risk: ActionRisk) -> Result<bool, ControlPlaneError>;
    async fn action_plan(&self, id: ActionPlanId) -> Result<StoredActionPlan, ControlPlaneError>;
    async fn compare_and_set_plan_status(
        &self,
        id: ActionPlanId,
        expected: PlanStatus,
        next: PlanStatus,
        submitted_at: Option<DateTime<Utc>>,
    ) -> Result<bool, ControlPlaneError>;
    async fn append_policy_decision(&self, decision: &PolicyDecision) -> Result<(), ControlPlaneError>;
    async fn append_approval(&self, approval: &ApprovalRecord) -> Result<(), ControlPlaneError>;
    async fn append_critic_review(&self, review: &CriticReview) -> Result<String, ControlPlaneError>;
    async fn append_audit_event(&self, event: &AuditEvent) -> Result<(), ControlPlaneError>;
}

impl SupervisedRepository for PostgresRepository {
    async fn store_action_plan(&self, plan: &ActionPlan, risk: ActionRisk) -> Result<bool, ControlPlaneError> {
        plan.verify_plan_hash()
            .map_err(|error| ControlPlaneError::validation("invalid_plan_hash", error.to_string()))?;
        let risk = action_risk_name(risk)?;
        let snapshot = json_value(plan)?;
        let result = sqlx::query(
            "INSERT INTO action_plans (
                id, tenant_id, cluster_id, incident_id, diagnosis_revision_id,
                primary_model_invocation_id, version, plan_hash, evidence_hash,
                risk, status, request_snapshot, created_by, created_at,
                expires_at, submitted_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, $11, $12, $13, $14,
                $15, $16
             )
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(plan.id.as_uuid())
        .bind(plan.tenant_id.as_uuid())
        .bind(plan.cluster_id.as_uuid())
        .bind(plan.incident_id.as_uuid())
        .bind(plan.diagnosis_revision.as_uuid())
        .bind(plan.primary_model_invocation_id.as_uuid())
        .bind(i32::try_from(plan.version).map_err(|_| {
            ControlPlaneError::validation("invalid_plan_version", "plan version exceeds PostgreSQL INTEGER")
        })?)
        .bind(&plan.plan_hash)
        .bind(&plan.evidence_hash)
        .bind(risk)
        .bind(plan_status_name(plan.status))
        .bind(snapshot)
        .bind(&plan.created_by)
        .bind(plan.created_at)
        .bind(plan.expires_at)
        .bind(plan.submitted_at)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 1 {
            return Ok(true);
        }
        let existing_hash: String = sqlx::query_scalar("SELECT plan_hash FROM action_plans WHERE id = $1")
            .bind(plan.id.as_uuid())
            .fetch_one(&self.pool)
            .await?;
        if existing_hash != plan.plan_hash {
            return Err(ControlPlaneError::conflict(
                "action plan identifier already exists with a different immutable hash",
            ));
        }
        Ok(false)
    }

    async fn action_plan(&self, id: ActionPlanId) -> Result<StoredActionPlan, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT request_snapshot, status, submitted_at
             FROM action_plans
             WHERE id = $1",
        )
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let status = parse_plan_status(row.try_get("status")?)?;
        let submitted_at = row.try_get("submitted_at")?;
        let mut plan: ActionPlan = from_json(row.try_get("request_snapshot")?)?;
        plan.status = status;
        plan.submitted_at = submitted_at;
        Ok(StoredActionPlan {
            plan,
            status,
            submitted_at,
        })
    }

    async fn compare_and_set_plan_status(
        &self,
        id: ActionPlanId,
        expected: PlanStatus,
        next: PlanStatus,
        submitted_at: Option<DateTime<Utc>>,
    ) -> Result<bool, ControlPlaneError> {
        let result = sqlx::query(
            "UPDATE action_plans
             SET status = $3,
                 submitted_at = COALESCE(submitted_at, $4)
             WHERE id = $1 AND status = $2",
        )
        .bind(id.as_uuid())
        .bind(plan_status_name(expected))
        .bind(plan_status_name(next))
        .bind(submitted_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn append_policy_decision(&self, decision: &PolicyDecision) -> Result<(), ControlPlaneError> {
        sqlx::query(
            "INSERT INTO policy_decisions (
                id, tenant_id, cluster_id, plan_id, plan_hash, policy_version,
                input_hash, effect, reason_codes, evaluated_by,
                decision_snapshot, evaluated_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10,
                $11, $12
             )",
        )
        .bind(decision.id.as_uuid())
        .bind(decision.tenant_id.as_uuid())
        .bind(decision.cluster_id.as_uuid())
        .bind(decision.plan_id.as_uuid())
        .bind(&decision.plan_hash)
        .bind(&decision.policy_version)
        .bind(&decision.input_hash)
        .bind(policy_effect_name(decision.effect))
        .bind(&decision.reason_codes)
        .bind(&decision.evaluated_by)
        .bind(json_value(decision)?)
        .bind(decision.evaluated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn append_approval(&self, approval: &ApprovalRecord) -> Result<(), ControlPlaneError> {
        sqlx::query(
            "INSERT INTO approvals (
                id, tenant_id, cluster_id, plan_id, plan_hash,
                requester_subject, approver_subject, approver_role,
                decision, reason, approval_snapshot, decided_at, expires_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8,
                $9, $10, $11, $12, $13
             )",
        )
        .bind(approval.id.as_uuid())
        .bind(approval.tenant_id.as_uuid())
        .bind(approval.cluster_id.as_uuid())
        .bind(approval.plan_id.as_uuid())
        .bind(&approval.plan_hash)
        .bind(&approval.requester_subject)
        .bind(&approval.approver_subject)
        .bind(&approval.approver_role)
        .bind(approval_decision_name(approval.decision))
        .bind(&approval.reason)
        .bind(json_value(approval)?)
        .bind(approval.decided_at)
        .bind(approval.expires_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn append_critic_review(&self, review: &CriticReview) -> Result<String, ControlPlaneError> {
        let review_hash = canonical_sha256(review)
            .map_err(|error| ControlPlaneError::validation("invalid_critic_review", error.to_string()))?;
        sqlx::query(
            "INSERT INTO critic_reviews (
                id, plan_id, plan_hash, diagnosis_revision_id, primary_invocation_id,
                critic_invocation_id, primary_model_family,
                critic_model_family, critic_provider, critic_profile,
                critic_model_revision, endpoint_instance, conclusion, status,
                review_hash, review_snapshot, created_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7,
                $8, $9, $10,
                $11, $12, $13, $14,
                $15, $16, $17
             )",
        )
        .bind(review.id.as_uuid())
        .bind(review.plan_id.as_uuid())
        .bind(&review.plan_hash)
        .bind(review.diagnosis_revision_id.as_uuid())
        .bind(review.primary_invocation_id.as_uuid())
        .bind(review.critic_invocation_id.map(ModelInvocationId::as_uuid))
        .bind(&review.primary_model_family)
        .bind(review.critic_model_family.as_deref())
        .bind(review.critic_provider.as_deref())
        .bind(review.critic_profile.as_deref())
        .bind(review.critic_model_revision.as_deref())
        .bind(review.endpoint_instance.as_deref())
        .bind(critic_conclusion_name(review.conclusion))
        .bind(critic_status_name(review.status))
        .bind(&review_hash)
        .bind(json_value(review)?)
        .bind(review.created_at)
        .execute(&self.pool)
        .await?;
        Ok(review_hash)
    }

    async fn append_audit_event(&self, event: &AuditEvent) -> Result<(), ControlPlaneError> {
        sqlx::query(
            "INSERT INTO audit_events (
                event_id, tenant_id, cluster_id, correlation_id, event_kind,
                actor_subject, actor_role, resource_kind, resource_id,
                reason_code, details, event_snapshot, occurred_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, $11, $12, $13
             )",
        )
        .bind(event.id.as_uuid())
        .bind(event.tenant_id.as_uuid())
        .bind(event.cluster_id.as_uuid())
        .bind(event.correlation_id.as_uuid())
        .bind(enum_name(&event.event_kind)?)
        .bind(&event.actor_subject)
        .bind(&event.actor_role)
        .bind(&event.resource_kind)
        .bind(&event.resource_id)
        .bind(&event.reason_code)
        .bind(&event.details)
        .bind(json_value(event)?)
        .bind(event.occurred_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

fn json_value(value: &impl Serialize) -> Result<Value, ControlPlaneError> {
    serde_json::to_value(value)
        .map_err(|error| ControlPlaneError::configuration(format!("snapshot encoding failed: {error}")))
}

fn from_json<T: DeserializeOwned>(value: Value) -> Result<T, ControlPlaneError> {
    serde_json::from_value(value)
        .map_err(|error| ControlPlaneError::configuration(format!("snapshot decoding failed: {error}")))
}

fn enum_name(value: &impl Serialize) -> Result<String, ControlPlaneError> {
    serde_json::to_value(value)
        .map_err(|error| ControlPlaneError::configuration(format!("enum encoding failed: {error}")))?
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| ControlPlaneError::configuration("enum did not encode as a string"))
}

const fn plan_status_name(status: PlanStatus) -> &'static str {
    match status {
        PlanStatus::Draft => "draft",
        PlanStatus::NeedsCritic => "needs_critic",
        PlanStatus::ReadyForApproval => "ready_for_approval",
        PlanStatus::InReview => "in_review",
        PlanStatus::Approved => "approved",
        PlanStatus::Rejected => "rejected",
        PlanStatus::Expired => "expired",
        PlanStatus::Superseded => "superseded",
    }
}

fn parse_plan_status(value: &str) -> Result<PlanStatus, ControlPlaneError> {
    match value {
        "draft" => Ok(PlanStatus::Draft),
        "needs_critic" => Ok(PlanStatus::NeedsCritic),
        "ready_for_approval" => Ok(PlanStatus::ReadyForApproval),
        "in_review" => Ok(PlanStatus::InReview),
        "approved" => Ok(PlanStatus::Approved),
        "rejected" => Ok(PlanStatus::Rejected),
        "expired" => Ok(PlanStatus::Expired),
        "superseded" => Ok(PlanStatus::Superseded),
        _ => Err(ControlPlaneError::configuration(
            "stored action plan has an unsupported status",
        )),
    }
}

fn action_risk_name(risk: ActionRisk) -> Result<&'static str, ControlPlaneError> {
    match risk {
        ActionRisk::R1 => Ok("r1"),
        ActionRisk::R2 => Ok("r2"),
        ActionRisk::Read | ActionRisk::Plan | ActionRisk::R3 => Err(ControlPlaneError::validation(
            "action_not_executable",
            "only R1 and R2 action plans may enter supervised persistence",
        )),
    }
}

const fn policy_effect_name(effect: PolicyEffect) -> &'static str {
    match effect {
        PolicyEffect::Allow => "allow",
        PolicyEffect::Deny => "deny",
        PolicyEffect::RequireApproval => "require_approval",
    }
}

const fn approval_decision_name(decision: ApprovalDecision) -> &'static str {
    match decision {
        ApprovalDecision::Approved => "approved",
        ApprovalDecision::Rejected => "rejected",
    }
}

const fn critic_conclusion_name(conclusion: CriticConclusion) -> &'static str {
    match conclusion {
        CriticConclusion::Accept => "accept",
        CriticConclusion::NeedsRevision => "needs_revision",
        CriticConclusion::Reject => "reject",
    }
}

const fn critic_status_name(status: CriticReviewStatus) -> &'static str {
    match status {
        CriticReviewStatus::Pending => "pending",
        CriticReviewStatus::Valid => "valid",
        CriticReviewStatus::Invalid => "invalid",
        CriticReviewStatus::Unavailable => "unavailable",
        CriticReviewStatus::Conflict => "conflict",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persistence_enums_match_database_constraints() {
        assert_eq!(plan_status_name(PlanStatus::ReadyForApproval), "ready_for_approval");
        assert_eq!(policy_effect_name(PolicyEffect::RequireApproval), "require_approval");
        assert_eq!(approval_decision_name(ApprovalDecision::Approved), "approved");
        assert_eq!(
            critic_conclusion_name(CriticConclusion::NeedsRevision),
            "needs_revision"
        );
    }

    #[test]
    fn display_only_and_r3_risks_never_enter_execution_storage() {
        for risk in [ActionRisk::Read, ActionRisk::Plan, ActionRisk::R3] {
            assert!(action_risk_name(risk).is_err());
        }
    }
}
