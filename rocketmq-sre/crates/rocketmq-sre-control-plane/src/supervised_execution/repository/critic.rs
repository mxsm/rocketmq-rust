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
use rocketmq_sre_contracts::CriticReview;
use rocketmq_sre_contracts::CriticReviewStatus;
use rocketmq_sre_contracts::ModelInvocationId;
use rocketmq_sre_contracts::canonical_sha256;

use super::*;

impl PostgresRepository {
    pub(crate) async fn latest_critic_review(
        &self,
        auth: &AuthContext,
        plan: &ActionPlan,
    ) -> Result<Option<CriticReview>, ControlPlaneError> {
        let snapshot: Option<Value> = sqlx::query_scalar(
            "SELECT review.review_snapshot
             FROM critic_reviews review
             JOIN action_plans plan ON plan.id = review.plan_id
             WHERE review.plan_id = $1
               AND review.plan_hash = $2
               AND review.diagnosis_revision_id = $3
               AND review.primary_invocation_id = $4
               AND plan.tenant_id = $5
               AND plan.cluster_id = $6
             ORDER BY review.created_at DESC, review.id DESC
             LIMIT 1",
        )
        .bind(plan.id.as_uuid())
        .bind(&plan.plan_hash)
        .bind(plan.diagnosis_revision.as_uuid())
        .bind(plan.primary_model_invocation_id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(plan.cluster_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        snapshot.map(from_json).transpose()
    }

    pub(crate) async fn valid_critic_review(
        &self,
        auth: &AuthContext,
        plan: &ActionPlan,
    ) -> Result<Option<CriticReview>, ControlPlaneError> {
        let review = self.latest_critic_review(auth, plan).await?;
        let Some(review) = review else {
            return Ok(None);
        };
        if review.status != CriticReviewStatus::Valid
            || review.conclusion != CriticConclusion::Accept
            || review.plan_id != plan.id
            || review.plan_hash != plan.plan_hash
            || review.diagnosis_revision_id != plan.diagnosis_revision
            || review.primary_invocation_id != plan.primary_model_invocation_id
            || review.critic_invocation_id.is_none()
        {
            return Ok(None);
        }
        let Some(critic_family) = review.critic_model_family.as_deref() else {
            return Ok(None);
        };
        let primary = rocketmq_sre_model_gateway::normalize_model_family(&review.primary_model_family)
            .map_err(|_| ControlPlaneError::configuration("stored primary model family is invalid"))?;
        let critic = rocketmq_sre_model_gateway::normalize_model_family(critic_family)
            .map_err(|_| ControlPlaneError::configuration("stored Critic model family is invalid"))?;
        Ok((primary != critic).then_some(review))
    }

    pub(crate) async fn persist_critic_review(
        &self,
        plan: &ActionPlan,
        review: &CriticReview,
        audit: &AuditEvent,
        advance_to_approval: bool,
    ) -> Result<(ActionPlan, String), ControlPlaneError> {
        let review_hash = canonical_sha256(review)
            .map_err(|error| ControlPlaneError::validation("invalid_critic_review", error.to_string()))?;
        let mut transaction = self.pool.begin().await?;
        let locked_status: Option<String> = sqlx::query_scalar(
            "SELECT status
             FROM action_plans
             WHERE id = $1
               AND tenant_id = $2
               AND cluster_id = $3
               AND plan_hash = $4
               AND diagnosis_revision_id = $5
               AND primary_model_invocation_id = $6
               AND expires_at > $7
             FOR UPDATE",
        )
        .bind(plan.id.as_uuid())
        .bind(plan.tenant_id.as_uuid())
        .bind(plan.cluster_id.as_uuid())
        .bind(&plan.plan_hash)
        .bind(plan.diagnosis_revision.as_uuid())
        .bind(plan.primary_model_invocation_id.as_uuid())
        .bind(review.created_at)
        .fetch_optional(&mut *transaction)
        .await?;
        if locked_status.as_deref() != Some("needs_critic") {
            return Err(ControlPlaneError::conflict_code(
                "plan_state_changed",
                "R2 plan is no longer awaiting a Critic review",
            ));
        }
        sqlx::query(
            "INSERT INTO critic_reviews (
                id, plan_id, plan_hash, diagnosis_revision_id,
                primary_invocation_id, critic_invocation_id,
                primary_model_family, critic_model_family,
                critic_provider, critic_profile, critic_model_revision,
                endpoint_instance, fallback_chain, prompt_version,
                schema_version, payload_hash, conclusion, status,
                review_hash, review_snapshot, created_at
             ) VALUES (
                $1, $2, $3, $4,
                $5, $6,
                $7, $8,
                $9, $10, $11,
                $12, $13, $14,
                $15, $16, $17, $18,
                $19, $20, $21
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
        .bind(&review.fallback_chain)
        .bind(&review.prompt_version)
        .bind(&review.schema_version)
        .bind(&review.payload_hash)
        .bind(critic_conclusion_name(review.conclusion))
        .bind(critic_status_name(review.status))
        .bind(&review_hash)
        .bind(json_value(review)?)
        .bind(review.created_at)
        .execute(&mut *transaction)
        .await?;
        if advance_to_approval {
            let changed = sqlx::query(
                "UPDATE action_plans
                 SET status = 'ready_for_approval'
                 WHERE id = $1 AND status = 'needs_critic' AND plan_hash = $2",
            )
            .bind(plan.id.as_uuid())
            .bind(&plan.plan_hash)
            .execute(&mut *transaction)
            .await?;
            if changed.rows_affected() != 1 {
                return Err(ControlPlaneError::conflict_code(
                    "plan_state_changed",
                    "R2 plan changed while the Critic review was committed",
                ));
            }
        }
        insert_audit(&mut transaction, audit).await?;
        transaction.commit().await?;
        let mut updated = plan.clone();
        if advance_to_approval {
            updated.status = PlanStatus::ReadyForApproval;
        }
        Ok((updated, review_hash))
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
