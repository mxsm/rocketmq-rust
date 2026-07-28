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
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ExecutorLease;
use rocketmq_sre_contracts::FenceAck;
use rocketmq_sre_contracts::LeaseEpoch;
use rocketmq_sre_contracts::LeaseId;
use rocketmq_sre_contracts::LeaseState;
use rocketmq_sre_contracts::TenantId;
use serde_json::Value;
use sqlx::PgPool;
use sqlx::Row;

use crate::ControlPlaneError;

#[derive(Clone, Debug)]
pub(crate) struct LeaseAuthorityRepository {
    pool: PgPool,
}

impl LeaseAuthorityRepository {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub(super) async fn begin_takeover(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        owner: &str,
        pending_nonce: &str,
        acquired_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<ExecutorLease, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(cluster_id.to_string())
            .execute(&mut *transaction)
            .await?;
        let cluster_matches: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1
                FROM clusters
                WHERE id = $1
                  AND tenant_id = $2
                  AND onboarding_state <> 'offboarded'
            )",
        )
        .bind(cluster_id.as_uuid())
        .bind(tenant_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        if !cluster_matches {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "lease scope does not identify an active tenant cluster",
            ));
        }
        sqlx::query(
            "UPDATE executor_leases
             SET state = 'expired',
                 expires_at = LEAST(expires_at, $2),
                 updated_at = $2
             WHERE cluster_id = $1 AND state IN ('pending_fence', 'active')",
        )
        .bind(cluster_id.as_uuid())
        .bind(acquired_at)
        .execute(&mut *transaction)
        .await?;
        let previous_epoch: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(epoch), 0)
             FROM executor_leases
             WHERE cluster_id = $1",
        )
        .bind(cluster_id.as_uuid())
        .fetch_one(&mut *transaction)
        .await?;
        let epoch = previous_epoch.checked_add(1).ok_or_else(|| {
            ControlPlaneError::conflict_code("lease_epoch_exhausted", "executor lease epoch is exhausted")
        })?;
        let id = LeaseId::new();
        sqlx::query(
            "INSERT INTO executor_leases (
                id, tenant_id, cluster_id, epoch, owner, state, pending_nonce,
                fence_ack_snapshot, acquired_at, activated_at, expires_at,
                updated_at
             ) VALUES (
                $1, $2, $3, $4, $5, 'pending_fence', $6,
                NULL, $7, NULL, $8,
                $7
             )",
        )
        .bind(id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(epoch)
        .bind(owner)
        .bind(pending_nonce)
        .bind(acquired_at)
        .bind(expires_at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(ExecutorLease {
            id,
            tenant_id,
            cluster_id,
            epoch: parse_epoch(epoch)?,
            owner: owner.to_owned(),
            state: LeaseState::PendingFence,
            pending_nonce: pending_nonce.to_owned(),
            acquired_at,
            activated_at: None,
            expires_at,
        })
    }

    pub(super) async fn lease(&self, lease_id: LeaseId) -> Result<ExecutorLease, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, epoch, owner, state,
                    pending_nonce, acquired_at, activated_at, expires_at
             FROM executor_leases
             WHERE id = $1",
        )
        .bind(lease_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        lease_from_row(&row)
    }

    pub(super) async fn activate(
        &self,
        lease: &ExecutorLease,
        ack: &FenceAck,
    ) -> Result<ExecutorLease, ControlPlaneError> {
        let snapshot = serde_json::to_value(ack)
            .map_err(|error| ControlPlaneError::configuration(format!("FenceAck cannot be encoded: {error}")))?;
        let result = sqlx::query(
            "UPDATE executor_leases
             SET state = 'active',
                 fence_ack_snapshot = $2,
                 activated_at = $3,
                 updated_at = $3
             WHERE id = $1
               AND tenant_id = $4
               AND cluster_id = $5
               AND state = 'pending_fence'
               AND epoch = $6
               AND pending_nonce = $7
               AND expires_at > $3
               AND EXISTS (
                   SELECT 1
                   FROM execution_agent_fences fence
                   WHERE fence.cluster_id = $5
                     AND fence.tenant_id = $4
                     AND fence.lease_id = $1
                     AND fence.highest_epoch = $6
                     AND fence.fence_ack_snapshot = $2
               )",
        )
        .bind(lease.id.as_uuid())
        .bind(snapshot)
        .bind(ack.acknowledged_at)
        .bind(lease.tenant_id.as_uuid())
        .bind(lease.cluster_id.as_uuid())
        .bind(epoch_i64(lease.epoch)?)
        .bind(&lease.pending_nonce)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(ControlPlaneError::conflict_code(
                "fence_ack_rejected",
                "pending lease could not be activated from the supplied FenceAck",
            ));
        }
        let mut active = lease.clone();
        active.state = LeaseState::Active;
        active.activated_at = Some(ack.acknowledged_at);
        Ok(active)
    }

    pub(super) async fn assert_pending(
        &self,
        tenant_id: TenantId,
        grant: &rocketmq_sre_contracts::ReconcileGrant,
        now: DateTime<Utc>,
    ) -> Result<(), ControlPlaneError> {
        let valid: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1
                FROM executor_leases
                WHERE id = $1
                  AND tenant_id = $2
                  AND cluster_id = $3
                  AND epoch = $4
                  AND owner = $5
                  AND state = 'pending_fence'
                  AND pending_nonce = $6
                  AND expires_at > $7
            )",
        )
        .bind(grant.lease_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(grant.cluster_id.as_uuid())
        .bind(epoch_i64(grant.pending_epoch)?)
        .bind(&grant.owner)
        .bind(&grant.nonce)
        .bind(now)
        .fetch_one(&self.pool)
        .await?;
        if valid {
            Ok(())
        } else {
            Err(ControlPlaneError::forbidden(
                "lease_not_pending",
                "reconcile grant no longer identifies the current pending lease",
            ))
        }
    }

    pub(super) async fn assert_active(
        &self,
        tenant_id: TenantId,
        grant: &rocketmq_sre_contracts::LeaseFenceGrant,
        now: DateTime<Utc>,
    ) -> Result<(), ControlPlaneError> {
        let valid: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1
                FROM executor_leases lease
                JOIN executions execution ON execution.id = $6
                WHERE lease.id = $1
                  AND lease.tenant_id = $2
                  AND lease.cluster_id = $3
                  AND lease.epoch = $4
                  AND lease.owner = $5
                  AND lease.state = 'active'
                  AND lease.expires_at > $7
                  AND lease.epoch = (
                      SELECT MAX(latest.epoch)
                      FROM executor_leases latest
                      WHERE latest.cluster_id = lease.cluster_id
                  )
                  AND execution.tenant_id = $2
                  AND execution.cluster_id = $3
                  AND execution.state NOT IN ('succeeded', 'rolled_back', 'escalated')
            )",
        )
        .bind(grant.lease_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(grant.cluster_id.as_uuid())
        .bind(epoch_i64(grant.epoch)?)
        .bind(&grant.owner)
        .bind(grant.execution_id.as_uuid())
        .bind(now)
        .fetch_one(&self.pool)
        .await?;
        if valid {
            Ok(())
        } else {
            Err(ControlPlaneError::forbidden(
                "stale_lease_epoch",
                "dispatch grant no longer identifies the active lease and execution",
            ))
        }
    }

    pub(super) async fn execution_is_current(
        &self,
        execution: &rocketmq_sre_contracts::ExecutionRequest,
        now: DateTime<Utc>,
    ) -> Result<(), ControlPlaneError> {
        let plan_current: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1
                FROM action_plans plan
                JOIN clusters cluster ON cluster.id = plan.cluster_id
                WHERE plan.id = $1
                  AND plan.tenant_id = $2
                  AND plan.cluster_id = $3
                  AND plan.plan_hash = $4
                  AND plan.expires_at > $5
                  AND cluster.onboarding_state <> 'offboarded'
                  AND (
                    (
                      $6
                      AND plan.risk = 'r1'
                      AND plan.status IN ('ready_for_approval', 'approved')
                    )
                    OR (
                      NOT $6
                      AND plan.risk IN ('r1', 'r2')
                      AND plan.status = 'approved'
                    )
                  )
            )",
        )
        .bind(execution.plan.id.as_uuid())
        .bind(execution.tenant_id.as_uuid())
        .bind(execution.cluster_id.as_uuid())
        .bind(&execution.plan.plan_hash)
        .bind(now)
        .bind(execution.is_autonomous())
        .fetch_one(&self.pool)
        .await?;
        if !plan_current {
            return Err(ControlPlaneError::conflict_code(
                "execution_request_stale",
                "execution plan is no longer approved, current, or onboarded",
            ));
        }
        for step in &execution.plan.steps {
            let quarantined: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                    SELECT 1
                    FROM resource_quarantines
                    WHERE tenant_id = $1
                      AND cluster_id = $2
                      AND resource_key = $3
                      AND cleared_at IS NULL
                      AND (action_id IS NULL OR action_id = $4)
                )",
            )
            .bind(execution.tenant_id.as_uuid())
            .bind(execution.cluster_id.as_uuid())
            .bind(&step.resource)
            .bind(step.action.id())
            .fetch_one(&self.pool)
            .await?;
            if quarantined {
                return Err(ControlPlaneError::conflict_code(
                    "resource_quarantined",
                    "execution target is under persistent quarantine",
                ));
            }
        }
        Ok(())
    }

    pub(crate) async fn autonomy_grant_is_current(
        &self,
        grant: &rocketmq_sre_contracts::AutonomyGrant,
    ) -> Result<(), ControlPlaneError> {
        let valid: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1
                FROM autonomy_lifecycle_states AS lifecycle
                JOIN autonomy_qualification_cohorts AS cohort
                  ON cohort.id = $10
                 AND cohort.level = 'autonomous'
                 AND cohort.tenant_id = lifecycle.tenant_id
                 AND cohort.cluster_id = lifecycle.cluster_id
                 AND cohort.action_id = lifecycle.action_id
                 AND cohort.action_version = lifecycle.action_version
                 AND cohort.policy_id = lifecycle.policy_id
                 AND cohort.policy_definition_version = lifecycle.policy_definition_version
                JOIN critic_reviews AS review
                  ON review.id = $12
                 AND review.plan_id = $3
                 AND review.plan_hash = $4
                 AND review.diagnosis_revision_id = $5
                 AND review.primary_invocation_id = $13
                 AND review.critic_invocation_id = $14
                 AND review.status = 'valid'
                 AND review.conclusion = 'accept'
                JOIN action_plans AS plan
                  ON plan.id = review.plan_id
                 AND plan.tenant_id = lifecycle.tenant_id
                 AND plan.cluster_id = lifecycle.cluster_id
                 AND plan.risk = 'r1'
                 AND plan.status IN ('ready_for_approval', 'approved')
                WHERE lifecycle.tenant_id = $1
                  AND lifecycle.cluster_id = $2
                  AND lifecycle.action_id = $6
                  AND lifecycle.action_version = $7
                  AND lifecycle.policy_id = $8
                  AND lifecycle.policy_definition_version = $9
                  AND lifecycle.lifecycle_revision = $11
                  AND lifecycle.mode = 'autonomous'
                  AND cohort.cohort_hash = $15
            )",
        )
        .bind(grant.tenant_id.as_uuid())
        .bind(grant.cluster_id.as_uuid())
        .bind(grant.plan_id.as_uuid())
        .bind(&grant.plan_hash)
        .bind(grant.diagnosis_revision_id.as_uuid())
        .bind(grant.action.id())
        .bind(&grant.action_version)
        .bind(grant.policy_id.as_uuid())
        .bind(
            i64::try_from(grant.policy_definition_version)
                .map_err(|_| ControlPlaneError::validation("invalid_autonomy_grant", "policy version is too large"))?,
        )
        .bind(grant.autonomous_cohort_id.as_uuid())
        .bind(
            i64::try_from(grant.lifecycle_revision).map_err(|_| {
                ControlPlaneError::validation("invalid_autonomy_grant", "lifecycle revision is too large")
            })?,
        )
        .bind(grant.critic_review_id.as_uuid())
        .bind(grant.primary_model_invocation_id.as_uuid())
        .bind(grant.critic_model_invocation_id.as_uuid())
        .bind(&grant.autonomous_cohort_hash)
        .fetch_one(&self.pool)
        .await?;
        if valid {
            Ok(())
        } else {
            Err(ControlPlaneError::forbidden(
                "autonomy_grant_stale",
                "autonomy grant no longer binds the current R1 policy, lifecycle, cohort, and Critic review",
            ))
        }
    }

    pub(super) async fn validate_fence_grant_binding(
        &self,
        grant: &rocketmq_sre_contracts::LeaseFenceGrant,
    ) -> Result<(), ControlPlaneError> {
        let request_snapshot: Option<Value> = sqlx::query_scalar(
            "SELECT request_snapshot
             FROM executions
             WHERE id = $1
               AND tenant_id = (
                   SELECT tenant_id FROM executor_leases WHERE id = $2
               )
               AND cluster_id = $3",
        )
        .bind(grant.execution_id.as_uuid())
        .bind(grant.lease_id.as_uuid())
        .bind(grant.cluster_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        let snapshot = request_snapshot.ok_or(ControlPlaneError::NotFound)?;
        let request: rocketmq_sre_contracts::ExecutionRequest = serde_json::from_value(snapshot).map_err(|_| {
            ControlPlaneError::conflict_code("execution_snapshot_invalid", "execution snapshot is invalid")
        })?;
        let matches = request.plan.steps.iter().any(|step| {
            step.id == grant.plan_step_id && step.action == grant.action && step.resource == grant.resource
        });
        if matches {
            Ok(())
        } else {
            Err(ControlPlaneError::forbidden(
                "grant_binding_mismatch",
                "dispatch grant does not bind an approved execution step",
            ))
        }
    }

    pub(super) async fn execution_step(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        execution_id: ExecutionId,
        plan_step_id: rocketmq_sre_contracts::PlanStepId,
    ) -> Result<(rocketmq_sre_contracts::ExecutionAction, String), ControlPlaneError> {
        let request_snapshot: Value = sqlx::query_scalar(
            "SELECT request_snapshot
             FROM executions
             WHERE id = $1 AND tenant_id = $2 AND cluster_id = $3",
        )
        .bind(execution_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let execution: rocketmq_sre_contracts::ExecutionRequest =
            serde_json::from_value(request_snapshot).map_err(|_| {
                ControlPlaneError::conflict_code("execution_snapshot_invalid", "execution snapshot is invalid")
            })?;
        execution
            .plan
            .steps
            .into_iter()
            .find(|step| step.id == plan_step_id)
            .map(|step| (step.action, step.resource))
            .ok_or_else(|| {
                ControlPlaneError::forbidden(
                    "grant_binding_mismatch",
                    "requested step is not present in the approved execution snapshot",
                )
            })
    }

    pub(super) async fn unresolved_old_effect_count(
        &self,
        cluster_id: ClusterId,
        pending_epoch: LeaseEpoch,
    ) -> Result<i64, ControlPlaneError> {
        let count = sqlx::query_scalar(
            "SELECT COUNT(*)
             FROM execution_agent_effects
             WHERE cluster_id = $1
               AND epoch < $2
               AND state IN ('prepared', 'dispatched', 'unknown')",
        )
        .bind(cluster_id.as_uuid())
        .bind(epoch_i64(pending_epoch)?)
        .fetch_one(&self.pool)
        .await?;
        Ok(count)
    }
}

fn lease_from_row(row: &sqlx::postgres::PgRow) -> Result<ExecutorLease, ControlPlaneError> {
    let epoch: i64 = row.try_get("epoch")?;
    let state: String = row.try_get("state")?;
    Ok(ExecutorLease {
        id: LeaseId::from_uuid(row.try_get("id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        epoch: parse_epoch(epoch)?,
        owner: row.try_get("owner")?,
        state: match state.as_str() {
            "pending_fence" => LeaseState::PendingFence,
            "active" => LeaseState::Active,
            "expired" => LeaseState::Expired,
            _ => {
                return Err(ControlPlaneError::conflict_code(
                    "lease_state_invalid",
                    "stored lease state is unsupported",
                ));
            }
        },
        pending_nonce: row.try_get("pending_nonce")?,
        acquired_at: row.try_get("acquired_at")?,
        activated_at: row.try_get("activated_at")?,
        expires_at: row.try_get("expires_at")?,
    })
}

fn parse_epoch(value: i64) -> Result<LeaseEpoch, ControlPlaneError> {
    u64::try_from(value)
        .map(LeaseEpoch)
        .map_err(|_| ControlPlaneError::conflict_code("lease_epoch_invalid", "stored lease epoch is invalid"))
}

fn epoch_i64(epoch: LeaseEpoch) -> Result<i64, ControlPlaneError> {
    i64::try_from(epoch.0)
        .map_err(|_| ControlPlaneError::validation("lease_epoch_invalid", "lease epoch exceeds BIGINT"))
}
