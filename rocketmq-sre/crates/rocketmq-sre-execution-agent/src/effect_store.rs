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
use rocketmq_sre_contracts::AgentStepRequest;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::EffectState;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ExecutionStepId;
use rocketmq_sre_contracts::FenceAck;
use rocketmq_sre_contracts::LeaseEpoch;
use rocketmq_sre_contracts::LeaseId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::is_sha256_digest;
use serde_json::Value;
use sqlx::PgPool;
use sqlx::Row;
use uuid::Uuid;

use crate::AgentStoreError;
use crate::error::database_message;

/// Idempotent Prepared effect result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectCreation {
    pub effect: AgentEffectRecord,
    pub created: bool,
}

/// Durable Agent effect projection recovered across restarts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentEffectRecord {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub execution_id: ExecutionId,
    pub step_id: ExecutionStepId,
    pub lease_id: LeaseId,
    pub epoch: LeaseEpoch,
    pub idempotency_key: String,
    pub action_id: String,
    pub target: String,
    pub state: EffectState,
    pub operation_id: Option<String>,
    pub outcome_code: Option<String>,
    pub sanitized_summary: Option<String>,
    pub prepared_at: DateTime<Utc>,
    pub dispatched_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
}

/// PostgreSQL-backed fence and effect ledger.
///
/// Callers must persist `Dispatched` successfully before invoking a driver. A
/// database error therefore prevents the external write rather than allowing
/// an unjournaled side effect.
#[derive(Clone, Debug)]
pub struct AgentEffectStore {
    pool: PgPool,
}

impl AgentEffectStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Persists a monotonically increasing fence before acknowledging it.
    ///
    /// # Errors
    ///
    /// Rejects missing identity, stale/equal-but-different epochs, and lease
    /// drift.
    pub async fn accept_fence(
        &self,
        tenant_id: TenantId,
        lease_id: LeaseId,
        ack: &FenceAck,
    ) -> Result<bool, AgentStoreError> {
        if ack.epoch.0 == 0
            || ack.pending_nonce.trim().is_empty()
            || ack.agent_subject.trim().is_empty()
            || ack.signature.trim().is_empty()
        {
            return Err(AgentStoreError::InvalidInput(
                "fence acknowledgement identity is incomplete".to_owned(),
            ));
        }
        let mut transaction = self.pool.begin().await?;
        let lease_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1
                FROM executor_leases
                WHERE id = $1
                  AND tenant_id = $2
                  AND cluster_id = $3
                  AND epoch = $4
                  AND pending_nonce = $5
                  AND state IN ('pending_fence', 'active')
                  AND expires_at > $6
            )",
        )
        .bind(lease_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(ack.cluster_id.as_uuid())
        .bind(epoch_i64(ack.epoch)?)
        .bind(&ack.pending_nonce)
        .bind(ack.acknowledged_at)
        .fetch_one(&mut *transaction)
        .await?;
        if !lease_exists {
            return Err(AgentStoreError::FenceRejected);
        }
        let current = sqlx::query(
            "SELECT highest_epoch, lease_id
             FROM execution_agent_fences
             WHERE cluster_id = $1
             FOR UPDATE",
        )
        .bind(ack.cluster_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = current {
            let highest: i64 = row.try_get("highest_epoch")?;
            let current_lease: Uuid = row.try_get("lease_id")?;
            let proposed = epoch_i64(ack.epoch)?;
            if highest > proposed || (highest == proposed && current_lease != lease_id.as_uuid()) {
                return Err(AgentStoreError::FenceRejected);
            }
            if highest == proposed {
                transaction.commit().await?;
                return Ok(false);
            }
        }
        let snapshot = serde_json::to_value(ack).map_err(AgentStoreError::SnapshotEncoding)?;
        sqlx::query(
            "INSERT INTO execution_agent_fences (
                cluster_id, tenant_id, highest_epoch, lease_id, agent_subject,
                fence_ack_snapshot, acknowledged_at, updated_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
             ON CONFLICT (cluster_id) DO UPDATE SET
                tenant_id = EXCLUDED.tenant_id,
                highest_epoch = EXCLUDED.highest_epoch,
                lease_id = EXCLUDED.lease_id,
                agent_subject = EXCLUDED.agent_subject,
                fence_ack_snapshot = EXCLUDED.fence_ack_snapshot,
                acknowledged_at = EXCLUDED.acknowledged_at,
                updated_at = EXCLUDED.updated_at",
        )
        .bind(ack.cluster_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(epoch_i64(ack.epoch)?)
        .bind(lease_id.as_uuid())
        .bind(&ack.agent_subject)
        .bind(snapshot)
        .bind(ack.acknowledged_at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(true)
    }

    /// Writes Prepared before any driver invocation.
    ///
    /// # Errors
    ///
    /// Rejects stale fences and conflicting idempotency keys. A persistence
    /// error means the caller must not invoke a driver.
    pub async fn prepare(
        &self,
        tenant_id: TenantId,
        request: &AgentStepRequest,
        prepared_at: DateTime<Utc>,
    ) -> Result<EffectCreation, AgentStoreError> {
        if request.intent.idempotency_key.trim().is_empty()
            || request.target.trim().is_empty()
            || request.target != request.intent.step.resource
            || request.intent.fence_grant.cluster_id.as_uuid().is_nil()
            || request.intent.fence_grant.owner.trim().is_empty()
            || request.intent.fence_grant.audience != "execution-agent"
            || request.intent.fence_grant.nonce.trim().is_empty()
            || request.intent.fence_grant.signature.trim().is_empty()
            || request.intent.fence_grant.expires_at <= prepared_at
            || request.intent.fence_grant.execution_id != request.intent.execution_id
            || request.intent.fence_grant.step_id != request.intent.step_id
            || request.intent.fence_grant.plan_step_id != request.intent.step.id
            || request.intent.fence_grant.action != request.intent.step.action
            || request.intent.fence_grant.resource != request.intent.step.resource
            || !is_sha256_digest(&request.intent.plan_hash)
            || request.action != request.intent.step.action
            || request.descriptor_version != request.intent.step.descriptor_version
            || request.parameters != request.intent.step.parameters
        {
            return Err(AgentStoreError::InvalidInput(
                "effect request target, action, version, idempotency, or fence is invalid".to_owned(),
            ));
        }
        let effect_id = Uuid::new_v4();
        let snapshot = serde_json::to_value(request).map_err(AgentStoreError::SnapshotEncoding)?;
        let insert = sqlx::query(
            "INSERT INTO execution_agent_effects (
                id, tenant_id, cluster_id, execution_id, step_id, lease_id,
                epoch, idempotency_key, action_id, target, state,
                request_snapshot, prepared_at, updated_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, 'prepared',
                $11, $12, $12
             )
             ON CONFLICT (idempotency_key) DO NOTHING",
        )
        .bind(effect_id)
        .bind(tenant_id.as_uuid())
        .bind(request.intent.fence_grant.cluster_id.as_uuid())
        .bind(request.intent.execution_id.as_uuid())
        .bind(request.intent.step_id.as_uuid())
        .bind(request.intent.fence_grant.lease_id.as_uuid())
        .bind(epoch_i64(request.intent.fence_grant.epoch)?)
        .bind(&request.intent.idempotency_key)
        .bind(request.action.id())
        .bind(&request.target)
        .bind(&snapshot)
        .bind(prepared_at)
        .execute(&self.pool)
        .await;
        let result = match insert {
            Ok(result) => result,
            Err(error) if database_message(&error) == Some("agent_effect_fence_rejected") => {
                return Err(AgentStoreError::FenceRejected);
            }
            Err(error) => return Err(error.into()),
        };
        if result.rows_affected() == 1 {
            return Ok(EffectCreation {
                effect: AgentEffectRecord {
                    id: effect_id,
                    tenant_id,
                    cluster_id: request.intent.fence_grant.cluster_id,
                    execution_id: request.intent.execution_id,
                    step_id: request.intent.step_id,
                    lease_id: request.intent.fence_grant.lease_id,
                    epoch: request.intent.fence_grant.epoch,
                    idempotency_key: request.intent.idempotency_key.clone(),
                    action_id: request.action.id().to_owned(),
                    target: request.target.clone(),
                    state: EffectState::Prepared,
                    operation_id: None,
                    outcome_code: None,
                    sanitized_summary: None,
                    prepared_at,
                    dispatched_at: None,
                    confirmed_at: None,
                },
                created: true,
            });
        }
        let row = sqlx::query(
            "SELECT request_snapshot
             FROM execution_agent_effects
             WHERE idempotency_key = $1",
        )
        .bind(&request.intent.idempotency_key)
        .fetch_one(&self.pool)
        .await?;
        let existing_snapshot: Value = row.try_get("request_snapshot")?;
        if existing_snapshot != snapshot {
            return Err(AgentStoreError::IdempotencyConflict);
        }
        Ok(EffectCreation {
            effect: self.effect(&request.intent.idempotency_key).await?,
            created: false,
        })
    }

    /// Persists Dispatched immediately before the caller sends the driver request.
    ///
    /// # Errors
    ///
    /// Rejects missing operation IDs and non-Prepared effects. On any error the
    /// caller must not send an external write.
    pub async fn mark_dispatched(
        &self,
        idempotency_key: &str,
        operation_id: &str,
        dispatched_at: DateTime<Utc>,
    ) -> Result<(), AgentStoreError> {
        if operation_id.trim().is_empty() {
            return Err(AgentStoreError::InvalidInput(
                "dispatch requires a stable operation id".to_owned(),
            ));
        }
        let result = sqlx::query(
            "UPDATE execution_agent_effects
             SET state = 'dispatched',
                 operation_id = $2,
                 dispatched_at = $3,
                 updated_at = $3
             WHERE idempotency_key = $1 AND state = 'prepared'",
        )
        .bind(idempotency_key)
        .bind(operation_id)
        .bind(dispatched_at)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(AgentStoreError::InvalidTransition);
        }
        Ok(())
    }

    /// Marks an indeterminate effect for read-only live-state reconciliation.
    ///
    /// # Errors
    ///
    /// Rejects terminal/missing effects.
    pub async fn mark_unknown(&self, idempotency_key: &str, updated_at: DateTime<Utc>) -> Result<(), AgentStoreError> {
        let result = sqlx::query(
            "UPDATE execution_agent_effects
             SET state = 'unknown', updated_at = $2
             WHERE idempotency_key = $1 AND state IN ('prepared', 'dispatched')",
        )
        .bind(idempotency_key)
        .bind(updated_at)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(AgentStoreError::InvalidTransition);
        }
        Ok(())
    }

    /// Confirms an effect only after a verifiable response or reconciliation.
    ///
    /// # Errors
    ///
    /// Rejects blank bounded results and effects outside Dispatched/Unknown.
    pub async fn confirm(
        &self,
        idempotency_key: &str,
        outcome_code: &str,
        sanitized_summary: &str,
        confirmed_at: DateTime<Utc>,
    ) -> Result<(), AgentStoreError> {
        if outcome_code.trim().is_empty() || sanitized_summary.trim().is_empty() || sanitized_summary.len() > 2048 {
            return Err(AgentStoreError::InvalidInput(
                "confirmed effect requires a bounded outcome and sanitized summary".to_owned(),
            ));
        }
        let result = sqlx::query(
            "UPDATE execution_agent_effects
             SET state = 'confirmed',
                 outcome_code = $2,
                 sanitized_summary = $3,
                 confirmed_at = $4,
                 updated_at = $4
             WHERE idempotency_key = $1 AND state IN ('dispatched', 'unknown')",
        )
        .bind(idempotency_key)
        .bind(outcome_code)
        .bind(sanitized_summary)
        .bind(confirmed_at)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(AgentStoreError::InvalidTransition);
        }
        Ok(())
    }

    /// Loads a durable effect by idempotency key after restart.
    ///
    /// # Errors
    ///
    /// Returns not-found, database, or invalid stored-state failures.
    pub async fn effect(&self, idempotency_key: &str) -> Result<AgentEffectRecord, AgentStoreError> {
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, execution_id, step_id, lease_id,
                    epoch, idempotency_key, action_id, target, state,
                    operation_id, outcome_code, sanitized_summary,
                    prepared_at, dispatched_at, confirmed_at
             FROM execution_agent_effects
             WHERE idempotency_key = $1",
        )
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(AgentStoreError::NotFound)?;
        effect_from_row(&row)
    }

    /// Loads the immutable typed request snapshot for read-only reconciliation.
    ///
    /// # Errors
    ///
    /// Returns not-found, database, or snapshot decoding failures.
    pub async fn request(&self, idempotency_key: &str) -> Result<AgentStepRequest, AgentStoreError> {
        let snapshot: Value = sqlx::query_scalar(
            "SELECT request_snapshot
             FROM execution_agent_effects
             WHERE idempotency_key = $1",
        )
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(AgentStoreError::NotFound)?;
        serde_json::from_value(snapshot).map_err(AgentStoreError::SnapshotDecoding)
    }

    /// Loads the exact persisted FenceAck for idempotent fence retries.
    ///
    /// # Errors
    ///
    /// Returns not-found, database, or snapshot decoding failures.
    pub async fn fence_ack(&self, cluster_id: ClusterId) -> Result<FenceAck, AgentStoreError> {
        let snapshot: Value = sqlx::query_scalar(
            "SELECT fence_ack_snapshot
             FROM execution_agent_fences
             WHERE cluster_id = $1",
        )
        .bind(cluster_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(AgentStoreError::NotFound)?;
        serde_json::from_value(snapshot).map_err(AgentStoreError::SnapshotDecoding)
    }

    /// Performs the minimal PostgreSQL readiness probe.
    ///
    /// # Errors
    ///
    /// Returns a database error when durable fencing cannot be reached.
    pub async fn ready(&self) -> Result<(), AgentStoreError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    /// Lists non-terminal effects in deterministic recovery order.
    ///
    /// # Errors
    ///
    /// Returns database or invalid stored-state failures.
    pub async fn unfinished(&self, limit: u32) -> Result<Vec<AgentEffectRecord>, AgentStoreError> {
        let rows = sqlx::query(
            "SELECT id, tenant_id, cluster_id, execution_id, step_id, lease_id,
                    epoch, idempotency_key, action_id, target, state,
                    operation_id, outcome_code, sanitized_summary,
                    prepared_at, dispatched_at, confirmed_at
             FROM execution_agent_effects
             WHERE state IN ('prepared', 'dispatched', 'unknown')
             ORDER BY updated_at, id
             LIMIT $1",
        )
        .bind(i64::from(limit.min(10_000)))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(effect_from_row).collect()
    }

    /// Lists old-epoch effects that must become terminal before fence advance.
    ///
    /// # Errors
    ///
    /// Returns database or invalid stored-state failures.
    pub async fn unfinished_before_epoch(
        &self,
        cluster_id: ClusterId,
        pending_epoch: LeaseEpoch,
        limit: u32,
    ) -> Result<Vec<AgentEffectRecord>, AgentStoreError> {
        let rows = sqlx::query(
            "SELECT id, tenant_id, cluster_id, execution_id, step_id, lease_id,
                    epoch, idempotency_key, action_id, target, state,
                    operation_id, outcome_code, sanitized_summary,
                    prepared_at, dispatched_at, confirmed_at
             FROM execution_agent_effects
             WHERE cluster_id = $1
               AND epoch < $2
               AND state IN ('prepared', 'dispatched', 'unknown')
             ORDER BY updated_at, id
             LIMIT $3",
        )
        .bind(cluster_id.as_uuid())
        .bind(epoch_i64(pending_epoch)?)
        .bind(i64::from(limit.min(10_000)))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(effect_from_row).collect()
    }

    /// Returns the highest durable Agent epoch for a cluster.
    ///
    /// # Errors
    ///
    /// Returns database or invalid epoch failures.
    pub async fn highest_epoch(&self, cluster_id: ClusterId) -> Result<Option<LeaseEpoch>, AgentStoreError> {
        let epoch: Option<i64> = sqlx::query_scalar(
            "SELECT highest_epoch
             FROM execution_agent_fences
             WHERE cluster_id = $1",
        )
        .bind(cluster_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        epoch
            .map(|value| {
                u64::try_from(value)
                    .map(LeaseEpoch)
                    .map_err(|_| AgentStoreError::InvalidInput("stored fence epoch is negative".to_owned()))
            })
            .transpose()
    }
}

fn effect_from_row(row: &sqlx::postgres::PgRow) -> Result<AgentEffectRecord, AgentStoreError> {
    let epoch: i64 = row.try_get("epoch")?;
    let state: String = row.try_get("state")?;
    Ok(AgentEffectRecord {
        id: row.try_get("id")?,
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        execution_id: ExecutionId::from_uuid(row.try_get("execution_id")?),
        step_id: ExecutionStepId::from_uuid(row.try_get("step_id")?),
        lease_id: LeaseId::from_uuid(row.try_get("lease_id")?),
        epoch: LeaseEpoch(
            u64::try_from(epoch)
                .map_err(|_| AgentStoreError::InvalidInput("stored effect epoch is negative".to_owned()))?,
        ),
        idempotency_key: row.try_get("idempotency_key")?,
        action_id: row.try_get("action_id")?,
        target: row.try_get("target")?,
        state: parse_effect_state(&state)?,
        operation_id: row.try_get("operation_id")?,
        outcome_code: row.try_get("outcome_code")?,
        sanitized_summary: row.try_get("sanitized_summary")?,
        prepared_at: row.try_get("prepared_at")?,
        dispatched_at: row.try_get("dispatched_at")?,
        confirmed_at: row.try_get("confirmed_at")?,
    })
}

fn epoch_i64(epoch: LeaseEpoch) -> Result<i64, AgentStoreError> {
    i64::try_from(epoch.0).map_err(|_| AgentStoreError::InvalidInput("fence epoch exceeds BIGINT".to_owned()))
}

fn parse_effect_state(value: &str) -> Result<EffectState, AgentStoreError> {
    match value {
        "prepared" => Ok(EffectState::Prepared),
        "dispatched" => Ok(EffectState::Dispatched),
        "confirmed" => Ok(EffectState::Confirmed),
        "unknown" => Ok(EffectState::Unknown),
        _ => Err(AgentStoreError::InvalidInput(
            "stored effect state is unsupported".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effect_state_decoder_fails_closed() {
        assert_eq!(
            parse_effect_state("dispatched").expect("known state"),
            EffectState::Dispatched
        );
        assert!(parse_effect_state("sent").is_err());
    }
}
