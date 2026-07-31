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
use rocketmq_sre_contracts::FenceAck;
use rocketmq_sre_contracts::LeaseEpoch;
use rocketmq_sre_contracts::LeaseId;
use rocketmq_sre_contracts::LeaseState;
use rocketmq_sre_contracts::TenantId;
use sqlx::PgPool;
use sqlx::Row;

use crate::JournalError;

/// Durable lease generation used to fence competing Executor instances.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorLeaseRecord {
    pub id: LeaseId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub epoch: LeaseEpoch,
    pub owner: String,
    pub state: LeaseState,
    pub pending_nonce: String,
    pub acquired_at: DateTime<Utc>,
    pub activated_at: Option<DateTime<Utc>>,
    pub expires_at: DateTime<Utc>,
}

/// Two-phase PostgreSQL lease and fencing coordinator.
#[derive(Clone, Debug)]
pub struct LeaseCoordinator {
    pool: PgPool,
}

impl LeaseCoordinator {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Creates a new monotonically increasing PendingFence generation.
    ///
    /// An advisory transaction lock serializes takeover per cluster. Existing
    /// active/pending generations are made Expired before the new row is
    /// inserted.
    ///
    /// # Errors
    ///
    /// Rejects invalid identity/window data or unavailable persistence.
    pub async fn begin_takeover(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        owner: &str,
        pending_nonce: &str,
        acquired_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<ExecutorLeaseRecord, JournalError> {
        if owner.trim().is_empty() || pending_nonce.trim().is_empty() || expires_at <= acquired_at {
            return Err(JournalError::InvalidInput(
                "lease takeover requires owner, nonce, and positive validity window".to_owned(),
            ));
        }
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(cluster_id.to_string())
            .execute(&mut *transaction)
            .await?;
        let cluster_tenant: Option<String> = sqlx::query_scalar("SELECT tenant_id FROM clusters WHERE id = $1")
            .bind(cluster_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await?;
        let expected_tenant = tenant_id.to_string();
        if cluster_tenant.as_deref() != Some(expected_tenant.as_str()) {
            return Err(JournalError::InvalidInput(
                "lease tenant does not own the target cluster".to_owned(),
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
        let epoch = previous_epoch
            .checked_add(1)
            .ok_or_else(|| JournalError::InvalidInput("lease epoch exhausted BIGINT".to_owned()))?;
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
        Ok(ExecutorLeaseRecord {
            id,
            tenant_id,
            cluster_id,
            epoch: LeaseEpoch(
                u64::try_from(epoch)
                    .map_err(|_| JournalError::InvalidInput("database returned a negative lease epoch".to_owned()))?,
            ),
            owner: owner.to_owned(),
            state: LeaseState::PendingFence,
            pending_nonce: pending_nonce.to_owned(),
            acquired_at,
            activated_at: None,
            expires_at,
        })
    }

    /// Activates a pending generation after a durable Agent FenceAck.
    ///
    /// # Errors
    ///
    /// Rejects stale epochs, nonce/cluster drift, expired leases, and
    /// incomplete acknowledgements.
    pub async fn activate(
        &self,
        lease: &ExecutorLeaseRecord,
        ack: &FenceAck,
    ) -> Result<ExecutorLeaseRecord, JournalError> {
        if lease.state != LeaseState::PendingFence
            || ack.cluster_id != lease.cluster_id
            || ack.epoch != lease.epoch
            || ack.pending_nonce != lease.pending_nonce
            || ack.agent_subject.trim().is_empty()
            || ack.signature.trim().is_empty()
        {
            return Err(JournalError::LeaseRejected);
        }
        let snapshot = serde_json::to_value(ack).map_err(JournalError::SnapshotEncoding)?;
        let result = sqlx::query(
            "UPDATE executor_leases
             SET state = 'active',
                 fence_ack_snapshot = $2,
                 activated_at = $3,
                 updated_at = $3
             WHERE id = $1
               AND state = 'pending_fence'
               AND epoch = $4
               AND pending_nonce = $5
               AND expires_at > $3
               AND EXISTS (
                   SELECT 1
                   FROM execution_agent_fences fence
                   WHERE fence.cluster_id = $6
                     AND fence.lease_id = $1
                     AND fence.highest_epoch = $4
                     AND fence.fence_ack_snapshot = $2
               )",
        )
        .bind(lease.id.as_uuid())
        .bind(&snapshot)
        .bind(ack.acknowledged_at)
        .bind(epoch_i64(lease.epoch)?)
        .bind(&lease.pending_nonce)
        .bind(lease.cluster_id.as_uuid())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(JournalError::LeaseRejected);
        }
        let mut active = lease.clone();
        active.state = LeaseState::Active;
        active.activated_at = Some(ack.acknowledged_at);
        Ok(active)
    }

    /// Expires one current generation without deleting history.
    ///
    /// # Errors
    ///
    /// Returns not-found or persistence failures.
    pub async fn expire(&self, id: LeaseId, expired_at: DateTime<Utc>) -> Result<(), JournalError> {
        let result = sqlx::query(
            "UPDATE executor_leases
             SET state = 'expired',
                 expires_at = LEAST(expires_at, $2),
                 updated_at = $2
             WHERE id = $1 AND state IN ('pending_fence', 'active')",
        )
        .bind(id.as_uuid())
        .bind(expired_at)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(JournalError::NotFound);
        }
        Ok(())
    }

    /// Loads an exact durable lease generation after process restart.
    ///
    /// # Errors
    ///
    /// Returns not-found, database, or invalid stored-state failures.
    pub async fn lease(&self, id: LeaseId) -> Result<ExecutorLeaseRecord, JournalError> {
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, epoch, owner, state,
                    pending_nonce, acquired_at, activated_at, expires_at
             FROM executor_leases
             WHERE id = $1",
        )
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(JournalError::NotFound)?;
        lease_from_row(&row)
    }

    /// Returns the highest durable epoch for a cluster.
    ///
    /// # Errors
    ///
    /// Returns a database or invalid epoch failure.
    pub async fn highest_epoch(&self, cluster_id: ClusterId) -> Result<Option<LeaseEpoch>, JournalError> {
        let epoch: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(epoch)
             FROM executor_leases
             WHERE cluster_id = $1",
        )
        .bind(cluster_id.as_uuid())
        .fetch_one(&self.pool)
        .await?;
        epoch
            .map(|value| {
                u64::try_from(value)
                    .map(LeaseEpoch)
                    .map_err(|_| JournalError::InvalidInput("stored lease epoch is negative".to_owned()))
            })
            .transpose()
    }
}

fn lease_from_row(row: &sqlx::postgres::PgRow) -> Result<ExecutorLeaseRecord, JournalError> {
    let epoch: i64 = row.try_get("epoch")?;
    let state: String = row.try_get("state")?;
    Ok(ExecutorLeaseRecord {
        id: LeaseId::from_uuid(row.try_get("id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        epoch: LeaseEpoch(
            u64::try_from(epoch)
                .map_err(|_| JournalError::InvalidInput("stored lease epoch is negative".to_owned()))?,
        ),
        owner: row.try_get("owner")?,
        state: parse_lease_state(&state)?,
        pending_nonce: row.try_get("pending_nonce")?,
        acquired_at: row.try_get("acquired_at")?,
        activated_at: row.try_get("activated_at")?,
        expires_at: row.try_get("expires_at")?,
    })
}

fn epoch_i64(epoch: LeaseEpoch) -> Result<i64, JournalError> {
    i64::try_from(epoch.0).map_err(|_| JournalError::InvalidInput("lease epoch exceeds BIGINT".to_owned()))
}

fn parse_lease_state(value: &str) -> Result<LeaseState, JournalError> {
    match value {
        "pending_fence" => Ok(LeaseState::PendingFence),
        "active" => Ok(LeaseState::Active),
        "expired" => Ok(LeaseState::Expired),
        _ => Err(JournalError::InvalidInput(
            "stored lease state is unsupported".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_state_decoder_fails_closed() {
        assert_eq!(
            parse_lease_state("pending_fence").expect("known state"),
            LeaseState::PendingFence
        );
        assert!(parse_lease_state("leader").is_err());
    }
}
