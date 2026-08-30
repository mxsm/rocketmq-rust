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
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ResourceLockId;
use rocketmq_sre_contracts::ResourceQuarantine;
use rocketmq_sre_contracts::ResourceQuarantineId;
use rocketmq_sre_contracts::TenantId;
use sqlx::PgPool;
use sqlx::Row;
use uuid::Uuid;

use crate::JournalError;
use crate::error::database_message;
use crate::error::has_database_code;

/// Temporary resource lock request owned by one execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceLockRequest {
    pub id: ResourceLockId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub resource_key: String,
    pub action: ExecutionAction,
    pub holder_execution_id: ExecutionId,
    pub acquired_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// Current temporary lock projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceLock {
    pub id: ResourceLockId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub resource_key: String,
    pub action_id: String,
    pub holder_execution_id: ExecutionId,
    pub acquired_at: DateTime<Utc>,
    pub renewed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub released_at: Option<DateTime<Utc>>,
}

/// PostgreSQL safety boundary for temporary locks and persistent quarantine.
#[derive(Clone, Debug)]
pub struct ResourceSafetyStore {
    pool: PgPool,
}

impl ResourceSafetyStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Acquires a lock after atomically reaping expired locks.
    ///
    /// # Errors
    ///
    /// Rejects invalid windows, active quarantine, and lock contention.
    pub async fn acquire(&self, request: &ResourceLockRequest) -> Result<ResourceLock, JournalError> {
        if request.resource_key.trim().is_empty() || request.expires_at <= request.acquired_at {
            return Err(JournalError::InvalidInput(
                "resource lock requires a target and positive validity window".to_owned(),
            ));
        }
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "UPDATE resource_locks
             SET released_at = expires_at,
                 release_reason = 'expired'
             WHERE released_at IS NULL AND expires_at <= $1",
        )
        .bind(request.acquired_at)
        .execute(&mut *transaction)
        .await?;
        let insert = sqlx::query(
            "INSERT INTO resource_locks (
                id, tenant_id, cluster_id, resource_key, action_id,
                holder_execution_id, acquired_at, renewed_at, expires_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8)",
        )
        .bind(request.id.as_uuid())
        .bind(request.tenant_id.as_uuid())
        .bind(request.cluster_id.as_uuid())
        .bind(&request.resource_key)
        .bind(request.action.id())
        .bind(request.holder_execution_id.as_uuid())
        .bind(request.acquired_at)
        .bind(request.expires_at)
        .execute(&mut *transaction)
        .await;
        match insert {
            Ok(_) => transaction.commit().await?,
            Err(error) if database_message(&error) == Some("resource_quarantined") => {
                return Err(JournalError::ResourceQuarantined);
            }
            Err(error) if database_message(&error) == Some("invalid_resource_lock_scope") => {
                return Err(JournalError::InvalidInput(
                    "resource lock scope does not match its execution".to_owned(),
                ));
            }
            Err(error) if has_database_code(&error, "23505") => {
                return Err(JournalError::ResourceLocked);
            }
            Err(error) => return Err(error.into()),
        }
        Ok(ResourceLock {
            id: request.id,
            tenant_id: request.tenant_id,
            cluster_id: request.cluster_id,
            resource_key: request.resource_key.clone(),
            action_id: request.action.id().to_owned(),
            holder_execution_id: request.holder_execution_id,
            acquired_at: request.acquired_at,
            renewed_at: request.acquired_at,
            expires_at: request.expires_at,
            released_at: None,
        })
    }

    /// Renews a live lock held by the same execution.
    ///
    /// # Errors
    ///
    /// Rejects expired, released, missing, or non-owner locks.
    pub async fn renew(
        &self,
        id: ResourceLockId,
        holder: ExecutionId,
        renewed_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<(), JournalError> {
        if expires_at <= renewed_at {
            return Err(JournalError::InvalidInput(
                "renewed lock expiry must be in the future".to_owned(),
            ));
        }
        let result = sqlx::query(
            "UPDATE resource_locks
             SET renewed_at = $3, expires_at = $4
             WHERE id = $1
               AND holder_execution_id = $2
               AND released_at IS NULL
               AND expires_at > $3",
        )
        .bind(id.as_uuid())
        .bind(holder.as_uuid())
        .bind(renewed_at)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(JournalError::ResourceLocked);
        }
        Ok(())
    }

    /// Releases a lock without removing its history.
    ///
    /// # Errors
    ///
    /// Rejects blank reasons and non-owner/missing locks.
    pub async fn release(
        &self,
        id: ResourceLockId,
        holder: ExecutionId,
        released_at: DateTime<Utc>,
        reason: &str,
    ) -> Result<(), JournalError> {
        if reason.trim().is_empty() {
            return Err(JournalError::InvalidInput(
                "resource lock release requires a reason".to_owned(),
            ));
        }
        let result = sqlx::query(
            "UPDATE resource_locks
             SET released_at = $3, release_reason = $4
             WHERE id = $1 AND holder_execution_id = $2 AND released_at IS NULL",
        )
        .bind(id.as_uuid())
        .bind(holder.as_uuid())
        .bind(released_at)
        .bind(reason)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(JournalError::NotFound);
        }
        Ok(())
    }

    /// Loads the unreleased lock history owned by one execution.
    ///
    /// Recovery releases these records only after a fresh fenced reconcile
    /// proves that every forward effect is absent.
    ///
    /// # Errors
    ///
    /// Returns database failures.
    pub async fn unreleased_for_execution(&self, execution_id: ExecutionId) -> Result<Vec<ResourceLock>, JournalError> {
        let rows = sqlx::query(
            "SELECT id, tenant_id, cluster_id, resource_key, action_id,
                    holder_execution_id, acquired_at, renewed_at, expires_at,
                    released_at
             FROM resource_locks
             WHERE holder_execution_id = $1 AND released_at IS NULL
             ORDER BY acquired_at, id",
        )
        .bind(execution_id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(resource_lock_from_row).collect()
    }

    /// Persists a quarantine that remains after temporary locks are released.
    ///
    /// # Errors
    ///
    /// Rejects cleared/incomplete input and active duplicate quarantine.
    pub async fn quarantine(&self, quarantine: &ResourceQuarantine) -> Result<bool, JournalError> {
        if !quarantine.is_active()
            || quarantine.resource_key.trim().is_empty()
            || quarantine.reason_code.trim().is_empty()
            || quarantine.created_by.trim().is_empty()
        {
            return Err(JournalError::InvalidInput(
                "new quarantine must be active and contain resource, reason, and actor".to_owned(),
            ));
        }
        let insert = sqlx::query(
            "INSERT INTO resource_quarantines (
                id, tenant_id, cluster_id, resource_key, action_id,
                reason_code, source_execution_id, evidence_ids,
                created_by, created_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8,
                $9, $10
             )
             ON CONFLICT DO NOTHING",
        )
        .bind(quarantine.id.as_uuid())
        .bind(quarantine.tenant_id.as_uuid())
        .bind(quarantine.cluster_id.as_uuid())
        .bind(&quarantine.resource_key)
        .bind(&quarantine.action_id)
        .bind(&quarantine.reason_code)
        .bind(quarantine.source_execution_id.map(ExecutionId::as_uuid))
        .bind(
            quarantine
                .evidence_ids
                .iter()
                .copied()
                .map(EvidenceId::as_uuid)
                .collect::<Vec<_>>(),
        )
        .bind(&quarantine.created_by)
        .bind(quarantine.created_at)
        .execute(&self.pool)
        .await;
        let result = match insert {
            Ok(result) => result,
            Err(error) if database_message(&error) == Some("invalid_quarantine_source_scope") => {
                return Err(JournalError::InvalidInput(
                    "quarantine source scope does not match its execution".to_owned(),
                ));
            }
            Err(error) => return Err(error.into()),
        };
        Ok(result.rows_affected() == 1)
    }

    /// Clears quarantine only with an approver, reason, and verification evidence.
    ///
    /// # Errors
    ///
    /// Rejects incomplete evidence or inactive/missing quarantine.
    pub async fn clear_quarantine(
        &self,
        id: ResourceQuarantineId,
        approver: &str,
        reason: &str,
        evidence_ids: &[EvidenceId],
        cleared_at: DateTime<Utc>,
    ) -> Result<(), JournalError> {
        if approver.trim().is_empty() || reason.trim().is_empty() || evidence_ids.is_empty() {
            return Err(JournalError::InvalidInput(
                "quarantine clear requires approver, reason, and verification evidence".to_owned(),
            ));
        }
        let result = sqlx::query(
            "UPDATE resource_quarantines
             SET cleared_by = $2,
                 clear_reason = $3,
                 clear_evidence_ids = $4,
                 cleared_at = $5
             WHERE id = $1 AND cleared_at IS NULL",
        )
        .bind(id.as_uuid())
        .bind(approver)
        .bind(reason)
        .bind(
            evidence_ids
                .iter()
                .copied()
                .map(EvidenceId::as_uuid)
                .collect::<Vec<_>>(),
        )
        .bind(cleared_at)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(JournalError::NotFound);
        }
        Ok(())
    }

    /// Loads one quarantine including its audited clear projection.
    ///
    /// # Errors
    ///
    /// Returns not-found or database errors.
    pub async fn quarantine_by_id(&self, id: ResourceQuarantineId) -> Result<ResourceQuarantine, JournalError> {
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, resource_key, action_id,
                    reason_code, source_execution_id, evidence_ids,
                    created_by, created_at, cleared_by, clear_reason,
                    clear_evidence_ids, cleared_at
             FROM resource_quarantines
             WHERE id = $1",
        )
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(JournalError::NotFound)?;
        Ok(quarantine_from_row(&row)?)
    }
}

fn quarantine_from_row(row: &sqlx::postgres::PgRow) -> Result<ResourceQuarantine, sqlx::Error> {
    Ok(ResourceQuarantine {
        id: ResourceQuarantineId::from_uuid(row.try_get("id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        resource_key: row.try_get("resource_key")?,
        action_id: row.try_get("action_id")?,
        reason_code: row.try_get("reason_code")?,
        source_execution_id: row
            .try_get::<Option<Uuid>, _>("source_execution_id")?
            .map(ExecutionId::from_uuid),
        evidence_ids: row
            .try_get::<Vec<Uuid>, _>("evidence_ids")?
            .into_iter()
            .map(EvidenceId::from_uuid)
            .collect(),
        created_by: row.try_get("created_by")?,
        created_at: row.try_get("created_at")?,
        cleared_by: row.try_get("cleared_by")?,
        clear_reason: row.try_get("clear_reason")?,
        clear_evidence_ids: row
            .try_get::<Vec<Uuid>, _>("clear_evidence_ids")?
            .into_iter()
            .map(EvidenceId::from_uuid)
            .collect(),
        cleared_at: row.try_get("cleared_at")?,
    })
}

fn resource_lock_from_row(row: sqlx::postgres::PgRow) -> Result<ResourceLock, JournalError> {
    Ok(ResourceLock {
        id: ResourceLockId::from_uuid(row.try_get("id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        resource_key: row.try_get("resource_key")?,
        action_id: row.try_get("action_id")?,
        holder_execution_id: ExecutionId::from_uuid(row.try_get("holder_execution_id")?),
        acquired_at: row.try_get("acquired_at")?,
        renewed_at: row.try_get("renewed_at")?,
        expires_at: row.try_get("expires_at")?,
        released_at: row.try_get("released_at")?,
    })
}
