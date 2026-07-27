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
use rocketmq_sre_contracts::AuditEvent;
use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ExecutionRequest;
use rocketmq_sre_contracts::ExecutionState;
use rocketmq_sre_contracts::ExecutionTransition;
use rocketmq_sre_contracts::StepIntent;
use rocketmq_sre_contracts::StepResult;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::PgPool;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use uuid::Uuid;

use crate::JournalError;
use crate::error::database_message;

/// Idempotent execution creation result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionCreation {
    pub id: ExecutionId,
    pub created: bool,
}

/// Durable intent without a matching append-only result.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingIntent {
    pub intent: StepIntent,
    pub execution_state: ExecutionState,
}

/// PostgreSQL journal used before and after every external side effect.
#[derive(Clone, Debug)]
pub struct ExecutionJournal {
    pool: PgPool,
    expected_audience: String,
}

impl ExecutionJournal {
    #[must_use]
    pub fn new(pool: PgPool, expected_audience: impl Into<String>) -> Self {
        Self {
            pool,
            expected_audience: expected_audience.into(),
        }
    }

    /// Creates one immutable execution request or returns an identical retry.
    ///
    /// # Errors
    ///
    /// Rejects invalid plans, action drift, and reuse of an idempotency key for
    /// a different request.
    pub async fn create_execution(
        &self,
        request: &ExecutionRequest,
        resource_key: &str,
        action: ExecutionAction,
        started_at: DateTime<Utc>,
    ) -> Result<ExecutionCreation, JournalError> {
        if self.expected_audience.trim().is_empty() {
            return Err(JournalError::InvalidInput(
                "executor audience must be configured".to_owned(),
            ));
        }
        request
            .validate_at(started_at, &self.expected_audience)
            .map_err(|error| JournalError::InvalidInput(error.to_string()))?;
        if resource_key.trim().is_empty() || !request.plan.steps.iter().any(|step| step.action == action) {
            return Err(JournalError::InvalidInput(
                "execution resource and action must match the approved plan".to_owned(),
            ));
        }
        let snapshot = json_value(request)?;
        let result = sqlx::query(
            "INSERT INTO executions (
                id, tenant_id, cluster_id, correlation_id, plan_id, plan_hash,
                resource_key, action_id, idempotency_key, state,
                request_snapshot, requested_by, started_at, updated_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, 'pending',
                $10, $11, $12, $12
             )
             ON CONFLICT (idempotency_key) DO NOTHING",
        )
        .bind(request.id.as_uuid())
        .bind(request.tenant_id.as_uuid())
        .bind(request.cluster_id.as_uuid())
        .bind(request.correlation_id.as_uuid())
        .bind(request.plan.id.as_uuid())
        .bind(&request.plan.plan_hash)
        .bind(resource_key)
        .bind(action.id())
        .bind(&request.idempotency_key)
        .bind(&snapshot)
        .bind(&request.requested_by)
        .bind(started_at)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 1 {
            return Ok(ExecutionCreation {
                id: request.id,
                created: true,
            });
        }
        let row = sqlx::query(
            "SELECT id, request_snapshot
             FROM executions
             WHERE idempotency_key = $1",
        )
        .bind(&request.idempotency_key)
        .fetch_one(&self.pool)
        .await?;
        let existing_snapshot: Value = row.try_get("request_snapshot")?;
        if existing_snapshot != snapshot {
            return Err(JournalError::IdempotencyConflict);
        }
        Ok(ExecutionCreation {
            id: ExecutionId::from_uuid(row.try_get("id")?),
            created: false,
        })
    }

    /// Applies one compare-and-set execution state transition.
    ///
    /// # Errors
    ///
    /// Rejects illegal graph edges and stale current states.
    pub async fn transition(&self, id: ExecutionId, transition: &ExecutionTransition) -> Result<bool, JournalError> {
        transition
            .validate()
            .map_err(|error| JournalError::InvalidInput(error.to_string()))?;
        let completed_at = if matches!(
            transition.to,
            ExecutionState::Succeeded | ExecutionState::RolledBack | ExecutionState::Escalated
        ) {
            Some(transition.occurred_at)
        } else {
            None
        };
        let result = sqlx::query(
            "UPDATE executions
             SET state = $3,
                 completed_at = COALESCE(completed_at, $4),
                 updated_at = $5
             WHERE id = $1 AND state = $2",
        )
        .bind(id.as_uuid())
        .bind(execution_state_name(transition.from))
        .bind(execution_state_name(transition.to))
        .bind(completed_at)
        .bind(transition.occurred_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Atomically appends a StepIntent and its audit event before dispatch.
    ///
    /// # Errors
    ///
    /// Rejects inactive/stale lease epochs, scope drift, and non-identical
    /// duplicate intents.
    pub async fn append_intent_with_audit(
        &self,
        intent: &StepIntent,
        audit: &AuditEvent,
    ) -> Result<bool, JournalError> {
        if audit.event_kind != AuditEventKind::StepIntentPersisted {
            return Err(JournalError::InvalidInput(
                "intent transaction requires a step_intent_persisted audit event".to_owned(),
            ));
        }
        let mut transaction = self.pool.begin().await?;
        ensure_execution_scope(&mut transaction, intent.execution_id, audit).await?;
        let snapshot = json_value(intent)?;
        let insert = sqlx::query(
            "INSERT INTO execution_steps (
                execution_id, step_id, attempt, record_kind, lease_id,
                lease_epoch, compensation, intent_snapshot, result_snapshot,
                reason_code, occurred_at
             ) VALUES (
                $1, $2, $3, 'intent', $4,
                $5, $6, $7, NULL,
                'step_intent_persisted', $8
             )
             ON CONFLICT (execution_id, step_id, attempt, record_kind)
             DO NOTHING",
        )
        .bind(intent.execution_id.as_uuid())
        .bind(intent.step_id.as_uuid())
        .bind(i32::from(intent.attempt))
        .bind(intent.fence_grant.lease_id.as_uuid())
        .bind(epoch_i64(intent.fence_grant.epoch.0)?)
        .bind(intent.compensation)
        .bind(&snapshot)
        .bind(intent.intended_at)
        .execute(&mut *transaction)
        .await;
        let result = match insert {
            Ok(result) => result,
            Err(error) if database_message(&error) == Some("invalid_executor_lease") => {
                return Err(JournalError::LeaseRejected);
            }
            Err(error) => return Err(error.into()),
        };
        if result.rows_affected() == 0 {
            let existing: Value = sqlx::query_scalar(
                "SELECT intent_snapshot
                 FROM execution_steps
                 WHERE execution_id = $1
                   AND step_id = $2
                   AND attempt = $3
                   AND record_kind = 'intent'",
            )
            .bind(intent.execution_id.as_uuid())
            .bind(intent.step_id.as_uuid())
            .bind(i32::from(intent.attempt))
            .fetch_one(&mut *transaction)
            .await?;
            if existing != snapshot {
                return Err(JournalError::IdempotencyConflict);
            }
        }
        append_audit(&mut transaction, audit).await?;
        transaction.commit().await?;
        Ok(result.rows_affected() == 1)
    }

    /// Appends the immutable result and audit event after an Agent response.
    ///
    /// # Errors
    ///
    /// Rejects scope drift and non-identical duplicate results.
    pub async fn append_result_with_audit(
        &self,
        execution_id: ExecutionId,
        attempt: u16,
        result: &StepResult,
        audit: &AuditEvent,
    ) -> Result<bool, JournalError> {
        if audit.event_kind != AuditEventKind::StepResultPersisted {
            return Err(JournalError::InvalidInput(
                "result transaction requires a step_result_persisted audit event".to_owned(),
            ));
        }
        let mut transaction = self.pool.begin().await?;
        ensure_execution_scope(&mut transaction, execution_id, audit).await?;
        let snapshot = json_value(result)?;
        let insert = sqlx::query(
            "INSERT INTO execution_steps (
                execution_id, step_id, attempt, record_kind, lease_id,
                lease_epoch, compensation, intent_snapshot, result_snapshot,
                reason_code, occurred_at
             ) VALUES (
                $1, $2, $3, 'result', NULL,
                NULL, FALSE, NULL, $4,
                $5, $6
             )
             ON CONFLICT (execution_id, step_id, attempt, record_kind)
             DO NOTHING",
        )
        .bind(execution_id.as_uuid())
        .bind(result.step_id.as_uuid())
        .bind(i32::from(attempt))
        .bind(&snapshot)
        .bind(&result.reason_code)
        .bind(result.completed_at)
        .execute(&mut *transaction)
        .await?;
        if insert.rows_affected() == 0 {
            let existing: Value = sqlx::query_scalar(
                "SELECT result_snapshot
                 FROM execution_steps
                 WHERE execution_id = $1
                   AND step_id = $2
                   AND attempt = $3
                   AND record_kind = 'result'",
            )
            .bind(execution_id.as_uuid())
            .bind(result.step_id.as_uuid())
            .bind(i32::from(attempt))
            .fetch_one(&mut *transaction)
            .await?;
            if existing != snapshot {
                return Err(JournalError::IdempotencyConflict);
            }
        }
        append_audit(&mut transaction, audit).await?;
        transaction.commit().await?;
        Ok(insert.rows_affected() == 1)
    }

    /// Returns durable intents that have no matching result after a restart.
    ///
    /// # Errors
    ///
    /// Returns a database or snapshot decoding failure.
    pub async fn pending_intents(&self, limit: u32) -> Result<Vec<PendingIntent>, JournalError> {
        let rows = sqlx::query(
            "SELECT intent.intent_snapshot, execution.state
             FROM execution_steps intent
             JOIN executions execution ON execution.id = intent.execution_id
             LEFT JOIN execution_steps result
               ON result.execution_id = intent.execution_id
              AND result.step_id = intent.step_id
              AND result.attempt = intent.attempt
              AND result.record_kind = 'result'
             WHERE intent.record_kind = 'intent'
               AND result.sequence_id IS NULL
               AND execution.state NOT IN ('succeeded', 'rolled_back', 'escalated')
             ORDER BY intent.sequence_id
             LIMIT $1",
        )
        .bind(i64::from(limit.min(10_000)))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(PendingIntent {
                    intent: from_json(row.try_get("intent_snapshot")?)?,
                    execution_state: parse_execution_state(row.try_get::<String, _>("state")?.as_str())?,
                })
            })
            .collect()
    }

    /// Returns the current durable execution state.
    ///
    /// # Errors
    ///
    /// Returns not-found, database, or unknown-state failures.
    pub async fn execution_state(&self, id: ExecutionId) -> Result<ExecutionState, JournalError> {
        let state: Option<String> = sqlx::query_scalar("SELECT state FROM executions WHERE id = $1")
            .bind(id.as_uuid())
            .fetch_optional(&self.pool)
            .await?;
        parse_execution_state(state.ok_or(JournalError::NotFound)?.as_str())
    }
}

async fn ensure_execution_scope(
    transaction: &mut Transaction<'_, Postgres>,
    execution_id: ExecutionId,
    audit: &AuditEvent,
) -> Result<(), JournalError> {
    let row = sqlx::query(
        "SELECT tenant_id, cluster_id, correlation_id
         FROM executions
         WHERE id = $1",
    )
    .bind(execution_id.as_uuid())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(JournalError::NotFound)?;
    let tenant_id: Uuid = row.try_get("tenant_id")?;
    let cluster_id: Uuid = row.try_get("cluster_id")?;
    let correlation_id: Uuid = row.try_get("correlation_id")?;
    if tenant_id != audit.tenant_id.as_uuid()
        || cluster_id != audit.cluster_id.as_uuid()
        || correlation_id != audit.correlation_id.as_uuid()
    {
        return Err(JournalError::InvalidInput(
            "audit event scope does not match execution".to_owned(),
        ));
    }
    Ok(())
}

async fn append_audit(transaction: &mut Transaction<'_, Postgres>, event: &AuditEvent) -> Result<(), JournalError> {
    let snapshot = json_value(event)?;
    let result = sqlx::query(
        "INSERT INTO audit_events (
            event_id, tenant_id, cluster_id, correlation_id, event_kind,
            actor_subject, actor_role, resource_kind, resource_id,
            reason_code, details, event_snapshot, occurred_at
         ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9,
            $10, $11, $12, $13
         )
         ON CONFLICT (event_id) DO NOTHING",
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
    .bind(&snapshot)
    .bind(event.occurred_at)
    .execute(&mut **transaction)
    .await?;
    if result.rows_affected() == 0 {
        let existing: Value = sqlx::query_scalar(
            "SELECT event_snapshot
             FROM audit_events
             WHERE event_id = $1",
        )
        .bind(event.id.as_uuid())
        .fetch_one(&mut **transaction)
        .await?;
        if existing != snapshot {
            return Err(JournalError::IdempotencyConflict);
        }
    }
    Ok(())
}

fn json_value(value: &impl Serialize) -> Result<Value, JournalError> {
    serde_json::to_value(value).map_err(JournalError::SnapshotEncoding)
}

fn from_json<T: DeserializeOwned>(value: Value) -> Result<T, JournalError> {
    serde_json::from_value(value).map_err(JournalError::SnapshotDecoding)
}

fn enum_name(value: &impl Serialize) -> Result<String, JournalError> {
    json_value(value)?
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| JournalError::InvalidInput("enum did not encode as a string".to_owned()))
}

fn epoch_i64(epoch: u64) -> Result<i64, JournalError> {
    i64::try_from(epoch).map_err(|_| JournalError::InvalidInput("lease epoch exceeds BIGINT".to_owned()))
}

const fn execution_state_name(state: ExecutionState) -> &'static str {
    match state {
        ExecutionState::Pending => "pending",
        ExecutionState::Prechecking => "prechecking",
        ExecutionState::IntentPersisted => "intent_persisted",
        ExecutionState::Applying => "applying",
        ExecutionState::Unknown => "unknown",
        ExecutionState::Reconciling => "reconciling",
        ExecutionState::Verifying => "verifying",
        ExecutionState::Compensating => "compensating",
        ExecutionState::Succeeded => "succeeded",
        ExecutionState::RolledBack => "rolled_back",
        ExecutionState::Escalated => "escalated",
    }
}

fn parse_execution_state(value: &str) -> Result<ExecutionState, JournalError> {
    match value {
        "pending" => Ok(ExecutionState::Pending),
        "prechecking" => Ok(ExecutionState::Prechecking),
        "intent_persisted" => Ok(ExecutionState::IntentPersisted),
        "applying" => Ok(ExecutionState::Applying),
        "unknown" => Ok(ExecutionState::Unknown),
        "reconciling" => Ok(ExecutionState::Reconciling),
        "verifying" => Ok(ExecutionState::Verifying),
        "compensating" => Ok(ExecutionState::Compensating),
        "succeeded" => Ok(ExecutionState::Succeeded),
        "rolled_back" => Ok(ExecutionState::RolledBack),
        "escalated" => Ok(ExecutionState::Escalated),
        _ => Err(JournalError::InvalidInput(
            "stored execution state is unsupported".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_encoding_matches_database_constraints() {
        assert_eq!(
            execution_state_name(ExecutionState::IntentPersisted),
            "intent_persisted"
        );
        assert_eq!(
            parse_execution_state("reconciling").expect("known state"),
            ExecutionState::Reconciling
        );
    }

    #[test]
    fn unknown_state_fails_closed() {
        assert!(parse_execution_state("running").is_err());
    }
}
