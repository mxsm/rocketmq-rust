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
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ExecutionRequest;
use rocketmq_sre_contracts::ExecutionState;
use rocketmq_sre_contracts::ExecutionTransition;
use rocketmq_sre_contracts::NotificationDeliveryId;
use rocketmq_sre_contracts::ResourceQuarantine;
use rocketmq_sre_contracts::StepIntent;
use rocketmq_sre_contracts::StepResult;
use rocketmq_sre_contracts::VerificationResult;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::PgPool;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use uuid::Uuid;

use crate::JournalError;
use crate::VerificationPhase;
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
    pub tenant_id: rocketmq_sre_contracts::TenantId,
    pub cluster_id: rocketmq_sre_contracts::ClusterId,
    pub correlation_id: rocketmq_sre_contracts::CorrelationId,
}

/// Immutable verification Evidence projection loaded from the journal.
#[derive(Clone, Debug, PartialEq)]
pub struct VerificationEvidenceRecord {
    pub execution_id: ExecutionId,
    pub step_id: rocketmq_sre_contracts::ExecutionStepId,
    pub attempt: u16,
    pub phase: VerificationPhase,
    pub evidence: EvidenceSnapshot,
}

/// Result of an atomic rollback-failure escalation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManualTakeoverEscalation {
    pub escalated: bool,
    pub quarantine_created: bool,
    pub notifications_enqueued: u64,
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

    /// Checks the PostgreSQL dependency used for readiness.
    ///
    /// # Errors
    ///
    /// Returns the database failure without exposing connection details.
    pub async fn ready(&self) -> Result<(), JournalError> {
        sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&self.pool).await?;
        Ok(())
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

    /// Applies a state transition and its append-only audit in one database
    /// transaction.
    ///
    /// # Errors
    ///
    /// Rejects illegal graph edges, stale current state, audit scope drift,
    /// and non-state-change audit kinds.
    pub async fn transition_with_audit(
        &self,
        id: ExecutionId,
        transition: &ExecutionTransition,
        audit: &AuditEvent,
    ) -> Result<bool, JournalError> {
        if audit.event_kind != AuditEventKind::StateChanged {
            return Err(JournalError::InvalidInput(
                "execution transition requires a state_changed audit event".to_owned(),
            ));
        }
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
        let mut transaction = self.pool.begin().await?;
        ensure_execution_scope(&mut transaction, id, audit).await?;
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
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 1 {
            append_audit(&mut transaction, audit).await?;
        }
        transaction.commit().await?;
        Ok(result.rows_affected() == 1)
    }

    /// Appends one execution-scoped audit event without changing state.
    ///
    /// # Errors
    ///
    /// Rejects scope drift, duplicate IDs with different content, and
    /// database failures.
    pub async fn append_audit_event(&self, execution_id: ExecutionId, audit: &AuditEvent) -> Result<(), JournalError> {
        let mut transaction = self.pool.begin().await?;
        ensure_execution_scope(&mut transaction, execution_id, audit).await?;
        append_audit(&mut transaction, audit).await?;
        transaction.commit().await?;
        Ok(())
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

    /// Persists one immutable pre/during/post verification snapshot.
    ///
    /// # Errors
    ///
    /// Rejects malformed Evidence, execution scope drift, invalid attempts,
    /// and non-identical duplicate IDs.
    pub async fn append_verification_evidence(
        &self,
        execution_id: ExecutionId,
        step_id: rocketmq_sre_contracts::ExecutionStepId,
        attempt: u16,
        phase: VerificationPhase,
        evidence: &EvidenceSnapshot,
    ) -> Result<bool, JournalError> {
        self.append_verification_evidence_internal(execution_id, step_id, attempt, phase, evidence, None)
            .await
    }

    /// Persists one immutable verification snapshot and its capture audit in
    /// the same transaction.
    ///
    /// # Errors
    ///
    /// Rejects malformed Evidence, execution scope drift, invalid attempts,
    /// wrong audit kinds, and non-identical duplicate IDs.
    pub async fn append_verification_evidence_with_audit(
        &self,
        execution_id: ExecutionId,
        step_id: rocketmq_sre_contracts::ExecutionStepId,
        attempt: u16,
        phase: VerificationPhase,
        evidence: &EvidenceSnapshot,
        audit: &AuditEvent,
    ) -> Result<bool, JournalError> {
        if audit.event_kind != AuditEventKind::VerificationCaptured {
            return Err(JournalError::InvalidInput(
                "verification Evidence requires a verification_captured audit".to_owned(),
            ));
        }
        self.append_verification_evidence_internal(execution_id, step_id, attempt, phase, evidence, Some(audit))
            .await
    }

    async fn append_verification_evidence_internal(
        &self,
        execution_id: ExecutionId,
        step_id: rocketmq_sre_contracts::ExecutionStepId,
        attempt: u16,
        phase: VerificationPhase,
        evidence: &EvidenceSnapshot,
        audit: Option<&AuditEvent>,
    ) -> Result<bool, JournalError> {
        if attempt == 0 {
            return Err(JournalError::InvalidInput(
                "verification Evidence attempt must be positive".to_owned(),
            ));
        }
        evidence
            .verify_content_hash()
            .map_err(|error| JournalError::InvalidInput(error.to_string()))?;
        let mut transaction = self.pool.begin().await?;
        if let Some(audit) = audit {
            ensure_execution_scope(&mut transaction, execution_id, audit).await?;
        }
        let scope = sqlx::query(
            "SELECT tenant_id, cluster_id, correlation_id
             FROM executions
             WHERE id = $1",
        )
        .bind(execution_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(JournalError::NotFound)?;
        if scope.try_get::<Uuid, _>("tenant_id")? != evidence.tenant_id.as_uuid()
            || scope.try_get::<Uuid, _>("cluster_id")? != evidence.cluster_id.as_uuid()
            || scope.try_get::<Uuid, _>("correlation_id")? != evidence.correlation_id.as_uuid()
        {
            return Err(JournalError::InvalidInput(
                "verification Evidence scope does not match execution".to_owned(),
            ));
        }
        let snapshot = json_value(evidence)?;
        let insert = sqlx::query(
            "INSERT INTO execution_verification_evidence (
                execution_id, step_id, attempt, phase,
                evidence_id, evidence_snapshot, observed_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (execution_id, step_id, attempt, phase, evidence_id)
             DO NOTHING",
        )
        .bind(execution_id.as_uuid())
        .bind(step_id.as_uuid())
        .bind(i32::from(attempt))
        .bind(verification_phase_name(phase))
        .bind(evidence.evidence_id.as_uuid())
        .bind(&snapshot)
        .bind(evidence.observed_at)
        .execute(&mut *transaction)
        .await?;
        if insert.rows_affected() == 0 {
            let existing: Value = sqlx::query_scalar(
                "SELECT evidence_snapshot
                 FROM execution_verification_evidence
                 WHERE execution_id = $1
                   AND step_id = $2
                   AND attempt = $3
                   AND phase = $4
                   AND evidence_id = $5",
            )
            .bind(execution_id.as_uuid())
            .bind(step_id.as_uuid())
            .bind(i32::from(attempt))
            .bind(verification_phase_name(phase))
            .bind(evidence.evidence_id.as_uuid())
            .fetch_one(&mut *transaction)
            .await?;
            if existing != snapshot {
                return Err(JournalError::IdempotencyConflict);
            }
        }
        if let Some(audit) = audit {
            append_audit(&mut transaction, audit).await?;
        }
        transaction.commit().await?;
        Ok(insert.rows_affected() == 1)
    }

    /// Persists one deterministic verification decision and timeline audit.
    ///
    /// # Errors
    ///
    /// Rejects scope drift, wrong audit kind, invalid attempts, and
    /// non-identical duplicate results.
    pub async fn append_verification_result_with_audit(
        &self,
        execution_id: ExecutionId,
        attempt: u16,
        compensation: bool,
        result: &VerificationResult,
        audit: &AuditEvent,
    ) -> Result<bool, JournalError> {
        if attempt == 0 || audit.event_kind != AuditEventKind::VerificationCompleted {
            return Err(JournalError::InvalidInput(
                "verification result requires a positive attempt and verification_completed audit".to_owned(),
            ));
        }
        let mut transaction = self.pool.begin().await?;
        ensure_execution_scope(&mut transaction, execution_id, audit).await?;
        let snapshot = json_value(result)?;
        let outcome = enum_name(&result.outcome)?;
        let insert = sqlx::query(
            "INSERT INTO execution_verifications (
                execution_id, step_id, attempt, compensation,
                outcome, result_snapshot, started_at, completed_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (execution_id, step_id, attempt, compensation)
             DO NOTHING",
        )
        .bind(execution_id.as_uuid())
        .bind(result.step_id.as_uuid())
        .bind(i32::from(attempt))
        .bind(compensation)
        .bind(&outcome)
        .bind(&snapshot)
        .bind(result.started_at)
        .bind(result.completed_at)
        .execute(&mut *transaction)
        .await?;
        if insert.rows_affected() == 0 {
            let existing: Value = sqlx::query_scalar(
                "SELECT result_snapshot
                 FROM execution_verifications
                 WHERE execution_id = $1
                   AND step_id = $2
                   AND attempt = $3
                   AND compensation = $4",
            )
            .bind(execution_id.as_uuid())
            .bind(result.step_id.as_uuid())
            .bind(i32::from(attempt))
            .bind(compensation)
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

    /// Loads verification Evidence in durable timeline order.
    ///
    /// # Errors
    ///
    /// Returns database or snapshot decoding failures.
    pub async fn verification_evidence(
        &self,
        execution_id: ExecutionId,
    ) -> Result<Vec<VerificationEvidenceRecord>, JournalError> {
        let rows = sqlx::query(
            "SELECT execution_id, step_id, attempt, phase, evidence_snapshot
             FROM execution_verification_evidence
             WHERE execution_id = $1
             ORDER BY sequence_id",
        )
        .bind(execution_id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let attempt = row.try_get::<i32, _>("attempt")?;
                Ok(VerificationEvidenceRecord {
                    execution_id: ExecutionId::from_uuid(row.try_get("execution_id")?),
                    step_id: rocketmq_sre_contracts::ExecutionStepId::from_uuid(row.try_get("step_id")?),
                    attempt: u16::try_from(attempt)
                        .map_err(|_| JournalError::InvalidInput("stored verification attempt is invalid".to_owned()))?,
                    phase: parse_verification_phase(row.try_get::<String, _>("phase")?.as_str())?,
                    evidence: from_json(row.try_get("evidence_snapshot")?)?,
                })
            })
            .collect()
    }

    /// Atomically escalates a failed rollback, quarantines its resource, emits
    /// the audit timeline, and enqueues configured human notifications.
    ///
    /// The temporary resource lock is intentionally not released here. The
    /// caller may release it only after this transaction commits; the durable
    /// quarantine remains independently active.
    ///
    /// # Errors
    ///
    /// Rejects non-compensation transitions, incomplete quarantine records,
    /// scope drift, wrong audit kinds, stale execution state, and database
    /// failures. An identical retry after commit is a no-op.
    #[allow(
        clippy::too_many_arguments,
        reason = "the atomic safety transaction requires three independently typed audit events"
    )]
    pub async fn escalate_manual_takeover(
        &self,
        execution_id: ExecutionId,
        transition: &ExecutionTransition,
        quarantine: &ResourceQuarantine,
        state_audit: &AuditEvent,
        quarantine_audit: &AuditEvent,
        manual_takeover_audit: &AuditEvent,
    ) -> Result<ManualTakeoverEscalation, JournalError> {
        if transition.from != ExecutionState::Compensating
            || transition.to != ExecutionState::Escalated
            || quarantine.source_execution_id != Some(execution_id)
            || !quarantine.is_active()
            || quarantine.resource_key.trim().is_empty()
            || quarantine.reason_code.trim().is_empty()
            || quarantine.created_by.trim().is_empty()
            || state_audit.event_kind != AuditEventKind::StateChanged
            || quarantine_audit.event_kind != AuditEventKind::QuarantineCreated
            || manual_takeover_audit.event_kind != AuditEventKind::ManualTakeoverRequired
        {
            return Err(JournalError::InvalidInput(
                "manual takeover requires a compensating escalation, active quarantine, and typed audits".to_owned(),
            ));
        }
        transition
            .validate()
            .map_err(|error| JournalError::InvalidInput(error.to_string()))?;

        let mut transaction = self.pool.begin().await?;
        for audit in [state_audit, quarantine_audit, manual_takeover_audit] {
            ensure_execution_scope(&mut transaction, execution_id, audit).await?;
        }
        let execution = sqlx::query(
            "SELECT execution.state, execution.tenant_id, execution.cluster_id,
                    execution.correlation_id, plan.incident_id
             FROM executions execution
             JOIN action_plans plan ON plan.id = execution.plan_id
             WHERE execution.id = $1
             FOR UPDATE OF execution",
        )
        .bind(execution_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(JournalError::NotFound)?;
        if execution.try_get::<Uuid, _>("tenant_id")? != quarantine.tenant_id.as_uuid()
            || execution.try_get::<Uuid, _>("cluster_id")? != quarantine.cluster_id.as_uuid()
            || execution.try_get::<Uuid, _>("correlation_id")? != state_audit.correlation_id.as_uuid()
        {
            return Err(JournalError::InvalidInput(
                "manual takeover quarantine scope does not match execution".to_owned(),
            ));
        }
        let current_state = parse_execution_state(execution.try_get::<String, _>("state")?.as_str())?;
        if current_state == ExecutionState::Escalated {
            let existing: Option<Uuid> = sqlx::query_scalar(
                "SELECT id
                 FROM resource_quarantines
                 WHERE id = $1
                   AND source_execution_id = $2
                   AND tenant_id = $3
                   AND cluster_id = $4
                   AND resource_key = $5
                   AND cleared_at IS NULL",
            )
            .bind(quarantine.id.as_uuid())
            .bind(execution_id.as_uuid())
            .bind(quarantine.tenant_id.as_uuid())
            .bind(quarantine.cluster_id.as_uuid())
            .bind(&quarantine.resource_key)
            .fetch_optional(&mut *transaction)
            .await?;
            if existing.is_none() {
                return Err(JournalError::IdempotencyConflict);
            }
            transaction.commit().await?;
            return Ok(ManualTakeoverEscalation {
                escalated: false,
                quarantine_created: false,
                notifications_enqueued: 0,
            });
        }
        if current_state != transition.from {
            return Err(JournalError::InvalidInput(
                "execution is not in the compensating state".to_owned(),
            ));
        }

        let quarantine_insert = sqlx::query(
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
        .bind(execution_id.as_uuid())
        .bind(
            quarantine
                .evidence_ids
                .iter()
                .copied()
                .map(rocketmq_sre_contracts::EvidenceId::as_uuid)
                .collect::<Vec<_>>(),
        )
        .bind(&quarantine.created_by)
        .bind(quarantine.created_at)
        .execute(&mut *transaction)
        .await;
        let quarantine_insert = match quarantine_insert {
            Ok(result) => result,
            Err(error) if database_message(&error) == Some("invalid_quarantine_source_scope") => {
                return Err(JournalError::InvalidInput(
                    "quarantine source scope does not match its execution".to_owned(),
                ));
            }
            Err(error) => return Err(error.into()),
        };
        if quarantine_insert.rows_affected() != 1 {
            return Err(JournalError::IdempotencyConflict);
        }

        let state_update = sqlx::query(
            "UPDATE executions
             SET state = 'escalated',
                 completed_at = $2,
                 updated_at = $2
             WHERE id = $1 AND state = 'compensating'",
        )
        .bind(execution_id.as_uuid())
        .bind(transition.occurred_at)
        .execute(&mut *transaction)
        .await?;
        if state_update.rows_affected() != 1 {
            return Err(JournalError::IdempotencyConflict);
        }
        append_audit(&mut transaction, state_audit).await?;
        append_audit(&mut transaction, quarantine_audit).await?;
        append_audit(&mut transaction, manual_takeover_audit).await?;

        let target_ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT id
             FROM notification_targets
             WHERE tenant_id = $1
               AND enabled
               AND (cluster_id IS NULL OR cluster_id = $2)
             ORDER BY id",
        )
        .bind(quarantine.tenant_id.as_uuid())
        .bind(quarantine.cluster_id.as_uuid())
        .fetch_all(&mut *transaction)
        .await?;
        let incident_id: Uuid = execution.try_get("incident_id")?;
        let summary = "Supervised RocketMQ SRE rollback requires manual takeover";
        let deep_link = format!("/incidents/{incident_id}?execution={execution_id}");
        let mut notifications_enqueued = 0;
        for target_id in target_ids {
            let delivery_key = format!("manual-takeover:{execution_id}:{target_id}");
            let insert = sqlx::query(
                "INSERT INTO notification_outbox (
                    id, target_id, tenant_id, cluster_id, incident_id,
                    delivery_key, status, sanitized_summary, deep_link,
                    attempt_count, next_attempt_at, created_at
                 ) VALUES (
                    $1, $2, $3, $4, $5,
                    $6, 'pending', $7, $8,
                    0, $9, $9
                 )
                 ON CONFLICT (tenant_id, delivery_key) DO NOTHING",
            )
            .bind(NotificationDeliveryId::new().as_uuid())
            .bind(target_id)
            .bind(quarantine.tenant_id.as_uuid())
            .bind(quarantine.cluster_id.as_uuid())
            .bind(incident_id)
            .bind(delivery_key)
            .bind(summary)
            .bind(&deep_link)
            .bind(transition.occurred_at)
            .execute(&mut *transaction)
            .await?;
            notifications_enqueued += insert.rows_affected();
        }
        transaction.commit().await?;
        Ok(ManualTakeoverEscalation {
            escalated: true,
            quarantine_created: true,
            notifications_enqueued,
        })
    }

    /// Returns durable intents that have no matching result after a restart.
    ///
    /// # Errors
    ///
    /// Returns a database or snapshot decoding failure.
    pub async fn pending_intents(&self, limit: u32) -> Result<Vec<PendingIntent>, JournalError> {
        let rows = sqlx::query(
            "SELECT intent.intent_snapshot, execution.state,
                    execution.tenant_id, execution.cluster_id,
                    execution.correlation_id
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
                    tenant_id: rocketmq_sre_contracts::TenantId::from_uuid(row.try_get("tenant_id")?),
                    cluster_id: rocketmq_sre_contracts::ClusterId::from_uuid(row.try_get("cluster_id")?),
                    correlation_id: rocketmq_sre_contracts::CorrelationId::from_uuid(row.try_get("correlation_id")?),
                })
            })
            .collect()
    }

    /// Returns unresolved intents for one cluster during a pending-epoch
    /// takeover.
    ///
    /// # Errors
    ///
    /// Returns persistence or snapshot-decoding failures.
    pub async fn pending_intents_for_cluster(
        &self,
        cluster_id: rocketmq_sre_contracts::ClusterId,
        limit: u32,
    ) -> Result<Vec<PendingIntent>, JournalError> {
        let rows = sqlx::query(
            "SELECT intent.intent_snapshot, execution.state,
                    execution.tenant_id, execution.cluster_id,
                    execution.correlation_id
             FROM execution_steps intent
             JOIN executions execution ON execution.id = intent.execution_id
             LEFT JOIN execution_steps result
               ON result.execution_id = intent.execution_id
              AND result.step_id = intent.step_id
              AND result.attempt = intent.attempt
              AND result.record_kind = 'result'
             WHERE intent.record_kind = 'intent'
               AND execution.cluster_id = $1
               AND result.sequence_id IS NULL
               AND execution.state NOT IN ('succeeded', 'rolled_back', 'escalated')
             ORDER BY intent.sequence_id
             LIMIT $2",
        )
        .bind(cluster_id.as_uuid())
        .bind(i64::from(limit.min(10_000)))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(PendingIntent {
                    intent: from_json(row.try_get("intent_snapshot")?)?,
                    execution_state: parse_execution_state(row.try_get::<String, _>("state")?.as_str())?,
                    tenant_id: rocketmq_sre_contracts::TenantId::from_uuid(row.try_get("tenant_id")?),
                    cluster_id: rocketmq_sre_contracts::ClusterId::from_uuid(row.try_get("cluster_id")?),
                    correlation_id: rocketmq_sre_contracts::CorrelationId::from_uuid(row.try_get("correlation_id")?),
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

    /// Loads the immutable request that was accepted when an execution was
    /// created.
    ///
    /// Recovery uses this durable snapshot after its dispatch validity window
    /// has elapsed. The caller must still acquire a fresh fenced lease before
    /// consulting the Agent or changing execution state.
    ///
    /// # Errors
    ///
    /// Returns not-found, database, or snapshot-decoding failures.
    pub async fn execution_request(&self, id: ExecutionId) -> Result<ExecutionRequest, JournalError> {
        let snapshot: Option<Value> = sqlx::query_scalar("SELECT request_snapshot FROM executions WHERE id = $1")
            .bind(id.as_uuid())
            .fetch_optional(&self.pool)
            .await?;
        from_json(snapshot.ok_or(JournalError::NotFound)?)
    }

    /// Loads the ordered forward intents whose live effects must be reconciled
    /// before a compensating execution can be closed after interruption.
    ///
    /// # Errors
    ///
    /// Returns database or snapshot-decoding failures.
    pub async fn forward_intents_for_execution(&self, id: ExecutionId) -> Result<Vec<StepIntent>, JournalError> {
        let rows = sqlx::query(
            "SELECT intent_snapshot
             FROM execution_steps
             WHERE execution_id = $1
               AND record_kind = 'intent'
               AND NOT compensation
             ORDER BY sequence_id",
        )
        .bind(id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| from_json(row.try_get("intent_snapshot")?))
            .collect()
    }

    /// Lists interrupted compensation records in deterministic recovery
    /// order.
    ///
    /// # Errors
    ///
    /// Returns database failures.
    pub async fn compensating_execution_ids(&self, limit: u32) -> Result<Vec<ExecutionId>, JournalError> {
        let rows: Vec<Uuid> = sqlx::query_scalar(
            "SELECT id
             FROM executions
             WHERE state = 'compensating'
             ORDER BY updated_at, id
             LIMIT $1",
        )
        .bind(i64::from(limit.min(1_000)))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(ExecutionId::from_uuid).collect())
    }

    /// Returns whether any immutable intent was persisted for an execution.
    ///
    /// # Errors
    ///
    /// Returns a database failure.
    pub async fn has_intent(&self, id: ExecutionId) -> Result<bool, JournalError> {
        sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1
                FROM execution_steps
                WHERE execution_id = $1 AND record_kind = 'intent'
            )",
        )
        .bind(id.as_uuid())
        .fetch_one(&self.pool)
        .await
        .map_err(Into::into)
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

const fn verification_phase_name(phase: VerificationPhase) -> &'static str {
    match phase {
        VerificationPhase::Pre => "pre",
        VerificationPhase::During => "during",
        VerificationPhase::Post => "post",
        VerificationPhase::RollbackPost => "rollback_post",
    }
}

fn parse_verification_phase(value: &str) -> Result<VerificationPhase, JournalError> {
    match value {
        "pre" => Ok(VerificationPhase::Pre),
        "during" => Ok(VerificationPhase::During),
        "post" => Ok(VerificationPhase::Post),
        "rollback_post" => Ok(VerificationPhase::RollbackPost),
        _ => Err(JournalError::InvalidInput(
            "stored verification phase is unsupported".to_owned(),
        )),
    }
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
