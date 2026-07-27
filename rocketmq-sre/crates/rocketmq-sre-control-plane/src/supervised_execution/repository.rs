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
use rocketmq_sre_contracts::ApprovalGrant;
use rocketmq_sre_contracts::ApprovalRecord;
use rocketmq_sre_contracts::AuditEvent;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::DiagnosisRevisionId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ExecutionState;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::PlanStatus;
use rocketmq_sre_contracts::PolicyDecision;
use rocketmq_sre_contracts::ResourceQuarantine;
use rocketmq_sre_contracts::ResourceQuarantineId;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use uuid::Uuid;

use super::model::DiagnosisPlanContext;
use super::model::NewExecutionProjection;
use super::model::PersistedPlanProjection;
use super::model::StoredExecutionProjection;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

mod critic;
mod support;

use support::*;

impl PostgresRepository {
    pub(super) async fn diagnosis_plan_context(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        incident_id: IncidentId,
        diagnosis_revision_id: DiagnosisRevisionId,
    ) -> Result<DiagnosisPlanContext, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT i.tenant_id, i.cluster_id, d.id, d.status, d.evidence_ids,
                    d.primary_model_invocation_id, d.execution_eligible, d.partial
             FROM diagnosis_revisions d
             JOIN sre_incidents i ON i.id = d.incident_id
             WHERE d.id = $1 AND d.incident_id = $2
               AND i.tenant_id = $3 AND i.cluster_id = $4",
        )
        .bind(diagnosis_revision_id.as_uuid())
        .bind(incident_id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let evidence_ids = row
            .try_get::<Vec<Uuid>, _>("evidence_ids")?
            .into_iter()
            .map(EvidenceId::from_uuid)
            .collect();
        Ok(DiagnosisPlanContext {
            tenant_id: rocketmq_sre_contracts::TenantId::from_uuid(row.try_get("tenant_id")?),
            cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
            incident_id,
            diagnosis_revision_id,
            status: row.try_get("status")?,
            evidence_ids,
            primary_model_invocation_id: row
                .try_get::<Option<Uuid>, _>("primary_model_invocation_id")?
                .map(rocketmq_sre_contracts::ModelInvocationId::from_uuid),
            execution_eligible: row.try_get("execution_eligible")?,
            partial: row.try_get("partial")?,
        })
    }

    pub(super) async fn next_action_plan_version(
        &self,
        auth: &AuthContext,
        incident_id: IncidentId,
    ) -> Result<u32, ControlPlaneError> {
        let next: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(p.version), 0)::BIGINT + 1
             FROM action_plans p
             JOIN sre_incidents i ON i.id = p.incident_id
             WHERE p.incident_id = $1 AND p.tenant_id = $2
               AND i.tenant_id = $2",
        )
        .bind(incident_id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_one(&self.pool)
        .await?;
        u32::try_from(next)
            .map_err(|_| ControlPlaneError::validation("invalid_plan_version", "plan version exceeds u32"))
    }

    pub(super) async fn persist_plan_bundle(
        &self,
        plan: &ActionPlan,
        risk: ActionRisk,
        decision: &PolicyDecision,
        audits: &[AuditEvent],
    ) -> Result<(), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
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
             )",
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
        .bind(risk_name(risk)?)
        .bind(plan_status_name(plan.status))
        .bind(json_value(plan)?)
        .bind(&plan.created_by)
        .bind(plan.created_at)
        .bind(plan.expires_at)
        .bind(plan.submitted_at)
        .execute(&mut *transaction)
        .await?;
        insert_policy(&mut transaction, decision).await?;
        for audit in audits {
            insert_audit(&mut transaction, audit).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn supervised_plan(
        &self,
        auth: &AuthContext,
        id: ActionPlanId,
    ) -> Result<PersistedPlanProjection, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT request_snapshot, risk, status, submitted_at
             FROM action_plans
             WHERE id = $1 AND tenant_id = $2",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let mut plan: ActionPlan = from_json(row.try_get("request_snapshot")?)?;
        plan.status = parse_plan_status(row.try_get("status")?)?;
        plan.submitted_at = row.try_get("submitted_at")?;
        if !auth.clusters.contains(&plan.cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "plan cluster is outside the authenticated scope",
            ));
        }
        Ok(PersistedPlanProjection {
            plan,
            risk: parse_risk(row.try_get("risk")?)?,
        })
    }

    pub(super) async fn latest_policy_decision(
        &self,
        auth: &AuthContext,
        plan: &ActionPlan,
    ) -> Result<Option<PolicyDecision>, ControlPlaneError> {
        let snapshot: Option<Value> = sqlx::query_scalar(
            "SELECT decision_snapshot
             FROM policy_decisions
             WHERE tenant_id = $1 AND cluster_id = $2
               AND plan_id = $3 AND plan_hash = $4
             ORDER BY evaluated_at DESC, id DESC
             LIMIT 1",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(plan.cluster_id.as_uuid())
        .bind(plan.id.as_uuid())
        .bind(&plan.plan_hash)
        .fetch_optional(&self.pool)
        .await?;
        snapshot.map(from_json).transpose()
    }

    pub(super) async fn latest_approval(
        &self,
        auth: &AuthContext,
        plan: &ActionPlan,
    ) -> Result<Option<ApprovalRecord>, ControlPlaneError> {
        let snapshot: Option<Value> = sqlx::query_scalar(
            "SELECT approval_snapshot
             FROM approvals
             WHERE tenant_id = $1 AND cluster_id = $2
               AND plan_id = $3 AND plan_hash = $4
             ORDER BY decided_at DESC, id DESC
             LIMIT 1",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(plan.cluster_id.as_uuid())
        .bind(plan.id.as_uuid())
        .bind(&plan.plan_hash)
        .fetch_optional(&self.pool)
        .await?;
        snapshot.map(from_json).transpose()
    }

    pub(super) async fn current_approval_grant(
        &self,
        auth: &AuthContext,
        plan: &ActionPlan,
        now: DateTime<Utc>,
    ) -> Result<Option<ApprovalGrant>, ControlPlaneError> {
        let snapshot: Option<Value> = sqlx::query_scalar(
            "SELECT approval_grant_snapshot
             FROM approvals
             WHERE tenant_id = $1 AND cluster_id = $2
               AND plan_id = $3 AND plan_hash = $4
               AND decision = 'approved' AND expires_at > $5
               AND approval_grant_snapshot IS NOT NULL
             ORDER BY decided_at DESC, id DESC
             LIMIT 1",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(plan.cluster_id.as_uuid())
        .bind(plan.id.as_uuid())
        .bind(&plan.plan_hash)
        .bind(now)
        .fetch_optional(&self.pool)
        .await?;
        snapshot.map(from_json).transpose()
    }

    pub(super) async fn persist_approval_decision(
        &self,
        plan: &ActionPlan,
        approval: &ApprovalRecord,
        grant: Option<&ApprovalGrant>,
        expected_statuses: &[PlanStatus],
        next_status: PlanStatus,
        audits: &[AuditEvent],
    ) -> Result<ActionPlan, ControlPlaneError> {
        let expected = expected_statuses
            .iter()
            .map(|status| plan_status_name(*status))
            .collect::<Vec<_>>();
        let mut transaction = self.pool.begin().await?;
        let changed = sqlx::query(
            "UPDATE action_plans
             SET status = $3
             WHERE id = $1 AND status = ANY($2)
               AND plan_hash = $4 AND expires_at > $5",
        )
        .bind(plan.id.as_uuid())
        .bind(&expected)
        .bind(plan_status_name(next_status))
        .bind(&plan.plan_hash)
        .bind(approval.decided_at)
        .execute(&mut *transaction)
        .await?;
        if changed.rows_affected() != 1 {
            return Err(ControlPlaneError::conflict_code(
                "plan_state_changed",
                "plan is no longer in an approvable state",
            ));
        }
        sqlx::query(
            "INSERT INTO approvals (
                id, tenant_id, cluster_id, plan_id, plan_hash,
                requester_subject, approver_subject, approver_role,
                decision, reason, approval_snapshot, decided_at, expires_at,
                precondition_hash, approval_grant_snapshot
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8,
                $9, $10, $11, $12, $13,
                $14, $15
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
        .bind(grant.map(|grant| grant.precondition_hash.as_str()))
        .bind(grant.map(json_value).transpose()?)
        .execute(&mut *transaction)
        .await?;
        for audit in audits {
            insert_audit(&mut transaction, audit).await?;
        }
        transaction.commit().await?;
        let mut updated = plan.clone();
        updated.status = next_status;
        Ok(updated)
    }

    pub(super) async fn resource_is_quarantined(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        resource_key: &str,
        action: ExecutionAction,
    ) -> Result<bool, ControlPlaneError> {
        sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1
                FROM resource_quarantines
                WHERE tenant_id = $1 AND cluster_id = $2
                  AND resource_key = $3 AND cleared_at IS NULL
                  AND (action_id IS NULL OR action_id = $4)
             )",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(resource_key)
        .bind(action.id())
        .fetch_one(&self.pool)
        .await
        .map_err(ControlPlaneError::from)
    }

    pub(super) async fn resource_has_active_change(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        resource_key: &str,
    ) -> Result<bool, ControlPlaneError> {
        sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1
                FROM executions
                WHERE tenant_id = $1 AND cluster_id = $2
                  AND resource_key = $3
                  AND state NOT IN ('succeeded', 'rolled_back', 'escalated')
             )",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(resource_key)
        .fetch_one(&self.pool)
        .await
        .map_err(ControlPlaneError::from)
    }

    pub(super) async fn persist_execution_submission(
        &self,
        projection: &NewExecutionProjection,
        audit: &AuditEvent,
    ) -> Result<StoredExecutionProjection, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT INTO executions (
                id, tenant_id, cluster_id, correlation_id, plan_id, plan_hash,
                resource_key, action_id, idempotency_key, state,
                request_snapshot, requested_by, started_at, completed_at, updated_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, 'pending',
                $10, $11, $12, NULL, $12
             )
             ON CONFLICT (idempotency_key) DO NOTHING",
        )
        .bind(projection.id.as_uuid())
        .bind(projection.tenant_id.as_uuid())
        .bind(projection.cluster_id.as_uuid())
        .bind(projection.correlation_id.as_uuid())
        .bind(projection.request.plan.id.as_uuid())
        .bind(&projection.request.plan.plan_hash)
        .bind(&projection.resource_key)
        .bind(&projection.action_id)
        .bind(&projection.request.idempotency_key)
        .bind(json_value(&projection.request)?)
        .bind(&projection.request.requested_by)
        .bind(projection.request.issued_at)
        .execute(&mut *transaction)
        .await?;
        if inserted.rows_affected() == 1 {
            insert_audit(&mut transaction, audit).await?;
            transaction.commit().await?;
            return Ok(StoredExecutionProjection {
                request: projection.request.clone(),
                state: ExecutionState::Pending,
                submitted_at: projection.request.issued_at,
            });
        }
        transaction.rollback().await?;
        let existing = self
            .execution_by_idempotency(&projection.request.idempotency_key)
            .await?;
        if existing.request.tenant_id != projection.tenant_id
            || existing.request.cluster_id != projection.cluster_id
            || existing.request.plan.plan_hash != projection.request.plan.plan_hash
            || existing.request.requested_by != projection.request.requested_by
        {
            return Err(ControlPlaneError::conflict_code(
                "idempotency_conflict",
                "idempotency key is already bound to a different execution request",
            ));
        }
        Ok(existing)
    }

    async fn execution_by_idempotency(
        &self,
        idempotency_key: &str,
    ) -> Result<StoredExecutionProjection, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT request_snapshot, state, started_at
             FROM executions
             WHERE idempotency_key = $1",
        )
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        Ok(StoredExecutionProjection {
            request: from_json(row.try_get("request_snapshot")?)?,
            state: parse_execution_state(row.try_get("state")?)?,
            submitted_at: row.try_get("started_at")?,
        })
    }

    pub(super) async fn supervised_execution_by_idempotency(
        &self,
        auth: &AuthContext,
        idempotency_key: &str,
    ) -> Result<Option<StoredExecutionProjection>, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT request_snapshot, state, started_at
             FROM executions
             WHERE idempotency_key = $1 AND tenant_id = $2",
        )
        .bind(idempotency_key)
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let projection = StoredExecutionProjection {
            request: from_json(row.try_get("request_snapshot")?)?,
            state: parse_execution_state(row.try_get("state")?)?,
            submitted_at: row.try_get("started_at")?,
        };
        if !auth.clusters.contains(&projection.request.cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "execution cluster is outside the authenticated scope",
            ));
        }
        Ok(Some(projection))
    }

    pub(super) async fn supervised_execution(
        &self,
        auth: &AuthContext,
        id: ExecutionId,
    ) -> Result<StoredExecutionProjection, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT request_snapshot, state, started_at
             FROM executions
             WHERE id = $1 AND tenant_id = $2",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let projection = StoredExecutionProjection {
            request: from_json(row.try_get("request_snapshot")?)?,
            state: parse_execution_state(row.try_get("state")?)?,
            submitted_at: row.try_get("started_at")?,
        };
        if !auth.clusters.contains(&projection.request.cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "execution cluster is outside the authenticated scope",
            ));
        }
        Ok(projection)
    }

    pub(super) async fn audit_timeline(
        &self,
        auth: &AuthContext,
        correlation_id: CorrelationId,
        limit: i64,
    ) -> Result<Vec<AuditEvent>, ControlPlaneError> {
        let clusters = auth.clusters.iter().map(|id| id.as_uuid()).collect::<Vec<_>>();
        let snapshots: Vec<Value> = sqlx::query_scalar(
            "SELECT event_snapshot
             FROM audit_events
             WHERE tenant_id = $1 AND correlation_id = $2
               AND cluster_id = ANY($3)
             ORDER BY sequence_id ASC
             LIMIT $4",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(correlation_id.as_uuid())
        .bind(&clusters)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        snapshots.into_iter().map(from_json).collect()
    }

    pub(super) async fn resource_quarantines(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        include_cleared: bool,
        limit: i64,
    ) -> Result<Vec<ResourceQuarantine>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT id, tenant_id, cluster_id, resource_key, action_id,
                    reason_code, source_execution_id, evidence_ids,
                    created_by, created_at, cleared_by, clear_reason,
                    clear_evidence_ids, cleared_at
             FROM resource_quarantines
             WHERE tenant_id = $1 AND cluster_id = $2
               AND ($3 OR cleared_at IS NULL)
             ORDER BY created_at DESC, id DESC
             LIMIT $4",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(include_cleared)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(quarantine_from_row).collect()
    }

    pub(super) async fn quarantine(
        &self,
        auth: &AuthContext,
        id: ResourceQuarantineId,
    ) -> Result<ResourceQuarantine, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, resource_key, action_id,
                    reason_code, source_execution_id, evidence_ids,
                    created_by, created_at, cleared_by, clear_reason,
                    clear_evidence_ids, cleared_at
             FROM resource_quarantines
             WHERE id = $1 AND tenant_id = $2",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let quarantine = quarantine_from_row(&row)?;
        if !auth.clusters.contains(&quarantine.cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "quarantine cluster is outside the authenticated scope",
            ));
        }
        Ok(quarantine)
    }

    pub(super) async fn clear_resource_quarantine(
        &self,
        quarantine: &ResourceQuarantine,
        cleared_by: &str,
        reason: &str,
        evidence_ids: &[EvidenceId],
        cleared_at: DateTime<Utc>,
        audits: &[AuditEvent],
    ) -> Result<ResourceQuarantine, ControlPlaneError> {
        let evidence_ids = evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>();
        let mut transaction = self.pool.begin().await?;
        for audit in audits.iter().take(1) {
            insert_audit(&mut transaction, audit).await?;
        }
        let row = sqlx::query(
            "UPDATE resource_quarantines
             SET cleared_by = $2, clear_reason = $3,
                 clear_evidence_ids = $4, cleared_at = $5
             WHERE id = $1 AND cleared_at IS NULL
             RETURNING id, tenant_id, cluster_id, resource_key, action_id,
                       reason_code, source_execution_id, evidence_ids,
                       created_by, created_at, cleared_by, clear_reason,
                       clear_evidence_ids, cleared_at",
        )
        .bind(quarantine.id.as_uuid())
        .bind(cleared_by)
        .bind(reason)
        .bind(&evidence_ids)
        .bind(cleared_at)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| {
            ControlPlaneError::conflict_code("quarantine_state_changed", "resource quarantine was already cleared")
        })?;
        for audit in audits.iter().skip(1) {
            insert_audit(&mut transaction, audit).await?;
        }
        transaction.commit().await?;
        quarantine_from_row(&row)
    }
}
