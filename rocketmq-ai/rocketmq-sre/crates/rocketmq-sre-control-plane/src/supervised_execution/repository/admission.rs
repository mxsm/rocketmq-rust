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

use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::DiagnosisRevisionId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::TenantId;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use super::super::model::DiagnosisPlanContext;
use super::super::policy::PolicyEvaluator;
use super::super::service::Clock;
use crate::ControlPlaneRequestFailure;

// The confirmed revision intentionally retains its source revision's invocation.
// Both direct lineage and that exact append-only confirmation link are checked.
pub(super) const DIAGNOSIS_CONTEXT_SQL: &str = "
    SELECT i.tenant_id, i.cluster_id, i.status AS incident_status, i.resource AS incident_resource,
           d.id, d.status, d.evidence_ids, d.primary_model_invocation_id,
           d.execution_eligible, d.partial,
           (SELECT id FROM diagnosis_revisions WHERE incident_id = i.id
            ORDER BY revision DESC LIMIT 1) AS latest_revision_id,
           COALESCE(m.purpose = 'primary_diagnosis' AND m.error_code IS NULL
             AND m.tenant_id = i.tenant_id AND m.cluster_id = i.cluster_id AND m.incident_id = i.id
             AND (m.diagnosis_revision_id = d.id OR
               (d.status = 'confirmed'
                AND d.rule_result #>> '{execution_confirmation,confirmed_revision_id}' = d.id::TEXT
                AND d.rule_result #>> '{execution_confirmation,source_revision_id}' = m.diagnosis_revision_id::TEXT
                AND EXISTS (SELECT 1 FROM diagnosis_revisions s
                            WHERE s.id = m.diagnosis_revision_id AND s.incident_id = i.id
                              AND s.primary_model_invocation_id = m.id AND NOT s.partial
                              AND s.revision < d.revision))), FALSE) AS lineage_valid
    FROM diagnosis_revisions d
    JOIN sre_incidents i ON i.id = d.incident_id
    LEFT JOIN model_invocations m ON m.id = d.primary_model_invocation_id
    WHERE d.id = $1 AND d.incident_id = $2 AND i.tenant_id = $3 AND i.cluster_id = $4
      AND EXISTS (SELECT 1 FROM clusters c WHERE c.id = i.cluster_id
                  AND c.tenant_id = i.tenant_id::TEXT AND c.onboarding_state <> 'offboarded')";

pub(super) fn context_from_row(
    row: &PgRow,
    incident_id: IncidentId,
    diagnosis_revision_id: DiagnosisRevisionId,
) -> Result<DiagnosisPlanContext, ControlPlaneRequestFailure> {
    Ok(DiagnosisPlanContext {
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        incident_id,
        diagnosis_revision_id,
        status: row.try_get("status")?,
        evidence_ids: row
            .try_get::<Vec<Uuid>, _>("evidence_ids")?
            .into_iter()
            .map(EvidenceId::from_uuid)
            .collect(),
        primary_model_invocation_id: row
            .try_get::<Option<Uuid>, _>("primary_model_invocation_id")?
            .map(rocketmq_sre_contracts::ModelInvocationId::from_uuid),
        execution_eligible: row.try_get("execution_eligible")?,
        partial: row.try_get("partial")?,
        incident_status: row.try_get("incident_status")?,
        incident_resource: row.try_get("incident_resource")?,
        latest_diagnosis_revision_id: DiagnosisRevisionId::from_uuid(row.try_get("latest_revision_id")?),
        primary_model_lineage_valid: row.try_get("lineage_valid")?,
    })
}

/// Already validated immutable inputs, checked again at the Incident linearization point.
/// Network reads and signing happen outside the transaction. The clock is sampled
/// after the row lock, so waiting cannot extend preparation or approval lifetime.
pub(in crate::supervised_execution) struct ActionAdmission {
    pub(in crate::supervised_execution) tenant_id: TenantId,
    pub(in crate::supervised_execution) cluster_id: ClusterId,
    pub(in crate::supervised_execution) incident_id: IncidentId,
    pub(in crate::supervised_execution) diagnosis_revision_id: DiagnosisRevisionId,
    pub(in crate::supervised_execution) valid_until: DateTime<Utc>,
    pub(in crate::supervised_execution) expires_before: Option<DateTime<Utc>>,
    pub(in crate::supervised_execution) evidence: Vec<(EvidenceId, String)>,
    pub(in crate::supervised_execution) clock: Clock,
    pub(in crate::supervised_execution) policy: Arc<PolicyEvaluator>,
}

impl ActionAdmission {
    pub(in crate::supervised_execution) async fn lock_and_check(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<DateTime<Utc>, ControlPlaneRequestFailure> {
        self.lock_incident(transaction).await?;
        self.check_locked(transaction).await
    }

    pub(in crate::supervised_execution) async fn lock_incident(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), ControlPlaneRequestFailure> {
        sqlx::query("SELECT id FROM sre_incidents WHERE id = $1 AND tenant_id = $2 AND cluster_id = $3 FOR UPDATE")
            .bind(self.incident_id.as_uuid())
            .bind(self.tenant_id.as_uuid())
            .bind(self.cluster_id.as_uuid())
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(ControlPlaneRequestFailure::not_found())?;
        Ok(())
    }

    pub(in crate::supervised_execution) async fn check_locked(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<DateTime<Utc>, ControlPlaneRequestFailure> {
        let now = (self.clock)();
        if now > self.valid_until
            || self.expires_before.is_some_and(|limit| now >= limit)
            || !self.policy.maintenance_window_open(now)
        {
            return Err(ControlPlaneRequestFailure::conflict_code(
                "action_readiness_expired",
                "action preparation or policy window expired before admission",
            ));
        }
        let row = sqlx::query(DIAGNOSIS_CONTEXT_SQL)
            .bind(self.diagnosis_revision_id.as_uuid())
            .bind(self.incident_id.as_uuid())
            .bind(self.tenant_id.as_uuid())
            .bind(self.cluster_id.as_uuid())
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(ControlPlaneRequestFailure::not_found())?;
        let context = context_from_row(&row, self.incident_id, self.diagnosis_revision_id)?;
        require_current_diagnosis(&context)?;
        for (id, hash) in &self.evidence {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                SELECT 1 FROM evidence_snapshots e JOIN evidence_links l ON l.evidence_id = e.id
                WHERE e.id = $1 AND e.tenant_id = $2 AND e.cluster_id = $3 AND e.content_hash = $4
                  AND l.incident_id = $5 AND (e.expires_at IS NULL OR e.expires_at > $6))",
            )
            .bind(id.as_uuid())
            .bind(self.tenant_id.as_uuid())
            .bind(self.cluster_id.as_uuid())
            .bind(hash)
            .bind(self.incident_id.as_uuid())
            .bind(now)
            .fetch_one(&mut **transaction)
            .await?;
            if !exists {
                return Err(ControlPlaneRequestFailure::conflict_code(
                    "action_evidence_changed",
                    "bound action evidence is no longer available",
                ));
            }
        }
        Ok(now)
    }
}

pub(in crate::supervised_execution) fn require_current_diagnosis(
    context: &DiagnosisPlanContext,
) -> Result<(), ControlPlaneRequestFailure> {
    if context.latest_diagnosis_revision_id != context.diagnosis_revision_id {
        return Err(ControlPlaneRequestFailure::conflict_code(
            "diagnosis_revision_stale",
            "the selected diagnosis is no longer current",
        ));
    }
    if context.incident_status != "monitoring"
        || context.status != "confirmed"
        || context.partial
        || !context.execution_eligible
        || !context.primary_model_lineage_valid
    {
        return Err(ControlPlaneRequestFailure::conflict_code(
            "diagnosis_not_execution_ready",
            "a current confirmed complete model-assisted diagnosis is required",
        ));
    }
    Ok(())
}
