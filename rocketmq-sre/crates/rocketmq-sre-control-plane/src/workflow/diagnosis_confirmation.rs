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
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::DiagnosisRevisionId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::InvestigationId;
use rocketmq_sre_contracts::ModelInvocationId;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

use super::repository::append_timeline;
use super::repository::append_workflow_event;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

pub(super) const DIAGNOSIS_CONFIRMATION_SCHEMA: &str = "rocketmq-sre.diagnosis-execution-confirmation.v1";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfirmDiagnosisExecutionRequest {
    pub human_confirmed: bool,
    pub reason: String,
}

impl ConfirmDiagnosisExecutionRequest {
    pub(crate) fn validate(&self) -> Result<(), ControlPlaneError> {
        if !self.human_confirmed {
            return Err(ControlPlaneError::validation(
                "human_confirmation_required",
                "execution eligibility requires an explicit human confirmation",
            ));
        }
        let reason = self.reason.trim();
        if !(8..=2_048).contains(&reason.len())
            || reason.chars().any(char::is_control)
            || reason.len() != self.reason.len()
        {
            return Err(ControlPlaneError::validation(
                "invalid_confirmation_reason",
                "confirmation reason must contain 8 to 2048 non-control characters without surrounding whitespace",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DiagnosisExecutionConfirmation {
    pub schema_version: &'static str,
    pub incident_id: IncidentId,
    pub source_revision_id: DiagnosisRevisionId,
    pub confirmed_revision_id: DiagnosisRevisionId,
    pub revision: u32,
    pub cluster_id: ClusterId,
    pub primary_model_invocation_id: ModelInvocationId,
    pub evidence_ids: Vec<EvidenceId>,
    pub execution_eligible: bool,
    pub confirmed_by: String,
    pub reason: String,
    pub correlation_id: CorrelationId,
    pub confirmed_at: DateTime<Utc>,
}

impl PostgresRepository {
    pub(super) async fn confirm_diagnosis_for_execution(
        &self,
        auth: &AuthContext,
        incident_id: IncidentId,
        source_revision_id: DiagnosisRevisionId,
        request: &ConfirmDiagnosisExecutionRequest,
        correlation_id: CorrelationId,
    ) -> Result<DiagnosisExecutionConfirmation, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT i.cluster_id, i.investigation_id, i.status AS incident_status,
                    d.revision, d.status AS diagnosis_status, d.rule_result,
                    d.hypotheses, d.evidence_ids, d.primary_model_invocation_id,
                    d.execution_eligible, d.partial,
                    m.purpose AS invocation_purpose,
                    m.diagnosis_revision_id AS invocation_diagnosis_revision_id
             FROM diagnosis_revisions d
             JOIN sre_incidents i ON i.id = d.incident_id
             LEFT JOIN model_invocations m ON m.id = d.primary_model_invocation_id
             WHERE d.id = $1 AND d.incident_id = $2 AND i.tenant_id = $3
             FOR UPDATE OF i, d",
        )
        .bind(source_revision_id.as_uuid())
        .bind(incident_id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;

        let cluster_id = ClusterId::from_uuid(row.try_get("cluster_id")?);
        if !auth.clusters.contains(&cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "diagnosis is outside the authenticated cluster scope",
            ));
        }
        if row.try_get::<String, _>("incident_status")? != "monitoring" {
            return Err(ControlPlaneError::conflict_code(
                "diagnosis_not_confirmable",
                "only a non-terminal monitoring incident can be confirmed for execution",
            ));
        }
        if row.try_get::<bool, _>("partial")? {
            return Err(ControlPlaneError::conflict_code(
                "diagnosis_partial",
                "a partial diagnosis cannot be confirmed for execution",
            ));
        }
        if row.try_get::<bool, _>("execution_eligible")? {
            return Err(ControlPlaneError::conflict_code(
                "diagnosis_already_execution_eligible",
                "the selected diagnosis revision is already execution eligible",
            ));
        }

        let evidence_uuids = row.try_get::<Vec<Uuid>, _>("evidence_ids")?;
        if evidence_uuids.is_empty() {
            return Err(ControlPlaneError::conflict_code(
                "diagnosis_evidence_missing",
                "execution eligibility requires at least one persisted Evidence snapshot",
            ));
        }
        let primary_invocation_uuid = row
            .try_get::<Option<Uuid>, _>("primary_model_invocation_id")?
            .ok_or_else(|| {
                ControlPlaneError::conflict_code(
                    "RulesOnlyDiagnosisNotExecutable",
                    "rules-only diagnosis cannot be confirmed for execution",
                )
            })?;
        if row.try_get::<Option<String>, _>("invocation_purpose")?.as_deref() != Some("primary_diagnosis")
            || row.try_get::<Option<Uuid>, _>("invocation_diagnosis_revision_id")? != Some(source_revision_id.as_uuid())
        {
            return Err(ControlPlaneError::conflict_code(
                "model_lineage_mismatch",
                "primary model invocation is not bound to the selected diagnosis revision",
            ));
        }

        let source_revision = row.try_get::<i32, _>("revision")?;
        let latest_revision: i32 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(revision), 0)
             FROM diagnosis_revisions
             WHERE incident_id = $1",
        )
        .bind(incident_id.as_uuid())
        .fetch_one(&mut *transaction)
        .await?;
        if latest_revision != source_revision {
            return Err(ControlPlaneError::conflict_code(
                "diagnosis_revision_stale",
                "only the latest diagnosis revision can be confirmed for execution",
            ));
        }
        let confirmed_revision = source_revision.checked_add(1).ok_or_else(|| {
            ControlPlaneError::validation("diagnosis_revision_overflow", "diagnosis revision exceeds INTEGER")
        })?;
        let confirmed_revision_id = DiagnosisRevisionId::new();
        let confirmed_at = Utc::now();
        let mut rule_result = row.try_get::<Value, _>("rule_result")?;
        let rule_result_object = rule_result.as_object_mut().ok_or_else(|| {
            ControlPlaneError::validation("source_unavailable", "diagnosis rule result is not a JSON object")
        })?;
        rule_result_object.insert(
            "execution_confirmation".to_owned(),
            json!({
                "schema_version": DIAGNOSIS_CONFIRMATION_SCHEMA,
                "source_revision_id": source_revision_id,
                "confirmed_revision_id": confirmed_revision_id,
                "confirmed_by": auth.subject,
                "reason": request.reason,
                "confirmed_at": confirmed_at,
            }),
        );
        let hypotheses = row.try_get::<Value, _>("hypotheses")?;
        sqlx::query(
            "INSERT INTO diagnosis_revisions (
                id, incident_id, revision, status, rule_result, hypotheses,
                evidence_ids, primary_model_invocation_id, execution_eligible,
                partial, created_at
             ) VALUES ($1, $2, $3, 'confirmed', $4, $5, $6, $7, TRUE, FALSE, $8)",
        )
        .bind(confirmed_revision_id.as_uuid())
        .bind(incident_id.as_uuid())
        .bind(confirmed_revision)
        .bind(rule_result)
        .bind(hypotheses)
        .bind(&evidence_uuids)
        .bind(primary_invocation_uuid)
        .bind(confirmed_at)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE sre_incidents
             SET workflow_checkpoint = workflow_checkpoint || $2::JSONB,
                 updated_at = $3
             WHERE id = $1",
        )
        .bind(incident_id.as_uuid())
        .bind(json!({
            "diagnosis_execution_confirmation": {
                "source_revision_id": source_revision_id,
                "confirmed_revision_id": confirmed_revision_id,
                "confirmed_by": auth.subject,
                "correlation_id": correlation_id,
            }
        }))
        .bind(confirmed_at)
        .execute(&mut *transaction)
        .await?;

        let investigation_id = row
            .try_get::<Option<Uuid>, _>("investigation_id")?
            .map(InvestigationId::from_uuid);
        append_timeline(
            &mut transaction,
            auth,
            cluster_id,
            investigation_id,
            Some(incident_id),
            "diagnosis_execution_confirmed",
            "Model-assisted diagnosis confirmed for supervised execution",
            json!({
                "source_revision_id": source_revision_id,
                "confirmed_revision_id": confirmed_revision_id,
                "execution_eligible": true,
                "primary_model_invocation_id": primary_invocation_uuid,
            }),
            correlation_id,
            confirmed_at,
        )
        .await?;
        append_workflow_event(
            &mut transaction,
            auth,
            cluster_id,
            "incident",
            incident_id.as_uuid(),
            "diagnosis_execution_confirmed",
            json!({
                "source_revision_id": source_revision_id,
                "confirmed_revision_id": confirmed_revision_id,
                "execution_eligible": true,
            }),
            correlation_id,
            confirmed_at,
        )
        .await?;
        transaction.commit().await?;

        Ok(DiagnosisExecutionConfirmation {
            schema_version: DIAGNOSIS_CONFIRMATION_SCHEMA,
            incident_id,
            source_revision_id,
            confirmed_revision_id,
            revision: u32::try_from(confirmed_revision).map_err(|_| {
                ControlPlaneError::validation(
                    "diagnosis_revision_overflow",
                    "diagnosis revision exceeds supported range",
                )
            })?,
            cluster_id,
            primary_model_invocation_id: ModelInvocationId::from_uuid(primary_invocation_uuid),
            evidence_ids: evidence_uuids.into_iter().map(EvidenceId::from_uuid).collect(),
            execution_eligible: true,
            confirmed_by: auth.subject.clone(),
            reason: request.reason.clone(),
            correlation_id,
            confirmed_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confirmation_requires_explicit_human_decision_and_bounded_reason() {
        ConfirmDiagnosisExecutionRequest {
            human_confirmed: true,
            reason: "Evidence and root cause reviewed".to_owned(),
        }
        .validate()
        .expect("valid confirmation");

        for request in [
            ConfirmDiagnosisExecutionRequest {
                human_confirmed: false,
                reason: "Evidence and root cause reviewed".to_owned(),
            },
            ConfirmDiagnosisExecutionRequest {
                human_confirmed: true,
                reason: "short".to_owned(),
            },
            ConfirmDiagnosisExecutionRequest {
                human_confirmed: true,
                reason: " surrounding whitespace ".to_owned(),
            },
        ] {
            assert!(request.validate().is_err());
        }
    }
}
