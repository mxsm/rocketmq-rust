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
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::InspectionRunId;
use rocketmq_sre_contracts::InspectionStatus;
use serde_json::Value;
use serde_json::json;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use super::EventEntryTargetKind;
use super::EventEntryWorkflowTarget;
use super::InspectionCreateRequest;
use super::UnifiedEventEntryRequest;
use super::UnifiedEventEntryResult;
use super::UnifiedEventPayload;
use super::event_entry_model::EVENT_ENTRY_RESULT_SCHEMA;
use super::repository::append_timeline;
use super::repository::append_workflow_event;
use super::repository::ensure_cluster_scope;
use super::repository::fingerprint;
use super::repository::insert_incident;
use super::repository::insert_investigation;
use super::repository::inspection_status_name;
use super::repository::inspection_template_name;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

impl PostgresRepository {
    pub(super) async fn event_entry(
        &self,
        auth: &AuthContext,
        request: &UnifiedEventEntryRequest,
        request_hash: &str,
    ) -> Result<Option<UnifiedEventEntryResult>, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT id, source_kind, request_hash, target_kind, target_id,
                    correlation_id, accepted_at
             FROM workflow_event_entries
             WHERE tenant_id = $1
               AND cluster_id = $2
               AND source_kind = $3
               AND idempotency_key = $4",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(request.cluster_id.as_uuid())
        .bind(request.source_kind().as_str())
        .bind(&request.idempotency_key)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| event_entry_from_row(&row, request_hash)).transpose()
    }

    pub(super) async fn create_non_alert_event_entry(
        &self,
        auth: &AuthContext,
        request: &UnifiedEventEntryRequest,
        request_hash: &str,
        correlation_id: CorrelationId,
    ) -> Result<UnifiedEventEntryResult, ControlPlaneError> {
        if request.alert_request().is_some() {
            return Err(ControlPlaneError::validation(
                "invalid_event_entry",
                "alert entries must use the correlation-backed persistence path",
            ));
        }
        if let Some(existing) = self.event_entry(auth, request, request_hash).await? {
            return Ok(existing);
        }

        let mut transaction = self.pool.begin().await?;
        ensure_cluster_scope(&mut transaction, auth.tenant_id, request.cluster_id).await?;
        let accepted_at = Utc::now();
        let (target_kind, target_id) =
            create_non_alert_target(&mut transaction, auth, request, correlation_id, accepted_at).await?;
        if target_kind != request.target_kind() {
            transaction.rollback().await?;
            return Err(ControlPlaneError::validation(
                "source_unavailable",
                "unified event entry produced an unexpected workflow target",
            ));
        }
        let result = UnifiedEventEntryResult {
            schema_version: EVENT_ENTRY_RESULT_SCHEMA,
            entry_id: Uuid::new_v4(),
            source_kind: request.source_kind(),
            target_kind,
            target_id,
            created: true,
            replayed: false,
            correlation_id,
            accepted_at,
        };
        let inserted = insert_event_entry(
            &mut transaction,
            auth,
            request,
            request_hash,
            &result,
            request.effective_occurred_at(),
        )
        .await?;
        if !inserted {
            transaction.rollback().await?;
            return self.event_entry(auth, request, request_hash).await?.ok_or_else(|| {
                ControlPlaneError::validation(
                    "source_unavailable",
                    "concurrent event entry replay could not be loaded",
                )
            });
        }
        transaction.commit().await?;
        Ok(result)
    }

    pub(super) async fn record_alert_event_entry(
        &self,
        auth: &AuthContext,
        request: &UnifiedEventEntryRequest,
        request_hash: &str,
        incident_id: IncidentId,
        correlation_id: CorrelationId,
    ) -> Result<UnifiedEventEntryResult, ControlPlaneError> {
        if let Some(existing) = self.event_entry(auth, request, request_hash).await? {
            return Ok(existing);
        }
        let mut transaction = self.pool.begin().await?;
        ensure_cluster_scope(&mut transaction, auth.tenant_id, request.cluster_id).await?;
        let result = UnifiedEventEntryResult {
            schema_version: EVENT_ENTRY_RESULT_SCHEMA,
            entry_id: Uuid::new_v4(),
            source_kind: request.source_kind(),
            target_kind: EventEntryTargetKind::Incident,
            target_id: incident_id.as_uuid(),
            created: true,
            replayed: false,
            correlation_id,
            accepted_at: Utc::now(),
        };
        let inserted = insert_event_entry(
            &mut transaction,
            auth,
            request,
            request_hash,
            &result,
            request.effective_occurred_at(),
        )
        .await?;
        if !inserted {
            transaction.rollback().await?;
            return self.event_entry(auth, request, request_hash).await?.ok_or_else(|| {
                ControlPlaneError::validation(
                    "source_unavailable",
                    "concurrent alert entry replay could not be loaded",
                )
            });
        }
        transaction.commit().await?;
        Ok(result)
    }
}

async fn create_non_alert_target(
    transaction: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    request: &UnifiedEventEntryRequest,
    correlation_id: CorrelationId,
    accepted_at: DateTime<Utc>,
) -> Result<(EventEntryTargetKind, Uuid), ControlPlaneError> {
    match &request.payload {
        UnifiedEventPayload::Alert { .. } => Err(ControlPlaneError::validation(
            "invalid_event_entry",
            "alert entries cannot use direct workflow persistence",
        )),
        UnifiedEventPayload::ManualIssue {
            title,
            resource,
            symptom_family,
        } => {
            let investigation = insert_investigation(
                transaction,
                auth,
                request.cluster_id,
                None,
                title.clone(),
                resource.as_deref(),
                symptom_family,
                accepted_at,
                correlation_id,
            )
            .await?;
            Ok((EventEntryTargetKind::Investigation, investigation.id.as_uuid()))
        }
        UnifiedEventPayload::ScheduledInspection { template, schedule } => {
            let id = create_inspection_target(
                transaction,
                auth,
                request,
                InspectionCreateRequest {
                    cluster_id: request.cluster_id,
                    template: *template,
                    schedule: schedule.clone(),
                },
                correlation_id,
                accepted_at,
            )
            .await?;
            Ok((EventEntryTargetKind::InspectionRun, id.as_uuid()))
        }
        UnifiedEventPayload::ChangeEvent {
            change_kind,
            target,
            title,
            resource,
            symptom_family,
        } => {
            let details = json!({
                "entry_source": request.source_kind(),
                "change_kind": change_kind,
                "idempotency_key": &request.idempotency_key,
            });
            create_named_workflow_target(
                transaction,
                auth,
                request,
                *target,
                title,
                resource.as_deref(),
                symptom_family,
                "change_event_ingested",
                "Release or change event ingested",
                details,
                correlation_id,
                accepted_at,
            )
            .await
        }
        UnifiedEventPayload::ExternalIntegration {
            channel,
            target,
            title,
            resource,
            symptom_family,
        } => {
            let details = json!({
                "entry_source": request.source_kind(),
                "channel": channel,
                "idempotency_key": &request.idempotency_key,
            });
            create_named_workflow_target(
                transaction,
                auth,
                request,
                *target,
                title,
                resource.as_deref(),
                symptom_family,
                "external_event_ingested",
                "External ITSM or ChatOps event ingested",
                details,
                correlation_id,
                accepted_at,
            )
            .await
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the unified boundary passes the complete typed workflow target tuple"
)]
async fn create_named_workflow_target(
    transaction: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    request: &UnifiedEventEntryRequest,
    target: EventEntryWorkflowTarget,
    title: &str,
    resource: Option<&str>,
    symptom_family: &str,
    event_type: &str,
    summary: &str,
    details: Value,
    correlation_id: CorrelationId,
    accepted_at: DateTime<Utc>,
) -> Result<(EventEntryTargetKind, Uuid), ControlPlaneError> {
    match target {
        EventEntryWorkflowTarget::Investigation => {
            let investigation = insert_investigation(
                transaction,
                auth,
                request.cluster_id,
                None,
                title.to_owned(),
                resource,
                symptom_family,
                accepted_at,
                correlation_id,
            )
            .await?;
            append_timeline(
                transaction,
                auth,
                request.cluster_id,
                Some(investigation.id),
                None,
                event_type,
                summary,
                details,
                correlation_id,
                request.effective_occurred_at(),
            )
            .await?;
            Ok((EventEntryTargetKind::Investigation, investigation.id.as_uuid()))
        }
        EventEntryWorkflowTarget::Incident => {
            let incident_id = IncidentId::new();
            let incident_fingerprint = fingerprint(
                auth.tenant_id,
                request.cluster_id,
                resource,
                symptom_family,
                request.effective_occurred_at(),
            );
            insert_incident(
                transaction,
                auth,
                incident_id,
                None,
                request.cluster_id,
                title,
                resource,
                symptom_family,
                &incident_fingerprint,
                accepted_at,
            )
            .await?;
            append_timeline(
                transaction,
                auth,
                request.cluster_id,
                None,
                Some(incident_id),
                event_type,
                summary,
                details.clone(),
                correlation_id,
                request.effective_occurred_at(),
            )
            .await?;
            append_workflow_event(
                transaction,
                auth,
                request.cluster_id,
                "incident",
                incident_id.as_uuid(),
                "incident_created",
                details,
                correlation_id,
                accepted_at,
            )
            .await?;
            Ok((EventEntryTargetKind::Incident, incident_id.as_uuid()))
        }
    }
}

async fn create_inspection_target(
    transaction: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    request: &UnifiedEventEntryRequest,
    inspection: InspectionCreateRequest,
    correlation_id: CorrelationId,
    accepted_at: DateTime<Utc>,
) -> Result<InspectionRunId, ControlPlaneError> {
    let id = InspectionRunId::new();
    let status = InspectionStatus::Scheduled;
    let next_run_at = inspection
        .schedule_interval()?
        .map(chrono::Duration::from_std)
        .transpose()
        .map_err(|_| {
            ControlPlaneError::validation(
                "invalid_schedule",
                "inspection interval cannot be represented by the scheduler",
            )
        })?
        .map(|interval| accepted_at + interval)
        .or(Some(accepted_at));
    sqlx::query(
        "INSERT INTO inspection_runs (
            id, tenant_id, cluster_id, template, status, schedule,
            created_at, started_at, next_run_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(id.as_uuid())
    .bind(auth.tenant_id.as_uuid())
    .bind(request.cluster_id.as_uuid())
    .bind(inspection_template_name(inspection.template))
    .bind(inspection_status_name(status))
    .bind(inspection.schedule.as_deref())
    .bind(accepted_at)
    .bind(Option::<DateTime<Utc>>::None)
    .bind(next_run_at)
    .execute(&mut **transaction)
    .await?;
    append_workflow_event(
        transaction,
        auth,
        request.cluster_id,
        "inspection",
        id.as_uuid(),
        "inspection_created",
        json!({
            "template": inspection.template,
            "status": status,
            "entry_source": request.source_kind(),
        }),
        correlation_id,
        accepted_at,
    )
    .await?;
    Ok(id)
}

async fn insert_event_entry(
    transaction: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    request: &UnifiedEventEntryRequest,
    request_hash: &str,
    result: &UnifiedEventEntryResult,
    occurred_at: DateTime<Utc>,
) -> Result<bool, ControlPlaneError> {
    Ok(sqlx::query(
        "INSERT INTO workflow_event_entries (
            id, tenant_id, cluster_id, source_kind, idempotency_key,
            request_hash, target_kind, target_id, correlation_id,
            actor_subject, occurred_at, accepted_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
         ON CONFLICT (tenant_id, cluster_id, source_kind, idempotency_key)
         DO NOTHING",
    )
    .bind(result.entry_id)
    .bind(auth.tenant_id.as_uuid())
    .bind(request.cluster_id.as_uuid())
    .bind(result.source_kind.as_str())
    .bind(&request.idempotency_key)
    .bind(request_hash)
    .bind(result.target_kind.as_str())
    .bind(result.target_id)
    .bind(result.correlation_id.as_uuid())
    .bind(&auth.subject)
    .bind(occurred_at)
    .bind(result.accepted_at)
    .execute(&mut **transaction)
    .await?
    .rows_affected()
        == 1)
}

fn event_entry_from_row(
    row: &PgRow,
    expected_request_hash: &str,
) -> Result<UnifiedEventEntryResult, ControlPlaneError> {
    let stored_request_hash: String = row.try_get("request_hash")?;
    if stored_request_hash != expected_request_hash {
        return Err(ControlPlaneError::conflict_code(
            "event_entry_idempotency_conflict",
            "event entry idempotency key is already bound to different request content",
        ));
    }
    Ok(UnifiedEventEntryResult {
        schema_version: EVENT_ENTRY_RESULT_SCHEMA,
        entry_id: row.try_get("id")?,
        source_kind: super::EventEntrySourceKind::parse(row.try_get("source_kind")?)?,
        target_kind: EventEntryTargetKind::parse(row.try_get("target_kind")?)?,
        target_id: row.try_get("target_id")?,
        created: false,
        replayed: true,
        correlation_id: CorrelationId::from_uuid(row.try_get("correlation_id")?),
        accepted_at: row.try_get("accepted_at")?,
    })
}
