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
use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::AlertSeverity;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::IncidentOperationRequest;
use rocketmq_sre_contracts::IncidentOperationResult;
use rocketmq_sre_contracts::IncidentOperationsState;
use rocketmq_sre_contracts::IncidentSlaState;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::TimelineEvent;
use rocketmq_sre_contracts::TimelineEventId;
use rocketmq_sre_contracts::WorkflowActor;
use serde_json::Value;
use serde_json::json;
use sqlx::PgPool;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::ControlPlaneError;
use crate::auth::AuthContext;

const OPERATIONS_STATE_SCHEMA: &str = "rocketmq-sre.incident-operations-state.v1";
const OPERATIONS_RESULT_SCHEMA: &str = "rocketmq-sre.incident-operation-result.v1";
const MAX_REASON_CHARS: usize = 2_048;
const MAX_TITLE_CHARS: usize = 512;
const MAX_RESOURCE_CHARS: usize = 1_024;
const MAX_OWNER_CHARS: usize = 256;
pub(super) const MAX_SUPPRESSION_DAYS: i64 = 30;

#[derive(Clone)]
pub(super) struct OperatorWorkbenchRepository {
    pool: PgPool,
}

#[derive(Clone, Debug)]
struct LockedIncident {
    id: IncidentId,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    title: String,
    resource: Option<String>,
    symptom_family: String,
    severity: Option<AlertSeverity>,
    owner: String,
    occurrence_count: u32,
    status: String,
}

struct AppliedOperation {
    kind: &'static str,
    summary: &'static str,
    reason: Option<String>,
    related_incident_id: Option<IncidentId>,
    details: Value,
}

impl OperatorWorkbenchRepository {
    pub(super) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub(super) async fn state(
        &self,
        auth: &AuthContext,
        incident_id: IncidentId,
    ) -> Result<IncidentOperationsState, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, owner_name, acknowledged_at,
                    acknowledged_by, suppressed_until, suppression_reason,
                    merged_into_incident_id, sla_ack_due_at, sla_resolve_due_at,
                    status, updated_at
             FROM sre_incidents
             WHERE id = $1 AND tenant_id = $2",
        )
        .bind(incident_id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        ensure_cluster_scope(auth, ClusterId::from_uuid(row.try_get("cluster_id")?))?;
        self.state_from_row(&row, Utc::now()).await
    }

    pub(super) async fn apply(
        &self,
        auth: &AuthContext,
        incident_id: IncidentId,
        request: &IncidentOperationRequest,
        correlation_id: CorrelationId,
    ) -> Result<IncidentOperationResult, ControlPlaneError> {
        validate_operation(request)?;
        let mut transaction = self.pool.begin().await?;
        let source = lock_incident(&mut transaction, auth, incident_id).await?;
        ensure_cluster_scope(auth, source.cluster_id)?;
        let now = Utc::now();
        let applied = match request {
            IncidentOperationRequest::Acknowledge { note } => {
                sqlx::query(
                    "UPDATE sre_incidents
                     SET acknowledged_at = COALESCE(acknowledged_at, $1),
                         acknowledged_by = COALESCE(acknowledged_by, $2),
                         updated_at = $1
                     WHERE id = $3",
                )
                .bind(now)
                .bind(&auth.subject)
                .bind(incident_id.as_uuid())
                .execute(&mut *transaction)
                .await?;
                AppliedOperation {
                    kind: "acknowledge",
                    summary: "Incident acknowledged",
                    reason: note.clone(),
                    related_incident_id: None,
                    details: json!({"note": note}),
                }
            }
            IncidentOperationRequest::Assign { owner, reason } => {
                sqlx::query(
                    "UPDATE sre_incidents
                     SET owner_name = $1, assigned_at = $2, updated_at = $2
                     WHERE id = $3",
                )
                .bind(owner.trim())
                .bind(now)
                .bind(incident_id.as_uuid())
                .execute(&mut *transaction)
                .await?;
                AppliedOperation {
                    kind: "assign",
                    summary: "Incident assigned",
                    reason: Some(reason.clone()),
                    related_incident_id: None,
                    details: json!({"owner": owner.trim()}),
                }
            }
            IncidentOperationRequest::Suppress { until, reason } => {
                ensure_active(&source, "suppression")?;
                sqlx::query(
                    "UPDATE sre_incidents
                     SET suppressed_until = $1, suppression_reason = $2, updated_at = $3
                     WHERE id = $4",
                )
                .bind(until)
                .bind(reason)
                .bind(now)
                .bind(incident_id.as_uuid())
                .execute(&mut *transaction)
                .await?;
                AppliedOperation {
                    kind: "suppress",
                    summary: "Incident suppressed for a bounded period",
                    reason: Some(reason.clone()),
                    related_incident_id: None,
                    details: json!({"suppressed_until": until}),
                }
            }
            IncidentOperationRequest::Merge {
                target_incident_id,
                reason,
            } => {
                if *target_incident_id == incident_id {
                    return Err(ControlPlaneError::validation(
                        "invalid_incident_operation",
                        "an incident cannot be merged into itself",
                    ));
                }
                let target = lock_incident(&mut transaction, auth, *target_incident_id).await?;
                if target.cluster_id != source.cluster_id {
                    return Err(ControlPlaneError::forbidden(
                        "cluster_not_allowed",
                        "merged incidents must belong to the same cluster",
                    ));
                }
                sqlx::query(
                    "UPDATE sre_incidents
                     SET merged_into_incident_id = $1, updated_at = $2
                     WHERE id = $3",
                )
                .bind(target_incident_id.as_uuid())
                .bind(now)
                .bind(incident_id.as_uuid())
                .execute(&mut *transaction)
                .await?;
                insert_relation(
                    &mut transaction,
                    auth,
                    &source,
                    *target_incident_id,
                    "duplicate",
                    "operator_merge",
                    now,
                )
                .await?;
                AppliedOperation {
                    kind: "merge",
                    summary: "Incident merged into related incident",
                    reason: Some(reason.clone()),
                    related_incident_id: Some(*target_incident_id),
                    details: json!({"target_incident_id": target_incident_id}),
                }
            }
            IncidentOperationRequest::Split {
                title,
                resource,
                symptom_family,
                reason,
            } => {
                let related = create_related_incident(
                    &mut transaction,
                    auth,
                    &source,
                    title.trim(),
                    resource.as_deref().map(str::trim),
                    symptom_family.trim(),
                    None,
                    now,
                )
                .await?;
                insert_relation(
                    &mut transaction,
                    auth,
                    &source,
                    related,
                    "parent",
                    "operator_split",
                    now,
                )
                .await?;
                append_related_timeline(
                    &mut transaction,
                    auth,
                    source.cluster_id,
                    related,
                    "incident_split_created",
                    "Incident created by operator split",
                    json!({"source_incident_id": incident_id, "reason": reason}),
                    correlation_id,
                    now,
                )
                .await?;
                AppliedOperation {
                    kind: "split",
                    summary: "Incident split into a linked incident",
                    reason: Some(reason.clone()),
                    related_incident_id: Some(related),
                    details: json!({"split_incident_id": related}),
                }
            }
            IncidentOperationRequest::Reopen { reason } => {
                ensure_terminal(&source)?;
                let related = create_related_incident(
                    &mut transaction,
                    auth,
                    &source,
                    &source.title,
                    source.resource.as_deref(),
                    &source.symptom_family,
                    Some(source.id),
                    now,
                )
                .await?;
                insert_relation(
                    &mut transaction,
                    auth,
                    &source,
                    related,
                    "recurrence",
                    "operator_reopen",
                    now,
                )
                .await?;
                append_related_timeline(
                    &mut transaction,
                    auth,
                    source.cluster_id,
                    related,
                    "incident_reopened",
                    "New incident created from terminal incident",
                    json!({"reopened_from_incident_id": incident_id, "reason": reason}),
                    correlation_id,
                    now,
                )
                .await?;
                AppliedOperation {
                    kind: "reopen",
                    summary: "Terminal incident reopened as a linked incident",
                    reason: Some(reason.clone()),
                    related_incident_id: Some(related),
                    details: json!({"reopened_incident_id": related}),
                }
            }
        };
        let timeline_event = persist_operation(&mut transaction, auth, &source, &applied, correlation_id, now).await?;
        transaction.commit().await?;
        let state = self.state(auth, incident_id).await?;
        Ok(IncidentOperationResult {
            schema_version: OPERATIONS_RESULT_SCHEMA.to_owned(),
            state,
            related_incident_id: applied.related_incident_id,
            timeline_event,
            cluster_mutation_performed: false,
        })
    }

    async fn state_from_row(
        &self,
        row: &PgRow,
        now: DateTime<Utc>,
    ) -> Result<IncidentOperationsState, ControlPlaneError> {
        let incident_id = IncidentId::from_uuid(row.try_get("id")?);
        let acknowledged_at: Option<DateTime<Utc>> = row.try_get("acknowledged_at")?;
        let acknowledgement_due_at: DateTime<Utc> = row.try_get("sla_ack_due_at")?;
        let resolution_due_at: DateTime<Utc> = row.try_get("sla_resolve_due_at")?;
        let status: String = row.try_get("status")?;
        let updated_at: DateTime<Utc> = row.try_get("updated_at")?;
        let split_ids = sqlx::query_scalar::<_, Uuid>(
            "SELECT related_incident_id
             FROM incident_operations
             WHERE incident_id = $1 AND operation_kind = 'split'
               AND related_incident_id IS NOT NULL
             ORDER BY sequence_id DESC
             LIMIT 64",
        )
        .bind(incident_id.as_uuid())
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(IncidentId::from_uuid)
        .collect();
        let acknowledgement_breached =
            acknowledged_at.map_or(now > acknowledgement_due_at, |at| at > acknowledgement_due_at);
        let resolution_clock = if matches!(status.as_str(), "resolved" | "escalated") {
            updated_at
        } else {
            now
        };
        Ok(IncidentOperationsState {
            schema_version: OPERATIONS_STATE_SCHEMA.to_owned(),
            incident_id,
            tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
            cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
            owner: row.try_get("owner_name")?,
            acknowledged_by: row.try_get("acknowledged_by")?,
            suppressed_until: row.try_get("suppressed_until")?,
            suppression_reason: row.try_get("suppression_reason")?,
            merged_into_incident_id: row
                .try_get::<Option<Uuid>, _>("merged_into_incident_id")?
                .map(IncidentId::from_uuid),
            split_incident_ids: split_ids,
            sla: IncidentSlaState {
                acknowledgement_due_at,
                resolution_due_at,
                acknowledged_at,
                acknowledgement_breached,
                resolution_breached: resolution_clock > resolution_due_at,
            },
            updated_at,
        })
    }
}

pub(super) fn validate_operation(request: &IncidentOperationRequest) -> Result<(), ControlPlaneError> {
    match request {
        IncidentOperationRequest::Acknowledge { note } => {
            if let Some(value) = note {
                validate_text("note", value, MAX_REASON_CHARS)?;
            }
        }
        IncidentOperationRequest::Assign { owner, reason } => {
            validate_text("owner", owner, MAX_OWNER_CHARS)?;
            validate_text("reason", reason, MAX_REASON_CHARS)?;
        }
        IncidentOperationRequest::Merge { reason, .. } | IncidentOperationRequest::Reopen { reason } => {
            validate_text("reason", reason, MAX_REASON_CHARS)?;
        }
        IncidentOperationRequest::Split {
            title,
            resource,
            symptom_family,
            reason,
        } => {
            validate_text("title", title, MAX_TITLE_CHARS)?;
            if let Some(value) = resource {
                validate_text("resource", value, MAX_RESOURCE_CHARS)?;
            }
            validate_text("symptom_family", symptom_family, 128)?;
            validate_text("reason", reason, MAX_REASON_CHARS)?;
        }
        IncidentOperationRequest::Suppress { until, reason } => {
            validate_text("reason", reason, MAX_REASON_CHARS)?;
            let now = Utc::now();
            if *until <= now || *until > now + Duration::days(MAX_SUPPRESSION_DAYS) {
                return Err(ControlPlaneError::validation(
                    "invalid_incident_operation",
                    "suppression must end within the next 30 days",
                ));
            }
        }
    }
    Ok(())
}

fn validate_text(name: &'static str, value: &str, max_chars: usize) -> Result<(), ControlPlaneError> {
    let length = value.trim().chars().count();
    if length == 0 || length > max_chars {
        return Err(ControlPlaneError::validation(
            "invalid_incident_operation",
            format!("{name} must contain between 1 and {max_chars} characters"),
        ));
    }
    Ok(())
}

async fn lock_incident(
    transaction: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    incident_id: IncidentId,
) -> Result<LockedIncident, ControlPlaneError> {
    let row = sqlx::query(
        "SELECT id, tenant_id, cluster_id, title, resource, symptom_family,
                severity, owner_name, occurrence_count, status
         FROM sre_incidents
         WHERE id = $1 AND tenant_id = $2
         FOR UPDATE",
    )
    .bind(incident_id.as_uuid())
    .bind(auth.tenant_id.as_uuid())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(ControlPlaneError::NotFound)?;
    let occurrence_count: i32 = row.try_get("occurrence_count")?;
    Ok(LockedIncident {
        id: IncidentId::from_uuid(row.try_get("id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        title: row.try_get("title")?,
        resource: row.try_get("resource")?,
        symptom_family: row.try_get("symptom_family")?,
        severity: parse_severity(row.try_get("severity")?)?,
        owner: row.try_get("owner_name")?,
        occurrence_count: u32::try_from(occurrence_count)
            .map_err(|_| ControlPlaneError::configuration("stored occurrence count is invalid"))?,
        status: row.try_get("status")?,
    })
}

fn parse_severity(value: Option<String>) -> Result<Option<AlertSeverity>, ControlPlaneError> {
    value
        .map(|severity| {
            serde_json::from_value(Value::String(severity))
                .map_err(|_| ControlPlaneError::configuration("stored incident severity is invalid"))
        })
        .transpose()
}

fn ensure_cluster_scope(auth: &AuthContext, cluster_id: ClusterId) -> Result<(), ControlPlaneError> {
    if !auth.clusters.contains(&cluster_id) {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "incident cluster is outside the authenticated scope",
        ));
    }
    Ok(())
}

fn ensure_active(incident: &LockedIncident, action: &str) -> Result<(), ControlPlaneError> {
    if matches!(incident.status.as_str(), "resolved" | "escalated") {
        return Err(ControlPlaneError::conflict(format!(
            "terminal incidents cannot accept {action}"
        )));
    }
    Ok(())
}

fn ensure_terminal(incident: &LockedIncident) -> Result<(), ControlPlaneError> {
    if !matches!(incident.status.as_str(), "resolved" | "escalated") {
        return Err(ControlPlaneError::conflict("only terminal incidents can be reopened"));
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the related incident insert records the complete immutable provenance tuple"
)]
async fn create_related_incident(
    transaction: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    source: &LockedIncident,
    title: &str,
    resource: Option<&str>,
    symptom_family: &str,
    reopened_from: Option<IncidentId>,
    now: DateTime<Utc>,
) -> Result<IncidentId, ControlPlaneError> {
    let id = IncidentId::new();
    let (ack_due, resolve_due) = sla_deadlines(source.severity, now);
    let fingerprint = if reopened_from.is_some() {
        format!("operator-reopen:{id}")
    } else {
        format!("operator-split:{id}")
    };
    sqlx::query(
        "INSERT INTO sre_incidents (
            id, tenant_id, cluster_id, title, resource, symptom_family,
            fingerprint, status, severity, owner_name, occurrence_count,
            reopened_from_incident_id, created_by_subject, created_at, updated_at,
            sla_ack_due_at, sla_resolve_due_at
         ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, 'new', $8, $9, $10, $11, $12,
            $13, $13, $14, $15
         )",
    )
    .bind(id.as_uuid())
    .bind(source.tenant_id.as_uuid())
    .bind(source.cluster_id.as_uuid())
    .bind(title)
    .bind(resource)
    .bind(symptom_family)
    .bind(fingerprint)
    .bind(source.severity.map(severity_name))
    .bind(&source.owner)
    .bind(i32::try_from(source.occurrence_count.saturating_add(1)).unwrap_or(i32::MAX))
    .bind(reopened_from.map(IncidentId::as_uuid))
    .bind(&auth.subject)
    .bind(now)
    .bind(ack_due)
    .bind(resolve_due)
    .execute(&mut **transaction)
    .await?;
    Ok(id)
}

pub(super) fn sla_deadlines(severity: Option<AlertSeverity>, now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    match severity {
        Some(AlertSeverity::Critical) => (now + Duration::minutes(15), now + Duration::hours(4)),
        Some(AlertSeverity::Error) => (now + Duration::minutes(30), now + Duration::hours(8)),
        Some(AlertSeverity::Warning) => (now + Duration::hours(2), now + Duration::hours(24)),
        Some(AlertSeverity::Info) | None => (now + Duration::hours(8), now + Duration::hours(72)),
    }
}

const fn severity_name(severity: AlertSeverity) -> &'static str {
    match severity {
        AlertSeverity::Info => "info",
        AlertSeverity::Warning => "warning",
        AlertSeverity::Error => "error",
        AlertSeverity::Critical => "critical",
    }
}

async fn insert_relation(
    transaction: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    source: &LockedIncident,
    target_id: IncidentId,
    relation_kind: &str,
    reason_code: &str,
    now: DateTime<Utc>,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO incident_relations (
            id, tenant_id, cluster_id, from_incident_id, to_incident_id,
            relation_kind, reason_code, evidence_ids, created_by, created_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, '{}', $8, $9)
         ON CONFLICT (tenant_id, cluster_id, from_incident_id, to_incident_id, relation_kind)
         DO NOTHING",
    )
    .bind(Uuid::new_v4())
    .bind(auth.tenant_id.as_uuid())
    .bind(source.cluster_id.as_uuid())
    .bind(source.id.as_uuid())
    .bind(target_id.as_uuid())
    .bind(relation_kind)
    .bind(reason_code)
    .bind(&auth.subject)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn persist_operation(
    transaction: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    source: &LockedIncident,
    applied: &AppliedOperation,
    correlation_id: CorrelationId,
    now: DateTime<Utc>,
) -> Result<TimelineEvent, ControlPlaneError> {
    let operation_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO incident_operations (
            operation_id, tenant_id, cluster_id, incident_id, operation_kind,
            actor_subject, reason, related_incident_id, details, correlation_id,
            occurred_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
    )
    .bind(operation_id)
    .bind(auth.tenant_id.as_uuid())
    .bind(source.cluster_id.as_uuid())
    .bind(source.id.as_uuid())
    .bind(applied.kind)
    .bind(&auth.subject)
    .bind(&applied.reason)
    .bind(applied.related_incident_id.map(IncidentId::as_uuid))
    .bind(&applied.details)
    .bind(correlation_id.as_uuid())
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    let event = TimelineEvent {
        id: TimelineEventId::new(),
        tenant_id: auth.tenant_id,
        cluster_id: source.cluster_id,
        investigation_id: None,
        incident_id: Some(source.id),
        event_type: format!("incident_{}", applied.kind),
        summary: applied.summary.to_owned(),
        details: json!({
            "operation_id": operation_id,
            "reason": applied.reason,
            "related_incident_id": applied.related_incident_id,
            "metadata": applied.details,
            "cluster_mutation_performed": false,
        }),
        correlation_id,
        actor: WorkflowActor {
            subject: auth.subject.clone(),
            display_name: None,
        },
        occurred_at: now,
    };
    insert_timeline(transaction, &event).await?;
    Ok(event)
}

#[allow(
    clippy::too_many_arguments,
    reason = "related timeline events preserve their complete audit tuple"
)]
async fn append_related_timeline(
    transaction: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    cluster_id: ClusterId,
    incident_id: IncidentId,
    event_type: &str,
    summary: &str,
    details: Value,
    correlation_id: CorrelationId,
    now: DateTime<Utc>,
) -> Result<(), ControlPlaneError> {
    insert_timeline(
        transaction,
        &TimelineEvent {
            id: TimelineEventId::new(),
            tenant_id: auth.tenant_id,
            cluster_id,
            investigation_id: None,
            incident_id: Some(incident_id),
            event_type: event_type.to_owned(),
            summary: summary.to_owned(),
            details,
            correlation_id,
            actor: WorkflowActor {
                subject: auth.subject.clone(),
                display_name: None,
            },
            occurred_at: now,
        },
    )
    .await
}

async fn insert_timeline(
    transaction: &mut Transaction<'_, Postgres>,
    event: &TimelineEvent,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO incident_timeline (
            event_id, tenant_id, cluster_id, investigation_id, incident_id,
            event_type, summary, details, correlation_id, actor_subject,
            actor_display_name, occurred_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
    )
    .bind(event.id.as_uuid())
    .bind(event.tenant_id.as_uuid())
    .bind(event.cluster_id.as_uuid())
    .bind(event.investigation_id.map(|id| id.as_uuid()))
    .bind(event.incident_id.map(IncidentId::as_uuid))
    .bind(&event.event_type)
    .bind(&event.summary)
    .bind(&event.details)
    .bind(event.correlation_id.as_uuid())
    .bind(&event.actor.subject)
    .bind(&event.actor.display_name)
    .bind(event.occurred_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}
