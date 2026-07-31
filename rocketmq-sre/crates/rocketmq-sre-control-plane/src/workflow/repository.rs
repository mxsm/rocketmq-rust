// Copyright 2023 The RocketMQ Rust Authors
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

use std::collections::BTreeSet;
use std::collections::HashMap;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::Conversation;
use rocketmq_sre_contracts::ConversationId;
use rocketmq_sre_contracts::ConversationStatus;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::DiagnosisRevision;
use rocketmq_sre_contracts::DiagnosisRevisionId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::Incident;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::IncidentStatus;
use rocketmq_sre_contracts::InspectionRun;
use rocketmq_sre_contracts::InspectionRunId;
use rocketmq_sre_contracts::InspectionStatus;
use rocketmq_sre_contracts::InspectionTemplate;
use rocketmq_sre_contracts::Investigation;
use rocketmq_sre_contracts::InvestigationId;
use rocketmq_sre_contracts::InvestigationStatus;
use rocketmq_sre_contracts::ModelInvocationId;
use rocketmq_sre_contracts::Recommendation;
use rocketmq_sre_contracts::RecommendationId;
use rocketmq_sre_contracts::RecommendationStatus;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::TimelineEvent;
use rocketmq_sre_contracts::TimelineEventId;
use rocketmq_sre_contracts::WorkflowActor;
use serde_json::Value;
use serde_json::json;
use sha2::Digest;
use sha2::Sha256;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use super::ConversationCreateRequest;
use super::ConversationView;
use super::IncidentCreateRequest;
use super::IncidentView;
use super::InspectionCreateRequest;
use super::InspectionView;
use super::InvestigationCreateRequest;
use super::InvestigationView;
use super::PromoteInvestigationRequest;
use super::RecommendationDispositionRequest;
use super::RecommendationPromotionTarget;
use super::WorkflowListQuery;
use super::WorkflowPage;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::inspection::DueInspection;
use crate::inspection::InspectionPackRun;
use crate::inspection::NewRecommendation;

impl PostgresRepository {
    pub(crate) async fn create_conversation(
        &self,
        auth: &AuthContext,
        request: &ConversationCreateRequest,
        correlation_id: CorrelationId,
    ) -> Result<ConversationView, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        ensure_cluster_scope(&mut transaction, auth.tenant_id, request.cluster_id).await?;
        let now = Utc::now();
        let conversation_id = ConversationId::new();
        sqlx::query(
            "INSERT INTO conversations (
                id, tenant_id, cluster_id, question, resource, status,
                created_by_subject, created_at, updated_at
             ) VALUES ($1, $2, $3, $4, $5, 'active', $6, $7, $7)",
        )
        .bind(conversation_id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(request.cluster_id.as_uuid())
        .bind(request.question.trim())
        .bind(request.resource.as_deref())
        .bind(&auth.subject)
        .bind(now)
        .execute(&mut *transaction)
        .await?;

        let investigation = if request.persist_investigation {
            let inferred = infer_intent(&request.question, request.resource.as_deref());
            let investigation = insert_investigation(
                &mut transaction,
                auth,
                request.cluster_id,
                Some(conversation_id),
                bounded_title(&request.question),
                request.resource.as_deref(),
                &inferred,
                now,
                correlation_id,
            )
            .await?;
            sqlx::query(
                "UPDATE conversations
                 SET status = 'promoted', investigation_id = $2, updated_at = $3
                 WHERE id = $1",
            )
            .bind(conversation_id.as_uuid())
            .bind(investigation.id.as_uuid())
            .bind(now)
            .execute(&mut *transaction)
            .await?;
            Some(investigation)
        } else {
            None
        };
        append_workflow_event(
            &mut transaction,
            auth,
            request.cluster_id,
            "conversation",
            conversation_id.as_uuid(),
            "conversation_created",
            json!({"persisted_investigation": investigation.is_some()}),
            correlation_id,
            now,
        )
        .await?;
        transaction.commit().await?;
        let mut conversation = Conversation {
            id: conversation_id,
            tenant_id: auth.tenant_id,
            cluster_id: request.cluster_id,
            question: request.question.trim().to_owned(),
            resource: request.resource.clone(),
            status: ConversationStatus::Active,
            investigation_id: None,
            created_by: actor(auth),
            created_at: now,
            updated_at: now,
        };
        if let Some(value) = &investigation {
            conversation.status = ConversationStatus::Promoted;
            conversation.investigation_id = Some(value.id);
        }
        Ok(ConversationView {
            conversation,
            investigation,
        })
    }

    pub(crate) async fn conversation(
        &self,
        auth: &AuthContext,
        id: ConversationId,
    ) -> Result<ConversationView, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, question, resource, status, investigation_id,
                    created_by_subject, created_by_display_name, created_at, updated_at
             FROM conversations
             WHERE id = $1 AND tenant_id = $2",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let conversation = conversation_from_row(&row)?;
        enforce_auth_cluster(auth, conversation.cluster_id)?;
        let investigation = if let Some(investigation_id) = conversation.investigation_id {
            Some(self.investigation_record(auth, investigation_id).await?)
        } else {
            None
        };
        Ok(ConversationView {
            conversation,
            investigation,
        })
    }

    pub(crate) async fn list_conversations(
        &self,
        auth: &AuthContext,
        query: &WorkflowListQuery,
    ) -> Result<WorkflowPage<ConversationView>, ControlPlaneError> {
        enforce_auth_cluster(auth, query.cluster_id)?;
        let limit = query.bounded_limit()?;
        let cursor = query.cursor_uuid()?;
        let rows = sqlx::query(
            "SELECT c.id, c.tenant_id, c.cluster_id, c.question, c.resource, c.status, c.investigation_id,
                    c.created_by_subject, c.created_by_display_name, c.created_at, c.updated_at
             FROM conversations c
             WHERE c.tenant_id = $1 AND c.cluster_id = $2
               AND (
                    $3::UUID IS NULL
                    OR (c.created_at, c.id) < (
                        SELECT cursor.created_at, cursor.id
                        FROM conversations cursor
                        WHERE cursor.id = $3 AND cursor.tenant_id = $1 AND cursor.cluster_id = $2
                    )
               )
             ORDER BY c.created_at DESC, c.id DESC
             LIMIT $4",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(query.cluster_id.as_uuid())
        .bind(cursor)
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await?;
        let conversations = rows.iter().map(conversation_from_row).collect::<Result<Vec<_>, _>>()?;
        let page = WorkflowPage::from_window(conversations, limit, |item| item.id.as_uuid());
        let investigation_ids = page
            .items
            .iter()
            .filter_map(|item| item.investigation_id)
            .collect::<Vec<_>>();
        let investigations = self
            .investigations_by_ids(auth, query.cluster_id, &investigation_ids)
            .await?;
        Ok(page.map(|conversation| ConversationView {
            investigation: conversation
                .investigation_id
                .and_then(|id| investigations.get(&id).cloned()),
            conversation,
        }))
    }

    pub(crate) async fn create_investigation(
        &self,
        auth: &AuthContext,
        request: &InvestigationCreateRequest,
        correlation_id: CorrelationId,
    ) -> Result<InvestigationView, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        ensure_cluster_scope(&mut transaction, auth.tenant_id, request.cluster_id).await?;
        let now = Utc::now();
        let investigation = insert_investigation(
            &mut transaction,
            auth,
            request.cluster_id,
            request.conversation_id,
            request.title.clone(),
            request.resource.as_deref(),
            &request.symptom_family,
            now,
            correlation_id,
        )
        .await?;
        if let Some(conversation_id) = request.conversation_id {
            let updated = sqlx::query(
                "UPDATE conversations
                 SET status = 'promoted', investigation_id = $3, updated_at = $4
                 WHERE id = $1 AND tenant_id = $2 AND cluster_id = $5",
            )
            .bind(conversation_id.as_uuid())
            .bind(auth.tenant_id.as_uuid())
            .bind(investigation.id.as_uuid())
            .bind(now)
            .bind(request.cluster_id.as_uuid())
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            if updated != 1 {
                return Err(ControlPlaneError::NotFound);
            }
        }
        transaction.commit().await?;
        Ok(InvestigationView {
            investigation,
            timeline: Vec::new(),
        })
    }

    pub(crate) async fn investigation(
        &self,
        auth: &AuthContext,
        id: InvestigationId,
    ) -> Result<InvestigationView, ControlPlaneError> {
        let investigation = self.investigation_record(auth, id).await?;
        let timeline = self.timeline(auth, Some(id), investigation.incident_id).await?;
        Ok(InvestigationView {
            investigation,
            timeline,
        })
    }

    pub(crate) async fn list_investigations(
        &self,
        auth: &AuthContext,
        query: &WorkflowListQuery,
    ) -> Result<WorkflowPage<InvestigationView>, ControlPlaneError> {
        enforce_auth_cluster(auth, query.cluster_id)?;
        let limit = query.bounded_limit()?;
        let cursor = query.cursor_uuid()?;
        let rows = sqlx::query(
            "SELECT i.id, i.tenant_id, i.cluster_id, i.conversation_id, i.incident_id, i.title, i.resource,
                    i.symptom_family, i.fingerprint, i.status, i.created_by_subject,
                    i.created_by_display_name, i.created_at, i.updated_at
             FROM investigations i
             WHERE i.tenant_id = $1 AND i.cluster_id = $2
               AND (
                    $3::UUID IS NULL
                    OR (i.created_at, i.id) < (
                        SELECT cursor.created_at, cursor.id
                        FROM investigations cursor
                        WHERE cursor.id = $3 AND cursor.tenant_id = $1 AND cursor.cluster_id = $2
                    )
               )
             ORDER BY i.created_at DESC, i.id DESC
             LIMIT $4",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(query.cluster_id.as_uuid())
        .bind(cursor)
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await?;
        let items = rows.iter().map(investigation_from_row).collect::<Result<Vec<_>, _>>()?;
        Ok(
            WorkflowPage::from_window(items, limit, |item| item.id.as_uuid()).map(|investigation| InvestigationView {
                investigation,
                timeline: Vec::new(),
            }),
        )
    }

    async fn investigation_record(
        &self,
        auth: &AuthContext,
        id: InvestigationId,
    ) -> Result<Investigation, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, conversation_id, incident_id, title, resource,
                    symptom_family, fingerprint, status, created_by_subject,
                    created_by_display_name, created_at, updated_at
             FROM investigations
             WHERE id = $1 AND tenant_id = $2",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let investigation = investigation_from_row(&row)?;
        enforce_auth_cluster(auth, investigation.cluster_id)?;
        Ok(investigation)
    }

    async fn investigations_by_ids(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        ids: &[InvestigationId],
    ) -> Result<HashMap<InvestigationId, Investigation>, ControlPlaneError> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let ids = ids.iter().copied().map(InvestigationId::as_uuid).collect::<Vec<_>>();
        let rows = sqlx::query(
            "SELECT id, tenant_id, cluster_id, conversation_id, incident_id, title, resource,
                    symptom_family, fingerprint, status, created_by_subject,
                    created_by_display_name, created_at, updated_at
             FROM investigations
             WHERE tenant_id = $1 AND cluster_id = $2 AND id = ANY($3)",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                let investigation = investigation_from_row(row)?;
                Ok((investigation.id, investigation))
            })
            .collect()
    }

    pub(crate) async fn promote_investigation(
        &self,
        auth: &AuthContext,
        id: InvestigationId,
        request: &PromoteInvestigationRequest,
        correlation_id: CorrelationId,
    ) -> Result<IncidentView, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, conversation_id, incident_id, title, resource,
                    symptom_family, fingerprint, status, created_by_subject,
                    created_by_display_name, created_at, updated_at
             FROM investigations
             WHERE id = $1 AND tenant_id = $2
             FOR UPDATE",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let investigation = investigation_from_row(&row)?;
        enforce_auth_cluster(auth, investigation.cluster_id)?;
        if let Some(incident_id) = investigation.incident_id {
            transaction.commit().await?;
            return self.incident(auth, incident_id).await;
        }
        let incident_id = IncidentId::new();
        let now = Utc::now();
        let title = request.title.as_deref().unwrap_or(&investigation.title);
        insert_incident(
            &mut transaction,
            auth,
            incident_id,
            Some(investigation.id),
            investigation.cluster_id,
            title,
            investigation.resource.as_deref(),
            &investigation.symptom_family,
            &investigation.fingerprint,
            now,
        )
        .await?;
        sqlx::query(
            "UPDATE investigations
             SET incident_id = $2, status = 'promoted', updated_at = $3
             WHERE id = $1",
        )
        .bind(id.as_uuid())
        .bind(incident_id.as_uuid())
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        append_timeline(
            &mut transaction,
            auth,
            investigation.cluster_id,
            Some(id),
            Some(incident_id),
            "investigation_promoted",
            "Investigation promoted to incident",
            json!({"reason": request.reason}),
            correlation_id,
            now,
        )
        .await?;
        append_workflow_event(
            &mut transaction,
            auth,
            investigation.cluster_id,
            "incident",
            incident_id.as_uuid(),
            "incident_created",
            json!({"investigation_id": id}),
            correlation_id,
            now,
        )
        .await?;
        transaction.commit().await?;
        self.incident(auth, incident_id).await
    }

    pub(crate) async fn create_incident(
        &self,
        auth: &AuthContext,
        request: &IncidentCreateRequest,
        correlation_id: CorrelationId,
    ) -> Result<IncidentView, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        ensure_cluster_scope(&mut transaction, auth.tenant_id, request.cluster_id).await?;
        let id = IncidentId::new();
        let now = Utc::now();
        let fingerprint = fingerprint(
            auth.tenant_id,
            request.cluster_id,
            request.resource.as_deref(),
            &request.symptom_family,
            now,
        );
        insert_incident(
            &mut transaction,
            auth,
            id,
            None,
            request.cluster_id,
            &request.title,
            request.resource.as_deref(),
            &request.symptom_family,
            &fingerprint,
            now,
        )
        .await?;
        append_timeline(
            &mut transaction,
            auth,
            request.cluster_id,
            None,
            Some(id),
            "incident_created",
            "Incident created",
            Value::Null,
            correlation_id,
            now,
        )
        .await?;
        append_workflow_event(
            &mut transaction,
            auth,
            request.cluster_id,
            "incident",
            id.as_uuid(),
            "incident_created",
            Value::Null,
            correlation_id,
            now,
        )
        .await?;
        transaction.commit().await?;
        self.incident(auth, id).await
    }

    pub(crate) async fn incident(&self, auth: &AuthContext, id: IncidentId) -> Result<IncidentView, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, investigation_id, title, resource,
                    symptom_family, fingerprint, status, severity, owner_name,
                    occurrence_count, last_alert_at, reopened_from_incident_id,
                    created_at, updated_at
             FROM sre_incidents
             WHERE id = $1 AND tenant_id = $2",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let incident = incident_from_row(&row)?;
        enforce_auth_cluster(auth, incident.cluster_id)?;
        let investigation_id = row
            .try_get::<Option<Uuid>, _>("investigation_id")
            .map_err(ControlPlaneError::from)?
            .map(InvestigationId::from_uuid);
        let investigation = if let Some(value) = investigation_id {
            Some(self.investigation_record(auth, value).await?)
        } else {
            None
        };
        let timeline = self.timeline(auth, investigation_id, Some(id)).await?;
        let revisions = self.diagnosis_revisions(auth, id).await?;
        Ok(IncidentView {
            incident,
            investigation,
            timeline,
            diagnosis_revisions: revisions,
        })
    }

    pub(crate) async fn list_incidents(
        &self,
        auth: &AuthContext,
        query: &WorkflowListQuery,
    ) -> Result<WorkflowPage<IncidentView>, ControlPlaneError> {
        enforce_auth_cluster(auth, query.cluster_id)?;
        let limit = query.bounded_limit()?;
        let cursor = query.cursor_uuid()?;
        let rows = sqlx::query(
            "SELECT i.id, i.tenant_id, i.cluster_id, i.title, i.resource,
                    i.symptom_family, i.fingerprint, i.status, i.severity,
                    i.owner_name, i.occurrence_count, i.last_alert_at,
                    i.reopened_from_incident_id, i.created_at, i.updated_at
             FROM sre_incidents i
             WHERE i.tenant_id = $1 AND i.cluster_id = $2
               AND (
                    $3::UUID IS NULL
                    OR (i.created_at, i.id) < (
                        SELECT cursor.created_at, cursor.id
                        FROM sre_incidents cursor
                        WHERE cursor.id = $3 AND cursor.tenant_id = $1 AND cursor.cluster_id = $2
                    )
               )
             ORDER BY i.created_at DESC, i.id DESC
             LIMIT $4",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(query.cluster_id.as_uuid())
        .bind(cursor)
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await?;
        let items = rows.iter().map(incident_from_row).collect::<Result<Vec<_>, _>>()?;
        Ok(
            WorkflowPage::from_window(items, limit, |item| item.id.as_uuid()).map(|incident| IncidentView {
                incident,
                investigation: None,
                timeline: Vec::new(),
                diagnosis_revisions: Vec::new(),
            }),
        )
    }

    pub(crate) async fn transition_incident(
        &self,
        auth: &AuthContext,
        id: IncidentId,
        next: IncidentStatus,
        reason: &str,
        correlation_id: CorrelationId,
    ) -> Result<IncidentView, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, investigation_id, title, resource,
                    symptom_family, fingerprint, status, severity, owner_name,
                    occurrence_count, last_alert_at, reopened_from_incident_id,
                    created_at, updated_at
             FROM sre_incidents
             WHERE id = $1 AND tenant_id = $2
             FOR UPDATE",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let mut incident = incident_from_row(&row)?;
        enforce_auth_cluster(auth, incident.cluster_id)?;
        let investigation_id = row
            .try_get::<Option<Uuid>, _>("investigation_id")?
            .map(InvestigationId::from_uuid);
        let previous = incident.status;
        let now = Utc::now();
        incident.transition(next, now).map_err(|_| {
            ControlPlaneError::conflict(format!(
                "incident cannot transition from {} to {}",
                incident_status_name(previous),
                incident_status_name(next)
            ))
        })?;
        let updated = sqlx::query(
            "UPDATE sre_incidents
             SET status = $2, updated_at = $3,
                 workflow_checkpoint = workflow_checkpoint || $4::JSONB
             WHERE id = $1 AND status = $5",
        )
        .bind(id.as_uuid())
        .bind(incident_status_name(next))
        .bind(now)
        .bind(json!({
            "last_transition": incident_status_name(next),
            "correlation_id": correlation_id,
            "at": now,
        }))
        .bind(incident_status_name(previous))
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(ControlPlaneError::conflict(
                "incident state changed while applying the transition",
            ));
        }
        append_timeline(
            &mut transaction,
            auth,
            incident.cluster_id,
            investigation_id,
            Some(id),
            "incident_status_changed",
            reason,
            json!({
                "from": incident_status_name(previous),
                "to": incident_status_name(next),
            }),
            correlation_id,
            now,
        )
        .await?;
        append_workflow_event(
            &mut transaction,
            auth,
            incident.cluster_id,
            "incident",
            id.as_uuid(),
            "incident_status_changed",
            json!({
                "from": incident_status_name(previous),
                "to": incident_status_name(next),
            }),
            correlation_id,
            now,
        )
        .await?;
        transaction.commit().await?;
        self.incident(auth, id).await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "an immutable diagnosis revision records its complete provenance tuple"
    )]
    pub(crate) async fn persist_diagnosis_revision(
        &self,
        auth: &AuthContext,
        incident_id: IncidentId,
        next_status: IncidentStatus,
        rule_result: Value,
        hypotheses: Value,
        evidence_ids: Vec<EvidenceId>,
        partial: bool,
        primary_model_invocation_id: Option<ModelInvocationId>,
        diagnosis_mode: &'static str,
        correlation_id: CorrelationId,
    ) -> Result<DiagnosisRevision, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, investigation_id, title, resource,
                    symptom_family, fingerprint, status, severity, owner_name,
                    occurrence_count, last_alert_at, reopened_from_incident_id,
                    created_at, updated_at
             FROM sre_incidents
             WHERE id = $1 AND tenant_id = $2
             FOR UPDATE",
        )
        .bind(incident_id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let mut incident = incident_from_row(&row)?;
        enforce_auth_cluster(auth, incident.cluster_id)?;
        let investigation_id = row
            .try_get::<Option<Uuid>, _>("investigation_id")?
            .map(InvestigationId::from_uuid);
        let previous = incident.status;
        let now = Utc::now();
        incident.transition(next_status, now).map_err(|_| {
            ControlPlaneError::conflict(format!(
                "diagnosis cannot transition incident from {} to {}",
                incident_status_name(previous),
                incident_status_name(next_status)
            ))
        })?;

        let revision: i32 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(revision), 0) + 1
             FROM diagnosis_revisions
             WHERE incident_id = $1",
        )
        .bind(incident_id.as_uuid())
        .fetch_one(&mut *transaction)
        .await?;
        let id = DiagnosisRevisionId::new();
        let evidence_uuids = evidence_ids.iter().map(|value| value.as_uuid()).collect::<Vec<_>>();
        sqlx::query(
            "INSERT INTO diagnosis_revisions (
                id, incident_id, revision, status, rule_result, hypotheses,
                evidence_ids, primary_model_invocation_id, execution_eligible, partial, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, FALSE, $9, $10)",
        )
        .bind(id.as_uuid())
        .bind(incident_id.as_uuid())
        .bind(revision)
        .bind(incident_status_name(next_status))
        .bind(&rule_result)
        .bind(&hypotheses)
        .bind(&evidence_uuids)
        .bind(primary_model_invocation_id.map(ModelInvocationId::as_uuid))
        .bind(partial)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE model_invocations
             SET diagnosis_revision_id = $1
             WHERE tenant_id = $2 AND cluster_id = $3 AND incident_id = $4
               AND correlation_id = $5 AND diagnosis_revision_id IS NULL",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(incident.cluster_id.as_uuid())
        .bind(incident_id.as_uuid())
        .bind(correlation_id.as_uuid())
        .execute(&mut *transaction)
        .await?;
        if let Some(model_invocation_id) = primary_model_invocation_id {
            let linked: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                    SELECT 1
                    FROM model_invocations
                    WHERE id = $1 AND tenant_id = $2 AND cluster_id = $3
                      AND incident_id = $4 AND diagnosis_revision_id = $5
                 )",
            )
            .bind(model_invocation_id.as_uuid())
            .bind(auth.tenant_id.as_uuid())
            .bind(incident.cluster_id.as_uuid())
            .bind(incident_id.as_uuid())
            .bind(id.as_uuid())
            .fetch_one(&mut *transaction)
            .await?;
            if !linked {
                return Err(ControlPlaneError::conflict(
                    "primary model invocation could not be linked to the diagnosis revision",
                ));
            }
        }

        sqlx::query(
            "UPDATE sre_incidents
             SET status = $2, updated_at = $3,
                 workflow_checkpoint = workflow_checkpoint || $4::JSONB
             WHERE id = $1 AND status = $5",
        )
        .bind(incident_id.as_uuid())
        .bind(incident_status_name(next_status))
        .bind(now)
        .bind(json!({
            "diagnosis_revision": revision,
            "diagnosis_mode": diagnosis_mode,
            "primary_model_invocation_id": primary_model_invocation_id,
            "execution_eligible": false,
            "correlation_id": correlation_id,
        }))
        .bind(incident_status_name(previous))
        .execute(&mut *transaction)
        .await?;
        append_timeline(
            &mut transaction,
            auth,
            incident.cluster_id,
            investigation_id,
            Some(incident_id),
            "diagnosis_revision_created",
            "Read-only diagnosis revision completed",
            json!({
                "revision": revision,
                "status": incident_status_name(next_status),
                "partial": partial,
                "evidence_count": evidence_ids.len(),
                "mode": diagnosis_mode,
                "primary_model_invocation_id": primary_model_invocation_id,
                "execution_eligible": false,
            }),
            correlation_id,
            now,
        )
        .await?;
        append_workflow_event(
            &mut transaction,
            auth,
            incident.cluster_id,
            "incident",
            incident_id.as_uuid(),
            "diagnosis_revision_created",
            json!({
                "revision": revision,
                "status": incident_status_name(next_status),
                "partial": partial,
            }),
            correlation_id,
            now,
        )
        .await?;
        transaction.commit().await?;

        Ok(DiagnosisRevision {
            id,
            incident_id,
            revision: u32::try_from(revision).map_err(|_| {
                ControlPlaneError::validation(
                    "source_unavailable",
                    "diagnosis revision is outside the supported range",
                )
            })?,
            status: next_status,
            rule_result,
            hypotheses,
            evidence_ids,
            primary_model_invocation_id,
            execution_eligible: false,
            partial,
            created_at: now,
        })
    }

    pub(crate) async fn create_inspection(
        &self,
        auth: &AuthContext,
        request: &InspectionCreateRequest,
        correlation_id: CorrelationId,
    ) -> Result<InspectionView, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        ensure_cluster_scope(&mut transaction, auth.tenant_id, request.cluster_id).await?;
        let id = InspectionRunId::new();
        let now = Utc::now();
        let status = InspectionStatus::Scheduled;
        let next_run_at = request
            .schedule_interval()?
            .map(chrono::Duration::from_std)
            .transpose()
            .map_err(|_| {
                ControlPlaneError::validation(
                    "invalid_schedule",
                    "inspection interval cannot be represented by the scheduler",
                )
            })?
            .map(|interval| now + interval)
            .or(Some(now));
        sqlx::query(
            "INSERT INTO inspection_runs (
                id, tenant_id, cluster_id, template, status, schedule, created_at, started_at, next_run_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(request.cluster_id.as_uuid())
        .bind(inspection_template_name(request.template))
        .bind(inspection_status_name(status))
        .bind(request.schedule.as_deref())
        .bind(now)
        .bind(Option::<DateTime<Utc>>::None)
        .bind(next_run_at)
        .execute(&mut *transaction)
        .await?;
        append_workflow_event(
            &mut transaction,
            auth,
            request.cluster_id,
            "inspection",
            id.as_uuid(),
            "inspection_created",
            json!({"template": request.template, "status": status}),
            correlation_id,
            now,
        )
        .await?;
        transaction.commit().await?;
        Ok(InspectionView {
            run: InspectionRun {
                id,
                tenant_id: auth.tenant_id,
                cluster_id: request.cluster_id,
                template: request.template,
                status,
                schedule: request.schedule.clone(),
                finding_count: 0,
                partial: false,
                started_at: None,
                completed_at: None,
                created_at: now,
            },
            recommendations: Vec::new(),
            pack_diffs: Vec::new(),
        })
    }

    pub(crate) async fn inspection(
        &self,
        auth: &AuthContext,
        id: InspectionRunId,
    ) -> Result<InspectionView, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, template, status, schedule, finding_count,
                    partial, started_at, completed_at, created_at
             FROM inspection_runs
             WHERE id = $1 AND tenant_id = $2",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let run = inspection_from_row(&row)?;
        enforce_auth_cluster(auth, run.cluster_id)?;
        let rows = sqlx::query(
            "SELECT id, inspection_run_id, tenant_id, cluster_id, severity, title, rationale,
                    evidence_ids, status, assignee, investigation_id, incident_id, created_at, updated_at
             FROM recommendations
             WHERE inspection_run_id = $1 AND tenant_id = $2
             ORDER BY created_at, id",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        let recommendations = rows.iter().map(recommendation_from_row).collect::<Result<_, _>>()?;
        let pack_diffs = sqlx::query(
            "SELECT jsonb_build_object(
                        'pack_id', pack_id,
                        'pack_version', pack_version,
                        'diff', output -> 'diff'
                    ) AS pack_diff
             FROM diagnostic_pack_runs
             WHERE inspection_run_id = $1
               AND tenant_id = $2
               AND output ? 'diff'
             ORDER BY completed_at, id",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_all(&self.pool)
        .await?
        .iter()
        .map(|row| row.try_get("pack_diff"))
        .collect::<Result<Vec<Value>, _>>()?;
        Ok(InspectionView {
            run,
            recommendations,
            pack_diffs,
        })
    }

    pub(crate) async fn list_inspections(
        &self,
        auth: &AuthContext,
        query: &WorkflowListQuery,
    ) -> Result<WorkflowPage<InspectionView>, ControlPlaneError> {
        enforce_auth_cluster(auth, query.cluster_id)?;
        let limit = query.bounded_limit()?;
        let cursor = query.cursor_uuid()?;
        let rows = sqlx::query(
            "SELECT i.id, i.tenant_id, i.cluster_id, i.template, i.status, i.schedule, i.finding_count,
                    i.partial, i.started_at, i.completed_at, i.created_at
             FROM inspection_runs i
             WHERE i.tenant_id = $1 AND i.cluster_id = $2
               AND (
                    $3::UUID IS NULL
                    OR (i.created_at, i.id) < (
                        SELECT cursor.created_at, cursor.id
                        FROM inspection_runs cursor
                        WHERE cursor.id = $3 AND cursor.tenant_id = $1 AND cursor.cluster_id = $2
                    )
               )
             ORDER BY i.created_at DESC, i.id DESC
             LIMIT $4",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(query.cluster_id.as_uuid())
        .bind(cursor)
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await?;
        let items = rows.iter().map(inspection_from_row).collect::<Result<Vec<_>, _>>()?;
        Ok(
            WorkflowPage::from_window(items, limit, |item| item.id.as_uuid()).map(|run| InspectionView {
                run,
                recommendations: Vec::new(),
                pack_diffs: Vec::new(),
            }),
        )
    }

    pub(crate) async fn claim_inspection(
        &self,
        auth: &AuthContext,
        id: InspectionRunId,
    ) -> Result<InspectionRun, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, template, status, schedule, finding_count,
                    partial, started_at, completed_at, created_at
             FROM inspection_runs
             WHERE id = $1 AND tenant_id = $2
             FOR UPDATE",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let mut run = inspection_from_row(&row)?;
        enforce_auth_cluster(auth, run.cluster_id)?;
        match run.status {
            InspectionStatus::Scheduled => {}
            InspectionStatus::NeedsEvidence if run.schedule.is_none() => {}
            InspectionStatus::NeedsEvidence => {
                return Err(ControlPlaneError::conflict(
                    "a recurring inspection already has a scheduled successor",
                ));
            }
            InspectionStatus::Running => {
                return Err(ControlPlaneError::conflict("inspection is already running"));
            }
            InspectionStatus::Completed | InspectionStatus::Failed | InspectionStatus::Cancelled => {
                return Err(ControlPlaneError::conflict(
                    "completed, failed, or cancelled inspections cannot be run again",
                ));
            }
        }
        let now = Utc::now();
        sqlx::query(
            "UPDATE inspection_runs
             SET status = 'running', started_at = $2, completed_at = NULL
             WHERE id = $1 AND status IN ('scheduled', 'needs_evidence')",
        )
        .bind(id.as_uuid())
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        run.status = InspectionStatus::Running;
        run.started_at = Some(now);
        run.completed_at = None;
        transaction.commit().await?;
        Ok(run)
    }

    pub(crate) async fn due_inspections(&self, limit: u32) -> Result<Vec<DueInspection>, ControlPlaneError> {
        if !(1..=64).contains(&limit) {
            return Err(ControlPlaneError::validation(
                "invalid_request",
                "scheduled inspection batch must contain between 1 and 64 runs",
            ));
        }
        let rows = sqlx::query(
            "SELECT id, tenant_id, cluster_id
             FROM inspection_runs
             WHERE status = 'scheduled' AND next_run_at <= NOW()
             ORDER BY next_run_at, id
             LIMIT $1",
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(DueInspection {
                    id: InspectionRunId::from_uuid(row.try_get("id")?),
                    tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
                    cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
                })
            })
            .collect()
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "inspection completion records bounded pack and recommendation artifacts atomically"
    )]
    pub(crate) async fn complete_inspection(
        &self,
        auth: &AuthContext,
        id: InspectionRunId,
        mut pack_runs: Vec<InspectionPackRun>,
        recommendations: Vec<NewRecommendation>,
        partial: bool,
        correlation_id: CorrelationId,
    ) -> Result<InspectionView, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, template, status, schedule, finding_count,
                    partial, started_at, completed_at, created_at
             FROM inspection_runs
             WHERE id = $1 AND tenant_id = $2
             FOR UPDATE",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let run = inspection_from_row(&row)?;
        enforce_auth_cluster(auth, run.cluster_id)?;
        if run.status != InspectionStatus::Running {
            return Err(ControlPlaneError::conflict(
                "only a running inspection can be completed",
            ));
        }
        let now = Utc::now();
        for pack_run in &mut pack_runs {
            attach_previous_pack_diff(&mut transaction, auth.tenant_id, run.cluster_id, id, pack_run).await?;
            let evidence_ids = pack_run
                .input_evidence_ids
                .iter()
                .map(|value| value.as_uuid())
                .collect::<Vec<_>>();
            sqlx::query(
                "INSERT INTO diagnostic_pack_runs (
                    id, tenant_id, cluster_id, incident_id, inspection_run_id,
                    pack_id, pack_version, input_evidence_ids, output, partial,
                    started_at, completed_at
                 ) VALUES ($1, $2, $3, NULL, $4, $5, $6, $7, $8, $9, $10, $11)",
            )
            .bind(Uuid::new_v4())
            .bind(auth.tenant_id.as_uuid())
            .bind(run.cluster_id.as_uuid())
            .bind(id.as_uuid())
            .bind(&pack_run.pack_id)
            .bind(&pack_run.pack_version)
            .bind(evidence_ids)
            .bind(&pack_run.output)
            .bind(pack_run.partial)
            .bind(pack_run.started_at)
            .bind(pack_run.completed_at)
            .execute(&mut *transaction)
            .await?;
        }
        for recommendation in &recommendations {
            let evidence_ids = recommendation
                .evidence_ids
                .iter()
                .map(|value| value.as_uuid())
                .collect::<Vec<_>>();
            sqlx::query(
                "INSERT INTO recommendations (
                    id, inspection_run_id, tenant_id, cluster_id, severity, title,
                    rationale, evidence_ids, status, created_at, updated_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'open', $9, $9)",
            )
            .bind(RecommendationId::new().as_uuid())
            .bind(id.as_uuid())
            .bind(auth.tenant_id.as_uuid())
            .bind(run.cluster_id.as_uuid())
            .bind(&recommendation.severity)
            .bind(&recommendation.title)
            .bind(&recommendation.rationale)
            .bind(evidence_ids)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        }
        let status = if partial {
            InspectionStatus::NeedsEvidence
        } else {
            InspectionStatus::Completed
        };
        let finding_count = i32::try_from(recommendations.len()).map_err(|_| {
            ControlPlaneError::validation(
                "output_too_large",
                "inspection recommendation count exceeds the supported range",
            )
        })?;
        sqlx::query(
            "UPDATE inspection_runs
             SET status = $2, finding_count = $3, partial = $4, completed_at = $5
             WHERE id = $1",
        )
        .bind(id.as_uuid())
        .bind(inspection_status_name(status))
        .bind(finding_count)
        .bind(partial)
        .bind(now)
        .execute(&mut *transaction)
        .await?;

        if let Some(schedule) = run.schedule.as_deref() {
            let interval = super::schedule_interval_from_expression(schedule)?;
            let interval = chrono::Duration::from_std(interval).map_err(|_| {
                ControlPlaneError::validation(
                    "invalid_schedule",
                    "inspection interval cannot be represented by the scheduler",
                )
            })?;
            let next_id = InspectionRunId::new();
            sqlx::query(
                "INSERT INTO inspection_runs (
                    id, tenant_id, cluster_id, template, status, schedule,
                    created_at, next_run_at
                 ) VALUES ($1, $2, $3, $4, 'scheduled', $5, $6, $7)",
            )
            .bind(next_id.as_uuid())
            .bind(auth.tenant_id.as_uuid())
            .bind(run.cluster_id.as_uuid())
            .bind(inspection_template_name(run.template))
            .bind(schedule)
            .bind(now)
            .bind(now + interval)
            .execute(&mut *transaction)
            .await?;
        }
        append_workflow_event(
            &mut transaction,
            auth,
            run.cluster_id,
            "inspection",
            id.as_uuid(),
            "inspection_completed",
            json!({
                "status": status,
                "partial": partial,
                "pack_count": pack_runs.len(),
                "recommendation_count": recommendations.len(),
                "execution_eligible": false,
            }),
            correlation_id,
            now,
        )
        .await?;
        transaction.commit().await?;
        self.inspection(auth, id).await
    }

    pub(crate) async fn disposition_recommendation(
        &self,
        auth: &AuthContext,
        id: RecommendationId,
        request: &RecommendationDispositionRequest,
        correlation_id: CorrelationId,
    ) -> Result<Recommendation, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT id, inspection_run_id, tenant_id, cluster_id, severity, title, rationale,
                    evidence_ids, status, assignee, investigation_id, incident_id, created_at, updated_at
             FROM recommendations
             WHERE id = $1 AND tenant_id = $2
             FOR UPDATE",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let current = recommendation_from_row(&row)?;
        enforce_auth_cluster(auth, current.cluster_id)?;
        if matches!(
            current.status,
            RecommendationStatus::Dismissed | RecommendationStatus::Resolved | RecommendationStatus::Promoted
        ) {
            return Err(ControlPlaneError::conflict(
                "terminal recommendation disposition cannot be changed",
            ));
        }
        let now = Utc::now();
        let (investigation_id, incident_id) = if request.status == RecommendationStatus::Promoted {
            let investigation = insert_investigation(
                &mut transaction,
                auth,
                current.cluster_id,
                None,
                current.title.clone(),
                None,
                "inspection_recommendation",
                now,
                correlation_id,
            )
            .await?;
            let incident_id = if request
                .promote_to
                .unwrap_or(RecommendationPromotionTarget::Investigation)
                == RecommendationPromotionTarget::Incident
            {
                let incident_id = IncidentId::new();
                insert_incident(
                    &mut transaction,
                    auth,
                    incident_id,
                    Some(investigation.id),
                    current.cluster_id,
                    &current.title,
                    None,
                    &investigation.symptom_family,
                    &investigation.fingerprint,
                    now,
                )
                .await?;
                sqlx::query(
                    "UPDATE investigations
                     SET incident_id = $2, status = 'promoted', updated_at = $3
                     WHERE id = $1",
                )
                .bind(investigation.id.as_uuid())
                .bind(incident_id.as_uuid())
                .bind(now)
                .execute(&mut *transaction)
                .await?;
                append_timeline(
                    &mut transaction,
                    auth,
                    current.cluster_id,
                    Some(investigation.id),
                    Some(incident_id),
                    "recommendation_promoted",
                    "Inspection recommendation promoted to incident",
                    json!({"recommendation_id": id, "reason": request.reason}),
                    correlation_id,
                    now,
                )
                .await?;
                append_workflow_event(
                    &mut transaction,
                    auth,
                    current.cluster_id,
                    "incident",
                    incident_id.as_uuid(),
                    "incident_created",
                    json!({"recommendation_id": id, "investigation_id": investigation.id}),
                    correlation_id,
                    now,
                )
                .await?;
                Some(incident_id)
            } else {
                None
            };
            (Some(investigation.id), incident_id)
        } else {
            (current.investigation_id, current.incident_id)
        };
        sqlx::query(
            "UPDATE recommendations
             SET status = $2, assignee = $3, updated_at = $4,
                 investigation_id = $5, incident_id = $6
             WHERE id = $1",
        )
        .bind(id.as_uuid())
        .bind(recommendation_status_name(request.status))
        .bind(request.assignee.as_deref())
        .bind(now)
        .bind(investigation_id.map(InvestigationId::as_uuid))
        .bind(incident_id.map(IncidentId::as_uuid))
        .execute(&mut *transaction)
        .await?;
        append_workflow_event(
            &mut transaction,
            auth,
            current.cluster_id,
            "recommendation",
            id.as_uuid(),
            "recommendation_disposition_changed",
            json!({"status": request.status, "reason": request.reason}),
            correlation_id,
            now,
        )
        .await?;
        transaction.commit().await?;
        Ok(Recommendation {
            status: request.status,
            assignee: request.assignee.clone(),
            investigation_id,
            incident_id,
            updated_at: now,
            ..current
        })
    }

    pub(crate) async fn list_recommendations(
        &self,
        auth: &AuthContext,
        query: &WorkflowListQuery,
    ) -> Result<WorkflowPage<Recommendation>, ControlPlaneError> {
        enforce_auth_cluster(auth, query.cluster_id)?;
        let limit = query.bounded_limit()?;
        let cursor = query.cursor_uuid()?;
        let rows = sqlx::query(
            "SELECT r.id, r.inspection_run_id, r.tenant_id, r.cluster_id, r.severity, r.title, r.rationale,
                    r.evidence_ids, r.status, r.assignee, r.investigation_id, r.incident_id,
                    r.created_at, r.updated_at
             FROM recommendations r
             WHERE r.tenant_id = $1 AND r.cluster_id = $2
               AND (
                    $3::UUID IS NULL
                    OR (r.created_at, r.id) < (
                        SELECT cursor.created_at, cursor.id
                        FROM recommendations cursor
                        WHERE cursor.id = $3 AND cursor.tenant_id = $1 AND cursor.cluster_id = $2
                    )
               )
             ORDER BY r.created_at DESC, r.id DESC
             LIMIT $4",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(query.cluster_id.as_uuid())
        .bind(cursor)
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await?;
        let items = rows
            .iter()
            .map(recommendation_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(WorkflowPage::from_window(items, limit, |item| item.id.as_uuid()))
    }

    async fn timeline(
        &self,
        auth: &AuthContext,
        investigation_id: Option<InvestigationId>,
        incident_id: Option<IncidentId>,
    ) -> Result<Vec<TimelineEvent>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT event_id, tenant_id, cluster_id, investigation_id, incident_id,
                    event_type, summary, details, correlation_id, actor_subject,
                    actor_display_name, occurred_at
             FROM incident_timeline
             WHERE tenant_id = $1
               AND (($2::UUID IS NOT NULL AND investigation_id = $2)
                    OR ($3::UUID IS NOT NULL AND incident_id = $3))
             ORDER BY sequence_id",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(investigation_id.map(InvestigationId::as_uuid))
        .bind(incident_id.map(IncidentId::as_uuid))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(timeline_from_row).collect()
    }

    async fn diagnosis_revisions(
        &self,
        auth: &AuthContext,
        incident_id: IncidentId,
    ) -> Result<Vec<DiagnosisRevision>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT d.id, d.incident_id, d.revision, d.status, d.rule_result, d.hypotheses,
                    d.evidence_ids, d.primary_model_invocation_id, d.execution_eligible,
                    d.partial, d.created_at
             FROM diagnosis_revisions d
             JOIN sre_incidents i ON i.id = d.incident_id
             WHERE d.incident_id = $1 AND i.tenant_id = $2
             ORDER BY d.revision",
        )
        .bind(incident_id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(diagnosis_revision_from_row).collect()
    }
}

async fn attach_previous_pack_diff(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    inspection_run_id: InspectionRunId,
    current: &mut InspectionPackRun,
) -> Result<(), ControlPlaneError> {
    let previous = sqlx::query(
        "SELECT inspection_run_id, output
         FROM diagnostic_pack_runs
         WHERE tenant_id = $1
           AND cluster_id = $2
           AND pack_id = $3
           AND inspection_run_id IS NOT NULL
           AND inspection_run_id <> $4
         ORDER BY completed_at DESC, id DESC
         LIMIT 1",
    )
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(&current.pack_id)
    .bind(inspection_run_id.as_uuid())
    .fetch_optional(&mut **transaction)
    .await?;
    let current_codes = finding_reason_codes(&current.output);
    let (previous_run_id, previous_codes) = previous
        .map(|row| {
            let run_id = row.try_get::<Uuid, _>("inspection_run_id")?;
            let output = row.try_get::<Value, _>("output")?;
            Ok::<_, sqlx::Error>((Some(run_id), finding_reason_codes(&output)))
        })
        .transpose()?
        .unwrap_or_else(|| (None, BTreeSet::new()));
    let added = current_codes.difference(&previous_codes).cloned().collect::<Vec<_>>();
    let resolved = previous_codes.difference(&current_codes).cloned().collect::<Vec<_>>();
    let unchanged = current_codes.intersection(&previous_codes).cloned().collect::<Vec<_>>();
    let output = current.output.as_object_mut().ok_or_else(|| {
        ControlPlaneError::validation(
            "diagnostic_evaluation_failed",
            "inspection pack output must be a JSON object",
        )
    })?;
    output.insert(
        "diff".to_owned(),
        json!({
            "schema_version": "rocketmq-sre.inspection-pack-diff.v1",
            "previous_inspection_run_id": previous_run_id,
            "added_reason_codes": added,
            "resolved_reason_codes": resolved,
            "unchanged_reason_codes": unchanged,
        }),
    );
    Ok(())
}

fn finding_reason_codes(output: &Value) -> BTreeSet<String> {
    output
        .get("findings")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|finding| finding.get("reason_code").and_then(Value::as_str))
        .filter(|code| !code.is_empty() && code.len() <= 128)
        .map(str::to_owned)
        .collect()
}

pub(super) async fn ensure_cluster_scope(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    cluster_id: ClusterId,
) -> Result<(), ControlPlaneError> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM clusters
            WHERE id = $1 AND tenant_id = $2 AND onboarding_state <> 'offboarded'
        )",
    )
    .bind(cluster_id.as_uuid())
    .bind(tenant_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if !exists {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "cluster is outside the authenticated tenant or is offboarded",
        ));
    }
    Ok(())
}

fn enforce_auth_cluster(auth: &AuthContext, cluster_id: ClusterId) -> Result<(), ControlPlaneError> {
    if !auth.clusters.contains(&cluster_id) {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "requested cluster is outside the authenticated scope",
        ));
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the persisted aggregate fields are explicit at the SQL boundary"
)]
pub(super) async fn insert_investigation(
    transaction: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    cluster_id: ClusterId,
    conversation_id: Option<ConversationId>,
    title: String,
    resource: Option<&str>,
    symptom_family: &str,
    now: DateTime<Utc>,
    correlation_id: CorrelationId,
) -> Result<Investigation, ControlPlaneError> {
    let id = InvestigationId::new();
    let fingerprint = fingerprint(auth.tenant_id, cluster_id, resource, symptom_family, now);
    sqlx::query(
        "INSERT INTO investigations (
            id, tenant_id, cluster_id, conversation_id, title, resource, symptom_family,
            fingerprint, status, created_by_subject, created_at, updated_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'open', $9, $10, $10)",
    )
    .bind(id.as_uuid())
    .bind(auth.tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(conversation_id.map(ConversationId::as_uuid))
    .bind(&title)
    .bind(resource)
    .bind(symptom_family)
    .bind(&fingerprint)
    .bind(&auth.subject)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    append_timeline(
        transaction,
        auth,
        cluster_id,
        Some(id),
        None,
        "investigation_created",
        "Investigation created",
        json!({"symptom_family": symptom_family}),
        correlation_id,
        now,
    )
    .await?;
    append_workflow_event(
        transaction,
        auth,
        cluster_id,
        "investigation",
        id.as_uuid(),
        "investigation_created",
        json!({"symptom_family": symptom_family}),
        correlation_id,
        now,
    )
    .await?;
    Ok(Investigation {
        id,
        tenant_id: auth.tenant_id,
        cluster_id,
        conversation_id,
        incident_id: None,
        title,
        resource: resource.map(ToOwned::to_owned),
        symptom_family: symptom_family.to_owned(),
        fingerprint,
        status: InvestigationStatus::Open,
        created_by: actor(auth),
        created_at: now,
        updated_at: now,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the persisted aggregate fields are explicit at the SQL boundary"
)]
pub(super) async fn insert_incident(
    transaction: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    id: IncidentId,
    investigation_id: Option<InvestigationId>,
    cluster_id: ClusterId,
    title: &str,
    resource: Option<&str>,
    symptom_family: &str,
    fingerprint: &str,
    now: DateTime<Utc>,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO sre_incidents (
            id, tenant_id, cluster_id, investigation_id, title, resource, symptom_family,
            fingerprint, status, created_by_subject, created_at, updated_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'new', $9, $10, $10)",
    )
    .bind(id.as_uuid())
    .bind(auth.tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(investigation_id.map(InvestigationId::as_uuid))
    .bind(title)
    .bind(resource)
    .bind(symptom_family)
    .bind(fingerprint)
    .bind(&auth.subject)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "timeline persistence intentionally records the full audit tuple"
)]
pub(super) async fn append_timeline(
    transaction: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    cluster_id: ClusterId,
    investigation_id: Option<InvestigationId>,
    incident_id: Option<IncidentId>,
    event_type: &str,
    summary: &str,
    details: Value,
    correlation_id: CorrelationId,
    occurred_at: DateTime<Utc>,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO incident_timeline (
            event_id, tenant_id, cluster_id, investigation_id, incident_id,
            event_type, summary, details, correlation_id, actor_subject, occurred_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
    )
    .bind(TimelineEventId::new().as_uuid())
    .bind(auth.tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(investigation_id.map(InvestigationId::as_uuid))
    .bind(incident_id.map(IncidentId::as_uuid))
    .bind(event_type)
    .bind(summary)
    .bind(details)
    .bind(correlation_id.as_uuid())
    .bind(&auth.subject)
    .bind(occurred_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "event persistence intentionally records the full audit tuple"
)]
pub(super) async fn append_workflow_event(
    transaction: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    cluster_id: ClusterId,
    aggregate_type: &str,
    aggregate_id: Uuid,
    event_type: &str,
    payload: Value,
    correlation_id: CorrelationId,
    occurred_at: DateTime<Utc>,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO workflow_events (
            event_id, tenant_id, cluster_id, aggregate_type, aggregate_id,
            event_type, event_payload, correlation_id, occurred_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(Uuid::new_v4())
    .bind(auth.tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(aggregate_type)
    .bind(aggregate_id)
    .bind(event_type)
    .bind(payload)
    .bind(correlation_id.as_uuid())
    .bind(occurred_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn conversation_from_row(row: &PgRow) -> Result<Conversation, ControlPlaneError> {
    Ok(Conversation {
        id: ConversationId::from_uuid(row.try_get("id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        question: row.try_get("question")?,
        resource: row.try_get("resource")?,
        status: parse_conversation_status(row.try_get("status")?)?,
        investigation_id: row
            .try_get::<Option<Uuid>, _>("investigation_id")?
            .map(InvestigationId::from_uuid),
        created_by: WorkflowActor {
            subject: row.try_get("created_by_subject")?,
            display_name: row.try_get("created_by_display_name")?,
        },
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn investigation_from_row(row: &PgRow) -> Result<Investigation, ControlPlaneError> {
    Ok(Investigation {
        id: InvestigationId::from_uuid(row.try_get("id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        conversation_id: row
            .try_get::<Option<Uuid>, _>("conversation_id")?
            .map(ConversationId::from_uuid),
        incident_id: row
            .try_get::<Option<Uuid>, _>("incident_id")?
            .map(IncidentId::from_uuid),
        title: row.try_get("title")?,
        resource: row.try_get("resource")?,
        symptom_family: row.try_get("symptom_family")?,
        fingerprint: row.try_get("fingerprint")?,
        status: parse_investigation_status(row.try_get("status")?)?,
        created_by: WorkflowActor {
            subject: row.try_get("created_by_subject")?,
            display_name: row.try_get("created_by_display_name")?,
        },
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn incident_from_row(row: &PgRow) -> Result<Incident, ControlPlaneError> {
    let occurrence_count: i32 = row.try_get("occurrence_count")?;
    Ok(Incident {
        id: IncidentId::from_uuid(row.try_get("id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        title: row.try_get("title")?,
        resource: row.try_get("resource")?,
        symptom_family: row.try_get("symptom_family")?,
        fingerprint: row.try_get("fingerprint")?,
        severity: row
            .try_get::<Option<String>, _>("severity")?
            .map(|value| {
                serde_json::from_value(serde_json::Value::String(value))
                    .map_err(|_| ControlPlaneError::configuration("stored incident severity is invalid"))
            })
            .transpose()?,
        owner: row.try_get("owner_name")?,
        occurrence_count: u32::try_from(occurrence_count)
            .map_err(|_| ControlPlaneError::configuration("stored incident occurrence count is negative"))?,
        last_alert_at: row.try_get("last_alert_at")?,
        reopened_from_incident_id: row
            .try_get::<Option<Uuid>, _>("reopened_from_incident_id")?
            .map(IncidentId::from_uuid),
        status: parse_incident_status(row.try_get("status")?)?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        hypotheses: Vec::new(),
    })
}

fn timeline_from_row(row: &PgRow) -> Result<TimelineEvent, ControlPlaneError> {
    Ok(TimelineEvent {
        id: TimelineEventId::from_uuid(row.try_get("event_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        investigation_id: row
            .try_get::<Option<Uuid>, _>("investigation_id")?
            .map(InvestigationId::from_uuid),
        incident_id: row
            .try_get::<Option<Uuid>, _>("incident_id")?
            .map(IncidentId::from_uuid),
        event_type: row.try_get("event_type")?,
        summary: row.try_get("summary")?,
        details: row.try_get("details")?,
        correlation_id: CorrelationId::from_uuid(row.try_get("correlation_id")?),
        actor: WorkflowActor {
            subject: row.try_get("actor_subject")?,
            display_name: row.try_get("actor_display_name")?,
        },
        occurred_at: row.try_get("occurred_at")?,
    })
}

fn inspection_from_row(row: &PgRow) -> Result<InspectionRun, ControlPlaneError> {
    Ok(InspectionRun {
        id: InspectionRunId::from_uuid(row.try_get("id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        template: parse_inspection_template(row.try_get("template")?)?,
        status: parse_inspection_status(row.try_get("status")?)?,
        schedule: row.try_get("schedule")?,
        finding_count: u32::try_from(row.try_get::<i32, _>("finding_count")?).map_err(|_| {
            ControlPlaneError::validation(
                "source_unavailable",
                "inspection finding count is outside the supported range",
            )
        })?,
        partial: row.try_get("partial")?,
        started_at: row.try_get("started_at")?,
        completed_at: row.try_get("completed_at")?,
        created_at: row.try_get("created_at")?,
    })
}

fn recommendation_from_row(row: &PgRow) -> Result<Recommendation, ControlPlaneError> {
    Ok(Recommendation {
        id: RecommendationId::from_uuid(row.try_get("id")?),
        inspection_run_id: InspectionRunId::from_uuid(row.try_get("inspection_run_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        severity: row.try_get("severity")?,
        title: row.try_get("title")?,
        rationale: row.try_get("rationale")?,
        evidence_ids: row
            .try_get::<Vec<Uuid>, _>("evidence_ids")?
            .into_iter()
            .map(rocketmq_sre_contracts::EvidenceId::from_uuid)
            .collect(),
        status: parse_recommendation_status(row.try_get("status")?)?,
        assignee: row.try_get("assignee")?,
        investigation_id: row
            .try_get::<Option<Uuid>, _>("investigation_id")?
            .map(InvestigationId::from_uuid),
        incident_id: row
            .try_get::<Option<Uuid>, _>("incident_id")?
            .map(IncidentId::from_uuid),
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn diagnosis_revision_from_row(row: &PgRow) -> Result<DiagnosisRevision, ControlPlaneError> {
    Ok(DiagnosisRevision {
        id: rocketmq_sre_contracts::DiagnosisRevisionId::from_uuid(row.try_get("id")?),
        incident_id: IncidentId::from_uuid(row.try_get("incident_id")?),
        revision: u32::try_from(row.try_get::<i32, _>("revision")?)
            .map_err(|_| ControlPlaneError::validation("source_unavailable", "diagnosis revision is invalid"))?,
        status: parse_diagnosis_revision_status(row.try_get("status")?)?,
        rule_result: row.try_get("rule_result")?,
        hypotheses: row.try_get("hypotheses")?,
        evidence_ids: row
            .try_get::<Vec<Uuid>, _>("evidence_ids")?
            .into_iter()
            .map(rocketmq_sre_contracts::EvidenceId::from_uuid)
            .collect(),
        primary_model_invocation_id: row
            .try_get::<Option<Uuid>, _>("primary_model_invocation_id")?
            .map(rocketmq_sre_contracts::ModelInvocationId::from_uuid),
        execution_eligible: row.try_get("execution_eligible")?,
        partial: row.try_get("partial")?,
        created_at: row.try_get("created_at")?,
    })
}

fn parse_conversation_status(value: &str) -> Result<ConversationStatus, ControlPlaneError> {
    match value {
        "active" => Ok(ConversationStatus::Active),
        "promoted" => Ok(ConversationStatus::Promoted),
        "closed" => Ok(ConversationStatus::Closed),
        _ => Err(invalid_database_enum("conversation status")),
    }
}

fn parse_investigation_status(value: &str) -> Result<InvestigationStatus, ControlPlaneError> {
    match value {
        "open" => Ok(InvestigationStatus::Open),
        "collecting" => Ok(InvestigationStatus::Collecting),
        "diagnosing" => Ok(InvestigationStatus::Diagnosing),
        "needs_evidence" => Ok(InvestigationStatus::NeedsEvidence),
        "monitoring" => Ok(InvestigationStatus::Monitoring),
        "promoted" => Ok(InvestigationStatus::Promoted),
        "closed" => Ok(InvestigationStatus::Closed),
        _ => Err(invalid_database_enum("investigation status")),
    }
}

fn parse_incident_status(value: &str) -> Result<IncidentStatus, ControlPlaneError> {
    match value {
        "new" => Ok(IncidentStatus::New),
        "collecting" => Ok(IncidentStatus::Collecting),
        "diagnosing" => Ok(IncidentStatus::Diagnosing),
        "needs_evidence" => Ok(IncidentStatus::NeedsEvidence),
        "monitoring" => Ok(IncidentStatus::Monitoring),
        "resolved" => Ok(IncidentStatus::Resolved),
        "escalated" => Ok(IncidentStatus::Escalated),
        _ => Err(invalid_database_enum("incident status")),
    }
}

fn parse_diagnosis_revision_status(value: &str) -> Result<IncidentStatus, ControlPlaneError> {
    if value == "confirmed" {
        // Confirmation is carried by `execution_eligible`; the public
        // diagnosis contract still exposes the owning Incident lifecycle.
        Ok(IncidentStatus::Monitoring)
    } else {
        parse_incident_status(value)
    }
}

const fn incident_status_name(value: IncidentStatus) -> &'static str {
    match value {
        IncidentStatus::New => "new",
        IncidentStatus::Collecting => "collecting",
        IncidentStatus::Diagnosing => "diagnosing",
        IncidentStatus::NeedsEvidence => "needs_evidence",
        IncidentStatus::Monitoring => "monitoring",
        IncidentStatus::Resolved => "resolved",
        IncidentStatus::Escalated => "escalated",
    }
}

pub(super) fn inspection_template_name(value: InspectionTemplate) -> &'static str {
    match value {
        InspectionTemplate::ClusterHealth => "cluster_health",
        InspectionTemplate::Consumer => "consumer",
        InspectionTemplate::Broker => "broker",
        InspectionTemplate::Telemetry => "telemetry",
        InspectionTemplate::FullCluster => "full_cluster",
        InspectionTemplate::ProducerConsumer => "producer_consumer",
        InspectionTemplate::StoreHa => "store_ha",
        InspectionTemplate::RoutingProxy => "routing_proxy",
        InspectionTemplate::Security => "security",
        InspectionTemplate::Upgrade => "upgrade",
        InspectionTemplate::DisasterRecovery => "disaster_recovery",
    }
}

fn parse_inspection_template(value: &str) -> Result<InspectionTemplate, ControlPlaneError> {
    match value {
        "cluster_health" => Ok(InspectionTemplate::ClusterHealth),
        "consumer" => Ok(InspectionTemplate::Consumer),
        "broker" => Ok(InspectionTemplate::Broker),
        "telemetry" => Ok(InspectionTemplate::Telemetry),
        "full_cluster" => Ok(InspectionTemplate::FullCluster),
        "producer_consumer" => Ok(InspectionTemplate::ProducerConsumer),
        "store_ha" => Ok(InspectionTemplate::StoreHa),
        "routing_proxy" => Ok(InspectionTemplate::RoutingProxy),
        "security" => Ok(InspectionTemplate::Security),
        "upgrade" => Ok(InspectionTemplate::Upgrade),
        "disaster_recovery" => Ok(InspectionTemplate::DisasterRecovery),
        _ => Err(invalid_database_enum("inspection template")),
    }
}

pub(super) fn inspection_status_name(value: InspectionStatus) -> &'static str {
    match value {
        InspectionStatus::Scheduled => "scheduled",
        InspectionStatus::Running => "running",
        InspectionStatus::NeedsEvidence => "needs_evidence",
        InspectionStatus::Completed => "completed",
        InspectionStatus::Failed => "failed",
        InspectionStatus::Cancelled => "cancelled",
    }
}

fn parse_inspection_status(value: &str) -> Result<InspectionStatus, ControlPlaneError> {
    match value {
        "scheduled" => Ok(InspectionStatus::Scheduled),
        "running" => Ok(InspectionStatus::Running),
        "needs_evidence" => Ok(InspectionStatus::NeedsEvidence),
        "completed" => Ok(InspectionStatus::Completed),
        "failed" => Ok(InspectionStatus::Failed),
        "cancelled" => Ok(InspectionStatus::Cancelled),
        _ => Err(invalid_database_enum("inspection status")),
    }
}

fn recommendation_status_name(value: RecommendationStatus) -> &'static str {
    match value {
        RecommendationStatus::Open => "open",
        RecommendationStatus::Acknowledged => "acknowledged",
        RecommendationStatus::Assigned => "assigned",
        RecommendationStatus::Dismissed => "dismissed",
        RecommendationStatus::Resolved => "resolved",
        RecommendationStatus::Promoted => "promoted",
    }
}

fn parse_recommendation_status(value: &str) -> Result<RecommendationStatus, ControlPlaneError> {
    match value {
        "open" => Ok(RecommendationStatus::Open),
        "acknowledged" => Ok(RecommendationStatus::Acknowledged),
        "assigned" => Ok(RecommendationStatus::Assigned),
        "dismissed" => Ok(RecommendationStatus::Dismissed),
        "resolved" => Ok(RecommendationStatus::Resolved),
        "promoted" => Ok(RecommendationStatus::Promoted),
        _ => Err(invalid_database_enum("recommendation status")),
    }
}

fn invalid_database_enum(name: &str) -> ControlPlaneError {
    ControlPlaneError::validation("source_unavailable", format!("stored {name} is not supported"))
}

fn actor(auth: &AuthContext) -> WorkflowActor {
    WorkflowActor {
        subject: auth.subject.clone(),
        display_name: None,
    }
}

fn infer_intent(question: &str, resource: Option<&str>) -> String {
    let lower = question.to_ascii_lowercase();
    if lower.contains("lag") || question.contains("堆积") || question.contains("消费") {
        "consumer_lag".to_owned()
    } else if lower.contains("producer") || lower.contains("send") || question.contains("发送") {
        "producer_connectivity".to_owned()
    } else if lower.contains("broker") || question.contains("存储") {
        "broker_health".to_owned()
    } else if lower.contains("telemetry") || lower.contains("metric") || question.contains("遥测") {
        "telemetry_pipeline".to_owned()
    } else if resource.is_some() {
        "resource_health".to_owned()
    } else {
        "cluster_health".to_owned()
    }
}

fn bounded_title(question: &str) -> String {
    question.trim().chars().take(512).collect()
}

pub(super) fn fingerprint(
    tenant_id: TenantId,
    cluster_id: ClusterId,
    resource: Option<&str>,
    symptom_family: &str,
    at: DateTime<Utc>,
) -> String {
    let bounded_window = at.timestamp().div_euclid(900);
    let canonical = format!(
        "{}\n{}\n{}\n{}\n{}",
        tenant_id,
        cluster_id,
        resource.unwrap_or("-"),
        symptom_family,
        bounded_window
    );
    format!("sha256:{:x}", Sha256::digest(canonical.as_bytes()))
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn inspection_diff_uses_only_bounded_reason_codes() {
        let codes = finding_reason_codes(&json!({
            "findings": [
                {"reason_code": "BROKER_DOWN"},
                {"reason_code": "BROKER_DOWN"},
                {"reason_code": ""},
                {"reason_code": "x".repeat(129)}
            ]
        }));

        assert_eq!(codes, BTreeSet::from(["BROKER_DOWN".to_owned()]));
    }

    #[test]
    fn intent_parser_is_deterministic_and_read_only() {
        assert_eq!(
            infer_intent("why is consumer lag rising", Some("group/orders")),
            "consumer_lag"
        );
        assert_eq!(infer_intent("Broker 磁盘异常", None), "broker_health");
    }

    #[test]
    fn fingerprint_is_stable_within_bounded_window() {
        let tenant = TenantId::new();
        let cluster = ClusterId::new();
        let first = Utc.with_ymd_and_hms(2026, 7, 27, 1, 1, 0).single().expect("time");
        let second = Utc.with_ymd_and_hms(2026, 7, 27, 1, 14, 59).single().expect("time");
        assert_eq!(
            fingerprint(tenant, cluster, Some("group/orders"), "consumer_lag", first),
            fingerprint(tenant, cluster, Some("group/orders"), "consumer_lag", second)
        );
    }

    #[test]
    fn confirmed_diagnosis_projects_to_monitoring_without_widening_incident_status() {
        assert_eq!(
            parse_diagnosis_revision_status("confirmed").expect("confirmed diagnosis"),
            IncidentStatus::Monitoring
        );
        assert!(parse_incident_status("confirmed").is_err());
    }
}
