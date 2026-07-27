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

use chrono::Utc;
use rocketmq_sre_contracts::ConversationId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::DiagnosisRevision;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::IncidentStatus;
use rocketmq_sre_contracts::InspectionRunId;
use rocketmq_sre_contracts::InvestigationId;
use rocketmq_sre_contracts::ModelInvocationId;
use rocketmq_sre_contracts::Recommendation;
use rocketmq_sre_contracts::RecommendationId;
use serde_json::Value;
use serde_json::json;
use tokio::sync::broadcast;

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
use super::WorkflowEventBus;
use super::WorkflowListQuery;
use super::WorkflowPage;
use super::WorkflowStreamEvent;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

/// Persistent, read-only product workflow facade.
#[derive(Clone)]
pub(crate) struct WorkflowService {
    repository: PostgresRepository,
    events: WorkflowEventBus,
}

impl WorkflowService {
    pub(crate) fn new(repository: PostgresRepository, events: WorkflowEventBus) -> Self {
        Self { repository, events }
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<WorkflowStreamEvent> {
        self.events.subscribe()
    }

    pub(crate) fn publish_external(&self, event: WorkflowStreamEvent) {
        self.events.publish(event);
    }

    pub(crate) async fn create_conversation(
        &self,
        auth: &AuthContext,
        request: &ConversationCreateRequest,
        correlation_id: CorrelationId,
    ) -> Result<ConversationView, ControlPlaneError> {
        request.validate()?;
        authorize_cluster(auth, request.cluster_id)?;
        let view = self
            .repository
            .create_conversation(auth, request, correlation_id)
            .await?;
        self.events.publish(WorkflowStreamEvent {
            tenant_id: auth.tenant_id,
            cluster_id: request.cluster_id,
            aggregate_type: "conversation",
            aggregate_id: view.conversation.id.to_string(),
            event_type: "conversation_created",
            payload: json!({
                "status": view.conversation.status,
                "investigation_id": view.investigation.as_ref().map(|value| value.id)
            }),
            correlation_id,
            occurred_at: Utc::now(),
        });
        Ok(view)
    }

    pub(crate) async fn conversation(
        &self,
        auth: &AuthContext,
        id: ConversationId,
    ) -> Result<ConversationView, ControlPlaneError> {
        self.repository.conversation(auth, id).await
    }

    pub(crate) async fn list_conversations(
        &self,
        auth: &AuthContext,
        query: &WorkflowListQuery,
    ) -> Result<WorkflowPage<ConversationView>, ControlPlaneError> {
        validate_list_scope(auth, query)?;
        self.repository.list_conversations(auth, query).await
    }

    pub(crate) async fn create_investigation(
        &self,
        auth: &AuthContext,
        request: &InvestigationCreateRequest,
        correlation_id: CorrelationId,
    ) -> Result<InvestigationView, ControlPlaneError> {
        request.validate()?;
        authorize_cluster(auth, request.cluster_id)?;
        let view = self
            .repository
            .create_investigation(auth, request, correlation_id)
            .await?;
        self.events.publish(WorkflowStreamEvent {
            tenant_id: auth.tenant_id,
            cluster_id: request.cluster_id,
            aggregate_type: "investigation",
            aggregate_id: view.investigation.id.to_string(),
            event_type: "investigation_created",
            payload: json!({"status": view.investigation.status}),
            correlation_id,
            occurred_at: Utc::now(),
        });
        Ok(view)
    }

    pub(crate) async fn investigation(
        &self,
        auth: &AuthContext,
        id: InvestigationId,
    ) -> Result<InvestigationView, ControlPlaneError> {
        self.repository.investigation(auth, id).await
    }

    pub(crate) async fn list_investigations(
        &self,
        auth: &AuthContext,
        query: &WorkflowListQuery,
    ) -> Result<WorkflowPage<InvestigationView>, ControlPlaneError> {
        validate_list_scope(auth, query)?;
        self.repository.list_investigations(auth, query).await
    }

    pub(crate) async fn promote_investigation(
        &self,
        auth: &AuthContext,
        id: InvestigationId,
        request: &PromoteInvestigationRequest,
        correlation_id: CorrelationId,
    ) -> Result<IncidentView, ControlPlaneError> {
        request.validate()?;
        authorize_operator(auth)?;
        let view = self
            .repository
            .promote_investigation(auth, id, request, correlation_id)
            .await?;
        self.events.publish(WorkflowStreamEvent {
            tenant_id: auth.tenant_id,
            cluster_id: view.incident.cluster_id,
            aggregate_type: "incident",
            aggregate_id: view.incident.id.to_string(),
            event_type: "incident_created",
            payload: json!({"status": view.incident.status, "investigation_id": id}),
            correlation_id,
            occurred_at: Utc::now(),
        });
        Ok(view)
    }

    pub(crate) async fn create_incident(
        &self,
        auth: &AuthContext,
        request: &IncidentCreateRequest,
        correlation_id: CorrelationId,
    ) -> Result<IncidentView, ControlPlaneError> {
        request.validate()?;
        authorize_cluster(auth, request.cluster_id)?;
        let view = self.repository.create_incident(auth, request, correlation_id).await?;
        self.events.publish(WorkflowStreamEvent {
            tenant_id: auth.tenant_id,
            cluster_id: request.cluster_id,
            aggregate_type: "incident",
            aggregate_id: view.incident.id.to_string(),
            event_type: "incident_created",
            payload: json!({"status": view.incident.status}),
            correlation_id,
            occurred_at: Utc::now(),
        });
        Ok(view)
    }

    pub(crate) async fn incident(&self, auth: &AuthContext, id: IncidentId) -> Result<IncidentView, ControlPlaneError> {
        self.repository.incident(auth, id).await
    }

    pub(crate) async fn list_incidents(
        &self,
        auth: &AuthContext,
        query: &WorkflowListQuery,
    ) -> Result<WorkflowPage<IncidentView>, ControlPlaneError> {
        validate_list_scope(auth, query)?;
        self.repository.list_incidents(auth, query).await
    }

    pub(crate) fn ensure_operator(&self, auth: &AuthContext) -> Result<(), ControlPlaneError> {
        authorize_operator(auth)
    }

    pub(crate) async fn transition_incident(
        &self,
        auth: &AuthContext,
        id: IncidentId,
        next: IncidentStatus,
        reason: &str,
        correlation_id: CorrelationId,
    ) -> Result<IncidentView, ControlPlaneError> {
        authorize_operator(auth)?;
        let view = self
            .repository
            .transition_incident(auth, id, next, reason, correlation_id)
            .await?;
        self.events.publish(WorkflowStreamEvent {
            tenant_id: auth.tenant_id,
            cluster_id: view.incident.cluster_id,
            aggregate_type: "incident",
            aggregate_id: id.to_string(),
            event_type: "incident_status_changed",
            payload: json!({"status": next, "reason": reason}),
            correlation_id,
            occurred_at: Utc::now(),
        });
        Ok(view)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "an immutable diagnosis revision records its complete provenance tuple"
    )]
    pub(crate) async fn persist_diagnosis_revision(
        &self,
        auth: &AuthContext,
        incident_id: IncidentId,
        status: IncidentStatus,
        rule_result: Value,
        hypotheses: Value,
        evidence_ids: Vec<EvidenceId>,
        partial: bool,
        primary_model_invocation_id: Option<ModelInvocationId>,
        diagnosis_mode: &'static str,
        correlation_id: CorrelationId,
    ) -> Result<DiagnosisRevision, ControlPlaneError> {
        authorize_operator(auth)?;
        let revision = self
            .repository
            .persist_diagnosis_revision(
                auth,
                incident_id,
                status,
                rule_result,
                hypotheses,
                evidence_ids,
                partial,
                primary_model_invocation_id,
                diagnosis_mode,
                correlation_id,
            )
            .await?;
        let incident = self.repository.incident(auth, incident_id).await?;
        self.events.publish(WorkflowStreamEvent {
            tenant_id: auth.tenant_id,
            cluster_id: incident.incident.cluster_id,
            aggregate_type: "incident",
            aggregate_id: incident_id.to_string(),
            event_type: "diagnosis_revision_created",
            payload: json!({
                "revision": revision.revision,
                "status": revision.status,
                "partial": revision.partial,
                "diagnosis_mode": diagnosis_mode,
                "primary_model_invocation_id": revision.primary_model_invocation_id,
                "execution_eligible": false,
            }),
            correlation_id,
            occurred_at: Utc::now(),
        });
        Ok(revision)
    }

    pub(crate) async fn create_inspection(
        &self,
        auth: &AuthContext,
        request: &InspectionCreateRequest,
        correlation_id: CorrelationId,
    ) -> Result<InspectionView, ControlPlaneError> {
        request.validate()?;
        authorize_cluster(auth, request.cluster_id)?;
        let view = self.repository.create_inspection(auth, request, correlation_id).await?;
        self.events.publish(WorkflowStreamEvent {
            tenant_id: auth.tenant_id,
            cluster_id: request.cluster_id,
            aggregate_type: "inspection",
            aggregate_id: view.run.id.to_string(),
            event_type: "inspection_created",
            payload: json!({"status": view.run.status, "template": view.run.template}),
            correlation_id,
            occurred_at: Utc::now(),
        });
        Ok(view)
    }

    pub(crate) async fn inspection(
        &self,
        auth: &AuthContext,
        id: InspectionRunId,
    ) -> Result<InspectionView, ControlPlaneError> {
        self.repository.inspection(auth, id).await
    }

    pub(crate) async fn list_inspections(
        &self,
        auth: &AuthContext,
        query: &WorkflowListQuery,
    ) -> Result<WorkflowPage<InspectionView>, ControlPlaneError> {
        validate_list_scope(auth, query)?;
        self.repository.list_inspections(auth, query).await
    }

    pub(crate) async fn disposition_recommendation(
        &self,
        auth: &AuthContext,
        id: RecommendationId,
        request: &RecommendationDispositionRequest,
        correlation_id: CorrelationId,
    ) -> Result<Recommendation, ControlPlaneError> {
        request.validate()?;
        authorize_operator(auth)?;
        let recommendation = self
            .repository
            .disposition_recommendation(auth, id, request, correlation_id)
            .await?;
        self.events.publish(WorkflowStreamEvent {
            tenant_id: auth.tenant_id,
            cluster_id: recommendation.cluster_id,
            aggregate_type: "recommendation",
            aggregate_id: recommendation.id.to_string(),
            event_type: "recommendation_disposition_changed",
            payload: json!({"status": recommendation.status}),
            correlation_id,
            occurred_at: Utc::now(),
        });
        Ok(recommendation)
    }

    pub(crate) async fn list_recommendations(
        &self,
        auth: &AuthContext,
        query: &WorkflowListQuery,
    ) -> Result<WorkflowPage<Recommendation>, ControlPlaneError> {
        validate_list_scope(auth, query)?;
        self.repository.list_recommendations(auth, query).await
    }
}

fn validate_list_scope(auth: &AuthContext, query: &WorkflowListQuery) -> Result<(), ControlPlaneError> {
    authorize_cluster(auth, query.cluster_id)?;
    query.bounded_limit()?;
    query.cursor_uuid()?;
    Ok(())
}

fn authorize_cluster(
    auth: &AuthContext,
    cluster_id: rocketmq_sre_contracts::ClusterId,
) -> Result<(), ControlPlaneError> {
    if !auth.clusters.contains(&cluster_id) {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "requested cluster is outside the authenticated scope",
        ));
    }
    Ok(())
}

fn authorize_operator(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if !auth.roles.iter().any(|role| {
        matches!(
            role.as_str(),
            "diagnose" | "operator" | "sre-admin" | "rocketmq:diagnose" | "rocketmq:sre"
        )
    }) {
        return Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "operator role is required for workflow promotion or disposition",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::TenantId;

    use super::*;

    #[test]
    fn workflow_lists_reject_cross_cluster_scope_before_repository_access() {
        let allowed_cluster = ClusterId::new();
        let auth = AuthContext {
            tenant_id: TenantId::new(),
            subject: "scope-test".to_owned(),
            clusters: BTreeSet::from([allowed_cluster]),
            roles: BTreeSet::new(),
        };
        let query = WorkflowListQuery {
            cluster_id: ClusterId::new(),
            cursor: None,
            limit: Some(50),
        };

        let error = validate_list_scope(&auth, &query).expect_err("cross-cluster query must fail closed");
        assert!(matches!(
            error,
            ControlPlaneError::Forbidden {
                code: "cluster_not_allowed",
                ..
            }
        ));
    }
}
