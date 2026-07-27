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

use std::collections::BTreeSet;

use chrono::Utc;
use rocketmq_sre_contracts::ActionItem;
use rocketmq_sre_contracts::ActionItemId;
use rocketmq_sre_contracts::ActionItemStatus;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::KnowledgeItem;
use rocketmq_sre_contracts::KnowledgeItemId;
use rocketmq_sre_contracts::KnowledgeReviewStatus;
use rocketmq_sre_contracts::PostmortemDraft;
use rocketmq_sre_contracts::PostmortemId;
use rocketmq_sre_contracts::PostmortemRevision;
use rocketmq_sre_contracts::PostmortemRevisionId;
use rocketmq_sre_contracts::PostmortemStatus;
use rocketmq_sre_core::postmortem::PostmortemAssembly;
use rocketmq_sre_core::postmortem::PostmortemAssemblyInput;
use rocketmq_sre_core::postmortem::assemble;
use rocketmq_sre_core::postmortem::render_markdown;
use rocketmq_sre_core::postmortem::validate_action_item_transition;
use rocketmq_sre_core::postmortem::validate_revision;
use semver::VersionReq;
use sha2::Digest;
use sha2::Sha256;

use super::ActionItemListQuery;
use super::ActionItemPage;
use super::ActionItemPatchRequest;
use super::CreatePostmortemRequest;
use super::PostmortemPatchRequest;
use super::PostmortemPublishRequest;
use super::PostmortemView;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::evidence::EvidenceListQuery;
use crate::evidence::EvidenceService;
use crate::models::ModelGatewayService;
use crate::phase2_repository::Phase2Repository;
use crate::workflow::WorkflowService;

#[derive(Clone)]
pub(crate) struct PostmortemService {
    repository: PostgresRepository,
    evidence: EvidenceService,
    model_gateway: ModelGatewayService,
    workflow: WorkflowService,
}

impl PostmortemService {
    pub(crate) fn new(
        repository: PostgresRepository,
        evidence: EvidenceService,
        model_gateway: ModelGatewayService,
        workflow: WorkflowService,
    ) -> Self {
        Self {
            repository,
            evidence,
            model_gateway,
            workflow,
        }
    }

    pub(crate) async fn create(
        &self,
        auth: &AuthContext,
        incident_id: IncidentId,
        request: &CreatePostmortemRequest,
    ) -> Result<PostmortemView, ControlPlaneError> {
        self.workflow.ensure_operator(auth)?;
        if request.operator_notes.len() > 32 || request.operator_notes.iter().any(|note| note.chars().count() > 1_024) {
            return Err(ControlPlaneError::validation(
                "invalid_request",
                "postmortem accepts at most 32 operator notes of 1024 characters",
            ));
        }
        if let Some(existing) = self.repository.postmortem_by_incident(auth, incident_id).await? {
            return self.view_from_draft(auth, existing).await;
        }
        let incident = self.workflow.incident(auth, incident_id).await?;
        let evidence = self.incident_evidence(auth, &incident).await?;
        let deterministic = assemble(PostmortemAssemblyInput {
            incident: &incident.incident,
            evidence: &evidence,
            diagnosis_revisions: &incident.diagnosis_revisions,
            timeline: &incident.timeline,
            operator_notes: &request.operator_notes,
        });
        let decision = self
            .model_gateway
            .draft_postmortem(
                auth,
                incident_id,
                incident.incident.cluster_id,
                &incident.incident.title,
                deterministic,
                &evidence,
                CorrelationId::new(),
            )
            .await?;
        let content = decision.content;
        let allowed = evidence.iter().map(|item| item.evidence_id).collect();
        validate_revision(&content, &allowed).map_err(validation_error)?;
        let now = Utc::now();
        let draft = PostmortemDraft {
            id: PostmortemId::new(),
            tenant_id: auth.tenant_id,
            cluster_id: incident.incident.cluster_id,
            incident_id,
            status: PostmortemStatus::Draft,
            current_revision: 1,
            confirmed_by: None,
            confirmed_at: None,
            published_knowledge_item_id: None,
            created_by: auth.subject.clone(),
            created_at: now,
            updated_at: now,
        };
        let mut revision = revision_from_content(&draft, 1, &content, &auth.subject, false, now)?;
        revision.model_invocation_id = decision.invocation_id;
        let actions = content
            .action_items
            .iter()
            .map(|proposal| ActionItem {
                id: ActionItemId::new(),
                tenant_id: auth.tenant_id,
                cluster_id: draft.cluster_id,
                postmortem_id: draft.id,
                incident_id,
                title: proposal.title.clone(),
                owner: None,
                due_at: None,
                status: ActionItemStatus::Open,
                verification: None,
                evidence_ids: proposal.evidence_ids.clone(),
                execution_journal: None,
                created_at: now,
                updated_at: now,
                completed_at: None,
            })
            .collect::<Vec<_>>();
        let root_cause_code = content.root_causes.first().map(|cause| cause.code.as_str());
        let component = incident.incident.resource.as_deref().or(Some("cluster"));
        self.repository
            .create_postmortem_bundle(
                &draft,
                &revision,
                &actions,
                incident.incident.fingerprint.as_deref(),
                root_cause_code,
                component,
            )
            .await?;
        self.repository
            .discover_recurrences(
                auth,
                incident_id,
                draft.cluster_id,
                incident.incident.fingerprint.as_deref(),
                root_cause_code,
                component,
                now,
            )
            .await?;
        self.view_from_draft(auth, draft).await
    }

    pub(crate) async fn get(&self, auth: &AuthContext, id: PostmortemId) -> Result<PostmortemView, ControlPlaneError> {
        let draft = self.repository.scoped_postmortem(auth, id).await?;
        self.view_from_draft(auth, draft).await
    }

    pub(crate) async fn patch(
        &self,
        auth: &AuthContext,
        id: PostmortemId,
        request: &PostmortemPatchRequest,
    ) -> Result<PostmortemView, ControlPlaneError> {
        self.workflow.ensure_operator(auth)?;
        let draft = self.repository.scoped_postmortem(auth, id).await?;
        if matches!(draft.status, PostmortemStatus::Published | PostmortemStatus::Archived) {
            return Err(ControlPlaneError::conflict(
                "published or archived postmortems cannot be edited",
            ));
        }
        let revisions = self.repository.scoped_revisions(auth, id).await?;
        let current = revisions
            .last()
            .ok_or_else(|| ControlPlaneError::configuration("postmortem has no immutable content revision"))?;
        let incident = self.workflow.incident(auth, draft.incident_id).await?;
        let evidence = self.incident_evidence(auth, &incident).await?;
        let allowed = evidence.iter().map(|item| item.evidence_id).collect::<BTreeSet<_>>();
        let content = merge_revision(current, request)?;
        validate_revision(&content, &allowed).map_err(validation_error)?;
        let revision_number = draft
            .current_revision
            .checked_add(1)
            .ok_or_else(|| ControlPlaneError::conflict("postmortem revision counter is exhausted"))?;
        let revision = revision_from_content(
            &draft,
            revision_number,
            &content,
            &auth.subject,
            request.human_confirmed,
            Utc::now(),
        )?;
        self.repository.append_postmortem_revision(&revision).await?;
        self.get(auth, id).await
    }

    pub(crate) async fn publish(
        &self,
        auth: &AuthContext,
        id: PostmortemId,
        request: &PostmortemPublishRequest,
    ) -> Result<PostmortemView, ControlPlaneError> {
        self.workflow.ensure_operator(auth)?;
        validate_publish_request(request)?;
        let draft = self.repository.scoped_postmortem(auth, id).await?;
        if draft.status == PostmortemStatus::Published {
            return self.view_from_draft(auth, draft).await;
        }
        if draft.status != PostmortemStatus::Confirmed || !request.human_confirmed {
            return Err(ControlPlaneError::validation(
                "human_validation_required",
                "postmortem publication requires a current human-confirmed revision",
            ));
        }
        let revisions = self.repository.scoped_revisions(auth, id).await?;
        let current = revisions
            .last()
            .ok_or_else(|| ControlPlaneError::configuration("postmortem has no immutable content revision"))?;
        if !current.human_confirmed {
            return Err(ControlPlaneError::validation(
                "human_validation_required",
                "the current postmortem revision is not human confirmed",
            ));
        }
        let content = content_from_revision(current)?;
        let markdown = render_markdown(&content);
        let now = Utc::now();
        let knowledge = KnowledgeItem {
            id: KnowledgeItemId::new(),
            tenant_id: auth.tenant_id,
            cluster_id: Some(draft.cluster_id),
            title: format!("Postmortem: {}", content.summary.chars().take(220).collect::<String>()),
            component: request.component.trim().to_owned(),
            rocketmq_version_range: request.rocketmq_version_range.trim().to_owned(),
            source_uri: format!("rocketmq-sre://postmortems/{id}"),
            source_version: format!("revision-{}", current.revision),
            valid_from: Some(now),
            valid_until: None,
            owner: request.owner.trim().to_owned(),
            review_status: KnowledgeReviewStatus::Validated,
            review_due_at: request.review_due_at,
            sensitivity: "internal".to_owned(),
            content_hash: format!("sha256:{:x}", Sha256::digest(markdown.as_bytes())),
            conflict: false,
            created_at: now,
            updated_at: now,
        };
        let root_cause_code = current.root_causes.first().map(|cause| cause.code.as_str());
        self.repository
            .publish_postmortem(
                auth,
                &draft,
                current,
                &knowledge,
                &markdown,
                root_cause_code,
                request.component.trim(),
            )
            .await?;
        self.get(auth, id).await
    }

    pub(crate) async fn list_action_items(
        &self,
        auth: &AuthContext,
        query: &ActionItemListQuery,
    ) -> Result<ActionItemPage, ControlPlaneError> {
        let items = self.repository.list_action_items_scoped(auth, query).await?;
        Ok(ActionItemPage {
            items,
            partial: false,
            observed_at: Utc::now(),
        })
    }

    pub(crate) async fn patch_action_item(
        &self,
        auth: &AuthContext,
        id: ActionItemId,
        request: &ActionItemPatchRequest,
    ) -> Result<ActionItem, ControlPlaneError> {
        self.workflow.ensure_operator(auth)?;
        let current = self.repository.scoped_action_item(auth, id).await?;
        if current.execution_journal.is_some() {
            return Err(ControlPlaneError::conflict(
                "Phase 2 action items cannot contain an execution journal",
            ));
        }
        let incident = self.workflow.incident(auth, current.incident_id).await?;
        let evidence = self.incident_evidence(auth, &incident).await?;
        let allowed = evidence.iter().map(|item| item.evidence_id).collect::<BTreeSet<_>>();
        if let Some(unknown) = request
            .evidence_ids
            .iter()
            .find(|evidence_id| !allowed.contains(evidence_id))
        {
            return Err(ControlPlaneError::validation(
                "unknown_evidence_citation",
                format!("Evidence citation {unknown} is outside the Incident scope"),
            ));
        }
        let owner = request.owner.as_deref().or(current.owner.as_deref());
        validate_action_item_transition(
            current.status,
            request.status,
            owner,
            request.verification.as_deref(),
            &request.evidence_ids,
        )
        .map_err(validation_error)?;
        let now = Utc::now();
        let mut next = current.clone();
        next.status = request.status;
        next.owner = request.owner.clone().or(current.owner.clone());
        next.due_at = request.due_at.or(current.due_at);
        next.verification = request.verification.clone();
        next.evidence_ids.clone_from(&request.evidence_ids);
        next.updated_at = now;
        next.completed_at = (request.status == ActionItemStatus::Completed).then_some(now);
        next.execution_journal = None;
        self.repository.update_action_item(auth, &current, &next).await?;
        Ok(next)
    }

    async fn view_from_draft(
        &self,
        auth: &AuthContext,
        draft: PostmortemDraft,
    ) -> Result<PostmortemView, ControlPlaneError> {
        let revisions = self.repository.scoped_revisions(auth, draft.id).await?;
        let action_items = self.repository.scoped_action_items(auth, draft.id).await?;
        let recurrences = self.repository.recurrences(auth, draft.id).await?;
        let todos = self.repository.todos_for_postmortem(auth, &draft).await?;
        let knowledge_item = self.repository.knowledge_for_postmortem(auth, &draft).await?;
        let execution_journal_empty = action_items.iter().all(|item| item.execution_journal.is_none());
        Ok(PostmortemView {
            postmortem: draft,
            revisions,
            action_items,
            recurrences,
            todos,
            knowledge_item,
            execution_journal_empty,
        })
    }

    async fn incident_evidence(
        &self,
        auth: &AuthContext,
        incident: &crate::workflow::IncidentView,
    ) -> Result<Vec<rocketmq_sre_contracts::EvidenceSnapshot>, ControlPlaneError> {
        let page = self
            .evidence
            .list(
                auth,
                &EvidenceListQuery {
                    cluster_id: incident.incident.cluster_id,
                    incident_id: Some(incident.incident.id),
                    source: None,
                    limit: Some(200),
                    cursor: None,
                },
            )
            .await?;
        Ok(page.items)
    }
}

fn merge_revision(
    current: &PostmortemRevision,
    request: &PostmortemPatchRequest,
) -> Result<PostmortemAssembly, ControlPlaneError> {
    Ok(PostmortemAssembly {
        summary: request.summary.clone().unwrap_or_else(|| current.summary.clone()),
        impact: request.impact.clone().unwrap_or_else(|| current.impact.clone()),
        detection: request.detection.clone().unwrap_or_else(|| current.detection.clone()),
        timeline: request.timeline.clone().unwrap_or(
            serde_json::from_value(current.timeline.clone())
                .map_err(|_| ControlPlaneError::configuration("stored postmortem timeline is invalid"))?,
        ),
        root_causes: request
            .root_causes
            .clone()
            .unwrap_or_else(|| current.root_causes.clone()),
        contributing_factors: request
            .contributing_factors
            .clone()
            .unwrap_or_else(|| current.contributing_factors.clone()),
        conclusions: request
            .conclusions
            .clone()
            .unwrap_or_else(|| current.conclusions.clone()),
        recovery: request.recovery.clone().unwrap_or_else(|| current.recovery.clone()),
        effective_actions: request
            .effective_actions
            .clone()
            .unwrap_or_else(|| current.effective_actions.clone()),
        ineffective_actions: request
            .ineffective_actions
            .clone()
            .unwrap_or_else(|| current.ineffective_actions.clone()),
        evidence_ids: request
            .evidence_ids
            .clone()
            .unwrap_or_else(|| current.evidence_ids.clone()),
        action_items: Vec::new(),
    })
}

fn content_from_revision(revision: &PostmortemRevision) -> Result<PostmortemAssembly, ControlPlaneError> {
    merge_revision(
        revision,
        &PostmortemPatchRequest {
            summary: None,
            impact: None,
            detection: None,
            timeline: None,
            root_causes: None,
            contributing_factors: None,
            conclusions: None,
            recovery: None,
            effective_actions: None,
            ineffective_actions: None,
            evidence_ids: None,
            human_confirmed: revision.human_confirmed,
        },
    )
}

fn revision_from_content(
    draft: &PostmortemDraft,
    number: u32,
    content: &PostmortemAssembly,
    actor: &str,
    human_confirmed: bool,
    at: chrono::DateTime<Utc>,
) -> Result<PostmortemRevision, ControlPlaneError> {
    Ok(PostmortemRevision {
        id: PostmortemRevisionId::new(),
        postmortem_id: draft.id,
        revision: number,
        summary: content.summary.clone(),
        impact: content.impact.clone(),
        detection: content.detection.clone(),
        timeline: serde_json::to_value(&content.timeline)
            .map_err(|_| ControlPlaneError::configuration("postmortem timeline cannot be encoded"))?,
        root_causes: content.root_causes.clone(),
        contributing_factors: content.contributing_factors.clone(),
        conclusions: content.conclusions.clone(),
        recovery: content.recovery.clone(),
        effective_actions: content.effective_actions.clone(),
        ineffective_actions: content.ineffective_actions.clone(),
        evidence_ids: content.evidence_ids.clone(),
        model_invocation_id: None,
        edited_by: actor.to_owned(),
        human_confirmed,
        created_at: at,
    })
}

fn validate_publish_request(request: &PostmortemPublishRequest) -> Result<(), ControlPlaneError> {
    for (field, value, max) in [
        ("owner", request.owner.as_str(), 200),
        ("component", request.component.as_str(), 200),
    ] {
        if value.trim().is_empty() || value.chars().count() > max {
            return Err(ControlPlaneError::validation(
                "invalid_request",
                format!("{field} must contain between 1 and {max} characters"),
            ));
        }
    }
    VersionReq::parse(request.rocketmq_version_range.trim()).map_err(|_| {
        ControlPlaneError::validation(
            "invalid_request",
            "RocketMQ version range must be a semantic version requirement",
        )
    })?;
    if request.review_due_at <= Utc::now() {
        return Err(ControlPlaneError::validation(
            "invalid_request",
            "knowledge review due date must be in the future",
        ));
    }
    Ok(())
}

fn validation_error(error: impl std::fmt::Display) -> ControlPlaneError {
    ControlPlaneError::validation("invalid_postmortem", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unconfirmed_publish_is_rejected_before_storage() {
        let request = PostmortemPublishRequest {
            human_confirmed: false,
            owner: "sre".to_owned(),
            component: "broker".to_owned(),
            rocketmq_version_range: "*".to_owned(),
            review_due_at: Utc::now() + chrono::Duration::days(30),
        };
        assert!(validate_publish_request(&request).is_ok());
        assert!(!request.human_confirmed);
    }
}
