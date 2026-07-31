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

use std::time::Duration;

use chrono::Utc;
use rocketmq_sre_contracts::AUTOMATION_SCHEMA_VERSION;
use rocketmq_sre_contracts::AutomationFeedbackId;
use rocketmq_sre_contracts::AutomationOperatorFeedback;
#[cfg(test)]
use rocketmq_sre_contracts::AutomationRunId;
use rocketmq_sre_contracts::NoSideEffectAutomationKind;
use rocketmq_sre_contracts::NoSideEffectAutomationRequest;
use rocketmq_sre_contracts::NoSideEffectAutomationRun;

use super::adapter::AutomationDispatchOutcome;
use super::adapter::AutomationDispatcher;
use super::model::AutomationRunListQuery;
use super::model::AutomationRunPage;
use super::model::CompleteAutomationRunRequest;
use super::model::RecordAutomationFeedbackRequest;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::connector_channel::PostgresConnectorChannelService;
use crate::evidence::EvidenceService;
use crate::postmortem::PostmortemService;

const MAX_RUN_PAGE: u16 = 200;

#[derive(Clone)]
pub(crate) struct AutomationService {
    repository: PostgresRepository,
    dispatcher: Option<AutomationDispatcher>,
}

impl AutomationService {
    pub(crate) fn new(
        repository: PostgresRepository,
        connector_channel: PostgresConnectorChannelService,
        evidence: EvidenceService,
        postmortems: PostmortemService,
    ) -> Result<Self, ControlPlaneError> {
        let dispatcher = AutomationDispatcher::new(repository.clone(), connector_channel, evidence, postmortems)?;
        Ok(Self {
            repository,
            dispatcher: Some(dispatcher),
        })
    }

    #[cfg(test)]
    pub(super) const fn persistence_only(repository: PostgresRepository) -> Self {
        Self {
            repository,
            dispatcher: None,
        }
    }

    pub(crate) async fn submit(
        &self,
        auth: &AuthContext,
        request: &NoSideEffectAutomationRequest,
    ) -> Result<NoSideEffectAutomationRun, ControlPlaneError> {
        require_automation_or_operator(auth)?;
        request
            .validate()
            .map_err(|error| ControlPlaneError::validation("invalid_automation_request", error.to_string()))?;
        if request.tenant_id != auth.tenant_id || request.requested_by != auth.subject {
            return Err(ControlPlaneError::forbidden(
                "automation_identity_mismatch",
                "automation request tenant and requester must match the authenticated identity",
            ));
        }
        if let Some(cluster_id) = request.cluster_id
            && !auth.clusters.contains(&cluster_id)
        {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "automation request is outside the authenticated cluster scope",
            ));
        }
        let deterministic_only = matches!(
            request.kind,
            NoSideEffectAutomationKind::AlertCorrelation
                | NoSideEffectAutomationKind::SeverityOwnerSuggestion
                | NoSideEffectAutomationKind::EvidenceCollection
                | NoSideEffectAutomationKind::ShiftSummary
                | NoSideEffectAutomationKind::Notification
        );
        if deterministic_only && request.budget.max_model_calls != 0 {
            return Err(ControlPlaneError::validation(
                "model_budget_not_allowed",
                "this automation kind is deterministic and cannot allocate model calls",
            ));
        }
        if request.kind == NoSideEffectAutomationKind::PostmortemDraft && request.budget.max_model_calls == 0 {
            return Err(ControlPlaneError::validation(
                "model_budget_required",
                "postmortem draft automation requires a bounded model call budget",
            ));
        }
        let run = self.repository.create_no_side_effect_run(request).await?;
        if run.status.is_terminal() {
            return Ok(run);
        }
        let Some(dispatcher) = &self.dispatcher else {
            return Ok(run);
        };
        let (run, claimed) = self.repository.claim_no_side_effect_run(auth.tenant_id, run.id).await?;
        if !claimed {
            return Ok(run);
        }
        let dispatch = tokio::time::timeout(
            Duration::from_secs(u64::from(request.budget.timeout_seconds)),
            dispatcher.dispatch(auth, request),
        )
        .await;
        let outcome = match dispatch {
            Ok(Ok(outcome)) => bounded_outcome(outcome, request.budget.max_output_bytes)?,
            Ok(Err(error)) => AutomationDispatchOutcome {
                status: rocketmq_sre_contracts::AutomationRunStatus::Failed,
                result_code: automation_failure_code(&error).to_owned(),
                sanitized_summary: "Bounded automation failed without changing RocketMQ resources".to_owned(),
                artifacts: Vec::new(),
                model_invocation_id: None,
            },
            Err(_) => AutomationDispatchOutcome {
                status: rocketmq_sre_contracts::AutomationRunStatus::Failed,
                result_code: "automation_timeout".to_owned(),
                sanitized_summary: "Bounded automation exceeded its configured timeout without mutation".to_owned(),
                artifacts: Vec::new(),
                model_invocation_id: None,
            },
        };
        self.repository
            .complete_no_side_effect_run(
                auth.tenant_id,
                run.id,
                &CompleteAutomationRunRequest {
                    status: outcome.status,
                    result_code: outcome.result_code,
                    sanitized_summary: outcome.sanitized_summary,
                    artifacts: outcome.artifacts,
                    model_invocation_id: outcome.model_invocation_id,
                    completed_at: Utc::now(),
                },
            )
            .await
    }

    #[cfg(test)]
    pub(super) async fn complete(
        &self,
        auth: &AuthContext,
        run_id: AutomationRunId,
        request: &CompleteAutomationRunRequest,
    ) -> Result<NoSideEffectAutomationRun, ControlPlaneError> {
        require_automation_service(auth)?;
        self.repository
            .complete_no_side_effect_run(auth.tenant_id, run_id, request)
            .await
    }

    pub(super) async fn list(
        &self,
        auth: &AuthContext,
        query: &AutomationRunListQuery,
    ) -> Result<AutomationRunPage, ControlPlaneError> {
        require_automation_reader(auth)?;
        if let Some(cluster_id) = query.cluster_id
            && !auth.clusters.contains(&cluster_id)
        {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "automation query is outside the authenticated cluster scope",
            ));
        }
        let limit = query.limit.clamp(1, MAX_RUN_PAGE);
        let mut items = self
            .repository
            .no_side_effect_runs(auth.tenant_id, query, i64::from(limit) + 1)
            .await?;
        let truncated = items.len() > usize::from(limit);
        items.truncate(usize::from(limit));
        Ok(AutomationRunPage {
            schema_version: AUTOMATION_SCHEMA_VERSION,
            items,
            truncated,
        })
    }

    pub(super) async fn record_feedback(
        &self,
        auth: &AuthContext,
        request: &RecordAutomationFeedbackRequest,
    ) -> Result<AutomationOperatorFeedback, ControlPlaneError> {
        require_human_operator(auth)?;
        if let Some(cluster_id) = request.cluster_id
            && !auth.clusters.contains(&cluster_id)
        {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "automation feedback is outside the authenticated cluster scope",
            ));
        }
        let feedback = AutomationOperatorFeedback {
            schema_version: AUTOMATION_SCHEMA_VERSION.to_owned(),
            id: AutomationFeedbackId::new(),
            tenant_id: auth.tenant_id,
            cluster_id: request.cluster_id,
            incident_id: request.incident_id,
            subject: request.subject,
            subject_id: request.subject_id,
            verdict: request.verdict,
            comment: request.comment.clone(),
            actor_subject: auth.subject.clone(),
            created_at: Utc::now(),
        };
        self.repository.store_automation_feedback(&feedback).await
    }
}

pub(super) fn require_automation_reader(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if auth.roles.iter().any(|role| {
        matches!(
            role.as_str(),
            "operator" | "approver" | "automation_service" | "rocketmq:diagnose"
        )
    }) {
        return Ok(());
    }
    Err(ControlPlaneError::forbidden(
        "automation_read_forbidden",
        "automation runs require an operator, automation service, or diagnose role",
    ))
}

pub(super) fn require_automation_or_operator(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if auth
        .roles
        .iter()
        .any(|role| matches!(role.as_str(), "operator" | "automation_service"))
    {
        return Ok(());
    }
    Err(ControlPlaneError::forbidden(
        "automation_submit_forbidden",
        "automation submission requires an operator or automation service role",
    ))
}

#[cfg(test)]
fn require_automation_service(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if auth.roles.contains("automation_service") {
        return Ok(());
    }
    Err(ControlPlaneError::forbidden(
        "automation_completion_forbidden",
        "only the automation service can complete a bounded run",
    ))
}

fn bounded_outcome(
    outcome: AutomationDispatchOutcome,
    maximum_bytes: u32,
) -> Result<AutomationDispatchOutcome, ControlPlaneError> {
    let encoded = serde_json::to_vec(&(
        &outcome.result_code,
        &outcome.sanitized_summary,
        &outcome.artifacts,
        outcome.model_invocation_id,
    ))
    .map_err(|_| ControlPlaneError::validation("invalid_automation_result", "automation outcome cannot be encoded"))?;
    if encoded.len() <= maximum_bytes as usize {
        return Ok(outcome);
    }
    Ok(AutomationDispatchOutcome {
        status: rocketmq_sre_contracts::AutomationRunStatus::Failed,
        result_code: "output_too_large".to_owned(),
        sanitized_summary: "Automation output exceeded its configured byte budget and was discarded".to_owned(),
        artifacts: Vec::new(),
        model_invocation_id: None,
    })
}

pub(super) const fn automation_failure_code(error: &ControlPlaneError) -> &'static str {
    match error {
        ControlPlaneError::Validation { code, .. }
        | ControlPlaneError::Forbidden { code, .. }
        | ControlPlaneError::Conflict { code, .. } => code,
        ControlPlaneError::Unauthorized => "unauthorized_scope",
        ControlPlaneError::NotFound
        | ControlPlaneError::Configuration { .. }
        | ControlPlaneError::Database(_)
        | ControlPlaneError::IdentityProvider(_)
        | ControlPlaneError::Executor(_)
        | ControlPlaneError::ObjectStore
        | ControlPlaneError::CapabilityDocument { .. }
        | ControlPlaneError::Io(_) => "source_unavailable",
    }
}

fn require_human_operator(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if auth.roles.contains("operator") && !auth.roles.contains("model_service") {
        return Ok(());
    }
    Err(ControlPlaneError::forbidden(
        "automation_feedback_forbidden",
        "automation feedback requires a human operator role",
    ))
}
