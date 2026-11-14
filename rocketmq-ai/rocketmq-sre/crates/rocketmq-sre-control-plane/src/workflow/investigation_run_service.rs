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

use chrono::Utc;
use rocketmq_sre_contracts::{
    CreateInvestigationRunRequest, CreateInvestigationRunResponse, EvidenceId, InvestigationCommand,
    InvestigationCommandRequest, InvestigationId, InvestigationPackRef, InvestigationRequirementRef, InvestigationRun,
    InvestigationRunContext, InvestigationRunEventPage, InvestigationRunId, InvestigationRunPermission,
    InvestigationRunScope,
};
use rocketmq_sre_core::diagnostics::{DiagnosticPackRegistry, full_registry};

use super::investigation_run_repository::RunCommandOutcome;
use crate::{ControlPlaneRequestFailure, PostgresRepository, auth::AuthContext};

mod api;
pub(super) mod bindings;
#[cfg(test)]
mod control_http_tests;
mod controls;
mod dispatcher;
mod failure;
#[cfg(test)]
mod http_tests;
#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod receipt_replay_tests;
mod recovery;
pub(super) mod scope;
#[cfg(test)]
mod scope_tests;
#[cfg(test)]
mod test_support;

pub(crate) use api::routes;
pub(crate) use failure::contract_failure;

/// Authenticated run operations and an optionally configured owned dispatcher.
#[derive(Clone)]
pub(crate) struct InvestigationRunService {
    repository: PostgresRepository,
    catalog: Arc<DiagnosticPackRegistry>,
    recovery_owner: uuid::Uuid,
    dispatcher: Option<Arc<dispatcher::RunDispatcher>>,
}

impl InvestigationRunService {
    pub(crate) fn new(repository: PostgresRepository) -> Result<Self, ControlPlaneRequestFailure> {
        Ok(Self {
            repository,
            catalog: Arc::new(
                full_registry()
                    .map_err(|_| ControlPlaneRequestFailure::configuration("diagnostic registry rejected"))?,
            ),
            recovery_owner: uuid::Uuid::new_v4(),
            dispatcher: None,
        })
    }

    pub(crate) fn with_dispatcher(
        mut self,
        channel: crate::connector_channel::PostgresConnectorChannelService,
        evidence: crate::evidence::EvidenceService,
        models: crate::models::ModelGatewayService,
    ) -> Self {
        self.dispatcher = Some(Arc::new(dispatcher::RunDispatcher::new(channel, evidence, models)));
        self
    }

    pub(crate) async fn create(
        &self,
        auth: &AuthContext,
        investigation_id: InvestigationId,
        request: &CreateInvestigationRunRequest,
    ) -> Result<CreateInvestigationRunResponse, ControlPlaneRequestFailure> {
        authorize_control(auth)?;
        if investigation_id != request.scope.investigation_id {
            return Err(invalid_identity());
        }
        // Admission deadlines belong inside the transaction's new/replay decision.
        let context = self.context(auth, &request.scope, &request.scope, &[]).await?;
        self.repository
            .investigation_run_create(auth, &context, request, Utc::now())
            .await
    }

    pub(crate) async fn get(
        &self,
        auth: &AuthContext,
        id: InvestigationRunId,
    ) -> Result<InvestigationRun, ControlPlaneRequestFailure> {
        let run = self.repository.investigation_run_get(auth, id).await?;
        scope::authorize_saved_scope(&self.repository, auth, &run.initial_scope).await?;
        Ok(run)
    }

    pub(crate) async fn command(
        &self,
        auth: &AuthContext,
        id: InvestigationRunId,
        request: &InvestigationCommandRequest,
    ) -> Result<RunCommandOutcome, ControlPlaneRequestFailure> {
        authorize_control(auth)?;
        if id != request.run_id {
            return Err(invalid_identity());
        }
        let run = self.get(auth, id).await?;
        let (evidence_scope, ids) = match &request.command {
            InvestigationCommand::Steer { refinement } => (&refinement.scope, refinement.evidence_ids.as_slice()),
            _ => (&run.initial_scope, &[][..]),
        };
        let context = self.context(auth, &run.initial_scope, evidence_scope, ids).await?;
        self.repository
            .investigation_run_command(auth, &context, request, Utc::now())
            .await
    }

    pub(crate) async fn events(
        &self,
        auth: &AuthContext,
        id: InvestigationRunId,
        after: u64,
        limit: u32,
    ) -> Result<InvestigationRunEventPage, ControlPlaneRequestFailure> {
        self.get(auth, id).await?;
        self.repository.investigation_run_events(auth, id, after, limit).await
    }

    async fn context(
        &self,
        auth: &AuthContext,
        authority_scope: &InvestigationRunScope,
        evidence_scope: &InvestigationRunScope,
        ids: &[EvidenceId],
    ) -> Result<InvestigationRunContext, ControlPlaneRequestFailure> {
        scope::authorize_saved_scope(&self.repository, auth, authority_scope).await?;
        let subject = scope::authorize_saved_scope(&self.repository, auth, evidence_scope).await?;
        let visible_evidence =
            scope::evidence_context(&self.repository, auth, evidence_scope, subject.as_ref(), ids).await?;
        let mut allowed_packs = Vec::new();
        let mut allowed_requirements = Vec::new();
        for pack in self.catalog.active_packs() {
            let reference = InvestigationPackRef {
                pack_id: pack.qualified_id(),
                pack_version: pack.version().to_string(),
            };
            for requirement in pack.required_evidence().iter().chain(pack.optional_evidence()) {
                allowed_requirements.push(InvestigationRequirementRef {
                    pack: reference.clone(),
                    requirement_id: requirement.key.to_owned(),
                });
            }
            allowed_packs.push(reference);
        }
        Ok(InvestigationRunContext {
            actor_subject: auth.subject.clone(),
            permission: InvestigationRunPermission::Control,
            authorized_scope: authority_scope.clone(),
            allowed_packs,
            allowed_requirements,
            visible_evidence,
        })
    }
}

pub(super) fn authorize_control(auth: &AuthContext) -> Result<(), ControlPlaneRequestFailure> {
    if auth.subject.trim().is_empty()
        || auth.subject.len() > 255
        || auth.subject.chars().any(char::is_control)
        || !auth.roles.iter().any(|role| {
            matches!(
                role.as_str(),
                "diagnose" | "operator" | "sre-admin" | "rocketmq:diagnose" | "rocketmq:sre"
            )
        })
    {
        return Err(ControlPlaneRequestFailure::forbidden(
            "unauthorized",
            "run control requires an operator",
        ));
    }
    Ok(())
}

fn invalid_identity() -> ControlPlaneRequestFailure {
    ControlPlaneRequestFailure::validation("invalid_input", "path and request identifiers differ")
}
