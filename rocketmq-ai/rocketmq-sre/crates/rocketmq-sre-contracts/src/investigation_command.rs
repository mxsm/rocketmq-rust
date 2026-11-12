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

use chrono::{DateTime, Duration, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::investigation_run::validate_packs;
use crate::investigation_run_error::{valid_run_key, valid_run_text};
use crate::{
    EvidenceId, INVESTIGATION_RUN_SCHEMA_VERSION, InvestigationBudget, InvestigationCommandId,
    InvestigationControlEpoch, InvestigationPackRef, InvestigationRevision, InvestigationRun, InvestigationRunContext,
    InvestigationRunFailure, InvestigationRunId, InvestigationRunScope, InvestigationRunStatus, TenantId,
    canonical_sha256,
};

/// Proposed creation body. D13 exports a contract, not an HTTP operation.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateInvestigationRunRequest {
    pub schema_version: String,
    pub run_id: InvestigationRunId,
    pub scope: InvestigationRunScope,
    pub packs: Vec<InvestigationPackRef>,
    pub budget: InvestigationBudget,
    pub deadline: DateTime<Utc>,
    pub idempotency_key: String,
}

/// Original creation response and its actor-bound receipt, retained for exact replay.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateInvestigationRunResponse {
    pub run: InvestigationRun,
    pub actor_subject: String,
    pub idempotency_key: String,
    pub request_digest: String,
    pub replayed: bool,
}

/// Typed operator refinement. It never resets the deadline or remaining budget.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationRefinement {
    pub scope: InvestigationRunScope,
    /// Order is meaningful: it expresses priority among the remaining packs.
    pub packs: Vec<InvestigationPackRef>,
    pub evidence_ids: Vec<EvidenceId>,
}

/// No arbitrary tool, URL, shell, prompt, or JSON patch is accepted here.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InvestigationCommand {
    Pause {},
    Resume {},
    Cancel {},
    Steer { refinement: InvestigationRefinement },
}

#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationCommandRequest {
    pub schema_version: String,
    pub command_id: InvestigationCommandId,
    pub run_id: InvestigationRunId,
    pub expected_revision: InvestigationRevision,
    pub idempotency_key: String,
    pub reason: String,
    pub command: InvestigationCommand,
}

/// Immutable result of an accepted command. Replay does not reapply its old status.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationCommandReceipt {
    pub schema_version: String,
    pub command_id: InvestigationCommandId,
    pub tenant_id: TenantId,
    pub actor_subject: String,
    pub run_id: InvestigationRunId,
    pub idempotency_key: String,
    pub request_digest: String,
    pub applied_revision: InvestigationRevision,
    pub control_epoch: InvestigationControlEpoch,
    pub status: InvestigationRunStatus,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvestigationCommandClassification {
    New,
    Replay,
}

impl CreateInvestigationRunRequest {
    /// Checks current authority before considering an original idempotent response.
    ///
    /// # Errors
    /// Rejects unbounded inputs, future observation windows, and unauthorized packs.
    pub fn validate(&self, context: &InvestigationRunContext) -> Result<(), InvestigationRunFailure> {
        context.authorize(&self.scope)?;
        self.scope.validate()?;
        self.budget.validate()?;
        validate_packs(&self.packs)?;
        if self.schema_version != INVESTIGATION_RUN_SCHEMA_VERSION
            || self.run_id.as_uuid().is_nil()
            || !valid_run_key(&self.idempotency_key)
        {
            return Err(InvestigationRunFailure::InvalidInput);
        }
        if self.packs.iter().any(|pack| !context.allowed_packs.contains(pack)) {
            return Err(InvestigationRunFailure::PackNotAllowed);
        }
        Ok(())
    }
}

impl InvestigationRun {
    /// Creates or replays a run without external effects. Callers supply time and identity.
    ///
    /// # Errors
    /// Rejects lost authority before replay, content conflicts, and invalid deadlines.
    /// D14 must atomically persist the chosen response under the same actor/run/key.
    pub fn create(
        context: &InvestigationRunContext,
        request: &CreateInvestigationRunRequest,
        previous: Option<&CreateInvestigationRunResponse>,
        at: DateTime<Utc>,
    ) -> Result<CreateInvestigationRunResponse, InvestigationRunFailure> {
        request.validate(context)?;
        let digest = canonical_sha256(request).map_err(|_| InvestigationRunFailure::InvalidInput)?;
        if let Some(previous) = previous {
            if previous.run.id != request.run_id
                || previous.run.initial_scope != request.scope
                || previous.actor_subject != context.actor_subject
                || previous.idempotency_key != request.idempotency_key
                || previous.request_digest != digest
            {
                return Err(InvestigationRunFailure::IdempotencyConflict);
            }
            previous.run.validate()?;
            let mut response = previous.clone();
            response.replayed = true;
            return Ok(response);
        }
        if request.scope.time_range.end > at
            || request.deadline <= at
            || request.deadline.signed_duration_since(at) > Duration::hours(1)
        {
            return Err(InvestigationRunFailure::InvalidInput);
        }
        let run = Self {
            schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
            id: request.run_id,
            initial_scope: request.scope.clone(),
            effective_scope: request.scope.clone(),
            initial_packs: request.packs.clone(),
            effective_packs: request.packs.clone(),
            status: InvestigationRunStatus::Queued,
            revision: InvestigationRevision::default(),
            control_epoch: InvestigationControlEpoch::default(),
            budget: request.budget.clone(),
            deadline: request.deadline,
            steps: Vec::new(),
            evidence_ids: Vec::new(),
            supplied_evidence_ids: Vec::new(),
            assessments: Vec::new(),
            created_by_subject: context.actor_subject.clone(),
            created_at: at,
            updated_at: at,
        };
        run.validate()?;
        Ok(CreateInvestigationRunResponse {
            run,
            actor_subject: context.actor_subject.clone(),
            idempotency_key: request.idempotency_key.clone(),
            request_digest: digest,
            replayed: false,
        })
    }

    /// Classifies a currently authorized request before checking a new command's revision.
    ///
    /// # Errors
    /// An exact old receipt may replay; a new key with an old revision conflicts.
    pub fn classify_command(
        &self,
        context: &InvestigationRunContext,
        request: &InvestigationCommandRequest,
        previous: Option<&InvestigationCommandReceipt>,
    ) -> Result<InvestigationCommandClassification, InvestigationRunFailure> {
        context.authorize(&self.initial_scope)?;
        self.validate()?;
        if request.schema_version != INVESTIGATION_RUN_SCHEMA_VERSION
            || request.run_id != self.id
            || request.command_id.as_uuid().is_nil()
            || !valid_run_key(&request.idempotency_key)
            || !valid_run_text(&request.reason, 1024)
        {
            return Err(InvestigationRunFailure::InvalidInput);
        }
        if let InvestigationCommand::Steer { refinement } = &request.command {
            refinement.scope.validate()?;
            context.authorize(&refinement.scope)?;
            validate_packs(&refinement.packs)?;
            context.evidence_visible(&refinement.evidence_ids, &refinement.scope)?;
            if refinement
                .packs
                .iter()
                .any(|pack| !context.allowed_packs.contains(pack))
            {
                return Err(InvestigationRunFailure::PackNotAllowed);
            }
        }
        let digest = command_digest(request)?;
        if let Some(previous) = previous {
            if previous.schema_version != INVESTIGATION_RUN_SCHEMA_VERSION
                || previous.tenant_id != self.initial_scope.tenant_id
                || previous.run_id != self.id
                || previous.actor_subject != context.actor_subject
                || previous.idempotency_key != request.idempotency_key
                || previous.request_digest != digest
                || previous.command_id != request.command_id
            {
                return Err(InvestigationRunFailure::IdempotencyConflict);
            }
            return Ok(InvestigationCommandClassification::Replay);
        }
        self.expect_revision(request.expected_revision)?;
        Ok(InvestigationCommandClassification::New)
    }

    /// Applies one command, or returns its original receipt without touching current state.
    ///
    /// # Errors
    /// Rejects invalid transitions, widening refinements, exhausted counters and stale new commands.
    pub fn apply_command(
        &mut self,
        context: &InvestigationRunContext,
        request: &InvestigationCommandRequest,
        previous: Option<&InvestigationCommandReceipt>,
        at: DateTime<Utc>,
    ) -> Result<InvestigationCommandReceipt, InvestigationRunFailure> {
        if self.classify_command(context, request, previous)? == InvestigationCommandClassification::Replay {
            return previous.cloned().ok_or(InvestigationRunFailure::IdempotencyConflict);
        }
        if self.status.is_terminal() {
            return Err(InvestigationRunFailure::InvalidStateTransition);
        }
        let mut next = self.clone();
        match &request.command {
            InvestigationCommand::Pause {} => {
                if next.status != InvestigationRunStatus::Running {
                    return Err(InvestigationRunFailure::InvalidStateTransition);
                }
                next.status = if next.in_flight_count() == 0 {
                    InvestigationRunStatus::Paused
                } else {
                    InvestigationRunStatus::Pausing
                };
            }
            InvestigationCommand::Resume {} => {
                if next.status != InvestigationRunStatus::Paused {
                    return Err(InvestigationRunFailure::InvalidStateTransition);
                }
                next.check_live(at)?;
                next.status = InvestigationRunStatus::Running;
            }
            InvestigationCommand::Cancel {} => {
                if next.status == InvestigationRunStatus::Cancelling {
                    return Err(InvestigationRunFailure::InvalidStateTransition);
                }
                next.next_epoch()?;
                next.status = if next.in_flight_count() == 0 {
                    InvestigationRunStatus::Cancelled
                } else {
                    InvestigationRunStatus::Cancelling
                };
            }
            InvestigationCommand::Steer { refinement } => {
                if !matches!(
                    next.status,
                    InvestigationRunStatus::Running
                        | InvestigationRunStatus::Paused
                        | InvestigationRunStatus::NeedsEvidence
                ) || !refinement.scope.narrows(&next.effective_scope)
                    || refinement
                        .packs
                        .iter()
                        .any(|pack| !next.effective_packs.contains(pack) || !context.allowed_packs.contains(pack))
                    || refinement.evidence_ids.len() > next.budget.limit.evidence_items as usize
                {
                    return Err(InvestigationRunFailure::ScopeMismatch);
                }
                next.check_live(at)?;
                next.next_epoch()?;
                next.effective_scope = refinement.scope.clone();
                next.effective_packs = refinement.packs.clone();
                for id in &refinement.evidence_ids {
                    if !next.supplied_evidence_ids.contains(id)
                        && !next
                            .steps
                            .iter()
                            .flat_map(|step| &step.attempts)
                            .filter_map(|attempt| attempt.result.as_ref())
                            .any(|result| result.evidence_ids.contains(id))
                    {
                        next.supplied_evidence_ids.push(*id);
                    }
                }
                // Old observations remain in attempts; only explicitly revalidated references stay active.
                next.evidence_ids = refinement.evidence_ids.clone();
                if next.status == InvestigationRunStatus::NeedsEvidence {
                    next.status = InvestigationRunStatus::Paused;
                }
            }
        }
        next.bump(at)?;
        next.validate()?;
        let receipt = InvestigationCommandReceipt {
            schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
            command_id: request.command_id,
            tenant_id: next.initial_scope.tenant_id,
            actor_subject: context.actor_subject.clone(),
            run_id: next.id,
            idempotency_key: request.idempotency_key.clone(),
            request_digest: command_digest(request)?,
            applied_revision: next.revision,
            control_epoch: next.control_epoch,
            status: next.status,
            recorded_at: at,
        };
        *self = next;
        Ok(receipt)
    }
}

fn command_digest(request: &InvestigationCommandRequest) -> Result<String, InvestigationRunFailure> {
    let mut semantic = request.clone();
    if let InvestigationCommand::Steer { refinement } = &mut semantic.command {
        refinement.evidence_ids.sort();
    }
    canonical_sha256(&semantic).map_err(|_| InvestigationRunFailure::InvalidInput)
}
