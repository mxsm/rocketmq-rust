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

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::investigation_run_error::{unique_run_ids, valid_run_key};
use crate::{
    EvidenceId, INVESTIGATION_RUN_SCHEMA_VERSION, InvestigationBudgetCost, InvestigationControlEpoch,
    InvestigationPackRef, InvestigationRequirementRef, InvestigationRevision, InvestigationRun,
    InvestigationRunContext, InvestigationRunFailure, InvestigationRunId, InvestigationRunScope,
    InvestigationRunStatus, InvestigationStepId, StepAttemptId, canonical_sha256,
};

mod recovery;

/// Closed read-only work: collectors are resolved from a trusted requirement catalog.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InvestigationStepKind {
    CollectEvidence { requirement_id: String },
    EvaluateRules { evidence_ids: Vec<EvidenceId> },
    ExplainEvidence { evidence_ids: Vec<EvidenceId> },
}

#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationStepSpec {
    pub scope: InvestigationRunScope,
    pub pack: InvestigationPackRef,
    pub work: InvestigationStepKind,
}

impl InvestigationStepSpec {
    pub(crate) fn validate_result_references(&self, ids: &[EvidenceId]) -> Result<(), InvestigationRunFailure> {
        match &self.work {
            InvestigationStepKind::EvaluateRules { evidence_ids }
            | InvestigationStepKind::ExplainEvidence { evidence_ids }
                if ids.iter().any(|id| !evidence_ids.contains(id)) =>
            {
                Err(InvestigationRunFailure::ScopeMismatch)
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn validate_reservation(
        &self,
        reservation: InvestigationBudgetCost,
    ) -> Result<(), InvestigationRunFailure> {
        let matches = match &self.work {
            InvestigationStepKind::CollectEvidence { .. } => {
                reservation.read_queries == 1 && reservation.model_calls == 0 && reservation.evidence_items > 0
            }
            InvestigationStepKind::EvaluateRules { .. } => {
                reservation.read_queries == 0 && reservation.model_calls == 0 && reservation.evidence_items == 0
            }
            InvestigationStepKind::ExplainEvidence { .. } => {
                reservation.read_queries == 0 && reservation.model_calls == 1 && reservation.evidence_items == 0
            }
        };
        if !matches || reservation.output_bytes == 0 {
            return Err(InvestigationRunFailure::InvalidInput);
        }
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<(), InvestigationRunFailure> {
        self.scope.validate()?;
        self.pack.validate()?;
        match &self.work {
            InvestigationStepKind::CollectEvidence { requirement_id } => {
                if !valid_run_key(requirement_id) {
                    return Err(InvestigationRunFailure::InvalidInput);
                }
            }
            InvestigationStepKind::EvaluateRules { evidence_ids }
            | InvestigationStepKind::ExplainEvidence { evidence_ids } => {
                if !unique_run_ids(evidence_ids, 256) || evidence_ids.iter().any(|id| id.as_uuid().is_nil()) {
                    return Err(InvestigationRunFailure::InvalidInput);
                }
            }
        }
        Ok(())
    }

    /// Stable work identity excludes attempt IDs, reservation sizes and control epochs.
    ///
    /// # Errors
    /// Returns a sanitized failure for malformed or unencodable work.
    pub fn semantic_key(&self) -> Result<String, InvestigationRunFailure> {
        self.validate()?;
        let mut spec = self.clone();
        match &mut spec.work {
            InvestigationStepKind::EvaluateRules { evidence_ids }
            | InvestigationStepKind::ExplainEvidence { evidence_ids } => evidence_ids.sort(),
            InvestigationStepKind::CollectEvidence { .. } => {}
        }
        canonical_sha256(&spec).map_err(|_| InvestigationRunFailure::InvalidInput)
    }
}

/// Server-side admission input, not an unrestricted browser query operation.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationStepAdmission {
    pub step_id: InvestigationStepId,
    pub attempt_id: StepAttemptId,
    pub expected_revision: InvestigationRevision,
    pub spec: InvestigationStepSpec,
    pub reservation: InvestigationBudgetCost,
}

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvestigationStepOutcome {
    Succeeded,
    Failed,
    /// Worker settlement with final usage; it does not assert that a remote RPC was withdrawn.
    CancelledAcknowledged,
}

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvestigationStepFailure {
    SourceUnavailable,
    EvidenceInsufficient,
    ModelUnavailable,
    DeadlineExceeded,
}

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvestigationResultDisposition {
    Applied,
    Superseded,
}

/// Final observation and actual usage for exactly one admitted attempt.
///
/// The worker must join/cancel its owned read and know final consumption before
/// acknowledging cancellation. No raw result, credentials or message body is carried.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationStepResult {
    pub schema_version: String,
    pub run_id: InvestigationRunId,
    pub step_id: InvestigationStepId,
    pub attempt_id: StepAttemptId,
    pub control_epoch: InvestigationControlEpoch,
    pub outcome: InvestigationStepOutcome,
    pub failure: Option<InvestigationStepFailure>,
    pub evidence_ids: Vec<EvidenceId>,
    pub actual_cost: InvestigationBudgetCost,
    pub finished_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationStepAttempt {
    pub id: StepAttemptId,
    pub control_epoch: InvestigationControlEpoch,
    pub admitted_revision: InvestigationRevision,
    pub admitted_at: DateTime<Utc>,
    pub reservation: InvestigationBudgetCost,
    pub result: Option<InvestigationStepResult>,
    pub disposition: Option<InvestigationResultDisposition>,
    /// Unknown work retains its reservation without inventing a measured result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interruption: Option<InvestigationStepInterruption>,
}

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvestigationInterruptionReason {
    WorkerLeaseExpired,
}

#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationStepInterruption {
    pub reason: InvestigationInterruptionReason,
    pub recovered_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvestigationInterruptionAcceptance {
    Recorded,
    Replay,
}

/// Logical work identity remains stable across bounded retries and restarts.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationStep {
    pub id: InvestigationStepId,
    pub spec: InvestigationStepSpec,
    pub attempts: Vec<InvestigationStepAttempt>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvestigationResultAcceptance {
    Applied,
    Superseded,
    Replay,
}

impl InvestigationRun {
    /// Atomically admits one bounded attempt in the in-memory contract model.
    ///
    /// # Errors
    /// Rejects paused/cancelled work, scope widening, finished work, duplicate
    /// identities, exhausted shared reservations, deadlines, and stale revisions.
    /// D14 must run this decision and its persisted write in one transaction.
    pub fn admit_step(
        &mut self,
        context: &InvestigationRunContext,
        request: &InvestigationStepAdmission,
        at: DateTime<Utc>,
    ) -> Result<(), InvestigationRunFailure> {
        context.authorize(&self.initial_scope)?;
        self.validate()?;
        self.expect_revision(request.expected_revision)?;
        self.check_live(at)?;
        request.spec.validate()?;
        request.spec.validate_reservation(request.reservation)?;
        if self.status != InvestigationRunStatus::Running {
            return Err(InvestigationRunFailure::InvalidStateTransition);
        }
        if request.step_id.as_uuid().is_nil()
            || request.attempt_id.as_uuid().is_nil()
            || !request.spec.scope.narrows(&self.effective_scope)
        {
            return Err(InvestigationRunFailure::ScopeMismatch);
        }
        if !self.effective_packs.contains(&request.spec.pack) || !context.allowed_packs.contains(&request.spec.pack) {
            return Err(InvestigationRunFailure::PackNotAllowed);
        }
        match &request.spec.work {
            InvestigationStepKind::CollectEvidence { requirement_id } => {
                let requirement = InvestigationRequirementRef {
                    pack: request.spec.pack.clone(),
                    requirement_id: requirement_id.clone(),
                };
                if !context.allowed_requirements.contains(&requirement)
                    || request.reservation.read_queries != 1
                    || request.reservation.model_calls != 0
                    || request.reservation.evidence_items == 0
                {
                    return Err(InvestigationRunFailure::PackNotAllowed);
                }
            }
            InvestigationStepKind::EvaluateRules { evidence_ids }
            | InvestigationStepKind::ExplainEvidence { evidence_ids } => {
                context.evidence_visible(evidence_ids, &request.spec.scope)?;
                if evidence_ids.iter().any(|id| !self.evidence_ids.contains(id)) {
                    return Err(InvestigationRunFailure::ScopeMismatch);
                }
                let model_calls = u64::from(matches!(
                    request.spec.work,
                    InvestigationStepKind::ExplainEvidence { .. }
                ));
                if request.reservation.read_queries != 0
                    || request.reservation.model_calls != model_calls
                    || request.reservation.evidence_items != 0
                {
                    return Err(InvestigationRunFailure::InvalidInput);
                }
            }
        }
        if request.reservation.output_bytes == 0 {
            return Err(InvestigationRunFailure::InvalidInput);
        }
        let key = request.spec.semantic_key()?;
        let existing = self.steps.iter().position(|step| step.id == request.step_id);
        if self
            .steps
            .iter()
            .any(|step| step.id != request.step_id && step.spec.semantic_key().is_ok_and(|item| item == key))
        {
            return Err(InvestigationRunFailure::StepIdentityConflict);
        }
        if let Some(index) = existing {
            let step = &self.steps[index];
            if step.spec.semantic_key()? != key {
                return Err(InvestigationRunFailure::StepIdentityConflict);
            }
            if step.attempts.iter().any(|attempt| {
                attempt
                    .result
                    .as_ref()
                    .is_some_and(|result| result.outcome == InvestigationStepOutcome::Succeeded)
            }) {
                return Err(InvestigationRunFailure::StepAlreadyCompleted);
            }
            if step
                .attempts
                .iter()
                .any(|attempt| attempt.result.is_none() && attempt.interruption.is_none())
            {
                return Err(InvestigationRunFailure::StepInFlight);
            }
            if step.attempts.len() >= usize::from(self.budget.max_attempts_per_step) {
                return Err(InvestigationRunFailure::BudgetExceeded);
            }
            if matches!(step.spec.work, InvestigationStepKind::CollectEvidence { .. })
                && self.query_retry_count()? >= u64::from(self.budget.max_query_retries)
            {
                return Err(InvestigationRunFailure::BudgetExceeded);
            }
        }
        let attempts: Vec<_> = self.steps.iter().flat_map(|step| &step.attempts).collect();
        if attempts.iter().any(|attempt| attempt.id == request.attempt_id) {
            return Err(InvestigationRunFailure::AttemptMismatch);
        }
        if attempts.len() >= self.budget.max_steps as usize
            || self.in_flight_count() >= usize::from(self.budget.max_in_flight)
        {
            return Err(InvestigationRunFailure::BudgetExceeded);
        }
        let (used, reserved) = self.budget_usage()?;
        if !used
            .add(reserved)?
            .add(request.reservation)?
            .fits_within(self.budget.limit)
        {
            return Err(InvestigationRunFailure::BudgetExceeded);
        }
        let mut next = self.clone();
        next.bump(at)?;
        let attempt = InvestigationStepAttempt {
            id: request.attempt_id,
            control_epoch: next.control_epoch,
            admitted_revision: next.revision,
            admitted_at: at,
            reservation: request.reservation,
            result: None,
            disposition: None,
            interruption: None,
        };
        if let Some(index) = existing {
            next.steps[index].attempts.push(attempt);
        } else {
            next.steps.push(InvestigationStep {
                id: request.step_id,
                spec: request.spec.clone(),
                attempts: vec![attempt],
            });
        }
        next.validate()?;
        *self = next;
        Ok(())
    }

    /// Settles an attempt even after pause, cancellation, deadline or a control-epoch change.
    ///
    /// # Errors
    /// Rejects wrong attempt/scope, inconsistent final usage, and conflicting
    /// duplicates. Exact duplicates are read-only replays and never charge twice.
    /// Current ordinary revisions do not invalidate independently admitted work.
    pub fn apply_step_result(
        &mut self,
        context: &InvestigationRunContext,
        result: &InvestigationStepResult,
        at: DateTime<Utc>,
    ) -> Result<InvestigationResultAcceptance, InvestigationRunFailure> {
        context.authorize(&self.initial_scope)?;
        self.validate()?;
        if result.schema_version != INVESTIGATION_RUN_SCHEMA_VERSION
            || result.run_id != self.id
            || !unique_run_ids(&result.evidence_ids, 256)
            || (result.outcome == InvestigationStepOutcome::Failed) != result.failure.is_some()
        {
            return Err(InvestigationRunFailure::InvalidInput);
        }
        let step_index = self
            .steps
            .iter()
            .position(|step| step.id == result.step_id)
            .ok_or(InvestigationRunFailure::StepNotFound)?;
        let step = &self.steps[step_index];
        let attempt_index = step
            .attempts
            .iter()
            .position(|attempt| attempt.id == result.attempt_id)
            .ok_or(InvestigationRunFailure::AttemptMismatch)?;
        let attempt = &step.attempts[attempt_index];
        if attempt.interruption.is_some() {
            return Err(InvestigationRunFailure::ResultConflict);
        }
        step.spec.validate_result_references(&result.evidence_ids)?;
        if result.control_epoch != attempt.control_epoch {
            return Err(InvestigationRunFailure::AttemptMismatch);
        }
        if let Some(previous) = &attempt.result {
            let mut previous = previous.clone();
            let mut incoming = result.clone();
            previous.evidence_ids.sort();
            incoming.evidence_ids.sort();
            return if previous == incoming {
                Ok(InvestigationResultAcceptance::Replay)
            } else {
                Err(InvestigationRunFailure::ResultConflict)
            };
        }
        context.evidence_visible(&result.evidence_ids, &step.spec.scope)?;
        let observed: BTreeSet<_> = self
            .supplied_evidence_ids
            .iter()
            .copied()
            .chain(
                self.steps
                    .iter()
                    .flat_map(|step| &step.attempts)
                    .filter_map(|attempt| attempt.result.as_ref())
                    .flat_map(|result| result.evidence_ids.iter().copied()),
            )
            .collect();
        let new_items = result.evidence_ids.iter().filter(|id| !observed.contains(id)).count() as u64;
        if result.actual_cost.evidence_items != new_items
            || !result.actual_cost.fits_within(attempt.reservation)
            || result.finished_at < attempt.admitted_at
            || result.finished_at > at
        {
            return Err(InvestigationRunFailure::InvalidInput);
        }
        let superseded = result.control_epoch != self.control_epoch
            || self.status.is_terminal()
            || self.status == InvestigationRunStatus::Cancelling;
        let disposition = if superseded {
            InvestigationResultDisposition::Superseded
        } else {
            InvestigationResultDisposition::Applied
        };
        let mut next = self.clone();
        next.bump(at)?;
        next.steps[step_index].attempts[attempt_index].result = Some(result.clone());
        next.steps[step_index].attempts[attempt_index].disposition = Some(disposition);
        if !superseded {
            for id in &result.evidence_ids {
                if !next.evidence_ids.contains(id) {
                    next.evidence_ids.push(*id);
                }
            }
        }
        if next.in_flight_count() == 0 {
            next.status = match next.status {
                InvestigationRunStatus::Pausing => InvestigationRunStatus::Paused,
                InvestigationRunStatus::Cancelling => InvestigationRunStatus::Cancelled,
                status => status,
            };
        }
        next.validate()?;
        *self = next;
        Ok(if superseded {
            InvestigationResultAcceptance::Superseded
        } else {
            InvestigationResultAcceptance::Applied
        })
    }
}
