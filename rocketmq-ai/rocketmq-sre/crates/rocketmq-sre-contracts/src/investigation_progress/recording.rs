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

use crate::{
    InvestigationRevision, InvestigationRun, InvestigationRunContext, InvestigationRunFailure, InvestigationRunStatus,
    InvestigationStepKind, InvestigationStepOutcome, InvestigationStopReason,
};

use super::*;

impl InvestigationRun {
    /// Records a projection of an actual saved revision after its evaluation
    /// result settles. Storage must insert the immutable revision, settlement,
    /// this snapshot and its event in one ownership-fenced transaction.
    ///
    /// # Errors
    /// Rejects false lineage, cross-scope/revision data and changed duplicates.
    pub fn record_pack_result(
        &mut self,
        context: &InvestigationRunContext,
        expected_revision: InvestigationRevision,
        revision: &SavedInvestigationRunRevision,
        at: DateTime<Utc>,
    ) -> Result<InvestigationProgressAcceptance, InvestigationRunFailure> {
        context.authorize(&self.initial_scope)?;
        self.validate()?;
        revision.validate()?;
        let result = revision.pack_result()?;
        if let Some(previous) = self
            .pack_results
            .iter()
            .find(|item| item.run_revision_id == revision.id || item.attempt_id == revision.attempt_id)
        {
            return if previous == &result {
                Ok(InvestigationProgressAcceptance::Replay)
            } else {
                Err(InvestigationRunFailure::ResultConflict)
            };
        }
        self.expect_revision(expected_revision)?;
        if revision.run_id != self.id
            || !revision.scope.narrows(&self.initial_scope)
            || !self.initial_packs.contains(&revision.pack)
            || revision.control_epoch > self.control_epoch
            || revision.created_at < self.created_at
            || revision.created_at > at
            || self.pack_results.len() >= 64
        {
            return Err(InvestigationRunFailure::ScopeMismatch);
        }
        let step = self
            .steps
            .iter()
            .find(|step| step.id == revision.step_id)
            .ok_or(InvestigationRunFailure::StepNotFound)?;
        let attempt = step
            .attempts
            .iter()
            .find(|attempt| attempt.id == revision.attempt_id)
            .ok_or(InvestigationRunFailure::AttemptMismatch)?;
        let observation = attempt.result.as_ref().ok_or(InvestigationRunFailure::StepInFlight)?;
        let InvestigationStepKind::EvaluateRules { evidence_ids } = &step.spec.work else {
            return Err(InvestigationRunFailure::InvalidAssessment);
        };
        if step.spec.scope != revision.scope
            || step.spec.pack != revision.pack
            || attempt.control_epoch != revision.control_epoch
            || observation.outcome != InvestigationStepOutcome::Succeeded
            || observation.finished_at > revision.created_at
            || revision
                .evidence_ids
                .iter()
                .any(|id| !evidence_ids.contains(id) || !observation.evidence_ids.contains(id))
        {
            return Err(InvestigationRunFailure::InvalidAssessment);
        }
        context.evidence_visible(&revision.evidence_ids, &revision.scope)?;
        let mut next = self.clone();
        next.bump(at)?;
        next.pack_results.push(result);
        next.validate()?;
        *self = next;
        Ok(InvestigationProgressAcceptance::Recorded)
    }

    /// Stops without manufacturing a saved revision when no evaluation could
    /// be admitted. Existing reservations and observed references are retained.
    ///
    /// # Errors
    /// Rejects stale decisions, draining work and unsupported terminal rewrites.
    pub fn stop_with_reason(
        &mut self,
        context: &InvestigationRunContext,
        expected_revision: InvestigationRevision,
        reason: InvestigationStopReason,
        at: DateTime<Utc>,
    ) -> Result<(), InvestigationRunFailure> {
        context.authorize(&self.initial_scope)?;
        self.validate()?;
        self.expect_revision(expected_revision)?;
        if !matches!(
            self.status,
            InvestigationRunStatus::Queued | InvestigationRunStatus::Running | InvestigationRunStatus::NeedsEvidence
        ) || self.in_flight_count() != 0
        {
            return Err(InvestigationRunFailure::InvalidStateTransition);
        }
        let mut next = self.clone();
        next.bump(at)?;
        next.status = InvestigationRunStatus::NeedsEvidence;
        next.stop_reason = Some(reason);
        next.validate()?;
        *self = next;
        Ok(())
    }
}
