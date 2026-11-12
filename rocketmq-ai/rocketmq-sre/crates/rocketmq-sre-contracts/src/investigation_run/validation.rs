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

use chrono::Duration;

use super::{InvestigationRun, validate_packs};
use crate::investigation_run_error::{unique_run_ids, valid_run_text};
use crate::{
    HypothesisAssessmentConclusion, INVESTIGATION_RUN_SCHEMA_VERSION, InvestigationResultDisposition,
    InvestigationRunFailure, InvestigationRunStatus, InvestigationStepOutcome,
};

impl InvestigationRun {
    /// Checks the stored model's identities, state, history and shared budget.
    ///
    /// # Errors
    /// Rejects inconsistent snapshots. This is structural validation, not server
    /// authentication, Evidence qualification, or a claim of persisted exactly-once effects.
    pub fn validate(&self) -> Result<(), InvestigationRunFailure> {
        self.initial_scope.validate()?;
        self.effective_scope.validate()?;
        validate_packs(&self.initial_packs)?;
        validate_packs(&self.effective_packs)?;
        self.budget.validate()?;
        if self.schema_version != INVESTIGATION_RUN_SCHEMA_VERSION
            || self.id.as_uuid().is_nil()
            || !self.effective_scope.narrows(&self.initial_scope)
            || self
                .effective_packs
                .iter()
                .any(|pack| !self.initial_packs.contains(pack))
            || !valid_run_text(&self.created_by_subject, 255)
            || self.updated_at < self.created_at
            || self.initial_scope.time_range.end > self.created_at
            || self.deadline <= self.created_at
            || self.deadline.signed_duration_since(self.created_at) > Duration::hours(1)
            || self.steps.len() > self.budget.max_steps as usize
            || self.assessments.len() > 16
            || !unique_run_ids(&self.evidence_ids, self.budget.limit.evidence_items as usize)
            || !unique_run_ids(&self.supplied_evidence_ids, self.budget.limit.evidence_items as usize)
            || self.supplied_evidence_ids.iter().any(|id| id.as_uuid().is_nil())
            || (self.status != InvestigationRunStatus::Queued && self.revision.get() == 0)
            || (self.status == InvestigationRunStatus::Completed && self.updated_at >= self.deadline)
        {
            return Err(InvestigationRunFailure::InvalidInput);
        }
        let mut step_ids = BTreeSet::new();
        let mut keys = BTreeSet::new();
        let mut attempts = BTreeSet::new();
        let mut observed = self.supplied_evidence_ids.iter().copied().collect::<BTreeSet<_>>();
        let mut applied = observed.clone();
        for step in &self.steps {
            step.spec.validate()?;
            if step.id.as_uuid().is_nil()
                || !step_ids.insert(step.id)
                || !keys.insert(step.spec.semantic_key()?)
                || !step.spec.scope.narrows(&self.initial_scope)
                || !self.initial_packs.contains(&step.spec.pack)
                || step.attempts.is_empty()
                || step.attempts.len() > usize::from(self.budget.max_attempts_per_step)
            {
                return Err(InvestigationRunFailure::InvalidInput);
            }
            let mut completed = false;
            let mut open_seen = false;
            let mut previous_admission = None;
            for attempt in &step.attempts {
                if completed
                    || open_seen
                    || attempt.id.as_uuid().is_nil()
                    || !attempts.insert(attempt.id)
                    || attempt.control_epoch > self.control_epoch
                    || attempt.admitted_revision > self.revision
                    || attempt.admitted_revision.get() == 0
                    || attempt.admitted_at < self.created_at
                    || attempt.admitted_at > self.updated_at
                    || attempt.reservation.output_bytes == 0
                    || !attempt.reservation.fits_within(self.budget.limit)
                    || previous_admission.is_some_and(|prior| attempt.admitted_revision <= prior)
                    || attempt.result.is_some() != attempt.disposition.is_some()
                {
                    return Err(InvestigationRunFailure::InvalidInput);
                }
                previous_admission = Some(attempt.admitted_revision);
                step.spec.validate_reservation(attempt.reservation)?;
                match &attempt.result {
                    Some(result) => {
                        step.spec.validate_result_references(&result.evidence_ids)?;
                        if result.schema_version != INVESTIGATION_RUN_SCHEMA_VERSION
                            || result.run_id != self.id
                            || result.step_id != step.id
                            || result.attempt_id != attempt.id
                            || result.control_epoch != attempt.control_epoch
                            || result.finished_at < attempt.admitted_at
                            || result.finished_at > self.updated_at
                            || !result.actual_cost.fits_within(attempt.reservation)
                            || !unique_run_ids(&result.evidence_ids, 256)
                            || result.evidence_ids.iter().any(|id| id.as_uuid().is_nil())
                            || (result.outcome == InvestigationStepOutcome::Failed) != result.failure.is_some()
                        {
                            return Err(InvestigationRunFailure::InvalidInput);
                        }
                        observed.extend(result.evidence_ids.iter().copied());
                        if attempt.disposition == Some(InvestigationResultDisposition::Applied) {
                            applied.extend(result.evidence_ids.iter().copied());
                        }
                        completed = result.outcome == InvestigationStepOutcome::Succeeded;
                    }
                    None => {
                        open_seen = true;
                        if self.status == InvestigationRunStatus::Cancelling
                            && attempt.control_epoch == self.control_epoch
                        {
                            return Err(InvestigationRunFailure::InvalidStateTransition);
                        }
                    }
                }
            }
        }
        if attempts.len() > self.budget.max_steps as usize
            || self.in_flight_count() > usize::from(self.budget.max_in_flight)
            || self
                .evidence_ids
                .iter()
                .any(|id| id.as_uuid().is_nil() || !applied.contains(id))
        {
            return Err(InvestigationRunFailure::InvalidInput);
        }
        let (used, reserved) = self.budget_usage()?;
        if self.query_retry_count()? > u64::from(self.budget.max_query_retries) {
            return Err(InvestigationRunFailure::BudgetExceeded);
        }
        if !used.add(reserved)?.fits_within(self.budget.limit) || used.evidence_items != observed.len() as u64 {
            return Err(InvestigationRunFailure::BudgetExceeded);
        }
        let mut assessment_ids = BTreeSet::new();
        for assessment in &self.assessments {
            assessment.validate_shape()?;
            if !assessment_ids.insert(assessment.id)
                || assessment.run_id != self.id
                || !assessment.scope.narrows(&self.initial_scope)
                || assessment.control_epoch > self.control_epoch
                || assessment.created_at < self.created_at
                || assessment.created_at > self.updated_at
                || assessment.references().iter().any(|id| !observed.contains(id))
            {
                return Err(InvestigationRunFailure::InvalidAssessment);
            }
        }
        let in_flight = self.in_flight_count();
        match self.status {
            InvestigationRunStatus::Queued
                if !self.steps.is_empty()
                    || !self.assessments.is_empty()
                    || !self.evidence_ids.is_empty()
                    || !self.supplied_evidence_ids.is_empty()
                    || self.effective_scope != self.initial_scope
                    || self.effective_packs != self.initial_packs
                    || self.revision.get() != 0
                    || self.control_epoch.get() != 0 =>
            {
                return Err(InvestigationRunFailure::InvalidStateTransition);
            }
            InvestigationRunStatus::Pausing | InvestigationRunStatus::Cancelling if in_flight == 0 => {
                return Err(InvestigationRunFailure::InvalidStateTransition);
            }
            InvestigationRunStatus::Paused
            | InvestigationRunStatus::NeedsEvidence
            | InvestigationRunStatus::Completed
            | InvestigationRunStatus::Inconclusive
            | InvestigationRunStatus::Cancelled
            | InvestigationRunStatus::Failed
                if in_flight != 0 =>
            {
                return Err(InvestigationRunFailure::InvalidStateTransition);
            }
            _ => {}
        }
        match self.status {
            InvestigationRunStatus::Completed | InvestigationRunStatus::Inconclusive => {
                let assessment = self
                    .assessments
                    .last()
                    .ok_or(InvestigationRunFailure::InvalidAssessment)?;
                if assessment.control_epoch != self.control_epoch
                    || assessment.scope != self.effective_scope
                    || assessment.references().iter().any(|id| !self.evidence_ids.contains(id))
                    || (self.status == InvestigationRunStatus::Inconclusive)
                        != matches!(
                            assessment.conclusion,
                            HypothesisAssessmentConclusion::Inconclusive { .. }
                        )
                {
                    return Err(InvestigationRunFailure::InvalidAssessment);
                }
            }
            _ => {}
        }
        Ok(())
    }
}
