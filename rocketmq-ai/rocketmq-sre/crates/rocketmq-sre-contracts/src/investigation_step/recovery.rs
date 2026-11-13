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

use chrono::{DateTime, Utc};

use crate::{
    InvestigationInterruptAttemptRequest, InvestigationInterruptionAcceptance, InvestigationInterruptionReason,
    InvestigationRevision, InvestigationRun, InvestigationRunContext, InvestigationRunFailure, InvestigationRunStatus,
    InvestigationStepId, InvestigationStepInterruption, StepAttemptId,
};

impl InvestigationRun {
    /// Records unknown work after the service has fenced out an expired worker.
    ///
    /// No result or measured cost is fabricated. Its reservation remains charged
    /// conservatively, including bytes and unknown Evidence. A later explicit
    /// retry needs fresh quota. Recovery pauses running admission and never starts
    /// queued work. The service must establish lease ownership transactionally.
    ///
    /// # Errors
    /// Rejects lost authority, stale new decisions, known results, invalid times
    /// and identities. A previously interrupted attempt is a read-only replay.
    pub fn interrupt_attempt(
        &mut self,
        context: &InvestigationRunContext,
        expected_revision: InvestigationRevision,
        step_id: InvestigationStepId,
        attempt_id: StepAttemptId,
        at: DateTime<Utc>,
    ) -> Result<InvestigationInterruptionAcceptance, InvestigationRunFailure> {
        self.interrupt_with_reason(
            context,
            InvestigationInterruptAttemptRequest {
                expected_revision,
                step_id,
                attempt_id,
                reason: InvestigationInterruptionReason::WorkerLeaseExpired,
            },
            at,
        )
    }

    /// Records an explicitly unknown source/owner outcome without claiming a
    /// final RPC result or refunding any reservation. Cancelling still drains to
    /// cancelled; unknown running work stops at needs-evidence after draining.
    ///
    /// # Errors
    /// Rejects wrong attempt/authority, conflicting reasons and stale decisions.
    pub fn interrupt_with_reason(
        &mut self,
        context: &InvestigationRunContext,
        request: InvestigationInterruptAttemptRequest,
        at: DateTime<Utc>,
    ) -> Result<InvestigationInterruptionAcceptance, InvestigationRunFailure> {
        context.authorize(&self.initial_scope)?;
        self.validate()?;
        let step_index = self
            .steps
            .iter()
            .position(|step| step.id == request.step_id)
            .ok_or(InvestigationRunFailure::StepNotFound)?;
        let attempt_index = self.steps[step_index]
            .attempts
            .iter()
            .position(|attempt| attempt.id == request.attempt_id)
            .ok_or(InvestigationRunFailure::AttemptMismatch)?;
        let attempt = &self.steps[step_index].attempts[attempt_index];
        if let Some(previous) = &attempt.interruption {
            return if previous.reason == request.reason {
                Ok(InvestigationInterruptionAcceptance::Replay)
            } else {
                Err(InvestigationRunFailure::ResultConflict)
            };
        }
        self.expect_revision(request.expected_revision)?;
        if attempt.result.is_some() || at < attempt.admitted_at || self.status.is_terminal() {
            return Err(InvestigationRunFailure::ResultConflict);
        }
        let mut next = self.clone();
        next.bump(at)?;
        next.steps[step_index].attempts[attempt_index].interruption = Some(InvestigationStepInterruption {
            reason: request.reason,
            recovered_at: at,
        });
        if next.status == InvestigationRunStatus::Running {
            next.status = InvestigationRunStatus::Pausing;
        }
        if self.status == InvestigationRunStatus::Running
            && matches!(
                request.reason,
                InvestigationInterruptionReason::SourceOutcomeUnknown
                    | InvestigationInterruptionReason::ModelOutcomeUnknown
                    | InvestigationInterruptionReason::OwnerShutdown
            )
        {
            next.stop_reason = Some(
                if request.reason == InvestigationInterruptionReason::ModelOutcomeUnknown {
                    crate::InvestigationStopReason::ModelUnavailable
                } else {
                    crate::InvestigationStopReason::SourceUnavailable
                },
            );
        }
        if next.in_flight_count() == 0 {
            next.status = match next.status {
                InvestigationRunStatus::Pausing
                    if matches!(
                        next.stop_reason,
                        Some(
                            crate::InvestigationStopReason::SourceUnavailable
                                | crate::InvestigationStopReason::ModelUnavailable
                        )
                    ) =>
                {
                    InvestigationRunStatus::NeedsEvidence
                }
                InvestigationRunStatus::Pausing => InvestigationRunStatus::Paused,
                InvestigationRunStatus::Cancelling => InvestigationRunStatus::Cancelled,
                status => status,
            };
        }
        next.validate()?;
        *self = next;
        Ok(InvestigationInterruptionAcceptance::Recorded)
    }
}
