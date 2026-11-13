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
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    ClusterId, HypothesisAssessmentId, InvestigationCommandId, InvestigationControlEpoch, InvestigationEventSequence,
    InvestigationId, InvestigationInterruptionReason, InvestigationResultDisposition, InvestigationRevision,
    InvestigationRun, InvestigationRunFailure, InvestigationRunId, InvestigationRunStatus, InvestigationStepId,
    StepAttemptId, TenantId,
};

pub const INVESTIGATION_RUN_EVENT_SCHEMA_VERSION: &str = "rocketmq-sre.investigation-run-event.v1";
pub const INVESTIGATION_RUN_EVENT_PAGE_SCHEMA_VERSION: &str = "rocketmq-sre.investigation-run-event-page.v1";

/// Event payloads carry bounded identities, never source bodies or worker lease tokens.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InvestigationRunEventKind {
    Created {},
    CommandAccepted {
        command_id: InvestigationCommandId,
    },
    WorkerClaimed {},
    WorkerReleased {},
    Started {},
    StepAdmitted {
        step_id: InvestigationStepId,
        attempt_id: StepAttemptId,
    },
    StepSettled {
        step_id: InvestigationStepId,
        attempt_id: StepAttemptId,
        disposition: InvestigationResultDisposition,
    },
    AttemptInterrupted {
        step_id: InvestigationStepId,
        attempt_id: StepAttemptId,
        reason: InvestigationInterruptionReason,
    },
    AssessmentRecorded {
        assessment_id: HypothesisAssessmentId,
    },
    Stopped {},
}

/// Durable per-run sequence is independent of the run's control revision.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationRunEvent {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub investigation_id: InvestigationId,
    pub run_id: InvestigationRunId,
    pub sequence: InvestigationEventSequence,
    pub revision: InvestigationRevision,
    pub control_epoch: InvestigationControlEpoch,
    pub status: InvestigationRunStatus,
    pub actor_subject: String,
    pub event: InvestigationRunEventKind,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvestigationCursorRecovery {
    CursorExpired,
    CursorAhead,
    RetainedEventGap,
}

/// A reset includes the authorized latest snapshot and resumes after its head.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationRunEventPage {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub investigation_id: InvestigationId,
    pub run_id: InvestigationRunId,
    pub events: Vec<InvestigationRunEvent>,
    pub earliest_sequence: InvestigationEventSequence,
    pub latest_sequence: InvestigationEventSequence,
    pub next_sequence: InvestigationEventSequence,
    pub has_more: bool,
    pub snapshot_required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_reason: Option<InvestigationCursorRecovery>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_snapshot: Option<InvestigationRun>,
}

impl InvestigationRunEventPage {
    /// Checks bounded, contiguous event pages and snapshot reset scope.
    ///
    /// # Errors
    /// Rejects mixed scopes, cursor inconsistencies, gaps and unbound snapshots.
    /// Authorization must still precede repository access at the service boundary.
    pub fn validate(&self) -> Result<(), InvestigationRunFailure> {
        if self.schema_version != INVESTIGATION_RUN_EVENT_PAGE_SCHEMA_VERSION
            || self.tenant_id.as_uuid().is_nil()
            || self.cluster_id.as_uuid().is_nil()
            || self.investigation_id.as_uuid().is_nil()
            || self.run_id.as_uuid().is_nil()
            || self.events.len() > 100
            || self.next_sequence > self.latest_sequence
            || self.earliest_sequence.get() == 0
            || (self.earliest_sequence > self.latest_sequence
                && self.earliest_sequence.get().checked_sub(1) != Some(self.latest_sequence.get()))
        {
            return Err(InvestigationRunFailure::InvalidInput);
        }
        if self.snapshot_required {
            let snapshot = self
                .current_snapshot
                .as_ref()
                .ok_or(InvestigationRunFailure::InvalidInput)?;
            snapshot.validate()?;
            if self.recovery_reason.is_none()
                || !self.events.is_empty()
                || self.has_more
                || self.next_sequence != self.latest_sequence
                || snapshot.id != self.run_id
                || snapshot.initial_scope.tenant_id != self.tenant_id
                || snapshot.initial_scope.cluster_id != self.cluster_id
                || snapshot.initial_scope.investigation_id != self.investigation_id
            {
                return Err(InvestigationRunFailure::ScopeMismatch);
            }
            return Ok(());
        }
        if self.current_snapshot.is_some() || self.recovery_reason.is_some() {
            return Err(InvestigationRunFailure::InvalidInput);
        }
        let mut previous: Option<InvestigationEventSequence> = None;
        for event in &self.events {
            if event.schema_version != INVESTIGATION_RUN_EVENT_SCHEMA_VERSION
                || event.tenant_id != self.tenant_id
                || event.cluster_id != self.cluster_id
                || event.investigation_id != self.investigation_id
                || event.run_id != self.run_id
                || event.sequence < self.earliest_sequence
                || event.sequence > self.latest_sequence
                || !crate::investigation_run_error::valid_run_text(&event.actor_subject, 255)
            {
                return Err(InvestigationRunFailure::ScopeMismatch);
            }
            if let Some(previous) = previous
                && previous.next()? != event.sequence
            {
                return Err(InvestigationRunFailure::InvalidInput);
            }
            previous = Some(event.sequence);
        }
        if let Some(last) = previous
            && last != self.next_sequence
        {
            return Err(InvestigationRunFailure::InvalidInput);
        }
        if self.has_more != (self.next_sequence < self.latest_sequence) {
            return Err(InvestigationRunFailure::InvalidInput);
        }
        Ok(())
    }
}
