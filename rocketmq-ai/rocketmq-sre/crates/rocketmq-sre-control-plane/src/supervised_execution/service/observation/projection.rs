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

use rocketmq_sre_contracts::{
    EXECUTION_OBSERVATION_SCHEMA_VERSION, ExecutionId, ExecutionObservationEvent,
    ExecutionObservationEventKind as EventKind, ExecutionObservationFact as Fact, ExecutionObservationReason as Reason,
    ExecutionObservationSource as Source, ExecutionObservationState as State, ExecutionObservationView,
    ExecutionStepObservation, MAX_EXECUTION_OBSERVATION_ATTEMPTS, MAX_EXECUTION_OBSERVATION_STEPS,
};

use super::{SupervisedExecutionService, receipt_reference, snapshot_deadline};
use crate::supervised_execution::{catalog::CatalogResolution, repository::EventRecord};
use crate::{ControlPlaneRequestFailure, auth::AuthContext};

mod attempts;
mod verification;

impl SupervisedExecutionService {
    pub(crate) async fn execution_observation(
        &self,
        auth: &AuthContext,
        id: ExecutionId,
    ) -> Result<ExecutionObservationView, ControlPlaneRequestFailure> {
        let journal = self.repository.execution_observation_journal(auth, id).await?;
        let binding = receipt_reference(auth, &journal.execution)?;
        let plan = &journal.execution.request.plan;
        let mut partial = journal.partial
            || plan.steps.len() > MAX_EXECUTION_OBSERVATION_STEPS
            || journal.attempts.iter().any(|row| {
                row.kind == "result"
                    && !journal.attempts.iter().any(|intent| {
                        intent.kind == "intent" && intent.step_id == row.step_id && intent.attempt == row.attempt
                    })
            });
        let mut steps = Vec::new();
        let mut attempt_count = 0;
        for step in plan.steps.iter().take(MAX_EXECUTION_OBSERVATION_STEPS) {
            let declaration = match self.catalog.resolve(step.action.id()) {
                Ok(CatalogResolution::Supervised(_, descriptor))
                    if descriptor.version == step.descriptor_version
                        && descriptor.verification == step.verification =>
                {
                    Some(step.verification.clone())
                }
                _ => None,
            };
            let (mut attempts, damaged) = attempts::project(&journal, step);
            let remaining = MAX_EXECUTION_OBSERVATION_ATTEMPTS.saturating_sub(attempt_count);
            let truncated = attempts.len() > remaining;
            attempts.truncate(remaining);
            attempt_count += attempts.len();
            let (verifications, proof_partial) = verification::project(&journal, step, declaration.as_ref());
            let effect = attempts::effect_fact(&attempts, false);
            let compensation = attempts::effect_fact(&attempts, true);
            let step_partial = damaged || truncated || proof_partial;
            partial |= step_partial;
            steps.push(ExecutionStepObservation {
                plan_step_id: step.id,
                sequence: step.sequence,
                action: step.action,
                descriptor_version: step.descriptor_version.clone(),
                target: step.resource.clone(),
                declared_verification: declaration,
                attempts,
                verifications,
                effect,
                compensation,
                partial: step_partial,
            });
        }
        let timeline = journal.events.iter().map(event).collect();
        let unknown_effect = partial || steps.iter().any(|step| matches!(step.effect.state, State::Unknown));
        let reconciled = steps
            .iter()
            .flat_map(|step| &step.attempts)
            .rev()
            .find(|attempt| attempt.fact.source == Source::ReconciliationJournal)
            .map(|attempt| attempt.fact.clone());
        let has_result = steps.iter().any(|step| {
            step.attempts.iter().any(|attempt| attempt.result_at.is_some()) || !step.verifications.is_empty()
        });
        let manual_recorded = journal.quarantine
            || journal
                .events
                .iter()
                .any(|event| event.kind == "manual_takeover_required");
        let observed_at = self.now();
        Ok(ExecutionObservationView {
            schema_version: EXECUTION_OBSERVATION_SCHEMA_VERSION.to_owned(),
            binding,
            observed_at,
            valid_until: snapshot_deadline(observed_at)?,
            steps,
            timeline,
            timeline_partial: journal.events.len() >= 256 && journal.partial,
            history: fact(
                if partial {
                    State::Unknown
                } else if has_result {
                    State::Observed
                } else {
                    State::NotObserved
                },
                Source::StepJournal,
                if partial {
                    Reason::PartialHistory
                } else if has_result {
                    Reason::ResultRecorded
                } else {
                    Reason::NoEffectResult
                },
            ),
            reconciliation: if unknown_effect {
                fact(
                    State::Unknown,
                    Source::ReconciliationJournal,
                    Reason::ReconciliationRequired,
                )
            } else {
                reconciled.unwrap_or_else(|| {
                    fact(
                        State::NotObserved,
                        Source::ReconciliationJournal,
                        Reason::NoEffectResult,
                    )
                })
            },
            manual_takeover: fact(
                if manual_recorded {
                    State::Observed
                } else {
                    State::NotObserved
                },
                if journal.quarantine {
                    Source::QuarantineJournal
                } else {
                    Source::StepJournal
                },
                if manual_recorded {
                    Reason::ManualTakeoverRequired
                } else {
                    Reason::ManualTakeoverNotRecorded
                },
            ),
            business_recovery: fact(
                State::Unknown,
                Source::NotObserved,
                Reason::IndependentBusinessObservationMissing,
            ),
        })
    }
}

pub(super) fn fact(state: State, source: Source, reason: Reason) -> Fact {
    Fact {
        state,
        source,
        reason_codes: vec![reason],
    }
}

fn event(row: &EventRecord) -> ExecutionObservationEvent {
    let (kind, reason) = match row.kind.as_str() {
        "execution_submitted" => (EventKind::Accepted, Reason::AcceptedReceipt),
        "state_changed" => (EventKind::StateChanged, Reason::RecordedStateChange),
        "step_intent_persisted" => (EventKind::IntentPersisted, Reason::IntentRecorded),
        "step_result_persisted" => (EventKind::ResultRecorded, Reason::ResultRecorded),
        "verification_captured" => (EventKind::VerificationCaptured, Reason::RecordedVerification),
        "verification_completed" => (EventKind::VerificationCompleted, Reason::RecordedVerification),
        "manual_takeover_required" => (EventKind::ManualTakeoverRequired, Reason::ManualTakeoverRequired),
        "quarantine_created" => (EventKind::QuarantineCreated, Reason::ManualTakeoverRequired),
        "quarantine_cleared" => (EventKind::QuarantineCleared, Reason::RecordedStateChange),
        _ => (EventKind::Other, Reason::UnrecognizedRecordedEvent),
    };
    let step_id = if matches!(
        row.resource_kind.as_str(),
        "step_intent" | "step_result" | "verification"
    ) {
        row.resource_id
            .parse::<uuid::Uuid>()
            .ok()
            .map(rocketmq_sre_contracts::ExecutionStepId::from_uuid)
    } else {
        None
    };
    let state = row
        .state
        .as_ref()
        .and_then(|value| serde_json::from_value(serde_json::Value::String(value.clone())).ok());
    ExecutionObservationEvent {
        sequence: row.sequence.to_string(),
        kind,
        occurred_at: row.occurred_at,
        step_id,
        state,
        reason,
    }
}
