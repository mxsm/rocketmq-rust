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

use super::fact;
use crate::supervised_execution::repository::ObservationJournal;
use rocketmq_sre_contracts::{
    EffectState, ExecutionAttemptObservation, ExecutionObservationFact, ExecutionObservationReason as Reason,
    ExecutionObservationSource as Source, ExecutionObservationState as State, PlanStep, StepIntent, StepResult,
};

pub(super) fn project(journal: &ObservationJournal, step: &PlanStep) -> (Vec<ExecutionAttemptObservation>, bool) {
    let mut output = Vec::new();
    let mut partial = false;
    for row in journal.attempts.iter().filter(|row| row.kind == "intent") {
        let Some(encoded) = &row.snapshot else {
            partial = true;
            continue;
        };
        let Ok(intent) = serde_json::from_value::<StepIntent>(encoded.clone()) else {
            partial = true;
            continue;
        };
        if intent.step.id != step.id {
            continue;
        }
        if intent.step != *step
            || intent.execution_id != journal.execution.request.id
            || intent.plan_hash != journal.execution.request.plan.plan_hash
            || intent.step_id != row.step_id
            || i32::from(intent.attempt) != row.attempt
            || intent.compensation != row.compensation
            || intent.intended_at.timestamp_micros() != row.occurred_at.timestamp_micros()
            || row.lease_id != Some(intent.fence_grant.lease_id.as_uuid())
            || !epoch_bound(row.lease_epoch, intent.fence_grant.epoch.0)
            || intent.fence_grant.epoch.0 == 0
            || intent.fence_grant.execution_id != intent.execution_id
            || intent.fence_grant.step_id != intent.step_id
            || intent.fence_grant.cluster_id != journal.execution.request.cluster_id
            || intent.fence_grant.plan_step_id != step.id
            || intent.fence_grant.action != step.action
            || intent.fence_grant.resource != step.resource
            || intent.fence_grant.compensation != intent.compensation
        {
            partial = true;
            continue;
        }
        let result_row = journal
            .attempts
            .iter()
            .find(|result| result.kind == "result" && result.step_id == row.step_id && result.attempt == row.attempt);
        let mut result_at = None;
        let mut effect_state = None;
        let mut observed = fact(State::Unknown, Source::StepJournal, Reason::EffectUnknown);
        if let Some(result_row) = result_row {
            let result = result_row
                .snapshot
                .as_ref()
                .and_then(|value| serde_json::from_value::<StepResult>(value.clone()).ok());
            if let Some(result) = result.filter(|result| {
                result.step_id == row.step_id
                    && result.completed_at >= intent.intended_at
                    && result.completed_at.timestamp_micros() == result_row.occurred_at.timestamp_micros()
            }) {
                result_at = Some(result.completed_at);
                match result.agent_result.filter(|agent| {
                    agent.execution_id == intent.execution_id
                        && agent.step_id == intent.step_id
                        && agent.completed_at <= result.completed_at
                        && agent.completed_at >= intent.intended_at
                }) {
                    Some(agent) => {
                        effect_state = Some(agent.state);
                        observed =
                            if result.reason_code == "reconciled_applied" && agent.state == EffectState::Confirmed {
                                fact(
                                    State::Observed,
                                    Source::ReconciliationJournal,
                                    Reason::ReconciledApplied,
                                )
                            } else {
                                fact(
                                    if agent.state == EffectState::Confirmed {
                                        State::Observed
                                    } else {
                                        State::Unknown
                                    },
                                    Source::StepJournal,
                                    if agent.state == EffectState::Confirmed {
                                        Reason::ResultRecorded
                                    } else {
                                        Reason::EffectUnknown
                                    },
                                )
                            };
                    }
                    None => {
                        observed = match result.reason_code.as_str() {
                            "reconciled_not_applied" => fact(
                                State::Observed,
                                Source::ReconciliationJournal,
                                Reason::ReconciledNotApplied,
                            ),
                            "reconciled_failed" => fact(
                                State::NotSatisfied,
                                Source::ReconciliationJournal,
                                Reason::ReconciledFailed,
                            ),
                            _ => fact(State::Unknown, Source::StepJournal, Reason::EffectUnknown),
                        };
                    }
                }
            } else {
                partial = true;
            }
        }
        output.push(ExecutionAttemptObservation {
            step_id: intent.step_id,
            attempt: intent.attempt,
            compensation: intent.compensation,
            intended_at: intent.intended_at,
            result_at,
            effect_state,
            fact: observed,
            lease_id: intent.fence_grant.lease_id.to_string(),
            lease_epoch: intent.fence_grant.epoch.0.to_string(),
        });
    }
    (output, partial)
}

fn epoch_bound(stored: Option<i64>, claimed: u64) -> bool {
    i64::try_from(claimed).is_ok_and(|epoch| epoch > 0 && stored == Some(epoch))
}

pub(super) fn effect_fact(attempts: &[ExecutionAttemptObservation], compensation: bool) -> ExecutionObservationFact {
    let mut selected = attempts
        .iter()
        .filter(|attempt| attempt.compensation == compensation)
        .peekable();
    if selected.peek().is_none() {
        return fact(
            State::NotObserved,
            Source::StepJournal,
            if compensation {
                Reason::CompensationNotObserved
            } else {
                Reason::NoEffectResult
            },
        );
    }
    let items: Vec<_> = selected.collect();
    if items.iter().any(|attempt| attempt.fact.state == State::Unknown) {
        return fact(
            State::Unknown,
            Source::StepJournal,
            if compensation {
                Reason::CompensationUnknown
            } else {
                Reason::EffectUnknown
            },
        );
    }
    if items.iter().any(|attempt| attempt.fact.state == State::NotSatisfied) {
        return fact(
            State::NotSatisfied,
            Source::ReconciliationJournal,
            if compensation {
                Reason::CompensationFailed
            } else {
                Reason::ReconciledFailed
            },
        );
    }
    fact(
        State::Observed,
        Source::StepJournal,
        if compensation {
            Reason::CompensationResultRecorded
        } else {
            Reason::ResultRecorded
        },
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn nullable_or_overflowed_epoch_never_establishes_a_binding() {
        assert!(!super::epoch_bound(None, u64::MAX));
        assert!(!super::epoch_bound(None, 1));
        assert!(!super::epoch_bound(Some(0), 0));
        assert!(super::epoch_bound(Some(7), 7));
    }
}
