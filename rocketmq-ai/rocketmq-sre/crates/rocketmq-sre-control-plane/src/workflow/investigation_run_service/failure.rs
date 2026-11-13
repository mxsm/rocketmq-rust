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

use rocketmq_sre_contracts::InvestigationRunFailure;

use crate::ControlPlaneRequestFailure;

pub(crate) fn contract_failure(failure: InvestigationRunFailure) -> ControlPlaneRequestFailure {
    use InvestigationRunFailure::*;
    let code = match failure {
        InvalidInput => return ControlPlaneRequestFailure::validation("invalid_input", "invalid run request"),
        InvalidAssessment => return ControlPlaneRequestFailure::validation("invalid_assessment", "invalid assessment"),
        Unauthorized => return ControlPlaneRequestFailure::forbidden("unauthorized", "run control is forbidden"),
        ScopeMismatch => return ControlPlaneRequestFailure::forbidden("scope_mismatch", "scope is forbidden"),
        PackNotAllowed => return ControlPlaneRequestFailure::forbidden("pack_not_allowed", "pack is forbidden"),
        StepNotFound => return ControlPlaneRequestFailure::not_found_code("step_not_found"),
        InvalidStateTransition => "invalid_state_transition",
        RevisionConflict { .. } => "revision_conflict",
        IdempotencyConflict => "idempotency_conflict",
        BudgetExceeded => "budget_exceeded",
        DeadlineExceeded => "deadline_exceeded",
        StepIdentityConflict => "step_identity_conflict",
        StepAlreadyCompleted => "step_already_completed",
        StepInFlight => "step_in_flight",
        AttemptMismatch => "attempt_mismatch",
        ResultConflict => "result_conflict",
        CounterExhausted => "counter_exhausted",
    };
    ControlPlaneRequestFailure::conflict_code(code, "run operation conflicts with current state")
}
