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

use std::sync::atomic::AtomicU8;
use std::sync::Arc;

use super::*;

#[path = "tests/races.rs"]
mod races;

#[derive(Clone, Copy, Debug)]
enum ModelOperation {
    Register,
    Claim,
    BeginSending,
    Complete,
    FailNotStarted,
    FailPossiblyPartial,
    Cancel,
    Close,
}

const OPERATIONS: [ModelOperation; 8] = [
    ModelOperation::Register,
    ModelOperation::Claim,
    ModelOperation::BeginSending,
    ModelOperation::Complete,
    ModelOperation::FailNotStarted,
    ModelOperation::FailPossiblyPartial,
    ModelOperation::Cancel,
    ModelOperation::Close,
];

const STATES: [u8; 9] = [
    OPEN,
    REGISTERED,
    CLAIMED,
    SENDING,
    COMPLETED,
    FAILED_NOT_STARTED,
    FAILED_POSSIBLY_PARTIAL,
    CANCELLED,
    CLOSED,
];

#[test]
fn response_state_is_exactly_one_atomic_and_inline_state_does_not_embed_it() {
    assert_eq!(std::mem::size_of::<ResponseState>(), std::mem::size_of::<AtomicU8>());
    assert_eq!(std::mem::align_of::<ResponseState>(), std::mem::align_of::<AtomicU8>());
    assert!(!std::mem::needs_drop::<ResponseState>());

    let inline = crate::dispatch::InlineResponseSlot::disabled();
    assert_eq!(
        std::mem::size_of_val(&inline),
        std::mem::size_of::<crate::dispatch::InlineResponseSlot>()
    );
    assert!(!inline.has_deferred_capability());
}

#[test]
fn every_state_operation_pair_matches_the_reference_model() {
    for initial in STATES {
        for operation in OPERATIONS {
            let state = Arc::new(ResponseState {
                state: AtomicU8::new(initial),
            });
            let expected = model_transition(snapshot(initial), operation);
            let (actual, claim) = apply_actual(&state, operation);

            assert_eq!(
                actual,
                expected.as_ref().map(|_| ()).map_err(Clone::clone),
                "operation {operation:?} from {:?}",
                snapshot(initial)
            );
            assert_eq!(
                state.snapshot(),
                expected.unwrap_or_else(|_| snapshot(initial)),
                "operation {operation:?} from {:?}",
                snapshot(initial)
            );
            drop(claim);
        }
    }
}

#[test]
fn legal_registered_paths_preserve_both_failure_progress_values() {
    let completed = Arc::new(ResponseState::open());
    completed.register().expect("register");
    completed.claim().expect("claim");
    completed
        .begin_sending()
        .expect("send claim")
        .complete()
        .expect("complete");
    assert_eq!(completed.terminal_state(), Some(ResponseTerminalState::Completed));

    for progress in [WriteProgress::NotStarted, WriteProgress::PossiblyPartial] {
        let failed = Arc::new(ResponseState::open());
        failed.register().expect("register");
        failed.claim().expect("claim");
        failed
            .begin_sending()
            .expect("send claim")
            .fail(progress)
            .expect("fail");
        assert_eq!(
            failed.terminal_state(),
            Some(ResponseTerminalState::Failed { progress })
        );
    }
}

#[test]
fn send_claim_drop_is_terminal_and_progress_never_downgrades() {
    let not_started = Arc::new(ResponseState::open());
    drop(not_started.begin_sending().expect("send claim"));
    assert_eq!(
        not_started.terminal_state(),
        Some(ResponseTerminalState::Failed {
            progress: WriteProgress::NotStarted,
        })
    );

    let partial_drop = Arc::new(ResponseState::open());
    let mut claim = partial_drop.begin_sending().expect("send claim");
    claim.mark_possibly_partial();
    drop(claim);
    assert_eq!(
        partial_drop.terminal_state(),
        Some(ResponseTerminalState::Failed {
            progress: WriteProgress::PossiblyPartial,
        })
    );

    let partial_explicit = Arc::new(ResponseState::open());
    let mut claim = partial_explicit.begin_sending().expect("send claim");
    claim.mark_possibly_partial();
    claim
        .fail(WriteProgress::NotStarted)
        .expect("an explicit failure cannot downgrade progress");
    assert_eq!(
        partial_explicit.terminal_state(),
        Some(ResponseTerminalState::Failed {
            progress: WriteProgress::PossiblyPartial,
        })
    );
}

#[test]
fn consuming_completion_disarms_the_claim_drop_path() {
    let state = Arc::new(ResponseState::open());
    state.begin_sending().expect("send claim").complete().expect("complete");
    assert_eq!(state.terminal_state(), Some(ResponseTerminalState::Completed));
    assert_eq!(
        state.close(),
        Err(ResponseStateError::AlreadyCompleted {
            state: ResponseTerminalState::Completed,
        })
    );
}

fn apply_actual(
    state: &Arc<ResponseState>,
    operation: ModelOperation,
) -> (Result<(), ResponseStateError>, Option<ResponseSendClaim>) {
    let claim_for_existing_sending = || ResponseSendClaim {
        state: Arc::clone(state),
        drop_progress: WriteProgress::NotStarted,
        delegated: None,
        active: true,
    };
    match operation {
        ModelOperation::Register => (state.register(), None),
        ModelOperation::Claim => (state.claim(), None),
        ModelOperation::BeginSending => match state.begin_sending() {
            Ok(claim) => (Ok(()), Some(claim)),
            Err(error) => (Err(error), None),
        },
        ModelOperation::Complete => (claim_for_existing_sending().complete(), None),
        ModelOperation::FailNotStarted => (claim_for_existing_sending().fail(WriteProgress::NotStarted), None),
        ModelOperation::FailPossiblyPartial => {
            (claim_for_existing_sending().fail(WriteProgress::PossiblyPartial), None)
        }
        ModelOperation::Cancel => (state.cancel(), None),
        ModelOperation::Close => (state.close(), None),
    }
}

fn model_transition(
    state: ResponseStateSnapshot,
    operation: ModelOperation,
) -> Result<ResponseStateSnapshot, ResponseStateError> {
    if let ResponseStateSnapshot::Terminal(state) = state {
        return Err(ResponseStateError::AlreadyCompleted { state });
    }

    let next = match (state, operation) {
        (ResponseStateSnapshot::Open, ModelOperation::Register) => ResponseStateSnapshot::Registered,
        (ResponseStateSnapshot::Registered, ModelOperation::Claim) => ResponseStateSnapshot::Claimed,
        (ResponseStateSnapshot::Open | ResponseStateSnapshot::Claimed, ModelOperation::BeginSending) => {
            ResponseStateSnapshot::Sending
        }
        (ResponseStateSnapshot::Sending, ModelOperation::Complete) => {
            ResponseStateSnapshot::Terminal(ResponseTerminalState::Completed)
        }
        (ResponseStateSnapshot::Sending, ModelOperation::FailNotStarted) => {
            ResponseStateSnapshot::Terminal(ResponseTerminalState::Failed {
                progress: WriteProgress::NotStarted,
            })
        }
        (ResponseStateSnapshot::Sending, ModelOperation::FailPossiblyPartial) => {
            ResponseStateSnapshot::Terminal(ResponseTerminalState::Failed {
                progress: WriteProgress::PossiblyPartial,
            })
        }
        (
            ResponseStateSnapshot::Open | ResponseStateSnapshot::Registered | ResponseStateSnapshot::Claimed,
            ModelOperation::Cancel,
        ) => ResponseStateSnapshot::Terminal(ResponseTerminalState::Cancelled),
        (
            ResponseStateSnapshot::Open | ResponseStateSnapshot::Registered | ResponseStateSnapshot::Claimed,
            ModelOperation::Close,
        ) => ResponseStateSnapshot::Terminal(ResponseTerminalState::Closed),
        (state, operation) => {
            return Err(ResponseStateError::InvalidTransition {
                transition: model_transition_name(operation),
                state,
            });
        }
    };
    Ok(next)
}

const fn model_transition_name(operation: ModelOperation) -> ResponseTransition {
    match operation {
        ModelOperation::Register => ResponseTransition::Register,
        ModelOperation::Claim => ResponseTransition::Claim,
        ModelOperation::BeginSending => ResponseTransition::BeginSending,
        ModelOperation::Complete => ResponseTransition::Complete,
        ModelOperation::FailNotStarted | ModelOperation::FailPossiblyPartial => ResponseTransition::Fail,
        ModelOperation::Cancel => ResponseTransition::Cancel,
        ModelOperation::Close => ResponseTransition::Close,
    }
}
