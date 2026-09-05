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

#[path = "deferred_response/races.rs"]
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

const STATES: [u8; 16] = [
    OPEN,
    REGISTERED,
    CLAIMED,
    SENDING,
    COMPLETED,
    FAILED_NOT_STARTED,
    FAILED_POSSIBLY_PARTIAL,
    CANCELLED_EXPLICIT,
    CLOSED_RECEIVER_DROPPED,
    CANCELLED_ABANDONED,
    CANCELLED_CLAIM_DROPPED,
    CANCELLED_OWNER_DEADLINE,
    CANCELLED_PARENT_CANCELLED,
    CANCELLED_PROCESSOR_UNAVAILABLE,
    CANCELLED_SERVICE_STOPPING,
    CLOSED_SESSION_CLOSED,
];

impl ModelOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Register => "register",
            Self::Claim => "claim",
            Self::BeginSending => "begin_sending",
            Self::Complete => "complete",
            Self::FailNotStarted | Self::FailPossiblyPartial => "fail",
            Self::Cancel => "cancel",
            Self::Close => "close",
        }
    }
}

#[test]
fn response_state_uses_one_atomic_and_inline_state_does_not_embed_it() {
    let state = ResponseState::open();
    assert_eq!(std::mem::size_of_val(&state.state), std::mem::size_of::<AtomicU8>());
    assert_eq!(std::mem::align_of_val(&state.state), std::mem::align_of::<AtomicU8>());

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
                observer: DeferredTerminalObserver::noop(),
            });
            let expected = model_transition(initial, operation);
            let (actual, claim) = apply_actual(&state, operation);

            assert_eq!(
                actual,
                expected.result,
                "operation {operation:?} from {:?}",
                snapshot(initial)
            );
            assert_eq!(
                state.snapshot(),
                expected.state,
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
    expect_applied(completed.register().expect("register contract"), "register");
    expect_applied(completed.claim().expect("claim contract"), "claim");
    expect_applied(
        expect_applied(completed.begin_sending().expect("send-claim contract"), "begin sending")
            .complete()
            .expect("complete contract"),
        "complete",
    );
    assert_eq!(completed.terminal_state(), Some(ResponseTerminalState::Completed));
    assert_eq!(completed.terminal_reason(), None);

    for progress in [WriteProgress::NotStarted, WriteProgress::PossiblyPartial] {
        let failed = Arc::new(ResponseState::open());
        expect_applied(failed.register().expect("register contract"), "register");
        expect_applied(failed.claim().expect("claim contract"), "claim");
        expect_applied(
            expect_applied(failed.begin_sending().expect("send-claim contract"), "begin sending")
                .fail(progress)
                .expect("fail contract"),
            "fail",
        );
        assert_eq!(
            failed.terminal_state(),
            Some(ResponseTerminalState::Failed { progress })
        );
        assert_eq!(failed.terminal_reason(), None);
    }
}

#[test]
fn every_terminal_reason_has_one_atomic_projection() {
    let cases = [
        (DeferredTerminalReason::Explicit, ResponseTerminalState::Cancelled),
        (DeferredTerminalReason::ReceiverDropped, ResponseTerminalState::Closed),
        (DeferredTerminalReason::Abandoned, ResponseTerminalState::Cancelled),
        (DeferredTerminalReason::ClaimDropped, ResponseTerminalState::Cancelled),
        (DeferredTerminalReason::OwnerDeadline, ResponseTerminalState::Cancelled),
        (
            DeferredTerminalReason::ParentCancelled,
            ResponseTerminalState::Cancelled,
        ),
        (
            DeferredTerminalReason::ProcessorUnavailable,
            ResponseTerminalState::Cancelled,
        ),
        (
            DeferredTerminalReason::ServiceStopping,
            ResponseTerminalState::Cancelled,
        ),
        (DeferredTerminalReason::SessionClosed, ResponseTerminalState::Closed),
    ];

    for (reason, expected) in cases {
        let state = ResponseState::open();
        assert_eq!(
            state.stop_with_reason(reason, ResponseTransition::Cancel, |_| {}),
            Ok(ResponseStateOutcome::Applied(())),
            "the first terminal reason wins"
        );
        assert_eq!(state.terminal_state(), Some(expected));
        assert_eq!(state.terminal_reason(), Some(reason));
        assert_eq!(reason.terminal_state(), expected);
        assert_eq!(
            decode_terminal_reason(state.state.load(Ordering::Acquire)),
            Some(reason)
        );

        assert_eq!(
            state.cancel(),
            Ok(ResponseStateOutcome::AlreadyCompleted {
                state: expected,
                reason: Some(reason),
            })
        );
    }
}

#[test]
fn deferred_terminal_metrics_use_only_fixed_request_code_buckets() {
    use rocketmq_protocol::code::request_code::RequestCode;

    let cases = [
        (RequestCode::PullMessage.to_i32(), "pull_message"),
        (RequestCode::PopMessage.to_i32(), "pop_message"),
        (RequestCode::Notification.to_i32(), "notification"),
        (RequestCode::SendMessage.to_i32(), "other"),
    ];

    for (request_code, expected_bucket) in cases {
        let (telemetry, terminals) = TransportTelemetry::with_deferred_terminal_capture();
        let state = ResponseState::observed(telemetry, request_code);
        assert_eq!(
            state.cancel(),
            Ok(ResponseStateOutcome::Applied(())),
            "the first terminal transition wins"
        );
        assert_eq!(
            terminals.lock().as_slice(),
            [(expected_bucket, DeferredTerminalReason::Explicit.as_str())]
        );
    }
}

#[test]
fn delivered_and_failed_responses_do_not_record_non_response_terminal_metrics() {
    for complete in [true, false] {
        let (telemetry, terminals) = TransportTelemetry::with_deferred_terminal_capture();
        let state = Arc::new(ResponseState::observed(
            telemetry,
            rocketmq_protocol::code::request_code::RequestCode::Notification.to_i32(),
        ));
        let claim = expect_applied(state.begin_sending().expect("begin-sending contract"), "begin sending");
        if complete {
            expect_applied(claim.complete().expect("complete contract"), "complete");
        } else {
            expect_applied(
                claim.fail(WriteProgress::PossiblyPartial).expect("failure contract"),
                "fail",
            );
        }
        assert!(terminals.lock().is_empty());
    }
}

#[test]
fn send_claim_drop_is_terminal_and_progress_never_downgrades() {
    let not_started = Arc::new(ResponseState::open());
    drop(expect_applied(
        not_started.begin_sending().expect("send-claim contract"),
        "begin sending",
    ));
    assert_eq!(
        not_started.terminal_state(),
        Some(ResponseTerminalState::Failed {
            progress: WriteProgress::NotStarted,
        })
    );

    let partial_drop = Arc::new(ResponseState::open());
    let mut claim = expect_applied(
        partial_drop.begin_sending().expect("send-claim contract"),
        "begin sending",
    );
    claim.mark_possibly_partial();
    drop(claim);
    assert_eq!(
        partial_drop.terminal_state(),
        Some(ResponseTerminalState::Failed {
            progress: WriteProgress::PossiblyPartial,
        })
    );

    let partial_explicit = Arc::new(ResponseState::open());
    let mut claim = expect_applied(
        partial_explicit.begin_sending().expect("send-claim contract"),
        "begin sending",
    );
    claim.mark_possibly_partial();
    expect_applied(
        claim.fail(WriteProgress::NotStarted).expect("failure contract"),
        "an explicit failure cannot downgrade progress",
    );
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
    expect_applied(
        expect_applied(state.begin_sending().expect("send-claim contract"), "begin sending")
            .complete()
            .expect("complete contract"),
        "complete",
    );
    assert_eq!(state.terminal_state(), Some(ResponseTerminalState::Completed));
    assert_eq!(
        state.close(),
        Ok(ResponseStateOutcome::AlreadyCompleted {
            state: ResponseTerminalState::Completed,
            reason: None,
        })
    );
}

fn apply_actual(
    state: &Arc<ResponseState>,
    operation: ModelOperation,
) -> (
    Result<ResponseStateOutcome, TransportContractViolation>,
    Option<ResponseSendClaim>,
) {
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
            Ok(ResponseStateOutcome::Applied(claim)) => (Ok(ResponseStateOutcome::Applied(())), Some(claim)),
            Ok(ResponseStateOutcome::AlreadyCompleted { state, reason }) => {
                (Ok(ResponseStateOutcome::AlreadyCompleted { state, reason }), None)
            }
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct ModelTransition {
    result: Result<ResponseStateOutcome, TransportContractViolation>,
    state: ResponseStateSnapshot,
}

fn model_transition(state_code: u8, operation: ModelOperation) -> ModelTransition {
    let state = snapshot(state_code);
    if let ResponseStateSnapshot::Terminal(state) = state {
        return ModelTransition {
            result: Ok(ResponseStateOutcome::AlreadyCompleted {
                state,
                reason: decode_terminal_reason(state_code),
            }),
            state: ResponseStateSnapshot::Terminal(state),
        };
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
        (state, _) => {
            return ModelTransition {
                result: Err(TransportContractViolation::DeferredResponseInvalidTransition {
                    operation: operation.as_str(),
                    state: state.as_str(),
                }),
                state,
            };
        }
    };
    ModelTransition {
        result: Ok(ResponseStateOutcome::Applied(())),
        state: next,
    }
}

fn expect_applied<T>(outcome: ResponseStateOutcome<T>, operation: &str) -> T {
    match outcome {
        ResponseStateOutcome::Applied(value) => value,
        ResponseStateOutcome::AlreadyCompleted { state, reason } => {
            panic!("{operation} unexpectedly observed terminal state {state:?} with reason {reason:?}");
        }
    }
}
