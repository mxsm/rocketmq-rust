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

use std::sync::Arc;
use std::sync::Barrier;

use super::*;

#[test]
fn cancel_and_close_choose_exactly_one_terminal_winner() {
    for _ in 0..256 {
        let state = Arc::new(ResponseState::open());
        let barrier = Arc::new(Barrier::new(3));

        let cancel_state = Arc::clone(&state);
        let cancel_barrier = Arc::clone(&barrier);
        let cancel = std::thread::spawn(move || {
            cancel_barrier.wait();
            cancel_state.cancel()
        });

        let close_state = Arc::clone(&state);
        let close_barrier = Arc::clone(&barrier);
        let close = std::thread::spawn(move || {
            close_barrier.wait();
            close_state.close()
        });

        barrier.wait();
        let cancel = cancel.join().expect("cancel thread");
        let close = close.join().expect("close thread");
        let terminal = state.terminal_state().expect("terminal winner");

        assert_eq!(
            usize::from(cancel == Ok(ResponseStateOutcome::Applied(())))
                + usize::from(close == Ok(ResponseStateOutcome::Applied(()))),
            1
        );
        match terminal {
            ResponseTerminalState::Cancelled => {
                assert_eq!(cancel, Ok(ResponseStateOutcome::Applied(())));
                assert_eq!(
                    close,
                    Ok(ResponseStateOutcome::AlreadyCompleted {
                        state: ResponseTerminalState::Cancelled,
                        reason: Some(DeferredTerminalReason::Explicit),
                    })
                );
            }
            ResponseTerminalState::Closed => {
                assert_eq!(close, Ok(ResponseStateOutcome::Applied(())));
                assert_eq!(
                    cancel,
                    Ok(ResponseStateOutcome::AlreadyCompleted {
                        state: ResponseTerminalState::Closed,
                        reason: Some(DeferredTerminalReason::SessionClosed),
                    })
                );
            }
            other => panic!("unexpected terminal winner: {other:?}"),
        }
    }
}

#[test]
fn racing_terminal_attempts_record_only_the_atomic_winner() {
    for _ in 0..64 {
        let (telemetry, terminals) = TransportTelemetry::with_deferred_terminal_capture();
        let state = Arc::new(ResponseState::observed(
            telemetry,
            rocketmq_protocol::code::request_code::RequestCode::PopMessage.to_i32(),
        ));
        let barrier = Arc::new(Barrier::new(3));

        let cancel_state = Arc::clone(&state);
        let cancel_barrier = Arc::clone(&barrier);
        let cancel = std::thread::spawn(move || {
            cancel_barrier.wait();
            cancel_state.cancel_with_reason(DeferredSystemCancellationReason::OWNER_DEADLINE)
        });

        let close_state = Arc::clone(&state);
        let close_barrier = Arc::clone(&barrier);
        let close = std::thread::spawn(move || {
            close_barrier.wait();
            close_state.close_with_reason(DeferredSystemCloseReason::SESSION_CLOSED)
        });

        barrier.wait();
        let cancel = cancel.join().expect("cancel thread");
        let close = close.join().expect("close thread");
        assert_eq!(
            usize::from(cancel == Ok(ResponseStateOutcome::Applied(())))
                + usize::from(close == Ok(ResponseStateOutcome::Applied(()))),
            1
        );

        let reason = state.terminal_reason().expect("one reason wins");
        match reason {
            DeferredTerminalReason::OwnerDeadline => {
                assert_eq!(state.terminal_state(), Some(ResponseTerminalState::Cancelled));
                assert_eq!(cancel, Ok(ResponseStateOutcome::Applied(())));
                assert_eq!(
                    close,
                    Ok(ResponseStateOutcome::AlreadyCompleted {
                        state: ResponseTerminalState::Cancelled,
                        reason: Some(DeferredTerminalReason::OwnerDeadline),
                    })
                );
            }
            DeferredTerminalReason::SessionClosed => {
                assert_eq!(state.terminal_state(), Some(ResponseTerminalState::Closed));
                assert_eq!(close, Ok(ResponseStateOutcome::Applied(())));
                assert_eq!(
                    cancel,
                    Ok(ResponseStateOutcome::AlreadyCompleted {
                        state: ResponseTerminalState::Closed,
                        reason: Some(DeferredTerminalReason::SessionClosed),
                    })
                );
            }
            other => panic!("unexpected terminal reason: {other:?}"),
        }
        assert_eq!(terminals.lock().as_slice(), [("pop_message", reason.as_str())]);
    }
}

#[test]
fn stop_races_retry_across_register_and_claim_progress() {
    for close in [false, true] {
        assert_stop_retries_after_lifecycle_advance(close, OPEN, |state| state.register());
        assert_stop_retries_after_lifecycle_advance(close, REGISTERED, |state| state.claim());
    }
}

fn assert_stop_retries_after_lifecycle_advance(
    close: bool,
    initial: u8,
    advance: impl FnOnce(&ResponseState) -> Result<ResponseStateOutcome, TransportContractViolation>,
) {
    let state = Arc::new(ResponseState {
        state: std::sync::atomic::AtomicU8::new(initial),
        observer: DeferredTerminalObserver::noop(),
    });
    let observed = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));

    let stop_state = Arc::clone(&state);
    let stop_observed = Arc::clone(&observed);
    let stop_resume = Arc::clone(&resume);
    let stop = std::thread::spawn(move || {
        let mut first_compare = true;
        let (reason, transition) = if close {
            (DeferredTerminalReason::SessionClosed, ResponseTransition::Close)
        } else {
            (DeferredTerminalReason::Explicit, ResponseTransition::Cancel)
        };
        stop_state.stop_with_reason(reason, transition, |loaded| {
            if first_compare {
                assert_eq!(loaded, initial);
                first_compare = false;
                stop_observed.wait();
                stop_resume.wait();
            }
        })
    });

    observed.wait();
    expect_applied(
        advance(&state).expect("lifecycle-advance contract"),
        "lifecycle state advance before the first stop CAS",
    );
    resume.wait();
    assert_eq!(stop.join().expect("stop thread"), Ok(ResponseStateOutcome::Applied(())));
    assert_eq!(
        state.terminal_state(),
        Some(if close {
            ResponseTerminalState::Closed
        } else {
            ResponseTerminalState::Cancelled
        })
    );
}

#[test]
fn begin_sending_and_stop_have_one_linearized_owner() {
    for close in [false, true] {
        for _ in 0..256 {
            let state = Arc::new(ResponseState::open());
            let barrier = Arc::new(Barrier::new(3));

            let send_state = Arc::clone(&state);
            let send_barrier = Arc::clone(&barrier);
            let send = std::thread::spawn(move || {
                send_barrier.wait();
                send_state.begin_sending()
            });

            let stop_state = Arc::clone(&state);
            let stop_barrier = Arc::clone(&barrier);
            let stop = std::thread::spawn(move || {
                stop_barrier.wait();
                if close {
                    stop_state.close()
                } else {
                    stop_state.cancel()
                }
            });

            barrier.wait();
            let send = send.join().expect("send thread");
            let stop = stop.join().expect("stop thread");
            let stop_terminal = if close {
                ResponseTerminalState::Closed
            } else {
                ResponseTerminalState::Cancelled
            };

            match send {
                Ok(ResponseStateOutcome::Applied(claim)) => {
                    assert_eq!(
                        stop,
                        Err(TransportContractViolation::DeferredResponseInvalidTransition {
                            operation: if close { "close" } else { "cancel" },
                            state: "sending",
                        })
                    );
                    drop(claim);
                    assert_eq!(
                        state.terminal_state(),
                        Some(ResponseTerminalState::Failed {
                            progress: WriteProgress::NotStarted,
                        })
                    );
                }
                Ok(ResponseStateOutcome::AlreadyCompleted { state: winner, reason }) => {
                    assert_eq!(winner, stop_terminal);
                    assert_eq!(
                        reason,
                        Some(if close {
                            DeferredTerminalReason::SessionClosed
                        } else {
                            DeferredTerminalReason::Explicit
                        })
                    );
                    assert_eq!(stop, Ok(ResponseStateOutcome::Applied(())));
                    assert_eq!(state.terminal_state(), Some(stop_terminal));
                }
                Err(_) => panic!("begin sending unexpectedly returned a contract violation"),
            }
        }
    }
}
