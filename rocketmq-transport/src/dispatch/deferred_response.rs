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

//! Atomic lifecycle state for responses retained beyond the handler call.

use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::ResponseTerminalState;
use super::WriteProgress;

const OPEN: u8 = 0;
const REGISTERED: u8 = 1;
const CLAIMED: u8 = 2;
const SENDING: u8 = 3;
const COMPLETED: u8 = 4;
const FAILED_NOT_STARTED: u8 = 5;
const FAILED_POSSIBLY_PARTIAL: u8 = 6;
const CANCELLED: u8 = 7;
const CLOSED: u8 = 8;

/// One atomic owner for a deferred response lifecycle.
///
/// This state is allocated only when a later stage creates a deferred
/// responder. Inline responses continue to use their stack-owned slot.
pub(crate) struct ResponseState {
    state: AtomicU8,
}

/// Stable internal view of the deferred response lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseStateSnapshot {
    Open,
    Registered,
    Claimed,
    Sending,
    Terminal(ResponseTerminalState),
}

/// Operation rejected by the deferred response state machine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseTransition {
    Register,
    Claim,
    BeginSending,
    Complete,
    Fail,
    Cancel,
    Close,
}

/// Failure to perform one deferred response state transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum ResponseStateError {
    /// A previous operation already selected the terminal state.
    #[error("deferred response already reached terminal state {state:?}")]
    AlreadyCompleted { state: ResponseTerminalState },
    /// The operation is not legal from the observed non-terminal state.
    #[error("deferred response cannot perform {transition:?} from {state:?}")]
    InvalidTransition {
        transition: ResponseTransition,
        state: ResponseStateSnapshot,
    },
}

/// Affine ownership of the right to terminate `Sending`.
///
/// A newly created claim proves that canonical socket I/O has not started.
/// Before entering any seam that might begin socket I/O, the owner must call
/// [`Self::mark_possibly_partial`]. Dropping an unfinished claim records the
/// most conservative progress reached by the owner.
#[must_use]
pub(crate) struct ResponseSendClaim {
    state: Arc<ResponseState>,
    drop_progress: WriteProgress,
    active: bool,
}

impl ResponseState {
    /// Creates the deferred-only state in `Open`.
    pub(crate) const fn open() -> Self {
        Self {
            state: AtomicU8::new(OPEN),
        }
    }

    /// Returns one acquire snapshot of the complete lifecycle state.
    pub(crate) fn snapshot(&self) -> ResponseStateSnapshot {
        snapshot(self.state.load(Ordering::Acquire))
    }

    /// Returns the terminal state, if a terminal transition has won.
    pub(crate) fn terminal_state(&self) -> Option<ResponseTerminalState> {
        match self.snapshot() {
            ResponseStateSnapshot::Terminal(state) => Some(state),
            ResponseStateSnapshot::Open
            | ResponseStateSnapshot::Registered
            | ResponseStateSnapshot::Claimed
            | ResponseStateSnapshot::Sending => None,
        }
    }

    /// Activates registry ownership of an open deferred response.
    pub(crate) fn register(&self) -> Result<(), ResponseStateError> {
        self.transition_exact(OPEN, REGISTERED, ResponseTransition::Register)
    }

    /// Claims one registered response for resume execution.
    pub(crate) fn claim(&self) -> Result<(), ResponseStateError> {
        self.transition_exact(REGISTERED, CLAIMED, ResponseTransition::Claim)
    }

    /// Acquires affine ownership of response delivery.
    pub(crate) fn begin_sending(self: &Arc<Self>) -> Result<ResponseSendClaim, ResponseStateError> {
        let mut observed = self.state.load(Ordering::Acquire);
        loop {
            match observed {
                OPEN | CLAIMED => {
                    match self
                        .state
                        .compare_exchange(observed, SENDING, Ordering::AcqRel, Ordering::Acquire)
                    {
                        Ok(_) => {
                            return Ok(ResponseSendClaim {
                                state: Arc::clone(self),
                                drop_progress: WriteProgress::NotStarted,
                                active: true,
                            });
                        }
                        Err(actual) => observed = actual,
                    }
                }
                actual => return Err(transition_error(ResponseTransition::BeginSending, actual)),
            }
        }
    }

    /// Cancels a response that has not begun delivery.
    pub(crate) fn cancel(&self) -> Result<(), ResponseStateError> {
        self.stop_with(CANCELLED, ResponseTransition::Cancel, |_| {})
    }

    /// Closes a response that has not begun delivery.
    pub(crate) fn close(&self) -> Result<(), ResponseStateError> {
        self.stop_with(CLOSED, ResponseTransition::Close, |_| {})
    }

    fn stop_with(
        &self,
        terminal: u8,
        transition: ResponseTransition,
        mut before_compare: impl FnMut(u8),
    ) -> Result<(), ResponseStateError> {
        let mut observed = self.state.load(Ordering::Acquire);
        loop {
            match observed {
                OPEN | REGISTERED | CLAIMED => {
                    before_compare(observed);
                    match self
                        .state
                        .compare_exchange(observed, terminal, Ordering::AcqRel, Ordering::Acquire)
                    {
                        Ok(_) => return Ok(()),
                        Err(actual) => observed = actual,
                    }
                }
                actual => return Err(transition_error(transition, actual)),
            }
        }
    }

    fn transition_exact(
        &self,
        expected: u8,
        target: u8,
        transition: ResponseTransition,
    ) -> Result<(), ResponseStateError> {
        self.state
            .compare_exchange(expected, target, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|actual| transition_error(transition, actual))
    }

    fn finish_sending(&self, terminal: ResponseTerminalState) -> Result<(), ResponseStateError> {
        let transition = match terminal {
            ResponseTerminalState::Completed => ResponseTransition::Complete,
            ResponseTerminalState::Failed { .. } => ResponseTransition::Fail,
            ResponseTerminalState::Cancelled | ResponseTerminalState::Closed => {
                unreachable!("a send claim can only complete or fail its response")
            }
        };
        self.transition_exact(SENDING, encode_terminal(terminal), transition)
    }
}

impl ResponseSendClaim {
    /// Marks that zero socket output can no longer be proven.
    ///
    /// This progress change is monotonic and cannot be downgraded.
    pub(crate) fn mark_possibly_partial(&mut self) {
        self.drop_progress = WriteProgress::PossiblyPartial;
    }

    /// Records successful canonical response delivery.
    pub(crate) fn complete(mut self) -> Result<(), ResponseStateError> {
        let result = self.state.finish_sending(ResponseTerminalState::Completed);
        self.active = false;
        result
    }

    /// Records failed canonical response delivery without losing prior progress.
    pub(crate) fn fail(mut self, progress: WriteProgress) -> Result<(), ResponseStateError> {
        let progress = match (self.drop_progress, progress) {
            (WriteProgress::PossiblyPartial, _) | (_, WriteProgress::PossiblyPartial) => WriteProgress::PossiblyPartial,
            (WriteProgress::NotStarted, WriteProgress::NotStarted) => WriteProgress::NotStarted,
        };
        let result = self.state.finish_sending(ResponseTerminalState::Failed { progress });
        self.active = false;
        result
    }
}

impl Drop for ResponseSendClaim {
    fn drop(&mut self) {
        if self.active {
            let _ = self.state.finish_sending(ResponseTerminalState::Failed {
                progress: self.drop_progress,
            });
            self.active = false;
        }
    }
}

fn transition_error(transition: ResponseTransition, actual: u8) -> ResponseStateError {
    match snapshot(actual) {
        ResponseStateSnapshot::Terminal(state) => ResponseStateError::AlreadyCompleted { state },
        state @ (ResponseStateSnapshot::Open
        | ResponseStateSnapshot::Registered
        | ResponseStateSnapshot::Claimed
        | ResponseStateSnapshot::Sending) => ResponseStateError::InvalidTransition { transition, state },
    }
}

fn snapshot(state: u8) -> ResponseStateSnapshot {
    match state {
        OPEN => ResponseStateSnapshot::Open,
        REGISTERED => ResponseStateSnapshot::Registered,
        CLAIMED => ResponseStateSnapshot::Claimed,
        SENDING => ResponseStateSnapshot::Sending,
        COMPLETED => ResponseStateSnapshot::Terminal(ResponseTerminalState::Completed),
        FAILED_NOT_STARTED => ResponseStateSnapshot::Terminal(ResponseTerminalState::Failed {
            progress: WriteProgress::NotStarted,
        }),
        FAILED_POSSIBLY_PARTIAL => ResponseStateSnapshot::Terminal(ResponseTerminalState::Failed {
            progress: WriteProgress::PossiblyPartial,
        }),
        CANCELLED => ResponseStateSnapshot::Terminal(ResponseTerminalState::Cancelled),
        CLOSED => ResponseStateSnapshot::Terminal(ResponseTerminalState::Closed),
        _ => unreachable!("ResponseState stores only module-owned monotonic state tags"),
    }
}

const fn encode_terminal(state: ResponseTerminalState) -> u8 {
    match state {
        ResponseTerminalState::Completed => COMPLETED,
        ResponseTerminalState::Failed {
            progress: WriteProgress::NotStarted,
        } => FAILED_NOT_STARTED,
        ResponseTerminalState::Failed {
            progress: WriteProgress::PossiblyPartial,
        } => FAILED_POSSIBLY_PARTIAL,
        ResponseTerminalState::Cancelled => CANCELLED,
        ResponseTerminalState::Closed => CLOSED,
    }
}

#[cfg(test)]
#[path = "deferred_response/tests.rs"]
mod tests;
