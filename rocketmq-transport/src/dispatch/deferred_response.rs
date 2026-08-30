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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_protocol::code::request_code::RequestCode;

use super::ResponseTerminalState;
use super::WriteProgress;
use crate::telemetry::TransportTelemetry;

const OPEN: u8 = 0;
const REGISTERED: u8 = 1;
const CLAIMED: u8 = 2;
const SENDING: u8 = 3;
const COMPLETED: u8 = 4;
const FAILED_NOT_STARTED: u8 = 5;
const FAILED_POSSIBLY_PARTIAL: u8 = 6;
const CANCELLED_EXPLICIT: u8 = 7;
const CLOSED_RECEIVER_DROPPED: u8 = 8;
const CANCELLED_ABANDONED: u8 = 9;
const CANCELLED_CLAIM_DROPPED: u8 = 10;
const CANCELLED_OWNER_DEADLINE: u8 = 11;
const CANCELLED_PARENT_CANCELLED: u8 = 12;
const CANCELLED_PROCESSOR_UNAVAILABLE: u8 = 13;
const CANCELLED_SERVICE_STOPPING: u8 = 14;
const CLOSED_SESSION_CLOSED: u8 = 15;

/// Stable reason that selected a non-response deferred terminal state.
///
/// The reason and its [`ResponseTerminalState`] projection are selected by one
/// atomic transition. Successful and failed response delivery do not have a
/// deferred terminal reason.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DeferredTerminalReason {
    /// The response owner explicitly cancelled its capability.
    Explicit,
    /// The caller-owned response receiver was dropped.
    ReceiverDropped,
    /// An unfinished deferred responder was dropped.
    Abandoned,
    /// An affine deferred claim was dropped without being resumed.
    ClaimDropped,
    /// The trusted request owner deadline expired.
    OwnerDeadline,
    /// The request's parent lifecycle was cancelled.
    ParentCancelled,
    /// No processor remained available to resume the request.
    ProcessorUnavailable,
    /// The owning service stopped before response completion.
    ServiceStopping,
    /// The trusted request session closed.
    SessionClosed,
}

impl DeferredTerminalReason {
    /// Returns the stable low-cardinality metric label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::ReceiverDropped => "receiver_dropped",
            Self::Abandoned => "abandoned",
            Self::ClaimDropped => "claim_dropped",
            Self::OwnerDeadline => "owner_deadline",
            Self::ParentCancelled => "parent_cancelled",
            Self::ProcessorUnavailable => "processor_unavailable",
            Self::ServiceStopping => "service_stopping",
            Self::SessionClosed => "session_closed",
        }
    }

    /// Returns the existing public terminal-state projection for this reason.
    #[must_use]
    pub const fn terminal_state(self) -> ResponseTerminalState {
        match self {
            Self::ReceiverDropped | Self::SessionClosed => ResponseTerminalState::Closed,
            Self::Explicit
            | Self::Abandoned
            | Self::ClaimDropped
            | Self::OwnerDeadline
            | Self::ParentCancelled
            | Self::ProcessorUnavailable
            | Self::ServiceStopping => ResponseTerminalState::Cancelled,
        }
    }
}

/// Sealed system-owned reasons that project to `Cancelled`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeferredSystemCancellationReason(DeferredTerminalReason);

impl DeferredSystemCancellationReason {
    pub(crate) const CLAIM_DROPPED: Self = Self(DeferredTerminalReason::ClaimDropped);
    pub(crate) const OWNER_DEADLINE: Self = Self(DeferredTerminalReason::OwnerDeadline);
    pub(crate) const PARENT_CANCELLED: Self = Self(DeferredTerminalReason::ParentCancelled);
    pub(crate) const PROCESSOR_UNAVAILABLE: Self = Self(DeferredTerminalReason::ProcessorUnavailable);
    pub(crate) const SERVICE_STOPPING: Self = Self(DeferredTerminalReason::ServiceStopping);

    pub(crate) const fn terminal_reason(self) -> DeferredTerminalReason {
        self.0
    }
}

/// Sealed system-owned reasons that project to `Closed`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeferredSystemCloseReason(DeferredTerminalReason);

impl DeferredSystemCloseReason {
    pub(crate) const SESSION_CLOSED: Self = Self(DeferredTerminalReason::SessionClosed);

    pub(crate) const fn terminal_reason(self) -> DeferredTerminalReason {
        self.0
    }
}

#[derive(Clone, Copy)]
enum DeferredRequestCodeBucket {
    PullMessage,
    PopMessage,
    Notification,
    Other,
}

impl DeferredRequestCodeBucket {
    fn from_request_code(request_code: i32) -> Self {
        match RequestCode::from(request_code) {
            RequestCode::PullMessage => Self::PullMessage,
            RequestCode::PopMessage => Self::PopMessage,
            RequestCode::Notification => Self::Notification,
            _ => Self::Other,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::PullMessage => "pull_message",
            Self::PopMessage => "pop_message",
            Self::Notification => "notification",
            Self::Other => "other",
        }
    }
}

struct DeferredTerminalObserver {
    telemetry: TransportTelemetry,
    request_code: DeferredRequestCodeBucket,
}

impl DeferredTerminalObserver {
    #[cfg(test)]
    fn noop() -> Self {
        Self {
            telemetry: TransportTelemetry::noop(),
            request_code: DeferredRequestCodeBucket::Other,
        }
    }

    fn new(telemetry: TransportTelemetry, request_code: i32) -> Self {
        Self {
            telemetry,
            request_code: DeferredRequestCodeBucket::from_request_code(request_code),
        }
    }

    fn record(&self, reason: DeferredTerminalReason) {
        self.telemetry
            .record_deferred_terminal(self.request_code.as_str(), reason.as_str());
    }
}

/// One atomic owner for a deferred response lifecycle.
///
/// This state is allocated when request processing creates a deferred
/// responder. Inline responses continue to use their stack-owned slot.
pub(crate) struct ResponseState {
    state: AtomicU8,
    observer: DeferredTerminalObserver,
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
    AlreadyCompleted {
        state: ResponseTerminalState,
        reason: Option<DeferredTerminalReason>,
    },
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
/// Before entering any seam that might begin socket I/O, the owner must either
/// record possibly partial progress or delegate terminal ownership to the
/// canonical transport guard. Dropping an unfinished, non-delegated claim records
/// the most conservative progress reached by the owner.
#[must_use]
pub(crate) struct ResponseSendClaim {
    state: Arc<ResponseState>,
    drop_progress: WriteProgress,
    delegated: Option<Arc<AtomicBool>>,
    active: bool,
}

/// Deferred-state half of the canonical queued-write Drop completion.
pub(crate) struct DeferredTransportDropHandle {
    state: Arc<ResponseState>,
    delegated: Arc<AtomicBool>,
}

impl DeferredTransportDropHandle {
    pub(crate) fn finish_dropped(&self, progress: WriteProgress) {
        if self.delegated.load(Ordering::Acquire) {
            let _ = self.state.finish_sending(ResponseTerminalState::Failed { progress });
        }
    }
}

impl ResponseState {
    /// Creates the deferred-only state in `Open`.
    #[cfg(test)]
    pub(crate) fn open() -> Self {
        Self {
            state: AtomicU8::new(OPEN),
            observer: DeferredTerminalObserver::noop(),
        }
    }

    /// Creates the deferred state with its one terminal observation owner.
    pub(crate) fn observed(telemetry: TransportTelemetry, request_code: i32) -> Self {
        Self {
            state: AtomicU8::new(OPEN),
            observer: DeferredTerminalObserver::new(telemetry, request_code),
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

    /// Returns the exact non-response terminal reason selected by the atomic winner.
    pub(crate) fn terminal_reason(&self) -> Option<DeferredTerminalReason> {
        decode_terminal_reason(self.state.load(Ordering::Acquire))
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
                                delegated: None,
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
        self.stop_with_reason(DeferredTerminalReason::Explicit, ResponseTransition::Cancel, |_| {})
    }

    /// Closes a response that has not begun delivery.
    #[cfg(test)]
    pub(crate) fn close(&self) -> Result<(), ResponseStateError> {
        self.stop_with_reason(DeferredTerminalReason::SessionClosed, ResponseTransition::Close, |_| {})
    }

    pub(crate) fn cancel_with_reason(
        &self,
        reason: DeferredSystemCancellationReason,
    ) -> Result<(), ResponseStateError> {
        self.stop_with_reason(reason.0, ResponseTransition::Cancel, |_| {})
    }

    pub(crate) fn close_with_reason(&self, reason: DeferredSystemCloseReason) -> Result<(), ResponseStateError> {
        self.stop_with_reason(reason.0, ResponseTransition::Close, |_| {})
    }

    pub(super) fn cancel_receiver_dropped(&self) -> Result<(), ResponseStateError> {
        self.stop_with_reason(
            DeferredTerminalReason::ReceiverDropped,
            ResponseTransition::Close,
            |_| {},
        )
    }

    pub(super) fn cancel_abandoned(&self) -> Result<(), ResponseStateError> {
        self.stop_with_reason(DeferredTerminalReason::Abandoned, ResponseTransition::Cancel, |_| {})
    }

    fn stop_with_reason(
        &self,
        reason: DeferredTerminalReason,
        transition: ResponseTransition,
        mut before_compare: impl FnMut(u8),
    ) -> Result<(), ResponseStateError> {
        let terminal = encode_terminal_reason(reason);
        let mut observed = self.state.load(Ordering::Acquire);
        loop {
            match observed {
                OPEN | REGISTERED | CLAIMED => {
                    before_compare(observed);
                    match self
                        .state
                        .compare_exchange(observed, terminal, Ordering::AcqRel, Ordering::Acquire)
                    {
                        Ok(_) => {
                            self.observer.record(reason);
                            return Ok(());
                        }
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
    #[cfg(test)]
    pub(crate) fn mark_possibly_partial(&mut self) {
        self.drop_progress = WriteProgress::PossiblyPartial;
    }

    pub(crate) fn observe_transport_drop(&mut self, delegated: Arc<AtomicBool>) -> DeferredTransportDropHandle {
        self.delegated = Some(Arc::clone(&delegated));
        DeferredTransportDropHandle {
            state: Arc::clone(&self.state),
            delegated,
        }
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
        if self
            .delegated
            .as_ref()
            .is_some_and(|delegated| delegated.load(Ordering::Acquire))
        {
            self.active = false;
            return;
        }
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
        ResponseStateSnapshot::Terminal(state) => ResponseStateError::AlreadyCompleted {
            state,
            reason: decode_terminal_reason(actual),
        },
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
        CANCELLED_EXPLICIT
        | CANCELLED_ABANDONED
        | CANCELLED_CLAIM_DROPPED
        | CANCELLED_OWNER_DEADLINE
        | CANCELLED_PARENT_CANCELLED
        | CANCELLED_PROCESSOR_UNAVAILABLE
        | CANCELLED_SERVICE_STOPPING => ResponseStateSnapshot::Terminal(ResponseTerminalState::Cancelled),
        CLOSED_RECEIVER_DROPPED | CLOSED_SESSION_CLOSED => {
            ResponseStateSnapshot::Terminal(ResponseTerminalState::Closed)
        }
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
        ResponseTerminalState::Cancelled => CANCELLED_EXPLICIT,
        ResponseTerminalState::Closed => CLOSED_SESSION_CLOSED,
    }
}

const fn encode_terminal_reason(reason: DeferredTerminalReason) -> u8 {
    match reason {
        DeferredTerminalReason::Explicit => CANCELLED_EXPLICIT,
        DeferredTerminalReason::ReceiverDropped => CLOSED_RECEIVER_DROPPED,
        DeferredTerminalReason::Abandoned => CANCELLED_ABANDONED,
        DeferredTerminalReason::ClaimDropped => CANCELLED_CLAIM_DROPPED,
        DeferredTerminalReason::OwnerDeadline => CANCELLED_OWNER_DEADLINE,
        DeferredTerminalReason::ParentCancelled => CANCELLED_PARENT_CANCELLED,
        DeferredTerminalReason::ProcessorUnavailable => CANCELLED_PROCESSOR_UNAVAILABLE,
        DeferredTerminalReason::ServiceStopping => CANCELLED_SERVICE_STOPPING,
        DeferredTerminalReason::SessionClosed => CLOSED_SESSION_CLOSED,
    }
}

fn decode_terminal_reason(state: u8) -> Option<DeferredTerminalReason> {
    match state {
        CANCELLED_EXPLICIT => Some(DeferredTerminalReason::Explicit),
        CLOSED_RECEIVER_DROPPED => Some(DeferredTerminalReason::ReceiverDropped),
        CANCELLED_ABANDONED => Some(DeferredTerminalReason::Abandoned),
        CANCELLED_CLAIM_DROPPED => Some(DeferredTerminalReason::ClaimDropped),
        CANCELLED_OWNER_DEADLINE => Some(DeferredTerminalReason::OwnerDeadline),
        CANCELLED_PARENT_CANCELLED => Some(DeferredTerminalReason::ParentCancelled),
        CANCELLED_PROCESSOR_UNAVAILABLE => Some(DeferredTerminalReason::ProcessorUnavailable),
        CANCELLED_SERVICE_STOPPING => Some(DeferredTerminalReason::ServiceStopping),
        CLOSED_SESSION_CLOSED => Some(DeferredTerminalReason::SessionClosed),
        OPEN | REGISTERED | CLAIMED | SENDING | COMPLETED | FAILED_NOT_STARTED | FAILED_POSSIBLY_PARTIAL => None,
        _ => unreachable!("ResponseState stores only module-owned monotonic state tags"),
    }
}

#[cfg(test)]
#[path = "../../tests/unit/dispatch/deferred_response.rs"]
mod tests;
