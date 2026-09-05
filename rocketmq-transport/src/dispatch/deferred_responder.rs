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

//! Affine ownership of one deferred response.

use std::fmt;
use std::sync::Arc;

use tracing::Instrument;

use super::deferred_response::DeferredSystemCancellationReason;
use super::deferred_response::DeferredSystemCloseReason;
use super::deferred_response::DeferredTerminalReason;
use super::OriginalRequestIdentity;
use super::RemotingResponse;
use super::RequestControlView;
use super::RequestId;
use super::ResponseCompletionOutcome;
use super::ResponseReceipt;
use super::ResponseSink;
use super::ResponseState;
use super::ResponseStateOutcome;
use super::WriteProgress;
use crate::admission::AdmissionClass;
use crate::contract::TransportContractViolation;
use crate::request_ordering::RequestOrdering;
use crate::session_executor::DeferredResumeExecutor;
use crate::session_view::SessionId;
use crate::telemetry::TransportTelemetry;

/// Result of transferring a request's affine deferred response right.
#[must_use]
pub enum DeferredResponderOutcome {
    /// The deferred response right was transferred.
    Taken(DeferredResponder),
    /// One-way ingress can never produce a response.
    OneWayRequest,
    /// This transport path has no durable deferred response owner.
    Unavailable,
    /// The request already transferred its deferred response right.
    AlreadyTaken,
    /// The handler outcome already consumed the response contract.
    OutcomeCompleted,
}

/// Result of consuming a deferred response capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum DeferredResponseOutcome {
    /// Canonical response delivery completed.
    Completed(ResponseReceipt),
    /// Another terminal operation already completed the response.
    AlreadyCompleted,
    /// The response deadline elapsed before writing.
    DeadlineExceeded,
    /// The request owner cancelled the response.
    Cancelled,
    /// The canonical response session closed.
    SessionClosed,
    /// The bounded response queue rejected the response.
    QueueSaturated,
}

/// Internal response result retaining the terminal winner needed by resume.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DeferredResponseAttempt {
    Completed(ResponseReceipt),
    AlreadyCompleted {
        state: super::ResponseTerminalState,
        reason: Option<DeferredTerminalReason>,
    },
    DeadlineExceeded,
    Cancelled,
    SessionClosed,
    QueueSaturated,
}

impl DeferredResponseAttempt {
    const fn public_outcome(self) -> DeferredResponseOutcome {
        match self {
            Self::Completed(receipt) => DeferredResponseOutcome::Completed(receipt),
            Self::AlreadyCompleted { .. } => DeferredResponseOutcome::AlreadyCompleted,
            Self::DeadlineExceeded => DeferredResponseOutcome::DeadlineExceeded,
            Self::Cancelled => DeferredResponseOutcome::Cancelled,
            Self::SessionClosed => DeferredResponseOutcome::SessionClosed,
            Self::QueueSaturated => DeferredResponseOutcome::QueueSaturated,
        }
    }
}

/// Caller-owned reason for cancelling a deferred response capability.
///
/// Trusted owner, session, processor, and service lifecycle reasons are sealed
/// inside the transport and cannot be selected through this API.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DeferredCancellationReason {
    /// The caller can prove that its response receiver was dropped.
    ReceiverDropped,
}

/// Trusted seed that retains the one canonical response sink and its composition telemetry.
pub(crate) struct DeferredResponseSeed {
    sink: ResponseSink,
    telemetry: TransportTelemetry,
    session_id: SessionId,
    control: RequestControlView,
    resume: Option<DeferredResumeContext>,
    session_cleanup: Option<super::DeferredSessionCleanupRegistration>,
    observation: Option<crate::telemetry::RequestObservation>,
}

#[derive(Clone)]
pub(crate) struct DeferredResumeContext {
    pub(crate) ordering: RequestOrdering,
    pub(crate) class: AdmissionClass,
    pub(crate) executor: DeferredResumeExecutor,
}

impl DeferredResponseSeed {
    pub(crate) const fn new(
        sink: ResponseSink,
        telemetry: TransportTelemetry,
        session_id: SessionId,
        control: RequestControlView,
    ) -> Self {
        Self {
            sink,
            telemetry,
            session_id,
            control,
            resume: None,
            session_cleanup: None,
            observation: None,
        }
    }

    pub(crate) fn with_resume_context(
        mut self,
        ordering: RequestOrdering,
        class: AdmissionClass,
        executor: DeferredResumeExecutor,
    ) -> Self {
        self.resume = Some(DeferredResumeContext {
            ordering,
            class,
            executor,
        });
        self
    }

    pub(crate) fn with_session_cleanup(mut self, session_cleanup: super::DeferredSessionCleanupRegistration) -> Self {
        self.session_cleanup = Some(session_cleanup);
        self
    }

    pub(crate) fn with_observation(mut self, observation: crate::telemetry::RequestObservation) -> Self {
        self.observation = Some(observation);
        self
    }

    pub(crate) fn into_responder(self, original: OriginalRequestIdentity) -> DeferredResponder {
        #[cfg(test)]
        {
            DEFERRED_STATE_ALLOCATIONS.with(|count| count.set(count.get() + 1));
            self.telemetry.record_deferred_state_construction();
        }
        let state = Arc::new(ResponseState::observed(self.telemetry, original.original_code()));
        DeferredResponder {
            original,
            sink: Some(self.sink),
            state,
            session_id: self.session_id,
            control: Box::new(self.control),
            resume: self.resume,
            session_cleanup: self.session_cleanup,
            observation: self.observation,
            active: true,
        }
    }
}

/// Affine capability to complete exactly one later response.
///
/// The capability exposes no channel, connection, session handle, cancellation
/// token, or arbitrary write API. Dropping it abandons only this response.
///
/// ```compile_fail
/// use rocketmq_transport::api::DeferredResponder;
///
/// fn responders_are_affine(responder: &DeferredResponder) {
///     let _: DeferredResponder = responder.clone();
/// }
/// ```
#[must_use]
pub struct DeferredResponder {
    original: OriginalRequestIdentity,
    sink: Option<ResponseSink>,
    state: Arc<ResponseState>,
    session_id: SessionId,
    control: Box<RequestControlView>,
    resume: Option<DeferredResumeContext>,
    session_cleanup: Option<super::DeferredSessionCleanupRegistration>,
    observation: Option<crate::telemetry::RequestObservation>,
    active: bool,
}

impl DeferredResponder {
    /// Returns the immutable process-local request identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.original.request_id()
    }

    /// Returns the trusted session identity that owns this response.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Returns the exact non-response terminal reason selected so far.
    #[must_use]
    pub fn terminal_reason(&self) -> Option<DeferredTerminalReason> {
        self.state.terminal_reason()
    }

    pub(crate) fn register(&self) -> Result<ResponseStateOutcome, TransportContractViolation> {
        self.state.register()
    }

    pub(crate) fn claim(&self) -> Result<ResponseStateOutcome, TransportContractViolation> {
        self.state.claim()
    }

    pub(crate) const fn control(&self) -> &RequestControlView {
        &self.control
    }

    pub(crate) fn resume_context(&self) -> Option<&DeferredResumeContext> {
        self.resume.as_ref()
    }

    pub(crate) fn response_state(&self) -> &Arc<ResponseState> {
        &self.state
    }

    pub(crate) fn request_span(&self) -> tracing::Span {
        self.observation
            .as_ref()
            .map_or_else(tracing::Span::none, crate::telemetry::RequestObservation::span)
    }

    pub(crate) fn take_session_cleanup(&mut self) -> Option<super::DeferredSessionCleanupRegistration> {
        self.session_cleanup.take()
    }

    pub(crate) fn session_cleanup(&self) -> Option<super::DeferredSessionCleanupRegistration> {
        self.session_cleanup.clone()
    }

    pub(crate) fn cleanup_close_with_reason(
        &mut self,
        reason: DeferredSystemCloseReason,
    ) -> Result<ResponseStateOutcome, TransportContractViolation> {
        let result = self.state.close_with_reason(reason);
        self.active = false;
        if matches!(result, Ok(ResponseStateOutcome::Applied(()))) {
            if let Some(observation) = &self.observation {
                observation.complete_cancelled(reason.terminal_reason());
            }
        }
        result
    }

    pub(crate) fn cleanup_cancel_with_reason(
        &mut self,
        reason: DeferredSystemCancellationReason,
    ) -> Result<ResponseStateOutcome, TransportContractViolation> {
        let result = self.state.cancel_with_reason(reason);
        self.active = false;
        if matches!(result, Ok(ResponseStateOutcome::Applied(()))) {
            if let Some(observation) = &self.observation {
                observation.complete_cancelled(reason.terminal_reason());
            }
        }
        result
    }

    pub(crate) const fn original_opaque(&self) -> i32 {
        self.original.original_opaque()
    }

    pub(crate) fn close_with_reason(
        mut self,
        reason: DeferredSystemCloseReason,
    ) -> Result<ResponseStateOutcome, TransportContractViolation> {
        let result = self.state.close_with_reason(reason);
        self.active = false;
        if matches!(result, Ok(ResponseStateOutcome::Applied(()))) {
            if let Some(observation) = &self.observation {
                observation.complete_cancelled(reason.terminal_reason());
            }
        }
        result
    }

    pub(crate) fn cancel_with_system_reason(
        mut self,
        reason: DeferredSystemCancellationReason,
    ) -> Result<ResponseStateOutcome, TransportContractViolation> {
        let result = self.state.cancel_with_reason(reason);
        self.active = false;
        if matches!(result, Ok(ResponseStateOutcome::Applied(()))) {
            if let Some(observation) = &self.observation {
                observation.complete_cancelled(reason.terminal_reason());
            }
        }
        result
    }

    /// Binds and delivers one response through the request's canonical response sink.
    ///
    /// # Errors
    ///
    /// Returns a typed, redacted error for immutable binding, encoding, or
    /// transport failure. Lifecycle, deadline, and capacity rejections are
    /// returned as source-free outcomes.
    pub async fn respond(
        self,
        response: RemotingResponse,
    ) -> Result<DeferredResponseOutcome, crate::error::TransportError> {
        self.respond_internal(response)
            .await
            .map(DeferredResponseAttempt::public_outcome)
    }

    pub(super) async fn respond_internal(
        self,
        response: RemotingResponse,
    ) -> Result<DeferredResponseAttempt, crate::error::TransportError> {
        let span = self.request_span();
        self.respond_in_request_span(response).instrument(span).await
    }

    async fn respond_in_request_span(
        mut self,
        response: RemotingResponse,
    ) -> Result<DeferredResponseAttempt, crate::error::TransportError> {
        let response_code = response.response_code();
        let body_kind = response.body_kind();
        let write_started = std::time::Instant::now();
        let mut claim = match self.state.begin_sending() {
            Ok(ResponseStateOutcome::Applied(claim)) => claim,
            Ok(ResponseStateOutcome::AlreadyCompleted { state, reason }) => {
                return Ok(DeferredResponseAttempt::AlreadyCompleted { state, reason })
            }
            Err(error) => return Err(crate::error::TransportError::response(error)),
        };
        self.active = false;
        let bound = match response.bind(self.original) {
            Ok(bound) => bound,
            Err(source) => {
                if let Some(outcome) = finish_deferred_claim(claim.fail(WriteProgress::NotStarted))? {
                    return Ok(outcome);
                }
                if let Some(observation) = &self.observation {
                    observation.complete_failure_without_kind(
                        crate::runtime::processor::ResponseObservationMode::Deferred,
                        Some(response_code),
                        Some(body_kind),
                        Some(WriteProgress::NotStarted),
                    );
                }
                return Err(crate::error::TransportError::response(source));
            }
        };
        let sink = self.sink.take().ok_or_else(|| {
            crate::error::TransportError::response(TransportContractViolation::DeferredResponseInvalidTransition {
                operation: "take_sink",
                state: "sending",
            })
        })?;
        let result = sink.send_deferred_response(bound, &mut claim).await;
        match result {
            Ok(response_outcome) => {
                let progress = response_completion_progress(response_outcome);
                let claim_result = match response_outcome {
                    ResponseCompletionOutcome::Completed(_) => claim.complete(),
                    _ => claim.fail(progress),
                };
                if let Some(outcome) = finish_deferred_claim(claim_result)? {
                    return Ok(outcome);
                }
                if let Some(observation) = &self.observation {
                    match response_outcome {
                        ResponseCompletionOutcome::Completed(receipt) => observation.complete_reply(
                            crate::runtime::processor::ResponseObservationMode::Deferred,
                            response_code,
                            body_kind,
                            write_started.elapsed(),
                            Ok(receipt),
                        ),
                        outcome => observation.complete_reply(
                            crate::runtime::processor::ResponseObservationMode::Deferred,
                            response_code,
                            body_kind,
                            write_started.elapsed(),
                            Err((Some(outcome), Some(progress))),
                        ),
                    }
                }
                Ok(deferred_response_outcome(response_outcome))
            }
            Err(error) => {
                let progress = error.write_progress();
                if let Some(outcome) = finish_deferred_claim(claim.fail(progress))? {
                    return Ok(outcome);
                }
                if let Some(observation) = &self.observation {
                    observation.complete_reply(
                        crate::runtime::processor::ResponseObservationMode::Deferred,
                        response_code,
                        body_kind,
                        write_started.elapsed(),
                        Err((None, Some(progress))),
                    );
                }
                Err(crate::error::TransportError::response(error))
            }
        }
    }

    /// Cancels only this deferred response right.
    ///
    /// # Errors
    ///
    /// Returns a transport error only for an invalid nonterminal transition.
    /// A prior terminal winner is the source-free
    /// [`DeferredResponseOutcome::AlreadyCompleted`] outcome.
    pub fn cancel(mut self) -> Result<DeferredResponseOutcome, crate::error::TransportError> {
        let result = self.state.cancel();
        self.active = false;
        if matches!(result, Ok(ResponseStateOutcome::Applied(()))) {
            if let Some(observation) = &self.observation {
                observation.complete_cancelled(DeferredTerminalReason::Explicit);
            }
        }
        match result {
            Ok(ResponseStateOutcome::Applied(())) => Ok(DeferredResponseOutcome::Cancelled),
            Ok(ResponseStateOutcome::AlreadyCompleted { .. }) => Ok(DeferredResponseOutcome::AlreadyCompleted),
            Err(error) => Err(crate::error::TransportError::response(error)),
        }
    }

    /// Cancels this deferred response after the caller-owned receiver is dropped.
    ///
    /// This method does not cancel the request, session, or parent lifecycle.
    ///
    /// # Errors
    ///
    /// Returns a transport error only for an invalid nonterminal transition.
    /// A prior terminal winner is the source-free
    /// [`DeferredResponseOutcome::AlreadyCompleted`] outcome.
    pub fn cancel_with_reason(
        mut self,
        reason: DeferredCancellationReason,
    ) -> Result<DeferredResponseOutcome, crate::error::TransportError> {
        let result = match reason {
            DeferredCancellationReason::ReceiverDropped => self.state.cancel_receiver_dropped(),
        };
        self.active = false;
        if matches!(result, Ok(ResponseStateOutcome::Applied(()))) {
            if let Some(observation) = &self.observation {
                observation.complete_cancelled(DeferredTerminalReason::ReceiverDropped);
            }
        }
        match result {
            Ok(ResponseStateOutcome::Applied(())) => Ok(DeferredResponseOutcome::Cancelled),
            Ok(ResponseStateOutcome::AlreadyCompleted { .. }) => Ok(DeferredResponseOutcome::AlreadyCompleted),
            Err(error) => Err(crate::error::TransportError::response(error)),
        }
    }
}

fn finish_deferred_claim(
    result: Result<ResponseStateOutcome, TransportContractViolation>,
) -> Result<Option<DeferredResponseAttempt>, crate::error::TransportError> {
    match result {
        Ok(ResponseStateOutcome::Applied(())) => Ok(None),
        Ok(ResponseStateOutcome::AlreadyCompleted { state, reason }) => {
            Ok(Some(DeferredResponseAttempt::AlreadyCompleted { state, reason }))
        }
        Err(error) => Err(crate::error::TransportError::response(error)),
    }
}

const fn response_completion_progress(outcome: ResponseCompletionOutcome) -> WriteProgress {
    match outcome {
        ResponseCompletionOutcome::Completed(_)
        | ResponseCompletionOutcome::AlreadyCompleted(_)
        | ResponseCompletionOutcome::DeadlineExpired
        | ResponseCompletionOutcome::Cancelled
        | ResponseCompletionOutcome::SessionClosed
        | ResponseCompletionOutcome::QueueSaturated => WriteProgress::NotStarted,
    }
}

const fn deferred_response_outcome(outcome: ResponseCompletionOutcome) -> DeferredResponseAttempt {
    match outcome {
        ResponseCompletionOutcome::Completed(receipt) => DeferredResponseAttempt::Completed(receipt),
        ResponseCompletionOutcome::AlreadyCompleted(state) => {
            DeferredResponseAttempt::AlreadyCompleted { state, reason: None }
        }
        ResponseCompletionOutcome::DeadlineExpired => DeferredResponseAttempt::DeadlineExceeded,
        ResponseCompletionOutcome::Cancelled => DeferredResponseAttempt::Cancelled,
        ResponseCompletionOutcome::SessionClosed => DeferredResponseAttempt::SessionClosed,
        ResponseCompletionOutcome::QueueSaturated => DeferredResponseAttempt::QueueSaturated,
    }
}

impl fmt::Debug for DeferredResponder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeferredResponder")
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}

impl Drop for DeferredResponder {
    fn drop(&mut self) {
        if self.active && self.state.cancel_abandoned().is_ok() {
            if let Some(observation) = &self.observation {
                observation.complete_cancelled(DeferredTerminalReason::Abandoned);
            }
        }
        self.active = false;
    }
}

#[cfg(test)]
thread_local! {
    static DEFERRED_STATE_ALLOCATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn deferred_state_allocations() -> usize {
    DEFERRED_STATE_ALLOCATIONS.with(std::cell::Cell::get)
}

#[cfg(test)]
#[path = "../../tests/unit/dispatch/deferred_responder.rs"]
mod tests;
