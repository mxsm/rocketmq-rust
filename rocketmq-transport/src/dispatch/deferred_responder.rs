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

//! Affine ownership of one deferred V2 response.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use super::OriginalRequestIdentity;
use super::RequestControlView;
use super::RequestId;
use super::ResponseBindingError;
use super::ResponseError;
use super::ResponseErrorKind;
use super::ResponsePlan;
use super::ResponsePlanError;
use super::ResponseReceipt;
use super::ResponseSink;
use super::ResponseState;
use super::ResponseStateError;
use super::ResponseTerminalState;
use super::WriteProgress;
use crate::admission::AdmissionClass;
use crate::request_ordering::RequestOrdering;
use crate::session_executor::DeferredResumeExecutor;
use crate::session_view::SessionId;
use crate::telemetry::TransportTelemetry;

/// Stable reason that a request cannot transfer its deferred response right.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TakeDeferredResponderError {
    /// One-way ingress can never produce a response.
    #[error("one-way requests cannot take a deferred responder")]
    OneWayRequest,
    /// This transport path has no durable deferred response owner.
    #[error("a deferred responder is unavailable for this request")]
    Unavailable,
    /// The request already transferred its deferred response right.
    #[error("the deferred responder was already taken")]
    AlreadyTaken,
    /// The handler outcome already consumed the response contract.
    #[error("the request outcome was already completed")]
    OutcomeCompleted,
}

/// Stable category for a deferred response failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum DeferredResponseErrorKind {
    /// A prior terminal operation already completed this response.
    AlreadyCompleted,
    /// The attempted operation was invalid for a non-terminal state.
    InvalidTransition,
    /// Immutable request binding rejected the response plan.
    Binding,
    /// The immutable response deadline elapsed before completion.
    DeadlineExceeded,
    /// The request owner cancelled the response.
    Cancelled,
    /// The canonical response session closed.
    SessionClosed,
    /// The bounded canonical queue rejected the response.
    QueueSaturated,
    /// Canonical response encoding failed.
    Encode,
    /// The canonical response transport failed.
    Transport,
}

impl DeferredResponseErrorKind {
    /// Returns the stable low-cardinality category label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AlreadyCompleted => "already_completed",
            Self::InvalidTransition => "invalid_transition",
            Self::Binding => "binding",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::Cancelled => "cancelled",
            Self::SessionClosed => "session_closed",
            Self::QueueSaturated => "queue_saturated",
            Self::Encode => "encode",
            Self::Transport => "transport",
        }
    }
}

enum DeferredResponseErrorSource {
    State(ResponseStateError),
    Binding(ResponseBindingError),
    Plan(ResponsePlanError),
    Response(ResponseError),
}

/// Typed, redacted failure from consuming a [`DeferredResponder`].
///
/// Display and Debug expose only stable lifecycle metadata. The original typed
/// cause remains available through [`Error::source`] without formatting request
/// bodies, commands, principals, tokens, opaque values, or session details.
pub struct DeferredResponseError {
    source: DeferredResponseErrorSource,
}

impl DeferredResponseError {
    pub(crate) const fn from_state(source: ResponseStateError) -> Self {
        Self {
            source: DeferredResponseErrorSource::State(source),
        }
    }

    pub(crate) const fn from_binding(source: ResponseBindingError) -> Self {
        Self {
            source: DeferredResponseErrorSource::Binding(source),
        }
    }

    pub(crate) const fn from_plan(source: ResponsePlanError) -> Self {
        Self {
            source: DeferredResponseErrorSource::Plan(source),
        }
    }

    pub(crate) const fn from_response(source: ResponseError) -> Self {
        Self {
            source: DeferredResponseErrorSource::Response(source),
        }
    }

    /// Returns this error's stable low-cardinality category.
    #[must_use]
    pub const fn kind(&self) -> DeferredResponseErrorKind {
        match &self.source {
            DeferredResponseErrorSource::State(ResponseStateError::AlreadyCompleted { .. })
            | DeferredResponseErrorSource::Response(ResponseError::AlreadyCompleted { .. }) => {
                DeferredResponseErrorKind::AlreadyCompleted
            }
            DeferredResponseErrorSource::State(ResponseStateError::InvalidTransition { .. }) => {
                DeferredResponseErrorKind::InvalidTransition
            }
            DeferredResponseErrorSource::Binding(_) => DeferredResponseErrorKind::Binding,
            DeferredResponseErrorSource::Plan(_) => DeferredResponseErrorKind::Encode,
            DeferredResponseErrorSource::Response(error) => match error.kind() {
                ResponseErrorKind::AlreadyCompleted => DeferredResponseErrorKind::AlreadyCompleted,
                ResponseErrorKind::DeadlineExceeded => DeferredResponseErrorKind::DeadlineExceeded,
                ResponseErrorKind::Cancelled => DeferredResponseErrorKind::Cancelled,
                ResponseErrorKind::SessionClosed => DeferredResponseErrorKind::SessionClosed,
                ResponseErrorKind::QueueSaturated => DeferredResponseErrorKind::QueueSaturated,
                ResponseErrorKind::Encode => DeferredResponseErrorKind::Encode,
                ResponseErrorKind::Transport => DeferredResponseErrorKind::Transport,
            },
        }
    }

    /// Returns the exact write progress associated with this failure.
    #[must_use]
    pub const fn write_progress(&self) -> Option<WriteProgress> {
        match &self.source {
            DeferredResponseErrorSource::State(ResponseStateError::AlreadyCompleted { .. })
            | DeferredResponseErrorSource::Response(ResponseError::AlreadyCompleted { .. }) => None,
            DeferredResponseErrorSource::State(ResponseStateError::InvalidTransition { .. })
            | DeferredResponseErrorSource::Binding(_)
            | DeferredResponseErrorSource::Plan(_) => Some(WriteProgress::NotStarted),
            DeferredResponseErrorSource::Response(error) => error.write_progress(),
        }
    }

    /// Returns whether response policy may consider retrying this failure.
    #[must_use]
    pub const fn retryable(&self) -> bool {
        match &self.source {
            DeferredResponseErrorSource::Response(error) => error.retryable(),
            DeferredResponseErrorSource::State(_)
            | DeferredResponseErrorSource::Binding(_)
            | DeferredResponseErrorSource::Plan(_) => false,
        }
    }

    /// Returns the exact prior terminal winner reported by a duplicate operation.
    #[must_use]
    pub const fn prior_terminal_state(&self) -> Option<ResponseTerminalState> {
        match &self.source {
            DeferredResponseErrorSource::State(ResponseStateError::AlreadyCompleted { state })
            | DeferredResponseErrorSource::Response(ResponseError::AlreadyCompleted { state }) => Some(*state),
            DeferredResponseErrorSource::State(ResponseStateError::InvalidTransition { .. })
            | DeferredResponseErrorSource::Binding(_)
            | DeferredResponseErrorSource::Plan(_)
            | DeferredResponseErrorSource::Response(_) => None,
        }
    }
}

impl fmt::Debug for DeferredResponseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeferredResponseError")
            .field("kind", &self.kind().as_str())
            .field("write_progress", &self.write_progress().map(WriteProgress::as_str))
            .field(
                "prior_terminal_state",
                &self.prior_terminal_state().map(ResponseTerminalState::as_str),
            )
            .finish()
    }
}

impl fmt::Display for DeferredResponseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "deferred response error: {}", self.kind().as_str())?;
        if let Some(progress) = self.write_progress() {
            write!(formatter, " (progress={})", progress.as_str())?;
        }
        if let Some(state) = self.prior_terminal_state() {
            write!(formatter, " (prior_terminal={})", state.as_str())?;
        }
        Ok(())
    }
}

impl Error for DeferredResponseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(match &self.source {
            DeferredResponseErrorSource::State(source) => source,
            DeferredResponseErrorSource::Binding(source) => source,
            DeferredResponseErrorSource::Plan(source) => source,
            DeferredResponseErrorSource::Response(source) => source,
        })
    }
}

pub(crate) struct CanonicalDeferredDeadlineResponse<T> {
    value: T,
}

impl<T> CanonicalDeferredDeadlineResponse<T> {
    fn new(value: T) -> Self {
        Self { value }
    }

    pub(crate) const fn value(&self) -> &T {
        &self.value
    }

    pub(crate) fn try_map<U, E>(
        self,
        map: impl FnOnce(T) -> Result<U, E>,
    ) -> Result<CanonicalDeferredDeadlineResponse<U>, E> {
        map(self.value).map(|value| CanonicalDeferredDeadlineResponse { value })
    }

    pub(crate) fn into_inner(self) -> T {
        self.value
    }
}

/// Trusted seed that retains the one canonical plan sink and its composition telemetry.
pub(crate) struct DeferredResponseSeed {
    sink: ResponseSink,
    telemetry: TransportTelemetry,
    session_id: SessionId,
    control: RequestControlView,
    resume: Option<DeferredResumeContext>,
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

    pub(crate) fn into_responder(self, original: OriginalRequestIdentity) -> DeferredResponder {
        #[cfg(test)]
        DEFERRED_STATE_ALLOCATIONS.with(|count| count.set(count.get() + 1));
        DeferredResponder {
            original,
            sink: Some(self.sink),
            state: Arc::new(ResponseState::open()),
            telemetry: self.telemetry,
            session_id: self.session_id,
            control: self.control,
            resume: self.resume,
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
/// use rocketmq_transport::api::v2::DeferredResponder;
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
    telemetry: TransportTelemetry,
    session_id: SessionId,
    #[allow(dead_code, reason = "DEF-04 consumes the retained canonical request control")]
    control: RequestControlView,
    resume: Option<DeferredResumeContext>,
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

    #[allow(dead_code, reason = "DEF-04 consumes this private registry transition")]
    pub(crate) fn register(&self) -> Result<(), DeferredResponseError> {
        self.state.register().map_err(DeferredResponseError::from_state)
    }

    #[allow(dead_code, reason = "DEF-04 consumes this private resume-claim transition")]
    pub(crate) fn claim(&self) -> Result<(), DeferredResponseError> {
        self.state.claim().map_err(DeferredResponseError::from_state)
    }

    #[allow(dead_code, reason = "DEF-04 consumes the retained canonical request control")]
    pub(crate) const fn control(&self) -> &RequestControlView {
        &self.control
    }

    pub(crate) fn resume_context(&self) -> Option<&DeferredResumeContext> {
        self.resume.as_ref()
    }

    pub(crate) fn response_state(&self) -> &Arc<ResponseState> {
        &self.state
    }

    pub(crate) const fn original_opaque(&self) -> i32 {
        self.original.original_opaque()
    }

    pub(crate) fn close(mut self) -> Result<(), DeferredResponseError> {
        let result = self.state.close().map_err(DeferredResponseError::from_state);
        self.active = false;
        result
    }

    pub(crate) async fn respond_deadline(mut self) -> Result<ResponseReceipt, DeferredResponseError> {
        let mut claim = self.state.begin_sending().map_err(DeferredResponseError::from_state)?;
        self.active = false;
        let plan = match ResponsePlan::command(super::authorized_dispatcher::deadline_response(
            self.original.original_opaque(),
        )) {
            Ok(plan) => plan,
            Err(source) => {
                claim
                    .fail(WriteProgress::NotStarted)
                    .map_err(DeferredResponseError::from_state)?;
                return Err(DeferredResponseError::from_plan(source));
            }
        };
        let bound = match plan.bind(self.original) {
            Ok(bound) => bound,
            Err(source) => {
                claim
                    .fail(WriteProgress::NotStarted)
                    .map_err(DeferredResponseError::from_state)?;
                return Err(DeferredResponseError::from_binding(source));
            }
        };
        let deadline = CanonicalDeferredDeadlineResponse::new(bound);
        let sink = self.sink.take().ok_or_else(|| {
            DeferredResponseError::from_state(ResponseStateError::InvalidTransition {
                transition: super::deferred_response::ResponseTransition::BeginSending,
                state: super::ResponseStateSnapshot::Sending,
            })
        })?;
        let result = sink.send_deferred_deadline(deadline, &mut claim).await;
        match result {
            Ok(receipt) => {
                claim.complete().map_err(DeferredResponseError::from_state)?;
                Ok(receipt)
            }
            Err(error) => {
                let progress = error.write_progress().unwrap_or(WriteProgress::NotStarted);
                claim.fail(progress).map_err(DeferredResponseError::from_state)?;
                Err(DeferredResponseError::from_response(error))
            }
        }
    }

    /// Binds and delivers one response through the request's canonical plan sink.
    ///
    /// # Errors
    ///
    /// Returns a typed, redacted error for lifecycle, immutable binding,
    /// encoding, queue, cancellation, deadline, session, or transport failure.
    pub async fn respond(mut self, plan: ResponsePlan) -> Result<ResponseReceipt, DeferredResponseError> {
        let mut claim = self.state.begin_sending().map_err(DeferredResponseError::from_state)?;
        self.active = false;
        let bound = match plan.bind(self.original) {
            Ok(bound) => bound,
            Err(source) => {
                claim
                    .fail(WriteProgress::NotStarted)
                    .map_err(DeferredResponseError::from_state)?;
                return Err(DeferredResponseError::from_binding(source));
            }
        };
        let sink = self.sink.take().ok_or_else(|| {
            DeferredResponseError::from_state(ResponseStateError::InvalidTransition {
                transition: super::deferred_response::ResponseTransition::BeginSending,
                state: super::ResponseStateSnapshot::Sending,
            })
        })?;
        let result = sink.send_deferred_plan(bound, &mut claim).await;
        match result {
            Ok(receipt) => {
                claim.complete().map_err(DeferredResponseError::from_state)?;
                Ok(receipt)
            }
            Err(error) => {
                let progress = error.write_progress().unwrap_or(WriteProgress::NotStarted);
                claim.fail(progress).map_err(DeferredResponseError::from_state)?;
                Err(DeferredResponseError::from_response(error))
            }
        }
    }

    /// Cancels only this deferred response right.
    ///
    /// # Errors
    ///
    /// Returns a typed prior-terminal or invalid-transition failure when
    /// another lifecycle owner has already won.
    pub fn cancel(mut self) -> Result<(), DeferredResponseError> {
        let result = self.state.cancel().map_err(DeferredResponseError::from_state);
        self.active = false;
        if result.is_ok() {
            self.telemetry.record_lifecycle_event("deferred_response", "explicit");
        }
        result
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
        if self.active && self.state.cancel().is_ok() {
            self.telemetry.record_lifecycle_event("deferred_response", "abandoned");
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
#[path = "deferred_responder/tests.rs"]
mod tests;
