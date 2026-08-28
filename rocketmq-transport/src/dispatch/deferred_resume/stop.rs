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

use tokio::time::Instant;

use crate::admission::AdmissionError;

use super::ResumeResult;
use crate::dispatch::deferred_registry::ClaimExecutionParts;
use crate::dispatch::deferred_registry::ClaimMarker;
use crate::dispatch::deferred_response::DeferredSystemCancellationReason;
use crate::dispatch::deferred_response::DeferredSystemCloseReason;
use crate::dispatch::ClaimedDeferred;
use crate::dispatch::DeferredResumeError;
use crate::dispatch::DeferredResumeErrorKind;
use crate::dispatch::DeferredTerminalReason;
use crate::dispatch::RequestControlView;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResumeStop {
    ParentCancelled,
    SessionClosed,
    OwnerDeadline,
    ProcessorUnavailable,
    ServiceStopping,
}

impl ResumeStop {
    pub(super) const fn terminal_reason(self) -> DeferredTerminalReason {
        match self {
            Self::ParentCancelled => DeferredTerminalReason::ParentCancelled,
            Self::SessionClosed => DeferredTerminalReason::SessionClosed,
            Self::OwnerDeadline => DeferredTerminalReason::OwnerDeadline,
            Self::ProcessorUnavailable => DeferredTerminalReason::ProcessorUnavailable,
            Self::ServiceStopping => DeferredTerminalReason::ServiceStopping,
        }
    }
}

#[derive(Clone)]
pub(super) struct ResumeStopView {
    control: Option<RequestControlView>,
    resume_cutoff: Option<Instant>,
    write_cutoff: Option<Instant>,
}

impl ResumeStopView {
    pub(super) fn new(control: RequestControlView, expiry: Option<crate::dispatch::DeferredExpiry>) -> Self {
        let owner_deadline = control.deadline().map(|deadline| deadline.instant());
        Self {
            resume_cutoff: expiry
                .and_then(crate::dispatch::DeferredExpiry::resume_cutoff)
                .or(owner_deadline),
            write_cutoff: expiry
                .and_then(crate::dispatch::DeferredExpiry::write_cutoff)
                .or(owner_deadline),
            control: Some(control),
        }
    }

    pub(super) fn from_execution_parts<R>(parts: &ClaimExecutionParts<R>) -> Self
    where
        R: Send + 'static,
    {
        let control = parts.responder.control().clone();
        let owner_deadline = control.deadline().map(|deadline| deadline.instant());
        let expiry = parts.expiry();
        let resume_cutoff = parts.resume_cutoff();
        let write_cutoff = parts.write_cutoff();
        debug_assert_eq!(
            resume_cutoff,
            expiry.and_then(crate::dispatch::DeferredExpiry::resume_cutoff)
        );
        debug_assert_eq!(
            write_cutoff,
            expiry.and_then(crate::dispatch::DeferredExpiry::write_cutoff)
        );
        Self {
            resume_cutoff: resume_cutoff.or(owner_deadline),
            write_cutoff: write_cutoff.or(owner_deadline),
            control: Some(control),
        }
    }

    #[cfg(test)]
    pub(super) const fn never() -> Self {
        Self {
            control: None,
            resume_cutoff: None,
            write_cutoff: None,
        }
    }

    pub(super) fn current_before_resume(&self) -> Option<ResumeStop> {
        self.current(self.resume_cutoff)
    }

    pub(super) fn current_before_write(&self) -> Option<ResumeStop> {
        self.current(self.write_cutoff)
    }

    fn current(&self, cutoff: Option<Instant>) -> Option<ResumeStop> {
        let Some(control) = &self.control else {
            return None;
        };
        if control.parent_is_cancelled() {
            Some(ResumeStop::ParentCancelled)
        } else if control.session_is_closed() {
            Some(ResumeStop::SessionClosed)
        } else if cutoff.is_some_and(|cutoff| Instant::now() >= cutoff) {
            Some(ResumeStop::OwnerDeadline)
        } else {
            None
        }
    }

    pub(super) async fn wait_before_resume(&self) -> ResumeStop {
        self.wait(self.resume_cutoff).await
    }

    pub(super) async fn wait_before_write(&self) -> ResumeStop {
        self.wait(self.write_cutoff).await
    }

    async fn wait(&self, cutoff: Option<Instant>) -> ResumeStop {
        let Some(control) = &self.control else {
            std::future::pending::<()>().await;
            unreachable!("a stop-free test view never wakes")
        };
        match cutoff {
            Some(cutoff) => {
                let _ = tokio::time::timeout_at(cutoff, control.parent_or_session_cancelled()).await;
            }
            None => control.parent_or_session_cancelled().await,
        }
        self.current(cutoff).unwrap_or(ResumeStop::OwnerDeadline)
    }
}

pub(super) fn finish_lifecycle<R>(
    id: crate::dispatch::DeferredId,
    request_id: crate::dispatch::RequestId,
    responder: crate::dispatch::DeferredResponder,
    marker: Arc<ClaimMarker<R>>,
    stop: ResumeStop,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
) -> ResumeResult
where
    R: Send + 'static,
{
    let (kind, result) = match stop {
        ResumeStop::ParentCancelled => (
            DeferredResumeErrorKind::Cancelled,
            responder.cancel_with_system_reason(DeferredSystemCancellationReason::PARENT_CANCELLED),
        ),
        ResumeStop::SessionClosed => (
            DeferredResumeErrorKind::SessionClosed,
            responder.close_with_reason(DeferredSystemCloseReason::SESSION_CLOSED),
        ),
        ResumeStop::OwnerDeadline => (
            DeferredResumeErrorKind::Cancelled,
            responder.cancel_with_system_reason(DeferredSystemCancellationReason::OWNER_DEADLINE),
        ),
        ResumeStop::ProcessorUnavailable => (
            DeferredResumeErrorKind::ExecutorClosing,
            responder.cancel_with_system_reason(DeferredSystemCancellationReason::PROCESSOR_UNAVAILABLE),
        ),
        ResumeStop::ServiceStopping => (
            DeferredResumeErrorKind::ExecutorClosing,
            responder.cancel_with_system_reason(DeferredSystemCancellationReason::SERVICE_STOPPING),
        ),
    };
    drop(marker);
    match result {
        Ok(()) => Err(DeferredResumeError::new_with_reason(
            kind,
            id,
            request_id,
            None,
            Some(stop.terminal_reason()),
            None,
            source,
        )),
        Err(terminal) => Err(DeferredResumeError::new_with_reason(
            terminal
                .prior_terminal_reason()
                .map_or(kind, resume_error_kind_for_reason),
            id,
            request_id,
            terminal.prior_terminal_state(),
            terminal.prior_terminal_reason(),
            terminal.write_progress(),
            Some(Box::new(terminal)),
        )),
    }
}

pub(super) fn finish_parts_stop<R>(
    parts: ClaimExecutionParts<R>,
    stop: ResumeStop,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
) -> ResumeResult
where
    R: Send + 'static,
{
    let ClaimExecutionParts {
        id,
        request_id,
        resume,
        responder,
        mut permit,
        marker,
        ..
    } = parts;
    let result = finish_lifecycle(id, request_id, responder, marker, stop, source);
    if let Some(permit) = permit.take() {
        permit.release();
    }
    drop(resume);
    result
}

pub(super) fn finish_parts_admission<R>(parts: ClaimExecutionParts<R>, source: AdmissionError) -> ResumeResult
where
    R: Send + 'static,
{
    let ClaimExecutionParts {
        id,
        request_id,
        resume,
        responder,
        mut permit,
        marker,
        ..
    } = parts;
    let terminal = responder.cancel_with_system_reason(DeferredSystemCancellationReason::PROCESSOR_UNAVAILABLE);
    drop(marker);
    if let Some(permit) = permit.take() {
        permit.release();
    }
    drop(resume);
    match terminal {
        Ok(()) => Err(DeferredResumeError::new_with_reason(
            DeferredResumeErrorKind::Admission,
            id,
            request_id,
            None,
            Some(DeferredTerminalReason::ProcessorUnavailable),
            None,
            Some(Box::new(source)),
        )),
        Err(terminal) => Err(DeferredResumeError::new_with_reason(
            terminal
                .prior_terminal_reason()
                .map_or(DeferredResumeErrorKind::Admission, resume_error_kind_for_reason),
            id,
            request_id,
            terminal.prior_terminal_state(),
            terminal.prior_terminal_reason(),
            terminal.write_progress(),
            Some(Box::new(terminal)),
        )),
    }
}

pub(super) fn finish_claimed_stop<R>(
    claimed: ClaimedDeferred<R>,
    stop: ResumeStop,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
) -> ResumeResult
where
    R: Send + 'static,
{
    finish_parts_stop(claimed.into_execution_parts(), stop, source)
}

pub(super) fn resume_error_kind_for_reason(reason: DeferredTerminalReason) -> DeferredResumeErrorKind {
    match reason {
        DeferredTerminalReason::SessionClosed | DeferredTerminalReason::ReceiverDropped => {
            DeferredResumeErrorKind::SessionClosed
        }
        DeferredTerminalReason::ProcessorUnavailable | DeferredTerminalReason::ServiceStopping => {
            DeferredResumeErrorKind::ExecutorClosing
        }
        DeferredTerminalReason::Explicit
        | DeferredTerminalReason::Abandoned
        | DeferredTerminalReason::ClaimDropped
        | DeferredTerminalReason::OwnerDeadline
        | DeferredTerminalReason::ParentCancelled => DeferredResumeErrorKind::Cancelled,
    }
}
