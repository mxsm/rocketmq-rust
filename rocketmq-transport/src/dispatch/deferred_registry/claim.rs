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

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;

use rocketmq_error::RocketMQResult;
use tokio::sync::Notify;

use super::internal::RegistryInner;
use super::DeferredId;
use super::DeferredRequest;
use super::DeferredResponseError;
use super::RequestId;
use crate::dispatch::deferred_session_cleanup::CleanupEnrollment;
use crate::dispatch::RequestControlView;
use crate::dispatch::ResponsePlan;
use crate::dispatch::ResponseReceipt;
use crate::dispatch::ResponseState;
use crate::dispatch::ResponseTerminalState;
use crate::dispatch::WriteProgress;
use crate::session_view::SessionId;

/// Reason that caused a deferred request to become eligible for resume.
///
/// ```compile_fail
/// use rocketmq_transport::api::v1::DeferredWakeReason;
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DeferredWakeReason {
    /// A matching message arrived.
    MessageArrived,
    /// The deferred business wait elapsed.
    Timeout,
    /// An operator or internal policy requested an immediate refresh.
    ForcedRefresh,
}

/// Caller-declared dynamic ownership retained while a resume handler runs.
///
/// The value is the peak additional ownership not represented by the handler
/// or returned future values themselves. The transport cannot validate this
/// declaration at runtime.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DeferredResumeRetainedSize {
    dynamic_bytes: usize,
}

impl DeferredResumeRetainedSize {
    /// Creates a declared dynamic resume charge.
    #[must_use]
    pub const fn new(dynamic_bytes: usize) -> Self {
        Self { dynamic_bytes }
    }

    /// Returns the declared peak additional ownership.
    #[must_use]
    pub const fn dynamic_bytes(self) -> usize {
        self.dynamic_bytes
    }
}

/// Stable category for a deferred claim failure.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DeferredClaimErrorKind {
    /// No live registry entry or transient claim marker exists.
    NotFound,
    /// Another live claimant owns the request.
    AlreadyClaimed,
    /// The response already reached a terminal state.
    AlreadyCompleted,
    /// The request's parent lifecycle was cancelled.
    ParentCancelled,
    /// The request's session was closed.
    SessionClosed,
    /// The immutable ingress deadline expired.
    DeadlineExpired,
    /// A sealed registry or response-state invariant was violated.
    RegistryInvariant,
}

impl DeferredClaimErrorKind {
    /// Returns a stable low-cardinality label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::AlreadyClaimed => "already_claimed",
            Self::AlreadyCompleted => "already_completed",
            Self::ParentCancelled => "parent_cancelled",
            Self::SessionClosed => "session_closed",
            Self::DeadlineExpired => "deadline_expired",
            Self::RegistryInvariant => "registry_invariant",
        }
    }
}

/// Typed, redacted failure to claim one deferred request.
pub struct DeferredClaimError {
    kind: DeferredClaimErrorKind,
    deferred_id: DeferredId,
    request_id: Option<RequestId>,
    terminal: Option<ResponseTerminalState>,
    source: Option<DeferredResponseError>,
}

impl DeferredClaimError {
    pub(super) const fn new(
        kind: DeferredClaimErrorKind,
        deferred_id: DeferredId,
        request_id: Option<RequestId>,
        terminal: Option<ResponseTerminalState>,
        source: Option<DeferredResponseError>,
    ) -> Self {
        Self {
            kind,
            deferred_id,
            request_id,
            terminal,
            source,
        }
    }

    /// Returns the stable failure category.
    #[must_use]
    pub const fn kind(&self) -> DeferredClaimErrorKind {
        self.kind
    }

    /// Returns the requested deferred identity.
    #[must_use]
    pub const fn deferred_id(&self) -> DeferredId {
        self.deferred_id
    }

    /// Returns the trusted request identity when one remained observable.
    #[must_use]
    pub const fn request_id(&self) -> Option<RequestId> {
        self.request_id
    }

    /// Returns the exact earlier terminal winner, when applicable.
    #[must_use]
    pub const fn prior_terminal_state(&self) -> Option<ResponseTerminalState> {
        self.terminal
    }
}

impl fmt::Debug for DeferredClaimError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeferredClaimError")
            .field("kind", &self.kind.as_str())
            .field("deferred_id", &self.deferred_id)
            .field("request_id", &self.request_id)
            .field("terminal", &self.terminal.map(ResponseTerminalState::as_str))
            .finish_non_exhaustive()
    }
}

impl fmt::Display for DeferredClaimError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "deferred claim error: {}", self.kind.as_str())
    }
}

impl Error for DeferredClaimError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_ref().map(|source| source as &(dyn Error + 'static))
    }
}

/// Stable category for deferred resume execution failures.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DeferredResumeErrorKind {
    /// The parent lifecycle was cancelled.
    Cancelled,
    /// The owning session was closed.
    SessionClosed,
    /// The weak session executor no longer accepts work.
    ExecutorClosing,
    /// Accepted task ownership ended before response completion.
    TaskTerminated,
    /// A bounded admission resource rejected the resume.
    Admission,
    /// Checked resume ownership accounting overflowed.
    RetainedSizeOverflow,
    /// Canonical response delivery failed.
    Response,
    /// A handler error could not be converted into a response plan.
    ResponsePlan,
}

impl DeferredResumeErrorKind {
    /// Returns a stable low-cardinality label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::SessionClosed => "session_closed",
            Self::ExecutorClosing => "executor_closing",
            Self::TaskTerminated => "task_terminated",
            Self::Admission => "admission",
            Self::RetainedSizeOverflow => "retained_size_overflow",
            Self::Response => "response",
            Self::ResponsePlan => "response_plan",
        }
    }
}

/// Typed, redacted failure from one deferred resume attempt.
pub struct DeferredResumeError {
    kind: DeferredResumeErrorKind,
    deferred_id: DeferredId,
    request_id: RequestId,
    terminal: Option<ResponseTerminalState>,
    progress: Option<WriteProgress>,
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl DeferredResumeError {
    pub(crate) fn new(
        kind: DeferredResumeErrorKind,
        deferred_id: DeferredId,
        request_id: RequestId,
        terminal: Option<ResponseTerminalState>,
        progress: Option<WriteProgress>,
        source: Option<Box<dyn Error + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            kind,
            deferred_id,
            request_id,
            terminal,
            progress,
            source,
        }
    }

    /// Returns the stable failure category.
    #[must_use]
    pub const fn kind(&self) -> DeferredResumeErrorKind {
        self.kind
    }

    /// Returns the claimed deferred identity.
    #[must_use]
    pub const fn deferred_id(&self) -> DeferredId {
        self.deferred_id
    }

    /// Returns the trusted request identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns an exact terminal winner when one was observed.
    #[must_use]
    pub const fn prior_terminal_state(&self) -> Option<ResponseTerminalState> {
        self.terminal
    }

    /// Returns the exact canonical write progress, when applicable.
    #[must_use]
    pub const fn write_progress(&self) -> Option<WriteProgress> {
        self.progress
    }
}

impl fmt::Debug for DeferredResumeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeferredResumeError")
            .field("kind", &self.kind.as_str())
            .field("deferred_id", &self.deferred_id)
            .field("request_id", &self.request_id)
            .field("terminal", &self.terminal.map(ResponseTerminalState::as_str))
            .field("progress", &self.progress.map(WriteProgress::as_str))
            .finish_non_exhaustive()
    }
}

impl fmt::Display for DeferredResumeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "deferred resume error: {}", self.kind.as_str())
    }
}

impl Error for DeferredResumeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|source| source as &(dyn Error + 'static))
    }
}

/// Affine ownership of one claimed deferred request.
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::ClaimedDeferred;
///
/// fn claimed_is_affine<R>(claimed: &ClaimedDeferred<R>) {
///     let _ = claimed.clone();
/// }
/// ```
#[must_use]
pub struct ClaimedDeferred<R>
where
    R: Send + 'static,
{
    id: DeferredId,
    request_id: RequestId,
    reason: DeferredWakeReason,
    request: Option<DeferredRequest<R>>,
    marker: Option<Arc<ClaimMarker<R>>>,
}

impl<R> ClaimedDeferred<R>
where
    R: Send + 'static,
{
    pub(super) const fn new(
        id: DeferredId,
        request_id: RequestId,
        reason: DeferredWakeReason,
        request: DeferredRequest<R>,
        marker: Arc<ClaimMarker<R>>,
    ) -> Self {
        Self {
            id,
            request_id,
            reason,
            request: Some(request),
            marker: Some(marker),
        }
    }

    /// Returns the claimed deferred identity.
    #[must_use]
    pub const fn deferred_id(&self) -> DeferredId {
        self.id
    }

    /// Returns the trusted original request identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the first coalesced wake reason.
    #[must_use]
    pub const fn reason(&self) -> DeferredWakeReason {
        self.reason
    }

    /// Returns the retained business resume data.
    #[must_use]
    pub fn resume_data(&self) -> &R {
        &self.request.as_ref().expect("claimed request remains owned").resume
    }

    /// Returns mutable retained business resume data.
    #[must_use]
    pub fn resume_data_mut(&mut self) -> &mut R {
        &mut self.request.as_mut().expect("claimed request remains owned").resume
    }

    /// Submits this claim through the original session's bounded execution tree.
    ///
    /// `handler_retained.dynamic_bytes()` declares the peak additional dynamic
    /// ownership retained by `handler`; the transport cannot verify it.
    ///
    /// # Errors
    ///
    /// Returns a typed failure for lifecycle stop, executor shutdown, bounded
    /// admission, retained-size overflow, response planning, or response I/O.
    pub async fn resume<F, Fut>(
        self,
        handler_retained: DeferredResumeRetainedSize,
        handler: F,
    ) -> Result<ResponseReceipt, DeferredResumeError>
    where
        F: FnOnce(R, DeferredWakeReason) -> Fut + Send + 'static,
        Fut: Future<Output = RocketMQResult<ResponsePlan>> + Send + 'static,
    {
        crate::dispatch::deferred_resume::resume_claimed(self, handler_retained, handler).await
    }

    pub(crate) fn take_request(&mut self) -> DeferredRequest<R> {
        self.request.take().expect("claimed request remains owned")
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.request
            .as_ref()
            .expect("claimed request remains owned")
            .retained_bytes()
    }

    pub(crate) fn resume_context(&self) -> Option<super::DeferredResumeContext> {
        self.request
            .as_ref()
            .expect("claimed request remains owned")
            .parts
            .responder
            .resume_context()
            .cloned()
    }

    pub(crate) fn control(&self) -> &RequestControlView {
        self.request
            .as_ref()
            .expect("claimed request remains owned")
            .parts
            .responder
            .control()
    }

    #[cfg(test)]
    pub(super) fn response_state_for_test(&self) -> Arc<ResponseState> {
        Arc::clone(
            self.request
                .as_ref()
                .expect("claimed request remains owned")
                .parts
                .responder
                .response_state(),
        )
    }

    pub(in crate::dispatch) fn disarm_marker(&mut self) -> Arc<ClaimMarker<R>> {
        self.marker.take().expect("claimed marker remains owned")
    }

    pub(in crate::dispatch) fn into_execution_parts(mut self) -> ClaimExecutionParts<R> {
        let request = self.take_request();
        let marker = self.disarm_marker();
        let (resume, parts) = request.into_resume_and_parts();
        let (responder, permit) = parts.into_resume_parts();
        ClaimExecutionParts {
            id: self.id,
            request_id: self.request_id,
            reason: self.reason,
            resume,
            responder,
            permit: Some(permit),
            marker,
        }
    }
}

pub(in crate::dispatch) struct ClaimExecutionParts<R>
where
    R: Send + 'static,
{
    pub(in crate::dispatch) id: DeferredId,
    pub(in crate::dispatch) request_id: RequestId,
    pub(in crate::dispatch) reason: DeferredWakeReason,
    pub(in crate::dispatch) resume: R,
    pub(in crate::dispatch) responder: super::DeferredResponder,
    pub(in crate::dispatch) permit: Option<super::DeferredWaitPermit>,
    pub(in crate::dispatch) marker: Arc<ClaimMarker<R>>,
}

impl<R> fmt::Debug for ClaimedDeferred<R>
where
    R: Send + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClaimedDeferred")
            .field("deferred_id", &self.id)
            .field("request_id", &self.request_id)
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

impl<R> Drop for ClaimedDeferred<R>
where
    R: Send + 'static,
{
    fn drop(&mut self) {
        if let Some(request) = self.request.take() {
            let DeferredRequest { resume, parts } = request;
            let super::DeferredParts { responder, permit } = parts;
            permit.release();
            drop(resume);
            drop(responder);
        }
        drop(self.marker.take());
    }
}

pub(in crate::dispatch) struct ClaimMarker<R>
where
    R: Send + 'static,
{
    registry: Weak<RegistryInner<R>>,
    id: DeferredId,
    request_id: RequestId,
    session_id: SessionId,
    control: RequestControlView,
    state: Arc<ResponseState>,
    enrollment: Option<CleanupEnrollment>,
}

impl<R> ClaimMarker<R>
where
    R: Send + 'static,
{
    pub(super) fn new(
        registry: &Arc<RegistryInner<R>>,
        id: DeferredId,
        request_id: RequestId,
        session_id: SessionId,
        control: RequestControlView,
        state: Arc<ResponseState>,
        enrollment: Option<CleanupEnrollment>,
    ) -> Self {
        Self {
            registry: Arc::downgrade(registry),
            id,
            request_id,
            session_id,
            control,
            state,
            enrollment,
        }
    }

    pub(super) const fn request_id(&self) -> RequestId {
        self.request_id
    }

    pub(super) fn terminal_state(&self) -> Option<ResponseTerminalState> {
        self.state.terminal_state()
    }

    pub(super) fn close_response(&self) -> Result<(), crate::dispatch::ResponseStateError> {
        self.state.close()
    }

    pub(super) fn cancel_response(&self) -> Result<(), crate::dispatch::ResponseStateError> {
        self.state.cancel()
    }

    pub(super) const fn control(&self) -> &RequestControlView {
        &self.control
    }
}

impl<R> Drop for ClaimMarker<R>
where
    R: Send + 'static,
{
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry.remove_claim_marker(self.id, self.session_id, self as *const Self);
        }
        drop(self.enrollment.take());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum TicketResolution {
    Pending = 0,
    Published = 1,
    RemovedNotFound = 2,
    RemovedParentCancelled = 3,
    RemovedSessionClosed = 4,
    RemovedDeadlineExpired = 5,
    RemovedInvariant = 6,
}

pub(super) struct ClaimTicket {
    epoch: u64,
    resolution: AtomicU8,
    changed: Notify,
    live_waiters: AtomicUsize,
}

impl ClaimTicket {
    pub(super) const fn new(epoch: u64) -> Self {
        Self {
            epoch,
            resolution: AtomicU8::new(TicketResolution::Pending as u8),
            changed: Notify::const_new(),
            live_waiters: AtomicUsize::new(0),
        }
    }

    pub(super) fn epoch(&self) -> u64 {
        self.epoch
    }

    pub(super) fn publish(&self, resolution: TicketResolution) {
        if self
            .resolution
            .compare_exchange(
                TicketResolution::Pending as u8,
                resolution as u8,
                Ordering::Release,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.changed.notify_waiters();
        }
    }

    pub(super) fn resolution(&self) -> TicketResolution {
        match self.resolution.load(Ordering::Acquire) {
            0 => TicketResolution::Pending,
            1 => TicketResolution::Published,
            2 => TicketResolution::RemovedNotFound,
            3 => TicketResolution::RemovedParentCancelled,
            4 => TicketResolution::RemovedSessionClosed,
            5 => TicketResolution::RemovedDeadlineExpired,
            6 => TicketResolution::RemovedInvariant,
            _ => TicketResolution::RemovedInvariant,
        }
    }

    pub(super) fn live_waiters(&self) -> usize {
        self.live_waiters.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn set_live_waiters(&self, count: usize) {
        self.live_waiters.store(count, Ordering::Release);
    }
}

pub(super) struct ClaimWaiter {
    ticket: Arc<ClaimTicket>,
    request_id: RequestId,
}

#[allow(
    clippy::large_enum_variant,
    reason = "the claimed request remains affine and unboxed; an extra allocation would be outside retained accounting"
)]
pub(super) enum ClaimStart<R>
where
    R: Send + 'static,
{
    Claimed(ClaimedDeferred<R>),
    Wait(ClaimWaiter),
    Error(DeferredClaimError),
}

impl ClaimWaiter {
    pub(super) fn try_new(ticket: Arc<ClaimTicket>, request_id: RequestId) -> Result<Self, ()> {
        ticket
            .live_waiters
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| count.checked_add(1))
            .map_err(|_| ())?;
        Ok(Self { ticket, request_id })
    }

    pub(super) fn epoch(&self) -> u64 {
        self.ticket.epoch()
    }

    pub(super) const fn request_id(&self) -> RequestId {
        self.request_id
    }

    pub(super) fn same_ticket(&self, ticket: &Arc<ClaimTicket>) -> bool {
        Arc::ptr_eq(&self.ticket, ticket)
    }

    pub(super) async fn wait(&self) -> TicketResolution {
        loop {
            let notified = self.ticket.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let resolution = self.ticket.resolution();
            if resolution != TicketResolution::Pending {
                return resolution;
            }
            notified.await;
        }
    }
}

impl Drop for ClaimWaiter {
    fn drop(&mut self) {
        let decremented = self
            .ticket
            .live_waiters
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| count.checked_sub(1));
        debug_assert!(decremented.is_ok(), "a live claim waiter owns one checked waiter count");
    }
}
