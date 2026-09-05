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
use super::RequestId;
use crate::dispatch::deferred_response::DeferredSystemCancellationReason;
use crate::dispatch::deferred_response::DeferredSystemCloseReason;
use crate::dispatch::deferred_session_cleanup::CleanupEnrollment;
use crate::dispatch::RemotingResponse;
use crate::dispatch::RequestControlView;
use crate::dispatch::ResponseReceipt;
use crate::dispatch::ResponseState;
use crate::dispatch::ResponseStateOutcome;
use crate::dispatch::ResponseStateSnapshot;
use crate::dispatch::ResponseTerminalState;
use crate::session_view::SessionId;

/// Reason that caused a deferred request to become eligible for resume.
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

/// Result of claiming one deferred request.
#[must_use]
pub enum DeferredClaimOutcome<R>
where
    R: Send + 'static,
{
    /// The caller acquired the affine request claim.
    Claimed(ClaimedDeferred<R>),
    /// No live registry entry exists for the identity.
    NotFound,
    /// Another live caller owns the claim.
    AlreadyClaimed,
    /// The response already reached a terminal state.
    AlreadyCompleted,
    /// The parent lifecycle was cancelled.
    ParentCancelled,
    /// The session was closed.
    SessionClosed,
    /// The immutable ingress deadline elapsed.
    DeadlineExpired,
}

/// Result of a deferred resume attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum DeferredResumeOutcome {
    /// Canonical response delivery completed.
    Completed(ResponseReceipt),
    /// The request lifecycle was cancelled.
    Cancelled,
    /// The owning session was closed.
    SessionClosed,
    /// Bounded processor admission rejected the resume.
    AdmissionRejected,
}

/// Notification result after Transport has resolved a deferred resume submission.
///
/// The private enqueue carrier retains the affine resume cell on rejection so
/// Transport can complete it before returning this source-free projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum DeferredResumeSubmitOutcome {
    /// The original session executor accepted the resume cell.
    Submitted,
    /// The request lifecycle was cancelled before submission.
    Cancelled,
    /// The owning session was closed before submission.
    SessionClosed,
    /// Bounded processor admission rejected the resume.
    AdmissionRejected,
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

pub(super) enum DeferredClaimRejection {
    NotFound,
    AlreadyClaimed,
    AlreadyCompleted,
    ParentCancelled,
    SessionClosed,
    DeadlineExpired,
    Operational(DeferredClaimOperationalFailure),
}

#[derive(Debug, thiserror::Error)]
#[error("deferred claim registry invariant failed")]
pub(super) struct DeferredClaimOperationalFailure {
    #[source]
    source: Option<crate::contract::TransportContractViolation>,
}

impl DeferredClaimOperationalFailure {
    pub(super) const fn invariant() -> Self {
        Self { source: None }
    }

    pub(super) const fn response(source: crate::contract::TransportContractViolation) -> Self {
        Self { source: Some(source) }
    }
}

/// Affine ownership of one claimed deferred request.
///
/// ```compile_fail
/// use rocketmq_transport::api::ClaimedDeferred;
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
    request: Option<Box<DeferredRequest<R>>>,
    marker: Option<Arc<ClaimMarker<R>>>,
}

impl<R> ClaimedDeferred<R>
where
    R: Send + 'static,
{
    pub(super) fn new(
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
            request: Some(Box::new(request)),
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
    /// Returns an operational transport error for retained-size contract
    /// violations, residual runtime failures, response construction, or
    /// response I/O. Lifecycle, bounded-admission, and known executor-closure
    /// states are source-free [`DeferredResumeOutcome`] variants.
    pub async fn resume<F, Fut>(
        self,
        handler_retained: DeferredResumeRetainedSize,
        handler: F,
    ) -> Result<DeferredResumeOutcome, crate::error::TransportError>
    where
        F: FnOnce(R, DeferredWakeReason) -> Fut + Send + 'static,
        Fut: Future<Output = RocketMQResult<RemotingResponse>> + Send + 'static,
    {
        crate::dispatch::deferred_resume::resume_claimed(self, handler_retained, handler).await
    }

    /// Submits this claim to the original session executor without waiting for
    /// handler execution or canonical response delivery.
    ///
    /// The terminal observer is owned by that session job and runs exactly once
    /// after response delivery reaches its canonical terminal. Submission
    /// failures invoke it synchronously before this method returns.
    ///
    /// # Errors
    ///
    /// Returns an operational transport error for retained-size contract
    /// violations or residual runtime failures. Lifecycle, bounded-admission,
    /// and known executor-closure states are source-free
    /// [`DeferredResumeSubmitOutcome`] variants.
    pub fn submit<F, Fut, O>(
        self,
        handler_retained: DeferredResumeRetainedSize,
        handler: F,
        terminal_observer: O,
    ) -> Result<DeferredResumeSubmitOutcome, crate::error::TransportError>
    where
        F: FnOnce(R, DeferredWakeReason) -> Fut + Send + 'static,
        Fut: Future<Output = RocketMQResult<RemotingResponse>> + Send + 'static,
        O: FnOnce(&Result<DeferredResumeOutcome, crate::error::TransportError>) + Send + 'static,
    {
        crate::dispatch::deferred_resume::submit_claimed(self, handler_retained, handler, terminal_observer)
    }

    pub(crate) fn take_request(&mut self) -> DeferredRequest<R> {
        *self.request.take().expect("claimed request remains owned")
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

    pub(in crate::dispatch) fn expiry(&self) -> Option<super::DeferredExpiry> {
        self.request
            .as_ref()
            .expect("claimed request remains owned")
            .parts
            .expiry()
    }

    #[cfg(test)]
    pub(in crate::dispatch) fn response_state_for_test(&self) -> Arc<ResponseState> {
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
        let expiry = parts.expiry();
        let (responder, permit) = parts.into_resume_parts();
        ClaimExecutionParts {
            id: self.id,
            request_id: self.request_id,
            reason: self.reason,
            resume,
            responder,
            permit: Some(permit),
            marker,
            expiry,
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
    expiry: Option<super::DeferredExpiry>,
}

impl<R> ClaimExecutionParts<R>
where
    R: Send + 'static,
{
    pub(in crate::dispatch) const fn expiry(&self) -> Option<super::DeferredExpiry> {
        self.expiry
    }

    pub(in crate::dispatch) fn resume_cutoff(&self) -> Option<tokio::time::Instant> {
        self.expiry.and_then(super::DeferredExpiry::resume_cutoff)
    }

    pub(in crate::dispatch) fn write_cutoff(&self) -> Option<tokio::time::Instant> {
        self.expiry.and_then(super::DeferredExpiry::write_cutoff)
    }
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
        if let Some(marker) = &self.marker {
            let _ = marker.terminalize_release();
        }
        if let Some(request) = self.request.take() {
            let DeferredRequest { resume, parts } = *request;
            let super::DeferredParts {
                mut responder,
                permit,
                expiry: _,
            } = parts;
            permit.release();
            drop(resume);
            let _ = responder.cleanup_cancel_with_reason(DeferredSystemCancellationReason::CLAIM_DROPPED);
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
    session_id: SessionId,
    control: RequestControlView,
    state: Arc<ResponseState>,
    expiry: Option<super::DeferredExpiry>,
    enrollment: Option<CleanupEnrollment>,
}

impl<R> ClaimMarker<R>
where
    R: Send + 'static,
{
    pub(super) fn new(
        registry: &Arc<RegistryInner<R>>,
        id: DeferredId,
        session_id: SessionId,
        control: RequestControlView,
        state: Arc<ResponseState>,
        expiry: Option<super::DeferredExpiry>,
        enrollment: Option<CleanupEnrollment>,
    ) -> Self {
        Self {
            registry: Arc::downgrade(registry),
            id,
            session_id,
            control,
            state,
            expiry,
            enrollment,
        }
    }

    pub(super) fn terminal_state(&self) -> Option<ResponseTerminalState> {
        self.state.terminal_state()
    }

    pub(super) fn close_session_response(
        &self,
    ) -> Result<ResponseStateOutcome, crate::contract::TransportContractViolation> {
        self.state.close_with_reason(DeferredSystemCloseReason::SESSION_CLOSED)
    }

    pub(super) fn cancel_parent_response(
        &self,
    ) -> Result<ResponseStateOutcome, crate::contract::TransportContractViolation> {
        self.state
            .cancel_with_reason(DeferredSystemCancellationReason::PARENT_CANCELLED)
    }

    pub(super) fn cancel_owner_response(
        &self,
    ) -> Result<ResponseStateOutcome, crate::contract::TransportContractViolation> {
        self.state
            .cancel_with_reason(DeferredSystemCancellationReason::OWNER_DEADLINE)
    }

    pub(super) const fn control(&self) -> &RequestControlView {
        &self.control
    }

    pub(super) fn response_snapshot(&self) -> ResponseStateSnapshot {
        self.state.snapshot()
    }

    fn terminalize_release(&self) -> Result<ResponseStateOutcome, crate::contract::TransportContractViolation> {
        if self.control.parent_is_cancelled() {
            self.cancel_parent_response()
        } else if self.control.session_is_closed() {
            self.close_session_response()
        } else if self
            .expiry
            .and_then(super::DeferredExpiry::resume_cutoff)
            .or_else(|| self.control.deadline().map(|deadline| deadline.instant()))
            .is_some_and(|cutoff| tokio::time::Instant::now() >= cutoff)
        {
            self.state
                .cancel_with_reason(DeferredSystemCancellationReason::OWNER_DEADLINE)
        } else {
            self.state
                .cancel_with_reason(DeferredSystemCancellationReason::CLAIM_DROPPED)
        }
    }
}

impl<R> Drop for ClaimMarker<R>
where
    R: Send + 'static,
{
    fn drop(&mut self) {
        let _ = self.terminalize_release();
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
}

pub(super) enum ClaimStart<R>
where
    R: Send + 'static,
{
    Claimed(ClaimedDeferred<R>),
    Wait(ClaimWaiter),
    Rejected(DeferredClaimRejection),
}

impl ClaimWaiter {
    pub(super) fn try_new(ticket: Arc<ClaimTicket>) -> Result<Self, ()> {
        ticket
            .live_waiters
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| count.checked_add(1))
            .map_err(|_| ())?;
        Ok(Self { ticket })
    }

    pub(super) fn epoch(&self) -> u64 {
        self.ticket.epoch()
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
