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

//! Transactional ownership for deferred requests.

use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use super::deferred_expiry::DeferredExpiry;
use super::deferred_expiry::DeferredExpiryMargins;
use super::deferred_expiry::DeferredExpiryOutcome;
use super::deferred_expiry::ExpiryRejection;
use super::deferred_responder::DeferredResumeContext;
use super::deferred_response::DeferredSystemCancellationReason;
use super::deferred_response::DeferredSystemCloseReason;
use super::DeferredResponder;
use super::DeferredRetainedSize;
use super::DeferredRetainedSizeParts;
use super::DeferredWaitPermit;
use super::RequestControlView;
use super::RequestId;
use crate::session_view::SessionId;

mod claim;
mod errors;
mod expiry;
mod internal;

pub(in crate::dispatch) use claim::ClaimExecutionParts;
pub(in crate::dispatch) use claim::ClaimMarker;
use claim::ClaimStart;
use claim::ClaimTicket;
use claim::ClaimWaiter;
pub use claim::ClaimedDeferred;
use claim::DeferredClaimOperationalFailure;
pub use claim::DeferredClaimOutcome;
use claim::DeferredClaimRejection;
pub use claim::DeferredResumeOutcome;
pub use claim::DeferredResumeRetainedSize;
pub use claim::DeferredResumeSubmitOutcome;
pub use claim::DeferredWakeReason;
use claim::TicketResolution;
pub use errors::DeferredRegistryOutcome;
pub use errors::DeferredRegistryRecovery;
pub(in crate::dispatch) use errors::RegistryFailure;
pub use expiry::DeferredExpiryBatch;
pub use expiry::DeferredExpiryBatchStats;
use internal::lifecycle_stop_with_expiry;
use internal::registry_additional_bytes;
use internal::validate_retained_floor;
use internal::BuildTransaction;
pub(crate) use internal::DeferredCommitError;
pub(crate) use internal::DeferredCommitErrorKind;
use internal::RegistrationOwner;
use internal::RegistrationOwnerImpl;
pub(in crate::dispatch) use internal::RegistryInner;
#[cfg(test)]
use internal::TestRegistrationOwner;

/// Opaque process-local identity for one deferred registry entry.
///
/// Zero and `u64::MAX` are never allocated. The allocator permanently stops
/// after returning `u64::MAX - 1`; identifiers are never reused.
///
/// ```compile_fail
/// use rocketmq_transport::api::DeferredId;
///
/// let forged = DeferredId(7);
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DeferredId(u64);

impl DeferredId {
    #[cfg(test)]
    pub(crate) const fn for_test(value: u64) -> Self {
        Self(value)
    }
}

/// Affine response ownership admitted for deferred storage.
///
/// This value deliberately excludes resume data so a fallible business-index
/// builder can run after the response capability and retained-byte permit have
/// been admitted, but before the registry publishes a prepared request.
///
/// ```compile_fail
/// use rocketmq_transport::api::DeferredParts;
///
/// fn parts_are_affine(parts: &DeferredParts) -> DeferredParts {
///     parts.clone()
/// }
/// ```
#[must_use]
pub struct DeferredParts {
    responder: DeferredResponder,
    permit: DeferredWaitPermit,
    expiry: Option<DeferredExpiry>,
}

impl DeferredParts {
    /// Joins the canonical response capability with its retained-byte permit.
    pub const fn new(responder: DeferredResponder, permit: DeferredWaitPermit) -> Self {
        Self {
            responder,
            permit,
            expiry: None,
        }
    }

    /// Attaches a checked protocol expiry derived from this responder's canonical owner.
    ///
    /// # Errors
    ///
    /// The parts remain owned by the caller on every outcome and contract
    /// violation.
    pub fn try_with_expiry(
        &mut self,
        protocol_at: tokio::time::Instant,
        margins: DeferredExpiryMargins,
    ) -> Result<DeferredExpiryOutcome, crate::contract::TransportContractViolation> {
        if self.expiry.is_some() {
            return Ok(DeferredExpiryOutcome::AlreadyAttached);
        }
        margins.validate()?;
        match DeferredExpiry::try_from_control(self.control(), protocol_at, margins) {
            Ok(expiry) => {
                self.expiry = Some(expiry);
                Ok(DeferredExpiryOutcome::Attached)
            }
            Err(ExpiryRejection::OwnerBudgetInsufficient) => Ok(DeferredExpiryOutcome::OwnerBudgetInsufficient),
            Err(ExpiryRejection::ProtocolAlreadyExpired) => Ok(DeferredExpiryOutcome::ProtocolAlreadyExpired),
            Err(ExpiryRejection::OwnerAlreadyExpired) => Ok(DeferredExpiryOutcome::OwnerAlreadyExpired),
        }
    }

    /// Returns the immutable request identity owned by the responder.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.responder.request_id()
    }

    /// Returns the trusted session identity owned by the responder.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.responder.session_id()
    }

    /// Returns the exact retained bytes owned by the permit.
    #[must_use]
    pub const fn retained_bytes(&self) -> usize {
        self.permit.retained_bytes()
    }

    /// Releases wait admission and returns the response capability for resume.
    pub fn into_responder(self) -> DeferredResponder {
        let Self {
            responder,
            permit,
            expiry: _,
        } = self;
        permit.release();
        responder
    }

    const fn control(&self) -> &RequestControlView {
        self.responder.control()
    }

    pub(in crate::dispatch) const fn expiry(&self) -> Option<DeferredExpiry> {
        self.expiry
    }

    fn response_state(&self) -> Arc<super::ResponseState> {
        Arc::clone(self.responder.response_state())
    }

    fn session_cleanup(&self) -> Option<super::DeferredSessionCleanupRegistration> {
        self.responder.session_cleanup()
    }

    fn clear_session_cleanup(&mut self) {
        drop(self.responder.take_session_cleanup());
    }

    fn cleanup_lifecycle(&mut self, kind: RegistryFailure) {
        let _ = match kind {
            RegistryFailure::ParentCancelled => self
                .responder
                .cleanup_cancel_with_reason(DeferredSystemCancellationReason::PARENT_CANCELLED),
            RegistryFailure::SessionClosed => self
                .responder
                .cleanup_close_with_reason(DeferredSystemCloseReason::SESSION_CLOSED),
            RegistryFailure::DeadlineExpired => self
                .responder
                .cleanup_cancel_with_reason(DeferredSystemCancellationReason::OWNER_DEADLINE),
            RegistryFailure::DuplicateRequest
            | RegistryFailure::IdentityExhausted
            | RegistryFailure::CleanupInstallerRejected
            | RegistryFailure::RegistryInvariant => return,
        };
    }

    pub(crate) fn into_resume_parts(self) -> (DeferredResponder, DeferredWaitPermit) {
        (self.responder, self.permit)
    }
}

impl fmt::Debug for DeferredParts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeferredParts")
            .field("request_id", &self.request_id())
            .field("session_id", &self.session_id())
            .field("retained_bytes", &self.retained_bytes())
            .finish_non_exhaustive()
    }
}

/// One resume value and its affine deferred response ownership.
///
/// ```compile_fail
/// use rocketmq_transport::api::DeferredRequest;
///
/// fn requests_are_affine(request: &DeferredRequest<String>) -> DeferredRequest<String> {
///     request.clone()
/// }
/// ```
#[must_use]
pub struct DeferredRequest<R> {
    resume: R,
    parts: DeferredParts,
}

impl<R> DeferredRequest<R> {
    /// Creates a request value ready for transactional registration.
    pub const fn new(resume: R, parts: DeferredParts) -> Self {
        Self { resume, parts }
    }

    /// Returns the immutable request identity owned by this request.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.parts.request_id()
    }

    /// Returns the trusted session identity owned by this request.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.parts.session_id()
    }

    /// Returns the admitted retained-byte charge.
    #[must_use]
    pub const fn retained_bytes(&self) -> usize {
        self.parts.retained_bytes()
    }

    /// Returns the business-owned resume value.
    #[must_use]
    pub const fn resume(&self) -> &R {
        &self.resume
    }

    /// Returns the mutable business-owned resume value.
    #[must_use]
    pub fn resume_mut(&mut self) -> &mut R {
        &mut self.resume
    }

    /// Separates resume data from affine response ownership.
    pub fn into_resume_and_parts(self) -> (R, DeferredParts) {
        (self.resume, self.parts)
    }

    const fn control(&self) -> &RequestControlView {
        self.parts.control()
    }

    const fn expiry(&self) -> Option<DeferredExpiry> {
        self.parts.expiry()
    }

    fn register_response(&self) -> Result<super::ResponseStateOutcome, crate::contract::TransportContractViolation> {
        self.parts.responder.register()
    }
}

impl<R> fmt::Debug for DeferredRequest<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeferredRequest")
            .field("request_id", &self.request_id())
            .field("session_id", &self.session_id())
            .field("retained_bytes", &self.retained_bytes())
            .finish_non_exhaustive()
    }
}

/// Cloneable owner of transactional deferred request state.
pub struct DeferredRegistry<R>
where
    R: Send + 'static,
{
    inner: Arc<RegistryInner<R>>,
}

/// Result of sealing a deferred registry and releasing registry-owned state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DeferredRegistryShutdownOutcome {
    /// This caller won shutdown and completed the detached registry-owned batch.
    Completed(DeferredRegistryShutdownStats),
    /// Another or reentrant caller is currently completing shutdown.
    InProgress,
    /// A prior caller already completed shutdown.
    AlreadyClosed,
}

/// Low-cardinality statistics for one completed registry shutdown batch.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct DeferredRegistryShutdownStats {
    detached_entries: usize,
    notified_tickets: usize,
    terminalized_responses: usize,
    in_progress_responses: usize,
    invariant_failures: usize,
}

impl DeferredRegistryShutdownStats {
    /// Returns the number of registry-owned primary entries detached by shutdown.
    #[must_use]
    pub const fn detached_entries(self) -> usize {
        self.detached_entries
    }

    /// Returns the number of live provisional claim tickets notified by shutdown.
    #[must_use]
    pub const fn notified_tickets(self) -> usize {
        self.notified_tickets
    }

    /// Returns the number of non-sending responses terminalized by shutdown.
    #[must_use]
    pub const fn terminalized_responses(self) -> usize {
        self.terminalized_responses
    }

    /// Returns the number of responses already in canonical sending ownership.
    #[must_use]
    pub const fn in_progress_responses(self) -> usize {
        self.in_progress_responses
    }

    /// Returns the number of unexpected registry or response-state failures.
    #[must_use]
    pub const fn invariant_failures(self) -> usize {
        self.invariant_failures
    }

    pub(super) fn record_detached_entry(&mut self) {
        self.detached_entries = self.detached_entries.saturating_add(1);
    }

    pub(super) fn record_ticket(&mut self) {
        self.notified_tickets = self.notified_tickets.saturating_add(1);
    }

    pub(super) fn record_terminalized(&mut self) {
        self.terminalized_responses = self.terminalized_responses.saturating_add(1);
    }

    pub(super) fn record_in_progress(&mut self) {
        self.in_progress_responses = self.in_progress_responses.saturating_add(1);
    }

    pub(super) fn record_invariant_failure(&mut self) {
        self.invariant_failures = self.invariant_failures.saturating_add(1);
    }
}

impl<R> Clone for DeferredRegistry<R>
where
    R: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<R> DeferredRegistry<R>
where
    R: Send + 'static,
{
    /// Creates an empty registry. Deferred identifiers remain process-global.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RegistryInner::default()),
        }
    }

    #[cfg(test)]
    fn with_test_sequence(sequence: Arc<std::sync::atomic::AtomicU64>) -> Self {
        Self {
            inner: Arc::new(RegistryInner::with_test_sequence(sequence)),
        }
    }

    /// Computes the canonical retained size for a request stored in this registry.
    ///
    /// The existing deferred-response fixed charge is counted exactly once.
    /// This method adds one inline `R`, the three registry indexes, the
    /// conservative per-session bucket charge, transient ticket/claim
    /// allocations, fixed resume completion/job-cell ownership, one logical
    /// `Arc<RegistryCleanupTarget<R>>` allocation, and one coordinator target
    /// record/map slot for a possible network cleanup enrollment.
    /// Caller-declared resume allocations, filters, secondary-index leases,
    /// and metadata remain unchanged.
    ///
    /// # Errors
    ///
    /// Returns the existing retained-size overflow error when any checked
    /// layout, addition, or subtraction cannot be represented.
    pub fn try_retained_size(
        parts: DeferredRetainedSizeParts,
    ) -> Result<DeferredRetainedSize, crate::contract::TransportContractViolation> {
        let retained = DeferredRetainedSize::try_from_parts(parts)?;
        retained.checked_add(
            registry_additional_bytes::<R>()
                .ok_or(crate::contract::TransportContractViolation::DeferredRetainedSizeOverflow)?,
        )
    }

    /// Registers an already-built resume request provisionally.
    ///
    /// The returned sealed guard must be returned through
    /// [`super::HandlerOutcome::Deferred`] before the dispatcher commits it.
    /// Lifecycle checkpoints resolve simultaneous stops in parent-cancellation,
    /// session-close, then deadline order. A stop published after the final
    /// registry publication checkpoint is handled by deferred-session cleanup.
    ///
    /// Duplicate ownership, identity exhaustion, and lifecycle stops are
    /// returned as source-free outcomes. Deterministic retained-size violations
    /// and operational registry failures retain the original request in their
    /// typed carrier branches.
    pub fn register(&self, mut request: DeferredRequest<R>) -> DeferredRegistryOutcome<R> {
        let request_id = request.request_id();
        if let Err(violation) = validate_retained_floor::<R>(request.retained_bytes()) {
            return DeferredRegistryOutcome::ContractViolation {
                violation,
                recovery: DeferredRegistryRecovery::Request(request),
            };
        }
        if let Some(kind) = lifecycle_stop_with_expiry(request.control(), request.expiry()) {
            request.parts.cleanup_lifecycle(kind);
            drop(request);
            return registry_lifecycle_outcome(kind);
        }
        let request_control = request.control().clone();
        let request_expiry = request.expiry();
        let response_state = request.parts.response_state();
        let id = match self.insert_shell(
            request_id,
            request.session_id(),
            request_control.clone(),
            Arc::clone(&response_state),
            request.parts.session_cleanup(),
            request_expiry,
        ) {
            Ok(id) => id,
            Err(kind) => {
                if matches!(kind, RegistryFailure::ParentCancelled | RegistryFailure::SessionClosed) {
                    request.parts.cleanup_lifecycle(kind);
                    drop(request);
                    return registry_lifecycle_outcome(kind);
                }
                return registry_failure_outcome(kind, DeferredRegistryRecovery::Request(request));
            }
        };
        if let Err(request) = self.inner.store_prepared_from_shell(id, request) {
            drop(self.inner.remove(id));
            if let Some(kind) = registration_stop(request.control(), request.expiry(), &response_state) {
                let mut request = request;
                request.parts.cleanup_lifecycle(kind);
                drop(request);
                return registry_lifecycle_outcome(kind);
            }
            return registry_failure_outcome(
                RegistryFailure::RegistryInvariant,
                DeferredRegistryRecovery::Request(*request),
            );
        }
        DeferredRegistryOutcome::Registered(DeferredRegistration::new(
            id,
            request_id,
            Box::new(RegistrationOwnerImpl {
                inner: Arc::clone(&self.inner),
                id,
                control: request_control,
                response_state,
                expiry: request_expiry,
                #[cfg(test)]
                commit_checkpoint: None,
            }),
        ))
    }

    /// Builds resume data outside the registry lock and registers it provisionally.
    ///
    /// The builder receives the allocated deferred identity so it can install
    /// an optional business index whose RAII lease is owned by the returned `R`.
    /// The synchronous builder runs to completion and is not interrupted by a
    /// lifecycle stop. Checkpoints before and after it resolve simultaneous
    /// stops in parent-cancellation, session-close, then deadline order. A stop
    /// published after the final registry publication checkpoint is handled by
    /// Deferred-session cleanup.
    ///
    /// Recoverable pre-builder outcomes return the uninvoked builder with the
    /// deferred parts. A builder rejection returns its error with the same
    /// parts. If lifecycle cancellation wins after the builder returns, the
    /// builder result and deferred parts are consumed and released.
    pub fn register_with<E, F>(&self, mut parts: DeferredParts, builder: F) -> DeferredRegistryOutcome<R, E, F>
    where
        E: Error + Send + Sync + 'static,
        F: FnOnce(DeferredId) -> Result<R, E>,
    {
        let request_id = parts.request_id();
        if let Err(violation) = validate_retained_floor::<R>(parts.retained_bytes()) {
            return DeferredRegistryOutcome::ContractViolation {
                violation,
                recovery: DeferredRegistryRecovery::Builder { builder, parts },
            };
        }
        if let Some(kind) = lifecycle_stop_with_expiry(parts.control(), parts.expiry()) {
            parts.cleanup_lifecycle(kind);
            drop(parts);
            return registry_lifecycle_outcome(kind);
        }
        let request_control = parts.control().clone();
        let parts_expiry = parts.expiry();
        let response_state = parts.response_state();
        let id = match self.insert_shell(
            request_id,
            parts.session_id(),
            request_control.clone(),
            Arc::clone(&response_state),
            parts.session_cleanup(),
            parts_expiry,
        ) {
            Ok(id) => id,
            Err(kind) => {
                if matches!(kind, RegistryFailure::ParentCancelled | RegistryFailure::SessionClosed) {
                    parts.cleanup_lifecycle(kind);
                    drop(parts);
                    return registry_lifecycle_outcome(kind);
                }
                return registry_failure_outcome(kind, DeferredRegistryRecovery::Builder { builder, parts });
            }
        };
        if !self.inner.transition_to_building(id) {
            drop(self.inner.remove(id));
            if let Some(kind) = registration_stop(parts.control(), parts_expiry, &response_state) {
                parts.cleanup_lifecycle(kind);
                drop(parts);
                return registry_lifecycle_outcome(kind);
            }
            return registry_failure_outcome(
                RegistryFailure::RegistryInvariant,
                DeferredRegistryRecovery::Builder { builder, parts },
            );
        }
        let mut transaction = BuildTransaction::new(Arc::clone(&self.inner), id, parts);
        match builder(id) {
            Ok(resume) => {
                if let Some(kind) =
                    lifecycle_stop_with_expiry(transaction.parts().control(), transaction.parts().expiry())
                {
                    let mut parts = transaction.rollback();
                    parts.cleanup_lifecycle(kind);
                    drop(resume);
                    drop(parts);
                    return registry_lifecycle_outcome(kind);
                }
                let request = DeferredRequest::new(resume, transaction.take_parts());
                match self.inner.store_prepared_from_building(id, request) {
                    Ok(()) => transaction.disarm(),
                    Err(request) => {
                        transaction.disarm_and_remove();
                        if let Some(kind) = registration_stop(request.control(), request.expiry(), &response_state) {
                            let mut request = request;
                            request.parts.cleanup_lifecycle(kind);
                            drop(request);
                            return registry_lifecycle_outcome(kind);
                        }
                        return registry_failure_outcome(
                            RegistryFailure::RegistryInvariant,
                            DeferredRegistryRecovery::Request(*request),
                        );
                    }
                }
            }
            Err(source) => {
                if let Some(kind) =
                    lifecycle_stop_with_expiry(transaction.parts().control(), transaction.parts().expiry())
                {
                    let mut parts = transaction.rollback();
                    parts.cleanup_lifecycle(kind);
                    drop(source);
                    drop(parts);
                    return registry_lifecycle_outcome(kind);
                }
                let parts = transaction.rollback();
                return DeferredRegistryOutcome::BuilderRejected { error: source, parts };
            }
        }
        DeferredRegistryOutcome::Registered(DeferredRegistration::new(
            id,
            request_id,
            Box::new(RegistrationOwnerImpl {
                inner: Arc::clone(&self.inner),
                id,
                control: request_control,
                response_state,
                expiry: parts_expiry,
                #[cfg(test)]
                commit_checkpoint: None,
            }),
        ))
    }

    /// Claims one active deferred request or waits for its provisional registration.
    ///
    /// Concurrent wake reasons coalesce to the first reason observed. A
    /// provisional waiter is notified durably if publication or lifecycle
    /// removal wins before the waiter begins polling.
    ///
    /// # Errors
    ///
    /// Returns an operational transport error only when an internal registry
    /// invariant cannot be preserved. Missing, already-claimed, terminal, and
    /// lifecycle states are source-free [`DeferredClaimOutcome`] variants.
    pub async fn claim(
        &self,
        id: DeferredId,
        reason: DeferredWakeReason,
    ) -> Result<DeferredClaimOutcome<R>, crate::error::TransportError> {
        let rejection = match self.inner.start_claim(id, reason, None) {
            ClaimStart::Claimed(claimed) => return Ok(DeferredClaimOutcome::Claimed(claimed)),
            ClaimStart::Rejected(rejection) => return converge_claim_rejection(rejection),
            ClaimStart::Wait(waiter) => {
                let resolution = waiter.wait().await;
                match resolution {
                    TicketResolution::Published => match self.inner.start_claim(id, reason, Some(&waiter)) {
                        ClaimStart::Claimed(claimed) => return Ok(DeferredClaimOutcome::Claimed(claimed)),
                        ClaimStart::Rejected(rejection) => return converge_claim_rejection(rejection),
                        ClaimStart::Wait(_) => {
                            DeferredClaimRejection::Operational(DeferredClaimOperationalFailure::invariant())
                        }
                    },
                    TicketResolution::RemovedNotFound => DeferredClaimRejection::NotFound,
                    TicketResolution::RemovedParentCancelled => DeferredClaimRejection::ParentCancelled,
                    TicketResolution::RemovedSessionClosed => DeferredClaimRejection::SessionClosed,
                    TicketResolution::RemovedDeadlineExpired => DeferredClaimRejection::DeadlineExpired,
                    TicketResolution::RemovedInvariant | TicketResolution::Pending => {
                        DeferredClaimRejection::Operational(DeferredClaimOperationalFailure::invariant())
                    }
                }
            }
        };
        converge_claim_rejection(rejection)
    }

    /// Examines at most `limit` due ordered entries using one frozen clock read.
    ///
    /// Long-poll expiry may return affine timeout claims. Owner expiry only
    /// terminalizes and releases registry-owned state. A persistent cursor
    /// prevents a provisional protocol wake from starving later active entries.
    pub fn sweep_expired(&self, limit: NonZeroUsize) -> DeferredExpiryBatch<R> {
        self.inner.sweep_expired(tokio::time::Instant::now(), limit)
    }

    #[cfg(test)]
    pub(crate) fn sweep_expired_at_for_test(
        &self,
        now: tokio::time::Instant,
        limit: NonZeroUsize,
    ) -> DeferredExpiryBatch<R> {
        self.inner.sweep_expired(now, limit)
    }

    /// Seals this registry and releases all state still owned by it.
    ///
    /// The winning call detaches registry indexes while holding the registry
    /// lock, then performs notifications, response terminalization, and user
    /// value destruction after releasing that lock. Externally held claims,
    /// synchronous builders, and accepted resume jobs retain their existing
    /// affine owners and may continue until those owners release them.
    /// `Completed` covers only the registry-owned batch detached by this call;
    /// it does not promise completion of those externally owned operations.
    /// After shutdown begins, otherwise-valid registrations and claims against
    /// live inputs are rejected as parent-cancelled; retained-size and request
    /// lifecycle preflights may reject first. A panic while releasing detached
    /// user state still seals the registry, so a later call reports
    /// `AlreadyClosed`.
    #[must_use]
    pub fn shutdown(&self) -> DeferredRegistryShutdownOutcome {
        self.inner.shutdown()
    }

    fn insert_shell(
        &self,
        request_id: RequestId,
        session_id: SessionId,
        control: RequestControlView,
        response_state: Arc<super::ResponseState>,
        cleanup: Option<super::DeferredSessionCleanupRegistration>,
        expiry: Option<DeferredExpiry>,
    ) -> Result<DeferredId, RegistryFailure> {
        match cleanup {
            Some(cleanup) => {
                if cleanup.session_id() != session_id {
                    return Err(RegistryFailure::RegistryInvariant);
                }
                let key = super::deferred_session_cleanup::RegistryCleanupTarget::<R>::key_for(&self.inner);
                cleanup.enroll(
                    key,
                    || {
                        super::deferred_session_cleanup::RegistryCleanupTarget::new(&self.inner)
                            as Arc<dyn super::deferred_session_cleanup::DeferredSessionCleanupTarget>
                    },
                    |enrollment| {
                        self.inner
                            .insert_shell(request_id, session_id, control, response_state, expiry, enrollment)
                    },
                )
            }
            None => {
                let mut enrollment = None;
                self.inner
                    .insert_shell(request_id, session_id, control, response_state, expiry, &mut enrollment)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn test_index_counts(&self) -> (usize, usize, usize) {
        self.inner.index_counts()
    }

    #[cfg(test)]
    pub(crate) fn test_contains(&self, id: DeferredId) -> bool {
        self.inner.contains(id)
    }

    #[cfg(test)]
    pub(crate) fn test_is_active(&self, id: DeferredId) -> bool {
        self.inner.is_active(id)
    }

    #[cfg(test)]
    pub(crate) fn test_session_member_count(&self, session_id: SessionId) -> usize {
        self.inner.session_member_count(session_id)
    }

    #[cfg(test)]
    pub(crate) fn test_claim_marker_count(&self) -> usize {
        self.inner.claim_marker_count()
    }
}

fn registration_stop(
    control: &RequestControlView,
    expiry: Option<DeferredExpiry>,
    response_state: &super::ResponseState,
) -> Option<RegistryFailure> {
    lifecycle_stop_with_expiry(control, expiry).or_else(|| match response_state.terminal_state() {
        Some(super::ResponseTerminalState::Closed) => Some(RegistryFailure::SessionClosed),
        Some(super::ResponseTerminalState::Cancelled) => Some(RegistryFailure::ParentCancelled),
        Some(super::ResponseTerminalState::Completed | super::ResponseTerminalState::Failed { .. }) | None => None,
    })
}

impl<R> Default for DeferredRegistry<R>
where
    R: Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<R> fmt::Debug for DeferredRegistry<R>
where
    R: Send + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("DeferredRegistry").finish_non_exhaustive()
    }
}

/// Sealed proof that one provisional registration matches a handler request.
///
/// The registry remains generic, but this affine proof is deliberately
/// non-generic so [`super::HandlerOutcome`] stays a closed public enum.
///
/// ```compile_fail
/// use rocketmq_transport::api::{DeferredId, DeferredRegistration, RequestId};
///
/// fn cannot_forge(id: DeferredId, request_id: RequestId) -> DeferredRegistration {
///     DeferredRegistration { id, request_id, owner: None }
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::DeferredRegistration;
///
/// fn registrations_are_affine(registration: &DeferredRegistration) {
///     let _: DeferredRegistration = registration.clone();
/// }
/// ```
#[must_use]
pub struct DeferredRegistration {
    id: DeferredId,
    request_id: RequestId,
    owner: Option<Box<dyn RegistrationOwner + Send + 'static>>,
}

impl DeferredRegistration {
    fn new(id: DeferredId, request_id: RequestId, owner: Box<dyn RegistrationOwner + Send + 'static>) -> Self {
        Self {
            id,
            request_id,
            owner: Some(owner),
        }
    }

    /// Returns the process-local deferred identity.
    #[must_use]
    pub const fn deferred_id(&self) -> DeferredId {
        self.id
    }

    /// Returns the exact request accepted by trusted deferred storage.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    pub(crate) fn commit(mut self) -> Result<(), DeferredCommitError> {
        let owner = self.owner.take().ok_or_else(DeferredCommitError::invariant)?;
        owner.commit()
    }

    #[cfg(test)]
    pub(crate) fn set_commit_checkpoint(&mut self, checkpoint: impl FnOnce() + Send + 'static) {
        self.owner
            .as_mut()
            .expect("active registration owns its transaction")
            .set_commit_checkpoint(Box::new(checkpoint));
    }

    #[cfg(test)]
    pub(crate) fn for_test(request_id: RequestId) -> Self {
        Self::new(
            DeferredId::for_test(1),
            request_id,
            Box::new(TestRegistrationOwner {
                drop_probe: None,
                commit_error: None,
            }),
        )
    }

    #[cfg(test)]
    pub(crate) fn with_commit_error_for_test(request_id: RequestId, kind: DeferredCommitErrorKind) -> Self {
        Self::new(
            DeferredId::for_test(1),
            request_id,
            Box::new(TestRegistrationOwner {
                drop_probe: None,
                commit_error: Some(DeferredCommitError::for_test(kind)),
            }),
        )
    }

    #[cfg(test)]
    pub(crate) fn with_drop_probe(request_id: RequestId, drop_probe: Arc<std::sync::atomic::AtomicUsize>) -> Self {
        Self::new(
            DeferredId::for_test(1),
            request_id,
            Box::new(TestRegistrationOwner {
                drop_probe: Some(drop_probe),
                commit_error: None,
            }),
        )
    }
}

impl fmt::Debug for DeferredRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeferredRegistration")
            .field("id", &self.id)
            .field("request_id", &self.request_id)
            .field("sealed", &true)
            .finish()
    }
}

impl Drop for DeferredRegistration {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.take() {
            owner.rollback();
        }
    }
}

fn converge_claim_rejection<R>(
    rejection: DeferredClaimRejection,
) -> Result<DeferredClaimOutcome<R>, crate::error::TransportError>
where
    R: Send + 'static,
{
    match rejection {
        DeferredClaimRejection::NotFound => Ok(DeferredClaimOutcome::NotFound),
        DeferredClaimRejection::AlreadyClaimed => Ok(DeferredClaimOutcome::AlreadyClaimed),
        DeferredClaimRejection::AlreadyCompleted => Ok(DeferredClaimOutcome::AlreadyCompleted),
        DeferredClaimRejection::ParentCancelled => Ok(DeferredClaimOutcome::ParentCancelled),
        DeferredClaimRejection::SessionClosed => Ok(DeferredClaimOutcome::SessionClosed),
        DeferredClaimRejection::DeadlineExpired => Ok(DeferredClaimOutcome::DeadlineExpired),
        DeferredClaimRejection::Operational(error) => Err(crate::error::TransportError::dispatch(error)),
    }
}

fn registry_lifecycle_outcome<R, E, F>(kind: RegistryFailure) -> DeferredRegistryOutcome<R, E, F> {
    match kind {
        RegistryFailure::ParentCancelled => DeferredRegistryOutcome::ParentCancelled,
        RegistryFailure::SessionClosed => DeferredRegistryOutcome::SessionClosed,
        RegistryFailure::DeadlineExpired => DeferredRegistryOutcome::DeadlineExpired,
        _ => registry_failure_outcome(kind, DeferredRegistryRecovery::None),
    }
}

fn registry_failure_outcome<R, E, F>(
    kind: RegistryFailure,
    recovery: DeferredRegistryRecovery<R, F>,
) -> DeferredRegistryOutcome<R, E, F> {
    if kind == RegistryFailure::DuplicateRequest {
        return DeferredRegistryOutcome::DuplicateRequest(recovery);
    }
    if kind == RegistryFailure::IdentityExhausted {
        return DeferredRegistryOutcome::IdentityExhausted(recovery);
    }
    let source = match kind {
        RegistryFailure::RegistryInvariant => RegistryOperationalFailure::Invariant,
        RegistryFailure::DuplicateRequest
        | RegistryFailure::IdentityExhausted
        | RegistryFailure::ParentCancelled
        | RegistryFailure::SessionClosed
        | RegistryFailure::DeadlineExpired
        | RegistryFailure::CleanupInstallerRejected => RegistryOperationalFailure::Invariant,
    };
    DeferredRegistryOutcome::OperationalFailure {
        error: crate::error::TransportError::dispatch(source),
        recovery,
    }
}

#[derive(Debug, thiserror::Error)]
enum RegistryOperationalFailure {
    #[error("deferred registry invariant failed")]
    Invariant,
}

#[cfg(test)]
#[path = "../../tests/unit/dispatch/deferred_registry.rs"]
mod tests;
