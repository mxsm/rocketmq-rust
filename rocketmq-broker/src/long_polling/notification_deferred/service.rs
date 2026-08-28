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

use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicBool;
#[cfg(test)]
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::RuntimeResult;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskId;
use rocketmq_store::ArcMessageFilter;
use rocketmq_transport::api::v2::ClaimedDeferred;
use rocketmq_transport::api::v2::DeferredAdmission;
use rocketmq_transport::api::v2::DeferredAdmissionAcquireError;
use rocketmq_transport::api::v2::DeferredAdmissionSnapshot;
use rocketmq_transport::api::v2::DeferredClaimError;
use rocketmq_transport::api::v2::DeferredClaimErrorKind;
use rocketmq_transport::api::v2::DeferredExpiryBatch;
use rocketmq_transport::api::v2::DeferredExpiryBatchStats;
use rocketmq_transport::api::v2::DeferredExpiryError;
use rocketmq_transport::api::v2::DeferredExpiryErrorKind;
use rocketmq_transport::api::v2::DeferredExpiryMargins;
use rocketmq_transport::api::v2::DeferredId;
use rocketmq_transport::api::v2::DeferredParts;
use rocketmq_transport::api::v2::DeferredRegistration;
use rocketmq_transport::api::v2::DeferredRegistry;
use rocketmq_transport::api::v2::DeferredRegistryError;
use rocketmq_transport::api::v2::DeferredRegistryErrorKind;
use rocketmq_transport::api::v2::DeferredRegistryShutdownOutcome;
use rocketmq_transport::api::v2::DeferredResumeError;
use rocketmq_transport::api::v2::DeferredResumeRetainedSize;
use rocketmq_transport::api::v2::DeferredRetainedSizeParts;
use rocketmq_transport::api::v2::DeferredWakeReason;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestOrigin;
use rocketmq_transport::api::v2::ResponsePlan;
use rocketmq_transport::api::v2::ResponseReceipt;
use rocketmq_transport::api::v2::TakeDeferredResponderError;

use super::deadline::NotificationWaitDeadline;
use super::deadline::NotificationWaitDeadlineError;
use super::index::NotificationArrivalView;
use super::index::NotificationCandidateReservation;
use super::index::NotificationCandidateSelection;
use super::index::NotificationCriteriaIndex;
use super::index::NotificationCriteriaKey;
use super::index::NotificationCriteriaLimits;
use super::index::NotificationIndexError;
use super::index::NotificationIndexSnapshot;
use super::index::NotificationMatchCriteria;
use super::index::NotificationScanCursor;

mod continuation;
mod data;

use continuation::ContinuationAdmission;
pub(crate) use continuation::NotificationArrivalContinuation;
use continuation::OwnedNotificationArrival;
use data::CounterObservation;
pub(crate) use data::NotificationRequestData;
pub(crate) use data::NotificationRetainedEstimate;
pub(crate) use data::PreparedNotificationRegistration;
use data::PreparedObservation;
use data::PreparedRequestProvenance;
pub(crate) use data::ResumeNotification;

pub(crate) struct NotificationDeferredService {
    admission: DeferredAdmission,
    registry: DeferredRegistry<ResumeNotification>,
    pub(super) index: NotificationCriteriaIndex,
    expiry_margins: DeferredExpiryMargins,
    scan_limit: NonZeroUsize,
    conflict_limit: NonZeroUsize,
    continuation_admission: Arc<ContinuationAdmission>,
    prepared: Arc<AtomicUsize>,
    pending_claims: Arc<AtomicUsize>,
    resume_executions: Arc<AtomicUsize>,
    resume_execution_bytes: Arc<AtomicUsize>,
    closed: AtomicBool,
    #[cfg(test)]
    register_fault: AtomicU8,
}

impl NotificationDeferredService {
    #[allow(
        clippy::too_many_arguments,
        reason = "constructor exposes each independent bounded resource"
    )]
    pub(crate) fn new(
        admission: DeferredAdmission,
        index_limits: NotificationCriteriaLimits,
        expiry_margins: DeferredExpiryMargins,
        scan_limit: NonZeroUsize,
        conflict_limit: NonZeroUsize,
        continuation_count: NonZeroUsize,
        continuation_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            admission,
            registry: DeferredRegistry::new(),
            index: NotificationCriteriaIndex::new(index_limits),
            expiry_margins,
            scan_limit,
            conflict_limit,
            continuation_admission: Arc::new(ContinuationAdmission::new(
                continuation_count.get(),
                continuation_bytes.get(),
            )),
            prepared: Arc::new(AtomicUsize::new(0)),
            pending_claims: Arc::new(AtomicUsize::new(0)),
            resume_executions: Arc::new(AtomicUsize::new(0)),
            resume_execution_bytes: Arc::new(AtomicUsize::new(0)),
            closed: AtomicBool::new(false),
            #[cfg(test)]
            register_fault: AtomicU8::new(0),
        }
    }

    pub(crate) fn prepare(
        &self,
        request: &RemotingRequest,
        subscription: Option<SubscriptionData>,
        filter: Option<ArcMessageFilter>,
        retained: NotificationRetainedEstimate,
    ) -> Result<PreparedNotificationRegistration, NotificationDeferredPrepareError> {
        if request.command().is_oneway_rpc() {
            return Err(NotificationDeferredPrepareError::OneWay);
        }
        let header = request
            .command()
            .decode_command_custom_header::<NotificationRequestHeader>()
            .map_err(NotificationDeferredPrepareError::Header)?;
        let effective_peer = match request.origin() {
            RequestOrigin::Network { peer } => peer.address(),
            RequestOrigin::Embedded { .. } => return Err(NotificationDeferredPrepareError::EmbeddedOrigin),
            _ => return Err(NotificationDeferredPrepareError::EmbeddedOrigin),
        };
        let wall_now =
            i64::try_from(current_millis()).map_err(|_| NotificationDeferredPrepareError::WallTimeOverflow)?;
        self.prepare_data_at(
            NotificationRequestData::new(header, effective_peer),
            subscription,
            filter,
            retained,
            wall_now,
            tokio::time::Instant::now(),
            Some(PreparedRequestProvenance::capture(request)),
        )
    }

    #[cfg(test)]
    pub(crate) fn prepare_at(
        &self,
        request: NotificationRequestData,
        subscription: Option<SubscriptionData>,
        filter: Option<ArcMessageFilter>,
        retained: NotificationRetainedEstimate,
        wall_now: i64,
        monotonic_now: tokio::time::Instant,
    ) -> Result<PreparedNotificationRegistration, NotificationDeferredPrepareError> {
        self.prepare_data_at(request, subscription, filter, retained, wall_now, monotonic_now, None)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "manual clock and provenance are explicit preparation inputs"
    )]
    fn prepare_data_at(
        &self,
        request: NotificationRequestData,
        subscription: Option<SubscriptionData>,
        filter: Option<ArcMessageFilter>,
        retained: NotificationRetainedEstimate,
        wall_now: i64,
        monotonic_now: tokio::time::Instant,
        provenance: Option<PreparedRequestProvenance>,
    ) -> Result<PreparedNotificationRegistration, NotificationDeferredPrepareError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(NotificationDeferredPrepareError::ServiceClosed);
        }
        if self.expiry_margins.recovery().is_zero() || self.expiry_margins.write().is_zero() {
            return Err(NotificationDeferredPrepareError::InvalidExpiryMargins);
        }
        let deadline = NotificationWaitDeadline::checked(
            request.header.born_time,
            request.header.poll_time,
            wall_now,
            monotonic_now,
        )
        .map_err(NotificationDeferredPrepareError::Deadline)?;
        let resume_bytes = retained
            .resume_bytes
            .checked_add(request.estimated_dynamic_bytes()?)
            .ok_or(NotificationDeferredPrepareError::RetainedSizeOverflow)?;
        let filter_bytes = retained
            .filter_bytes
            .checked_add(std::mem::size_of::<NotificationMatchCriteria>())
            .ok_or(NotificationDeferredPrepareError::RetainedSizeOverflow)?;
        let retained_size = DeferredRegistry::<ResumeNotification>::try_retained_size(
            DeferredRetainedSizeParts::new(resume_bytes)
                .with_filter_bytes(filter_bytes)
                .with_secondary_index_bytes(NotificationCriteriaIndex::<DeferredId>::retained_bytes_per_entry())
                .with_metadata_bytes(retained.metadata_bytes),
        )
        .map_err(NotificationDeferredPrepareError::Admission)?;
        let key = NotificationCriteriaKey::from_parts(request.topic(), request.consumer_group(), request.queue_id());
        let reservation = self
            .index
            .reserve_at(key, monotonic_now)
            .map_err(NotificationDeferredPrepareError::Index)?;
        let permit = self
            .admission
            .try_reserve(retained_size)
            .map_err(NotificationDeferredPrepareError::Admission)?;
        let observation = PreparedObservation::new(Arc::clone(&self.prepared));
        let prepared = PreparedNotificationRegistration {
            request,
            criteria: Arc::new(NotificationMatchCriteria::new(subscription, filter)),
            deadline,
            reservation,
            permit,
            provenance,
            observation,
        };
        if self.closed.load(Ordering::Acquire) {
            drop(prepared);
            return Err(NotificationDeferredPrepareError::ServiceClosed);
        }
        Ok(prepared)
    }

    /// Takes the responder only after every recoverable reservation succeeds.
    pub(crate) fn register(
        &self,
        prepared: PreparedNotificationRegistration,
        request: &mut RemotingRequest,
    ) -> Result<DeferredRegistration, NotificationDeferredRegisterError> {
        if !prepared
            .provenance
            .is_some_and(|provenance| provenance.matches(request))
        {
            return Err(NotificationDeferredRegisterError::ProvenanceMismatch);
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(NotificationDeferredRegisterError::ServiceClosedBeforeTake);
        }
        let responder = request
            .take_deferred_responder()
            .map_err(NotificationDeferredRegisterError::Responder)?;
        #[cfg(test)]
        let register_fault = self.register_fault.swap(0, Ordering::AcqRel);
        #[cfg(test)]
        if register_fault == REGISTER_FAULT_CLOSE_AFTER_TAKE {
            let _ = self.shutdown();
        }
        if self.closed.load(Ordering::Acquire) {
            drop(responder);
            return Err(NotificationDeferredRegisterError::ServiceClosedAfterTake);
        }
        let PreparedNotificationRegistration {
            request,
            criteria,
            deadline,
            reservation,
            permit,
            provenance: _,
            observation,
        } = prepared;
        #[cfg(test)]
        let protocol_at = if register_fault == REGISTER_FAULT_EXPIRY_AFTER_TAKE {
            tokio::time::Instant::now()
                .checked_sub(Duration::from_millis(1))
                .unwrap_or_else(tokio::time::Instant::now)
        } else {
            deadline.protocol_at()
        };
        #[cfg(not(test))]
        let protocol_at = deadline.protocol_at();
        let parts = DeferredParts::new(responder, permit)
            .try_with_expiry(protocol_at, self.expiry_margins)
            .map_err(NotificationDeferredRegisterError::Expiry)?;
        #[cfg(test)]
        if register_fault == REGISTER_FAULT_BUILDER_AFTER_TAKE {
            self.closed.store(true, Ordering::Release);
            let _ = self.registry.shutdown();
        }
        let registration = self
            .registry
            .register_with(parts, move |id| {
                let index_lease = reservation.publish(id, deadline, Arc::clone(&criteria));
                Ok::<_, Infallible>(ResumeNotification::new(request, criteria, deadline, index_lease))
            })
            .map_err(NotificationDeferredRegisterError::Registry);
        drop(observation);
        registration
    }

    /// Performs the one synchronous, bounded, borrow-only filtering batch.
    pub(crate) fn prepare_arrival_batch<'a>(
        &self,
        arrival: NotificationArrivalView<'a>,
        cursor: Option<NotificationScanCursor>,
    ) -> NotificationPreparedArrivalBatch {
        self.prepare_arrival_batch_with_conflict_budget(
            arrival,
            cursor,
            self.conflict_limit.get().min(self.scan_limit.get()),
        )
    }

    fn prepare_arrival_batch_with_conflict_budget<'a>(
        &self,
        arrival: NotificationArrivalView<'a>,
        cursor: Option<NotificationScanCursor>,
        conflict_budget: usize,
    ) -> NotificationPreparedArrivalBatch {
        let mut cursor = cursor.unwrap_or_else(|| self.index.scan_cursor(&arrival));
        let cq = arrival.cq_ext_unit();
        let mut candidates = Vec::new();
        let mut inspected = 0;
        let mut conflicts = 0;
        while inspected < self.scan_limit.get() && conflicts < conflict_budget && !cursor.is_complete() {
            match self.index.reserve_next(&mut cursor) {
                NotificationCandidateSelection::Candidate(candidate) => {
                    inspected += 1;
                    if candidate.criteria().matches(&arrival, &cq) {
                        cursor.advance_key();
                        candidates.push(candidate);
                    }
                }
                NotificationCandidateSelection::Conflict => {
                    conflicts += 1;
                    cursor.record_conflict();
                }
                NotificationCandidateSelection::Complete => break,
            }
        }
        NotificationPreparedArrivalBatch {
            candidates,
            cursor,
            inspected,
            conflicts,
        }
    }

    /// Claims only already-filtered affine candidates; no arrival borrow crosses await.
    pub(crate) async fn claim_prepared_arrival(
        &self,
        prepared: NotificationPreparedArrivalBatch,
    ) -> NotificationClaimBatch {
        let _observation = CounterObservation::new(Arc::clone(&self.pending_claims));
        let NotificationPreparedArrivalBatch {
            candidates,
            cursor,
            inspected,
            conflicts,
        } = prepared;
        let mut claims = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let id = candidate.id();
            match self.claim(id, DeferredWakeReason::MessageArrived).await {
                Ok(claim) => claims.push(claim),
                Err(error) if is_candidate_race(error.kind()) => {}
                Err(_) => {}
            }
            drop(candidate);
        }
        NotificationClaimBatch {
            claims,
            cursor,
            inspected,
            conflicts,
        }
    }

    async fn claim(
        &self,
        id: DeferredId,
        reason: DeferredWakeReason,
    ) -> Result<ClaimedDeferred<ResumeNotification>, DeferredClaimError> {
        let mut claimed = self.registry.claim(id, reason).await?;
        drop(claimed.resume_data_mut().take_index_lease());
        Ok(claimed)
    }

    pub(crate) fn admit_continuation(
        &self,
        arrival: NotificationArrivalView<'_>,
        cursor: NotificationScanCursor,
    ) -> Result<NotificationArrivalContinuation, NotificationContinuationError> {
        if cursor.is_complete() {
            return Err(NotificationContinuationError::Complete);
        }
        let remaining_conflicts = self.conflict_limit.get().saturating_sub(cursor.conflicts_spent());
        if remaining_conflicts == 0 {
            return Err(NotificationContinuationError::ConflictBudgetExhausted);
        }
        let retained_bytes = OwnedNotificationArrival::retained_bytes(arrival, &cursor)?;
        let permit = self.continuation_admission.reserve(retained_bytes)?;
        let owned = OwnedNotificationArrival::try_from_view(arrival)?;
        Ok(NotificationArrivalContinuation {
            owned,
            cursor,
            remaining_conflicts,
            _permit: permit,
        })
    }

    /// Runs all post-callback batches in one injected, lifecycle-owned task.
    pub(crate) fn spawn_continuation<F, Fut>(
        self: &Arc<Self>,
        task_group: &TaskGroup,
        mut continuation: NotificationArrivalContinuation,
        handle_claims: Arc<F>,
    ) -> RuntimeResult<TaskId>
    where
        F: Fn(Vec<ClaimedDeferred<ResumeNotification>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let service = Arc::clone(self);
        task_group.spawn_service("broker.notification-deferred.arrival", async move {
            loop {
                let prepared = {
                    let view = continuation.owned.view();
                    let batch_conflicts = continuation.remaining_conflicts.min(service.scan_limit.get());
                    service.prepare_arrival_batch_with_conflict_budget(view, Some(continuation.cursor), batch_conflicts)
                };
                let batch = service.claim_prepared_arrival(prepared).await;
                continuation.remaining_conflicts = continuation.remaining_conflicts.saturating_sub(batch.conflicts());
                let (claims, cursor) = batch.into_parts();
                handle_claims(claims).await;
                continuation.cursor = cursor;
                if continuation.cursor.is_complete()
                    || continuation.remaining_conflicts == 0
                    || service.closed.load(Ordering::Acquire)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
    }

    pub(crate) fn sweep_expired(&self) -> NotificationDeferredSweepBatch {
        NotificationDeferredSweepBatch::from_transport(self.registry.sweep_expired(self.scan_limit))
    }

    pub(crate) fn start_sweeper<F, Fut>(
        self: &Arc<Self>,
        task_group: &TaskGroup,
        interval_millis: NonZeroU64,
        handle_claims: F,
    ) -> RuntimeResult<TaskId>
    where
        F: Fn(Vec<ClaimedDeferred<ResumeNotification>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let service = Arc::clone(self);
        let cancellation = task_group.cancellation_token();
        let interval = Duration::from_millis(interval_millis.get());
        task_group.spawn_service("broker.notification-deferred.sweep", async move {
            loop {
                if service.closed.load(Ordering::Acquire)
                    || tokio::time::timeout(interval, cancellation.cancelled()).await.is_ok()
                {
                    break;
                }
                let claims = service.sweep_expired().into_claims();
                if !claims.is_empty() {
                    handle_claims(claims).await;
                }
            }
        })
    }

    pub(crate) async fn resume_claimed<F, Fut>(
        &self,
        claimed: ClaimedDeferred<ResumeNotification>,
        handler_retained: DeferredResumeRetainedSize,
        handler: F,
    ) -> Result<ResponseReceipt, DeferredResumeError>
    where
        F: FnOnce(ResumeNotification, DeferredWakeReason) -> Fut + Send + 'static,
        Fut: Future<Output = rocketmq_error::RocketMQResult<ResponsePlan>> + Send + 'static,
    {
        let resume_executions = Arc::clone(&self.resume_executions);
        let resume_execution_bytes = Arc::clone(&self.resume_execution_bytes);
        let dynamic_bytes = handler_retained.dynamic_bytes();
        let result = claimed
            .resume(handler_retained, move |resume, reason| async move {
                let _observation =
                    CounterObservation::new_with_bytes(resume_executions, resume_execution_bytes, dynamic_bytes);
                handler(resume, reason).await
            })
            .await;
        result
    }

    #[must_use]
    pub(crate) fn snapshot(&self) -> NotificationDeferredSnapshot {
        let continuation = self.continuation_admission.snapshot();
        NotificationDeferredSnapshot {
            admission: self.admission.snapshot(),
            index: self.index.snapshot(),
            prepared: self.prepared.load(Ordering::Acquire),
            pending_claims: self.pending_claims.load(Ordering::Acquire),
            resume_executions: self.resume_executions.load(Ordering::Acquire),
            resume_execution_bytes: self.resume_execution_bytes.load(Ordering::Acquire),
            active_continuations: continuation.count,
            continuation_bytes: continuation.bytes,
            continuation_rejected: continuation.rejected,
        }
    }

    #[must_use]
    pub(crate) fn shutdown(&self) -> DeferredRegistryShutdownOutcome {
        self.closed.store(true, Ordering::Release);
        self.registry.shutdown()
    }

    #[cfg(test)]
    pub(crate) fn force_register_fault(&self, fault: NotificationRegisterFault) {
        self.register_fault.store(fault as u8, Ordering::Release);
    }
}

#[cfg(test)]
const REGISTER_FAULT_CLOSE_AFTER_TAKE: u8 = 1;
#[cfg(test)]
const REGISTER_FAULT_EXPIRY_AFTER_TAKE: u8 = 2;
#[cfg(test)]
const REGISTER_FAULT_BUILDER_AFTER_TAKE: u8 = 3;

#[cfg(test)]
#[repr(u8)]
#[derive(Clone, Copy)]
pub(crate) enum NotificationRegisterFault {
    Close = REGISTER_FAULT_CLOSE_AFTER_TAKE,
    Expiry = REGISTER_FAULT_EXPIRY_AFTER_TAKE,
    Builder = REGISTER_FAULT_BUILDER_AFTER_TAKE,
}

#[must_use]
pub(crate) struct NotificationPreparedArrivalBatch {
    pub(super) candidates: Vec<NotificationCandidateReservation>,
    pub(super) cursor: NotificationScanCursor,
    inspected: usize,
    conflicts: usize,
}

impl NotificationPreparedArrivalBatch {
    #[must_use]
    pub(crate) const fn inspected(&self) -> usize {
        self.inspected
    }

    #[must_use]
    pub(crate) const fn conflicts(&self) -> usize {
        self.conflicts
    }

    #[must_use]
    pub(crate) fn candidate_count(&self) -> usize {
        self.candidates.len()
    }

    pub(crate) const fn cursor(&self) -> &NotificationScanCursor {
        &self.cursor
    }
}

#[must_use]
pub(crate) struct NotificationClaimBatch {
    claims: Vec<ClaimedDeferred<ResumeNotification>>,
    cursor: NotificationScanCursor,
    inspected: usize,
    conflicts: usize,
}

impl NotificationClaimBatch {
    #[must_use]
    pub(crate) const fn inspected(&self) -> usize {
        self.inspected
    }

    #[must_use]
    pub(crate) const fn conflicts(&self) -> usize {
        self.conflicts
    }

    pub(crate) fn into_parts(self) -> (Vec<ClaimedDeferred<ResumeNotification>>, NotificationScanCursor) {
        (self.claims, self.cursor)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct NotificationDeferredSnapshot {
    admission: DeferredAdmissionSnapshot,
    index: NotificationIndexSnapshot,
    prepared: usize,
    pending_claims: usize,
    resume_executions: usize,
    resume_execution_bytes: usize,
    active_continuations: usize,
    continuation_bytes: usize,
    continuation_rejected: usize,
}

impl NotificationDeferredSnapshot {
    #[must_use]
    pub(crate) const fn admission(self) -> DeferredAdmissionSnapshot {
        self.admission
    }

    #[must_use]
    pub(crate) const fn index(self) -> NotificationIndexSnapshot {
        self.index
    }

    #[must_use]
    pub(crate) const fn prepared(self) -> usize {
        self.prepared
    }

    #[must_use]
    pub(crate) const fn pending_claims(self) -> usize {
        self.pending_claims
    }

    #[must_use]
    pub(crate) const fn resume_executions(self) -> usize {
        self.resume_executions
    }

    #[must_use]
    pub(crate) const fn resume_execution_bytes(self) -> usize {
        self.resume_execution_bytes
    }

    #[must_use]
    pub(crate) const fn active_continuations(self) -> usize {
        self.active_continuations
    }

    #[must_use]
    pub(crate) const fn continuation_bytes(self) -> usize {
        self.continuation_bytes
    }

    #[must_use]
    pub(crate) const fn continuation_rejected(self) -> usize {
        self.continuation_rejected
    }
}

#[must_use]
pub(crate) struct NotificationDeferredSweepBatch {
    stats: DeferredExpiryBatchStats,
    claims: Vec<ClaimedDeferred<ResumeNotification>>,
}

impl NotificationDeferredSweepBatch {
    fn from_transport(batch: DeferredExpiryBatch<ResumeNotification>) -> Self {
        let stats = batch.stats();
        let mut claims = batch.into_claims();
        for claim in &mut claims {
            drop(claim.resume_data_mut().take_index_lease());
        }
        Self { stats, claims }
    }

    #[must_use]
    pub(crate) const fn stats(&self) -> DeferredExpiryBatchStats {
        self.stats
    }

    #[must_use]
    pub(crate) fn into_claims(self) -> Vec<ClaimedDeferred<ResumeNotification>> {
        self.claims
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NotificationContinuationError {
    Complete,
    ConflictBudgetExhausted,
    CountFull,
    BytesFull,
    SizeOverflow,
    Allocation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NotificationDeferredPrepareErrorKind {
    ServiceClosed,
    OneWay,
    EmbeddedOrigin,
    Header,
    WallTimeOverflow,
    InvalidExpiryMargins,
    RetainedSizeOverflow,
    Deadline,
    Index,
    Admission,
}

pub(crate) enum NotificationDeferredPrepareError {
    ServiceClosed,
    OneWay,
    EmbeddedOrigin,
    Header(RocketMQError),
    WallTimeOverflow,
    InvalidExpiryMargins,
    RetainedSizeOverflow,
    Deadline(NotificationWaitDeadlineError),
    Index(NotificationIndexError),
    Admission(DeferredAdmissionAcquireError),
}

impl NotificationDeferredPrepareError {
    #[must_use]
    pub(crate) const fn kind(&self) -> NotificationDeferredPrepareErrorKind {
        match self {
            Self::ServiceClosed => NotificationDeferredPrepareErrorKind::ServiceClosed,
            Self::OneWay => NotificationDeferredPrepareErrorKind::OneWay,
            Self::EmbeddedOrigin => NotificationDeferredPrepareErrorKind::EmbeddedOrigin,
            Self::Header(_) => NotificationDeferredPrepareErrorKind::Header,
            Self::WallTimeOverflow => NotificationDeferredPrepareErrorKind::WallTimeOverflow,
            Self::InvalidExpiryMargins => NotificationDeferredPrepareErrorKind::InvalidExpiryMargins,
            Self::RetainedSizeOverflow => NotificationDeferredPrepareErrorKind::RetainedSizeOverflow,
            Self::Deadline(_) => NotificationDeferredPrepareErrorKind::Deadline,
            Self::Index(_) => NotificationDeferredPrepareErrorKind::Index,
            Self::Admission(_) => NotificationDeferredPrepareErrorKind::Admission,
        }
    }
}

impl fmt::Debug for NotificationDeferredPrepareError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NotificationDeferredPrepareError")
            .field("kind", &self.kind())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for NotificationDeferredPrepareError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Notification deferred preparation failed: {:?}", self.kind())
    }
}

impl Error for NotificationDeferredPrepareError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NotificationDeferredRegisterErrorKind {
    ServiceClosedBeforeTake,
    ServiceClosedAfterTake,
    ProvenanceMismatch,
    Responder,
    Expiry(DeferredExpiryErrorKind),
    Registry(DeferredRegistryErrorKind),
}

pub(crate) enum NotificationDeferredRegisterError {
    ServiceClosedBeforeTake,
    ServiceClosedAfterTake,
    ProvenanceMismatch,
    Responder(TakeDeferredResponderError),
    Expiry(DeferredExpiryError),
    Registry(DeferredRegistryError<ResumeNotification, Infallible>),
}

impl NotificationDeferredRegisterError {
    #[must_use]
    pub(crate) const fn kind(&self) -> NotificationDeferredRegisterErrorKind {
        match self {
            Self::ServiceClosedBeforeTake => NotificationDeferredRegisterErrorKind::ServiceClosedBeforeTake,
            Self::ServiceClosedAfterTake => NotificationDeferredRegisterErrorKind::ServiceClosedAfterTake,
            Self::ProvenanceMismatch => NotificationDeferredRegisterErrorKind::ProvenanceMismatch,
            Self::Responder(_) => NotificationDeferredRegisterErrorKind::Responder,
            Self::Expiry(error) => NotificationDeferredRegisterErrorKind::Expiry(error.kind()),
            Self::Registry(error) => NotificationDeferredRegisterErrorKind::Registry(error.kind()),
        }
    }
}

impl fmt::Debug for NotificationDeferredRegisterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NotificationDeferredRegisterError")
            .field("kind", &self.kind())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for NotificationDeferredRegisterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Notification deferred registration failed: {:?}",
            self.kind()
        )
    }
}

impl Error for NotificationDeferredRegisterError {}

const fn is_candidate_race(kind: DeferredClaimErrorKind) -> bool {
    matches!(
        kind,
        DeferredClaimErrorKind::NotFound
            | DeferredClaimErrorKind::AlreadyClaimed
            | DeferredClaimErrorKind::AlreadyCompleted
            | DeferredClaimErrorKind::SessionClosed
            | DeferredClaimErrorKind::DeadlineExpired
    )
}
