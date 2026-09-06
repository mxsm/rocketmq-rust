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
use std::sync::atomic::AtomicU64;
#[cfg(test)]
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::RuntimeResult;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskId;
use rocketmq_store::ArcMessageFilter;
use rocketmq_transport::api::ClaimedDeferred;
use rocketmq_transport::api::DeferredAdmission;
use rocketmq_transport::api::DeferredAdmissionAcquireOutcome;
use rocketmq_transport::api::DeferredAdmissionSnapshot;
use rocketmq_transport::api::DeferredClaimOutcome;
use rocketmq_transport::api::DeferredExpiryBatch;
use rocketmq_transport::api::DeferredExpiryBatchStats;
use rocketmq_transport::api::DeferredExpiryMargins;
use rocketmq_transport::api::DeferredExpiryOutcome;
use rocketmq_transport::api::DeferredId;
use rocketmq_transport::api::DeferredParts;
use rocketmq_transport::api::DeferredRegistration;
use rocketmq_transport::api::DeferredRegistry;
use rocketmq_transport::api::DeferredRegistryOutcome;
use rocketmq_transport::api::DeferredRegistryRecovery;
use rocketmq_transport::api::DeferredRegistryShutdownOutcome;
use rocketmq_transport::api::DeferredResponderOutcome;
use rocketmq_transport::api::DeferredResumeOutcome;
use rocketmq_transport::api::DeferredResumeRetainedSize;
use rocketmq_transport::api::DeferredResumeSubmitOutcome;
use rocketmq_transport::api::DeferredRetainedSizeParts;
use rocketmq_transport::api::DeferredWakeReason;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RemotingResponse;
use rocketmq_transport::api::RequestOrigin;
use rocketmq_transport::api::TransportContractViolation;
use rocketmq_transport::api::TransportError;

use super::deadline::NotificationWaitDeadline;
use super::deadline::NotificationWaitDeadlineOperationalError;
use super::deadline::NotificationWaitDeadlineOutcome;
use super::deadline::NotificationWaitDeadlineRejection;
use super::index::NotificationArrivalView;
use super::index::NotificationCandidateReservation;
use super::index::NotificationCandidateSelection;
use super::index::NotificationCriteriaIndex;
use super::index::NotificationCriteriaKey;
use super::index::NotificationCriteriaLimits;
use super::index::NotificationIndexOperationalError;
use super::index::NotificationIndexReserveOutcome;
use super::index::NotificationIndexReserveRejection;
use super::index::NotificationIndexSnapshot;
use super::index::NotificationMatchCriteria;
use super::index::NotificationScanCursor;
use crate::long_polling::pending_arrival_latch::PendingArrivalInsertOperationalError;
use crate::long_polling::pending_arrival_latch::PendingArrivalInsertOutcome;
use crate::long_polling::pending_arrival_latch::PendingArrivalInsertRejection;
use crate::long_polling::pending_arrival_latch::PendingArrivalLatch;
use crate::long_polling::pending_arrival_latch::PendingArrivalReservation;
use crate::long_polling::pending_arrival_latch::PendingOffsetRangeLatch;
use crate::long_polling::pending_arrival_latch::PendingOffsetRangeReservation;
use crate::long_polling::pending_arrival_latch::PendingOffsetTarget;

mod continuation;
mod data;

use continuation::ContinuationAdmission;
use continuation::ContinuationPermit;
pub(crate) use continuation::NotificationArrivalContinuation;
use continuation::NotificationPendingArrival;
use continuation::NotificationPendingArrivalKey;
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
    pending_arrivals: Arc<PendingArrivalLatch<NotificationPendingArrivalKey, NotificationPendingArrival>>,
    pending_offsets: Arc<PendingOffsetRangeLatch<NotificationPendingOffsetTarget>>,
    pending_arrival_sequence: AtomicU64,
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
        let limits = admission.limits();
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
            pending_arrivals: PendingArrivalLatch::new(limits.max_waiters(), limits.max_retained_bytes()),
            pending_offsets: PendingOffsetRangeLatch::new(
                combined_budget(limits.max_waiters(), continuation_count.get()),
                combined_budget(limits.max_retained_bytes(), continuation_bytes.get()),
            ),
            pending_arrival_sequence: AtomicU64::new(0),
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
    ) -> Result<NotificationDeferredPrepareOutcome, NotificationDeferredPrepareFailure> {
        if request.command().is_oneway_rpc() {
            return Ok(NotificationDeferredPrepareOutcome::Rejected(
                NotificationDeferredPrepareRejection::OneWay,
            ));
        }
        let header = request
            .command()
            .decode_command_custom_header::<NotificationRequestHeader>()
            .map_err(NotificationDeferredPrepareFailure::Header)?;
        let effective_peer = match request.origin() {
            RequestOrigin::Network { peer } => peer.address(),
            _ => {
                return Ok(NotificationDeferredPrepareOutcome::Rejected(
                    NotificationDeferredPrepareRejection::EmbeddedOrigin,
                ));
            }
        };
        let Ok(wall_now) = i64::try_from(current_millis()) else {
            return Err(NotificationDeferredPrepareFailure::WallTimeOverflow);
        };
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
    ) -> Result<NotificationDeferredPrepareOutcome, NotificationDeferredPrepareFailure> {
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
    ) -> Result<NotificationDeferredPrepareOutcome, NotificationDeferredPrepareFailure> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(NotificationDeferredPrepareOutcome::Rejected(
                NotificationDeferredPrepareRejection::ServiceClosed,
            ));
        }
        if self.expiry_margins.recovery().is_zero() || self.expiry_margins.write().is_zero() {
            return Err(NotificationDeferredPrepareFailure::InvalidExpiryMargins);
        }
        let deadline = match NotificationWaitDeadline::checked(
            request.header.born_time,
            request.header.poll_time,
            wall_now,
            monotonic_now,
        ) {
            Ok(NotificationWaitDeadlineOutcome::Pending(deadline)) => deadline,
            Ok(NotificationWaitDeadlineOutcome::Rejected(rejection)) => {
                return Ok(NotificationDeferredPrepareOutcome::Rejected(
                    NotificationDeferredPrepareRejection::Deadline(rejection),
                ));
            }
            Err(error) => return Err(NotificationDeferredPrepareFailure::Deadline(error)),
        };
        let dynamic_bytes = request.estimated_dynamic_bytes()?;
        let Some(resume_bytes) = retained.resume_bytes.checked_add(dynamic_bytes) else {
            return Err(NotificationDeferredPrepareFailure::RetainedSizeOverflow);
        };
        let Some(filter_bytes) = retained
            .filter_bytes
            .checked_add(std::mem::size_of::<NotificationMatchCriteria>())
        else {
            return Err(NotificationDeferredPrepareFailure::RetainedSizeOverflow);
        };
        let retained_size = DeferredRegistry::<ResumeNotification>::try_retained_size(
            DeferredRetainedSizeParts::new(resume_bytes)
                .with_filter_bytes(filter_bytes)
                .with_secondary_index_bytes(NotificationCriteriaIndex::<DeferredId>::retained_bytes_per_entry())
                .with_metadata_bytes(retained.metadata_bytes),
        )
        .map_err(NotificationDeferredPrepareFailure::Contract)?;
        let key = NotificationCriteriaKey::from_parts(request.topic(), request.consumer_group(), request.queue_id());
        let reservation = match self
            .index
            .reserve_at(key, monotonic_now)
            .map_err(NotificationDeferredPrepareFailure::Index)?
        {
            NotificationIndexReserveOutcome::Reserved(reservation) => reservation,
            NotificationIndexReserveOutcome::Rejected(rejection) => {
                return Ok(NotificationDeferredPrepareOutcome::Rejected(
                    NotificationDeferredPrepareRejection::IndexCapacity(rejection),
                ));
            }
        };
        let permit = match self.admission.try_reserve(retained_size) {
            DeferredAdmissionAcquireOutcome::Acquired(permit) => permit,
            outcome => {
                return Ok(NotificationDeferredPrepareOutcome::Rejected(
                    NotificationDeferredPrepareRejection::Admission(outcome),
                ));
            }
        };
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
            return Ok(NotificationDeferredPrepareOutcome::Rejected(
                NotificationDeferredPrepareRejection::ServiceClosed,
            ));
        }
        Ok(NotificationDeferredPrepareOutcome::Prepared(Box::new(prepared)))
    }

    /// Takes the responder only after every recoverable reservation succeeds.
    pub(crate) fn register(
        &self,
        prepared: PreparedNotificationRegistration,
        request: &mut RemotingRequest,
    ) -> Result<NotificationDeferredRegisterOutcome, NotificationDeferredRegisterFailure> {
        if !prepared
            .provenance
            .is_some_and(|provenance| provenance.matches(request))
        {
            return Ok(NotificationDeferredRegisterOutcome::Rejected(Box::new(
                NotificationDeferredRegisterRejection::ProvenanceMismatch,
            )));
        }
        if self.closed.load(Ordering::Acquire) {
            return Ok(NotificationDeferredRegisterOutcome::Rejected(Box::new(
                NotificationDeferredRegisterRejection::ServiceClosedBeforeTake,
            )));
        }
        let responder = match request.take_deferred_responder() {
            DeferredResponderOutcome::Taken(responder) => responder,
            outcome => {
                return Ok(NotificationDeferredRegisterOutcome::Rejected(Box::new(
                    NotificationDeferredRegisterRejection::Responder(outcome),
                )));
            }
        };
        #[cfg(test)]
        let register_fault = self.register_fault.swap(0, Ordering::AcqRel);
        #[cfg(test)]
        if register_fault == REGISTER_FAULT_CLOSE_AFTER_TAKE {
            let _ = self.shutdown();
        }
        if self.closed.load(Ordering::Acquire) {
            drop(responder);
            return Ok(NotificationDeferredRegisterOutcome::Rejected(Box::new(
                NotificationDeferredRegisterRejection::ServiceClosedAfterTake,
            )));
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
        let mut parts = DeferredParts::new(responder, permit);
        match parts.try_with_expiry(protocol_at, self.expiry_margins) {
            Ok(DeferredExpiryOutcome::Attached) => {}
            Ok(outcome) => {
                return Ok(NotificationDeferredRegisterOutcome::Rejected(Box::new(
                    NotificationDeferredRegisterRejection::Expiry { outcome, parts },
                )));
            }
            Err(violation) => {
                return Err(NotificationDeferredRegisterFailure::Contract {
                    violation,
                    parts: Box::new(parts),
                });
            }
        };
        #[cfg(test)]
        if register_fault == REGISTER_FAULT_BUILDER_AFTER_TAKE {
            self.closed.store(true, Ordering::Release);
            let _ = self.registry.shutdown();
        }
        let registration = match self.registry.register_with(parts, move |id| {
            let index_lease = reservation.publish(id, deadline, Arc::clone(&criteria));
            Ok::<_, Infallible>(ResumeNotification::new(request, criteria, deadline, index_lease))
        }) {
            DeferredRegistryOutcome::Registered(registration) => {
                Ok(NotificationDeferredRegisterOutcome::Registered(Box::new(registration)))
            }
            DeferredRegistryOutcome::DuplicateRequest(recovery) => {
                release_deferred_registry_recovery(recovery);
                Ok(NotificationDeferredRegisterOutcome::Rejected(Box::new(
                    NotificationDeferredRegisterRejection::DuplicateRequest,
                )))
            }
            DeferredRegistryOutcome::IdentityExhausted(recovery) => {
                release_deferred_registry_recovery(recovery);
                Err(NotificationDeferredRegisterFailure::IdentityExhausted)
            }
            DeferredRegistryOutcome::ParentCancelled => Ok(NotificationDeferredRegisterOutcome::Rejected(Box::new(
                NotificationDeferredRegisterRejection::ParentCancelled,
            ))),
            DeferredRegistryOutcome::SessionClosed => Ok(NotificationDeferredRegisterOutcome::Rejected(Box::new(
                NotificationDeferredRegisterRejection::SessionClosed,
            ))),
            DeferredRegistryOutcome::DeadlineExpired => Ok(NotificationDeferredRegisterOutcome::Rejected(Box::new(
                NotificationDeferredRegisterRejection::DeadlineExpired,
            ))),
            DeferredRegistryOutcome::BuilderRejected { error, parts } => {
                drop(parts);
                match error {}
            }
            DeferredRegistryOutcome::ContractViolation { violation, recovery } => {
                release_deferred_registry_recovery(recovery);
                Err(NotificationDeferredRegisterFailure::RegistryContract(violation))
            }
            DeferredRegistryOutcome::OperationalFailure { error, recovery } => {
                release_deferred_registry_recovery(recovery);
                Err(NotificationDeferredRegisterFailure::RegistryOperational(error))
            }
        };
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

    pub(crate) fn arrival_cursor(&self, arrival: NotificationArrivalView<'_>) -> NotificationScanCursor {
        self.index.scan_cursor(&arrival)
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
                Ok(DeferredClaimOutcome::Claimed(claim)) => claims.push(claim),
                Ok(outcome) if is_candidate_race(&outcome) => {}
                Ok(_) | Err(_) => {}
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
    ) -> Result<DeferredClaimOutcome<ResumeNotification>, TransportError> {
        match self.registry.claim(id, reason).await? {
            DeferredClaimOutcome::Claimed(mut claimed) => {
                drop(claimed.resume_data_mut().take_index_lease());
                Ok(DeferredClaimOutcome::Claimed(claimed))
            }
            outcome => Ok(outcome),
        }
    }

    pub(crate) async fn claim_arrival_candidate(
        &self,
        candidate: NotificationCandidateReservation,
    ) -> Result<DeferredClaimOutcome<ResumeNotification>, TransportError> {
        let result = self.claim(candidate.id(), DeferredWakeReason::MessageArrived).await;
        drop(candidate);
        result
    }

    pub(crate) fn admit_continuation(
        &self,
        arrival: NotificationArrivalView<'_>,
        cursor: NotificationScanCursor,
    ) -> Result<
        NotificationContinuationOutcome<NotificationArrivalContinuation>,
        NotificationContinuationOperationalError,
    > {
        if cursor.is_complete() {
            return Ok(NotificationContinuationOutcome::Rejected(
                NotificationContinuationRejection::AlreadyComplete,
            ));
        }
        let remaining_conflicts = self.conflict_limit.get().saturating_sub(cursor.conflicts_spent());
        if remaining_conflicts == 0 {
            return Ok(NotificationContinuationOutcome::Rejected(
                NotificationContinuationRejection::ConflictBudgetExhausted,
            ));
        }
        let retained_bytes = OwnedNotificationArrival::retained_bytes(arrival, &cursor)?;
        let permit = match self.continuation_admission.reserve(retained_bytes)? {
            NotificationContinuationOutcome::Continued(permit) => permit,
            NotificationContinuationOutcome::Rejected(rejection) => {
                return Ok(NotificationContinuationOutcome::Rejected(rejection));
            }
        };
        let owned = OwnedNotificationArrival::try_from_view(arrival)?;
        Ok(NotificationContinuationOutcome::Continued(
            NotificationArrivalContinuation {
                owned,
                cursor,
                remaining_conflicts,
                _permit: permit,
            },
        ))
    }

    pub(crate) fn latch_arrival(
        &self,
        arrival: NotificationArrivalView<'_>,
        cursor: NotificationScanCursor,
    ) -> Result<NotificationPendingArrivalOutcome, NotificationPendingArrivalOperationalError> {
        if cursor.is_complete() {
            return Ok(NotificationPendingArrivalOutcome::Rejected(
                NotificationPendingArrivalRejection::Continuation(NotificationContinuationRejection::AlreadyComplete),
            ));
        }
        let key = NotificationPendingArrivalKey::new(
            self.pending_arrival_sequence.fetch_add(1, Ordering::Relaxed),
            arrival.topic().clone(),
            arrival.queue_id(),
        );
        let remaining_conflicts = self.conflict_limit.get().saturating_sub(cursor.conflicts_spent());
        if remaining_conflicts == 0 {
            return Ok(NotificationPendingArrivalOutcome::Rejected(
                NotificationPendingArrivalRejection::Continuation(
                    NotificationContinuationRejection::ConflictBudgetExhausted,
                ),
            ));
        }
        let pending =
            match NotificationPendingArrival::new(arrival, cursor, remaining_conflicts, self.conflict_limit.get())
                .map_err(NotificationPendingArrivalOperationalError::Continuation)?
            {
                NotificationContinuationOutcome::Continued(pending) => pending,
                NotificationContinuationOutcome::Rejected(rejection) => {
                    return Ok(NotificationPendingArrivalOutcome::Rejected(
                        NotificationPendingArrivalRejection::Continuation(rejection),
                    ));
                }
            };
        match self.pending_arrivals.insert(key, pending) {
            Ok(PendingArrivalInsertOutcome::Inserted) => Ok(NotificationPendingArrivalOutcome::Latched),
            Ok(PendingArrivalInsertOutcome::Rejected(rejection)) => Ok(NotificationPendingArrivalOutcome::Rejected(
                NotificationPendingArrivalRejection::Latch(rejection),
            )),
            Err(error) => Err(NotificationPendingArrivalOperationalError::Latch(error)),
        }
    }

    pub(crate) fn pending_arrival_reservations(&self) -> Vec<NotificationPendingArrivalReservation> {
        self.pending_arrivals.reserve_batch(self.scan_limit.get())
    }

    pub(crate) fn latch_offset(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        logical_offset: i64,
    ) -> Result<PendingArrivalInsertOutcome, PendingArrivalInsertOperationalError> {
        if logical_offset <= 0 || !self.index.has_arrival_target(topic, queue_id) {
            return Ok(PendingArrivalInsertOutcome::Inserted);
        }
        self.pending_offsets
            .retain_targets(|target| self.index.has_arrival_target(target.topic(), target.queue_id()));
        self.pending_offsets.merge(
            NotificationPendingOffsetTarget::new(topic.clone(), queue_id),
            logical_offset - 1,
        )
    }

    pub(crate) fn pending_offset_reservations(&self) -> Vec<NotificationPendingOffsetReservation> {
        self.pending_offsets
            .reserve_batch(self.scan_limit.get())
            .into_iter()
            .filter_map(|reservation| {
                let permit = match self.continuation_admission.reserve(reservation.retained_bytes()) {
                    Ok(NotificationContinuationOutcome::Continued(permit)) => permit,
                    Ok(NotificationContinuationOutcome::Rejected(_)) | Err(_) => return None,
                };
                Some(NotificationPendingOffsetReservation {
                    reservation,
                    _permit: permit,
                })
            })
            .collect()
    }

    pub(crate) fn reserve_pending_arrival_batch(
        &self,
        pending: &mut NotificationPendingArrival,
    ) -> NotificationPendingCandidateBatch {
        if self.closed.load(Ordering::Acquire) {
            return NotificationPendingCandidateBatch {
                candidates: Vec::new(),
                exhausted: true,
            };
        }
        let prepared = {
            let view = pending.owned.view();
            let batch_conflicts = pending.remaining_conflicts.min(self.scan_limit.get());
            self.prepare_arrival_batch_with_conflict_budget(
                view,
                Some(std::mem::replace(&mut pending.cursor, NotificationScanCursor::empty())),
                batch_conflicts,
            )
        };
        pending.remaining_conflicts = pending.remaining_conflicts.saturating_sub(prepared.conflicts());
        let (candidates, cursor) = prepared.into_parts();
        pending.cursor = cursor;
        NotificationPendingCandidateBatch {
            exhausted: pending.cursor.is_complete() || pending.remaining_conflicts == 0,
            candidates,
        }
    }

    pub(crate) fn reserve_offset_replay_batch(
        &self,
        target: &NotificationPendingOffsetTarget,
        cursor: Option<NotificationScanCursor>,
    ) -> NotificationPreparedArrivalBatch {
        let arrival = NotificationArrivalView::new(target.topic(), target.queue_id()).forced();
        self.prepare_arrival_batch(arrival, cursor)
    }

    pub(crate) fn replay_read_limit(&self) -> i32 {
        i32::try_from(self.scan_limit.get()).unwrap_or(i32::MAX)
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

    /// Runs post-callback filtering batches while the caller retains routing
    /// ownership for each bounded candidate batch.
    pub(crate) fn spawn_candidate_continuation<F, Fut>(
        self: &Arc<Self>,
        task_group: &TaskGroup,
        mut continuation: NotificationArrivalContinuation,
        handle_candidates: Arc<F>,
    ) -> RuntimeResult<TaskId>
    where
        F: Fn(Vec<NotificationCandidateReservation>) -> Fut + Send + Sync + 'static,
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
                continuation.remaining_conflicts =
                    continuation.remaining_conflicts.saturating_sub(prepared.conflicts());
                let (candidates, cursor) = prepared.into_parts();
                handle_candidates(candidates).await;
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
    ) -> Result<DeferredResumeOutcome, TransportError>
    where
        F: FnOnce(ResumeNotification, DeferredWakeReason) -> Fut + Send + 'static,
        Fut: Future<Output = rocketmq_error::RocketMQResult<RemotingResponse>> + Send + 'static,
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

    /// Transfers one claimed Notification execution to its canonical session
    /// owner and returns after bounded admission, before response terminal.
    pub(crate) fn submit_claimed<F, Fut>(
        &self,
        claimed: ClaimedDeferred<ResumeNotification>,
        handler_retained: DeferredResumeRetainedSize,
        handler: F,
    ) -> Result<DeferredResumeSubmitOutcome, TransportError>
    where
        F: FnOnce(ResumeNotification, DeferredWakeReason) -> Fut + Send + 'static,
        Fut: Future<Output = rocketmq_error::RocketMQResult<RemotingResponse>> + Send + 'static,
    {
        let resume_executions = Arc::clone(&self.resume_executions);
        let resume_execution_bytes = Arc::clone(&self.resume_execution_bytes);
        let dynamic_bytes = handler_retained.dynamic_bytes();
        let observation = CounterObservation::new_with_bytes(resume_executions, resume_execution_bytes, dynamic_bytes);
        claimed.submit(handler_retained, handler, move |_| drop(observation))
    }

    #[must_use]
    pub(crate) fn snapshot(&self) -> NotificationDeferredSnapshot {
        let continuation = self.continuation_admission.snapshot();
        let pending = self.pending_arrivals.snapshot();
        let offsets = self.pending_offsets.snapshot();
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
            pending_arrivals: pending.count.saturating_add(offsets.count),
            pending_arrival_bytes: pending.bytes.saturating_add(offsets.bytes),
            pending_arrival_rejected: pending.rejected.saturating_add(offsets.rejected),
            pending_offset_invariant_failures: offsets.rejected,
        }
    }

    pub(crate) fn seal(&self) {
        self.closed.store(true, Ordering::Release);
        self.pending_arrivals.seal();
        self.pending_offsets.seal();
    }

    #[must_use]
    pub(crate) fn shutdown(&self) -> DeferredRegistryShutdownOutcome {
        self.seal();
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

    pub(crate) fn into_parts(self) -> (Vec<NotificationCandidateReservation>, NotificationScanCursor) {
        (self.candidates, self.cursor)
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
    pending_arrivals: usize,
    pending_arrival_bytes: usize,
    pending_arrival_rejected: usize,
    pending_offset_invariant_failures: usize,
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

    #[must_use]
    pub(crate) const fn pending_arrivals(self) -> usize {
        self.pending_arrivals
    }

    #[must_use]
    pub(crate) const fn pending_arrival_bytes(self) -> usize {
        self.pending_arrival_bytes
    }

    #[must_use]
    pub(crate) const fn pending_arrival_rejected(self) -> usize {
        self.pending_arrival_rejected
    }

    #[must_use]
    pub(crate) const fn pending_offset_invariant_failures(self) -> usize {
        self.pending_offset_invariant_failures
    }
}

pub(crate) type NotificationPendingArrivalReservation =
    PendingArrivalReservation<NotificationPendingArrivalKey, NotificationPendingArrival>;
pub(crate) struct NotificationPendingOffsetReservation {
    reservation: PendingOffsetRangeReservation<NotificationPendingOffsetTarget>,
    _permit: ContinuationPermit,
}

impl NotificationPendingOffsetReservation {
    pub(crate) fn key(&self) -> &NotificationPendingOffsetTarget {
        self.reservation.key()
    }

    pub(crate) const fn range(&self) -> crate::long_polling::pending_arrival_latch::PendingOffsetRange {
        self.reservation.range()
    }

    pub(crate) fn finish_or_updated(
        &mut self,
    ) -> Option<crate::long_polling::pending_arrival_latch::PendingOffsetRange> {
        self.reservation.finish_or_updated()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct NotificationPendingOffsetTarget {
    topic: CheetahString,
    queue_id: i32,
}

impl NotificationPendingOffsetTarget {
    fn new(topic: CheetahString, queue_id: i32) -> Self {
        Self { topic, queue_id }
    }

    pub(crate) const fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub(crate) const fn queue_id(&self) -> i32 {
        self.queue_id
    }
}

impl PendingOffsetTarget for NotificationPendingOffsetTarget {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(self.topic.len().saturating_mul(2))
    }
}

const fn combined_budget(left: usize, right: usize) -> usize {
    match left.checked_add(right) {
        Some(combined) => combined,
        None => usize::MAX,
    }
}

#[must_use]
pub(crate) enum NotificationPendingArrivalOutcome {
    Latched,
    Rejected(NotificationPendingArrivalRejection),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NotificationPendingArrivalRejection {
    Continuation(NotificationContinuationRejection),
    Latch(PendingArrivalInsertRejection),
}

#[derive(Debug)]
pub(crate) enum NotificationPendingArrivalOperationalError {
    Continuation(NotificationContinuationOperationalError),
    Latch(PendingArrivalInsertOperationalError),
}

impl fmt::Display for NotificationPendingArrivalOperationalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Notification pending-arrival processing failed")
    }
}

impl Error for NotificationPendingArrivalOperationalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Continuation(source) => Some(source),
            Self::Latch(source) => Some(source),
        }
    }
}

pub(crate) struct NotificationPendingCandidateBatch {
    candidates: Vec<NotificationCandidateReservation>,
    exhausted: bool,
}

impl NotificationPendingCandidateBatch {
    pub(crate) const fn exhausted(&self) -> bool {
        self.exhausted
    }

    pub(crate) fn into_candidates(self) -> Vec<NotificationCandidateReservation> {
        self.candidates
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
pub(crate) enum NotificationContinuationRejection {
    AlreadyComplete,
    ConflictBudgetExhausted,
    CountFull,
    BytesFull,
}

#[must_use]
pub(crate) enum NotificationContinuationOutcome<T> {
    Continued(T),
    Rejected(NotificationContinuationRejection),
}

#[derive(Debug)]
pub(crate) enum NotificationContinuationOperationalError {
    SizeOverflow,
    Allocation(std::collections::TryReserveError),
}

impl fmt::Display for NotificationContinuationOperationalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::SizeOverflow => "Notification continuation retained size overflowed",
            Self::Allocation(_) => "Notification continuation allocation failed",
        })
    }
}

impl Error for NotificationContinuationOperationalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Allocation(source) => Some(source),
            Self::SizeOverflow => None,
        }
    }
}

pub(crate) enum NotificationDeferredPrepareRejection {
    ServiceClosed,
    OneWay,
    EmbeddedOrigin,
    Deadline(NotificationWaitDeadlineRejection),
    IndexCapacity(NotificationIndexReserveRejection),
    Admission(DeferredAdmissionAcquireOutcome),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NotificationDeferredPrepareRejectionKind {
    ServiceClosed,
    OneWay,
    EmbeddedOrigin,
    Deadline,
    Index,
    Admission,
}

impl NotificationDeferredPrepareRejection {
    pub(crate) const fn kind(&self) -> NotificationDeferredPrepareRejectionKind {
        match self {
            Self::ServiceClosed => NotificationDeferredPrepareRejectionKind::ServiceClosed,
            Self::OneWay => NotificationDeferredPrepareRejectionKind::OneWay,
            Self::EmbeddedOrigin => NotificationDeferredPrepareRejectionKind::EmbeddedOrigin,
            Self::Deadline(_) => NotificationDeferredPrepareRejectionKind::Deadline,
            Self::IndexCapacity(_) => NotificationDeferredPrepareRejectionKind::Index,
            Self::Admission(_) => NotificationDeferredPrepareRejectionKind::Admission,
        }
    }
}

#[must_use]
pub(crate) enum NotificationDeferredPrepareOutcome {
    Prepared(Box<PreparedNotificationRegistration>),
    Rejected(NotificationDeferredPrepareRejection),
}

pub(crate) enum NotificationDeferredPrepareFailure {
    WallTimeOverflow,
    InvalidExpiryMargins,
    RetainedSizeOverflow,
    Deadline(NotificationWaitDeadlineOperationalError),
    Header(RocketMQError),
    Index(NotificationIndexOperationalError),
    Contract(TransportContractViolation),
}

impl fmt::Debug for NotificationDeferredPrepareFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NotificationDeferredPrepareFailure")
            .finish_non_exhaustive()
    }
}

impl fmt::Display for NotificationDeferredPrepareFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Notification deferred preparation failed")
    }
}

impl Error for NotificationDeferredPrepareFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Header(source) => Some(source),
            Self::Index(source) => Some(source),
            Self::Contract(source) => Some(source),
            Self::Deadline(source) => Some(source),
            Self::WallTimeOverflow | Self::InvalidExpiryMargins | Self::RetainedSizeOverflow => None,
        }
    }
}

pub(crate) enum NotificationDeferredRegisterRejection {
    ServiceClosedBeforeTake,
    ServiceClosedAfterTake,
    ProvenanceMismatch,
    Responder(DeferredResponderOutcome),
    Expiry {
        outcome: DeferredExpiryOutcome,
        parts: DeferredParts,
    },
    DuplicateRequest,
    ParentCancelled,
    SessionClosed,
    DeadlineExpired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NotificationDeferredRegisterRejectionKind {
    ServiceClosedBeforeTake,
    ServiceClosedAfterTake,
    ProvenanceMismatch,
    Responder,
    Expiry,
    Registry,
}

impl NotificationDeferredRegisterRejection {
    pub(crate) const fn kind(&self) -> NotificationDeferredRegisterRejectionKind {
        match self {
            Self::ServiceClosedBeforeTake => NotificationDeferredRegisterRejectionKind::ServiceClosedBeforeTake,
            Self::ServiceClosedAfterTake => NotificationDeferredRegisterRejectionKind::ServiceClosedAfterTake,
            Self::ProvenanceMismatch => NotificationDeferredRegisterRejectionKind::ProvenanceMismatch,
            Self::Responder(_) => NotificationDeferredRegisterRejectionKind::Responder,
            Self::Expiry { .. } => NotificationDeferredRegisterRejectionKind::Expiry,
            Self::DuplicateRequest | Self::ParentCancelled | Self::SessionClosed | Self::DeadlineExpired => {
                NotificationDeferredRegisterRejectionKind::Registry
            }
        }
    }
}

#[must_use]
pub(crate) enum NotificationDeferredRegisterOutcome {
    Registered(Box<DeferredRegistration>),
    Rejected(Box<NotificationDeferredRegisterRejection>),
}

pub(crate) enum NotificationDeferredRegisterFailure {
    IdentityExhausted,
    RegistryContract(TransportContractViolation),
    RegistryOperational(TransportError),
    Contract {
        violation: TransportContractViolation,
        parts: Box<DeferredParts>,
    },
}

fn release_deferred_registry_recovery<R, F>(recovery: DeferredRegistryRecovery<R, F>) {
    match recovery {
        DeferredRegistryRecovery::None => {}
        DeferredRegistryRecovery::Request(request) => drop(request),
        DeferredRegistryRecovery::Parts(parts) => drop(parts),
        DeferredRegistryRecovery::Builder { builder, parts } => {
            drop(builder);
            drop(parts);
        }
    }
}

impl fmt::Debug for NotificationDeferredRegisterFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NotificationDeferredRegisterFailure")
            .finish_non_exhaustive()
    }
}

impl fmt::Display for NotificationDeferredRegisterFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Notification deferred registration failed")
    }
}

impl Error for NotificationDeferredRegisterFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RegistryContract(violation) => Some(violation),
            Self::RegistryOperational(error) => Some(error),
            Self::Contract { violation, .. } => Some(violation),
            Self::IdentityExhausted => None,
        }
    }
}

const fn is_candidate_race<R>(outcome: &DeferredClaimOutcome<R>) -> bool
where
    R: Send + 'static,
{
    matches!(
        outcome,
        DeferredClaimOutcome::NotFound
            | DeferredClaimOutcome::AlreadyClaimed
            | DeferredClaimOutcome::AlreadyCompleted
            | DeferredClaimOutcome::SessionClosed
            | DeferredClaimOutcome::DeadlineExpired
    )
}
