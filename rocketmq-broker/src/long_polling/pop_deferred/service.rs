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

use std::alloc::Layout;
use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader;
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
use rocketmq_transport::api::RequestId;
use rocketmq_transport::api::RequestOrigin;
use rocketmq_transport::api::SessionId;
use rocketmq_transport::api::TransportContractViolation;
use rocketmq_transport::api::TransportError;
use tokio::sync::oneshot;

use crate::long_polling::pending_arrival_latch::PendingArrivalInsertOperationalError;
use crate::long_polling::pending_arrival_latch::PendingArrivalInsertOutcome;
use crate::long_polling::pending_arrival_latch::PendingArrivalInsertRejection;
use crate::long_polling::pending_arrival_latch::PendingArrivalLatch;
use crate::long_polling::pending_arrival_latch::PendingArrivalReservation;
use crate::long_polling::pending_arrival_latch::PendingOffsetRangeLatch;
use crate::long_polling::pending_arrival_latch::PendingOffsetRangeReservation;
use crate::long_polling::pending_arrival_latch::PendingOffsetTarget;

use super::deadline::LongPollingDeadline;
use super::deadline::LongPollingDeadlineError;
use super::deadline::LongPollingDeadlineOutcome;
use super::index::PopArrival;
use super::index::PopArrivalView;
use super::index::PopCandidateReservation;
use super::index::PopCriteriaIndex;
use super::index::PopCriteriaKey;
use super::index::PopCriteriaLimits;
use super::index::PopFanoutBatch;
use super::index::PopFanoutCursor;
use super::index::PopIndexLease;
use super::index::PopIndexOperationalError;
use super::index::PopIndexRejection;
use super::index::PopIndexReservation;
use super::index::PopIndexReserveOutcome;
use super::index::PopIndexSnapshot;
use super::index::PopMatchCriteria;
use super::index::PopSelectionOrder;

mod continuation;

pub(crate) use continuation::PopArrivalContinuation;
use continuation::PopContinuationAdmission;
pub(crate) use continuation::PopContinuationError;
pub(crate) use continuation::PopContinuationOutcome;
use continuation::PopContinuationPermit;
use continuation::PopPendingArrival;
use continuation::PopPendingArrivalKey;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PopWakeupOutcome {
    ProcessingCompleted,
    ProcessingFailed,
    InactiveChannel,
    AlreadyCompleted,
    ProcessorUnavailable,
    ServiceNotRunning,
    ServiceCancelled,
}

pub(crate) type PopWakeupCompletion = oneshot::Receiver<PopWakeupOutcome>;

pub(crate) trait PollingCountProvider: Send + Sync {
    fn polling_count(&self, topic: &CheetahString, consumer_group: &CheetahString, queue_id: i32) -> i32;
}

/// Typed POP request data retained without a channel or connection context.
pub(crate) struct PopRequestData {
    header: PopMessageRequestHeader,
    caller_host: CheetahString,
}

impl PopRequestData {
    const fn new(header: PopMessageRequestHeader, caller_host: CheetahString) -> Self {
        Self { header, caller_host }
    }

    #[cfg(test)]
    pub(crate) const fn from_test_header(header: PopMessageRequestHeader, caller_host: CheetahString) -> Self {
        Self::new(header, caller_host)
    }

    #[must_use]
    pub(crate) const fn header(&self) -> &PopMessageRequestHeader {
        &self.header
    }

    #[must_use]
    pub(crate) const fn topic(&self) -> &CheetahString {
        &self.header.topic
    }

    #[must_use]
    pub(crate) const fn consumer_group(&self) -> &CheetahString {
        &self.header.consumer_group
    }

    #[must_use]
    pub(crate) const fn queue_id(&self) -> i32 {
        self.header.queue_id
    }

    /// Returns the trusted effective peer captured by the ingress boundary.
    #[must_use]
    pub(crate) const fn caller_host(&self) -> &CheetahString {
        &self.caller_host
    }

    fn try_estimated_dynamic_bytes(&self) -> Result<usize, PopDeferredPrepareError> {
        let mut bytes = checked_retained_sum([
            self.header.topic.len(),
            self.header.consumer_group.len(),
            self.caller_host.len(),
        ])?;
        for value in [
            self.header.exp_type.as_ref(),
            self.header.exp.as_ref(),
            self.header.attempt_id.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            bytes = bytes
                .checked_add(value.len())
                .ok_or(PopDeferredPrepareError::RetainedSizeOverflow)?;
        }
        if let Some(rpc) = self
            .header
            .topic_request_header
            .as_ref()
            .and_then(|topic| topic.rpc.as_ref())
        {
            for value in [rpc.namespace.as_ref(), rpc.broker_name.as_ref()].into_iter().flatten() {
                bytes = bytes
                    .checked_add(value.len())
                    .ok_or(PopDeferredPrepareError::RetainedSizeOverflow)?;
            }
        }
        Ok(bytes)
    }

    #[cfg(test)]
    pub(crate) fn estimated_dynamic_bytes(&self) -> Result<usize, PopDeferredPrepareError> {
        self.try_estimated_dynamic_bytes()
    }

    #[must_use]
    pub(crate) fn into_header(self) -> PopMessageRequestHeader {
        self.header
    }
}

fn checked_retained_sum<const N: usize>(parts: [usize; N]) -> Result<usize, PopDeferredPrepareError> {
    parts.into_iter().try_fold(0usize, |total, part| {
        total
            .checked_add(part)
            .ok_or(PopDeferredPrepareError::RetainedSizeOverflow)
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PreparedRequestProvenance {
    request_id: RequestId,
    session_id: SessionId,
}

impl PreparedRequestProvenance {
    fn capture(request: &RemotingRequest) -> Self {
        Self {
            request_id: request.original_identity().request_id(),
            session_id: request.session().id(),
        }
    }

    fn matches(self, request: &RemotingRequest) -> bool {
        self.request_id == request.original_identity().request_id() && self.session_id == request.session().id()
    }
}

/// Caller-declared dynamic ownership not represented by inline `ResumePop`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PopRetainedEstimate {
    resume_bytes: usize,
    filter_bytes: usize,
    metadata_bytes: usize,
}

impl PopRetainedEstimate {
    pub(crate) const fn new(resume_bytes: usize, filter_bytes: usize, metadata_bytes: usize) -> Self {
        Self {
            resume_bytes,
            filter_bytes,
            metadata_bytes,
        }
    }
}

/// Affine business ownership carried from registry claim into POP recovery.
#[must_use]
pub(crate) struct ResumePop {
    request: PopRequestData,
    criteria: Arc<PopMatchCriteria>,
    protocol_wait_deadline: LongPollingDeadline,
    index_lease: Option<PopIndexLease>,
}

impl ResumePop {
    fn new(
        request: PopRequestData,
        criteria: Arc<PopMatchCriteria>,
        protocol_wait_deadline: LongPollingDeadline,
        index_lease: PopIndexLease,
    ) -> Self {
        Self {
            request,
            criteria,
            protocol_wait_deadline,
            index_lease: Some(index_lease),
        }
    }

    #[must_use]
    pub(crate) const fn request(&self) -> &PopRequestData {
        &self.request
    }

    #[must_use]
    pub(crate) fn subscription(&self) -> Option<&SubscriptionData> {
        self.criteria.subscription()
    }

    #[must_use]
    pub(crate) fn filter(&self) -> Option<&ArcMessageFilter> {
        self.criteria.filter()
    }

    #[must_use]
    pub(crate) const fn protocol_wait_deadline(&self) -> LongPollingDeadline {
        self.protocol_wait_deadline
    }

    pub(crate) fn take_index_lease(&mut self) -> Option<PopIndexLease> {
        self.index_lease.take()
    }
}

/// Fully admitted pre-responder ownership for one POP registration.
#[must_use]
pub(crate) struct PreparedPopRegistration {
    request: PopRequestData,
    criteria: Arc<PopMatchCriteria>,
    deadline: LongPollingDeadline,
    reservation: PopIndexReservation,
    permit: rocketmq_transport::api::DeferredWaitPermit,
    provenance: Option<PreparedRequestProvenance>,
}

impl PreparedPopRegistration {
    #[must_use]
    pub(crate) const fn deadline(&self) -> LongPollingDeadline {
        self.deadline
    }

    #[must_use]
    pub(crate) const fn retained_bytes(&self) -> usize {
        self.permit.retained_bytes()
    }
}

/// Private composition of POP wait admission, criteria indexing, and registry ownership.
pub(crate) struct PopDeferredService {
    admission: DeferredAdmission,
    registry: DeferredRegistry<ResumePop>,
    index: PopCriteriaIndex,
    expiry_margins: DeferredExpiryMargins,
    sweep_limit: NonZeroUsize,
    continuation_admission: Arc<PopContinuationAdmission>,
    pending_arrivals: Arc<PendingArrivalLatch<PopPendingArrivalKey, PopPendingArrival>>,
    pending_offsets: Arc<PendingOffsetRangeLatch<PopPendingOffsetTarget>>,
    pending_arrival_sequence: AtomicU64,
    resume_executions: Arc<AtomicUsize>,
    resume_execution_bytes: Arc<AtomicUsize>,
    closed: AtomicBool,
}

/// Affine completion owner for one POP wake attempt.
///
/// The sender is completed only from the authoritative registry claim or
/// canonical deferred resume/write result. Dropping it reports cancellation
/// so legacy callers never wait forever for an accepted wake attempt.
#[must_use]
pub(crate) struct PopDeferredWakeupObserver {
    sender: Option<oneshot::Sender<PopWakeupOutcome>>,
}

impl PopDeferredWakeupObserver {
    pub(crate) fn new() -> (Self, PopWakeupCompletion) {
        let (sender, completion) = oneshot::channel();
        (Self { sender: Some(sender) }, completion)
    }

    pub(crate) fn complete_claim_result(self, result: &Result<DeferredClaimOutcome<ResumePop>, TransportError>) {
        self.complete(pop_wakeup_outcome_from_claim_result(result));
    }

    fn complete_resume_result(self, result: &Result<DeferredResumeOutcome, TransportError>) {
        self.complete(pop_wakeup_outcome_from_resume_result(result));
    }

    fn complete(mut self, outcome: PopWakeupOutcome) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(outcome);
        }
    }
}

impl Drop for PopDeferredWakeupObserver {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(PopWakeupOutcome::ServiceCancelled);
        }
    }
}

impl PopDeferredService {
    pub(crate) fn new(
        admission: DeferredAdmission,
        index_limits: PopCriteriaLimits,
        expiry_margins: DeferredExpiryMargins,
        sweep_limit: NonZeroUsize,
    ) -> Self {
        let limits = admission.limits();
        let continuation_bytes = limits.max_retained_bytes();
        Self {
            admission,
            registry: DeferredRegistry::new(),
            index: PopCriteriaIndex::new(index_limits),
            expiry_margins,
            sweep_limit,
            continuation_admission: Arc::new(PopContinuationAdmission::new(sweep_limit.get(), continuation_bytes)),
            pending_arrivals: PendingArrivalLatch::new(limits.max_waiters(), continuation_bytes),
            pending_offsets: PendingOffsetRangeLatch::new(
                combined_budget(limits.max_waiters(), sweep_limit.get()),
                combined_budget(continuation_bytes, continuation_bytes),
            ),
            pending_arrival_sequence: AtomicU64::new(0),
            resume_executions: Arc::new(AtomicUsize::new(0)),
            resume_execution_bytes: Arc::new(AtomicUsize::new(0)),
            closed: AtomicBool::new(false),
        }
    }

    /// Completes every recoverable business-side allocation before responder transfer.
    pub(crate) fn prepare(
        &self,
        request: &RemotingRequest,
        subscription: Option<SubscriptionData>,
        filter: Option<ArcMessageFilter>,
        retained: PopRetainedEstimate,
    ) -> Result<PopDeferredPrepareOutcome, PopDeferredPrepareError> {
        let header = request
            .command()
            .decode_command_custom_header::<PopMessageRequestHeader>()
            .map_err(PopDeferredPrepareError::Header)?;
        let caller_host = match request.origin() {
            RequestOrigin::Network { peer } => CheetahString::from_string(peer.address().to_string()),
            RequestOrigin::Embedded { .. } => return Err(PopDeferredPrepareError::EmbeddedOrigin),
            _ => return Err(PopDeferredPrepareError::EmbeddedOrigin),
        };
        let wall_now = current_millis();
        self.prepare_data_at(
            PopRequestData::new(header, caller_host),
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
        request: PopRequestData,
        subscription: Option<SubscriptionData>,
        filter: Option<ArcMessageFilter>,
        retained: PopRetainedEstimate,
        wall_now: u64,
        monotonic_now: tokio::time::Instant,
    ) -> Result<PopDeferredPrepareOutcome, PopDeferredPrepareError> {
        self.prepare_data_at(request, subscription, filter, retained, wall_now, monotonic_now, None)
    }

    fn prepare_data_at(
        &self,
        request: PopRequestData,
        subscription: Option<SubscriptionData>,
        filter: Option<ArcMessageFilter>,
        retained: PopRetainedEstimate,
        wall_now: u64,
        monotonic_now: tokio::time::Instant,
        provenance: Option<PreparedRequestProvenance>,
    ) -> Result<PopDeferredPrepareOutcome, PopDeferredPrepareError> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(PopDeferredPrepareOutcome::Rejected(
                PopDeferredPrepareRejection::ServiceClosed,
            ));
        }
        if request.caller_host.is_empty() {
            return Err(PopDeferredPrepareError::MissingCallerHost);
        }
        if self.expiry_margins.recovery().is_zero() || self.expiry_margins.write().is_zero() {
            return Err(PopDeferredPrepareError::InvalidExpiryMargins);
        }
        let deadline = match LongPollingDeadline::checked(
            request.header.born_time,
            request.header.poll_time,
            wall_now,
            monotonic_now,
        )
        .map_err(PopDeferredPrepareError::Deadline)?
        {
            LongPollingDeadlineOutcome::Pending(deadline) => deadline,
            LongPollingDeadlineOutcome::Immediate => {
                return Ok(PopDeferredPrepareOutcome::Rejected(
                    PopDeferredPrepareRejection::DeadlineElapsed,
                ));
            }
        };
        let key = PopCriteriaKey::from_parts(request.topic(), request.consumer_group(), request.queue_id());
        let reservation = match self.index.reserve(key) {
            Ok(PopIndexReserveOutcome::Reserved(reservation)) => reservation,
            Ok(PopIndexReserveOutcome::Rejected(rejection)) => {
                return Ok(PopDeferredPrepareOutcome::Rejected(PopDeferredPrepareRejection::Index(
                    rejection,
                )));
            }
            Err(error) => return Err(PopDeferredPrepareError::Index(error)),
        };
        let criteria = Arc::new(PopMatchCriteria::new(subscription, filter));
        let resume_bytes = retained
            .resume_bytes
            .checked_add(request.try_estimated_dynamic_bytes()?)
            .ok_or(PopDeferredPrepareError::RetainedSizeOverflow)?;
        let criteria_bytes = match_criteria_allocation_bytes().ok_or(PopDeferredPrepareError::RetainedSizeOverflow)?;
        let filter_bytes = retained
            .filter_bytes
            .checked_add(criteria_bytes)
            .ok_or(PopDeferredPrepareError::RetainedSizeOverflow)?;
        let index_bytes = PopCriteriaIndex::<DeferredId>::try_retained_bytes_per_entry()
            .ok_or(PopDeferredPrepareError::RetainedSizeOverflow)?;
        let retained_parts = DeferredRetainedSizeParts::new(resume_bytes)
            .with_filter_bytes(filter_bytes)
            .with_secondary_index_bytes(index_bytes)
            .with_metadata_bytes(retained.metadata_bytes);
        let retained_size = DeferredRegistry::<ResumePop>::try_retained_size(retained_parts)
            .map_err(PopDeferredPrepareError::Contract)?;
        let permit = match self.admission.try_reserve(retained_size) {
            DeferredAdmissionAcquireOutcome::Acquired(permit) => permit,
            outcome => {
                return Ok(PopDeferredPrepareOutcome::Rejected(
                    PopDeferredPrepareRejection::Admission(outcome),
                ));
            }
        };
        let prepared = PreparedPopRegistration {
            request,
            criteria,
            deadline,
            reservation,
            permit,
            provenance,
        };
        if self.closed.load(Ordering::Acquire) {
            drop(prepared);
            return Ok(PopDeferredPrepareOutcome::Rejected(
                PopDeferredPrepareRejection::ServiceClosed,
            ));
        }
        Ok(PopDeferredPrepareOutcome::Prepared(Box::new(prepared)))
    }

    /// Moves the prepared index reservation into `register_with`'s infallible builder.
    pub(crate) fn register(
        &self,
        prepared: PreparedPopRegistration,
        request: &mut RemotingRequest,
    ) -> Result<PopDeferredRegisterOutcome, PopDeferredRegisterError> {
        if !prepared
            .provenance
            .is_some_and(|provenance| provenance.matches(request))
        {
            return Ok(PopDeferredRegisterOutcome::Rejected(Box::new(
                PopDeferredRegisterRejection::ProvenanceMismatch,
            )));
        }
        if self.closed.load(Ordering::Acquire) {
            return Ok(PopDeferredRegisterOutcome::Rejected(Box::new(
                PopDeferredRegisterRejection::ServiceClosed,
            )));
        }
        let responder = match request.take_deferred_responder() {
            DeferredResponderOutcome::Taken(responder) => responder,
            outcome => {
                return Ok(PopDeferredRegisterOutcome::Rejected(Box::new(
                    PopDeferredRegisterRejection::Responder(outcome),
                )));
            }
        };
        let PreparedPopRegistration {
            request,
            criteria,
            deadline,
            reservation,
            permit,
            provenance: _,
        } = prepared;
        let mut parts = DeferredParts::new(responder, permit);
        match parts.try_with_expiry(deadline.protocol_at(), self.expiry_margins) {
            Ok(DeferredExpiryOutcome::Attached) => {}
            Ok(outcome) => {
                return Ok(PopDeferredRegisterOutcome::Rejected(Box::new(
                    PopDeferredRegisterRejection::Expiry { outcome, parts },
                )));
            }
            Err(violation) => {
                return Err(PopDeferredRegisterError::Contract {
                    violation,
                    parts: Box::new(parts),
                });
            }
        }
        match self.registry.register_with(parts, move |id| {
            let index_lease = reservation.publish(id, deadline, Arc::clone(&criteria));
            Ok::<_, Infallible>(ResumePop::new(request, criteria, deadline, index_lease))
        }) {
            DeferredRegistryOutcome::Registered(registration) => {
                Ok(PopDeferredRegisterOutcome::Registered(Box::new(registration)))
            }
            DeferredRegistryOutcome::DuplicateRequest(recovery) => {
                release_deferred_registry_recovery(recovery);
                Ok(PopDeferredRegisterOutcome::Rejected(Box::new(
                    PopDeferredRegisterRejection::RegistryRejected,
                )))
            }
            DeferredRegistryOutcome::IdentityExhausted(recovery) => {
                release_deferred_registry_recovery(recovery);
                Err(PopDeferredRegisterError::RegistryIdentityExhausted)
            }
            DeferredRegistryOutcome::ParentCancelled
            | DeferredRegistryOutcome::SessionClosed
            | DeferredRegistryOutcome::DeadlineExpired => Ok(PopDeferredRegisterOutcome::Rejected(Box::new(
                PopDeferredRegisterRejection::RegistryRejected,
            ))),
            DeferredRegistryOutcome::BuilderRejected { error, parts } => {
                drop(parts);
                match error {}
            }
            DeferredRegistryOutcome::ContractViolation { violation, recovery } => {
                release_deferred_registry_recovery(recovery);
                Err(PopDeferredRegisterError::RegistryContract(violation))
            }
            DeferredRegistryOutcome::OperationalFailure { error, recovery } => {
                release_deferred_registry_recovery(recovery);
                Err(PopDeferredRegisterError::RegistryOperational(error))
            }
        }
    }

    pub(crate) async fn claim(
        &self,
        id: DeferredId,
        reason: DeferredWakeReason,
    ) -> Result<DeferredClaimOutcome<ResumePop>, TransportError> {
        match self.registry.claim(id, reason).await? {
            DeferredClaimOutcome::Claimed(mut claimed) => {
                drop(claimed.resume_data_mut().take_index_lease());
                Ok(DeferredClaimOutcome::Claimed(claimed))
            }
            outcome => Ok(outcome),
        }
    }

    /// Reserves one matching candidate while all arrival metadata is still
    /// borrowed by the synchronous message-listener callback.
    pub(crate) fn reserve_arrival_candidate(
        &self,
        arrival: PopArrivalView<'_>,
        order: PopSelectionOrder,
    ) -> Option<PopCandidateReservation> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }
        self.index
            .reserve_next_matching_view(arrival, order, self.sweep_limit)
            .into_candidate()
    }

    /// Reserves one candidate from a target whose generation route is already stable.
    pub(crate) fn reserve_target_arrival_candidate(
        &self,
        key: &PopCriteriaKey,
        arrival: PopArrivalView<'_>,
        order: PopSelectionOrder,
    ) -> Option<PopCandidateReservation> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }
        self.index
            .reserve_target_matching_view(key, arrival, order, self.sweep_limit)
            .into_candidate()
    }

    pub(crate) async fn claim_candidate(
        &self,
        candidate: PopCandidateReservation,
        reason: DeferredWakeReason,
    ) -> Result<DeferredClaimOutcome<ResumePop>, TransportError> {
        let result = self.claim(candidate.id(), reason).await;
        drop(candidate);
        result
    }

    /// Claims a candidate selected by the bounded POP lag-refresh producer.
    pub(crate) async fn claim_forced_candidate(
        &self,
        candidate: PopCandidateReservation,
    ) -> Result<DeferredClaimOutcome<ResumePop>, TransportError> {
        self.claim_candidate(candidate, DeferredWakeReason::ForcedRefresh).await
    }

    pub(crate) async fn claim_message(
        &self,
        arrival: &PopArrival,
        order: PopSelectionOrder,
    ) -> Result<Option<ClaimedDeferred<ResumePop>>, TransportError> {
        self.claim_matching(arrival, order, DeferredWakeReason::MessageArrived)
            .await
    }

    pub(crate) async fn claim_forced(
        &self,
        arrival: PopArrival,
        order: PopSelectionOrder,
    ) -> Result<Option<ClaimedDeferred<ResumePop>>, TransportError> {
        self.claim_matching(&arrival.forced(), order, DeferredWakeReason::ForcedRefresh)
            .await
    }

    /// Removes business-index visibility, then delegates execution and writing
    /// to the canonical affine transport resume path.
    pub(crate) async fn resume_claimed<F, Fut>(
        &self,
        claimed: ClaimedDeferred<ResumePop>,
        handler_retained: DeferredResumeRetainedSize,
        handler: F,
    ) -> Result<DeferredResumeOutcome, TransportError>
    where
        F: FnOnce(ResumePop, DeferredWakeReason) -> Fut + Send + 'static,
        Fut: Future<Output = rocketmq_error::RocketMQResult<RemotingResponse>> + Send + 'static,
    {
        let observation = Arc::new(Mutex::new(None));
        let accepted = Arc::clone(&observation);
        let resume_executions = Arc::clone(&self.resume_executions);
        let resume_execution_bytes = Arc::clone(&self.resume_execution_bytes);
        let retained_bytes = handler_retained.dynamic_bytes();
        let result = claimed
            .resume(handler_retained, move |resume, reason| {
                *accepted.lock() = Some(ResumeExecutionObservation::new(
                    resume_executions,
                    resume_execution_bytes,
                    retained_bytes,
                ));
                handler(resume, reason)
            })
            .await;
        drop(observation.lock().take());
        result
    }

    /// Resumes and writes one claimed POP response, completing the legacy
    /// observer from the final canonical outcome rather than task submission.
    pub(crate) async fn resume_claimed_observed<F, Fut>(
        &self,
        claimed: ClaimedDeferred<ResumePop>,
        handler_retained: DeferredResumeRetainedSize,
        observer: PopDeferredWakeupObserver,
        handler: F,
    ) -> Result<DeferredResumeOutcome, TransportError>
    where
        F: FnOnce(ResumePop, DeferredWakeReason) -> Fut + Send + 'static,
        Fut: Future<Output = rocketmq_error::RocketMQResult<RemotingResponse>> + Send + 'static,
    {
        let result = self.resume_claimed(claimed, handler_retained, handler).await;
        observer.complete_resume_result(&result);
        result
    }

    /// Transfers one claimed POP execution to its canonical session owner and
    /// returns after bounded session admission, before handler/write terminal.
    pub(crate) fn submit_claimed<F, Fut>(
        &self,
        claimed: ClaimedDeferred<ResumePop>,
        handler_retained: DeferredResumeRetainedSize,
        handler: F,
    ) -> Result<DeferredResumeSubmitOutcome, TransportError>
    where
        F: FnOnce(ResumePop, DeferredWakeReason) -> Fut + Send + 'static,
        Fut: Future<Output = rocketmq_error::RocketMQResult<RemotingResponse>> + Send + 'static,
    {
        let resume_executions = Arc::clone(&self.resume_executions);
        let resume_execution_bytes = Arc::clone(&self.resume_execution_bytes);
        let retained_bytes = handler_retained.dynamic_bytes();
        let observation = ResumeExecutionObservation::new(resume_executions, resume_execution_bytes, retained_bytes);
        claimed.submit(handler_retained, handler, move |_| drop(observation))
    }

    /// Transfers one lag-refresh execution to the session owner. The observer
    /// remains attached to the canonical response terminal, not this submitter.
    pub(crate) fn submit_claimed_observed<F, Fut>(
        &self,
        claimed: ClaimedDeferred<ResumePop>,
        handler_retained: DeferredResumeRetainedSize,
        observer: PopDeferredWakeupObserver,
        handler: F,
    ) -> Result<DeferredResumeSubmitOutcome, TransportError>
    where
        F: FnOnce(ResumePop, DeferredWakeReason) -> Fut + Send + 'static,
        Fut: Future<Output = rocketmq_error::RocketMQResult<RemotingResponse>> + Send + 'static,
    {
        let resume_executions = Arc::clone(&self.resume_executions);
        let resume_execution_bytes = Arc::clone(&self.resume_execution_bytes);
        let retained_bytes = handler_retained.dynamic_bytes();
        let observation = ResumeExecutionObservation::new(resume_executions, resume_execution_bytes, retained_bytes);
        claimed.submit(handler_retained, handler, move |result| {
            observer.complete_resume_result(result);
            drop(observation);
        })
    }

    async fn claim_matching(
        &self,
        arrival: &PopArrival,
        order: PopSelectionOrder,
        reason: DeferredWakeReason,
    ) -> Result<Option<ClaimedDeferred<ResumePop>>, TransportError> {
        let mut remaining = self.sweep_limit.get();
        while remaining > 0 {
            let Some(remaining_limit) = NonZeroUsize::new(remaining) else {
                break;
            };
            let selection = self.index.reserve_next_matching(arrival, order, remaining_limit);
            let inspected = selection.inspected();
            if inspected == 0 {
                break;
            }
            remaining -= inspected;
            let Some(candidate) = selection.into_candidate() else {
                break;
            };
            let id = candidate.id();
            match self.claim(id, reason).await {
                Ok(DeferredClaimOutcome::Claimed(claimed)) => {
                    drop(candidate);
                    return Ok(Some(claimed));
                }
                Ok(outcome) if is_skippable_candidate_outcome(&outcome) => {
                    drop(candidate);
                }
                Ok(_) => {
                    drop(candidate);
                }
                Err(error) => {
                    drop(candidate);
                    return Err(error);
                }
            }
        }
        Ok(None)
    }

    pub(crate) fn sweep_expired(&self) -> PopDeferredSweepBatch {
        PopDeferredSweepBatch::from_transport(self.registry.sweep_expired(self.sweep_limit))
    }

    /// Projects a producer's topic/queue arrival into a bounded set of live
    /// consumer groups without exposing requests or transport capabilities.
    pub(crate) fn consumer_groups_for_arrival(&self, topic: &CheetahString, queue_id: i32) -> Vec<CheetahString> {
        self.index.consumer_groups(topic, queue_id, self.sweep_limit)
    }

    pub(crate) const fn fanout_cursor(&self) -> PopFanoutCursor {
        PopFanoutCursor::new()
    }

    pub(crate) fn consumer_group_batch(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        cursor: &mut PopFanoutCursor,
    ) -> PopFanoutBatch {
        if self.closed.load(Ordering::Acquire) {
            return PopFanoutBatch::empty();
        }
        self.index
            .consumer_group_batch(topic, queue_id, cursor, self.sweep_limit)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the continuation retains the exact Store arrival callback metadata"
    )]
    pub(crate) fn admit_arrival_continuation(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        tags_code: Option<i64>,
        message_store_time: i64,
        filter_bitmap: Option<&[u8]>,
        properties: Option<&std::collections::HashMap<CheetahString, CheetahString>>,
        cursor: PopFanoutCursor,
    ) -> Result<PopContinuationOutcome, PopContinuationError> {
        PopArrivalContinuation::try_admit(
            &self.continuation_admission,
            topic,
            queue_id,
            tags_code,
            message_store_time,
            filter_bitmap,
            properties,
            cursor,
        )
    }

    pub(crate) fn continuation_consumer_group_batch(
        &self,
        continuation: &mut PopArrivalContinuation,
    ) -> PopFanoutBatch {
        if self.closed.load(Ordering::Acquire) {
            return PopFanoutBatch::empty();
        }
        continuation.next_batch(&self.index, self.sweep_limit)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the replay latch retains the exact Store arrival callback metadata"
    )]
    pub(crate) fn latch_arrival(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        tags_code: Option<i64>,
        message_store_time: i64,
        filter_bitmap: Option<&[u8]>,
        properties: Option<&std::collections::HashMap<CheetahString, CheetahString>>,
        cursor: PopFanoutCursor,
    ) -> Result<PopPendingArrivalOutcome, PopPendingArrivalError> {
        let key = PopPendingArrivalKey::new(
            self.pending_arrival_sequence.fetch_add(1, Ordering::Relaxed),
            topic.clone(),
            queue_id,
        );
        let pending = PopPendingArrival::new(
            topic,
            queue_id,
            tags_code,
            message_store_time,
            filter_bitmap,
            properties,
            cursor,
        )
        .map_err(PopPendingArrivalError::Continuation)?;
        match self.pending_arrivals.insert(key, pending) {
            Ok(PendingArrivalInsertOutcome::Inserted) => Ok(PopPendingArrivalOutcome::Latched),
            Ok(PendingArrivalInsertOutcome::Rejected(PendingArrivalInsertRejection::Closed)) => {
                Ok(PopPendingArrivalOutcome::Rejected(PopPendingArrivalRejection::Closed))
            }
            Ok(PendingArrivalInsertOutcome::Rejected(PendingArrivalInsertRejection::CountFull)) => Ok(
                PopPendingArrivalOutcome::Rejected(PopPendingArrivalRejection::CountFull),
            ),
            Ok(PendingArrivalInsertOutcome::Rejected(PendingArrivalInsertRejection::BytesFull)) => Ok(
                PopPendingArrivalOutcome::Rejected(PopPendingArrivalRejection::BytesFull),
            ),
            Err(error) => Err(PopPendingArrivalError::Latch(error)),
        }
    }

    pub(crate) fn pending_arrival_reservations(&self) -> Vec<PopPendingArrivalReservation> {
        self.pending_arrivals.reserve_batch(self.sweep_limit.get())
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
        self.pending_offsets
            .merge(PopPendingOffsetTarget::new(topic.clone(), queue_id), logical_offset - 1)
    }

    #[cfg(test)]
    pub(super) fn latch_queue_offset_for_test(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        queue_offset: i64,
    ) -> Result<PendingArrivalInsertOutcome, PendingArrivalInsertOperationalError> {
        self.pending_offsets
            .merge(PopPendingOffsetTarget::new(topic.clone(), queue_id), queue_offset)
    }

    pub(crate) fn pending_offset_reservations(&self) -> Vec<PopPendingOffsetReservation> {
        self.pending_offsets
            .reserve_batch(self.sweep_limit.get())
            .into_iter()
            .filter_map(
                |reservation| match self.continuation_admission.reserve(reservation.retained_bytes()) {
                    Ok(continuation::PopContinuationReserveOutcome::Reserved(permit)) => {
                        Some(PopPendingOffsetReservation {
                            reservation,
                            _permit: permit,
                        })
                    }
                    Ok(continuation::PopContinuationReserveOutcome::Rejected(_)) | Err(_) => None,
                },
            )
            .collect()
    }

    pub(crate) fn pending_consumer_group_batch(&self, pending: &mut PopPendingArrival) -> PopFanoutBatch {
        if self.closed.load(Ordering::Acquire) {
            return PopFanoutBatch::empty();
        }
        pending.next_batch(&self.index, self.sweep_limit)
    }

    pub(crate) fn pending_offset_consumer_group_batch(
        &self,
        target: &PopPendingOffsetTarget,
        cursor: &mut PopFanoutCursor,
    ) -> PopFanoutBatch {
        if self.closed.load(Ordering::Acquire) {
            return PopFanoutBatch::empty();
        }
        self.index
            .consumer_group_batch(target.topic(), target.queue_id(), cursor, self.sweep_limit)
    }

    pub(crate) fn replay_read_limit(&self) -> i32 {
        i32::try_from(self.sweep_limit.get()).unwrap_or(i32::MAX)
    }

    pub(crate) fn forced_target_batch(
        &self,
        topic: &CheetahString,
        consumer_group: &CheetahString,
    ) -> Vec<PopCriteriaKey> {
        if self.closed.load(Ordering::Acquire) {
            return Vec::new();
        }
        self.index.forced_targets(topic, consumer_group, self.sweep_limit)
    }

    /// Starts one lifecycle-owned bounded sweep loop; the callback owns each claim batch.
    pub(crate) fn start_sweeper<F, Fut>(
        self: &Arc<Self>,
        task_group: &TaskGroup,
        interval_millis: NonZeroU64,
        handle_claims: F,
    ) -> RuntimeResult<TaskId>
    where
        F: Fn(Vec<ClaimedDeferred<ResumePop>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let service = Arc::clone(self);
        let cancellation = task_group.cancellation_token();
        let interval = Duration::from_millis(interval_millis.get());
        task_group.spawn_service("broker.pop-deferred.sweep", async move {
            let _shutdown = PopSweepShutdownGuard(Arc::clone(&service));
            loop {
                if service.closed.load(Ordering::Acquire) {
                    break;
                }
                if tokio::time::timeout(interval, cancellation.cancelled()).await.is_ok() {
                    break;
                }
                if service.closed.load(Ordering::Acquire) {
                    break;
                }
                let claims = service.sweep_expired().into_claims();
                if !claims.is_empty() {
                    handle_claims(claims).await;
                }
            }
        })
    }

    #[must_use]
    pub(crate) fn admission_snapshot(&self) -> DeferredAdmissionSnapshot {
        self.admission.snapshot()
    }

    #[must_use]
    pub(crate) fn index_snapshot(&self) -> PopIndexSnapshot {
        self.index.snapshot()
    }

    #[must_use]
    pub(crate) fn resource_snapshot(&self) -> PopDeferredResourceSnapshot {
        let continuation = self.continuation_admission.snapshot();
        let pending = self.pending_arrivals.snapshot();
        let offsets = self.pending_offsets.snapshot();
        PopDeferredResourceSnapshot {
            admission: self.admission.snapshot(),
            index: self.index.snapshot(),
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

    pub(crate) fn index_contains(&self, id: DeferredId) -> bool {
        self.index.contains(id)
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
}

impl PollingCountProvider for PopDeferredService {
    fn polling_count(&self, topic: &CheetahString, consumer_group: &CheetahString, queue_id: i32) -> i32 {
        self.index
            .polling_count(&PopCriteriaKey::from_parts(topic, consumer_group, queue_id))
    }
}

struct ResumeExecutionObservation {
    executions: Arc<AtomicUsize>,
    bytes: Arc<AtomicUsize>,
    retained_bytes: usize,
}

impl ResumeExecutionObservation {
    fn new(executions: Arc<AtomicUsize>, bytes: Arc<AtomicUsize>, retained_bytes: usize) -> Self {
        executions.fetch_add(1, Ordering::AcqRel);
        bytes.fetch_add(retained_bytes, Ordering::AcqRel);
        Self {
            executions,
            bytes,
            retained_bytes,
        }
    }
}

impl Drop for ResumeExecutionObservation {
    fn drop(&mut self) {
        self.bytes.fetch_sub(self.retained_bytes, Ordering::AcqRel);
        self.executions.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PopDeferredResourceSnapshot {
    pub(crate) admission: DeferredAdmissionSnapshot,
    pub(crate) index: PopIndexSnapshot,
    pub(crate) resume_executions: usize,
    pub(crate) resume_execution_bytes: usize,
    pub(crate) active_continuations: usize,
    pub(crate) continuation_bytes: usize,
    pub(crate) continuation_rejected: usize,
    pub(crate) pending_arrivals: usize,
    pub(crate) pending_arrival_bytes: usize,
    pub(crate) pending_arrival_rejected: usize,
    pub(crate) pending_offset_invariant_failures: usize,
}

pub(crate) type PopPendingArrivalReservation = PendingArrivalReservation<PopPendingArrivalKey, PopPendingArrival>;
pub(crate) struct PopPendingOffsetReservation {
    reservation: PendingOffsetRangeReservation<PopPendingOffsetTarget>,
    _permit: PopContinuationPermit,
}

impl PopPendingOffsetReservation {
    pub(crate) fn key(&self) -> &PopPendingOffsetTarget {
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
pub(crate) struct PopPendingOffsetTarget {
    topic: CheetahString,
    queue_id: i32,
}

impl PopPendingOffsetTarget {
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

impl PendingOffsetTarget for PopPendingOffsetTarget {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PopPendingArrivalOutcome {
    Latched,
    Rejected(PopPendingArrivalRejection),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PopPendingArrivalRejection {
    Closed,
    CountFull,
    BytesFull,
}

#[derive(Debug)]
pub(crate) enum PopPendingArrivalError {
    Continuation(PopContinuationError),
    Latch(PendingArrivalInsertOperationalError),
}

impl fmt::Display for PopPendingArrivalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Continuation(_) => formatter.write_str("POP pending-arrival continuation failed"),
            Self::Latch(_) => formatter.write_str("POP pending-arrival latch failed"),
        }
    }
}

impl Error for PopPendingArrivalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Continuation(source) => Some(source),
            Self::Latch(source) => Some(source),
        }
    }
}

fn match_criteria_allocation_bytes() -> Option<usize> {
    Layout::array::<AtomicUsize>(2)
        .and_then(|header| header.extend(Layout::new::<PopMatchCriteria>()))
        .map(|(allocation, _)| allocation.pad_to_align().size())
        .ok()
}

/// Timeout claims with business-index membership already detached.
#[must_use]
pub(crate) struct PopDeferredSweepBatch {
    stats: DeferredExpiryBatchStats,
    claims: Vec<ClaimedDeferred<ResumePop>>,
}

impl PopDeferredSweepBatch {
    fn from_transport(batch: DeferredExpiryBatch<ResumePop>) -> Self {
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
    pub(crate) fn into_claims(self) -> Vec<ClaimedDeferred<ResumePop>> {
        self.claims
    }
}

struct PopSweepShutdownGuard(Arc<PopDeferredService>);

impl Drop for PopSweepShutdownGuard {
    fn drop(&mut self) {
        let _ = self.0.shutdown();
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PopDeferredPrepareErrorKind {
    EmbeddedOrigin,
    Header,
    MissingCallerHost,
    InvalidExpiryMargins,
    RetainedSizeOverflow,
    Deadline,
    Index,
    Contract,
}

#[must_use]
pub(crate) enum PopDeferredPrepareOutcome {
    Prepared(Box<PreparedPopRegistration>),
    Rejected(PopDeferredPrepareRejection),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PopDeferredPrepareRejectionKind {
    ServiceClosed,
    DeadlineElapsed,
    IndexCapacity,
    AdmissionCapacity,
}

pub(crate) enum PopDeferredPrepareRejection {
    ServiceClosed,
    DeadlineElapsed,
    Index(PopIndexRejection),
    Admission(DeferredAdmissionAcquireOutcome),
}

impl PopDeferredPrepareRejection {
    #[must_use]
    pub(crate) const fn kind(&self) -> PopDeferredPrepareRejectionKind {
        match self {
            Self::ServiceClosed => PopDeferredPrepareRejectionKind::ServiceClosed,
            Self::DeadlineElapsed => PopDeferredPrepareRejectionKind::DeadlineElapsed,
            Self::Index(_) => PopDeferredPrepareRejectionKind::IndexCapacity,
            Self::Admission(_) => PopDeferredPrepareRejectionKind::AdmissionCapacity,
        }
    }
}

pub(crate) enum PopDeferredPrepareError {
    EmbeddedOrigin,
    Header(RocketMQError),
    MissingCallerHost,
    InvalidExpiryMargins,
    RetainedSizeOverflow,
    Deadline(LongPollingDeadlineError),
    Index(PopIndexOperationalError),
    Contract(TransportContractViolation),
}

impl PopDeferredPrepareError {
    #[must_use]
    pub(crate) const fn kind(&self) -> PopDeferredPrepareErrorKind {
        match self {
            Self::EmbeddedOrigin => PopDeferredPrepareErrorKind::EmbeddedOrigin,
            Self::Header(_) => PopDeferredPrepareErrorKind::Header,
            Self::MissingCallerHost => PopDeferredPrepareErrorKind::MissingCallerHost,
            Self::InvalidExpiryMargins => PopDeferredPrepareErrorKind::InvalidExpiryMargins,
            Self::RetainedSizeOverflow => PopDeferredPrepareErrorKind::RetainedSizeOverflow,
            Self::Deadline(_) => PopDeferredPrepareErrorKind::Deadline,
            Self::Index(_) => PopDeferredPrepareErrorKind::Index,
            Self::Contract(_) => PopDeferredPrepareErrorKind::Contract,
        }
    }
}

impl fmt::Debug for PopDeferredPrepareError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PopDeferredPrepareError")
            .field("kind", &self.kind())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for PopDeferredPrepareError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "POP deferred preparation failed: {:?}", self.kind())
    }
}

impl Error for PopDeferredPrepareError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Header(source) => Some(source),
            Self::Deadline(source) => Some(source),
            Self::Index(source) => Some(source),
            Self::Contract(source) => Some(source),
            Self::EmbeddedOrigin
            | Self::MissingCallerHost
            | Self::InvalidExpiryMargins
            | Self::RetainedSizeOverflow => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PopDeferredRegisterErrorKind {
    Registry,
    Contract,
}

#[must_use]
pub(crate) enum PopDeferredRegisterOutcome {
    Registered(Box<DeferredRegistration>),
    Rejected(Box<PopDeferredRegisterRejection>),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PopDeferredRegisterRejectionKind {
    ProvenanceMismatch,
    ServiceClosed,
    Responder,
    Expiry,
    Registry,
}

pub(crate) enum PopDeferredRegisterRejection {
    ProvenanceMismatch,
    ServiceClosed,
    Responder(DeferredResponderOutcome),
    Expiry {
        outcome: DeferredExpiryOutcome,
        parts: DeferredParts,
    },
    RegistryRejected,
}

impl PopDeferredRegisterRejection {
    #[must_use]
    pub(crate) const fn kind(&self) -> PopDeferredRegisterRejectionKind {
        match self {
            Self::ProvenanceMismatch => PopDeferredRegisterRejectionKind::ProvenanceMismatch,
            Self::ServiceClosed => PopDeferredRegisterRejectionKind::ServiceClosed,
            Self::Responder(_) => PopDeferredRegisterRejectionKind::Responder,
            Self::Expiry { .. } => PopDeferredRegisterRejectionKind::Expiry,
            Self::RegistryRejected => PopDeferredRegisterRejectionKind::Registry,
        }
    }
}

pub(crate) enum PopDeferredRegisterError {
    RegistryIdentityExhausted,
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

impl PopDeferredRegisterError {
    #[must_use]
    pub(crate) const fn kind(&self) -> PopDeferredRegisterErrorKind {
        match self {
            Self::RegistryIdentityExhausted | Self::RegistryContract(_) | Self::RegistryOperational(_) => {
                PopDeferredRegisterErrorKind::Registry
            }
            Self::Contract { .. } => PopDeferredRegisterErrorKind::Contract,
        }
    }
}

impl fmt::Debug for PopDeferredRegisterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PopDeferredRegisterError")
            .field("kind", &self.kind())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for PopDeferredRegisterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "POP deferred registration failed: {:?}", self.kind())
    }
}

impl Error for PopDeferredRegisterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RegistryContract(violation) => Some(violation),
            Self::RegistryOperational(error) => Some(error),
            Self::Contract { violation, .. } => Some(violation),
            Self::RegistryIdentityExhausted => None,
        }
    }
}

fn pop_wakeup_outcome_from_claim_result(
    result: &Result<DeferredClaimOutcome<ResumePop>, TransportError>,
) -> PopWakeupOutcome {
    match result {
        Ok(DeferredClaimOutcome::Claimed(_)) => PopWakeupOutcome::ProcessingCompleted,
        Ok(
            DeferredClaimOutcome::NotFound
            | DeferredClaimOutcome::AlreadyClaimed
            | DeferredClaimOutcome::AlreadyCompleted,
        ) => PopWakeupOutcome::AlreadyCompleted,
        Ok(DeferredClaimOutcome::SessionClosed) => PopWakeupOutcome::InactiveChannel,
        Ok(DeferredClaimOutcome::ParentCancelled) => PopWakeupOutcome::ServiceCancelled,
        Ok(DeferredClaimOutcome::DeadlineExpired) | Err(_) => PopWakeupOutcome::ProcessingFailed,
    }
}

const fn is_skippable_candidate_outcome<R>(outcome: &DeferredClaimOutcome<R>) -> bool
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

fn pop_wakeup_outcome_from_resume_result(result: &Result<DeferredResumeOutcome, TransportError>) -> PopWakeupOutcome {
    match result {
        Ok(DeferredResumeOutcome::Completed(_)) => PopWakeupOutcome::ProcessingCompleted,
        Ok(DeferredResumeOutcome::SessionClosed) => PopWakeupOutcome::InactiveChannel,
        Ok(DeferredResumeOutcome::Cancelled) => PopWakeupOutcome::ServiceCancelled,
        Ok(DeferredResumeOutcome::AdmissionRejected) | Err(_) => PopWakeupOutcome::ProcessingFailed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retired_candidate_outcomes_are_local_but_parent_cancellation_is_not() {
        for outcome in [
            DeferredClaimOutcome::<ResumePop>::NotFound,
            DeferredClaimOutcome::AlreadyClaimed,
            DeferredClaimOutcome::AlreadyCompleted,
            DeferredClaimOutcome::SessionClosed,
            DeferredClaimOutcome::DeadlineExpired,
        ] {
            assert!(is_skippable_candidate_outcome(&outcome));
        }
        let outcome = DeferredClaimOutcome::<ResumePop>::ParentCancelled;
        assert!(!is_skippable_candidate_outcome(&outcome));
    }
}
