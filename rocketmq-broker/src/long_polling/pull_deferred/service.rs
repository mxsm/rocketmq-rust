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
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use parking_lot::Mutex;

use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_runtime::common::time_utils::current_millis;
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

use super::data::PullHookMetadata;
use super::data::PullMatchCriteria;
use super::data::PullRequestData;
use super::deadline::PullWaitDeadline;
use super::deadline::PullWaitDeadlineError;
use super::deadline::PullWaitDeadlineOutcome;
use super::index::PullArrivalView;
use super::index::PullCandidateBatch;
use super::index::PullCandidateReservation;
use super::index::PullCriteriaIndex;
use super::index::PullCriteriaKey;
use super::index::PullCriteriaLimits;
use super::index::PullIndexLease;
use super::index::PullIndexOperationalError;
use super::index::PullIndexRejection;
use super::index::PullIndexReservation;
use super::index::PullIndexReserveOutcome;
use super::index::PullIndexSnapshot;
use super::index::PullScanCursor;

mod continuation;

use crate::long_polling::pending_arrival_latch::PendingArrivalInsertOperationalError;
use crate::long_polling::pending_arrival_latch::PendingArrivalInsertOutcome;
use crate::long_polling::pending_arrival_latch::PendingArrivalInsertRejection;
use crate::long_polling::pending_arrival_latch::PendingArrivalLatch;
use crate::long_polling::pending_arrival_latch::PendingArrivalReservation;
use crate::long_polling::pending_arrival_latch::PendingOffsetRangeLatch;
use crate::long_polling::pending_arrival_latch::PendingOffsetRangeReservation;
use crate::long_polling::pending_arrival_latch::PendingOffsetTarget;
pub(crate) use continuation::PullArrivalContinuation;
use continuation::PullContinuationAdmission;
pub(crate) use continuation::PullContinuationError;
pub(crate) use continuation::PullContinuationOutcome;
use continuation::PullContinuationPermit;
use continuation::PullPendingArrival;
use continuation::PullPendingArrivalKey;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PullSuspendTiming {
    suspend_wall_millis: u64,
    suspend_monotonic: tokio::time::Instant,
    effective_timeout_millis: u64,
}

impl PullSuspendTiming {
    pub(crate) const fn new(
        suspend_wall_millis: u64,
        suspend_monotonic: tokio::time::Instant,
        effective_timeout_millis: u64,
    ) -> Self {
        Self {
            suspend_wall_millis,
            suspend_monotonic,
            effective_timeout_millis,
        }
    }

    pub(crate) const fn from_policy(
        suspend_wall_millis: u64,
        suspend_monotonic: tokio::time::Instant,
        long_polling_enabled: bool,
        header_timeout_millis: u64,
        short_polling_time_millis: u64,
    ) -> Self {
        Self::new(
            suspend_wall_millis,
            suspend_monotonic,
            if long_polling_enabled {
                header_timeout_millis
            } else {
                short_polling_time_millis
            },
        )
    }

    pub(crate) const fn suspend_wall_millis(self) -> u64 {
        self.suspend_wall_millis
    }

    pub(crate) const fn suspend_monotonic(self) -> tokio::time::Instant {
        self.suspend_monotonic
    }

    pub(crate) const fn effective_timeout_millis(self) -> u64 {
        self.effective_timeout_millis
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PullRetainedEstimate {
    filter_bytes: usize,
    hook_metadata_bytes: usize,
}

impl PullRetainedEstimate {
    pub(crate) const fn new(filter_bytes: usize, hook_metadata_bytes: usize) -> Self {
        Self {
            filter_bytes,
            hook_metadata_bytes,
        }
    }
}

/// Affine fallback and business data retained until responder transfer succeeds.
#[must_use]
pub(crate) struct PullSuspensionCandidate {
    request: PullRequestData,
    criteria: Arc<PullMatchCriteria>,
    fallback: RemotingResponse,
    timing: PullSuspendTiming,
    retained: PullRetainedEstimate,
    provenance: PreparedRequestProvenance,
}

impl PullSuspensionCandidate {
    pub(crate) fn from_request(
        request: &RemotingRequest,
        criteria: PullMatchCriteria,
        fallback: RemotingResponse,
        timing: PullSuspendTiming,
        retained: PullRetainedEstimate,
    ) -> Result<Self, PullCandidateBuildError> {
        let original = request.original_identity();
        let request_code = RequestCode::from(original.original_code());
        if !matches!(request_code, RequestCode::PullMessage | RequestCode::LitePullMessage) {
            return Err(PullCandidateBuildError::new(
                PullCandidateBuildErrorKind::UnsupportedRequestCode,
                fallback,
                None,
            ));
        }
        if original.is_one_way() {
            return Err(PullCandidateBuildError::new(
                PullCandidateBuildErrorKind::OneWayRequest,
                fallback,
                None,
            ));
        }
        let effective_peer = match request.origin() {
            RequestOrigin::Network { peer } => peer.address(),
            RequestOrigin::Embedded { .. } => {
                return Err(PullCandidateBuildError::new(
                    PullCandidateBuildErrorKind::EmbeddedOrigin,
                    fallback,
                    None,
                ));
            }
            _ => {
                return Err(PullCandidateBuildError::new(
                    PullCandidateBuildErrorKind::EmbeddedOrigin,
                    fallback,
                    None,
                ));
            }
        };
        let header = match request
            .command()
            .decode_command_custom_header::<PullMessageRequestHeader>()
        {
            Ok(header) => header,
            Err(source) => {
                return Err(PullCandidateBuildError::new(
                    PullCandidateBuildErrorKind::Header,
                    fallback,
                    Some(source),
                ));
            }
        };
        let request_data = PullRequestData::new(
            request_code,
            header,
            effective_peer,
            request.session().id(),
            PullHookMetadata::from_command(request.command()),
        );
        Ok(Self {
            request: request_data,
            criteria: Arc::new(criteria),
            fallback,
            timing,
            retained,
            provenance: PreparedRequestProvenance::capture(request),
        })
    }

    #[cfg(test)]
    fn from_test_parts(
        request: PullRequestData,
        criteria: Arc<PullMatchCriteria>,
        fallback: RemotingResponse,
        timing: PullSuspendTiming,
        retained: PullRetainedEstimate,
        provenance: PreparedRequestProvenance,
    ) -> Self {
        Self {
            request,
            criteria,
            fallback,
            timing,
            retained,
            provenance,
        }
    }

    pub(crate) fn into_fallback(self) -> RemotingResponse {
        self.fallback
    }

    pub(crate) const fn request(&self) -> &PullRequestData {
        &self.request
    }

    pub(crate) const fn criteria(&self) -> &Arc<PullMatchCriteria> {
        &self.criteria
    }

    pub(crate) const fn timing(&self) -> PullSuspendTiming {
        self.timing
    }

    pub(crate) const fn retained(&self) -> PullRetainedEstimate {
        self.retained
    }
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

/// Affine business ownership carried from registry claim into Pull recovery.
#[must_use]
pub(crate) struct ResumePull {
    request: PullRequestData,
    criteria: Arc<PullMatchCriteria>,
    protocol_wait_deadline: PullWaitDeadline,
    index_lease: Option<PullIndexLease>,
}

impl ResumePull {
    fn new(
        request: PullRequestData,
        criteria: Arc<PullMatchCriteria>,
        protocol_wait_deadline: PullWaitDeadline,
        index_lease: PullIndexLease,
    ) -> Self {
        Self {
            request,
            criteria,
            protocol_wait_deadline,
            index_lease: Some(index_lease),
        }
    }

    #[cfg(test)]
    pub(crate) fn without_index_for_test(
        request: PullRequestData,
        criteria: Arc<PullMatchCriteria>,
        protocol_wait_deadline: PullWaitDeadline,
    ) -> Self {
        Self {
            request,
            criteria,
            protocol_wait_deadline,
            index_lease: None,
        }
    }

    #[must_use]
    pub(crate) const fn request(&self) -> &PullRequestData {
        &self.request
    }

    #[must_use]
    pub(crate) const fn criteria(&self) -> &Arc<PullMatchCriteria> {
        &self.criteria
    }

    #[must_use]
    pub(crate) const fn protocol_wait_deadline(&self) -> PullWaitDeadline {
        self.protocol_wait_deadline
    }

    pub(super) fn take_index_lease(&mut self) -> Option<PullIndexLease> {
        self.index_lease.take()
    }

    pub(crate) fn into_parts(self) -> (PullRequestData, Arc<PullMatchCriteria>, PullWaitDeadline) {
        let Self {
            request,
            criteria,
            protocol_wait_deadline,
            index_lease,
        } = self;
        drop(index_lease);
        (request, criteria, protocol_wait_deadline)
    }
}

#[must_use]
pub(crate) struct PreparedPullRegistration {
    candidate: PullSuspensionCandidate,
    deadline: PullWaitDeadline,
    reservation: PullIndexReservation,
    permit: rocketmq_transport::api::DeferredWaitPermit,
}

impl PreparedPullRegistration {
    #[must_use]
    pub(crate) const fn deadline(&self) -> PullWaitDeadline {
        self.deadline
    }

    #[must_use]
    pub(crate) const fn retained_bytes(&self) -> usize {
        self.permit.retained_bytes()
    }

    pub(crate) fn into_candidate(self) -> PullSuspensionCandidate {
        self.candidate
    }
}

pub(crate) struct PullDeferredService {
    admission: DeferredAdmission,
    registry: DeferredRegistry<ResumePull>,
    index: PullCriteriaIndex,
    expiry_margins: DeferredExpiryMargins,
    scan_limit: NonZeroUsize,
    candidate_limit: NonZeroUsize,
    continuation_admission: Arc<PullContinuationAdmission>,
    pending_arrivals: Arc<PendingArrivalLatch<PullPendingArrivalKey, PullPendingArrival>>,
    pending_offsets: Arc<PendingOffsetRangeLatch<PullCriteriaKey>>,
    pending_arrival_sequence: AtomicU64,
    resume_executions: Arc<AtomicUsize>,
    resume_execution_bytes: Arc<AtomicUsize>,
    closed: AtomicBool,
}

impl PullDeferredService {
    pub(crate) fn new(
        admission: DeferredAdmission,
        index_limits: PullCriteriaLimits,
        expiry_margins: DeferredExpiryMargins,
        scan_limit: NonZeroUsize,
        candidate_limit: NonZeroUsize,
    ) -> Self {
        let limits = admission.limits();
        let continuation_bytes = limits.max_retained_bytes();
        Self {
            admission,
            registry: DeferredRegistry::new(),
            index: PullCriteriaIndex::new(index_limits),
            expiry_margins,
            scan_limit,
            candidate_limit,
            continuation_admission: Arc::new(PullContinuationAdmission::new(scan_limit.get(), continuation_bytes)),
            pending_arrivals: PendingArrivalLatch::new(limits.max_waiters(), continuation_bytes),
            pending_offsets: PendingOffsetRangeLatch::new(
                combined_budget(limits.max_waiters(), scan_limit.get()),
                combined_budget(continuation_bytes, continuation_bytes),
            ),
            pending_arrival_sequence: AtomicU64::new(0),
            resume_executions: Arc::new(AtomicUsize::new(0)),
            resume_execution_bytes: Arc::new(AtomicUsize::new(0)),
            closed: AtomicBool::new(false),
        }
    }

    pub(crate) fn prepare(
        &self,
        request: &RemotingRequest,
        criteria: PullMatchCriteria,
        fallback: RemotingResponse,
        timing: PullSuspendTiming,
        retained: PullRetainedEstimate,
    ) -> Result<PullDeferredPrepareOutcome, PullDeferredPrepareError> {
        let candidate = PullSuspensionCandidate::from_request(request, criteria, fallback, timing, retained)
            .map_err(PullDeferredPrepareError::Build)?;
        self.prepare_candidate_at(candidate, current_millis(), tokio::time::Instant::now())
    }

    fn prepare_candidate_at(
        &self,
        candidate: PullSuspensionCandidate,
        wall_now: u64,
        monotonic_now: tokio::time::Instant,
    ) -> Result<PullDeferredPrepareOutcome, PullDeferredPrepareError> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(PullDeferredPrepareOutcome::Rejected(
                PullDeferredPrepareRejection::ServiceClosed(candidate),
            ));
        }
        if self.expiry_margins.recovery().is_zero() || self.expiry_margins.write().is_zero() {
            return Err(PullDeferredPrepareError::InvalidExpiryMargins { candidate });
        }
        let deadline = match PullWaitDeadline::checked(
            candidate.timing.suspend_wall_millis,
            candidate.timing.suspend_monotonic,
            candidate.timing.effective_timeout_millis,
            wall_now,
            monotonic_now,
        ) {
            Ok(PullWaitDeadlineOutcome::Pending(deadline)) => deadline,
            Ok(PullWaitDeadlineOutcome::AlreadyExpired) => {
                return Ok(PullDeferredPrepareOutcome::Rejected(
                    PullDeferredPrepareRejection::DeadlineElapsed(candidate),
                ));
            }
            Err(source) => {
                return Err(PullDeferredPrepareError::Deadline { source, candidate });
            }
        };
        let key = PullCriteriaKey::from_criteria(&candidate.criteria);
        let reservation = match self.index.reserve(key) {
            Ok(PullIndexReserveOutcome::Reserved(reservation)) => reservation,
            Ok(PullIndexReserveOutcome::Rejected(rejection)) => {
                return Ok(PullDeferredPrepareOutcome::Rejected(
                    PullDeferredPrepareRejection::Index { rejection, candidate },
                ));
            }
            Err(source) => {
                return Err(PullDeferredPrepareError::Index { source, candidate });
            }
        };
        let retained_size = match try_retained_size(&candidate) {
            Ok(size) => size,
            Err(PullRetainedSizeError::Overflow) => {
                drop(reservation);
                return Err(PullDeferredPrepareError::RetainedSizeOverflow { candidate });
            }
            Err(PullRetainedSizeError::Contract(source)) => {
                drop(reservation);
                return Err(PullDeferredPrepareError::Contract { source, candidate });
            }
        };
        let permit = match self.admission.try_reserve(retained_size) {
            DeferredAdmissionAcquireOutcome::Acquired(permit) => permit,
            outcome => {
                drop(reservation);
                return Ok(PullDeferredPrepareOutcome::Rejected(
                    PullDeferredPrepareRejection::Admission { outcome, candidate },
                ));
            }
        };
        let prepared = PreparedPullRegistration {
            candidate,
            deadline,
            reservation,
            permit,
        };
        if self.closed.load(Ordering::Acquire) {
            return Ok(PullDeferredPrepareOutcome::Rejected(
                PullDeferredPrepareRejection::ServiceClosed(prepared.into_candidate()),
            ));
        }
        Ok(PullDeferredPrepareOutcome::Prepared(prepared))
    }

    pub(crate) fn register(
        &self,
        prepared: PreparedPullRegistration,
        request: &mut RemotingRequest,
    ) -> Result<PullDeferredRegisterOutcome, PullDeferredRegisterError> {
        if !prepared.candidate.provenance.matches(request) {
            return Ok(PullDeferredRegisterOutcome::Rejected(Box::new(
                PullDeferredRegisterRejection::PreTake {
                    kind: PullDeferredRegisterRejectionKind::ProvenanceMismatch,
                    prepared: Box::new(prepared),
                    responder: None,
                },
            )));
        }
        if self.closed.load(Ordering::Acquire) {
            return Ok(PullDeferredRegisterOutcome::Rejected(Box::new(
                PullDeferredRegisterRejection::PreTake {
                    kind: PullDeferredRegisterRejectionKind::ServiceClosed,
                    prepared: Box::new(prepared),
                    responder: None,
                },
            )));
        }
        let responder = match request.take_deferred_responder() {
            DeferredResponderOutcome::Taken(responder) => responder,
            outcome => {
                return Ok(PullDeferredRegisterOutcome::Rejected(Box::new(
                    PullDeferredRegisterRejection::PreTake {
                        kind: PullDeferredRegisterRejectionKind::Responder,
                        prepared: Box::new(prepared),
                        responder: Some(outcome),
                    },
                )));
            }
        };
        let PreparedPullRegistration {
            candidate,
            deadline,
            reservation,
            permit,
        } = prepared;
        let PullSuspensionCandidate {
            request,
            criteria,
            fallback,
            timing: _,
            retained: _,
            provenance: _,
        } = candidate;
        let mut parts = DeferredParts::new(responder, permit);
        match parts.try_with_expiry(deadline.protocol_at(), self.expiry_margins) {
            Ok(DeferredExpiryOutcome::Attached) => {}
            Ok(outcome) => {
                return Ok(PullDeferredRegisterOutcome::Rejected(Box::new(
                    PullDeferredRegisterRejection::Expiry { outcome, parts },
                )));
            }
            Err(violation) => {
                return Err(PullDeferredRegisterError::Contract {
                    violation,
                    parts: Box::new(parts),
                });
            }
        }
        drop(fallback);
        match self.registry.register_with(parts, move |id| {
            let lease = reservation.publish(id, Arc::clone(&criteria));
            Ok::<_, Infallible>(ResumePull::new(request, criteria, deadline, lease))
        }) {
            DeferredRegistryOutcome::Registered(registration) => {
                Ok(PullDeferredRegisterOutcome::Registered(Box::new(registration)))
            }
            DeferredRegistryOutcome::DuplicateRequest(recovery) => {
                release_deferred_registry_recovery(recovery);
                Ok(PullDeferredRegisterOutcome::Rejected(Box::new(
                    PullDeferredRegisterRejection::RegistryRejected,
                )))
            }
            DeferredRegistryOutcome::IdentityExhausted(recovery) => {
                release_deferred_registry_recovery(recovery);
                Err(PullDeferredRegisterError::RegistryIdentityExhausted)
            }
            DeferredRegistryOutcome::ParentCancelled
            | DeferredRegistryOutcome::SessionClosed
            | DeferredRegistryOutcome::DeadlineExpired => Ok(PullDeferredRegisterOutcome::Rejected(Box::new(
                PullDeferredRegisterRejection::RegistryRejected,
            ))),
            DeferredRegistryOutcome::BuilderRejected { error, parts } => {
                drop(parts);
                match error {}
            }
            DeferredRegistryOutcome::ContractViolation { violation, recovery } => {
                release_deferred_registry_recovery(recovery);
                Err(PullDeferredRegisterError::RegistryContract(violation))
            }
            DeferredRegistryOutcome::OperationalFailure { error, recovery } => {
                release_deferred_registry_recovery(recovery);
                Err(PullDeferredRegisterError::RegistryOperational(error))
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn reserve_arrival_batch(
        &self,
        arrival: &PullArrivalView<'_>,
        cursor: &mut PullScanCursor,
    ) -> Vec<PullCandidateReservation> {
        self.reserve_arrival_batch_state(arrival, cursor).into_candidates()
    }

    pub(crate) fn reserve_arrival_batch_state(
        &self,
        arrival: &PullArrivalView<'_>,
        cursor: &mut PullScanCursor,
    ) -> PullCandidateBatch {
        if self.closed.load(Ordering::Acquire) {
            return PullCandidateBatch::empty();
        }
        self.index
            .reserve_matching_batch(arrival, cursor, self.scan_limit, self.candidate_limit)
    }

    pub(crate) fn needs_offset_refresh(&self, arrival: &PullArrivalView<'_>) -> bool {
        !self.closed.load(Ordering::Acquire) && self.index.needs_offset_refresh(arrival)
    }

    pub(crate) const fn scan_cursor(&self) -> PullScanCursor {
        PullScanCursor::new()
    }

    pub(crate) fn reserve_forced_batch(&self, cursor: &mut PullScanCursor) -> PullCandidateBatch {
        if self.closed.load(Ordering::Acquire) {
            return PullCandidateBatch::empty();
        }
        self.index
            .reserve_forced_batch(cursor, self.scan_limit, self.candidate_limit)
    }

    pub(crate) fn admit_arrival_continuation(
        &self,
        arrival: PullArrivalView<'_>,
        cursor: PullScanCursor,
    ) -> Result<PullContinuationOutcome, PullContinuationError> {
        PullArrivalContinuation::arrival(&self.continuation_admission, arrival, cursor)
    }

    pub(crate) fn admit_forced_continuation(
        &self,
        cursor: PullScanCursor,
    ) -> Result<PullContinuationOutcome, PullContinuationError> {
        PullArrivalContinuation::forced(&self.continuation_admission, cursor)
    }

    pub(crate) fn reserve_continuation_batch(&self, continuation: &mut PullArrivalContinuation) -> PullCandidateBatch {
        if self.closed.load(Ordering::Acquire) {
            return PullCandidateBatch::empty();
        }
        continuation.reserve_next(&self.index, self.scan_limit, self.candidate_limit)
    }

    pub(crate) fn latch_arrival(
        &self,
        arrival: PullArrivalView<'_>,
        cursor: PullScanCursor,
    ) -> Result<PullPendingArrivalOutcome, PullPendingArrivalError> {
        let key = PullPendingArrivalKey::Arrival(
            self.pending_arrival_sequence.fetch_add(1, Ordering::Relaxed),
            PullCriteriaKey::new(arrival.topic().clone(), arrival.queue_id()),
        );
        let pending = PullPendingArrival::arrival(arrival, cursor).map_err(PullPendingArrivalError::Continuation)?;
        match self.pending_arrivals.insert(key, pending) {
            Ok(PendingArrivalInsertOutcome::Inserted) => Ok(PullPendingArrivalOutcome::Latched),
            Ok(PendingArrivalInsertOutcome::Rejected(PendingArrivalInsertRejection::Closed)) => {
                Ok(PullPendingArrivalOutcome::Rejected(PullPendingArrivalRejection::Closed))
            }
            Ok(PendingArrivalInsertOutcome::Rejected(PendingArrivalInsertRejection::CountFull)) => Ok(
                PullPendingArrivalOutcome::Rejected(PullPendingArrivalRejection::CountFull),
            ),
            Ok(PendingArrivalInsertOutcome::Rejected(PendingArrivalInsertRejection::BytesFull)) => Ok(
                PullPendingArrivalOutcome::Rejected(PullPendingArrivalRejection::BytesFull),
            ),
            Err(error) => Err(PullPendingArrivalError::Latch(error)),
        }
    }

    pub(crate) fn latch_forced(
        &self,
        cursor: PullScanCursor,
    ) -> Result<PullPendingArrivalOutcome, PullPendingArrivalError> {
        let key = PullPendingArrivalKey::Forced;
        if self.pending_arrivals.coalesce_existing(&key) {
            return Ok(PullPendingArrivalOutcome::Latched);
        }
        match self.pending_arrivals.insert(key, PullPendingArrival::forced(cursor)) {
            Ok(PendingArrivalInsertOutcome::Inserted) => Ok(PullPendingArrivalOutcome::Latched),
            Ok(PendingArrivalInsertOutcome::Rejected(PendingArrivalInsertRejection::Closed)) => {
                Ok(PullPendingArrivalOutcome::Rejected(PullPendingArrivalRejection::Closed))
            }
            Ok(PendingArrivalInsertOutcome::Rejected(PendingArrivalInsertRejection::CountFull)) => Ok(
                PullPendingArrivalOutcome::Rejected(PullPendingArrivalRejection::CountFull),
            ),
            Ok(PendingArrivalInsertOutcome::Rejected(PendingArrivalInsertRejection::BytesFull)) => Ok(
                PullPendingArrivalOutcome::Rejected(PullPendingArrivalRejection::BytesFull),
            ),
            Err(error) => Err(PullPendingArrivalError::Latch(error)),
        }
    }

    pub(crate) fn pending_arrival_reservations(&self) -> Vec<PullPendingArrivalReservation> {
        self.pending_arrivals.reserve_batch(self.scan_limit.get())
    }

    pub(crate) fn latch_offset(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        logical_offset: i64,
    ) -> Result<PendingArrivalInsertOutcome, PendingArrivalInsertOperationalError> {
        if logical_offset <= 0 {
            return Ok(PendingArrivalInsertOutcome::Inserted);
        }
        self.latch_queue_offset_range(topic, queue_id, logical_offset - 1, logical_offset - 1)
    }

    pub(crate) fn latch_max_offset_range(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        max_offset: i64,
    ) -> Result<PendingArrivalInsertOutcome, PendingArrivalInsertOperationalError> {
        if max_offset <= 0 {
            return Ok(PendingArrivalInsertOutcome::Inserted);
        }
        self.latch_queue_offset_range(topic, queue_id, 0, max_offset - 1)
    }

    fn latch_queue_offset_range(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        first: i64,
        last: i64,
    ) -> Result<PendingArrivalInsertOutcome, PendingArrivalInsertOperationalError> {
        let key = PullCriteriaKey::new(topic.clone(), queue_id);
        if !self.index.has_target(&key) {
            return Ok(PendingArrivalInsertOutcome::Inserted);
        }
        self.pending_offsets
            .retain_targets(|target| self.index.has_target(target));
        self.pending_offsets.merge_range(key, first, last)
    }

    pub(crate) fn pending_offset_reservations(&self) -> Vec<PullPendingOffsetReservation> {
        self.pending_offsets
            .reserve_batch(self.scan_limit.get())
            .into_iter()
            .filter_map(
                |reservation| match self.continuation_admission.reserve(reservation.retained_bytes()) {
                    Ok(continuation::PullContinuationReserveOutcome::Reserved(permit)) => {
                        Some(PullPendingOffsetReservation {
                            reservation,
                            _permit: permit,
                        })
                    }
                    Ok(continuation::PullContinuationReserveOutcome::Rejected(_)) | Err(_) => None,
                },
            )
            .collect()
    }

    pub(crate) fn reserve_pending_arrival_batch(&self, pending: &mut PullPendingArrival) -> PullCandidateBatch {
        if self.closed.load(Ordering::Acquire) {
            return PullCandidateBatch::empty();
        }
        pending.reserve_next(&self.index, self.scan_limit, self.candidate_limit)
    }

    pub(crate) fn reserve_offset_replay_batch(
        &self,
        key: &PullCriteriaKey,
        cursor: &mut PullScanCursor,
    ) -> PullCandidateBatch {
        let arrival = PullArrivalView::new(key.topic(), key.queue_id(), 0).forced();
        self.index
            .reserve_matching_batch(&arrival, cursor, self.scan_limit, self.candidate_limit)
    }

    pub(crate) fn replay_read_limit(&self) -> i32 {
        i32::try_from(self.candidate_limit.get()).unwrap_or(i32::MAX)
    }

    /// Returns one bounded, round-robin batch of live topic/queue targets.
    pub(crate) fn target_batch(&self) -> Vec<PullCriteriaKey> {
        if self.closed.load(Ordering::Acquire) {
            return Vec::new();
        }
        self.index.target_batch(self.scan_limit)
    }

    /// Test helper that selects every waiter matching one borrowed arrival.
    ///
    /// `submit` must synchronously transfer the affine candidates to a
    /// lifecycle-owned task. If it rejects a batch, dropping that batch restores
    /// index visibility and this callback stops without spinning.
    #[cfg(test)]
    pub(crate) fn produce_arrival<E, R, S>(
        &self,
        arrival: PullArrivalView<'_>,
        resolve_current_max_offset: R,
        mut submit: S,
    ) -> Result<PullProducerStats, E>
    where
        R: FnOnce() -> Result<i64, E>,
        S: FnMut(Vec<PullCandidateReservation>) -> Result<(), E>,
    {
        if self.closed.load(Ordering::Acquire) {
            return Ok(PullProducerStats::default());
        }
        let arrival = if self.index.needs_offset_refresh(&arrival) {
            arrival.with_max_offset(resolve_current_max_offset()?)
        } else {
            arrival
        };
        self.produce_batches(
            |cursor| {
                self.index
                    .reserve_matching_batch(&arrival, cursor, self.scan_limit, self.candidate_limit)
            },
            &mut submit,
        )
    }

    /// Test helper that selects every indexed waiter for master-online refresh.
    #[cfg(test)]
    pub(crate) fn produce_forced<S, E>(&self, mut submit: S) -> Result<PullProducerStats, E>
    where
        S: FnMut(Vec<PullCandidateReservation>) -> Result<(), E>,
    {
        if self.closed.load(Ordering::Acquire) {
            return Ok(PullProducerStats::default());
        }
        self.produce_batches(
            |cursor| {
                self.index
                    .reserve_forced_batch(cursor, self.scan_limit, self.candidate_limit)
            },
            &mut submit,
        )
    }

    #[cfg(test)]
    fn produce_batches<E, N, S>(&self, mut next: N, submit: &mut S) -> Result<PullProducerStats, E>
    where
        N: FnMut(&mut PullScanCursor) -> PullCandidateBatch,
        S: FnMut(Vec<PullCandidateReservation>) -> Result<(), E>,
    {
        let mut cursor = PullScanCursor::new();
        let mut stats = PullProducerStats::default();
        loop {
            if self.closed.load(Ordering::Acquire) {
                break;
            }
            let batch = next(&mut cursor);
            stats.inspected += batch.inspected();
            let exhausted = batch.exhausted();
            let candidates = batch.into_candidates();
            if !candidates.is_empty() {
                stats.candidates += candidates.len();
                stats.batches += 1;
                submit(candidates)?;
            }
            if exhausted {
                break;
            }
        }
        Ok(stats)
    }

    pub(crate) async fn claim_candidate(
        &self,
        candidate: PullCandidateReservation,
        reason: DeferredWakeReason,
    ) -> Result<DeferredClaimOutcome<ResumePull>, TransportError> {
        let id = candidate.id();
        match self.registry.claim(id, reason).await? {
            DeferredClaimOutcome::Claimed(mut claimed) => {
                candidate.commit();
                drop(claimed.resume_data_mut().take_index_lease());
                Ok(DeferredClaimOutcome::Claimed(claimed))
            }
            outcome => Ok(outcome),
        }
    }

    pub(crate) async fn claim(
        &self,
        id: DeferredId,
        reason: DeferredWakeReason,
    ) -> Result<DeferredClaimOutcome<ResumePull>, TransportError> {
        match self.registry.claim(id, reason).await? {
            DeferredClaimOutcome::Claimed(mut claimed) => {
                drop(claimed.resume_data_mut().take_index_lease());
                Ok(DeferredClaimOutcome::Claimed(claimed))
            }
            outcome => Ok(outcome),
        }
    }

    pub(crate) fn sweep_expired(&self) -> PullDeferredSweepBatch {
        PullDeferredSweepBatch::from_transport(self.registry.sweep_expired(self.candidate_limit))
    }

    pub(crate) async fn resume_claimed<F, Fut>(
        &self,
        claimed: ClaimedDeferred<ResumePull>,
        retained: DeferredResumeRetainedSize,
        handler: F,
    ) -> Result<DeferredResumeOutcome, TransportError>
    where
        F: FnOnce(ResumePull, DeferredWakeReason) -> Fut + Send + 'static,
        Fut: Future<Output = rocketmq_error::RocketMQResult<RemotingResponse>> + Send + 'static,
    {
        let observation = Arc::new(Mutex::new(None));
        let accepted = Arc::clone(&observation);
        let resume_executions = Arc::clone(&self.resume_executions);
        let resume_execution_bytes = Arc::clone(&self.resume_execution_bytes);
        let retained_bytes = retained.dynamic_bytes();
        let result = claimed
            .resume(retained, move |resume, reason| {
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

    /// Transfers one claimed Pull execution to its canonical session owner and
    /// returns after bounded session admission, before handler/write terminal.
    pub(crate) fn submit_claimed<F, Fut>(
        &self,
        claimed: ClaimedDeferred<ResumePull>,
        retained: DeferredResumeRetainedSize,
        handler: F,
    ) -> Result<DeferredResumeSubmitOutcome, TransportError>
    where
        F: FnOnce(ResumePull, DeferredWakeReason) -> Fut + Send + 'static,
        Fut: Future<Output = rocketmq_error::RocketMQResult<RemotingResponse>> + Send + 'static,
    {
        let resume_executions = Arc::clone(&self.resume_executions);
        let resume_execution_bytes = Arc::clone(&self.resume_execution_bytes);
        let retained_bytes = retained.dynamic_bytes();
        let observation = ResumeExecutionObservation::new(resume_executions, resume_execution_bytes, retained_bytes);
        claimed.submit(retained, handler, move |_| drop(observation))
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

    #[must_use]
    pub(crate) fn admission_snapshot(&self) -> DeferredAdmissionSnapshot {
        self.admission.snapshot()
    }

    #[must_use]
    pub(crate) fn index_snapshot(&self) -> PullIndexSnapshot {
        self.index.snapshot()
    }

    #[must_use]
    pub(crate) fn resource_snapshot(&self) -> PullDeferredResourceSnapshot {
        let continuation = self.continuation_admission.snapshot();
        let pending = self.pending_arrivals.snapshot();
        let offsets = self.pending_offsets.snapshot();
        PullDeferredResourceSnapshot {
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
pub(crate) struct PullDeferredResourceSnapshot {
    pub(crate) admission: DeferredAdmissionSnapshot,
    pub(crate) index: PullIndexSnapshot,
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

pub(crate) type PullPendingArrivalReservation = PendingArrivalReservation<PullPendingArrivalKey, PullPendingArrival>;
pub(crate) struct PullPendingOffsetReservation {
    reservation: PendingOffsetRangeReservation<PullCriteriaKey>,
    _permit: PullContinuationPermit,
}

impl PullPendingOffsetReservation {
    pub(crate) fn key(&self) -> &PullCriteriaKey {
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

impl PendingOffsetTarget for PullCriteriaKey {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(self.topic().len().saturating_mul(2))
    }
}

const fn combined_budget(left: usize, right: usize) -> usize {
    match left.checked_add(right) {
        Some(combined) => combined,
        None => usize::MAX,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PullPendingArrivalOutcome {
    Latched,
    Rejected(PullPendingArrivalRejection),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PullPendingArrivalRejection {
    Closed,
    CountFull,
    BytesFull,
}

#[derive(Debug)]
pub(crate) enum PullPendingArrivalError {
    Continuation(PullContinuationError),
    Latch(PendingArrivalInsertOperationalError),
}

impl fmt::Display for PullPendingArrivalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Continuation(_) => formatter.write_str("Pull pending-arrival continuation failed"),
            Self::Latch(_) => formatter.write_str("Pull pending-arrival latch failed"),
        }
    }
}

impl Error for PullPendingArrivalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Continuation(source) => Some(source),
            Self::Latch(source) => Some(source),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PullProducerStats {
    inspected: usize,
    candidates: usize,
    batches: usize,
}

impl PullProducerStats {
    pub(crate) const fn inspected(self) -> usize {
        self.inspected
    }

    pub(crate) const fn candidates(self) -> usize {
        self.candidates
    }

    pub(crate) const fn batches(self) -> usize {
        self.batches
    }
}

fn try_retained_size(
    candidate: &PullSuspensionCandidate,
) -> Result<rocketmq_transport::api::DeferredRetainedSize, PullRetainedSizeError> {
    let request_bytes = candidate
        .request
        .dynamic_bytes()
        .ok_or(PullRetainedSizeError::Overflow)?;
    let criteria_bytes = candidate
        .criteria
        .dynamic_bytes()
        .ok_or(PullRetainedSizeError::Overflow)?;
    let filter_bytes = candidate
        .retained
        .filter_bytes
        .checked_add(criteria_bytes)
        .ok_or(PullRetainedSizeError::Overflow)?;
    let index_bytes =
        PullCriteriaIndex::<DeferredId>::try_retained_bytes_per_entry().ok_or(PullRetainedSizeError::Overflow)?;
    DeferredRegistry::<ResumePull>::try_retained_size(
        DeferredRetainedSizeParts::new(request_bytes)
            .with_filter_bytes(filter_bytes)
            .with_secondary_index_bytes(index_bytes)
            .with_metadata_bytes(candidate.retained.hook_metadata_bytes),
    )
    .map_err(PullRetainedSizeError::Contract)
}

enum PullRetainedSizeError {
    Overflow,
    Contract(TransportContractViolation),
}

#[must_use]
pub(crate) struct PullDeferredSweepBatch {
    stats: DeferredExpiryBatchStats,
    claims: Vec<ClaimedDeferred<ResumePull>>,
}

impl PullDeferredSweepBatch {
    fn from_transport(batch: DeferredExpiryBatch<ResumePull>) -> Self {
        let stats = batch.stats();
        let mut claims = batch.into_claims();
        for claim in &mut claims {
            drop(claim.resume_data_mut().take_index_lease());
        }
        Self { stats, claims }
    }

    pub(crate) const fn stats(&self) -> DeferredExpiryBatchStats {
        self.stats
    }

    pub(crate) fn into_claims(self) -> Vec<ClaimedDeferred<ResumePull>> {
        self.claims
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PullCandidateBuildErrorKind {
    UnsupportedRequestCode,
    OneWayRequest,
    EmbeddedOrigin,
    Header,
}

pub(crate) struct PullCandidateBuildError {
    kind: PullCandidateBuildErrorKind,
    fallback: RemotingResponse,
    source: Option<rocketmq_error::RocketMQError>,
}

impl PullCandidateBuildError {
    fn new(
        kind: PullCandidateBuildErrorKind,
        fallback: RemotingResponse,
        source: Option<rocketmq_error::RocketMQError>,
    ) -> Self {
        Self { kind, fallback, source }
    }

    pub(crate) const fn kind(&self) -> PullCandidateBuildErrorKind {
        self.kind
    }

    pub(crate) fn into_fallback(self) -> RemotingResponse {
        self.fallback
    }
}

impl fmt::Debug for PullCandidateBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PullCandidateBuildError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for PullCandidateBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Pull suspension candidate failed: {:?}", self.kind)
    }
}

impl Error for PullCandidateBuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_ref().map(|source| source as &(dyn Error + 'static))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PullDeferredPrepareErrorKind {
    Build(PullCandidateBuildErrorKind),
    InvalidExpiryMargins,
    Deadline,
    Index,
    RetainedSizeOverflow,
    Contract,
}

#[must_use]
pub(crate) enum PullDeferredPrepareOutcome {
    Prepared(PreparedPullRegistration),
    Rejected(PullDeferredPrepareRejection),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PullDeferredPrepareRejectionKind {
    ServiceClosed,
    DeadlineElapsed,
    IndexCapacity,
    AdmissionCapacity,
}

pub(crate) enum PullDeferredPrepareRejection {
    ServiceClosed(PullSuspensionCandidate),
    DeadlineElapsed(PullSuspensionCandidate),
    Index {
        rejection: PullIndexRejection,
        candidate: PullSuspensionCandidate,
    },
    Admission {
        outcome: DeferredAdmissionAcquireOutcome,
        candidate: PullSuspensionCandidate,
    },
}

impl PullDeferredPrepareRejection {
    pub(crate) const fn kind(&self) -> PullDeferredPrepareRejectionKind {
        match self {
            Self::ServiceClosed(_) => PullDeferredPrepareRejectionKind::ServiceClosed,
            Self::DeadlineElapsed(_) => PullDeferredPrepareRejectionKind::DeadlineElapsed,
            Self::Index { .. } => PullDeferredPrepareRejectionKind::IndexCapacity,
            Self::Admission { .. } => PullDeferredPrepareRejectionKind::AdmissionCapacity,
        }
    }

    pub(crate) fn into_fallback(self) -> RemotingResponse {
        match self {
            Self::ServiceClosed(candidate)
            | Self::DeadlineElapsed(candidate)
            | Self::Index { candidate, .. }
            | Self::Admission { candidate, .. } => candidate.into_fallback(),
        }
    }
}

pub(crate) enum PullDeferredPrepareError {
    Build(PullCandidateBuildError),
    InvalidExpiryMargins {
        candidate: PullSuspensionCandidate,
    },
    Deadline {
        source: PullWaitDeadlineError,
        candidate: PullSuspensionCandidate,
    },
    Index {
        source: PullIndexOperationalError,
        candidate: PullSuspensionCandidate,
    },
    RetainedSizeOverflow {
        candidate: PullSuspensionCandidate,
    },
    Contract {
        source: TransportContractViolation,
        candidate: PullSuspensionCandidate,
    },
}

impl PullDeferredPrepareError {
    pub(crate) const fn kind(&self) -> PullDeferredPrepareErrorKind {
        match self {
            Self::Build(source) => PullDeferredPrepareErrorKind::Build(source.kind()),
            Self::InvalidExpiryMargins { .. } => PullDeferredPrepareErrorKind::InvalidExpiryMargins,
            Self::Deadline { .. } => PullDeferredPrepareErrorKind::Deadline,
            Self::Index { .. } => PullDeferredPrepareErrorKind::Index,
            Self::RetainedSizeOverflow { .. } => PullDeferredPrepareErrorKind::RetainedSizeOverflow,
            Self::Contract { .. } => PullDeferredPrepareErrorKind::Contract,
        }
    }

    pub(crate) fn into_fallback(self) -> RemotingResponse {
        match self {
            Self::Build(source) => source.into_fallback(),
            Self::InvalidExpiryMargins { candidate }
            | Self::Deadline { candidate, .. }
            | Self::Index { candidate, .. }
            | Self::RetainedSizeOverflow { candidate }
            | Self::Contract { candidate, .. } => candidate.into_fallback(),
        }
    }
}

impl fmt::Debug for PullDeferredPrepareError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PullDeferredPrepareError")
            .field("kind", &self.kind())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for PullDeferredPrepareError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Pull deferred preparation failed: {:?}", self.kind())
    }
}

impl Error for PullDeferredPrepareError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Build(source) => Some(source),
            Self::Deadline { source, .. } => Some(source),
            Self::Index { source, .. } => Some(source),
            Self::Contract { source, .. } => Some(source),
            Self::InvalidExpiryMargins { .. } | Self::RetainedSizeOverflow { .. } => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PullDeferredRegisterErrorKind {
    Registry,
    Contract,
}

#[must_use]
pub(crate) enum PullDeferredRegisterOutcome {
    Registered(Box<DeferredRegistration>),
    Rejected(Box<PullDeferredRegisterRejection>),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PullDeferredRegisterRejectionKind {
    ProvenanceMismatch,
    ServiceClosed,
    Responder,
    Expiry,
    Registry,
}

pub(crate) enum PullDeferredRegisterRejection {
    PreTake {
        kind: PullDeferredRegisterRejectionKind,
        prepared: Box<PreparedPullRegistration>,
        responder: Option<DeferredResponderOutcome>,
    },
    Expiry {
        outcome: DeferredExpiryOutcome,
        parts: DeferredParts,
    },
    RegistryRejected,
}

impl PullDeferredRegisterRejection {
    pub(crate) const fn kind(&self) -> PullDeferredRegisterRejectionKind {
        match self {
            Self::PreTake { kind, .. } => *kind,
            Self::Expiry { .. } => PullDeferredRegisterRejectionKind::Expiry,
            Self::RegistryRejected => PullDeferredRegisterRejectionKind::Registry,
        }
    }

    pub(crate) fn into_pre_take_fallback(self) -> Result<RemotingResponse, Self> {
        match self {
            Self::PreTake { prepared, .. } => Ok((*prepared).into_candidate().into_fallback()),
            rejection => Err(rejection),
        }
    }

    pub(crate) fn into_candidate(self) -> Result<PullSuspensionCandidate, Self> {
        match self {
            Self::PreTake { prepared, .. } => Ok((*prepared).into_candidate()),
            rejection => Err(rejection),
        }
    }
}

pub(crate) enum PullDeferredRegisterError {
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

impl PullDeferredRegisterError {
    pub(crate) const fn kind(&self) -> PullDeferredRegisterErrorKind {
        match self {
            Self::RegistryIdentityExhausted | Self::RegistryContract(_) | Self::RegistryOperational(_) => {
                PullDeferredRegisterErrorKind::Registry
            }
            Self::Contract { .. } => PullDeferredRegisterErrorKind::Contract,
        }
    }
}

impl fmt::Debug for PullDeferredRegisterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PullDeferredRegisterError")
            .field("kind", &self.kind())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for PullDeferredRegisterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Pull deferred registration failed: {:?}", self.kind())
    }
}

impl Error for PullDeferredRegisterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RegistryContract(violation) => Some(violation),
            Self::RegistryOperational(error) => Some(error),
            Self::Contract { violation, .. } => Some(violation),
            Self::RegistryIdentityExhausted => None,
        }
    }
}
