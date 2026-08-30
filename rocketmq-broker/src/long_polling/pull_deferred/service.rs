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
use rocketmq_transport::api::DeferredAdmissionAcquireError;
use rocketmq_transport::api::DeferredAdmissionSnapshot;
use rocketmq_transport::api::DeferredClaimError;
use rocketmq_transport::api::DeferredExpiryBatch;
use rocketmq_transport::api::DeferredExpiryBatchStats;
use rocketmq_transport::api::DeferredExpiryErrorKind;
use rocketmq_transport::api::DeferredExpiryMargins;
use rocketmq_transport::api::DeferredId;
use rocketmq_transport::api::DeferredParts;
use rocketmq_transport::api::DeferredRegistration;
use rocketmq_transport::api::DeferredRegistry;
use rocketmq_transport::api::DeferredRegistryErrorKind;
use rocketmq_transport::api::DeferredRegistryShutdownOutcome;
use rocketmq_transport::api::DeferredResumeError;
use rocketmq_transport::api::DeferredResumeRetainedSize;
use rocketmq_transport::api::DeferredRetainedSizeParts;
use rocketmq_transport::api::DeferredWakeReason;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestId;
use rocketmq_transport::api::RequestOrigin;
use rocketmq_transport::api::ResponsePlan;
use rocketmq_transport::api::ResponseReceipt;
use rocketmq_transport::api::SessionId;
use rocketmq_transport::api::TakeDeferredResponderError;

use super::data::PullHookMetadata;
use super::data::PullMatchCriteria;
use super::data::PullRequestData;
use super::deadline::PullWaitDeadline;
use super::deadline::PullWaitDeadlineError;
use super::index::PullArrivalView;
use super::index::PullCandidateBatch;
use super::index::PullCandidateReservation;
use super::index::PullCriteriaIndex;
use super::index::PullCriteriaKey;
use super::index::PullCriteriaLimits;
use super::index::PullIndexError;
use super::index::PullIndexLease;
use super::index::PullIndexReservation;
use super::index::PullIndexSnapshot;
use super::index::PullScanCursor;

mod continuation;

use crate::long_polling::pending_arrival_latch::PendingArrivalInsertError;
use crate::long_polling::pending_arrival_latch::PendingArrivalLatch;
use crate::long_polling::pending_arrival_latch::PendingArrivalReservation;
use crate::long_polling::pending_arrival_latch::PendingOffsetRangeLatch;
use crate::long_polling::pending_arrival_latch::PendingOffsetRangeReservation;
use crate::long_polling::pending_arrival_latch::PendingOffsetTarget;
pub(crate) use continuation::PullArrivalContinuation;
use continuation::PullContinuationAdmission;
pub(crate) use continuation::PullContinuationError;
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
    fallback: ResponsePlan,
    timing: PullSuspendTiming,
    retained: PullRetainedEstimate,
    provenance: PreparedRequestProvenance,
}

impl PullSuspensionCandidate {
    pub(crate) fn from_request(
        request: &RemotingRequest,
        criteria: PullMatchCriteria,
        fallback: ResponsePlan,
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
        fallback: ResponsePlan,
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

    pub(crate) fn into_fallback(self) -> ResponsePlan {
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
        fallback: ResponsePlan,
        timing: PullSuspendTiming,
        retained: PullRetainedEstimate,
    ) -> Result<PreparedPullRegistration, PullDeferredPrepareError> {
        let candidate = PullSuspensionCandidate::from_request(request, criteria, fallback, timing, retained)
            .map_err(PullDeferredPrepareError::Build)?;
        self.prepare_candidate_at(candidate, current_millis(), tokio::time::Instant::now())
    }

    fn prepare_candidate_at(
        &self,
        candidate: PullSuspensionCandidate,
        wall_now: u64,
        monotonic_now: tokio::time::Instant,
    ) -> Result<PreparedPullRegistration, PullDeferredPrepareError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PullDeferredPrepareError::rejected(
                PullDeferredPrepareErrorKind::ServiceClosed,
                candidate,
                None,
            ));
        }
        if self.expiry_margins.recovery().is_zero() || self.expiry_margins.write().is_zero() {
            return Err(PullDeferredPrepareError::rejected(
                PullDeferredPrepareErrorKind::InvalidExpiryMargins,
                candidate,
                None,
            ));
        }
        let deadline = match PullWaitDeadline::checked(
            candidate.timing.suspend_wall_millis,
            candidate.timing.suspend_monotonic,
            candidate.timing.effective_timeout_millis,
            wall_now,
            monotonic_now,
        ) {
            Ok(deadline) => deadline,
            Err(source) => {
                return Err(PullDeferredPrepareError::Deadline { source, candidate });
            }
        };
        let key = PullCriteriaKey::from_criteria(&candidate.criteria);
        let reservation = match self.index.reserve(key) {
            Ok(reservation) => reservation,
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
            Err(PullRetainedSizeError::Admission(source)) => {
                drop(reservation);
                return Err(PullDeferredPrepareError::Admission { source, candidate });
            }
        };
        let permit = match self.admission.try_reserve(retained_size) {
            Ok(permit) => permit,
            Err(source) => {
                drop(reservation);
                return Err(PullDeferredPrepareError::Admission { source, candidate });
            }
        };
        let prepared = PreparedPullRegistration {
            candidate,
            deadline,
            reservation,
            permit,
        };
        if self.closed.load(Ordering::Acquire) {
            return Err(PullDeferredPrepareError::rejected(
                PullDeferredPrepareErrorKind::ServiceClosed,
                prepared.into_candidate(),
                None,
            ));
        }
        Ok(prepared)
    }

    pub(crate) fn register(
        &self,
        prepared: PreparedPullRegistration,
        request: &mut RemotingRequest,
    ) -> Result<DeferredRegistration, PullDeferredRegisterError> {
        if !prepared.candidate.provenance.matches(request) {
            return Err(PullDeferredRegisterError::pre_take(
                PullDeferredRegisterErrorKind::ProvenanceMismatch,
                prepared,
                None,
            ));
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(PullDeferredRegisterError::pre_take(
                PullDeferredRegisterErrorKind::ServiceClosed,
                prepared,
                None,
            ));
        }
        let responder = match request.take_deferred_responder() {
            Ok(responder) => responder,
            Err(source) => {
                return Err(PullDeferredRegisterError::pre_take(
                    PullDeferredRegisterErrorKind::Responder,
                    prepared,
                    Some(source),
                ));
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
        let parts =
            match DeferredParts::new(responder, permit).try_with_expiry(deadline.protocol_at(), self.expiry_margins) {
                Ok(parts) => parts,
                Err(source) => {
                    let kind = source.kind();
                    let request_id = source.request_id();
                    drop(source.into_parts());
                    return Err(PullDeferredRegisterError::Expiry { kind, request_id });
                }
            };
        drop(fallback);
        match self.registry.register_with(parts, move |id| {
            let lease = reservation.publish(id, Arc::clone(&criteria));
            Ok::<_, Infallible>(ResumePull::new(request, criteria, deadline, lease))
        }) {
            Ok(registration) => Ok(registration),
            Err(source) => {
                let kind = source.kind();
                let request_id = source.request_id();
                drop(source);
                Err(PullDeferredRegisterError::Registry { kind, request_id })
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
    ) -> Result<PullArrivalContinuation, PullContinuationError> {
        PullArrivalContinuation::arrival(&self.continuation_admission, arrival, cursor)
    }

    pub(crate) fn admit_forced_continuation(
        &self,
        cursor: PullScanCursor,
    ) -> Result<PullArrivalContinuation, PullContinuationError> {
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
    ) -> Result<(), PullPendingArrivalError> {
        let key = PullPendingArrivalKey::Arrival(
            self.pending_arrival_sequence.fetch_add(1, Ordering::Relaxed),
            PullCriteriaKey::new(arrival.topic().clone(), arrival.queue_id()),
        );
        let pending = PullPendingArrival::arrival(arrival, cursor).map_err(PullPendingArrivalError::Continuation)?;
        self.pending_arrivals
            .insert(key, pending)
            .map_err(PullPendingArrivalError::Latch)
    }

    pub(crate) fn latch_forced(&self, cursor: PullScanCursor) -> Result<(), PullPendingArrivalError> {
        let key = PullPendingArrivalKey::Forced;
        if self.pending_arrivals.coalesce_existing(&key) {
            return Ok(());
        }
        self.pending_arrivals
            .insert(key, PullPendingArrival::forced(cursor))
            .map_err(PullPendingArrivalError::Latch)
    }

    pub(crate) fn pending_arrival_reservations(&self) -> Vec<PullPendingArrivalReservation> {
        self.pending_arrivals.reserve_batch(self.scan_limit.get())
    }

    pub(crate) fn latch_offset(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        logical_offset: i64,
    ) -> Result<(), PendingArrivalInsertError> {
        if logical_offset <= 0 {
            return Ok(());
        }
        self.latch_queue_offset_range(topic, queue_id, logical_offset - 1, logical_offset - 1)
    }

    pub(crate) fn latch_max_offset_range(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        max_offset: i64,
    ) -> Result<(), PendingArrivalInsertError> {
        if max_offset <= 0 {
            return Ok(());
        }
        self.latch_queue_offset_range(topic, queue_id, 0, max_offset - 1)
    }

    fn latch_queue_offset_range(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        first: i64,
        last: i64,
    ) -> Result<(), PendingArrivalInsertError> {
        let key = PullCriteriaKey::new(topic.clone(), queue_id);
        if !self.index.has_target(&key) {
            return Ok(());
        }
        self.pending_offsets
            .retain_targets(|target| self.index.has_target(target));
        self.pending_offsets.merge_range(key, first, last)
    }

    pub(crate) fn pending_offset_reservations(&self) -> Vec<PullPendingOffsetReservation> {
        self.pending_offsets
            .reserve_batch(self.scan_limit.get())
            .into_iter()
            .filter_map(|reservation| {
                let permit = self.continuation_admission.reserve(reservation.retained_bytes()).ok()?;
                Some(PullPendingOffsetReservation {
                    reservation,
                    _permit: permit,
                })
            })
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
    ) -> Result<ClaimedDeferred<ResumePull>, DeferredClaimError> {
        let id = candidate.id();
        let mut claimed = self.registry.claim(id, reason).await?;
        candidate.commit();
        drop(claimed.resume_data_mut().take_index_lease());
        Ok(claimed)
    }

    pub(crate) async fn claim(
        &self,
        id: DeferredId,
        reason: DeferredWakeReason,
    ) -> Result<ClaimedDeferred<ResumePull>, DeferredClaimError> {
        let mut claimed = self.registry.claim(id, reason).await?;
        drop(claimed.resume_data_mut().take_index_lease());
        Ok(claimed)
    }

    pub(crate) fn sweep_expired(&self) -> PullDeferredSweepBatch {
        PullDeferredSweepBatch::from_transport(self.registry.sweep_expired(self.candidate_limit))
    }

    pub(crate) async fn resume_claimed<F, Fut>(
        &self,
        claimed: ClaimedDeferred<ResumePull>,
        retained: DeferredResumeRetainedSize,
        handler: F,
    ) -> Result<ResponseReceipt, DeferredResumeError>
    where
        F: FnOnce(ResumePull, DeferredWakeReason) -> Fut + Send + 'static,
        Fut: Future<Output = rocketmq_error::RocketMQResult<ResponsePlan>> + Send + 'static,
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
    ) -> Result<(), DeferredResumeError>
    where
        F: FnOnce(ResumePull, DeferredWakeReason) -> Fut + Send + 'static,
        Fut: Future<Output = rocketmq_error::RocketMQResult<ResponsePlan>> + Send + 'static,
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
pub(crate) enum PullPendingArrivalError {
    Continuation(PullContinuationError),
    Latch(PendingArrivalInsertError),
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
    .map_err(PullRetainedSizeError::Admission)
}

enum PullRetainedSizeError {
    Overflow,
    Admission(DeferredAdmissionAcquireError),
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
    fallback: ResponsePlan,
    source: Option<rocketmq_error::RocketMQError>,
}

impl PullCandidateBuildError {
    fn new(
        kind: PullCandidateBuildErrorKind,
        fallback: ResponsePlan,
        source: Option<rocketmq_error::RocketMQError>,
    ) -> Self {
        Self { kind, fallback, source }
    }

    pub(crate) const fn kind(&self) -> PullCandidateBuildErrorKind {
        self.kind
    }

    pub(crate) fn into_fallback(self) -> ResponsePlan {
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
    ServiceClosed,
    InvalidExpiryMargins,
    Deadline,
    Index,
    RetainedSizeOverflow,
    Admission,
}

pub(crate) enum PullDeferredPrepareError {
    Build(PullCandidateBuildError),
    Rejected {
        kind: PullDeferredPrepareErrorKind,
        candidate: PullSuspensionCandidate,
    },
    Deadline {
        source: PullWaitDeadlineError,
        candidate: PullSuspensionCandidate,
    },
    Index {
        source: PullIndexError,
        candidate: PullSuspensionCandidate,
    },
    RetainedSizeOverflow {
        candidate: PullSuspensionCandidate,
    },
    Admission {
        source: DeferredAdmissionAcquireError,
        candidate: PullSuspensionCandidate,
    },
}

impl PullDeferredPrepareError {
    fn rejected(
        kind: PullDeferredPrepareErrorKind,
        candidate: PullSuspensionCandidate,
        _source: Option<Infallible>,
    ) -> Self {
        Self::Rejected { kind, candidate }
    }

    pub(crate) const fn kind(&self) -> PullDeferredPrepareErrorKind {
        match self {
            Self::Build(source) => PullDeferredPrepareErrorKind::Build(source.kind()),
            Self::Rejected { kind, .. } => *kind,
            Self::Deadline { .. } => PullDeferredPrepareErrorKind::Deadline,
            Self::Index { .. } => PullDeferredPrepareErrorKind::Index,
            Self::RetainedSizeOverflow { .. } => PullDeferredPrepareErrorKind::RetainedSizeOverflow,
            Self::Admission { .. } => PullDeferredPrepareErrorKind::Admission,
        }
    }

    pub(crate) fn into_fallback(self) -> ResponsePlan {
        match self {
            Self::Build(source) => source.into_fallback(),
            Self::Rejected { candidate, .. }
            | Self::Deadline { candidate, .. }
            | Self::Index { candidate, .. }
            | Self::RetainedSizeOverflow { candidate }
            | Self::Admission { candidate, .. } => candidate.into_fallback(),
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
            Self::Admission { source, .. } => Some(source),
            Self::Rejected { .. } | Self::RetainedSizeOverflow { .. } => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PullDeferredRegisterErrorKind {
    ServiceClosed,
    ProvenanceMismatch,
    Responder,
    Expiry(DeferredExpiryErrorKind),
    Registry(DeferredRegistryErrorKind),
}

pub(crate) enum PullDeferredRegisterError {
    PreTake {
        kind: PullDeferredRegisterErrorKind,
        prepared: Box<PreparedPullRegistration>,
        source: Option<TakeDeferredResponderError>,
    },
    Expiry {
        kind: DeferredExpiryErrorKind,
        request_id: RequestId,
    },
    Registry {
        kind: DeferredRegistryErrorKind,
        request_id: RequestId,
    },
}

impl PullDeferredRegisterError {
    fn pre_take(
        kind: PullDeferredRegisterErrorKind,
        prepared: PreparedPullRegistration,
        source: Option<TakeDeferredResponderError>,
    ) -> Self {
        Self::PreTake {
            kind,
            prepared: Box::new(prepared),
            source,
        }
    }

    pub(crate) const fn kind(&self) -> PullDeferredRegisterErrorKind {
        match self {
            Self::PreTake { kind, .. } => *kind,
            Self::Expiry { kind, .. } => PullDeferredRegisterErrorKind::Expiry(*kind),
            Self::Registry { kind, .. } => PullDeferredRegisterErrorKind::Registry(*kind),
        }
    }

    pub(crate) const fn request_id(&self) -> Option<RequestId> {
        match self {
            Self::PreTake { .. } => None,
            Self::Expiry { request_id, .. } | Self::Registry { request_id, .. } => Some(*request_id),
        }
    }

    pub(crate) fn into_pre_take_fallback(self) -> Result<ResponsePlan, Self> {
        self.into_candidate().map(PullSuspensionCandidate::into_fallback)
    }

    /// Recovers the exact affine suspension candidate from every pre-take failure.
    pub(crate) fn into_candidate(self) -> Result<PullSuspensionCandidate, Self> {
        match self {
            Self::PreTake { prepared, .. } => Ok((*prepared).into_candidate()),
            error => Err(error),
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
            Self::PreTake { source, .. } => source.as_ref().map(|source| source as &(dyn Error + 'static)),
            Self::Expiry { .. } | Self::Registry { .. } => None,
        }
    }
}
