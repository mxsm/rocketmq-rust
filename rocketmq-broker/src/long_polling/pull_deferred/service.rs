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
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_transport::api::v2::ClaimedDeferred;
use rocketmq_transport::api::v2::DeferredAdmission;
use rocketmq_transport::api::v2::DeferredAdmissionAcquireError;
use rocketmq_transport::api::v2::DeferredAdmissionSnapshot;
use rocketmq_transport::api::v2::DeferredClaimError;
use rocketmq_transport::api::v2::DeferredExpiryBatch;
use rocketmq_transport::api::v2::DeferredExpiryBatchStats;
use rocketmq_transport::api::v2::DeferredExpiryErrorKind;
use rocketmq_transport::api::v2::DeferredExpiryMargins;
use rocketmq_transport::api::v2::DeferredId;
use rocketmq_transport::api::v2::DeferredParts;
use rocketmq_transport::api::v2::DeferredRegistration;
use rocketmq_transport::api::v2::DeferredRegistry;
use rocketmq_transport::api::v2::DeferredRegistryErrorKind;
use rocketmq_transport::api::v2::DeferredRegistryShutdownOutcome;
use rocketmq_transport::api::v2::DeferredResumeError;
use rocketmq_transport::api::v2::DeferredResumeRetainedSize;
use rocketmq_transport::api::v2::DeferredRetainedSizeParts;
use rocketmq_transport::api::v2::DeferredWakeReason;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestId;
use rocketmq_transport::api::v2::RequestOrigin;
use rocketmq_transport::api::v2::ResponsePlan;
use rocketmq_transport::api::v2::ResponseReceipt;
use rocketmq_transport::api::v2::SessionId;
use rocketmq_transport::api::v2::TakeDeferredResponderError;

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
    permit: rocketmq_transport::api::v2::DeferredWaitPermit,
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
        Self {
            admission,
            registry: DeferredRegistry::new(),
            index: PullCriteriaIndex::new(index_limits),
            expiry_margins,
            scan_limit,
            candidate_limit,
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

    pub(crate) fn reserve_arrival_batch(
        &self,
        arrival: &PullArrivalView<'_>,
        cursor: &mut PullScanCursor,
    ) -> Vec<PullCandidateReservation> {
        if self.closed.load(Ordering::Acquire) {
            return Vec::new();
        }
        self.index
            .reserve_matching(arrival, cursor, self.scan_limit, self.candidate_limit)
    }

    /// Selects every waiter matching one borrowed arrival in bounded batches.
    ///
    /// `submit` must synchronously transfer the affine candidates to a
    /// lifecycle-owned task. If it rejects a batch, dropping that batch restores
    /// index visibility and this callback stops without spinning.
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

    /// Selects every currently indexed waiter for master-online refresh.
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
        claimed.resume(retained, handler).await
    }

    #[must_use]
    pub(crate) fn shutdown(&self) -> DeferredRegistryShutdownOutcome {
        self.closed.store(true, Ordering::Release);
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
) -> Result<rocketmq_transport::api::v2::DeferredRetainedSize, PullRetainedSizeError> {
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
