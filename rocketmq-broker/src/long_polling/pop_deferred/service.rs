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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader;
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
use rocketmq_transport::api::v2::DeferredResumeErrorKind;
use rocketmq_transport::api::v2::DeferredResumeRetainedSize;
use rocketmq_transport::api::v2::DeferredRetainedSizeParts;
use rocketmq_transport::api::v2::DeferredTerminalReason;
use rocketmq_transport::api::v2::DeferredWakeReason;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestId;
use rocketmq_transport::api::v2::RequestOrigin;
use rocketmq_transport::api::v2::ResponsePlan;
use rocketmq_transport::api::v2::ResponseReceipt;
use rocketmq_transport::api::v2::SessionId;
use rocketmq_transport::api::v2::TakeDeferredResponderError;
use tokio::sync::oneshot;

use crate::long_polling::long_polling_service::pop_long_polling_service::PopWakeupCompletion;
use crate::long_polling::long_polling_service::pop_long_polling_service::PopWakeupOutcome;

use super::deadline::LongPollingDeadline;
use super::deadline::LongPollingDeadlineError;
use super::index::PopArrival;
use super::index::PopCriteriaIndex;
use super::index::PopCriteriaKey;
use super::index::PopCriteriaLimits;
use super::index::PopIndexError;
use super::index::PopIndexLease;
use super::index::PopIndexReservation;
use super::index::PopIndexSnapshot;
use super::index::PopMatchCriteria;
use super::index::PopSelectionOrder;

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

    /// Returns the trusted effective peer captured by the V2 ingress boundary.
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
    permit: rocketmq_transport::api::v2::DeferredWaitPermit,
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
    closed: AtomicBool,
}

/// Affine completion owner for one V2 POP wake attempt.
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

    pub(crate) fn complete_claim_error(self, error: &DeferredClaimError) {
        self.complete(pop_wakeup_outcome_from_claim_error(error));
    }

    fn complete_resume_result(self, result: &Result<ResponseReceipt, DeferredResumeError>) {
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
        Self {
            admission,
            registry: DeferredRegistry::new(),
            index: PopCriteriaIndex::new(index_limits),
            expiry_margins,
            sweep_limit,
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
    ) -> Result<PreparedPopRegistration, PopDeferredPrepareError> {
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
    ) -> Result<PreparedPopRegistration, PopDeferredPrepareError> {
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
    ) -> Result<PreparedPopRegistration, PopDeferredPrepareError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PopDeferredPrepareError::ServiceClosed);
        }
        if request.caller_host.is_empty() {
            return Err(PopDeferredPrepareError::MissingCallerHost);
        }
        if self.expiry_margins.recovery().is_zero() || self.expiry_margins.write().is_zero() {
            return Err(PopDeferredPrepareError::InvalidExpiryMargins);
        }
        let deadline = LongPollingDeadline::checked(
            request.header.born_time,
            request.header.poll_time,
            wall_now,
            monotonic_now,
        )
        .map_err(PopDeferredPrepareError::Deadline)?;
        let key = PopCriteriaKey::from_parts(request.topic(), request.consumer_group(), request.queue_id());
        let reservation = self.index.reserve(key).map_err(PopDeferredPrepareError::Index)?;
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
            .map_err(PopDeferredPrepareError::Admission)?;
        let permit = self
            .admission
            .try_reserve(retained_size)
            .map_err(PopDeferredPrepareError::Admission)?;
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
            return Err(PopDeferredPrepareError::ServiceClosed);
        }
        Ok(prepared)
    }

    /// Moves the prepared index reservation into `register_with`'s infallible builder.
    pub(crate) fn register(
        &self,
        prepared: PreparedPopRegistration,
        request: &mut RemotingRequest,
    ) -> Result<DeferredRegistration, PopDeferredRegisterError> {
        if !prepared
            .provenance
            .is_some_and(|provenance| provenance.matches(request))
        {
            return Err(PopDeferredRegisterError::ProvenanceMismatch);
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(PopDeferredRegisterError::ServiceClosed);
        }
        let responder = request
            .take_deferred_responder()
            .map_err(PopDeferredRegisterError::Responder)?;
        let PreparedPopRegistration {
            request,
            criteria,
            deadline,
            reservation,
            permit,
            provenance: _,
        } = prepared;
        let parts = DeferredParts::new(responder, permit)
            .try_with_expiry(deadline.protocol_at(), self.expiry_margins)
            .map_err(PopDeferredRegisterError::Expiry)?;
        self.registry
            .register_with(parts, move |id| {
                let index_lease = reservation.publish(id, deadline, Arc::clone(&criteria));
                Ok::<_, Infallible>(ResumePop::new(request, criteria, deadline, index_lease))
            })
            .map_err(PopDeferredRegisterError::Registry)
    }

    pub(crate) async fn claim(
        &self,
        id: DeferredId,
        reason: DeferredWakeReason,
    ) -> Result<ClaimedDeferred<ResumePop>, DeferredClaimError> {
        let mut claimed = self.registry.claim(id, reason).await?;
        drop(claimed.resume_data_mut().take_index_lease());
        Ok(claimed)
    }

    pub(crate) async fn claim_message(
        &self,
        arrival: &PopArrival,
        order: PopSelectionOrder,
    ) -> Result<Option<ClaimedDeferred<ResumePop>>, DeferredClaimError> {
        self.claim_matching(arrival, order, DeferredWakeReason::MessageArrived)
            .await
    }

    pub(crate) async fn claim_forced(
        &self,
        arrival: PopArrival,
        order: PopSelectionOrder,
    ) -> Result<Option<ClaimedDeferred<ResumePop>>, DeferredClaimError> {
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
    ) -> Result<ResponseReceipt, DeferredResumeError>
    where
        F: FnOnce(ResumePop, DeferredWakeReason) -> Fut + Send + 'static,
        Fut: Future<Output = rocketmq_error::RocketMQResult<ResponsePlan>> + Send + 'static,
    {
        claimed.resume(handler_retained, handler).await
    }

    /// Resumes and writes one claimed POP response, completing the legacy
    /// observer from the final canonical outcome rather than task submission.
    pub(crate) async fn resume_claimed_observed<F, Fut>(
        &self,
        claimed: ClaimedDeferred<ResumePop>,
        handler_retained: DeferredResumeRetainedSize,
        observer: PopDeferredWakeupObserver,
        handler: F,
    ) -> Result<ResponseReceipt, DeferredResumeError>
    where
        F: FnOnce(ResumePop, DeferredWakeReason) -> Fut + Send + 'static,
        Fut: Future<Output = rocketmq_error::RocketMQResult<ResponsePlan>> + Send + 'static,
    {
        let result = self.resume_claimed(claimed, handler_retained, handler).await;
        observer.complete_resume_result(&result);
        result
    }

    async fn claim_matching(
        &self,
        arrival: &PopArrival,
        order: PopSelectionOrder,
        reason: DeferredWakeReason,
    ) -> Result<Option<ClaimedDeferred<ResumePop>>, DeferredClaimError> {
        let mut last_race = None;
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
                Ok(claimed) => {
                    drop(candidate);
                    return Ok(Some(claimed));
                }
                Err(error) if is_skippable_candidate_error(error.kind()) => {
                    drop(candidate);
                    last_race = Some(error);
                }
                Err(error) => {
                    drop(candidate);
                    return Err(error);
                }
            }
        }
        match last_race {
            Some(error) => Err(error),
            None => Ok(None),
        }
    }

    pub(crate) fn sweep_expired(&self) -> PopDeferredSweepBatch {
        PopDeferredSweepBatch::from_transport(self.registry.sweep_expired(self.sweep_limit))
    }

    /// Projects a producer's topic/queue arrival into a bounded set of live
    /// consumer groups without exposing requests or transport capabilities.
    pub(crate) fn consumer_groups_for_arrival(&self, topic: &CheetahString, queue_id: i32) -> Vec<CheetahString> {
        self.index.consumer_groups(topic, queue_id, self.sweep_limit)
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

    pub(crate) fn index_contains(&self, id: DeferredId) -> bool {
        self.index.contains(id)
    }

    #[must_use]
    pub(crate) fn shutdown(&self) -> DeferredRegistryShutdownOutcome {
        self.closed.store(true, Ordering::Release);
        self.registry.shutdown()
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
    ServiceClosed,
    EmbeddedOrigin,
    Header,
    MissingCallerHost,
    InvalidExpiryMargins,
    RetainedSizeOverflow,
    Deadline,
    Index,
    Admission,
}

pub(crate) enum PopDeferredPrepareError {
    ServiceClosed,
    EmbeddedOrigin,
    Header(RocketMQError),
    MissingCallerHost,
    InvalidExpiryMargins,
    RetainedSizeOverflow,
    Deadline(LongPollingDeadlineError),
    Index(PopIndexError),
    Admission(DeferredAdmissionAcquireError),
}

impl PopDeferredPrepareError {
    #[must_use]
    pub(crate) const fn kind(&self) -> PopDeferredPrepareErrorKind {
        match self {
            Self::ServiceClosed => PopDeferredPrepareErrorKind::ServiceClosed,
            Self::EmbeddedOrigin => PopDeferredPrepareErrorKind::EmbeddedOrigin,
            Self::Header(_) => PopDeferredPrepareErrorKind::Header,
            Self::MissingCallerHost => PopDeferredPrepareErrorKind::MissingCallerHost,
            Self::InvalidExpiryMargins => PopDeferredPrepareErrorKind::InvalidExpiryMargins,
            Self::RetainedSizeOverflow => PopDeferredPrepareErrorKind::RetainedSizeOverflow,
            Self::Deadline(_) => PopDeferredPrepareErrorKind::Deadline,
            Self::Index(_) => PopDeferredPrepareErrorKind::Index,
            Self::Admission(_) => PopDeferredPrepareErrorKind::Admission,
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
            Self::Admission(source) => Some(source),
            Self::ServiceClosed
            | Self::EmbeddedOrigin
            | Self::MissingCallerHost
            | Self::InvalidExpiryMargins
            | Self::RetainedSizeOverflow => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PopDeferredRegisterErrorKind {
    ServiceClosed,
    ProvenanceMismatch,
    Responder,
    Expiry(DeferredExpiryErrorKind),
    Registry(DeferredRegistryErrorKind),
}

pub(crate) enum PopDeferredRegisterError {
    ServiceClosed,
    ProvenanceMismatch,
    Responder(TakeDeferredResponderError),
    Expiry(DeferredExpiryError),
    Registry(DeferredRegistryError<ResumePop, Infallible>),
}

impl PopDeferredRegisterError {
    #[must_use]
    pub(crate) const fn kind(&self) -> PopDeferredRegisterErrorKind {
        match self {
            Self::ServiceClosed => PopDeferredRegisterErrorKind::ServiceClosed,
            Self::ProvenanceMismatch => PopDeferredRegisterErrorKind::ProvenanceMismatch,
            Self::Responder(_) => PopDeferredRegisterErrorKind::Responder,
            Self::Expiry(source) => PopDeferredRegisterErrorKind::Expiry(source.kind()),
            Self::Registry(source) => PopDeferredRegisterErrorKind::Registry(source.kind()),
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
            Self::Responder(source) => Some(source),
            Self::Expiry(source) => Some(source),
            Self::Registry(source) => Some(source),
            Self::ServiceClosed | Self::ProvenanceMismatch => None,
        }
    }
}

fn pop_wakeup_outcome_from_claim_error(error: &DeferredClaimError) -> PopWakeupOutcome {
    match error.kind() {
        DeferredClaimErrorKind::NotFound
        | DeferredClaimErrorKind::AlreadyClaimed
        | DeferredClaimErrorKind::AlreadyCompleted => PopWakeupOutcome::AlreadyCompleted,
        DeferredClaimErrorKind::SessionClosed => PopWakeupOutcome::InactiveChannel,
        DeferredClaimErrorKind::ParentCancelled => PopWakeupOutcome::ServiceCancelled,
        DeferredClaimErrorKind::DeadlineExpired | DeferredClaimErrorKind::RegistryInvariant => {
            PopWakeupOutcome::ProcessingFailed
        }
    }
}

const fn is_skippable_candidate_error(kind: DeferredClaimErrorKind) -> bool {
    matches!(
        kind,
        DeferredClaimErrorKind::NotFound
            | DeferredClaimErrorKind::AlreadyClaimed
            | DeferredClaimErrorKind::AlreadyCompleted
            | DeferredClaimErrorKind::SessionClosed
            | DeferredClaimErrorKind::DeadlineExpired
    )
}

fn pop_wakeup_outcome_from_resume_result(result: &Result<ResponseReceipt, DeferredResumeError>) -> PopWakeupOutcome {
    let Err(error) = result else {
        return PopWakeupOutcome::ProcessingCompleted;
    };
    match error.prior_terminal_reason() {
        Some(DeferredTerminalReason::SessionClosed | DeferredTerminalReason::ReceiverDropped) => {
            return PopWakeupOutcome::InactiveChannel;
        }
        Some(DeferredTerminalReason::ParentCancelled | DeferredTerminalReason::ServiceStopping) => {
            return PopWakeupOutcome::ServiceCancelled;
        }
        Some(DeferredTerminalReason::ProcessorUnavailable) => {
            return PopWakeupOutcome::ProcessorUnavailable;
        }
        _ => {}
    }
    match error.kind() {
        DeferredResumeErrorKind::SessionClosed => PopWakeupOutcome::InactiveChannel,
        DeferredResumeErrorKind::Cancelled => PopWakeupOutcome::ServiceCancelled,
        DeferredResumeErrorKind::ExecutorClosing => PopWakeupOutcome::ServiceNotRunning,
        DeferredResumeErrorKind::TaskTerminated
        | DeferredResumeErrorKind::Admission
        | DeferredResumeErrorKind::RetainedSizeOverflow
        | DeferredResumeErrorKind::Response
        | DeferredResumeErrorKind::ResponsePlan => PopWakeupOutcome::ProcessingFailed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retired_candidate_errors_are_local_but_parent_and_invariant_fail_closed() {
        for kind in [
            DeferredClaimErrorKind::NotFound,
            DeferredClaimErrorKind::AlreadyClaimed,
            DeferredClaimErrorKind::AlreadyCompleted,
            DeferredClaimErrorKind::SessionClosed,
            DeferredClaimErrorKind::DeadlineExpired,
        ] {
            assert!(is_skippable_candidate_error(kind), "{kind:?}");
        }
        for kind in [
            DeferredClaimErrorKind::ParentCancelled,
            DeferredClaimErrorKind::RegistryInvariant,
        ] {
            assert!(!is_skippable_candidate_error(kind), "{kind:?}");
        }
    }
}
