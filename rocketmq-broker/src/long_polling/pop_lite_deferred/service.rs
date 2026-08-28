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

use std::collections::HashSet;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Barrier;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_transport::api::v2::ClaimedDeferred;
use rocketmq_transport::api::v2::DeferredAdmission;
use rocketmq_transport::api::v2::DeferredAdmissionSnapshot;
use rocketmq_transport::api::v2::DeferredClaimError;
use rocketmq_transport::api::v2::DeferredExpiryBatch;
use rocketmq_transport::api::v2::DeferredExpiryBatchStats;
use rocketmq_transport::api::v2::DeferredExpiryMargins;
use rocketmq_transport::api::v2::DeferredRegistry;
use rocketmq_transport::api::v2::DeferredRegistryShutdownOutcome;
use rocketmq_transport::api::v2::DeferredResumeError;
use rocketmq_transport::api::v2::DeferredResumeRetainedSize;
use rocketmq_transport::api::v2::DeferredWakeReason;
use rocketmq_transport::api::v2::ResponsePlan;
use rocketmq_transport::api::v2::ResponseReceipt;

use crate::lite::lite_event_dispatcher::LiteEventBatchExecution;
use crate::lite::lite_event_dispatcher::LiteEventBatchReservation;
use crate::lite::lite_event_dispatcher::LiteEventBatchTerminal;
use crate::lite::lite_event_dispatcher::LiteEventDispatcher;
use crate::lite::lite_event_dispatcher::LiteEventReservationSnapshot;

use super::data::ResumePopLite;
use super::gate::PopLiteEventGate;
use super::gate::PopLiteEventGateReservation;
use super::index::PopLiteCriteriaIndex;
use super::index::PopLiteIndexLimits;
use super::index::PopLiteIndexSnapshot;

#[derive(Default)]
pub(super) struct PopLiteServiceObservations {
    prepared: AtomicUsize,
    pending_claims: AtomicUsize,
    accepted_resumes: AtomicUsize,
    resume_execution_bytes: AtomicUsize,
}

pub(super) enum ObservationKind {
    Prepared,
    PendingClaim,
    AcceptedResume,
}

pub(super) struct ObservationGuard {
    observations: Arc<PopLiteServiceObservations>,
    kind: ObservationKind,
    retained_bytes: usize,
}

impl ObservationGuard {
    pub(super) fn new(observations: Arc<PopLiteServiceObservations>, kind: ObservationKind) -> Self {
        match kind {
            ObservationKind::Prepared => observations.prepared.fetch_add(1, Ordering::AcqRel),
            ObservationKind::PendingClaim => observations.pending_claims.fetch_add(1, Ordering::AcqRel),
            ObservationKind::AcceptedResume => observations.accepted_resumes.fetch_add(1, Ordering::AcqRel),
        };
        Self {
            observations,
            kind,
            retained_bytes: 0,
        }
    }

    pub(super) fn accepted(observations: Arc<PopLiteServiceObservations>, retained_bytes: usize) -> Self {
        observations.accepted_resumes.fetch_add(1, Ordering::AcqRel);
        observations
            .resume_execution_bytes
            .fetch_add(retained_bytes, Ordering::AcqRel);
        Self {
            observations,
            kind: ObservationKind::AcceptedResume,
            retained_bytes,
        }
    }
}

impl Drop for ObservationGuard {
    fn drop(&mut self) {
        match self.kind {
            ObservationKind::Prepared => self.observations.prepared.fetch_sub(1, Ordering::AcqRel),
            ObservationKind::PendingClaim => self.observations.pending_claims.fetch_sub(1, Ordering::AcqRel),
            ObservationKind::AcceptedResume => {
                self.observations.accepted_resumes.fetch_sub(1, Ordering::AcqRel);
                self.observations
                    .resume_execution_bytes
                    .fetch_sub(self.retained_bytes, Ordering::AcqRel)
            }
        };
    }
}

pub(crate) struct PopLiteDeferredService {
    pub(super) admission: DeferredAdmission,
    pub(super) registry: DeferredRegistry<ResumePopLite>,
    pub(super) index: PopLiteCriteriaIndex,
    event_gate: PopLiteEventGate,
    pub(super) dispatcher: LiteEventDispatcher,
    pub(super) expiry_margins: DeferredExpiryMargins,
    pub(super) max_age: Duration,
    sweep_limit: NonZeroUsize,
    pub(super) closed: Arc<AtomicBool>,
    pub(super) pending_replays: Arc<Mutex<HashSet<CheetahString>>>,
    pub(super) observations: Arc<PopLiteServiceObservations>,
    #[cfg(test)]
    replay_insert_hook: Arc<Mutex<Option<ReplayInsertHook>>>,
    #[cfg(test)]
    register_after_take_hook: Arc<Mutex<Option<RegisterAfterTakeHook>>>,
    #[cfg(test)]
    terminal_ready_hook: Arc<Mutex<Option<TerminalReadyHook>>>,
    #[cfg(test)]
    pub(super) fail_next_expiry_attachment: AtomicBool,
}

impl PopLiteDeferredService {
    pub(crate) fn new(
        admission: DeferredAdmission,
        index_limits: PopLiteIndexLimits,
        dispatcher: LiteEventDispatcher,
        expiry_margins: DeferredExpiryMargins,
        max_age: Duration,
        sweep_limit: NonZeroUsize,
    ) -> Self {
        Self {
            admission,
            registry: DeferredRegistry::new(),
            index: PopLiteCriteriaIndex::new(index_limits),
            event_gate: PopLiteEventGate::default(),
            dispatcher,
            expiry_margins,
            max_age,
            sweep_limit,
            closed: Arc::new(AtomicBool::new(false)),
            pending_replays: Arc::new(Mutex::new(HashSet::new())),
            observations: Arc::new(PopLiteServiceObservations::default()),
            #[cfg(test)]
            replay_insert_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            register_after_take_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            terminal_ready_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            fail_next_expiry_attachment: AtomicBool::new(false),
        }
    }

    pub(crate) fn observe_pending_event(&self, client_id: &CheetahString) -> bool {
        observe_replay(
            &self.closed,
            &self.pending_replays,
            &self.dispatcher,
            client_id,
            #[cfg(test)]
            &self.replay_insert_hook,
        )
    }

    pub(crate) fn take_pending_replays(&self, limit: NonZeroUsize) -> Vec<CheetahString> {
        let mut pending = self.pending_replays.lock();
        let clients = pending.iter().take(limit.get()).cloned().collect::<Vec<_>>();
        for client in &clients {
            pending.remove(client);
        }
        clients
    }

    pub(crate) async fn claim_event(
        &self,
        client_id: &CheetahString,
    ) -> Result<Option<PopLiteEventClaim>, DeferredClaimError> {
        let _pending = ObservationGuard::new(Arc::clone(&self.observations), ObservationKind::PendingClaim);
        let Some(gate) = self.event_gate.try_reserve(client_id) else {
            return Ok(None);
        };
        let Some(candidate) = self.index.reserve_oldest(client_id) else {
            drop(gate);
            return Ok(None);
        };
        let Some(events) = self.dispatcher.reserve_pending_events(client_id) else {
            drop(candidate);
            drop(gate);
            return Ok(None);
        };
        self.pending_replays.lock().remove(client_id);
        let id = candidate.id();
        match self.registry.claim(id, DeferredWakeReason::MessageArrived).await {
            Ok(mut claimed) => {
                drop(claimed.resume_data_mut().take_index_lease());
                drop(candidate);
                Ok(Some(PopLiteEventClaim {
                    claimed,
                    gate,
                    events,
                    client_id: client_id.clone(),
                }))
            }
            Err(error) => {
                drop(events);
                drop(candidate);
                drop(gate);
                self.observe_pending_event(client_id);
                Err(error)
            }
        }
    }

    pub(crate) async fn resume_event_claim<F, Fut>(
        &self,
        event_claim: PopLiteEventClaim,
        handler_retained: DeferredResumeRetainedSize,
        handler: F,
    ) -> Result<ResponseReceipt, DeferredResumeError>
    where
        F: FnOnce(ResumePopLite, DeferredWakeReason, LiteEventBatchExecution) -> Fut + Send + 'static,
        Fut: Future<Output = rocketmq_error::RocketMQResult<ResponsePlan>> + Send + 'static,
    {
        let PopLiteEventClaim {
            claimed,
            gate,
            events,
            client_id,
        } = event_claim;
        let retained = handler_retained.dynamic_bytes().saturating_add(events.retained_bytes());
        let (event_execution, event_terminal) = events.into_terminal_ownership();
        let observations = Arc::clone(&self.observations);
        let terminal = PopLiteEventTerminal {
            events: Some(event_terminal),
            gate: Some(gate),
            client_id,
            dispatcher: self.dispatcher.clone(),
            pending_replays: Arc::clone(&self.pending_replays),
            closed: Arc::clone(&self.closed),
            accepted: None,
            #[cfg(test)]
            replay_insert_hook: Arc::clone(&self.replay_insert_hook),
            #[cfg(test)]
            terminal_ready_hook: Arc::clone(&self.terminal_ready_hook),
        };
        claimed
            .resume(DeferredResumeRetainedSize::new(retained), move |resume, reason| {
                let mut terminal = terminal;
                terminal.accepted = Some(ObservationGuard::accepted(observations, retained));
                PopLiteEventTerminalFuture {
                    inner: handler(resume, reason, event_execution),
                    _terminal: terminal,
                }
            })
            .await
    }

    pub(crate) async fn resume_claimed<F, Fut>(
        &self,
        claimed: ClaimedDeferred<ResumePopLite>,
        handler_retained: DeferredResumeRetainedSize,
        handler: F,
    ) -> Result<ResponseReceipt, DeferredResumeError>
    where
        F: FnOnce(ResumePopLite, DeferredWakeReason) -> Fut + Send + 'static,
        Fut: Future<Output = rocketmq_error::RocketMQResult<ResponsePlan>> + Send + 'static,
    {
        let retained = handler_retained.dynamic_bytes();
        let observations = Arc::clone(&self.observations);
        claimed
            .resume(handler_retained, move |resume, reason| PopLiteAcceptedFuture {
                inner: handler(resume, reason),
                _accepted: ObservationGuard::accepted(observations, retained),
            })
            .await
    }

    pub(crate) fn sweep_expired(&self) -> PopLiteDeferredSweepBatch {
        PopLiteDeferredSweepBatch::from_transport(self.registry.sweep_expired(self.sweep_limit))
    }

    pub(crate) fn resource_snapshot(&self) -> PopLiteDeferredResourceSnapshot {
        PopLiteDeferredResourceSnapshot {
            admission: self.admission.snapshot(),
            index: self.index.snapshot(),
            event_reservations: self.dispatcher.reservation_snapshot(),
            active_client_gates: self.event_gate.active_count(),
            prepared_registrations: self.observations.prepared.load(Ordering::Acquire),
            pending_claims: self.observations.pending_claims.load(Ordering::Acquire),
            accepted_resumes: self.observations.accepted_resumes.load(Ordering::Acquire),
            resume_execution_count: self.observations.accepted_resumes.load(Ordering::Acquire),
            resume_execution_bytes: self.observations.resume_execution_bytes.load(Ordering::Acquire),
            pending_replays: self.pending_replays.lock().len(),
        }
    }

    pub(crate) fn shutdown(&self) -> DeferredRegistryShutdownOutcome {
        self.closed.store(true, Ordering::Release);
        self.pending_replays.lock().clear();
        self.registry.shutdown()
    }

    #[cfg(test)]
    pub(crate) fn set_replay_insert_hook(&self, checked: Arc<Barrier>, resume: Arc<Barrier>) {
        *self.replay_insert_hook.lock() = Some(ReplayInsertHook { checked, resume });
    }

    #[cfg(test)]
    pub(crate) fn set_register_after_take_hook(&self, taken: Arc<Barrier>, resume: Arc<Barrier>) {
        *self.register_after_take_hook.lock() = Some(RegisterAfterTakeHook { taken, resume });
    }

    #[cfg(test)]
    pub(crate) fn set_terminal_ready_hook(&self, ready: Arc<Barrier>, resume: Arc<Barrier>) {
        *self.terminal_ready_hook.lock() = Some(TerminalReadyHook { ready, resume });
    }

    #[cfg(test)]
    pub(crate) fn fail_next_expiry_attachment_after_take(&self) {
        self.fail_next_expiry_attachment.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn wait_register_after_take_hook(&self) {
        if let Some(hook) = self.register_after_take_hook.lock().take() {
            hook.taken.wait();
            hook.resume.wait();
        }
    }
}

#[must_use]
pub(crate) struct PopLiteEventClaim {
    claimed: ClaimedDeferred<ResumePopLite>,
    gate: PopLiteEventGateReservation,
    events: LiteEventBatchReservation,
    client_id: CheetahString,
}

struct PopLiteEventTerminal {
    events: Option<LiteEventBatchTerminal>,
    gate: Option<PopLiteEventGateReservation>,
    client_id: CheetahString,
    dispatcher: LiteEventDispatcher,
    pending_replays: Arc<Mutex<HashSet<CheetahString>>>,
    closed: Arc<AtomicBool>,
    accepted: Option<ObservationGuard>,
    #[cfg(test)]
    replay_insert_hook: Arc<Mutex<Option<ReplayInsertHook>>>,
    #[cfg(test)]
    terminal_ready_hook: Arc<Mutex<Option<TerminalReadyHook>>>,
}

impl Drop for PopLiteEventTerminal {
    fn drop(&mut self) {
        drop(self.events.take());
        drop(self.gate.take());
        observe_replay(
            &self.closed,
            &self.pending_replays,
            &self.dispatcher,
            &self.client_id,
            #[cfg(test)]
            &self.replay_insert_hook,
        );
        drop(self.accepted.take());
    }
}

fn observe_replay(
    closed: &AtomicBool,
    pending_replays: &Mutex<HashSet<CheetahString>>,
    dispatcher: &LiteEventDispatcher,
    client_id: &CheetahString,
    #[cfg(test)] replay_insert_hook: &Mutex<Option<ReplayInsertHook>>,
) -> bool {
    if closed.load(Ordering::Acquire) || dispatcher.pending_events(client_id).is_empty() {
        return false;
    }
    #[cfg(test)]
    if let Some(hook) = replay_insert_hook.lock().take() {
        hook.checked.wait();
        hook.resume.wait();
    }
    let mut pending = pending_replays.lock();
    if closed.load(Ordering::Acquire) || dispatcher.pending_events(client_id).is_empty() {
        return false;
    }
    pending.insert(client_id.clone());
    true
}

#[cfg(test)]
struct ReplayInsertHook {
    checked: Arc<Barrier>,
    resume: Arc<Barrier>,
}

#[cfg(test)]
struct RegisterAfterTakeHook {
    taken: Arc<Barrier>,
    resume: Arc<Barrier>,
}

#[cfg(test)]
struct TerminalReadyHook {
    ready: Arc<Barrier>,
    resume: Arc<Barrier>,
}

#[must_use]
struct PopLiteEventTerminalFuture<F> {
    inner: F,
    _terminal: PopLiteEventTerminal,
}

#[must_use]
struct PopLiteAcceptedFuture<F> {
    inner: F,
    _accepted: ObservationGuard,
}

impl<F> Future for PopLiteAcceptedFuture<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `inner` is structurally pinned with `self`; it is never moved
        // after the wrapper is pinned and is dropped in place with the wrapper.
        unsafe { self.as_mut().map_unchecked_mut(|future| &mut future.inner) }.poll(context)
    }
}

impl<F> Future for PopLiteEventTerminalFuture<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `inner` is structurally pinned with `self`; it is never moved
        // after the wrapper is pinned and is dropped in place with the wrapper.
        let result = unsafe { self.as_mut().map_unchecked_mut(|future| &mut future.inner) }.poll(context);
        #[cfg(test)]
        if result.is_ready() {
            if let Some(hook) = self._terminal.terminal_ready_hook.lock().take() {
                hook.ready.wait();
                hook.resume.wait();
            }
        }
        result
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PopLiteDeferredResourceSnapshot {
    pub(crate) admission: DeferredAdmissionSnapshot,
    pub(crate) index: PopLiteIndexSnapshot,
    pub(crate) event_reservations: LiteEventReservationSnapshot,
    pub(crate) active_client_gates: usize,
    pub(crate) prepared_registrations: usize,
    pub(crate) pending_claims: usize,
    pub(crate) accepted_resumes: usize,
    pub(crate) resume_execution_count: usize,
    pub(crate) resume_execution_bytes: usize,
    pub(crate) pending_replays: usize,
}

#[must_use]
pub(crate) struct PopLiteDeferredSweepBatch {
    stats: DeferredExpiryBatchStats,
    claims: Vec<ClaimedDeferred<ResumePopLite>>,
}

impl PopLiteDeferredSweepBatch {
    fn from_transport(batch: DeferredExpiryBatch<ResumePopLite>) -> Self {
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

    pub(crate) fn into_claims(self) -> Vec<ClaimedDeferred<ResumePopLite>> {
        self.claims
    }
}
