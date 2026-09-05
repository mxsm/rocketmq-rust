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
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;

use parking_lot::Mutex;

use super::expiry::EntryExpiry;
pub(super) use super::expiry::ExpiryKey;
use super::expiry::ExpiryKeyKind;
use super::ClaimMarker;
use super::ClaimStart;
use super::ClaimTicket;
use super::ClaimWaiter;
use super::ClaimedDeferred;
use super::DeferredClaimOperationalFailure;
use super::DeferredClaimRejection;
use super::DeferredExpiry;
use super::DeferredId;
use super::DeferredParts;
use super::DeferredRegistry;
use super::DeferredRegistryShutdownOutcome;
use super::DeferredRegistryShutdownStats;
use super::DeferredRequest;
use super::DeferredResponder;
use super::DeferredRetainedSizeParts;
use super::DeferredWaitPermit;
use super::DeferredWakeReason;
use super::RegistryFailure;
use super::RequestControlView;
use super::RequestId;
use super::SessionId;
use super::TicketResolution;
use crate::deadline::RequestDeadline;
use crate::dispatch::deferred_response::DeferredSystemCancellationReason;
use crate::dispatch::deferred_response::DeferredSystemCloseReason;
use crate::dispatch::deferred_session_cleanup::CleanupEnrollment;
use crate::dispatch::deferred_session_cleanup::RegistryCleanupTarget;
use crate::dispatch::deferred_session_cleanup::TargetRecord;
use crate::dispatch::ResponseStateOutcome;
use crate::dispatch::ResponseStateSnapshot;

const FIRST_DEFERRED_ID: u64 = 1;
const EXHAUSTED_DEFERRED_ID: u64 = u64::MAX;

static NEXT_DEFERRED_ID: AtomicU64 = AtomicU64::new(FIRST_DEFERRED_ID);

pub(super) trait RegistrationOwner {
    fn commit(self: Box<Self>) -> Result<(), DeferredCommitError>;

    fn rollback(self: Box<Self>);

    #[cfg(test)]
    fn set_commit_checkpoint(&mut self, checkpoint: Box<dyn FnOnce() + Send + 'static>);
}

pub(super) struct RegistrationOwnerImpl<R>
where
    R: Send + 'static,
{
    pub(super) inner: Arc<RegistryInner<R>>,
    pub(super) id: DeferredId,
    pub(super) control: RequestControlView,
    pub(super) response_state: Arc<crate::dispatch::ResponseState>,
    pub(super) expiry: Option<DeferredExpiry>,
    #[cfg(test)]
    pub(super) commit_checkpoint: Option<Box<dyn FnOnce() + Send + 'static>>,
}

impl<R> RegistrationOwner for RegistrationOwnerImpl<R>
where
    R: Send + 'static,
{
    fn commit(self: Box<Self>) -> Result<(), DeferredCommitError> {
        let inner = Arc::clone(&self.inner);
        let id = self.id;
        let control = self.control.clone();
        let response_state = Arc::clone(&self.response_state);
        #[cfg(test)]
        let commit_checkpoint = self.commit_checkpoint;
        let expiry = self.expiry;
        let mut transaction = CommitTransaction::begin(inner, id)
            .map_err(|error| commit_race_error(&control, expiry, &response_state).unwrap_or(error))?;
        if let Some(kind) = lifecycle_stop_with_expiry(transaction.request().control(), transaction.request().expiry())
        {
            transaction.request_mut().parts.cleanup_lifecycle(kind);
            return Err(DeferredCommitError::lifecycle(kind));
        }
        match transaction.request().register_response() {
            Ok(ResponseStateOutcome::Applied(())) => {}
            Ok(ResponseStateOutcome::AlreadyCompleted { .. }) => {
                return Err(commit_race_error(&control, expiry, &response_state)
                    .unwrap_or_else(DeferredCommitError::response_state));
            }
            Err(source) => {
                return Err(commit_race_error(&control, expiry, &response_state)
                    .unwrap_or_else(|| DeferredCommitError::response(source)));
            }
        }
        #[cfg(test)]
        if let Some(checkpoint) = commit_checkpoint {
            checkpoint();
        }
        if let Some(kind) = lifecycle_stop_with_expiry(transaction.request().control(), transaction.request().expiry())
        {
            transaction.request_mut().parts.cleanup_lifecycle(kind);
            return Err(DeferredCommitError::lifecycle(kind));
        }
        transaction
            .publish()
            .map_err(|error| commit_race_error(&control, expiry, &response_state).unwrap_or(error))
    }

    fn rollback(self: Box<Self>) {
        drop(self.inner.remove(self.id));
    }

    #[cfg(test)]
    fn set_commit_checkpoint(&mut self, checkpoint: Box<dyn FnOnce() + Send + 'static>) {
        self.commit_checkpoint = Some(checkpoint);
    }
}

#[cfg(test)]
pub(super) struct TestRegistrationOwner {
    pub(super) drop_probe: Option<Arc<std::sync::atomic::AtomicUsize>>,
    pub(super) commit_error: Option<DeferredCommitError>,
}

#[cfg(test)]
impl RegistrationOwner for TestRegistrationOwner {
    fn commit(mut self: Box<Self>) -> Result<(), DeferredCommitError> {
        self.commit_error.take().map_or(Ok(()), Err)
    }

    fn rollback(self: Box<Self>) {
        drop(self);
    }

    fn set_commit_checkpoint(&mut self, checkpoint: Box<dyn FnOnce() + Send + 'static>) {
        checkpoint();
    }
}

#[cfg(test)]
impl Drop for TestRegistrationOwner {
    fn drop(&mut self) {
        if let Some(probe) = &self.drop_probe {
            probe.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[derive(Debug)]
pub(crate) struct DeferredCommitError {
    kind: DeferredCommitErrorKind,
    source: Option<crate::contract::TransportContractViolation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeferredCommitErrorKind {
    ParentCancelled,
    SessionClosed,
    DeadlineExpired,
    ResponseState,
    RegistryInvariant,
}

impl DeferredCommitError {
    pub(super) const fn invariant() -> Self {
        Self {
            kind: DeferredCommitErrorKind::RegistryInvariant,
            source: None,
        }
    }

    const fn lifecycle(kind: RegistryFailure) -> Self {
        let kind = match kind {
            RegistryFailure::ParentCancelled => DeferredCommitErrorKind::ParentCancelled,
            RegistryFailure::SessionClosed => DeferredCommitErrorKind::SessionClosed,
            RegistryFailure::DeadlineExpired => DeferredCommitErrorKind::DeadlineExpired,
            RegistryFailure::DuplicateRequest
            | RegistryFailure::IdentityExhausted
            | RegistryFailure::CleanupInstallerRejected
            | RegistryFailure::RegistryInvariant => DeferredCommitErrorKind::RegistryInvariant,
        };
        Self { kind, source: None }
    }

    const fn response(source: crate::contract::TransportContractViolation) -> Self {
        Self {
            kind: DeferredCommitErrorKind::ResponseState,
            source: Some(source),
        }
    }

    const fn response_state() -> Self {
        Self {
            kind: DeferredCommitErrorKind::ResponseState,
            source: None,
        }
    }

    pub(crate) const fn category(&self) -> &'static str {
        match self.kind {
            DeferredCommitErrorKind::ParentCancelled => "parent_cancelled",
            DeferredCommitErrorKind::SessionClosed => "session_closed",
            DeferredCommitErrorKind::DeadlineExpired => "deadline_expired",
            DeferredCommitErrorKind::ResponseState => "response_state",
            DeferredCommitErrorKind::RegistryInvariant => "registry_invariant",
        }
    }

    pub(crate) const fn kind(&self) -> DeferredCommitErrorKind {
        self.kind
    }

    #[cfg(test)]
    pub(crate) const fn for_test(kind: DeferredCommitErrorKind) -> Self {
        Self { kind, source: None }
    }
}

impl fmt::Display for DeferredCommitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "deferred registration commit failed: {}", self.category())
    }
}

impl Error for DeferredCommitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_ref().map(|source| source as &(dyn Error + 'static))
    }
}

pub(in crate::dispatch) struct RegistryInner<R>
where
    R: Send + 'static,
{
    pub(super) state: Mutex<RegistryState<R>>,
    #[cfg(test)]
    test_sequence: Option<Arc<AtomicU64>>,
    #[cfg(test)]
    claim_marker_checkpoint: Mutex<Option<Box<dyn FnOnce() + Send + 'static>>>,
    #[cfg(test)]
    pub(super) sweep_claim_checkpoint: Mutex<Option<Box<dyn FnOnce() + Send + 'static>>>,
    #[cfg(test)]
    session_cleanup_calls: AtomicUsize,
}

impl<R> Default for RegistryInner<R>
where
    R: Send + 'static,
{
    fn default() -> Self {
        Self {
            state: Mutex::new(RegistryState::default()),
            #[cfg(test)]
            test_sequence: None,
            #[cfg(test)]
            claim_marker_checkpoint: Mutex::new(None),
            #[cfg(test)]
            sweep_claim_checkpoint: Mutex::new(None),
            #[cfg(test)]
            session_cleanup_calls: AtomicUsize::new(0),
        }
    }
}

pub(super) struct RegistryState<R: Send + 'static> {
    pub(super) lifecycle: RegistryLifecycle,
    pub(super) primary: HashMap<DeferredId, Entry<R>>,
    pub(super) request_index: HashMap<RequestId, DeferredId>,
    pub(super) session_index: HashMap<SessionId, HashSet<DeferredId>>,
    pub(super) claims: HashMap<DeferredId, Weak<ClaimMarker<R>>>,
    pub(super) expiry_index: BTreeSet<ExpiryKey>,
    pub(super) expiry_cursor: Option<ExpiryKey>,
}

impl<R: Send + 'static> Default for RegistryState<R> {
    fn default() -> Self {
        Self {
            lifecycle: RegistryLifecycle::Open,
            primary: HashMap::new(),
            request_index: HashMap::new(),
            session_index: HashMap::new(),
            claims: HashMap::new(),
            expiry_index: BTreeSet::new(),
            expiry_cursor: None,
        }
    }
}

fn commit_race_error(
    control: &RequestControlView,
    expiry: Option<DeferredExpiry>,
    response_state: &crate::dispatch::ResponseState,
) -> Option<DeferredCommitError> {
    lifecycle_stop_with_expiry(control, expiry)
        .map(DeferredCommitError::lifecycle)
        .or_else(|| match response_state.terminal_state() {
            Some(crate::dispatch::ResponseTerminalState::Closed) => {
                Some(DeferredCommitError::lifecycle(RegistryFailure::SessionClosed))
            }
            Some(crate::dispatch::ResponseTerminalState::Cancelled) => {
                Some(DeferredCommitError::lifecycle(RegistryFailure::ParentCancelled))
            }
            Some(
                crate::dispatch::ResponseTerminalState::Completed
                | crate::dispatch::ResponseTerminalState::Failed { .. },
            )
            | None => None,
        })
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum RegistryLifecycle {
    Open,
    Closing,
    Closed,
}

pub(super) struct Entry<R> {
    pub(super) request_id: RequestId,
    pub(super) session_id: SessionId,
    pub(super) control: RequestControlView,
    pub(super) response_state: Arc<crate::dispatch::ResponseState>,
    pub(super) enrollment: Option<CleanupEnrollment>,
    pub(super) phase: EntryPhase<R>,
    pub(super) first_reason: Option<DeferredWakeReason>,
    pub(super) claim_ticket: Weak<ClaimTicket>,
    pub(super) ticket_epoch: u64,
    pub(super) expiry: EntryExpiry,
}

pub(super) enum EntryPhase<R> {
    Shell,
    Building,
    Prepared(DeferredRequest<R>),
    Activating,
    Active(DeferredRequest<R>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EntryPhaseTag {
    Shell,
    Building,
    Prepared,
    Activating,
    Active,
}

impl<R> EntryPhase<R> {
    pub(super) const fn tag(&self) -> EntryPhaseTag {
        match self {
            Self::Shell => EntryPhaseTag::Shell,
            Self::Building => EntryPhaseTag::Building,
            Self::Prepared(_) => EntryPhaseTag::Prepared,
            Self::Activating => EntryPhaseTag::Activating,
            Self::Active(_) => EntryPhaseTag::Active,
        }
    }
}

impl<R> RegistryInner<R>
where
    R: Send + 'static,
{
    #[cfg(test)]
    pub(super) fn with_test_sequence(sequence: Arc<AtomicU64>) -> Self {
        Self {
            state: Mutex::new(RegistryState::default()),
            test_sequence: Some(sequence),
            claim_marker_checkpoint: Mutex::new(None),
            sweep_claim_checkpoint: Mutex::new(None),
            session_cleanup_calls: AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    pub(super) fn set_claim_marker_checkpoint(&self, checkpoint: Box<dyn FnOnce() + Send + 'static>) {
        *self.claim_marker_checkpoint.lock() = Some(checkpoint);
    }

    #[cfg(test)]
    pub(super) fn set_sweep_claim_checkpoint(&self, checkpoint: Box<dyn FnOnce() + Send + 'static>) {
        *self.sweep_claim_checkpoint.lock() = Some(checkpoint);
    }

    pub(super) fn insert_shell(
        &self,
        request_id: RequestId,
        session_id: SessionId,
        control: RequestControlView,
        response_state: Arc<crate::dispatch::ResponseState>,
        expiry: Option<DeferredExpiry>,
        enrollment: &mut Option<CleanupEnrollment>,
    ) -> Result<DeferredId, RegistryFailure> {
        let mut state = self.state.lock();
        if state.lifecycle != RegistryLifecycle::Open {
            return Err(RegistryFailure::ParentCancelled);
        }
        if state.request_index.contains_key(&request_id) {
            return Err(RegistryFailure::DuplicateRequest);
        }
        #[cfg(test)]
        let sequence = self.test_sequence.as_deref().unwrap_or(&NEXT_DEFERRED_ID);
        #[cfg(not(test))]
        let sequence = &NEXT_DEFERRED_ID;
        let id = reserve_deferred_id(sequence).ok_or(RegistryFailure::IdentityExhausted)?;
        let entry_expiry = EntryExpiry::new(id, &control, expiry);
        if let Some(key) = entry_expiry.scheduled {
            state.expiry_index.insert(key);
        }
        state.primary.insert(
            id,
            Entry {
                request_id,
                session_id,
                control,
                response_state,
                enrollment: enrollment.take(),
                phase: EntryPhase::Shell,
                first_reason: None,
                claim_ticket: Weak::new(),
                ticket_epoch: 0,
                expiry: entry_expiry,
            },
        );
        state.request_index.insert(request_id, id);
        state.session_index.entry(session_id).or_default().insert(id);
        Ok(id)
    }

    pub(super) fn transition_to_building(&self, id: DeferredId) -> bool {
        let mut state = self.state.lock();
        let Some(entry) = state.primary.get_mut(&id) else {
            return false;
        };
        if entry.phase.tag() != EntryPhaseTag::Shell {
            return false;
        }
        entry.phase = EntryPhase::Building;
        true
    }

    pub(super) fn store_prepared_from_shell(
        &self,
        id: DeferredId,
        request: DeferredRequest<R>,
    ) -> Result<(), Box<DeferredRequest<R>>> {
        self.store_prepared(id, EntryPhaseTag::Shell, request)
    }

    pub(super) fn store_prepared_from_building(
        &self,
        id: DeferredId,
        request: DeferredRequest<R>,
    ) -> Result<(), Box<DeferredRequest<R>>> {
        self.store_prepared(id, EntryPhaseTag::Building, request)
    }

    fn store_prepared(
        &self,
        id: DeferredId,
        expected: EntryPhaseTag,
        mut request: DeferredRequest<R>,
    ) -> Result<(), Box<DeferredRequest<R>>> {
        let mut state = self.state.lock();
        let Some(entry) = state.primary.get_mut(&id) else {
            return Err(Box::new(request));
        };
        if entry.phase.tag() != expected {
            return Err(Box::new(request));
        }
        request.parts.clear_session_cleanup();
        entry.phase = EntryPhase::Prepared(request);
        Ok(())
    }

    pub(super) fn begin_activation(&self, id: DeferredId) -> Result<DeferredRequest<R>, DeferredCommitError> {
        let mut state = self.state.lock();
        let entry = state.primary.get_mut(&id).ok_or_else(DeferredCommitError::invariant)?;
        let phase = std::mem::replace(&mut entry.phase, EntryPhase::Activating);
        match phase {
            EntryPhase::Prepared(request) => Ok(request),
            phase @ (EntryPhase::Shell | EntryPhase::Building | EntryPhase::Activating | EntryPhase::Active(_)) => {
                entry.phase = phase;
                Err(DeferredCommitError::invariant())
            }
        }
    }

    pub(super) fn publish_active(
        &self,
        id: DeferredId,
        request: DeferredRequest<R>,
    ) -> Result<(), Box<DeferredRequest<R>>> {
        let ticket = {
            let mut state = self.state.lock();
            let (ticket, schedule, old) = {
                let Some(entry) = state.primary.get_mut(&id) else {
                    return Err(Box::new(request));
                };
                if entry.phase.tag() != EntryPhaseTag::Activating {
                    return Err(Box::new(request));
                }
                if lifecycle_stop_with_expiry(&entry.control, entry.expiry.policy).is_some() {
                    return Err(Box::new(request));
                }
                entry.phase = EntryPhase::Active(request);
                let schedule = entry
                    .expiry
                    .timeout_pending
                    .then(|| {
                        entry.expiry.policy.map(|policy| ExpiryKey {
                            at: policy.protocol_at(),
                            kind: ExpiryKeyKind::LongPollTimeout,
                            id,
                        })
                    })
                    .flatten();
                let old = schedule.and_then(|key| entry.expiry.scheduled.replace(key));
                (entry.claim_ticket.upgrade(), schedule, old)
            };
            if let Some(old) = old {
                state.expiry_index.remove(&old);
            }
            if let Some(key) = schedule {
                state.expiry_index.insert(key);
            }
            ticket
        };
        if let Some(ticket) = ticket {
            ticket.publish(TicketResolution::Published);
        }
        Ok(())
    }

    pub(super) fn remove(&self, id: DeferredId) -> Option<Entry<R>> {
        let entry = {
            let mut state = self.state.lock();
            remove_entry(&mut state, id)?
        };
        if let Some(ticket) = entry.claim_ticket.upgrade() {
            ticket.publish(ticket_resolution_for_entry(&entry));
        }
        Some(entry)
    }

    pub(in crate::dispatch) fn remove_session(&self, session_id: SessionId) -> usize {
        #[cfg(test)]
        self.session_cleanup_calls.fetch_add(1, Ordering::SeqCst);
        let batch = {
            let mut state = self.state.lock();
            let Some(ids) = state.session_index.remove(&session_id) else {
                return 0;
            };
            let mut batch = DetachedBatch::default();
            for id in ids {
                if let Some(entry) = remove_entry(&mut state, id) {
                    batch.push_entry(entry, CleanupCause::SessionClosed);
                }
                if let Some(marker) = state.claims.remove(&id).and_then(|marker| marker.upgrade()) {
                    let cause = if marker.control().parent_is_cancelled() {
                        CleanupCause::ParentCancelled
                    } else {
                        CleanupCause::SessionClosed
                    };
                    batch.markers.push((marker, cause));
                }
            }
            batch
        };
        let removed_waiters = batch.waiter_count();
        let _ = batch.finish();
        removed_waiters
    }

    pub(super) fn shutdown(&self) -> DeferredRegistryShutdownOutcome {
        let batch = {
            let mut state = self.state.lock();
            match state.lifecycle {
                RegistryLifecycle::Open => state.lifecycle = RegistryLifecycle::Closing,
                RegistryLifecycle::Closing => return DeferredRegistryShutdownOutcome::InProgress,
                RegistryLifecycle::Closed => return DeferredRegistryShutdownOutcome::AlreadyClosed,
            }
            let mut batch = DetachedBatch::default();
            for (_, entry) in std::mem::take(&mut state.primary) {
                batch.push_entry(entry, CleanupCause::ParentCancelled);
            }
            for (_, marker) in std::mem::take(&mut state.claims) {
                if let Some(marker) = marker.upgrade() {
                    batch.markers.push((marker, CleanupCause::ParentCancelled));
                }
            }
            state.request_index.clear();
            state.session_index.clear();
            state.expiry_index.clear();
            state.expiry_cursor = None;
            batch
        };
        let completion = RegistryShutdownCompletion::new(self);
        let stats = batch.finish();
        completion.complete();
        DeferredRegistryShutdownOutcome::Completed(stats)
    }

    pub(super) fn start_claim(
        self: &Arc<Self>,
        id: DeferredId,
        reason: DeferredWakeReason,
        expected: Option<&ClaimWaiter>,
    ) -> ClaimStart<R> {
        let mut removed = None;
        let mut removed_cause = None;
        let mut removed_ticket = None;
        let mut removed_ticket_resolution = None;
        let outcome = {
            let mut state = self.state.lock();
            if state.lifecycle != RegistryLifecycle::Open {
                return ClaimStart::Rejected(DeferredClaimRejection::ParentCancelled);
            }
            if !state.primary.contains_key(&id) {
                let marker = state.claims.get(&id).and_then(Weak::upgrade);
                if marker.is_none() {
                    state.claims.remove(&id);
                }
                #[cfg(test)]
                if let Some(checkpoint) = self.claim_marker_checkpoint.lock().take() {
                    checkpoint();
                }
                drop(state);
                return claim_marker_outcome(id, marker);
            }
            let entry = state
                .primary
                .get_mut(&id)
                .expect("the primary entry was observed while the registry lock is held");
            if let Some(kind) = lifecycle_stop_with_expiry(&entry.control, entry.expiry.policy) {
                let entry = remove_entry(&mut state, id).expect("entry was observed while the registry lock is held");
                removed_ticket = entry.claim_ticket.upgrade();
                removed = Some(entry);
                removed_cause = Some(match kind {
                    RegistryFailure::ParentCancelled => CleanupCause::ParentCancelled,
                    RegistryFailure::SessionClosed => CleanupCause::SessionClosed,
                    RegistryFailure::DeadlineExpired => CleanupCause::OwnerDeadline,
                    RegistryFailure::DuplicateRequest
                    | RegistryFailure::IdentityExhausted
                    | RegistryFailure::CleanupInstallerRejected
                    | RegistryFailure::RegistryInvariant => CleanupCause::ParentCancelled,
                });
                ClaimStart::Rejected(claim_rejection_from_registry(kind))
            } else if matches!(entry.phase, EntryPhase::Active(_)) {
                let expected_matches = expected.is_none_or(|waiter| {
                    entry.claim_ticket.upgrade().is_some_and(|ticket| {
                        ticket.epoch() == waiter.epoch()
                            && waiter.same_ticket(&ticket)
                            && ticket.resolution() == TicketResolution::Published
                    })
                });
                if !expected_matches {
                    let entry = remove_entry(&mut state, id)
                        .expect("active entry was observed while the registry lock is held");
                    removed_ticket = entry.claim_ticket.upgrade();
                    removed_ticket_resolution = Some(TicketResolution::RemovedInvariant);
                    removed = Some(entry);
                    ClaimStart::Rejected(DeferredClaimRejection::Operational(
                        DeferredClaimOperationalFailure::invariant(),
                    ))
                } else {
                    let claim_result = match &entry.phase {
                        EntryPhase::Active(request) => request.parts.responder.claim(),
                        EntryPhase::Shell | EntryPhase::Building | EntryPhase::Prepared(_) | EntryPhase::Activating => {
                            unreachable!("the active phase was checked above")
                        }
                    };
                    match claim_result {
                        Ok(ResponseStateOutcome::Applied(())) => {
                            ClaimStart::Claimed(take_claim_locked(self, &mut state, id, reason))
                        }
                        Ok(ResponseStateOutcome::AlreadyCompleted { .. }) => {
                            removed = remove_entry(&mut state, id);
                            ClaimStart::Rejected(DeferredClaimRejection::AlreadyCompleted)
                        }
                        Err(source) => {
                            removed = remove_entry(&mut state, id);
                            ClaimStart::Rejected(DeferredClaimRejection::Operational(
                                DeferredClaimOperationalFailure::response(source),
                            ))
                        }
                    }
                }
            } else {
                entry.first_reason.get_or_insert(reason);
                let ticket = match entry
                    .claim_ticket
                    .upgrade()
                    .filter(|ticket| ticket.live_waiters() > 0 && ticket.resolution() == TicketResolution::Pending)
                {
                    Some(ticket) => Some(ticket),
                    None => match entry.ticket_epoch.checked_add(1).filter(|epoch| *epoch != 0) {
                        Some(epoch) => {
                            entry.ticket_epoch = epoch;
                            let ticket = Arc::new(ClaimTicket::new(epoch));
                            entry.claim_ticket = Arc::downgrade(&ticket);
                            Some(ticket)
                        }
                        None => {
                            let retired = remove_entry(&mut state, id)
                                .expect("entry was observed while the registry lock is held");
                            removed_ticket = retired.claim_ticket.upgrade();
                            removed_ticket_resolution = Some(TicketResolution::RemovedInvariant);
                            removed = Some(retired);
                            None
                        }
                    },
                };
                match ticket {
                    None => ClaimStart::Rejected(DeferredClaimRejection::Operational(
                        DeferredClaimOperationalFailure::invariant(),
                    )),
                    Some(ticket) => match ClaimWaiter::try_new(ticket) {
                        Ok(waiter) => ClaimStart::Wait(waiter),
                        Err(()) => {
                            let entry = remove_entry(&mut state, id)
                                .expect("entry was observed while the registry lock is held");
                            removed_ticket = entry.claim_ticket.upgrade();
                            removed_ticket_resolution = Some(TicketResolution::RemovedInvariant);
                            removed = Some(entry);
                            ClaimStart::Rejected(DeferredClaimRejection::Operational(
                                DeferredClaimOperationalFailure::invariant(),
                            ))
                        }
                    },
                }
            }
        };
        if let Some(ticket) = removed_ticket {
            let resolution = removed_ticket_resolution.unwrap_or_else(|| {
                removed
                    .as_ref()
                    .map_or(TicketResolution::RemovedInvariant, ticket_resolution_for_entry)
            });
            ticket.publish(resolution);
        }
        if let (Some(entry), Some(cause)) = (&mut removed, removed_cause) {
            let _ = entry.terminalize_response(&cause);
        }
        drop(removed);
        outcome
    }

    pub(super) fn remove_claim_marker(&self, id: DeferredId, session_id: SessionId, marker: *const ClaimMarker<R>) {
        let mut state = self.state.lock();
        let matches = state
            .claims
            .get(&id)
            .is_some_and(|current| std::ptr::eq(current.as_ptr(), marker));
        if matches {
            state.claims.remove(&id);
            remove_session_member(&mut state, session_id, id);
        }
    }

    #[cfg(test)]
    pub(super) fn index_counts(&self) -> (usize, usize, usize) {
        let state = self.state.lock();
        (
            state.primary.len(),
            state.request_index.len(),
            state.session_index.len(),
        )
    }

    #[cfg(test)]
    pub(super) fn phase(&self, id: DeferredId) -> Option<EntryPhaseTag> {
        self.state.lock().primary.get(&id).map(|entry| entry.phase.tag())
    }

    #[cfg(test)]
    pub(super) fn contains(&self, id: DeferredId) -> bool {
        self.state.lock().primary.contains_key(&id)
    }

    #[cfg(test)]
    pub(super) fn is_active(&self, id: DeferredId) -> bool {
        self.state
            .lock()
            .primary
            .get(&id)
            .is_some_and(|entry| matches!(entry.phase, EntryPhase::Active(_)))
    }

    #[cfg(test)]
    pub(super) fn claim_marker_count(&self) -> usize {
        self.state.lock().claims.len()
    }

    #[cfg(test)]
    pub(super) fn session_member_count(&self, session_id: SessionId) -> usize {
        self.state.lock().session_index.get(&session_id).map_or(0, HashSet::len)
    }

    #[cfg(test)]
    pub(super) fn session_cleanup_call_count(&self) -> usize {
        self.session_cleanup_calls.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(super) fn ticket_epoch(&self, id: DeferredId) -> Option<u64> {
        self.state.lock().primary.get(&id).map(|entry| entry.ticket_epoch)
    }

    #[cfg(test)]
    pub(super) fn set_ticket_epoch(&self, id: DeferredId, epoch: u64) {
        self.state.lock().primary.get_mut(&id).expect("test entry").ticket_epoch = epoch;
    }

    #[cfg(test)]
    pub(super) fn install_claim_ticket(&self, id: DeferredId, epoch: u64, live_waiters: usize) -> Arc<ClaimTicket> {
        let ticket = Arc::new(ClaimTicket::new(epoch));
        ticket.set_live_waiters(live_waiters);
        let mut state = self.state.lock();
        let entry = state.primary.get_mut(&id).expect("test entry");
        entry.ticket_epoch = epoch;
        entry.claim_ticket = Arc::downgrade(&ticket);
        ticket
    }
}

pub(super) fn remove_entry<R: Send + 'static>(state: &mut RegistryState<R>, id: DeferredId) -> Option<Entry<R>> {
    let mut entry = state.primary.remove(&id)?;
    if let Some(key) = entry.expiry.scheduled.take() {
        state.expiry_index.remove(&key);
    }
    if state.request_index.get(&entry.request_id) == Some(&id) {
        state.request_index.remove(&entry.request_id);
    }
    let remove_session = state.session_index.get_mut(&entry.session_id).is_some_and(|ids| {
        ids.remove(&id);
        ids.is_empty()
    });
    if remove_session {
        state.session_index.remove(&entry.session_id);
    }
    Some(entry)
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum CleanupCause {
    SessionClosed,
    ParentCancelled,
    OwnerDeadline,
}

struct RegistryShutdownCompletion<'a, R>
where
    R: Send + 'static,
{
    registry: &'a RegistryInner<R>,
    armed: bool,
}

impl<'a, R> RegistryShutdownCompletion<'a, R>
where
    R: Send + 'static,
{
    fn new(registry: &'a RegistryInner<R>) -> Self {
        Self { registry, armed: true }
    }

    fn complete(mut self) {
        self.registry.state.lock().lifecycle = RegistryLifecycle::Closed;
        self.armed = false;
    }
}

impl<R> Drop for RegistryShutdownCompletion<'_, R>
where
    R: Send + 'static,
{
    fn drop(&mut self) {
        if self.armed {
            self.registry.state.lock().lifecycle = RegistryLifecycle::Closed;
            self.armed = false;
        }
    }
}

pub(super) struct DetachedBatch<R>
where
    R: Send + 'static,
{
    entries: Vec<(Entry<R>, CleanupCause)>,
    tickets: Vec<(Arc<ClaimTicket>, TicketResolution)>,
    markers: Vec<(Arc<ClaimMarker<R>>, CleanupCause)>,
}

impl<R> Default for DetachedBatch<R>
where
    R: Send + 'static,
{
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            tickets: Vec::new(),
            markers: Vec::new(),
        }
    }
}

impl<R> DetachedBatch<R>
where
    R: Send + 'static,
{
    fn waiter_count(&self) -> usize {
        self.entries.len().saturating_add(self.markers.len())
    }

    pub(super) fn push_entry(&mut self, entry: Entry<R>, fallback: CleanupCause) {
        let cause = match fallback {
            CleanupCause::SessionClosed if entry.control.parent_is_cancelled() => CleanupCause::ParentCancelled,
            cause => cause,
        };
        if let Some(ticket) = entry
            .claim_ticket
            .upgrade()
            .filter(|ticket| ticket.resolution() == TicketResolution::Pending)
        {
            let resolution = match cause {
                CleanupCause::SessionClosed => TicketResolution::RemovedSessionClosed,
                CleanupCause::ParentCancelled => TicketResolution::RemovedParentCancelled,
                CleanupCause::OwnerDeadline => TicketResolution::RemovedDeadlineExpired,
            };
            self.tickets.push((ticket, resolution));
        }
        self.entries.push((entry, cause));
    }

    pub(super) fn finish(mut self) -> DeferredRegistryShutdownStats {
        let mut stats = DeferredRegistryShutdownStats::default();
        for (ticket, resolution) in self.tickets.drain(..) {
            ticket.publish(resolution);
            stats.record_ticket();
        }
        for (entry, entry_cause) in &mut self.entries {
            stats.record_detached_entry();
            let result = entry.terminalize_response(entry_cause);
            record_terminalization(&mut stats, result, entry.response_state.snapshot());
        }
        for (marker, marker_cause) in &self.markers {
            let result = match marker_cause {
                CleanupCause::SessionClosed => marker.close_session_response(),
                CleanupCause::ParentCancelled => marker.cancel_parent_response(),
                CleanupCause::OwnerDeadline => marker.cancel_owner_response(),
            };
            record_state_terminalization(&mut stats, result, marker.response_snapshot());
        }
        drop(self.markers);
        drop(self.entries);
        stats
    }
}

impl<R> Entry<R> {
    fn terminalize_response(
        &mut self,
        cause: &CleanupCause,
    ) -> Result<ResponseStateOutcome, crate::contract::TransportContractViolation> {
        match &mut self.phase {
            EntryPhase::Prepared(request) | EntryPhase::Active(request) => match cause {
                CleanupCause::SessionClosed => request
                    .parts
                    .responder
                    .cleanup_close_with_reason(DeferredSystemCloseReason::SESSION_CLOSED),
                CleanupCause::ParentCancelled => request
                    .parts
                    .responder
                    .cleanup_cancel_with_reason(DeferredSystemCancellationReason::PARENT_CANCELLED),
                CleanupCause::OwnerDeadline => request
                    .parts
                    .responder
                    .cleanup_cancel_with_reason(DeferredSystemCancellationReason::OWNER_DEADLINE),
            },
            EntryPhase::Shell | EntryPhase::Building | EntryPhase::Activating => match cause {
                CleanupCause::SessionClosed => self
                    .response_state
                    .close_with_reason(DeferredSystemCloseReason::SESSION_CLOSED),
                CleanupCause::ParentCancelled => self
                    .response_state
                    .cancel_with_reason(DeferredSystemCancellationReason::PARENT_CANCELLED),
                CleanupCause::OwnerDeadline => self
                    .response_state
                    .cancel_with_reason(DeferredSystemCancellationReason::OWNER_DEADLINE),
            },
        }
    }
}

fn record_terminalization(
    stats: &mut DeferredRegistryShutdownStats,
    result: Result<ResponseStateOutcome, crate::contract::TransportContractViolation>,
    snapshot: ResponseStateSnapshot,
) {
    match result {
        Ok(ResponseStateOutcome::Applied(())) => stats.record_terminalized(),
        Ok(ResponseStateOutcome::AlreadyCompleted { .. }) => {}
        Err(_) if snapshot == ResponseStateSnapshot::Sending => stats.record_in_progress(),
        Err(_) => stats.record_invariant_failure(),
    }
}

fn record_state_terminalization(
    stats: &mut DeferredRegistryShutdownStats,
    result: Result<ResponseStateOutcome, crate::contract::TransportContractViolation>,
    snapshot: ResponseStateSnapshot,
) {
    match result {
        Ok(ResponseStateOutcome::Applied(())) => stats.record_terminalized(),
        Ok(ResponseStateOutcome::AlreadyCompleted { .. }) => {}
        Err(_) if snapshot == ResponseStateSnapshot::Sending => stats.record_in_progress(),
        Err(_) => stats.record_invariant_failure(),
    }
}

fn remove_entry_for_claim<R: Send + 'static>(state: &mut RegistryState<R>, id: DeferredId) -> Option<Entry<R>> {
    let mut entry = state.primary.remove(&id)?;
    if let Some(key) = entry.expiry.scheduled.take() {
        state.expiry_index.remove(&key);
    }
    if state.request_index.get(&entry.request_id) == Some(&id) {
        state.request_index.remove(&entry.request_id);
    }
    Some(entry)
}

fn remove_session_member<R: Send + 'static>(state: &mut RegistryState<R>, session_id: SessionId, id: DeferredId) {
    let remove_session = state.session_index.get_mut(&session_id).is_some_and(|ids| {
        ids.remove(&id);
        ids.is_empty()
    });
    if remove_session {
        state.session_index.remove(&session_id);
    }
}

fn claim_marker_outcome<R>(_id: DeferredId, marker: Option<Arc<ClaimMarker<R>>>) -> ClaimStart<R>
where
    R: Send + 'static,
{
    let Some(marker) = marker else {
        return ClaimStart::Rejected(DeferredClaimRejection::NotFound);
    };
    let terminal = marker.terminal_state();
    ClaimStart::Rejected(if terminal.is_some() {
        DeferredClaimRejection::AlreadyCompleted
    } else {
        DeferredClaimRejection::AlreadyClaimed
    })
}

fn claim_rejection_from_registry(kind: RegistryFailure) -> DeferredClaimRejection {
    match kind {
        RegistryFailure::ParentCancelled => DeferredClaimRejection::ParentCancelled,
        RegistryFailure::SessionClosed => DeferredClaimRejection::SessionClosed,
        RegistryFailure::DeadlineExpired => DeferredClaimRejection::DeadlineExpired,
        RegistryFailure::DuplicateRequest
        | RegistryFailure::IdentityExhausted
        | RegistryFailure::CleanupInstallerRejected
        | RegistryFailure::RegistryInvariant => {
            DeferredClaimRejection::Operational(DeferredClaimOperationalFailure::invariant())
        }
    }
}

fn ticket_resolution_for_entry<R>(entry: &Entry<R>) -> TicketResolution {
    match lifecycle_stop_with_expiry(&entry.control, entry.expiry.policy) {
        Some(RegistryFailure::ParentCancelled) => TicketResolution::RemovedParentCancelled,
        Some(RegistryFailure::SessionClosed) => TicketResolution::RemovedSessionClosed,
        Some(RegistryFailure::DeadlineExpired) => TicketResolution::RemovedDeadlineExpired,
        Some(
            RegistryFailure::DuplicateRequest
            | RegistryFailure::IdentityExhausted
            | RegistryFailure::CleanupInstallerRejected
            | RegistryFailure::RegistryInvariant,
        ) => TicketResolution::RemovedInvariant,
        None => TicketResolution::RemovedNotFound,
    }
}

pub(super) struct BuildTransaction<R>
where
    R: Send + 'static,
{
    inner: Arc<RegistryInner<R>>,
    id: DeferredId,
    parts: Option<DeferredParts>,
    active: bool,
}

impl<R> BuildTransaction<R>
where
    R: Send + 'static,
{
    pub(super) const fn new(inner: Arc<RegistryInner<R>>, id: DeferredId, parts: DeferredParts) -> Self {
        Self {
            inner,
            id,
            parts: Some(parts),
            active: true,
        }
    }

    pub(super) fn parts(&self) -> &DeferredParts {
        self.parts
            .as_ref()
            .expect("active build transaction owns deferred parts")
    }

    pub(super) fn take_parts(&mut self) -> DeferredParts {
        self.parts.take().expect("active build transaction owns deferred parts")
    }

    pub(super) fn rollback(mut self) -> DeferredParts {
        let removed = self.inner.remove(self.id);
        self.active = false;
        let parts = self.take_parts();
        drop(removed);
        parts
    }

    pub(super) fn disarm(&mut self) {
        self.active = false;
    }

    pub(super) fn disarm_and_remove(&mut self) {
        let removed = self.inner.remove(self.id);
        self.active = false;
        drop(removed);
    }
}

impl<R> Drop for BuildTransaction<R>
where
    R: Send + 'static,
{
    fn drop(&mut self) {
        if self.active {
            let removed = self.inner.remove(self.id);
            self.active = false;
            drop(removed);
        }
    }
}

struct CommitTransaction<R>
where
    R: Send + 'static,
{
    inner: Arc<RegistryInner<R>>,
    id: DeferredId,
    request: Option<DeferredRequest<R>>,
    active: bool,
}

impl<R> CommitTransaction<R>
where
    R: Send + 'static,
{
    fn begin(inner: Arc<RegistryInner<R>>, id: DeferredId) -> Result<Self, DeferredCommitError> {
        let request = inner.begin_activation(id)?;
        Ok(Self {
            inner,
            id,
            request: Some(request),
            active: true,
        })
    }

    fn request(&self) -> &DeferredRequest<R> {
        self.request
            .as_ref()
            .expect("active commit transaction owns deferred request")
    }

    fn request_mut(&mut self) -> &mut DeferredRequest<R> {
        self.request
            .as_mut()
            .expect("active commit transaction owns deferred request")
    }

    fn publish(&mut self) -> Result<(), DeferredCommitError> {
        let request = self.request.take().ok_or_else(DeferredCommitError::invariant)?;
        match self.inner.publish_active(self.id, request) {
            Ok(()) => {
                self.active = false;
                Ok(())
            }
            Err(request) => {
                self.request = Some(*request);
                if let Some(kind) = lifecycle_stop_with_expiry(self.request().control(), self.request().expiry()) {
                    self.request_mut().parts.cleanup_lifecycle(kind);
                    Err(DeferredCommitError::lifecycle(kind))
                } else {
                    Err(DeferredCommitError::invariant())
                }
            }
        }
    }
}

impl<R> Drop for CommitTransaction<R>
where
    R: Send + 'static,
{
    fn drop(&mut self) {
        if self.active {
            let removed = self.inner.remove(self.id);
            self.active = false;
            drop(removed);
        }
    }
}

pub(super) fn reserve_deferred_id(sequence: &AtomicU64) -> Option<DeferredId> {
    sequence
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| match current {
            0 | EXHAUSTED_DEFERRED_ID => None,
            value if value == EXHAUSTED_DEFERRED_ID - 1 => Some(EXHAUSTED_DEFERRED_ID),
            value => Some(value + 1),
        })
        .ok()
        .map(DeferredId)
}

pub(super) fn validate_retained_floor<R>(
    retained_bytes: usize,
) -> Result<(), crate::contract::TransportContractViolation>
where
    R: Send + 'static,
{
    let required = DeferredRegistry::<R>::try_retained_size(DeferredRetainedSizeParts::new(0))
        .map_err(|_| crate::contract::TransportContractViolation::DeferredRetainedSizeOverflow)?;
    if retained_bytes < required.bytes() {
        Err(crate::contract::TransportContractViolation::DeferredRetainedSizeUnderreported)
    } else {
        Ok(())
    }
}

pub(super) fn registry_additional_bytes<R>() -> Option<usize>
where
    R: Send + 'static,
{
    let claim_runtime = checked_claim_runtime_sum(
        arc_allocation_bytes::<ClaimTicket>()?,
        arc_allocation_bytes::<ClaimMarker<R>>()?,
        Layout::new::<(DeferredId, Weak<ClaimMarker<R>>)>().size(),
        crate::dispatch::deferred_resume::deferred_resume_fixed_bytes()?,
    )?;
    checked_registry_with_expiry_bytes(
        checked_registry_layout_bytes(RegistryLayoutSizes {
            inline_resume: Layout::new::<R>().size(),
            primary_entry: Layout::new::<(DeferredId, Entry<R>)>().size(),
            responder: Layout::new::<DeferredResponder>().size(),
            permit: Layout::new::<DeferredWaitPermit>().size(),
            request_index: Layout::new::<(RequestId, DeferredId)>().size(),
            session_owner: Layout::new::<(SessionId, HashSet<DeferredId>)>().size(),
            session_member: Layout::new::<DeferredId>().size(),
            cleanup_target: arc_allocation_bytes::<RegistryCleanupTarget<R>>()?,
            cleanup_target_record: Layout::new::<(usize, TargetRecord)>().size(),
            claim_runtime,
        })?,
        Layout::new::<ExpiryKey>().size(),
    )
}

pub(super) fn checked_registry_with_expiry_bytes(base: usize, expiry_index: usize) -> Option<usize> {
    base.checked_add(expiry_index)
}

#[derive(Clone, Copy)]
pub(super) struct RegistryLayoutSizes {
    pub(super) inline_resume: usize,
    pub(super) primary_entry: usize,
    pub(super) responder: usize,
    pub(super) permit: usize,
    pub(super) request_index: usize,
    pub(super) session_owner: usize,
    pub(super) session_member: usize,
    pub(super) cleanup_target: usize,
    pub(super) cleanup_target_record: usize,
    pub(super) claim_runtime: usize,
}

pub(super) fn checked_registry_layout_bytes(sizes: RegistryLayoutSizes) -> Option<usize> {
    let primary_payload = sizes
        .inline_resume
        .checked_add(sizes.responder)?
        .checked_add(sizes.permit)?;
    let primary_net = sizes.primary_entry.checked_sub(primary_payload)?;
    let session = sizes.session_owner.checked_add(sizes.session_member)?;
    let cleanup = sizes.cleanup_target.checked_add(sizes.cleanup_target_record)?;
    checked_registry_component_sum(
        sizes.inline_resume,
        primary_net,
        sizes.request_index,
        session,
        cleanup,
        sizes.claim_runtime,
    )
}

pub(super) fn checked_registry_component_sum(
    inline_resume: usize,
    primary_net: usize,
    request_index: usize,
    session: usize,
    cleanup: usize,
    claim_runtime: usize,
) -> Option<usize> {
    inline_resume
        .checked_add(primary_net)?
        .checked_add(request_index)?
        .checked_add(session)?
        .checked_add(cleanup)?
        .checked_add(claim_runtime)
}

fn arc_allocation_bytes<T>() -> Option<usize> {
    let header = Layout::array::<AtomicUsize>(2).ok()?;
    let (allocation, _) = header.extend(Layout::new::<T>()).ok()?;
    Some(allocation.pad_to_align().size())
}

pub(super) fn checked_claim_runtime_sum(
    ticket: usize,
    marker: usize,
    claim_slot: usize,
    resume: usize,
) -> Option<usize> {
    ticket.checked_add(marker)?.checked_add(claim_slot)?.checked_add(resume)
}

fn take_claim_locked<R>(
    registry: &Arc<RegistryInner<R>>,
    state: &mut RegistryState<R>,
    id: DeferredId,
    fallback_reason: DeferredWakeReason,
) -> ClaimedDeferred<R>
where
    R: Send + 'static,
{
    let mut entry = remove_entry_for_claim(state, id).expect("claimed entry remains indexed");
    let request_id = entry.request_id;
    let request = match entry.phase {
        EntryPhase::Active(request) => request,
        EntryPhase::Shell | EntryPhase::Building | EntryPhase::Prepared(_) | EntryPhase::Activating => {
            unreachable!("the response claim transition requires an active entry")
        }
    };
    let marker = Arc::new(ClaimMarker::new(
        registry,
        id,
        entry.session_id,
        entry.control.clone(),
        Arc::clone(request.parts.responder.response_state()),
        request.parts.expiry(),
        entry.enrollment.take(),
    ));
    state.claims.insert(id, Arc::downgrade(&marker));
    ClaimedDeferred::new(
        id,
        request_id,
        entry.first_reason.unwrap_or(fallback_reason),
        request,
        marker,
    )
}

pub(super) fn lifecycle_stop_with_expiry(
    control: &RequestControlView,
    expiry: Option<DeferredExpiry>,
) -> Option<RegistryFailure> {
    if control.parent_is_cancelled() {
        Some(RegistryFailure::ParentCancelled)
    } else if control.session_is_closed() {
        Some(RegistryFailure::SessionClosed)
    } else if expiry
        .and_then(DeferredExpiry::resume_cutoff)
        .is_some_and(|cutoff| tokio::time::Instant::now() >= cutoff)
        || expiry.is_none() && control.deadline().is_some_and(RequestDeadline::is_expired)
    {
        Some(RegistryFailure::DeadlineExpired)
    } else {
        None
    }
}
