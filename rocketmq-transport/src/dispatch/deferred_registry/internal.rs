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

use super::ClaimMarker;
use super::ClaimStart;
use super::ClaimTicket;
use super::ClaimWaiter;
use super::ClaimedDeferred;
use super::DeferredClaimError;
use super::DeferredClaimErrorKind;
use super::DeferredId;
use super::DeferredParts;
use super::DeferredRegistry;
use super::DeferredRegistryErrorKind;
use super::DeferredRegistryShutdownOutcome;
use super::DeferredRegistryShutdownStats;
use super::DeferredRequest;
use super::DeferredResponder;
use super::DeferredResponseError;
use super::DeferredRetainedSizeParts;
use super::DeferredWaitPermit;
use super::DeferredWakeReason;
use super::RequestControlView;
use super::RequestId;
use super::SessionId;
use super::TicketResolution;
use crate::deadline::RequestDeadline;
use crate::dispatch::deferred_session_cleanup::CleanupEnrollment;
use crate::dispatch::deferred_session_cleanup::RegistryCleanupTarget;
use crate::dispatch::deferred_session_cleanup::TargetRecord;
use crate::dispatch::DeferredResponseErrorKind;
use crate::dispatch::ResponseStateError;
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
        let mut transaction = CommitTransaction::begin(inner, id)
            .map_err(|error| commit_race_error(&control, &response_state).unwrap_or(error))?;
        if let Some(kind) = lifecycle_stop(transaction.request().control()) {
            return Err(DeferredCommitError::lifecycle(kind));
        }
        transaction.request().register_response().map_err(|source| {
            commit_race_error(&control, &response_state).unwrap_or_else(|| DeferredCommitError::response(source))
        })?;
        #[cfg(test)]
        if let Some(checkpoint) = commit_checkpoint {
            checkpoint();
        }
        if let Some(kind) = lifecycle_stop(transaction.request().control()) {
            return Err(DeferredCommitError::lifecycle(kind));
        }
        transaction
            .publish()
            .map_err(|error| commit_race_error(&control, &response_state).unwrap_or(error))
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
}

#[cfg(test)]
impl RegistrationOwner for TestRegistrationOwner {
    fn commit(self: Box<Self>) -> Result<(), DeferredCommitError> {
        drop(self);
        Ok(())
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
    source: Option<DeferredResponseError>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeferredCommitErrorKind {
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

    const fn lifecycle(kind: DeferredRegistryErrorKind) -> Self {
        let kind = match kind {
            DeferredRegistryErrorKind::ParentCancelled => DeferredCommitErrorKind::ParentCancelled,
            DeferredRegistryErrorKind::SessionClosed => DeferredCommitErrorKind::SessionClosed,
            DeferredRegistryErrorKind::DeadlineExpired => DeferredCommitErrorKind::DeadlineExpired,
            DeferredRegistryErrorKind::RetainedSizeOverflow
            | DeferredRegistryErrorKind::RetainedSizeUnderreported
            | DeferredRegistryErrorKind::DuplicateRequest
            | DeferredRegistryErrorKind::IdentityExhausted
            | DeferredRegistryErrorKind::Builder
            | DeferredRegistryErrorKind::RegistryInvariant => DeferredCommitErrorKind::RegistryInvariant,
        };
        Self { kind, source: None }
    }

    const fn response(source: DeferredResponseError) -> Self {
        Self {
            kind: DeferredCommitErrorKind::ResponseState,
            source: Some(source),
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
    state: Mutex<RegistryState<R>>,
    #[cfg(test)]
    test_sequence: Option<Arc<AtomicU64>>,
    #[cfg(test)]
    claim_marker_checkpoint: Mutex<Option<Box<dyn FnOnce() + Send + 'static>>>,
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
            session_cleanup_calls: AtomicUsize::new(0),
        }
    }
}

struct RegistryState<R: Send + 'static> {
    lifecycle: RegistryLifecycle,
    primary: HashMap<DeferredId, Entry<R>>,
    request_index: HashMap<RequestId, DeferredId>,
    session_index: HashMap<SessionId, HashSet<DeferredId>>,
    claims: HashMap<DeferredId, Weak<ClaimMarker<R>>>,
}

impl<R: Send + 'static> Default for RegistryState<R> {
    fn default() -> Self {
        Self {
            lifecycle: RegistryLifecycle::Open,
            primary: HashMap::new(),
            request_index: HashMap::new(),
            session_index: HashMap::new(),
            claims: HashMap::new(),
        }
    }
}

fn commit_race_error(
    control: &RequestControlView,
    response_state: &crate::dispatch::ResponseState,
) -> Option<DeferredCommitError> {
    lifecycle_stop(control)
        .map(DeferredCommitError::lifecycle)
        .or_else(|| match response_state.terminal_state() {
            Some(crate::dispatch::ResponseTerminalState::Closed) => {
                Some(DeferredCommitError::lifecycle(DeferredRegistryErrorKind::SessionClosed))
            }
            Some(crate::dispatch::ResponseTerminalState::Cancelled) => Some(DeferredCommitError::lifecycle(
                DeferredRegistryErrorKind::ParentCancelled,
            )),
            Some(
                crate::dispatch::ResponseTerminalState::Completed
                | crate::dispatch::ResponseTerminalState::Failed { .. },
            )
            | None => None,
        })
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum RegistryLifecycle {
    Open,
    Closing,
    Closed,
}

pub(super) struct Entry<R> {
    request_id: RequestId,
    session_id: SessionId,
    control: RequestControlView,
    response_state: Arc<crate::dispatch::ResponseState>,
    enrollment: Option<CleanupEnrollment>,
    phase: EntryPhase<R>,
    first_reason: Option<DeferredWakeReason>,
    claim_ticket: Weak<ClaimTicket>,
    ticket_epoch: u64,
}

enum EntryPhase<R> {
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
    const fn tag(&self) -> EntryPhaseTag {
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
            session_cleanup_calls: AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    pub(super) fn set_claim_marker_checkpoint(&self, checkpoint: Box<dyn FnOnce() + Send + 'static>) {
        *self.claim_marker_checkpoint.lock() = Some(checkpoint);
    }

    pub(super) fn insert_shell(
        &self,
        request_id: RequestId,
        session_id: SessionId,
        control: RequestControlView,
        response_state: Arc<crate::dispatch::ResponseState>,
        enrollment: &mut Option<CleanupEnrollment>,
    ) -> Result<DeferredId, DeferredRegistryErrorKind> {
        let mut state = self.state.lock();
        if state.lifecycle != RegistryLifecycle::Open {
            return Err(DeferredRegistryErrorKind::ParentCancelled);
        }
        if state.request_index.contains_key(&request_id) {
            return Err(DeferredRegistryErrorKind::DuplicateRequest);
        }
        #[cfg(test)]
        let sequence = self.test_sequence.as_deref().unwrap_or(&NEXT_DEFERRED_ID);
        #[cfg(not(test))]
        let sequence = &NEXT_DEFERRED_ID;
        let id = reserve_deferred_id(sequence).ok_or(DeferredRegistryErrorKind::IdentityExhausted)?;
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
        request: DeferredRequest<R>,
    ) -> Result<(), Box<DeferredRequest<R>>> {
        let mut state = self.state.lock();
        let Some(entry) = state.primary.get_mut(&id) else {
            return Err(Box::new(request));
        };
        if entry.phase.tag() != expected {
            return Err(Box::new(request));
        }
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
            let Some(entry) = state.primary.get_mut(&id) else {
                return Err(Box::new(request));
            };
            if entry.phase.tag() != EntryPhaseTag::Activating {
                return Err(Box::new(request));
            }
            entry.phase = EntryPhase::Active(request);
            entry.claim_ticket.upgrade()
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

    pub(in crate::dispatch) fn remove_session(&self, session_id: SessionId) {
        #[cfg(test)]
        self.session_cleanup_calls.fetch_add(1, Ordering::SeqCst);
        let batch = {
            let mut state = self.state.lock();
            let Some(ids) = state.session_index.remove(&session_id) else {
                return;
            };
            let mut batch = DetachedBatch::default();
            for id in ids {
                if let Some(entry) = state.primary.remove(&id) {
                    if state.request_index.get(&entry.request_id) == Some(&id) {
                        state.request_index.remove(&entry.request_id);
                    }
                    batch.push_entry(entry, CleanupCause::SessionClosed);
                }
                if let Some(marker) = state.claims.remove(&id).and_then(|marker| marker.upgrade()) {
                    batch.markers.push(marker);
                }
            }
            batch
        };
        let _ = batch.finish(CleanupCause::SessionClosed);
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
                    batch.markers.push(marker);
                }
            }
            state.request_index.clear();
            state.session_index.clear();
            batch
        };
        let completion = RegistryShutdownCompletion::new(self);
        let stats = batch.finish(CleanupCause::ParentCancelled);
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
        let mut removed_ticket = None;
        let mut removed_ticket_resolution = None;
        let outcome = {
            let mut state = self.state.lock();
            if state.lifecycle != RegistryLifecycle::Open {
                return ClaimStart::Error(DeferredClaimError::new(
                    DeferredClaimErrorKind::ParentCancelled,
                    id,
                    None,
                    None,
                    None,
                ));
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
            let request_id = entry.request_id;
            if let Some(kind) = lifecycle_stop(&entry.control) {
                let entry = remove_entry(&mut state, id).expect("entry was observed while the registry lock is held");
                removed_ticket = entry.claim_ticket.upgrade();
                removed = Some(entry);
                ClaimStart::Error(DeferredClaimError::new(
                    claim_kind_from_registry(kind),
                    id,
                    Some(request_id),
                    None,
                    None,
                ))
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
                    ClaimStart::Error(DeferredClaimError::new(
                        DeferredClaimErrorKind::RegistryInvariant,
                        id,
                        Some(request_id),
                        None,
                        None,
                    ))
                } else {
                    let claim_result = match &entry.phase {
                        EntryPhase::Active(request) => request.parts.responder.claim(),
                        EntryPhase::Shell | EntryPhase::Building | EntryPhase::Prepared(_) | EntryPhase::Activating => {
                            unreachable!("the active phase was checked above")
                        }
                    };
                    match claim_result {
                        Ok(()) => {
                            let mut entry = remove_entry_for_claim(&mut state, id)
                                .expect("active entry was observed while the registry lock is held");
                            let request = match entry.phase {
                                EntryPhase::Active(request) => request,
                                EntryPhase::Shell
                                | EntryPhase::Building
                                | EntryPhase::Prepared(_)
                                | EntryPhase::Activating => unreachable!("the active phase was checked above"),
                            };
                            let first_reason = entry.first_reason.unwrap_or(reason);
                            let marker = Arc::new(ClaimMarker::new(
                                self,
                                id,
                                request_id,
                                entry.session_id,
                                entry.control.clone(),
                                Arc::clone(request.parts.responder.response_state()),
                                entry.enrollment.take(),
                            ));
                            state.claims.insert(id, Arc::downgrade(&marker));
                            ClaimStart::Claimed(ClaimedDeferred::new(id, request_id, first_reason, request, marker))
                        }
                        Err(source) => {
                            let terminal = source.prior_terminal_state();
                            let kind = match source.kind() {
                                DeferredResponseErrorKind::AlreadyCompleted => DeferredClaimErrorKind::AlreadyCompleted,
                                DeferredResponseErrorKind::InvalidTransition
                                | DeferredResponseErrorKind::Binding
                                | DeferredResponseErrorKind::DeadlineExceeded
                                | DeferredResponseErrorKind::Cancelled
                                | DeferredResponseErrorKind::SessionClosed
                                | DeferredResponseErrorKind::QueueSaturated
                                | DeferredResponseErrorKind::Encode
                                | DeferredResponseErrorKind::Transport => DeferredClaimErrorKind::RegistryInvariant,
                            };
                            removed = remove_entry(&mut state, id);
                            ClaimStart::Error(DeferredClaimError::new(
                                kind,
                                id,
                                Some(request_id),
                                terminal,
                                Some(source),
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
                    None => ClaimStart::Error(DeferredClaimError::new(
                        DeferredClaimErrorKind::RegistryInvariant,
                        id,
                        Some(request_id),
                        None,
                        None,
                    )),
                    Some(ticket) => match ClaimWaiter::try_new(ticket, request_id) {
                        Ok(waiter) => ClaimStart::Wait(waiter),
                        Err(()) => {
                            let entry = remove_entry(&mut state, id)
                                .expect("entry was observed while the registry lock is held");
                            removed_ticket = entry.claim_ticket.upgrade();
                            removed_ticket_resolution = Some(TicketResolution::RemovedInvariant);
                            removed = Some(entry);
                            ClaimStart::Error(DeferredClaimError::new(
                                DeferredClaimErrorKind::RegistryInvariant,
                                id,
                                Some(request_id),
                                None,
                                None,
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

fn remove_entry<R: Send + 'static>(state: &mut RegistryState<R>, id: DeferredId) -> Option<Entry<R>> {
    let entry = state.primary.remove(&id)?;
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

#[derive(Clone, Copy)]
enum CleanupCause {
    SessionClosed,
    ParentCancelled,
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

struct DetachedBatch<R>
where
    R: Send + 'static,
{
    entries: Vec<(Entry<R>, CleanupCause)>,
    tickets: Vec<(Arc<ClaimTicket>, TicketResolution)>,
    markers: Vec<Arc<ClaimMarker<R>>>,
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
    fn push_entry(&mut self, entry: Entry<R>, fallback: CleanupCause) {
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
            };
            self.tickets.push((ticket, resolution));
        }
        self.entries.push((entry, cause));
    }

    fn finish(mut self, cause: CleanupCause) -> DeferredRegistryShutdownStats {
        let mut stats = DeferredRegistryShutdownStats::default();
        for (ticket, resolution) in self.tickets.drain(..) {
            ticket.publish(resolution);
            stats.record_ticket();
        }
        for (entry, entry_cause) in &mut self.entries {
            stats.record_detached_entry();
            record_terminalization(&mut stats, entry.terminalize_response(entry_cause));
        }
        for marker in &self.markers {
            let result = match cause {
                CleanupCause::SessionClosed if marker.control().parent_is_cancelled() => marker.cancel_response(),
                CleanupCause::SessionClosed => marker.close_response(),
                CleanupCause::ParentCancelled => marker.cancel_response(),
            };
            record_state_terminalization(&mut stats, result);
        }
        drop(self.markers);
        drop(self.entries);
        stats
    }
}

impl<R> Entry<R> {
    fn terminalize_response(&mut self, cause: &CleanupCause) -> Result<(), DeferredResponseError> {
        match &mut self.phase {
            EntryPhase::Prepared(request) | EntryPhase::Active(request) => match cause {
                CleanupCause::SessionClosed => request.parts.responder.cleanup_terminalize(),
                CleanupCause::ParentCancelled => request.parts.responder.cleanup_cancel(),
            },
            EntryPhase::Shell | EntryPhase::Building | EntryPhase::Activating => match cause {
                CleanupCause::SessionClosed => self.response_state.close().map_err(DeferredResponseError::from_state),
                CleanupCause::ParentCancelled => {
                    self.response_state.cancel().map_err(DeferredResponseError::from_state)
                }
            },
        }
    }
}

fn record_terminalization(stats: &mut DeferredRegistryShutdownStats, result: Result<(), DeferredResponseError>) {
    match result {
        Ok(()) => stats.record_terminalized(),
        Err(error) if error.kind() == DeferredResponseErrorKind::InvalidTransition => {
            if error.source().is_some_and(|source| {
                source.downcast_ref::<ResponseStateError>().is_some_and(|error| {
                    matches!(
                        error,
                        ResponseStateError::InvalidTransition {
                            state: ResponseStateSnapshot::Sending,
                            ..
                        }
                    )
                })
            }) {
                stats.record_in_progress();
            } else {
                stats.record_invariant_failure();
            }
        }
        Err(_) => {}
    }
}

fn record_state_terminalization(stats: &mut DeferredRegistryShutdownStats, result: Result<(), ResponseStateError>) {
    match result {
        Ok(()) => stats.record_terminalized(),
        Err(ResponseStateError::InvalidTransition {
            state: ResponseStateSnapshot::Sending,
            ..
        }) => stats.record_in_progress(),
        Err(ResponseStateError::InvalidTransition { .. }) => stats.record_invariant_failure(),
        Err(ResponseStateError::AlreadyCompleted { .. }) => {}
    }
}

fn remove_entry_for_claim<R: Send + 'static>(state: &mut RegistryState<R>, id: DeferredId) -> Option<Entry<R>> {
    let entry = state.primary.remove(&id)?;
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

fn claim_marker_outcome<R>(id: DeferredId, marker: Option<Arc<ClaimMarker<R>>>) -> ClaimStart<R>
where
    R: Send + 'static,
{
    let Some(marker) = marker else {
        return ClaimStart::Error(DeferredClaimError::new(
            DeferredClaimErrorKind::NotFound,
            id,
            None,
            None,
            None,
        ));
    };
    let terminal = marker.terminal_state();
    ClaimStart::Error(DeferredClaimError::new(
        if terminal.is_some() {
            DeferredClaimErrorKind::AlreadyCompleted
        } else {
            DeferredClaimErrorKind::AlreadyClaimed
        },
        id,
        Some(marker.request_id()),
        terminal,
        None,
    ))
}

fn claim_kind_from_registry(kind: DeferredRegistryErrorKind) -> DeferredClaimErrorKind {
    match kind {
        DeferredRegistryErrorKind::ParentCancelled => DeferredClaimErrorKind::ParentCancelled,
        DeferredRegistryErrorKind::SessionClosed => DeferredClaimErrorKind::SessionClosed,
        DeferredRegistryErrorKind::DeadlineExpired => DeferredClaimErrorKind::DeadlineExpired,
        DeferredRegistryErrorKind::RetainedSizeOverflow
        | DeferredRegistryErrorKind::RetainedSizeUnderreported
        | DeferredRegistryErrorKind::DuplicateRequest
        | DeferredRegistryErrorKind::IdentityExhausted
        | DeferredRegistryErrorKind::Builder
        | DeferredRegistryErrorKind::RegistryInvariant => DeferredClaimErrorKind::RegistryInvariant,
    }
}

fn ticket_resolution_for_entry<R>(entry: &Entry<R>) -> TicketResolution {
    match lifecycle_stop(&entry.control) {
        Some(DeferredRegistryErrorKind::ParentCancelled) => TicketResolution::RemovedParentCancelled,
        Some(DeferredRegistryErrorKind::SessionClosed) => TicketResolution::RemovedSessionClosed,
        Some(DeferredRegistryErrorKind::DeadlineExpired) => TicketResolution::RemovedDeadlineExpired,
        Some(
            DeferredRegistryErrorKind::RetainedSizeOverflow
            | DeferredRegistryErrorKind::RetainedSizeUnderreported
            | DeferredRegistryErrorKind::DuplicateRequest
            | DeferredRegistryErrorKind::IdentityExhausted
            | DeferredRegistryErrorKind::Builder
            | DeferredRegistryErrorKind::RegistryInvariant,
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

    fn publish(&mut self) -> Result<(), DeferredCommitError> {
        let request = self.request.take().ok_or_else(DeferredCommitError::invariant)?;
        match self.inner.publish_active(self.id, request) {
            Ok(()) => {
                self.active = false;
                Ok(())
            }
            Err(request) => {
                self.request = Some(*request);
                Err(DeferredCommitError::invariant())
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

pub(super) fn validate_retained_floor<R>(retained_bytes: usize) -> Result<(), DeferredRegistryErrorKind>
where
    R: Send + 'static,
{
    let required = DeferredRegistry::<R>::try_retained_size(DeferredRetainedSizeParts::new(0))
        .map_err(|_| DeferredRegistryErrorKind::RetainedSizeOverflow)?;
    if retained_bytes < required.bytes() {
        Err(DeferredRegistryErrorKind::RetainedSizeUnderreported)
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
    })
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

pub(super) fn lifecycle_stop(control: &RequestControlView) -> Option<DeferredRegistryErrorKind> {
    if control.parent_is_cancelled() {
        Some(DeferredRegistryErrorKind::ParentCancelled)
    } else if control.session_is_closed() {
        Some(DeferredRegistryErrorKind::SessionClosed)
    } else if control.deadline().is_some_and(RequestDeadline::is_expired) {
        Some(DeferredRegistryErrorKind::DeadlineExpired)
    } else {
        None
    }
}
