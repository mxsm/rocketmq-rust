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
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;

use super::DeferredId;
use super::DeferredParts;
use super::DeferredRegistry;
use super::DeferredRegistryErrorKind;
use super::DeferredRequest;
use super::DeferredResponder;
use super::DeferredResponseError;
use super::DeferredRetainedSizeParts;
use super::DeferredWaitPermit;
use super::RequestControlView;
use super::RequestId;
use super::SessionId;
use crate::deadline::RequestDeadline;

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
        #[cfg(test)]
        let commit_checkpoint = self.commit_checkpoint;
        let mut transaction = CommitTransaction::begin(inner, id)?;
        if let Some(kind) = lifecycle_stop(transaction.request().control()) {
            return Err(DeferredCommitError::lifecycle(kind));
        }
        transaction
            .request()
            .register_response()
            .map_err(DeferredCommitError::response)?;
        #[cfg(test)]
        if let Some(checkpoint) = commit_checkpoint {
            checkpoint();
        }
        if let Some(kind) = lifecycle_stop(transaction.request().control()) {
            return Err(DeferredCommitError::lifecycle(kind));
        }
        transaction.publish()
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeferredWakeResult {
    Recorded,
    Coalesced,
    NotFound,
}

pub(super) struct RegistryInner<R>
where
    R: Send + 'static,
{
    state: Mutex<RegistryState<R>>,
    #[cfg(test)]
    test_sequence: Option<Arc<AtomicU64>>,
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
        }
    }
}

struct RegistryState<R> {
    primary: HashMap<DeferredId, Entry<R>>,
    request_index: HashMap<RequestId, DeferredId>,
    session_index: HashMap<SessionId, HashSet<DeferredId>>,
}

impl<R> Default for RegistryState<R> {
    fn default() -> Self {
        Self {
            primary: HashMap::new(),
            request_index: HashMap::new(),
            session_index: HashMap::new(),
        }
    }
}

pub(super) struct Entry<R> {
    request_id: RequestId,
    session_id: SessionId,
    phase: EntryPhase<R>,
    pending: bool,
    ready: bool,
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
        }
    }

    pub(super) fn insert_shell(
        &self,
        request_id: RequestId,
        session_id: SessionId,
    ) -> Result<DeferredId, DeferredRegistryErrorKind> {
        let mut state = self.state.lock();
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
                phase: EntryPhase::Shell,
                pending: false,
                ready: false,
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
        let mut state = self.state.lock();
        let Some(entry) = state.primary.get_mut(&id) else {
            return Err(Box::new(request));
        };
        if entry.phase.tag() != EntryPhaseTag::Activating {
            return Err(Box::new(request));
        }
        entry.ready |= entry.pending;
        entry.pending = false;
        entry.phase = EntryPhase::Active(request);
        Ok(())
    }

    pub(super) fn remove(&self, id: DeferredId) -> Option<Entry<R>> {
        let mut state = self.state.lock();
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

    pub(super) fn wake(&self, id: DeferredId) -> DeferredWakeResult {
        let mut state = self.state.lock();
        let Some(entry) = state.primary.get_mut(&id) else {
            return DeferredWakeResult::NotFound;
        };
        match entry.phase {
            EntryPhase::Active(_) => {
                if entry.ready {
                    DeferredWakeResult::Coalesced
                } else {
                    entry.ready = true;
                    DeferredWakeResult::Recorded
                }
            }
            EntryPhase::Shell | EntryPhase::Building | EntryPhase::Prepared(_) | EntryPhase::Activating => {
                if entry.pending {
                    DeferredWakeResult::Coalesced
                } else {
                    entry.pending = true;
                    DeferredWakeResult::Recorded
                }
            }
        }
    }

    pub(super) fn take_ready(&self, id: DeferredId) -> bool {
        let mut state = self.state.lock();
        let Some(entry) = state.primary.get_mut(&id) else {
            return false;
        };
        if !matches!(entry.phase, EntryPhase::Active(_)) || !entry.ready {
            return false;
        }
        entry.ready = false;
        true
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

pub(super) fn registry_additional_bytes<R>() -> Option<usize> {
    checked_registry_layout_bytes(RegistryLayoutSizes {
        inline_resume: Layout::new::<R>().size(),
        primary_entry: Layout::new::<(DeferredId, Entry<R>)>().size(),
        responder: Layout::new::<DeferredResponder>().size(),
        permit: Layout::new::<DeferredWaitPermit>().size(),
        request_index: Layout::new::<(RequestId, DeferredId)>().size(),
        session_owner: Layout::new::<(SessionId, HashSet<DeferredId>)>().size(),
        session_member: Layout::new::<DeferredId>().size(),
        ready: Layout::new::<DeferredId>().size(),
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
    pub(super) ready: usize,
}

pub(super) fn checked_registry_layout_bytes(sizes: RegistryLayoutSizes) -> Option<usize> {
    let primary_payload = sizes
        .inline_resume
        .checked_add(sizes.responder)?
        .checked_add(sizes.permit)?;
    let primary_net = sizes.primary_entry.checked_sub(primary_payload)?;
    let session = sizes.session_owner.checked_add(sizes.session_member)?;
    checked_registry_component_sum(
        sizes.inline_resume,
        primary_net,
        sizes.request_index,
        session,
        sizes.ready,
    )
}

pub(super) fn checked_registry_component_sum(
    inline_resume: usize,
    primary_net: usize,
    request_index: usize,
    session: usize,
    ready: usize,
) -> Option<usize> {
    inline_resume
        .checked_add(primary_net)?
        .checked_add(request_index)?
        .checked_add(session)?
        .checked_add(ready)
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
