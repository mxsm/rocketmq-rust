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
use std::collections::TryReserveError;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_transport::api::DeferredId;

use super::deadline::PopLiteWaitDeadline;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PopLiteIndexLimits {
    pub(crate) max_entries: NonZeroUsize,
    pub(crate) max_clients: NonZeroUsize,
    pub(crate) max_entries_per_client: NonZeroUsize,
}

impl PopLiteIndexLimits {
    pub(crate) const fn new(
        max_entries: NonZeroUsize,
        max_clients: NonZeroUsize,
        max_entries_per_client: NonZeroUsize,
    ) -> Self {
        Self {
            max_entries,
            max_clients,
            max_entries_per_client,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PopLiteIndexSnapshot {
    pub(crate) live: usize,
    pub(crate) reserved: usize,
    pub(crate) candidates: usize,
    pub(crate) clients: usize,
    pub(crate) oldest_waiter_age: Option<Duration>,
}

pub(crate) struct PopLiteCriteriaIndex<I = DeferredId> {
    inner: Arc<PopLiteCriteriaIndexInner<I>>,
}

impl<I> Clone for PopLiteCriteriaIndex<I> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<I> PopLiteCriteriaIndex<I>
where
    I: Copy + Eq,
{
    pub(crate) fn new(limits: PopLiteIndexLimits) -> Self {
        Self {
            inner: Arc::new(PopLiteCriteriaIndexInner {
                limits,
                state: Mutex::new(PopLiteCriteriaIndexState::default()),
            }),
        }
    }

    pub(crate) fn try_retained_bytes_per_entry() -> Option<usize> {
        let (membership, _) = Layout::new::<AtomicBool>().extend(Layout::new::<AtomicBool>()).ok()?;
        [
            std::mem::size_of::<PopLiteIndexRecord<I>>(),
            std::mem::size_of::<CheetahString>(),
            std::mem::size_of::<PopLiteClientBucket<I>>(),
            membership.pad_to_align().size(),
        ]
        .into_iter()
        .try_fold(0usize, usize::checked_add)
    }

    pub(crate) fn reserve(
        &self,
        client_id: CheetahString,
        registered_at: tokio::time::Instant,
    ) -> Result<PopLiteIndexReserveOutcome<I>, PopLiteIndexOperationalError> {
        let mut state = self.inner.state.lock();
        let occupied = state
            .live
            .checked_add(state.reserved)
            .ok_or(PopLiteIndexOperationalError::AccountingOverflow)?;
        if occupied >= self.inner.limits.max_entries.get() {
            return Ok(PopLiteIndexReserveOutcome::Rejected(
                PopLiteIndexReserveRejection::Global,
            ));
        }
        let bucket_missing = !state.clients.contains_key(&client_id);
        if bucket_missing && state.clients.len() >= self.inner.limits.max_clients.get() {
            return Ok(PopLiteIndexReserveOutcome::Rejected(
                PopLiteIndexReserveRejection::Client,
            ));
        }
        let client_occupied = match state.clients.get(&client_id) {
            Some(bucket) => bucket
                .entries
                .len()
                .checked_add(bucket.candidates)
                .and_then(|occupied| occupied.checked_add(bucket.reserved))
                .ok_or(PopLiteIndexOperationalError::AccountingOverflow)?,
            None => 0,
        };
        if client_occupied >= self.inner.limits.max_entries_per_client.get() {
            return Ok(PopLiteIndexReserveOutcome::Rejected(
                PopLiteIndexReserveRejection::PerClient,
            ));
        }
        let next_sequence = state
            .next_sequence
            .checked_add(1)
            .ok_or(PopLiteIndexOperationalError::SequenceExhausted)?;
        if bucket_missing {
            state
                .clients
                .try_reserve(1)
                .map_err(PopLiteIndexOperationalError::Allocation)?;
        }
        let mut new_bucket = bucket_missing.then(PopLiteClientBucket::default);
        if let Some(bucket) = new_bucket.as_mut() {
            bucket
                .entries
                .try_reserve(1)
                .map_err(PopLiteIndexOperationalError::Allocation)?;
        } else {
            state
                .clients
                .get_mut(&client_id)
                .expect("existing PopLite client bucket remains present")
                .entries
                .try_reserve(1)
                .map_err(PopLiteIndexOperationalError::Allocation)?;
        }
        if let Some(bucket) = new_bucket {
            state.clients.insert(client_id.clone(), bucket);
        }
        let bucket = state
            .clients
            .get_mut(&client_id)
            .expect("reserved PopLite client bucket exists");
        bucket.reserved += 1;
        state.reserved += 1;
        state.next_sequence = next_sequence;
        drop(state);
        Ok(PopLiteIndexReserveOutcome::Reserved(PopLiteIndexReservation {
            inner: Some(Arc::clone(&self.inner)),
            client_id: Some(client_id),
            sequence: next_sequence - 1,
            registered_at,
            membership: Arc::new(PopLiteIndexMembership::default()),
        }))
    }

    pub(crate) fn reserve_oldest(&self, client_id: &CheetahString) -> Option<PopLiteCandidateReservation<I>> {
        let mut state = self.inner.state.lock();
        let bucket = state.clients.get_mut(client_id)?;
        let record = bucket.entries.pop_front()?;
        record.membership.candidate.store(true, Ordering::Release);
        bucket.candidates += 1;
        state.candidates += 1;
        Some(PopLiteCandidateReservation {
            inner: Arc::downgrade(&self.inner),
            client_id: client_id.clone(),
            record: Some(record),
        })
    }

    pub(crate) fn snapshot(&self) -> PopLiteIndexSnapshot {
        let state = self.inner.state.lock();
        let now = tokio::time::Instant::now();
        let oldest_registered = state
            .clients
            .values()
            .flat_map(|bucket| bucket.entries.iter())
            .map(|record| record.registered_at)
            .min();
        PopLiteIndexSnapshot {
            live: state.live,
            reserved: state.reserved,
            candidates: state.candidates,
            clients: state.clients.len(),
            oldest_waiter_age: oldest_registered.map(|registered| now.saturating_duration_since(registered)),
        }
    }
}

struct PopLiteCriteriaIndexInner<I> {
    limits: PopLiteIndexLimits,
    state: Mutex<PopLiteCriteriaIndexState<I>>,
}

struct PopLiteCriteriaIndexState<I> {
    clients: HashMap<CheetahString, PopLiteClientBucket<I>>,
    live: usize,
    reserved: usize,
    candidates: usize,
    next_sequence: u64,
}

impl<I> Default for PopLiteCriteriaIndexState<I> {
    fn default() -> Self {
        Self {
            clients: HashMap::new(),
            live: 0,
            reserved: 0,
            candidates: 0,
            next_sequence: 0,
        }
    }
}

struct PopLiteClientBucket<I> {
    entries: VecDeque<PopLiteIndexRecord<I>>,
    reserved: usize,
    candidates: usize,
}

impl<I> Default for PopLiteClientBucket<I> {
    fn default() -> Self {
        Self {
            entries: VecDeque::new(),
            reserved: 0,
            candidates: 0,
        }
    }
}

struct PopLiteIndexRecord<I> {
    id: I,
    deadline: PopLiteWaitDeadline,
    sequence: u64,
    registered_at: tokio::time::Instant,
    membership: Arc<PopLiteIndexMembership>,
}

impl<I> PopLiteIndexRecord<I> {
    const fn order_key(&self) -> (tokio::time::Instant, u64) {
        (self.deadline.protocol_at(), self.sequence)
    }
}

#[derive(Default)]
struct PopLiteIndexMembership {
    live: AtomicBool,
    candidate: AtomicBool,
}

fn insert_ordered<I>(entries: &mut VecDeque<PopLiteIndexRecord<I>>, record: PopLiteIndexRecord<I>) {
    let position = entries
        .binary_search_by_key(&record.order_key(), PopLiteIndexRecord::order_key)
        .unwrap_or_else(|position| position);
    entries.insert(position, record);
}

fn bucket_is_empty<I>(bucket: &PopLiteClientBucket<I>) -> bool {
    bucket.entries.is_empty() && bucket.reserved == 0 && bucket.candidates == 0
}

#[must_use]
pub(crate) struct PopLiteIndexReservation<I = DeferredId> {
    inner: Option<Arc<PopLiteCriteriaIndexInner<I>>>,
    client_id: Option<CheetahString>,
    sequence: u64,
    registered_at: tokio::time::Instant,
    membership: Arc<PopLiteIndexMembership>,
}

impl<I> PopLiteIndexReservation<I>
where
    I: Copy + Eq,
{
    pub(crate) fn publish(mut self, id: I, deadline: PopLiteWaitDeadline) -> PopLiteIndexLease<I> {
        let inner = self.inner.take().expect("armed PopLite index reservation owns index");
        let client_id = self
            .client_id
            .take()
            .expect("armed PopLite index reservation owns client id");
        {
            let mut state = inner.state.lock();
            let bucket = state
                .clients
                .get_mut(&client_id)
                .expect("armed PopLite reservation retains client bucket");
            bucket.reserved -= 1;
            insert_ordered(
                &mut bucket.entries,
                PopLiteIndexRecord {
                    id,
                    deadline,
                    sequence: self.sequence,
                    registered_at: self.registered_at,
                    membership: Arc::clone(&self.membership),
                },
            );
            state.reserved -= 1;
            state.live += 1;
            self.membership.live.store(true, Ordering::Release);
        }
        PopLiteIndexLease {
            inner: Arc::downgrade(&inner),
            client_id,
            id,
            order: (deadline.protocol_at(), self.sequence),
            membership: Arc::clone(&self.membership),
        }
    }
}

impl<I> Drop for PopLiteIndexReservation<I> {
    fn drop(&mut self) {
        let (Some(inner), Some(client_id)) = (self.inner.take(), self.client_id.take()) else {
            return;
        };
        let mut state = inner.state.lock();
        let remove = if let Some(bucket) = state.clients.get_mut(&client_id) {
            bucket.reserved -= 1;
            bucket_is_empty(bucket)
        } else {
            false
        };
        state.reserved -= 1;
        if remove {
            state.clients.remove(&client_id);
        }
    }
}

#[must_use]
pub(crate) struct PopLiteIndexLease<I: Eq = DeferredId> {
    inner: Weak<PopLiteCriteriaIndexInner<I>>,
    client_id: CheetahString,
    id: I,
    order: (tokio::time::Instant, u64),
    membership: Arc<PopLiteIndexMembership>,
}

impl<I> Drop for PopLiteIndexLease<I>
where
    I: Eq,
{
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut state = inner.state.lock();
        if !self.membership.live.swap(false, Ordering::AcqRel) {
            return;
        }
        let mut remove_client = false;
        let mut candidate_removed = false;
        if let Some(bucket) = state.clients.get_mut(&self.client_id) {
            if self.membership.candidate.swap(false, Ordering::AcqRel) {
                bucket.candidates -= 1;
                candidate_removed = true;
            } else if let Ok(position) = bucket
                .entries
                .binary_search_by_key(&self.order, PopLiteIndexRecord::order_key)
            {
                if bucket.entries.get(position).is_some_and(|record| record.id == self.id) {
                    bucket.entries.remove(position);
                }
            }
            remove_client = bucket_is_empty(bucket);
        }
        if candidate_removed {
            state.candidates -= 1;
        }
        state.live -= 1;
        if remove_client {
            state.clients.remove(&self.client_id);
        }
    }
}

#[must_use]
pub(crate) struct PopLiteCandidateReservation<I: Eq = DeferredId> {
    inner: Weak<PopLiteCriteriaIndexInner<I>>,
    client_id: CheetahString,
    record: Option<PopLiteIndexRecord<I>>,
}

impl<I> PopLiteCandidateReservation<I>
where
    I: Copy + Eq,
{
    pub(crate) fn id(&self) -> I {
        self.record.as_ref().expect("live PopLite candidate owns record").id
    }
}

impl<I> Drop for PopLiteCandidateReservation<I>
where
    I: Eq,
{
    fn drop(&mut self) {
        let Some(record) = self.record.take() else {
            return;
        };
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut state = inner.state.lock();
        let Some(bucket) = state.clients.get_mut(&self.client_id) else {
            return;
        };
        if !record.membership.candidate.swap(false, Ordering::AcqRel) {
            return;
        }
        bucket.candidates -= 1;
        if record.membership.live.load(Ordering::Acquire) {
            insert_ordered(&mut bucket.entries, record);
        }
        state.candidates -= 1;
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PopLiteIndexReserveRejection {
    Global,
    Client,
    PerClient,
}

#[must_use]
pub(crate) enum PopLiteIndexReserveOutcome<I> {
    Reserved(PopLiteIndexReservation<I>),
    Rejected(PopLiteIndexReserveRejection),
}

pub(crate) enum PopLiteIndexOperationalError {
    AccountingOverflow,
    SequenceExhausted,
    Allocation(TryReserveError),
}

impl fmt::Debug for PopLiteIndexOperationalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AccountingOverflow => "PopLiteIndexOperationalError::AccountingOverflow",
            Self::SequenceExhausted => "PopLiteIndexOperationalError::SequenceExhausted",
            Self::Allocation(_) => "PopLiteIndexOperationalError::Allocation",
        })
    }
}

impl fmt::Display for PopLiteIndexOperationalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AccountingOverflow => "PopLite deferred index accounting overflowed",
            Self::SequenceExhausted => "PopLite deferred index sequence exhausted",
            Self::Allocation(_) => "PopLite deferred index allocation failed",
        })
    }
}

impl Error for PopLiteIndexOperationalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Allocation(source) => Some(source),
            Self::AccountingOverflow | Self::SequenceExhausted => None,
        }
    }
}
