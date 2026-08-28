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
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_store::CqExtUnit;
use rocketmq_transport::api::v2::DeferredId;

use super::data::PullMatchCriteria;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PullCriteriaLimits {
    max_entries: NonZeroUsize,
    max_entries_per_key: NonZeroUsize,
}

impl PullCriteriaLimits {
    pub(crate) const fn new(max_entries: NonZeroUsize, max_entries_per_key: NonZeroUsize) -> Self {
        Self {
            max_entries,
            max_entries_per_key,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct PullCriteriaKey {
    topic: CheetahString,
    queue_id: i32,
}

impl PullCriteriaKey {
    pub(crate) fn new(topic: CheetahString, queue_id: i32) -> Self {
        Self { topic, queue_id }
    }

    pub(super) fn from_criteria(criteria: &PullMatchCriteria) -> Self {
        Self::new(criteria.physical_topic().clone(), criteria.physical_queue_id())
    }
}

/// Borrowed listener metadata. Nothing in this view may cross an async boundary.
#[derive(Clone, Copy)]
pub(crate) struct PullArrivalView<'a> {
    topic: &'a CheetahString,
    queue_id: i32,
    max_offset: i64,
    tags_code: Option<i64>,
    message_store_time: i64,
    filter_bitmap: Option<&'a [u8]>,
    properties: Option<&'a HashMap<CheetahString, CheetahString>>,
    forced: bool,
}

impl<'a> PullArrivalView<'a> {
    pub(crate) const fn new(topic: &'a CheetahString, queue_id: i32, max_offset: i64) -> Self {
        Self {
            topic,
            queue_id,
            max_offset,
            tags_code: None,
            message_store_time: 0,
            filter_bitmap: None,
            properties: None,
            forced: false,
        }
    }

    pub(crate) const fn with_filter_metadata(
        mut self,
        tags_code: Option<i64>,
        message_store_time: i64,
        filter_bitmap: Option<&'a [u8]>,
        properties: Option<&'a HashMap<CheetahString, CheetahString>>,
    ) -> Self {
        self.tags_code = tags_code;
        self.message_store_time = message_store_time;
        self.filter_bitmap = filter_bitmap;
        self.properties = properties;
        self
    }

    pub(crate) const fn with_max_offset(mut self, max_offset: i64) -> Self {
        self.max_offset = max_offset;
        self
    }

    #[must_use]
    pub(crate) const fn forced(mut self) -> Self {
        self.forced = true;
        self
    }
}

/// Per-callback continuation. A later arrival starts with a fresh cursor.
#[derive(Default)]
pub(crate) struct PullScanCursor {
    after_sequence: Option<u64>,
}

impl PullScanCursor {
    pub(crate) const fn new() -> Self {
        Self { after_sequence: None }
    }
}

pub(crate) struct PullCriteriaIndex<I = DeferredId> {
    inner: Arc<PullCriteriaIndexInner<I>>,
}

impl<I> Clone for PullCriteriaIndex<I> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<I> PullCriteriaIndex<I>
where
    I: Copy + Eq,
{
    pub(crate) fn new(limits: PullCriteriaLimits) -> Self {
        Self {
            inner: Arc::new(PullCriteriaIndexInner {
                limits,
                state: Mutex::new(PullCriteriaIndexState::default()),
            }),
        }
    }

    pub(crate) fn try_retained_bytes_per_entry() -> Option<usize> {
        let arc_header = Layout::array::<usize>(2).ok()?;
        let (membership, _) = arc_header.extend(Layout::new::<PullIndexMembership>()).ok()?;
        [
            std::mem::size_of::<PullIndexRecord<I>>(),
            std::mem::size_of::<PullCriteriaKey>() * 2,
            std::mem::size_of::<VecDeque<PullIndexRecord<I>>>(),
            membership.pad_to_align().size(),
            std::mem::size_of::<usize>() * 8,
        ]
        .into_iter()
        .try_fold(0usize, |total, value| total.checked_add(value))
    }

    pub(crate) fn reserve(&self, key: PullCriteriaKey) -> Result<PullIndexReservation<I>, PullIndexError> {
        let mut state = self.inner.state.lock();
        let occupied = state
            .live
            .checked_add(state.reserved)
            .ok_or_else(|| PullIndexError::new(PullIndexErrorKind::GlobalCapacity))?;
        if occupied >= self.inner.limits.max_entries.get() {
            return Err(PullIndexError::new(PullIndexErrorKind::GlobalCapacity));
        }
        let per_key = state.buckets.get(&key).map_or(0, VecDeque::len);
        let reserved_for_key = state.reserved_keys.get(&key).copied().unwrap_or_default();
        let occupied_for_key = per_key
            .checked_add(reserved_for_key)
            .ok_or_else(|| PullIndexError::new(PullIndexErrorKind::BucketCapacity))?;
        if occupied_for_key >= self.inner.limits.max_entries_per_key.get() {
            return Err(PullIndexError::new(PullIndexErrorKind::BucketCapacity));
        }
        let sequence = state.next_sequence;
        state.next_sequence = sequence
            .checked_add(1)
            .ok_or_else(|| PullIndexError::new(PullIndexErrorKind::SequenceExhausted))?;
        if !state.reserved_keys.contains_key(&key) {
            state
                .reserved_keys
                .try_reserve(1)
                .map_err(|_| PullIndexError::new(PullIndexErrorKind::Allocation))?;
        }
        if !state.buckets.contains_key(&key) {
            state
                .buckets
                .try_reserve(1)
                .map_err(|_| PullIndexError::new(PullIndexErrorKind::Allocation))?;
            state.buckets.insert(key.clone(), VecDeque::new());
        }
        state
            .buckets
            .get_mut(&key)
            .expect("reserved Pull bucket was inserted")
            .try_reserve(
                reserved_for_key
                    .checked_add(1)
                    .ok_or_else(|| PullIndexError::new(PullIndexErrorKind::BucketCapacity))?,
            )
            .map_err(|_| PullIndexError::new(PullIndexErrorKind::Allocation))?;
        let reserved_count = state.reserved_keys.entry(key.clone()).or_default();
        *reserved_count = reserved_count
            .checked_add(1)
            .ok_or_else(|| PullIndexError::new(PullIndexErrorKind::BucketCapacity))?;
        state.reserved = state
            .reserved
            .checked_add(1)
            .ok_or_else(|| PullIndexError::new(PullIndexErrorKind::GlobalCapacity))?;
        Ok(PullIndexReservation {
            inner: Some(Arc::clone(&self.inner)),
            key: Some(key),
            sequence,
            membership: Arc::new(PullIndexMembership::default()),
        })
    }

    /// Filters a bounded ordered prefix outside the mutex and atomically hides matches.
    pub(crate) fn reserve_matching(
        &self,
        arrival: &PullArrivalView<'_>,
        cursor: &mut PullScanCursor,
        scan_limit: NonZeroUsize,
        candidate_limit: NonZeroUsize,
    ) -> Vec<PullCandidateReservation<I>> {
        self.reserve_matching_batch(arrival, cursor, scan_limit, candidate_limit)
            .into_candidates()
    }

    pub(crate) fn reserve_matching_batch(
        &self,
        arrival: &PullArrivalView<'_>,
        cursor: &mut PullScanCursor,
        scan_limit: NonZeroUsize,
        candidate_limit: NonZeroUsize,
    ) -> PullCandidateBatch<I> {
        let key = PullCriteriaKey::new(arrival.topic.clone(), arrival.queue_id);
        let mut candidates = Vec::new();
        let mut inspected = 0;
        let mut exhausted = false;
        while inspected < scan_limit.get() && candidates.len() < candidate_limit.get() {
            let Some(snapshot) = self.next_snapshot(&key, cursor.after_sequence) else {
                exhausted = true;
                break;
            };
            cursor.after_sequence = Some(snapshot.sequence);
            inspected += 1;
            if !matches_arrival(&snapshot.criteria, arrival) {
                continue;
            }
            if let Some(candidate) = self.try_reserve_candidate(snapshot) {
                candidates.push(candidate);
            }
        }
        PullCandidateBatch {
            candidates,
            inspected,
            exhausted,
        }
    }

    pub(crate) fn needs_offset_refresh(&self, arrival: &PullArrivalView<'_>) -> bool {
        if arrival.forced {
            return false;
        }
        let key = PullCriteriaKey::new(arrival.topic.clone(), arrival.queue_id);
        let state = self.inner.state.lock();
        state.buckets.get(&key).is_some_and(|bucket| {
            bucket
                .iter()
                .any(|record| arrival.max_offset <= record.criteria.pull_from_offset())
        })
    }

    pub(crate) fn reserve_forced_batch(
        &self,
        cursor: &mut PullScanCursor,
        scan_limit: NonZeroUsize,
        candidate_limit: NonZeroUsize,
    ) -> PullCandidateBatch<I> {
        let mut candidates = Vec::new();
        let mut inspected = 0;
        let mut exhausted = false;
        while inspected < scan_limit.get() && candidates.len() < candidate_limit.get() {
            let Some(snapshot) = self.next_forced_snapshot(cursor.after_sequence) else {
                exhausted = true;
                break;
            };
            cursor.after_sequence = Some(snapshot.sequence);
            inspected += 1;
            if let Some(candidate) = self.try_reserve_candidate(snapshot) {
                candidates.push(candidate);
            }
        }
        PullCandidateBatch {
            candidates,
            inspected,
            exhausted,
        }
    }

    fn next_snapshot(&self, key: &PullCriteriaKey, after: Option<u64>) -> Option<PullCandidateSnapshot<I>> {
        let state = self.inner.state.lock();
        state.buckets.get(key)?.iter().find_map(|record| {
            (after.is_none_or(|after| record.sequence > after)).then(|| PullCandidateSnapshot {
                key: key.clone(),
                id: record.id,
                sequence: record.sequence,
                criteria: Arc::clone(&record.criteria),
            })
        })
    }

    fn next_forced_snapshot(&self, after: Option<u64>) -> Option<PullCandidateSnapshot<I>> {
        let state = self.inner.state.lock();
        state
            .buckets
            .iter()
            .flat_map(|(key, bucket)| bucket.iter().map(move |record| (key, record)))
            .filter(|(_, record)| after.is_none_or(|after| record.sequence > after))
            .min_by_key(|(_, record)| record.sequence)
            .map(|(key, record)| PullCandidateSnapshot {
                key: key.clone(),
                id: record.id,
                sequence: record.sequence,
                criteria: Arc::clone(&record.criteria),
            })
    }

    fn try_reserve_candidate(&self, snapshot: PullCandidateSnapshot<I>) -> Option<PullCandidateReservation<I>> {
        let mut state = self.inner.state.lock();
        let bucket = state.buckets.get_mut(&snapshot.key)?;
        let position = bucket
            .iter()
            .position(|record| record.sequence == snapshot.sequence && record.id == snapshot.id)?;
        let record = bucket.remove(position)?;
        record.membership.candidate.store(true, Ordering::Release);
        state.candidates += 1;
        Some(PullCandidateReservation {
            inner: Arc::downgrade(&self.inner),
            key: snapshot.key,
            record: Some(record),
        })
    }

    #[must_use]
    pub(crate) fn snapshot(&self) -> PullIndexSnapshot {
        let state = self.inner.state.lock();
        PullIndexSnapshot {
            live: state.live,
            reserved: state.reserved,
            candidates: state.candidates,
            buckets: state.buckets.values().filter(|bucket| !bucket.is_empty()).count(),
        }
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, id: I) -> bool {
        let state = self.inner.state.lock();
        state
            .buckets
            .values()
            .any(|bucket| bucket.iter().any(|record| record.id == id))
    }
}

#[must_use]
pub(crate) struct PullCandidateBatch<I = DeferredId>
where
    I: Copy + Eq,
{
    candidates: Vec<PullCandidateReservation<I>>,
    inspected: usize,
    exhausted: bool,
}

impl<I> PullCandidateBatch<I>
where
    I: Copy + Eq,
{
    pub(crate) const fn inspected(&self) -> usize {
        self.inspected
    }

    pub(crate) const fn exhausted(&self) -> bool {
        self.exhausted
    }

    pub(crate) fn into_candidates(self) -> Vec<PullCandidateReservation<I>> {
        self.candidates
    }
}

fn matches_arrival(criteria: &PullMatchCriteria, arrival: &PullArrivalView<'_>) -> bool {
    if arrival.forced {
        return true;
    }
    if arrival.max_offset <= criteria.pull_from_offset() {
        return false;
    }
    let cq = CqExtUnit::new(
        arrival.tags_code.unwrap_or_default(),
        arrival.message_store_time,
        arrival.filter_bitmap.map(<[u8]>::to_vec),
    );
    if !criteria
        .filter()
        .is_matched_by_consume_queue(arrival.tags_code, Some(&cq))
    {
        return false;
    }
    arrival
        .properties
        .is_none_or(|properties| criteria.filter().is_matched_by_commit_log(None, Some(properties)))
}

struct PullCriteriaIndexInner<I> {
    limits: PullCriteriaLimits,
    state: Mutex<PullCriteriaIndexState<I>>,
}

struct PullCriteriaIndexState<I> {
    buckets: HashMap<PullCriteriaKey, VecDeque<PullIndexRecord<I>>>,
    reserved_keys: HashMap<PullCriteriaKey, usize>,
    live: usize,
    reserved: usize,
    candidates: usize,
    next_sequence: u64,
}

impl<I> Default for PullCriteriaIndexState<I> {
    fn default() -> Self {
        Self {
            buckets: HashMap::new(),
            reserved_keys: HashMap::new(),
            live: 0,
            reserved: 0,
            candidates: 0,
            next_sequence: 0,
        }
    }
}

struct PullIndexRecord<I> {
    id: I,
    sequence: u64,
    criteria: Arc<PullMatchCriteria>,
    membership: Arc<PullIndexMembership>,
}

#[derive(Default)]
struct PullIndexMembership {
    candidate: AtomicBool,
    detached: AtomicBool,
}

struct PullCandidateSnapshot<I> {
    key: PullCriteriaKey,
    id: I,
    sequence: u64,
    criteria: Arc<PullMatchCriteria>,
}

#[must_use]
pub(crate) struct PullIndexReservation<I = DeferredId>
where
    I: Copy + Eq,
{
    inner: Option<Arc<PullCriteriaIndexInner<I>>>,
    key: Option<PullCriteriaKey>,
    sequence: u64,
    membership: Arc<PullIndexMembership>,
}

impl<I> PullIndexReservation<I>
where
    I: Copy + Eq,
{
    pub(crate) fn publish(mut self, id: I, criteria: Arc<PullMatchCriteria>) -> PullIndexLease<I> {
        let inner = self.inner.take().expect("Pull reservation publishes only once");
        let key = self.key.take().expect("Pull reservation retains its key");
        let mut state = inner.state.lock();
        release_reserved_key(&mut state, &key);
        state.live += 1;
        let record = PullIndexRecord {
            id,
            sequence: self.sequence,
            criteria,
            membership: Arc::clone(&self.membership),
        };
        let bucket = state.buckets.entry(key.clone()).or_default();
        let position = bucket
            .iter()
            .position(|current| current.sequence > self.sequence)
            .unwrap_or(bucket.len());
        bucket.insert(position, record);
        drop(state);
        PullIndexLease {
            inner: Arc::downgrade(&inner),
            key,
            id,
            membership: Arc::clone(&self.membership),
        }
    }
}

impl<I> Drop for PullIndexReservation<I>
where
    I: Copy + Eq,
{
    fn drop(&mut self) {
        let (Some(inner), Some(key)) = (self.inner.take(), self.key.take()) else {
            return;
        };
        let mut state = inner.state.lock();
        release_reserved_key(&mut state, &key);
    }
}

fn release_reserved_key<I>(state: &mut PullCriteriaIndexState<I>, key: &PullCriteriaKey) {
    state.reserved = state.reserved.saturating_sub(1);
    if let Some(count) = state.reserved_keys.get_mut(key) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            state.reserved_keys.remove(key);
            if state.buckets.get(key).is_some_and(VecDeque::is_empty) {
                state.buckets.remove(key);
            }
        }
    }
}

#[must_use]
pub(crate) struct PullCandidateReservation<I = DeferredId>
where
    I: Copy + Eq,
{
    inner: Weak<PullCriteriaIndexInner<I>>,
    key: PullCriteriaKey,
    record: Option<PullIndexRecord<I>>,
}

impl<I> PullCandidateReservation<I>
where
    I: Copy + Eq,
{
    #[must_use]
    pub(crate) fn id(&self) -> I {
        self.record.as_ref().expect("candidate owns its record").id
    }

    /// Permanently hides a candidate after the registry accepts its single claim.
    pub(crate) fn commit(mut self) {
        let Some(record) = self.record.take() else {
            return;
        };
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut state = inner.state.lock();
        if record.membership.detached.swap(true, Ordering::AcqRel) {
            return;
        }
        record.membership.candidate.store(false, Ordering::Release);
        state.live -= 1;
        state.candidates -= 1;
    }
}

impl<I> Drop for PullCandidateReservation<I>
where
    I: Copy + Eq,
{
    fn drop(&mut self) {
        let Some(record) = self.record.take() else {
            return;
        };
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut state = inner.state.lock();
        if record.membership.detached.load(Ordering::Acquire) {
            return;
        }
        record.membership.candidate.store(false, Ordering::Release);
        state.candidates -= 1;
        let bucket = state.buckets.entry(self.key.clone()).or_default();
        let position = bucket
            .iter()
            .position(|current| current.sequence > record.sequence)
            .unwrap_or(bucket.len());
        bucket.insert(position, record);
    }
}

#[must_use]
pub(crate) struct PullIndexLease<I = DeferredId>
where
    I: Copy + Eq,
{
    inner: Weak<PullCriteriaIndexInner<I>>,
    key: PullCriteriaKey,
    id: I,
    membership: Arc<PullIndexMembership>,
}

impl<I> Drop for PullIndexLease<I>
where
    I: Copy + Eq,
{
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut state = inner.state.lock();
        if self.membership.detached.swap(true, Ordering::AcqRel) {
            return;
        }
        state.live -= 1;
        if self.membership.candidate.load(Ordering::Acquire) {
            state.candidates -= 1;
            return;
        }
        if let Some(bucket) = state.buckets.get_mut(&self.key) {
            if let Some(position) = bucket.iter().position(|record| record.id == self.id) {
                bucket.remove(position);
            }
            if bucket.is_empty() {
                state.buckets.remove(&self.key);
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PullIndexSnapshot {
    live: usize,
    reserved: usize,
    candidates: usize,
    buckets: usize,
}

impl PullIndexSnapshot {
    pub(crate) const fn live(self) -> usize {
        self.live
    }

    pub(crate) const fn reserved(self) -> usize {
        self.reserved
    }

    pub(crate) const fn candidates(self) -> usize {
        self.candidates
    }

    pub(crate) const fn buckets(self) -> usize {
        self.buckets
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PullIndexErrorKind {
    GlobalCapacity,
    BucketCapacity,
    SequenceExhausted,
    Allocation,
}

pub(crate) struct PullIndexError {
    kind: PullIndexErrorKind,
}

impl PullIndexError {
    const fn new(kind: PullIndexErrorKind) -> Self {
        Self { kind }
    }

    pub(crate) const fn kind(&self) -> PullIndexErrorKind {
        self.kind
    }
}

impl fmt::Debug for PullIndexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PullIndexError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for PullIndexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Pull criteria index failed: {:?}", self.kind)
    }
}

impl Error for PullIndexError {}
