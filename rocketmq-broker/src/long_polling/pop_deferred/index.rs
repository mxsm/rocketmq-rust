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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_transport::api::DeferredId;

use super::deadline::LongPollingDeadline;

mod types;

pub(crate) use types::PopArrival;
pub(crate) use types::PopArrivalView;
pub(crate) use types::PopCriteriaKey;
pub(crate) use types::PopCriteriaLimits;
pub(crate) use types::PopMatchCriteria;
pub(crate) use types::PopSelectionOrder;

#[derive(Default)]
pub(crate) struct PopFanoutCursor {
    after_group: Option<CheetahString>,
    complete: bool,
}

impl PopFanoutCursor {
    pub(crate) const fn new() -> Self {
        Self {
            after_group: None,
            complete: false,
        }
    }
}

#[must_use]
pub(crate) struct PopFanoutBatch {
    consumer_groups: Vec<CheetahString>,
    exhausted: bool,
}

impl PopFanoutBatch {
    pub(crate) const fn empty() -> Self {
        Self {
            consumer_groups: Vec::new(),
            exhausted: true,
        }
    }

    pub(crate) const fn exhausted(&self) -> bool {
        self.exhausted
    }

    pub(crate) fn into_consumer_groups(self) -> Vec<CheetahString> {
        self.consumer_groups
    }
}

/// Bounded POP criteria index. Registry ownership remains authoritative.
pub(crate) struct PopCriteriaIndex<I = DeferredId> {
    inner: Arc<PopCriteriaIndexInner<I>>,
}

impl<I> Clone for PopCriteriaIndex<I> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<I> PopCriteriaIndex<I>
where
    I: Copy + Eq,
{
    pub(crate) fn new(limits: PopCriteriaLimits) -> Self {
        Self {
            inner: Arc::new(PopCriteriaIndexInner {
                limits,
                state: Mutex::new(PopCriteriaIndexState::default()),
            }),
        }
    }

    pub(crate) fn try_retained_bytes_per_entry() -> Option<usize> {
        let membership = arc_allocation_size::<PopIndexMembership>()?;
        checked_retained_layout_sum([
            std::mem::size_of::<PopIndexRecord<I>>(),
            std::mem::size_of::<PopCriteriaKey>(),
            std::mem::size_of::<PopIndexBucket<I>>(),
            std::mem::size_of::<PopTopicQueueKey>(),
            std::mem::size_of::<PopFanoutBucket>(),
            std::mem::size_of::<PopFanoutGroup>(),
            membership,
        ])
    }

    /// Reserves index capacity before a deferred responder is taken.
    pub(crate) fn reserve(&self, key: PopCriteriaKey) -> Result<PopIndexReservation<I>, PopIndexError> {
        let mut state = self.inner.state.lock();
        let total = state
            .live
            .checked_add(state.reserved)
            .ok_or_else(|| PopIndexError::new(PopIndexErrorKind::GlobalCapacity))?;
        if total >= self.inner.limits.max_entries.get() {
            return Err(PopIndexError::new(PopIndexErrorKind::GlobalCapacity));
        }
        let next_sequence = state
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| PopIndexError::new(PopIndexErrorKind::SequenceExhausted))?;
        let occupied = state.buckets.get(&key).map_or(0, |bucket| {
            bucket
                .entries
                .len()
                .saturating_add(bucket.candidates)
                .saturating_add(bucket.reserved)
        });
        if occupied >= self.inner.limits.max_entries_per_key.get() {
            return Err(PopIndexError::new(PopIndexErrorKind::BucketCapacity));
        }

        let fanout_key = PopTopicQueueKey::from_criteria(&key);
        let group_exists = state.fanout.get(&fanout_key).is_some_and(|bucket| {
            bucket
                .groups
                .iter()
                .any(|group| group.consumer_group == key.consumer_group)
        });
        let bucket_missing = !state.buckets.contains_key(&key);
        let fanout_missing = !state.fanout.contains_key(&fanout_key);
        let additional_entry_capacity = state.buckets.get(&key).map_or(Ok(1), |bucket| {
            bucket
                .reserved
                .checked_add(bucket.candidates)
                .and_then(|pending| pending.checked_add(1))
                .ok_or_else(|| PopIndexError::new(PopIndexErrorKind::Allocation))
        })?;
        if bucket_missing {
            state
                .buckets
                .try_reserve(1)
                .map_err(|_| PopIndexError::new(PopIndexErrorKind::Allocation))?;
        }
        if fanout_missing {
            state
                .fanout
                .try_reserve(1)
                .map_err(|_| PopIndexError::new(PopIndexErrorKind::Allocation))?;
        }
        let mut new_bucket = bucket_missing.then(PopIndexBucket::default);
        if let Some(bucket) = new_bucket.as_mut() {
            bucket
                .entries
                .try_reserve(additional_entry_capacity)
                .map_err(|_| PopIndexError::new(PopIndexErrorKind::Allocation))?;
        } else {
            state
                .buckets
                .get_mut(&key)
                .expect("existing POP bucket remains present")
                .entries
                .try_reserve(additional_entry_capacity)
                .map_err(|_| PopIndexError::new(PopIndexErrorKind::Allocation))?;
        }
        let mut new_fanout = fanout_missing.then(PopFanoutBucket::default);
        if !group_exists {
            if let Some(bucket) = new_fanout.as_mut() {
                bucket
                    .groups
                    .try_reserve(1)
                    .map_err(|_| PopIndexError::new(PopIndexErrorKind::Allocation))?;
            } else {
                state
                    .fanout
                    .get_mut(&fanout_key)
                    .expect("existing POP fanout bucket remains present")
                    .groups
                    .try_reserve(1)
                    .map_err(|_| PopIndexError::new(PopIndexErrorKind::Allocation))?;
            }
        }
        if let Some(bucket) = new_bucket {
            state.buckets.insert(key.clone(), bucket);
        }
        if let Some(bucket) = new_fanout {
            state.fanout.insert(fanout_key.clone(), bucket);
        }

        state
            .buckets
            .get_mut(&key)
            .expect("reserved POP bucket exists")
            .reserved += 1;
        let fanout = state.fanout.entry(fanout_key.clone()).or_default();
        if let Some(group) = fanout
            .groups
            .iter_mut()
            .find(|group| group.consumer_group == key.consumer_group)
        {
            group.reserved += 1;
        } else {
            fanout.groups.push(PopFanoutGroup {
                consumer_group: key.consumer_group.clone(),
                live: 0,
                reserved: 1,
            });
        }
        state.next_sequence = next_sequence;
        state.reserved += 1;
        let sequence = next_sequence - 1;
        drop(state);
        Ok(PopIndexReservation {
            inner: Some(Arc::clone(&self.inner)),
            key: Some(key),
            fanout_key: Some(fanout_key),
            sequence,
            membership: Arc::new(PopIndexMembership::default()),
        })
    }

    #[cfg(test)]
    /// Reserves at most `scan_limit` matching candidates for one test arrival.
    ///
    /// Filters run without the index mutex. Each successful match is then
    /// revalidated and removed from availability under the mutex. A losing
    /// concurrent arrival retries the next ordered candidate without spending
    /// its scan budget, so a one-candidate budget cannot lose a second wake.
    pub(crate) fn reserve_matching(
        &self,
        arrival: &PopArrival,
        order: PopSelectionOrder,
        scan_limit: NonZeroUsize,
    ) -> Vec<PopCandidateReservation<I>> {
        let mut remaining = scan_limit.get();
        let mut reserved = Vec::new();
        while remaining > 0 {
            let Some(remaining_limit) = NonZeroUsize::new(remaining) else {
                break;
            };
            let selection = self.reserve_next_matching(arrival, order, remaining_limit);
            let inspected = selection.inspected();
            if inspected == 0 {
                break;
            }
            remaining -= inspected;
            if let Some(candidate) = selection.into_candidate() {
                reserved.push(candidate);
            } else {
                break;
            }
        }
        reserved
    }

    /// Reserves only the first match in a bounded ordered prefix.
    ///
    /// Holding at most one candidate lets a claim wait on a provisional
    /// registry entry without hiding later waiters from another arrival.
    pub(crate) fn reserve_next_matching(
        &self,
        arrival: &PopArrival,
        order: PopSelectionOrder,
        scan_limit: NonZeroUsize,
    ) -> PopCandidateSelection<I> {
        self.reserve_next_matching_view(arrival.view(), order, scan_limit)
    }

    pub(crate) fn reserve_next_matching_view(
        &self,
        arrival: PopArrivalView<'_>,
        order: PopSelectionOrder,
        scan_limit: NonZeroUsize,
    ) -> PopCandidateSelection<I> {
        let exact = PopCriteriaKey::from_parts(arrival.topic, arrival.consumer_group, arrival.queue_id);
        let wildcard = PopCriteriaKey::from_parts(arrival.topic, arrival.consumer_group, -1);
        let mut skipped = Vec::new();
        let mut inspected = 0;
        let mut conflicts = 0;
        while inspected < scan_limit.get() && conflicts <= scan_limit.get() {
            let Some(snapshot) = self.next_candidate(&exact, &wildcard, order, &skipped) else {
                break;
            };
            if !snapshot.criteria.matches(arrival) {
                skipped.push((snapshot.key, snapshot.order));
                inspected += 1;
                continue;
            }
            match self.try_reserve_candidate(snapshot) {
                Some(candidate) => {
                    inspected += 1;
                    return PopCandidateSelection::new(Some(candidate), inspected);
                }
                None => conflicts += 1,
            }
        }
        PopCandidateSelection::new(None, inspected)
    }

    /// Reserves the first matching candidate from one already-routed target.
    pub(crate) fn reserve_target_matching_view(
        &self,
        key: &PopCriteriaKey,
        arrival: PopArrivalView<'_>,
        order: PopSelectionOrder,
        scan_limit: NonZeroUsize,
    ) -> PopCandidateSelection<I> {
        let mut skipped = Vec::new();
        let mut inspected = 0;
        let mut conflicts = 0;
        while inspected < scan_limit.get() && conflicts <= scan_limit.get() {
            let Some(snapshot) = self.next_candidate(key, key, order, &skipped) else {
                break;
            };
            if !snapshot.criteria.matches(arrival) {
                skipped.push((snapshot.key, snapshot.order));
                inspected += 1;
                continue;
            }
            match self.try_reserve_candidate(snapshot) {
                Some(candidate) => {
                    inspected += 1;
                    return PopCandidateSelection::new(Some(candidate), inspected);
                }
                None => conflicts += 1,
            }
        }
        PopCandidateSelection::new(None, inspected)
    }

    #[cfg(test)]
    /// Test-only identity projection that immediately releases each candidate.
    pub(crate) fn matching_ids(&self, arrival: &PopArrival, order: PopSelectionOrder, limit: NonZeroUsize) -> Vec<I> {
        self.reserve_matching(arrival, order, limit)
            .iter()
            .map(PopCandidateReservation::id)
            .collect()
    }

    /// Returns a bounded topic/queue-to-consumer-group fanout projection.
    ///
    /// The exact and wildcard scopes each contribute at most `per_scope_limit`
    /// groups. Each scope advances a round-robin cursor even when its groups
    /// duplicate the other scope, preventing a hot group from permanently
    /// hiding another group. The index stores only group refcounts, not a
    /// request, responder, or transport capability.
    pub(crate) fn consumer_groups(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        per_scope_limit: NonZeroUsize,
    ) -> Vec<CheetahString> {
        let exact = PopTopicQueueKey::from_arrival(topic, queue_id);
        let wildcard = PopTopicQueueKey::from_arrival(topic, -1);
        let mut state = self.inner.state.lock();
        let mut groups = Vec::new();
        append_fanout_groups(&mut state, &exact, per_scope_limit, &mut groups);
        if wildcard != exact {
            append_fanout_groups(&mut state, &wildcard, per_scope_limit, &mut groups);
        }
        groups
    }

    pub(crate) fn has_arrival_target(&self, topic: &CheetahString, queue_id: i32) -> bool {
        let exact = PopTopicQueueKey::from_arrival(topic, queue_id);
        let wildcard = PopTopicQueueKey::from_arrival(topic, -1);
        let state = self.inner.state.lock();
        let has_target = [&exact, &wildcard].into_iter().any(|key| {
            state
                .fanout
                .get(key)
                .is_some_and(|fanout| fanout.groups.iter().any(|group| group.live > 0))
        });
        has_target
    }

    /// Returns one deterministic bounded batch from the union of exact and
    /// wildcard fanout groups for a single arrival continuation.
    pub(crate) fn consumer_group_batch(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        cursor: &mut PopFanoutCursor,
        limit: NonZeroUsize,
    ) -> PopFanoutBatch {
        if cursor.complete {
            return PopFanoutBatch::empty();
        }
        let exact = PopTopicQueueKey::from_arrival(topic, queue_id);
        let wildcard = PopTopicQueueKey::from_arrival(topic, -1);
        let state = self.inner.state.lock();
        let mut groups = Vec::new();
        while groups.len() < limit.get() {
            let Some(next) = next_fanout_group(&state, &exact, &wildcard, cursor.after_group.as_ref()) else {
                cursor.complete = true;
                break;
            };
            cursor.after_group = Some(next.clone());
            groups.push(next);
        }
        if !cursor.complete && next_fanout_group(&state, &exact, &wildcard, cursor.after_group.as_ref()).is_none() {
            cursor.complete = true;
        }
        PopFanoutBatch {
            consumer_groups: groups,
            exhausted: cursor.complete,
        }
    }

    /// Returns a bounded round-robin batch of live targets for one lag refresh.
    pub(crate) fn forced_targets(
        &self,
        topic: &CheetahString,
        consumer_group: &CheetahString,
        limit: NonZeroUsize,
    ) -> Vec<PopCriteriaKey> {
        let mut state = self.inner.state.lock();
        let mut keys = state
            .buckets
            .iter()
            .filter(|(key, bucket)| {
                &key.topic == topic && &key.consumer_group == consumer_group && !bucket.entries.is_empty()
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        keys.sort_by_key(PopCriteriaKey::queue_id);
        if keys.is_empty() {
            state.target_cursor = 0;
            return keys;
        }
        let start = state.target_cursor % keys.len();
        let take = limit.get().min(keys.len());
        let selected = (0..take)
            .map(|offset| keys[(start + offset) % keys.len()].clone())
            .collect();
        state.target_cursor = (start + take) % keys.len();
        selected
    }

    fn next_candidate(
        &self,
        exact: &PopCriteriaKey,
        wildcard: &PopCriteriaKey,
        order: PopSelectionOrder,
        skipped: &[(PopCriteriaKey, PopOrderKey)],
    ) -> Option<PopIndexCandidateSnapshot<I>> {
        let state = self.inner.state.lock();
        let exact_candidate = candidate_from_bucket(&state, exact, order, skipped);
        let wildcard_candidate = (wildcard != exact)
            .then(|| candidate_from_bucket(&state, wildcard, order, skipped))
            .flatten();
        match (exact_candidate, wildcard_candidate) {
            (Some(exact), Some(wildcard)) => {
                let exact_first = match order {
                    PopSelectionOrder::Oldest => exact.order <= wildcard.order,
                    PopSelectionOrder::Newest => exact.order >= wildcard.order,
                };
                Some(if exact_first { exact } else { wildcard })
            }
            (Some(candidate), None) | (None, Some(candidate)) => Some(candidate),
            (None, None) => None,
        }
    }

    fn try_reserve_candidate(&self, snapshot: PopIndexCandidateSnapshot<I>) -> Option<PopCandidateReservation<I>> {
        let mut state = self.inner.state.lock();
        let bucket = state.buckets.get_mut(&snapshot.key)?;
        let position = bucket
            .entries
            .binary_search_by_key(&snapshot.order, PopIndexRecord::order_key)
            .ok()?;
        if bucket.entries.get(position)?.id != snapshot.id {
            return None;
        }
        let record = bucket.entries.remove(position)?;
        record.membership.candidate_reserved.store(true, Ordering::Release);
        bucket.candidates += 1;
        Some(PopCandidateReservation {
            inner: Arc::downgrade(&self.inner),
            key: snapshot.key,
            record: Some(record),
        })
    }

    #[must_use]
    pub(crate) fn snapshot(&self) -> PopIndexSnapshot {
        let state = self.inner.state.lock();
        PopIndexSnapshot {
            live: state.live,
            reserved: state.reserved,
            buckets: state.buckets.len(),
            candidates: state.buckets.values().map(|bucket| bucket.candidates).sum(),
        }
    }

    pub(crate) fn contains(&self, id: I) -> bool {
        self.inner
            .state
            .lock()
            .buckets
            .values()
            .any(|bucket| bucket.entries.iter().any(|record| record.id == id))
    }

    pub(crate) fn polling_count(&self, key: &PopCriteriaKey) -> i32 {
        self.inner
            .state
            .lock()
            .buckets
            .get(key)
            .map_or(0, |bucket| i32::try_from(bucket.entries.len()).unwrap_or(i32::MAX))
    }
}

fn arc_allocation_size<T>() -> Option<usize> {
    let (layout, _) = Layout::new::<AtomicUsize>().extend(Layout::new::<AtomicUsize>()).ok()?;
    let (layout, _) = layout.extend(Layout::new::<T>()).ok()?;
    Some(layout.pad_to_align().size())
}

fn checked_retained_layout_sum<const N: usize>(parts: [usize; N]) -> Option<usize> {
    parts
        .into_iter()
        .try_fold(0usize, |total, part| total.checked_add(part))
}

fn candidate_from_bucket<I: Copy>(
    state: &PopCriteriaIndexState<I>,
    key: &PopCriteriaKey,
    order: PopSelectionOrder,
    skipped: &[(PopCriteriaKey, PopOrderKey)],
) -> Option<PopIndexCandidateSnapshot<I>> {
    let bucket = state.buckets.get(key)?;
    let is_available = |record: &&PopIndexRecord<I>| {
        !skipped
            .iter()
            .any(|(skipped_key, skipped_order)| skipped_key == key && *skipped_order == record.order_key())
    };
    let record = match order {
        PopSelectionOrder::Oldest => bucket.entries.iter().find(is_available),
        PopSelectionOrder::Newest => bucket.entries.iter().rev().find(is_available),
    }?;
    Some(PopIndexCandidateSnapshot {
        key: key.clone(),
        id: record.id,
        order: record.order_key(),
        criteria: Arc::clone(&record.criteria),
    })
}

fn append_fanout_groups<I>(
    state: &mut PopCriteriaIndexState<I>,
    key: &PopTopicQueueKey,
    limit: NonZeroUsize,
    groups: &mut Vec<CheetahString>,
) {
    let Some(bucket) = state.fanout.get_mut(key) else {
        return;
    };
    let len = bucket.groups.len();
    if len == 0 {
        return;
    }
    let scan = len.min(limit.get());
    for offset in 0..scan {
        let position = (bucket.cursor + offset) % len;
        let group = &bucket.groups[position];
        if group.live > 0 && !groups.contains(&group.consumer_group) {
            groups.push(group.consumer_group.clone());
        }
    }
    bucket.cursor = (bucket.cursor + scan) % len;
}

fn insert_ordered<I>(entries: &mut VecDeque<PopIndexRecord<I>>, record: PopIndexRecord<I>) {
    let position = entries
        .binary_search_by_key(&record.order_key(), PopIndexRecord::order_key)
        .unwrap_or_else(|position| position);
    entries.insert(position, record);
}

fn bucket_is_empty<I>(bucket: &PopIndexBucket<I>) -> bool {
    bucket.entries.is_empty() && bucket.reserved == 0 && bucket.candidates == 0
}

fn release_fanout_reservation<I>(
    state: &mut PopCriteriaIndexState<I>,
    key: &PopTopicQueueKey,
    consumer_group: &CheetahString,
) {
    release_fanout(state, key, consumer_group, false);
}

fn release_fanout_live<I>(
    state: &mut PopCriteriaIndexState<I>,
    key: &PopTopicQueueKey,
    consumer_group: &CheetahString,
) {
    release_fanout(state, key, consumer_group, true);
}

fn release_fanout<I>(
    state: &mut PopCriteriaIndexState<I>,
    key: &PopTopicQueueKey,
    consumer_group: &CheetahString,
    live: bool,
) {
    let mut remove_bucket = false;
    if let Some(bucket) = state.fanout.get_mut(key) {
        if let Some(position) = bucket
            .groups
            .iter()
            .position(|group| &group.consumer_group == consumer_group)
        {
            let group = &mut bucket.groups[position];
            if live {
                debug_assert!(group.live > 0);
                group.live -= 1;
            } else {
                debug_assert!(group.reserved > 0);
                group.reserved -= 1;
            }
            if group.live == 0 && group.reserved == 0 {
                bucket.groups.remove(position);
                if bucket.groups.is_empty() {
                    bucket.cursor = 0;
                } else {
                    bucket.cursor %= bucket.groups.len();
                }
            }
        }
        remove_bucket = bucket.groups.is_empty();
    }
    if remove_bucket {
        state.fanout.remove(key);
    }
}

struct PopCriteriaIndexInner<I> {
    limits: PopCriteriaLimits,
    state: Mutex<PopCriteriaIndexState<I>>,
}

struct PopCriteriaIndexState<I> {
    buckets: HashMap<PopCriteriaKey, PopIndexBucket<I>>,
    fanout: HashMap<PopTopicQueueKey, PopFanoutBucket>,
    live: usize,
    reserved: usize,
    next_sequence: u64,
    target_cursor: usize,
}

impl<I> Default for PopCriteriaIndexState<I> {
    fn default() -> Self {
        Self {
            buckets: HashMap::new(),
            fanout: HashMap::new(),
            live: 0,
            reserved: 0,
            next_sequence: 0,
            target_cursor: 0,
        }
    }
}

struct PopIndexBucket<I> {
    entries: VecDeque<PopIndexRecord<I>>,
    reserved: usize,
    candidates: usize,
}

impl<I> Default for PopIndexBucket<I> {
    fn default() -> Self {
        Self {
            entries: VecDeque::new(),
            reserved: 0,
            candidates: 0,
        }
    }
}

struct PopIndexRecord<I> {
    id: I,
    deadline: LongPollingDeadline,
    sequence: u64,
    criteria: Arc<PopMatchCriteria>,
    membership: Arc<PopIndexMembership>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PopOrderKey {
    deadline: tokio::time::Instant,
    sequence: u64,
}

impl<I> PopIndexRecord<I> {
    const fn order_key(&self) -> PopOrderKey {
        PopOrderKey {
            deadline: self.deadline.protocol_at(),
            sequence: self.sequence,
        }
    }
}

struct PopIndexCandidateSnapshot<I> {
    key: PopCriteriaKey,
    id: I,
    order: PopOrderKey,
    criteria: Arc<PopMatchCriteria>,
}

#[derive(Default)]
struct PopIndexMembership {
    live: AtomicBool,
    candidate_reserved: AtomicBool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PopTopicQueueKey {
    topic: CheetahString,
    queue_id: i32,
}

impl PopTopicQueueKey {
    fn from_criteria(key: &PopCriteriaKey) -> Self {
        Self {
            topic: key.topic.clone(),
            queue_id: key.queue_id,
        }
    }

    fn from_arrival(topic: &CheetahString, queue_id: i32) -> Self {
        Self {
            topic: topic.clone(),
            queue_id,
        }
    }
}

#[derive(Default)]
struct PopFanoutBucket {
    groups: Vec<PopFanoutGroup>,
    cursor: usize,
}

struct PopFanoutGroup {
    consumer_group: CheetahString,
    live: usize,
    reserved: usize,
}

/// Capacity proof moved into the registry's synchronous builder.
#[must_use]
pub(crate) struct PopIndexReservation<I = DeferredId> {
    inner: Option<Arc<PopCriteriaIndexInner<I>>>,
    key: Option<PopCriteriaKey>,
    fanout_key: Option<PopTopicQueueKey>,
    sequence: u64,
    membership: Arc<PopIndexMembership>,
}

impl<I> PopIndexReservation<I>
where
    I: Copy + Eq,
{
    /// Publishes the already-reserved record without a recoverable failure.
    pub(crate) fn publish(
        mut self,
        id: I,
        deadline: LongPollingDeadline,
        criteria: Arc<PopMatchCriteria>,
    ) -> PopIndexLease<I> {
        let inner = self.inner.take().expect("armed POP index reservation owns its index");
        let key = self.key.take().expect("armed POP index reservation owns its key");
        let fanout_key = self
            .fanout_key
            .take()
            .expect("armed POP index reservation owns its fanout key");
        {
            let mut state = inner.state.lock();
            let bucket = state
                .buckets
                .get_mut(&key)
                .expect("armed POP index reservation retains its bucket");
            debug_assert!(bucket.reserved > 0);
            bucket.reserved -= 1;
            let record = PopIndexRecord {
                id,
                deadline,
                sequence: self.sequence,
                criteria,
                membership: Arc::clone(&self.membership),
            };
            insert_ordered(&mut bucket.entries, record);
            let fanout = state
                .fanout
                .get_mut(&fanout_key)
                .expect("armed POP reservation retains its fanout bucket");
            let group = fanout
                .groups
                .iter_mut()
                .find(|group| group.consumer_group == key.consumer_group)
                .expect("armed POP reservation retains its fanout group");
            debug_assert!(group.reserved > 0);
            group.reserved -= 1;
            group.live += 1;
            state.reserved -= 1;
            state.live += 1;
            self.membership.live.store(true, Ordering::Release);
        }
        PopIndexLease {
            inner: Arc::downgrade(&inner),
            key,
            fanout_key,
            id,
            order: PopOrderKey {
                deadline: deadline.protocol_at(),
                sequence: self.sequence,
            },
            membership: Arc::clone(&self.membership),
        }
    }
}

impl<I> Drop for PopIndexReservation<I> {
    fn drop(&mut self) {
        let (Some(inner), Some(key), Some(fanout_key)) = (self.inner.take(), self.key.take(), self.fanout_key.take())
        else {
            return;
        };
        let mut state = inner.state.lock();
        let remove_bucket = if let Some(bucket) = state.buckets.get_mut(&key) {
            debug_assert!(bucket.reserved > 0);
            bucket.reserved -= 1;
            bucket_is_empty(bucket)
        } else {
            false
        };
        release_fanout_reservation(&mut state, &fanout_key, &key.consumer_group);
        state.reserved -= 1;
        if remove_bucket {
            state.buckets.remove(&key);
        }
    }
}

/// Affine membership owner stored in `ResumePop`.
#[must_use]
pub(crate) struct PopIndexLease<I: Eq = DeferredId> {
    inner: Weak<PopCriteriaIndexInner<I>>,
    key: PopCriteriaKey,
    fanout_key: PopTopicQueueKey,
    id: I,
    order: PopOrderKey,
    membership: Arc<PopIndexMembership>,
}

impl<I> PopIndexLease<I>
where
    I: Copy + Eq,
{
    #[must_use]
    pub(crate) const fn deferred_id(&self) -> I {
        self.id
    }
}

impl<I> Drop for PopIndexLease<I>
where
    I: Eq,
{
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let removed = {
            let mut state = inner.state.lock();
            if !self.membership.live.swap(false, Ordering::AcqRel) {
                return;
            }
            let mut removed = None;
            let mut remove_bucket = false;
            if let Some(bucket) = state.buckets.get_mut(&self.key) {
                if self.membership.candidate_reserved.swap(false, Ordering::AcqRel) {
                    debug_assert!(bucket.candidates > 0);
                    bucket.candidates -= 1;
                } else if let Ok(position) = bucket
                    .entries
                    .binary_search_by_key(&self.order, PopIndexRecord::order_key)
                {
                    debug_assert!(bucket.entries.get(position).is_some_and(|record| record.id == self.id));
                    removed = Some(bucket.entries.remove(position));
                }
                remove_bucket = bucket_is_empty(bucket);
            }
            state.live -= 1;
            release_fanout_live(&mut state, &self.fanout_key, &self.key.consumer_group);
            if remove_bucket {
                state.buckets.remove(&self.key);
            }
            removed
        };
        drop(removed);
    }
}

/// Affine claim opportunity held across registry claim arbitration.
#[must_use]
pub(crate) struct PopCandidateReservation<I: Eq = DeferredId> {
    inner: Weak<PopCriteriaIndexInner<I>>,
    key: PopCriteriaKey,
    record: Option<PopIndexRecord<I>>,
}

/// One bounded selection result and the filter budget it consumed.
#[must_use]
pub(crate) struct PopCandidateSelection<I: Eq = DeferredId> {
    candidate: Option<PopCandidateReservation<I>>,
    inspected: usize,
}

impl<I> PopCandidateSelection<I>
where
    I: Eq,
{
    const fn new(candidate: Option<PopCandidateReservation<I>>, inspected: usize) -> Self {
        Self { candidate, inspected }
    }

    #[must_use]
    pub(crate) const fn inspected(&self) -> usize {
        self.inspected
    }

    pub(crate) fn into_candidate(self) -> Option<PopCandidateReservation<I>> {
        self.candidate
    }
}

impl<I> PopCandidateReservation<I>
where
    I: Copy + Eq,
{
    #[must_use]
    pub(crate) fn id(&self) -> I {
        self.record.as_ref().expect("live POP candidate owns its record").id
    }

    #[must_use]
    pub(crate) const fn key(&self) -> &PopCriteriaKey {
        &self.key
    }

    pub(crate) fn criteria(&self) -> &Arc<PopMatchCriteria> {
        &self
            .record
            .as_ref()
            .expect("live POP candidate owns its record")
            .criteria
    }
}

fn next_fanout_group<I>(
    state: &PopCriteriaIndexState<I>,
    exact: &PopTopicQueueKey,
    wildcard: &PopTopicQueueKey,
    after: Option<&CheetahString>,
) -> Option<CheetahString> {
    let exact_groups = state
        .fanout
        .get(exact)
        .into_iter()
        .flat_map(|bucket| bucket.groups.iter());
    let wildcard_groups = (wildcard != exact)
        .then(|| state.fanout.get(wildcard))
        .flatten()
        .into_iter()
        .flat_map(|bucket| bucket.groups.iter());
    exact_groups
        .chain(wildcard_groups)
        .filter(|group| group.live > 0)
        .map(|group| &group.consumer_group)
        .filter(|group| after.is_none_or(|after| *group > after))
        .min()
        .cloned()
}

impl<I> Drop for PopCandidateReservation<I>
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
        let Some(bucket) = state.buckets.get_mut(&self.key) else {
            debug_assert!(!record.membership.live.load(Ordering::Acquire));
            return;
        };
        if !record.membership.candidate_reserved.swap(false, Ordering::AcqRel) {
            debug_assert!(!record.membership.live.load(Ordering::Acquire));
            return;
        }
        debug_assert!(bucket.candidates > 0);
        bucket.candidates -= 1;
        if record.membership.live.load(Ordering::Acquire) {
            insert_ordered(&mut bucket.entries, record);
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PopIndexSnapshot {
    live: usize,
    reserved: usize,
    buckets: usize,
    candidates: usize,
}

impl PopIndexSnapshot {
    #[must_use]
    pub(crate) const fn live(self) -> usize {
        self.live
    }

    #[must_use]
    pub(crate) const fn reserved(self) -> usize {
        self.reserved
    }

    #[must_use]
    pub(crate) const fn buckets(self) -> usize {
        self.buckets
    }

    #[must_use]
    pub(crate) const fn candidates(self) -> usize {
        self.candidates
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PopIndexErrorKind {
    GlobalCapacity,
    BucketCapacity,
    SequenceExhausted,
    Allocation,
}

impl PopIndexErrorKind {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::GlobalCapacity => "global_capacity",
            Self::BucketCapacity => "bucket_capacity",
            Self::SequenceExhausted => "sequence_exhausted",
            Self::Allocation => "allocation",
        }
    }
}

pub(crate) struct PopIndexError {
    kind: PopIndexErrorKind,
}

impl PopIndexError {
    const fn new(kind: PopIndexErrorKind) -> Self {
        Self { kind }
    }

    #[must_use]
    pub(crate) const fn kind(&self) -> PopIndexErrorKind {
        self.kind
    }
}

impl fmt::Debug for PopIndexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PopIndexError")
            .field("kind", &self.kind.as_str())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for PopIndexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "POP criteria index failed: {}", self.kind.as_str())
    }
}

impl Error for PopIndexError {}

#[cfg(test)]
#[path = "../../../tests/unit/long_polling/pop_deferred/index/p1_tests.rs"]
mod p1_tests;
