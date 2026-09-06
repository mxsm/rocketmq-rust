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

use std::collections::HashMap;
use std::collections::TryReserveError;
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicU8;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_model::common::key_builder::KeyBuilder;
use rocketmq_model::common::key_builder::POP_RETRY_SEPARATOR_V2;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_store::ArcMessageFilter;
use rocketmq_store::CqExtUnit;
use rocketmq_transport::api::DeferredId;

use super::deadline::NotificationWaitDeadline;

const INDEXED: u8 = 0;
const CANDIDATE: u8 = 1;
const REMOVED: u8 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NotificationCriteriaLimits {
    max_entries: NonZeroUsize,
    max_entries_per_key: NonZeroUsize,
    allocation_hint: usize,
}

impl NotificationCriteriaLimits {
    pub(crate) fn new(max_entries: NonZeroUsize, legacy_pop_polling_size: usize, allocation_hint: usize) -> Self {
        // Legacy rejects only when `queue.len() > pop_polling_size`.
        let compatible_per_key = legacy_pop_polling_size.saturating_add(1);
        Self {
            max_entries,
            max_entries_per_key: NonZeroUsize::new(compatible_per_key).unwrap_or(NonZeroUsize::MIN),
            allocation_hint,
        }
    }

    #[cfg(test)]
    pub(crate) const fn direct(max_entries: NonZeroUsize, max_entries_per_key: NonZeroUsize) -> Self {
        Self {
            max_entries,
            max_entries_per_key,
            allocation_hint: 0,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct NotificationCriteriaKey {
    topic: CheetahString,
    consumer_group: CheetahString,
    queue_id: i32,
}

impl NotificationCriteriaKey {
    pub(crate) fn new(topic: CheetahString, consumer_group: CheetahString, queue_id: i32) -> Self {
        Self {
            topic,
            consumer_group,
            queue_id,
        }
    }

    pub(crate) fn from_parts(topic: &CheetahString, consumer_group: &CheetahString, queue_id: i32) -> Self {
        Self::new(topic.clone(), consumer_group.clone(), queue_id)
    }

    #[must_use]
    pub(crate) const fn topic(&self) -> &CheetahString {
        &self.topic
    }

    #[must_use]
    pub(crate) const fn consumer_group(&self) -> &CheetahString {
        &self.consumer_group
    }

    #[must_use]
    pub(crate) const fn queue_id(&self) -> i32 {
        self.queue_id
    }
}

/// Borrowed message-arrival metadata. It cannot escape the producer callback.
#[derive(Clone, Copy)]
pub(crate) struct NotificationArrivalView<'a> {
    pub(super) topic: &'a CheetahString,
    pub(super) queue_id: i32,
    pub(super) tags_code: Option<i64>,
    pub(super) msg_store_time: i64,
    pub(super) filter_bit_map: Option<&'a [u8]>,
    pub(super) properties: Option<&'a HashMap<CheetahString, CheetahString>>,
    pub(super) forced: bool,
}

impl<'a> NotificationArrivalView<'a> {
    pub(crate) const fn new(topic: &'a CheetahString, queue_id: i32) -> Self {
        Self {
            topic,
            queue_id,
            tags_code: None,
            msg_store_time: 0,
            filter_bit_map: None,
            properties: None,
            forced: false,
        }
    }

    #[must_use]
    pub(crate) const fn with_filter_metadata(
        mut self,
        tags_code: Option<i64>,
        msg_store_time: i64,
        filter_bit_map: Option<&'a [u8]>,
        properties: Option<&'a HashMap<CheetahString, CheetahString>>,
    ) -> Self {
        self.tags_code = tags_code;
        self.msg_store_time = msg_store_time;
        self.filter_bit_map = filter_bit_map;
        self.properties = properties;
        self
    }

    #[must_use]
    pub(crate) const fn forced(mut self) -> Self {
        self.forced = true;
        self
    }

    #[must_use]
    pub(crate) const fn topic(&self) -> &CheetahString {
        self.topic
    }

    #[must_use]
    pub(crate) const fn queue_id(&self) -> i32 {
        self.queue_id
    }

    pub(super) fn cq_ext_unit(&self) -> CqExtUnit {
        CqExtUnit::new(
            self.tags_code.unwrap_or_default(),
            self.msg_store_time,
            self.filter_bit_map.map(<[u8]>::to_vec),
        )
    }
}

/// Immutable subscription/filter snapshot shared by index and resume data.
pub(crate) struct NotificationMatchCriteria {
    subscription: Option<SubscriptionData>,
    filter: Option<ArcMessageFilter>,
}

impl NotificationMatchCriteria {
    pub(crate) fn new(subscription: Option<SubscriptionData>, filter: Option<ArcMessageFilter>) -> Self {
        Self { subscription, filter }
    }

    #[must_use]
    pub(crate) const fn subscription(&self) -> Option<&SubscriptionData> {
        self.subscription.as_ref()
    }

    #[must_use]
    pub(crate) const fn filter(&self) -> Option<&ArcMessageFilter> {
        self.filter.as_ref()
    }

    pub(super) fn matches(&self, arrival: &NotificationArrivalView<'_>, cq: &CqExtUnit) -> bool {
        if arrival.forced {
            return true;
        }
        let Some(filter) = self.filter.as_ref() else {
            return true;
        };
        if !filter.is_matched_by_consume_queue(arrival.tags_code, Some(cq)) {
            return false;
        }
        arrival
            .properties
            .is_none_or(|properties| filter.is_matched_by_commit_log(None, Some(properties)))
    }
}

/// Frozen fanout and newest-first position for one arrival.
#[must_use]
pub(crate) struct NotificationScanCursor {
    keys: Vec<NotificationKeyCursor>,
    key_position: usize,
    conflicts_spent: usize,
}

impl NotificationScanCursor {
    pub(super) const fn empty() -> Self {
        Self {
            keys: Vec::new(),
            key_position: 0,
            conflicts_spent: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(remaining_keys: usize) -> Self {
        let key = NotificationCriteriaKey::new(
            CheetahString::from_static_str("test-topic"),
            CheetahString::from_static_str("test-group"),
            0,
        );
        Self {
            keys: (0..remaining_keys)
                .map(|_| NotificationKeyCursor {
                    key: key.clone(),
                    before_sequence: u64::MAX,
                })
                .collect(),
            key_position: 0,
            conflicts_spent: 0,
        }
    }

    #[must_use]
    pub(crate) fn is_complete(&self) -> bool {
        self.key_position >= self.keys.len()
    }

    pub(crate) fn retain_remaining_targets(&mut self, mut retain: impl FnMut(&NotificationCriteriaKey) -> bool) {
        if self.key_position > 0 {
            self.keys.drain(..self.key_position);
            self.key_position = 0;
        }
        self.keys.retain(|key| retain(&key.key));
    }

    pub(super) fn try_retained_bytes(&self) -> Option<usize> {
        let mut bytes = self
            .keys
            .capacity()
            .checked_mul(std::mem::size_of::<NotificationKeyCursor>())?;
        for cursor in &self.keys {
            bytes = bytes
                .checked_add(cursor.key.topic.len())?
                .checked_add(cursor.key.consumer_group.len())?;
        }
        Some(bytes)
    }

    pub(super) fn restart_for_refresh(&mut self) {
        self.key_position = 0;
        self.conflicts_spent = 0;
        for key in &mut self.keys {
            key.before_sequence = u64::MAX;
        }
    }

    fn current(&self) -> Option<&NotificationKeyCursor> {
        self.keys.get(self.key_position)
    }

    fn advance_before(&mut self, sequence: u64) {
        if let Some(key) = self.keys.get_mut(self.key_position) {
            key.before_sequence = sequence;
        }
    }

    pub(super) fn advance_key(&mut self) {
        self.key_position = self.key_position.saturating_add(1);
    }

    pub(super) fn record_conflict(&mut self) {
        self.conflicts_spent = self.conflicts_spent.saturating_add(1);
    }

    pub(super) const fn conflicts_spent(&self) -> usize {
        self.conflicts_spent
    }
}

struct NotificationKeyCursor {
    key: NotificationCriteriaKey,
    before_sequence: u64,
}

pub(crate) struct NotificationCriteriaIndex<I = DeferredId> {
    inner: Arc<NotificationCriteriaIndexInner<I>>,
}

impl<I> Clone for NotificationCriteriaIndex<I> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<I> NotificationCriteriaIndex<I>
where
    I: Copy + Eq,
{
    pub(crate) fn new(limits: NotificationCriteriaLimits) -> Self {
        let state = NotificationIndexState::with_hint(limits.allocation_hint);
        Self {
            inner: Arc::new(NotificationCriteriaIndexInner {
                limits,
                state: Mutex::new(state),
                #[cfg(test)]
                forced_conflicts: AtomicUsize::new(0),
            }),
        }
    }

    pub(crate) const fn retained_bytes_per_entry() -> usize {
        std::mem::size_of::<NotificationRecord<I>>()
            + std::mem::size_of::<NotificationCriteriaKey>()
            + std::mem::size_of::<NotificationIndexLease<I>>()
    }

    pub(crate) fn reserve(
        &self,
        key: NotificationCriteriaKey,
    ) -> Result<NotificationIndexReserveOutcome<I>, NotificationIndexOperationalError> {
        self.reserve_at(key, tokio::time::Instant::now())
    }

    pub(crate) fn reserve_at(
        &self,
        key: NotificationCriteriaKey,
        admitted_at: tokio::time::Instant,
    ) -> Result<NotificationIndexReserveOutcome<I>, NotificationIndexOperationalError> {
        let mut state = self.inner.state.lock();
        let occupied = state
            .live
            .checked_add(state.reserved)
            .ok_or(NotificationIndexOperationalError::AccountingOverflow)?;
        if occupied >= self.inner.limits.max_entries.get() {
            return Ok(NotificationIndexReserveOutcome::Rejected(
                NotificationIndexReserveRejection::GlobalCapacity,
            ));
        }
        let occupied = match state.buckets.get(&key) {
            Some(bucket) => bucket
                .records
                .len()
                .checked_add(bucket.reserved)
                .and_then(|occupied| occupied.checked_add(bucket.candidates))
                .ok_or(NotificationIndexOperationalError::AccountingOverflow)?,
            None => 0,
        };
        if occupied >= self.inner.limits.max_entries_per_key.get() {
            return Ok(NotificationIndexReserveOutcome::Rejected(
                NotificationIndexReserveRejection::PerKeyCapacity,
            ));
        }
        let sequence = state
            .next_sequence
            .checked_add(1)
            .ok_or(NotificationIndexOperationalError::SequenceExhausted)?;
        let bucket_missing = !state.buckets.contains_key(&key);
        if bucket_missing {
            state
                .buckets
                .try_reserve(1)
                .map_err(NotificationIndexOperationalError::Allocation)?;
        }
        let mut new_bucket = bucket_missing.then(NotificationBucket::default);
        if let Some(bucket) = new_bucket.as_mut() {
            bucket
                .records
                .try_reserve(1)
                .map_err(NotificationIndexOperationalError::Allocation)?;
        } else {
            state
                .buckets
                .get_mut(&key)
                .expect("existing Notification bucket remains present")
                .records
                .try_reserve(1)
                .map_err(NotificationIndexOperationalError::Allocation)?;
        }
        if let Some(bucket) = new_bucket {
            state.buckets.insert(key.clone(), bucket);
        }
        let bucket = state
            .buckets
            .get_mut(&key)
            .expect("reserved Notification bucket exists");
        bucket.reserved += 1;
        state.reserved += 1;
        state.next_sequence = sequence;
        drop(state);
        Ok(NotificationIndexReserveOutcome::Reserved(
            NotificationIndexReservation {
                inner: Some(Arc::clone(&self.inner)),
                key: Some(key),
                sequence,
                admitted_at,
            },
        ))
    }

    /// Freezes groups, wildcard-before-exact key order, and sequence ceilings.
    pub(crate) fn scan_cursor(&self, arrival: &NotificationArrivalView<'_>) -> NotificationScanCursor {
        let normalized = if KeyBuilder::is_pop_retry_topic_v2(arrival.topic.as_str()) {
            let normal_topic = arrival
                .topic
                .as_str()
                .split_once(POP_RETRY_SEPARATOR_V2)
                .map_or(arrival.topic.as_str(), |(_, topic)| topic);
            CheetahString::from_slice(normal_topic)
        } else {
            arrival.topic.clone()
        };
        let state = self.inner.state.lock();
        let mut groups = state
            .buckets
            .keys()
            .filter(|key| key.topic == normalized && (key.queue_id == -1 || key.queue_id == arrival.queue_id))
            .map(|key| key.consumer_group.clone())
            .collect::<Vec<_>>();
        groups.sort_unstable();
        groups.dedup();
        let mut keys = Vec::with_capacity(groups.len().saturating_mul(2));
        for group in groups {
            for queue_id in [-1, arrival.queue_id] {
                if queue_id == -1
                    && arrival.queue_id == -1
                    && keys.last().is_some_and(|cursor: &NotificationKeyCursor| {
                        cursor.key.topic == normalized
                            && cursor.key.consumer_group == group
                            && cursor.key.queue_id == -1
                    })
                {
                    continue;
                }
                let key = NotificationCriteriaKey::from_parts(&normalized, &group, queue_id);
                let Some(bucket) = state.buckets.get(&key) else {
                    continue;
                };
                let before_sequence = bucket
                    .records
                    .last()
                    .map_or(state.next_sequence.saturating_add(1), |record| {
                        record.sequence.saturating_add(1)
                    });
                keys.push(NotificationKeyCursor { key, before_sequence });
            }
        }
        NotificationScanCursor {
            keys,
            key_position: 0,
            conflicts_spent: 0,
        }
    }

    pub(crate) fn has_arrival_target(&self, topic: &CheetahString, queue_id: i32) -> bool {
        let normalized = if KeyBuilder::is_pop_retry_topic_v2(topic.as_str()) {
            topic
                .as_str()
                .split_once(POP_RETRY_SEPARATOR_V2)
                .map_or_else(|| topic.clone(), |(_, normal)| CheetahString::from_slice(normal))
        } else {
            topic.clone()
        };
        self.inner.state.lock().buckets.iter().any(|(key, bucket)| {
            key.topic == normalized && (key.queue_id == -1 || key.queue_id == queue_id) && !bucket.records.is_empty()
        })
    }

    /// Reserves one newest candidate without retaining arrival payload.
    pub(crate) fn reserve_next(&self, cursor: &mut NotificationScanCursor) -> NotificationCandidateSelection<I> {
        loop {
            let Some(current) = cursor.current() else {
                return NotificationCandidateSelection::Complete;
            };
            #[cfg(test)]
            if self
                .inner
                .forced_conflicts
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return NotificationCandidateSelection::Conflict;
            }
            let snapshot = {
                let state = self.inner.state.lock();
                state.buckets.get(&current.key).and_then(|bucket| {
                    bucket
                        .records
                        .iter()
                        .rev()
                        .find(|record| record.sequence < current.before_sequence)
                        .map(|record| (record.id, record.sequence))
                })
            };
            let Some((id, sequence)) = snapshot else {
                cursor.advance_key();
                continue;
            };
            let mut state = self.inner.state.lock();
            let Some(bucket) = state.buckets.get_mut(&current.key) else {
                return NotificationCandidateSelection::Conflict;
            };
            let Some(position) = bucket
                .records
                .iter()
                .position(|record| record.id == id && record.sequence == sequence)
            else {
                return NotificationCandidateSelection::Conflict;
            };
            let record = bucket.records.remove(position);
            record.membership.store(CANDIDATE, Ordering::Release);
            bucket.candidates += 1;
            let candidate_key = current.key.clone();
            drop(state);
            cursor.advance_before(sequence);
            return NotificationCandidateSelection::Candidate(NotificationCandidateReservation {
                inner: Arc::downgrade(&self.inner),
                key: candidate_key,
                record: Some(record),
            });
        }
    }

    #[must_use]
    pub(crate) fn snapshot(&self) -> NotificationIndexSnapshot {
        self.snapshot_at(tokio::time::Instant::now())
    }

    fn snapshot_at(&self, now: tokio::time::Instant) -> NotificationIndexSnapshot {
        let state = self.inner.state.lock();
        NotificationIndexSnapshot {
            live: state.live,
            reserved: state.reserved,
            candidates: state.buckets.values().map(|bucket| bucket.candidates).sum(),
            keys: state.buckets.len(),
            oldest_waiter_age_millis: state
                .buckets
                .values()
                .flat_map(|bucket| bucket.records.iter())
                .map(|record| now.saturating_duration_since(record.admitted_at).as_millis())
                .max()
                .and_then(|age| u64::try_from(age).ok()),
        }
    }

    #[cfg(test)]
    pub(crate) fn snapshot_at_for_test(&self, now: tokio::time::Instant) -> NotificationIndexSnapshot {
        self.snapshot_at(now)
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, id: I) -> bool {
        self.inner
            .state
            .lock()
            .buckets
            .values()
            .any(|bucket| bucket.records.iter().any(|record| record.id == id))
    }

    #[cfg(test)]
    pub(crate) fn force_conflicts(&self, count: usize) {
        self.inner.forced_conflicts.store(count, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn forced_conflicts_remaining(&self) -> usize {
        self.inner.forced_conflicts.load(Ordering::Acquire)
    }
}

struct NotificationCriteriaIndexInner<I> {
    limits: NotificationCriteriaLimits,
    state: Mutex<NotificationIndexState<I>>,
    #[cfg(test)]
    forced_conflicts: AtomicUsize,
}

struct NotificationIndexState<I> {
    buckets: HashMap<NotificationCriteriaKey, NotificationBucket<I>>,
    live: usize,
    reserved: usize,
    next_sequence: u64,
}

impl<I> NotificationIndexState<I> {
    fn with_hint(hint: usize) -> Self {
        Self {
            buckets: HashMap::with_capacity(hint),
            live: 0,
            reserved: 0,
            next_sequence: 0,
        }
    }
}

struct NotificationBucket<I> {
    records: Vec<NotificationRecord<I>>,
    reserved: usize,
    candidates: usize,
}

impl<I> Default for NotificationBucket<I> {
    fn default() -> Self {
        Self {
            records: Vec::new(),
            reserved: 0,
            candidates: 0,
        }
    }
}

struct NotificationRecord<I> {
    id: I,
    sequence: u64,
    _deadline: NotificationWaitDeadline,
    admitted_at: tokio::time::Instant,
    criteria: Arc<NotificationMatchCriteria>,
    membership: Arc<AtomicU8>,
}

#[must_use]
pub(crate) struct NotificationIndexReservation<I = DeferredId> {
    inner: Option<Arc<NotificationCriteriaIndexInner<I>>>,
    key: Option<NotificationCriteriaKey>,
    sequence: u64,
    admitted_at: tokio::time::Instant,
}

impl<I> NotificationIndexReservation<I>
where
    I: Copy + Eq,
{
    pub(crate) fn publish(
        mut self,
        id: I,
        deadline: NotificationWaitDeadline,
        criteria: Arc<NotificationMatchCriteria>,
    ) -> NotificationIndexLease<I> {
        let inner = self
            .inner
            .take()
            .expect("Notification index reservation owns its index");
        let key = self.key.take().expect("Notification index reservation owns its key");
        let membership = Arc::new(AtomicU8::new(INDEXED));
        let mut state = inner.state.lock();
        debug_assert!(state.reserved > 0);
        state.reserved = state.reserved.saturating_sub(1);
        state.live = state.live.saturating_add(1);
        {
            let bucket = state.buckets.entry(key.clone()).or_default();
            debug_assert!(bucket.reserved > 0);
            bucket.reserved = bucket.reserved.saturating_sub(1);
            bucket.records.push(NotificationRecord {
                id,
                sequence: self.sequence,
                _deadline: deadline,
                admitted_at: self.admitted_at,
                criteria,
                membership: Arc::clone(&membership),
            });
        }
        drop(state);
        NotificationIndexLease {
            inner: Arc::downgrade(&inner),
            key: Some(key),
            id,
            membership,
        }
    }
}

impl<I> Drop for NotificationIndexReservation<I> {
    fn drop(&mut self) {
        let (Some(inner), Some(key)) = (self.inner.take(), self.key.take()) else {
            return;
        };
        let mut state = inner.state.lock();
        if let Some(bucket) = state.buckets.get_mut(&key) {
            bucket.reserved = bucket.reserved.saturating_sub(1);
        }
        state.reserved = state.reserved.saturating_sub(1);
        let remove = state
            .buckets
            .get(&key)
            .is_some_and(|bucket| bucket.records.is_empty() && bucket.reserved == 0 && bucket.candidates == 0);
        if remove {
            state.buckets.remove(&key);
        }
    }
}

#[must_use]
pub(crate) struct NotificationIndexLease<I: Eq = DeferredId> {
    inner: Weak<NotificationCriteriaIndexInner<I>>,
    key: Option<NotificationCriteriaKey>,
    id: I,
    membership: Arc<AtomicU8>,
}

impl<I> Drop for NotificationIndexLease<I>
where
    I: Eq,
{
    fn drop(&mut self) {
        let previous = self.membership.swap(REMOVED, Ordering::AcqRel);
        if previous != INDEXED {
            return;
        }
        let (Some(inner), Some(key)) = (self.inner.upgrade(), self.key.take()) else {
            return;
        };
        let mut state = inner.state.lock();
        let mut removed = false;
        if let Some(bucket) = state.buckets.get_mut(&key) {
            if let Some(position) = bucket.records.iter().position(|record| record.id == self.id) {
                bucket.records.remove(position);
                removed = true;
            }
        }
        if removed {
            state.live = state.live.saturating_sub(1);
        }
        let remove = state
            .buckets
            .get(&key)
            .is_some_and(|bucket| bucket.records.is_empty() && bucket.reserved == 0 && bucket.candidates == 0);
        if remove {
            state.buckets.remove(&key);
        }
    }
}

pub(crate) enum NotificationCandidateSelection<I = DeferredId> {
    Candidate(NotificationCandidateReservation<I>),
    Conflict,
    Complete,
}

#[must_use]
pub(crate) struct NotificationCandidateReservation<I = DeferredId> {
    inner: Weak<NotificationCriteriaIndexInner<I>>,
    key: NotificationCriteriaKey,
    record: Option<NotificationRecord<I>>,
}

impl<I> NotificationCandidateReservation<I>
where
    I: Copy,
{
    #[must_use]
    pub(crate) fn id(&self) -> I {
        self.record.as_ref().expect("candidate owns its record").id
    }

    #[must_use]
    pub(crate) fn sequence(&self) -> u64 {
        self.record.as_ref().expect("candidate owns its record").sequence
    }

    #[must_use]
    pub(crate) fn criteria(&self) -> &Arc<NotificationMatchCriteria> {
        &self.record.as_ref().expect("candidate owns its record").criteria
    }

    #[must_use]
    pub(crate) const fn key(&self) -> &NotificationCriteriaKey {
        &self.key
    }
}

impl<I> Drop for NotificationCandidateReservation<I> {
    fn drop(&mut self) {
        let Some(record) = self.record.take() else {
            return;
        };
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut state = inner.state.lock();
        let restored = record
            .membership
            .compare_exchange(CANDIDATE, INDEXED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        {
            let bucket = state.buckets.entry(self.key.clone()).or_default();
            bucket.candidates = bucket.candidates.saturating_sub(1);
            if restored {
                let position = bucket
                    .records
                    .binary_search_by_key(&record.sequence, |record| record.sequence)
                    .unwrap_or_else(|position| position);
                bucket.records.insert(position, record);
            }
        }
        if !restored {
            state.live = state.live.saturating_sub(1);
        }
        let remove = state
            .buckets
            .get(&self.key)
            .is_some_and(|bucket| bucket.records.is_empty() && bucket.reserved == 0 && bucket.candidates == 0);
        if remove {
            state.buckets.remove(&self.key);
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct NotificationIndexSnapshot {
    live: usize,
    reserved: usize,
    candidates: usize,
    keys: usize,
    oldest_waiter_age_millis: Option<u64>,
}

impl NotificationIndexSnapshot {
    #[must_use]
    pub(crate) const fn live(self) -> usize {
        self.live
    }

    #[must_use]
    pub(crate) const fn reserved(self) -> usize {
        self.reserved
    }

    #[must_use]
    pub(crate) const fn candidates(self) -> usize {
        self.candidates
    }

    #[must_use]
    pub(crate) const fn keys(self) -> usize {
        self.keys
    }

    #[must_use]
    pub(crate) const fn oldest_waiter_age_millis(self) -> Option<u64> {
        self.oldest_waiter_age_millis
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum NotificationIndexReserveRejection {
    GlobalCapacity,
    PerKeyCapacity,
}

#[must_use]
pub(crate) enum NotificationIndexReserveOutcome<I> {
    Reserved(NotificationIndexReservation<I>),
    Rejected(NotificationIndexReserveRejection),
}

pub(crate) enum NotificationIndexOperationalError {
    AccountingOverflow,
    SequenceExhausted,
    Allocation(TryReserveError),
}

impl fmt::Debug for NotificationIndexOperationalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AccountingOverflow => "NotificationIndexOperationalError::AccountingOverflow",
            Self::SequenceExhausted => "NotificationIndexOperationalError::SequenceExhausted",
            Self::Allocation(_) => "NotificationIndexOperationalError::Allocation",
        })
    }
}

impl fmt::Display for NotificationIndexOperationalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AccountingOverflow => "Notification criteria index accounting overflowed",
            Self::SequenceExhausted => "Notification criteria index sequence exhausted",
            Self::Allocation(_) => "Notification criteria index allocation failed",
        })
    }
}

impl Error for NotificationIndexOperationalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Allocation(source) => Some(source),
            Self::AccountingOverflow | Self::SequenceExhausted => None,
        }
    }
}
