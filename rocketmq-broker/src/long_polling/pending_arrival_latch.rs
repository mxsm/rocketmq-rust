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
use std::hash::Hash;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;

pub(crate) trait PendingArrivalValue {
    fn retained_bytes(&self) -> usize;

    /// Rewinds a partially consumed replay so the next producer tick starts
    /// from the first candidate without requiring another Store arrival.
    fn rewind_from_start(&mut self);

    /// Coalesces another arrival for the same target into a conservative
    /// refresh. The refresh may inspect more waiters, but registry claim
    /// ownership still prevents duplicate canonical resumes.
    fn coalesce_refresh(&mut self);
}

pub(crate) struct PendingArrivalLatch<K, V> {
    limits: PendingArrivalLimits,
    closed: AtomicBool,
    state: Mutex<PendingArrivalState<K, V>>,
}

impl<K, V> PendingArrivalLatch<K, V>
where
    K: Clone + Eq + Hash,
    V: PendingArrivalValue,
{
    pub(crate) fn new(max_count: usize, max_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            limits: PendingArrivalLimits {
                max_count: max_count.max(1),
                max_bytes: max_bytes.max(1),
            },
            closed: AtomicBool::new(false),
            state: Mutex::new(PendingArrivalState::default()),
        })
    }

    /// Avoids copying callback metadata when a signal for this target is
    /// already retained. An active replay observes `dirty` before completing;
    /// an inactive replay is compacted to a target refresh immediately.
    pub(crate) fn coalesce_existing(&self, key: &K) -> bool {
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        let mut state = self.state.lock();
        if state.closed {
            return false;
        }
        let Some(entry) = state.entries.get_mut(key) else {
            return false;
        };
        if entry.active {
            entry.dirty = true;
            return true;
        }
        let previous = entry.charged_bytes;
        let value = entry
            .value
            .as_mut()
            .expect("inactive pending arrival retains its value");
        value.coalesce_refresh();
        let current = value.retained_bytes().max(1);
        entry.charged_bytes = current;
        state.bytes = state.bytes.saturating_sub(previous).saturating_add(current);
        true
    }

    pub(crate) fn insert(self: &Arc<Self>, key: K, value: V) -> Result<(), PendingArrivalInsertError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PendingArrivalInsertError::Closed);
        }
        let mut state = self.state.lock();
        if state.closed {
            return Err(PendingArrivalInsertError::Closed);
        }
        if let Some(entry) = state.entries.get_mut(&key) {
            if entry.active {
                entry.dirty = true;
            } else {
                let previous = entry.charged_bytes;
                let retained = {
                    let current = entry
                        .value
                        .as_mut()
                        .expect("inactive pending arrival retains its value");
                    current.coalesce_refresh();
                    current.retained_bytes().max(1)
                };
                entry.charged_bytes = retained;
                state.bytes = state.bytes.saturating_sub(previous).saturating_add(retained);
            }
            return Ok(());
        }
        if state.entries.len() >= self.limits.max_count {
            state.rejected = state.rejected.saturating_add(1);
            return Err(PendingArrivalInsertError::CountFull);
        }
        let retained = value.retained_bytes().max(1);
        let Some(next_bytes) = state.bytes.checked_add(retained) else {
            state.rejected = state.rejected.saturating_add(1);
            return Err(PendingArrivalInsertError::SizeOverflow);
        };
        if next_bytes > self.limits.max_bytes {
            state.rejected = state.rejected.saturating_add(1);
            return Err(PendingArrivalInsertError::BytesFull);
        }
        state.bytes = next_bytes;
        state.entries.insert(
            key,
            PendingArrivalEntry {
                value: Some(value),
                charged_bytes: retained,
                active: false,
                dirty: false,
            },
        );
        Ok(())
    }

    pub(crate) fn reserve_batch(self: &Arc<Self>, limit: usize) -> Vec<PendingArrivalReservation<K, V>> {
        if limit == 0 || self.closed.load(Ordering::Acquire) {
            return Vec::new();
        }
        let mut state = self.state.lock();
        if state.closed {
            return Vec::new();
        }
        let keys = state
            .entries
            .iter_mut()
            .filter_map(|(key, entry)| {
                if entry.active || entry.value.is_none() {
                    None
                } else {
                    entry.active = true;
                    Some(key.clone())
                }
            })
            .take(limit)
            .collect::<Vec<_>>();
        drop(state);
        keys.into_iter()
            .map(|key| PendingArrivalReservation {
                latch: Arc::clone(self),
                key: Some(key),
            })
            .collect()
    }

    pub(crate) fn seal(&self) {
        self.closed.store(true, Ordering::Release);
        let mut state = self.state.lock();
        state.closed = true;
        state.entries.clear();
        state.bytes = 0;
    }

    pub(crate) fn snapshot(&self) -> PendingArrivalSnapshot {
        let state = self.state.lock();
        PendingArrivalSnapshot {
            count: state.entries.len(),
            bytes: state.bytes,
            rejected: state.rejected,
        }
    }

    fn release_reservation(&self, key: &K) {
        let mut state = self.state.lock();
        let Some(entry) = state.entries.get_mut(key) else {
            return;
        };
        if entry.value.is_none() {
            return;
        }
        entry.active = false;
        if entry.dirty {
            let previous = entry.charged_bytes;
            let value = entry
                .value
                .as_mut()
                .expect("reserved pending arrival retains its value");
            value.coalesce_refresh();
            let current = value.retained_bytes().max(1);
            entry.charged_bytes = current;
            entry.dirty = false;
            state.bytes = state.bytes.saturating_sub(previous).saturating_add(current);
        }
    }
}

struct PendingArrivalLimits {
    max_count: usize,
    max_bytes: usize,
}

struct PendingArrivalState<K, V> {
    entries: HashMap<K, PendingArrivalEntry<V>>,
    bytes: usize,
    rejected: usize,
    closed: bool,
}

impl<K, V> Default for PendingArrivalState<K, V> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            bytes: 0,
            rejected: 0,
            closed: false,
        }
    }
}

struct PendingArrivalEntry<V> {
    value: Option<V>,
    charged_bytes: usize,
    active: bool,
    dirty: bool,
}

#[must_use]
pub(crate) struct PendingArrivalReservation<K, V>
where
    K: Clone + Eq + Hash,
    V: PendingArrivalValue,
{
    latch: Arc<PendingArrivalLatch<K, V>>,
    key: Option<K>,
}

impl<K, V> PendingArrivalReservation<K, V>
where
    K: Clone + Eq + Hash,
    V: PendingArrivalValue,
{
    pub(crate) fn claim(mut self) -> Option<PendingArrivalClaim<K, V>> {
        let key = self.key.as_ref()?;
        let mut state = self.latch.state.lock();
        if state.closed {
            return None;
        }
        let entry = state.entries.get_mut(key)?;
        if !entry.active {
            return None;
        }
        let value = entry.value.take()?;
        let key = self.key.take().expect("claimed reservation retains its key");
        drop(state);
        Some(PendingArrivalClaim {
            latch: Arc::clone(&self.latch),
            key,
            value: Some(value),
            complete: false,
        })
    }
}

impl<K, V> Drop for PendingArrivalReservation<K, V>
where
    K: Clone + Eq + Hash,
    V: PendingArrivalValue,
{
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.latch.release_reservation(&key);
        }
    }
}

#[must_use]
pub(crate) struct PendingArrivalClaim<K, V>
where
    K: Clone + Eq + Hash,
    V: PendingArrivalValue,
{
    latch: Arc<PendingArrivalLatch<K, V>>,
    key: K,
    value: Option<V>,
    complete: bool,
}

impl<K, V> PendingArrivalClaim<K, V>
where
    K: Clone + Eq + Hash,
    V: PendingArrivalValue,
{
    pub(crate) fn value_mut(&mut self) -> &mut V {
        self.value.as_mut().expect("active pending claim retains its value")
    }

    /// Rearms this exact pending value after routing observes a Legacy target.
    ///
    /// Drop returns the value and its original admission ownership to the
    /// latch; rewinding here prevents a cursor that already passed the Legacy
    /// waiter from deleting the event on the next producer tick.
    pub(crate) fn rearm_from_start(&mut self) {
        self.value_mut().rewind_from_start();
    }

    /// Returns true only after atomically proving that no arrival was merged
    /// while this replay was active. A dirty claim is compacted and restarted.
    pub(crate) fn finish_if_clean(&mut self) -> bool {
        let mut state = self.latch.state.lock();
        let Some(entry) = state.entries.get_mut(&self.key) else {
            self.complete = true;
            self.value.take();
            return true;
        };
        if entry.dirty {
            entry.dirty = false;
            let previous = entry.charged_bytes;
            drop(state);
            let value = self.value_mut();
            value.coalesce_refresh();
            let current = value.retained_bytes().max(1);
            let mut state = self.latch.state.lock();
            if let Some(entry) = state.entries.get_mut(&self.key) {
                entry.charged_bytes = current;
                state.bytes = state.bytes.saturating_sub(previous).saturating_add(current);
            }
            return false;
        }
        let charged = entry.charged_bytes;
        state.entries.remove(&self.key);
        state.bytes = state.bytes.saturating_sub(charged);
        self.complete = true;
        self.value.take();
        true
    }
}

impl<K, V> Drop for PendingArrivalClaim<K, V>
where
    K: Clone + Eq + Hash,
    V: PendingArrivalValue,
{
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        let Some(mut value) = self.value.take() else {
            return;
        };
        let mut state = self.latch.state.lock();
        if state.closed {
            return;
        }
        let Some(entry) = state.entries.get_mut(&self.key) else {
            return;
        };
        if entry.dirty {
            value.coalesce_refresh();
        }
        let previous = entry.charged_bytes;
        let current = value.retained_bytes().max(1);
        entry.value = Some(value);
        entry.charged_bytes = current;
        entry.active = false;
        entry.dirty = false;
        state.bytes = state.bytes.saturating_sub(previous).saturating_add(current);
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PendingArrivalSnapshot {
    pub(crate) count: usize,
    pub(crate) bytes: usize,
    pub(crate) rejected: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingArrivalInsertError {
    Closed,
    CountFull,
    BytesFull,
    SizeOverflow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PendingOffsetRange {
    pub(crate) first: i64,
    pub(crate) last: i64,
}

impl PendingOffsetRange {
    pub(crate) const fn single(offset: i64) -> Self {
        Self {
            first: offset,
            last: offset,
        }
    }

    fn include(&mut self, offset: i64) {
        self.first = self.first.min(offset);
        self.last = self.last.max(offset);
    }

    fn include_range(&mut self, first: i64, last: i64) {
        self.first = self.first.min(first);
        self.last = self.last.max(last);
    }
}

pub(crate) trait PendingOffsetTarget {
    fn retained_bytes(&self) -> usize;
}

pub(crate) struct PendingOffsetRangeLatch<K> {
    max_count: usize,
    max_bytes: usize,
    closed: AtomicBool,
    state: Mutex<PendingOffsetRangeState<K>>,
}

impl<K> PendingOffsetRangeLatch<K>
where
    K: Clone + Eq + Hash + PendingOffsetTarget,
{
    pub(crate) fn new(max_count: usize, max_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            max_count: max_count.max(1),
            max_bytes: max_bytes.max(1),
            closed: AtomicBool::new(false),
            state: Mutex::new(PendingOffsetRangeState::default()),
        })
    }

    pub(crate) fn merge(&self, key: K, offset: i64) -> Result<(), PendingArrivalInsertError> {
        self.merge_range(key, offset, offset)
    }

    pub(crate) fn merge_range(&self, key: K, first: i64, last: i64) -> Result<(), PendingArrivalInsertError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PendingArrivalInsertError::Closed);
        }
        let mut state = self.state.lock();
        if state.closed {
            return Err(PendingArrivalInsertError::Closed);
        }
        if let Some(entry) = state.entries.get_mut(&key) {
            entry.range.include_range(first.min(last), first.max(last));
            return Ok(());
        }
        if state.entries.len() >= self.max_count {
            state.rejected = state.rejected.saturating_add(1);
            return Err(PendingArrivalInsertError::CountFull);
        }
        let retained = key
            .retained_bytes()
            .checked_add(std::mem::size_of::<PendingOffsetRangeEntry>())
            .ok_or(PendingArrivalInsertError::SizeOverflow)?;
        let Some(bytes) = state.bytes.checked_add(retained) else {
            state.rejected = state.rejected.saturating_add(1);
            return Err(PendingArrivalInsertError::SizeOverflow);
        };
        if bytes > self.max_bytes {
            state.rejected = state.rejected.saturating_add(1);
            return Err(PendingArrivalInsertError::BytesFull);
        }
        state.bytes = bytes;
        state.entries.insert(
            key,
            PendingOffsetRangeEntry {
                range: PendingOffsetRange {
                    first: first.min(last),
                    last: first.max(last),
                },
                retained_bytes: retained,
                active: false,
            },
        );
        Ok(())
    }

    pub(crate) fn reserve_batch(self: &Arc<Self>, limit: usize) -> Vec<PendingOffsetRangeReservation<K>> {
        if limit == 0 || self.closed.load(Ordering::Acquire) {
            return Vec::new();
        }
        let mut state = self.state.lock();
        if state.closed {
            return Vec::new();
        }
        let reservations = state
            .entries
            .iter_mut()
            .filter_map(|(key, entry)| {
                if entry.active {
                    return None;
                }
                entry.active = true;
                Some(PendingOffsetRangeReservation {
                    latch: Arc::clone(self),
                    key: Some(key.clone()),
                    range: entry.range,
                })
            })
            .take(limit)
            .collect();
        reservations
    }

    pub(crate) fn retain_targets(&self, mut retain: impl FnMut(&K) -> bool) {
        let mut state = self.state.lock();
        let removed = state
            .entries
            .iter()
            .filter(|(key, entry)| !entry.active && !retain(key))
            .map(|(key, entry)| (key.clone(), entry.retained_bytes))
            .collect::<Vec<_>>();
        for (key, bytes) in removed {
            state.entries.remove(&key);
            state.bytes = state.bytes.saturating_sub(bytes);
        }
    }

    pub(crate) fn seal(&self) {
        self.closed.store(true, Ordering::Release);
        let mut state = self.state.lock();
        state.closed = true;
        state.entries.clear();
        state.bytes = 0;
    }

    pub(crate) fn snapshot(&self) -> PendingArrivalSnapshot {
        let state = self.state.lock();
        PendingArrivalSnapshot {
            count: state.entries.len(),
            bytes: state.bytes,
            rejected: state.rejected,
        }
    }

    fn release(&self, key: &K) {
        if let Some(entry) = self.state.lock().entries.get_mut(key) {
            entry.active = false;
        }
    }
}

struct PendingOffsetRangeState<K> {
    entries: HashMap<K, PendingOffsetRangeEntry>,
    bytes: usize,
    rejected: usize,
    closed: bool,
}

impl<K> Default for PendingOffsetRangeState<K> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            bytes: 0,
            rejected: 0,
            closed: false,
        }
    }
}

struct PendingOffsetRangeEntry {
    range: PendingOffsetRange,
    retained_bytes: usize,
    active: bool,
}

#[must_use]
pub(crate) struct PendingOffsetRangeReservation<K>
where
    K: Clone + Eq + Hash + PendingOffsetTarget,
{
    latch: Arc<PendingOffsetRangeLatch<K>>,
    key: Option<K>,
    range: PendingOffsetRange,
}

impl<K> PendingOffsetRangeReservation<K>
where
    K: Clone + Eq + Hash + PendingOffsetTarget,
{
    pub(crate) fn key(&self) -> &K {
        self.key.as_ref().expect("pending offset reservation retains its key")
    }

    pub(crate) const fn range(&self) -> PendingOffsetRange {
        self.range
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.key()
            .retained_bytes()
            .saturating_add(std::mem::size_of::<PendingOffsetRangeEntry>())
    }

    /// Completes only the exact range snapshot processed by the worker. A
    /// concurrent extension stays active and returns the new envelope.
    pub(crate) fn finish_or_updated(&mut self) -> Option<PendingOffsetRange> {
        let key = self.key.as_ref()?;
        let mut state = self.latch.state.lock();
        let Some(entry) = state.entries.get_mut(key) else {
            self.key.take();
            return None;
        };
        if entry.range != self.range {
            self.range = entry.range;
            return Some(entry.range);
        }
        let bytes = entry.retained_bytes;
        state.entries.remove(key);
        state.bytes = state.bytes.saturating_sub(bytes);
        self.key.take();
        None
    }
}

impl<K> Drop for PendingOffsetRangeReservation<K>
where
    K: Clone + Eq + Hash + PendingOffsetTarget,
{
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.latch.release(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, Eq, Hash, PartialEq)]
    struct TestTarget(u8);

    impl PendingOffsetTarget for TestTarget {
        fn retained_bytes(&self) -> usize {
            1
        }
    }

    #[test]
    fn offset_range_reservation_replays_only_concurrent_extensions() {
        let latch = PendingOffsetRangeLatch::new(2, 1024);
        latch.merge_range(TestTarget(1), 10, 12).expect("initial range");
        let mut reservation = latch.reserve_batch(1).pop().expect("reservation");

        latch.merge_range(TestTarget(1), 8, 15).expect("extend active range");
        assert_eq!(
            reservation.finish_or_updated(),
            Some(PendingOffsetRange { first: 8, last: 15 })
        );
        assert_eq!(reservation.finish_or_updated(), None);
        assert_eq!(latch.snapshot().count, 0);
    }

    #[test]
    fn rejected_worker_submission_releases_range_for_next_tick() {
        let latch = PendingOffsetRangeLatch::new(1, 1024);
        latch.merge(TestTarget(1), 7).expect("pending range");
        let reservation = latch.reserve_batch(1).pop().expect("first tick");
        drop(reservation);

        assert_eq!(latch.reserve_batch(1).len(), 1, "drop must make the range retryable");
    }

    #[test]
    fn active_stale_and_live_targets_fit_combined_waiter_and_worker_budget() {
        // N=2 live waiters plus C=2 admitted replay workers. Active stale
        // reservations cannot exceed C because each owns a continuation permit.
        let latch = PendingOffsetRangeLatch::new(4, 1024);
        latch.merge(TestTarget(1), 1).expect("first live target");
        latch.merge(TestTarget(2), 1).expect("second live target");
        let active_stale = latch.reserve_batch(2);
        latch.retain_targets(|_| false);

        latch.merge(TestTarget(3), 2).expect("replacement live target");
        latch.merge(TestTarget(4), 2).expect("second replacement live target");
        assert_eq!(latch.snapshot().count, 4);
        assert_eq!(
            latch.merge(TestTarget(5), 2),
            Err(PendingArrivalInsertError::CountFull),
            "N+C is the hard invariant boundary"
        );

        drop(active_stale);
        latch.retain_targets(|target| target.0 >= 3);
        latch
            .merge(TestTarget(5), 3)
            .expect("inactive stale capacity is reclaimed");
    }

    #[test]
    fn sealing_clears_active_and_inactive_ranges() {
        let latch = PendingOffsetRangeLatch::new(2, 1024);
        latch.merge(TestTarget(1), 1).expect("active target");
        latch.merge(TestTarget(2), 2).expect("inactive target");
        let active = latch.reserve_batch(1).pop().expect("active reservation");

        latch.seal();
        assert_eq!(latch.snapshot().count, 0);
        assert_eq!(latch.snapshot().bytes, 0);
        drop(active);
        assert!(latch.reserve_batch(2).is_empty());
    }
}
