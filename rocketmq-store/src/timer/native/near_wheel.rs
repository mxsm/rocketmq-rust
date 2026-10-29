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

use std::collections::BTreeMap;

use rocketmq_store_local::timer::native_format::NativeDueCursor;
use rocketmq_store_local::timer::native_format::NativeIndexEntry;

/// Memory one timer is counted as: its entry and its place in an ordered map.
pub(crate) const ENTRY_MEMORY_BYTES: usize = 128;

/// The timers due soon, per lane in delivery order.
///
/// A timer is keyed by its `(due_ms, cq_offset)` cursor, which identifies it, so loading a
/// bucket whose timers are partly in the wheel already adds each of them once.
pub(crate) struct NearWheel {
    lanes: Vec<BTreeMap<NativeDueCursor, NativeIndexEntry>>,
    len: usize,
}

impl NearWheel {
    pub(crate) fn new(lane_count: u16) -> Self {
        Self {
            lanes: (0..lane_count).map(|_| BTreeMap::new()).collect(),
            len: 0,
        }
    }

    /// Adds `entry` to its lane; an entry outside the lanes is ignored. Returns whether the
    /// wheel did not hold it yet.
    pub(crate) fn insert(&mut self, entry: NativeIndexEntry) -> bool {
        let Some(lane) = self.lanes.get_mut(usize::from(entry.lane)) else {
            return false;
        };
        let added = lane.insert(entry.cursor(), entry).is_none();
        if added {
            self.len += 1;
        }
        added
    }

    /// Takes up to `max` timers of `lane` due at or before `now_ms`, in delivery order.
    pub(crate) fn take_due(&mut self, lane: u16, now_ms: i64, max: usize) -> Vec<NativeIndexEntry> {
        let Some(timers) = self.lanes.get_mut(usize::from(lane)) else {
            return Vec::new();
        };
        let mut due = Vec::new();
        while due.len() < max {
            match timers.first_key_value() {
                Some((cursor, _)) if cursor.due_ms <= now_ms => {}
                _ => break,
            }
            if let Some((_, entry)) = timers.pop_first() {
                due.push(entry);
            }
        }
        self.len -= due.len();
        due
    }

    /// The earliest deadline in the wheel.
    pub(crate) fn next_due_ms(&self) -> Option<i64> {
        self.lanes
            .iter()
            .filter_map(|lane| lane.first_key_value().map(|(cursor, _)| cursor.due_ms))
            .min()
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn memory_bytes(&self) -> usize {
        self.len * ENTRY_MEMORY_BYTES
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_store_local::timer::native_format::NativeLocator;

    use super::*;

    fn entry(due_ms: i64, cq_offset: i64, lane: u16) -> NativeIndexEntry {
        NativeIndexEntry {
            due_ms,
            cq_offset,
            locator: NativeLocator::CommitLog { offset: 0 },
            generation: 0,
            key_hash: 0,
            size: 10,
            lane,
        }
    }

    #[test]
    fn a_lane_hands_out_due_timers_in_delivery_order() {
        let mut wheel = NearWheel::new(2);
        for entry in [entry(300, 5, 0), entry(100, 9, 0), entry(100, 2, 0), entry(50, 1, 1)] {
            assert!(wheel.insert(entry));
        }
        assert!(!wheel.insert(entry(100, 2, 0)));
        assert!(!wheel.insert(entry(1, 1, 7)));
        assert_eq!((wheel.len(), wheel.next_due_ms()), (4, Some(50)));

        let taken = wheel.take_due(0, 299, 10);
        assert_eq!(
            taken.iter().map(|entry| entry.cq_offset).collect::<Vec<_>>(),
            vec![2, 9]
        );
        assert!(wheel.take_due(0, 299, 10).is_empty());
        assert_eq!(wheel.take_due(0, 300, 1)[0].cq_offset, 5);
        assert_eq!((wheel.len(), wheel.memory_bytes()), (1, ENTRY_MEMORY_BYTES));
    }

    #[test]
    fn a_take_stops_at_its_limit() {
        let mut wheel = NearWheel::new(1);
        for cq_offset in 0..5 {
            wheel.insert(entry(10, cq_offset, 0));
        }
        assert_eq!(wheel.take_due(0, 10, 3).len(), 3);
        assert_eq!(wheel.take_due(0, 10, 3).len(), 2);
        assert_eq!(wheel.next_due_ms(), None);
    }
}
