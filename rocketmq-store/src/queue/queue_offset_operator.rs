// Copyright 2023 The RocketMQ Rust Authors
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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use rocketmq_model::common::attribute::cq_type::CQType;
use rocketmq_model::common::mix_all::is_lmq;
use tracing::info;

use crate::queue::multi_dispatch_utils::lmq_queue_key;

/// The next logical offset of one queue.
///
/// An offset table hands out the same slot for a key until the queue is removed from it, and
/// recovery or truncation updates the slot in place. An append that resolved the slot before the
/// CommitLog writer lock can therefore assign and advance the live offset with one atomic
/// operation each. Removing the queue retires its slot, and a holder must then resolve it again.
#[derive(Debug)]
pub(crate) struct QueueOffsetSlot {
    next: AtomicI64,
    retired: AtomicBool,
}

impl QueueOffsetSlot {
    fn new(next: i64) -> Self {
        Self {
            next: AtomicI64::new(next),
            retired: AtomicBool::new(false),
        }
    }

    /// Returns the offset the next message of this queue receives.
    #[inline]
    pub(crate) fn next_offset(&self) -> i64 {
        self.next.load(Ordering::Acquire)
    }

    /// Moves the next offset past `count` appended messages.
    #[inline]
    pub(crate) fn advance(&self, count: i64) {
        self.next.fetch_add(count, Ordering::AcqRel);
    }

    #[inline]
    fn set(&self, next: i64) {
        self.next.store(next, Ordering::Release);
    }

    #[inline]
    fn advance_to(&self, next: i64) {
        self.next.fetch_max(next, Ordering::AcqRel);
    }

    #[inline]
    fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }

    #[inline]
    fn retire(&self) {
        self.retired.store(true, Ordering::Release);
    }
}

/// The offset table a consume queue draws its logical offsets from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueueOffsetTable {
    /// The table of simple consume queues.
    Simple,
    /// The table of batch consume queues.
    Batch,
}

impl QueueOffsetTable {
    /// Returns the table a consume queue of `cq_type` assigns offsets from, as its
    /// `assign_queue_offset` does.
    #[inline]
    pub(crate) fn of(cq_type: CQType) -> Self {
        match cq_type {
            CQType::SimpleCQ | CQType::RocksDBCQ => Self::Simple,
            CQType::BatchCQ => Self::Batch,
        }
    }
}

/// A queue's offset slot, resolved before an append takes the CommitLog writer lock.
pub(crate) struct QueueOffsetHandle {
    table: QueueOffsetTable,
    key: CheetahString,
    slot: Arc<QueueOffsetSlot>,
}

impl QueueOffsetHandle {
    /// Returns the queue's live slot, resolving it again when the queue was removed since.
    #[inline]
    pub(crate) fn live_slot(&mut self, operator: &QueueOffsetOperator) -> &QueueOffsetSlot {
        if self.slot.is_retired() {
            self.slot = operator.queue_offset_slot(self.table, &self.key);
        }
        &self.slot
    }
}

type QueueOffsetSlots = DashMap<CheetahString, Arc<QueueOffsetSlot>>;

pub struct QueueOffsetOperator {
    topic_queue_table: Arc<QueueOffsetSlots>,
    batch_topic_queue_table: Arc<QueueOffsetSlots>,
    lmq_topic_queue_table: Arc<DashMap<CheetahString, i64>>,
}

impl Default for QueueOffsetOperator {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl QueueOffsetOperator {
    #[inline]
    pub fn new() -> Self {
        QueueOffsetOperator {
            topic_queue_table: Arc::new(DashMap::new()),
            batch_topic_queue_table: Arc::new(DashMap::new()),
            lmq_topic_queue_table: Arc::new(DashMap::new()),
        }
    }

    #[inline]
    fn slots(&self, table: QueueOffsetTable) -> &QueueOffsetSlots {
        match table {
            QueueOffsetTable::Simple => &self.topic_queue_table,
            QueueOffsetTable::Batch => &self.batch_topic_queue_table,
        }
    }

    /// Returns the slot for `key`, creating it at offset 0 like the first message of a queue.
    pub(crate) fn queue_offset_slot(&self, table: QueueOffsetTable, key: &CheetahString) -> Arc<QueueOffsetSlot> {
        let slots = self.slots(table);
        if let Some(slot) = slots.get(key) {
            return Arc::clone(slot.value());
        }
        Arc::clone(
            slots
                .entry(key.clone())
                .or_insert_with(|| Arc::new(QueueOffsetSlot::new(0)))
                .value(),
        )
    }

    /// Resolves `key`'s slot in `table` for an append that will run under the writer lock.
    pub(crate) fn queue_offset_handle(&self, table: QueueOffsetTable, key: CheetahString) -> QueueOffsetHandle {
        let slot = self.queue_offset_slot(table, &key);
        QueueOffsetHandle { table, key, slot }
    }

    fn set_slot(slots: &QueueOffsetSlots, key: CheetahString, next: i64) {
        match slots.entry(key) {
            Entry::Occupied(entry) => entry.get().set(next),
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(QueueOffsetSlot::new(next)));
            }
        }
    }

    /// Replaces a table's contents while keeping the slots of queues that stay in it.
    fn replace_slots(slots: &QueueOffsetSlots, offsets: HashMap<CheetahString, i64>) {
        slots.retain(|key, slot| {
            let keep = offsets.contains_key(key);
            if !keep {
                slot.retire();
            }
            keep
        });
        for (key, next) in offsets {
            Self::set_slot(slots, key, next);
        }
    }

    fn remove_slot(slots: &QueueOffsetSlots, key: &CheetahString) {
        if let Some((_, slot)) = slots.remove(key) {
            slot.retire();
        }
    }

    #[inline]
    pub fn get_queue_offset(&self, topic_queue_key: CheetahString) -> i64 {
        self.queue_offset_slot(QueueOffsetTable::Simple, &topic_queue_key)
            .next_offset()
    }

    #[inline]
    pub fn get_topic_queue_next_offset(&self, topic_queue_key: &CheetahString) -> Option<i64> {
        self.topic_queue_table
            .get(topic_queue_key)
            .map(|slot| slot.next_offset())
    }

    #[inline]
    pub fn increase_queue_offset(&self, topic_queue_key: CheetahString, message_num: i16) {
        self.queue_offset_slot(QueueOffsetTable::Simple, &topic_queue_key)
            .advance(i64::from(message_num));
    }

    #[inline]
    pub fn update_queue_offset(&self, topic_queue_key: &CheetahString, offset: i64) {
        Self::set_slot(&self.topic_queue_table, topic_queue_key.clone(), offset);
    }

    #[inline]
    pub fn advance_queue_offset(&self, topic_queue_key: CheetahString, offset: i64) {
        match self.topic_queue_table.entry(topic_queue_key) {
            Entry::Occupied(entry) => entry.get().advance_to(offset),
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(QueueOffsetSlot::new(offset)));
            }
        }
    }

    #[inline]
    pub fn get_batch_queue_offset(&self, topic_queue_key: &CheetahString) -> i64 {
        self.queue_offset_slot(QueueOffsetTable::Batch, topic_queue_key)
            .next_offset()
    }

    #[inline]
    pub fn increase_batch_queue_offset(&self, topic_queue_key: &CheetahString, message_num: i16) {
        self.queue_offset_slot(QueueOffsetTable::Batch, topic_queue_key)
            .advance(i64::from(message_num));
    }

    #[inline]
    pub fn get_lmq_offset(&self, topic_queue_key: &CheetahString) -> i64 {
        self.lmq_topic_queue_table
            .get(topic_queue_key)
            .map_or(0, |offset| *offset)
    }

    #[inline]
    pub fn get_lmq_topic_queue_next_offset(&self, topic_queue_key: &CheetahString) -> Option<i64> {
        self.lmq_topic_queue_table.get(topic_queue_key).map(|offset| *offset)
    }

    #[inline]
    pub fn increase_lmq_offset(&self, queue_key: &CheetahString, message_num: i16) {
        self.lmq_topic_queue_table
            .entry(queue_key.clone())
            .and_modify(|offset| *offset += message_num as i64)
            .or_insert(message_num as i64);
    }

    #[inline]
    pub fn current_queue_offset(&self, topic_queue_key: &CheetahString) -> i64 {
        self.topic_queue_table
            .get(topic_queue_key)
            .map_or(0, |slot| slot.next_offset())
    }

    #[inline]
    pub fn remove(&self, topic: &CheetahString, queue_id: i32) {
        let topic_queue_key = CheetahString::from(format!("{topic}-{queue_id}"));
        Self::remove_slot(&self.topic_queue_table, &topic_queue_key);
        Self::remove_slot(&self.batch_topic_queue_table, &topic_queue_key);
        self.lmq_topic_queue_table.remove(&topic_queue_key);

        info!(
            "removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}",
            topic, queue_id
        );
    }

    #[inline]
    pub fn set_topic_queue_table(&self, topic_queue_table: HashMap<CheetahString, i64>) {
        Self::replace_slots(&self.topic_queue_table, topic_queue_table);
    }

    #[inline]
    pub fn set_lmq_topic_queue_table(&self, lmq_topic_queue_table: HashMap<CheetahString, i64>) {
        self.lmq_topic_queue_table.clear();
        for (key, value) in lmq_topic_queue_table {
            if is_lmq(Some(key.as_str())) {
                self.lmq_topic_queue_table.insert(key, value);
            }
        }
    }

    #[inline]
    pub fn get_lmq_num(&self) -> i32 {
        self.lmq_topic_queue_table.len() as i32
    }

    #[inline]
    pub fn get_lmq_topic_names(&self) -> Vec<CheetahString> {
        self.lmq_topic_queue_table
            .iter()
            .filter_map(|entry| entry.key().strip_suffix("-0").map(CheetahString::from))
            .collect()
    }

    #[inline]
    pub fn is_lmq_exist(&self, lmq_topic: &str) -> bool {
        let queue_key = CheetahString::from_string(lmq_queue_key(lmq_topic));
        self.lmq_topic_queue_table.contains_key(&queue_key)
    }

    #[inline]
    pub fn set_batch_topic_queue_table(&self, batch_topic_queue_table: HashMap<CheetahString, i64>) {
        Self::replace_slots(&self.batch_topic_queue_table, batch_topic_queue_table);
    }

    #[inline]
    pub fn get_topic_queue_table(&self) -> HashMap<CheetahString, i64> {
        self.topic_queue_table
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().next_offset()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;

    #[test]
    fn queue_offset_operator_initializes_empty_tables() {
        let operator = QueueOffsetOperator::new();

        assert!(operator.topic_queue_table.is_empty());
        assert!(operator.batch_topic_queue_table.is_empty());
        assert!(operator.lmq_topic_queue_table.is_empty());
    }

    #[test]
    fn get_queue_offset_returns_zero_for_new_key() {
        let operator = QueueOffsetOperator::new();
        assert_eq!(operator.get_queue_offset("new_key".into()), 0);
    }

    #[test]
    fn get_queue_offset_returns_existing_value() {
        let operator = QueueOffsetOperator::new();
        operator.increase_queue_offset("existing_key".into(), 5);
        assert_eq!(operator.get_queue_offset("existing_key".into()), 5);
    }

    #[test]
    fn increase_queue_offset_increases_existing_value() {
        let operator = QueueOffsetOperator::new();
        operator.increase_queue_offset("key".into(), 5);
        operator.increase_queue_offset("key".into(), 3);
        assert_eq!(operator.get_queue_offset("key".into()), 8);
    }

    #[test]
    fn update_queue_offset_updates_existing_value() {
        let operator = QueueOffsetOperator::new();
        operator.increase_queue_offset("key".into(), 5);
        operator.update_queue_offset(&CheetahString::from_static_str("key"), 10);
        assert_eq!(operator.get_queue_offset("key".into()), 10);
    }

    #[test]
    fn remove_clears_key_from_all_tables() {
        let operator = QueueOffsetOperator::new();
        operator.increase_queue_offset("topic-1".into(), 5);
        operator.increase_batch_queue_offset(&CheetahString::from_static_str("topic-1"), 5);
        operator.increase_lmq_offset(&CheetahString::from_static_str("topic-1"), 5);

        operator.remove(&CheetahString::from_static_str("topic"), 1);

        assert_eq!(operator.get_queue_offset("topic-1".into()), 0);
        assert_eq!(
            operator.get_batch_queue_offset(&CheetahString::from_static_str("topic-1")),
            0
        );
        assert_eq!(operator.get_lmq_offset(&CheetahString::from_static_str("topic-1")), 0);
    }

    #[test]
    fn set_topic_queue_table_replaces_existing_table() {
        let operator = QueueOffsetOperator::new();
        operator.increase_queue_offset("old_key".into(), 5);

        let mut new_table = HashMap::new();
        new_table.insert("new_key".into(), 10);
        operator.set_topic_queue_table(new_table);

        let old_key = CheetahString::from_static_str("old_key");
        assert_eq!(operator.get_topic_queue_next_offset(&old_key), None);
        assert_eq!(operator.get_queue_offset("new_key".into()), 10);
    }

    #[test]
    fn set_lmq_topic_queue_table_keeps_prefixed_lmq_entries() {
        let operator = QueueOffsetOperator::new();
        let mut table = HashMap::new();
        table.insert(CheetahString::from_static_str("%LMQ%group-0"), 7);
        table.insert(CheetahString::from_static_str("normal-topic-0"), 3);

        operator.set_lmq_topic_queue_table(table);

        assert_eq!(operator.lmq_topic_queue_table.len(), 1);
        assert_eq!(
            operator
                .lmq_topic_queue_table
                .get(&CheetahString::from_static_str("%LMQ%group-0"))
                .map(|offset| *offset),
            Some(7)
        );
        assert_eq!(operator.get_lmq_num(), 1);
        assert!(operator.is_lmq_exist("%LMQ%group"));
        assert!(!operator.is_lmq_exist("normal-topic"));
    }

    #[test]
    fn concurrent_increase_queue_offset_keeps_offsets_consistent() {
        let operator = Arc::new(QueueOffsetOperator::new());
        let mut handles = Vec::new();

        for thread_id in 0..8 {
            let operator = operator.clone();
            handles.push(std::thread::spawn(move || {
                for iteration in 0..1_000 {
                    let key = CheetahString::from_string(format!("topic-{}", thread_id % 4));
                    let batch_key = CheetahString::from_string(format!("batch-topic-{}", iteration % 4));
                    operator.increase_queue_offset(key, 1);
                    operator.increase_batch_queue_offset(&batch_key, 1);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("offset worker should finish");
        }

        let total_normal: i64 = operator.get_topic_queue_table().values().sum();
        assert_eq!(total_normal, 8_000);

        let total_batch: i64 = operator
            .batch_topic_queue_table
            .iter()
            .map(|entry| entry.value().next_offset())
            .sum();
        assert_eq!(total_batch, 8_000);
    }

    #[test]
    fn resolved_slot_follows_recovery_and_truncation_updates_in_place() {
        let operator = QueueOffsetOperator::new();
        let key = CheetahString::from_static_str("topic-0");
        let mut handle = operator.queue_offset_handle(QueueOffsetTable::Simple, key.clone());
        handle.live_slot(&operator).advance(2);

        operator.set_topic_queue_table(HashMap::from([(key.clone(), 7)]));
        assert_eq!(handle.live_slot(&operator).next_offset(), 7);
        operator.update_queue_offset(&key, 4);
        assert_eq!(handle.live_slot(&operator).next_offset(), 4);
        operator.advance_queue_offset(key.clone(), 9);
        handle.live_slot(&operator).advance(1);

        assert_eq!(operator.current_queue_offset(&key), 10);
        assert!(Arc::ptr_eq(
            &handle.slot,
            &operator.queue_offset_slot(QueueOffsetTable::Simple, &key)
        ));
    }

    #[test]
    fn removed_queue_retires_its_slot_and_a_holder_resolves_a_fresh_one() {
        let operator = QueueOffsetOperator::new();
        let key = CheetahString::from_static_str("topic-1");
        let mut handle = operator.queue_offset_handle(QueueOffsetTable::Simple, key.clone());
        handle.live_slot(&operator).advance(5);

        operator.remove(&CheetahString::from_static_str("topic"), 1);
        assert_eq!(handle.live_slot(&operator).next_offset(), 0);
        handle.live_slot(&operator).advance(1);
        assert_eq!(operator.current_queue_offset(&key), 1);

        operator.set_topic_queue_table(HashMap::new());
        assert_eq!(handle.live_slot(&operator).next_offset(), 0);
        assert_eq!(operator.get_topic_queue_next_offset(&key), Some(0));
    }

    #[test]
    fn batch_handles_draw_from_the_batch_table() {
        let operator = QueueOffsetOperator::new();
        let key = CheetahString::from_static_str("batch-topic-0");
        operator.set_batch_topic_queue_table(HashMap::from([(key.clone(), 32)]));

        let mut handle = operator.queue_offset_handle(QueueOffsetTable::of(CQType::BatchCQ), key.clone());
        assert_eq!(handle.live_slot(&operator).next_offset(), 32);
        handle.live_slot(&operator).advance(8);

        assert_eq!(operator.get_batch_queue_offset(&key), 40);
        assert_eq!(operator.get_topic_queue_next_offset(&key), None);
        assert_eq!(QueueOffsetTable::of(CQType::RocksDBCQ), QueueOffsetTable::Simple);
    }
}
