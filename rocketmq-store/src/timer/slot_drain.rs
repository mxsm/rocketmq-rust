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

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use rocketmq_store_local::timer::metrics::TimerStorageMetrics;
use rocketmq_store_local::timer::service::TimerLogRecord;
use rocketmq_store_local::timer::slot::Slot;
use rocketmq_store_local::timer::slot_drain_file::SlotDrainFile;
use rocketmq_store_local::timer::slot_drain_file::SlotDrainFileBuilder;
use rocketmq_store_local::timer::slot_drain_file::SlotDrainLocator;
use rocketmq_store_local::timer::storage_format::crc32c;

use crate::timer::timer_message_store::MAGIC_DEFAULT;
use crate::timer::timer_message_store::MAGIC_DELETE;
use crate::timer::timer_message_store::MAGIC_ROLL;

pub(crate) const DEFAULT_IN_MEMORY_DRAIN_ENTRIES: usize = 8_192;

#[derive(Clone, Copy, Debug)]
pub(crate) struct SlotDrainEntry {
    pub position: i64,
    pub record: TimerLogRecord,
    pub generation: u64,
}

impl SlotDrainEntry {
    fn locator(self) -> SlotDrainLocator {
        SlotDrainLocator {
            timer_log_position: self.position,
            commit_log_offset: self.record.commit_log_offset,
            size: self.record.size,
            magic: self.record.magic,
            queue_offset: self.record.queue_offset,
            generation: self.generation,
        }
    }

    fn from_locator(slot_time_ms: i64, locator: SlotDrainLocator) -> Self {
        Self {
            position: locator.timer_log_position,
            record: TimerLogRecord {
                deliver_time_ms: slot_time_ms,
                commit_log_offset: locator.commit_log_offset,
                size: locator.size,
                queue_offset: locator.queue_offset,
                prev_pos: -1,
                magic: locator.magic,
            },
            generation: locator.generation,
        }
    }
}

pub(crate) struct SlotDrainPlanBuilder {
    slot_time_ms: i64,
    generation: u64,
    memory_limit: usize,
    drain_path: PathBuf,
    metrics: Arc<TimerStorageMetrics>,
    reverse_entries: Vec<SlotDrainEntry>,
    spill_builder: Option<SlotDrainFileBuilder>,
    magic_counts: [usize; 3],
}

impl SlotDrainPlanBuilder {
    pub fn new(
        root: impl AsRef<Path>,
        slot: Slot,
        memory_limit: usize,
        metrics: Arc<TimerStorageMetrics>,
    ) -> std::io::Result<Self> {
        let generation = slot_drain_generation(slot);
        let drain_directory = root.as_ref().join("timer").join("drain");
        std::fs::create_dir_all(&drain_directory)?;
        Ok(Self {
            slot_time_ms: slot.time_ms,
            generation,
            memory_limit: memory_limit.max(1),
            drain_path: drain_directory.join(format!("{}-{generation}", slot.time_ms)),
            metrics,
            reverse_entries: Vec::with_capacity(memory_limit.min(slot.num.max(0) as usize)),
            spill_builder: None,
            magic_counts: [0; 3],
        })
    }

    /// Receives entries in newest-to-oldest chain order.
    pub fn push_reverse(&mut self, entry: SlotDrainEntry) -> std::io::Result<()> {
        self.metrics.record_hot_slot_scan(1);
        update_magic_counts(&mut self.magic_counts, entry.record.magic, 1);
        if let Some(builder) = self.spill_builder.as_mut() {
            return builder.push_reverse(entry.locator());
        }
        if self.reverse_entries.len() < self.memory_limit {
            self.reverse_entries.push(entry);
            return Ok(());
        }

        let mut builder = SlotDrainFileBuilder::create(&self.drain_path, self.slot_time_ms, self.generation)?;
        for buffered in self.reverse_entries.drain(..) {
            builder.push_reverse(buffered.locator())?;
        }
        builder.push_reverse(entry.locator())?;
        self.spill_builder = Some(builder);
        Ok(())
    }

    pub fn finish(mut self) -> std::io::Result<SlotDrainPlan> {
        let storage = if let Some(builder) = self.spill_builder.take() {
            let file = builder.finish()?;
            self.metrics.record_spill_bytes(file.physical_bytes());
            SlotDrainStorage::Spill(file)
        } else {
            self.reverse_entries.reverse();
            SlotDrainStorage::Memory(self.reverse_entries)
        };
        Ok(SlotDrainPlan {
            slot_time_ms: self.slot_time_ms,
            generation: self.generation,
            total: storage.len(),
            cursor: 0,
            magic_counts: self.magic_counts,
            storage,
        })
    }
}

pub(crate) struct SlotDrainPlan {
    slot_time_ms: i64,
    generation: u64,
    total: usize,
    cursor: usize,
    magic_counts: [usize; 3],
    storage: SlotDrainStorage,
}

impl SlotDrainPlan {
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn cursor(&self) -> usize {
        self.cursor
    }

    pub fn remaining(&self) -> usize {
        self.total.saturating_sub(self.cursor)
    }

    pub fn read_batch(&self, max_records: usize) -> std::io::Result<Vec<SlotDrainEntry>> {
        self.storage
            .read_batch(self.slot_time_ms, self.cursor, max_records.min(self.remaining()))
    }

    pub fn advance(&mut self, entries: &[SlotDrainEntry]) {
        for entry in entries {
            update_magic_counts(&mut self.magic_counts, entry.record.magic, -1);
        }
        self.cursor = (self.cursor + entries.len()).min(self.total);
    }

    pub fn remaining_slot(&self) -> std::io::Result<Option<Slot>> {
        if self.remaining() == 0 {
            return Ok(None);
        }
        let first = self.storage.read_batch(self.slot_time_ms, self.cursor, 1)?[0];
        let last = self.storage.read_batch(self.slot_time_ms, self.total - 1, 1)?[0];
        let mut magic = 0;
        for (index, bit) in [MAGIC_DEFAULT, MAGIC_ROLL, MAGIC_DELETE].into_iter().enumerate() {
            if self.magic_counts[index] > 0 {
                magic |= bit;
            }
        }
        Ok(Some(Slot::new_with_num_magic(
            self.slot_time_ms,
            first.position,
            last.position,
            self.remaining() as i32,
            magic,
        )))
    }

    pub fn matches_slot(&self, slot: Slot) -> std::io::Result<bool> {
        Ok(self.remaining_slot()?.is_some_and(|remaining| {
            remaining.time_ms == slot.time_ms
                && remaining.first_pos == slot.first_pos
                && remaining.last_pos == slot.last_pos
                && remaining.num == slot.num
        }))
    }

    pub fn remove(self) -> std::io::Result<()> {
        match self.storage {
            SlotDrainStorage::Memory(_) => Ok(()),
            SlotDrainStorage::Spill(file) => file.remove(),
        }
    }
}

enum SlotDrainStorage {
    Memory(Vec<SlotDrainEntry>),
    Spill(SlotDrainFile),
}

impl SlotDrainStorage {
    fn len(&self) -> usize {
        match self {
            Self::Memory(entries) => entries.len(),
            Self::Spill(file) => file.record_count(),
        }
    }

    fn read_batch(&self, slot_time_ms: i64, cursor: usize, max_records: usize) -> std::io::Result<Vec<SlotDrainEntry>> {
        match self {
            Self::Memory(entries) => Ok(entries[cursor..cursor + max_records].to_vec()),
            Self::Spill(file) => Ok(file
                .read_batch(cursor, max_records)?
                .into_iter()
                .map(|locator| SlotDrainEntry::from_locator(slot_time_ms, locator))
                .collect()),
        }
    }
}

pub(crate) fn slot_drain_generation(slot: Slot) -> u64 {
    let mut bytes = [0u8; 36];
    bytes[0..8].copy_from_slice(&slot.time_ms.to_be_bytes());
    bytes[8..16].copy_from_slice(&slot.first_pos.to_be_bytes());
    bytes[16..24].copy_from_slice(&slot.last_pos.to_be_bytes());
    bytes[24..28].copy_from_slice(&slot.num.to_be_bytes());
    bytes[28..32].copy_from_slice(&slot.magic.to_be_bytes());
    let first = crc32c(&bytes[..32]) as u64;
    bytes[32..36].copy_from_slice(&(first as u32).to_be_bytes());
    (first << 32) | u64::from(crc32c(&bytes))
}

fn update_magic_counts(counts: &mut [usize; 3], magic: i32, delta: i32) {
    for (index, bit) in [MAGIC_DEFAULT, MAGIC_ROLL, MAGIC_DELETE].into_iter().enumerate() {
        if magic & bit != 0 {
            counts[index] = if delta > 0 {
                counts[index].saturating_add(delta as usize)
            } else {
                counts[index].saturating_sub(delta.unsigned_abs() as usize)
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn entry(position: i64, magic: i32) -> SlotDrainEntry {
        SlotDrainEntry {
            position,
            record: TimerLogRecord {
                deliver_time_ms: 1_000,
                commit_log_offset: position * 10,
                size: 64,
                queue_offset: position / 40,
                prev_pos: position - 40,
                magic,
            },
            generation: 1,
        }
    }

    #[test]
    fn spill_plan_keeps_delivery_order_and_constant_size_continuation() {
        let directory = tempdir().unwrap();
        let slot = Slot::new_with_num_magic(1_000, 0, 160, 5, MAGIC_DEFAULT | MAGIC_DELETE);
        let mut builder =
            SlotDrainPlanBuilder::new(directory.path(), slot, 2, Arc::new(TimerStorageMetrics::default())).unwrap();
        for position in [160, 120, 80, 40, 0] {
            builder
                .push_reverse(entry(
                    position,
                    if position == 160 { MAGIC_DELETE } else { MAGIC_DEFAULT },
                ))
                .unwrap();
        }
        let mut plan = builder.finish().unwrap();
        let first = plan.read_batch(2).unwrap();
        assert_eq!(
            first.iter().map(|value| value.position).collect::<Vec<_>>(),
            vec![0, 40]
        );
        plan.advance(&first);
        assert_eq!(plan.remaining_slot().unwrap().unwrap().first_pos, 80);
        assert_eq!(plan.remaining(), 3);
        plan.remove().unwrap();
    }
}
