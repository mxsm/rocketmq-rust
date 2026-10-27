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

//! The Apache RocketMQ 5.x `timerwheel` file: two rounds of 32-byte slots, kept in memory and
//! written back on flush, as Java's `TimerWheel` does with its buffer and memory map.

use std::collections::BTreeSet;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use std::path::PathBuf;

use parking_lot::Mutex;
use thiserror::Error;

use crate::timer::java_format::decode_java_timer_wheel_slot;
use crate::timer::java_format::encode_java_timer_wheel_slot;
use crate::timer::java_format::java_timer_wheel_len;
use crate::timer::java_format::java_timer_wheel_slot_index;
use crate::timer::java_format::JAVA_TIMER_WHEEL_SLOT_SIZE;
use crate::timer::positional_io::read_exact_at;
use crate::timer::positional_io::write_all_at;
use crate::timer::slot::Slot;

/// Bytes the wheel writes back together; a slot never spans two pages.
const PAGE_SIZE: usize = 4_096;

/// A Java-format TimerWheel that cannot be opened or written.
#[derive(Debug, Error)]
pub enum JavaTimerWheelError {
    #[error("timer wheel I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error(
        "timer wheel needs at least one slot and a positive precision, got {slots_total} slots of {precision_ms} ms"
    )]
    InvalidGeometry { slots_total: usize, precision_ms: i64 },
    #[error("timer wheel file has {length} bytes, expected {expected}")]
    Length { length: u64, expected: u64 },
}

/// A `timerwheel` file in the Apache RocketMQ 5.x format.
pub struct JavaTimerWheel {
    path: PathBuf,
    slots_total: usize,
    precision_ms: i64,
    file: File,
    state: Mutex<JavaTimerWheelState>,
    /// Keeps flushes in order, so an older copy of a page never lands after a newer one.
    flush_lock: Mutex<()>,
}

struct JavaTimerWheelState {
    bytes: Vec<u8>,
    dirty_pages: BTreeSet<usize>,
    /// Non-empty slots on each page, so scans skip the empty part of the wheel.
    occupied: Vec<u16>,
}

impl JavaTimerWheelState {
    fn new(bytes: Vec<u8>) -> Self {
        let mut occupied = vec![0u16; bytes.len().div_ceil(PAGE_SIZE)];
        for (index, slot) in bytes.chunks_exact(JAVA_TIMER_WHEEL_SLOT_SIZE).enumerate() {
            if !is_empty_slot(slot) {
                occupied[index * JAVA_TIMER_WHEEL_SLOT_SIZE / PAGE_SIZE] += 1;
            }
        }
        Self {
            bytes,
            dirty_pages: BTreeSet::new(),
            occupied,
        }
    }

    /// Replaces the bytes of slot `index` and keeps the page counts.
    fn write(&mut self, index: usize, slot: &[u8; JAVA_TIMER_WHEEL_SLOT_SIZE]) {
        let start = index * JAVA_TIMER_WHEEL_SLOT_SIZE;
        let page = start / PAGE_SIZE;
        let target = &mut self.bytes[start..start + JAVA_TIMER_WHEEL_SLOT_SIZE];
        match (is_empty_slot(target), is_empty_slot(slot)) {
            (true, false) => self.occupied[page] += 1,
            (false, true) => self.occupied[page] -= 1,
            _ => {}
        }
        target.copy_from_slice(slot);
        self.dirty_pages.insert(page);
    }

    /// Indexes of the non-empty slots, in index order.
    fn occupied_indexes(&self) -> impl Iterator<Item = usize> + '_ {
        let slots_per_page = PAGE_SIZE / JAVA_TIMER_WHEEL_SLOT_SIZE;
        let slot_count = self.bytes.len() / JAVA_TIMER_WHEEL_SLOT_SIZE;
        self.occupied
            .iter()
            .enumerate()
            .filter(|(_, count)| **count > 0)
            .flat_map(move |(page, _)| page * slots_per_page..((page + 1) * slots_per_page).min(slot_count))
            .filter(|index| {
                let start = index * JAVA_TIMER_WHEEL_SLOT_SIZE;
                !is_empty_slot(&self.bytes[start..start + JAVA_TIMER_WHEEL_SLOT_SIZE])
            })
    }
}

fn is_empty_slot(bytes: &[u8]) -> bool {
    bytes.iter().all(|byte| *byte == 0)
}

impl JavaTimerWheel {
    /// Opens the wheel file at `path`, creating it when missing, and reads it into memory. Like
    /// Java, an existing file must be empty or exactly the wheel length.
    ///
    /// The default Java configuration keeps one `timerwheel` file. The snapshot copies newer
    /// Java versions write when `timerWheelSnapshotFlush` is enabled are not read.
    ///
    /// # Errors
    ///
    /// Returns [`JavaTimerWheelError`] when the geometry is invalid, the file has another
    /// length, or it cannot be created or read.
    pub fn open(path: impl AsRef<Path>, slots_total: usize, precision_ms: i64) -> Result<Self, JavaTimerWheelError> {
        if slots_total == 0 || precision_ms <= 0 {
            return Err(JavaTimerWheelError::InvalidGeometry {
                slots_total,
                precision_ms,
            });
        }
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let wheel_len = java_timer_wheel_len(slots_total);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        let length = file.metadata()?.len();
        if length != 0 && length != wheel_len as u64 {
            return Err(JavaTimerWheelError::Length {
                length,
                expected: wheel_len as u64,
            });
        }
        if length == 0 {
            file.set_len(wheel_len as u64)?;
        }
        let mut bytes = vec![0u8; wheel_len];
        read_exact_at(&file, &mut bytes, 0)?;
        Ok(Self {
            path,
            slots_total,
            precision_ms,
            file,
            state: Mutex::new(JavaTimerWheelState::new(bytes)),
            flush_lock: Mutex::new(()),
        })
    }

    /// The slot for `time_ms`, or `None` when its position holds another round
    /// (`TimerWheel.getSlot`).
    pub fn get_slot(&self, time_ms: i64) -> Option<Slot> {
        let index = self.slot_index(time_ms);
        let state = self.state.lock();
        decode_java_timer_wheel_slot(Self::slot_bytes(&state.bytes, index), time_ms, self.precision_ms)
            .ok()
            .flatten()
    }

    /// Writes the slot for `time_ms` (`TimerWheel.putSlot`).
    pub fn put_slot(&self, time_ms: i64, first_pos: i64, last_pos: i64, num: i32, magic: i32) {
        let slot = Slot::new_with_num_magic(time_ms, first_pos, last_pos, num, magic);
        let index = self.slot_index(time_ms);
        let mut state = self.state.lock();
        self.write_slot(&mut state, index, slot);
    }

    /// Points the slot for `time_ms` at a unit recovery found at `last_pos`, as Java's
    /// `reviseSlot(timeMs, IGNORE, lastPos, true)`: a slot of this round keeps its first unit and
    /// count; any other content becomes a slot of this round holding only that unit.
    ///
    /// Java keeps the count and magic of the older round in that case. They describe units
    /// that are no longer in the slot, so the new slot starts from zero, as a slot Java creates
    /// on enqueue does.
    pub fn revise_last_pos(&self, time_ms: i64, last_pos: i64) {
        let index = self.slot_index(time_ms);
        let mut state = self.state.lock();
        let slot = match decode_java_timer_wheel_slot(Self::slot_bytes(&state.bytes, index), time_ms, self.precision_ms)
        {
            Ok(Some(slot)) => Slot::new_with_num_magic(time_ms, slot.first_pos, last_pos, slot.num, slot.magic),
            _ => Slot::new(time_ms, last_pos, last_pos),
        };
        self.write_slot(&mut state, index, slot);
    }

    /// Empties the slot for `time_ms`, so it reads as absent in every round. Java leaves a
    /// drained slot in place and moves its read time past it; an engine that rewrites a slot
    /// while draining it needs a slot that reads as gone.
    pub fn clear_slot(&self, time_ms: i64) {
        let index = self.slot_index(time_ms);
        self.state.lock().write(index, &[0; JAVA_TIMER_WHEEL_SLOT_SIZE]);
    }

    /// Calls `visit` with every non-empty slot, whatever its round, with its stored time.
    pub fn visit_occupied_slots(&self, mut visit: impl FnMut(Slot)) {
        let state = self.state.lock();
        for index in state.occupied_indexes() {
            visit(self.decode_stored(&state.bytes, index));
        }
    }

    /// Empties every slot whose first or last unit is at or after `log_end`, which recovery
    /// found to be the end of the TimerLog, and returns how many it emptied.
    pub fn clear_slots_beyond(&self, log_end: i64) -> usize {
        let mut state = self.state.lock();
        let beyond: Vec<usize> = state
            .occupied_indexes()
            .filter(|index| {
                let slot = self.decode_stored(&state.bytes, *index);
                slot.first_pos >= log_end || slot.last_pos >= log_end
            })
            .collect();
        for index in &beyond {
            state.write(*index, &[0; JAVA_TIMER_WHEEL_SLOT_SIZE]);
        }
        beyond.len()
    }

    /// The smallest first position among the slots of this round, from `time_start_ms` on,
    /// whose last position is beyond `max_offset`, or `i64::MAX` when there is none
    /// (`TimerWheel.checkPhyPos`). Recovery rescans the TimerLog from there.
    pub fn check_phy_pos(&self, time_start_ms: i64, max_offset: i64) -> i64 {
        let mut min_first = i64::MAX;
        self.visit_round(time_start_ms, |slot| {
            if slot.last_pos > max_offset {
                min_first = min_first.min(slot.first_pos);
            }
        });
        min_first
    }

    /// Messages counted in the slots of this round from `time_start_ms` on
    /// (`TimerWheel.getAllNum`).
    pub fn all_num(&self, time_start_ms: i64) -> i64 {
        let mut total = 0i64;
        self.visit_round(time_start_ms, |slot| total += i64::from(slot.num));
        total
    }

    /// Writes the pages changed since the last flush and syncs the file. Slots can change while
    /// the pages are written; those pages stay dirty for the next flush.
    ///
    /// # Errors
    ///
    /// Returns [`JavaTimerWheelError::Io`] when the file cannot be written or synced; the pages
    /// stay dirty.
    pub fn flush(&self) -> Result<(), JavaTimerWheelError> {
        let _flush = self.flush_lock.lock();
        let pages: Vec<(usize, Vec<u8>)> = {
            let mut state = self.state.lock();
            let dirty = std::mem::take(&mut state.dirty_pages);
            dirty
                .into_iter()
                .map(|page| {
                    let start = page * PAGE_SIZE;
                    let end = (start + PAGE_SIZE).min(state.bytes.len());
                    (page, state.bytes[start..end].to_vec())
                })
                .collect()
        };
        let written = pages
            .iter()
            .try_for_each(|(page, bytes)| write_all_at(&self.file, bytes, (*page * PAGE_SIZE) as u64));
        if let Err(error) = written.and_then(|()| self.file.sync_data()) {
            self.state
                .lock()
                .dirty_pages
                .extend(pages.iter().map(|(page, _)| *page));
            return Err(error.into());
        }
        Ok(())
    }

    /// Slots in one round (`timerWheelSlots`, without the second round).
    pub fn slots_total(&self) -> usize {
        self.slots_total
    }

    /// Milliseconds each slot covers.
    pub fn precision_ms(&self) -> i64 {
        self.precision_ms
    }

    /// Path of the wheel file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn slot_index(&self, time_ms: i64) -> usize {
        java_timer_wheel_slot_index(time_ms, self.precision_ms, self.slots_total)
    }

    /// Visits the slots that belong to the round starting at `time_start_ms`: the slot at each
    /// index whose stored time is the time that index stands for in that round.
    fn visit_round(&self, time_start_ms: i64, mut visit: impl FnMut(Slot)) {
        let state = self.state.lock();
        let first_index = self.slot_index(time_start_ms);
        let wheel_slots = self.slots_total * 2;
        for index in state.occupied_indexes() {
            let step = (index + wheel_slots - first_index) % wheel_slots;
            let time_ms = time_start_ms.saturating_add(step as i64 * self.precision_ms);
            if let Ok(Some(slot)) =
                decode_java_timer_wheel_slot(Self::slot_bytes(&state.bytes, index), time_ms, self.precision_ms)
            {
                visit(slot);
            }
        }
    }

    /// The slot at `index` with the time it stores, whichever round that is.
    fn decode_stored(&self, bytes: &[u8], index: usize) -> Slot {
        let slot_bytes = Self::slot_bytes(bytes, index);
        let mut stored = [0u8; 8];
        stored.copy_from_slice(&slot_bytes[..8]);
        let time_ms = i64::from_be_bytes(stored).saturating_mul(self.precision_ms);
        decode_java_timer_wheel_slot(slot_bytes, time_ms, self.precision_ms)
            .ok()
            .flatten()
            .unwrap_or_else(|| Slot::new(time_ms, -1, -1))
    }

    fn write_slot(&self, state: &mut JavaTimerWheelState, index: usize, slot: Slot) {
        state.write(index, &encode_java_timer_wheel_slot(slot, self.precision_ms));
    }

    fn slot_bytes(bytes: &[u8], index: usize) -> &[u8] {
        let start = index * JAVA_TIMER_WHEEL_SLOT_SIZE;
        &bytes[start..start + JAVA_TIMER_WHEEL_SLOT_SIZE]
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    const PRECISION_MS: i64 = 1_000;
    // 256 slots per round fill four 4 KiB pages.
    const SLOTS_TOTAL: usize = 256;
    const BASE_MS: i64 = 1_700_000_000_000;

    fn opened(path: &Path) -> JavaTimerWheel {
        JavaTimerWheel::open(path, SLOTS_TOTAL, PRECISION_MS).unwrap()
    }

    #[test]
    fn slots_reach_the_file_in_java_bytes_and_survive_a_reopen() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("timerwheel");
        let wheel = opened(&path);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 16_384);

        wheel.put_slot(BASE_MS + 400, 52, 104, 2, 1);
        let slot = Slot::new_with_num_magic(BASE_MS, 52, 104, 2, 1);
        assert_eq!(wheel.get_slot(BASE_MS + 999), Some(slot));
        let next_round_ms = BASE_MS + 2 * SLOTS_TOTAL as i64 * PRECISION_MS;
        assert_eq!(wheel.get_slot(next_round_ms), None);
        wheel.flush().unwrap();

        let index = java_timer_wheel_slot_index(BASE_MS, PRECISION_MS, SLOTS_TOTAL);
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(
            &bytes[index * 32..index * 32 + 32],
            &encode_java_timer_wheel_slot(slot, PRECISION_MS)
        );
        assert_eq!(&bytes[index * 32..index * 32 + 8], &(BASE_MS / 1_000).to_be_bytes());
        drop(wheel);
        assert_eq!(opened(&path).get_slot(BASE_MS), Some(slot));
    }

    #[test]
    fn open_rejects_a_file_of_another_length() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("timerwheel");
        std::fs::write(&path, [0u8; 64]).unwrap();
        assert!(matches!(
            JavaTimerWheel::open(&path, SLOTS_TOTAL, PRECISION_MS),
            Err(JavaTimerWheelError::Length {
                length: 64,
                expected: 16_384
            })
        ));
        assert!(matches!(
            JavaTimerWheel::open(&path, 0, PRECISION_MS),
            Err(JavaTimerWheelError::InvalidGeometry { .. })
        ));
    }

    #[test]
    fn revise_moves_the_last_unit_and_starts_other_rounds_afresh() {
        let directory = tempdir().unwrap();
        let wheel = opened(&directory.path().join("timerwheel"));
        wheel.put_slot(BASE_MS, 0, 52, 2, 1);
        wheel.revise_last_pos(BASE_MS, 156);
        assert_eq!(
            wheel.get_slot(BASE_MS),
            Some(Slot::new_with_num_magic(BASE_MS, 0, 156, 2, 1))
        );

        // The slot one round later sits at the same index and replaces the old content.
        let next_round_ms = BASE_MS + 2 * SLOTS_TOTAL as i64 * PRECISION_MS;
        wheel.revise_last_pos(next_round_ms, 208);
        assert_eq!(wheel.get_slot(next_round_ms), Some(Slot::new(next_round_ms, 208, 208)));
        assert_eq!(wheel.get_slot(BASE_MS), None);
    }

    #[test]
    fn scans_count_only_slots_of_the_current_round() {
        let directory = tempdir().unwrap();
        let wheel = opened(&directory.path().join("timerwheel"));
        wheel.put_slot(BASE_MS, 0, 52, 2, 1);
        wheel.put_slot(BASE_MS + 5 * PRECISION_MS, 104, 400, 3, 1);
        wheel.put_slot(BASE_MS + 9 * PRECISION_MS, 300, 300, 1, 1);
        // Before the start: it belongs to the previous round of its index.
        wheel.put_slot(BASE_MS - PRECISION_MS, 7, 900, 4, 1);

        assert_eq!(wheel.all_num(BASE_MS), 6);
        assert_eq!(wheel.check_phy_pos(BASE_MS, 299), 104);
        assert_eq!(wheel.check_phy_pos(BASE_MS, 400), i64::MAX);
        assert_eq!(wheel.all_num(BASE_MS - PRECISION_MS), 10);
        assert_eq!(wheel.check_phy_pos(BASE_MS - PRECISION_MS, 400), 7);
    }

    #[test]
    fn cleared_and_stale_slots_are_tracked_for_scans_and_recovery() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("timerwheel");
        let wheel = opened(&path);
        wheel.put_slot(BASE_MS, 0, 52, 2, 1);
        wheel.put_slot(BASE_MS + PRECISION_MS, 104, 156, 1, 0);
        wheel.put_slot(BASE_MS + 2 * PRECISION_MS, 208, 260, 3, 0);

        wheel.clear_slot(BASE_MS);
        assert_eq!(wheel.get_slot(BASE_MS), None);
        let mut visited = Vec::new();
        wheel.visit_occupied_slots(|slot| visited.push(slot.time_ms));
        assert_eq!(visited, vec![BASE_MS + PRECISION_MS, BASE_MS + 2 * PRECISION_MS]);

        // Recovery found the log ending at 200: the slot whose units start at 208 goes.
        assert_eq!(wheel.clear_slots_beyond(200), 1);
        assert_eq!(wheel.get_slot(BASE_MS + 2 * PRECISION_MS), None);
        assert_eq!(wheel.all_num(BASE_MS), 1);
        wheel.flush().unwrap();
        drop(wheel);

        let reopened = opened(&path);
        let mut visited = Vec::new();
        reopened.visit_occupied_slots(|slot| visited.push(slot));
        assert_eq!(
            visited,
            vec![Slot::new_with_num_magic(BASE_MS + PRECISION_MS, 104, 156, 1, 0)]
        );
    }

    #[test]
    fn flush_writes_only_the_pages_that_changed() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("timerwheel");
        let wheel = opened(&path);
        wheel.put_slot(BASE_MS, 0, 52, 1, 1);
        let page = java_timer_wheel_slot_index(BASE_MS, PRECISION_MS, SLOTS_TOTAL) * 32 / PAGE_SIZE;
        let untouched = (page + 2) % 4;
        // A marker the wheel never wrote: it survives the flush only if the page is not written.
        let mut file = std::fs::read(&path).unwrap();
        file[untouched * PAGE_SIZE] = 0x5A;
        std::fs::write(&path, &file).unwrap();

        wheel.flush().unwrap();
        let file = std::fs::read(&path).unwrap();
        assert_eq!(file[untouched * PAGE_SIZE], 0x5A);
        assert_ne!(&file[page * PAGE_SIZE..(page + 1) * PAGE_SIZE], &[0u8; PAGE_SIZE][..]);
    }
}
