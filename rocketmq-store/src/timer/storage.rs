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

//! The TimerLog and TimerWheel of the JavaCompat engine behind one interface, so the engine's
//! enqueue, delivery and checkpoint paths do not depend on one file format. Loading and
//! recovery stay with each format.

use std::path::Path;

use rocketmq_store_local::timer::java_format::JavaTimerLogRecord;
use rocketmq_store_local::timer::java_format::JavaTimerLogUnit;
use rocketmq_store_local::timer::java_format::JAVA_TIMER_LOG_UNIT_SIZE;
use rocketmq_store_local::timer::java_timer_log::java_timer_log_unit_offset;
use rocketmq_store_local::timer::java_timer_log::JavaTimerLog;
use rocketmq_store_local::timer::java_timer_wheel::JavaTimerWheel;
use rocketmq_store_local::timer::service::TimerLogRecord;

use crate::timer::slot::Slot;
use crate::timer::timer_log::TimerLog;
use crate::timer::timer_wheel::TimerWheel;

/// The loaded timer files of the JavaCompat engine.
#[allow(
    clippy::large_enum_variant,
    reason = "one storage value lives behind an Arc for the store's lifetime; boxing a variant only adds an indirection"
)]
pub(crate) enum TimerStorage {
    /// The segmented V2 log and the paged A/B wheel.
    RustV2 { log: TimerLog, wheel: TimerWheel },
    /// The Apache RocketMQ 5.x `timerlog` and `timerwheel`.
    Java { log: JavaTimerLog, wheel: JavaTimerWheel },
}

/// A record to append, with what each format keeps beyond the shared record.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TimerLogAppend {
    pub(crate) record: TimerLogRecord,
    /// Delivery generation; the V2 log keeps it.
    pub(crate) generation: u64,
    /// Enqueue time the slot time is stored against; the Java log keeps it (`currWriteTimeMs`).
    pub(crate) write_time_ms: i64,
    /// `String.hashCode()` of the real topic; the Java log keeps it for metrics.
    pub(crate) topic_hash: i32,
}

/// How a format places records and counts slots, so a batch can be planned against the wheel
/// before it is written.
#[derive(Clone, Copy, Debug)]
pub(crate) enum TimerLayout {
    /// Contiguous records of one logical size; a slot counts every record, tombstones too, and
    /// has records while its count is positive.
    RustV2,
    /// 52-byte units in files of `file_size` bytes, each closed by a blank; a slot counts a
    /// tombstone as minus one, as Java does, and has records while it is in the wheel.
    Java { file_size: u64 },
}

impl TimerLayout {
    /// Position of a record appended at `end`.
    pub(crate) fn place(self, end: i64) -> i64 {
        match self {
            Self::RustV2 => end,
            Self::Java { file_size } => {
                java_timer_log_unit_offset(file_size, end.max(0) as u64).min(i64::MAX as u64) as i64
            }
        }
    }

    /// End of the log after a record placed at `position`.
    pub(crate) fn end_after(self, position: i64) -> i64 {
        match self {
            Self::RustV2 => position + TimerLogRecord::SIZE as i64,
            Self::Java { .. } => position + JAVA_TIMER_LOG_UNIT_SIZE as i64,
        }
    }

    /// Whether `slot` holds records to deliver.
    pub(crate) fn has_records(self, slot: &Slot) -> bool {
        match self {
            Self::RustV2 => slot.num > 0,
            Self::Java { .. } => true,
        }
    }

    /// Count of a slot after one more record, from its count before (`None` for a new slot).
    pub(crate) fn count_after(self, count: Option<i32>, is_delete: bool) -> i32 {
        let count = count.unwrap_or_default();
        match self {
            Self::RustV2 => count.saturating_add(1),
            Self::Java { .. } if is_delete => count.saturating_sub(1),
            Self::Java { .. } => count.saturating_add(1),
        }
    }
}

impl TimerStorage {
    pub(crate) fn layout(&self) -> TimerLayout {
        match self {
            Self::RustV2 { .. } => TimerLayout::RustV2,
            Self::Java { log, .. } => TimerLayout::Java {
                file_size: log.file_size(),
            },
        }
    }

    pub(crate) fn get_slot(&self, time_ms: i64) -> Option<Slot> {
        match self {
            Self::RustV2 { wheel, .. } => wheel.get_slot(time_ms),
            Self::Java { wheel, .. } => wheel.get_slot(time_ms),
        }
    }

    /// The slot for `time_ms` when it holds records to deliver.
    pub(crate) fn slot_with_records(&self, time_ms: i64) -> Option<Slot> {
        self.get_slot(time_ms).filter(|slot| self.layout().has_records(slot))
    }

    pub(crate) fn put_slot(&self, slot: Slot) -> std::io::Result<()> {
        match self {
            Self::RustV2 { wheel, .. } => {
                wheel.put_slot(slot.time_ms, slot.first_pos, slot.last_pos, slot.num, slot.magic)
            }
            Self::Java { wheel, .. } => {
                wheel.put_slot(slot.time_ms, slot.first_pos, slot.last_pos, slot.num, slot.magic);
                Ok(())
            }
        }
    }

    pub(crate) fn put_slots(&self, slots: &[Slot]) -> std::io::Result<()> {
        match self {
            Self::RustV2 { wheel, .. } => wheel.put_slots(slots),
            Self::Java { .. } => slots.iter().try_for_each(|slot| self.put_slot(*slot)),
        }
    }

    /// Marks the slot for `time_ms` as holding nothing to deliver.
    pub(crate) fn clear_slot(&self, time_ms: i64) -> std::io::Result<()> {
        match self {
            Self::RustV2 { wheel, .. } => wheel.put_slot(time_ms, 0, 0, 0, 0),
            Self::Java { wheel, .. } => {
                wheel.clear_slot(time_ms);
                Ok(())
            }
        }
    }

    /// Messages counted in the wheel window that starts at `time_start_ms`.
    pub(crate) fn all_num(&self, time_start_ms: i64) -> i64 {
        match self {
            Self::RustV2 { wheel, .. } => wheel.get_all_num(time_start_ms),
            Self::Java { wheel, .. } => wheel.all_num(time_start_ms),
        }
    }

    /// Time of the latest slot at or after `from_ms` that holds records to deliver.
    pub(crate) fn latest_slot_with_records(&self, from_ms: i64) -> Option<i64> {
        let layout = self.layout();
        let mut latest = None;
        self.visit_occupied_slots(|slot| {
            if slot.time_ms >= from_ms && layout.has_records(&slot) {
                latest = latest.max(Some(slot.time_ms));
            }
        });
        latest
    }

    /// Calls `visit` with every slot that holds records.
    pub(crate) fn visit_occupied_slots(&self, mut visit: impl FnMut(Slot)) {
        match self {
            Self::RustV2 { wheel, .. } => wheel.visit_occupied_slots(|_, slot| visit(slot)),
            Self::Java { wheel, .. } => wheel.visit_occupied_slots(visit),
        }
    }

    /// Offset the next record is appended at.
    pub(crate) fn log_end(&self) -> std::io::Result<u64> {
        match self {
            Self::RustV2 { log, .. } => log.len(),
            Self::Java { log, .. } => Ok(log.end_offset()),
        }
    }

    /// Appends records and returns their positions.
    pub(crate) fn append_records(&self, records: &[TimerLogAppend]) -> std::io::Result<Vec<u64>> {
        match self {
            Self::RustV2 { log, .. } => {
                let records: Vec<_> = records
                    .iter()
                    .map(|append| (append.record, append.generation))
                    .collect();
                log.append_batch(&records)
            }
            Self::Java { log, .. } => {
                let units = records.iter().map(java_unit).collect::<std::io::Result<Vec<_>>>()?;
                log.append_batch(&units).map_err(std::io::Error::other)
            }
        }
    }

    /// Reads the record at `position` with its delivery generation. A Java unit keeps neither the
    /// queue offset nor the generation; they read as `-1` and `0`.
    pub(crate) fn read_record(&self, position: u64) -> std::io::Result<(TimerLogRecord, u64)> {
        match self {
            Self::RustV2 { log, .. } => log.read_record(position),
            Self::Java { log, .. } => {
                let unit = log.read_unit(position).map_err(std::io::Error::other)?;
                Ok((java_record(&unit), 0))
            }
        }
    }

    /// Reads the records of `slot` from its last one back and passes each, with its position
    /// and generation, to `visit`. Returns how many of them come before the slot's first record:
    /// entries a drain already processed, kept for their tombstones.
    ///
    /// A V2 slot counts its records, so the walk reads `slot.num` of them and `processed` more.
    /// A Java slot cannot be counted, since Java subtracts tombstones, so the walk follows the
    /// chain to its start, or to the oldest file left, and counts what lies before the first
    /// record itself.
    pub(crate) fn walk_slot(
        &self,
        slot: Slot,
        processed: usize,
        mut visit: impl FnMut(i64, TimerLogRecord, u64) -> std::io::Result<()>,
    ) -> std::io::Result<usize> {
        match self {
            Self::RustV2 { .. } => {
                let total = usize::try_from(slot.num.max(0))
                    .unwrap_or_default()
                    .saturating_add(processed);
                let mut position = slot.last_pos;
                for _ in 0..total {
                    if position < 0 {
                        break;
                    }
                    let (record, generation) = self.read_record(position as u64)?;
                    visit(position, record, generation)?;
                    position = record.prev_pos;
                }
                Ok(processed)
            }
            Self::Java { log, .. } => {
                // Every unit is at most once on a chain; a longer walk means a cycle.
                let limit = (log.end_offset().saturating_sub(log.min_offset()) / JAVA_TIMER_LOG_UNIT_SIZE as u64)
                    .saturating_add(1);
                let mut position = slot.last_pos;
                let mut before_first = None::<usize>;
                let mut steps = 0u64;
                let mut units = log.chain_reader();
                // Cleanup deletes only files without live units, so a link below the oldest
                // file left reaches units that were already delivered.
                let oldest = i64::try_from(log.min_offset()).unwrap_or(i64::MAX);
                while position >= oldest {
                    if steps == limit {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("timer slot {} has a chain that does not end", slot.time_ms),
                        ));
                    }
                    steps += 1;
                    let unit = units.read_unit(position as u64).map_err(std::io::Error::other)?;
                    let record = java_record(&unit);
                    visit(position, record, 0)?;
                    before_first = match before_first {
                        Some(count) => Some(count + 1),
                        None if position == slot.first_pos => Some(0),
                        None => None,
                    };
                    position = record.prev_pos;
                }
                Ok(before_first.unwrap_or_default())
            }
        }
    }

    /// Records a full walk of `slot` must find, when the format counts them exactly.
    pub(crate) fn exact_chain_len(&self, slot: Slot, processed: usize) -> Option<usize> {
        match self {
            Self::RustV2 { .. } => usize::try_from(slot.num.max(0))
                .ok()
                .map(|count| count.saturating_add(processed)),
            Self::Java { .. } => None,
        }
    }

    /// Deletes the log files that lie entirely before `oldest_live`, the oldest position a
    /// pending record can still be read from, or before the log end when nothing is pending.
    /// V2 removes sealed segments below its durable length too; Java keeps its last file.
    /// Returns how many files were deleted.
    pub(crate) fn collect_garbage(&self, oldest_live: Option<i64>) -> std::io::Result<usize> {
        let live = match oldest_live {
            Some(position) => u64::try_from(position).unwrap_or_default(),
            None => self.log_end()?,
        };
        match self {
            Self::RustV2 { log, .. } => {
                let durable = log.durable_length()?;
                log.gc(live, durable, durable)
            }
            Self::Java { log, .. } => log.delete_files_below(live).map_err(std::io::Error::other),
        }
    }

    /// Makes the log durable and returns its durable length.
    pub(crate) fn flush_log(&self) -> std::io::Result<i64> {
        match self {
            Self::RustV2 { log, .. } => {
                log.flush()?;
                Ok(log.len()? as i64)
            }
            Self::Java { log, .. } => {
                let flushed = log.flush().map_err(std::io::Error::other)?;
                i64::try_from(flushed).map_err(std::io::Error::other)
            }
        }
    }

    /// Writes the wheel's changed pages and returns the generation that holds them; the Java
    /// wheel has a single copy and returns `0`.
    pub(crate) fn flush_wheel(&self) -> std::io::Result<u64> {
        match self {
            Self::RustV2 { wheel, .. } => wheel.flush_generation(),
            Self::Java { wheel, .. } => wheel.flush().map(|()| 0).map_err(std::io::Error::other),
        }
    }

    /// Completes a checkpoint that names `wheel_generation` and was written as
    /// `checkpoint_generation`.
    pub(crate) fn commit_checkpoint(&self, wheel_generation: u64, checkpoint_generation: u64) -> std::io::Result<()> {
        match self {
            Self::RustV2 { log, wheel } => {
                wheel.commit_generation(wheel_generation)?;
                if checkpoint_generation > 0 {
                    log.mark_clean_checkpoint(checkpoint_generation)?;
                }
                Ok(())
            }
            Self::Java { .. } => Ok(()),
        }
    }

    /// Appends raw bytes to the log.
    pub(crate) fn append_raw(&self, payload: &[u8]) -> std::io::Result<u64> {
        match self {
            Self::RustV2 { log, .. } => log.append(payload),
            Self::Java { .. } => Err(java_raw_access()),
        }
    }

    /// Reads raw bytes from the log.
    pub(crate) fn read_raw(&self, offset: u64, length: usize) -> std::io::Result<Vec<u8>> {
        match self {
            Self::RustV2 { log, .. } => log.read_at(offset, length),
            Self::Java { .. } => Err(java_raw_access()),
        }
    }

    /// Flushes and releases the files. Failures are left to the next load to repair.
    pub(crate) fn shutdown(&self) {
        match self {
            Self::RustV2 { log, wheel } => {
                let _ = log.shutdown();
                let _ = wheel.shutdown(true);
            }
            Self::Java { log, wheel } => {
                let _ = log.flush();
                let _ = wheel.flush();
            }
        }
    }
}

/// The record of a Java unit, which keeps no queue offset.
fn java_record(unit: &JavaTimerLogUnit) -> TimerLogRecord {
    TimerLogRecord {
        deliver_time_ms: unit.slot_time_ms(),
        commit_log_offset: unit.commit_log_offset,
        size: unit.size,
        queue_offset: -1,
        prev_pos: unit.prev_pos,
        magic: unit.magic,
    }
}

fn java_unit(append: &TimerLogAppend) -> std::io::Result<JavaTimerLogUnit> {
    let delay_ms = i32::try_from(append.record.deliver_time_ms.saturating_sub(append.write_time_ms)).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "a Java timer log unit stores its slot within an int of milliseconds from its write time",
        )
    })?;
    Ok(JavaTimerLogUnit {
        prev_pos: append.record.prev_pos,
        magic: append.record.magic,
        curr_write_time_ms: append.write_time_ms,
        delay_ms,
        commit_log_offset: append.record.commit_log_offset,
        size: append.record.size,
        topic_hash: append.topic_hash,
        reserved: 0,
    })
}

fn java_raw_access() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "raw timer log access is only available for the Rust V2 format",
    )
}

/// Whether the timer log directory holds a file a Java Broker wrote: a 20-digit file that starts
/// with a unit or a blank, or one of exactly `file_size` bytes that starts with zeros. A legacy
/// Rust V1 file starts with a delivery time instead.
pub(crate) fn holds_java_timer_log(timer_log_dir: &Path, file_size: u64) -> std::io::Result<bool> {
    let entries = match std::fs::read_dir(timer_log_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let is_segment_name = name
            .to_str()
            .is_some_and(|name| name.len() == 20 && name.bytes().all(|byte| byte.is_ascii_digit()));
        if !is_segment_name || !entry.file_type()?.is_file() {
            continue;
        }
        let mut head = [0u8; JAVA_TIMER_LOG_UNIT_SIZE];
        let read = std::io::Read::read(&mut std::fs::File::open(entry.path())?, &mut head)?;
        let head = &head[..read];
        let empty_java_file = entry.metadata()?.len() == file_size && head.iter().all(|byte| *byte == 0);
        if empty_java_file || (read > 0 && JavaTimerLogRecord::decode(head).is_ok()) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::timer::timer_message_store::MAGIC_DEFAULT;
    use crate::timer::timer_message_store::MAGIC_DELETE;

    const FILE_SIZE: u64 = 200;

    fn java_storage(directory: &Path) -> TimerStorage {
        let log = JavaTimerLog::new(directory.join("timerlog"), FILE_SIZE).unwrap();
        log.load().unwrap();
        let wheel = JavaTimerWheel::open(directory.join("timerwheel"), 64, 1_000).unwrap();
        TimerStorage::Java { log, wheel }
    }

    fn append(storage: &TimerStorage, slot_time_ms: i64, prev_pos: i64, magic: i32) -> i64 {
        let positions = storage
            .append_records(&[TimerLogAppend {
                record: TimerLogRecord {
                    deliver_time_ms: slot_time_ms,
                    commit_log_offset: 7,
                    size: 9,
                    queue_offset: 3,
                    prev_pos,
                    magic,
                },
                generation: 5,
                write_time_ms: slot_time_ms - 2_000,
                topic_hash: 11,
            }])
            .unwrap();
        positions[0] as i64
    }

    #[test]
    fn java_layout_skips_the_blank_and_counts_tombstones_down() {
        let layout = TimerLayout::Java { file_size: FILE_SIZE };
        let first = layout.place(0);
        let second = layout.place(layout.end_after(first));
        let third = layout.place(layout.end_after(second));
        let fourth = layout.place(layout.end_after(third));
        assert_eq!([first, second, third, fourth], [0, 52, 104, 200]);
        assert_eq!(layout.count_after(Some(1), true), 0);
        assert_eq!(layout.count_after(None, true), -1);
        assert!(layout.has_records(&Slot::new_with_num_magic(1_000, 0, 0, 0, 0)));
        assert!(!TimerLayout::RustV2.has_records(&Slot::new_with_num_magic(1_000, 0, 0, 0, 0)));
        assert_eq!(TimerLayout::RustV2.count_after(Some(1), true), 2);
    }

    #[test]
    fn java_records_round_trip_without_queue_offset_or_generation() {
        let directory = tempdir().unwrap();
        let storage = java_storage(directory.path());
        let position = append(&storage, 1_700_000_005_000, -1, MAGIC_DEFAULT);
        let (record, generation) = storage.read_record(position as u64).unwrap();
        assert_eq!(record.deliver_time_ms, 1_700_000_005_000);
        assert_eq!((record.commit_log_offset, record.size, record.prev_pos), (7, 9, -1));
        assert_eq!((record.queue_offset, generation), (-1, 0));
        assert!(storage.read_raw(0, 52).is_err());
    }

    #[test]
    fn java_walk_follows_the_chain_past_a_count_that_tombstones_lowered() {
        let directory = tempdir().unwrap();
        let storage = java_storage(directory.path());
        let slot_time_ms = 1_700_000_005_000;
        let a = append(&storage, slot_time_ms, -1, MAGIC_DEFAULT);
        let b = append(&storage, slot_time_ms, a, MAGIC_DEFAULT);
        let tombstone = append(&storage, slot_time_ms, b, MAGIC_DEFAULT | MAGIC_DELETE);
        // Java's count: two messages and one tombstone.
        let slot = Slot::new_with_num_magic(slot_time_ms, a, tombstone, 1, 0);

        let mut walked = Vec::new();
        let processed = storage
            .walk_slot(slot, 0, |position, _, _| {
                walked.push(position);
                Ok(())
            })
            .unwrap();
        assert_eq!((walked, processed), (vec![tombstone, b, a], 0));
        assert_eq!(storage.exact_chain_len(slot, 0), None);

        // After a drain processed `a`, the slot starts at `b`; `a` is still walked for its
        // tombstones and reported as processed.
        let drained = Slot::new_with_num_magic(slot_time_ms, b, tombstone, 2, 0);
        let mut walked = Vec::new();
        let processed = storage
            .walk_slot(drained, 0, |position, _, _| {
                walked.push(position);
                Ok(())
            })
            .unwrap();
        assert_eq!((walked, processed), (vec![tombstone, b, a], 1));
    }

    #[test]
    fn java_walk_ends_at_the_oldest_file_left_after_cleanup() {
        let directory = tempdir().unwrap();
        let storage = java_storage(directory.path());
        let slot_time_ms = 1_700_000_005_000;
        // Three units fill the first file; the fourth opens the file at 200.
        let mut previous = -1;
        for _ in 0..4 {
            previous = append(&storage, slot_time_ms, previous, MAGIC_DEFAULT);
        }
        assert_eq!(previous, 200);
        // The first file holds only delivered units, so cleanup removes it.
        assert_eq!(storage.collect_garbage(Some(200)).unwrap(), 1);

        let drained = Slot::new_with_num_magic(slot_time_ms, 200, 200, 1, 0);
        let mut walked = Vec::new();
        let processed = storage
            .walk_slot(drained, 0, |position, _, _| {
                walked.push(position);
                Ok(())
            })
            .unwrap();
        assert_eq!((walked, processed), (vec![200], 0));
    }

    #[test]
    fn java_timer_log_files_are_told_apart_from_a_rust_v1_log() {
        let directory = tempdir().unwrap();
        let timer_log = directory.path().join("timerlog");
        assert!(!holds_java_timer_log(&timer_log, FILE_SIZE).unwrap());
        std::fs::create_dir_all(timer_log.join("v2")).unwrap();
        // A V1 record starts with its delivery time.
        let mut v1 = vec![0u8; FILE_SIZE as usize];
        v1[..8].copy_from_slice(&1_700_000_005_000i64.to_be_bytes());
        std::fs::write(timer_log.join("00000000000000000000"), &v1).unwrap();
        assert!(!holds_java_timer_log(&timer_log, FILE_SIZE).unwrap());

        std::fs::remove_file(timer_log.join("00000000000000000000")).unwrap();
        let storage = java_storage(directory.path());
        append(&storage, 1_700_000_005_000, -1, MAGIC_DEFAULT);
        drop(storage);
        assert!(holds_java_timer_log(&timer_log, FILE_SIZE).unwrap());
    }
}
