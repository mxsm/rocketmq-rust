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

use rocketmq_store_local::timer::service::TimerLogRecord;

use crate::timer::slot::Slot;
use crate::timer::timer_log::TimerLog;
use crate::timer::timer_wheel::TimerWheel;

/// The loaded timer files of the JavaCompat engine.
pub(crate) enum TimerStorage {
    /// The segmented V2 log and the paged A/B wheel.
    RustV2 { log: TimerLog, wheel: TimerWheel },
}

/// Where the records of one batch land, so a record can link to an earlier record of the same
/// batch before the batch is written.
#[derive(Clone, Copy, Debug)]
pub(crate) enum TimerRecordLayout {
    /// Contiguous records of one logical size.
    RustV2,
}

impl TimerRecordLayout {
    /// Position of a record appended at `end`.
    pub(crate) fn place(self, end: i64) -> i64 {
        match self {
            Self::RustV2 => end,
        }
    }

    /// End of the log after a record placed at `position`.
    pub(crate) fn end_after(self, position: i64) -> i64 {
        match self {
            Self::RustV2 => position + TimerLogRecord::SIZE as i64,
        }
    }
}

impl TimerStorage {
    pub(crate) fn record_layout(&self) -> TimerRecordLayout {
        match self {
            Self::RustV2 { .. } => TimerRecordLayout::RustV2,
        }
    }

    pub(crate) fn get_slot(&self, time_ms: i64) -> Option<Slot> {
        match self {
            Self::RustV2 { wheel, .. } => wheel.get_slot(time_ms),
        }
    }

    pub(crate) fn put_slot(&self, slot: Slot) -> std::io::Result<()> {
        match self {
            Self::RustV2 { wheel, .. } => {
                wheel.put_slot(slot.time_ms, slot.first_pos, slot.last_pos, slot.num, slot.magic)
            }
        }
    }

    pub(crate) fn put_slots(&self, slots: &[Slot]) -> std::io::Result<()> {
        match self {
            Self::RustV2 { wheel, .. } => wheel.put_slots(slots),
        }
    }

    /// Marks the slot for `time_ms` as holding nothing to deliver.
    pub(crate) fn clear_slot(&self, time_ms: i64) -> std::io::Result<()> {
        match self {
            Self::RustV2 { wheel, .. } => wheel.put_slot(time_ms, 0, 0, 0, 0),
        }
    }

    /// Messages counted in the wheel window that starts at `time_start_ms`.
    pub(crate) fn all_num(&self, time_start_ms: i64) -> i64 {
        match self {
            Self::RustV2 { wheel, .. } => wheel.get_all_num(time_start_ms),
        }
    }

    /// Calls `visit` with every slot that holds records.
    pub(crate) fn visit_occupied_slots(&self, mut visit: impl FnMut(Slot)) {
        match self {
            Self::RustV2 { wheel, .. } => wheel.visit_occupied_slots(|_, slot| visit(slot)),
        }
    }

    /// Offset the next record is appended at.
    pub(crate) fn log_end(&self) -> std::io::Result<u64> {
        match self {
            Self::RustV2 { log, .. } => log.len(),
        }
    }

    /// Appends records with their delivery generations and returns their positions.
    pub(crate) fn append_records(&self, records: &[(TimerLogRecord, u64)]) -> std::io::Result<Vec<u64>> {
        match self {
            Self::RustV2 { log, .. } => log.append_batch(records),
        }
    }

    /// Reads the record at `position` with its delivery generation.
    pub(crate) fn read_record(&self, position: u64) -> std::io::Result<(TimerLogRecord, u64)> {
        match self {
            Self::RustV2 { log, .. } => log.read_record(position),
        }
    }

    /// Makes the log durable and returns its durable length.
    pub(crate) fn flush_log(&self) -> std::io::Result<i64> {
        match self {
            Self::RustV2 { log, .. } => {
                log.flush()?;
                Ok(log.len()? as i64)
            }
        }
    }

    /// Writes the wheel's changed pages and returns the generation that holds them.
    pub(crate) fn flush_wheel(&self) -> std::io::Result<u64> {
        match self {
            Self::RustV2 { wheel, .. } => wheel.flush_generation(),
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
        }
    }

    /// Appends raw bytes to the log.
    pub(crate) fn append_raw(&self, payload: &[u8]) -> std::io::Result<u64> {
        match self {
            Self::RustV2 { log, .. } => log.append(payload),
        }
    }

    /// Reads raw bytes from the log.
    pub(crate) fn read_raw(&self, offset: u64, length: usize) -> std::io::Result<Vec<u8>> {
        match self {
            Self::RustV2 { log, .. } => log.read_at(offset, length),
        }
    }

    /// Flushes and releases the files. Failures are left to the next load to repair.
    pub(crate) fn shutdown(&self) {
        match self {
            Self::RustV2 { log, wheel } => {
                let _ = log.shutdown();
                let _ = wheel.shutdown(true);
            }
        }
    }
}
