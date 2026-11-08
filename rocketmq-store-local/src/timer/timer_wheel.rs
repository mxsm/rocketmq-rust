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

use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::timer::metrics::TimerStorageMetrics;
use crate::timer::metrics::TimerStorageMetricsSnapshot;
use crate::timer::paged_timer_wheel::PagedTimerWheel;
use crate::timer::paged_timer_wheel::PagedTimerWheelFailure;
use crate::timer::slot::Slot;

const DEFAULT_PAGE_SIZE: usize = 4_096;
const V2_SUFFIX: &str = "v2";
const MIGRATION_COMMITTED: &str = "MIGRATION_COMMITTED";

/// Compatibility facade that keeps the existing Wheel API while persisting V2 dirty pages.
pub struct TimerWheel {
    file_name: PathBuf,
    slots_total: usize,
    precision_ms: u64,
    page_size: usize,
    metrics: Arc<TimerStorageMetrics>,
    /// Whether a legacy `timerwheel` file is imported into the first V2 generation.
    import_legacy: bool,
    inner: Mutex<Option<PagedTimerWheel>>,
}

impl TimerWheel {
    pub fn new<P: AsRef<Path>>(file_name: P, slots_total: usize, precision_ms: u64) -> Self {
        Self::with_page_size_and_metrics(
            file_name,
            slots_total,
            precision_ms,
            DEFAULT_PAGE_SIZE,
            Arc::new(TimerStorageMetrics::default()),
        )
    }

    pub fn with_page_size_and_metrics<P: AsRef<Path>>(
        file_name: P,
        slots_total: usize,
        precision_ms: u64,
        page_size: usize,
        metrics: Arc<TimerStorageMetrics>,
    ) -> Self {
        Self {
            file_name: file_name.as_ref().to_path_buf(),
            slots_total,
            precision_ms: precision_ms.max(1),
            page_size,
            metrics,
            import_legacy: true,
            inner: Mutex::new(None),
        }
    }

    /// Leaves a `timerwheel` file at the legacy path alone instead of importing it: it belongs
    /// to another format, such as an Apache RocketMQ 5.x wheel that shares the name.
    pub fn ignoring_legacy_file(mut self) -> Self {
        self.import_legacy = false;
        self
    }

    pub fn load(&self) -> std::io::Result<()> {
        self.load_at_generation(u64::MAX)
    }

    pub fn load_at_generation(&self, committed_generation: u64) -> std::io::Result<()> {
        let paged = PagedTimerWheel::new_checked(
            self.v2_directory(),
            self.wheel_len(),
            self.page_size,
            Arc::clone(&self.metrics),
        )
        .map_err(as_io_error)?;
        paged.load_checked(committed_generation).map_err(as_io_error)?;
        if committed_generation == 0 && self.should_import_legacy()? {
            paged
                .import_legacy_slots_checked(&self.read_legacy_slots()?)
                .map_err(as_io_error)?;
        }
        *self.inner.lock() = Some(paged);
        Ok(())
    }

    pub fn load_rebuilt(&self, committed_generation: u64, pending_slots: &[Slot]) -> std::io::Result<()> {
        let paged = PagedTimerWheel::new_checked(
            self.v2_directory(),
            self.wheel_len(),
            self.page_size,
            Arc::clone(&self.metrics),
        )
        .map_err(as_io_error)?;
        paged
            .reset_for_repair_checked(committed_generation)
            .map_err(as_io_error)?;
        let mut slots = vec![Slot::new_with_num_magic(0, 0, 0, 0, 0); self.wheel_len()];
        for slot in pending_slots.iter().copied().filter(|slot| slot.num > 0) {
            slots[self.get_slot_index(slot.time_ms)] = slot;
        }
        paged.import_legacy_slots_checked(&slots).map_err(as_io_error)?;
        for _ in 0..paged.dirty_page_count() {
            self.metrics.record_wheel_repair();
        }
        *self.inner.lock() = Some(paged);
        Ok(())
    }

    pub fn flush(&self) -> std::io::Result<()> {
        let generation = self.flush_generation()?;
        self.commit_generation(generation)
    }

    pub fn flush_generation(&self) -> std::io::Result<u64> {
        self.with_inner(PagedTimerWheel::flush_dirty_checked)
    }

    pub fn commit_generation(&self, generation: u64) -> std::io::Result<()> {
        self.with_inner(|wheel| wheel.commit_generation_checked(generation))?;
        if self.file_name.exists() && !self.migration_marker().exists() {
            let mut marker = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(self.migration_marker())?;
            marker.write_all(b"paged-timer-wheel-v2\n")?;
            marker.sync_data()?;
        }
        Ok(())
    }

    pub fn committed_generation(&self) -> u64 {
        self.inner
            .lock()
            .as_ref()
            .map(PagedTimerWheel::committed_generation)
            .unwrap_or_default()
    }

    pub fn shutdown(&self, flush: bool) -> std::io::Result<()> {
        if flush {
            self.flush()?;
        }
        Ok(())
    }

    pub fn get_slot(&self, time_ms: i64) -> Option<Slot> {
        let guard = self.inner.lock();
        let wheel = guard.as_ref()?;
        let slot = wheel.get_slot(self.get_slot_index(time_ms))?;
        (slot.time_ms == self.format_time_ms(time_ms)).then_some(slot)
    }

    pub fn put_slot(&self, time_ms: i64, first_pos: i64, last_pos: i64, num: i32, magic: i32) -> std::io::Result<()> {
        let index = self.get_slot_index(time_ms);
        let slot = Slot::new_with_num_magic(self.format_time_ms(time_ms), first_pos, last_pos, num, magic);
        self.with_inner(|wheel| wheel.put_slot_checked(index, slot))
    }

    /// Writes several slots at once, each at the wheel index of its `time_ms`.
    ///
    /// # Errors
    ///
    /// Returns an error, with no slot written, when the wheel is not loaded or a slot falls
    /// outside it.
    pub fn put_slots(&self, slots: &[Slot]) -> std::io::Result<()> {
        let updates: Vec<_> = slots
            .iter()
            .map(|slot| {
                (
                    self.get_slot_index(slot.time_ms),
                    Slot::new_with_num_magic(
                        self.format_time_ms(slot.time_ms),
                        slot.first_pos,
                        slot.last_pos,
                        slot.num,
                        slot.magic,
                    ),
                )
            })
            .collect();
        self.with_inner(|wheel| wheel.put_slots_checked(&updates))
    }

    pub fn get_num(&self, time_ms: i64) -> i64 {
        self.get_slot(time_ms).map(|slot| slot.num as i64).unwrap_or_default()
    }

    pub fn revise_slots<F>(&self, revise: F) -> std::io::Result<()>
    where
        F: FnMut(Slot) -> Slot,
    {
        self.with_inner(|wheel| {
            wheel.revise_slots(revise);
            Ok::<_, PagedTimerWheelFailure>(())
        })
    }

    /// Sums the records of the slots in the wheel window that starts at `time_start_ms`, like
    /// Java's `TimerWheel#getAllNum`. Only occupied slots are read, and the wheel is not copied.
    pub fn get_all_num(&self, time_start_ms: i64) -> i64 {
        let wheel_len = self.wheel_len();
        let start_index = self.get_slot_index(time_start_ms);
        let mut all_num = 0i64;
        self.visit_occupied_slots(|index, slot| {
            let offset = (index + wheel_len - start_index) % wheel_len;
            if slot.time_ms == self.format_time_ms(time_start_ms + offset as i64 * self.precision_ms as i64) {
                all_num += i64::from(slot.num);
            }
        });
        all_num
    }

    /// Calls `visit` with the wheel index and value of every slot that holds records, without
    /// copying the wheel. Does nothing before the wheel is loaded.
    pub fn visit_occupied_slots(&self, visit: impl FnMut(usize, Slot)) {
        if let Some(wheel) = self.inner.lock().as_ref() {
            wheel.visit_occupied_slots(visit);
        }
    }

    pub fn slots_snapshot(&self) -> Vec<Slot> {
        self.inner
            .lock()
            .as_ref()
            .map(PagedTimerWheel::slots_snapshot)
            .unwrap_or_default()
    }

    pub fn dirty_page_count(&self) -> usize {
        self.inner
            .lock()
            .as_ref()
            .map(PagedTimerWheel::dirty_page_count)
            .unwrap_or_default()
    }

    pub fn non_empty_page_count(&self) -> usize {
        self.inner
            .lock()
            .as_ref()
            .map(PagedTimerWheel::non_empty_page_count)
            .unwrap_or_default()
    }

    pub fn metrics_snapshot(&self) -> TimerStorageMetricsSnapshot {
        self.metrics.snapshot()
    }

    fn should_import_legacy(&self) -> std::io::Result<bool> {
        Ok(self.import_legacy
            && self.file_name.exists()
            && self.file_name.metadata()?.len() > 0
            && !self.migration_marker().exists())
    }

    fn read_legacy_slots(&self) -> std::io::Result<Vec<Slot>> {
        let expected_len = self.wheel_len() * Slot::SIZE as usize;
        let mut file = OpenOptions::new().read(true).open(&self.file_name)?;
        let length = file.metadata()?.len() as usize;
        if length != expected_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("legacy timer wheel length {length} does not match expected {expected_len}"),
            ));
        }
        let mut bytes = vec![0u8; expected_len];
        file.read_exact(&mut bytes)?;
        Ok(bytes.chunks_exact(Slot::SIZE as usize).map(decode_slot).collect())
    }

    fn with_inner<T, E>(&self, operation: impl FnOnce(&PagedTimerWheel) -> Result<T, E>) -> std::io::Result<T>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let guard = self.inner.lock();
        let wheel = guard
            .as_ref()
            .ok_or_else(|| std::io::Error::other("timer wheel is not loaded"))?;
        operation(wheel).map_err(as_io_error)
    }

    fn v2_directory(&self) -> PathBuf {
        PathBuf::from(format!("{}.{}", self.file_name.display(), V2_SUFFIX))
    }

    fn migration_marker(&self) -> PathBuf {
        self.v2_directory().join(MIGRATION_COMMITTED)
    }

    fn wheel_len(&self) -> usize {
        self.slots_total.saturating_mul(2)
    }

    fn get_slot_index(&self, time_ms: i64) -> usize {
        let wheel_len = self.wheel_len() as i64;
        (time_ms.div_euclid(self.precision_ms as i64).rem_euclid(wheel_len)) as usize
    }

    fn format_time_ms(&self, time_ms: i64) -> i64 {
        time_ms.div_euclid(self.precision_ms as i64) * self.precision_ms as i64
    }
}

fn decode_slot(bytes: &[u8]) -> Slot {
    Slot::new_with_num_magic(
        read_i64(bytes, 0),
        read_i64(bytes, 8),
        read_i64(bytes, 16),
        read_i32(bytes, 24),
        read_i32(bytes, 28),
    )
}

fn read_i64(bytes: &[u8], offset: usize) -> i64 {
    i64::from_be_bytes(bytes[offset..offset + 8].try_into().expect("fixed i64 field"))
}

fn read_i32(bytes: &[u8], offset: usize) -> i32 {
    i32::from_be_bytes(bytes[offset..offset + 4].try_into().expect("fixed i32 field"))
}

fn as_io_error(error: impl std::error::Error + Send + Sync + 'static) -> std::io::Error {
    std::io::Error::other(error)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn put_flush_and_reload_preserves_timer_wheel_slot() {
        let temp_dir = tempdir().unwrap();
        let timer_wheel = TimerWheel::with_page_size_and_metrics(
            temp_dir.path().join("timerwheel"),
            16,
            1_000,
            288,
            Arc::new(TimerStorageMetrics::default()),
        );
        timer_wheel.load().unwrap();
        timer_wheel.put_slot(5_000, 10, 20, 1, 2).unwrap();
        timer_wheel.flush().unwrap();

        let reloaded = TimerWheel::with_page_size_and_metrics(
            temp_dir.path().join("timerwheel"),
            16,
            1_000,
            288,
            Arc::new(TimerStorageMetrics::default()),
        );
        reloaded.load().unwrap();
        assert_eq!(
            reloaded.get_slot(5_000),
            Some(Slot::new_with_num_magic(5_000, 10, 20, 1, 2))
        );
    }

    #[test]
    fn get_all_num_counts_only_slots_inside_the_window() {
        let temp_dir = tempdir().unwrap();
        let wheel = TimerWheel::with_page_size_and_metrics(
            temp_dir.path().join("timerwheel"),
            16,
            1_000,
            288,
            Arc::new(TimerStorageMetrics::default()),
        );
        assert_eq!(wheel.get_all_num(5_000), 0);
        wheel.load().unwrap();
        wheel.put_slot(5_000, 0, 40, 2, 1).unwrap();
        wheel.put_slot(9_000, 80, 160, 3, 1).unwrap();
        wheel.put_slot(30_000, 200, 200, 1, 1).unwrap();

        assert_eq!(wheel.get_all_num(5_000), 6);
        // The 32-slot window starting at 6 s ends at 37 s, so the slot at 5 s is behind it.
        assert_eq!(wheel.get_all_num(6_000), 4);
        // From 31 s, the slots at 5 s and 9 s would be 37 s and 41 s; they hold older records.
        assert_eq!(wheel.get_all_num(31_000), 0);
    }

    #[test]
    fn revise_slots_marks_only_changed_pages_dirty() {
        let temp_dir = tempdir().unwrap();
        let wheel = TimerWheel::with_page_size_and_metrics(
            temp_dir.path().join("timerwheel"),
            16,
            1_000,
            288,
            Arc::new(TimerStorageMetrics::default()),
        );
        wheel.load().unwrap();
        wheel.put_slot(5_000, 10, 20, 1, 2).unwrap();
        wheel.flush().unwrap();
        wheel
            .revise_slots(|slot| {
                if slot.time_ms == 5_000 {
                    Slot::new_with_num_magic(0, 0, 0, 0, 0)
                } else {
                    slot
                }
            })
            .unwrap();
        assert_eq!(wheel.dirty_page_count(), 1);
    }
}
