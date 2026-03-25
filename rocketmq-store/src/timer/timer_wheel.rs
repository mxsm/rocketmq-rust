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

use parking_lot::Mutex;
use rocketmq_common::UtilAll::ensure_dir_ok;

use crate::timer::slot::Slot;

pub struct TimerWheel {
    file_name: PathBuf,
    slots_total: usize,
    precision_ms: u64,
    slots: Mutex<Vec<Slot>>,
}

impl TimerWheel {
    pub fn new<P: AsRef<Path>>(file_name: P, slots_total: usize, precision_ms: u64) -> Self {
        Self {
            file_name: file_name.as_ref().to_path_buf(),
            slots_total,
            precision_ms: precision_ms.max(1),
            slots: Mutex::new(Vec::new()),
        }
    }

    pub fn load(&self) -> std::io::Result<()> {
        if let Some(parent) = self.file_name.parent() {
            ensure_dir_ok(parent.to_string_lossy().as_ref());
        }

        let expected_len = self.expected_file_len();
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&self.file_name)?;
        let file_len = file.metadata()?.len();
        if file_len == 0 {
            file.set_len(expected_len as u64)?;
            *self.slots.lock() = vec![empty_slot(); self.wheel_len()];
            return Ok(());
        }
        if file_len != expected_len as u64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "timer wheel length {} does not match expected {}",
                    file_len, expected_len
                ),
            ));
        }

        let mut buffer = vec![0; expected_len];
        file.read_exact(&mut buffer)?;
        let mut slots = Vec::with_capacity(self.wheel_len());
        for chunk in buffer.chunks_exact(Slot::SIZE as usize) {
            slots.push(Slot::new_with_num_magic(
                read_i64(&chunk[0..8]),
                read_i64(&chunk[8..16]),
                read_i64(&chunk[16..24]),
                read_i32(&chunk[24..28]),
                read_i32(&chunk[28..32]),
            ));
        }
        *self.slots.lock() = slots;
        Ok(())
    }

    pub fn flush(&self) -> std::io::Result<()> {
        let slots = self.slots.lock();
        if slots.is_empty() {
            return Ok(());
        }

        if let Some(parent) = self.file_name.parent() {
            ensure_dir_ok(parent.to_string_lossy().as_ref());
        }

        let mut buffer = vec![0; self.expected_file_len()];
        for (index, slot) in slots.iter().enumerate() {
            let start = index * Slot::SIZE as usize;
            buffer[start..start + 8].copy_from_slice(&slot.time_ms.to_be_bytes());
            buffer[start + 8..start + 16].copy_from_slice(&slot.first_pos.to_be_bytes());
            buffer[start + 16..start + 24].copy_from_slice(&slot.last_pos.to_be_bytes());
            buffer[start + 24..start + 28].copy_from_slice(&slot.num.to_be_bytes());
            buffer[start + 28..start + 32].copy_from_slice(&slot.magic.to_be_bytes());
        }

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&self.file_name)?;
        file.write_all(&buffer)?;
        file.sync_data()
    }

    pub fn shutdown(&self, flush: bool) -> std::io::Result<()> {
        if flush {
            self.flush()?;
        }
        Ok(())
    }

    pub fn get_slot(&self, time_ms: i64) -> Option<Slot> {
        let slots = self.slots.lock();
        if slots.is_empty() {
            return None;
        }

        let slot = slots.get(self.get_slot_index(time_ms)).copied()?;
        if slot.time_ms == self.format_time_ms(time_ms) {
            Some(slot)
        } else {
            None
        }
    }

    pub fn put_slot(&self, time_ms: i64, first_pos: i64, last_pos: i64, num: i32, magic: i32) -> std::io::Result<()> {
        let mut slots = self.slots.lock();
        if slots.is_empty() {
            return Err(std::io::Error::other("timer wheel is not loaded"));
        }

        let index = self.get_slot_index(time_ms);
        slots[index] = Slot::new_with_num_magic(self.format_time_ms(time_ms), first_pos, last_pos, num, magic);
        Ok(())
    }

    pub fn get_num(&self, time_ms: i64) -> i64 {
        self.get_slot(time_ms).map(|slot| slot.num as i64).unwrap_or_default()
    }

    pub fn get_all_num(&self, time_start_ms: i64) -> i64 {
        let slots = self.slots.lock();
        if slots.is_empty() {
            return 0;
        }

        let mut all_num = 0i64;
        let start_index = self.get_slot_index(time_start_ms);
        for offset in 0..self.wheel_len() {
            let index = (start_index + offset) % self.wheel_len();
            let slot = slots[index];
            if slot.time_ms == self.format_time_ms(time_start_ms + (offset as i64 * self.precision_ms as i64)) {
                all_num += slot.num as i64;
            }
        }
        all_num
    }

    fn expected_file_len(&self) -> usize {
        self.wheel_len() * Slot::SIZE as usize
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

fn empty_slot() -> Slot {
    Slot::new_with_num_magic(0, 0, 0, 0, 0)
}

fn read_i64(buffer: &[u8]) -> i64 {
    i64::from_be_bytes(buffer.try_into().expect("timer wheel i64 field size must be 8 bytes"))
}

fn read_i32(buffer: &[u8]) -> i32 {
    i32::from_be_bytes(buffer.try_into().expect("timer wheel i32 field size must be 4 bytes"))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn put_flush_and_reload_preserves_timer_wheel_slot() {
        let temp_dir = tempdir().unwrap();
        let timer_wheel = TimerWheel::new(temp_dir.path().join("timerwheel"), 16, 1_000);
        timer_wheel.load().unwrap();
        timer_wheel.put_slot(5_000, 10, 20, 1, 2).unwrap();
        timer_wheel.flush().unwrap();

        let reloaded = TimerWheel::new(temp_dir.path().join("timerwheel"), 16, 1_000);
        reloaded.load().unwrap();

        let slot = reloaded.get_slot(5_000).unwrap();
        assert_eq!(slot.time_ms, 5_000);
        assert_eq!(slot.first_pos, 10);
        assert_eq!(slot.last_pos, 20);
        assert_eq!(slot.num, 1);
        assert_eq!(slot.magic, 2);
    }
}
