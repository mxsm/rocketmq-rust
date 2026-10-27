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

//! The Apache RocketMQ 5.x `config/timercheck` file: one 4 KiB page whose first 56 bytes are
//! the [`TimerCheckpointRecord`], rewritten in place on every flush as Java's `TimerCheckpoint`
//! does.

use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;

use parking_lot::Mutex;

use crate::timer::checkpoint::TimerCheckpointRecord;
use crate::timer::checkpoint::TIMER_CHECKPOINT_SIZE;
use crate::timer::java_format::JAVA_TIMER_CHECKPOINT_FILE_SIZE;
use crate::timer::positional_io::read_exact_at;
use crate::timer::positional_io::write_all_at;

/// A `timercheck` file in the Apache RocketMQ 5.x format.
pub struct JavaTimerCheckpointFile {
    file: File,
    record: Mutex<TimerCheckpointRecord>,
}

impl JavaTimerCheckpointFile {
    /// Opens the checkpoint at `path`, creating it and its directory when missing. Like Java's
    /// memory map, a shorter file grows to one page with zeros, so a file holding only the
    /// 56-byte record reads the same.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the file cannot be created, extended or read.
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        if file.metadata()?.len() < JAVA_TIMER_CHECKPOINT_FILE_SIZE as u64 {
            file.set_len(JAVA_TIMER_CHECKPOINT_FILE_SIZE as u64)?;
        }
        let mut bytes = [0u8; TIMER_CHECKPOINT_SIZE];
        read_exact_at(&file, &mut bytes, 0)?;
        let record = TimerCheckpointRecord::decode(&bytes)?;
        Ok(Self {
            file,
            record: Mutex::new(record),
        })
    }

    /// The record last read or flushed.
    pub fn record(&self) -> TimerCheckpointRecord {
        *self.record.lock()
    }

    /// Writes `record` over the start of the page and syncs the file
    /// (`TimerCheckpoint.flush`).
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the file cannot be written or synced; [`record`](Self::record)
    /// then still returns the previous record.
    pub fn flush(&self, record: TimerCheckpointRecord) -> std::io::Result<()> {
        let mut current = self.record.lock();
        write_all_at(&self.file, &record.encode(), 0)?;
        self.file.sync_data()?;
        *current = record;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::timer::checkpoint::TimerCheckpointVersion;

    fn record() -> TimerCheckpointRecord {
        TimerCheckpointRecord {
            last_read_time_ms: 1_700_000_000_000,
            last_timer_log_flush_pos: 104_857_600,
            last_timer_queue_offset: 42,
            master_timer_queue_offset: 43,
            version: TimerCheckpointVersion {
                state_version: 1,
                timestamp: 1_700_000_000_123,
                counter: 9,
            },
        }
    }

    #[test]
    fn a_new_checkpoint_is_one_zeroed_page_and_flushes_seven_longs() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("config").join("timercheck");
        let checkpoint = JavaTimerCheckpointFile::open(&path).unwrap();
        assert_eq!(checkpoint.record(), TimerCheckpointRecord::default());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 4_096);

        checkpoint.flush(record()).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), 4_096);
        assert_eq!(&bytes[0..8], &1_700_000_000_000i64.to_be_bytes());
        assert_eq!(&bytes[8..16], &104_857_600i64.to_be_bytes());
        assert_eq!(&bytes[48..56], &9i64.to_be_bytes());
        assert!(bytes[56..].iter().all(|byte| *byte == 0));
        drop(checkpoint);
        assert_eq!(JavaTimerCheckpointFile::open(&path).unwrap().record(), record());
    }

    #[test]
    fn a_record_only_file_grows_to_a_page_and_keeps_its_values() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("timercheck");
        std::fs::write(&path, record().encode()).unwrap();

        let checkpoint = JavaTimerCheckpointFile::open(&path).unwrap();
        assert_eq!(checkpoint.record(), record());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 4_096);
    }
}
