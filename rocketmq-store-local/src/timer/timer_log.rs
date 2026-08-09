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
use crate::timer::migration::detect_timer_log_layout;
use crate::timer::migration::reset_migration_directory;
use crate::timer::migration::TimerMigrationLayout;
use crate::timer::segmented_timer_log::SegmentedTimerLog;
use crate::timer::segmented_timer_log::SegmentedTimerLogError;
use crate::timer::segmented_timer_log::TimerLogReadBatch;
use crate::timer::segmented_timer_log::TimerLogV2Record;
use crate::timer::service::TimerLogRecord;
use crate::timer::storage_format::TimerLogOffset;

const LEGACY_ACTIVE_FILE_NAME: &str = "00000000000000000000";
const V2_DIRECTORY_NAME: &str = "v2";
const V2_LOG_DIRECTORY_NAME: &str = "log";
const MIGRATION_DIRECTORY_NAME: &str = "v2.migrating";
const COMMITTED_MARKER: &str = "MIGRATION_COMMITTED";
const CLEAN_CHECKPOINT_MARKER: &str = "V2_CLEAN_CHECKPOINT";
const DEFAULT_HANDLE_CACHE: usize = 32;

/// Narrow compatibility facade over the V2 segmented timer log.
///
/// Callers continue to exchange the 40-byte Java-compatible logical record. The facade validates
/// and stores it as a checksummed V2 physical record and preserves every logical V1 offset.
pub struct TimerLog {
    dir_path: PathBuf,
    file_size: usize,
    metrics: Arc<TimerStorageMetrics>,
    inner: Mutex<Option<SegmentedTimerLog>>,
}

impl TimerLog {
    pub fn new<P: AsRef<Path>>(dir_path: P, file_size: usize) -> Self {
        Self::with_metrics(dir_path, file_size, Arc::new(TimerStorageMetrics::default()))
    }

    pub fn with_metrics<P: AsRef<Path>>(dir_path: P, file_size: usize, metrics: Arc<TimerStorageMetrics>) -> Self {
        Self {
            dir_path: dir_path.as_ref().to_path_buf(),
            file_size,
            metrics,
            inner: Mutex::new(None),
        }
    }

    pub fn load(&self) -> std::io::Result<bool> {
        std::fs::create_dir_all(&self.dir_path)?;
        let v2_root = self.v2_root();
        match detect_timer_log_layout(
            &self.dir_path,
            LEGACY_ACTIVE_FILE_NAME,
            V2_DIRECTORY_NAME,
            COMMITTED_MARKER,
        )? {
            TimerMigrationLayout::V2Complete => {}
            TimerMigrationLayout::V1Only | TimerMigrationLayout::Empty => self.initialize_or_migrate()?,
            TimerMigrationLayout::IncompleteV2 => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "incomplete V2 timer log exists at {}; restore V1 or remove the incomplete migration explicitly",
                        v2_root.display()
                    ),
                ));
            }
        }
        let log = self.open_segmented(v2_root.join(V2_LOG_DIRECTORY_NAME))?;
        log.load().map_err(as_io_error)?;
        *self.inner.lock() = Some(log);
        Ok(true)
    }

    pub fn append(&self, payload: &[u8]) -> std::io::Result<u64> {
        let record = TimerLogRecord::decode(payload)?;
        self.append_record(record, 0)
    }

    pub fn append_record(&self, record: TimerLogRecord, generation: u64) -> std::io::Result<u64> {
        Ok(self
            .with_inner(|log| log.append(TimerLogV2Record::from_legacy(record, generation)))?
            .get())
    }

    pub fn append_batch(&self, records: &[(TimerLogRecord, u64)]) -> std::io::Result<Vec<u64>> {
        let records: Vec<_> = records
            .iter()
            .map(|(record, generation)| TimerLogV2Record::from_legacy(*record, *generation))
            .collect();
        Ok(self
            .with_inner(|log| log.append_batch(&records))?
            .into_iter()
            .map(TimerLogOffset::get)
            .collect())
    }

    pub fn read_at(&self, offset: u64, length: usize) -> std::io::Result<Vec<u8>> {
        if length != TimerLogRecord::SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "timer log logical read length must be {}, got {length}",
                    TimerLogRecord::SIZE
                ),
            ));
        }
        let record = self.with_inner(|log| log.read(TimerLogOffset::new(offset)))?;
        Ok(record.to_legacy().encode().to_vec())
    }

    pub fn read_record(&self, offset: u64) -> std::io::Result<(TimerLogRecord, u64)> {
        let record = self.with_inner(|log| log.read(TimerLogOffset::new(offset)))?;
        Ok((record.to_legacy(), record.generation))
    }

    pub fn read_batch(&self, cursor: u64, max_messages: usize, max_bytes: usize) -> std::io::Result<TimerLogReadBatch> {
        self.with_inner(|log| log.read_batch(TimerLogOffset::new(cursor), max_messages, max_bytes))
    }

    pub fn len(&self) -> std::io::Result<u64> {
        self.with_inner(|log| Ok::<_, SegmentedTimerLogError>(log.len()))
    }

    pub fn is_empty(&self) -> std::io::Result<bool> {
        Ok(self.len()? == 0)
    }

    pub fn truncate(&self, length: u64) -> std::io::Result<()> {
        self.with_inner(|log| log.truncate(TimerLogOffset::new(length)))
    }

    pub fn flush(&self) -> std::io::Result<()> {
        self.with_inner(SegmentedTimerLog::flush)
    }

    pub fn flush_up_to(&self, offset: u64) -> std::io::Result<()> {
        self.with_inner(|log| log.flush_up_to(TimerLogOffset::new(offset)))
    }

    pub fn durable_length(&self) -> std::io::Result<u64> {
        self.with_inner(|log| Ok::<_, SegmentedTimerLogError>(log.durable_length()))
    }

    pub fn min_live_offset(&self) -> std::io::Result<u64> {
        self.with_inner(|log| Ok::<_, SegmentedTimerLogError>(log.min_live_offset().get()))
    }

    pub fn gc(&self, min_live_offset: u64, checkpoint: u64, snapshot: u64) -> std::io::Result<usize> {
        self.with_inner(|log| {
            log.gc(
                TimerLogOffset::new(min_live_offset),
                TimerLogOffset::new(checkpoint),
                TimerLogOffset::new(snapshot),
            )
        })
    }

    pub fn metrics_snapshot(&self) -> TimerStorageMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn mark_clean_checkpoint(&self, generation: u64) -> std::io::Result<()> {
        let path = self.v2_root().join(CLEAN_CHECKPOINT_MARKER);
        if path.exists() {
            return Ok(());
        }
        let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
        file.write_all(format!("{generation}\n").as_bytes())?;
        file.sync_data()
    }

    /// Explicitly removes the retained V1 source only after a clean V2 checkpoint and an external
    /// snapshot/rollback policy authorizes cleanup.
    pub fn cleanup_legacy_after_snapshot_release(&self, snapshot_released: bool) -> std::io::Result<bool> {
        let legacy = self.active_file_path();
        if !snapshot_released || !self.v2_root().join(CLEAN_CHECKPOINT_MARKER).exists() || !legacy.exists() {
            return Ok(false);
        }
        std::fs::remove_file(legacy)?;
        Ok(true)
    }

    pub fn shutdown(&self) -> std::io::Result<()> {
        self.flush()
    }

    pub fn active_file_path(&self) -> PathBuf {
        self.dir_path.join(LEGACY_ACTIVE_FILE_NAME)
    }

    pub fn v2_root(&self) -> PathBuf {
        self.dir_path.join(V2_DIRECTORY_NAME)
    }

    fn initialize_or_migrate(&self) -> std::io::Result<()> {
        let migration_root = reset_migration_directory(&self.dir_path, MIGRATION_DIRECTORY_NAME)?;
        let v2_root = self.v2_root();
        std::fs::create_dir_all(migration_root.join(V2_LOG_DIRECTORY_NAME))?;
        let migrated = self.open_segmented(migration_root.join(V2_LOG_DIRECTORY_NAME))?;
        migrated.load().map_err(as_io_error)?;

        let legacy_path = self.active_file_path();
        if legacy_path.exists() && legacy_path.metadata()?.len() > 0 {
            let mut legacy = OpenOptions::new().read(true).open(&legacy_path)?;
            let length = legacy.metadata()?.len();
            if !length.is_multiple_of(TimerLogRecord::SIZE as u64) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("legacy timer log length {length} is not record aligned"),
                ));
            }
            let mut bytes = [0u8; TimerLogRecord::SIZE];
            let mut old_offset = 0u64;
            while old_offset < length {
                legacy.read_exact(&mut bytes)?;
                let record = TimerLogRecord::decode(&bytes)?;
                let migrated_offset = migrated
                    .append(TimerLogV2Record::from_legacy(record, 0))
                    .map_err(as_io_error)?;
                if migrated_offset.get() != old_offset {
                    return Err(std::io::Error::other("timer log migration changed a logical offset"));
                }
                old_offset += TimerLogRecord::SIZE as u64;
            }
        }
        migrated.flush().map_err(as_io_error)?;
        write_marker(&migration_root.join(COMMITTED_MARKER))?;
        std::fs::rename(&migration_root, &v2_root)?;
        Ok(())
    }

    fn open_segmented(&self, directory: PathBuf) -> std::io::Result<SegmentedTimerLog> {
        SegmentedTimerLog::new(
            directory,
            self.file_size,
            DEFAULT_HANDLE_CACHE,
            Arc::clone(&self.metrics),
        )
        .map_err(as_io_error)
    }

    fn with_inner<T, E>(&self, operation: impl FnOnce(&SegmentedTimerLog) -> Result<T, E>) -> std::io::Result<T>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let guard = self.inner.lock();
        let log = guard
            .as_ref()
            .ok_or_else(|| std::io::Error::other("timer log is not loaded"))?;
        operation(log).map_err(as_io_error)
    }
}

fn write_marker(path: &Path) -> std::io::Result<()> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(b"timer-log-v2\n")?;
    file.sync_data()
}

fn as_io_error(error: impl std::error::Error + Send + Sync + 'static) -> std::io::Error {
    std::io::Error::other(error)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn record(queue_offset: i64, previous_offset: i64) -> TimerLogRecord {
        TimerLogRecord {
            deliver_time_ms: 1_000,
            commit_log_offset: queue_offset * 100,
            size: 64,
            queue_offset,
            prev_pos: previous_offset,
            magic: 1,
        }
    }

    #[test]
    fn append_and_reload_preserves_timer_log_record() {
        let temp_dir = tempdir().unwrap();
        let timer_log = TimerLog::new(temp_dir.path().join("timerlog"), 800);
        assert!(timer_log.load().unwrap());

        let expected = record(0, -1);
        let offset = timer_log.append(&expected.encode()).unwrap();
        timer_log.flush().unwrap();

        let reloaded = TimerLog::new(temp_dir.path().join("timerlog"), 800);
        assert!(reloaded.load().unwrap());
        assert_eq!(offset, 0);
        assert_eq!(
            TimerLogRecord::decode(&reloaded.read_at(0, 40).unwrap()).unwrap(),
            expected
        );
        assert_eq!(reloaded.len().unwrap(), 40);
    }

    #[test]
    fn truncate_discards_uncommitted_tail() {
        let temp_dir = tempdir().unwrap();
        let timer_log = TimerLog::new(temp_dir.path().join("timerlog"), 800);
        assert!(timer_log.load().unwrap());

        timer_log.append(&record(0, -1).encode()).unwrap();
        timer_log.append(&record(1, 0).encode()).unwrap();
        timer_log.truncate(40).unwrap();

        let reloaded = TimerLog::new(temp_dir.path().join("timerlog"), 800);
        assert!(reloaded.load().unwrap());
        assert_eq!(reloaded.len().unwrap(), 40);
        assert_eq!(
            TimerLogRecord::decode(&reloaded.read_at(0, 40).unwrap()).unwrap(),
            record(0, -1)
        );
    }

    #[test]
    fn legacy_file_migrates_without_changing_offsets() {
        let temp_dir = tempdir().unwrap();
        let directory = temp_dir.path().join("timerlog");
        std::fs::create_dir_all(&directory).unwrap();
        let mut legacy = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(directory.join(LEGACY_ACTIVE_FILE_NAME))
            .unwrap();
        legacy.write_all(&record(0, -1).encode()).unwrap();
        legacy.write_all(&record(1, 0).encode()).unwrap();
        legacy.sync_data().unwrap();

        let timer_log = TimerLog::new(&directory, 800);
        timer_log.load().unwrap();
        assert_eq!(timer_log.len().unwrap(), 80);
        assert_eq!(
            TimerLogRecord::decode(&timer_log.read_at(40, 40).unwrap()).unwrap(),
            record(1, 0)
        );
        assert!(timer_log.v2_root().join(COMMITTED_MARKER).exists());
        assert!(directory.join(LEGACY_ACTIVE_FILE_NAME).exists());
    }
}
