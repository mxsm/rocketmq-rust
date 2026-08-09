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
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use parking_lot::Mutex;
use rocketmq_protocol::protocol::data_version_facade::DataVersionExt;
use rocketmq_protocol::protocol::DataVersion;
use rocketmq_store_local::timer::checkpoint::TimerCheckpointRecord;
use rocketmq_store_local::timer::checkpoint::TimerCheckpointRecoveryReport;
use rocketmq_store_local::timer::checkpoint::TimerCheckpointState;
use rocketmq_store_local::timer::checkpoint::TimerCheckpointV2Record;
use rocketmq_store_local::timer::checkpoint::TimerCheckpointVersion;
use rocketmq_store_local::timer::checkpoint::VersionedTimerCheckpointStore;
use rocketmq_store_local::timer::checkpoint::TIMER_CHECKPOINT_SIZE;

#[derive(Clone, Debug, PartialEq)]
pub struct TimerCheckpointSnapshot {
    last_read_time_ms: i64,
    last_timer_log_flush_pos: i64,
    last_timer_queue_offset: i64,
    master_timer_queue_offset: i64,
    data_version: DataVersion,
}

pub struct TimerCheckpoint {
    _legacy_path: PathBuf,
    state: TimerCheckpointState,
    data_version: Mutex<DataVersion>,
    versioned: VersionedTimerCheckpointStore,
    wheel_generation: AtomicU64,
    role_epoch: AtomicU64,
    policy_hash: u64,
    drain_generation: AtomicU64,
    drain_page: AtomicU64,
    drain_record: AtomicU64,
    recovery_report: Mutex<TimerCheckpointRecoveryReport>,
}

impl TimerCheckpoint {
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        Self::new_with_policy(path, 0)
    }

    pub fn new_with_policy<P: AsRef<Path>>(path: P, policy_hash: u64) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        if file.metadata()?.len() == 0 {
            file.set_len(TIMER_CHECKPOINT_SIZE as u64)?;
        }

        let versioned = VersionedTimerCheckpointStore::new(&path);
        let (v2_record, report) = versioned.load_best(|record| {
            if policy_hash != 0 && record.policy_hash != policy_hash {
                Err(format!(
                    "policy hash {} does not match configured {}",
                    record.policy_hash, policy_hash
                ))
            } else {
                Ok(())
            }
        })?;
        if versioned.has_any_copy() && v2_record.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("no valid V2 timer checkpoint: {}", report.rejected.join("; ")),
            ));
        }
        let effective_policy_hash = if policy_hash == 0 {
            v2_record.map(|record| record.policy_hash).unwrap_or_default()
        } else {
            policy_hash
        };

        let legacy = if v2_record.is_none() {
            read_legacy_checkpoint(&path)?
        } else {
            TimerCheckpointRecord::default()
        };
        let state_record = v2_record.map(v1_record_from_v2).unwrap_or(legacy);
        let (wheel_generation, role_epoch, drain_generation, drain_page, drain_record) = v2_record
            .map(|record| {
                (
                    record.wheel_generation,
                    record.role_epoch,
                    record.drain_generation,
                    u64::from(record.drain_page),
                    u64::from(record.drain_record),
                )
            })
            .unwrap_or_default();
        Ok(Self {
            _legacy_path: path,
            state: TimerCheckpointState::from_record(state_record),
            data_version: Mutex::new(data_version_from_record(state_record.version)),
            versioned,
            wheel_generation: AtomicU64::new(wheel_generation),
            role_epoch: AtomicU64::new(role_epoch),
            policy_hash: effective_policy_hash,
            drain_generation: AtomicU64::new(drain_generation),
            drain_page: AtomicU64::new(drain_page),
            drain_record: AtomicU64::new(drain_record),
            recovery_report: Mutex::new(report),
        })
    }

    pub fn flush(&self) -> std::io::Result<()> {
        let data_version = self.data_version();
        self.versioned.commit(TimerCheckpointV2Record {
            generation: 0,
            durable_queue_offset: self.last_timer_queue_offset(),
            timer_log_durable_length: self.last_timer_log_flush_pos(),
            dequeue_slot_ms: self.last_read_time_ms(),
            drain_generation: self.drain_generation.load(Ordering::Relaxed),
            drain_page: u32::try_from(self.drain_page.load(Ordering::Relaxed))
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "timer drain page exceeds u32"))?,
            drain_record: u32::try_from(self.drain_record.load(Ordering::Relaxed))
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "timer drain record exceeds u32"))?,
            wheel_generation: self.wheel_generation(),
            role_epoch: self.role_epoch(),
            policy_hash: self.policy_hash,
            master_queue_offset: self.master_timer_queue_offset(),
            data_version: version_record(&data_version),
        })?;
        Ok(())
    }

    pub fn shutdown(&self) -> std::io::Result<()> {
        self.flush()
    }

    pub fn last_read_time_ms(&self) -> i64 {
        self.state.last_read_time_ms()
    }

    pub fn set_last_read_time_ms(&self, value: i64) {
        self.state.set_last_read_time_ms(value);
    }

    pub fn last_timer_log_flush_pos(&self) -> i64 {
        self.state.last_timer_log_flush_pos()
    }

    pub fn set_last_timer_log_flush_pos(&self, value: i64) {
        self.state.set_last_timer_log_flush_pos(value);
    }

    pub fn last_timer_queue_offset(&self) -> i64 {
        self.state.last_timer_queue_offset()
    }

    pub fn set_last_timer_queue_offset(&self, value: i64) {
        self.state.set_last_timer_queue_offset(value);
    }

    pub fn master_timer_queue_offset(&self) -> i64 {
        self.state.master_timer_queue_offset()
    }

    pub fn set_master_timer_queue_offset(&self, value: i64) {
        self.state.set_master_timer_queue_offset(value);
    }

    pub fn wheel_generation(&self) -> u64 {
        self.wheel_generation.load(Ordering::Acquire)
    }

    pub fn set_wheel_generation(&self, generation: u64) {
        self.wheel_generation.store(generation, Ordering::Release);
    }

    pub fn role_epoch(&self) -> u64 {
        self.role_epoch.load(Ordering::Acquire)
    }

    pub fn set_role_epoch(&self, epoch: u64) {
        self.role_epoch.store(epoch, Ordering::Release);
    }

    pub fn set_drain_cursor(&self, generation: u64, page: u32, record: u32) {
        self.drain_generation.store(generation, Ordering::Release);
        self.drain_page.store(u64::from(page), Ordering::Release);
        self.drain_record.store(u64::from(record), Ordering::Release);
    }

    pub fn drain_cursor(&self) -> (u64, u32, u32) {
        (
            self.drain_generation.load(Ordering::Acquire),
            u32::try_from(self.drain_page.load(Ordering::Acquire)).unwrap_or(u32::MAX),
            u32::try_from(self.drain_record.load(Ordering::Acquire)).unwrap_or(u32::MAX),
        )
    }

    pub fn recovery_report(&self) -> TimerCheckpointRecoveryReport {
        self.recovery_report.lock().clone()
    }

    pub fn local_generation(&self) -> u64 {
        self.versioned
            .current()
            .map(|record| record.generation)
            .unwrap_or_default()
    }

    pub fn select_for_storage(&self, max_log_length: i64, before_generation: Option<u64>) -> std::io::Result<bool> {
        let (selected, report) = self.versioned.load_best(|record| {
            if self.policy_hash != 0 && record.policy_hash != self.policy_hash {
                return Err("policy fingerprint does not match".into());
            }
            if record.timer_log_durable_length < 0 || record.timer_log_durable_length > max_log_length {
                return Err(format!(
                    "timer log durable length {} is unavailable; current length is {}",
                    record.timer_log_durable_length, max_log_length
                ));
            }
            if before_generation.is_some_and(|generation| record.generation >= generation) {
                return Err("checkpoint is not older than the rejected generation".into());
            }
            Ok(())
        })?;
        *self.recovery_report.lock() = report;
        let Some(record) = selected else {
            return Ok(false);
        };
        self.apply_v2_record(record);
        Ok(true)
    }

    pub fn data_version(&self) -> DataVersion {
        self.data_version.lock().clone()
    }

    pub fn update_data_version(&self, state_version: i64) {
        self.data_version.lock().next_version_with(state_version);
    }

    pub fn snapshot(&self) -> TimerCheckpointSnapshot {
        TimerCheckpointSnapshot {
            last_read_time_ms: self.last_read_time_ms(),
            last_timer_log_flush_pos: self.last_timer_log_flush_pos(),
            last_timer_queue_offset: self.last_timer_queue_offset(),
            master_timer_queue_offset: self.master_timer_queue_offset(),
            data_version: self.data_version(),
        }
    }

    pub fn sync_from_master_snapshot(&self, snapshot: &TimerCheckpointSnapshot) {
        self.set_master_timer_queue_offset(snapshot.master_timer_queue_offset());
        *self.data_version.lock() = snapshot.data_version().clone();
    }

    fn apply_v2_record(&self, record: TimerCheckpointV2Record) {
        let state = v1_record_from_v2(record);
        self.set_last_read_time_ms(state.last_read_time_ms);
        self.set_last_timer_log_flush_pos(state.last_timer_log_flush_pos);
        self.set_last_timer_queue_offset(state.last_timer_queue_offset);
        self.set_master_timer_queue_offset(state.master_timer_queue_offset);
        *self.data_version.lock() = data_version_from_record(state.version);
        self.set_wheel_generation(record.wheel_generation);
        self.set_role_epoch(record.role_epoch);
        self.set_drain_cursor(record.drain_generation, record.drain_page, record.drain_record);
    }
}

impl TimerCheckpointSnapshot {
    pub fn new(
        last_read_time_ms: i64,
        last_timer_log_flush_pos: i64,
        last_timer_queue_offset: i64,
        master_timer_queue_offset: i64,
        data_version: DataVersion,
    ) -> Self {
        Self {
            last_read_time_ms,
            last_timer_log_flush_pos,
            last_timer_queue_offset,
            master_timer_queue_offset,
            data_version,
        }
    }

    pub fn last_read_time_ms(&self) -> i64 {
        self.last_read_time_ms
    }

    pub fn last_timer_log_flush_pos(&self) -> i64 {
        self.last_timer_log_flush_pos
    }

    pub fn last_timer_queue_offset(&self) -> i64 {
        self.last_timer_queue_offset
    }

    pub fn master_timer_queue_offset(&self) -> i64 {
        self.master_timer_queue_offset
    }

    pub fn data_version(&self) -> &DataVersion {
        &self.data_version
    }

    pub fn encode(&self) -> Vec<u8> {
        TimerCheckpointRecord {
            last_read_time_ms: self.last_read_time_ms,
            last_timer_log_flush_pos: self.last_timer_log_flush_pos,
            last_timer_queue_offset: self.last_timer_queue_offset,
            master_timer_queue_offset: self.master_timer_queue_offset,
            version: version_record(&self.data_version),
        }
        .encode()
        .to_vec()
    }

    pub fn decode(buffer: &[u8]) -> std::io::Result<Self> {
        let record = TimerCheckpointRecord::decode(buffer)?;
        Ok(Self {
            last_read_time_ms: record.last_read_time_ms,
            last_timer_log_flush_pos: record.last_timer_log_flush_pos,
            last_timer_queue_offset: record.last_timer_queue_offset,
            master_timer_queue_offset: record.master_timer_queue_offset,
            data_version: data_version_from_record(record.version),
        })
    }
}

fn version_record(data_version: &DataVersion) -> TimerCheckpointVersion {
    TimerCheckpointVersion {
        state_version: data_version.state_version(),
        timestamp: data_version.timestamp(),
        counter: data_version.counter(),
    }
}

fn data_version_from_record(version: TimerCheckpointVersion) -> DataVersion {
    let mut data_version = rocketmq_protocol::protocol::data_version_facade::new_data_version();
    data_version.set_state_version(version.state_version);
    data_version.set_timestamp(version.timestamp);
    data_version.set_counter(version.counter);
    data_version
}

fn read_legacy_checkpoint(path: &Path) -> std::io::Result<TimerCheckpointRecord> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    if buffer.len() < TIMER_CHECKPOINT_SIZE {
        return Ok(TimerCheckpointRecord::default());
    }
    TimerCheckpointRecord::decode(&buffer)
}

fn v1_record_from_v2(record: TimerCheckpointV2Record) -> TimerCheckpointRecord {
    TimerCheckpointRecord {
        last_read_time_ms: record.dequeue_slot_ms,
        last_timer_log_flush_pos: record.timer_log_durable_length,
        last_timer_queue_offset: record.durable_queue_offset,
        master_timer_queue_offset: record.master_queue_offset,
        version: record.data_version,
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn flush_and_reload_preserves_timer_checkpoint_state() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("config").join("timercheck");

        let checkpoint = TimerCheckpoint::new(&path).unwrap();
        checkpoint.set_last_read_time_ms(1_000);
        checkpoint.set_last_timer_log_flush_pos(2_000);
        checkpoint.set_last_timer_queue_offset(3_000);
        checkpoint.set_master_timer_queue_offset(4_000);
        checkpoint.set_wheel_generation(5);
        checkpoint.set_role_epoch(6);
        checkpoint.update_data_version(99);
        checkpoint.flush().unwrap();

        let reloaded = TimerCheckpoint::new(&path).unwrap();
        assert_eq!(reloaded.last_read_time_ms(), 1_000);
        assert_eq!(reloaded.last_timer_log_flush_pos(), 2_000);
        assert_eq!(reloaded.last_timer_queue_offset(), 3_000);
        assert_eq!(reloaded.master_timer_queue_offset(), 4_000);
        assert_eq!(reloaded.wheel_generation(), 5);
        assert_eq!(reloaded.role_epoch(), 6);
        assert_eq!(reloaded.data_version().state_version(), 99);
    }

    #[test]
    fn snapshot_encode_decode_keeps_legacy_56_byte_wire_format() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("config").join("timercheck");
        let checkpoint = TimerCheckpoint::new(&path).unwrap();
        checkpoint.set_last_read_time_ms(11);
        checkpoint.set_last_timer_log_flush_pos(22);
        checkpoint.set_last_timer_queue_offset(33);
        checkpoint.set_master_timer_queue_offset(44);
        checkpoint.update_data_version(55);

        let encoded = checkpoint.snapshot().encode();
        assert_eq!(encoded.len(), TIMER_CHECKPOINT_SIZE);
        let decoded = TimerCheckpointSnapshot::decode(&encoded).unwrap();
        assert_eq!(decoded.last_read_time_ms(), 11);
        assert_eq!(decoded.last_timer_log_flush_pos(), 22);
        assert_eq!(decoded.last_timer_queue_offset(), 33);
        assert_eq!(decoded.master_timer_queue_offset(), 44);
        assert_eq!(decoded.data_version().state_version(), 55);
    }

    #[test]
    fn local_checkpoint_rejects_policy_mismatch() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("config").join("timercheck");
        let checkpoint = TimerCheckpoint::new_with_policy(&path, 11).unwrap();
        checkpoint.flush().unwrap();
        assert!(TimerCheckpoint::new_with_policy(&path, 12).is_err());
    }
}
