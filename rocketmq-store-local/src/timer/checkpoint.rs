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
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

use parking_lot::Mutex;

use crate::timer::storage_format::crc32c;

pub const TIMER_CHECKPOINT_SIZE: usize = 56;
pub const TIMER_CHECKPOINT_V2_SIZE: usize = 128;
const TIMER_CHECKPOINT_V2_MAGIC: u32 = 0x5443_5032;
const TIMER_CHECKPOINT_V2_VERSION: u16 = 2;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TimerCheckpointVersion {
    pub state_version: i64,
    pub timestamp: i64,
    pub counter: i64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TimerCheckpointRecord {
    pub last_read_time_ms: i64,
    pub last_timer_log_flush_pos: i64,
    pub last_timer_queue_offset: i64,
    pub master_timer_queue_offset: i64,
    pub version: TimerCheckpointVersion,
}

impl TimerCheckpointRecord {
    pub fn encode(self) -> [u8; TIMER_CHECKPOINT_SIZE] {
        let mut buffer = [0u8; TIMER_CHECKPOINT_SIZE];
        for (index, value) in [
            self.last_read_time_ms,
            self.last_timer_log_flush_pos,
            self.last_timer_queue_offset,
            self.master_timer_queue_offset,
            self.version.state_version,
            self.version.timestamp,
            self.version.counter,
        ]
        .into_iter()
        .enumerate()
        {
            let start = index * 8;
            buffer[start..start + 8].copy_from_slice(&value.to_be_bytes());
        }
        buffer
    }

    pub fn decode(buffer: &[u8]) -> std::io::Result<Self> {
        if buffer.len() < TIMER_CHECKPOINT_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "timer checkpoint requires {TIMER_CHECKPOINT_SIZE} bytes, got {}",
                    buffer.len()
                ),
            ));
        }
        Ok(Self {
            last_read_time_ms: read_i64(buffer, 0),
            last_timer_log_flush_pos: read_i64(buffer, 8),
            last_timer_queue_offset: read_i64(buffer, 16),
            master_timer_queue_offset: read_i64(buffer, 24),
            version: TimerCheckpointVersion {
                state_version: read_i64(buffer, 32),
                timestamp: read_i64(buffer, 40),
                counter: read_i64(buffer, 48),
            },
        })
    }
}

/// Local V2 checkpoint. This codec is intentionally distinct from the stable 56-byte HA/admin
/// snapshot above.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TimerCheckpointV2Record {
    pub generation: u64,
    pub durable_queue_offset: i64,
    pub timer_log_durable_length: i64,
    pub dequeue_slot_ms: i64,
    pub drain_generation: u64,
    pub drain_page: u32,
    pub drain_record: u32,
    pub wheel_generation: u64,
    pub role_epoch: u64,
    pub policy_hash: u64,
    pub master_queue_offset: i64,
    pub data_version: TimerCheckpointVersion,
}

impl TimerCheckpointV2Record {
    pub fn encode(self) -> [u8; TIMER_CHECKPOINT_V2_SIZE] {
        let mut bytes = [0u8; TIMER_CHECKPOINT_V2_SIZE];
        bytes[0..4].copy_from_slice(&TIMER_CHECKPOINT_V2_MAGIC.to_be_bytes());
        bytes[4..6].copy_from_slice(&TIMER_CHECKPOINT_V2_VERSION.to_be_bytes());
        bytes[6..8].copy_from_slice(&(TIMER_CHECKPOINT_V2_SIZE as u16).to_be_bytes());
        bytes[8..16].copy_from_slice(&self.generation.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.durable_queue_offset.to_be_bytes());
        bytes[24..32].copy_from_slice(&self.timer_log_durable_length.to_be_bytes());
        bytes[32..40].copy_from_slice(&self.dequeue_slot_ms.to_be_bytes());
        bytes[40..48].copy_from_slice(&self.drain_generation.to_be_bytes());
        bytes[48..52].copy_from_slice(&self.drain_page.to_be_bytes());
        bytes[52..56].copy_from_slice(&self.drain_record.to_be_bytes());
        bytes[56..64].copy_from_slice(&self.wheel_generation.to_be_bytes());
        bytes[64..72].copy_from_slice(&self.role_epoch.to_be_bytes());
        bytes[72..80].copy_from_slice(&self.policy_hash.to_be_bytes());
        bytes[80..88].copy_from_slice(&self.master_queue_offset.to_be_bytes());
        bytes[88..96].copy_from_slice(&self.data_version.state_version.to_be_bytes());
        bytes[96..104].copy_from_slice(&self.data_version.timestamp.to_be_bytes());
        bytes[104..112].copy_from_slice(&self.data_version.counter.to_be_bytes());
        let checksum = crc32c(&bytes[..124]);
        bytes[124..128].copy_from_slice(&checksum.to_be_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> std::io::Result<Self> {
        if bytes.len() != TIMER_CHECKPOINT_V2_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "timer V2 checkpoint requires {TIMER_CHECKPOINT_V2_SIZE} bytes, got {}",
                    bytes.len()
                ),
            ));
        }
        if read_u32(bytes, 0) != TIMER_CHECKPOINT_V2_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "timer V2 checkpoint magic is invalid",
            ));
        }
        let version = read_u16(bytes, 4);
        if version != TIMER_CHECKPOINT_V2_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("timer V2 checkpoint version {version} is unsupported"),
            ));
        }
        if read_u16(bytes, 6) as usize != TIMER_CHECKPOINT_V2_SIZE || crc32c(&bytes[..124]) != read_u32(bytes, 124) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "timer V2 checkpoint length or checksum is invalid",
            ));
        }
        Ok(Self {
            generation: read_u64(bytes, 8),
            durable_queue_offset: read_i64(bytes, 16),
            timer_log_durable_length: read_i64(bytes, 24),
            dequeue_slot_ms: read_i64(bytes, 32),
            drain_generation: read_u64(bytes, 40),
            drain_page: read_u32(bytes, 48),
            drain_record: read_u32(bytes, 52),
            wheel_generation: read_u64(bytes, 56),
            role_epoch: read_u64(bytes, 64),
            policy_hash: read_u64(bytes, 72),
            master_queue_offset: read_i64(bytes, 80),
            data_version: TimerCheckpointVersion {
                state_version: read_i64(bytes, 88),
                timestamp: read_i64(bytes, 96),
                counter: read_i64(bytes, 104),
            },
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TimerCheckpointRecoveryReport {
    pub selected_generation: Option<u64>,
    pub rejected: Vec<String>,
}

/// Alternating local checkpoint store. A damaged or incomplete write can invalidate at most one
/// copy, leaving the previous generation available for recovery.
pub struct VersionedTimerCheckpointStore {
    path_a: PathBuf,
    path_b: PathBuf,
    current: Mutex<Option<TimerCheckpointV2Record>>,
}

impl VersionedTimerCheckpointStore {
    pub fn new(base_path: impl AsRef<Path>) -> Self {
        let base = base_path.as_ref().to_string_lossy();
        Self {
            path_a: PathBuf::from(format!("{base}.a")),
            path_b: PathBuf::from(format!("{base}.b")),
            current: Mutex::new(None),
        }
    }

    pub fn load_best(
        &self,
        mut validate: impl FnMut(&TimerCheckpointV2Record) -> Result<(), String>,
    ) -> std::io::Result<(Option<TimerCheckpointV2Record>, TimerCheckpointRecoveryReport)> {
        let mut report = TimerCheckpointRecoveryReport::default();
        let mut candidates = Vec::new();
        for (name, path) in [("checkpoint.a", &self.path_a), ("checkpoint.b", &self.path_b)] {
            match read_v2_checkpoint(path) {
                Ok(Some(record)) => candidates.push((name, record)),
                Ok(None) => {}
                Err(error) => report.rejected.push(format!("{name}: {error}")),
            }
        }
        candidates.sort_by_key(|(_, record)| std::cmp::Reverse(record.generation));
        let selected = candidates
            .into_iter()
            .find_map(|(name, record)| match validate(&record) {
                Ok(()) => Some(record),
                Err(reason) => {
                    report.rejected.push(format!("{name}: {reason}"));
                    None
                }
            });
        report.selected_generation = selected.map(|record| record.generation);
        *self.current.lock() = selected;
        Ok((selected, report))
    }

    pub fn commit(&self, mut record: TimerCheckpointV2Record) -> std::io::Result<TimerCheckpointV2Record> {
        let current = *self.current.lock();
        record.generation = current.map(|value| value.generation.saturating_add(1)).unwrap_or(1);
        let generation_a = read_v2_checkpoint(&self.path_a)
            .ok()
            .flatten()
            .map(|value| value.generation)
            .unwrap_or_default();
        let generation_b = read_v2_checkpoint(&self.path_b)
            .ok()
            .flatten()
            .map(|value| value.generation)
            .unwrap_or_default();
        let target = if generation_a <= generation_b {
            &self.path_a
        } else {
            &self.path_b
        };
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(target)?;
        file.write_all(&record.encode())?;
        file.sync_data()?;
        *self.current.lock() = Some(record);
        Ok(record)
    }

    pub fn current(&self) -> Option<TimerCheckpointV2Record> {
        *self.current.lock()
    }

    pub fn has_any_copy(&self) -> bool {
        self.path_a.exists() || self.path_b.exists()
    }
}

fn read_v2_checkpoint(path: &Path) -> std::io::Result<Option<TimerCheckpointV2Record>> {
    if !path.exists() {
        return Ok(None);
    }
    let mut bytes = Vec::new();
    OpenOptions::new().read(true).open(path)?.read_to_end(&mut bytes)?;
    TimerCheckpointV2Record::decode(&bytes).map(Some)
}

#[derive(Debug, Default)]
pub struct TimerCheckpointState {
    last_read_time_ms: AtomicI64,
    last_timer_log_flush_pos: AtomicI64,
    last_timer_queue_offset: AtomicI64,
    master_timer_queue_offset: AtomicI64,
}

impl TimerCheckpointState {
    pub fn from_record(record: TimerCheckpointRecord) -> Self {
        Self {
            last_read_time_ms: AtomicI64::new(record.last_read_time_ms),
            last_timer_log_flush_pos: AtomicI64::new(record.last_timer_log_flush_pos),
            last_timer_queue_offset: AtomicI64::new(record.last_timer_queue_offset),
            master_timer_queue_offset: AtomicI64::new(record.master_timer_queue_offset),
        }
    }

    pub fn record(&self, version: TimerCheckpointVersion) -> TimerCheckpointRecord {
        TimerCheckpointRecord {
            last_read_time_ms: self.last_read_time_ms(),
            last_timer_log_flush_pos: self.last_timer_log_flush_pos(),
            last_timer_queue_offset: self.last_timer_queue_offset(),
            master_timer_queue_offset: self.master_timer_queue_offset(),
            version,
        }
    }

    pub fn last_read_time_ms(&self) -> i64 {
        self.last_read_time_ms.load(Ordering::Relaxed)
    }

    pub fn set_last_read_time_ms(&self, value: i64) {
        self.last_read_time_ms.store(value, Ordering::Relaxed);
    }

    pub fn last_timer_log_flush_pos(&self) -> i64 {
        self.last_timer_log_flush_pos.load(Ordering::Relaxed)
    }

    pub fn set_last_timer_log_flush_pos(&self, value: i64) {
        self.last_timer_log_flush_pos.store(value, Ordering::Relaxed);
    }

    pub fn last_timer_queue_offset(&self) -> i64 {
        self.last_timer_queue_offset.load(Ordering::Relaxed)
    }

    pub fn set_last_timer_queue_offset(&self, value: i64) {
        self.last_timer_queue_offset.store(value, Ordering::Relaxed);
    }

    pub fn master_timer_queue_offset(&self) -> i64 {
        self.master_timer_queue_offset.load(Ordering::Relaxed)
    }

    pub fn set_master_timer_queue_offset(&self, value: i64) {
        self.master_timer_queue_offset.store(value, Ordering::Relaxed);
    }
}

fn read_i64(buffer: &[u8], start: usize) -> i64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&buffer[start..start + 8]);
    i64::from_be_bytes(bytes)
}

fn read_u16(buffer: &[u8], start: usize) -> u16 {
    u16::from_be_bytes(buffer[start..start + 2].try_into().expect("fixed u16 field"))
}

fn read_u32(buffer: &[u8], start: usize) -> u32 {
    u32::from_be_bytes(buffer[start..start + 4].try_into().expect("fixed u32 field"))
}

fn read_u64(buffer: &[u8], start: usize) -> u64 {
    u64::from_be_bytes(buffer[start..start + 8].try_into().expect("fixed u64 field"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_record_round_trips_exact_56_byte_layout() {
        let record = TimerCheckpointRecord {
            last_read_time_ms: 1,
            last_timer_log_flush_pos: 2,
            last_timer_queue_offset: 3,
            master_timer_queue_offset: 4,
            version: TimerCheckpointVersion {
                state_version: 5,
                timestamp: 6,
                counter: 7,
            },
        };
        let encoded = record.encode();
        assert_eq!(encoded.len(), TIMER_CHECKPOINT_SIZE);
        assert_eq!(TimerCheckpointRecord::decode(&encoded).unwrap(), record);
    }

    #[test]
    fn checkpoint_state_projects_progress_without_owning_protocol_version() {
        let state = TimerCheckpointState::default();
        state.set_last_read_time_ms(11);
        state.set_last_timer_log_flush_pos(22);
        state.set_last_timer_queue_offset(33);
        state.set_master_timer_queue_offset(44);
        let record = state.record(TimerCheckpointVersion::default());
        assert_eq!((record.last_read_time_ms, record.last_timer_log_flush_pos), (11, 22));
        assert_eq!(
            (record.last_timer_queue_offset, record.master_timer_queue_offset),
            (33, 44)
        );
    }

    #[test]
    fn v2_checkpoint_codec_detects_corruption() {
        let record = TimerCheckpointV2Record {
            generation: 9,
            durable_queue_offset: 10,
            timer_log_durable_length: 80,
            dequeue_slot_ms: 1_000,
            drain_generation: 4,
            drain_page: 5,
            drain_record: 6,
            wheel_generation: 7,
            role_epoch: 8,
            policy_hash: 11,
            master_queue_offset: 12,
            data_version: TimerCheckpointVersion {
                state_version: 13,
                timestamp: 14,
                counter: 15,
            },
        };
        let encoded = record.encode();
        assert_eq!(TimerCheckpointV2Record::decode(&encoded).unwrap(), record);
        let mut corrupt = encoded;
        corrupt[30] ^= 1;
        assert!(TimerCheckpointV2Record::decode(&corrupt).is_err());
    }

    #[test]
    fn alternating_checkpoint_falls_back_when_newest_is_corrupt() {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().join("timercheck");
        let store = VersionedTimerCheckpointStore::new(&base);
        store.load_best(|_| Ok(())).unwrap();
        let first = store.commit(TimerCheckpointV2Record::default()).unwrap();
        let second = store.commit(TimerCheckpointV2Record::default()).unwrap();
        assert_eq!((first.generation, second.generation), (1, 2));
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(format!("{}.b", base.display()))
            .unwrap()
            .write_all(b"broken")
            .unwrap();

        let reloaded = VersionedTimerCheckpointStore::new(&base);
        let (selected, report) = reloaded.load_best(|_| Ok(())).unwrap();
        assert_eq!(selected.unwrap().generation, 1);
        assert_eq!(report.selected_generation, Some(1));
        assert_eq!(report.rejected.len(), 1);
    }
}
