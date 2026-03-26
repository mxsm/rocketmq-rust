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
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

use parking_lot::Mutex;
use rocketmq_common::UtilAll::ensure_dir_ok;
use rocketmq_remoting::protocol::DataVersion;

const TIMER_CHECKPOINT_SIZE: usize = 56;

#[derive(Clone, Debug, PartialEq)]
pub struct TimerCheckpointSnapshot {
    last_read_time_ms: i64,
    last_timer_log_flush_pos: i64,
    last_timer_queue_offset: i64,
    master_timer_queue_offset: i64,
    data_version: DataVersion,
}

pub struct TimerCheckpoint {
    path: PathBuf,
    last_read_time_ms: AtomicI64,
    last_timer_log_flush_pos: AtomicI64,
    last_timer_queue_offset: AtomicI64,
    master_timer_queue_offset: AtomicI64,
    data_version: Mutex<DataVersion>,
}

impl TimerCheckpoint {
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            ensure_dir_ok(parent.to_string_lossy().as_ref());
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

        let mut checkpoint = Self {
            path,
            last_read_time_ms: AtomicI64::new(0),
            last_timer_log_flush_pos: AtomicI64::new(0),
            last_timer_queue_offset: AtomicI64::new(0),
            master_timer_queue_offset: AtomicI64::new(0),
            data_version: Mutex::new(DataVersion::new()),
        };
        checkpoint.load_from_disk()?;
        Ok(checkpoint)
    }

    pub fn flush(&self) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            ensure_dir_ok(parent.to_string_lossy().as_ref());
        }

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&self.path)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&self.encode())?;
        file.set_len(TIMER_CHECKPOINT_SIZE as u64)?;
        file.sync_data()
    }

    pub fn shutdown(&self) -> std::io::Result<()> {
        self.flush()
    }

    pub fn last_read_time_ms(&self) -> i64 {
        self.last_read_time_ms.load(Ordering::Relaxed)
    }

    pub fn set_last_read_time_ms(&self, last_read_time_ms: i64) {
        self.last_read_time_ms.store(last_read_time_ms, Ordering::Relaxed);
    }

    pub fn last_timer_log_flush_pos(&self) -> i64 {
        self.last_timer_log_flush_pos.load(Ordering::Relaxed)
    }

    pub fn set_last_timer_log_flush_pos(&self, last_timer_log_flush_pos: i64) {
        self.last_timer_log_flush_pos
            .store(last_timer_log_flush_pos, Ordering::Relaxed);
    }

    pub fn last_timer_queue_offset(&self) -> i64 {
        self.last_timer_queue_offset.load(Ordering::Relaxed)
    }

    pub fn set_last_timer_queue_offset(&self, last_timer_queue_offset: i64) {
        self.last_timer_queue_offset
            .store(last_timer_queue_offset, Ordering::Relaxed);
    }

    pub fn master_timer_queue_offset(&self) -> i64 {
        self.master_timer_queue_offset.load(Ordering::Relaxed)
    }

    pub fn set_master_timer_queue_offset(&self, master_timer_queue_offset: i64) {
        self.master_timer_queue_offset
            .store(master_timer_queue_offset, Ordering::Relaxed);
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

    fn load_from_disk(&mut self) -> std::io::Result<()> {
        if !self.path.exists() {
            return Ok(());
        }

        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        if buffer.len() < TIMER_CHECKPOINT_SIZE {
            return Ok(());
        }

        self.last_read_time_ms = AtomicI64::new(read_i64(&buffer[0..8]));
        self.last_timer_log_flush_pos = AtomicI64::new(read_i64(&buffer[8..16]));
        self.last_timer_queue_offset = AtomicI64::new(read_i64(&buffer[16..24]));
        self.master_timer_queue_offset = AtomicI64::new(read_i64(&buffer[24..32]));

        let mut data_version = DataVersion::new();
        data_version.set_state_version(read_i64(&buffer[32..40]));
        data_version.set_timestamp(read_i64(&buffer[40..48]));
        data_version.set_counter(read_i64(&buffer[48..56]));
        *self.data_version.get_mut() = data_version;
        Ok(())
    }

    fn encode(&self) -> [u8; TIMER_CHECKPOINT_SIZE] {
        let mut buffer = [0u8; TIMER_CHECKPOINT_SIZE];
        buffer[0..8].copy_from_slice(&self.last_read_time_ms().to_be_bytes());
        buffer[8..16].copy_from_slice(&self.last_timer_log_flush_pos().to_be_bytes());
        buffer[16..24].copy_from_slice(&self.last_timer_queue_offset().to_be_bytes());
        buffer[24..32].copy_from_slice(&self.master_timer_queue_offset().to_be_bytes());

        let data_version = self.data_version();
        buffer[32..40].copy_from_slice(&data_version.state_version().to_be_bytes());
        buffer[40..48].copy_from_slice(&data_version.timestamp().to_be_bytes());
        buffer[48..56].copy_from_slice(&data_version.counter().to_be_bytes());
        buffer
    }
}

fn read_i64(buffer: &[u8]) -> i64 {
    i64::from_be_bytes(buffer.try_into().expect("timer checkpoint field size must be 8 bytes"))
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
        let mut buffer = [0u8; TIMER_CHECKPOINT_SIZE];
        buffer[0..8].copy_from_slice(&self.last_read_time_ms.to_be_bytes());
        buffer[8..16].copy_from_slice(&self.last_timer_log_flush_pos.to_be_bytes());
        buffer[16..24].copy_from_slice(&self.last_timer_queue_offset.to_be_bytes());
        buffer[24..32].copy_from_slice(&self.master_timer_queue_offset.to_be_bytes());
        buffer[32..40].copy_from_slice(&self.data_version.state_version().to_be_bytes());
        buffer[40..48].copy_from_slice(&self.data_version.timestamp().to_be_bytes());
        buffer[48..56].copy_from_slice(&self.data_version.counter().to_be_bytes());
        buffer.to_vec()
    }

    pub fn decode(buffer: &[u8]) -> std::io::Result<Self> {
        if buffer.len() < TIMER_CHECKPOINT_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "timer checkpoint snapshot requires {} bytes, got {}",
                    TIMER_CHECKPOINT_SIZE,
                    buffer.len()
                ),
            ));
        }

        let mut data_version = DataVersion::new();
        data_version.set_state_version(read_i64(&buffer[32..40]));
        data_version.set_timestamp(read_i64(&buffer[40..48]));
        data_version.set_counter(read_i64(&buffer[48..56]));

        Ok(Self {
            last_read_time_ms: read_i64(&buffer[0..8]),
            last_timer_log_flush_pos: read_i64(&buffer[8..16]),
            last_timer_queue_offset: read_i64(&buffer[16..24]),
            master_timer_queue_offset: read_i64(&buffer[24..32]),
            data_version,
        })
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
        checkpoint.update_data_version(99);
        checkpoint.flush().unwrap();

        let reloaded = TimerCheckpoint::new(&path).unwrap();
        assert_eq!(reloaded.last_read_time_ms(), 1_000);
        assert_eq!(reloaded.last_timer_log_flush_pos(), 2_000);
        assert_eq!(reloaded.last_timer_queue_offset(), 3_000);
        assert_eq!(reloaded.master_timer_queue_offset(), 4_000);
        assert_eq!(reloaded.data_version().state_version(), 99);
    }

    #[test]
    fn snapshot_encode_decode_round_trips_timer_checkpoint_state() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("config").join("timercheck");

        let checkpoint = TimerCheckpoint::new(&path).unwrap();
        checkpoint.set_last_read_time_ms(11);
        checkpoint.set_last_timer_log_flush_pos(22);
        checkpoint.set_last_timer_queue_offset(33);
        checkpoint.set_master_timer_queue_offset(44);
        checkpoint.update_data_version(55);

        let snapshot = checkpoint.snapshot();
        let decoded = TimerCheckpointSnapshot::decode(snapshot.encode().as_slice()).unwrap();

        assert_eq!(decoded.last_read_time_ms(), 11);
        assert_eq!(decoded.last_timer_log_flush_pos(), 22);
        assert_eq!(decoded.last_timer_queue_offset(), 33);
        assert_eq!(decoded.master_timer_queue_offset(), 44);
        assert_eq!(decoded.data_version().state_version(), 55);
    }
}
