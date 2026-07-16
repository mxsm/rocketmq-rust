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

use parking_lot::Mutex;
use rocketmq_remoting::protocol::data_version_facade::DataVersionExt;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_store_local::timer::checkpoint::TimerCheckpointRecord;
use rocketmq_store_local::timer::checkpoint::TimerCheckpointState;
use rocketmq_store_local::timer::checkpoint::TimerCheckpointVersion;
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
    path: PathBuf,
    state: TimerCheckpointState,
    data_version: Mutex<DataVersion>,
}

impl TimerCheckpoint {
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
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

        let mut checkpoint = Self {
            path,
            state: TimerCheckpointState::default(),
            data_version: Mutex::new(rocketmq_remoting::protocol::data_version_facade::new_data_version()),
        };
        checkpoint.load_from_disk()?;
        Ok(checkpoint)
    }

    pub fn flush(&self) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
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
        self.state.last_read_time_ms()
    }

    pub fn set_last_read_time_ms(&self, last_read_time_ms: i64) {
        self.state.set_last_read_time_ms(last_read_time_ms);
    }

    pub fn last_timer_log_flush_pos(&self) -> i64 {
        self.state.last_timer_log_flush_pos()
    }

    pub fn set_last_timer_log_flush_pos(&self, last_timer_log_flush_pos: i64) {
        self.state.set_last_timer_log_flush_pos(last_timer_log_flush_pos);
    }

    pub fn last_timer_queue_offset(&self) -> i64 {
        self.state.last_timer_queue_offset()
    }

    pub fn set_last_timer_queue_offset(&self, last_timer_queue_offset: i64) {
        self.state.set_last_timer_queue_offset(last_timer_queue_offset);
    }

    pub fn master_timer_queue_offset(&self) -> i64 {
        self.state.master_timer_queue_offset()
    }

    pub fn set_master_timer_queue_offset(&self, master_timer_queue_offset: i64) {
        self.state.set_master_timer_queue_offset(master_timer_queue_offset);
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

        let record = TimerCheckpointRecord::decode(&buffer)?;
        self.state = TimerCheckpointState::from_record(record);
        *self.data_version.get_mut() = data_version_from_record(record.version);
        Ok(())
    }

    fn encode(&self) -> [u8; TIMER_CHECKPOINT_SIZE] {
        let data_version = self.data_version();
        self.state.record(version_record(&data_version)).encode()
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
    let mut data_version = rocketmq_remoting::protocol::data_version_facade::new_data_version();
    data_version.set_state_version(version.state_version);
    data_version.set_timestamp(version.timestamp);
    data_version.set_counter(version.counter);
    data_version
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
