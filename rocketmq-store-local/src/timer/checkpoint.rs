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

use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

pub const TIMER_CHECKPOINT_SIZE: usize = 56;

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
}
