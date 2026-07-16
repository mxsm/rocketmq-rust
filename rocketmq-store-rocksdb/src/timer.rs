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

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use rocketmq_error::RocketMQError;

use crate::message::MessageRocksDbStorage;
use crate::message::TimerRocksDbAction;
use crate::message::TimerRocksDbRecord;
use crate::message::TIMER_SYS_TOPIC_SCAN_OFFSET_CHECKPOINT;

/// Backend-neutral projection of one Timer CommitLog dispatch record.
pub trait RocksDbTimerDispatch {
    fn is_timer_topic(&self) -> bool;
    fn commit_log_offset(&self) -> i64;
    fn message_size(&self) -> i32;
    fn consume_queue_offset(&self) -> i64;
    fn delay_time_ms(&self) -> Option<i64>;
    fn uniq_key(&self) -> Option<&str>;
    fn delete_uniq_key(&self) -> Option<&str>;
    fn is_roll_update(&self) -> bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RocksDbTimerBuildConfig {
    pub queue_capacity: usize,
    pub batch_size: usize,
}

impl Default for RocksDbTimerBuildConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 100_000,
            batch_size: 1000,
        }
    }
}

impl RocksDbTimerBuildConfig {
    fn validate(self) -> Result<Self, RocketMQError> {
        if self.queue_capacity == 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.timer.queue_capacity",
                value: self.queue_capacity.to_string(),
                reason: "queue capacity must be greater than zero".to_string(),
            });
        }
        if self.batch_size == 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.timer.batch_size",
                value: self.batch_size.to_string(),
                reason: "batch size must be greater than zero".to_string(),
            });
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerRocksDbBuildEntry {
    pub record: TimerRocksDbRecord,
    pub queue_offset: i64,
}

pub struct RocksDbTimerBuildService {
    storage: Arc<MessageRocksDbStorage>,
    config: RocksDbTimerBuildConfig,
    pending: Mutex<VecDeque<TimerRocksDbBuildEntry>>,
}

impl RocksDbTimerBuildService {
    pub fn new(storage: Arc<MessageRocksDbStorage>, config: RocksDbTimerBuildConfig) -> Result<Self, RocketMQError> {
        let config = config.validate()?;
        Ok(Self {
            storage,
            config,
            pending: Mutex::new(VecDeque::with_capacity(config.queue_capacity.min(1024))),
        })
    }

    pub fn enqueue(&self, entry: TimerRocksDbBuildEntry) -> Result<usize, RocketMQError> {
        validate_timer_entry(&entry)?;
        let mut pending = self.pending.lock().map_err(|error| {
            RocketMQError::storage_write_failed("rocksdb", format!("timer queue lock poisoned: {error}"))
        })?;
        if pending.len() >= self.config.queue_capacity {
            return Err(RocketMQError::storage_write_failed(
                "rocksdb",
                format!(
                    "timer queue full: capacity={}, pending={}",
                    self.config.queue_capacity,
                    pending.len()
                ),
            ));
        }
        pending.push_back(entry);
        Ok(1)
    }

    pub fn build_timer_index<S>(&self, dispatch: &S) -> Result<usize, RocketMQError>
    where
        S: RocksDbTimerDispatch + ?Sized,
    {
        let Some(entry) = self.entry_for_dispatch(dispatch)? else {
            return Ok(0);
        };
        self.enqueue(entry)
    }

    pub fn pending_len(&self) -> usize {
        self.pending.lock().map_or(0, |pending| pending.len())
    }

    pub fn flush_pending(&self) -> Result<usize, RocketMQError> {
        let mut flushed = 0;
        loop {
            let batch = self.drain_batch()?;
            if batch.is_empty() {
                return Ok(flushed);
            }

            let batch_len = batch.len();
            if let Err(error) = self.flush_batch(&batch) {
                self.requeue_front(batch)?;
                return Err(error);
            }
            flushed += batch_len;
        }
    }

    pub async fn flush_pending_blocking(self: Arc<Self>) -> Result<usize, RocketMQError> {
        crate::runtime::spawn_io("rocksdb.timer.flush_pending", move || self.flush_pending()).await?
    }

    pub fn get_dispatch_from_queue_offset(&self) -> Result<Option<i64>, RocketMQError> {
        let offset = self
            .storage
            .get_checkpoint_for_timer(TIMER_SYS_TOPIC_SCAN_OFFSET_CHECKPOINT)?;
        Ok((offset > 0).then_some(offset))
    }

    fn entry_for_dispatch<S>(&self, dispatch: &S) -> Result<Option<TimerRocksDbBuildEntry>, RocketMQError>
    where
        S: RocksDbTimerDispatch + ?Sized,
    {
        if !dispatch.is_timer_topic()
            || dispatch.commit_log_offset() < 0
            || dispatch.message_size() <= 0
            || dispatch.consume_queue_offset() < 0
        {
            return Ok(None);
        }

        if self
            .get_dispatch_from_queue_offset()?
            .is_some_and(|last_queue_offset| dispatch.consume_queue_offset() < last_queue_offset)
        {
            return Ok(None);
        }

        let Some(delay_time) = dispatch.delay_time_ms().filter(|delay_time| *delay_time > 0) else {
            return Ok(None);
        };
        let (uniq_key, action) = if let Some(delete_key) = dispatch.delete_uniq_key().filter(|key| !key.is_empty()) {
            (
                extract_delete_uniq_key(delete_key).to_string(),
                TimerRocksDbAction::Delete,
            )
        } else {
            let Some(uniq_key) = dispatch.uniq_key().filter(|key| !key.is_empty()) else {
                return Ok(None);
            };
            let action = if dispatch.is_roll_update() {
                TimerRocksDbAction::Update
            } else {
                TimerRocksDbAction::Put
            };
            (uniq_key.to_string(), action)
        };

        Ok(Some(TimerRocksDbBuildEntry {
            record: TimerRocksDbRecord {
                delay_time,
                uniq_key,
                offset_py: dispatch.commit_log_offset(),
                size_py: dispatch.message_size(),
                action,
            },
            queue_offset: dispatch.consume_queue_offset(),
        }))
    }

    fn flush_batch(&self, batch: &[TimerRocksDbBuildEntry]) -> Result<(), RocketMQError> {
        let records = batch.iter().map(|entry| entry.record.clone()).collect::<Vec<_>>();
        self.storage.write_records_for_timer(&records)?;
        if let Some(max_queue_offset) = batch.iter().map(|entry| entry.queue_offset).max() {
            let next_offset = max_queue_offset.saturating_add(1);
            let stored_offset = self
                .storage
                .get_checkpoint_for_timer(TIMER_SYS_TOPIC_SCAN_OFFSET_CHECKPOINT)?;
            if next_offset > stored_offset {
                self.storage
                    .write_checkpoint_for_timer(TIMER_SYS_TOPIC_SCAN_OFFSET_CHECKPOINT, next_offset)?;
            }
        }
        Ok(())
    }

    fn drain_batch(&self) -> Result<Vec<TimerRocksDbBuildEntry>, RocketMQError> {
        let mut pending = self.pending.lock().map_err(|error| {
            RocketMQError::storage_write_failed("rocksdb", format!("timer queue lock poisoned: {error}"))
        })?;
        let batch_len = self.config.batch_size.min(pending.len());
        Ok(pending.drain(..batch_len).collect())
    }

    fn requeue_front(&self, mut batch: Vec<TimerRocksDbBuildEntry>) -> Result<(), RocketMQError> {
        let mut pending = self.pending.lock().map_err(|error| {
            RocketMQError::storage_write_failed("rocksdb", format!("timer queue lock poisoned: {error}"))
        })?;
        while let Some(entry) = batch.pop() {
            pending.push_front(entry);
        }
        Ok(())
    }
}

fn extract_delete_uniq_key(delete_key: &str) -> &str {
    delete_key
        .split_once('+')
        .map(|(_, uniq_key)| uniq_key)
        .unwrap_or(delete_key)
}

fn validate_timer_entry(entry: &TimerRocksDbBuildEntry) -> Result<(), RocketMQError> {
    if entry.queue_offset < 0 {
        return Err(RocketMQError::ConfigInvalidValue {
            key: "rocksdb.timer.queue_offset",
            value: entry.queue_offset.to_string(),
            reason: "queue offset must be non-negative".to_string(),
        });
    }
    Ok(())
}
