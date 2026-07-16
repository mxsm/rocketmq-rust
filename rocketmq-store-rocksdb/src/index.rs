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

use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use rocketmq_error::RocketMQError;

use crate::column_family::RocksDbColumnFamily;
use crate::message::IndexRocksDbRecord;
use crate::message::MessageRocksDbStorage;

pub trait RocksDbIndexDispatch {
    fn topic(&self) -> &str;

    fn commit_log_offset(&self) -> i64;

    fn message_size(&self) -> i32;

    fn store_timestamp(&self) -> i64;

    fn is_transaction_rollback(&self) -> bool;

    fn keys(&self) -> &str;

    fn uniq_key(&self) -> Option<&str>;

    fn tags(&self) -> Option<&str>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RocksDbIndexBuildConfig {
    pub queue_capacity: usize,
    pub batch_size: usize,
}

impl Default for RocksDbIndexBuildConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 100_000,
            batch_size: 1000,
        }
    }
}

impl RocksDbIndexBuildConfig {
    fn validate(self) -> Result<Self, RocketMQError> {
        if self.queue_capacity == 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.index.queue_capacity",
                value: self.queue_capacity.to_string(),
                reason: "queue capacity must be greater than zero".to_string(),
            });
        }
        if self.batch_size == 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.index.batch_size",
                value: self.batch_size.to_string(),
                reason: "batch size must be greater than zero".to_string(),
            });
        }
        Ok(self)
    }
}

pub struct RocksDbIndexBuildService {
    storage: Arc<MessageRocksDbStorage>,
    config: RocksDbIndexBuildConfig,
    pending: Mutex<VecDeque<IndexRocksDbRecord>>,
}

impl RocksDbIndexBuildService {
    pub fn new(storage: Arc<MessageRocksDbStorage>, config: RocksDbIndexBuildConfig) -> Result<Self, RocketMQError> {
        let config = config.validate()?;
        Ok(Self {
            storage,
            config,
            pending: Mutex::new(VecDeque::with_capacity(config.queue_capacity.min(1024))),
        })
    }

    pub fn build_index<R>(&self, dispatch_request: &R) -> Result<usize, RocketMQError>
    where
        R: RocksDbIndexDispatch + ?Sized,
    {
        let records = self.records_for_dispatch(dispatch_request)?;
        if records.is_empty() {
            return Ok(0);
        }

        let mut pending = self.pending.lock().map_err(|error| {
            RocketMQError::storage_write_failed("rocksdb", format!("index queue lock poisoned: {error}"))
        })?;
        let available = self.config.queue_capacity.saturating_sub(pending.len());
        if records.len() > available {
            return Err(RocketMQError::storage_write_failed(
                "rocksdb",
                format!(
                    "index queue full: capacity={}, pending={}, incoming={}",
                    self.config.queue_capacity,
                    pending.len(),
                    records.len()
                ),
            ));
        }

        let count = records.len();
        pending.extend(records);
        Ok(count)
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
            if let Err(error) = self.storage.write_records_for_index(&batch) {
                self.requeue_front(batch)?;
                return Err(error);
            }
            flushed += batch_len;
        }
    }

    pub async fn flush_pending_blocking(self: Arc<Self>) -> Result<usize, RocketMQError> {
        crate::runtime::spawn_io("rocksdb.index.flush_pending", move || self.flush_pending()).await?
    }

    pub fn get_dispatch_from_phy_offset(&self) -> Result<Option<i64>, RocketMQError> {
        let last_offset = self.storage.get_last_offset_py(RocksDbColumnFamily::Default.name())?;
        Ok((last_offset > 0).then_some(last_offset))
    }

    fn records_for_dispatch<R>(&self, dispatch_request: &R) -> Result<Vec<IndexRocksDbRecord>, RocketMQError>
    where
        R: RocksDbIndexDispatch + ?Sized,
    {
        if dispatch_request.commit_log_offset() < 0
            || dispatch_request.message_size() <= 0
            || dispatch_request.topic().is_empty()
            || dispatch_request.store_timestamp() <= 0
        {
            return Ok(Vec::new());
        }

        if dispatch_request.is_transaction_rollback() {
            return Ok(Vec::new());
        }

        if self
            .get_dispatch_from_phy_offset()?
            .is_some_and(|last_offset| dispatch_request.commit_log_offset() < last_offset)
        {
            return Ok(Vec::new());
        }

        let Some(uniq_key) = dispatch_request.uniq_key().filter(|key| !key.is_empty()) else {
            return Ok(Vec::new());
        };

        let topic = dispatch_request.topic();
        let mut records = Vec::new();
        let mut seen_keys = HashSet::new();
        for key in dispatch_request.keys().split(' ') {
            if key.is_empty() || !seen_keys.insert(key) {
                continue;
            }
            records.push(IndexRocksDbRecord::normal_key(
                topic,
                key,
                uniq_key,
                dispatch_request.store_timestamp(),
                dispatch_request.commit_log_offset(),
            ));
        }

        if let Some(tag) = dispatch_request.tags().filter(|tag| !tag.is_empty()) {
            records.push(IndexRocksDbRecord::tag_key(
                topic,
                tag,
                uniq_key,
                dispatch_request.store_timestamp(),
                dispatch_request.commit_log_offset(),
            ));
        }

        records.push(IndexRocksDbRecord::unique_key(
            topic,
            uniq_key,
            dispatch_request.store_timestamp(),
            dispatch_request.commit_log_offset(),
        ));
        Ok(records)
    }

    fn drain_batch(&self) -> Result<Vec<IndexRocksDbRecord>, RocketMQError> {
        let mut pending = self.pending.lock().map_err(|error| {
            RocketMQError::storage_write_failed("rocksdb", format!("index queue lock poisoned: {error}"))
        })?;
        let batch_len = self.config.batch_size.min(pending.len());
        Ok(pending.drain(..batch_len).collect())
    }

    fn requeue_front(&self, mut batch: Vec<IndexRocksDbRecord>) -> Result<(), RocketMQError> {
        let mut pending = self.pending.lock().map_err(|error| {
            RocketMQError::storage_write_failed("rocksdb", format!("index queue lock poisoned: {error}"))
        })?;
        while let Some(record) = batch.pop() {
            pending.push_front(record);
        }
        Ok(())
    }
}
