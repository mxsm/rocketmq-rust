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

use crate::column_family::RocksDbColumnFamily;
use crate::message::MessageRocksDbStorage;
use crate::message::TransRocksDbRecord;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RocksDbTransactionDispatchKind {
    Half,
    Operation,
    Other,
}

/// Backend-neutral projection of one transaction CommitLog dispatch record.
pub trait RocksDbTransactionDispatch {
    fn kind(&self) -> RocksDbTransactionDispatchKind;
    fn commit_log_offset(&self) -> i64;
    fn message_size(&self) -> i32;
    fn real_topic(&self) -> Option<&str>;
    fn transaction_id(&self) -> Option<&str>;
    fn transaction_offset(&self) -> Option<i64>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RocksDbTransBuildConfig {
    pub queue_capacity: usize,
    pub batch_size: usize,
}

impl Default for RocksDbTransBuildConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 100_000,
            batch_size: 1000,
        }
    }
}

impl RocksDbTransBuildConfig {
    fn validate(self) -> Result<Self, RocketMQError> {
        if self.queue_capacity == 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.trans.queue_capacity",
                value: self.queue_capacity.to_string(),
                reason: "queue capacity must be greater than zero".to_string(),
            });
        }
        if self.batch_size == 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.trans.batch_size",
                value: self.batch_size.to_string(),
                reason: "batch size must be greater than zero".to_string(),
            });
        }
        Ok(self)
    }
}

pub struct RocksDbTransBuildService {
    storage: Arc<MessageRocksDbStorage>,
    config: RocksDbTransBuildConfig,
    pending: Mutex<VecDeque<TransRocksDbRecord>>,
}

impl RocksDbTransBuildService {
    pub fn new(storage: Arc<MessageRocksDbStorage>, config: RocksDbTransBuildConfig) -> Result<Self, RocketMQError> {
        let config = config.validate()?;
        Ok(Self {
            storage,
            config,
            pending: Mutex::new(VecDeque::with_capacity(config.queue_capacity.min(1024))),
        })
    }

    pub fn enqueue(&self, record: TransRocksDbRecord) -> Result<usize, RocketMQError> {
        let mut pending = self.pending.lock().map_err(|error| {
            RocketMQError::storage_write_failed("rocksdb", format!("trans queue lock poisoned: {error}"))
        })?;
        if pending.len() >= self.config.queue_capacity {
            return Err(RocketMQError::storage_write_failed(
                "rocksdb",
                format!(
                    "trans queue full: capacity={}, pending={}",
                    self.config.queue_capacity,
                    pending.len()
                ),
            ));
        }
        pending.push_back(record);
        Ok(1)
    }

    pub fn build_trans_index<S>(&self, dispatch: &S) -> Result<usize, RocketMQError>
    where
        S: RocksDbTransactionDispatch + ?Sized,
    {
        let Some(record) = self.record_for_dispatch(dispatch)? else {
            return Ok(0);
        };
        self.enqueue(record)
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
            if let Err(error) = self.storage.write_records_for_trans(&batch) {
                self.requeue_front(batch)?;
                return Err(error);
            }
            flushed += batch_len;
        }
    }

    pub async fn flush_pending_blocking(self: Arc<Self>) -> Result<usize, RocketMQError> {
        crate::runtime::spawn_io("rocksdb.transaction.flush_pending", move || self.flush_pending()).await?
    }

    pub fn get_dispatch_from_phy_offset(&self) -> Result<Option<i64>, RocketMQError> {
        let last_offset = self
            .storage
            .get_last_offset_py(RocksDbColumnFamily::Transaction.name())?;
        Ok((last_offset > 0).then_some(last_offset))
    }

    fn record_for_dispatch<S>(&self, dispatch: &S) -> Result<Option<TransRocksDbRecord>, RocketMQError>
    where
        S: RocksDbTransactionDispatch + ?Sized,
    {
        if dispatch.commit_log_offset() < 0
            || dispatch.message_size() <= 0
            || dispatch.kind() == RocksDbTransactionDispatchKind::Other
        {
            return Ok(None);
        }

        if self
            .get_dispatch_from_phy_offset()?
            .is_some_and(|last_offset| dispatch.commit_log_offset() < last_offset)
        {
            return Ok(None);
        }

        let Some(real_topic) = dispatch.real_topic().filter(|topic| !topic.is_empty()) else {
            return Ok(None);
        };
        let Some(transaction_id) = dispatch.transaction_id().filter(|id| !id.is_empty()) else {
            return Ok(None);
        };

        let (offset_py, is_op) = match dispatch.kind() {
            RocksDbTransactionDispatchKind::Half => (dispatch.commit_log_offset(), false),
            RocksDbTransactionDispatchKind::Operation => {
                let Some(offset) = dispatch.transaction_offset().filter(|offset| *offset >= 0) else {
                    return Ok(None);
                };
                (offset, true)
            }
            RocksDbTransactionDispatchKind::Other => return Ok(None),
        };

        Ok(Some(TransRocksDbRecord {
            offset_py,
            topic: real_topic.to_string(),
            uniq_key: transaction_id.to_string(),
            check_times: 0,
            size_py: dispatch.message_size(),
            is_op,
            delete: false,
        }))
    }

    fn drain_batch(&self) -> Result<Vec<TransRocksDbRecord>, RocketMQError> {
        let mut pending = self.pending.lock().map_err(|error| {
            RocketMQError::storage_write_failed("rocksdb", format!("trans queue lock poisoned: {error}"))
        })?;
        let batch_len = self.config.batch_size.min(pending.len());
        Ok(pending.drain(..batch_len).collect())
    }

    fn requeue_front(&self, mut batch: Vec<TransRocksDbRecord>) -> Result<(), RocketMQError> {
        let mut pending = self.pending.lock().map_err(|error| {
            RocketMQError::storage_write_failed("rocksdb", format!("trans queue lock poisoned: {error}"))
        })?;
        while let Some(record) = batch.pop() {
            pending.push_front(record);
        }
        Ok(())
    }
}
