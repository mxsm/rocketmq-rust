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
use std::sync::Weak;

use rocketmq_common::common::message::MessageConst;
use rocketmq_error::RocketMQError;
use tracing::warn;

use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::config::message_store_config::MessageStoreConfig;
use crate::rocksdb::column_family::RocksDbColumnFamily;
use crate::rocksdb::message::MessageRocksDbStorage;
use crate::rocksdb::message::TransRocksDbRecord;

pub const RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC: &str = "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC";
pub const RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC: &str = "RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC";
pub const PROPERTY_TRANS_OFFSET: &str = "TRANS_OFFSET";

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

    pub fn build_trans_index(&self, dispatch_request: &DispatchRequest) -> Result<usize, RocketMQError> {
        let Some(record) = self.record_for_dispatch(dispatch_request)? else {
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
        crate::rocksdb::runtime::spawn_io("rocksdb.transaction.flush_pending", move || self.flush_pending()).await?
    }

    pub fn get_dispatch_from_phy_offset(&self) -> Result<Option<i64>, RocketMQError> {
        let last_offset = self
            .storage
            .get_last_offset_py(RocksDbColumnFamily::Transaction.name())?;
        Ok((last_offset > 0).then_some(last_offset))
    }

    fn record_for_dispatch(
        &self,
        dispatch_request: &DispatchRequest,
    ) -> Result<Option<TransRocksDbRecord>, RocketMQError> {
        if dispatch_request.commit_log_offset < 0
            || dispatch_request.msg_size <= 0
            || dispatch_request.topic.is_empty()
            || !is_rocksdb_trans_topic(dispatch_request.topic.as_str())
        {
            return Ok(None);
        }

        if self
            .get_dispatch_from_phy_offset()?
            .is_some_and(|last_offset| dispatch_request.commit_log_offset < last_offset)
        {
            return Ok(None);
        }

        let Some(properties) = dispatch_request.properties_map.as_ref() else {
            return Ok(None);
        };
        let Some(real_topic) = properties
            .get(MessageConst::PROPERTY_REAL_TOPIC)
            .filter(|topic| !topic.is_empty())
        else {
            return Ok(None);
        };
        let Some(transaction_id) = properties
            .get(MessageConst::PROPERTY_TRANSACTION_ID)
            .filter(|transaction_id| !transaction_id.is_empty())
        else {
            return Ok(None);
        };

        if dispatch_request.topic.as_str() == RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC {
            return Ok(Some(TransRocksDbRecord {
                offset_py: dispatch_request.commit_log_offset,
                topic: real_topic.to_string(),
                uniq_key: transaction_id.to_string(),
                check_times: 0,
                size_py: dispatch_request.msg_size,
                is_op: false,
                delete: false,
            }));
        }

        let trans_offset = properties
            .get(PROPERTY_TRANS_OFFSET)
            .and_then(|offset| offset.as_str().parse::<i64>().ok())
            .filter(|offset| *offset >= 0);
        Ok(trans_offset.map(|offset_py| TransRocksDbRecord {
            offset_py,
            topic: real_topic.to_string(),
            uniq_key: transaction_id.to_string(),
            check_times: 0,
            size_py: dispatch_request.msg_size,
            is_op: true,
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

pub struct CommitLogDispatcherBuildRocksDbTrans {
    trans_service: Weak<RocksDbTransBuildService>,
    message_store_config: Arc<MessageStoreConfig>,
}

impl CommitLogDispatcherBuildRocksDbTrans {
    pub fn new(trans_service: Arc<RocksDbTransBuildService>, message_store_config: Arc<MessageStoreConfig>) -> Self {
        Self {
            trans_service: Arc::downgrade(&trans_service),
            message_store_config,
        }
    }

    pub fn trans_service(&self) -> Option<Arc<RocksDbTransBuildService>> {
        self.trans_service.upgrade()
    }
}

impl CommitLogDispatcher for CommitLogDispatcherBuildRocksDbTrans {
    fn dispatch(&self, dispatch_request: &mut DispatchRequest) {
        if !self.message_store_config.trans_rocksdb_enable {
            return;
        }
        let Some(trans_service) = self.trans_service.upgrade() else {
            warn!("skip RocksDB transaction dispatch because trans service has been dropped");
            return;
        };
        if let Err(error) = trans_service.build_trans_index(dispatch_request) {
            warn!(error = %error, "failed to enqueue RocksDB transaction record");
        }
    }

    fn dispatch_batch(&self, dispatch_requests: &mut [DispatchRequest]) {
        for request in dispatch_requests {
            self.dispatch(request);
        }
        if !self.message_store_config.trans_rocksdb_enable {
            return;
        }
        let Some(trans_service) = self.trans_service.upgrade() else {
            warn!("skip RocksDB transaction batch flush because trans service has been dropped");
            return;
        };
        if let Err(error) = trans_service.flush_pending() {
            warn!(error = %error, "failed to flush RocksDB transaction batch");
        }
    }

    fn dispatch_progress_offset(&self, _commit_log_min_offset: i64) -> Option<i64> {
        if !self.message_store_config.trans_rocksdb_enable {
            return None;
        }
        let Some(trans_service) = self.trans_service.upgrade() else {
            warn!("skip RocksDB transaction progress because trans service has been dropped");
            return None;
        };
        trans_service.get_dispatch_from_phy_offset().unwrap_or_else(|error| {
            warn!(error = %error, "failed to read RocksDB transaction dispatch progress");
            None
        })
    }
}

fn is_rocksdb_trans_topic(topic: &str) -> bool {
    topic == RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC || topic == RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC
}
