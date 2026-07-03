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
use std::sync::Weak;

use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_error::RocketMQError;

use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::config::message_store_config::MessageStoreConfig;
use crate::rocksdb::column_family::RocksDbColumnFamily;
use crate::rocksdb::message::IndexRocksDbRecord;
use crate::rocksdb::message::MessageRocksDbStorage;
use tracing::warn;

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

    pub fn build_index(&self, dispatch_request: &DispatchRequest) -> Result<usize, RocketMQError> {
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
        crate::rocksdb::runtime::spawn_io("rocksdb.index.flush_pending", move || self.flush_pending()).await?
    }

    pub fn get_dispatch_from_phy_offset(&self) -> Result<Option<i64>, RocketMQError> {
        let last_offset = self.storage.get_last_offset_py(RocksDbColumnFamily::Default.name())?;
        Ok((last_offset > 0).then_some(last_offset))
    }

    fn records_for_dispatch(
        &self,
        dispatch_request: &DispatchRequest,
    ) -> Result<Vec<IndexRocksDbRecord>, RocketMQError> {
        if dispatch_request.commit_log_offset < 0
            || dispatch_request.msg_size <= 0
            || dispatch_request.topic.is_empty()
            || dispatch_request.store_timestamp <= 0
        {
            return Ok(Vec::new());
        }

        match MessageSysFlag::get_transaction_value(dispatch_request.sys_flag) {
            MessageSysFlag::TRANSACTION_ROLLBACK_TYPE => return Ok(Vec::new()),
            MessageSysFlag::TRANSACTION_NOT_TYPE
            | MessageSysFlag::TRANSACTION_PREPARED_TYPE
            | MessageSysFlag::TRANSACTION_COMMIT_TYPE => {}
            _ => {}
        }

        if self
            .get_dispatch_from_phy_offset()?
            .is_some_and(|last_offset| dispatch_request.commit_log_offset < last_offset)
        {
            return Ok(Vec::new());
        }

        let Some(uniq_key) = dispatch_request.uniq_key.as_ref().filter(|key| !key.is_empty()) else {
            return Ok(Vec::new());
        };

        let topic = dispatch_request.topic.as_str();
        let uniq_key = uniq_key.as_str();
        let mut records = Vec::new();
        let mut seen_keys = HashSet::new();
        for key in dispatch_request.keys.split(MessageConst::KEY_SEPARATOR) {
            if key.is_empty() || !seen_keys.insert(key) {
                continue;
            }
            records.push(IndexRocksDbRecord::normal_key(
                topic,
                key,
                uniq_key,
                dispatch_request.store_timestamp,
                dispatch_request.commit_log_offset,
            ));
        }

        if let Some(tag) = dispatch_request
            .properties_map
            .as_ref()
            .and_then(|properties| properties.get(MessageConst::PROPERTY_TAGS))
            .filter(|tag| !tag.is_empty())
        {
            records.push(IndexRocksDbRecord::tag_key(
                topic,
                tag.as_str(),
                uniq_key,
                dispatch_request.store_timestamp,
                dispatch_request.commit_log_offset,
            ));
        }

        records.push(IndexRocksDbRecord::unique_key(
            topic,
            uniq_key,
            dispatch_request.store_timestamp,
            dispatch_request.commit_log_offset,
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

pub struct CommitLogDispatcherBuildRocksDbIndex {
    index_service: Weak<RocksDbIndexBuildService>,
    message_store_config: Arc<MessageStoreConfig>,
}

impl CommitLogDispatcherBuildRocksDbIndex {
    pub fn new(index_service: Arc<RocksDbIndexBuildService>, message_store_config: Arc<MessageStoreConfig>) -> Self {
        Self {
            index_service: Arc::downgrade(&index_service),
            message_store_config,
        }
    }

    pub fn index_service(&self) -> Option<Arc<RocksDbIndexBuildService>> {
        self.index_service.upgrade()
    }
}

impl CommitLogDispatcher for CommitLogDispatcherBuildRocksDbIndex {
    fn dispatch(&self, dispatch_request: &mut DispatchRequest) {
        if !self.message_store_config.message_index_enable {
            return;
        }
        let Some(index_service) = self.index_service.upgrade() else {
            warn!("skip RocksDB index dispatch because index service has been dropped");
            return;
        };
        if let Err(error) = index_service.build_index(dispatch_request) {
            warn!(error = %error, "failed to enqueue RocksDB index record");
        }
    }

    fn dispatch_batch(&self, dispatch_requests: &mut [DispatchRequest]) {
        for request in dispatch_requests {
            self.dispatch(request);
        }
        let Some(index_service) = self.index_service.upgrade() else {
            warn!("skip RocksDB index batch flush because index service has been dropped");
            return;
        };
        if let Err(error) = index_service.flush_pending() {
            warn!(error = %error, "failed to flush RocksDB index batch");
        }
    }

    fn dispatch_progress_offset(&self, _commit_log_min_offset: i64) -> Option<i64> {
        if !self.message_store_config.message_index_enable {
            return None;
        }
        let Some(index_service) = self.index_service.upgrade() else {
            warn!("skip RocksDB index progress because index service has been dropped");
            return None;
        };
        index_service.get_dispatch_from_phy_offset().unwrap_or_else(|error| {
            warn!(error = %error, "failed to read RocksDB index dispatch progress");
            None
        })
    }
}
