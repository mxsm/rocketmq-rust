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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_error::RocketMQError;
use rocketmq_store_api::GetStatus;
use rocketmq_store_api::StoreError as ApiStoreError;
use rocketmq_store_local::commit_log::read::LocalWalPort;
use thiserror::Error;
use tracing::warn;

use crate::column_family::RocksDbColumnFamily;
use crate::config::RocksDbConfig;
use crate::config::RocksDbConfigSource;
use crate::consume_queue::RocksDbConsumeQueueStore;
use crate::index::RocksDbIndexBuildConfig;
use crate::index::RocksDbIndexBuildService;
use crate::maintenance::RocksDbMaintenanceService;
use crate::message::MessageRocksDbStorage;
use crate::store::RocksDbStore;
use crate::timer::RocksDbTimerBuildConfig;
use crate::timer::RocksDbTimerBuildService;
use crate::transaction::RocksDbTransBuildConfig;
use crate::transaction::RocksDbTransBuildService;
use crate::value::ConsumeQueueValue;

const INDEX_KEY_TYPE: &str = "K";
const INDEX_UNIQUE_TYPE: &str = "U";
const MILLIS_PER_DAY: i64 = 24 * 60 * 60 * 1000;

#[derive(Debug, Error)]
pub enum RocksDbMessageStoreError {
    #[error("RocksDB message-store configuration error: {0}")]
    Config(String),
    #[error("RocksDB derived-store error: {source}")]
    Backend {
        #[source]
        source: RocketMQError,
    },
    #[error("Local CommitLog read error: {source}")]
    Local {
        #[source]
        source: ApiStoreError,
    },
}

impl RocksDbMessageStoreError {
    pub fn into_backend(self) -> Option<RocketMQError> {
        match self {
            Self::Backend { source } => Some(source),
            Self::Config(_) | Self::Local { .. } => None,
        }
    }
}

impl From<RocketMQError> for RocksDbMessageStoreError {
    fn from(source: RocketMQError) -> Self {
        Self::Backend { source }
    }
}

impl From<ApiStoreError> for RocksDbMessageStoreError {
    fn from(source: ApiStoreError) -> Self {
        Self::Local { source }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RocksDbMessageStoreOptions {
    pub timer_enabled: bool,
    pub transaction_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RocksDbTimeBoundary {
    Lower,
    Upper,
}

pub struct RocksDbReadRequest<'a> {
    pub topic: &'a str,
    pub queue_id: i32,
    pub offset: i64,
    pub max_message_count: i32,
    pub max_total_message_size: i32,
    pub max_pull_message_size: i32,
}

pub struct RocksDbReadRecord<S> {
    pub selection: S,
    pub queue_offset: u64,
    pub batch_count: i32,
}

pub struct RocksDbReadResult<S> {
    pub records: Vec<RocksDbReadRecord<S>>,
    pub status: GetStatus,
    pub next_begin_offset: i64,
    pub min_offset: i64,
    pub max_offset: i64,
    pub buffer_total_size: i32,
    pub message_count: i32,
}

pub struct RocksDbIndexLookup<S> {
    pub records: Vec<S>,
    pub last_update_timestamp: i64,
    pub last_update_physical_offset: i64,
}

/// Canonical RocksDB-derived state owned independently from the Local WAL.
pub struct RocksDbDerivedStore {
    rocksdb_config: RocksDbConfig,
    rocksdb_store: Arc<RocksDbStore>,
    consume_queue_store: RocksDbConsumeQueueStore,
    message_rocksdb_config: RocksDbConfig,
    message_rocksdb_storage: Arc<MessageRocksDbStorage>,
    rocksdb_index_service: Arc<RocksDbIndexBuildService>,
    rocksdb_timer_service: Option<Arc<RocksDbTimerBuildService>>,
    rocksdb_trans_service: Option<Arc<RocksDbTransBuildService>>,
    rocksdb_maintenance_service: RocksDbMaintenanceService,
    message_rocksdb_maintenance_service: RocksDbMaintenanceService,
}

impl RocksDbDerivedStore {
    /// Opens the two RocksDB derived-state databases without creating a message log.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for an incompatible path or a backend error
    /// when native RocksDB cannot be opened.
    pub fn open<S>(source: &S, options: RocksDbMessageStoreOptions) -> Result<Self, RocksDbMessageStoreError>
    where
        S: RocksDbConfigSource + ?Sized,
    {
        if !source.rocksdb_store_enabled() {
            return Err(RocksDbMessageStoreError::Config(
                "RocksDBMessageStore requires store_type=RocksDB".to_string(),
            ));
        }
        let conflict_path = RocksDbConfig::consume_queue_conflict_path_from_message_store_config(source);
        if conflict_path.join("CURRENT").is_file() {
            return Err(RocksDbMessageStoreError::Config(format!(
                "found RocksDB consume queue in incompatible path: {}, maybe incompatible \
                 use_separate_store_path_for_rocksdb_cq config",
                conflict_path.display()
            )));
        }

        let rocksdb_config = RocksDbConfig::consume_queue_from_message_store_config(source);
        rocksdb_config.validate()?;
        let rocksdb_store = Arc::new(RocksDbStore::open(rocksdb_config.clone())?);
        let consume_queue_store = RocksDbConsumeQueueStore::new(Arc::clone(&rocksdb_store));
        let message_rocksdb_config = RocksDbConfig::message_from_message_store_config(source);
        message_rocksdb_config.validate()?;
        let message_rocksdb_storage = Arc::new(MessageRocksDbStorage::open(message_rocksdb_config.clone())?);
        let rocksdb_maintenance_service =
            RocksDbMaintenanceService::new(Arc::clone(&rocksdb_store), rocksdb_config.clone());
        let message_rocksdb_maintenance_service =
            RocksDbMaintenanceService::new(message_rocksdb_storage.store_arc(), message_rocksdb_config.clone());
        let rocksdb_index_service = Arc::new(RocksDbIndexBuildService::new(
            Arc::clone(&message_rocksdb_storage),
            RocksDbIndexBuildConfig::default(),
        )?);
        let rocksdb_timer_service = options
            .timer_enabled
            .then(|| {
                RocksDbTimerBuildService::new(Arc::clone(&message_rocksdb_storage), RocksDbTimerBuildConfig::default())
                    .map(Arc::new)
            })
            .transpose()?;
        let rocksdb_trans_service = options
            .transaction_enabled
            .then(|| {
                RocksDbTransBuildService::new(Arc::clone(&message_rocksdb_storage), RocksDbTransBuildConfig::default())
                    .map(Arc::new)
            })
            .transpose()?;

        Ok(Self {
            rocksdb_config,
            rocksdb_store,
            consume_queue_store,
            message_rocksdb_config,
            message_rocksdb_storage,
            rocksdb_index_service,
            rocksdb_timer_service,
            rocksdb_trans_service,
            rocksdb_maintenance_service,
            message_rocksdb_maintenance_service,
        })
    }

    pub const fn rocksdb_config(&self) -> &RocksDbConfig {
        &self.rocksdb_config
    }

    pub const fn message_rocksdb_config(&self) -> &RocksDbConfig {
        &self.message_rocksdb_config
    }

    pub fn rocksdb_store(&self) -> Arc<RocksDbStore> {
        Arc::clone(&self.rocksdb_store)
    }

    pub const fn consume_queue_store(&self) -> &RocksDbConsumeQueueStore {
        &self.consume_queue_store
    }

    pub fn message_rocksdb_storage(&self) -> Arc<MessageRocksDbStorage> {
        Arc::clone(&self.message_rocksdb_storage)
    }

    pub fn rocksdb_index_service(&self) -> Arc<RocksDbIndexBuildService> {
        Arc::clone(&self.rocksdb_index_service)
    }

    pub fn rocksdb_timer_service(&self) -> Option<Arc<RocksDbTimerBuildService>> {
        self.rocksdb_timer_service.as_ref().map(Arc::clone)
    }

    pub fn rocksdb_trans_service(&self) -> Option<Arc<RocksDbTransBuildService>> {
        self.rocksdb_trans_service.as_ref().map(Arc::clone)
    }

    pub fn is_rocksdb_maintenance_running(&self) -> bool {
        self.rocksdb_maintenance_service.is_running()
    }

    pub fn is_message_rocksdb_maintenance_running(&self) -> bool {
        self.message_rocksdb_maintenance_service.is_running()
    }

    pub fn start_maintenance(&mut self) {
        self.rocksdb_maintenance_service.start();
        self.message_rocksdb_maintenance_service.start();
    }

    pub async fn shutdown_maintenance(&mut self) -> Result<(), RocksDbMessageStoreError> {
        let consume_queue_result = self.rocksdb_maintenance_service.shutdown_gracefully().await;
        let message_result = self.message_rocksdb_maintenance_service.shutdown_gracefully().await;
        consume_queue_result?;
        message_result?;
        Ok(())
    }

    pub fn close(&self) {
        if let Err(error) = self.rocksdb_index_service.flush_pending() {
            warn!(error = %error, "failed to flush pending RocksDB index records before close");
        }
        if let Some(timer_service) = self.rocksdb_timer_service.as_ref() {
            if let Err(error) = timer_service.flush_pending() {
                warn!(error = %error, "failed to flush pending RocksDB timer records before close");
            }
        }
        if let Some(trans_service) = self.rocksdb_trans_service.as_ref() {
            if let Err(error) = trans_service.flush_pending() {
                warn!(error = %error, "failed to flush pending RocksDB transaction records before close");
            }
        }
        self.rocksdb_store.close();
        self.message_rocksdb_storage.store().close();
    }

    pub fn flush_derived(&self) -> Result<(), RocksDbMessageStoreError> {
        self.rocksdb_index_service.flush_pending()?;
        self.rocksdb_store.flush()?;
        self.message_rocksdb_storage.store().flush()?;
        Ok(())
    }

    pub fn max_offset(&self, topic: &str, queue_id: i32) -> Result<i64, RocksDbMessageStoreError> {
        Ok(self
            .consume_queue_store
            .get_max_offset_in_queue(topic.to_string(), queue_id)?)
    }

    pub fn min_offset(&self, topic: &str, queue_id: i32) -> Result<i64, RocksDbMessageStoreError> {
        Ok(self
            .consume_queue_store
            .get_min_offset_in_queue(topic.to_string(), queue_id)?)
    }

    pub fn consume_queue_value(
        &self,
        topic: &str,
        queue_id: i32,
        offset: i64,
    ) -> Result<Option<ConsumeQueueValue>, RocksDbMessageStoreError> {
        self.consume_queue_store
            .get(topic.to_string(), queue_id, offset)?
            .map(|value| ConsumeQueueValue::decode(value.as_ref()).map_err(Into::into))
            .transpose()
    }

    pub fn offset_by_time(
        &self,
        topic: &str,
        queue_id: i32,
        timestamp: i64,
        boundary: RocksDbTimeBoundary,
    ) -> Result<i64, RocksDbMessageStoreError> {
        let min_offset = self.min_offset(topic, queue_id)?;
        let max_offset = self.max_offset(topic, queue_id)?;
        if max_offset <= min_offset {
            return Ok(0);
        }

        let mut low = min_offset;
        let mut high = max_offset - 1;
        let mut lower = max_offset;
        while low <= high {
            let middle = low + (high - low) / 2;
            let Some(value) = self.consume_queue_value(topic, queue_id, middle)? else {
                return Ok(0);
            };
            if value.msg_store_time >= timestamp {
                lower = middle;
                high = middle - 1;
            } else {
                low = middle + 1;
            }
        }
        if boundary == RocksDbTimeBoundary::Lower {
            return Ok(lower);
        }

        low = min_offset;
        high = max_offset - 1;
        let mut upper = None;
        while low <= high {
            let middle = low + (high - low) / 2;
            let Some(value) = self.consume_queue_value(topic, queue_id, middle)? else {
                return Ok(0);
            };
            if value.msg_store_time <= timestamp {
                upper = Some(middle);
                low = middle + 1;
            } else {
                high = middle - 1;
            }
        }
        Ok(upper.unwrap_or(0))
    }

    pub fn topic_queue_offsets(&self) -> Result<HashMap<(String, i32), i64>, RocksDbMessageStoreError> {
        Ok(self.consume_queue_store.max_offsets_by_topic_queue()?)
    }

    pub fn delete_topic(&self, topic: &str) -> Result<(), RocksDbMessageStoreError> {
        self.consume_queue_store.destroy_topic(topic)?;
        Ok(())
    }

    pub fn truncate_dirty(&self, physical_offset: i64) -> Result<(), RocksDbMessageStoreError> {
        self.consume_queue_store.truncate_dirty(physical_offset)?;
        Ok(())
    }

    pub fn clean_expired(&self, min_physical_offset: i64) -> Result<(), RocksDbMessageStoreError> {
        self.consume_queue_store.clean_expired_background(min_physical_offset)?;
        Ok(())
    }

    fn index_offsets(
        &self,
        topic: &str,
        key: &str,
        max_num: usize,
        begin: i64,
        end: i64,
        max_query_days: usize,
    ) -> Result<(Vec<i64>, i64, i64), RocksDbMessageStoreError> {
        let (begin, end) = normalize_index_query_time_range(begin, end, max_query_days);
        let mut offsets =
            self.message_rocksdb_storage
                .query_offsets_for_index(topic, INDEX_KEY_TYPE, key, begin, end, max_num)?;
        if offsets.is_empty() {
            offsets = self.message_rocksdb_storage.query_offsets_for_index(
                topic,
                INDEX_UNIQUE_TYPE,
                key,
                begin,
                end,
                max_num,
            )?;
        }
        offsets.sort_unstable();
        let last_timestamp = self.message_rocksdb_storage.get_last_store_timestamp_for_index()?;
        let last_offset = self
            .message_rocksdb_storage
            .get_last_offset_py(RocksDbColumnFamily::Default.name())?;
        Ok((offsets, last_timestamp, last_offset))
    }
}

/// Canonical RocksDB message-store adapter over an injected Local WAL port.
pub struct RocksDbMessageStoreRoot {
    derived: RocksDbDerivedStore,
}

impl RocksDbMessageStoreRoot {
    pub const fn new(derived: RocksDbDerivedStore) -> Self {
        Self { derived }
    }

    pub const fn derived(&self) -> &RocksDbDerivedStore {
        &self.derived
    }

    pub fn derived_mut(&mut self) -> &mut RocksDbDerivedStore {
        &mut self.derived
    }
}

impl RocksDbMessageStoreRoot {
    pub fn read<L, CqFilter, MessageFilter>(
        &self,
        local: &L,
        request: RocksDbReadRequest<'_>,
        mut cq_filter: CqFilter,
        mut message_filter: MessageFilter,
    ) -> Result<RocksDbReadResult<L::Selection>, RocksDbMessageStoreError>
    where
        L: LocalWalPort,
        CqFilter: FnMut(i64) -> bool,
        MessageFilter: FnMut(&[u8]) -> bool,
    {
        let min_offset = self.derived.min_offset(request.topic, request.queue_id)?;
        let max_offset = self.derived.max_offset(request.topic, request.queue_id)?;
        let mut status = GetStatus::NoMessageInQueue;
        let mut next_begin_offset = request.offset;
        let mut records = Vec::new();
        let mut buffer_total_size = 0;
        let mut message_count = 0;

        if max_offset == 0 {
            next_begin_offset = local.correct_queue_offset(request.offset, 0);
        } else if request.offset < min_offset {
            status = GetStatus::OffsetTooSmall;
            next_begin_offset = local.correct_queue_offset(request.offset, min_offset);
        } else if request.offset == max_offset {
            status = GetStatus::OffsetOverflowOne;
            next_begin_offset = local.correct_queue_offset(request.offset, request.offset);
        } else if request.offset > max_offset {
            status = GetStatus::OffsetOverflowBadly;
            next_begin_offset = local.correct_queue_offset(request.offset, max_offset);
        } else {
            status = GetStatus::NoMatchedMessage;
            let max_pull_size = request.max_total_message_size.clamp(100, request.max_pull_message_size);
            let read_count = request.max_message_count.max(0);
            let values = self.derived.consume_queue_store.range_query(
                request.topic.to_string(),
                request.queue_id,
                request.offset,
                read_count,
            )?;
            if values.is_empty() {
                status = GetStatus::OffsetFoundNull;
                next_begin_offset = local.correct_queue_offset(request.offset, request.offset.saturating_add(1));
            } else {
                for (index, encoded_value) in values.into_iter().enumerate() {
                    if message_count >= request.max_message_count || buffer_total_size >= max_pull_size {
                        break;
                    }
                    let queue_offset = request.offset + index as i64;
                    next_begin_offset = queue_offset + 1;
                    let value = match ConsumeQueueValue::decode(encoded_value.as_ref()) {
                        Ok(value) => value,
                        Err(_) => {
                            status = GetStatus::OffsetFoundNull;
                            break;
                        }
                    };
                    if !cq_filter(value.tag_hash_code) {
                        continue;
                    }
                    let Some(selection) = local.read_message(value.commit_log_physical_offset, value.body_size)? else {
                        if buffer_total_size == 0 {
                            status = GetStatus::MessageWasRemoving;
                        }
                        continue;
                    };
                    let bytes = local.selection_bytes(&selection);
                    if !message_filter(bytes) {
                        continue;
                    }
                    let size = i32::try_from(bytes.len()).unwrap_or(i32::MAX);
                    buffer_total_size = buffer_total_size.saturating_add(size);
                    message_count += 1;
                    records.push(RocksDbReadRecord {
                        selection,
                        queue_offset: queue_offset as u64,
                        batch_count: 1,
                    });
                    status = GetStatus::Found;
                }
            }
        }

        Ok(RocksDbReadResult {
            records,
            status,
            next_begin_offset,
            min_offset,
            max_offset,
            buffer_total_size,
            message_count,
        })
    }

    pub fn query_index<L>(
        &self,
        local: &L,
        topic: &str,
        key: &str,
        max_num: usize,
        begin: i64,
        end: i64,
        max_query_days: usize,
    ) -> Result<RocksDbIndexLookup<L::Selection>, RocksDbMessageStoreError>
    where
        L: LocalWalPort,
    {
        let (offsets, last_update_timestamp, last_update_physical_offset) =
            self.derived
                .index_offsets(topic, key, max_num, begin, end, max_query_days)?;
        let mut records = Vec::with_capacity(offsets.len());
        for offset in offsets {
            if let Some(selection) = local.read_from(offset)? {
                records.push(selection);
            }
        }
        Ok(RocksDbIndexLookup {
            records,
            last_update_timestamp,
            last_update_physical_offset,
        })
    }
}

fn normalize_index_query_time_range(begin: i64, end: i64, max_query_days: usize) -> (i64, i64) {
    if begin > 0 && end > 0 && begin <= end && end != i64::MAX {
        return (begin, end);
    }
    let end = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX));
    let max_query_days = i64::try_from(max_query_days).unwrap_or(i64::MAX / MILLIS_PER_DAY);
    let begin = end.saturating_sub(max_query_days.saturating_mul(MILLIS_PER_DAY));
    (begin, end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_index_query_time_range_uses_configured_query_days() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should follow Unix epoch")
            .as_millis() as i64;
        let (begin, end) = normalize_index_query_time_range(0, i64::MAX, 2);
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should follow Unix epoch")
            .as_millis() as i64;
        assert!(end >= before && end <= after);
        assert_eq!(end - begin, 2 * MILLIS_PER_DAY);
    }
}
