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

use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::system_clock::SystemClock;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_rust::ArcMut;
use tracing::warn;

use crate::base::allocate_mapped_file_service::AllocateMappedFileService;
use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::base::get_message_result::GetMessageResult;
use crate::base::message_result::AppendMessageResult;
use crate::base::message_result::PutMessageResult;
use crate::base::message_status_enum::GetMessageStatus;
use crate::base::message_store::MessageStore;
use crate::base::query_message_result::QueryMessageResult;
use crate::base::select_result::SelectMappedBufferResult;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::base::store_stats_service::StoreStatsService;
use crate::base::transient_store_pool::TransientStorePool;
use crate::config::message_store_config::MessageStoreConfig;
use crate::filter::ArcMessageFilter;
use crate::filter::MessageFilter;
use crate::ha::general_ha_service::GeneralHAService;
use crate::hook::put_message_hook::BoxedPutMessageHook;
use crate::hook::put_message_hook::PutMessageHook;
use crate::hook::send_message_back_hook::SendMessageBackHook;
use crate::log_file::commit_log::CommitLog;
use crate::log_file::mapped_file::MappedFile;
use crate::log_file::MAX_PULL_MSG_SIZE;
use crate::message_store::local_file_message_store::LocalFileMessageStore;
use crate::queue::ArcConsumeQueue;
use crate::rocksdb::config::RocksDbConfig;
use crate::rocksdb::consume_queue::CommitLogDispatcherBuildRocksDbConsumeQueue;
use crate::rocksdb::consume_queue::RocksDbConsumeQueueStore;
use crate::rocksdb::index::CommitLogDispatcherBuildRocksDbIndex;
use crate::rocksdb::index::RocksDbIndexBuildConfig;
use crate::rocksdb::index::RocksDbIndexBuildService;
use crate::rocksdb::maintenance::RocksDbMaintenanceService;
use crate::rocksdb::message::MessageRocksDbStorage;
use crate::rocksdb::store::RocksDbStore;
use crate::rocksdb::timer::CommitLogDispatcherBuildRocksDbTimer;
use crate::rocksdb::timer::RocksDbTimerBuildConfig;
use crate::rocksdb::timer::RocksDbTimerBuildService;
use crate::rocksdb::transaction::CommitLogDispatcherBuildRocksDbTrans;
use crate::rocksdb::transaction::RocksDbTransBuildConfig;
use crate::rocksdb::transaction::RocksDbTransBuildService;
use crate::rocksdb::value::ConsumeQueueValue;
use crate::stats::broker_stats_manager::BrokerStatsManager;
use crate::store::running_flags::RunningFlags;
use crate::store_error::StoreError;
use crate::timer::timer_message_store::TimerMessageStore;

const MILLIS_PER_DAY: i64 = 24 * 60 * 60 * 1000;

/// RocksDB-backed message store boundary.
///
/// The current implementation still reuses the local-file commit log path, but
/// consume queue metadata is opened through the RocksDB foundation layer instead
/// of being a type alias to `LocalFileMessageStore`. This keeps the later broker
/// wiring and commit-log dispatcher migration anchored to a concrete RocksDB
/// owner.
pub struct RocksDBMessageStore {
    local_file_store: ArcMut<LocalFileMessageStore>,
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

impl fmt::Debug for RocksDBMessageStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RocksDBMessageStore")
            .field("rocksdb_config", &self.rocksdb_config)
            .finish_non_exhaustive()
    }
}

impl RocksDBMessageStore {
    pub fn try_new(
        message_store_config: Arc<MessageStoreConfig>,
        broker_config: Arc<BrokerConfig>,
        topic_config_table: Arc<DashMap<CheetahString, ArcMut<TopicConfig>>>,
        broker_stats_manager: Option<Arc<BrokerStatsManager>>,
        notify_message_arrive_in_batch: bool,
    ) -> Result<Self, StoreError> {
        if !message_store_config.is_enable_rocksdb_store() {
            return Err(StoreError::General(
                "RocksDBMessageStore requires store_type=RocksDB".to_string(),
            ));
        }

        let message_store_config_for_index = Arc::clone(&message_store_config);
        let message_store_config_for_timer = Arc::clone(&message_store_config);
        let message_store_config_for_trans = Arc::clone(&message_store_config);
        let rocksdb_config = RocksDbConfig::consume_queue_from_message_store_config(message_store_config.as_ref());
        rocksdb_config
            .validate()
            .map_err(|error| StoreError::RocksDb(error.to_string()))?;
        let rocksdb_store = Arc::new(
            RocksDbStore::open(rocksdb_config.clone()).map_err(|error| StoreError::RocksDb(error.to_string()))?,
        );
        let consume_queue_store = RocksDbConsumeQueueStore::new(Arc::clone(&rocksdb_store));
        let message_rocksdb_config = RocksDbConfig::message_from_message_store_config(message_store_config.as_ref());
        message_rocksdb_config
            .validate()
            .map_err(|error| StoreError::RocksDb(error.to_string()))?;
        let message_rocksdb_storage = Arc::new(
            MessageRocksDbStorage::open(message_rocksdb_config.clone())
                .map_err(|error| StoreError::RocksDb(error.to_string()))?,
        );
        let rocksdb_maintenance_service =
            RocksDbMaintenanceService::new(Arc::clone(&rocksdb_store), rocksdb_config.clone());
        let message_rocksdb_maintenance_service =
            RocksDbMaintenanceService::new(message_rocksdb_storage.store_arc(), message_rocksdb_config.clone());
        let rocksdb_index_service = Arc::new(
            RocksDbIndexBuildService::new(Arc::clone(&message_rocksdb_storage), RocksDbIndexBuildConfig::default())
                .map_err(|error| StoreError::RocksDb(error.to_string()))?,
        );
        let rocksdb_timer_service = if message_store_config.timer_rocksdb_enable {
            Some(Arc::new(
                RocksDbTimerBuildService::new(Arc::clone(&message_rocksdb_storage), RocksDbTimerBuildConfig::default())
                    .map_err(|error| StoreError::RocksDb(error.to_string()))?,
            ))
        } else {
            None
        };
        let rocksdb_trans_service = if message_store_config.trans_rocksdb_enable {
            Some(Arc::new(
                RocksDbTransBuildService::new(Arc::clone(&message_rocksdb_storage), RocksDbTransBuildConfig::default())
                    .map_err(|error| StoreError::RocksDb(error.to_string()))?,
            ))
        } else {
            None
        };
        let mut local_file_store = ArcMut::new(LocalFileMessageStore::try_new(
            message_store_config,
            broker_config,
            topic_config_table,
            broker_stats_manager,
            notify_message_arrive_in_batch,
        )?);
        let local_file_store_clone = local_file_store.clone();
        local_file_store.set_message_store_arc(local_file_store_clone);
        local_file_store.add_dispatcher(Arc::new(CommitLogDispatcherBuildRocksDbConsumeQueue::new(
            consume_queue_store.clone(),
        )));
        local_file_store.add_dispatcher(Arc::new(CommitLogDispatcherBuildRocksDbIndex::new(
            Arc::clone(&rocksdb_index_service),
            message_store_config_for_index,
        )));
        if let Some(rocksdb_timer_service) = rocksdb_timer_service.as_ref() {
            local_file_store.add_dispatcher(Arc::new(CommitLogDispatcherBuildRocksDbTimer::new(
                Arc::clone(rocksdb_timer_service),
                message_store_config_for_timer,
            )));
        }
        if let Some(rocksdb_trans_service) = rocksdb_trans_service.as_ref() {
            local_file_store.add_dispatcher(Arc::new(CommitLogDispatcherBuildRocksDbTrans::new(
                Arc::clone(rocksdb_trans_service),
                message_store_config_for_trans,
            )));
        }

        Ok(Self {
            local_file_store,
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

    pub fn local_file_store(&self) -> &LocalFileMessageStore {
        self.local_file_store.as_ref()
    }

    pub fn local_file_store_mut(&mut self) -> &mut LocalFileMessageStore {
        self.local_file_store.as_mut()
    }

    pub fn local_file_store_arc(&self) -> ArcMut<LocalFileMessageStore> {
        self.local_file_store.clone()
    }

    pub fn rocksdb_config(&self) -> &RocksDbConfig {
        &self.rocksdb_config
    }

    pub fn message_rocksdb_config(&self) -> &RocksDbConfig {
        &self.message_rocksdb_config
    }

    pub fn rocksdb_store(&self) -> Arc<RocksDbStore> {
        Arc::clone(&self.rocksdb_store)
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

    pub fn close_rocksdb(&self) {
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

    pub fn consume_queue_store(&self) -> &RocksDbConsumeQueueStore {
        &self.consume_queue_store
    }

    pub fn try_get_max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> Result<i64, StoreError> {
        self.consume_queue_store
            .get_max_offset_in_queue(topic.to_string(), queue_id)
            .map_err(rocksdb_store_error)
    }

    pub fn get_max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        match self.try_get_max_offset_in_queue(topic, queue_id) {
            Ok(offset) => offset,
            Err(error) => {
                warn!(topic = %topic, queue_id, error = %error, "failed to read RocksDB max consume queue offset");
                0
            }
        }
    }

    pub fn try_get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> Result<i64, StoreError> {
        self.consume_queue_store
            .get_min_offset_in_queue(topic.to_string(), queue_id)
            .map_err(rocksdb_store_error)
    }

    pub fn get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        match self.try_get_min_offset_in_queue(topic, queue_id) {
            Ok(offset) => offset,
            Err(error) => {
                warn!(topic = %topic, queue_id, error = %error, "failed to read RocksDB min consume queue offset");
                0
            }
        }
    }

    pub fn try_get_commit_log_offset_in_queue(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> Result<i64, StoreError> {
        Ok(self
            .rocksdb_cq_value(topic, queue_id, consume_queue_offset)?
            .map_or(-1, |value| value.commit_log_physical_offset))
    }

    pub fn get_commit_log_offset_in_queue(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> i64 {
        match self.try_get_commit_log_offset_in_queue(topic, queue_id, consume_queue_offset) {
            Ok(offset) => offset,
            Err(error) => {
                warn!(
                    topic = %topic,
                    queue_id,
                    consume_queue_offset,
                    error = %error,
                    "failed to read RocksDB consume queue physical offset"
                );
                -1
            }
        }
    }

    pub fn try_get_message_store_timestamp(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> Result<i64, StoreError> {
        Ok(self
            .rocksdb_cq_value(topic, queue_id, consume_queue_offset)?
            .map_or(-1, |value| value.msg_store_time))
    }

    pub fn get_message_store_timestamp(&self, topic: &CheetahString, queue_id: i32, consume_queue_offset: i64) -> i64 {
        match self.try_get_message_store_timestamp(topic, queue_id, consume_queue_offset) {
            Ok(timestamp) => timestamp,
            Err(error) => {
                warn!(
                    topic = %topic,
                    queue_id,
                    consume_queue_offset,
                    error = %error,
                    "failed to read RocksDB consume queue store timestamp"
                );
                -1
            }
        }
    }

    pub async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Option<GetMessageResult> {
        self.get_message_with_size_limit(
            group,
            topic,
            queue_id,
            offset,
            max_msg_nums,
            MAX_PULL_MSG_SIZE,
            message_filter,
        )
        .await
    }

    pub async fn get_message_with_size_limit(
        &self,
        _group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Option<GetMessageResult> {
        let min_offset = self
            .try_get_min_offset_in_queue(topic, queue_id)
            .unwrap_or_else(|error| {
                warn!(topic = %topic, queue_id, error = %error, "failed to read RocksDB min offset for get_message");
                0
            });
        let max_offset = self
            .try_get_max_offset_in_queue(topic, queue_id)
            .unwrap_or_else(|error| {
                warn!(topic = %topic, queue_id, error = %error, "failed to read RocksDB max offset for get_message");
                0
            });

        let mut result = GetMessageResult::new();
        let mut status = GetMessageStatus::NoMessageInQueue;
        let mut next_begin_offset = offset;

        if max_offset == 0 {
            next_begin_offset = self.local_file_store.next_offset_correction(offset, 0);
        } else if offset < min_offset {
            status = GetMessageStatus::OffsetTooSmall;
            next_begin_offset = self.local_file_store.next_offset_correction(offset, min_offset);
        } else if offset == max_offset {
            status = GetMessageStatus::OffsetOverflowOne;
            next_begin_offset = self.local_file_store.next_offset_correction(offset, offset);
        } else if offset > max_offset {
            status = GetMessageStatus::OffsetOverflowBadly;
            next_begin_offset = self.local_file_store.next_offset_correction(offset, max_offset);
        } else {
            status = GetMessageStatus::NoMatchedMessage;
            let max_pull_size = max_total_msg_size.clamp(100, MAX_PULL_MSG_SIZE);
            let read_count = max_msg_nums.max(0);
            match self
                .consume_queue_store
                .range_query(topic.to_string(), queue_id, offset, read_count)
            {
                Ok(values) if values.is_empty() => {
                    status = GetMessageStatus::OffsetFoundNull;
                    next_begin_offset = self.local_file_store.next_offset_correction(offset, offset + 1);
                }
                Ok(values) => {
                    for (index, encoded_value) in values.into_iter().enumerate() {
                        if result.message_count() >= max_msg_nums || result.buffer_total_size() >= max_pull_size {
                            break;
                        }

                        let queue_offset = offset + index as i64;
                        next_begin_offset = queue_offset + 1;
                        let value = match ConsumeQueueValue::decode(encoded_value.as_ref()) {
                            Ok(value) => value,
                            Err(error) => {
                                warn!(topic = %topic, queue_id, queue_offset, error = %error, "failed to decode RocksDB CQ value");
                                status = GetMessageStatus::OffsetFoundNull;
                                break;
                            }
                        };

                        if let Some(filter) = message_filter.as_ref() {
                            if !filter.is_matched_by_consume_queue(Some(value.tag_hash_code), None) {
                                continue;
                            }
                        }

                        let Some(select_result) = self
                            .local_file_store
                            .get_commit_log()
                            .get_message(value.commit_log_physical_offset, value.body_size)
                        else {
                            if result.buffer_total_size() == 0 {
                                status = GetMessageStatus::MessageWasRemoving;
                            }
                            continue;
                        };

                        if let Some(filter) = message_filter.as_ref() {
                            if !filter.is_matched_by_commit_log(Some(select_result.get_buffer()), None) {
                                continue;
                            }
                        }

                        result.add_message(select_result, queue_offset as u64, 1);
                        status = GetMessageStatus::Found;
                    }
                }
                Err(error) => {
                    warn!(topic = %topic, queue_id, error = %error, "failed to range query RocksDB CQ for get_message");
                    return None;
                }
            }
        }

        result.set_status(Some(status));
        result.set_next_begin_offset(next_begin_offset);
        result.set_min_offset(min_offset);
        result.set_max_offset(max_offset);
        Some(result)
    }

    fn query_message_by_rocksdb_index(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Result<QueryMessageResult, StoreError> {
        let max_num = max_num
            .max(0)
            .min(self.get_message_store_config().max_msgs_num_batch as i32);
        if max_num == 0 {
            return Ok(QueryMessageResult::default());
        }
        let (begin, end) = normalize_rocksdb_index_query_time_range(
            begin,
            end,
            self.get_message_store_config().max_rocksdb_index_query_days,
        );

        let mut offsets = self
            .message_rocksdb_storage
            .query_offsets_for_index(
                topic.as_str(),
                MessageConst::INDEX_KEY_TYPE,
                key.as_str(),
                begin,
                end,
                max_num as usize,
            )
            .map_err(rocksdb_store_error)?;
        if offsets.is_empty() {
            offsets = self
                .message_rocksdb_storage
                .query_offsets_for_index(
                    topic.as_str(),
                    MessageConst::INDEX_UNIQUE_TYPE,
                    key.as_str(),
                    begin,
                    end,
                    max_num as usize,
                )
                .map_err(rocksdb_store_error)?;
        }
        offsets.sort_unstable();

        let mut result = QueryMessageResult {
            index_last_update_timestamp: self
                .message_rocksdb_storage
                .get_last_store_timestamp_for_index()
                .map_err(rocksdb_store_error)?,
            index_last_update_phyoffset: self
                .message_rocksdb_storage
                .get_last_offset_py(crate::rocksdb::column_family::RocksDbColumnFamily::Default.name())
                .map_err(rocksdb_store_error)?,
            ..QueryMessageResult::default()
        };

        for offset in offsets {
            if let Some(select_result) = self
                .local_file_store
                .get_commit_log()
                .get_data_with_option(offset, false)
            {
                result.add_message(select_result);
            } else {
                warn!(offset, "RocksDB index query returned unreadable message offset");
            }
        }
        Ok(result)
    }

    fn rocksdb_cq_value(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> Result<Option<ConsumeQueueValue>, StoreError> {
        self.consume_queue_store
            .get(topic.to_string(), queue_id, consume_queue_offset)
            .map_err(rocksdb_store_error)?
            .map(|value| ConsumeQueueValue::decode(value.as_ref()).map_err(rocksdb_store_error))
            .transpose()
    }
}

fn rocksdb_store_error(error: rocketmq_error::RocketMQError) -> StoreError {
    StoreError::RocksDb(error.to_string())
}

fn normalize_rocksdb_index_query_time_range(begin: i64, end: i64, max_query_days: usize) -> (i64, i64) {
    if begin > 0 && end > 0 && begin <= end && end != i64::MAX {
        return (begin, end);
    }
    let end = current_millis() as i64;
    let max_query_days = i64::try_from(max_query_days).unwrap_or(i64::MAX / MILLIS_PER_DAY);
    let begin = end.saturating_sub(max_query_days.saturating_mul(MILLIS_PER_DAY));
    (begin, end)
}

impl MessageStore for RocksDBMessageStore {
    async fn load(&mut self) -> bool {
        self.local_file_store.load().await
    }

    async fn start(&mut self) -> Result<(), StoreError> {
        self.local_file_store.start().await?;
        self.rocksdb_maintenance_service.start();
        self.message_rocksdb_maintenance_service.start();
        Ok(())
    }

    async fn init(&mut self) -> Result<(), StoreError> {
        self.local_file_store.init().await
    }

    async fn shutdown(&mut self) {
        if let Err(error) = self.rocksdb_maintenance_service.shutdown_gracefully().await {
            warn!(error = %error, "failed to shutdown RocksDB consume queue maintenance service");
        }
        if let Err(error) = self.message_rocksdb_maintenance_service.shutdown_gracefully().await {
            warn!(error = %error, "failed to shutdown RocksDB message maintenance service");
        }
        self.local_file_store.shutdown().await;
        self.close_rocksdb();
    }

    fn destroy(&mut self) {
        self.local_file_store.destroy();
        self.close_rocksdb();
    }

    async fn put_message(&mut self, msg: MessageExtBrokerInner) -> PutMessageResult {
        self.local_file_store.put_message(msg).await
    }

    async fn put_messages(&mut self, message_ext_batch: MessageExtBatch) -> PutMessageResult {
        self.local_file_store.put_messages(message_ext_batch).await
    }

    async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Option<GetMessageResult> {
        RocksDBMessageStore::get_message(self, group, topic, queue_id, offset, max_msg_nums, message_filter).await
    }

    async fn get_message_with_size_limit(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Option<GetMessageResult> {
        RocksDBMessageStore::get_message_with_size_limit(
            self,
            group,
            topic,
            queue_id,
            offset,
            max_msg_nums,
            max_total_msg_size,
            message_filter,
        )
        .await
    }

    fn get_max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        RocksDBMessageStore::get_max_offset_in_queue(self, topic, queue_id)
    }

    fn get_max_offset_in_queue_committed(&self, topic: &CheetahString, queue_id: i32, _committed: bool) -> i64 {
        RocksDBMessageStore::get_max_offset_in_queue(self, topic, queue_id)
    }

    fn get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        RocksDBMessageStore::get_min_offset_in_queue(self, topic, queue_id)
    }

    fn get_timer_message_store(&self) -> Option<&Arc<TimerMessageStore>> {
        self.local_file_store.get_timer_message_store()
    }

    fn set_timer_message_store(&mut self, timer_message_store: Arc<TimerMessageStore>) {
        self.local_file_store.set_timer_message_store(timer_message_store);
    }

    fn get_commit_log_offset_in_queue(&self, topic: &CheetahString, queue_id: i32, consume_queue_offset: i64) -> i64 {
        RocksDBMessageStore::get_commit_log_offset_in_queue(self, topic, queue_id, consume_queue_offset)
    }

    fn get_offset_in_queue_by_time(&self, topic: &CheetahString, queue_id: i32, timestamp: i64) -> i64 {
        self.local_file_store
            .get_offset_in_queue_by_time(topic, queue_id, timestamp)
    }

    fn get_offset_in_queue_by_time_with_boundary(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64 {
        self.local_file_store
            .get_offset_in_queue_by_time_with_boundary(topic, queue_id, timestamp, boundary_type)
    }

    async fn get_offset_in_queue_by_time_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
    ) -> Result<i64, StoreError> {
        self.local_file_store
            .get_offset_in_queue_by_time_async(topic, queue_id, timestamp)
            .await
    }

    async fn get_offset_in_queue_by_time_with_boundary_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> Result<i64, StoreError> {
        self.local_file_store
            .get_offset_in_queue_by_time_with_boundary_async(topic, queue_id, timestamp, boundary_type)
            .await
    }

    fn look_message_by_offset(&self, commit_log_offset: i64) -> Option<MessageExt> {
        self.local_file_store.look_message_by_offset(commit_log_offset)
    }

    fn look_message_by_offset_with_size(&self, commit_log_offset: i64, size: i32) -> Option<MessageExt> {
        self.local_file_store
            .look_message_by_offset_with_size(commit_log_offset, size)
    }

    fn select_one_message_by_offset(&self, commit_log_offset: i64) -> Option<SelectMappedBufferResult> {
        self.local_file_store.select_one_message_by_offset(commit_log_offset)
    }

    fn select_one_message_by_offset_with_size(
        &self,
        commit_log_offset: i64,
        msg_size: i32,
    ) -> Option<SelectMappedBufferResult> {
        self.local_file_store
            .select_one_message_by_offset_with_size(commit_log_offset, msg_size)
    }

    fn get_running_data_info(&self) -> String {
        self.local_file_store.get_running_data_info()
    }

    fn get_timing_message_count(&self, topic: &CheetahString) -> i64 {
        self.local_file_store.get_timing_message_count(topic)
    }

    fn get_runtime_info(&self) -> HashMap<String, String> {
        self.local_file_store.get_runtime_info()
    }

    fn get_max_phy_offset(&self) -> i64 {
        self.local_file_store.get_max_phy_offset()
    }

    fn get_min_phy_offset(&self) -> i64 {
        self.local_file_store.get_min_phy_offset()
    }

    fn get_earliest_message_time(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        let min_offset = RocksDBMessageStore::get_min_offset_in_queue(self, topic, queue_id);
        if RocksDBMessageStore::get_max_offset_in_queue(self, topic, queue_id) > min_offset {
            return RocksDBMessageStore::get_message_store_timestamp(self, topic, queue_id, min_offset);
        }
        -1
    }

    fn get_earliest_message_time_store(&self) -> i64 {
        self.local_file_store.get_earliest_message_time_store()
    }

    fn get_message_store_timestamp(&self, topic: &CheetahString, queue_id: i32, consume_queue_offset: i64) -> i64 {
        RocksDBMessageStore::get_message_store_timestamp(self, topic, queue_id, consume_queue_offset)
    }

    async fn get_message_store_timestamp_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> Result<i64, StoreError> {
        self.try_get_message_store_timestamp(topic, queue_id, consume_queue_offset)
    }

    fn get_message_total_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        let min_offset = RocksDBMessageStore::get_min_offset_in_queue(self, topic, queue_id);
        let max_offset = RocksDBMessageStore::get_max_offset_in_queue(self, topic, queue_id);
        max_offset.saturating_sub(min_offset)
    }

    fn get_commit_log_data(&self, offset: i64) -> Option<SelectMappedBufferResult> {
        self.local_file_store.get_commit_log_data(offset)
    }

    fn get_bulk_commit_log_data(&self, offset: i64, size: i32) -> Option<Vec<SelectMappedBufferResult>> {
        self.local_file_store.get_bulk_commit_log_data(offset, size)
    }

    async fn append_to_commit_log(
        &mut self,
        start_offset: i64,
        data: &[u8],
        data_start: i32,
        data_length: i32,
    ) -> Result<bool, StoreError> {
        self.local_file_store
            .append_to_commit_log(start_offset, data, data_start, data_length)
            .await
    }

    fn execute_delete_files_manually(&self) {
        self.local_file_store.execute_delete_files_manually();
    }

    async fn query_message(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Option<QueryMessageResult> {
        match self.query_message_by_rocksdb_index(topic, key, max_num, begin, end) {
            Ok(result) if result.buffer_total_size > 0 => Some(result),
            Ok(_) => {
                self.local_file_store
                    .query_message(topic, key, max_num, begin, end)
                    .await
            }
            Err(error) => {
                warn!(topic = %topic, key = %key, error = %error, "failed to query message by RocksDB index");
                self.local_file_store
                    .query_message(topic, key, max_num, begin, end)
                    .await
            }
        }
    }

    async fn update_ha_master_address(&self, new_addr: &str) {
        self.local_file_store.update_ha_master_address(new_addr).await;
    }

    fn update_master_address(&self, new_addr: &CheetahString) {
        self.local_file_store.update_master_address(new_addr);
    }

    fn slave_fall_behind_much(&self) -> i64 {
        self.local_file_store.slave_fall_behind_much()
    }

    fn delete_topics(&mut self, delete_topics: Vec<&CheetahString>) -> i32 {
        for topic in &delete_topics {
            if let Err(error) = self.consume_queue_store.destroy_topic(topic) {
                warn!(topic = %topic, error = %error, "failed to delete RocksDB consume queue topic state");
            }
        }
        self.local_file_store.delete_topics(delete_topics)
    }

    fn clean_unused_topic(&self, retain_topics: &HashSet<String>) -> i32 {
        self.local_file_store.clean_unused_topic(retain_topics)
    }

    fn clean_expired_consumer_queue(&self) {
        let min_phy_offset = self.local_file_store.get_min_phy_offset();
        if let Err(error) = self.consume_queue_store.clean_expired_background(min_phy_offset) {
            warn!(
                min_phy_offset,
                error = %error,
                "failed to schedule RocksDB consume queue manual compaction"
            );
        }
        self.local_file_store.clean_expired_consumer_queue();
    }

    fn check_in_mem_by_consume_offset(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_offset: i64,
        batch_size: i32,
    ) -> bool {
        self.local_file_store
            .check_in_mem_by_consume_offset(topic, queue_id, consume_offset, batch_size)
    }

    fn check_in_store_by_consume_offset(&self, topic: &CheetahString, queue_id: i32, consume_offset: i64) -> bool {
        self.rocksdb_cq_value(topic, queue_id, consume_offset)
            .is_ok_and(|value| value.is_some())
    }

    fn dispatch_behind_bytes(&self) -> i64 {
        self.local_file_store.dispatch_behind_bytes()
    }

    fn flush(&self) -> i64 {
        if let Err(error) = self.rocksdb_index_service.flush_pending() {
            warn!(error = %error, "failed to flush pending RocksDB index records");
        }
        if let Err(error) = self.rocksdb_store.flush() {
            warn!(error = %error, "failed to flush RocksDB message store");
        }
        if let Err(error) = self.message_rocksdb_storage.store().flush() {
            warn!(error = %error, "failed to flush RocksDB index/timer/trans store");
        }
        self.local_file_store.flush()
    }

    fn get_flushed_where(&self) -> i64 {
        self.local_file_store.get_flushed_where()
    }

    fn reset_write_offset(&self, phy_offset: i64) -> bool {
        self.local_file_store.reset_write_offset(phy_offset)
    }

    fn get_confirm_offset(&self) -> i64 {
        self.local_file_store.get_confirm_offset()
    }

    fn set_confirm_offset(&mut self, phy_offset: i64) {
        self.local_file_store.set_confirm_offset(phy_offset);
    }

    fn is_os_page_cache_busy(&self) -> bool {
        self.local_file_store.is_os_page_cache_busy()
    }

    fn lock_time_millis(&self) -> i64 {
        self.local_file_store.lock_time_millis()
    }

    fn is_transient_store_pool_deficient(&self) -> bool {
        self.local_file_store.is_transient_store_pool_deficient()
    }

    fn get_dispatcher_list(&self) -> &[Arc<dyn CommitLogDispatcher>] {
        self.local_file_store.get_dispatcher_list()
    }

    fn add_dispatcher(&mut self, dispatcher: Arc<dyn CommitLogDispatcher>) {
        self.local_file_store.add_dispatcher(dispatcher);
    }

    fn add_first_dispatcher(&mut self, dispatcher: Arc<dyn CommitLogDispatcher>) {
        self.local_file_store.add_first_dispatcher(dispatcher);
    }

    fn get_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> Option<ArcConsumeQueue> {
        self.local_file_store.get_consume_queue(topic, queue_id)
    }

    fn find_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> Option<ArcConsumeQueue> {
        self.local_file_store.find_consume_queue(topic, queue_id)
    }

    fn get_broker_stats_manager(&self) -> Option<&Arc<BrokerStatsManager>> {
        self.local_file_store.get_broker_stats_manager()
    }

    fn on_commit_log_append<MF: MappedFile>(
        &self,
        msg: &MessageExtBrokerInner,
        result: &AppendMessageResult,
        commit_log_file: &MF,
    ) {
        self.local_file_store.on_commit_log_append(msg, result, commit_log_file);
    }

    fn on_commit_log_dispatch<MF: MappedFile>(
        &self,
        dispatch_request: &DispatchRequest,
        do_dispatch: bool,
        commit_log_file: &MF,
        is_recover: bool,
        is_file_end: bool,
    ) -> Result<(), StoreError> {
        self.local_file_store.on_commit_log_dispatch(
            dispatch_request,
            do_dispatch,
            commit_log_file,
            is_recover,
            is_file_end,
        )
    }

    fn finish_commit_log_dispatch(&self) {
        self.local_file_store.finish_commit_log_dispatch();
    }

    fn get_message_store_config(&self) -> &MessageStoreConfig {
        self.local_file_store.message_store_config_ref()
    }

    fn get_store_stats_service(&self) -> Arc<StoreStatsService> {
        self.local_file_store.get_store_stats_service()
    }

    fn get_store_checkpoint(&self) -> &StoreCheckpoint {
        self.local_file_store.get_store_checkpoint()
    }

    fn get_store_checkpoint_arc(&self) -> Arc<StoreCheckpoint> {
        self.local_file_store.get_store_checkpoint_arc()
    }

    fn get_system_clock(&self) -> Arc<SystemClock> {
        self.local_file_store.get_system_clock()
    }

    fn get_commit_log(&self) -> &CommitLog {
        self.local_file_store.get_commit_log()
    }

    fn get_commit_log_mut_from_ref(&self) -> &mut CommitLog {
        self.local_file_store.get_commit_log_mut_from_ref()
    }

    fn get_commit_log_mut(&mut self) -> &mut CommitLog {
        self.local_file_store.get_commit_log_mut()
    }

    fn set_commitlog_read_mode(&mut self, read_ahead_mode: i32) -> Result<(), StoreError> {
        self.local_file_store.set_commitlog_read_mode(read_ahead_mode)
    }

    fn get_running_flags(&self) -> &RunningFlags {
        self.local_file_store.get_running_flags()
    }

    fn get_running_flags_arc(&self) -> Arc<RunningFlags> {
        self.local_file_store.get_running_flags_arc()
    }

    fn get_transient_store_pool(&self) -> Arc<TransientStorePool> {
        self.local_file_store.get_transient_store_pool()
    }

    fn get_allocate_mapped_file_service(&self) -> Arc<AllocateMappedFileService> {
        self.local_file_store.get_allocate_mapped_file_service()
    }

    fn truncate_dirty_logic_files(&self, phy_offset: i64) {
        if let Err(error) = self.consume_queue_store.truncate_dirty(phy_offset) {
            warn!(
                phy_offset,
                error = %error,
                "failed to truncate RocksDB consume queue dirty offsets"
            );
        }
        self.local_file_store.truncate_dirty_logic_files(phy_offset);
    }

    fn unlock_mapped_file<MF: MappedFile>(&self, unlock_mapped_file: &MF) {
        self.local_file_store.unlock_mapped_file(unlock_mapped_file);
    }

    fn get_queue_store(&self) -> &dyn Any {
        self.local_file_store.get_queue_store()
    }

    fn is_sync_disk_flush(&self) -> bool {
        self.local_file_store.is_sync_disk_flush()
    }

    fn is_sync_master(&self) -> bool {
        self.local_file_store.is_sync_master()
    }

    fn assign_offset(&self, msg: &mut MessageExtBrokerInner) -> Result<(), StoreError> {
        self.local_file_store.assign_offset(msg)
    }

    fn increase_offset(&self, msg: &MessageExtBrokerInner, message_num: i16) {
        self.local_file_store.increase_offset(msg, message_num);
    }

    fn get_master_store_in_process<M: MessageStore + Send + Sync + 'static>(&self) -> Option<Arc<M>> {
        self.local_file_store.get_master_store_in_process::<M>()
    }

    fn set_master_store_in_process<M: MessageStore + Send + Sync + 'static>(&self, master_store_in_process: Arc<M>) {
        self.local_file_store
            .set_master_store_in_process(master_store_in_process);
    }

    fn get_data(&self, offset: i64, size: i32, byte_buffer: &mut BytesMut) -> bool {
        self.local_file_store.get_data(offset, size, byte_buffer)
    }

    fn set_alive_replica_num_in_group(&self, alive_replica_nums: i32) {
        self.local_file_store.set_alive_replica_num_in_group(alive_replica_nums);
    }

    fn get_alive_replica_num_in_group(&self) -> i32 {
        self.local_file_store.get_alive_replica_num_in_group()
    }

    fn sync_controller_sync_state_set(&self, local_broker_id: i64, sync_state_set: &HashSet<i64>) {
        self.local_file_store
            .sync_controller_sync_state_set(local_broker_id, sync_state_set);
    }

    fn wakeup_ha_client(&self) {
        self.local_file_store.wakeup_ha_client();
    }

    fn get_master_flushed_offset(&self) -> i64 {
        self.local_file_store.get_master_flushed_offset()
    }

    fn get_broker_init_max_offset(&self) -> i64 {
        self.local_file_store.get_broker_init_max_offset()
    }

    fn set_master_flushed_offset(&self, master_flushed_offset: i64) {
        self.local_file_store.set_master_flushed_offset(master_flushed_offset);
    }

    fn set_broker_init_max_offset(&mut self, broker_init_max_offset: i64) {
        self.local_file_store.set_broker_init_max_offset(broker_init_max_offset);
    }

    fn sync_broker_role(&mut self, broker_role: BrokerRole) {
        self.local_file_store.sync_broker_role(broker_role);
    }

    fn calc_delta_checksum(&self, from: i64, to: i64) -> Vec<u8> {
        self.local_file_store.calc_delta_checksum(from, to)
    }

    fn truncate_files(&self, offset_to_truncate: i64) -> Result<bool, StoreError> {
        self.local_file_store.truncate_files(offset_to_truncate)
    }

    fn is_offset_aligned(&self, offset: i64) -> bool {
        self.local_file_store.is_offset_aligned(offset)
    }

    fn get_put_message_hook_list(&self) -> Vec<Arc<dyn PutMessageHook>> {
        self.local_file_store.get_put_message_hook_list()
    }

    fn set_send_message_back_hook(&self, send_message_back_hook: Arc<dyn SendMessageBackHook>) {
        self.local_file_store.set_send_message_back_hook(send_message_back_hook);
    }

    fn get_send_message_back_hook(&self) -> Option<Arc<dyn SendMessageBackHook>> {
        self.local_file_store.get_send_message_back_hook()
    }

    fn get_last_file_from_offset(&self) -> i64 {
        self.local_file_store.get_last_file_from_offset()
    }

    fn get_last_mapped_file(&self, start_offset: i64) -> bool {
        self.local_file_store.get_last_mapped_file(start_offset)
    }

    fn set_physical_offset(&self, phy_offset: i64) {
        self.local_file_store.set_physical_offset(phy_offset);
    }

    fn is_mapped_files_empty(&self) -> bool {
        self.local_file_store.is_mapped_files_empty()
    }

    fn get_state_machine_version(&self) -> i64 {
        self.local_file_store.get_state_machine_version()
    }

    fn check_message_and_return_size(
        &self,
        bytes: &mut Bytes,
        check_crc: bool,
        check_dup_info: bool,
        read_body: bool,
    ) -> DispatchRequest {
        self.local_file_store
            .check_message_and_return_size(bytes, check_crc, check_dup_info, read_body)
    }

    fn remain_transient_store_buffer_numbs(&self) -> i32 {
        self.local_file_store.remain_transient_store_buffer_numbs()
    }

    fn remain_how_many_data_to_commit(&self) -> i64 {
        self.local_file_store.remain_how_many_data_to_commit()
    }

    fn remain_how_many_data_to_flush(&self) -> i64 {
        self.local_file_store.remain_how_many_data_to_flush()
    }

    fn is_shutdown(&self) -> bool {
        self.local_file_store.is_shutdown()
    }

    fn estimate_message_count(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        from: i64,
        to: i64,
        filter: &dyn MessageFilter,
    ) -> i64 {
        self.local_file_store
            .estimate_message_count(topic, queue_id, from, to, filter)
    }

    fn recover_topic_queue_table(&mut self) {
        self.local_file_store.recover_topic_queue_table();
    }

    fn notify_message_arrive_if_necessary(&self, dispatch_request: &mut DispatchRequest) {
        self.local_file_store
            .notify_message_arrive_if_necessary(dispatch_request);
    }

    fn set_put_message_hook(&mut self, put_message_hook: BoxedPutMessageHook) {
        self.local_file_store.set_put_message_hook(put_message_hook);
    }

    fn get_ha_service(&self) -> Option<&GeneralHAService> {
        self.local_file_store.get_ha_service()
    }

    fn get_ha_runtime_info(&self) -> Option<HARuntimeInfo> {
        self.local_file_store.get_ha_runtime_info()
    }
}

impl Deref for RocksDBMessageStore {
    type Target = LocalFileMessageStore;

    fn deref(&self) -> &Self::Target {
        self.local_file_store.as_ref()
    }
}

impl DerefMut for RocksDBMessageStore {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.local_file_store.as_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_rocksdb_index_query_time_range_uses_configured_query_days() {
        let before = current_millis() as i64;
        let (begin, end) = normalize_rocksdb_index_query_time_range(0, i64::MAX, 2);
        let after = current_millis() as i64;

        assert!(end >= before);
        assert!(end <= after);
        assert_eq!(end - begin, 2 * 24 * 60 * 60 * 1000);
    }
}
