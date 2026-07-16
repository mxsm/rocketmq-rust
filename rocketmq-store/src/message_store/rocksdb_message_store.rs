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
use rocketmq_common::common::system_clock::SystemClock;
use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_rust::ArcMut;
use rocketmq_store_api::GetStatus as ApiGetStatus;
use rocketmq_store_local::commit_log::read::LocalWalPort;
use rocketmq_store_rocksdb::message_store::RocksDbDerivedStore;
use rocketmq_store_rocksdb::message_store::RocksDbMessageStoreError;
use rocketmq_store_rocksdb::message_store::RocksDbMessageStoreOptions;
use rocketmq_store_rocksdb::message_store::RocksDbMessageStoreRoot;
use rocketmq_store_rocksdb::message_store::RocksDbReadRequest;
use rocketmq_store_rocksdb::message_store::RocksDbTimeBoundary;
use tracing::warn;

use crate::base::allocate_mapped_file_service::AllocateMappedFileService;
use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::base::get_message_result::GetMessageResult;
use crate::base::message_result::AppendMessageResult;
use crate::base::message_result::PutMessageResult;
use crate::base::message_status_enum::GetMessageStatus;
use crate::base::message_store::MessageStore;
use crate::base::message_store::MessageStoreShutdownReport;
use crate::base::message_store::StoreHealthSnapshot;
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
use crate::rocksdb::index::RocksDbIndexBuildService;
use crate::rocksdb::message::MessageRocksDbStorage;
use crate::rocksdb::store::RocksDbStore;
use crate::rocksdb::timer::CommitLogDispatcherBuildRocksDbTimer;
use crate::rocksdb::timer::RocksDbTimerBuildService;
use crate::rocksdb::transaction::CommitLogDispatcherBuildRocksDbTrans;
use crate::rocksdb::transaction::RocksDbTransBuildService;
use crate::rocksdb::value::ConsumeQueueValue;
use crate::stats::broker_stats_manager::BrokerStatsManager;
use crate::store::running_flags::RunningFlags;
use crate::store_error::StoreError;
use crate::timer::timer_message_store::TimerMessageStore;

/// RocksDB-backed message store boundary.
///
/// The current implementation still reuses the local-file commit log path, but
/// consume queue metadata is opened through the RocksDB foundation layer instead
/// of being a type alias to `LocalFileMessageStore`. This keeps the later broker
/// wiring and commit-log dispatcher migration anchored to a concrete RocksDB
/// owner.
pub struct RocksDBMessageStore {
    local_file_store: ArcMut<LocalFileMessageStore>,
    root: RocksDbMessageStoreRoot,
}

struct StoreLocalWalAdapter<'a> {
    commit_log: &'a CommitLog,
    correct_to_new_offset: bool,
}

impl LocalWalPort for StoreLocalWalAdapter<'_> {
    type Selection = SelectMappedBufferResult;

    fn read_message(&self, offset: i64, size: i32) -> Result<Option<Self::Selection>, rocketmq_store_api::StoreError> {
        Ok(self.commit_log.get_message(offset, size))
    }

    fn read_from(&self, offset: i64) -> Result<Option<Self::Selection>, rocketmq_store_api::StoreError> {
        Ok(self.commit_log.get_data_with_option(offset, false))
    }

    fn selection_bytes<'a>(&self, selection: &'a Self::Selection) -> &'a [u8] {
        selection.get_buffer()
    }

    fn correct_queue_offset(&self, old_offset: i64, new_offset: i64) -> i64 {
        if self.correct_to_new_offset {
            new_offset
        } else {
            old_offset
        }
    }
}

impl fmt::Debug for RocksDBMessageStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RocksDBMessageStore")
            .field("rocksdb_config", self.root.derived().rocksdb_config())
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
        let message_store_config_for_index = Arc::clone(&message_store_config);
        let message_store_config_for_timer = Arc::clone(&message_store_config);
        let message_store_config_for_trans = Arc::clone(&message_store_config);
        let derived = RocksDbDerivedStore::open(
            message_store_config.as_ref(),
            RocksDbMessageStoreOptions {
                timer_enabled: message_store_config.timer_rocksdb_enable,
                transaction_enabled: message_store_config.trans_rocksdb_enable,
            },
        )
        .map_err(message_store_adapter_error)?;
        let consume_queue_store = derived.consume_queue_store().clone();
        let rocksdb_index_service = derived.rocksdb_index_service();
        let rocksdb_timer_service = derived.rocksdb_timer_service();
        let rocksdb_trans_service = derived.rocksdb_trans_service();
        let mut local_file_store = ArcMut::new(LocalFileMessageStore::try_new(
            Arc::clone(&message_store_config),
            broker_config,
            topic_config_table,
            broker_stats_manager,
            notify_message_arrive_in_batch,
        )?);
        let local_file_store_clone = local_file_store.clone();
        local_file_store.set_message_store_arc(local_file_store_clone);
        let local_queue_offsets = local_file_store.consume_queue_store_mut().clone();
        local_file_store.add_dispatcher(Arc::new(
            CommitLogDispatcherBuildRocksDbConsumeQueue::new_with_local_queue_offsets(
                consume_queue_store.clone(),
                local_queue_offsets,
            ),
        ));
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

        let root = RocksDbMessageStoreRoot::new(derived);
        Ok(Self { local_file_store, root })
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

    fn local_wal_adapter(&self) -> StoreLocalWalAdapter<'_> {
        StoreLocalWalAdapter {
            commit_log: self.local_file_store.get_commit_log(),
            correct_to_new_offset: self.local_file_store.next_offset_correction(0, 1) == 1,
        }
    }

    pub fn rocksdb_config(&self) -> &RocksDbConfig {
        self.root.derived().rocksdb_config()
    }

    pub fn message_rocksdb_config(&self) -> &RocksDbConfig {
        self.root.derived().message_rocksdb_config()
    }

    pub fn rocksdb_store(&self) -> Arc<RocksDbStore> {
        self.root.derived().rocksdb_store()
    }

    pub fn message_rocksdb_storage(&self) -> Arc<MessageRocksDbStorage> {
        self.root.derived().message_rocksdb_storage()
    }

    pub fn rocksdb_index_service(&self) -> Arc<RocksDbIndexBuildService> {
        self.root.derived().rocksdb_index_service()
    }

    pub fn rocksdb_timer_service(&self) -> Option<Arc<RocksDbTimerBuildService>> {
        self.root.derived().rocksdb_timer_service()
    }

    pub fn rocksdb_trans_service(&self) -> Option<Arc<RocksDbTransBuildService>> {
        self.root.derived().rocksdb_trans_service()
    }

    pub fn is_rocksdb_maintenance_running(&self) -> bool {
        self.root.derived().is_rocksdb_maintenance_running()
    }

    pub fn is_message_rocksdb_maintenance_running(&self) -> bool {
        self.root.derived().is_message_rocksdb_maintenance_running()
    }

    pub fn close_rocksdb(&self) {
        self.root.derived().close();
    }

    pub fn consume_queue_store(&self) -> &RocksDbConsumeQueueStore {
        self.root.derived().consume_queue_store()
    }

    pub fn try_get_max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> Result<i64, StoreError> {
        self.root
            .derived()
            .max_offset(topic.as_str(), queue_id)
            .map_err(message_store_adapter_error)
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
        self.root
            .derived()
            .min_offset(topic.as_str(), queue_id)
            .map_err(message_store_adapter_error)
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
        let local_wal = self.local_wal_adapter();
        let read_result = self.root.read(
            &local_wal,
            RocksDbReadRequest {
                topic: topic.as_str(),
                queue_id,
                offset,
                max_message_count: max_msg_nums,
                max_total_message_size: max_total_msg_size,
                max_pull_message_size: MAX_PULL_MSG_SIZE,
            },
            |tags_code| {
                message_filter
                    .as_ref()
                    .is_none_or(|filter| filter.is_matched_by_consume_queue(Some(tags_code), None))
            },
            |bytes| {
                message_filter
                    .as_ref()
                    .is_none_or(|filter| filter.is_matched_by_commit_log(Some(bytes), None))
            },
        );
        let read_result = match read_result {
            Ok(result) => result,
            Err(error) => {
                warn!(topic = %topic, queue_id, error = %error, "failed to read message through RocksDB adapter");
                return None;
            }
        };
        let mut result = GetMessageResult::new();
        for record in read_result.records {
            result.add_message(record.selection, record.queue_offset, record.batch_count);
        }
        result.set_status(Some(legacy_get_status(read_result.status)));
        result.set_next_begin_offset(read_result.next_begin_offset);
        result.set_min_offset(read_result.min_offset);
        result.set_max_offset(read_result.max_offset);
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
        let local_wal = self.local_wal_adapter();
        let lookup = self
            .root
            .query_index(
                &local_wal,
                topic.as_str(),
                key.as_str(),
                max_num as usize,
                begin,
                end,
                self.get_message_store_config().max_rocksdb_index_query_days,
            )
            .map_err(message_store_adapter_error)?;
        let mut result = QueryMessageResult {
            index_last_update_timestamp: lookup.last_update_timestamp,
            index_last_update_phyoffset: lookup.last_update_physical_offset,
            ..QueryMessageResult::default()
        };
        for record in lookup.records {
            result.add_message(record);
        }
        Ok(result)
    }

    fn rocksdb_cq_value(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> Result<Option<ConsumeQueueValue>, StoreError> {
        self.root
            .derived()
            .consume_queue_value(topic.as_str(), queue_id, consume_queue_offset)
            .map_err(message_store_adapter_error)
    }

    fn sync_local_topic_queue_offsets(&self) -> Result<(), StoreError> {
        let topic_queue_table = self
            .root
            .derived()
            .topic_queue_offsets()
            .map_err(message_store_adapter_error)?
            .into_iter()
            .map(|((topic, queue_id), offset)| (CheetahString::from_string(format!("{topic}-{queue_id}")), offset))
            .collect();
        self.local_file_store.replace_topic_queue_table(topic_queue_table);
        Ok(())
    }
}

fn legacy_get_status(status: ApiGetStatus) -> GetMessageStatus {
    match status {
        ApiGetStatus::Found => GetMessageStatus::Found,
        ApiGetStatus::NoMatchedMessage => GetMessageStatus::NoMatchedMessage,
        ApiGetStatus::MessageWasRemoving => GetMessageStatus::MessageWasRemoving,
        ApiGetStatus::OffsetFoundNull => GetMessageStatus::OffsetFoundNull,
        ApiGetStatus::OffsetOverflowBadly => GetMessageStatus::OffsetOverflowBadly,
        ApiGetStatus::OffsetOverflowOne => GetMessageStatus::OffsetOverflowOne,
        ApiGetStatus::OffsetTooSmall => GetMessageStatus::OffsetTooSmall,
        ApiGetStatus::NoMatchedLogicQueue => GetMessageStatus::NoMatchedLogicQueue,
        ApiGetStatus::NoMessageInQueue => GetMessageStatus::NoMessageInQueue,
        ApiGetStatus::OffsetReset => GetMessageStatus::OffsetReset,
    }
}

fn message_store_adapter_error(error: RocksDbMessageStoreError) -> StoreError {
    match error {
        RocksDbMessageStoreError::Config(message) => StoreError::Config(message),
        RocksDbMessageStoreError::Backend { source } => StoreError::rocksdb(source),
        RocksDbMessageStoreError::Local { source } => StoreError::Storage(source.to_string()),
    }
}

impl MessageStore for RocksDBMessageStore {
    async fn load(&mut self) -> bool {
        if !self.local_file_store.load().await {
            return false;
        }
        if let Err(error) = self.sync_local_topic_queue_offsets() {
            warn!(error = %error, "failed to restore Local queue-offset table from RocksDB");
            return false;
        }
        true
    }

    async fn start(&mut self) -> Result<(), StoreError> {
        self.local_file_store.start().await?;
        self.root.derived_mut().start_maintenance();
        Ok(())
    }

    async fn init(&mut self) -> Result<(), StoreError> {
        self.local_file_store.init().await
    }

    async fn shutdown_gracefully(&mut self) -> Result<MessageStoreShutdownReport, StoreError> {
        let derived_maintenance_result = self.root.derived_mut().shutdown_maintenance().await;
        let local_file_result = self.local_file_store.shutdown_gracefully().await;
        self.close_rocksdb();

        let report = local_file_result?;
        derived_maintenance_result.map_err(message_store_adapter_error)?;
        Ok(report)
    }

    async fn shutdown(&mut self) {
        if let Err(error) = self.shutdown_gracefully().await {
            warn!(error = %error, "RocksDB message store shutdown failed");
        }
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
        match self
            .root
            .derived()
            .offset_by_time(topic.as_str(), queue_id, timestamp, RocksDbTimeBoundary::Lower)
        {
            Ok(offset) => offset,
            Err(error) => {
                warn!(topic = %topic, queue_id, timestamp, error = %error, "failed to seek RocksDB consume queue by time");
                0
            }
        }
    }

    fn get_offset_in_queue_by_time_with_boundary(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64 {
        let boundary = match boundary_type {
            BoundaryType::Lower => RocksDbTimeBoundary::Lower,
            BoundaryType::Upper => RocksDbTimeBoundary::Upper,
        };
        match self
            .root
            .derived()
            .offset_by_time(topic.as_str(), queue_id, timestamp, boundary)
        {
            Ok(offset) => offset,
            Err(error) => {
                warn!(topic = %topic, queue_id, timestamp, error = %error, "failed to seek RocksDB consume queue by time boundary");
                0
            }
        }
    }

    async fn get_offset_in_queue_by_time_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
    ) -> Result<i64, StoreError> {
        self.root
            .derived()
            .offset_by_time(topic.as_str(), queue_id, timestamp, RocksDbTimeBoundary::Lower)
            .map_err(message_store_adapter_error)
    }

    async fn get_offset_in_queue_by_time_with_boundary_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> Result<i64, StoreError> {
        let boundary = match boundary_type {
            BoundaryType::Lower => RocksDbTimeBoundary::Lower,
            BoundaryType::Upper => RocksDbTimeBoundary::Upper,
        };
        self.root
            .derived()
            .offset_by_time(topic.as_str(), queue_id, timestamp, boundary)
            .map_err(message_store_adapter_error)
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
            if let Err(error) = self.root.derived().delete_topic(topic.as_str()) {
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
        if let Err(error) = self.root.derived().clean_expired(min_phy_offset) {
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
        match self.try_flush() {
            Ok(progress) => progress.durable,
            Err(error) => {
                warn!(error = %error, "RocksDB message store flush failed; returning last durable watermark");
                self.local_file_store.get_flushed_where()
            }
        }
    }

    fn try_flush(&self) -> Result<crate::consume_queue::mapped_file_queue::FlushProgress, StoreError> {
        if let Err(error) = self.root.derived().flush_derived() {
            let error = message_store_adapter_error(error);
            self.local_file_store.record_flush_failure(&error);
            return Err(error);
        }
        self.local_file_store.try_flush()
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

    fn sync_flush_runtime_info(&self) -> crate::base::flush_manager::SyncFlushRuntimeInfo {
        self.local_file_store.sync_flush_runtime_info()
    }

    fn health_snapshot(&self) -> StoreHealthSnapshot {
        self.local_file_store.health_snapshot()
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
        let truncate_result = self.root.derived().truncate_dirty(phy_offset);
        if let Err(error) = truncate_result {
            warn!(
                phy_offset,
                error = %error,
                "failed to truncate RocksDB consume queue dirty offsets"
            );
        } else if let Err(error) = self.sync_local_topic_queue_offsets() {
            warn!(phy_offset, error = %error, "failed to restore queue offsets after RocksDB truncation");
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
