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

//! Compatibility bridge from the legacy message-store facade to narrow capabilities.

use std::future::Future;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_store_api::AppendReceipt;
use rocketmq_store_api::AppendReceiptError;
use rocketmq_store_api::AppendStatus;
use rocketmq_store_api::Durability;
use rocketmq_store_api::FlushBacklog as ApiFlushBacklog;
use rocketmq_store_api::GetResult;
use rocketmq_store_api::GetStatus;
use rocketmq_store_api::LeasedBytes;
use rocketmq_store_api::MessageAppender;
use rocketmq_store_api::MessageReader;
use rocketmq_store_api::QueryResult;
use rocketmq_store_api::ReadCacheState;
use rocketmq_store_api::SelectResult;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreErrorKind as ApiStoreErrorKind;
use rocketmq_store_api::StoreHealth;
use rocketmq_store_api::StoreHealthSnapshot as ApiStoreHealthSnapshot;

use crate::base::get_message_result::GetMessageResult;
use crate::base::message_result::PutMessageResult;
use crate::base::message_status_enum::GetMessageStatus;
use crate::base::message_status_enum::PutMessageStatus;
use crate::base::message_store::MessageStore;
use crate::base::query_message_result::QueryMessageResult;
use crate::base::select_result::SelectMappedBufferCacheState;
use crate::base::select_result::SelectMappedBufferResult;
use crate::store_error::StoreErrorKind;

/// Legacy append output plus independent append and durable progress.
#[derive(Clone)]
pub struct LegacyAppendReceipt {
    result: PutMessageResult,
    canonical: Result<AppendReceipt, AppendReceiptError>,
    appended_watermark: i64,
    durable_watermark: i64,
}

impl LegacyAppendReceipt {
    /// Returns the unchanged legacy append result.
    pub const fn result(&self) -> &PutMessageResult {
        &self.result
    }

    /// Returns the canonical backend-neutral receipt projection.
    pub fn canonical(&self) -> Result<&AppendReceipt, &AppendReceiptError> {
        self.canonical.as_ref()
    }

    /// Returns the exclusive primary-log append watermark observed after the operation.
    pub const fn appended_watermark(&self) -> i64 {
        self.appended_watermark
    }

    /// Returns the exclusive durable watermark observed after the operation.
    pub const fn durable_watermark(&self) -> i64 {
        self.durable_watermark
    }
}

/// Builds the legacy receipt without interpreting or rewriting its status/output fields.
pub fn legacy_append_receipt(
    result: PutMessageResult,
    appended_watermark: i64,
    durable_watermark: i64,
) -> LegacyAppendReceipt {
    let status = legacy_put_status_to_append_status(result.put_message_status());
    let canonical = match result.append_message_result() {
        Some(append) => {
            let start = append.wrote_offset;
            let end = start.saturating_add(i64::from(append.wrote_bytes));
            let durability = if durable_watermark >= end {
                Durability::Local
            } else {
                Durability::Memory
            };
            AppendReceipt::try_new(status, start..end, appended_watermark, durable_watermark, durability)
        }
        None => AppendReceipt::try_rejected(status, appended_watermark, durable_watermark),
    };
    LegacyAppendReceipt {
        result,
        canonical,
        appended_watermark,
        durable_watermark,
    }
}

/// Maps every legacy append outcome to a distinct neutral status.
pub const fn legacy_put_status_to_append_status(status: PutMessageStatus) -> AppendStatus {
    match status {
        PutMessageStatus::PutOk => AppendStatus::PutOk,
        PutMessageStatus::FlushDiskTimeout => AppendStatus::FlushDiskTimeout,
        PutMessageStatus::FlushSlaveTimeout => AppendStatus::FlushReplicaTimeout,
        PutMessageStatus::SlaveNotAvailable => AppendStatus::ReplicaUnavailable,
        PutMessageStatus::ServiceNotAvailable => AppendStatus::ServiceUnavailable,
        PutMessageStatus::CreateMappedFileFailed => AppendStatus::StorageUnavailable,
        PutMessageStatus::MessageIllegal => AppendStatus::InvalidMessage,
        PutMessageStatus::PropertiesSizeExceeded => AppendStatus::PropertiesTooLarge,
        PutMessageStatus::OsPageCacheBusy => AppendStatus::PageCacheBusy,
        PutMessageStatus::UnknownError => AppendStatus::Unknown,
        PutMessageStatus::InSyncReplicasNotEnough => AppendStatus::InsufficientReplicas,
        PutMessageStatus::PutToRemoteBrokerFail => AppendStatus::RemoteAppendFailed,
        PutMessageStatus::LmqConsumeQueueNumExceeded => AppendStatus::QueueLimitExceeded,
        PutMessageStatus::WheelTimerFlowControl => AppendStatus::ScheduleFlowControl,
        PutMessageStatus::WheelTimerMsgIllegal => AppendStatus::ScheduleMessageIllegal,
        PutMessageStatus::WheelTimerNotEnable => AppendStatus::ScheduleDisabled,
    }
}

/// Maps exact legacy failure vocabulary to closed backend-neutral categories.
pub const fn legacy_error_kind_to_api(kind: StoreErrorKind) -> ApiStoreErrorKind {
    match kind {
        StoreErrorKind::NotStarted => ApiStoreErrorKind::NotStarted,
        StoreErrorKind::MessageNotFound | StoreErrorKind::MappedFileNotFound => ApiStoreErrorKind::NotFound,
        StoreErrorKind::Config => ApiStoreErrorKind::InvalidRequest,
        StoreErrorKind::Unsupported => ApiStoreErrorKind::Unsupported,
        StoreErrorKind::MappedFile | StoreErrorKind::RocksDb | StoreErrorKind::Storage => ApiStoreErrorKind::Storage,
        StoreErrorKind::TieredStore | StoreErrorKind::Ha | StoreErrorKind::DLedger => ApiStoreErrorKind::Unavailable,
        StoreErrorKind::InvalidState => ApiStoreErrorKind::Internal,
    }
}

/// Maps every legacy get outcome without changing its semantics.
pub const fn legacy_get_status_to_api(status: GetMessageStatus) -> GetStatus {
    match status {
        GetMessageStatus::Found => GetStatus::Found,
        GetMessageStatus::NoMatchedMessage => GetStatus::NoMatchedMessage,
        GetMessageStatus::MessageWasRemoving => GetStatus::MessageWasRemoving,
        GetMessageStatus::OffsetFoundNull => GetStatus::OffsetFoundNull,
        GetMessageStatus::OffsetOverflowBadly => GetStatus::OffsetOverflowBadly,
        GetMessageStatus::OffsetOverflowOne => GetStatus::OffsetOverflowOne,
        GetMessageStatus::OffsetTooSmall => GetStatus::OffsetTooSmall,
        GetMessageStatus::NoMatchedLogicQueue => GetStatus::NoMatchedLogicQueue,
        GetMessageStatus::NoMessageInQueue => GetStatus::NoMessageInQueue,
        GetMessageStatus::OffsetReset => GetStatus::OffsetReset,
    }
}

/// Exact legacy health error retained only by the compatibility projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LegacyStoreHealthError {
    kind: StoreErrorKind,
}

impl LegacyStoreHealthError {
    /// Creates an exact compatibility projection from the legacy kind.
    pub const fn new(kind: StoreErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the original low-cardinality compatibility token.
    pub const fn compatibility_token(self) -> &'static str {
        self.kind.as_str()
    }
}

/// Compact sync-flush pressure retained by the compatibility projection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LegacyFlushBacklog {
    pub queue_depth: u64,
    pub oldest_wait_millis: u64,
}

/// Exact health data consumed by the existing Broker admission behavior.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyStoreHealthSnapshot {
    pub writable: bool,
    pub last_error: Option<LegacyStoreHealthError>,
    pub page_cache_busy: bool,
    pub transient_pool_deficient: bool,
    pub flush_backlog: LegacyFlushBacklog,
    pub dispatch_behind_bytes: i64,
    pub shutdown: bool,
    pub replication_pending_count: u64,
    pub replication_oldest_wait_millis: u64,
    pub appended_watermark: i64,
    pub durable_watermark: i64,
}

impl Default for LegacyStoreHealthSnapshot {
    fn default() -> Self {
        Self {
            writable: true,
            last_error: None,
            page_cache_busy: false,
            transient_pool_deficient: false,
            flush_backlog: LegacyFlushBacklog::default(),
            dispatch_behind_bytes: 0,
            shutdown: false,
            replication_pending_count: 0,
            replication_oldest_wait_millis: 0,
            appended_watermark: 0,
            durable_watermark: 0,
        }
    }
}

impl LegacyStoreHealthSnapshot {
    /// Returns the backend-neutral health result while retaining exact legacy data in this value.
    pub fn canonical(&self) -> ApiStoreHealthSnapshot {
        ApiStoreHealthSnapshot {
            writable: self.writable,
            last_error: self.last_error.map(|error| legacy_error_kind_to_api(error.kind)),
            page_cache_busy: self.page_cache_busy,
            transient_pool_deficient: self.transient_pool_deficient,
            flush_backlog: ApiFlushBacklog {
                queue_depth: self.flush_backlog.queue_depth,
                oldest_wait_millis: self.flush_backlog.oldest_wait_millis,
            },
            dispatch_behind_bytes: self.dispatch_behind_bytes,
            shutdown: self.shutdown,
            replication_pending_count: self.replication_pending_count,
            replication_oldest_wait_millis: self.replication_oldest_wait_millis,
            appended_watermark: self.appended_watermark,
            durable_watermark: self.durable_watermark,
        }
    }
}

/// Borrowing adapter that preserves the legacy [`MessageStore`] implementation.
pub struct LegacyMessageStoreAdapter<'a, MS> {
    store: &'a mut MS,
}

impl<'a, MS> LegacyMessageStoreAdapter<'a, MS> {
    /// Wraps an existing store without changing ownership or lifecycle.
    pub fn new(store: &'a mut MS) -> Self {
        Self { store }
    }
}

/// Read-only borrowing adapter for admission paths that only need store health.
pub struct LegacyMessageStoreHealthAdapter<'a, MS> {
    store: &'a MS,
}

impl<'a, MS> LegacyMessageStoreHealthAdapter<'a, MS> {
    /// Wraps an immutable store view without changing ownership.
    pub fn new(store: &'a MS) -> Self {
        Self { store }
    }
}

/// Adapter-local narrow port for the legacy read methods used by [`MessageReader`].
///
/// Filtered reads remain on the unchanged legacy trait. This canonical read port intentionally
/// forwards only unfiltered reads, so its hot request path has no dynamic filter allocation.
/// Test doubles can implement these four operations without copying `MessageStore`.
pub trait LegacyReadCallBoundary: Sync {
    fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_messages: i32,
    ) -> impl Future<Output = Option<GetMessageResult>> + Send;

    fn get_message_with_size_limit(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_messages: i32,
        max_total_size: i32,
    ) -> impl Future<Output = Option<GetMessageResult>> + Send;

    fn query_message(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_messages: i32,
        begin: i64,
        end: i64,
    ) -> impl Future<Output = Option<QueryMessageResult>> + Send;

    fn select_message(&self, physical_offset: i64, size: Option<i32>) -> Option<SelectMappedBufferResult>;
}

impl<MS: MessageStore> LegacyReadCallBoundary for MS {
    async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_messages: i32,
    ) -> Option<GetMessageResult> {
        MessageStore::get_message(self, group, topic, queue_id, offset, max_messages, None).await
    }

    async fn get_message_with_size_limit(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_messages: i32,
        max_total_size: i32,
    ) -> Option<GetMessageResult> {
        MessageStore::get_message_with_size_limit(
            self,
            group,
            topic,
            queue_id,
            offset,
            max_messages,
            max_total_size,
            None,
        )
        .await
    }

    async fn query_message(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_messages: i32,
        begin: i64,
        end: i64,
    ) -> Option<QueryMessageResult> {
        MessageStore::query_message(self, topic, key, max_messages, begin, end).await
    }

    fn select_message(&self, physical_offset: i64, size: Option<i32>) -> Option<SelectMappedBufferResult> {
        match size {
            Some(size) => MessageStore::select_one_message_by_offset_with_size(self, physical_offset, size),
            None => MessageStore::select_one_message_by_offset(self, physical_offset),
        }
    }
}

/// Read-only compatibility adapter that forwards legacy reads through [`MessageReader`].
pub struct LegacyMessageStoreReadAdapter<'a, MS> {
    store: &'a MS,
}

impl<'a, MS> LegacyMessageStoreReadAdapter<'a, MS> {
    /// Wraps an immutable store view without changing ownership or adding legacy methods.
    pub const fn new(store: &'a MS) -> Self {
        Self { store }
    }
}

/// Legacy read calls represented as one closed compatibility request enum.
pub enum LegacyReadRequest {
    Get {
        group: CheetahString,
        topic: CheetahString,
        queue_id: i32,
        offset: i64,
        max_messages: i32,
        max_total_size: Option<i32>,
    },
    Query {
        topic: CheetahString,
        key: CheetahString,
        max_messages: i32,
        begin: i64,
        end: i64,
    },
    Select {
        physical_offset: i64,
        size: Option<i32>,
    },
}

/// Neutral result variants corresponding to legacy Get, Query, and selected-buffer reads.
pub enum LegacyReadResult {
    Get(GetResult<LegacyReadLease>),
    Query(QueryResult<LegacyReadLease>),
    Select(SelectResult<LegacyReadLease>),
}

/// Lease guard that keeps the legacy selected result alive behind the compatibility boundary.
///
/// The native selected-buffer type remains private and is released by its existing `Drop`
/// implementation when the neutral result is dropped.
pub struct LegacyReadLease {
    _selected: SelectMappedBufferResult,
}

/// Converts a legacy selected buffer into neutral leased bytes.
pub fn selected_result_from_legacy(selected: SelectMappedBufferResult) -> SelectResult<LegacyReadLease> {
    let start_offset = selected.start_offset;
    let cache_state = match selected.cache_state {
        SelectMappedBufferCacheState::Unknown => ReadCacheState::Unknown,
        SelectMappedBufferCacheState::Hot => ReadCacheState::Hot,
        SelectMappedBufferCacheState::Cold => ReadCacheState::Cold,
    };
    let bytes = selected
        .get_bytes()
        .unwrap_or_else(|| Bytes::copy_from_slice(selected.get_buffer()));
    SelectResult::new(
        start_offset,
        LeasedBytes::new(bytes, LegacyReadLease { _selected: selected }),
        cache_state,
    )
}

/// Converts a legacy logical get result without changing navigation or accounting fields.
pub fn get_result_from_legacy(result: GetMessageResult) -> GetResult<LegacyReadLease> {
    let status = result.status().map(legacy_get_status_to_api);
    let queue_offsets = result.message_queue_offset().clone();
    let next_begin_offset = result.next_begin_offset();
    let min_offset = result.min_offset();
    let max_offset = result.max_offset();
    let buffer_total_size = result.buffer_total_size();
    let message_count = result.message_count();
    let suggest_pulling_from_replica = result.suggest_pulling_from_slave();
    let commercial_message_count = result.msg_count4_commercial();
    let commercial_size_per_message = result.commercial_size_per_msg();
    let cold_data_sum = result.cold_data_sum();
    let records = result
        .message_mapped_vec()
        .into_iter()
        .map(selected_result_from_legacy)
        .collect();
    GetResult {
        records,
        queue_offsets,
        status,
        next_begin_offset,
        min_offset,
        max_offset,
        buffer_total_size,
        message_count,
        suggest_pulling_from_replica,
        commercial_message_count,
        commercial_size_per_message,
        cold_data_sum,
    }
}

/// Converts a legacy key-query result without changing its index-safety projection.
pub fn query_result_from_legacy(result: QueryMessageResult) -> QueryResult<LegacyReadLease> {
    let QueryMessageResult {
        message_maped_list,
        index_last_update_timestamp,
        index_last_update_phyoffset,
        buffer_total_size,
        index_query_safe,
        index_safe_phyoffset,
        index_confirm_phyoffset,
    } = result;
    QueryResult {
        records: message_maped_list
            .into_iter()
            .map(selected_result_from_legacy)
            .collect(),
        index_last_update_timestamp,
        index_last_update_physical_offset: index_last_update_phyoffset,
        buffer_total_size,
        index_query_safe,
        index_safe_physical_offset: index_safe_phyoffset,
        index_confirm_physical_offset: index_confirm_phyoffset,
    }
}

impl<MS: MessageStore> MessageAppender<MessageExtBrokerInner> for LegacyMessageStoreAdapter<'_, MS> {
    type Receipt = LegacyAppendReceipt;
    type Error = StoreError;

    async fn append_message(&mut self, message: MessageExtBrokerInner) -> Result<Self::Receipt, Self::Error> {
        let result = self.store.put_message(message).await;
        Ok(legacy_append_receipt(
            result,
            self.store.get_max_phy_offset(),
            self.store.get_flushed_where(),
        ))
    }
}

impl<MS: MessageStore> MessageAppender<MessageExtBatch> for LegacyMessageStoreAdapter<'_, MS> {
    type Receipt = LegacyAppendReceipt;
    type Error = StoreError;

    async fn append_message(&mut self, message: MessageExtBatch) -> Result<Self::Receipt, Self::Error> {
        let result = self.store.put_messages(message).await;
        Ok(legacy_append_receipt(
            result,
            self.store.get_max_phy_offset(),
            self.store.get_flushed_where(),
        ))
    }
}

impl<MS> MessageReader for LegacyMessageStoreReadAdapter<'_, MS>
where
    MS: LegacyReadCallBoundary,
{
    type Request = LegacyReadRequest;
    type Output = Option<LegacyReadResult>;
    type Error = StoreError;

    async fn read(&self, request: Self::Request) -> Result<Self::Output, Self::Error> {
        let result = match request {
            LegacyReadRequest::Get {
                group,
                topic,
                queue_id,
                offset,
                max_messages,
                max_total_size,
            } => {
                let result = match max_total_size {
                    Some(max_total_size) => {
                        self.store
                            .get_message_with_size_limit(&group, &topic, queue_id, offset, max_messages, max_total_size)
                            .await
                    }
                    None => {
                        self.store
                            .get_message(&group, &topic, queue_id, offset, max_messages)
                            .await
                    }
                };
                result.map(get_result_from_legacy).map(LegacyReadResult::Get)
            }
            LegacyReadRequest::Query {
                topic,
                key,
                max_messages,
                begin,
                end,
            } => self
                .store
                .query_message(&topic, &key, max_messages, begin, end)
                .await
                .map(query_result_from_legacy)
                .map(LegacyReadResult::Query),
            LegacyReadRequest::Select { physical_offset, size } => self
                .store
                .select_message(physical_offset, size)
                .map(selected_result_from_legacy)
                .map(LegacyReadResult::Select),
        };
        Ok(result)
    }
}

impl<MS: MessageStore> StoreHealth for LegacyMessageStoreAdapter<'_, MS> {
    type Snapshot = LegacyStoreHealthSnapshot;

    fn health_snapshot(&self) -> Self::Snapshot {
        health_snapshot_from_legacy(self.store)
    }
}

impl<MS: MessageStore> StoreHealth for LegacyMessageStoreHealthAdapter<'_, MS> {
    type Snapshot = LegacyStoreHealthSnapshot;

    fn health_snapshot(&self) -> Self::Snapshot {
        health_snapshot_from_legacy(self.store)
    }
}

fn health_snapshot_from_legacy<MS: MessageStore>(store: &MS) -> LegacyStoreHealthSnapshot {
    let legacy = store.health_snapshot();
    LegacyStoreHealthSnapshot {
        writable: legacy.writeable,
        last_error: legacy
            .last_flush_error
            .as_ref()
            .map(|error| LegacyStoreHealthError::new(error.kind)),
        page_cache_busy: legacy.os_page_cache_busy,
        transient_pool_deficient: legacy.transient_store_pool_deficient,
        flush_backlog: LegacyFlushBacklog {
            queue_depth: legacy.sync_flush.queue_depth,
            oldest_wait_millis: legacy.sync_flush.oldest_wait_millis,
        },
        dispatch_behind_bytes: legacy.dispatch_behind_bytes,
        shutdown: legacy.shutdown,
        replication_pending_count: legacy.ha_pending_request_count,
        replication_oldest_wait_millis: legacy.ha_pending_oldest_wait_millis,
        appended_watermark: store.get_max_phy_offset(),
        durable_watermark: store.get_flushed_where(),
    }
}
