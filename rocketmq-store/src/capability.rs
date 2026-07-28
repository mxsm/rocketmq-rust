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

//! Canonical capability projections for the composed message store.
//!
//! The legacy `MessageStore` implementations below are compatibility
//! projections. New request paths depend on the corresponding
//! `rocketmq-store-api` capability and must not extend `MessageStoreInner`.

use std::future::Future;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_store_api::AppendReceipt;
use rocketmq_store_api::AppendReceiptError;
use rocketmq_store_api::AppendStatus;
use rocketmq_store_api::Durability;
use rocketmq_store_api::FlushBacklog as ApiFlushBacklog;
use rocketmq_store_api::GetResult;
use rocketmq_store_api::GetStatus;
use rocketmq_store_api::LeasedBytes;
use rocketmq_store_api::MessageReader;
use rocketmq_store_api::QueryResult;
use rocketmq_store_api::ReadCacheState;
use rocketmq_store_api::SelectResult;
use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreErrorKind;
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

/// Backend append output plus independent append and durable progress.
#[derive(Clone)]
pub struct StoreAppendReceipt {
    result: PutMessageResult,
    canonical: Result<AppendReceipt, AppendReceiptError>,
    appended_watermark: i64,
    durable_watermark: i64,
}

impl StoreAppendReceipt {
    /// Returns the backend append result.
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

/// Builds the canonical receipt without interpreting or rewriting backend status fields.
pub fn store_append_receipt(
    result: PutMessageResult,
    appended_watermark: i64,
    durable_watermark: i64,
) -> StoreAppendReceipt {
    let status = put_status_to_append_status(result.put_message_status());
    let canonical = if status.is_accepted() {
        match result.append_message_result() {
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
        }
    } else {
        AppendReceipt::try_rejected(status, appended_watermark, durable_watermark)
    };
    StoreAppendReceipt {
        result,
        canonical,
        appended_watermark,
        durable_watermark,
    }
}

/// Maps every backend append outcome to a distinct neutral status.
pub const fn put_status_to_append_status(status: PutMessageStatus) -> AppendStatus {
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

/// Maps every backend get outcome without changing its semantics.
pub const fn get_status_to_api(status: GetMessageStatus) -> GetStatus {
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

/// Exact backend health error retained by the Broker admission projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreHealthError {
    kind: StoreErrorKind,
    component: StoreComponent,
}

impl StoreHealthError {
    /// Creates an exact projection from the backend kind.
    pub const fn new(kind: StoreErrorKind) -> Self {
        Self {
            kind,
            component: StoreComponent::Store,
        }
    }

    /// Creates a health projection with an exact component token.
    pub const fn in_component(kind: StoreErrorKind, component: StoreComponent) -> Self {
        Self { kind, component }
    }

    /// Creates an exact projection from one canonical store error.
    pub const fn from_error(error: &crate::base::message_store::StoreHealthError) -> Self {
        Self {
            kind: error.kind,
            component: error.component,
        }
    }

    /// Returns the original low-cardinality backend token.
    pub const fn backend_token(self) -> &'static str {
        match self.component {
            StoreComponent::Store | StoreComponent::Configuration => self.kind.as_str(),
            component => component.as_str(),
        }
    }
}

/// Compact sync-flush pressure retained by the Broker admission projection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StoreFlushBacklog {
    pub queue_depth: u64,
    pub oldest_wait_millis: u64,
}

/// Exact health data consumed by Broker admission behavior.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreHealthSnapshot {
    pub writable: bool,
    pub last_error: Option<StoreHealthError>,
    pub page_cache_busy: bool,
    pub transient_pool_deficient: bool,
    pub flush_backlog: StoreFlushBacklog,
    pub dispatch_behind_bytes: i64,
    pub shutdown: bool,
    pub replication_pending_count: u64,
    pub replication_oldest_wait_millis: u64,
    pub appended_watermark: i64,
    pub durable_watermark: i64,
}

impl Default for StoreHealthSnapshot {
    fn default() -> Self {
        Self {
            writable: true,
            last_error: None,
            page_cache_busy: false,
            transient_pool_deficient: false,
            flush_backlog: StoreFlushBacklog::default(),
            dispatch_behind_bytes: 0,
            shutdown: false,
            replication_pending_count: 0,
            replication_oldest_wait_millis: 0,
            appended_watermark: 0,
            durable_watermark: 0,
        }
    }
}

impl StoreHealthSnapshot {
    /// Returns the backend-neutral health result while retaining exact backend data in this value.
    pub fn canonical(&self) -> ApiStoreHealthSnapshot {
        ApiStoreHealthSnapshot {
            writable: self.writable,
            last_error: self.last_error.map(|error| error.kind),
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

/// Read-only capability for admission paths that only need store health.
pub struct MessageStoreHealthCapability<'a, MS> {
    store: &'a MS,
}

impl<'a, MS> MessageStoreHealthCapability<'a, MS> {
    /// Wraps an immutable store view without changing ownership.
    pub fn new(store: &'a MS) -> Self {
        Self { store }
    }
}

/// Narrow port for message-store read methods used by [`MessageReader`].
///
/// Filtered reads remain on the backend trait. This canonical read port intentionally
/// forwards only unfiltered reads, so its hot request path has no dynamic filter allocation.
/// Test doubles can implement these four operations without copying `MessageStore`.
pub(crate) trait MessageStoreReadPort: Sync {
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

impl<MS: MessageStore> MessageStoreReadPort for MS {
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

/// Read-only capability that forwards backend reads through [`MessageReader`].
pub struct MessageStoreReadCapability<'a, MS> {
    store: &'a MS,
}

impl<'a, MS> MessageStoreReadCapability<'a, MS> {
    /// Wraps an immutable store view without changing ownership.
    pub const fn new(store: &'a MS) -> Self {
        Self { store }
    }
}

/// Message-store read calls represented as one closed request enum.
pub enum MessageReadRequest {
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

/// Neutral result variants corresponding to get, query, and selected-buffer reads.
pub enum MessageReadResult {
    Get(GetResult<MessageReadLease>),
    Query(QueryResult<MessageReadLease>),
    Select(SelectResult<MessageReadLease>),
}

/// Lease guard that keeps the backend selected result alive behind the capability boundary.
///
/// The native selected-buffer type remains private and is released by its existing `Drop`
/// implementation when the neutral result is dropped.
pub struct MessageReadLease {
    _selected: SelectMappedBufferResult,
}

/// Converts a backend selected buffer into neutral leased bytes.
pub fn selected_result(selected: SelectMappedBufferResult) -> SelectResult<MessageReadLease> {
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
        LeasedBytes::new(bytes, MessageReadLease { _selected: selected }),
        cache_state,
    )
}

/// Converts a backend logical get result without changing navigation or accounting fields.
pub fn get_result(result: GetMessageResult) -> GetResult<MessageReadLease> {
    let status = result.status().map(get_status_to_api);
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
    let records = result.message_mapped_vec().into_iter().map(selected_result).collect();
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

/// Converts a backend key-query result without changing its index-safety projection.
pub fn query_result(result: QueryMessageResult) -> QueryResult<MessageReadLease> {
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
        records: message_maped_list.into_iter().map(selected_result).collect(),
        index_last_update_timestamp,
        index_last_update_physical_offset: index_last_update_phyoffset,
        buffer_total_size,
        index_query_safe,
        index_safe_physical_offset: index_safe_phyoffset,
        index_confirm_physical_offset: index_confirm_phyoffset,
    }
}

impl<MS> MessageReader for MessageStoreReadCapability<'_, MS>
where
    MS: MessageStoreReadPort,
{
    type Request = MessageReadRequest;
    type Output = Option<MessageReadResult>;
    type Error = StoreError;

    async fn read(&self, request: Self::Request) -> Result<Self::Output, Self::Error> {
        let result = match request {
            MessageReadRequest::Get {
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
                result.map(get_result).map(MessageReadResult::Get)
            }
            MessageReadRequest::Query {
                topic,
                key,
                max_messages,
                begin,
                end,
            } => self
                .store
                .query_message(&topic, &key, max_messages, begin, end)
                .await
                .map(query_result)
                .map(MessageReadResult::Query),
            MessageReadRequest::Select { physical_offset, size } => self
                .store
                .select_message(physical_offset, size)
                .map(selected_result)
                .map(MessageReadResult::Select),
        };
        Ok(result)
    }
}

impl<MS: MessageStore> StoreHealth for MessageStoreHealthCapability<'_, MS> {
    type Snapshot = StoreHealthSnapshot;

    fn health_snapshot(&self) -> Self::Snapshot {
        store_health_snapshot(self.store)
    }
}

pub fn store_health_snapshot<MS: MessageStore>(store: &MS) -> StoreHealthSnapshot {
    let backend = store.health_snapshot();
    StoreHealthSnapshot {
        writable: backend.writeable,
        last_error: backend.last_flush_error.as_ref().map(StoreHealthError::from_error),
        page_cache_busy: backend.os_page_cache_busy,
        transient_pool_deficient: backend.transient_store_pool_deficient,
        flush_backlog: StoreFlushBacklog {
            queue_depth: backend.sync_flush.queue_depth,
            oldest_wait_millis: backend.sync_flush.oldest_wait_millis,
        },
        dispatch_behind_bytes: backend.dispatch_behind_bytes,
        shutdown: backend.shutdown,
        replication_pending_count: backend.ha_pending_request_count,
        replication_oldest_wait_millis: backend.ha_pending_oldest_wait_millis,
        appended_watermark: store.get_max_phy_offset(),
        durable_watermark: store.get_flushed_where(),
    }
}

#[cfg(test)]
#[path = "capability_test.rs"]
mod tests;
