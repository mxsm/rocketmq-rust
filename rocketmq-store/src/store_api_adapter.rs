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

use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_store_api::AppendReceipt;
use rocketmq_store_api::AppendStatus;
use rocketmq_store_api::Durability;
use rocketmq_store_api::FlushBacklog;
use rocketmq_store_api::MessageAppender;
use rocketmq_store_api::StoreErrorKind as ApiStoreErrorKind;
use rocketmq_store_api::StoreFuture;
use rocketmq_store_api::StoreHealth;
use rocketmq_store_api::StoreHealthSnapshot as ApiStoreHealthSnapshot;

use crate::base::message_result::PutMessageResult;
use crate::base::message_status_enum::PutMessageStatus;
use crate::base::message_store::MessageStore;
use crate::store_error::StoreErrorKind;

/// Borrowing adapter that preserves the legacy [`MessageStore`] implementation while exposing
/// only narrow store capabilities to new consumers.
pub struct LegacyMessageStoreAdapter<'a, MS> {
    store: &'a mut MS,
}

/// Read-only borrowing adapter for admission paths that only need store health.
pub struct LegacyMessageStoreHealthAdapter<'a, MS> {
    store: &'a MS,
}

impl<'a, MS> LegacyMessageStoreHealthAdapter<'a, MS> {
    /// Wraps an immutable legacy store view without changing its ownership.
    pub fn new(store: &'a MS) -> Self {
        Self { store }
    }
}

impl<'a, MS> LegacyMessageStoreAdapter<'a, MS> {
    /// Wraps an existing legacy store without changing its ownership or lifecycle.
    pub fn new(store: &'a mut MS) -> Self {
        Self { store }
    }
}

impl<MS: MessageStore> MessageAppender<MessageExtBrokerInner> for LegacyMessageStoreAdapter<'_, MS> {
    fn append_message(&mut self, message: MessageExtBrokerInner) -> StoreFuture<'_, AppendReceipt> {
        Box::pin(async move {
            let result = self.store.put_message(message).await;
            Ok(append_receipt_from_legacy(
                result,
                self.store.get_max_phy_offset(),
                self.store.get_flushed_where(),
            ))
        })
    }
}

impl<MS: MessageStore> MessageAppender<MessageExtBatch> for LegacyMessageStoreAdapter<'_, MS> {
    fn append_message(&mut self, message: MessageExtBatch) -> StoreFuture<'_, AppendReceipt> {
        Box::pin(async move {
            let result = self.store.put_messages(message).await;
            Ok(append_receipt_from_legacy(
                result,
                self.store.get_max_phy_offset(),
                self.store.get_flushed_where(),
            ))
        })
    }
}

impl<MS: MessageStore> StoreHealth for LegacyMessageStoreAdapter<'_, MS> {
    fn health_snapshot(&self) -> ApiStoreHealthSnapshot {
        health_snapshot_from_legacy(self.store)
    }
}

impl<MS: MessageStore> StoreHealth for LegacyMessageStoreHealthAdapter<'_, MS> {
    fn health_snapshot(&self) -> ApiStoreHealthSnapshot {
        health_snapshot_from_legacy(self.store)
    }
}

/// Converts a legacy append result without leaking its implementation types across the new API.
pub fn append_receipt_from_legacy(
    result: PutMessageResult,
    appended_watermark: i64,
    durable_watermark: i64,
) -> AppendReceipt {
    let status = legacy_put_status_to_append_status(result.put_message_status());
    let Some(append) = result.append_message_result() else {
        return AppendReceipt::new(
            status,
            appended_watermark..appended_watermark,
            appended_watermark,
            durable_watermark,
            Durability::Memory,
        );
    };
    let start = append.wrote_offset;
    let end = start.saturating_add(i64::from(append.wrote_bytes));
    let durability = if durable_watermark >= end {
        Durability::Local
    } else {
        Durability::Memory
    };
    let mut receipt = AppendReceipt::new(status, start..end, appended_watermark, durable_watermark, durability);
    receipt.wrote_bytes = append.wrote_bytes;
    receipt.message_id = append.get_message_id();
    receipt.store_timestamp = append.store_timestamp;
    receipt.logical_offset = append.logics_offset;
    receipt.message_count = append.msg_num;
    receipt
}

/// Maps every legacy append outcome to its stable backend-neutral counterpart.
pub const fn legacy_put_status_to_append_status(status: PutMessageStatus) -> AppendStatus {
    match status {
        PutMessageStatus::PutOk => AppendStatus::PutOk,
        PutMessageStatus::FlushDiskTimeout => AppendStatus::FlushDiskTimeout,
        PutMessageStatus::FlushSlaveTimeout => AppendStatus::FlushReplicaTimeout,
        PutMessageStatus::SlaveNotAvailable => AppendStatus::ReplicaUnavailable,
        PutMessageStatus::ServiceNotAvailable => AppendStatus::ServiceUnavailable,
        PutMessageStatus::CreateMappedFileFailed => AppendStatus::StorageUnavailable,
        PutMessageStatus::MessageIllegal | PutMessageStatus::PropertiesSizeExceeded => AppendStatus::InvalidMessage,
        PutMessageStatus::OsPageCacheBusy => AppendStatus::PageCacheBusy,
        PutMessageStatus::UnknownError => AppendStatus::Unknown,
        PutMessageStatus::InSyncReplicasNotEnough => AppendStatus::ReplicasUnavailable,
        PutMessageStatus::PutToRemoteBrokerFail => AppendStatus::RemoteAppendFailed,
        PutMessageStatus::LmqConsumeQueueNumExceeded => AppendStatus::QueueLimit,
        PutMessageStatus::WheelTimerFlowControl => AppendStatus::TimerFlowControl,
        PutMessageStatus::WheelTimerMsgIllegal => AppendStatus::TimerMessageIllegal,
        PutMessageStatus::WheelTimerNotEnable => AppendStatus::TimerNotEnabled,
    }
}

const fn map_error_kind(kind: StoreErrorKind) -> ApiStoreErrorKind {
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

fn health_snapshot_from_legacy<MS: MessageStore>(store: &MS) -> ApiStoreHealthSnapshot {
    let legacy = store.health_snapshot();
    ApiStoreHealthSnapshot {
        writable: legacy.writeable,
        last_error: legacy.last_flush_error.as_ref().map(|error| map_error_kind(error.kind)),
        page_cache_busy: legacy.os_page_cache_busy,
        transient_pool_deficient: legacy.transient_store_pool_deficient,
        flush_backlog: FlushBacklog {
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
