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
use rocketmq_store_api::MessageAppender;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreHealth;

use crate::base::message_result::PutMessageResult;
use crate::base::message_store::MessageStore;
use crate::store_error::StoreErrorKind;

/// Legacy append output plus independent append and durable progress.
#[derive(Clone)]
pub struct LegacyAppendReceipt {
    result: PutMessageResult,
    appended_watermark: i64,
    durable_watermark: i64,
}

impl LegacyAppendReceipt {
    /// Returns the unchanged legacy append result.
    pub const fn result(&self) -> &PutMessageResult {
        &self.result
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
    LegacyAppendReceipt {
        result,
        appended_watermark,
        durable_watermark,
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
