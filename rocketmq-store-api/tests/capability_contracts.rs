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

use bytes::Bytes;
use rocketmq_model::message::MessageQueue;
use rocketmq_store_api::AdminRequest;
use rocketmq_store_api::AdminResponse;
use rocketmq_store_api::AdminStore;
use rocketmq_store_api::AppendReceipt;
use rocketmq_store_api::AppendStatus;
use rocketmq_store_api::DerivedProgress;
use rocketmq_store_api::DerivedRecord;
use rocketmq_store_api::DerivedRecordSink;
use rocketmq_store_api::Durability;
use rocketmq_store_api::MessageAppender;
use rocketmq_store_api::MessageReader;
use rocketmq_store_api::OffsetIndex;
use rocketmq_store_api::OffsetRange;
use rocketmq_store_api::ReadRequest;
use rocketmq_store_api::ReadResult;
use rocketmq_store_api::ReplicationControl;
use rocketmq_store_api::ReplicationState;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreErrorKind;
use rocketmq_store_api::StoreFuture;
use rocketmq_store_api::StoreHealth;
use rocketmq_store_api::StoreHealthSnapshot;
use rocketmq_store_api::StoreLifecycle;

#[derive(Default)]
struct ContractStore;

impl StoreLifecycle for ContractStore {
    fn load(&mut self) -> StoreFuture<'_, bool> {
        Box::pin(async { Ok(true) })
    }

    fn start(&mut self) -> StoreFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn shutdown(&mut self) -> StoreFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

impl MessageAppender<Bytes> for ContractStore {
    fn append_message(&mut self, message: Bytes) -> StoreFuture<'_, AppendReceipt> {
        let end = i64::try_from(message.len()).expect("fixture length fits i64");
        Box::pin(async move {
            Ok(AppendReceipt::new(
                AppendStatus::PutOk,
                0..end,
                end,
                end,
                Durability::Local,
            ))
        })
    }
}

impl MessageReader for ContractStore {
    fn read(&self, _request: ReadRequest) -> StoreFuture<'_, ReadResult> {
        Box::pin(async { Ok(ReadResult::default()) })
    }
}

impl OffsetIndex for ContractStore {
    fn offset_range(&self, _queue: &MessageQueue) -> Result<OffsetRange, StoreError> {
        Ok(OffsetRange { min: 0, max: 1 })
    }
}

impl StoreHealth for ContractStore {
    fn health_snapshot(&self) -> StoreHealthSnapshot {
        StoreHealthSnapshot::default()
    }
}

impl ReplicationControl for ContractStore {
    fn replication_state(&self) -> ReplicationState {
        ReplicationState::default()
    }

    fn set_confirmed_offset(&mut self, offset: i64) -> Result<(), StoreError> {
        if offset < 0 {
            return Err(StoreError::new(StoreErrorKind::InvalidRequest, "set_confirmed_offset"));
        }
        Ok(())
    }
}

impl DerivedRecordSink for ContractStore {
    fn append_derived(&mut self, record: DerivedRecord) -> StoreFuture<'_, DerivedProgress> {
        Box::pin(async move {
            Ok(DerivedProgress {
                source_watermark: record.source_offset,
                derived_watermark: record.source_offset,
            })
        })
    }
}

impl AdminStore for ContractStore {
    fn execute_admin(&mut self, request: AdminRequest) -> StoreFuture<'_, AdminResponse> {
        Box::pin(async move {
            Ok(AdminResponse {
                operation: request.operation,
                affected: 0,
            })
        })
    }
}

fn assert_capabilities<T>()
where
    T: StoreLifecycle
        + MessageAppender<Bytes>
        + MessageReader
        + OffsetIndex
        + StoreHealth
        + ReplicationControl
        + DerivedRecordSink
        + AdminStore,
{
}

#[test]
fn all_narrow_capabilities_are_independently_composable() {
    assert_capabilities::<ContractStore>();
}

#[test]
fn append_receipt_distinguishes_appended_and_durable_watermarks() {
    let receipt = AppendReceipt::new(AppendStatus::FlushDiskTimeout, 40..60, 60, 48, Durability::Memory);

    assert_eq!(40..60, receipt.offsets);
    assert_eq!(60, receipt.appended_watermark);
    assert_eq!(48, receipt.durable_watermark);
    assert!(receipt.is_accepted());
    assert!(!receipt.is_durable());
}

#[test]
fn store_error_exposes_only_stable_backend_neutral_classification() {
    let error = StoreError::new(StoreErrorKind::Unavailable, "append_message");

    assert_eq!(StoreErrorKind::Unavailable, error.kind());
    assert_eq!("append_message", error.operation());
    assert_eq!("store operation append_message failed: unavailable", error.to_string());
}

#[test]
fn health_defaults_to_writable_without_backpressure() {
    let health = StoreHealthSnapshot::default();

    assert!(health.writable);
    assert!(!health.shutdown);
    assert_eq!(0, health.appended_watermark);
    assert_eq!(0, health.durable_watermark);
}
