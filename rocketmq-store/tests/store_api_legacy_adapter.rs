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

use rocketmq_store::base::message_result::AppendMessageResult;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::AppendMessageStatus;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::message_store::GenericMessageStore;
use rocketmq_store::message_store::OwnedMessageStore;
use rocketmq_store::store_api_adapter::legacy_append_receipt;
use rocketmq_store::store_api_adapter::legacy_error_kind_to_api;
use rocketmq_store::store_api_adapter::legacy_get_status_to_api;
use rocketmq_store::store_api_adapter::legacy_put_status_to_append_status;
use rocketmq_store::store_api_adapter::LegacyAppendReceipt;
use rocketmq_store::store_api_adapter::LegacyMessageStoreAdapter;
use rocketmq_store::store_api_adapter::LegacyMessageStoreReadAdapter;
use rocketmq_store::store_api_adapter::LegacyReadRequest;
use rocketmq_store::store_api_adapter::LegacyReadResult;
use rocketmq_store::store_api_adapter::LegacyStoreHealthError;
use rocketmq_store::store_api_adapter::LegacyStoreHealthSnapshot;
use rocketmq_store::store_error::StoreErrorKind;
use rocketmq_store_api::AppendReceiptError;
use rocketmq_store_api::AppendStatus;
use rocketmq_store_api::Durability;
use rocketmq_store_api::GetStatus;
use rocketmq_store_api::MessageAppender;
use rocketmq_store_api::MessageReader;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreErrorKind as ApiStoreErrorKind;
use rocketmq_store_api::StoreHealth;

fn append_result() -> AppendMessageResult {
    AppendMessageResult {
        status: AppendMessageStatus::PutOk,
        wrote_offset: 40,
        wrote_bytes: 20,
        msg_id: Some("message-id".to_string()),
        msg_id_supplier: None,
        store_timestamp: 1234,
        logics_offset: 7,
        page_cache_rt: 0,
        msg_num: 2,
    }
}

#[test]
fn legacy_receipt_preserves_result_and_separate_watermarks() {
    let legacy = PutMessageResult::new_append_result(PutMessageStatus::FlushDiskTimeout, Some(append_result()));

    let receipt = legacy_append_receipt(legacy, 80, 48);

    assert_eq!(
        PutMessageStatus::FlushDiskTimeout,
        receipt.result().put_message_status()
    );
    assert_eq!(80, receipt.appended_watermark());
    assert_eq!(48, receipt.durable_watermark());
    let canonical = receipt.canonical().expect("valid canonical receipt");
    assert_eq!(AppendStatus::FlushDiskTimeout, canonical.status());
    assert_eq!(Some(40..60), canonical.appended_range());
    assert_eq!(Durability::Memory, canonical.durability());
    let append = receipt.result().append_message_result().expect("append result");
    assert_eq!(40, append.wrote_offset);
    assert_eq!(20, append.wrote_bytes);
    assert_eq!(Some("message-id"), append.get_message_id().as_deref());
    assert_eq!(1234, append.store_timestamp);
    assert_eq!(7, append.logics_offset);
    assert_eq!(2, append.msg_num);
}

#[test]
fn legacy_receipt_reports_invalid_projection_without_panicking() {
    let mut empty = append_result();
    empty.wrote_bytes = 0;
    let receipt = legacy_append_receipt(
        PutMessageResult::new_append_result(PutMessageStatus::PutOk, Some(empty)),
        80,
        48,
    );
    assert_eq!(Some(&AppendReceiptError::EmptyRange), receipt.canonical().err());
    assert_eq!(PutMessageStatus::PutOk, receipt.result().put_message_status());

    let remote = legacy_append_receipt(PutMessageResult::new(PutMessageStatus::PutOk, None, true), 80, 48);
    assert_eq!(
        Some(&AppendReceiptError::AcceptedStatusWithoutRange),
        remote.canonical().err()
    );
    assert!(remote.result().remote_put());
}

#[test]
fn rejected_outer_status_ignores_inner_append_shape_for_canonical_projection() {
    let cases = [
        PutMessageStatus::CreateMappedFileFailed,
        PutMessageStatus::MessageIllegal,
        PutMessageStatus::UnknownError,
    ];

    for status in cases {
        for wrote_bytes in [20, 0] {
            let mut diagnostics = append_result();
            diagnostics.wrote_bytes = wrote_bytes;
            let receipt = legacy_append_receipt(PutMessageResult::new_append_result(status, Some(diagnostics)), 80, 48);

            let canonical = receipt.canonical().expect("rejected status projects without a range");
            assert_eq!(legacy_put_status_to_append_status(status), canonical.status());
            assert_eq!(None, canonical.appended_range());
            assert!(!canonical.is_accepted());
            assert_eq!(
                wrote_bytes,
                receipt
                    .result()
                    .append_message_result()
                    .expect("legacy diagnostics remain available")
                    .wrote_bytes
            );
        }
    }
}

#[test]
fn every_legacy_append_status_maps_exhaustively_without_collapsing() {
    let cases = [
        (PutMessageStatus::PutOk, AppendStatus::PutOk),
        (PutMessageStatus::FlushDiskTimeout, AppendStatus::FlushDiskTimeout),
        (PutMessageStatus::FlushSlaveTimeout, AppendStatus::FlushReplicaTimeout),
        (PutMessageStatus::SlaveNotAvailable, AppendStatus::ReplicaUnavailable),
        (PutMessageStatus::ServiceNotAvailable, AppendStatus::ServiceUnavailable),
        (
            PutMessageStatus::CreateMappedFileFailed,
            AppendStatus::StorageUnavailable,
        ),
        (PutMessageStatus::MessageIllegal, AppendStatus::InvalidMessage),
        (
            PutMessageStatus::PropertiesSizeExceeded,
            AppendStatus::PropertiesTooLarge,
        ),
        (PutMessageStatus::OsPageCacheBusy, AppendStatus::PageCacheBusy),
        (PutMessageStatus::UnknownError, AppendStatus::Unknown),
        (
            PutMessageStatus::InSyncReplicasNotEnough,
            AppendStatus::InsufficientReplicas,
        ),
        (
            PutMessageStatus::PutToRemoteBrokerFail,
            AppendStatus::RemoteAppendFailed,
        ),
        (
            PutMessageStatus::LmqConsumeQueueNumExceeded,
            AppendStatus::QueueLimitExceeded,
        ),
        (
            PutMessageStatus::WheelTimerFlowControl,
            AppendStatus::ScheduleFlowControl,
        ),
        (
            PutMessageStatus::WheelTimerMsgIllegal,
            AppendStatus::ScheduleMessageIllegal,
        ),
        (PutMessageStatus::WheelTimerNotEnable, AppendStatus::ScheduleDisabled),
    ];

    for (legacy, expected) in cases {
        assert_eq!(expected, legacy_put_status_to_append_status(legacy));
    }
}

#[test]
fn every_legacy_error_kind_maps_to_a_neutral_kind_exhaustively() {
    let cases = [
        (StoreErrorKind::MappedFile, ApiStoreErrorKind::Storage),
        (StoreErrorKind::RocksDb, ApiStoreErrorKind::Storage),
        (StoreErrorKind::NotStarted, ApiStoreErrorKind::NotStarted),
        (StoreErrorKind::MessageNotFound, ApiStoreErrorKind::NotFound),
        (StoreErrorKind::Config, ApiStoreErrorKind::InvalidRequest),
        (StoreErrorKind::Unsupported, ApiStoreErrorKind::Unsupported),
        (StoreErrorKind::InvalidState, ApiStoreErrorKind::Internal),
        (StoreErrorKind::Storage, ApiStoreErrorKind::Storage),
        (StoreErrorKind::TieredStore, ApiStoreErrorKind::Unavailable),
        (StoreErrorKind::Ha, ApiStoreErrorKind::Unavailable),
        (StoreErrorKind::DLedger, ApiStoreErrorKind::Unavailable),
        (StoreErrorKind::MappedFileNotFound, ApiStoreErrorKind::NotFound),
    ];

    for (legacy, expected) in cases {
        assert_eq!(expected, legacy_error_kind_to_api(legacy));
    }
}

#[test]
fn every_legacy_get_status_maps_exhaustively() {
    use rocketmq_store::base::message_status_enum::GetMessageStatus;

    let cases = [
        (GetMessageStatus::Found, GetStatus::Found),
        (GetMessageStatus::NoMatchedMessage, GetStatus::NoMatchedMessage),
        (GetMessageStatus::MessageWasRemoving, GetStatus::MessageWasRemoving),
        (GetMessageStatus::OffsetFoundNull, GetStatus::OffsetFoundNull),
        (GetMessageStatus::OffsetOverflowBadly, GetStatus::OffsetOverflowBadly),
        (GetMessageStatus::OffsetOverflowOne, GetStatus::OffsetOverflowOne),
        (GetMessageStatus::OffsetTooSmall, GetStatus::OffsetTooSmall),
        (GetMessageStatus::NoMatchedLogicQueue, GetStatus::NoMatchedLogicQueue),
        (GetMessageStatus::NoMessageInQueue, GetStatus::NoMessageInQueue),
        (GetMessageStatus::OffsetReset, GetStatus::OffsetReset),
    ];

    for (legacy, expected) in cases {
        assert_eq!(expected, legacy_get_status_to_api(legacy));
    }
}

#[test]
fn every_legacy_health_error_preserves_its_exact_compatibility_token() {
    let cases = [
        (StoreErrorKind::MappedFile, "mapped_file"),
        (StoreErrorKind::RocksDb, "rocksdb"),
        (StoreErrorKind::NotStarted, "not_started"),
        (StoreErrorKind::MessageNotFound, "message_not_found"),
        (StoreErrorKind::Config, "config"),
        (StoreErrorKind::Unsupported, "unsupported"),
        (StoreErrorKind::InvalidState, "invalid_state"),
        (StoreErrorKind::Storage, "storage"),
        (StoreErrorKind::TieredStore, "tiered_store"),
        (StoreErrorKind::Ha, "ha"),
        (StoreErrorKind::DLedger, "dledger"),
        (StoreErrorKind::MappedFileNotFound, "mapped_file_not_found"),
    ];

    for (kind, expected) in cases {
        assert_eq!(expected, LegacyStoreHealthError::new(kind).compatibility_token());
    }
}

#[test]
fn legacy_health_exposes_a_backend_neutral_canonical_projection() {
    let legacy = LegacyStoreHealthSnapshot {
        writable: false,
        last_error: Some(LegacyStoreHealthError::new(StoreErrorKind::Ha)),
        appended_watermark: 80,
        durable_watermark: 48,
        ..LegacyStoreHealthSnapshot::default()
    };

    let canonical = legacy.canonical();

    assert!(!canonical.writable());
    assert_eq!(Some(ApiStoreErrorKind::Unavailable), canonical.last_error());
    assert_eq!(80, canonical.appended_watermark());
    assert_eq!(48, canonical.durable_watermark());
}

fn assert_adapter_contract<MS>()
where
    MS: MessageStore,
    for<'a> LegacyMessageStoreAdapter<'a, MS>: MessageAppender<
            rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner,
            Receipt = LegacyAppendReceipt,
            Error = StoreError,
        > + MessageAppender<
            rocketmq_common::common::message::message_batch::MessageExtBatch,
            Receipt = LegacyAppendReceipt,
            Error = StoreError,
        > + StoreHealth<Snapshot = LegacyStoreHealthSnapshot>,
    for<'a> LegacyMessageStoreReadAdapter<'a, MS>:
        MessageReader<Request = LegacyReadRequest, Output = Option<LegacyReadResult>, Error = StoreError>,
{
    assert!(std::mem::size_of::<LegacyMessageStoreAdapter<'_, MS>>() > 0);
}

#[test]
fn legacy_adapter_compile_fixture_is_monomorphized() {
    assert_adapter_contract::<GenericMessageStore>();
    assert_adapter_contract::<OwnedMessageStore>();
}
