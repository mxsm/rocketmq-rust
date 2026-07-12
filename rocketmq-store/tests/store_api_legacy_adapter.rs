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
use rocketmq_store::store_api_adapter::legacy_append_receipt;
use rocketmq_store::store_api_adapter::LegacyAppendReceipt;
use rocketmq_store::store_api_adapter::LegacyMessageStoreAdapter;
use rocketmq_store::store_api_adapter::LegacyStoreHealthError;
use rocketmq_store::store_api_adapter::LegacyStoreHealthSnapshot;
use rocketmq_store::store_error::StoreErrorKind;
use rocketmq_store_api::MessageAppender;
use rocketmq_store_api::StoreError;
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
    let append = receipt.result().append_message_result().expect("append result");
    assert_eq!(40, append.wrote_offset);
    assert_eq!(20, append.wrote_bytes);
    assert_eq!(Some("message-id"), append.get_message_id().as_deref());
    assert_eq!(1234, append.store_timestamp);
    assert_eq!(7, append.logics_offset);
    assert_eq!(2, append.msg_num);
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
{
    assert!(std::mem::size_of::<LegacyMessageStoreAdapter<'_, MS>>() > 0);
}

#[test]
fn legacy_adapter_compile_fixture_is_monomorphized() {
    assert_adapter_contract::<GenericMessageStore>();
}
