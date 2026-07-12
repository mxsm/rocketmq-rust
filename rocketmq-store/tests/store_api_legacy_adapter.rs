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
use rocketmq_store::store_api_adapter::append_receipt_from_legacy;
use rocketmq_store::store_api_adapter::legacy_put_status_to_append_status;
use rocketmq_store::store_api_adapter::LegacyMessageStoreAdapter;
use rocketmq_store_api::AppendStatus;
use rocketmq_store_api::MessageAppender;
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
fn legacy_receipt_preserves_output_and_separate_watermarks() {
    let legacy = PutMessageResult::new_append_result(PutMessageStatus::FlushDiskTimeout, Some(append_result()));

    let receipt = append_receipt_from_legacy(legacy, 80, 48);

    assert_eq!(AppendStatus::FlushDiskTimeout, receipt.status);
    assert_eq!(40..60, receipt.offsets);
    assert_eq!(80, receipt.appended_watermark);
    assert_eq!(48, receipt.durable_watermark);
    assert_eq!(Some("message-id"), receipt.message_id.as_deref());
    assert_eq!(1234, receipt.store_timestamp);
    assert_eq!(7, receipt.logical_offset);
    assert_eq!(2, receipt.message_count);
    assert!(receipt.is_accepted());
    assert!(!receipt.is_durable());
}

#[test]
fn every_legacy_status_has_an_explicit_backend_neutral_mapping() {
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
        (PutMessageStatus::PropertiesSizeExceeded, AppendStatus::InvalidMessage),
        (PutMessageStatus::OsPageCacheBusy, AppendStatus::PageCacheBusy),
        (PutMessageStatus::UnknownError, AppendStatus::Unknown),
        (
            PutMessageStatus::InSyncReplicasNotEnough,
            AppendStatus::ReplicasUnavailable,
        ),
        (
            PutMessageStatus::PutToRemoteBrokerFail,
            AppendStatus::RemoteAppendFailed,
        ),
        (PutMessageStatus::LmqConsumeQueueNumExceeded, AppendStatus::QueueLimit),
        (PutMessageStatus::WheelTimerFlowControl, AppendStatus::TimerFlowControl),
        (
            PutMessageStatus::WheelTimerMsgIllegal,
            AppendStatus::TimerMessageIllegal,
        ),
        (PutMessageStatus::WheelTimerNotEnable, AppendStatus::TimerNotEnabled),
    ];

    for (legacy, expected) in cases {
        assert_eq!(expected, legacy_put_status_to_append_status(legacy));
    }
}

#[allow(dead_code)]
fn legacy_adapter_compile_fixture<'a, MS>(adapter: LegacyMessageStoreAdapter<'a, MS>)
where
    MS: MessageStore,
    LegacyMessageStoreAdapter<'a, MS>: MessageAppender<rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner>
        + MessageAppender<rocketmq_common::common::message::message_batch::MessageExtBatch>
        + StoreHealth,
{
    let _ = adapter;
}
