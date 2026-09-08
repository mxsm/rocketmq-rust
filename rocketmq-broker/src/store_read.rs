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

use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_store::get_result;
use rocketmq_store::GetMessageResult;
use rocketmq_store_api::GetStatus;
use rocketmq_store_api::ReadOutcome;
use tracing::error;

use crate::broker_error::BrokerResult;

enum DecodeFailurePolicy {
    SkipRecord,
    FailRead,
}

/// Decodes a backend store result into the owned store-api read boundary.
///
/// The backend lease remains local to this capability and is released after every selected
/// record has been decoded. Consumers only receive owned model messages and canonical store
/// navigation metadata.
pub(crate) fn decode_read_outcome(result: GetMessageResult, decompress_body: bool) -> Option<ReadOutcome<MessageExt>> {
    decode_store_records(result, decompress_body, DecodeFailurePolicy::SkipRecord).ok()
}

pub(crate) fn decode_transaction_read_outcome(result: GetMessageResult) -> BrokerResult<ReadOutcome<MessageExt>> {
    decode_store_records(result, false, DecodeFailurePolicy::FailRead)
}

fn decode_store_records(
    result: GetMessageResult,
    decompress_body: bool,
    failure_policy: DecodeFailurePolicy,
) -> BrokerResult<ReadOutcome<MessageExt>> {
    let canonical = get_result(result);
    let Some(status) = canonical.status else {
        error!("store read result did not include a status");
        return Err(crate::broker_error::storage_read_failed());
    };
    let records = if status == GetStatus::Found {
        let mut decoded = Vec::with_capacity(canonical.records.len());
        for (index, selected) in canonical.records.iter().enumerate() {
            let mut bytes = selected.data().bytes().clone();
            if let Some(message) = MessageDecoder::decode(&mut bytes, true, decompress_body, false, false, false) {
                decoded.push(message);
            } else {
                error!(
                    index,
                    start_offset = selected.start_offset(),
                    size = selected.size(),
                    "failed to decode selected store record"
                );
                if matches!(failure_policy, DecodeFailurePolicy::FailRead) {
                    return Err(crate::broker_error::storage_read_failed());
                }
            }
        }
        Some(decoded)
    } else {
        None
    };

    Ok(ReadOutcome::new(
        status,
        canonical.next_begin_offset,
        canonical.min_offset,
        canonical.max_offset,
        records,
    ))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rocketmq_store::GetMessageStatus;
    use rocketmq_store::SelectMappedBufferResult;

    use super::*;

    #[test]
    fn transaction_decode_preserves_empty_navigation_but_rejects_missing_status() {
        let error = decode_transaction_read_outcome(GetMessageResult::new()).unwrap_err();
        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_READ_FAILED);

        let mut empty = GetMessageResult::new();
        empty.set_status(Some(GetMessageStatus::NoMessageInQueue));
        empty.set_next_begin_offset(7);
        empty.set_min_offset(7);
        empty.set_max_offset(7);
        let decoded = decode_transaction_read_outcome(empty).unwrap();
        assert_eq!(decoded.status(), GetStatus::NoMessageInQueue);
        assert_eq!(decoded.next_begin_offset(), 7);
        assert!(decoded.records().is_none());
    }

    #[test]
    fn transaction_decode_does_not_advance_past_a_corrupt_selected_record() {
        let mut result = GetMessageResult::new();
        result.set_status(Some(GetMessageStatus::Found));
        result.set_next_begin_offset(8);
        result.add_message(
            SelectMappedBufferResult::from_bytes(0, Bytes::from_static(b"truncated")).unwrap(),
            7,
            1,
        );
        let error = decode_transaction_read_outcome(result).unwrap_err();
        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_READ_FAILED);
    }
}
