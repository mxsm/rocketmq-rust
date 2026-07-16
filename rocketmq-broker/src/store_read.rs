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

use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::store_api_adapter::get_result_from_legacy;
use rocketmq_store_api::GetStatus;
use rocketmq_store_api::ReadOutcome;
use tracing::error;

/// Decodes a legacy store result into the owned store-api read boundary.
///
/// The compatibility lease remains local to this adapter and is released after every selected
/// record has been decoded. Consumers only receive owned model messages and canonical store
/// navigation metadata.
pub(crate) fn decode_read_outcome(result: GetMessageResult, decompress_body: bool) -> Option<ReadOutcome<MessageExt>> {
    let canonical = get_result_from_legacy(result);
    let Some(status) = canonical.status else {
        error!("store read result did not include a status");
        return None;
    };
    let records = if status == GetStatus::Found {
        let mut decoded = Vec::with_capacity(canonical.records.len());
        for (index, selected) in canonical.records.iter().enumerate() {
            let mut bytes = selected.data().bytes().clone();
            if let Some(message) = message_decoder::decode(&mut bytes, true, decompress_body, false, false, false) {
                decoded.push(message);
            } else {
                error!(
                    index,
                    start_offset = selected.start_offset(),
                    size = selected.size(),
                    "failed to decode selected store record"
                );
            }
        }
        Some(decoded)
    } else {
        None
    };

    Some(ReadOutcome::new(
        status,
        canonical.next_begin_offset,
        canonical.min_offset,
        canonical.max_offset,
        records,
    ))
}
