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

use rocketmq_store::base::compaction_append_msg_callback::CompactionAppendMsgCallback as LegacyCompactionCallback;
use rocketmq_store::base::message_result::AppendMessageResult as LegacyAppendMessageResult;
use rocketmq_store::base::message_status_enum::AppendMessageStatus as LegacyAppendMessageStatus;
use rocketmq_store::base::put_message_context::PutMessageContext as LegacyPutMessageContext;
use rocketmq_store::config::flush_disk_type::FlushDiskType as LegacyFlushDiskType;
use rocketmq_store_local::commit_log::append::AppendMessageResult;
use rocketmq_store_local::commit_log::append::AppendMessageStatus;
use rocketmq_store_local::commit_log::append::CompactionAppendMsgCallback;
use rocketmq_store_local::commit_log::append::PutMessageContext;
use rocketmq_store_local::config::FlushDiskType;

fn canonical_callback(value: &dyn CompactionAppendMsgCallback) -> &dyn CompactionAppendMsgCallback {
    value
}

fn legacy_callback(value: &dyn LegacyCompactionCallback) -> &dyn LegacyCompactionCallback {
    value
}

#[test]
fn legacy_store_paths_are_exact_reexports_of_local_canonical_types() {
    let status: LegacyAppendMessageStatus = AppendMessageStatus::PutOk;
    let _: AppendMessageStatus = status;

    let result: LegacyAppendMessageResult = AppendMessageResult::default();
    let _: AppendMessageResult = result;

    let context: LegacyPutMessageContext = PutMessageContext::default();
    let _: PutMessageContext = context;

    let flush: LegacyFlushDiskType = FlushDiskType::AsyncFlush;
    let _: FlushDiskType = flush;

    let _canonical_identity: for<'a> fn(&'a dyn LegacyCompactionCallback) -> &'a dyn CompactionAppendMsgCallback =
        canonical_callback;
    let _legacy_identity: for<'a> fn(&'a dyn CompactionAppendMsgCallback) -> &'a dyn LegacyCompactionCallback =
        legacy_callback;
}
