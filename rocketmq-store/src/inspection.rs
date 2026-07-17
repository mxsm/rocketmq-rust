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

//! Compatibility entrypoints for inspecting Local CommitLog records.

pub use rocketmq_store_local::commit_log::record_parser::decode_commit_log_record;
pub use rocketmq_store_local::commit_log::record_parser::CommitLogRecord;
pub use rocketmq_store_local::commit_log::record_parser::CommitLogRecordBodyMode;
pub use rocketmq_store_local::commit_log::record_parser::CommitLogRecordChecksum;
pub use rocketmq_store_local::commit_log::record_parser::CommitLogRecordError;
pub use rocketmq_store_local::commit_log::record_parser::CommitLogRecordErrorKind;
pub use rocketmq_store_local::commit_log::record_parser::CommitLogRecordField;
pub use rocketmq_store_local::commit_log::record_parser::CommitLogRecordOutcome;
pub use rocketmq_store_local::commit_log::record_parser::CommitLogRecordVersion;
