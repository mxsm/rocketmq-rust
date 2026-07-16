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
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use tracing::error;
use tracing::warn;

use crate::base::append_message_callback::AppendMessageCallback;
use crate::base::compaction_append_msg_callback::CompactionAppendMsgCallback;
use crate::base::message_result::AppendMessageResult;
use crate::base::message_status_enum::AppendMessageStatus;
use crate::base::put_message_context::PutMessageContext;

pub mod default_mapped_file_impl;

mod builder;
mod memory;

pub use builder::MappedFileBuilder;
pub use memory::MmapRegionSlice;
pub use memory::StoreMappedMemory;
pub use rocketmq_store_local::mapped_file::io_uring_backend_status;
pub use rocketmq_store_local::mapped_file::io_uring_impl;
pub use rocketmq_store_local::mapped_file::DirectIoBuffer;
pub use rocketmq_store_local::mapped_file::DirectIoRequest;
pub use rocketmq_store_local::mapped_file::DirectIoValidationError;
pub use rocketmq_store_local::mapped_file::FlushStrategy;
pub use rocketmq_store_local::mapped_file::IoUringBackendStatus;
pub use rocketmq_store_local::mapped_file::MappedBuffer;
pub use rocketmq_store_local::mapped_file::MappedFile;
pub use rocketmq_store_local::mapped_file::MappedFileError;
pub use rocketmq_store_local::mapped_file::MappedFileMetrics;
pub use rocketmq_store_local::mapped_file::MappedFileResult;

/// Store message adapter layered over the runtime-neutral Local mapped-file contract.
pub trait MappedFileAppend: MappedFile {
    fn append_message<AMC: AppendMessageCallback>(
        &self,
        message: &mut MessageExtBrokerInner,
        message_callback: &AMC,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult
    where
        Self: Sized,
    {
        let current_pos = self.get_wrote_position() as u64;
        if current_pos >= self.get_file_size() {
            error!(
                "MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}",
                current_pos,
                self.get_file_size()
            );
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        }
        let result = message_callback.do_append(
            self.get_file_from_offset() as i64,
            self,
            (self.get_file_size() - current_pos) as i32,
            message,
            put_message_context,
        );
        self.record_append(result.wrote_bytes, result.store_timestamp as u64);
        result
    }

    fn append_messages<AMC: AppendMessageCallback>(
        &self,
        message: &mut MessageExtBatch,
        message_callback: &AMC,
        put_message_context: &mut PutMessageContext,
        enabled_append_prop_crc: bool,
    ) -> AppendMessageResult
    where
        Self: Sized,
    {
        let current_pos = self.get_wrote_position() as u64;
        if current_pos >= self.get_file_size() {
            error!(
                "MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}",
                current_pos,
                self.get_file_size()
            );
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        }
        let result = message_callback.do_append_batch(
            self.get_file_from_offset() as i64,
            self,
            (self.get_file_size() - current_pos) as i32,
            message,
            put_message_context,
            enabled_append_prop_crc,
        );
        self.record_append(result.wrote_bytes, result.store_timestamp as u64);
        result
    }

    fn append_messages_batch<AMC: AppendMessageCallback>(
        &self,
        messages: &mut [MessageExtBrokerInner],
        message_callback: &AMC,
        put_message_context: &PutMessageContext,
    ) -> Vec<AppendMessageResult>
    where
        Self: Sized,
    {
        messages
            .iter_mut()
            .map(|message| self.append_message(message, message_callback, put_message_context))
            .collect()
    }

    fn append_message_compaction(
        &mut self,
        byte_buffer_msg: &mut Bytes,
        _callback: &dyn CompactionAppendMsgCallback,
    ) -> AppendMessageResult {
        warn!(
            "append_message_compaction is not supported for mapped file: file={}, msg_len={}",
            self.get_file_name(),
            byte_buffer_msg.len()
        );
        AppendMessageResult {
            status: AppendMessageStatus::UnknownError,
            ..Default::default()
        }
    }
}

impl<T: MappedFile> MappedFileAppend for T {}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use tempfile::TempDir;

    use super::default_mapped_file_impl::DefaultMappedFile;
    use super::*;

    struct RejectingCompactionCallback;

    impl CompactionAppendMsgCallback for RejectingCompactionCallback {
        fn do_append(
            &self,
            bb_dest: &mut Bytes,
            file_from_offset: i64,
            max_blank: i32,
            bb_src: &mut Bytes,
        ) -> AppendMessageResult {
            let _ = (bb_dest, file_from_offset, max_blank, bb_src);
            AppendMessageResult::default()
        }
    }

    #[test]
    fn compaction_append_is_explicitly_rejected_by_store_adapter() {
        let temp_dir = TempDir::new().expect("temporary mapped-file directory");
        let file_path = temp_dir.path().join("00000000000000000000");
        let mut mapped_file =
            DefaultMappedFile::new(CheetahString::from(file_path.to_string_lossy().into_owned()), 4096);
        let mut message = Bytes::from_static(b"compaction-message");

        let result = mapped_file.append_message_compaction(&mut message, &RejectingCompactionCallback);

        assert_eq!(result.status, AppendMessageStatus::UnknownError);
        assert_eq!(result.wrote_bytes, 0);
    }
}
