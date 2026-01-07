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

use crate::base::message_result::AppendMessageResult;

/// Callback interface for compaction append message
pub trait CompactionAppendMsgCallback {
    /// Append messages during compaction
    ///
    /// # Arguments
    ///
    /// * `bb_dest` - The destination buffer to append to
    /// * `file_from_offset` - The offset of the file
    /// * `max_blank` - The maximum blank space
    /// * `bb_src` - The source buffer containing the message to be appended
    ///
    /// # Returns
    ///
    /// The result of the append operation
    fn do_append(
        &self,
        bb_dest: &mut bytes::Bytes,
        file_from_offset: i64,
        max_blank: i32,
        bb_src: &mut bytes::Bytes,
    ) -> AppendMessageResult;
}
