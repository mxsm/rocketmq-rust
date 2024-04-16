/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::base::{
    message_status_enum::{AppendMessageStatus, GetMessageStatus, PutMessageStatus},
    select_result::SelectMappedBufferResult,
};

/// Represents the result of an append message operation.
pub struct AppendMessageResult {
    /// Return code.
    pub status: AppendMessageStatus,
    /// Where to start writing.
    pub wrote_offset: i64,
    /// Write Bytes.
    pub wrote_bytes: i32,
    /// Message ID.
    pub msg_id: String,
    /// Message ID supplier.
    pub msg_id_supplier: Box<dyn Fn() -> String>,
    /// Message storage timestamp.
    pub store_timestamp: i64,
    /// Consume queue's offset (step by one).
    pub logics_offset: i64,
    /// Page cache RT.
    pub page_cache_rt: i64,
    /// Message number.
    pub msg_num: i32,
}

pub struct PutMessageResult {
    put_message_status: PutMessageStatus,
    append_message_result: Option<AppendMessageResult>,
    remote_put: bool,
}

impl Default for AppendMessageResult {
    fn default() -> Self {
        unimplemented!()
    }
}

impl Default for PutMessageResult {
    fn default() -> Self {
        unimplemented!()
    }
}

impl PutMessageResult {
    pub fn new(
        put_message_status: PutMessageStatus,
        append_message_result: Option<AppendMessageResult>,
        remote_put: bool,
    ) -> Self {
        Self {
            put_message_status,
            append_message_result,
            remote_put,
        }
    }

    pub fn new_default(put_message_status: PutMessageStatus) -> Self {
        Self {
            put_message_status,
            append_message_result: None,
            remote_put: false,
        }
    }

    pub fn put_message_status(&self) -> PutMessageStatus {
        self.put_message_status
    }
    pub fn append_message_result(&self) -> Option<&AppendMessageResult> {
        self.append_message_result.as_ref()
    }
    pub fn remote_put(&self) -> bool {
        self.remote_put
    }
}

/// Represents the result of getting a message.
pub struct GetMessageResult<'a> {
    /// The list of mapped buffer results.
    pub message_mapped_list: Vec<SelectMappedBufferResult<'a>>,
    /// The list of message buffers.
    pub message_buffer_list: Vec<Vec<u8>>, /* Using Vec<u8> as a simplified representation of
                                            * ByteBuffer in Rust */
    /// The list of message queue offsets.
    pub message_queue_offset: Vec<i64>,
    /// The status of getting the message.
    pub status: GetMessageStatus,
    /// The next begin offset.
    pub next_begin_offset: i64,
    /// The minimum offset.
    pub min_offset: i64,
    /// The maximum offset.
    pub max_offset: i64,
    /// The total size of buffers.
    pub buffer_total_size: i32,
    /// The count of messages.
    pub message_count: i32,
    /// Indicates whether pulling from a slave is suggested.
    pub suggest_pulling_from_slave: bool,
    /// The count of messages for commercial purposes.
    pub msg_count4_commercial: i32,
    /// The size per message for commercial purposes.
    pub commercial_size_per_msg: i32,
    /// The sum of cold data.
    pub cold_data_sum: i64,
}
