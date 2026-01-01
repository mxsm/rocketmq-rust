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

use std::fmt;

use crate::base::message_status_enum::GetMessageStatus;
use crate::base::select_result::SelectMappedBufferResult;

/// Represents the result of getting a message.
pub struct GetMessageResult {
    /// The list of mapped buffer results.
    message_mapped_list: Vec<SelectMappedBufferResult>,
    /// The list of message buffers.
    /* message_buffer_list: Vec<Bytes>, *//* Using Vec<u8> as a simplified representation of
     * ByteBuffer in Rust */
    /// The list of message queue offsets.
    message_queue_offset: Vec<u64>,
    /// The status of getting the message.
    status: Option<GetMessageStatus>,
    /// The next beginning offset. Consume queue from this offset.
    next_begin_offset: i64,
    /// The minimum offset of consume queue
    min_offset: i64,
    /// The maximum offset of consume queue
    max_offset: i64,
    /// The total size of buffers.
    buffer_total_size: i32,
    /// The count of messages.
    message_count: i32,
    /// Indicates whether pulling from a slave is suggested.
    suggest_pulling_from_slave: bool,
    /// The count of messages for commercial purposes.
    msg_count4_commercial: i32,
    /// The size per message for commercial purposes.
    commercial_size_per_msg: i32,
    /// The sum of cold data.
    cold_data_sum: i64,
}

impl Default for GetMessageResult {
    fn default() -> Self {
        Self {
            message_mapped_list: vec![],
            message_queue_offset: vec![],
            status: None,
            next_begin_offset: 0,
            min_offset: 0,
            max_offset: 0,
            buffer_total_size: 0,
            message_count: 0,
            suggest_pulling_from_slave: false,
            msg_count4_commercial: 0,
            commercial_size_per_msg: 4 * 1024,
            cold_data_sum: 0,
        }
    }
}

impl fmt::Display for GetMessageResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GetMessageResult [status={:?}, nextBeginOffset={}, minOffset={}, maxOffset={}, bufferTotalSize={}, \
             messageCount={}, suggestPullingFromSlave={}]",
            self.status,
            self.next_begin_offset,
            self.min_offset,
            self.max_offset,
            self.buffer_total_size,
            self.message_count,
            self.suggest_pulling_from_slave
        )
    }
}

impl GetMessageResult {
    #[inline]
    pub fn new() -> Self {
        GetMessageResult {
            message_mapped_list: Vec::with_capacity(100),
            // message_buffer_list: Vec::with_capacity(100),
            message_queue_offset: Vec::with_capacity(100),
            ..Default::default()
        }
    }

    #[inline]
    pub fn new_result_size(result_size: usize) -> Self {
        GetMessageResult {
            message_mapped_list: Vec::with_capacity(result_size),
            //  message_buffer_list: Vec::with_capacity(result_size),
            message_queue_offset: Vec::with_capacity(result_size),
            ..Default::default()
        }
    }

    fn new_with_params(
        status: GetMessageStatus,
        next_begin_offset: i64,
        min_offset: i64,
        max_offset: i64,
        message_mapped_list: Vec<SelectMappedBufferResult>,
        //message_buffer_list: Vec<ByteBuffer>,
        message_queue_offset: Vec<u64>,
    ) -> Self {
        GetMessageResult {
            status: Some(status),
            next_begin_offset,
            min_offset,
            max_offset,
            message_mapped_list,
            message_queue_offset,
            // Other fields would be initialized with default values
            buffer_total_size: 0,
            message_count: 0,
            suggest_pulling_from_slave: false,
            msg_count4_commercial: 0,
            commercial_size_per_msg: 4 * 1024,
            cold_data_sum: 0,
        }
    }
}

impl GetMessageResult {
    #[inline]
    pub fn status(&self) -> Option<GetMessageStatus> {
        self.status
    }

    #[inline]
    pub fn next_begin_offset(&self) -> i64 {
        self.next_begin_offset
    }

    #[inline]
    pub fn min_offset(&self) -> i64 {
        self.min_offset
    }

    #[inline]
    pub fn max_offset(&self) -> i64 {
        self.max_offset
    }

    #[inline]
    pub fn buffer_total_size(&self) -> i32 {
        self.buffer_total_size
    }

    #[inline]
    pub fn message_count(&self) -> i32 {
        self.message_count
    }

    #[inline]
    pub fn suggest_pulling_from_slave(&self) -> bool {
        self.suggest_pulling_from_slave
    }

    #[inline]
    pub fn msg_count4_commercial(&self) -> i32 {
        self.msg_count4_commercial
    }

    #[inline]
    pub fn commercial_size_per_msg(&self) -> i32 {
        self.commercial_size_per_msg
    }

    #[inline]
    pub fn cold_data_sum(&self) -> i64 {
        self.cold_data_sum
    }

    #[inline]
    pub fn set_message_mapped_list(&mut self, message_mapped_list: Vec<SelectMappedBufferResult>) {
        self.message_mapped_list = message_mapped_list;
    }

    /*
    #[inline]
    pub fn set_message_buffer_list(&mut self, message_buffer_list: Vec<Bytes>) {
        self.message_buffer_list = message_buffer_list;
    }*/

    #[inline]
    pub fn set_message_queue_offset(&mut self, message_queue_offset: Vec<u64>) {
        self.message_queue_offset = message_queue_offset;
    }

    #[inline]
    pub fn set_status(&mut self, status: Option<GetMessageStatus>) {
        self.status = status;
    }

    #[inline]
    pub fn set_next_begin_offset(&mut self, next_begin_offset: i64) {
        self.next_begin_offset = next_begin_offset;
    }

    #[inline]
    pub fn set_min_offset(&mut self, min_offset: i64) {
        self.min_offset = min_offset;
    }

    #[inline]
    pub fn set_max_offset(&mut self, max_offset: i64) {
        self.max_offset = max_offset;
    }

    #[inline]
    pub fn set_buffer_total_size(&mut self, buffer_total_size: i32) {
        self.buffer_total_size = buffer_total_size;
    }

    #[inline]
    pub fn set_message_count(&mut self, message_count: i32) {
        self.message_count = message_count;
    }
    #[inline]
    pub fn set_suggest_pulling_from_slave(&mut self, suggest_pulling_from_slave: bool) {
        self.suggest_pulling_from_slave = suggest_pulling_from_slave;
    }

    #[inline]
    pub fn set_msg_count4_commercial(&mut self, msg_count4_commercial: i32) {
        self.msg_count4_commercial = msg_count4_commercial;
    }

    #[inline]
    pub fn set_commercial_size_per_msg(&mut self, commercial_size_per_msg: i32) {
        self.commercial_size_per_msg = commercial_size_per_msg;
    }

    #[inline]
    pub fn set_cold_data_sum(&mut self, cold_data_sum: i64) {
        self.cold_data_sum = cold_data_sum;
    }

    #[inline]
    pub fn add_message(&mut self, mapped_buffer: SelectMappedBufferResult, queue_offset: u64, batch_num: i32) {
        self.buffer_total_size += mapped_buffer.size;
        self.message_count += batch_num;
        self.msg_count4_commercial += (mapped_buffer.size as f64 / self.commercial_size_per_msg as f64).ceil() as i32;
        self.message_queue_offset.push(queue_offset);
        self.message_mapped_list.push(mapped_buffer);
    }

    #[inline]
    pub fn add_message_inner(&mut self, mapped_buffer: SelectMappedBufferResult) {
        self.buffer_total_size += mapped_buffer.size;
        self.msg_count4_commercial += (mapped_buffer.size as f64 / self.commercial_size_per_msg as f64).ceil() as i32;
        self.message_count += 1;
        self.message_mapped_list.push(mapped_buffer);
    }

    #[inline]
    pub fn message_mapped_list(&self) -> &[SelectMappedBufferResult] {
        self.message_mapped_list.as_slice()
    }

    #[inline]
    pub fn message_mapped_list_mut(&mut self) -> &mut [SelectMappedBufferResult] {
        self.message_mapped_list.as_mut()
    }

    #[inline]
    pub fn message_mapped_vec(self) -> Vec<SelectMappedBufferResult> {
        self.message_mapped_list
    }

    #[inline]
    pub fn message_queue_offset(&self) -> &Vec<u64> {
        &self.message_queue_offset
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::base::message_status_enum::GetMessageStatus;
    #[test]
    fn get_message_result_new() {
        let result = GetMessageResult::new();
        assert_eq!(result.message_mapped_list.capacity(), 100);
        //assert_eq!(result.message_buffer_list.capacity(), 100);
        assert_eq!(result.message_queue_offset.capacity(), 100);
    }

    #[test]
    fn get_message_result_new_result_size() {
        let result = GetMessageResult::new_result_size(50);
        assert_eq!(result.message_mapped_list.capacity(), 50);
        //assert_eq!(result.message_buffer_list.capacity(), 50);
        assert_eq!(result.message_queue_offset.capacity(), 50);
    }

    #[test]
    fn get_message_result_setters() {
        let mut result = GetMessageResult::new();
        let buffer_list = vec![Bytes::from(vec![0u8; 10]); 10];
        let queue_offset = vec![0u64; 10];
        let status = Some(GetMessageStatus::Found);
        let next_begin_offset = 10;
        let min_offset = 0;
        let max_offset = 20;
        let buffer_total_size = 100;
        let message_count = 10;
        let suggest_pulling_from_slave = true;
        let msg_count4_commercial = 5;
        let commercial_size_per_msg = 20;
        let cold_data_sum = 500;

        //result.set_message_buffer_list(buffer_list);
        result.set_message_queue_offset(queue_offset);
        result.set_status(status);
        result.set_next_begin_offset(next_begin_offset);
        result.set_min_offset(min_offset);
        result.set_max_offset(max_offset);
        result.set_buffer_total_size(buffer_total_size);
        result.set_message_count(message_count);
        result.set_suggest_pulling_from_slave(suggest_pulling_from_slave);
        result.set_msg_count4_commercial(msg_count4_commercial);
        result.set_commercial_size_per_msg(commercial_size_per_msg);
        result.set_cold_data_sum(cold_data_sum);

        assert_eq!(result.message_mapped_list.len(), 0);
        // assert_eq!(result.message_buffer_list.len(), 10);
        assert_eq!(result.message_queue_offset.len(), 10);
        assert_eq!(result.status, status);
        assert_eq!(result.next_begin_offset, next_begin_offset);
        assert_eq!(result.min_offset, min_offset);
        assert_eq!(result.max_offset, max_offset);
        assert_eq!(result.buffer_total_size, buffer_total_size);
        assert_eq!(result.message_count, message_count);
        assert_eq!(result.suggest_pulling_from_slave, suggest_pulling_from_slave);
        assert_eq!(result.msg_count4_commercial, msg_count4_commercial);
        assert_eq!(result.commercial_size_per_msg, commercial_size_per_msg);
        assert_eq!(result.cold_data_sum, cold_data_sum);
    }
}
