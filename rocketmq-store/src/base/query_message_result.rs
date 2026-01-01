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
use bytes::BytesMut;

use crate::base::select_result::SelectMappedBufferResult;

#[derive(Default)]
pub struct QueryMessageResult {
    pub message_maped_list: Vec<SelectMappedBufferResult>,
    pub index_last_update_timestamp: i64,
    pub index_last_update_phyoffset: i64,
    pub buffer_total_size: i32,
}

impl QueryMessageResult {
    pub fn get_message_data(&self) -> Option<Bytes> {
        if self.buffer_total_size <= 0 || self.message_maped_list.is_empty() {
            return None;
        }

        let mut bytes_mut = BytesMut::with_capacity(self.buffer_total_size as usize);
        for msg in self.message_maped_list.iter() {
            let data = &msg.mapped_file.as_ref().unwrap().get_mapped_file()
                [msg.start_offset as usize..(msg.start_offset + msg.size as u64) as usize];
            bytes_mut.extend_from_slice(data);
        }
        Some(bytes_mut.freeze())
    }

    pub fn add_message(&mut self, result: SelectMappedBufferResult) {
        self.buffer_total_size += result.size;
        self.message_maped_list.push(result);
    }
}
