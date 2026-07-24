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
use crate::log_file::mapped_file::MappedFile;

pub struct QueryMessageResult {
    pub message_maped_list: Vec<SelectMappedBufferResult>,
    pub index_last_update_timestamp: i64,
    pub index_last_update_phyoffset: i64,
    pub buffer_total_size: i32,
    pub index_query_safe: bool,
    pub index_safe_phyoffset: i64,
    pub index_confirm_phyoffset: i64,
}

impl Default for QueryMessageResult {
    fn default() -> Self {
        Self {
            message_maped_list: Vec::new(),
            index_last_update_timestamp: 0,
            index_last_update_phyoffset: 0,
            buffer_total_size: 0,
            index_query_safe: true,
            index_safe_phyoffset: 0,
            index_confirm_phyoffset: 0,
        }
    }
}

impl QueryMessageResult {
    pub fn set_index_query_safety(&mut self, safe: bool, safe_phyoffset: i64, confirm_phyoffset: i64) {
        self.index_query_safe = safe;
        self.index_safe_phyoffset = safe_phyoffset.max(0);
        self.index_confirm_phyoffset = confirm_phyoffset.max(0);
    }

    pub fn get_message_data(&self) -> Option<Bytes> {
        if self.buffer_total_size <= 0 || self.message_maped_list.is_empty() {
            return None;
        }

        let mut bytes_mut = BytesMut::with_capacity(self.buffer_total_size as usize);
        for msg in self.message_maped_list.iter() {
            if let Some(bytes) = msg.get_bytes_ref() {
                bytes_mut.extend_from_slice(bytes.as_ref());
                continue;
            }
            let mapped_file = msg.mapped_file.as_ref()?;
            let data = mapped_file.get_bytes(msg.file_offset as usize, msg.size as usize)?;
            bytes_mut.extend_from_slice(data.as_ref());
        }
        Some(bytes_mut.freeze())
    }

    pub fn add_message(&mut self, result: SelectMappedBufferResult) {
        self.buffer_total_size += result.size;
        self.message_maped_list.push(result);
    }
}

#[cfg(test)]
mod tests {
    use super::QueryMessageResult;

    #[test]
    fn query_message_result_is_safe_by_default() {
        let result = QueryMessageResult::default();

        assert!(result.index_query_safe);
        assert_eq!(result.index_safe_phyoffset, 0);
        assert_eq!(result.index_confirm_phyoffset, 0);
    }

    #[test]
    fn set_index_query_safety_records_offsets() {
        let mut result = QueryMessageResult::default();

        result.set_index_query_safety(false, 128, 256);

        assert!(!result.index_query_safe);
        assert_eq!(result.index_safe_phyoffset, 128);
        assert_eq!(result.index_confirm_phyoffset, 256);
    }
}
