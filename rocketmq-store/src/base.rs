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

use memmap2::MmapMut;

pub mod allocate_mapped_file_service;
pub mod append_message_callback;
pub mod commit_log_dispatcher;
pub mod compaction_append_msg_callback;
pub mod dispatch_request;
pub mod flush_manager;
pub mod get_message_result;
pub mod message_arriving_listener;
pub mod message_encoder_pool;
pub mod message_result;
pub mod message_status_enum;
pub mod message_store;
pub mod put_message_context;
pub mod query_message_result;
pub mod select_result;
pub mod store_checkpoint;
pub mod store_enum;
pub mod store_stats_service;
pub mod swappable;
pub mod topic_queue_lock;
pub mod transient_store_pool;

pub struct ByteBuffer<'a> {
    data: &'a mut MmapMut,
    position: i64,
}

impl<'a> ByteBuffer<'a> {
    pub fn new(data: &'a mut MmapMut, position: i64) -> ByteBuffer<'a> {
        ByteBuffer { data, position }
    }

    pub fn get_position(&self) -> i64 {
        self.position
    }

    pub fn set_position(&mut self, position: i64) {
        self.position = position;
    }

    pub fn get_data_mut(&mut self) -> &mut [u8] {
        self.data
    }
}
