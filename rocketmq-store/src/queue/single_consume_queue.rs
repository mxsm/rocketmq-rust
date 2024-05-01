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
use std::{path::PathBuf, sync::Arc};

use bytes::Buf;
use rocketmq_common::common::{
    attribute::cq_type::CQType, boundary_type::BoundaryType,
    message::message_single::MessageExtBrokerInner,
};

use crate::{
    base::{dispatch_request::DispatchRequest, swappable::Swappable},
    config::message_store_config::MessageStoreConfig,
    consume_queue::mapped_file_queue::MappedFileQueue,
    filter::MessageFilter,
    queue::{
        consume_queue_ext::ConsumeQueueExt, queue_offset_operator::QueueOffsetOperator,
        ConsumeQueueTrait, CqUnit, FileQueueLifeCycle,
    },
    store_path_config_helper::get_store_path_consume_queue_ext,
};

///
/// ConsumeQueue's store unit. Format:
///
/// ┌───────────────────────────────┬───────────────────┬───────────────────────────────┐
/// │    CommitLog Physical Offset  │      Body Size    │            Tag HashCode       │
/// │          (8 Bytes)            │      (4 Bytes)    │             (8 Bytes)         │
/// ├───────────────────────────────┴───────────────────┴───────────────────────────────┤
/// │                                     Store Unit                                    │
/// │                                                                                   │
/// </pre>
/// ConsumeQueue's store unit. Size: CommitLog Physical Offset(8) + Body Size(4) + Tag HashCode(8) =
/// 20 Bytes

pub const CQ_STORE_UNIT_SIZE: i32 = 20;
pub const MSG_TAG_OFFSET_INDEX: i32 = 12;

#[derive(Clone)]
pub struct ConsumeQueue {
    message_store_config: Arc<MessageStoreConfig>,
    mapped_file_queue: MappedFileQueue,
    topic: String,
    queue_id: i32,
    store_path: String,
    mapped_file_size: i32,
    max_physic_offset: Arc<parking_lot::Mutex<i64>>,
    min_logic_offset: Arc<parking_lot::Mutex<i64>>,
    consume_queue_ext: Option<ConsumeQueueExt>,
}

impl ConsumeQueue {
    pub fn new(
        topic: String,
        queue_id: i32,
        store_path: String,
        mapped_file_size: i32,
        message_store_config: Arc<MessageStoreConfig>,
    ) -> Self {
        let queue_dir = PathBuf::from(store_path.clone())
            .join(topic.clone())
            .join(queue_id.to_string());
        let mapped_file_queue = MappedFileQueue::new(
            queue_dir.to_string_lossy().to_string(),
            mapped_file_size as u64,
            None,
        );
        let consume_queue_ext = if message_store_config.enable_consume_queue_ext {
            Some(ConsumeQueueExt::new(
                topic.clone(),
                queue_id,
                get_store_path_consume_queue_ext(message_store_config.store_path_root_dir.as_str()),
                message_store_config.mapped_file_size_consume_queue_ext as i32,
                message_store_config.bit_map_length_consume_queue_ext as i32,
            ))
        } else {
            None
        };
        Self {
            message_store_config,
            mapped_file_queue,
            topic,
            queue_id,
            store_path,
            mapped_file_size,
            max_physic_offset: Arc::new(parking_lot::Mutex::new(-1)),
            min_logic_offset: Arc::new(parking_lot::Mutex::new(0)),
            consume_queue_ext,
        }
    }

    pub fn is_ext_addr(tags_code: i64) -> bool {
        ConsumeQueueExt::is_ext_addr(tags_code)
    }
}

impl ConsumeQueue {
    pub fn set_max_physic_offset(&self, max_physic_offset: i64) {
        *self.max_physic_offset.lock() = max_physic_offset;
    }

    pub fn truncate_dirty_logic_files_handler(&self, phy_offset: i64, delete_file: bool) {
        self.set_max_physic_offset(phy_offset);
        let mut max_ext_addr = -1i64;
        let mut should_delete_file = false;

        loop {
            let mapped_file_option = self.mapped_file_queue.get_last_mapped_file();
            if mapped_file_option.is_none() {
                break;
            }
            let mapped_file = mapped_file_option.unwrap();
            mapped_file.set_wrote_position(0);
            mapped_file.set_committed_position(0);
            mapped_file.set_flushed_position(0);

            for index in 0..(self.mapped_file_size / CQ_STORE_UNIT_SIZE) {
                let bytes_option = mapped_file.get_bytes(
                    (index * CQ_STORE_UNIT_SIZE) as usize,
                    CQ_STORE_UNIT_SIZE as usize,
                );
                if bytes_option.is_none() {
                    break;
                }
                let mut byte_buffer = bytes_option.unwrap();
                let offset = byte_buffer.get_i64();
                let size = byte_buffer.get_i32();
                let tags_code = byte_buffer.get_i64();
                if 0 == index {
                    if offset > phy_offset {
                        should_delete_file = true;
                        break;
                    } else {
                        let pos = index * CQ_STORE_UNIT_SIZE + CQ_STORE_UNIT_SIZE;
                        mapped_file.set_wrote_position(pos);
                        mapped_file.set_committed_position(pos);
                        mapped_file.set_flushed_position(pos);
                        self.set_max_physic_offset(offset + size as i64);
                        if Self::is_ext_addr(tags_code) {
                            max_ext_addr = tags_code;
                        }
                    }
                } else if offset >= 0 && size > 0 {
                    if offset >= phy_offset {
                        return;
                    }
                    let pos = index * CQ_STORE_UNIT_SIZE + CQ_STORE_UNIT_SIZE;
                    mapped_file.set_wrote_position(pos);
                    mapped_file.set_committed_position(pos);
                    mapped_file.set_flushed_position(pos);
                    self.set_max_physic_offset(offset + size as i64);
                    if Self::is_ext_addr(tags_code) {
                        max_ext_addr = tags_code;
                    }
                    if pos == self.mapped_file_size {
                        return;
                    }
                } else {
                    return;
                }
            }
            if should_delete_file {
                if delete_file {
                    self.mapped_file_queue.delete_last_mapped_file();
                } else {
                    self.mapped_file_queue
                        .delete_expired_file(vec![self.mapped_file_queue.get_last_mapped_file()]);
                }
            }
        }
        if self.is_ext_read_enable() {
            self.consume_queue_ext
                .as_ref()
                .unwrap()
                .truncate_by_max_address(max_ext_addr);
        }
    }

    pub fn is_ext_read_enable(&self) -> bool {
        self.consume_queue_ext.is_some()
    }
}

impl FileQueueLifeCycle for ConsumeQueue {
    fn load(&self) -> bool {
        todo!()
    }

    fn recover(&self) {
        println!("LocalFileConsumeQueue recover")
    }

    fn check_self(&self) {
        todo!()
    }

    fn flush(&self, flush_least_pages: i32) -> bool {
        todo!()
    }

    fn destroy(&self) {
        todo!()
    }

    fn truncate_dirty_logic_files(&self, max_commit_log_pos: i64) {
        self.truncate_dirty_logic_files_handler(max_commit_log_pos, true);
    }

    fn delete_expired_file(&self, min_commit_log_pos: i64) -> i32 {
        todo!()
    }

    fn roll_next_file(&self, next_begin_offset: i64) -> i64 {
        todo!()
    }

    fn is_first_file_available(&self) -> bool {
        todo!()
    }

    fn is_first_file_exist(&self) -> bool {
        todo!()
    }
}

impl Swappable for ConsumeQueue {
    fn swap_map(
        &self,
        reserve_num: i32,
        force_swap_interval_ms: i64,
        normal_swap_interval_ms: i64,
    ) {
        todo!()
    }

    fn clean_swapped_map(&self, _force_clean_swap_interval_ms: i64) {
        todo!()
    }
}

#[allow(unused_variables)]
impl ConsumeQueueTrait for ConsumeQueue {
    fn get_topic(&self) -> String {
        todo!()
    }

    fn get_queue_id(&self) -> i32 {
        todo!()
    }

    fn get(&self, index: i64) -> CqUnit {
        todo!()
    }

    fn get_earliest_unit(&self) -> CqUnit {
        todo!()
    }

    fn get_latest_unit(&self) -> CqUnit {
        todo!()
    }

    fn get_last_offset(&self) -> i64 {
        todo!()
    }

    fn get_min_offset_in_queue(&self) -> i64 {
        todo!()
    }

    fn get_max_offset_in_queue(&self) -> i64 {
        todo!()
    }

    fn get_message_total_in_queue(&self) -> i64 {
        todo!()
    }

    fn get_offset_in_queue_by_time(&self, timestamp: i64) -> i64 {
        todo!()
    }

    fn get_offset_in_queue_by_time_boundary(
        &self,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64 {
        todo!()
    }

    fn get_max_physic_offset(&self) -> i64 {
        todo!()
    }

    fn get_min_logic_offset(&self) -> i64 {
        todo!()
    }

    fn get_cq_type(&self) -> CQType {
        todo!()
    }

    fn get_total_size(&self) -> i64 {
        todo!()
    }

    fn get_unit_size(&self) -> i32 {
        todo!()
    }

    fn correct_min_offset(&self, min_commit_log_offset: i64) {
        todo!()
    }

    fn put_message_position_info_wrapper(&self, request: DispatchRequest) {
        todo!()
    }

    fn increase_queue_offset(
        &self,
        queue_offset_assigner: QueueOffsetOperator,
        msg: MessageExtBrokerInner,
        message_num: i16,
    ) {
        todo!()
    }

    fn estimate_message_count(&self, from: i64, to: i64, filter: &dyn MessageFilter) -> i64 {
        todo!()
    }
}
