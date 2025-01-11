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
use std::path::PathBuf;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_common::common::attribute::cq_type::CQType;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::dispatch_request::DispatchRequest;
use crate::base::select_result::SelectMappedBufferResult;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::base::swappable::Swappable;
use crate::config::message_store_config::MessageStoreConfig;
use crate::consume_queue::consume_queue_ext::CqExtUnit;
use crate::consume_queue::mapped_file_queue::MappedFileQueue;
use crate::filter::MessageFilter;
use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::MappedFile;
use crate::queue::consume_queue_ext::ConsumeQueueExt;
use crate::queue::queue_offset_operator::QueueOffsetOperator;
use crate::queue::ConsumeQueueTrait;
use crate::queue::CqUnit;
use crate::queue::FileQueueLifeCycle;
use crate::store::running_flags::RunningFlags;
use crate::store_path_config_helper::get_store_path_consume_queue_ext;

pub const CQ_STORE_UNIT_SIZE: i32 = 20;
pub const MSG_TAG_OFFSET_INDEX: i32 = 12;

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
#[derive(Clone)]
pub struct ConsumeQueue {
    message_store_config: Arc<MessageStoreConfig>,
    mapped_file_queue: MappedFileQueue,
    topic: CheetahString,
    queue_id: i32,
    store_path: CheetahString,
    mapped_file_size: i32,
    max_physic_offset: Arc<AtomicI64>,
    min_logic_offset: Arc<AtomicI64>,
    consume_queue_ext: Option<ConsumeQueueExt>,
    running_flags: Arc<RunningFlags>,
    store_checkpoint: Arc<StoreCheckpoint>,
}

impl ConsumeQueue {
    #[inline]
    pub fn new(
        topic: CheetahString,
        queue_id: i32,
        store_path: CheetahString,
        mapped_file_size: i32,
        message_store_config: Arc<MessageStoreConfig>,
        running_flags: Arc<RunningFlags>,
        store_checkpoint: Arc<StoreCheckpoint>,
    ) -> Self {
        let queue_dir = PathBuf::from(store_path.as_str())
            .join(topic.as_str())
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
                CheetahString::from_string(get_store_path_consume_queue_ext(
                    message_store_config.store_path_root_dir.as_str(),
                )),
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
            max_physic_offset: Arc::new(AtomicI64::new(-1)),
            min_logic_offset: Arc::new(AtomicI64::new(0)),
            consume_queue_ext,
            running_flags,
            store_checkpoint,
        }
    }
}

impl ConsumeQueue {
    #[inline]
    pub fn set_max_physic_offset(&self, max_physic_offset: i64) {
        self.max_physic_offset
            .store(max_physic_offset, std::sync::atomic::Ordering::SeqCst);
    }

    #[inline]
    pub fn truncate_dirty_logic_files_handler(&mut self, phy_offset: i64, delete_file: bool) {
        self.set_max_physic_offset(phy_offset);
        let mut max_ext_addr = 1i64;
        let mut should_delete_file = false;
        let mapped_file_size = self.mapped_file_size;
        loop {
            let mapped_file_option = self.mapped_file_queue.get_last_mapped_file();
            if mapped_file_option.is_none() {
                break;
            }
            let mapped_file = mapped_file_option.unwrap();
            mapped_file.set_wrote_position(0);
            mapped_file.set_committed_position(0);
            mapped_file.set_flushed_position(0);

            for index in 0..(mapped_file_size / CQ_STORE_UNIT_SIZE) {
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
                    if offset >= phy_offset {
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
                    if pos == mapped_file_size {
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
                    self.mapped_file_queue.delete_expired_file(vec![self
                        .mapped_file_queue
                        .get_last_mapped_file()
                        .unwrap()]);
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

    #[inline]
    pub fn is_ext_read_enable(&self) -> bool {
        self.consume_queue_ext.is_some()
    }

    #[inline]
    pub fn is_ext_addr(tags_code: i64) -> bool {
        ConsumeQueueExt::is_ext_addr(tags_code)
    }

    #[inline]
    pub fn is_ext_write_enable(&self) -> bool {
        self.consume_queue_ext.is_some() && self.message_store_config.enable_consume_queue_ext
    }

    #[inline]
    pub fn put_message_position_info(
        &mut self,
        offset: i64,
        size: i32,
        tags_code: i64,
        cq_offset: i64,
    ) -> bool {
        if offset + size as i64 <= self.get_max_physic_offset() {
            warn!(
                "Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}, \
                 size={}",
                self.get_max_physic_offset(),
                offset,
                size
            );
            return true;
        }
        let mut bytes = BytesMut::with_capacity(CQ_STORE_UNIT_SIZE as usize);
        bytes.put_i64(offset);
        bytes.put_i32(size);
        bytes.put_i64(tags_code);

        let expect_logic_offset = cq_offset * CQ_STORE_UNIT_SIZE as i64;
        if let Some(mapped_file) = self
            .mapped_file_queue
            .get_last_mapped_file_mut_start_offset(expect_logic_offset as u64, true)
        {
            if mapped_file.is_first_create_in_queue()
                && cq_offset != 0
                && mapped_file.get_wrote_position() == 0
            {
                self.min_logic_offset
                    .store(expect_logic_offset, Ordering::SeqCst);
                self.mapped_file_queue
                    .set_flushed_where(expect_logic_offset);
                self.mapped_file_queue
                    .set_committed_where(expect_logic_offset);
                self.fill_pre_blank(&mapped_file, expect_logic_offset);
                info!(
                    "fill pre blank space {} {}",
                    mapped_file.get_file_name(),
                    mapped_file.get_wrote_position()
                );
            }

            if cq_offset != 0 {
                let current_logic_offset = mapped_file.get_wrote_position() as i64
                    + mapped_file.get_file_from_offset() as i64;

                if expect_logic_offset < current_logic_offset {
                    warn!(
                        "Build  consume queue repeatedly, expectLogicOffset: {} \
                         currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expect_logic_offset,
                        current_logic_offset,
                        self.topic,
                        self.queue_id,
                        expect_logic_offset - current_logic_offset
                    );
                    return true;
                }

                if expect_logic_offset != current_logic_offset {
                    warn!(
                        "[BUG]logic queue order maybe wrong, expectLogicOffset: {} \
                         currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expect_logic_offset,
                        current_logic_offset,
                        self.topic,
                        self.queue_id,
                        expect_logic_offset - current_logic_offset
                    );
                }
            }
            self.set_max_physic_offset(offset + size as i64);
            mapped_file.append_message_bytes(&bytes.freeze())
        } else {
            false
        }
    }

    #[inline]
    fn fill_pre_blank(&self, mapped_file: &Arc<DefaultMappedFile>, until_where: i64) {
        let mut bytes_mut = BytesMut::with_capacity(CQ_STORE_UNIT_SIZE as usize);

        bytes_mut.put_i64(0);
        bytes_mut.put_i32(i32::MAX);
        bytes_mut.put_i64(0);
        let bytes = bytes_mut.freeze();
        let until = (until_where % self.mapped_file_queue.mapped_file_size as i64) as i32
            / CQ_STORE_UNIT_SIZE;
        for n in 0..until {
            mapped_file.append_message_bytes(&bytes);
        }
    }

    #[inline]
    pub fn get_index_buffer(&self, start_index: i64) -> Option<SelectMappedBufferResult> {
        let mapped_file_size = self.mapped_file_size;
        let offset = start_index * CQ_STORE_UNIT_SIZE as i64;
        if offset >= self.get_min_logic_offset() {
            if let Some(mapped_file) = self
                .mapped_file_queue
                .find_mapped_file_by_offset(offset, false)
            {
                let mut result = mapped_file
                    .select_mapped_buffer_with_position((offset % mapped_file_size as i64) as i32);
                if let Some(ref mut result) = result {
                    result.mapped_file = Some(mapped_file);
                }
                return result;
            }
        }
        None
    }
}

impl FileQueueLifeCycle for ConsumeQueue {
    #[inline]
    fn load(&mut self) -> bool {
        let mut result = self.mapped_file_queue.load();
        info!(
            "load consume queue {}-{}  {}",
            self.topic,
            self.queue_id,
            if result { "OK" } else { "Failed" }
        );
        if self.is_ext_read_enable() {
            result &= self.consume_queue_ext.as_mut().unwrap().load();
        }
        result
    }

    #[inline]
    fn recover(&mut self) {
        let binding = self.mapped_file_queue.get_mapped_files();
        let mapped_files = binding.read();
        if mapped_files.is_empty() {
            return;
        }
        let mut index = (mapped_files.len()) as i32 - 3;
        if index < 0 {
            index = 0;
        }
        let mut index = index as usize;
        let mapped_file_size_logics = self.mapped_file_size;
        let mut mapped_file = mapped_files.get(index).unwrap();
        let mut process_offset = mapped_file.get_file_from_offset();
        let mut mapped_file_offset = 0i64;
        let mut max_ext_addr = 1i64;
        loop {
            for index in 0..(mapped_file_size_logics / CQ_STORE_UNIT_SIZE) {
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
                if offset >= 0 && size > 0 {
                    mapped_file_offset =
                        (index * CQ_STORE_UNIT_SIZE) as i64 + CQ_STORE_UNIT_SIZE as i64;
                    self.set_max_physic_offset(offset + size as i64);
                    if Self::is_ext_addr(tags_code) {
                        max_ext_addr = tags_code;
                    }
                    //println!("offset {}, size {}, tags_code {}", offset, size, tags_code);
                } else {
                    info!(
                        "recover current consume queue file over,  {}, {} {} {}",
                        mapped_file.get_file_name(),
                        offset,
                        size,
                        tags_code
                    );
                    break;
                }
            }
            if mapped_file_offset == mapped_file_size_logics as i64 {
                index += 1;
                if index >= mapped_files.len() {
                    info!(
                        "recover last consume queue file over, last mapped file {}",
                        mapped_file.get_file_name()
                    );
                    break;
                } else {
                    mapped_file = mapped_files.get(index).unwrap();
                    process_offset = mapped_file.get_file_from_offset();
                    mapped_file_offset = 0;
                    info!(
                        "recover next consume queue file, {}",
                        mapped_file.get_file_name()
                    );
                }
            } else {
                info!(
                    "recover current consume queue file over, {} {}",
                    mapped_file.get_file_name(),
                    process_offset + (mapped_file_offset as u64),
                );
                break;
            }
        }
        process_offset += mapped_file_offset as u64;
        self.mapped_file_queue
            .set_flushed_where(process_offset as i64);
        self.mapped_file_queue
            .set_committed_where(process_offset as i64);
        self.mapped_file_queue
            .truncate_dirty_files(process_offset as i64);

        if self.is_ext_read_enable() {
            let consume_queue_ext = self.consume_queue_ext.as_mut().unwrap();
            consume_queue_ext.recover();
            info!("Truncate consume queue extend file by max {}", max_ext_addr);
            consume_queue_ext.truncate_by_max_address(max_ext_addr);
        }
    }

    #[inline]
    fn check_self(&self) {
        todo!()
    }

    #[inline]
    fn flush(&self, flush_least_pages: i32) -> bool {
        todo!()
    }

    #[inline]
    fn destroy(&mut self) {
        self.set_max_physic_offset(-1);
        self.min_logic_offset.store(0, Ordering::SeqCst);
        self.mapped_file_queue.destroy();
        if self.is_ext_read_enable() {
            self.consume_queue_ext.as_mut().unwrap().destroy();
        }
    }

    #[inline]
    fn truncate_dirty_logic_files(&mut self, max_commit_log_pos: i64) {
        self.truncate_dirty_logic_files_handler(max_commit_log_pos, true);
    }

    #[inline]
    fn delete_expired_file(&self, min_commit_log_pos: i64) -> i32 {
        todo!()
    }

    #[inline]
    fn roll_next_file(&self, next_begin_offset: i64) -> i64 {
        todo!()
    }

    #[inline]
    fn is_first_file_available(&self) -> bool {
        todo!()
    }

    #[inline]
    fn is_first_file_exist(&self) -> bool {
        todo!()
    }
}

impl Swappable for ConsumeQueue {
    #[inline]
    fn swap_map(
        &self,
        reserve_num: i32,
        force_swap_interval_ms: i64,
        normal_swap_interval_ms: i64,
    ) {
        todo!()
    }

    #[inline]
    fn clean_swapped_map(&self, _force_clean_swap_interval_ms: i64) {
        todo!()
    }
}

#[allow(unused_variables)]
impl ConsumeQueueTrait for ConsumeQueue {
    #[inline]
    fn get_topic(&self) -> &CheetahString {
        &self.topic
    }

    #[inline]
    fn get_queue_id(&self) -> i32 {
        self.queue_id
    }

    #[inline]
    fn get(&self, index: i64) -> Option<CqUnit> {
        match self.iterate_from(index) {
            None => None,
            Some(value) => None,
        }
    }

    #[inline]
    fn get_cq_unit_and_store_time(&self, index: i64) -> Option<(CqUnit, i64)> {
        todo!()
    }

    #[inline]
    fn get_earliest_unit_and_store_time(&self) -> Option<(CqUnit, i64)> {
        todo!()
    }

    #[inline]
    fn get_earliest_unit(&self) -> CqUnit {
        todo!()
    }

    #[inline]
    fn get_latest_unit(&self) -> CqUnit {
        todo!()
    }

    #[inline]
    fn get_last_offset(&self) -> i64 {
        todo!()
    }

    #[inline]
    fn get_min_offset_in_queue(&self) -> i64 {
        self.min_logic_offset.load(Ordering::Acquire) / CQ_STORE_UNIT_SIZE as i64
    }

    #[inline]
    fn get_max_offset_in_queue(&self) -> i64 {
        self.mapped_file_queue.get_max_offset() / CQ_STORE_UNIT_SIZE as i64
    }

    #[inline]
    fn get_message_total_in_queue(&self) -> i64 {
        todo!()
    }

    #[inline]
    fn get_offset_in_queue_by_time(&self, timestamp: i64) -> i64 {
        todo!()
    }

    #[inline]
    fn get_offset_in_queue_by_time_boundary(
        &self,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64 {
        todo!()
    }

    #[inline]
    fn get_max_physic_offset(&self) -> i64 {
        self.max_physic_offset.load(Ordering::SeqCst)
    }

    #[inline]
    fn get_min_logic_offset(&self) -> i64 {
        self.min_logic_offset.load(Ordering::Relaxed)
    }

    #[inline]
    fn get_cq_type(&self) -> CQType {
        CQType::SimpleCQ
    }

    #[inline]
    fn get_total_size(&self) -> i64 {
        todo!()
    }

    #[inline]
    fn get_unit_size(&self) -> i32 {
        CQ_STORE_UNIT_SIZE
    }

    #[inline]
    fn correct_min_offset(&self, min_commit_log_offset: i64) {
        if min_commit_log_offset >= self.mapped_file_queue.get_max_offset() {
            info!(
                "ConsumeQueue[Topic={}, queue-id={}] contains no valid entries",
                self.topic, self.queue_id
            );
            return;
        }
        // Check whether the consume queue maps no valid data at all. This check may cost 1 IO
        // operation. The rationale is that consume queue always preserves the last file. In
        // case there are many deprecated topics, This check would save a lot of efforts.
        let last_mapped_file = self.mapped_file_queue.get_last_mapped_file();
        if last_mapped_file.is_none() {
            return;
        }
        let last_mapped_file = last_mapped_file.unwrap();
        let max_readable_position = last_mapped_file.get_read_position();
        let mut last_record = last_mapped_file.select_mapped_buffer(
            max_readable_position - CQ_STORE_UNIT_SIZE,
            CQ_STORE_UNIT_SIZE,
        );
        if let Some(ref mut result) = last_record {
            result.mapped_file = Some(last_mapped_file.clone());
        }
        if let Some(last_record) = last_record {
            let mut bytes = last_record
                .mapped_file
                .as_ref()
                .unwrap()
                .get_bytes(last_record.start_offset as usize, last_record.size as usize)
                .unwrap();
            let commit_log_offset = bytes.get_i64();
            if commit_log_offset < min_commit_log_offset {
                self.min_logic_offset.store(
                    max_readable_position as i64 + last_mapped_file.get_file_from_offset() as i64,
                    Ordering::SeqCst,
                );
                info!(
                    "ConsumeQueue[topic={}, queue-id={}] contains no valid entries. Min-offset is \
                     assigned as: {}.",
                    self.topic,
                    self.queue_id,
                    self.get_min_offset_in_queue()
                );
                return;
            }
        }

        let mapped_file = self.mapped_file_queue.get_first_mapped_file();
        let mut min_ext_addr = 1i64;
        if let Some(mapped_file) = mapped_file {
            // Search from previous min logical offset. Typically, a consume queue file segment
            // contains 300,000 entries searching from previous position saves
            // significant amount of comparisons and IOs
            let mut intact = true; // Assume previous value is still valid
            let mut start = self.min_logic_offset.load(Ordering::Acquire)
                - mapped_file.get_file_from_offset() as i64;
            if start < 0 {
                intact = false;
                start = 0;
            }
            if start > mapped_file.get_file_size() as i64 {
                error!(
                    "[Bug][InconsistentState] ConsumeQueue file {} should have been deleted",
                    mapped_file.get_file_name()
                );
                return;
            }
            let result = mapped_file.select_mapped_buffer_with_position(start as i32);
            if result.is_none() {
                warn!(
                    "[Bug] Failed to scan consume queue entries from file on correcting min \
                     offset: {}",
                    mapped_file.get_file_name()
                );
                return;
            }
            let mut result = result.unwrap();
            result.mapped_file = Some(mapped_file);
            if result.size == 0 {
                debug!(
                    "ConsumeQueue[topic={}, queue-id={}] contains no valid entries",
                    self.topic, self.queue_id
                );
                return;
            }
            let mapped = result.mapped_file.as_ref().unwrap();
            let commit_log_offset = mapped
                .get_bytes(result.start_offset as usize, 8)
                .unwrap()
                .get_i64();
            if intact && commit_log_offset >= min_commit_log_offset {
                info!(
                    "Abort correction as previous min-offset points to {}, which is greater than \
                     {}",
                    commit_log_offset, min_commit_log_offset
                );
                return;
            }
            let mut low = 0;
            let mut high = result.size - CQ_STORE_UNIT_SIZE;
            loop {
                if high - low <= CQ_STORE_UNIT_SIZE {
                    break;
                }
                let mid = (low + high) / 2 / CQ_STORE_UNIT_SIZE * CQ_STORE_UNIT_SIZE;
                let commit_log_offset = mapped.get_bytes(mid as usize, 8).unwrap().get_i64();

                match commit_log_offset.cmp(&min_commit_log_offset) {
                    std::cmp::Ordering::Greater => high = mid,
                    std::cmp::Ordering::Equal => {
                        low = mid;
                        high = mid;
                        break;
                    }
                    std::cmp::Ordering::Less => low = mid,
                }
            }
            let mut i = low;
            while i <= high {
                let offset_py = mapped.get_bytes(i as usize, 8).unwrap().get_i64();
                let tags_code = mapped.get_bytes((i + 12) as usize, 8).unwrap().get_i64();
                if offset_py >= min_commit_log_offset {
                    self.min_logic_offset.store(
                        mapped.get_file_from_offset() as i64 + i as i64 + start,
                        Ordering::SeqCst,
                    );
                    if Self::is_ext_addr(tags_code) {
                        min_ext_addr = tags_code;
                    }
                    break;
                }
                i += CQ_STORE_UNIT_SIZE;
            }
        }

        if self.is_ext_read_enable() {
            self.consume_queue_ext
                .as_ref()
                .unwrap()
                .truncate_by_min_address(min_ext_addr);
        }
    }

    #[inline]
    fn put_message_position_info_wrapper(&mut self, request: &DispatchRequest) {
        let max_retries = 30i32;
        let can_write = self.running_flags.is_cq_writeable();
        let mut i = 0i32;
        while i < max_retries && can_write {
            let mut tags_code = request.tags_code;
            if self.is_ext_write_enable() {
                let ext_addr = self.consume_queue_ext.as_ref().unwrap().put(CqExtUnit::new(
                    tags_code,
                    request.store_timestamp,
                    request.bit_map.clone(),
                ));

                if Self::is_ext_addr(ext_addr) {
                    tags_code = ext_addr;
                } else {
                    warn!(
                        "Save consume queue extend fail, So just save tagsCode!  topic:{}, \
                         queueId:{}, offset:{}",
                        self.topic, self.queue_id, request.commit_log_offset,
                    )
                }
            }
            if self.put_message_position_info(
                request.commit_log_offset,
                request.msg_size,
                tags_code,
                request.consume_queue_offset,
            ) {
                if self.message_store_config.broker_role == BrokerRole::Slave
                    || self.message_store_config.enable_dledger_commit_log
                {
                    unimplemented!("slave or dledger commit log not support")
                }
                self.store_checkpoint
                    .set_logics_msg_timestamp(request.store_timestamp as u64);
                //if (MultiDispatchUtils.checkMultiDispatchQueue(this.messageStore.
                // getMessageStoreConfig(), request)) {
                // multiDispatchLmqQueue(request, maxRetries);                 }
                return;
            } else {
                warn!(
                    "[BUG]put commit log position info to {}:{} failed, retry {} times",
                    self.topic, self.queue_id, i
                );
            }
            i += 1;
        }
        error!(
            "[BUG]consume queue can not write, {} {}",
            self.topic, self.queue_id
        );
        self.running_flags.make_logics_queue_error();
    }

    #[inline]
    fn increase_queue_offset(
        &self,
        queue_offset_assigner: &QueueOffsetOperator,
        msg: &MessageExtBrokerInner,
        message_num: i16,
    ) {
        queue_offset_assigner.increase_queue_offset(
            CheetahString::from_string(format!("{}-{}", msg.topic(), msg.queue_id())),
            message_num,
        );
    }

    #[inline]
    fn assign_queue_offset(
        &self,
        queue_offset_operator: &QueueOffsetOperator,
        msg: &mut MessageExtBrokerInner,
    ) {
        let queue_offset = queue_offset_operator.get_queue_offset(CheetahString::from_string(
            format!("{}-{}", msg.topic(), msg.queue_id()),
        ));
        msg.message_ext_inner.queue_offset = queue_offset;
    }

    #[inline]
    fn estimate_message_count(&self, from: i64, to: i64, filter: &dyn MessageFilter) -> i64 {
        todo!()
    }

    #[inline]
    fn iterate_from(&self, start_index: i64) -> Option<Box<dyn Iterator<Item = CqUnit>>> {
        match self.get_index_buffer(start_index) {
            None => None,
            Some(value) => Some(Box::new(ConsumeQueueIterator {
                smbr: Some(value),
                relative_pos: 0,
                counter: 0,
                consume_queue_ext: self.consume_queue_ext.clone(),
            })),
        }
    }

    #[inline]
    fn iterate_from_inner(
        &self,
        start_index: i64,
        _count: i32,
    ) -> Option<Box<dyn Iterator<Item = CqUnit>>> {
        self.iterate_from(start_index)
    }
}

struct ConsumeQueueIterator {
    smbr: Option<SelectMappedBufferResult>,
    relative_pos: i32,
    counter: i32,
    consume_queue_ext: Option<ConsumeQueueExt>,
}

impl ConsumeQueueIterator {
    fn get_ext(&self, offset: i64, cq_ext_unit: &CqExtUnit) -> bool {
        match self.consume_queue_ext.as_ref() {
            None => false,
            Some(value) => value.get(offset, cq_ext_unit),
        }
    }
}

impl Iterator for ConsumeQueueIterator {
    type Item = CqUnit;

    fn next(&mut self) -> Option<Self::Item> {
        match self.smbr.as_ref() {
            None => None,
            Some(value) => {
                if self.counter * CQ_STORE_UNIT_SIZE >= value.size {
                    return None;
                }
                let mmp = value.mapped_file.as_ref().unwrap().get_mapped_file();
                let start =
                    value.start_offset as usize + (self.counter * CQ_STORE_UNIT_SIZE) as usize;
                self.counter += 1;
                let end = start + CQ_STORE_UNIT_SIZE as usize;
                let mut bytes = Bytes::copy_from_slice(&mmp[start..end]);
                let pos = bytes.get_i64();
                let size = bytes.get_i32();
                let tags_code = bytes.get_i64();
                let mut cq_unit = CqUnit {
                    queue_offset: start as i64 / CQ_STORE_UNIT_SIZE as i64,
                    size,
                    pos,
                    tags_code,
                    ..CqUnit::default()
                };

                if ConsumeQueueExt::is_ext_addr(cq_unit.tags_code) {
                    let cq_ext_unit = CqExtUnit::default();
                    let ext_ret = self.get_ext(cq_unit.tags_code, &cq_ext_unit);
                    if ext_ret {
                        cq_unit.tags_code = cq_ext_unit.tags_code();
                        cq_unit.cq_ext_unit = Some(cq_ext_unit);
                    } else {
                        error!(
                            "[BUG] can't find consume queue extend file content! addr={}, \
                             offsetPy={}, sizePy={}",
                            cq_unit.tags_code, cq_unit.pos, cq_unit.pos,
                        );
                    }
                }
                Some(cq_unit)
            }
        }
    }
}
