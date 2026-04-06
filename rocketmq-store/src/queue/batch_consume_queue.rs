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

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Buf;
use bytes::BufMut;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_common::common::attribute::cq_type::CQType;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use tracing::info;
use tracing::warn;

use crate::base::dispatch_request::DispatchRequest;
use crate::base::swappable::Swappable;
use crate::config::message_store_config::MessageStoreConfig;
use crate::consume_queue::mapped_file_queue::MappedFileQueue;
use crate::filter::MessageFilter;
use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::MappedFile;
use crate::queue::consume_queue::ConsumeQueueTrait;
use crate::queue::queue_offset_operator::QueueOffsetOperator;
use crate::queue::CqUnit;
use crate::queue::FileQueueLifeCycle;

pub(crate) const CQ_STORE_UNIT_SIZE: i32 = 46;
const MSG_STORE_TIME_OFFSET_INDEX: i32 = 20;
const MSG_BASE_OFFSET_INDEX: i32 = 28;
const MSG_BATCH_SIZE_INDEX: i32 = 36;
const MSG_COMPACT_OFFSET_LENGTH: i32 = 4;
const INVALID_POS: i32 = -1;

#[derive(Clone, Debug)]
struct BatchQueueEntry {
    pos: i64,
    size: i32,
    tags_code: i64,
    store_time: i64,
    msg_base_offset: i64,
    batch_size: i16,
    compacted_offset: i32,
    raw: Vec<u8>,
}

#[derive(Clone, Copy, Debug)]
struct BatchRecordPosition {
    file_index: usize,
    position: i32,
}

pub struct BatchConsumeQueue {
    message_store_config: Arc<MessageStoreConfig>,
    mapped_file_queue: MappedFileQueue,
    topic: CheetahString,
    queue_id: i32,
    byte_buffer_item: Vec<u8>,
    store_path: CheetahString,
    mapped_file_size: usize,
    max_msg_phy_offset_in_commit_log: Arc<AtomicI64>,
    min_logic_offset: Arc<AtomicI64>,
    max_offset_in_queue: Arc<AtomicI64>,
    min_offset_in_queue: Arc<AtomicI64>,
    commit_log_size: i32,
    offset_cache: Arc<parking_lot::RwLock<BTreeMap<i64, DefaultMappedFile>>>,
    time_cache: Arc<parking_lot::RwLock<BTreeMap<i64, DefaultMappedFile>>>,
}

impl BatchConsumeQueue {
    #[inline]
    pub fn new(
        topic: CheetahString,
        queue_id: i32,
        store_path: CheetahString,
        mapped_file_size: usize,
        subfolder: Option<CheetahString>,
        message_store_config: Arc<MessageStoreConfig>,
    ) -> Self {
        let commit_log_size = message_store_config.mapped_file_size_commit_log;

        let mapped_file_queue = if let Some(subfolder) = subfolder {
            let queue_dir = PathBuf::from(store_path.as_str())
                .join(topic.as_str())
                .join(queue_id.to_string())
                .join(subfolder.as_str());
            MappedFileQueue::new(queue_dir.to_string_lossy().to_string(), mapped_file_size as u64, None)
        } else {
            let queue_dir = PathBuf::from(store_path.as_str())
                .join(topic.as_str())
                .join(queue_id.to_string());
            MappedFileQueue::new(queue_dir.to_string_lossy().to_string(), mapped_file_size as u64, None)
        };

        BatchConsumeQueue {
            message_store_config,
            mapped_file_queue,
            topic,
            queue_id,
            byte_buffer_item: vec![0u8; CQ_STORE_UNIT_SIZE as usize],
            store_path,
            mapped_file_size,
            max_msg_phy_offset_in_commit_log: Arc::new(AtomicI64::new(-1)),
            min_logic_offset: Arc::new(AtomicI64::new(-1)),
            max_offset_in_queue: Arc::new(AtomicI64::new(0)),
            min_offset_in_queue: Arc::new(AtomicI64::new(-1)),
            commit_log_size: commit_log_size as i32,
            offset_cache: Arc::new(parking_lot::RwLock::new(BTreeMap::new())),
            time_cache: Arc::new(parking_lot::RwLock::new(BTreeMap::new())),
        }
    }

    fn encode_unit(
        offset: i64,
        size: i32,
        tags_code: i64,
        store_time: i64,
        msg_base_offset: i64,
        batch_size: i16,
        compacted_offset: i32,
    ) -> Vec<u8> {
        let mut bytes = BytesMut::with_capacity(CQ_STORE_UNIT_SIZE as usize);
        bytes.put_i64(offset);
        bytes.put_i32(size);
        bytes.put_i64(tags_code);
        bytes.put_i64(store_time);
        bytes.put_i64(msg_base_offset);
        bytes.put_i16(batch_size);
        bytes.put_i32(compacted_offset);
        bytes.put_i32(0);
        bytes.to_vec()
    }

    fn read_entry(mapped_file: &Arc<DefaultMappedFile>, position: i32) -> Option<BatchQueueEntry> {
        if position < 0 || position + CQ_STORE_UNIT_SIZE > mapped_file.get_read_position() {
            return None;
        }

        let mut bytes = mapped_file.get_bytes(position as usize, CQ_STORE_UNIT_SIZE as usize)?;
        let raw = bytes.as_ref().to_vec();
        let pos = bytes.get_i64();
        let size = bytes.get_i32();
        let tags_code = bytes.get_i64();
        let store_time = bytes.get_i64();
        let msg_base_offset = bytes.get_i64();
        let batch_size = bytes.get_i16();
        let compacted_offset = bytes.get_i32();
        let _reserved = bytes.get_i32();

        if pos < 0 || size <= 0 || msg_base_offset < 0 || batch_size <= 0 {
            return None;
        }

        Some(BatchQueueEntry {
            pos,
            size,
            tags_code,
            store_time,
            msg_base_offset,
            batch_size,
            compacted_offset,
            raw,
        })
    }

    fn read_first_valid_entry(mapped_file: &Arc<DefaultMappedFile>) -> Option<(i32, BatchQueueEntry)> {
        let mut position = 0;
        while position + CQ_STORE_UNIT_SIZE <= mapped_file.get_read_position() {
            if let Some(entry) = Self::read_entry(mapped_file, position) {
                return Some((position, entry));
            }
            position += CQ_STORE_UNIT_SIZE;
        }
        None
    }

    fn read_last_valid_entry(mapped_file: &Arc<DefaultMappedFile>) -> Option<(i32, BatchQueueEntry)> {
        let mut position = mapped_file.get_read_position() - CQ_STORE_UNIT_SIZE;
        while position >= 0 {
            if let Some(entry) = Self::read_entry(mapped_file, position) {
                return Some((position, entry));
            }
            if position < CQ_STORE_UNIT_SIZE {
                break;
            }
            position -= CQ_STORE_UNIT_SIZE;
        }
        None
    }

    fn find_record_position_in_snapshot(
        files: &[Arc<DefaultMappedFile>],
        queue_offset: i64,
    ) -> Option<(BatchRecordPosition, BatchQueueEntry)> {
        let mut candidate: Option<(BatchRecordPosition, BatchQueueEntry)> = None;

        for (file_index, mapped_file) in files.iter().enumerate() {
            let mut position = 0;
            while position + CQ_STORE_UNIT_SIZE <= mapped_file.get_read_position() {
                let Some(entry) = Self::read_entry(mapped_file, position) else {
                    break;
                };
                if entry.msg_base_offset <= queue_offset {
                    candidate = Some((BatchRecordPosition { file_index, position }, entry));
                    position += CQ_STORE_UNIT_SIZE;
                    continue;
                }
                return candidate;
            }
        }

        candidate
    }

    fn find_record_position(&self, queue_offset: i64) -> Option<(Vec<Arc<DefaultMappedFile>>, BatchRecordPosition)> {
        let normalized_offset = if self.get_min_offset_in_queue() >= 0 {
            queue_offset.max(self.get_min_offset_in_queue())
        } else {
            queue_offset
        };
        let files = self.mapped_file_queue.snapshot();
        let (position, _) = Self::find_record_position_in_snapshot(&files, normalized_offset)?;
        Some((files, position))
    }

    fn entry_to_cq_unit(entry: BatchQueueEntry) -> CqUnit {
        CqUnit {
            queue_offset: entry.msg_base_offset,
            size: entry.size,
            pos: entry.pos,
            batch_num: entry.batch_size,
            tags_code: entry.tags_code,
            native_buffer: entry.raw,
            compacted_offset: entry.compacted_offset,
            ..CqUnit::default()
        }
    }

    fn revise_min_offset_in_queue(&self) {
        for mapped_file in self.mapped_file_queue.iter() {
            if let Some((position, entry)) = Self::read_first_valid_entry(&mapped_file) {
                self.min_logic_offset.store(
                    mapped_file.get_file_from_offset() as i64 + position as i64,
                    Ordering::Release,
                );
                self.min_offset_in_queue.store(entry.msg_base_offset, Ordering::Release);
                return;
            }
        }

        self.min_logic_offset.store(-1, Ordering::Release);
        self.min_offset_in_queue.store(-1, Ordering::Release);
    }

    fn revise_max_offset_in_queue(&self) {
        for mapped_file in self.mapped_file_queue.iter_reversed() {
            if let Some((_, entry)) = Self::read_last_valid_entry(&mapped_file) {
                self.max_offset_in_queue
                    .store(entry.msg_base_offset + entry.batch_size as i64, Ordering::Release);
                self.max_msg_phy_offset_in_commit_log
                    .store(entry.pos + entry.size as i64, Ordering::Release);
                return;
            }
        }

        self.max_offset_in_queue.store(0, Ordering::Release);
        self.max_msg_phy_offset_in_commit_log.store(-1, Ordering::Release);
    }

    fn revise_max_and_min_offset_in_queue(&self) {
        self.revise_min_offset_in_queue();
        self.revise_max_offset_in_queue();
    }

    fn put_batch_message_position_info(
        &mut self,
        offset: i64,
        size: i32,
        tags_code: i64,
        store_time: i64,
        msg_base_offset: i64,
        batch_size: i16,
    ) -> bool {
        if offset + size as i64 <= self.get_max_physic_offset() {
            warn!(
                "Build batch consume queue repeatedly, maxMsgPhyOffsetInCommitLog={} offset={} topic={} queue_id={}",
                self.get_max_physic_offset(),
                offset,
                self.topic,
                self.queue_id
            );
            return true;
        }

        let record = Self::encode_unit(
            offset,
            size,
            tags_code,
            store_time,
            msg_base_offset,
            batch_size,
            INVALID_POS,
        );
        let start_offset = self.mapped_file_queue.get_max_offset().max(0) as u64;
        let Some(mapped_file) = self
            .mapped_file_queue
            .get_last_mapped_file_mut_start_offset(start_offset, true)
        else {
            return false;
        };

        let is_new_file = mapped_file.get_read_position() < CQ_STORE_UNIT_SIZE;
        if mapped_file.append_message_bytes(&record) {
            self.max_msg_phy_offset_in_commit_log
                .store(offset + size as i64, Ordering::Release);
            self.max_offset_in_queue
                .store(msg_base_offset + batch_size as i64, Ordering::Release);
            if is_new_file && self.min_offset_in_queue.load(Ordering::Acquire) < 0 {
                self.min_logic_offset
                    .store(mapped_file.get_file_from_offset() as i64, Ordering::Release);
                self.min_offset_in_queue.store(msg_base_offset, Ordering::Release);
            }
            return true;
        }

        false
    }
}

impl FileQueueLifeCycle for BatchConsumeQueue {
    #[inline]
    fn load(&mut self) -> bool {
        let result = self.mapped_file_queue.load();
        info!(
            "Load batch consume queue {}-{} {} {}",
            self.topic,
            self.queue_id,
            if result { "OK" } else { "Failed" },
            self.mapped_file_queue.get_mapped_files_size()
        );
        result
    }

    #[inline]
    fn recover(&mut self) {
        let mapped_files = self.mapped_file_queue.snapshot();
        if mapped_files.is_empty() {
            self.revise_max_and_min_offset_in_queue();
            return;
        }

        let mut index = mapped_files.len().saturating_sub(3);
        let mut mapped_file = mapped_files[index].clone();
        let mut process_offset = mapped_file.get_file_from_offset();
        let mut mapped_file_offset;

        loop {
            let read_position = mapped_file.get_read_position();
            mapped_file_offset = 0;

            while mapped_file_offset + CQ_STORE_UNIT_SIZE <= read_position {
                let Some(entry) = Self::read_entry(&mapped_file, mapped_file_offset) else {
                    info!(
                        "Recover current batch consume queue file over, file={} mappedFileOffset={}",
                        mapped_file.get_file_name(),
                        mapped_file_offset
                    );
                    break;
                };

                mapped_file_offset += CQ_STORE_UNIT_SIZE;
                self.max_msg_phy_offset_in_commit_log
                    .store(entry.pos + entry.size as i64, Ordering::Release);
            }

            if mapped_file_offset == read_position {
                index += 1;
                if index >= mapped_files.len() {
                    info!(
                        "Recover last batch consume queue file over, last mapped file={}",
                        mapped_file.get_file_name()
                    );
                    break;
                }

                mapped_file = mapped_files[index].clone();
                process_offset = mapped_file.get_file_from_offset();
                continue;
            }

            info!(
                "Recover current batch consume queue file over, file={} processOffset={}",
                mapped_file.get_file_name(),
                process_offset + mapped_file_offset as u64
            );
            break;
        }

        process_offset += mapped_file_offset as u64;
        self.mapped_file_queue.set_flushed_where(process_offset as i64);
        self.mapped_file_queue.set_committed_where(process_offset as i64);
        self.mapped_file_queue.truncate_dirty_files(process_offset as i64);
        self.revise_max_and_min_offset_in_queue();
    }

    #[inline]
    fn check_self(&self) {
        self.mapped_file_queue.check_self();
    }

    #[inline]
    fn flush(&self, flush_least_pages: i32) -> bool {
        self.mapped_file_queue.flush(flush_least_pages)
    }

    #[inline]
    fn destroy(&mut self) {
        self.max_msg_phy_offset_in_commit_log.store(-1, Ordering::Release);
        self.min_logic_offset.store(-1, Ordering::Release);
        self.min_offset_in_queue.store(-1, Ordering::Release);
        self.max_offset_in_queue.store(0, Ordering::Release);
        self.mapped_file_queue.destroy();
        self.offset_cache.write().clear();
        self.time_cache.write().clear();
    }

    #[inline]
    fn truncate_dirty_logic_files(&mut self, max_commit_log_pos: i64) {
        loop {
            let Some(mapped_file) = self.mapped_file_queue.get_last_mapped_file() else {
                break;
            };

            mapped_file.set_wrote_position(0);
            mapped_file.set_committed_position(0);
            mapped_file.set_flushed_position(0);

            let mut remove_file = false;
            let mut stop = false;
            let mut position = 0;

            while position + CQ_STORE_UNIT_SIZE <= self.mapped_file_size as i32 {
                let Some(entry) = Self::read_entry(&mapped_file, position) else {
                    stop = true;
                    break;
                };

                if position == 0 && entry.pos >= max_commit_log_pos {
                    remove_file = true;
                    break;
                }

                if entry.pos >= max_commit_log_pos {
                    stop = true;
                    break;
                }

                position += CQ_STORE_UNIT_SIZE;
                mapped_file.set_wrote_position(position);
                mapped_file.set_committed_position(position);
                mapped_file.set_flushed_position(position);
                self.max_msg_phy_offset_in_commit_log
                    .store(entry.pos + entry.size as i64, Ordering::Release);

                if position == self.mapped_file_size as i32 {
                    stop = true;
                    break;
                }
            }

            if remove_file {
                self.mapped_file_queue.delete_last_mapped_file();
                continue;
            }

            if stop {
                break;
            }
        }

        self.revise_max_and_min_offset_in_queue();
    }

    #[inline]
    fn delete_expired_file(&self, min_commit_log_pos: i64) -> i32 {
        self.mapped_file_queue
            .delete_expired_file_by_offset(min_commit_log_pos, CQ_STORE_UNIT_SIZE)
    }

    #[inline]
    fn roll_next_file(&self, next_begin_offset: i64) -> i64 {
        let files = self.mapped_file_queue.snapshot();
        let Some((position, _)) = Self::find_record_position_in_snapshot(&files, next_begin_offset) else {
            return self.get_max_offset_in_queue();
        };

        let next_file_index = position.file_index + 1;
        if let Some(next_file) = files.get(next_file_index) {
            if let Some((_, entry)) = Self::read_first_valid_entry(next_file) {
                return entry.msg_base_offset;
            }
        }

        self.get_max_offset_in_queue()
    }

    #[inline]
    fn is_first_file_available(&self) -> bool {
        self.mapped_file_queue
            .get_first_mapped_file()
            .is_some_and(|mapped_file| mapped_file.is_available())
    }

    #[inline]
    fn is_first_file_exist(&self) -> bool {
        self.mapped_file_queue.get_first_mapped_file().is_some()
    }
}

impl Swappable for BatchConsumeQueue {
    #[inline]
    fn swap_map(&self, reserve_num: i32, force_swap_interval_ms: i64, normal_swap_interval_ms: i64) {
        self.mapped_file_queue
            .swap_map(reserve_num, force_swap_interval_ms, normal_swap_interval_ms);
    }

    #[inline]
    fn clean_swapped_map(&self, force_clean_swap_interval_ms: i64) {
        self.mapped_file_queue.clean_swapped_map(force_clean_swap_interval_ms);
    }
}

impl ConsumeQueueTrait for BatchConsumeQueue {
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
        if self.get_min_offset_in_queue() >= 0
            && (index < self.get_min_offset_in_queue() || index >= self.get_max_offset_in_queue())
        {
            return None;
        }

        let files = self.mapped_file_queue.snapshot();
        let (_, entry) = Self::find_record_position_in_snapshot(&files, index)?;
        Some(Self::entry_to_cq_unit(entry))
    }

    #[inline]
    fn get_cq_unit_and_store_time(&self, index: i64) -> Option<(CqUnit, i64)> {
        if self.get_min_offset_in_queue() >= 0
            && (index < self.get_min_offset_in_queue() || index >= self.get_max_offset_in_queue())
        {
            return None;
        }

        let files = self.mapped_file_queue.snapshot();
        let (_, entry) = Self::find_record_position_in_snapshot(&files, index)?;
        let store_time = entry.store_time;
        Some((Self::entry_to_cq_unit(entry), store_time))
    }

    #[inline]
    fn get_earliest_unit_and_store_time(&self) -> Option<(CqUnit, i64)> {
        let min_offset = self.get_min_offset_in_queue();
        if min_offset < 0 {
            return None;
        }
        self.get_cq_unit_and_store_time(min_offset)
    }

    #[inline]
    fn get_earliest_unit(&self) -> Option<CqUnit> {
        let min_offset = self.get_min_offset_in_queue();
        if min_offset < 0 {
            return None;
        }
        self.get(min_offset)
    }

    #[inline]
    fn get_latest_unit(&self) -> Option<CqUnit> {
        let max_offset = self.get_max_offset_in_queue();
        let min_offset = self.get_min_offset_in_queue();
        if min_offset < 0 || max_offset <= min_offset {
            return None;
        }
        self.get(max_offset - 1)
    }

    #[inline]
    fn get_last_offset(&self) -> i64 {
        self.get_latest_unit()
            .map(|cq_unit| cq_unit.pos + cq_unit.size as i64)
            .unwrap_or(-1)
    }

    #[inline]
    fn get_min_offset_in_queue(&self) -> i64 {
        self.min_offset_in_queue.load(Ordering::Acquire)
    }

    #[inline]
    fn get_max_offset_in_queue(&self) -> i64 {
        self.max_offset_in_queue.load(Ordering::Acquire)
    }

    #[inline]
    fn get_message_total_in_queue(&self) -> i64 {
        let min_offset = self.get_min_offset_in_queue();
        if min_offset < 0 {
            return 0;
        }
        self.get_max_offset_in_queue() - min_offset
    }

    #[inline]
    fn get_offset_in_queue_by_time(&self, timestamp: i64) -> i64 {
        self.get_offset_in_queue_by_time_with_boundary(timestamp, BoundaryType::Lower)
    }

    #[inline]
    fn get_max_physic_offset(&self) -> i64 {
        self.max_msg_phy_offset_in_commit_log.load(Ordering::Acquire)
    }

    #[inline]
    fn get_min_logic_offset(&self) -> i64 {
        self.min_logic_offset.load(Ordering::Acquire)
    }

    #[inline]
    fn get_cq_type(&self) -> CQType {
        CQType::BatchCQ
    }

    #[inline]
    fn get_total_size(&self) -> i64 {
        self.mapped_file_queue.get_total_file_size()
    }

    #[inline]
    fn get_unit_size(&self) -> i32 {
        CQ_STORE_UNIT_SIZE
    }

    #[inline]
    fn correct_min_offset(&self, min_commit_log_offset: i64) {
        if self.get_max_physic_offset() < 0 {
            return;
        }

        let mut next_min_offset = self.get_min_offset_in_queue();
        let mut next_logic_offset = self.get_min_logic_offset();
        let mut found_valid = false;

        for mapped_file in self.mapped_file_queue.iter() {
            let mut position = 0;
            while position + CQ_STORE_UNIT_SIZE <= mapped_file.get_read_position() {
                let Some(entry) = Self::read_entry(&mapped_file, position) else {
                    break;
                };

                if entry.pos < min_commit_log_offset {
                    next_min_offset = entry.msg_base_offset + entry.batch_size as i64;
                    next_logic_offset =
                        mapped_file.get_file_from_offset() as i64 + position as i64 + CQ_STORE_UNIT_SIZE as i64;
                    position += CQ_STORE_UNIT_SIZE;
                    continue;
                }

                next_min_offset = entry.msg_base_offset;
                next_logic_offset = mapped_file.get_file_from_offset() as i64 + position as i64;
                found_valid = true;
                break;
            }

            if found_valid {
                break;
            }
        }

        self.min_offset_in_queue.store(next_min_offset, Ordering::Release);
        self.min_logic_offset.store(next_logic_offset, Ordering::Release);
    }

    #[inline]
    fn put_message_position_info_wrapper(&mut self, request: &DispatchRequest) {
        if request.msg_base_offset < 0 || request.batch_size <= 0 {
            warn!(
                "unexpected dispatch request in batch consume queue topic={} queue={} commit_log_offset={}",
                self.topic, self.queue_id, request.commit_log_offset
            );
            return;
        }

        for _ in 0..30 {
            if self.put_batch_message_position_info(
                request.commit_log_offset,
                request.msg_size,
                request.tags_code,
                request.store_timestamp,
                request.msg_base_offset,
                request.batch_size,
            ) {
                return;
            }
        }

        warn!(
            "batch consume queue can not write, topic={} queue={} store_path={} commit_log_size={}",
            self.topic, self.queue_id, self.store_path, self.commit_log_size
        );
    }

    #[inline]
    fn increase_queue_offset(
        &self,
        queue_offset_assigner: &QueueOffsetOperator,
        msg: &MessageExtBrokerInner,
        message_num: i16,
    ) {
        queue_offset_assigner.increase_batch_queue_offset(
            &CheetahString::from_string(format!("{}-{}", msg.topic(), msg.queue_id())),
            message_num,
        );
    }

    #[inline]
    fn assign_queue_offset(&self, queue_offset_operator: &QueueOffsetOperator, msg: &mut MessageExtBrokerInner) {
        let queue_offset = queue_offset_operator.get_batch_queue_offset(&CheetahString::from_string(format!(
            "{}-{}",
            msg.topic(),
            msg.queue_id()
        )));
        msg.message_ext_inner.queue_offset = queue_offset;
    }

    #[inline]
    fn estimate_message_count(&self, from: i64, to: i64, filter: &dyn MessageFilter) -> i64 {
        let start = from.max(self.get_min_offset_in_queue());
        let end = to.min(self.get_max_offset_in_queue() - 1);
        if start < 0 || end < start {
            return 0;
        }

        let mut count = 0;
        let Some(iter) = self.iterate_from(start) else {
            return 0;
        };

        for cq_unit in iter {
            let batch_start = cq_unit.queue_offset;
            let batch_end = batch_start + cq_unit.batch_num as i64 - 1;
            if batch_start > end {
                break;
            }
            if batch_end < start {
                continue;
            }
            if filter.is_matched_by_consume_queue(Some(cq_unit.tags_code), None) {
                count += batch_end.min(end) - batch_start.max(start) + 1;
            }
        }
        count
    }

    #[inline]
    fn iterate_from(&self, start_index: i64) -> Option<Box<dyn Iterator<Item = CqUnit> + Send + '_>> {
        let (files, position) = self.find_record_position(start_index)?;
        Some(Box::new(BatchConsumeQueueIterator {
            files,
            file_index: position.file_index,
            position: position.position,
        }))
    }

    fn iterate_from_with_count(
        &self,
        start_index: i64,
        _count: i32,
    ) -> Option<Box<dyn Iterator<Item = CqUnit> + Send + '_>> {
        self.iterate_from(start_index)
    }

    fn get_offset_in_queue_by_time_with_boundary(&self, timestamp: i64, boundary_type: BoundaryType) -> i64 {
        let mut candidate = None;
        let mut last_seen = None;

        for mapped_file in self.mapped_file_queue.iter() {
            let mut position = 0;
            while position + CQ_STORE_UNIT_SIZE <= mapped_file.get_read_position() {
                let Some(entry) = Self::read_entry(&mapped_file, position) else {
                    break;
                };
                last_seen = Some(entry.msg_base_offset);
                if entry.store_time < timestamp {
                    position += CQ_STORE_UNIT_SIZE;
                    continue;
                }

                match boundary_type {
                    BoundaryType::Lower => return entry.msg_base_offset,
                    BoundaryType::Upper => {
                        candidate = Some(entry.msg_base_offset);
                        position += CQ_STORE_UNIT_SIZE;
                        continue;
                    }
                }
            }

            if let Some(offset) = candidate {
                return offset;
            }
        }

        candidate.or(last_seen).unwrap_or(-1)
    }
}

struct BatchConsumeQueueIterator {
    files: Vec<Arc<DefaultMappedFile>>,
    file_index: usize,
    position: i32,
}

impl Iterator for BatchConsumeQueueIterator {
    type Item = CqUnit;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(mapped_file) = self.files.get(self.file_index) {
            let read_position = mapped_file.get_read_position();
            if self.position + CQ_STORE_UNIT_SIZE > read_position {
                self.file_index += 1;
                self.position = 0;
                continue;
            }

            let entry = BatchConsumeQueue::read_entry(mapped_file, self.position)?;
            self.position += CQ_STORE_UNIT_SIZE;
            return Some(BatchConsumeQueue::entry_to_cq_unit(entry));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::BufMut;
    use cheetah_string::CheetahString;
    use tempfile::tempdir;

    use super::*;

    fn create_batch_consume_queue(store_path: CheetahString, mapped_file_size: usize) -> BatchConsumeQueue {
        BatchConsumeQueue::new(
            CheetahString::from_static_str("BatchTopic"),
            1,
            store_path,
            mapped_file_size,
            None,
            Arc::new(MessageStoreConfig::default()),
        )
    }

    fn dispatch_request(
        commit_log_offset: i64,
        msg_size: i32,
        store_timestamp: i64,
        msg_base_offset: i64,
        batch_size: i16,
    ) -> DispatchRequest {
        DispatchRequest {
            topic: CheetahString::from_static_str("BatchTopic"),
            queue_id: 1,
            commit_log_offset,
            msg_size,
            tags_code: 7,
            store_timestamp,
            msg_base_offset,
            batch_size,
            success: true,
            ..DispatchRequest::default()
        }
    }

    fn encode_invalid_record(
        offset: i64,
        size: i32,
        tags_code: i64,
        store_time: i64,
        msg_base_offset: i64,
        batch_size: i16,
    ) -> Vec<u8> {
        let mut bytes = BytesMut::with_capacity(CQ_STORE_UNIT_SIZE as usize);
        bytes.put_i64(offset);
        bytes.put_i32(size);
        bytes.put_i64(tags_code);
        bytes.put_i64(store_time);
        bytes.put_i64(msg_base_offset);
        bytes.put_i16(batch_size);
        bytes.put_i32(INVALID_POS);
        bytes.put_i32(0);
        bytes.to_vec()
    }

    #[test]
    fn batch_consume_queue_put_and_get_round_trip() {
        let root = tempdir().expect("tempdir");
        let store_path = CheetahString::from_string(root.path().join("batch-cq").to_string_lossy().to_string());
        let mut consume_queue = create_batch_consume_queue(store_path, (CQ_STORE_UNIT_SIZE * 4) as usize);

        consume_queue.put_message_position_info_wrapper(&dispatch_request(100, 40, 1_000, 0, 3));
        consume_queue.put_message_position_info_wrapper(&dispatch_request(140, 32, 2_000, 3, 2));

        let first = consume_queue.get(0).expect("first batch");
        assert_eq!(first.queue_offset, 0);
        assert_eq!(first.batch_num, 3);
        assert_eq!(first.pos, 100);
        assert_eq!(first.size, 40);
        assert_eq!(first.tags_code, 7);
        assert_eq!(first.native_buffer.len(), CQ_STORE_UNIT_SIZE as usize);

        let same_first = consume_queue.get(2).expect("same batch");
        assert_eq!(same_first.queue_offset, 0);
        assert_eq!(same_first.batch_num, 3);

        let second = consume_queue.get(3).expect("second batch");
        assert_eq!(second.queue_offset, 3);
        assert_eq!(second.batch_num, 2);
        assert_eq!(second.pos, 140);

        let (stored, store_time) = consume_queue.get_cq_unit_and_store_time(3).expect("store time");
        assert_eq!(stored.queue_offset, 3);
        assert_eq!(store_time, 2_000);

        let iterated: Vec<_> = consume_queue.iterate_from(1).expect("iterator").take(2).collect();
        assert_eq!(iterated.len(), 2);
        assert_eq!(iterated[0].queue_offset, 0);
        assert_eq!(iterated[1].queue_offset, 3);

        assert_eq!(consume_queue.get_min_offset_in_queue(), 0);
        assert_eq!(consume_queue.get_max_offset_in_queue(), 5);
        assert_eq!(consume_queue.get_message_total_in_queue(), 5);
        assert_eq!(consume_queue.get_last_offset(), 172);
        assert_eq!(consume_queue.get_offset_in_queue_by_time(1_500), 3);
        assert_eq!(
            consume_queue.get_offset_in_queue_by_time_with_boundary(2_000, BoundaryType::Upper),
            3
        );
    }

    #[test]
    fn batch_consume_queue_recover_truncates_dirty_tail() {
        let root = tempdir().expect("tempdir");
        let store_path = CheetahString::from_string(root.path().join("batch-cq").to_string_lossy().to_string());
        let mut consume_queue = create_batch_consume_queue(store_path.clone(), (CQ_STORE_UNIT_SIZE * 4) as usize);

        consume_queue.put_message_position_info_wrapper(&dispatch_request(100, 40, 1_000, 0, 3));
        consume_queue.put_message_position_info_wrapper(&dispatch_request(140, 32, 2_000, 3, 2));
        let mapped_file = consume_queue
            .mapped_file_queue
            .get_last_mapped_file()
            .expect("mapped file");
        let dirty_record = encode_invalid_record(200, 24, 8, 3_000, -1, 1);
        assert!(mapped_file.put_slice(&dirty_record, (CQ_STORE_UNIT_SIZE * 2) as usize));
        mapped_file.set_wrote_position(CQ_STORE_UNIT_SIZE * 3);
        mapped_file.set_committed_position(CQ_STORE_UNIT_SIZE * 3);
        consume_queue.flush(0);

        drop(consume_queue);

        let mut recovered = create_batch_consume_queue(store_path, (CQ_STORE_UNIT_SIZE * 4) as usize);
        assert!(recovered.load());
        recovered.recover();

        assert_eq!(recovered.get_min_offset_in_queue(), 0);
        assert_eq!(recovered.get_max_offset_in_queue(), 5);
        assert_eq!(recovered.get_message_total_in_queue(), 5);
        assert_eq!(recovered.get_max_physic_offset(), 172);
        assert!(recovered.get(5).is_none());
    }

    #[test]
    fn batch_consume_queue_delete_expired_file_and_correct_min_offset() {
        let root = tempdir().expect("tempdir");
        let store_path = CheetahString::from_string(root.path().join("batch-cq").to_string_lossy().to_string());
        let mut consume_queue = create_batch_consume_queue(store_path, (CQ_STORE_UNIT_SIZE * 2) as usize);

        consume_queue.put_message_position_info_wrapper(&dispatch_request(100, 40, 1_000, 0, 3));
        consume_queue.put_message_position_info_wrapper(&dispatch_request(200, 40, 2_000, 3, 3));
        consume_queue.put_message_position_info_wrapper(&dispatch_request(300, 40, 3_000, 6, 2));

        let first_file = consume_queue
            .mapped_file_queue
            .get_first_mapped_file()
            .expect("first file");
        first_file.shutdown(0);

        assert_eq!(consume_queue.delete_expired_file(250), 1);
        consume_queue.correct_min_offset(250);

        assert_eq!(consume_queue.get_min_offset_in_queue(), 6);
        assert_eq!(consume_queue.get_max_offset_in_queue(), 8);
        assert_eq!(consume_queue.get_message_total_in_queue(), 2);
        assert!(consume_queue.get(3).is_none());
        let latest = consume_queue.get(6).expect("remaining batch");
        assert_eq!(latest.queue_offset, 6);
        assert_eq!(latest.batch_num, 2);
    }
}
