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

use std::path::PathBuf;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::attribute::cq_type::CQType;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::mix_all::is_lmq;
use rocketmq_common::common::mix_all::LMQ_QUEUE_ID;
use rocketmq_common::common::mix_all::MULTI_DISPATCH_QUEUE_SPLITTER;
use rocketmq_store_local::consume_queue::record::ConsumeQueueRecord;
pub use rocketmq_store_local::consume_queue::record::CQ_STORE_UNIT_SIZE;
pub use rocketmq_store_local::consume_queue::record::MSG_TAG_OFFSET_INDEX;
use rocketmq_store_local::consume_queue::root::drive_consume_queue_dispatch;
use rocketmq_store_local::consume_queue::root::ConsumeQueueDispatchMetadata;
use rocketmq_store_local::consume_queue::root::ConsumeQueueDispatchMode;
use rocketmq_store_local::consume_queue::root::ConsumeQueueDispatchOutcome;
use rocketmq_store_local::consume_queue::single::find_min_offset_record;
use rocketmq_store_local::consume_queue::single::plan_truncate_records;
use rocketmq_store_local::consume_queue::single::scan_recovery_records;
use rocketmq_store_local::consume_queue::single::search_queue_offset_by_time;
use rocketmq_store_local::consume_queue::single::ConsumeQueueTimeBoundary;
use rocketmq_store_local::consume_queue::single::ConsumeQueueTruncatePlan;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::dispatch_request::DispatchRequest;
use crate::base::select_result::SelectMappedBufferResult;
use crate::base::swappable::Swappable;
use crate::consume_queue::cq_ext_unit::CqExtUnit;
use crate::consume_queue::mapped_file_queue::MappedFileQueue;
use crate::filter::MessageFilter;
use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::MappedFile;
use crate::queue::consume_queue::ConsumeQueueTrait;
use crate::queue::consume_queue_ext::ConsumeQueueExt;
use crate::queue::local_file_consume_queue_store::ConsumeQueueLookupHandle;
use crate::queue::local_file_consume_queue_store::ConsumeQueueStoreContext;
use crate::queue::multi_dispatch_utils::check_multi_dispatch_queue;
use crate::queue::queue_offset_operator::QueueOffsetOperator;
use crate::queue::CqUnit;
use crate::queue::FileQueueLifeCycle;
use crate::store_path_config_helper::get_store_path_consume_queue_ext;

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
pub struct ConsumeQueue {
    context: ConsumeQueueStoreContext,
    queue_lookup: ConsumeQueueLookupHandle,
    mapped_file_queue: MappedFileQueue,
    topic: CheetahString,
    queue_id: i32,
    store_path: CheetahString,
    mapped_file_size: i32,
    max_physic_offset: Arc<AtomicI64>,
    min_logic_offset: Arc<AtomicI64>,
    consume_queue_ext: Option<ConsumeQueueExt>,
}

impl ConsumeQueue {
    #[inline]
    pub(crate) fn new(
        topic: CheetahString,
        queue_id: i32,
        store_path: CheetahString,
        mapped_file_size: i32,
        context: ConsumeQueueStoreContext,
        queue_lookup: ConsumeQueueLookupHandle,
    ) -> Self {
        let message_store_config = context.message_store_config();
        let queue_dir = PathBuf::from(store_path.as_str())
            .join(topic.as_str())
            .join(queue_id.to_string());
        let mapped_file_queue =
            MappedFileQueue::new(queue_dir.to_string_lossy().to_string(), mapped_file_size as u64, None);
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
            context,
            queue_lookup,
            mapped_file_queue,
            topic,
            queue_id,
            store_path,
            mapped_file_size,
            max_physic_offset: Arc::new(AtomicI64::new(-1)),
            min_logic_offset: Arc::new(AtomicI64::new(0)),
            consume_queue_ext,
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
        let mut should_delete_file = false;
        let mapped_file_size = self.mapped_file_size;
        loop {
            let Some(mapped_file) = self.mapped_file_queue.get_last_mapped_file() else {
                break;
            };
            mapped_file.set_wrote_position(0);
            mapped_file.set_committed_position(0);
            mapped_file.set_flushed_position(0);

            let plan = plan_truncate_records(
                mapped_file_size,
                phy_offset,
                |relative_offset| {
                    mapped_file
                        .get_bytes(relative_offset as usize, CQ_STORE_UNIT_SIZE as usize)
                        .and_then(|bytes| ConsumeQueueRecord::decode(bytes.as_ref()))
                },
                Self::is_ext_addr,
            );
            match plan {
                ConsumeQueueTruncatePlan::RetryFile => {
                    if !should_delete_file {
                        continue;
                    }
                }
                ConsumeQueueTruncatePlan::DeleteFile => should_delete_file = true,
                ConsumeQueueTruncatePlan::Retain {
                    valid_bytes,
                    max_physical_offset,
                    max_extension_address: _,
                } => {
                    mapped_file.set_wrote_position(valid_bytes);
                    mapped_file.set_committed_position(valid_bytes);
                    mapped_file.set_flushed_position(valid_bytes);
                    if let Some(max_physical_offset) = max_physical_offset {
                        self.set_max_physic_offset(max_physical_offset);
                    }
                    return;
                }
            }
            if should_delete_file {
                if delete_file {
                    self.mapped_file_queue.delete_last_mapped_file();
                } else {
                    self.mapped_file_queue
                        .delete_expired_file(vec![self.mapped_file_queue.get_last_mapped_file().unwrap()]);
                }
            }
        }
        if self.is_ext_read_enable() {
            self.consume_queue_ext.as_ref().unwrap().truncate_by_max_address(1);
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
        self.consume_queue_ext.is_some() && self.context.message_store_config().enable_consume_queue_ext
    }

    pub fn put_message_position_info(&mut self, offset: i64, size: i32, tags_code: i64, cq_offset: i64) -> bool {
        if offset + size as i64 <= self.get_max_physic_offset() {
            warn!(
                "Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}, size={}",
                self.get_max_physic_offset(),
                offset,
                size
            );
            return true;
        }
        let encoded_record = ConsumeQueueRecord::new(offset, size, tags_code).encode();

        let expect_logic_offset = cq_offset * CQ_STORE_UNIT_SIZE as i64;
        if let Some(mapped_file) = self
            .mapped_file_queue
            .get_last_mapped_file_mut_start_offset(expect_logic_offset as u64, true)
        {
            if mapped_file.is_first_create_in_queue() && cq_offset != 0 && mapped_file.get_wrote_position() == 0 {
                self.min_logic_offset.store(expect_logic_offset, Ordering::Release);
                self.mapped_file_queue.set_flushed_where(expect_logic_offset);
                self.mapped_file_queue.set_committed_where(expect_logic_offset);
                self.fill_pre_blank(&mapped_file, expect_logic_offset);
                info!(
                    "fill pre blank space {} {}",
                    mapped_file.get_file_name(),
                    mapped_file.get_wrote_position()
                );
            }

            if cq_offset != 0 {
                let current_logic_offset =
                    mapped_file.get_wrote_position() as i64 + mapped_file.get_file_from_offset() as i64;

                if expect_logic_offset < current_logic_offset {
                    warn!(
                        "Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: \
                         {} Diff: {}",
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
                        "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} \
                         QID: {} Diff: {}",
                        expect_logic_offset,
                        current_logic_offset,
                        self.topic,
                        self.queue_id,
                        expect_logic_offset - current_logic_offset
                    );
                }
            }
            self.set_max_physic_offset(offset + size as i64);
            mapped_file.append_message_bytes(&encoded_record)
        } else {
            false
        }
    }

    #[inline]
    fn fill_pre_blank(&self, mapped_file: &Arc<DefaultMappedFile>, until_where: i64) {
        let bytes = ConsumeQueueRecord::pre_blank().encode();
        let until =
            (until_where % self.mapped_file_queue.get_mapped_file_size_config() as i64) as i32 / CQ_STORE_UNIT_SIZE;
        for n in 0..until {
            mapped_file.append_message_bytes(&bytes);
        }
    }

    #[inline]
    pub fn get_index_buffer(&self, start_index: i64) -> Option<SelectMappedBufferResult> {
        let mapped_file_size = self.mapped_file_size;
        let offset = start_index * CQ_STORE_UNIT_SIZE as i64;
        if offset >= self.get_min_logic_offset() {
            if let Some(mapped_file) = self.mapped_file_queue.find_mapped_file_by_offset(offset, false) {
                let mut result =
                    mapped_file.select_mapped_buffer_with_position((offset % mapped_file_size as i64) as i32);
                if let Some(ref mut result) = result {
                    result.mapped_file = Some(mapped_file);
                }
                return result;
            }
        }
        None
    }

    fn multi_dispatch_lmq_queue(&self, request: &DispatchRequest, _max_retries: i32) {
        let Some(properties_map) = request.properties_map.as_ref() else {
            return;
        };
        let Some(multi_dispatch_queue) = properties_map.get(MessageConst::PROPERTY_INNER_MULTI_DISPATCH) else {
            return;
        };
        let Some(multi_queue_offset) = properties_map.get(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET) else {
            return;
        };

        let queues: Vec<&str> = multi_dispatch_queue.split(MULTI_DISPATCH_QUEUE_SPLITTER).collect();
        let queue_offsets: Vec<&str> = multi_queue_offset.split(MULTI_DISPATCH_QUEUE_SPLITTER).collect();
        if queues.len() != queue_offsets.len() {
            error!(
                "[BUG] queues length mismatches queueOffsets length for topic {}",
                request.topic
            );
            return;
        }

        for (queue_name, queue_offset) in queues.into_iter().zip(queue_offsets) {
            if queue_name.chars().any(std::path::is_separator) {
                continue;
            }

            let Ok(queue_offset) = queue_offset.parse::<i64>() else {
                warn!(
                    "Skip invalid multi-dispatch queue offset, topic={}, queueName={}, queueOffset={}",
                    request.topic, queue_name, queue_offset
                );
                continue;
            };

            let queue_id = if self.context.message_store_config().enable_lmq && is_lmq(Some(queue_name)) {
                LMQ_QUEUE_ID as i32
            } else {
                request.queue_id
            };

            let Some(consume_queue) = self
                .queue_lookup
                .find_or_create_consume_queue(&CheetahString::from(queue_name), queue_id)
            else {
                warn!(
                    "Skip multi-dispatch queue because consume queue lookup failed, topic={}, queueName={}, queueId={}",
                    request.topic, queue_name, queue_id
                );
                continue;
            };

            let mut lmq_dispatch_request = request.clone();
            lmq_dispatch_request.topic = CheetahString::from(queue_name);
            lmq_dispatch_request.queue_id = queue_id;
            lmq_dispatch_request.consume_queue_offset = queue_offset;
            lmq_dispatch_request.properties_map = None;

            consume_queue
                .write()
                .put_message_position_info_wrapper(&lmq_dispatch_request);
        }
    }

    /// Binary search within a mapped file to find the offset by timestamp.
    ///
    /// # Arguments
    ///
    /// * `mapped_file` - The mapped file to search in
    /// * `timestamp` - The timestamp to search for
    /// * `boundary_type` - The boundary type (Lower or Upper)
    ///
    /// # Returns
    ///
    /// The queue offset that matches the criteria, or 0 if not found
    fn binary_search_in_queue_by_time(
        &self,
        mapped_file: &Arc<DefaultMappedFile>,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64 {
        let commit_log = self.context.commit_log();
        let min_physic_offset = commit_log.get_min_offset();
        let min_logic_offset = self.min_logic_offset.load(Ordering::Relaxed);

        // Calculate the range to search
        let mut range = mapped_file.get_file_size() as i32;
        let wrote_position = mapped_file.get_wrote_position();
        if wrote_position != 0 && wrote_position != mapped_file.get_file_size() as i32 {
            // mappedFile is the last one and is currently being written.
            range = wrote_position;
        }

        let select_result = mapped_file.select_mapped_buffer(0, range);
        if select_result.is_none() {
            return 0;
        }
        let select_result = select_result.unwrap();
        let buffer = match &select_result.bytes {
            Some(b) => b,
            None => return 0,
        };

        let mapped_file_from_offset = mapped_file.get_file_from_offset() as i64;
        let boundary = match boundary_type {
            BoundaryType::Lower => ConsumeQueueTimeBoundary::Lower,
            BoundaryType::Upper => ConsumeQueueTimeBoundary::Upper,
        };
        search_queue_offset_by_time(
            buffer,
            mapped_file_from_offset,
            min_logic_offset,
            min_physic_offset,
            timestamp,
            boundary,
            |physical_offset, message_size| commit_log.pickup_store_timestamp(physical_offset, message_size),
            |physical_offset| {
                warn!(
                    "Failed to query store timestamp for commit log offset: {}",
                    physical_offset
                );
            },
        )
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

    fn recover(&mut self) {
        let binding = self.mapped_file_queue.get_mapped_files();
        let mapped_files = binding.load();
        if mapped_files.is_empty() {
            return;
        }
        let mut index = mapped_files.len() as i32 - 3;
        if index < 0 {
            index = 0;
        }
        let mut index = index as usize;
        let mapped_file_size_logics = self.mapped_file_size;
        let mut mapped_file = mapped_files.get(index).unwrap();
        let mut process_offset = mapped_file.get_file_from_offset();
        let mut max_ext_addr = 1i64;
        let mapped_file_offset = loop {
            let scan = scan_recovery_records(
                mapped_file_size_logics,
                |relative_offset| {
                    mapped_file
                        .get_bytes(relative_offset as usize, CQ_STORE_UNIT_SIZE as usize)
                        .and_then(|bytes| ConsumeQueueRecord::decode(bytes.as_ref()))
                },
                ConsumeQueue::is_ext_addr,
            );
            let mapped_file_offset = i64::from(scan.valid_bytes);
            if let Some(max_physical_offset) = scan.max_physical_offset {
                self.set_max_physic_offset(max_physical_offset);
            }
            if let Some(max_extension_address) = scan.max_extension_address {
                max_ext_addr = max_extension_address;
            }
            if let Some(record) = scan.stopped_record {
                info!(
                    "recover current consume queue file over,  {}, {} {} {}",
                    mapped_file.get_file_name(),
                    record.physical_offset,
                    record.message_size,
                    record.tags_code
                );
            }
            if scan.fills_file(mapped_file_size_logics) {
                index += 1;
                if index >= mapped_files.len() {
                    info!(
                        "recover last consume queue file over, last mapped file {}",
                        mapped_file.get_file_name()
                    );
                    break mapped_file_offset;
                } else {
                    mapped_file = mapped_files.get(index).unwrap();
                    process_offset = mapped_file.get_file_from_offset();
                    info!("recover next consume queue file, {}", mapped_file.get_file_name());
                }
            } else {
                info!(
                    "recover current consume queue file over, {} {}",
                    mapped_file.get_file_name(),
                    process_offset + (mapped_file_offset as u64),
                );
                break mapped_file_offset;
            }
        };
        process_offset += mapped_file_offset as u64;
        self.mapped_file_queue.set_flushed_where(process_offset as i64);
        self.mapped_file_queue.set_committed_where(process_offset as i64);
        self.mapped_file_queue.truncate_dirty_files(process_offset as i64);

        if self.is_ext_read_enable() {
            let consume_queue_ext = self.consume_queue_ext.as_mut().unwrap();
            consume_queue_ext.recover();
            info!("Truncate consume queue extend file by max {}", max_ext_addr);
            consume_queue_ext.truncate_by_max_address(max_ext_addr);
        }
    }

    #[inline]
    fn check_self(&self) {
        self.mapped_file_queue.check_self();
        if self.is_ext_read_enable() {
            self.consume_queue_ext.as_ref().unwrap().check_self();
        }
    }

    #[inline]
    fn flush(&self, flush_least_pages: i32) -> bool {
        let mut result = self.mapped_file_queue.flush(flush_least_pages);
        if self.is_ext_read_enable() {
            result &= self.consume_queue_ext.as_ref().unwrap().flush(flush_least_pages);
        }
        result
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
        self.mapped_file_queue
            .delete_expired_file_by_offset(min_commit_log_pos, CQ_STORE_UNIT_SIZE)
    }

    #[inline]
    fn roll_next_file(&self, next_begin_offset: i64) -> i64 {
        let units_per_file = self.mapped_file_size as i64 / CQ_STORE_UNIT_SIZE as i64;
        next_begin_offset + units_per_file - next_begin_offset % units_per_file
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

impl Swappable for ConsumeQueue {
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
        self.iterate_from(index).and_then(|mut iter| iter.next())
    }

    #[inline]
    fn get_cq_unit_and_store_time(&self, index: i64) -> Option<(CqUnit, i64)> {
        let cq_unit = self.get(index)?;
        let i = self
            .context
            .commit_log()
            .pickup_store_timestamp(cq_unit.pos, cq_unit.size);
        Some((cq_unit, i))
    }

    #[inline]
    fn get_earliest_unit_and_store_time(&self) -> Option<(CqUnit, i64)> {
        let min_offset = self.get_min_offset_in_queue();
        self.get_cq_unit_and_store_time(min_offset)
    }

    #[inline]
    fn get_earliest_unit(&self) -> Option<CqUnit> {
        self.get(self.get_min_offset_in_queue())
    }

    #[inline]
    fn get_latest_unit(&self) -> Option<CqUnit> {
        let max_offset = self.get_max_offset_in_queue();
        if max_offset <= self.get_min_offset_in_queue() {
            return None;
        }
        self.get(max_offset - 1)
    }

    #[inline]
    fn get_last_offset(&self) -> i64 {
        let max_offset_in_queue = self.get_max_offset_in_queue();
        if max_offset_in_queue <= self.get_min_offset_in_queue() {
            return -1;
        }

        self.get(max_offset_in_queue - 1)
            .map(|cq_unit| cq_unit.pos + cq_unit.size as i64)
            .unwrap_or(-1)
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
        self.get_max_offset_in_queue() - self.get_min_offset_in_queue()
    }

    #[inline]
    fn get_offset_in_queue_by_time(&self, timestamp: i64) -> i64 {
        self.get_offset_in_queue_by_time_with_boundary(timestamp, BoundaryType::Lower)
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
        let mut total_size: i64 = self
            .mapped_file_queue
            .get_mapped_files()
            .load()
            .iter()
            .map(|mapped_file| mapped_file.get_file_size() as i64)
            .sum();
        if self.is_ext_read_enable() {
            total_size += self.consume_queue_ext.as_ref().unwrap().get_total_size();
        }
        total_size
    }

    #[inline]
    fn get_mapped_file_count(&self) -> usize {
        self.mapped_file_queue.get_mapped_files_size()
    }

    #[inline]
    fn get_unit_size(&self) -> i32 {
        CQ_STORE_UNIT_SIZE
    }

    #[inline]
    fn correct_min_offset(&self, min_commit_log_offset: i64) {
        if self.get_max_physic_offset() < 0 {
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
        let mut last_record =
            last_mapped_file.select_mapped_buffer(max_readable_position - CQ_STORE_UNIT_SIZE, CQ_STORE_UNIT_SIZE);
        if let Some(ref mut result) = last_record {
            result.mapped_file = Some(last_mapped_file.clone());
        }
        if let Some(last_record) = last_record {
            let last_record_pos = (last_record.start_offset - last_mapped_file.get_file_from_offset()) as usize;
            let bytes = last_record
                .mapped_file
                .as_ref()
                .unwrap()
                .get_bytes(last_record_pos, last_record.size as usize)
                .unwrap();
            let Some(last_record) = ConsumeQueueRecord::decode(bytes.as_ref()) else {
                return;
            };
            let commit_log_offset = last_record.physical_offset;
            if commit_log_offset < min_commit_log_offset {
                self.min_logic_offset.store(
                    max_readable_position as i64 + last_mapped_file.get_file_from_offset() as i64,
                    Ordering::SeqCst,
                );
                info!(
                    "ConsumeQueue[topic={}, queue-id={}] contains no valid entries. Min-offset is assigned as: {}.",
                    self.topic,
                    self.queue_id,
                    self.get_min_offset_in_queue()
                );
                return;
            }
        }

        let mut min_ext_addr = 1i64;
        let mapped_files = self.mapped_file_queue.get_mapped_files().load().clone();
        for mapped_file in mapped_files.iter() {
            let read_position = mapped_file.get_read_position();
            let Some(selected) = find_min_offset_record(read_position, min_commit_log_offset, |relative_offset| {
                mapped_file
                    .get_bytes(relative_offset as usize, CQ_STORE_UNIT_SIZE as usize)
                    .and_then(|bytes| ConsumeQueueRecord::decode(bytes.as_ref()))
            }) else {
                continue;
            };
            self.min_logic_offset.store(
                mapped_file.get_file_from_offset() as i64 + i64::from(selected.relative_offset),
                Ordering::SeqCst,
            );
            if ConsumeQueue::is_ext_addr(selected.record.tags_code) {
                min_ext_addr = selected.record.tags_code;
            }
            if self.is_ext_read_enable() {
                self.consume_queue_ext
                    .as_ref()
                    .unwrap()
                    .truncate_by_min_address(min_ext_addr);
            }
            return;
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
        const MAX_RETRIES: usize = 30;
        let can_write = self.context.running_flags().is_cq_writeable();
        let outcome = drive_consume_queue_dispatch(
            ConsumeQueueDispatchMode::Single,
            ConsumeQueueDispatchMetadata {
                message_base_offset: request.msg_base_offset,
                batch_size: request.batch_size,
            },
            can_write,
            MAX_RETRIES,
            |attempt| {
                let mut tags_code = request.tags_code;
                if self.is_ext_write_enable() {
                    let ext_addr = self.consume_queue_ext.as_ref().unwrap().put(CqExtUnit::new(
                        tags_code,
                        request.store_timestamp,
                        request.bit_map.clone(),
                    ));

                    if ConsumeQueue::is_ext_addr(ext_addr) {
                        tags_code = ext_addr;
                    } else {
                        warn!(
                            "Save consume queue extend fail, So just save tagsCode!  topic:{}, queueId:{}, offset:{}",
                            self.topic, self.queue_id, request.commit_log_offset,
                        )
                    }
                }

                let appended = self.put_message_position_info(
                    request.commit_log_offset,
                    request.msg_size,
                    tags_code,
                    request.consume_queue_offset,
                );
                if !appended {
                    warn!(
                        "[BUG]put commit log position info to {}:{} failed, retry {} times",
                        self.topic, self.queue_id, attempt
                    );
                }
                appended
            },
        );
        if matches!(outcome, ConsumeQueueDispatchOutcome::Appended { .. }) {
            let message_store_config = self.context.message_store_config();
            let store_checkpoint = self.context.store_checkpoint();
            if message_store_config.broker_role == BrokerRole::Slave || message_store_config.enable_dledger_commit_log {
                store_checkpoint.set_physic_msg_timestamp(request.store_timestamp as u64);
            }
            store_checkpoint.set_logics_msg_timestamp(request.store_timestamp as u64);

            if check_multi_dispatch_queue(message_store_config, request) {
                self.multi_dispatch_lmq_queue(request, MAX_RETRIES as i32);
            }
            return;
        }
        error!("[BUG]consume queue can not write, {} {}", self.topic, self.queue_id);
        self.context.running_flags().make_logics_queue_error();
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
    fn assign_queue_offset(&self, queue_offset_operator: &QueueOffsetOperator, msg: &mut MessageExtBrokerInner) {
        let queue_offset = queue_offset_operator.get_queue_offset(CheetahString::from_string(format!(
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
        if end < start {
            return 0;
        }

        let mut count = 0;
        for index in start..=end {
            let Some(cq_unit) = self.get(index) else {
                continue;
            };

            if !filter.is_matched_by_consume_queue(Some(cq_unit.tags_code), cq_unit.cq_ext_unit.as_ref()) {
                continue;
            }

            let matched_commit_log = self
                .context
                .commit_log()
                .get_message(cq_unit.pos, cq_unit.size)
                .map(|buffer| filter.is_matched_by_commit_log(Some(buffer.get_buffer()), None))
                .unwrap_or_else(|| filter.is_matched_by_commit_log(None, None));
            if matched_commit_log {
                count += 1;
            }
        }
        count
    }

    #[inline]
    fn iterate_from(&self, start_index: i64) -> Option<Box<dyn Iterator<Item = CqUnit> + Send + '_>> {
        match self.get_index_buffer(start_index) {
            None => None,
            Some(value) => Some(Box::new(ConsumeQueueIterator {
                smbr: Some(value),
                counter: 0,
                remaining: usize::MAX,
                consume_queue_ext: self.consume_queue_ext.clone(),
            })),
        }
    }

    fn iterate_from_with_count(
        &self,
        start_index: i64,
        count: i32,
    ) -> Option<Box<dyn Iterator<Item = CqUnit> + Send + '_>> {
        self.get_index_buffer(start_index).map(|value| {
            Box::new(ConsumeQueueIterator {
                smbr: Some(value),
                counter: 0,
                remaining: count.max(0) as usize,
                consume_queue_ext: self.consume_queue_ext.clone(),
            }) as Box<dyn Iterator<Item = CqUnit> + Send + '_>
        })
    }

    fn get_offset_in_queue_by_time_with_boundary(&self, timestamp: i64, boundary_type: BoundaryType) -> i64 {
        let commit_log = self.context.commit_log();
        let mapped_file =
            self.mapped_file_queue
                .get_consume_queue_mapped_file_by_time(timestamp, commit_log, boundary_type);
        if let Some(mapped_file) = mapped_file {
            return self.binary_search_in_queue_by_time(&mapped_file, timestamp, boundary_type);
        }
        -1
    }
}

struct ConsumeQueueIterator {
    smbr: Option<SelectMappedBufferResult>,
    counter: i32,
    remaining: usize,
    consume_queue_ext: Option<ConsumeQueueExt>,
}

impl ConsumeQueueIterator {
    fn get_ext(&self, offset: i64, cq_ext_unit: &mut CqExtUnit) -> bool {
        match self.consume_queue_ext.as_ref() {
            None => false,
            Some(value) => value.get(offset, cq_ext_unit),
        }
    }
}

impl Iterator for ConsumeQueueIterator {
    type Item = CqUnit;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        match self.smbr.as_ref() {
            None => None,
            Some(value) => {
                if self.counter * CQ_STORE_UNIT_SIZE >= value.size {
                    return None;
                }
                let mmp = value.mapped_file.as_ref().unwrap().get_mapped_file();
                let start = value.start_offset as usize + (self.counter * CQ_STORE_UNIT_SIZE) as usize;
                self.counter += 1;
                let end = start + CQ_STORE_UNIT_SIZE as usize;
                let record = ConsumeQueueRecord::decode(&mmp[start..end])?;
                self.remaining = self.remaining.saturating_sub(1);
                let mut cq_unit = CqUnit {
                    queue_offset: start as i64 / CQ_STORE_UNIT_SIZE as i64,
                    size: record.message_size,
                    pos: record.physical_offset,
                    tags_code: record.tags_code,
                    ..CqUnit::default()
                };

                if ConsumeQueueExt::is_ext_addr(cq_unit.tags_code) {
                    let mut cq_ext_unit = CqExtUnit::default();
                    let ext_ret = self.get_ext(cq_unit.tags_code, &mut cq_ext_unit);
                    if ext_ret {
                        cq_unit.tags_code = cq_ext_unit.tags_code();
                        cq_unit.cq_ext_unit = Some(cq_ext_unit);
                    } else {
                        error!(
                            "[BUG] can't find consume queue extend file content! addr={}, offsetPy={}, sizePy={}",
                            cq_unit.tags_code, cq_unit.pos, cq_unit.pos,
                        );
                    }
                }
                Some(cq_unit)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::ops::DerefMut;
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::config::TopicConfig;
    use tempfile::tempdir;

    use super::*;
    use crate::base::dispatch_request::DispatchRequest;
    use crate::config::message_store_config::MessageStoreConfig;
    use crate::message_store::local_file_message_store::LocalFileMessageStore;
    use crate::queue::file_queue_life_cycle::FileQueueLifeCycle;
    use crate::queue::local_file_consume_queue_store::ConsumeQueueStore;
    use crate::store_path_config_helper::get_store_path_consume_queue;

    struct TestConsumeQueue {
        queue: ConsumeQueue,
        _queue_store: ConsumeQueueStore,
    }

    impl Deref for TestConsumeQueue {
        type Target = ConsumeQueue;

        fn deref(&self) -> &Self::Target {
            &self.queue
        }
    }

    impl DerefMut for TestConsumeQueue {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.queue
        }
    }

    fn new_test_store(temp_dir: &tempfile::TempDir, mapped_file_size_consume_queue: usize) -> LocalFileMessageStore {
        new_configured_test_store(
            temp_dir,
            MessageStoreConfig {
                mapped_file_size_consume_queue,
                ..MessageStoreConfig::default()
            },
        )
    }

    fn new_configured_test_store(
        temp_dir: &tempfile::TempDir,
        mut config: MessageStoreConfig,
    ) -> LocalFileMessageStore {
        config.store_path_root_dir = temp_dir.path().to_string_lossy().to_string().into();
        LocalFileMessageStore::new(
            Arc::new(config),
            Arc::new(BrokerConfig::default()),
            Arc::new(DashMap::<CheetahString, Arc<TopicConfig>>::new()),
            None,
            false,
        )
    }

    fn new_test_consume_queue(
        temp_dir: &tempfile::TempDir,
        topic: &CheetahString,
        mapped_file_size_consume_queue: usize,
    ) -> TestConsumeQueue {
        let mut store = new_test_store(temp_dir, mapped_file_size_consume_queue);
        let context = store.consume_queue_context();
        let queue_lookup = store.consume_queue_lookup_handle();
        let queue_store = store.consume_queue_store_mut().clone();
        queue_store.set_context(context.clone());
        let queue = ConsumeQueue::new(
            topic.clone(),
            0,
            CheetahString::from_string(get_store_path_consume_queue(
                store.message_store_config_ref().store_path_root_dir.as_str(),
            )),
            mapped_file_size_consume_queue as i32,
            context,
            queue_lookup,
        );
        TestConsumeQueue {
            queue,
            _queue_store: queue_store,
        }
    }

    fn new_configured_test_consume_queue(
        temp_dir: &tempfile::TempDir,
        topic: &CheetahString,
        config: MessageStoreConfig,
    ) -> TestConsumeQueue {
        let mapped_file_size_consume_queue = config.mapped_file_size_consume_queue;
        let mut store = new_configured_test_store(temp_dir, config);
        let context = store.consume_queue_context();
        let queue_lookup = store.consume_queue_lookup_handle();
        let queue_store = store.consume_queue_store_mut().clone();
        queue_store.set_context(context.clone());
        let queue = ConsumeQueue::new(
            topic.clone(),
            0,
            CheetahString::from_string(get_store_path_consume_queue(
                store.message_store_config_ref().store_path_root_dir.as_str(),
            )),
            mapped_file_size_consume_queue as i32,
            context,
            queue_lookup,
        );
        TestConsumeQueue {
            queue,
            _queue_store: queue_store,
        }
    }

    #[test]
    fn delete_expired_file_removes_shutdown_rolled_file() {
        let temp_dir = tempdir().unwrap();
        let topic = CheetahString::from_static_str("single-cq-delete-expired-topic");
        let mut consume_queue = new_test_consume_queue(&temp_dir, &topic, (2 * CQ_STORE_UNIT_SIZE) as usize);

        for (queue_offset, commit_log_offset) in [(0_i64, 0_i64), (1, 32), (2, 64)] {
            consume_queue.put_message_position_info_wrapper(&DispatchRequest {
                topic: topic.clone(),
                queue_id: 0,
                commit_log_offset,
                msg_size: 32,
                consume_queue_offset: queue_offset,
                store_timestamp: 1000 + queue_offset,
                success: true,
                ..DispatchRequest::default()
            });
        }

        assert_eq!(consume_queue.mapped_file_queue.get_mapped_files_size(), 2);
        let first_file = consume_queue
            .mapped_file_queue
            .get_first_mapped_file()
            .expect("first mapped file");
        first_file.shutdown(0);

        assert_eq!(consume_queue.delete_expired_file(64), 1);
        assert_eq!(consume_queue.mapped_file_queue.get_mapped_files_size(), 1);
    }

    #[test]
    fn swap_methods_delegate_without_panicking() {
        let temp_dir = tempdir().unwrap();
        let topic = CheetahString::from_static_str("single-cq-swap-topic");
        let mut consume_queue = new_test_consume_queue(&temp_dir, &topic, (2 * CQ_STORE_UNIT_SIZE) as usize);

        for (queue_offset, commit_log_offset) in [(0_i64, 0_i64), (1, 32), (2, 64), (3, 96)] {
            consume_queue.put_message_position_info_wrapper(&DispatchRequest {
                topic: topic.clone(),
                queue_id: 0,
                commit_log_offset,
                msg_size: 32,
                consume_queue_offset: queue_offset,
                store_timestamp: 2000 + queue_offset,
                success: true,
                ..DispatchRequest::default()
            });
        }

        consume_queue.swap_map(3, 0, 0);
        consume_queue.clean_swapped_map(0);
        assert_eq!(consume_queue.mapped_file_queue.get_mapped_files_size(), 2);
    }

    #[test]
    fn consume_queue_round_trips_cqext_bitmap_on_iterate() {
        let temp_dir = tempdir().unwrap();
        let topic = CheetahString::from_static_str("single-cq-cqext-topic");
        let mut consume_queue = new_configured_test_consume_queue(
            &temp_dir,
            &topic,
            MessageStoreConfig {
                enable_consume_queue_ext: true,
                mapped_file_size_consume_queue: (2 * CQ_STORE_UNIT_SIZE) as usize,
                mapped_file_size_consume_queue_ext: 64,
                ..MessageStoreConfig::default()
            },
        );

        let bitmap = vec![0xAB, 0xCD, 0xEF];
        consume_queue.put_message_position_info_wrapper(&DispatchRequest {
            topic: topic.clone(),
            queue_id: 0,
            commit_log_offset: 123,
            msg_size: 32,
            tags_code: 456,
            consume_queue_offset: 0,
            store_timestamp: 7890,
            bit_map: Some(bitmap.clone()),
            success: true,
            ..DispatchRequest::default()
        });

        let cq_unit = consume_queue.get(0).expect("consume queue unit");
        assert_eq!(cq_unit.tags_code, 456);
        let cq_ext_unit = cq_unit.cq_ext_unit.expect("cq ext unit");
        assert_eq!(cq_ext_unit.tags_code(), 456);
        assert_eq!(cq_ext_unit.msg_store_time(), 7890);
        assert_eq!(cq_ext_unit.filter_bit_map(), Some(bitmap.as_slice()));
    }

    #[test]
    fn consume_queue_borrowed_iterator_honors_request_count() {
        let temp_dir = tempdir().unwrap();
        let topic = CheetahString::from_static_str("single-cq-bounded-iterator-topic");
        let mut consume_queue = new_test_consume_queue(&temp_dir, &topic, (8 * CQ_STORE_UNIT_SIZE) as usize);

        for queue_offset in 0_i64..4 {
            consume_queue.put_message_position_info_wrapper(&DispatchRequest {
                topic: topic.clone(),
                queue_id: 0,
                commit_log_offset: queue_offset * 32,
                msg_size: 32,
                consume_queue_offset: queue_offset,
                success: true,
                ..DispatchRequest::default()
            });
        }

        let records = consume_queue
            .iterate_from_with_count(0, 2)
            .expect("bounded iterator")
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].queue_offset, 0);
        assert_eq!(records[1].queue_offset, 1);
        assert_eq!(
            consume_queue
                .iterate_from_with_count(0, 0)
                .expect("empty bounded iterator")
                .count(),
            0
        );
    }

    #[test]
    fn consume_queue_flush_and_total_size_include_cqext() {
        let temp_dir = tempdir().unwrap();
        let topic = CheetahString::from_static_str("single-cq-cqext-flush-topic");
        let mut consume_queue = new_configured_test_consume_queue(
            &temp_dir,
            &topic,
            MessageStoreConfig {
                enable_consume_queue_ext: true,
                mapped_file_size_consume_queue: (2 * CQ_STORE_UNIT_SIZE) as usize,
                mapped_file_size_consume_queue_ext: 64,
                ..MessageStoreConfig::default()
            },
        );

        consume_queue.put_message_position_info_wrapper(&DispatchRequest {
            topic: topic.clone(),
            queue_id: 0,
            commit_log_offset: 321,
            msg_size: 32,
            tags_code: 654,
            consume_queue_offset: 0,
            store_timestamp: 9876,
            bit_map: Some(vec![1, 2, 3, 4]),
            success: true,
            ..DispatchRequest::default()
        });

        let ext_queue = consume_queue
            .consume_queue_ext
            .as_ref()
            .expect("consume queue ext should exist");
        let base_total_size: i64 = consume_queue
            .mapped_file_queue
            .get_mapped_files()
            .load()
            .iter()
            .map(|mapped_file| mapped_file.get_file_size() as i64)
            .sum();

        let _ = consume_queue.flush(0);
        let _ = ext_queue.flush(0);
        assert_eq!(
            consume_queue.get_total_size(),
            base_total_size + ext_queue.get_total_size()
        );
    }

    #[test]
    fn truncate_dirty_logic_files_applies_the_local_retained_prefix() {
        let temp_dir = tempdir().unwrap();
        let topic = CheetahString::from_static_str("single-cq-truncate-topic");
        let mut consume_queue = new_test_consume_queue(&temp_dir, &topic, (4 * CQ_STORE_UNIT_SIZE) as usize);

        for (queue_offset, commit_log_offset) in [(0_i64, 0_i64), (1, 32), (2, 64)] {
            consume_queue.put_message_position_info_wrapper(&DispatchRequest {
                topic: topic.clone(),
                queue_id: 0,
                commit_log_offset,
                msg_size: 32,
                consume_queue_offset: queue_offset,
                success: true,
                ..DispatchRequest::default()
            });
        }

        consume_queue.truncate_dirty_logic_files_handler(64, true);

        let mapped_file = consume_queue
            .mapped_file_queue
            .get_last_mapped_file()
            .expect("retained mapped file");
        assert_eq!(mapped_file.get_wrote_position(), 2 * CQ_STORE_UNIT_SIZE);
        assert_eq!(mapped_file.get_committed_position(), 2 * CQ_STORE_UNIT_SIZE);
        assert_eq!(mapped_file.get_flushed_position(), 2 * CQ_STORE_UNIT_SIZE);
        assert_eq!(consume_queue.get_max_physic_offset(), 64);
    }

    #[test]
    fn correct_min_offset_applies_the_local_record_match() {
        let temp_dir = tempdir().unwrap();
        let topic = CheetahString::from_static_str("single-cq-min-offset-topic");
        let mut consume_queue = new_test_consume_queue(&temp_dir, &topic, (4 * CQ_STORE_UNIT_SIZE) as usize);

        for (queue_offset, commit_log_offset) in [(0_i64, 0_i64), (1, 32), (2, 64)] {
            consume_queue.put_message_position_info_wrapper(&DispatchRequest {
                topic: topic.clone(),
                queue_id: 0,
                commit_log_offset,
                msg_size: 32,
                consume_queue_offset: queue_offset,
                success: true,
                ..DispatchRequest::default()
            });
        }

        consume_queue.correct_min_offset(32);

        assert_eq!(consume_queue.get_min_logic_offset(), CQ_STORE_UNIT_SIZE as i64);
        assert_eq!(consume_queue.get_min_offset_in_queue(), 1);
    }
}
