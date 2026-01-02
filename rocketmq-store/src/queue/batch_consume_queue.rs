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
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::attribute::cq_type::CQType;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use tracing::info;

use crate::base::dispatch_request::DispatchRequest;
use crate::base::swappable::Swappable;
use crate::config::message_store_config::MessageStoreConfig;
use crate::consume_queue::mapped_file_queue::MappedFileQueue;
use crate::filter::MessageFilter;
use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::queue::consume_queue::ConsumeQueueTrait;
use crate::queue::queue_offset_operator::QueueOffsetOperator;
use crate::queue::CqUnit;
use crate::queue::FileQueueLifeCycle;

const CQ_STORE_UNIT_SIZE: i32 = 46;
const MSG_TAG_OFFSET_INDEX: i32 = 12;
const MSG_STORE_TIME_OFFSET_INDEX: i32 = 20;
const MSG_BASE_OFFSET_INDEX: i32 = 28;
const MSG_BATCH_SIZE_INDEX: i32 = 36;
const MSG_COMPACT_OFFSET_INDEX: i32 = 38;
const MSG_COMPACT_OFFSET_LENGTH: i32 = 4;
const INVALID_POS: i32 = -1;

///
/// BatchConsumeQueue's store unit. Format:
///
/// ┌─────────────────────────┬───────────┬────────────┬──────────┐
/// │CommitLog Physical Offset│ Body Size │Tag HashCode│Store time│
/// │        (8 Bytes)        │ (4 Bytes) │ (8 Bytes)  │(8 Bytes) │
/// ├─────────────────────────┼───────────┼────────────┼──────────┤
/// │       msgBaseOffset     │ batchSize │compOffset  │ reserved │
/// │         (8 Bytes)       │ (2 Bytes) │ (4 Bytes)  │(4 Bytes) │
///Store Unit
/// BatchConsumeQueue's store unit. Size:
/// CommitLog Physical Offset(8) + Body Size(4) + Tag HashCode(8) + Store time(8) +
/// msgBaseOffset(8) + batchSize(2) + compactedOffset(4) + reserved(4)= 46 Bytes
pub struct BatchConsumeQueue {
    message_store_config: Arc<MessageStoreConfig>,
    mapped_file_queue: MappedFileQueue,
    //message_store: Arc<RwLock<dyn MessageStore>>,
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

        let byte_buffer_item = vec![0u8; CQ_STORE_UNIT_SIZE as usize];

        BatchConsumeQueue {
            message_store_config,
            mapped_file_queue,
            topic,
            queue_id,
            byte_buffer_item,
            store_path,
            mapped_file_size,
            max_msg_phy_offset_in_commit_log: Arc::new(AtomicI64::new(-1)),
            min_logic_offset: Arc::new(AtomicI64::new(0)),
            max_offset_in_queue: Arc::new(AtomicI64::new(0)),
            min_offset_in_queue: Arc::new(AtomicI64::new(-1)),
            commit_log_size: commit_log_size as i32,
            offset_cache: Arc::new(parking_lot::RwLock::new(BTreeMap::new())),
            time_cache: Arc::new(parking_lot::RwLock::new(BTreeMap::new())),
        }
    }
}

#[allow(unused_variables)]
impl FileQueueLifeCycle for BatchConsumeQueue {
    #[inline]
    fn load(&mut self) -> bool {
        let result = self.mapped_file_queue.load();
        info!(
            "Load batch consume queue {}-{}  {} {}",
            self.topic,
            self.queue_id,
            if result { "OK" } else { "Failed" },
            self.mapped_file_queue.get_mapped_files_size()
        );
        result
    }

    #[inline]
    fn recover(&mut self) {
        todo!()
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
        todo!()
    }

    #[inline]
    fn truncate_dirty_logic_files(&mut self, max_commit_log_pos: i64) {
        todo!()
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

impl Swappable for BatchConsumeQueue {
    #[inline]
    fn swap_map(&self, reserve_num: i32, force_swap_interval_ms: i64, normal_swap_interval_ms: i64) {
        todo!()
    }

    #[inline]
    fn clean_swapped_map(&self, force_clean_swap_interval_ms: i64) {
        todo!()
    }
}

impl ConsumeQueueTrait for BatchConsumeQueue {
    #[inline]
    fn get_topic(&self) -> &CheetahString {
        todo!()
    }

    #[inline]
    fn get_queue_id(&self) -> i32 {
        todo!()
    }

    #[inline]
    fn get(&self, index: i64) -> Option<CqUnit> {
        todo!()
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
    fn get_earliest_unit(&self) -> Option<CqUnit> {
        todo!()
    }

    #[inline]
    fn get_latest_unit(&self) -> Option<CqUnit> {
        todo!()
    }

    #[inline]
    fn get_last_offset(&self) -> i64 {
        todo!()
    }

    #[inline]
    fn get_min_offset_in_queue(&self) -> i64 {
        todo!()
    }

    #[inline]
    fn get_max_offset_in_queue(&self) -> i64 {
        todo!()
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
    fn get_max_physic_offset(&self) -> i64 {
        todo!()
    }

    #[inline]
    fn get_min_logic_offset(&self) -> i64 {
        todo!()
    }

    #[inline]
    fn get_cq_type(&self) -> CQType {
        todo!()
    }

    #[inline]
    fn get_total_size(&self) -> i64 {
        todo!()
    }

    #[inline]
    fn get_unit_size(&self) -> i32 {
        todo!()
    }

    #[inline]
    fn correct_min_offset(&self, min_commit_log_offset: i64) {
        todo!()
    }

    #[inline]
    fn put_message_position_info_wrapper(&mut self, request: &DispatchRequest) {
        todo!()
    }

    #[inline]
    fn increase_queue_offset(
        &self,
        queue_offset_assigner: &QueueOffsetOperator,
        msg: &MessageExtBrokerInner,
        message_num: i16,
    ) {
        todo!()
    }

    #[inline]
    fn assign_queue_offset(&self, queue_offset_operator: &QueueOffsetOperator, msg: &mut MessageExtBrokerInner) {
        todo!()
    }

    #[inline]
    fn estimate_message_count(&self, from: i64, to: i64, filter: &dyn MessageFilter) -> i64 {
        todo!()
    }

    #[inline]
    fn iterate_from(&self, start_index: i64) -> Option<Box<dyn Iterator<Item = CqUnit> + Send + '_>> {
        todo!()
    }

    fn iterate_from_with_count(
        &self,
        start_index: i64,
        _count: i32,
    ) -> Option<Box<dyn Iterator<Item = CqUnit> + Send + '_>> {
        todo!()
    }

    fn get_offset_in_queue_by_time_with_boundary(&self, timestamp: i64, boundary_type: BoundaryType) -> i64 {
        todo!()
    }
}
