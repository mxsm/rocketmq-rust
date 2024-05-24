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
mod batch_consume_queue;
pub mod build_consume_queue;
mod consume_queue_ext;
pub mod local_file_consume_queue_store;
mod queue_offset_operator;
pub mod single_consume_queue;

use std::{collections::HashMap, sync::Arc};

use rocketmq_common::common::{
    attribute::cq_type::CQType, boundary_type::BoundaryType,
    message::message_single::MessageExtBrokerInner,
};

use crate::{
    base::{dispatch_request::DispatchRequest, swappable::Swappable},
    consume_queue::consume_queue_ext::CqExtUnit,
    filter::MessageFilter,
    queue::queue_offset_operator::QueueOffsetOperator,
};

/// FileQueueLifeCycle contains life cycle methods of ConsumerQueue that is directly implemented by
/// FILE.
pub trait FileQueueLifeCycle: Swappable {
    /// Load from file.
    /// Returns true if loaded successfully.
    fn load(&mut self) -> bool;

    /// Recover from file.
    fn recover(&mut self);

    /// Check files.
    fn check_self(&self);

    /// Flush cache to file.
    /// `flush_least_pages`: The minimum number of pages to be flushed.
    /// Returns true if any data has been flushed.
    fn flush(&self, flush_least_pages: i32) -> bool;

    /// Destroy files.
    fn destroy(&mut self);

    /// Truncate dirty logic files starting at max commit log position.
    /// `max_commit_log_pos`: Max commit log position.
    fn truncate_dirty_logic_files(&mut self, max_commit_log_pos: i64);

    /// Delete expired files ending at min commit log position.
    /// `min_commit_log_pos`: Min commit log position.
    /// Returns deleted file numbers.
    fn delete_expired_file(&self, min_commit_log_pos: i64) -> i32;

    /// Roll to next file.
    /// `next_begin_offset`: Next begin offset.
    /// Returns the beginning offset of the next file.
    fn roll_next_file(&self, next_begin_offset: i64) -> i64;

    /// Is the first file available?
    /// Returns true if it's available.
    fn is_first_file_available(&self) -> bool;

    /// Does the first file exist?
    /// Returns true if it exists.
    fn is_first_file_exist(&self) -> bool;
}

pub struct CqUnit {
    pub queue_offset: i64,
    pub size: i32,
    pub pos: i64,
    pub batch_num: i16,
    pub tags_code: i64,
    pub cq_ext_unit: Option<CqExtUnit>,
    pub native_buffer: Vec<u8>,
    pub compacted_offset: i32,
}

/// Trait representing ConsumeQueueStoreInterface.
pub trait ConsumeQueueStoreTrait: Send + Sync {
    /// Start the consumeQueueStore.
    fn start(&self);

    /// Load from file.
    /// Returns true if loaded successfully.
    fn load(&mut self) -> bool;

    /// Load after destroy.
    fn load_after_destroy(&self) -> bool;

    /// Recover from file.
    fn recover(&mut self);

    /// Recover concurrently from file.
    /// Returns true if recovered successfully.
    fn recover_concurrently(&mut self) -> bool;

    /// Shutdown the consumeQueueStore.
    /// Returns true if shutdown successfully.
    fn shutdown(&self) -> bool;

    /// Destroy all consumeQueues.
    fn destroy(&self);

    /// Destroy the specific consumeQueue.
    /// Throws RocksDBException only in rocksdb mode.
    fn destroy_consume_queue(&self, consume_queue: &dyn ConsumeQueueTrait);

    /// Flush cache to file.
    /// `consume_queue`: The consumeQueue to be flushed.
    /// `flush_least_pages`: The minimum number of pages to be flushed.
    /// Returns true if any data has been flushed.
    fn flush(&self, consume_queue: &dyn ConsumeQueueTrait, flush_least_pages: i32) -> bool;

    /// Clean expired data from minPhyOffset.
    /// `min_phy_offset`: Minimum physical offset.
    fn clean_expired(&self, min_phy_offset: i64);

    /// Check files.
    fn check_self(&self);

    /// Delete expired files ending at min commit log position.
    /// `consume_queue`: The consumeQueue.
    /// `min_commit_log_pos`: Minimum commit log position.
    /// Returns deleted file numbers.
    fn delete_expired_file(
        &self,
        consume_queue: &dyn ConsumeQueueTrait,
        min_commit_log_pos: i64,
    ) -> i32;

    /// Is the first file available?
    /// `consume_queue`: The consumeQueue.
    /// Returns true if it's available.
    fn is_first_file_available(&self, consume_queue: &dyn ConsumeQueueTrait) -> bool;

    /// Does the first file exist?
    /// `consume_queue`: The consumeQueue.
    /// Returns true if it exists.
    fn is_first_file_exist(&self, consume_queue: &dyn ConsumeQueueTrait) -> bool;

    /// Roll to next file.
    /// `consume_queue`: The consumeQueue.
    /// `offset`: Next beginning offset.
    /// Returns the beginning offset of the next file.
    fn roll_next_file(&self, consume_queue: &dyn ConsumeQueueTrait, offset: i64) -> i64;

    /// Truncate dirty data.
    /// `offset_to_truncate`: Offset to truncate.
    /// Throws RocksDBException only in rocksdb mode.
    fn truncate_dirty(&self, offset_to_truncate: i64);

    /// Apply the dispatched request. This function should be idempotent.
    ///
    /// `request`: Dispatch request.
    /// Throws RocksDBException only in rocksdb mode.
    fn put_message_position_info_wrapper(&self, request: &DispatchRequest);

    fn put_message_position_info_wrapper_with_cq(
        &self,
        consume_queue: &mut dyn ConsumeQueueTrait,
        request: &DispatchRequest,
    );

    fn range_query(
        &self,
        topic: &str,
        queue_id: i32,
        start_index: i64,
        num: i32,
    ) -> Option<Vec<bytes::Bytes>>;

    fn get_signal(&self, topic: &str, queue_id: i32, start_index: i64) -> Option<bytes::Bytes>;

    /// Increase queue offset.
    /// `msg`: Message itself.
    /// `message_num`: Message number.
    fn increase_queue_offset(&self, msg: &MessageExtBrokerInner, message_num: i16);

    fn assign_queue_offset(&self, msg: &mut MessageExtBrokerInner);

    /// Increase lmq offset.
    /// `queue_key`: Queue key.
    /// `message_num`: Message number.
    fn increase_lmq_offset(&mut self, queue_key: &str, message_num: i16);

    /// Get lmq queue offset.
    /// `queue_key`: Queue key.
    fn get_lmq_queue_offset(&self, queue_key: &str) -> i64;

    /// Recover topicQueue table by minPhyOffset.
    /// `min_phy_offset`: Minimum physical offset.
    fn recover_offset_table(&mut self, min_phy_offset: i64);

    /// Set topicQueue table.
    /// `topic_queue_table`: Topic queue table.
    fn set_topic_queue_table(&mut self, topic_queue_table: HashMap<String, i64>);

    /// Remove topic-queueId from topicQueue table.
    /// `topic`: Topic.
    /// `queue_id`: Queue ID.
    fn remove_topic_queue_table(&mut self, topic: &str, queue_id: i32);

    /// Get topicQueue table.
    fn get_topic_queue_table(&self) -> HashMap<String, i64>;

    /// Get the max physical offset in consumeQueue.
    /// `topic`: Topic.
    /// `queue_id`: Queue ID.
    fn get_max_phy_offset_in_consume_queue_id(&self, topic: &str, queue_id: i32) -> i64;

    fn get_max_phy_offset_in_consume_queue(&self) -> i64;

    /// Get maxOffset of specific topic-queueId in topicQueue table.
    /// `topic`: Topic.
    /// `queue_id`: Queue ID.
    /// Returns the max offset in QueueOffsetOperator.
    fn get_max_offset(&self, topic: &str, queue_id: i32) -> i64;

    /// Find or create the consumeQueue.
    /// `topic`: Topic.
    /// `queue_id`: Queue ID.
    /// Returns the consumeQueue.
    fn find_or_create_consume_queue(
        &self,
        topic: &str,
        queue_id: i32,
    ) -> Arc<parking_lot::Mutex<Box<dyn ConsumeQueueTrait>>>;

    /// Find the consumeQueueMap of topic.
    /// `topic`: Topic.
    /// Returns the consumeQueueMap of topic.
    fn find_consume_queue_map(
        &self,
        topic: &str,
    ) -> Option<HashMap<i32, Box<dyn ConsumeQueueTrait>>>;

    /// Get the total size of all consumeQueue.
    /// Returns the total size of all consumeQueue.
    fn get_total_size(&self) -> i64;

    /// Get store time from commitlog by cqUnit.
    /// `cq_unit`: cqUnit.
    fn get_store_time(&self, cq_unit: CqUnit) -> i64;
}

/// Trait representing ConsumeQueueInterface.
pub trait ConsumeQueueTrait: Send + Sync + FileQueueLifeCycle {
    /// Get the topic name.
    fn get_topic(&self) -> String;

    /// Get queue id.
    fn get_queue_id(&self) -> i32;

    /// Get the units from the start offset.
    /*    fn iterate_from_count(
        &self,
        start_index: i64,
        count: i32,
    ) -> Result<Box<dyn Iterator<Item = CqUnit>>, RocksDBException>;*/

    /// Get cq unit at specified index.
    fn get(&self, index: i64) -> CqUnit;

    fn get_cq_unit_and_store_time(&self, index: i64) -> Option<(CqUnit, i64)>;

    /// Get earliest cq unit.
    fn get_earliest_unit_and_store_time(&self) -> Option<(CqUnit, i64)>;

    /// Get earliest cq unit.
    fn get_earliest_unit(&self) -> CqUnit;

    /// Get last cq unit.
    fn get_latest_unit(&self) -> CqUnit;

    /// Get last commit log offset.
    fn get_last_offset(&self) -> i64;

    /// Get min offset(index) in queue.
    fn get_min_offset_in_queue(&self) -> i64;

    /// Get max offset(index) in queue.
    fn get_max_offset_in_queue(&self) -> i64;

    /// Get total message count.
    fn get_message_total_in_queue(&self) -> i64;

    /// Get the message whose timestamp is the smallest, greater than or equal to the given time.
    fn get_offset_in_queue_by_time(&self, timestamp: i64) -> i64;

    /// Get the message whose timestamp is the smallest, greater than or equal to the given time
    /// and when there are more than one message satisfy the condition, decide which one to return
    /// based on boundaryType.
    fn get_offset_in_queue_by_time_boundary(
        &self,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64;

    /// The max physical offset of commitlog has been dispatched to this queue.
    fn get_max_physic_offset(&self) -> i64;

    /// Usually, the cq files are not exactly consistent with the commitlog, there maybe some
    /// redundant data in the first cq file.
    fn get_min_logic_offset(&self) -> i64;

    /// Get cq type.
    fn get_cq_type(&self) -> CQType;

    /// Gets the occupied size of CQ file on disk.
    fn get_total_size(&self) -> i64;

    /// Get the unit size of this CQ which is different in different CQ impl.
    fn get_unit_size(&self) -> i32;

    /// Correct min offset by min commit log offset.
    fn correct_min_offset(&self, min_commit_log_offset: i64);

    /// Do dispatch.
    fn put_message_position_info_wrapper(&mut self, request: &DispatchRequest);

    /// Assign queue offset.
    /*    fn assign_queue_offset(
            &self,
            queue_offset_assigner: &dyn QueueOffsetOperator,
            msg: MessageExtBrokerInner,
        ) -> Result<(), RocksDBException>;
    */
    /// Increase queue offset.
    fn increase_queue_offset(
        &self,
        queue_offset_assigner: &QueueOffsetOperator,
        msg: &MessageExtBrokerInner,
        message_num: i16,
    );

    fn assign_queue_offset(
        &self,
        queue_offset_operator: &QueueOffsetOperator,
        msg: &mut MessageExtBrokerInner,
    );

    /// Estimate number of records matching given filter.
    fn estimate_message_count(&self, from: i64, to: i64, filter: &dyn MessageFilter) -> i64;
}
