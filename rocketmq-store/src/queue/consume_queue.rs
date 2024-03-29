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

use std::collections::HashMap;

use rocketmq_common::common::{
    attribute::cq_type::CQType, boundary_type::BoundaryType,
    message::message_single::MessageExtBrokerInner,
};

use crate::{
    base::dispatch_request::DispatchRequest,
    consume_queue::consume_queue_ext::ConsumeQueueExtCqExtUnit,
    filter::MessageFilter,
    queue::{file_queue::FileQueueLifeCycle, queue_offset_operator::QueueOffsetOperator},
};

pub struct CqUnit {
    pub queue_offset: i64,
    pub size: i32,
    pub pos: i64,
    pub batch_num: i16,
    pub tags_code: i64,
    pub cq_ext_unit: Option<ConsumeQueueExtCqExtUnit>,
    pub native_buffer: Vec<u8>,
    pub compacted_offset: i32,
}

/// Trait representing ConsumeQueueStoreInterface.
pub trait ConsumeQueueStoreInterface {
    /// Start the consumeQueueStore.
    fn start(&self);

    /// Load from file.
    /// Returns true if loaded successfully.
    fn load(&self) -> bool;

    /// Load after destroy.
    fn load_after_destroy(&self) -> bool;

    /// Recover from file.
    fn recover(&self);

    /// Recover concurrently from file.
    /// Returns true if recovered successfully.
    fn recover_concurrently(&self) -> bool;

    /// Shutdown the consumeQueueStore.
    /// Returns true if shutdown successfully.
    fn shutdown(&self) -> bool;

    /// Destroy all consumeQueues.
    fn destroy(&self);

    /// Destroy the specific consumeQueue.
    /// Throws RocksDBException only in rocksdb mode.
    fn destroy_consume_queue(&self, consume_queue: &dyn ConsumeQueueInterface);

    /// Flush cache to file.
    /// `consume_queue`: The consumeQueue to be flushed.
    /// `flush_least_pages`: The minimum number of pages to be flushed.
    /// Returns true if any data has been flushed.
    fn flush(&self, consume_queue: &dyn ConsumeQueueInterface, flush_least_pages: i32) -> bool;

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
        consume_queue: &dyn ConsumeQueueInterface,
        min_commit_log_pos: i64,
    ) -> i32;

    /// Is the first file available?
    /// `consume_queue`: The consumeQueue.
    /// Returns true if it's available.
    fn is_first_file_available(&self, consume_queue: &dyn ConsumeQueueInterface) -> bool;

    /// Does the first file exist?
    /// `consume_queue`: The consumeQueue.
    /// Returns true if it exists.
    fn is_first_file_exist(&self, consume_queue: &dyn ConsumeQueueInterface) -> bool;

    /// Roll to next file.
    /// `consume_queue`: The consumeQueue.
    /// `offset`: Next beginning offset.
    /// Returns the beginning offset of the next file.
    fn roll_next_file(&self, consume_queue: &dyn ConsumeQueueInterface, offset: i64) -> i64;

    /// Truncate dirty data.
    /// `offset_to_truncate`: Offset to truncate.
    /// Throws RocksDBException only in rocksdb mode.
    fn truncate_dirty(&self, offset_to_truncate: i64);

    /// Apply the dispatched request and build the consume queue. This function should be
    /// idempotent.
    ///
    /// `consume_queue`: Consume queue.
    /// `request`: Dispatch request.
    fn put_message_position_info_wrapper(
        &self,
        consume_queue: &dyn ConsumeQueueInterface,
        request: DispatchRequest,
    );

    /// Apply the dispatched request. This function should be idempotent.
    ///
    /// `request`: Dispatch request.
    /// Throws RocksDBException only in rocksdb mode.
    fn put_message_position_info_wrapper_single(&self, request: DispatchRequest);

    /// Range query cqUnit(ByteBuffer) in rocksdb.
    /// `topic`: Topic.
    /// `queue_id`: Queue ID.
    /// `start_index`: Start index.
    /// `num`: Number of items.
    /// Returns the byteBuffer list of the topic-queueId in rocksdb.
    /// Throws RocksDBException only in rocksdb mode.
    /* fn range_query(
        &self,
        topic: &str,
        queue_id: i32,
        start_index: i64,
        num: i32,
    ) -> Result<Vec<ByteBuffer>, RocksDBException>;*/

    /// Query cqUnit(ByteBuffer) in rocksdb.
    /// `topic`: Topic.
    /// `queue_id`: Queue ID.
    /// `start_index`: Start index.
    /// Returns the byteBuffer of the topic-queueId in rocksdb.
    /// Throws RocksDBException only in rocksdb mode.
    /*fn get(
        &self,
        topic: &str,
        queue_id: i32,
        start_index: i64,
    ) -> Result<ByteBuffer, RocksDBException>;*/

    /// Get consumeQueue table.
    fn get_consume_queue_table(
        &self,
    ) -> HashMap<String, HashMap<i32, Box<dyn ConsumeQueueInterface>>>;

    /// Assign queue offset.
    /// `msg`: Message itself.
    /// Throws RocksDBException only in rocksdb mode.
    /* fn assign_queue_offset(&self, msg: MessageExtBrokerInner) -> Result<(), RocksDBException>; */

    /// Increase queue offset.
    /// `msg`: Message itself.
    /// `message_num`: Message number.
    fn increase_queue_offset(&self, msg: MessageExtBrokerInner, message_num: i16);

    /// Increase lmq offset.
    /// `queue_key`: Queue key.
    /// `message_num`: Message number.
    fn increase_lmq_offset(&self, queue_key: &str, message_num: i16);

    /// Get lmq queue offset.
    /// `queue_key`: Queue key.
    fn get_lmq_queue_offset(&self, queue_key: &str) -> i64;

    /// Recover topicQueue table by minPhyOffset.
    /// `min_phy_offset`: Minimum physical offset.
    fn recover_offset_table(&self, min_phy_offset: i64);

    /// Set topicQueue table.
    /// `topic_queue_table`: Topic queue table.
    fn set_topic_queue_table(&self, topic_queue_table: HashMap<String, i64>);

    /// Remove topic-queueId from topicQueue table.
    /// `topic`: Topic.
    /// `queue_id`: Queue ID.
    fn remove_topic_queue_table(&self, topic: &str, queue_id: i32);

    /// Get topicQueue table.
    fn get_topic_queue_table(&self) -> HashMap<String, i64>;

    /// Get the max physical offset in consumeQueue.
    /// `topic`: Topic.
    /// `queue_id`: Queue ID.
    fn get_max_phy_offset_in_consume_queue(&self, topic: &str, queue_id: i32) -> i64;

    /// Get maxOffset of specific topic-queueId in topicQueue table.
    /// `topic`: Topic.
    /// `queue_id`: Queue ID.
    /// Returns the max offset in QueueOffsetOperator.
    fn get_max_offset(&self, topic: &str, queue_id: i32) -> i64;

    /// Get max physic offset in consumeQueue.
    /// Returns the max physic offset in consumeQueue.
    /// Throws RocksDBException only in rocksdb mode.
    /* fn get_max_phy_offset_in_consume_queue_total(&self) -> Result<i64, RocksDBException>; */

    /// Get min logic offset of specific topic-queueId in consumeQueue.
    /// `topic`: Topic.
    /// `queue_id`: Queue ID.
    /// Returns the min logic offset of specific topic-queueId in consumeQueue.
    /// Throws RocksDBException only in rocksdb mode.
    /* fn get_min_offset_in_queue(&self, topic: &str, queue_id: i32) -> Result<i64,
     * RocksDBException>; */

    /// Get max logic offset of specific topic-queueId in consumeQueue.
    /// `topic`: Topic.
    /// `queue_id`: Queue ID.
    /// Returns the max logic offset of specific topic-queueId in consumeQueue.
    /// Throws RocksDBException only in rocksdb mode.
    /* fn get_max_offset_in_queue(&self, topic: &str, queue_id: i32) -> Result<i64,
     * RocksDBException>; */

    /// Get the message whose timestamp is the smallest, greater than or equal to the given time
    /// and when there are more than one message satisfy the condition,
    /// decide which one to return based on boundaryType.
    ///
    /// `topic`: Topic.
    /// `queue_id`: Queue ID.
    /// `timestamp`: Timestamp.
    /// `boundary_type`: Lower or Upper.
    /// Returns the offset(index).
    /// Throws RocksDBException only in rocksdb mode.
    /*    fn get_offset_in_queue_by_time(
            &self,
            topic: &str,
            queue_id: i32,
            timestamp: i64,
            boundary_type: BoundaryType,
        ) -> Result<i64, RocksDBException>;
    */
    /// Find or create the consumeQueue.
    /// `topic`: Topic.
    /// `queue_id`: Queue ID.
    /// Returns the consumeQueue.
    fn find_or_create_consume_queue(
        &self,
        topic: &str,
        queue_id: i32,
    ) -> Box<dyn ConsumeQueueInterface>;

    /// Find the consumeQueueMap of topic.
    /// `topic`: Topic.
    /// Returns the consumeQueueMap of topic.
    fn find_consume_queue_map(
        &self,
        topic: &str,
    ) -> Option<HashMap<i32, Box<dyn ConsumeQueueInterface>>>;

    /// Get the total size of all consumeQueue.
    /// Returns the total size of all consumeQueue.
    fn get_total_size(&self) -> i64;

    /// Get store time from commitlog by cqUnit.
    /// `cq_unit`: cqUnit.
    fn get_store_time(&self, cq_unit: CqUnit) -> i64;
}

/// Trait representing ConsumeQueueInterface.
pub trait ConsumeQueueInterface: FileQueueLifeCycle {
    /// Get the topic name.
    fn get_topic(&self) -> String;

    /// Get queue id.
    fn get_queue_id(&self) -> i32;

    /// Get the units from the start offset.
    fn iterate_from(&self, start_index: i64) -> Box<dyn Iterator<Item = CqUnit>>;

    /// Get the units from the start offset.
    /*    fn iterate_from_count(
        &self,
        start_index: i64,
        count: i32,
    ) -> Result<Box<dyn Iterator<Item = CqUnit>>, RocksDBException>;*/

    /// Get cq unit at specified index.
    fn get(&self, index: i64) -> CqUnit;

    /// Get earliest cq unit.
    /*    fn get_earliest_unit_and_store_time(
            &self,
            index: i64,
        ) -> Result<Pair<CqUnit, i64>, RocksDBException>;
    */
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
    fn put_message_position_info_wrapper(&self, request: DispatchRequest);

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
        queue_offset_assigner: QueueOffsetOperator,
        msg: MessageExtBrokerInner,
        message_num: i16,
    );

    /// Estimate number of records matching given filter.
    fn estimate_message_count(&self, from: i64, to: i64, filter: &dyn MessageFilter) -> i64;
}
