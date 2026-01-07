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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;

use crate::base::dispatch_request::DispatchRequest;
use crate::queue::consume_queue::ConsumeQueueTrait;
use crate::queue::ArcConsumeQueue;
use crate::queue::ConsumeQueueTable;
use crate::queue::CqUnit;

#[trait_variant::make(ConsumeQueueStoreTrait: Send)]
pub trait ConsumeQueueStoreInterface: Sync + Any {
    /// Start the consume queue store
    fn start(&self);

    /// Load from file
    ///
    /// # Returns
    /// `true` if loaded successfully
    fn load(&mut self) -> bool;

    /// Load after destroy
    ///
    /// # Returns
    /// `true` if loaded successfully
    fn load_after_destroy(&self) -> bool;

    /// Recover from file
    async fn recover(&self);

    /// Recover concurrently from file
    ///
    /// # Returns
    /// `true` if recovered successfully
    async fn recover_concurrently(&self) -> bool;

    /// Shutdown the consume queue store
    ///
    /// # Returns
    /// `true` if shutdown successfully
    fn shutdown(&self) -> bool;

    /// Destroy all consume queues
    fn destroy(&self);

    /// Destroy the specific consume queue
    ///
    /// # Parameters
    /// * `consume_queue` - The consume queue to destroy
    ///
    /// # Returns
    /// Result indicating success or failure
    fn destroy_queue(&self, consume_queue: &dyn ConsumeQueueTrait);

    /// Flush cache to file
    ///
    /// # Parameters
    /// * `consume_queue` - The consume queue to flush
    /// * `flush_least_pages` - The minimum number of pages to flush
    ///
    /// # Returns
    /// `true` if any data has been flushed
    fn flush(&self, consume_queue: &dyn ConsumeQueueTrait, flush_least_pages: i32) -> bool;

    /// Clean expired data from min physical offset
    ///
    /// # Parameters
    /// * `min_phy_offset` - The minimum physical offset
    async fn clean_expired(&self, min_phy_offset: i64);

    /// Check file integrity
    fn check_self(&self);

    /// Delete expired files ending at min commit log position
    ///
    /// # Parameters
    /// * `consume_queue` - The consume queue to clean
    /// * `min_commit_log_pos` - The minimum commit log position
    ///
    /// # Returns
    /// Number of deleted files
    fn delete_expired_file(&self, consume_queue: &dyn ConsumeQueueTrait, min_commit_log_pos: i64) -> i32;

    /// Check if the first file is available
    ///
    /// # Parameters
    /// * `consume_queue` - The consume queue to check
    ///
    /// # Returns
    /// `true` if the first file is available
    fn is_first_file_available(&self, consume_queue: &dyn ConsumeQueueTrait) -> bool;

    /// Check if the first file exists
    ///
    /// # Parameters
    /// * `consume_queue` - The consume queue to check
    ///
    /// # Returns
    /// `true` if the first file exists
    fn is_first_file_exist(&self, consume_queue: &dyn ConsumeQueueTrait) -> bool;

    /// Roll to next file
    ///
    /// # Parameters
    /// * `consume_queue` - The consume queue to roll
    /// * `offset` - The next beginning offset
    ///
    /// # Returns
    /// The beginning offset of the next file
    fn roll_next_file(&self, consume_queue: &dyn ConsumeQueueTrait, offset: i64) -> i64;

    /// Truncate dirty data
    ///
    /// # Parameters
    /// * `offset_to_truncate` - The offset to truncate
    ///
    /// # Returns
    /// Result indicating success or failure
    fn truncate_dirty(&self, offset_to_truncate: i64);

    /// Apply the dispatched request and build the consume queue
    ///
    /// This function should be idempotent.
    ///
    /// # Parameters
    /// * `consume_queue` - The consume queue
    /// * `request` - The dispatch request
    fn put_message_position_info_wrapper_with_cq(
        &self,
        consume_queue: &mut dyn ConsumeQueueTrait,
        request: &DispatchRequest,
    );

    /// Apply the dispatched request
    ///
    /// This function should be idempotent.
    ///
    /// # Parameters
    /// * `request` - The dispatch request
    ///
    /// # Returns
    /// Result indicating success or failure
    fn put_message_position_info_wrapper(&self, request: &DispatchRequest);

    /// Range query consume queue units in RocksDB
    ///
    /// # Parameters
    /// * `topic` - The topic
    /// * `queue_id` - The queue ID
    /// * `start_index` - The start index
    /// * `num` - Number of items to retrieve
    ///
    /// # Returns
    /// List of Bytes for the topic-queueId in RocksDB
    async fn range_query(&self, topic: &CheetahString, queue_id: i32, start_index: i64, num: i32) -> Vec<Bytes>;

    /// Get a specific consume queue unit from RocksDB
    ///
    /// # Parameters
    /// * `topic` - The topic
    /// * `queue_id` - The queue ID
    /// * `start_index` - The index to retrieve
    ///
    /// # Returns
    /// Bytes for the topic-queueId in RocksDB
    async fn get(&self, topic: &CheetahString, queue_id: i32, start_index: i64) -> Bytes;

    /// Get the consume queue table
    ///
    /// # Returns
    /// The consume queue table mapping topics to queues
    fn get_consume_queue_table(&self) -> Arc<ConsumeQueueTable>;

    /// Assign queue offset to a message
    ///
    /// # Parameters
    /// * `msg` - The message
    ///
    /// # Returns
    /// Result indicating success or failure
    fn assign_queue_offset(&self, msg: &mut MessageExtBrokerInner);

    /// Increase queue offset
    ///
    /// # Parameters
    /// * `msg` - The message
    /// * `message_num` - Number of messages
    fn increase_queue_offset(&self, msg: &MessageExtBrokerInner, message_num: i16);

    /// Increase light message queue offset
    ///
    /// # Parameters
    /// * `queue_key` - The queue key
    /// * `message_num` - Number of messages
    fn increase_lmq_offset(&self, queue_key: &str, message_num: i16);

    /// Get light message queue offset
    ///
    /// # Parameters
    /// * `queue_key` - The queue key
    ///
    /// # Returns
    /// The queue offset
    fn get_lmq_queue_offset(&self, queue_key: &str) -> i64;

    /// Recover offset table by min physical offset
    ///
    /// # Parameters
    /// * `min_phy_offset` - The minimum physical offset
    fn recover_offset_table(&mut self, min_phy_offset: i64);

    /// Sets the topic queue table with the provided mapping.
    ///
    /// This method updates the internal mapping of topic names to their latest logical offsets. It
    /// is used for internal management and inspection of topic queues.
    ///
    /// # Arguments
    /// * `topic_queue_table` - A `HashMap` where the key is a `String` representing the topic name,
    ///   and the value is a 64-bit integer representing the latest logical offset for that topic.
    fn set_topic_queue_table(&mut self, topic_queue_table: HashMap<CheetahString, i64>);

    /// Removes the mapping for a specific topic and queue ID from the topic queue table.
    ///
    /// This method is used to remove the consume queue associated with a given topic and queue ID
    /// from the internal mapping. It is typically called when a consume queue is no longer needed
    /// or when cleaning up resources.
    ///
    /// # Arguments
    /// * `topic` - A string slice that holds the name of the topic.
    /// * `queue_id` - An integer representing the ID of the queue to be removed.
    fn remove_topic_queue_table(&mut self, topic: &CheetahString, queue_id: i32);

    /// Get topic queue table
    ///
    /// # Returns
    /// The topic queue table
    fn get_topic_queue_table(&self) -> HashMap<CheetahString, i64>;

    /// Get the max physical offset in consume queue for a topic-queue
    ///
    /// # Parameters
    /// * `topic` - The topic
    /// * `queue_id` - The queue ID
    ///
    /// # Returns
    /// The max physical offset, or None if not found
    fn get_max_phy_offset_in_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> Option<i64>;

    /// Get max offset of specific topic-queueId in topic queue table
    ///
    /// # Parameters
    /// * `topic` - The topic
    /// * `queue_id` - The queue ID
    ///
    /// # Returns
    /// The max offset, or None if not found
    fn get_max_offset(&self, topic: &CheetahString, queue_id: i32) -> Option<i64>;

    /// Get max physical offset across all consume queues
    ///
    /// # Returns
    /// The max physical offset
    fn get_max_phy_offset_in_consume_queue_global(&self) -> i64;

    /// Get min logical offset for a specific topic-queue
    ///
    /// # Parameters
    /// * `topic` - The topic
    /// * `queue_id` - The queue ID
    ///
    /// # Returns
    /// The min logical offset
    fn get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64;

    /// Get max logical offset for a specific topic-queue
    ///
    /// # Parameters
    /// * `topic` - The topic
    /// * `queue_id` - The queue ID
    ///
    /// # Returns
    /// The max logical offset
    fn get_max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64;

    /// Get the offset in queue by timestamp
    ///
    /// Get the message whose timestamp is the smallest, greater than or equal to the given time.
    /// When there are more than one message satisfying the condition, the returned message
    /// is decided by boundaryType.
    ///
    /// # Parameters
    /// * `topic` - The topic
    /// * `queue_id` - The queue ID
    /// * `timestamp` - The timestamp
    /// * `boundary_type` - Lower or Upper boundary type
    ///
    /// # Returns
    /// The offset (index)
    fn get_offset_in_queue_by_time(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64;

    /// Find or create a consume queue
    ///
    /// # Parameters
    /// * `topic` - The topic
    /// * `queue_id` - The queue ID
    ///
    /// # Returns
    /// The consume queue
    fn find_or_create_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> ArcConsumeQueue;

    /// Find the consume queue map for a topic
    ///
    /// # Parameters
    /// * `topic` - The topic
    ///
    /// # Returns
    /// The consume queue map for the topic, or None if not found
    fn find_consume_queue_map(&self, topic: &CheetahString) -> Option<HashMap<i32, ArcConsumeQueue>>;

    /// Get the total size of all consume queues
    ///
    /// # Returns
    /// The total size
    fn get_total_size(&self) -> i64;

    /// Get store time from commit log by consume queue unit
    ///
    /// # Parameters
    /// * `cq_unit` - The consume queue unit
    ///
    /// # Returns
    /// The store time
    fn get_store_time(&self, cq_unit: &CqUnit) -> i64;

    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}
