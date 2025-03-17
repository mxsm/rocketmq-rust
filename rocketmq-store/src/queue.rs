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
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::attribute::cq_type::CQType;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_rust::ArcMut;

use crate::base::dispatch_request::DispatchRequest;
use crate::base::swappable::Swappable;
use crate::consume_queue::consume_queue_ext::CqExtUnit;
use crate::filter::MessageFilter;
use crate::queue::consume_queue_ext::ConsumeQueueExt;
use crate::queue::queue_offset_operator::QueueOffsetOperator;

mod batch_consume_queue;
pub mod build_consume_queue;
mod consume_queue_ext;
pub mod local_file_consume_queue_store;
mod queue_offset_operator;
pub mod single_consume_queue;

pub type ArcConsumeQueue = ArcMut<Box<dyn ConsumeQueueTrait>>;
pub type ConsumeQueueTable =
    parking_lot::Mutex<HashMap<CheetahString, HashMap<i32, ArcConsumeQueue>>>;

/// Trait defining the lifecycle of a file-based queue, including operations for loading,
/// recovery, flushing, and destruction.
pub trait FileQueueLifeCycle: Swappable {
    /// Loads the queue from persistent storage.
    ///
    /// # Returns
    /// `true` if the queue was successfully loaded, `false` otherwise.
    fn load(&mut self) -> bool;

    /// Recovers the queue state from persistent storage.
    fn recover(&mut self);

    /// Performs a self-check to ensure the queue's integrity.
    fn check_self(&self);

    /// Flushes the queue's data to persistent storage.
    ///
    /// # Arguments
    /// * `flush_least_pages` - The minimum number of pages to flush.
    ///
    /// # Returns
    /// `true` if any data was flushed, `false` otherwise.
    fn flush(&self, flush_least_pages: i32) -> bool;

    /// Destroys the queue, cleaning up resources.
    fn destroy(&mut self);

    /// Truncates dirty logic files beyond a specified commit log position.
    ///
    /// # Arguments
    /// * `max_commit_log_pos` - The maximum commit log position to retain.
    fn truncate_dirty_logic_files(&mut self, max_commit_log_pos: i64);

    /// Deletes expired files based on a minimum commit log position.
    ///
    /// # Arguments
    /// * `min_commit_log_pos` - The minimum commit log position to consider.
    ///
    /// # Returns
    /// The number of files deleted.
    fn delete_expired_file(&self, min_commit_log_pos: i64) -> i32;

    /// Rolls over to the next file in the queue, based on the provided offset.
    ///
    /// # Arguments
    /// * `next_begin_offset` - The offset to start the next file at.
    ///
    /// # Returns
    /// The offset at which the next file begins.
    fn roll_next_file(&self, next_begin_offset: i64) -> i64;

    /// Checks if the first file in the queue is available for operations.
    ///
    /// # Returns
    /// `true` if the first file is available, `false` otherwise.
    fn is_first_file_available(&self) -> bool;

    /// Checks if the first file in the queue exists on the storage medium.
    ///
    /// # Returns
    /// `true` if the first file exists, `false` otherwise.
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

impl Default for CqUnit {
    fn default() -> Self {
        CqUnit {
            queue_offset: 0,
            size: 0,
            pos: 0,
            batch_num: 1,
            tags_code: 0,
            cq_ext_unit: None,
            native_buffer: vec![],
            compacted_offset: 0,
        }
    }
}

impl CqUnit {
    pub fn get_valid_tags_code_as_long(&self) -> Option<i64> {
        if !self.is_tags_code_valid() {
            return None;
        }
        Some(self.tags_code)
    }

    pub fn is_tags_code_valid(&self) -> bool {
        !ConsumeQueueExt::is_ext_addr(self.tags_code)
    }
}

/// Trait representing ConsumeQueueStoreInterface.
pub trait ConsumeQueueStoreTrait: Send + Sync {
    /// Initializes the consume queue store.
    ///
    /// This method should be called to initialize the consume queue store before performing any
    /// operations. It prepares the store for loading, recovery, and normal operation.
    fn start(&self);

    /// Loads the consume queue from persistent storage.
    ///
    /// This method attempts to load the consume queue from its persistent storage representation.
    /// It should be called during the initialization process, after `start`, to restore the state
    /// of the consume queue.
    ///
    /// # Returns
    /// `true` if the consume queue was successfully loaded, `false` otherwise.
    fn load(&mut self) -> bool;

    /// Attempts to load the consume queue after it has been destroyed.
    ///
    /// This method is similar to `load` but is specifically designed to be called after the consume
    /// queue store has been destroyed, to attempt a recovery of the queue's state.
    ///
    /// # Returns
    /// `true` if the consume queue was successfully loaded after destruction, `false` otherwise.
    fn load_after_destroy(&self) -> bool;

    /// Recovers the state of the consume queue from persistent storage.
    ///
    /// This method is used to recover the state of the consume queue, ensuring that it is
    /// consistent with the state stored in persistent storage. It is typically called after
    /// `load` during the initialization process.
    fn recover(&mut self);

    /// Recovers the state of the consume queue concurrently.
    ///
    /// This method performs the same function as `recover`, but it does so concurrently,
    /// potentially improving performance on systems with multiple processing cores.
    ///
    /// # Returns
    /// `true` if the recovery process was successful, `false` otherwise.
    fn recover_concurrently(&mut self) -> bool;

    /// Shuts down the consume queue store, preparing it for destruction or a restart.
    ///
    /// This method should be called to cleanly shut down the consume queue store, ensuring that all
    /// resources are properly released and data is flushed to persistent storage.
    ///
    /// # Returns
    /// `true` if the shutdown process was successful, `false` otherwise.
    fn shutdown(&self) -> bool;

    /// Destroys the consume queue store, cleaning up all resources.
    ///
    /// This method should be called when the consume queue store is no longer needed, ensuring that
    /// all resources are properly released. It is a critical part of the lifecycle management for
    /// consume queue stores.
    fn destroy(&self);

    /// Destroys a specific consume queue.
    ///
    /// This method is used to clean up a specific consume queue identified by the `consume_queue`
    /// parameter. It ensures that the resources allocated for the specified consume queue are
    /// properly released.
    ///
    /// # Arguments
    /// * `consume_queue` - A reference to the consume queue trait implementation that should be
    ///   destroyed.
    fn destroy_consume_queue(&self, consume_queue: &dyn ConsumeQueueTrait);

    /// Flushes the consume queue to persistent storage.
    ///
    /// This method ensures that any in-memory data for the specified consume queue is written to
    /// persistent storage. It is an essential part of ensuring data durability.
    ///
    /// # Arguments
    /// * `consume_queue` - A reference to the consume queue trait implementation that should be
    ///   flushed.
    /// * `flush_least_pages` - The minimum number of pages to flush. This parameter can be used to
    ///   control the granularity of the flush operation.
    ///
    /// # Returns
    /// `true` if the flush operation was successful, `false` otherwise.
    fn flush(&self, consume_queue: &dyn ConsumeQueueTrait, flush_least_pages: i32) -> bool;

    /// Cleans up expired data based on the minimum physical offset.
    ///
    /// This method is used to remove data that is no longer needed, based on the specified minimum
    /// physical offset. It helps in managing the storage space by removing obsolete data.
    ///
    /// # Arguments
    /// * `min_phy_offset` - The minimum physical offset. Data with a physical offset less than this
    ///   value will be considered expired and eligible for cleanup.
    fn clean_expired(&self, min_phy_offset: i64);

    /// Performs a self-check to ensure the consume queue's integrity.
    ///
    /// This method is used to perform internal checks on the consume queue to ensure its integrity.
    /// It can be used to detect and possibly correct any inconsistencies within the consume queue.
    fn check_self(&self);

    /// Deletes expired files based on a minimum commit log position.
    ///
    /// This method is used to delete files in the consume queue that are older than a specified
    /// commit log position. It is typically used for cleanup and to free up storage space.
    ///
    /// # Arguments
    /// * `consume_queue` - A reference to the consume queue trait implementation.
    /// * `min_commit_log_pos` - The minimum commit log position. Files with a commit log position
    ///   less than this value will be considered expired and eligible for deletion.
    ///
    /// # Returns
    /// The number of files deleted.
    fn delete_expired_file(
        &self,
        consume_queue: &dyn ConsumeQueueTrait,
        min_commit_log_pos: i64,
    ) -> i32;

    /// Checks if the first file in the consume queue is available for operations.
    ///
    /// This method determines whether the first file in the consume queue is available for read or
    /// write operations. It can be used to check the status of the queue before performing
    /// operations that depend on the availability of the first file.
    ///
    /// # Arguments
    /// * `consume_queue` - A reference to the consume queue trait implementation.
    ///
    /// # Returns
    /// `true` if the first file is available, `false` otherwise.
    fn is_first_file_available(&self, consume_queue: &dyn ConsumeQueueTrait) -> bool;

    /// Checks if the first file in the consume queue exists on the storage medium.
    ///
    /// This method is used to check the existence of the first file in the consume queue on the
    /// storage medium. It can be useful for verifying the integrity of the queue or for
    /// initialization procedures that require the presence of at least one file in the queue.
    ///
    /// # Arguments
    /// * `consume_queue` - A reference to the consume queue trait implementation.
    ///
    /// # Returns
    /// `true` if the first file exists, `false` otherwise.
    fn is_first_file_exist(&self, consume_queue: &dyn ConsumeQueueTrait) -> bool;

    /// Rolls over to the next file in the consume queue, based on the provided offset.
    ///
    /// This method is used to transition the consume queue to the next file, based on a specified
    /// offset. It is typically called when the current file reaches a certain size or when a new
    /// file is needed for organizational purposes.
    ///
    /// # Arguments
    /// * `consume_queue` - A reference to the consume queue trait implementation.
    /// * `offset` - The offset to start the next file at.
    ///
    /// # Returns
    /// The offset at which the next file begins.
    fn roll_next_file(&self, consume_queue: &dyn ConsumeQueueTrait, offset: i64) -> i64;

    /// Truncates dirty logic files beyond a specified offset.
    ///
    /// This method is used to truncate any dirty (uncommitted or partial) logic files in the
    /// consume queue that are beyond the specified offset. It is typically used during recovery
    /// or cleanup processes to ensure data consistency.
    ///
    /// # Arguments
    /// * `offset_to_truncate` - The offset beyond which all data is considered dirty and should be
    ///   truncated.
    fn truncate_dirty(&self, offset_to_truncate: i64);

    /// Stores message position information based on a dispatch request.
    ///
    /// This method processes a dispatch request to store message position information in the
    /// consume queue. It is a critical part of message storage, ensuring that messages can be
    /// accurately located and retrieved based on their position in the queue.
    ///
    /// # Arguments
    /// * `request` - The dispatch request containing information about the message to be stored.
    fn put_message_position_info_wrapper(&self, request: &DispatchRequest);

    /// Stores message position information with consume queue context.
    ///
    /// Similar to `put_message_position_info_wrapper`, but allows specifying a particular consume
    /// queue for the operation. This method provides more control over where the message position
    /// information is stored, especially useful in systems with multiple consume queues.
    ///
    /// # Arguments
    /// * `consume_queue` - A mutable reference to the consume queue where the message position
    ///   information should be stored.
    /// * `request` - The dispatch request containing information about the message to be stored.
    fn put_message_position_info_wrapper_with_cq(
        &self,
        consume_queue: &mut dyn ConsumeQueueTrait,
        request: &DispatchRequest,
    );

    /// Performs a range query on the consume queue.
    ///
    /// This method retrieves a range of messages from the consume queue based on the specified
    /// topic, queue ID, start index, and number of messages. It is useful for batch retrieval
    /// of messages for processing or inspection.
    ///
    /// # Arguments
    /// * `topic` - The topic of the messages to retrieve.
    /// * `queue_id` - The ID of the queue from which to retrieve messages.
    /// * `start_index` - The index of the first message to retrieve.
    /// * `num` - The number of messages to retrieve.
    ///
    /// # Returns
    /// An `Option` containing a vector of messages in the form of `bytes::Bytes` if any are found,
    /// or `None` if no messages match the criteria.
    fn range_query(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        start_index: i64,
        num: i32,
    ) -> Option<Vec<bytes::Bytes>>;

    /// Retrieves a signal message from the consume queue.
    ///
    /// This method is used to retrieve a specific signal message based on the topic, queue ID, and
    /// start index. Signal messages are special messages used for control or synchronization
    /// purposes within the messaging system.
    ///
    /// # Arguments
    /// * `topic` - The topic of the signal message to retrieve.
    /// * `queue_id` - The ID of the queue from which to retrieve the signal message.
    /// * `start_index` - The index of the signal message to retrieve.
    ///
    /// # Returns
    /// An `Option` containing the signal message in the form of `bytes::Bytes` if found, or `None`
    /// if no signal message matches the criteria.
    fn get_signal(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        start_index: i64,
    ) -> Option<bytes::Bytes>;

    /// Increase queue offset.
    /// `msg`: Message itself.
    /// `message_num`: Message number.
    fn increase_queue_offset(&self, msg: &MessageExtBrokerInner, message_num: i16);

    /// Assigns a queue offset to a message.
    ///
    /// This method assigns an offset to a message before it is stored in the queue. It is crucial
    /// for ensuring that each message can be accurately located and retrieved from the queue.
    ///
    /// # Arguments
    /// * `queue_offset_operator` - Operator for assigning queue offsets.
    /// * `msg` - The message to which the offset will be assigned.
    fn assign_queue_offset(&self, msg: &mut MessageExtBrokerInner);

    /// Increases the logical message queue (LMQ) offset for a given queue key.
    ///
    /// This method updates the offset for a specific logical message queue identified by the
    /// `queue_key`. It is used when a new message is added to the queue, ensuring the offset is
    /// incremented to reflect the new message count.
    ///
    /// # Arguments
    /// * `queue_key` - The key identifying the logical message queue.
    /// * `message_num` - The number of messages added, used for batch operations.
    fn increase_lmq_offset(&mut self, queue_key: &CheetahString, message_num: i16);

    /// Retrieves the current offset for a given logical message queue (LMQ).
    ///
    /// This method returns the current offset for a specific logical message queue identified by
    /// the `queue_key`. The offset represents the position of the last message in the queue.
    ///
    /// # Arguments
    /// * `queue_key` - The key identifying the logical message queue.
    ///
    /// # Returns
    /// The current offset of the logical message queue as a 64-bit integer.
    fn get_lmq_queue_offset(&self, queue_key: &CheetahString) -> i64;

    /// Recovers the offset table based on the minimum physical offset.
    ///
    /// This method is used to recover or adjust the offset table for consume queues based on the
    /// minimum physical offset. It ensures that the consume queue offsets are consistent with the
    /// physical storage state.
    ///
    /// # Arguments
    /// * `min_phy_offset` - The minimum physical offset to recover the offset table against.
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

    /// Retrieves a reference to the topic queue table.
    ///
    /// This method provides access to the internal structure that maps topic names to their
    /// latest logical offsets. It is primarily used for internal management and inspection
    /// of topic queues.
    ///
    /// # Returns
    /// A `HashMap` where the key is a `String` representing the topic name, and the value is
    /// a 64-bit integer representing the latest logical offset for that topic.
    fn get_topic_queue_table(&self) -> HashMap<CheetahString, i64>;

    /// Retrieves the maximum physical offset in the consume queue for a given topic and queue ID.
    ///
    /// This method is used to find the highest physical offset within a specific consume queue,
    /// which can be useful for determining the position of the latest message in that queue.
    ///
    /// # Arguments
    /// * `topic` - A string slice that holds the name of the topic.
    /// * `queue_id` - An integer representing the ID of the queue.
    ///
    /// # Returns
    /// The maximum physical offset as a 64-bit integer.
    fn get_max_phy_offset_in_consume_queue_id(&self, topic: &CheetahString, queue_id: i32) -> i64;

    /// Retrieves the maximum physical offset across all consume queues.
    ///
    /// This method calculates the highest physical offset present in any of the consume queues,
    /// which is useful for global operations that need to consider the latest state across all
    /// queues.
    ///
    /// # Returns
    /// The maximum physical offset as a 64-bit integer.
    fn get_max_phy_offset_in_consume_queue(&self) -> i64;

    /// Retrieves the maximum logical offset in the consume queue for a given topic and queue ID.
    ///
    /// This method is used to find the highest logical offset within a specific consume queue,
    /// which can be useful for determining how many messages are in the queue.
    ///
    /// # Arguments
    /// * `topic` - A string slice that holds the name of the topic.
    /// * `queue_id` - An integer representing the ID of the queue.
    ///
    /// # Returns
    /// An `Option` containing the maximum logical offset as a 64-bit integer, or `None` if the
    /// consume queue does not exist.
    fn get_max_offset(&self, topic: &CheetahString, queue_id: i32) -> Option<i64>;

    /// Finds or creates a consume queue for a given topic and queue ID.
    ///
    /// This method ensures that a consume queue exists for the specified topic and queue ID.
    /// If the consume queue does not exist, it is created. This is useful for dynamically managing
    /// consume queues based on demand.
    ///
    /// # Arguments
    /// * `topic` - A string slice that holds the name of the topic.
    /// * `queue_id` - An integer representing the ID of the queue.
    ///
    /// # Returns
    /// An atomic reference counted (`Arc`) wrapper around the consume queue.
    fn find_or_create_consume_queue(&self, topic: &CheetahString, queue_id: i32)
        -> ArcConsumeQueue;

    /// Finds the consume queue map for a given topic.
    ///
    /// This method retrieves the mapping of queue IDs to their respective consume queues for a
    /// specific topic. It is used for accessing all queues associated with a topic.
    ///
    /// # Arguments
    /// * `topic` - A string slice that holds the name of the topic.
    ///
    /// # Returns
    /// An `Option` containing a `HashMap` where the key is an integer representing the queue ID,
    /// and the value is an atomic reference counted (`Arc`) wrapper around the consume queue,
    /// or `None` if the topic does not exist.
    fn find_consume_queue_map(
        &self,
        topic: &CheetahString,
    ) -> Option<HashMap<i32, ArcConsumeQueue>>;

    /// Returns the total size of the consume queue.
    ///
    /// This method calculates the total size of the consume queue in bytes, which includes the size
    /// of all messages stored in the queue.
    ///
    /// # Returns
    /// The total size of the consume queue as a 64-bit integer.
    fn get_total_size(&self) -> i64;

    /// Retrieves the store time for a given `CqUnit`.
    ///
    /// This method returns the store time of the specified `CqUnit`. The store time is a 64-bit
    /// integer representing the time at which the `CqUnit` was stored in the consume queue.
    ///
    /// # Arguments
    /// * `cq_unit` - The `CqUnit` for which the store time is being retrieved.
    ///
    /// # Returns
    /// The store time of the `CqUnit` as a 64-bit integer.
    fn get_store_time(&self, cq_unit: CqUnit) -> i64;

    /// Returns the minimum offset in the queue for a given topic and queue ID.
    ///
    /// This method retrieves the smallest offset in the specified consume queue. It is useful for
    /// determining the starting point of unread messages in a queue.
    ///
    /// # Arguments
    /// * `topic` - A string slice that holds the name of the topic.
    /// * `queue_id` - An integer representing the ID of the queue.
    ///
    /// # Returns
    /// The minimum offset as a 64-bit integer.
    fn get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64;

    /// Returns the maximum offset in the queue for a given topic and queue ID.
    ///
    /// This method retrieves the largest offset in the specified consume queue. It can be used to
    /// find the end of the queue and is helpful in determining how many messages are in the queue.
    ///
    /// # Arguments
    /// * `topic` - A string slice that holds the name of the topic.
    /// * `queue_id` - An integer representing the ID of the queue.
    ///
    /// # Returns
    /// The maximum offset as a 64-bit integer.
    fn get_max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64;

    /// Retrieves a reference to the consume queue table.
    ///
    /// This method provides access to the internal structure that maps topic names to their
    /// corresponding queue IDs and consume queues. It is primarily used for internal management
    /// and inspection of consume queues.
    ///
    /// # Returns
    /// An atomic reference counted (`Arc`) wrapper around a `ConsumeQueueTable`, which is a
    /// mutex-protected hash map mapping topic names to another hash map of queue IDs and their
    /// associated consume queues.
    fn get_consume_queue_table(&self) -> Arc<ConsumeQueueTable>;
}

/// Trait representing ConsumeQueueInterface.
pub trait ConsumeQueueTrait: Send + Sync + FileQueueLifeCycle {
    /// Retrieves the topic of the consume queue.
    ///
    /// This method returns a reference to the string slice that represents the topic associated
    /// with the consume queue. The topic is a key identifier for messages and is used to
    /// categorize them within the messaging system.
    ///
    /// # Returns
    /// A string slice reference to the topic name.
    fn get_topic(&self) -> &CheetahString;

    /// Retrieves the queue ID of the consume queue.
    ///
    /// This method returns the queue ID as an integer. The queue ID is a unique identifier for a
    /// specific queue within a topic, allowing for parallel processing and organization of
    /// messages.
    ///
    /// # Returns
    /// An integer representing the queue ID.
    fn get_queue_id(&self) -> i32;

    /// Retrieves a `CqUnit` at a specified index within the consume queue.
    ///
    /// This method allows for accessing a specific `CqUnit` by its index in the consume queue. It
    /// is useful for direct access to a message's metadata stored in the queue without
    /// iterating over the entire queue. If the `CqUnit` at the specified index does not exist,
    /// `None` is returned, indicating the absence of a `CqUnit` at that index.
    ///
    /// # Arguments
    /// * `index` - The index of the `CqUnit` in the consume queue.
    ///
    /// # Returns
    /// An `Option<CqUnit>` which is `Some(CqUnit)` if a `CqUnit` exists at the specified index, or
    /// `None` if no `CqUnit` is found.
    fn get(&self, index: i64) -> Option<CqUnit>;

    /// Retrieves a `CqUnit` and its associated store time at a specified index.
    ///
    /// # Arguments
    /// * `index` - The index of the `CqUnit` in the consume queue.
    ///
    /// # Returns
    /// An `Option` containing a tuple of `CqUnit` and its store time as a 64-bit integer if found,
    /// or `None`.
    fn get_cq_unit_and_store_time(&self, index: i64) -> Option<(CqUnit, i64)>;

    /// Retrieves the earliest `CqUnit` and its associated store time in the consume queue.
    ///
    /// # Returns
    /// An `Option` containing a tuple of the earliest `CqUnit` and its store time as a 64-bit
    /// integer if available, or `None`.
    fn get_earliest_unit_and_store_time(&self) -> Option<(CqUnit, i64)>;

    /// Retrieves the earliest `CqUnit` in the consume queue.
    ///
    /// # Returns
    /// The earliest `CqUnit` in the consume queue.
    fn get_earliest_unit(&self) -> CqUnit;

    /// Retrieves the latest `CqUnit` in the consume queue.
    ///
    /// # Returns
    /// The latest `CqUnit` in the consume queue.
    fn get_latest_unit(&self) -> CqUnit;

    /// Retrieves the last offset in the consume queue.
    ///
    /// # Returns
    /// The last offset as a 64-bit integer.
    fn get_last_offset(&self) -> i64;

    /// Retrieves the minimum offset in the queue for a given topic and queue ID.
    ///
    /// # Returns
    /// The minimum offset in the queue as a 64-bit integer.
    fn get_min_offset_in_queue(&self) -> i64;

    /// Retrieves the maximum offset in the queue for a given topic and queue ID.
    ///
    /// # Returns
    /// The maximum offset in the queue as a 64-bit integer.
    fn get_max_offset_in_queue(&self) -> i64;

    /// Retrieves the total number of messages in the queue.
    ///
    /// # Returns
    /// The total number of messages in the queue as a 64-bit integer.
    fn get_message_total_in_queue(&self) -> i64;

    /// Retrieves the offset in the queue by a specific timestamp.
    ///
    /// # Arguments
    /// * `timestamp` - The timestamp to query by.
    ///
    /// # Returns
    /// The offset in the queue as a 64-bit integer.
    fn get_offset_in_queue_by_time(&self, timestamp: i64) -> i64;

    /// Retrieves the offset in the queue by a specific timestamp, considering boundary conditions.
    ///
    /// # Arguments
    /// * `timestamp` - The timestamp to query by.
    /// * `boundary_type` - The boundary condition to apply (e.g., exact match, closest match).
    ///
    /// # Returns
    /// The offset in the queue as a 64-bit integer.
    fn get_offset_in_queue_by_time_boundary(
        &self,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64;

    /// Returns the maximum physical offset in the consume queue.
    ///
    /// This method retrieves the largest physical offset in the consume queue, which can be used to
    /// determine the end of the queue and the position of the latest message.
    ///
    /// # Returns
    /// The maximum physical offset as a 64-bit integer.
    fn get_max_physic_offset(&self) -> i64;

    /// Returns the minimum logical offset in the consume queue.
    ///
    /// This method retrieves the smallest logical offset in the consume queue, useful for
    /// identifying the starting point of messages in the queue that are available for
    /// consumption.
    ///
    /// # Returns
    /// The minimum logical offset as a 64-bit integer.
    fn get_min_logic_offset(&self) -> i64;

    /// Retrieves the type of the consume queue.
    ///
    /// This method returns the type of the consume queue, indicating whether it is a standard queue
    /// or a long-term queue based on the `CQType` enum.
    ///
    /// # Returns
    /// The consume queue type as `CQType`.
    fn get_cq_type(&self) -> CQType;

    /// Returns the total size of the consume queue.
    ///
    /// This method calculates the total size of the consume queue in bytes, which includes the size
    /// of all messages stored in the queue.
    ///
    /// # Returns
    /// The total size of the consume queue as a 64-bit integer.
    fn get_total_size(&self) -> i64;

    /// Returns the size of a single unit in the consume queue.
    ///
    /// This method retrieves the size of a single unit within the consume queue, which is
    /// consistent across all units in the queue.
    ///
    /// # Returns
    /// The size of a single unit in the consume queue as a 32-bit integer.
    fn get_unit_size(&self) -> i32;

    /// Corrects the minimum offset in the consume queue.
    ///
    /// This method is used to correct the minimum offset in the consume queue based on the minimum
    /// commit log offset. It ensures that the consume queue does not reference any messages that
    /// have been physically deleted from the commit log.
    ///
    /// # Arguments
    /// * `min_commit_log_offset` - The minimum commit log offset to correct the consume queue
    ///   offset against.
    fn correct_min_offset(&self, min_commit_log_offset: i64);

    /// Applies the dispatched request to the consume queue.
    ///
    /// This method is responsible for processing a dispatch request and applying it to the consume
    /// queue. It is an essential part of message storage, ensuring that messages are correctly
    /// positioned within the queue for retrieval.
    ///
    /// # Arguments
    /// * `request` - The dispatch request containing information about the message to be stored.
    fn put_message_position_info_wrapper(&mut self, request: &DispatchRequest);

    /// Increases the queue offset based on the message and its number.
    ///
    /// This method updates the queue offset after a new message is added. It ensures that the queue
    /// maintains a correct and up-to-date index for message retrieval.
    ///
    /// # Arguments
    /// * `queue_offset_assigner` - Operator for assigning queue offsets.
    /// * `msg` - The message that has been added to the queue.
    /// * `message_num` - The number of messages added, used for batch operations.
    fn increase_queue_offset(
        &self,
        queue_offset_assigner: &QueueOffsetOperator,
        msg: &MessageExtBrokerInner,
        message_num: i16,
    );

    /// Assigns a queue offset to a message.
    ///
    /// This method assigns an offset to a message before it is stored in the queue. It is crucial
    /// for ensuring that each message can be accurately located and retrieved from the queue.
    ///
    /// # Arguments
    /// * `queue_offset_operator` - Operator for assigning queue offsets.
    /// * `msg` - The message to which the offset will be assigned.
    fn assign_queue_offset(
        &self,
        queue_offset_operator: &QueueOffsetOperator,
        msg: &mut MessageExtBrokerInner,
    );

    /// Estimates the number of messages between two offsets using a filter.
    ///
    /// This method provides an estimate of the number of messages that match a given filter between
    /// two offsets. It is useful for understanding the volume of messages that meet specific
    /// criteria.
    ///
    /// # Arguments
    /// * `from` - The starting offset for the estimation.
    /// * `to` - The ending offset for the estimation.
    /// * `filter` - The filter used to match messages.
    ///
    /// # Returns
    /// The estimated number of messages that match the filter between the specified offsets.
    fn estimate_message_count(&self, from: i64, to: i64, filter: &dyn MessageFilter) -> i64;

    /// Iterates over messages from a specified start index.
    ///
    /// This method returns an iterator that allows for traversing messages starting from a given
    /// index. It is useful for sequential message processing.
    ///
    /// # Arguments
    /// * `start_index` - The index from which to start iterating.
    ///
    /// # Returns
    /// An optional box containing an iterator over `CqUnit` items, or `None` if iteration cannot
    /// start.
    fn iterate_from(&self, start_index: i64) -> Option<Box<dyn Iterator<Item = CqUnit> + Send>>;

    /// Iterates over a specified number of messages from a start index.
    ///
    /// Similar to `iterate_from`, but allows specifying the number of messages to iterate over.
    /// This method is useful for processing a fixed number of messages starting from a specific
    /// index.
    ///
    /// # Arguments
    /// * `start_index` - The index from which to start iterating.
    /// * `count` - The number of messages to iterate over.
    ///
    /// # Returns
    /// An optional box containing an iterator over `CqUnit` items, or `None` if iteration cannot
    /// start.
    fn iterate_from_inner(
        &self,
        start_index: i64,
        count: i32,
    ) -> Option<Box<dyn Iterator<Item = CqUnit> + Send>>;
}
