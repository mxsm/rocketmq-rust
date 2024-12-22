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
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::system_clock::SystemClock;

use crate::base::allocate_mapped_file_service::AllocateMappedFileService;
use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::base::get_message_result::GetMessageResult;
use crate::base::message_result::AppendMessageResult;
use crate::base::message_result::PutMessageResult;
use crate::base::query_message_result::QueryMessageResult;
use crate::base::select_result::SelectMappedBufferResult;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::base::store_stats_service::StoreStatsService;
use crate::base::transient_store_pool::TransientStorePool;
use crate::config::message_store_config::MessageStoreConfig;
use crate::filter::MessageFilter;
use crate::hook::put_message_hook::PutMessageHook;
use crate::hook::send_message_back_hook::SendMessageBackHook;
use crate::log_file::commit_log::CommitLog;
use crate::log_file::mapped_file::MappedFile;
use crate::queue::ConsumeQueueStoreTrait;
use crate::queue::ConsumeQueueTrait;
use crate::stats::broker_stats_manager::BrokerStatsManager;
use crate::store::running_flags::RunningFlags;
use crate::timer::timer_message_store::TimerMessageStore;

pub trait MessageStoreInner {
    /// Loads the message store.
    ///
    /// # Returns
    ///
    /// * `true` if the message store was loaded successfully.
    /// * `false` otherwise.
    fn load(&self) -> bool;

    /// Starts the message store.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the message store started successfully.
    /// * `Err` containing a boxed error if the message store failed to start.
    fn start(&self) -> Result<(), Box<dyn std::error::Error>>;

    /// Shuts down the message store.
    fn shutdown(&self);

    /// Destroys the message store, releasing all resources.
    fn destroy(&self);

    /// Asynchronously puts a single message into the message store.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be put into the store.
    ///
    /// # Returns
    ///
    /// A `Future` that resolves to a `PutMessageResult`.
    fn async_put_message(
        &self,
        msg: MessageExtBrokerInner,
    ) -> Pin<Box<dyn Future<Output = PutMessageResult>>> {
        Box::pin(async { self.put_message(msg) })
    }

    /// Asynchronously puts a batch of messages into the message store.
    ///
    /// # Arguments
    ///
    /// * `message_ext_batch` - The batch of messages to be put into the store.
    ///
    /// # Returns
    ///
    /// A `Future` that resolves to a `PutMessageResult`.
    fn async_put_messages(
        &self,
        message_ext_batch: MessageExtBatch,
    ) -> Pin<Box<dyn Future<Output = PutMessageResult>>> {
        Box::pin(async { self.put_messages(message_ext_batch) })
    }

    /// Puts a single message into the message store.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be put into the store.
    ///
    /// # Returns
    ///
    /// A `PutMessageResult` indicating the result of the operation.
    fn put_message(&self, msg: MessageExtBrokerInner) -> PutMessageResult;

    /// Puts a batch of messages into the message store.
    ///
    /// # Arguments
    ///
    /// * `message_ext_batch` - The batch of messages to be put into the store.
    ///
    /// # Returns
    ///
    /// A `PutMessageResult` indicating the result of the operation.
    fn put_messages(&self, message_ext_batch: MessageExtBatch) -> PutMessageResult;

    /// Retrieves messages from the store using a message filter.
    ///
    /// # Arguments
    ///
    /// * `group` - The consumer group.
    /// * `topic` - The topic from which to retrieve messages.
    /// * `queue_id` - The ID of the queue.
    /// * `offset` - The offset from which to start retrieving messages.
    /// * `max_msg_nums` - The maximum number of messages to retrieve.
    /// * `message_filter` - The filter to apply to the messages.
    ///
    /// # Returns
    ///
    /// A `GetMessageResult` containing the retrieved messages.
    fn get_message_filter(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        message_filter: Arc<dyn MessageFilter>,
    ) -> GetMessageResult;

    /// Asynchronously retrieves messages from the store.
    ///
    /// # Arguments
    ///
    /// * `group` - The consumer group.
    /// * `topic` - The topic from which to retrieve messages.
    /// * `queue_id` - The ID of the queue.
    /// * `offset` - The offset from which to start retrieving messages.
    /// * `max_msg_nums` - The maximum number of messages to retrieve.
    /// * `message_filter` - The filter to apply to the messages.
    ///
    /// # Returns
    ///
    /// A `Future` that resolves to a `GetMessageResult` containing the retrieved messages.
    fn get_message_async(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        message_filter: Arc<dyn MessageFilter>,
    ) -> Pin<Box<dyn Future<Output = GetMessageResult>>>;

    /// Retrieves messages from the store with a specified maximum total message size.
    ///
    /// # Arguments
    ///
    /// * `group` - The consumer group.
    /// * `topic` - The topic from which to retrieve messages.
    /// * `queue_id` - The ID of the queue.
    /// * `offset` - The offset from which to start retrieving messages.
    /// * `max_msg_nums` - The maximum number of messages to retrieve.
    /// * `max_total_msg_size` - The maximum total size of the messages to retrieve.
    /// * `message_filter` - The filter to apply to the messages.
    ///
    /// # Returns
    ///
    /// A `GetMessageResult` containing the retrieved messages.
    fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: Arc<dyn MessageFilter>,
    ) -> GetMessageResult;

    /// Asynchronously retrieves messages from the store with a specified maximum total message
    /// size.
    ///
    /// # Arguments
    ///
    /// * `group` - The consumer group.
    /// * `topic` - The topic from which to retrieve messages.
    /// * `queue_id` - The ID of the queue.
    /// * `offset` - The offset from which to start retrieving messages.
    /// * `max_msg_nums` - The maximum number of messages to retrieve.
    /// * `max_total_msg_size` - The maximum total size of the messages to retrieve.
    /// * `message_filter` - The filter to apply to the messages.
    ///
    /// # Returns
    ///
    /// A `Future` that resolves to a `GetMessageResult` containing the retrieved messages.
    fn get_message_async_with_size(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: Arc<dyn MessageFilter>,
    ) -> Pin<Box<dyn Future<Output = GetMessageResult>>>;

    /// Retrieves the maximum offset in the queue.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic of the queue.
    /// * `queue_id` - The ID of the queue.
    ///
    /// # Returns
    ///
    /// The maximum offset in the queue.
    fn get_max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64;

    /// Retrieves the maximum committed offset in the queue.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic of the queue.
    /// * `queue_id` - The ID of the queue.
    /// * `committed` - Whether to retrieve the committed offset.
    ///
    /// # Returns
    ///
    /// The maximum committed offset in the queue.
    fn get_max_offset_in_queue_committed(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        committed: bool,
    ) -> i64;

    /// Retrieves the minimum offset in the queue.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic of the queue.
    /// * `queue_id` - The ID of the queue.
    ///
    /// # Returns
    ///
    /// The minimum offset in the queue.
    fn get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64;

    /// Retrieves the timer message store.
    ///
    /// # Returns
    ///
    /// A reference to the `TimerMessageStore`.
    fn get_timer_message_store(&self) -> &TimerMessageStore;

    /// Sets the timer message store.
    ///
    /// # Arguments
    ///
    /// * `timer_message_store` - The `TimerMessageStore` to set.
    fn set_timer_message_store(&mut self, timer_message_store: TimerMessageStore);

    /// Retrieves the commit log offset in the queue.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic of the queue.
    /// * `queue_id` - The ID of the queue.
    /// * `consume_queue_offset` - The consume queue offset.
    ///
    /// # Returns
    ///
    /// The commit log offset in the queue.
    fn get_commit_log_offset_in_queue(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> i64;

    /// Retrieves the offset in the queue by timestamp.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic of the queue.
    /// * `queue_id` - The ID of the queue.
    /// * `timestamp` - The timestamp to retrieve the offset.
    ///
    /// # Returns
    ///
    /// The offset in the queue by timestamp.
    fn get_offset_in_queue_by_time(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
    ) -> i64;

    /// Retrieves the offset in the queue by timestamp with a boundary.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic of the queue.
    /// * `queue_id` - The ID of the queue.
    /// * `timestamp` - The timestamp to retrieve the offset.
    /// * `boundary_type` - The boundary type.
    ///
    /// # Returns
    ///
    /// The offset in the queue by timestamp with a boundary.
    fn get_offset_in_queue_by_time_with_boundary(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64;

    /// Looks up a message by commit log offset.
    ///
    /// # Arguments
    ///
    /// * `commit_log_offset` - The commit log offset.
    ///
    /// # Returns
    ///
    /// The message at the specified commit log offset.
    fn look_message_by_offset(&self, commit_log_offset: i64) -> MessageExt;

    /// Looks up a message by commit log offset with a specified size.
    ///
    /// # Arguments
    ///
    /// * `commit_log_offset` - The commit log offset.
    /// * `size` - The size of the message.
    ///
    /// # Returns
    ///
    /// The message at the specified commit log offset with the specified size.
    fn look_message_by_offset_with_size(&self, commit_log_offset: i64, size: i32) -> MessageExt;

    /// Selects one message by commit log offset.
    ///
    /// # Arguments
    ///
    /// * `commit_log_offset` - The commit log offset.
    ///
    /// # Returns
    ///
    /// A `SelectMappedBufferResult` containing the selected message.
    fn select_one_message_by_offset(&self, commit_log_offset: i64) -> SelectMappedBufferResult;

    /// Selects one message by commit log offset with a specified size.
    ///
    /// # Arguments
    ///
    /// * `commit_log_offset` - The commit log offset.
    /// * `msg_size` - The size of the message.
    ///
    /// # Returns
    ///
    /// A `SelectMappedBufferResult` containing the selected message with the specified size.
    fn select_one_message_by_offset_with_size(
        &self,
        commit_log_offset: i64,
        msg_size: i32,
    ) -> SelectMappedBufferResult;

    /// Retrieves the running data information.
    ///
    /// # Returns
    ///
    /// A `String` containing the running data information.
    fn get_running_data_info(&self) -> String;

    /// Retrieves the timing message count for a specific topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic for which to retrieve the timing message count.
    ///
    /// # Returns
    ///
    /// The timing message count as an `i64`.
    fn get_timing_message_count(&self, topic: &CheetahString) -> i64;

    /// Retrieves the runtime information.
    ///
    /// # Returns
    ///
    /// A `HashMap` containing the runtime information as key-value pairs.
    fn get_runtime_info(&self) -> HashMap<String, String>;

    // fn get_ha_runtime_info(&self) -> HARuntimeInfo;

    /// Retrieves the maximum physical offset.
    ///
    /// # Returns
    ///
    /// The maximum physical offset as an `i64`.
    fn get_max_phy_offset(&self) -> i64;

    /// Retrieves the minimum physical offset.
    ///
    /// # Returns
    ///
    /// The minimum physical offset as an `i64`.
    fn get_min_phy_offset(&self) -> i64;

    /// Retrieves the earliest message time for a specific topic and queue.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic for which to retrieve the earliest message time.
    /// * `queue_id` - The ID of the queue.
    ///
    /// # Returns
    ///
    /// The earliest message time as an `i64`.
    fn get_earliest_message_time(&self, topic: &CheetahString, queue_id: i32) -> i64;

    /// Retrieves the earliest message time globally.
    ///
    /// # Returns
    ///
    /// The earliest message time globally as an `i64`.
    fn get_earliest_message_time_global(&self) -> i64;

    /// Asynchronously retrieves the earliest message time for a specific topic and queue.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic for which to retrieve the earliest message time.
    /// * `queue_id` - The ID of the queue.
    ///
    /// # Returns
    ///
    /// The earliest message time as an `i64`.
    fn get_earliest_message_time_async(&self, topic: &CheetahString, queue_id: i32) -> i64;

    /// Retrieves the message store timestamp for a specific topic, queue, and consume queue offset.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic for which to retrieve the message store timestamp.
    /// * `queue_id` - The ID of the queue.
    /// * `consume_queue_offset` - The consume queue offset.
    ///
    /// # Returns
    ///
    /// The message store timestamp as an `i64`.
    fn get_message_store_timestamp(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> i64;

    /// Asynchronously retrieves the message store timestamp for a specific topic, queue, and
    /// consume queue offset.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic for which to retrieve the message store timestamp.
    /// * `queue_id` - The ID of the queue.
    /// * `consume_queue_offset` - The consume queue offset.
    ///
    /// # Returns
    ///
    /// The message store timestamp as an `i64`.
    fn get_message_store_timestamp_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> i64;

    /// Retrieves the total number of messages in the queue.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic of the queue.
    /// * `queue_id` - The ID of the queue.
    ///
    /// # Returns
    ///
    /// The total number of messages in the queue as an `i64`.
    fn get_message_total_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64;

    /// Retrieves the commit log data at a specific offset.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset in the commit log.
    ///
    /// # Returns
    ///
    /// A `SelectMappedBufferResult` containing the commit log data.
    fn get_commit_log_data(&self, offset: i64) -> SelectMappedBufferResult;

    /// Retrieves bulk commit log data starting at a specific offset.
    ///
    /// # Arguments
    ///
    /// * `offset` - The starting offset in the commit log.
    /// * `size` - The number of entries to retrieve.
    ///
    /// # Returns
    ///
    /// A `Vec` of `SelectMappedBufferResult` containing the commit log data.
    fn get_bulk_commit_log_data(&self, offset: i64, size: i32) -> Vec<SelectMappedBufferResult>;

    /// Appends data to the commit log.
    ///
    /// # Arguments
    ///
    /// * `start_offset` - The starting offset in the commit log.
    /// * `data` - The data to append.
    /// * `data_start` - The starting index in the data array.
    /// * `data_length` - The length of the data to append.
    ///
    /// # Returns
    ///
    /// `true` if the data was appended successfully, `false` otherwise.
    fn append_to_commit_log(
        &self,
        start_offset: i64,
        data: &[u8],
        data_start: usize,
        data_length: usize,
    ) -> bool;

    /// Executes the deletion of files manually.
    fn execute_delete_files_manually(&self);

    /// Queries messages in the store.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic of the messages.
    /// * `key` - The key of the messages.
    /// * `max_num` - The maximum number of messages to retrieve.
    /// * `begin` - The beginning timestamp for the query.
    /// * `end` - The ending timestamp for the query.
    ///
    /// # Returns
    ///
    /// A `QueryMessageResult` containing the queried messages.
    fn query_message(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> QueryMessageResult;

    /// Asynchronously queries messages in the store.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic of the messages.
    /// * `key` - The key of the messages.
    /// * `max_num` - The maximum number of messages to retrieve.
    /// * `begin` - The beginning timestamp for the query.
    /// * `end` - The ending timestamp for the query.
    ///
    /// # Returns
    ///
    /// A `QueryMessageResult` containing the queried messages.
    fn query_message_async(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> QueryMessageResult;

    /// Updates the HA master address.
    ///
    /// # Arguments
    ///
    /// * `new_addr` - The new HA master address.
    fn update_ha_master_address(&self, new_addr: &CheetahString);

    /// Updates the master address.
    ///
    /// # Arguments
    ///
    /// * `new_addr` - The new master address.
    fn update_master_address(&self, new_addr: &CheetahString);

    /// Retrieves the amount by which the slave has fallen behind.
    ///
    /// # Returns
    ///
    /// The amount by which the slave has fallen behind as an `i64`.
    fn slave_fall_behind_much(&self) -> i64;

    /// Retrieves the current time.
    ///
    /// # Returns
    ///
    /// The current time as an `i64`.
    fn now(&self) -> i64;

    /// Deletes the specified topics.
    ///
    /// # Arguments
    ///
    /// * `delete_topics` - A `HashSet` containing the topics to delete.
    ///
    /// # Returns
    ///
    /// The number of topics deleted as an `i32`.
    fn delete_topics(&self, delete_topics: &HashSet<String>) -> i32;

    /// Cleans up unused topics.
    ///
    /// # Arguments
    ///
    /// * `retain_topics` - A `HashSet` containing the topics to retain.
    ///
    /// # Returns
    ///
    /// The number of topics cleaned up as an `i32`.
    fn clean_unused_topic(&self, retain_topics: &HashSet<String>) -> i32;

    /// Cleans up expired consumer queues.
    fn clean_expired_consumer_queue(&self);

    //#[deprecated]
    /// Checks if the message is in disk by consume offset.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic of the message.
    /// * `queue_id` - The ID of the queue.
    /// * `consume_offset` - The consume offset of the message.
    ///
    /// # Returns
    ///
    /// `true` if the message is in disk, `false` otherwise.
    fn check_in_disk_by_consume_offset(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_offset: i64,
    ) -> bool;

    /// Checks if the message is in memory by consume offset.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic of the message.
    /// * `queue_id` - The ID of the queue.
    /// * `consume_offset` - The consume offset of the message.
    /// * `batch_size` - The batch size.
    ///
    /// # Returns
    ///
    /// `true` if the message is in memory, `false` otherwise.
    fn check_in_mem_by_consume_offset(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_offset: i64,
        batch_size: i32,
    ) -> bool;

    /// Checks if the message is in store by consume offset.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic of the message.
    /// * `queue_id` - The ID of the queue.
    /// * `consume_offset` - The consume offset of the message.
    ///
    /// # Returns
    ///
    /// `true` if the message is in store, `false` otherwise.
    fn check_in_store_by_consume_offset(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_offset: i64,
    ) -> bool;

    /// Retrieves the number of bytes behind in dispatch.
    ///
    /// # Returns
    ///
    /// The number of bytes behind in dispatch as an `i64`.
    fn dispatch_behind_bytes(&self) -> i64;

    /// Flushes the message store.
    ///
    /// # Returns
    ///
    /// The flushed offset as an `i64`.
    fn flush(&self) -> i64;

    /// Retrieves the flushed offset.
    ///
    /// # Returns
    ///
    /// The flushed offset as an `i64`.
    fn get_flushed_where(&self) -> i64;

    /// Resets the write offset.
    ///
    /// # Arguments
    ///
    /// * `phy_offset` - The physical offset to reset to.
    ///
    /// # Returns
    ///
    /// `true` if the write offset was reset successfully, `false` otherwise.
    fn reset_write_offset(&self, phy_offset: i64) -> bool;

    /// Retrieves the confirm offset.
    ///
    /// # Returns
    ///
    /// The confirm offset as an `i64`.
    fn get_confirm_offset(&self) -> i64;

    /// Sets the confirm offset.
    ///
    /// # Arguments
    ///
    /// * `phy_offset` - The physical offset to set as the confirm offset.
    fn set_confirm_offset(&self, phy_offset: i64);

    /// Checks if the OS page cache is busy.
    ///
    /// # Returns
    ///
    /// `true` if the OS page cache is busy, `false` otherwise.
    fn is_os_page_cache_busy(&self) -> bool;

    /// Retrieves the lock time in milliseconds.
    ///
    /// # Returns
    ///
    /// The lock time in milliseconds as an `i64`.
    fn lock_time_mills(&self) -> i64;

    /// Checks if the transient store pool is deficient.
    ///
    /// # Returns
    ///
    /// `true` if the transient store pool is deficient, `false` otherwise.
    fn is_transient_store_pool_deficient(&self) -> bool;

    /// Retrieves the list of commit log dispatchers.
    ///
    /// # Returns
    ///
    /// A `Vec` of `Arc<dyn CommitLogDispatcher>` containing the commit log dispatchers.
    fn get_dispatcher_list(&self) -> Vec<Arc<dyn CommitLogDispatcher>>;

    /// Adds a commit log dispatcher.
    ///
    /// # Arguments
    ///
    /// * `dispatcher` - The dispatcher to add.
    fn add_dispatcher(&self, dispatcher: Arc<dyn CommitLogDispatcher>);

    /// Retrieves the consume queue for a specific topic and queue ID.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic of the consume queue.
    /// * `queue_id` - The ID of the consume queue.
    ///
    /// # Returns
    ///
    /// A reference to the `dyn ConsumeQueueTrait` representing the consume queue.
    fn get_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> &dyn ConsumeQueueTrait;

    /// Finds the consume queue for a specific topic and queue ID.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic of the consume queue.
    /// * `queue_id` - The ID of the consume queue.
    ///
    /// # Returns
    ///
    /// A reference to the `dyn ConsumeQueueTrait` representing the consume queue.
    fn find_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> &dyn ConsumeQueueTrait;

    /// Retrieves the broker stats manager.
    ///
    /// # Returns
    ///
    /// A reference to the `BrokerStatsManager`.
    fn get_broker_stats_manager(&self) -> &BrokerStatsManager;

    /// Handles the event when a message is appended to the commit log.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message that was appended.
    /// * `result` - The result of the append operation.
    /// * `commit_log_file` - The commit log file.
    fn on_commit_log_append(
        &self,
        msg: &MessageExtBrokerInner,
        result: &AppendMessageResult,
        commit_log_file: Arc<Box<dyn MappedFile>>,
    );

    /// Handles the event when a commit log is dispatched.
    ///
    /// # Arguments
    ///
    /// * `dispatch_request` - The dispatch request.
    /// * `do_dispatch` - Whether to perform the dispatch.
    /// * `commit_log_file` - The commit log file.
    /// * `is_recover` - Whether the dispatch is part of a recovery process.
    /// * `is_file_end` - Whether the dispatch is at the end of the file.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the dispatch was successful, `Err` containing a string if the dispatch failed.
    fn on_commit_log_dispatch(
        &self,
        dispatch_request: &DispatchRequest,
        do_dispatch: bool,
        commit_log_file: Arc<dyn MappedFile>,
        is_recover: bool,
        is_file_end: bool,
    ) -> Result<(), String>;

    /// Finishes the commit log dispatch process.
    fn finish_commit_log_dispatch(&self);

    /// Retrieves the message store configuration.
    ///
    /// # Returns
    ///
    /// A reference to the `MessageStoreConfig`.
    fn get_message_store_config(&self) -> &MessageStoreConfig;

    /// Retrieves the store statistics service.
    ///
    /// # Returns
    ///
    /// A reference to the `StoreStatsService`.
    fn get_store_stats_service(&self) -> &StoreStatsService;

    /// Retrieves the store checkpoint.
    ///
    /// # Returns
    ///
    /// A reference to the `StoreCheckpoint`.
    fn get_store_checkpoint(&self) -> &StoreCheckpoint;

    /// Retrieves the system clock.
    ///
    /// # Returns
    ///
    /// A reference to the `SystemClock`.
    fn get_system_clock(&self) -> &SystemClock;

    /// Retrieves the commit log.
    ///
    /// # Returns
    ///
    /// A reference to the `CommitLog`.
    fn get_commit_log(&self) -> &CommitLog;

    /// Retrieves the running flags.
    ///
    /// # Returns
    ///
    /// A reference to the `RunningFlags`.
    fn get_running_flags(&self) -> &RunningFlags;

    /// Retrieves the transient store pool.
    ///
    /// # Returns
    ///
    /// A reference to the `TransientStorePool`.
    fn get_transient_store_pool(&self) -> &TransientStorePool;

    // fn get_ha_service(&self) -> &HAService;

    /// Retrieves the allocate mapped file service.
    ///
    /// # Returns
    ///
    /// A reference to the `AllocateMappedFileService`.
    fn get_allocate_mapped_file_service(&self) -> &AllocateMappedFileService;

    /// Truncates dirty logic files up to the specified physical offset.
    ///
    /// # Arguments
    ///
    /// * `phy_offset` - The physical offset up to which to truncate the files.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the files were truncated successfully, `Err` containing a string if the
    /// truncation failed.
    fn truncate_dirty_logic_files(&self, phy_offset: i64) -> Result<(), String>;

    /// Unlocks the specified mapped file.
    ///
    /// # Arguments
    ///
    /// * `unlock_mapped_file` - The mapped file to unlock.
    fn unlock_mapped_file(&self, unlock_mapped_file: Arc<dyn MappedFile>);

    //fn get_perf_counter(&self) -> &PerfCounter::Ticks;

    /// Retrieves the queue store.
    ///
    /// # Returns
    ///
    /// A reference to the `dyn ConsumeQueueStoreTrait`.
    fn get_queue_store(&self) -> &dyn ConsumeQueueStoreTrait;

    /// Checks if the disk flush is synchronous.
    ///
    /// # Returns
    ///
    /// `true` if the disk flush is synchronous, `false` otherwise.
    fn is_sync_disk_flush(&self) -> bool;

    /// Checks if the master is synchronous.
    ///
    /// # Returns
    ///
    /// `true` if the master is synchronous, `false` otherwise.
    fn is_sync_master(&self) -> bool;

    //fn assign_offset(&self, msg: &mut MessageExtBrokerInner) -> Result<(), RocksDBError>;
    /// Assigns an offset to the given message.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to which the offset will be assigned.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the offset was assigned successfully, `Err` containing a string if the
    /// assignment failed.
    fn assign_offset(&self, msg: &mut MessageExtBrokerInner) -> Result<(), String>;

    /// Increases the offset of the given message by the specified number of messages.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message whose offset will be increased.
    /// * `message_num` - The number of messages by which to increase the offset.
    fn increase_offset(&self, msg: &mut MessageExtBrokerInner, message_num: u16);

    /// Retrieves the master store in process.
    ///
    /// # Returns
    ///
    /// An `Option` containing a boxed `MessageStoreInner` if the master store is in process, `None`
    /// otherwise.
    fn get_master_store_in_process(&self) -> Option<Box<dyn MessageStoreInner>>;

    /// Sets the master store in process.
    ///
    /// # Arguments
    ///
    /// * `master_store_in_process` - A boxed `MessageStoreInner` representing the master store in
    ///   process.
    fn set_master_store_in_process(&mut self, master_store_in_process: Box<dyn MessageStoreInner>);

    //fn get_data(&self, offset: i64, size: usize, byte_buffer: &mut ByteBuffer) -> bool;

    /// Sets the number of alive replicas in the group.
    ///
    /// # Arguments
    ///
    /// * `alive_replica_nums` - The number of alive replicas to set.
    fn set_alive_replica_num_in_group(&mut self, alive_replica_nums: i32);

    /// Retrieves the number of alive replicas in the group.
    ///
    /// # Returns
    ///
    /// The number of alive replicas as an `i32`.
    fn get_alive_replica_num_in_group(&self) -> i32;

    /// Wakes up the HA client.
    fn wakeup_ha_client(&self);

    /// Retrieves the master flushed offset.
    ///
    /// # Returns
    ///
    /// The master flushed offset as an `i64`.
    fn get_master_flushed_offset(&self) -> i64;

    /// Retrieves the broker initial maximum offset.
    ///
    /// # Returns
    ///
    /// The broker initial maximum offset as an `i64`.
    fn get_broker_init_max_offset(&self) -> i64;

    /// Sets the master flushed offset.
    ///
    /// # Arguments
    ///
    /// * `master_flushed_offset` - The master flushed offset to set.
    fn set_master_flushed_offset(&mut self, master_flushed_offset: i64);

    /// Sets the broker initial maximum offset.
    ///
    /// # Arguments
    ///
    /// * `broker_init_max_offset` - The broker initial maximum offset to set.
    fn set_broker_init_max_offset(&mut self, broker_init_max_offset: i64);

    /// Calculates the delta checksum between two offsets.
    ///
    /// # Arguments
    ///
    /// * `from` - The starting offset.
    /// * `to` - The ending offset.
    ///
    /// # Returns
    ///
    /// A `Vec<u8>` containing the delta checksum.
    fn calc_delta_checksum(&self, from: u64, to: u64) -> Vec<u8>;

    //fn truncate_files(&self, offset_to_truncate: u64) -> Result<bool, RocksDBError>;
    /// Truncates files up to the specified offset.
    ///
    /// # Arguments
    ///
    /// * `offset_to_truncate` - The offset up to which to truncate the files.
    ///
    /// # Returns
    ///
    /// `Ok(true)` if the files were truncated successfully, `Err` containing a string if the
    /// truncation failed.
    fn truncate_files(&self, offset_to_truncate: u64) -> Result<bool, String>;

    /// Checks if the given offset is aligned.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset to check.
    ///
    /// # Returns
    ///
    /// `true` if the offset is aligned, `false` otherwise.
    fn is_offset_aligned(&self, offset: u64) -> bool;

    /// Retrieves the list of put message hooks.
    ///
    /// # Returns
    ///
    /// A `Vec` of boxed `PutMessageHook` traits.
    fn get_put_message_hook_list(&self) -> Vec<Box<dyn PutMessageHook>>;

    /// Sets the send message back hook.
    ///
    /// # Arguments
    ///
    /// * `hook` - A boxed `SendMessageBackHook` trait to set.
    fn set_send_message_back_hook(&self, hook: Box<dyn SendMessageBackHook>);

    /// Retrieves the send message back hook.
    ///
    /// # Returns
    ///
    /// An `Option` containing a boxed `SendMessageBackHook` trait if set, `None` otherwise.
    fn get_send_message_back_hook(&self) -> Option<Box<dyn SendMessageBackHook>>;

    /// Retrieves the offset of the last file.
    ///
    /// # Returns
    ///
    /// The offset of the last file as a `u64`.
    fn get_last_file_from_offset(&self) -> u64;

    /// Retrieves the last mapped file starting from the specified offset.
    ///
    /// # Arguments
    ///
    /// * `start_offset` - The starting offset.
    ///
    /// # Returns
    ///
    /// `true` if the last mapped file was retrieved successfully, `false` otherwise.
    fn get_last_mapped_file(&self, start_offset: u64) -> bool;

    /// Sets the physical offset.
    ///
    /// # Arguments
    ///
    /// * `phy_offset` - The physical offset to set.
    fn set_physical_offset(&self, phy_offset: u64);

    /// Checks if the mapped files are empty.
    ///
    /// # Returns
    ///
    /// `true` if the mapped files are empty, `false` otherwise.
    fn is_mapped_files_empty(&self) -> bool;

    /// Retrieves the state machine version.
    ///
    /// # Returns
    ///
    /// The state machine version as a `u64`.
    fn get_state_machine_version(&self) -> u64;

    /*    fn check_message_and_return_size(
        &self,
        byte_buffer: &ByteBuffer,
        check_crc: bool,
        check_dup_info: bool,
        read_body: bool,
    ) -> DispatchRequest;*/

    /// Retrieves the number of remaining transient store buffer numbers.
    ///
    /// # Returns
    ///
    /// The number of remaining transient store buffer numbers as an `i32`.
    fn remain_transient_store_buffer_numbs(&self) -> i32;

    /// Retrieves the amount of data remaining to be committed.
    ///
    /// # Returns
    ///
    /// The amount of data remaining to be committed as a `u64`.
    fn remain_how_many_data_to_commit(&self) -> u64;

    /// Retrieves the amount of data remaining to be flushed.
    ///
    /// # Returns
    ///
    /// The amount of data remaining to be flushed as a `u64`.
    fn remain_how_many_data_to_flush(&self) -> u64;

    /// Checks if the message store is shut down.
    ///
    /// # Returns
    ///
    /// `true` if the message store is shut down, `false` otherwise.
    fn is_shutdown(&self) -> bool;

    /// Estimates the number of messages in a specific topic and queue within a range.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic of the messages.
    /// * `queue_id` - The ID of the queue.
    /// * `from` - The starting offset.
    /// * `to` - The ending offset.
    /// * `filter` - The filter to apply to the messages.
    ///
    /// # Returns
    ///
    /// The estimated number of messages as a `u64`.
    fn estimate_message_count(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        from: u64,
        to: u64,
        filter: &dyn MessageFilter,
    ) -> u64;

    // fn get_metrics_view(&self) -> Vec<Pair<InstrumentSelector, ViewBuilder>>;

    /*    fn init_metrics(
        &self,
        meter: &Meter,
        attributes_builder_supplier: &dyn Fn() -> AttributesBuilder,
    );*/

    /// Recovers the topic queue table.
    fn recover_topic_queue_table(&self);

    /// Notifies that a message has arrived if necessary.
    ///
    /// # Arguments
    ///
    /// * `dispatch_request` - The dispatch request containing the message details.
    fn notify_message_arrive_if_necessary(&self, dispatch_request: &DispatchRequest);
}
