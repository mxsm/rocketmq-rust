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
use std::error::Error;
use std::sync::Arc;

use parking_lot::RwLock;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_single::MessageExt;
use rocketmq_common::common::message::message_single::MessageExtBrokerInner;
use rocketmq_common::TimeUtils::get_current_millis;

use crate::base::dispatch_request::DispatchRequest;
use crate::base::get_message_result::GetMessageResult;
use crate::base::message_result::PutMessageResult;
use crate::base::query_message_result::QueryMessageResult;
use crate::base::select_result::SelectMappedBufferResult;
use crate::filter::MessageFilter;
use crate::hook::put_message_hook::BoxedPutMessageHook;
use crate::queue::ArcConsumeQueue;
use crate::stats::broker_stats_manager::BrokerStatsManager;
use crate::store::running_flags::RunningFlags;

pub(crate) mod cold_data_check_service;
pub mod commit_log;
pub mod flush_manager_impl;
pub mod mapped_file;

pub const MAX_PULL_MSG_SIZE: i32 = 128 * 1024 * 1024;

#[trait_variant::make(MessageStore: Send)]
pub trait RocketMQMessageStore: Clone + 'static {
    /// Load previously stored messages.
    ///
    /// # Returns
    ///
    /// `true` if the messages were successfully loaded; `false` otherwise.
    async fn load(&mut self) -> bool;

    /// Launch the message store.
    ///
    /// # Errors
    ///
    /// Returns an `Err` if there is any error during the start.
    fn start(&mut self) -> Result<(), Box<dyn Error>>;

    /// Shutdown the message store.
    fn shutdown(&mut self);

    /// Set the confirm offset.
    ///
    /// # Arguments
    ///
    /// * `phy_offset` - The physical offset to set as the confirm offset.
    fn set_confirm_offset(&mut self, phy_offset: i64);

    /// Get the maximum physical offset.
    ///
    /// # Returns
    ///
    /// The maximum physical offset.
    fn get_max_phy_offset(&self) -> i64;

    /// Set the broker initial maximum offset.
    ///
    /// # Arguments
    ///
    /// * `broker_init_max_offset` - The initial maximum offset of the broker.
    fn set_broker_init_max_offset(&mut self, broker_init_max_offset: i64);

    /// Get the current time in milliseconds.
    ///
    /// # Returns
    ///
    /// The current time in milliseconds.
    #[inline]
    fn now(&self) -> u64 {
        get_current_millis()
    }

    /// Get the version of the state machine.
    ///
    /// # Returns
    ///
    /// The version of the state machine.
    fn get_state_machine_version(&self) -> i64;

    /// Store a message asynchronously.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to store.
    ///
    /// # Returns
    ///
    /// A `PutMessageResult` indicating the result of the operation.
    async fn put_message(&mut self, msg: MessageExtBrokerInner) -> PutMessageResult;

    /// Store a batch of messages asynchronously.
    ///
    /// # Arguments
    ///
    /// * `msg_batch` - The batch of messages to store.
    ///
    /// # Returns
    ///
    /// A `PutMessageResult` indicating the result of the operation.
    async fn put_messages(&mut self, msg_batch: MessageExtBatch) -> PutMessageResult;

    /// Truncate files up to a specified offset.
    ///
    /// # Arguments
    ///
    /// * `offset_to_truncate` - The offset up to which files should be truncated.
    ///
    /// # Returns
    ///
    /// `true` if the operation was successful; `false` otherwise.
    fn truncate_files(&mut self, offset_to_truncate: i64) -> bool;

    /// Check if the OS page cache is busy.
    ///
    /// # Returns
    ///
    /// `false` if the page cache is not busy; `true` otherwise.
    fn is_os_page_cache_busy(&self) -> bool {
        false
    }

    /// Get the running flags of the message store.
    ///
    /// # Returns
    ///
    /// A reference to the running flags.
    fn get_running_flags(&self) -> &RunningFlags;

    /// Check if the message store is shutdown.
    ///
    /// # Returns
    ///
    /// `true` if the message store is shutdown; `false` otherwise.
    fn is_shutdown(&self) -> bool;

    /// Get the list of put message hooks.
    ///
    /// # Returns
    ///
    /// An `Arc` containing a read-write lock around a vector of boxed put message hooks.
    fn get_put_message_hook_list(&self) -> Arc<RwLock<Vec<BoxedPutMessageHook>>>;

    /// Set a put message hook.
    ///
    /// # Arguments
    ///
    /// * `put_message_hook` - The hook to set.
    fn set_put_message_hook(&self, put_message_hook: BoxedPutMessageHook);

    /// Get the broker statistics manager.
    ///
    /// # Returns
    ///
    /// An `Option` containing an `Arc` to the broker statistics manager, if it exists.
    fn get_broker_stats_manager(&self) -> Option<Arc<BrokerStatsManager>>;

    /// Dispatch bytes that are behind.
    fn dispatch_behind_bytes(&self);

    /// Get the minimum offset in the queue.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic name.
    /// * `queue_id` - The queue identifier.
    ///
    /// # Returns
    ///
    /// The minimum offset in the queue.
    fn get_min_offset_in_queue(&self, topic: &str, queue_id: i32) -> i64;

    /// Get the maximum offset in the queue.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic name.
    /// * `queue_id` - The queue identifier.
    ///
    /// # Returns
    ///
    /// The maximum offset in the queue.
    fn get_max_offset_in_queue(&self, topic: &str, queue_id: i32) -> i64;

    /// Get the maximum committed offset in the queue.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic name.
    /// * `queue_id` - The queue identifier.
    /// * `committed` - Whether to return the committed offset.
    ///
    /// # Returns
    ///
    /// The maximum committed offset in the queue.
    fn get_max_offset_in_queue_committed(&self, topic: &str, queue_id: i32, committed: bool)
        -> i64;

    /// Get a message asynchronously.
    ///
    /// # Arguments
    ///
    /// * `group` - The group name.
    /// * `topic` - The topic name.
    /// * `queue_id` - The queue identifier.
    /// * `offset` - The offset of the message.
    /// * `max_msg_nums` - The maximum number of messages.
    /// * `max_total_msg_size` - The maximum total message size.
    /// * `message_filter` - An optional message filter.
    ///
    /// # Returns
    ///
    /// An `Option` containing the result of the message retrieval.
    async fn get_message(
        &self,
        group: &str,
        topic: &str,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: Option<&dyn MessageFilter>,
    ) -> Option<GetMessageResult>;

    /// Check if messages are in memory by consume offset.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic name.
    /// * `queue_id` - The queue identifier.
    /// * `consume_offset` - The consume offset.
    /// * `batch_size` - The batch size.
    ///
    /// # Returns
    ///
    /// `true` if messages are in memory; `false` otherwise.
    fn check_in_mem_by_consume_offset(
        &self,
        topic: &str,
        queue_id: i32,
        consume_offset: i64,
        batch_size: i32,
    ) -> bool;

    /// Notify that a message has arrived if necessary.
    ///
    /// # Arguments
    ///
    /// * `dispatch_request` - The dispatch request.
    fn notify_message_arrive_if_necessary(&self, dispatch_request: &mut DispatchRequest);

    /// Find the consume queue for a topic and queue identifier.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic name.
    /// * `queue_id` - The queue identifier.
    ///
    /// # Returns
    ///
    /// An `Option` containing an `Arc` to the consume queue, if it exists.
    fn find_consume_queue(&self, topic: &str, queue_id: i32) -> Option<ArcConsumeQueue>;

    /// Delete topics from the message store.
    ///
    /// # Arguments
    ///
    /// * `delete_topics` - A vector of topic names to delete.
    ///
    /// # Returns
    ///
    /// The number of topics deleted.
    fn delete_topics(&mut self, delete_topics: Vec<&str>) -> i32;

    /// Query messages asynchronously.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic name.
    /// * `key` - The message key.
    /// * `max_num` - The maximum number of messages.
    /// * `begin_timestamp` - The begin timestamp.
    /// * `end_timestamp` - The end timestamp.
    ///
    /// # Returns
    ///
    /// An `Option` containing the result of the message query.
    async fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Option<QueryMessageResult>;

    /// Select one message by offset asynchronously.
    ///
    /// # Arguments
    ///
    /// * `commit_log_offset` - The commit log offset.
    ///
    /// # Returns
    ///
    /// An `Option` containing the result of the message selection.
    async fn select_one_message_by_offset(
        &self,
        commit_log_offset: i64,
    ) -> Option<SelectMappedBufferResult>;

    /// Select one message by offset and size asynchronously.
    ///
    /// # Arguments
    ///
    /// * `commit_log_offset` - The commit log offset.
    /// * `size` - The size of the message.
    ///
    /// # Returns
    ///
    /// An `Option` containing the result of the message selection.
    async fn select_one_message_by_offset_with_size(
        &self,
        commit_log_offset: i64,
        size: i32,
    ) -> Option<SelectMappedBufferResult>;

    /// Look up a message by offset.
    ///
    /// # Arguments
    ///
    /// * `commit_log_offset` - The commit log offset.
    ///
    /// # Returns
    ///
    /// An `Option` containing the message, if it exists.
    fn look_message_by_offset(&self, commit_log_offset: i64) -> Option<MessageExt>;

    /// Look up a message by offset and size.
    ///
    /// # Arguments
    ///
    /// * `commit_log_offset` - The commit log offset.
    /// * `size` - The size of the message.
    ///
    /// # Returns
    ///
    /// An `Option` containing the message, if it exists.
    fn look_message_by_offset_with_size(
        &self,
        commit_log_offset: i64,
        size: i32,
    ) -> Option<MessageExt>;

    /// Gets the store time of the specified message.
    ///
    /// # Arguments
    ///
    /// * `topic` - The message topic.
    /// * `queue_id` - The queue ID.
    /// * `consume_queue_offset` - The consume queue offset.
    ///
    /// # Returns
    ///
    /// The store timestamp of the message.
    fn get_message_store_timestamp(
        &self,
        topic: &str,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> i64;
}
