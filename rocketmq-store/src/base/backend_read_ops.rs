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

//! Backend logical reads. Implementations own physical storage and queue-index knowledge.

use crate::base::get_message_result::GetMessageResult;
use crate::base::query_message_request::QueryMessageRequest;
use crate::base::query_message_result::QueryMessageResult;
use crate::base::select_result::SelectMappedBufferResult;
use crate::capability::ConsumeQueueStatistics;
use crate::filter::ArcMessageFilter;
use crate::store_error::StoreError;
use cheetah_string::CheetahString;
use rocketmq_model::common::boundary_type::BoundaryType;
use rocketmq_model::common::message::message_ext::MessageExt;

/// Logical reads implemented without exposing a concrete storage owner to capability consumers.
#[trait_variant::make(Send)]
pub trait BackendReadOps: Send + Sync + 'static {
    fn is_message_in_cold_area(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        queue_offset: i64,
    ) -> bool;
    fn pickup_store_timestamp(&self, offset: i64, size: i32) -> i64;
    /// Reads the LMQ index only; zero leaves ordinary-queue fallback to the capability.
    fn lmq_queue_offset(&self, topic: &CheetahString) -> i64;
    fn contains_lmq(&self, topic: &CheetahString) -> bool;
    fn get_lmq_topic_names(&self) -> Vec<CheetahString>;
    fn consume_queue_statistics(&self) -> ConsumeQueueStatistics;

    /// Query messages belonging to a topic at a queue starting from given offset.
    ///
    /// # Parameters
    /// * `group` - Consumer group that launches this query
    /// * `topic` - Topic to query
    /// * `queue_id` - Queue ID to query
    /// * `offset` - Logical offset to start from
    /// * `max_msg_nums` - Maximum count of messages to query
    /// * `message_filter` - Message filter used to screen desired messages
    ///
    /// # Returns
    /// Matched messages
    async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Option<GetMessageResult>;

    /// Get message with size constraint
    async fn get_message_with_size_limit(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Option<GetMessageResult>;

    /// Get maximum offset of the topic queue.
    fn get_max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64;

    /// Get maximum offset of the topic queue.
    fn get_max_offset_in_queue_committed(&self, topic: &CheetahString, queue_id: i32, committed: bool) -> i64;

    /// Get the minimum offset of the topic queue.
    fn get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64;

    /// Get the offset of the message in the commit log (physical offset).
    fn get_commit_log_offset_in_queue(&self, topic: &CheetahString, queue_id: i32, consume_queue_offset: i64) -> i64;

    /// Look up the physical offset of the message by timestamp.
    fn get_offset_in_queue_by_time(&self, topic: &CheetahString, queue_id: i32, timestamp: i64) -> i64;

    /// Look up the physical offset of the message by timestamp with boundary type.
    fn get_offset_in_queue_by_time_with_boundary(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64;

    /// Look up the logical offset by timestamp, allowing asynchronous tiered-store fallback.
    async fn get_offset_in_queue_by_time_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
    ) -> Result<i64, StoreError>;

    /// Look up the logical offset by timestamp with boundary type, allowing asynchronous
    /// tiered-store fallback.
    async fn get_offset_in_queue_by_time_with_boundary_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> Result<i64, StoreError>;

    /// Look up the message by given commit log offset.
    fn look_message_by_offset(&self, commit_log_offset: i64) -> Option<MessageExt>;

    /// Look up the message by given commit log offset and size.
    fn look_message_by_offset_with_size(&self, commit_log_offset: i64, size: i32) -> Option<MessageExt>;

    /// Get one message from the specified commit log offset.
    fn select_one_message_by_offset(&self, commit_log_offset: i64) -> Option<SelectMappedBufferResult>;

    /// Get one message from the specified commit log offset and message size.
    fn select_one_message_by_offset_with_size(
        &self,
        commit_log_offset: i64,
        msg_size: i32,
    ) -> Option<SelectMappedBufferResult>;

    /// Get timing message count for a topic.
    fn get_timing_message_count(&self, topic: &CheetahString) -> i64;

    /// Get the store time of the earliest message in the given queue.
    fn get_earliest_message_time(&self, topic: &CheetahString, queue_id: i32) -> i64;

    /// Get the store time of the earliest message in this store.
    fn get_earliest_message_time_store(&self) -> i64;

    /// Get the store time of the message specified.
    fn get_message_store_timestamp(&self, topic: &CheetahString, queue_id: i32, consume_queue_offset: i64) -> i64;

    /// Asynchronous get the store time of the message specified.
    async fn get_message_store_timestamp_async(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> Result<i64, StoreError>;

    /// Get the total number of the messages in the specified queue.
    fn get_message_total_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64;

    /// Query messages by given key.
    async fn query_message(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Option<QueryMessageResult>;

    /// Query messages while preserving the optional index type and continuation cursor.
    async fn query_message_with_options(&self, request: &QueryMessageRequest) -> Option<QueryMessageResult>;

    /// Check if the given message is in the page cache.
    fn check_in_mem_by_consume_offset(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_offset: i64,
        batch_size: i32,
    ) -> bool;

    /// Check if the given message is in store.
    fn check_in_store_by_consume_offset(&self, topic: &CheetahString, queue_id: i32, consume_offset: i64) -> bool;
}
