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
pub mod commit_log;
pub mod mapped_file;

pub trait MessageStore {
    /// Load previously stored messages.
    ///
    /// Returns `true` if success; `false` otherwise.
    fn load(&mut self) -> bool;

    /// Launch this message store.
    ///
    /// # Throws
    ///
    /// Throws an `Exception` if there is any error.
    // fn start(&self) -> Result<(), Box<dyn std::error::Error>>;

    /// Shutdown this message store.
    //fn shutdown(&self);

    /// Destroy this message store. Generally, all persistent files should be removed after
    /// invocation.
    // fn destroy(&self);

    /// Store a message into the store in an async manner. The processor can process the next
    /// request rather than wait for the result. When the result is completed, notify the client
    /// in an async manner.
    ///
    /// # Arguments
    ///
    /// * `msg` - MessageInstance to store.
    ///
    /// # Returns
    ///
    /// A `Future` for the result of the store operation.
    /* fn async_put_message(
        &self,
        msg: MessageExtBrokerInner,
    ) -> impl Future<Output = PutMessageResult>;*/

    /// Store a batch of messages in an async manner.
    ///
    /// # Arguments
    ///
    /// * `message_ext_batch` - The message batch.
    ///
    /// # Returns
    ///
    /// A `Future` for the result of the store operation.
    /*fn async_put_messages(
        &self,
        message_ext_batch: MessageExtBatch,
    ) -> impl Future<Output = PutMessageResult>;*/

    /// Store a message into the store.
    ///
    /// # Arguments
    ///
    /// * `msg` - Message instance to store.
    ///
    /// # Returns
    ///
    /// Result of the store operation.
    /* fn put_message(&self, msg: MessageExtBrokerInner) -> PutMessageResult; */

    /// Store a batch of messages.
    ///
    /// # Arguments
    ///
    /// * `message_ext_batch` - Message batch.
    ///
    /// # Returns
    ///
    /// Result of storing batch messages.
    // fn put_messages(&self, message_ext_batch: MessageExtBatch) -> PutMessageResult;

    /// Query at most `max_msg_nums` messages belonging to `topic` at `queue_id` starting
    /// from given `offset`. Resulting messages will further be screened using provided message
    /// filter.
    ///
    /// # Arguments
    ///
    /// * `group` - Consumer group that launches this query.
    /// * `topic` - Topic to query.
    /// * `queue_id` - Queue ID to query.
    /// * `offset` - Logical offset to start from.
    /// * `max_msg_nums` - Maximum count of messages to query.
    /// * `message_filter` - Message filter used to screen desired messages.
    ///
    /// # Returns
    ///
    /// Matched messages.
    /*fn get_message(
        &self,
        group: &str,
        topic: &str,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        message_filter: impl MessageFilter,
    ) -> GetMessageResult;*/

    /// Asynchronous get message.
    ///
    /// # See
    ///
    /// [`get_message`](#method.get_message)
    ///
    /// # Arguments
    ///
    /// * `group` - Consumer group that launches this query.
    /// * `topic` - Topic to query.
    /// * `queue_id` - Queue ID to query.
    /// * `offset` - Logical offset to start from.
    /// * `max_msg_nums` - Maximum count of messages to query.
    /// * `message_filter` - Message filter used to screen desired messages.
    ///
    /// # Returns
    ///
    /// Matched messages.
    /* fn get_message_async(
        &self,
        group: &str,
        topic: &str,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        message_filter: impl MessageFilter,
    ) -> impl Future<Output = GetMessageResult>;*/

    /// Query at most `max_msg_nums` messages belonging to `topic` at `queue_id` starting
    /// from given `offset`. Resulting messages will further be screened using provided message
    /// filter.
    ///
    /// # Arguments
    ///
    /// * `group` - Consumer group that launches this query.
    /// * `topic` - Topic to query.
    /// * `queue_id` - Queue ID to query.
    /// * `offset` - Logical offset to start from.
    /// * `max_msg_nums` - Maximum count of messages to query.
    /// * `max_total_msg_size` - Maximum total msg size of the messages.
    /// * `message_filter` - Message filter used to screen desired messages.
    ///
    /// # Returns
    ///
    /// Matched messages.
    /*fn get_message_with_size(
        &self,
        group: &str,
        topic: &str,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: impl MessageFilter,
    ) -> GetMessageResult;*/

    /// Asynchronous get message.
    ///
    /// # See
    ///
    /// [`get_message_with_size`](#method.get_message_with_size)
    ///
    /// # Arguments
    ///
    /// * `group` - Consumer group that launches this query.
    /// * `topic` - Topic to query.
    /// * `queue_id` - Queue ID to query.
    /// * `offset` - Logical offset to start from.
    /// * `max_msg_nums` - Maximum count of messages to query.
    /// * `max_total_msg_size` - Maximum total msg size of the messages.
    /// * `message_filter` - Message filter used to screen desired messages.
    ///
    /// # Returns
    ///
    /// Matched messages.
    /*fn get_message_with_size_async(
        &self,
        group: &str,
        topic: &str,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: impl MessageFilter,
    ) -> impl Future<Output = GetMessageResult>;*/

    /// Get the maximum offset of the topic queue.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic name.
    /// * `queue_id` - Queue ID.
    ///
    /// # Returns
    ///
    /// Maximum offset at present.
    /* fn get_max_offset_in_queue(&self, topic: &str, queue_id: i32) -> i64; */

    /// Get the maximum offset of the topic queue.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic name.
    /// * `queue_id` - Queue ID.
    /// * `committed` - Return the max offset in ConsumeQueue if true, or the max offset in
    ///   CommitLog if false.
    ///
    /// # Returns
    ///
    /// Maximum offset at present.

    fn get_max_offset_in_queue_with_commit(
        &self,
        topic: &str,
        queue_id: i32,
        committed: bool,
    ) -> i64;
}
