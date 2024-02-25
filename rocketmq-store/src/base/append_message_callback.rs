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
use rocketmq_common::common::message::{
    message_batch::MessageExtBatch, message_single::MessageExtBrokerInner,
};

use crate::base::{message_result::AppendMessageResult, put_message_context::PutMessageContext};

/// Write messages callback interface
pub trait AppendMessageCallback {
    /// After message serialization, write MappedByteBuffer
    ///
    /// # Arguments
    ///
    /// * `file_from_offset` - The offset of the file
    /// * `byte_buffer` - The buffer to write
    /// * `max_blank` - The maximum blank space
    /// * `msg` - The message to write
    /// * `put_message_context` - The context of putting message
    ///
    /// # Returns
    ///
    /// The number of bytes written
    fn do_append(
        &self,
        file_from_offset: i64,
        byte_buffer: &mut [u8],
        max_blank: i32,
        msg: &MessageExtBrokerInner,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult;

    /// After batched message serialization, write MappedByteBuffer
    ///
    /// # Arguments
    ///
    /// * `file_from_offset` - The offset of the file
    /// * `byte_buffer` - The buffer to write
    /// * `max_blank` - The maximum blank space
    /// * `message_ext_batch` - The batched message to write
    /// * `put_message_context` - The context of putting message
    ///
    /// # Returns
    ///
    /// The number of bytes written
    fn do_append_batch(
        &self,
        file_from_offset: i64,
        byte_buffer: &mut [u8],
        max_blank: i32,
        message_ext_batch: &MessageExtBatch,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult;
}
