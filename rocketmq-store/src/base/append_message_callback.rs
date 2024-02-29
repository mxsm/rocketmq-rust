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
use std::sync::Arc;

use rocketmq_common::common::message::{
    message_batch::MessageExtBatch, message_single::MessageExtBrokerInner,
};

use crate::{
    base::{
        message_result::AppendMessageResult, put_message_context::PutMessageContext, ByteBuffer,
    },
    config::message_store_config::MessageStoreConfig,
    log_file::commit_log::CRC32_RESERVED_LEN,
};

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
        byte_buffer: &mut ByteBuffer,
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

// File at the end of the minimum fixed length empty
const END_FILE_MIN_BLANK_LENGTH: i32 = 4 + 4;

pub(crate) struct DefaultAppendMessageCallback {
    pub msg_store_item_memory: bytes::BytesMut,
    pub crc32_reserved_length: i32,
    pub message_store_config: Arc<MessageStoreConfig>,
}

impl DefaultAppendMessageCallback {
    pub fn new(message_store_config: Arc<MessageStoreConfig>) -> Self {
        Self {
            msg_store_item_memory: bytes::BytesMut::with_capacity(
                END_FILE_MIN_BLANK_LENGTH as usize,
            ),
            crc32_reserved_length: CRC32_RESERVED_LEN,
            message_store_config,
        }
    }
}

#[allow(unused_variables)]
impl AppendMessageCallback for DefaultAppendMessageCallback {
    fn do_append(
        &self,
        file_from_offset: i64,
        byte_buffer: &mut ByteBuffer,
        max_blank: i32,
        msg_inner: &MessageExtBrokerInner,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        /* let mut pre_encode_buffer = msg_inner.encoded_buff.clone();
        let is_multi_dispatch_msg = self.message_store_config.enable_multi_dispatch
            && CommitLog::is_multi_dispatch_msg(&msg_inner);
        if is_multi_dispatch_msg {
            unimplemented!()
        }
        let msg_len = pre_encode_buffer.get_i32();
        let size = std::mem::size_of::<i32>();
        let wrote_offset = file_from_offset + byte_buffer.position;
        pre_encode_buffer.advance(size);

        let msg_id_supplier = || {
            let sys_flag = msg_inner.get_sys_flag();
            let msg_id_len = if (sys_flag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG) == 0 {
                4 + 4 + 8
            } else {
                16 + 4 + 8
            };

            let mut msg_id_buffer = bytes::BytesMut::from(
                MessageExt::socket_address_2_byte_buffer(&msg_inner.store_host()),
            );
            msg_id_buffer.put_i64(wrote_offset);
            UtilAll::bytes_to_string(msg_id_buffer.array())
        };

        let queue_offset = msg_inner.queue_offset();

        let message_num = get_message_num(&msg_inner);

        let tran_type = MessageSysFlag::get_transaction_value(msg_inner.get_sys_flag());
        match tran_type {
            MessageSysFlag::TRANSACTION_PREPARED_TYPE
            | MessageSysFlag::TRANSACTION_ROLLBACK_TYPE => {
                queue_offset = Some(0);
            }
            MessageSysFlag::TRANSACTION_NOT_TYPE | MessageSysFlag::TRANSACTION_COMMIT_TYPE | _ => {}
        }

        if (msg_len + END_FILE_MIN_BLANK_LENGTH as i32) > max_blank {
            msg_store_item_memory.clear();
            msg_store_item_memory.put_int(max_blank);
            msg_store_item_memory.put_int(CommitLog::BLANK_MAGIC_CODE);
            byte_buffer.put(&msg_store_item_memory.array()[0..8]);
            let begin_time_mills = CommitLog::default_message_store().now();
            return AppendMessageResult::new(
                AppendMessageStatus::END_OF_FILE,
                wrote_offset,
                max_blank as usize,
                msg_id_supplier,
                msg_inner.get_store_timestamp(),
                queue_offset,
                CommitLog::default_message_store().now() - begin_time_mills,
                None,
            );
        }

        let mut pos = 4 + 4 + 4 + 4 + 4;
        pre_encode_buffer.put_long(pos, queue_offset.unwrap());
        pos += 8;
        pre_encode_buffer.put_long(pos, file_from_offset + byte_buffer.position() as i64);
        let ip_len = if (msg_inner.get_sys_flag() & MessageSysFlag::BORNHOST_V6_FLAG) == 0 {
            4 + 4
        } else {
            16 + 4
        };
        pos += 8 + 4 + 8 + ip_len;
        pre_encode_buffer.put_long(pos, msg_inner.get_store_timestamp());
        if enabled_append_prop_crc {
            let check_size = msg_len - MessageDecoder::CRC32_RESERVED_LENGTH;
            let mut tmp_buffer = pre_encode_buffer.duplicate();
            tmp_buffer.limit(tmp_buffer.position() + check_size as usize);
            let crc32 = MessageDecoder::crc32(&tmp_buffer);
            tmp_buffer.limit(tmp_buffer.position() + MessageDecoder::CRC32_RESERVED_LENGTH);
            MessageDecoder::create_crc32(&mut tmp_buffer, crc32);
        }

        let begin_time_mills = CommitLog::default_message_store().now();
        CommitLog::default_message_store()
            .perf_counter()
            .start_tick("WRITE_MEMORY_TIME_MS");
        byte_buffer.put(&pre_encode_buffer);
        CommitLog::default_message_store()
            .perf_counter()
            .end_tick("WRITE_MEMORY_TIME_MS");
        msg_inner.set_encoded_buff(None);

        if is_multi_dispatch_msg {
            CommitLog::multi_dispatch().update_multi_queue_offset(&msg_inner);
        }

        AppendMessageResult::new(
            AppendMessageStatus::PutOk,
            wrote_offset,
            msg_len as usize,
            msg_id_supplier,
            msg_inner.get_store_timestamp(),
            queue_offset,
            CommitLog::default_message_store().now() - begin_time_mills,
            Some(message_num),
        );*/
        unimplemented!()
    }

    fn do_append_batch(
        &self,
        file_from_offset: i64,
        byte_buffer: &mut [u8],
        max_blank: i32,
        message_ext_batch: &MessageExtBatch,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        unimplemented!()
    }
}
