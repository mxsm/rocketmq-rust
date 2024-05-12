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
use std::{collections::HashMap, sync::Arc};

use bytes::{Bytes, BytesMut};
use rocketmq_common::{
    common::{
        attribute::cq_type::CQType,
        config::TopicConfig,
        message::{
            message_batch::MessageExtBatch, message_single::MessageExtBrokerInner, MessageConst,
        },
        sys_flag::message_sys_flag::MessageSysFlag,
    },
    utils::{message_utils, queue_type_utils::QueueTypeUtils},
};

use crate::{
    base::{
        message_result::AppendMessageResult, message_status_enum::AppendMessageStatus,
        put_message_context::PutMessageContext,
    },
    config::message_store_config::MessageStoreConfig,
    log_file::{
        commit_log::{CommitLog, CRC32_RESERVED_LEN},
        mapped_file::MappedFile,
    },
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
    fn do_append<MF: MappedFile>(
        &self,
        file_from_offset: i64,
        mapped_file: &MF,
        max_blank: i32,
        msg: &mut MessageExtBrokerInner,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult;

    /*    fn do_append_back(
        &self,
        file_from_offset: i64,
        mapped_file: &DefaultMappedFile,
        max_blank: i32,
        msg: &mut MessageExtBrokerInner,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult;*/

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
        byte_buffer: &mut BytesMut,
        max_blank: i32,
        message_ext_batch: &MessageExtBatch,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult;
}

// File at the end of the minimum fixed length empty
const END_FILE_MIN_BLANK_LENGTH: i32 = 4 + 4;

pub(crate) struct DefaultAppendMessageCallback {
    msg_store_item_memory: bytes::BytesMut,
    crc32_reserved_length: i32,
    message_store_config: Arc<MessageStoreConfig>,
    topic_config_table: Arc<parking_lot::Mutex<HashMap<String, TopicConfig>>>,
}

impl DefaultAppendMessageCallback {
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        topic_config_table: Arc<parking_lot::Mutex<HashMap<String, TopicConfig>>>,
    ) -> Self {
        Self {
            msg_store_item_memory: bytes::BytesMut::with_capacity(
                END_FILE_MIN_BLANK_LENGTH as usize,
            ),
            crc32_reserved_length: CRC32_RESERVED_LEN,
            message_store_config,
            topic_config_table,
        }
    }
}

impl DefaultAppendMessageCallback {
    fn get_message_num(&self, msg_inner: &MessageExtBrokerInner) -> i16 {
        let mut message_num = 1i16;
        let cq_type = self.get_cq_type(msg_inner);
        if MessageSysFlag::check(msg_inner.sys_flag(), MessageSysFlag::INNER_BATCH_FLAG)
            || CQType::BatchCQ == cq_type
        {
            if let Some(key) = msg_inner.property(MessageConst::PROPERTY_INNER_NUM) {
                message_num = key.parse::<i16>().unwrap_or(1);
            }
        }
        message_num
    }

    fn get_cq_type(&self, msg_inner: &MessageExtBrokerInner) -> CQType {
        let topic_config = self
            .topic_config_table
            .lock()
            .get(msg_inner.message_ext_inner.topic())
            .cloned();
        QueueTypeUtils::get_cq_type(&topic_config)
    }
}

#[allow(unused_variables)]
impl AppendMessageCallback for DefaultAppendMessageCallback {
    fn do_append<MF: MappedFile>(
        &self,
        file_from_offset: i64,
        mapped_file: &MF,
        max_blank: i32,
        msg_inner: &mut MessageExtBrokerInner,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        let mut pre_encode_buffer = msg_inner.encoded_buff.take().unwrap(); // Assuming get_encoded_buff returns Option<ByteBuffer>
        let is_multi_dispatch_msg = self.message_store_config.enable_multi_dispatch
            && CommitLog::is_multi_dispatch_msg(msg_inner);
        if is_multi_dispatch_msg {
            /*if let Some(result) = self.handle_properties_for_lmq_msg(&msg_inner) {
                return result;
            }*/
        }

        let msg_len = i32::from_le_bytes(pre_encode_buffer[0..4].try_into().unwrap());
        let wrote_offset = file_from_offset + mapped_file.get_wrote_position() as i64;

        let msg_id =
            message_utils::build_message_id(msg_inner.message_ext_inner.store_host, wrote_offset);

        let mut queue_offset = msg_inner.queue_offset();
        //let message_num = CommitLog::get_message_num(msg_inner);
        let message_num = self.get_message_num(msg_inner);
        match MessageSysFlag::get_transaction_value(msg_inner.sys_flag()) {
            MessageSysFlag::TRANSACTION_PREPARED_TYPE
            | MessageSysFlag::TRANSACTION_ROLLBACK_TYPE => queue_offset = 0,
            // MessageSysFlag::TRANSACTION_NOT_TYPE | MessageSysFlag::TRANSACTION_COMMIT_TYPE | _ =>
            // {}
            _ => {}
        }

        if (msg_len + END_FILE_MIN_BLANK_LENGTH) > max_blank {
            /*self.msg_store_item_memory.borrow_mut().clear();
            self.msg_store_item_memory.borrow_mut().put_i32(max_blank);
            self.msg_store_item_memory
                .borrow_mut()
                .put_i32(BLANK_MAGIC_CODE);
            let bytes = self.msg_store_item_memory.borrow_mut().split().freeze();
            mapped_file.append_message_bytes(&bytes);*/
            return AppendMessageResult {
                status: AppendMessageStatus::EndOfFile,
                wrote_offset,
                wrote_bytes: max_blank,
                msg_id,
                store_timestamp: msg_inner.store_timestamp(),
                logics_offset: queue_offset,
                ..Default::default()
            };
        }

        let mut pos = 4 + 4 + 4 + 4 + 4;
        pre_encode_buffer[pos..(pos + 8)].copy_from_slice(&queue_offset.to_le_bytes());
        pos += 8;
        pre_encode_buffer[pos..(pos + 8)].copy_from_slice(&wrote_offset.to_le_bytes());
        let ip_len = if msg_inner.sys_flag() & MessageSysFlag::BORNHOST_V6_FLAG == 0 {
            4 + 4
        } else {
            16 + 4
        };
        pos += 8 + 4 + 8 + ip_len;
        pre_encode_buffer[pos..(pos + 8)]
            .copy_from_slice(&msg_inner.store_timestamp().to_le_bytes());

        // msg_inner.encoded_buff = pre_encode_buffer;
        let bytes = Bytes::from(pre_encode_buffer);
        mapped_file.append_message_bytes(&bytes);
        AppendMessageResult {
            status: AppendMessageStatus::PutOk,
            wrote_offset,
            wrote_bytes: msg_len,
            msg_id,
            store_timestamp: msg_inner.store_timestamp(),
            logics_offset: queue_offset,
            ..Default::default()
        }
    }

    fn do_append_batch(
        &self,
        file_from_offset: i64,
        byte_buffer: &mut BytesMut,
        max_blank: i32,
        message_ext_batch: &MessageExtBatch,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        todo!()
    }

    /* fn do_append_batch(
        &self,
        file_from_offset: i64,
        byte_buffer: &mut BytesMut,
        max_blank: i32,
        message_ext_batch: &MessageExtBatch,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        todo!()
    }*/
    /* fn do_append(
        &mut self,
        file_from_offset: i64,
        byte_buffer: &mut ByteBuffer,
        max_blank: i32,
        msg_inner: &mut MessageExtBrokerInner,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        let mut pre_encode_buffer = bytes::BytesMut::from(msg_inner.encoded_buff.as_ref());
        let is_multi_dispatch_msg = self.message_store_config.enable_multi_dispatch
            && CommitLog::is_multi_dispatch_msg(msg_inner);
        if is_multi_dispatch_msg {
            unimplemented!()
        }
        //can optimize?
        let msg_len = pre_encode_buffer.clone().get_i32();
        //physical offset
        let wrote_offset = file_from_offset + byte_buffer.position;

        let msg_id_supplier = || {
            let sys_flag = msg_inner.sys_flag();
            let msg_id_len = if (sys_flag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG) == 0 {
                4 + 4 + 8
            } else {
                16 + 4 + 8
            };

            let mut msg_id_buffer = bytes::BytesMut::from(
                MessageExt::socket_address_2_byte_buffer(&msg_inner.store_host()).as_ref(),
            );
            msg_id_buffer.put_i64(wrote_offset);
            UtilAll::bytes_to_string(msg_id_buffer.as_ref())
        };

        let mut queue_offset = msg_inner.queue_offset();

        let message_num = CommitLog::get_message_num(msg_inner);

        let tran_type = MessageSysFlag::get_transaction_value(msg_inner.sys_flag());
        match tran_type {
            MessageSysFlag::TRANSACTION_PREPARED_TYPE
            | MessageSysFlag::TRANSACTION_ROLLBACK_TYPE => {
                queue_offset = 0;
            }
            _ => {}
        }

        if (msg_len + END_FILE_MIN_BLANK_LENGTH) > max_blank {
            self.msg_store_item_memory.clear();
            self.msg_store_item_memory.put_i32(max_blank);
            self.msg_store_item_memory.put_i32(BLANK_MAGIC_CODE);
            //  byte_buffer.put(&self.msg_store_item_memory[0..8]);
            /* let begin_time_mills = CommitLog::default_message_store().now(); */
            /*return AppendMessageResult::new(
                AppendMessageStatus::EndOfFile,
                wrote_offset,
                max_blank as usize,
                msg_id_supplier,
                msg_inner.get_store_timestamp(),
                queue_offset,
                CommitLog::default_message_store().now() - begin_time_mills,
                None,
            );*/
            return AppendMessageResult::default();
        }

        let mut pos = 4 + 4 + 4 + 4 + 4;
        pre_encode_buffer[pos..(pos + mem::size_of::<i64>())]
            .copy_from_slice(&queue_offset.to_be_bytes());
        pos += 8;

        pre_encode_buffer[pos..(pos + mem::size_of::<i64>())]
            .copy_from_slice(&(file_from_offset + byte_buffer.get_position()).to_be_bytes());
        let ip_len = if (msg_inner.sys_flag() & MessageSysFlag::BORNHOST_V6_FLAG) == 0 {
            4 + 4
        } else {
            16 + 4
        };
        pos += 8 + 4 + 8 + ip_len;
        // refresh store time stamp in lock
        pre_encode_buffer[pos..(pos + mem::size_of::<i64>())]
            .copy_from_slice(msg_inner.store_timestamp().to_be_bytes().as_ref());
        /*        if enabled_append_prop_crc {
            let check_size = msg_len - MessageDecoder::CRC32_RESERVED_LENGTH;
            let mut tmp_buffer = pre_encode_buffer.duplicate();
            tmp_buffer.limit(tmp_buffer.position() + check_size as usize);
            let crc32 = MessageDecoder::crc32(&tmp_buffer);
            tmp_buffer.limit(tmp_buffer.position() + MessageDecoder::CRC32_RESERVED_LENGTH);
            MessageDecoder::create_crc32(&mut tmp_buffer, crc32);
        }*/

        //let begin_time_mills = CommitLog::default_message_store().now();
        /*   CommitLog::default_message_store()
        .perf_counter()
        .start_tick("WRITE_MEMORY_TIME_MS");*/
        byte_buffer
            .get_data_mut()
            .write_all(&pre_encode_buffer)
            .unwrap();
        /* CommitLog::default_message_store()
        .perf_counter()
        .end_tick("WRITE_MEMORY_TIME_MS");*/
        msg_inner.encoded_buff.clear();

        /*        if is_multi_dispatch_msg {
            CommitLog::multi_dispatch().update_multi_queue_offset(&msg_inner);
        }*/

        /*AppendMessageResult::new(
            AppendMessageStatus::PutOk,
            wrote_offset,
            msg_len as usize,
            msg_id_supplier,
            msg_inner.store_timestamp(),
            queue_offset,
            0,
            Some(message_num),
        )*/
        AppendMessageResult::default()
    }*/
}
