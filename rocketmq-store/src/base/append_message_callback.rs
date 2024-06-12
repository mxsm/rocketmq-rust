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

use bytes::BufMut;
use bytes::BytesMut;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_single::MessageExtBrokerInner;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::utils::message_utils;

use crate::base::message_result::AppendMessageResult;
use crate::base::message_status_enum::AppendMessageStatus;
use crate::base::put_message_context::PutMessageContext;
use crate::config::message_store_config::MessageStoreConfig;
use crate::log_file::commit_log::get_message_num;
use crate::log_file::commit_log::CommitLog;
use crate::log_file::commit_log::BLANK_MAGIC_CODE;
use crate::log_file::commit_log::CRC32_RESERVED_LEN;
use crate::log_file::mapped_file::MappedFile;

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

    fn do_append_batch<MF: MappedFile>(
        &self,
        file_from_offset: i64,
        mapped_file: &MF,
        max_blank: i32,
        msg: &mut MessageExtBatch,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult;
}

// File at the end of the minimum fixed length empty
const END_FILE_MIN_BLANK_LENGTH: i32 = 4 + 4;

pub(crate) struct DefaultAppendMessageCallback {
    //msg_store_item_memory: bytes::BytesMut,
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
            /*            msg_store_item_memory: bytes::BytesMut::with_capacity(
                END_FILE_MIN_BLANK_LENGTH as usize,
            ),*/
            crc32_reserved_length: CRC32_RESERVED_LEN,
            message_store_config,
            topic_config_table,
        }
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

        let msg_len = i32::from_be_bytes(pre_encode_buffer[0..4].try_into().unwrap());
        let wrote_offset = file_from_offset + mapped_file.get_wrote_position() as i64;

        let msg_id =
            message_utils::build_message_id(msg_inner.message_ext_inner.store_host, wrote_offset);

        let mut queue_offset = msg_inner.queue_offset();
        //let message_num = CommitLog::get_message_num(msg_inner);
        let message_num = get_message_num(&self.topic_config_table, msg_inner);
        match MessageSysFlag::get_transaction_value(msg_inner.sys_flag()) {
            MessageSysFlag::TRANSACTION_PREPARED_TYPE
            | MessageSysFlag::TRANSACTION_ROLLBACK_TYPE => queue_offset = 0,
            _ => {}
        }

        if (msg_len + END_FILE_MIN_BLANK_LENGTH) > max_blank {
            let mut bytes = BytesMut::with_capacity(END_FILE_MIN_BLANK_LENGTH as usize);
            bytes.put_i32(max_blank);
            bytes.put_i32(BLANK_MAGIC_CODE);
            mapped_file.append_message_bytes(&bytes.freeze());
            return AppendMessageResult {
                status: AppendMessageStatus::EndOfFile,
                wrote_offset,
                wrote_bytes: max_blank,
                msg_id,
                store_timestamp: msg_inner.store_timestamp(),
                logics_offset: queue_offset,
                msg_num: message_num as i32,
                ..Default::default()
            };
        }

        let mut pos = 4 + 4 + 4 + 4 + 4;
        pre_encode_buffer[pos..(pos + 8)].copy_from_slice(&queue_offset.to_be_bytes());
        pos += 8;
        pre_encode_buffer[pos..(pos + 8)].copy_from_slice(&wrote_offset.to_be_bytes());
        let ip_len = if msg_inner.sys_flag() & MessageSysFlag::BORNHOST_V6_FLAG == 0 {
            4 + 4
        } else {
            16 + 4
        };
        pos += 8 + 4 + 8 + ip_len;
        pre_encode_buffer[pos..(pos + 8)]
            .copy_from_slice(&msg_inner.store_timestamp().to_be_bytes());

        // msg_inner.encoded_buff = pre_encode_buffer;
        let bytes = pre_encode_buffer.freeze();
        mapped_file.append_message_bytes(&bytes);
        AppendMessageResult {
            status: AppendMessageStatus::PutOk,
            wrote_offset,
            wrote_bytes: msg_len,
            msg_id,
            store_timestamp: msg_inner.store_timestamp(),
            logics_offset: queue_offset,
            msg_num: message_num as i32,
            ..Default::default()
        }
    }

    fn do_append_batch<MF: MappedFile>(
        &self,
        file_from_offset: i64,
        mapped_file: &MF,
        max_blank: i32,
        msg: &mut MessageExtBatch,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        todo!()
    }
}
