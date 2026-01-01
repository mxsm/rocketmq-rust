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

use std::sync::Arc;
use std::time::Instant;

use bytes::Buf;
use bytes::BufMut;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::utils::message_utils;
use rocketmq_common::CRC32Utils::crc32;
use rocketmq_common::MessageDecoder::create_crc32;
use rocketmq_common::MessageUtils::build_batch_message_id;
use rocketmq_rust::ArcMut;
use rocketmq_rust::SyncUnsafeCellWrapper;
use tracing::error;

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
        put_message_context: &mut PutMessageContext,
        enabled_append_prop_crc: bool,
    ) -> AppendMessageResult;

    /// Encode message directly to mmap buffer
    ///
    /// This method eliminates memory copying by encoding the message directly into the
    /// memory-mapped file region, bypassing the intermediate pre_encode_buffer.
    ///
    ///
    /// # Arguments
    ///
    /// * `file_from_offset` - The offset of the file
    /// * `mapped_file` - The mapped file to write to
    /// * `max_blank` - The maximum blank space available
    /// * `msg` - The message to encode
    /// * `put_message_context` - The context of putting message
    ///
    /// # Returns
    ///
    /// AppendMessageResult containing status and metrics
    ///
    /// # Implementation Note
    ///
    /// Default implementation falls back to standard `do_append` for compatibility.
    /// Override this method to enable zero-copy encoding.
    fn do_append_zerocopy<MF: MappedFile>(
        &self,
        file_from_offset: i64,
        mapped_file: &MF,
        max_blank: i32,
        msg: &mut MessageExtBrokerInner,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        // Default: fall back to standard append
        self.do_append(file_from_offset, mapped_file, max_blank, msg, put_message_context)
    }
}

// File at the end of the minimum fixed length empty
const END_FILE_MIN_BLANK_LENGTH: i32 = 4 + 4;

pub struct DefaultAppendMessageCallback {
    msg_store_item_memory: SyncUnsafeCellWrapper<bytes::BytesMut>,
    crc32_reserved_length: i32,
    message_store_config: Arc<MessageStoreConfig>,
    topic_config_table: Arc<DashMap<CheetahString, ArcMut<TopicConfig>>>,
}

impl DefaultAppendMessageCallback {
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        topic_config_table: Arc<DashMap<CheetahString, ArcMut<TopicConfig>>>,
    ) -> Self {
        let crc32_reserved_length = if message_store_config.enabled_append_prop_crc {
            CRC32_RESERVED_LEN
        } else {
            0
        };
        Self {
            msg_store_item_memory: SyncUnsafeCellWrapper::new(bytes::BytesMut::with_capacity(
                END_FILE_MIN_BLANK_LENGTH as usize,
            )),
            crc32_reserved_length,
            message_store_config,
            topic_config_table,
        }
    }
}

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
        let is_multi_dispatch_msg =
            self.message_store_config.enable_multi_dispatch && CommitLog::is_multi_dispatch_msg(msg_inner);
        if is_multi_dispatch_msg {
            unimplemented!("Multi dispatch message is not supported yet");
        }

        let msg_len = i32::from_be_bytes(pre_encode_buffer[0..4].try_into().unwrap());
        //physic offset
        let wrote_offset = file_from_offset + mapped_file.get_wrote_position() as i64;
        let addr = msg_inner.message_ext_inner.store_host;
        let msg_id_supplier = move || -> String { message_utils::build_message_id(addr, wrote_offset) };

        let mut queue_offset = msg_inner.queue_offset();
        let message_num = get_message_num(&self.topic_config_table, msg_inner);
        // Transaction messages that require special handling
        if let MessageSysFlag::TRANSACTION_PREPARED_TYPE | MessageSysFlag::TRANSACTION_ROLLBACK_TYPE =
            MessageSysFlag::get_transaction_value(msg_inner.sys_flag())
        {
            queue_offset = 0;
        }

        // Determines whether there is sufficient free space
        if (msg_len + END_FILE_MIN_BLANK_LENGTH) > max_blank {
            let bytes = self.msg_store_item_memory.mut_from_ref();
            bytes.clear();
            bytes.put_i32(max_blank);
            bytes.put_i32(BLANK_MAGIC_CODE);
            let instant = Instant::now();
            mapped_file.write_bytes_segment(bytes.as_ref(), wrote_offset as usize, 0, bytes.len());
            return AppendMessageResult {
                status: AppendMessageStatus::EndOfFile,
                wrote_offset,
                /* only wrote 8 bytes, but declare wrote maxBlank for compute write position */
                wrote_bytes: max_blank,
                store_timestamp: msg_inner.store_timestamp(),
                logics_offset: queue_offset,
                msg_num: message_num as i32,
                msg_id_supplier: Some(Arc::new(msg_id_supplier)),
                page_cache_rt: instant.elapsed().as_millis() as i64,
                ..Default::default()
            };
        }

        /*        let mut pos = 4 // 1 TOTALSIZE
        + 4// 2 MAGICCODE
        + 4// 3 BODYCRC
        + 4 // 4 QUEUEID
        + 4; // 5 FLAG*/
        let mut pos = 20; // TOTALSIZE +  MAGICCODE + BODYCRC + QUEUEID + FLAG
        pre_encode_buffer[pos..(pos + 8)].copy_from_slice(&queue_offset.to_be_bytes()); // 6 QUEUEOFFSET
        pos += 8;
        pre_encode_buffer[pos..(pos + 8)].copy_from_slice(&wrote_offset.to_be_bytes()); // 7 PHYSICALOFFSET
        let ip_len = if msg_inner.sys_flag() & MessageSysFlag::BORNHOST_V6_FLAG == 0 {
            4 + 4
        } else {
            16 + 4
        };
        // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST
        pos += 8 + 4 + 8 + ip_len;

        // 11 STORETIMESTAMP refresh store time stamp in lock
        pre_encode_buffer[pos..(pos + 8)].copy_from_slice(&msg_inner.store_timestamp().to_be_bytes());

        if self.message_store_config.enabled_append_prop_crc {
            // 18 CRC32
            let check_size = msg_len - self.crc32_reserved_length;
            let crc32 = crc32(&pre_encode_buffer[..check_size as usize]);
            create_crc32(&mut pre_encode_buffer[check_size as usize..msg_len as usize], crc32);
        }

        //let bytes = pre_encode_buffer.freeze();
        let instant = Instant::now();
        mapped_file.append_message_bytes_no_position_update_ref(pre_encode_buffer.chunk());
        AppendMessageResult {
            status: AppendMessageStatus::PutOk,
            wrote_offset,
            wrote_bytes: msg_len,
            store_timestamp: msg_inner.store_timestamp(),
            logics_offset: queue_offset,
            msg_num: message_num as i32,
            msg_id_supplier: Some(Arc::new(Box::new(msg_id_supplier))),
            page_cache_rt: instant.elapsed().as_millis() as i64,
            ..Default::default()
        }
    }

    fn do_append_batch<MF: MappedFile>(
        &self,
        file_from_offset: i64,
        mapped_file: &MF,
        max_blank: i32,
        msg_batch: &mut MessageExtBatch,
        put_message_context: &mut PutMessageContext,
        enabled_append_prop_crc: bool,
    ) -> AppendMessageResult {
        //physic offset--The starting point for writing this message file.If, while writing a
        // message, it is found that the length is insufficient,the remaining length of the file
        // and the end-of-file marker should be rewritten at this point.
        let wrote_offset = file_from_offset + mapped_file.get_wrote_position() as i64;
        // Record ConsumeQueue information
        let queue_offset = msg_batch.message_ext_broker_inner.queue_offset();
        let begin_queue_offset = queue_offset;

        let begin_time_mills = Instant::now();

        // Assuming get_encoded_buff returns Option<ByteBuffer>
        let mut messages_byte_buffer = msg_batch.encoded_buff.take().unwrap();
        let sys_flag = msg_batch.message_ext_broker_inner.sys_flag();
        //born host length
        let born_host_length = if sys_flag & MessageSysFlag::BORNHOST_V6_FLAG == 0 {
            4 + 4
        } else {
            16 + 4
        };
        //store host length
        let store_host_length = if sys_flag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG == 0 {
            4 + 4
        } else {
            16 + 4
        };
        let addr = msg_batch.message_ext_broker_inner.store_host();
        let batch_size = put_message_context.get_batch_size();
        let phy_ops = put_message_context.get_phy_pos().to_vec();
        let msg_id_supplier =
            move || -> String { build_batch_message_id(addr, store_host_length, batch_size as usize, &phy_ops) };
        let mut total_msg_len = 0;
        let mut msg_num = 0;
        let mut msg_pos = 0;
        let mut index = 0;
        while total_msg_len < messages_byte_buffer.len() as i32 {
            let msg_len = i32::from_be_bytes(
                messages_byte_buffer[total_msg_len as usize..(total_msg_len + 4) as usize]
                    .try_into()
                    .unwrap(),
            );
            total_msg_len += msg_len;
            if total_msg_len + END_FILE_MIN_BLANK_LENGTH > max_blank {
                let bytes = self.msg_store_item_memory.mut_from_ref();
                bytes.clear();
                bytes.put_i32(max_blank);
                bytes.put_i32(BLANK_MAGIC_CODE);
                mapped_file.write_bytes_segment(bytes.as_ref(), wrote_offset as usize, 0, bytes.len());
                return AppendMessageResult {
                    status: AppendMessageStatus::EndOfFile,
                    wrote_offset,
                    wrote_bytes: max_blank,
                    msg_id_supplier: Some(Arc::new(Box::new(msg_id_supplier))),
                    store_timestamp: msg_batch.message_ext_broker_inner.store_timestamp(),
                    logics_offset: begin_queue_offset,
                    page_cache_rt: begin_time_mills.elapsed().as_millis() as i64,
                    ..Default::default()
                };
            }
            let mut pos = msg_pos + 20;
            messages_byte_buffer[pos..(pos + 8)].copy_from_slice(&queue_offset.to_be_bytes());
            pos += 8;
            let phy_pos = wrote_offset + total_msg_len as i64 - msg_len as i64;
            messages_byte_buffer[pos..(pos + 8)].copy_from_slice(&phy_pos.to_be_bytes());
            pos += 8 + 4 + 8 + born_host_length;
            messages_byte_buffer[pos..(pos + 8)]
                .copy_from_slice(&msg_batch.message_ext_broker_inner.store_timestamp().to_be_bytes());
            if enabled_append_prop_crc {
                let _check_size = msg_len - self.crc32_reserved_length;
            }
            put_message_context.get_phy_pos_mut()[index] = phy_pos;
            msg_num += 1;
            msg_pos += msg_len as usize;
            index += 1;
        }

        let bytes = messages_byte_buffer.freeze();
        mapped_file.append_message_bytes_no_position_update(&bytes);
        AppendMessageResult {
            status: AppendMessageStatus::PutOk,
            wrote_offset,
            wrote_bytes: total_msg_len,
            msg_id_supplier: Some(Arc::new(Box::new(msg_id_supplier))),
            store_timestamp: msg_batch.message_ext_broker_inner.store_timestamp(),
            logics_offset: begin_queue_offset,
            page_cache_rt: begin_time_mills.elapsed().as_millis() as i64,
            msg_num,
            ..Default::default()
        }
    }

    /// **Zero-Copy Implementation**
    ///
    /// Encodes message directly into memory-mapped file region, eliminating intermediate buffer.
    ///
    ///
    /// # Implementation Details
    /// 1. Get direct mutable buffer from MappedFile
    /// 2. Encode message fields directly into buffer (no intermediate BytesMut)
    /// 3. Commit write position atomically
    /// 4. Return append result
    fn do_append_zerocopy<MF: MappedFile>(
        &self,
        file_from_offset: i64,
        mapped_file: &MF,
        max_blank: i32,
        msg_inner: &mut MessageExtBrokerInner,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        // Extract pre-encoded buffer (still contains metadata we need)
        let pre_encode_buffer = msg_inner.encoded_buff.take().unwrap();
        let is_multi_dispatch_msg =
            self.message_store_config.enable_multi_dispatch && CommitLog::is_multi_dispatch_msg(msg_inner);

        if is_multi_dispatch_msg {
            // Fall back to standard implementation for multi-dispatch
            msg_inner.encoded_buff = Some(pre_encode_buffer);
            return self.do_append(file_from_offset, mapped_file, max_blank, msg_inner, put_message_context);
        }

        let msg_len = i32::from_be_bytes(pre_encode_buffer[0..4].try_into().unwrap());

        // Calculate physical offset
        let wrote_offset = file_from_offset + mapped_file.get_wrote_position() as i64;
        let addr = msg_inner.message_ext_inner.store_host;
        let msg_id_supplier = move || -> String { message_utils::build_message_id(addr, wrote_offset) };

        let mut queue_offset = msg_inner.queue_offset();
        let message_num = get_message_num(&self.topic_config_table, msg_inner);

        // Handle transaction messages
        if let MessageSysFlag::TRANSACTION_PREPARED_TYPE | MessageSysFlag::TRANSACTION_ROLLBACK_TYPE =
            MessageSysFlag::get_transaction_value(msg_inner.sys_flag())
        {
            queue_offset = 0;
        }

        // Check if we have enough space
        if (msg_len + END_FILE_MIN_BLANK_LENGTH) > max_blank {
            // Write end-of-file marker
            let bytes = self.msg_store_item_memory.mut_from_ref();
            bytes.clear();
            bytes.put_i32(max_blank);
            bytes.put_i32(BLANK_MAGIC_CODE);
            let instant = Instant::now();
            mapped_file.write_bytes_segment(bytes.as_ref(), wrote_offset as usize, 0, bytes.len());
            return AppendMessageResult {
                status: AppendMessageStatus::EndOfFile,
                wrote_offset,
                wrote_bytes: max_blank,
                store_timestamp: msg_inner.store_timestamp(),
                logics_offset: queue_offset,
                msg_num: message_num as i32,
                msg_id_supplier: Some(Arc::new(msg_id_supplier)),
                page_cache_rt: instant.elapsed().as_millis() as i64,
                ..Default::default()
            };
        }

        // **ZERO-COPY PATH**: Get direct mutable buffer from mmap
        let instant = Instant::now();
        if let Some((buffer, _pos)) = mapped_file.get_direct_write_buffer(msg_len as usize) {
            // Copy pre-encoded buffer directly to mmap (single copy, no intermediate buffer)
            buffer[..msg_len as usize].copy_from_slice(&pre_encode_buffer[..msg_len as usize]);

            // Update runtime fields that weren't known at pre-encode time
            let mut pos = 20; // Skip TOTALSIZE, MAGICCODE, BODYCRC, QUEUEID, FLAG

            // 6 QUEUEOFFSET - update with actual queue offset
            buffer[pos..pos + 8].copy_from_slice(&queue_offset.to_be_bytes());
            pos += 8;

            // 7 PHYSICALOFFSET - update with actual physical offset
            buffer[pos..pos + 8].copy_from_slice(&wrote_offset.to_be_bytes());

            // Calculate IP length to skip to store timestamp
            let ip_len = if msg_inner.sys_flag() & MessageSysFlag::BORNHOST_V6_FLAG == 0 {
                4 + 4
            } else {
                16 + 4
            };
            pos += 8 + 4 + 8 + ip_len; // Skip SYSFLAG, BORNTIMESTAMP, BORNHOST

            // 11 STORETIMESTAMP - update with current timestamp
            buffer[pos..pos + 8].copy_from_slice(&msg_inner.store_timestamp().to_be_bytes());

            // Update CRC32 if enabled
            if self.message_store_config.enabled_append_prop_crc {
                let check_size = msg_len - self.crc32_reserved_length;
                let crc32 = crc32(&buffer[..check_size as usize]);
                create_crc32(&mut buffer[check_size as usize..msg_len as usize], crc32);
            }

            // Commit the write atomically
            if !mapped_file.commit_direct_write(msg_len as usize) {
                error!("Failed to commit zero-copy write");
                return AppendMessageResult {
                    status: AppendMessageStatus::UnknownError,
                    ..Default::default()
                };
            }

            AppendMessageResult {
                status: AppendMessageStatus::PutOk,
                wrote_offset,
                wrote_bytes: msg_len,
                store_timestamp: msg_inner.store_timestamp(),
                logics_offset: queue_offset,
                msg_num: message_num as i32,
                msg_id_supplier: Some(Arc::new(Box::new(msg_id_supplier))),
                page_cache_rt: instant.elapsed().as_millis() as i64,
                ..Default::default()
            }
        } else {
            // Fall back to standard implementation if direct buffer unavailable
            msg_inner.encoded_buff = Some(pre_encode_buffer);
            self.do_append(file_from_offset, mapped_file, max_blank, msg_inner, put_message_context)
        }
    }
}
