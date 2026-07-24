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

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_store_local::commit_log::append_frame::AppendBatchFrameCursor;
use rocketmq_store_local::commit_log::append_frame::AppendFrameCrcPlan;
use rocketmq_store_local::commit_log::append_frame::AppendFrameKernel;
use rocketmq_store_local::commit_log::append_frame::HostWidth;
use rocketmq_store_local::commit_log::append_frame::SegmentAppendDecision;
use rocketmq_store_local::commit_log::append_frame::BLANK_MARKER_LENGTH;

use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::utils::message_utils;
use rocketmq_common::CRC32Utils::crc32;
use rocketmq_common::MessageDecoder::create_crc32;
use rocketmq_common::MessageUtils::build_batch_message_id;
use tracing::error;

use crate::base::message_result::AppendMessageResult;
use crate::base::message_status_enum::AppendMessageStatus;
use crate::base::put_message_context::PutMessageContext;
use crate::config::message_store_config::MessageStoreConfig;
use crate::log_file::commit_log::get_message_num;
use crate::log_file::commit_log::CRC32_RESERVED_LEN;
use crate::log_file::mapped_file::MappedFile;
use rocketmq_store_local::mapped_file::MappedWriteLease;

/// Write messages callback interface.
///
/// Implementations that take ownership of a message's encoded buffer must restore that exact
/// buffer before returning [`AppendMessageStatus::EndOfFile`], because the caller retries the same
/// message against the next CommitLog segment. The EOF path must not clone or re-encode the buffer.
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
}

pub struct DefaultAppendMessageCallback {
    crc32_reserved_length: i32,
    message_store_config: Arc<MessageStoreConfig>,
    topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
}

impl DefaultAppendMessageCallback {
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
    ) -> Self {
        let crc32_reserved_length = if message_store_config.enabled_append_prop_crc {
            CRC32_RESERVED_LEN
        } else {
            0
        };
        Self {
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
        _max_blank: i32,
        msg_inner: &mut MessageExtBrokerInner,
        _put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        let Some(pre_encode_buffer) = msg_inner.encoded_buff.take() else {
            error!("Message append requires a pre-encoded frame");
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        };
        let msg_len = AppendFrameKernel::declared_frame_length(pre_encode_buffer.as_ref());
        let Ok(msg_len_usize) = usize::try_from(msg_len) else {
            msg_inner.encoded_buff = Some(pre_encode_buffer);
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        };
        let Some(reservation_size) = msg_len_usize.checked_add(BLANK_MARKER_LENGTH) else {
            msg_inner.encoded_buff = Some(pre_encode_buffer);
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        };
        let mut lease = match mapped_file.reserve_write(reservation_size) {
            Ok(lease) => lease,
            Err(error) => {
                msg_inner.encoded_buff = Some(pre_encode_buffer);
                error!(%error, "Failed to reserve mapped-file append");
                return AppendMessageResult {
                    status: AppendMessageStatus::UnknownError,
                    ..Default::default()
                };
            }
        };
        let max_blank = i32::try_from(lease.capacity()).unwrap_or(i32::MAX);
        let wrote_offset = file_from_offset + lease.start_position() as i64;
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

        if let SegmentAppendDecision::Roll = AppendFrameKernel::segment_append_decision(msg_len, max_blank) {
            let marker = AppendFrameKernel::blank_marker(max_blank);
            let instant = Instant::now();
            if lease.capacity() >= BLANK_MARKER_LENGTH {
                lease.buffer_mut()[..BLANK_MARKER_LENGTH].copy_from_slice(marker.bytes());
            }
            let commit_bytes = lease.capacity();
            if let Err(error) = lease.commit(commit_bytes, Some(msg_inner.store_timestamp() as u64)) {
                msg_inner.encoded_buff = Some(pre_encode_buffer);
                error!(%error, "Failed to publish mapped-file end marker");
                return AppendMessageResult {
                    status: AppendMessageStatus::UnknownError,
                    ..Default::default()
                };
            }
            msg_inner.encoded_buff = Some(pre_encode_buffer);
            return AppendMessageResult {
                status: AppendMessageStatus::EndOfFile,
                wrote_offset,
                wrote_bytes: marker.declared_wrote_bytes(),
                store_timestamp: msg_inner.store_timestamp(),
                logics_offset: queue_offset,
                msg_num: message_num as i32,
                msg_id_supplier: Some(Arc::new(msg_id_supplier)),
                page_cache_rt: instant.elapsed().as_millis() as i64,
                ..Default::default()
            };
        }

        lease.buffer_mut()[..msg_len_usize].copy_from_slice(&pre_encode_buffer[..msg_len_usize]);
        let born_host_width = if msg_inner.sys_flag() & MessageSysFlag::BORNHOST_V6_FLAG == 0 {
            HostWidth::Ipv4
        } else {
            HostWidth::Ipv6
        };
        let crc_plan = AppendFrameKernel::finalize_frame(
            &mut lease.buffer_mut()[..msg_len_usize],
            queue_offset,
            wrote_offset,
            msg_inner.store_timestamp(),
            born_host_width,
            self.crc32_reserved_length,
        );
        if let AppendFrameCrcPlan::Trailer {
            covered_end,
            trailer_start,
            trailer_end,
        } = crc_plan
        {
            let crc32 = crc32(&lease.buffer_mut()[..covered_end]);
            create_crc32(&mut lease.buffer_mut()[trailer_start..trailer_end], crc32);
        }

        let instant = Instant::now();
        if let Err(error) = lease.commit(msg_len_usize, Some(msg_inner.store_timestamp() as u64)) {
            msg_inner.encoded_buff = Some(pre_encode_buffer);
            error!(%error, "Failed to commit mapped-file append");
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
    }

    fn do_append_batch<MF: MappedFile>(
        &self,
        file_from_offset: i64,
        mapped_file: &MF,
        _max_blank: i32,
        msg_batch: &mut MessageExtBatch,
        put_message_context: &mut PutMessageContext,
        enabled_append_prop_crc: bool,
    ) -> AppendMessageResult {
        let Some(messages_byte_buffer) = msg_batch.encoded_buff.take() else {
            error!("Batch append requires a pre-encoded frame");
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        };
        let Some(reservation_size) = messages_byte_buffer.len().checked_add(BLANK_MARKER_LENGTH) else {
            msg_batch.encoded_buff = Some(messages_byte_buffer);
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        };
        let mut lease = match mapped_file.reserve_write(reservation_size) {
            Ok(lease) => lease,
            Err(error) => {
                msg_batch.encoded_buff = Some(messages_byte_buffer);
                error!(%error, "Failed to reserve mapped-file batch append");
                return AppendMessageResult {
                    status: AppendMessageStatus::UnknownError,
                    ..Default::default()
                };
            }
        };
        let max_blank = i32::try_from(lease.capacity()).unwrap_or(i32::MAX);
        let wrote_offset = file_from_offset + lease.start_position() as i64;
        let queue_offset = msg_batch.message_ext_broker_inner.queue_offset();
        let begin_queue_offset = queue_offset;
        let begin_time_mills = Instant::now();
        let sys_flag = msg_batch.message_ext_broker_inner.sys_flag();
        let born_host_width = if sys_flag & MessageSysFlag::BORNHOST_V6_FLAG == 0 {
            HostWidth::Ipv4
        } else {
            HostWidth::Ipv6
        };
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

        if AppendFrameKernel::segment_append_decision(messages_byte_buffer.len() as i32, max_blank)
            == SegmentAppendDecision::Roll
        {
            let marker = AppendFrameKernel::blank_marker(max_blank);
            if lease.capacity() >= BLANK_MARKER_LENGTH {
                lease.buffer_mut()[..BLANK_MARKER_LENGTH].copy_from_slice(marker.bytes());
            }
            let commit_bytes = lease.capacity();
            if let Err(error) = lease.commit(
                commit_bytes,
                Some(msg_batch.message_ext_broker_inner.store_timestamp() as u64),
            ) {
                msg_batch.encoded_buff = Some(messages_byte_buffer);
                error!(%error, "Failed to publish mapped-file batch end marker");
                return AppendMessageResult {
                    status: AppendMessageStatus::UnknownError,
                    ..Default::default()
                };
            }
            msg_batch.encoded_buff = Some(messages_byte_buffer);
            return AppendMessageResult {
                status: AppendMessageStatus::EndOfFile,
                wrote_offset,
                wrote_bytes: marker.declared_wrote_bytes(),
                msg_id_supplier: Some(Arc::new(Box::new(msg_id_supplier))),
                store_timestamp: msg_batch.message_ext_broker_inner.store_timestamp(),
                logics_offset: begin_queue_offset,
                page_cache_rt: begin_time_mills.elapsed().as_millis() as i64,
                ..Default::default()
            };
        }

        lease.buffer_mut()[..messages_byte_buffer.len()].copy_from_slice(messages_byte_buffer.as_ref());
        let mut cursor = AppendBatchFrameCursor::new();
        while let Some(frame) = cursor.next(messages_byte_buffer.as_ref()) {
            let msg_len = frame.declared_len();
            let phy_pos = frame.physical_offset(wrote_offset);
            let _crc_plan = AppendFrameKernel::finalize_batch_frame(
                &mut lease.buffer_mut()[frame.start()..frame.end()],
                queue_offset,
                phy_pos,
                msg_batch.message_ext_broker_inner.store_timestamp(),
                born_host_width,
            );
            if enabled_append_prop_crc {
                let _check_size = msg_len - self.crc32_reserved_length;
            }
            put_message_context.get_phy_pos_mut()[frame.index()] = phy_pos;
            cursor.finish_frame(msg_len);
        }

        let committed_bytes = cursor.total_msg_len();
        let Ok(committed_bytes_usize) = usize::try_from(committed_bytes) else {
            msg_batch.encoded_buff = Some(messages_byte_buffer);
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        };
        if let Err(error) = lease.commit(
            committed_bytes_usize,
            Some(msg_batch.message_ext_broker_inner.store_timestamp() as u64),
        ) {
            msg_batch.encoded_buff = Some(messages_byte_buffer);
            error!(%error, "Failed to commit mapped-file batch append");
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        }
        AppendMessageResult {
            status: AppendMessageStatus::PutOk,
            wrote_offset,
            wrote_bytes: committed_bytes,
            msg_id_supplier: Some(Arc::new(Box::new(msg_id_supplier))),
            store_timestamp: msg_batch.message_ext_broker_inner.store_timestamp(),
            logics_offset: begin_queue_offset,
            page_cache_rt: begin_time_mills.elapsed().as_millis() as i64,
            msg_num: cursor.msg_num(),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Buf;
    use bytes::BufMut;
    use bytes::Bytes;
    use rocketmq_common::common::message::MessageTrait;
    use rocketmq_common::common::message::MessageVersion;
    use rocketmq_common::UtilAll::offset_to_file_name;
    use tempfile::tempdir;

    use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
    use crate::message_encoder::message_ext_encoder::MessageExtEncoder;

    fn callback_and_config() -> (DefaultAppendMessageCallback, Arc<MessageStoreConfig>) {
        let config = Arc::new(MessageStoreConfig::default());
        let callback = DefaultAppendMessageCallback::new(Arc::clone(&config), Arc::new(DashMap::new()));
        (callback, config)
    }

    fn encoded_single_message(config: Arc<MessageStoreConfig>) -> MessageExtBrokerInner {
        let mut msg = MessageExtBrokerInner::default();
        msg.with_version(MessageVersion::V1);
        msg.set_topic(CheetahString::from_static_str("af0s"));
        msg.set_body(Bytes::from(vec![7_u8; 75]));
        msg.message_ext_inner.set_queue_id(0);
        msg.message_ext_inner.set_store_timestamp(1234);

        let mut encoder = MessageExtEncoder::new(config);
        assert!(encoder.encode(&msg).is_none());
        msg.encoded_buff = Some(encoder.byte_buf());
        assert_eq!(msg.encoded_buff.as_ref().unwrap().len(), 170);
        msg
    }

    fn encoded_batch_message(config: Arc<MessageStoreConfig>) -> (MessageExtBatch, PutMessageContext) {
        let mut batch_body = bytes::BytesMut::new();
        for byte in [8_u8, 9_u8] {
            let body = [byte; 75];
            let record_size = 4 + 4 + 4 + 4 + 4 + body.len() + 2;
            batch_body.put_i32(record_size as i32);
            batch_body.put_i32(0);
            batch_body.put_i32(crc32(&body) as i32);
            batch_body.put_i32(0);
            batch_body.put_i32(body.len() as i32);
            batch_body.put_slice(&body);
            batch_body.put_i16(0);
        }

        let mut inner = MessageExtBrokerInner::default();
        inner.with_version(MessageVersion::V1);
        inner.set_topic(CheetahString::from_static_str("af0b"));
        inner.message_ext_inner.set_queue_id(0);
        inner.set_body(batch_body.freeze());
        inner.message_ext_inner.set_store_timestamp(1234);
        let mut batch = MessageExtBatch {
            message_ext_broker_inner: inner,
            is_inner_batch: false,
            encoded_buff: None,
        };
        let mut context = PutMessageContext::new("af0b-0".to_string());
        let mut encoder = MessageExtEncoder::new(config);
        batch.encoded_buff = encoder.encode_batch(&batch, &mut context);
        assert_eq!(batch.encoded_buff.as_ref().unwrap().len(), 340);
        (batch, context)
    }

    #[test]
    fn blank_marker_uses_file_local_position_for_nonzero_file_offset() {
        let file_size = 128;
        let file_from_offset = file_size;
        let wrote_position = 96;
        let max_blank = file_size as i32 - wrote_position;

        let temp_dir = tempdir().expect("temp dir");
        let new_mapped_file = |file_from_offset: u64, file_size: u64, wrote_position: i32| {
            let file_path = temp_dir.path().join(offset_to_file_name(file_from_offset));
            let mapped_file = DefaultMappedFile::try_new(
                CheetahString::from_string(file_path.to_string_lossy().into_owned()),
                file_size,
            )
            .expect("mapped file");
            mapped_file.set_wrote_position(wrote_position);
            mapped_file
        };
        let mapped_file = new_mapped_file(file_from_offset, file_size, wrote_position);

        let config = Arc::new(MessageStoreConfig::default());
        let topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>> = Arc::new(DashMap::new());
        let callback = DefaultAppendMessageCallback::new(Arc::clone(&config), topic_config_table);

        let mut msg = MessageExtBrokerInner::default();
        msg.with_version(MessageVersion::V1);
        msg.set_topic(CheetahString::from_static_str("blank-marker-topic"));
        msg.set_body(Bytes::from(vec![7_u8; 64]));
        msg.message_ext_inner.set_queue_id(0);
        msg.message_ext_inner.set_store_timestamp(1234);

        let mut encoder = MessageExtEncoder::new(config);
        assert!(encoder.encode(&msg).is_none());
        msg.encoded_buff = Some(encoder.byte_buf());

        let context = PutMessageContext::new("blank-marker-topic-0".to_string());
        let result = callback.do_append(file_from_offset as i64, &mapped_file, max_blank, &mut msg, &context);

        assert_eq!(result.status, AppendMessageStatus::EndOfFile);
        assert_eq!(result.wrote_offset, file_from_offset as i64 + wrote_position as i64);
        assert_eq!(result.wrote_bytes, max_blank);

        let mut marker = mapped_file
            .get_bytes(wrote_position as usize, BLANK_MARKER_LENGTH)
            .expect("blank marker");
        assert_eq!(marker.get_i32(), max_blank);
        assert_eq!(
            marker.get_i32(),
            rocketmq_store_local::commit_log::record::BLANK_MAGIC_CODE
        );

        let (callback, config) = callback_and_config();
        let context = PutMessageContext::new("af0s-0".to_string());

        let standard_first = new_mapped_file(512, 512, 340);
        let standard_retry = new_mapped_file(1024, 512, 0);
        let mut standard_msg = encoded_single_message(Arc::clone(&config));
        let standard_eof = callback.do_append(512, &standard_first, 172, &mut standard_msg, &context);
        assert_eq!(standard_eof.status, AppendMessageStatus::EndOfFile);
        assert_eq!(standard_first.get_wrote_position(), 512);
        assert!(standard_msg.encoded_buff.is_some());
        let standard_result = callback.do_append(1024, &standard_retry, 512, &mut standard_msg, &context);
        assert_eq!(standard_result.status, AppendMessageStatus::PutOk);
        assert_eq!(standard_retry.get_wrote_position(), 170);

        let batch_first = new_mapped_file(1536, 512, 170);
        let batch_retry = new_mapped_file(2048, 512, 0);
        let (mut batch, mut batch_context) = encoded_batch_message(Arc::clone(&config));
        let batch_eof = callback.do_append_batch(1536, &batch_first, 342, &mut batch, &mut batch_context, false);
        assert_eq!(batch_eof.status, AppendMessageStatus::EndOfFile);
        assert_eq!(batch_first.get_wrote_position(), 512);
        assert_eq!(batch.encoded_buff.as_ref().map(|buffer| buffer.len()), Some(340));
        let batch_result = callback.do_append_batch(2048, &batch_retry, 512, &mut batch, &mut batch_context, false);
        assert_eq!(batch_result.status, AppendMessageStatus::PutOk);
        assert_eq!(batch_result.wrote_bytes, 340);
        assert_eq!(batch_result.msg_num, 2);
        assert_eq!(batch_retry.get_wrote_position(), 340);

        let lease_first = new_mapped_file(2560, 512, 340);
        let lease_retry = new_mapped_file(3072, 512, 0);
        let mut lease_msg = encoded_single_message(config);
        let lease_eof = callback.do_append(2560, &lease_first, 172, &mut lease_msg, &context);
        assert_eq!(lease_eof.status, AppendMessageStatus::EndOfFile);
        assert_eq!(lease_first.get_wrote_position(), 512);
        assert!(lease_msg.encoded_buff.is_some());
        let lease_result = callback.do_append(3072, &lease_retry, 512, &mut lease_msg, &context);
        assert_eq!(lease_result.status, AppendMessageStatus::PutOk);
        assert_eq!(lease_retry.get_wrote_position(), 170);
    }
}
