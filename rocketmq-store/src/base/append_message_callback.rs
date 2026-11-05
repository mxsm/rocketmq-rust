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
use rocketmq_store_local::commit_log::append::finalized_append::FinalizedAppend;
use rocketmq_store_local::commit_log::append::prepared_payload::PreparedPayload;
use rocketmq_store_local::commit_log::append::prepared_payload::PreparedPayloadKind;
use rocketmq_store_local::commit_log::append_frame::AppendFrameCrcPlan;
use rocketmq_store_local::commit_log::append_frame::AppendFrameKernel;
use rocketmq_store_local::commit_log::append_frame::SegmentAppendDecision;
use rocketmq_store_local::commit_log::append_frame::BLANK_MARKER_LENGTH;

use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::message_batch::MessageExtBatch;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_model::utils::crc32_utils::crc32;
use rocketmq_model::utils::message_utils;
use rocketmq_model::utils::message_utils::build_batch_message_id;
use rocketmq_protocol::common::message::message_decoder::create_crc32;
use tracing::error;

use crate::base::message_result::AppendMessageResult;
use crate::base::message_status_enum::AppendMessageStatus;
use crate::base::put_message_context::PutMessageContext;
use crate::log_file::commit_log::get_message_num;
use crate::log_file::mapped_file::MappedFile;
use rocketmq_store_local::mapped_file::MappedWriteLease;

pub struct DefaultAppendMessageCallback {
    topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
}

impl DefaultAppendMessageCallback {
    pub fn new(topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>) -> Self {
        Self { topic_config_table }
    }

    #[inline]
    pub(crate) fn message_num(&self, message: &MessageExtBrokerInner) -> i16 {
        get_message_num(&self.topic_config_table, message)
    }

    /// Appends one structurally validated immutable payload.
    ///
    /// All fallible offset calculations complete before mapped-file reservation. The reservation
    /// path then performs only rollover publication or stable-byte copy, runtime-field patching,
    /// checksum finalization, and lease commit.
    pub(crate) fn append_prepared_message<MF: MappedFile>(
        &self,
        file_from_offset: i64,
        mapped_file: &MF,
        msg_inner: &MessageExtBrokerInner,
        prepared: &PreparedPayload,
        queue_offset: i64,
        message_num: i32,
    ) -> AppendMessageResult {
        if prepared.kind() != PreparedPayloadKind::Single {
            error!("Single-message append received a batch PreparedPayload");
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        }
        self.append_prepared(
            file_from_offset,
            mapped_file,
            prepared,
            queue_offset,
            msg_inner.store_timestamp(),
            message_num,
            |wrote_offset, _| {
                let address = msg_inner.message_ext_inner.store_host;
                Arc::new(move || message_utils::build_message_id(address, wrote_offset))
            },
            |_| {},
        )
    }

    /// Appends a structurally validated immutable payload containing one or more batch frames.
    pub(crate) fn append_prepared_batch<MF: MappedFile>(
        &self,
        file_from_offset: i64,
        mapped_file: &MF,
        msg_batch: &MessageExtBatch,
        prepared: &PreparedPayload,
        put_message_context: &mut PutMessageContext,
    ) -> AppendMessageResult {
        if prepared.kind() != PreparedPayloadKind::Batch {
            error!("Batch append received a single-frame PreparedPayload");
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        }
        let queue_offset = msg_batch.message_ext_broker_inner.queue_offset();
        let address = msg_batch.message_ext_broker_inner.store_host();
        let store_host_length =
            if msg_batch.message_ext_broker_inner.sys_flag() & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG == 0 {
                4 + 4
            } else {
                16 + 4
            };
        self.append_prepared(
            file_from_offset,
            mapped_file,
            prepared,
            queue_offset,
            msg_batch.message_ext_broker_inner.store_timestamp(),
            i32::from(prepared.message_count()),
            move |_, physical_offsets| {
                let physical_offsets = physical_offsets.to_vec();
                Arc::new(move || {
                    build_batch_message_id(address, store_host_length, physical_offsets.len(), &physical_offsets)
                })
            },
            |physical_offsets| {
                put_message_context.set_batch_size(physical_offsets.len() as i32);
                put_message_context.set_phy_pos(physical_offsets.to_vec());
            },
        )
    }

    fn append_prepared<MF, I, C>(
        &self,
        file_from_offset: i64,
        mapped_file: &MF,
        prepared: &PreparedPayload,
        queue_offset: i64,
        store_timestamp: i64,
        message_num: i32,
        message_id_supplier: I,
        mut publish_physical_offsets: C,
    ) -> AppendMessageResult
    where
        MF: MappedFile,
        I: FnOnce(i64, &[i64]) -> Arc<dyn Fn() -> String + Send + Sync>,
        C: FnMut(&[i64]),
    {
        let encoded_len = prepared.retained_bytes();
        let Ok(encoded_len_i32) = i32::try_from(encoded_len) else {
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        };
        let current_position = mapped_file.get_wrote_position().max(0) as usize;
        let remaining = mapped_file.get_file_size().saturating_sub(current_position as u64) as usize;
        let max_blank = i32::try_from(remaining).unwrap_or(i32::MAX);
        let Some(wrote_offset) = file_from_offset.checked_add(current_position as i64) else {
            error!("CommitLog physical offset overflow before mapped-file reservation");
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        };
        let Some(reservation_size) = encoded_len.checked_add(BLANK_MARKER_LENGTH) else {
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        };

        if AppendFrameKernel::segment_append_decision(encoded_len_i32, max_blank) == SegmentAppendDecision::Roll {
            let mut lease = match mapped_file.reserve_write(reservation_size) {
                Ok(Some(lease)) => lease,
                Ok(None) => {
                    return AppendMessageResult {
                        status: AppendMessageStatus::UnknownError,
                        ..Default::default()
                    };
                }
                Err(error) => {
                    error!(%error, "Failed to reserve mapped-file rollover marker");
                    return AppendMessageResult {
                        status: AppendMessageStatus::UnknownError,
                        ..Default::default()
                    };
                }
            };
            let marker = AppendFrameKernel::blank_marker(max_blank);
            let started = Instant::now();
            if lease.capacity() >= BLANK_MARKER_LENGTH {
                lease.buffer_mut()[..BLANK_MARKER_LENGTH].copy_from_slice(marker.bytes());
            }
            let committed_bytes = lease.capacity();
            match lease.commit(committed_bytes, Some(store_timestamp as u64)) {
                Ok(Some(_)) => {}
                Ok(None) => {
                    return AppendMessageResult {
                        status: AppendMessageStatus::UnknownError,
                        ..Default::default()
                    };
                }
                Err(error) => {
                    error!(%error, "Failed to publish mapped-file rollover marker");
                    return AppendMessageResult {
                        status: AppendMessageStatus::UnknownError,
                        ..Default::default()
                    };
                }
            }
            return AppendMessageResult {
                status: AppendMessageStatus::EndOfFile,
                wrote_offset,
                wrote_bytes: marker.declared_wrote_bytes(),
                store_timestamp,
                logics_offset: queue_offset,
                msg_num: message_num,
                msg_id_supplier: Some(message_id_supplier(wrote_offset, &[])),
                page_cache_rt: started.elapsed().as_millis() as i64,
                ..Default::default()
            };
        }

        let finalized = match FinalizedAppend::try_new(prepared, queue_offset, wrote_offset, store_timestamp) {
            Some(finalized) => finalized,
            None => {
                error!("Failed to finalize PreparedPayload before mapped-file reservation");
                return AppendMessageResult {
                    status: AppendMessageStatus::UnknownError,
                    ..Default::default()
                };
            }
        };
        let physical_offsets = PhysicalOffsets::collect(finalized.physical_offsets());
        let mut lease = match mapped_file.reserve_write(reservation_size) {
            Ok(Some(lease)) => lease,
            Ok(None) => {
                return AppendMessageResult {
                    status: AppendMessageStatus::UnknownError,
                    ..Default::default()
                };
            }
            Err(error) => {
                error!(%error, "Failed to reserve mapped-file prepared append");
                return AppendMessageResult {
                    status: AppendMessageStatus::UnknownError,
                    ..Default::default()
                };
            }
        };
        if lease.start_position() != current_position {
            error!(
                expected = current_position,
                actual = lease.start_position(),
                "Mapped-file position changed between append finalization and reservation"
            );
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        }
        let started = Instant::now();
        let write_result = finalized.write_into(&mut lease.buffer_mut()[..encoded_len], |frame, crc_plan| {
            if let AppendFrameCrcPlan::Trailer {
                covered_end,
                trailer_start,
                trailer_end,
            } = crc_plan
            {
                let checksum = crc32(&frame[..covered_end]);
                create_crc32(&mut frame[trailer_start..trailer_end], checksum);
            }
        });
        if write_result.is_none() {
            error!("Prepared append staging buffer rejected a preflighted payload");
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        }
        match lease.commit(encoded_len, Some(store_timestamp as u64)) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return AppendMessageResult {
                    status: AppendMessageStatus::UnknownError,
                    ..Default::default()
                };
            }
            Err(error) => {
                error!(%error, "Failed to commit mapped-file prepared append");
                return AppendMessageResult {
                    status: AppendMessageStatus::UnknownError,
                    ..Default::default()
                };
            }
        }
        publish_physical_offsets(physical_offsets.as_slice());
        AppendMessageResult {
            status: AppendMessageStatus::PutOk,
            wrote_offset,
            wrote_bytes: encoded_len_i32,
            store_timestamp,
            logics_offset: queue_offset,
            msg_num: message_num,
            msg_id_supplier: Some(message_id_supplier(wrote_offset, physical_offsets.as_slice())),
            page_cache_rt: started.elapsed().as_millis() as i64,
            ..Default::default()
        }
    }
}

/// Physical offsets of the frames one append wrote. A single message, the common case, keeps its
/// offset inline instead of allocating while the CommitLog writer lock is held.
enum PhysicalOffsets {
    One([i64; 1]),
    Many(Vec<i64>),
}

impl PhysicalOffsets {
    fn collect(mut offsets: impl ExactSizeIterator<Item = i64>) -> Self {
        if offsets.len() == 1 {
            if let Some(offset) = offsets.next() {
                return Self::One([offset]);
            }
        }
        Self::Many(offsets.collect())
    }

    fn as_slice(&self) -> &[i64] {
        match self {
            Self::One(offset) => offset,
            Self::Many(offsets) => offsets,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::utils::store_util::offset_to_file_name;
    use bytes::Buf;
    use bytes::BufMut;
    use bytes::Bytes;
    use rocketmq_model::common::message::MessageTrait;
    use rocketmq_model::common::message::MessageVersion;
    use tempfile::tempdir;

    use crate::config::message_store_config::MessageStoreConfig;
    use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
    use crate::message_encoder::message_ext_encoder::MessageExtEncoder;

    fn callback_and_config() -> (DefaultAppendMessageCallback, Arc<MessageStoreConfig>) {
        let config = Arc::new(MessageStoreConfig::default());
        let callback = DefaultAppendMessageCallback::new(Arc::new(DashMap::new()));
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
        let callback = DefaultAppendMessageCallback::new(topic_config_table);

        let mut msg = MessageExtBrokerInner::default();
        msg.with_version(MessageVersion::V1);
        msg.set_topic(CheetahString::from_static_str("blank-marker-topic"));
        msg.set_body(Bytes::from(vec![7_u8; 64]));
        msg.message_ext_inner.set_queue_id(0);
        msg.message_ext_inner.set_store_timestamp(1234);

        let mut encoder = MessageExtEncoder::new(config);
        assert!(encoder.encode(&msg).is_none());
        msg.encoded_buff = Some(encoder.byte_buf());

        let prepared = PreparedPayload::try_single(msg.encoded_buff.take().unwrap().freeze(), 0).unwrap();
        let result = callback.append_prepared_message(file_from_offset as i64, &mapped_file, &msg, &prepared, 0, 1);

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

        let standard_first = new_mapped_file(512, 512, 340);
        let standard_retry = new_mapped_file(1024, 512, 0);
        let mut standard_msg = encoded_single_message(Arc::clone(&config));
        let prepared = PreparedPayload::try_single(standard_msg.encoded_buff.take().unwrap().freeze(), 0).unwrap();
        let standard_eof = callback.append_prepared_message(512, &standard_first, &standard_msg, &prepared, 0, 1);
        assert_eq!(standard_eof.status, AppendMessageStatus::EndOfFile);
        assert_eq!(standard_first.get_wrote_position(), 512);
        let standard_result = callback.append_prepared_message(1024, &standard_retry, &standard_msg, &prepared, 0, 1);
        assert_eq!(standard_result.status, AppendMessageStatus::PutOk);
        assert_eq!(standard_retry.get_wrote_position(), 170);

        let batch_first = new_mapped_file(1536, 512, 170);
        let batch_retry = new_mapped_file(2048, 512, 0);
        let (mut batch, mut batch_context) = encoded_batch_message(Arc::clone(&config));
        let prepared = PreparedPayload::try_batch(batch.encoded_buff.take().unwrap().freeze(), 0).unwrap();
        let batch_eof = callback.append_prepared_batch(1536, &batch_first, &batch, &prepared, &mut batch_context);
        assert_eq!(batch_eof.status, AppendMessageStatus::EndOfFile);
        assert_eq!(batch_first.get_wrote_position(), 512);
        assert_eq!(prepared.retained_bytes(), 340);
        let batch_result = callback.append_prepared_batch(2048, &batch_retry, &batch, &prepared, &mut batch_context);
        assert_eq!(batch_result.status, AppendMessageStatus::PutOk);
        assert_eq!(batch_result.wrote_bytes, 340);
        assert_eq!(batch_result.msg_num, 2);
        assert_eq!(batch_retry.get_wrote_position(), 340);
    }
}
