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

use bytes::Buf;
use bytes::BufMut;
use bytes::BytesMut;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageVersion;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::CRC32Utils::crc32;
use rocketmq_common::MessageDecoder;
use rocketmq_common::MessageDecoder::PROPERTY_SEPARATOR;
use tracing::warn;

use crate::base::message_result::PutMessageResult;
use crate::base::message_status_enum::PutMessageStatus;
use crate::base::put_message_context::PutMessageContext;
use crate::config::message_store_config::MessageStoreConfig;
use crate::log_file::commit_log::CommitLog;
use crate::log_file::commit_log::CRC32_RESERVED_LEN;

pub struct MessageExtEncoder {
    byte_buf: BytesMut,
    max_message_body_size: i32,
    max_message_size: i32,
    crc32_reserved_length: i32,
    message_store_config: Arc<MessageStoreConfig>,
}

impl MessageExtEncoder {
    pub fn new(message_store_config: Arc<MessageStoreConfig>) -> MessageExtEncoder {
        let max_message_body_size = message_store_config.max_message_size;
        let max_message_size = if i32::MAX - max_message_body_size >= 64 * 1024 {
            max_message_body_size + 64 * 1024
        } else {
            i32::MAX
        };
        let crc32_reserved_length = if message_store_config.enabled_append_prop_crc {
            CRC32_RESERVED_LEN
        } else {
            0
        };
        MessageExtEncoder {
            byte_buf: BytesMut::with_capacity(max_message_size as usize),
            max_message_body_size,
            max_message_size,
            crc32_reserved_length,
            message_store_config,
        }
    }

    pub fn cal_msg_length(
        message_version: MessageVersion,
        sys_flag: i32,
        body_length: i32,
        topic_length: i32,
        properties_length: i32,
    ) -> i32 {
        let bornhost_length = if (sys_flag & MessageSysFlag::BORNHOST_V6_FLAG) == 0 {
            8
        } else {
            20
        };
        let storehost_address_length = if (sys_flag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG) == 0 {
            8
        } else {
            20
        };

        4 + // TOTALSIZE
             4 + // MAGICCODE
             4 + // BODYCRC
             4 + // QUEUEID
             4 + // FLAG
             8 + // QUEUEOFFSET
             8 + // PHYSICALOFFSET
             4 + // SYSFLAG
             8 + // BORNTIMESTAMP
             bornhost_length + // BORNHOST
             8 + // STORETIMESTAMP
             storehost_address_length + // STOREHOSTADDRESS
             4 + // RECONSUMETIMES
             8 + // Prepared Transaction Offset
             4 + (body_length.max(0)) + // BODY
             (message_version.get_topic_length_size() as i32) + topic_length + // TOPIC
             2 + (properties_length.max(0)) // propertiesLength
    }

    pub fn cal_msg_length_no_properties(
        message_version: MessageVersion,
        sys_flag: i32,
        body_length: i32,
        topic_length: i32,
    ) -> i32 {
        let bornhost_length = if (sys_flag & MessageSysFlag::BORNHOST_V6_FLAG) == 0 {
            8
        } else {
            20
        };
        let storehost_address_length = if (sys_flag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG) == 0 {
            8
        } else {
            20
        };

        4 + // TOTALSIZE
             4 + // MAGICCODE
             4 + // BODYCRC
             4 + // QUEUEID
             4 + // FLAG
             8 + // QUEUEOFFSET
             8 + // PHYSICALOFFSET
             4 + // SYSFLAG
             8 + // BORNTIMESTAMP
             bornhost_length + // BORNHOST
             8 + // STORETIMESTAMP
             storehost_address_length + // STOREHOSTADDRESS
             4 + // RECONSUMETIMES
             8 + // Prepared Transaction Offset
             4 + (body_length.max(0)) + // BODY
             (message_version.get_topic_length_size() as i32) + topic_length // TOPIC
    }

    pub fn encode_without_properties(&mut self, msg_inner: &MessageExtBrokerInner) -> Option<PutMessageResult> {
        let topic_data = msg_inner.topic().as_bytes();
        let topic_length = topic_data.len();

        let body_length = msg_inner.body().map_or(0, |body| body.len());

        if body_length > self.max_message_body_size as usize {
            warn!(
                "message body size exceeded, msg body size: {}, maxMessageSize: {}",
                body_length, self.max_message_body_size
            );
            return Some(PutMessageResult::new_default(PutMessageStatus::MessageIllegal));
        }

        let msg_len_no_properties = Self::cal_msg_length_no_properties(
            msg_inner.version(),
            msg_inner.sys_flag(),
            body_length as i32,
            topic_length as i32,
        );

        // 1 TOTALSIZE
        self.byte_buf.put_i32(msg_len_no_properties);

        // 2 MAGICCODE
        self.byte_buf.put_i32(msg_inner.version().get_magic_code());

        // 3 BODYCRC
        self.byte_buf.put_u32(msg_inner.body_crc());

        // 4 QUEUEID
        self.byte_buf.put_i32(msg_inner.queue_id());

        // 5 FLAG
        self.byte_buf.put_i32(msg_inner.flag());

        // 6 QUEUEOFFSET, need update later
        self.byte_buf.put_i64(0);

        // 7 PHYSICALOFFSET, need update later
        self.byte_buf.put_i64(0);

        // 8 SYSFLAG
        self.byte_buf.put_i32(msg_inner.sys_flag());

        // 9 BORNTIMESTAMP
        self.byte_buf.put_i64(msg_inner.born_timestamp());

        // 10 BORNHOST
        self.byte_buf.put(msg_inner.born_host_bytes());

        // 11 STORETIMESTAMP
        self.byte_buf.put_i64(msg_inner.store_timestamp());

        // 12 STOREHOSTADDRESS
        self.byte_buf.put(msg_inner.store_host_bytes());

        // 13 RECONSUMETIMES
        self.byte_buf.put_i32(msg_inner.reconsume_times());

        // 14 Prepared Transaction Offset
        self.byte_buf.put_i64(msg_inner.prepared_transaction_offset());

        // 15 BODY
        self.byte_buf.put_i32(body_length as i32);
        if let Some(body) = msg_inner.body() {
            self.byte_buf.put(body);
        }

        // 16 TOPIC
        if MessageVersion::V2 == msg_inner.version() {
            self.byte_buf.put_u16(topic_length as u16);
        } else {
            self.byte_buf.put_u8(topic_length as u8);
        }
        self.byte_buf.put_slice(topic_data);

        None
    }

    pub fn encode(&mut self, msg_inner: &MessageExtBrokerInner) -> Option<PutMessageResult> {
        self.byte_buf.clear();

        if self.message_store_config.enable_multi_dispatch && CommitLog::is_multi_dispatch_msg(msg_inner) {
            return self.encode_without_properties(msg_inner);
        }

        // Serialize message
        let properties_data = msg_inner.properties_string().as_bytes();
        let need_append_last_property_separator = self.crc32_reserved_length > 0
            && !properties_data.is_empty()
            && properties_data[properties_data.len() - 1..][0] != PROPERTY_SEPARATOR as u8;

        let properties_length = properties_data.len()
            + if need_append_last_property_separator { 1 } else { 0 }
            + self.crc32_reserved_length as usize;

        if properties_length > i16::MAX as usize {
            warn!(
                "putMessage message properties length too long. length={}",
                properties_length
            );
            return Some(PutMessageResult::new(
                PutMessageStatus::PropertiesSizeExceeded,
                None,
                false,
            ));
        }

        let topic_data = msg_inner.topic().as_bytes();
        let topic_length = topic_data.len();

        let body_length = msg_inner.body().map_or(0, |body| body.len());
        let msg_len = Self::cal_msg_length(
            msg_inner.version(),
            msg_inner.sys_flag(),
            body_length as i32,
            topic_length as i32,
            properties_length as i32,
        );

        // Exceeds the maximum message body
        if body_length > self.max_message_body_size as usize {
            warn!(
                "message body size exceeded, msg total size: {}, msg body size: {}, maxMessageSize: {}",
                msg_len, body_length, self.max_message_body_size
            );
            return Some(PutMessageResult::new(PutMessageStatus::MessageIllegal, None, false));
        }

        let queue_offset = msg_inner.queue_offset();

        // Exceeds the maximum message
        if msg_len > self.max_message_size {
            warn!(
                "message size exceeded, msg total size: {}, msg body size: {}, maxMessageSize: {}",
                msg_len, body_length, self.max_message_size
            );
            return Some(PutMessageResult::new(PutMessageStatus::MessageIllegal, None, false));
        }

        // 1 TOTALSIZE
        self.byte_buf.put_i32(msg_len);

        // 2 MAGICCODE
        self.byte_buf.put_i32(msg_inner.version().get_magic_code());

        // 3 BODYCRC
        self.byte_buf.put_u32(msg_inner.body_crc());

        // 4 QUEUEID
        self.byte_buf.put_i32(msg_inner.queue_id());

        // 5 FLAG
        self.byte_buf.put_i32(msg_inner.flag());

        // 6 QUEUEOFFSET
        self.byte_buf.put_i64(queue_offset);

        // 7 PHYSICALOFFSET, need update later
        self.byte_buf.put_i64(0);

        // 8 SYSFLAG
        self.byte_buf.put_i32(msg_inner.sys_flag());

        // 9 BORNTIMESTAMP
        self.byte_buf.put_i64(msg_inner.born_timestamp());

        // 10 BORNHOST
        self.byte_buf.put(msg_inner.born_host_bytes());

        // 11 STORETIMESTAMP
        self.byte_buf.put_i64(msg_inner.store_timestamp());

        // 12 STOREHOSTADDRESS
        self.byte_buf.put(msg_inner.store_host_bytes());

        // 13 RECONSUMETIMES
        self.byte_buf.put_i32(msg_inner.reconsume_times());

        // 14 Prepared Transaction Offset
        self.byte_buf.put_i64(msg_inner.prepared_transaction_offset());

        // 15 BODY
        self.byte_buf.put_i32(body_length as i32);
        if let Some(body) = msg_inner.body() {
            if body_length > 0 {
                self.byte_buf.put(body);
            }
        }

        // 16 TOPIC
        if MessageVersion::V2 == msg_inner.version() {
            self.byte_buf.put_u16(topic_length as u16);
        } else {
            self.byte_buf.put_u8(topic_length as u8);
        }
        self.byte_buf.put_slice(topic_data);

        // 17 PROPERTIES
        self.byte_buf.put_u16(properties_length as u16);
        if properties_length > self.crc32_reserved_length as usize {
            self.byte_buf.put(properties_data);
        }
        if need_append_last_property_separator {
            self.byte_buf.put_u8(MessageDecoder::PROPERTY_SEPARATOR as u8);
        }
        // 18 CRC32
        self.byte_buf.advance(self.crc32_reserved_length as usize);
        None
    }

    pub fn encode_batch(
        &mut self,
        message_ext_batch: &MessageExtBatch,
        put_message_context: &mut PutMessageContext,
    ) -> Option<BytesMut> {
        self.byte_buf.clear();

        let messages_byte_buff = message_ext_batch.wrap();
        let mut messages_byte_buff = messages_byte_buff?;
        let total_length = messages_byte_buff.len();
        if total_length > self.max_message_body_size as usize {
            warn!(
                "message body size exceeded, msg body size: {}, maxMessageSize: {}",
                total_length, self.max_message_body_size
            );
            return None;
        }

        let batch_prop_str = MessageDecoder::message_properties_to_string(
            message_ext_batch
                .message_ext_broker_inner
                .message_ext_inner
                .properties(),
        );
        let batch_prop_data = batch_prop_str.as_bytes();
        let batch_prop_data_len = batch_prop_data.len();
        if batch_prop_data_len > i16::MAX as usize {
            warn!(
                "Properties size of messageExtBatch exceeded, properties size: {}, maxSize: {}",
                batch_prop_data_len,
                i16::MAX
            );
            return None;
        }
        let batch_prop_len = batch_prop_data_len as i16;

        let mut batch_size = 0;
        while messages_byte_buff.has_remaining() {
            batch_size += 1;
            let total_size = messages_byte_buff.get_i32();
            let magic_code = messages_byte_buff.get_i32();
            let body_crc = messages_byte_buff.get_i32();
            let flag = messages_byte_buff.get_i32();
            let body_len = messages_byte_buff.get_i32();
            let body = messages_byte_buff.copy_to_bytes(body_len as usize);
            let body_crc_calculated = crc32(body.as_ref());
            let properties_len = messages_byte_buff.get_i16();
            let properties_body = messages_byte_buff.copy_to_bytes(properties_len as usize);
            let current = total_length - messages_byte_buff.remaining();
            let need_append_last_property_separator = properties_len > 0
                && batch_prop_len > 0
                && properties_body.as_ref()[(properties_len - 1) as usize..][0] != PROPERTY_SEPARATOR as u8;
            let topic_data = message_ext_batch.message_ext_broker_inner.topic().as_bytes();
            let topic_length = topic_data.len() as i32;
            let mut total_prop_len = if need_append_last_property_separator {
                properties_len + batch_prop_len + 1
            } else {
                properties_len + batch_prop_len
            };

            // Properties need to add crc32
            total_prop_len += self.crc32_reserved_length as i16;
            let msg_len = Self::cal_msg_length(
                message_ext_batch.message_ext_broker_inner.version(),
                message_ext_batch.message_ext_broker_inner.sys_flag(),
                body_len,
                topic_length,
                total_prop_len as i32,
            );

            // 1 TOTALSIZE
            self.byte_buf.put_i32(msg_len);
            // 2 MAGICCODE
            self.byte_buf
                .put_i32(message_ext_batch.message_ext_broker_inner.version().get_magic_code());
            // 3 BODYCRC
            self.byte_buf.put_i32(body_crc);
            // 4 QUEUEID
            self.byte_buf
                .put_i32(message_ext_batch.message_ext_broker_inner.queue_id());
            // 5 FLAG
            self.byte_buf.put_i32(flag);
            // 6 QUEUEOFFSET
            self.byte_buf.put_i64(0);
            // 7 PHYSICALOFFSET
            self.byte_buf.put_i64(0);
            // 8 SYSFLAG
            self.byte_buf
                .put_i32(message_ext_batch.message_ext_broker_inner.sys_flag());
            // 9 BORNTIMESTAMP
            self.byte_buf
                .put_i64(message_ext_batch.message_ext_broker_inner.born_timestamp());
            // 10 BORNHOST
            self.byte_buf
                .put(message_ext_batch.message_ext_broker_inner.born_host_bytes());
            // 11 STORETIMESTAMP
            self.byte_buf
                .put_i64(message_ext_batch.message_ext_broker_inner.store_timestamp());
            // 12 STOREHOSTADDRESS
            self.byte_buf
                .put(message_ext_batch.message_ext_broker_inner.store_host_bytes());
            // 13 RECONSUMETIMES
            self.byte_buf
                .put_i32(message_ext_batch.message_ext_broker_inner.reconsume_times());
            // Prepared Transaction Offset, batch does not support transaction
            self.byte_buf.put_i64(0);
            // 15 BODY
            self.byte_buf.put_i32(body_len);
            if body_len > 0 {
                self.byte_buf.put(body);
            }
            // 16 TOPIC
            if message_ext_batch.message_ext_broker_inner.version() == MessageVersion::V2 {
                self.byte_buf.put_i16(topic_length as i16);
            } else {
                self.byte_buf.put_u8(topic_length as u8);
            }
            self.byte_buf.put(topic_data);

            // 17 PROPERTIES
            self.byte_buf.put_i16(total_prop_len);
            if properties_len > 0 {
                self.byte_buf.put(properties_body);
            }
            if batch_prop_len > 0 {
                if need_append_last_property_separator {
                    self.byte_buf.put_u8(MessageDecoder::PROPERTY_SEPARATOR as u8);
                }
                self.byte_buf.put_slice(batch_prop_data);
            }
            // 18 CRC32
            self.byte_buf.advance(self.crc32_reserved_length as usize);
        }
        put_message_context.set_batch_size(batch_size);
        put_message_context.set_phy_pos(vec![0; batch_size as usize]);

        Some(self.byte_buf.split())
    }

    pub fn get_encoder_buffer(&mut self) -> bytes::Bytes {
        let len = self.byte_buf.len();
        self.byte_buf.copy_to_bytes(len)
    }

    pub fn get_max_message_body_size(&self) -> i32 {
        self.max_message_body_size
    }

    pub fn update_encoder_buffer_capacity(&mut self, new_max_message_body_size: i32) {
        self.max_message_body_size = new_max_message_body_size;
        self.max_message_size = if i32::MAX - new_max_message_body_size >= 64 * 1024 {
            new_max_message_body_size + 64 * 1024
        } else {
            i32::MAX
        };
        self.byte_buf.resize(self.max_message_size as usize, 0);
    }

    pub fn byte_buf(&mut self) -> BytesMut {
        self.byte_buf.split()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn message_ext_encoder_new_creates_encoder_with_correct_config() {
        let config = Arc::new(MessageStoreConfig::default());
        let encoder = MessageExtEncoder::new(Arc::clone(&config));

        assert_eq!(encoder.max_message_body_size, config.max_message_size);
        assert_eq!(encoder.message_store_config, config);
    }

    #[test]
    fn cal_msg_length_calculates_correct_length() {
        let length = MessageExtEncoder::cal_msg_length(MessageVersion::V1, 0, 10, 5, 15);
        assert_eq!(length, 121);
        let length = MessageExtEncoder::cal_msg_length(MessageVersion::V1, MessageSysFlag::BORNHOST_V6_FLAG, 10, 5, 15);
        assert_eq!(length, 133);
        let length = MessageExtEncoder::cal_msg_length(MessageVersion::V2, 0, 10, 5, 15);
        assert_eq!(length, 122);
    }

    #[test]
    fn cal_msg_length_no_properties_calculates_correct_length() {
        let length = MessageExtEncoder::cal_msg_length_no_properties(MessageVersion::V1, 0, 10, 5);
        assert_eq!(length, 104);
        let length = MessageExtEncoder::cal_msg_length_no_properties(
            MessageVersion::V1,
            MessageSysFlag::BORNHOST_V6_FLAG,
            10,
            5,
        );
        assert_eq!(length, 116);
        let length = MessageExtEncoder::cal_msg_length_no_properties(MessageVersion::V2, 0, 10, 5);
        assert_eq!(length, 105);
    }

    #[test]
    fn encode_without_properties_encodes_message_correctly() {
        let config = Arc::new(MessageStoreConfig::default());
        let mut encoder = MessageExtEncoder::new(Arc::clone(&config));
        let msg_inner = MessageExtBrokerInner::default();

        let result = encoder.encode_without_properties(&msg_inner);

        assert!(result.is_none());
    }

    #[test]
    fn encode_encodes_message_correctly() {
        let config = Arc::new(MessageStoreConfig::default());
        let mut encoder = MessageExtEncoder::new(Arc::clone(&config));
        let msg_inner = MessageExtBrokerInner::default();

        let result = encoder.encode(&msg_inner);

        assert!(result.is_none());
    }

    #[test]
    fn get_encoder_buffer_returns_correct_buffer() {
        let config = Arc::new(MessageStoreConfig::default());
        let mut encoder = MessageExtEncoder::new(Arc::clone(&config));

        let buffer = encoder.get_encoder_buffer();

        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn get_max_message_body_size_returns_correct_size() {
        let config = Arc::new(MessageStoreConfig::default());
        let encoder = MessageExtEncoder::new(Arc::clone(&config));

        let size = encoder.get_max_message_body_size();

        assert_eq!(size, config.max_message_size);
    }

    #[test]
    fn update_encoder_buffer_capacity_updates_capacity_correctly() {
        let config = Arc::new(MessageStoreConfig::default());
        let mut encoder = MessageExtEncoder::new(Arc::clone(&config));

        encoder.update_encoder_buffer_capacity(200);

        assert_eq!(encoder.max_message_body_size, 200);
    }
}
