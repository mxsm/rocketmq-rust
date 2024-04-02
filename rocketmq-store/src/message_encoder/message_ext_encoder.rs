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

use bytes::{Buf, BufMut};
use rocketmq_common::{
    common::{
        message::{
            message_batch::MessageExtBatch, message_single::MessageExtBrokerInner, MessageVersion,
        },
        sys_flag::message_sys_flag::MessageSysFlag,
    },
    MessageDecoder,
};
use tracing::{info, warn};

use crate::{
    base::{
        message_result::PutMessageResult, message_status_enum::PutMessageStatus,
        put_message_context::PutMessageContext,
    },
    config::message_store_config::MessageStoreConfig,
    log_file::commit_log::{CommitLog, CRC32_RESERVED_LEN},
};

pub struct MessageExtEncoder {
    byte_buf: Option<bytes::BytesMut>,
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
            byte_buf: Some(bytes::BytesMut::with_capacity(max_message_size as usize)),
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
        let storehost_address_length = if (sys_flag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG) == 0
        {
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
        let storehost_address_length = if (sys_flag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG) == 0
        {
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

    pub fn encode_without_properties(
        &mut self,
        msg_inner: &MessageExtBrokerInner,
    ) -> Option<PutMessageResult> {
        let topic_data = msg_inner.topic().as_bytes();
        let topic_length = topic_data.len();

        let body_length = msg_inner.body().map_or(0, |body| body.len());

        if body_length > self.max_message_body_size as usize {
            warn!(
                "message body size exceeded, msg body size: {}, maxMessageSize: {}",
                body_length, self.max_message_body_size
            );
            return Some(PutMessageResult::new_default(
                PutMessageStatus::MessageIllegal,
            ));
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
        self.byte_buf
            .put_i64(msg_inner.prepared_transaction_offset());

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

        if self.message_store_config.enable_multi_dispatch
            && CommitLog::is_multi_dispatch_msg(msg_inner)
        {
            return self.encode_without_properties(msg_inner);
        }

        // Serialize message
        let properties_data = msg_inner.properties_string().as_bytes();
        let need_append_last_property_separator = self.crc32_reserved_length > 0
            && !properties_data.is_empty()
            && properties_data[properties_data.len() - 1..][0] == 2u8;

        let properties_length = properties_data.len()
            + if need_append_last_property_separator {
                1
            } else {
                0
            }
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
                "message body size exceeded, msg total size: {}, msg body size: {}, \
                 maxMessageSize: {}",
                msg_len, body_length, self.max_message_body_size
            );
            return Some(PutMessageResult::new(
                PutMessageStatus::MessageIllegal,
                None,
                false,
            ));
        }

        let queue_offset = msg_inner.queue_offset();

        // Exceeds the maximum message
        if msg_len > self.max_message_size {
            warn!(
                "message size exceeded, msg total size: {}, msg body size: {}, maxMessageSize: {}",
                msg_len, body_length, self.max_message_size
            );
            return Some(PutMessageResult::new(
                PutMessageStatus::MessageIllegal,
                None,
                false,
            ));
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
        self.byte_buf
            .put_i64(msg_inner.prepared_transaction_offset());

        // 15 BODY
        self.byte_buf.put_i32(body_length.try_into().unwrap());
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
        self.byte_buf.put(topic_data);

        // 17 PROPERTIES
        self.byte_buf.put_u16(properties_length as u16);
        if properties_length > self.crc32_reserved_length as usize {
            self.byte_buf.put_slice(properties_data);
        }
        if need_append_last_property_separator {
            self.byte_buf
                .put_u8(MessageDecoder::PROPERTY_SEPARATOR as u32 as u8);
        }

        // 18 CRC32

        None
    }

    pub fn encode_batch(
        &mut self,
        _message_ext_batch: &MessageExtBatch,
        _put_message_context: &mut PutMessageContext,
    ) -> Option<bytes::Bytes> {
        /*self.byte_buf.clear();

        let messages_byte_buff = message_ext_batch.wrap();
        if messages_byte_buff.is_none() {
            return None;
        }
        let mut messages_byte_buff = messages_byte_buff.unwrap();
        let total_length = messages_byte_buff.len() as i32;
        if total_length > self.max_message_body_size {
            warn!(
                "message body size exceeded, msg body size: {}, maxMessageSize: {}",
                total_length, self.max_message_body_size
            );
            return None;
        }

        let batch_prop_str =
            MessageDecoder::message_properties_to_string(message_ext_batch.getProperties());
        let batch_prop_data = batch_prop_str.as_bytes();
        let batch_prop_data_len = batch_prop_data.len();
        if batch_prop_data_len > i16::MAX_VALUE as usize {
            warn!(
                "Properties size of messageExtBatch exceeded, properties size: {}, maxSize: {}",
                batch_prop_data_len,
                i16::MAX_VALUE
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
            let body_crc_calculated = crc32(messages_byte_buff.copy_to_bytes(body_len).as_ref());
            messages_byte_buff.set_position((body_pos + body_len) as u64);
            let properties_len = messages_byte_buff.get_i16();
            let properties_pos = messages_byte_buff.position() as usize;
            messages_byte_buff.set_position((properties_pos + properties_len) as u64);
            let need_append_last_property_separator = properties_len > 0
                && batch_prop_len > 0
                && messages_byte_buff.get((properties_pos + properties_len - 1) as usize)
                    != MessageDecoder::PROPERTY_SEPARATOR;

            let topic_data = message_ext_batch.get_topic().as_bytes();
            let topic_length = topic_data.len() as i32;
            let total_prop_len = if need_append_last_property_separator {
                properties_len + batch_prop_len as i32 + 1
            } else {
                properties_len + batch_prop_len as i32
            };

            // Properties need to add crc32
            let total_prop_len_with_crc32 = total_prop_len + self.crc32_reserved_length;
            let msg_len = Self::cal_msg_length(
                message_ext_batch.get_version(),
                message_ext_batch.get_sys_flag(),
                body_len,
                topic_length,
                total_prop_len_with_crc32,
            );

            self.byte_buf.put_i32(msg_len);
            self.byte_buf.put_i32(magic_code);
            self.byte_buf.put_i32(body_crc);
            self.byte_buf.put_i32(message_ext_batch.get_queue_id());
            self.byte_buf.put_i32(flag);
            self.byte_buf.put_i64(0);
            self.byte_buf.put_i64(0);
            self.byte_buf.put_i32(message_ext_batch.get_sys_flag());
            self.byte_buf
                .put_i64(message_ext_batch.get_born_timestamp());

            self.byte_buf.put(born_host_bytes.array());

            self.byte_buf
                .put_i64(message_ext_batch.get_store_timestamp());

            self.byte_buf.put(store_host_bytes.array());

            self.byte_buf
                .put_i32(message_ext_batch.get_reconsume_times());
            self.byte_buf.put_i64(0); // Prepared Transaction Offset, batch does not support transaction
            self.byte_buf.put_i32(body_len);
            if body_len > 0 {
                self.byte_buf
                    .put(&messages_byte_buff.bytes()[body_pos..(body_pos + body_len) as usize]);
            }

            if message_ext_batch.get_version() == MessageVersion::MESSAGE_VERSION_V2 {
                self.byte_buf.put_i16(topic_length as i16);
            } else {
                self.byte_buf.put_u8(topic_length as u8);
            }
            self.byte_buf.put(topic_data);

            self.byte_buf.put_i16(total_prop_len as i16);
            if properties_len > 0 {
                self.byte_buf.put(
                    &messages_byte_buff.bytes()
                        [properties_pos..(properties_pos + properties_len) as usize],
                );
            }
            if batch_prop_len > 0 {
                if need_append_last_property_separator {
                    self.byte_buf
                        .put_u8(MessageDecoder::PROPERTY_SEPARATOR as u8);
                }
                self.byte_buf.put(batch_prop_data);
            }
            self.byte_buf.set_writer_index(
                (self.byte_buf.writer_index() + self.crc32_reserved_length) as usize,
            );
        }
        put_message_context.set_batch_size(batch_size);
        put_message_context.set_phy_pos(vec![0; batch_size]);

        self.byte_buf.nio_buffer()*/
        unimplemented!()
    }

    pub fn get_encoder_buffer(&mut self) -> bytes::Bytes {
        self.byte_buf.copy_to_bytes(self.byte_buf.len())
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
    pub fn byte_buf(&mut self) -> bytes::Bytes {
        let bytes = self.byte_buf.copy_to_bytes(self.byte_buf.len());
        self.byte_buf.clear();
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_encoder_buffer() {
        let mut obj = MessageExtEncoder::new(Arc::new(MessageStoreConfig {
            max_message_size: 268435456,
            ..MessageStoreConfig::default()
        }));
        let result = obj.get_encoder_buffer();
        assert_eq!(result.len(), obj.byte_buf.len());
        for i in 0..result.len() {
            assert_eq!(result[i], obj.byte_buf[i]);
        }
    }

    #[test]
    fn test_get_max_message_body_size() {
        let obj = MessageExtEncoder::new(Arc::new(MessageStoreConfig {
            max_message_size: 100,
            ..MessageStoreConfig::default()
        }));
        assert_eq!(obj.get_max_message_body_size(), 100);
    }

    #[test]
    fn test_update_encoder_buffer_capacity() {
        let mut obj = MessageExtEncoder::new(Arc::new(MessageStoreConfig {
            max_message_size: 268435456,
            ..MessageStoreConfig::default()
        }));
        obj.update_encoder_buffer_capacity(200);
        assert_eq!(obj.get_max_message_body_size(), 200);
        assert_eq!(obj.byte_buf.len(), 65736);
    }
}
