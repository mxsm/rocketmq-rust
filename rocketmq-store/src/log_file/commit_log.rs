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

use std::{cell::Cell, ops::Deref, sync::Arc};

use rocketmq_common::{
    common::{
        attribute::cq_type::CQType, message::{message_single::MessageExtBrokerInner, MessageConst, MessageVersion}, mix_all
    },
    utils::time_utils,
    CRC32Utils::crc32,
};
use tokio::runtime::Handle;

use crate::{
    base::{
        append_message_callback::DefaultAppendMessageCallback,
        message_result::PutMessageResult,
        message_status_enum::{AppendMessageStatus, PutMessageStatus},
        put_message_context::PutMessageContext,
        swappable::Swappable,
    },
    config::message_store_config::MessageStoreConfig,
    consume_queue::mapped_file_queue::MappedFileQueue,
    message_encoder::message_ext_encoder::MessageExtEncoder,
};

// Message's MAGIC CODE daa320a7
pub const MESSAGE_MAGIC_CODE: i32 = -626843481;

// End of file empty MAGIC CODE cbd43194
pub const BLANK_MAGIC_CODE: i32 = -875286124;

//CRC32 Format: [PROPERTY_CRC32 + NAME_VALUE_SEPARATOR + 10-digit fixed-length string +
// PROPERTY_SEPARATOR]
pub const CRC32_RESERVED_LEN: i32 = (MessageConst::PROPERTY_CRC32.len() + 1 + 10 + 1) as i32;

struct PutMessageThreadLocal {
    encoder: Cell<Option<MessageExtEncoder>>,
    key: Cell<Option<String>>,
}

// thread_local! {
//     static PUT_MESSAGE_THREAD_LOCAL: Arc<PutMessageThreadLocal> = Arc::new(PutMessageThreadLocal{
//         encoder: Cell::new(None),
//         key: Cell::new(None),
//     });
// }

pub struct CommitLog {
    mapped_file_queue: Arc<tokio::sync::RwLock<MappedFileQueue>>,
    message_store_config: Arc<MessageStoreConfig>,
    enabled_append_prop_crc: bool,
}

impl CommitLog {
    pub fn new(message_store_config: Arc<MessageStoreConfig>) -> Self {
        let enabled_append_prop_crc = message_store_config.enabled_append_prop_crc;
        let store_path = message_store_config.get_store_path_commit_log();
        let mapped_file_size = message_store_config.mapped_file_size_commit_log;
        Self {
            mapped_file_queue: Arc::new(tokio::sync::RwLock::new(MappedFileQueue::new(
                store_path,
                mapped_file_size as u64,
                None,
            ))),
            message_store_config,
            enabled_append_prop_crc,
        }
    }
}

impl CommitLog {
    pub fn load(&mut self) -> bool {
        let arc = self.mapped_file_queue.clone();
        Handle::current().block_on(async move { arc.write().await.load() })
    }

    pub async fn put_message(&self, msg: MessageExtBrokerInner) -> PutMessageResult {
        let mut msg = msg;
        if !self.message_store_config.duplication_enable {
            msg.message_ext_inner.store_timestamp = time_utils::get_current_millis() as i64;
        }
        msg.message_ext_inner.body_crc = crc32(
            msg.message_ext_inner
                .message
                .body
                .clone()
                .expect("REASON")
                .deref(),
        );
        if !self.enabled_append_prop_crc {
            msg.delete_property(MessageConst::PROPERTY_CRC32);
        }

        //setting message version
        msg.with_version(MessageVersion::V1);
        let topic = msg.topic();
        // setting auto message on topic length
        if self.message_store_config.auto_message_version_on_topic_len
            && topic.len() > i8::MAX as usize
        {
            msg.with_version(MessageVersion::V2);
        }

        //setting ip type:IPV4 OR IPV6, default is ipv4
        let born_host = msg.born_host();
        if born_host.is_ipv6() {
            msg.with_born_host_v6_flag();
        }

        let store_host = msg.store_host();
        if store_host.is_ipv6() {
            msg.with_store_host_v6_flag();
        }

        let mut encoder = MessageExtEncoder::new(self.message_store_config.clone());
        let put_message_result = encoder.encode(&msg);
        if let Some(result) = put_message_result {
            return result;
        }
        msg.encoded_buff = encoder.byte_buf();

        let mut mapped_file_guard = self.mapped_file_queue.write().await;
        let mapped_file = match mapped_file_guard.get_last_mapped_file_mut() {
            None => mapped_file_guard
                .get_last_mapped_file_mut_start_offset(0, true)
                .unwrap(),
            Some(mapped_file) => mapped_file,
        };
        let topic_queue_key = generate_key(&msg);
        let mut put_message_context = PutMessageContext::new(topic_queue_key);
        let append_message_callback =
            DefaultAppendMessageCallback::new(self.message_store_config.clone());
        /* let result =
            append_message_callback.do_append(mapped_file.file_from_offset() as i64, 0, &mut msg);
        mapped_file.append_data(msg.encoded_buff.clone(), false);*/

        let result =
            mapped_file.lock().append_message(msg, append_message_callback, &mut put_message_context);

        match result.status {
            AppendMessageStatus::PutOk => {
                PutMessageResult::new_append_result(PutMessageStatus::PutOk, Some(result))
            }
            AppendMessageStatus::EndOfFile => {
                unimplemented!()
            }
            AppendMessageStatus::MessageSizeExceeded
            | AppendMessageStatus::PropertiesSizeExceeded => {
                PutMessageResult::new_append_result(PutMessageStatus::MessageIllegal, Some(result))
            }
            AppendMessageStatus::UnknownError => {
                PutMessageResult::new_append_result(PutMessageStatus::UnknownError, Some(result))
            }
        }
    }

    pub fn is_multi_dispatch_msg(msg_inner: &MessageExtBrokerInner) -> bool {
        msg_inner
            .property(MessageConst::PROPERTY_INNER_MULTI_DISPATCH)
            .map_or(false, |s| !s.is_empty())
            && msg_inner
                .topic()
                .starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX)
    }

    pub fn get_message_num(&self,_msg_inner: &MessageExtBrokerInner) -> i16 {
        let mut message_num = 1i16;

        message_num
    }

    fn get_cq_type(&self,_msg_inner:MessageExtBrokerInner) -> CQType{
        CQType::SimpleCQ
    }
}

fn generate_key(msg: &MessageExtBrokerInner) -> String {
    let mut topic_queue_key = String::new();
    topic_queue_key.push_str(msg.topic());
    topic_queue_key.push('-');
    topic_queue_key.push_str(msg.queue_id().to_string().as_str());
    topic_queue_key
}

impl Swappable for CommitLog {
    fn swap_map(
        &self,
        _reserve_num: i32,
        _force_swap_interval_ms: i64,
        _normal_swap_interval_ms: i64,
    ) {
        todo!()
    }

    fn clean_swapped_map(&self, _force_clean_swap_interval_ms: i64) {
        todo!()
    }
}
