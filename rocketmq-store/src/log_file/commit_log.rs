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

use std::{ops::Deref, sync::Arc};

use rocketmq_common::{
    common::message::{
        message_single::MessageExtBrokerInner, MessageConst, MessageVersion, MESSAGE_MAGIC_CODE_V1,
        MESSAGE_MAGIC_CODE_V2,
    },
    utils::time_utils,
    CRC32Utils::crc32,
};

use crate::{
    base::{message_result::PutMessageResult, swappable::Swappable},
    config::message_store_config::MessageStoreConfig,
    consume_queue::mapped_file_queue::MappedFileQueue,
};

// Message's MAGIC CODE daa320a7
pub const MESSAGE_MAGIC_CODE: i32 = -626843481;

// End of file empty MAGIC CODE cbd43194
pub const BLANK_MAGIC_CODE: i32 = -875286124;

#[derive(Default)]
pub struct CommitLog {
    pub(crate) mapped_file_queue: MappedFileQueue,
    pub(crate) message_store_config: Arc<MessageStoreConfig>,
    pub(crate) enabled_append_prop_crc: bool,
}

impl CommitLog {
    pub fn load(&mut self) -> bool {
        self.mapped_file_queue.load()
    }

    async fn async_put_message(&self, msg: MessageExtBrokerInner) -> PutMessageResult {
        let mut msg = msg;
        if !self.message_store_config.duplication_enable {
            msg.message_ext_inner.store_timestamp = time_utils::get_current_millis() as i64;
        }
        msg.message_ext_inner.body_crc = crc32(msg.message_ext_inner.message_inner.body.deref());
        if !self.enabled_append_prop_crc {
            msg.delete_property(MessageConst::PROPERTY_CRC32);
        }

        //setting message version
        msg.with_version(MessageVersion::V1(MESSAGE_MAGIC_CODE_V1));
        let topic = msg.topic();
        // setting auto message on topic length
        if self.message_store_config.auto_message_version_on_topic_len
            && topic.len() > i8::MAX as usize
        {
            msg.with_version(MessageVersion::V2(MESSAGE_MAGIC_CODE_V2));
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

        PutMessageResult::default()
    }
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
