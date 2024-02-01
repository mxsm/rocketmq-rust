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

use std::{collections::HashMap, net::SocketAddr};

use crate::common::message::{MessageTrait, MessageVersion, MESSAGE_MAGIC_CODE_V1};

#[derive(Clone, Debug)]
pub struct Message {
    pub topic: String,
    pub flag: i32,
    pub properties: HashMap<String, String>,
    pub body: bytes::Bytes,
    pub transaction_id: Option<String>,
}

impl MessageTrait for Message {
    fn get_topic(&self) -> &str {
        todo!()
    }

    fn set_topic(&mut self, _topic: impl Into<String>) {
        todo!()
    }

    fn get_tags(&self) -> Option<&str> {
        todo!()
    }

    fn set_tags(&mut self, _tags: impl Into<String>) {
        todo!()
    }

    fn put_property(&mut self, _key: impl Into<String>, _value: impl Into<String>) {
        todo!()
    }

    fn get_properties(&self) -> &HashMap<String, String> {
        todo!()
    }

    fn put_user_property(&mut self, _name: impl Into<String>, _value: impl Into<String>) {
        todo!()
    }

    fn get_delay_time_level(&self) -> i32 {
        todo!()
    }

    fn set_delay_time_level(&self, _level: i32) -> i32 {
        todo!()
    }
}

#[derive(Clone, Debug)]
pub struct MessageExt {
    pub message_inner: Message,
    pub broker_name: String,
    pub queue_id: i32,
    pub store_size: i32,
    pub queue_offset: i64,
    pub sys_flag: i32,
    pub born_timestamp: i64,
    pub born_host: SocketAddr,
    pub store_timestamp: i64,
    pub store_host: SocketAddr,
    pub msg_id: String,
    pub commit_log_offset: i64,
    pub body_crc: i32,
    pub reconsume_times: i32,
    pub prepared_transaction_offset: i64,
}

#[derive(Clone, Debug)]
pub struct MessageClientExt {
    pub message_ext_inner: MessageExt,
}

#[derive(Debug)]
pub struct MessageExtBrokerInner {
    pub message_ext_inner: MessageExt,
    pub properties_string: String,
    pub tags_code: i64,
    pub encoded_buff: bytes::Bytes,
    pub encode_completed: bool,
}

impl MessageExtBrokerInner {
    const VERSION: MessageVersion = MessageVersion::V1(MESSAGE_MAGIC_CODE_V1);
}
