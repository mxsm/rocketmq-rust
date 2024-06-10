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
use std::any::Any;
use std::collections::HashMap;

use rocketmq_common::common::message::message_enum::MessageType;
use rocketmq_store::stats::stats_type::StatsType;

#[derive(Debug, Default)]
pub struct SendMessageContext {
    pub namespace: String,
    pub producer_group: String,
    pub topic: String,
    pub msg_id: String,
    pub origin_msg_id: String,
    pub queue_id: Option<i32>,
    pub queue_offset: Option<i64>,
    pub broker_addr: String,
    pub born_host: String,
    pub body_length: i32,
    pub code: i32,
    pub error_msg: String,
    pub msg_props: String,
    pub mq_trace_context: Option<Box<dyn Any + Send + 'static>>,
    pub ext_props: HashMap<String, String>,
    pub broker_region_id: String,
    pub msg_unique_key: String,
    pub born_time_stamp: i64,
    pub request_time_stamp: i64,
    pub msg_type: MessageType,
    pub is_success: bool,
    pub account_auth_type: String,
    pub account_owner_parent: String,
    pub account_owner_self: String,
    pub send_msg_num: i32,
    pub send_msg_size: i32,
    pub send_stat: StatsType,
    pub commercial_send_msg_num: i32,
    pub commercial_owner: String,
    pub commercial_send_stats: StatsType,
    pub commercial_send_size: i32,
    pub commercial_send_times: i32,
}
impl SendMessageContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn topic(&mut self, topic: String) {
        self.topic = topic;
    }

    pub fn body_length(&mut self, body_length: i32) {
        self.body_length = body_length;
    }

    pub fn msg_props(&mut self, msg_props: String) {
        self.msg_props = msg_props;
    }

    pub fn broker_addr(&mut self, broker_addr: String) {
        self.broker_addr = broker_addr;
    }

    pub fn queue_id(&mut self, queue_id: Option<i32>) {
        self.queue_id = queue_id;
    }

    pub fn queue_offset(&mut self, queue_offset: i64) {
        self.queue_offset = Some(queue_offset);
    }

    pub fn born_host(&mut self, born_host: String) {
        self.born_host = born_host;
    }

    pub fn broker_region_id(&mut self, broker_region_id: String) {
        self.broker_region_id = broker_region_id;
    }

    pub fn msg_unique_key(&mut self, msg_unique_key: String) {
        self.msg_unique_key = msg_unique_key;
    }

    pub fn born_time_stamp(&mut self, born_time_stamp: i64) {
        self.born_time_stamp = born_time_stamp;
    }

    pub fn request_time_stamp(&mut self, request_time_stamp: i64) {
        self.request_time_stamp = request_time_stamp;
    }

    pub fn msg_type(&mut self, msg_type: MessageType) {
        self.msg_type = msg_type;
    }

    pub fn is_success(&mut self, is_success: bool) {
        self.is_success = is_success;
    }

    pub fn account_auth_type(&mut self, account_auth_type: String) {
        self.account_auth_type = account_auth_type;
    }

    pub fn account_owner_parent(&mut self, account_owner_parent: String) {
        self.account_owner_parent = account_owner_parent;
    }

    pub fn account_owner_self(&mut self, account_owner_self: String) {
        self.account_owner_self = account_owner_self;
    }

    pub fn send_msg_num(&mut self, send_msg_num: i32) {
        self.send_msg_num = send_msg_num;
    }

    pub fn send_msg_size(&mut self, send_msg_size: i32) {
        self.send_msg_size = send_msg_size;
    }

    pub fn send_stat(&mut self, send_stat: StatsType) {
        self.send_stat = send_stat;
    }

    pub fn commercial_send_msg_num(&mut self, commercial_send_msg_num: i32) {
        self.commercial_send_msg_num = commercial_send_msg_num;
    }

    pub fn commercial_owner(&mut self, commercial_owner: String) {
        self.commercial_owner = commercial_owner;
    }

    pub fn commercial_send_stats(&mut self, commercial_send_stats: StatsType) {
        self.commercial_send_stats = commercial_send_stats;
    }

    pub fn commercial_send_size(&mut self, commercial_send_size: i32) {
        self.commercial_send_size = commercial_send_size;
    }

    pub fn commercial_send_times(&mut self, commercial_send_times: i32) {
        self.commercial_send_times = commercial_send_times;
    }
}
