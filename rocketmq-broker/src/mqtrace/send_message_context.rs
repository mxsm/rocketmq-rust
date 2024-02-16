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
use std::{any::Any, collections::HashMap};

use rocketmq_common::common::message::message_enum::MessageType;
use rocketmq_store::status::StatsType;

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
    pub mq_trace_context: Option<Box<dyn Any>>,
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
