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
use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_store::stats::stats_type::StatsType;

#[derive(Default)]
pub struct ConsumeMessageContext {
    pub consumer_group: CheetahString,
    pub topic: CheetahString,
    pub queue_id: Option<i32>,
    pub client_host: CheetahString,
    pub store_host: CheetahString,
    pub message_ids: HashMap<String, i64>,
    pub body_length: i32,
    pub success: bool,
    pub status: CheetahString,
    //mq_trace_context: Option<Box<dyn std::any::Any>>, // Replace with actual type
    pub topic_config: Arc<TopicConfig>,

    pub account_auth_type: Option<CheetahString>,
    pub account_owner_parent: Option<CheetahString>,
    pub account_owner_self: Option<CheetahString>,
    pub rcv_msg_num: i32,
    pub rcv_msg_size: i32,
    pub rcv_stat: StatsType,
    pub commercial_rcv_msg_num: i32,

    pub commercial_owner: Option<CheetahString>,
    pub commercial_rcv_stats: StatsType,
    pub commercial_rcv_times: i32,
    pub commercial_rcv_size: i32,

    pub namespace: CheetahString,
}
