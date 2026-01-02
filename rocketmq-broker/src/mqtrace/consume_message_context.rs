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

use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_store::stats::stats_type::StatsType;

pub struct ConsumeMessageContext<'a> {
    pub consumer_group: &'a CheetahString,
    pub topic: &'a CheetahString,
    pub queue_id: Option<i32>,
    pub client_host: Option<&'a CheetahString>,
    pub store_host: Option<&'a CheetahString>,
    pub message_ids: Option<HashMap<String, i64>>,
    pub body_length: i32,
    pub success: bool,
    pub status: Option<&'a CheetahString>,
    //mq_trace_context: Option<Box<dyn std::any::Any>>, // Replace with actual type
    pub topic_config: Option<&'a TopicConfig>,

    pub account_auth_type: Option<&'a CheetahString>,
    pub account_owner_parent: Option<&'a CheetahString>,
    pub account_owner_self: Option<&'a CheetahString>,
    pub rcv_msg_num: i32,
    pub rcv_msg_size: i32,
    pub rcv_stat: StatsType,
    pub commercial_rcv_msg_num: i32,

    pub commercial_owner: Option<&'a CheetahString>,
    pub commercial_rcv_stats: StatsType,
    pub commercial_rcv_times: i32,
    pub commercial_rcv_size: i32,

    pub namespace: &'a CheetahString,
}
