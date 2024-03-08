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
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::protocol::subscription::{
    group_retry_policy::GroupRetryPolicy, simple_subscription_data::SimpleSubscriptionData,
};
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionGroupConfig {
    group_name: String,

    consume_enable: bool,
    consume_from_min_enable: bool,
    consume_broadcast_enable: bool,
    consume_message_orderly: bool,

    retry_queue_nums: i32,
    retry_max_times: i32,
    group_retry_policy: GroupRetryPolicy,

    broker_id: u64,
    which_broker_when_consume_slowly: u64,

    notify_consumer_ids_changed_enable: bool,

    group_sys_flag: i32,

    consume_timeout_minute: i32,

    subscription_data_set: HashSet<SimpleSubscriptionData>,
    attributes: HashMap<String, String>,
}

impl SubscriptionGroupConfig {
    pub fn group_name(&self) -> &str {
        &self.group_name
    }
    pub fn consume_enable(&self) -> bool {
        self.consume_enable
    }
    pub fn consume_from_min_enable(&self) -> bool {
        self.consume_from_min_enable
    }
    pub fn consume_broadcast_enable(&self) -> bool {
        self.consume_broadcast_enable
    }
    pub fn consume_message_orderly(&self) -> bool {
        self.consume_message_orderly
    }
    pub fn retry_queue_nums(&self) -> i32 {
        self.retry_queue_nums
    }
    pub fn retry_max_times(&self) -> i32 {
        self.retry_max_times
    }
    pub fn group_retry_policy(&self) -> &GroupRetryPolicy {
        &self.group_retry_policy
    }
    pub fn broker_id(&self) -> u64 {
        self.broker_id
    }
    pub fn which_broker_when_consume_slowly(&self) -> u64 {
        self.which_broker_when_consume_slowly
    }
    pub fn notify_consumer_ids_changed_enable(&self) -> bool {
        self.notify_consumer_ids_changed_enable
    }
    pub fn group_sys_flag(&self) -> i32 {
        self.group_sys_flag
    }
    pub fn consume_timeout_minute(&self) -> i32 {
        self.consume_timeout_minute
    }
    pub fn subscription_data_set(&self) -> &HashSet<SimpleSubscriptionData> {
        &self.subscription_data_set
    }
    pub fn attributes(&self) -> &HashMap<String, String> {
        &self.attributes
    }
}
