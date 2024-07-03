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
use std::collections::HashSet;

use rocketmq_common::common::mix_all::MASTER_ID;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::subscription::group_retry_policy::GroupRetryPolicy;
use crate::protocol::subscription::simple_subscription_data::SimpleSubscriptionData;

#[derive(Debug, Clone, Serialize, Deserialize)]
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

    subscription_data_set: Option<HashSet<SimpleSubscriptionData>>,
    attributes: HashMap<String, String>,
}

impl SubscriptionGroupConfig {
    pub fn new(group_name: &str) -> Self {
        Self {
            group_name: group_name.to_string(),
            ..Default::default()
        }
    }
}

impl Default for SubscriptionGroupConfig {
    fn default() -> Self {
        SubscriptionGroupConfig {
            group_name: "".to_string(),

            consume_enable: true,
            consume_from_min_enable: false,
            consume_broadcast_enable: true,
            consume_message_orderly: false,

            retry_queue_nums: 1,
            retry_max_times: 16,
            group_retry_policy: GroupRetryPolicy::default(),

            broker_id: MASTER_ID,
            which_broker_when_consume_slowly: 1,

            notify_consumer_ids_changed_enable: true,

            group_sys_flag: 0,

            consume_timeout_minute: 15,

            subscription_data_set: None,
            attributes: HashMap::new(),
        }
    }
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

    pub fn subscription_data_set(&self) -> Option<&HashSet<SimpleSubscriptionData>> {
        self.subscription_data_set.as_ref()
    }

    pub fn attributes(&self) -> &HashMap<String, String> {
        &self.attributes
    }

    pub fn set_group_name(&mut self, group_name: String) {
        self.group_name = group_name;
    }
    pub fn set_consume_enable(&mut self, consume_enable: bool) {
        self.consume_enable = consume_enable;
    }
    pub fn set_consume_from_min_enable(&mut self, consume_from_min_enable: bool) {
        self.consume_from_min_enable = consume_from_min_enable;
    }
    pub fn set_consume_broadcast_enable(&mut self, consume_broadcast_enable: bool) {
        self.consume_broadcast_enable = consume_broadcast_enable;
    }
    pub fn set_consume_message_orderly(&mut self, consume_message_orderly: bool) {
        self.consume_message_orderly = consume_message_orderly;
    }
    pub fn set_retry_queue_nums(&mut self, retry_queue_nums: i32) {
        self.retry_queue_nums = retry_queue_nums;
    }
    pub fn set_retry_max_times(&mut self, retry_max_times: i32) {
        self.retry_max_times = retry_max_times;
    }
    pub fn set_group_retry_policy(&mut self, group_retry_policy: GroupRetryPolicy) {
        self.group_retry_policy = group_retry_policy;
    }
    pub fn set_broker_id(&mut self, broker_id: u64) {
        self.broker_id = broker_id;
    }
    pub fn set_which_broker_when_consume_slowly(&mut self, which_broker_when_consume_slowly: u64) {
        self.which_broker_when_consume_slowly = which_broker_when_consume_slowly;
    }
    pub fn set_notify_consumer_ids_changed_enable(
        &mut self,
        notify_consumer_ids_changed_enable: bool,
    ) {
        self.notify_consumer_ids_changed_enable = notify_consumer_ids_changed_enable;
    }
    pub fn set_group_sys_flag(&mut self, group_sys_flag: i32) {
        self.group_sys_flag = group_sys_flag;
    }
    pub fn set_consume_timeout_minute(&mut self, consume_timeout_minute: i32) {
        self.consume_timeout_minute = consume_timeout_minute;
    }
    pub fn set_subscription_data_set(
        &mut self,
        subscription_data_set: Option<HashSet<SimpleSubscriptionData>>,
    ) {
        self.subscription_data_set = subscription_data_set;
    }
    pub fn set_attributes(&mut self, attributes: HashMap<String, String>) {
        self.attributes = attributes;
    }
}

#[cfg(test)]
mod subscription_group_config_tests {
    use super::*;
    //use crate::protocol::subscription::group_retry_policy::RetryPolicy;

    #[test]
    fn creating_default_subscription_group_config() {
        let config = SubscriptionGroupConfig::default();
        assert_eq!(config.group_name, "");
        assert!(config.consume_enable);
        assert!(!config.consume_from_min_enable);
        assert!(config.consume_broadcast_enable);
        assert!(!config.consume_message_orderly);
        assert_eq!(config.retry_queue_nums, 1);
        assert_eq!(config.retry_max_times, 16);
        // assert_eq!(config.group_retry_policy, GroupRetryPolicy::default());
        assert_eq!(config.broker_id, MASTER_ID);
        assert_eq!(config.which_broker_when_consume_slowly, 1);
        assert!(config.notify_consumer_ids_changed_enable);
        assert_eq!(config.group_sys_flag, 0);
        assert_eq!(config.consume_timeout_minute, 15);
        assert!(config.subscription_data_set.is_none());
        assert!(config.attributes.is_empty());
    }

    #[test]
    fn setting_and_getting_fields() {
        let mut config = SubscriptionGroupConfig::default();
        config.set_group_name("test_group".to_string());
        config.set_consume_enable(false);
        config.set_consume_from_min_enable(true);
        config.set_consume_broadcast_enable(false);
        config.set_consume_message_orderly(true);
        config.set_retry_queue_nums(2);
        config.set_retry_max_times(10);
        //config.set_group_retry_policy(GroupRetryPolicy::Custom(RetryPolicy::FixedDelay(100)));
        config.set_broker_id(2);
        config.set_which_broker_when_consume_slowly(2);
        config.set_notify_consumer_ids_changed_enable(false);
        config.set_group_sys_flag(1);
        config.set_consume_timeout_minute(30);
        config.set_subscription_data_set(Some(HashSet::new()));
        config.set_attributes(HashMap::from([("key".to_string(), "value".to_string())]));

        assert_eq!(config.group_name(), "test_group");
        assert!(!config.consume_enable());
        assert!(config.consume_from_min_enable());
        assert!(!config.consume_broadcast_enable());
        assert!(config.consume_message_orderly());
        assert_eq!(config.retry_queue_nums(), 2);
        assert_eq!(config.retry_max_times(), 10);
        /*        assert_eq!(
            config.group_retry_policy(),
            &GroupRetryPolicy::Custom(RetryPolicy::FixedDelay(100))
        );*/
        assert_eq!(config.broker_id(), 2);
        assert_eq!(config.which_broker_when_consume_slowly(), 2);
        assert!(!config.notify_consumer_ids_changed_enable());
        assert_eq!(config.group_sys_flag(), 1);
        assert_eq!(config.consume_timeout_minute(), 30);
        assert!(config.subscription_data_set().is_some());
        assert_eq!(
            config.attributes(),
            &HashMap::from([("key".to_string(), "value".to_string())])
        );
    }
}
