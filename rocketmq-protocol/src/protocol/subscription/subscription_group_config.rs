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
use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_model::common::attribute::subscription_group_attributes::LITE_BIND_TOPIC_ATTRIBUTE_NAME;
use rocketmq_model::common::attribute::subscription_group_attributes::LITE_SUB_CLIENT_MAX_EVENT_COUNT_ATTRIBUTE_NAME;
use rocketmq_model::common::attribute::subscription_group_attributes::LITE_SUB_CLIENT_QUOTA_ATTRIBUTE_NAME;
use rocketmq_model::common::attribute::subscription_group_attributes::LITE_SUB_MODEL_ATTRIBUTE_NAME;
use rocketmq_model::common::attribute::subscription_group_attributes::LITE_SUB_RESET_OFFSET_EXCLUSIVE_ATTRIBUTE_NAME;
use rocketmq_model::common::attribute::subscription_group_attributes::LITE_SUB_RESET_OFFSET_UNSUBSCRIBE_ATTRIBUTE_NAME;
use rocketmq_model::common::attribute::subscription_group_attributes::LITE_SUB_WILDCARD_ATTRIBUTE_NAME;
use rocketmq_model::common::attribute::subscription_group_attributes::PRIORITY_FACTOR_ATTRIBUTE_NAME;
use rocketmq_model::common::topic::TopicValidator;
use serde::Deserialize;
use serde::Serialize;

use crate::common::wire_constants::MASTER_ID;
use crate::protocol::subscription::group_retry_policy::GroupRetryPolicy;
use crate::protocol::subscription::simple_subscription_data::SimpleSubscriptionData;

pub const SUBSCRIPTION_GROUP_NAME_MAX_LENGTH: usize = 255;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionGroupValidationError {
    Blank,
    TooLong {
        group_name: CheetahString,
        max_length: usize,
    },
    IllegalCharacters {
        group_name: CheetahString,
    },
    Duplicate {
        group_name: CheetahString,
    },
}

impl std::fmt::Display for SubscriptionGroupValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Blank => formatter.write_str("The specified group is blank."),
            Self::TooLong { group_name, max_length } => write!(
                formatter,
                "The specified group: {group_name}, is longer than group max length: {max_length}"
            ),
            Self::IllegalCharacters { group_name } => write!(
                formatter,
                "The specified group: {group_name}, contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$"
            ),
            Self::Duplicate { group_name } => {
                write!(
                    formatter,
                    "The specified group list contains duplicate group {group_name}."
                )
            }
        }
    }
}

impl std::error::Error for SubscriptionGroupValidationError {}

/// Validates a consumer group name using the Java 5.5 `TopicValidator.validateGroup` contract.
pub fn validate_subscription_group_name(group_name: &str) -> Result<(), SubscriptionGroupValidationError> {
    if group_name.trim().is_empty() {
        return Err(SubscriptionGroupValidationError::Blank);
    }
    if TopicValidator::is_topic_or_group_illegal(group_name) {
        return Err(SubscriptionGroupValidationError::IllegalCharacters {
            group_name: CheetahString::from(group_name),
        });
    }
    if group_name.len() > SUBSCRIPTION_GROUP_NAME_MAX_LENGTH {
        return Err(SubscriptionGroupValidationError::TooLong {
            group_name: CheetahString::from(group_name),
            max_length: SUBSCRIPTION_GROUP_NAME_MAX_LENGTH,
        });
    }
    Ok(())
}

/// Validates every group before a batch mutation and rejects duplicate names.
pub fn validate_subscription_group_configs(
    configs: &[SubscriptionGroupConfig],
) -> Result<(), SubscriptionGroupValidationError> {
    let mut seen = HashSet::with_capacity(configs.len());
    for config in configs {
        validate_subscription_group_name(config.group_name().as_str())?;
        if !seen.insert(config.group_name().clone()) {
            return Err(SubscriptionGroupValidationError::Duplicate {
                group_name: config.group_name().clone(),
            });
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SubscriptionGroupConfig {
    group_name: CheetahString,

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
    attributes: HashMap<CheetahString, CheetahString>,
}

impl SubscriptionGroupConfig {
    pub fn new(group_name: CheetahString) -> Self {
        Self {
            group_name,
            ..Default::default()
        }
    }
}

impl Default for SubscriptionGroupConfig {
    fn default() -> Self {
        SubscriptionGroupConfig {
            group_name: CheetahString::default(),

            consume_enable: true,
            consume_from_min_enable: true,
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
    #[inline]
    pub fn group_name(&self) -> &CheetahString {
        &self.group_name
    }

    #[inline]
    pub fn consume_enable(&self) -> bool {
        self.consume_enable
    }

    #[inline]
    pub fn consume_from_min_enable(&self) -> bool {
        self.consume_from_min_enable
    }

    #[inline]
    pub fn consume_broadcast_enable(&self) -> bool {
        self.consume_broadcast_enable
    }

    #[inline]
    pub fn consume_message_orderly(&self) -> bool {
        self.consume_message_orderly
    }

    #[inline]
    pub fn retry_queue_nums(&self) -> i32 {
        self.retry_queue_nums
    }

    #[inline]
    pub fn retry_max_times(&self) -> i32 {
        self.retry_max_times
    }

    #[inline]
    pub fn group_retry_policy(&self) -> &GroupRetryPolicy {
        &self.group_retry_policy
    }

    #[inline]
    pub fn broker_id(&self) -> u64 {
        self.broker_id
    }

    #[inline]
    pub fn which_broker_when_consume_slowly(&self) -> u64 {
        self.which_broker_when_consume_slowly
    }

    #[inline]
    pub fn notify_consumer_ids_changed_enable(&self) -> bool {
        self.notify_consumer_ids_changed_enable
    }

    #[inline]
    pub fn group_sys_flag(&self) -> i32 {
        self.group_sys_flag
    }

    #[inline]
    pub fn consume_timeout_minute(&self) -> i32 {
        self.consume_timeout_minute
    }

    #[inline]
    pub fn subscription_data_set(&self) -> Option<&HashSet<SimpleSubscriptionData>> {
        self.subscription_data_set.as_ref()
    }

    #[inline]
    pub fn attributes(&self) -> &HashMap<CheetahString, CheetahString> {
        &self.attributes
    }

    #[inline]
    pub fn lite_bind_topic(&self) -> Option<&CheetahString> {
        self.attributes
            .get(&CheetahString::from_static_str(LITE_BIND_TOPIC_ATTRIBUTE_NAME))
            .filter(|value| !value.is_empty())
    }

    #[inline]
    pub fn lite_sub_client_quota(&self) -> i32 {
        self.attributes
            .get(&CheetahString::from_static_str(LITE_SUB_CLIENT_QUOTA_ATTRIBUTE_NAME))
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(2000) as i32
    }

    #[inline]
    pub fn lite_sub_exclusive(&self) -> bool {
        self.attributes
            .get(&CheetahString::from_static_str(LITE_SUB_MODEL_ATTRIBUTE_NAME))
            .is_some_and(|value| value == "Exclusive")
    }

    #[inline]
    pub fn max_client_event_count(&self) -> i32 {
        self.attributes
            .get(&CheetahString::from_static_str(
                LITE_SUB_CLIENT_MAX_EVENT_COUNT_ATTRIBUTE_NAME,
            ))
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(-1)
    }

    /// Returns whether this group follows every active Lite topic under its bound parent topic.
    ///
    /// Java 5.5 models this as the presence of the `lite.sub.wildcard` attribute rather than as a
    /// normal tag expression. Preserve that presence-based wire contract for compatibility.
    #[inline]
    pub fn lite_sub_wildcard(&self) -> bool {
        self.attributes
            .contains_key(&CheetahString::from_static_str(LITE_SUB_WILDCARD_ATTRIBUTE_NAME))
    }

    #[inline]
    pub fn set_lite_sub_wildcard(&mut self, wildcard: bool) {
        let key = CheetahString::from_static_str(LITE_SUB_WILDCARD_ATTRIBUTE_NAME);
        if wildcard {
            self.attributes.insert(key, CheetahString::from_static_str("true"));
        } else {
            self.attributes.remove(&key);
        }
    }

    #[inline]
    pub fn reset_offset_in_exclusive_mode(&self) -> bool {
        self.attributes
            .get(&CheetahString::from_static_str(
                LITE_SUB_RESET_OFFSET_EXCLUSIVE_ATTRIBUTE_NAME,
            ))
            .is_some_and(|value| value.parse::<bool>().unwrap_or(false))
    }

    #[inline]
    pub fn reset_offset_on_unsubscribe(&self) -> bool {
        self.attributes
            .get(&CheetahString::from_static_str(
                LITE_SUB_RESET_OFFSET_UNSUBSCRIBE_ATTRIBUTE_NAME,
            ))
            .is_some_and(|value| value.parse::<bool>().unwrap_or(false))
    }

    #[inline]
    pub fn priority_factor(&self) -> i32 {
        self.attributes
            .get(&CheetahString::from_static_str(PRIORITY_FACTOR_ATTRIBUTE_NAME))
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(100)
    }

    #[inline]
    pub fn set_group_name(&mut self, group_name: CheetahString) {
        self.group_name = group_name;
    }

    #[inline]
    pub fn set_consume_enable(&mut self, consume_enable: bool) {
        self.consume_enable = consume_enable;
    }

    #[inline]
    pub fn set_consume_from_min_enable(&mut self, consume_from_min_enable: bool) {
        self.consume_from_min_enable = consume_from_min_enable;
    }

    #[inline]
    pub fn set_consume_broadcast_enable(&mut self, consume_broadcast_enable: bool) {
        self.consume_broadcast_enable = consume_broadcast_enable;
    }

    #[inline]
    pub fn set_consume_message_orderly(&mut self, consume_message_orderly: bool) {
        self.consume_message_orderly = consume_message_orderly;
    }

    #[inline]
    pub fn set_retry_queue_nums(&mut self, retry_queue_nums: i32) {
        self.retry_queue_nums = retry_queue_nums;
    }

    #[inline]
    pub fn set_retry_max_times(&mut self, retry_max_times: i32) {
        self.retry_max_times = retry_max_times;
    }

    #[inline]
    pub fn set_group_retry_policy(&mut self, group_retry_policy: GroupRetryPolicy) {
        self.group_retry_policy = group_retry_policy;
    }

    #[inline]
    pub fn set_broker_id(&mut self, broker_id: u64) {
        self.broker_id = broker_id;
    }

    #[inline]
    pub fn set_which_broker_when_consume_slowly(&mut self, which_broker_when_consume_slowly: u64) {
        self.which_broker_when_consume_slowly = which_broker_when_consume_slowly;
    }

    #[inline]
    pub fn set_notify_consumer_ids_changed_enable(&mut self, notify_consumer_ids_changed_enable: bool) {
        self.notify_consumer_ids_changed_enable = notify_consumer_ids_changed_enable;
    }

    #[inline]
    pub fn set_group_sys_flag(&mut self, group_sys_flag: i32) {
        self.group_sys_flag = group_sys_flag;
    }

    #[inline]
    pub fn set_consume_timeout_minute(&mut self, consume_timeout_minute: i32) {
        self.consume_timeout_minute = consume_timeout_minute;
    }

    #[inline]
    pub fn set_subscription_data_set(&mut self, subscription_data_set: Option<HashSet<SimpleSubscriptionData>>) {
        self.subscription_data_set = subscription_data_set;
    }

    #[inline]
    pub fn set_attributes(&mut self, attributes: HashMap<CheetahString, CheetahString>) {
        self.attributes = attributes;
    }

    #[inline]
    pub fn set_lite_bind_topic(&mut self, lite_bind_topic: Option<CheetahString>) {
        let key = CheetahString::from_static_str(LITE_BIND_TOPIC_ATTRIBUTE_NAME);
        match lite_bind_topic {
            Some(value) if !value.is_empty() => {
                self.attributes.insert(key, value);
            }
            _ => {
                self.attributes.remove(&key);
            }
        }
    }
}

#[cfg(test)]
mod subscription_group_config_tests {
    use super::*;
    use crate::protocol::subscription::group_retry_policy_type::GroupRetryPolicyType;

    #[test]
    fn defaults_and_mutators_preserve_configuration() {
        let mut config = SubscriptionGroupConfig::new("initial_group".into());
        assert_eq!(config.group_name(), "initial_group");
        assert_eq!(
            (
                config.consume_enable(),
                config.consume_from_min_enable(),
                config.consume_broadcast_enable(),
                config.consume_message_orderly(),
                config.retry_queue_nums(),
                config.retry_max_times(),
                config.broker_id(),
                config.which_broker_when_consume_slowly(),
                config.notify_consumer_ids_changed_enable(),
                config.group_sys_flag(),
                config.consume_timeout_minute(),
            ),
            (true, true, true, false, 1, 16, MASTER_ID, 1, true, 0, 15)
        );
        assert_eq!(config.group_retry_policy().type_(), GroupRetryPolicyType::Customized);
        assert!(config.subscription_data_set().is_none());
        assert!(config.attributes().is_empty());

        let mut retry_policy = GroupRetryPolicy::default();
        retry_policy.set_type_(GroupRetryPolicyType::Exponential);
        config.set_group_name("test_group".into());
        config.set_consume_enable(false);
        config.set_consume_from_min_enable(true);
        config.set_consume_broadcast_enable(false);
        config.set_consume_message_orderly(true);
        config.set_retry_queue_nums(2);
        config.set_retry_max_times(10);
        config.set_group_retry_policy(retry_policy);
        config.set_broker_id(2);
        config.set_which_broker_when_consume_slowly(2);
        config.set_notify_consumer_ids_changed_enable(false);
        config.set_group_sys_flag(1);
        config.set_consume_timeout_minute(30);
        config.set_subscription_data_set(Some(HashSet::from([SimpleSubscriptionData::new(
            "topic".to_string(),
            "TAG".to_string(),
            "*".to_string(),
            1,
        )])));
        config.set_attributes(HashMap::from([("key".into(), "value".into())]));

        assert_eq!(
            (
                config.group_name().as_str(),
                config.consume_enable(),
                config.consume_from_min_enable(),
                config.consume_broadcast_enable(),
                config.consume_message_orderly(),
                config.retry_queue_nums(),
                config.retry_max_times(),
                config.broker_id(),
                config.which_broker_when_consume_slowly(),
                config.notify_consumer_ids_changed_enable(),
                config.group_sys_flag(),
                config.consume_timeout_minute(),
            ),
            ("test_group", false, true, false, true, 2, 10, 2, 2, false, 1, 30)
        );
        assert_eq!(config.group_retry_policy().type_(), GroupRetryPolicyType::Exponential);
        assert_eq!(config.subscription_data_set().unwrap().len(), 1);
        assert_eq!(config.attributes(), &HashMap::from([("key".into(), "value".into())]));
    }

    #[test]
    fn lite_bind_topic_round_trips_through_attributes() {
        let mut config = SubscriptionGroupConfig::default();

        assert!(config.lite_bind_topic().is_none());

        config.set_lite_bind_topic(Some("parent-topic".into()));
        assert_eq!(
            config.lite_bind_topic(),
            Some(&CheetahString::from_static_str("parent-topic"))
        );

        config.set_lite_bind_topic(None);
        assert!(config.lite_bind_topic().is_none());
    }

    #[test]
    fn lite_subscription_attributes_use_defaults_and_parse_values() {
        let config = SubscriptionGroupConfig::default();

        assert_eq!(
            (
                config.lite_sub_client_quota(),
                config.lite_sub_exclusive(),
                config.max_client_event_count(),
                config.reset_offset_in_exclusive_mode(),
                config.reset_offset_on_unsubscribe(),
                config.priority_factor(),
                config.lite_sub_wildcard(),
            ),
            (2000, false, -1, false, false, 100, false)
        );

        let mut config = config;
        config.set_attributes(HashMap::from([
            (
                CheetahString::from_static_str(LITE_SUB_CLIENT_QUOTA_ATTRIBUTE_NAME),
                CheetahString::from_static_str("128"),
            ),
            (
                CheetahString::from_static_str(LITE_SUB_MODEL_ATTRIBUTE_NAME),
                CheetahString::from_static_str("Exclusive"),
            ),
            (
                CheetahString::from_static_str(LITE_SUB_CLIENT_MAX_EVENT_COUNT_ATTRIBUTE_NAME),
                CheetahString::from_static_str("256"),
            ),
            (
                CheetahString::from_static_str(LITE_SUB_RESET_OFFSET_EXCLUSIVE_ATTRIBUTE_NAME),
                CheetahString::from_static_str("true"),
            ),
            (
                CheetahString::from_static_str(LITE_SUB_RESET_OFFSET_UNSUBSCRIBE_ATTRIBUTE_NAME),
                CheetahString::from_static_str("true"),
            ),
            (
                CheetahString::from_static_str(PRIORITY_FACTOR_ATTRIBUTE_NAME),
                CheetahString::from_static_str("25"),
            ),
        ]));

        assert_eq!(
            (
                config.lite_sub_client_quota(),
                config.lite_sub_exclusive(),
                config.max_client_event_count(),
                config.reset_offset_in_exclusive_mode(),
                config.reset_offset_on_unsubscribe(),
                config.priority_factor(),
            ),
            (128, true, 256, true, true, 25)
        );

        config.set_attributes(HashMap::from([
            (
                CheetahString::from_static_str(LITE_SUB_CLIENT_QUOTA_ATTRIBUTE_NAME),
                CheetahString::from_static_str("invalid"),
            ),
            (
                CheetahString::from_static_str(LITE_SUB_CLIENT_MAX_EVENT_COUNT_ATTRIBUTE_NAME),
                CheetahString::from_static_str("invalid"),
            ),
            (
                CheetahString::from_static_str(PRIORITY_FACTOR_ATTRIBUTE_NAME),
                CheetahString::from_static_str("invalid"),
            ),
        ]));
        assert_eq!(
            (
                config.lite_sub_client_quota(),
                config.max_client_event_count(),
                config.priority_factor(),
            ),
            (2000, -1, 100)
        );
    }

    #[test]
    fn wildcard_lite_group_uses_attribute_presence_and_can_be_disabled() {
        let mut config = SubscriptionGroupConfig::default();
        config.set_attributes(HashMap::from([(
            CheetahString::from_static_str(LITE_SUB_WILDCARD_ATTRIBUTE_NAME),
            CheetahString::from_static_str("false"),
        )]));

        assert!(config.lite_sub_wildcard());

        config.set_lite_sub_wildcard(false);
        assert!(!config.lite_sub_wildcard());
        config.set_lite_sub_wildcard(true);
        assert!(config.lite_sub_wildcard());
    }
}
