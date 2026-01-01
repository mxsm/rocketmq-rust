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
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_remoting::code::response_code::ResponseCode;

use crate::producer::default_mq_producer::ProducerConfig;

pub struct Validators;

impl Validators {
    pub const CHARACTER_MAX_LENGTH: usize = 255;
    pub const TOPIC_MAX_LENGTH: usize = 127;

    pub fn check_group(group: &str) -> rocketmq_error::RocketMQResult<()> {
        if group.trim().is_empty() {
            return Err(mq_client_err!("the specified group is blank"));
        }

        if group.len() > Self::CHARACTER_MAX_LENGTH {
            return Err(mq_client_err!(
                "the specified group is longer than group max length 255."
            ));
        }

        if TopicValidator::is_topic_or_group_illegal(group) {
            return Err(mq_client_err!(format!(
                "the specified group[{}] contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$",
                group
            )));
        }
        Ok(())
    }

    pub fn check_message<M>(msg: Option<&M>, producer_config: &ProducerConfig) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait,
    {
        if msg.is_none() {
            return Err(mq_client_err!(
                ResponseCode::MessageIllegal as i32,
                "the message is null".to_string()
            ));
        }
        let msg = msg.unwrap();
        Self::check_topic(msg.get_topic())?;
        Self::is_not_allowed_send_topic(msg.get_topic())?;

        if msg.get_body().is_none() {
            return Err(mq_client_err!(
                ResponseCode::MessageIllegal as i32,
                "the message body is null".to_string()
            ));
        }

        let length = msg.get_body().unwrap().len();
        if length == 0 {
            return Err(mq_client_err!(
                ResponseCode::MessageIllegal as i32,
                "the message body length is zero".to_string()
            ));
        }

        if length > producer_config.max_message_size() as usize {
            return Err(mq_client_err!(
                ResponseCode::MessageIllegal as i32,
                format!(
                    "the message body size over max value, MAX: {}",
                    producer_config.max_message_size()
                )
            ));
        }

        let lmq_path = msg.get_user_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_INNER_MULTI_DISPATCH,
        ));
        if let Some(value) = lmq_path {
            if value.contains(std::path::MAIN_SEPARATOR) {
                return Err(mq_client_err!(
                    ResponseCode::MessageIllegal as i32,
                    format!(
                        "INNER_MULTI_DISPATCH {} can not contains {} character",
                        value,
                        std::path::MAIN_SEPARATOR
                    )
                ));
            }
        }

        Ok(())
    }

    pub fn check_topic(topic: &str) -> rocketmq_error::RocketMQResult<()> {
        if topic.trim().is_empty() {
            return Err(mq_client_err!("The specified topic is blank"));
        }

        if topic.len() > Self::TOPIC_MAX_LENGTH {
            return Err(mq_client_err!(format!(
                "The specified topic is longer than topic max length {}.",
                Self::TOPIC_MAX_LENGTH
            )));
        }

        if TopicValidator::is_topic_or_group_illegal(topic) {
            return Err(mq_client_err!(format!(
                "The specified topic[{}] contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$",
                topic
            )));
        }

        Ok(())
    }

    pub fn is_system_topic(topic: &str) -> rocketmq_error::RocketMQResult<()> {
        if TopicValidator::is_system_topic(topic) {
            return Err(mq_client_err!(format!(
                "The topic[{}] is conflict with system topic.",
                topic
            )));
        }
        Ok(())
    }

    pub fn is_not_allowed_send_topic(topic: &str) -> rocketmq_error::RocketMQResult<()> {
        if TopicValidator::is_not_allowed_send_topic(topic) {
            return Err(mq_client_err!(format!(
                "Sending message to topic[{}] is forbidden.",
                topic
            )));
        }

        Ok(())
    }

    pub fn check_topic_config(topic_config: &TopicConfig) -> rocketmq_error::RocketMQResult<()> {
        if !PermName::is_valid(topic_config.perm) {
            return Err(mq_client_err!(
                ResponseCode::NoPermission as i32,
                format!("topicPermission value: {} is invalid.", topic_config.perm)
            ));
        }

        Ok(())
    }

    pub fn check_broker_config(broker_config: &HashMap<String, String>) -> rocketmq_error::RocketMQResult<()> {
        if let Some(broker_permission) = broker_config.get("brokerPermission") {
            if !PermName::is_valid(broker_permission.parse().unwrap()) {
                return Err(mq_client_err!(format!(
                    "brokerPermission value: {} is invalid.",
                    broker_permission
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rocketmq_common::common::config::TopicConfig;

    use super::*;

    #[test]
    fn check_group_blank_group() {
        let result = Validators::check_group("");
        assert!(result.is_err());
    }

    #[test]
    fn check_group_long_group() {
        let long_group = "a".repeat(256);
        let result = Validators::check_group(&long_group);
        assert!(result.is_err());
    }

    #[test]
    fn check_group_illegal_characters() {
        let result = Validators::check_group("illegal@group");
        assert!(result.is_err());
    }

    #[test]
    fn check_group_valid_group() {
        let result = Validators::check_group("valid_group");
        assert!(result.is_ok());
    }

    #[test]
    fn check_topic_blank_topic() {
        let result = Validators::check_topic("");
        assert!(result.is_err());
    }

    #[test]
    fn check_topic_long_topic() {
        let long_topic = "a".repeat(128);
        let result = Validators::check_topic(&long_topic);
        assert!(result.is_err());
    }

    #[test]
    fn check_topic_illegal_characters() {
        let result = Validators::check_topic("illegal@topic");
        assert!(result.is_err());
    }

    #[test]
    fn check_topic_valid_topic() {
        let result = Validators::check_topic("valid_topic");
        assert!(result.is_ok());
    }

    #[test]
    fn check_topic_config_invalid_permission() {
        let topic_config = TopicConfig {
            perm: 999,
            ..Default::default()
        };
        let result = Validators::check_topic_config(&topic_config);
        assert!(result.is_err());
    }

    #[test]
    fn check_topic_config_valid_permission() {
        let topic_config = TopicConfig {
            perm: 6,
            ..Default::default()
        };
        let result = Validators::check_topic_config(&topic_config);
        assert!(result.is_ok());
    }

    #[test]
    fn check_broker_config_invalid_permission() {
        let mut broker_config = HashMap::new();
        broker_config.insert("brokerPermission".to_string(), "999".to_string());
        let result = Validators::check_broker_config(&broker_config);
        assert!(result.is_err());
    }

    #[test]
    fn check_broker_config_valid_permission() {
        let mut broker_config = HashMap::new();
        broker_config.insert("brokerPermission".to_string(), "6".to_string());
        let result = Validators::check_broker_config(&broker_config);
        assert!(result.is_ok());
    }
}
