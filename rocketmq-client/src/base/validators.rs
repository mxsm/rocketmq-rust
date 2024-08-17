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

use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_remoting::code::response_code::ResponseCode;

use crate::error::MQClientError::MQClientException;
use crate::producer::default_mq_producer::ProducerConfig;
use crate::Result;

pub struct Validators;

impl Validators {
    pub const CHARACTER_MAX_LENGTH: usize = 255;
    pub const TOPIC_MAX_LENGTH: usize = 127;

    pub fn check_group(group: &str) -> Result<()> {
        if group.trim().is_empty() {
            return Err(MQClientException(
                -1,
                "the specified group is blank".to_string(),
            ));
        }

        if group.len() > Self::CHARACTER_MAX_LENGTH {
            return Err(MQClientException(
                -1,
                "the specified group is longer than group max length 255.".to_string(),
            ));
        }

        if TopicValidator::is_topic_or_group_illegal(group) {
            return Err(MQClientException(
                -1,
                format!(
                    "the specified group[{}] contains illegal characters, allowing only \
                     ^[%|a-zA-Z0-9_-]+$",
                    group
                ),
            ));
        }

        Ok(())
    }

    pub fn check_message<M>(msg: Option<&M>, producer_config: &ProducerConfig) -> Result<()>
    where
        M: MessageTrait,
    {
        if msg.is_none() {
            return Err(MQClientException(
                ResponseCode::MessageIllegal as i32,
                "the message is null".to_string(),
            ));
        }
        let msg = msg.unwrap();
        Self::check_topic(msg.get_topic())?;
        Self::is_not_allowed_send_topic(msg.get_topic())?;

        if msg.get_body().is_none() {
            return Err(MQClientException(
                ResponseCode::MessageIllegal as i32,
                "the message body is null".to_string(),
            ));
        }

        let length = msg.get_body().unwrap().len();
        if length == 0 {
            return Err(MQClientException(
                ResponseCode::MessageIllegal as i32,
                "the message body length is zero".to_string(),
            ));
        }

        if length > producer_config.max_message_size() as usize {
            return Err(MQClientException(
                ResponseCode::MessageIllegal as i32,
                format!(
                    "the message body size over max value, MAX: {}",
                    producer_config.max_message_size()
                ),
            ));
        }

        let lmq_path = msg.get_user_property(MessageConst::PROPERTY_INNER_MULTI_DISPATCH);
        if let Some(value) = lmq_path {
            if value.contains(std::path::MAIN_SEPARATOR) {
                return Err(MQClientException(
                    ResponseCode::MessageIllegal as i32,
                    format!(
                        "INNER_MULTI_DISPATCH {} can not contains {} character",
                        value,
                        std::path::MAIN_SEPARATOR
                    ),
                ));
            }
        }

        Ok(())
    }

    pub fn check_topic(topic: &str) -> Result<()> {
        if topic.trim().is_empty() {
            return Err(MQClientException(
                -1,
                "The specified topic is blank".to_string(),
            ));
        }

        if topic.len() > Self::TOPIC_MAX_LENGTH {
            return Err(MQClientException(
                -1,
                format!(
                    "The specified topic is longer than topic max length {}.",
                    Self::TOPIC_MAX_LENGTH
                ),
            ));
        }

        if TopicValidator::is_topic_or_group_illegal(topic) {
            return Err(MQClientException(
                -1,
                format!(
                    "The specified topic[{}] contains illegal characters, allowing only \
                     ^[%|a-zA-Z0-9_-]+$",
                    topic
                ),
            ));
        }

        Ok(())
    }

    pub fn is_system_topic(topic: &str) -> Result<()> {
        if TopicValidator::is_system_topic(topic) {
            return Err(MQClientException(
                -1,
                format!("The topic[{}] is conflict with system topic.", topic),
            ));
        }
        Ok(())
    }

    pub fn is_not_allowed_send_topic(topic: &str) -> Result<()> {
        if TopicValidator::is_not_allowed_send_topic(topic) {
            return Err(MQClientException(
                -1,
                format!("Sending message to topic[{}] is forbidden.", topic),
            ));
        }

        Ok(())
    }

    pub fn check_topic_config(topic_config: &TopicConfig) -> Result<()> {
        if !PermName::is_valid(topic_config.perm) {
            return Err(MQClientException(
                ResponseCode::NoPermission as i32,
                format!("topicPermission value: {} is invalid.", topic_config.perm),
            ));
        }

        Ok(())
    }

    pub fn check_broker_config(broker_config: &HashMap<String, String>) -> Result<()> {
        if let Some(broker_permission) = broker_config.get("brokerPermission") {
            if !PermName::is_valid(broker_permission.parse().unwrap()) {
                return Err(MQClientException(
                    -1,
                    format!("brokerPermission value: {} is invalid.", broker_permission),
                ));
            }
        }

        Ok(())
    }
}
