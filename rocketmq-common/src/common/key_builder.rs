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

use crate::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use crate::common::pop_ack_constants::PopAckConstants;

pub const POP_ORDER_REVIVE_QUEUE: i32 = 999;
pub const POP_RETRY_SEPARATOR_V1: char = '_';
pub const POP_RETRY_SEPARATOR_V2: char = '+';
pub const POP_RETRY_REGEX_SEPARATOR_V2: &str = "\\+";

pub struct KeyBuilder;

impl KeyBuilder {
    pub fn build_pop_retry_topic(topic: &str, cid: &str, enable_retry_v2: bool) -> String {
        if enable_retry_v2 {
            return KeyBuilder::build_pop_retry_topic_v2(topic, cid);
        }
        KeyBuilder::build_pop_retry_topic_v1(topic, cid)
    }

    pub fn build_pop_retry_topic_default(topic: &str, cid: &str) -> String {
        format!("{RETRY_GROUP_TOPIC_PREFIX}{cid}{POP_RETRY_SEPARATOR_V1}{topic}")
    }

    pub fn build_pop_retry_topic_v2(topic: &str, cid: &str) -> String {
        format!("{RETRY_GROUP_TOPIC_PREFIX}{cid}{POP_RETRY_SEPARATOR_V2}{topic}")
    }

    pub fn build_pop_retry_topic_v1(topic: &str, cid: &str) -> String {
        format!("{RETRY_GROUP_TOPIC_PREFIX}{cid}{POP_RETRY_SEPARATOR_V1}{topic}")
    }

    pub fn parse_normal_topic(topic: &str, cid: &str) -> String {
        if topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
            if topic.starts_with(&format!("{RETRY_GROUP_TOPIC_PREFIX}{cid}{POP_RETRY_SEPARATOR_V2}")) {
                return topic[RETRY_GROUP_TOPIC_PREFIX.len() + cid.len() + 1..].to_string();
            }
            return topic[RETRY_GROUP_TOPIC_PREFIX.len() + cid.len() + 1..].to_string();
        }
        topic.to_string()
    }

    pub fn parse_normal_topic_default(retry_topic: &str) -> String {
        if KeyBuilder::is_pop_retry_topic_v2(retry_topic) {
            let result: Vec<&str> = retry_topic.split(POP_RETRY_REGEX_SEPARATOR_V2).collect();
            if result.len() == 2 {
                return result[1].to_string();
            }
        }
        retry_topic.to_string()
    }

    pub fn parse_group(retry_topic: &str) -> String {
        if KeyBuilder::is_pop_retry_topic_v2(retry_topic) {
            let result: Vec<&str> = retry_topic.split(POP_RETRY_REGEX_SEPARATOR_V2).collect();
            if result.len() == 2 {
                return result[0][RETRY_GROUP_TOPIC_PREFIX.len()..].to_string();
            }
        }
        retry_topic[RETRY_GROUP_TOPIC_PREFIX.len()..].to_string()
    }

    pub fn build_polling_key(topic: &str, cid: &str, queue_id: i32) -> String {
        format!(
            "{}{}{}{}{}",
            topic,
            PopAckConstants::SPLIT,
            cid,
            PopAckConstants::SPLIT,
            queue_id
        )
    }

    pub fn is_pop_retry_topic_v2(retry_topic: &str) -> bool {
        retry_topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) && retry_topic.contains(POP_RETRY_SEPARATOR_V2)
    }
}
