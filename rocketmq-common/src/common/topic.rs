/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License; Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing; software
 * distributed under the License is distributed on an "AS IS" BASIS;
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND; either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use lazy_static::lazy_static;

/*const VALID_CHAR_BIT_MAP: [bool; 128] = {
    let mut bit_map = [false; 128];
    bit_map['%' as usize] = true;
    bit_map['-' as usize] = true;
    bit_map['_' as usize] = true;
    bit_map['|' as usize] = true;
    for i in '0' as u8..'9' as u8 + 1 {
        bit_map[i as usize] = true;
    }
    for i in 'A' as u8..'Z' as u8 + 1 {
        bit_map[i as usize] = true;
    }
    for i in 'a' as u8..'z' as u8 + 1 {
        bit_map[i as usize] = true;
    }
    bit_map
};*/

const TOPIC_MAX_LENGTH: usize = 127;

pub struct ValidateTopicResult {
    valid: bool,
    remark: String,
}

impl ValidateTopicResult {
    pub fn new(valid: bool, remark: impl Into<String>) -> Self {
        Self {
            valid,
            remark: remark.into(),
        }
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    pub fn remark(&self) -> &str {
        &self.remark
    }
}

pub struct TopicValidator;

lazy_static! {
    pub static ref SYSTEM_TOPIC_SET: std::collections::HashSet<&'static str> = {
        let mut set = std::collections::HashSet::new();
        set.insert(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC);
        set.insert(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC);
        set.insert(TopicValidator::RMQ_SYS_BENCHMARK_TOPIC);
        set.insert(TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC);
        set.insert(TopicValidator::RMQ_SYS_TRACE_TOPIC);
        set.insert(TopicValidator::RMQ_SYS_TRANS_OP_HALF_TOPIC);
        set.insert(TopicValidator::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
        set.insert(TopicValidator::RMQ_SYS_SELF_TEST_TOPIC);
        set.insert(TopicValidator::RMQ_SYS_OFFSET_MOVED_EVENT);
        set.insert(TopicValidator::RMQ_SYS_ROCKSDB_OFFSET_TOPIC);
        set
    };
    pub static ref NOT_ALLOWED_SEND_TOPIC_SET: std::collections::HashSet<&'static str> = {
        let mut set = std::collections::HashSet::new();

        set.insert(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC);
        set.insert(TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC);
        set.insert(TopicValidator::RMQ_SYS_TRANS_OP_HALF_TOPIC);
        set.insert(TopicValidator::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
        set.insert(TopicValidator::RMQ_SYS_SELF_TEST_TOPIC);
        set.insert(TopicValidator::RMQ_SYS_OFFSET_MOVED_EVENT);
        set
    };
}

impl TopicValidator {
    pub const AUTO_CREATE_TOPIC_KEY_TOPIC: &'static str = "TBW102";
    pub const RMQ_SYS_BENCHMARK_TOPIC: &'static str = "BenchmarkTest";
    pub const RMQ_SYS_OFFSET_MOVED_EVENT: &'static str = "OFFSET_MOVED_EVENT";
    pub const RMQ_SYS_ROCKSDB_OFFSET_TOPIC: &'static str = "CHECKPOINT_TOPIC";
    pub const RMQ_SYS_SCHEDULE_TOPIC: &'static str = "SCHEDULE_TOPIC_XXXX";
    pub const RMQ_SYS_SELF_TEST_TOPIC: &'static str = "SELF_TEST_TOPIC";
    pub const RMQ_SYS_TRACE_TOPIC: &'static str = "RMQ_SYS_TRACE_TOPIC";
    pub const RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC: &'static str = "TRANS_CHECK_MAX_TIME_TOPIC";
    pub const RMQ_SYS_TRANS_HALF_TOPIC: &'static str = "RMQ_SYS_TRANS_HALF_TOPIC";
    pub const RMQ_SYS_TRANS_OP_HALF_TOPIC: &'static str = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
    pub const SYNC_BROKER_MEMBER_GROUP_PREFIX: &'static str = "SYNC_BROKER_MEMBER_";
    pub const SYSTEM_TOPIC_PREFIX: &'static str = "rmq_sys_";

    pub fn is_system_topic(topic: &str) -> bool {
        SYSTEM_TOPIC_SET.contains(topic) || topic.starts_with(Self::SYSTEM_TOPIC_PREFIX)
    }

    pub fn is_topic_or_group_illegal(_topic: &str) -> bool {
        /*topic
        .chars()
        .any(|ch| ch as usize >= 128 || !VALID_CHAR_BIT_MAP[ch as usize])*/
        false
    }

    pub fn validate_topic(topic: &str) -> ValidateTopicResult {
        if topic.is_empty() {
            return ValidateTopicResult::new(false, "The specified topic is blank.");
        }

        if Self::is_topic_or_group_illegal(topic) {
            return ValidateTopicResult::new(
                false,
                "The specified topic contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$",
            );
        }

        if topic.len() > TOPIC_MAX_LENGTH {
            return ValidateTopicResult::new(
                false,
                "The specified topic is longer than topic max length.",
            );
        }

        ValidateTopicResult::new(true, "")
    }

    pub fn is_not_allowed_send_topic(topic: &str) -> bool {
        NOT_ALLOWED_SEND_TOPIC_SET.contains(topic)
    }

    /*    pub fn add_system_topic(&mut self, system_topic: &str) {
        self.system_topic_set.insert(system_topic.to_string());
    }*/
}
