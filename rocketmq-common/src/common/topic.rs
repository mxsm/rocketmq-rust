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

use std::collections::HashSet;
use std::sync::Mutex;

use lazy_static::lazy_static;

pub const TOPIC_MAX_LENGTH: usize = 127;

lazy_static! {
    static ref VALID_CHAR_BIT_MAP: [bool; 128] = {
        let mut map = [false; 128];
        map['%' as usize] = true;
        map['-' as usize] = true;
        map['_' as usize] = true;
        map['|' as usize] = true;
        for i in b'0'..=b'9' {
            map[i as usize] = true;
        }
        for i in b'A'..=b'Z' {
            map[i as usize] = true;
        }
        for i in b'a'..=b'z' {
            map[i as usize] = true;
        }
        map
    };
    static ref SYSTEM_TOPIC_SET: Mutex<HashSet<&'static str>> = {
        let mut set = HashSet::new();
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
        Mutex::new(set)
    };
    static ref NOT_ALLOWED_SEND_TOPIC_SET: Mutex<HashSet<&'static str>> = {
        let mut set = HashSet::new();
        set.insert(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC);
        set.insert(TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC);
        set.insert(TopicValidator::RMQ_SYS_TRANS_OP_HALF_TOPIC);
        set.insert(TopicValidator::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
        set.insert(TopicValidator::RMQ_SYS_SELF_TEST_TOPIC);
        set.insert(TopicValidator::RMQ_SYS_OFFSET_MOVED_EVENT);
        Mutex::new(set)
    };
}

pub struct TopicValidator;

impl TopicValidator {
    pub const AUTO_CREATE_TOPIC_KEY_TOPIC: &'static str = "TBW102";
    pub const RMQ_SYS_SCHEDULE_TOPIC: &'static str = "SCHEDULE_TOPIC_XXXX";
    pub const RMQ_SYS_BENCHMARK_TOPIC: &'static str = "BenchmarkTest";
    pub const RMQ_SYS_TRANS_HALF_TOPIC: &'static str = "RMQ_SYS_TRANS_HALF_TOPIC";
    pub const RMQ_SYS_TRACE_TOPIC: &'static str = "RMQ_SYS_TRACE_TOPIC";
    pub const RMQ_SYS_TRANS_OP_HALF_TOPIC: &'static str = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
    pub const RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC: &'static str = "TRANS_CHECK_MAX_TIME_TOPIC";
    pub const RMQ_SYS_SELF_TEST_TOPIC: &'static str = "SELF_TEST_TOPIC";
    pub const RMQ_SYS_OFFSET_MOVED_EVENT: &'static str = "OFFSET_MOVED_EVENT";
    pub const RMQ_SYS_ROCKSDB_OFFSET_TOPIC: &'static str = "CHECKPOINT_TOPIC";

    pub const SYSTEM_TOPIC_PREFIX: &'static str = "rmq_sys_";
    pub const SYNC_BROKER_MEMBER_GROUP_PREFIX: &'static str = "rmq_sys_SYNC_BROKER_MEMBER_";
}

impl TopicValidator {
    pub fn is_topic_or_group_illegal(name: &str) -> bool {
        let len = VALID_CHAR_BIT_MAP.len();
        for ch in name.chars() {
            if (ch as usize) >= len || !VALID_CHAR_BIT_MAP[ch as usize] {
                return true;
            }
        }
        false
    }

    pub fn validate_topic(topic: &str) -> ValidateTopicResult {
        if topic.trim().is_empty() {
            return ValidateTopicResult {
                valid: false,
                remark: String::from("The specified topic is blank."),
            };
        }

        if Self::is_topic_or_group_illegal(topic) {
            return ValidateTopicResult {
                valid: false,
                remark: String::from(
                    "The specified topic contains illegal characters, allowing only \
                     ^[%|a-zA-Z0-9_-]+$",
                ),
            };
        }

        if topic.len() > TOPIC_MAX_LENGTH {
            return ValidateTopicResult {
                valid: false,
                remark: format!(
                    "The specified topic is longer than topic max length {}.",
                    TOPIC_MAX_LENGTH
                ),
            };
        }

        ValidateTopicResult {
            valid: true,
            remark: String::new(),
        }
    }

    pub fn is_system_topic(topic: &str) -> bool {
        let system_topics = SYSTEM_TOPIC_SET.lock().unwrap();
        system_topics.contains(topic) || topic.starts_with(TopicValidator::SYSTEM_TOPIC_PREFIX)
    }

    pub fn is_not_allowed_send_topic(topic: &str) -> bool {
        let not_allowed_topics = NOT_ALLOWED_SEND_TOPIC_SET.lock().unwrap();
        not_allowed_topics.contains(topic)
    }

    pub fn add_system_topic(system_topic: &'static str) {
        let mut system_topics = SYSTEM_TOPIC_SET.lock().unwrap();
        system_topics.insert(system_topic);
    }

    pub fn get_system_topic_set() -> HashSet<&'static str> {
        SYSTEM_TOPIC_SET.lock().unwrap().clone()
    }

    pub fn get_not_allowed_send_topic_set() -> HashSet<&'static str> {
        NOT_ALLOWED_SEND_TOPIC_SET.lock().unwrap().clone()
    }
}

pub struct ValidateTopicResult {
    valid: bool,
    remark: String,
}

impl ValidateTopicResult {
    pub fn valid(&self) -> bool {
        self.valid
    }
    pub fn remark(&self) -> &str {
        &self.remark
    }

    pub fn set_valid(&mut self, valid: bool) {
        self.valid = valid;
    }
    pub fn set_remark(&mut self, remark: String) {
        self.remark = remark;
    }
}
