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

use std::sync::LazyLock;

use cheetah_string::CheetahString;
use dashmap::DashSet;

pub const TOPIC_MAX_LENGTH: usize = 127;

/// Pre-computed valid character bitmap for topic/group name validation
/// Allows: 0-9, a-z, A-Z, %, -, _, |
const VALID_CHAR_BIT_MAP: [bool; 128] = {
    let mut map = [false; 128];
    map['%' as usize] = true;
    map['-' as usize] = true;
    map['_' as usize] = true;
    map['|' as usize] = true;

    let mut i = b'0';
    while i <= b'9' {
        map[i as usize] = true;
        i += 1;
    }

    let mut i = b'A';
    while i <= b'Z' {
        map[i as usize] = true;
        i += 1;
    }

    let mut i = b'a';
    while i <= b'z' {
        map[i as usize] = true;
        i += 1;
    }
    map
};

static SYSTEM_TOPIC_SET: LazyLock<DashSet<CheetahString>> = LazyLock::new(|| {
    let set = DashSet::new();
    set.insert(CheetahString::from_static_str(
        TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC,
    ));
    set.insert(CheetahString::from_static_str(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC));
    set.insert(CheetahString::from_static_str(TopicValidator::RMQ_SYS_BENCHMARK_TOPIC));
    set.insert(CheetahString::from_static_str(TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC));
    set.insert(CheetahString::from_static_str(TopicValidator::RMQ_SYS_TRACE_TOPIC));
    set.insert(CheetahString::from_static_str(
        TopicValidator::RMQ_SYS_TRANS_OP_HALF_TOPIC,
    ));
    set.insert(CheetahString::from_static_str(
        TopicValidator::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC,
    ));
    set.insert(CheetahString::from_static_str(TopicValidator::RMQ_SYS_SELF_TEST_TOPIC));
    set.insert(CheetahString::from_static_str(
        TopicValidator::RMQ_SYS_OFFSET_MOVED_EVENT,
    ));
    set.insert(CheetahString::from_static_str(
        TopicValidator::RMQ_SYS_ROCKSDB_OFFSET_TOPIC,
    ));
    set
});

static NOT_ALLOWED_SEND_TOPIC_SET: LazyLock<DashSet<CheetahString>> = LazyLock::new(|| {
    let set = DashSet::new();
    set.insert(CheetahString::from_static_str(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC));
    set.insert(CheetahString::from_static_str(TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC));
    set.insert(CheetahString::from_static_str(
        TopicValidator::RMQ_SYS_TRANS_OP_HALF_TOPIC,
    ));
    set.insert(CheetahString::from_static_str(
        TopicValidator::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC,
    ));
    set.insert(CheetahString::from_static_str(TopicValidator::RMQ_SYS_SELF_TEST_TOPIC));
    set.insert(CheetahString::from_static_str(
        TopicValidator::RMQ_SYS_OFFSET_MOVED_EVENT,
    ));
    set
});

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
    /// Check if topic or group name contains illegal characters
    ///
    /// # Performance
    /// Uses pre-computed bitmap for O(1) character validation
    #[inline]
    pub fn is_topic_or_group_illegal(name: &str) -> bool {
        name.bytes()
            .any(|b| (b as usize) >= VALID_CHAR_BIT_MAP.len() || !VALID_CHAR_BIT_MAP[b as usize])
    }

    /// Validate topic name according to RocketMQ rules
    ///
    /// # Rules
    /// - Cannot be blank
    /// - Must contain only: 0-9, a-z, A-Z, %, -, _, |
    /// - Length must not exceed TOPIC_MAX_LENGTH (127)
    #[inline]
    pub fn validate_topic(topic: &str) -> ValidateTopicResult {
        // Fast path: check length first (cheaper than trim)
        if topic.is_empty() || topic.len() > TOPIC_MAX_LENGTH {
            if topic.is_empty() || topic.trim().is_empty() {
                const REMARK: &str = "The specified topic is blank.";
                return ValidateTopicResult {
                    valid: false,
                    remark: CheetahString::from_static_str(REMARK),
                };
            }
            return ValidateTopicResult {
                valid: false,
                remark: CheetahString::from(format!(
                    "The specified topic is longer than topic max length {TOPIC_MAX_LENGTH}."
                )),
            };
        }

        if Self::is_topic_or_group_illegal(topic) {
            const REMARK: &str = "The specified topic contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$";
            return ValidateTopicResult {
                valid: false,
                remark: CheetahString::from_static_str(REMARK),
            };
        }

        ValidateTopicResult {
            valid: true,
            remark: CheetahString::empty(),
        }
    }

    #[inline]
    pub fn is_system_topic(topic: &str) -> bool {
        // Fast path: check prefix first (no lock needed)
        if topic.starts_with(TopicValidator::SYSTEM_TOPIC_PREFIX) {
            return true;
        }
        // DashSet::contains is lock-free for reads
        SYSTEM_TOPIC_SET.iter().any(|entry| entry.as_str() == topic)
    }

    #[inline]
    pub fn is_not_allowed_send_topic(topic: &str) -> bool {
        // DashSet iteration is lock-free
        NOT_ALLOWED_SEND_TOPIC_SET.iter().any(|entry| entry.as_str() == topic)
    }

    pub fn add_system_topic(system_topic: impl Into<CheetahString>) {
        SYSTEM_TOPIC_SET.insert(system_topic.into());
    }

    pub fn get_system_topic_set() -> Vec<CheetahString> {
        SYSTEM_TOPIC_SET.iter().map(|entry| entry.clone()).collect()
    }

    pub fn get_not_allowed_send_topic_set() -> Vec<CheetahString> {
        NOT_ALLOWED_SEND_TOPIC_SET.iter().map(|entry| entry.clone()).collect()
    }
}

pub struct ValidateTopicResult {
    valid: bool,
    remark: CheetahString,
}

impl ValidateTopicResult {
    #[inline]
    pub fn valid(&self) -> bool {
        self.valid
    }

    #[inline]
    pub fn remark(&self) -> &CheetahString {
        &self.remark
    }

    #[inline]
    pub fn take_remark(self) -> CheetahString {
        self.remark
    }

    #[inline]
    pub fn set_valid(&mut self, valid: bool) {
        self.valid = valid;
    }

    #[inline]
    pub fn set_remark(&mut self, remark: CheetahString) {
        self.remark = remark;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validate_topic_with_valid_topic() {
        let result = TopicValidator::validate_topic("valid_topic");
        assert!(result.valid());
        assert_eq!(result.remark(), "");
    }

    #[test]
    fn validate_topic_with_empty_topic() {
        let result = TopicValidator::validate_topic("");
        assert!(!result.valid());
        assert_eq!(result.remark(), "The specified topic is blank.");
    }

    #[test]
    fn validate_topic_with_illegal_characters() {
        let result = TopicValidator::validate_topic("invalid@topic");
        assert!(!result.valid());
        assert_eq!(
            result.remark(),
            "The specified topic contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$"
        );
    }

    #[test]
    fn validate_topic_with_exceeding_length() {
        let long_topic = "a".repeat(TOPIC_MAX_LENGTH + 1);
        let result = TopicValidator::validate_topic(&long_topic);
        assert!(!result.valid());
        assert_eq!(
            result.remark().as_str(),
            format!(
                "The specified topic is longer than topic max length {}.",
                TOPIC_MAX_LENGTH
            )
        );
    }

    #[test]
    fn is_system_topic_with_system_topic() {
        assert!(TopicValidator::is_system_topic(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC));
    }

    #[test]
    fn is_system_topic_with_non_system_topic() {
        assert!(!TopicValidator::is_system_topic("non_system_topic"));
    }

    #[test]
    fn is_not_allowed_send_topic_with_not_allowed_topic() {
        assert!(TopicValidator::is_not_allowed_send_topic(
            TopicValidator::RMQ_SYS_SCHEDULE_TOPIC
        ));
    }

    #[test]
    fn is_not_allowed_send_topic_with_allowed_topic() {
        assert!(!TopicValidator::is_not_allowed_send_topic("allowed_topic"));
    }

    #[test]
    fn add_system_topic_adds_new_topic() {
        TopicValidator::add_system_topic("new_system_topic");
        assert!(TopicValidator::is_system_topic("new_system_topic"));
    }

    #[test]
    fn get_system_topic_set_returns_all_system_topics() {
        let system_topics = TopicValidator::get_system_topic_set();
        assert!(system_topics
            .iter()
            .any(|s| s.as_str() == TopicValidator::RMQ_SYS_SCHEDULE_TOPIC));
    }

    #[test]
    fn get_not_allowed_send_topic_set_returns_all_not_allowed_topics() {
        let not_allowed_topics = TopicValidator::get_not_allowed_send_topic_set();
        assert!(not_allowed_topics
            .iter()
            .any(|s| s.as_str() == TopicValidator::RMQ_SYS_SCHEDULE_TOPIC));
    }
}
