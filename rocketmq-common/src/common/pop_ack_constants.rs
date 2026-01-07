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

use crate::common::mix_all;
use crate::common::topic::TopicValidator;

pub struct PopAckConstants;

impl PopAckConstants {
    pub const ACK_TIME_INTERVAL: i64 = 1000;
    pub const SECOND: i64 = 1000;
    pub const LOCK_TIME: i64 = 5000;
    pub const RETRY_QUEUE_NUM: i32 = 1;
    pub const REVIVE_GROUP: &'static str = "CID_RMQ_SYS_REVIVE_GROUP";
    pub const LOCAL_HOST: &'static str = "127.0.0.1";
    pub const REVIVE_TOPIC: &'static str = "rmq_sys_REVIVE_LOG_";
    pub const CK_TAG: &'static str = "ck";
    pub const ACK_TAG: &'static str = "ack";
    pub const BATCH_ACK_TAG: &'static str = "bAck";
    pub const SPLIT: &'static str = "@";

    #[inline]
    pub fn build_cluster_revive_topic(cluster_name: &str) -> String {
        format!("{}{}", PopAckConstants::REVIVE_TOPIC, cluster_name)
    }

    #[inline]
    pub fn is_start_with_revive_prefix(topic_name: &str) -> bool {
        topic_name.starts_with(PopAckConstants::REVIVE_TOPIC)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_cluster_revive_topic_appends_correctly() {
        let cluster_name = "test_cluster";
        let expected = "rmq_sys_REVIVE_LOG_test_cluster";
        assert_eq!(PopAckConstants::build_cluster_revive_topic(cluster_name), expected);
    }

    #[test]
    fn is_start_with_revive_prefix_returns_true_for_valid_prefix() {
        let topic_name = "rmq_sys_REVIVE_LOG_test";
        assert!(PopAckConstants::is_start_with_revive_prefix(topic_name));
    }

    #[test]
    fn is_start_with_revive_prefix_returns_false_for_invalid_prefix() {
        let topic_name = "invalid_prefix_test";
        assert!(!PopAckConstants::is_start_with_revive_prefix(topic_name));
    }

    #[test]
    fn constants_have_correct_values() {
        assert_eq!(PopAckConstants::ACK_TIME_INTERVAL, 1000);
        assert_eq!(PopAckConstants::SECOND, 1000);
        assert_eq!(PopAckConstants::LOCK_TIME, 5000);
        assert_eq!(PopAckConstants::RETRY_QUEUE_NUM, 1);
        assert_eq!(PopAckConstants::REVIVE_GROUP, "CID_RMQ_SYS_REVIVE_GROUP");
        assert_eq!(PopAckConstants::LOCAL_HOST, "127.0.0.1");
        assert_eq!(PopAckConstants::REVIVE_TOPIC, "rmq_sys_REVIVE_LOG_");
        assert_eq!(PopAckConstants::CK_TAG, "ck");
        assert_eq!(PopAckConstants::ACK_TAG, "ack");
        assert_eq!(PopAckConstants::BATCH_ACK_TAG, "bAck");
        assert_eq!(PopAckConstants::SPLIT, "@");
    }
}
