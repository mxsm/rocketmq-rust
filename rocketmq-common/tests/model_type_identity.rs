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

use rocketmq_common::common::attribute::topic_message_type::TopicMessageType as LegacyTopicMessageType;
use rocketmq_common::common::config::TopicConfig as LegacyTopicConfig;
use rocketmq_common::common::consistenthash::ConsistentHashRouter as LegacyConsistentHashRouter;
use rocketmq_common::common::consistenthash::Node as LegacyNode;
use rocketmq_common::common::message::message_queue::MessageQueue as LegacyMessageQueue;
use rocketmq_common::common::TopicFilterType as LegacyTopicFilterType;
use rocketmq_model::consistent_hash::ConsistentHashRouter;
use rocketmq_model::consistent_hash::Node;
use rocketmq_model::message::MessageQueue;
use rocketmq_model::topic::TopicConfig;
use rocketmq_model::topic::TopicFilterType;
use rocketmq_model::topic::TopicMessageType;

#[test]
fn legacy_paths_are_the_canonical_model_types() {
    fn queue_identity(value: MessageQueue) -> LegacyMessageQueue {
        value
    }
    fn config_identity(value: TopicConfig) -> LegacyTopicConfig {
        value
    }
    fn filter_identity(value: TopicFilterType) -> LegacyTopicFilterType {
        value
    }
    fn message_type_identity(value: TopicMessageType) -> LegacyTopicMessageType {
        value
    }

    assert_eq!(queue_identity(MessageQueue::default()), LegacyMessageQueue::default());
    assert_eq!(config_identity(TopicConfig::default()), LegacyTopicConfig::default());
    assert_eq!(
        filter_identity(TopicFilterType::MultiTag),
        LegacyTopicFilterType::MultiTag
    );
    assert_eq!(
        message_type_identity(TopicMessageType::Transaction),
        LegacyTopicMessageType::Transaction
    );
}

#[derive(Clone)]
struct TestNode(String);

impl Node for TestNode {
    fn get_key(&self) -> &str {
        &self.0
    }
}

fn assert_legacy_node<T: LegacyNode>(_value: &T) {}

#[test]
fn legacy_consistent_hash_path_is_the_canonical_model_type() {
    fn identity(value: ConsistentHashRouter<TestNode>) -> LegacyConsistentHashRouter<TestNode> {
        value
    }

    let node = TestNode("node-a".to_owned());
    assert_legacy_node(&node);
    assert_eq!(identity(ConsistentHashRouter::new(vec![node], 2)).size(), 2);
}
