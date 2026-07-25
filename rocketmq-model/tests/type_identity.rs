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

use rocketmq_model::common::attribute::topic_message_type::TopicMessageType as CommonTopicMessageType;
use rocketmq_model::common::config::TopicConfig as CommonTopicConfig;
use rocketmq_model::common::consistenthash::ConsistentHashRouter as CommonConsistentHashRouter;
use rocketmq_model::common::consistenthash::Node as CommonNode;
use rocketmq_model::common::message::message_queue::MessageQueue as CommonMessageQueue;
use rocketmq_model::common::TopicFilterType as CommonTopicFilterType;
use rocketmq_model::consistent_hash::ConsistentHashRouter;
use rocketmq_model::consistent_hash::Node;
use rocketmq_model::message::MessageQueue;
use rocketmq_model::topic::TopicConfig;
use rocketmq_model::topic::TopicFilterType;
use rocketmq_model::topic::TopicMessageType;

#[test]
fn common_paths_are_the_canonical_model_types() {
    fn queue_identity(value: MessageQueue) -> CommonMessageQueue {
        value
    }
    fn config_identity(value: TopicConfig) -> CommonTopicConfig {
        value
    }
    fn filter_identity(value: TopicFilterType) -> CommonTopicFilterType {
        value
    }
    fn message_type_identity(value: TopicMessageType) -> CommonTopicMessageType {
        value
    }

    assert_eq!(queue_identity(MessageQueue::default()), CommonMessageQueue::default());
    assert_eq!(config_identity(TopicConfig::default()), CommonTopicConfig::default());
    assert_eq!(
        filter_identity(TopicFilterType::MultiTag),
        CommonTopicFilterType::MultiTag
    );
    assert_eq!(
        message_type_identity(TopicMessageType::Transaction),
        CommonTopicMessageType::Transaction
    );
}

#[derive(Clone)]
struct TestNode(String);

impl Node for TestNode {
    fn get_key(&self) -> &str {
        &self.0
    }
}

fn assert_common_node<T: CommonNode>(_value: &T) {}

#[test]
fn common_consistent_hash_path_is_the_canonical_model_type() {
    fn identity(value: ConsistentHashRouter<TestNode>) -> CommonConsistentHashRouter<TestNode> {
        value
    }

    let node = TestNode("node-a".to_owned());
    assert_common_node(&node);
    assert_eq!(identity(ConsistentHashRouter::new(vec![node], 2)).size(), 2);
}
