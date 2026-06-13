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

use std::fmt;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::consistenthash::ConsistentHashRouter;
use rocketmq_common::common::consistenthash::HashFunction;
use rocketmq_common::common::consistenthash::Node;
use rocketmq_common::common::message::message_queue::MessageQueue;

use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::rebalance_strategy::check;

#[derive(Clone)]
pub struct AllocateMessageQueueConsistentHash {
    virtual_node_count: i32,
    hash_function: Option<Arc<dyn HashFunction + Send + Sync>>,
}

impl fmt::Debug for AllocateMessageQueueConsistentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AllocateMessageQueueConsistentHash")
            .field("virtual_node_count", &self.virtual_node_count)
            .field("custom_hash_function", &self.hash_function.is_some())
            .finish()
    }
}

impl Default for AllocateMessageQueueConsistentHash {
    fn default() -> Self {
        Self::new(10)
    }
}

impl AllocateMessageQueueConsistentHash {
    pub fn new(virtual_node_count: i32) -> Self {
        Self {
            virtual_node_count,
            hash_function: None,
        }
    }

    pub fn try_new(virtual_node_count: i32) -> rocketmq_error::RocketMQResult<Self> {
        if virtual_node_count < 0 {
            return Err(mq_client_err!(format!("illegal virtualNodeCnt :{virtual_node_count}")));
        }
        Ok(Self::new(virtual_node_count))
    }

    pub fn with_hash_function(
        virtual_node_count: i32,
        hash_function: Arc<dyn HashFunction + Send + Sync>,
    ) -> rocketmq_error::RocketMQResult<Self> {
        if virtual_node_count < 0 {
            return Err(mq_client_err!(format!("illegal virtualNodeCnt :{virtual_node_count}")));
        }
        Ok(Self {
            virtual_node_count,
            hash_function: Some(hash_function),
        })
    }
}

#[derive(Clone)]
struct ClientNode {
    client_id: CheetahString,
}

impl Node for ClientNode {
    fn get_key(&self) -> &str {
        self.client_id.as_str()
    }
}

impl AllocateMessageQueueStrategy for AllocateMessageQueueConsistentHash {
    fn allocate(
        &self,
        consumer_group: &CheetahString,
        current_cid: &CheetahString,
        mq_all: &[MessageQueue],
        cid_all: &[CheetahString],
    ) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>> {
        if !check(consumer_group, current_cid, mq_all, cid_all)? {
            return Ok(Vec::new());
        }
        if self.virtual_node_count < 0 {
            return Err(mq_client_err!(format!(
                "illegal virtualNodeCnt :{}",
                self.virtual_node_count
            )));
        }

        let nodes = cid_all
            .iter()
            .cloned()
            .map(|client_id| ClientNode { client_id })
            .collect();
        let router = if let Some(hash_function) = &self.hash_function {
            ConsistentHashRouter::try_new_with_hash_function(nodes, self.virtual_node_count, hash_function.clone())?
        } else {
            ConsistentHashRouter::try_new(nodes, self.virtual_node_count)?
        };

        let mut result = Vec::with_capacity(mq_all.len().div_ceil(cid_all.len()));
        for mq in mq_all {
            if let Some(client_node) = router.route_node_ref(&mq.to_string()) {
                if client_node.client_id == *current_cid {
                    result.push(mq.clone());
                }
            }
        }

        Ok(result)
    }

    fn get_name(&self) -> &'static str {
        "CONSISTENT_HASH"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct QueueIdHash;

    impl HashFunction for QueueIdHash {
        fn hash(&self, key: &str) -> i64 {
            if key.starts_with("cid-a-") {
                return 100;
            }
            if key.starts_with("cid-b-") {
                return 200;
            }
            if key.contains("queueId=0") {
                return 50;
            }
            if key.contains("queueId=1") {
                return 150;
            }
            250
        }
    }

    #[test]
    fn allocate_consistent_hash_is_stable_for_same_inputs() {
        let strategy = AllocateMessageQueueConsistentHash::default();
        let group = CheetahString::from("group");
        let cid_all = vec![
            CheetahString::from("cid-a"),
            CheetahString::from("cid-b"),
            CheetahString::from("cid-c"),
        ];
        let mq_all = (0..12)
            .map(|queue_id| MessageQueue::from_parts("topic", "broker-a", queue_id))
            .collect::<Vec<_>>();

        let first = strategy
            .allocate(&group, &cid_all[0], &mq_all, &cid_all)
            .expect("first allocation");
        let second = strategy
            .allocate(&group, &cid_all[0], &mq_all, &cid_all)
            .expect("second allocation");

        assert_eq!(first, second);
        assert_eq!(strategy.get_name(), "CONSISTENT_HASH");
    }

    #[test]
    fn allocate_consistent_hash_uses_custom_hash_function_like_java() {
        let strategy = AllocateMessageQueueConsistentHash::with_hash_function(1, Arc::new(QueueIdHash))
            .expect("custom hash function should build");
        let group = CheetahString::from("group");
        let cid_all = vec![CheetahString::from("cid-a"), CheetahString::from("cid-b")];
        let mq_all = (0..3)
            .map(|queue_id| MessageQueue::from_parts("topic", "broker-a", queue_id))
            .collect::<Vec<_>>();

        let cid_a_queues = strategy
            .allocate(&group, &cid_all[0], &mq_all, &cid_all)
            .expect("custom hash allocation should succeed");
        let cid_b_queues = strategy
            .allocate(&group, &cid_all[1], &mq_all, &cid_all)
            .expect("custom hash allocation should succeed");

        assert_eq!(
            cid_a_queues,
            vec![
                MessageQueue::from_parts("topic", "broker-a", 0),
                MessageQueue::from_parts("topic", "broker-a", 2)
            ]
        );
        assert_eq!(cid_b_queues, vec![MessageQueue::from_parts("topic", "broker-a", 1)]);
    }

    #[test]
    fn allocate_consistent_hash_rejects_negative_virtual_nodes() {
        let strategy = AllocateMessageQueueConsistentHash::new(-1);
        let group = CheetahString::from("group");
        let cid_all = vec![CheetahString::from("cid-a")];
        let mq_all = vec![MessageQueue::from_parts("topic", "broker-a", 0)];

        let result = strategy.allocate(&group, &cid_all[0], &mq_all, &cid_all);

        assert!(result.is_err());
    }

    #[test]
    fn allocate_consistent_hash_try_constructors_reject_negative_virtual_nodes_like_java() {
        let result = AllocateMessageQueueConsistentHash::try_new(-1);
        assert!(result
            .err()
            .is_some_and(|error| error.to_string().contains("illegal virtualNodeCnt :-1")));

        let result = AllocateMessageQueueConsistentHash::with_hash_function(-1, Arc::new(QueueIdHash));
        assert!(result
            .err()
            .is_some_and(|error| error.to_string().contains("illegal virtualNodeCnt :-1")));
    }
}
