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

//! Integration tests for OpenRaft implementation

use std::collections::BTreeMap;
use std::sync::Arc;

use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::openraft::RaftNodeManager;
use rocketmq_controller::typ::ControllerRequest;
use rocketmq_controller::typ::Node;

#[tokio::test]
async fn test_node_creation() {
    // Create a test configuration
    let config = Arc::new(
        ControllerConfig::default()
            .with_node_info(1, "127.0.0.1:9876".parse().unwrap())
            .with_election_timeout_ms(1000)
            .with_heartbeat_interval_ms(300),
    );

    // Create a Raft node
    let node = RaftNodeManager::new(config).await;
    assert!(node.is_ok(), "Failed to create Raft node: {:?}", node.err());

    let _node = node.unwrap();
    // Note: Raft instance doesn't expose id() method directly
    // Node ID is verified through configuration
}

#[tokio::test]
async fn test_cluster_initialization() {
    // Create configuration for node 1
    let config = Arc::new(
        ControllerConfig::default()
            .with_node_info(1, "127.0.0.1:19876".parse().unwrap())
            .with_election_timeout_ms(1000)
            .with_heartbeat_interval_ms(300),
    );

    // Create the node
    let node = RaftNodeManager::new(config).await.unwrap();

    // Initialize a single-node cluster
    let mut nodes = BTreeMap::new();
    nodes.insert(
        1,
        Node {
            node_id: 1,
            rpc_addr: "127.0.0.1:19876".to_string(),
        },
    );

    let result = node.initialize_cluster(nodes).await;
    assert!(result.is_ok(), "Failed to initialize cluster: {:?}", result.err());

    // Wait a bit for election
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Check if this node becomes leader
    let is_leader = node.is_leader().await.unwrap();
    assert!(is_leader, "Node should be the leader in a single-node cluster");
}

#[tokio::test]
async fn test_log_store() {
    use openraft::storage::RaftLogReader;
    use openraft::storage::RaftLogStorage;
    use rocketmq_controller::openraft::LogStore;

    let mut store = LogStore::new();

    // Test new store is empty
    let log_state = store.get_log_state().await.unwrap();
    assert!(log_state.last_log_id.is_none());
    assert!(log_state.last_purged_log_id.is_none());

    // Test vote operations
    let vote = openraft::Vote::new(1, 1);
    store.save_vote(&vote).await.unwrap();

    let saved_vote = store.read_vote().await.unwrap();
    assert_eq!(saved_vote, Some(vote));
}

#[tokio::test]
async fn test_state_machine() {
    use rocketmq_controller::openraft::BrokerMetadata;
    use rocketmq_controller::openraft::StateMachine;
    use rocketmq_controller::openraft::TopicConfig;

    let sm = StateMachine::new();

    // Test broker operations
    let _broker = BrokerMetadata {
        broker_name: "test-broker".to_string(),
        cluster_name: "test-cluster".to_string(),
        broker_addr: "127.0.0.1:10911".to_string(),
        broker_id: 0,
        epoch: 0,
        max_offset: 0,
        election_priority: 0,
        last_heartbeat: 0,
    };

    // Note: StateMachine doesn't have direct register methods as it's managed by Raft
    // We can only verify getter methods
    let retrieved = sm.get_broker("test-broker");
    assert!(retrieved.is_none(), "Should not have broker before registration");

    // Test topic operations
    let _topic = TopicConfig {
        topic_name: "test-topic".to_string(),
        read_queue_nums: 4,
        write_queue_nums: 4,
        perm: 6,
    };

    // Similarly, topics are managed through Raft requests
    let retrieved = sm.get_topic("test-topic");
    assert!(retrieved.is_none(), "Should not have topic before creation");
}

#[tokio::test]
async fn test_storage_operations() {
    use openraft::storage::RaftLogStorage;
    use rocketmq_controller::openraft::Store;

    let store = Store::new();

    // Test log store access
    let mut log_store = store.log_store.clone();
    let log_state = log_store.get_log_state().await.unwrap();
    assert!(log_state.last_log_id.is_none());

    // Test state machine access
    let all_brokers = store.state_machine.get_all_brokers();
    assert_eq!(all_brokers.len(), 0);
}

#[tokio::test]
async fn test_client_write() {
    // Create configuration
    let config = Arc::new(
        ControllerConfig::default()
            .with_node_info(1, "127.0.0.1:29876".parse().unwrap())
            .with_election_timeout_ms(1000)
            .with_heartbeat_interval_ms(300),
    );

    // Create and initialize node
    let node = RaftNodeManager::new(config).await.unwrap();

    let mut nodes = BTreeMap::new();
    nodes.insert(
        1,
        Node {
            node_id: 1,
            rpc_addr: "127.0.0.1:29876".to_string(),
        },
    );

    node.initialize_cluster(nodes).await.unwrap();

    // Wait for leader election
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Try to write a request
    let request = ControllerRequest::RegisterBroker {
        cluster_name: "test-cluster".to_string(),
        broker_addr: "127.0.0.1:10911".to_string(),
        broker_name: "test-broker".to_string(),
        broker_id: 0,
        epoch: 0,
        max_offset: 0,
        election_priority: 0,
    };

    let result = node.client_write(request).await;
    // In a single-node cluster, this should succeed
    assert!(result.is_ok(), "Client write failed: {:?}", result.err());
}
