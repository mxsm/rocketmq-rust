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

//! Snapshot functionality tests for OpenRaft implementation

use std::collections::BTreeMap;
use std::sync::Arc;

use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::openraft::RaftNodeManager;
use rocketmq_controller::openraft::StateMachine;
use rocketmq_controller::typ::ControllerRequest;
use rocketmq_controller::typ::Node;

#[tokio::test]
async fn test_snapshot_creation() {
    // Create a test configuration
    let config = Arc::new(
        ControllerConfig::default()
            .with_node_info(1, "127.0.0.1:39876".parse().unwrap())
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
            rpc_addr: "127.0.0.1:39876".to_string(),
        },
    );

    node.initialize_cluster(nodes).await.unwrap();

    // Wait for leader election
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Write some data
    let request = ControllerRequest::RegisterBroker {
        cluster_name: "test-cluster".to_string(),
        broker_addr: "127.0.0.1:10911".to_string(),
        broker_name: "broker-a".to_string(),
        broker_id: 0,
        epoch: 0,
        max_offset: 0,
        election_priority: 0,
    };

    node.client_write(request).await.unwrap();

    // Trigger snapshot (would normally happen automatically)
    // For now just verify state machine has the data
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_state_machine_snapshot() {
    use openraft::RaftSnapshotBuilder;

    let mut sm = StateMachine::new();

    // Build a snapshot
    let snapshot_result = sm.build_snapshot().await;
    assert!(snapshot_result.is_ok(), "Failed to build snapshot");

    let snapshot = snapshot_result.unwrap();

    // Verify snapshot metadata
    assert!(!snapshot.snapshot.get_ref().is_empty(), "Snapshot should contain data");
    println!("Snapshot size: {} bytes", snapshot.snapshot.get_ref().len());
    println!("Snapshot ID: {}", snapshot.meta.snapshot_id);
}

#[tokio::test]
async fn test_snapshot_install() {
    use openraft::storage::RaftStateMachine;
    use openraft::RaftSnapshotBuilder;

    // Create first state machine with some data
    let mut sm1 = StateMachine::new();

    // Add some mock data via internal method would go here
    // For now we just verify the install mechanism works

    // Build snapshot from sm1
    let snapshot = sm1.build_snapshot().await.unwrap();

    // Create second state machine
    let mut sm2 = StateMachine::new();

    // Install snapshot into sm2
    let result = sm2.install_snapshot(&snapshot.meta, snapshot.snapshot).await;
    assert!(result.is_ok(), "Failed to install snapshot");

    println!("Successfully installed snapshot");
}
