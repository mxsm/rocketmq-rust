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

//! Example: Single-node Raft cluster
//!
//! This example demonstrates how to create and run a single-node OpenRaft cluster.
//! It shows:
//! - Node initialization
//! - Cluster setup
//! - Leader election
//! - Client write operations
//! - State queries

use std::collections::BTreeMap;
use std::sync::Arc;

use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::openraft::RaftNodeManager;
use rocketmq_controller::typ::ControllerRequest;
use rocketmq_controller::typ::Node;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();

    println!("=== OpenRaft Single Node Example ===\n");

    // Step 1: Create configuration
    println!("1. Creating node configuration...");
    let node_id = 1;
    let listen_addr = "127.0.0.1:60109".parse()?;

    let config = Arc::new(
        ControllerConfig::new_node(node_id, listen_addr)
            .with_election_timeout_ms(1000)
            .with_heartbeat_interval_ms(300),
    );
    println!("   Node ID: {}", node_id);
    println!("   Listen Address: {}", listen_addr);
    println!();

    // Step 2: Create Raft node
    println!("2. Creating Raft node...");
    let node = RaftNodeManager::new(config).await?;
    println!("    Node created successfully");
    println!();

    // Step 3: Initialize single-node cluster
    println!("3. Initializing single-node cluster...");
    let mut nodes = BTreeMap::new();
    nodes.insert(
        node_id,
        Node {
            node_id,
            rpc_addr: listen_addr.to_string(),
        },
    );

    node.initialize_cluster(nodes).await?;
    println!("    Cluster initialized");
    println!();

    // Step 4: Wait for leader election
    println!("4. Waiting for leader election...");
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let is_leader = node.is_leader().await?;
    println!("   Is Leader: {}", is_leader);

    if is_leader {
        println!("    This node is now the leader");
    } else {
        println!("   ✗ Leader election did not complete");
    }
    println!();

    // Step 5: Perform some client writes
    println!("5. Performing client write operations...");

    // Register a broker
    let register_request = ControllerRequest::RegisterBroker {
        cluster_name: "DefaultCluster".to_string(),
        broker_addr: "127.0.0.1:10911".to_string(),
        broker_name: "broker-a".to_string(),
        broker_id: 0,
        epoch: 0,
        max_offset: 0,
        election_priority: 0,
    };

    println!("   Writing: Register broker 'broker-a'");
    match node.client_write(register_request).await {
        Ok(response) => {
            println!("    Write succeeded: {:?}", response.data);
        }
        Err(e) => {
            println!("   ✗ Write failed: {}", e);
        }
    }
    println!();

    // Create a topic
    let create_topic = ControllerRequest::UpdateTopic {
        topic_name: "test-topic".to_string(),
        read_queue_nums: 4,
        write_queue_nums: 4,
        perm: 6,
    };

    println!("   Writing: Create topic 'test-topic'");
    match node.client_write(create_topic).await {
        Ok(response) => {
            println!("    Write succeeded: {:?}", response.data);
        }
        Err(e) => {
            println!("   ✗ Write failed: {}", e);
        }
    }
    println!();

    // Step 6: Query state
    println!("6. Querying cluster state...");
    let store = node.store();

    let all_brokers = store.state_machine.get_all_brokers();
    println!("   Registered Brokers: {}", all_brokers.len());
    for broker in all_brokers {
        println!("     - {}: {}", broker.broker_name, broker.broker_addr);
    }

    let all_topics = store.state_machine.get_all_topics();
    println!("   Registered Topics: {}", all_topics.len());
    for topic in all_topics {
        println!("     - {}: {} queues", topic.topic_name, topic.read_queue_nums);
    }
    println!();

    // Step 7: Shutdown
    println!("7. Shutting down...");
    node.shutdown().await?;
    println!("    Node shut down gracefully");
    println!();

    println!("=== Example Complete ===");

    Ok(())
}
