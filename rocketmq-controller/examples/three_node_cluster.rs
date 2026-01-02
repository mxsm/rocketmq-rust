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

//! Example: Three-node Raft cluster
//!
//! This example demonstrates how to create and run a three-node OpenRaft cluster.
//! Run this example with different node IDs to start different nodes:
//!
//! ```bash
//! # Terminal 1
//! cargo run --example three_node_cluster -- --node-id 1
//!
//! # Terminal 2
//! cargo run --example three_node_cluster -- --node-id 2
//!
//! # Terminal 3
//! cargo run --example three_node_cluster -- --node-id 3
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use clap::Parser;
use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::config::RaftPeer;
use rocketmq_controller::openraft::RaftNodeManager;
use rocketmq_controller::typ::ControllerRequest;
use rocketmq_controller::typ::Node;

#[derive(Parser, Debug)]
#[clap(name = "three-node-cluster")]
struct Args {
    /// Node ID (1, 2, or 3)
    #[clap(long, default_value = "1")]
    node_id: u64,

    /// Initialize cluster (only for node 1)
    #[clap(long)]
    init: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize logging
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();

    println!("=== OpenRaft Three-Node Cluster ===");
    println!("Node ID: {}", args.node_id);
    println!();

    // Define cluster topology
    let peers = vec![
        RaftPeer {
            id: 1,
            addr: "127.0.0.1:9876".parse()?,
        },
        RaftPeer {
            id: 2,
            addr: "127.0.0.1:9877".parse()?,
        },
        RaftPeer {
            id: 3,
            addr: "127.0.0.1:9878".parse()?,
        },
    ];

    let current_peer = peers.iter().find(|p| p.id == args.node_id).ok_or("Invalid node ID")?;

    // Create configuration
    let config = Arc::new(
        ControllerConfig::new_node(args.node_id, current_peer.addr)
            .with_election_timeout_ms(1000)
            .with_heartbeat_interval_ms(300)
            .with_raft_peers(peers.clone()),
    );

    // Create Raft node
    println!("Creating Raft node...");
    let node = RaftNodeManager::new(config).await?;
    println!(" Node created");
    println!();

    // If this is node 1 and --init flag is set, initialize the cluster
    if args.node_id == 1 && args.init {
        println!("Initializing cluster...");

        let mut nodes = BTreeMap::new();
        for peer in &peers {
            nodes.insert(
                peer.id,
                Node {
                    node_id: peer.id,
                    rpc_addr: peer.addr.to_string(),
                },
            );
        }

        node.initialize_cluster(nodes).await?;
        println!(" Cluster initialized");
        println!();
    } else if args.node_id != 1 {
        // Non-leader nodes: add as learner and wait to join
        println!("Waiting to join cluster...");
        println!("(Node 1 should be started with --init flag first)");
        println!();
    }

    // Wait a bit for leader election
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Check leadership
    let is_leader = node.is_leader().await?;
    let leader_id = node.get_leader().await?;

    println!("Cluster Status:");
    println!("  Is Leader: {}", is_leader);
    println!("  Current Leader: {:?}", leader_id);
    println!();

    // If this is the leader, perform some writes
    if is_leader {
        println!("This node is the leader. Performing test writes...");

        // Register a broker
        let register_request = ControllerRequest::RegisterBroker {
            cluster_name: "DefaultCluster".to_string(),
            broker_addr: "127.0.0.1:10911".to_string(),
            broker_name: format!("broker-from-node-{}", args.node_id),
            broker_id: 0,
            epoch: 0,
            max_offset: 0,
            election_priority: 0,
        };

        match node.client_write(register_request).await {
            Ok(response) => {
                println!(" Broker registered: {:?}", response.data);
            }
            Err(e) => {
                println!("✗ Write failed: {}", e);
            }
        }
        println!();
    }

    // Query state
    let store = node.store();
    let all_brokers = store.state_machine.get_all_brokers();
    println!("Cluster Data:");
    println!("  Brokers: {}", all_brokers.len());
    for broker in all_brokers {
        println!("    - {}: {}", broker.broker_name, broker.broker_addr);
    }
    println!();

    // Keep the node running
    println!("Node is running. Press Ctrl+C to stop.");
    println!();

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;

    println!("\nShutting down...");
    node.shutdown().await?;
    println!(" Node shut down gracefully");

    Ok(())
}
