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

//! Simple cluster test

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::config::RaftPeer;
use rocketmq_controller::openraft::GrpcRaftService;
use rocketmq_controller::openraft::RaftNodeManager;
use rocketmq_controller::protobuf::openraft::open_raft_service_server::OpenRaftServiceServer;
use rocketmq_controller::typ::Node;
use tonic::transport::Server;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_simple_cluster_setup() {
    println!("Starting simple cluster test...");

    const BASE_PORT: u16 = 55000;
    const NODE_COUNT: u64 = 3;

    // Create peer configurations
    let mut all_peers = Vec::new();
    for node_id in 1..=NODE_COUNT {
        let port = BASE_PORT + node_id as u16;
        all_peers.push(RaftPeer {
            id: node_id,
            addr: format!("127.0.0.1:{}", port).parse().unwrap(),
        });
    }

    println!("✓ Peer configurations created");

    // Create nodes
    let mut nodes = Vec::new();
    for node_id in 1..=NODE_COUNT {
        let port = BASE_PORT + node_id as u16;
        let addr = format!("127.0.0.1:{}", port).parse().unwrap();

        let peers: Vec<RaftPeer> = all_peers.iter().filter(|p| p.id != node_id).cloned().collect();

        let config = ControllerConfig::default()
            .with_node_info(node_id, addr)
            .with_election_timeout_ms(1000)
            .with_heartbeat_interval_ms(300)
            .with_raft_peers(peers);

        let node = Arc::new(RaftNodeManager::new(Arc::new(config)).await.unwrap());

        // Start gRPC server
        let service = GrpcRaftService::new(node.raft());
        let server = Server::builder()
            .add_service(OpenRaftServiceServer::new(service))
            .serve(addr);

        tokio::spawn(async move {
            if let Err(e) = server.await {
                eprintln!("gRPC server error: {}", e);
            }
        });

        nodes.push((node_id, node));
        println!("Node {} created and server started on {}", node_id, addr);
    }

    // Give servers time to start
    tokio::time::sleep(Duration::from_millis(200)).await;
    println!(" All servers started");

    // Initialize cluster from first node
    let mut cluster_nodes = BTreeMap::new();
    for (node_id, _) in &nodes {
        let port = BASE_PORT + *node_id as u16;
        cluster_nodes.insert(
            *node_id,
            Node {
                node_id: *node_id,
                rpc_addr: format!("127.0.0.1:{}", port),
            },
        );
    }

    nodes[0]
        .1
        .initialize_cluster(cluster_nodes)
        .await
        .expect("Failed to initialize cluster");

    println!(" Cluster initialized");

    // Wait for leader election
    println!(" Waiting for leader election...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Check for leader
    let mut leader_count = 0;
    let mut follower_count = 0;

    for (node_id, node) in &nodes {
        match node.is_leader().await {
            Ok(true) => {
                leader_count += 1;
                println!(" Node {} is LEADER", node_id);
            }
            Ok(false) => {
                follower_count += 1;
                println!(" Node {} is follower", node_id);
            }
            Err(e) => {
                println!(" Node {} error: {}", node_id, e);
            }
        }

        // Print metrics
        use openraft::async_runtime::WatchReceiver;
        let metrics = node.raft().metrics().borrow_watched().clone();
        println!("  - State: {:?}", metrics.state);
        println!("  - Term: {}", metrics.current_term);
        println!("  - Leader: {:?}", metrics.current_leader);
    }

    println!("\nResults:");
    println!("  Leaders: {}", leader_count);
    println!("  Followers: {}", follower_count);

    // We should have exactly 1 leader
    assert_eq!(leader_count, 1, "Should have exactly one leader");
    assert_eq!(follower_count, 2, "Should have two followers");

    println!("\n Test passed!");
}
