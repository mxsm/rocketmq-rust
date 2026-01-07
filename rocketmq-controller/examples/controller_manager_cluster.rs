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

//! Example: 3-Node ControllerManager cluster
//!
//! This example demonstrates:
//! 1. Creating a 3-node controller cluster
//! 2. Leader election process
//! 3. Cluster state monitoring
//! 4. Graceful cluster shutdown
//!
//! Run with:
//! ```bash
//! cargo run --example controller_manager_cluster
//! ```

use std::time::Duration;

use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::config::RaftPeer;
use rocketmq_controller::config::StorageBackendType;
use rocketmq_controller::error::Result;
use rocketmq_controller::manager::ControllerManager;
use rocketmq_rust::ArcMut;
use tracing::error;
use tracing::info;
use tracing::warn;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::registry().with(fmt::layer()).init();

    info!("Starting 3-node ControllerManager cluster example");

    // Start 3-node cluster
    let managers = start_cluster().await?;

    // Wait for leader election
    info!("Waiting for leader election...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Check cluster state
    info!("Checking cluster state:");
    check_cluster_state(&managers).await;

    // Simulate some operations
    info!("Cluster is running... (will shutdown in 10 seconds)");
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Graceful shutdown
    shutdown_cluster(managers).await?;

    info!("Cluster shutdown complete");
    Ok(())
}

/// Start a 3-node controller cluster
async fn start_cluster() -> Result<Vec<ArcMut<ControllerManager>>> {
    let mut managers = Vec::new();

    // Define node configurations
    let nodes = vec![(1u64, 9878u16), (2u64, 9868u16), (3u64, 9858u16)];

    // Create peer list
    let peers: Vec<RaftPeer> = nodes
        .iter()
        .map(|(id, port)| RaftPeer {
            id: *id,
            addr: format!("127.0.0.1:{}", port).parse().unwrap(),
        })
        .collect();

    // Start each node
    for (node_id, port) in nodes {
        info!("Starting node {} on port {}...", node_id, port);

        // Create configuration
        let config = create_cluster_config(node_id, port, peers.clone())?;

        // Create manager
        let manager = ControllerManager::new(config).await?;

        // Wrap in ArcMut for initialization and start
        let manager = ArcMut::new(manager);

        // Initialize
        if !manager.clone().initialize().await? {
            error!("Failed to initialize node {}", node_id);
            return Err(rocketmq_controller::error::ControllerError::InitializationFailed);
        }

        // Start
        manager.clone().start().await?;
        info!("✓ Node {} started successfully", node_id);

        managers.push(manager);

        // Small delay between starts
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Ok(managers)
}

/// Create configuration for a cluster node
fn create_cluster_config(node_id: u64, port: u16, peers: Vec<RaftPeer>) -> Result<ControllerConfig> {
    use std::net::SocketAddr;

    let listen_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let config = ControllerConfig::default()
        .with_node_info(node_id, listen_addr)
        .with_raft_peers(peers)
        .with_storage_path(format!("/tmp/rocketmq-controller-cluster/node{}", node_id))
        .with_storage_backend(StorageBackendType::Memory)
        .with_election_timeout_ms(1000)
        .with_heartbeat_interval_ms(300)
        .with_enable_elect_unclean_master_local(false);

    Ok(config)
}

/// Check and display cluster state
async fn check_cluster_state(managers: &[ArcMut<ControllerManager>]) {
    let mut leader_count = 0;
    let mut follower_count = 0;
    let mut leader_node_id = None;

    for (i, manager) in managers.iter().enumerate() {
        let node_id = i + 1;
        let is_leader = manager.is_leader();
        let is_running = manager.is_running();

        info!("Node {} - Running: {}, Is Leader: {}", node_id, is_running, is_leader);

        if is_leader {
            leader_count += 1;
            leader_node_id = Some(node_id);
        } else {
            follower_count += 1;
        }
    }

    info!("Cluster Summary:");
    info!("  - Total Nodes: {}", managers.len());
    info!("  - Leaders: {}", leader_count);
    info!("  - Followers: {}", follower_count);

    if leader_count == 1 {
        info!("Leader elected: Node {}", leader_node_id.unwrap());
    } else if leader_count == 0 {
        warn!("No leader elected yet");
    } else {
        error!("Multiple leaders detected! (Split brain)");
    }
}

/// Shutdown all nodes in the cluster
async fn shutdown_cluster(managers: Vec<ArcMut<ControllerManager>>) -> Result<()> {
    info!("Shutting down cluster...");

    for (i, manager) in managers.iter().enumerate() {
        let node_id = i + 1;
        info!("Shutting down node {}...", node_id);

        if let Err(e) = manager.shutdown().await {
            error!("Failed to shutdown node {}: {}", node_id, e);
        } else {
            info!(" Node {} shut down successfully", node_id);
        }
    }

    Ok(())
}
