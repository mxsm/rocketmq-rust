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

//! Example: Basic ControllerManager usage
//!
//! This example demonstrates how to:
//! 1. Create a ControllerManager instance
//! 2. Initialize and start the controller
//! 3. Query controller state
//! 4. Gracefully shutdown
//!
//! Run with:
//! ```bash
//! cargo run --example controller_manager_basic
//! ```

use std::time::Duration;

use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::config::RaftPeer;
use rocketmq_controller::error::Result;
use rocketmq_controller::manager::ControllerManager;
use rocketmq_rust::ArcMut;
use tracing::error;
use tracing::info;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::registry().with(fmt::layer()).init();

    info!("Starting ControllerManager example");

    // Create configuration for a single-node cluster (for testing)
    let config = create_controller_config(1, 9878)?;

    // Create the controller manager
    info!("Creating ControllerManager...");
    let manager = ControllerManager::new(config).await?;

    // Wrap in ArcMut for initialization and start
    let manager = ArcMut::new(manager);

    // Initialize the manager
    info!("Initializing ControllerManager...");
    if !manager.clone().initialize().await? {
        error!("Failed to initialize ControllerManager");
        return Err(rocketmq_controller::error::ControllerError::InitializationFailed);
    }
    info!(" Controller initialized successfully");

    // Start the manager
    info!("Starting ControllerManager...");
    manager.clone().start().await?;
    info!(" Controller started successfully");

    // Query controller state
    info!("Checking controller state...");
    let is_running = manager.is_running();
    let is_initialized = manager.is_initialized();
    let is_leader = manager.is_leader();

    info!("Controller State:");
    info!("  - Running: {}", is_running);
    info!("  - Initialized: {}", is_initialized);
    info!("  - Is Leader: {}", is_leader);

    // Run for a short time
    info!("Controller is running... (will shutdown in 5 seconds)");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Graceful shutdown
    info!("Shutting down ControllerManager...");
    manager.shutdown().await?;
    info!(" Controller shut down successfully");

    Ok(())
}

/// Create a controller configuration
fn create_controller_config(node_id: u64, port: u16) -> Result<ControllerConfig> {
    use std::net::SocketAddr;

    let listen_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let mut config = ControllerConfig::new_node(node_id, listen_addr);

    // Configure Raft peers (single node for this example)
    config = config.with_raft_peers(vec![RaftPeer {
        id: node_id,
        addr: listen_addr,
    }]);

    // Set storage path
    config = config.with_storage_path(format!("/tmp/rocketmq-controller/node{}", node_id));

    // Configure timeouts
    config.election_timeout_ms = 1000;
    config.heartbeat_interval_ms = 300;

    Ok(config)
}
