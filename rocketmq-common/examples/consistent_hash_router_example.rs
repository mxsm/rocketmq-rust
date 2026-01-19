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

//! Example demonstrating the usage of ConsistentHashRouter
//!
//! This example shows how to use consistent hashing to distribute
//! keys across a set of server nodes.

use rocketmq_common::common::consistenthash::ConsistentHashRouter;
use rocketmq_common::common::consistenthash::Node;

#[derive(Debug, Clone)]
struct ServerNode {
    address: String,
}

impl ServerNode {
    fn new(address: &str) -> Self {
        Self {
            address: address.to_string(),
        }
    }
}

impl Node for ServerNode {
    fn get_key(&self) -> &str {
        &self.address
    }
}

fn main() {
    println!("=== Consistent Hash Router Example ===\n");

    // Create server nodes
    let servers = vec![
        ServerNode::new("192.168.1.1:8080"),
        ServerNode::new("192.168.1.2:8080"),
        ServerNode::new("192.168.1.3:8080"),
        ServerNode::new("192.168.1.4:8080"),
    ];

    // Create router with 150 virtual nodes per physical node
    let router = ConsistentHashRouter::new(servers.clone(), 150);

    println!(
        "Created hash ring with {} physical nodes and {} total virtual nodes\n",
        servers.len(),
        router.size()
    );

    // Route some keys
    println!("Routing keys to servers:");
    let test_keys = vec![
        "user:1001",
        "user:1002",
        "user:1003",
        "order:2001",
        "order:2002",
        "product:3001",
        "product:3002",
        "session:4001",
    ];

    for key in &test_keys {
        if let Some(node) = router.route_node(key) {
            println!("  {} -> {}", key, node.get_key());
        }
    }

    // Demonstrate consistency - same key always routes to same node
    println!("\n=== Consistency Test ===");
    let key = "user:1001";
    println!("Routing '{}' multiple times:", key);
    for i in 1..=5 {
        if let Some(node) = router.route_node(key) {
            println!("  Attempt {}: {}", i, node.get_key());
        }
    }

    // Demonstrate key distribution
    println!("\n=== Distribution Statistics ===");
    let mut distribution = std::collections::HashMap::new();
    for i in 0..1000 {
        let key = format!("key:{}", i);
        if let Some(node) = router.route_node(&key) {
            *distribution.entry(node.get_key().to_string()).or_insert(0) += 1;
        }
    }

    println!("Distribution of 1000 keys:");
    for (server, count) in distribution {
        println!("  {}: {} keys ({:.1}%)", server, count, (count as f64 / 10.0));
    }

    // Demonstrate adding a node
    println!("\n=== Adding a New Node ===");
    let mut mutable_router = router.clone();
    let new_server = ServerNode::new("192.168.1.5:8080");
    mutable_router.add_node(new_server.clone(), 150);
    println!(
        "Added new server: {}. Total virtual nodes: {}",
        new_server.get_key(),
        mutable_router.size()
    );

    // Show how keys redistribute
    println!("\nKey redistribution after adding node:");
    for key in &test_keys[0..4] {
        let old_node = router.route_node(key).unwrap();
        let new_node = mutable_router.route_node(key).unwrap();
        if old_node.get_key() != new_node.get_key() {
            println!("  {} moved from {} to {}", key, old_node.get_key(), new_node.get_key());
        } else {
            println!("  {} stayed on {}", key, old_node.get_key());
        }
    }

    // Demonstrate removing a node
    println!("\n=== Removing a Node ===");
    let remove_server = servers[0].clone();
    mutable_router.remove_node(&remove_server);
    println!(
        "Removed server: {}. Total virtual nodes: {}",
        remove_server.get_key(),
        mutable_router.size()
    );

    println!("\nKeys after node removal:");
    for key in &test_keys {
        if let Some(node) = mutable_router.route_node(key) {
            println!("  {} -> {}", key, node.get_key());
        }
    }
}
