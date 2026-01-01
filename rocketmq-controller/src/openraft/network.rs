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

//! OpenRaft network implementation
//!
//! Provides RPC-based communication between Raft nodes.

mod grpc_client;

use std::collections::HashMap;
use std::sync::Arc;

pub use grpc_client::GrpcNetworkClient;
use openraft::network::RaftNetworkFactory;
use tokio::sync::RwLock;
use tracing::debug;

use crate::typ::Node;
use crate::typ::NodeId;
use crate::typ::TypeConfig;

/// Network factory for creating connections to Raft nodes
#[derive(Clone)]
pub struct NetworkFactory {
    /// Node addresses
    peer_addrs: Arc<RwLock<HashMap<NodeId, String>>>,
}

impl NetworkFactory {
    /// Create a new network factory
    pub fn new() -> Self {
        Self {
            peer_addrs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a peer address
    pub async fn add_peer(&self, node_id: NodeId, addr: String) {
        self.peer_addrs.write().await.insert(node_id, addr);
    }

    /// Remove a peer
    pub async fn remove_peer(&self, node_id: NodeId) {
        self.peer_addrs.write().await.remove(&node_id);
    }

    /// Get all peer addresses
    pub async fn get_peers(&self) -> HashMap<NodeId, String> {
        self.peer_addrs.read().await.clone()
    }
}

impl Default for NetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = GrpcNetworkClient;

    async fn new_client(&mut self, target: NodeId, _node: &Node) -> Self::Network {
        let peer_addrs = self.peer_addrs.read().await;
        let target_addr = peer_addrs
            .get(&target)
            .cloned()
            .unwrap_or_else(|| format!("unknown-{}", target));

        debug!("Creating gRPC network connection to node {} at {}", target, target_addr);

        GrpcNetworkClient::new(target, target_addr)
    }
}
