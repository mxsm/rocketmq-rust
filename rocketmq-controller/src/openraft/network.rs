//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

//! OpenRaft network implementation
//!
//! Provides RPC-based communication between Raft nodes.

use std::collections::HashMap;
use std::sync::Arc;

use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use tokio::sync::RwLock;
use tracing::debug;

use crate::typ::Node;
use crate::typ::NodeId;
use crate::typ::TypeConfig;

/// Network connection to a single Raft node
#[derive(Clone)]
pub struct NetworkConnection {
    /// Target node
    target: NodeId,

    /// Target address
    target_addr: String,

    /// gRPC client (placeholder for actual implementation)
    /// In production, this would be a tonic gRPC client
    client: Arc<RwLock<Option<()>>>,
}

impl NetworkConnection {
    /// Create a new network connection
    pub fn new(target: NodeId, target_addr: String) -> Self {
        Self {
            target,
            target_addr,
            client: Arc::new(RwLock::new(None)),
        }
    }

    /// Send a request and get response
    async fn send_rpc<Req, Resp, Err, F>(
        &self,
        _req: Req,
        _handler: F,
    ) -> Result<Resp, RPCError<TypeConfig, Err>>
    where
        F: FnOnce() -> Resp,
        Err: std::error::Error + 'static,
    {
        // TODO: Implement actual gRPC call
        // For now, return a network error indicating not implemented
        Err(RPCError::Network(NetworkError::new(&std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            format!(
                "RPC not implemented for node {} at {}",
                self.target, self.target_addr
            ),
        ))))
    }
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>>
    {
        debug!(
            "Sending append_entries to node {}: prev_log_id={:?}, entries={}",
            self.target,
            req.prev_log_id,
            req.entries.len()
        );

        self.send_rpc(req, || {
            // Placeholder response
            AppendEntriesResponse::Success
        })
        .await
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<TypeConfig>,
        RPCError<TypeConfig, RaftError<TypeConfig, InstallSnapshotError>>,
    > {
        debug!(
            "Sending install_snapshot to node {}: meta={:?}",
            self.target, req.meta
        );

        self.send_rpc(req, || {
            // Placeholder response
            InstallSnapshotResponse {
                vote: Default::default(),
            }
        })
        .await
    }

    async fn vote(
        &mut self,
        req: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>> {
        debug!(
            "Sending vote request to node {}: vote={:?}, last_log_id={:?}",
            self.target, req.vote, req.last_log_id
        );

        self.send_rpc(req, || {
            // Placeholder response
            VoteResponse {
                vote: Default::default(),
                vote_granted: false,
                last_log_id: None,
            }
        })
        .await
    }
}

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
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, _node: &Node) -> Self::Network {
        let peer_addrs = self.peer_addrs.read().await;
        let target_addr = peer_addrs
            .get(&target)
            .cloned()
            .unwrap_or_else(|| format!("unknown-{}", target));

        debug!(
            "Creating network connection to node {} at {}",
            target, target_addr
        );

        NetworkConnection::new(target, target_addr)
    }
}
