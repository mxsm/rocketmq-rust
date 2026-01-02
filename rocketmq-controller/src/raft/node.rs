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

use std::collections::HashMap;
use std::sync::Arc;

use raft::prelude::*;
use raft::storage::MemStorage as RaftMemStorage;
use raft::StateRole;
use tokio::sync::RwLock;
use tracing::debug;
use tracing::info;

use crate::config::ControllerConfig;
use crate::error::ControllerError;
use crate::error::Result;

/// Type alias for proposal map to reduce complexity
type ProposalMap = HashMap<Vec<u8>, tokio::sync::oneshot::Sender<Result<Vec<u8>>>>;

/// Raft node wrapper
pub struct RaftNode {
    /// Node ID
    id: u64,

    /// Raft raw node
    raw_node: Arc<RwLock<RawNode<RaftMemStorage>>>,

    /// Pending proposals
    proposals: Arc<RwLock<ProposalMap>>,
}

impl RaftNode {
    /// Create a new Raft node
    pub async fn new(id: u64, config: Arc<ControllerConfig>) -> Result<Self> {
        // Create Raft configuration
        let raft_config = Config {
            id,
            election_tick: (config.election_timeout_ms / 100) as usize,
            heartbeat_tick: (config.heartbeat_interval_ms / 100) as usize,
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 256,
            ..Default::default()
        };
        raft_config
            .validate()
            .map_err(|e| ControllerError::ConfigError(format!("Invalid Raft config: {:?}", e)))?;

        // Create storage
        let storage = RaftMemStorage::new();

        // Initialize peers
        let peers: Vec<u64> = config.raft_peers.iter().map(|p| p.id).collect();
        if !peers.is_empty() {
            let mut snapshot = Snapshot::default();
            snapshot.mut_metadata().index = 0;
            snapshot.mut_metadata().term = 0;
            snapshot.mut_metadata().mut_conf_state().voters.clone_from(&peers);

            storage
                .wl()
                .apply_snapshot(snapshot)
                .map_err(|e| ControllerError::Raft(format!("Failed to apply snapshot: {:?}", e)))?;
        }

        // Create raw node
        let raw_node = RawNode::new(&raft_config, storage, &slog_global::get_global())
            .map_err(|e| ControllerError::Raft(format!("Failed to create RawNode: {:?}", e)))?;

        info!("Created Raft node {} with peers: {:?}", id, peers);

        Ok(Self {
            id,
            raw_node: Arc::new(RwLock::new(raw_node)),
            proposals: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Propose a new entry
    pub async fn propose(&self, data: Vec<u8>) -> Result<Vec<u8>> {
        let mut raw_node = self.raw_node.write().await;

        // Check if we are the leader
        if raw_node.raft.state != StateRole::Leader {
            let leader = raw_node.raft.leader_id;
            return Err(ControllerError::NotLeader {
                leader_id: if leader == 0 { None } else { Some(leader) },
            });
        }

        // Propose the entry
        raw_node
            .propose(vec![], data.clone())
            .map_err(|e| ControllerError::Raft(format!("Failed to propose: {:?}", e)))?;

        // For now, return immediately
        // In a real implementation, we would wait for the entry to be committed
        Ok(data)
    }

    /// Query current state (read-only)
    pub async fn query(&self, _data: Vec<u8>) -> Result<Vec<u8>> {
        let raw_node = self.raw_node.read().await;

        // Check if we are the leader
        if raw_node.raft.state != StateRole::Leader {
            let leader = raw_node.raft.leader_id;
            return Err(ControllerError::NotLeader {
                leader_id: if leader == 0 { None } else { Some(leader) },
            });
        }

        // For now, return empty response
        // In a real implementation, we would query the state machine
        Ok(vec![])
    }

    /// Step the Raft state machine with a message
    pub async fn step(&self, message: Message) -> Result<()> {
        let mut raw_node = self.raw_node.write().await;
        raw_node
            .step(message)
            .map_err(|e| ControllerError::Raft(format!("Failed to step: {:?}", e)))?;
        Ok(())
    }

    /// Tick the Raft state machine
    pub async fn tick(&self) -> Result<()> {
        let mut raw_node = self.raw_node.write().await;
        raw_node.tick();

        // Process ready
        if raw_node.has_ready() {
            let mut ready = raw_node.ready();

            // Handle messages
            if !ready.messages().is_empty() {
                // In a real implementation, send these messages to peers
                debug!("Need to send {} messages to peers", ready.messages().len());
            }

            // Handle committed entries
            for entry in ready.take_committed_entries() {
                if entry.data.is_empty() {
                    // Empty entry, from leadership transfer
                    continue;
                }
                // Apply to state machine
                debug!("Applying entry: {:?}", entry);
            }

            // Advance the Raft
            let light_rd = raw_node.advance(ready);
            if let Some(commit) = light_rd.commit_index() {
                raw_node.mut_store().wl().commit_to(commit).ok();
            }
            raw_node.advance_apply();
        }

        Ok(())
    }

    /// Check if this node is the leader
    pub async fn is_leader(&self) -> bool {
        let raw_node = self.raw_node.read().await;
        raw_node.raft.state == StateRole::Leader
    }

    /// Get the current leader ID
    pub async fn get_leader(&self) -> Option<u64> {
        let raw_node = self.raw_node.read().await;
        let leader = raw_node.raft.leader_id;
        if leader == 0 {
            None
        } else {
            Some(leader)
        }
    }
}

// Helper for slog logger
mod slog_global {
    use slog::o;
    use slog::Discard;
    use slog::Logger;

    pub fn get_global() -> Logger {
        Logger::root(Discard, o!())
    }
}
