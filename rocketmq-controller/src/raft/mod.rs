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

mod network;
mod node;
mod storage;
mod transport;

use std::sync::Arc;
use std::time::Duration;

pub use network::NetworkManager;
pub use node::RaftNode;
use raft::prelude::*;
pub use storage::MemStorage;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::time;
use tracing::debug;
use tracing::error;
use tracing::info;
pub use transport::MessageCodec;
pub use transport::PeerConnection;
pub use transport::RaftTransport;

use crate::config::ControllerConfig;
use crate::error::ControllerError;
use crate::error::Result;

/// Messages that can be sent to the Raft controller
#[derive(Debug)]
pub enum RaftMessage {
    /// Propose a new entry
    Propose {
        data: Vec<u8>,
        response: tokio::sync::oneshot::Sender<Result<Vec<u8>>>,
    },
    /// Process a Raft message from peer
    Step { message: Message },
    /// Tick the Raft state machine
    Tick,
    /// Query current state (read-only)
    Query {
        data: Vec<u8>,
        response: tokio::sync::oneshot::Sender<Result<Vec<u8>>>,
    },
    /// Shutdown the Raft controller
    Shutdown,
}

/// Raft controller - replaces Java DLedger
///
/// This component provides distributed consensus using the Raft algorithm.
/// It manages:
/// - Leader election
/// - Log replication
/// - Snapshot management
/// - State machine application
/// - Network communication
pub struct RaftController {
    /// Node ID
    node_id: u64,

    /// Raft node
    node: Arc<RwLock<Option<RaftNode>>>,

    /// Network manager
    network: Arc<RwLock<Option<NetworkManager>>>,

    /// Message sender
    tx: mpsc::UnboundedSender<RaftMessage>,

    /// Configuration
    config: Arc<ControllerConfig>,
}

impl RaftController {
    /// Create a new Raft controller
    pub async fn new(config: Arc<ControllerConfig>) -> Result<Self> {
        let node_id = config.node_id;
        let (tx, rx) = mpsc::unbounded_channel();

        let controller = Self {
            node_id,
            node: Arc::new(RwLock::new(None)),
            network: Arc::new(RwLock::new(None)),
            tx,
            config: config.clone(),
        };

        // Initialize Raft node
        let node = RaftNode::new(node_id, config.clone()).await?;
        *controller.node.write().await = Some(node);

        // Initialize network manager
        let (network_manager, incoming_rx) = NetworkManager::new(config.clone());
        *controller.network.write().await = Some(network_manager);

        // Start message processing loop
        let node_clone = controller.node.clone();
        tokio::spawn(async move {
            Self::message_loop(node_clone, rx).await;
        });

        // Start incoming message handler
        let tx_clone = controller.tx.clone();
        tokio::spawn(async move {
            Self::incoming_message_loop(incoming_rx, tx_clone).await;
        });

        Ok(controller)
    }

    /// Start the Raft controller
    pub async fn start(&self) -> Result<()> {
        info!("Starting Raft controller for node {}", self.node_id);

        // Start network manager
        if let Some(network) = self.network.write().await.as_mut() {
            network.start().await?;
        }

        // Start tick timer
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                if tx.send(RaftMessage::Tick).is_err() {
                    break;
                }
            }
        });

        Ok(())
    }

    /// Shutdown the Raft controller
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down Raft controller for node {}", self.node_id);

        // Shutdown network manager
        if let Some(network) = self.network.read().await.as_ref() {
            network.shutdown().await?;
        }

        self.tx
            .send(RaftMessage::Shutdown)
            .map_err(|_| ControllerError::Shutdown)?;
        Ok(())
    }

    /// Propose a new entry (write operation)
    pub async fn propose(&self, data: Vec<u8>) -> Result<Vec<u8>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(RaftMessage::Propose { data, response: tx })
            .map_err(|_| ControllerError::Shutdown)?;

        rx.await
            .map_err(|_| ControllerError::Internal("Response channel closed".to_string()))?
    }

    /// Query current state (read-only operation)
    pub async fn query(&self, data: Vec<u8>) -> Result<Vec<u8>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(RaftMessage::Query { data, response: tx })
            .map_err(|_| ControllerError::Shutdown)?;

        rx.await
            .map_err(|_| ControllerError::Internal("Response channel closed".to_string()))?
    }

    /// Check if this node is the leader
    pub async fn is_leader(&self) -> bool {
        if let Some(node) = self.node.read().await.as_ref() {
            node.is_leader().await
        } else {
            false
        }
    }

    /// Get the current leader ID
    pub async fn get_leader(&self) -> Option<u64> {
        if let Some(node) = self.node.read().await.as_ref() {
            node.get_leader().await
        } else {
            None
        }
    }

    /// Handle a Raft message from a peer
    pub async fn step(&self, message: Message) -> Result<()> {
        self.tx
            .send(RaftMessage::Step { message })
            .map_err(|_| ControllerError::Shutdown)?;
        Ok(())
    }

    /// Message processing loop
    async fn message_loop(node: Arc<RwLock<Option<RaftNode>>>, mut rx: mpsc::UnboundedReceiver<RaftMessage>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                RaftMessage::Propose { data, response } => {
                    let result = if let Some(n) = node.read().await.as_ref() {
                        n.propose(data).await
                    } else {
                        Err(ControllerError::Internal("Node not initialized".to_string()))
                    };
                    let _ = response.send(result);
                }
                RaftMessage::Step { message } => {
                    if let Some(n) = node.read().await.as_ref() {
                        if let Err(e) = n.step(message).await {
                            error!("Failed to step Raft: {}", e);
                        }
                    }
                }
                RaftMessage::Tick => {
                    if let Some(n) = node.read().await.as_ref() {
                        if let Err(e) = n.tick().await {
                            error!("Failed to tick Raft: {}", e);
                        }
                    }
                }
                RaftMessage::Query { data, response } => {
                    let result = if let Some(n) = node.read().await.as_ref() {
                        n.query(data).await
                    } else {
                        Err(ControllerError::Internal("Node not initialized".to_string()))
                    };
                    let _ = response.send(result);
                }
                RaftMessage::Shutdown => {
                    info!("Raft controller shutting down");
                    break;
                }
            }
        }
    }

    /// Incoming message loop - handles messages from network
    async fn incoming_message_loop(
        mut incoming_rx: mpsc::UnboundedReceiver<Message>,
        tx: mpsc::UnboundedSender<RaftMessage>,
    ) {
        info!("Starting incoming message loop");

        while let Some(message) = incoming_rx.recv().await {
            debug!("Received Raft message from network: {:?}", message.get_msg_type());

            if tx.send(RaftMessage::Step { message }).is_err() {
                error!("Failed to forward incoming message to Raft");
                break;
            }
        }

        info!("Incoming message loop stopped");
    }
}

/*#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_raft_controller_creation() {
        // Placeholder test - actual test would require full setup
        assert!(true);
    }
}
*/
