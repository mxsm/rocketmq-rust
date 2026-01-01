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

use raft::prelude::Message;
use tokio::sync::mpsc;
use tracing::debug;
use tracing::error;
use tracing::info;

use crate::config::ControllerConfig;
use crate::error::Result;
use crate::raft::RaftTransport;

/// Network manager for Raft communication
///
/// This component manages the network layer for Raft messages,
/// handling both sending and receiving messages between nodes.
pub struct NetworkManager {
    /// Configuration
    config: Arc<ControllerConfig>,

    /// Transport layer
    transport: Arc<RaftTransport>,

    /// Message receiver for outgoing messages
    outgoing_rx: Option<mpsc::UnboundedReceiver<Message>>,

    /// Message sender for incoming messages
    incoming_tx: mpsc::UnboundedSender<Message>,

    /// Running state
    running: Arc<tokio::sync::RwLock<bool>>,
}

impl NetworkManager {
    /// Create a new network manager
    pub fn new(config: Arc<ControllerConfig>) -> (Self, mpsc::UnboundedReceiver<Message>) {
        // Build peer address map
        let mut peer_addrs = HashMap::new();
        for peer in &config.raft_peers {
            peer_addrs.insert(peer.id, peer.addr);
        }

        // Create transport
        let (transport, outgoing_rx, incoming_rx) = RaftTransport::new(config.node_id, config.listen_addr, peer_addrs);

        let incoming_tx = transport.message_sender();

        let manager = Self {
            config,
            transport: Arc::new(transport),
            outgoing_rx: Some(outgoing_rx),
            incoming_tx,
            running: Arc::new(tokio::sync::RwLock::new(false)),
        };

        (manager, incoming_rx)
    }

    /// Start the network manager
    pub async fn start(&mut self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            return Ok(());
        }

        info!("Starting network manager for node {}", self.config.node_id);

        // Start transport
        self.transport.clone().start().await?;

        // Start outgoing message handler
        if let Some(mut outgoing_rx) = self.outgoing_rx.take() {
            let transport = self.transport.clone();
            let running_clone = self.running.clone();

            tokio::spawn(async move {
                info!("Starting outgoing message handler");

                while let Some(msg) = outgoing_rx.recv().await {
                    if !*running_clone.read().await {
                        break;
                    }

                    let to = msg.get_to();
                    debug!("Sending message to peer {}, type: {:?}", to, msg.get_msg_type());

                    if let Err(e) = transport.send_to_peer(to, msg).await {
                        error!("Failed to send message to peer {}: {}", to, e);
                    }
                }

                info!("Outgoing message handler stopped");
            });
        }

        *running = true;
        info!("Network manager started successfully");

        Ok(())
    }

    /// Shutdown the network manager
    pub async fn shutdown(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if !*running {
            return Ok(());
        }

        info!("Shutting down network manager");
        *running = false;

        Ok(())
    }

    /// Get the incoming message sender
    pub fn incoming_sender(&self) -> mpsc::UnboundedSender<Message> {
        self.incoming_tx.clone()
    }

    /// Send a message to a peer
    pub async fn send_message(&self, msg: Message) -> Result<()> {
        self.transport.send_to_peer(msg.get_to(), msg).await
    }

    /// Broadcast a message to all peers
    pub async fn broadcast_message(&self, msg: Message) -> Result<()> {
        self.transport.broadcast(msg).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_network_manager_creation() {
        let config = Arc::new(ControllerConfig::test_config());

        let (manager, _rx) = NetworkManager::new(config);
        assert!(!*manager.running.read().await);
    }
}
