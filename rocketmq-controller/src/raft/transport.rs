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
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use bytes::BytesMut;
use protobuf::Message as ProtobufMessage;
use raft::eraftpb;
use raft::prelude::Message;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::error::ControllerError;
use crate::error::Result;

/// Message codec for Raft messages
pub struct MessageCodec;

impl MessageCodec {
    /// Encode a Raft message to bytes using protobuf 2.x
    pub fn encode(msg: &Message) -> Result<Bytes> {
        // Encode using protobuf 2.x
        let encoded = msg
            .write_to_bytes()
            .map_err(|e| ControllerError::SerializationError(e.to_string()))?;

        // Length prefix (4 bytes) + message data
        let len = encoded.len() as u32;
        let mut result = BytesMut::with_capacity(4 + encoded.len());
        result.extend_from_slice(&len.to_be_bytes());
        result.extend_from_slice(&encoded);

        Ok(result.freeze())
    }

    /// Decode bytes to a Raft message using protobuf
    pub async fn decode(stream: &mut TcpStream) -> Result<Message> {
        // Read length prefix
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| ControllerError::NetworkError(e.to_string()))?;

        let len = u32::from_be_bytes(len_buf) as usize;

        // Validate length
        if len > 10 * 1024 * 1024 {
            return Err(ControllerError::InvalidRequest(format!(
                "Message too large: {} bytes",
                len
            )));
        }

        // Read message data
        let mut buf = vec![0u8; len];
        stream
            .read_exact(&mut buf)
            .await
            .map_err(|e| ControllerError::NetworkError(e.to_string()))?;

        // Deserialize using protobuf 2.x
        let msg =
            eraftpb::Message::parse_from_bytes(&buf).map_err(|e| ControllerError::SerializationError(e.to_string()))?;

        Ok(msg)
    }
}

/// Connection to a peer
pub struct PeerConnection {
    /// Peer ID
    peer_id: u64,

    /// Peer address
    addr: SocketAddr,

    /// TCP stream
    stream: Option<TcpStream>,

    /// Send queue
    tx: mpsc::UnboundedSender<Message>,

    /// Receive handler
    rx: mpsc::UnboundedReceiver<Message>,
}

impl PeerConnection {
    /// Create a new peer connection
    pub fn new(peer_id: u64, addr: SocketAddr) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        Self {
            peer_id,
            addr,
            stream: None,
            tx,
            rx,
        }
    }

    /// Connect to the peer
    pub async fn connect(&mut self) -> Result<()> {
        debug!("Connecting to peer {} at {}", self.peer_id, self.addr);

        match TcpStream::connect(self.addr).await {
            Ok(stream) => {
                info!("Successfully connected to peer {} at {}", self.peer_id, self.addr);
                self.stream = Some(stream);
                Ok(())
            }
            Err(e) => {
                warn!("Failed to connect to peer {} at {}: {}", self.peer_id, self.addr, e);
                Err(ControllerError::NetworkError(e.to_string()))
            }
        }
    }

    /// Send a message to the peer
    pub async fn send(&mut self, msg: Message) -> Result<()> {
        // Ensure we're connected
        if self.stream.is_none() {
            self.connect().await?;
        }

        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| ControllerError::NetworkError("Not connected".to_string()))?;

        // Encode message
        let bytes = MessageCodec::encode(&msg)?;

        // Send
        stream.write_all(&bytes).await.map_err(|e| {
            error!("Failed to send message to peer {}: {}", self.peer_id, e);
            self.stream = None; // Reset connection on error
            ControllerError::NetworkError(e.to_string())
        })?;

        debug!("Sent message to peer {}, type: {:?}", self.peer_id, msg.get_msg_type());
        Ok(())
    }

    /// Receive a message from the peer
    pub async fn receive(&mut self) -> Result<Message> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| ControllerError::NetworkError("Not connected".to_string()))?;

        MessageCodec::decode(stream).await
    }

    /// Get the sender channel
    pub fn sender(&self) -> mpsc::UnboundedSender<Message> {
        self.tx.clone()
    }
}

/// Network transport for Raft
pub struct RaftTransport {
    /// Node ID
    node_id: u64,

    /// Listen address
    listen_addr: SocketAddr,

    /// Peer connections
    peers: Arc<RwLock<HashMap<u64, Arc<RwLock<PeerConnection>>>>>,

    /// Message receiver from Raft
    message_tx: mpsc::UnboundedSender<Message>,

    /// Incoming message sender to Raft
    incoming_tx: mpsc::UnboundedSender<Message>,
}

impl RaftTransport {
    /// Create a new transport
    pub fn new(
        node_id: u64,
        listen_addr: SocketAddr,
        peer_addrs: HashMap<u64, SocketAddr>,
    ) -> (Self, mpsc::UnboundedReceiver<Message>, mpsc::UnboundedReceiver<Message>) {
        let (message_tx, message_rx) = mpsc::unbounded_channel();
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();

        let mut peers = HashMap::new();
        for (peer_id, addr) in peer_addrs {
            if peer_id != node_id {
                let conn = PeerConnection::new(peer_id, addr);
                peers.insert(peer_id, Arc::new(RwLock::new(conn)));
            }
        }

        let transport = Self {
            node_id,
            listen_addr,
            peers: Arc::new(RwLock::new(peers)),
            message_tx,
            incoming_tx,
        };

        (transport, message_rx, incoming_rx)
    }

    /// Start the transport
    pub async fn start(self: Arc<Self>) -> Result<()> {
        info!("Starting Raft transport on {}", self.listen_addr);

        // Start listening for incoming connections
        let self_clone = self.clone();
        tokio::spawn(async move {
            if let Err(e) = self_clone.accept_loop().await {
                error!("Accept loop error: {}", e);
            }
        });

        // Start message sending loop
        let self_clone = self;
        tokio::spawn(async move {
            if let Err(e) = self_clone.send_loop().await {
                error!("Send loop error: {}", e);
            }
        });

        info!("Raft transport started successfully");
        Ok(())
    }

    /// Accept incoming connections
    async fn accept_loop(&self) -> Result<()> {
        let listener = TcpListener::bind(self.listen_addr)
            .await
            .map_err(|e| ControllerError::NetworkError(e.to_string()))?;

        info!("Listening for Raft connections on {}", self.listen_addr);

        loop {
            match listener.accept().await {
                Ok((mut stream, addr)) => {
                    debug!("Accepted connection from {}", addr);

                    let incoming_tx = self.incoming_tx.clone();
                    tokio::spawn(async move {
                        loop {
                            match MessageCodec::decode(&mut stream).await {
                                Ok(msg) => {
                                    debug!("Received message from {}: {:?}", addr, msg.get_msg_type());
                                    if incoming_tx.send(msg).is_err() {
                                        warn!("Failed to forward incoming message");
                                        break;
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to decode message from {}: {}", addr, e);
                                    break;
                                }
                            }
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }

    /// Send messages to peers
    async fn send_loop(&self) -> Result<()> {
        // This will be implemented to actually send messages
        // For now, it's a placeholder
        Ok(())
    }

    /// Send a message to a specific peer
    pub async fn send_to_peer(&self, peer_id: u64, msg: Message) -> Result<()> {
        debug!("Sending message to peer {}", peer_id);

        let peers = self.peers.read().await;
        let peer = peers
            .get(&peer_id)
            .ok_or_else(|| ControllerError::NetworkError(format!("Unknown peer: {}", peer_id)))?;

        let mut conn = peer.write().await;
        conn.send(msg).await
    }

    /// Broadcast a message to all peers
    pub async fn broadcast(&self, msg: Message) -> Result<()> {
        debug!("Broadcasting message to all peers");

        let peers = self.peers.read().await;
        for (peer_id, peer) in peers.iter() {
            let mut conn = peer.write().await;
            if let Err(e) = conn.send(msg.clone()).await {
                warn!("Failed to send message to peer {}: {}", peer_id, e);
            }
        }

        Ok(())
    }

    /// Get the message sender
    pub fn message_sender(&self) -> mpsc::UnboundedSender<Message> {
        self.message_tx.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_peer_connection_creation() {
        let addr: SocketAddr = "127.0.0.1:9876".parse().unwrap();
        let conn = PeerConnection::new(1, addr);
        assert_eq!(conn.peer_id, 1);
        assert_eq!(conn.addr, addr);
    }
}
