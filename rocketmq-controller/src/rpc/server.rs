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

use std::net::SocketAddr;
use std::sync::Arc;

use futures::stream::StreamExt;
use futures::SinkExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio_util::codec::Framed;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::error::Result;
use crate::processor::ProcessorManager;
use crate::rpc::codec::RpcCodec;
use crate::rpc::codec::RpcRequest;
use crate::rpc::codec::RpcResponse;

/// RPC server
///
/// Handles incoming TCP connections from brokers and clients,
/// decodes RPC requests, routes them to appropriate processors,
/// and sends back responses.
pub struct RpcServer {
    /// Listen address
    listen_addr: SocketAddr,

    /// Processor manager
    processor_manager: Arc<ProcessorManager>,

    /// Server state
    state: Arc<RwLock<ServerState>>,
}

/// Server state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServerState {
    /// Server is not started
    Stopped,

    /// Server is running
    Running,

    /// Server is shutting down
    ShuttingDown,
}

impl RpcServer {
    /// Create a new RPC server
    pub fn new(listen_addr: SocketAddr, processor_manager: Arc<ProcessorManager>) -> Self {
        Self {
            listen_addr,
            processor_manager,
            state: Arc::new(RwLock::new(ServerState::Stopped)),
        }
    }

    /// Start the RPC server
    pub async fn start(&self) -> Result<()> {
        // Check state
        {
            let mut state = self.state.write().await;
            if *state != ServerState::Stopped {
                warn!("RPC server already started");
                return Ok(());
            }
            *state = ServerState::Running;
        }

        info!("Starting RPC server on {}", self.listen_addr);

        // Bind to the address
        let listener = TcpListener::bind(self.listen_addr).await?;
        info!("RPC server listening on {}", self.listen_addr);

        // Clone Arc for the task
        let processor_manager = self.processor_manager.clone();
        let state = self.state.clone();

        // Spawn accept loop
        tokio::spawn(async move {
            loop {
                // Check if we should stop
                {
                    let current_state = state.read().await;
                    if *current_state == ServerState::ShuttingDown {
                        info!("RPC server accept loop stopping");
                        break;
                    }
                }

                // Accept new connection
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        debug!("Accepted connection from {}", addr);

                        // Spawn handler for this connection
                        let processor_manager = processor_manager.clone();
                        tokio::spawn(async move {
                            if let Err(e) = Self::handle_connection(stream, addr, processor_manager).await {
                                error!("Error handling connection from {}: {}", addr, e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept connection: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
            }
        });

        Ok(())
    }

    /// Handle a single connection
    async fn handle_connection(
        stream: TcpStream,
        addr: SocketAddr,
        processor_manager: Arc<ProcessorManager>,
    ) -> Result<()> {
        info!("Handling connection from {}", addr);

        // Create framed stream with codec
        let mut framed = Framed::new(stream, RpcCodec::new());

        // Process requests
        while let Some(result) = framed.next().await {
            match result {
                Ok(request) => {
                    debug!(
                        "Received request from {}: id={}, type={:?}",
                        addr, request.request_id, request.request_type
                    );

                    // Process the request
                    let response = Self::process_request(request, &processor_manager).await;

                    // Send response
                    if let Err(e) = framed.send(response).await {
                        error!("Failed to send response to {}: {}", addr, e);
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to decode request from {}: {}", addr, e);
                    break;
                }
            }
        }

        info!("Connection from {} closed", addr);
        Ok(())
    }

    /// Process a single request
    async fn process_request(request: RpcRequest, processor_manager: &Arc<ProcessorManager>) -> RpcResponse {
        debug!(
            "Processing request: id={}, type={:?}",
            request.request_id, request.request_type
        );

        // Process the request
        match processor_manager
            .process_request(request.request_type.clone(), &request.payload)
            .await
        {
            Ok(response_data) => {
                debug!("Request {} processed successfully", request.request_id);
                RpcResponse {
                    request_id: request.request_id,
                    success: true,
                    error: None,
                    payload: response_data,
                }
            }
            Err(e) => {
                error!("Failed to process request {}: {}", request.request_id, e);
                RpcResponse {
                    request_id: request.request_id,
                    success: false,
                    error: Some(e.to_string()),
                    payload: Vec::new(),
                }
            }
        }
    }

    /// Shutdown the RPC server
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down RPC server");

        // Update state
        {
            let mut state = self.state.write().await;
            *state = ServerState::ShuttingDown;
        }

        // Wait a bit for accept loop to stop
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Update state
        {
            let mut state = self.state.write().await;
            *state = ServerState::Stopped;
        }

        info!("RPC server stopped");
        Ok(())
    }

    /// Check if server is running
    pub async fn is_running(&self) -> bool {
        let state = self.state.read().await;
        *state == ServerState::Running
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_server_state() {
        // Test that we can work with server states
        let state = ServerState::Stopped;
        assert_eq!(state, ServerState::Stopped);
        assert_ne!(state, ServerState::Running);

        // Test codec creation
        let _codec = RpcCodec::new();
    }
}
