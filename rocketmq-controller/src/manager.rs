/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::config::ControllerConfig;
use crate::error::Result;
use crate::metadata::MetadataStore;
use crate::processor::ProcessorManager;
use crate::raft::RaftController;
use crate::rpc::RpcServer;

/// Main controller manager
///
/// This is the central component that coordinates all controller operations.
/// It manages:
/// - Raft consensus layer
/// - Metadata storage
/// - Request processing
/// - RPC server
/// - Lifecycle management
pub struct ControllerManager {
    /// Configuration
    config: Arc<ControllerConfig>,

    /// Raft controller
    raft: Arc<RaftController>,

    /// Metadata store
    metadata: Arc<MetadataStore>,

    /// Request processor
    processor: Arc<ProcessorManager>,

    /// RPC server
    rpc_server: Arc<RpcServer>,

    /// Running state
    running: Arc<RwLock<bool>>,
}

impl ControllerManager {
    /// Create a new controller manager
    pub async fn new(config: ControllerConfig) -> Result<Self> {
        let config = Arc::new(config);

        info!("Initializing controller manager with config: {:?}", config);

        // Initialize Raft controller
        let raft = Arc::new(RaftController::new(config.clone()).await?);

        // Initialize metadata store
        let metadata = Arc::new(MetadataStore::new(config.clone()).await?);

        // Initialize processor manager
        let processor = Arc::new(ProcessorManager::new(
            config.clone(),
            raft.clone(),
            metadata.clone(),
        ));

        // Initialize RPC server
        let rpc_server = Arc::new(RpcServer::new(config.listen_addr, processor.clone()));

        Ok(Self {
            config,
            raft,
            metadata,
            processor,
            rpc_server,
            running: Arc::new(RwLock::new(false)),
        })
    }

    /// Start the controller
    pub async fn start(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            warn!("Controller is already running");
            return Ok(());
        }

        info!("Starting controller manager...");

        // Start Raft controller
        self.raft.start().await?;

        // Start metadata store
        self.metadata.start().await?;

        // Start processor manager
        self.processor.start().await?;

        // Start RPC server
        self.rpc_server.start().await?;

        *running = true;
        info!("Controller manager started successfully");

        Ok(())
    }

    /// Shutdown the controller
    pub async fn shutdown(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if !*running {
            warn!("Controller is not running");
            return Ok(());
        }

        info!("Shutting down controller manager...");

        // Shutdown RPC server first to stop accepting requests
        if let Err(e) = self.rpc_server.shutdown().await {
            error!("Failed to shutdown RPC server: {}", e);
        }

        // Shutdown processor
        if let Err(e) = self.processor.shutdown().await {
            error!("Failed to shutdown processor: {}", e);
        }

        // Shutdown metadata store
        if let Err(e) = self.metadata.shutdown().await {
            error!("Failed to shutdown metadata store: {}", e);
        }

        // Shutdown Raft last
        if let Err(e) = self.raft.shutdown().await {
            error!("Failed to shutdown Raft controller: {}", e);
        }

        *running = false;
        info!("Controller manager shut down successfully");

        Ok(())
    }

    /// Check if this node is the leader
    pub async fn is_leader(&self) -> bool {
        self.raft.is_leader().await
    }

    /// Get the current leader ID
    pub async fn get_leader(&self) -> Option<u64> {
        self.raft.get_leader().await
    }

    /// Check if the controller is running
    pub async fn is_running(&self) -> bool {
        *self.running.read().await
    }

    /// Get the Raft controller
    pub fn raft(&self) -> &Arc<RaftController> {
        &self.raft
    }

    /// Get the metadata store
    pub fn metadata(&self) -> &Arc<MetadataStore> {
        &self.metadata
    }

    /// Get the processor manager
    pub fn processor(&self) -> &Arc<ProcessorManager> {
        &self.processor
    }
}

impl Drop for ControllerManager {
    fn drop(&mut self) {
        // Best effort shutdown on drop
        let running = self.running.clone();
        let processor = self.processor.clone();
        let metadata = self.metadata.clone();
        let raft = self.raft.clone();

        tokio::spawn(async move {
            let is_running = *running.read().await;
            if is_running {
                warn!("Controller manager dropped while running, performing emergency shutdown");
                let _ = processor.shutdown().await;
                let _ = metadata.shutdown().await;
                let _ = raft.shutdown().await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_manager_lifecycle() {
        // This is a placeholder test
        // Real tests will be added after implementing the dependencies
        assert!(true);
    }
}
