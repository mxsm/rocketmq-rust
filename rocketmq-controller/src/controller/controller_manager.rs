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

use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::config::ControllerConfig;
use crate::controller::broker_heartbeat_manager::BrokerHeartbeatManager;
use crate::error::Result;
use crate::heartbeat::default_broker_heartbeat_manager::DefaultBrokerHeartbeatManager;
use crate::metadata::MetadataStore;
#[cfg(feature = "metrics")]
use crate::metrics::ControllerMetricsManager;
use crate::processor::ProcessorManager;
use crate::raft::RaftController;

/// Main controller manager
///
/// This is the central component that coordinates all controller operations.
/// It manages:
/// - Raft consensus layer for leader election
/// - Metadata storage for broker and topic information
/// - Broker heartbeat monitoring
/// - Request processing
/// - Metrics collection (optional)
///
/// # Architecture
///
/// ```text
/// ┌────────────────────────────────────────┐
/// │      ControllerManager                 │
/// ├────────────────────────────────────────┤
/// │  - Configuration                       │
/// │  - RaftController (Leader Election)   │
/// │  - MetadataStore                      │
/// │  - HeartbeatManager                   │
/// │  - ProcessorManager                   │
/// │  - MetricsManager (optional)          │
/// └────────────────────────────────────────┘
///          │           │            │
///          ▼           ▼            ▼
///    Leader       Metadata     Heartbeat
///    Election     Storage      Monitoring
/// ```
///
/// # Lifecycle
///
/// 1. **Creation**: `new()` - Initialize all components
/// 2. **Initialization**: `initialize()` - Allocate resources, register listeners
/// 3. **Start**: `start()` - Start all components
/// 4. **Runtime**: Handle requests, monitor brokers, manage metadata
/// 5. **Shutdown**: `shutdown()` - Gracefully stop all components
pub struct ControllerManager {
    /// Configuration
    config: Arc<ControllerConfig>,

    /// Raft controller for consensus and leader election
    raft: Arc<RaftController>,

    /// Metadata store for broker and topic information
    metadata: Arc<MetadataStore>,

    /// Heartbeat manager for broker liveness detection
    heartbeat_manager: Arc<tokio::sync::Mutex<DefaultBrokerHeartbeatManager>>,

    /// Request processor manager
    processor: Arc<ProcessorManager>,

    /// Metrics manager (optional, enabled with "metrics" feature)
    #[cfg(feature = "metrics")]
    metrics_manager: Arc<ControllerMetricsManager>,

    /// Running state
    running: Arc<RwLock<bool>>,

    /// Initialization state
    initialized: Arc<RwLock<bool>>,
}

impl ControllerManager {
    /// Create a new controller manager
    ///
    /// # Arguments
    ///
    /// * `config` - Controller configuration
    ///
    /// # Returns
    ///
    /// Returns a new `ControllerManager` instance
    pub async fn new(config: ControllerConfig) -> Result<Self> {
        let config = Arc::new(config);

        info!("Creating controller manager with config: {:?}", config);

        // Initialize Raft controller for leader election
        let raft = Arc::new(RaftController::new(config.clone()).await?);

        // Initialize metadata store
        let metadata = Arc::new(MetadataStore::new(config.clone()).await?);

        // Initialize heartbeat manager
        let heartbeat_manager = Arc::new(tokio::sync::Mutex::new(
            DefaultBrokerHeartbeatManager::new(config.clone()),
        ));

        // Initialize processor manager
        let processor = Arc::new(ProcessorManager::new(
            config.clone(),
            raft.clone(),
            metadata.clone(),
        ));

        // Initialize metrics manager if feature is enabled
        #[cfg(feature = "metrics")]
        let metrics_manager = {
            info!("Initializing metrics manager");
            ControllerMetricsManager::get_instance(config.clone())
        };

        Ok(Self {
            config,
            raft,
            metadata,
            heartbeat_manager,
            processor,
            #[cfg(feature = "metrics")]
            metrics_manager,
            running: Arc::new(RwLock::new(false)),
            initialized: Arc::new(RwLock::new(false)),
        })
    }

    /// Initialize the controller manager
    ///
    /// This method must be called before `start()`. It performs:
    /// - Resource allocation
    /// - Component initialization
    /// - Lifecycle listener registration
    ///
    /// # Returns
    ///
    /// Returns `Ok(true)` if initialization succeeds, `Ok(false)` or an error otherwise
    pub async fn initialize(&self) -> Result<bool> {
        let mut initialized = self.initialized.write().await;
        if *initialized {
            warn!("Controller manager is already initialized");
            return Ok(true);
        }

        info!("Initializing controller manager...");

        // Initialize heartbeat manager
        {
            let mut hb_manager = self.heartbeat_manager.lock().await;
            BrokerHeartbeatManager::initialize(&mut *hb_manager);
            info!("Heartbeat manager initialized");
        }

        // Initialize Raft controller
        // (Note: Raft initialization might be handled in new(), depending on implementation)

        // Initialize processor manager
        // (Note: Processor initialization is typically done in new())

        // Metrics manager is already initialized via get_instance()
        #[cfg(feature = "metrics")]
        info!("Metrics manager is ready");

        *initialized = true;
        info!("Controller manager initialized successfully");

        Ok(true)
    }

    /// Start the controller manager
    ///
    /// Starts all components in the correct order:
    /// 1. Raft controller (for leader election)
    /// 2. Metadata store
    /// 3. Heartbeat manager (for broker liveness detection)
    /// 4. Processor manager (for request handling)
    /// 5. Metrics collection (optional)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if all components start successfully
    pub async fn start(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            warn!("Controller manager is already running");
            return Ok(());
        }

        let initialized = *self.initialized.read().await;
        if !initialized {
            return Err(crate::error::ControllerError::NotInitialized(
                "Controller manager must be initialized before starting".to_string(),
            ));
        }

        info!("Starting controller manager...");

        // Start Raft controller
        self.raft.start().await?;
        info!("Raft controller started");

        // Start metadata store
        self.metadata.start().await?;
        info!("Metadata store started");

        // Start heartbeat manager
        {
            let mut hb_manager = self.heartbeat_manager.lock().await;
            BrokerHeartbeatManager::start(&mut *hb_manager);
            info!("Heartbeat manager started");
        }

        // Start processor manager
        self.processor.start().await?;
        info!("Processor manager started");

        // Start metrics if enabled
        #[cfg(feature = "metrics")]
        {
            info!("Metrics manager is already running (singleton)");
        }

        *running = true;
        info!("Controller manager started successfully");

        Ok(())
    }

    /// Shutdown the controller manager
    ///
    /// Gracefully shuts down all components in reverse order:
    /// 1. Stop accepting new requests (processor)
    /// 2. Shutdown heartbeat manager
    /// 3. Shutdown metadata store
    /// 4. Shutdown Raft controller
    /// 5. Cleanup metrics (optional)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after shutdown completes
    pub async fn shutdown(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if !*running {
            warn!("Controller manager is not running");
            return Ok(());
        }

        info!("Shutting down controller manager...");

        // Shutdown processor first to stop accepting requests
        if let Err(e) = self.processor.shutdown().await {
            error!("Failed to shutdown processor: {}", e);
        }

        // Shutdown heartbeat manager
        {
            let mut hb_manager = self.heartbeat_manager.lock().await;
            BrokerHeartbeatManager::shutdown(&mut *hb_manager);
            info!("Heartbeat manager shut down");
        }

        // Shutdown metadata store
        if let Err(e) = self.metadata.shutdown().await {
            error!("Failed to shutdown metadata: {}", e);
        }

        // Shutdown Raft controller last
        if let Err(e) = self.raft.shutdown().await {
            error!("Failed to shutdown Raft: {}", e);
        }

        // Metrics manager cleanup is automatic via Drop
        #[cfg(feature = "metrics")]
        info!("Metrics manager will be cleaned up automatically");

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

    /// Check if the controller manager is running
    pub async fn is_running(&self) -> bool {
        *self.running.read().await
    }

    /// Check if the controller manager is initialized
    pub async fn is_initialized(&self) -> bool {
        *self.initialized.read().await
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

    /// Get the configuration
    pub fn config(&self) -> &Arc<ControllerConfig> {
        &self.config
    }

    /// Get the metrics manager (only available with "metrics" feature)
    #[cfg(feature = "metrics")]
    pub fn metrics_manager(&self) -> &Arc<ControllerMetricsManager> {
        &self.metrics_manager
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
    use std::net::SocketAddr;

    use super::*;

    #[tokio::test]
    async fn test_manager_lifecycle() {
        let config = ControllerConfig::new(1, "127.0.0.1:9878".parse::<SocketAddr>().unwrap());

        let manager = ControllerManager::new(config)
            .await
            .expect("Failed to create manager");

        // Test initialization
        assert!(!manager.is_initialized().await);
        assert!(manager.initialize().await.unwrap());
        assert!(manager.is_initialized().await);

        // Test double initialization
        assert!(manager.initialize().await.unwrap());

        // Test running state
        assert!(!manager.is_running().await);
    }

    #[tokio::test]
    async fn test_manager_shutdown() {
        let config = ControllerConfig::new(1, "127.0.0.1:9879".parse::<SocketAddr>().unwrap());

        let manager = ControllerManager::new(config)
            .await
            .expect("Failed to create manager");

        // Initialize first
        manager.initialize().await.expect("Failed to initialize");

        // Shutdown should work even if not started
        manager.shutdown().await.expect("Failed to shutdown");
        assert!(!manager.is_running().await);
    }
}
