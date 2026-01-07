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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::raft_controller::RaftController;
use crate::controller::broker_heartbeat_manager::BrokerHeartbeatManager;
use crate::controller::broker_housekeeping_service::BrokerHousekeepingService;
use crate::controller::Controller;
use crate::error::ControllerError;
use crate::error::Result;
use crate::heartbeat::default_broker_heartbeat_manager::DefaultBrokerHeartbeatManager;
use crate::metadata::MetadataStore;
#[cfg(feature = "metrics")]
use crate::metrics::ControllerMetricsManager;
use crate::processor::controller_request_processor::ControllerRequestProcessor;
use crate::processor::ProcessorManager;
use rocketmq_common::common::controller::ControllerConfig;
use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_remoting::base::channel_event_listener::ChannelEventListener;
use rocketmq_remoting::clients::rocketmq_tokio_client::RocketmqDefaultClient;
use rocketmq_remoting::remoting::RemotingService;
use rocketmq_remoting::remoting_server::rocketmq_tokio_server::RocketMQServer;
use rocketmq_remoting::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_rust::ArcMut;
use tracing::error;
use tracing::info;
use tracing::warn;

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
/// 1. **Creation**: `new()` - Initialize basic components
/// 2. **Initialization**: `initialize()` - Allocate resources, register listeners
/// 3. **Start**: `start()` - Start all components in correct order
/// 4. **Runtime**: Handle requests, monitor brokers, manage metadata
/// 5. **Shutdown**: `shutdown()` - Gracefully stop all components in reverse order
///
/// # Thread Safety
///
/// All methods are thread-safe and can be called from multiple tasks concurrently.
/// Uses AtomicBool for state flags instead of RwLock to minimize lock contention.
pub struct ControllerManager {
    /// Configuration
    config: Arc<ControllerConfig>,

    /// Raft controller for consensus and leader election
    /// Note: Uses ArcMut to allow mutable access via &self
    raft_controller: ArcMut<RaftController>,

    /// Metadata store for broker and topic information
    metadata: Arc<MetadataStore>,

    /// Heartbeat manager for broker liveness detection
    /// Uses Mutex instead of RwLock as it's always exclusively accessed
    heartbeat_manager: ArcMut<DefaultBrokerHeartbeatManager>,

    /// Request processor manager
    processor: Arc<ProcessorManager>,

    /// Remoting server for inbound RPC requests
    remoting_server: Option<RocketMQServer<ControllerRequestProcessor>>,

    /// Remoting client for outbound RPC calls
    remoting_client: ArcMut<RocketmqDefaultClient>,

    /// Metrics manager (optional, enabled with "metrics" feature)
    #[cfg(feature = "metrics")]
    metrics_manager: Arc<ControllerMetricsManager>,

    /// Running state - uses AtomicBool for lock-free reads
    running: Arc<AtomicBool>,

    /// Initialization state - uses AtomicBool for lock-free reads
    initialized: Arc<AtomicBool>,

    broker_housekeeping_service: Option<Arc<BrokerHousekeepingService>>,
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
    /// Returns a new `ControllerManager` instance or an error if component initialization fails
    ///
    /// # Errors
    ///
    /// Returns `ControllerError` if:
    /// - Raft controller creation fails
    /// - Metadata store creation fails
    /// - Configuration is invalid
    pub async fn new(config: ControllerConfig) -> Result<Self> {
        let config = Arc::new(config);

        info!("Creating controller manager with config: {:?}", config);

        // Initialize RocketMQ runtime for Raft controller
        //let runtime = Arc::new(RocketMQRuntime::new_multi(2, "controller-runtime"));

        // Initialize Raft controller for leader election
        // This MUST succeed before proceeding
        // Using OpenRaft implementation by default
        let raft_arc = ArcMut::new(RaftController::new_open_raft(Arc::clone(&config)));

        // Initialize metadata store
        // This MUST succeed before proceeding
        let metadata = Arc::new(
            MetadataStore::new(config.clone())
                .await
                .map_err(|e| ControllerError::Internal(format!("Failed to create metadata store: {}", e)))?,
        );

        // Initialize heartbeat manager
        let heartbeat_manager = ArcMut::new(DefaultBrokerHeartbeatManager::new(config.clone()));

        // Initialize processor manager (needs Arc<RaftController>)
        let processor = Arc::new(ProcessorManager::new(
            config.clone(),
            raft_arc.clone(),
            metadata.clone(),
        ));

        // Initialize remoting server for inbound requests
        let listen_port = config.listen_addr.port() as u32;

        let server_config = ServerConfig {
            listen_port,
            ..Default::default()
        };
        let remoting_server = Some(RocketMQServer::new(Arc::new(server_config)));
        info!("Remoting server created on port {}", listen_port);

        // Initialize remoting client for outbound RPC
        let client_config = TokioClientConfig::default();
        let remoting_client = ArcMut::new(RocketmqDefaultClient::new(
            Arc::new(client_config),
            DefaultRemotingRequestProcessor,
        ));
        info!("Remoting client created");

        // Initialize metrics manager if feature is enabled
        #[cfg(feature = "metrics")]
        let metrics_manager = {
            info!("Initializing metrics manager");
            ControllerMetricsManager::get_instance(config.clone())
        };

        info!("Controller manager created successfully");

        Ok(Self {
            config,
            raft_controller: raft_arc,
            metadata,
            heartbeat_manager,
            processor,
            remoting_server,
            remoting_client,
            #[cfg(feature = "metrics")]
            metrics_manager,
            running: Arc::new(AtomicBool::new(false)),
            initialized: Arc::new(AtomicBool::new(false)),
            broker_housekeeping_service: None,
        })
    }

    /// Initialize the controller manager
    ///
    /// This method must be called before `start()`. It performs:
    /// - Resource allocation
    /// - Component initialization
    /// - Lifecycle listener registration
    /// - Thread pool creation
    /// - Heartbeat manager initialization
    /// - Processor registration
    /// - Metrics initialization
    ///
    /// # Returns
    ///
    /// Returns `Ok(true)` if initialization succeeds, `Ok(false)` if already initialized
    ///
    /// # Errors
    ///
    /// Returns `ControllerError` if initialization fails
    ///
    /// # Thread Safety
    ///
    /// This method is idempotent - calling it multiple times is safe
    pub async fn initialize(mut self: ArcMut<Self>) -> Result<bool> {
        // Check if already initialized using atomic operation
        if self
            .initialized
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            warn!("Controller manager is already initialized");
            return Ok(false);
        }

        info!("Initializing controller manager...");

        // Initialize heartbeat manager
        {
            self.heartbeat_manager.initialize();
            info!("Heartbeat manager initialized");
        }

        // Register broker lifecycle listeners
        // TODO: Implement broker inactive handler and register it
        // self.heartbeat_manager.lock().await.register_broker_lifecycle_listener(Arc::new(...));

        // Initialize broker housekeeping service
        {
            let housekeeping_service = Arc::new(BrokerHousekeepingService::new_with_controller_manager(ArcMut::clone(
                &self,
            )));
            self.broker_housekeeping_service = Some(housekeeping_service);

            info!("Broker housekeeping service initialized");
        }

        // Initialize processor manager (processors are already registered in new())
        info!("Processor manager initialized with built-in processors");

        // Register request processors to remoting server
        self.register_processor();
        info!("Request processors registered to remoting server");

        // Metrics manager is already initialized via get_instance() in new()
        #[cfg(feature = "metrics")]
        info!("Metrics manager is ready");

        info!("Controller manager initialized successfully");
        Ok(true)
    }

    /// Register request processors to the remoting server
    fn register_processor(&self) {
        // Current implementation note:
        // The remoting_server is started with a DefaultRemotingRequestProcessor in start().
        // Once ControllerRequestProcessor is fully implemented and RocketMQServer
        // supports dynamic processor registration, this method should register
        // individual request code handlers.

        info!("Processor registration placeholder - will be implemented once ControllerRequestProcessor is ready");

        // When implemented, this should register:
        // - ControllerAlterSyncStateSet
        // - ControllerElectMaster
        // - ControllerRegisterBroker
        // - ControllerGetReplicaInfo
        // - ControllerGetMetadataInfo
        // - ControllerGetSyncStateData
        // - BrokerHeartbeat
        // - UpdateControllerConfig
        // - GetControllerConfig
        // - CleanBrokerData
        // - ControllerGetNextBrokerId
        // - ControllerApplyBrokerId
    }

    /// Initialize request processors
    ///
    /// Aligned with NameServerRuntime.init_processors():
    /// Creates and configures the ControllerRequestProcessor that handles all
    /// incoming RPC requests from brokers.
    ///
    /// # Arguments
    ///
    /// * `controller_manager` - Arc reference to the ControllerManager
    ///
    /// # Returns
    ///
    /// A configured ControllerRequestProcessor ready to handle requests
    fn init_processors(controller_manager: ArcMut<ControllerManager>) -> ControllerRequestProcessor {
        ControllerRequestProcessor::new(controller_manager)
    }

    /// Start the controller manager
    ///
    /// Starts all components in the correct order:
    /// 1. Raft controller (for leader election)
    /// 2. Heartbeat manager (for broker liveness detection)
    /// 3. Metadata store
    /// 4. Processor manager (for request handling)
    /// 5. Remoting server (for inbound RPC - processors registered in initialize())
    /// 6. Remoting client (for outbound RPC)
    /// 7. Metrics collection (optional)
    ///
    /// # Arguments
    ///
    /// * `self_arc` - Arc reference to self, needed for creating request processors
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if all components start successfully
    ///
    /// # Errors
    ///
    /// Returns `ControllerError` if:
    /// - Controller is not initialized
    /// - Any component fails to start
    ///
    /// # Thread Safety
    ///
    /// This method is idempotent - calling it multiple times is safe
    pub async fn start(mut self: ArcMut<Self>) -> Result<()> {
        // Check if already running using atomic operation
        if self
            .running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            warn!("Controller manager is already running");
            return Ok(());
        }

        // Check if initialized
        if !self.initialized.load(Ordering::SeqCst) {
            // Rollback running state
            self.running.store(false, Ordering::SeqCst);
            return Err(ControllerError::NotInitialized(
                "Controller manager must be initialized before starting".to_string(),
            ));
        }

        info!("Starting controller manager...");

        // Start Raft controller first (critical for leader election)
        if let Err(e) = self.raft_controller.mut_from_ref().startup().await {
            self.running.store(false, Ordering::SeqCst);
            return Err(ControllerError::Internal(format!(
                "Failed to start Raft controller: {}",
                e
            )));
        }
        info!("Raft controller started");

        // Start heartbeat manager (for broker monitoring)
        {
            self.heartbeat_manager.start();
            info!("Heartbeat manager started");
        }

        // Start metadata store
        if let Err(e) = self.metadata.start().await {
            self.running.store(false, Ordering::SeqCst);
            return Err(ControllerError::Internal(format!(
                "Failed to start metadata store: {}",
                e
            )));
        }
        info!("Metadata store started");

        // Start processor manager (for request handling)
        if let Err(e) = self.processor.start().await {
            self.running.store(false, Ordering::SeqCst);
            return Err(ControllerError::Internal(format!(
                "Failed to start processor manager: {}",
                e
            )));
        }
        info!("Processor manager started");

        // Start remoting server (for inbound RPC requests)
        // Reference: NameServerRuntime.start() - register processors then start server
        if let Some(mut server) = self.remoting_server.take() {
            // Create ControllerRequestProcessor using init_processors()
            let request_processor = Self::init_processors(self.clone());
            let broker_housekeeping_service = self
                .broker_housekeeping_service
                .take()
                .map(|service| service as Arc<dyn ChannelEventListener>);
            tokio::spawn(async move {
                server.run(request_processor, broker_housekeeping_service).await;
            });
            info!("Remoting server started with ControllerRequestProcessor");
        }

        // Start remoting client (for outbound RPC calls)
        {
            let weak_client = ArcMut::downgrade(&self.remoting_client);
            self.remoting_client.start(weak_client).await;
            info!("Remoting client started");
        }

        // Metrics are already running if enabled
        #[cfg(feature = "metrics")]
        info!("Metrics manager is already running (singleton)");

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
    /// 5. Cleanup resources
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after shutdown completes
    ///
    /// # Thread Safety
    ///
    /// This method is idempotent - calling it multiple times is safe
    pub async fn shutdown(&self) -> Result<()> {
        // Check if already stopped using atomic operation
        if self
            .running
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            warn!("Controller manager is not running");
            return Ok(());
        }

        info!("Shutting down controller manager...");

        // Shutdown processor first to stop accepting requests
        // Errors are logged but don't stop the shutdown process
        if let Err(e) = self.processor.shutdown().await {
            error!("Failed to shutdown processor: {}", e);
        } else {
            info!("Processor manager shut down");
        }

        // Shutdown heartbeat manager
        {
            self.heartbeat_manager.mut_from_ref().shutdown();
            info!("Heartbeat manager shut down");
        }

        // Shutdown metadata store
        if let Err(e) = self.metadata.shutdown().await {
            error!("Failed to shutdown metadata: {}", e);
        } else {
            info!("Metadata store shut down");
        }

        // Shutdown remoting client
        {
            self.remoting_client.mut_from_ref().shutdown();
            info!("Remoting client shut down");
        }

        // Shutdown Raft controller last (it coordinates distributed operations)
        if let Err(e) = self.raft_controller.mut_from_ref().shutdown().await {
            error!("Failed to shutdown Raft: {}", e);
        } else {
            info!("Raft controller shut down");
        }

        // Metrics manager cleanup is automatic via Drop
        #[cfg(feature = "metrics")]
        info!("Metrics manager will be cleaned up automatically");

        info!("Controller manager shut down successfully");
        Ok(())
    }

    /// Check if this node is the leader
    ///
    /// # Returns
    ///
    /// true if this node is the Raft leader, false otherwise
    pub fn is_leader(&self) -> bool {
        self.raft_controller.is_leader()
    }

    /// Check if the controller manager is running
    ///
    /// # Returns
    ///
    /// true if running, false otherwise
    ///
    /// This method uses atomic load for lock-free read
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Check if the controller manager is initialized
    ///
    /// # Returns
    ///
    /// true if initialized, false otherwise
    ///
    /// This method uses atomic load for lock-free read
    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::Acquire)
    }

    /// Get the Raft controller
    ///
    /// # Returns
    ///
    /// A reference to the Raft controller
    pub fn raft(&self) -> &RaftController {
        &self.raft_controller
    }

    /// Get the metadata store
    ///
    /// # Returns
    ///
    /// A reference to the metadata store
    pub fn metadata(&self) -> &Arc<MetadataStore> {
        &self.metadata
    }

    /// Get the processor manager
    ///
    /// # Returns
    ///
    /// A reference to the processor manager
    pub fn processor(&self) -> &Arc<ProcessorManager> {
        &self.processor
    }

    /// Get the configuration
    ///
    /// # Returns
    ///
    /// A reference to the controller configuration
    pub fn config(&self) -> &Arc<ControllerConfig> {
        &self.config
    }

    /// Get the metrics manager (only available with "metrics" feature)
    ///
    /// # Returns
    ///
    /// A reference to the metrics manager
    #[cfg(feature = "metrics")]
    pub fn metrics_manager(&self) -> &Arc<ControllerMetricsManager> {
        &self.metrics_manager
    }

    /// Get the controller configuration
    ///
    /// # Returns
    ///
    /// A reference to the controller configuration
    ///
    /// This is an alias for `config()` for API compatibility
    pub fn controller_config(&self) -> &ControllerConfig {
        &self.config
    }

    /// Get the heartbeat manager
    ///
    /// # Returns
    ///
    /// A reference to the heartbeat manager (wrapped in Arc<Mutex>)
    ///
    /// Note: Caller must lock the mutex to access the manager
    pub fn heartbeat_manager(&self) -> &ArcMut<DefaultBrokerHeartbeatManager> {
        &self.heartbeat_manager
    }

    /// Get the remoting client
    ///
    /// # Returns
    ///
    /// A clone of the Arc-wrapped remoting client for making outbound RPC calls
    pub fn remoting_client(&self) -> ArcMut<RocketmqDefaultClient> {
        self.remoting_client.clone()
    }

    pub fn controller(&self) -> &ArcMut<RaftController> {
        &self.raft_controller
    }
}

/// Drop implementation for emergency shutdown
///
/// If the manager is still running when dropped,
/// we attempt an emergency shutdown to clean up resources.
impl Drop for ControllerManager {
    fn drop(&mut self) {
        // Check if still running using atomic load
        if self.running.load(Ordering::Acquire) {
            warn!("Controller manager dropped while running, attempting emergency shutdown");

            // Spawn blocking emergency shutdown
            // We use try_lock to avoid blocking the drop
            let running = self.running.clone();
            let processor = self.processor.clone();
            let mut heartbeat_manager = self.heartbeat_manager.clone();
            let metadata = self.metadata.clone();

            // Clone remoting_client for emergency shutdown
            let mut remoting_client = self.remoting_client.clone();

            // Spawn a task to perform shutdown
            // Note: This is best-effort; drop should not block
            tokio::spawn(async move {
                running.store(false, Ordering::SeqCst);

                // Try to shutdown components
                let _ = processor.shutdown().await;

                BrokerHeartbeatManager::shutdown(heartbeat_manager.as_mut());

                let _ = metadata.shutdown().await;

                // Shutdown remoting client

                remoting_client.shutdown();

                // Note: raft shutdown handled by Drop impl of RaftController

                info!("Emergency shutdown completed");
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;

    #[tokio::test]
    async fn test_manager_lifecycle() {
        let config = ControllerConfig::default().with_node_info(1, "127.0.0.1:9878".parse::<SocketAddr>().unwrap());

        let manager = ControllerManager::new(config).await.expect("Failed to create manager");
        let manager_arc = ArcMut::new(manager);

        // Test initialization state (should use non-async is_initialized now)
        assert!(!manager_arc.is_initialized());
        assert!(manager_arc.clone().initialize().await.expect("Failed to initialize"));
        assert!(manager_arc.is_initialized());

        // Test double initialization (should return Ok(false))
        assert!(!manager_arc
            .clone()
            .initialize()
            .await
            .expect("Double initialization failed"));

        // Test running state (should use non-async is_running now)
        assert!(!manager_arc.is_running());

        // Prevent dropping runtime in async context
        std::mem::forget(manager_arc);
    }

    #[tokio::test]
    async fn test_manager_shutdown() {
        let config = ControllerConfig::default().with_node_info(1, "127.0.0.1:9879".parse::<SocketAddr>().unwrap());

        let manager = ControllerManager::new(config).await.expect("Failed to create manager");
        let manager_arc = ArcMut::new(manager);

        // Initialize first
        manager_arc.clone().initialize().await.expect("Failed to initialize");

        // Test shutdown without starting (should succeed)
        manager_arc.shutdown().await.expect("Failed to shutdown");

        // Prevent dropping runtime in async context
        std::mem::forget(manager_arc);
    }

    #[tokio::test]
    async fn test_start_without_initialize() {
        let config = ControllerConfig::default().with_node_info(1, "127.0.0.1:9880".parse::<SocketAddr>().unwrap());

        let manager = ControllerManager::new(config).await.expect("Failed to create manager");
        let manager_arc = ArcMut::new(manager);

        // Try to start without initializing (should fail)
        let result = manager_arc.clone().start().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ControllerError::NotInitialized(_)));

        // Prevent dropping runtime in async context
        std::mem::forget(manager_arc);
    }

    #[tokio::test]
    async fn test_atomic_state_checks() {
        let config = ControllerConfig::default().with_node_info(1, "127.0.0.1:9881".parse::<SocketAddr>().unwrap());

        let manager = ControllerManager::new(config).await.expect("Failed to create manager");

        // Test that is_initialized and is_running don't need await
        let _ = manager.is_initialized();
        let _ = manager.is_running();

        // These should compile and run successfully
        assert!(!manager.is_initialized());
        assert!(!manager.is_running());

        // Prevent dropping runtime in async context
        std::mem::forget(manager);
    }
}
