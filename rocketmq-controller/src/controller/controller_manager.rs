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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use super::raft_controller::RaftController;
use crate::controller::broker_heartbeat_manager::BrokerHeartbeatManager;
use crate::controller::broker_housekeeping_service::BrokerHousekeepingService;
use crate::controller::Controller;
use crate::error::ControllerError;
use crate::error::Result;
use crate::heartbeat::default_broker_heartbeat_manager::DefaultBrokerHeartbeatManager;
use crate::helper::broker_lifecycle_listener::BrokerLifecycleListener;
use crate::metadata::MetadataStore;
#[cfg(feature = "metrics")]
use crate::metrics::ControllerMetricsManager;
use crate::processor::controller_request_processor::ControllerRequestProcessor;
use crate::processor::ProcessorManager;
use cheetah_string::CheetahString;
use parking_lot::Mutex;
use parking_lot::RwLock;
use rocketmq_common::common::controller::ControllerConfig;
use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::base::channel_event_listener::ChannelEventListener;
use rocketmq_remoting::clients::rocketmq_tokio_client::RocketmqDefaultClient;
use rocketmq_remoting::clients::RemotingClient;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::elect_master_response_body::ElectMasterResponseBody;
use rocketmq_remoting::protocol::body::sync_state_set_body::SyncStateSet;
use rocketmq_remoting::protocol::header::controller::elect_master_request_header::ElectMasterRequestHeader;
use rocketmq_remoting::protocol::header::elect_master_response_header::ElectMasterResponseHeader;
use rocketmq_remoting::protocol::header::notify_broker_role_change_request_header::NotifyBrokerRoleChangedRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::remoting::RemotingService;
use rocketmq_remoting::remoting_server::rocketmq_tokio_server::RocketMQServer;
use rocketmq_remoting::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::error;
use tracing::info;
use tracing::warn;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct NotifyCacheKey {
    cluster_name: String,
    broker_name: String,
    broker_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NotifyCacheState {
    master_broker_id: u64,
    master_epoch: i32,
    sync_state_set_epoch: i32,
    master_address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NotifyTask {
    cache_key: NotifyCacheKey,
    cache_state: NotifyCacheState,
    broker_addr: CheetahString,
    master_address: Option<CheetahString>,
    sync_state_set: Vec<u8>,
    attempt: u32,
    generation: u64,
}

impl NotifyTask {
    fn build_request(&self) -> RemotingCommand {
        let request_header = NotifyBrokerRoleChangedRequestHeader {
            master_address: self.master_address.clone(),
            master_epoch: Some(self.cache_state.master_epoch),
            sync_state_set_epoch: Some(self.cache_state.sync_state_set_epoch),
            master_broker_id: Some(self.cache_state.master_broker_id),
        };
        RemotingCommand::create_request_command(RequestCode::NotifyBrokerRoleChanged, request_header)
            .set_body(self.sync_state_set.clone())
    }

    fn retry(&self) -> Self {
        let mut next = self.clone();
        next.attempt += 1;
        next
    }

    #[cfg(test)]
    fn new_for_test(cache_key: NotifyCacheKey, cache_state: NotifyCacheState, generation: u64) -> Self {
        Self {
            broker_addr: CheetahString::from_static_str("127.0.0.1:10911"),
            master_address: cache_state.master_address.clone().map(CheetahString::from_string),
            sync_state_set: Vec::new(),
            cache_key,
            cache_state,
            attempt: 0,
            generation,
        }
    }
}

struct BrokerInactiveListener {
    controller_manager: WeakArcMut<ControllerManager>,
}

impl BrokerInactiveListener {
    fn new(controller_manager: WeakArcMut<ControllerManager>) -> Self {
        Self { controller_manager }
    }
}

impl BrokerLifecycleListener for BrokerInactiveListener {
    fn on_broker_inactive(&self, cluster_name: &str, broker_name: &str, broker_id: i64) {
        let Some(controller_manager) = self.controller_manager.upgrade() else {
            return;
        };

        let cluster_name = CheetahString::from_string(cluster_name.to_owned());
        let broker_name = CheetahString::from_string(broker_name.to_owned());

        tokio::spawn(async move {
            if !controller_manager.is_leader() {
                warn!(
                    "Broker inactive event ignored on follower controller, cluster={}, broker={}, broker_id={}",
                    cluster_name, broker_name, broker_id
                );
                return;
            }

            let request =
                ElectMasterRequestHeader::new(cluster_name.clone(), broker_name.clone(), -1, false, current_millis());

            match controller_manager.controller().elect_master(&request).await {
                Ok(Some(response)) if response.code() == ResponseCode::Success as i32 => {
                    info!(
                        "Triggered controller-side elect-master after broker inactive, cluster={}, broker={}, \
                         broker_id={}",
                        cluster_name, broker_name, broker_id
                    );

                    if controller_manager.controller_config().notify_broker_role_changed {
                        if let Err(error) = controller_manager.notify_broker_role_changed(response).await {
                            warn!(
                                "Failed to notify brokers after role change, cluster={}, broker={}, error={}",
                                cluster_name, broker_name, error
                            );
                        }
                    }
                }
                Ok(Some(response)) => {
                    warn!(
                        "Elect-master after broker inactive did not succeed, cluster={}, broker={}, broker_id={}, \
                         code={}, remark={:?}",
                        cluster_name,
                        broker_name,
                        broker_id,
                        response.code(),
                        response.remark()
                    );
                }
                Ok(None) => {
                    warn!(
                        "Elect-master after broker inactive returned no response, cluster={}, broker={}, broker_id={}",
                        cluster_name, broker_name, broker_id
                    );
                }
                Err(error) => {
                    error!(
                        "Elect-master after broker inactive failed, cluster={}, broker={}, broker_id={}, error={}",
                        cluster_name, broker_name, broker_id, error
                    );
                }
            }
        });
    }
}

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
    config: ArcMut<ControllerConfig>,

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
    remoting_server_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    remoting_server_shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,

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
    leadership_watch_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    notify_worker_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    notify_dispatch_tx: Arc<Mutex<Option<mpsc::UnboundedSender<NotifyTask>>>>,
    notify_cache: Arc<RwLock<HashMap<NotifyCacheKey, NotifyCacheState>>>,
    pending_notify_state: Arc<RwLock<HashMap<NotifyCacheKey, NotifyCacheState>>>,
    notify_generation: Arc<AtomicU64>,
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
        let config = ArcMut::new(config);

        info!("Creating controller manager with config: {:?}", config);

        // Initialize heartbeat manager
        let heartbeat_manager = ArcMut::new(DefaultBrokerHeartbeatManager::new(config.clone()));

        // Initialize RocketMQ runtime for Raft controller
        //let runtime = Arc::new(RocketMQRuntime::new_multi(2, "controller-runtime"));

        // Initialize Raft controller for leader election.
        // The controller and request processor must share the same heartbeat manager so that
        // liveness-aware paths observe the broker heartbeats recorded by RPC handlers.
        let raft_arc = ArcMut::new(RaftController::new_open_raft_with_heartbeat(
            config.clone(),
            heartbeat_manager.clone(),
        ));

        // Initialize metadata store
        // This MUST succeed before proceeding
        let metadata = Arc::new(
            MetadataStore::new(config.clone())
                .await
                .map_err(|e| ControllerError::Internal(format!("Failed to create metadata store: {}", e)))?,
        );

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
            remoting_server_handle: Arc::new(Mutex::new(None)),
            remoting_server_shutdown_tx: Arc::new(Mutex::new(None)),
            remoting_client,
            #[cfg(feature = "metrics")]
            metrics_manager,
            running: Arc::new(AtomicBool::new(false)),
            initialized: Arc::new(AtomicBool::new(false)),
            broker_housekeeping_service: None,
            leadership_watch_task: Arc::new(Mutex::new(None)),
            notify_worker_task: Arc::new(Mutex::new(None)),
            notify_dispatch_tx: Arc::new(Mutex::new(None)),
            notify_cache: Arc::new(RwLock::new(HashMap::new())),
            pending_notify_state: Arc::new(RwLock::new(HashMap::new())),
            notify_generation: Arc::new(AtomicU64::new(0)),
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
        {
            let inactive_listener = Arc::new(BrokerInactiveListener::new(ArcMut::downgrade(&self)));
            self.heartbeat_manager
                .register_broker_lifecycle_listener(inactive_listener.clone());
            self.raft_controller
                .register_broker_lifecycle_listener(inactive_listener);
            info!("Broker inactive listener registered");
        }

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
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            *self.remoting_server_shutdown_tx.lock() = Some(shutdown_tx);
            let handle = tokio::spawn(async move {
                server
                    .run_with_shutdown(request_processor, broker_housekeeping_service, async move {
                        let _ = shutdown_rx.await;
                    })
                    .await;
            });
            *self.remoting_server_handle.lock() = Some(handle);
            info!("Remoting server started with ControllerRequestProcessor");
        }

        // Start remoting client (for outbound RPC calls)
        {
            let weak_client = ArcMut::downgrade(&self.remoting_client);
            self.remoting_client.start(weak_client).await;
            info!("Remoting client started");
        }

        self.start_notify_worker_loop().await;
        self.start_leadership_watch_loop();

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

        if let Some(handle) = self.leadership_watch_task.lock().take() {
            handle.abort();
        }
        if let Some(tx) = self.notify_dispatch_tx.lock().take() {
            drop(tx);
        }
        if let Some(handle) = self.notify_worker_task.lock().take() {
            handle.abort();
        }
        if let Some(shutdown_tx) = self.remoting_server_shutdown_tx.lock().take() {
            let _ = shutdown_tx.send(());
        }
        if let Err(error) = self.apply_leadership_state(false).await {
            warn!("Failed to stop leader-only scheduling during shutdown: {}", error);
        }

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

        let remoting_server_handle = { self.remoting_server_handle.lock().take() };
        if let Some(handle) = remoting_server_handle {
            match tokio::time::timeout(Duration::from_secs(10), handle).await {
                Ok(Ok(_)) => info!("Remoting server shut down"),
                Ok(Err(error)) => warn!("Remoting server task exited with error: {}", error),
                Err(_) => warn!("Timed out waiting for remoting server shutdown"),
            }
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
    pub fn config(&self) -> ArcMut<ControllerConfig> {
        self.config.clone()
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

    fn scheduling_enabled(&self) -> bool {
        self.raft_controller.scheduling_enabled()
    }

    pub fn set_raft_runtime_tick_enabled(&self, enabled: bool) -> Result<()> {
        self.raft_controller.set_runtime_tick_enabled(enabled)
    }

    pub fn set_raft_runtime_heartbeat_enabled(&self, enabled: bool) -> Result<()> {
        self.raft_controller.set_runtime_heartbeat_enabled(enabled)
    }

    pub fn set_raft_runtime_elect_enabled(&self, enabled: bool) -> Result<()> {
        self.raft_controller.set_runtime_elect_enabled(enabled)
    }

    fn start_leadership_watch_loop(self: &ArcMut<Self>) {
        if self.leadership_watch_task.lock().is_some() {
            return;
        }

        let weak_manager = ArcMut::downgrade(self);
        let interval = Duration::from_millis(self.config.heartbeat_interval_ms.max(100));
        let handle = tokio::spawn(async move {
            let mut was_leader = false;
            let mut ticker = tokio::time::interval(interval);

            loop {
                ticker.tick().await;

                let Some(manager) = weak_manager.upgrade() else {
                    break;
                };

                if !manager.is_running() {
                    break;
                }

                let is_leader = manager.is_leader();
                if is_leader == was_leader {
                    continue;
                }

                if let Err(error) = manager.apply_leadership_state(is_leader).await {
                    warn!("Failed to apply leadership state transition: {}", error);
                } else {
                    was_leader = is_leader;
                }
            }
        });

        *self.leadership_watch_task.lock() = Some(handle);
    }

    async fn apply_leadership_state(&self, is_leader: bool) -> Result<()> {
        if is_leader {
            self.raft_controller.start_scheduling().await.map_err(|error| {
                ControllerError::Internal(format!("Failed to start controller scheduling: {}", error))
            })?;
            info!("Leader-only scheduling enabled on controller {}", self.config.node_id);
        } else {
            self.raft_controller.stop_scheduling().await.map_err(|error| {
                ControllerError::Internal(format!("Failed to stop controller scheduling: {}", error))
            })?;
            self.reset_notify_dispatch_state();
            info!(
                "Leader-only scheduling disabled and notify dispatch state cleared on controller {}",
                self.config.node_id
            );
        }
        Ok(())
    }

    fn is_same_or_newer_notify_state(previous: &NotifyCacheState, current: &NotifyCacheState) -> bool {
        previous.master_epoch > current.master_epoch
            || (previous.master_epoch == current.master_epoch
                && previous.sync_state_set_epoch >= current.sync_state_set_epoch
                && previous.master_broker_id == current.master_broker_id
                && previous.master_address == current.master_address)
    }

    fn can_notify_broker_role_change(&self, cache_key: &NotifyCacheKey, cache_state: &NotifyCacheState) -> bool {
        let notify_cache = self.notify_cache.read();
        !matches!(
            notify_cache.get(cache_key),
            Some(previous)
                if Self::is_same_or_newer_notify_state(previous, cache_state)
        )
    }

    fn record_notified_broker_role_change(&self, cache_key: NotifyCacheKey, cache_state: NotifyCacheState) {
        self.notify_cache.write().insert(cache_key, cache_state);
    }

    fn notify_generation(&self) -> u64 {
        self.notify_generation.load(Ordering::SeqCst)
    }

    fn stage_notify_task(&self, task: &NotifyTask) -> bool {
        let mut pending_notify_state = self.pending_notify_state.write();
        match pending_notify_state.get(&task.cache_key) {
            Some(previous) if Self::is_same_or_newer_notify_state(previous, &task.cache_state) => false,
            _ => {
                pending_notify_state.insert(task.cache_key.clone(), task.cache_state.clone());
                true
            }
        }
    }

    fn should_process_notify_task(&self, task: &NotifyTask) -> bool {
        self.notify_generation() == task.generation
            && self.pending_notify_state.read().get(&task.cache_key) == Some(&task.cache_state)
            && self.can_notify_broker_role_change(&task.cache_key, &task.cache_state)
    }

    fn complete_notify_task(&self, task: &NotifyTask) {
        if !self.should_process_notify_task(task) {
            return;
        }
        self.record_notified_broker_role_change(task.cache_key.clone(), task.cache_state.clone());
        let mut pending_notify_state = self.pending_notify_state.write();
        if pending_notify_state.get(&task.cache_key) == Some(&task.cache_state) {
            pending_notify_state.remove(&task.cache_key);
        }
    }

    fn reset_notify_dispatch_state(&self) {
        self.notify_generation.fetch_add(1, Ordering::SeqCst);
        self.pending_notify_state.write().clear();
        self.notify_cache.write().clear();
    }

    async fn enqueue_notify_task(&self, task: NotifyTask) -> Result<()> {
        let sender = self
            .notify_dispatch_tx
            .lock()
            .clone()
            .ok_or_else(|| ControllerError::Internal("Notify worker is not initialized".to_string()))?;
        sender
            .send(task)
            .map_err(|error| ControllerError::Internal(format!("Failed to enqueue notify task: {}", error)))
    }

    fn notify_retry_delay(&self, attempt: u32) -> Duration {
        let base_delay = self.config.heartbeat_interval_ms.max(100);
        Duration::from_millis((base_delay * u64::from(attempt + 1)).min(2_000))
    }

    fn should_retry_notify_task(&self, task: &NotifyTask) -> bool {
        const MAX_NOTIFY_ATTEMPTS: u32 = 3;
        self.is_running()
            && self.is_leader()
            && task.attempt + 1 < MAX_NOTIFY_ATTEMPTS
            && self.should_process_notify_task(task)
    }

    fn schedule_notify_retry(&self, task: NotifyTask) {
        let notify_dispatch_tx = self.notify_dispatch_tx.clone();
        let notify_generation = self.notify_generation.clone();
        let delay = self.notify_retry_delay(task.attempt);
        tokio::spawn(async move {
            sleep(delay).await;
            if notify_generation.load(Ordering::SeqCst) != task.generation {
                return;
            }
            if let Some(sender) = notify_dispatch_tx.lock().clone() {
                let _ = sender.send(task.retry());
            }
        });
    }

    async fn process_notify_task(&self, task: NotifyTask) {
        if !self.is_running() || !self.is_leader() || !self.should_process_notify_task(&task) {
            return;
        }

        match self
            .remoting_client
            .invoke_request(Some(&task.broker_addr), task.build_request(), 3000)
            .await
        {
            Ok(response) if response.code() == ResponseCode::Success as i32 => {
                self.complete_notify_task(&task);
                info!(
                    "Notified broker role change, target={}, broker_id={}, broker={}",
                    task.broker_addr, task.cache_key.broker_id, task.cache_key.broker_name
                );
            }
            Ok(response) => {
                warn!(
                    "Broker role notify did not succeed, target={}, broker_id={}, broker={}, code={}, remark={:?}",
                    task.broker_addr,
                    task.cache_key.broker_id,
                    task.cache_key.broker_name,
                    response.code(),
                    response.remark()
                );
                if self.should_retry_notify_task(&task) {
                    self.schedule_notify_retry(task);
                }
            }
            Err(error) => {
                warn!(
                    "Failed to notify broker role change, target={}, broker_id={}, broker={}, error={}",
                    task.broker_addr, task.cache_key.broker_id, task.cache_key.broker_name, error
                );
                if self.should_retry_notify_task(&task) {
                    self.schedule_notify_retry(task);
                }
            }
        }
    }

    async fn start_notify_worker_loop(self: &ArcMut<Self>) {
        if self.notify_worker_task.lock().is_some() {
            return;
        }

        let (sender, mut receiver) = mpsc::unbounded_channel();
        *self.notify_dispatch_tx.lock() = Some(sender);

        let weak_manager = ArcMut::downgrade(self);
        let handle = tokio::spawn(async move {
            while let Some(task) = receiver.recv().await {
                let Some(manager) = weak_manager.upgrade() else {
                    break;
                };
                manager.process_notify_task(task).await;
            }
        });

        *self.notify_worker_task.lock() = Some(handle);
    }

    async fn notify_broker_role_changed(&self, response: RemotingCommand) -> Result<()> {
        let response_header = response
            .decode_command_custom_header::<ElectMasterResponseHeader>()
            .map_err(|error| {
                ControllerError::Internal(format!(
                    "Failed to decode elect-master response header for broker role notify: {:?}",
                    error
                ))
            })?;

        let Some(body) = response.body() else {
            return Ok(());
        };

        let response_body = ElectMasterResponseBody::decode(body).map_err(|error| {
            ControllerError::Internal(format!(
                "Failed to decode elect-master response body for broker role notify: {}",
                error
            ))
        })?;

        let Some(member_group) = response_body.broker_member_group else {
            return Ok(());
        };

        let Some(master_broker_id) = response_header.master_broker_id.and_then(|id| u64::try_from(id).ok()) else {
            warn!(
                "Skip broker role notify because master broker id is absent, broker={}",
                member_group.broker_name
            );
            return Ok(());
        };

        let sync_state_set_epoch = response_header.sync_state_set_epoch.unwrap_or_default();
        let master_epoch = response_header.master_epoch.unwrap_or_default();
        let master_address = response_header.master_address.clone().map(|value| value.to_string());
        let sync_state_set = SyncStateSet::with_values(response_body.sync_state_set, sync_state_set_epoch)
            .encode()
            .map_err(|error| {
                ControllerError::Internal(format!(
                    "Failed to encode sync state set for broker role notify: {}",
                    error
                ))
            })?;

        for (broker_id, broker_addr) in member_group.broker_addrs {
            if !self.heartbeat_manager.is_broker_active(
                &member_group.cluster,
                &member_group.broker_name,
                broker_id as i64,
            ) {
                continue;
            }

            let cache_key = NotifyCacheKey {
                cluster_name: member_group.cluster.to_string(),
                broker_name: member_group.broker_name.to_string(),
                broker_id,
            };
            let cache_state = NotifyCacheState {
                master_broker_id,
                master_epoch,
                sync_state_set_epoch,
                master_address: master_address.clone(),
            };
            if !self.can_notify_broker_role_change(&cache_key, &cache_state) {
                continue;
            }
            let task = NotifyTask {
                cache_key,
                cache_state,
                broker_addr: broker_addr.clone(),
                master_address: response_header.master_address.clone(),
                sync_state_set: sync_state_set.clone(),
                attempt: 0,
                generation: self.notify_generation(),
            };
            if !self.stage_notify_task(&task) {
                continue;
            }
            if let Err(error) = self.enqueue_notify_task(task.clone()).await {
                self.pending_notify_state.write().remove(&task.cache_key);
                warn!(
                    "Failed to enqueue broker role notify, target={}, broker_id={}, broker={}, error={}",
                    broker_addr, broker_id, member_group.broker_name, error
                );
            }
        }

        Ok(())
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
            let leadership_watch_task = self.leadership_watch_task.clone();
            let notify_worker_task = self.notify_worker_task.clone();
            let notify_dispatch_tx = self.notify_dispatch_tx.clone();
            let notify_cache = self.notify_cache.clone();
            let pending_notify_state = self.pending_notify_state.clone();
            let notify_generation = self.notify_generation.clone();
            let remoting_server_handle = self.remoting_server_handle.clone();
            let remoting_server_shutdown_tx = self.remoting_server_shutdown_tx.clone();

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
                if let Some(shutdown_tx) = remoting_server_shutdown_tx.lock().take() {
                    let _ = shutdown_tx.send(());
                }

                remoting_client.shutdown();
                if let Some(handle) = remoting_server_handle.lock().take() {
                    handle.abort();
                }
                if let Some(handle) = leadership_watch_task.lock().take() {
                    handle.abort();
                }
                if let Some(tx) = notify_dispatch_tx.lock().take() {
                    drop(tx);
                }
                if let Some(handle) = notify_worker_task.lock().take() {
                    handle.abort();
                }
                notify_generation.fetch_add(1, Ordering::SeqCst);
                pending_notify_state.write().clear();
                notify_cache.write().clear();

                // Note: raft shutdown handled by Drop impl of RaftController

                info!("Emergency shutdown completed");
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::SocketAddr;

    use super::*;
    use crate::typ::Node;

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

    #[tokio::test]
    async fn test_notify_cache_skips_duplicate_and_stale_role_changes() {
        let config = ControllerConfig::default().with_node_info(1, "127.0.0.1:9882".parse::<SocketAddr>().unwrap());
        let manager = ControllerManager::new(config).await.expect("Failed to create manager");

        let key = NotifyCacheKey {
            cluster_name: "test-cluster".to_string(),
            broker_name: "broker-a".to_string(),
            broker_id: 1,
        };
        let first_state = NotifyCacheState {
            master_broker_id: 1,
            master_epoch: 2,
            sync_state_set_epoch: 3,
            master_address: Some("127.0.0.1:10911".to_string()),
        };

        assert!(manager.can_notify_broker_role_change(&key, &first_state));
        manager.record_notified_broker_role_change(key.clone(), first_state.clone());
        assert!(!manager.can_notify_broker_role_change(&key, &first_state));
        assert!(!manager.can_notify_broker_role_change(
            &key,
            &NotifyCacheState {
                sync_state_set_epoch: 2,
                ..first_state.clone()
            }
        ));
        assert!(manager.can_notify_broker_role_change(
            &key,
            &NotifyCacheState {
                master_epoch: 3,
                ..first_state.clone()
            }
        ));

        manager.apply_leadership_state(false).await.expect("stop scheduling");
        assert!(manager.notify_cache.read().is_empty());

        std::mem::forget(manager);
    }

    #[tokio::test]
    async fn test_notify_dispatch_replaces_stale_pending_task() {
        let config = ControllerConfig::default().with_node_info(1, "127.0.0.1:9884".parse::<SocketAddr>().unwrap());
        let manager = ControllerManager::new(config).await.expect("Failed to create manager");

        let key = NotifyCacheKey {
            cluster_name: "test-cluster".to_string(),
            broker_name: "broker-a".to_string(),
            broker_id: 1,
        };
        let first_state = NotifyCacheState {
            master_broker_id: 1,
            master_epoch: 2,
            sync_state_set_epoch: 3,
            master_address: Some("127.0.0.1:10911".to_string()),
        };
        let second_state = NotifyCacheState {
            master_broker_id: 2,
            master_epoch: 3,
            sync_state_set_epoch: 4,
            master_address: Some("127.0.0.1:10912".to_string()),
        };
        let generation = manager.notify_generation();
        let first_task = NotifyTask::new_for_test(key.clone(), first_state.clone(), generation);
        let second_task = NotifyTask::new_for_test(key.clone(), second_state.clone(), generation);

        manager.stage_notify_task(&first_task);
        manager.stage_notify_task(&second_task);

        assert!(!manager.should_process_notify_task(&first_task));
        assert!(manager.should_process_notify_task(&second_task));

        std::mem::forget(manager);
    }

    #[tokio::test]
    async fn test_notify_dispatch_reset_invalidates_old_generation() {
        let config = ControllerConfig::default().with_node_info(1, "127.0.0.1:9885".parse::<SocketAddr>().unwrap());
        let manager = ControllerManager::new(config).await.expect("Failed to create manager");

        let key = NotifyCacheKey {
            cluster_name: "test-cluster".to_string(),
            broker_name: "broker-a".to_string(),
            broker_id: 1,
        };
        let state = NotifyCacheState {
            master_broker_id: 1,
            master_epoch: 2,
            sync_state_set_epoch: 3,
            master_address: Some("127.0.0.1:10911".to_string()),
        };
        let task = NotifyTask::new_for_test(key.clone(), state.clone(), manager.notify_generation());

        manager.stage_notify_task(&task);
        assert!(manager.should_process_notify_task(&task));

        manager.reset_notify_dispatch_state();

        assert!(!manager.should_process_notify_task(&task));
        assert!(manager.notify_cache.read().is_empty());

        std::mem::forget(manager);
    }

    #[tokio::test]
    async fn test_leadership_watch_enables_scheduling_for_openraft_leader() {
        let port = 9883;
        let config = ControllerConfig::default()
            .with_node_info(1, format!("127.0.0.1:{port}").parse::<SocketAddr>().unwrap())
            .with_heartbeat_interval_ms(100)
            .with_election_timeout_ms(300);

        let manager = ArcMut::new(ControllerManager::new(config).await.expect("Failed to create manager"));
        manager.clone().initialize().await.expect("initialize manager");
        manager.clone().start().await.expect("start manager");

        let mut nodes = BTreeMap::new();
        nodes.insert(
            1,
            Node {
                node_id: 1,
                rpc_addr: format!("127.0.0.1:{port}"),
            },
        );
        manager
            .controller()
            .initialize_cluster(nodes)
            .await
            .expect("initialize single-node cluster");

        for _ in 0..30 {
            if manager.is_leader() && manager.scheduling_enabled() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        assert!(manager.is_leader(), "controller manager should become leader");
        assert!(
            manager.scheduling_enabled(),
            "leadership watcher should enable leader-only scheduling"
        );

        manager.shutdown().await.expect("shutdown manager");
        std::mem::forget(manager);
    }
}
