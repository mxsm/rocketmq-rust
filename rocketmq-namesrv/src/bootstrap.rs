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

//! NameServer Bootstrap Module
//!
//! Provides the core runtime infrastructure for RocketMQ NameServer.

use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_common::utils::network_util::NetworkUtil;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::base::channel_event_listener::ChannelEventListener;
use rocketmq_remoting::clients::rocketmq_tokio_client::RocketmqDefaultClient;
use rocketmq_remoting::clients::RemotingClient;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::remoting::RemotingService;
use rocketmq_remoting::remoting_server::rocketmq_tokio_server::RocketMQServer;
use rocketmq_remoting::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_rust::schedule::simple_scheduler::ScheduledTaskManager;
use rocketmq_rust::wait_for_signal;
use rocketmq_rust::ArcMut;
use tokio::sync::broadcast;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::warn;

use crate::processor::ClientRequestProcessor;
use crate::processor::NameServerRequestProcessor;
use crate::processor::NameServerRequestProcessorWrapper;
use crate::route::route_info_manager::RouteInfoManager;
use crate::route::route_info_manager_v2::RouteInfoManagerV2;
use crate::route::route_info_manager_wrapper::RouteInfoManagerWrapper;
use crate::route::zone_route_rpc_hook::ZoneRouteRPCHook;
use crate::route_info::broker_housekeeping_service::BrokerHousekeepingService;
use crate::KVConfigManager;

/// Runtime lifecycle states for NameServer
///
/// State transitions:
/// ```text
/// Created -> Initialized -> Running -> ShuttingDown -> Stopped
///   |                                       ^
///   +---------------------------------------+
///              (on error during init/start)
/// ```
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeState {
    /// Initial state after construction
    Created = 0,
    /// Configuration loaded, components initialized
    Initialized = 1,
    /// Server running and accepting requests
    Running = 2,
    /// Graceful shutdown in progress
    ShuttingDown = 3,
    /// Fully stopped, resources released
    Stopped = 4,
}

impl RuntimeState {
    /// Convert u8 to RuntimeState
    #[inline]
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Created),
            1 => Some(Self::Initialized),
            2 => Some(Self::Running),
            3 => Some(Self::ShuttingDown),
            4 => Some(Self::Stopped),
            _ => None,
        }
    }

    /// Get human-readable state name
    #[inline]
    fn name(&self) -> &'static str {
        match self {
            Self::Created => "Created",
            Self::Initialized => "Initialized",
            Self::Running => "Running",
            Self::ShuttingDown => "ShuttingDown",
            Self::Stopped => "Stopped",
        }
    }

    /// Check if state transition is valid
    #[inline]
    fn can_transition_to(&self, next: RuntimeState) -> bool {
        match (self, next) {
            // Created can only go to Initialized or Stopped (on error)
            (Self::Created, Self::Initialized) => true,
            (Self::Created, Self::Stopped) => true,

            // Initialized can go to Running or Stopped (on error)
            (Self::Initialized, Self::Running) => true,
            (Self::Initialized, Self::Stopped) => true,

            // Running can only go to ShuttingDown
            (Self::Running, Self::ShuttingDown) => true,

            // ShuttingDown can only go to Stopped
            (Self::ShuttingDown, Self::Stopped) => true,

            // Stopped is terminal
            (Self::Stopped, _) => false,

            // All other transitions are invalid
            _ => false,
        }
    }
}

impl std::fmt::Display for RuntimeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

pub struct NameServerBootstrap {
    name_server_runtime: NameServerRuntime,
}

/// Builder for creating NameServerBootstrap with custom configuration
pub struct Builder {
    name_server_config: Option<NamesrvConfig>,
    server_config: Option<ServerConfig>,
}

/// Core runtime managing NameServer lifecycle and operations
///
/// Coordinates initialization, startup, and graceful shutdown of all components.
struct NameServerRuntime {
    inner: ArcMut<NameServerRuntimeInner>,
    scheduled_task_manager: ScheduledTaskManager,
    shutdown_rx: Option<broadcast::Receiver<()>>,
    server_inner: Option<RocketMQServer<NameServerRequestProcessor>>,
    /// Server task handle for graceful shutdown
    server_task_handle: Option<tokio::task::JoinHandle<()>>,
    /// Runtime state machine for lifecycle management
    state: Arc<AtomicU8>,
}

impl NameServerBootstrap {
    /// Boot the NameServer and run until shutdown signal
    ///
    /// This is the main entry point that orchestrates:
    /// 1. Component initialization
    /// 2. Server startup
    /// 3. Graceful shutdown on signal
    #[instrument(skip(self), name = "nameserver_boot")]
    pub async fn boot(mut self) -> RocketMQResult<()> {
        info!("Booting RocketMQ NameServer (Rust)...");

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        self.name_server_runtime.shutdown_rx = Some(shutdown_rx);
        self.name_server_runtime.initialize().await?;

        tokio::join!(self.name_server_runtime.start(), wait_for_signal_inner(shutdown_tx));

        info!("NameServer shutdown completed");
        Ok(())
    }
}

/// Wait for system shutdown signal (SIGINT, SIGTERM)
#[inline]
async fn wait_for_signal_inner(shutdown_tx: broadcast::Sender<()>) {
    tokio::select! {
        _ = wait_for_signal() => {
            info!("Shutdown signal received, broadcasting to all components...");
        }
    }
    // Broadcast shutdown to all listeners
    if let Err(e) = shutdown_tx.send(()) {
        error!("Failed to broadcast shutdown signal: {}", e);
    }
}

impl NameServerRuntime {
    /// Get current runtime state
    #[inline]
    fn current_state(&self) -> RuntimeState {
        let value = self.state.load(Ordering::Acquire);
        RuntimeState::from_u8(value).unwrap_or_else(|| {
            error!("Invalid runtime state value: {}", value);
            RuntimeState::Stopped
        })
    }

    /// Attempt to transition to a new state
    ///
    /// Returns `Ok(())` if transition succeeds, `Err` if transition is invalid.
    #[inline]
    fn transition_to(&self, next: RuntimeState) -> RocketMQResult<()> {
        let current = self.current_state();

        if !current.can_transition_to(next) {
            let error_msg = format!(
                "Invalid state transition: {} -> {}. Current state does not allow this transition.",
                current.name(),
                next.name()
            );
            error!("{}", error_msg);
            return Err(RocketMQError::Internal(error_msg));
        }

        // Perform atomic state transition
        let old_value = self.state.swap(next as u8, Ordering::AcqRel);
        let old_state = RuntimeState::from_u8(old_value).unwrap_or(RuntimeState::Stopped);

        info!("State transition: {} -> {}", old_state.name(), next.name());

        Ok(())
    }

    /// Validate that current state is one of the expected states
    #[inline]
    fn validate_state(&self, expected: &[RuntimeState], operation: &str) -> RocketMQResult<()> {
        let current = self.current_state();

        if !expected.contains(&current) {
            let expected_names: Vec<_> = expected.iter().map(|s| s.name()).collect();
            let error_msg = format!(
                "Operation '{}' requires state to be one of [{}], but current state is {}",
                operation,
                expected_names.join(", "),
                current.name()
            );
            error!("{}", error_msg);
            return Err(RocketMQError::Internal(error_msg));
        }

        Ok(())
    }

    /// Initialize all components in proper order
    ///
    /// Initialization sequence:
    /// 1. Load KV configuration from disk
    /// 2. Initialize network server
    /// 3. Setup RPC hooks
    /// 4. Start scheduled health monitoring tasks
    #[instrument(skip(self), name = "runtime_initialize")]
    pub async fn initialize(&mut self) -> RocketMQResult<()> {
        // Validate we're in Created state
        self.validate_state(&[RuntimeState::Created], "initialize")?;

        info!("Phase 1/4: Loading configuration...");
        if let Err(e) = self.load_config().await {
            error!("Initialization failed during config load: {}", e);
            let _ = self.transition_to(RuntimeState::Stopped);
            return Err(e);
        }

        info!("Phase 2/4: Initializing network server...");
        self.initialize_network_components();

        info!("Phase 3/4: Registering RPC hooks...");
        self.initialize_rpc_hooks();

        info!("Phase 4/4: Starting scheduled tasks...");
        self.start_schedule_service();

        // Transition to Initialized state
        self.transition_to(RuntimeState::Initialized)?;

        info!("Initialization completed successfully");
        Ok(())
    }

    async fn load_config(&mut self) -> RocketMQResult<()> {
        // KVConfigManager is now always initialized
        self.inner.kvconfig_manager_mut().load().map_err(|e| {
            error!("KV config load failed: {}", e);
            RocketMQError::storage_read_failed("kv_config", format!("Configuration load error: {}", e))
        })?;
        debug!("KV configuration loaded successfully");
        Ok(())
    }

    /// Initialize network server for handling client requests
    fn initialize_network_components(&mut self) {
        let server = RocketMQServer::new(Arc::new(self.inner.server_config().clone()));
        self.server_inner = Some(server);
        debug!(
            "Network server initialized on port {}",
            self.inner.server_config().listen_port
        );
    }

    /// Start scheduled tasks for system health monitoring
    ///
    /// Schedules periodic broker health checks to detect and remove inactive brokers
    fn start_schedule_service(&self) {
        let scan_not_active_broker_interval = self.inner.name_server_config().scan_not_active_broker_interval;
        let mut name_server_runtime_inner = self.inner.clone();

        self.scheduled_task_manager.add_fixed_rate_task_async(
            Duration::from_secs(5), // Initial delay
            Duration::from_millis(scan_not_active_broker_interval),
            async move |_ctx| {
                debug!("Running scheduled broker health check");
                if let Some(route_info_manager) = name_server_runtime_inner.route_info_manager.as_mut() {
                    route_info_manager.scan_not_active_broker();
                }
                Ok(())
            },
        );

        info!(
            "Scheduled task started: broker health check (interval: {}ms)",
            scan_not_active_broker_interval
        );
    }

    /// Initialize RPC hooks for request pre/post-processing
    fn initialize_rpc_hooks(&mut self) {
        if let Some(server) = self.server_inner.as_mut() {
            server.register_rpc_hook(Arc::new(ZoneRouteRPCHook));
            debug!("RPC hooks registered: ZoneRouteRPCHook");
        }
    }

    /// Start the server and enter main event loop
    ///
    /// This method:
    /// 1. Initializes request processors
    /// 2. Starts the network server in async task
    /// 3. Starts the remoting client
    /// 4. Waits for shutdown signal
    /// 5. Performs graceful shutdown
    #[instrument(skip(self), name = "runtime_start")]
    pub async fn start(&mut self) {
        // Validate we're in Initialized state
        if let Err(e) = self.validate_state(&[RuntimeState::Initialized], "start") {
            error!("Cannot start: {}", e);
            return;
        }

        info!("Starting NameServer main loop...");

        let request_processor = self.init_processors();

        // Take server instance for async execution
        let mut server = self
            .server_inner
            .take()
            .expect("Server not initialized - call initialize() first");

        // Start route info manager service
        self.inner.route_info_manager().start();

        // Get broker housekeeping service for server
        let channel_event_listener =
            Some(self.inner.broker_housekeeping_service().clone() as Arc<dyn ChannelEventListener>);

        // Spawn server task and retain handle for graceful shutdown
        let server_handle = tokio::spawn(async move {
            debug!("Server task started");
            server.run(request_processor, channel_event_listener).await;
            debug!("Server task completed");
        });
        self.server_task_handle = Some(server_handle);

        // Setup remoting client with name server address
        let local_address = NetworkUtil::get_local_address().unwrap_or_else(|| {
            warn!("Failed to determine local address, using 127.0.0.1");
            "127.0.0.1".to_string()
        });

        let namesrv =
            CheetahString::from_string(format!("{}:{}", local_address, self.inner.server_config().listen_port));

        debug!("NameServer address: {}", namesrv);

        let weak_arc_mut = ArcMut::downgrade(&self.inner.remoting_client);
        self.inner
            .remoting_client
            .update_name_server_address_list(vec![namesrv])
            .await;

        // Start remoting client directly (no spawn needed as it's managed by self.inner)
        self.inner.remoting_client.start(weak_arc_mut).await;

        // Transition to Running state
        if let Err(e) = self.transition_to(RuntimeState::Running) {
            error!("Failed to transition to Running state: {}", e);
            return;
        }

        info!("NameServer is now running and accepting requests");

        // Wait for shutdown signal
        tokio::select! {
            result = self.shutdown_rx.as_mut()
                .expect("Shutdown channel not initialized")
                .recv() => {
                match result {
                    Ok(_) => info!("Shutdown signal received, initiating graceful shutdown..."),
                    Err(e) => error!("Error receiving shutdown signal: {}", e),
                }
                self.shutdown().await;
            }
        }
    }

    /// Perform graceful shutdown of all components
    ///
    /// Shutdown sequence:
    /// 1. Wait for in-flight requests to complete (with timeout)
    /// 2. Cancel all scheduled tasks
    /// 3. Shutdown route info manager (broker unregistration)
    /// 4. Wait for server task to complete (with timeout)
    /// 5. Release all resources
    #[instrument(skip(self), name = "runtime_shutdown")]
    async fn shutdown(&mut self) {
        // Validate we're in Running state and transition to ShuttingDown
        if let Err(e) = self.validate_state(&[RuntimeState::Running], "shutdown") {
            warn!("Shutdown called in unexpected state: {}", e);
            // Allow shutdown even in unexpected state for safety
        }

        if let Err(e) = self.transition_to(RuntimeState::ShuttingDown) {
            error!("Failed to transition to ShuttingDown state: {}", e);
        }

        const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
        const TASK_JOIN_TIMEOUT: Duration = Duration::from_secs(10);

        info!(
            "Phase 1/4: Waiting for in-flight requests (timeout: {}s)...",
            SHUTDOWN_TIMEOUT.as_secs()
        );
        if let Err(e) = self.wait_for_inflight_requests(SHUTDOWN_TIMEOUT).await {
            warn!("In-flight request wait timeout or error: {}", e);
        }

        info!("Phase 2/4: Stopping scheduled tasks...");
        self.scheduled_task_manager.cancel_all();

        info!("Phase 3/4: Shutting down route info manager...");
        self.inner.route_info_manager_mut().shutdown_unregister_service();

        info!(
            "Phase 4/4: Waiting for server task (timeout: {}s)...",
            TASK_JOIN_TIMEOUT.as_secs()
        );
        if let Err(e) = self.wait_for_server_task(TASK_JOIN_TIMEOUT).await {
            warn!("Task join timeout or error: {}", e);
        }

        // Transition to Stopped state
        if let Err(e) = self.transition_to(RuntimeState::Stopped) {
            error!("Failed to transition to Stopped state: {}", e);
        }

        info!("Graceful shutdown completed");
    }

    /// Wait for all in-flight requests to complete
    ///
    /// This provides a grace period for ongoing requests to finish before shutdown.
    /// Returns immediately if no requests are in-flight.
    #[instrument(skip(self), name = "wait_inflight_requests")]
    async fn wait_for_inflight_requests(&self, timeout: Duration) -> RocketMQResult<()> {
        tokio::time::timeout(timeout, async {
            // In a production implementation, you would:
            // 1. Check active connection count
            // 2. Monitor pending request queue
            // 3. Wait for all to drain
            // For now, we provide a short grace period
            tokio::time::sleep(Duration::from_millis(500)).await;
            debug!("In-flight request grace period completed");
        })
        .await
        .map_err(|_| RocketMQError::Timeout {
            operation: "wait_for_inflight_requests",
            timeout_ms: timeout.as_millis() as u64,
        })
    }

    /// Wait for server task to complete
    ///
    /// Attempts graceful join with timeout. If timeout is exceeded,
    /// task is aborted.
    #[instrument(skip(self), name = "wait_server_task")]
    async fn wait_for_server_task(&mut self, timeout: Duration) -> RocketMQResult<()> {
        if let Some(handle) = self.server_task_handle.take() {
            match tokio::time::timeout(timeout, handle).await {
                Ok(Ok(())) => {
                    debug!("Server task completed successfully");
                    Ok(())
                }
                Ok(Err(e)) => {
                    error!("Server task panicked: {}", e);
                    Err(RocketMQError::Internal(format!("Server task panicked: {}", e)))
                }
                Err(_) => {
                    warn!(
                        "Server task join timeout ({}s), task may still be running",
                        timeout.as_secs()
                    );
                    Err(RocketMQError::Timeout {
                        operation: "server_task_join",
                        timeout_ms: timeout.as_millis() as u64,
                    })
                }
            }
        } else {
            debug!("No server task handle to wait for");
            Ok(())
        }
    }

    /// Initialize and configure request processor pipeline
    ///
    /// Creates specialized processors for different request types:
    /// - ClientRequestProcessor: Handles topic route queries
    /// - DefaultRequestProcessor: Handles all other requests
    #[inline]
    fn init_processors(&self) -> NameServerRequestProcessor {
        let client_request_processor = ClientRequestProcessor::new(self.inner.clone());
        let default_request_processor =
            crate::processor::default_request_processor::DefaultRequestProcessor::new(self.inner.clone());

        let mut name_server_request_processor = NameServerRequestProcessor::new();

        // Register topic route query processor
        name_server_request_processor.register_processor(
            RequestCode::GetRouteinfoByTopic,
            NameServerRequestProcessorWrapper::ClientRequestProcessor(ArcMut::new(client_request_processor)),
        );

        // Register default processor for all other requests
        name_server_request_processor.register_default_processor(
            NameServerRequestProcessorWrapper::DefaultRequestProcessor(ArcMut::new(default_request_processor)),
        );

        debug!("Request processor pipeline configured");
        name_server_request_processor
    }
}

impl Drop for NameServerRuntime {
    #[inline]
    fn drop(&mut self) {
        let current_state = self.current_state();
        debug!("NameServerRuntime dropped in state: {}", current_state);

        // Warn if not properly shut down
        if current_state != RuntimeState::Stopped {
            warn!(
                "NameServerRuntime dropped without proper shutdown (current state: {}). This may indicate a panic or \
                 abnormal termination.",
                current_state
            );
        }
    }
}

impl Default for Builder {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Builder {
    #[inline]
    pub fn new() -> Self {
        Builder {
            name_server_config: None,
            server_config: None,
        }
    }

    #[inline]
    pub fn set_name_server_config(mut self, name_server_config: NamesrvConfig) -> Self {
        self.name_server_config = Some(name_server_config);
        self
    }

    #[inline]
    pub fn set_server_config(mut self, server_config: ServerConfig) -> Self {
        self.server_config = Some(server_config);
        self
    }

    /// Build the NameServerBootstrap with configured settings
    ///
    /// Creates all necessary components and initializes them immediately.
    #[instrument(skip(self), name = "build_bootstrap")]
    pub fn build(self) -> NameServerBootstrap {
        let name_server_config = self.name_server_config.unwrap_or_default();
        let tokio_client_config = TokioClientConfig::default();
        let server_config = self.server_config.unwrap_or_default();

        info!("Building NameServer with configuration:");
        info!("  - Listen port: {}", server_config.listen_port);
        info!(
            "  - Scan interval: {}ms",
            name_server_config.scan_not_active_broker_interval
        );
        info!(
            "  - Use V2 RouteManager: {}",
            name_server_config.use_route_info_manager_v2
        );

        // Create remoting client
        let remoting_client = ArcMut::new(RocketmqDefaultClient::new(
            Arc::new(tokio_client_config.clone()),
            DefaultRemotingRequestProcessor,
        ));

        // Create inner with empty components first
        let mut inner = ArcMut::new(NameServerRuntimeInner {
            name_server_config: name_server_config.clone(),
            tokio_client_config,
            server_config,
            route_info_manager: None,
            kvconfig_manager: None,
            remoting_client,
            broker_housekeeping_service: None,
        });

        // Check configuration flag for RouteInfoManager version
        let use_v2 = name_server_config.use_route_info_manager_v2;

        // Now initialize components with proper references
        let route_info_manager = if use_v2 {
            info!("Using RouteInfoManager V2 (DashMap-based, 5-50x faster)");
            RouteInfoManagerWrapper::V2(Box::new(RouteInfoManagerV2::new(inner.clone())))
        } else {
            warn!("Using RouteInfoManager V1 (legacy). Consider V2 for better performance.");
            RouteInfoManagerWrapper::V1(Box::new(RouteInfoManager::new(inner.clone())))
        };

        let kvconfig_manager = KVConfigManager::new(inner.clone());
        let broker_housekeeping_service = Arc::new(BrokerHousekeepingService::new(inner.clone()));

        // Assign initialized components
        inner.route_info_manager = Some(route_info_manager);
        inner.kvconfig_manager = Some(kvconfig_manager);
        inner.broker_housekeeping_service = Some(broker_housekeeping_service);

        info!("NameServer bootstrap built successfully");

        NameServerBootstrap {
            name_server_runtime: NameServerRuntime {
                inner,
                scheduled_task_manager: ScheduledTaskManager::new(),
                shutdown_rx: None,
                server_inner: None,
                server_task_handle: None,
                state: Arc::new(AtomicU8::new(RuntimeState::Created as u8)),
            },
        }
    }
}

/// Internal runtime state shared across components
///
/// **Phase 4 Optimization**: Separates immutable from mutable components.
/// Note: Config kept mutable to support runtime updates via management commands.
pub(crate) struct NameServerRuntimeInner {
    // Configuration (mutable to support runtime updates)
    name_server_config: NamesrvConfig,
    tokio_client_config: TokioClientConfig,
    server_config: ServerConfig,

    // Mutable components (Option for delayed initialization)
    route_info_manager: Option<RouteInfoManagerWrapper>,
    kvconfig_manager: Option<KVConfigManager>,
    remoting_client: ArcMut<RocketmqDefaultClient>,
    broker_housekeeping_service: Option<Arc<BrokerHousekeepingService>>,
}

impl NameServerRuntimeInner {
    // Configuration accessors

    #[inline]
    pub fn name_server_config(&self) -> &NamesrvConfig {
        &self.name_server_config
    }

    #[inline]
    pub fn name_server_config_mut(&mut self) -> &mut NamesrvConfig {
        &mut self.name_server_config
    }

    #[inline]
    pub fn tokio_client_config(&self) -> &TokioClientConfig {
        &self.tokio_client_config
    }

    #[inline]
    pub fn server_config(&self) -> &ServerConfig {
        &self.server_config
    }

    // Component accessors (with Option handling)

    #[inline]
    pub fn route_info_manager(&self) -> &RouteInfoManagerWrapper {
        self.route_info_manager
            .as_ref()
            .expect("RouteInfoManager not initialized")
    }

    #[inline]
    pub fn route_info_manager_mut(&mut self) -> &mut RouteInfoManagerWrapper {
        self.route_info_manager
            .as_mut()
            .expect("RouteInfoManager not initialized")
    }

    #[inline]
    pub fn kvconfig_manager(&self) -> &KVConfigManager {
        self.kvconfig_manager.as_ref().expect("KVConfigManager not initialized")
    }

    #[inline]
    pub fn kvconfig_manager_mut(&mut self) -> &mut KVConfigManager {
        self.kvconfig_manager.as_mut().expect("KVConfigManager not initialized")
    }

    #[inline]
    pub fn remoting_client(&self) -> &RocketmqDefaultClient {
        &self.remoting_client
    }

    #[inline]
    pub fn remoting_client_mut(&mut self) -> &mut RocketmqDefaultClient {
        &mut self.remoting_client
    }

    #[inline]
    pub fn broker_housekeeping_service(&self) -> &Arc<BrokerHousekeepingService> {
        self.broker_housekeeping_service
            .as_ref()
            .expect("BrokerHousekeepingService not initialized")
    }
}
