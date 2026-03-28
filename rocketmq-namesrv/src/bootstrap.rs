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

use std::collections::HashMap;
use std::future::Future;
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
    pub async fn boot(self) -> RocketMQResult<()> {
        self.boot_with_shutdown(wait_for_signal()).await
    }

    /// Boot the NameServer and stop when the provided shutdown future resolves.
    ///
    /// This keeps the default `boot()` behavior unchanged while giving tests and
    /// embedding callers a deterministic shutdown path.
    #[instrument(skip(self, shutdown_signal), name = "nameserver_boot_with_shutdown")]
    pub async fn boot_with_shutdown<F>(mut self, shutdown_signal: F) -> RocketMQResult<()>
    where
        F: Future<Output = ()> + Send,
    {
        info!("Booting RocketMQ NameServer (Rust)...");

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        self.name_server_runtime.shutdown_rx = Some(shutdown_rx);
        self.name_server_runtime.initialize().await?;

        tokio::join!(
            self.name_server_runtime.start(),
            relay_shutdown_signal(shutdown_tx, shutdown_signal)
        );

        info!("NameServer shutdown completed");
        Ok(())
    }
}

#[inline]
async fn relay_shutdown_signal<F>(shutdown_tx: broadcast::Sender<()>, shutdown_signal: F)
where
    F: Future<Output = ()>,
{
    shutdown_signal.await;
    info!("Shutdown signal received, broadcasting to all components...");
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
        self.validate_runtime_config()?;

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

    fn validate_runtime_config(&self) -> RocketMQResult<()> {
        let namesrv_config = self.inner.name_server_config();

        if namesrv_config.cluster_test {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "clusterTest",
                value: namesrv_config.cluster_test.to_string(),
                reason: "cluster test mode is not implemented in rocketmq-namesrv yet".to_string(),
            });
        }

        if namesrv_config.enable_controller_in_namesrv {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "enableControllerInNamesrv",
                value: namesrv_config.enable_controller_in_namesrv.to_string(),
                reason: "controller-in-namesrv startup is not implemented in rocketmq-namesrv yet".to_string(),
            });
        }

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
/// Separates immutable from mutable components.
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
    pub fn tokio_client_config_mut(&mut self) -> &mut TokioClientConfig {
        &mut self.tokio_client_config
    }

    #[inline]
    pub fn server_config(&self) -> &ServerConfig {
        &self.server_config
    }

    #[inline]
    pub fn server_config_mut(&mut self) -> &mut ServerConfig {
        &mut self.server_config
    }

    pub fn get_all_configs_format_string(&self) -> Result<String, String> {
        let mut entries = Vec::with_capacity(41);

        push_config_entry(&mut entries, "rocketmqHome", &self.name_server_config.rocketmq_home);
        push_config_entry(&mut entries, "kvConfigPath", &self.name_server_config.kv_config_path);
        push_config_entry(
            &mut entries,
            "configStorePath",
            &self.name_server_config.config_store_path,
        );
        push_config_entry(
            &mut entries,
            "productEnvName",
            &self.name_server_config.product_env_name,
        );
        push_config_entry(&mut entries, "clusterTest", self.name_server_config.cluster_test);
        push_config_entry(
            &mut entries,
            "orderMessageEnable",
            self.name_server_config.order_message_enable,
        );
        push_config_entry(
            &mut entries,
            "returnOrderTopicConfigToBroker",
            self.name_server_config.return_order_topic_config_to_broker,
        );
        push_config_entry(
            &mut entries,
            "clientRequestThreadPoolNums",
            self.name_server_config.client_request_thread_pool_nums,
        );
        push_config_entry(
            &mut entries,
            "defaultThreadPoolNums",
            self.name_server_config.default_thread_pool_nums,
        );
        push_config_entry(
            &mut entries,
            "clientRequestThreadPoolQueueCapacity",
            self.name_server_config.client_request_thread_pool_queue_capacity,
        );
        push_config_entry(
            &mut entries,
            "defaultThreadPoolQueueCapacity",
            self.name_server_config.default_thread_pool_queue_capacity,
        );
        push_config_entry(
            &mut entries,
            "scanNotActiveBrokerInterval",
            self.name_server_config.scan_not_active_broker_interval,
        );
        push_config_entry(
            &mut entries,
            "unRegisterBrokerQueueCapacity",
            self.name_server_config.unregister_broker_queue_capacity,
        );
        push_config_entry(
            &mut entries,
            "supportActingMaster",
            self.name_server_config.support_acting_master,
        );
        push_config_entry(
            &mut entries,
            "enableAllTopicList",
            self.name_server_config.enable_all_topic_list,
        );
        push_config_entry(
            &mut entries,
            "enableTopicList",
            self.name_server_config.enable_topic_list,
        );
        push_config_entry(
            &mut entries,
            "notifyMinBrokerIdChanged",
            self.name_server_config.notify_min_broker_id_changed,
        );
        push_config_entry(
            &mut entries,
            "enableControllerInNamesrv",
            self.name_server_config.enable_controller_in_namesrv,
        );
        push_config_entry(
            &mut entries,
            "needWaitForService",
            self.name_server_config.need_wait_for_service,
        );
        push_config_entry(
            &mut entries,
            "waitSecondsForService",
            self.name_server_config.wait_seconds_for_service,
        );
        push_config_entry(
            &mut entries,
            "deleteTopicWithBrokerRegistration",
            self.name_server_config.delete_topic_with_broker_registration,
        );
        push_config_entry(
            &mut entries,
            "configBlackList",
            &self.name_server_config.config_black_list,
        );
        push_config_entry(
            &mut entries,
            "useRouteInfoManagerV2",
            self.name_server_config.use_route_info_manager_v2,
        );

        push_config_entry(&mut entries, "listenPort", self.server_config.listen_port);
        push_config_entry(&mut entries, "bindAddress", &self.server_config.bind_address);

        push_config_entry(
            &mut entries,
            "clientWorkerThreads",
            self.tokio_client_config.client_worker_threads,
        );
        push_config_entry(
            &mut entries,
            "clientCallbackExecutorThreads",
            self.tokio_client_config.client_callback_executor_threads,
        );
        push_config_entry(
            &mut entries,
            "clientOnewaySemaphoreValue",
            self.tokio_client_config.client_oneway_semaphore_value,
        );
        push_config_entry(
            &mut entries,
            "clientAsyncSemaphoreValue",
            self.tokio_client_config.client_async_semaphore_value,
        );
        push_config_entry(
            &mut entries,
            "connectTimeoutMillis",
            self.tokio_client_config.connect_timeout_millis,
        );
        push_config_entry(
            &mut entries,
            "channelNotActiveInterval",
            self.tokio_client_config.channel_not_active_interval,
        );
        push_config_entry(
            &mut entries,
            "clientChannelMaxIdleTimeSeconds",
            self.tokio_client_config.client_channel_max_idle_time_seconds,
        );
        push_config_entry(
            &mut entries,
            "clientSocketSndBufSize",
            self.tokio_client_config.client_socket_snd_buf_size,
        );
        push_config_entry(
            &mut entries,
            "clientSocketRcvBufSize",
            self.tokio_client_config.client_socket_rcv_buf_size,
        );
        push_config_entry(
            &mut entries,
            "clientPooledByteBufAllocatorEnable",
            self.tokio_client_config.client_pooled_byte_buf_allocator_enable,
        );
        push_config_entry(
            &mut entries,
            "clientCloseSocketIfTimeout",
            self.tokio_client_config.client_close_socket_if_timeout,
        );
        push_config_entry(
            &mut entries,
            "socksProxyConfig",
            &self.tokio_client_config.socks_proxy_config,
        );
        push_config_entry(
            &mut entries,
            "writeBufferHighWaterMark",
            self.tokio_client_config.write_buffer_high_water_mark,
        );
        push_config_entry(
            &mut entries,
            "writeBufferLowWaterMark",
            self.tokio_client_config.write_buffer_low_water_mark,
        );
        push_config_entry(
            &mut entries,
            "disableCallbackExecutor",
            self.tokio_client_config.disable_callback_executor,
        );
        push_config_entry(
            &mut entries,
            "disableNettyWorkerGroup",
            self.tokio_client_config.disable_netty_worker_group,
        );
        push_config_entry(
            &mut entries,
            "maxReconnectIntervalTimeSeconds",
            self.tokio_client_config.max_reconnect_interval_time_seconds,
        );
        push_config_entry(
            &mut entries,
            "enableReconnectForGoAway",
            self.tokio_client_config.enable_reconnect_for_go_away,
        );
        push_config_entry(
            &mut entries,
            "enableTransparentRetry",
            self.tokio_client_config.enable_transparent_retry,
        );

        entries.sort_by_key(|(key, _)| *key);

        let estimated_len = entries
            .iter()
            .map(|(key, value)| key.len() + value.len() + 2)
            .sum::<usize>()
            .saturating_sub(1);
        let mut config = String::with_capacity(estimated_len);

        for (index, (key, value)) in entries.into_iter().enumerate() {
            if index > 0 {
                config.push('\n');
            }
            config.push_str(key);
            config.push('=');
            config.push_str(&value);
        }

        Ok(config)
    }

    pub fn update_runtime_config(&mut self, updates: HashMap<CheetahString, CheetahString>) -> Result<(), String> {
        let mut namesrv_updates = HashMap::new();

        for (key, value) in updates {
            match key.as_str() {
                "rocketmqHome"
                | "kvConfigPath"
                | "configStorePath"
                | "productEnvName"
                | "clusterTest"
                | "orderMessageEnable"
                | "returnOrderTopicConfigToBroker"
                | "clientRequestThreadPoolNums"
                | "defaultThreadPoolNums"
                | "clientRequestThreadPoolQueueCapacity"
                | "defaultThreadPoolQueueCapacity"
                | "scanNotActiveBrokerInterval"
                | "unRegisterBrokerQueueCapacity"
                | "supportActingMaster"
                | "enableAllTopicList"
                | "enableTopicList"
                | "notifyMinBrokerIdChanged"
                | "enableControllerInNamesrv"
                | "needWaitForService"
                | "waitSecondsForService"
                | "deleteTopicWithBrokerRegistration"
                | "configBlackList"
                | "useRouteInfoManagerV2" => {
                    namesrv_updates.insert(key, value);
                }
                "listenPort" => {
                    self.server_config.listen_port = parse_config_value(&key, &value)?;
                }
                "bindAddress" => {
                    self.server_config.bind_address = value.to_string();
                }
                "clientWorkerThreads" => {
                    self.tokio_client_config.client_worker_threads = parse_config_value(&key, &value)?;
                }
                "clientCallbackExecutorThreads" => {
                    self.tokio_client_config.client_callback_executor_threads = parse_config_value(&key, &value)?;
                }
                "clientOnewaySemaphoreValue" => {
                    self.tokio_client_config.client_oneway_semaphore_value = parse_config_value(&key, &value)?;
                }
                "clientAsyncSemaphoreValue" => {
                    self.tokio_client_config.client_async_semaphore_value = parse_config_value(&key, &value)?;
                }
                "connectTimeoutMillis" => {
                    self.tokio_client_config.connect_timeout_millis = parse_config_value(&key, &value)?;
                }
                "channelNotActiveInterval" => {
                    self.tokio_client_config.channel_not_active_interval = parse_config_value(&key, &value)?;
                }
                "clientChannelMaxIdleTimeSeconds" => {
                    self.tokio_client_config.client_channel_max_idle_time_seconds = parse_config_value(&key, &value)?;
                }
                "clientSocketSndBufSize" => {
                    self.tokio_client_config.client_socket_snd_buf_size = parse_config_value(&key, &value)?;
                }
                "clientSocketRcvBufSize" => {
                    self.tokio_client_config.client_socket_rcv_buf_size = parse_config_value(&key, &value)?;
                }
                "clientPooledByteBufAllocatorEnable" => {
                    self.tokio_client_config.client_pooled_byte_buf_allocator_enable =
                        parse_config_value(&key, &value)?;
                }
                "clientCloseSocketIfTimeout" => {
                    self.tokio_client_config.client_close_socket_if_timeout = parse_config_value(&key, &value)?;
                }
                "socksProxyConfig" => {
                    self.tokio_client_config.socks_proxy_config = value.to_string();
                }
                "writeBufferHighWaterMark" => {
                    self.tokio_client_config.write_buffer_high_water_mark = parse_config_value(&key, &value)?;
                }
                "writeBufferLowWaterMark" => {
                    self.tokio_client_config.write_buffer_low_water_mark = parse_config_value(&key, &value)?;
                }
                "disableCallbackExecutor" => {
                    self.tokio_client_config.disable_callback_executor = parse_config_value(&key, &value)?;
                }
                "disableNettyWorkerGroup" => {
                    self.tokio_client_config.disable_netty_worker_group = parse_config_value(&key, &value)?;
                }
                "maxReconnectIntervalTimeSeconds" => {
                    self.tokio_client_config.max_reconnect_interval_time_seconds = parse_config_value(&key, &value)?;
                }
                "enableReconnectForGoAway" => {
                    self.tokio_client_config.enable_reconnect_for_go_away = parse_config_value(&key, &value)?;
                }
                "enableTransparentRetry" => {
                    self.tokio_client_config.enable_transparent_retry = parse_config_value(&key, &value)?;
                }
                _ => {}
            }
        }

        if !namesrv_updates.is_empty() {
            self.name_server_config.update(namesrv_updates)?;
        }

        Ok(())
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

fn push_config_entry(entries: &mut Vec<(&'static str, String)>, key: &'static str, value: impl ToString) {
    entries.push((key, value.to_string()));
}

fn parse_config_value<T>(key: &str, value: &CheetahString) -> Result<T, String>
where
    T: std::str::FromStr,
{
    value
        .as_str()
        .parse()
        .map_err(|_| format!("Invalid configuration value for key '{key}'"))
}

#[cfg(test)]
mod tests {
    use std::str;
    use std::time::Duration;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_common::common::constant::PermName;
    use rocketmq_common::common::mix_all::string_to_properties;
    use rocketmq_common::common::mix_all::MASTER_ID;
    use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
    use rocketmq_common::common::server::config::ServerConfig;
    use rocketmq_common::common::TopicSysFlag;
    use rocketmq_remoting::code::request_code::RequestCode;
    use rocketmq_remoting::code::response_code::ResponseCode;
    use rocketmq_remoting::connection::ConnectionState;
    use rocketmq_remoting::local::LocalRequestHarness;
    use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
    use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
    use rocketmq_remoting::runtime::processor::RequestProcessor;
    use tokio::time::sleep;

    use super::*;
    use crate::processor::default_request_processor::DefaultRequestProcessor;

    fn build_bootstrap_with_v2_config(mut namesrv_config: NamesrvConfig) -> NameServerBootstrap {
        namesrv_config.use_route_info_manager_v2 = true;
        let bootstrap = Builder::new().set_name_server_config(namesrv_config).build();
        assert!(matches!(
            bootstrap.name_server_runtime.inner.route_info_manager(),
            RouteInfoManagerWrapper::V2(_)
        ));
        bootstrap
    }

    fn build_bootstrap_with_default_v2() -> NameServerBootstrap {
        build_bootstrap_with_v2_config(NamesrvConfig::default())
    }

    async fn process_with_default_processor(
        bootstrap: &NameServerBootstrap,
        harness: &LocalRequestHarness,
        request: &mut RemotingCommand,
    ) -> RemotingCommand {
        let mut processor = DefaultRequestProcessor::new(bootstrap.name_server_runtime.inner.clone());
        processor
            .process_request(harness.channel(), harness.context(), request)
            .await
            .expect("request processing should succeed")
            .expect("processor should always return a response")
    }

    fn topic_config_wrapper(entries: &[(&str, u32, u32)]) -> TopicConfigAndMappingSerializeWrapper {
        let mut wrapper = TopicConfigAndMappingSerializeWrapper::default();
        for (topic_name, topic_sys_flag, perm) in entries {
            wrapper.topic_config_serialize_wrapper.topic_config_table.insert(
                CheetahString::from(*topic_name),
                TopicConfig::with_sys_flag(CheetahString::from(*topic_name), 8, 8, *perm, *topic_sys_flag),
            );
        }
        wrapper
    }

    fn start_unregister_service(bootstrap: &NameServerBootstrap) {
        bootstrap.name_server_runtime.inner.route_info_manager().start();
    }

    fn shutdown_unregister_service(bootstrap: &NameServerBootstrap) {
        bootstrap.name_server_runtime.inner.route_info_manager().shutdown();
    }

    async fn wait_until<F>(description: &str, mut condition: F)
    where
        F: FnMut() -> bool,
    {
        for _ in 0..100 {
            if condition() {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for {description}");
    }

    #[allow(clippy::too_many_arguments)]
    async fn register_test_broker_with_harness(
        bootstrap: &NameServerBootstrap,
        cluster_name: &CheetahString,
        broker_name: &CheetahString,
        broker_addr: &CheetahString,
        broker_id: u64,
        zone_name: &CheetahString,
        enable_acting_master: bool,
        topic_config_wrapper: TopicConfigAndMappingSerializeWrapper,
        filter_server_list: Vec<CheetahString>,
        timeout_millis: Option<i64>,
        harness: &LocalRequestHarness,
    ) {
        let result = bootstrap
            .name_server_runtime
            .inner
            .route_info_manager()
            .register_broker(
                cluster_name.clone(),
                broker_addr.clone(),
                broker_name.clone(),
                broker_id,
                CheetahString::from_static_str("10.0.0.1:10912"),
                Some(zone_name.clone()),
                timeout_millis,
                Some(enable_acting_master),
                topic_config_wrapper,
                filter_server_list,
                harness.channel(),
            );

        assert!(result.is_some());
    }

    async fn register_test_broker(
        bootstrap: &NameServerBootstrap,
        cluster_name: &CheetahString,
        broker_name: &CheetahString,
        broker_addr: &CheetahString,
        broker_id: u64,
        zone_name: &CheetahString,
        enable_acting_master: bool,
        topic_config_wrapper: TopicConfigAndMappingSerializeWrapper,
    ) {
        let harness = LocalRequestHarness::new().await.unwrap();
        register_test_broker_with_harness(
            bootstrap,
            cluster_name,
            broker_name,
            broker_addr,
            broker_id,
            zone_name,
            enable_acting_master,
            topic_config_wrapper,
            vec![],
            Some(30_000),
            &harness,
        )
        .await;
    }

    #[tokio::test]
    async fn default_v2_system_topic_list_includes_cluster_and_broker_names() {
        let bootstrap = build_bootstrap_with_default_v2();
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let broker_addr = CheetahString::from_static_str("10.0.0.1:10911");
        let zone_name = CheetahString::from_static_str("zone-a");

        register_test_broker(
            &bootstrap,
            &cluster_name,
            &broker_name,
            &broker_addr,
            MASTER_ID,
            &zone_name,
            true,
            TopicConfigAndMappingSerializeWrapper::default(),
        )
        .await;

        let topic_list = bootstrap
            .name_server_runtime
            .inner
            .route_info_manager()
            .get_system_topic_list();

        assert!(topic_list.topic_list.contains(&cluster_name));
        assert!(topic_list.topic_list.contains(&broker_name));
        assert_eq!(topic_list.broker_addr.as_ref(), Some(&broker_addr));
    }

    #[tokio::test]
    async fn default_v2_cluster_info_preserves_zone_and_acting_master_metadata() {
        let bootstrap = build_bootstrap_with_default_v2();
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let broker_addr = CheetahString::from_static_str("10.0.0.1:10911");
        let zone_name = CheetahString::from_static_str("zone-a");

        register_test_broker(
            &bootstrap,
            &cluster_name,
            &broker_name,
            &broker_addr,
            MASTER_ID,
            &zone_name,
            true,
            TopicConfigAndMappingSerializeWrapper::default(),
        )
        .await;

        let cluster_info = bootstrap
            .name_server_runtime
            .inner
            .route_info_manager()
            .get_all_cluster_info();
        let broker_data = cluster_info
            .broker_addr_table
            .as_ref()
            .and_then(|brokers| brokers.get(&broker_name))
            .expect("registered broker must exist in cluster info");
        let broker_names = cluster_info
            .cluster_addr_table
            .as_ref()
            .and_then(|clusters| clusters.get(&cluster_name))
            .expect("registered cluster must exist in cluster info");

        assert!(broker_names.contains(&broker_name));
        assert_eq!(broker_data.zone_name(), Some(&zone_name));
        assert!(broker_data.enable_acting_master());
        assert_eq!(broker_data.broker_addrs().get(&MASTER_ID), Some(&broker_addr));
    }

    #[tokio::test]
    async fn default_v2_topics_by_cluster_matches_java_duplicate_semantics() {
        let bootstrap = build_bootstrap_with_default_v2();
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let zone_name = CheetahString::from_static_str("zone-a");
        let shared_topic = "shared-topic";

        register_test_broker(
            &bootstrap,
            &cluster_name,
            &CheetahString::from_static_str("broker-a"),
            &CheetahString::from_static_str("10.0.0.1:10911"),
            MASTER_ID,
            &zone_name,
            false,
            topic_config_wrapper(&[(shared_topic, 0, PermName::PERM_READ | PermName::PERM_WRITE)]),
        )
        .await;
        register_test_broker(
            &bootstrap,
            &cluster_name,
            &CheetahString::from_static_str("broker-b"),
            &CheetahString::from_static_str("10.0.0.2:10911"),
            MASTER_ID,
            &zone_name,
            false,
            topic_config_wrapper(&[(shared_topic, 0, PermName::PERM_READ | PermName::PERM_WRITE)]),
        )
        .await;

        let topic_list = bootstrap
            .name_server_runtime
            .inner
            .route_info_manager()
            .get_topics_by_cluster(&cluster_name);
        let missing_cluster_topics = bootstrap
            .name_server_runtime
            .inner
            .route_info_manager()
            .get_topics_by_cluster(&CheetahString::from_static_str("missing-cluster"));

        assert_eq!(
            topic_list
                .topic_list
                .iter()
                .filter(|topic| topic.as_str() == shared_topic)
                .count(),
            2
        );
        assert!(missing_cluster_topics.topic_list.is_empty());
    }

    #[tokio::test]
    async fn default_v2_unit_topic_queries_match_java_flag_semantics() {
        let bootstrap = build_bootstrap_with_default_v2();
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let broker_addr = CheetahString::from_static_str("10.0.0.1:10911");
        let zone_name = CheetahString::from_static_str("zone-a");

        register_test_broker(
            &bootstrap,
            &cluster_name,
            &broker_name,
            &broker_addr,
            MASTER_ID,
            &zone_name,
            false,
            topic_config_wrapper(&[
                (
                    "unit-only",
                    TopicSysFlag::build_sys_flag(true, false),
                    PermName::PERM_READ | PermName::PERM_WRITE,
                ),
                (
                    "unit-sub-only",
                    TopicSysFlag::build_sys_flag(false, true),
                    PermName::PERM_READ | PermName::PERM_WRITE,
                ),
                (
                    "unit-and-sub",
                    TopicSysFlag::build_sys_flag(true, true),
                    PermName::PERM_READ | PermName::PERM_WRITE,
                ),
            ]),
        )
        .await;

        let unit_topics = bootstrap
            .name_server_runtime
            .inner
            .route_info_manager()
            .get_unit_topics();
        let unit_sub_topics = bootstrap
            .name_server_runtime
            .inner
            .route_info_manager()
            .get_has_unit_sub_topic_list();
        let unit_sub_ununit_topics = bootstrap
            .name_server_runtime
            .inner
            .route_info_manager()
            .get_has_unit_sub_un_unit_topic_list();

        assert!(unit_topics
            .topic_list
            .contains(&CheetahString::from_static_str("unit-only")));
        assert!(unit_topics
            .topic_list
            .contains(&CheetahString::from_static_str("unit-and-sub")));
        assert!(!unit_topics
            .topic_list
            .contains(&CheetahString::from_static_str("unit-sub-only")));

        assert!(unit_sub_topics
            .topic_list
            .contains(&CheetahString::from_static_str("unit-sub-only")));
        assert!(unit_sub_topics
            .topic_list
            .contains(&CheetahString::from_static_str("unit-and-sub")));

        assert!(unit_sub_ununit_topics
            .topic_list
            .contains(&CheetahString::from_static_str("unit-sub-only")));
        assert!(!unit_sub_ununit_topics
            .topic_list
            .contains(&CheetahString::from_static_str("unit-and-sub")));
    }

    #[tokio::test]
    async fn default_v2_pickup_topic_route_data_promotes_read_only_prime_slave_to_acting_master() {
        let namesrv_config = NamesrvConfig {
            support_acting_master: true,
            ..NamesrvConfig::default()
        };
        let bootstrap = build_bootstrap_with_v2_config(namesrv_config);
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let broker_addr = CheetahString::from_static_str("10.0.0.1:10911");
        let zone_name = CheetahString::from_static_str("zone-a");
        let topic_name = CheetahString::from_static_str("acting-master-topic");

        register_test_broker(
            &bootstrap,
            &cluster_name,
            &broker_name,
            &broker_addr,
            1,
            &zone_name,
            true,
            topic_config_wrapper(&[("acting-master-topic", 0, PermName::PERM_READ | PermName::PERM_WRITE)]),
        )
        .await;

        let topic_route_data = bootstrap
            .name_server_runtime
            .inner
            .route_info_manager()
            .pickup_topic_route_data(&topic_name)
            .expect("topic route data should exist");
        let broker_data = topic_route_data
            .broker_datas
            .iter()
            .find(|broker_data| broker_data.broker_name() == &broker_name)
            .expect("registered broker should be present in route data");
        let queue_data = topic_route_data
            .queue_datas
            .iter()
            .find(|queue_data| queue_data.broker_name() == &broker_name)
            .expect("queue data should exist");

        assert!(!PermName::is_writeable(queue_data.perm()));
        assert_eq!(broker_data.broker_addrs().get(&MASTER_ID), Some(&broker_addr));
        assert!(!broker_data.broker_addrs().contains_key(&1));
    }

    #[tokio::test]
    async fn default_v2_scan_not_active_broker_cleans_route_views_via_batch_unregister() {
        let mut bootstrap = build_bootstrap_with_default_v2();
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let broker_addr = CheetahString::from_static_str("10.0.0.1:10911");
        let zone_name = CheetahString::from_static_str("zone-a");
        let topic_name = CheetahString::from_static_str("scan-cleanup-topic");
        let harness = LocalRequestHarness::new().await.unwrap();

        start_unregister_service(&bootstrap);
        register_test_broker_with_harness(
            &bootstrap,
            &cluster_name,
            &broker_name,
            &broker_addr,
            MASTER_ID,
            &zone_name,
            false,
            topic_config_wrapper(&[("scan-cleanup-topic", 0, PermName::PERM_READ | PermName::PERM_WRITE)]),
            vec![CheetahString::from_static_str("fs-a")],
            Some(10),
            &harness,
        )
        .await;

        sleep(Duration::from_millis(30)).await;

        let expired_count = bootstrap
            .name_server_runtime
            .inner
            .route_info_manager_mut()
            .scan_not_active_broker();
        assert_eq!(expired_count, 1);

        wait_until("expired broker cleanup", || {
            let route_manager = bootstrap.name_server_runtime.inner.route_info_manager();
            let cluster_info = route_manager.get_all_cluster_info();

            route_manager.pickup_topic_route_data(&topic_name).is_none()
                && route_manager
                    .query_broker_topic_config(cluster_name.clone(), broker_addr.clone())
                    .is_none()
                && cluster_info
                    .cluster_addr_table
                    .as_ref()
                    .is_none_or(|clusters| !clusters.contains_key(&cluster_name))
                && cluster_info
                    .broker_addr_table
                    .as_ref()
                    .is_none_or(|brokers| !brokers.contains_key(&broker_name))
                && route_manager.get_topics_by_cluster(&cluster_name).topic_list.is_empty()
        })
        .await;

        shutdown_unregister_service(&bootstrap);
    }

    #[tokio::test]
    async fn default_v2_scan_not_active_broker_closes_expired_connection_before_batch_unregister() {
        let mut bootstrap = build_bootstrap_with_default_v2();
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let broker_addr = CheetahString::from_static_str("10.0.0.1:10911");
        let zone_name = CheetahString::from_static_str("zone-a");
        let harness = LocalRequestHarness::new().await.unwrap();

        start_unregister_service(&bootstrap);
        register_test_broker_with_harness(
            &bootstrap,
            &cluster_name,
            &broker_name,
            &broker_addr,
            MASTER_ID,
            &zone_name,
            false,
            topic_config_wrapper(&[("scan-close-topic", 0, PermName::PERM_READ | PermName::PERM_WRITE)]),
            vec![],
            Some(10),
            &harness,
        )
        .await;

        assert_eq!(harness.channel().connection_ref().state(), ConnectionState::Healthy);

        sleep(Duration::from_millis(30)).await;

        let expired_count = bootstrap
            .name_server_runtime
            .inner
            .route_info_manager_mut()
            .scan_not_active_broker();
        assert_eq!(expired_count, 1);

        wait_until("expired broker connection close", || {
            harness.channel().connection_ref().state() == ConnectionState::Closed
        })
        .await;

        shutdown_unregister_service(&bootstrap);
    }

    #[tokio::test]
    async fn default_v2_connection_disconnected_by_socket_addr_matches_channel_destroy_cleanup() {
        let mut bootstrap = build_bootstrap_with_default_v2();
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let broker_addr = CheetahString::from_static_str("10.0.0.1:10911");
        let zone_name = CheetahString::from_static_str("zone-a");
        let topic_name = CheetahString::from_static_str("socket-disconnect-topic");
        let harness = LocalRequestHarness::new().await.unwrap();

        start_unregister_service(&bootstrap);
        register_test_broker_with_harness(
            &bootstrap,
            &cluster_name,
            &broker_name,
            &broker_addr,
            MASTER_ID,
            &zone_name,
            false,
            topic_config_wrapper(&[("socket-disconnect-topic", 0, PermName::PERM_READ | PermName::PERM_WRITE)]),
            vec![CheetahString::from_static_str("fs-a")],
            Some(30_000),
            &harness,
        )
        .await;

        bootstrap
            .name_server_runtime
            .inner
            .route_info_manager_mut()
            .connection_disconnected(harness.remote_address());

        wait_until("socket disconnect cleanup", || {
            let route_manager = bootstrap.name_server_runtime.inner.route_info_manager();
            let cluster_info = route_manager.get_all_cluster_info();

            harness.channel().connection_ref().state() == ConnectionState::Closed
                && route_manager.pickup_topic_route_data(&topic_name).is_none()
                && route_manager
                    .query_broker_topic_config(cluster_name.clone(), broker_addr.clone())
                    .is_none()
                && cluster_info
                    .cluster_addr_table
                    .as_ref()
                    .is_none_or(|clusters| !clusters.contains_key(&cluster_name))
                && cluster_info
                    .broker_addr_table
                    .as_ref()
                    .is_none_or(|brokers| !brokers.contains_key(&broker_name))
        })
        .await;

        shutdown_unregister_service(&bootstrap);
    }

    #[tokio::test]
    async fn default_v2_duplicate_channel_destroy_submission_is_idempotent_for_acting_master_cleanup() {
        let namesrv_config = NamesrvConfig {
            support_acting_master: true,
            ..NamesrvConfig::default()
        };
        let bootstrap = build_bootstrap_with_v2_config(namesrv_config);
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let master_addr = CheetahString::from_static_str("10.0.0.1:10911");
        let slave_addr = CheetahString::from_static_str("10.0.0.2:10911");
        let zone_name = CheetahString::from_static_str("zone-a");
        let topic_name = CheetahString::from_static_str("duplicate-unregister-topic");
        let master_harness = LocalRequestHarness::new().await.unwrap();
        let slave_harness = LocalRequestHarness::new().await.unwrap();

        register_test_broker_with_harness(
            &bootstrap,
            &cluster_name,
            &broker_name,
            &master_addr,
            MASTER_ID,
            &zone_name,
            true,
            topic_config_wrapper(&[(
                "duplicate-unregister-topic",
                0,
                PermName::PERM_READ | PermName::PERM_WRITE,
            )]),
            vec![],
            Some(30_000),
            &master_harness,
        )
        .await;
        register_test_broker_with_harness(
            &bootstrap,
            &cluster_name,
            &broker_name,
            &slave_addr,
            1,
            &zone_name,
            true,
            TopicConfigAndMappingSerializeWrapper::default(),
            vec![],
            Some(30_000),
            &slave_harness,
        )
        .await;

        let master_channel = master_harness.channel();
        bootstrap
            .name_server_runtime
            .inner
            .route_info_manager()
            .on_channel_destroy(&master_channel);
        bootstrap
            .name_server_runtime
            .inner
            .route_info_manager()
            .on_channel_destroy(&master_channel);

        start_unregister_service(&bootstrap);

        wait_until("duplicate unregister cleanup", || {
            let route_manager = bootstrap.name_server_runtime.inner.route_info_manager();
            let cluster_info = route_manager.get_all_cluster_info();
            let Some(route_data) = route_manager.pickup_topic_route_data(&topic_name) else {
                return false;
            };
            let Some(route_broker_data) = route_data
                .broker_datas
                .iter()
                .find(|broker_data| broker_data.broker_name() == &broker_name)
            else {
                return false;
            };
            let Some(route_queue_data) = route_data
                .queue_datas
                .iter()
                .find(|queue_data| queue_data.broker_name() == &broker_name)
            else {
                return false;
            };
            let Some(cluster_broker_data) = cluster_info
                .broker_addr_table
                .as_ref()
                .and_then(|brokers| brokers.get(&broker_name))
            else {
                return false;
            };

            route_manager
                .query_broker_topic_config(cluster_name.clone(), master_addr.clone())
                .is_none()
                && route_manager
                    .query_broker_topic_config(cluster_name.clone(), slave_addr.clone())
                    .is_some()
                && !PermName::is_writeable(route_queue_data.perm())
                && route_broker_data.broker_addrs().get(&MASTER_ID) == Some(&slave_addr)
                && !route_broker_data.broker_addrs().contains_key(&1)
                && cluster_broker_data.broker_addrs().get(&1) == Some(&slave_addr)
                && !cluster_broker_data.broker_addrs().contains_key(&MASTER_ID)
        })
        .await;

        shutdown_unregister_service(&bootstrap);
    }

    #[tokio::test]
    async fn default_v2_on_channel_destroy_cleans_removed_broker_and_preserves_survivor() {
        let bootstrap = build_bootstrap_with_default_v2();
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let removed_broker_name = CheetahString::from_static_str("broker-a");
        let surviving_broker_name = CheetahString::from_static_str("broker-b");
        let removed_broker_addr = CheetahString::from_static_str("10.0.0.1:10911");
        let surviving_broker_addr = CheetahString::from_static_str("10.0.0.2:10911");
        let zone_name = CheetahString::from_static_str("zone-a");
        let topic_name = CheetahString::from_static_str("channel-destroy-topic");
        let removed_harness = LocalRequestHarness::new().await.unwrap();
        let surviving_harness = LocalRequestHarness::new().await.unwrap();

        start_unregister_service(&bootstrap);
        register_test_broker_with_harness(
            &bootstrap,
            &cluster_name,
            &removed_broker_name,
            &removed_broker_addr,
            MASTER_ID,
            &zone_name,
            false,
            topic_config_wrapper(&[("channel-destroy-topic", 0, PermName::PERM_READ | PermName::PERM_WRITE)]),
            vec![CheetahString::from_static_str("fs-a")],
            Some(30_000),
            &removed_harness,
        )
        .await;
        register_test_broker_with_harness(
            &bootstrap,
            &cluster_name,
            &surviving_broker_name,
            &surviving_broker_addr,
            MASTER_ID,
            &zone_name,
            false,
            topic_config_wrapper(&[("channel-destroy-topic", 0, PermName::PERM_READ | PermName::PERM_WRITE)]),
            vec![CheetahString::from_static_str("fs-b")],
            Some(30_000),
            &surviving_harness,
        )
        .await;

        let removed_channel = removed_harness.channel();
        bootstrap
            .name_server_runtime
            .inner
            .route_info_manager()
            .on_channel_destroy(&removed_channel);

        wait_until("channel destroy cleanup", || {
            let route_manager = bootstrap.name_server_runtime.inner.route_info_manager();
            let cluster_info = route_manager.get_all_cluster_info();
            let Some(route_data) = route_manager.pickup_topic_route_data(&topic_name) else {
                return false;
            };

            route_manager
                .query_broker_topic_config(cluster_name.clone(), removed_broker_addr.clone())
                .is_none()
                && route_manager
                    .query_broker_topic_config(cluster_name.clone(), surviving_broker_addr.clone())
                    .is_some()
                && route_data
                    .broker_datas
                    .iter()
                    .all(|broker_data| broker_data.broker_name() != &removed_broker_name)
                && route_data
                    .broker_datas
                    .iter()
                    .any(|broker_data| broker_data.broker_name() == &surviving_broker_name)
                && !route_data.filter_server_table.contains_key(&removed_broker_addr)
                && route_data
                    .filter_server_table
                    .get(&surviving_broker_addr)
                    .is_some_and(|servers| servers.len() == 1 && servers[0] == CheetahString::from_static_str("fs-b"))
                && cluster_info
                    .cluster_addr_table
                    .as_ref()
                    .and_then(|clusters| clusters.get(&cluster_name))
                    .is_some_and(|brokers| brokers.len() == 1 && brokers.contains(&surviving_broker_name))
                && cluster_info.broker_addr_table.as_ref().is_some_and(|brokers| {
                    !brokers.contains_key(&removed_broker_name) && brokers.contains_key(&surviving_broker_name)
                })
        })
        .await;

        shutdown_unregister_service(&bootstrap);
    }

    #[tokio::test]
    async fn default_v2_on_channel_destroy_reduces_to_read_only_acting_master() {
        let namesrv_config = NamesrvConfig {
            support_acting_master: true,
            ..NamesrvConfig::default()
        };
        let bootstrap = build_bootstrap_with_v2_config(namesrv_config);
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let master_addr = CheetahString::from_static_str("10.0.0.1:10911");
        let slave_addr = CheetahString::from_static_str("10.0.0.2:10911");
        let zone_name = CheetahString::from_static_str("zone-a");
        let topic_name = CheetahString::from_static_str("acting-master-cleanup-topic");
        let master_harness = LocalRequestHarness::new().await.unwrap();
        let slave_harness = LocalRequestHarness::new().await.unwrap();

        start_unregister_service(&bootstrap);
        register_test_broker_with_harness(
            &bootstrap,
            &cluster_name,
            &broker_name,
            &master_addr,
            MASTER_ID,
            &zone_name,
            true,
            topic_config_wrapper(&[(
                "acting-master-cleanup-topic",
                0,
                PermName::PERM_READ | PermName::PERM_WRITE,
            )]),
            vec![],
            Some(30_000),
            &master_harness,
        )
        .await;
        register_test_broker_with_harness(
            &bootstrap,
            &cluster_name,
            &broker_name,
            &slave_addr,
            1,
            &zone_name,
            true,
            TopicConfigAndMappingSerializeWrapper::default(),
            vec![],
            Some(30_000),
            &slave_harness,
        )
        .await;

        let master_channel = master_harness.channel();
        bootstrap
            .name_server_runtime
            .inner
            .route_info_manager()
            .on_channel_destroy(&master_channel);

        wait_until("acting master cleanup", || {
            let route_manager = bootstrap.name_server_runtime.inner.route_info_manager();
            let cluster_info = route_manager.get_all_cluster_info();
            let Some(route_data) = route_manager.pickup_topic_route_data(&topic_name) else {
                return false;
            };
            let Some(route_broker_data) = route_data
                .broker_datas
                .iter()
                .find(|broker_data| broker_data.broker_name() == &broker_name)
            else {
                return false;
            };
            let Some(route_queue_data) = route_data
                .queue_datas
                .iter()
                .find(|queue_data| queue_data.broker_name() == &broker_name)
            else {
                return false;
            };
            let Some(cluster_broker_data) = cluster_info
                .broker_addr_table
                .as_ref()
                .and_then(|brokers| brokers.get(&broker_name))
            else {
                return false;
            };

            route_manager
                .query_broker_topic_config(cluster_name.clone(), master_addr.clone())
                .is_none()
                && route_manager
                    .query_broker_topic_config(cluster_name.clone(), slave_addr.clone())
                    .is_some()
                && !PermName::is_writeable(route_queue_data.perm())
                && route_broker_data.broker_addrs().get(&MASTER_ID) == Some(&slave_addr)
                && !route_broker_data.broker_addrs().contains_key(&1)
                && cluster_broker_data.broker_addrs().get(&1) == Some(&slave_addr)
                && !cluster_broker_data.broker_addrs().contains_key(&MASTER_ID)
        })
        .await;

        shutdown_unregister_service(&bootstrap);
    }

    #[tokio::test]
    async fn boot_fails_fast_when_cluster_test_mode_is_enabled() {
        let namesrv_config = NamesrvConfig {
            cluster_test: true,
            ..NamesrvConfig::default()
        };
        let bootstrap = Builder::new().set_name_server_config(namesrv_config).build();

        let error = bootstrap
            .boot_with_shutdown(async {})
            .await
            .expect_err("cluster test mode should fail fast until implemented");

        assert!(error.to_string().contains("clusterTest"));
    }

    #[tokio::test]
    async fn boot_fails_fast_when_controller_in_namesrv_is_enabled() {
        let namesrv_config = NamesrvConfig {
            enable_controller_in_namesrv: true,
            ..NamesrvConfig::default()
        };
        let bootstrap = Builder::new().set_name_server_config(namesrv_config).build();

        let error = bootstrap
            .boot_with_shutdown(async {})
            .await
            .expect_err("controller-in-namesrv mode should fail fast until implemented");

        assert!(error.to_string().contains("enableControllerInNamesrv"));
    }

    #[tokio::test]
    async fn default_v2_unsupported_request_code_returns_request_code_not_supported() {
        let bootstrap = build_bootstrap_with_default_v2();
        let harness = LocalRequestHarness::new().await.unwrap();
        let mut processor = DefaultRequestProcessor::new(bootstrap.name_server_runtime.inner.clone());
        let mut request = RemotingCommand::create_remoting_command(RequestCode::SendMessage);

        let response = processor
            .process_request_inner(
                harness.channel(),
                harness.context(),
                RequestCode::SendMessage,
                &mut request,
            )
            .expect("request should be handled")
            .expect("processor should return a response");

        assert_eq!(
            ResponseCode::from(response.code()),
            ResponseCode::RequestCodeNotSupported
        );
        assert_eq!(
            response.remark().map(|remark| remark.as_str()),
            Some(" request type 10 not supported")
        );
    }

    #[tokio::test]
    async fn default_v2_get_namesrv_config_returns_aggregated_runtime_properties() {
        let namesrv_config = NamesrvConfig {
            client_request_thread_pool_nums: 12,
            ..NamesrvConfig::default()
        };
        let server_config = ServerConfig {
            listen_port: 19876,
            bind_address: "127.0.0.2".to_string(),
        };
        let bootstrap = Builder::new()
            .set_name_server_config(namesrv_config)
            .set_server_config(server_config)
            .build();
        let harness = LocalRequestHarness::new().await.unwrap();
        let mut request = RemotingCommand::create_remoting_command(RequestCode::GetNamesrvConfig);

        let response = process_with_default_processor(&bootstrap, &harness, &mut request).await;

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);

        let body = response.body().expect("config response should include a body");
        let body = str::from_utf8(body).expect("config body should be utf-8");
        let properties = string_to_properties(body).expect("config body should use java properties format");
        let client_worker_threads = bootstrap
            .name_server_runtime
            .inner
            .tokio_client_config()
            .client_worker_threads
            .to_string();

        assert_eq!(properties.get("listenPort").map(|value| value.as_str()), Some("19876"));
        assert_eq!(
            properties.get("bindAddress").map(|value| value.as_str()),
            Some("127.0.0.2")
        );
        assert_eq!(
            properties
                .get("clientRequestThreadPoolNums")
                .map(|value| value.as_str()),
            Some("12")
        );
        assert_eq!(
            properties.get("clientWorkerThreads").map(|value| value.as_str()),
            Some(client_worker_threads.as_str())
        );
        assert_eq!(
            properties.get("useRouteInfoManagerV2").map(|value| value.as_str()),
            Some("true")
        );
    }

    #[tokio::test]
    async fn default_v2_update_namesrv_config_updates_aggregate_known_keys_and_ignores_unknown() {
        let bootstrap = build_bootstrap_with_default_v2();
        let harness = LocalRequestHarness::new().await.unwrap();
        let mut request = RemotingCommand::create_remoting_command(RequestCode::UpdateNamesrvConfig).set_body(
            b"listenPort=19876\nbindAddress=127.0.0.2\nclientWorkerThreads=9\nenableTopicList=false\nunknownKey=42"
                .as_slice(),
        );

        let response = process_with_default_processor(&bootstrap, &harness, &mut request).await;

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert_eq!(bootstrap.name_server_runtime.inner.server_config().listen_port, 19876);
        assert_eq!(
            bootstrap.name_server_runtime.inner.server_config().bind_address,
            "127.0.0.2"
        );
        assert_eq!(
            bootstrap
                .name_server_runtime
                .inner
                .tokio_client_config()
                .client_worker_threads,
            9
        );
        assert!(
            !bootstrap
                .name_server_runtime
                .inner
                .name_server_config()
                .enable_topic_list
        );
    }

    #[tokio::test]
    async fn default_v2_update_namesrv_config_rejects_fixed_blacklist_keys() {
        let bootstrap = build_bootstrap_with_default_v2();
        let harness = LocalRequestHarness::new().await.unwrap();
        let mut request = RemotingCommand::create_remoting_command(RequestCode::UpdateNamesrvConfig)
            .set_body(b"rocketmqHome=/tmp/namesrv".as_slice());

        let response = process_with_default_processor(&bootstrap, &harness, &mut request).await;

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::NoPermission);
        assert_eq!(
            response.remark().map(|remark| remark.as_str()),
            Some("Cannot update config in blacklist.")
        );
        assert_ne!(
            bootstrap.name_server_runtime.inner.name_server_config().rocketmq_home,
            "/tmp/namesrv"
        );
    }
}
