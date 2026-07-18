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
use std::net::IpAddr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;
use std::time::Duration;
use std::time::Instant;

use arc_swap::ArcSwap;
use cheetah_string::CheetahString;
use rocketmq_common::common::controller::ControllerConfig;
use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_common::utils::network_util::NetworkUtil;
use rocketmq_controller::ControllerManager;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::UnifiedServiceError;
use rocketmq_remoting::base::channel_event_listener::ChannelEventListener;
use rocketmq_remoting::clients::rocketmq_tokio_client::RemotingClientShutdownReport;
use rocketmq_remoting::clients::rocketmq_tokio_client::RocketmqDefaultClient;
use rocketmq_remoting::clients::RemotingClient;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::remoting::RemotingService;
use rocketmq_remoting::remoting_server::rocketmq_tokio_server::RocketMQServer;
use rocketmq_remoting::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_runtime::wait_for_signal;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ServiceContext;
use rocketmq_runtime::ServiceLifecycle;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReason;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use serde::Serialize;
use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tokio::sync::Notify;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::warn;

use crate::processor::ClientRequestProcessor;
use crate::processor::ClusterTestRequestProcessor;
use crate::processor::ClusterTestRouteLookup;
use crate::processor::NameServerRequestProcessor;
use crate::processor::NameServerRequestProcessorWrapper;
use crate::processor::TransportClusterTestRouteLookup;
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

#[doc(hidden)]
#[derive(Debug, Clone, Default, Serialize)]
pub struct NameServerInFlightDrainReport {
    pub elapsed_ms: u64,
    pub timeout_ms: u64,
    pub completed: u64,
    pub remaining: usize,
    pub timed_out: bool,
}

impl NameServerInFlightDrainReport {
    #[doc(hidden)]
    pub fn is_healthy(&self) -> bool {
        !self.timed_out && self.remaining == 0
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, Default, Serialize)]
pub struct NameServerShutdownReport {
    pub elapsed_ms: u64,
    pub deadline_expired: bool,
    pub in_flight: NameServerInFlightDrainReport,
    pub scheduled: Option<ShutdownReport>,
    pub embedded_controller_healthy: Option<bool>,
    pub route_unregistration: Option<ShutdownReport>,
    pub cluster_test_route_lookup_healthy: Option<bool>,
    pub server: Option<ShutdownReport>,
    pub remoting_server: Option<ShutdownReport>,
    pub remoting_client: Option<RemotingClientShutdownReport>,
    pub root: Option<ShutdownReport>,
}

impl NameServerShutdownReport {
    #[doc(hidden)]
    pub fn is_healthy(&self) -> bool {
        !self.deadline_expired
            && self.in_flight.is_healthy()
            && self.scheduled.as_ref().is_none_or(ShutdownReport::is_healthy)
            && self.embedded_controller_healthy.unwrap_or(true)
            && self
                .route_unregistration
                .as_ref()
                .is_none_or(ShutdownReport::is_healthy)
            && self.cluster_test_route_lookup_healthy.unwrap_or(true)
            && self.server.as_ref().is_none_or(ShutdownReport::is_healthy)
            && self.remoting_server.as_ref().is_none_or(ShutdownReport::is_healthy)
            && self
                .remoting_client
                .as_ref()
                .is_none_or(RemotingClientShutdownReport::is_healthy)
            && self.root.as_ref().is_none_or(ShutdownReport::is_healthy)
    }
}

#[derive(Debug, Default)]
pub(crate) struct InFlightRequestTracker {
    active: AtomicUsize,
    completed: AtomicU64,
    notify: Notify,
}

impl InFlightRequestTracker {
    pub(crate) fn enter(self: &Arc<Self>) -> InFlightRequestGuard {
        self.active.fetch_add(1, Ordering::AcqRel);
        InFlightRequestGuard {
            tracker: Arc::clone(self),
        }
    }

    async fn drain(&self, timeout: Duration) -> NameServerInFlightDrainReport {
        let started_at = Instant::now();
        let timed_out = if self.active.load(Ordering::Acquire) == 0 {
            false
        } else {
            tokio::time::timeout(timeout, async {
                loop {
                    let notified = self.notify.notified();
                    if self.active.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    notified.await;
                }
            })
            .await
            .is_err()
        };

        NameServerInFlightDrainReport {
            elapsed_ms: started_at.elapsed().as_millis() as u64,
            timeout_ms: timeout.as_millis() as u64,
            completed: self.completed.load(Ordering::Acquire),
            remaining: self.active.load(Ordering::Acquire),
            timed_out,
        }
    }
}

#[derive(Debug)]
pub(crate) struct InFlightRequestGuard {
    tracker: Arc<InFlightRequestTracker>,
}

impl Drop for InFlightRequestGuard {
    fn drop(&mut self) {
        let previous = self.tracker.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "in-flight request counter underflow");
        self.tracker.completed.fetch_add(1, Ordering::AcqRel);
        self.tracker.notify.notify_waiters();
    }
}

/// Builder for creating NameServerBootstrap with custom configuration
pub struct Builder {
    name_server_config: Option<NamesrvConfig>,
    server_config: Option<ServerConfig>,
    controller_config: Option<ControllerConfig>,
    cluster_test_route_lookup: Option<Arc<dyn ClusterTestRouteLookup>>,
    service_context: Option<ServiceContext>,
}

/// Core runtime managing NameServer lifecycle and operations
///
/// Coordinates initialization, startup, and graceful shutdown of all components.
struct NameServerRuntime {
    inner: Arc<NameServerRuntimeInner>,
    scheduled_tasks: Option<ScheduledTaskGroup>,
    shutdown_tx: Option<broadcast::Sender<()>>,
    shutdown_rx: Option<broadcast::Receiver<()>>,
    server_inner: Option<RocketMQServer<NameServerRequestProcessor>>,
    /// Server task group for graceful shutdown
    server_task_group: Option<TaskGroup>,
    server_report_rx: Option<oneshot::Receiver<Option<ShutdownReport>>>,
    /// Runtime state machine for lifecycle management
    state: Arc<AtomicU8>,
}

impl NameServerBootstrap {
    #[inline]
    pub(crate) fn runtime_inner(&self) -> Arc<NameServerRuntimeInner> {
        Arc::clone(&self.name_server_runtime.inner)
    }

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
    pub async fn boot_with_shutdown<F>(self, shutdown_signal: F) -> RocketMQResult<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.boot_with_shutdown_report(shutdown_signal).await.map(|_| ())
    }

    #[doc(hidden)]
    #[instrument(skip(self, shutdown_signal), name = "nameserver_boot_with_shutdown_report")]
    pub async fn boot_with_shutdown_report<F>(self, shutdown_signal: F) -> RocketMQResult<NameServerShutdownReport>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.boot_with_shutdown_report_and_lifecycle(shutdown_signal, None)
            .await
    }

    /// Boots the NameServer under the shared process lifecycle and shutdown deadline.
    ///
    /// # Errors
    ///
    /// Returns the NameServer startup error or a typed runtime lifecycle error.
    pub async fn boot_with_lifecycle(self, lifecycle: ServiceLifecycle) -> RocketMQResult<NameServerShutdownReport> {
        let shutdown_lifecycle = lifecycle.clone();
        self.boot_with_shutdown_report_and_lifecycle(
            async move {
                if let Err(error) = shutdown_lifecycle.wait_for_shutdown_signal().await {
                    warn!(error = %error, "NameServer signal observation failed");
                    shutdown_lifecycle.mark_failed();
                    shutdown_lifecycle.request_shutdown(ShutdownReason::Internal);
                }
            },
            Some(lifecycle),
        )
        .await
    }

    async fn boot_with_shutdown_report_and_lifecycle<F>(
        mut self,
        shutdown_signal: F,
        lifecycle: Option<ServiceLifecycle>,
    ) -> RocketMQResult<NameServerShutdownReport>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        info!("Booting RocketMQ NameServer (Rust)...");

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        self.name_server_runtime.shutdown_tx = Some(shutdown_tx.clone());
        self.name_server_runtime.shutdown_rx = Some(shutdown_rx);
        self.name_server_runtime.initialize().await?;

        let relay_group = self
            .name_server_runtime
            .inner
            .task_group()
            .map(|task_group| task_group.child("namesrv.shutdown-relay"))
            .ok_or_else(|| namesrv_task_group_unavailable("spawn shutdown relay"))?;
        relay_group
            .spawn_service(
                "namesrv.shutdown-relay",
                relay_shutdown_signal(shutdown_tx, shutdown_signal),
            )
            .map_err(|error| namesrv_startup_failed("spawn shutdown relay", error))?;
        let start_result = self
            .name_server_runtime
            .start_with_shutdown_report(lifecycle.as_ref())
            .await;
        if start_result.is_err() {
            if let Some(lifecycle) = lifecycle.as_ref() {
                lifecycle.mark_failed();
                lifecycle.request_shutdown(ShutdownReason::Internal);
            }
        }
        let relay_timeout = lifecycle
            .as_ref()
            .and_then(ServiceLifecycle::shutdown_request)
            .map(|request| request.deadline.remaining())
            .unwrap_or(Duration::from_secs(5));
        let report = relay_group.shutdown(relay_timeout).await;
        if let Err(error) = report.assert_no_task_leak() {
            warn!("NameServer shutdown relay task group stopped with report: {error}");
            if let Some(lifecycle) = lifecycle.as_ref() {
                lifecycle.mark_failed();
                lifecycle.request_shutdown(ShutdownReason::Internal);
            }
        }
        let shutdown_report = start_result?;
        if let Some(lifecycle) = lifecycle {
            if shutdown_report.is_healthy() {
                lifecycle.mark_stopped();
            } else {
                lifecycle.mark_failed();
            }
        }

        info!("NameServer shutdown completed");
        Ok(shutdown_report)
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

fn namesrv_startup_failed(operation: &'static str, error: impl std::fmt::Display) -> RocketMQError {
    RocketMQError::Service(UnifiedServiceError::StartupFailed(format!(
        "NameServer {operation}: {error}"
    )))
}

fn namesrv_task_group_unavailable(operation: &'static str) -> RocketMQError {
    RocketMQError::Service(UnifiedServiceError::StartupFailed(format!(
        "NameServer {operation}: task group is unavailable"
    )))
}

fn namesrv_runtime_state_error(message: impl Into<String>) -> RocketMQError {
    RocketMQError::Service(UnifiedServiceError::StartupFailed(format!(
        "NameServer runtime state: {}",
        message.into()
    )))
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
            return Err(namesrv_runtime_state_error(error_msg));
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
            return Err(namesrv_runtime_state_error(error_msg));
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
        self.start_schedule_service()?;

        // Transition to Initialized state
        self.transition_to(RuntimeState::Initialized)?;

        info!("Initialization completed successfully");
        Ok(())
    }

    fn validate_runtime_config(&self) -> RocketMQResult<()> {
        let namesrv_config = self.inner.name_server_config();

        if namesrv_config.enable_controller_in_namesrv {
            let controller_config =
                self.inner
                    .controller_config()
                    .ok_or_else(|| RocketMQError::ConfigInvalidValue {
                        key: "enableControllerInNamesrv",
                        value: namesrv_config.enable_controller_in_namesrv.to_string(),
                        reason: "controller config is missing".to_string(),
                    })?;

            let server_config = self.inner.server_config();
            if controller_conflicts_with_namesrv(controller_config.as_ref(), server_config.as_ref()) {
                return Err(RocketMQError::ConfigInvalidValue {
                    key: "enableControllerInNamesrv",
                    value: namesrv_config.enable_controller_in_namesrv.to_string(),
                    reason: format!(
                        "controller listen address {} conflicts with namesrv address {}:{}",
                        controller_config.listen_addr,
                        self.inner.server_config().bind_address,
                        self.inner.server_config().listen_port
                    ),
                });
            }
        }

        if namesrv_config.cluster_test && self.inner.cluster_test_route_lookup().is_none() {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "clusterTest",
                value: namesrv_config.cluster_test.to_string(),
                reason: "cluster-test route lookup requires an injected ServiceContext owner".to_string(),
            });
        }

        Ok(())
    }

    async fn load_config(&mut self) -> RocketMQResult<()> {
        // KVConfigManager is now always initialized
        self.inner.kvconfig_manager().load().map_err(|e| {
            error!("KV config load failed: {}", e);
            RocketMQError::storage_read_failed("kv_config", format!("Configuration load error: {}", e))
        })?;
        debug!("KV configuration loaded successfully");

        if let Some(cluster_test_route_lookup) = self.inner.cluster_test_route_lookup() {
            cluster_test_route_lookup.start().await?;
            debug!("Cluster test route lookup started successfully");
        }

        if self.inner.name_server_config().enable_controller_in_namesrv {
            let controller_config = self
                .inner
                .controller_config()
                .expect("controller config should exist when embedded controller is enabled");
            let controller_manager = Arc::new(ControllerManager::new((*controller_config).clone()).await?);
            let initialized = controller_manager.initialize().await?;
            if !initialized {
                return Err(namesrv_startup_failed(
                    "initialize embedded controller",
                    "controller manager initialization returned false",
                ));
            }
            self.inner.install_controller_manager(controller_manager)?;
            debug!("Embedded controller initialized successfully");
        }
        Ok(())
    }

    /// Initialize network server for handling client requests
    fn initialize_network_components(&mut self) {
        let config = self.inner.server_config();
        let server = match self.inner.service_context.as_ref() {
            Some(context) => RocketMQServer::new_with_service_context(config, context.child("namesrv.remoting-server")),
            None => RocketMQServer::new(config),
        };
        self.server_inner = Some(server);
        debug!(
            "Network server initialized on port {}",
            self.inner.server_config().listen_port
        );
    }

    /// Start scheduled tasks for system health monitoring
    ///
    /// Schedules periodic broker health checks to detect and remove inactive brokers
    fn start_schedule_service(&mut self) -> RocketMQResult<()> {
        let scan_not_active_broker_interval = self.inner.name_server_config().scan_not_active_broker_interval;
        let name_server_runtime_inner = NameServerRuntimeHandle::new(&self.inner);
        let task_group = self
            .inner
            .task_group()
            .map(|task_group| task_group.child("namesrv.scheduled"))
            .ok_or_else(|| namesrv_task_group_unavailable("start scheduled tasks"))?;
        let scheduled_tasks = ScheduledTaskGroup::new(task_group);
        let mut config = ScheduledTaskConfig::fixed_rate_no_overlap(
            "namesrv.scan-not-active-broker",
            Duration::from_millis(scan_not_active_broker_interval),
        );
        config.initial_delay = Duration::from_secs(5);

        scheduled_tasks
            .schedule_fixed_rate_no_overlap(config, move || {
                let name_server_runtime_inner = name_server_runtime_inner.clone();
                async move {
                    debug!("Running scheduled broker health check");
                    if let Some(runtime) = name_server_runtime_inner.upgrade() {
                        let route_info_manager = runtime.route_info_manager();
                        route_info_manager.scan_not_active_broker();
                    }
                }
            })
            .map_err(|error| namesrv_startup_failed("start broker health check scheduled task", error))?;
        self.scheduled_tasks = Some(scheduled_tasks);

        info!(
            "Scheduled task started: broker health check (interval: {}ms)",
            scan_not_active_broker_interval
        );
        Ok(())
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
    pub async fn start(&mut self) -> RocketMQResult<()> {
        self.start_with_shutdown_report(None).await.map(|_| ())
    }

    #[instrument(skip(self), name = "runtime_start_with_shutdown_report")]
    async fn start_with_shutdown_report(
        &mut self,
        lifecycle: Option<&ServiceLifecycle>,
    ) -> RocketMQResult<NameServerShutdownReport> {
        // Validate we're in Initialized state
        if let Err(e) = self.validate_state(&[RuntimeState::Initialized], "start") {
            error!("Cannot start: {}", e);
            return Err(e);
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
        let channel_event_listener = Some(self.inner.broker_housekeeping_service() as Arc<dyn ChannelEventListener>);

        // Spawn server task and retain handle for graceful shutdown
        let mut server_shutdown_rx = self
            .shutdown_tx
            .as_ref()
            .expect("Shutdown channel not initialized")
            .subscribe();
        let server_task_group = self
            .inner
            .task_group()
            .map(|task_group| task_group.child("namesrv.server"))
            .ok_or_else(|| namesrv_task_group_unavailable("spawn server task"))?;
        let (server_report_tx, server_report_rx) = oneshot::channel();
        server_task_group
            .spawn_service("namesrv.server", async move {
                debug!("Server task started");
                let report = server
                    .run_with_shutdown_report(request_processor, channel_event_listener, async move {
                        let _ = server_shutdown_rx.recv().await;
                    })
                    .await;
                if let Some(report) = report.as_ref() {
                    report.log_if_unhealthy();
                }
                let _ = server_report_tx.send(report);
                debug!("Server task completed");
            })
            .map_err(|error| namesrv_startup_failed("spawn server task", error))?;
        self.server_task_group = Some(server_task_group);
        self.server_report_rx = Some(server_report_rx);

        // Setup remoting client with name server address
        let local_address = NetworkUtil::get_local_address().unwrap_or_else(|| {
            warn!("Failed to determine local address, using 127.0.0.1");
            "127.0.0.1".to_string()
        });

        let namesrv =
            CheetahString::from_string(format!("{}:{}", local_address, self.inner.server_config().listen_port));

        debug!("NameServer address: {}", namesrv);

        let weak_client = Arc::downgrade(&self.inner.remoting_client);
        self.inner
            .remoting_client
            .update_name_server_address_list(vec![namesrv])
            .await;

        // Start remoting client directly (no spawn needed as it's managed by self.inner)
        self.inner.remoting_client.start(weak_client).await;

        if let Some(controller_manager) = self.inner.controller_manager() {
            if let Err(error) = controller_manager.start().await {
                if let Some(shutdown_tx) = self.shutdown_tx.as_ref() {
                    let _ = shutdown_tx.send(());
                }
                let _ = self.shutdown().await;
                return Err(error.into());
            }
        }

        // Transition to Running state
        if let Err(e) = self.transition_to(RuntimeState::Running) {
            error!("Failed to transition to Running state: {}", e);
            return Err(e);
        }

        info!("NameServer is now running and accepting requests");
        if let Some(lifecycle) = lifecycle {
            if let Err(error) = lifecycle.mark_ready() {
                warn!(error = %error, "NameServer entered drain before readiness publication");
            }
        }

        // Wait for shutdown signal
        let shutdown_report = tokio::select! {
            result = self.shutdown_rx.as_mut()
                .expect("Shutdown channel not initialized")
                .recv() => {
                match result {
                    Ok(_) => info!("Shutdown signal received, initiating graceful shutdown..."),
                    Err(e) => error!("Error receiving shutdown signal: {}", e),
                }
                let deadline = lifecycle
                    .and_then(ServiceLifecycle::shutdown_request)
                    .map(|request| request.deadline)
                    .unwrap_or_else(|| ShutdownDeadline::after(Duration::from_secs(30)));
                self.shutdown_until(deadline).await
            }
        };

        Ok(shutdown_report)
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
    async fn shutdown(&mut self) -> NameServerShutdownReport {
        self.shutdown_until(ShutdownDeadline::after(Duration::from_secs(30)))
            .await
    }

    async fn shutdown_until(&mut self, deadline: ShutdownDeadline) -> NameServerShutdownReport {
        let started_at = Instant::now();
        let mut shutdown_report = NameServerShutdownReport::default();
        // Validate we're in Running state and transition to ShuttingDown
        if let Err(e) = self.validate_state(&[RuntimeState::Running], "shutdown") {
            warn!("Shutdown called in unexpected state: {}", e);
            // Allow shutdown even in unexpected state for safety
        }

        if let Err(e) = self.transition_to(RuntimeState::ShuttingDown) {
            error!("Failed to transition to ShuttingDown state: {}", e);
        }

        const TASK_JOIN_TIMEOUT: Duration = Duration::from_secs(10);
        let in_flight_timeout = deadline.remaining().min(Duration::from_secs(30));

        info!(
            "Phase 1/5: Waiting for in-flight requests (remaining: {}ms)...",
            in_flight_timeout.as_millis()
        );
        shutdown_report.in_flight = self.wait_for_inflight_requests(in_flight_timeout).await;
        if !shutdown_report.in_flight.is_healthy() {
            warn!(
                "In-flight request drain report is unhealthy: {:?}",
                shutdown_report.in_flight
            );
        }

        info!("Phase 2/5: Stopping scheduled tasks...");
        if let Some(scheduled_tasks) = self.scheduled_tasks.take() {
            let scheduled_report = scheduled_tasks.shutdown(deadline.remaining()).await;
            if let Err(error) = scheduled_report.assert_no_task_leak() {
                warn!("NameServer scheduled task shutdown report is unhealthy: {error}");
            }
            shutdown_report.scheduled = Some(scheduled_report);
        }

        info!("Phase 3/5: Shutting down embedded controller...");
        if let Some(controller_manager) = self.inner.controller_manager() {
            shutdown_report.embedded_controller_healthy =
                Some(match controller_manager.shutdown_until(deadline).await {
                    Ok(()) => true,
                    Err(error) => {
                        warn!("Embedded controller shutdown failed: {}", error);
                        false
                    }
                });
        }

        info!("Phase 4/5: Shutting down route info manager...");
        shutdown_report.route_unregistration = match tokio::time::timeout(
            deadline.remaining(),
            self.inner.route_info_manager().shutdown_unregister_service(),
        )
        .await
        {
            Ok(report) => report,
            Err(_) => {
                warn!("NameServer route unregistration exhausted the shutdown deadline");
                None
            }
        };

        if let Some(cluster_test_route_lookup) = self.inner.cluster_test_route_lookup() {
            shutdown_report.cluster_test_route_lookup_healthy = Some(
                match tokio::time::timeout(deadline.remaining(), cluster_test_route_lookup.shutdown()).await {
                    Ok(Ok(())) => true,
                    Ok(Err(error)) => {
                        warn!("Cluster test route lookup shutdown failed: {error}");
                        false
                    }
                    Err(_) => {
                        warn!("Cluster test route lookup exhausted the NameServer deadline");
                        false
                    }
                },
            );
        }

        info!(
            "Phase 5/5: Waiting for server task (timeout: {}s)...",
            TASK_JOIN_TIMEOUT.as_secs()
        );
        shutdown_report.server = self
            .wait_for_server_task(deadline.remaining().min(TASK_JOIN_TIMEOUT))
            .await;
        shutdown_report.remoting_server = self
            .wait_for_remoting_server_report(deadline.remaining().min(TASK_JOIN_TIMEOUT))
            .await;
        let remoting_client_report = self
            .inner
            .remoting_client
            .shutdown_with_report(deadline.remaining().min(TASK_JOIN_TIMEOUT))
            .await;
        if !remoting_client_report.is_healthy() {
            warn!("NameServer remoting client shutdown report is unhealthy: {remoting_client_report:?}");
        }
        shutdown_report.remoting_client = Some(remoting_client_report);

        if let Some(task_group) = self.inner.task_group.get().cloned() {
            let report = task_group.shutdown_until(deadline).await;
            if let Err(error) = report.assert_no_task_leak() {
                warn!("NameServer task group shutdown report is unhealthy: {error}");
            }
            shutdown_report.root = Some(report);
        }

        // Transition to Stopped state
        if let Err(e) = self.transition_to(RuntimeState::Stopped) {
            error!("Failed to transition to Stopped state: {}", e);
        }

        shutdown_report.deadline_expired = deadline.is_expired();
        shutdown_report.elapsed_ms = started_at.elapsed().as_millis() as u64;
        info!("Graceful shutdown completed");
        shutdown_report
    }

    /// Wait for all in-flight requests to complete
    ///
    /// This provides a grace period for ongoing requests to finish before shutdown.
    /// Returns immediately if no requests are in-flight.
    #[instrument(skip(self), name = "wait_inflight_requests")]
    async fn wait_for_inflight_requests(&self, timeout: Duration) -> NameServerInFlightDrainReport {
        self.inner.in_flight_requests.drain(timeout).await
    }

    /// Wait for server task to complete
    ///
    /// Attempts graceful task-group shutdown with timeout. If timeout is exceeded,
    /// tracked server tasks are aborted and reported.
    #[instrument(skip(self), name = "wait_server_task")]
    async fn wait_for_server_task(&mut self, timeout: Duration) -> Option<ShutdownReport> {
        if let Some(task_group) = self.server_task_group.take() {
            let report = task_group.shutdown(timeout).await;
            if let Err(error) = report.assert_no_task_leak() {
                warn!("Server task group shutdown report is unhealthy: {error}");
            }
            debug!("Server task completed successfully");
            Some(report)
        } else {
            debug!("No server task group to wait for");
            None
        }
    }

    async fn wait_for_remoting_server_report(&mut self, timeout: Duration) -> Option<ShutdownReport> {
        let Some(server_report_rx) = self.server_report_rx.take() else {
            debug!("No remoting server shutdown report receiver to wait for");
            return None;
        };

        match tokio::time::timeout(timeout, server_report_rx).await {
            Ok(Ok(Some(report))) => {
                if let Err(error) = report.assert_no_task_leak() {
                    warn!("Remoting server shutdown report is unhealthy: {error}");
                }
                Some(report)
            }
            Ok(Ok(None)) => {
                warn!("Remoting server exited without a shutdown report");
                None
            }
            Ok(Err(_closed)) => {
                warn!("Remoting server shutdown report channel closed before report was sent");
                None
            }
            Err(_elapsed) => {
                warn!("Timed out waiting for remoting server shutdown report");
                None
            }
        }
    }

    /// Initialize and configure request processor pipeline
    ///
    /// Creates specialized processors for different request types:
    /// - ClientRequestProcessor: Handles topic route queries
    /// - DefaultRequestProcessor: Handles all other requests
    #[inline]
    fn init_processors(&self) -> NameServerRequestProcessor {
        let runtime_handle = NameServerRuntimeHandle::new(&self.inner);
        let route_request_processor = if self.inner.name_server_config().cluster_test {
            NameServerRequestProcessorWrapper::ClusterTestRequestProcessor(Arc::new(ClusterTestRequestProcessor::new(
                runtime_handle.clone(),
            )))
        } else {
            NameServerRequestProcessorWrapper::ClientRequestProcessor(Arc::new(ClientRequestProcessor::new(
                runtime_handle.clone(),
            )))
        };
        let default_request_processor =
            crate::processor::default_request_processor::DefaultRequestProcessor::new(runtime_handle);

        let mut name_server_request_processor =
            NameServerRequestProcessor::new_with_in_flight_tracker(self.inner.in_flight_request_tracker());

        // Register topic route query processor
        name_server_request_processor.register_processor(RequestCode::GetRouteinfoByTopic, route_request_processor);

        // Register default processor for all other requests
        name_server_request_processor.register_default_processor(
            NameServerRequestProcessorWrapper::DefaultRequestProcessor(Arc::new(default_request_processor)),
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
            controller_config: None,
            cluster_test_route_lookup: None,
            service_context: None,
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

    #[inline]
    pub fn set_controller_config(mut self, controller_config: ControllerConfig) -> Self {
        self.controller_config = Some(controller_config);
        self
    }

    #[inline]
    pub fn set_controller_config_opt(mut self, controller_config: Option<ControllerConfig>) -> Self {
        self.controller_config = controller_config;
        self
    }

    #[inline]
    pub(crate) fn set_cluster_test_route_lookup(
        mut self,
        cluster_test_route_lookup: Arc<dyn ClusterTestRouteLookup>,
    ) -> Self {
        self.cluster_test_route_lookup = Some(cluster_test_route_lookup);
        self
    }

    pub fn set_service_context(mut self, service_context: ServiceContext) -> Self {
        self.service_context = Some(service_context);
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
        let controller_config = if name_server_config.enable_controller_in_namesrv {
            Some(self.controller_config.unwrap_or_else(|| {
                ControllerConfig::default().with_rocketmq_home(name_server_config.rocketmq_home.clone())
            }))
        } else {
            self.controller_config
        };
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

        let service_context = self
            .service_context
            .as_ref()
            .map(|context| context.child("rocketmq-namesrv"));
        let cluster_test_route_lookup = if name_server_config.cluster_test {
            self.cluster_test_route_lookup.or_else(|| {
                service_context.as_ref().map(|context| {
                    Arc::new(TransportClusterTestRouteLookup::new(
                        &name_server_config.product_env_name,
                        context.child("namesrv.cluster-test-route-lookup"),
                    )) as Arc<dyn ClusterTestRouteLookup>
                })
            })
        } else {
            self.cluster_test_route_lookup
        };

        // Create remoting client
        let remoting_client = Arc::new(match service_context.as_ref() {
            Some(context) => RocketmqDefaultClient::new_with_service_context(
                Arc::new(tokio_client_config.clone()),
                DefaultRemotingRequestProcessor,
                context.child("namesrv.remoting-client"),
            ),
            None => RocketmqDefaultClient::new(Arc::new(tokio_client_config.clone()), DefaultRemotingRequestProcessor),
        });

        // Check configuration flag for RouteInfoManager version
        let use_v2 = name_server_config.use_route_info_manager_v2;
        let unregister_broker_queue_capacity = name_server_config.unregister_broker_queue_capacity as usize;
        let initial_config = Arc::new(NameServerRuntimeConfig {
            name_server_config: Arc::new(name_server_config),
            tokio_client_config: Arc::new(tokio_client_config),
            server_config: Arc::new(server_config),
            controller_config: controller_config.map(Arc::new),
        });

        // Child services retain only a weak runtime handle. `Arc::new_cyclic` lets the
        // root own every service without creating a service -> runtime strong cycle.
        let inner = Arc::new_cyclic(|weak_inner| {
            let runtime_handle = NameServerRuntimeHandle::from_weak(weak_inner.clone());
            let route_info_manager = if use_v2 {
                info!("Using RouteInfoManager V2 (DashMap-based, 5-50x faster)");
                RouteInfoManagerWrapper::V2(Box::new(RouteInfoManagerV2::new(
                    runtime_handle.clone(),
                    unregister_broker_queue_capacity,
                )))
            } else {
                warn!("Using RouteInfoManager V1 (legacy). Consider V2 for better performance.");
                RouteInfoManagerWrapper::V1(Box::new(parking_lot::Mutex::new(RouteInfoManager::new(
                    runtime_handle.clone(),
                    unregister_broker_queue_capacity,
                ))))
            };

            NameServerRuntimeInner {
                config: ArcSwap::from(Arc::clone(&initial_config)),
                config_update_lock: parking_lot::Mutex::new(()),
                route_info_manager: Arc::new(route_info_manager),
                kvconfig_manager: Arc::new(KVConfigManager::new(runtime_handle.clone())),
                remoting_client,
                broker_housekeeping_service: Arc::new(BrokerHousekeepingService::new(runtime_handle)),
                controller_manager: OnceLock::new(),
                cluster_test_route_lookup,
                service_context,
                task_group: OnceLock::new(),
                in_flight_requests: Arc::new(InFlightRequestTracker::default()),
            }
        });

        info!("NameServer bootstrap built successfully");

        NameServerBootstrap {
            name_server_runtime: NameServerRuntime {
                inner,
                scheduled_tasks: None,
                shutdown_rx: None,
                shutdown_tx: None,
                server_inner: None,
                server_task_group: None,
                server_report_rx: None,
                state: Arc::new(AtomicU8::new(RuntimeState::Created as u8)),
            },
        }
    }
}

/// Internal runtime state shared across components
///
/// Separates immutable components from explicitly synchronized runtime state.
/// Configuration updates publish a new immutable composite snapshot.
pub(crate) struct NameServerRuntimeInner {
    config: ArcSwap<NameServerRuntimeConfig>,
    config_update_lock: parking_lot::Mutex<()>,
    route_info_manager: Arc<RouteInfoManagerWrapper>,
    kvconfig_manager: Arc<KVConfigManager>,
    remoting_client: Arc<RocketmqDefaultClient>,
    broker_housekeeping_service: Arc<BrokerHousekeepingService>,
    controller_manager: OnceLock<Arc<ControllerManager>>,
    cluster_test_route_lookup: Option<Arc<dyn ClusterTestRouteLookup>>,
    service_context: Option<ServiceContext>,
    task_group: OnceLock<TaskGroup>,
    in_flight_requests: Arc<InFlightRequestTracker>,
}

struct NameServerRuntimeConfig {
    name_server_config: Arc<NamesrvConfig>,
    tokio_client_config: Arc<TokioClientConfig>,
    server_config: Arc<ServerConfig>,
    controller_config: Option<Arc<ControllerConfig>>,
}

/// Cloneable non-owning access to the NameServer runtime.
///
/// Runtime-owned child services use this handle so the service graph cannot
/// keep its root alive after `NameServerRuntime` is dropped.
#[derive(Clone)]
pub(crate) struct NameServerRuntimeHandle {
    inner: Weak<NameServerRuntimeInner>,
}

impl NameServerRuntimeHandle {
    fn from_weak(inner: Weak<NameServerRuntimeInner>) -> Self {
        Self { inner }
    }

    pub(crate) fn new(inner: &Arc<NameServerRuntimeInner>) -> Self {
        Self::from_weak(Arc::downgrade(inner))
    }

    pub(crate) fn upgrade(&self) -> Option<Arc<NameServerRuntimeInner>> {
        self.inner.upgrade()
    }

    fn runtime(&self) -> Arc<NameServerRuntimeInner> {
        // Child services are shut down and awaited before the runtime root is
        // released. Callers that may outlive that boundary use `upgrade`.
        self.upgrade()
            .expect("NameServer runtime must outlive its owned service")
    }

    pub(crate) fn name_server_config(&self) -> Arc<NamesrvConfig> {
        self.runtime().name_server_config()
    }

    pub(crate) fn update_name_server_config(
        &self,
        updates: HashMap<CheetahString, CheetahString>,
    ) -> Result<(), String> {
        self.runtime().update_name_server_config(updates)
    }

    pub(crate) fn route_info_manager(&self) -> Arc<RouteInfoManagerWrapper> {
        self.runtime().route_info_manager()
    }

    pub(crate) fn kvconfig_manager(&self) -> Arc<KVConfigManager> {
        self.runtime().kvconfig_manager()
    }

    pub(crate) fn task_group(&self) -> Option<TaskGroup> {
        self.runtime().task_group()
    }

    pub(crate) fn cluster_test_route_lookup(&self) -> Option<Arc<dyn ClusterTestRouteLookup>> {
        self.runtime().cluster_test_route_lookup()
    }

    pub(crate) fn update_runtime_config(&self, updates: HashMap<CheetahString, CheetahString>) -> Result<(), String> {
        self.runtime().update_runtime_config(updates)
    }

    pub(crate) fn get_all_configs_format_string(&self) -> Result<String, String> {
        self.runtime().get_all_configs_format_string()
    }
}

impl NameServerRuntimeInner {
    // Configuration accessors

    #[inline]
    pub fn name_server_config(&self) -> Arc<NamesrvConfig> {
        Arc::clone(&self.config.load().name_server_config)
    }

    pub(crate) fn task_group(&self) -> Option<TaskGroup> {
        if let Some(task_group) = self.task_group.get() {
            return Some(task_group.clone());
        }

        if let Some(service_context) = self.service_context.as_ref() {
            let _ = self.task_group.set(service_context.task_group().clone());
            return self.task_group.get().cloned();
        }

        let handle = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(error) => {
                warn!("NameServer task group is unavailable outside Tokio runtime: {error}");
                return None;
            }
        };
        let task_group = TaskGroup::root("rocketmq-namesrv", RuntimeHandle::new(handle));
        let _ = self.task_group.set(task_group);
        self.task_group.get().cloned()
    }

    pub(crate) fn in_flight_request_tracker(&self) -> Arc<InFlightRequestTracker> {
        Arc::clone(&self.in_flight_requests)
    }

    pub(crate) fn in_flight_request_guard(&self) -> InFlightRequestGuard {
        self.in_flight_requests.enter()
    }

    #[inline]
    pub fn tokio_client_config(&self) -> Arc<TokioClientConfig> {
        Arc::clone(&self.config.load().tokio_client_config)
    }

    #[inline]
    pub fn server_config(&self) -> Arc<ServerConfig> {
        Arc::clone(&self.config.load().server_config)
    }

    #[inline]
    pub fn controller_config(&self) -> Option<Arc<ControllerConfig>> {
        self.config.load().controller_config.clone()
    }

    fn config_snapshot(&self) -> Arc<NameServerRuntimeConfig> {
        self.config.load_full()
    }

    pub(crate) fn update_name_server_config(
        &self,
        updates: HashMap<CheetahString, CheetahString>,
    ) -> Result<(), String> {
        let _update_guard = self.config_update_lock.lock();
        let current = self.config_snapshot();
        let mut name_server_config = (*current.name_server_config).clone();
        name_server_config.update(updates)?;
        self.config.store(Arc::new(NameServerRuntimeConfig {
            name_server_config: Arc::new(name_server_config),
            tokio_client_config: Arc::clone(&current.tokio_client_config),
            server_config: Arc::clone(&current.server_config),
            controller_config: current.controller_config.clone(),
        }));
        Ok(())
    }

    pub fn get_all_configs_format_string(&self) -> Result<String, String> {
        let config_snapshot = self.config_snapshot();
        let name_server_config = &config_snapshot.name_server_config;
        let server_config = &config_snapshot.server_config;
        let tokio_client_config = &config_snapshot.tokio_client_config;
        let mut entries = Vec::with_capacity(41);

        push_config_entry(&mut entries, "rocketmqHome", &name_server_config.rocketmq_home);
        push_config_entry(&mut entries, "kvConfigPath", &name_server_config.kv_config_path);
        push_config_entry(&mut entries, "configStorePath", &name_server_config.config_store_path);
        push_config_entry(&mut entries, "productEnvName", &name_server_config.product_env_name);
        push_config_entry(&mut entries, "clusterTest", name_server_config.cluster_test);
        push_config_entry(
            &mut entries,
            "orderMessageEnable",
            name_server_config.order_message_enable,
        );
        push_config_entry(
            &mut entries,
            "returnOrderTopicConfigToBroker",
            name_server_config.return_order_topic_config_to_broker,
        );
        push_config_entry(
            &mut entries,
            "clientRequestThreadPoolNums",
            name_server_config.client_request_thread_pool_nums,
        );
        push_config_entry(
            &mut entries,
            "defaultThreadPoolNums",
            name_server_config.default_thread_pool_nums,
        );
        push_config_entry(
            &mut entries,
            "clientRequestThreadPoolQueueCapacity",
            name_server_config.client_request_thread_pool_queue_capacity,
        );
        push_config_entry(
            &mut entries,
            "defaultThreadPoolQueueCapacity",
            name_server_config.default_thread_pool_queue_capacity,
        );
        push_config_entry(
            &mut entries,
            "scanNotActiveBrokerInterval",
            name_server_config.scan_not_active_broker_interval,
        );
        push_config_entry(
            &mut entries,
            "unRegisterBrokerQueueCapacity",
            name_server_config.unregister_broker_queue_capacity,
        );
        push_config_entry(
            &mut entries,
            "supportActingMaster",
            name_server_config.support_acting_master,
        );
        push_config_entry(
            &mut entries,
            "enableAllTopicList",
            name_server_config.enable_all_topic_list,
        );
        push_config_entry(&mut entries, "enableTopicList", name_server_config.enable_topic_list);
        push_config_entry(
            &mut entries,
            "notifyMinBrokerIdChanged",
            name_server_config.notify_min_broker_id_changed,
        );
        push_config_entry(
            &mut entries,
            "enableControllerInNamesrv",
            name_server_config.enable_controller_in_namesrv,
        );
        push_config_entry(
            &mut entries,
            "needWaitForService",
            name_server_config.need_wait_for_service,
        );
        push_config_entry(
            &mut entries,
            "waitSecondsForService",
            name_server_config.wait_seconds_for_service,
        );
        push_config_entry(
            &mut entries,
            "deleteTopicWithBrokerRegistration",
            name_server_config.delete_topic_with_broker_registration,
        );
        push_config_entry(&mut entries, "configBlackList", &name_server_config.config_black_list);
        push_config_entry(
            &mut entries,
            "useRouteInfoManagerV2",
            name_server_config.use_route_info_manager_v2,
        );

        push_config_entry(&mut entries, "listenPort", server_config.listen_port);
        push_config_entry(&mut entries, "bindAddress", &server_config.bind_address);
        for (key, value) in server_config.tls_config.java_property_entries() {
            push_config_entry(&mut entries, key, value);
        }

        push_config_entry(
            &mut entries,
            "clientWorkerThreads",
            tokio_client_config.client_worker_threads,
        );
        push_config_entry(
            &mut entries,
            "clientCallbackExecutorThreads",
            tokio_client_config.client_callback_executor_threads,
        );
        push_config_entry(
            &mut entries,
            "clientOnewaySemaphoreValue",
            tokio_client_config.client_oneway_semaphore_value,
        );
        push_config_entry(
            &mut entries,
            "clientAsyncSemaphoreValue",
            tokio_client_config.client_async_semaphore_value,
        );
        push_config_entry(
            &mut entries,
            "connectTimeoutMillis",
            tokio_client_config.connect_timeout_millis,
        );
        push_config_entry(
            &mut entries,
            "channelNotActiveInterval",
            tokio_client_config.channel_not_active_interval,
        );
        push_config_entry(
            &mut entries,
            "clientChannelMaxIdleTimeSeconds",
            tokio_client_config.client_channel_max_idle_time_seconds,
        );
        push_config_entry(
            &mut entries,
            "clientSocketSndBufSize",
            tokio_client_config.client_socket_snd_buf_size,
        );
        push_config_entry(
            &mut entries,
            "clientSocketRcvBufSize",
            tokio_client_config.client_socket_rcv_buf_size,
        );
        push_config_entry(
            &mut entries,
            "clientPooledByteBufAllocatorEnable",
            tokio_client_config.client_pooled_byte_buf_allocator_enable,
        );
        push_config_entry(
            &mut entries,
            "clientCloseSocketIfTimeout",
            tokio_client_config.client_close_socket_if_timeout,
        );
        push_config_entry(
            &mut entries,
            "socksProxyConfig",
            &tokio_client_config.socks_proxy_config,
        );
        push_config_entry(
            &mut entries,
            "writeBufferHighWaterMark",
            tokio_client_config.write_buffer_high_water_mark,
        );
        push_config_entry(
            &mut entries,
            "writeBufferLowWaterMark",
            tokio_client_config.write_buffer_low_water_mark,
        );
        push_config_entry(
            &mut entries,
            "disableCallbackExecutor",
            tokio_client_config.disable_callback_executor,
        );
        push_config_entry(
            &mut entries,
            "disableNettyWorkerGroup",
            tokio_client_config.disable_netty_worker_group,
        );
        push_config_entry(
            &mut entries,
            "maxReconnectIntervalTimeSeconds",
            tokio_client_config.max_reconnect_interval_time_seconds,
        );
        push_config_entry(
            &mut entries,
            "enableReconnectForGoAway",
            tokio_client_config.enable_reconnect_for_go_away,
        );
        push_config_entry(
            &mut entries,
            "enableTransparentRetry",
            tokio_client_config.enable_transparent_retry,
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

    pub fn update_runtime_config(&self, updates: HashMap<CheetahString, CheetahString>) -> Result<(), String> {
        let _update_guard = self.config_update_lock.lock();
        let current = self.config_snapshot();
        let mut name_server_config = (*current.name_server_config).clone();
        let mut tokio_client_config = (*current.tokio_client_config).clone();
        let mut server_config = (*current.server_config).clone();
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
                    server_config.listen_port = parse_config_value(&key, &value)?;
                }
                "bindAddress" => {
                    server_config.bind_address = value.to_string();
                }
                key if is_tls_config_key(key) => {
                    server_config.tls_config.apply_java_property(key, value.as_str());
                }
                "clientWorkerThreads" => {
                    tokio_client_config.client_worker_threads = parse_config_value(&key, &value)?;
                }
                "clientCallbackExecutorThreads" => {
                    tokio_client_config.client_callback_executor_threads = parse_config_value(&key, &value)?;
                }
                "clientOnewaySemaphoreValue" => {
                    tokio_client_config.client_oneway_semaphore_value = parse_config_value(&key, &value)?;
                }
                "clientAsyncSemaphoreValue" => {
                    tokio_client_config.client_async_semaphore_value = parse_config_value(&key, &value)?;
                }
                "connectTimeoutMillis" => {
                    tokio_client_config.connect_timeout_millis = parse_config_value(&key, &value)?;
                }
                "channelNotActiveInterval" => {
                    tokio_client_config.channel_not_active_interval = parse_config_value(&key, &value)?;
                }
                "clientChannelMaxIdleTimeSeconds" => {
                    tokio_client_config.client_channel_max_idle_time_seconds = parse_config_value(&key, &value)?;
                }
                "clientSocketSndBufSize" => {
                    tokio_client_config.client_socket_snd_buf_size = parse_config_value(&key, &value)?;
                }
                "clientSocketRcvBufSize" => {
                    tokio_client_config.client_socket_rcv_buf_size = parse_config_value(&key, &value)?;
                }
                "clientPooledByteBufAllocatorEnable" => {
                    tokio_client_config.client_pooled_byte_buf_allocator_enable = parse_config_value(&key, &value)?;
                }
                "clientCloseSocketIfTimeout" => {
                    tokio_client_config.client_close_socket_if_timeout = parse_config_value(&key, &value)?;
                }
                "socksProxyConfig" => {
                    tokio_client_config.socks_proxy_config = value.to_string();
                }
                "writeBufferHighWaterMark" => {
                    tokio_client_config.write_buffer_high_water_mark = parse_config_value(&key, &value)?;
                }
                "writeBufferLowWaterMark" => {
                    tokio_client_config.write_buffer_low_water_mark = parse_config_value(&key, &value)?;
                }
                "disableCallbackExecutor" => {
                    tokio_client_config.disable_callback_executor = parse_config_value(&key, &value)?;
                }
                "disableNettyWorkerGroup" => {
                    tokio_client_config.disable_netty_worker_group = parse_config_value(&key, &value)?;
                }
                "maxReconnectIntervalTimeSeconds" => {
                    tokio_client_config.max_reconnect_interval_time_seconds = parse_config_value(&key, &value)?;
                }
                "enableReconnectForGoAway" => {
                    tokio_client_config.enable_reconnect_for_go_away = parse_config_value(&key, &value)?;
                }
                "enableTransparentRetry" => {
                    tokio_client_config.enable_transparent_retry = parse_config_value(&key, &value)?;
                }
                _ => {}
            }
        }

        if !namesrv_updates.is_empty() {
            name_server_config.update(namesrv_updates)?;
        }

        self.config.store(Arc::new(NameServerRuntimeConfig {
            name_server_config: Arc::new(name_server_config),
            tokio_client_config: Arc::new(tokio_client_config),
            server_config: Arc::new(server_config),
            controller_config: current.controller_config.clone(),
        }));
        Ok(())
    }

    // Component accessors

    #[inline]
    pub fn route_info_manager(&self) -> Arc<RouteInfoManagerWrapper> {
        Arc::clone(&self.route_info_manager)
    }

    #[inline]
    pub fn kvconfig_manager(&self) -> Arc<KVConfigManager> {
        Arc::clone(&self.kvconfig_manager)
    }

    #[inline]
    pub fn remoting_client(&self) -> &RocketmqDefaultClient {
        &self.remoting_client
    }

    #[inline]
    pub fn broker_housekeeping_service(&self) -> Arc<BrokerHousekeepingService> {
        Arc::clone(&self.broker_housekeeping_service)
    }

    #[inline]
    pub fn controller_manager(&self) -> Option<Arc<ControllerManager>> {
        self.controller_manager.get().cloned()
    }

    fn install_controller_manager(&self, controller_manager: Arc<ControllerManager>) -> RocketMQResult<()> {
        self.controller_manager
            .set(controller_manager)
            .map_err(|_| namesrv_runtime_state_error("embedded controller manager was already initialized"))
    }

    #[inline]
    pub fn cluster_test_route_lookup(&self) -> Option<Arc<dyn ClusterTestRouteLookup>> {
        self.cluster_test_route_lookup.clone()
    }
}

fn controller_conflicts_with_namesrv(controller_config: &ControllerConfig, server_config: &ServerConfig) -> bool {
    if controller_config.listen_addr.port() != server_config.listen_port as u16 {
        return false;
    }

    let bind_address = server_config.bind_address.as_str();
    if bind_address == "0.0.0.0" || bind_address == "::" {
        return true;
    }

    match bind_address.parse::<IpAddr>() {
        Ok(bind_ip) => bind_ip.is_unspecified() || bind_ip == controller_config.listen_addr.ip(),
        Err(_) => bind_address == controller_config.listen_addr.ip().to_string(),
    }
}

fn push_config_entry(entries: &mut Vec<(&'static str, String)>, key: &'static str, value: impl ToString) {
    entries.push((key, value.to_string()));
}

fn is_tls_config_key(key: &str) -> bool {
    matches!(
        key,
        "tls.enable"
            | "tls.test.mode.enable"
            | "tls.config.file"
            | "tls.server.mode"
            | "tls.server.need.client.auth"
            | "tls.server.keyPath"
            | "tls.server.keyPassword"
            | "tls.server.certPath"
            | "tls.server.authClient"
            | "tls.server.trustCertPath"
            | "tls.client.keyPath"
            | "tls.client.keyPassword"
            | "tls.client.certPath"
            | "tls.client.authServer"
            | "tls.client.trustCertPath"
            | "tls.ciphers"
            | "tls.protocols"
    )
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
    use std::net::TcpListener;
    use std::str;
    use std::time::Duration;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_common::common::constant::PermName;
    use rocketmq_common::common::controller::ControllerConfig;
    use rocketmq_common::common::mix_all::string_to_properties;
    use rocketmq_common::common::mix_all::MASTER_ID;
    use rocketmq_common::common::mix_all::ZONE_NAME;
    use rocketmq_common::common::mq_version::RocketMqVersion;
    use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
    use rocketmq_common::common::server::config::ServerConfig;
    use rocketmq_common::common::tls_config::TlsMode;
    use rocketmq_common::common::TopicSysFlag;
    use rocketmq_common::CRC32Utils;
    use rocketmq_error::ErrorKind;
    use rocketmq_remoting::code::request_code::RequestCode;
    use rocketmq_remoting::code::response_code::ResponseCode;
    use rocketmq_remoting::connection::ConnectionState;
    use rocketmq_remoting::local::LocalRequestHarness;
    use rocketmq_remoting::protocol::body::broker_body::broker_member_group::GetBrokerMemberGroupResponseBody;
    use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
    use rocketmq_remoting::protocol::body::broker_body::register_broker_body::RegisterBrokerBody;
    use rocketmq_remoting::protocol::body::kv_table::KVTable;
    use rocketmq_remoting::protocol::body::topic::topic_list::TopicList;
    use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
    use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
    use rocketmq_remoting::protocol::header::namesrv::broker_request::BrokerHeartbeatRequestHeader;
    use rocketmq_remoting::protocol::header::namesrv::broker_request::GetBrokerMemberGroupRequestHeader;
    use rocketmq_remoting::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
    use rocketmq_remoting::protocol::header::namesrv::kv_config_header::DeleteKVConfigRequestHeader;
    use rocketmq_remoting::protocol::header::namesrv::kv_config_header::GetKVConfigRequestHeader;
    use rocketmq_remoting::protocol::header::namesrv::kv_config_header::GetKVConfigResponseHeader;
    use rocketmq_remoting::protocol::header::namesrv::kv_config_header::GetKVListByNamespaceRequestHeader;
    use rocketmq_remoting::protocol::header::namesrv::kv_config_header::PutKVConfigRequestHeader;
    use rocketmq_remoting::protocol::header::namesrv::perm_broker_header::AddWritePermOfBrokerRequestHeader;
    use rocketmq_remoting::protocol::header::namesrv::perm_broker_header::AddWritePermOfBrokerResponseHeader;
    use rocketmq_remoting::protocol::header::namesrv::perm_broker_header::WipeWritePermOfBrokerRequestHeader;
    use rocketmq_remoting::protocol::header::namesrv::perm_broker_header::WipeWritePermOfBrokerResponseHeader;
    use rocketmq_remoting::protocol::header::namesrv::query_data_version_header::QueryDataVersionRequestHeader;
    use rocketmq_remoting::protocol::header::namesrv::query_data_version_header::QueryDataVersionResponseHeader;
    use rocketmq_remoting::protocol::header::namesrv::register_broker_header::RegisterBrokerRequestHeader;
    use rocketmq_remoting::protocol::header::namesrv::register_broker_header::RegisterBrokerResponseHeader;
    use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::DeleteTopicFromNamesrvRequestHeader;
    use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::GetTopicsByClusterRequestHeader;
    use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::RegisterTopicRequestHeader;
    use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
    use rocketmq_remoting::protocol::route::route_data_view::QueueData;
    use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
    use rocketmq_remoting::protocol::DataVersion;
    use rocketmq_remoting::protocol::RemotingDeserializable;
    use rocketmq_remoting::protocol::RemotingSerializable;
    use rocketmq_remoting::runtime::processor::RequestProcessor;
    use rocketmq_runtime::RuntimeContext;
    use tokio::net::TcpStream as TokioTcpStream;
    use tokio::sync::broadcast;
    use tokio::sync::oneshot;
    use tokio::time::sleep;

    use super::*;
    use crate::processor::default_request_processor::DefaultRequestProcessor;
    use crate::processor::ClientRequestProcessor;

    fn build_bootstrap_with_v2_config(mut namesrv_config: NamesrvConfig) -> NameServerBootstrap {
        namesrv_config.use_route_info_manager_v2 = true;
        let bootstrap = Builder::new().set_name_server_config(namesrv_config).build();
        assert!(matches!(
            bootstrap.name_server_runtime.inner.route_info_manager().as_ref(),
            RouteInfoManagerWrapper::V2(_)
        ));
        bootstrap
    }

    fn build_bootstrap_with_default_v2() -> NameServerBootstrap {
        build_bootstrap_with_v2_config(NamesrvConfig::default())
    }

    #[test]
    fn runtime_config_failed_update_preserves_published_snapshot() {
        let bootstrap = build_bootstrap_with_default_v2();
        let runtime = bootstrap.runtime_inner();
        let before = runtime.config_snapshot();
        let before_port = before.server_config.listen_port;
        let before_workers = before.tokio_client_config.client_worker_threads;

        let result = runtime.update_runtime_config(HashMap::from([
            (
                CheetahString::from_static_str("listenPort"),
                CheetahString::from_static_str("19876"),
            ),
            (
                CheetahString::from_static_str("clientWorkerThreads"),
                CheetahString::from_static_str("not-a-number"),
            ),
        ]));

        assert!(result.is_err());
        let after = runtime.config_snapshot();
        assert!(Arc::ptr_eq(&before, &after));
        assert_eq!(after.server_config.listen_port, before_port);
        assert_eq!(after.tokio_client_config.client_worker_threads, before_workers);
    }

    #[test]
    fn runtime_config_concurrent_readers_only_observe_complete_snapshots() {
        let namesrv_config = NamesrvConfig {
            enable_topic_list: true,
            ..NamesrvConfig::default()
        };
        let server_config = ServerConfig {
            listen_port: 10000,
            ..ServerConfig::default()
        };
        let bootstrap = Builder::new()
            .set_name_server_config(namesrv_config)
            .set_server_config(server_config)
            .build();
        let runtime = bootstrap.runtime_inner();

        let writer =
            |runtime: Arc<NameServerRuntimeInner>, listen_port: &'static str, enable_topic_list: &'static str| {
                std::thread::spawn(move || {
                    for _ in 0..500 {
                        runtime
                            .update_runtime_config(HashMap::from([
                                (
                                    CheetahString::from_static_str("listenPort"),
                                    CheetahString::from_static_str(listen_port),
                                ),
                                (
                                    CheetahString::from_static_str("enableTopicList"),
                                    CheetahString::from_static_str(enable_topic_list),
                                ),
                            ]))
                            .expect("valid snapshot update should succeed");
                    }
                })
            };

        let first_writer = writer(Arc::clone(&runtime), "20001", "false");
        let second_writer = writer(Arc::clone(&runtime), "20002", "true");
        for _ in 0..10_000 {
            let snapshot = runtime.config_snapshot();
            let observed = (
                snapshot.server_config.listen_port,
                snapshot.name_server_config.enable_topic_list,
            );
            assert!(
                matches!(observed, (10000, true) | (20001, false) | (20002, true)),
                "reader observed a torn runtime configuration: {observed:?}"
            );
        }
        first_writer.join().expect("first config writer should not panic");
        second_writer.join().expect("second config writer should not panic");
    }

    #[test]
    fn runtime_owned_service_clones_do_not_keep_root_alive() {
        for use_route_info_manager_v2 in [false, true] {
            let bootstrap = Builder::new()
                .set_name_server_config(NamesrvConfig {
                    use_route_info_manager_v2,
                    ..NamesrvConfig::default()
                })
                .build();
            let request_processor = bootstrap.name_server_runtime.init_processors();
            let runtime = bootstrap.runtime_inner();
            let runtime_handle = NameServerRuntimeHandle::new(&runtime);
            let weak_runtime = Arc::downgrade(&runtime);
            let route_info_manager = runtime.route_info_manager();
            let kvconfig_manager = runtime.kvconfig_manager();
            let broker_housekeeping_service = runtime.broker_housekeeping_service();

            assert_eq!(route_info_manager.scan_not_active_broker(), 0);
            drop(runtime);
            drop(bootstrap);

            assert!(weak_runtime.upgrade().is_none());
            assert!(runtime_handle.upgrade().is_none());

            drop(route_info_manager);
            drop(kvconfig_manager);
            drop(broker_housekeeping_service);
            drop(request_processor);
        }
    }

    #[test]
    fn namesrv_startup_failed_uses_service_error_kind() {
        let error = namesrv_startup_failed("spawn test service", "task group closed");

        assert_eq!(error.kind(), ErrorKind::Service);
        assert!(error.to_string().contains("NameServer spawn test service"));
    }

    #[test]
    fn namesrv_task_group_unavailable_uses_service_error_kind() {
        let error = namesrv_task_group_unavailable("spawn test service");

        assert_eq!(error.kind(), ErrorKind::Service);
        assert!(error.to_string().contains("task group is unavailable"));
    }

    #[test]
    fn namesrv_runtime_state_error_uses_service_error_kind() {
        let error = namesrv_runtime_state_error("invalid Created -> Running transition");

        assert_eq!(error.kind(), ErrorKind::Service);
        assert!(error.to_string().contains("NameServer runtime state"));
    }

    #[test]
    fn invalid_runtime_transition_uses_service_error_kind() {
        let bootstrap = build_bootstrap_with_default_v2();
        let error = bootstrap
            .name_server_runtime
            .transition_to(RuntimeState::Running)
            .expect_err("Created -> Running should be invalid");

        assert_eq!(error.kind(), ErrorKind::Service);
        assert!(error.to_string().contains("Invalid state transition"));
    }

    #[tokio::test]
    async fn builder_service_context_parents_namesrv_task_group() {
        let context = RuntimeContext::from_current("namesrv-context-runtime-test");
        let service = context.service_context("namesrv-service");
        let bootstrap = Builder::new().set_service_context(service.clone()).build();

        let task_group = bootstrap
            .name_server_runtime
            .inner
            .task_group()
            .expect("service context should provide namesrv task group");

        assert_eq!(task_group.parent_id(), Some(service.task_group().id()));
        assert_eq!(task_group.name(), "rocketmq-namesrv");

        let report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn service_context_shutdown_report_includes_remoting_client() {
        let context = RuntimeContext::from_current("namesrv-runtime-owner-test");
        let service = context.service_context("namesrv-service");
        let bootstrap = Builder::new()
            .set_server_config(namesrv_server_config())
            .set_service_context(service)
            .build();

        let report = bootstrap
            .boot_with_shutdown_report(async {})
            .await
            .expect("namesrv should boot and return shutdown report");

        assert!(report.is_healthy(), "{report:?}");
        assert!(
            report
                .remoting_client
                .as_ref()
                .is_some_and(RemotingClientShutdownReport::is_healthy),
            "remoting client shutdown report should be present and healthy: {report:?}"
        );
    }

    #[tokio::test]
    async fn namesrv_in_flight_drain_zero_requests_is_healthy() {
        let bootstrap = build_bootstrap_with_default_v2();

        let report = bootstrap
            .name_server_runtime
            .wait_for_inflight_requests(Duration::from_millis(10))
            .await;

        assert!(report.is_healthy(), "{report:?}");
        assert_eq!(report.completed, 0);
        assert_eq!(report.remaining, 0);
        assert!(!report.timed_out);
    }

    #[tokio::test]
    async fn namesrv_in_flight_drain_waits_for_controlled_request() {
        let bootstrap = build_bootstrap_with_default_v2();
        let guard = bootstrap.name_server_runtime.inner.in_flight_request_guard();
        let wait = bootstrap
            .name_server_runtime
            .wait_for_inflight_requests(Duration::from_secs(1));
        tokio::pin!(wait);

        tokio::select! {
            report = &mut wait => panic!("drain completed while request was still in flight: {report:?}"),
            _ = sleep(Duration::from_millis(10)) => {}
        }

        drop(guard);
        let report = wait.await;

        assert!(report.is_healthy(), "{report:?}");
        assert_eq!(report.completed, 1);
        assert_eq!(report.remaining, 0);
        assert!(!report.timed_out);
    }

    #[tokio::test]
    async fn namesrv_in_flight_drain_timeout_reports_remaining() {
        let bootstrap = build_bootstrap_with_default_v2();
        let guard = bootstrap.name_server_runtime.inner.in_flight_request_guard();

        let report = bootstrap
            .name_server_runtime
            .wait_for_inflight_requests(Duration::from_millis(1))
            .await;

        assert!(!report.is_healthy(), "{report:?}");
        assert_eq!(report.completed, 0);
        assert_eq!(report.remaining, 1);
        assert!(report.timed_out);

        drop(guard);
    }

    fn reserve_local_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .expect("should reserve a local port")
            .local_addr()
            .expect("reserved listener should expose a local addr")
            .port()
    }

    fn namesrv_server_config() -> ServerConfig {
        ServerConfig {
            listen_port: reserve_local_port() as u32,
            bind_address: "127.0.0.1".to_string(),
            ..ServerConfig::default()
        }
    }

    fn embedded_controller_config() -> ControllerConfig {
        ControllerConfig::default()
            .with_node_info(1, format!("127.0.0.1:{}", reserve_local_port()).parse().unwrap())
            .with_storage_path("".to_string())
    }

    async fn process_with_default_processor(
        bootstrap: &NameServerBootstrap,
        harness: &LocalRequestHarness,
        request: &mut RemotingCommand,
    ) -> RemotingCommand {
        let mut processor =
            DefaultRequestProcessor::new(NameServerRuntimeHandle::new(&bootstrap.name_server_runtime.inner));
        processor
            .process_request(harness.channel(), harness.context(), request)
            .await
            .expect("request processing should succeed")
            .expect("processor should always return a response")
    }

    async fn process_with_client_processor(
        bootstrap: &NameServerBootstrap,
        harness: &LocalRequestHarness,
        request: &mut RemotingCommand,
    ) -> RemotingCommand {
        let mut processor =
            ClientRequestProcessor::new(NameServerRuntimeHandle::new(&bootstrap.name_server_runtime.inner));
        processor
            .process_request(harness.channel(), harness.context(), request)
            .await
            .expect("request processing should succeed")
            .expect("processor should always return a response")
    }

    async fn process_with_name_server_processor(
        bootstrap: &NameServerBootstrap,
        harness: &LocalRequestHarness,
        request: &mut RemotingCommand,
    ) -> RemotingCommand {
        let mut processor = bootstrap.name_server_runtime.init_processors();
        processor
            .process_request(harness.channel(), harness.context(), request)
            .await
            .expect("request processing should succeed")
            .expect("processor should always return a response")
    }

    #[tokio::test]
    async fn name_server_processor_records_completed_request() {
        let bootstrap = build_bootstrap_with_default_v2();
        let harness = LocalRequestHarness::new().await.unwrap();
        let mut request = RemotingCommand::create_remoting_command(999_999);

        let response = process_with_name_server_processor(&bootstrap, &harness, &mut request).await;
        assert_eq!(response.code(), ResponseCode::RequestCodeNotSupported as i32);

        let report = bootstrap
            .name_server_runtime
            .wait_for_inflight_requests(Duration::from_millis(10))
            .await;
        assert!(report.is_healthy(), "{report:?}");
        assert_eq!(report.completed, 1);
        assert_eq!(report.remaining, 0);
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

    #[allow(clippy::too_many_arguments)]
    fn register_broker_request(
        cluster_name: &CheetahString,
        broker_name: &CheetahString,
        broker_addr: &CheetahString,
        broker_id: u64,
        ha_server_addr: &CheetahString,
        zone_name: &CheetahString,
        enable_acting_master: bool,
        topic_config_wrapper: TopicConfigAndMappingSerializeWrapper,
        filter_server_list: Vec<CheetahString>,
    ) -> RemotingCommand {
        let body = RegisterBrokerBody::new((&topic_config_wrapper).into(), filter_server_list).encode(false);
        let body_crc32 = CRC32Utils::crc32(&body);
        let mut request = RemotingCommand::create_request_command(
            RequestCode::RegisterBroker,
            RegisterBrokerRequestHeader::new(
                broker_name.clone(),
                broker_addr.clone(),
                cluster_name.clone(),
                ha_server_addr.clone(),
                broker_id,
                Some(30_000),
                Some(enable_acting_master),
                false,
                body_crc32,
            ),
        )
        .set_version(RocketMqVersion::V5_0_0 as i32)
        .set_body(body);
        request.make_custom_header_to_net();
        request.add_ext_field(ZONE_NAME, zone_name.clone());
        request
    }

    fn start_unregister_service(bootstrap: &NameServerBootstrap) {
        bootstrap.name_server_runtime.inner.route_info_manager().start();
    }

    async fn shutdown_unregister_service(bootstrap: &NameServerBootstrap) {
        bootstrap
            .name_server_runtime
            .inner
            .route_info_manager()
            .shutdown()
            .await;
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
    async fn default_v2_aggregate_processor_routes_register_and_route_queries() {
        let bootstrap = build_bootstrap_with_default_v2();
        let harness = LocalRequestHarness::new().await.unwrap();
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let broker_addr = CheetahString::from_static_str("10.0.0.10:10911");
        let ha_server_addr = CheetahString::from_static_str("10.0.0.10:10912");
        let zone_name = CheetahString::from_static_str("zone-a");
        let topic_name = CheetahString::from_static_str("aggregate-route-topic");

        let mut register_request = register_broker_request(
            &cluster_name,
            &broker_name,
            &broker_addr,
            MASTER_ID,
            &ha_server_addr,
            &zone_name,
            true,
            topic_config_wrapper(&[("aggregate-route-topic", 0, PermName::PERM_READ | PermName::PERM_WRITE)]),
            vec![],
        );

        let register_response = process_with_name_server_processor(&bootstrap, &harness, &mut register_request).await;
        assert_eq!(ResponseCode::from(register_response.code()), ResponseCode::Success);

        let mut route_request = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new(topic_name.clone(), Some(true)),
        )
        .set_version(RocketMqVersion::V4_9_3 as i32);
        route_request.make_custom_header_to_net();

        let route_response = process_with_name_server_processor(&bootstrap, &harness, &mut route_request).await;
        assert_eq!(ResponseCode::from(route_response.code()), ResponseCode::Success);

        let topic_route_data =
            TopicRouteData::decode(route_response.body().expect("route response should include body"))
                .expect("route response body should decode");
        assert_eq!(topic_route_data.queue_datas.len(), 1);
        assert_eq!(topic_route_data.broker_datas[0].broker_name(), &broker_name);
        assert_eq!(
            topic_route_data.broker_datas[0].broker_addrs().get(&MASTER_ID),
            Some(&broker_addr)
        );
    }

    #[tokio::test]
    async fn default_v2_namesrv_metadata_processors_return_java_compatible_bodies() {
        let bootstrap = build_bootstrap_with_default_v2();
        let harness = LocalRequestHarness::new().await.unwrap();
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let broker_addr = CheetahString::from_static_str("10.0.0.10:10911");
        let ha_server_addr = CheetahString::from_static_str("10.0.0.10:10912");
        let zone_name = CheetahString::from_static_str("zone-a");
        let topic_name = CheetahString::from_static_str("metadata-processor-topic");
        let topic_wrapper = topic_config_wrapper(&[(
            "metadata-processor-topic",
            0,
            PermName::PERM_READ | PermName::PERM_WRITE,
        )]);
        let registered_data_version = topic_wrapper.topic_config_serialize_wrapper.data_version.clone();

        let mut register_request = register_broker_request(
            &cluster_name,
            &broker_name,
            &broker_addr,
            MASTER_ID,
            &ha_server_addr,
            &zone_name,
            true,
            topic_wrapper,
            vec![],
        );
        let register_response = process_with_default_processor(&bootstrap, &harness, &mut register_request).await;
        assert_eq!(ResponseCode::from(register_response.code()), ResponseCode::Success);

        let mut cluster_request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo);
        let cluster_response = process_with_default_processor(&bootstrap, &harness, &mut cluster_request).await;
        assert_eq!(ResponseCode::from(cluster_response.code()), ResponseCode::Success);
        let cluster_info = ClusterInfo::decode(cluster_response.body().expect("cluster info should include a body"))
            .expect("cluster info body should decode");
        assert!(cluster_info
            .cluster_addr_table
            .as_ref()
            .and_then(|clusters| clusters.get(&cluster_name))
            .is_some_and(|brokers| brokers.contains(&broker_name)));
        assert_eq!(
            cluster_info
                .broker_addr_table
                .as_ref()
                .and_then(|brokers| brokers.get(&broker_name))
                .and_then(|broker| broker.broker_addrs().get(&MASTER_ID)),
            Some(&broker_addr)
        );

        let mut topic_list_request =
            RemotingCommand::create_remoting_command(RequestCode::GetAllTopicListFromNameserver);
        let topic_list_response = process_with_default_processor(&bootstrap, &harness, &mut topic_list_request).await;
        assert_eq!(ResponseCode::from(topic_list_response.code()), ResponseCode::Success);
        let topic_list = TopicList::decode(topic_list_response.body().expect("topic list should include a body"))
            .expect("topic list body should decode");
        assert!(topic_list.topic_list.contains(&topic_name));

        let mut member_group_request = RemotingCommand::create_request_command(
            RequestCode::GetBrokerMemberGroup,
            GetBrokerMemberGroupRequestHeader::new(cluster_name.clone(), broker_name.clone()),
        );
        member_group_request.make_custom_header_to_net();
        let member_group_response =
            process_with_default_processor(&bootstrap, &harness, &mut member_group_request).await;
        assert_eq!(ResponseCode::from(member_group_response.code()), ResponseCode::Success);
        let member_group_body = GetBrokerMemberGroupResponseBody::decode(
            member_group_response
                .body()
                .expect("member group should include a body"),
        )
        .expect("member group body should decode");
        let member_group = member_group_body
            .broker_member_group
            .expect("registered broker should have member group");
        assert_eq!(member_group.cluster, cluster_name);
        assert_eq!(member_group.broker_name, broker_name);
        assert_eq!(member_group.broker_addrs.get(&MASTER_ID), Some(&broker_addr));

        let mut query_version_request = RemotingCommand::create_request_command(
            RequestCode::QueryDataVersion,
            QueryDataVersionRequestHeader::new(
                broker_name.clone(),
                broker_addr.clone(),
                cluster_name.clone(),
                MASTER_ID,
            ),
        )
        .set_body(registered_data_version.encode().expect("data version should encode"));
        query_version_request.make_custom_header_to_net();
        let query_version_response =
            process_with_default_processor(&bootstrap, &harness, &mut query_version_request).await;
        assert_eq!(ResponseCode::from(query_version_response.code()), ResponseCode::Success);
        let query_version_header = query_version_response
            .read_custom_header_ref::<QueryDataVersionResponseHeader>()
            .expect("query data version should include a response header");
        assert!(!query_version_header.changed());
        let returned_data_version = DataVersion::decode(
            query_version_response
                .body()
                .expect("data version should include a body"),
        )
        .expect("query data version body should decode");
        assert_eq!(returned_data_version, registered_data_version);

        let mut heartbeat_request = RemotingCommand::create_request_command(
            RequestCode::BrokerHeartbeat,
            BrokerHeartbeatRequestHeader {
                cluster_name: cluster_name.clone(),
                broker_addr: broker_addr.clone(),
                broker_name: broker_name.clone(),
                broker_id: Some(MASTER_ID as i64),
                epoch: Some(1),
                max_offset: Some(128),
                confirm_offset: Some(64),
                heartbeat_timeout_mills: Some(30_000),
                election_priority: Some(1),
            },
        );
        heartbeat_request.make_custom_header_to_net();
        let heartbeat_response = process_with_default_processor(&bootstrap, &harness, &mut heartbeat_request).await;
        assert_eq!(ResponseCode::from(heartbeat_response.code()), ResponseCode::Success);
    }

    #[tokio::test]
    async fn default_v2_namesrv_topic_admin_processors_complete_phase2_contracts() {
        let bootstrap = build_bootstrap_with_default_v2();
        let harness = LocalRequestHarness::new().await.unwrap();
        let cluster_name = CheetahString::from_static_str("phase2-cluster");
        let broker_name = CheetahString::from_static_str("phase2-broker");
        let broker_addr = CheetahString::from_static_str("10.0.0.20:10911");
        let ha_server_addr = CheetahString::from_static_str("10.0.0.20:10912");
        let zone_name = CheetahString::from_static_str("phase2-zone");
        let normal_topic = CheetahString::from_static_str("phase2-normal-topic");
        let unit_only_topic = CheetahString::from_static_str("phase2-unit-only-topic");
        let unit_sub_only_topic = CheetahString::from_static_str("phase2-unit-sub-only-topic");
        let unit_and_sub_topic = CheetahString::from_static_str("phase2-unit-and-sub-topic");
        let registered_topic = CheetahString::from_static_str("phase2-registered-topic");

        let mut register_request = register_broker_request(
            &cluster_name,
            &broker_name,
            &broker_addr,
            MASTER_ID,
            &ha_server_addr,
            &zone_name,
            false,
            topic_config_wrapper(&[
                ("phase2-normal-topic", 0, PermName::PERM_READ | PermName::PERM_WRITE),
                (
                    "phase2-unit-only-topic",
                    TopicSysFlag::build_sys_flag(true, false),
                    PermName::PERM_READ | PermName::PERM_WRITE,
                ),
                (
                    "phase2-unit-sub-only-topic",
                    TopicSysFlag::build_sys_flag(false, true),
                    PermName::PERM_READ | PermName::PERM_WRITE,
                ),
                (
                    "phase2-unit-and-sub-topic",
                    TopicSysFlag::build_sys_flag(true, true),
                    PermName::PERM_READ | PermName::PERM_WRITE,
                ),
            ]),
            vec![],
        );
        let register_response = process_with_default_processor(&bootstrap, &harness, &mut register_request).await;
        assert_eq!(ResponseCode::from(register_response.code()), ResponseCode::Success);

        let mut wipe_request = RemotingCommand::create_request_command(
            RequestCode::WipeWritePermOfBroker,
            WipeWritePermOfBrokerRequestHeader::new(broker_name.clone()),
        );
        wipe_request.make_custom_header_to_net();
        let wipe_response = process_with_default_processor(&bootstrap, &harness, &mut wipe_request).await;
        assert_eq!(ResponseCode::from(wipe_response.code()), ResponseCode::Success);
        let wipe_header = wipe_response
            .read_custom_header_ref::<WipeWritePermOfBrokerResponseHeader>()
            .expect("wipe write perm should include a response header");
        assert!(wipe_header.get_wipe_topic_count() >= 4);

        let mut route_request = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new(normal_topic.clone(), Some(true)),
        )
        .set_version(RocketMqVersion::V4_9_3 as i32);
        route_request.make_custom_header_to_net();
        let route_response = process_with_client_processor(&bootstrap, &harness, &mut route_request).await;
        assert_eq!(ResponseCode::from(route_response.code()), ResponseCode::Success);
        let route_after_wipe =
            TopicRouteData::decode(route_response.body().expect("route response should include body"))
                .expect("route response body should decode");
        let queue_after_wipe = route_after_wipe
            .queue_datas
            .iter()
            .find(|queue| queue.broker_name() == &broker_name)
            .expect("broker queue data should exist after wipe");
        assert_ne!(queue_after_wipe.perm & PermName::PERM_READ, 0);
        assert_eq!(queue_after_wipe.perm & PermName::PERM_WRITE, 0);

        let mut add_request = RemotingCommand::create_request_command(
            RequestCode::AddWritePermOfBroker,
            AddWritePermOfBrokerRequestHeader::new(broker_name.clone()),
        );
        add_request.make_custom_header_to_net();
        let add_response = process_with_default_processor(&bootstrap, &harness, &mut add_request).await;
        assert_eq!(ResponseCode::from(add_response.code()), ResponseCode::Success);
        let add_header = add_response
            .read_custom_header_ref::<AddWritePermOfBrokerResponseHeader>()
            .expect("add write perm should include a response header");
        assert!(add_header.get_add_topic_count() >= 4);

        let mut route_request = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new(normal_topic.clone(), Some(true)),
        )
        .set_version(RocketMqVersion::V4_9_3 as i32);
        route_request.make_custom_header_to_net();
        let route_response = process_with_client_processor(&bootstrap, &harness, &mut route_request).await;
        assert_eq!(ResponseCode::from(route_response.code()), ResponseCode::Success);
        let route_after_add =
            TopicRouteData::decode(route_response.body().expect("route response should include body"))
                .expect("route response body should decode");
        let queue_after_add = route_after_add
            .queue_datas
            .iter()
            .find(|queue| queue.broker_name() == &broker_name)
            .expect("broker queue data should exist after add");
        assert_ne!(queue_after_add.perm & PermName::PERM_WRITE, 0);

        let mut topics_by_cluster_request = RemotingCommand::create_request_command(
            RequestCode::GetTopicsByCluster,
            GetTopicsByClusterRequestHeader::new(cluster_name.clone()),
        );
        topics_by_cluster_request.make_custom_header_to_net();
        let topics_by_cluster_response =
            process_with_default_processor(&bootstrap, &harness, &mut topics_by_cluster_request).await;
        assert_eq!(
            ResponseCode::from(topics_by_cluster_response.code()),
            ResponseCode::Success
        );
        let topics_by_cluster = TopicList::decode(
            topics_by_cluster_response
                .body()
                .expect("topics by cluster should include a body"),
        )
        .expect("topics by cluster body should decode");
        for expected in [
            &normal_topic,
            &unit_only_topic,
            &unit_sub_only_topic,
            &unit_and_sub_topic,
        ] {
            assert!(topics_by_cluster.topic_list.contains(expected));
        }

        let mut system_topics_request = RemotingCommand::create_remoting_command(RequestCode::GetSystemTopicListFromNs);
        let system_topics_response =
            process_with_default_processor(&bootstrap, &harness, &mut system_topics_request).await;
        assert_eq!(ResponseCode::from(system_topics_response.code()), ResponseCode::Success);
        let system_topics = TopicList::decode(
            system_topics_response
                .body()
                .expect("system topics should include a body"),
        )
        .expect("system topics body should decode");
        assert!(system_topics.topic_list.contains(&cluster_name));
        assert!(system_topics.topic_list.contains(&broker_name));
        assert_eq!(system_topics.broker_addr.as_ref(), Some(&broker_addr));

        let mut unit_topics_request = RemotingCommand::create_remoting_command(RequestCode::GetUnitTopicList);
        let unit_topics_response = process_with_default_processor(&bootstrap, &harness, &mut unit_topics_request).await;
        assert_eq!(ResponseCode::from(unit_topics_response.code()), ResponseCode::Success);
        let unit_topics = TopicList::decode(unit_topics_response.body().expect("unit topics should include a body"))
            .expect("unit topics body should decode");
        assert!(unit_topics.topic_list.contains(&unit_only_topic));
        assert!(unit_topics.topic_list.contains(&unit_and_sub_topic));
        assert!(!unit_topics.topic_list.contains(&unit_sub_only_topic));

        let mut unit_sub_topics_request = RemotingCommand::create_remoting_command(RequestCode::GetHasUnitSubTopicList);
        let unit_sub_topics_response =
            process_with_default_processor(&bootstrap, &harness, &mut unit_sub_topics_request).await;
        assert_eq!(
            ResponseCode::from(unit_sub_topics_response.code()),
            ResponseCode::Success
        );
        let unit_sub_topics = TopicList::decode(
            unit_sub_topics_response
                .body()
                .expect("unit sub topics should include a body"),
        )
        .expect("unit sub topics body should decode");
        assert!(unit_sub_topics.topic_list.contains(&unit_sub_only_topic));
        assert!(unit_sub_topics.topic_list.contains(&unit_and_sub_topic));

        let mut unit_sub_ununit_topics_request =
            RemotingCommand::create_remoting_command(RequestCode::GetHasUnitSubUnunitTopicList);
        let unit_sub_ununit_topics_response =
            process_with_default_processor(&bootstrap, &harness, &mut unit_sub_ununit_topics_request).await;
        assert_eq!(
            ResponseCode::from(unit_sub_ununit_topics_response.code()),
            ResponseCode::Success
        );
        let unit_sub_ununit_topics = TopicList::decode(
            unit_sub_ununit_topics_response
                .body()
                .expect("unit sub ununit topics should include a body"),
        )
        .expect("unit sub ununit topics body should decode");
        assert!(unit_sub_ununit_topics.topic_list.contains(&unit_sub_only_topic));
        assert!(!unit_sub_ununit_topics.topic_list.contains(&unit_and_sub_topic));

        let registered_route = TopicRouteData {
            order_topic_conf: None,
            queue_datas: vec![QueueData::new(
                broker_name.clone(),
                2,
                2,
                PermName::PERM_READ | PermName::PERM_WRITE,
                0,
            )],
            broker_datas: vec![],
            filter_server_table: HashMap::new(),
            topic_queue_mapping_by_broker: None,
        };
        let mut register_topic_request = RemotingCommand::create_request_command(
            RequestCode::RegisterTopicInNamesrv,
            RegisterTopicRequestHeader::new(registered_topic.clone()),
        )
        .set_body(registered_route.encode().expect("topic route data should encode"));
        register_topic_request.make_custom_header_to_net();
        let register_topic_response =
            process_with_default_processor(&bootstrap, &harness, &mut register_topic_request).await;
        assert_eq!(
            ResponseCode::from(register_topic_response.code()),
            ResponseCode::Success
        );
        assert!(
            bootstrap
                .name_server_runtime
                .inner
                .route_info_manager()
                .pickup_topic_route_data(&registered_topic)
                .is_some(),
            "registered topic should be visible through route manager"
        );

        let mut delete_topic_request = RemotingCommand::create_request_command(
            RequestCode::DeleteTopicInNamesrv,
            DeleteTopicFromNamesrvRequestHeader::new(registered_topic.clone(), Some(cluster_name.clone())),
        );
        delete_topic_request.make_custom_header_to_net();
        let delete_topic_response =
            process_with_default_processor(&bootstrap, &harness, &mut delete_topic_request).await;
        assert_eq!(ResponseCode::from(delete_topic_response.code()), ResponseCode::Success);
        assert!(
            bootstrap
                .name_server_runtime
                .inner
                .route_info_manager()
                .pickup_topic_route_data(&registered_topic)
                .is_none(),
            "deleted topic should no longer have route data"
        );
    }

    #[tokio::test]
    async fn default_v2_register_broker_via_default_processor_populates_route_contract() {
        let bootstrap = build_bootstrap_with_default_v2();
        let harness = LocalRequestHarness::new().await.unwrap();
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let broker_addr = CheetahString::from_static_str("10.0.0.10:10911");
        let ha_server_addr = CheetahString::from_static_str("10.0.0.10:10912");
        let zone_name = CheetahString::from_static_str("zone-a");
        let filter_server_addr = CheetahString::from_static_str("10.0.0.10:12000");
        let topic_name = CheetahString::from_static_str("register-processor-topic");

        let mut request = register_broker_request(
            &cluster_name,
            &broker_name,
            &broker_addr,
            MASTER_ID,
            &ha_server_addr,
            &zone_name,
            true,
            topic_config_wrapper(&[(
                "register-processor-topic",
                0,
                PermName::PERM_READ | PermName::PERM_WRITE,
            )]),
            vec![filter_server_addr.clone()],
        );

        let response = process_with_default_processor(&bootstrap, &harness, &mut request).await;

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let response_header = response
            .read_custom_header_ref::<RegisterBrokerResponseHeader>()
            .expect("register broker should include a response header");
        assert_eq!(
            response_header.master_addr.as_ref().map(|value| value.as_str()),
            Some("")
        );
        assert_eq!(
            response_header.ha_server_addr.as_ref().map(|value| value.as_str()),
            Some("")
        );

        let cluster_info = bootstrap
            .name_server_runtime
            .inner
            .route_info_manager()
            .get_all_cluster_info();
        assert!(cluster_info
            .cluster_addr_table
            .as_ref()
            .and_then(|clusters| clusters.get(&cluster_name))
            .is_some_and(|broker_names| broker_names.contains(&broker_name)));

        let mut route_request = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new(topic_name.clone(), Some(true)),
        )
        .set_version(RocketMqVersion::V4_9_3 as i32);
        route_request.make_custom_header_to_net();

        let route_response = process_with_client_processor(&bootstrap, &harness, &mut route_request).await;
        assert_eq!(ResponseCode::from(route_response.code()), ResponseCode::Success);

        let topic_route_data =
            TopicRouteData::decode(route_response.body().expect("route response should include body"))
                .expect("route response body should decode");
        assert_eq!(topic_route_data.queue_datas.len(), 1);
        assert_eq!(topic_route_data.broker_datas.len(), 1);
        assert_eq!(topic_route_data.broker_datas[0].broker_name(), &broker_name);
        assert_eq!(
            topic_route_data.broker_datas[0].broker_addrs().get(&MASTER_ID),
            Some(&broker_addr)
        );
        assert_eq!(topic_route_data.broker_datas[0].zone_name(), Some(&zone_name));
        assert_eq!(
            topic_route_data.filter_server_table.get(&broker_addr),
            Some(&vec![filter_server_addr])
        );
    }

    #[tokio::test]
    async fn default_v2_unregister_broker_via_default_processor_removes_route_contract() {
        let bootstrap = build_bootstrap_with_default_v2();
        let harness = LocalRequestHarness::new().await.unwrap();
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let broker_addr = CheetahString::from_static_str("10.0.0.10:10911");
        let ha_server_addr = CheetahString::from_static_str("10.0.0.10:10912");
        let zone_name = CheetahString::from_static_str("zone-a");
        let topic_name = CheetahString::from_static_str("unregister-processor-topic");

        let mut register_request = register_broker_request(
            &cluster_name,
            &broker_name,
            &broker_addr,
            MASTER_ID,
            &ha_server_addr,
            &zone_name,
            false,
            topic_config_wrapper(&[(
                "unregister-processor-topic",
                0,
                PermName::PERM_READ | PermName::PERM_WRITE,
            )]),
            vec![],
        );

        let register_response = process_with_default_processor(&bootstrap, &harness, &mut register_request).await;
        assert_eq!(ResponseCode::from(register_response.code()), ResponseCode::Success);
        assert!(
            bootstrap
                .name_server_runtime
                .inner
                .route_info_manager()
                .pickup_topic_route_data(&topic_name)
                .is_some(),
            "registered topic route should exist before unregister"
        );

        start_unregister_service(&bootstrap);

        let mut unregister_request = RemotingCommand::create_request_command(
            RequestCode::UnregisterBroker,
            UnRegisterBrokerRequestHeader::new(
                broker_name.clone(),
                broker_addr.clone(),
                cluster_name.clone(),
                MASTER_ID,
            ),
        );
        unregister_request.make_custom_header_to_net();

        let unregister_response = process_with_default_processor(&bootstrap, &harness, &mut unregister_request).await;
        assert_eq!(ResponseCode::from(unregister_response.code()), ResponseCode::Success);

        wait_until("processor unregister broker route cleanup", || {
            bootstrap
                .name_server_runtime
                .inner
                .route_info_manager()
                .pickup_topic_route_data(&topic_name)
                .is_none()
        })
        .await;
        shutdown_unregister_service(&bootstrap).await;
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
        let bootstrap = build_bootstrap_with_default_v2();
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
            .route_info_manager()
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

        shutdown_unregister_service(&bootstrap).await;
    }

    #[tokio::test]
    async fn default_v2_scan_not_active_broker_closes_expired_connection_before_batch_unregister() {
        let bootstrap = build_bootstrap_with_default_v2();
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
            .route_info_manager()
            .scan_not_active_broker();
        assert_eq!(expired_count, 1);

        wait_until("expired broker connection close", || {
            harness.channel().connection_ref().state() == ConnectionState::Closed
        })
        .await;

        shutdown_unregister_service(&bootstrap).await;
    }

    #[tokio::test]
    async fn default_v2_connection_disconnected_by_socket_addr_matches_channel_destroy_cleanup() {
        let bootstrap = build_bootstrap_with_default_v2();
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
            .route_info_manager()
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

        shutdown_unregister_service(&bootstrap).await;
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

        shutdown_unregister_service(&bootstrap).await;
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

        shutdown_unregister_service(&bootstrap).await;
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

        shutdown_unregister_service(&bootstrap).await;
    }

    #[tokio::test]
    async fn boot_supports_cluster_test_mode() {
        let runtime = RuntimeContext::from_current("namesrv-cluster-test-mode");
        let namesrv_config = NamesrvConfig {
            cluster_test: true,
            ..NamesrvConfig::default()
        };
        let bootstrap = Builder::new()
            .set_name_server_config(namesrv_config)
            .set_server_config(namesrv_server_config())
            .set_service_context(runtime.service_context("namesrv"))
            .build();

        bootstrap
            .boot_with_shutdown(async {})
            .await
            .expect("cluster test mode should boot and shut down cleanly once implemented");
        runtime
            .shutdown_tasks(Duration::from_secs(1))
            .await
            .assert_no_task_leak()
            .unwrap();
    }

    #[tokio::test]
    async fn boot_supports_enable_controller_in_namesrv_mode() {
        let namesrv_config = NamesrvConfig {
            enable_controller_in_namesrv: true,
            ..NamesrvConfig::default()
        };
        let bootstrap = Builder::new()
            .set_name_server_config(namesrv_config)
            .set_server_config(namesrv_server_config())
            .set_controller_config(embedded_controller_config())
            .build();

        bootstrap
            .boot_with_shutdown(async {})
            .await
            .expect("controller-in-namesrv mode should boot and shut down cleanly once implemented");
    }

    #[tokio::test]
    async fn boot_shutdown_report_includes_remoting_server_report() {
        let bootstrap = Builder::new().set_server_config(namesrv_server_config()).build();

        let report = bootstrap
            .boot_with_shutdown_report(async {})
            .await
            .expect("namesrv should boot and return shutdown report");

        assert!(report.is_healthy(), "{report:?}");
        let route_report = report
            .route_unregistration
            .as_ref()
            .expect("namesrv report should include route unregistration report");
        assert!(route_report.is_healthy(), "{}", route_report.to_json());
        assert_eq!(route_report.leaked, 0, "{}", route_report.to_json());
        assert!(report.server.is_some(), "{report:?}");
        let remoting_report = report
            .remoting_server
            .as_ref()
            .expect("namesrv report should include remoting server report");
        assert!(remoting_report.is_healthy(), "{}", remoting_report.to_json());
        assert_eq!(remoting_report.leaked, 0, "{}", remoting_report.to_json());
    }

    #[tokio::test]
    async fn boot_shutdown_is_healthy_with_connection_waiting_for_first_byte() {
        let server_config = namesrv_server_config();
        let addr = format!("127.0.0.1:{}", server_config.listen_port);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let bootstrap = Builder::new().set_server_config(server_config).build();
        let server_task = tokio::spawn(async move {
            bootstrap
                .boot_with_shutdown_report(async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let client = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match TokioTcpStream::connect(&addr).await {
                    Ok(stream) => break stream,
                    Err(_) => sleep(Duration::from_millis(10)).await,
                }
            }
        })
        .await
        .expect("namesrv should accept TCP connections before timeout");
        sleep(Duration::from_millis(50)).await;

        let _ = shutdown_tx.send(());
        let report = tokio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("namesrv should shut down before the server task join timeout")
            .expect("namesrv task should not panic")
            .expect("namesrv should return shutdown report");
        drop(client);

        assert!(report.is_healthy(), "{report:?}");
        assert!(
            report.server.as_ref().is_some_and(ShutdownReport::is_healthy),
            "server shutdown report should be present and healthy: {report:?}"
        );
        assert!(
            report.remoting_server.as_ref().is_some_and(ShutdownReport::is_healthy),
            "remoting server shutdown report should be present and healthy: {report:?}"
        );
    }

    #[tokio::test]
    async fn enable_controller_in_namesrv_rejects_conflicting_listen_addr() {
        let server_config = namesrv_server_config();
        let conflicting_controller = ControllerConfig::default()
            .with_node_info(1, format!("127.0.0.1:{}", server_config.listen_port).parse().unwrap());
        let namesrv_config = NamesrvConfig {
            enable_controller_in_namesrv: true,
            ..NamesrvConfig::default()
        };
        let mut bootstrap = Builder::new()
            .set_name_server_config(namesrv_config)
            .set_server_config(server_config)
            .set_controller_config(conflicting_controller)
            .build();

        let error = bootstrap
            .name_server_runtime
            .initialize()
            .await
            .expect_err("embedded controller should reject conflicting listen addresses");

        assert!(error.to_string().contains("conflicts with namesrv address"));
    }

    #[tokio::test]
    async fn enable_controller_in_namesrv_lifecycle_matches_namesrv_runtime() {
        let namesrv_config = NamesrvConfig {
            enable_controller_in_namesrv: true,
            ..NamesrvConfig::default()
        };
        let mut bootstrap = Builder::new()
            .set_name_server_config(namesrv_config)
            .set_server_config(namesrv_server_config())
            .set_controller_config(embedded_controller_config())
            .build();

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        bootstrap.name_server_runtime.shutdown_tx = Some(shutdown_tx.clone());
        bootstrap.name_server_runtime.shutdown_rx = Some(shutdown_rx);
        bootstrap
            .name_server_runtime
            .initialize()
            .await
            .expect("runtime initialize should succeed");

        let controller_manager = bootstrap
            .name_server_runtime
            .inner
            .controller_manager()
            .expect("embedded controller should be initialized");
        assert!(controller_manager.is_initialized());
        assert!(!controller_manager.is_running());

        let mut runtime = bootstrap.name_server_runtime;
        let start_handle = tokio::spawn(async move { runtime.start().await });
        wait_until("embedded controller to start", || controller_manager.is_running()).await;

        shutdown_tx
            .send(())
            .expect("shutdown broadcast should reach the running runtime");

        start_handle
            .await
            .expect("runtime task should not panic")
            .expect("runtime start should exit cleanly");

        assert!(!controller_manager.is_running());
    }

    #[tokio::test]
    async fn default_v2_unsupported_request_code_returns_request_code_not_supported() {
        let bootstrap = build_bootstrap_with_default_v2();
        let harness = LocalRequestHarness::new().await.unwrap();
        let processor =
            DefaultRequestProcessor::new(NameServerRuntimeHandle::new(&bootstrap.name_server_runtime.inner));
        let mut request = RemotingCommand::create_remoting_command(RequestCode::SendMessage);

        let response = processor
            .process_request_inner(harness.channel(), RequestCode::SendMessage, &mut request)
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
            ..ServerConfig::default()
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
        assert_eq!(
            properties.get("tls.server.mode").map(|value| value.as_str()),
            Some("permissive")
        );
        assert_eq!(
            properties.get("tls.client.authServer").map(|value| value.as_str()),
            Some("true")
        );
    }

    #[tokio::test]
    async fn default_v2_update_namesrv_config_updates_aggregate_known_keys_and_ignores_unknown() {
        let bootstrap = build_bootstrap_with_default_v2();
        let harness = LocalRequestHarness::new().await.unwrap();
        let mut request = RemotingCommand::create_remoting_command(RequestCode::UpdateNamesrvConfig).set_body(
            b"listenPort=19876\nbindAddress=127.0.0.2\nclientWorkerThreads=9\nenableTopicList=false\ntls.server.mode=enforcing\ntls.server.certPath=/certs/server.pem\nunknownKey=42"
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
        assert_eq!(
            bootstrap
                .name_server_runtime
                .inner
                .server_config()
                .tls_config
                .server
                .mode,
            TlsMode::Enforcing
        );
        assert_eq!(
            bootstrap
                .name_server_runtime
                .inner
                .server_config()
                .tls_config
                .server
                .cert_path
                .as_deref(),
            Some("/certs/server.pem")
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

    #[tokio::test]
    async fn default_v2_kvconfig_crud_roundtrip_via_default_processor() {
        let bootstrap = build_bootstrap_with_default_v2();
        let harness = LocalRequestHarness::new().await.unwrap();
        let namespace = CheetahString::from_static_str("phase5-namespace");
        let key = CheetahString::from_static_str("phase5-key");
        let value = CheetahString::from_static_str("phase5-value");

        let mut put_request = RemotingCommand::create_request_command(
            RequestCode::PutKvConfig,
            PutKVConfigRequestHeader::new(namespace.clone(), key.clone(), value.clone()),
        );
        put_request.make_custom_header_to_net();
        let put_response = process_with_default_processor(&bootstrap, &harness, &mut put_request).await;
        assert_eq!(ResponseCode::from(put_response.code()), ResponseCode::Success);

        let mut get_request = RemotingCommand::create_request_command(
            RequestCode::GetKvConfig,
            GetKVConfigRequestHeader::new(namespace.clone(), key.clone()),
        );
        get_request.make_custom_header_to_net();
        let get_response = process_with_default_processor(&bootstrap, &harness, &mut get_request).await;
        assert_eq!(ResponseCode::from(get_response.code()), ResponseCode::Success);
        let get_header = get_response
            .read_custom_header_ref::<GetKVConfigResponseHeader>()
            .expect("get kv config should include a response header");
        assert_eq!(get_header.value.as_ref(), Some(&value));

        let mut list_request = RemotingCommand::create_request_command(
            RequestCode::GetKvlistByNamespace,
            GetKVListByNamespaceRequestHeader::new(namespace.clone()),
        );
        list_request.make_custom_header_to_net();
        let list_response = process_with_default_processor(&bootstrap, &harness, &mut list_request).await;
        assert_eq!(ResponseCode::from(list_response.code()), ResponseCode::Success);
        let kv_table = KVTable::decode(list_response.body().expect("list response should include a body")).unwrap();
        assert_eq!(kv_table.table.get(&key), Some(&value));

        let mut delete_request = RemotingCommand::create_request_command(
            RequestCode::DeleteKvConfig,
            DeleteKVConfigRequestHeader::new(namespace.clone(), key.clone()),
        );
        delete_request.make_custom_header_to_net();
        let delete_response = process_with_default_processor(&bootstrap, &harness, &mut delete_request).await;
        assert_eq!(ResponseCode::from(delete_response.code()), ResponseCode::Success);

        let mut get_after_delete_request = RemotingCommand::create_request_command(
            RequestCode::GetKvConfig,
            GetKVConfigRequestHeader::new(namespace, key),
        );
        get_after_delete_request.make_custom_header_to_net();
        let get_after_delete_response =
            process_with_default_processor(&bootstrap, &harness, &mut get_after_delete_request).await;
        assert_eq!(
            ResponseCode::from(get_after_delete_response.code()),
            ResponseCode::QueryNotFound
        );
    }

    #[tokio::test]
    async fn default_v2_route_query_via_client_processor_returns_order_config_and_route_contract() {
        let bootstrap = build_bootstrap_with_v2_config(NamesrvConfig {
            order_message_enable: true,
            ..NamesrvConfig::default()
        });
        let harness = LocalRequestHarness::new().await.unwrap();
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let primary_broker_addr = CheetahString::from_static_str("10.0.0.10:10911");
        let zone_name = CheetahString::from_static_str("zone-a");
        let topic_name = CheetahString::from_static_str("route-query-contract-topic");
        let order_conf = CheetahString::from_static_str("broker-a:4");

        register_test_broker(
            &bootstrap,
            &cluster_name,
            &broker_name,
            &primary_broker_addr,
            MASTER_ID,
            &zone_name,
            false,
            topic_config_wrapper(&[(
                "route-query-contract-topic",
                0,
                PermName::PERM_READ | PermName::PERM_WRITE,
            )]),
        )
        .await;

        bootstrap
            .name_server_runtime
            .inner
            .kvconfig_manager()
            .put_kv_config(
                CheetahString::from_static_str("ORDER_TOPIC_CONFIG"),
                topic_name.clone(),
                order_conf.clone(),
            )
            .unwrap();

        let mut route_request = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new(topic_name.clone(), Some(true)),
        )
        .set_version(RocketMqVersion::V4_9_3 as i32);
        route_request.make_custom_header_to_net();

        let route_response = process_with_client_processor(&bootstrap, &harness, &mut route_request).await;
        assert_eq!(ResponseCode::from(route_response.code()), ResponseCode::Success);

        let body = route_response.body().expect("route response should include a body");
        let topic_route_data = TopicRouteData::decode(body).unwrap();
        assert_eq!(topic_route_data.order_topic_conf.as_ref(), Some(&order_conf));
        assert_eq!(topic_route_data.queue_datas.len(), 1);
        assert_eq!(topic_route_data.broker_datas.len(), 1);

        let broker_data = &topic_route_data.broker_datas[0];
        assert_eq!(broker_data.broker_name(), &broker_name);
        assert_eq!(broker_data.broker_addrs().get(&MASTER_ID), Some(&primary_broker_addr));
        assert_eq!(topic_route_data.queue_datas[0].broker_name(), &broker_name);
    }
}
