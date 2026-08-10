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
#[cfg(feature = "embedded-controller")]
use std::net::IpAddr;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;
use std::time::Duration;
use std::time::Instant;

use arc_swap::ArcSwap;
use cheetah_string::CheetahString;
use rocketmq_auth::AuthRuntime;
use rocketmq_auth::AuthRuntimeBuilder;
#[cfg(feature = "embedded-controller")]
use rocketmq_controller::ControllerConfig;
#[cfg(feature = "embedded-controller")]
use rocketmq_controller::ControllerManager;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::UnifiedServiceError;
use rocketmq_observability::metrics::namesrv::NameServerMetrics;
use rocketmq_observability::TelemetryHandle;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_runtime::wait_for_signal;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::MetadataDeadline;
use rocketmq_runtime::MetadataIoActor;
use rocketmq_runtime::MetadataIoConfig;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ServiceLifecycle;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReason;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_security_api::Principal;
use rocketmq_transport::api::v1::ChannelEventListener;
#[cfg(test)]
use rocketmq_transport::api::v1::ClientShutdownReport;
use rocketmq_transport::api::v1::DefaultRequestProcessor;
use rocketmq_transport::api::v1::NetworkUtil;
use rocketmq_transport::api::v1::RemotingClient;
use rocketmq_transport::api::v1::ServerConfig;
use rocketmq_transport::api::v1::TransportClientConfig;
use rocketmq_transport::api::v1::TransportSecurity;
use rocketmq_transport::api::v1::TransportServer;
use rocketmq_transport::api::v1::TransportTelemetry;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::warn;

use crate::processor::workload_admission::NameServerWorkloadAdmission;
use crate::processor::ClientRequestProcessor;
use crate::processor::ClusterTestRequestProcessor;
use crate::processor::ClusterTestRouteLookup;
use crate::processor::NameServerRequestProcessor;
use crate::processor::NameServerRequestProcessorWrapper;
use crate::processor::TransportClusterTestRouteLookup;
use crate::route::response_cache::RouteResponseCache;
use crate::route::route_info_manager::RouteInfoManager;
use crate::route::zone_route_rpc_hook::ZoneRouteRPCHook;
use crate::route_info::broker_housekeeping_service::BrokerHousekeepingService;
use crate::KVConfigManager;
use crate::NamesrvConfig;

use self::config_apply::ConfigApplyOutcome;
use self::config_apply::ConfigGenerationState;

pub(crate) mod config_apply;
mod lifecycle;
mod signals;

pub(crate) use lifecycle::InFlightRequestGuard;
pub(crate) use lifecycle::InFlightRequestTracker;
pub use lifecycle::NameServerInFlightDrainReport;
pub use lifecycle::NameServerShutdownReport;
use lifecycle::RuntimeState;

pub struct NameServerBootstrap {
    name_server_runtime: NameServerRuntime,
}

#[derive(Default)]
struct NameServerStartupJournal {
    shutdown_relay: Option<TaskGroup>,
}

/// Builder for creating NameServerBootstrap with custom configuration
pub struct Builder {
    name_server_config: Option<NamesrvConfig>,
    server_config: Option<ServerConfig>,
    tokio_client_config: Option<TransportClientConfig>,
    #[cfg(feature = "embedded-controller")]
    controller_config: Option<ControllerConfig>,
    cluster_test_route_lookup: Option<Arc<dyn ClusterTestRouteLookup>>,
    transport_security: Option<Arc<TransportSecurity>>,
    transport_principal: Option<Principal>,
    telemetry: TelemetryHandle,
    service_context: ChildServiceContext,
}

/// Core runtime managing NameServer lifecycle and operations
///
/// Coordinates initialization, startup, and graceful shutdown of all components.
struct NameServerRuntime {
    inner: Arc<NameServerRuntimeInner>,
    scheduled_tasks: Option<ScheduledTaskGroup>,
    shutdown_tx: Option<watch::Sender<bool>>,
    shutdown_rx: Option<watch::Receiver<bool>>,
    server_inner: Option<TransportServer<NameServerRequestProcessor>>,
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

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        self.name_server_runtime.shutdown_tx = Some(shutdown_tx.clone());
        self.name_server_runtime.shutdown_rx = Some(shutdown_rx);
        let mut startup_journal = NameServerStartupJournal::default();

        if let Err(primary_error) = self.name_server_runtime.initialize().await {
            let primary_error = self
                .rollback_startup(primary_error, lifecycle.as_ref(), &mut startup_journal)
                .await;
            return Err(primary_error);
        }

        let relay_group = self
            .name_server_runtime
            .inner
            .component_task_group("namesrv.shutdown-relay");
        startup_journal.shutdown_relay = Some(relay_group);

        let relay_spawn_result = match startup_journal.shutdown_relay.as_ref() {
            Some(relay_group) => {
                let relay_cancellation = relay_group.cancellation_token();
                relay_group
                    .spawn_service(
                        "namesrv.shutdown-relay",
                        signals::relay(shutdown_tx, shutdown_signal, relay_cancellation),
                    )
                    .map_err(|error| namesrv_startup_failed("spawn shutdown relay", error))
            }
            None => Err(namesrv_task_group_unavailable("spawn shutdown relay")),
        };
        if let Err(primary_error) = relay_spawn_result {
            let primary_error = self
                .rollback_startup(primary_error, lifecycle.as_ref(), &mut startup_journal)
                .await;
            return Err(primary_error);
        }

        let mut shutdown_report = match self
            .name_server_runtime
            .start_with_shutdown_report(lifecycle.as_ref())
            .await
        {
            Ok(shutdown_report) => shutdown_report,
            Err(primary_error) => {
                let primary_error = self
                    .rollback_startup(primary_error, lifecycle.as_ref(), &mut startup_journal)
                    .await;
                return Err(primary_error);
            }
        };
        let relay_deadline = lifecycle
            .as_ref()
            .and_then(ServiceLifecycle::shutdown_request)
            .map(|request| request.deadline)
            .unwrap_or_else(|| ShutdownDeadline::after(Duration::from_secs(5)));
        shutdown_report.shutdown_relay = shutdown_startup_relay_until(&mut startup_journal, relay_deadline).await;
        if let Some(report) = shutdown_report
            .shutdown_relay
            .as_ref()
            .filter(|report| !report.is_healthy())
        {
            if let Err(error) = report.assert_no_task_leak() {
                warn!("NameServer shutdown relay task group stopped with report: {error}");
            }
            if let Some(lifecycle) = lifecycle.as_ref() {
                lifecycle.mark_failed();
                lifecycle.request_shutdown(ShutdownReason::Internal);
            }
        }
        if let Some(lifecycle) = lifecycle {
            if !shutdown_report.is_healthy() {
                lifecycle.mark_failed();
            }
        }

        info!("NameServer shutdown completed");
        Ok(shutdown_report)
    }

    async fn rollback_startup(
        &mut self,
        primary_error: RocketMQError,
        lifecycle: Option<&ServiceLifecycle>,
        startup_journal: &mut NameServerStartupJournal,
    ) -> RocketMQError {
        let deadline = lifecycle.map_or_else(
            || ShutdownDeadline::after(Duration::from_secs(30)),
            |lifecycle| {
                lifecycle.mark_failed();
                lifecycle.request_shutdown(ShutdownReason::Internal).deadline
            },
        );

        if let Some(shutdown_tx) = self.name_server_runtime.shutdown_tx.as_ref() {
            let _ = shutdown_tx.send(true);
        }

        let relay_report = shutdown_startup_relay_until(startup_journal, deadline).await;
        let cleanup_report = self.name_server_runtime.shutdown_until(deadline).await;

        if let Some(relay_report) = relay_report.as_ref().filter(|report| !report.is_healthy()) {
            warn!(
                primary_error = %primary_error,
                relay_cleanup = %relay_report.to_json(),
                "NameServer startup failed and shutdown relay cleanup was unhealthy"
            );
        }
        if !cleanup_report.is_healthy() {
            warn!(
                primary_error = %primary_error,
                runtime_cleanup = ?cleanup_report,
                "NameServer startup failed and runtime cleanup was unhealthy"
            );
        }

        primary_error
    }
}

async fn shutdown_startup_relay_until(
    startup_journal: &mut NameServerStartupJournal,
    deadline: ShutdownDeadline,
) -> Option<ShutdownReport> {
    let relay_group = startup_journal.shutdown_relay.take()?;
    Some(relay_group.shutdown_until(deadline).await)
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

        info!("Phase 1/5: Loading configuration...");
        if let Err(e) = self.load_config().await {
            error!("Initialization failed during config load: {}", e);
            return Err(e);
        }

        info!("Phase 2/5: Initializing authentication and authorization...");
        self.initialize_auth_runtime().await?;

        info!("Phase 3/5: Initializing network server...");
        self.initialize_network_components();

        info!("Phase 4/5: Registering RPC hooks...");
        self.initialize_rpc_hooks();

        info!("Phase 5/5: Starting scheduled tasks...");
        self.start_schedule_service()?;

        // Transition to Initialized state
        self.transition_to(RuntimeState::Initialized)?;

        info!("Initialization completed successfully");
        Ok(())
    }

    fn validate_runtime_config(&self) -> RocketMQResult<()> {
        let namesrv_config = self.inner.name_server_config();
        namesrv_config.validate_domains()?;

        #[cfg(not(feature = "embedded-controller"))]
        if namesrv_config.enable_controller_in_namesrv {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "enableControllerInNamesrv",
                value: namesrv_config.enable_controller_in_namesrv.to_string(),
                reason: "the NameServer binary was compiled without the `embedded-controller` feature".to_string(),
            });
        }

        #[cfg(feature = "embedded-controller")]
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
                reason: "cluster-test route lookup requires an injected ChildServiceContext owner".to_string(),
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

        #[cfg(feature = "embedded-controller")]
        if self.inner.name_server_config().enable_controller_in_namesrv {
            let controller_config = self
                .inner
                .controller_config()
                .expect("controller config should exist when embedded controller is enabled");
            let controller_context = self
                .inner
                .service_context
                .as_ref()
                .expect("NameServerRuntime always has an injected ChildServiceContext")
                .component("namesrv.embedded-controller");
            let controller_manager = Arc::new(
                ControllerManager::new(
                    (*controller_config).clone(),
                    controller_context,
                    self.inner.telemetry.clone(),
                )
                .await?,
            );
            self.inner.install_controller_manager(Arc::clone(&controller_manager))?;
            let initialized = controller_manager.initialize().await?;
            if !initialized {
                return Err(namesrv_startup_failed(
                    "initialize embedded controller",
                    "controller manager initialization returned false",
                ));
            }
            debug!("Embedded controller initialized successfully");
        }
        Ok(())
    }

    async fn initialize_auth_runtime(&self) -> RocketMQResult<()> {
        let namesrv_config = self.inner.name_server_config();
        let mut auth_config = namesrv_config.auth_config.clone();
        if !auth_config.authentication_enabled && !auth_config.authorization_enabled {
            return Ok(());
        }
        if auth_config.config_name.trim().is_empty() {
            auth_config.config_name = CheetahString::from_static_str("namesrv");
        }
        if auth_config.cluster_name.trim().is_empty() {
            auth_config.cluster_name = CheetahString::from_string(namesrv_config.product_env_name.clone());
        }
        let service_context = self
            .inner
            .service_context
            .as_ref()
            .expect("NameServerRuntime always has an injected ChildServiceContext")
            .component("namesrv.auth");
        let mut builder = AuthRuntimeBuilder::new(auth_config, service_context);
        if let Some(Ok(metadata_io)) = self.inner.config_metadata_io.as_ref() {
            builder = builder.with_metadata_io_actor(metadata_io.clone());
        }
        let auth_runtime = Arc::new(builder.build().await?);
        self.inner
            .auth_runtime
            .set(auth_runtime)
            .map_err(|_| namesrv_runtime_state_error("NameServer auth runtime is already initialized"))?;
        Ok(())
    }

    /// Initialize network server for handling client requests
    fn initialize_network_components(&mut self) {
        let config = self.inner.server_config();
        let context = self
            .inner
            .service_context
            .as_ref()
            .expect("NameServerRuntime always has an injected ChildServiceContext");
        let mut server = TransportServer::new_with_telemetry(
            config,
            context.component("namesrv.remoting-server"),
            self.inner.transport_telemetry.clone(),
        );
        if let Some(transport_security) = &self.inner.transport_security {
            server =
                server.with_transport_security(Arc::clone(transport_security), self.inner.transport_principal.clone());
        }
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
        let task_group = self.inner.component_task_group("namesrv.scheduled");
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
            server.register_rpc_hook(Arc::new(ZoneRouteRPCHook::new(self.inner.namesrv_metrics())));
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
        let server_task_group = self.inner.component_task_group("namesrv.server");
        let (server_report_tx, server_report_rx) = oneshot::channel();
        let (server_startup_tx, server_startup_rx) = oneshot::channel();
        server_task_group
            .spawn_service("namesrv.server", async move {
                debug!("Server task started");
                let report = server
                    .run_with_shutdown_report_and_startup(
                        request_processor,
                        channel_event_listener,
                        async move {
                            signals::wait(&mut server_shutdown_rx).await;
                        },
                        server_startup_tx,
                    )
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

        let bound_address = server_startup_rx
            .await
            .map_err(|error| namesrv_startup_failed("await listener startup acknowledgement", error))??;

        // Setup remoting client with name server address
        let local_address = NetworkUtil::get_local_address().unwrap_or_else(|| {
            warn!("Failed to determine local address, using 127.0.0.1");
            "127.0.0.1".to_string()
        });

        let namesrv = CheetahString::from_string(format!("{}:{}", local_address, bound_address.port()));

        debug!("NameServer address: {}", namesrv);

        self.inner
            .remoting_client
            .update_name_server_address_list(vec![namesrv])
            .await;

        // Start remoting client directly (no spawn needed as it's managed by self.inner)
        self.inner
            .remoting_client
            .start()
            .await
            .map_err(|error| namesrv_startup_failed("start remoting client", error))?;

        #[cfg(feature = "embedded-controller")]
        if let Some(controller_manager) = self.inner.controller_manager() {
            controller_manager.start().await?;
        }

        // Transition to Running state
        if let Err(e) = self.transition_to(RuntimeState::Running) {
            error!("Failed to transition to Running state: {}", e);
            return Err(e);
        }

        info!("NameServer is now running and accepting requests");
        if let Some(lifecycle) = lifecycle {
            if let Err(error) = lifecycle.mark_ready() {
                warn!(error = %error, "NameServer readiness publication failed");
                return Err(namesrv_startup_failed("publish readiness", error));
            }
        }

        // Wait for shutdown signal
        let shutdown_rx = self
            .shutdown_rx
            .as_mut()
            .ok_or_else(|| namesrv_runtime_state_error("shutdown channel is not initialized"))?;
        signals::wait(shutdown_rx).await;
        info!("Shutdown signal received, initiating graceful shutdown...");
        let deadline = lifecycle
            .and_then(ServiceLifecycle::shutdown_request)
            .map(|request| request.deadline)
            .unwrap_or_else(|| ShutdownDeadline::after(Duration::from_secs(30)));
        let shutdown_report = self.shutdown_until(deadline).await;

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
    async fn shutdown_until(&mut self, deadline: ShutdownDeadline) -> NameServerShutdownReport {
        let started_at = Instant::now();
        let mut shutdown_report = NameServerShutdownReport::default();
        if let Some(shutdown_tx) = self.shutdown_tx.as_ref() {
            let _ = shutdown_tx.send(true);
        }

        match self.current_state() {
            RuntimeState::Created | RuntimeState::Initialized | RuntimeState::Running => {
                if let Err(error) = self.transition_to(RuntimeState::ShuttingDown) {
                    error!("Failed to transition to ShuttingDown state: {error}");
                }
            }
            RuntimeState::ShuttingDown => {
                debug!("NameServer shutdown is already in progress");
            }
            RuntimeState::Stopped => {
                debug!("NameServer is stopped; sweeping any remaining partially initialized resources");
            }
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

        if let Some(auth_runtime) = self.inner.auth_runtime() {
            shutdown_report.auth_runtime_healthy = Some(match auth_runtime.shutdown_with_report().await {
                Ok(report) => report.as_ref().is_none_or(ShutdownReport::is_healthy),
                Err(error) => {
                    warn!(%error, "NameServer auth runtime shutdown failed");
                    false
                }
            });
        }

        let metadata_deadline = MetadataDeadline::after(deadline.remaining());
        let metadata_persisted = match self.inner.kvconfig_manager().force_persist(metadata_deadline).await {
            Ok(()) => true,
            Err(error) => {
                warn!(%error, "NameServer final KV metadata persistence failed");
                false
            }
        };
        let metadata_drained = self
            .inner
            .kvconfig_manager()
            .shutdown_metadata_io(metadata_deadline)
            .await
            .is_none_or(|report| !report.timed_out && report.pending_operations == 0 && report.pending_bytes == 0);
        shutdown_report.metadata_io_healthy = Some(metadata_persisted && metadata_drained);

        info!("Phase 3/5: Shutting down embedded controller...");
        #[cfg(feature = "embedded-controller")]
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
        shutdown_report.route_unregistration =
            match tokio::time::timeout(deadline.remaining(), self.inner.route_info_manager().shutdown()).await {
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
        if self.current_state() != RuntimeState::Stopped {
            if let Err(error) = self.transition_to(RuntimeState::Stopped) {
                error!("Failed to transition to Stopped state: {error}");
            }
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
            crate::processor::default_request_processor::DefaultRequestProcessor::new(runtime_handle.clone());

        let mut name_server_request_processor = NameServerRequestProcessor::new_with_in_flight_tracker(
            self.inner.in_flight_request_tracker(),
            self.inner.namesrv_metrics(),
        )
        .with_runtime_handle(runtime_handle)
        .with_workload_admission(Arc::clone(&self.inner.workload_admission))
        .with_auth_runtime(self.inner.auth_runtime());

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

impl Builder {
    #[inline]
    pub fn new(service_context: ChildServiceContext, telemetry: TelemetryHandle) -> Self {
        Builder {
            name_server_config: None,
            server_config: None,
            tokio_client_config: None,
            #[cfg(feature = "embedded-controller")]
            controller_config: None,
            cluster_test_route_lookup: None,
            transport_security: None,
            transport_principal: None,
            telemetry,
            service_context,
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
    pub fn set_tokio_client_config(mut self, tokio_client_config: TransportClientConfig) -> Self {
        self.tokio_client_config = Some(tokio_client_config);
        self
    }

    #[inline]
    #[cfg(feature = "embedded-controller")]
    pub fn set_controller_config(mut self, controller_config: ControllerConfig) -> Self {
        self.controller_config = Some(controller_config);
        self
    }

    #[inline]
    #[cfg(feature = "embedded-controller")]
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

    /// Installs the validated listener security boundary.
    #[must_use]
    pub fn set_transport_security(
        mut self,
        transport_security: Arc<TransportSecurity>,
        principal: Option<Principal>,
    ) -> Self {
        self.transport_security = Some(transport_security);
        self.transport_principal = principal;
        self
    }

    /// Build the NameServerBootstrap with configured settings
    ///
    /// Creates all necessary components and initializes them immediately.
    #[instrument(skip(self), name = "build_bootstrap")]
    pub fn build(self) -> NameServerBootstrap {
        let namesrv_metrics = NameServerMetrics::from_handle(&self.telemetry);
        #[cfg(any(feature = "observability", feature = "otel-traces"))]
        let transport_telemetry = TransportTelemetry::from_handle(&self.telemetry);
        #[cfg(not(any(feature = "observability", feature = "otel-traces")))]
        let transport_telemetry = TransportTelemetry::noop();
        let name_server_config = self.name_server_config.unwrap_or_default();
        let tokio_client_config = self.tokio_client_config.unwrap_or_default();
        let server_config = self.server_config.unwrap_or_default();
        #[cfg(feature = "embedded-controller")]
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

        let service_context = self.service_context.component("rocketmq-namesrv");
        let metadata_io = Some(MetadataIoActor::start(
            &service_context.component("namesrv.metadata-io"),
            MetadataIoConfig::default(),
        ));
        let config_metadata_io = metadata_io.clone();
        let cluster_test_route_lookup = if name_server_config.cluster_test {
            self.cluster_test_route_lookup.or_else(|| {
                Some(Arc::new(TransportClusterTestRouteLookup::new(
                    &name_server_config.product_env_name,
                    service_context.component("namesrv.cluster-test-route-lookup"),
                    transport_telemetry.clone(),
                )) as Arc<dyn ClusterTestRouteLookup>)
            })
        } else {
            self.cluster_test_route_lookup
        };
        let transport_security = self.transport_security;
        let transport_principal = self.transport_principal;

        // Create remoting client
        let remoting_client = Arc::new(
            RemotingClient::builder(
                Arc::new(tokio_client_config.clone()),
                DefaultRequestProcessor,
                service_context.component("namesrv.remoting-client"),
            )
            .telemetry(transport_telemetry.clone())
            .build()
            .expect("clamped nameserver remoting client budgets must be valid"),
        );

        // Invalid values are rejected by `validate_runtime_config` before the
        // listener starts. Keep construction itself panic-free so startup can
        // return that typed error instead of panicking in `mpsc::channel`.
        let unregister_broker_queue_capacity = name_server_config.unregister_broker_queue_capacity().unwrap_or(1);
        let unregister_broker_batch_size = name_server_config.unregister_broker_batch_size;
        let unregister_broker_batch_time =
            std::time::Duration::from_millis(name_server_config.unregister_broker_batch_time_millis);
        let expiry_index_mode = name_server_config.expiry_index_mode;
        let expiry_safety_scan_interval = name_server_config.expiry_safety_scan_interval;
        let min_broker_notify_concurrency = name_server_config.min_broker_notify_concurrency;
        let route_response_cache = Arc::new(RouteResponseCache::from_namesrv_config(&name_server_config));
        let workload_admission = Arc::new(NameServerWorkloadAdmission::from_namesrv_config(&name_server_config));
        let initial_config = Arc::new(NameServerRuntimeConfig {
            name_server_config: Arc::new(name_server_config),
            tokio_client_config: Arc::new(tokio_client_config),
            server_config: Arc::new(server_config),
            #[cfg(feature = "embedded-controller")]
            controller_config: controller_config.map(Arc::new),
        });

        // Child services retain only a weak runtime handle. `Arc::new_cyclic` lets the
        // root own every service without creating a service -> runtime strong cycle.
        let inner = Arc::new_cyclic(|weak_inner| {
            let runtime_handle = NameServerRuntimeHandle::from_weak(weak_inner.clone());
            let route_info_manager = RouteInfoManager::new(
                runtime_handle.clone(),
                unregister_broker_queue_capacity,
                unregister_broker_batch_size,
                unregister_broker_batch_time,
                expiry_index_mode,
                expiry_safety_scan_interval,
                min_broker_notify_concurrency,
                namesrv_metrics.clone(),
            );

            NameServerRuntimeInner {
                config: ArcSwap::from(Arc::clone(&initial_config)),
                config_update_lock: parking_lot::Mutex::new(()),
                config_transaction_lock: tokio::sync::Mutex::new(()),
                config_generations: parking_lot::RwLock::new(ConfigGenerationState::new(Arc::clone(&initial_config))),
                config_metadata_io,
                auth_runtime: OnceLock::new(),
                route_info_manager: Arc::new(route_info_manager),
                route_response_cache: Arc::clone(&route_response_cache),
                workload_admission: Arc::clone(&workload_admission),
                kvconfig_manager: Arc::new(KVConfigManager::new(runtime_handle.clone(), metadata_io.clone())),
                remoting_client,
                broker_housekeeping_service: Arc::new(BrokerHousekeepingService::new(runtime_handle)),
                #[cfg(feature = "embedded-controller")]
                controller_manager: OnceLock::new(),
                cluster_test_route_lookup,
                service_context: Some(service_context),
                task_group: OnceLock::new(),
                in_flight_requests: Arc::new(InFlightRequestTracker::default()),
                namesrv_metrics: namesrv_metrics.clone(),
                telemetry: self.telemetry.clone(),
                transport_telemetry,
                transport_security,
                transport_principal,
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
    config_transaction_lock: tokio::sync::Mutex<()>,
    config_generations: parking_lot::RwLock<ConfigGenerationState>,
    config_metadata_io: Option<Result<MetadataIoActor, rocketmq_runtime::MetadataIoError>>,
    auth_runtime: OnceLock<Arc<AuthRuntime>>,
    route_info_manager: Arc<RouteInfoManager>,
    route_response_cache: Arc<RouteResponseCache>,
    workload_admission: Arc<NameServerWorkloadAdmission>,
    kvconfig_manager: Arc<KVConfigManager>,
    remoting_client: Arc<RemotingClient>,
    broker_housekeeping_service: Arc<BrokerHousekeepingService>,
    #[cfg(feature = "embedded-controller")]
    controller_manager: OnceLock<Arc<ControllerManager>>,
    telemetry: TelemetryHandle,
    transport_telemetry: TransportTelemetry,
    transport_security: Option<Arc<TransportSecurity>>,
    transport_principal: Option<Principal>,
    cluster_test_route_lookup: Option<Arc<dyn ClusterTestRouteLookup>>,
    service_context: Option<ChildServiceContext>,
    task_group: OnceLock<TaskGroup>,
    in_flight_requests: Arc<InFlightRequestTracker>,
    namesrv_metrics: NameServerMetrics,
}

#[derive(Clone)]
struct NameServerRuntimeConfig {
    name_server_config: Arc<NamesrvConfig>,
    tokio_client_config: Arc<TransportClientConfig>,
    server_config: Arc<ServerConfig>,
    #[cfg(feature = "embedded-controller")]
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
    ) -> RocketMQResult<()> {
        self.runtime().update_name_server_config(updates)
    }

    pub(crate) fn route_info_manager(&self) -> Arc<RouteInfoManager> {
        self.runtime().route_info_manager()
    }

    pub(crate) fn route_response_cache(&self) -> Arc<RouteResponseCache> {
        self.runtime().route_response_cache()
    }

    pub(crate) fn kvconfig_manager(&self) -> Arc<KVConfigManager> {
        self.runtime().kvconfig_manager()
    }

    pub(crate) fn task_group(&self) -> Option<TaskGroup> {
        self.runtime().task_group()
    }

    pub(crate) fn component_task_group(&self, scope: &'static str) -> TaskGroup {
        self.runtime().component_task_group(scope)
    }

    pub(crate) fn namesrv_metrics(&self) -> NameServerMetrics {
        self.runtime().namesrv_metrics()
    }

    pub(crate) fn cluster_test_route_lookup(&self) -> Option<Arc<dyn ClusterTestRouteLookup>> {
        self.runtime().cluster_test_route_lookup()
    }

    pub(crate) async fn update_runtime_config(
        &self,
        updates: HashMap<CheetahString, CheetahString>,
    ) -> RocketMQResult<ConfigApplyOutcome> {
        self.runtime().update_runtime_config(updates).await
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

        let service_context = self
            .service_context
            .as_ref()
            .expect("NameServerRuntime always has an injected ChildServiceContext");
        let _ = self.task_group.set(service_context.task_group().clone());
        self.task_group.get().cloned()
    }

    pub(crate) fn component_task_group(&self, scope: &'static str) -> TaskGroup {
        self.service_context
            .as_ref()
            .expect("NameServerRuntime always has an injected ChildServiceContext")
            .component(scope)
            .task_group()
            .clone()
    }

    pub(crate) fn in_flight_request_tracker(&self) -> Arc<InFlightRequestTracker> {
        Arc::clone(&self.in_flight_requests)
    }

    pub(crate) fn namesrv_metrics(&self) -> NameServerMetrics {
        self.namesrv_metrics.clone()
    }

    pub(crate) fn route_response_cache(&self) -> Arc<RouteResponseCache> {
        Arc::clone(&self.route_response_cache)
    }

    pub(crate) fn auth_runtime(&self) -> Option<Arc<AuthRuntime>> {
        self.auth_runtime.get().cloned()
    }

    pub(crate) fn in_flight_request_guard(&self) -> InFlightRequestGuard {
        self.in_flight_requests.enter()
    }

    #[inline]
    pub fn tokio_client_config(&self) -> Arc<TransportClientConfig> {
        Arc::clone(&self.config.load().tokio_client_config)
    }

    #[inline]
    pub fn server_config(&self) -> Arc<ServerConfig> {
        Arc::clone(&self.config.load().server_config)
    }

    #[inline]
    #[cfg(feature = "embedded-controller")]
    pub fn controller_config(&self) -> Option<Arc<ControllerConfig>> {
        self.config.load().controller_config.clone()
    }

    fn config_snapshot(&self) -> Arc<NameServerRuntimeConfig> {
        self.config.load_full()
    }

    pub(crate) fn update_name_server_config(
        &self,
        updates: HashMap<CheetahString, CheetahString>,
    ) -> RocketMQResult<()> {
        let _update_guard = self.config_update_lock.lock();
        let current = self.config_snapshot();
        let mut name_server_config = (*current.name_server_config).clone();
        name_server_config.update(updates)?;
        validate_startup_only_namesrv_config(&current.name_server_config, &name_server_config)?;
        self.config.store(Arc::new(NameServerRuntimeConfig {
            name_server_config: Arc::new(name_server_config),
            tokio_client_config: Arc::clone(&current.tokio_client_config),
            server_config: Arc::clone(&current.server_config),
            #[cfg(feature = "embedded-controller")]
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
            "routeFreshnessSampleInterval",
            name_server_config.route_freshness_sample_interval,
        );
        push_config_entry(
            &mut entries,
            "namesrvTypedZoneRouteEnable",
            name_server_config.namesrv_typed_zone_route_enable,
        );
        push_config_entry(
            &mut entries,
            "namesrvTypedZoneRouteShadow",
            name_server_config.namesrv_typed_zone_route_shadow,
        );
        push_config_entry(
            &mut entries,
            "namesrvRouteResponseCacheEnable",
            name_server_config.namesrv_route_response_cache_enable,
        );
        push_config_entry(
            &mut entries,
            "namesrvRouteResponseCacheMaxBytes",
            name_server_config.namesrv_route_response_cache_max_bytes,
        );
        push_config_entry(
            &mut entries,
            "namesrvRouteResponseCacheMaxEntries",
            name_server_config.namesrv_route_response_cache_max_entries,
        );
        push_config_entry(
            &mut entries,
            "namesrvRouteResponseCacheMaxSingleResponseBytes",
            name_server_config.namesrv_route_response_cache_max_single_response_bytes,
        );
        push_config_entry(
            &mut entries,
            "namesrvRouteResponseCacheShards",
            name_server_config.namesrv_route_response_cache_shards,
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
        push_config_entry(
            &mut entries,
            "allowInsecurePublicListener",
            name_server_config.allow_insecure_public_listener,
        );
        push_config_entry(
            &mut entries,
            "authenticationEnabled",
            name_server_config.auth_config.authentication_enabled,
        );
        push_config_entry(
            &mut entries,
            "authorizationEnabled",
            name_server_config.auth_config.authorization_enabled,
        );
        push_config_entry(&mut entries, "configBlackList", &name_server_config.config_black_list);
        push_config_entry(&mut entries, "listenPort", server_config.listen_port);
        push_config_entry(&mut entries, "bindAddress", &server_config.bind_address);
        for (key, value) in server_config.tls_config.java_property_entries() {
            push_config_entry(&mut entries, key, value);
        }

        push_config_entry(
            &mut entries,
            "connectTimeoutMillis",
            tokio_client_config.connect.timeout.as_millis(),
        );
        push_config_entry(
            &mut entries,
            "channelNotActiveInterval",
            tokio_client_config
                .maintenance
                .idle_scan_interval
                .map_or(0, |interval| interval.as_millis()),
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

    pub async fn update_runtime_config(
        &self,
        updates: HashMap<CheetahString, CheetahString>,
    ) -> RocketMQResult<ConfigApplyOutcome> {
        config_apply::apply_runtime_updates(self, updates).await
    }

    // Component accessors

    #[inline]
    pub fn route_info_manager(&self) -> Arc<RouteInfoManager> {
        Arc::clone(&self.route_info_manager)
    }

    #[inline]
    pub fn kvconfig_manager(&self) -> Arc<KVConfigManager> {
        Arc::clone(&self.kvconfig_manager)
    }

    #[inline]
    pub fn remoting_client(&self) -> &RemotingClient {
        &self.remoting_client
    }

    #[inline]
    pub fn broker_housekeeping_service(&self) -> Arc<BrokerHousekeepingService> {
        Arc::clone(&self.broker_housekeeping_service)
    }

    #[inline]
    #[cfg(feature = "embedded-controller")]
    pub fn controller_manager(&self) -> Option<Arc<ControllerManager>> {
        self.controller_manager.get().cloned()
    }

    #[cfg(feature = "embedded-controller")]
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

fn validate_startup_only_namesrv_config(current: &NamesrvConfig, candidate: &NamesrvConfig) -> RocketMQResult<()> {
    if current.enable_controller_in_namesrv != candidate.enable_controller_in_namesrv {
        return Err(RocketMQError::ConfigInvalidValue {
            key: "enableControllerInNamesrv",
            value: candidate.enable_controller_in_namesrv.to_string(),
            reason: "embedded Controller topology is startup-only; restart a binary built with the required feature"
                .to_string(),
        });
    }
    Ok(())
}

#[cfg(feature = "embedded-controller")]
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

fn parse_config_value<T>(key: &str, value: &CheetahString) -> RocketMQResult<T>
where
    T: std::str::FromStr,
{
    value
        .as_str()
        .parse()
        .map_err(|_| RocketMQError::nameserver_config_invalid(format!("invalid configuration value for key '{key}'")))
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::net::TcpListener;
    use std::str;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use cheetah_string::CheetahString;
    #[cfg(feature = "embedded-controller")]
    use rocketmq_controller::ControllerConfig;
    use rocketmq_error::ErrorKind;
    use rocketmq_model::common::config::TopicConfig;
    use rocketmq_model::common::constant::PermName;
    use rocketmq_model::common::mix_all::string_to_properties;
    use rocketmq_model::common::mix_all::MASTER_ID;
    use rocketmq_model::common::mix_all::ZONE_MODE;
    use rocketmq_model::common::mix_all::ZONE_NAME;
    use rocketmq_model::common::TopicSysFlag;
    use rocketmq_model::utils::crc32_utils;
    use rocketmq_model::version::RocketMqVersion;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::body::broker_body::broker_member_group::GetBrokerMemberGroupResponseBody;
    use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
    use rocketmq_protocol::protocol::body::broker_body::register_broker_body::RegisterBrokerBody;
    use rocketmq_protocol::protocol::body::kv_table::KVTable;
    use rocketmq_protocol::protocol::body::topic::topic_list::TopicList;
    use rocketmq_protocol::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
    use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
    use rocketmq_protocol::protocol::header::namesrv::broker_request::BrokerHeartbeatRequestHeader;
    use rocketmq_protocol::protocol::header::namesrv::broker_request::GetBrokerMemberGroupRequestHeader;
    use rocketmq_protocol::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
    use rocketmq_protocol::protocol::header::namesrv::kv_config_header::DeleteKVConfigRequestHeader;
    use rocketmq_protocol::protocol::header::namesrv::kv_config_header::GetKVConfigRequestHeader;
    use rocketmq_protocol::protocol::header::namesrv::kv_config_header::GetKVConfigResponseHeader;
    use rocketmq_protocol::protocol::header::namesrv::kv_config_header::GetKVListByNamespaceRequestHeader;
    use rocketmq_protocol::protocol::header::namesrv::kv_config_header::PutKVConfigRequestHeader;
    use rocketmq_protocol::protocol::header::namesrv::perm_broker_header::AddWritePermOfBrokerRequestHeader;
    use rocketmq_protocol::protocol::header::namesrv::perm_broker_header::AddWritePermOfBrokerResponseHeader;
    use rocketmq_protocol::protocol::header::namesrv::perm_broker_header::WipeWritePermOfBrokerRequestHeader;
    use rocketmq_protocol::protocol::header::namesrv::perm_broker_header::WipeWritePermOfBrokerResponseHeader;
    use rocketmq_protocol::protocol::header::namesrv::query_data_version_header::QueryDataVersionRequestHeader;
    use rocketmq_protocol::protocol::header::namesrv::query_data_version_header::QueryDataVersionResponseHeader;
    use rocketmq_protocol::protocol::header::namesrv::register_broker_header::RegisterBrokerRequestHeader;
    use rocketmq_protocol::protocol::header::namesrv::register_broker_header::RegisterBrokerResponseHeader;
    use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::DeleteTopicFromNamesrvRequestHeader;
    use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::GetTopicsByClusterRequestHeader;
    use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::RegisterTopicRequestHeader;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_protocol::protocol::route::route_data_view::QueueData;
    use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
    use rocketmq_protocol::protocol::DataVersion;
    use rocketmq_protocol::protocol::RemotingDeserializable;
    use rocketmq_protocol::protocol::RemotingSerializable;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_transport::api::v1::ConnectionState;
    use rocketmq_transport::api::v1::RPCHook;
    use rocketmq_transport::api::v1::RequestProcessor;
    use rocketmq_transport::api::v1::ServerConfig;
    use rocketmq_transport::api::v1::TlsMode;
    use rocketmq_transport::test_support::LocalRequestHarness;
    use tokio::net::TcpStream as TokioTcpStream;
    use tokio::sync::oneshot;
    use tokio::time::sleep;

    use super::*;

    fn test_task_group(name: &'static str) -> rocketmq_runtime::TaskGroup {
        RuntimeContext::from_current(name)
            .service_context("namesrv-local-harness")
            .task_group()
            .clone()
    }
    use crate::processor::default_request_processor::DefaultRequestProcessor;
    use crate::processor::ClientRequestProcessor;

    fn test_service_context() -> ChildServiceContext {
        static OWNER: std::sync::OnceLock<rocketmq_runtime::RuntimeOwner> = std::sync::OnceLock::new();
        OWNER
            .get_or_init(|| {
                rocketmq_runtime::RuntimeOwner::new(rocketmq_runtime::RuntimeConfig::server_default(
                    "namesrv-bootstrap-test",
                ))
                .expect("test runtime owner should build")
            })
            .root_context()
            .component("namesrv")
    }

    fn build_bootstrap_with_config(namesrv_config: NamesrvConfig) -> NameServerBootstrap {
        Builder::new(test_service_context(), TelemetryHandle::noop())
            .set_name_server_config(namesrv_config)
            .build()
    }

    fn build_default_bootstrap() -> NameServerBootstrap {
        build_bootstrap_with_config(NamesrvConfig::default())
    }

    struct PartiallyStartedRouteLookup {
        task_group: TaskGroup,
        started: AtomicBool,
        shutdown_called: AtomicBool,
    }

    type TestRouteLookupFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = RocketMQResult<T>> + Send + 'a>>;

    impl PartiallyStartedRouteLookup {
        fn new(service_context: ChildServiceContext) -> Self {
            Self {
                task_group: service_context.task_group().clone(),
                started: AtomicBool::new(false),
                shutdown_called: AtomicBool::new(false),
            }
        }
    }

    impl ClusterTestRouteLookup for PartiallyStartedRouteLookup {
        fn start(&self) -> TestRouteLookupFuture<'_, ()> {
            Box::pin(async move {
                self.started.store(true, Ordering::Release);
                let cancellation = self.task_group.cancellation_token();
                self.task_group
                    .spawn_service("namesrv.test-partial-route-lookup", async move {
                        cancellation.cancelled().await;
                    })
                    .map_err(|error| namesrv_startup_failed("start test route lookup task", error))?;
                Err(RocketMQError::network_connection_failed(
                    "namesrv.test-partial-route-lookup",
                    "simulated partial startup failure",
                ))
            })
        }

        fn lookup_topic_route(&self, _topic: &CheetahString) -> TestRouteLookupFuture<'_, Option<TopicRouteData>> {
            Box::pin(async { Ok(None) })
        }

        fn shutdown(&self) -> TestRouteLookupFuture<'_, ()> {
            Box::pin(async move {
                self.shutdown_called.store(true, Ordering::Release);
                let report = self
                    .task_group
                    .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
                    .await;
                report.assert_no_task_leak().map_err(|error| {
                    RocketMQError::network_connection_failed("namesrv.test-partial-route-lookup", error)
                })
            })
        }
    }

    #[tokio::test]
    async fn runtime_config_failed_update_preserves_published_snapshot() {
        let bootstrap = build_default_bootstrap();
        let runtime = bootstrap.runtime_inner();
        let before = runtime.config_snapshot();
        let before_port = before.server_config.listen_port;
        let before_connect_timeout = before.tokio_client_config.connect.timeout;

        let result = runtime
            .update_runtime_config(HashMap::from([
                (
                    CheetahString::from_static_str("listenPort"),
                    CheetahString::from_static_str("19876"),
                ),
                (
                    CheetahString::from_static_str("connectTimeoutMillis"),
                    CheetahString::from_static_str("not-a-number"),
                ),
            ]))
            .await;

        assert!(result.is_err());
        let after = runtime.config_snapshot();
        assert!(Arc::ptr_eq(&before, &after));
        assert_eq!(after.server_config.listen_port, before_port);
        assert_eq!(after.tokio_client_config.connect.timeout, before_connect_timeout);
    }

    #[tokio::test]
    async fn durable_runtime_update_publishes_only_live_keys() {
        let (config, root) = isolated_namesrv_config(NamesrvConfig::default());
        let config_path = config.config_store_path.clone();
        let bootstrap = build_bootstrap_with_config(config);
        let runtime = bootstrap.runtime_inner();
        let original_port = runtime.server_config().listen_port;

        let outcome = runtime
            .update_runtime_config(HashMap::from([
                (
                    CheetahString::from_static_str("enableTopicList"),
                    CheetahString::from_static_str("false"),
                ),
                (
                    CheetahString::from_static_str("listenPort"),
                    CheetahString::from_static_str("19876"),
                ),
            ]))
            .await
            .expect("valid desired configuration should become durable");

        assert!(!runtime.name_server_config().enable_topic_list);
        assert_eq!(runtime.server_config().listen_port, original_port);
        assert_eq!(outcome.restart_required_keys, vec!["listenPort"]);
        assert_eq!(outcome.desired_generation, outcome.durable_generation);
        assert_eq!(outcome.effective_generation, 1);
        let persisted = std::fs::read_to_string(config_path).expect("desired snapshot should be durable");
        assert!(persisted.contains("listenPort=19876"));
        drop(root);
    }

    #[tokio::test]
    async fn persistence_failure_does_not_publish_live_configuration() {
        let (mut config, root) = isolated_namesrv_config(NamesrvConfig::default());
        config.config_store_path = root.path().to_string_lossy().into_owned();
        let bootstrap = build_bootstrap_with_config(config);
        let runtime = bootstrap.runtime_inner();
        let before = runtime.config_snapshot();

        let result = runtime
            .update_runtime_config(HashMap::from([(
                CheetahString::from_static_str("enableTopicList"),
                CheetahString::from_static_str("false"),
            )]))
            .await;

        assert!(result.is_err());
        assert!(Arc::ptr_eq(&before, &runtime.config_snapshot()));
    }

    #[tokio::test]
    async fn runtime_config_rejects_removed_route_manager_switch_with_typed_error() {
        let bootstrap = build_default_bootstrap();
        let runtime = bootstrap.runtime_inner();
        let before = runtime.config_snapshot();

        let error = runtime
            .update_runtime_config(HashMap::from([(
                CheetahString::from_static_str("useRouteInfoManagerV2"),
                CheetahString::from_static_str("false"),
            )]))
            .await
            .expect_err("removed route manager switch must fail");

        assert!(matches!(
            error,
            RocketMQError::Tools(rocketmq_error::ToolsError::NameServerConfigInvalid { .. })
        ));
        assert!(Arc::ptr_eq(&before, &runtime.config_snapshot()));
    }

    #[tokio::test]
    async fn runtime_config_persists_dynamic_embedded_controller_topology_for_restart() {
        let (config, _root) = isolated_namesrv_config(NamesrvConfig::default());
        let bootstrap = build_bootstrap_with_config(config);
        let runtime = bootstrap.runtime_inner();
        let before = runtime.config_snapshot();
        let update = HashMap::from([(
            CheetahString::from_static_str("enableControllerInNamesrv"),
            CheetahString::from_static_str("true"),
        )]);

        let outcome = runtime
            .update_runtime_config(update.clone())
            .await
            .expect("restart-required update should be persisted as desired state");
        assert_eq!(outcome.restart_required_keys, vec!["enableControllerInNamesrv"]);
        assert!(Arc::ptr_eq(&before, &runtime.config_snapshot()));

        let namesrv_error = runtime
            .update_name_server_config(update)
            .expect_err("NameServer-only update must reject an embedded Controller topology change");
        assert!(matches!(
            namesrv_error,
            RocketMQError::ConfigInvalidValue {
                key: "enableControllerInNamesrv",
                ..
            }
        ));
        assert!(Arc::ptr_eq(&before, &runtime.config_snapshot()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_config_concurrent_readers_only_observe_complete_snapshots() {
        let (namesrv_config, _root) = isolated_namesrv_config(NamesrvConfig {
            enable_all_topic_list: true,
            enable_topic_list: true,
            ..NamesrvConfig::default()
        });
        let bootstrap = Builder::new(test_service_context(), TelemetryHandle::noop())
            .set_name_server_config(namesrv_config)
            .build();
        let runtime = bootstrap.runtime_inner();

        let writer = |runtime: Arc<NameServerRuntimeInner>, enable_all: &'static str, enable_topic: &'static str| {
            tokio::spawn(async move {
                for _ in 0..20 {
                    runtime
                        .update_runtime_config(HashMap::from([
                            (
                                CheetahString::from_static_str("enableAllTopicList"),
                                CheetahString::from_static_str(enable_all),
                            ),
                            (
                                CheetahString::from_static_str("enableTopicList"),
                                CheetahString::from_static_str(enable_topic),
                            ),
                        ]))
                        .await
                        .expect("valid snapshot update should succeed");
                }
            })
        };

        let first_writer = writer(Arc::clone(&runtime), "false", "false");
        let second_writer = writer(Arc::clone(&runtime), "true", "false");
        for _ in 0..10_000 {
            let snapshot = runtime.config_snapshot();
            let observed = (
                snapshot.name_server_config.enable_all_topic_list,
                snapshot.name_server_config.enable_topic_list,
            );
            assert!(
                matches!(observed, (true, true) | (false, false) | (true, false)),
                "reader observed a torn runtime configuration: {observed:?}"
            );
        }
        first_writer.await.expect("first config writer should not panic");
        second_writer.await.expect("second config writer should not panic");
    }

    #[test]
    fn runtime_owned_service_clones_do_not_keep_root_alive() {
        let bootstrap = build_default_bootstrap();
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
        let bootstrap = build_default_bootstrap();
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
        let bootstrap = Builder::new(service.clone(), TelemetryHandle::noop()).build();

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
        let (namesrv_config, _namesrv_root) = isolated_namesrv_config(NamesrvConfig::default());
        let bootstrap = Builder::new(service, TelemetryHandle::noop())
            .set_name_server_config(namesrv_config)
            .set_server_config(namesrv_server_config())
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
                .is_some_and(ClientShutdownReport::is_healthy),
            "remoting client shutdown report should be present and healthy: {report:?}"
        );
    }

    #[tokio::test]
    async fn namesrv_in_flight_drain_zero_requests_is_healthy() {
        let bootstrap = build_default_bootstrap();

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
        let bootstrap = build_default_bootstrap();
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
        let bootstrap = build_default_bootstrap();
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

    #[tokio::test]
    async fn occupied_listener_fails_startup_before_running() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test should reserve a listener");
        let port = listener
            .local_addr()
            .expect("listener should expose its address")
            .port();
        let (config, _root) = isolated_namesrv_config(NamesrvConfig::default());
        let server_config = ServerConfig {
            bind_address: "127.0.0.1".to_string(),
            listen_port: u32::from(port),
            ..ServerConfig::default()
        };
        let bootstrap = Builder::new(test_service_context(), TelemetryHandle::noop())
            .set_name_server_config(config)
            .set_server_config(server_config)
            .build();

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            bootstrap.boot_with_shutdown_report(std::future::pending()),
        )
        .await
        .expect("listener failure must be reported instead of entering the main loop");

        assert!(result.is_err());
        drop(listener);
    }

    #[tokio::test]
    async fn invalid_tls_material_fails_startup_before_running() {
        let (config, root) = isolated_namesrv_config(NamesrvConfig::default());
        let mut server_config = namesrv_server_config();
        server_config.tls_config.enable = true;
        server_config.tls_config.server.mode = TlsMode::Enforcing;
        server_config.tls_config.server.key_path =
            Some(root.path().join("missing-key.pem").to_string_lossy().into_owned());
        server_config.tls_config.server.cert_path =
            Some(root.path().join("missing-cert.pem").to_string_lossy().into_owned());
        let bootstrap = Builder::new(test_service_context(), TelemetryHandle::noop())
            .set_name_server_config(config)
            .set_server_config(server_config)
            .build();

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            bootstrap.boot_with_shutdown_report(std::future::pending()),
        )
        .await
        .expect("TLS initialization failure must be reported instead of entering the main loop");

        assert!(result.is_err());
    }

    fn namesrv_server_config() -> ServerConfig {
        ServerConfig {
            listen_port: reserve_local_port() as u32,
            bind_address: "127.0.0.1".to_string(),
            ..ServerConfig::default()
        }
    }

    fn isolated_namesrv_config(mut config: NamesrvConfig) -> (NamesrvConfig, tempfile::TempDir) {
        let root = tempfile::Builder::new()
            .prefix("rocketmq-namesrv-test-")
            .tempdir()
            .expect("test should create an isolated NameServer directory");
        config.rocketmq_home = root.path().to_string_lossy().into_owned();
        config.kv_config_path = root.path().join("kvConfig.json").to_string_lossy().into_owned();
        config.config_store_path = root.path().join("namesrv.properties").to_string_lossy().into_owned();
        (config, root)
    }

    #[cfg(feature = "embedded-controller")]
    fn embedded_controller_config() -> (ControllerConfig, tempfile::TempDir) {
        let root = tempfile::Builder::new()
            .prefix("rocketmq-controller-test-")
            .tempdir()
            .expect("test should create an isolated controller directory");
        let config = ControllerConfig::default()
            .with_node_info(1, format!("127.0.0.1:{}", reserve_local_port()).parse().unwrap())
            .with_storage_path(root.path().to_string_lossy().into_owned());
        (config, root)
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
        let bootstrap = build_default_bootstrap();
        let harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();
        let mut request = RemotingCommand::create_remoting_command(999_999);

        let response = process_with_name_server_processor(&bootstrap, &harness, &mut request).await;
        assert_eq!(response.code(), ResponseCode::NoPermission as i32);

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
        let body = RegisterBrokerBody::new(topic_config_wrapper.clone(), filter_server_list).encode(false);
        let body_crc32 = crc32_utils::crc32(&body);
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
        timeout_millis: Option<u64>,
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

        assert!(result.is_ok());
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
        let harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();
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
    async fn aggregate_processor_routes_register_and_route_queries() {
        let bootstrap = build_default_bootstrap();
        let harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();
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
    async fn namesrv_metadata_processors_return_java_compatible_bodies() {
        let bootstrap = build_default_bootstrap();
        let harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();
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
    async fn namesrv_topic_admin_processors_complete_contracts() {
        let bootstrap = build_default_bootstrap();
        let harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();
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
                .is_ok(),
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
                .is_err(),
            "deleted topic should no longer have route data"
        );
    }

    #[tokio::test]
    async fn register_broker_via_default_processor_populates_route_contract() {
        let bootstrap = build_default_bootstrap();
        let harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();
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
    async fn unregister_broker_via_default_processor_removes_route_contract() {
        let bootstrap = build_default_bootstrap();
        let harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();
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
                .is_ok(),
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
                .is_err()
        })
        .await;
        shutdown_unregister_service(&bootstrap).await;
    }

    #[tokio::test]
    async fn system_topic_list_includes_cluster_and_broker_names() {
        let bootstrap = build_default_bootstrap();
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
    async fn cluster_info_preserves_zone_and_acting_master_metadata() {
        let bootstrap = build_default_bootstrap();
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
    async fn topics_by_cluster_matches_java_duplicate_semantics() {
        let bootstrap = build_default_bootstrap();
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

        let topics = bootstrap
            .name_server_runtime
            .inner
            .route_info_manager()
            .get_topics_by_cluster(&cluster_name)
            .expect("registered cluster should have topics");
        let missing_cluster_topics = bootstrap
            .name_server_runtime
            .inner
            .route_info_manager()
            .get_topics_by_cluster(&CheetahString::from_static_str("missing-cluster"));

        assert_eq!(topics.iter().filter(|topic| topic.as_str() == shared_topic).count(), 2);
        assert!(missing_cluster_topics.is_err());
    }

    #[tokio::test]
    async fn unit_topic_queries_match_java_flag_semantics() {
        let bootstrap = build_default_bootstrap();
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
            .get_has_unit_sub_ununit_topic_list();

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
    async fn pickup_topic_route_data_promotes_read_only_prime_slave_to_acting_master() {
        let namesrv_config = NamesrvConfig {
            support_acting_master: true,
            ..NamesrvConfig::default()
        };
        let bootstrap = build_bootstrap_with_config(namesrv_config);
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
    async fn scan_not_active_broker_cleans_route_views_via_batch_unregister() {
        let bootstrap = build_default_bootstrap();
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let broker_addr = CheetahString::from_static_str("10.0.0.1:10911");
        let zone_name = CheetahString::from_static_str("zone-a");
        let topic_name = CheetahString::from_static_str("scan-cleanup-topic");
        let harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();

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

            route_manager.pickup_topic_route_data(&topic_name).is_err()
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
                && route_manager.get_topics_by_cluster(&cluster_name).is_err()
        })
        .await;

        shutdown_unregister_service(&bootstrap).await;
    }

    #[tokio::test]
    async fn scan_not_active_broker_closes_expired_connection_before_batch_unregister() {
        let bootstrap = build_default_bootstrap();
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let broker_addr = CheetahString::from_static_str("10.0.0.1:10911");
        let zone_name = CheetahString::from_static_str("zone-a");
        let harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();

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
    async fn connection_disconnected_by_socket_addr_matches_channel_destroy_cleanup() {
        let bootstrap = build_default_bootstrap();
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let broker_addr = CheetahString::from_static_str("10.0.0.1:10911");
        let zone_name = CheetahString::from_static_str("zone-a");
        let topic_name = CheetahString::from_static_str("socket-disconnect-topic");
        let harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();

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
                && route_manager.pickup_topic_route_data(&topic_name).is_err()
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
    async fn duplicate_channel_destroy_submission_is_idempotent_for_acting_master_cleanup() {
        let namesrv_config = NamesrvConfig {
            support_acting_master: true,
            ..NamesrvConfig::default()
        };
        let bootstrap = build_bootstrap_with_config(namesrv_config);
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let master_addr = CheetahString::from_static_str("10.0.0.1:10911");
        let slave_addr = CheetahString::from_static_str("10.0.0.2:10911");
        let zone_name = CheetahString::from_static_str("zone-a");
        let topic_name = CheetahString::from_static_str("duplicate-unregister-topic");
        let master_harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();
        let slave_harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();

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
            let Ok(route_data) = route_manager.pickup_topic_route_data(&topic_name) else {
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
    async fn on_channel_destroy_cleans_removed_broker_and_preserves_survivor() {
        let bootstrap = build_default_bootstrap();
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let removed_broker_name = CheetahString::from_static_str("broker-a");
        let surviving_broker_name = CheetahString::from_static_str("broker-b");
        let removed_broker_addr = CheetahString::from_static_str("10.0.0.1:10911");
        let surviving_broker_addr = CheetahString::from_static_str("10.0.0.2:10911");
        let zone_name = CheetahString::from_static_str("zone-a");
        let topic_name = CheetahString::from_static_str("channel-destroy-topic");
        let removed_harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();
        let surviving_harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();

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
            let Ok(route_data) = route_manager.pickup_topic_route_data(&topic_name) else {
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
    async fn on_channel_destroy_reduces_to_read_only_acting_master() {
        let namesrv_config = NamesrvConfig {
            support_acting_master: true,
            ..NamesrvConfig::default()
        };
        let bootstrap = build_bootstrap_with_config(namesrv_config);
        let cluster_name = CheetahString::from_static_str("cluster-a");
        let broker_name = CheetahString::from_static_str("broker-a");
        let master_addr = CheetahString::from_static_str("10.0.0.1:10911");
        let slave_addr = CheetahString::from_static_str("10.0.0.2:10911");
        let zone_name = CheetahString::from_static_str("zone-a");
        let topic_name = CheetahString::from_static_str("acting-master-cleanup-topic");
        let master_harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();
        let slave_harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();

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
            let Ok(route_data) = route_manager.pickup_topic_route_data(&topic_name) else {
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
    async fn boot_rolls_back_partially_started_cluster_lookup() {
        let runtime = RuntimeContext::from_current("namesrv-partial-startup-rollback");
        let service_context = runtime.service_context("namesrv");
        let route_lookup = Arc::new(PartiallyStartedRouteLookup::new(
            service_context.component("partial-route-lookup"),
        ));
        let (namesrv_config, _namesrv_root) = isolated_namesrv_config(NamesrvConfig {
            cluster_test: true,
            ..NamesrvConfig::default()
        });
        let bootstrap = Builder::new(service_context, TelemetryHandle::noop())
            .set_name_server_config(namesrv_config)
            .set_server_config(namesrv_server_config())
            .set_cluster_test_route_lookup(route_lookup.clone())
            .build();
        let runtime_state = Arc::clone(&bootstrap.name_server_runtime.state);

        let error = tokio::time::timeout(
            Duration::from_secs(5),
            bootstrap.boot_with_shutdown_report(std::future::pending()),
        )
        .await
        .expect("startup rollback should honor its deadline")
        .expect_err("partially started route lookup should fail startup");

        assert!(error.to_string().contains("simulated partial startup failure"));
        assert!(route_lookup.started.load(Ordering::Acquire));
        assert!(route_lookup.shutdown_called.load(Ordering::Acquire));
        assert_eq!(route_lookup.task_group.task_count(), 0);
        assert_eq!(
            RuntimeState::from_u8(runtime_state.load(Ordering::Acquire)),
            Some(RuntimeState::Stopped)
        );

        runtime
            .shutdown_tasks(Duration::from_secs(1))
            .await
            .assert_no_task_leak()
            .expect("startup rollback must not leak owned tasks");
    }

    #[tokio::test]
    async fn boot_supports_cluster_test_mode() {
        let runtime = RuntimeContext::from_current("namesrv-cluster-test-mode");
        let (namesrv_config, _namesrv_root) = isolated_namesrv_config(NamesrvConfig {
            cluster_test: true,
            ..NamesrvConfig::default()
        });
        let bootstrap = Builder::new(runtime.service_context("namesrv"), TelemetryHandle::noop())
            .set_name_server_config(namesrv_config)
            .set_server_config(namesrv_server_config())
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

    #[cfg(feature = "embedded-controller")]
    #[tokio::test]
    async fn boot_supports_enable_controller_in_namesrv_mode() {
        let (namesrv_config, _namesrv_root) = isolated_namesrv_config(NamesrvConfig {
            enable_controller_in_namesrv: true,
            ..NamesrvConfig::default()
        });
        let (controller_config, _controller_root) = embedded_controller_config();
        let bootstrap = Builder::new(test_service_context(), TelemetryHandle::noop())
            .set_name_server_config(namesrv_config)
            .set_server_config(namesrv_server_config())
            .set_controller_config(controller_config)
            .build();

        bootstrap
            .boot_with_shutdown(async {})
            .await
            .expect("controller-in-namesrv mode should boot and shut down cleanly once implemented");
    }

    #[cfg(not(feature = "embedded-controller"))]
    #[tokio::test]
    async fn embedded_controller_config_requires_compile_time_capability() {
        let namesrv_config = NamesrvConfig {
            enable_controller_in_namesrv: true,
            ..NamesrvConfig::default()
        };
        let mut bootstrap = Builder::new(test_service_context(), TelemetryHandle::noop())
            .set_name_server_config(namesrv_config)
            .set_server_config(namesrv_server_config())
            .build();

        let error = bootstrap
            .name_server_runtime
            .initialize()
            .await
            .expect_err("a binary without embedded-controller must reject the runtime setting");

        assert!(error
            .to_string()
            .contains("compiled without the `embedded-controller` feature"));
    }

    #[tokio::test]
    async fn boot_shutdown_report_includes_remoting_server_report() {
        let (namesrv_config, _namesrv_root) = isolated_namesrv_config(NamesrvConfig::default());
        let bootstrap = Builder::new(test_service_context(), TelemetryHandle::noop())
            .set_name_server_config(namesrv_config)
            .set_server_config(namesrv_server_config())
            .build();

        let report = bootstrap
            .boot_with_shutdown_report(async {})
            .await
            .expect("namesrv should boot and return shutdown report");

        assert!(report.is_healthy(), "{report:?}");
        assert!(
            report.shutdown_relay.as_ref().is_some_and(ShutdownReport::is_healthy),
            "shutdown relay report should be present and healthy: {report:?}"
        );
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
        let (namesrv_config, namesrv_root) = isolated_namesrv_config(NamesrvConfig::default());
        let bootstrap = Builder::new(test_service_context(), TelemetryHandle::noop())
            .set_name_server_config(namesrv_config)
            .set_server_config(server_config)
            .build();
        let server_task = tokio::spawn(async move {
            let _namesrv_root = namesrv_root;
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

    #[cfg(feature = "embedded-controller")]
    #[tokio::test]
    async fn enable_controller_in_namesrv_rejects_conflicting_listen_addr() {
        let server_config = namesrv_server_config();
        let conflicting_controller = ControllerConfig::default()
            .with_node_info(1, format!("127.0.0.1:{}", server_config.listen_port).parse().unwrap());
        let namesrv_config = NamesrvConfig {
            enable_controller_in_namesrv: true,
            ..NamesrvConfig::default()
        };
        let mut bootstrap = Builder::new(test_service_context(), TelemetryHandle::noop())
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

    #[cfg(feature = "embedded-controller")]
    #[tokio::test]
    async fn enable_controller_in_namesrv_lifecycle_matches_namesrv_runtime() {
        let (namesrv_config, _namesrv_root) = isolated_namesrv_config(NamesrvConfig {
            enable_controller_in_namesrv: true,
            ..NamesrvConfig::default()
        });
        let (controller_config, _controller_root) = embedded_controller_config();
        let mut bootstrap = Builder::new(test_service_context(), TelemetryHandle::noop())
            .set_name_server_config(namesrv_config)
            .set_server_config(namesrv_server_config())
            .set_controller_config(controller_config)
            .build();

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
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
            .send(true)
            .expect("shutdown broadcast should reach the running runtime");

        start_handle
            .await
            .expect("runtime task should not panic")
            .expect("runtime start should exit cleanly");

        assert!(!controller_manager.is_running());
    }

    #[tokio::test]
    async fn unsupported_request_code_returns_request_code_not_supported() {
        let bootstrap = build_default_bootstrap();
        let harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();
        let processor =
            DefaultRequestProcessor::new(NameServerRuntimeHandle::new(&bootstrap.name_server_runtime.inner));
        let mut request = RemotingCommand::create_remoting_command(RequestCode::SendMessage);

        let response = processor
            .process_request_inner(harness.channel(), RequestCode::SendMessage, &mut request)
            .await
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
    async fn get_namesrv_config_returns_aggregated_runtime_properties() {
        let namesrv_config = NamesrvConfig {
            client_request_thread_pool_nums: 12,
            ..NamesrvConfig::default()
        };
        let server_config = ServerConfig {
            listen_port: 19876,
            bind_address: "127.0.0.2".to_string(),
            ..ServerConfig::default()
        };
        let bootstrap = Builder::new(test_service_context(), TelemetryHandle::noop())
            .set_name_server_config(namesrv_config)
            .set_server_config(server_config)
            .build();
        let harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();
        let mut request = RemotingCommand::create_remoting_command(RequestCode::GetNamesrvConfig);

        let response = process_with_default_processor(&bootstrap, &harness, &mut request).await;

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);

        let body = response.body().expect("config response should include a body");
        let body = str::from_utf8(body).expect("config body should be utf-8");
        let properties = string_to_properties(body).expect("config body should use java properties format");
        let connect_timeout_millis = bootstrap
            .name_server_runtime
            .inner
            .tokio_client_config()
            .connect
            .timeout
            .as_millis()
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
            properties.get("connectTimeoutMillis").map(|value| value.as_str()),
            Some(connect_timeout_millis.as_str())
        );
        assert!(!properties.contains_key("useRouteInfoManagerV2"));
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
    async fn update_namesrv_config_rejects_unknown_key_atomically() {
        let (config, _root) = isolated_namesrv_config(NamesrvConfig::default());
        let bootstrap = build_bootstrap_with_config(config);
        let harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();
        let before = bootstrap.name_server_runtime.inner.config_snapshot();
        let mut request = RemotingCommand::create_remoting_command(RequestCode::UpdateNamesrvConfig).set_body(
            b"listenPort=19876\nbindAddress=127.0.0.2\nconnectTimeoutMillis=9\nenableTopicList=false\ntls.server.mode=enforcing\ntls.server.certPath=/certs/server.pem\nunknownKey=42"
                .as_slice(),
        );

        let processor =
            DefaultRequestProcessor::new(NameServerRuntimeHandle::new(&bootstrap.name_server_runtime.inner));
        let result = processor
            .process_request_inner(harness.channel(), RequestCode::UpdateNamesrvConfig, &mut request)
            .await;

        assert!(result.is_err());
        assert!(Arc::ptr_eq(
            &before,
            &bootstrap.name_server_runtime.inner.config_snapshot()
        ));
    }

    #[tokio::test]
    async fn update_namesrv_config_reports_durable_and_restart_generations() {
        let (config, _root) = isolated_namesrv_config(NamesrvConfig::default());
        let bootstrap = build_bootstrap_with_config(config);
        let original_port = bootstrap.name_server_runtime.inner.server_config().listen_port;
        let harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();
        let mut request = RemotingCommand::create_remoting_command(RequestCode::UpdateNamesrvConfig)
            .set_body(b"listenPort=19876\nenableTopicList=false".as_slice());

        let response = process_with_default_processor(&bootstrap, &harness, &mut request).await;

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert_eq!(
            bootstrap.name_server_runtime.inner.server_config().listen_port,
            original_port
        );
        assert!(
            !bootstrap
                .name_server_runtime
                .inner
                .name_server_config()
                .enable_topic_list
        );
        let ext = response
            .ext_fields()
            .expect("config outcome should include extension fields");
        assert_eq!(ext.get("desiredGeneration").map(|value| value.as_str()), Some("1"));
        assert_eq!(ext.get("durableGeneration").map(|value| value.as_str()), Some("1"));
        assert_eq!(ext.get("effectiveGeneration").map(|value| value.as_str()), Some("1"));
        assert_eq!(
            ext.get("appliedKeys").map(|value| value.as_str()),
            Some("enableTopicList")
        );
        assert_eq!(
            ext.get("restartRequiredKeys").map(|value| value.as_str()),
            Some("listenPort")
        );
    }

    #[tokio::test]
    async fn update_namesrv_config_rejects_fixed_blacklist_keys() {
        let bootstrap = build_default_bootstrap();
        let harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();
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
    async fn kvconfig_crud_roundtrip_via_default_processor() {
        let bootstrap = build_default_bootstrap();
        let harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();
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
    async fn namesrv_kvconfig_metadata_io_actor_persists_durable_generation() {
        let temp = tempfile::tempdir().expect("NameServer metadata test directory should be created");
        let config_path = temp.path().join("kv-config.json");
        let context = RuntimeContext::try_from_current("namesrv-metadata-io-test").unwrap();
        let bootstrap = Builder::new(context.service_context("namesrv"), TelemetryHandle::noop())
            .set_name_server_config(NamesrvConfig {
                kv_config_path: config_path.to_string_lossy().into_owned(),
                ..NamesrvConfig::default()
            })
            .build();
        let manager = bootstrap.name_server_runtime.inner.kvconfig_manager();
        manager
            .put_kv_config(
                CheetahString::from_static_str("namespace"),
                CheetahString::from_static_str("key"),
                CheetahString::from_static_str("value"),
                MetadataDeadline::after(Duration::from_secs(5)),
            )
            .await
            .unwrap();

        let snapshot = manager
            .metadata_io_snapshot()
            .expect("service-context NameServer should own metadata I/O actor");
        let resource = snapshot
            .resources
            .iter()
            .find(|resource| resource.resource.as_ref() == "namesrv.kv-config")
            .expect("KV metadata resource should be tracked");
        assert!(resource.durable_generation.is_some());
        assert_eq!(snapshot.pending_operations, 0);
        assert!(config_path.is_file());

        let report = manager
            .shutdown_metadata_io(MetadataDeadline::after(Duration::from_secs(5)))
            .await
            .expect("metadata actor should produce shutdown report");
        assert!(!report.timed_out);
    }

    #[tokio::test]
    async fn route_query_via_client_processor_returns_order_config_and_route_contract() {
        let (namesrv_config, _namesrv_root) = isolated_namesrv_config(NamesrvConfig {
            order_message_enable: true,
            ..NamesrvConfig::default()
        });
        let bootstrap = build_bootstrap_with_config(namesrv_config);
        let harness = LocalRequestHarness::new(test_task_group("namesrv-local-harness"))
            .await
            .unwrap();
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
                MetadataDeadline::after(Duration::from_secs(5)),
            )
            .await
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

    #[tokio::test]
    async fn noop_metrics_skip_freshness_lookup() {
        let (namesrv_config, _namesrv_root) = isolated_namesrv_config(NamesrvConfig::default());
        let bootstrap = build_bootstrap_with_config(namesrv_config);
        let harness = LocalRequestHarness::new(test_task_group("namesrv-freshness-gate-test"))
            .await
            .unwrap();
        register_test_broker(
            &bootstrap,
            &CheetahString::from_static_str("freshness-cluster"),
            &CheetahString::from_static_str("freshness-broker"),
            &CheetahString::from_static_str("10.0.0.20:10911"),
            MASTER_ID,
            &CheetahString::from_static_str("zone-a"),
            false,
            topic_config_wrapper(&[("freshness-topic", 0, PermName::PERM_READ | PermName::PERM_WRITE)]),
        )
        .await;
        let route_manager = bootstrap.runtime_inner().route_info_manager();
        let mut processor =
            ClientRequestProcessor::new(NameServerRuntimeHandle::new(&bootstrap.name_server_runtime.inner));
        let mut request = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new(CheetahString::from_static_str("freshness-topic"), Some(true)),
        );
        request.make_custom_header_to_net();

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("route processing should succeed")
            .expect("route processing should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert_eq!(route_manager.route_freshness_lookup_count(), 0);
    }

    #[tokio::test]
    async fn next_request_observes_effective_config_generation() {
        let (namesrv_config, _namesrv_root) = isolated_namesrv_config(NamesrvConfig::default());
        let bootstrap = build_bootstrap_with_config(namesrv_config);
        let harness = LocalRequestHarness::new(test_task_group("namesrv-live-route-config-test"))
            .await
            .unwrap();
        let topic = CheetahString::from_static_str("live-route-config-topic");
        let order_conf = CheetahString::from_static_str("live-route-config-broker:4");
        register_test_broker(
            &bootstrap,
            &CheetahString::from_static_str("live-route-config-cluster"),
            &CheetahString::from_static_str("live-route-config-broker"),
            &CheetahString::from_static_str("10.0.0.21:10911"),
            MASTER_ID,
            &CheetahString::from_static_str("zone-a"),
            false,
            topic_config_wrapper(&[("live-route-config-topic", 0, PermName::PERM_READ | PermName::PERM_WRITE)]),
        )
        .await;
        bootstrap
            .runtime_inner()
            .kvconfig_manager()
            .put_kv_config(
                CheetahString::from_static_str("ORDER_TOPIC_CONFIG"),
                topic.clone(),
                order_conf.clone(),
                MetadataDeadline::after(Duration::from_secs(5)),
            )
            .await
            .unwrap();
        let mut processor =
            ClientRequestProcessor::new(NameServerRuntimeHandle::new(&bootstrap.name_server_runtime.inner));

        let mut before_request = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new(topic.clone(), Some(true)),
        );
        before_request.make_custom_header_to_net();
        let before = processor
            .process_request(harness.channel(), harness.context(), &mut before_request)
            .await
            .unwrap()
            .unwrap();
        let before_route =
            TopicRouteData::decode(before.body().expect("route response should include a body")).unwrap();
        assert!(before_route.order_topic_conf.is_none());

        let outcome = bootstrap
            .runtime_inner()
            .update_runtime_config(HashMap::from([(
                CheetahString::from_static_str("orderMessageEnable"),
                CheetahString::from_static_str("true"),
            )]))
            .await
            .expect("live route configuration should update durably");
        assert_eq!(outcome.effective_generation, 1);
        assert_eq!(outcome.applied_keys, vec!["orderMessageEnable"]);

        let mut after_request = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new(topic, Some(true)),
        );
        after_request.make_custom_header_to_net();
        let after = processor
            .process_request(harness.channel(), harness.context(), &mut after_request)
            .await
            .unwrap()
            .unwrap();
        let after_route = TopicRouteData::decode(after.body().expect("route response should include a body")).unwrap();
        assert_eq!(after_route.order_topic_conf.as_ref(), Some(&order_conf));
    }

    #[tokio::test]
    async fn typed_zone_route_encodes_once_without_hook_decode() {
        let (namesrv_config, _namesrv_root) = isolated_namesrv_config(NamesrvConfig {
            namesrv_typed_zone_route_enable: true,
            ..NamesrvConfig::default()
        });
        let bootstrap = build_bootstrap_with_config(namesrv_config);
        let harness = LocalRequestHarness::new(test_task_group("namesrv-typed-zone-test"))
            .await
            .unwrap();
        let topic_wrapper =
            || topic_config_wrapper(&[("typed-zone-topic", 0, PermName::PERM_READ | PermName::PERM_WRITE)]);
        register_test_broker(
            &bootstrap,
            &CheetahString::from_static_str("typed-zone-cluster"),
            &CheetahString::from_static_str("typed-zone-broker-a"),
            &CheetahString::from_static_str("10.0.0.31:10911"),
            MASTER_ID,
            &CheetahString::from_static_str("zone-a"),
            false,
            topic_wrapper(),
        )
        .await;
        register_test_broker(
            &bootstrap,
            &CheetahString::from_static_str("typed-zone-cluster"),
            &CheetahString::from_static_str("typed-zone-broker-b"),
            &CheetahString::from_static_str("10.0.0.32:10911"),
            MASTER_ID,
            &CheetahString::from_static_str("zone-b"),
            false,
            topic_wrapper(),
        )
        .await;
        let mut processor =
            ClientRequestProcessor::new(NameServerRuntimeHandle::new(&bootstrap.name_server_runtime.inner));
        let mut request = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new(CheetahString::from_static_str("typed-zone-topic"), Some(true)),
        );
        request.make_custom_header_to_net();
        request
            .add_ext_field(ZONE_MODE, "true")
            .add_ext_field(ZONE_NAME, "zone-a");
        crate::processor::client_request_processor::reset_route_encode_count();
        crate::route::zone_route_rpc_hook::reset_zone_hook_decode_count();

        let mut response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .unwrap()
            .unwrap();
        ZoneRouteRPCHook::default()
            .do_after_response(SocketAddr::from(([127, 0, 0, 1], 10911)), &request, &mut response)
            .unwrap();

        assert_eq!(crate::processor::client_request_processor::route_encode_count(), 1);
        assert_eq!(crate::route::zone_route_rpc_hook::zone_hook_decode_count(), 0);
        let route = TopicRouteData::decode(response.body().expect("route response should include a body")).unwrap();
        assert_eq!(route.broker_datas.len(), 1);
        assert_eq!(
            route.broker_datas[0].zone_name().map(CheetahString::as_str),
            Some("zone-a")
        );
    }

    #[tokio::test]
    async fn cache_disabled_uses_live_encode() {
        let (namesrv_config, _namesrv_root) = isolated_namesrv_config(NamesrvConfig::default());
        let bootstrap = build_bootstrap_with_config(namesrv_config);
        let harness = LocalRequestHarness::new(test_task_group("namesrv-cache-disabled-test"))
            .await
            .unwrap();
        register_test_broker(
            &bootstrap,
            &CheetahString::from_static_str("cache-disabled-cluster"),
            &CheetahString::from_static_str("cache-disabled-broker"),
            &CheetahString::from_static_str("10.0.0.41:10911"),
            MASTER_ID,
            &CheetahString::from_static_str("zone-a"),
            false,
            topic_config_wrapper(&[("cache-disabled-topic", 0, PermName::PERM_READ | PermName::PERM_WRITE)]),
        )
        .await;
        let mut processor =
            ClientRequestProcessor::new(NameServerRuntimeHandle::new(&bootstrap.name_server_runtime.inner));
        crate::processor::client_request_processor::reset_route_encode_count();

        for _ in 0..2 {
            let mut request = RemotingCommand::create_request_command(
                RequestCode::GetRouteinfoByTopic,
                GetRouteInfoRequestHeader::new(CheetahString::from_static_str("cache-disabled-topic"), Some(true)),
            );
            request.make_custom_header_to_net();
            let response = processor
                .process_request(harness.channel(), harness.context(), &mut request)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        }

        assert_eq!(crate::processor::client_request_processor::route_encode_count(), 2);
        let stats = bootstrap.runtime_inner().route_response_cache().stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
    }

    #[tokio::test]
    async fn cache_hit_reuses_the_versioned_encoded_body() {
        let (namesrv_config, _namesrv_root) = isolated_namesrv_config(NamesrvConfig {
            namesrv_route_response_cache_enable: true,
            ..NamesrvConfig::default()
        });
        let bootstrap = build_bootstrap_with_config(namesrv_config);
        let harness = LocalRequestHarness::new(test_task_group("namesrv-cache-hit-test"))
            .await
            .unwrap();
        register_test_broker(
            &bootstrap,
            &CheetahString::from_static_str("cache-hit-cluster"),
            &CheetahString::from_static_str("cache-hit-broker"),
            &CheetahString::from_static_str("10.0.0.42:10911"),
            MASTER_ID,
            &CheetahString::from_static_str("zone-a"),
            false,
            topic_config_wrapper(&[("cache-hit-topic", 0, PermName::PERM_READ | PermName::PERM_WRITE)]),
        )
        .await;
        let mut processor =
            ClientRequestProcessor::new(NameServerRuntimeHandle::new(&bootstrap.name_server_runtime.inner));
        crate::processor::client_request_processor::reset_route_encode_count();
        let mut bodies = Vec::new();

        for _ in 0..2 {
            let mut request = RemotingCommand::create_request_command(
                RequestCode::GetRouteinfoByTopic,
                GetRouteInfoRequestHeader::new(CheetahString::from_static_str("cache-hit-topic"), Some(true)),
            );
            request.make_custom_header_to_net();
            let response = processor
                .process_request(harness.channel(), harness.context(), &mut request)
                .await
                .unwrap()
                .unwrap();
            bodies.push(response.body().expect("route response should include a body").clone());
        }

        assert_eq!(bodies[0], bodies[1]);
        assert_eq!(crate::processor::client_request_processor::route_encode_count(), 1);
        let stats = bootstrap.runtime_inner().route_response_cache().stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }
}
