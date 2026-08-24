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
use std::future::Future;
use std::sync::atomic::AtomicBool;
#[cfg(test)]
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use super::raft_controller::RaftController;
use crate::config::ControllerConfig;
use crate::config::ControllerConfigHandle;
use crate::controller::broker_heartbeat_manager::BrokerHeartbeatManager;
use crate::controller::broker_housekeeping_service::BrokerHousekeepingService;
use crate::controller::broker_role_notifier::BrokerRoleNotifier;
use crate::controller::broker_role_notifier::NotifyKey;
use crate::controller::broker_role_notifier::NotifySnapshot;
use crate::controller::broker_role_notifier::NotifyState;
use crate::controller::broker_role_notifier::NotifyTask;
use crate::controller::broker_role_notifier::SubmitOutcome;
use crate::controller::Controller;
use crate::error::ControllerError;
use crate::error::Result;
use crate::heartbeat::default_broker_heartbeat_manager::DefaultBrokerHeartbeatManager;
use crate::helper::broker_lifecycle_listener::BrokerLifecycleListener;
#[cfg(feature = "metrics")]
use crate::metrics::controller_metrics_manager::active_broker_count_from_snapshot;
use crate::metrics::controller_metrics_manager::ControllerMetricsManager;
use crate::processor::controller_request_processor::ControllerRequestProcessor;
use crate::security::ControllerSecurity;
use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_observability::TelemetryHandle;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::elect_master_response_body::ElectMasterResponseBody;
use rocketmq_protocol::protocol::body::sync_state_set_body::SyncStateSet;
use rocketmq_protocol::protocol::header::controller::elect_master_request_header::ElectMasterRequestHeader;
use rocketmq_protocol::protocol::header::controller::get_replica_info_request_header::GetReplicaInfoRequestHeader;
use rocketmq_protocol::protocol::header::controller::get_replica_info_response_header::GetReplicaInfoResponseHeader;
use rocketmq_protocol::protocol::header::elect_master_response_header::ElectMasterResponseHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::RemotingDeserializable;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::OperationContext;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;
use rocketmq_transport::api::v1::ChannelEventListener;
use rocketmq_transport::api::v1::DefaultRequestProcessor;
use rocketmq_transport::api::v1::RemotingClient;
use rocketmq_transport::api::v1::ServerConfig;
use rocketmq_transport::api::v1::TransportClientConfig;
use rocketmq_transport::api::v1::TransportServer;
use rocketmq_transport::api::v1::TransportTelemetry;
use tokio::sync::oneshot;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::sleep;
use tracing::error;
use tracing::info;
use tracing::warn;

struct BrokerInactiveListener {
    controller_manager: Weak<ControllerManager>,
}

impl BrokerInactiveListener {
    fn new(controller_manager: Weak<ControllerManager>) -> Self {
        Self { controller_manager }
    }
}

fn spawn_inactive_broker_worker<F>(task_group: &TaskGroup, future: F) -> rocketmq_runtime::RuntimeResult<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let operation = OperationContext::without_deadline(TaskKind::Worker);
    task_group
        .spawn_operation(&operation, "controller.broker-inactive", future)
        .map(|_| ())
}

impl BrokerLifecycleListener for BrokerInactiveListener {
    fn on_broker_inactive(&self, cluster_name: Option<&str>, broker_name: &str, broker_id: Option<i64>) {
        let Some(controller_manager) = self.controller_manager.upgrade() else {
            return;
        };

        if !controller_manager.is_running() {
            return;
        }

        let cluster_name = cluster_name.map(str::to_owned);
        let broker_name = CheetahString::from_string(broker_name.to_owned());

        let Some(task_group) = controller_manager.manager_task_group() else {
            warn!(
                "Skip inactive broker handling because controller task group is not initialized, cluster={:?}, \
                 broker={}, broker_id={:?}",
                cluster_name, broker_name, broker_id
            );
            return;
        };

        if let Err(error) = spawn_inactive_broker_worker(&task_group, async move {
            if !controller_manager.is_leader() {
                warn!(
                    "Broker inactive event ignored on follower controller, cluster={:?}, broker={}, broker_id={:?}",
                    cluster_name, broker_name, broker_id
                );
                return;
            }

            if let Err(error) = controller_manager
                .controller()
                .remove_broker_live_info(cluster_name.as_deref(), broker_name.as_str(), broker_id)
                .await
            {
                warn!(
                    "Failed to remove inactive broker live state, cluster={:?}, broker={}, broker_id={:?}, error={}",
                    cluster_name, broker_name, broker_id, error
                );
            }

            if let Some(inactive_broker_id) = broker_id {
                let replica_request = GetReplicaInfoRequestHeader {
                    broker_name: broker_name.clone(),
                };
                let should_elect = match controller_manager.controller().get_replica_info(&replica_request).await {
                    Ok(Some(response)) if response.code() == ResponseCode::Success as i32 => {
                        response
                            .decode_command_custom_header::<GetReplicaInfoResponseHeader>()
                            .ok()
                            .and_then(|header| header.master_broker_id)
                            == Some(inactive_broker_id)
                    }
                    Ok(Some(response)) => {
                        warn!(
                            "Skip inactive broker election because replica info query failed, broker={}, code={}, \
                             remark={:?}",
                            broker_name,
                            response.code(),
                            response.remark()
                        );
                        false
                    }
                    Ok(None) => {
                        warn!(
                            "Skip inactive broker election because replica info query returned no response, broker={}",
                            broker_name
                        );
                        false
                    }
                    Err(error) => {
                        warn!(
                            "Skip inactive broker election because replica info query errored, broker={}, error={}",
                            broker_name, error
                        );
                        false
                    }
                };

                if !should_elect {
                    info!(
                        "Inactive broker is not current master, skip election, cluster={:?}, broker={}, broker_id={}",
                        cluster_name, broker_name, inactive_broker_id
                    );
                    return;
                }
            }

            let request = ElectMasterRequestHeader::new(
                cluster_name.as_deref().unwrap_or_default(),
                broker_name.clone(),
                -1,
                false,
                current_millis(),
            );

            let config = controller_manager.controller_config();
            let max_retry = config.elect_master_max_retry_count;
            for attempt in 0..max_retry {
                let elect_result = tokio::time::timeout(
                    Duration::from_secs(3),
                    controller_manager.controller().elect_master(&request),
                )
                .await;

                match elect_result {
                    Ok(Ok(Some(response))) if response.code() == ResponseCode::Success as i32 => {
                        info!(
                            "Triggered controller-side elect-master after broker inactive, cluster={:?}, broker={}, \
                             broker_id={:?}, attempt={}",
                            cluster_name,
                            broker_name,
                            broker_id,
                            attempt + 1
                        );

                        if config.notify_broker_role_changed {
                            if let Err(error) = controller_manager.notify_broker_role_changed(response).await {
                                warn!(
                                    "Failed to notify brokers after role change, cluster={:?}, broker={}, error={}",
                                    cluster_name, broker_name, error
                                );
                            }
                        }
                        return;
                    }
                    Ok(Ok(Some(response))) => {
                        warn!(
                            "Elect-master after broker inactive did not succeed, cluster={:?}, broker={}, \
                             broker_id={:?}, attempt={}, code={}, remark={:?}",
                            cluster_name,
                            broker_name,
                            broker_id,
                            attempt + 1,
                            response.code(),
                            response.remark()
                        );
                    }
                    Ok(Ok(None)) => {
                        warn!(
                            "Elect-master after broker inactive returned no response, cluster={:?}, broker={}, \
                             broker_id={:?}, attempt={}",
                            cluster_name,
                            broker_name,
                            broker_id,
                            attempt + 1
                        );
                    }
                    Ok(Err(error)) => {
                        error!(
                            "Elect-master after broker inactive failed, cluster={:?}, broker={}, broker_id={:?}, \
                             attempt={}, error={}",
                            cluster_name,
                            broker_name,
                            broker_id,
                            attempt + 1,
                            error
                        );
                    }
                    Err(_) => {
                        warn!(
                            "Elect-master after broker inactive timed out, cluster={:?}, broker={}, broker_id={:?}, \
                             attempt={}",
                            cluster_name,
                            broker_name,
                            broker_id,
                            attempt + 1
                        );
                    }
                }

                if attempt + 1 < max_retry {
                    sleep(Duration::from_millis(100)).await;
                }
            }

            warn!(
                "Elect-master after broker inactive exhausted retries, cluster={:?}, broker={}, broker_id={:?}",
                cluster_name, broker_name, broker_id
            );
        }) {
            warn!(?error, "failed to spawn inactive broker handling task");
        }
    }
}

struct LeadershipGateState {
    /// Invariant: `applied_is_leader` changes only after its side effects succeed, and `stopping` blocks later promotion.
    applied_is_leader: bool,
    stopping: bool,
}

/// Main controller manager
///
/// This is the central component that coordinates all controller operations.
/// It manages:
/// - Raft consensus and the only authoritative metadata state machine
/// - Broker heartbeat monitoring
/// - Broker-facing remoting request processing
/// - Metrics collection (optional)
///
/// # Architecture
///
/// ```text
/// ControllerManager
///   +-- ControllerRequestProcessor
///   +-- RaftController
///   |     +-- OpenRaft consensus
///   |     +-- committed ReplicasInfoManager state
///   |     `-- RocksDB durable storage
///   +-- HeartbeatManager
///   `-- MetricsManager (optional)
/// ```
///
/// # Lifecycle
///
/// 1. **Creation**: `new()` - Initialize basic components
/// 2. **Initialization**: `initialize()` - Allocate resources, register listeners
/// 3. **Start**: `start()` - Start all components in correct order
/// 4. **Runtime**: Handle requests and monitor brokers through committed Raft state
/// 5. **Shutdown**: `shutdown()` - Gracefully stop all components in reverse order
///
/// # Thread Safety
///
/// All methods are thread-safe and can be called from multiple tasks concurrently.
/// Uses AtomicBool for state flags instead of RwLock to minimize lock contention.
pub struct ControllerManager {
    /// Configuration
    config: ControllerConfigHandle,

    /// Immutable wire defaults shared by all command producers owned by this Controller.
    command_factory: RemotingCommandFactory,

    /// Raft controller for consensus and leader election.
    /// Lifecycle mutation is synchronized inside the controller.
    raft_controller: Arc<RaftController>,

    /// Heartbeat manager for broker liveness detection
    /// Shared safely; lifecycle slots and listeners are synchronized internally.
    heartbeat_manager: Arc<DefaultBrokerHeartbeatManager>,

    /// Remoting server for inbound RPC requests
    remoting_server: Mutex<Option<TransportServer<ControllerRequestProcessor>>>,
    remoting_server_shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    manager_task_group: Arc<Mutex<Option<TaskGroup>>>,
    leadership_watch_tasks: Arc<Mutex<Option<ScheduledTaskGroup>>>,

    /// Runtime-neutral security capabilities supplied by the composition root.
    security: Option<ControllerSecurity>,

    /// Remoting client for outbound RPC calls
    remoting_client: Arc<RemotingClient>,

    /// Metrics manager (optional, enabled with "metrics" feature)
    #[cfg(feature = "metrics")]
    metrics_manager: Arc<ControllerMetricsManager>,

    /// Running state - uses AtomicBool for lock-free reads
    running: Arc<AtomicBool>,

    /// Initialization state - uses AtomicBool for lock-free reads
    initialized: Arc<AtomicBool>,

    /// A started controller consumes its one-shot remoting and task resources when it stops.
    lifecycle_terminated: Arc<AtomicBool>,

    /// Serializes initialize, start, and graceful shutdown transitions.
    lifecycle_lock: AsyncMutex<()>,

    /// Serializes leadership side effects and manual role-change notification submission.
    leadership_gate: AsyncMutex<LeadershipGateState>,

    #[cfg(test)]
    test_leadership_override: AtomicU8,

    broker_housekeeping_service: Mutex<Option<Arc<BrokerHousekeepingService>>>,
    broker_role_notifier: BrokerRoleNotifier,
    service_context: ChildServiceContext,
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
    /// - Configuration is invalid
    pub async fn new(
        config: ControllerConfig,
        service_context: ChildServiceContext,
        telemetry_handle: TelemetryHandle,
    ) -> Result<Self> {
        Self::new_with_remoting_command_factory(
            config,
            service_context,
            telemetry_handle,
            application_remoting_command_factory(),
        )
        .await
    }

    /// Creates a Controller manager with explicitly injected remoting defaults.
    ///
    /// # Errors
    ///
    /// Returns [`ControllerError`] when configuration validation or component
    /// initialization fails.
    pub async fn new_with_remoting_command_factory(
        config: ControllerConfig,
        service_context: ChildServiceContext,
        telemetry_handle: TelemetryHandle,
        command_factory: RemotingCommandFactory,
    ) -> Result<Self> {
        Self::new_with_security_and_remoting_command_factory(
            config,
            service_context,
            telemetry_handle,
            None,
            command_factory,
        )
        .await
    }

    /// Creates a Controller manager with an explicitly injected security boundary.
    ///
    /// Callers enabling authentication, authorization, or privileged maintenance
    /// must supply the concrete adapter here. The Controller crate does not choose
    /// or initialize a credential provider.
    ///
    /// # Errors
    ///
    /// Returns [`ControllerError`] when configuration is invalid, a required
    /// security capability is absent, or component initialization fails.
    pub async fn new_with_security(
        config: ControllerConfig,
        service_context: ChildServiceContext,
        telemetry_handle: TelemetryHandle,
        security: Option<ControllerSecurity>,
    ) -> Result<Self> {
        Self::new_with_security_and_remoting_command_factory(
            config,
            service_context,
            telemetry_handle,
            security,
            application_remoting_command_factory(),
        )
        .await
    }

    /// Creates a Controller manager with explicit security and wire-default owners.
    ///
    /// # Errors
    ///
    /// Returns [`ControllerError`] when configuration is invalid, an enabled
    /// security capability is missing, or component initialization fails.
    pub async fn new_with_security_and_remoting_command_factory(
        config: ControllerConfig,
        service_context: ChildServiceContext,
        telemetry_handle: TelemetryHandle,
        security: Option<ControllerSecurity>,
        command_factory: RemotingCommandFactory,
    ) -> Result<Self> {
        config.validate().map_err(ControllerError::ConfigError)?;
        let security_enabled =
            config.authentication_enabled || config.authorization_enabled || config.maintenance_enabled;
        if security_enabled && security.is_none() {
            return Err(ControllerError::ConfigError(
                "Controller security is enabled but no ControllerSecurity adapter was injected".to_string(),
            ));
        }
        if config.maintenance_enabled
            && security
                .as_ref()
                .and_then(ControllerSecurity::maintenance_authorizer)
                .is_none()
        {
            return Err(ControllerError::ConfigError(
                "Controller maintenance is enabled but no validated maintenance authorizer was injected".to_string(),
            ));
        }
        let config = ControllerConfigHandle::new(config);
        let config_snapshot = config.snapshot();
        info!(
            node_id = config_snapshot.node_id,
            listen_port = config_snapshot.listen_addr.port(),
            raft_peer_count = config_snapshot.raft_peers.len(),
            security_enabled,
            "Creating controller manager"
        );

        // Initialize heartbeat manager
        let heartbeat_manager = Arc::new(DefaultBrokerHeartbeatManager::new(
            config.reader(),
            service_context.component("controller.heartbeat").task_group().clone(),
        ));

        #[cfg(feature = "metrics")]
        let metrics_manager = {
            info!("Initializing metrics manager");
            let active_broker_heartbeat_manager = heartbeat_manager.clone();
            ControllerMetricsManager::new_with_active_broker_source(config.reader(), &telemetry_handle, move || {
                active_broker_count_from_snapshot(&active_broker_heartbeat_manager.get_active_brokers_num())
            })
        };
        #[cfg(not(feature = "metrics"))]
        let metrics_manager = ControllerMetricsManager::new(config.reader(), &telemetry_handle);

        // Initialize RocketMQ runtime for Raft controller
        //let runtime = Arc::new(RocketMQRuntime::new_multi(2, "controller-runtime"));

        // Initialize Raft controller for leader election.
        // The controller and request processor must share the same heartbeat manager so that
        // liveness-aware paths observe the broker heartbeats recorded by RPC handlers.
        let raft_arc = Arc::new(
            RaftController::new_open_raft_with_heartbeat_metrics_and_remoting_command_factory(
                config.reader(),
                heartbeat_manager.clone(),
                service_context.component("controller.openraft"),
                metrics_manager.clone(),
                command_factory,
            ),
        );

        // Initialize remoting server for inbound requests
        let listen_port = config.snapshot().listen_addr.port() as u32;

        let server_config = ServerConfig {
            listen_port,
            ..Default::default()
        };
        #[cfg(any(feature = "metrics", feature = "otel-traces"))]
        let transport_telemetry = TransportTelemetry::from_handle(&telemetry_handle);
        #[cfg(not(any(feature = "metrics", feature = "otel-traces")))]
        let transport_telemetry = TransportTelemetry::noop();
        let remoting_server = Some(TransportServer::new_with_telemetry(
            Arc::new(server_config),
            service_context.component("controller.remoting-server"),
            transport_telemetry.clone(),
        ));
        info!("Remoting server created on port {}", listen_port);

        // Initialize remoting client for outbound RPC
        let client_config = TransportClientConfig::default();
        let remoting_client = Arc::new(
            RemotingClient::builder(
                Arc::new(client_config),
                DefaultRequestProcessor,
                service_context.component("controller.remoting-client"),
            )
            .telemetry(transport_telemetry)
            .build()
            .map_err(|error| ControllerError::runtime_error(format!("Failed to build remoting client: {error}")))?,
        );
        let notify_retry_base_delay = Duration::from_millis(config.snapshot().heartbeat_interval_ms.max(100));
        let broker_role_notifier = BrokerRoleNotifier::new(
            remoting_client.transport_client(),
            notify_retry_base_delay,
            command_factory,
        );
        info!("Remoting client created");

        info!("Controller manager created successfully");

        Ok(Self {
            config,
            command_factory,
            raft_controller: raft_arc,
            heartbeat_manager,
            remoting_server: Mutex::new(remoting_server),
            remoting_server_shutdown_tx: Arc::new(Mutex::new(None)),
            manager_task_group: Arc::new(Mutex::new(None)),
            leadership_watch_tasks: Arc::new(Mutex::new(None)),
            security,
            remoting_client,
            #[cfg(feature = "metrics")]
            metrics_manager,
            running: Arc::new(AtomicBool::new(false)),
            initialized: Arc::new(AtomicBool::new(false)),
            lifecycle_terminated: Arc::new(AtomicBool::new(false)),
            lifecycle_lock: AsyncMutex::new(()),
            leadership_gate: AsyncMutex::new(LeadershipGateState {
                applied_is_leader: false,
                stopping: false,
            }),
            #[cfg(test)]
            test_leadership_override: AtomicU8::new(0),
            broker_housekeeping_service: Mutex::new(None),
            broker_role_notifier,
            service_context,
        })
    }

    fn ensure_manager_task_group(&self) -> Result<TaskGroup> {
        let mut guard = self.manager_task_group.lock();
        if let Some(task_group) = guard.as_ref() {
            return Ok(task_group.clone());
        }

        let task_group = self
            .service_context
            .component("rocketmq-controller.manager")
            .task_group()
            .clone();
        *guard = Some(task_group.clone());
        Ok(task_group)
    }

    fn manager_task_group(&self) -> Option<TaskGroup> {
        self.manager_task_group.lock().clone()
    }

    async fn shutdown_manager_tasks(&self, deadline: ShutdownDeadline) -> bool {
        self.leadership_watch_tasks.lock().take();
        let task_group = self.manager_task_group.lock().take();
        let Some(task_group) = task_group else {
            return true;
        };

        let report = task_group.shutdown_until(deadline).await;
        if !report.is_healthy() {
            warn!(
                report = %report.to_json(),
                "Controller manager task shutdown report is unhealthy"
            );
        }
        report.is_healthy()
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
    pub async fn initialize(self: &Arc<Self>) -> Result<bool> {
        let _lifecycle_guard = self.lifecycle_lock.lock().await;
        // Check if already initialized using atomic operation
        if self.initialized.load(Ordering::Acquire) {
            warn!("Controller manager is already initialized");
            return Ok(false);
        }

        info!("Initializing controller manager...");

        // Initialize heartbeat manager
        {
            self.heartbeat_manager.initialize_shared();
            info!("Heartbeat manager initialized");
        }

        // Register broker lifecycle listeners
        {
            let inactive_listener = Arc::new(BrokerInactiveListener::new(Arc::downgrade(self)));
            self.heartbeat_manager
                .register_broker_lifecycle_listener_shared(inactive_listener.clone());
            self.raft_controller
                .register_broker_lifecycle_listener(inactive_listener);
            info!("Broker inactive listener registered");
        }

        // Initialize broker housekeeping service
        {
            let housekeeping_service =
                Arc::new(BrokerHousekeepingService::new_with_controller_manager(Arc::clone(self)));
            *self.broker_housekeeping_service.lock() = Some(housekeeping_service);

            info!("Broker housekeeping service initialized");
        }

        // Initialize processor manager (processors are already registered in new())
        info!("Processor manager initialized with built-in processors");

        // Register request processors to remoting server
        self.register_processor();
        info!("Request processors registered to remoting server");

        // Metrics manager is already initialized from the injected telemetry handle in new().
        #[cfg(feature = "metrics")]
        info!("Metrics manager is ready");

        self.initialized.store(true, Ordering::Release);
        info!("Controller manager initialized successfully");
        Ok(true)
    }

    /// Register request processors to the remoting server
    fn register_processor(&self) {
        // Current implementation note:
        // The remoting_server is started with a DefaultRequestProcessor in start().
        // Once ControllerRequestProcessor is fully implemented and TransportServer
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
    fn init_processors(controller_manager: Arc<ControllerManager>) -> ControllerRequestProcessor {
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
    /// - A previously started controller has already shut down or rolled back a failed start
    /// - Any component fails to start
    ///
    /// # Thread Safety
    ///
    /// Repeated calls while the controller is running are idempotent. Once a started controller
    /// shuts down or rolls back a failed start, this method returns an error instead of attempting
    /// to recreate its consumed resources.
    pub async fn start(self: &Arc<Self>) -> Result<()> {
        let _lifecycle_guard = self.lifecycle_lock.lock().await;
        if self.running.load(Ordering::Acquire) {
            warn!("Controller manager is already running");
            return Ok(());
        }

        if self.lifecycle_terminated.load(Ordering::Acquire) {
            return Err(ControllerError::runtime_error(
                "Controller manager cannot be restarted after shutdown or a failed startup",
            ));
        }

        // Check if initialized
        if !self.initialized.load(Ordering::SeqCst) {
            return Err(ControllerError::NotInitialized(
                "Controller manager must be initialized before starting".to_string(),
            ));
        }

        if self
            .running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            warn!("Controller manager is already running");
            return Ok(());
        }

        info!("Starting controller manager...");

        // Start Raft controller first (critical for leader election)
        if let Err(e) = self.raft_controller.startup_shared().await {
            self.running.store(false, Ordering::SeqCst);
            return Err(self
                .cleanup_after_start_failure(ControllerError::runtime_error(format!(
                    "Failed to start Raft controller: {e}"
                )))
                .await);
        }
        info!("Raft controller started");

        // Start heartbeat manager (for broker monitoring)
        {
            self.heartbeat_manager.start_shared();
            info!("Heartbeat manager started");
        }

        let manager_task_group = match self.ensure_manager_task_group() {
            Ok(task_group) => task_group,
            Err(error) => return Err(self.cleanup_after_start_failure(error).await),
        };

        // Start remoting server (for inbound RPC requests)
        // Reference: NameServerRuntime.start() - register processors then start server
        let remoting_server = self.remoting_server.lock().take();
        if let Some(mut server) = remoting_server {
            // Create ControllerRequestProcessor using init_processors()
            let request_processor = Self::init_processors(Arc::clone(self));
            let broker_housekeeping_service = self
                .broker_housekeeping_service
                .lock()
                .take()
                .map(|service| service as Arc<dyn ChannelEventListener>);
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            *self.remoting_server_shutdown_tx.lock() = Some(shutdown_tx);
            let (startup_tx, startup_rx) = oneshot::channel();
            if let Err(error) = manager_task_group.spawn_service("controller.remoting-server", async move {
                let report = server
                    .try_run_with_shutdown_report_and_startup(
                        request_processor,
                        broker_housekeeping_service,
                        async move {
                            let _ = shutdown_rx.await;
                        },
                        startup_tx,
                    )
                    .await;
                match report.as_ref() {
                    Ok(report) if !report.is_healthy() => {
                        warn!(
                            report = %report.to_json(),
                            "Controller remoting server shutdown report is unhealthy"
                        );
                    }
                    Err(error) => warn!(%error, "Controller remoting server stopped before startup completed"),
                    _ => {}
                }
            }) {
                let error =
                    ControllerError::runtime_error(format!("Failed to spawn controller remoting server task: {error}"));
                return Err(self.cleanup_after_start_failure(error).await);
            }
            match startup_rx.await {
                Ok(Ok(_address)) => info!("Remoting server started with ControllerRequestProcessor"),
                Ok(Err(error)) => {
                    return Err(self
                        .cleanup_after_start_failure(ControllerError::runtime_error(format!(
                            "Controller remoting server failed to start: {error}"
                        )))
                        .await);
                }
                Err(error) => {
                    return Err(self
                        .cleanup_after_start_failure(ControllerError::runtime_error(format!(
                            "Controller remoting server startup acknowledgement was dropped: {error}"
                        )))
                        .await);
                }
            }
        }

        // Start remoting client (for outbound RPC calls)
        {
            if let Err(error) = self.remoting_client.start().await {
                let error = ControllerError::runtime_error(format!("Failed to start remoting client: {error}"));
                return Err(self.cleanup_after_start_failure(error).await);
            }
            info!("Remoting client started");
        }

        if let Err(error) = self
            .start_broker_role_notifier_and_synchronize(&manager_task_group)
            .await
        {
            return Err(self.cleanup_after_start_failure(error).await);
        }
        if let Err(error) = self.start_leadership_watch_loop().await {
            return Err(self.cleanup_after_start_failure(error).await);
        }

        // Metrics are already running if enabled
        #[cfg(feature = "metrics")]
        info!("Metrics manager is already running (singleton)");

        info!("Controller manager started successfully");
        Ok(())
    }

    /// Rolls back a partial start while the caller owns `lifecycle_lock`.
    async fn cleanup_after_start_failure(&self, start_error: ControllerError) -> ControllerError {
        self.running.store(true, Ordering::Release);
        let deadline = ShutdownDeadline::after(Duration::from_secs(30));
        let cleanup = tokio::time::timeout(deadline.remaining(), self.shutdown_inner(deadline)).await;

        match cleanup {
            Ok(Ok(())) => start_error,
            Ok(Err(cleanup_error)) => ControllerError::runtime_error(format!(
                "Controller startup failed: {start_error}; startup cleanup was unhealthy: {cleanup_error}"
            )),
            Err(_) => ControllerError::runtime_error(format!(
                "Controller startup failed: {start_error}; startup cleanup exhausted its absolute deadline"
            )),
        }
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
        self.shutdown_until(ShutdownDeadline::after(Duration::from_secs(30)))
            .await
    }

    /// Shuts down the Controller without extending the process-level absolute deadline.
    ///
    /// # Errors
    ///
    /// Returns a typed runtime error when the deadline expires or a shutdown phase fails.
    pub async fn shutdown_until(&self, deadline: ShutdownDeadline) -> Result<()> {
        let shutdown = async {
            let _lifecycle_guard = self.lifecycle_lock.lock().await;
            self.shutdown_inner(deadline).await
        };
        match tokio::time::timeout(deadline.remaining(), shutdown).await {
            Ok(result) => result,
            Err(_) => Err(ControllerError::runtime_error(
                "Controller shutdown exhausted its absolute deadline",
            )),
        }
    }

    async fn shutdown_inner(&self, deadline: ShutdownDeadline) -> Result<()> {
        // Check if already stopped using atomic operation
        if self
            .running
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            warn!("Controller manager is not running");
            return Ok(());
        }
        self.lifecycle_terminated.store(true, Ordering::Release);
        info!("Shutting down controller manager...");
        let mut failures = Vec::new();

        if let Err(error) = self.stop_leadership_gate().await {
            warn!("Failed to stop leader-only scheduling during shutdown: {}", error);
            failures.push(format!("leadership scheduling: {error}"));
        }
        self.broker_role_notifier.close();
        if let Some(shutdown_tx) = self.remoting_server_shutdown_tx.lock().take() {
            let _ = shutdown_tx.send(());
        }
        let heartbeat_report = self.heartbeat_manager.shutdown_gracefully_until(deadline).await;
        if heartbeat_report.is_healthy() {
            info!("Heartbeat manager shut down");
        } else {
            let detail = heartbeat_report.to_json();
            warn!(report = %detail, "Heartbeat manager shutdown was unhealthy");
            failures.push(format!("heartbeat manager: {detail}"));
        }

        if !self.shutdown_manager_tasks(deadline).await {
            failures.push("manager tasks did not stop cleanly".to_string());
        }

        if let Some(security) = &self.security {
            match tokio::time::timeout(deadline.remaining(), security.authenticator().shutdown()).await {
                Ok(Ok(())) => info!("Controller security adapter shut down"),
                Ok(Err(error)) => {
                    warn!(%error, "Controller security adapter shutdown failed");
                    failures.push(format!("security adapter: {error}"));
                }
                Err(_) => {
                    warn!("Timed out waiting for Controller security adapter shutdown");
                    failures.push("security adapter shutdown timed out".to_string());
                }
            }
        }

        // Shutdown remoting client
        {
            let report = self.remoting_client.shutdown_with_report(deadline.remaining()).await;
            if report.is_healthy() {
                info!("Remoting client shut down");
            } else {
                let detail = serde_json::to_string(&report)
                    .unwrap_or_else(|error| format!("failed to serialize remoting shutdown report: {error}"));
                warn!(report = %detail, "Remoting client shutdown was unhealthy");
                failures.push(format!("remoting client: {detail}"));
            }
        }

        // Shutdown Raft controller last (it coordinates distributed operations)
        match tokio::time::timeout(
            deadline.remaining().min(Duration::from_secs(10)),
            self.raft_controller.shutdown_shared(),
        )
        .await
        {
            Ok(Ok(())) => info!("Raft controller shut down"),
            Ok(Err(e)) => {
                error!("Failed to shutdown Raft: {}", e);
                failures.push(format!("Raft: {e}"));
            }
            Err(_) => {
                warn!("Timed out waiting for Raft controller shutdown");
                failures.push("Raft shutdown timed out".to_string());
            }
        }

        // Metrics manager cleanup is automatic via Drop
        #[cfg(feature = "metrics")]
        info!("Metrics manager will be cleaned up automatically");

        if failures.is_empty() {
            info!("Controller manager shut down successfully");
            Ok(())
        } else {
            Err(ControllerError::runtime_error(format!(
                "Controller shutdown completed with unhealthy phases: {}",
                failures.join("; ")
            )))
        }
    }

    /// Check if this node is the leader
    ///
    /// # Returns
    ///
    /// true if this node is the Raft leader, false otherwise
    pub fn is_leader(&self) -> bool {
        #[cfg(test)]
        match self.test_leadership_override.load(Ordering::Acquire) {
            1 => return false,
            2 => return true,
            _ => {}
        }
        self.raft_controller.is_leader()
    }

    #[cfg(test)]
    fn set_test_leadership_override(&self, is_leader: Option<bool>) {
        self.test_leadership_override.store(
            match is_leader {
                None => 0,
                Some(false) => 1,
                Some(true) => 2,
            },
            Ordering::Release,
        );
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

    /// Get the configuration
    ///
    /// # Returns
    ///
    /// A reference to the controller configuration
    pub fn config(&self) -> Arc<ControllerConfig> {
        self.config.snapshot()
    }

    /// Returns the immutable command factory owned by this Controller instance.
    pub fn remoting_command_factory(&self) -> RemotingCommandFactory {
        self.command_factory
    }

    /// Returns the security boundary injected by the composition root.
    pub fn security(&self) -> Option<&ControllerSecurity> {
        self.security.as_ref()
    }

    pub(crate) async fn update_config(
        &self,
        properties: HashMap<String, String>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.config.update(properties).await
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
    pub fn controller_config(&self) -> Arc<ControllerConfig> {
        self.config.snapshot()
    }

    /// Get the heartbeat manager
    ///
    /// # Returns
    ///
    /// A shared reference to the internally synchronized heartbeat manager.
    pub fn heartbeat_manager(&self) -> &Arc<DefaultBrokerHeartbeatManager> {
        &self.heartbeat_manager
    }

    /// Get the remoting client
    ///
    /// # Returns
    ///
    /// A clone of the Arc-wrapped remoting client for making outbound RPC calls
    pub fn remoting_client(&self) -> Arc<RemotingClient> {
        self.remoting_client.clone()
    }

    pub fn controller(&self) -> &Arc<RaftController> {
        &self.raft_controller
    }

    pub(crate) fn scheduling_enabled(&self) -> bool {
        self.raft_controller.scheduling_enabled()
    }

    pub(crate) fn leadership_watch_task_count(&self) -> usize {
        self.leadership_watch_tasks
            .lock()
            .as_ref()
            .map(|scheduled_tasks| scheduled_tasks.group().task_count())
            .unwrap_or_default()
    }

    pub(crate) fn leadership_watch_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.leadership_watch_tasks
            .lock()
            .as_ref()
            .map(ScheduledTaskGroup::snapshot)
            .unwrap_or_default()
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

    async fn start_leadership_watch_loop(self: &Arc<Self>) -> Result<()> {
        let weak_manager = Arc::downgrade(self);
        let interval = Duration::from_millis(self.config.snapshot().heartbeat_interval_ms.max(100));
        let task_group = self.ensure_manager_task_group()?;
        self.synchronize_leadership_gate().await?;
        let scheduled_tasks = ScheduledTaskGroup::new(task_group.clone());
        let task_config = ScheduledTaskConfig::fixed_delay("controller.leadership-watch", interval);

        scheduled_tasks
            .schedule_fixed_delay(task_config, move || {
                let weak_manager = weak_manager.clone();
                async move {
                    let Some(manager) = weak_manager.upgrade() else {
                        return;
                    };

                    if !manager.is_running() {
                        return;
                    }

                    if let Err(error) = manager.synchronize_leadership_gate().await {
                        warn!("Failed to apply leadership state transition: {}", error);
                    }
                }
            })
            .map_err(|error| {
                ControllerError::runtime_error(format!("Failed to schedule leadership watch task: {error}"))
            })?;

        *self.leadership_watch_tasks.lock() = Some(scheduled_tasks);
        Ok(())
    }

    async fn synchronize_leadership_gate(&self) -> Result<bool> {
        let mut gate = self.leadership_gate.lock().await;
        self.synchronize_leadership_gate_locked(&mut gate).await
    }

    async fn start_broker_role_notifier_and_synchronize(&self, task_group: &TaskGroup) -> Result<()> {
        let mut gate = self.leadership_gate.lock().await;
        if gate.stopping {
            return Err(ControllerError::runtime_error(
                "Controller leadership gate cannot be started after shutdown",
            ));
        }

        self.broker_role_notifier.start(task_group)?;
        let is_leader = self.is_leader();
        self.apply_leadership_state(is_leader).await?;
        gate.applied_is_leader = is_leader;
        Ok(())
    }

    async fn synchronize_leadership_gate_locked(&self, gate: &mut LeadershipGateState) -> Result<bool> {
        if gate.stopping {
            return Ok(false);
        }

        let is_leader = self.is_leader();
        if is_leader != gate.applied_is_leader {
            self.apply_leadership_state(is_leader).await?;
            gate.applied_is_leader = is_leader;
        }
        Ok(is_leader)
    }

    async fn stop_leadership_gate(&self) -> Result<()> {
        let mut gate = self.leadership_gate.lock().await;
        gate.stopping = true;
        self.apply_leadership_state(false).await?;
        gate.applied_is_leader = false;
        Ok(())
    }

    async fn apply_leadership_state(&self, is_leader: bool) -> Result<()> {
        if is_leader {
            self.raft_controller.start_scheduling().await.map_err(|error| {
                ControllerError::runtime_error(format!("Failed to start controller scheduling: {error}"))
            })?;
            self.broker_role_notifier.enable();
            info!(
                "Leader-only scheduling enabled on controller {}",
                self.config.snapshot().node_id
            );
        } else {
            self.raft_controller.stop_scheduling().await.map_err(|error| {
                ControllerError::runtime_error(format!("Failed to stop controller scheduling: {error}"))
            })?;
            self.broker_role_notifier.reset();
            info!(
                "Leader-only scheduling disabled and notify dispatch state cleared on controller {}",
                self.config.snapshot().node_id
            );
        }
        Ok(())
    }

    pub(crate) fn broker_role_notifier_snapshot(&self) -> NotifySnapshot {
        self.broker_role_notifier.snapshot()
    }

    pub async fn notify_broker_role_changed(&self, mut response: RemotingCommand) -> Result<()> {
        response.make_custom_header_to_net();
        let response_header = response
            .decode_command_custom_header::<ElectMasterResponseHeader>()
            .map_err(|error| {
                ControllerError::serialization_source(
                    "decode elect-master response header for broker role notify",
                    error,
                )
            })?;

        let Some(body) = response.body() else {
            return Ok(());
        };

        let response_body = ElectMasterResponseBody::decode(body).map_err(|error| {
            ControllerError::serialization_source("decode elect-master response body for broker role notify", error)
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

        let Some(master_epoch) = response_header.master_epoch else {
            warn!(broker = %member_group.broker_name, "Skip broker role notify because master epoch is absent");
            return Ok(());
        };
        let Ok(master_epoch) = rocketmq_store_api::MasterEpoch::try_from(master_epoch) else {
            warn!(broker = %member_group.broker_name, master_epoch, "Skip broker role notify because master epoch is invalid");
            return Ok(());
        };
        let Some(sync_state_set_epoch) = response_header.sync_state_set_epoch else {
            warn!(broker = %member_group.broker_name, "Skip broker role notify because sync-state-set epoch is absent");
            return Ok(());
        };
        let Ok(sync_state_set_epoch) = rocketmq_store_api::SyncStateSetEpoch::try_from(sync_state_set_epoch) else {
            warn!(broker = %member_group.broker_name, sync_state_set_epoch, "Skip broker role notify because sync-state-set epoch is invalid");
            return Ok(());
        };
        let master_address = response_header.master_address.clone().map(|value| value.to_string());
        let sync_state_set = SyncStateSet::with_values(response_body.sync_state_set, sync_state_set_epoch.get())
            .encode()
            .map_err(|error| {
                ControllerError::serialization_source("encode sync state set for broker role notify", error)
            })?;

        let mut tasks = Vec::new();
        for (broker_id, broker_addr) in member_group.broker_addrs {
            if !self.heartbeat_manager.is_broker_active(
                &member_group.cluster,
                &member_group.broker_name,
                broker_id as i64,
            ) {
                continue;
            }

            let key = NotifyKey {
                cluster_name: member_group.cluster.to_string(),
                broker_name: member_group.broker_name.to_string(),
                broker_id,
            };
            let state = match NotifyState::try_new(
                master_broker_id,
                master_epoch,
                sync_state_set_epoch,
                master_address.clone(),
            ) {
                Ok(state) => state,
                Err(error) => {
                    warn!(%error, broker = %member_group.broker_name, "Skip broker role notify because authority is invalid");
                    return Ok(());
                }
            };
            tasks.push(NotifyTask::new(
                key,
                state,
                broker_addr.clone(),
                response_header.master_address.clone(),
                sync_state_set.clone(),
            ));
        }

        self.submit_broker_role_notifications(tasks).await
    }

    async fn submit_broker_role_notifications<I>(&self, tasks: I) -> Result<()>
    where
        I: IntoIterator<Item = NotifyTask>,
    {
        let mut leadership_gate = self.leadership_gate.lock().await;
        if !self.synchronize_leadership_gate_locked(&mut leadership_gate).await? {
            return Ok(());
        }

        for task in tasks {
            let broker_id = task.key.broker_id;
            let broker_name = task.key.broker_name.clone();
            let broker_addr = task.broker_addr.clone();
            let outcome = self.broker_role_notifier.submit(task);
            if matches!(outcome, SubmitOutcome::Full | SubmitOutcome::Closed) {
                warn!(
                    ?outcome,
                    target = %broker_addr,
                    broker_id,
                    broker = %broker_name,
                    "Broker role notify was not retained"
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

            self.running.store(false, Ordering::SeqCst);
            if let Some(shutdown_tx) = self.remoting_server_shutdown_tx.lock().take() {
                let _ = shutdown_tx.send(());
            }
            self.broker_role_notifier.close();
            self.heartbeat_manager.shutdown_shared();
            self.remoting_client.shutdown();
            self.leadership_watch_tasks.lock().take();
            if let Some(task_group) = self.manager_task_group.lock().take() {
                task_group.cancel();
            }

            info!("Emergency shutdown completed");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::HashSet;
    use std::net::SocketAddr;

    use super::*;

    #[test]
    fn controller_manager_does_not_log_the_full_configuration() {
        let source = include_str!("controller_manager.rs");
        let full_config_log = ["Creating controller manager with config: ", "{:?}"].concat();

        assert!(!source.contains(&full_config_log));
    }
    use crate::typ::Node;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::protocol::body::sync_state_set_body::SyncStateSet;
    use rocketmq_protocol::protocol::header::controller::alter_sync_state_set_request_header::AlterSyncStateSetRequestHeader;
    use rocketmq_protocol::protocol::header::controller::apply_broker_id_request_header::ApplyBrokerIdRequestHeader;
    use rocketmq_protocol::protocol::header::controller::register_broker_to_controller_request_header::RegisterBrokerToControllerRequestHeader;
    use rocketmq_protocol::protocol::header::namesrv::broker_request::BrokerHeartbeatRequestHeader;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_protocol::protocol::remoting_command_defaults::{RemotingCommandDefaults, RemotingCommandFactory};
    use rocketmq_protocol::protocol::SerializeType;
    use rocketmq_transport::api::v1::Channel;
    use rocketmq_transport::api::v1::ConnectionHandlerContextWrapper;
    use rocketmq_transport::api::v1::RequestProcessor;
    use rocketmq_transport::test_support::Connection;

    fn test_telemetry_handle() -> TelemetryHandle {
        TelemetryHandle::noop()
    }

    async fn wait_until<F>(timeout: Duration, mut predicate: F, context: &str)
    where
        F: FnMut() -> bool,
    {
        let start = current_millis();
        loop {
            if predicate() {
                return;
            }
            assert!(
                current_millis().saturating_sub(start) < timeout.as_millis() as u64,
                "timed out waiting for {context}"
            );
            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn create_test_channel() -> Channel {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local test listener");
        let local_addr = listener.local_addr().expect("local listener addr");
        let std_stream = std::net::TcpStream::connect(local_addr).expect("connect local test listener");
        std_stream.set_nonblocking(true).expect("set nonblocking");
        drop(listener);
        let tcp_stream = tokio::net::TcpStream::from_std(std_stream).expect("convert tcp stream");
        let connection = Connection::new(tcp_stream);
        rocketmq_transport::test_support::TestChannelBuilder::new(
            connection,
            test_service_context().component("test-channel").task_group().clone(),
        )
        .addresses(local_addr, local_addr)
        .build()
        .expect("build test channel")
    }

    fn reserve_controller_addresses() -> (SocketAddr, SocketAddr) {
        let remoting = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve remoting address");
        let raft = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve raft address");
        let addresses = (
            remoting.local_addr().expect("remoting address"),
            raft.local_addr().expect("raft address"),
        );
        drop((remoting, raft));
        addresses
    }

    fn test_service_context() -> ChildServiceContext {
        rocketmq_runtime::RuntimeContext::from_current("controller-manager-test").service_context("controller-manager")
    }

    fn test_notify_task(broker_id: u64) -> NotifyTask {
        let state = NotifyState::try_new(
            1,
            rocketmq_store_api::MasterEpoch::try_from(1).expect("test master epoch"),
            rocketmq_store_api::SyncStateSetEpoch::try_from(1).expect("test sync-state-set epoch"),
            Some("127.0.0.1:10911".to_string()),
        )
        .expect("test notify state");
        NotifyTask::new(
            NotifyKey {
                cluster_name: "test-cluster".to_string(),
                broker_name: "broker-a".to_string(),
                broker_id,
            },
            state,
            CheetahString::from_static_str("127.0.0.1:10911"),
            Some(CheetahString::from_static_str("127.0.0.1:10911")),
            Vec::new(),
        )
    }

    #[tokio::test]
    async fn inactive_broker_worker_observes_manager_cancellation() {
        let task_group = test_service_context()
            .component("inactive-broker-worker-cancellation")
            .task_group()
            .clone();
        let started = Arc::new(tokio::sync::Notify::new());
        let started_task = started.clone();

        spawn_inactive_broker_worker(&task_group, async move {
            started_task.notify_one();
            std::future::pending::<()>().await;
        })
        .expect("spawn inactive broker worker");
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("inactive broker worker should start");

        let report = task_group.shutdown(Duration::from_secs(1)).await;

        assert!(report.is_healthy(), "shutdown report: {}", report.to_json());
        assert_eq!(report.cancelled, 1);
        assert_eq!(task_group.task_count(), 0);
    }

    #[tokio::test]
    async fn manager_retains_its_injected_remoting_command_factory() {
        let factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(667, SerializeType::ROCKETMQ));
        let config = ControllerConfig::default().with_node_info(1, reserve_controller_addresses().0);

        let manager = ControllerManager::new_with_security_and_remoting_command_factory(
            config,
            test_service_context(),
            test_telemetry_handle(),
            None,
            factory,
        )
        .await
        .expect("create manager with explicit command factory");

        assert_eq!(manager.remoting_command_factory(), factory);
        let response = manager
            .controller()
            .get_controller_metadata()
            .await
            .expect("query unstarted Controller")
            .expect("unstarted Controller response");
        assert_eq!(response.version(), 667);
        assert_eq!(response.serialize_type(), SerializeType::ROCKETMQ);
    }

    #[tokio::test]
    async fn request_processor_uses_its_manager_command_factory() {
        let binary_factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(668, SerializeType::ROCKETMQ));
        let json_factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(669, SerializeType::JSON));
        let binary_manager = Arc::new(
            ControllerManager::new_with_remoting_command_factory(
                ControllerConfig::default().with_node_info(1, reserve_controller_addresses().0),
                test_service_context(),
                test_telemetry_handle(),
                binary_factory,
            )
            .await
            .expect("create binary manager"),
        );
        let json_manager = Arc::new(
            ControllerManager::new_with_remoting_command_factory(
                ControllerConfig::default().with_node_info(2, reserve_controller_addresses().0),
                test_service_context(),
                test_telemetry_handle(),
                json_factory,
            )
            .await
            .expect("create JSON manager"),
        );

        async fn unsupported_response(manager: Arc<ControllerManager>) -> RemotingCommand {
            let mut processor = ControllerRequestProcessor::new(manager);
            let channel = create_test_channel().await;
            let ctx = Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
            let mut request = RemotingCommand::create_remoting_command(-12345);
            processor
                .process_request(channel, ctx, &mut request)
                .await
                .expect("unsupported request dispatch")
                .expect("unsupported request response")
        }

        let binary = unsupported_response(binary_manager).await;
        let json = unsupported_response(json_manager).await;

        assert_eq!(binary.code(), ResponseCode::RequestCodeNotSupported as i32);
        assert_eq!(binary.version(), 668);
        assert_eq!(binary.serialize_type(), SerializeType::ROCKETMQ);
        assert_eq!(json.code(), ResponseCode::RequestCodeNotSupported as i32);
        assert_eq!(json.version(), 669);
        assert_eq!(json.serialize_type(), SerializeType::JSON);
    }

    #[tokio::test]
    async fn test_manager_lifecycle() {
        let config = ControllerConfig::default().with_node_info(1, "127.0.0.1:9878".parse::<SocketAddr>().unwrap());

        let manager = ControllerManager::new(config, test_service_context(), test_telemetry_handle())
            .await
            .expect("Failed to create manager");
        let manager_arc = Arc::new(manager);

        // Test initialization state (should use non-async is_initialized now)
        assert!(!manager_arc.is_initialized());
        assert!(manager_arc.initialize().await.expect("Failed to initialize"));
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
    async fn enabled_security_requires_an_injected_adapter() {
        let mut config = ControllerConfig::default().with_node_info(1, reserve_controller_addresses().0);
        config.authentication_enabled = true;

        let error = match ControllerManager::new(config, test_service_context(), test_telemetry_handle()).await {
            Ok(_) => panic!("security-enabled Controller must fail closed without an adapter"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("no ControllerSecurity adapter was injected"));
    }

    #[tokio::test]
    async fn concurrent_initialize_is_serialized_and_manager_handles_do_not_form_a_cycle() {
        let config = ControllerConfig::default().with_node_info(1, reserve_controller_addresses().0);
        let manager = Arc::new(
            ControllerManager::new(config, test_service_context(), test_telemetry_handle())
                .await
                .expect("create manager"),
        );

        let (first, second) = tokio::join!(manager.initialize(), manager.initialize());
        let results = [first.expect("first initialize"), second.expect("second initialize")];
        assert_eq!(results.into_iter().filter(|initialized| *initialized).count(), 1);
        assert!(manager.broker_housekeeping_service.lock().is_some());

        let processor = Arc::new(ControllerRequestProcessor::new(manager.clone()));
        let wrapper =
            crate::processor::ControllerRequestProcessorWrapper::ControllerRequestProcessor(processor.clone());
        let wrapper_clone = wrapper.clone();
        assert_eq!(Arc::strong_count(&processor), 3);
        let weak_manager = Arc::downgrade(&manager);
        drop(manager);

        assert!(weak_manager.upgrade().is_none());
        drop(wrapper_clone);
        drop(wrapper);
        drop(processor);
    }

    #[tokio::test]
    async fn concurrent_start_waits_for_the_single_lifecycle_transition() {
        let (remoting_addr, raft_addr) = reserve_controller_addresses();
        let config = ControllerConfig::default()
            .with_node_info(1, remoting_addr)
            .with_raft_peers(vec![crate::config::RaftPeer { id: 1, addr: raft_addr }])
            .with_storage_backend(crate::config::StorageBackendType::Memory);
        let manager = Arc::new(
            ControllerManager::new(config, test_service_context(), test_telemetry_handle())
                .await
                .expect("create manager"),
        );
        manager.initialize().await.expect("initialize manager");

        let (first, second) = tokio::join!(manager.start(), manager.start());
        first.expect("first start");
        second.expect("second start");
        assert!(manager.is_running());

        manager.shutdown().await.expect("shutdown manager");
        assert!(!manager.is_running());
        let restart_error = manager
            .start()
            .await
            .expect_err("a stopped controller must not restart");
        assert_eq!(
            restart_error.to_string(),
            "Runtime error: Controller manager cannot be restarted after shutdown or a failed startup"
        );
        std::mem::forget(manager);
    }

    #[tokio::test]
    async fn startup_failure_cleanup_stops_owned_components() {
        let (remoting_addr, raft_addr) = reserve_controller_addresses();
        let config = ControllerConfig::default()
            .with_node_info(1, remoting_addr)
            .with_raft_peers(vec![crate::config::RaftPeer { id: 1, addr: raft_addr }])
            .with_storage_backend(crate::config::StorageBackendType::Memory);
        let manager = Arc::new(
            ControllerManager::new(config, test_service_context(), test_telemetry_handle())
                .await
                .expect("create manager"),
        );
        manager.initialize().await.expect("initialize manager");
        manager.start().await.expect("start manager before simulated failure");
        assert!(manager.is_running());
        assert_eq!(manager.heartbeat_manager.scan_task_count(), 1);

        let _lifecycle_guard = manager.lifecycle_lock.lock().await;
        let error = manager
            .cleanup_after_start_failure(ControllerError::runtime_error(
                "simulated failure after component startup",
            ))
            .await;

        assert!(error.to_string().contains("simulated failure after component startup"));
        assert!(!manager.is_running());
        assert_eq!(manager.heartbeat_manager.scan_task_count(), 0);
        assert!(manager.manager_task_group.lock().is_none());
        drop(_lifecycle_guard);
        manager
            .shutdown()
            .await
            .expect("idempotent shutdown after startup cleanup");
    }

    #[tokio::test]
    async fn occupied_remoting_listener_fails_startup_and_cleans_up_owned_components() {
        let occupied = std::net::TcpListener::bind("0.0.0.0:0").expect("occupy remoting listener");
        let remoting_addr = std::net::SocketAddr::from((
            std::net::Ipv4Addr::LOCALHOST,
            occupied.local_addr().expect("occupied remoting address").port(),
        ));
        let (_unused_remoting_addr, raft_addr) = reserve_controller_addresses();
        let config = ControllerConfig::default()
            .with_node_info(1, remoting_addr)
            .with_raft_peers(vec![crate::config::RaftPeer { id: 1, addr: raft_addr }])
            .with_storage_backend(crate::config::StorageBackendType::Memory);
        let manager = Arc::new(
            ControllerManager::new(config, test_service_context(), test_telemetry_handle())
                .await
                .expect("create manager"),
        );
        manager.initialize().await.expect("initialize manager");

        let error = manager
            .start()
            .await
            .expect_err("occupied remoting listener must fail startup");

        assert!(error.to_string().contains("Controller remoting server failed to start"));
        assert!(!manager.is_running());
        assert_eq!(manager.heartbeat_manager.scan_task_count(), 0);
        assert!(manager.manager_task_group.lock().is_none());
        assert!(manager.remoting_server_shutdown_tx.lock().is_none());
        drop(occupied);
        let restart_error = manager
            .start()
            .await
            .expect_err("a failed startup must not consume the released listener on retry");
        assert_eq!(
            restart_error.to_string(),
            "Runtime error: Controller manager cannot be restarted after shutdown or a failed startup"
        );
        assert!(!manager.is_running());
        manager
            .shutdown()
            .await
            .expect("shutdown remains idempotent after listener startup cleanup");
    }

    #[tokio::test]
    async fn test_manager_shutdown() {
        let (remoting_addr, raft_addr) = reserve_controller_addresses();
        let config = ControllerConfig::default()
            .with_node_info(1, remoting_addr)
            .with_raft_peers(vec![crate::config::RaftPeer { id: 1, addr: raft_addr }])
            .with_storage_backend(crate::config::StorageBackendType::Memory);

        let manager = ControllerManager::new(config, test_service_context(), test_telemetry_handle())
            .await
            .expect("Failed to create manager");
        let manager_arc = Arc::new(manager);

        // Initialize first
        manager_arc.initialize().await.expect("Failed to initialize");

        // Test shutdown without starting (should succeed)
        manager_arc.shutdown().await.expect("Failed to shutdown");
        manager_arc
            .start()
            .await
            .expect("shutdown before start must not prevent the first start");
        assert!(manager_arc.is_running());
        manager_arc.shutdown().await.expect("shutdown after start");

        // Prevent dropping runtime in async context
        std::mem::forget(manager_arc);
    }

    #[tokio::test]
    async fn test_start_without_initialize() {
        let config = ControllerConfig::default().with_node_info(1, "127.0.0.1:9880".parse::<SocketAddr>().unwrap());

        let manager = ControllerManager::new(config, test_service_context(), test_telemetry_handle())
            .await
            .expect("Failed to create manager");
        let manager_arc = Arc::new(manager);

        // Try to start without initializing (should fail)
        let result = manager_arc.start().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ControllerError::NotInitialized(_)));

        // Prevent dropping runtime in async context
        std::mem::forget(manager_arc);
    }

    #[tokio::test]
    async fn test_atomic_state_checks() {
        let config = ControllerConfig::default().with_node_info(1, "127.0.0.1:9881".parse::<SocketAddr>().unwrap());

        let manager = ControllerManager::new(config, test_service_context(), test_telemetry_handle())
            .await
            .expect("Failed to create manager");

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
    async fn test_leadership_watch_enables_scheduling_for_openraft_leader() {
        let port = 9883;
        let config = ControllerConfig::default()
            .with_node_info(1, format!("127.0.0.1:{port}").parse::<SocketAddr>().unwrap())
            .with_heartbeat_interval_ms(100)
            .with_election_timeout_ms(300)
            .with_storage_backend(crate::config::StorageBackendType::Memory);

        let manager = Arc::new(
            ControllerManager::new(config, test_service_context(), test_telemetry_handle())
                .await
                .expect("Failed to create manager"),
        );
        manager.initialize().await.expect("initialize manager");
        manager.start().await.expect("start manager");

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

    #[tokio::test]
    async fn leadership_gate_recovers_from_pre_start_notifier_poisoning() {
        let (remoting_addr, raft_addr) = reserve_controller_addresses();
        let config = ControllerConfig::default()
            .with_node_info(1, remoting_addr)
            .with_raft_peers(vec![crate::config::RaftPeer { id: 1, addr: raft_addr }])
            .with_storage_backend(crate::config::StorageBackendType::Memory);
        let manager = Arc::new(
            ControllerManager::new(config, test_service_context(), test_telemetry_handle())
                .await
                .expect("create manager"),
        );
        manager.initialize().await.expect("initialize manager");

        manager.set_test_leadership_override(Some(true));
        manager
            .submit_broker_role_notifications([test_notify_task(1)])
            .await
            .expect("pre-start notification submission");
        let poisoned = manager.broker_role_notifier_snapshot();
        assert_eq!(
            poisoned.accepted, 0,
            "an unstarted notifier must reject the poisoned submission"
        );
        assert!(
            manager.scheduling_enabled(),
            "the pre-start leader path must have applied the poisoned gate state"
        );

        manager.start().await.expect("start manager");
        manager
            .submit_broker_role_notifications([test_notify_task(2)])
            .await
            .expect("post-start notification submission");
        let recovered = manager.broker_role_notifier_snapshot();
        assert_eq!(recovered.accepted, 1, "post-start leader notification must be accepted");
        assert!(
            manager.scheduling_enabled(),
            "the forced post-start synchronization must retain leader scheduling"
        );

        manager.shutdown().await.expect("shutdown manager");
        std::mem::forget(manager);
    }

    #[tokio::test]
    async fn leadership_gate_orders_authoritative_promotion_and_demotion_before_notify_submission() {
        let (remoting_addr, raft_addr) = reserve_controller_addresses();
        let config = ControllerConfig::default()
            .with_node_info(1, remoting_addr)
            .with_raft_peers(vec![crate::config::RaftPeer { id: 1, addr: raft_addr }])
            .with_storage_backend(crate::config::StorageBackendType::Memory);
        let manager = Arc::new(
            ControllerManager::new(config, test_service_context(), test_telemetry_handle())
                .await
                .expect("create manager"),
        );
        manager.initialize().await.expect("initialize manager");
        manager.start().await.expect("start manager");

        manager.set_test_leadership_override(Some(true));
        let (promotion_submission, promotion_watch) = tokio::join!(biased;
            manager.submit_broker_role_notifications([test_notify_task(1)]),
            manager.synchronize_leadership_gate(),
        );
        promotion_submission.expect("leader notification submission");
        assert!(promotion_watch.expect("leader watch synchronization"));
        let promoted = manager.broker_role_notifier_snapshot();
        assert_eq!(promoted.accepted, 1, "leader submission must be retained immediately");
        assert!(manager.scheduling_enabled());

        manager.set_test_leadership_override(Some(false));
        let (demotion_watch, demotion_submission) = tokio::join!(biased;
            manager.synchronize_leadership_gate(),
            manager.submit_broker_role_notifications([test_notify_task(2)]),
        );
        assert!(!demotion_watch.expect("leader watch demotion"));
        demotion_submission.expect("follower notification submission");
        let demoted = manager.broker_role_notifier_snapshot();
        assert_eq!(
            demoted.accepted, promoted.accepted,
            "a demoted controller must not retain a stale role-change notification"
        );
        assert!(
            demoted.generation > promoted.generation,
            "demotion must reset the notifier generation"
        );
        assert_eq!(demoted.queued_keys, 0, "demotion must clear queued notifications");
        assert_eq!(
            demoted.retry_waiting_keys, 0,
            "demotion must clear retry-waiting notifications"
        );
        assert!(!manager.scheduling_enabled());

        manager.shutdown().await.expect("shutdown manager");
        manager.set_test_leadership_override(Some(true));
        manager
            .submit_broker_role_notifications([test_notify_task(3)])
            .await
            .expect("stopped notification submission");
        let stopped = manager.broker_role_notifier_snapshot();
        assert!(stopped.closed, "shutdown must close the notifier");
        assert_eq!(
            stopped.accepted, demoted.accepted,
            "a stopped controller must not accept a later leader notification"
        );
        std::mem::forget(manager);
    }

    #[tokio::test]
    async fn inactive_slave_does_not_elect_but_inactive_master_does() {
        let port = 9886;
        let config = ControllerConfig::default()
            .with_node_info(1, format!("127.0.0.1:{port}").parse::<SocketAddr>().unwrap())
            .with_heartbeat_interval_ms(100)
            .with_election_timeout_ms(300)
            .with_storage_backend(crate::config::StorageBackendType::Memory)
            .with_notify_broker_role_changed(false);

        let manager = Arc::new(
            ControllerManager::new(config, test_service_context(), test_telemetry_handle())
                .await
                .expect("create manager"),
        );
        manager.initialize().await.expect("initialize manager");
        manager.start().await.expect("start manager");

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
            .expect("initialize cluster");
        wait_until(Duration::from_secs(5), || manager.is_leader(), "controller leader").await;

        for (broker_id, addr, check_code) in [
            (1_i64, "127.0.0.1:10911", "master-check"),
            (2_i64, "127.0.0.1:10912", "slave-check"),
        ] {
            let apply_header = ApplyBrokerIdRequestHeader {
                cluster_name: CheetahString::from_static_str("test-cluster"),
                broker_name: CheetahString::from_static_str("broker-a"),
                applied_broker_id: broker_id,
                register_check_code: CheetahString::from_string(format!("{addr};{check_code}")),
            };
            let apply_response = manager
                .controller()
                .apply_broker_id(&apply_header)
                .await
                .expect("apply broker id")
                .expect("apply response");
            assert_eq!(apply_response.code(), ResponseCode::Success as i32);

            let register_header = RegisterBrokerToControllerRequestHeader {
                cluster_name: Some(CheetahString::from_static_str("test-cluster")),
                broker_name: Some(CheetahString::from_static_str("broker-a")),
                broker_id: Some(broker_id),
                broker_address: Some(CheetahString::from_static_str(addr)),
                ..Default::default()
            };
            let register_response = manager
                .controller()
                .register_broker(&register_header)
                .await
                .expect("register broker")
                .expect("register response");
            assert_eq!(register_response.code(), ResponseCode::Success as i32);

            let heartbeat_header = BrokerHeartbeatRequestHeader {
                cluster_name: CheetahString::from_static_str("test-cluster"),
                broker_addr: CheetahString::from_static_str(addr),
                broker_name: CheetahString::from_static_str("broker-a"),
                broker_id: Some(broker_id),
                epoch: Some(1),
                max_offset: Some(100),
                confirm_offset: Some(80),
                store_ready: Some(true),
                heartbeat_timeout_mills: Some(60_000),
                election_priority: Some(1),
            };
            let heartbeat_response = manager
                .controller()
                .record_broker_heartbeat(&heartbeat_header)
                .await
                .expect("record heartbeat")
                .expect("heartbeat response");
            assert_eq!(heartbeat_response.code(), ResponseCode::Success as i32);
        }

        let elect_header = ElectMasterRequestHeader::new("test-cluster", "broker-a", 1, false, current_millis());
        let mut elect_response = manager
            .controller()
            .elect_master(&elect_header)
            .await
            .expect("elect master")
            .expect("elect response");
        assert_eq!(elect_response.code(), ResponseCode::Success as i32);
        elect_response.make_custom_header_to_net();
        let elect_response_header = elect_response
            .decode_command_custom_header::<ElectMasterResponseHeader>()
            .expect("decode elect response");
        let alter_header = AlterSyncStateSetRequestHeader {
            broker_name: CheetahString::from_static_str("broker-a"),
            master_broker_id: 1,
            master_epoch: elect_response_header.master_epoch.expect("master epoch"),
            invoke_time: rocketmq_model::time::current_millis(),
        };
        let alter_body = SyncStateSet::with_values(
            HashSet::from([1_i64, 2_i64]),
            elect_response_header
                .sync_state_set_epoch
                .expect("sync state set epoch"),
        );
        let alter_response = manager
            .controller()
            .alter_sync_state_set(&alter_header, alter_body)
            .await
            .expect("alter sync state")
            .expect("alter response");
        assert_eq!(alter_response.code(), ResponseCode::Success as i32);

        let listener = BrokerInactiveListener::new(Arc::downgrade(&manager));
        listener.on_broker_inactive(Some("test-cluster"), "broker-a", Some(2));
        sleep(Duration::from_millis(300)).await;

        let replica_header = GetReplicaInfoRequestHeader {
            broker_name: CheetahString::from_static_str("broker-a"),
        };
        let mut replica_response = manager
            .controller()
            .get_replica_info(&replica_header)
            .await
            .expect("get replica info after inactive slave")
            .expect("replica response");
        replica_response.make_custom_header_to_net();
        let replica_info = replica_response
            .decode_command_custom_header::<GetReplicaInfoResponseHeader>()
            .expect("decode replica info");
        assert_eq!(replica_info.master_broker_id, Some(1));

        let slave_heartbeat_header = BrokerHeartbeatRequestHeader {
            cluster_name: CheetahString::from_static_str("test-cluster"),
            broker_addr: CheetahString::from_static_str("127.0.0.1:10912"),
            broker_name: CheetahString::from_static_str("broker-a"),
            broker_id: Some(2),
            epoch: Some(1),
            max_offset: Some(100),
            confirm_offset: Some(80),
            store_ready: Some(true),
            heartbeat_timeout_mills: Some(60_000),
            election_priority: Some(1),
        };
        let slave_heartbeat_response = manager
            .controller()
            .record_broker_heartbeat(&slave_heartbeat_header)
            .await
            .expect("record slave heartbeat before master inactive")
            .expect("heartbeat response");
        assert_eq!(slave_heartbeat_response.code(), ResponseCode::Success as i32);

        listener.on_broker_inactive(Some("test-cluster"), "broker-a", Some(1));
        let start = current_millis();
        loop {
            let mut replica_response = manager
                .controller()
                .get_replica_info(&replica_header)
                .await
                .expect("get replica info after inactive master")
                .expect("replica response");
            replica_response.make_custom_header_to_net();
            let replica_info = replica_response
                .decode_command_custom_header::<GetReplicaInfoResponseHeader>()
                .expect("decode replica info");
            if replica_info.master_broker_id == Some(2) {
                break;
            }
            assert!(
                current_millis().saturating_sub(start) < 5_000,
                "timed out waiting for master reelection after inactive master"
            );
            sleep(Duration::from_millis(50)).await;
        }

        manager.shutdown().await.expect("shutdown manager");
        std::mem::forget(manager);
    }

    #[tokio::test]
    async fn processor_successful_manual_election_records_role_change_notification() {
        let port = 9887;
        let config = ControllerConfig::default()
            .with_node_info(1, format!("127.0.0.1:{port}").parse::<SocketAddr>().unwrap())
            .with_heartbeat_interval_ms(100)
            .with_election_timeout_ms(300)
            .with_storage_backend(crate::config::StorageBackendType::Memory)
            .with_notify_broker_role_changed(true);

        let manager = Arc::new(
            ControllerManager::new(config, test_service_context(), test_telemetry_handle())
                .await
                .expect("create manager"),
        );
        manager.initialize().await.expect("initialize manager");
        manager.start().await.expect("start manager");

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
            .expect("initialize cluster");
        wait_until(Duration::from_secs(5), || manager.is_leader(), "controller leader").await;

        let channel = create_test_channel().await;
        for (broker_id, addr, check_code) in [
            (1_i64, "127.0.0.1:10911", "master-check"),
            (2_i64, "127.0.0.1:10912", "slave-check"),
        ] {
            let apply_header = ApplyBrokerIdRequestHeader {
                cluster_name: CheetahString::from_static_str("test-cluster"),
                broker_name: CheetahString::from_static_str("broker-a"),
                applied_broker_id: broker_id,
                register_check_code: CheetahString::from_string(format!("{addr};{check_code}")),
            };
            let apply_response = manager
                .controller()
                .apply_broker_id(&apply_header)
                .await
                .expect("apply broker id")
                .expect("apply response");
            assert_eq!(apply_response.code(), ResponseCode::Success as i32);

            let register_header = RegisterBrokerToControllerRequestHeader {
                cluster_name: Some(CheetahString::from_static_str("test-cluster")),
                broker_name: Some(CheetahString::from_static_str("broker-a")),
                broker_id: Some(broker_id),
                broker_address: Some(CheetahString::from_static_str(addr)),
                ..Default::default()
            };
            let register_response = manager
                .controller()
                .register_broker(&register_header)
                .await
                .expect("register broker")
                .expect("register response");
            assert_eq!(register_response.code(), ResponseCode::Success as i32);

            let heartbeat_header = BrokerHeartbeatRequestHeader {
                cluster_name: CheetahString::from_static_str("test-cluster"),
                broker_addr: CheetahString::from_static_str(addr),
                broker_name: CheetahString::from_static_str("broker-a"),
                broker_id: Some(broker_id),
                epoch: Some(1),
                max_offset: Some(100),
                confirm_offset: Some(80),
                store_ready: Some(true),
                heartbeat_timeout_mills: Some(60_000),
                election_priority: Some(1),
            };
            let heartbeat_response = manager
                .controller()
                .record_broker_heartbeat(&heartbeat_header)
                .await
                .expect("record replicated heartbeat")
                .expect("heartbeat response");
            assert_eq!(heartbeat_response.code(), ResponseCode::Success as i32);
            manager.heartbeat_manager().on_broker_heartbeat(
                "test-cluster",
                "broker-a",
                addr,
                broker_id,
                Some(60_000),
                channel.clone(),
                Some(1),
                Some(100),
                Some(80),
                Some(1),
            );
        }

        let initial_elect_header =
            ElectMasterRequestHeader::new("test-cluster", "broker-a", 1, false, current_millis());
        let mut initial_elect_response = manager
            .controller()
            .elect_master(&initial_elect_header)
            .await
            .expect("elect initial master")
            .expect("initial elect response");
        assert_eq!(initial_elect_response.code(), ResponseCode::Success as i32);
        initial_elect_response.make_custom_header_to_net();
        let initial_header = initial_elect_response
            .decode_command_custom_header::<ElectMasterResponseHeader>()
            .expect("decode initial elect response");

        let alter_header = AlterSyncStateSetRequestHeader {
            broker_name: CheetahString::from_static_str("broker-a"),
            master_broker_id: 1,
            master_epoch: initial_header.master_epoch.expect("master epoch"),
            invoke_time: rocketmq_model::time::current_millis(),
        };
        let alter_body = SyncStateSet::with_values(
            HashSet::from([1_i64, 2_i64]),
            initial_header.sync_state_set_epoch.expect("sync state set epoch"),
        );
        let alter_response = manager
            .controller()
            .alter_sync_state_set(&alter_header, alter_body)
            .await
            .expect("alter sync state")
            .expect("alter response");
        assert_eq!(alter_response.code(), ResponseCode::Success as i32);
        assert!(
            manager
                .heartbeat_manager()
                .is_broker_active("test-cluster", "broker-a", 2),
            "local heartbeat manager must consider target broker active for role-change notification"
        );

        let mut processor = ControllerRequestProcessor::new(manager.clone());
        let ctx = Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut request = RemotingCommand::create_request_command(
            RequestCode::ControllerElectMaster,
            ElectMasterRequestHeader::new("test-cluster", "broker-a", 2, true, current_millis()),
        );
        request.make_custom_header_to_net();
        let mut response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor elect request")
            .expect("processor elect response");
        response.make_custom_header_to_net();
        assert_eq!(response.code(), ResponseCode::Success as i32);
        let response_header = response
            .decode_command_custom_header::<ElectMasterResponseHeader>()
            .expect("decode processor elect response");
        assert_eq!(response_header.master_broker_id, Some(2));
        let response_body = ElectMasterResponseBody::decode(response.body().expect("elect response body").as_ref())
            .expect("decode processor elect response body");
        assert!(
            response_body.broker_member_group.is_some(),
            "successful manual election must carry broker member group for role-change notification"
        );

        wait_until(
            Duration::from_secs(2),
            || {
                let snapshot = manager.broker_role_notifier_snapshot();
                snapshot.accepted > 0
            },
            "processor elect-master to record broker role notification",
        )
        .await;

        manager.shutdown().await.expect("shutdown manager");
        std::mem::forget(processor);
        std::mem::forget(manager);
    }
}
