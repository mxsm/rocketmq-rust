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
use crate::controller::broker_heartbeat_manager::BrokerSessionId;
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
use rocketmq_transport::api::v1::DefaultRequestProcessor;
use rocketmq_transport::api::v1::RemotingClient;
use rocketmq_transport::api::v1::ServerConfig;
use rocketmq_transport::api::v1::TransportClientConfig;
use rocketmq_transport::api::v1::TransportTelemetry;
use rocketmq_transport::api::v2::TransportServerV2;
use rocketmq_transport::api::v2::V2SessionEvent;
use rocketmq_transport::api::v2::V2SessionRegistry;
use tokio::sync::oneshot;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::sleep;
use tracing::error;
use tracing::info;
use tracing::warn;

mod leadership;
mod lifecycle;

struct BrokerInactiveListener {
    controller_manager: Weak<ControllerManager>,
}

struct PendingControllerRemotingServer {
    config: Arc<ServerConfig>,
    service_context: ChildServiceContext,
    telemetry: TransportTelemetry,
    session_registry: Arc<V2SessionRegistry>,
}

impl PendingControllerRemotingServer {
    fn build(self, request_processor: ControllerRequestProcessor) -> TransportServerV2<ControllerRequestProcessor> {
        TransportServerV2::new_with_telemetry(self.config, self.service_context, request_processor, self.telemetry)
            .with_session_registry(self.session_registry)
    }
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

/// Coordinates the Controller's Raft, heartbeat, remoting, and leadership components.
pub struct ControllerManager {
    config: ControllerConfigHandle,

    /// Immutable wire defaults shared by all command producers owned by this Controller.
    command_factory: RemotingCommandFactory,

    /// Lifecycle mutation is synchronized inside the Raft controller.
    raft_controller: Arc<RaftController>,

    /// Lifecycle slots and listeners are synchronized inside the heartbeat manager.
    heartbeat_manager: Arc<DefaultBrokerHeartbeatManager>,

    remoting_server: Mutex<Option<PendingControllerRemotingServer>>,
    session_registry: Arc<V2SessionRegistry>,
    remoting_server_shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    manager_task_group: Arc<Mutex<Option<TaskGroup>>>,
    leadership_watch_tasks: Arc<Mutex<Option<ScheduledTaskGroup>>>,

    /// Runtime-neutral security capabilities supplied by the composition root.
    security: Option<ControllerSecurity>,

    remoting_client: Arc<RemotingClient>,

    #[cfg(feature = "metrics")]
    metrics_manager: Arc<ControllerMetricsManager>,

    running: Arc<AtomicBool>,

    initialized: Arc<AtomicBool>,

    /// A started controller consumes its one-shot remoting and task resources when it stops.
    lifecycle_terminated: Arc<AtomicBool>,

    /// Serializes initialize, start, and graceful shutdown transitions.
    lifecycle_lock: AsyncMutex<()>,

    /// Serializes leadership side effects and manual role-change notification submission.
    leadership_gate: AsyncMutex<LeadershipGateState>,

    #[cfg(test)]
    test_leadership_override: AtomicU8,

    broker_role_notifier: BrokerRoleNotifier,
    service_context: ChildServiceContext,
}

impl ControllerManager {
    /// Creates a Controller manager using the application remoting defaults.
    ///
    /// # Errors
    ///
    /// Returns [`ControllerError`] when configuration validation or component
    /// initialization fails.
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

        let listen_port = config.snapshot().listen_addr.port() as u32;

        let server_config = ServerConfig {
            listen_port,
            ..Default::default()
        };
        #[cfg(any(feature = "metrics", feature = "otel-traces"))]
        let transport_telemetry = TransportTelemetry::from_handle(&telemetry_handle);
        #[cfg(not(any(feature = "metrics", feature = "otel-traces")))]
        let transport_telemetry = TransportTelemetry::noop();
        let session_registry = Arc::new(V2SessionRegistry::new());
        let remoting_server = Some(PendingControllerRemotingServer {
            config: Arc::new(server_config),
            service_context: service_context.component("controller.remoting-server"),
            telemetry: transport_telemetry.clone(),
            session_registry: Arc::clone(&session_registry),
        });
        info!("Remoting server created on port {}", listen_port);

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
            session_registry,
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
            broker_role_notifier,
            service_context,
        })
    }

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

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::Acquire)
    }

    pub fn raft(&self) -> &RaftController {
        &self.raft_controller
    }

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

    #[cfg(feature = "metrics")]
    pub fn metrics_manager(&self) -> &Arc<ControllerMetricsManager> {
        &self.metrics_manager
    }

    /// Alias for [`Self::config`] retained for API compatibility.
    pub fn controller_config(&self) -> Arc<ControllerConfig> {
        self.config.snapshot()
    }

    pub fn heartbeat_manager(&self) -> &Arc<DefaultBrokerHeartbeatManager> {
        &self.heartbeat_manager
    }

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
}

impl Drop for ControllerManager {
    fn drop(&mut self) {
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
mod tests;
