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

//! OpenRaft-based controller implementation
//!
//! This module provides a production-ready OpenRaft controller with:
//! - Graceful shutdown support
//! - Proper resource cleanup
//! - Thread-safe state management
//! - gRPC server lifecycle management

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use crate::config::ControllerConfigReader;
use crate::controller::broker_heartbeat_manager::DEFAULT_BROKER_CHANNEL_EXPIRED_TIME;
use crate::controller::membership::ConsensusMembership;
use crate::controller::membership::MembershipChangeOutcome;
use crate::controller::membership::MembershipChangeRequest;
use crate::controller::release_snapshot::controller_snapshot_error;
use crate::controller::release_snapshot::ControllerReleaseSnapshot;
use crate::controller::release_snapshot::ControllerReleaseSnapshotRepository;
use crate::controller::Controller;
use crate::error::ControllerError;
use crate::error::Result;
use crate::event::controller_result::ControllerResult as EventControllerResult;
use crate::heartbeat::default_broker_heartbeat_manager::DefaultBrokerHeartbeatManager;
use crate::helper::broker_lifecycle_listener::BrokerLifecycleListener;
use crate::manager::write_lease_manager::WriteLeaseGrantGate;
use crate::metrics::controller_metrics_manager::ControllerMetricsManager;
use crate::openraft::GrpcRaftService;
use crate::openraft::RaftNodeManager;
use crate::openraft::StateMachineReadView;
use crate::protobuf::openraft::open_raft_service_server::OpenRaftServiceServer;
use crate::typ::BrokerIdentityInfoSnapshot;
use crate::typ::BrokerLiveInfoSnapshot;
use crate::typ::ControllerRequest;
use crate::typ::ControllerResponse;
use crate::typ::Node;
use crate::typ::NodeId;
use crate::ReplicasInfoManager;
use cheetah_string::CheetahString;
use parking_lot::Mutex;
use parking_lot::RwLock;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::SerializationError;
use rocketmq_error::UnifiedServiceError;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::release_checkpoint::ControllerReleaseSnapshotManifest;
use rocketmq_protocol::protocol::body::release_checkpoint::ControllerReleaseSnapshotRequest;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointArtifact;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointRestoreVerification;
use rocketmq_protocol::protocol::body::release_checkpoint::RELEASE_CHECKPOINT_SCHEMA_VERSION;
use rocketmq_protocol::protocol::body::sync_state_set_body::SyncStateSet;
use rocketmq_protocol::protocol::header::controller::alter_sync_state_set_request_header::AlterSyncStateSetRequestHeader;
use rocketmq_protocol::protocol::header::controller::apply_broker_id_request_header::ApplyBrokerIdRequestHeader;
use rocketmq_protocol::protocol::header::controller::clean_broker_data_request_header::CleanBrokerDataRequestHeader;
use rocketmq_protocol::protocol::header::controller::elect_master_request_header::ElectMasterRequestHeader;
use rocketmq_protocol::protocol::header::controller::get_next_broker_id_request_header::GetNextBrokerIdRequestHeader;
use rocketmq_protocol::protocol::header::controller::get_replica_info_request_header::GetReplicaInfoRequestHeader;
use rocketmq_protocol::protocol::header::controller::register_broker_to_controller_request_header::RegisterBrokerToControllerRequestHeader;
use rocketmq_protocol::protocol::header::controller::register_broker_to_controller_response_header::RegisterBrokerToControllerResponseHeader;
use rocketmq_protocol::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader;
use rocketmq_protocol::protocol::header::namesrv::broker_request::BrokerHeartbeatRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::broker_request::MAX_BROKER_HEARTBEAT_TIMEOUT_MILLIS;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::CommandCustomHeader;
use rocketmq_protocol::protocol::RemotingDeserializable;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::BlockingKind;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::TaskGroup;
use rocketmq_security_api::MaintenanceAuthorizationGrant;
use rocketmq_security_api::MaintenanceCapability;
use sha2::Digest;
use sha2::Sha256;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::sync::Mutex as AsyncMutex;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing::info;
use tracing::Instrument;

const CONTROLLER_RAFT_BIND_ADDR_ENV: &str = "ROCKETMQ_CONTROLLER_RAFT_BIND_ADDR";

fn openraft_startup_failed(operation: &'static str, error: impl std::fmt::Display) -> RocketMQError {
    RocketMQError::Service(UnifiedServiceError::StartupFailed(format!(
        "OpenRaft controller {operation}: {error}"
    )))
}

fn openraft_response_decode_failed(error: serde_json::Error) -> RocketMQError {
    RocketMQError::Serialization(SerializationError::decode_failed(
        "JSON",
        format!("OpenRaft inactive broker scan response: {error}"),
    ))
}

#[derive(Default)]
struct OpenRaftLifecycle {
    node: Option<Arc<RaftNodeManager>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

/// OpenRaft-based controller implementation
///
/// # Graceful Shutdown
///
/// This implementation provides proper graceful shutdown:
/// 1. Sends shutdown signal to gRPC server via oneshot channel
/// 2. Waits for Raft node to shutdown cleanly
/// 3. Waits for gRPC server task to complete (with 10s timeout)
pub struct OpenRaftController {
    config: ControllerConfigReader,
    command_factory: RemotingCommandFactory,
    /// Serializes startup and shutdown transitions.
    lifecycle_lock: AsyncMutex<()>,
    /// Raft node and gRPC shutdown handle owned by the lifecycle transition.
    lifecycle: Mutex<OpenRaftLifecycle>,
    /// gRPC server and scheduling task groups
    task_group: Arc<Mutex<Option<TaskGroup>>>,
    scan_task_group: Arc<Mutex<Option<TaskGroup>>>,
    scan_scheduled_tasks: Arc<Mutex<Option<ScheduledTaskGroup>>>,
    heartbeat_manager: Arc<DefaultBrokerHeartbeatManager>,
    lifecycle_listeners: Arc<RwLock<Vec<Arc<dyn BrokerLifecycleListener>>>>,
    scheduling: Arc<AtomicBool>,
    first_received_heartbeat_time: Arc<AtomicU64>,
    service_context: ChildServiceContext,
    metrics_manager: Arc<ControllerMetricsManager>,
    write_lease_grant_gate: Mutex<WriteLeaseGrantGate>,
}

impl OpenRaftController {
    pub fn new(config: ControllerConfigReader, service_context: ChildServiceContext) -> Self {
        Self::new_with_remoting_command_factory(config, service_context, application_remoting_command_factory())
    }

    /// Creates a Controller with explicit immutable remoting defaults.
    pub fn new_with_remoting_command_factory(
        config: ControllerConfigReader,
        service_context: ChildServiceContext,
        command_factory: RemotingCommandFactory,
    ) -> Self {
        let heartbeat_manager = Arc::new(DefaultBrokerHeartbeatManager::new(
            config.clone(),
            service_context.task_group().clone(),
        ));
        Self::new_with_heartbeat_and_remoting_command_factory(
            config,
            heartbeat_manager,
            service_context,
            command_factory,
        )
    }

    pub fn new_with_heartbeat(
        config: ControllerConfigReader,
        heartbeat_manager: Arc<DefaultBrokerHeartbeatManager>,
        service_context: ChildServiceContext,
    ) -> Self {
        Self::new_with_heartbeat_and_remoting_command_factory(
            config,
            heartbeat_manager,
            service_context,
            application_remoting_command_factory(),
        )
    }

    /// Creates a Controller that shares the supplied heartbeat manager and
    /// uses explicit immutable remoting defaults.
    pub fn new_with_heartbeat_and_remoting_command_factory(
        config: ControllerConfigReader,
        heartbeat_manager: Arc<DefaultBrokerHeartbeatManager>,
        service_context: ChildServiceContext,
        command_factory: RemotingCommandFactory,
    ) -> Self {
        let metrics_manager =
            ControllerMetricsManager::new(config.clone(), &rocketmq_observability::TelemetryHandle::noop());
        Self::new_with_heartbeat_metrics_and_remoting_command_factory(
            config,
            heartbeat_manager,
            service_context,
            metrics_manager,
            command_factory,
        )
    }

    pub(crate) fn new_with_heartbeat_and_metrics(
        config: ControllerConfigReader,
        heartbeat_manager: Arc<DefaultBrokerHeartbeatManager>,
        service_context: ChildServiceContext,
        metrics_manager: Arc<ControllerMetricsManager>,
    ) -> Self {
        Self::new_with_heartbeat_metrics_and_remoting_command_factory(
            config,
            heartbeat_manager,
            service_context,
            metrics_manager,
            application_remoting_command_factory(),
        )
    }

    pub(crate) fn new_with_heartbeat_metrics_and_remoting_command_factory(
        config: ControllerConfigReader,
        heartbeat_manager: Arc<DefaultBrokerHeartbeatManager>,
        service_context: ChildServiceContext,
        metrics_manager: Arc<ControllerMetricsManager>,
        command_factory: RemotingCommandFactory,
    ) -> Self {
        Self {
            config,
            command_factory,
            lifecycle_lock: AsyncMutex::new(()),
            lifecycle: Mutex::new(OpenRaftLifecycle::default()),
            task_group: Arc::new(Mutex::new(None)),
            scan_task_group: Arc::new(Mutex::new(None)),
            scan_scheduled_tasks: Arc::new(Mutex::new(None)),
            heartbeat_manager,
            lifecycle_listeners: Arc::new(RwLock::new(Vec::new())),
            scheduling: Arc::new(AtomicBool::new(false)),
            first_received_heartbeat_time: Arc::new(AtomicU64::new(0)),
            service_context,
            metrics_manager,
            write_lease_grant_gate: Mutex::new(WriteLeaseGrantGate::default()),
        }
    }

    fn ensure_task_group(&self) -> RocketMQResult<TaskGroup> {
        let mut guard = self.task_group.lock();
        if let Some(task_group) = guard.as_ref() {
            return Ok(task_group.clone());
        }

        let task_group = self
            .service_context
            .component("rocketmq-controller.openraft")
            .task_group()
            .clone();
        *guard = Some(task_group.clone());
        Ok(task_group)
    }

    async fn start_scan_task_group(&self) -> RocketMQResult<ScheduledTaskGroup> {
        self.scan_scheduled_tasks.lock().take();
        let previous_task_group = {
            let mut guard = self.scan_task_group.lock();
            guard.take()
        };
        if let Some(task_group) = previous_task_group {
            let report = task_group.shutdown(Duration::from_secs(5)).await;
            if !report.is_healthy() {
                tracing::warn!(
                    report = %report.to_json(),
                    "OpenRaft scan task shutdown report is unhealthy before restart"
                );
            }
        }

        let task_group = self
            .service_context
            .component("controller.openraft.scan-not-active-broker")
            .task_group()
            .clone();
        let scheduled_tasks = ScheduledTaskGroup::new(task_group.clone());
        {
            let mut guard = self.scan_task_group.lock();
            *guard = Some(task_group.clone());
        }
        {
            let mut guard = self.scan_scheduled_tasks.lock();
            *guard = Some(scheduled_tasks.clone());
        }
        Ok(scheduled_tasks)
    }

    async fn stop_scan_task_group(&self) {
        self.scan_scheduled_tasks.lock().take();
        let task_group = { self.scan_task_group.lock().take() };
        if let Some(task_group) = task_group {
            let report = task_group.shutdown(Duration::from_secs(5)).await;
            if !report.is_healthy() {
                tracing::warn!(
                    report = %report.to_json(),
                    "OpenRaft scan task shutdown report is unhealthy"
                );
            }
        }
    }

    pub(crate) fn scan_task_count(&self) -> usize {
        let root_count = self
            .scan_task_group
            .lock()
            .as_ref()
            .map(TaskGroup::task_count)
            .unwrap_or_default();
        let scheduled_count = self
            .scan_scheduled_tasks
            .lock()
            .as_ref()
            .map(|scheduled_tasks| scheduled_tasks.group().task_count())
            .unwrap_or_default();
        root_count + scheduled_count
    }

    pub(crate) fn scan_schedule_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.scan_scheduled_tasks
            .lock()
            .as_ref()
            .map(ScheduledTaskGroup::snapshot)
            .unwrap_or_default()
    }

    async fn shutdown_task_group(&self) {
        let task_group = self.task_group.lock().take();
        let Some(task_group) = task_group else {
            return;
        };

        let report = task_group.shutdown(Duration::from_secs(10)).await;
        if !report.is_healthy() {
            tracing::warn!(
                report = %report.to_json(),
                "OpenRaft controller task shutdown report is unhealthy"
            );
        }
    }

    fn node(&self) -> Option<Arc<RaftNodeManager>> {
        self.lifecycle.lock().node.clone()
    }

    fn is_current_leader(&self) -> bool {
        let Some(node) = self.node() else {
            return false;
        };

        use openraft::async_runtime::WatchReceiver;
        let metrics = node.raft().metrics().borrow_watched().clone();
        metrics.current_leader == Some(self.config.snapshot().node_id)
    }

    fn current_leader_term(&self) -> Option<u64> {
        let Some(node) = self.node() else {
            self.write_lease_grant_gate.lock().observe_not_leader();
            return None;
        };

        use openraft::async_runtime::WatchReceiver;
        let metrics = node.raft().metrics().borrow_watched().clone();
        if metrics.current_leader == Some(self.config.snapshot().node_id) {
            Some(metrics.current_term)
        } else {
            self.write_lease_grant_gate.lock().observe_not_leader();
            None
        }
    }

    pub(crate) fn has_recovered_cluster_state(&self) -> bool {
        let Some(node) = self.node() else {
            return false;
        };

        use openraft::async_runtime::WatchReceiver;
        let metrics = node.raft().metrics().borrow_watched().clone();
        metrics.current_leader.is_some() && metrics.last_applied.is_some()
    }

    fn not_leader_response(&self) -> Option<RemotingCommand> {
        Some(self.command_factory.create_response_command_with_code_remark(
            ResponseCode::ControllerNotLeader,
            "The controller is not in leader state",
        ))
    }

    fn not_started_response(&self) -> Option<RemotingCommand> {
        Some(self.command_factory.create_response_command_with_code_remark(
            ResponseCode::SystemError,
            "The controller raft node is not started",
        ))
    }

    fn command_from_result<T>(&self, result: EventControllerResult<T>) -> RemotingCommand
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        let (_events, header, body, response_code, remark) = result.into_parts();
        let mut response = match header {
            Some(header) => self
                .command_factory
                .create_response_command_with_code_and_header(response_code, header),
            None => self.command_factory.create_response_command_with_code(response_code),
        };
        if let Some(remark) = remark {
            response = response.set_remark(remark);
        }
        if let Some(body) = body {
            response = response.set_body(body);
        }
        response
    }

    fn command_from_result_without_header(&self, result: EventControllerResult<()>) -> RemotingCommand {
        let (_events, _header, body, response_code, remark) = result.into_parts();
        let mut response = self.command_factory.create_response_command_with_code(response_code);
        if let Some(remark) = remark {
            response = response.set_remark(remark);
        }
        if let Some(body) = body {
            response = response.set_body(body);
        }
        response
    }

    fn replicas_info_manager(&self) -> Option<Arc<ReplicasInfoManager>> {
        self.node()
            .map(|node| node.store().state_machine.replicas_info_manager())
    }

    async fn linearizable_replicas_info_manager(&self) -> RocketMQResult<Option<Arc<ReplicasInfoManager>>> {
        let Some(node) = self.node() else {
            return Ok(None);
        };
        node.ensure_linearizable_read().await?;
        Ok(Some(node.store().state_machine.replicas_info_manager()))
    }

    /// Returns the local state-machine view without a Raft read barrier.
    ///
    /// This API is diagnostic-only. Broker-facing business reads use
    /// `linearizable_replicas_info_manager` and ReadIndex.
    #[must_use]
    pub fn diagnostic_stale_read_view(&self) -> Option<StateMachineReadView> {
        self.node().map(|node| node.store().state_machine.read_view())
    }

    fn snapshot_live_broker_infos(
        replicas_info_manager: &ReplicasInfoManager,
        cluster_name: &str,
        broker_name: &str,
        alive_broker_ids: &HashSet<u64>,
    ) -> HashMap<u64, BrokerLiveInfoSnapshot> {
        alive_broker_ids
            .iter()
            .filter_map(|broker_id| {
                replicas_info_manager
                    .get_broker_live_info(cluster_name, broker_name, *broker_id as i64)
                    .map(|live_info| (*broker_id, live_info))
            })
            .collect()
    }

    async fn write_request(&self, request: ControllerRequest) -> RocketMQResult<Option<RemotingCommand>> {
        if !self.is_current_leader() {
            return Ok(self.not_leader_response());
        }

        let Some(node) = self.node() else {
            return Ok(self.not_started_response());
        };

        let response = node.client_write(request).await?;
        Ok(Some(
            response.data.into_remoting_command_with_factory(&self.command_factory),
        ))
    }

    async fn write_internal_request(&self, request: ControllerRequest) -> RocketMQResult<Option<ControllerResponse>> {
        if !self.is_current_leader() {
            return Ok(None);
        }

        let Some(node) = self.node() else {
            return Ok(None);
        };

        let response = node.client_write(request).await?;
        Ok(Some(response.data))
    }

    pub async fn record_broker_heartbeat(
        &self,
        request: &BrokerHeartbeatRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let Some(broker_id) = request.broker_id else {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::ControllerInvalidRequest,
                "Heart beat with empty brokerId",
            )));
        };

        if broker_id < 0 {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::ControllerInvalidRequest,
                "Heart beat with invalid brokerId",
            )));
        }

        let heartbeat_timeout_millis = match request.heartbeat_timeout_mills {
            Some(timeout) => match u64::try_from(timeout) {
                Ok(timeout) if timeout <= MAX_BROKER_HEARTBEAT_TIMEOUT_MILLIS => timeout,
                _ => {
                    return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                        ResponseCode::InvalidParameter,
                        format!(
                            "Heart beat timeout must be between 0 and {MAX_BROKER_HEARTBEAT_TIMEOUT_MILLIS} milliseconds"
                        ),
                    )));
                }
            },
            None => DEFAULT_BROKER_CHANNEL_EXPIRED_TIME,
        };

        let now_millis = current_millis();
        let _ = self
            .first_received_heartbeat_time
            .compare_exchange(0, now_millis, Ordering::AcqRel, Ordering::Acquire);

        let broker_id = broker_id as u64;
        let broker_identity = BrokerIdentityInfoSnapshot::new(
            request.cluster_name.to_string(),
            request.broker_name.to_string(),
            Some(broker_id),
        );
        let broker_live_info = BrokerLiveInfoSnapshot {
            cluster_name: request.cluster_name.to_string(),
            broker_name: request.broker_name.to_string(),
            broker_addr: request.broker_addr.to_string(),
            broker_id,
            last_update_timestamp: now_millis,
            heartbeat_timeout_millis,
            epoch: request.epoch.unwrap_or(-1),
            max_offset: request.max_offset.unwrap_or(-1),
            confirm_offset: request.confirm_offset.unwrap_or(-1),
            election_priority: request.election_priority.or(Some(i32::MAX)),
            store_ready: request.store_ready.unwrap_or(false),
        };
        let lease_grant_allowed = self.current_leader_term().is_some_and(|leader_term| {
            let authority = self.node().and_then(|node| {
                node.store()
                    .state_machine
                    .replicas_info_manager()
                    .current_write_authority(&request.cluster_name, &request.broker_name)
            });
            self.write_lease_grant_gate
                .lock()
                .allow(leader_term, authority, Instant::now())
        });

        match self
            .write_internal_request(ControllerRequest::BrokerHeartbeat {
                broker_identity,
                broker_live_info,
                lease_grant_allowed,
            })
            .await?
        {
            Some(response) => Ok(Some(response.into_remoting_command_with_factory(&self.command_factory))),
            None => Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::Success,
                "Heart beat success",
            ))),
        }
    }

    pub async fn remove_broker_live_info(
        &self,
        cluster_name: Option<&str>,
        broker_name: &str,
        broker_id: Option<i64>,
    ) -> RocketMQResult<()> {
        let Some(broker_id) = broker_id else {
            return Ok(());
        };
        if broker_id < 0 {
            return Ok(());
        }

        let broker_identity = BrokerIdentityInfoSnapshot::new(
            cluster_name.unwrap_or_default().to_string(),
            broker_name.to_string(),
            Some(broker_id as u64),
        );
        let _ = self
            .write_internal_request(ControllerRequest::BrokerChannelClose { broker_identity })
            .await?;
        Ok(())
    }

    async fn scan_not_active_broker_once(
        node: Arc<RaftNodeManager>,
        check_time_millis: u64,
    ) -> RocketMQResult<Vec<BrokerIdentityInfoSnapshot>> {
        let response = node
            .client_write(ControllerRequest::CheckNotActiveBroker { check_time_millis })
            .await?;
        let Some(body) = response.data.body else {
            return Ok(Vec::new());
        };
        serde_json::from_slice(&body).map_err(openraft_response_decode_failed)
    }

    /// Initializes a brand-new cluster during first-node bootstrap only.
    ///
    /// Post-bootstrap membership changes must use [`Self::apply_membership_change`].
    pub async fn initialize_cluster(&self, nodes: BTreeMap<NodeId, Node>) -> Result<()> {
        let node = self
            .node()
            .ok_or_else(|| ControllerError::NotInitialized("OpenRaft node is not started".to_string()))?;
        node.initialize_cluster(nodes).await
    }

    /// Creates a release snapshot after a leader ReadIndex barrier.
    ///
    /// The authorization grant is minted only by the auth-owned maintenance
    /// policy and carries the absolute deadline, fencing token, and resource
    /// budget into this storage boundary.
    ///
    /// # Errors
    ///
    /// Returns a typed error when authorization is unsuitable, the request is
    /// invalid, this node is unavailable/not leader, the absolute deadline is
    /// exceeded, snapshot creation fails, or the resulting artifact violates
    /// its integrity/resource contract.
    pub async fn create_release_snapshot(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        request: ControllerReleaseSnapshotRequest,
    ) -> RocketMQResult<ControllerReleaseSnapshot> {
        if authorization.capability() != MaintenanceCapability::ReleaseCheckpoint {
            return Err(RocketMQError::authentication_failed(
                "maintenance grant does not authorize release checkpoints",
            ));
        }
        request.validate().map_err(controller_snapshot_error)?;

        let now_unix_millis = current_millis();
        let timeout_millis = authorization
            .deadline_unix_millis()
            .checked_sub(now_unix_millis)
            .filter(|remaining| *remaining > 0)
            .ok_or(RocketMQError::ControllerConsensusTimeout {
                operation: "create release snapshot",
                timeout_ms: 0,
            })?;
        let deadline = ShutdownDeadline::after(Duration::from_millis(timeout_millis));
        let node = self
            .node()
            .ok_or_else(|| RocketMQError::not_initialized("OpenRaft node is not started"))?;

        let read_barrier = tokio::time::timeout(deadline.remaining(), node.ensure_linearizable_read())
            .await
            .map_err(|_| RocketMQError::ControllerConsensusTimeout {
                operation: "release snapshot ReadIndex",
                timeout_ms: timeout_millis,
            })?
            .map_err(RocketMQError::Controller)?
            .ok_or_else(|| controller_snapshot_error("Controller has no committed Raft state to snapshot"))?;

        tokio::time::timeout(deadline.remaining(), node.raft().trigger().snapshot())
            .await
            .map_err(|_| RocketMQError::ControllerConsensusTimeout {
                operation: "trigger release snapshot",
                timeout_ms: timeout_millis,
            })?
            .map_err(|error| {
                RocketMQError::Controller(ControllerError::raft_source("trigger release snapshot", error))
            })?;

        let snapshot = loop {
            let current = tokio::time::timeout(deadline.remaining(), node.raft().get_snapshot())
                .await
                .map_err(|_| RocketMQError::ControllerConsensusTimeout {
                    operation: "await release snapshot",
                    timeout_ms: timeout_millis,
                })?
                .map_err(|error| {
                    RocketMQError::Controller(ControllerError::raft_source("read current release snapshot", error))
                })?;
            if let Some(snapshot) = current {
                if snapshot
                    .meta
                    .last_log_id
                    .is_some_and(|last_log_id| last_log_id >= read_barrier)
                {
                    break snapshot;
                }
            }
            if deadline.is_expired() {
                return Err(RocketMQError::ControllerConsensusTimeout {
                    operation: "await release snapshot",
                    timeout_ms: timeout_millis,
                });
            }
            tokio::task::yield_now().await;
        };

        let snapshot_id = snapshot.meta.snapshot_id.clone();
        let last_applied = snapshot
            .meta
            .last_log_id
            .ok_or_else(|| controller_snapshot_error("release snapshot has no last-applied Raft position"))?;
        let voter_ids = snapshot.meta.last_membership.voter_ids().collect::<Vec<_>>();
        let payload = snapshot.snapshot.into_inner();
        crate::openraft::validate_snapshot_payload(&payload).map_err(controller_snapshot_error)?;
        let max_snapshot_bytes =
            (crate::openraft::SNAPSHOT_MAX_BYTES as u64).min(authorization.resource_budget().max_checkpoint_bytes);
        if payload.len() as u64 > max_snapshot_bytes {
            return Err(RocketMQError::MessageTooLarge {
                actual: payload.len(),
                limit: usize::try_from(max_snapshot_bytes).unwrap_or(usize::MAX),
            });
        }

        let (payload, sha256) = self
            .service_context
            .cpu_crypto()
            .spawn_until(
                "controller.release-snapshot.sha256",
                BlockingKind::CpuBound,
                deadline,
                move || {
                    let sha256 = hex::encode(Sha256::digest(&payload));
                    (payload, sha256)
                },
            )
            .await
            .map_err(|error| RocketMQError::internal("hash controller release snapshot", error))?;
        let node_id = self.config.snapshot().node_id;
        let manifest = ControllerReleaseSnapshotManifest {
            artifact: ReleaseCheckpointArtifact {
                schema_version: RELEASE_CHECKPOINT_SCHEMA_VERSION,
                checkpoint_id: request.checkpoint_id,
                checkpoint_set_id: request.checkpoint_set_id,
                generation: request.generation,
                barrier_id: request.barrier_id,
                created_at_unix_millis: current_millis(),
                length_bytes: payload.len() as u64,
                sha256: sha256.clone(),
                uri: format!("controller://node-{node_id}/objects/{sha256}/{snapshot_id}"),
            },
            snapshot_id,
            last_applied_index: last_applied.index,
            last_applied_term: last_applied.leader_id.term,
            voter_ids,
        };
        manifest.validate().map_err(controller_snapshot_error)?;

        let snapshot = ControllerReleaseSnapshot { manifest, payload };
        self.release_snapshot_repository()?
            .publish(authorization, &snapshot)
            .await?;
        Ok(snapshot)
    }

    /// Verifies a durably published Controller release snapshot without installing it.
    ///
    /// # Errors
    ///
    /// Returns a typed error when authorization, deadline, manifest binding,
    /// resource budget, immutable artifact I/O, or snapshot integrity fails.
    pub async fn verify_release_snapshot(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        manifest: &ControllerReleaseSnapshotManifest,
    ) -> RocketMQResult<ReleaseCheckpointRestoreVerification> {
        self.release_snapshot_repository()?
            .verify(authorization, manifest)
            .await
    }

    fn release_snapshot_repository(&self) -> RocketMQResult<ControllerReleaseSnapshotRepository> {
        let config = self.config.snapshot();
        if config.maintenance_checkpoint_root.trim().is_empty() {
            return Err(RocketMQError::not_initialized(
                "Controller maintenance checkpoint root is not configured",
            ));
        }
        Ok(ControllerReleaseSnapshotRepository::new(
            PathBuf::from(&config.maintenance_checkpoint_root),
            config.node_id,
            &self.service_context,
        ))
    }

    pub(crate) async fn add_learner(&self, node_id: NodeId, node_info: Node, blocking: bool) -> Result<()> {
        let node = self
            .node()
            .ok_or_else(|| ControllerError::NotInitialized("OpenRaft node is not started".to_string()))?;
        node.add_learner(node_id, node_info, blocking).await
    }

    pub(crate) async fn change_membership(&self, members: BTreeSet<NodeId>, retain: bool) -> Result<()> {
        let node = self
            .node()
            .ok_or_else(|| ControllerError::NotInitialized("OpenRaft node is not started".to_string()))?;
        node.change_membership(members, retain).await
    }

    /// Applies one authorized, version-fenced, process-local-idempotent membership step.
    ///
    /// This boundary temporarily reuses the release-maintenance
    /// [`rocketmq_security_api::MaintenanceCapability::ReleaseCheckpoint`] permission. A future
    /// versioned maintenance policy should split membership administration into a dedicated
    /// capability.
    ///
    /// # Errors
    ///
    /// Returns a typed error when authorization, request validation, optimistic membership
    /// fencing, learner catch-up, remaining quorum, deadline, or consensus verification fails.
    pub async fn apply_membership_change(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        request: MembershipChangeRequest,
    ) -> RocketMQResult<MembershipChangeOutcome> {
        let node = self
            .node()
            .ok_or_else(|| RocketMQError::not_initialized("OpenRaft node is not started"))?;
        node.apply_membership_change(authorization, request).await
    }

    /// Returns the current OpenRaft-independent consensus membership projection.
    pub async fn consensus_membership(&self) -> Result<ConsensusMembership> {
        let node = self
            .node()
            .ok_or_else(|| ControllerError::NotInitialized("OpenRaft node is not started".to_string()))?;
        node.consensus_membership().await
    }

    pub async fn allow_next_revert(&self, node_id: NodeId, allow: bool) -> Result<()> {
        let node = self
            .node()
            .ok_or_else(|| ControllerError::NotInitialized("OpenRaft node is not started".to_string()))?;
        node.allow_next_revert(node_id, allow).await
    }

    pub fn set_runtime_tick_enabled(&self, enabled: bool) -> Result<()> {
        let node = self
            .node()
            .ok_or_else(|| ControllerError::NotInitialized("OpenRaft node is not started".to_string()))?;
        node.raft().runtime_config().tick(enabled);
        Ok(())
    }

    pub fn set_runtime_heartbeat_enabled(&self, enabled: bool) -> Result<()> {
        let node = self
            .node()
            .ok_or_else(|| ControllerError::NotInitialized("OpenRaft node is not started".to_string()))?;
        node.raft().runtime_config().heartbeat(enabled);
        Ok(())
    }

    pub fn set_runtime_elect_enabled(&self, enabled: bool) -> Result<()> {
        let node = self
            .node()
            .ok_or_else(|| ControllerError::NotInitialized("OpenRaft node is not started".to_string()))?;
        node.raft().runtime_config().elect(enabled);
        Ok(())
    }

    pub fn has_committed_log(&self) -> Result<bool> {
        let node = self
            .node()
            .ok_or_else(|| ControllerError::NotInitialized("OpenRaft node is not started".to_string()))?;
        Ok(node.has_committed_log())
    }

    pub async fn has_persisted_committed_log(&self) -> Result<bool> {
        let node = self
            .node()
            .ok_or_else(|| ControllerError::NotInitialized("OpenRaft node is not started".to_string()))?;
        Ok(node.has_persisted_committed_log().await)
    }

    pub(crate) async fn startup_shared(&self) -> RocketMQResult<()> {
        let _lifecycle_guard = self.lifecycle_lock.lock().await;
        if self.node().is_some() {
            info!("OpenRaft controller is already started");
            return Ok(());
        }

        let startup_config = self.config.snapshot();
        let advertised_raft_addr = startup_config.local_raft_addr();
        let raft_bind_addr = resolve_controller_raft_bind_addr(advertised_raft_addr)?;
        info!(
            "Starting OpenRaft controller, remoting_addr={}, raft_bind_addr={}, advertised_raft_addr={}",
            startup_config.listen_addr, raft_bind_addr, advertised_raft_addr
        );

        let addr = raft_bind_addr;
        let node_id = startup_config.node_id;
        let listener = TcpListener::bind(addr).await.map_err(|error| {
            RocketMQError::network_connection_failed(
                addr.to_string(),
                format!("bind OpenRaft gRPC server for node {node_id}: {error}"),
            )
        })?;
        let task_group = self.ensure_task_group()?;
        let node =
            Arc::new(RaftNodeManager::new(self.config.clone(), self.service_context.storage_io().clone()).await?);
        let service = GrpcRaftService::new(node.raft());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let shutdown_token = task_group.cancellation_token();

        if let Err(error) = task_group.spawn_service("controller.openraft.grpc-server", async move {
            info!("gRPC server starting for node {} on {}", node_id, addr);

            let result = Server::builder()
                .add_service(OpenRaftServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    tokio::select! {
                        _ = shutdown_rx => {}
                        _ = shutdown_token.cancelled() => {}
                    }
                    info!("Shutdown signal received for node {}, stopping gRPC server", node_id);
                })
                .await;

            if let Err(e) = result {
                eprintln!("gRPC server error for node {}: {}", node_id, e);
            } else {
                info!("gRPC server for node {} stopped gracefully", node_id);
            }
        }) {
            let _ = tokio::time::timeout(Duration::from_secs(10), node.shutdown()).await;
            self.shutdown_task_group().await;
            return Err(openraft_startup_failed(
                "spawn gRPC server task",
                format!("node {node_id}: {error}"),
            ));
        }

        let mut lifecycle = self.lifecycle.lock();
        lifecycle.node = Some(node);
        lifecycle.shutdown_tx = Some(shutdown_tx);
        drop(lifecycle);

        info!("OpenRaft controller started successfully");
        Ok(())
    }

    pub(crate) async fn shutdown_shared(&self) -> RocketMQResult<()> {
        let _lifecycle_guard = self.lifecycle_lock.lock().await;
        info!("Shutting down OpenRaft controller");

        self.scheduling.store(false, Ordering::Release);
        self.stop_scan_task_group().await;

        let (shutdown_tx, node) = {
            let mut lifecycle = self.lifecycle.lock();
            (lifecycle.shutdown_tx.take(), lifecycle.node.take())
        };

        if let Some(tx) = shutdown_tx {
            if tx.send(()).is_err() {
                eprintln!("Failed to send shutdown signal to gRPC server (receiver dropped)");
            } else {
                info!("Shutdown signal sent to gRPC server");
            }
        }

        if let Some(node) = node {
            match tokio::time::timeout(tokio::time::Duration::from_secs(10), node.shutdown()).await {
                Ok(Ok(())) => info!("Raft node shutdown successfully"),
                Ok(Err(e)) => eprintln!("Error shutting down Raft node: {}", e),
                Err(_) => eprintln!("Timeout waiting for Raft node shutdown"),
            }
        }

        self.shutdown_task_group().await;

        info!("OpenRaft controller shutdown completed");
        Ok(())
    }

    pub fn scheduling_enabled(&self) -> bool {
        self.scheduling.load(Ordering::Acquire)
    }

    pub(crate) fn record_election_attempt(&self, latency_ms: u64) {
        self.metrics_manager.record_election_attempt(latency_ms);
    }
}

impl Controller for OpenRaftController {
    async fn startup(&mut self) -> RocketMQResult<()> {
        self.startup_shared().await
    }

    async fn shutdown(&mut self) -> RocketMQResult<()> {
        self.shutdown_shared().await
    }

    async fn start_scheduling(&self) -> RocketMQResult<()> {
        if self.scheduling.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let Some(node) = self.node() else {
            return Ok(());
        };

        let config = self.config.clone();
        let initial_config = config.snapshot();
        let listeners = self.lifecycle_listeners.clone();
        let scheduling = self.scheduling.clone();
        let first_received_heartbeat_time = self.first_received_heartbeat_time.clone();
        let metrics_manager = self.metrics_manager.clone();
        let scheduled_tasks = self.start_scan_task_group().await?;
        let mut task_config = ScheduledTaskConfig::fixed_delay(
            "controller.openraft.scan-not-active-broker",
            Duration::from_millis(initial_config.scan_not_active_broker_interval.max(1)),
        );
        task_config.initial_delay = Duration::from_millis(2000);
        if let Err(error) = scheduled_tasks.schedule_fixed_delay(task_config, move || {
            let node = node.clone();
            let config = config.clone();
            let listeners = listeners.clone();
            let scheduling = scheduling.clone();
            let first_received_heartbeat_time = first_received_heartbeat_time.clone();
            let metrics_manager = metrics_manager.clone();
            async move {
                if !scheduling.load(Ordering::Acquire) {
                    return;
                }

                use openraft::async_runtime::WatchReceiver;
                let metrics = node.raft().metrics().borrow_watched().clone();
                let config = config.snapshot();
                metrics_manager
                    .record_quorum_health(metrics.running_state.is_ok() && metrics.last_quorum_acked.is_some());
                if metrics.current_leader != Some(config.node_id) {
                    return;
                }

                let first_heartbeat = first_received_heartbeat_time.load(Ordering::Acquire);
                let now_millis = current_millis();
                if first_heartbeat == 0 || now_millis < first_heartbeat.saturating_add(config.raft_scan_wait_timeout_ms)
                {
                    return;
                }

                let heartbeat_ages = node
                    .store()
                    .state_machine
                    .replicas_info_manager()
                    .heartbeat_age_samples_at(now_millis);
                for age_millis in heartbeat_ages {
                    metrics_manager.record_heartbeat_age(age_millis);
                }

                let scan_span = rocketmq_observability::trace::controller::heartbeat_scan_span();
                match Self::scan_not_active_broker_once(node.clone(), now_millis)
                    .instrument(scan_span.clone())
                    .await
                {
                    Ok(inactive_brokers) => {
                        let stale_count = inactive_brokers.len();
                        metrics_manager.record_stale_brokers(u64::try_from(stale_count).unwrap_or(u64::MAX));
                        scan_span.record("result", "success");
                        scan_span.record("stale_count", i64::try_from(stale_count).unwrap_or(i64::MAX));
                        for broker_identity in inactive_brokers {
                            for listener in listeners.read().iter() {
                                listener.on_broker_inactive(
                                    Some(broker_identity.cluster_name.as_str()),
                                    broker_identity.broker_name.as_str(),
                                    broker_identity.broker_id.map(|broker_id| broker_id as i64),
                                );
                            }
                        }
                        info!(
                            event = rocketmq_observability::semantic::events::CONTROLLER_HEARTBEAT,
                            state = "scanned",
                            result = "success",
                            stale_count,
                            "Controller replicated heartbeat scan completed"
                        );
                    }
                    Err(_) => {
                        scan_span.record("result", "error");
                        scan_span.record("stale_count", 0_i64);
                        tracing::warn!(
                            event = rocketmq_observability::semantic::events::CONTROLLER_HEARTBEAT,
                            state = "scanned",
                            result = "error",
                            stale_count = 0_usize,
                            "Controller replicated heartbeat scan failed"
                        );
                    }
                }
            }
        }) {
            self.scheduling.store(false, Ordering::Release);
            self.stop_scan_task_group().await;
            return Err(openraft_startup_failed("schedule active broker scan task", error));
        }
        Ok(())
    }

    async fn stop_scheduling(&self) -> RocketMQResult<()> {
        self.scheduling.store(false, Ordering::Release);
        self.stop_scan_task_group().await;
        Ok(())
    }

    fn is_leader(&self) -> bool {
        self.is_current_leader()
    }

    async fn register_broker(
        &self,
        request: &RegisterBrokerToControllerRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let cluster_name = request.cluster_name.clone().unwrap_or_default();
        let broker_name = request.broker_name.clone().unwrap_or_default();
        let broker_address = request.broker_address.clone().unwrap_or_default();
        let broker_id = request.broker_id.unwrap_or_default() as u64;
        let Some(read_state) = self.linearizable_replicas_info_manager().await? else {
            return Ok(self.not_started_response());
        };
        let alive_broker_ids = read_state.active_broker_ids(cluster_name.as_str(), broker_name.as_str());

        let response = self
            .write_request(ControllerRequest::RegisterBroker {
                cluster_name: cluster_name.to_string(),
                broker_name: broker_name.to_string(),
                broker_address: broker_address.to_string(),
                broker_id,
                alive_broker_ids,
            })
            .await?;
        let Some(response) = response else {
            return Ok(None);
        };

        if ResponseCode::from(response.code()) != ResponseCode::Success {
            return Ok(Some(response));
        }

        let Some(replicas_info_manager) = self.linearizable_replicas_info_manager().await? else {
            return Ok(Some(response));
        };
        let replica_info = replicas_info_manager.get_replica_info(broker_name.as_str());
        let mut register_header = RegisterBrokerToControllerResponseHeader {
            cluster_name: Some(cluster_name.clone()),
            broker_name: Some(broker_name.clone()),
            master_broker_id: None,
            master_address: None,
            master_epoch: None,
            sync_state_set_epoch: None,
        };

        if let Some(replica_header) = replica_info.response() {
            if let Some(master_broker_id) = replica_header.master_broker_id {
                if replicas_info_manager.is_broker_promotion_ready_at(
                    cluster_name.as_str(),
                    broker_name.as_str(),
                    master_broker_id,
                    rocketmq_runtime::common::time_utils::current_millis(),
                ) {
                    register_header.master_broker_id = Some(master_broker_id);
                    register_header.master_address =
                        replica_header.master_address.clone().map(CheetahString::from_string);
                    register_header.master_epoch = replica_header.master_epoch;
                }
            }
        }

        let mut command = self
            .command_factory
            .create_success_response_command_with_header(register_header.clone());
        if let Some(body) = replica_info.body().cloned() {
            if let Ok(sync_state_set) = SyncStateSet::decode(body.as_ref()) {
                register_header.sync_state_set_epoch = Some(sync_state_set.get_sync_state_set_epoch());
                command = command.set_command_custom_header(register_header);
            }
            command = command.set_body(body);
        }
        Ok(Some(command))
    }

    async fn get_next_broker_id(
        &self,
        request: &GetNextBrokerIdRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let Some(replicas_info_manager) = self.linearizable_replicas_info_manager().await? else {
            return Ok(self.not_started_response());
        };

        let result =
            replicas_info_manager.get_next_broker_id(request.cluster_name.as_str(), request.broker_name.as_str());
        Ok(Some(self.command_from_result(result)))
    }

    async fn apply_broker_id(&self, request: &ApplyBrokerIdRequestHeader) -> RocketMQResult<Option<RemotingCommand>> {
        if request.applied_broker_id < 0 {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::ControllerBrokerIdInvalid,
                format!(
                    "Invalid broker ID: {}. Broker ID must be non-negative.",
                    request.applied_broker_id
                ),
            )));
        }

        self.write_request(ControllerRequest::ApplyBrokerId {
            cluster_name: request.cluster_name.to_string(),
            broker_name: request.broker_name.to_string(),
            broker_address: request
                .register_check_code
                .split_char(';')
                .next()
                .unwrap_or_default()
                .to_string(),
            applied_broker_id: request.applied_broker_id as u64,
            register_check_code: request.register_check_code.to_string(),
        })
        .await
    }

    async fn clean_broker_data(
        &self,
        request: &CleanBrokerDataRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let cluster_name = request.cluster_name.clone().unwrap_or_default();
        let Some(read_state) = self.linearizable_replicas_info_manager().await? else {
            return Ok(self.not_started_response());
        };
        let alive_broker_ids = read_state.active_broker_ids(cluster_name.as_str(), request.broker_name.as_str());

        self.write_request(ControllerRequest::CleanBrokerData {
            cluster_name: cluster_name.to_string(),
            broker_name: request.broker_name.to_string(),
            broker_controller_ids_to_clean: request
                .broker_controller_ids_to_clean
                .as_ref()
                .map(|ids| ids.to_string()),
            clean_living_broker: request.clean_living_broker,
            alive_broker_ids,
        })
        .await
    }

    async fn elect_master(&self, request: &ElectMasterRequestHeader) -> RocketMQResult<Option<RemotingCommand>> {
        let Some(read_state) = self.linearizable_replicas_info_manager().await? else {
            return Ok(self.not_started_response());
        };
        let alive_broker_ids =
            read_state.active_broker_ids(request.cluster_name.as_str(), request.broker_name.as_str());
        let live_broker_infos = Self::snapshot_live_broker_infos(
            read_state.as_ref(),
            request.cluster_name.as_str(),
            request.broker_name.as_str(),
            &alive_broker_ids,
        );

        self.write_request(ControllerRequest::ElectMaster {
            cluster_name: request.cluster_name.to_string(),
            broker_name: request.broker_name.to_string(),
            broker_id: (request.broker_id >= 0).then_some(request.broker_id as u64),
            designate_elect: request.designate_elect,
            alive_broker_ids,
            live_broker_infos,
        })
        .await
    }

    async fn alter_sync_state_set(
        &self,
        request: &AlterSyncStateSetRequestHeader,
        sync_state_set: SyncStateSet,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let Some(replicas_info_manager) = self.linearizable_replicas_info_manager().await? else {
            return Ok(self.not_started_response());
        };

        let cluster_name = replicas_info_manager
            .cluster_name(request.broker_name.as_str())
            .unwrap_or_default();
        let new_sync_state_set = sync_state_set
            .get_sync_state_set()
            .map(|state_set| state_set.iter().copied().map(|id| id as u64).collect())
            .unwrap_or_default();
        let alive_broker_ids =
            replicas_info_manager.active_broker_ids(cluster_name.as_str(), request.broker_name.as_str());

        self.write_request(ControllerRequest::AlterSyncStateSet {
            cluster_name,
            broker_name: request.broker_name.to_string(),
            master_broker_id: request.master_broker_id as u64,
            master_epoch: request.master_epoch,
            new_sync_state_set,
            sync_state_set_epoch: sync_state_set.get_sync_state_set_epoch(),
            alive_broker_ids,
        })
        .await
    }

    async fn get_replica_info(&self, request: &GetReplicaInfoRequestHeader) -> RocketMQResult<Option<RemotingCommand>> {
        let Some(replicas_info_manager) = self.linearizable_replicas_info_manager().await? else {
            return Ok(self.not_started_response());
        };

        let result = replicas_info_manager.get_replica_info(&request.broker_name);
        Ok(Some(self.command_from_result(result)))
    }

    async fn get_controller_metadata(&self) -> RocketMQResult<Option<RemotingCommand>> {
        let controller_metadata_info: GetMetaDataResponseHeader = {
            let config = self.config.snapshot();
            let peers: Option<CheetahString> = {
                let joined = config
                    .controller_peer_addrs()
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<String>>()
                    .join(";");

                (!joined.is_empty()).then_some(joined.as_str().into())
            };

            let raft_node_manager = self.node();
            let (controller_leader_id, last_log_index, committed_log_index, applied_log_index) =
                if let Some(raft_node_manager) = raft_node_manager {
                    let metrics = raft_node_manager.raft_metrics();
                    (
                        metrics.current_leader,
                        metrics.last_log_index,
                        metrics.committed.map(|log_id| log_id.index),
                        metrics.last_applied.map(|log_id| log_id.index),
                    )
                } else {
                    (None, None, None, None)
                };

            let controller_leader_address: Option<CheetahString> = controller_leader_id
                .and_then(|leader_node_id| config.controller_addr_for(leader_node_id))
                .map(|addr| CheetahString::from(addr.to_string()));

            let is_leader = controller_leader_id.map(|id| config.node_id == id);

            let controller_leader_id: Option<CheetahString> =
                controller_leader_id.map(|id| CheetahString::from(id.to_string()));

            GetMetaDataResponseHeader {
                group: None,
                controller_leader_id,
                controller_leader_address,
                is_leader,
                peers,
                last_log_index,
                committed_log_index,
                applied_log_index,
            }
        };
        Ok(Some(
            self.command_factory
                .create_success_response_command_with_header(controller_metadata_info),
        ))
    }

    async fn get_sync_state_data(&self, broker_names: &[CheetahString]) -> RocketMQResult<Option<RemotingCommand>> {
        let Some(replicas_info_manager) = self.linearizable_replicas_info_manager().await? else {
            return Ok(self.not_started_response());
        };

        let broker_names_str: Vec<String> = broker_names.iter().map(|s| s.to_string()).collect();
        let result = replicas_info_manager.get_sync_state_data(&broker_names_str, replicas_info_manager.as_ref());

        Ok(Some(self.command_from_result_without_header(result)))
    }

    fn register_broker_lifecycle_listener(&self, listener: Arc<dyn BrokerLifecycleListener>) {
        self.lifecycle_listeners.write().push(listener);
    }
}

/// Resolves the actual Raft listener address used by standalone and embedded Controllers.
///
/// # Errors
///
/// Returns a typed configuration error when `ROCKETMQ_CONTROLLER_RAFT_BIND_ADDR`
/// is non-UTF-8 or not a socket address.
pub fn resolve_controller_raft_bind_addr(fallback: SocketAddr) -> RocketMQResult<SocketAddr> {
    let Some(raw) = std::env::var_os(CONTROLLER_RAFT_BIND_ADDR_ENV) else {
        return Ok(fallback);
    };
    let raw = raw.into_string().map_err(|_| {
        ControllerError::ConfigError(format!("{CONTROLLER_RAFT_BIND_ADDR_ENV} must contain valid UTF-8"))
    })?;
    parse_controller_raft_bind_addr(&raw).map_err(|error| {
        ControllerError::ConfigError(format!(
            "{CONTROLLER_RAFT_BIND_ADDR_ENV} must be a socket address: {error}"
        ))
        .into()
    })
}

fn parse_controller_raft_bind_addr(raw: &str) -> std::result::Result<SocketAddr, std::net::AddrParseError> {
    raw.parse::<SocketAddr>()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::TcpListener as StdTcpListener;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::config::ControllerConfig;
    use crate::config::RaftPeer;
    use rocketmq_error::ErrorKind;

    use super::openraft_response_decode_failed;
    use super::openraft_startup_failed;
    use super::parse_controller_raft_bind_addr;
    use super::OpenRaftController;
    use crate::config::ControllerConfigReader;
    use crate::typ::Node;
    use std::net::SocketAddr;

    #[test]
    fn openraft_startup_failed_uses_service_error_kind() {
        let error = openraft_startup_failed("spawn test service", "task group closed");

        assert_eq!(error.kind(), ErrorKind::Service);
        assert!(error.to_string().contains("OpenRaft controller spawn test service"));
    }

    #[test]
    fn openraft_response_decode_failed_uses_serialization_error_kind() {
        let serde_error = serde_json::from_str::<serde_json::Value>("{").expect_err("invalid json");
        let error = openraft_response_decode_failed(serde_error);

        assert_eq!(error.kind(), ErrorKind::Serialization);
        assert!(error.to_string().contains("OpenRaft inactive broker scan response"));
    }

    #[test]
    fn controller_raft_bind_override_requires_a_socket_address() {
        assert_eq!(
            parse_controller_raft_bind_addr("0.0.0.0:60110").expect("valid Kubernetes Raft bind address"),
            SocketAddr::from(([0, 0, 0, 0], 60110))
        );
        assert!(parse_controller_raft_bind_addr("rocketmq-controller:60110").is_err());
    }

    #[tokio::test]
    async fn shared_lifecycle_serializes_startup_and_shutdown() {
        let probe = StdTcpListener::bind("127.0.0.1:0").expect("bind probe listener");
        let addr = probe.local_addr().expect("read probe address");
        drop(probe);

        let config = ControllerConfig::default()
            .with_node_info(1, addr)
            .with_raft_peers(vec![RaftPeer { id: 1, addr }])
            .with_storage_backend(crate::config::StorageBackendType::Memory);
        let context = rocketmq_runtime::RuntimeContext::from_current("openraft-lifecycle-test");
        let controller = Arc::new(OpenRaftController::new(
            ControllerConfigReader::new(config),
            context.service_context("openraft"),
        ));

        let (first_start, second_start) = tokio::join!(controller.startup_shared(), controller.startup_shared());
        first_start.expect("start OpenRaft controller");
        second_start.expect("repeat OpenRaft controller startup");
        assert!(controller.node().is_some(), "startup should publish the Raft node");
        assert!(
            !controller.has_recovered_cluster_state(),
            "a bound Raft listener alone must not publish business readiness"
        );
        assert!(
            StdTcpListener::bind(addr).is_err(),
            "OpenRaft controller startup should return only after binding the gRPC listener"
        );

        controller
            .initialize_cluster(BTreeMap::from([(
                1,
                Node {
                    node_id: 1,
                    rpc_addr: addr.to_string(),
                },
            )]))
            .await
            .expect("initialize single-node cluster");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !controller.has_recovered_cluster_state() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Controller should observe a leader and committed state");

        let (first_shutdown, second_shutdown) =
            tokio::join!(controller.shutdown_shared(), controller.shutdown_shared());
        first_shutdown.expect("shutdown OpenRaft controller");
        second_shutdown.expect("repeat OpenRaft controller shutdown");
        assert!(controller.node().is_none(), "shutdown should clear the Raft node");
        let rebound = StdTcpListener::bind(addr).expect("shutdown should release the gRPC listener");
        drop(rebound);
    }
}
