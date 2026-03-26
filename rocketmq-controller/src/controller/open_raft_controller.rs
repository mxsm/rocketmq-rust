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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::controller::Controller;
use crate::event::controller_result::ControllerResult as EventControllerResult;
use crate::heartbeat::default_broker_heartbeat_manager::DefaultBrokerHeartbeatManager;
use crate::helper::broker_lifecycle_listener::BrokerLifecycleListener;
use crate::openraft::GrpcRaftService;
use crate::openraft::RaftNodeManager;
use crate::protobuf::openraft::open_raft_service_server::OpenRaftServiceServer;
use crate::DefaultElectPolicy;
use crate::ReplicasInfoManager;
use cheetah_string::CheetahString;
use parking_lot::RwLock;
use rocketmq_common::common::controller::ControllerConfig;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::sync_state_set_body::SyncStateSet;
use rocketmq_remoting::protocol::header::controller::alter_sync_state_set_request_header::AlterSyncStateSetRequestHeader;
use rocketmq_remoting::protocol::header::controller::apply_broker_id_request_header::ApplyBrokerIdRequestHeader;
use rocketmq_remoting::protocol::header::controller::clean_broker_data_request_header::CleanBrokerDataRequestHeader;
use rocketmq_remoting::protocol::header::controller::elect_master_request_header::ElectMasterRequestHeader;
use rocketmq_remoting::protocol::header::controller::get_next_broker_id_request_header::GetNextBrokerIdRequestHeader;
use rocketmq_remoting::protocol::header::controller::get_replica_info_request_header::GetReplicaInfoRequestHeader;
use rocketmq_remoting::protocol::header::controller::register_broker_to_controller_request_header::RegisterBrokerToControllerRequestHeader;
use rocketmq_remoting::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::CommandCustomHeader;
use rocketmq_rust::ArcMut;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tracing::info;

/// OpenRaft-based controller implementation
///
/// # Graceful Shutdown
///
/// This implementation provides proper graceful shutdown:
/// 1. Sends shutdown signal to gRPC server via oneshot channel
/// 2. Waits for Raft node to shutdown cleanly
/// 3. Waits for gRPC server task to complete (with 10s timeout)
pub struct OpenRaftController {
    config: ArcMut<ControllerConfig>,
    /// Raft node manager
    node: Option<Arc<RaftNodeManager>>,
    /// gRPC server task handle
    handle: Option<JoinHandle<()>>,
    /// Shutdown signal sender for gRPC server
    shutdown_tx: Option<oneshot::Sender<()>>,

    replica_info_manager: Arc<ReplicasInfoManager>,

    heartbeat_manager: ArcMut<DefaultBrokerHeartbeatManager>,
    lifecycle_listeners: Arc<RwLock<Vec<Arc<dyn BrokerLifecycleListener>>>>,
    scheduling: Arc<AtomicBool>,
}

impl OpenRaftController {
    pub fn new(config: ArcMut<ControllerConfig>) -> Self {
        let heartbeat_manager = ArcMut::new(DefaultBrokerHeartbeatManager::new(config.clone()));
        Self::new_with_heartbeat(config, heartbeat_manager)
    }

    pub fn new_with_heartbeat(
        config: ArcMut<ControllerConfig>,
        heartbeat_manager: ArcMut<DefaultBrokerHeartbeatManager>,
    ) -> Self {
        let replica_info_manager = Arc::new(ReplicasInfoManager::new(config.clone()));
        Self {
            config,
            node: None,
            handle: None,
            shutdown_tx: None,
            replica_info_manager,
            heartbeat_manager,
            lifecycle_listeners: Arc::new(RwLock::new(Vec::new())),
            scheduling: Arc::new(AtomicBool::new(false)),
        }
    }

    fn is_current_leader(&self) -> bool {
        let Some(node) = &self.node else {
            return false;
        };

        use openraft::async_runtime::WatchReceiver;
        let metrics = node.raft().metrics().borrow_watched().clone();
        metrics.current_leader == Some(self.config.node_id)
    }

    fn not_leader_response(&self) -> Option<RemotingCommand> {
        Some(RemotingCommand::create_response_command_with_code_remark(
            ResponseCode::ControllerNotLeader,
            "The controller is not in leader state",
        ))
    }

    fn command_from_result<T>(&self, result: EventControllerResult<T>) -> RemotingCommand
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        let (_events, header, body, response_code, remark) = result.into_parts();
        let mut response = RemotingCommand::create_response_command().set_code(response_code);
        if let Some(remark) = remark {
            response = response.set_remark(remark);
        }
        if let Some(header) = header {
            response = response.set_command_custom_header(header);
        }
        if let Some(body) = body {
            response = response.set_body(body);
        }
        response
    }

    fn command_from_result_without_header(&self, result: EventControllerResult<()>) -> RemotingCommand {
        let (_events, _header, body, response_code, remark) = result.into_parts();
        let mut response = RemotingCommand::create_response_command().set_code(response_code);
        if let Some(remark) = remark {
            response = response.set_remark(remark);
        }
        if let Some(body) = body {
            response = response.set_body(body);
        }
        response
    }

    fn apply_events<T>(&self, result: &EventControllerResult<T>) {
        for event in result.events() {
            self.replica_info_manager.apply_event(event.as_ref());
        }
    }
}

impl Controller for OpenRaftController {
    async fn startup(&mut self) -> RocketMQResult<()> {
        info!("Starting OpenRaft controller on {}", self.config.listen_addr);

        let node = Arc::new(RaftNodeManager::new(self.config.clone()).await?);
        let service = GrpcRaftService::new(node.raft());

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let addr = self.config.listen_addr;
        let node_id = self.config.node_id;

        let handle = tokio::spawn(async move {
            info!("gRPC server starting for node {} on {}", node_id, addr);

            let result = Server::builder()
                .add_service(OpenRaftServiceServer::new(service))
                .serve_with_shutdown(addr, async {
                    shutdown_rx.await.ok();
                    info!("Shutdown signal received for node {}, stopping gRPC server", node_id);
                })
                .await;

            if let Err(e) = result {
                eprintln!("gRPC server error for node {}: {}", node_id, e);
            } else {
                info!("gRPC server for node {} stopped gracefully", node_id);
            }
        });

        self.node = Some(node);
        self.handle = Some(handle);
        self.shutdown_tx = Some(shutdown_tx);

        info!("OpenRaft controller started successfully");
        Ok(())
    }

    async fn shutdown(&mut self) -> RocketMQResult<()> {
        info!("Shutting down OpenRaft controller");

        // Take and send shutdown signal to gRPC server
        if let Some(tx) = self.shutdown_tx.take() {
            if tx.send(()).is_err() {
                eprintln!("Failed to send shutdown signal to gRPC server (receiver dropped)");
            } else {
                info!("Shutdown signal sent to gRPC server");
            }
        }

        // Shutdown Raft node
        if let Some(node) = self.node.take() {
            if let Err(e) = node.shutdown().await {
                eprintln!("Error shutting down Raft node: {}", e);
            } else {
                info!("Raft node shutdown successfully");
            }
        }

        // Wait for server task to complete (with timeout)
        if let Some(handle) = self.handle.take() {
            let timeout = tokio::time::Duration::from_secs(10);
            match tokio::time::timeout(timeout, handle).await {
                Ok(Ok(_)) => info!("Server task completed successfully"),
                Ok(Err(e)) => eprintln!("Server task panicked: {}", e),
                Err(_) => eprintln!("Timeout waiting for server task to complete"),
            }
        }

        info!("OpenRaft controller shutdown completed");
        Ok(())
    }

    async fn start_scheduling(&self) -> RocketMQResult<()> {
        self.scheduling.store(true, Ordering::Release);
        Ok(())
    }

    async fn stop_scheduling(&self) -> RocketMQResult<()> {
        self.scheduling.store(false, Ordering::Release);
        Ok(())
    }

    fn is_leader(&self) -> bool {
        self.is_current_leader()
    }

    async fn register_broker(
        &self,
        request: &RegisterBrokerToControllerRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        if !self.is_current_leader() {
            return Ok(self.not_leader_response());
        }

        let cluster_name = request.cluster_name.clone().unwrap_or_default();
        let broker_name = request.broker_name.clone().unwrap_or_default();
        let broker_address = request.broker_address.clone().unwrap_or_default();
        let broker_id = request.broker_id.unwrap_or_default() as u64;

        let result = self.replica_info_manager.register_broker(
            cluster_name.as_str(),
            broker_name.as_str(),
            broker_address.as_str(),
            broker_id,
            self.heartbeat_manager.as_ref(),
        );
        self.apply_events(&result);

        Ok(Some(self.command_from_result(result)))
    }

    async fn get_next_broker_id(
        &self,
        request: &GetNextBrokerIdRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let result = self
            .replica_info_manager
            .get_next_broker_id(request.cluster_name.as_str(), request.broker_name.as_str());
        Ok(Some(self.command_from_result(result)))
    }

    async fn apply_broker_id(&self, request: &ApplyBrokerIdRequestHeader) -> RocketMQResult<Option<RemotingCommand>> {
        if !self.is_current_leader() {
            return Ok(self.not_leader_response());
        }

        if request.applied_broker_id < 0 {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::ControllerBrokerIdInvalid,
                format!(
                    "Invalid broker ID: {}. Broker ID must be non-negative.",
                    request.applied_broker_id
                ),
            )));
        }

        let result = self.replica_info_manager.apply_broker_id(
            request.cluster_name.as_str(),
            request.broker_name.as_str(),
            request.register_check_code.split(';').next().unwrap_or_default(),
            request.applied_broker_id as u64,
            request.register_check_code.as_str(),
        );
        self.apply_events(&result);

        Ok(Some(self.command_from_result(result)))
    }

    async fn clean_broker_data(
        &self,
        request: &CleanBrokerDataRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        if !self.is_current_leader() {
            return Ok(self.not_leader_response());
        }

        let cluster_name = request.cluster_name.clone().unwrap_or_default();
        let result = self.replica_info_manager.clean_broker_data(
            cluster_name.as_str(),
            request.broker_name.as_str(),
            request.broker_controller_ids_to_clean.as_ref().map(|s| s.as_str()),
            request.clean_living_broker,
            self.heartbeat_manager.as_ref(),
        );
        self.apply_events(&result);

        Ok(Some(self.command_from_result_without_header(result)))
    }

    async fn elect_master(&self, request: &ElectMasterRequestHeader) -> RocketMQResult<Option<RemotingCommand>> {
        if !self.is_current_leader() {
            return Ok(self.not_leader_response());
        }

        let elect_policy = DefaultElectPolicy::default_instance();
        let result = self.replica_info_manager.elect_master(
            request.broker_name.as_str(),
            (request.broker_id >= 0).then_some(request.broker_id as u64),
            request.designate_elect,
            &elect_policy,
        );
        self.apply_events(&result);

        Ok(Some(self.command_from_result(result)))
    }

    async fn alter_sync_state_set(
        &self,
        request: &AlterSyncStateSetRequestHeader,
        sync_state_set: SyncStateSet,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        if !self.is_current_leader() {
            return Ok(self.not_leader_response());
        }

        let new_sync_state_set = sync_state_set
            .get_sync_state_set()
            .map(|state_set| state_set.iter().copied().map(|id| id as u64).collect())
            .unwrap_or_default();
        let result = self.replica_info_manager.alter_sync_state_set(
            request.broker_name.as_str(),
            request.master_broker_id as u64,
            request.master_epoch,
            new_sync_state_set,
            sync_state_set.get_sync_state_set_epoch(),
            self.heartbeat_manager.as_ref(),
        );
        self.apply_events(&result);

        Ok(Some(self.command_from_result(result)))
    }

    async fn get_replica_info(&self, request: &GetReplicaInfoRequestHeader) -> RocketMQResult<Option<RemotingCommand>> {
        let result = self.replica_info_manager.get_replica_info(&request.broker_name);
        Ok(Some(self.command_from_result(result)))
    }

    async fn get_controller_metadata(&self) -> RocketMQResult<Option<RemotingCommand>> {
        let controller_metadata_info: GetMetaDataResponseHeader = {
            let peers: Option<CheetahString> = {
                let joined = self
                    .config
                    .raft_peers
                    .iter()
                    .map(|raft_peer| raft_peer.addr.to_string())
                    .collect::<Vec<String>>()
                    .join(";");

                (!joined.is_empty()).then_some(joined.as_str().into())
            };

            let raft_node_manager = self.node.as_ref();
            let controller_leader_id: Option<u64> = if let Some(raft_node_manager) = raft_node_manager {
                raft_node_manager.get_leader().await.ok().flatten()
            } else {
                None
            };

            let controller_leader_address: Option<CheetahString> = controller_leader_id
                .and_then(|leader_node_id| {
                    self.config
                        .raft_peers
                        .iter()
                        .find(|raft_peer| raft_peer.id == leader_node_id)
                })
                .map(|node_info| CheetahString::from(node_info.addr.to_string()));

            let is_leader = controller_leader_id.map(|id| self.config.node_id == id);

            let controller_leader_id: Option<CheetahString> =
                controller_leader_id.map(|id| CheetahString::from(id.to_string()));

            GetMetaDataResponseHeader {
                group: None,
                controller_leader_id,
                controller_leader_address,
                is_leader,
                peers,
            }
        };
        Ok(Some(
            RemotingCommand::create_response_command().set_command_custom_header(controller_metadata_info),
        ))
    }

    async fn get_sync_state_data(&self, broker_names: &[CheetahString]) -> RocketMQResult<Option<RemotingCommand>> {
        let broker_names_str: Vec<String> = broker_names.iter().map(|s| s.to_string()).collect();
        let result = self
            .replica_info_manager
            .get_sync_state_data(&broker_names_str, self.heartbeat_manager.as_ref());

        Ok(Some(self.command_from_result_without_header(result)))
    }

    fn register_broker_lifecycle_listener(&self, listener: Arc<dyn BrokerLifecycleListener>) {
        self.lifecycle_listeners.write().push(listener);
    }
}
