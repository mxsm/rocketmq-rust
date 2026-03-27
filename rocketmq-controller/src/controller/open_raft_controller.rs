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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::controller::broker_heartbeat_manager::BrokerHeartbeatManager;
use crate::controller::Controller;
use crate::error::ControllerError;
use crate::error::Result;
use crate::event::controller_result::ControllerResult as EventControllerResult;
use crate::heartbeat::default_broker_heartbeat_manager::DefaultBrokerHeartbeatManager;
use crate::helper::broker_lifecycle_listener::BrokerLifecycleListener;
use crate::openraft::GrpcRaftService;
use crate::openraft::RaftNodeManager;
use crate::protobuf::openraft::open_raft_service_server::OpenRaftServiceServer;
use crate::typ::BrokerLiveInfoSnapshot;
use crate::typ::ControllerRequest;
use crate::typ::Node;
use crate::typ::NodeId;
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
use rocketmq_remoting::protocol::header::controller::register_broker_to_controller_response_header::RegisterBrokerToControllerResponseHeader;
use rocketmq_remoting::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::CommandCustomHeader;
use rocketmq_remoting::protocol::RemotingDeserializable;
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
        Self {
            config,
            node: None,
            handle: None,
            shutdown_tx: None,
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

    fn not_started_response(&self) -> Option<RemotingCommand> {
        Some(RemotingCommand::create_response_command_with_code_remark(
            ResponseCode::SystemError,
            "The controller raft node is not started",
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

    fn replicas_info_manager(&self) -> Option<Arc<ReplicasInfoManager>> {
        self.node
            .as_ref()
            .map(|node| node.store().state_machine.replicas_info_manager())
    }

    fn snapshot_alive_broker_ids(&self, cluster_name: &str, broker_name: &str) -> HashSet<u64> {
        self.replicas_info_manager()
            .map(|manager| {
                manager
                    .broker_ids(broker_name)
                    .into_iter()
                    .filter(|broker_id| {
                        self.heartbeat_manager
                            .is_broker_active(cluster_name, broker_name, *broker_id as i64)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn snapshot_live_broker_infos(
        &self,
        cluster_name: &str,
        broker_name: &str,
        alive_broker_ids: &HashSet<u64>,
    ) -> HashMap<u64, BrokerLiveInfoSnapshot> {
        alive_broker_ids
            .iter()
            .filter_map(|broker_id| {
                self.heartbeat_manager
                    .get_broker_live_info(cluster_name, broker_name, *broker_id as i64)
                    .map(|live_info| {
                        (
                            *broker_id,
                            BrokerLiveInfoSnapshot {
                                broker_id: *broker_id,
                                epoch: live_info.epoch(),
                                max_offset: live_info.max_offset(),
                                election_priority: live_info.election_priority(),
                            },
                        )
                    })
            })
            .collect()
    }

    async fn write_request(&self, request: ControllerRequest) -> RocketMQResult<Option<RemotingCommand>> {
        if !self.is_current_leader() {
            return Ok(self.not_leader_response());
        }

        let Some(node) = &self.node else {
            return Ok(self.not_started_response());
        };

        let response = node.client_write(request).await?;
        Ok(Some(response.data.into_remoting_command()))
    }

    pub async fn initialize_cluster(&self, nodes: BTreeMap<NodeId, Node>) -> Result<()> {
        let node = self
            .node
            .as_ref()
            .ok_or_else(|| ControllerError::NotInitialized("OpenRaft node is not started".to_string()))?;
        node.initialize_cluster(nodes).await
    }

    pub async fn add_learner(&self, node_id: NodeId, node_info: Node, blocking: bool) -> Result<()> {
        let node = self
            .node
            .as_ref()
            .ok_or_else(|| ControllerError::NotInitialized("OpenRaft node is not started".to_string()))?;
        node.add_learner(node_id, node_info, blocking).await
    }

    pub async fn change_membership(&self, members: BTreeSet<NodeId>, retain: bool) -> Result<()> {
        let node = self
            .node
            .as_ref()
            .ok_or_else(|| ControllerError::NotInitialized("OpenRaft node is not started".to_string()))?;
        node.change_membership(members, retain).await
    }

    pub fn has_committed_log(&self) -> Result<bool> {
        let node = self
            .node
            .as_ref()
            .ok_or_else(|| ControllerError::NotInitialized("OpenRaft node is not started".to_string()))?;
        Ok(node.has_committed_log())
    }
}

impl Controller for OpenRaftController {
    async fn startup(&mut self) -> RocketMQResult<()> {
        let raft_addr = self.config.local_raft_addr();
        info!(
            "Starting OpenRaft controller, remoting_addr={}, raft_addr={}",
            self.config.listen_addr, raft_addr
        );

        let node = Arc::new(RaftNodeManager::new(self.config.clone()).await?);
        let service = GrpcRaftService::new(node.raft());

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let addr = raft_addr;
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
        let cluster_name = request.cluster_name.clone().unwrap_or_default();
        let broker_name = request.broker_name.clone().unwrap_or_default();
        let broker_address = request.broker_address.clone().unwrap_or_default();
        let broker_id = request.broker_id.unwrap_or_default() as u64;
        let alive_broker_ids = self.snapshot_alive_broker_ids(cluster_name.as_str(), broker_name.as_str());

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

        let Some(replicas_info_manager) = self.replicas_info_manager() else {
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
                if self.heartbeat_manager.is_broker_active(
                    cluster_name.as_str(),
                    broker_name.as_str(),
                    master_broker_id,
                ) {
                    register_header.master_broker_id = Some(master_broker_id);
                    register_header.master_address =
                        replica_header.master_address.clone().map(CheetahString::from_string);
                    register_header.master_epoch = replica_header.master_epoch;
                }
            }
        }

        let mut command = RemotingCommand::create_response_command()
            .set_code(ResponseCode::Success)
            .set_command_custom_header(register_header.clone());
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
        let Some(replicas_info_manager) = self.replicas_info_manager() else {
            return Ok(self.not_started_response());
        };

        let result =
            replicas_info_manager.get_next_broker_id(request.cluster_name.as_str(), request.broker_name.as_str());
        Ok(Some(self.command_from_result(result)))
    }

    async fn apply_broker_id(&self, request: &ApplyBrokerIdRequestHeader) -> RocketMQResult<Option<RemotingCommand>> {
        if request.applied_broker_id < 0 {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
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
                .split(';')
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
        let alive_broker_ids = self.snapshot_alive_broker_ids(cluster_name.as_str(), request.broker_name.as_str());

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
        let alive_broker_ids =
            self.snapshot_alive_broker_ids(request.cluster_name.as_str(), request.broker_name.as_str());
        let live_broker_infos = self.snapshot_live_broker_infos(
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
        let Some(replicas_info_manager) = self.replicas_info_manager() else {
            return Ok(self.not_started_response());
        };

        let cluster_name = replicas_info_manager
            .cluster_name(request.broker_name.as_str())
            .unwrap_or_default();
        let new_sync_state_set = sync_state_set
            .get_sync_state_set()
            .map(|state_set| state_set.iter().copied().map(|id| id as u64).collect())
            .unwrap_or_default();
        let alive_broker_ids = self.snapshot_alive_broker_ids(cluster_name.as_str(), request.broker_name.as_str());

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
        let Some(replicas_info_manager) = self.replicas_info_manager() else {
            return Ok(self.not_started_response());
        };

        let result = replicas_info_manager.get_replica_info(&request.broker_name);
        Ok(Some(self.command_from_result(result)))
    }

    async fn get_controller_metadata(&self) -> RocketMQResult<Option<RemotingCommand>> {
        let controller_metadata_info: GetMetaDataResponseHeader = {
            let peers: Option<CheetahString> = {
                let joined = self
                    .config
                    .controller_peer_addrs()
                    .iter()
                    .map(std::string::ToString::to_string)
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
                .and_then(|leader_node_id| self.config.controller_addr_for(leader_node_id))
                .map(|addr| CheetahString::from(addr.to_string()));

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
        let Some(replicas_info_manager) = self.replicas_info_manager() else {
            return Ok(self.not_started_response());
        };

        let broker_names_str: Vec<String> = broker_names.iter().map(|s| s.to_string()).collect();
        let result = replicas_info_manager.get_sync_state_data(&broker_names_str, self.heartbeat_manager.as_ref());

        Ok(Some(self.command_from_result_without_header(result)))
    }

    fn register_broker_lifecycle_listener(&self, listener: Arc<dyn BrokerLifecycleListener>) {
        self.lifecycle_listeners.write().push(listener);
    }
}
