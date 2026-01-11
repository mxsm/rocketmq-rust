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

use std::sync::Arc;

use crate::controller::Controller;
use crate::helper::broker_lifecycle_listener::BrokerLifecycleListener;
use crate::openraft::GrpcRaftService;
use crate::openraft::RaftNodeManager;
use crate::protobuf::openraft::open_raft_service_server::OpenRaftServiceServer;
use crate::ReplicasInfoManager;
use cheetah_string::CheetahString;
use rocketmq_common::common::controller::ControllerConfig;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::controller::controller_metadata_info::ControllerMetadataInfo;
use rocketmq_remoting::protocol::body::controller::node_info::NodeInfo;
use rocketmq_remoting::protocol::body::sync_state_set_body::SyncStateSet;
use rocketmq_remoting::protocol::header::controller::alter_sync_state_set_request_header::AlterSyncStateSetRequestHeader;
use rocketmq_remoting::protocol::header::controller::apply_broker_id_request_header::ApplyBrokerIdRequestHeader;
use rocketmq_remoting::protocol::header::controller::apply_broker_id_response_header::ApplyBrokerIdResponseHeader;
use rocketmq_remoting::protocol::header::controller::elect_master_request_header::ElectMasterRequestHeader;
use rocketmq_remoting::protocol::header::controller::get_next_broker_id_request_header::GetNextBrokerIdRequestHeader;
use rocketmq_remoting::protocol::header::controller::get_replica_info_request_header::GetReplicaInfoRequestHeader;
use rocketmq_remoting::protocol::header::controller::register_broker_to_controller_request_header::RegisterBrokerToControllerRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
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
    config: Arc<ControllerConfig>,
    /// Raft node manager
    node: Option<Arc<RaftNodeManager>>,
    /// gRPC server task handle
    handle: Option<JoinHandle<()>>,
    /// Shutdown signal sender for gRPC server
    shutdown_tx: Option<oneshot::Sender<()>>,

    replica_info_manager: Arc<ReplicasInfoManager>,
}

impl OpenRaftController {
    pub fn new(config: Arc<ControllerConfig>) -> Self {
        let replica_info_manager = Arc::new(ReplicasInfoManager::new(config.clone()));

        Self {
            config,
            node: None,
            handle: None,
            shutdown_tx: None,
            replica_info_manager,
        }
    }
}

impl Controller for OpenRaftController {
    async fn startup(&mut self) -> RocketMQResult<()> {
        info!("Starting OpenRaft controller on {}", self.config.listen_addr);

        let node = Arc::new(RaftNodeManager::new(Arc::clone(&self.config)).await?);
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
        // TODO: Start leader scheduling tasks
        Ok(())
    }

    async fn stop_scheduling(&self) -> RocketMQResult<()> {
        // TODO: Stop leader scheduling tasks
        Ok(())
    }

    fn is_leader(&self) -> bool {
        // TODO: Check OpenRaft leadership status
        false
    }

    async fn register_broker(
        &self,
        _request: &RegisterBrokerToControllerRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        // TODO: Implement broker registration via OpenRaft
        Ok(Some(RemotingCommand::create_response_command()))
    }

    async fn get_next_broker_id(
        &self,
        _request: &GetNextBrokerIdRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        // TODO: Implement broker ID allocation via OpenRaft
        Ok(Some(RemotingCommand::create_response_command()))
    }

    async fn apply_broker_id(&self, request: &ApplyBrokerIdRequestHeader) -> RocketMQResult<Option<RemotingCommand>> {
        // Validate the requested broker ID
        if request.applied_broker_id < 0 {
            tracing::warn!(
                "Invalid broker ID {} requested by broker {} in cluster {}",
                request.applied_broker_id,
                request.broker_name,
                request.cluster_name
            );
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::ControllerBrokerIdInvalid,
                format!(
                    "Invalid broker ID: {}. Broker ID must be non-negative.",
                    request.applied_broker_id
                ),
            )));
        }

        // Check if we have a Raft node
        let Some(node) = &self.node else {
            tracing::error!("OpenRaft node not initialized");
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::ControllerNotLeader,
                "Controller not initialized".to_string(),
            )));
        };

        // Check if this node is the leader
        match node.is_leader().await {
            Ok(true) => {}
            Ok(false) => {
                tracing::info!(
                    "This node is not the leader, cannot apply broker ID for {}",
                    request.broker_name
                );
                return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::ControllerNotLeader,
                    "This controller is not the leader".to_string(),
                )));
            }
            Err(e) => {
                tracing::error!("Failed to check leader status: {}", e);
                return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    format!("Failed to check leader status: {}", e),
                )));
            }
        }

        tracing::info!(
            "Processing ApplyBrokerId request: cluster={}, broker={}, broker_id={}",
            request.cluster_name,
            request.broker_name,
            request.applied_broker_id
        );

        // Create the Raft request
        let raft_request = crate::typ::ControllerRequest::ApplyBrokerId {
            cluster_name: request.cluster_name.to_string(),
            broker_name: request.broker_name.to_string(),
            broker_addr: String::new(),
            applied_broker_id: request.applied_broker_id as u64,
            register_check_code: request.register_check_code.to_string(),
        };

        // Submit to Raft for consensus
        match node.client_write(raft_request).await {
            Ok(response) => {
                // Check the response from state machine
                match response.data {
                    crate::typ::ControllerResponse::ApplyBrokerId {
                        success,
                        error,
                        cluster_name,
                        broker_name,
                    } => {
                        if success {
                            tracing::info!(
                                "Successfully applied broker ID {} to broker {} in cluster {}",
                                request.applied_broker_id,
                                broker_name,
                                cluster_name
                            );

                            let response_header = ApplyBrokerIdResponseHeader {
                                cluster_name: Some(cluster_name.into()),
                                broker_name: Some(broker_name.into()),
                            };

                            Ok(Some(
                                RemotingCommand::create_response_command().set_command_custom_header(response_header),
                            ))
                        } else {
                            let error_msg = error.unwrap_or_else(|| "Unknown error".to_string());
                            tracing::warn!(
                                "Failed to apply broker ID {} to broker {}: {}",
                                request.applied_broker_id,
                                broker_name,
                                error_msg
                            );

                            Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                                ResponseCode::ControllerBrokerIdInvalid,
                                error_msg,
                            )))
                        }
                    }
                    _ => {
                        tracing::error!("Unexpected response type from state machine");
                        Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                            ResponseCode::SystemError,
                            "Unexpected response from state machine".to_string(),
                        )))
                    }
                }
            }
            Err(e) => {
                tracing::error!(
                    "Failed to apply broker ID {} via Raft consensus: {}",
                    request.applied_broker_id,
                    e
                );
                Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    format!("Raft consensus failed: {}", e),
                )))
            }
        }
    }

    async fn clean_broker_data(
        &self,
        _cluster_name: CheetahString,
        _broker_name: CheetahString,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        // TODO: Implement broker data cleanup via OpenRaft
        Ok(Some(RemotingCommand::create_response_command()))
    }

    async fn elect_master(&self, _request: &ElectMasterRequestHeader) -> RocketMQResult<Option<RemotingCommand>> {
        // TODO: Implement master election via OpenRaft
        Ok(Some(RemotingCommand::create_response_command()))
    }

    async fn alter_sync_state_set(
        &self,
        _request: &AlterSyncStateSetRequestHeader,
        _sync_state_set: SyncStateSet,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        // TODO: Implement ISR update via OpenRaft
        Ok(Some(RemotingCommand::create_response_command()))
    }

    async fn get_replica_info(
        &self,
        _request: &GetReplicaInfoRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let result = self.replica_info_manager.get_replica_info(&_request.broker_name);

        let mut response = RemotingCommand::create_response_command();

        if let Some(body) = response.body() {
            response.set_body_mut_ref(body.clone());
        }

        response = response.set_code(result.response_code());

        Ok(Some(response))
    }

    async fn get_controller_metadata(&self) -> RocketMQResult<Option<RemotingCommand>> {
        let controller_metadata_info: ControllerMetadataInfo = {
            let raft_peers: Vec<NodeInfo> = self
                .config
                .raft_peers
                .iter()
                .map(|raft_peer| NodeInfo {
                    node_id: raft_peer.id,
                    addr: raft_peer.addr.to_string(),
                })
                .collect::<Vec<NodeInfo>>();

            let raft_node_manager = self.node.as_ref();
            let controller_leader_id: Option<u64> = if let Some(raft_node_manager) = raft_node_manager {
                raft_node_manager.get_leader().await.ok().flatten()
            } else {
                None
            };

            let controller_leader_address: Option<String> = controller_leader_id
                .and_then(|leader_node_id| raft_peers.iter().find(|raft_peer| raft_peer.node_id == leader_node_id))
                .map(|node_info| node_info.addr.clone());

            let is_leader = controller_leader_id
                .map(|id| self.config.node_id == id)
                .unwrap_or(false);

            ControllerMetadataInfo {
                controller_leader_id,
                controller_leader_address,
                is_leader,
                raft_peers,
            }
        };
        Ok(Some(
            RemotingCommand::create_response_command().set_body(serde_json::to_string(&controller_metadata_info)?),
        ))
    }

    async fn get_sync_state_data(&self, _broker_names: &[CheetahString]) -> RocketMQResult<Option<RemotingCommand>> {
        // TODO: Implement sync state data query
        Ok(Some(RemotingCommand::create_response_command()))
    }

    fn register_broker_lifecycle_listener(&self, _listener: Arc<dyn BrokerLifecycleListener>) {
        // TODO: Register listener
    }
}
