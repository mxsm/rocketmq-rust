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

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::controller::ControllerConfig;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::sync_state_set_body::SyncStateSet;
use rocketmq_remoting::protocol::header::controller::alter_sync_state_set_request_header::AlterSyncStateSetRequestHeader;
use rocketmq_remoting::protocol::header::controller::apply_broker_id_request_header::ApplyBrokerIdRequestHeader;
use rocketmq_remoting::protocol::header::controller::elect_master_request_header::ElectMasterRequestHeader;
use rocketmq_remoting::protocol::header::controller::get_next_broker_id_request_header::GetNextBrokerIdRequestHeader;
use rocketmq_remoting::protocol::header::controller::get_replica_info_request_header::GetReplicaInfoRequestHeader;
use rocketmq_remoting::protocol::header::controller::register_broker_to_controller_request_header::RegisterBrokerToControllerRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use tonic::transport::Server;

use crate::controller::Controller;
use crate::helper::broker_lifecycle_listener::BrokerLifecycleListener;
use crate::openraft::GrpcRaftService;
use crate::openraft::RaftNodeManager;
use crate::protobuf::openraft::open_raft_service_server::OpenRaftServiceServer;

/// OpenRaft-based controller implementation
pub struct OpenRaftController {
    config: Arc<ControllerConfig>,
}

impl OpenRaftController {
    pub fn new(config: Arc<ControllerConfig>) -> Self {
        Self { config }
    }
}

impl Controller for OpenRaftController {
    async fn startup(&self) -> RocketMQResult<()> {
        let node = Arc::new(RaftNodeManager::new(Arc::clone(&self.config)).await?);
        let service = GrpcRaftService::new(node.raft());
        let server = Server::builder()
            .add_service(OpenRaftServiceServer::new(service))
            .serve(self.config.listen_addr);

        let node_id_for_error = self.config.node_id;
        tokio::spawn(async move {
            if let Err(e) = server.await {
                eprintln!("gRPC server error for node {}: {}", node_id_for_error, e);
            }
        });
        Ok(())
    }

    async fn shutdown(&self) -> RocketMQResult<()> {
        // TODO: Shutdown OpenRaft node
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

    async fn apply_broker_id(&self, _request: &ApplyBrokerIdRequestHeader) -> RocketMQResult<Option<RemotingCommand>> {
        // TODO: Implement broker ID application via OpenRaft
        Ok(Some(RemotingCommand::create_response_command()))
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
        // TODO: Implement replica info query
        Ok(Some(RemotingCommand::create_response_command()))
    }

    fn get_controller_metadata(&self) -> RocketMQResult<Option<RemotingCommand>> {
        // TODO: Implement metadata query
        Ok(Some(RemotingCommand::create_response_command()))
    }

    async fn get_sync_state_data(&self, _broker_names: &[CheetahString]) -> RocketMQResult<Option<RemotingCommand>> {
        // TODO: Implement sync state data query
        Ok(Some(RemotingCommand::create_response_command()))
    }

    fn register_broker_lifecycle_listener(&self, _listener: Arc<dyn BrokerLifecycleListener>) {
        // TODO: Register listener
    }
}
