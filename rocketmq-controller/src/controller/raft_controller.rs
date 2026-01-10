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

//! RaftController wrapper for different Raft implementations

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
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::ArcMut;

use crate::controller::open_raft_controller::OpenRaftController;
use crate::controller::raft_rs_controller::RaftRsController;
use crate::controller::Controller;
use crate::helper::broker_lifecycle_listener::BrokerLifecycleListener;

/// Unified controller wrapper supporting multiple Raft implementations
///
/// This enum allows runtime selection between OpenRaft and raft-rs implementations.
#[derive(Clone)]
pub enum RaftController {
    /// OpenRaft-based implementation
    OpenRaft(ArcMut<OpenRaftController>),
    /// raft-rs (TiKV) based implementation
    RaftRs(ArcMut<RaftRsController>),
}

impl RaftController {
    /// Create a new OpenRaft-based controller
    pub fn new_open_raft(config: Arc<ControllerConfig>) -> Self {
        Self::OpenRaft(ArcMut::new(OpenRaftController::new(config)))
    }

    /// Create a new raft-rs based controller
    pub fn new_raft_rs(runtime: Arc<RocketMQRuntime>) -> Self {
        Self::RaftRs(ArcMut::new(RaftRsController::new(runtime)))
    }
}

impl Controller for RaftController {
    async fn startup(&mut self) -> RocketMQResult<()> {
        match self {
            Self::OpenRaft(controller) => controller.startup().await,
            Self::RaftRs(controller) => controller.startup().await,
        }
    }

    async fn shutdown(&mut self) -> RocketMQResult<()> {
        match self {
            Self::OpenRaft(controller) => controller.shutdown().await,
            Self::RaftRs(controller) => controller.shutdown().await,
        }
    }

    async fn start_scheduling(&self) -> RocketMQResult<()> {
        match self {
            Self::OpenRaft(controller) => controller.start_scheduling().await,
            Self::RaftRs(controller) => controller.start_scheduling().await,
        }
    }

    async fn stop_scheduling(&self) -> RocketMQResult<()> {
        match self {
            Self::OpenRaft(controller) => controller.stop_scheduling().await,
            Self::RaftRs(controller) => controller.stop_scheduling().await,
        }
    }

    fn is_leader(&self) -> bool {
        match self {
            Self::OpenRaft(controller) => controller.is_leader(),
            Self::RaftRs(controller) => controller.is_leader(),
        }
    }

    async fn register_broker(
        &self,
        request: &RegisterBrokerToControllerRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        match self {
            Self::OpenRaft(controller) => controller.register_broker(request).await,
            Self::RaftRs(controller) => controller.register_broker(request).await,
        }
    }

    async fn get_next_broker_id(
        &self,
        request: &GetNextBrokerIdRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        match self {
            Self::OpenRaft(controller) => controller.get_next_broker_id(request).await,
            Self::RaftRs(controller) => controller.get_next_broker_id(request).await,
        }
    }

    async fn apply_broker_id(&self, request: &ApplyBrokerIdRequestHeader) -> RocketMQResult<Option<RemotingCommand>> {
        match self {
            Self::OpenRaft(controller) => controller.apply_broker_id(request).await,
            Self::RaftRs(controller) => controller.apply_broker_id(request).await,
        }
    }

    async fn clean_broker_data(
        &self,
        cluster_name: CheetahString,
        broker_name: CheetahString,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        match self {
            Self::OpenRaft(controller) => controller.clean_broker_data(cluster_name, broker_name).await,
            Self::RaftRs(controller) => controller.clean_broker_data(cluster_name, broker_name).await,
        }
    }

    async fn elect_master(&self, request: &ElectMasterRequestHeader) -> RocketMQResult<Option<RemotingCommand>> {
        match self {
            Self::OpenRaft(controller) => controller.elect_master(request).await,
            Self::RaftRs(controller) => controller.elect_master(request).await,
        }
    }

    async fn alter_sync_state_set(
        &self,
        request: &AlterSyncStateSetRequestHeader,
        sync_state_set: SyncStateSet,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        match self {
            Self::OpenRaft(controller) => controller.alter_sync_state_set(request, sync_state_set).await,
            Self::RaftRs(controller) => controller.alter_sync_state_set(request, sync_state_set).await,
        }
    }

    async fn get_replica_info(&self, request: &GetReplicaInfoRequestHeader) -> RocketMQResult<Option<RemotingCommand>> {
        match self {
            Self::OpenRaft(controller) => controller.get_replica_info(request).await,
            Self::RaftRs(controller) => controller.get_replica_info(request).await,
        }
    }

    async fn get_controller_metadata(&self) -> RocketMQResult<Option<RemotingCommand>> {
        match self {
            Self::OpenRaft(controller) => controller.get_controller_metadata().await,
            Self::RaftRs(controller) => controller.get_controller_metadata().await,
        }
    }

    async fn get_sync_state_data(&self, broker_names: &[CheetahString]) -> RocketMQResult<Option<RemotingCommand>> {
        match self {
            Self::OpenRaft(controller) => controller.get_sync_state_data(broker_names).await,
            Self::RaftRs(controller) => controller.get_sync_state_data(broker_names).await,
        }
    }

    fn register_broker_lifecycle_listener(&self, listener: Arc<dyn BrokerLifecycleListener>) {
        match self {
            Self::OpenRaft(controller) => controller.register_broker_lifecycle_listener(listener),
            Self::RaftRs(controller) => controller.register_broker_lifecycle_listener(listener),
        }
    }
}
