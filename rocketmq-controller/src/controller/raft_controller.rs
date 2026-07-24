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

//! OpenRaft-backed controller wrapper.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::sync_state_set_body::SyncStateSet;
use rocketmq_remoting::protocol::header::controller::alter_sync_state_set_request_header::AlterSyncStateSetRequestHeader;
use rocketmq_remoting::protocol::header::controller::apply_broker_id_request_header::ApplyBrokerIdRequestHeader;
use rocketmq_remoting::protocol::header::controller::clean_broker_data_request_header::CleanBrokerDataRequestHeader;
use rocketmq_remoting::protocol::header::controller::elect_master_request_header::ElectMasterRequestHeader;
use rocketmq_remoting::protocol::header::controller::get_next_broker_id_request_header::GetNextBrokerIdRequestHeader;
use rocketmq_remoting::protocol::header::controller::get_replica_info_request_header::GetReplicaInfoRequestHeader;
use rocketmq_remoting::protocol::header::controller::register_broker_to_controller_request_header::RegisterBrokerToControllerRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::broker_request::BrokerHeartbeatRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

use crate::config::ControllerConfigReader;
use crate::controller::open_raft_controller::OpenRaftController;
use crate::controller::Controller;
use crate::error::Result;
use crate::heartbeat::default_broker_heartbeat_manager::DefaultBrokerHeartbeatManager;
use crate::helper::broker_lifecycle_listener::BrokerLifecycleListener;
use crate::typ::Node;
use crate::typ::NodeId;

/// Controller wrapper used by the rest of the controller stack.
///
/// The controller mode is backed exclusively by OpenRaft.
#[derive(Clone)]
pub struct RaftController {
    inner: Arc<OpenRaftController>,
}

impl RaftController {
    /// Create a new OpenRaft-based controller
    pub fn new_open_raft(config: ControllerConfigReader) -> Self {
        Self {
            inner: Arc::new(OpenRaftController::new(config)),
        }
    }

    /// Create a new OpenRaft-based controller that shares a heartbeat manager.
    pub fn new_open_raft_with_heartbeat(
        config: ControllerConfigReader,
        heartbeat_manager: Arc<DefaultBrokerHeartbeatManager>,
    ) -> Self {
        Self {
            inner: Arc::new(OpenRaftController::new_with_heartbeat(config, heartbeat_manager)),
        }
    }

    /// Create an OpenRaft controller whose networking, storage, and blocking work are owned by
    /// the supplied controller task group.
    pub fn new_open_raft_with_heartbeat_and_task_group(
        config: ControllerConfigReader,
        heartbeat_manager: Arc<DefaultBrokerHeartbeatManager>,
        parent_task_group: rocketmq_runtime::TaskGroup,
    ) -> Self {
        Self {
            inner: Arc::new(OpenRaftController::new_with_heartbeat_and_task_group(
                config,
                heartbeat_manager,
                parent_task_group,
            )),
        }
    }

    pub(crate) async fn startup_shared(&self) -> RocketMQResult<()> {
        self.inner.startup_shared().await
    }

    pub(crate) async fn shutdown_shared(&self) -> RocketMQResult<()> {
        self.inner.shutdown_shared().await
    }

    pub async fn initialize_cluster(&self, nodes: BTreeMap<NodeId, Node>) -> Result<()> {
        self.inner.initialize_cluster(nodes).await
    }

    pub async fn add_learner(&self, node_id: NodeId, node: Node, blocking: bool) -> Result<()> {
        self.inner.add_learner(node_id, node, blocking).await
    }

    pub async fn change_membership(&self, members: BTreeSet<NodeId>, retain: bool) -> Result<()> {
        self.inner.change_membership(members, retain).await
    }

    pub async fn allow_next_revert(&self, node_id: NodeId, allow: bool) -> Result<()> {
        self.inner.allow_next_revert(node_id, allow).await
    }

    pub fn set_runtime_tick_enabled(&self, enabled: bool) -> Result<()> {
        self.inner.set_runtime_tick_enabled(enabled)
    }

    pub fn set_runtime_heartbeat_enabled(&self, enabled: bool) -> Result<()> {
        self.inner.set_runtime_heartbeat_enabled(enabled)
    }

    pub fn set_runtime_elect_enabled(&self, enabled: bool) -> Result<()> {
        self.inner.set_runtime_elect_enabled(enabled)
    }

    pub fn has_committed_log(&self) -> Result<bool> {
        self.inner.has_committed_log()
    }

    /// Returns whether this node has observed a Raft leader and applied committed state.
    ///
    /// This is the minimum recovery evidence required before publishing process readiness.
    pub fn has_recovered_cluster_state(&self) -> bool {
        self.inner.has_recovered_cluster_state()
    }

    pub fn scheduling_enabled(&self) -> bool {
        self.inner.scheduling_enabled()
    }

    pub async fn record_broker_heartbeat(
        &self,
        request: &BrokerHeartbeatRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        self.inner.record_broker_heartbeat(request).await
    }

    pub async fn remove_broker_live_info(
        &self,
        cluster_name: Option<&str>,
        broker_name: &str,
        broker_id: Option<i64>,
    ) -> RocketMQResult<()> {
        self.inner
            .remove_broker_live_info(cluster_name, broker_name, broker_id)
            .await
    }
}

impl Controller for RaftController {
    async fn startup(&mut self) -> RocketMQResult<()> {
        self.startup_shared().await
    }

    async fn shutdown(&mut self) -> RocketMQResult<()> {
        self.shutdown_shared().await
    }

    async fn start_scheduling(&self) -> RocketMQResult<()> {
        self.inner.start_scheduling().await
    }

    async fn stop_scheduling(&self) -> RocketMQResult<()> {
        self.inner.stop_scheduling().await
    }

    fn is_leader(&self) -> bool {
        self.inner.is_leader()
    }

    async fn register_broker(
        &self,
        request: &RegisterBrokerToControllerRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        self.inner.register_broker(request).await
    }

    async fn get_next_broker_id(
        &self,
        request: &GetNextBrokerIdRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        self.inner.get_next_broker_id(request).await
    }

    async fn apply_broker_id(&self, request: &ApplyBrokerIdRequestHeader) -> RocketMQResult<Option<RemotingCommand>> {
        self.inner.apply_broker_id(request).await
    }

    async fn clean_broker_data(
        &self,
        request: &CleanBrokerDataRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        self.inner.clean_broker_data(request).await
    }

    async fn elect_master(&self, request: &ElectMasterRequestHeader) -> RocketMQResult<Option<RemotingCommand>> {
        self.inner.elect_master(request).await
    }

    async fn alter_sync_state_set(
        &self,
        request: &AlterSyncStateSetRequestHeader,
        sync_state_set: SyncStateSet,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        self.inner.alter_sync_state_set(request, sync_state_set).await
    }

    async fn get_replica_info(&self, request: &GetReplicaInfoRequestHeader) -> RocketMQResult<Option<RemotingCommand>> {
        self.inner.get_replica_info(request).await
    }

    async fn get_controller_metadata(&self) -> RocketMQResult<Option<RemotingCommand>> {
        self.inner.get_controller_metadata().await
    }

    async fn get_sync_state_data(&self, broker_names: &[CheetahString]) -> RocketMQResult<Option<RemotingCommand>> {
        self.inner.get_sync_state_data(broker_names).await
    }

    fn register_broker_lifecycle_listener(&self, listener: Arc<dyn BrokerLifecycleListener>) {
        self.inner.register_broker_lifecycle_listener(listener);
    }
}
