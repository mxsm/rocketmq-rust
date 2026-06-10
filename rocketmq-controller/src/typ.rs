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

//! OpenRaft type definitions for RocketMQ Controller.

use std::collections::HashMap;
use std::collections::HashSet;

use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;

use crate::protobuf;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::header::controller::alter_sync_state_set_response_header::AlterSyncStateSetResponseHeader;
use rocketmq_remoting::protocol::header::controller::apply_broker_id_response_header::ApplyBrokerIdResponseHeader;
use rocketmq_remoting::protocol::header::controller::register_broker_to_controller_response_header::RegisterBrokerToControllerResponseHeader;
use rocketmq_remoting::protocol::header::elect_master_response_header::ElectMasterResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

/// Node ID type - represents a unique identifier for a controller node.
pub type NodeId = u64;

/// Node information containing the node ID and RPC address.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Node {
    pub node_id: NodeId,
    pub rpc_addr: String,
}

impl From<protobuf::Node> for Node {
    fn from(node: protobuf::Node) -> Self {
        Self {
            node_id: node.node_id,
            rpc_addr: node.rpc_addr,
        }
    }
}

impl From<Node> for protobuf::Node {
    fn from(node: Node) -> Self {
        Self {
            node_id: node.node_id,
            rpc_addr: node.rpc_addr,
        }
    }
}

/// Serializable broker identity used by replicated heartbeat state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BrokerIdentityInfoSnapshot {
    pub cluster_name: String,
    pub broker_name: String,
    pub broker_id: Option<u64>,
}

impl BrokerIdentityInfoSnapshot {
    pub fn new(cluster_name: impl Into<String>, broker_name: impl Into<String>, broker_id: Option<u64>) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_name: broker_name.into(),
            broker_id,
        }
    }
}

impl std::fmt::Display for BrokerIdentityInfoSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BrokerIdentityInfo{{clusterName='{}', brokerName='{}', brokerId={:?}}}",
            self.cluster_name, self.broker_name, self.broker_id
        )
    }
}

/// Serializable heartbeat state needed for replicated liveness and master election.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrokerLiveInfoSnapshot {
    pub cluster_name: String,
    pub broker_name: String,
    pub broker_addr: String,
    pub broker_id: u64,
    pub last_update_timestamp: u64,
    pub heartbeat_timeout_millis: u64,
    pub epoch: i32,
    pub max_offset: i64,
    pub confirm_offset: i64,
    pub election_priority: Option<i32>,
}

impl BrokerLiveInfoSnapshot {
    pub fn identity(&self) -> BrokerIdentityInfoSnapshot {
        BrokerIdentityInfoSnapshot::new(&self.cluster_name, &self.broker_name, Some(self.broker_id))
    }

    pub fn is_active_at(&self, timestamp_millis: u64) -> bool {
        self.last_update_timestamp
            .checked_add(self.heartbeat_timeout_millis)
            .is_some_and(|expires_at| expires_at >= timestamp_millis)
    }
}

/// Controller write requests that must be replicated through OpenRaft.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControllerRequest {
    ApplyBrokerId {
        cluster_name: String,
        broker_name: String,
        broker_address: String,
        applied_broker_id: u64,
        register_check_code: String,
    },
    RegisterBroker {
        cluster_name: String,
        broker_name: String,
        broker_address: String,
        broker_id: u64,
        alive_broker_ids: HashSet<u64>,
    },
    AlterSyncStateSet {
        cluster_name: String,
        broker_name: String,
        master_broker_id: u64,
        master_epoch: i32,
        new_sync_state_set: HashSet<u64>,
        sync_state_set_epoch: i32,
        alive_broker_ids: HashSet<u64>,
    },
    ElectMaster {
        cluster_name: String,
        broker_name: String,
        broker_id: Option<u64>,
        designate_elect: bool,
        alive_broker_ids: HashSet<u64>,
        live_broker_infos: HashMap<u64, BrokerLiveInfoSnapshot>,
    },
    CleanBrokerData {
        cluster_name: String,
        broker_name: String,
        broker_controller_ids_to_clean: Option<String>,
        clean_living_broker: bool,
        alive_broker_ids: HashSet<u64>,
    },
    BrokerHeartbeat {
        broker_identity: BrokerIdentityInfoSnapshot,
        broker_live_info: BrokerLiveInfoSnapshot,
    },
    BrokerChannelClose {
        broker_identity: BrokerIdentityInfoSnapshot,
    },
    CheckNotActiveBroker {
        check_time_millis: u64,
    },
}

impl std::fmt::Display for ControllerRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApplyBrokerId {
                broker_name,
                applied_broker_id,
                ..
            } => write!(f, "ApplyBrokerId({}, id={})", broker_name, applied_broker_id),
            Self::RegisterBroker {
                broker_name, broker_id, ..
            } => write!(f, "RegisterBroker({}, id={})", broker_name, broker_id),
            Self::AlterSyncStateSet { broker_name, .. } => write!(f, "AlterSyncStateSet({})", broker_name),
            Self::ElectMaster {
                broker_name, broker_id, ..
            } => write!(f, "ElectMaster({}, broker_id={:?})", broker_name, broker_id),
            Self::CleanBrokerData { broker_name, .. } => write!(f, "CleanBrokerData({})", broker_name),
            Self::BrokerHeartbeat { broker_identity, .. } => write!(
                f,
                "BrokerHeartbeat({}, id={:?})",
                broker_identity.broker_name, broker_identity.broker_id
            ),
            Self::BrokerChannelClose { broker_identity } => write!(
                f,
                "BrokerChannelClose({}, id={:?})",
                broker_identity.broker_name, broker_identity.broker_id
            ),
            Self::CheckNotActiveBroker { check_time_millis } => {
                write!(f, "CheckNotActiveBroker({check_time_millis})")
            }
        }
    }
}

/// Serializable response header variants produced by the controller state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControllerResponseHeader {
    ApplyBrokerId(ApplyBrokerIdResponseHeader),
    RegisterBroker(RegisterBrokerToControllerResponseHeader),
    AlterSyncStateSet(AlterSyncStateSetResponseHeader),
    ElectMaster(ElectMasterResponseHeader),
}

/// Serializable response returned by the replicated state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerResponse {
    pub response_code: i32,
    pub remark: Option<String>,
    pub header: Option<ControllerResponseHeader>,
    pub body: Option<Vec<u8>>,
}

impl ControllerResponse {
    pub fn success() -> Self {
        Self {
            response_code: ResponseCode::Success.into(),
            remark: None,
            header: None,
            body: None,
        }
    }

    pub fn new(
        response_code: i32,
        remark: Option<String>,
        header: Option<ControllerResponseHeader>,
        body: Option<Vec<u8>>,
    ) -> Self {
        Self {
            response_code,
            remark,
            header,
            body,
        }
    }

    pub fn into_remoting_command(self) -> RemotingCommand {
        let mut command = RemotingCommand::create_response_command().set_code(self.response_code);
        if let Some(remark) = self.remark {
            command = command.set_remark(remark);
        }
        if let Some(header) = self.header {
            command = match header {
                ControllerResponseHeader::ApplyBrokerId(header) => command.set_command_custom_header(header),
                ControllerResponseHeader::RegisterBroker(header) => command.set_command_custom_header(header),
                ControllerResponseHeader::AlterSyncStateSet(header) => command.set_command_custom_header(header),
                ControllerResponseHeader::ElectMaster(header) => command.set_command_custom_header(header),
            };
        }
        if let Some(body) = self.body {
            command = command.set_body(Bytes::from(body));
        }
        command
    }
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = ControllerRequest,
        R = ControllerResponse,
        Node = Node,
        SnapshotData = std::io::Cursor<Vec<u8>>,
);

pub type Raft = openraft::Raft<TypeConfig, crate::openraft::StateMachine>;
pub type RaftConfig = openraft::Config;
pub type LogId = openraft::type_config::alias::LogIdOf<TypeConfig>;
pub type LogEntry = openraft::type_config::alias::EntryOf<TypeConfig>;
pub type CommittedLogEntry = openraft::type_config::alias::EntryOf<TypeConfig>;
pub type Vote = openraft::type_config::alias::VoteOf<TypeConfig>;
pub type EntryPayload = openraft::type_config::alias::EntryPayloadOf<TypeConfig>;
pub type SnapshotMeta = openraft::type_config::alias::SnapshotMetaOf<TypeConfig>;
pub type Snapshot = openraft::type_config::alias::SnapshotOf<TypeConfig>;
pub type StoredMembership = openraft::type_config::alias::StoredMembershipOf<TypeConfig>;
pub type RaftMetrics = openraft::metrics::RaftMetrics<TypeConfig>;
pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;
pub type AppendEntriesRequest = openraft::raft::AppendEntriesRequest<TypeConfig>;
pub type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<TypeConfig>;
pub type VoteRequest = openraft::raft::VoteRequest<TypeConfig>;
pub type VoteResponse = openraft::raft::VoteResponse<TypeConfig>;
pub type InstallSnapshotRequest = openraft::raft::InstallSnapshotRequest<TypeConfig>;
pub type InstallSnapshotResponse = openraft::raft::InstallSnapshotResponse<TypeConfig>;
