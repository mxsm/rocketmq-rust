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
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::controller::alter_sync_state_set_response_header::AlterSyncStateSetResponseHeader;
use rocketmq_protocol::protocol::header::controller::apply_broker_id_response_header::ApplyBrokerIdResponseHeader;
use rocketmq_protocol::protocol::header::controller::register_broker_to_controller_response_header::RegisterBrokerToControllerResponseHeader;
use rocketmq_protocol::protocol::header::elect_master_response_header::ElectMasterResponseHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;

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
    #[serde(default)]
    pub store_ready: bool,
}

impl BrokerLiveInfoSnapshot {
    pub fn identity(&self) -> BrokerIdentityInfoSnapshot {
        BrokerIdentityInfoSnapshot::new(&self.cluster_name, &self.broker_name, Some(self.broker_id))
    }

    /// Returns whether this heartbeat remains within its liveness timeout.
    pub fn is_active_at(&self, timestamp_millis: u64) -> bool {
        self.last_update_timestamp
            .checked_add(self.heartbeat_timeout_millis)
            .is_some_and(|expires_at| expires_at >= timestamp_millis)
    }

    /// Returns whether the live Broker has also reported a recovered, writable Store.
    pub fn is_promotion_ready_at(&self, timestamp_millis: u64) -> bool {
        self.store_ready && self.is_active_at(timestamp_millis)
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
        #[serde(default)]
        lease_grant_allowed: bool,
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
        self.into_remoting_command_with_factory(&application_remoting_command_factory())
    }

    /// Converts this replicated response with the immutable defaults owned by a
    /// specific Controller instance.
    pub fn into_remoting_command_with_factory(self, command_factory: &RemotingCommandFactory) -> RemotingCommand {
        let mut command = match self.header {
            Some(ControllerResponseHeader::ApplyBrokerId(header)) => {
                command_factory.create_response_command_with_code_and_header(self.response_code, header)
            }
            Some(ControllerResponseHeader::RegisterBroker(header)) => {
                command_factory.create_response_command_with_code_and_header(self.response_code, header)
            }
            Some(ControllerResponseHeader::AlterSyncStateSet(header)) => {
                command_factory.create_response_command_with_code_and_header(self.response_code, header)
            }
            Some(ControllerResponseHeader::ElectMaster(header)) => {
                command_factory.create_response_command_with_code_and_header(self.response_code, header)
            }
            None => command_factory.create_response_command_with_code(self.response_code),
        };
        if let Some(remark) = self.remark {
            command = command.set_remark(remark);
        }
        if let Some(body) = self.body {
            command = command.set_body(Bytes::from(body));
        }
        command
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_protocol::protocol::remoting_command_defaults::{RemotingCommandDefaults, RemotingCommandFactory};
    use rocketmq_protocol::protocol::SerializeType;

    use super::*;

    #[test]
    fn legacy_heartbeat_snapshot_requires_fresh_store_readiness_before_promotion() {
        let encoded = r#"{
            "cluster_name":"cluster-a",
            "broker_name":"broker-a",
            "broker_addr":"127.0.0.1:10911",
            "broker_id":0,
            "last_update_timestamp":1000,
            "heartbeat_timeout_millis":30000,
            "epoch":1,
            "max_offset":64,
            "confirm_offset":48,
            "election_priority":1
        }"#;
        let heartbeat: BrokerLiveInfoSnapshot = serde_json::from_str(encoded).expect("legacy heartbeat snapshot");

        assert!(heartbeat.is_active_at(1_001));
        assert!(!heartbeat.is_promotion_ready_at(1_001));
    }

    #[test]
    fn controller_response_preserves_success_contract() {
        let command = ControllerResponse::success().into_remoting_command();

        assert_eq!(ResponseCode::from(command.code()), ResponseCode::Success);
        assert!(command.is_response_type());
        assert!(command.remark().is_none());
        assert!(command.body().is_none());
    }

    #[test]
    fn controller_response_preserves_code_header_remark_and_body() {
        let command = ControllerResponse::new(
            ResponseCode::ControllerElectMasterFailed.into(),
            Some("election rejected".to_owned()),
            Some(ControllerResponseHeader::ApplyBrokerId(ApplyBrokerIdResponseHeader {
                cluster_name: Some(CheetahString::from_static_str("cluster-a")),
                broker_name: Some(CheetahString::from_static_str("broker-a")),
            })),
            Some(vec![1, 2, 3]),
        )
        .into_remoting_command();

        assert_eq!(
            ResponseCode::from(command.code()),
            ResponseCode::ControllerElectMasterFailed
        );
        assert!(command.is_response_type());
        assert_eq!(command.remark().map(CheetahString::as_str), Some("election rejected"));
        assert_eq!(command.body().map(Bytes::as_ref), Some(&[1, 2, 3][..]));
        let header = command
            .read_custom_header_ref::<ApplyBrokerIdResponseHeader>()
            .expect("controller response header");
        assert_eq!(header.cluster_name.as_deref(), Some("cluster-a"));
        assert_eq!(header.broker_name.as_deref(), Some("broker-a"));
    }

    #[test]
    fn controller_response_uses_the_owning_command_factory() {
        let json_factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(664, SerializeType::JSON));
        let binary_factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(665, SerializeType::ROCKETMQ));

        let json = ControllerResponse::success().into_remoting_command_with_factory(&json_factory);
        let binary = ControllerResponse::success().into_remoting_command_with_factory(&binary_factory);

        assert_eq!(json.version(), 664);
        assert_eq!(json.serialize_type(), SerializeType::JSON);
        assert_eq!(binary.version(), 665);
        assert_eq!(binary.serialize_type(), SerializeType::ROCKETMQ);
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
