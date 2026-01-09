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

//! OpenRaft type definitions for RocketMQ Controller
//!
//! This module defines the core types used by OpenRaft for the controller implementation.

use serde::Deserialize;
use serde::Serialize;

use crate::protobuf;

/// Node ID type - represents a unique identifier for a controller node
pub type NodeId = u64;

/// Node information containing the node ID and RPC address
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

/// Controller request types - operations that need to be replicated via Raft
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControllerRequest {
    /// Register a broker with the controller
    RegisterBroker {
        broker_name: String,
        broker_addr: String,
        broker_id: u64,
        cluster_name: String,
        epoch: i32,
        max_offset: i64,
        election_priority: i64,
    },
    /// Update broker heartbeat
    BrokerHeartbeat {
        broker_name: String,
        broker_addr: String,
        broker_id: u64,
        epoch: i32,
        max_offset: i64,
        confirm_offset: i64,
        heartbeat_timeout_millis: i64,
    },
    /// Update controller configuration
    UpdateConfig { key: String, value: String },
    /// Create or update topic metadata
    UpdateTopic {
        topic_name: String,
        read_queue_nums: i32,
        write_queue_nums: i32,
        perm: i32,
    },
    /// Apply for a specific broker ID
    ApplyBrokerId {
        cluster_name: String,
        broker_name: String,
        broker_addr: String,
        applied_broker_id: u64,
        register_check_code: String,
    },
}

impl std::fmt::Display for ControllerRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RegisterBroker { broker_name, .. } => {
                write!(f, "RegisterBroker({})", broker_name)
            }
            Self::BrokerHeartbeat { broker_name, .. } => {
                write!(f, "BrokerHeartbeat({})", broker_name)
            }
            Self::UpdateConfig { key, .. } => write!(f, "UpdateConfig({})", key),
            Self::UpdateTopic { topic_name, .. } => write!(f, "UpdateTopic({})", topic_name),
            Self::ApplyBrokerId {
                broker_name,
                applied_broker_id,
                ..
            } => write!(f, "ApplyBrokerId({}, id={})", broker_name, applied_broker_id),
        }
    }
}

/// Controller response types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControllerResponse {
    /// Response for broker registration
    RegisterBroker {
        success: bool,
        error: Option<String>,
        master_addr: Option<String>,
        master_epoch: Option<i32>,
        sync_state_set_epoch: Option<i32>,
    },
    /// Response for broker heartbeat
    BrokerHeartbeat {
        success: bool,
        error: Option<String>,
        is_master: bool,
        master_addr: Option<String>,
        master_epoch: Option<i32>,
        sync_state_set_epoch: Option<i32>,
    },
    /// Response for broker ID application
    ApplyBrokerId {
        success: bool,
        error: Option<String>,
        cluster_name: String,
        broker_name: String,
    },
    /// Generic success response
    Success,
    /// Generic error response
    Error(String),
}

// Declare OpenRaft types for RocketMQ Controller
openraft::declare_raft_types!(
    pub TypeConfig:
        D = ControllerRequest,
        R = ControllerResponse,
        Node = Node,
        SnapshotData = std::io::Cursor<Vec<u8>>,
);

/// Type alias for the Raft instance
pub type Raft = openraft::Raft<TypeConfig>;

/// Type alias for Raft configuration
pub type RaftConfig = openraft::Config;

/// Type alias for log ID (using TypeConfig)
pub type LogId = openraft::LogId<TypeConfig>;

/// Type alias for log entry
pub type LogEntry = openraft::Entry<TypeConfig>;

/// Type alias for committed log entry
pub type CommittedLogEntry = openraft::entry::Entry<TypeConfig>;

/// Type alias for vote (using TypeConfig)
pub type Vote = openraft::Vote<TypeConfig>;

/// Type alias for Raft metrics
pub type RaftMetrics = openraft::metrics::RaftMetrics<TypeConfig>;

/// Type alias for client write response
pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;

/// Type alias for append entries request
pub type AppendEntriesRequest = openraft::raft::AppendEntriesRequest<TypeConfig>;

/// Type alias for append entries response
pub type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<TypeConfig>;

/// Type alias for vote request
pub type VoteRequest = openraft::raft::VoteRequest<TypeConfig>;

/// Type alias for vote response
pub type VoteResponse = openraft::raft::VoteResponse<TypeConfig>;

/// Type alias for install snapshot request
pub type InstallSnapshotRequest = openraft::raft::InstallSnapshotRequest<TypeConfig>;

/// Type alias for install snapshot response
pub type InstallSnapshotResponse = openraft::raft::InstallSnapshotResponse<TypeConfig>;
