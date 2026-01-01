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

use std::net::SocketAddr;

use serde::Deserialize;
use serde::Serialize;

use crate::metadata::BrokerInfo;
use crate::metadata::BrokerRole;
use crate::metadata::TopicInfo;

/// Request type enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum RequestType {
    RegisterBroker,
    UnregisterBroker,
    BrokerHeartbeat,
    ElectMaster,
    GetMetadata,
    CreateTopic,
    UpdateTopic,
    DeleteTopic,
    UpdateConfig,
    GetConfig,
}

/// Register broker request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterBrokerRequest {
    /// Broker name
    pub broker_name: String,

    /// Broker ID
    pub broker_id: u64,

    /// Cluster name
    pub cluster_name: String,

    /// Broker address
    pub broker_addr: SocketAddr,

    /// Broker version
    pub version: String,

    /// Broker role
    pub role: BrokerRole,

    /// Additional metadata
    pub metadata: serde_json::Value,
}

/// Register broker response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterBrokerResponse {
    /// Success flag
    pub success: bool,

    /// Error message if failed
    pub error: Option<String>,

    /// Assigned broker ID
    pub broker_id: Option<u64>,
}

/// Unregister broker request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnregisterBrokerRequest {
    /// Broker name
    pub broker_name: String,
}

/// Unregister broker response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnregisterBrokerResponse {
    /// Success flag
    pub success: bool,

    /// Error message if failed
    pub error: Option<String>,
}

/// Broker heartbeat request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerHeartbeatRequest {
    /// Broker name
    pub broker_name: String,

    /// Timestamp
    pub timestamp: u64,
}

/// Broker heartbeat response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerHeartbeatResponse {
    /// Success flag
    pub success: bool,

    /// Error message if failed
    pub error: Option<String>,
}

/// Elect master request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElectMasterRequest {
    /// Cluster name
    pub cluster_name: String,

    /// Broker name
    pub broker_name: String,
}

/// Elect master response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElectMasterResponse {
    /// Success flag
    pub success: bool,

    /// Error message if failed
    pub error: Option<String>,

    /// Master broker name
    pub master_broker: Option<String>,

    /// Master broker address
    pub master_addr: Option<SocketAddr>,
}

/// Get metadata request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetMetadataRequest {
    /// Metadata type
    pub metadata_type: MetadataType,

    /// Filter key (optional)
    pub key: Option<String>,
}

/// Metadata type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataType {
    Broker,
    Topic,
    Config,
    All,
}

/// Get metadata response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetMetadataResponse {
    /// Success flag
    pub success: bool,

    /// Error message if failed
    pub error: Option<String>,

    /// Brokers
    pub brokers: Vec<BrokerInfo>,

    /// Topics
    pub topics: Vec<TopicInfo>,

    /// Configs
    pub configs: serde_json::Value,
}

/// Create topic request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTopicRequest {
    /// Topic name
    pub topic_name: String,

    /// Read queue nums
    pub read_queue_nums: u32,

    /// Write queue nums
    pub write_queue_nums: u32,

    /// Permission
    pub perm: u32,

    /// Topic system flag
    pub topic_sys_flag: u32,
}

/// Create topic response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTopicResponse {
    /// Success flag
    pub success: bool,

    /// Error message if failed
    pub error: Option<String>,
}

/// Update topic request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTopicRequest {
    /// Topic name
    pub topic_name: String,

    /// Topic info
    pub topic_info: TopicInfo,
}

/// Update topic response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTopicResponse {
    /// Success flag
    pub success: bool,

    /// Error message if failed
    pub error: Option<String>,
}

/// Delete topic request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteTopicRequest {
    /// Topic name
    pub topic_name: String,
}

/// Delete topic response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteTopicResponse {
    /// Success flag
    pub success: bool,

    /// Error message if failed
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_serialization() {
        let req = RegisterBrokerRequest {
            broker_name: "broker-a".to_string(),
            broker_id: 0,
            cluster_name: "DefaultCluster".to_string(),
            broker_addr: "127.0.0.1:10911".parse().unwrap(),
            version: "5.0.0".to_string(),
            role: BrokerRole::Master,
            metadata: serde_json::json!({}),
        };

        let json = serde_json::to_string(&req).unwrap();
        let deserialized: RegisterBrokerRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(req.broker_name, deserialized.broker_name);
    }
}
