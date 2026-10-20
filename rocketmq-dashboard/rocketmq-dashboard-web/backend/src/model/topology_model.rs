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
use serde::Deserialize;
use serde::Serialize;

/// A backend query the topology is assembled from.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TopologySource {
    Config,
    Nameservers,
    Brokers,
    Topics,
    Producers,
    Consumers,
    /// Master election mode, HA state of each master and the Controllers.
    Replication,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TopologySourceState {
    Ok,
    Failed,
}

/// Outcome of one layer query. A failed layer is empty in the view and carries the public error code.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopologySourceStatus {
    pub source: TopologySource,
    pub status: TopologySourceState,
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TopologyNameServerStatus {
    Available,
    Unavailable,
    /// The availability check could not be read.
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopologyNameServer {
    pub address: String,
    pub current: bool,
    pub status: TopologyNameServerStatus,
    pub checked_at: Option<i64>,
}

/// One broker name: the master (broker id 0) and the slaves registered under the same name.
/// What decides which broker of a group is the master.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TopologyMasterElection {
    /// The configuration fixes the master: broker id 0.
    Configured,
    /// A Controller group elects the master (`enableControllerMode`).
    Controller,
    /// The master's broker config could not be read.
    #[default]
    Unknown,
}

/// A slave of a broker group, with the replication state its master reports.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopologyReplica {
    pub broker_id: u64,
    pub address: String,
    /// Commit log bytes the replica has not acknowledged; absent while the master reports no
    /// connection from it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lag_bytes: Option<i64>,
    /// Whether the master counts the replica as in sync.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_sync: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_bytes_per_second: Option<u64>,
}

/// The state the Controllers keep for a broker group in Controller mode. Broker ids are the
/// NameServer's, so the elected master is broker id 0.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopologyControllerGroupState {
    pub master_epoch: i32,
    pub sync_state_set_epoch: i32,
    /// The sync state set, the master included.
    pub in_sync_broker_ids: Vec<u64>,
    /// Replicas the Controllers know but count out of sync.
    pub out_of_sync_broker_ids: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TopologyBrokerGroup {
    pub broker_name: String,
    pub master_address: Option<String>,
    pub slave_addresses: Vec<String>,
    pub version: String,
    pub produce_tps: f64,
    pub consume_tps: f64,
    #[serde(default)]
    pub election: TopologyMasterElection,
    /// Controllers the master names in `controllerAddr`, in Controller mode only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub controller_addresses: Vec<String>,
    /// The slaves, ordered by broker id, with their replication state when it could be read.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replicas: Vec<TopologyReplica>,
    /// Slaves the master counts in sync.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_sync_slave_count: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller_state: Option<TopologyControllerGroupState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TopologyCluster {
    pub cluster_name: String,
    pub produce_tps: f64,
    pub consume_tps: f64,
    pub brokers: Vec<TopologyBrokerGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopologyProducer {
    pub producer_group: String,
    pub connection_count: usize,
    pub topics: Vec<String>,
    /// Brokers serving at least one of the group's topics.
    pub broker_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopologyConsumer {
    /// Raw group name, the identity used by the consumer routes.
    pub group: String,
    pub display_name: String,
    pub connection_count: usize,
    pub consume_tps: i64,
    pub diff_total: i64,
    pub broker_names: Vec<String>,
}

/// Counts over every group, including the ones left out of the ranked lists.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopologyTotals {
    pub producer_groups: usize,
    pub consumer_groups: usize,
    pub message_backlog: i64,
}

/// Every layer of the topology map from one request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TopologyControllerStatus {
    Available,
    /// Its metadata could not be read.
    Unavailable,
}

/// A Controller that Controller-mode brokers name, with the Raft state it reports.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopologyController {
    pub address: String,
    pub status: TopologyControllerStatus,
    /// Whether it leads its Raft group; absent while no leader is known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader: Option<bool>,
    /// Raft node id of the group's leader.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_log_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_log_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_log_index: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TopologyView {
    pub generated_at: i64,
    pub sources: Vec<TopologySourceStatus>,
    pub name_servers: Vec<TopologyNameServer>,
    pub clusters: Vec<TopologyCluster>,
    /// Controllers named by Controller-mode brokers, ordered by address.
    #[serde(default)]
    pub controllers: Vec<TopologyController>,
    /// Busiest groups first, at most [`crate::service::TOPOLOGY_GROUP_LIMIT`].
    pub producers: Vec<TopologyProducer>,
    /// Busiest groups first, at most [`crate::service::TOPOLOGY_GROUP_LIMIT`].
    pub consumers: Vec<TopologyConsumer>,
    pub totals: TopologyTotals,
}
