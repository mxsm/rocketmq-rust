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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TopologyBrokerGroup {
    pub broker_name: String,
    pub master_address: Option<String>,
    pub slave_addresses: Vec<String>,
    pub version: String,
    pub produce_tps: f64,
    pub consume_tps: f64,
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TopologyView {
    pub generated_at: i64,
    pub sources: Vec<TopologySourceStatus>,
    pub name_servers: Vec<TopologyNameServer>,
    pub clusters: Vec<TopologyCluster>,
    /// Busiest groups first, at most [`crate::service::TOPOLOGY_GROUP_LIMIT`].
    pub producers: Vec<TopologyProducer>,
    /// Busiest groups first, at most [`crate::service::TOPOLOGY_GROUP_LIMIT`].
    pub consumers: Vec<TopologyConsumer>,
    pub totals: TopologyTotals,
}
