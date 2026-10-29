// Copyright 2026 The RocketMQ Rust Authors
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

//! Layered observations and stable selection shared by native topology presentations.

#[path = "topology/cache.rs"]
mod cache;
#[path = "topology/layout.rs"]
mod layout;
pub use cache::*;
pub use layout::*;

use std::collections::{BTreeMap, BTreeSet};

use crate::{BrokerIdentity, ConnectionScope, EndpointAvailability, EnvironmentId};

pub const TOPOLOGY_GROUP_DISPLAY_LIMIT: usize = 50;
pub const TOPOLOGY_NODE_LIMIT: usize = 320;
pub const TOPOLOGY_EDGE_LIMIT: usize = 640;
pub const TOPOLOGY_CACHE_TTL_MS: u64 = 120_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TopologySource {
    Config,
    Nameservers,
    Brokers,
    Topics,
    Producers,
    Consumers,
    Replication,
}
impl TopologySource {
    pub const ALL: [Self; 7] = [
        Self::Config,
        Self::Nameservers,
        Self::Brokers,
        Self::Topics,
        Self::Producers,
        Self::Consumers,
        Self::Replication,
    ];
    pub const fn label(self) -> &'static str {
        match self {
            Self::Config => "Connection configuration",
            Self::Nameservers => "NameServer health",
            Self::Brokers => "Broker inventory and runtime",
            Self::Topics => "Topic routes",
            Self::Producers => "Producer registrations",
            Self::Consumers => "Consumer groups",
            Self::Replication => "Replication and Controllers",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyContext {
    pub environment: Option<EnvironmentId>,
    pub revision: u64,
    /// The actual connection scope used by the reads, independent of the display filter.
    pub scope: ConnectionScope,
}

/// Composite identities are compared structurally; labels and Broker names are not unique keys.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TopologyNodeId {
    Configuration,
    NameServer(String),
    Cluster(String),
    Broker(BrokerIdentity),
    Topic(String),
    Producer(String),
    Consumer(String),
    Controller(String),
}
impl TopologyNodeId {
    pub const fn kind_label(&self) -> &'static str {
        match self {
            Self::Configuration => "Configuration",
            Self::NameServer(_) => "NameServer",
            Self::Cluster(_) => "Cluster",
            Self::Broker(_) => "Broker",
            Self::Topic(_) => "Topic",
            Self::Producer(_) => "Producer",
            Self::Consumer(_) => "Consumer",
            Self::Controller(_) => "Controller",
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopologyTraffic {
    pub produce_tps: Option<f64>,
    pub consume_tps: Option<f64>,
    pub backlog_messages: Option<i64>,
    pub connections: Option<usize>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TopologyNode {
    pub id: TopologyNodeId,
    pub label: String,
    pub source: TopologySource,
    pub clusters: BTreeSet<String>,
    pub observed_at_ms: u64,
    pub availability: EndpointAvailability,
    pub broker_version: Option<String>,
    pub traffic: TopologyTraffic,
}
impl TopologyNode {
    pub fn new(id: TopologyNodeId, label: String, source: TopologySource, observed_at_ms: u64) -> Self {
        Self {
            id,
            label,
            source,
            clusters: BTreeSet::new(),
            observed_at_ms,
            availability: EndpointAvailability::Unknown,
            broker_version: None,
            traffic: TopologyTraffic::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TopologyEdgeKind {
    ConfiguredEndpoint,
    Registration,
    TopicRoute,
    Produce,
    Consume,
    Replication,
    Controller,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TopologyEdgeId {
    pub from: TopologyNodeId,
    pub to: TopologyNodeId,
    pub kind: TopologyEdgeKind,
}
#[derive(Clone, Debug, PartialEq)]
pub struct TopologyEdge {
    pub id: TopologyEdgeId,
    pub source: TopologySource,
    pub observed_at_ms: u64,
    pub lag_bytes: Option<i64>,
    pub in_sync: Option<bool>,
    pub bytes_per_second: Option<u64>,
}
impl TopologyEdge {
    pub fn new(
        from: TopologyNodeId,
        to: TopologyNodeId,
        kind: TopologyEdgeKind,
        source: TopologySource,
        observed_at_ms: u64,
    ) -> Self {
        Self {
            id: TopologyEdgeId { from, to, kind },
            source,
            observed_at_ms,
            lag_bytes: None,
            in_sync: None,
            bytes_per_second: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TopologyElection {
    Configured,
    Controller,
    #[default]
    Unknown,
}
impl TopologyElection {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Configured => "Configured",
            Self::Controller => "Controller",
            Self::Unknown => "Unknown",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyReplica {
    pub identity: BrokerIdentity,
    pub lag_bytes: Option<i64>,
    pub in_sync: Option<bool>,
    pub bytes_per_second: Option<u64>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologySyncState {
    pub master_broker_id: u64,
    pub master_epoch: i32,
    pub sync_state_set_epoch: i32,
    pub in_sync_broker_ids: Vec<u64>,
    pub out_of_sync_broker_ids: Vec<u64>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyReplicationGroup {
    pub cluster: String,
    pub broker_name: String,
    /// Time of the Broker-group inventory observation, independently of HA and Controller reads.
    pub observed_at_ms: u64,
    pub election: TopologyElection,
    pub controllers: Vec<String>,
    pub replicas: Vec<TopologyReplica>,
    pub sync_state: Option<TopologySyncState>,
    pub master_commit_log_max_offset: Option<u64>,
    pub in_sync_slave_count: Option<u32>,
    pub config_observed_at_ms: Option<u64>,
    pub ha_observed_at_ms: Option<u64>,
    pub sync_observed_at_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyControllerInfo {
    pub address: String,
    pub observed_at_ms: Option<u64>,
    pub group: Option<String>,
    pub leader_id: Option<String>,
    pub is_leader: Option<bool>,
    pub peer_count: Option<usize>,
    pub last_log_index: Option<u64>,
    pub committed_log_index: Option<u64>,
    pub applied_log_index: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopologySourceState {
    Complete,
    Partial,
    Failed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopologyFailureCode {
    NotConfigured,
    Authentication,
    Connection,
    Unsupported,
    Timeout,
    InvalidData,
    Unavailable,
}
impl TopologyFailureCode {
    pub const fn label(self) -> &'static str {
        match self {
            Self::NotConfigured => "not_configured",
            Self::Authentication => "authentication",
            Self::Connection => "connection",
            Self::Unsupported => "unsupported_scope",
            Self::Timeout => "timeout",
            Self::InvalidData => "invalid_data",
            Self::Unavailable => "unavailable",
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct TopologyLayer {
    pub source: TopologySource,
    pub state: TopologySourceState,
    pub code: Option<TopologyFailureCode>,
    pub attempted_at_ms: u64,
    pub observed_at_ms: Option<u64>,
    /// A complete inventory establishes absence; partial metrics alone need not revoke that fact.
    pub inventory_complete: bool,
    pub truncated: bool,
    /// Count before presentation truncation; false `total_complete` means this is a lower bound.
    pub total: usize,
    pub total_edges: usize,
    pub total_complete: bool,
    /// The latest source observation has passed its TTL; individual cached records expire separately.
    pub expired: bool,
    pub nodes: Vec<TopologyNode>,
    pub edges: Vec<TopologyEdge>,
    pub replication: Vec<TopologyReplicationGroup>,
    pub controllers: Vec<TopologyControllerInfo>,
}
impl TopologyLayer {
    pub fn empty(source: TopologySource, now: u64) -> Self {
        Self {
            source,
            state: TopologySourceState::Complete,
            code: None,
            attempted_at_ms: now,
            observed_at_ms: Some(now),
            inventory_complete: true,
            truncated: false,
            total: 0,
            total_edges: 0,
            total_complete: true,
            expired: false,
            nodes: Vec::new(),
            edges: Vec::new(),
            replication: Vec::new(),
            controllers: Vec::new(),
        }
    }
    pub fn failed(source: TopologySource, now: u64, code: TopologyFailureCode) -> Self {
        Self {
            state: TopologySourceState::Failed,
            code: Some(code),
            observed_at_ms: None,
            inventory_complete: false,
            total_complete: false,
            ..Self::empty(source, now)
        }
    }
    pub fn is_stale(&self) -> bool {
        self.state == TopologySourceState::Failed
            || self.expired
            || self.nodes.iter().any(|node| node.observed_at_ms < self.attempted_at_ms)
            || self.edges.iter().any(|edge| edge.observed_at_ms < self.attempted_at_ms)
            || self.controllers.iter().any(|controller| {
                controller
                    .observed_at_ms
                    .is_some_and(|time| time < self.attempted_at_ms)
            })
            || self.replication.iter().any(|group| {
                group.observed_at_ms < self.attempted_at_ms
                    || [
                        group.config_observed_at_ms,
                        group.ha_observed_at_ms,
                        group.sync_observed_at_ms,
                    ]
                    .into_iter()
                    .flatten()
                    .any(|time| time < self.attempted_at_ms)
            })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TopologySnapshot {
    pub context: TopologyContext,
    pub generated_at_ms: u64,
    pub layers: BTreeMap<TopologySource, TopologyLayer>,
}
impl TopologySnapshot {
    pub fn all_remote_failed(&self) -> bool {
        // Configuration and a successful health probe do not establish that any operational
        // topology layer was observed. In particular, reachable NameServers may reject Admin RPCs.
        TopologySource::ALL
            .into_iter()
            .filter(|source| !matches!(source, TopologySource::Config | TopologySource::Nameservers))
            .all(|source| {
                self.layers
                    .get(&source)
                    .is_none_or(|layer| layer.state == TopologySourceState::Failed || layer.expired)
            })
    }
    pub fn nodes(&self) -> Vec<TopologyNode> {
        let mut nodes = BTreeMap::new();
        for layer in self.layers.values() {
            for node in &layer.nodes {
                nodes.insert(node.id.clone(), node.clone());
            }
        }
        nodes.into_values().collect()
    }
    pub fn clusters(&self) -> Vec<String> {
        self.nodes()
            .into_iter()
            .filter_map(|node| match node.id {
                TopologyNodeId::Cluster(name) => Some(name),
                _ => None,
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologySelection {
    pub context: TopologyContext,
    pub node: TopologyNodeId,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TopologyRefresh {
    #[default]
    Manual,
    FiveSeconds,
    TenSeconds,
    ThirtySeconds,
}
impl TopologyRefresh {
    pub const fn seconds(self) -> Option<u64> {
        match self {
            Self::Manual => None,
            Self::FiveSeconds => Some(5),
            Self::TenSeconds => Some(10),
            Self::ThirtySeconds => Some(30),
        }
    }
}

#[cfg(test)]
#[path = "topology/tests.rs"]
mod tests;
