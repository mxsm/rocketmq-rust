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

//! Bounded topology reads. Existing Dashboard list capabilities retain their contracts.

use super::{
    infrastructure_observation::{BrokerHaObservation, BrokerSyncStateObservation, ControllerMetadataObservation},
    AdminFuture,
};

pub const TOPOLOGY_BROKER_TARGETS: usize = 32;
pub const TOPOLOGY_MASTER_TARGETS: usize = 16;
pub const TOPOLOGY_TOPIC_ROUTES: usize = 128;
pub const TOPOLOGY_GROUPS: usize = 50;
/// Exact-Broker Consumer connection/progress pairs across the whole layer, never per Group.
pub const TOPOLOGY_CONSUMER_OBSERVATIONS: usize = 64;
pub const TOPOLOGY_REPLICATION_GROUPS: usize = 16;
pub const TOPOLOGY_CONTROLLERS: usize = 8;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TopologyBrokerKey {
    pub cluster: String,
    pub broker_name: String,
    pub broker_id: u64,
    pub address: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyReadQuality {
    pub total: usize,
    pub total_complete: bool,
    pub inventory_complete: bool,
    pub truncated: bool,
    pub failures: usize,
    pub successful_reads: usize,
}
impl TopologyReadQuality {
    pub fn complete(total: usize) -> Self {
        Self {
            total,
            total_complete: true,
            inventory_complete: true,
            truncated: false,
            failures: 0,
            successful_reads: 0,
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct TopologyRead<T> {
    pub items: Vec<T>,
    pub quality: TopologyReadQuality,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TopologyBrokerRow {
    pub identity: TopologyBrokerKey,
    pub version: Option<String>,
    pub produce_tps: Option<f64>,
    pub consume_tps: Option<f64>,
    pub runtime_available: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyTopicRow {
    pub topic: String,
    pub brokers: Vec<TopologyBrokerKey>,
    pub route_observed: bool,
}
#[derive(Clone, Debug, PartialEq)]
pub struct TopologyGroupRow {
    pub group: String,
    pub brokers: Vec<TopologyBrokerKey>,
    pub connections: Option<usize>,
    pub backlog_messages: Option<i64>,
    pub consume_tps: Option<f64>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopologyMasterElection {
    Configured,
    Controller,
    Unknown,
}
#[derive(Clone, Debug, PartialEq)]
pub struct TopologyReplicationRow {
    pub cluster: String,
    pub broker_name: String,
    pub election: TopologyMasterElection,
    pub controllers: Vec<String>,
    pub members: Vec<TopologyBrokerKey>,
    pub ha: Option<BrokerHaObservation>,
    pub sync_state: Option<BrokerSyncStateObservation>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct TopologyControllerRow {
    pub address: String,
    pub metadata: Option<ControllerMetadataObservation>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct TopologyReplicationRead {
    pub groups: TopologyRead<TopologyReplicationRow>,
    pub controllers: Vec<TopologyControllerRow>,
    pub controller_total: usize,
    pub controller_total_complete: bool,
}

/// Read-only, independently fallible layers with actual RPC fan-out limits.
///
/// Inventory totals precede presentation truncation. `total_complete=false` is a lower bound, not
/// an exact count. Consumer enrichment spends one shared pair budget across all selected Groups.
pub trait TopologyReadAdmin: Send + Sync {
    fn topology_brokers(&self) -> AdminFuture<'_, TopologyRead<TopologyBrokerRow>>;
    fn topology_topics(&self) -> AdminFuture<'_, TopologyRead<TopologyTopicRow>>;
    fn topology_producers(&self) -> AdminFuture<'_, TopologyRead<TopologyGroupRow>>;
    fn topology_consumers(&self) -> AdminFuture<'_, TopologyRead<TopologyGroupRow>>;
    fn topology_replication(&self) -> AdminFuture<'_, TopologyReplicationRead>;
}
