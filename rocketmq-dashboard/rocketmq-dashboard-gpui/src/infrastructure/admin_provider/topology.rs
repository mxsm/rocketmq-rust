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

use super::{GpuiAdminProvider, ProviderFailure, query_for_revision, select_admin};
use crate::infrastructure::admin_session::TopologyAdminData;
use rocketmq_admin_core::core::topology as core;
use rocketmq_dashboard_common::*;
use std::sync::Arc;

impl GpuiAdminProvider {
    pub async fn topology_nameservers(
        self: &Arc<Self>,
        revision: u64,
        addresses: Vec<String>,
        now: u64,
    ) -> Result<TopologyLayer, ProviderFailure> {
        let snapshot = self.snapshot_for_revision(revision)?;
        if snapshot.scope != ConnectionScope::NameServer {
            return Err(ProviderFailure::invalid_request(
                "Topology reads require NameServer Direct scope.",
            ));
        }
        let total = addresses.len();
        let snapshots = addresses
            .into_iter()
            .take(16)
            .map(|address| ConnectionSnapshot {
                nameserver: Some(address),
                ..snapshot.clone()
            })
            .collect();
        let readings = self.check_endpoints(snapshots).await?;
        self.snapshot_for_revision(revision)?;
        let mut layer = TopologyLayer::empty(TopologySource::Nameservers, now);
        layer.total = total;
        layer.truncated = total > 16;
        layer.inventory_complete = !layer.truncated;
        for reading in readings {
            let mut node = TopologyNode::new(
                TopologyNodeId::NameServer(reading.endpoint.clone()),
                reading.endpoint,
                layer.source,
                reading.checked_at_epoch_ms.unwrap_or(now),
            );
            if reading.availability != EndpointAvailability::Available {
                layer.state = TopologySourceState::Partial;
                layer.code = Some(TopologyFailureCode::Connection);
            }
            node.availability = reading.availability;
            layer.nodes.push(node);
        }
        Ok(layer)
    }
    pub async fn topology_layer(
        self: &Arc<Self>,
        revision: u64,
        source: TopologySource,
        now: u64,
    ) -> Result<TopologyLayer, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-topology-layer", move |cancellation| async move {
            let snapshot = this.snapshot_for_revision(revision)?;
            if snapshot.scope != ConnectionScope::NameServer {
                return Err(ProviderFailure::invalid_request(
                    "Topology reads require NameServer Direct scope.",
                ));
            }
            let guard = this.query_session.read().await;
            let result = select_admin(
                cancellation,
                query_for_revision(&guard, revision)?.topology_layer(source),
            )
            .await?;
            let mut layer = project(source, result, now)?;
            if source == TopologySource::Brokers
                && let Some(address) = snapshot.nameserver
            {
                for node in &layer.nodes {
                    if matches!(node.id, TopologyNodeId::Cluster(_)) {
                        layer.edges.push(TopologyEdge::new(
                            TopologyNodeId::NameServer(address.clone()),
                            node.id.clone(),
                            TopologyEdgeKind::Registration,
                            source,
                            now,
                        ));
                    }
                }
            }
            layer.total_edges = layer.edges.len();
            Ok(layer)
        })
        .await
    }
}

fn key(value: &core::TopologyBrokerKey) -> BrokerIdentity {
    BrokerIdentity {
        cluster: value.cluster.clone(),
        broker_name: value.broker_name.clone(),
        broker_id: value.broker_id,
        address: value.address.clone(),
    }
}
fn quality(layer: &mut TopologyLayer, value: core::TopologyReadQuality) {
    layer.total = value.total;
    layer.total_complete = value.total_complete;
    layer.inventory_complete = value.inventory_complete;
    layer.truncated = value.truncated;
    if value.failures > 0 || value.truncated {
        layer.state = if value.successful_reads == 0 && value.failures > 0 {
            TopologySourceState::Failed
        } else {
            TopologySourceState::Partial
        };
        layer.code = (value.failures > 0).then_some(TopologyFailureCode::Unavailable);
    }
}
fn project(source: TopologySource, data: TopologyAdminData, now: u64) -> Result<TopologyLayer, ProviderFailure> {
    let mut layer = TopologyLayer::empty(source, now);
    match (source, data) {
        (TopologySource::Brokers, TopologyAdminData::Brokers(read)) => {
            quality(&mut layer, read.quality);
            let mut clusters = std::collections::BTreeSet::new();
            for row in read.items {
                let identity = key(&row.identity);
                clusters.insert(identity.cluster.clone());
                let mut node = TopologyNode::new(
                    TopologyNodeId::Broker(identity.clone()),
                    format!("{} · id {}", identity.broker_name, identity.broker_id),
                    source,
                    now,
                );
                node.clusters.insert(identity.cluster.clone());
                node.availability = if row.runtime_available {
                    EndpointAvailability::Available
                } else {
                    EndpointAvailability::Unknown
                };
                node.broker_version = row.version;
                node.traffic.produce_tps = row.produce_tps;
                node.traffic.consume_tps = row.consume_tps;
                layer.edges.push(TopologyEdge::new(
                    TopologyNodeId::Cluster(identity.cluster),
                    node.id.clone(),
                    TopologyEdgeKind::Registration,
                    source,
                    now,
                ));
                layer.nodes.push(node);
            }
            for cluster in clusters {
                let mut node =
                    TopologyNode::new(TopologyNodeId::Cluster(cluster.clone()), cluster.clone(), source, now);
                node.clusters.insert(cluster);
                layer.nodes.push(node);
            }
        }
        (TopologySource::Topics, TopologyAdminData::Topics(read)) => {
            quality(&mut layer, read.quality);
            for row in read.items {
                // An unanswered route has no current relationship observation. Its last successful
                // node and edges remain in the same-context cache with their original timestamps.
                if !row.route_observed {
                    continue;
                }
                let mut node = TopologyNode::new(TopologyNodeId::Topic(row.topic.clone()), row.topic, source, now);
                for broker in row.brokers {
                    node.clusters.insert(broker.cluster.clone());
                    layer.edges.push(TopologyEdge::new(
                        TopologyNodeId::Broker(key(&broker)),
                        node.id.clone(),
                        TopologyEdgeKind::TopicRoute,
                        source,
                        now,
                    ));
                }
                layer.nodes.push(node);
            }
        }
        (TopologySource::Producers, TopologyAdminData::Producers(read))
        | (TopologySource::Consumers, TopologyAdminData::Consumers(read)) => {
            quality(&mut layer, read.quality);
            for row in read.items {
                let id = if source == TopologySource::Producers {
                    TopologyNodeId::Producer(row.group.clone())
                } else {
                    TopologyNodeId::Consumer(row.group.clone())
                };
                let mut node = TopologyNode::new(id, row.group, source, now);
                node.traffic.connections = row.connections;
                node.traffic.backlog_messages = row.backlog_messages;
                node.traffic.consume_tps = row.consume_tps;
                for broker in row.brokers {
                    node.clusters.insert(broker.cluster.clone());
                    let broker = TopologyNodeId::Broker(key(&broker));
                    let (from, to, kind) = if source == TopologySource::Producers {
                        (node.id.clone(), broker, TopologyEdgeKind::Produce)
                    } else {
                        (broker, node.id.clone(), TopologyEdgeKind::Consume)
                    };
                    layer.edges.push(TopologyEdge::new(from, to, kind, source, now));
                }
                layer.nodes.push(node);
            }
        }
        (TopologySource::Replication, TopologyAdminData::Replication(read)) => {
            quality(&mut layer, read.groups.quality);
            layer.total = layer.total.saturating_add(read.controller_total);
            layer.total_complete &= read.controller_total_complete;
            for group in read.groups.items {
                let election = match group.election {
                    core::TopologyMasterElection::Configured => TopologyElection::Configured,
                    core::TopologyMasterElection::Controller => TopologyElection::Controller,
                    core::TopologyMasterElection::Unknown => TopologyElection::Unknown,
                };
                let mut replicas = Vec::new();
                let master_id = group
                    .ha
                    .as_ref()
                    .map(|ha| ha.broker_id)
                    .or_else(|| group.sync_state.as_ref().map(|sync| sync.master_broker_id))
                    .or_else(|| (election == TopologyElection::Configured).then_some(0));
                for member in &group.members {
                    if Some(member.broker_id) == master_id {
                        continue;
                    }
                    let connection = group.ha.as_ref().and_then(|ha| {
                        ha.connections
                            .iter()
                            .find(|connection| connection.replica.broker_id == member.broker_id)
                    });
                    let replica = TopologyReplica {
                        identity: key(member),
                        lag_bytes: connection.map(|connection| connection.diff),
                        in_sync: connection.map(|connection| connection.in_sync),
                        bytes_per_second: connection.map(|connection| connection.transferred_bytes_per_second),
                    };
                    if let Some(master) = group.members.iter().find(|member| Some(member.broker_id) == master_id)
                        && connection.is_some()
                    {
                        let mut edge = TopologyEdge::new(
                            TopologyNodeId::Broker(key(master)),
                            TopologyNodeId::Broker(replica.identity.clone()),
                            TopologyEdgeKind::Replication,
                            source,
                            now,
                        );
                        edge.lag_bytes = replica.lag_bytes;
                        edge.in_sync = replica.in_sync;
                        edge.bytes_per_second = replica.bytes_per_second;
                        layer.edges.push(edge);
                    }
                    replicas.push(replica);
                }
                let sync_state = group.sync_state.as_ref().map(|sync| TopologySyncState {
                    master_broker_id: sync.master_broker_id,
                    master_epoch: sync.master_epoch,
                    sync_state_set_epoch: sync.sync_state_set_epoch,
                    in_sync_broker_ids: sync.in_sync_replicas.iter().map(|replica| replica.broker_id).collect(),
                    out_of_sync_broker_ids: sync
                        .not_in_sync_replicas
                        .iter()
                        .map(|replica| replica.broker_id)
                        .collect(),
                });
                for address in &group.controllers {
                    if let Some(master) = group.members.iter().find(|member| Some(member.broker_id) == master_id) {
                        layer.edges.push(TopologyEdge::new(
                            TopologyNodeId::Controller(address.clone()),
                            TopologyNodeId::Broker(key(master)),
                            TopologyEdgeKind::Controller,
                            source,
                            now,
                        ));
                    }
                }
                layer.replication.push(TopologyReplicationGroup {
                    cluster: group.cluster,
                    broker_name: group.broker_name,
                    observed_at_ms: now,
                    election,
                    controllers: group.controllers,
                    replicas,
                    sync_state,
                    master_commit_log_max_offset: group.ha.as_ref().map(|ha| ha.master_commit_log_max_offset),
                    in_sync_slave_count: group.ha.as_ref().map(|ha| ha.in_sync_slave_count),
                    config_observed_at_ms: (election != TopologyElection::Unknown).then_some(now),
                    ha_observed_at_ms: group.ha.as_ref().map(|_| now),
                    sync_observed_at_ms: group.sync_state.as_ref().map(|_| now),
                });
            }
            for controller in read.controllers {
                let mut node = TopologyNode::new(
                    TopologyNodeId::Controller(controller.address.clone()),
                    controller.address.clone(),
                    source,
                    now,
                );
                node.availability = if controller.metadata.is_some() {
                    EndpointAvailability::Available
                } else {
                    EndpointAvailability::Unknown
                };
                layer.nodes.push(node);
                let metadata = controller.metadata.as_ref();
                layer.controllers.push(TopologyControllerInfo {
                    address: controller.address,
                    observed_at_ms: metadata.map(|_| now),
                    group: metadata.and_then(|meta| meta.group.clone()),
                    leader_id: metadata.and_then(|meta| meta.leader_id.clone()),
                    is_leader: metadata.and_then(|meta| meta.is_leader),
                    peer_count: metadata.and_then(|meta| meta.peer_count),
                    last_log_index: metadata.and_then(|meta| meta.last_log_index),
                    committed_log_index: metadata.and_then(|meta| meta.committed_log_index),
                    applied_log_index: metadata.and_then(|meta| meta.applied_log_index),
                });
            }
        }
        _ => {
            return Err(ProviderFailure::invalid_data(
                "Topology returned a different observation layer.",
            ));
        }
    }
    if layer.state == TopologySourceState::Failed {
        if !layer.nodes.is_empty() || !layer.replication.is_empty() {
            layer.state = TopologySourceState::Partial;
        } else {
            layer.observed_at_ms = None;
        }
    }
    Ok(layer)
}
