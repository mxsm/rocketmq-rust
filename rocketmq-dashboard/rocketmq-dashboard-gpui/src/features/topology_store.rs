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

use crate::route::{AppRoute, BrokerTab, ConsumerTab, RouteKey, TopicTab};
use rocketmq_dashboard_common::*;

#[derive(Default)]
pub(crate) struct TopologyStore {
    pub cache: TopologyCache,
    pub layout: TopologyLayout,
    pub selection: Option<TopologySelection>,
    pub cluster: Option<String>,
    pub clusters: Vec<String>,
    pub diff: TopologyDiff,
    pub changes: Vec<String>,
    pub sources: Vec<String>,
    pub detail: Vec<String>,
    pub traffic: Vec<String>,
    pub route: Option<AppRoute>,
    scale_index: usize,
}
impl TopologyStore {
    pub fn apply(&mut self, snapshot: TopologySnapshot) {
        self.diff = self.cache.apply(
            snapshot.context,
            snapshot.layers.into_values().collect(),
            snapshot.generated_at_ms,
        );
        self.clusters = self
            .cache
            .snapshot()
            .map(TopologySnapshot::clusters)
            .unwrap_or_default();
        if self
            .cluster
            .as_ref()
            .is_some_and(|cluster| !self.clusters.contains(cluster))
        {
            self.cluster = None;
        }
        self.rebuild();
    }
    pub fn clear(&mut self) {
        *self = Self::default();
    }
    pub fn expire(&mut self, now: u64) {
        self.diff = self.cache.expire(now);
        self.rebuild();
    }
    pub fn set_cluster(&mut self, cluster: Option<String>) {
        self.cluster = cluster;
        self.rebuild();
    }
    pub fn zoom(&mut self, increase: bool) {
        self.scale_index = if increase {
            (self.scale_index + 1).min(2)
        } else {
            self.scale_index.saturating_sub(1)
        };
        self.rebuild();
    }
    pub fn select(&mut self, id: TopologyNodeId) {
        if let Some(snapshot) = self.cache.snapshot() {
            self.selection = Some(TopologySelection {
                context: snapshot.context.clone(),
                node: id,
            });
        }
        self.rebuild();
    }
    pub fn rebuild(&mut self) {
        let Some(snapshot) = self.cache.snapshot() else {
            return;
        };
        self.layout = layout_topology(snapshot, self.cluster.as_deref(), [0.75, 1., 1.25][self.scale_index]);
        self.changes = vec![
            format!(
                "{} nodes added · {} node observations changed · {} confirmed node removals · {} expired/unconfirmed missing",
                self.diff.added.len(),
                self.diff.changed.len(),
                self.diff.removed.len(),
                self.diff.unconfirmed_missing
            ),
            format!(
                "Display omitted {} nodes and {} edges. Producer and Consumer sides are limited to 50 each.",
                self.layout.omitted_nodes, self.layout.omitted_edges
            ),
        ];
        self.sources = snapshot
            .layers
            .values()
            .map(|layer| {
                format!(
                    "{} · {:?}{}{} · {}{} records, {} drawn · {} edges · observed {}{}",
                    layer.source.label(),
                    layer.state,
                    layer
                        .code
                        .map_or_else(String::new, |code| format!(" / {}", code.label())),
                    if layer.truncated { " / bounded" } else { "" },
                    if layer.total_complete { "" } else { "≥" },
                    layer.total,
                    layer.nodes.len(),
                    layer.total_edges,
                    time(layer.observed_at_ms),
                    if layer.expired {
                        " / expired"
                    } else if layer.is_stale() {
                        " / cached"
                    } else {
                        ""
                    }
                )
            })
            .collect();
        self.traffic = self
            .layout
            .nodes
            .iter()
            .filter(|node| {
                node.node.traffic.produce_tps.is_some()
                    || node.node.traffic.consume_tps.is_some()
                    || node.node.traffic.backlog_messages.is_some()
            })
            .take(50)
            .map(|placed| {
                format!(
                    "{} · produce {} · consume {} · backlog {} messages{}",
                    placed.node.label,
                    rate(placed.node.traffic.produce_tps),
                    rate(placed.node.traffic.consume_tps),
                    number(placed.node.traffic.backlog_messages),
                    if placed.stale { " · cached" } else { "" }
                )
            })
            .collect();
        if self.traffic.is_empty() {
            self.traffic
                .push("No traffic metrics were observed in this view.".into());
        }
        let selected = self
            .selection
            .as_ref()
            .filter(|selection| selection.context == snapshot.context)
            .and_then(|selection| self.layout.nodes.iter().find(|node| node.node.id == selection.node));
        self.detail.clear();
        self.route = None;
        let Some(selected) = selected else {
            self.selection = None;
            return;
        };
        let node = &selected.node;
        self.detail.push(format!(
            "{} · {}{}",
            node.id.kind_label(),
            node.label,
            if selected.stale { " · cached" } else { "" }
        ));
        self.detail.push(format!(
            "Observed {} · availability {:?}",
            time(Some(node.observed_at_ms)),
            node.availability
        ));
        self.detail.push(format!(
            "Connections {} · produce {} · consume {} · backlog {} messages",
            number(node.traffic.connections),
            rate(node.traffic.produce_tps),
            rate(node.traffic.consume_tps),
            number(node.traffic.backlog_messages)
        ));
        self.route = match &node.id {
            TopologyNodeId::Configuration | TopologyNodeId::NameServer(_) => Some(AppRoute::OpsSettings),
            TopologyNodeId::Cluster(_) => Some(AppRoute::Brokers),
            TopologyNodeId::Broker(identity) => {
                self.detail.push(format!(
                    "Cluster {} · Broker {} · id {} · {} · version {}",
                    identity.cluster,
                    identity.broker_name,
                    identity.broker_id,
                    identity.address,
                    node.broker_version.as_deref().unwrap_or("Unknown")
                ));
                let mut observed_replication = false;
                for group in snapshot
                    .layers
                    .values()
                    .flat_map(|layer| layer.replication.iter())
                    .filter(|group| group.cluster == identity.cluster && group.broker_name == identity.broker_name)
                {
                    observed_replication = true;
                    self.detail.push(format!(
                        "Election {} · config {} · HA {} · Controller sync {}",
                        group.election.label(),
                        time(group.config_observed_at_ms),
                        time(group.ha_observed_at_ms),
                        time(group.sync_observed_at_ms)
                    ));
                    self.detail.push(format!(
                        "Master commit-log bytes {} · in-sync slave count {}",
                        number(group.master_commit_log_max_offset),
                        number(group.in_sync_slave_count)
                    ));
                    if let Some(replica) = group.replicas.iter().find(|replica| replica.identity == *identity) {
                        self.detail.push(format!(
                            "HA lag {} bytes · HA in-sync {} · transfer {} bytes/s",
                            number(replica.lag_bytes),
                            number(replica.in_sync),
                            number(replica.bytes_per_second)
                        ));
                    }
                    if let Some(sync) = &group.sync_state {
                        self.detail.push(format!("Controller master id {} · master epoch {} · sync epoch {} · in-sync {:?} · out-of-sync {:?}", sync.master_broker_id, sync.master_epoch, sync.sync_state_set_epoch, sync.in_sync_broker_ids, sync.out_of_sync_broker_ids));
                    }
                }
                if !observed_replication {
                    self.detail
                        .push("Election Unknown · No current replication observation for this Broker group.".into());
                }
                RouteKey::parse(&identity.address)
                    .ok()
                    .map(|broker| AppRoute::BrokerDetail {
                        broker,
                        tab: BrokerTab::Overview,
                    })
            }
            TopologyNodeId::Topic(name) => RouteKey::parse(name).ok().map(|topic| AppRoute::TopicDetail {
                topic,
                tab: TopicTab::Overview,
            }),
            TopologyNodeId::Consumer(name) => RouteKey::parse(name).ok().map(|group| AppRoute::ConsumerDetail {
                group,
                tab: ConsumerTab::Clients,
            }),
            TopologyNodeId::Producer(_) => Some(AppRoute::Producers),
            TopologyNodeId::Controller(address) => {
                for controller in snapshot
                    .layers
                    .values()
                    .flat_map(|layer| layer.controllers.iter())
                    .filter(|controller| controller.address == *address)
                {
                    self.detail.push(format!(
                        "Metadata {} · group {} · leader {} · leader id {} · peers {}",
                        time(controller.observed_at_ms),
                        controller.group.as_deref().unwrap_or("Unknown"),
                        number(controller.is_leader),
                        controller.leader_id.as_deref().unwrap_or("Unknown"),
                        number(controller.peer_count)
                    ));
                    self.detail.push(format!(
                        "Raft last {} · committed {} · applied {}",
                        number(controller.last_log_index),
                        number(controller.committed_log_index),
                        number(controller.applied_log_index)
                    ));
                }
                None
            }
        };
    }
}
pub(crate) fn time(value: Option<u64>) -> String {
    value
        .and_then(|value| i64::try_from(value).ok())
        .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis)
        .map(|time| time.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "Unknown".into())
}
fn number(value: Option<impl std::fmt::Display>) -> String {
    value.map(|value| value.to_string()).unwrap_or_else(|| "Unknown".into())
}
fn rate(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.2} msg/s"))
        .unwrap_or_else(|| "Unknown".into())
}
